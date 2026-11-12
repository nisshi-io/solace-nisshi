// Copyright ⓒ Solace Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! One DeleteRecords request over several partitions cuts each of them, as the
//! partitions run concurrently in the service ([`ReplicaManager.scala#L1122-L1146`]
//! answers each partition on its own).
//!
//! [`ReplicaManager.scala#L1122-L1146`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1122-L1146

use crate::common::{self, StorageType, alphanumeric_string, init_tracing, register_broker};
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    DeleteRecordsRequest, ErrorCode, RequestInput,
    create_topics_request::CreatableTopic,
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
    record::{Record, inflated},
};
use nisshi_storage::{DeleteRecordsService, Storage, Topition};
use rama::{Service as _, extensions::Extensions};
use rand::{prelude::*, rng};
use url::Url;
use uuid::Uuid;

/// More partitions than the service runs at once, so that calls queue.
const PARTITIONS: i32 = 10;

/// Each partition gets `partition + 2` records and a cut at 1, so the
/// partitions' answers differ and a mixed-up answer shows.
async fn every_partition_is_cut<G>(sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = alphanumeric_string(15);

    _ = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic.clone())
                .num_partitions(PARTITIONS)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    for partition in 0..PARTITIONS {
        for _ in 0..partition + 2 {
            let batch = inflated::Batch::builder()
                .record(Record::builder().value(Some(Bytes::from_static(b"v"))))
                .build()
                .and_then(TryInto::try_into)?;

            _ = sc
                .produce(None, &Topition::new(topic.clone(), partition), batch)
                .await?;
        }
    }

    let response = DeleteRecordsService {
        storage: sc.clone(),
    }
    .serve(RequestInput {
        request: DeleteRecordsRequest::default().topics(Some(
            [DeleteRecordsTopic::default()
                .name(topic.clone())
                .partitions(Some(
                    (0..PARTITIONS)
                        .map(|partition| {
                            DeleteRecordsPartition::default()
                                .partition_index(partition)
                                .offset(1)
                        })
                        .collect(),
                ))]
            .into(),
        )),
        extensions: Extensions::default(),
    })
    .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());

    let partitions = topics[0].partitions.clone().unwrap_or_default();
    assert_eq!(
        (0..PARTITIONS).collect::<Vec<_>>(),
        partitions
            .iter()
            .map(|partition| partition.partition_index)
            .collect::<Vec<_>>()
    );

    for partition in &partitions {
        assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
        assert_eq!(1, partition.low_watermark);

        let stage = sc
            .offset_stage(&Topition::new(topic.clone(), partition.partition_index))
            .await?;
        assert_eq!(1, stage.log_start());
        assert_eq!(
            i64::from(partition.partition_index) + 2,
            stage.high_watermark()
        );
    }

    Ok(())
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage_type:expr) => {
        #[cfg(feature = $feature)]
        mod $name {
            use super::*;

            #[tokio::test]
            async fn every_partition_is_cut() -> Result<()> {
                let _guard = init_tracing()?;

                let cluster_id = Uuid::now_v7();
                let broker_id = rng().random_range(0..i32::MAX);

                let sc = common::storage_container(
                    $storage_type,
                    cluster_id,
                    broker_id,
                    Url::parse("tcp://127.0.0.1/")?,
                    None,
                )
                .await?;
                register_broker(cluster_id, broker_id, &sc).await?;

                super::every_partition_is_cut(sc).await
            }
        }
    };
}

backend!(pg, "postgres", StorageType::Postgres);
backend!(lite, "libsql", StorageType::Lite);
backend!(in_memory, "dynostore", StorageType::InMemory);
