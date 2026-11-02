// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

//! A `read_committed` Fetch never answers a last stable offset below its log start offset. When
//! DeleteRecords moves a partition's log start past the first offset of a transaction that is
//! still open, Kafka raises the first unstable offset to the log start
//! ([`UnifiedLog.scala#L964-L978`]).
//!
//! [`UnifiedLog.scala#L964-L978`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/log/UnifiedLog.scala#L964-L978

use std::time::SystemTime;

use crate::{
    common::{self, StorageType, alphanumeric_string, init_tracing, register_broker},
    list_offsets_after_retention::{Broker, broker, create_topic_with_open_transaction},
};
use nisshi_broker::Result;
use nisshi_sans_io::{
    ErrorCode, FetchRequest, IsolationLevel, RequestInput,
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
    fetch_request::{FetchPartition, FetchTopic},
    fetch_response::PartitionData,
};
use nisshi_storage::{Storage, TopicId};
use rama::{Service as _, extensions::Extensions};
use rand::{prelude::*, rng};
use url::Url;
use uuid::Uuid;

/// Fetches partition 0 of `name` from `fetch_offset` and returns the partition's answer.
async fn fetch<G>(
    sc: &G,
    broker: &Broker,
    extensions: &Extensions,
    name: &str,
    isolation_level: IsolationLevel,
    fetch_offset: i64,
) -> Result<PartitionData>
where
    G: Storage,
{
    // Fetch names a topic by its id from version 13, and the broker answers the latest version.
    let topic_id = sc
        .metadata(Some([TopicId::Name(name.into())].as_slice()))
        .await?
        .topics()[0]
        .topic_id;

    let response = broker
        .serve(RequestInput {
            request: FetchRequest::default()
                // No record is fetchable while the transaction is open, so the fetch waits this long.
                .max_wait_ms(100)
                .min_bytes(1)
                .max_bytes(Some(50 * 1024))
                .isolation_level(Some(isolation_level.into()))
                .session_id(Some(0))
                .session_epoch(Some(-1))
                .forgotten_topics_data(Some([].into()))
                .rack_id(Some(String::new()))
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(name.into()))
                        .topic_id(topic_id)
                        .partitions(Some(
                            [FetchPartition::default()
                                .partition(0)
                                .current_leader_epoch(Some(-1))
                                .fetch_offset(fetch_offset)
                                .last_fetched_epoch(Some(-1))
                                .log_start_offset(Some(-1))
                                .partition_max_bytes(50 * 1024)]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    Ok(response.responses.unwrap_or_default()[0]
        .partitions
        .clone()
        .unwrap_or_default()[0]
        .clone())
}

/// When DeleteRecords moves the log start past an open transaction's first offset, a
/// `read_committed` Fetch from the new log start answers the log start as its last stable offset.
pub async fn last_stable_offset_is_log_start_after_delete_records_past_open_transaction<G>(
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;
    let extensions = Extensions::default();
    let topic = &alphanumeric_string(15)[..];

    let transaction_start =
        create_topic_with_open_transaction(&sc, &broker, &extensions, topic, SystemTime::now())
            .await?;

    let log_start = transaction_start + 1;

    let results = sc
        .delete_records(&[DeleteRecordsTopic::default()
            .name(topic.into())
            .partitions(Some(
                [DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(log_start)]
                .into(),
            ))])
        .await?;
    let partition = &results[0].partitions.as_deref().unwrap_or_default()[0];
    assert_eq!(i16::from(ErrorCode::None), partition.error_code);
    assert_eq!(log_start, partition.low_watermark);

    let partition = fetch(
        &sc,
        &broker,
        &extensions,
        topic,
        IsolationLevel::ReadCommitted,
        log_start,
    )
    .await?;
    assert_eq!(i16::from(ErrorCode::None), partition.error_code);
    assert_eq!(Some(log_start), partition.log_start_offset);
    assert_eq!(Some(log_start), partition.last_stable_offset);

    Ok(())
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage_type:expr) => {
        #[cfg(feature = $feature)]
        mod $name {
            use super::*;

            #[tokio::test]
            async fn last_stable_offset_is_log_start_after_delete_records_past_open_transaction()
            -> Result<()> {
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

                super::last_stable_offset_is_log_start_after_delete_records_past_open_transaction(
                    sc,
                )
                .await
            }
        }
    };
}

backend!(pg, "postgres", StorageType::Postgres);
backend!(lite, "libsql", StorageType::Lite);
backend!(in_memory, "dynostore", StorageType::InMemory);
