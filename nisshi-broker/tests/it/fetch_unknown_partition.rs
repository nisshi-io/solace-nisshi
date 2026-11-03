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

//! Fetch answers `UNKNOWN_TOPIC_OR_PARTITION` in the response for a partition
//! index that an existing topic does not have, on every engine, and the
//! service goes on answering.

use crate::common::{self, StorageType, alphanumeric_string, init_tracing, register_broker};
use nisshi_broker::Result;
use nisshi_sans_io::{
    ErrorCode, FetchRequest, IsolationLevel, NULL_TOPIC_ID, RequestInput,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
};
use nisshi_storage::{FetchService, FetchValidationLayer, Storage};
use rama::{Layer as _, Service, extensions::Extensions};
use rand::{prelude::*, rng};
use tracing::debug;
use url::Url;
use uuid::Uuid;

const PARTITIONS: i32 = 3;

fn fetch_request(topic_name: &str, partitions: &[i32]) -> FetchRequest {
    FetchRequest::default()
        .max_wait_ms(200)
        .min_bytes(1)
        .max_bytes(Some(50 * 1024))
        .isolation_level(Some((&IsolationLevel::ReadUncommitted).into()))
        .topics(Some(
            [FetchTopic::default()
                .topic(Some(topic_name.into()))
                .topic_id(Some(NULL_TOPIC_ID))
                .partitions(Some(
                    partitions
                        .iter()
                        .map(|partition| {
                            FetchPartition::default()
                                .partition(*partition)
                                .current_leader_epoch(Some(-1))
                                .fetch_offset(0)
                                .last_fetched_epoch(Some(-1))
                                .log_start_offset(Some(-1))
                                .partition_max_bytes(50 * 1024)
                        })
                        .collect(),
                ))]
            .into(),
        ))
}

pub async fn partition_index_outside_topic<C, G>(cluster_id: C, broker_id: i32, sc: G) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    _ = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(PARTITIONS)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let service = FetchValidationLayer::new().layer(FetchService { storage: sc });

    let requested = [0, PARTITIONS, -1, i32::MAX];

    let response = service
        .serve(RequestInput {
            request: fetch_request(&topic_name, &requested),
            extensions: Extensions::default(),
        })
        .await?;

    let partitions = response
        .responses
        .unwrap_or_default()
        .into_iter()
        .flat_map(|topic| topic.partitions.unwrap_or_default())
        .collect::<Vec<_>>();

    assert_eq!(
        requested.to_vec(),
        partitions
            .iter()
            .map(|partition| partition.partition_index)
            .collect::<Vec<_>>()
    );

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );

    for partition in &partitions[1..] {
        assert_eq!(
            ErrorCode::UnknownTopicOrPartition,
            ErrorCode::try_from(partition.error_code)?,
            "partition {}",
            partition.partition_index
        );
        assert_eq!(None, partition.records);
    }

    // The same service answers the next request on the topic.
    let response = service
        .serve(RequestInput {
            request: fetch_request(&topic_name, &[PARTITIONS - 1]),
            extensions: Extensions::default(),
        })
        .await?;

    let partitions = response
        .responses
        .unwrap_or_default()
        .into_iter()
        .flat_map(|topic| topic.partitions.unwrap_or_default())
        .collect::<Vec<_>>();

    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );

    Ok(())
}

async fn storage_container(
    storage_type: StorageType,
    cluster: impl Into<String> + Clone,
    node: i32,
) -> Result<nisshi_storage::ArcDynStorage> {
    common::storage_container(
        storage_type,
        cluster,
        node,
        Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await
}

async fn run(storage_type: StorageType) -> Result<()> {
    let _guard = init_tracing()?;

    let cluster_id = Uuid::now_v7();
    let broker_id = rng().random_range(0..i32::MAX);

    partition_index_outside_topic(
        cluster_id,
        broker_id,
        storage_container(storage_type, cluster_id, broker_id).await?,
    )
    .await
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    #[tokio::test]
    async fn partition_index_outside_topic() -> Result<()> {
        run(StorageType::Postgres).await
    }
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;

    #[tokio::test]
    async fn partition_index_outside_topic() -> Result<()> {
        run(StorageType::InMemory).await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;

    #[tokio::test]
    async fn partition_index_outside_topic() -> Result<()> {
        run(StorageType::Lite).await
    }
}

#[cfg(feature = "turso")]
mod turso {
    use super::*;

    #[tokio::test]
    async fn partition_index_outside_topic() -> Result<()> {
        run(StorageType::Turso).await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use super::*;

    #[tokio::test]
    async fn partition_index_outside_topic() -> Result<()> {
        run(StorageType::SlateDb).await
    }
}
