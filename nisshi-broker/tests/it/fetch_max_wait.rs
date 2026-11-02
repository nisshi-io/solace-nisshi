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

//! How a Fetch's `max_wait` and `min_bytes` decide when it answers, on every engine, as in
//! Kafka. A Fetch answers at once with what the log holds when `max_wait` is 0, or when the log
//! already holds `min_bytes`. Otherwise it waits for records produced during `max_wait`, and
//! answers once they reach `min_bytes`, or when `max_wait` has passed.

use std::time::{Duration, Instant};

use crate::common::{self, StorageType, alphanumeric_string, init_tracing, register_broker};
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    ErrorCode, FetchRequest, IsolationLevel, NULL_TOPIC_ID, RequestInput,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    fetch_response::PartitionData,
    record::{Record, inflated},
};
use nisshi_storage::{FetchService, FetchValidationLayer, Storage, Topition};
use rama::{Layer as _, Service, extensions::Extensions};
use rand::{prelude::*, rng};
use tracing::debug;
use url::Url;
use uuid::Uuid;

/// The `max_wait` of a fetch that must answer without waiting for it to pass.
const MAX_WAIT: Duration = Duration::from_secs(10);

/// How long after a fetch starts a test produces the record that the fetch waits for: well into
/// the fetch's wait, and well short of [`MAX_WAIT`].
const PRODUCED_AFTER: Duration = Duration::from_millis(500);

/// The `max_wait` of a fetch that must wait for it to pass, kept short so the test is quick.
const SHORT_WAIT: Duration = Duration::from_millis(500);

/// A response size limit for these tests: the Java consumer's default `fetch.max.bytes`, far more
/// than the one small record that each test writes, so it never cuts an answer short.
const TEST_MAX_BYTES: i32 = 52_428_800;

/// A partition size limit for these tests: the Java consumer's default
/// `max.partition.fetch.bytes`, far more than the one small record that each test writes.
const TEST_PARTITION_MAX_BYTES: i32 = 1_048_576;

/// Creates a topic with one partition, and returns that partition.
async fn create_topic<G>(cluster_id: Uuid, broker_id: i32, storage: &G) -> Result<Topition>
where
    G: Storage,
{
    create_topic_with_partitions(cluster_id, broker_id, storage, 1)
        .await
        .map(|topic_name| Topition::new(topic_name, 0))
}

/// Creates a topic with `partition_count` partitions, and returns its name.
async fn create_topic_with_partitions<G>(
    cluster_id: Uuid,
    broker_id: i32,
    storage: &G,
    partition_count: i32,
) -> Result<String>
where
    G: Storage,
{
    register_broker(cluster_id, broker_id, storage).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(partition_count)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(topic_name)
}

async fn produce_one_record<G>(storage: &G, topition: &Topition) -> Result<()>
where
    G: Storage,
{
    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::copy_from_slice(
            alphanumeric_string(15).as_bytes(),
        ))))
        .build()
        .and_then(TryInto::try_into)?;

    _ = storage.produce(None, topition, batch).await?;

    Ok(())
}

/// Fetches `topition` from offset 0, and returns its answer and how long the request took.
async fn fetch_from_start<G>(
    storage: &G,
    topition: &Topition,
    max_wait: Duration,
    min_bytes: i32,
) -> Result<(Vec<PartitionData>, Duration)>
where
    G: Storage + Clone,
{
    fetch_partitions_from_start(
        storage,
        topition.topic(),
        &[topition.partition()],
        max_wait,
        min_bytes,
    )
    .await
}

/// Fetches `partitions` of `topic_name` from offset 0, and returns their answers, in request
/// order, and how long the request took.
async fn fetch_partitions_from_start<G>(
    storage: &G,
    topic_name: &str,
    partitions: &[i32],
    max_wait: Duration,
    min_bytes: i32,
) -> Result<(Vec<PartitionData>, Duration)>
where
    G: Storage + Clone,
{
    let started_at = Instant::now();

    let response = FetchValidationLayer::new()
        .layer(FetchService {
            storage: storage.clone(),
        })
        .serve(RequestInput {
            request: FetchRequest::default()
                .max_wait_ms(i32::try_from(max_wait.as_millis())?)
                .min_bytes(min_bytes)
                .max_bytes(Some(TEST_MAX_BYTES))
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
                                        .partition_max_bytes(TEST_PARTITION_MAX_BYTES)
                                })
                                .collect(),
                        ))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let elapsed = started_at.elapsed();

    let partitions = response
        .responses
        .unwrap_or_default()
        .into_iter()
        .flat_map(|topic| topic.partitions.unwrap_or_default())
        .collect();

    Ok((partitions, elapsed))
}

/// The number of records in a partition answer.
fn record_count(partition: &PartitionData) -> u32 {
    partition.records.as_ref().map_or(0, |frame| {
        frame.batches.iter().map(|batch| batch.record_count).sum()
    })
}

/// The size of the records in a partition answer, as Fetch counts it against `min_bytes`.
fn record_bytes(partition: &PartitionData) -> usize {
    partition.records.as_ref().map_or(0, |frame| {
        frame
            .batches
            .iter()
            .map(|batch| batch.record_data.len())
            .sum()
    })
}

pub async fn exactly_min_bytes_answers_at_once<G>(
    cluster_id: Uuid,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topition = create_topic(cluster_id, broker_id, &storage).await?;
    produce_one_record(&storage, &topition).await?;

    // The size of the record depends on how the engine encodes it, so a first fetch measures it.
    // It must be more than the 1 byte that this fetch asks for, or the second fetch below tests
    // nothing new.
    let (partitions, _) = fetch_from_start(&storage, &topition, MAX_WAIT, 1).await?;
    let record_size = i32::try_from(record_bytes(&partitions[0]))?;
    assert!(record_size > 1, "{record_size}");

    let (partitions, elapsed) =
        fetch_from_start(&storage, &topition, MAX_WAIT, record_size).await?;

    assert!(elapsed < MAX_WAIT, "{elapsed:?}");
    assert_eq!(1, record_count(&partitions[0]));

    Ok(())
}

pub async fn zero_wait_answers_every_partition<G>(
    cluster_id: Uuid,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic_name = create_topic_with_partitions(cluster_id, broker_id, &storage, 3).await?;

    for partition in [0, 2] {
        produce_one_record(&storage, &Topition::new(topic_name.clone(), partition)).await?;
    }

    let (partitions, _) =
        fetch_partitions_from_start(&storage, &topic_name, &[0, 1, 2], Duration::ZERO, 1).await?;

    assert_eq!(
        vec![0, 1, 2],
        partitions
            .iter()
            .map(|partition| partition.partition_index)
            .collect::<Vec<_>>()
    );

    for partition in &partitions {
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partition.error_code)?,
            "partition {}",
            partition.partition_index
        );
    }

    assert_eq!(
        vec![1, 0, 1],
        partitions.iter().map(record_count).collect::<Vec<_>>()
    );
    assert_eq!(
        vec![1, 0, 1],
        partitions
            .iter()
            .map(|partition| partition.high_watermark)
            .collect::<Vec<_>>()
    );
    assert!(
        partitions
            .iter()
            .all(|partition| partition.log_start_offset == Some(0)),
        "{partitions:?}"
    );

    Ok(())
}

pub async fn records_produced_during_the_wait_are_returned<G>(
    cluster_id: Uuid,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topition = create_topic(cluster_id, broker_id, &storage).await?;

    // The record arrives after the fetch's first read found the partition empty, and long before
    // `max_wait` ends.
    let produce_later = async {
        tokio::time::sleep(PRODUCED_AFTER).await;
        produce_one_record(&storage, &topition).await
    };

    let (fetched, produced) = tokio::join!(
        fetch_from_start(&storage, &topition, MAX_WAIT, 1),
        produce_later
    );
    produced?;
    let (partitions, elapsed) = fetched?;

    assert!(elapsed < MAX_WAIT, "{elapsed:?}");
    assert_eq!(1, partitions.len());
    assert_eq!(1, record_count(&partitions[0]));

    Ok(())
}

pub async fn an_empty_partition_waits_for_max_wait<G>(
    cluster_id: Uuid,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topition = create_topic(cluster_id, broker_id, &storage).await?;

    let (partitions, elapsed) = fetch_from_start(&storage, &topition, SHORT_WAIT, 1).await?;

    // The fetch answers once another round of reads would end after `max_wait`, so it can answer
    // up to one round early. Half of `max_wait` shows that it waited rather than answered at once.
    assert!(elapsed > SHORT_WAIT / 2, "{elapsed:?}");
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert_eq!(0, record_count(&partitions[0]));

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

/// Generates one test for each scenario on `$storage_type`.
macro_rules! scenarios {
    ($storage_type:expr) => {
        use super::*;

        async fn storage() -> Result<(Uuid, i32, nisshi_storage::ArcDynStorage)> {
            let cluster_id = Uuid::now_v7();
            let broker_id = rng().random_range(0..i32::MAX);
            let storage = storage_container($storage_type, cluster_id, broker_id).await?;
            Ok((cluster_id, broker_id, storage))
        }

        #[tokio::test]
        async fn exactly_min_bytes_answers_at_once() -> Result<()> {
            let _guard = init_tracing()?;
            let (cluster_id, broker_id, storage) = storage().await?;
            super::exactly_min_bytes_answers_at_once(cluster_id, broker_id, storage).await
        }

        #[tokio::test]
        async fn zero_wait_answers_every_partition() -> Result<()> {
            let _guard = init_tracing()?;
            let (cluster_id, broker_id, storage) = storage().await?;
            super::zero_wait_answers_every_partition(cluster_id, broker_id, storage).await
        }

        #[tokio::test]
        async fn records_produced_during_the_wait_are_returned() -> Result<()> {
            let _guard = init_tracing()?;
            let (cluster_id, broker_id, storage) = storage().await?;
            super::records_produced_during_the_wait_are_returned(cluster_id, broker_id, storage)
                .await
        }

        #[tokio::test]
        async fn an_empty_partition_waits_for_max_wait() -> Result<()> {
            let _guard = init_tracing()?;
            let (cluster_id, broker_id, storage) = storage().await?;
            super::an_empty_partition_waits_for_max_wait(cluster_id, broker_id, storage).await
        }
    };
}

#[cfg(feature = "postgres")]
mod pg {
    scenarios!(StorageType::Postgres);
}

#[cfg(feature = "dynostore")]
mod in_memory {
    scenarios!(StorageType::InMemory);
}

#[cfg(feature = "libsql")]
mod lite {
    scenarios!(StorageType::Lite);
}
