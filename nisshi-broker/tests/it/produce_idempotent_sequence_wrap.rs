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

//! The next sequence that the Postgres, libSQL and Turso engines store for an idempotent producer
//! wraps to 0 after `i32::MAX`, as Kafka's [`DefaultRecordBatch.incrementSequence`] does.
//!
//! A producer needs about two billion records on one partition to reach the largest sequence. Each
//! test gets there in one batch instead: an engine takes how far a batch advances the sequence from
//! the batch's `last_offset_delta`, so a batch of one record can claim a long span. The Produce
//! service refuses such a batch, so the tests call the storage's `produce` and not the service.
//!
//! [`DefaultRecordBatch.incrementSequence`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L556-L560

use std::assert_matches;

use crate::common::{alphanumeric_string, init_tracing};
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    ErrorCode,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{Error, Storage, Topition};

/// Produces to `topition` as `producer`, a batch of one record at `base_sequence` that advances the
/// producer's sequence by `span`.
async fn produce<G>(
    storage: &G,
    topition: &Topition,
    producer: i64,
    base_sequence: i32,
    span: i32,
) -> nisshi_storage::Result<i64>
where
    G: Storage,
{
    let batch = inflated::Batch::builder()
        .producer_id(producer)
        .producer_epoch(0)
        .base_sequence(base_sequence)
        .last_offset_delta(span - 1)
        .record(
            Record::builder()
                .offset_delta(0)
                .value(Some(Bytes::from_static(b"value"))),
        )
        .build()
        .and_then(deflated::Batch::try_from)
        .expect("well-formed batch");

    storage.produce(None, topition, batch).await
}

/// Creates a topic of one partition, an idempotent producer, and stores the producer's first batch,
/// which leaves the next sequence at `i32::MAX - 1`.
async fn partition_near_the_largest_sequence<G>(storage: &G) -> Result<(Topition, i64)>
where
    G: Storage,
{
    let topic = alphanumeric_string(15);

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?;
    assert_eq!(ErrorCode::None, producer.error);

    let topition = Topition::new(topic, 0);

    // A new partition accepts only sequence 0 as the first batch of these engines.
    _ = produce(storage, &topition, producer.id, 0, i32::MAX - 1).await?;

    Ok((topition, producer.id))
}

pub async fn a_batch_that_ends_at_the_largest_sequence_wraps_the_next_sequence_to_0<G>(
    storage: G,
) -> Result<()>
where
    G: Storage,
{
    let (topition, producer) = partition_near_the_largest_sequence(&storage).await?;

    // The batch's sequences are `i32::MAX - 1` and `i32::MAX`, so the next one is 0.
    _ = produce(&storage, &topition, producer, i32::MAX - 1, 2).await?;
    _ = produce(&storage, &topition, producer, 0, 1).await?;

    Ok(())
}

pub async fn a_batch_that_crosses_the_largest_sequence_wraps_the_next_sequence<G>(
    storage: G,
) -> Result<()>
where
    G: Storage,
{
    let (topition, producer) = partition_near_the_largest_sequence(&storage).await?;

    // The batch's sequences are `i32::MAX - 1`, `i32::MAX`, 0, 1 and 2, so the next one is 3.
    _ = produce(&storage, &topition, producer, i32::MAX - 1, 5).await?;
    _ = produce(&storage, &topition, producer, 3, 1).await?;

    Ok(())
}

pub async fn the_wrapped_sequence_is_the_next_sequence_exactly<G>(storage: G) -> Result<()>
where
    G: Storage,
{
    let (topition, producer) = partition_near_the_largest_sequence(&storage).await?;

    _ = produce(&storage, &topition, producer, i32::MAX - 1, 5).await?;

    // The next sequence is 3: one below it is a duplicate, and one above it is out of order.
    assert_matches!(
        produce(&storage, &topition, producer, 2, 1).await,
        Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
    );
    assert_matches!(
        produce(&storage, &topition, producer, 4, 1).await,
        Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
    );

    Ok(())
}

/// Runs each listed test above against one engine.
macro_rules! backend_tests {
    ($module:ident, $feature:literal, $storage:path, [$($test:ident),+ $(,)?]) => {
        #[cfg(feature = $feature)]
        mod $module {
            use nisshi_broker::Result;
            use rand::{RngExt as _, rng};
            use uuid::Uuid;

            use super::*;

            async fn storage() -> Result<impl Storage + Clone> {
                $storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await
            }

            $(
                #[tokio::test]
                async fn $test() -> Result<()> {
                    let _guard = init_tracing()?;
                    super::$test(storage().await?).await
                }
            )+
        }
    };
}

#[cfg(feature = "turso")]
async fn turso_storage(cluster: uuid::Uuid, node: i32) -> Result<nisshi_storage::ArcDynStorage> {
    crate::common::storage_container(
        crate::common::StorageType::Turso,
        cluster,
        node,
        url::Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await
}

backend_tests!(
    lite,
    "libsql",
    crate::common::lite_storage,
    [
        a_batch_that_ends_at_the_largest_sequence_wraps_the_next_sequence_to_0,
        a_batch_that_crosses_the_largest_sequence_wraps_the_next_sequence,
        the_wrapped_sequence_is_the_next_sequence_exactly,
    ]
);
backend_tests!(
    pg,
    "postgres",
    crate::common::postgres_storage,
    [
        a_batch_that_ends_at_the_largest_sequence_wraps_the_next_sequence_to_0,
        a_batch_that_crosses_the_largest_sequence_wraps_the_next_sequence,
        the_wrapped_sequence_is_the_next_sequence_exactly,
    ]
);
backend_tests!(
    turso,
    "turso",
    turso_storage,
    [
        a_batch_that_ends_at_the_largest_sequence_wraps_the_next_sequence_to_0,
        a_batch_that_crosses_the_largest_sequence_wraps_the_next_sequence,
        the_wrapped_sequence_is_the_next_sequence_exactly,
    ]
);
