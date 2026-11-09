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

//! What a partition that holds no state for an idempotent producer answers to the producer's first
//! batch.
//!
//! The DynoStore engine (the in-memory engine here) accepts any first sequence, as Kafka does,
//! because it remembers its recent batches and so tells a batch it stored from a gap. The
//! Postgres, libSQL and Turso engines keep no such record: they answer every sequence below the
//! stored one as a duplicate, which a Java producer completes as a success. They accept only
//! sequence 0 as a first batch, so that a later batch is never stored ahead of an earlier one that
//! failed and is then sent again.

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

/// Produces to `topition` as `producer`, a batch of `record_count` records at `base_sequence`.
async fn produce<G>(
    storage: &G,
    topition: &Topition,
    producer: i64,
    base_sequence: i32,
    record_count: i32,
) -> nisshi_storage::Result<i64>
where
    G: Storage,
{
    let mut builder = inflated::Batch::builder()
        .producer_id(producer)
        .producer_epoch(0)
        .base_sequence(base_sequence)
        .last_offset_delta(record_count - 1);

    for delta in 0..record_count {
        builder = builder.record(
            Record::builder()
                .offset_delta(delta)
                .value(Some(Bytes::from_static(b"value"))),
        );
    }

    let batch = builder
        .build()
        .and_then(deflated::Batch::try_from)
        .expect("well-formed batch");

    storage.produce(None, topition, batch).await
}

/// Creates a topic of one partition and an idempotent producer.
async fn topic_and_producer<G>(storage: &G) -> Result<(Topition, i64)>
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

    Ok((Topition::new(topic, 0), producer.id))
}

pub async fn a_new_partition_accepts_any_first_sequence<G>(storage: G) -> Result<()>
where
    G: Storage,
{
    let (topition, producer) = topic_and_producer(&storage).await?;

    // The partition holds no state for the producer, so it accepts sequence 5, and the next
    // sequence it stores for the producer is 8.
    assert_eq!(0, produce(&storage, &topition, producer, 5, 3).await?);

    assert_matches!(
        produce(&storage, &topition, producer, 9, 1).await,
        Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
    );
    assert_eq!(3, produce(&storage, &topition, producer, 8, 2).await?);
    assert_eq!(5, storage.offset_stage(&topition).await?.high_watermark());

    Ok(())
}

pub async fn a_new_partition_accepts_the_largest_first_sequence_and_wraps<G>(
    storage: G,
) -> Result<()>
where
    G: Storage,
{
    let (topition, producer) = topic_and_producer(&storage).await?;

    // The batch ends at the largest sequence, so the next one wraps to 0, as in Kafka.
    assert_eq!(
        0,
        produce(&storage, &topition, producer, i32::MAX - 1, 2).await?
    );
    assert_eq!(2, produce(&storage, &topition, producer, 0, 1).await?);

    Ok(())
}

pub async fn a_negative_first_sequence_is_refused_and_stores_nothing<G>(storage: G) -> Result<()>
where
    G: Storage,
{
    let (topition, producer) = topic_and_producer(&storage).await?;

    assert_matches!(
        produce(&storage, &topition, producer, -3, 1).await,
        Err(Error::Api(ErrorCode::InvalidRecord))
    );
    assert_eq!(0, storage.offset_stage(&topition).await?.high_watermark());

    // The refused batch left no state, so the producer still starts at sequence 0.
    assert_eq!(0, produce(&storage, &topition, producer, 0, 1).await?);

    Ok(())
}

/// Kafka refuses a negative sequence in `LogValidator`, before it reads the partition's producer
/// state, so a partition that holds state for the producer answers INVALID_RECORD too, not
/// DUPLICATE_SEQUENCE_NUMBER or OUT_OF_ORDER_SEQUENCE_NUMBER.
pub async fn a_negative_sequence_on_a_partition_with_state_is_refused_and_stores_nothing<G>(
    storage: G,
) -> Result<()>
where
    G: Storage,
{
    let (topition, producer) = topic_and_producer(&storage).await?;

    assert_eq!(0, produce(&storage, &topition, producer, 0, 2).await?);

    // -1 is left out: with a producer ID it is a batch without a sequence, which storage takes as
    // a plain batch, and the Produce service refuses it before storage (see `rejection`).
    for base_sequence in [-2, -3, i32::MIN] {
        assert_matches!(
            produce(&storage, &topition, producer, base_sequence, 1).await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        );
    }
    assert_eq!(2, storage.offset_stage(&topition).await?.high_watermark());

    // The refused batches changed no state, so the next sequence is accepted.
    assert_eq!(2, produce(&storage, &topition, producer, 2, 1).await?);

    Ok(())
}

pub async fn a_batch_that_overtook_a_failed_first_batch_does_not_make_its_resend_a_duplicate<G>(
    storage: G,
) -> Result<()>
where
    G: Storage,
{
    let (topition, producer) = topic_and_producer(&storage).await?;

    // The first batch (sequence 0, three records) never reaches storage. The second batch
    // (sequence 3) is accepted as the producer's first.
    assert_eq!(0, produce(&storage, &topition, producer, 3, 1).await?);

    // The client sends the first batch again, whole or in pieces. Neither matches a batch that the
    // engine remembers, so each is out of order. A duplicate answer would make the client
    // report records delivered that were never stored.
    assert_matches!(
        produce(&storage, &topition, producer, 0, 3).await,
        Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
    );
    assert_matches!(
        produce(&storage, &topition, producer, 0, 1).await,
        Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
    );
    assert_eq!(1, storage.offset_stage(&topition).await?.high_watermark());

    Ok(())
}

pub async fn a_first_sequence_above_zero_is_out_of_order_and_stores_nothing<G>(
    storage: G,
) -> Result<()>
where
    G: Storage,
{
    let (topition, producer) = topic_and_producer(&storage).await?;

    assert_matches!(
        produce(&storage, &topition, producer, 5, 1).await,
        Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
    );
    assert_eq!(0, storage.offset_stage(&topition).await?.high_watermark());

    // The refused batches left no state, so the producer still starts at sequence 0.
    assert_eq!(0, produce(&storage, &topition, producer, 0, 2).await?);

    Ok(())
}

pub async fn a_later_batch_is_not_stored_ahead_of_a_first_batch_that_failed<G>(
    storage: G,
) -> Result<()>
where
    G: Storage,
{
    let (topition, producer) = topic_and_producer(&storage).await?;

    // The first batch (sequence 0, three records) never reaches storage. The engine keeps no
    // record of recent batches, so it refuses the second batch (sequence 3) instead of storing it
    // ahead of the first, because it could not tell the first batch's resend from a duplicate.
    assert_matches!(
        produce(&storage, &topition, producer, 3, 1).await,
        Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
    );
    assert_eq!(0, produce(&storage, &topition, producer, 0, 3).await?);
    assert_eq!(3, produce(&storage, &topition, producer, 3, 1).await?);

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
    in_memory,
    "dynostore",
    crate::common::memory_storage,
    [
        a_new_partition_accepts_any_first_sequence,
        a_new_partition_accepts_the_largest_first_sequence_and_wraps,
        a_negative_first_sequence_is_refused_and_stores_nothing,
        a_negative_sequence_on_a_partition_with_state_is_refused_and_stores_nothing,
        a_batch_that_overtook_a_failed_first_batch_does_not_make_its_resend_a_duplicate,
    ]
);
backend_tests!(
    lite,
    "libsql",
    crate::common::lite_storage,
    [
        a_first_sequence_above_zero_is_out_of_order_and_stores_nothing,
        a_negative_first_sequence_is_refused_and_stores_nothing,
        a_negative_sequence_on_a_partition_with_state_is_refused_and_stores_nothing,
        a_later_batch_is_not_stored_ahead_of_a_first_batch_that_failed,
    ]
);
backend_tests!(
    pg,
    "postgres",
    crate::common::postgres_storage,
    [
        a_first_sequence_above_zero_is_out_of_order_and_stores_nothing,
        a_negative_first_sequence_is_refused_and_stores_nothing,
        a_negative_sequence_on_a_partition_with_state_is_refused_and_stores_nothing,
        a_later_batch_is_not_stored_ahead_of_a_first_batch_that_failed,
    ]
);
backend_tests!(
    turso,
    "turso",
    turso_storage,
    [
        a_first_sequence_above_zero_is_out_of_order_and_stores_nothing,
        a_negative_first_sequence_is_refused_and_stores_nothing,
        a_negative_sequence_on_a_partition_with_state_is_refused_and_stores_nothing,
        a_later_batch_is_not_stored_ahead_of_a_first_batch_that_failed,
    ]
);
