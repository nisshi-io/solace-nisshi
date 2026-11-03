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

//! An idempotent producer bumps its own epoch after an error: it keeps its producer ID,
//! adds one to its epoch, and restarts its sequence at 0 ([KIP-360]). Kafka 3.9.1 accepts
//! a higher epoch whose first sequence is 0, and answers OUT_OF_ORDER_SEQUENCE_NUMBER for
//! any other sequence ([ProducerAppendInfo.java]).
//!
//! [KIP-360]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-360%3A+Improve+reliability+of+idempotent%2Ftransactional+producer
//! [ProducerAppendInfo.java]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerAppendInfo.java#L112-L143

use std::assert_matches;

use crate::common::{
    alphanumeric_string, init_tracing, lite_storage, memory_storage, postgres_storage,
    register_broker,
};
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    BatchAttribute, ErrorCode, IsolationLevel, ListOffset,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{ArcDynStorage, Error, ProducerIdResponse, Storage, Topition};
use rand::{RngExt as _, rng};
use uuid::Uuid;

/// Creates a topic with two partitions, and returns its first partition.
async fn create_topition(storage: &impl Storage) -> Result<Topition> {
    let name = alphanumeric_string(15);

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.clone())
                .num_partitions(2)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(Topition::new(name, 0))
}

async fn init_idempotent_producer(storage: &impl Storage) -> Result<ProducerIdResponse> {
    let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?;
    assert_eq!(ErrorCode::None, producer.error, "{producer:?}");
    assert_eq!(0, producer.epoch, "{producer:?}");

    Ok(producer)
}

/// Returns a batch of `records` records from this producer, epoch and first sequence.
fn batch(
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    records: i32,
    transactional: bool,
) -> Result<deflated::Batch> {
    let mut builder = inflated::Batch::builder()
        .attributes(BatchAttribute::default().transaction(transactional).into())
        .producer_id(producer_id)
        .producer_epoch(producer_epoch)
        .base_sequence(base_sequence)
        .last_offset_delta(records - 1);

    for offset_delta in 0..records {
        builder = builder.record(
            Record::builder()
                .offset_delta(offset_delta)
                .value(Bytes::from_static(b"value").into()),
        );
    }

    builder
        .build()
        .and_then(TryInto::try_into)
        .map_err(Into::into)
}

/// Returns the offset after the partition's last record.
async fn high_watermark(storage: &impl Storage, topition: &Topition) -> Result<Option<i64>> {
    let offsets = storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await?;

    assert_eq!(1, offsets.len());
    assert_eq!(ErrorCode::None, offsets[0].1.error_code);

    Ok(offsets[0].1.offset)
}

/// A higher epoch at sequence 0 starts that epoch, and the producer's sequence continues
/// from there.
/// `remembers_batches` is whether the engine remembers the producer's recent batches. Such an
/// engine answers a resent batch with the stored batch's offset, as Kafka does, and the others
/// answer DUPLICATE_SEQUENCE_NUMBER.
async fn bump_at_sequence_zero_is_stored(
    storage: impl Storage,
    remembers_batches: bool,
) -> Result<()> {
    let topition = create_topition(&storage).await?;
    let producer = init_idempotent_producer(&storage).await?;

    assert_eq!(
        0,
        storage
            .produce(None, &topition, batch(producer.id, 0, 0, 2, false)?)
            .await?
    );

    assert_eq!(
        2,
        storage
            .produce(None, &topition, batch(producer.id, 1, 0, 2, false)?)
            .await?
    );

    // The client resends the bump after a lost response, and treats the duplicate as stored.
    let resend = storage
        .produce(None, &topition, batch(producer.id, 1, 0, 2, false)?)
        .await;

    if remembers_batches {
        assert_eq!(2, resend?);
    } else {
        assert_matches!(resend, Err(Error::Api(ErrorCode::DuplicateSequenceNumber)));
    }

    assert_eq!(
        4,
        storage
            .produce(None, &topition, batch(producer.id, 1, 2, 2, false)?)
            .await?
    );

    assert_eq!(Some(6), high_watermark(&storage, &topition).await?);

    Ok(())
}

/// A bump on one partition leaves the previous epoch on another partition, as Kafka keeps
/// the epoch for each partition. The Java client retries a batch at its old epoch there.
async fn previous_epoch_continues_on_other_partition(storage: impl Storage) -> Result<()> {
    let bumped = create_topition(&storage).await?;
    let other = Topition::new(bumped.topic().to_owned(), 1);
    let producer = init_idempotent_producer(&storage).await?;

    for topition in [&bumped, &other] {
        _ = storage
            .produce(None, topition, batch(producer.id, 0, 0, 2, false)?)
            .await?;
    }

    _ = storage
        .produce(None, &bumped, batch(producer.id, 1, 0, 2, false)?)
        .await?;

    assert_eq!(
        2,
        storage
            .produce(None, &other, batch(producer.id, 0, 2, 2, false)?)
            .await?
    );

    assert_eq!(
        4,
        storage
            .produce(None, &other, batch(producer.id, 1, 0, 2, false)?)
            .await?
    );

    assert_matches!(
        storage
            .produce(None, &other, batch(producer.id, 0, 4, 2, false)?)
            .await,
        Err(Error::Api(ErrorCode::InvalidProducerEpoch))
    );

    assert_eq!(Some(6), high_watermark(&storage, &other).await?);

    Ok(())
}

/// After a bump, a batch at the previous epoch is answered INVALID_PRODUCER_EPOCH.
async fn previous_epoch_is_invalid_after_bump(storage: impl Storage) -> Result<()> {
    let topition = create_topition(&storage).await?;
    let producer = init_idempotent_producer(&storage).await?;

    _ = storage
        .produce(None, &topition, batch(producer.id, 0, 0, 2, false)?)
        .await?;

    _ = storage
        .produce(None, &topition, batch(producer.id, 1, 0, 2, false)?)
        .await?;

    assert_matches!(
        storage
            .produce(None, &topition, batch(producer.id, 0, 2, 2, false)?)
            .await,
        Err(Error::Api(ErrorCode::InvalidProducerEpoch))
    );

    assert_eq!(Some(4), high_watermark(&storage, &topition).await?);

    Ok(())
}

/// A higher epoch whose first sequence is not 0 is out of order, and is not stored.
async fn bump_at_other_sequence_is_out_of_order(storage: impl Storage) -> Result<()> {
    let topition = create_topition(&storage).await?;
    let producer = init_idempotent_producer(&storage).await?;

    _ = storage
        .produce(None, &topition, batch(producer.id, 0, 0, 2, false)?)
        .await?;

    assert_matches!(
        storage
            .produce(None, &topition, batch(producer.id, 1, 5, 2, false)?)
            .await,
        Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
    );

    assert_eq!(Some(2), high_watermark(&storage, &topition).await?);

    // The refused batch did not start epoch 1, so epoch 0 continues.
    assert_eq!(
        2,
        storage
            .produce(None, &topition, batch(producer.id, 0, 2, 2, false)?)
            .await?
    );

    Ok(())
}

/// A bump into an exhausted epoch is answered INVALID_PRODUCER_EPOCH and is not stored,
/// because the broker keeps `i16::MAX` to retire a producer ID.
async fn bump_into_exhausted_epoch_is_invalid(storage: impl Storage) -> Result<()> {
    let topition = create_topition(&storage).await?;
    let producer = init_idempotent_producer(&storage).await?;

    assert_eq!(
        0,
        storage
            .produce(
                None,
                &topition,
                batch(producer.id, i16::MAX - 2, 0, 2, false)?
            )
            .await?
    );

    assert_matches!(
        storage
            .produce(
                None,
                &topition,
                batch(producer.id, i16::MAX - 1, 0, 2, false)?
            )
            .await,
        Err(Error::Api(ErrorCode::InvalidProducerEpoch))
    );

    assert_eq!(Some(2), high_watermark(&storage, &topition).await?);

    Ok(())
}

/// A transactional producer's epoch moves only through InitProducerId, so its batch at a
/// higher epoch is answered INVALID_PRODUCER_EPOCH.
async fn transactional_bump_is_invalid(storage: impl Storage) -> Result<()> {
    let topition = create_topition(&storage).await?;
    let transactional_id = Uuid::now_v7().to_string();

    let producer = storage
        .init_producer(Some(&transactional_id), 60_000, Some(-1), Some(-1))
        .await?;
    assert_eq!(ErrorCode::None, producer.error, "{producer:?}");

    assert_matches!(
        storage
            .produce(
                Some(&transactional_id),
                &topition,
                batch(producer.id, producer.epoch + 1, 0, 2, true)?
            )
            .await,
        Err(Error::Api(ErrorCode::InvalidProducerEpoch))
    );

    assert_eq!(Some(0), high_watermark(&storage, &topition).await?);

    Ok(())
}

/// A non-transactional batch that names a transactional producer's ID cannot move that
/// producer's epoch, so its transactional ID can still bump the epoch.
async fn non_transactional_batch_cannot_bump_transactional_producer(
    storage: impl Storage,
) -> Result<()> {
    let topition = create_topition(&storage).await?;
    let transactional_id = Uuid::now_v7().to_string();

    let producer = storage
        .init_producer(Some(&transactional_id), 60_000, Some(-1), Some(-1))
        .await?;
    assert_eq!(ErrorCode::None, producer.error, "{producer:?}");

    assert_matches!(
        storage
            .produce(
                None,
                &topition,
                batch(producer.id, producer.epoch + 1, 0, 2, false)?
            )
            .await,
        Err(Error::Api(ErrorCode::InvalidProducerEpoch))
    );

    assert_eq!(Some(0), high_watermark(&storage, &topition).await?);

    let bumped = storage
        .init_producer(
            Some(&transactional_id),
            60_000,
            Some(producer.id),
            Some(producer.epoch),
        )
        .await?;
    assert_eq!(ErrorCode::None, bumped.error, "{bumped:?}");
    assert_eq!(producer.id, bumped.id, "{bumped:?}");
    assert_eq!(producer.epoch + 1, bumped.epoch, "{bumped:?}");

    Ok(())
}

async fn registered(
    storage: ArcDynStorage,
    cluster_id: Uuid,
    broker_id: i32,
) -> Result<ArcDynStorage> {
    register_broker(cluster_id, broker_id, &storage).await?;
    Ok(storage)
}

macro_rules! engine_tests {
    ($engine:ident, $storage:path, remembers_batches: $remembers_batches:expr) => {
        mod $engine {
            use super::*;

            async fn registered_storage() -> Result<ArcDynStorage> {
                let cluster_id = Uuid::now_v7();
                let broker_id = rng().random_range(0..i32::MAX);

                registered(
                    $storage(cluster_id, broker_id).await?,
                    cluster_id,
                    broker_id,
                )
                .await
            }

            #[tokio::test]
            async fn bump_at_sequence_zero_is_stored() -> Result<()> {
                let _guard = init_tracing()?;
                super::bump_at_sequence_zero_is_stored(
                    registered_storage().await?,
                    $remembers_batches,
                )
                .await
            }

            #[tokio::test]
            async fn previous_epoch_is_invalid_after_bump() -> Result<()> {
                let _guard = init_tracing()?;
                super::previous_epoch_is_invalid_after_bump(registered_storage().await?).await
            }

            #[tokio::test]
            async fn bump_at_other_sequence_is_out_of_order() -> Result<()> {
                let _guard = init_tracing()?;
                super::bump_at_other_sequence_is_out_of_order(registered_storage().await?).await
            }

            #[tokio::test]
            async fn bump_into_exhausted_epoch_is_invalid() -> Result<()> {
                let _guard = init_tracing()?;
                super::bump_into_exhausted_epoch_is_invalid(registered_storage().await?).await
            }

            #[tokio::test]
            async fn transactional_bump_is_invalid() -> Result<()> {
                let _guard = init_tracing()?;
                super::transactional_bump_is_invalid(registered_storage().await?).await
            }

            #[tokio::test]
            async fn previous_epoch_continues_on_other_partition() -> Result<()> {
                let _guard = init_tracing()?;
                super::previous_epoch_continues_on_other_partition(registered_storage().await?)
                    .await
            }

            #[tokio::test]
            async fn non_transactional_batch_cannot_bump_transactional_producer() -> Result<()> {
                let _guard = init_tracing()?;
                super::non_transactional_batch_cannot_bump_transactional_producer(
                    registered_storage().await?,
                )
                .await
            }
        }
    };
}

#[cfg(feature = "dynostore")]
engine_tests!(in_memory, memory_storage, remembers_batches: true);

#[cfg(feature = "libsql")]
engine_tests!(lite, lite_storage, remembers_batches: false);

#[cfg(feature = "postgres")]
engine_tests!(pg, postgres_storage, remembers_batches: false);
