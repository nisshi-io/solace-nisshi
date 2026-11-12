// Copyright (c) Solace Corporation
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

//! A producer ID that the broker allocated but no longer holds is recreated from the producer's
//! next batch, which may start at any sequence, as in Kafka.

use std::assert_matches;

use bytes::Bytes;
use nisshi_sans_io::{
    ErrorCode,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{Error, Storage as _, Topition};
use object_store::memory::InMemory;

use crate::dynostore::{DynoStore, IdempotentBatch, tests::init_tracing};

fn storage() -> Result<DynoStore, Error> {
    DynoStore::new("nisshi", 111, InMemory::new())
}

fn idempotent_batch(producer_id: i64, base_sequence: i32) -> Result<deflated::Batch, Error> {
    idempotent_batch_at(producer_id, 0, base_sequence)
}

fn idempotent_batch_at(
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
) -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .producer_id(producer_id)
        .producer_epoch(producer_epoch)
        .base_sequence(base_sequence)
        .record(Record::builder().value(Bytes::from_static(b"value").into()))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

async fn create_topic(storage: &DynoStore, name: &str) -> Result<(), Error> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(())
}

/// Allocates `count` idempotent producers, 1 to `count`.
async fn init_producers(storage: &DynoStore, count: i64) -> Result<(), Error> {
    for id in 1..=count {
        assert_eq!(
            id,
            storage.init_producer(None, 0, Some(-1), Some(-1)).await?.id
        );
    }

    Ok(())
}

/// Drops the producer's state, as a broker that no longer holds it does.
async fn forget_producer(storage: &DynoStore, producer: i64) -> Result<bool, Error> {
    storage
        .meta
        .with_mut(&storage.object_store, |meta| {
            Ok(meta.forget_producer(producer))
        })
        .await
}

async fn producers(storage: &DynoStore) -> Result<Vec<i64>, Error> {
    storage
        .meta
        .with(&storage.object_store, |meta| {
            Ok(meta.producers.keys().copied().collect())
        })
        .await
}

#[tokio::test]
async fn a_dropped_producer_starts_again_and_a_never_allocated_id_is_unknown() -> Result<(), Error>
{
    let _guard = init_tracing()?;
    let storage = storage()?;

    create_topic(&storage, "abc").await?;
    init_producers(&storage, 2).await?;

    let topition = Topition::new("abc", 0);
    assert_eq!(
        0,
        storage
            .produce(None, &topition, idempotent_batch(1, 0)?)
            .await?
    );

    assert!(forget_producer(&storage, 1).await?);
    assert_eq!(vec![2], producers(&storage).await?);

    // A Java client that is answered UNKNOWN_PRODUCER_ID retries with the same producer ID and
    // never recovers, so the broker recreates the producer that it allocated.
    assert_eq!(
        1,
        storage
            .produce(None, &topition, idempotent_batch(1, 1)?)
            .await?
    );
    assert_eq!(vec![1, 2], producers(&storage).await?);

    // No producer was ever given 3, so a client cannot fill meta.json with IDs of its own.
    assert_matches!(
        storage
            .produce(None, &topition, idempotent_batch(3, 0)?)
            .await,
        Err(Error::Api(ErrorCode::UnknownProducerId))
    );

    // Only -1 means a batch has no producer, so 0 and the IDs below it reach the check. They are
    // below the newest ID, and without the check that an ID is positive each would create an
    // entry in meta.json under an ID that the client chose.
    for producer_id in [0, -7] {
        assert_matches!(
            storage
                .produce(None, &topition, idempotent_batch(producer_id, 0)?)
                .await,
            Err(Error::Api(ErrorCode::UnknownProducerId))
        );
    }
    assert_eq!(vec![1, 2], producers(&storage).await?);

    Ok(())
}

#[tokio::test]
async fn the_newest_producer_id_is_not_forgotten() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;

    init_producers(&storage, 2).await?;

    assert!(!forget_producer(&storage, 2).await?);
    assert!(forget_producer(&storage, 1).await?);

    // The next ID follows the newest, so it does not reuse an ID with an epoch that a partition
    // still holds.
    assert_eq!(
        3,
        storage.init_producer(None, 0, Some(-1), Some(-1)).await?.id
    );

    Ok(())
}

#[tokio::test]
async fn a_dropped_producer_resumes_at_its_next_sequence() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;

    create_topic(&storage, "abc").await?;
    init_producers(&storage, 2).await?;

    let topition = Topition::new("abc", 0);
    assert_eq!(
        0,
        storage
            .produce(None, &topition, idempotent_batch(1, 0)?)
            .await?
    );
    assert_eq!(
        1,
        storage
            .produce(None, &topition, idempotent_batch(1, 1)?)
            .await?
    );

    assert!(forget_producer(&storage, 1).await?);

    // The client kept its sequence, so its next batch starts at 2. Kafka accepts that batch,
    // and a librdkafka client treats a refusal as fatal.
    assert_eq!(
        2,
        storage
            .produce(None, &topition, idempotent_batch(1, 2)?)
            .await?
    );
    assert_eq!(
        3,
        storage
            .produce(None, &topition, idempotent_batch(1, 3)?)
            .await?
    );

    // A batch that repeats the last one is a duplicate, and Kafka answers with the stored
    // offset.
    assert_eq!(
        3,
        storage
            .produce(None, &topition, idempotent_batch(1, 3)?)
            .await?
    );

    // A batch that skips a sequence is out of order again, because the partition now holds state.
    assert_matches!(
        storage
            .produce(None, &topition, idempotent_batch(1, 9)?)
            .await,
        Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
    );

    Ok(())
}

#[tokio::test]
async fn a_dropped_producer_resumes_at_the_largest_sequence_and_wraps_to_0() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;

    create_topic(&storage, "abc").await?;
    init_producers(&storage, 2).await?;

    let topition = Topition::new("abc", 0);
    assert_eq!(
        0,
        storage
            .produce(None, &topition, idempotent_batch(1, 0)?)
            .await?
    );

    assert!(forget_producer(&storage, 1).await?);

    // A client chooses the sequence of a resumed batch, and the sum with the batch's record count
    // is stored. That sum overflows an i32, which panics in a build with overflow checks while
    // the lock on meta.json is held.
    assert_eq!(
        1,
        storage
            .produce(None, &topition, idempotent_batch(1, i32::MAX)?)
            .await?
    );

    // The Java client wraps its sequence to 0 after i32::MAX, so its next batch starts there.
    assert_eq!(
        2,
        storage
            .produce(None, &topition, idempotent_batch(1, 0)?)
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn a_dropped_producer_resumes_only_at_a_sequence_that_is_not_negative() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;

    create_topic(&storage, "abc").await?;
    init_producers(&storage, 2).await?;

    let topition = Topition::new("abc", 0);
    assert_eq!(
        0,
        storage
            .produce(None, &topition, idempotent_batch(1, 0)?)
            .await?
    );

    assert!(forget_producer(&storage, 1).await?);

    // Kafka's LogValidator refuses any negative first sequence, also for a producer that the
    // broker lost state for. The check before the write refuses it, so the producer is not
    // recreated and the log gets no batch.
    assert_matches!(
        storage
            .produce(None, &topition, idempotent_batch(1, -100)?)
            .await,
        Err(Error::Api(ErrorCode::InvalidRecord))
    );
    assert_eq!(vec![2], producers(&storage).await?);

    assert_eq!(
        1,
        storage
            .produce(None, &topition, idempotent_batch(1, 5)?)
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn a_dropped_producer_resumes_on_each_partition_it_wrote() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;

    create_topic(&storage, "abc").await?;
    create_topic(&storage, "other").await?;
    init_producers(&storage, 2).await?;

    let abc = Topition::new("abc", 0);
    let other = Topition::new("other", 0);
    for sequence in 0..2 {
        _ = storage
            .produce(None, &abc, idempotent_batch(1, sequence)?)
            .await?;
    }
    for sequence in 0..5 {
        _ = storage
            .produce(None, &other, idempotent_batch(1, sequence)?)
            .await?;
    }

    assert!(forget_producer(&storage, 1).await?);

    // The broker holds no state for either partition, and the producer resumes on each at the
    // sequence that it had there.
    assert_matches!(
        storage.produce(None, &abc, idempotent_batch(1, 2)?).await,
        Ok(_)
    );
    assert_matches!(
        storage.produce(None, &other, idempotent_batch(1, 5)?).await,
        Ok(_)
    );

    Ok(())
}

#[tokio::test]
async fn a_dropped_producer_resumes_at_its_epoch_and_the_epoch_fence_holds() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;

    create_topic(&storage, "abc").await?;
    init_producers(&storage, 2).await?;

    // The producer bumped its epoch twice before the broker dropped it. The partition's
    // watermark holds epoch 2 for it, and the producer's own state is gone.
    let topition = Topition::new("abc", 0);
    for sequence in 0..2 {
        _ = storage
            .produce(None, &topition, idempotent_batch_at(1, 2, sequence)?)
            .await?;
    }

    assert!(forget_producer(&storage, 1).await?);

    assert_matches!(
        storage
            .produce(None, &topition, idempotent_batch_at(1, 2, 2)?)
            .await,
        Ok(_)
    );

    // A batch from an older epoch is a zombie's.
    assert_matches!(
        storage
            .produce(None, &topition, idempotent_batch_at(1, 1, 3)?)
            .await,
        Err(Error::Api(ErrorCode::InvalidProducerEpoch))
    );

    // The next epoch starts at sequence 0, and the epoch that it replaces is then a zombie's.
    assert_matches!(
        storage
            .produce(None, &topition, idempotent_batch_at(1, 3, 0)?)
            .await,
        Ok(_)
    );
    assert_matches!(
        storage
            .produce(None, &topition, idempotent_batch_at(1, 2, 3)?)
            .await,
        Err(Error::Api(ErrorCode::InvalidProducerEpoch))
    );

    Ok(())
}

#[tokio::test]
async fn dropping_a_producer_between_the_check_and_the_advance_does_not_fail_a_stored_batch()
-> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;

    create_topic(&storage, "abc").await?;
    init_producers(&storage, 2).await?;

    let topition = Topition::new("abc", 0);
    for sequence in 0..7 {
        _ = storage
            .produce(None, &topition, idempotent_batch(1, sequence)?)
            .await?;
    }

    let batch = IdempotentBatch {
        producer_id: 1,
        producer_epoch: 0,
        base_sequence: 7,
        last_offset_delta: 0,
    };

    assert_matches!(
        storage
            .check_idempotent_sequence(None, &topition, &batch, false)
            .await,
        Ok(_)
    );

    assert!(forget_producer(&storage, 1).await?);

    // The batch is stored, so its advance must not answer an error, also at a sequence above 0.
    storage
        .advance_idempotent_sequence(None, &topition, batch, false, 7, None)
        .await?;

    storage
        .meta
        .with(&storage.object_store, |meta| {
            let producer = meta.producers.get(&1).expect("producer");
            assert_eq!(8, producer.next_sequence(0, &topition));
            Ok(())
        })
        .await?;

    Ok(())
}

#[tokio::test]
async fn a_recreated_producer_that_the_advance_refuses_is_not_kept() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;

    create_topic(&storage, "abc").await?;
    init_producers(&storage, 2).await?;

    assert!(forget_producer(&storage, 1).await?);
    assert_eq!(vec![2], producers(&storage).await?);

    // An epoch that is exhausted is refused, so the producer that the advance recreated for the
    // batch must go again. with_mut stores the closure's changes also when it answers an error.
    let batch = IdempotentBatch {
        producer_id: 1,
        producer_epoch: i16::MAX - 1,
        base_sequence: 0,
        last_offset_delta: 0,
    };

    assert_matches!(
        storage
            .advance_idempotent_sequence(None, &Topition::new("abc", 0), batch, false, 0, None)
            .await,
        Err(Error::Api(ErrorCode::InvalidProducerEpoch))
    );
    assert_eq!(vec![2], producers(&storage).await?);

    Ok(())
}
