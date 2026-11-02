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

//! An idempotent producer bumps its epoch and restarts its sequence at 0 after an error
//! ([KIP-360]), while a batch at its older epoch can still be inside the broker. A batch that
//! gets its offset after the bump must be refused and not stored. A batch that got its offset
//! before the bump is in the log ahead of the new epoch's batches, so it must be acknowledged,
//! as Kafka does.
//!
//! [KIP-360]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-360%3A+Improve+reliability+of+idempotent%2Ftransactional+producer

use std::{assert_matches, time::Duration};

use bytes::Bytes;
use nisshi_sans_io::{
    BatchAttribute, ErrorCode, IsolationLevel,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    record::{Record, deflated, inflated},
    topic_config,
};
use nisshi_schema::lake::House;
use nisshi_storage::{Error, OLDER_EPOCH_ERROR, Storage as _, Topition, TxnAddPartitionsRequest};
use object_store::{ObjectStoreExt as _, PutPayload};

use crate::dynostore::{
    DynoStore, partition_prefix,
    tests::{
        held_put_store::{self, HeldPutStore},
        init_tracing,
    },
};

const TRANSACTION_ID: &str = "txn";
const TIMEOUT_MS: i32 = 10_000;

fn idempotent_batch(
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

fn transactional_batch(
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
) -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .attributes(BatchAttribute::default().transaction(true).into())
        .producer_id(producer_id)
        .producer_epoch(producer_epoch)
        .base_sequence(base_sequence)
        .record(Record::builder().value(Bytes::from_static(b"value").into()))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// Removes the `producer_epochs` key from the partition's watermark document, as a broker
/// without the key does when it writes the document.
async fn drop_producer_epochs(
    object_store: &HeldPutStore,
    topition: &Topition,
) -> Result<(), Error> {
    let location = partition_prefix("nisshi", topition).join("watermark.json");

    let mut watermark: serde_json::Value =
        serde_json::from_slice(&object_store.get(&location).await?.bytes().await?)?;

    _ = watermark
        .as_object_mut()
        .and_then(|watermark| watermark.remove("producer_epochs"))
        .ok_or(Error::Message(String::from("no producer_epochs to drop")))?;

    _ = object_store
        .put(&location, PutPayload::from(serde_json::to_vec(&watermark)?))
        .await?;

    Ok(())
}

async fn create_topic(storage: &DynoStore) -> Result<Topition, Error> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name("abc".into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(Topition::new("abc", 0))
}

/// Returns the base offset and producer epoch of each stored batch, in offset order.
///
/// A fetch can return fewer batches than the partition holds, so this fetches again from the
/// offset after the last batch until a fetch returns none.
async fn stored_batches(
    storage: &DynoStore,
    topition: &Topition,
) -> Result<Vec<(i64, i16)>, Error> {
    let mut stored = vec![];
    let mut offset = 0;

    loop {
        let batches = storage
            .fetch(
                topition,
                offset,
                1,
                u32::MAX,
                IsolationLevel::ReadUncommitted,
                Duration::ZERO,
            )
            .await?;

        let Some(last) = batches.last() else {
            return Ok(stored);
        };

        offset = last.base_offset + i64::from(last.last_offset_delta) + 1;
        stored.extend(
            batches
                .iter()
                .map(|batch| (batch.base_offset, batch.producer_epoch)),
        );
    }
}

#[tokio::test]
async fn older_epoch_batch_held_at_watermark_put_is_refused_after_bump() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = HeldPutStore::new(held_put_store::is_watermark);
    let storage = DynoStore::new("nisshi", 111, object_store.clone())?;
    let topition = create_topic(&storage).await?;
    let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?;

    assert_eq!(
        0,
        storage
            .produce(None, &topition, idempotent_batch(producer.id, 0, 0)?)
            .await?
    );

    // The epoch 0 batch passes the sequence check, and then waits before its watermark write
    // gives it an offset.
    let watermark_write = object_store.hold_next_put();
    let older_epoch = storage.produce(None, &topition, idempotent_batch(producer.id, 0, 1)?);

    let bump = async {
        watermark_write.wait_until_held().await?;

        let bumped = storage
            .produce(None, &topition, idempotent_batch(producer.id, 1, 0)?)
            .await;

        watermark_write.release();
        bumped
    };

    let (older_epoch, bumped) = tokio::join!(older_epoch, bump);

    assert_eq!(1, bumped?);
    assert_matches!(older_epoch, Err(Error::Api(error)) if error == OLDER_EPOCH_ERROR);

    // The refused batch left no gap: the next batch gets the offset after the last stored one.
    assert_eq!(
        2,
        storage
            .produce(None, &topition, idempotent_batch(producer.id, 1, 1)?)
            .await?
    );

    assert_eq!(
        vec![(0, 0), (1, 1), (2, 1)],
        stored_batches(&storage, &topition).await?
    );

    Ok(())
}

/// When the watermark no longer holds the producer's epoch, as after a broker without the
/// fence wrote it, an older-epoch batch gets an offset after the bump and is stored. The
/// advance after the record write still refuses it, because the batch is behind the newer
/// epoch's first batch.
#[tokio::test]
async fn older_epoch_batch_stored_after_bump_without_fence_is_refused() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = HeldPutStore::new(held_put_store::is_watermark);
    let storage = DynoStore::new("nisshi", 111, object_store.clone())?;
    let topition = create_topic(&storage).await?;
    let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?;

    assert_eq!(
        0,
        storage
            .produce(None, &topition, idempotent_batch(producer.id, 0, 0)?)
            .await?
    );

    let watermark_write = object_store.hold_next_put();
    let older_epoch = storage.produce(None, &topition, idempotent_batch(producer.id, 0, 1)?);

    let bump = async {
        watermark_write.wait_until_held().await?;

        let bumped = storage
            .produce(None, &topition, idempotent_batch(producer.id, 1, 0)?)
            .await?;

        drop_producer_epochs(&object_store, &topition).await?;

        watermark_write.release();
        Ok::<_, Error>(bumped)
    };

    let (older_epoch, bumped) = tokio::join!(older_epoch, bump);

    assert_eq!(1, bumped?);
    assert_matches!(older_epoch, Err(Error::Api(error)) if error == OLDER_EPOCH_ERROR);

    // The batch is stored behind the newer epoch's batch, as before the fence existed.
    assert_eq!(
        vec![(0, 0), (1, 1), (2, 0)],
        stored_batches(&storage, &topition).await?
    );

    Ok(())
}

/// A transactional producer's epoch moves only through InitProducerId, so a batch at its
/// previous epoch is refused after its record write, also when it got its offset before the
/// bump.
#[tokio::test]
async fn transactional_batch_held_at_record_put_before_bump_is_refused() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = HeldPutStore::new(held_put_store::is_batch);
    let storage = DynoStore::new("nisshi", 111, object_store.clone())?;
    let topition = create_topic(&storage).await?;
    let producer = storage
        .init_producer(Some(TRANSACTION_ID), TIMEOUT_MS, None, None)
        .await?;

    _ = storage
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: TRANSACTION_ID.into(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: vec![
                AddPartitionsToTxnTopic::default()
                    .name(topition.topic().to_owned())
                    .partitions(Some(vec![topition.partition()])),
            ],
        })
        .await?;

    assert_eq!(
        0,
        storage
            .produce(
                Some(TRANSACTION_ID),
                &topition,
                transactional_batch(producer.id, producer.epoch, 0)?
            )
            .await?
    );

    let record_write = object_store.hold_next_put();
    let older_epoch = storage.produce(
        Some(TRANSACTION_ID),
        &topition,
        transactional_batch(producer.id, producer.epoch, 1)?,
    );

    let bump = async {
        record_write.wait_until_held().await?;

        let bumped = storage
            .init_producer(
                Some(TRANSACTION_ID),
                TIMEOUT_MS,
                Some(producer.id),
                Some(producer.epoch),
            )
            .await;

        record_write.release();
        bumped
    };

    let (older_epoch, bumped) = tokio::join!(older_epoch, bump);

    let bumped = bumped?;
    assert_eq!(
        (ErrorCode::None, producer.id, producer.epoch + 1),
        (bumped.error, bumped.id, bumped.epoch)
    );
    assert_matches!(
        older_epoch,
        Err(Error::Api(ErrorCode::InvalidProducerEpoch))
    );

    Ok(())
}

#[tokio::test]
async fn batch_held_at_record_put_before_bump_is_acknowledged_at_its_offset() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = HeldPutStore::new(held_put_store::is_batch);
    let storage = DynoStore::new("nisshi", 111, object_store.clone())?;
    let topition = create_topic(&storage).await?;
    let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?;

    assert_eq!(
        0,
        storage
            .produce(None, &topition, idempotent_batch(producer.id, 0, 0)?)
            .await?
    );

    // The epoch 0 batch gets offset 1, and then waits before its record write, so the bumped
    // epoch's batch advances the partition's epoch before the epoch 0 batch advances its
    // sequence.
    let record_write = object_store.hold_next_put();
    let older_epoch = storage.produce(None, &topition, idempotent_batch(producer.id, 0, 1)?);

    let bump = async {
        record_write.wait_until_held().await?;

        let bumped = storage
            .produce(None, &topition, idempotent_batch(producer.id, 1, 0)?)
            .await;

        record_write.release();
        bumped
    };

    let (older_epoch, bumped) = tokio::join!(older_epoch, bump);

    assert_eq!(2, bumped?);
    assert_eq!(1, older_epoch?);

    // The acknowledged epoch 0 batch left the bumped epoch's sequence where it was.
    assert_eq!(
        3,
        storage
            .produce(None, &topition, idempotent_batch(producer.id, 1, 1)?)
            .await?
    );

    assert_eq!(
        vec![(0, 0), (1, 0), (2, 1), (3, 1)],
        stored_batches(&storage, &topition).await?
    );

    Ok(())
}

/// Returns a store with a lake and one lake-sink topic, `abc`, with one partition.
async fn lake_sink_storage(object_store: HeldPutStore) -> Result<(DynoStore, Topition), Error> {
    // `House::None` sends the produce down the lake-sink path, and without a schema registry
    // that path writes nothing to the lake, so the test needs no lake service.
    let storage = DynoStore::new("nisshi", 111, object_store)?.lake(Some(House::None));

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name("abc".into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some(
                    [CreatableTopicConfig::default()
                        .name(topic_config::LAKE_SINK.into())
                        .value(Some("true".into()))]
                    .into(),
                )),
            false,
        )
        .await?;

    Ok((storage, Topition::new("abc", 0)))
}

#[tokio::test]
async fn lake_sink_older_epoch_batch_held_at_watermark_put_is_refused_after_bump()
-> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = HeldPutStore::new(held_put_store::is_watermark);
    let (storage, topition) = lake_sink_storage(object_store.clone()).await?;
    let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?;

    assert_eq!(
        0,
        storage
            .produce(None, &topition, idempotent_batch(producer.id, 0, 0)?)
            .await?
    );

    let watermark_write = object_store.hold_next_put();
    let older_epoch = storage.produce(None, &topition, idempotent_batch(producer.id, 0, 1)?);

    let bump = async {
        watermark_write.wait_until_held().await?;

        let bumped = storage
            .produce(None, &topition, idempotent_batch(producer.id, 1, 0)?)
            .await;

        watermark_write.release();
        bumped
    };

    let (older_epoch, bumped) = tokio::join!(older_epoch, bump);

    assert_eq!(1, bumped?);
    assert_matches!(older_epoch, Err(Error::Api(error)) if error == OLDER_EPOCH_ERROR);

    // The refused batch took no offset.
    assert_eq!(2, storage.offset_stage(&topition).await?.high_watermark);

    Ok(())
}

/// A lake-sink partition with no stored sequence for the producer, as after an upgrade from a
/// broker that kept none for it, gets the answers of the sequence check alone: the fence holds
/// nothing for the producer, and nothing records where an epoch started.
#[tokio::test]
async fn lake_sink_partition_without_producer_state_answers_as_the_sequence_check()
-> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, topition) =
        lake_sink_storage(HeldPutStore::new(held_put_store::is_watermark)).await?;
    let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?;

    // The producer continues a sequence that the partition never stored.
    assert_matches!(
        storage
            .produce(None, &topition, idempotent_batch(producer.id, 0, 5)?)
            .await,
        Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
    );

    // The producer bumps its epoch and restarts at sequence 0.
    assert_eq!(
        0,
        storage
            .produce(None, &topition, idempotent_batch(producer.id, 1, 0)?)
            .await?
    );

    // A batch at the older epoch is now refused.
    assert_matches!(
        storage
            .produce(None, &topition, idempotent_batch(producer.id, 0, 6)?)
            .await,
        Err(Error::Api(error)) if error == OLDER_EPOCH_ERROR
    );

    Ok(())
}
