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

//! InitProducerId on a transactional ID whose epoch is exhausted moves it to a
//! new producer ID at epoch 0, and fences the previous producer ID. The tests
//! seed the stored epoch near `i16::MAX` instead of sending 32k requests.

use std::{assert_matches, collections::BTreeMap};

use bytes::Bytes;
use nisshi_sans_io::{
    BatchAttribute, ErrorCode,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{
    Error, ProducerIdResponse, Storage as _, Topition, TxnAddPartitionsRequest, TxnState,
};
use object_store::memory::InMemory;

use crate::dynostore::{DynoStore, Meta, ProducerDetail, Txn, TxnDetail, tests::init_tracing};

const TRANSACTION_ID: &str = "txn";
const TIMEOUT_MS: i32 = 10_000;
const PRODUCER_ID: i64 = 1;

fn storage() -> Result<DynoStore, Error> {
    DynoStore::new("nisshi", 111, InMemory::new())
}

/// Stores `TRANSACTION_ID` with `producer` at the given epochs, each with its
/// transaction state, and gives `producer` the same epochs.
async fn seed(
    storage: &DynoStore,
    producer: i64,
    epochs: &[(i16, Option<TxnState>)],
) -> Result<(), Error> {
    storage
        .meta
        .with_mut(&storage.object_store, |meta| {
            _ = meta.producers.insert(
                producer,
                ProducerDetail {
                    sequences: epochs
                        .iter()
                        .map(|(epoch, _)| (*epoch, BTreeMap::new()))
                        .collect(),
                    batches: BTreeMap::new(),
                    epoch_starts: BTreeMap::new(),
                },
            );

            _ = meta.transactions.insert(
                TRANSACTION_ID.into(),
                Txn {
                    producer,
                    epochs: epochs
                        .iter()
                        .map(|(epoch, state)| {
                            (
                                *epoch,
                                TxnDetail {
                                    transaction_timeout_ms: TIMEOUT_MS,
                                    state: *state,
                                    ..Default::default()
                                },
                            )
                        })
                        .collect(),
                },
            );

            Ok(())
        })
        .await
}

async fn init_producer(
    storage: &DynoStore,
    claim: Option<(i64, i16)>,
) -> Result<ProducerIdResponse, Error> {
    storage
        .init_producer(
            Some(TRANSACTION_ID),
            TIMEOUT_MS,
            claim.map(|(id, _)| id),
            claim.map(|(_, epoch)| epoch),
        )
        .await
}

fn granted(id: i64, epoch: i16) -> ProducerIdResponse {
    ProducerIdResponse {
        error: ErrorCode::None,
        id,
        epoch,
    }
}

async fn create_topic(storage: &DynoStore, topic: &str) -> Result<(), Error> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(())
}

fn idempotent_batch(producer_id: i64, producer_epoch: i16) -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"abc"))))
        .producer_id(producer_id)
        .producer_epoch(producer_epoch)
        .base_sequence(0)
        .build()
        .and_then(TryInto::try_into)
        .map_err(Into::into)
}

fn transactional_batch(producer_id: i64, producer_epoch: i16) -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"abc"))))
        .attributes(BatchAttribute::default().transaction(true).into())
        .producer_id(producer_id)
        .producer_epoch(producer_epoch)
        .base_sequence(0)
        .build()
        .and_then(TryInto::try_into)
        .map_err(Into::into)
}

#[tokio::test]
async fn epoch_below_the_limit_bumps() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    seed(&storage, PRODUCER_ID, &[(32765, None)]).await?;

    assert_eq!(
        granted(PRODUCER_ID, 32766),
        init_producer(&storage, None).await?
    );

    Ok(())
}

#[tokio::test]
async fn exhausted_epoch_moves_a_fresh_request_to_a_new_producer_id() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    seed(&storage, PRODUCER_ID, &[(32766, Some(TxnState::Committed))]).await?;

    assert_eq!(
        granted(PRODUCER_ID + 1, 0),
        init_producer(&storage, None).await?
    );
    assert_eq!(
        granted(PRODUCER_ID + 1, 1),
        init_producer(&storage, Some((PRODUCER_ID + 1, 0))).await?
    );

    Ok(())
}

#[tokio::test]
async fn exhausted_epoch_moves_a_claim_to_a_new_producer_id() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    seed(&storage, PRODUCER_ID, &[(32766, None)]).await?;

    assert_eq!(
        granted(PRODUCER_ID + 1, 0),
        init_producer(&storage, Some((PRODUCER_ID, 32766))).await?
    );

    Ok(())
}

#[tokio::test]
async fn claim_of_the_previous_producer_id_is_fenced() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    seed(&storage, PRODUCER_ID, &[(32766, None)]).await?;

    assert_eq!(
        granted(PRODUCER_ID + 1, 0),
        init_producer(&storage, Some((PRODUCER_ID, 32766))).await?
    );
    assert_eq!(
        ProducerIdResponse::failed(ErrorCode::ProducerFenced),
        init_producer(&storage, Some((PRODUCER_ID, 32766))).await?
    );

    Ok(())
}

#[tokio::test]
async fn previous_producer_id_cannot_produce() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topition = Topition::new("rotated", 0);
    create_topic(&storage, topition.topic()).await?;
    seed(&storage, PRODUCER_ID, &[(32766, None)]).await?;

    assert_eq!(
        granted(PRODUCER_ID + 1, 0),
        init_producer(&storage, None).await?
    );

    assert_matches!(
        storage
            .produce(None, &topition, idempotent_batch(PRODUCER_ID, 32766)?)
            .await,
        Err(Error::Api(ErrorCode::ProducerFenced))
    );
    assert_eq!(
        0,
        storage
            .produce(None, &topition, idempotent_batch(PRODUCER_ID + 1, 0)?)
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn exhausted_epoch_with_a_prepared_transaction_is_concurrent() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    seed(
        &storage,
        PRODUCER_ID,
        &[
            (32765, Some(TxnState::Committed)),
            (32766, Some(TxnState::PrepareAbort)),
        ],
    )
    .await?;

    assert_eq!(
        ProducerIdResponse::failed(ErrorCode::ConcurrentTransactions),
        init_producer(&storage, None).await?
    );

    Ok(())
}

/// The stored metadata, to show that an InitProducerId that answers an error
/// changes nothing.
async fn snapshot(storage: &DynoStore) -> Result<Meta, Error> {
    storage
        .meta
        .with(&storage.object_store, |meta| Ok(meta.clone()))
        .await
}

/// A producer that already holds the epoch that a bump would give it answers
/// an error, changes nothing, and leaves the broker able to serve the next
/// request.
#[tokio::test]
async fn bump_onto_an_epoch_the_producer_holds_is_an_error() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    seed(&storage, PRODUCER_ID, &[(100, None)]).await?;

    storage
        .meta
        .with_mut(&storage.object_store, |meta| {
            if let Some(producer) = meta.producers.get_mut(&PRODUCER_ID) {
                _ = producer.sequences.insert(101, BTreeMap::new());
            }
            Ok(())
        })
        .await?;

    let before = snapshot(&storage).await?;

    assert_eq!(
        ProducerIdResponse::failed(ErrorCode::UnknownServerError),
        init_producer(&storage, None).await?
    );
    assert_eq!(before, snapshot(&storage).await?);

    _ = storage.metadata(None).await?;
    assert_eq!(
        ProducerIdResponse::failed(ErrorCode::ProducerFenced),
        init_producer(&storage, Some((PRODUCER_ID, 0))).await?
    );

    Ok(())
}

/// With no producer ID left to allocate, InitProducerId answers an error for
/// a new transactional ID, an exhausted one and an idempotent producer,
/// changes nothing, and leaves the broker able to serve the next request.
#[tokio::test]
async fn no_producer_id_left_is_an_error() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    seed(&storage, i64::MAX, &[(32766, None)]).await?;

    let before = snapshot(&storage).await?;

    assert_eq!(
        ProducerIdResponse::failed(ErrorCode::UnknownServerError),
        init_producer(&storage, None).await?
    );
    assert_eq!(
        ProducerIdResponse::failed(ErrorCode::UnknownServerError),
        storage
            .init_producer(Some("another"), TIMEOUT_MS, None, None)
            .await?
    );
    assert_eq!(
        ProducerIdResponse::failed(ErrorCode::UnknownServerError),
        storage.init_producer(None, TIMEOUT_MS, None, None).await?
    );
    assert_eq!(before, snapshot(&storage).await?);

    _ = storage.metadata(None).await?;
    assert_eq!(
        ProducerIdResponse::failed(ErrorCode::ProducerFenced),
        init_producer(&storage, Some((i64::MAX, 0))).await?
    );

    Ok(())
}

/// Only the transaction of the current epoch holds back the move, as in Kafka.
/// A transaction left prepared on an older epoch does not.
#[tokio::test]
async fn older_prepared_transaction_does_not_hold_back_the_move() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    seed(
        &storage,
        PRODUCER_ID,
        &[
            (100, Some(TxnState::PrepareAbort)),
            (32766, Some(TxnState::Committed)),
        ],
    )
    .await?;

    assert_eq!(
        granted(PRODUCER_ID + 1, 0),
        init_producer(&storage, None).await?
    );

    Ok(())
}

/// A move keeps only the fencing epoch of the previous producer ID, instead of
/// an entry for each of its epochs.
#[tokio::test]
async fn move_keeps_only_the_fencing_epoch() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    seed(&storage, PRODUCER_ID, &[(32765, None), (32766, None)]).await?;

    assert_eq!(
        granted(PRODUCER_ID + 1, 0),
        init_producer(&storage, None).await?
    );

    let meta = snapshot(&storage).await?;
    assert_eq!(
        Some(vec![i16::MAX]),
        meta.producers.get(&PRODUCER_ID).map(|producer| producer
            .sequences
            .keys()
            .copied()
            .collect::<Vec<_>>())
    );

    Ok(())
}

/// A producer that restarts in the middle of a transaction at the exhausted
/// epoch: InitProducerId aborts the transaction, then moves to a new producer
/// ID.
#[tokio::test]
async fn open_transaction_at_the_exhausted_epoch_is_aborted_then_moved() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    seed(&storage, PRODUCER_ID, &[(32766, Some(TxnState::Begin))]).await?;

    assert_eq!(
        granted(PRODUCER_ID + 1, 0),
        init_producer(&storage, None).await?
    );

    let meta = snapshot(&storage).await?;
    assert_eq!(
        Some(PRODUCER_ID + 1),
        meta.transactions
            .get(TRANSACTION_ID)
            .map(|txn| txn.producer)
    );

    Ok(())
}

/// Opens a transaction for `producer` on `topition`, and produces one record in
/// it.
async fn open_transaction(
    storage: &DynoStore,
    transaction_id: &str,
    producer: ProducerIdResponse,
    topition: &Topition,
) -> Result<(), Error> {
    _ = storage
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: transaction_id.into(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: vec![
                AddPartitionsToTxnTopic::default()
                    .name(topition.topic().to_owned())
                    .partitions(Some(vec![topition.partition()])),
            ],
        })
        .await?;

    _ = storage
        .produce(
            Some(transaction_id),
            topition,
            transactional_batch(producer.id, producer.epoch)?,
        )
        .await?;

    Ok(())
}

/// When the abort of the open transaction waits behind an older open
/// transaction on the same partition, the answer is CONCURRENT_TRANSACTIONS,
/// and the producer is fenced, as in Kafka. The move happens once the older
/// transaction ends, which finishes the abort.
#[tokio::test]
async fn deferred_abort_at_the_exhausted_epoch_fences_then_moves() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topition = Topition::new("deferred", 0);
    create_topic(&storage, topition.topic()).await?;
    seed(&storage, PRODUCER_ID, &[(32766, None)]).await?;

    let older = storage
        .init_producer(Some("older"), TIMEOUT_MS, None, None)
        .await?;
    open_transaction(&storage, "older", older, &topition).await?;
    open_transaction(
        &storage,
        TRANSACTION_ID,
        granted(PRODUCER_ID, 32766),
        &topition,
    )
    .await?;

    assert_eq!(
        ProducerIdResponse::failed(ErrorCode::ConcurrentTransactions),
        init_producer(&storage, None).await?
    );
    assert_matches!(
        storage
            .produce(None, &topition, idempotent_batch(PRODUCER_ID, 32766)?)
            .await,
        Err(Error::Api(ErrorCode::ProducerFenced))
    );
    assert_eq!(
        ProducerIdResponse::failed(ErrorCode::ConcurrentTransactions),
        init_producer(&storage, None).await?
    );

    assert_eq!(
        ErrorCode::None,
        storage
            .txn_end("older", older.id, older.epoch, true)
            .await?
    );

    let moved = init_producer(&storage, None).await?;
    assert_eq!((ErrorCode::None, 0), (moved.error, moved.epoch));
    assert_ne!(PRODUCER_ID, moved.id);

    let stage = storage.offset_stage(&topition).await?;
    assert_eq!(stage.high_watermark, stage.last_stable);

    Ok(())
}

/// After a move, a whole transaction runs on the new producer ID, and a
/// transactional produce from the previous producer ID is fenced.
#[tokio::test]
async fn transaction_after_a_move() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topition = Topition::new("moved", 0);
    create_topic(&storage, topition.topic()).await?;
    seed(&storage, PRODUCER_ID, &[(32766, Some(TxnState::Committed))]).await?;

    let moved = init_producer(&storage, None).await?;
    assert_eq!(granted(PRODUCER_ID + 1, 0), moved);

    assert_matches!(
        storage
            .produce(
                Some(TRANSACTION_ID),
                &topition,
                transactional_batch(PRODUCER_ID, 32766)?
            )
            .await,
        Err(Error::Api(ErrorCode::ProducerFenced))
    );

    _ = storage
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: TRANSACTION_ID.into(),
            producer_id: moved.id,
            producer_epoch: moved.epoch,
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
                transactional_batch(moved.id, moved.epoch)?
            )
            .await?
    );
    assert_eq!(
        ErrorCode::None,
        storage
            .txn_end(TRANSACTION_ID, moved.id, moved.epoch, true)
            .await?
    );

    let stage = storage.offset_stage(&topition).await?;
    assert_eq!(stage.high_watermark, stage.last_stable);

    Ok(())
}
