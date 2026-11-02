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

//! A transaction that outlives `transaction.timeout.ms` is aborted by the
//! maintenance sweep, and its producer is fenced, as in Kafka. A transaction
//! starts at its first `AddOffsetsToTxn`, `AddPartitionsToTxn` or stored
//! `TxnOffsetCommit`, so one that only commits offsets times out too, and
//! its pending offsets stop holding the group's partitions unstable.

use std::{
    assert_matches,
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use nisshi_sans_io::{
    BatchAttribute, ErrorCode,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::CreatableTopic,
    record::{Record, inflated},
    txn_offset_commit_request::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic},
};
use nisshi_storage::{
    Error, ProducerIdResponse, Storage as _, Topition, TxnAddPartitionsRequest,
    TxnAddPartitionsResponse, TxnOffsetCommitRequest,
};
use object_store::memory::InMemory;

use crate::dynostore::{DynoStore, TxnDetail, tests::init_tracing};

const TRANSACTION: &str = "txn";
const GROUP: &str = "group";
const TOPIC: &str = "topic";
const TIMEOUT_MS: i32 = 10_000;

fn after_timeout() -> SystemTime {
    SystemTime::now() + Duration::from_millis(TIMEOUT_MS as u64 + 1_000)
}

async fn storage() -> Result<DynoStore, Error> {
    let storage = DynoStore::new("nisshi", 1, Arc::new(InMemory::new()))?;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(storage)
}

async fn init(storage: &DynoStore, timeout_ms: i32) -> Result<ProducerIdResponse, Error> {
    let producer = storage
        .init_producer(Some(TRANSACTION), timeout_ms, Some(-1), Some(-1))
        .await?;
    assert_eq!(ErrorCode::None, producer.error);
    Ok(producer)
}

async fn commit_offset(storage: &DynoStore, producer: &ProducerIdResponse) -> Result<(), Error> {
    let outcome = storage
        .txn_offset_commit(TxnOffsetCommitRequest {
            transaction_id: TRANSACTION.into(),
            group_id: GROUP.into(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            generation_id: None,
            member_id: None,
            group_instance_id: None,
            topics: vec![
                TxnOffsetCommitRequestTopic::default()
                    .name(TOPIC.into())
                    .partitions(Some(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .partition_index(0)
                            .committed_offset(7)
                            .committed_leader_epoch(None)
                            .committed_metadata(None),
                    ])),
            ],
        })
        .await?;

    assert_eq!(
        vec![i16::from(ErrorCode::None)],
        outcome
            .iter()
            .flat_map(|topic| topic.partitions.iter().flatten())
            .map(|partition| partition.error_code)
            .collect::<Vec<_>>()
    );

    Ok(())
}

async fn add_partition(
    storage: &DynoStore,
    producer: &ProducerIdResponse,
) -> Result<ErrorCode, Error> {
    let TxnAddPartitionsResponse::VersionZeroToThree(results) = storage
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: TRANSACTION.into(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: vec![
                AddPartitionsToTxnTopic::default()
                    .name(TOPIC.into())
                    .partitions(Some(vec![0])),
            ],
        })
        .await?
    else {
        unreachable!("version zero to three request")
    };

    Ok(results
        .iter()
        .flat_map(|topic| topic.results_by_partition.iter().flatten())
        .map(|partition| ErrorCode::try_from(partition.partition_error_code))
        .next()
        .expect("one partition")
        .expect("known error code"))
}

fn pending() -> BTreeSet<Topition> {
    BTreeSet::from([Topition::new(TOPIC, 0)])
}

/// A producer that commits offsets and never ends the transaction: its offset
/// is pending until the timeout, and not after it.
#[tokio::test]
async fn offsets_only_transaction_times_out() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage().await?;
    let producer = init(&storage, TIMEOUT_MS).await?;

    assert_eq!(
        ErrorCode::None,
        storage
            .txn_add_offsets(TRANSACTION, producer.id, producer.epoch, GROUP)
            .await?
    );
    commit_offset(&storage, &producer).await?;
    assert_eq!(pending(), storage.pending_txn_offsets(GROUP).await?);

    storage.maintain_transactions(SystemTime::now()).await?;
    assert_eq!(pending(), storage.pending_txn_offsets(GROUP).await?);

    storage.maintain_transactions(after_timeout()).await?;
    assert_eq!(BTreeSet::new(), storage.pending_txn_offsets(GROUP).await?);

    // aborted: the offset was not committed.
    assert_eq!(
        None,
        storage
            .committed_offset_topitions(GROUP)
            .await?
            .get(&Topition::new(TOPIC, 0))
    );

    Ok(())
}

/// A `TxnOffsetCommit` without an `AddOffsetsToTxn` starts the transaction too.
#[tokio::test]
async fn first_txn_offset_commit_starts_the_transaction() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage().await?;
    let producer = init(&storage, TIMEOUT_MS).await?;

    commit_offset(&storage, &producer).await?;
    assert_eq!(pending(), storage.pending_txn_offsets(GROUP).await?);

    storage.maintain_transactions(after_timeout()).await?;
    assert_eq!(BTreeSet::new(), storage.pending_txn_offsets(GROUP).await?);

    Ok(())
}

/// The sweep fences the producer: it cannot end the transaction it lost, nor
/// add to it, and its `InitProducerId` moves on from the epoch that fenced it.
#[tokio::test]
async fn timed_out_producer_is_fenced() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage().await?;
    let producer = init(&storage, TIMEOUT_MS).await?;

    commit_offset(&storage, &producer).await?;
    storage.maintain_transactions(after_timeout()).await?;

    assert_matches!(
        storage
            .txn_end(TRANSACTION, producer.id, producer.epoch, true)
            .await,
        Err(Error::Api(ErrorCode::ProducerFenced))
    );
    assert_eq!(
        ErrorCode::ProducerFenced,
        add_partition(&storage, &producer).await?
    );

    let next = init(&storage, TIMEOUT_MS).await?;
    // The sweep took the next epoch to fence the producer, as Kafka's does.
    assert_eq!((producer.id, producer.epoch + 2), (next.id, next.epoch));

    // The sweep of a later time finds nothing more to abort.
    storage.maintain_transactions(after_timeout()).await?;
    assert_eq!(BTreeSet::new(), storage.pending_txn_offsets(GROUP).await?);

    Ok(())
}

/// A transaction that produced has its records closed with an abort marker, so
/// the last stable offset reaches the high watermark again.
#[tokio::test]
async fn timed_out_transaction_that_produced_is_aborted() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage().await?;
    let producer = init(&storage, TIMEOUT_MS).await?;

    assert_eq!(ErrorCode::None, add_partition(&storage, &producer).await?);

    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"abc"))))
        .attributes(BatchAttribute::default().transaction(true).into())
        .producer_id(producer.id)
        .producer_epoch(producer.epoch)
        .base_sequence(0)
        .build()
        .and_then(TryInto::try_into)?;

    _ = storage
        .produce(Some(TRANSACTION), &Topition::new(TOPIC, 0), batch)
        .await?;

    let stage = storage.offset_stage(&Topition::new(TOPIC, 0)).await?;
    assert_eq!(0, stage.last_stable());

    storage.maintain_transactions(after_timeout()).await?;

    let stage = storage.offset_stage(&Topition::new(TOPIC, 0)).await?;
    assert_eq!(stage.high_watermark(), stage.last_stable());
    assert_eq!(2, stage.high_watermark());

    Ok(())
}

/// A timeout of 0 never expires, and a transaction that has not started
/// has nothing to expire.
#[tokio::test]
async fn transaction_without_timeout_or_start_is_left_alone() -> Result<(), Error> {
    let _guard = init_tracing()?;
    let storage = storage().await?;

    let producer = init(&storage, 0).await?;
    commit_offset(&storage, &producer).await?;
    storage.maintain_transactions(after_timeout()).await?;
    assert_eq!(pending(), storage.pending_txn_offsets(GROUP).await?);

    Ok(())
}

/// Adding to an ongoing transaction keeps the time it started, as in Kafka,
/// and a transaction that is preparing its end stays so.
#[test]
fn begin_starts_only_an_empty_or_finished_transaction() {
    let started = SystemTime::UNIX_EPOCH;
    let later = started + Duration::from_secs(5);

    for state in [
        None,
        Some(nisshi_storage::TxnState::Committed),
        Some(nisshi_storage::TxnState::Aborted),
    ] {
        let mut detail = TxnDetail {
            state,
            ..Default::default()
        };
        detail.begin(started);
        assert_eq!(Some(nisshi_storage::TxnState::Begin), detail.state);
        assert_eq!(Some(started), detail.started_at);
    }

    for state in [
        nisshi_storage::TxnState::Begin,
        nisshi_storage::TxnState::PrepareCommit,
        nisshi_storage::TxnState::PrepareAbort,
    ] {
        let mut detail = TxnDetail {
            state: Some(state),
            started_at: Some(started),
            ..Default::default()
        };
        detail.begin(later);
        assert_eq!(Some(state), detail.state);
        assert_eq!(Some(started), detail.started_at);
    }
}

/// The sweep takes an abort that is prepared in an older epoch, which only a sweep that fenced
/// the producer leaves, and not the client's own abort that waits in the newest epoch: that
/// producer is not dead, and fencing it would turn a wait into a failure.
#[test]
fn prepared_abort_is_swept_only_in_an_older_epoch() {
    let started = SystemTime::UNIX_EPOCH;
    let detail = TxnDetail {
        transaction_timeout_ms: 1_000,
        started_at: Some(started),
        state: Some(nisshi_storage::TxnState::PrepareAbort),
        ..Default::default()
    };
    let now = started + Duration::from_secs(5);

    assert!(!detail.timed_out(now, true));
    assert!(detail.timed_out(now, false));
}
