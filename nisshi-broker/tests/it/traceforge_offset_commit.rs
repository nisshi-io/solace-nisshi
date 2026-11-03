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

//! Phase 3: dynostore's
//! `offset_commit` writes with `PutMode::Overwrite`
//! (`nisshi-storage-dynostore/src/dynostore.rs:1356-1400`) — unlike
//! `update_group`'s CAS or `produce`'s `OptiCon`-guarded watermark bump,
//! there is no conditional check at all here, by design: whichever commit
//! is applied last simply wins, matching real Kafka's own offset-commit
//! semantics (no compare-and-swap on committed offsets).
//!
//! Real, well-behaved clients only submit strictly increasing offsets for a
//! partition they own, and the group coordinator's generation fencing
//! (`Inner::offset_commit_fence`) is supposed to keep a superseded
//! generation's commits from ever reaching storage - so this isn't expected
//! to surface a logic bug. What it does check, exhaustively rather than by
//! spot-check, is that the *unconditional* write path is still safe to race:
//! no cross-partition contamination (a commit for one partition never
//! affects another's stored value), no commit silently dropped or
//! duplicated, and - because
//! `committed_offset_topitions` recovers topic/partition names by parsing
//! object-store paths (`dynostore.rs:1422-1441`) rather than reading them
//! back structurally - that this listing-based lookup always agrees with
//! the direct per-topition `offset_fetch`.
//!
//! `offset_commit_overwrite_is_last_write_wins_per_partition` shuffles 4
//! commit events across 2 partitions (2 differently-valued commits each)
//! into every one of the `4! = 24` possible arrival orders; after all 4 are
//! applied, both lookup paths must report exactly whichever commit landed
//! last for each partition.
//!
//! `offset_commit_survives_dropped_and_ack_lost_faults` goes further and
//! actually injects faults, rather than only varying ordering: nisshi's own
//! `Storage` trait is object-safe and has no non-`pub` internals blocking
//! this (unlike dynostore's private `OptiCon`, see
//! `traceforge_produce_idempotent.rs`), so `FaultInjectingStorage` wraps the
//! real storage and, for each of the same 4 commit events, independently
//! chooses one of three outcomes: the call succeeds normally; the write
//! never reaches storage at all (a dropped request) and the caller sees an
//! error; or the write reaches storage for real but the caller sees an
//! error anyway (a lost acknowledgement - modelling a network fault between
//! broker and client that nisshi's storage layer has no way to detect or
//! prevent, and the reason well-behaved clients must treat an offset-commit
//! error as ambiguous rather than as proof nothing happened). The two error
//! cases are indistinguishable to the caller by construction; what the test
//! checks is that the *real*, unwrapped storage - read directly, bypassing
//! the fault-injecting wrapper - always ends up in exactly the state that
//! actually landed, never a partial or corrupted one, regardless of which
//! of the `4! x 3^4 = 1944` order/fault combinations occurred.

#![cfg(feature = "dynostore")]

use crate::common::{alphanumeric_string, memory_storage};
use async_trait::async_trait;
use nisshi_sans_io::{
    ConfigResource, ErrorCode, IsolationLevel, ListOffset, ScramMechanism,
    create_topics_request::CreatableTopic, delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic, delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    fetch_response::AbortedTransaction, incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse, record::deflated,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use nisshi_storage::{
    ArcDynStorage, BrokerRegistrationRequest, CommittedOffset, Error, GroupDetail,
    ListOffsetResponse, MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage,
    ProducerIdResponse, Result as StorageResult, ScramCredential, Storage, TopicId, Topition,
    TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError,
    Version,
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use traceforge::{Config, Nondet, cover, future, verify};
use url::Url;

/// Returns the offset of each committed offset, dropping its leader epoch and metadata, which
/// these tests do not commit.
fn committed_offsets(committed: BTreeMap<Topition, CommittedOffset>) -> BTreeMap<Topition, i64> {
    committed
        .into_iter()
        .map(|(topition, committed)| (topition, committed.offset))
        .collect()
}

#[test]
fn offset_commit_overwrite_is_last_write_wins_per_partition() {
    let stats = verify(Config::builder().build(), || {
        let storage = future::block_on(memory_storage("spike", 111)).expect("storage");
        let group_id = alphanumeric_string(15);
        let topic = alphanumeric_string(15);

        _ = future::block_on(
            storage.create_topic(
                CreatableTopic::default()
                    .name(topic.clone())
                    .num_partitions(2)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            ),
        )
        .expect("create topic");

        let topition0 = Topition::new(topic.clone(), 0);
        let topition1 = Topition::new(topic.clone(), 1);

        // 4 commit events across 2 partitions, shuffled into every one of
        // the 4! = 24 possible arrival orders, rather than enumerating them
        // by hand.
        let events = [
            (topition0.clone(), 10i64),
            (topition0.clone(), 20i64),
            (topition1.clone(), 100i64),
            (topition1.clone(), 200i64),
        ];

        let mut remaining: Vec<usize> = (0..events.len()).collect();
        let mut order = Vec::new();
        while !remaining.is_empty() {
            let i = (0..remaining.len()).nondet();
            order.push(remaining.remove(i));
        }

        let mut expected: BTreeMap<Topition, i64> = BTreeMap::new();

        for &event_index in &order {
            let (topition, offset) = &events[event_index];

            let responses = future::block_on(storage.offset_commit(
                &group_id,
                None,
                &[(
                    topition.clone(),
                    OffsetCommitRequest::default().offset(*offset),
                )],
            ))
            .expect("offset commit");

            assert_eq!(responses.len(), 1);
            assert_eq!(&responses[0].0, topition);
            assert_eq!(
                responses[0].1,
                ErrorCode::None,
                "commit for {topition:?} must be accepted"
            );

            _ = expected.insert(topition.clone(), *offset);
            cover!("commit_applied");
        }

        let fetched = future::block_on(storage.offset_fetch(
            Some(&group_id),
            &[topition0.clone(), topition1.clone()],
            Some(false),
        ))
        .map(committed_offsets)
        .expect("offset fetch");

        assert_eq!(
            fetched, expected,
            "direct fetch must reflect exactly the last-applied commit per partition, \
             with no cross-partition contamination"
        );

        let listed = future::block_on(storage.committed_offset_topitions(&group_id))
            .map(committed_offsets)
            .expect("committed offset topitions");

        assert_eq!(
            listed, expected,
            "the listing-based lookup must agree with the direct fetch"
        );

        cover!("all_orders_consistent");
    });

    println!(
        "traceforge stats: execs={} blocked={}",
        stats.execs, stats.block
    );

    assert_eq!(stats.execs, 24, "expected all 4! delivery orders explored");
    assert!(stats.coverage.is_covered("commit_applied".to_string()));
    assert!(
        stats
            .coverage
            .is_covered("all_orders_consistent".to_string())
    );
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum OffsetCommitFault {
    /// No injected fault: behaves exactly like the real storage.
    None,
    /// The write never reaches storage; the caller sees an error, as if
    /// the request itself never arrived.
    Dropped,
    /// The write reaches storage for real, but the caller sees an error
    /// anyway - a lost acknowledgement, indistinguishable from `Dropped`
    /// to the caller.
    AckLost,
}

/// Wraps a real `Storage` and injects a fault into each `offset_commit`
/// call in turn, consuming one scheduled fault per call; every other method
/// delegates unchanged. Scheduling a fault before a call and consuming it
/// immediately (rather than concurrently) keeps this usable the same way
/// `traceforge_group_join.rs`'s hand-rolled model functions are: sequenced
/// by `nondet()`, not by real concurrent access.
#[derive(Clone, Debug)]
struct FaultInjectingStorage {
    inner: ArcDynStorage,
    offset_commit_faults: Arc<Mutex<VecDeque<OffsetCommitFault>>>,
}

impl FaultInjectingStorage {
    fn new(inner: ArcDynStorage) -> Self {
        Self {
            inner,
            offset_commit_faults: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    fn schedule_offset_commit(&self, fault: OffsetCommitFault) {
        self.offset_commit_faults
            .lock()
            .expect("offset_commit_faults")
            .push_back(fault);
    }
}

#[async_trait]
impl Storage for FaultInjectingStorage {
    async fn register_broker(
        &self,
        broker_registration: BrokerRegistrationRequest,
    ) -> StorageResult<()> {
        self.inner.register_broker(broker_registration).await
    }

    async fn create_topic(
        &self,
        topic: CreatableTopic,
        validate_only: bool,
    ) -> StorageResult<uuid::Uuid> {
        self.inner.create_topic(topic, validate_only).await
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> StorageResult<AlterConfigsResourceResponse> {
        self.inner.incremental_alter_resource(resource).await
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> StorageResult<Vec<DeleteRecordsTopicResult>> {
        self.inner.delete_records(topics).await
    }

    async fn delete_topic(&self, topic: &TopicId) -> StorageResult<ErrorCode> {
        self.inner.delete_topic(topic).await
    }

    async fn brokers(&self) -> StorageResult<Vec<DescribeClusterBroker>> {
        self.inner.brokers().await
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> StorageResult<i64> {
        self.inner.produce(transaction_id, topition, batch).await
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        max_wait: Duration,
    ) -> StorageResult<Vec<deflated::Batch>> {
        self.inner
            .fetch(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            .await
    }

    async fn offset_stage(&self, topition: &Topition) -> StorageResult<OffsetStage> {
        self.inner.offset_stage(topition).await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> StorageResult<Vec<(Topition, ListOffsetResponse)>> {
        self.inner.list_offsets(isolation_level, offsets).await
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> StorageResult<Vec<(Topition, ErrorCode)>> {
        let fault = self
            .offset_commit_faults
            .lock()
            .expect("offset_commit_faults")
            .pop_front()
            .unwrap_or(OffsetCommitFault::None);

        match fault {
            OffsetCommitFault::None => {
                self.inner
                    .offset_commit(group_id, retention_time_ms, offsets)
                    .await
            }

            OffsetCommitFault::Dropped => Err(Error::Api(ErrorCode::UnknownServerError)),

            OffsetCommitFault::AckLost => {
                _ = self
                    .inner
                    .offset_commit(group_id, retention_time_ms, offsets)
                    .await;
                Err(Error::Api(ErrorCode::UnknownServerError))
            }
        }
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> StorageResult<BTreeMap<Topition, CommittedOffset>> {
        self.inner
            .offset_fetch(group_id, topics, require_stable)
            .await
    }

    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> StorageResult<BTreeMap<Topition, CommittedOffset>> {
        self.inner.committed_offset_topitions(group_id).await
    }

    async fn pending_txn_offsets(&self, group_id: &str) -> StorageResult<BTreeSet<Topition>> {
        self.inner.pending_txn_offsets(group_id).await
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> StorageResult<MetadataResponse> {
        self.inner.metadata(topics).await
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> StorageResult<()> {
        self.inner
            .upsert_user_scram_credential(user, mechanism, credential)
            .await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> StorageResult<()> {
        self.inner
            .delete_user_scram_credential(user, mechanism)
            .await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> StorageResult<Option<ScramCredential>> {
        self.inner.user_scram_credential(user, mechanism).await
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> StorageResult<DescribeConfigsResult> {
        self.inner.describe_config(name, resource, keys).await
    }

    async fn list_groups(&self) -> StorageResult<Vec<NamedGroupDetail>> {
        self.inner.list_groups().await
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> StorageResult<Vec<DeletableGroupResult>> {
        self.inner.delete_groups(group_ids).await
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> StorageResult<Vec<NamedGroupDetail>> {
        self.inner
            .describe_groups(group_ids, include_authorized_operations)
            .await
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> StorageResult<Vec<DescribeTopicPartitionsResponseTopic>> {
        self.inner
            .describe_topic_partitions(topics, partition_limit, cursor)
            .await
    }

    async fn group_with_version(
        &self,
        group_id: &str,
    ) -> StorageResult<Option<(GroupDetail, Version)>> {
        self.inner.group_with_version(group_id).await
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> StorageResult<Version, UpdateError<GroupDetail>> {
        self.inner.update_group(group_id, detail, version).await
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> StorageResult<ProducerIdResponse> {
        self.inner
            .init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            )
            .await
    }

    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> StorageResult<ErrorCode> {
        self.inner
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> StorageResult<TxnAddPartitionsResponse> {
        self.inner.txn_add_partitions(partitions).await
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> StorageResult<Vec<TxnOffsetCommitResponseTopic>> {
        self.inner.txn_offset_commit(offsets).await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> StorageResult<ErrorCode> {
        self.inner
            .txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
    }

    async fn maintain(&self, now: SystemTime) -> StorageResult<()> {
        self.inner.maintain(now).await
    }

    async fn maintain_transactions(&self, now: SystemTime) -> StorageResult<()> {
        self.inner.maintain_transactions(now).await
    }

    async fn aborted_transactions(
        &self,
        topition: &Topition,
        offset: i64,
        last_stable_offset: i64,
    ) -> StorageResult<Vec<AbortedTransaction>> {
        self.inner
            .aborted_transactions(topition, offset, last_stable_offset)
            .await
    }

    async fn cluster_id(&self) -> StorageResult<String> {
        self.inner.cluster_id().await
    }

    async fn node(&self) -> StorageResult<i32> {
        self.inner.node().await
    }

    async fn advertised_listener(&self) -> StorageResult<Url> {
        self.inner.advertised_listener().await
    }

    async fn ping(&self) -> StorageResult<()> {
        self.inner.ping().await
    }
}

#[test]
fn offset_commit_survives_dropped_and_ack_lost_faults() {
    let stats = verify(Config::builder().build(), || {
        let real_storage = future::block_on(memory_storage("spike", 111)).expect("storage");
        let group_id = alphanumeric_string(15);
        let topic = alphanumeric_string(15);

        _ = future::block_on(
            real_storage.create_topic(
                CreatableTopic::default()
                    .name(topic.clone())
                    .num_partitions(2)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            ),
        )
        .expect("create topic");

        let faulty = FaultInjectingStorage::new(real_storage.clone());

        let topition0 = Topition::new(topic.clone(), 0);
        let topition1 = Topition::new(topic.clone(), 1);

        let events = [
            (topition0.clone(), 10i64),
            (topition0.clone(), 20i64),
            (topition1.clone(), 100i64),
            (topition1.clone(), 200i64),
        ];

        let mut remaining: Vec<usize> = (0..events.len()).collect();
        let mut order = Vec::new();
        while !remaining.is_empty() {
            let i = (0..remaining.len()).nondet();
            order.push(remaining.remove(i));
        }

        // Ground truth: only a `None` or `AckLost` fault lets the write
        // actually reach the real storage; `Dropped` never does, regardless
        // of what the caller is told.
        let mut expected: BTreeMap<Topition, i64> = BTreeMap::new();

        for &event_index in &order {
            let (topition, offset) = &events[event_index];

            let fault = match (0..3).nondet() {
                0 => OffsetCommitFault::None,
                1 => OffsetCommitFault::Dropped,
                2 => OffsetCommitFault::AckLost,
                _ => unreachable!(),
            };
            faulty.schedule_offset_commit(fault);

            let result = future::block_on(faulty.offset_commit(
                &group_id,
                None,
                &[(
                    topition.clone(),
                    OffsetCommitRequest::default().offset(*offset),
                )],
            ));

            match fault {
                OffsetCommitFault::None => {
                    let responses = result.expect("undisturbed commit must succeed");
                    assert_eq!(responses.len(), 1);
                    assert_eq!(
                        responses[0].1,
                        ErrorCode::None,
                        "commit for {topition:?} must be accepted"
                    );
                    _ = expected.insert(topition.clone(), *offset);
                    cover!("fault_none");
                }

                OffsetCommitFault::Dropped => {
                    assert!(
                        result.is_err(),
                        "a dropped write must be reported as an error"
                    );
                    cover!("fault_dropped");
                }

                OffsetCommitFault::AckLost => {
                    assert!(
                        result.is_err(),
                        "a lost acknowledgement must be reported as an error, \
                         indistinguishable from a dropped write"
                    );
                    _ = expected.insert(topition.clone(), *offset);
                    cover!("fault_ack_lost");
                }
            }
        }

        // Read the *real*, unwrapped storage directly: whatever the caller
        // was told, the persisted state must be exactly whichever writes
        // actually landed - no partial state, no phantom write from a
        // dropped request, no lost write despite an `AckLost` success.
        let fetched = future::block_on(real_storage.offset_fetch(
            Some(&group_id),
            &[topition0.clone(), topition1.clone()],
            Some(false),
        ))
        .map(committed_offsets)
        .expect("offset fetch");

        // Unlike `committed_offset_topitions` below, `offset_fetch` always
        // returns an entry for every topition it was asked about, using -1
        // as the "never committed" sentinel for one with no successful
        // write at all (which, under fault injection, both partitions can
        // now genuinely reach) - so the comparison here fills in that same
        // default rather than reusing `expected` as-is.
        let expected_fetch: BTreeMap<Topition, i64> = [&topition0, &topition1]
            .into_iter()
            .map(|topition| {
                (
                    topition.clone(),
                    expected.get(topition).copied().unwrap_or(-1),
                )
            })
            .collect();

        assert_eq!(
            fetched, expected_fetch,
            "real storage must reflect exactly the writes that actually landed, \
             regardless of what the caller was told about any of them"
        );

        let listed = future::block_on(real_storage.committed_offset_topitions(&group_id))
            .map(committed_offsets)
            .expect("committed offset topitions");

        assert_eq!(
            listed, expected,
            "the listing-based lookup must agree with the direct fetch under faults too"
        );

        cover!("all_consistent_after_faults");
    });

    println!(
        "traceforge stats: execs={} blocked={}",
        stats.execs, stats.block
    );

    assert_eq!(
        stats.execs,
        24 * 81,
        "expected all 4! order x 3^4 fault combinations explored"
    );
    assert!(stats.coverage.is_covered("fault_none".to_string()));
    assert!(stats.coverage.is_covered("fault_dropped".to_string()));
    assert!(stats.coverage.is_covered("fault_ack_lost".to_string()));
    assert!(
        stats
            .coverage
            .is_covered("all_consistent_after_faults".to_string())
    );
}
