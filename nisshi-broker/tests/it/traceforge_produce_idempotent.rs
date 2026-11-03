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

//! Phase 2: idempotent-producer sequence
//! dedup (`nisshi-storage-dynostore/src/dynostore.rs:824-880`) under every
//! delivery order and duplicate-redelivery combination of 3 single-record
//! batches from one producer/epoch.
//!
//! Unlike group coordination, dynostore's dedup state (`Meta.producers`,
//! guarded by a private `OptiCon<Meta>`) is entirely internal to the crate —
//! there is no `pub` `Wrapper`/`Inner`-equivalent to call directly, and no
//! way to point two separate `DynoStore`s at the same underlying in-memory
//! object store (`MemoryEngineFactory::build` always creates a fresh
//! `object_store::memory::InMemory`). So this drives the real production
//! entry points end to end - `CreateTopicsService`, `InitProducerIdService`,
//! `ProduceService`, the same `rama::Service` wrappers a real `ProduceRequest`
//! goes through - with no hand-rolled retry loop at all: `OptiCon::with_mut`
//! already retries its own conflicts internally, so each `produce.serve(..)`
//! call is a single, final, self-contained answer.
//!
//! The race being explored is delivery order, not concurrent execution —
//! TraceForge only discovers a race through choices made explicit via
//! `nondet()`, not by exploring real concurrent calls on its own — so
//! `nondet()` picks one of the `3! = 6`
//! delivery orders for the 3 batches, and independently, for each batch,
//! whether it is also immediately redelivered as a duplicate (`2^3 = 8`),
//! modeling a producer retry or network-level reordering/resend. That's
//! `48` combinations, matched against an oracle that mirrors dynostore's
//! exact sequence-check logic (`expected < base_sequence` -> out of order,
//! `expected > base_sequence` -> a repeat of a stored batch, answered with its
//! offset, else accept) tracked
//! independently in the test - not just special-cased for the one in-order
//! delivery - and finally cross-checked against what a real `Storage::fetch`
//! actually returns, so a silently dropped accept or a silently applied
//! reject would show up as a record-count mismatch, not just a wrong
//! response code.
//!
//! `idempotent_producer_survives_dropped_and_ack_lost_faults` adds actual
//! fault injection, the way `traceforge_offset_commit.rs`'s second test
//! does for `offset_commit`: `FaultInjectingProduceStorage` wraps the real
//! storage and injects a fault into `produce` calls only. This is the
//! scenario idempotent producers exist to survive: a real client cannot
//! tell a dropped request (never reached storage) from a lost
//! acknowledgement (reached storage, but the response didn't reach the
//! client), so it must retry either way, and the retry must be safe
//! regardless of which actually happened. For each of 3 batches sent in
//! order, up to 2 attempts get an independent `nondet()`-chosen fault
//! (succeed / drop / lose the ack) before a final guaranteed-clean attempt,
//! and the client-side resolution logic mirrors what a real producer does:
//! a clean success, or on retry the stored batch's offset (proof a prior
//! ack-lost attempt's hidden write already landed), both count as resolved.
//! Across all reachable fault combinations, `Storage::fetch` must always
//! show exactly the 3 distinct batches - never fewer (a drop never
//! actually retried) and never more (a hidden ack-lost write duplicated by
//! a careless retry).

#![cfg(feature = "dynostore")]

use crate::common::{alphanumeric_string, memory_storage};
use async_trait::async_trait;
use bytes::Bytes;
use nisshi_sans_io::{
    ConfigResource, CreateTopicsRequest, ErrorCode, InitProducerIdRequest, IsolationLevel,
    ListOffset, ProduceRequest, RequestInput, ScramMechanism,
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    fetch_response::AbortedTransaction,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{
        Record,
        deflated::{self, Frame},
        inflated,
    },
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use nisshi_storage::{
    ArcDynStorage, BrokerRegistrationRequest, CommittedOffset, CreateTopicsService,
    CreateTopicsValidationLayer, Error, GroupDetail, InitProducerIdService, ListOffsetResponse,
    MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProduceService,
    ProducerIdResponse, Result as StorageResult, ScramCredential, Storage, TopicId, Topition,
    TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError,
    Version,
};
use rama::{Layer as _, Service as _, extensions::Extensions};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};
use traceforge::{Config, Nondet, TypeNondet, cover, future, verify};
use url::Url;

fn batch_data(
    topic: &str,
    index: i32,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
) -> Option<Vec<TopicProduceData>> {
    let deflated = inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(b"payload").into()))
        .producer_id(producer_id)
        .producer_epoch(producer_epoch)
        .base_sequence(base_sequence)
        .build()
        .and_then(deflated::Batch::try_from)
        .expect("well-formed batch");

    Some(vec![
        TopicProduceData::default()
            .name(topic.into())
            .partition_data(Some(vec![
                PartitionProduceData::default()
                    .index(index)
                    .records(Some(Frame {
                        batches: vec![deflated],
                    })),
            ])),
    ])
}

#[test]
fn idempotent_producer_rejects_misordered_and_duplicate_batches() {
    let stats = verify(Config::builder().build(), || {
        let storage = future::block_on(memory_storage("spike", 111)).expect("storage");
        let topic = alphanumeric_string(15);
        let extensions = Extensions::default();
        let index = 0;

        let create_topics = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
            storage: storage.clone(),
        });
        let response = future::block_on(
            create_topics.serve(RequestInput {
                request: CreateTopicsRequest::default()
                    .validate_only(Some(false))
                    .topics(Some(vec![
                        CreatableTopic::default()
                            .name(topic.clone())
                            .num_partitions(1)
                            .replication_factor(1)
                            .assignments(Some([].into()))
                            .configs(Some([].into())),
                    ])),
                extensions: extensions.clone(),
            }),
        )
        .expect("create topic");

        let topics = response.topics.as_deref().unwrap_or_default();
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(topics[0].error_code).unwrap()
        );

        let init_producer_id = InitProducerIdService {
            storage: storage.clone(),
        };
        let producer = future::block_on(
            init_producer_id.serve(RequestInput {
                request: InitProducerIdRequest::default()
                    .transactional_id(None)
                    .transaction_timeout_ms(0)
                    .producer_id(Some(-1))
                    .producer_epoch(Some(-1)),
                extensions: extensions.clone(),
            }),
        )
        .expect("init producer id");

        // Pick a delivery order for base_sequence 0, 1, 2 by repeatedly
        // choosing an index among whoever's left, rather than enumerating
        // all 6 permutations by hand.
        let mut remaining: Vec<i32> = (0..3).collect();
        let mut order = Vec::new();
        while !remaining.is_empty() {
            let i = (0..remaining.len()).nondet();
            order.push(remaining.remove(i));
        }

        let produce = ProduceService {
            storage: storage.clone(),
        };

        // Oracle: mirrors dynostore's sequence check exactly
        // but tracked independently here, so it can
        // be checked against every one of the 48 order/duplicate
        // combinations rather than only the one fully-in-order case.
        let mut expected_sequence: i32 = 0;
        let mut next_offset: i64 = 0;
        let mut accepted_batches: usize = 0;

        for &base_sequence in &order {
            let redeliver = bool::nondet();
            let deliveries = if redeliver { 2 } else { 1 };

            for _ in 0..deliveries {
                let response = future::block_on(
                    produce.serve(RequestInput {
                        request: ProduceRequest::default()
                            .transactional_id(None)
                            .acks(1)
                            .timeout_ms(0)
                            .topic_data(batch_data(
                                &topic,
                                index,
                                producer.producer_id,
                                producer.producer_epoch,
                                base_sequence,
                            )),
                        extensions: extensions.clone(),
                    }),
                )
                .expect("produce");

                let partition_response = &response.responses.as_ref().unwrap()[0]
                    .partition_responses
                    .as_ref()
                    .unwrap()[0];
                let error_code = ErrorCode::try_from(partition_response.error_code).unwrap();

                if expected_sequence < base_sequence {
                    assert_eq!(
                        error_code,
                        ErrorCode::OutOfOrderSequenceNumber,
                        "base_sequence={base_sequence} expected_sequence={expected_sequence}"
                    );
                    cover!("rejected_out_of_order");
                } else if expected_sequence > base_sequence {
                    // Each batch holds one record and was accepted in
                    // order, so the batch at this sequence is stored at the
                    // offset of the same number.
                    assert_eq!(
                        error_code,
                        ErrorCode::None,
                        "base_sequence={base_sequence} expected_sequence={expected_sequence}"
                    );
                    assert_eq!(partition_response.base_offset, i64::from(base_sequence));
                    cover!("answered_duplicate");
                } else {
                    assert_eq!(
                        error_code,
                        ErrorCode::None,
                        "base_sequence={base_sequence} expected_sequence={expected_sequence}"
                    );
                    assert_eq!(partition_response.base_offset, next_offset);

                    expected_sequence += 1;
                    next_offset += 1;
                    accepted_batches += 1;

                    cover!("accepted_in_order");
                }
            }
        }

        // Cross-check against what a fetcher actually sees: a silently
        // dropped accept or a silently applied reject shows up here as a
        // record-count mismatch, not just a wrong response code above.
        let topition = Topition::new(topic.clone(), index);
        let fetched = future::block_on(storage.fetch(
            &topition,
            0,
            0,
            i32::MAX as u32,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(1000),
        ))
        .expect("fetch");

        let fetched_records: usize = fetched
            .iter()
            .map(|batch| batch.record_count as usize)
            .sum();
        assert_eq!(
            fetched_records, accepted_batches,
            "fetcher must see exactly the accepted batches, no more, no less"
        );

        if order == [0, 1, 2] {
            cover!("in_order_delivery_explored");
        }
    });

    println!(
        "traceforge stats: execs={} blocked={}",
        stats.execs, stats.block
    );

    assert!(
        stats
            .coverage
            .is_covered("rejected_out_of_order".to_string())
    );
    assert!(stats.coverage.is_covered("answered_duplicate".to_string()));
    assert!(stats.coverage.is_covered("accepted_in_order".to_string()));
    assert!(
        stats
            .coverage
            .is_covered("in_order_delivery_explored".to_string())
    );
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum ProduceFault {
    /// No injected fault: behaves exactly like the real storage.
    None,
    /// The write never reaches storage; the caller sees an error, as if
    /// the request itself never arrived.
    Dropped,
    /// The write reaches storage for real, but the caller sees an error
    /// anyway - a lost acknowledgement, indistinguishable from `Dropped`
    /// to the caller. This is exactly the ambiguity idempotent producers
    /// exist to survive.
    AckLost,
}

/// Wraps a real `Storage` and injects a fault into each `produce` call in
/// turn, consuming one scheduled fault per call; every other method
/// delegates unchanged. See `FaultInjectingStorage` in
/// `traceforge_offset_commit.rs` for the same pattern applied to
/// `offset_commit`.
#[derive(Clone, Debug)]
struct FaultInjectingProduceStorage {
    inner: ArcDynStorage,
    produce_faults: Arc<Mutex<VecDeque<ProduceFault>>>,
}

impl FaultInjectingProduceStorage {
    fn new(inner: ArcDynStorage) -> Self {
        Self {
            inner,
            produce_faults: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    fn schedule_produce(&self, fault: ProduceFault) {
        self.produce_faults
            .lock()
            .expect("produce_faults")
            .push_back(fault);
    }
}

#[async_trait]
impl Storage for FaultInjectingProduceStorage {
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
        let fault = self
            .produce_faults
            .lock()
            .expect("produce_faults")
            .pop_front()
            .unwrap_or(ProduceFault::None);

        match fault {
            ProduceFault::None => self.inner.produce(transaction_id, topition, batch).await,

            ProduceFault::Dropped => Err(Error::Api(ErrorCode::RequestTimedOut)),

            ProduceFault::AckLost => {
                _ = self.inner.produce(transaction_id, topition, batch).await;
                Err(Error::Api(ErrorCode::RequestTimedOut))
            }
        }
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
        self.inner
            .offset_commit(group_id, retention_time_ms, offsets)
            .await
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
fn idempotent_producer_survives_dropped_and_ack_lost_faults() {
    let stats = verify(Config::builder().build(), || {
        let real_storage = future::block_on(memory_storage("spike", 111)).expect("storage");
        let topic = alphanumeric_string(15);
        let extensions = Extensions::default();
        let index = 0;

        _ = future::block_on(
            real_storage.create_topic(
                CreatableTopic::default()
                    .name(topic.clone())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            ),
        )
        .expect("create topic");

        let init_producer_id = InitProducerIdService {
            storage: real_storage.clone(),
        };
        let producer = future::block_on(
            init_producer_id.serve(RequestInput {
                request: InitProducerIdRequest::default()
                    .transactional_id(None)
                    .transaction_timeout_ms(0)
                    .producer_id(Some(-1))
                    .producer_epoch(Some(-1)),
                extensions: extensions.clone(),
            }),
        )
        .expect("init producer id");

        let faulty = FaultInjectingProduceStorage::new(real_storage.clone());
        let produce = ProduceService {
            storage: faulty.clone(),
        };

        const MAX_ATTEMPTS: usize = 3;

        for base_sequence in 0..3i32 {
            let mut resolved = false;
            let mut stored_by_an_ack_lost_attempt = false;

            for attempt in 0..MAX_ATTEMPTS {
                let fault = if attempt + 1 == MAX_ATTEMPTS {
                    // Guarantee termination: after 2 faulted attempts, the
                    // 3rd is always clean.
                    ProduceFault::None
                } else {
                    match (0..3).nondet() {
                        0 => ProduceFault::None,
                        1 => ProduceFault::Dropped,
                        2 => ProduceFault::AckLost,
                        _ => unreachable!(),
                    }
                };
                faulty.schedule_produce(fault);

                let response = future::block_on(
                    produce.serve(RequestInput {
                        request: ProduceRequest::default()
                            .transactional_id(None)
                            .acks(1)
                            .timeout_ms(0)
                            .topic_data(batch_data(
                                &topic,
                                index,
                                producer.producer_id,
                                producer.producer_epoch,
                                base_sequence,
                            )),
                        extensions: extensions.clone(),
                    }),
                )
                .expect("produce");

                let partition_response = &response.responses.as_ref().unwrap()[0]
                    .partition_responses
                    .as_ref()
                    .unwrap()[0];
                let error_code = ErrorCode::try_from(partition_response.error_code).unwrap();

                match (fault, error_code) {
                    // Each batch holds one record and is stored in order, so
                    // it is at the offset of its sequence number. A retry
                    // after a lost acknowledgement repeats the stored batch,
                    // and the broker answers it with that batch's offset.
                    (ProduceFault::None, ErrorCode::None) => {
                        assert_eq!(partition_response.base_offset, i64::from(base_sequence));

                        cover!(if stored_by_an_ack_lost_attempt {
                            "resolved_by_duplicate_detection"
                        } else {
                            "resolved_by_direct_accept"
                        });
                        resolved = true;
                    }

                    (ProduceFault::None, other) => {
                        panic!(
                            "unexpected error on an undisturbed attempt: {other:?} \
                             (base_sequence={base_sequence}, attempt={attempt})"
                        );
                    }

                    (ProduceFault::Dropped, _) | (ProduceFault::AckLost, _) => {
                        assert_ne!(
                            error_code,
                            ErrorCode::None,
                            "an injected fault must never look like success to the caller"
                        );
                        stored_by_an_ack_lost_attempt |= fault == ProduceFault::AckLost;

                        cover!(match fault {
                            ProduceFault::Dropped => "attempt_dropped",
                            ProduceFault::AckLost => "attempt_ack_lost",
                            ProduceFault::None => unreachable!(),
                        });
                    }
                }

                if resolved {
                    break;
                }
            }

            assert!(
                resolved,
                "base_sequence={base_sequence} must resolve within {MAX_ATTEMPTS} attempts"
            );
        }

        // Regardless of which attempts were faulted, idempotence must hold:
        // exactly the 3 distinct batches sent, no more (a hidden ack-lost
        // write duplicated by a careless retry) and no fewer (a dropped
        // write never actually retried).
        let topition = Topition::new(topic.clone(), index);
        let fetched = future::block_on(real_storage.fetch(
            &topition,
            0,
            0,
            i32::MAX as u32,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(1000),
        ))
        .expect("fetch");

        let fetched_records: usize = fetched
            .iter()
            .map(|batch| batch.record_count as usize)
            .sum();
        assert_eq!(
            fetched_records, 3,
            "exactly the 3 distinct batches must be durably persisted, regardless of faults"
        );

        cover!("all_batches_resolved");
    });

    println!(
        "traceforge stats: execs={} blocked={}",
        stats.execs, stats.block
    );

    assert!(
        stats
            .coverage
            .is_covered("resolved_by_direct_accept".to_string())
    );
    assert!(
        stats
            .coverage
            .is_covered("resolved_by_duplicate_detection".to_string())
    );
    assert!(stats.coverage.is_covered("attempt_dropped".to_string()));
    assert!(stats.coverage.is_covered("attempt_ack_lost".to_string()));
    assert!(
        stats
            .coverage
            .is_covered("all_batches_resolved".to_string())
    );
}
