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

//! Phase 3: fault injection at the
//! `object_store::ObjectStore` layer underneath dynostore, one level below
//! `traceforge_produce_idempotent.rs`'s `Storage`-level fault injection.
//! `nisshi_storage_dynostore::DynoStore` and its `new(cluster, node,
//! object_store: impl ObjectStore)` constructor are `pub`, so a test can
//! supply its own `ObjectStore` and target *which specific write inside a
//! single `Storage::produce` call* fails, rather than faulting the call as
//! a whole.
//!
//! `FaultInjectingObjectStore` wraps a real `object_store::memory::InMemory`
//! and injects a failure into `put_opts` calls whose path contains
//! `/records/` - the final write in `Storage::produce`
//! (`nisshi-storage-dynostore/src/dynostore.rs:989-1010`), which happens
//! *after* the idempotent-producer sequence number has already been
//! durably advanced by a separate, already-committed write
//! (`meta.with_mut`, `dynostore.rs:824-880`). Those are two independent
//! objects with no cross-object atomicity, so a failure confined to just
//! the record write is exactly the fault this test targets.
//!
//! # Bug found and fixed
//!
//! This test originally demonstrated real, reproducible data loss: if a
//! batch's record write failed after its sequence number had already been
//! committed, the client's mandatory retry (the whole point of an
//! idempotent producer - a client cannot tell a dropped request from a
//! lost acknowledgement, and must retry either way) was told
//! `DuplicateSequenceNumber`. Every real Kafka client treats that response
//! as confirmation the original attempt already succeeded and takes no
//! further action - but `Storage::fetch` showed the record was never
//! written. The failure was silent and unrecoverable: nothing about the
//! client-visible protocol distinguished it from a genuine duplicate.
//!
//! Reproduced first with a single deterministic case
//! (`create_topics`/`producer_id`=1/`base_sequence`=0), confirmed via
//! `println!` before this exhaustive version was written:
//! attempt 1 (record write faulted) -> `UnknownServerError`; attempt 2
//! (retry, clean) -> `DuplicateSequenceNumber`; `Storage::fetch` -> zero
//! records.
//!
//! Fixed in `produce()` by splitting the sequence check into a read-only
//! pre-check (rejects an out-of-order/duplicate batch before any watermark
//! bump or write, as before) and a durable advance that now only commits
//! *after* the record write below succeeds - so a transient write failure
//! leaves the sequence un-advanced and a retry is accepted normally at a
//! new offset, rather than falsely rejected as a duplicate. The test below
//! asserts the *correct* behavior - that a `DuplicateSequenceNumber`
//! response only ever occurs when the data really is there - across all
//! `2^3 = 8` combinations of which of 3 batches (sent in order) has its
//! record write faulted once, and now passes on all of them.

#![cfg(feature = "dynostore")]

use crate::common::alphanumeric_string;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use nisshi_sans_io::{
    BatchAttribute, CreateTopicsRequest, ErrorCode, InitProducerIdRequest, IsolationLevel,
    ProduceRequest, RequestInput,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::CreatableTopic,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{
        Record,
        deflated::{self, Frame},
        inflated,
    },
};
use nisshi_storage::{
    ArcDynStorage, CreateTopicsService, CreateTopicsValidationLayer, InitProducerIdService,
    ProduceService, Storage, Topition, TxnAddPartitionsRequest,
};
use nisshi_storage_dynostore::DynoStore;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use rama::{Layer as _, Service as _, extensions::Extensions};
use std::{
    assert_matches,
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;
use traceforge::{Config, Nondet, TypeNondet, cover, future, verify};
use url::Url;

#[derive(Debug)]
struct FaultInjectingObjectStore<O> {
    inner: O,
    record_write_faults: Mutex<VecDeque<bool>>,
}

impl<O> FaultInjectingObjectStore<O> {
    fn new(inner: O) -> Self {
        Self {
            inner,
            record_write_faults: Mutex::new(VecDeque::new()),
        }
    }

    fn schedule_record_write(&self, should_fail: bool) {
        self.record_write_faults
            .lock()
            .expect("record_write_faults")
            .push_back(should_fail);
    }
}

impl<O> std::fmt::Display for FaultInjectingObjectStore<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultInjectingObjectStore")
    }
}

#[async_trait]
impl<O: ObjectStore> ObjectStore for FaultInjectingObjectStore<O> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if location.as_ref().contains("/records/") {
            let should_fail = self
                .record_write_faults
                .lock()
                .expect("record_write_faults")
                .pop_front()
                .unwrap_or(false);

            if should_fail {
                return Err(object_store::Error::Generic {
                    store: "FaultInjectingObjectStore",
                    source: "injected fault: record write failed".into(),
                });
            }
        }

        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

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
fn record_write_failure_never_causes_silent_data_loss() {
    let stats = verify(Config::builder().build(), || {
        let object_store = Arc::new(FaultInjectingObjectStore::new(InMemory::new()));
        let storage: ArcDynStorage = Arc::new(Box::new(
            DynoStore::new("spike", 111, object_store.clone())
                .expect("dynostore")
                .advertised_listener(Url::parse("tcp://127.0.0.1/").expect("url"))
                .schemas(None)
                .lake(None),
        ));

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
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(response.topics.as_deref().unwrap_or_default()[0].error_code)
                .unwrap()
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

        let produce = ProduceService {
            storage: storage.clone(),
        };

        // A client believes a batch is durably stored once it sees either
        // a direct success or a `DuplicateSequenceNumber` on retry (proof,
        // to a real client, that an earlier attempt already landed).
        let mut client_believes_durable = 0usize;

        for base_sequence in 0..3i32 {
            let fault_this_batch = bool::nondet();

            if fault_this_batch {
                object_store.schedule_record_write(true);

                let response1 = future::block_on(
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
                let error1 = ErrorCode::try_from(
                    response1.responses.as_ref().unwrap()[0]
                        .partition_responses
                        .as_ref()
                        .unwrap()[0]
                        .error_code,
                )
                .unwrap();
                assert_ne!(
                    error1,
                    ErrorCode::None,
                    "the faulted record write must be reported as an error"
                );
                cover!("first_attempt_faulted");

                // A well-behaved idempotent producer always retries on an
                // ambiguous error, using the same base_sequence.
                object_store.schedule_record_write(false);

                let response2 = future::block_on(
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
                let error2 = ErrorCode::try_from(
                    response2.responses.as_ref().unwrap()[0]
                        .partition_responses
                        .as_ref()
                        .unwrap()[0]
                        .error_code,
                )
                .unwrap();

                match error2 {
                    ErrorCode::None => {
                        cover!("retry_directly_succeeded");
                    }
                    ErrorCode::DuplicateSequenceNumber => {
                        cover!("retry_told_duplicate");
                    }
                    other => panic!("unexpected retry error: {other:?}"),
                }

                client_believes_durable += 1;
            } else {
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
                let error = ErrorCode::try_from(
                    response.responses.as_ref().unwrap()[0]
                        .partition_responses
                        .as_ref()
                        .unwrap()[0]
                        .error_code,
                )
                .unwrap();
                assert_eq!(
                    error,
                    ErrorCode::None,
                    "an undisturbed produce must succeed"
                );
                client_believes_durable += 1;
                cover!("undisturbed");
            }
        }

        let topition = Topition::new(topic.clone(), index);
        let fetched = future::block_on(storage.fetch(
            &topition,
            0,
            0,
            i32::MAX as u32,
            IsolationLevel::ReadUncommitted,
            Instant::now() + Duration::from_millis(1000),
        ))
        .expect("fetch");
        let fetched_records: usize = fetched
            .iter()
            .map(|batch| batch.record_count as usize)
            .sum();

        // Regression guard for the fixed bug: a `DuplicateSequenceNumber`
        // response must mean the data really is there. It used to not,
        // when the record write itself was what failed after the
        // sequence number had already been durably committed by a
        // separate write.
        assert_eq!(
            fetched_records, client_believes_durable,
            "every batch the client believes is durable must actually be retrievable"
        );
    });

    println!(
        "traceforge stats: execs={} blocked={}",
        stats.execs, stats.block
    );

    assert_eq!(
        stats.execs, 8,
        "expected all 2^3 fault combinations explored"
    );
}

/// Phase 3: `txn_end`'s end-of-transaction marker write is a second
/// instance of the same class of bug as
/// `record_write_failure_never_causes_silent_data_loss` above, found the
/// same way: `txn_end` (`dynostore.rs:2421-2661`) transitions
/// `txn_detail.state` from `Begin` to `PrepareCommit` in one durably
/// committed `meta.with_mut` write, *then* loops over every partition the
/// transaction touched, writing an end-of-transaction control marker to
/// each via a separate `produce` call per partition. If one of those
/// marker writes fails, `txn_end` returns an error - but on retry, the
/// `if txn_detail.state == Some(TxnState::Begin)` check
/// (`dynostore.rs:2453`) is now false, so the set of partitions needing a
/// marker is computed as empty and none of the remaining marker writes are
/// ever retried. The retry proceeds straight to marking the transaction
/// `Committed` anyway.
///
/// For 2 partitions with one record each, `nondet()` picks one of 3
/// outcomes: no fault, the first marker write fails, or the second does
/// (the loop iterates partitions in order, so failing the first means
/// neither marker is ever written, since the loop returns before reaching
/// the second; failing the second leaves the first correctly marked and
/// only the second missing). Confirmed first as a deterministic
/// `println!`-traced case: attempt 1 (second marker faulted) -> error;
/// attempt 2 (retry) -> `ErrorCode::None` (committed); partition 0 fetch:
/// 2 batches, has a control marker; partition 1 fetch: 1 batch, **no**
/// control marker, despite the transaction being reported committed.
///
/// The consequence for a real `read_committed` consumer: without an
/// end-of-transaction marker, records in a still-open (as far as that
/// partition's log is concerned) transaction are held back indefinitely -
/// the data in partition 1 is not lost the way the first bug's is, but it
/// is invisible to `read_committed` readers forever, despite every other
/// signal (the coordinator's state, the producer's own view) agreeing the
/// transaction committed cleanly.
///
/// Fixed in `txn_end` by recomputing the set of partitions still needing a
/// marker from whatever is still in `txn_detail.produces` whenever the
/// transaction is in (freshly, or already) a `Prepare*` state, not only on
/// the one-time `Begin -> Prepare*` transition - `produces` isn't cleared
/// until every marker has actually been written, so a retry after a
/// partial failure now retries whichever partitions are still missing
/// one (accepting a second, harmless marker for a partition that already
/// got one, as a deliberately minimal trade-off). All 3 combinations now
/// pass.
#[test]
fn end_txn_marker_write_failure_completes_transaction_anyway() {
    let stats = verify(Config::builder().build(), || {
        let object_store = Arc::new(FaultInjectingObjectStore::new(InMemory::new()));
        let storage: ArcDynStorage = Arc::new(Box::new(
            DynoStore::new("spike", 111, object_store.clone())
                .expect("dynostore")
                .advertised_listener(Url::parse("tcp://127.0.0.1/").expect("url"))
                .schemas(None)
                .lake(None),
        ));

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

        let transaction_id = alphanumeric_string(15);
        let producer = future::block_on(storage.init_producer(
            Some(&transaction_id),
            10_000,
            Some(-1),
            Some(-1),
        ))
        .expect("init producer");

        let topition0 = Topition::new(topic.clone(), 0);
        let topition1 = Topition::new(topic.clone(), 1);

        _ = future::block_on(
            storage.txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: [AddPartitionsToTxnTopic::default()
                    .name(topic.clone())
                    .partitions(Some([0, 1].into()))]
                .into(),
            }),
        )
        .expect("add partitions");

        // One record to each partition, both part of the transaction, both
        // undisturbed.
        for topition in [&topition0, &topition1] {
            let batch = inflated::Batch::builder()
                .record(Record::builder().value(Bytes::from_static(b"payload").into()))
                .attributes(BatchAttribute::default().transaction(true).into())
                .producer_id(producer.id)
                .producer_epoch(producer.epoch)
                .base_sequence(0)
                .build()
                .and_then(deflated::Batch::try_from)
                .expect("well-formed batch");

            _ = future::block_on(storage.produce(Some(&transaction_id), topition, batch))
                .expect("produce");
        }

        // The `txn_end` marker-write loop iterates `produced` (a BTreeMap)
        // in partition order, so scheduling `[false]` only reaches
        // partition 0's marker before returning, `[true]` fails partition
        // 0's marker directly, and `[false, true]` lets partition 0's
        // succeed before failing partition 1's.
        let fault_slot = (0..3).nondet();
        match fault_slot {
            0 => {
                object_store.schedule_record_write(false);
                object_store.schedule_record_write(false);
            }
            1 => {
                object_store.schedule_record_write(true);
            }
            2 => {
                object_store.schedule_record_write(false);
                object_store.schedule_record_write(true);
            }
            _ => unreachable!(),
        }

        let attempt1 =
            future::block_on(storage.txn_end(&transaction_id, producer.id, producer.epoch, true));

        if fault_slot == 0 {
            assert_eq!(
                attempt1.expect("undisturbed txn_end must succeed"),
                ErrorCode::None
            );
            cover!("no_fault");
        } else {
            assert!(
                attempt1.is_err(),
                "the faulted marker write must surface as an error"
            );
            cover!(if fault_slot == 1 {
                "first_marker_faulted"
            } else {
                "second_marker_faulted"
            });

            // The client retries, as it must on an ambiguous EndTxn error.
            let attempt2 = future::block_on(storage.txn_end(
                &transaction_id,
                producer.id,
                producer.epoch,
                true,
            ))
            .expect("txn_end retry");
            assert_eq!(
                attempt2,
                ErrorCode::None,
                "the retry must report the transaction as committed"
            );
        }

        // Regression guard for the fixed bug: `txn_end` having reported
        // the transaction committed must mean every partition it touched
        // has both its data and its end-of-transaction marker -
        // otherwise a `read_committed` consumer of that partition can
        // never learn the transaction concluded.
        for topition in [&topition0, &topition1] {
            let fetched = future::block_on(storage.fetch(
                topition,
                0,
                0,
                i32::MAX as u32,
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_millis(1000),
            ))
            .expect("fetch");

            let has_control_marker = fetched.iter().any(|batch| {
                BatchAttribute::try_from(batch.attributes)
                    .map(|attributes| attributes.control)
                    .unwrap_or(false)
            });

            assert!(
                has_control_marker,
                "{topition:?} is part of a transaction reported committed \
                 but has no end-of-transaction marker"
            );
        }

        cover!("all_partitions_marked");
    });

    println!(
        "traceforge stats: execs={} blocked={}",
        stats.execs, stats.block
    );

    assert_eq!(
        stats.execs, 3,
        "expected all 3 fault-slot combinations explored"
    );
}

/// A `txn_end` retry must repeat the decision that was prepared. After a
/// commit whose partition 1 marker write failed, an abort retry would put
/// abort markers on partition 1 after partition 0's commit marker; it is
/// rejected with `InvalidTxnState`, and a commit retry still completes.
#[test]
fn end_txn_retry_with_opposite_decision_is_rejected() {
    _ = verify(Config::builder().build(), || {
        let object_store = Arc::new(FaultInjectingObjectStore::new(InMemory::new()));
        let storage: ArcDynStorage = Arc::new(Box::new(
            DynoStore::new("spike", 111, object_store.clone())
                .expect("dynostore")
                .advertised_listener(Url::parse("tcp://127.0.0.1/").expect("url"))
                .schemas(None)
                .lake(None),
        ));

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

        let transaction_id = alphanumeric_string(15);
        let producer = future::block_on(storage.init_producer(
            Some(&transaction_id),
            10_000,
            Some(-1),
            Some(-1),
        ))
        .expect("init producer");

        _ = future::block_on(
            storage.txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: [AddPartitionsToTxnTopic::default()
                    .name(topic.clone())
                    .partitions(Some([0, 1].into()))]
                .into(),
            }),
        )
        .expect("add partitions");

        for partition in 0..2 {
            let batch = inflated::Batch::builder()
                .record(Record::builder().value(Bytes::from_static(b"payload").into()))
                .attributes(BatchAttribute::default().transaction(true).into())
                .producer_id(producer.id)
                .producer_epoch(producer.epoch)
                .base_sequence(0)
                .build()
                .and_then(deflated::Batch::try_from)
                .expect("well-formed batch");

            _ = future::block_on(storage.produce(
                Some(&transaction_id),
                &Topition::new(topic.clone(), partition),
                batch,
            ))
            .expect("produce");
        }

        // Partition 0's marker lands, partition 1's fails: the transaction is
        // left in `PrepareCommit`.
        object_store.schedule_record_write(false);
        object_store.schedule_record_write(true);

        _ = future::block_on(storage.txn_end(&transaction_id, producer.id, producer.epoch, true))
            .expect_err("faulted marker write must surface as an error");

        let opposite =
            future::block_on(storage.txn_end(&transaction_id, producer.id, producer.epoch, false));
        assert_matches!(
            opposite,
            Err(nisshi_storage::Error::Api(ErrorCode::InvalidTxnState))
        );

        let retry =
            future::block_on(storage.txn_end(&transaction_id, producer.id, producer.epoch, true))
                .expect("commit retry");
        assert_eq!(retry, ErrorCode::None);
    });
}
