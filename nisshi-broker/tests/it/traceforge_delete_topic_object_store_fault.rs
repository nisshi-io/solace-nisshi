// Copyright ⓒ Solace Corporation
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
//! `object_store::ObjectStore` layer under `delete_topic`
//! (`nisshi-storage-dynostore/src/dynostore.rs:642-712`), the same
//! technique as `traceforge_produce_object_store_fault.rs` applied to a
//! non-transactional API (per direction to leave the transaction bugs
//! alone and check other APIs).
//!
//! `delete_topic` removes the topic from `meta.topics` in one durably
//! committed write, *then* runs two separate `delete_stream` calls: one
//! deleting the topic's partition record objects, one deleting its
//! consumer-group offset objects. If the first `delete_stream` fails,
//! `delete_topic` returns an error - but the metadata removal already
//! landed, so `topic_metadata` now returns `None` for this topic. A retry
//! hits the `if let Some(metadata) = self.topic_metadata(topic).await?`
//! guard (`dynostore.rs:643`), finds nothing, and returns
//! `UnknownTopicOrPartition` without ever retrying the delete.
//!
//! # Bug found and fixed
//!
//! This was worse than an orphaned-but-harmless leftover object. Storage
//! paths are keyed by cluster + topic *name*, not a topic UUID
//! (`clusters/{cluster}/topics/{topic}/partitions/{partition}/records/
//! {offset}.batch`). A topic recreated with the same name resets its
//! watermark to 0 but does not - because it doesn't know anything was ever
//! there - delete the orphaned record left behind at that same path. The
//! recreated topic's own first produce to that partition also lands at
//! offset 0, uses `PutMode::Create`, and collides with the orphaned
//! object: **the recreated topic could never produce to that partition
//! again**, permanently, confirmed via `println!`-traced reproduction:
//! `produce to recreated topic = Err(ObjectStore(AlreadyExists { path:
//! ".../partitions/0000000000/records/00000000000000000000.batch", .. }))`.
//!
//! This is the same structural pattern as the two transaction bugs -
//! commit a state transition durably, then do more non-atomic work, with
//! the retry path keyed off "does metadata for this still exist" rather
//! than "did the cleanup actually finish" - just in a different API.
//!
//! Fixed by moving the metadata removal to *last*, only after both
//! `delete_stream` cleanups succeed, so a transient failure leaves the
//! topic's metadata in place and a retry finds it again and finishes the
//! job (`delete_stream` over an already-emptied prefix is a no-op, so
//! retrying is safe). The trade-off: a concurrent `produce()` in the
//! now-larger window before metadata removal succeeds against a
//! mid-delete topic, where before it failed fast - judged acceptable for
//! closing off a permanent, unrecoverable outage.
//!
//! The exhaustive test below explores `nondet()` over which of the two
//! `delete_stream` calls (if either) fails, asserting a freshly (re)created
//! topic must always be able to produce to a fresh partition, and now
//! passes on all `3` combinations. Faulting only the second
//! `delete_stream` (consumer offsets, a real but lesser leak that doesn't
//! block production) never triggered the failure in the first place.

#![cfg(feature = "dynostore")]

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use nisshi_sans_io::{
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{ArcDynStorage, Storage, TopicId, Topition};
use nisshi_storage_dynostore::DynoStore;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use traceforge::{Config, Nondet, cover, future, verify};
use url::Url;

#[derive(Debug)]
struct FaultInjectingObjectStore<O> {
    inner: O,
    delete_stream_faults: Mutex<VecDeque<bool>>,
}

impl<O> FaultInjectingObjectStore<O> {
    fn new(inner: O) -> Self {
        Self {
            inner,
            delete_stream_faults: Mutex::new(VecDeque::new()),
        }
    }

    fn schedule_delete_stream(&self, should_fail: bool) {
        self.delete_stream_faults
            .lock()
            .expect("delete_stream_faults")
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
        let should_fail = self
            .delete_stream_faults
            .lock()
            .expect("delete_stream_faults")
            .pop_front()
            .unwrap_or(false);

        if should_fail {
            return stream::once(async {
                Err(object_store::Error::Generic {
                    store: "FaultInjectingObjectStore",
                    source: "injected fault: delete_stream failed".into(),
                })
            })
            .boxed();
        }

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

fn topic_with_one_partition(name: &str) -> CreatableTopic {
    CreatableTopic::default()
        .name(name.into())
        .num_partitions(1)
        .replication_factor(1)
        .assignments(Some([].into()))
        .configs(Some([].into()))
}

#[test]
fn delete_topic_partial_failure_can_permanently_block_recreated_topic() {
    let stats = verify(Config::builder().build(), || {
        let object_store = Arc::new(FaultInjectingObjectStore::new(InMemory::new()));
        let storage: ArcDynStorage = Arc::new(Box::new(
            DynoStore::new("spike", 111, object_store.clone())
                .expect("dynostore")
                .advertised_listener(Url::parse("tcp://127.0.0.1/").expect("url"))
                .schemas(None)
                .lake(None),
        ));

        let topic = "delete-topic-fault".to_string();
        _ = future::block_on(storage.create_topic(topic_with_one_partition(&topic), false))
            .expect("create topic");

        let topition = Topition::new(topic.clone(), 0);

        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"original-data").into()))
            .build()
            .and_then(deflated::Batch::try_from)
            .expect("well-formed batch");
        _ = future::block_on(storage.produce(None, &topition, batch)).expect("produce");

        // `delete_topic` calls `delete_stream` twice: once for the
        // partition's record objects, once for consumer-group offsets.
        // `nondet()` picks whether neither, the first, or the second
        // fails.
        let fault_slot = (0..3).nondet();
        match fault_slot {
            0 => {}
            1 => object_store.schedule_delete_stream(true),
            2 => {
                object_store.schedule_delete_stream(false);
                object_store.schedule_delete_stream(true);
            }
            _ => unreachable!(),
        }

        let attempt1 = future::block_on(storage.delete_topic(&TopicId::Name(topic.clone())));

        if fault_slot == 0 {
            _ = attempt1.expect("undisturbed delete_topic must succeed");
            cover!("no_fault");
        } else {
            assert!(
                attempt1.is_err(),
                "the faulted delete_stream must surface as an error"
            );
            cover!(if fault_slot == 1 {
                "records_delete_faulted"
            } else {
                "offsets_delete_faulted"
            });

            // The client retries, as it must on an ambiguous error.
            _ = future::block_on(storage.delete_topic(&TopicId::Name(topic.clone())))
                .expect("delete_topic retry");
        }

        // An operator recreating the topic with the same name (an
        // ordinary thing to do once told the old one is gone) must be
        // able to produce to it.
        _ = future::block_on(storage.create_topic(topic_with_one_partition(&topic), false))
            .expect("recreate topic");

        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"new-data").into()))
            .build()
            .and_then(deflated::Batch::try_from)
            .expect("well-formed batch");

        // Regression guard for the fixed bug: when the *records*
        // delete_stream was the one that faulted, the old record's path
        // was still occupied, and this used to fail with `AlreadyExists`
        // even though the topic was just freshly created.
        _ = future::block_on(storage.produce(None, &topition, batch))
            .expect("a freshly (re)created topic must accept produces to a fresh partition");

        cover!("recreated_topic_accepts_produce");
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
