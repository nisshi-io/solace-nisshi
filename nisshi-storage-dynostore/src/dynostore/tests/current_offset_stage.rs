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

//! `current_offset_stage` answers from the `watermark.json` and `meta.json` that it read,
//! and reads `meta.json` from the store only when the high watermark has moved.

use std::{
    fmt::{self, Debug, Display},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    FutureExt as _, StreamExt as _,
    future::BoxFuture,
    stream::{self, BoxStream},
};
use nisshi_sans_io::{
    ErrorCode, FetchRequest, IsolationLevel, RequestInput,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
};
use nisshi_storage::{
    Error, FetchService, FetchValidationLayer, Storage as _, Topition, TxnAddPartitionsRequest,
};
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    memory::InMemory, path::Path,
};
use rama::{Layer as _, Service as _, extensions::Extensions};

use crate::dynostore::{
    DynoStore,
    tests::{
        init_tracing,
        transaction_last_stable::{plain_batch, transactional_batch},
    },
};

type Hook = Box<dyn FnOnce() -> BoxFuture<'static, Result<(), Error>> + Send>;

/// Counts the GETs of `watermark.json` and `meta.json` that carry no `If-None-Match`, which
/// are the ones that reach the store, and runs the hook once, before the first such GET of
/// `meta.json`.
///
/// It can also keep the next `watermark.json` that a GET returns, and answer the next GET of
/// `watermark.json` with that document in place of the current one, as a read that was issued
/// before a write and returns after it does.
#[derive(Clone)]
struct ObservedStore {
    object_store: Arc<InMemory>,
    unconditional_watermark_gets: Arc<AtomicUsize>,
    unconditional_meta_gets: Arc<AtomicUsize>,
    before_meta_get: Arc<Mutex<Option<Hook>>>,
    keep_next_watermark: Arc<AtomicBool>,
    kept_watermark: Arc<Mutex<Option<(Bytes, ObjectMeta)>>>,
    replay_kept_watermark: Arc<AtomicBool>,
}

impl ObservedStore {
    fn new() -> Self {
        Self {
            object_store: Arc::new(InMemory::new()),
            unconditional_watermark_gets: Arc::new(AtomicUsize::new(0)),
            unconditional_meta_gets: Arc::new(AtomicUsize::new(0)),
            before_meta_get: Arc::new(Mutex::new(None)),
            keep_next_watermark: Arc::new(AtomicBool::new(false)),
            kept_watermark: Arc::new(Mutex::new(None)),
            replay_kept_watermark: Arc::new(AtomicBool::new(false)),
        }
    }

    fn keep_next_watermark(&self) {
        self.keep_next_watermark.store(true, Ordering::SeqCst);
    }

    fn replay_kept_watermark(&self) {
        self.replay_kept_watermark.store(true, Ordering::SeqCst);
    }

    fn reset_counts(&self) {
        self.unconditional_watermark_gets.store(0, Ordering::SeqCst);
        self.unconditional_meta_gets.store(0, Ordering::SeqCst);
    }

    fn watermark_gets(&self) -> usize {
        self.unconditional_watermark_gets.load(Ordering::SeqCst)
    }

    fn meta_gets(&self) -> usize {
        self.unconditional_meta_gets.load(Ordering::SeqCst)
    }
}

impl Debug for ObservedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservedStore").finish()
    }
}

impl Display for ObservedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObservedStore").finish()
    }
}

#[async_trait]
impl ObjectStore for ObservedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        self.object_store.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.object_store.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        if options.if_none_match.is_none() {
            if location.as_ref().ends_with("watermark.json") {
                _ = self
                    .unconditional_watermark_gets
                    .fetch_add(1, Ordering::SeqCst);
            } else if location.as_ref().ends_with("meta.json") {
                _ = self.unconditional_meta_gets.fetch_add(1, Ordering::SeqCst);

                let hook = self
                    .before_meta_get
                    .lock()
                    .ok()
                    .and_then(|mut hook| hook.take());

                if let Some(hook) = hook {
                    hook().await.map_err(|error| object_store::Error::Generic {
                        store: "test",
                        source: error.to_string().into(),
                    })?;
                }
            }
        }

        if location.as_ref().ends_with("watermark.json") {
            if self.replay_kept_watermark.swap(false, Ordering::SeqCst)
                && let Some((bytes, meta)) = self
                    .kept_watermark
                    .lock()
                    .ok()
                    .and_then(|kept| kept.clone())
            {
                return Ok(get_result(bytes, meta));
            }

            if self.keep_next_watermark.swap(false, Ordering::SeqCst) {
                let result = self.object_store.get_opts(location, options).await?;
                let meta = result.meta.clone();
                let bytes = result.bytes().await?;

                if let Ok(mut kept) = self.kept_watermark.lock() {
                    *kept = Some((bytes.clone(), meta.clone()));
                }

                return Ok(get_result(bytes, meta));
            }
        }

        self.object_store.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.object_store.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.object_store.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.object_store.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.object_store.copy_opts(from, to, opts).await
    }
}

/// Returns a GET result that holds `bytes` as the object described by `meta`.
fn get_result(bytes: Bytes, meta: ObjectMeta) -> GetResult {
    GetResult {
        range: 0..bytes.len() as u64,
        payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
        meta,
        attributes: Attributes::default(),
        extensions: Default::default(),
    }
}

const TOPIC: &str = "abc";

/// Broker A writes, and broker B reads. Both share one bucket.
async fn brokers(object_store: ObservedStore) -> Result<(DynoStore, DynoStore, Topition), Error> {
    let a = DynoStore::new("nisshi", 1, object_store.clone())?;
    let b = DynoStore::new("nisshi", 2, object_store)?;

    _ = a
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

    Ok((a, b, Topition::new(TOPIC, 0)))
}

#[tokio::test]
async fn answers_from_the_watermark_it_read_when_a_concurrent_read_holds_an_older_one()
-> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = ObservedStore::new();
    let (a, b, topition) = brokers(object_store.clone()).await?;

    assert_eq!(0, a.produce(None, &topition, plain_batch()?).await?);

    // The store keeps the document that broker B reads here, which holds the high watermark of
    // 1, so that the hook can return it again after A's writes.
    object_store.keep_next_watermark();
    assert_eq!(1, b.offset_stage(&topition).await?.high_watermark());

    assert_eq!(1, a.produce(None, &topition, plain_batch()?).await?);
    assert_eq!(2, a.produce(None, &topition, plain_batch()?).await?);

    // While `current_offset_stage` waits for `meta.json`, another read of broker B returns the
    // document from before A's writes. That read replaces the document that the `OptiCon` kept,
    // and the version that the metadata cache holds, as a read that was issued before A's
    // writes and returns after them does.
    object_store.reset_counts();
    let watermark = b.watermark_for(&topition)?;
    let cache = b.object_store.clone();
    let replayed = object_store.clone();
    *object_store.before_meta_get.lock()? = Some(Box::new(move || {
        async move {
            replayed.replay_kept_watermark();
            watermark.get(&cache).await.map(|_| ())
        }
        .boxed()
    }));

    let stage = b.current_offset_stage(&topition).await?;

    // This stops the test from passing when the hook never ran.
    assert_eq!(1, object_store.meta_gets());

    assert_eq!(3, stage.high_watermark());

    Ok(())
}

/// Broker A opens a transaction, and a call on broker B reads the high watermark that the
/// transaction raised and then fails before it has read `meta.json`. The next call must read
/// `meta.json`, because the cached one is from before the transaction.
#[tokio::test]
async fn a_failed_call_does_not_make_the_next_one_answer_from_an_older_meta() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = ObservedStore::new();
    let (a, b, topition) = brokers(object_store.clone()).await?;

    assert_eq!(0, a.produce(None, &topition, plain_batch()?).await?);
    assert_eq!(1, a.produce(None, &topition, plain_batch()?).await?);

    // broker B holds the high watermark of 2 and the `meta.json` from before the transaction
    assert_eq!(2, b.offset_stage(&topition).await?.high_watermark());

    let producer = a
        .init_producer(Some("txn"), 10_000, Some(-1), Some(-1))
        .await?;
    assert_eq!(ErrorCode::None, producer.error);

    _ = a
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: "txn".into(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: vec![
                AddPartitionsToTxnTopic::default()
                    .name(TOPIC.into())
                    .partitions(Some(vec![0])),
            ],
        })
        .await?;

    // the transaction's 3 records take offsets 2 to 4
    assert_eq!(
        2,
        a.produce(
            Some("txn"),
            &topition,
            transactional_batch(producer.id, producer.epoch, 0)?
        )
        .await?
    );

    object_store.reset_counts();
    *object_store.before_meta_get.lock()? = Some(Box::new(|| {
        async { Err(Error::Message("the store is unavailable".into())) }.boxed()
    }));

    assert!(b.current_offset_stage(&topition).await.is_err());

    // This stops the test from passing when the hook never ran.
    assert_eq!(1, object_store.meta_gets());

    let stage = b.current_offset_stage(&topition).await?;

    assert_eq!(5, stage.high_watermark());
    assert_eq!(2, stage.last_stable());
    assert_eq!(2, object_store.meta_gets());

    Ok(())
}

#[tokio::test]
async fn reads_meta_only_when_the_high_watermark_moved() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = ObservedStore::new();
    let (a, b, topition) = brokers(object_store.clone()).await?;

    assert_eq!(0, a.produce(None, &topition, plain_batch()?).await?);
    assert_eq!(1, b.offset_stage(&topition).await?.high_watermark());

    object_store.reset_counts();

    let stage = b.current_offset_stage(&topition).await?;
    assert_eq!(1, stage.high_watermark());
    assert_eq!(1, object_store.watermark_gets());
    assert_eq!(0, object_store.meta_gets());

    assert_eq!(1, a.produce(None, &topition, plain_batch()?).await?);
    object_store.reset_counts();

    let stage = b.current_offset_stage(&topition).await?;
    assert_eq!(2, stage.high_watermark());
    assert_eq!(1, object_store.watermark_gets());
    assert_eq!(1, object_store.meta_gets());

    Ok(())
}

/// Broker A opens a transaction and produces to the partition. Broker B still caches the high
/// watermark and `meta.json` from before, and a read committed consumer fetches an offset
/// inside the transaction, which is above that high watermark. The answer is in range, and
/// holds the consumer at the transaction's first offset.
#[tokio::test]
async fn read_committed_fetch_ahead_of_a_stale_high_watermark_stops_at_an_open_transaction()
-> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = ObservedStore::new();
    let (a, b, topition) = brokers(object_store).await?;

    assert_eq!(0, a.produce(None, &topition, plain_batch()?).await?);
    assert_eq!(1, a.produce(None, &topition, plain_batch()?).await?);

    assert_eq!(2, b.offset_stage(&topition).await?.high_watermark());

    let producer = a
        .init_producer(Some("txn"), 10_000, Some(-1), Some(-1))
        .await?;
    assert_eq!(ErrorCode::None, producer.error);

    _ = a
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: "txn".into(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: vec![
                AddPartitionsToTxnTopic::default()
                    .name(TOPIC.into())
                    .partitions(Some(vec![0])),
            ],
        })
        .await?;

    // the transaction's 3 records take offsets 2 to 4
    assert_eq!(
        2,
        a.produce(
            Some("txn"),
            &topition,
            transactional_batch(producer.id, producer.epoch, 0)?
        )
        .await?
    );

    // broker B has not seen any of it
    assert_eq!(2, b.offset_stage(&topition).await?.high_watermark());

    let service = FetchValidationLayer::new().layer(FetchService { storage: b });

    let response = service
        .serve(RequestInput {
            request: FetchRequest::default()
                .max_wait_ms(100)
                .min_bytes(1)
                .max_bytes(Some(50 * 1024))
                .isolation_level(Some(IsolationLevel::ReadCommitted.into()))
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(TOPIC.into()))
                        .partitions(Some(
                            [FetchPartition::default()
                                .partition(0)
                                .fetch_offset(3)
                                .partition_max_bytes(50 * 1024)]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let partition = response
        .responses
        .and_then(|topics| topics.into_iter().next())
        .and_then(|topic| topic.partitions)
        .and_then(|partitions| partitions.into_iter().next())
        .ok_or_else(|| Error::Message("the response has no partition".into()))?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
    assert_eq!(5, partition.high_watermark);
    assert_eq!(Some(2), partition.last_stable_offset);
    assert!(partition.records.is_none());

    Ok(())
}
