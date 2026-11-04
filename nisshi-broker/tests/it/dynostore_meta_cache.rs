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

//! The object store engine caches `meta.json` and each partition's `watermark.json` in the
//! process. A read that started before a write and finished after it must not put the older
//! version back in that cache, or every later read gets the older version until the next write.

use std::{
    fmt,
    sync::{Arc, Mutex},
};

use crate::common::init_tracing;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use nisshi_broker::Result;
use nisshi_sans_io::{
    CreateTopicsRequest, ErrorCode, InitProducerIdRequest, IsolationLevel, ListOffset,
    MetadataRequest, ProduceRequest, RequestInput,
    create_topics_request::CreatableTopic,
    metadata_request::MetadataRequestTopic,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{
        Record,
        deflated::{self, Frame},
        inflated,
    },
};
use nisshi_storage::{
    ArcDynStorage, AutoCreateRefusals, CreateTopicsService, CreateTopicsValidationLayer,
    InitProducerIdService, MetadataService, ProduceService, Storage as _, TopicCreationConfig,
    Topition,
};
use nisshi_storage_dynostore::DynoStore;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use rama::{Layer as _, Service as _, extensions::Extensions};
use tokio::sync::Notify;
use url::Url;

const CLUSTER: &str = "meta-cache";
const TOPIC: &str = "meta-cache";

/// Holds back the response of one read of an object whose path ends with the armed suffix,
/// after the read, until [`GateStore::release`]. Everything else passes through.
#[derive(Debug)]
struct GateStore {
    inner: Arc<InMemory>,
    armed: Mutex<Option<&'static str>>,
    reached: Notify,
    released: Notify,
}

impl GateStore {
    fn new(inner: Arc<InMemory>) -> Self {
        Self {
            inner,
            armed: Mutex::new(None),
            reached: Notify::new(),
            released: Notify::new(),
        }
    }

    fn hold_next_read_of(&self, suffix: &'static str) {
        _ = self.armed.lock().expect("armed").replace(suffix);
    }

    fn release(&self) {
        self.released.notify_one();
    }
}

impl fmt::Display for GateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GateStore")
    }
}

#[async_trait]
impl ObjectStore for GateStore {
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
        let result = self.inner.get_opts(location, options).await;

        let hold = {
            let mut armed = self.armed.lock().expect("armed");
            armed
                .take_if(|suffix| location.as_ref().ends_with(*suffix))
                .is_some()
        };

        if hold {
            self.reached.notify_one();
            self.released.notified().await;
        }

        result
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

fn storage(object_store: impl ObjectStore) -> Result<ArcDynStorage> {
    Ok(Arc::new(Box::new(
        DynoStore::new(CLUSTER, 111, object_store)?
            .advertised_listener(Url::parse("tcp://127.0.0.1/")?)
            .schemas(None)
            .lake(None),
    )))
}

async fn create_topic(storage: ArcDynStorage) -> Result<()> {
    let created = CreateTopicsValidationLayer::new()
        .layer(CreateTopicsService { storage })
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .validate_only(Some(false))
                .topics(Some(
                    [CreatableTopic::default()
                        .name(TOPIC.into())
                        .num_partitions(1)
                        .replication_factor(1)
                        .assignments(Some([].into()))
                        .configs(Some([].into()))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = created.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
    Ok(())
}

async fn metadata(storage: ArcDynStorage) -> Result<()> {
    _ = MetadataService {
        storage,
        topic_creation: TopicCreationConfig::default(),
        auto_create_refusals: AutoCreateRefusals::default(),
    }
    .serve(RequestInput {
        request: MetadataRequest::default()
            .allow_auto_topic_creation(Some(false))
            .topics(Some(vec![
                MetadataRequestTopic::default().name(Some(TOPIC.into())),
            ])),
        extensions: Extensions::default(),
    })
    .await?;
    Ok(())
}

async fn init_producer_id(storage: ArcDynStorage) -> Result<(i64, i16)> {
    let response = InitProducerIdService { storage }
        .serve(RequestInput {
            request: InitProducerIdRequest::default()
                .transactional_id(None)
                .transaction_timeout_ms(0)
                .producer_id(Some(-1))
                .producer_epoch(Some(-1)),
            extensions: Extensions::default(),
        })
        .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
    Ok((response.producer_id, response.producer_epoch))
}

async fn produce(
    storage: ArcDynStorage,
    producer_id: i64,
    producer_epoch: i16,
) -> Result<ErrorCode> {
    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(b"payload").into()))
        .producer_id(producer_id)
        .producer_epoch(producer_epoch)
        .base_sequence(0)
        .build()
        .and_then(deflated::Batch::try_from)?;

    let response = ProduceService::new(storage)
        .with_validation()
        .serve(RequestInput {
            request: ProduceRequest::default().acks(1).topic_data(Some(
                [TopicProduceData::default()
                    .name(TOPIC.into())
                    .partition_data(Some(
                        [PartitionProduceData::default()
                            .index(0)
                            .records(Some(Frame {
                                batches: vec![batch],
                            }))]
                        .into(),
                    ))]
                .into(),
            )),
            extensions: Extensions::default(),
        })
        .await?;

    Ok(ErrorCode::try_from(
        response.responses.unwrap_or_default()[0]
            .partition_responses
            .clone()
            .unwrap_or_default()[0]
            .error_code,
    )?)
}

/// A read of `meta.json` gets the version before an `InitProducerId`, and returns after
/// `InitProducerId` has written the next version. The first produce of the new producer must
/// still find its producer ID.
///
/// A second engine over the same object store starts with an empty cache, so its first read of
/// `meta.json` goes to the store. In a running broker the same read happens when a write has
/// stored its version in the cache, but has not yet stored its data.
#[tokio::test]
async fn read_that_returns_after_a_write_does_not_hide_the_write() -> Result<()> {
    let _guard = init_tracing()?;

    let inner = Arc::new(InMemory::new());

    create_topic(storage(Arc::new(GateStore::new(inner.clone())))?).await?;

    let gate = Arc::new(GateStore::new(inner));
    let storage = storage(gate.clone())?;

    gate.hold_next_read_of("/meta.json");
    let reader = tokio::spawn(metadata(storage.clone()));
    gate.reached.notified().await;

    let (producer_id, producer_epoch) = init_producer_id(storage.clone()).await?;

    gate.release();
    reader.await.expect("reader")?;

    assert_eq!(
        ErrorCode::None,
        produce(storage, producer_id, producer_epoch).await?
    );

    Ok(())
}

/// A read of a partition's watermark gets the watermark before a produce, and returns after the
/// produce has written the next watermark. A later `ListOffsets(Latest)` must still include the
/// produced batch.
#[tokio::test]
async fn watermark_read_that_returns_after_a_produce_does_not_hide_the_produce() -> Result<()> {
    let _guard = init_tracing()?;

    let inner = Arc::new(InMemory::new());

    let first = storage(Arc::new(GateStore::new(inner.clone())))?;
    create_topic(first.clone()).await?;
    assert_eq!(ErrorCode::None, produce(first, -1, -1).await?);

    let gate = Arc::new(GateStore::new(inner));
    let storage = storage(gate.clone())?;

    let latest = |storage: ArcDynStorage| async move {
        storage
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[(Topition::new(TOPIC, 0), ListOffset::Latest)],
            )
            .await
            .map(|offsets| offsets[0].1.offset())
    };

    gate.hold_next_read_of("/watermark.json");
    let reader = tokio::spawn(latest(storage.clone()));
    gate.reached.notified().await;

    assert_eq!(ErrorCode::None, produce(storage.clone(), -1, -1).await?);

    gate.release();

    // The held read began before the produce, so it may see the older watermark.
    assert_eq!(Some(1), reader.await.expect("reader")?);

    assert_eq!(Some(2), latest(storage).await?);

    Ok(())
}
