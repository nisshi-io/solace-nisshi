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

//! The error code that Produce and DescribeConfigs answer when the object store engine can't
//! read a topic's config. Kafka answers a failed log read with `KAFKA_STORAGE_ERROR`, which
//! clients retry, so a failed read of the topic's metadata answers that code too.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::common::init_tracing;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use nisshi_broker::Result;
use nisshi_sans_io::{
    ConfigResource, CreateTopicsRequest, DescribeConfigsRequest, ErrorCode, ProduceRequest,
    RequestInput,
    create_topics_request::CreatableTopic,
    describe_configs_request::DescribeConfigsResource,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{
        Record,
        deflated::{self, Frame},
        inflated,
    },
};
use nisshi_storage::{
    ArcDynStorage, CreateTopicsService, CreateTopicsValidationLayer, DescribeConfigsService,
    ProduceService,
};
use nisshi_storage_dynostore::DynoStore;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use rama::{Layer as _, Service as _, extensions::Extensions};
use url::Url;

const CLUSTER: &str = "topic-config-read-failed";
const TOPIC: &str = "topic-config-read-failed";

/// Fails each read of `meta.json`, which holds each topic's config, while it is armed, and
/// passes everything else through to a shared [`InMemory`] store.
#[derive(Debug)]
struct FailMetaReadStore {
    inner: InMemory,
    armed: AtomicBool,
}

impl FailMetaReadStore {
    fn new(inner: InMemory) -> Self {
        Self {
            inner,
            armed: AtomicBool::new(true),
        }
    }

    fn disarm(&self) {
        self.armed.store(false, Ordering::SeqCst);
    }
}

impl fmt::Display for FailMetaReadStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailMetaReadStore")
    }
}

#[async_trait]
impl ObjectStore for FailMetaReadStore {
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
        if location.as_ref().ends_with("/meta.json") && self.armed.load(Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "FailMetaReadStore",
                source: "injected fault: metadata read failed".into(),
            });
        }

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

/// Creates the topic through one engine, and returns a second engine over the same objects,
/// whose reads of `meta.json` fail until [`FailMetaReadStore::disarm`]. Each engine caches the
/// objects it reads for 5 s, so the second engine starts with an empty cache, and its first
/// read reaches the object store.
async fn failing_storage() -> Result<(ArcDynStorage, Arc<FailMetaReadStore>)> {
    let objects = InMemory::new();
    create_topic(storage(objects.clone())?).await?;

    let failing = Arc::new(FailMetaReadStore::new(objects));
    Ok((storage(failing.clone())?, failing))
}

async fn produce(storage: ArcDynStorage) -> Result<ErrorCode> {
    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(b"payload").into()))
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

/// The topic gets `KAFKA_STORAGE_ERROR`, and the broker resource after it still gets an answer.
#[tokio::test]
async fn describe_configs_answers_kafka_storage_error() -> Result<()> {
    let _guard = init_tracing()?;

    let (storage, _) = failing_storage().await?;

    let response = DescribeConfigsService { storage }
        .serve(RequestInput {
            request: DescribeConfigsRequest::default()
                .include_documentation(Some(false))
                .include_synonyms(Some(false))
                .resources(Some(
                    [
                        DescribeConfigsResource::default()
                            .resource_name(TOPIC.into())
                            .resource_type(ConfigResource::Topic.into())
                            .configuration_keys(None),
                        DescribeConfigsResource::default()
                            .resource_name("".into())
                            .resource_type(ConfigResource::Broker.into())
                            .configuration_keys(None),
                    ]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let results = response.results.unwrap_or_default();
    assert_eq!(2, results.len());

    assert_eq!(
        ErrorCode::KafkaStorageError,
        ErrorCode::try_from(results[0].error_code)?
    );
    assert_eq!(TOPIC, results[0].resource_name);
    assert_eq!(i8::from(ConfigResource::Topic), results[0].resource_type);
    assert_eq!(Some(vec![]), results[0].configs);

    assert_eq!(ErrorCode::None, ErrorCode::try_from(results[1].error_code)?);
    assert_eq!(i8::from(ConfigResource::Broker), results[1].resource_type);

    Ok(())
}

#[tokio::test]
async fn produce_answers_kafka_storage_error_until_the_read_succeeds() -> Result<()> {
    let _guard = init_tracing()?;

    let (storage, failing) = failing_storage().await?;

    assert_eq!(
        ErrorCode::KafkaStorageError,
        produce(storage.clone()).await?
    );

    failing.disarm();

    assert_eq!(ErrorCode::None, produce(storage).await?);

    Ok(())
}
