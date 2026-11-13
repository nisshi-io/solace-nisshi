// Copyright (c) Solace Corporation
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

//! A failed read of a topic's `meta.json` on the object store engine reaches the callers of
//! `Storage::metadata` as an error, so a Fetch answers `KAFKA_STORAGE_ERROR` in each partition at
//! once, instead of `UNKNOWN_TOPIC_OR_PARTITION`, which tells the client the topic is gone. A
//! topic that does not exist still answers `UNKNOWN_TOPIC_OR_PARTITION` in the response. The
//! Metadata service answers each named topic with the code of the error and keeps the connection
//! open, and the `validate_only` dry runs of CreateTopics and IncrementalAlterConfigs answer
//! with the code of the error.

use std::{
    assert_matches, fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use crate::common::init_tracing;
use async_trait::async_trait;
use futures::stream::BoxStream;
use nisshi_broker::Result;
use nisshi_sans_io::{
    ConfigResource, CreateTopicsRequest, ErrorCode, FetchRequest, IncrementalAlterConfigsRequest,
    IsolationLevel, MetadataRequest, NULL_TOPIC_ID, OpType, RequestInput,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
    metadata_request::MetadataRequestTopic,
};
use nisshi_storage::{
    ArcDynStorage, AutoCreateRefusals, CreateTopicsService, CreateTopicsValidationLayer, Error,
    FetchService, FetchValidationLayer, IncrementalAlterConfigsService, MetadataService,
    Storage as _, TopicCreationConfig, TopicId,
};
use nisshi_storage_dynostore::DynoStore;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use rama::{Layer as _, Service as _, extensions::Extensions};
use url::Url;

const CLUSTER: &str = "fetch-metadata-storage-error";
const TOPIC: &str = "fetch-metadata-storage-error";

/// Long enough that waiting it out fails the elapsed check.
const MAX_WAIT: Duration = Duration::from_secs(10);

/// Fails every read of the cluster's `meta.json` while armed, and passes everything else through.
#[derive(Debug)]
struct FailMetaReadStore {
    inner: InMemory,
    armed: AtomicBool,
    permission_denied: AtomicBool,
}

impl FailMetaReadStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            armed: AtomicBool::new(false),
            permission_denied: AtomicBool::new(false),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    /// Makes the armed fault an error that a retry does not fix.
    fn deny_permission(&self) {
        self.permission_denied.store(true, Ordering::SeqCst);
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
            let source = "injected fault: meta.json read failed".into();

            return Err(if self.permission_denied.load(Ordering::SeqCst) {
                object_store::Error::PermissionDenied {
                    path: location.to_string(),
                    source,
                }
            } else {
                object_store::Error::Generic {
                    store: "FailMetaReadStore",
                    source,
                }
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

fn dyno_store(object_store: Arc<FailMetaReadStore>) -> Result<ArcDynStorage> {
    Ok(Arc::new(Box::new(
        DynoStore::new(CLUSTER, 111, object_store)?
            .advertised_listener(Url::parse("tcp://127.0.0.1/")?)
            .schemas(None)
            .lake(None),
    )))
}

/// Returns a DynoStore on `object_store` that holds [`TOPIC`] of two partitions.
///
/// The topic is created through another DynoStore on the same object store. A DynoStore answers
/// a repeat read of `meta.json` from its cache for a few seconds, so only one that has not read
/// the file yet meets an armed fault.
async fn storage_with_topic(object_store: Arc<FailMetaReadStore>) -> Result<ArcDynStorage> {
    _ = dyno_store(object_store.clone())?
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(2)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    dyno_store(object_store)
}

/// Serves a Fetch of both partitions of the topic `name`, and returns each partition's error code
/// with how long the Fetch took.
async fn fetch(
    storage: &ArcDynStorage,
    name: &str,
    max_wait: Duration,
) -> Result<(Vec<ErrorCode>, Duration)> {
    let started_at = Instant::now();

    let response = FetchValidationLayer::new()
        .layer(FetchService {
            storage: storage.clone(),
        })
        .serve(RequestInput {
            request: FetchRequest::default()
                .max_wait_ms(i32::try_from(max_wait.as_millis())?)
                .min_bytes(1)
                .max_bytes(Some(1_048_576))
                .isolation_level(Some((&IsolationLevel::ReadUncommitted).into()))
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(name.into()))
                        .topic_id(Some(NULL_TOPIC_ID))
                        .partitions(Some(
                            (0..2)
                                .map(|partition| {
                                    FetchPartition::default()
                                        .partition(partition)
                                        .current_leader_epoch(Some(-1))
                                        .fetch_offset(0)
                                        .last_fetched_epoch(Some(-1))
                                        .log_start_offset(Some(-1))
                                        .partition_max_bytes(1_048_576)
                                })
                                .collect(),
                        ))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let elapsed = started_at.elapsed();

    let topics = response.responses.unwrap_or_default();
    assert_eq!(1, topics.len());

    let codes = topics[0]
        .partitions
        .iter()
        .flatten()
        .map(|partition| ErrorCode::try_from(partition.error_code))
        .collect::<Result<Vec<_>, _>>()?;

    Ok((codes, elapsed))
}

/// A failed read of `meta.json` answers each partition `KAFKA_STORAGE_ERROR` at once, and the
/// next Fetch reads the topic.
#[tokio::test]
async fn failed_meta_read_answers_storage_error_at_once() -> Result<()> {
    let _guard = init_tracing()?;

    let object_store = Arc::new(FailMetaReadStore::new());
    let storage = storage_with_topic(object_store.clone()).await?;

    object_store.arm();

    let (codes, elapsed) = fetch(&storage, TOPIC, MAX_WAIT).await?;

    assert_eq!(vec![ErrorCode::KafkaStorageError; 2], codes);
    assert!(elapsed < MAX_WAIT / 2, "{elapsed:?}");

    object_store.disarm();

    let (codes, _) = fetch(&storage, TOPIC, Duration::ZERO).await?;
    assert_eq!(vec![ErrorCode::None; 2], codes);

    Ok(())
}

/// A read error that a retry does not fix answers `UNKNOWN_SERVER_ERROR` at once, so the client
/// does not retry it forever.
#[tokio::test]
async fn denied_meta_read_answers_unknown_server_error_at_once() -> Result<()> {
    let _guard = init_tracing()?;

    let object_store = Arc::new(FailMetaReadStore::new());
    let storage = storage_with_topic(object_store.clone()).await?;

    object_store.deny_permission();
    object_store.arm();

    let (codes, elapsed) = fetch(&storage, TOPIC, MAX_WAIT).await?;

    assert_eq!(vec![ErrorCode::UnknownServerError; 2], codes);
    assert!(elapsed < MAX_WAIT / 2, "{elapsed:?}");

    Ok(())
}

/// A topic that does not exist still answers `UNKNOWN_TOPIC_OR_PARTITION` in the metadata, and
/// in each partition of a Fetch.
#[tokio::test]
async fn missing_topic_answers_unknown_topic() -> Result<()> {
    let _guard = init_tracing()?;

    let object_store = Arc::new(FailMetaReadStore::new());
    let storage = storage_with_topic(object_store).await?;

    let metadata = storage
        .metadata(Some(&[TopicId::Name("absent".into())]))
        .await?;
    assert_eq!(
        vec![i16::from(ErrorCode::UnknownTopicOrPartition)],
        metadata
            .topics()
            .iter()
            .map(|topic| topic.error_code)
            .collect::<Vec<_>>()
    );

    let (codes, _) = fetch(&storage, "absent", MAX_WAIT).await?;
    assert_eq!(vec![ErrorCode::UnknownTopicOrPartition; 2], codes);

    Ok(())
}

/// `Storage::metadata` for named topics returns the read error, as the listing of all topics
/// does, and answers again once the read works.
#[tokio::test]
async fn metadata_returns_the_read_error() -> Result<()> {
    let _guard = init_tracing()?;

    let object_store = Arc::new(FailMetaReadStore::new());
    let storage = storage_with_topic(object_store.clone()).await?;

    object_store.arm();

    assert_matches!(
        storage.metadata(Some(&[TopicId::Name(TOPIC.into())])).await,
        Err(Error::ObjectStore(_))
    );
    assert_matches!(storage.metadata(None).await, Err(Error::ObjectStore(_)));

    object_store.disarm();

    let metadata = storage
        .metadata(Some(&[TopicId::Name(TOPIC.into())]))
        .await?;
    assert_eq!(
        vec![i16::from(ErrorCode::None)],
        metadata
            .topics()
            .iter()
            .map(|topic| topic.error_code)
            .collect::<Vec<_>>()
    );

    Ok(())
}

/// Returns a DynoStore on an object store whose `meta.json` reads fail, with [`TOPIC`] stored.
///
/// The DynoStore has not read `meta.json` yet, so the fault reaches its first read. The fault is
/// a permission error, which a retry does not fix, when `permission_denied` is true, and an error
/// that a retry may fix otherwise.
async fn storage_failing_meta_reads(permission_denied: bool) -> Result<ArcDynStorage> {
    let object_store = Arc::new(FailMetaReadStore::new());
    let storage = storage_with_topic(object_store.clone()).await?;

    if permission_denied {
        object_store.deny_permission();
    }
    object_store.arm();

    Ok(storage)
}

fn metadata_service(storage: ArcDynStorage) -> MetadataService<ArcDynStorage> {
    MetadataService {
        storage,
        topic_creation: TopicCreationConfig::default(),
        auto_create_refusals: AutoCreateRefusals::default(),
    }
}

fn metadata_request(topics: Option<Vec<MetadataRequestTopic>>) -> RequestInput<MetadataRequest> {
    RequestInput {
        request: MetadataRequest::default()
            .allow_auto_topic_creation(Some(false))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false))
            .topics(topics),
        extensions: Extensions::default(),
    }
}

/// A request for a topic that does not exist keeps its topic-level `UNKNOWN_TOPIC_OR_PARTITION`.
#[tokio::test]
async fn metadata_service_answers_a_missing_topic_as_unknown() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = storage_with_topic(Arc::new(FailMetaReadStore::new())).await?;

    let response = metadata_service(storage)
        .serve(metadata_request(Some(vec![
            MetadataRequestTopic::default().name(Some("absent".into())),
        ])))
        .await?;

    assert_eq!(
        vec![i16::from(ErrorCode::UnknownTopicOrPartition)],
        response
            .topics
            .iter()
            .flatten()
            .map(|topic| topic.error_code)
            .collect::<Vec<_>>()
    );

    Ok(())
}

/// A failed read answers each named topic with the code of the error, and still lists the broker.
/// The service returns a response, so the broker keeps the connection open.
#[tokio::test]
async fn metadata_service_answers_a_failed_read_in_each_named_topic() -> Result<()> {
    let _guard = init_tracing()?;

    for (permission_denied, expected) in [
        (false, ErrorCode::KafkaStorageError),
        (true, ErrorCode::UnknownServerError),
    ] {
        let storage = storage_failing_meta_reads(permission_denied).await?;

        let response = metadata_service(storage)
            .serve(metadata_request(Some(vec![
                MetadataRequestTopic::default().name(Some(TOPIC.into())),
                MetadataRequestTopic::default().name(Some("absent".into())),
            ])))
            .await?;

        // The read of the first topic fails, so the whole request answers the error.
        let topics = response.topics.unwrap_or_default();
        assert_eq!(
            vec![
                (Some(TOPIC.to_owned()), i16::from(expected)),
                (Some("absent".to_owned()), i16::from(expected))
            ],
            topics
                .iter()
                .map(|topic| (topic.name.clone(), topic.error_code))
                .collect::<Vec<_>>(),
            "permission_denied = {permission_denied}"
        );
        assert_eq!(1, response.brokers.unwrap_or_default().len());
    }

    Ok(())
}

/// A topic that the client names by ID keeps its ID in the answer for a failed read, because a
/// client that cannot match an answer to a topic it asked for drops the answer.
#[tokio::test]
async fn metadata_service_echoes_the_topic_id_of_a_failed_read() -> Result<()> {
    let _guard = init_tracing()?;

    let id = uuid::Uuid::new_v4();

    let response = metadata_service(storage_failing_meta_reads(false).await?)
        .serve(metadata_request(Some(vec![
            MetadataRequestTopic::default().topic_id(Some(id.into_bytes())),
        ])))
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(Some(id.into_bytes()), topics[0].topic_id);
    assert_eq!(
        i16::from(ErrorCode::KafkaStorageError),
        topics[0].error_code
    );

    Ok(())
}

/// A request for all topics has no topic to answer, so the service returns the error.
#[tokio::test]
async fn metadata_service_returns_the_error_of_a_failed_read_of_all_topics() -> Result<()> {
    let _guard = init_tracing()?;

    assert_matches!(
        metadata_service(storage_failing_meta_reads(false).await?)
            .serve(metadata_request(None))
            .await,
        Err(Error::ObjectStore(_))
    );

    Ok(())
}

/// Serves a `validate_only` CreateTopics for a new topic on `storage`, and returns its error code.
async fn create_topics_dry_run(storage: ArcDynStorage) -> Result<ErrorCode> {
    let response = CreateTopicsValidationLayer::new()
        .layer(CreateTopicsService { storage })
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name("dry-run-topic".into())
                        .num_partitions(1)
                        .replication_factor(1)
                        .assignments(Some([].into()))
                        .configs(Some([].into())),
                ]))
                .validate_only(Some(true)),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());

    Ok(ErrorCode::try_from(topics[0].error_code)?)
}

/// A CreateTopics dry run whose read of the topic fails answers with the code of the error: the
/// retriable `NOT_CONTROLLER` when a retry may fix it, and `UNKNOWN_SERVER_ERROR` when not.
#[tokio::test]
async fn create_topics_dry_run_answers_the_code_of_a_failed_read() -> Result<()> {
    let _guard = init_tracing()?;

    assert_eq!(
        ErrorCode::NotController,
        create_topics_dry_run(storage_failing_meta_reads(false).await?).await?
    );
    assert_eq!(
        ErrorCode::UnknownServerError,
        create_topics_dry_run(storage_failing_meta_reads(true).await?).await?
    );

    Ok(())
}

/// Serves a `validate_only` IncrementalAlterConfigs for [`TOPIC`] on `storage`, and returns the
/// error code of its resource.
async fn alter_configs_dry_run(storage: ArcDynStorage) -> Result<ErrorCode> {
    let response = IncrementalAlterConfigsService { storage }
        .serve(RequestInput {
            request: IncrementalAlterConfigsRequest::default()
                .resources(Some(vec![
                    AlterConfigsResource::default()
                        .resource_name(TOPIC.into())
                        .resource_type(ConfigResource::Topic.into())
                        .configs(Some(vec![
                            AlterableConfig::default()
                                .config_operation(OpType::Set.into())
                                .name("retention.ms".into())
                                .value(Some("1000".into())),
                        ])),
                ]))
                .validate_only(true),
            extensions: Extensions::default(),
        })
        .await?;

    let responses = response.responses.unwrap_or_default();
    assert_eq!(1, responses.len());

    Ok(ErrorCode::try_from(responses[0].error_code)?)
}

/// An IncrementalAlterConfigs dry run whose read of the topic fails answers with the code of the
/// error: the retriable `REQUEST_TIMED_OUT` when a retry may fix it, and `UNKNOWN_SERVER_ERROR`
/// when not.
#[tokio::test]
async fn incremental_alter_configs_dry_run_answers_the_code_of_a_failed_read() -> Result<()> {
    let _guard = init_tracing()?;

    assert_eq!(
        ErrorCode::RequestTimedOut,
        alter_configs_dry_run(storage_failing_meta_reads(false).await?).await?
    );
    assert_eq!(
        ErrorCode::UnknownServerError,
        alter_configs_dry_run(storage_failing_meta_reads(true).await?).await?
    );

    Ok(())
}
