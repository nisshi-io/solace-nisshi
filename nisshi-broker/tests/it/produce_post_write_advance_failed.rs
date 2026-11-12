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

//! An idempotent produce on the object store engine writes the record, and
//! only then advances the producer's sequence in `meta.json`. If that advance
//! fails the batch is stored while the client is told it failed, so
//! `nisshi_produce_post_write_advance_failed` counts every such failure, with
//! the `reason` it failed for. The client's retry of the same sequence would
//! store the batch a second time, so the answer is a code that it does not
//! retry. A record write that fails after it completed gets the same answer.
//!
//! The metric is read from a process-wide meter provider, which has to be
//! installed before the first instrument is created. nextest runs each test in
//! a process of its own, so that holds here; `cargo test` would share one
//! process across the whole `it` binary and could create the instrument first.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::common::init_tracing;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, TryStreamExt as _};
use nisshi_broker::Result;
use nisshi_sans_io::{
    CreateTopicsRequest, ErrorCode, InitProducerIdRequest, ProduceRequest, RequestInput,
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
    ProduceService,
};
use nisshi_storage_dynostore::DynoStore;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use opentelemetry::global;
use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
    data::{AggregatedMetrics, MetricData},
};
use rama::{Layer as _, Service as _, extensions::Extensions};
use url::Url;

const CLUSTER: &str = "post-write-advance-failed";
const TOPIC: &str = "post-write-advance-failed";
const COUNTER: &str = "nisshi_produce_post_write_advance_failed";

/// Fails the write of `meta.json` that follows the next record write, which is
/// the producer's sequence advance, and passes everything else through.
#[derive(Debug)]
struct FailAdvanceStore {
    inner: InMemory,
    armed: AtomicBool,
    fail_meta: AtomicBool,
    fail_record_after_storing: AtomicBool,
}

impl FailAdvanceStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            armed: AtomicBool::new(false),
            fail_meta: AtomicBool::new(false),
            fail_record_after_storing: AtomicBool::new(false),
        }
    }

    /// Fail the `meta.json` write that comes after the next record write.
    fn fail_next_advance(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    /// Store the next record write and then answer it with an error, as an object store does
    /// when a write that it completed times out before it answers.
    fn fail_next_record_write_after_storing(&self) {
        self.fail_record_after_storing.store(true, Ordering::SeqCst);
    }

    /// How many record batches are in the log.
    async fn records(&self) -> object_store::Result<usize> {
        Ok(self
            .inner
            .list(None)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .filter(|meta| meta.location.as_ref().contains("/records/"))
            .count())
    }
}

impl fmt::Display for FailAdvanceStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailAdvanceStore")
    }
}

#[async_trait]
impl ObjectStore for FailAdvanceStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if location.as_ref().ends_with("/meta.json") && self.fail_meta.swap(false, Ordering::SeqCst)
        {
            return Err(object_store::Error::Generic {
                store: "FailAdvanceStore",
                source: "injected fault: sequence advance failed".into(),
            });
        }

        let result = self.inner.put_opts(location, payload, opts).await;

        if location.as_ref().contains("/records/")
            && result.is_ok()
            && self.fail_record_after_storing.swap(false, Ordering::SeqCst)
        {
            return Err(object_store::Error::Generic {
                store: "FailAdvanceStore",
                source: "injected fault: record write timed out after it completed".into(),
            });
        }

        if location.as_ref().contains("/records/")
            && result.is_ok()
            && self.armed.swap(false, Ordering::SeqCst)
        {
            self.fail_meta.store(true, Ordering::SeqCst);
        }

        result
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

fn batch(producer_id: i64, producer_epoch: i16, base_sequence: i32) -> Result<deflated::Batch> {
    Ok(inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(b"payload").into()))
        .producer_id(producer_id)
        .producer_epoch(producer_epoch)
        .base_sequence(base_sequence)
        .build()
        .and_then(deflated::Batch::try_from)?)
}

/// The data points of the counter as `(attributes, value)`.
fn counted(exporter: &InMemoryMetricExporter) -> Vec<(BTreeMap<String, String>, u64)> {
    exporter
        .get_finished_metrics()
        .expect("finished metrics")
        .iter()
        .flat_map(|resource| resource.scope_metrics())
        .flat_map(|scope| scope.metrics())
        .filter(|metric| metric.name() == COUNTER)
        .filter_map(|metric| match metric.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => Some(sum),
            _ => None,
        })
        .flat_map(|sum| sum.data_points())
        .map(|point| {
            (
                point
                    .attributes()
                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect(),
                point.value(),
            )
        })
        .collect()
}

/// An object store engine on a [`FailAdvanceStore`], with a topic of one partition and an
/// idempotent producer.
struct Setup {
    storage: ArcDynStorage,
    object_store: Arc<FailAdvanceStore>,
    extensions: Extensions,
    producer_id: i64,
    producer_epoch: i16,
}

impl Setup {
    async fn new() -> Result<Self> {
        let object_store = Arc::new(FailAdvanceStore::new());
        let storage: ArcDynStorage = Arc::new(Box::new(
            DynoStore::new(CLUSTER, 111, object_store.clone())?
                .advertised_listener(Url::parse("tcp://127.0.0.1/")?)
                .schemas(None)
                .lake(None),
        ));

        let extensions = Extensions::default();

        let created = CreateTopicsValidationLayer::new()
            .layer(CreateTopicsService {
                storage: storage.clone(),
            })
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
                extensions: extensions.clone(),
            })
            .await?;

        let topics = created.topics.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

        let producer = InitProducerIdService {
            storage: storage.clone(),
        }
        .serve(RequestInput {
            request: InitProducerIdRequest::default()
                .transactional_id(None)
                .transaction_timeout_ms(0)
                .producer_id(Some(-1))
                .producer_epoch(Some(-1)),
            extensions: extensions.clone(),
        })
        .await?;

        Ok(Self {
            storage,
            object_store,
            extensions,
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
        })
    }

    /// Produces one idempotent batch that starts at `base_sequence`, and returns the error
    /// code that the partition answers.
    async fn produce(&self, base_sequence: i32) -> Result<ErrorCode> {
        let response = ProduceService::new(self.storage.clone())
            .with_validation()
            .serve(RequestInput {
                request: ProduceRequest::default().acks(1).topic_data(Some(
                    [TopicProduceData::default()
                        .name(TOPIC.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(0)
                                .records(Some(Frame {
                                    batches: vec![batch(
                                        self.producer_id,
                                        self.producer_epoch,
                                        base_sequence,
                                    )?],
                                }))]
                            .into(),
                        ))]
                    .into(),
                )),
                extensions: self.extensions.clone(),
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
}

#[tokio::test]
async fn failed_sequence_advance_is_counted_with_its_reason() -> Result<()> {
    let _guard = init_tracing()?;

    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    global::set_meter_provider(provider.clone());

    let setup = Setup::new().await?;
    setup.object_store.fail_next_advance();

    // The client is told the batch failed with a code that it does not retry, because a retry
    // of the same sequence would store the batch a second time ...
    assert_eq!(ErrorCode::UnknownServerError, setup.produce(0).await?);

    // ... while it is in the log.
    assert_eq!(1, setup.object_store.records().await.expect("records"));

    provider.force_flush().expect("flush");

    assert_eq!(
        vec![(
            BTreeMap::from([
                ("cluster".to_owned(), CLUSTER.to_owned()),
                // `Generic` is not one of the errors `object_store_error_name`
                // names, so it is `otherwise`.
                ("reason".to_owned(), "otherwise".to_owned()),
            ]),
            1
        )],
        counted(&exporter)
    );

    Ok(())
}

/// A record write of an idempotent batch that fails with an error that a retry may fix can have
/// completed, so the answer is a code that the client does not retry.
#[tokio::test]
async fn record_write_that_may_have_completed_is_not_retried_for_an_idempotent_batch() -> Result<()>
{
    let _guard = init_tracing()?;

    let setup = Setup::new().await?;
    setup.object_store.fail_next_record_write_after_storing();

    assert_eq!(ErrorCode::UnknownServerError, setup.produce(0).await?);

    // The write completed, which is why a retry would store the batch twice.
    assert_eq!(1, setup.object_store.records().await.expect("records"));

    Ok(())
}
