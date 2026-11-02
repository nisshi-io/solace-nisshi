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

//! A produce on the object store engine takes its offsets in the watermark
//! document before it writes the batch. If the batch write fails, those
//! offsets hold no batch, and the engine does not give them back: the high
//! watermark stays past them, the next produce takes the offset after them,
//! and a fetch from inside the gap returns the next batch that exists.

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
    CreateTopicsRequest, ErrorCode, FetchRequest, IsolationLevel, ListOffset, ListOffsetsRequest,
    NULL_TOPIC_ID, ProduceRequest, RequestInput,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::PartitionProduceResponse,
    record::{
        Record,
        deflated::{self, Frame},
        inflated,
    },
};
use nisshi_storage::{
    ArcDynStorage, CreateTopicsService, CreateTopicsValidationLayer, FetchService,
    FetchValidationLayer, ListOffsetsService, ProduceService,
};
use nisshi_storage_dynostore::DynoStore;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use rama::{Layer as _, Service as _, extensions::Extensions};
use url::Url;

const CLUSTER: &str = "post-watermark-failure";
const TOPIC: &str = "post-watermark-failure";
const PARTITION: i32 = 0;

/// Fails the next write of a record batch, and passes everything else
/// through.
#[derive(Debug)]
struct FailBatchStore {
    inner: InMemory,
    fail_batch: AtomicBool,
}

impl FailBatchStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            fail_batch: AtomicBool::new(false),
        }
    }

    /// Fail the next record batch write.
    fn fail_next_batch(&self) {
        self.fail_batch.store(true, Ordering::SeqCst);
    }
}

impl fmt::Display for FailBatchStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailBatchStore")
    }
}

#[async_trait]
impl ObjectStore for FailBatchStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if location.as_ref().contains("/records/") && self.fail_batch.swap(false, Ordering::SeqCst)
        {
            return Err(object_store::Error::Generic {
                store: "FailBatchStore",
                source: "injected fault: batch write failed".into(),
            });
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

async fn create_topic(storage: &ArcDynStorage) -> Result<()> {
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
            extensions: Extensions::default(),
        })
        .await?;

    let topics = created.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    Ok(())
}

/// Produces one non-idempotent batch with a record for each of `values`,
/// and returns the partition's response.
async fn produce(
    storage: &ArcDynStorage,
    values: &[&'static str],
) -> Result<PartitionProduceResponse> {
    let last_offset_delta = i32::try_from(values.len())? - 1;

    let batch = (0..)
        .zip(values)
        .fold(
            inflated::Batch::builder().last_offset_delta(last_offset_delta),
            |builder, (offset_delta, value)| {
                builder.record(
                    Record::builder()
                        .offset_delta(offset_delta)
                        .value(Some(Bytes::from_static(value.as_bytes()))),
                )
            },
        )
        .build()
        .and_then(deflated::Batch::try_from)?;

    let response = ProduceService {
        storage: storage.clone(),
    }
    .serve(RequestInput {
        request: ProduceRequest::default().acks(1).topic_data(Some(
            [TopicProduceData::default()
                .name(TOPIC.into())
                .partition_data(Some(
                    [PartitionProduceData::default()
                        .index(PARTITION)
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

    let topics = response.responses.unwrap_or_default();
    assert_eq!(1, topics.len());
    let partitions = topics[0].partition_responses.clone().unwrap_or_default();
    assert_eq!(1, partitions.len());

    Ok(partitions[0].clone())
}

/// Returns the partition's latest offset, as `ListOffsets` reports it.
async fn latest_offset(storage: &ArcDynStorage) -> Result<Option<i64>> {
    let response = ListOffsetsService {
        storage: storage.clone(),
    }
    .serve(RequestInput {
        request: ListOffsetsRequest::default()
            .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
            .replica_id(-1)
            .topics(Some(
                [ListOffsetsTopic::default()
                    .name(TOPIC.into())
                    .partitions(Some(
                        [ListOffsetsPartition::default()
                            .current_leader_epoch(Some(-1))
                            .max_num_offsets(Some(1))
                            .partition_index(PARTITION)
                            .timestamp(ListOffset::Latest.try_into()?)]
                        .into(),
                    ))]
                .into(),
            )),
        extensions: Extensions::default(),
    })
    .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );

    Ok(partitions[0].offset)
}

/// Fetches from `fetch_offset`, and returns the batches of the partition.
async fn fetch(storage: &ArcDynStorage, fetch_offset: i64) -> Result<Vec<inflated::Batch>> {
    let response = FetchValidationLayer::new()
        .layer(FetchService {
            storage: storage.clone(),
        })
        .serve(RequestInput {
            request: FetchRequest::default()
                .max_wait_ms(500)
                .min_bytes(1)
                .max_bytes(Some(50 * 1024))
                .isolation_level(Some((&IsolationLevel::ReadUncommitted).into()))
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(TOPIC.into()))
                        .topic_id(Some(NULL_TOPIC_ID))
                        .partitions(Some(
                            [FetchPartition::default()
                                .partition(PARTITION)
                                .current_leader_epoch(Some(-1))
                                .fetch_offset(fetch_offset)
                                .last_fetched_epoch(Some(-1))
                                .log_start_offset(Some(-1))
                                .partition_max_bytes(50 * 1024)
                                .replica_directory_id(None)]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.responses.unwrap_or_default();
    assert_eq!(1, topics.len());
    let partitions = topics[0].partitions.clone().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );

    partitions[0]
        .records
        .clone()
        .map(|frame| frame.batches)
        .unwrap_or_default()
        .into_iter()
        .map(|batch| inflated::Batch::try_from(batch).map_err(Into::into))
        .collect()
}

#[tokio::test]
async fn failed_batch_write_leaves_an_offset_gap_that_fetch_skips() -> Result<()> {
    let _guard = init_tracing()?;

    let object_store = Arc::new(FailBatchStore::new());
    let storage: ArcDynStorage = Arc::new(Box::new(
        DynoStore::new(CLUSTER, 111, object_store.clone())?
            .advertised_listener(Url::parse("tcp://127.0.0.1/")?)
            .schemas(None)
            .lake(None),
    ));

    create_topic(&storage).await?;

    let before = produce(&storage, &["before"]).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(before.error_code)?);
    assert_eq!(0, before.base_offset);

    object_store.fail_next_batch();

    let failed = produce(&storage, &["lost", "lost", "lost"]).await?;
    // The object store failed the write, which a producer retries.
    assert_eq!(
        ErrorCode::KafkaStorageError,
        ErrorCode::try_from(failed.error_code)?
    );

    // The failed produce took offsets 1 to 3, and the engine did not give them back.
    assert_eq!(Some(4), latest_offset(&storage).await?);

    let after = produce(&storage, &["after"]).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(after.error_code)?);
    assert_eq!(4, after.base_offset);

    let batches = fetch(&storage, 1).await?;
    let past_gap = batches.first().expect("a batch past the gap");
    assert_eq!(4, past_gap.base_offset);
    assert_eq!(
        Some(Bytes::from_static(b"after")),
        past_gap.records.first().and_then(|record| record.value())
    );

    Ok(())
}
