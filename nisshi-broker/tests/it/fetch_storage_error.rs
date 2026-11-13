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

//! A storage error in one partition of a Fetch answers `KAFKA_STORAGE_ERROR` in that partition,
//! and the other partitions still answer, as in Kafka, where each partition is read in its own
//! `try` ([ReplicaManager.scala#L1628-L1727]). The service returns `Ok`, so the connection stays
//! open.
//!
//! [ReplicaManager.scala#L1628-L1727]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1628-L1727

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use crate::common::init_tracing;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use nisshi_broker::Result;
use nisshi_sans_io::{
    ErrorCode, FetchRequest, IsolationLevel, NULL_TOPIC_ID, RequestInput,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    fetch_response::PartitionData,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{ArcDynStorage, FetchService, FetchValidationLayer, Storage as _, Topition};
use nisshi_storage_dynostore::DynoStore;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use rama::{Layer as _, Service as _, extensions::Extensions};
use url::Url;

const CLUSTER: &str = "fetch-storage-error";
const TOPIC: &str = "fetch-storage-error";
const PARTITIONS: i32 = 2;

/// Long enough that waiting it out fails the elapsed check.
const MAX_WAIT: Duration = Duration::from_secs(10);

/// A fetch offset above the one record of each partition.
const ABOVE_THE_LOG: i64 = 5;

/// Fails the next unconditional read of partition 0's `watermark.json` once armed, and passes
/// everything else through.
///
/// `DynoStore::offset_stage` reads the watermark with a conditional GET, which the store's cache
/// can answer from memory, and `DynoStore::current_offset_stage` reads it with a GET that has no
/// `If-None-Match`. Only that GET fails, so the fault reaches `current_offset_stage` whatever the
/// cache holds.
#[derive(Debug)]
struct FailWatermarkReadStore {
    inner: InMemory,
    armed: AtomicBool,
}

impl FailWatermarkReadStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            armed: AtomicBool::new(false),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn disarm(&self) {
        self.armed.store(false, Ordering::SeqCst);
    }

    fn is_armed(&self) -> bool {
        self.armed.load(Ordering::SeqCst)
    }
}

impl fmt::Display for FailWatermarkReadStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailWatermarkReadStore")
    }
}

#[async_trait]
impl ObjectStore for FailWatermarkReadStore {
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
        let path = location.as_ref();

        if path.contains("/partitions/0000000000/")
            && path.ends_with("/watermark.json")
            && options.if_none_match.is_none()
            && self.armed.swap(false, Ordering::SeqCst)
        {
            return Err(object_store::Error::Generic {
                store: "FailWatermarkReadStore",
                source: "injected fault: watermark.json read failed".into(),
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

/// Returns a DynoStore on `object_store`.
fn dyno_store(object_store: Arc<FailWatermarkReadStore>) -> Result<ArcDynStorage> {
    Ok(Arc::new(Box::new(
        DynoStore::new(CLUSTER, 111, object_store)?
            .advertised_listener(Url::parse("tcp://127.0.0.1/")?)
            .schemas(None)
            .lake(None),
    )))
}

/// Creates a topic of [`PARTITIONS`] partitions, with one record at offset 0 in each.
async fn create_topic_with_one_record_per_partition(storage: &ArcDynStorage) -> Result<()> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(PARTITIONS)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    for partition in 0..PARTITIONS {
        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"payload").into()))
            .build()
            .and_then(deflated::Batch::try_from)?;

        assert_eq!(
            0,
            storage
                .produce(None, &Topition::new(TOPIC, partition), batch)
                .await?
        );
    }

    Ok(())
}

/// Serves one Fetch of the topic with `offsets` as the fetch offsets of partitions 0 and 1, and
/// returns the partitions' answers in partition order, and how long the Fetch took.
async fn fetch(
    storage: &ArcDynStorage,
    offsets: [i64; PARTITIONS as usize],
) -> Result<(Vec<PartitionData>, Duration)> {
    let started_at = Instant::now();

    let response = FetchValidationLayer::new()
        .layer(FetchService {
            storage: storage.clone(),
        })
        .serve(RequestInput {
            request: FetchRequest::default()
                .max_wait_ms(i32::try_from(MAX_WAIT.as_millis())?)
                .min_bytes(1)
                .max_bytes(Some(1_048_576))
                .isolation_level(Some((&IsolationLevel::ReadUncommitted).into()))
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(TOPIC.into()))
                        .topic_id(Some(NULL_TOPIC_ID))
                        .partitions(Some(
                            (0..PARTITIONS)
                                .zip(offsets)
                                .map(|(partition, fetch_offset)| {
                                    FetchPartition::default()
                                        .partition(partition)
                                        .current_leader_epoch(Some(-1))
                                        .fetch_offset(fetch_offset)
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

    let mut partitions = topics[0].partitions.clone().unwrap_or_default();
    partitions.sort_by_key(|partition| partition.partition_index);

    Ok((partitions, elapsed))
}

/// The error code of `partition`, and how many batches it holds.
fn answer(partition: &PartitionData) -> Result<(ErrorCode, usize)> {
    Ok((
        ErrorCode::try_from(partition.error_code)?,
        partition
            .records
            .as_ref()
            .map_or(0, |records| records.batches.len()),
    ))
}

/// A failed read of a partition's watermark, made for a fetch offset above the cached high
/// watermark, answers `KAFKA_STORAGE_ERROR` in that partition alone, at once. The other
/// partition keeps its record, and the next Fetch on the same service reads the partition.
#[tokio::test]
async fn failed_watermark_read_answers_in_its_partition() -> Result<()> {
    let _guard = init_tracing()?;

    let object_store = Arc::new(FailWatermarkReadStore::new());
    let storage = dyno_store(object_store.clone())?;
    create_topic_with_one_record_per_partition(&storage).await?;

    // This Fetch reads each partition's watermark through the cache, so that the fault below
    // meets only the read that `current_offset_stage` makes.
    let (partitions, _) = fetch(&storage, [0, 0]).await?;
    assert_eq!(
        vec![(ErrorCode::None, 1), (ErrorCode::None, 1)],
        partitions.iter().map(answer).collect::<Result<Vec<_>>>()?
    );

    object_store.arm();

    let (partitions, elapsed) = fetch(&storage, [ABOVE_THE_LOG, 0]).await?;

    assert!(
        !object_store.is_armed(),
        "the fault was not consumed by a read of the watermark"
    );
    assert!(elapsed < MAX_WAIT / 2, "{elapsed:?}");
    assert_eq!(
        vec![(ErrorCode::KafkaStorageError, 0), (ErrorCode::None, 1)],
        partitions.iter().map(answer).collect::<Result<Vec<_>>>()?
    );
    assert_eq!(-1, partitions[0].high_watermark);

    object_store.disarm();

    // Offset 5 is out of range once the read works, so the retry reads from the start.
    let (partitions, _) = fetch(&storage, [0, 0]).await?;
    assert_eq!(
        vec![(ErrorCode::None, 1), (ErrorCode::None, 1)],
        partitions.iter().map(answer).collect::<Result<Vec<_>>>()?
    );

    Ok(())
}
