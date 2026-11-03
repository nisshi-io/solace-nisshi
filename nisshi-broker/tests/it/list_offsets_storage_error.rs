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

//! A storage error in one partition of a ListOffsets request answers
//! `KAFKA_STORAGE_ERROR` in that partition, and the other partitions still
//! answer, as in Kafka, where each partition is looked up in its own `try`
//! ([KafkaApis.scala#L1200-L1252]). The service returns `Ok`, so the
//! connection stays open.
//!
//! [KafkaApis.scala#L1200-L1252]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1200-L1252

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
use futures::stream::{BoxStream, TryStreamExt as _};
use nisshi_broker::Result;
use nisshi_sans_io::{
    ErrorCode, IsolationLevel, ListOffset, ListOffsetsRequest,
    create_topics_request::CreatableTopic,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    list_offsets_response::ListOffsetsPartitionResponse,
    record::{Record, deflated, inflated},
    to_system_time,
};
use nisshi_storage::{ArcDynStorage, ListOffsetsService, Storage as _, Topition};
use nisshi_storage_dynostore::DynoStore;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};
use rama::Service as _;
use url::Url;

const CLUSTER: &str = "list-offsets-storage-error";
const TOPIC: &str = "list-offsets-storage-error";
const PARTITIONS: i32 = 3;

/// The timestamp, in milliseconds, of the one record in each partition.
const RECORDED_AT: i64 = 1_577_836_800_000;

/// Fails the next read of `meta.json` once armed, and passes everything else
/// through.
#[derive(Debug)]
struct FailMetaReadStore {
    inner: InMemory,
    armed: AtomicBool,
}

impl FailMetaReadStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            armed: AtomicBool::new(false),
        }
    }

    fn fail_next_meta_read(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    /// Overwrites every batch object of `topition` with bytes that do not
    /// decode as a batch.
    async fn corrupt_batches(&self, topition: &Topition) -> object_store::Result<()> {
        let records = format!("/partitions/{:0>10}/records/", topition.partition());

        for meta in self.inner.list(None).try_collect::<Vec<_>>().await? {
            if meta.location.as_ref().contains(&records) {
                _ = self
                    .inner
                    .put(&meta.location, PutPayload::from_static(b"not a batch"))
                    .await?;
            }
        }

        Ok(())
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
        if location.as_ref().ends_with("/meta.json") && self.armed.swap(false, Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "FailMetaReadStore",
                source: "injected fault: meta.json read failed".into(),
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

/// Returns a DynoStore on `object_store`, with its own empty cache of
/// metadata reads.
fn dyno_store(object_store: Arc<FailMetaReadStore>) -> Result<ArcDynStorage> {
    Ok(Arc::new(Box::new(
        DynoStore::new(CLUSTER, 111, object_store)?
            .advertised_listener(Url::parse("tcp://127.0.0.1/")?)
            .schemas(None)
            .lake(None),
    )))
}

/// Creates a topic of [`PARTITIONS`] partitions on `object_store`, with one
/// record at offset 0 in each partition.
async fn create_topic_with_one_record_per_partition(
    object_store: Arc<FailMetaReadStore>,
) -> Result<()> {
    let storage = dyno_store(object_store)?;

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
            .base_timestamp(RECORDED_AT)
            .max_timestamp(RECORDED_AT)
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

/// Serves one ListOffsets request for `list_offset` on every partition of
/// the topic, and returns the partitions' answers in partition order.
async fn list_offsets(
    storage: &ArcDynStorage,
    list_offset: ListOffset,
) -> Result<Vec<ListOffsetsPartitionResponse>> {
    let response = ListOffsetsService {
        storage: storage.clone(),
    }
    .serve(
        ListOffsetsRequest::default()
            .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
            .replica_id(-1)
            .topics(Some(vec![
                ListOffsetsTopic::default()
                    .name(TOPIC.into())
                    .partitions(Some(
                        (0..PARTITIONS)
                            .map(|partition| {
                                list_offset.try_into().map(|timestamp| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition)
                                        .current_leader_epoch(Some(-1))
                                        .timestamp(timestamp)
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
            ])),
    )
    .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());

    let mut partitions = topics[0].partitions.clone().unwrap_or_default();
    partitions.sort_by_key(|partition| partition.partition_index);
    Ok(partitions)
}

/// The `(error code, offset)` that each partition answers.
fn answers(partitions: &[ListOffsetsPartitionResponse]) -> Result<Vec<(ErrorCode, Option<i64>)>> {
    partitions
        .iter()
        .map(|partition| {
            ErrorCode::try_from(partition.error_code)
                .map(|error_code| (error_code, partition.offset))
                .map_err(Into::into)
        })
        .collect()
}

/// A failed read of the topic's metadata answers `KAFKA_STORAGE_ERROR` in
/// the one partition whose read failed, and the next request on the same
/// service answers every partition.
#[tokio::test]
async fn failed_metadata_read_answers_in_its_partition() -> Result<()> {
    let _guard = init_tracing()?;

    let object_store = Arc::new(FailMetaReadStore::new());
    create_topic_with_one_record_per_partition(object_store.clone()).await?;

    // The DynoStore that wrote the topic answers a read of its metadata from
    // its cache, without a read of `meta.json`. This one has not read it yet,
    // so its first read for each partition reaches the store.
    let storage = dyno_store(object_store.clone())?;

    object_store.fail_next_meta_read();

    let partitions = list_offsets(&storage, ListOffset::Latest).await?;
    let mut answered = answers(&partitions)?;
    answered.sort_unstable();

    assert_eq!(
        vec![
            (ErrorCode::None, Some(1)),
            (ErrorCode::None, Some(1)),
            (ErrorCode::KafkaStorageError, Some(-1)),
        ],
        answered
    );
    assert!(
        partitions
            .iter()
            .all(|partition| partition.timestamp == Some(-1)),
        "{partitions:?}"
    );

    assert_eq!(
        vec![(ErrorCode::None, Some(1)); 3],
        answers(&list_offsets(&storage, ListOffset::Latest).await?)?
    );

    Ok(())
}

/// A timestamp lookup that reads a batch that does not decode answers
/// `KAFKA_STORAGE_ERROR` in that batch's partition only.
#[tokio::test]
async fn corrupt_batch_answers_in_its_partition() -> Result<()> {
    let _guard = init_tracing()?;

    let object_store = Arc::new(FailMetaReadStore::new());
    create_topic_with_one_record_per_partition(object_store.clone()).await?;
    let storage = dyno_store(object_store.clone())?;

    object_store
        .corrupt_batches(&Topition::new(TOPIC, 1))
        .await
        .expect("corrupt batches");

    assert_eq!(
        vec![
            (ErrorCode::None, Some(0)),
            (ErrorCode::KafkaStorageError, Some(-1)),
            (ErrorCode::None, Some(0)),
        ],
        answers(
            &list_offsets(
                &storage,
                ListOffset::Timestamp(to_system_time(RECORDED_AT)?)
            )
            .await?
        )?
    );

    Ok(())
}
