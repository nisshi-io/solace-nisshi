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

use std::{
    assert_matches,
    fmt::{self, Display},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::SystemTime,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    StreamExt as _, TryStreamExt as _,
    stream::{self, BoxStream},
};
use nisshi_sans_io::{
    ErrorCode, IsolationLevel, ListOffset,
    create_topics_request::CreatableTopic,
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
    record::{Record, inflated},
};
use nisshi_storage::{Error, Storage as _, TopicId, Topition};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};
use tokio::sync::Notify;

use crate::dynostore::{DynoStore, tests::init_tracing};

/// Returns the base offsets of the batch objects stored under one partition's
/// `records/` prefix, in ascending order.
async fn stored_base_offsets(bucket: &InMemory, topition: &Topition) -> Result<Vec<i64>, Error> {
    let prefix = Path::from(format!(
        "clusters/nisshi/topics/{}/partitions/{:0>10}/records/",
        topition.topic(),
        topition.partition()
    ));

    let mut base_offsets = bucket
        .list(Some(&prefix))
        .map_ok(|meta| meta.location)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .filter_map(|location| {
            location
                .parts()
                .next_back()
                .and_then(|name| name.as_ref().get(0..20)?.parse::<i64>().ok())
        })
        .collect::<Vec<_>>();

    base_offsets.sort_unstable();

    Ok(base_offsets)
}

async fn delete_records_partition(
    storage: &DynoStore,
    topic: &str,
    offset: i64,
) -> Result<(ErrorCode, i64), Error> {
    let results = storage
        .delete_records(&[DeleteRecordsTopic::default()
            .name(topic.into())
            .partitions(Some(
                [DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(offset)]
                .into(),
            ))])
        .await?;

    let partitions = results[0].partitions.clone().unwrap_or_default();

    Ok((
        ErrorCode::try_from(partitions[0].error_code)?,
        partitions[0].low_watermark,
    ))
}

/// Returns the location of one partition's watermark document.
fn watermark_location(topition: &Topition) -> Path {
    Path::from(format!(
        "clusters/nisshi/topics/{}/partitions/{:0>10}/watermark.json",
        topition.topic(),
        topition.partition()
    ))
}

/// Holds a delete of the object store until the test lets it go: the delete
/// tells `entered`, and then waits for `release`.
#[derive(Clone, Debug, Default)]
struct DeleteHold {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

/// An object store that counts the listings of batch objects, can fail
/// every delete and every listing of batch objects, and can hold the next
/// delete.
#[derive(Clone, Debug, Default)]
struct Counting {
    inner: Arc<InMemory>,
    batch_lists: Arc<AtomicUsize>,
    deletes_fail: Arc<AtomicBool>,
    lists_fail: Arc<AtomicBool>,
    held_delete: Arc<Mutex<Option<DeleteHold>>>,
}

impl Counting {
    fn batch_lists(&self) -> usize {
        self.batch_lists.load(Ordering::SeqCst)
    }

    /// Holds the next delete only: a later delete runs at once.
    fn hold_next_delete(&self) -> Result<DeleteHold, Error> {
        let hold = DeleteHold::default();

        *self.held_delete.lock()? = Some(hold.clone());

        Ok(hold)
    }
}

impl Display for Counting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Counting").finish()
    }
}

#[async_trait]
impl ObjectStore for Counting {
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
        let hold = self
            .held_delete
            .lock()
            .ok()
            .and_then(|mut held| held.take());

        if let Some(hold) = hold {
            let inner = self.inner.clone();

            return stream::once(async move {
                hold.entered.notify_one();
                hold.release.notified().await;

                inner.delete_stream(locations)
            })
            .flatten()
            .boxed();
        }

        if self.deletes_fail.load(Ordering::SeqCst) {
            return locations
                .map(|_| {
                    Err(object_store::Error::Generic {
                        store: "test",
                        source: "forced failure".into(),
                    })
                })
                .boxed();
        }

        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        if prefix.is_some_and(|prefix| prefix.as_ref().ends_with("/records")) {
            _ = self.batch_lists.fetch_add(1, Ordering::SeqCst);

            if self.lists_fail.load(Ordering::SeqCst) {
                return stream::once(async {
                    Err(object_store::Error::Generic {
                        store: "test",
                        source: "forced failure".into(),
                    })
                })
                .boxed();
            }
        }

        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

/// DeleteRecords moves the low watermark and lists no batch objects, however
/// many the partition holds, and a retry that moves nothing lists none
/// either. `maintain` removes the objects with one listing, and lists
/// nothing for the partition once it has reclaimed up to the cut.
#[tokio::test]
async fn delete_records_lists_no_batch_objects() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let counting = Counting::default();
    let storage = DynoStore::new("nisshi", 1, counting.clone())?;

    let name = "many";
    create_topic(&storage, name).await?;
    let topition = Topition::new(name, 0);

    for _ in 0..50 {
        _ = produce_batch(&storage, &topition, 1).await?;
    }

    counting.batch_lists.store(0, Ordering::SeqCst);

    assert_eq!(
        (ErrorCode::None, 40),
        delete_records_partition(&storage, name, 40).await?
    );
    // The retry moves nothing.
    assert_eq!(
        (ErrorCode::None, 40),
        delete_records_partition(&storage, name, 40).await?
    );
    assert_eq!(0, counting.batch_lists());
    assert_eq!(
        50,
        stored_base_offsets(&counting.inner, &topition).await?.len()
    );

    storage.maintain(SystemTime::now()).await?;
    assert_eq!(1, counting.batch_lists());
    assert_eq!(
        (40..50).collect::<Vec<_>>(),
        stored_base_offsets(&counting.inner, &topition).await?
    );

    storage.maintain(SystemTime::now()).await?;
    assert_eq!(1, counting.batch_lists());

    // A cut that moves lists again, once.
    assert_eq!(
        (ErrorCode::None, 50),
        delete_records_partition(&storage, name, -1).await?
    );
    storage.maintain(SystemTime::now()).await?;
    assert_eq!(2, counting.batch_lists());
    assert_eq!(
        Vec::<i64>::new(),
        stored_base_offsets(&counting.inner, &topition).await?
    );

    Ok(())
}

/// A reclaim that fails leaves the objects and is retried by the next
/// `maintain`, which then cleans up.
#[tokio::test]
async fn maintain_retries_a_failed_reclaim() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let counting = Counting::default();
    let storage = DynoStore::new("nisshi", 1, counting.clone())?;

    let name = "retry";
    create_topic(&storage, name).await?;
    let topition = Topition::new(name, 0);

    for _ in 0..4 {
        _ = produce_batch(&storage, &topition, 1).await?;
    }

    assert_eq!(
        (ErrorCode::None, 3),
        delete_records_partition(&storage, name, 3).await?
    );

    counting.deletes_fail.store(true, Ordering::SeqCst);
    storage.maintain(SystemTime::now()).await?;
    assert_eq!(
        vec![0, 1, 2, 3],
        stored_base_offsets(&counting.inner, &topition).await?
    );

    counting.deletes_fail.store(false, Ordering::SeqCst);
    storage.maintain(SystemTime::now()).await?;
    assert_eq!(
        vec![3],
        stored_base_offsets(&counting.inner, &topition).await?
    );

    Ok(())
}

/// A listing of the batch objects that fails leaves the objects, and the
/// next `maintain` lists again and removes them.
#[tokio::test]
async fn maintain_retries_a_failed_listing() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let counting = Counting::default();
    let storage = DynoStore::new("nisshi", 1, counting.clone())?;

    let name = "relist";
    create_topic(&storage, name).await?;
    let topition = Topition::new(name, 0);

    for _ in 0..4 {
        _ = produce_batch(&storage, &topition, 1).await?;
    }

    assert_eq!(
        (ErrorCode::None, 3),
        delete_records_partition(&storage, name, 3).await?
    );

    counting.lists_fail.store(true, Ordering::SeqCst);
    storage.maintain(SystemTime::now()).await?;
    assert_eq!(
        vec![0, 1, 2, 3],
        stored_base_offsets(&counting.inner, &topition).await?
    );

    // The failed listing must not record the cut as reclaimed.
    counting.lists_fail.store(false, Ordering::SeqCst);
    storage.maintain(SystemTime::now()).await?;
    assert_eq!(
        vec![3],
        stored_base_offsets(&counting.inner, &topition).await?
    );

    Ok(())
}

/// A topic that is created again under the name of a deleted topic does not
/// inherit the deleted topic's reclaim mark, so `maintain` removes the new
/// topic's objects below a cut that lies under that mark.
#[tokio::test]
async fn recreated_topic_does_not_inherit_the_reclaim_mark() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let storage = DynoStore::new("nisshi", 1, bucket.clone())?;

    let name = "again";
    create_topic(&storage, name).await?;
    let topition = Topition::new(name, 0);

    for _ in 0..10 {
        _ = produce_batch(&storage, &topition, 1).await?;
    }

    assert_eq!(
        (ErrorCode::None, 8),
        delete_records_partition(&storage, name, 8).await?
    );
    storage.maintain(SystemTime::now()).await?;

    // Deleting the topic removes its watermark document. A reclaim that
    // finishes after the delete can write one back, so the test puts the
    // document back to be that leftover.
    let location = watermark_location(&topition);
    let leftover = bucket.get(&location).await?.bytes().await?;

    _ = storage.delete_topic(&TopicId::Name(name.into())).await?;
    _ = bucket.put(&location, leftover.into()).await?;

    create_topic(&storage, name).await?;

    for _ in 0..10 {
        _ = produce_batch(&storage, &topition, 1).await?;
    }

    // The cut is under the leftover mark of 8.
    assert_eq!(
        (ErrorCode::None, 5),
        delete_records_partition(&storage, name, 5).await?
    );
    storage.maintain(SystemTime::now()).await?;

    assert_eq!(
        (5..10).collect::<Vec<_>>(),
        stored_base_offsets(&bucket, &topition).await?
    );

    Ok(())
}

/// A topic that is deleted while `maintain` deletes its batch objects does
/// not get a watermark document back from that reclaim.
#[tokio::test]
async fn maintain_does_not_recreate_the_watermark_of_a_deleted_topic() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let counting = Counting::default();
    let storage = DynoStore::new("nisshi", 1, counting.clone())?;

    let name = "vanishing";
    create_topic(&storage, name).await?;
    let topition = Topition::new(name, 0);

    for _ in 0..4 {
        _ = produce_batch(&storage, &topition, 1).await?;
    }

    assert_eq!(
        (ErrorCode::None, 3),
        delete_records_partition(&storage, name, 3).await?
    );

    let hold = counting.hold_next_delete()?;

    let delete_the_topic = async {
        hold.entered.notified().await;

        let outcome = storage.delete_topic(&TopicId::Name(name.into())).await;

        hold.release.notify_one();

        outcome
    };

    let (maintained, deleted) = tokio::join!(storage.maintain(SystemTime::now()), delete_the_topic);

    maintained?;
    assert_eq!(ErrorCode::None, deleted?);

    assert_matches!(
        counting.inner.head(&watermark_location(&topition)).await,
        Err(object_store::Error::NotFound { .. })
    );

    Ok(())
}

/// A broker that does not use a partition reads its watermark in `maintain`
/// and does not keep the partition's watermark in its cache.
#[tokio::test]
async fn maintain_does_not_cache_the_watermark_of_a_partition_it_does_not_use() -> Result<(), Error>
{
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let serving = DynoStore::new("nisshi", 1, bucket.clone())?;

    let name = "elsewhere";
    create_topic(&serving, name).await?;
    let topition = Topition::new(name, 0);

    for _ in 0..4 {
        _ = produce_batch(&serving, &topition, 1).await?;
    }

    assert_eq!(
        (ErrorCode::None, 3),
        delete_records_partition(&serving, name, 3).await?
    );

    let other = DynoStore::new("nisshi", 2, bucket.clone())?;
    other.maintain(SystemTime::now()).await?;

    assert_eq!(vec![3], stored_base_offsets(&bucket, &topition).await?);
    assert!(other.watermarks.lock()?.is_empty());

    Ok(())
}

/// A DeleteRecords cut removes the batch objects that lie wholly below it,
/// and keeps the object of a batch that straddles the cut. A cut at the
/// high watermark removes every object. The watermark alone does not show
/// this, so the test lists the bucket.
#[tokio::test]
async fn delete_records_removes_batch_objects_below_the_cut() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let storage = DynoStore::new("nisshi", 1, bucket.clone())?;

    let name = "abc";

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(name, 0);

    // single-record batches at offsets 0, 1 and 2
    for _ in 0..3 {
        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Some(Bytes::from_static(b"abc"))))
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage.produce(None, &topition, batch).await?;
    }

    // a two-record batch at offsets 3 and 4
    let batch = inflated::Batch::builder()
        .last_offset_delta(1)
        .record(
            Record::builder()
                .offset_delta(0)
                .value(Some(Bytes::from_static(b"abc"))),
        )
        .record(
            Record::builder()
                .offset_delta(1)
                .value(Some(Bytes::from_static(b"abc"))),
        )
        .build()
        .and_then(TryInto::try_into)?;

    _ = storage.produce(None, &topition, batch).await?;

    // a single-record batch at offset 5, the last object
    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"abc"))))
        .build()
        .and_then(TryInto::try_into)?;

    _ = storage.produce(None, &topition, batch).await?;

    assert_eq!(
        vec![0, 1, 2, 3, 5],
        stored_base_offsets(&bucket, &topition).await?
    );

    // A cut inside the two-record batch removes the objects wholly below it
    // and keeps that batch whole.
    assert_eq!(
        (ErrorCode::None, 4),
        delete_records_partition(&storage, name, 4).await?
    );
    storage.maintain(SystemTime::now()).await?;
    assert_eq!(vec![3, 5], stored_base_offsets(&bucket, &topition).await?);
    assert_eq!(4, storage.offset_stage(&topition).await?.log_start());

    // A cut to the high watermark removes every batch object, the last one
    // too. Earliest and Latest still answer the high watermark.
    assert_eq!(
        (ErrorCode::None, 6),
        delete_records_partition(&storage, name, -1).await?
    );
    storage.maintain(SystemTime::now()).await?;
    assert_eq!(
        Vec::<i64>::new(),
        stored_base_offsets(&bucket, &topition).await?
    );
    assert_eq!(6, storage.offset_stage(&topition).await?.log_start());

    for list_offset in [ListOffset::Earliest, ListOffset::Latest] {
        let responses = storage
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[(topition.clone(), list_offset)],
            )
            .await?;

        assert_eq!(1, responses.len());
        assert_eq!(ErrorCode::None, responses[0].1.error_code);
        assert_eq!(Some(6), responses[0].1.offset, "{list_offset:?}");
    }

    Ok(())
}

/// Creates the topic `name` with one partition in `storage`.
async fn create_topic(storage: &DynoStore, name: &str) -> Result<(), Error> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(())
}

/// Produces one batch of `records` records to `topition`.
async fn produce_batch(
    storage: &DynoStore,
    topition: &Topition,
    records: i32,
) -> Result<i64, Error> {
    let mut builder = inflated::Batch::builder().last_offset_delta(records - 1);

    for offset_delta in 0..records {
        builder = builder.record(
            Record::builder()
                .offset_delta(offset_delta)
                .value(Some(Bytes::from_static(b"abc"))),
        );
    }

    let batch = builder.build().and_then(TryInto::try_into)?;

    storage.produce(None, topition, batch).await
}

/// A cut below the high watermark keeps the last object, because that
/// batch can hold records at and above the cut.
#[tokio::test]
async fn delete_records_keeps_a_last_batch_that_straddles_the_cut() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let storage = DynoStore::new("nisshi", 1, bucket.clone())?;

    let name = "straddle";
    create_topic(&storage, name).await?;
    let topition = Topition::new(name, 0);

    assert_eq!(0, produce_batch(&storage, &topition, 4).await?);

    assert_eq!(
        (ErrorCode::None, 2),
        delete_records_partition(&storage, name, 2).await?
    );
    storage.maintain(SystemTime::now()).await?;
    assert_eq!(vec![0], stored_base_offsets(&bucket, &topition).await?);

    Ok(())
}

/// A produce that races a cut at the high watermark writes its batch at
/// the cut. The cleanup keeps that batch, although it is the last object
/// and the cut equals the high watermark that the delete read.
#[tokio::test]
async fn delete_records_keeps_a_batch_written_at_the_cut() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let storage = DynoStore::new("nisshi", 1, bucket.clone())?;

    let name = "race";
    create_topic(&storage, name).await?;
    let topition = Topition::new(name, 0);

    assert_eq!(0, produce_batch(&storage, &topition, 6).await?);
    assert_eq!(6, produce_batch(&storage, &topition, 2).await?);

    // The delete read a high watermark of 6, and the batch at 6 was written
    // after that.
    assert!(storage.delete_records_batches(&topition, 6, 6).await);

    assert_eq!(vec![6], stored_base_offsets(&bucket, &topition).await?);

    Ok(())
}

/// A produce that lands after a cut at the high watermark, and before
/// `maintain`, keeps its batch.
#[tokio::test]
async fn maintain_keeps_a_batch_produced_after_the_cut() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let storage = DynoStore::new("nisshi", 1, bucket.clone())?;

    let name = "after";
    create_topic(&storage, name).await?;
    let topition = Topition::new(name, 0);

    assert_eq!(0, produce_batch(&storage, &topition, 3).await?);
    assert_eq!(
        (ErrorCode::None, 3),
        delete_records_partition(&storage, name, -1).await?
    );
    assert_eq!(3, produce_batch(&storage, &topition, 2).await?);

    storage.maintain(SystemTime::now()).await?;

    assert_eq!(vec![3], stored_base_offsets(&bucket, &topition).await?);

    Ok(())
}
