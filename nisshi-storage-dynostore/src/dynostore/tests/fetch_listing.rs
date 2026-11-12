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

//! Tests for how much of a partition's listing a Fetch reads: it lists nothing when a batch
//! starts at the fetch offset, and otherwise lists from near the fetch offset, so its cost does
//! not grow with the partition.

use nisshi_storage::fetched::FetchUntilDeadline as _;
use std::{
    assert_matches,
    fmt::Display,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};
use tokio::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    StreamExt as _, TryStreamExt as _,
    stream::{self, BoxStream},
};
use nisshi_sans_io::{
    ErrorCode, IsolationLevel,
    create_topics_request::CreatableTopic,
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
    record::{Record, inflated},
};
use nisshi_storage::{Error, Result, Storage as _, TopicId, Topition};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};

use crate::dynostore::{
    BATCH_SETTLED_AFTER, DynoStore, FETCH_LISTING_FIRST_WINDOW, batch_path,
    tests::{
        failing_object_store::FailingObjectStore,
        held_put_store::{self, HeldPutStore},
        init_tracing,
    },
};

/// An object store that counts the listings of batch objects and the batch
/// objects they return, records the start-after key of each, and can delay
/// each listed object or list the objects out of key order.
#[derive(Clone, Debug, Default)]
struct Listing {
    inner: Arc<InMemory>,
    /// Listings of batch objects that start at the first key.
    full_lists: Arc<AtomicUsize>,
    /// The start-after key of each listing of batch objects that has one.
    start_after: Arc<Mutex<Vec<Path>>>,
    /// Batch objects that every listing returned, in total.
    listed_batches: Arc<AtomicUsize>,
    /// How long the store waits before it returns each listed object, in
    /// milliseconds.
    delay_millis: Arc<AtomicU64>,
    /// The store lists objects in descending key order.
    descending: Arc<AtomicBool>,
    /// The store lists objects in key order, except the last two, which it
    /// lists in descending order.
    last_two_swapped: Arc<AtomicBool>,
    /// Zero lets every read of a batch object finish. Any other value `n` lets the first `n - 1`
    /// reads finish, and makes every later read wait forever.
    stall_batch_reads_after: Arc<AtomicUsize>,
    /// Reads of batch objects, in total.
    batch_reads: Arc<AtomicUsize>,
    /// How long before its real write time the store reports each object's write time.
    backdate: Backdate,
}

/// How long before its real write time an object store reports each object's write time. The
/// clones of a `Backdate` share one value, so a test can change it after it gives a clone to the
/// store.
#[derive(Clone, Debug, Default)]
struct Backdate(Arc<Mutex<Duration>>);

impl Backdate {
    /// Sets the backdate to `backdate`, in place of the backdate before.
    fn set(&self, backdate: Duration) {
        if let Ok(mut current) = self.0.lock() {
            *current = backdate;
        }
    }

    /// Returns `meta` with its write time moved back by the backdate.
    fn apply(&self, mut meta: ObjectMeta) -> ObjectMeta {
        let backdate = self.0.lock().map_or(Duration::ZERO, |backdate| *backdate);

        // `last_modified` is a chrono time, which subtracts a chrono `TimeDelta` and not a
        // `Duration`, so the subtraction goes through `SystemTime`.
        meta.last_modified = (SystemTime::from(meta.last_modified) - backdate).into();
        meta
    }

    /// Returns `listing` with the write time of each object moved back by the backdate.
    fn apply_to_listing(
        &self,
        listing: BoxStream<'static, Result<ObjectMeta, object_store::Error>>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        let backdate = self.clone();
        listing.map_ok(move |meta| backdate.apply(meta)).boxed()
    }
}

impl Listing {
    fn full_lists(&self) -> usize {
        self.full_lists.load(Ordering::SeqCst)
    }

    fn start_after(&self) -> Vec<Path> {
        self.start_after
            .lock()
            .map(|start_after| start_after.clone())
            .unwrap_or_default()
    }

    fn listed_batches(&self) -> usize {
        self.listed_batches.load(Ordering::SeqCst)
    }

    fn reset_counts(&self) {
        self.full_lists.store(0, Ordering::SeqCst);
        self.listed_batches.store(0, Ordering::SeqCst);
        if let Ok(mut start_after) = self.start_after.lock() {
            start_after.clear();
        }
    }

    fn delay_each_listed_object(&self, delay: Duration) {
        self.delay_millis.store(
            u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            Ordering::SeqCst,
        );
    }

    fn list_in_descending_order(&self) {
        self.descending.store(true, Ordering::SeqCst);
    }

    fn list_last_two_swapped(&self) {
        self.last_two_swapped.store(true, Ordering::SeqCst);
    }

    /// Makes the store report each object's write time `backdate` before its real write time, as
    /// if `backdate` had passed since each write. Each call replaces the backdate before it.
    fn backdate_writes(&self, backdate: Duration) {
        self.backdate.set(backdate);
    }

    /// Stalls every read of a batch object after the first `batch_count`: each later read waits
    /// forever.
    fn stall_batch_reads_after(&self, batch_count: usize) {
        self.stall_batch_reads_after
            .store(batch_count + 1, Ordering::SeqCst);
    }

    /// Counts, delays and orders the objects of `listing` as this store is
    /// set up to.
    ///
    /// The listing panics when it is polled after it ends, as the S3
    /// store's listing does, so that a test fails for a caller that polls
    /// a listing again after it ends.
    fn shape(
        &self,
        listing: BoxStream<'static, Result<ObjectMeta, object_store::Error>>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        let descending = self.descending.load(Ordering::SeqCst);
        let last_two_swapped = self.last_two_swapped.load(Ordering::SeqCst);

        let listing = if descending || last_two_swapped {
            stream::once(async move {
                let mut objects = listing.collect::<Vec<_>>().await;

                if descending {
                    objects.reverse();
                }

                if last_two_swapped && objects.len() >= 2 {
                    let last = objects.len() - 1;
                    objects.swap(last - 1, last);
                }

                stream::iter(objects)
            })
            .flatten()
            .boxed()
        } else {
            listing
        };

        let delay = Duration::from_millis(self.delay_millis.load(Ordering::SeqCst));
        let listed_batches = self.listed_batches.clone();

        stream::unfold(listing, |mut listing| async move {
            listing.next().await.map(|object| (object, listing))
        })
        .then(move |object| {
            let listed_batches = listed_batches.clone();

            async move {
                if let Ok(meta) = &object
                    && meta.location.as_ref().ends_with(".batch")
                {
                    _ = listed_batches.fetch_add(1, Ordering::SeqCst);
                }

                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }

                object
            }
        })
        .boxed()
    }
}

/// Whether `prefix` is where a partition's batch objects are stored.
fn lists_batches(prefix: Option<&Path>) -> bool {
    prefix.is_some_and(|prefix| prefix.as_ref().ends_with("/records"))
}

impl Display for Listing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Listing").finish()
    }
}

#[async_trait]
impl ObjectStore for Listing {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        let stall_after = self.stall_batch_reads_after.load(Ordering::SeqCst);

        if stall_after > 0
            && location.as_ref().ends_with(".batch")
            && self.batch_reads.fetch_add(1, Ordering::SeqCst) >= stall_after - 1
        {
            std::future::pending::<()>().await;
        }

        let mut result = self.inner.get_opts(location, options).await?;
        result.meta = self.backdate.apply(result.meta);
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        if !lists_batches(prefix) {
            return self.backdate.apply_to_listing(self.inner.list(prefix));
        }

        _ = self.full_lists.fetch_add(1, Ordering::SeqCst);
        self.backdate
            .apply_to_listing(self.shape(self.inner.list(prefix)))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        if !lists_batches(prefix) {
            return self
                .backdate
                .apply_to_listing(self.inner.list_with_offset(prefix, offset));
        }

        if let Ok(mut start_after) = self.start_after.lock() {
            start_after.push(offset.to_owned());
        }

        self.backdate
            .apply_to_listing(self.shape(self.inner.list_with_offset(prefix, offset)))
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        let mut result = self.inner.list_with_delimiter(prefix).await?;

        result.objects = result
            .objects
            .into_iter()
            .map(|meta| self.backdate.apply(meta))
            .collect();

        Ok(result)
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.inner.copy_opts(from, to, opts).await
    }
}

/// Creates a one-partition topic named `name` on a store that counts its
/// listings, and returns the storage, the store and the partition.
async fn partition(name: &str) -> Result<(DynoStore, Listing, Topition)> {
    let listing = Listing::default();
    let storage = DynoStore::new("nisshi", 111, listing.clone())?;
    let topition = create_partition(&storage, name).await?;

    Ok((storage, listing, topition))
}

/// Creates a one-partition topic named `name` in `storage`, and returns the partition.
async fn create_partition(storage: &DynoStore, name: &str) -> Result<Topition> {
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

    Ok(Topition::new(name, 0))
}

/// Produces `batches` batches of `records_per_batch` records each.
async fn produce(
    storage: &DynoStore,
    topition: &Topition,
    batches: usize,
    records_per_batch: usize,
) -> Result<()> {
    for _ in 0..batches {
        let batch = (0..records_per_batch)
            .fold(inflated::Batch::builder(), |builder, offset_delta| {
                builder.record(
                    Record::builder()
                        .value(Some(Bytes::from_static(b"abc")))
                        .offset_delta(i32::try_from(offset_delta).unwrap_or_default()),
                )
            })
            .last_offset_delta(i32::try_from(records_per_batch - 1)?)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage.produce(None, topition, batch).await?;
    }

    Ok(())
}

/// Returns the base offsets of the batches that a fetch at `offset` returns when it may read for
/// `read_time`.
async fn fetch_base_offsets(
    storage: &DynoStore,
    topition: &Topition,
    offset: i64,
    max_bytes: u32,
    read_time: Duration,
) -> Result<Vec<i64>> {
    Ok(storage
        .fetch_until_deadline(
            topition,
            offset,
            1,
            max_bytes,
            IsolationLevel::ReadUncommitted,
            Instant::now() + read_time,
        )
        .await
        .map(|read| read.batches)?
        .iter()
        .map(|batch| batch.base_offset)
        .collect())
}

const MAX_BYTES: u32 = 50 * 1024 * 1024;

const READ_TIME: Duration = Duration::from_secs(60);

/// A fetch at a batch's base offset reads that batch by its key, and lists nothing.
#[tokio::test]
async fn fetch_at_a_batch_base_offset_lists_nothing() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, listing, topition) =
        partition("fetch_at_a_batch_base_offset_lists_nothing").await?;
    produce(&storage, &topition, 20, 1).await?;
    listing.reset_counts();

    let base_offsets = fetch_base_offsets(&storage, &topition, 10, MAX_BYTES, READ_TIME).await?;

    assert_eq!((10..20).collect::<Vec<_>>(), base_offsets);
    assert_eq!(0, listing.full_lists());
    assert_eq!(Vec::<Path>::new(), listing.start_after());

    Ok(())
}

/// A fetch inside a batch near the end of a partition lists as many batch objects in a
/// partition of 500 batches as in one of 50.
#[tokio::test]
async fn head_fetch_listing_does_not_grow_with_the_partition() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let mut listed = vec![];

    for batches in [50, 500] {
        let (storage, listing, topition) = partition(&format!("head_fetch_{batches}")).await?;
        produce(&storage, &topition, batches, 100).await?;
        listing.reset_counts();

        // The last offset is inside the last batch, so the fetch lists to find that batch.
        let last = i64::try_from(batches)? * 100 - 1;
        let base_offsets =
            fetch_base_offsets(&storage, &topition, last, MAX_BYTES, READ_TIME).await?;

        assert_eq!(vec![last - 99], base_offsets);
        listed.push(listing.listed_batches());
    }

    assert!(listed[0] > 0, "{listed:?}");
    assert_eq!(listed[0], listed[1], "{listed:?}");
    assert!(listed[1] <= 6, "{listed:?}");

    Ok(())
}

/// A fetch at an offset inside a batch returns that batch whole, as Kafka
/// does, also when the batch starts far below the fetch offset.
#[tokio::test]
async fn fetch_inside_a_batch_returns_the_batch_whole() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, _, topition) = partition("small_batches").await?;
    produce(&storage, &topition, 5, 10).await?;

    assert_eq!(
        vec![10, 20, 30, 40],
        fetch_base_offsets(&storage, &topition, 15, MAX_BYTES, READ_TIME).await?
    );

    let (storage, _, topition) = partition("large_batch").await?;
    produce(&storage, &topition, 3, 10).await?;
    produce(&storage, &topition, 1, 1000).await?;
    produce(&storage, &topition, 1, 1).await?;

    assert_eq!(
        vec![30, 1030],
        fetch_base_offsets(&storage, &topition, 900, MAX_BYTES, READ_TIME).await?
    );
    assert_eq!(
        vec![30, 1030],
        fetch_base_offsets(&storage, &topition, 1029, MAX_BYTES, READ_TIME).await?
    );

    Ok(())
}

/// A fetch whose first batch fills `max_bytes` returns that batch, and lists
/// only up to it.
#[tokio::test]
async fn fetch_stops_listing_at_max_bytes() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, listing, topition) = partition("fetch_stops_listing_at_max_bytes").await?;
    produce(&storage, &topition, 100, 1).await?;
    listing.reset_counts();

    assert_eq!(
        vec![0],
        fetch_base_offsets(&storage, &topition, 0, 1, READ_TIME).await?
    );
    assert!(
        listing.listed_batches() <= 2,
        "{}",
        listing.listed_batches()
    );

    Ok(())
}

/// A slow listing slows only the first step of a fetch: finding the batch that holds the fetch
/// offset. The fetch then reads each later batch straight from its object key, which it builds
/// from the previous batch's last offset, so it does not list those batches. It reads all 15
/// batches from the one that holds offset 1,550 before its deadline.
#[tokio::test(start_paused = true)]
async fn slow_listing_delays_only_the_first_batch() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, listing, topition) =
        partition("slow_listing_delays_only_the_first_batch").await?;
    produce(&storage, &topition, 30, 100).await?;

    // Listing each batch takes 100ms. To find the batch that holds 1,550, the fetch lists the 6
    // batches from 1,100 to 1,600, in 600ms. Listing the 15 batches from 1,500 on would take
    // another 1.5s, past the deadline.
    listing.delay_each_listed_object(Duration::from_millis(100));

    let base_offsets = fetch_base_offsets(
        &storage,
        &topition,
        1_550,
        MAX_BYTES,
        Duration::from_secs(1),
    )
    .await?;

    assert_eq!(
        (1_500..3_000).step_by(100).collect::<Vec<_>>(),
        base_offsets
    );

    Ok(())
}

/// If a fetch's read of a batch object does not finish by its deadline, the fetch answers at that
/// deadline, with the batches that it finished reading before then.
#[tokio::test(start_paused = true)]
async fn stalled_batch_read_answers_at_the_deadline() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, listing, topition) =
        partition("stalled_batch_read_answers_at_the_deadline").await?;
    produce(&storage, &topition, 10, 1).await?;

    // The fetch reads the batch at offset 0, and its read of the batch at offset 1 never ends.
    listing.stall_batch_reads_after(1);

    let read_time = Duration::from_secs(1);
    let started_at = Instant::now();

    let base_offsets = fetch_base_offsets(&storage, &topition, 0, MAX_BYTES, read_time).await?;

    assert_eq!(vec![0], base_offsets);
    assert_eq!(read_time, started_at.elapsed());

    Ok(())
}

/// If a fetch's listing does not reach the batch at the fetch offset by its deadline, the fetch
/// answers empty at that deadline, instead of holding the read until the listing ends.
#[tokio::test(start_paused = true)]
async fn slower_listing_than_the_deadline_answers_empty_at_the_deadline() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, listing, topition) =
        partition("slower_listing_than_the_deadline_answers_empty_at_the_deadline").await?;
    produce(&storage, &topition, 10, 10).await?;

    // Listing one batch takes longer than the read time, so the listing does not reach the batch
    // at the fetch offset by the deadline.
    listing.delay_each_listed_object(Duration::from_secs(2));

    let read_time = Duration::from_secs(1);
    let started_at = Instant::now();

    // 85 is inside a batch, so the fetch lists to find that batch.
    let base_offsets = fetch_base_offsets(&storage, &topition, 85, MAX_BYTES, read_time).await?;

    assert_eq!(Vec::<i64>::new(), base_offsets);
    assert_eq!(read_time, started_at.elapsed());

    Ok(())
}

/// A store that lists batch keys out of order still answers a fetch with the
/// right batches, in offset order.
#[tokio::test]
async fn out_of_order_listing_falls_back_to_a_full_listing() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, listing, topition) =
        partition("out_of_order_listing_falls_back_to_a_full_listing").await?;
    produce(&storage, &topition, 10, 1).await?;
    produce(&storage, &topition, 5, 10).await?;
    listing.list_in_descending_order();

    assert_eq!(
        (5..10).chain([10, 20, 30, 40, 50]).collect::<Vec<_>>(),
        fetch_base_offsets(&storage, &topition, 5, MAX_BYTES, READ_TIME).await?
    );
    assert_eq!(
        vec![20, 30, 40, 50],
        fetch_base_offsets(&storage, &topition, 25, MAX_BYTES, READ_TIME).await?
    );
    assert_eq!(
        vec![20],
        fetch_base_offsets(&storage, &topition, 25, 1, READ_TIME).await?
    );

    Ok(())
}

/// A store that lists batch keys out of key order still answers a fetch with
/// the batch that holds the fetch offset, also when the part of the listing
/// that the fetch reads is in ascending order and `max_bytes` stops the
/// fetch after one batch.
#[tokio::test]
async fn ascending_but_out_of_order_listing_falls_back_to_a_full_listing() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, listing, topition) =
        partition("ascending_but_out_of_order_listing_falls_back_to_a_full_listing").await?;
    produce(&storage, &topition, 2, 10).await?;
    produce(&storage, &topition, 1, 1).await?;
    listing.list_last_two_swapped();

    assert_eq!(
        vec![10],
        fetch_base_offsets(&storage, &topition, 15, 1, READ_TIME).await?
    );

    Ok(())
}

/// A fetch uses the listing only to find its first batch, and then reads each later batch by its
/// key. So if the listing is out of order after that first batch, the fetch still returns the
/// batches in offset order, without listing the whole partition.
#[tokio::test]
async fn out_of_order_listing_after_the_holding_batch_lists_nothing_more() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, listing, topition) =
        partition("out_of_order_listing_after_the_holding_batch_lists_nothing_more").await?;
    produce(&storage, &topition, 2, 10).await?;
    produce(&storage, &topition, 1, 1).await?;
    // The store lists the batches at 0, 20 and 10, in that order.
    listing.list_last_two_swapped();
    listing.reset_counts();

    // The fetch offset is inside the first batch, so the fetch lists to find that batch.
    assert_eq!(
        vec![0, 10, 20],
        fetch_base_offsets(&storage, &topition, 5, MAX_BYTES, READ_TIME).await?
    );
    // The search for the batch that holds offset 5 lists that batch and the one after it, and
    // nothing more. A full listing would list all three batches, after that search.
    assert_eq!(2, listing.listed_batches());

    Ok(())
}

/// A fetch below the first batch of the partition, such as one whose batch
/// was deleted after the fetch read the log start, returns from the first batch.
#[tokio::test]
async fn fetch_below_the_first_batch_returns_from_the_first_batch() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let name = "fetch_below_the_first_batch_returns_from_the_first_batch";
    let (storage, _, topition) = partition(name).await?;
    produce(&storage, &topition, 3, 1).await?;

    // DeleteRecords moves the log start to 1, and `maintain` would delete the batch at 0 later.
    // This deletes it at once.
    _ = storage
        .delete_records(&[DeleteRecordsTopic::default()
            .name(name.into())
            .partitions(Some(
                [DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(1)]
                .into(),
            ))])
        .await?;
    storage
        .object_store
        .delete(&batch_path(&storage.cluster, &topition, 0))
        .await?;

    assert_eq!(
        vec![1, 2],
        fetch_base_offsets(&storage, &topition, 0, MAX_BYTES, READ_TIME).await?
    );

    Ok(())
}

/// A fetch skips a settled gap, such as one that a broker leaves when it stops between its offset
/// write and its batch write, and returns the batches around it: the batch that holds the fetch
/// offset, or the first batch after the gap for an offset inside it.
#[tokio::test]
async fn fetch_skips_a_gap_to_the_batch_after_it() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, listing, topition) = partition("fetch_skips_a_gap_to_the_batch_after_it").await?;
    produce(&storage, &topition, 3, 10).await?;
    produce(&storage, &topition, 1, 1).await?;

    storage
        .object_store
        .delete(&batch_path(&storage.cluster, &topition, 10))
        .await?;
    listing.backdate_writes(BATCH_SETTLED_AFTER);

    assert_eq!(
        vec![0, 20, 30],
        fetch_base_offsets(&storage, &topition, 5, MAX_BYTES, READ_TIME).await?
    );

    assert_eq!(
        vec![0],
        fetch_base_offsets(&storage, &topition, 5, 1, READ_TIME).await?
    );
    assert_eq!(
        vec![20, 30],
        fetch_base_offsets(&storage, &topition, 15, MAX_BYTES, READ_TIME).await?
    );

    Ok(())
}

/// A fetch stops at the offsets of a produce that is still writing its batch, even when a later
/// produce to the partition has already written its own batch. The fetch returns the first batch
/// once it lands. A consumer that skipped past those offsets would never read their records.
#[tokio::test]
async fn fetch_stops_at_a_produce_that_is_still_writing_its_batch() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = HeldPutStore::new(held_put_store::is_batch);
    let storage = DynoStore::new("nisshi", 111, store.clone())?;
    let topition = create_partition(
        &storage,
        "fetch_stops_at_a_produce_that_is_still_writing_its_batch",
    )
    .await?;
    produce(&storage, &topition, 1, 10).await?;

    // Produce A takes offsets 10 to 19, and waits at the write of its batch object.
    let held = store.hold_next_put();
    let produce_a = tokio::spawn({
        let storage = storage.clone();
        let topition = topition.clone();
        async move { produce(&storage, &topition, 1, 10).await }
    });
    held.wait_until_held().await?;

    // Produce B takes offsets 20 to 29, and writes its batch object.
    produce(&storage, &topition, 1, 10).await?;

    assert!(
        fetch_base_offsets(&storage, &topition, 10, MAX_BYTES, READ_TIME)
            .await?
            .is_empty()
    );

    held.release();
    produce_a
        .await
        .map_err(|error| Error::Message(error.to_string()))??;

    assert_eq!(
        vec![10, 20],
        fetch_base_offsets(&storage, &topition, 10, MAX_BYTES, READ_TIME).await?
    );

    Ok(())
}

/// A fetch skips the offsets of a produce that failed to write its batch at once, because the
/// produce recorded its failure, although the batch after them was written just now.
#[tokio::test]
async fn fetch_skips_the_offsets_of_a_failed_produce_at_once() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = FailingObjectStore::failing_put_on_demand(InMemory::new());
    let storage = DynoStore::new("nisshi", 111, store.clone())?;
    let topition = create_partition(
        &storage,
        "fetch_skips_the_offsets_of_a_failed_produce_at_once",
    )
    .await?;
    produce(&storage, &topition, 1, 10).await?;

    // The produce takes offsets 10 to 19, and fails to write its batch object.
    store.fail_next_put(|location, _| held_put_store::is_batch(location));
    assert!(produce(&storage, &topition, 1, 10).await.is_err());

    produce(&storage, &topition, 2, 10).await?;

    assert_eq!(
        vec![0, 20, 30],
        fetch_base_offsets(&storage, &topition, 0, MAX_BYTES, READ_TIME).await?
    );
    assert_eq!(
        vec![20, 30],
        fetch_base_offsets(&storage, &topition, 15, MAX_BYTES, READ_TIME).await?
    );

    Ok(())
}

/// A fetch that skips a failed produce's offsets reads the batch of the next produce, also when
/// that produce stored its batch and still reported a failure, which recorded its offsets too.
#[tokio::test]
async fn fetch_reads_a_stored_batch_whose_produce_reported_a_failure() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let store = FailingObjectStore::failing_put_on_demand(InMemory::new());
    let storage = DynoStore::new("nisshi", 111, store.clone())?;
    let topition = create_partition(
        &storage,
        "fetch_reads_a_stored_batch_whose_produce_reported_a_failure",
    )
    .await?;
    produce(&storage, &topition, 1, 10).await?;

    // The produce of offsets 10 to 19 writes no batch.
    store.fail_next_put(|location, _| held_put_store::is_batch(location));
    assert!(produce(&storage, &topition, 1, 10).await.is_err());

    // The produce of offsets 20 to 29 stores its batch, and its put still reports an error.
    store.fail_next_put_after_it_stores(|location, _| held_put_store::is_batch(location));
    assert!(produce(&storage, &topition, 1, 10).await.is_err());

    produce(&storage, &topition, 1, 10).await?;

    assert_eq!(
        vec![0, 20, 30],
        fetch_base_offsets(&storage, &topition, 0, MAX_BYTES, READ_TIME).await?
    );

    Ok(())
}

/// A topic created again under the name of a deleted topic starts with no failed produces, also
/// when a produce of the deleted topic recorded its failure after the deletion.
#[tokio::test]
async fn a_recreated_topic_has_no_failed_produces_of_the_deleted_topic() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new("nisshi", 111, InMemory::new())?;
    let name = "a_recreated_topic_has_no_failed_produces_of_the_deleted_topic";
    let topition = create_partition(&storage, name).await?;

    _ = storage.delete_topic(&TopicId::Name(name.into())).await?;
    storage.record_failed_produce(&topition, 10..20).await;
    _ = create_partition(&storage, name).await?;

    let failed_produces = storage
        .watermark_for(&topition)?
        .with(&storage.object_store, |watermark| {
            Ok(watermark.failed_produces.clone())
        })
        .await?;

    assert!(failed_produces.is_empty(), "{failed_produces:?}");

    Ok(())
}

/// A fetch stops at a gap with no recorded failed produce while the batch after it is younger than
/// [`BATCH_SETTLED_AFTER`], because a produce can still be writing the gap's batch, and skips the
/// gap once that batch is older.
#[tokio::test]
async fn fetch_skips_a_gap_with_no_recorded_failure_only_once_it_settles() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, listing, topition) =
        partition("fetch_skips_a_gap_with_no_recorded_failure_only_once_it_settles").await?;
    produce(&storage, &topition, 3, 10).await?;

    // The produce at 10 took its offsets and stopped before it wrote its batch or recorded its
    // failure, as a broker that stops between the two writes does.
    storage
        .object_store
        .delete(&batch_path(&storage.cluster, &topition, 10))
        .await?;

    assert_eq!(
        vec![0],
        fetch_base_offsets(&storage, &topition, 0, MAX_BYTES, READ_TIME).await?
    );
    assert!(
        fetch_base_offsets(&storage, &topition, 10, MAX_BYTES, READ_TIME)
            .await?
            .is_empty()
    );

    // A minute short of settled, the batch after the gap is still too young to skip the gap.
    listing.backdate_writes(BATCH_SETTLED_AFTER - Duration::from_secs(60));

    assert!(
        fetch_base_offsets(&storage, &topition, 10, MAX_BYTES, READ_TIME)
            .await?
            .is_empty()
    );

    listing.backdate_writes(BATCH_SETTLED_AFTER);

    assert_eq!(
        vec![0, 20],
        fetch_base_offsets(&storage, &topition, 0, MAX_BYTES, READ_TIME).await?
    );
    assert_eq!(
        vec![20],
        fetch_base_offsets(&storage, &topition, 10, MAX_BYTES, READ_TIME).await?
    );

    Ok(())
}

/// A fetch that reaches a gap with no recorded failed produce lists only the first keys after the
/// gap on a store that lists keys in order, however many batches follow the gap.
#[tokio::test]
async fn gap_lookup_lists_only_the_first_keys_after_the_gap() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, listing, topition) =
        partition("gap_lookup_lists_only_the_first_keys_after_the_gap").await?;
    produce(&storage, &topition, 50, 10).await?;

    // The produce at 10 took its offsets and stopped before it wrote its batch or recorded its
    // failure.
    storage
        .object_store
        .delete(&batch_path(&storage.cluster, &topition, 10))
        .await?;
    listing.backdate_writes(BATCH_SETTLED_AFTER);

    listing.reset_counts();
    assert_eq!(
        vec![20],
        fetch_base_offsets(&storage, &topition, 10, 1, READ_TIME).await?
    );

    // The gap's lookup listed two keys, and the lookup of the batch that holds offset 10 listed
    // the batch at 0 and the first key above 10.
    assert!(
        listing.listed_batches() <= 4,
        "listed {} batches",
        listing.listed_batches()
    );

    Ok(())
}

/// A fetch that crosses a settled gap on a store that lists keys out of order goes on from the
/// first batch after the gap, not from the first batch that the store lists.
#[tokio::test]
async fn gap_in_an_out_of_order_listing_goes_on_from_the_next_batch() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, listing, topition) =
        partition("gap_in_an_out_of_order_listing_goes_on_from_the_next_batch").await?;
    produce(&storage, &topition, 4, 10).await?;

    storage
        .object_store
        .delete(&batch_path(&storage.cluster, &topition, 10))
        .await?;

    // The listing after the gap returns the batch at 30 before the batch at 20.
    listing.list_in_descending_order();
    listing.backdate_writes(BATCH_SETTLED_AFTER);

    assert_eq!(
        vec![0, 20, 30],
        fetch_base_offsets(&storage, &topition, 0, MAX_BYTES, READ_TIME).await?
    );

    Ok(())
}

/// A fetch that reaches a base offset without a batch object at the end of the partition, such
/// as while a produce writes its batch, stops there without listing the whole partition.
#[tokio::test]
async fn gap_at_the_end_of_the_partition_does_not_list_the_whole_partition() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, listing, topition) =
        partition("gap_at_the_end_of_the_partition_does_not_list_the_whole_partition").await?;

    // The partition holds more offsets than the first listing window below a fetch offset, so
    // the search for the batch that holds the fetch offset never lists from offset 0, which the
    // store counts as a full listing.
    let batches = usize::try_from(FETCH_LISTING_FIRST_WINDOW)? + 100;
    produce(&storage, &topition, batches, 1).await?;
    let last = i64::try_from(batches)? - 1;

    // The high watermark stays past `last`, so `last` is taken but holds no batch object, as it
    // does while the produce that took it writes its batch.
    storage
        .object_store
        .delete(&batch_path(&storage.cluster, &topition, last))
        .await?;

    listing.reset_counts();

    assert_eq!(
        vec![last - 1],
        fetch_base_offsets(&storage, &topition, last - 1, MAX_BYTES, READ_TIME).await?
    );
    assert!(
        fetch_base_offsets(&storage, &topition, last, MAX_BYTES, READ_TIME)
            .await?
            .is_empty()
    );
    assert_eq!(0, listing.full_lists());

    Ok(())
}

/// A fetch returns first the batch that holds the fetch offset, for offsets
/// at each end of a batch and on each side of the edge of each listing's
/// window below the fetch offset.
#[tokio::test]
async fn fetch_returns_first_the_batch_that_holds_the_offset() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, _, topition) =
        partition("fetch_returns_first_the_batch_that_holds_the_offset").await?;

    let mut batches = vec![];
    let mut base_offset = 0;

    for records_per_batch in [1, 512, 1, 513, 2, 4097, 1] {
        produce(&storage, &topition, 1, records_per_batch).await?;

        let records = i64::try_from(records_per_batch)?;
        batches.push(base_offset..base_offset + records);
        base_offset += records;
    }

    for batch in batches {
        let offsets = [0, 1, 511, 512, 513, 4095, 4096, 4097]
            .map(|records_before| batch.start + records_before)
            .into_iter()
            .chain([batch.end - 1])
            .filter(|offset| batch.contains(offset));

        for offset in offsets {
            assert_eq!(
                Some(batch.start),
                fetch_base_offsets(&storage, &topition, offset, 1, READ_TIME)
                    .await?
                    .first()
                    .copied(),
                "{offset}"
            );
        }
    }

    Ok(())
}

/// A fetch whose listing fails answers `KAFKA_STORAGE_ERROR`, which a client
/// retries, as Kafka answers for a log that it cannot read.
#[tokio::test]
async fn failed_listing_answers_kafka_storage_error() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let name = "failed_listing_answers_kafka_storage_error";
    let topition = Topition::new(name, 0);

    let writer = DynoStore::new("nisshi", 111, bucket.clone())?;

    _ = writer
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

    produce(&writer, &topition, 1, 3).await?;

    let reader = DynoStore::new("nisshi", 111, FailingObjectStore::failing_list(bucket))?;

    // Offset 1 is inside the batch, so the fetch lists to find that batch.
    assert_matches!(
        reader
            .fetch_until_deadline(
                &topition,
                1,
                1,
                MAX_BYTES,
                IsolationLevel::ReadUncommitted,
                Instant::now() + READ_TIME,
            )
            .await
            .map(|read| read.batches),
        Err(Error::Api(ErrorCode::KafkaStorageError))
    );

    Ok(())
}
