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

//! Tests for how much of a partition's listing a Fetch reads: it lists from
//! near the fetch offset, and stops at `max_bytes`, so its cost does not
//! grow with the partition.

use std::{
    assert_matches,
    fmt::Display,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    StreamExt as _,
    stream::{self, BoxStream},
};
use nisshi_sans_io::{
    ErrorCode, IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, inflated},
};
use nisshi_storage::{Error, Result, Storage as _, Topition};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt as _, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};

use crate::dynostore::{
    DynoStore, batch_path,
    tests::{failing_object_store::FailingObjectStore, init_tracing},
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
                if object
                    .as_ref()
                    .is_ok_and(|meta| meta.location.as_ref().ends_with(".batch"))
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
        self.inner.get_opts(location, options).await
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
            return self.inner.list(prefix);
        }

        _ = self.full_lists.fetch_add(1, Ordering::SeqCst);
        self.shape(self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        if !lists_batches(prefix) {
            return self.inner.list_with_offset(prefix, offset);
        }

        if let Ok(mut start_after) = self.start_after.lock() {
            start_after.push(offset.to_owned());
        }

        self.shape(self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.inner.list_with_delimiter(prefix).await
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

    Ok((storage, listing, Topition::new(name, 0)))
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

/// The base offsets of the batches that a fetch at `offset` returns.
async fn fetch_base_offsets(
    storage: &DynoStore,
    topition: &Topition,
    offset: i64,
    max_bytes: u32,
    max_wait: Duration,
) -> Result<Vec<i64>> {
    Ok(storage
        .fetch(
            topition,
            offset,
            1,
            max_bytes,
            IsolationLevel::ReadUncommitted,
            max_wait,
        )
        .await?
        .iter()
        .map(|batch| batch.base_offset)
        .collect())
}

const MAX_BYTES: u32 = 50 * 1024 * 1024;

const MAX_WAIT: Duration = Duration::from_secs(60);

/// A fetch at a batch's base offset lists from that batch, with a
/// start-after LIST, instead of from the first batch of the partition.
#[tokio::test]
async fn head_fetch_lists_after_the_fetch_offset() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, listing, topition) = partition("head_fetch_lists_after_the_fetch_offset").await?;
    produce(&storage, &topition, 20, 1).await?;
    listing.reset_counts();

    let base_offsets = fetch_base_offsets(&storage, &topition, 10, MAX_BYTES, MAX_WAIT).await?;

    assert_eq!((10..20).collect::<Vec<_>>(), base_offsets);
    assert_eq!(0, listing.full_lists());
    assert_eq!(
        vec![batch_path(&storage.cluster, &topition, 9)],
        listing.start_after()
    );

    Ok(())
}

/// A fetch near the end of a partition lists as many batch objects in a
/// partition of 500 batches as in one of 50.
#[tokio::test]
async fn head_fetch_listing_does_not_grow_with_the_partition() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let mut listed = vec![];

    for batches in [50, 500] {
        let (storage, listing, topition) = partition(&format!("head_fetch_{batches}")).await?;
        produce(&storage, &topition, batches, 1).await?;
        listing.reset_counts();

        let last = i64::try_from(batches)? - 1;
        let base_offsets =
            fetch_base_offsets(&storage, &topition, last, MAX_BYTES, MAX_WAIT).await?;

        assert_eq!(vec![last], base_offsets);
        listed.push(listing.listed_batches());
    }

    assert_eq!(listed[0], listed[1], "{listed:?}");
    assert!(listed[1] <= 2, "{listed:?}");

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
        fetch_base_offsets(&storage, &topition, 15, MAX_BYTES, MAX_WAIT).await?
    );

    let (storage, _, topition) = partition("large_batch").await?;
    produce(&storage, &topition, 3, 10).await?;
    produce(&storage, &topition, 1, 1000).await?;
    produce(&storage, &topition, 1, 1).await?;

    assert_eq!(
        vec![30, 1030],
        fetch_base_offsets(&storage, &topition, 900, MAX_BYTES, MAX_WAIT).await?
    );
    assert_eq!(
        vec![30, 1030],
        fetch_base_offsets(&storage, &topition, 1029, MAX_BYTES, MAX_WAIT).await?
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
        fetch_base_offsets(&storage, &topition, 0, 1, MAX_WAIT).await?
    );
    assert!(
        listing.listed_batches() <= 2,
        "{}",
        listing.listed_batches()
    );

    Ok(())
}

/// A listing slower than the fetch's `max_wait` still answers with the batch
/// at the fetch offset, instead of answering empty on every retry.
#[tokio::test]
async fn slow_listing_still_returns_a_batch() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, listing, topition) = partition("slow_listing_still_returns_a_batch").await?;
    produce(&storage, &topition, 100, 1).await?;

    // Listing the 80 batches below the fetch offset takes 4s, far more than
    // the fetch's `max_wait`. The fetch must not depend on how fast the test
    // runs, so it returns the batch at the fetch offset however long its
    // listing takes.
    listing.delay_each_listed_object(Duration::from_millis(50));

    let base_offsets = fetch_base_offsets(
        &storage,
        &topition,
        80,
        MAX_BYTES,
        Duration::from_millis(500),
    )
    .await?;

    assert_eq!(Some(&80), base_offsets.first(), "{base_offsets:?}");

    // Each of the 20 batches from the fetch offset takes at least 50ms to
    // list, so the deadline ends the fetch before it reads them all.
    assert!(base_offsets.len() < 20, "{base_offsets:?}");

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
        fetch_base_offsets(&storage, &topition, 5, MAX_BYTES, MAX_WAIT).await?
    );
    assert_eq!(
        vec![20, 30, 40, 50],
        fetch_base_offsets(&storage, &topition, 25, MAX_BYTES, MAX_WAIT).await?
    );
    assert_eq!(
        vec![20],
        fetch_base_offsets(&storage, &topition, 25, 1, MAX_WAIT).await?
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
        fetch_base_offsets(&storage, &topition, 15, 1, MAX_WAIT).await?
    );

    Ok(())
}

/// A fetch below the first batch of the partition, such as one whose batch
/// was deleted after the fetch read the log start, returns from the first
/// batch.
#[tokio::test]
async fn fetch_below_the_first_batch_returns_from_the_first_batch() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, _, topition) =
        partition("fetch_below_the_first_batch_returns_from_the_first_batch").await?;
    produce(&storage, &topition, 3, 1).await?;

    storage
        .object_store
        .delete(&batch_path(&storage.cluster, &topition, 0))
        .await?;

    assert_eq!(
        vec![1, 2],
        fetch_base_offsets(&storage, &topition, 0, MAX_BYTES, MAX_WAIT).await?
    );

    Ok(())
}

/// A gap in a listing in key order, such as one that a failed produce
/// leaves, makes the fetch list the whole partition, and the fetch still
/// returns the batches around the gap: the batch that holds the fetch
/// offset, or the first batch after the gap for an offset inside it.
#[tokio::test]
async fn gap_in_an_in_order_listing_falls_back_to_a_full_listing() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let (storage, listing, topition) =
        partition("gap_in_an_in_order_listing_falls_back_to_a_full_listing").await?;
    produce(&storage, &topition, 3, 10).await?;
    produce(&storage, &topition, 1, 1).await?;

    storage
        .object_store
        .delete(&batch_path(&storage.cluster, &topition, 10))
        .await?;

    listing.reset_counts();

    assert_eq!(
        vec![0, 20, 30],
        fetch_base_offsets(&storage, &topition, 5, MAX_BYTES, MAX_WAIT).await?
    );
    assert!(listing.full_lists() > 0, "{}", listing.full_lists());

    assert_eq!(
        vec![0],
        fetch_base_offsets(&storage, &topition, 5, 1, MAX_WAIT).await?
    );
    assert_eq!(
        vec![20, 30],
        fetch_base_offsets(&storage, &topition, 15, MAX_BYTES, MAX_WAIT).await?
    );

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
                fetch_base_offsets(&storage, &topition, offset, 1, MAX_WAIT)
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

    produce(&writer, &topition, 3, 1).await?;

    let reader = DynoStore::new("nisshi", 111, FailingObjectStore::failing_list(bucket))?;

    assert_matches!(
        reader
            .fetch(
                &topition,
                1,
                1,
                MAX_BYTES,
                IsolationLevel::ReadUncommitted,
                MAX_WAIT,
            )
            .await,
        Err(Error::Api(ErrorCode::KafkaStorageError))
    );

    Ok(())
}
