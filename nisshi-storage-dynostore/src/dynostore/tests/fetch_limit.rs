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

//! Tests for the byte limit of a fetch: a fetch sends only the batches that fit its limit, and a
//! soft limit also takes a first batch larger than the limit. Of a batch that does not fit, a fetch
//! downloads at most the bytes left in its limit, as Kafka reads its log only up to the limit.

use std::{
    collections::BTreeMap,
    fmt::Display,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt as _, stream::BoxStream};
use nisshi_sans_io::{
    ByteSize as _, IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{
    ByteLimit, Result, Storage as _, Topition,
    fetched::{FetchRead, FetchUntilDeadline as _},
};
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
    path::Path,
};
use tokio::time::Instant;

use crate::dynostore::{DynoStore, batch_path, tests::init_tracing};

const CLUSTER: &str = "nisshi";

/// An object store that records how many bytes of each object's body a caller downloaded.
#[derive(Clone, Debug, Default)]
struct RecordsDownloads {
    inner: Arc<InMemory>,
    downloaded: Arc<Mutex<BTreeMap<Path, usize>>>,
}

impl RecordsDownloads {
    /// Returns how many bytes of each object's body a caller downloaded.
    fn downloaded(&self) -> BTreeMap<Path, usize> {
        self.downloaded
            .lock()
            .map(|downloaded| downloaded.clone())
            .unwrap_or_default()
    }
}

impl Display for RecordsDownloads {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordsDownloads").finish()
    }
}

#[async_trait]
impl ObjectStore for RecordsDownloads {
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
        let mut result = self.inner.get_opts(location, options).await?;

        result.payload = match result.payload {
            GetResultPayload::Stream(body) => {
                let downloaded = self.downloaded.clone();
                let location = location.clone();

                GetResultPayload::Stream(
                    body.inspect(move |chunk| {
                        if let (Ok(chunk), Ok(mut downloaded)) = (chunk, downloaded.lock()) {
                            *downloaded.entry(location.clone()).or_default() += chunk.len();
                        }
                    })
                    .boxed(),
                )
            }
            #[allow(unreachable_patterns)]
            payload => payload,
        };

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
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.inner.list_with_offset(prefix, offset)
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

/// Returns a batch of one record whose value is `value_len` bytes.
fn batch(value_len: usize) -> Result<deflated::Batch> {
    batch_of_records(1, value_len)
}

/// Returns a batch of `record_count` records, each with a value of `value_len` bytes.
fn batch_of_records(record_count: i32, value_len: usize) -> Result<deflated::Batch> {
    (0..record_count)
        .fold(inflated::Batch::builder(), |builder, offset_delta| {
            builder.record(
                Record::builder()
                    .offset_delta(offset_delta)
                    .value(Some(Bytes::from(vec![b'v'; value_len]))),
            )
        })
        .last_offset_delta(record_count - 1)
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// Creates a one-partition topic on a store that records its downloads, produces one batch for
/// each of `value_lens`, and returns the storage, the store, the partition and the batches.
async fn partition_with_batches(
    value_lens: &[usize],
) -> Result<(DynoStore, RecordsDownloads, Topition, Vec<deflated::Batch>)> {
    let batches = value_lens
        .iter()
        .map(|&value_len| batch(value_len))
        .collect::<Result<Vec<_>>>()?;

    partition_with(batches).await
}

/// Creates a one-partition topic on a store that records its downloads, produces `batches`, and
/// returns the storage, the store, the partition and the batches.
async fn partition_with(
    batches: Vec<deflated::Batch>,
) -> Result<(DynoStore, RecordsDownloads, Topition, Vec<deflated::Batch>)> {
    let store = RecordsDownloads::default();
    let storage = DynoStore::new(CLUSTER, 111, store.clone())?;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name("abc".into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new("abc", 0);

    for batch in &batches {
        _ = storage.produce(None, &topition, batch.clone()).await?;
    }

    Ok((storage, store, topition, batches))
}

/// Reads `topition` from offset 0 within `limit`.
async fn fetch_within(
    storage: &DynoStore,
    topition: &Topition,
    limit: ByteLimit,
) -> Result<FetchRead> {
    fetch_from(storage, topition, 0, limit).await
}

/// Reads `topition` from `offset` within `limit`.
async fn fetch_from(
    storage: &DynoStore,
    topition: &Topition,
    offset: i64,
    limit: ByteLimit,
) -> Result<FetchRead> {
    storage
        .fetch_until_limit(
            topition,
            offset,
            1,
            limit,
            IsolationLevel::ReadUncommitted,
            Instant::now() + Duration::from_secs(5),
        )
        .await
}

fn size(batch: &deflated::Batch) -> Result<u32> {
    Ok(u32::try_from(batch.size_in_bytes()?)?)
}

fn base_offsets(read: &FetchRead) -> Vec<i64> {
    read.batches.iter().map(|batch| batch.base_offset).collect()
}

/// The partition holds one batch with a 1,000-byte record. A fetch under a hard limit of half the
/// batch's size returns nothing and reports that it reached its limit. It downloads only the
/// limit's bytes of the batch.
#[tokio::test]
async fn a_hard_limit_leaves_out_a_first_batch_that_does_not_fit() -> Result<()> {
    let _guard = init_tracing()?;

    let (storage, store, topition, batches) = partition_with_batches(&[1_000]).await?;
    let limit = size(&batches[0])? / 2;

    let read = fetch_within(&storage, &topition, ByteLimit::Hard(limit)).await?;

    assert!(read.batches.is_empty());
    assert!(read.reached_limit);
    assert_eq!(
        BTreeMap::from([(batch_path(CLUSTER, &topition, 0), usize::try_from(limit)?)]),
        store.downloaded()
    );

    Ok(())
}

/// The partition holds one batch with a 1,000-byte record. A fetch under a soft limit of 1 byte
/// still returns that batch, because a soft limit lets the first batch exceed it.
#[tokio::test]
async fn a_soft_limit_takes_a_first_batch_larger_than_the_limit() -> Result<()> {
    let _guard = init_tracing()?;

    let (storage, _store, topition, _batches) = partition_with_batches(&[1_000]).await?;

    let read = fetch_within(&storage, &topition, ByteLimit::Soft(1)).await?;

    assert_eq!(vec![0], base_offsets(&read));

    Ok(())
}

/// The partition holds a small batch, then a batch with a 1,000-byte record. The limit leaves 10
/// bytes after the first batch. The fetch returns the first batch, downloads only 10 bytes of the
/// second, and reports that it reached its limit.
#[tokio::test]
async fn a_fetch_stops_before_a_later_batch_that_does_not_fit() -> Result<()> {
    let _guard = init_tracing()?;

    let (storage, store, topition, batches) = partition_with_batches(&[10, 1_000]).await?;
    let first_size = size(&batches[0])?;

    // The limit leaves 10 bytes after the first batch, too few for the second.
    let read = fetch_within(&storage, &topition, ByteLimit::Soft(first_size + 10)).await?;

    assert_eq!(vec![0], base_offsets(&read));
    assert!(read.reached_limit);
    assert_eq!(
        BTreeMap::from([
            (
                batch_path(CLUSTER, &topition, 0),
                usize::try_from(first_size)?
            ),
            (batch_path(CLUSTER, &topition, 1), 10),
        ]),
        store.downloaded()
    );

    Ok(())
}

/// The partition holds a small batch at offset 0, then a batch of two 1,000-byte records at
/// offsets 1 and 2. A fetch from offset 2, under a hard limit of half the second batch's size,
/// returns nothing and reports that it reached its limit. It downloads only the limit's bytes of
/// the second batch, not the whole batch.
#[tokio::test]
async fn a_mid_batch_fetch_downloads_only_the_limit_of_its_batch() -> Result<()> {
    let _guard = init_tracing()?;

    // The second batch's stored base offset is 0, as its producer sent it, and its object's name
    // says 1. The fetch must take the offset from the name.
    let (storage, store, topition, batches) =
        partition_with(vec![batch(10)?, batch_of_records(2, 1_000)?]).await?;
    let limit = size(&batches[1])? / 2;

    // No batch starts at offset 2, so the fetch finds the batch at 1 by a listing.
    let read = fetch_from(&storage, &topition, 2, ByteLimit::Hard(limit)).await?;

    assert!(read.batches.is_empty());
    assert!(read.reached_limit);
    assert_eq!(
        BTreeMap::from([(batch_path(CLUSTER, &topition, 1), usize::try_from(limit)?)]),
        store.downloaded()
    );

    Ok(())
}

/// The partition holds one batch with a 1,000-byte record. A fetch under a hard limit of exactly
/// the batch's size returns the batch, downloads it whole, and does not report that it reached its
/// limit.
#[tokio::test]
async fn a_hard_limit_of_exactly_a_batch_takes_the_batch() -> Result<()> {
    let _guard = init_tracing()?;

    let (storage, store, topition, batches) = partition_with_batches(&[1_000]).await?;
    let limit = size(&batches[0])?;

    let read = fetch_within(&storage, &topition, ByteLimit::Hard(limit)).await?;

    assert_eq!(vec![0], base_offsets(&read));
    assert!(!read.reached_limit);
    assert_eq!(
        BTreeMap::from([(batch_path(CLUSTER, &topition, 0), usize::try_from(limit)?)]),
        store.downloaded()
    );

    Ok(())
}

/// The partition holds a small batch at offset 0, then a batch of two 1,000-byte records at
/// offsets 1 and 2. A fetch from offset 2 under a hard limit smaller than a batch's header returns
/// nothing and reports that it reached its limit. It downloads the second batch's header, which
/// shows that the batch holds offset 2.
#[tokio::test]
async fn a_mid_batch_fetch_below_the_header_size_downloads_the_header() -> Result<()> {
    let _guard = init_tracing()?;

    let (storage, store, topition, _batches) =
        partition_with(vec![batch(10)?, batch_of_records(2, 1_000)?]).await?;

    // The limit leaves too few bytes to decode the header's last offset delta.
    let limit = u32::try_from(deflated::BATCH_OVERHEAD / 2)?;

    let read = fetch_from(&storage, &topition, 2, ByteLimit::Hard(limit)).await?;

    assert!(read.batches.is_empty());
    assert!(read.reached_limit);
    assert_eq!(
        BTreeMap::from([(batch_path(CLUSTER, &topition, 1), deflated::BATCH_OVERHEAD)]),
        store.downloaded()
    );

    Ok(())
}

/// The partition holds two batches, each with a 1,000-byte record. A fetch under a hard limit of
/// exactly the first batch's size returns the first batch, and reports that it reached its limit,
/// because the first batch leaves no bytes of the limit for the second.
#[tokio::test]
async fn a_batch_that_fills_a_hard_limit_reaches_the_limit() -> Result<()> {
    let _guard = init_tracing()?;

    let (storage, _store, topition, batches) = partition_with_batches(&[1_000, 1_000]).await?;
    let limit = size(&batches[0])?;

    let read = fetch_within(&storage, &topition, ByteLimit::Hard(limit)).await?;

    assert_eq!(vec![0], base_offsets(&read));
    assert!(read.reached_limit);

    Ok(())
}
