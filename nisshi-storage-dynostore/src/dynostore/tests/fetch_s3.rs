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

//! Tests for the byte limit of a fetch on S3, through `object_store`'s HTTP client. A fetch reads
//! a batch with a ranged GET, and the HTTP client fails a ranged GET unless the store answers
//! 206 Partial Content with the range that it asked for, clamped to the object's size. An
//! in-memory store does not check the answer.
//!
//! The tests need the S3 store that `AWS_ENDPOINT` names, with a bucket named `nisshi`, as the
//! docker compose MinIO service provides.

use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt as _;
use nisshi_sans_io::{
    ByteSize as _, IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{
    ByteLimit, Result, Storage as _, Topition,
    fetched::{FetchRead, FetchUntilDeadline as _},
};
use object_store::{ObjectStore as _, ObjectStoreExt as _, aws::AmazonS3Builder, path::Path};
use tokio::time::Instant;
use uuid::Uuid;

use crate::dynostore::{DynoStore, tests::init_tracing};

/// The bucket that the docker compose MinIO service creates.
const BUCKET: &str = "nisshi";

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

/// A one-partition topic on S3 that holds three batches: one record of 10 bytes at offset 0, one
/// record of 1,000 bytes at offset 1, and two records of 1,000 bytes at offsets 2 and 3.
struct S3Partition {
    storage: DynoStore,
    topition: Topition,
    sizes: [u32; 3],
}

impl S3Partition {
    /// Creates the topic in a cluster of its own, so that the objects of one run do not meet those
    /// of another, and produces its batches.
    async fn create() -> Result<Self> {
        let cluster = format!("fetch-s3-{}", Uuid::new_v4());
        let object_store = AmazonS3Builder::from_env()
            .with_bucket_name(BUCKET)
            .build()?;
        let storage = DynoStore::new(&cluster, 111, object_store)?;

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
        let batches = [
            batch_of_records(1, 10)?,
            batch_of_records(1, 1_000)?,
            batch_of_records(2, 1_000)?,
        ];
        let mut sizes = [0; 3];

        for (size, batch) in sizes.iter_mut().zip(batches) {
            *size = u32::try_from(batch.size_in_bytes()?)?;
            _ = storage.produce(None, &topition, batch).await?;
        }

        Ok(Self {
            storage,
            topition,
            sizes,
        })
    }

    /// Reads the partition from `offset` within `limit`.
    async fn fetch_from(&self, offset: i64, limit: ByteLimit) -> Result<FetchRead> {
        self.storage
            .fetch_until_limit(
                &self.topition,
                offset,
                1,
                limit,
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_secs(10),
            )
            .await
    }

    /// Deletes every object of the partition's cluster, so that the bucket does not grow with each
    /// run. A test calls it before its assertions, so that a failed test also deletes its objects.
    async fn delete(self) -> Result<()> {
        let object_store = AmazonS3Builder::from_env()
            .with_bucket_name(BUCKET)
            .build()?;
        let cluster = Path::from(format!("clusters/{}", self.storage.cluster));

        let locations = object_store
            .list(Some(&cluster))
            .map_ok(|meta| meta.location)
            .try_collect::<Vec<_>>()
            .await?;

        for location in locations {
            object_store.delete(&location).await?;
        }

        Ok(())
    }
}

fn base_offsets(read: &FetchRead) -> Vec<i64> {
    read.batches.iter().map(|batch| batch.base_offset).collect()
}

/// A fetch under a limit larger than every object asks for more bytes than each object holds.
/// S3 answers each ranged GET with the whole object, and the fetch returns every batch, under a
/// soft and under a hard limit.
#[tokio::test]
async fn a_limit_larger_than_the_objects_returns_every_batch() -> Result<()> {
    let _guard = init_tracing()?;

    let partition = S3Partition::create().await?;
    let limits = [ByteLimit::Soft(1024 * 1024), ByteLimit::Hard(1024 * 1024)];

    let mut reads = Vec::new();
    for limit in limits {
        reads.push(partition.fetch_from(0, limit).await?);
    }

    partition.delete().await?;

    for (limit, read) in limits.iter().zip(reads) {
        assert_eq!(vec![0, 1, 2], base_offsets(&read), "{limit:?}");
        assert!(!read.reached_limit, "{limit:?}");
    }

    Ok(())
}

/// A fetch whose limit ends inside a batch downloads only part of that batch, and returns the
/// batches before it. Under a soft limit of 1 byte, the first batch exceeds the limit, and the
/// fetch stops at the second. Under a hard limit of the first two batches and half the third, the
/// fetch stops at the third. Under a hard limit of half the first batch, it returns nothing.
#[tokio::test]
async fn a_batch_that_does_not_fit_stops_the_fetch() -> Result<()> {
    let _guard = init_tracing()?;

    let partition = S3Partition::create().await?;
    let [first, second, third] = partition.sizes;

    let cases = [
        (ByteLimit::Soft(1), vec![0]),
        (ByteLimit::Hard(first + second + third / 2), vec![0, 1]),
        (ByteLimit::Hard(first / 2), vec![]),
    ];

    let mut reads = Vec::new();
    for (limit, _) in &cases {
        reads.push(partition.fetch_from(0, *limit).await?);
    }

    partition.delete().await?;

    for ((limit, expected), read) in cases.iter().zip(reads) {
        assert_eq!(*expected, base_offsets(&read), "{limit:?}");
        assert!(read.reached_limit, "{limit:?}");
    }

    Ok(())
}

/// A fetch from offset 3, inside the third batch, finds the batch by a listing. Under a hard limit
/// of half the batch, it downloads the batch's header to learn that the batch holds offset 3, and
/// stops there. Under a hard limit larger than the batch, it returns the batch.
#[tokio::test]
async fn a_mid_batch_fetch_reads_its_batch_by_a_ranged_get() -> Result<()> {
    let _guard = init_tracing()?;

    let partition = S3Partition::create().await?;
    let [_, _, third] = partition.sizes;

    let within_half = partition.fetch_from(3, ByteLimit::Hard(third / 2)).await?;
    let within_double = partition.fetch_from(3, ByteLimit::Hard(third * 2)).await?;

    partition.delete().await?;

    assert!(within_half.batches.is_empty());
    assert!(within_half.reached_limit);

    assert_eq!(vec![2], base_offsets(&within_double));
    assert!(!within_double.reached_limit);

    Ok(())
}
