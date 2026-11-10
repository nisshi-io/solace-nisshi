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

//! The deadline contract of [`Storage::fetch`] that the fetch service relies on, on each storage
//! engine.

use nisshi_storage::fetched::FetchUntilDeadline as _;
use std::time::Duration;

use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{Storage, Topition};
use rand::{prelude::*, rng};
use tokio::time::Instant;
use url::Url;
use uuid::Uuid;

use crate::common::{self, StorageType, alphanumeric_string, init_tracing, register_broker};

/// Creates a topic of one partition, and writes `batches` batches of one record each to it.
async fn topic_with_batches<G>(storage: &G, batches: i64) -> Result<Topition>
where
    G: Storage,
{
    let name = alphanumeric_string(15);

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.clone())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(name, 0);

    for _ in 0..batches {
        let batch: deflated::Batch = inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"value").into()))
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage.produce(None, &topition, batch).await?;
    }

    Ok(topition)
}

/// Returns the batches of a fetch of `topition` from offset 0 that stops at `deadline`, and how
/// long the fetch took.
async fn timed_fetch<G>(
    storage: &G,
    topition: &Topition,
    deadline: Instant,
) -> Result<(Vec<deflated::Batch>, Duration)>
where
    G: Storage,
{
    let started_at = Instant::now();

    let batches = storage
        .fetch_until_deadline(
            topition,
            0,
            0,
            u32::MAX,
            IsolationLevel::ReadUncommitted,
            deadline,
        )
        .await
        .map(|read| read.batches)?;

    Ok((batches, started_at.elapsed()))
}

/// Returns the number of records in `batches`.
fn records(batches: &[deflated::Batch]) -> u32 {
    batches.iter().map(|batch| batch.record_count).sum()
}

/// If its deadline has passed, a fetch answers with no batches.
async fn fetch_after_its_deadline_answers_with_no_batches<G>(storage: G) -> Result<()>
where
    G: Storage,
{
    let topition = topic_with_batches(&storage, 1).await?;

    let (batches, _) = timed_fetch(&storage, &topition, Instant::now()).await?;

    assert!(
        batches.is_empty(),
        "the fetch returned {} batches",
        batches.len()
    );

    Ok(())
}

/// If its deadline falls partway through a read of its partition, a fetch answers with part of
/// the partition: batches that run in offset order from the fetch offset, with no gap between them.
async fn fetch_stopped_partway_answers_with_part_of_the_partition<G>(storage: G) -> Result<()>
where
    G: Storage,
{
    const BATCHES: u32 = 2_000;

    let topition = topic_with_batches(&storage, i64::from(BATCHES)).await?;

    // The fastest of three full reads measures how long this engine takes to read the partition.
    // A delay on a busy machine only makes a read slower, so the fastest read is the least delayed.
    let mut full_read = Duration::MAX;

    for _ in 0..3 {
        let (all, elapsed) = timed_fetch(
            &storage,
            &topition,
            Instant::now() + Duration::from_secs(60),
        )
        .await?;

        assert_eq!(BATCHES, records(&all));
        full_read = full_read.min(elapsed);
    }

    // The deadline gives the read half the time that the fastest full read took.
    let (stopped, _) = timed_fetch(&storage, &topition, Instant::now() + full_read / 2).await?;
    let returned = records(&stopped);

    assert!(
        returned < BATCHES,
        "the deadline didn't stop the fetch: it returned all {BATCHES} records"
    );

    // A consumer fetches again from the offset after the last record it got, so a gap would skip
    // the records in it.
    let mut next_offset = 0;

    for batch in &stopped {
        assert_eq!(next_offset, batch.base_offset, "a gap before this batch");
        next_offset = batch.base_offset + i64::from(batch.last_offset_delta) + 1;
    }

    assert!(
        returned > 0,
        "the fetch kept none of the records that it read before its deadline"
    );

    Ok(())
}

/// Returns storage of `storage_type`, with a broker registered.
async fn storage(storage_type: StorageType) -> Result<impl Storage + Clone> {
    let cluster_id = Uuid::now_v7();
    let broker_id = rng().random_range(0..i32::MAX);

    let storage = common::storage_container(
        storage_type,
        cluster_id,
        broker_id,
        Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await?;

    register_broker(cluster_id, broker_id, &storage).await?;

    Ok(storage)
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage_type:expr $(, $stops_partway:ident)?) => {
        #[cfg(feature = $feature)]
        mod $name {
            use super::*;

            #[tokio::test]
            async fn fetch_after_its_deadline_answers_with_no_batches() -> Result<()> {
                let _guard = init_tracing()?;

                let storage = storage($storage_type).await?;

                super::fetch_after_its_deadline_answers_with_no_batches(storage).await
            }

            $(
                #[tokio::test]
                async fn $stops_partway() -> Result<()> {
                    let _guard = init_tracing()?;

                    super::$stops_partway(storage($storage_type).await?).await
                }
            )?
        }
    };
}

// PostgreSQL builds the batches of a fetch after its statement, and checks the deadline only
// between statements, so a deadline that passes while it builds them doesn't stop the read.
// SOL-157421 adds PostgreSQL to the partway test.
backend!(pg, "postgres", StorageType::Postgres);
backend!(
    lite,
    "libsql",
    StorageType::Lite,
    fetch_stopped_partway_answers_with_part_of_the_partition
);
backend!(
    turso,
    "turso",
    StorageType::Turso,
    fetch_stopped_partway_answers_with_part_of_the_partition
);
backend!(
    in_memory,
    "dynostore",
    StorageType::InMemory,
    fetch_stopped_partway_answers_with_part_of_the_partition
);
