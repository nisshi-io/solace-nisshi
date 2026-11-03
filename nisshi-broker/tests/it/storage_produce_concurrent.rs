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

//! Many requests produce to one partition at the same time. Every one of them must be stored, at
//! an offset of its own. An engine that lets two writers collide answers one of them with an
//! error, and a produce that the client believes it sent is lost.

use std::time::Duration;

use crate::common::{self, StorageType, alphanumeric_string, init_tracing};
use bytes::Bytes;
use nisshi_broker::{Error, Result};
use nisshi_sans_io::{
    IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{ArcDynStorage, Storage, Topition};
use rand::{prelude::*, rng};
use url::Url;
use uuid::Uuid;

const PRODUCERS: usize = 16;
const RECORDS_PER_PRODUCER: usize = 20;

async fn concurrent_produce_stores_every_record(storage: ArcDynStorage) -> Result<()> {
    let topic = alphanumeric_string(15);

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(topic, 0);

    let mut producers = Vec::with_capacity(PRODUCERS);
    for producer in 0..PRODUCERS {
        let storage = storage.clone();
        let topition = topition.clone();

        producers.push(tokio::spawn(async move {
            let mut offsets = Vec::with_capacity(RECORDS_PER_PRODUCER);

            for record in 0..RECORDS_PER_PRODUCER {
                let batch = inflated::Batch::builder()
                    .record(
                        Record::builder().value(Some(Bytes::from(format!("{producer}-{record}")))),
                    )
                    .producer_id(-1)
                    .producer_epoch(-1)
                    .build()
                    .and_then(deflated::Batch::try_from)?;

                offsets.push(storage.produce(None, &topition, batch).await?);
            }

            Ok::<_, Error>(offsets)
        }));
    }

    let mut offsets = Vec::with_capacity(PRODUCERS * RECORDS_PER_PRODUCER);
    for producer in producers {
        offsets.extend(
            producer
                .await
                .map_err(|err| Error::Message(err.to_string()))??,
        );
    }

    offsets.sort_unstable();
    offsets.dedup();
    assert_eq!(PRODUCERS * RECORDS_PER_PRODUCER, offsets.len());

    let stored = storage
        .fetch(
            &topition,
            0,
            1,
            16 * 1024 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_secs(60),
        )
        .await?
        .into_iter()
        .map(|batch| inflated::Batch::try_from(batch).map(|batch| batch.records.len()))
        .sum::<Result<usize, _>>()?;
    assert_eq!(PRODUCERS * RECORDS_PER_PRODUCER, stored);

    Ok(())
}

/// Prints the throughput of producers to several partitions at once. Run it by hand:
/// `cargo nextest run -p nisshi-broker --all-features --run-ignored only --no-capture -E 'test(bench_)'`
async fn bench_partitions(
    storage: ArcDynStorage,
    partitions: i32,
    per_partition: usize,
) -> Result<()> {
    const RECORDS: usize = 50;

    let topic = alphanumeric_string(15);

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.clone())
                .num_partitions(partitions)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let started = std::time::Instant::now();

    let mut producers = Vec::new();
    for partition in 0..partitions {
        for _ in 0..per_partition {
            let storage = storage.clone();
            let topition = Topition::new(topic.clone(), partition);

            producers.push(tokio::spawn(async move {
                for record in 0..RECORDS {
                    let batch = inflated::Batch::builder()
                        .record(Record::builder().value(Some(Bytes::from(format!("{record}")))))
                        .producer_id(-1)
                        .producer_epoch(-1)
                        .build()
                        .and_then(deflated::Batch::try_from)?;

                    _ = storage.produce(None, &topition, batch).await?;
                }

                Ok::<_, Error>(())
            }));
        }
    }

    for producer in producers {
        producer
            .await
            .map_err(|err| Error::Message(err.to_string()))??;
    }

    let elapsed = started.elapsed();
    let total = partitions as usize * per_partition * RECORDS;
    println!(
        "BENCH partitions={partitions} producers_per_partition={per_partition} records={total} elapsed={elapsed:?} rate={:.0}/s",
        total as f64 / elapsed.as_secs_f64()
    );

    Ok(())
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage_type:expr) => {
        #[cfg(feature = $feature)]
        mod $name {
            use super::*;

            #[ignore = "a benchmark that prints its result"]
            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn bench_partitions() -> Result<()> {
                let _guard = init_tracing()?;

                for (partitions, per_partition) in [(1, 1), (1, 8), (8, 1), (8, 2), (16, 1)] {
                    let storage = common::storage_container(
                        $storage_type,
                        Uuid::now_v7(),
                        rng().random_range(0..i32::MAX),
                        Url::parse("tcp://127.0.0.1/")?,
                        None,
                    )
                    .await?;

                    super::bench_partitions(storage, partitions, per_partition).await?;
                }

                Ok(())
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn concurrent_produce_stores_every_record() -> Result<()> {
                let _guard = init_tracing()?;

                let storage = common::storage_container(
                    $storage_type,
                    Uuid::now_v7(),
                    rng().random_range(0..i32::MAX),
                    Url::parse("tcp://127.0.0.1/")?,
                    None,
                )
                .await?;

                super::concurrent_produce_stores_every_record(storage).await
            }
        }
    };
}

backend!(lite, "libsql", StorageType::Lite);
backend!(turso, "turso", StorageType::Turso);
