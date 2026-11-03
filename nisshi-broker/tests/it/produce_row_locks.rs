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

//! On PostgreSQL, a produce locks only the watermark row of its own partition. Produces to other
//! partitions run while it holds that lock, whether they are in the same topic or in another
//! topic. Produces to the same partition wait for the lock, because each one assigns its offsets
//! from the watermark. Concurrent produces therefore keep each partition's log contiguous, with
//! each acknowledged record stored once, at the offset that its produce returned.

use crate::{
    common::{StorageType, alphanumeric_string, postgres_url, storage_container},
    produce::{create_time_batch, now_ms, produce_batches},
};
use anyhow::Result;
use bytes::Bytes;
use nisshi_sans_io::{
    Compression, ErrorCode, IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{ArcDynStorage, Storage as _, Topition};
use rand::{RngExt as _, rng};
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    task::JoinSet,
    time::{Instant, sleep},
};
use tokio_postgres::{Client, NoTls};
use url::Url;
use uuid::Uuid;

/// The time after which the test treats a running produce as stuck, and fails. A stuck produce
/// waits for something other than the holder's lock.
const STUCK_PRODUCE_LIMIT: Duration = Duration::from_secs(60);

/// A cluster with two topics, `locked_topic` and `other_topic`, of [`PARTITIONS_PER_TOPIC`]
/// partitions each.
struct Cluster {
    name: String,
    storage: ArcDynStorage,
    locked_topic: String,
    other_topic: String,
}

impl Cluster {
    async fn new() -> Result<Self> {
        let name = Uuid::now_v7().to_string();

        let storage = storage_container(
            StorageType::Postgres,
            name.clone(),
            rng().random_range(0..i32::MAX),
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await?;

        let locked_topic = alphanumeric_string(15);
        let other_topic = alphanumeric_string(15);

        for topic in [&locked_topic, &other_topic] {
            _ = storage
                .create_topic(
                    CreatableTopic::default()
                        .name(topic.clone())
                        .num_partitions(PARTITIONS_PER_TOPIC)
                        .replication_factor(1)
                        .assignments(Some([].into()))
                        .configs(Some([].into())),
                    false,
                )
                .await?;
        }

        Ok(Self {
            name,
            storage,
            locked_topic,
            other_topic,
        })
    }

    /// Returns whether a produce of one record to `produced` waits while another transaction, the
    /// holder, keeps `locked` locked, as a produce in progress does.
    ///
    /// When `produced` is a different partition from `locked`, the answer shows whether produces
    /// to different partitions run at the same time. When `produced` is `locked`, the answer shows
    /// that produces to one partition wait for each other.
    ///
    /// The test reads the answer from PostgreSQL instead of from a timer. The produce waits when
    /// `pg_blocking_pids` lists the holder for a backend. The holder then rolls back, so the
    /// produce completes in both cases.
    async fn produce_waits_for_lock(&self, locked: &Topition, produced: &Topition) -> Result<bool> {
        let holder = connect().await?;
        let observer = connect().await?;

        holder.batch_execute("begin").await?;

        let holder_pid: i32 = holder
            .query_one("select pg_backend_pid()", &[])
            .await?
            .get(0);

        let locked = holder
            .query(
                nisshi_storage_sql::watermark_select_for_write()?,
                &[&self.name, &locked.topic(), &locked.partition()],
            )
            .await?;
        assert_eq!(1, locked.len());

        let now = now_ms()?;
        let batch = create_time_batch(Compression::None, now, now, &[0])?;
        let storage = self.storage.clone();
        let produced_topic = produced.topic().to_owned();
        let produced_partition = produced.partition();
        let produce = tokio::spawn(async move {
            produce_batches(storage, &produced_topic, produced_partition, vec![batch]).await
        });

        let started = Instant::now();

        let waited = loop {
            if produce.is_finished() {
                break false;
            }

            assert!(
                started.elapsed() < STUCK_PRODUCE_LIMIT,
                "a produce to {produced:?} neither completed nor waited on the lock of {locked:?}"
            );

            let blocked: bool = observer
                .query_one(
                    // The query detects a wait from `pg_blocking_pids` alone, because it lists
                    // a blocker only while a backend waits for a lock. The query does not filter
                    // on `wait_event`. If a later PostgreSQL renamed the event, the filter would
                    // match no backend, and the tests that require no wait would pass unchecked.
                    "select exists (select 1 from pg_stat_activity \
                     where $1 = any(pg_blocking_pids(pid)))",
                    &[&holder_pid],
                )
                .await?
                .get(0);

            if blocked {
                break true;
            }

            sleep(Duration::from_millis(10)).await;
        };

        holder.batch_execute("rollback").await?;

        let response = produce.await??;
        assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

        Ok(waited)
    }
}

async fn connect() -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(&postgres_url(), NoTls).await?;
    _ = tokio::spawn(connection);
    Ok(client)
}

#[tokio::test]
async fn produce_to_another_topic_runs_while_a_produce_holds_its_lock() -> Result<()> {
    let cluster = Cluster::new().await?;
    let locked = Topition::new(cluster.locked_topic.as_str(), 0);
    let produced = Topition::new(cluster.other_topic.as_str(), 0);

    assert!(
        !cluster.produce_waits_for_lock(&locked, &produced).await?,
        "a produce to {produced:?} waited on the produce lock of {locked:?}"
    );

    Ok(())
}

#[tokio::test]
async fn produce_to_another_partition_runs_while_a_produce_holds_its_lock() -> Result<()> {
    let cluster = Cluster::new().await?;
    let locked = Topition::new(cluster.locked_topic.as_str(), 0);
    let produced = Topition::new(cluster.locked_topic.as_str(), 1);

    assert!(
        !cluster.produce_waits_for_lock(&locked, &produced).await?,
        "a produce to {produced:?} waited on the produce lock of {locked:?}"
    );

    Ok(())
}

#[tokio::test]
async fn produce_to_the_same_partition_waits_for_the_lock() -> Result<()> {
    let cluster = Cluster::new().await?;
    let locked = Topition::new(cluster.locked_topic.as_str(), 0);

    assert!(
        cluster.produce_waits_for_lock(&locked, &locked).await?,
        "a produce to {locked:?} ran while another transaction held its lock"
    );

    Ok(())
}

/// The number of partitions of each topic of a [`Cluster`].
const PARTITIONS_PER_TOPIC: i32 = 4;

/// The number of tasks that produce at the same time.
const PRODUCER_TASKS: usize = 16;

/// The number of single-record batches that each task produces.
const BATCHES_PER_TASK: usize = 25;

/// Returns a batch of one record with `value`, from `producer_id` at `base_sequence`. A producer id
/// of -1 makes a batch from a producer that is not idempotent.
fn single_record_batch(
    value: String,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .producer_id(producer_id)
        .producer_epoch(producer_epoch)
        .base_sequence(base_sequence)
        .record(Record::builder().value(Bytes::from(value).into()))
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// Produces [`BATCHES_PER_TASK`] batches, the next one after the previous one is acknowledged, and
/// returns the offset of each batch's record by its value. Batch `n` goes to partition
/// `(task + n) % partitions.len()`, so each task writes to every partition, and each partition gets
/// writes from every task. An idempotent task keeps a sequence for each partition, as a Kafka
/// producer does.
async fn produce_from_task(
    storage: ArcDynStorage,
    task: usize,
    partitions: Vec<Topition>,
    is_idempotent: bool,
) -> Result<BTreeMap<String, (Topition, i64)>> {
    let (producer_id, producer_epoch) = if is_idempotent {
        let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?;
        assert_eq!(ErrorCode::None, producer.error);
        (producer.id, producer.epoch)
    } else {
        (-1, 0)
    };

    let mut sequences = vec![0; partitions.len()];
    let mut offsets = BTreeMap::new();

    for batch_index in 0..BATCHES_PER_TASK {
        let partition = (task + batch_index) % partitions.len();
        let value = format!("task {task} batch {batch_index}");

        let batch = single_record_batch(
            value.clone(),
            producer_id,
            producer_epoch,
            sequences[partition],
        )?;

        let offset = storage.produce(None, &partitions[partition], batch).await?;

        sequences[partition] += 1;
        _ = offsets.insert(value, (partitions[partition].clone(), offset));
    }

    Ok(offsets)
}

/// Returns the value of each record stored in `partition`, by its offset.
async fn stored_records(
    storage: &ArcDynStorage,
    partition: &Topition,
    high_watermark: i64,
) -> Result<BTreeMap<i64, String>> {
    let mut records = BTreeMap::new();
    let mut next_offset = 0;

    while next_offset < high_watermark {
        let batches = storage
            .fetch(
                partition,
                next_offset,
                1,
                1024 * 1024,
                IsolationLevel::ReadUncommitted,
                Duration::ZERO,
            )
            .await?;
        assert!(
            !batches.is_empty(),
            "{partition:?} has no batch at {next_offset}"
        );

        for batch in &batches {
            let batch = inflated::Batch::try_from(batch)?;

            for record in &batch.records {
                let offset = batch.base_offset + i64::from(record.offset_delta);
                let value = record.value().expect("each produced record has a value");
                let previous = records.insert(offset, String::from_utf8(value.to_vec())?);
                assert_eq!(None, previous, "{partition:?} stores offset {offset} twice");
                next_offset = offset + 1;
            }
        }
    }

    Ok(records)
}

/// Many produces run at the same time, to the same partitions and to different partitions, from
/// plain and idempotent producers. Each partition then holds the offsets from 0 up to its high
/// watermark, with no gap and no duplicate, and each acknowledged record is stored once, at the
/// offset that its produce returned.
#[tokio::test]
async fn concurrent_produces_keep_each_partition_contiguous() -> Result<()> {
    let cluster = Cluster::new().await?;

    let partitions = [&cluster.locked_topic, &cluster.other_topic]
        .into_iter()
        .flat_map(|topic| {
            (0..PARTITIONS_PER_TOPIC).map(move |partition| Topition::new(topic.as_str(), partition))
        })
        .collect::<Vec<_>>();

    let mut tasks = JoinSet::new();

    for task in 0..PRODUCER_TASKS {
        _ = tasks.spawn(produce_from_task(
            cluster.storage.clone(),
            task,
            partitions.clone(),
            task % 2 == 1,
        ));
    }

    let mut acknowledged = BTreeMap::new();

    while let Some(offsets) = tasks.join_next().await {
        acknowledged.extend(offsets??);
    }

    assert_eq!(PRODUCER_TASKS * BATCHES_PER_TASK, acknowledged.len());

    for partition in &partitions {
        let expected = acknowledged
            .iter()
            .filter(|(_, (acknowledged_partition, _))| acknowledged_partition == partition)
            .map(|(value, (_, offset))| (*offset, value.clone()))
            .collect::<BTreeMap<_, _>>();

        let high_watermark = cluster
            .storage
            .offset_stage(partition)
            .await?
            .high_watermark();
        assert_eq!(
            i64::try_from(expected.len())?,
            high_watermark,
            "{partition:?}"
        );

        assert_eq!(
            (0..high_watermark).collect::<Vec<_>>(),
            expected.keys().copied().collect::<Vec<_>>(),
            "{partition:?} acknowledged offsets that are not contiguous from 0"
        );

        assert_eq!(
            expected,
            stored_records(&cluster.storage, partition, high_watermark).await?,
            "{partition:?}"
        );
    }

    Ok(())
}
