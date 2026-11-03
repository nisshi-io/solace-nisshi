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

//! On PostgreSQL, two idempotent batches from one producer to one partition race only for the
//! partition's lock. Kafka checks a batch's producer epoch and sequence under the partition's log
//! lock, so a batch is checked against every batch stored before it. A retry of a batch that is
//! still in flight is then stored once, and a batch at an older epoch is never stored after a
//! batch at the producer's newer epoch.
//!
//! Each test holds the batch that comes first at its producer's `producer_detail` row, from a
//! second connection. A produce that checks the batch before it takes the partition's lock waits
//! there without the lock, so the second batch can pass its check, or be stored, before the first
//! one is. The tests read every wait from `pg_blocking_pids`, not from a timer.

use std::{assert_matches, collections::BTreeMap, time::Duration};

use crate::common::{alphanumeric_string, postgres_storage, postgres_url};
use anyhow::Result;
use bytes::Bytes;
use nisshi_sans_io::{
    ErrorCode, IsolationLevel, ListOffset,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{ArcDynStorage, Error, Storage as _, Topition};
use rand::{RngExt as _, rng};
use tokio::{
    task::JoinHandle,
    time::{Instant, sleep},
};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

/// The time after which the test treats a running produce as stuck, and fails.
const STUCK_PRODUCE_LIMIT: Duration = Duration::from_secs(60);

/// The value of the record that each test stores first, at epoch 0 and sequence 0.
const FIRST: &str = "first";

/// A produce running in a task of its own.
type Produce = JoinHandle<std::result::Result<i64, Error>>;

/// An idempotent producer at epoch 0, and the single partition that it writes to.
struct Producer {
    storage: ArcDynStorage,
    topition: Topition,
    id: i64,
}

impl Producer {
    /// Creates a topic of one partition and a producer, and stores [`FIRST`] at epoch 0 and
    /// sequence 0, so that the producer has a `producer_detail` row for the partition.
    async fn new() -> Result<Self> {
        let storage =
            postgres_storage(Uuid::now_v7().to_string(), rng().random_range(0..i32::MAX)).await?;

        let topic = alphanumeric_string(15);

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic.clone())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let producer = storage.init_producer(None, 0, Some(-1), Some(-1)).await?;
        assert_eq!(ErrorCode::None, producer.error, "{producer:?}");
        assert_eq!(0, producer.epoch, "{producer:?}");

        let this = Self {
            storage,
            topition: Topition::new(topic, 0),
            id: producer.id,
        };

        assert_eq!(0, this.produce(0, 0, FIRST).await??);

        Ok(this)
    }

    /// Starts a produce of one record with `value`, at `epoch` and `base_sequence`.
    fn produce(&self, epoch: i16, base_sequence: i32, value: &str) -> Produce {
        let storage = self.storage.clone();
        let topition = self.topition.clone();
        let batch = inflated::Batch::builder()
            .producer_id(self.id)
            .producer_epoch(epoch)
            .base_sequence(base_sequence)
            .record(Record::builder().value(Bytes::from(value.to_owned()).into()))
            .build()
            .and_then(deflated::Batch::try_from);

        tokio::spawn(async move { storage.produce(None, &topition, batch?).await })
    }

    /// Returns the value of each record stored in the partition, by its offset.
    async fn stored_records(&self) -> Result<BTreeMap<i64, String>> {
        let offsets = self
            .storage
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[(self.topition.clone(), ListOffset::Latest)],
            )
            .await?;
        assert_eq!(1, offsets.len());
        assert_eq!(ErrorCode::None, offsets[0].1.error_code);
        let high_watermark = offsets[0].1.offset.unwrap_or_default();

        let mut records = BTreeMap::new();
        let mut next_offset = 0;

        while next_offset < high_watermark {
            let batches = self
                .storage
                .fetch(
                    &self.topition,
                    next_offset,
                    1,
                    1024 * 1024,
                    IsolationLevel::ReadUncommitted,
                    Duration::ZERO,
                )
                .await?;
            assert!(!batches.is_empty(), "no batch at {next_offset}");

            for batch in &batches {
                let batch = inflated::Batch::try_from(batch)?;

                for record in &batch.records {
                    let offset = batch.base_offset + i64::from(record.offset_delta);
                    let value = record.value().expect("each produced record has a value");
                    let previous = records.insert(offset, String::from_utf8(value.to_vec())?);
                    assert_eq!(None, previous, "offset {offset} is stored twice");
                    next_offset = offset + 1;
                }
            }
        }

        Ok(records)
    }
}

/// A transaction on a second connection that holds the producer's `producer_detail` row, and an
/// observer connection that reads which produces wait for it.
struct Holder {
    holder: Client,
    observer: Client,
    pid: i32,
}

impl Holder {
    /// Locks the `producer_detail` row of `producer` at `epoch`. The producer writes to one
    /// partition only, so the producer and epoch select one row.
    async fn lock(producer: &Producer, epoch: i16) -> Result<Self> {
        let holder = connect().await?;
        let observer = connect().await?;

        holder.batch_execute("begin").await?;

        let pid: i32 = holder
            .query_one("select pg_backend_pid()", &[])
            .await?
            .get(0);

        let locked = holder
            .query(
                "select pd.id \
                 from producer_detail pd \
                 join producer_epoch pe on pe.id = pd.producer_epoch \
                 where pe.producer = $1 and pe.epoch = $2 \
                 for update of pd",
                &[&producer.id, &epoch],
            )
            .await?;
        assert_eq!(1, locked.len());

        Ok(Self {
            holder,
            observer,
            pid,
        })
    }

    /// Returns the number of backends that wait for the holder, directly or through a chain of
    /// waits. A produce that waits for a lock that another waiting produce holds counts too.
    async fn waiting(&self) -> Result<i64> {
        self.observer
            .query_one(
                // `pg_blocking_pids` lists a blocker only while a backend waits for a lock, so the
                // query does not filter on `wait_event`.
                "with recursive waiting (pid) as ( \
                     select pid from pg_stat_activity \
                     where $1 = any(pg_blocking_pids(pid)) \
                     union \
                     select a.pid from pg_stat_activity a \
                     join waiting w on w.pid = any(pg_blocking_pids(a.pid)) \
                 ) \
                 select count(*) from waiting",
                &[&self.pid],
            )
            .await
            .map(|row| row.get(0))
            .map_err(Into::into)
    }

    /// Waits until `produce` has finished, or until `waiting` backends wait for the holder.
    /// Returns whether `produce` has finished.
    async fn until_finished_or_waiting(&self, produce: &Produce, waiting: i64) -> Result<bool> {
        let started = Instant::now();

        loop {
            if produce.is_finished() {
                return Ok(true);
            }

            if self.waiting().await? >= waiting {
                return Ok(false);
            }

            assert!(
                started.elapsed() < STUCK_PRODUCE_LIMIT,
                "a produce neither finished nor waited for the holder"
            );

            sleep(Duration::from_millis(10)).await;
        }
    }

    /// Ends the holder's transaction, which releases the row.
    async fn release(self) -> Result<()> {
        self.holder
            .batch_execute("rollback")
            .await
            .map_err(Into::into)
    }
}

async fn connect() -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(&postgres_url(), NoTls).await?;
    _ = tokio::spawn(connection);
    Ok(client)
}

/// A client resends a batch whose produce is still in flight, after the request times out. The
/// batch is stored once. One produce is answered with its offset, and the other
/// with DUPLICATE_SEQUENCE_NUMBER, which a client counts as delivered.
#[tokio::test]
async fn retry_of_a_batch_in_flight_is_stored_once() -> Result<()> {
    let producer = Producer::new().await?;
    let holder = Holder::lock(&producer, 0).await?;

    let original = producer.produce(0, 1, "second");
    assert!(
        !holder.until_finished_or_waiting(&original, 1).await?,
        "the original produce finished while the holder held its producer's row"
    );

    let retry = producer.produce(0, 1, "second");
    assert!(
        !holder.until_finished_or_waiting(&retry, 2).await?,
        "the retry finished while the original produce waited"
    );

    holder.release().await?;

    let mut answers = [original.await?, retry.await?];
    answers.sort_by_key(Result::is_err);

    assert_eq!(
        1,
        *answers[0].as_ref().expect("one produce stores the batch")
    );
    assert_matches!(
        answers[1],
        Err(Error::Api(ErrorCode::DuplicateSequenceNumber)),
        "the other produce is answered as a duplicate"
    );

    assert_eq!(
        BTreeMap::from([(0, FIRST.to_owned()), (1, "second".to_owned())]),
        producer.stored_records().await?
    );

    Ok(())
}

/// A client gives up on a batch at epoch 0, bumps its own epoch, and sends its records again at
/// epoch 1 from sequence 0 (KIP-360). The produce at epoch 0 is still in flight. Each batch that
/// the broker acknowledges is stored once, at the offset that its produce returned, and the batch
/// at epoch 0 is not stored after the batch at epoch 1.
#[tokio::test]
async fn older_epoch_is_not_stored_after_the_bump() -> Result<()> {
    const OLDER: &str = "epoch 0";
    const NEWER: &str = "epoch 1";

    let producer = Producer::new().await?;
    let holder = Holder::lock(&producer, 0).await?;

    let older = producer.produce(0, 1, OLDER);
    assert!(
        !holder.until_finished_or_waiting(&older, 1).await?,
        "the produce at epoch 0 finished while the holder held its producer's row"
    );

    // The bump either completes now, or waits for the produce at epoch 0.
    let newer = producer.produce(1, 0, NEWER);
    _ = holder.until_finished_or_waiting(&newer, 2).await?;

    holder.release().await?;

    let older = older.await?;
    let newer = newer.await?;
    let stored = producer.stored_records().await?;

    let mut offsets = BTreeMap::new();

    for (value, answer) in [(OLDER, &older), (NEWER, &newer)] {
        let at: Vec<i64> = stored
            .iter()
            .filter(|(_, stored)| stored.as_str() == value)
            .map(|(offset, _)| *offset)
            .collect();

        match answer {
            Ok(offset) => {
                assert_eq!(vec![*offset], at, "{value} is stored once, at {offset}");
                _ = offsets.insert(value, *offset);
            }

            Err(error) => assert!(at.is_empty(), "{value} was refused ({error:?}) and stored"),
        }
    }

    assert!(newer.is_ok(), "the bump is stored: {newer:?}");

    if let (Some(older), Some(newer)) = (offsets.get(OLDER), offsets.get(NEWER)) {
        assert!(
            older < newer,
            "the batch at epoch 0 is stored at {older}, after the batch at epoch 1 at {newer}"
        );
    }

    Ok(())
}
