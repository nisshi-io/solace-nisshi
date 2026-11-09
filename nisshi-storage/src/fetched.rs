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

//! What a [`Storage::fetch`] read hands to its caller while it reads.
//!
//! A storage engine sends what it reads through a [`FetchedSender`], as soon as it has it, instead
//! of returning it at the end. The caller then owns everything that the read sent, and keeps it
//! when the read's deadline drops the read.

use bytes::Bytes;
use nisshi_sans_io::{
    IsolationLevel, TimestampType,
    record::{Record, deflated, header, inflated},
};
use tokio::{sync::mpsc, time::Instant};

use crate::{
    Error, OffsetStage, Result, Storage, StorageOperation, Topition,
    read_deadline::{ReadDeadline, TimedOut},
};

/// How many items the channel of a [`Storage::fetch`] read holds.
///
/// A read sends without waiting while the channel has room, and its caller takes every item
/// waiting at once. So the caller handles up to this many items each time it gets control,
/// instead of one, which costs it less per item.
const CHANNEL_CAPACITY: usize = 64;

/// Returns the two ends of the channel of one [`Storage::fetch`] read.
///
/// The channel holds `CHANNEL_CAPACITY` items. A read that sends faster than its caller takes
/// waits when the channel is full, and the caller gets control then.
pub fn channel() -> (FetchedSender, FetchedReceiver) {
    let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
    (FetchedSender(sender), FetchedReceiver(receiver))
}

/// The sending end of a [`Storage::fetch`] read, before the read has sent its offsets.
///
/// The read first sends the offset stage that bounds it, through
/// [`FetchedSender::send_offsets`], and then its data through the [`FetchedDataSender`] that this
/// returns. So the caller has the offsets that match every record that it receives.
///
/// Each send waits until the channel has room for the item. A send fails with
/// [`Error::UnableToSend`] once the caller has stopped reading, and the read then ends.
#[derive(Debug)]
pub struct FetchedSender(mpsc::Sender<Fetched>);

impl FetchedSender {
    /// Returns a future that ends once the caller has stopped reading.
    pub(crate) fn reader_stopped(&self) -> impl Future<Output = ()> + Send + 'static {
        let sender = self.0.clone();
        async move { sender.closed().await }
    }

    /// Returns whether the caller has stopped reading.
    pub(crate) fn reader_has_stopped(&self) -> bool {
        self.0.is_closed()
    }

    /// Sends the offset stage that bounds the read, and returns the sender for its data.
    pub async fn send_offsets(self, offset_stage: OffsetStage) -> Result<FetchedDataSender> {
        send(&self.0, Fetched::Offsets(offset_stage)).await?;
        Ok(FetchedDataSender(self.0))
    }
}

/// The sending end of a [`Storage::fetch`] read, for the data that it reads after its offsets.
#[derive(Debug)]
pub struct FetchedDataSender(mpsc::Sender<Fetched>);

impl FetchedDataSender {
    /// Sends one record, which follows the record sent before it in offset order.
    pub async fn send_record(&mut self, record: FetchedRecord) -> Result<()> {
        send(&self.0, Fetched::Record(record)).await
    }

    /// Sends one whole stored batch, which follows the data sent before it in offset order.
    pub async fn send_batch(&mut self, batch: deflated::Batch) -> Result<()> {
        send(&self.0, Fetched::Batch(batch)).await
    }
}

/// One record that a row-based storage engine read, with the attributes of its batch.
#[derive(Clone, Debug, Default)]
pub struct FetchedRecord {
    pub offset: i64,
    /// The record's timestamp, in milliseconds since the epoch.
    pub timestamp: i64,
    /// The attributes of the batch that stored the record.
    pub attributes: i16,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<header::Builder>,
}

/// The receiving end of a [`Storage::fetch`] read.
#[derive(Debug)]
pub struct FetchedReceiver(mpsc::Receiver<Fetched>);

/// An item that a [`Storage::fetch`] read sends to its caller.
#[derive(Debug)]
pub(crate) enum Fetched {
    Offsets(OffsetStage),
    Record(FetchedRecord),
    Batch(deflated::Batch),
}

/// Sends `fetched` through `sender`, waiting only when the channel is full.
///
/// It tries the send without waiting first, because a waiting send takes a permit from the
/// channel's semaphore, which costs a little for every item, and most items find room.
async fn send(sender: &mpsc::Sender<Fetched>, fetched: Fetched) -> Result<()> {
    match sender.try_send(fetched) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(fetched)) => {
            sender.send(fetched).await.map_err(|_| Error::UnableToSend)
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(Error::UnableToSend),
    }
}

/// Runs a [`Storage::fetch`] read until its deadline, for any [`Storage`].
pub trait FetchUntilDeadline: Storage {
    /// Runs a [`Storage::fetch`] read of `topition` from `offset` until `deadline`, and returns
    /// what it sent.
    ///
    /// The deadline drops the read when it passes, while the read waits for I/O or for room in
    /// its channel. A read that never waits for I/O stops at the deadline too, because it waits
    /// for room once its channel is full. Everything that the read sent before the deadline,
    /// including the batch that it was building, stays in the returned [`FetchRead`].
    fn fetch_until_deadline(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        deadline: Instant,
    ) -> impl Future<Output = Result<FetchRead>> + Send;
}

/// What one [`Storage::fetch`] read handed to its caller.
#[derive(Debug, Default)]
pub struct FetchRead {
    /// The offset stage that bounded the read, if it sent it before it stopped.
    pub offsets: Option<OffsetStage>,
    /// The batches that the read sent, in offset order, with the batch that it was building
    /// when it stopped.
    pub batches: Vec<deflated::Batch>,
    /// Why the read stopped before its end, if its deadline stopped it.
    pub timed_out: Option<TimedOut>,
}

impl<G> FetchUntilDeadline for G
where
    G: Storage + ?Sized,
{
    fn fetch_until_deadline(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        deadline: Instant,
    ) -> impl Future<Output = Result<FetchRead>> + Send {
        read(
            self, topition, offset, min_bytes, max_bytes, isolation, deadline,
        )
    }
}

/// Runs a read as [`FetchUntilDeadline::fetch_until_deadline`] describes.
async fn read<G>(
    storage: &G,
    topition: &Topition,
    offset: i64,
    min_bytes: u32,
    max_bytes: u32,
    isolation: IsolationLevel,
    deadline: Instant,
) -> Result<FetchRead>
where
    G: Storage + ?Sized,
{
    let (sender, mut receiver) = channel();

    let mut read_deadline = ReadDeadline::new(StorageOperation::Fetch, deadline);
    let mut engine = storage.fetch(topition, offset, min_bytes, max_bytes, isolation, sender);
    let mut engine_finished = false;

    let mut fetched = FetchRead::default();
    let mut batches = FetchedBatches::default();
    let mut items = Vec::with_capacity(CHANNEL_CAPACITY);

    loop {
        let next = read_deadline
            .run_until_deadline(next_items(
                &mut engine,
                &mut engine_finished,
                &mut receiver.0,
                &mut items,
            ))
            .await;

        match next {
            Ok(Ok(0)) => break,
            Ok(Ok(_)) => {
                for item in items.drain(..) {
                    add_item(item, &mut fetched, &mut batches)?;
                }
            }
            Ok(Err(error)) => return Err(error),
            Err(timed_out) => {
                fetched.timed_out = Some(timed_out);

                // A read behind `RequestChannelService` runs on another task, so it can send items
                // while this loop handles the items before them, and the deadline then stops the
                // loop before it receives them. The channel outlives the read that the deadline
                // dropped, so those items are still in it.
                while let Ok(item) = receiver.0.try_recv() {
                    add_item(item, &mut fetched, &mut batches)?;
                }

                break;
            }
        }
    }

    fetched.batches = batches.into_batches()?;
    Ok(fetched)
}

/// Receives into `items` every item that `engine` has sent, waiting for at least one, and
/// returns how many it received, or 0 once `engine` has finished and sent everything.
///
/// It polls `engine` while it waits, because the engine runs only while it is polled. It polls
/// `engine` before `receiver`, so that the items that `engine` sends in a poll are received in the
/// same poll.
///
/// Returns the error of `engine` if it fails, also when it fails after it closed the channel.
async fn next_items<F>(
    engine: &mut F,
    engine_finished: &mut bool,
    receiver: &mut mpsc::Receiver<Fetched>,
    items: &mut Vec<Fetched>,
) -> Result<usize>
where
    F: Future<Output = Result<()>> + Unpin,
{
    loop {
        tokio::select! {
            biased;
            finished = &mut *engine, if !*engine_finished => {
                *engine_finished = true;
                finished?;
            }
            received = receiver.recv_many(items, CHANNEL_CAPACITY) => {
                // The channel can close before `engine` finishes: a read behind
                // `RequestChannelService` drops its sender on another task, and the error of that
                // read reaches `engine` only after the drop. The read waits for `engine` here
                // instead of ending, so that a failed read returns its error, not an empty answer.
                if received == 0 && !*engine_finished {
                    *engine_finished = true;
                    (&mut *engine).await?;
                }

                return Ok(received);
            }
        }
    }
}

/// Adds `item` to `fetched`, or to the batches in `batches`.
fn add_item(item: Fetched, fetched: &mut FetchRead, batches: &mut FetchedBatches) -> Result<()> {
    match item {
        Fetched::Offsets(offsets) => fetched.offsets = Some(offsets),
        Fetched::Record(record) => batches.push_record(record)?,
        Fetched::Batch(batch) => batches.push_batch(batch)?,
    }

    Ok(())
}

/// The batches that a caller builds from the items of one fetch, in offset order.
#[derive(Debug, Default)]
pub(crate) struct FetchedBatches {
    /// The batches that are finished.
    closed: Vec<deflated::Batch>,
    /// The batch that records are added to.
    open: Option<inflated::Builder>,
}

impl FetchedBatches {
    /// Adds `record` to the open batch, or to a new batch when its batch's attributes differ from
    /// those of the open batch.
    ///
    /// A `LogAppendTime` record whose timestamp differs from the batch's starts a new batch. A
    /// consumer reads one timestamp, `max_timestamp`, for every record of a `LogAppendTime`
    /// batch. Each stored record holds the broker's time of its own produce request, so one batch
    /// cannot hold two such times.
    pub(crate) fn push_record(&mut self, record: FetchedRecord) -> Result<()> {
        // An engine sends records in offset order, and a batch's deltas assume it.
        debug_assert!(
            self.next_offset().is_none_or(|next| record.offset >= next),
            "record {} follows offset {:?}",
            record.offset,
            self.next_offset()
        );

        let starts_batch = self.open.as_ref().is_none_or(|open| {
            open.attributes != record.attributes
                || open.producer_id != record.producer_id
                || open.producer_epoch != record.producer_epoch
                || (TimestampType::from(record.attributes) == TimestampType::LogAppendTime
                    && record.timestamp != open.base_timestamp)
        });

        if starts_batch {
            self.close()?;
        }

        let open = self.open.take().unwrap_or_else(|| {
            inflated::Batch::builder()
                .base_offset(record.offset)
                .base_timestamp(record.timestamp)
                .attributes(record.attributes)
                .producer_id(record.producer_id)
                .producer_epoch(record.producer_epoch)
        });

        let offset_delta = i32::try_from(record.offset - open.base_offset)?;

        let record_builder = record.headers.into_iter().fold(
            Record::builder()
                .offset_delta(offset_delta)
                .timestamp_delta(record.timestamp - open.base_timestamp)
                .key(record.key)
                .value(record.value),
            |record_builder, header| record_builder.header(header),
        );

        // The builder moves out of `open` and back, because `mem::take` would leave a default
        // builder in its place, and a default builder reads the system clock.
        self.open = Some(open.record(record_builder).last_offset_delta(offset_delta));

        Ok(())
    }

    /// Adds a whole stored batch, after the open batch.
    pub(crate) fn push_batch(&mut self, batch: deflated::Batch) -> Result<()> {
        self.close()?;
        self.closed.push(batch);
        Ok(())
    }

    /// Returns the batches, with the open batch last.
    pub(crate) fn into_batches(mut self) -> Result<Vec<deflated::Batch>> {
        self.close()?;
        Ok(self.closed)
    }

    /// Returns the offset after the last record or batch added.
    fn next_offset(&self) -> Option<i64> {
        self.open
            .as_ref()
            .map(|open| open.base_offset + i64::from(open.last_offset_delta) + 1)
            .or_else(|| self.closed.last().map(|batch| batch.max_offset() + 1))
    }

    /// Finishes the open batch, if there is one.
    fn close(&mut self) -> Result<()> {
        if let Some(open) = self.open.take() {
            self.closed.push(
                open.max_timestamp_of_records()
                    .build()
                    .and_then(TryInto::try_into)?,
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{assert_matches, future::pending};

    use nisshi_sans_io::ErrorCode;
    use tokio::time::Duration;

    use super::*;
    use crate::storage_double::storage_double;

    fn record(offset: i64, timestamp: i64, attributes: i16) -> FetchedRecord {
        FetchedRecord {
            offset,
            timestamp,
            attributes,
            producer_id: -1,
            producer_epoch: -1,
            value: Some(Bytes::from(format!("value-{offset}"))),
            ..FetchedRecord::default()
        }
    }

    fn record_offsets(batches: &[deflated::Batch]) -> Result<Vec<Vec<i64>>> {
        batches
            .iter()
            .map(|batch| {
                inflated::Batch::try_from(batch.clone())
                    .map_err(Error::from)
                    .map(|inflated| {
                        inflated
                            .records
                            .iter()
                            .map(|record| batch.base_offset + i64::from(record.offset_delta))
                            .collect()
                    })
            })
            .collect()
    }

    #[test]
    fn records_with_the_same_attributes_share_a_batch() -> Result<()> {
        let mut batches = FetchedBatches::default();

        for offset in 0..3 {
            batches.push_record(record(offset, 1_000 + offset, 0))?;
        }

        let batches = batches.into_batches()?;

        assert_eq!(vec![vec![0, 1, 2]], record_offsets(&batches)?);
        assert_eq!(2, batches[0].last_offset_delta);
        assert_eq!(1_002, batches[0].max_timestamp);

        Ok(())
    }

    #[test]
    fn a_record_with_other_attributes_starts_a_batch() -> Result<()> {
        let mut batches = FetchedBatches::default();

        batches.push_record(record(0, 1_000, 0))?;
        batches.push_record(record(1, 1_000, 0))?;
        // Attributes 1 compress the batch with gzip, so this record can't join the open batch.
        batches.push_record(record(2, 1_000, 1))?;

        let batches = batches.into_batches()?;

        assert_eq!(vec![vec![0, 1], vec![2]], record_offsets(&batches)?);
        assert_eq!(2, batches[1].base_offset);

        Ok(())
    }

    #[test]
    fn a_log_append_time_record_with_another_time_starts_a_batch() -> Result<()> {
        let log_append_time = 8;
        let mut batches = FetchedBatches::default();

        batches.push_record(record(0, 1_000, log_append_time))?;
        batches.push_record(record(1, 1_000, log_append_time))?;
        batches.push_record(record(2, 2_000, log_append_time))?;

        let batches = batches.into_batches()?;

        assert_eq!(vec![vec![0, 1], vec![2]], record_offsets(&batches)?);

        Ok(())
    }

    #[test]
    fn a_record_of_another_producer_starts_a_batch() -> Result<()> {
        let mut batches = FetchedBatches::default();

        for (offset, producer_id) in [(0, 7), (1, 7), (2, 8), (3, 7)] {
            batches.push_record(FetchedRecord {
                producer_id,
                producer_epoch: 0,
                ..record(offset, 1_000, 0)
            })?;
        }

        let batches = batches.into_batches()?;

        assert_eq!(
            vec![vec![0, 1], vec![2], vec![3]],
            record_offsets(&batches)?
        );
        assert_eq!(
            vec![7, 8, 7],
            batches
                .iter()
                .map(|batch| batch.producer_id)
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    #[test]
    fn a_record_of_another_producer_epoch_starts_a_batch() -> Result<()> {
        let mut batches = FetchedBatches::default();

        for (offset, producer_epoch) in [(0, 0), (1, 0), (2, 1)] {
            batches.push_record(FetchedRecord {
                producer_id: 7,
                producer_epoch,
                ..record(offset, 1_000, 0)
            })?;
        }

        let batches = batches.into_batches()?;

        assert_eq!(vec![vec![0, 1], vec![2]], record_offsets(&batches)?);
        assert_eq!(
            vec![0, 1],
            batches
                .iter()
                .map(|batch| batch.producer_epoch)
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    /// A record below the offset after the record before it breaks the order that the batch's
    /// offset deltas assume, so a debug build stops.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "record 1 follows offset Some(3)")]
    fn a_record_out_of_offset_order_fails_the_assertion() {
        let mut batches = FetchedBatches::default();

        batches.push_record(record(2, 1_000, 0)).expect("record 2");
        batches.push_record(record(1, 1_000, 0)).expect("record 1");
    }

    #[test]
    fn a_whole_batch_follows_the_open_batch() -> Result<()> {
        let stored = inflated::Batch::builder()
            .base_offset(2)
            .record(Record::builder().value(Some(Bytes::from_static(b"stored"))))
            .build()
            .and_then(deflated::Batch::try_from)?;

        let mut batches = FetchedBatches::default();
        batches.push_record(record(0, 1_000, 0))?;
        batches.push_record(record(1, 1_000, 0))?;
        batches.push_batch(stored)?;

        let batches = batches.into_batches()?;

        assert_eq!(vec![vec![0, 1], vec![2]], record_offsets(&batches)?);

        Ok(())
    }

    /// Sends the offsets of a partition of `records` records, and then each record, spending
    /// `record_time` on each without waiting. Returns the sender, so that a read that keeps it
    /// keeps its channel open.
    async fn send_records(
        fetched_sender: FetchedSender,
        records: i64,
        record_time: Duration,
    ) -> Result<FetchedDataSender> {
        let mut data = fetched_sender
            .send_offsets(OffsetStage {
                last_stable: records,
                high_watermark: records,
                log_start: 0,
            })
            .await?;

        for offset in 0..records {
            let read_until = std::time::Instant::now() + record_time;
            while std::time::Instant::now() < read_until {}

            data.send_record(record(offset, 1_000, 0)).await?;
        }

        Ok(data)
    }

    /// Runs one read of `storage` from offset 0 until `deadline`.
    async fn fetch_from_start(storage: &impl Storage, deadline: Instant) -> Result<FetchRead> {
        storage
            .fetch_until_deadline(
                &Topition::new("abc", 0),
                0,
                0,
                u32::MAX,
                IsolationLevel::ReadUncommitted,
                deadline,
            )
            .await
    }

    /// A storage double whose read sends `records` records, and then waits forever, holding its
    /// sender.
    #[derive(Debug)]
    struct SendsThenWaits {
        records: i64,
    }

    storage_double!(SendsThenWaits, async fn fetch(&self, fetched_sender) {
        let _data = send_records(fetched_sender, self.records, Duration::ZERO).await?;
        pending().await
    });

    /// A read that never returns answers at its deadline, with its offsets and every record that
    /// it sent, in the batch that it was building.
    #[tokio::test(start_paused = true)]
    async fn a_read_that_blocks_answers_at_its_deadline_with_what_it_sent() -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(1);

        let read = fetch_from_start(&SendsThenWaits { records: 3 }, deadline).await?;

        assert_eq!(deadline, Instant::now());
        assert_eq!(Some(TimedOut::Reading), read.timed_out);
        assert_eq!(Some(3), read.offsets.map(|offsets| offsets.high_watermark));
        assert_eq!(vec![vec![0, 1, 2]], record_offsets(&read.batches)?);

        Ok(())
    }

    /// A storage double whose read sends `records` records, spending `record_time` on each without
    /// waiting, and then returns.
    #[derive(Debug)]
    struct SendsWithoutWaiting {
        records: i64,
        record_time: Duration,
    }

    storage_double!(SendsWithoutWaiting, async fn fetch(&self, fetched_sender) {
        send_records(fetched_sender, self.records, self.record_time)
            .await
            .map(drop)
    });

    /// A read that never waits for I/O still stops at its deadline, because it waits for room once
    /// its channel is full, and the caller can drop it then.
    #[tokio::test]
    async fn a_read_that_never_waits_stops_at_its_deadline() -> Result<()> {
        let records = 1_000;

        let storage = SendsWithoutWaiting {
            records,
            record_time: Duration::from_millis(1),
        };

        let read = fetch_from_start(&storage, Instant::now() + Duration::from_millis(50)).await?;

        let received = record_offsets(&read.batches)?.concat().len();

        assert_eq!(Some(TimedOut::Reading), read.timed_out);
        assert!(
            received > 0,
            "the read kept none of the records that it sent"
        );
        assert!(
            received < usize::try_from(records)?,
            "the read ran past its deadline to its end"
        );

        Ok(())
    }

    /// A storage double whose read waits until `send_at`, sends one record, and then waits forever,
    /// holding its sender.
    #[derive(Debug)]
    struct SendsAt {
        send_at: Instant,
    }

    storage_double!(SendsAt, async fn fetch(&self, fetched_sender) {
        tokio::time::sleep_until(self.send_at).await;
        let _data = send_records(fetched_sender, 1, Duration::ZERO).await?;
        pending().await
    });

    /// The items that a read sends at the moment of its deadline, before the read waits, are kept.
    #[tokio::test(start_paused = true)]
    async fn the_items_sent_at_the_deadline_are_kept() -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(1);

        let read = fetch_from_start(&SendsAt { send_at: deadline }, deadline).await?;

        assert_eq!(
            Some(OffsetStage {
                last_stable: 1,
                high_watermark: 1,
                log_start: 0,
            }),
            read.offsets
        );
        assert_eq!(vec![vec![0]], record_offsets(&read.batches)?);
        assert_eq!(Some(TimedOut::Reading), read.timed_out);

        Ok(())
    }

    /// A storage double whose read sends `records` records, closes its channel, and then waits
    /// forever.
    #[derive(Debug)]
    struct ClosesThenWaits {
        records: i64,
    }

    storage_double!(ClosesThenWaits, async fn fetch(&self, fetched_sender) {
        drop(send_records(fetched_sender, self.records, Duration::ZERO).await?);
        pending().await
    });

    /// A read that closes its channel and then never returns answers at its deadline, with what
    /// it sent.
    #[tokio::test(start_paused = true)]
    async fn a_read_that_closes_its_channel_and_blocks_answers_at_its_deadline() -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(1);

        let read = fetch_from_start(&ClosesThenWaits { records: 2 }, deadline).await?;

        assert_eq!(deadline, Instant::now());
        assert_eq!(Some(TimedOut::Reading), read.timed_out);
        assert_eq!(vec![vec![0, 1]], record_offsets(&read.batches)?);

        Ok(())
    }

    /// A storage double whose read sends one record, closes its channel, and fails later.
    #[derive(Debug)]
    struct ClosesThenFails;

    storage_double!(ClosesThenFails, async fn fetch(&self, fetched_sender) {
        drop(send_records(fetched_sender, 1, Duration::ZERO).await?);

        // The caller sees the channel close before the read fails.
        tokio::time::sleep(Duration::from_millis(1)).await;

        Err(Error::Api(ErrorCode::UnknownTopicOrPartition))
    });

    /// A read that closes its channel and then fails returns its error, not an empty answer.
    #[tokio::test(start_paused = true)]
    async fn a_read_that_closes_its_channel_and_then_fails_returns_its_error() {
        let read =
            fetch_from_start(&ClosesThenFails, Instant::now() + Duration::from_secs(1)).await;

        assert_matches!(read, Err(Error::Api(ErrorCode::UnknownTopicOrPartition)));
    }
}
