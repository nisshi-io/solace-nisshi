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

//! What a [`Storage::fetch`] read hands to its caller while it reads.
//!
//! A storage engine sends what it reads through a [`FetchedSender`], as soon as it has it, instead
//! of returning it at the end. The caller then owns everything that the read sent, and keeps it
//! when the read's deadline drops the read.

use bytes::Bytes;
use nisshi_sans_io::{
    ByteSize as _, IsolationLevel, TimestampType,
    primitive::varint::VarInt,
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

/// How a [`Storage::fetch`] read ended.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReadEnd {
    /// The read ended before its [`ByteLimit`] stopped it: at the high watermark, at the last
    /// stable offset for `ReadCommitted`, or at the engine's own cap on one read.
    #[default]
    Finished,
    /// The read stopped before a record or batch that does not fit its [`ByteLimit`], so the
    /// partition holds more records after what the read sent.
    ReachedLimit,
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

/// Runs [`Storage::fetch`] reads until their deadline, for any [`Storage`].
pub trait FetchUntilDeadline: Storage {
    /// Runs one [`Storage::fetch`] read of `topition` from `offset` until `deadline`, and returns
    /// what it sent.
    ///
    /// The deadline drops the read when it passes, while the read waits for I/O or for room in
    /// its channel. A read that never waits for I/O stops at the deadline too, because it waits
    /// for room once its channel is full. Everything that the read sent before the deadline,
    /// including the batch that it was building, stays in the returned [`FetchRead`].
    ///
    /// The read returns its first record or batch even when it is larger than `max_bytes`.
    ///
    /// A read that fails returns its error, also after it has sent records.
    fn fetch_until_deadline(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        deadline: Instant,
    ) -> impl Future<Output = Result<FetchRead>> + Send;

    /// Reads `topition` from `offset` until the records reach `limit`, the partition has no more
    /// records, or `deadline` passes, and returns what the reads sent.
    ///
    /// One [`Storage::fetch`] read can stop before the limit, for example when an engine caps the
    /// rows of one read. So each time a read ends, this starts another at the offset after the last
    /// record it received.
    ///
    /// If a read fails after the reads have sent a record, this returns those records, with the
    /// error in [`FetchRead::failed`]. If a read fails before that, this returns the error.
    #[allow(clippy::too_many_arguments)]
    fn fetch_until_limit(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        limit: ByteLimit,
        isolation: IsolationLevel,
        deadline: Instant,
    ) -> impl Future<Output = Result<FetchRead>> + Send;
}

/// A limit on the encoded size of the batches that a fetch returns.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ByteLimit {
    /// The batches hold at most this many bytes, except that the first record or batch is returned
    /// whatever its size, so that a consumer gets past a record larger than its limit (KIP-74).
    Soft(u32),
    /// The batches hold at most this many bytes.
    Hard(u32),
}

impl ByteLimit {
    /// Returns the limit's bytes.
    pub fn bytes(self) -> u32 {
        match self {
            Self::Soft(bytes) | Self::Hard(bytes) => bytes,
        }
    }

    /// Returns whether the limit is soft: the first record or batch of a fetch may exceed it.
    pub fn is_soft(self) -> bool {
        matches!(self, Self::Soft(_))
    }
}

/// What [`Storage::fetch`] reads handed to their caller.
#[derive(Debug, Default)]
pub struct FetchRead {
    /// The latest offset stage that the reads sent, if they sent one before they stopped.
    pub offsets: Option<OffsetStage>,
    /// The batches that the reads sent, in offset order, with the batch that they were building
    /// when they stopped. Their encoded size is at most the reads' limit, except that the first
    /// record or batch can exceed a [`ByteLimit::Soft`] limit.
    pub batches: Vec<deflated::Batch>,
    /// Whether the reads stopped before a record or batch that did not fit their limit. If true,
    /// the partition holds more records after `batches`.
    pub reached_limit: bool,
    /// Why the reads stopped before their end, if the deadline stopped them.
    pub timed_out: Option<TimedOut>,
    /// The error that ended the reads, if a read failed after they had sent a record. Only
    /// [`FetchUntilDeadline::fetch_until_limit`] sets it.
    pub failed: Option<Error>,
}

impl<G> FetchUntilDeadline for G
where
    G: Storage + ?Sized,
{
    async fn fetch_until_deadline(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        deadline: Instant,
    ) -> Result<FetchRead> {
        let mut progress = ReadProgress::within(ByteLimit::Soft(max_bytes));

        progress
            .read(self, topition, offset, min_bytes, isolation, deadline)
            .await?;

        let read = progress.finish()?;

        match read.failed {
            Some(error) => Err(error),
            None => Ok(read),
        }
    }

    async fn fetch_until_limit(
        &self,
        topition: &Topition,
        mut offset: i64,
        min_bytes: u32,
        limit: ByteLimit,
        isolation: IsolationLevel,
        deadline: Instant,
    ) -> Result<FetchRead> {
        let mut progress = ReadProgress::within(limit);

        while !progress.batches.is_full() {
            progress
                .read(self, topition, offset, min_bytes, isolation, deadline)
                .await?;

            // A read started after the deadline returns no records and counts as timed out, so the
            // loop stops at the deadline.
            if progress.has_stopped() || Instant::now() >= deadline {
                break;
            }

            // The next read starts after the last record received, because compaction can leave
            // gaps in a batch's offsets. If the read added no records, it reached the end of the
            // partition.
            match progress.batches.next_offset() {
                Some(next) if next > offset => offset = next,
                _ => break,
            }
        }

        progress.finish()
    }
}

/// What the [`Storage::fetch`] reads of one caller have sent so far, within the caller's limit.
#[derive(Debug)]
struct ReadProgress {
    fetched: FetchRead,
    batches: FetchedBatches,
}

impl ReadProgress {
    /// Returns empty progress whose batches stay within `limit`.
    fn within(limit: ByteLimit) -> Self {
        Self {
            fetched: FetchRead::default(),
            batches: FetchedBatches::within(limit),
        }
    }

    /// Returns whether a read stopped at the limit, at the deadline or at a failure. The caller
    /// then stops reading, because another read would stop at the same limit or deadline, or add
    /// load to storage that is failing.
    fn has_stopped(&self) -> bool {
        self.fetched.reached_limit
            || self.fetched.timed_out.is_some()
            || self.fetched.failed.is_some()
    }

    /// Runs one [`Storage::fetch`] read from `offset` until `deadline`, and adds what it sends.
    ///
    /// This applies the limit itself, by encoded size, instead of trusting the engine's count,
    /// because an engine can count a record as fewer bytes than its encoding.
    ///
    /// After the limit is reached, this keeps polling the engine, and discards what it sends, until
    /// the engine ends or the deadline passes. Dropping the engine instead can leave its query
    /// running on the store until the store cancels it. The engine ends soon anyway, because it
    /// stops at its own count of the limit.
    ///
    /// If the read fails before the progress holds a record, this returns the error.
    async fn read<G>(
        &mut self,
        storage: &G,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        isolation: IsolationLevel,
        deadline: Instant,
    ) -> Result<()>
    where
        G: Storage + ?Sized,
    {
        let (sender, mut receiver) = channel();

        let mut read_deadline = ReadDeadline::new(StorageOperation::Fetch, deadline);
        let mut engine = storage.fetch(
            topition,
            offset,
            min_bytes,
            self.batches.limit(),
            isolation,
            sender,
        );
        // The `ReadEnd` that `engine` returned, or `None` while `engine` still runs.
        let mut engine_end = None;

        let mut items = Vec::with_capacity(CHANNEL_CAPACITY);

        loop {
            let next = read_deadline
                .run_until_deadline(next_items(
                    &mut engine,
                    &mut engine_end,
                    &mut receiver.0,
                    &mut items,
                ))
                .await;

            match next {
                Ok(Ok(0)) => {
                    self.fetched.reached_limit |= engine_end == Some(ReadEnd::ReachedLimit);
                    break;
                }
                Ok(Ok(_)) => {
                    for item in items.drain(..) {
                        self.add(item)?;
                    }
                }
                Ok(Err(error)) => {
                    // Keep the records that the engine sent before it failed, so that the caller's
                    // next read starts after them. If this returned only the error, a read that
                    // fails at the same point every time would never get past it.
                    while let Ok(item) = receiver.0.try_recv() {
                        self.add(item)?;
                    }

                    if !self.batches.has_records() {
                        return Err(error);
                    }

                    self.fetched.failed = Some(error);
                    break;
                }
                Err(timed_out) => {
                    self.fetched.timed_out = Some(timed_out);

                    // A read behind `RequestChannelService` runs on another task, so it can send
                    // items after this loop last received and before the deadline stopped it.
                    // Those items are still in the channel, because the channel outlives the read.
                    while let Ok(item) = receiver.0.try_recv() {
                        self.add(item)?;
                    }

                    break;
                }
            }
        }

        Ok(())
    }

    /// Adds `item` to the progress: an offset stage updates the latest stage, and a record or batch
    /// joins the batches if it fits their limit.
    ///
    /// The first record or batch that does not fit sets `reached_limit`. After that, this rejects
    /// every record or batch, smaller ones too, because the batches must hold contiguous offsets.
    fn add(&mut self, item: Fetched) -> Result<()> {
        let added = match item {
            Fetched::Offsets(offsets) => {
                self.fetched.offsets = Some(
                    self.fetched
                        .offsets
                        .map_or(offsets, |latest| latest.latest(offsets)),
                );
                true
            }
            Fetched::Record(_) | Fetched::Batch(_) if self.fetched.reached_limit => false,
            Fetched::Record(record) => self.batches.push_record(record)?,
            Fetched::Batch(batch) => self.batches.push_batch(batch)?,
        };

        self.fetched.reached_limit |= !added;

        Ok(())
    }

    /// Returns what the reads sent, with the batch that they were building last.
    fn finish(self) -> Result<FetchRead> {
        Ok(FetchRead {
            batches: self.batches.into_batches()?,
            ..self.fetched
        })
    }
}

/// Receives into `items` every item that `engine` has sent, waiting for at least one, and
/// returns how many it received, or 0 once `engine` has finished and sent everything. When `engine`
/// finishes, this stores its [`ReadEnd`] in `engine_end`.
///
/// It polls `engine` while it waits, because the engine runs only while it is polled. It polls
/// `engine` before `receiver`, so that the items that `engine` sends in a poll are received in the
/// same poll.
///
/// Returns the error of `engine` if it fails, also when it fails after it closed the channel.
async fn next_items<F>(
    engine: &mut F,
    engine_end: &mut Option<ReadEnd>,
    receiver: &mut mpsc::Receiver<Fetched>,
    items: &mut Vec<Fetched>,
) -> Result<usize>
where
    F: Future<Output = Result<ReadEnd>> + Unpin,
{
    loop {
        tokio::select! {
            biased;
            end = &mut *engine, if engine_end.is_none() => {
                *engine_end = Some(end?);
            }
            received = receiver.recv_many(items, CHANNEL_CAPACITY) => {
                // The channel can close before `engine` finishes: a read behind
                // `RequestChannelService` drops its sender on another task, and the error of that
                // read reaches `engine` only after the drop. The read waits for `engine` here
                // instead of ending, so that a failed read returns its error, not an empty answer.
                if received == 0 && engine_end.is_none() {
                    *engine_end = Some((&mut *engine).await?);
                }

                return Ok(received);
            }
        }
    }
}

/// The batches that a caller builds from the items of one fetch, in offset order, within a limit on
/// their encoded size.
#[derive(Debug)]
pub(crate) struct FetchedBatches {
    /// The batches that are finished.
    closed: Vec<deflated::Batch>,
    /// The batch that records are added to.
    open: Option<inflated::Builder>,
    /// The bytes that the batches can still take.
    bytes_left: u64,
    /// Whether the first record or batch may be larger than the limit.
    first_may_exceed: bool,
}

impl Default for FetchedBatches {
    /// Returns batches that do not limit their size.
    fn default() -> Self {
        Self {
            closed: Vec::new(),
            open: None,
            bytes_left: u64::MAX,
            first_may_exceed: true,
        }
    }
}

impl FetchedBatches {
    /// Returns batches whose encoded size, headers included, stays within `limit`, as Kafka
    /// measures the batches of a log segment. The first record or batch can exceed a
    /// [`ByteLimit::Soft`] limit.
    ///
    /// A record of a compressed batch counts as its size before compression. If the records don't
    /// compress, the codec's framing can make the batch a few tens of bytes larger than that, and
    /// so larger than the limit.
    pub(crate) fn within(limit: ByteLimit) -> Self {
        Self {
            bytes_left: u64::from(limit.bytes()),
            first_may_exceed: matches!(limit, ByteLimit::Soft(_)),
            ..Self::default()
        }
    }

    /// Adds `record` to the open batch, or to a new batch when its batch's attributes differ from
    /// those of the open batch, and returns whether it fits the limit. A record that does not fit
    /// is left out.
    ///
    /// A `LogAppendTime` record whose timestamp differs from the batch's starts a new batch. A
    /// consumer reads one timestamp, `max_timestamp`, for every record of a `LogAppendTime`
    /// batch. Each stored record holds the broker's time of its own produce request, so one batch
    /// cannot hold two such times.
    pub(crate) fn push_record(&mut self, record: FetchedRecord) -> Result<bool> {
        // An engine sends records in offset order, and a batch's deltas assume it.
        debug_assert!(
            self.next_offset().is_none_or(|next| record.offset >= next),
            "record {} follows offset {:?}",
            record.offset,
            self.next_offset()
        );

        let open = self.open.as_ref().filter(|open| {
            open.attributes == record.attributes
                && open.producer_id == record.producer_id
                && open.producer_epoch == record.producer_epoch
                && (TimestampType::from(record.attributes) != TimestampType::LogAppendTime
                    || record.timestamp == open.base_timestamp)
        });

        let (base_offset, base_timestamp) = open
            .map_or((record.offset, record.timestamp), |open| {
                (open.base_offset, open.base_timestamp)
            });
        let starts_batch = open.is_none();

        let offset_delta = i32::try_from(record.offset - base_offset)?;

        let record_builder = record.headers.into_iter().fold(
            Record::builder()
                .offset_delta(offset_delta)
                .timestamp_delta(record.timestamp - base_timestamp)
                .key(record.key)
                .value(record.value),
            |record_builder, header| record_builder.header(header),
        );

        // A record's encoded size in its batch is its body and the varint of the body's length. A
        // record that starts a batch also takes the batch's header.
        let header = if starts_batch {
            deflated::BATCH_OVERHEAD
        } else {
            0
        };

        if !self.take(header + VarInt::size_inclusive(&record_builder)?) {
            return Ok(false);
        }

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

        // The builder moves out of `open` and back, because `mem::take` would leave a default
        // builder in its place, and a default builder reads the system clock.
        self.open = Some(open.record(record_builder).last_offset_delta(offset_delta));

        Ok(true)
    }

    /// Adds a whole stored batch, after the open batch, and returns whether it fits the limit. A
    /// batch that does not fit is left out.
    pub(crate) fn push_batch(&mut self, batch: deflated::Batch) -> Result<bool> {
        if !self.take(batch.size_in_bytes()?) {
            return Ok(false);
        }

        self.close()?;
        self.closed.push(batch);
        Ok(true)
    }

    /// Takes `size` bytes from the limit, and returns whether they fit. The first record or batch
    /// fits whatever its size when `first_may_exceed` is set.
    fn take(&mut self, size: usize) -> bool {
        let size = size as u64;
        let fits = size <= self.bytes_left || (self.first_may_exceed && !self.has_records());

        if fits {
            self.bytes_left = self.bytes_left.saturating_sub(size);
        }

        fits
    }

    /// Returns whether the batches are full: the limit is spent, and no soft limit still lets a
    /// first record or batch exceed it.
    fn is_full(&self) -> bool {
        self.bytes_left == 0 && !(self.first_may_exceed && !self.has_records())
    }

    /// Returns the limit of the next read: the bytes that the batches can still take, at most
    /// `u32::MAX`. The limit is soft while a first record or batch may still exceed it, and hard
    /// otherwise.
    fn limit(&self) -> ByteLimit {
        let bytes = u32::try_from(self.bytes_left).unwrap_or(u32::MAX);

        if self.first_may_exceed && !self.has_records() {
            ByteLimit::Soft(bytes)
        } else {
            ByteLimit::Hard(bytes)
        }
    }

    /// Returns whether a record or a whole batch has been added.
    pub(crate) fn has_records(&self) -> bool {
        self.open.is_some() || !self.closed.is_empty()
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
    use std::{
        assert_matches,
        future::pending,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

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
            assert!(batches.push_record(record(offset, 1_000 + offset, 0))?);
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

        assert!(batches.push_record(record(0, 1_000, 0))?);
        assert!(batches.push_record(record(1, 1_000, 0))?);
        // Attributes 1 compress the batch with gzip, so this record can't join the open batch.
        assert!(batches.push_record(record(2, 1_000, 1))?);

        let batches = batches.into_batches()?;

        assert_eq!(vec![vec![0, 1], vec![2]], record_offsets(&batches)?);
        assert_eq!(2, batches[1].base_offset);

        Ok(())
    }

    #[test]
    fn a_log_append_time_record_with_another_time_starts_a_batch() -> Result<()> {
        let log_append_time = 8;
        let mut batches = FetchedBatches::default();

        assert!(batches.push_record(record(0, 1_000, log_append_time))?);
        assert!(batches.push_record(record(1, 1_000, log_append_time))?);
        assert!(batches.push_record(record(2, 2_000, log_append_time))?);

        let batches = batches.into_batches()?;

        assert_eq!(vec![vec![0, 1], vec![2]], record_offsets(&batches)?);

        Ok(())
    }

    #[test]
    fn a_record_of_another_producer_starts_a_batch() -> Result<()> {
        let mut batches = FetchedBatches::default();

        for (offset, producer_id) in [(0, 7), (1, 7), (2, 8), (3, 7)] {
            assert!(batches.push_record(FetchedRecord {
                producer_id,
                producer_epoch: 0,
                ..record(offset, 1_000, 0)
            })?);
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
            assert!(batches.push_record(FetchedRecord {
                producer_id: 7,
                producer_epoch,
                ..record(offset, 1_000, 0)
            })?);
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

        assert!(batches.push_record(record(2, 1_000, 0)).expect("record 2"));
        _ = batches.push_record(record(1, 1_000, 0)).expect("record 1");
    }

    #[test]
    fn a_whole_batch_follows_the_open_batch() -> Result<()> {
        let stored = inflated::Batch::builder()
            .base_offset(2)
            .record(Record::builder().value(Some(Bytes::from_static(b"stored"))))
            .build()
            .and_then(deflated::Batch::try_from)?;

        let mut batches = FetchedBatches::default();
        assert!(batches.push_record(record(0, 1_000, 0))?);
        assert!(batches.push_record(record(1, 1_000, 0))?);
        assert!(batches.push_batch(stored)?);

        let batches = batches.into_batches()?;

        assert_eq!(vec![vec![0, 1], vec![2]], record_offsets(&batches)?);

        Ok(())
    }

    /// The value length of [`large_record`]. A SQL engine counts that record as 1,007 bytes, a few
    /// bytes less than its encoding.
    const LARGE_VALUE: usize = 1_000;

    fn large_record(offset: i64) -> FetchedRecord {
        FetchedRecord {
            value: Some(Bytes::from(vec![b'x'; LARGE_VALUE])),
            ..record(offset, 1_000, 0)
        }
    }

    /// Returns a stored batch at `base_offset`, with one record of `value_len` bytes of value.
    fn stored_batch(base_offset: i64, value_len: usize) -> Result<deflated::Batch> {
        inflated::Batch::builder()
            .base_offset(base_offset)
            .record(Record::builder().value(Some(Bytes::from(vec![b'x'; value_len]))))
            .build()
            .and_then(deflated::Batch::try_from)
            .map_err(Into::into)
    }

    /// 1,000-byte records are added under a 64 KiB hard limit until one does not fit. The batch
    /// ends at most one record short of the limit, and holds one record fewer than a SQL engine's
    /// count of 1,007 bytes per record would let in.
    #[test]
    fn records_fill_the_limit_by_their_encoded_size() -> Result<()> {
        let limit = 64 * 1024;
        let mut batches = FetchedBatches::within(ByteLimit::Hard(limit));

        let mut added = 0;
        while batches.push_record(large_record(added))? {
            added += 1;
        }

        let size = batches.into_batches()?[0].size_in_bytes()?;
        let limit = usize::try_from(limit)?;
        let added = usize::try_from(added)?;

        assert!(size <= limit, "{size} bytes");
        // The record left out would have taken the batch past the limit, so the batch is full.
        assert!(size + LARGE_VALUE > limit, "{size} bytes");
        // A SQL engine counts each record as 1,007 bytes, so the engine fits one more record in the
        // same limit.
        assert!((added + 1) * (LARGE_VALUE + 7) < limit, "{added} records");

        Ok(())
    }

    /// A limit equal to a batch's encoded size, header included, takes the batch, and a limit one
    /// byte smaller does not. The same holds for a record that starts a new batch.
    #[test]
    fn a_batch_counts_its_header() -> Result<()> {
        let stored = stored_batch(0, 10)?;
        let size = u32::try_from(stored.size_in_bytes()?)?;

        assert!(FetchedBatches::within(ByteLimit::Hard(size)).push_batch(stored.clone())?);
        assert!(!FetchedBatches::within(ByteLimit::Hard(size - 1)).push_batch(stored)?);

        let mut batches = FetchedBatches::within(ByteLimit::Hard(u32::MAX));
        assert!(batches.push_record(record(0, 1_000, 0))?);
        let size = u32::try_from(batches.into_batches()?[0].size_in_bytes()?)?;

        assert!(FetchedBatches::within(ByteLimit::Hard(size)).push_record(record(0, 1_000, 0))?);
        assert!(
            !FetchedBatches::within(ByteLimit::Hard(size - 1)).push_record(record(0, 1_000, 0))?
        );

        Ok(())
    }

    /// A 1,000-byte first record is taken under a soft limit of 10 bytes, and left out under a hard
    /// limit of 10 bytes.
    #[test]
    fn only_a_limit_that_lets_it_takes_a_first_record_larger_than_the_limit() -> Result<()> {
        assert!(FetchedBatches::within(ByteLimit::Soft(10)).push_record(large_record(0))?);
        assert!(!FetchedBatches::within(ByteLimit::Hard(10)).push_record(large_record(0))?);

        Ok(())
    }

    /// Under a soft limit of 10 bytes, a 1,000-byte first batch is taken, and a 1-byte batch after
    /// it is not, because the first batch used up the limit.
    #[test]
    fn after_a_first_batch_larger_than_the_limit_no_batch_fits() -> Result<()> {
        let mut batches = FetchedBatches::within(ByteLimit::Soft(10));

        assert!(batches.push_batch(stored_batch(0, LARGE_VALUE)?)?);
        assert!(!batches.push_batch(stored_batch(1, 1)?)?);

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

    /// A storage double whose read sends each of `batches` in turn, and then returns.
    #[derive(Debug)]
    struct SendsBatches {
        batches: Vec<deflated::Batch>,
    }

    storage_double!(SendsBatches, async fn fetch(&self, fetched_sender) {
        let mut data = fetched_sender
            .send_offsets(OffsetStage {
                last_stable: 3,
                high_watermark: 3,
                log_start: 0,
            })
            .await?;

        for batch in &self.batches {
            data.send_batch(batch.clone()).await?;
        }

        Ok(ReadEnd::Finished)
    });

    /// The engine sends a small batch, a large batch, and another small batch. The limit holds two
    /// small batches but not the large one. The read returns only the first batch: it drops the
    /// third, although it fits, because the returned records must be contiguous.
    #[tokio::test]
    async fn a_read_returns_nothing_after_a_batch_that_does_not_fit() -> Result<()> {
        let first = stored_batch(0, 10)?;
        // The third batch fits what is left of the limit after the first.
        let max_bytes = u32::try_from(2 * first.size_in_bytes()?)?;

        let storage = SendsBatches {
            batches: vec![first, stored_batch(1, LARGE_VALUE)?, stored_batch(2, 10)?],
        };

        let read = storage
            .fetch_until_limit(
                &Topition::new("abc", 0),
                0,
                0,
                ByteLimit::Hard(max_bytes),
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_secs(1),
            )
            .await?;

        assert_eq!(vec![vec![0]], record_offsets(&read.batches)?);
        assert!(read.reached_limit);

        Ok(())
    }

    /// A storage double whose read sends the offsets of a 3-record partition and does not send its
    /// records. If `limit_reached` is set, the read returns [`ReadEnd::ReachedLimit`].
    #[derive(Debug)]
    struct SendsOnlyOffsets {
        limit_reached: bool,
    }

    storage_double!(SendsOnlyOffsets, async fn fetch(&self, fetched_sender) {
        let _data = fetched_sender
            .send_offsets(OffsetStage {
                last_stable: 3,
                high_watermark: 3,
                log_start: 0,
            })
            .await?;

        Ok(if self.limit_reached {
            ReadEnd::ReachedLimit
        } else {
            ReadEnd::Finished
        })
    });

    /// The engine sends the offsets of 3 records and does not send the records. The read reports
    /// that it reached its limit only if the engine returned [`ReadEnd::ReachedLimit`], because
    /// offsets can exist before their records are readable.
    #[tokio::test]
    async fn an_empty_read_reaches_its_limit_only_when_its_engine_says_so() -> Result<()> {
        for limit_reached in [true, false] {
            let read = SendsOnlyOffsets { limit_reached }
                .fetch_until_limit(
                    &Topition::new("abc", 0),
                    0,
                    0,
                    ByteLimit::Hard(10),
                    IsolationLevel::ReadUncommitted,
                    Instant::now() + Duration::from_secs(1),
                )
                .await?;

            assert!(read.batches.is_empty());
            assert_eq!(limit_reached, read.reached_limit);
        }

        Ok(())
    }

    /// A storage double whose read sends `records` records, yields once, and then sets `ended` and
    /// returns.
    #[derive(Debug)]
    struct SendsThenEnds {
        records: i64,
        ended: Arc<AtomicBool>,
    }

    storage_double!(SendsThenEnds, async fn fetch(&self, fetched_sender) {
        let _data = send_records(fetched_sender, self.records, Duration::ZERO).await?;
        // The yield lets the read receive the records first. So `ended` is set only if the read
        // polls the double again after the records reach the limit.
        tokio::task::yield_now().await;
        self.ended.store(true, Ordering::Relaxed);
        Ok(ReadEnd::Finished)
    });

    /// The engine sends 3 records, then ends after one more poll. Under a soft limit of 1 byte,
    /// only the first record fits. The read keeps polling until the engine ends, instead of
    /// dropping it, and returns that one record.
    #[tokio::test]
    async fn a_read_that_reaches_its_limit_lets_its_engine_end() -> Result<()> {
        let storage = SendsThenEnds {
            records: 3,
            ended: Arc::new(AtomicBool::new(false)),
        };

        // With a soft limit of 1 byte, only the first record fits.
        let read = storage
            .fetch_until_limit(
                &Topition::new("abc", 0),
                0,
                0,
                ByteLimit::Soft(1),
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_secs(1),
            )
            .await?;

        assert!(storage.ended.load(Ordering::Relaxed));
        assert_eq!(None, read.timed_out);
        assert!(read.reached_limit);
        assert_eq!(vec![vec![0]], record_offsets(&read.batches)?);

        Ok(())
    }

    /// The engine sends 3 records and then never ends. Under a soft limit of 1 byte, only the first
    /// record fits. The read polls the engine until the deadline, then returns that one record.
    #[tokio::test(start_paused = true)]
    async fn a_read_that_reaches_its_limit_waits_for_its_engine_until_its_deadline() -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(1);

        let read = SendsThenWaits { records: 3 }
            .fetch_until_limit(
                &Topition::new("abc", 0),
                0,
                0,
                ByteLimit::Soft(1),
                IsolationLevel::ReadUncommitted,
                deadline,
            )
            .await?;

        assert_eq!(deadline, Instant::now());
        assert_eq!(Some(TimedOut::Reading), read.timed_out);
        assert!(read.reached_limit);
        assert_eq!(vec![vec![0]], record_offsets(&read.batches)?);

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
            .map(|_data| ReadEnd::Finished)
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

    /// A storage double whose read sends its offsets, closes its channel, and fails later.
    #[derive(Debug)]
    struct ClosesThenFails;

    storage_double!(ClosesThenFails, async fn fetch(&self, fetched_sender) {
        drop(send_records(fetched_sender, 0, Duration::ZERO).await?);

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

    /// A read that fails after it has sent records returns its error from `fetch_until_deadline`,
    /// so a caller that only reads `batches` never takes a partial read for a whole one.
    #[tokio::test(start_paused = true)]
    async fn a_read_that_fails_after_it_sent_records_returns_its_error() {
        let read = fetch_from_start(
            &SendsThenFails { records: 2 },
            Instant::now() + Duration::from_secs(1),
        )
        .await;

        assert_matches!(read, Err(Error::Message(_)));
    }

    /// A storage double whose read sends `records` records, and then fails while it holds its
    /// sender.
    #[derive(Debug)]
    struct SendsThenFails {
        records: i64,
    }

    storage_double!(SendsThenFails, async fn fetch(&self, fetched_sender) {
        let _data = send_records(fetched_sender, self.records, Duration::ZERO).await?;
        Err(Error::Message("the read failed".into()))
    });

    /// A read that fails after it has sent records answers with its offsets, every record that it
    /// sent, and its error.
    #[tokio::test(start_paused = true)]
    async fn a_read_that_fails_after_it_sent_records_answers_with_them() -> Result<()> {
        // More records than the channel holds, so that some are still in the channel when the
        // read fails, and the caller takes them after the failure.
        let records = i64::try_from(CHANNEL_CAPACITY * 2)?;

        let read = SendsThenFails { records }
            .fetch_until_limit(
                &Topition::new("abc", 0),
                0,
                0,
                ByteLimit::Soft(u32::MAX),
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_secs(1),
            )
            .await?;

        assert_matches!(read.failed, Some(Error::Message(_)));
        assert_eq!(None, read.timed_out);
        assert_eq!(
            Some(records),
            read.offsets.map(|offsets| offsets.high_watermark)
        );
        assert_eq!(
            (0..records).collect::<Vec<_>>(),
            record_offsets(&read.batches)?.concat()
        );

        Ok(())
    }
}
