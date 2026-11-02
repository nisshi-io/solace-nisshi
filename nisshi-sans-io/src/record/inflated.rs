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

use crate::{
    BatchAttribute, ByteSize, Compression, Error, Result, TimestampType,
    record::{Record, codec::Sequence, deflated},
    ser::RecordBatchEncoder,
    to_timestamp,
};
use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::debug;

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Frame {
    pub batches: Vec<Batch>,
}

impl TryFrom<deflated::Frame> for Frame {
    type Error = Error;

    fn try_from(deflated: deflated::Frame) -> Result<Self, Self::Error> {
        deflated
            .batches
            .into_iter()
            .map(Batch::try_from)
            .collect::<Result<Vec<_>>>()
            .map(|batches| Self { batches })
    }
}

impl TryFrom<&deflated::Frame> for Frame {
    type Error = Error;

    fn try_from(deflated: &deflated::Frame) -> Result<Self, Self::Error> {
        deflated
            .batches
            .iter()
            .try_fold(Vec::new(), |mut acc, batch| {
                Batch::try_from(batch).map(|inflated| {
                    acc.push(inflated);
                    acc
                })
            })
            .map(|batches| Self { batches })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "deflated::Batch")]
pub struct Batch {
    pub base_offset: i64,
    pub batch_length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,

    #[serde(serialize_with = "Sequence::<Record>::serialize")]
    pub records: Vec<Record>,
}

const MAGIC: i8 = 2;

impl Default for Batch {
    fn default() -> Self {
        let base_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_default();

        Self {
            base_offset: 0,
            batch_length: Default::default(),
            partition_leader_epoch: -1,
            magic: MAGIC,
            crc: Default::default(),
            attributes: BatchAttribute::default()
                .compression(Compression::None)
                .timestamp(TimestampType::CreateTime)
                .into(),
            last_offset_delta: Default::default(),
            base_timestamp,
            max_timestamp: base_timestamp,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: Default::default(),
        }
    }
}

impl TryFrom<deflated::Batch> for Batch {
    type Error = Error;

    fn try_from(value: deflated::Batch) -> Result<Self, Self::Error> {
        let base_offset = value.base_offset;
        let batch_length = value.batch_length;
        let partition_leader_epoch = value.partition_leader_epoch;
        let magic = value.magic;
        let crc = value.crc;
        let attributes =
            BatchAttribute::try_from(value.attributes).inspect(|attribute| debug!(?attribute))?;
        let last_offset_delta = value.last_offset_delta;
        let base_timestamp = value.base_timestamp;
        let max_timestamp = value.max_timestamp;
        let producer_id = value.producer_id;
        let producer_epoch = value.producer_epoch;
        let base_sequence = value.base_sequence;

        let records: Vec<Record> = value.try_into()?;

        Ok(Self {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes: attributes.compression(Compression::None).into(),
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }
}

impl TryFrom<&deflated::Batch> for Batch {
    type Error = Error;

    fn try_from(value: &deflated::Batch) -> Result<Self, Self::Error> {
        let base_offset = value.base_offset;
        let batch_length = value.batch_length;
        let partition_leader_epoch = value.partition_leader_epoch;
        let magic = value.magic;
        let crc = value.crc;
        let attributes = value.attributes;
        let last_offset_delta = value.last_offset_delta;
        let base_timestamp = value.base_timestamp;
        let max_timestamp = value.max_timestamp;
        let producer_id = value.producer_id;
        let producer_epoch = value.producer_epoch;
        let base_sequence = value.base_sequence;

        let records: Vec<Record> = value.try_into()?;

        Ok(Self {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Compaction {
    pub batch: Batch,
    pub records: usize,
}

/// How [`Batch::append_rebased`] moves the records of a batch onto the end of another.
#[derive(Clone, Copy, Debug)]
struct Rebase {
    /// The offset delta of the first appended record, added to each appended offset delta.
    first_offset_delta: i32,

    /// The `last_offset_delta` of the merged batch.
    last_offset_delta: i32,

    /// The difference between the two base timestamps, added to each appended timestamp delta.
    timestamp_shift: i64,
}

impl Rebase {
    /// The rebase of `other` onto the end of `sink`, or [`Error::OffsetDeltaOverflow`] when the
    /// merged `last_offset_delta` does not fit in an `i32`.
    fn onto(sink: &Batch, other: &Batch) -> Result<Self> {
        let first_offset_delta = sink
            .last_offset_delta
            .checked_add(1)
            .ok_or(Error::OffsetDeltaOverflow("last_offset_delta"))?;

        let last_offset_delta = other
            .last_offset_delta
            .checked_add(1)
            .and_then(|appended| sink.last_offset_delta.checked_add(appended))
            .ok_or(Error::OffsetDeltaOverflow("last_offset_delta"))?;

        Ok(Self {
            first_offset_delta,
            last_offset_delta,
            timestamp_shift: other.base_timestamp.wrapping_sub(sink.base_timestamp),
        })
    }

    /// The timestamp delta and offset delta of `record` once rebased, or
    /// [`Error::OffsetDeltaOverflow`] when its offset delta does not fit in an `i32`.
    fn deltas(&self, record: &Record) -> Result<(i64, i32)> {
        let offset_delta = record
            .offset_delta
            .checked_add(self.first_offset_delta)
            .ok_or(Error::OffsetDeltaOverflow("record offset_delta"))?;

        Ok((
            record.timestamp_delta.wrapping_add(self.timestamp_shift),
            offset_delta,
        ))
    }
}

/// What decoding `record` charges to its batch's decode budget, when the batch is encoded with
/// `compression`. `encoded_size` gives the record's encoded size, which only a compressed batch
/// charges.
fn decoded_record_size(
    record: &Record,
    compression: &Compression,
    encoded_size: impl FnOnce() -> Result<usize>,
) -> Result<usize> {
    if *compression == Compression::None {
        Ok(record.allocated_size())
    } else {
        encoded_size().map(|encoded_size| record.allocated_size().saturating_add(encoded_size))
    }
}

impl Batch {
    pub fn compression(&self) -> Result<Compression> {
        Compression::try_from(self.attributes)
    }

    #[must_use]
    pub fn builder() -> Builder {
        Builder::default()
    }

    pub fn into_builder(self) -> Builder {
        self.into()
    }

    pub fn max_offset(&self) -> i64 {
        self.base_offset + i64::from(self.last_offset_delta)
    }

    /// Returns the timestamp that a consumer reads for `record`.
    ///
    /// For each record of a [`TimestampType::LogAppendTime`] batch, a consumer reads the batch's
    /// `max_timestamp`. For each record of a [`TimestampType::CreateTime`] batch, it reads
    /// `base_timestamp` plus the record's `timestamp_delta`, with Java's wrapping `long`
    /// addition. Kafka defines `max_timestamp` for each type in [`DefaultRecordBatch`].
    ///
    /// [`DefaultRecordBatch`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L90-L93
    pub fn record_timestamp(&self, record: &Record) -> i64 {
        match TimestampType::from(self.attributes) {
            TimestampType::LogAppendTime => self.max_timestamp,
            TimestampType::CreateTime => self.base_timestamp.wrapping_add(record.timestamp_delta),
        }
    }

    /// Appends the records of `other` to this batch, rebased onto this batch's base offset and
    /// base timestamp. Each record keeps its timestamp, and each offset delta moves past this
    /// batch's `last_offset_delta`, so the records of a well-formed `other` continue this batch's
    /// offsets.
    ///
    /// Returns [`Error::OffsetDeltaOverflow`] when an offset delta of the merged batch does not
    /// fit in an `i32`. This batch is then unchanged.
    ///
    /// Timestamp deltas use wrapping arithmetic, not checked arithmetic, because a checked rebase
    /// would reject batches that Kafka accepts: Kafka's default `timestamp.before.max.ms` is
    /// `Long.MAX_VALUE`, and nothing checks the producer's `base_timestamp` on a
    /// [`TimestampType::LogAppendTime`] batch, so two valid bases can be more than `i64::MAX`
    /// apart. The wrapping sum of this batch's base and a rebased delta is each record's original
    /// timestamp, which is the sum that a consumer computes in [`DefaultRecord`].
    ///
    /// [`DefaultRecord`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecord.java#L314
    pub fn append_rebased(&mut self, other: Batch) -> Result<()> {
        let rebase = Rebase::onto(self, &other)?;

        let records = other
            .records
            .into_iter()
            .map(|record| {
                rebase
                    .deltas(&record)
                    .map(|(timestamp_delta, offset_delta)| Record {
                        offset_delta,
                        timestamp_delta,
                        ..record
                    })
            })
            .collect::<Result<Vec<_>>>()?;

        self.records.extend(records);
        self.last_offset_delta = rebase.last_offset_delta;
        self.max_timestamp = self.max_timestamp.max(other.max_timestamp);

        Ok(())
    }

    /// Returns what the decoder charges to the decode budget for this batch once it is encoded
    /// with `compression`.
    ///
    /// The size is what the decoder charges to a batch's decode budget, which
    /// [`MAX_DECODED_BATCH_BYTES`] limits: the `Record` and `Header` structures of every record,
    /// plus the inflated record data when `compression` is not [`Compression::None`]. A batch whose
    /// size is over the limit does not decode.
    ///
    /// The size is exact for the encoding that this crate writes. It is not a bound on a batch as
    /// a client sent it, whose record data can be longer, for example with varints that are not
    /// minimal, so measure the batch that you will encode. Decoding takes a small multiple of this
    /// size in memory at its peak (see [`MAX_DECODED_BATCH_BYTES`]).
    ///
    /// [`MAX_DECODED_BATCH_BYTES`]: crate::record::MAX_DECODED_BATCH_BYTES
    pub fn decoded_size(&self, compression: &Compression) -> Result<usize> {
        self.records.iter().try_fold(0, |size: usize, record| {
            decoded_record_size(record, compression, || record.size_in_bytes())
                .map(|record_size| size.saturating_add(record_size))
        })
    }

    /// Returns how much [`Batch::decoded_size`] grows when [`Batch::append_rebased`] appends
    /// `other` to this batch, once the merged batch is encoded with `compression`.
    ///
    /// The growth is not `other.decoded_size(compression)`: the rebase changes the offset and
    /// timestamp deltas of each record of `other`, and with them the size of their varints.
    ///
    /// Returns the error that [`Batch::append_rebased`] would return for `other`.
    pub fn appended_decoded_size(&self, other: &Batch, compression: &Compression) -> Result<usize> {
        let rebase = Rebase::onto(self, other)?;

        other.records.iter().try_fold(0, |size: usize, record| {
            let (timestamp_delta, offset_delta) = rebase.deltas(record)?;

            decoded_record_size(record, compression, || {
                record.size_in_bytes_with_deltas(timestamp_delta, offset_delta)
            })
            .map(|record_size| size.saturating_add(record_size))
        })
    }

    /// Returns the greatest timestamp that a consumer reads from this batch (see
    /// [`Batch::record_timestamp`]), whatever its header claims.
    ///
    /// For a [`TimestampType::CreateTime`] batch this is the greatest record timestamp, starting
    /// from `-1` (Kafka's `NO_TIMESTAMP`), as Kafka's [`LogValidator`] computes the
    /// `max_timestamp` that it writes to the header. For a [`TimestampType::LogAppendTime`]
    /// batch, it is `max_timestamp`.
    ///
    /// [`LogValidator`]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogValidator.java#L249-L281
    pub fn max_record_timestamp(&self) -> i64 {
        match TimestampType::from(self.attributes) {
            TimestampType::LogAppendTime => self.max_timestamp,
            TimestampType::CreateTime => self
                .records
                .iter()
                .map(|record| self.record_timestamp(record))
                .fold(-1, i64::max),
        }
    }

    pub fn keys(&self) -> BTreeSet<Bytes> {
        self.records
            .iter()
            .fold(BTreeSet::new(), |mut acc, record| {
                if let Some(key) = record.key() {
                    _ = acc.insert(key);
                }

                acc
            })
    }

    /// Compact this batch, retaining only the most recent record per key.
    ///
    /// `head` contains the keys of records in newer batches: a record whose
    /// key is in `head` is dropped, as is a record whose key reappears later
    /// in this batch. Records without a key are always retained. The batch
    /// structure (base offset, offset deltas, timestamps) is preserved, so
    /// compaction leaves gaps in the offsets, as in Apache Kafka.
    ///
    /// The returned [`Compaction`] holds the compacted batch and the number
    /// of records that were removed.
    pub fn compact(mut self, head: &BTreeSet<Bytes>) -> Result<Compaction> {
        let mut last_delta_offset_for_key = BTreeMap::new();
        let mut records = 0;

        for record in self.records.iter() {
            if let Some(key) = record.key() {
                if head.contains(&key) {
                    records += 1;
                    continue;
                }

                if last_delta_offset_for_key
                    .insert(key, record.offset_delta)
                    .is_some()
                {
                    records += 1;
                }
            }
        }

        debug!(?records);

        if records > 0 {
            let delta_offsets_to_retain: BTreeSet<i32> =
                last_delta_offset_for_key.into_values().collect();
            debug!(?delta_offsets_to_retain);

            self.records.retain(|record| {
                record.key().is_none() || delta_offsets_to_retain.contains(&record.offset_delta)
            });

            self.into_builder()
                .build()
                .map(|batch| Compaction { batch, records })
        } else {
            Ok(Compaction {
                batch: self,
                records,
            })
        }
    }
}

impl From<Batch> for Builder {
    fn from(value: Batch) -> Self {
        Self {
            base_offset: value.base_offset,
            partition_leader_epoch: value.partition_leader_epoch,
            magic: value.magic,
            attributes: value.attributes,
            last_offset_delta: value.last_offset_delta,
            base_timestamp: value.base_timestamp,
            max_timestamp: value.max_timestamp,
            producer_id: value.producer_id,
            producer_epoch: value.producer_epoch,
            base_sequence: value.base_sequence,
            records: value.records.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Builder {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Sequence<super::Builder>,
}

impl Default for Builder {
    fn default() -> Self {
        let base_timestamp = to_timestamp(&SystemTime::now()).unwrap_or_default();

        Self {
            base_offset: 0,
            partition_leader_epoch: -1,
            magic: 2,
            attributes: 0,
            last_offset_delta: 0,
            base_timestamp,
            max_timestamp: base_timestamp,
            producer_id: -1,
            producer_epoch: 0,
            base_sequence: 0,
            records: Sequence::default(),
        }
    }
}

impl ByteSize for Builder {
    fn size_in_bytes(&self) -> Result<usize> {
        Ok(size_of_val(&self.partition_leader_epoch)
            + size_of_val(&self.magic)
            // size of crc is u32
            + size_of::<u32>()
            + size_of_val(&self.attributes)
            + size_of_val(&self.last_offset_delta)
            + size_of_val(&self.base_timestamp)
            + size_of_val(&self.max_timestamp)
            + size_of_val(&self.producer_id)
            + size_of_val(&self.producer_epoch)
            + size_of_val(&self.base_sequence)
            + self.records.size_in_bytes()?)
    }
}

impl Builder {
    #[must_use]
    pub fn base_offset(self, base_offset: i64) -> Self {
        Self {
            base_offset,
            ..self
        }
    }

    #[must_use]
    pub fn partition_leader_epoch(self, partition_leader_epoch: i32) -> Self {
        Self {
            partition_leader_epoch,
            ..self
        }
    }

    #[must_use]
    pub fn magic(self, magic: i8) -> Self {
        Self { magic, ..self }
    }

    #[must_use]
    pub fn attributes(self, attributes: i16) -> Self {
        Self { attributes, ..self }
    }

    #[must_use]
    pub fn last_offset_delta(self, last_offset_delta: i32) -> Self {
        Self {
            last_offset_delta,
            ..self
        }
    }

    #[must_use]
    pub fn base_timestamp(self, base_timestamp: i64) -> Self {
        Self {
            base_timestamp,
            ..self
        }
    }

    #[must_use]
    pub fn max_timestamp(self, max_timestamp: i64) -> Self {
        Self {
            max_timestamp,
            ..self
        }
    }

    #[must_use]
    pub fn producer_id(self, producer_id: i64) -> Self {
        Self {
            producer_id,
            ..self
        }
    }

    #[must_use]
    pub fn producer_epoch(self, producer_epoch: i16) -> Self {
        Self {
            producer_epoch,
            ..self
        }
    }

    #[must_use]
    pub fn base_sequence(self, base_sequence: i32) -> Self {
        Self {
            base_sequence,
            ..self
        }
    }

    #[must_use]
    pub fn record(mut self, record: super::Builder) -> Self {
        self.records.extend(vec![record]);
        self
    }

    /// Sets `max_timestamp` to the largest timestamp of the records. The largest timestamp is
    /// `base_timestamp` plus the largest `timestamp_delta`. If the builder has no records,
    /// `max_timestamp` does not change.
    #[must_use]
    pub fn max_timestamp_of_records(self) -> Self {
        let Some(max_delta) = self
            .records
            .iter()
            .map(|record| i64::from(record.timestamp_delta))
            .max()
        else {
            return self;
        };

        Self {
            max_timestamp: self.base_timestamp + max_delta,
            ..self
        }
    }

    fn crc(&self) -> Result<u32> {
        let mut encoder = RecordBatchEncoder::new(BytesMut::with_capacity(self.size_in_bytes()?));
        self.attributes.serialize(&mut encoder)?;
        self.last_offset_delta.serialize(&mut encoder)?;
        self.base_timestamp.serialize(&mut encoder)?;
        self.max_timestamp.serialize(&mut encoder)?;
        self.producer_id.serialize(&mut encoder)?;
        self.producer_epoch.serialize(&mut encoder)?;
        self.base_sequence.serialize(&mut encoder)?;
        self.records.serialize(&mut encoder)?;

        let encoded = Bytes::from(encoder);

        let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
        digest.update(&encoded[..]);

        Ok(digest.finalize() as u32)
    }

    pub fn build(self) -> Result<Batch> {
        let batch_length = self
            .size_in_bytes()
            .and_then(|size| i32::try_from(size).map_err(Into::into))?;

        let crc = self.crc()?;

        let records = self
            .records
            .0
            .into_iter()
            .try_fold(Vec::new(), |mut acc, record| {
                Record::try_from(record).map(|record| {
                    acc.push(record);
                    acc
                })
            })?;

        Ok(Batch {
            base_offset: self.base_offset,
            batch_length,
            partition_leader_epoch: self.partition_leader_epoch,
            magic: self.magic,
            crc,
            attributes: self.attributes,
            last_offset_delta: self.last_offset_delta,
            base_timestamp: self.base_timestamp,
            max_timestamp: self.max_timestamp,
            producer_id: self.producer_id,
            producer_epoch: self.producer_epoch,
            base_sequence: self.base_sequence,
            records,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use super::*;
    use crate::{MaximumAllocationSize, Result, de::BatchDecoder};

    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(EnvFilter::from_default_env().add_directive(
                    format!("{}=debug", env!("CARGO_PKG_NAME").replace("-", "_")).parse()?,
                ))
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[test]
    fn record_maximum_allocation() -> Result<()> {
        let record = Record::builder()
            .value(Some(Bytes::from_static(&[100, 101, 102])))
            .build()?;

        let mut encoder = RecordBatchEncoder::new(BytesMut::new());
        record.serialize(&mut encoder)?;

        let encoded = BytesMut::from(encoder);
        assert_eq!(record.maximum_allocation_size()?, encoded.len());

        Ok(())
    }

    #[test]
    fn batch() -> Result<()> {
        let _guard = init_tracing()?;

        let decoded = Batch::builder()
            .base_offset(0)
            .partition_leader_epoch(-1)
            .magic(2)
            .attributes(0)
            .last_offset_delta(0)
            .base_timestamp(1_707_058_170_165)
            .max_timestamp(1_707_058_170_165)
            .producer_id(1)
            .producer_epoch(0)
            .base_sequence(1)
            .record(Record::builder().value(Some(Bytes::from_static(&[100, 101, 102]))))
            .build()?;

        assert_eq!(decoded.batch_length, 59);
        assert_eq!(decoded.crc, 1_126_819_645);

        Ok(())
    }

    #[test]
    fn batch_decode() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = vec![
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 59, 255, 255, 255, 255, 2, 67, 41, 231, 61, 0, 0, 0,
            0, 0, 0, 0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 0, 0,
            0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 18, 0, 0, 0, 1, 6, 100, 101, 102, 0,
        ];

        let decoded = Batch::builder()
            .base_offset(0)
            .partition_leader_epoch(-1)
            .magic(2)
            .attributes(0)
            .last_offset_delta(0)
            .base_timestamp(1_707_058_170_165)
            .max_timestamp(1_707_058_170_165)
            .producer_id(1)
            .producer_epoch(0)
            .base_sequence(1)
            .record(Record::builder().value(Some(Bytes::from(vec![100, 101, 102]))))
            .build()?;

        let decoder = BatchDecoder::new(Bytes::copy_from_slice(&encoded[..]));
        let actual = Batch::deserialize(decoder)?;

        assert_eq!(decoded, actual);

        Ok(())
    }

    #[test]
    fn batch_encode() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = Bytes::from_static(&[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 59, 255, 255, 255, 255, 2, 67, 41, 231, 61, 0, 0, 0,
            0, 0, 0, 0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 0, 0,
            0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 18, 0, 0, 0, 1, 6, 100, 101, 102, 0,
        ]);

        let decoded = Batch::builder()
            .base_offset(0)
            .partition_leader_epoch(-1)
            .magic(2)
            .attributes(0)
            .last_offset_delta(0)
            .base_timestamp(1_707_058_170_165)
            .max_timestamp(1_707_058_170_165)
            .producer_id(1)
            .producer_epoch(0)
            .base_sequence(1)
            .record(Record::builder().value(Some(Bytes::from(vec![100, 101, 102]))))
            .build()?;

        let decoder = BatchDecoder::new(encoded);
        let actual = Batch::deserialize(decoder)?;

        assert_eq!(decoded, actual);

        Ok(())
    }

    #[test]
    fn build_batch_records() -> Result<()> {
        let _guard = init_tracing()?;

        let keys: Vec<_> = (0..=6).map(|i| format!("k{i}")).map(Bytes::from).collect();
        let values: Vec<_> = (0..=11).map(|i| format!("v{i}")).map(Bytes::from).collect();

        let mut builder = Batch::builder();
        let indexes = [
            (1, 1),
            (2, 2),
            (1, 3),
            (1, 4),
            (3, 5),
            (2, 6),
            (4, 7),
            (5, 8),
            (5, 9),
            (2, 10),
            (6, 11),
        ];

        for (offset_delta, (key_index, value_index)) in indexes.into_iter().enumerate() {
            builder = builder.record(
                Record::builder()
                    .offset_delta(i32::try_from(offset_delta)?)
                    .key(Some(keys[key_index].clone()))
                    .value(Some(values[value_index].clone())),
            );
        }

        let batch = builder.build()?;
        assert_eq!(indexes.len(), batch.records.len());

        Ok(())
    }

    fn keyed(offset_delta: i32, key: &'static [u8], value: &'static [u8]) -> super::super::Builder {
        Record::builder()
            .offset_delta(offset_delta)
            .key(Some(Bytes::from_static(key)))
            .value(Some(Bytes::from_static(value)))
    }

    #[test]
    fn compact_noop_without_duplicates() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = Batch::builder()
            .record(keyed(0, b"k1", b"v1"))
            .record(keyed(1, b"k2", b"v2"))
            .last_offset_delta(1)
            .build()?;

        let compaction = batch.clone().compact(&BTreeSet::new())?;

        assert_eq!(0, compaction.records);
        assert_eq!(batch, compaction.batch);

        Ok(())
    }

    #[test]
    fn compact_within_batch_keeps_last_occurrence_per_key() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = Batch::builder()
            .base_offset(5)
            .record(keyed(0, b"k1", b"v1"))
            .record(keyed(1, b"k2", b"v2"))
            .record(keyed(2, b"k1", b"v3"))
            .last_offset_delta(2)
            .build()?;

        let compaction = batch.compact(&BTreeSet::new())?;

        assert_eq!(1, compaction.records);

        let records = &compaction.batch.records;
        assert_eq!(2, records.len());
        assert_eq!(1, records[0].offset_delta);
        assert_eq!(Some(Bytes::from_static(b"k2")), records[0].key);
        assert_eq!(2, records[1].offset_delta);
        assert_eq!(Some(Bytes::from_static(b"v3")), records[1].value);

        // batch structure is preserved so offsets keep their gaps
        assert_eq!(5, compaction.batch.base_offset);
        assert_eq!(2, compaction.batch.last_offset_delta);

        Ok(())
    }

    #[test]
    fn compact_drops_keys_in_head() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = Batch::builder()
            .record(keyed(0, b"k1", b"v1"))
            .record(keyed(1, b"k2", b"v2"))
            .last_offset_delta(1)
            .build()?;

        let head = BTreeSet::from([Bytes::from_static(b"k1"), Bytes::from_static(b"k2")]);
        let compaction = batch.compact(&head)?;

        assert_eq!(2, compaction.records);
        assert!(compaction.batch.records.is_empty());

        Ok(())
    }

    #[test]
    fn compact_retains_keyless_records() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = Batch::builder()
            .record(
                Record::builder()
                    .offset_delta(0)
                    .value(Some(Bytes::from_static(b"v0"))),
            )
            .record(keyed(1, b"k1", b"v1"))
            .record(keyed(2, b"k1", b"v2"))
            .last_offset_delta(2)
            .build()?;

        let compaction = batch.compact(&BTreeSet::new())?;

        assert_eq!(1, compaction.records);

        let records = &compaction.batch.records;
        assert_eq!(2, records.len());
        assert_eq!(0, records[0].offset_delta);
        assert_eq!(None, records[0].key);
        assert_eq!(2, records[1].offset_delta);
        assert_eq!(Some(Bytes::from_static(b"k1")), records[1].key);

        Ok(())
    }

    /// Returns a `CreateTime` batch with one record for each of `timestamp_deltas`, at
    /// consecutive offset deltas from 0.
    fn batch_with_timestamp_deltas(base_timestamp: i64, timestamp_deltas: &[i64]) -> Result<Batch> {
        let builder = timestamp_deltas.iter().zip(0..).fold(
            Batch::builder()
                .base_timestamp(base_timestamp)
                .max_timestamp(
                    base_timestamp
                        .wrapping_add(timestamp_deltas.iter().copied().max().unwrap_or_default()),
                )
                .last_offset_delta(i32::try_from(timestamp_deltas.len())? - 1),
            |builder, (timestamp_delta, offset_delta)| {
                builder.record(
                    Record::builder()
                        .offset_delta(offset_delta)
                        .timestamp_delta(*timestamp_delta)
                        .value(Some(Bytes::from_static(b"v"))),
                )
            },
        );

        builder.build()
    }

    fn record_timestamps(batch: &Batch) -> Vec<i64> {
        batch
            .records
            .iter()
            .map(|record| batch.record_timestamp(record))
            .collect()
    }

    fn offset_deltas(batch: &Batch) -> Vec<i32> {
        batch
            .records
            .iter()
            .map(|record| record.offset_delta)
            .collect()
    }

    #[test]
    fn append_rebased_gives_contiguous_offsets_and_keeps_each_record_timestamp() -> Result<()> {
        let _guard = init_tracing()?;

        let mut sink = batch_with_timestamp_deltas(1_000, &[0, 5])?;
        let appended = batch_with_timestamp_deltas(2_000, &[0, 7])?;

        sink.append_rebased(appended)?;

        assert_eq!(vec![0, 1, 2, 3], offset_deltas(&sink));
        assert_eq!(vec![1_000, 1_005, 2_000, 2_007], record_timestamps(&sink));
        assert_eq!(3, sink.last_offset_delta);
        assert_eq!(2_007, sink.max_timestamp);

        Ok(())
    }

    #[test]
    fn append_rebased_keeps_each_record_timestamp_when_the_appended_base_is_earlier() -> Result<()>
    {
        let _guard = init_tracing()?;

        let mut sink = batch_with_timestamp_deltas(2_000, &[0, 7])?;
        let appended = batch_with_timestamp_deltas(1_000, &[0, 5])?;

        sink.append_rebased(appended)?;

        assert_eq!(vec![0, 1, 2, 3], offset_deltas(&sink));
        assert_eq!(vec![2_000, 2_007, 1_000, 1_005], record_timestamps(&sink));
        assert_eq!(3, sink.last_offset_delta);
        assert_eq!(2_007, sink.max_timestamp);

        Ok(())
    }

    #[test]
    fn append_rebased_keeps_each_record_timestamp_when_the_bases_are_more_than_i64_max_apart()
    -> Result<()> {
        let _guard = init_tracing()?;

        let mut sink = batch_with_timestamp_deltas(i64::MIN, &[0])?;
        let appended = batch_with_timestamp_deltas(i64::MAX, &[0])?;

        assert_eq!(vec![i64::MIN], record_timestamps(&sink));
        assert_eq!(vec![i64::MAX], record_timestamps(&appended));

        sink.append_rebased(appended)?;

        assert_eq!(vec![0, 1], offset_deltas(&sink));
        assert_eq!(vec![i64::MIN, i64::MAX], record_timestamps(&sink));
        assert_eq!(i64::MAX, sink.max_timestamp);

        Ok(())
    }

    #[test]
    fn append_rebased_rejects_a_record_offset_delta_that_overflows_and_leaves_the_sink_unchanged()
    -> Result<()> {
        let _guard = init_tracing()?;

        let mut sink = batch_with_timestamp_deltas(1_000, &[0])?;
        let appended = Batch::builder()
            .base_timestamp(2_000)
            .max_timestamp(2_000)
            .record(
                Record::builder()
                    .offset_delta(i32::MAX)
                    .value(Some(Bytes::from_static(b"v"))),
            )
            .build()?;

        let before = sink.clone();

        assert_matches!(
            sink.appended_decoded_size(&appended, &Compression::None),
            Err(Error::OffsetDeltaOverflow("record offset_delta"))
        );
        assert_matches!(
            sink.append_rebased(appended),
            Err(Error::OffsetDeltaOverflow("record offset_delta"))
        );
        assert_eq!(before, sink);

        Ok(())
    }

    #[test]
    fn append_rebased_rejects_a_last_offset_delta_that_overflows_and_leaves_the_sink_unchanged()
    -> Result<()> {
        let _guard = init_tracing()?;

        for (sink_last_offset_delta, appended_last_offset_delta) in [
            (i32::MAX, 0),
            (i32::MAX, -1),
            (0, i32::MAX),
            (i32::MAX - 1, 1),
        ] {
            let mut sink = batch_with_timestamp_deltas(1_000, &[0])?;
            sink.last_offset_delta = sink_last_offset_delta;

            let mut appended = batch_with_timestamp_deltas(2_000, &[0])?;
            appended.last_offset_delta = appended_last_offset_delta;

            let before = sink.clone();

            assert_matches!(
                sink.appended_decoded_size(&appended, &Compression::None),
                Err(Error::OffsetDeltaOverflow("last_offset_delta"))
            );
            assert_matches!(
                sink.append_rebased(appended),
                Err(Error::OffsetDeltaOverflow("last_offset_delta")),
                "sink last_offset_delta: {sink_last_offset_delta}, appended last_offset_delta: \
                 {appended_last_offset_delta}"
            );
            assert_eq!(before, sink);
        }

        Ok(())
    }

    #[test]
    fn compact_retains_keyless_when_all_keyed_records_drop() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = Batch::builder()
            .record(
                Record::builder()
                    .offset_delta(0)
                    .value(Some(Bytes::from_static(b"v0"))),
            )
            .record(keyed(1, b"k1", b"v1"))
            .last_offset_delta(1)
            .build()?;

        let head = BTreeSet::from([Bytes::from_static(b"k1")]);
        let compaction = batch.compact(&head)?;

        assert_eq!(1, compaction.records);

        let records = &compaction.batch.records;
        assert_eq!(1, records.len());
        assert_eq!(0, records[0].offset_delta);
        assert_eq!(None, records[0].key);

        Ok(())
    }

    fn timestamped(
        timestamp_type: TimestampType,
        base_timestamp: i64,
        max_timestamp: i64,
        deltas: &[i64],
    ) -> Result<Batch> {
        deltas
            .iter()
            .zip(0..)
            .fold(
                Batch::builder()
                    .attributes(BatchAttribute::default().timestamp(timestamp_type).into())
                    .base_timestamp(base_timestamp)
                    .max_timestamp(max_timestamp)
                    .last_offset_delta(i32::try_from(deltas.len())? - 1),
                |builder, (&timestamp_delta, offset_delta)| {
                    builder.record(
                        Record::builder()
                            .timestamp_delta(timestamp_delta)
                            .offset_delta(offset_delta)
                            .value(Some(Bytes::from_static(b"v"))),
                    )
                },
            )
            .build()
    }

    #[test]
    fn max_record_timestamp_is_the_largest_record_not_the_last() -> Result<()> {
        let batch = timestamped(TimestampType::CreateTime, 1_000, -1, &[0, 300, 100])?;
        assert_eq!(1_300, batch.max_record_timestamp());

        Ok(())
    }

    /// A record whose `base_timestamp + timestamp_delta` wraps has the wrapped timestamp, as
    /// Java's `long` addition gives a consumer, so it is below the record before it here.
    #[test]
    fn max_record_timestamp_takes_a_wrapped_record_timestamp() -> Result<()> {
        let batch = timestamped(TimestampType::CreateTime, i64::MAX - 10, -1, &[5, 11])?;
        assert_eq!(i64::MIN, batch.record_timestamp(&batch.records[1]));
        assert_eq!(i64::MAX - 5, batch.max_record_timestamp());

        Ok(())
    }

    #[test]
    fn max_record_timestamp_of_a_batch_without_a_timestamp_is_no_timestamp() -> Result<()> {
        let batch = timestamped(TimestampType::CreateTime, -1, 5_000, &[0])?;
        assert_eq!(-1, batch.max_record_timestamp());

        Ok(())
    }

    /// A consumer reads `max_timestamp` for each record of a `LogAppendTime` batch, whatever its
    /// deltas.
    #[test]
    fn max_record_timestamp_of_log_append_time_is_the_header() -> Result<()> {
        let batch = timestamped(TimestampType::LogAppendTime, 1_000, 2_000, &[0, 5_000])?;
        assert_eq!(2_000, batch.max_record_timestamp());

        Ok(())
    }
}
