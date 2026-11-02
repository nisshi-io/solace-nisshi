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
//
//! Deflated (compressed) Kafka Records
use std::{
    fmt::{self, Formatter},
    io::{BufRead, Read, Write},
    result,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use flate2::write::GzEncoder;
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, Visitor},
};
use tracing::{debug, error, instrument};

use crate::{
    ByteSize, Compression, Decode as _, Encode, Error, ErrorCode, Result, TimestampType,
    record::{
        Record,
        codec::{
            DecodeBudget, MAX_DECODED_BATCH_BYTES, MAX_PREALLOCATED_ELEMENTS,
            MIN_ENCODED_RECORD_SIZE, exceeds_decoded_batch_limit,
        },
    },
};

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Frame {
    pub batches: Vec<Batch>,
}

impl ByteSize for Frame {
    #[instrument(skip_all, ret)]
    fn size_in_bytes(&self) -> Result<usize> {
        self.batches
            .iter()
            .map(ByteSize::size_in_bytes)
            .sum::<Result<usize>>()
            .map(|batches| size_of::<i32>() + batches)
    }
}

/// The encoded size of the batch, from `base_offset` to the end of `record_data`. Kafka's
/// `RecordBatch.sizeInBytes` measures the same span.
impl ByteSize for Batch {
    fn size_in_bytes(&self) -> Result<usize> {
        // base_offset
        Ok(size_of::<i64>()
            // batch_length
            + size_of::<i32>()
            + FIXED_BATCH_LENGTH
            + self.record_data.len())
    }
}

impl TryFrom<crate::record::inflated::Frame> for Frame {
    type Error = Error;

    fn try_from(inflated: crate::record::inflated::Frame) -> Result<Self, Self::Error> {
        inflated
            .batches
            .into_iter()
            .map(Batch::try_from)
            .collect::<Result<Vec<_>>>()
            .map(|batches| Self { batches })
    }
}

/// A deflated (compressed) batch of Kafka records
///
/// `Debug` writes the length of `record_data`, not its content, so logging a
/// batch, or a produce or fetch message that holds one, doesn't log records.
#[derive(Clone, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
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
    pub record_count: u32,
    pub record_data: Bytes,
}

impl fmt::Debug for Batch {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Batch")
            .field("base_offset", &self.base_offset)
            .field("batch_length", &self.batch_length)
            .field("partition_leader_epoch", &self.partition_leader_epoch)
            .field("magic", &self.magic)
            .field("crc", &self.crc)
            .field("attributes", &self.attributes)
            .field("last_offset_delta", &self.last_offset_delta)
            .field("base_timestamp", &self.base_timestamp)
            .field("max_timestamp", &self.max_timestamp)
            .field("producer_id", &self.producer_id)
            .field("producer_epoch", &self.producer_epoch)
            .field("base_sequence", &self.base_sequence)
            .field("record_count", &self.record_count)
            .field("record_data_len", &self.record_data.len())
            .finish()
    }
}

impl From<Batch> for Bytes {
    fn from(value: Batch) -> Self {
        let mut encoded =
            BytesMut::with_capacity(value.batch_length as usize + size_of_val(&value.base_offset));

        encoded.put_i64(value.base_offset);
        encoded.put_i32(value.batch_length);
        encoded.put_i32(value.partition_leader_epoch);
        encoded.put_i8(value.magic);
        encoded.put_u32(value.crc);
        encoded.put_i16(value.attributes);
        encoded.put_i32(value.last_offset_delta);
        encoded.put_i64(value.base_timestamp);
        encoded.put_i64(value.max_timestamp);
        encoded.put_i64(value.producer_id);
        encoded.put_i16(value.producer_epoch);
        encoded.put_i32(value.base_sequence);
        encoded.put_u32(value.record_count);
        encoded.put(value.record_data);

        Bytes::from(encoded)
    }
}

impl TryFrom<Bytes> for Batch {
    type Error = Error;
    fn try_from(mut encoded: Bytes) -> result::Result<Self, Self::Error> {
        let base_offset = encoded.try_get_i64()?;
        let batch_length = encoded.try_get_i32()?;

        let partition_leader_epoch = encoded.try_get_i32()?;
        let magic = encoded.try_get_i8()?;
        let crc = encoded.try_get_u32()?;

        debug!(base_offset, batch_length);

        let crc_data_size = usize::try_from(batch_length)
            .map_err(Into::into)
            .and_then(|batch_length| {
                batch_length
                    .checked_sub(size_of_val(&partition_leader_epoch))
                    .ok_or(Error::Overflow)
                    .inspect_err(|err| {
                        debug!(
                            batch_length,
                            size_of_val = size_of_val(&partition_leader_epoch),
                            ?err
                        )
                    })
            })
            .and_then(|batch_length| {
                batch_length
                    .checked_sub(size_of_val(&magic))
                    .ok_or(Error::Overflow)
                    .inspect_err(|err| {
                        debug!(batch_length, size_of_val = size_of_val(&magic), ?err)
                    })
            })
            .and_then(|batch_length| {
                batch_length
                    .checked_sub(size_of_val(&crc))
                    .ok_or(Error::Overflow)
                    .inspect_err(|err| debug!(batch_length, size_of_val = size_of_val(&crc), ?err))
            })?;

        debug!(crc_data_size, encoded = encoded.len(), crc);

        if crc_data_size > encoded.len() {
            return Err(Error::Overflow);
        }

        let crc_data = &encoded[..crc_data_size];

        let computed = {
            let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
            digest.update(crc_data);

            digest.finalize() as u32
        };

        if computed != crc {
            error!(crc, computed);
        }

        let attributes = encoded.try_get_i16()?;
        let last_offset_delta = encoded.try_get_i32()?;
        let base_timestamp = encoded.try_get_i64()?;
        let max_timestamp = encoded.try_get_i64()?;
        let producer_id = encoded.try_get_i64()?;
        let producer_epoch = encoded.try_get_i16()?;
        let base_sequence = encoded.try_get_i32()?;
        let record_count = encoded.try_get_u32()?;

        let record_data_size =
            usize::try_from(batch_length)
                .map_err(Into::into)
                .and_then(|batch_length| {
                    batch_length
                        .checked_sub(FIXED_BATCH_LENGTH)
                        .ok_or(Error::Overflow)
                })?;

        debug!(record_data_size, encoded = encoded.len(), computed);

        if record_data_size > encoded.len() {
            return Err(Error::Overflow);
        }

        let record_data = encoded.slice(..record_data_size);

        let batch = Batch {
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
            record_count,
            record_data,
        };

        Ok(batch)
    }
}

impl Batch {
    const TRANSACTIONAL_BITMASK: i16 = 0b1_0000i16;
    const CONTROL_BITMASK: i16 = 0b10_0000i16;

    pub fn is_transactional(&self) -> bool {
        self.attributes & Self::TRANSACTIONAL_BITMASK == Self::TRANSACTIONAL_BITMASK
    }

    pub fn is_control(&self) -> bool {
        self.attributes & Self::CONTROL_BITMASK == Self::CONTROL_BITMASK
    }

    pub fn is_idempotent(&self) -> bool {
        self.producer_id != -1 && self.base_sequence != -1
    }

    /// True if `record_count` alone implies more decoded memory than
    /// `MAX_DECODED_BATCH_BYTES` allows, without attempting any
    /// decompression. A cheap, public pre-check so a caller that wants to
    /// reject a batch before invoking storage (and before paying for
    /// decompression) can do so with a specific, client-facing error rather
    /// than waiting for the full decode's failure; see `nisshi-storage`'s
    /// produce-path rejection, which uses this the same way it already uses
    /// `record_count < 1` and the `last_offset_delta` mismatch check.
    pub fn exceeds_decoded_record_count_limit(&self) -> bool {
        usize::try_from(self.record_count)
            .map(|record_count| exceeds_decoded_batch_limit(record_count, size_of::<Record>()))
            .unwrap_or(true)
    }

    /// The CRC-32C of this batch's current contents, as Kafka's own encoder
    /// would compute it: everything from `attributes` through `record_data`.
    pub fn computed_crc(&self) -> u32 {
        CrcData::from(self).crc()
    }

    /// Sets the timestamp type, `base_timestamp` and `max_timestamp`, and recomputes `crc` so that
    /// it covers the new values.
    ///
    /// The timestamp type is a bit of `attributes`. A consumer reads each record's timestamp from
    /// these fields, by the rule of [`inflated::Batch::record_timestamp`].
    ///
    /// The method verifies `crc` against the current contents first, because
    /// a new `crc` would make a batch that was damaged in transit look intact
    /// to every consumer. Kafka rejects such a batch with `CORRUPT_MESSAGE`
    /// before it validates timestamps.
    ///
    /// Returns [`Error::ApiError`] with [`ErrorCode::CorruptMessage`] when
    /// `crc` does not match the current contents. The batch is then
    /// unchanged.
    ///
    /// [`inflated::Batch::record_timestamp`]: crate::record::inflated::Batch::record_timestamp
    pub fn set_timestamps(
        &mut self,
        timestamp_type: TimestampType,
        base_timestamp: i64,
        max_timestamp: i64,
    ) -> Result<()> {
        if self.computed_crc() != self.crc {
            return Err(Error::ApiError(ErrorCode::CorruptMessage));
        }

        self.attributes =
            (self.attributes & !TimestampType::TIMESTAMP_TYPE_BITMASK) | i16::from(timestamp_type);
        self.base_timestamp = base_timestamp;
        self.max_timestamp = max_timestamp;
        self.crc = self.computed_crc();
        Ok(())
    }
}

/// `Debug` writes the length of `record_data`, not its content, as
/// [`Batch`] does.
#[derive(Clone, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct CrcData {
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub record_count: u32,
    pub record_data: Bytes,
}

impl From<&Batch> for CrcData {
    fn from(batch: &Batch) -> Self {
        Self {
            attributes: batch.attributes,
            last_offset_delta: batch.last_offset_delta,
            base_timestamp: batch.base_timestamp,
            max_timestamp: batch.max_timestamp,
            producer_id: batch.producer_id,
            producer_epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
            record_count: batch.record_count,
            record_data: batch.record_data.clone(),
        }
    }
}

impl fmt::Debug for CrcData {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CrcData")
            .field("attributes", &self.attributes)
            .field("last_offset_delta", &self.last_offset_delta)
            .field("base_timestamp", &self.base_timestamp)
            .field("max_timestamp", &self.max_timestamp)
            .field("producer_id", &self.producer_id)
            .field("producer_epoch", &self.producer_epoch)
            .field("base_sequence", &self.base_sequence)
            .field("record_count", &self.record_count)
            .field("record_data_len", &self.record_data.len())
            .finish()
    }
}

impl CrcData {
    fn into_batch(self, base_offset: i64, partition_leader_epoch: i32, magic: i8) -> Result<Batch> {
        let crc = self.crc();
        debug!(?self, base_offset, partition_leader_epoch, magic, crc);

        Ok(Batch {
            base_offset,
            batch_length: i32::try_from(FIXED_BATCH_LENGTH + self.record_data.len())?,
            partition_leader_epoch,
            magic,
            crc,
            attributes: self.attributes,
            last_offset_delta: self.last_offset_delta,
            base_timestamp: self.base_timestamp,
            max_timestamp: self.max_timestamp,
            producer_id: self.producer_id,
            producer_epoch: self.producer_epoch,
            base_sequence: self.base_sequence,
            record_count: self.record_count,
            record_data: self.record_data,
        })
    }

    fn crc(&self) -> u32 {
        let mut header = Vec::with_capacity(FIXED_CRC_HEADER_LENGTH);
        header.put_i16(self.attributes);
        header.put_i32(self.last_offset_delta);
        header.put_i64(self.base_timestamp);
        header.put_i64(self.max_timestamp);
        header.put_i64(self.producer_id);
        header.put_i16(self.producer_epoch);
        header.put_i32(self.base_sequence);
        header.put_u32(self.record_count);

        let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
        digest.update(&header[..]);
        digest.update(&self.record_data[..]);

        digest.finalize() as u32
    }
}

fn into_record_data(records: &[Record], compression: Compression) -> Result<Bytes> {
    let sizing = records
        .iter()
        .map(|record| record.size_in_bytes())
        .collect::<Result<Vec<_>>>()
        .map(|sizes| sizes.iter().sum::<usize>())?;

    debug!(sizing);

    match compression {
        Compression::None => records.encode(),

        Compression::Gzip => {
            let uncompressed = records
                .encode()
                .inspect(|inflated| debug!(inflated_len = inflated.len()))?;

            let mut gz = GzEncoder::new(
                BytesMut::with_capacity(uncompressed.len()).writer(),
                flate2::Compression::default(),
            );

            gz.write_all(&uncompressed)?;

            gz.finish()
                .map(|w| w.into_inner())
                .map(Bytes::from)
                .inspect(|deflated| debug!(deflated_len = deflated.len()))
                .map_err(Into::into)
        }

        // A raw snappy block, without xerial framing. Every Kafka client
        // decoder accepts the raw form (franz-go, librdkafka, kafka-python
        // and snappy-java all check for the xerial magic and fall back to
        // a raw block), as does [`Compression::inflator`].
        Compression::Snappy => {
            let uncompressed = records.encode()?;

            snap::raw::Encoder::new()
                .compress_vec(&uncompressed[..])
                .map(Bytes::from)
                .map_err(Into::into)
        }

        Compression::Lz4 => {
            let uncompressed = records.encode()?;

            let mut lz4 = lz4::EncoderBuilder::new()
                .build(BytesMut::with_capacity(uncompressed.len()).writer())?;

            lz4.write_all(&uncompressed[..])?;

            let (w, _) = lz4.finish();
            Ok(Bytes::from(w.into_inner()))
        }

        Compression::Zstd => {
            let uncompressed = records.encode()?;

            let mut zstd = zstd::stream::write::Encoder::new(
                BytesMut::with_capacity(uncompressed.len()).writer(),
                0,
            )?;

            zstd.write_all(&uncompressed[..])?;

            zstd.finish()
                .map(|w| w.into_inner())
                .map(Bytes::from)
                .map_err(Into::into)
        }
    }
}

impl TryFrom<crate::record::inflated::Batch> for Batch {
    type Error = Error;

    fn try_from(batch: crate::record::inflated::Batch) -> std::result::Result<Self, Self::Error> {
        CrcData {
            attributes: batch.attributes,
            last_offset_delta: batch.last_offset_delta,
            base_timestamp: batch.base_timestamp,
            max_timestamp: batch.max_timestamp,
            producer_id: batch.producer_id,
            producer_epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
            record_count: u32::try_from(batch.records.len())?,
            record_data: into_record_data(&batch.records[..], batch.compression()?)?,
        }
        .into_batch(batch.base_offset, batch.partition_leader_epoch, batch.magic)
    }
}

impl Batch {
    pub fn max_offset(&self) -> i64 {
        self.base_offset + i64::from(self.last_offset_delta)
    }

    fn compression(&self) -> Result<Compression> {
        Compression::try_from(self.attributes)
    }
}

impl TryFrom<Batch> for Vec<Record> {
    type Error = Error;

    #[instrument(skip_all)]
    fn try_from(batch: Batch) -> Result<Self, Self::Error> {
        let record_count = usize::try_from(batch.record_count)?;

        debug!(?record_count);
        debug!(record_data_len = batch.record_data.len());

        let uncompressed = batch
            .compression()
            .is_ok_and(|compression| compression == Compression::None);

        // On the uncompressed path `record_data` is the byte stream the
        // records are decoded from, and no record encodes in fewer than
        // `MIN_ENCODED_RECORD_SIZE` bytes, so a larger `record_count` means
        // the batch is corrupt. This runs ahead of the decoded-size check so
        // that such a batch is reported as corrupt, not as too large.
        if uncompressed && record_count > batch.record_data.len() / MIN_ENCODED_RECORD_SIZE {
            return Err(Error::Overflow);
        }

        if batch.exceeds_decoded_record_count_limit() {
            return Err(Error::MessageMaxSizeExceeded(
                record_count.saturating_mul(size_of::<Record>()),
            ));
        }

        if uncompressed {
            decode_records(
                batch.record_data,
                record_count,
                &mut DecodeBudget::new(MAX_DECODED_BATCH_BYTES),
            )
        } else {
            batch.compression().and_then(|compression| {
                inflate_records(
                    compression,
                    batch.record_data.reader(),
                    record_count,
                    MAX_DECODED_BATCH_BYTES,
                    MAX_DECODED_BATCH_BYTES,
                )
            })
        }
    }
}

/// Decodes `record_count` records from `record_data`, which `compression`
/// inflates.
///
/// `inflated_limit` caps the bytes that the decompressing reader may produce
/// (`LimitedRead`), and `decoded_limit` caps the memory that the inflated
/// bytes and the decoded records use together. Production passes
/// `MAX_DECODED_BATCH_BYTES` for both. The two limits are separate parameters
/// so that a test can exercise each one on its own.
fn inflate_records(
    compression: Compression,
    record_data: impl BufRead + 'static,
    record_count: usize,
    inflated_limit: usize,
    decoded_limit: usize,
) -> Result<Vec<Record>> {
    let mut inflated = Vec::new();

    _ = compression
        .inflator(record_data, inflated_limit)?
        .read_to_end(&mut inflated)?;

    // The batch holds the inflated bytes in memory, so the budget counts them
    // before any record.
    let mut budget = DecodeBudget::new(decoded_limit);
    budget.charge(inflated.len())?;

    decode_records(Bytes::from(inflated), record_count, &mut budget)
}

/// Decodes `record_count` records from the front of `record_data`, and charges
/// the structures of each record to `budget`.
///
/// Each key and value is a slice of `record_data`, not a copy. A batch with
/// bytes left after its last record is corrupt, as in Kafka's
/// [`DefaultRecordBatch`].
///
/// [`DefaultRecordBatch`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L605-L606
fn decode_records(
    mut record_data: Bytes,
    record_count: usize,
    budget: &mut DecodeBudget,
) -> Result<Vec<Record>> {
    let mut records = Vec::with_capacity(record_count.min(MAX_PREALLOCATED_ELEMENTS));

    for _ in 0..record_count {
        let record = Record::decode(&mut record_data)?;
        budget.charge(record.allocated_size())?;
        records.push(record);
    }

    if !record_data.is_empty() {
        return Err(Error::Overflow);
    }

    Ok(records)
}

impl TryFrom<&Batch> for Vec<Record> {
    type Error = Error;

    fn try_from(batch: &Batch) -> Result<Self, Self::Error> {
        // This delegates so that both conversions decode alike. `record_data`
        // is a `Bytes`, so the clone shares its buffer.
        Self::try_from(batch.clone())
    }
}

const FIXED_BATCH_LENGTH: usize =
    // partition leader epoch
    size_of::<i32>()
    // magic
    + size_of::<i8>()
    // CRC
    + size_of::<u32>()
    // attributes
    + size_of::<i16>()
    // last_offset_delta
    + size_of::<i32>()
    // base timestamp
    + size_of::<i64>()
    // max timestamp
    + size_of::<i64>()
    // producer id
    + size_of::<i64>()
    // producer epoch
    + size_of::<i16>()
    // base sequence
    + size_of::<i32>()
    // record count
    + size_of::<u32>();

/// The length of the fixed fields that the CRC covers: `attributes` through
/// `record_count`, the part of [`FIXED_BATCH_LENGTH`] after the CRC itself.
const FIXED_CRC_HEADER_LENGTH: usize =
    FIXED_BATCH_LENGTH - size_of::<i32>() - size_of::<i8>() - size_of::<u32>();

impl<'de> Deserialize<'de> for Batch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = Batch;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(stringify!(Batch))
            }

            fn visit_byte_buf<E>(self, v: Vec<u8>) -> result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                debug!(len = v.len());
                Batch::try_from(Bytes::from(v)).map_err(|err| de::Error::custom(err.to_string()))
            }
        }

        deserializer.deserialize_byte_buf(V)
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        BatchAttribute, ControlBatch, EndTransactionMarker,
        de::BatchDecoder,
        primitive::varint::{LongVarInt, VarInt},
        record::{Header, inflated},
    };

    use super::*;

    use std::assert_matches;

    use tracing::subscriber::DefaultGuard;

    #[cfg(miri)]
    fn init_tracing() -> Result<()> {
        Ok(())
    }

    #[cfg(not(miri))]
    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        use tracing_subscriber::fmt::format::FmtSpan;

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_max_level(tracing::Level::DEBUG)
                .with_span_events(FmtSpan::ACTIVE)
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    const LOREM: &[u8] = b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do \
    eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad \
    minim veniam, quis nostrud exercitation ullamco laboris nisi ut \
    aliquip ex ea commodo consequat. Duis aute irure dolor in \
    reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla \
    pariatur. Excepteur sint occaecat cupidatat non proident, sunt in \
    culpa qui officia deserunt mollit anim id est laborum.";

    /// One record whose value alone is a byte over the decoded limit, deflated
    /// with `compression`. Compresses to almost nothing for every codec, so
    /// the only thing that can reject it is the limit on the decoded side.
    fn batch_just_over_the_decoded_limit(compression: Compression) -> Result<Batch> {
        let mut inflated = inflated::Batch {
            records: vec![Record {
                value: Some(Bytes::from(vec![0u8; MAX_DECODED_BATCH_BYTES + 1])),
                ..Default::default()
            }],
            ..Default::default()
        };

        inflated.attributes = BatchAttribute::try_from(inflated.attributes)
            .map(|attribute| attribute.compression(compression).into())?;

        Batch::try_from(inflated)
    }

    /// Both decode paths must reject the batch with the typed error, not a
    /// generic I/O failure: that is what lets the broker answer
    /// `MESSAGE_TOO_LARGE` rather than `UNKNOWN_SERVER_ERROR`.
    fn assert_rejected_by_reference(batch: &Batch) {
        let err = Vec::<Record>::try_from(batch)
            .expect_err("a batch over the decoded limit must be rejected (by reference)");
        assert!(
            matches!(err, Error::MessageMaxSizeExceeded(_)),
            "by reference: {err:?}"
        );
    }

    fn assert_rejected_by_value(batch: Batch) {
        let err = Vec::<Record>::try_from(batch)
            .expect_err("a batch over the decoded limit must be rejected (by value)");
        assert!(
            matches!(err, Error::MessageMaxSizeExceeded(_)),
            "by value: {err:?}"
        );
    }

    /// `LimitedRead` on its own, through the record decode both `TryFrom`
    /// impls share. The `DecodeBudget` charges at least one byte for every
    /// byte the decoder reads, so at equal limits it always trips first and a
    /// test at `MAX_DECODED_BATCH_BYTES` cannot tell whether `LimitedRead` is
    /// there. Here the budget is unlimited, so only `LimitedRead` can reject
    /// the batch, and its error (`limit + 1`) must come through typed.
    #[test]
    fn limited_read_rejects_a_batch_the_budget_would_allow() -> Result<()> {
        const VALUE_LEN: usize = 64 * 1024;

        for compression in [Compression::Gzip, Compression::Lz4, Compression::Zstd] {
            let mut inflated = inflated::Batch {
                records: vec![Record {
                    value: Some(Bytes::from(vec![0u8; VALUE_LEN])),
                    ..Default::default()
                }],
                ..Default::default()
            };

            inflated.attributes = BatchAttribute::try_from(inflated.attributes)
                .map(|attribute| attribute.compression(compression.clone()).into())?;

            let batch = Batch::try_from(inflated)?;
            let record_count = usize::try_from(batch.record_count)?;

            // Control: with room for the whole record, it decodes.
            let records = inflate_records(
                compression.clone(),
                batch.record_data.clone().reader(),
                record_count,
                2 * VALUE_LEN,
                usize::MAX,
            )?;
            assert_eq!(Some(VALUE_LEN), records[0].value.as_ref().map(Bytes::len));

            // A decompressed-byte limit inside the value is rejected by
            // LimitedRead alone.
            let limit = VALUE_LEN / 2;
            let err = inflate_records(
                compression.clone(),
                batch.record_data.clone().reader(),
                record_count,
                limit,
                usize::MAX,
            )
            .expect_err("a batch over the decompressed-byte limit must be rejected");

            assert!(
                matches!(err, Error::MessageMaxSizeExceeded(size) if size == limit + 1),
                "{compression:?}: {err:?}"
            );
        }

        Ok(())
    }

    #[test]
    fn batch_just_over_the_decoded_limit_is_rejected_snappy_raw() -> Result<()> {
        // Rejected on the block's claimed length before any decompression,
        // so both paths are cheap enough for one test.
        let batch = batch_just_over_the_decoded_limit(Compression::Snappy)?;
        assert_rejected_by_reference(&batch);
        assert_rejected_by_value(batch);

        Ok(())
    }

    #[test]
    fn batch_just_over_the_decoded_limit_is_rejected_snappy_xerial() -> Result<()> {
        let mut batch = batch_just_over_the_decoded_limit(Compression::Snappy)?;

        // Wrap the raw block in xerial framing: magic, version, compatible
        // version, block size, then the block, the form snappy-java produces.
        let block = batch.record_data.clone();
        let mut framed = BytesMut::new();
        framed.put_slice(b"\x82SNAPPY\0");
        framed.put_i32(1);
        framed.put_i32(1);
        framed.put_i32(i32::try_from(block.len())?);
        framed.put(block);
        batch.record_data = framed.freeze();

        assert_rejected_by_reference(&batch);
        assert_rejected_by_value(batch);

        Ok(())
    }

    #[test]
    fn record_count_beyond_decompressed_limit_is_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = Batch {
            attributes: BatchAttribute::default()
                .compression(Compression::Zstd)
                .into(),
            record_count: u32::MAX,
            record_data: Bytes::new(),
            ..Default::default()
        };

        let err = Vec::<Record>::try_from(batch.clone())
            .expect_err("a record_count this large must be rejected before decompressing");
        assert!(matches!(err, Error::MessageMaxSizeExceeded(_)));

        let err = Vec::<Record>::try_from(&batch)
            .expect_err("a record_count this large must be rejected before decompressing");
        assert!(matches!(err, Error::MessageMaxSizeExceeded(_)));

        Ok(())
    }

    #[test]
    fn record_count_bound_reflects_decoded_struct_size_not_minimum_wire_size() -> Result<()> {
        let _guard = init_tracing()?;

        // A record_count bounded only by the minimum *encoded* record size (7
        // bytes) does not bound the memory actually allocated: each record
        // costs size_of::<Record>() (112 bytes) once decoded into a
        // Vec<Record>, regardless of how little of that came from the wire.
        // 1,000,000 is comfortably under the old (wrong) bound
        // (MAX_DECODED_BATCH_BYTES / 7 ~= 14,979,656) but must be
        // rejected under the correct one
        // (MAX_DECODED_BATCH_BYTES / size_of::<Record>() ~= 936,864),
        // since 1,000,000 * size_of::<Record>() alone is already ~105 MiB.
        assert!(
            size_of::<Record>() > 7,
            "test assumes the struct outgrew the minimum wire size"
        );
        let record_count = 1_000_000;
        assert!(record_count < MAX_DECODED_BATCH_BYTES / 7);
        assert!(record_count > MAX_DECODED_BATCH_BYTES / size_of::<Record>());

        let batch = Batch {
            attributes: BatchAttribute::default()
                .compression(Compression::Zstd)
                .into(),
            record_count: u32::try_from(record_count)?,
            record_data: Bytes::new(),
            ..Default::default()
        };

        let err = Vec::<Record>::try_from(batch).expect_err(
            "a record_count whose decoded struct cost alone exceeds the limit must be rejected",
        );
        assert!(matches!(err, Error::MessageMaxSizeExceeded(_)));

        Ok(())
    }

    #[test]
    fn batch_just_under_the_decompressed_limit_round_trips() -> Result<()> {
        // Deliberately no init_tracing() here: with it on, `debug!(?...)`
        // calls elsewhere in this path fully Debug-format the ~100 MiB
        // buffer on every call, which is not a cost real production tracing
        // would pay (nobody runs with DEBUG-level + ACTIVE span events in
        // production) but turns this one test from sub-second to ~30
        // minutes — comfortably over the CI nextest profile's 5 minute
        // per-test timeout.

        // Comfortable headroom under the limit for the record's other
        // fields (length/attributes/timestamp/offset/headers overhead), so
        // this stays a "just under the limit" case rather than brushing
        // against it.
        let size = MAX_DECODED_BATCH_BYTES - 1024 * 1024;
        let value = Bytes::from(vec![0u8; size]);

        let mut inflated = inflated::Batch {
            records: vec![Record {
                value: Some(value.clone()),
                ..Default::default()
            }],
            ..Default::default()
        };

        inflated.attributes = BatchAttribute::try_from(inflated.attributes)
            .map(|attribute| attribute.compression(Compression::Zstd).into())?;

        let deflated = Batch::try_from(inflated)?;
        let records: Vec<Record> = deflated.try_into()?;

        assert_eq!(1, records.len());
        assert_eq!(Some(value), records[0].value);

        Ok(())
    }

    #[test]
    fn decode_gzip() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = &[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 89, 0, 0, 0, 0, 2, 198, 48, 56, 83, 0, 1, 0, 0, 0, 0,
            0, 0, 1, 145, 183, 231, 239, 158, 0, 0, 1, 145, 183, 231, 239, 158, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 31, 139, 8, 0, 0, 0, 0,
            0, 0, 19, 53, 144, 205, 81, 67, 49, 12, 132, 31, 23, 104, 99, 11, 200, 188, 42, 224,
            198, 149, 2, 132, 172, 4, 205, 248, 47, 182, 148, 73, 9, 169, 153, 19, 50, 15, 110,
            150, 37, 173, 118, 191, 199, 203, 182, 109, 79, 223, 207, 239, 109, 72, 129, 246, 233,
            5, 169, 229, 54, 48, 213, 64, 69, 236, 4, 110, 117, 10, 155, 152, 15, 80, 210, 174,
            147, 181, 94, 32, 89, 163, 57, 37, 197, 2, 68, 125, 150, 150, 96, 82, 122, 44, 107,
            101, 77, 154, 188, 26, 220, 144, 233, 51, 228, 33, 118, 72, 11, 10, 93, 42, 129, 178,
            94, 157, 118, 124, 24, 164, 106, 9, 109, 20, 93, 143, 91, 148, 84, 78, 184, 186, 78,
            212, 54, 109, 120, 130, 220, 101, 176, 26, 153, 182, 10, 207, 153, 10, 183, 67, 121,
            13, 233, 212, 117, 233, 87, 82, 123, 12, 67, 40, 140, 151, 240, 212, 142, 0, 113, 202,
            118, 188, 46, 73, 114, 19, 232, 240, 112, 114, 100, 213, 138, 33, 125, 200, 151, 212,
            36, 35, 130, 199, 199, 173, 101, 239, 113, 78, 194, 78, 36, 133, 204, 41, 96, 205, 249,
            159, 80, 4, 114, 156, 253, 162, 100, 168, 203, 16, 58, 141, 40, 124, 236, 120, 187,
            179, 116, 19, 95, 24, 131, 65, 99, 38, 225, 152, 99, 239, 154, 200, 214, 70, 164, 232,
            163, 105, 146, 186, 40, 46, 82, 113, 148, 61, 119, 90, 185, 209, 206, 103, 101, 37, 36,
            153, 50, 86, 183, 180, 188, 108, 208, 2, 164, 129, 99, 254, 113, 245, 178, 111, 63,
            143, 62, 223, 101, 198, 1, 0, 0, 0, 0, 0,
        ];

        let decoder = BatchDecoder::new(Bytes::from_static(encoded));
        let decoded = Batch::deserialize(decoder)?;

        // The fixture's CRC is the one its encoder wrote, so this pins the
        // range `computed_crc` covers to the bytes a client sends.
        assert_eq!(decoded.crc, decoded.computed_crc());

        assert_eq!(
            Compression::Gzip,
            Compression::try_from(decoded.attributes)?
        );

        let mut inflated = crate::record::inflated::Batch::try_from(decoded.clone())
            .inspect(|inflated| debug!(?inflated))?;

        assert_eq!(
            Compression::None,
            Compression::try_from(inflated.attributes)?
        );

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            inflated.records
        );

        inflated.attributes = BatchAttribute::try_from(inflated.attributes)
            .map(|attribute| attribute.compression(Compression::Gzip).into())?;

        let deflated = Batch::try_from(inflated)?;
        assert_eq!(decoded.base_offset, deflated.base_offset);
        assert_eq!(
            decoded.partition_leader_epoch,
            deflated.partition_leader_epoch
        );
        assert_eq!(decoded.magic, deflated.magic);
        assert_eq!(decoded.attributes, deflated.attributes);
        assert_eq!(decoded.last_offset_delta, deflated.last_offset_delta);
        assert_eq!(decoded.base_timestamp, deflated.base_timestamp);

        let records: Vec<Record> = deflated.try_into()?;

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            records
        );

        Ok(())
    }

    #[test]
    fn decode_zstd() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = &[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 91, 0, 0, 0, 0, 2, 200, 21, 172, 244, 0, 4, 0, 0, 0,
            0, 0, 0, 1, 145, 183, 250, 201, 221, 0, 0, 1, 145, 183, 250, 201, 221, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 40, 181, 47, 253, 0,
            88, 13, 9, 0, 70, 217, 64, 36, 160, 37, 73, 7, 255, 255, 255, 255, 143, 174, 211, 102,
            147, 114, 239, 182, 165, 188, 148, 244, 91, 75, 123, 39, 146, 211, 241, 3, 167, 201,
            64, 245, 234, 24, 45, 9, 56, 0, 53, 0, 53, 0, 197, 44, 90, 146, 147, 15, 123, 209, 29,
            99, 44, 57, 147, 242, 238, 145, 18, 167, 14, 240, 197, 53, 216, 71, 250, 57, 169, 162,
            68, 227, 112, 178, 27, 29, 160, 77, 21, 159, 138, 174, 28, 169, 201, 116, 94, 99, 116,
            4, 0, 36, 40, 40, 138, 34, 0, 130, 229, 38, 220, 115, 204, 62, 221, 154, 126, 195, 84,
            41, 89, 187, 99, 225, 7, 114, 194, 37, 137, 48, 157, 53, 61, 15, 29, 152, 186, 25, 81,
            115, 187, 41, 169, 154, 155, 139, 5, 74, 168, 60, 188, 84, 203, 12, 101, 106, 116, 141,
            206, 64, 149, 40, 177, 142, 234, 180, 74, 73, 43, 214, 13, 89, 104, 186, 46, 229, 163,
            78, 201, 197, 23, 35, 106, 44, 60, 89, 14, 81, 123, 241, 200, 196, 124, 192, 232, 24,
            213, 94, 163, 68, 175, 49, 171, 233, 41, 160, 4, 172, 60, 28, 57, 99, 110, 52, 5, 193,
            100, 114, 201, 107, 110, 85, 124, 41, 103, 138, 150, 200, 201, 164, 245, 241, 66, 225,
            250, 60, 180, 78, 201, 97, 153, 58, 64, 51, 249, 161, 83, 59, 43, 22, 22, 249, 201, 81,
            172, 225, 26, 227, 138, 246, 84, 148, 198, 227, 145, 24, 80, 66, 8, 0, 220, 218, 44,
            15, 30, 45, 186, 100, 95, 73, 49, 124, 17, 109, 0, 43, 70, 43, 93, 140, 227, 122, 67,
            136, 2, 0, 0, 0,
        ];

        let decoder = BatchDecoder::new(Bytes::from_static(encoded));
        let decoded = Batch::deserialize(decoder)?;

        // The fixture's CRC is the one its encoder wrote, so this pins the
        // range `computed_crc` covers to the bytes a client sends.
        assert_eq!(decoded.crc, decoded.computed_crc());

        assert_eq!(
            Compression::Zstd,
            Compression::try_from(decoded.attributes)?
        );

        let mut inflated = crate::record::inflated::Batch::try_from(decoded.clone())?;

        assert_eq!(
            Compression::None,
            Compression::try_from(inflated.attributes)?
        );

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            inflated.records
        );

        inflated.attributes = BatchAttribute::try_from(inflated.attributes)
            .map(|attribute| attribute.compression(Compression::Zstd).into())?;

        let deflated = Batch::try_from(inflated)?;
        assert_eq!(decoded.base_offset, deflated.base_offset);
        assert_eq!(
            decoded.partition_leader_epoch,
            deflated.partition_leader_epoch
        );
        assert_eq!(decoded.magic, deflated.magic);
        assert_eq!(decoded.attributes, deflated.attributes);
        assert_eq!(decoded.last_offset_delta, deflated.last_offset_delta);
        assert_eq!(decoded.base_timestamp, deflated.base_timestamp);

        let records: Vec<Record> = deflated.try_into()?;

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            records
        );

        Ok(())
    }

    #[test]
    fn decode_lz4() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = &[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 237, 0, 0, 0, 0, 2, 43, 216, 167, 237, 0, 3, 0, 0, 0,
            0, 0, 0, 1, 145, 184, 77, 37, 242, 0, 0, 1, 145, 184, 77, 37, 242, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 4, 34, 77, 24, 96, 64,
            130, 173, 1, 0, 0, 242, 95, 136, 7, 0, 0, 0, 1, 250, 6, 76, 111, 114, 101, 109, 32,
            105, 112, 115, 117, 109, 32, 100, 111, 108, 111, 114, 32, 115, 105, 116, 32, 97, 109,
            101, 116, 44, 32, 99, 111, 110, 115, 101, 99, 116, 101, 116, 117, 114, 32, 97, 100,
            105, 112, 105, 115, 99, 105, 110, 103, 32, 101, 108, 105, 116, 44, 32, 115, 101, 100,
            32, 100, 111, 32, 101, 105, 117, 115, 109, 111, 100, 32, 116, 101, 109, 112, 111, 114,
            32, 105, 110, 99, 105, 100, 105, 100, 117, 110, 116, 32, 117, 116, 32, 108, 97, 98,
            111, 114, 101, 32, 101, 116, 91, 0, 240, 14, 101, 32, 109, 97, 103, 110, 97, 32, 97,
            108, 105, 113, 117, 97, 46, 32, 85, 116, 32, 101, 110, 105, 109, 32, 97, 100, 32, 109,
            105, 9, 0, 242, 26, 118, 101, 110, 105, 97, 109, 44, 32, 113, 117, 105, 115, 32, 110,
            111, 115, 116, 114, 117, 100, 32, 101, 120, 101, 114, 99, 105, 116, 97, 116, 105, 111,
            110, 32, 117, 108, 108, 97, 109, 99, 111, 90, 0, 0, 37, 0, 98, 105, 115, 105, 32, 117,
            116, 83, 0, 242, 1, 105, 112, 32, 101, 120, 32, 101, 97, 32, 99, 111, 109, 109, 111,
            100, 111, 193, 0, 112, 113, 117, 97, 116, 46, 32, 68, 83, 0, 162, 97, 117, 116, 101,
            32, 105, 114, 117, 114, 101, 145, 0, 240, 2, 32, 105, 110, 32, 114, 101, 112, 114, 101,
            104, 101, 110, 100, 101, 114, 105, 116, 17, 0, 176, 118, 111, 108, 117, 112, 116, 97,
            116, 101, 32, 118, 234, 0, 164, 32, 101, 115, 115, 101, 32, 99, 105, 108, 108, 34, 1,
            208, 101, 32, 101, 117, 32, 102, 117, 103, 105, 97, 116, 32, 110, 145, 0, 240, 4, 32,
            112, 97, 114, 105, 97, 116, 117, 114, 46, 32, 69, 120, 99, 101, 112, 116, 101, 117, 71,
            1, 240, 4, 110, 116, 32, 111, 99, 99, 97, 101, 99, 97, 116, 32, 99, 117, 112, 105, 100,
            97, 116, 50, 0, 160, 111, 110, 32, 112, 114, 111, 105, 100, 101, 110, 70, 1, 0, 42, 1,
            128, 105, 110, 32, 99, 117, 108, 112, 97, 248, 0, 224, 32, 111, 102, 102, 105, 99, 105,
            97, 32, 100, 101, 115, 101, 114, 30, 0, 64, 109, 111, 108, 108, 147, 1, 0, 33, 1, 240,
            1, 105, 100, 32, 101, 115, 116, 32, 108, 97, 98, 111, 114, 117, 109, 46, 0, 0, 0, 0, 0,
            0, 0, 0,
        ];

        let decoder = BatchDecoder::new(Bytes::from_static(encoded));
        let decoded = Batch::deserialize(decoder)?;

        // The fixture's CRC is the one its encoder wrote, so this pins the
        // range `computed_crc` covers to the bytes a client sends.
        assert_eq!(decoded.crc, decoded.computed_crc());
        assert_eq!(Compression::Lz4, Compression::try_from(decoded.attributes)?);

        let mut inflated = crate::record::inflated::Batch::try_from(decoded.clone())?;

        assert_eq!(
            Compression::None,
            Compression::try_from(inflated.attributes)?
        );

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            inflated.records
        );

        inflated.attributes = BatchAttribute::try_from(inflated.attributes)
            .map(|attribute| attribute.compression(Compression::Lz4).into())?;

        let deflated = Batch::try_from(inflated)?;
        assert_eq!(decoded.base_offset, deflated.base_offset);
        assert_eq!(
            decoded.partition_leader_epoch,
            deflated.partition_leader_epoch
        );
        assert_eq!(decoded.magic, deflated.magic);
        assert_eq!(decoded.attributes, deflated.attributes);
        assert_eq!(decoded.last_offset_delta, deflated.last_offset_delta);
        assert_eq!(decoded.base_timestamp, deflated.base_timestamp);

        let records: Vec<Record> = deflated.try_into()?;

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            records
        );

        Ok(())
    }

    #[test]
    fn decode_snappy() -> Result<()> {
        let _guard = init_tracing()?;

        let encoded = &[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 218, 0, 0, 0, 0, 2, 228, 189, 111, 249, 0, 2, 0, 0, 0,
            0, 0, 0, 1, 145, 184, 92, 90, 192, 0, 0, 1, 145, 184, 92, 90, 192, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 198, 3, 240, 111, 136, 7,
            0, 0, 0, 1, 250, 6, 76, 111, 114, 101, 109, 32, 105, 112, 115, 117, 109, 32, 100, 111,
            108, 111, 114, 32, 115, 105, 116, 32, 97, 109, 101, 116, 44, 32, 99, 111, 110, 115,
            101, 99, 116, 101, 116, 117, 114, 32, 97, 100, 105, 112, 105, 115, 99, 105, 110, 103,
            32, 101, 108, 105, 116, 44, 32, 115, 101, 100, 32, 100, 111, 32, 101, 105, 117, 115,
            109, 111, 100, 32, 116, 101, 109, 112, 111, 114, 32, 105, 110, 99, 105, 100, 105, 100,
            117, 110, 116, 32, 117, 116, 32, 108, 97, 98, 111, 114, 101, 32, 101, 116, 32, 100, 1,
            91, 112, 101, 32, 109, 97, 103, 110, 97, 32, 97, 108, 105, 113, 117, 97, 46, 32, 85,
            116, 32, 101, 110, 105, 109, 32, 97, 100, 32, 109, 105, 1, 9, 160, 118, 101, 110, 105,
            97, 109, 44, 32, 113, 117, 105, 115, 32, 110, 111, 115, 116, 114, 117, 100, 32, 101,
            120, 101, 114, 99, 105, 116, 97, 116, 105, 111, 110, 32, 117, 108, 108, 97, 109, 99,
            111, 9, 90, 1, 37, 8, 105, 115, 105, 1, 106, 5, 83, 60, 105, 112, 32, 101, 120, 32,
            101, 97, 32, 99, 111, 109, 109, 111, 100, 111, 9, 193, 24, 113, 117, 97, 116, 46, 32,
            68, 1, 83, 36, 97, 117, 116, 101, 32, 105, 114, 117, 114, 101, 13, 236, 60, 105, 110,
            32, 114, 101, 112, 114, 101, 104, 101, 110, 100, 101, 114, 105, 116, 1, 17, 40, 118,
            111, 108, 117, 112, 116, 97, 116, 101, 32, 118, 1, 234, 36, 32, 101, 115, 115, 101, 32,
            99, 105, 108, 108, 49, 34, 232, 101, 32, 101, 117, 32, 102, 117, 103, 105, 97, 116, 32,
            110, 117, 108, 108, 97, 32, 112, 97, 114, 105, 97, 116, 117, 114, 46, 32, 69, 120, 99,
            101, 112, 116, 101, 117, 114, 32, 115, 105, 110, 116, 32, 111, 99, 99, 97, 101, 99, 97,
            116, 32, 99, 117, 112, 105, 100, 97, 116, 1, 50, 60, 111, 110, 32, 112, 114, 111, 105,
            100, 101, 110, 116, 44, 32, 115, 117, 110, 5, 117, 88, 99, 117, 108, 112, 97, 32, 113,
            117, 105, 32, 111, 102, 102, 105, 99, 105, 97, 32, 100, 101, 115, 101, 114, 1, 30, 12,
            109, 111, 108, 108, 33, 147, 33, 33, 60, 105, 100, 32, 101, 115, 116, 32, 108, 97, 98,
            111, 114, 117, 109, 46, 0, 0, 0, 0,
        ];

        let decoder = BatchDecoder::new(Bytes::from_static(encoded));
        let decoded = Batch::deserialize(decoder)?;

        // The fixture's CRC is the one its encoder wrote, so this pins the
        // range `computed_crc` covers to the bytes a client sends.
        assert_eq!(decoded.crc, decoded.computed_crc());
        assert_eq!(
            Compression::Snappy,
            Compression::try_from(decoded.attributes)?
        );

        let records: Vec<Record> = decoded.try_into()?;

        assert_eq!(
            vec![Record {
                length: 452,
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: None,
                value: Some(Bytes::from_static(LOREM)),
                headers: [].into()
            }],
            records
        );

        Ok(())
    }

    /// An inflated batch marked snappy deflates to a raw snappy block that
    /// both the batch's own decoder and [`Compression::inflator`] accept.
    #[test]
    fn deflate_snappy_round_trip() -> Result<()> {
        let _guard = init_tracing()?;

        let values = [
            Bytes::from_static(LOREM),
            Bytes::from_static(b"foobarfoobarfoobarfoobar"),
            Bytes::from_static(&[0u8; 8_192]),
        ];

        let mut builder = inflated::Batch::builder()
            .attributes(
                BatchAttribute::default()
                    .compression(Compression::Snappy)
                    .into(),
            )
            .last_offset_delta(values.len() as i32 - 1);

        for (delta, value) in values.iter().enumerate() {
            builder = builder.record(
                Record::builder()
                    .offset_delta(delta as i32)
                    .value(Some(value.clone())),
            );
        }

        let deflated = builder.build().and_then(Batch::try_from)?;

        assert_eq!(
            Compression::Snappy,
            Compression::try_from(deflated.attributes)?
        );
        assert_eq!(values.len() as u32, deflated.record_count);
        assert!(
            deflated.record_data.len() < values.iter().map(Bytes::len).sum::<usize>(),
            "record data is not compressed"
        );

        let records: Vec<Record> = (&deflated).try_into()?;
        assert_eq!(
            values.to_vec(),
            records.iter().filter_map(Record::value).collect::<Vec<_>>()
        );

        let records: Vec<Record> = deflated.try_into()?;
        assert_eq!(
            values.to_vec(),
            records.iter().filter_map(Record::value).collect::<Vec<_>>()
        );

        Ok(())
    }

    #[test]
    pub fn is_transactional() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = Batch {
            base_offset: 0,
            batch_length: 68,
            partition_leader_epoch: 0,
            magic: 2,
            crc: 3650210183,
            attributes: 16,
            last_offset_delta: 0,
            base_timestamp: 1729509915759,
            max_timestamp: 1729509915759,
            producer_id: 5,
            producer_epoch: 0,
            base_sequence: 0,
            record_count: 1,
            record_data: Bytes::from_static(b"$\0\0\0\x08\0\0\0\0\x10test0-ok\0"),
        };

        assert!(batch.is_transactional());

        Ok(())
    }

    #[test]
    pub fn is_transactional_control() -> Result<()> {
        use crate::record::inflated;

        let _guard = init_tracing()?;

        let deflated = Batch {
            base_offset: 1,
            batch_length: 66,
            partition_leader_epoch: 0,
            magic: 2,
            crc: 820655041,
            attributes: 48,
            last_offset_delta: 0,
            base_timestamp: 1729509916024,
            max_timestamp: 1729509916024,
            producer_id: 5,
            producer_epoch: 0,
            base_sequence: -1,
            record_count: 1,
            record_data: Bytes::from_static(b" \0\0\0\x08\0\0\0\x01\x0c\0\0\0\0\0\0\0"),
        };

        assert!(deflated.is_transactional());
        assert!(deflated.is_control());

        let inflated = inflated::Batch::try_from(deflated)?;

        assert_eq!(1, inflated.records.len());
        assert_eq!(
            Some(Bytes::from_static(b"\0\0\0\x01")),
            inflated.records[0].key
        );

        let control_batch = ControlBatch::try_from(inflated.records[0].clone().key().unwrap())?;
        assert_eq!(0, control_batch.version);
        assert!(control_batch.is_commit());
        assert!(!control_batch.is_abort());

        assert_eq!(
            Some(Bytes::from_static(b"\0\0\0\0\0\0")),
            inflated.records[0].value
        );

        let txn_marker =
            EndTransactionMarker::try_from(inflated.records[0].clone().value.unwrap())?;
        assert_eq!(0, txn_marker.version);
        assert_eq!(0, txn_marker.coordinator_epoch);

        Ok(())
    }

    #[test]
    fn deflate() -> Result<()> {
        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");
        let value = Bytes::from_static(b"consectetur adipiscing elit");

        let producer_id = 54345;
        let producer_epoch = 32123;
        let base_sequence = 78987;
        let base_offset = 9876789;
        let attributes: i16 = BatchAttribute::default().transaction(true).into();

        let batch: Batch = inflated::Batch::builder()
            .record(
                Record::builder()
                    .key(key.clone().into())
                    .value(value.clone().into()),
            )
            .attributes(attributes)
            .producer_id(producer_id)
            .producer_epoch(producer_epoch)
            .base_offset(base_offset)
            .base_sequence(base_sequence)
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        assert_eq!(base_sequence, batch.base_sequence);
        assert_eq!(producer_id, batch.producer_id);
        assert_eq!(producer_epoch, batch.producer_epoch);
        assert_eq!(attributes, batch.attributes);
        assert_eq!(1, batch.record_count);
        assert_eq!(base_offset, batch.base_offset);

        Ok(())
    }

    #[test]
    fn encode_decode() -> Result<()> {
        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");
        let value = Bytes::from_static(b"consectetur adipiscing elit");

        let producer_id = 54345;
        let producer_epoch = 32123;
        let base_sequence = 78987;
        let base_offset = 9876789;
        let attributes: i16 = BatchAttribute::default().transaction(true).into();

        let batch: Batch = inflated::Batch::builder()
            .record(
                Record::builder()
                    .key(key.clone().into())
                    .value(value.clone().into()),
            )
            .attributes(attributes)
            .producer_id(producer_id)
            .producer_epoch(producer_epoch)
            .base_offset(base_offset)
            .base_sequence(base_sequence)
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        let expected = batch.batch_length as usize;

        let encoded = Bytes::from(batch);

        let deflated = Batch::try_from(encoded)?;

        assert_eq!(deflated.producer_id, producer_id);
        assert_eq!(deflated.producer_epoch, producer_epoch);
        assert_eq!(deflated.base_sequence, base_sequence);
        assert_eq!(deflated.base_offset, base_offset);
        assert_eq!(deflated.attributes, attributes);

        let inflated = inflated::Batch::try_from(deflated)?;
        assert_eq!(1, inflated.records.len());

        assert_eq!(Some(key), inflated.records[0].key);
        assert_eq!(Some(value), inflated.records[0].value);

        Ok(())
    }

    /// A `record_count` taken straight off the wire, with no `record_data`
    /// to back it, must not succeed in preallocating hundreds of GB of
    /// `Record`s before ever inspecting the data. Whether an
    /// allocation that large is rejected by the allocator is platform
    /// dependent; what must hold everywhere is that this returns `Err`
    /// rather than `Ok` with a nonsensical result.
    ///
    /// Before the fix this also returned `Err` whenever the reservation
    /// succeeded, because decoding the first record from empty data fails.
    /// So on macOS, or on Linux with enough overcommit headroom, it passes
    /// with or without the fix, and catches a regression only by aborting
    /// the test process. A green local run is not proof the cap is in place.
    #[test]
    fn record_count_near_u32_max_with_no_data_returns_err() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = Batch {
            base_offset: 0,
            batch_length: 0,
            partition_leader_epoch: 0,
            magic: 2,
            crc: 0,
            attributes: 0,
            last_offset_delta: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            record_count: u32::MAX,
            record_data: Bytes::new(),
        };

        assert!(Vec::<Record>::try_from(batch.clone()).is_err());
        assert!(Vec::<Record>::try_from(&batch).is_err());

        Ok(())
    }

    /// Same as above, but for the compressed branch, where the inflator
    /// itself never rejects the (empty) input up front.
    #[test]
    fn record_count_near_u32_max_with_empty_compressed_data_returns_err() -> Result<()> {
        let _guard = init_tracing()?;

        let attributes: i16 = BatchAttribute::default()
            .compression(Compression::Gzip)
            .into();

        let batch = Batch {
            base_offset: 0,
            batch_length: 0,
            partition_leader_epoch: 0,
            magic: 2,
            crc: 0,
            attributes,
            last_offset_delta: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            record_count: u32::MAX,
            record_data: Bytes::new(),
        };

        assert!(Vec::<Record>::try_from(batch.clone()).is_err());
        assert!(Vec::<Record>::try_from(&batch).is_err());

        Ok(())
    }

    /// One record whose header count is `i32::MAX`, with no header bytes
    /// behind it.
    fn record_data_with_header_count(header_count: i32) -> Result<Bytes> {
        let mut body = BytesMut::new();
        body.put_u8(0); // attributes
        body.put(LongVarInt(0).encode()?); // timestamp delta
        body.put(VarInt(0).encode()?); // offset delta
        body.put(VarInt(-1).encode()?); // null key
        body.put(VarInt(-1).encode()?); // null value
        body.put(VarInt(header_count).encode()?);

        let mut record = BytesMut::new();
        record.put(VarInt(i32::try_from(body.len())?).encode()?);
        record.put(body);
        Ok(record.freeze())
    }

    fn batch_with_header_count(compression: Compression) -> Result<Batch> {
        let uncompressed = record_data_with_header_count(i32::MAX)?;

        let record_data = if compression == Compression::None {
            uncompressed
        } else {
            let mut gz = GzEncoder::new(BytesMut::new().writer(), flate2::Compression::default());
            gz.write_all(&uncompressed)?;
            gz.finish().map(|w| w.into_inner().freeze())?
        };

        Ok(Batch {
            base_offset: 0,
            batch_length: 0,
            partition_leader_epoch: 0,
            magic: 2,
            crc: 0,
            attributes: BatchAttribute::default().compression(compression).into(),
            last_offset_delta: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            record_count: 1,
            record_data,
        })
    }

    /// A header count of `i32::MAX` on one record asked for about 137 GB
    /// before the fix. The uncompressed batch by value goes through
    /// `VarIntSequence::decode`; the other three calls go through the serde
    /// path in `VarIntSequence::deserialize`, so together they pin both caps.
    /// Like the `record_count` tests above, a regression shows up as an
    /// allocation abort on a platform that refuses the reservation.
    #[test]
    fn header_count_near_i32_max_with_no_header_data_returns_err() -> Result<()> {
        let _guard = init_tracing()?;

        for compression in [Compression::None, Compression::Gzip] {
            let batch = batch_with_header_count(compression)?;

            assert!(Vec::<Record>::try_from(batch.clone()).is_err());
            assert!(Vec::<Record>::try_from(&batch).is_err());
        }

        Ok(())
    }

    /// A compressed record whose `length` varint has a sixth byte, one more
    /// than an `i32` can hold, is rejected by every conversion that decodes
    /// compressed records. The record is otherwise valid, so only the
    /// overlong varint can reject it: a record decoder that wrapped the value
    /// would decode this batch.
    #[test]
    fn overlong_varint_in_a_compressed_record_is_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let mut record = BytesMut::new();
        record.put_slice(&[0xff, 0xff, 0xff, 0xff, 0xff, 0x01]); // length, overlong
        record.put_u8(0); // attributes
        record.put(LongVarInt(0).encode()?); // timestamp delta
        record.put(VarInt(0).encode()?); // offset delta
        record.put(VarInt(-1).encode()?); // null key
        record.put(VarInt(-1).encode()?); // null value
        record.put(VarInt(0).encode()?); // no headers

        let mut gz = GzEncoder::new(BytesMut::new().writer(), flate2::Compression::default());
        gz.write_all(&record)?;
        let record_data = gz.finish().map(|w| w.into_inner().freeze())?;

        let batch = Batch {
            attributes: BatchAttribute::default()
                .compression(Compression::Gzip)
                .into(),
            record_count: 1,
            record_data,
            ..Default::default()
        };

        let err = Vec::<Record>::try_from(&batch)
            .expect_err("an overlong varint must be rejected by reference");
        assert_matches!(err, Error::Overflow);
        assert!(
            Vec::<Record>::try_from(batch.clone()).is_err(),
            "an overlong varint must be rejected by value"
        );
        assert!(
            inflated::Batch::try_from(&batch).is_err(),
            "an overlong varint must be rejected when inflating"
        );

        Ok(())
    }

    /// A header count that outruns this record's own declared length is
    /// rejected at the record's boundary, instead of reading into the bytes
    /// that follow it in the batch (here, two bytes that belong to no
    /// record). `record_count: 1` exercises one `Record::decode` call on the
    /// uncompressed by-value path.
    #[test]
    fn header_count_reads_past_record_boundary_is_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        // record: length=6, body=[attributes=0, timestamp_delta=0,
        // offset_delta=0, null key, null value, header_count=1], followed
        // by two bytes that belong to no record: a phantom null/null header
        // that decode must not consume as this record's one declared
        // header.
        let record_data = Bytes::from_static(&[12, 0, 0, 0, 1, 1, 2, 1, 1]);

        let batch = Batch {
            attributes: BatchAttribute::default().into(),
            record_count: 1,
            record_data,
            ..Default::default()
        };

        let err = Vec::<Record>::try_from(batch)
            .expect_err("a header count outrunning this record's declared length must be rejected");
        assert!(matches!(err, Error::Overflow), "{err:?}");

        Ok(())
    }

    /// `record_count` claims more records than the 10 bytes of
    /// `record_data` could hold, since every record is at least
    /// `MIN_ENCODED_RECORD_SIZE` bytes, so the batch is rejected before any
    /// record is decoded.
    #[test]
    fn record_count_inconsistent_with_remaining_data_returns_overflow() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = Batch {
            attributes: BatchAttribute::default().into(),
            record_count: 5,
            record_data: Bytes::from_static(&[0u8; 10]),
            ..Default::default()
        };

        let err = Vec::<Record>::try_from(batch).expect_err(
            "a record_count inconsistent with the remaining record_data must be rejected",
        );
        assert!(matches!(err, Error::Overflow), "{err:?}");

        Ok(())
    }

    /// A `record_count` the bytes cannot hold is reported as corrupt rather
    /// than as too large, even when it is also past the decoded-size limit.
    #[test]
    fn record_count_inconsistent_with_data_wins_over_decoded_size_limit() -> Result<()> {
        let _guard = init_tracing()?;

        let batch = Batch {
            attributes: BatchAttribute::default().into(),
            record_count: u32::MAX,
            record_data: Bytes::from_static(&[0u8; 10]),
            ..Default::default()
        };

        assert!(batch.exceeds_decoded_record_count_limit());

        let err = Vec::<Record>::try_from(batch)
            .expect_err("a record_count the record_data cannot hold must be rejected");
        assert!(matches!(err, Error::Overflow), "{err:?}");

        Ok(())
    }

    /// Bytes left in `record_data` after `record_count` records mean the
    /// batch's declared size and its records disagree, so the batch is
    /// rejected, as Kafka's `DefaultRecordBatch` does ("Incorrect declared
    /// batch size, records still remaining").
    #[test]
    fn bytes_after_last_record_are_rejected() -> Result<()> {
        let _guard = init_tracing()?;

        let mut record_data = BytesMut::new();
        record_data.put(Record::builder().build()?.encode()?);
        record_data.put(Record::builder().build()?.encode()?);

        let batch = Batch {
            attributes: BatchAttribute::default().into(),
            record_count: 1,
            record_data: record_data.freeze(),
            ..Default::default()
        };

        let err = Vec::<Record>::try_from(batch)
            .expect_err("bytes after the last declared record must be rejected");
        assert!(matches!(err, Error::Overflow), "{err:?}");

        Ok(())
    }

    const EVERY_CODEC: [Compression; 5] = [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ];

    /// `uncompressed` as a producer compresses the record data of a batch.
    fn compress(compression: &Compression, uncompressed: &[u8]) -> Result<Bytes> {
        match compression {
            Compression::None => Ok(Bytes::copy_from_slice(uncompressed)),

            Compression::Gzip => {
                let mut gz =
                    GzEncoder::new(BytesMut::new().writer(), flate2::Compression::default());
                gz.write_all(uncompressed)?;
                gz.finish()
                    .map(|writer| writer.into_inner().freeze())
                    .map_err(Into::into)
            }

            Compression::Snappy => snap::raw::Encoder::new()
                .compress_vec(uncompressed)
                .map(Bytes::from)
                .map_err(Into::into),

            Compression::Lz4 => {
                let mut lz4 = lz4::EncoderBuilder::new().build(BytesMut::new().writer())?;
                lz4.write_all(uncompressed)?;
                let (writer, finished) = lz4.finish();
                finished?;
                Ok(writer.into_inner().freeze())
            }

            Compression::Zstd => {
                let mut zstd = zstd::stream::write::Encoder::new(BytesMut::new().writer(), 0)?;
                zstd.write_all(uncompressed)?;
                zstd.finish()
                    .map(|writer| writer.into_inner().freeze())
                    .map_err(Into::into)
            }
        }
    }

    /// A batch of `record_count` records whose record data is `uncompressed`
    /// compressed with `compression`. The bytes need not be valid records.
    fn batch_of_record_data(
        compression: Compression,
        record_count: u32,
        uncompressed: &[u8],
    ) -> Result<Batch> {
        Ok(Batch {
            record_data: compress(&compression, uncompressed)?,
            attributes: BatchAttribute::default().compression(compression).into(),
            record_count,
            ..Default::default()
        })
    }

    /// The encoding of `record` with `adjustment` added to its declared
    /// `length`, and the fields left as they are.
    fn encode_with_adjusted_length(record: &Record, adjustment: i32) -> Result<BytesMut> {
        let mut fields = record.encode()?;
        let length: i32 = VarInt::decode(&mut fields).map(Into::into)?;

        let mut encoded = BytesMut::new();
        encoded.put(VarInt(length + adjustment).encode()?);
        encoded.put(fields);
        Ok(encoded)
    }

    /// Both conversions reject `batch` with an error that is not
    /// `MessageMaxSizeExceeded`, which is what lets the broker answer
    /// `INVALID_RECORD` rather than `MESSAGE_TOO_LARGE`. A record that ends
    /// before its fields do fails as `TryGet` when a field has no byte left,
    /// and as `Overflow` when a field claims more bytes than are left.
    fn assert_rejected_as_corrupt_by_both_conversions(batch: &Batch) {
        let by_reference =
            Vec::<Record>::try_from(batch).expect_err("a corrupt batch must be rejected");
        assert_matches!(by_reference, Error::Overflow | Error::TryGet(_));

        let by_value =
            Vec::<Record>::try_from(batch.clone()).expect_err("a corrupt batch must be rejected");
        assert_matches!(by_value, Error::Overflow | Error::TryGet(_));
    }

    /// The by-reference and by-value conversions decode a batch the same way,
    /// whether it is uncompressed or compressed with any codec.
    #[test]
    fn by_reference_and_by_value_conversions_give_the_same_records_for_every_codec() -> Result<()> {
        let _guard = init_tracing()?;

        let records = vec![
            Record::builder()
                .key(Some(Bytes::from_static(b"key")))
                .value(Some(Bytes::from_static(LOREM)))
                .header(
                    Header::builder()
                        .key(Bytes::from_static(b"format"))
                        .value(Bytes::from_static(b"text")),
                )
                .build()?,
            Record::builder().offset_delta(1).build()?,
            Record::builder()
                .offset_delta(2)
                .timestamp_delta(7)
                .key(Some(Bytes::new()))
                .value(Some(Bytes::from(vec![0xa5; 4096])))
                .build()?,
        ];

        for compression in EVERY_CODEC {
            let mut inflated = inflated::Batch {
                records: records.clone(),
                ..Default::default()
            };

            inflated.attributes = BatchAttribute::try_from(inflated.attributes)
                .map(|attribute| attribute.compression(compression.clone()).into())?;

            let batch = Batch::try_from(inflated)?;

            assert_eq!(
                records,
                Vec::<Record>::try_from(&batch)?,
                "by reference, {compression:?}"
            );
            assert_eq!(
                records,
                Vec::<Record>::try_from(batch)?,
                "by value, {compression:?}"
            );
        }

        Ok(())
    }

    /// Kafka's `DefaultRecord` answers `INVALID_RECORD` for a record whose
    /// fields end before its declared `length` does, and the same holds for
    /// every codec.
    #[test]
    fn record_with_length_longer_than_its_fields_is_rejected_for_every_codec() -> Result<()> {
        let _guard = init_tracing()?;

        let mut record_data = encode_with_adjusted_length(
            &Record::builder()
                .key(Some(Bytes::from_static(b"key")))
                .value(Some(Bytes::from_static(b"value")))
                .build()?,
            1,
        )?;
        record_data.put_u8(0xff); // padding inside the declared length

        for compression in EVERY_CODEC {
            assert_rejected_as_corrupt_by_both_conversions(&batch_of_record_data(
                compression,
                1,
                &record_data,
            )?);
        }

        Ok(())
    }

    /// Kafka's `DefaultRecord` answers `INVALID_RECORD` for a record whose
    /// fields run past its declared `length`, and the same holds for every
    /// codec.
    #[test]
    fn record_with_length_shorter_than_its_fields_is_rejected_for_every_codec() -> Result<()> {
        let _guard = init_tracing()?;

        let record_data = encode_with_adjusted_length(
            &Record::builder()
                .key(Some(Bytes::from_static(b"key")))
                .value(Some(Bytes::from_static(b"value")))
                .build()?,
            -1,
        )?;

        for compression in EVERY_CODEC {
            assert_rejected_as_corrupt_by_both_conversions(&batch_of_record_data(
                compression,
                1,
                &record_data,
            )?);
        }

        Ok(())
    }

    /// Kafka's `DefaultRecordBatch` answers `INVALID_RECORD` for a batch that
    /// has bytes after its last record, and the same holds for every codec.
    #[test]
    fn bytes_after_the_last_record_are_rejected_for_every_codec() -> Result<()> {
        let _guard = init_tracing()?;

        let mut record_data = BytesMut::new();
        record_data.put(Record::builder().build()?.encode()?);
        record_data.put_u8(0);

        for compression in EVERY_CODEC {
            assert_rejected_as_corrupt_by_both_conversions(&batch_of_record_data(
                compression,
                1,
                &record_data,
            )?);
        }

        Ok(())
    }

    #[test]
    fn computed_crc_matches_a_freshly_built_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let batch: Batch = inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(LOREM).into()))
            .build()
            .and_then(TryInto::try_into)?;

        assert_eq!(batch.crc, batch.computed_crc());

        Ok(())
    }

    #[test]
    fn set_timestamps_recomputes_the_crc() -> Result<()> {
        let _guard = init_tracing()?;

        let mut batch: Batch = inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(LOREM).into()))
            .build()
            .and_then(TryInto::try_into)?;

        let original_crc = batch.crc;

        batch.set_timestamps(
            TimestampType::CreateTime,
            batch.base_timestamp + 1,
            batch.max_timestamp + 2,
        )?;

        assert_ne!(original_crc, batch.crc);
        assert_eq!(batch.computed_crc(), batch.crc);

        Ok(())
    }

    #[test]
    fn set_timestamps_sets_and_clears_the_timestamp_type() -> Result<()> {
        let _guard = init_tracing()?;

        let mut batch: Batch = inflated::Batch::builder()
            .attributes(Compression::Gzip.into())
            .record(Record::builder().value(Bytes::from_static(LOREM).into()))
            .build()
            .and_then(TryInto::try_into)?;

        batch.set_timestamps(TimestampType::LogAppendTime, 7, 7)?;
        assert_eq!(
            BatchAttribute::default()
                .compression(Compression::Gzip)
                .timestamp(TimestampType::LogAppendTime),
            BatchAttribute::try_from(batch.attributes)?
        );
        assert_eq!(batch.computed_crc(), batch.crc);

        batch.set_timestamps(TimestampType::CreateTime, 7, 7)?;
        assert_eq!(
            BatchAttribute::default().compression(Compression::Gzip),
            BatchAttribute::try_from(batch.attributes)?
        );
        assert_eq!(batch.computed_crc(), batch.crc);

        Ok(())
    }

    #[test]
    fn set_timestamps_rejects_a_batch_whose_crc_does_not_match() -> Result<()> {
        let _guard = init_tracing()?;

        let mut batch: Batch = inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(LOREM).into()))
            .build()
            .and_then(TryInto::try_into)?;

        batch.crc ^= 1;
        let damaged = batch.clone();

        let err = batch
            .set_timestamps(
                TimestampType::LogAppendTime,
                batch.base_timestamp,
                batch.max_timestamp + 1,
            )
            .expect_err("a batch whose crc does not match must be rejected");

        assert!(matches!(err, Error::ApiError(ErrorCode::CorruptMessage)));
        assert_eq!(damaged, batch);

        Ok(())
    }

    /// Decodes the records of `batch`, compressed with `compression`, against a decode budget of
    /// `limit` bytes, as `Vec::<Record>::try_from` does against `MAX_DECODED_BATCH_BYTES`.
    fn decode_within(
        batch: &Batch,
        compression: &Compression,
        limit: usize,
    ) -> Result<Vec<Record>> {
        let record_count = usize::try_from(batch.record_count)?;

        if *compression == Compression::None {
            decode_records(
                batch.record_data.clone(),
                record_count,
                &mut DecodeBudget::new(limit),
            )
        } else {
            inflate_records(
                compression.clone(),
                batch.record_data.clone().reader(),
                record_count,
                limit,
                limit,
            )
        }
    }

    /// Asserts that `inflated`, encoded with `compression`, decodes within a budget of exactly
    /// its `decoded_size` and not within a byte less.
    fn assert_decodes_at_exactly_its_decoded_size(
        inflated: &inflated::Batch,
        compression: &Compression,
    ) -> Result<()> {
        let size = inflated.decoded_size(compression)?;

        let mut encoded = inflated.clone();
        encoded.attributes = BatchAttribute::try_from(encoded.attributes)
            .map(|attribute| attribute.compression(compression.clone()).into())?;
        let batch = Batch::try_from(encoded)?;

        // A rebased record keeps the `length` that it was decoded with, and encoding computes
        // the length from the fields, so the records are compared as encoded.
        let encoded = |records: &[Record]| {
            records
                .iter()
                .map(Encode::encode)
                .collect::<Result<Vec<_>>>()
        };

        assert_eq!(
            encoded(&inflated.records)?,
            encoded(&decode_within(&batch, compression, size)?)?,
            "{compression:?}: decodes within its decoded size, {size}"
        );

        assert_matches!(
            decode_within(&batch, compression, size - 1),
            Err(Error::MessageMaxSizeExceeded(_)),
            "{compression:?}: does not decode within a byte less than its decoded size, {size}"
        );

        Ok(())
    }

    fn records_to_measure() -> Result<Vec<Record>> {
        Ok(vec![
            Record::builder()
                .key(Some(Bytes::from_static(b"key")))
                .value(Some(Bytes::from_static(LOREM)))
                .header(
                    Header::builder()
                        .key(Bytes::from_static(b"format"))
                        .value(Bytes::from_static(b"text")),
                )
                .header(Header::builder().key(Bytes::from_static(b"empty")))
                .build()?,
            Record::builder().offset_delta(1).build()?,
            Record::builder()
                .offset_delta(2)
                .timestamp_delta(7)
                .key(Some(Bytes::new()))
                .value(Some(Bytes::from(vec![0xa5; 4096])))
                .build()?,
        ])
    }

    /// `decoded_size` is exactly what the decoder charges to the decode budget, for every codec.
    #[test]
    fn decoded_size_is_what_decoding_charges_for_every_codec() -> Result<()> {
        let _guard = init_tracing()?;

        let inflated = inflated::Batch {
            records: records_to_measure()?,
            last_offset_delta: 2,
            ..Default::default()
        };

        for compression in EVERY_CODEC {
            assert_decodes_at_exactly_its_decoded_size(&inflated, &compression)?;
        }

        Ok(())
    }

    /// A gzip batch whose `decoded_size` is exactly `MAX_DECODED_BATCH_BYTES` decodes through the
    /// production conversion, and one a byte larger does not.
    #[test]
    fn gzip_batch_decodes_at_the_decoded_limit_and_not_a_byte_over() -> Result<()> {
        let batch_of = |len: usize| -> Result<inflated::Batch> {
            Ok(inflated::Batch {
                attributes: BatchAttribute::default()
                    .compression(Compression::Gzip)
                    .into(),
                records: vec![Record {
                    value: Some(Bytes::from(vec![0u8; len])),
                    ..Default::default()
                }],
                ..Default::default()
            })
        };

        // The length varint of the value is the same width throughout, so each byte of value
        // adds one byte of decoded size.
        let mut len = MAX_DECODED_BATCH_BYTES - 1024;
        len += MAX_DECODED_BATCH_BYTES - batch_of(len)?.decoded_size(&Compression::Gzip)?;

        let at_the_limit = batch_of(len)?;
        assert_eq!(
            MAX_DECODED_BATCH_BYTES,
            at_the_limit.decoded_size(&Compression::Gzip)?
        );
        assert_eq!(
            1,
            Vec::<Record>::try_from(Batch::try_from(at_the_limit)?)?.len()
        );

        let over_the_limit = batch_of(len + 1)?;
        assert_eq!(
            MAX_DECODED_BATCH_BYTES + 1,
            over_the_limit.decoded_size(&Compression::Gzip)?
        );
        assert_matches!(
            Vec::<Record>::try_from(Batch::try_from(over_the_limit)?),
            Err(Error::MessageMaxSizeExceeded(_))
        );

        Ok(())
    }

    /// `appended_decoded_size` is exactly how much `decoded_size` grows when `append_rebased`
    /// appends a batch, for every codec, including when the rebase widens the varints of the
    /// offset and timestamp deltas.
    #[test]
    fn appended_decoded_size_is_the_growth_of_the_merged_decoded_size_for_every_codec() -> Result<()>
    {
        let _guard = init_tracing()?;

        // Offset delta 64 is the first that zigzag-encodes in two bytes, so the appended records
        // move from one-byte to two-byte offset deltas.
        let sink_records = (0..64)
            .map(|offset_delta| Record::builder().offset_delta(offset_delta).build())
            .collect::<Result<Vec<_>>>()?;

        let sink = inflated::Batch {
            base_timestamp: 1_000,
            max_timestamp: 1_000,
            records: sink_records,
            last_offset_delta: 63,
            ..Default::default()
        };

        // A base timestamp a long way after the sink's widens the appended timestamp deltas.
        let appended = inflated::Batch {
            base_timestamp: 1_000 + (1 << 40),
            max_timestamp: 1_000 + (1 << 40) + 7,
            records: records_to_measure()?,
            last_offset_delta: 2,
            ..Default::default()
        };

        for compression in EVERY_CODEC {
            let growth = sink.appended_decoded_size(&appended, &compression)?;

            let mut merged = sink.clone();
            merged.append_rebased(appended.clone())?;

            assert_eq!(
                sink.decoded_size(&compression)? + growth,
                merged.decoded_size(&compression)?,
                "{compression:?}"
            );

            if compression != Compression::None {
                assert!(
                    growth > appended.decoded_size(&compression)?,
                    "{compression:?}: the rebase widens the varints of the appended records"
                );
            }

            assert_decodes_at_exactly_its_decoded_size(&merged, &compression)?;
        }

        Ok(())
    }
}
