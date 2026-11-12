// Copyright ⓒ Solace Corporation
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

//! Property-based tests for encode→decode round-trips (Gap 4.1).
//!
//! Verifies that arbitrary valid values of core types survive
//! encode→decode without data loss or corruption.

use bytes::{Buf as _, Bytes, BytesMut};
use nisshi_sans_io::{
    BatchAttribute, ByteSize, Compression, Decode, Decoder, Encode, Encoder,
    primitive::varint::{LongVarInt, UnsignedVarInt, VarInt},
    record::{self, Header, Record, deflated, inflated},
};
use proptest::prelude::*;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

fn arb_optional_bytes(max_len: usize) -> impl Strategy<Value = Option<Bytes>> {
    prop_oneof![
        1 => Just(None),
        3 => prop::collection::vec(any::<u8>(), 0..=max_len)
                .prop_map(|v| Some(Bytes::from(v))),
    ]
}

fn arb_bytes(max_len: usize) -> impl Strategy<Value = Bytes> {
    prop::collection::vec(any::<u8>(), 0..=max_len).prop_map(Bytes::from)
}

fn arb_record_header() -> impl Strategy<Value = Header> {
    (arb_optional_bytes(64), arb_optional_bytes(64)).prop_map(|(key, value)| Header { key, value })
}

// Compression codecs that support both inflate and deflate.
// Snappy is excluded because `into_record_data` does not implement
// snappy encoding (only decoding is supported).
fn arb_compression() -> impl Strategy<Value = Compression> {
    prop_oneof![
        Just(Compression::None),
        Just(Compression::Gzip),
        Just(Compression::Lz4),
        Just(Compression::Zstd),
    ]
}

/// Serde round-trip helper: serialize with Encoder, deserialize with Decoder.
fn serde_roundtrip<T>(value: &T) -> nisshi_sans_io::Result<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    let mut encoder = Encoder::new(BytesMut::new());
    value.serialize(&mut encoder)?;

    let mut encoded = Bytes::from(encoder).reader();

    let mut decoder = Decoder::new(&mut encoded);
    T::deserialize(&mut decoder)
}

// ---------------------------------------------------------------------------
// VarInt round-trip
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn varint_encode_decode_roundtrip(value in any::<i32>()) {
        let original = VarInt(value);
        let mut encoded = original.encode()?;
        let decoded = VarInt::decode(&mut encoded)?;

        prop_assert_eq!(original, decoded);
        prop_assert_eq!(0, encoded.len(), "all bytes consumed");
    }

    #[test]
    fn varint_zigzag_identity(value in any::<i32>()) {
        let original = VarInt(value);
        let mut encoded = original.encode()?;
        let decoded = VarInt::decode(&mut encoded)?;
        prop_assert_eq!(value, decoded.0);
    }

    #[test]
    fn varint_size_matches_encoding(value in any::<i32>()) {
        let v = VarInt(value);
        let encoded = v.encode()?;
        let reported = v.size_in_bytes()?;
        prop_assert_eq!(reported, encoded.len(),
            "size_in_bytes must match actual encoded length");
    }
}

// ---------------------------------------------------------------------------
// UnsignedVarInt round-trip (serde-based, no Encode/Decode trait)
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn unsigned_varint_serde_roundtrip(value in any::<u32>()) {
        let original = UnsignedVarInt(value);
        let decoded = serde_roundtrip(&original)?;
        prop_assert_eq!(original, decoded);
    }

    #[test]
    fn unsigned_varint_size_consistency(value in any::<u32>()) {
        let v = UnsignedVarInt(value);
        let mut encoder = Encoder::new(BytesMut::new());
        v.serialize(&mut encoder)?;

        let encoded = Bytes::from(encoder);

        let reported = v.size_in_bytes()?;
        prop_assert_eq!(reported, encoded.len(),
            "size_in_bytes must match actual serialized length");
    }
}

// ---------------------------------------------------------------------------
// LongVarInt round-trip
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn long_varint_encode_decode_roundtrip(value in any::<i64>()) {
        let original = LongVarInt(value);
        let mut encoded = original.encode()?;
        let decoded = LongVarInt::decode(&mut encoded)?;

        prop_assert_eq!(original, decoded);
        prop_assert_eq!(0, encoded.len(), "all bytes consumed");
    }

    #[test]
    fn long_varint_size_matches_encoding(value in any::<i64>()) {
        let v = LongVarInt(value);
        let encoded = v.encode()?;
        let reported = v.size_in_bytes()?;
        prop_assert_eq!(reported, encoded.len(),
            "size_in_bytes must match actual encoded length");
    }
}

// ---------------------------------------------------------------------------
// Record Header round-trip (Encode/Decode trait)
// ---------------------------------------------------------------------------

proptest! {
    #[test]
    fn record_header_encode_decode_roundtrip(
        key in arb_optional_bytes(128),
        value in arb_optional_bytes(128),
    ) {
        let original = Header { key, value };
        let mut encoded = original.encode()?;
        let decoded = Header::decode(&mut encoded)?;

        prop_assert_eq!(&original, &decoded);
        prop_assert_eq!(0, encoded.len(), "all bytes consumed");
    }
}

// ---------------------------------------------------------------------------
// Record round-trip (Encode/Decode trait)
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn record_encode_decode_roundtrip(
        timestamp_delta in any::<i64>(),
        offset_delta in any::<i32>(),
        key in arb_optional_bytes(256),
        value in arb_optional_bytes(512),
        headers in prop::collection::vec(arb_record_header(), 0..=4),
    ) {
        let mut builder = Record::builder()
            .timestamp_delta(timestamp_delta)
            .offset_delta(offset_delta)
            .key(key)
            .value(value);

        for h in headers {
            let hb = record::Header::builder();
            let hb = match h.key {
                Some(k) => hb.key(k),
                None => hb,
            };
            let hb = match h.value {
                Some(v) => hb.value(v),
                None => hb,
            };
            builder = builder.header(hb);
        }

        let original = builder.build()?;
        let mut encoded = original.encode()?;
        let decoded = Record::decode(&mut encoded)?;

        prop_assert_eq!(&original, &decoded);
        prop_assert_eq!(0, encoded.len(), "all bytes consumed");
    }
}

// ---------------------------------------------------------------------------
// inflated::Batch → deflated::Batch → inflated::Batch round-trip
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn batch_inflate_deflate_roundtrip(
        base_offset in any::<i64>(),
        partition_leader_epoch in any::<i32>(),
        base_timestamp in 0i64..=i64::MAX / 2,
        max_timestamp in 0i64..=i64::MAX / 2,
        producer_id in any::<i64>(),
        producer_epoch in any::<i16>(),
        base_sequence in any::<i32>(),
        compression in arb_compression(),
        rec_keys in prop::collection::vec(arb_optional_bytes(64), 1..=3),
        rec_values in prop::collection::vec(arb_optional_bytes(64), 1..=3),
    ) {
        let attributes: i16 = BatchAttribute::default()
            .compression(compression)
            .into();

        let mut builder = inflated::Batch::builder()
            .base_offset(base_offset)
            .partition_leader_epoch(partition_leader_epoch)
            .magic(2)
            .attributes(attributes)
            .last_offset_delta(0)
            .base_timestamp(base_timestamp)
            .max_timestamp(max_timestamp)
            .producer_id(producer_id)
            .producer_epoch(producer_epoch)
            .base_sequence(base_sequence);

        let record_count = rec_keys.len().min(rec_values.len());
        for i in 0..record_count {
            builder = builder.record(
                Record::builder()
                    .key(rec_keys[i].clone())
                    .value(rec_values[i].clone()),
            );
        }

        let original = builder.build()?;

        // inflated → deflated
        let deflated = deflated::Batch::try_from(original.clone())?;
        prop_assert_eq!(record_count as u32, deflated.record_count);

        // deflated → inflated
        let roundtripped = inflated::Batch::try_from(deflated)?;

        prop_assert_eq!(original.base_offset, roundtripped.base_offset);
        prop_assert_eq!(original.partition_leader_epoch, roundtripped.partition_leader_epoch);
        prop_assert_eq!(original.magic, roundtripped.magic);
        // prop_assert_eq!(original.attributes, roundtripped.attributes);
        prop_assert_eq!(original.last_offset_delta, roundtripped.last_offset_delta);
        prop_assert_eq!(original.base_timestamp, roundtripped.base_timestamp);
        prop_assert_eq!(original.max_timestamp, roundtripped.max_timestamp);
        prop_assert_eq!(original.producer_id, roundtripped.producer_id);
        prop_assert_eq!(original.producer_epoch, roundtripped.producer_epoch);
        prop_assert_eq!(original.base_sequence, roundtripped.base_sequence);
        prop_assert_eq!(original.records.len(), roundtripped.records.len());

        for (orig_rec, rt_rec) in original.records.iter().zip(roundtripped.records.iter()) {
            prop_assert_eq!(&orig_rec.key, &rt_rec.key);
            prop_assert_eq!(&orig_rec.value, &rt_rec.value);
            prop_assert_eq!(orig_rec.timestamp_delta, rt_rec.timestamp_delta);
            prop_assert_eq!(orig_rec.offset_delta, rt_rec.offset_delta);
            prop_assert_eq!(orig_rec.headers.len(), rt_rec.headers.len());
        }
    }
}

// ---------------------------------------------------------------------------
// deflated::Batch encode→decode round-trip (Bytes conversion)
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn deflated_batch_bytes_roundtrip(
        base_offset in any::<i64>(),
        base_timestamp in 0i64..=i64::MAX / 2,
        producer_id in any::<i64>(),
        value in arb_bytes(128),
    ) {
        let batch = inflated::Batch::builder()
            .base_offset(base_offset)
            .base_timestamp(base_timestamp)
            .max_timestamp(base_timestamp)
            .producer_id(producer_id)
            .record(Record::builder().value(Some(value)))
            .build()?;

        let deflated = deflated::Batch::try_from(batch)?;

        // deflated → Bytes → deflated
        let encoded: Bytes = deflated.clone().into();
        let decoded = deflated::Batch::try_from(encoded)?;

        prop_assert_eq!(deflated.base_offset, decoded.base_offset);
        prop_assert_eq!(deflated.batch_length, decoded.batch_length);
        prop_assert_eq!(deflated.partition_leader_epoch, decoded.partition_leader_epoch);
        prop_assert_eq!(deflated.magic, decoded.magic);
        prop_assert_eq!(deflated.crc, decoded.crc);
        prop_assert_eq!(deflated.attributes, decoded.attributes);
        prop_assert_eq!(deflated.last_offset_delta, decoded.last_offset_delta);
        prop_assert_eq!(deflated.base_timestamp, decoded.base_timestamp);
        prop_assert_eq!(deflated.max_timestamp, decoded.max_timestamp);
        prop_assert_eq!(deflated.producer_id, decoded.producer_id);
        prop_assert_eq!(deflated.producer_epoch, decoded.producer_epoch);
        prop_assert_eq!(deflated.base_sequence, decoded.base_sequence);
        prop_assert_eq!(deflated.record_count, decoded.record_count);
        prop_assert_eq!(deflated.record_data, decoded.record_data);
    }
}
