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
    ByteSize, Decode, Encode, Error, Result,
    primitive::varint::{UnsignedVarInt, VarInt},
};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, SeqAccess, Visitor},
    ser::{self, SerializeSeq, SerializeStruct},
};
use std::{
    fmt::{self, Formatter},
    marker::PhantomData,
    slice,
};
use tracing::{debug, instrument};

/// Upper bound on how eagerly we preallocate for a wire-supplied count or
/// length (record count, header count, octet length, ...). Those values are
/// untrusted input and must never be trusted at face value for sizing an
/// allocation; a real, larger batch still decodes correctly,
/// it just grows the buffer past this point as elements are pushed.
pub(crate) const MAX_PREALLOCATED_ELEMENTS: usize = 1_024;

/// Upper bound on the memory one produce batch may occupy once decoded.
/// Without it, a few KB of zstd/gzip/lz4 input representing a long run of
/// repeated bytes can decompress into gigabytes, a Snappy block's header
/// claims its decompressed length before any data is read, and the wire
/// encoding amplifies on decode: a null/null header is 2 bytes on the wire
/// and `size_of::<Header>()` (64) decoded, a minimal record 7 bytes on the
/// wire and `size_of::<Record>()` (112) decoded. Matches `nisshi-service`'s
/// `DEFAULT_MAXIMUM_FRAME_SIZE` in value only, kept as its own constant since
/// this crate is sans-I/O and has no access to that connection-level setting.
///
/// Enforced three ways against the same number: as the byte limit on the
/// decompressing reader, as a pre-check on every wire-supplied count
/// (`record_count`, a sequence length) before anything is decoded, and as a
/// running `DecodeBudget` that every allocation made while decoding a batch
/// is charged to.
///
/// The real peak per batch is a small multiple of this, not this exactly: a
/// record's headers are charged after they are allocated (the
/// sequence-length pre-check compares one record against the whole limit,
/// not what is left of the budget), a Snappy batch's decompressed block is
/// live alongside the buffer that the records decode from, the spare capacity
/// of that buffer is not charged, and the zstd decoder's window (up to
/// 128 MiB by default) is not charged.
///
/// A caller that merges batches must keep each merged batch within this
/// limit, or the merged batch no longer decodes. [`inflated::Batch::decoded_size`]
/// and [`inflated::Batch::appended_decoded_size`] measure a batch against it.
///
/// [`inflated::Batch::decoded_size`]: crate::record::inflated::Batch::decoded_size
/// [`inflated::Batch::appended_decoded_size`]: crate::record::inflated::Batch::appended_decoded_size
pub const MAX_DECODED_BATCH_BYTES: usize = 100 * 1024 * 1024;

/// The decoded-memory budget of one batch. The inflated bytes of a compressed
/// batch, each `Record` and each `Header` are charged as they are decoded, and
/// the batch is rejected once the budget is spent, so the memory a batch
/// decodes into is bounded by the budget however small its wire encoding is.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DecodeBudget {
    limit: usize,
    remaining: usize,
}

impl DecodeBudget {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            remaining: limit,
        }
    }

    /// Charge `cost` bytes. The error carries the decoded size reached at
    /// the point the budget ran out.
    pub(crate) fn charge(&mut self, cost: usize) -> Result<()> {
        self.remaining = self.remaining.checked_sub(cost).ok_or_else(|| {
            Error::MessageMaxSizeExceeded((self.limit - self.remaining).saturating_add(cost))
        })?;

        Ok(())
    }
}

/// True if `count` elements of `size` bytes each would on their own exceed
/// [`MAX_DECODED_BATCH_BYTES`]. A cheap check on a wire-supplied count
/// before any element is decoded; the [`DecodeBudget`] then bounds the real
/// total as elements are produced.
pub(crate) fn exceeds_decoded_batch_limit(count: usize, size: usize) -> bool {
    count.saturating_mul(size) > MAX_DECODED_BATCH_BYTES
}

/// The smallest a wire-encoded [`crate::record::Record`] can be: a 1-byte
/// length prefix, a 1-byte attributes field, 1-byte (zero-valued) varints for
/// the timestamp and offset deltas, and 1-byte null-sentinel varints for the
/// key, value, and header count.
pub(crate) const MIN_ENCODED_RECORD_SIZE: usize = 7;

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Octets(pub Option<Bytes>);

impl ByteSize for Octets {
    fn size_in_bytes(&self) -> Result<usize> {
        self.0.as_ref().map_or_else(
            || VarInt(-1).size_in_bytes(),
            |bytes| {
                VarInt::try_from(bytes.len())
                    .and_then(|v| v.size_in_bytes())
                    .map(|vlen| vlen + bytes.len())
            },
        )
    }
}

impl Encode for Octets {
    #[instrument(skip_all)]
    fn encode(&self) -> Result<Bytes> {
        match self.0.clone() {
            None => VarInt::from(-1).encode(),
            Some(data) => {
                let mut encoded = self.size_in_bytes().map(BytesMut::with_capacity)?;

                let length = VarInt::try_from(data.len())?;
                encoded.put(length.encode()?);
                encoded.put(data);

                Ok(encoded.into())
            }
        }
    }
}

impl Decode for Octets {
    #[instrument(skip_all)]
    fn decode(encoded: &mut Bytes) -> Result<Self> {
        let length = VarInt::decode(encoded)?.0;

        if length == -1 {
            Ok(Self(None))
        } else {
            let length = usize::try_from(length).map_err(|_| Error::Overflow)?;

            if length > encoded.len() {
                return Err(Error::Overflow);
            }

            Ok(Self(Some(encoded.split_to(length))))
        }
    }
}

impl Octets {
    #[must_use]
    pub fn empty() -> Self {
        Self(None)
    }

    pub fn serialize<S>(i: &Option<Bytes>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match i {
            None => {
                let mut s = serializer.serialize_struct(stringify!(Octets), 1)?;
                s.serialize_field("length", &VarInt(-1))?;
                s.end()
            }

            Some(bytes) => {
                let mut s = serializer.serialize_struct(stringify!(Octets), 2)?;

                i32::try_from(bytes.len())
                    .map_err(|e| ser::Error::custom(e.to_string()))
                    .map(VarInt)
                    .and_then(|length| s.serialize_field("length", &length))
                    .and(s.serialize_field("data", &bytes))
                    .and(s.end())
            }
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Bytes>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = Option<Bytes>;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(stringify!(Bytes))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut length = seq
                    .next_element::<VarInt>()?
                    .ok_or_else(|| de::Error::custom("length"))?
                    .0;

                debug!(?length);

                if length == -1 {
                    Ok(None)
                } else {
                    let mut r = usize::try_from(length)
                        .map(|length| {
                            BytesMut::with_capacity(length.min(MAX_PREALLOCATED_ELEMENTS))
                        })
                        .map_err(|error| de::Error::custom(format!("{error:?}")))?;

                    while length >= 1 {
                        let char = seq
                            .next_element::<u8>()?
                            .ok_or_else(|| de::Error::custom("byte"))?;

                        r.put_u8(char);
                        length -= 1;
                    }

                    debug!(len = r.len());

                    Ok(Some(r.into()))
                }
            }
        }

        deserializer.deserialize_seq(V)
    }
}

impl From<Octets> for Option<Bytes> {
    fn from(value: Octets) -> Self {
        value.0
    }
}

impl From<Bytes> for Octets {
    fn from(value: Bytes) -> Self {
        Self(Some(value))
    }
}

impl From<Option<Bytes>> for Octets {
    fn from(value: Option<Bytes>) -> Self {
        Self(value)
    }
}

impl From<Option<Vec<u8>>> for Octets {
    fn from(value: Option<Vec<u8>>) -> Self {
        Self(value.map(Into::into))
    }
}

impl From<Vec<u8>> for Octets {
    fn from(value: Vec<u8>) -> Self {
        Self(Some(value.into()))
    }
}

impl From<&[u8]> for Octets {
    fn from(value: &[u8]) -> Self {
        Self(Some(Bytes::copy_from_slice(value)))
    }
}

impl Serialize for Octets {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Self::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for Octets {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::deserialize(deserializer).map(Self)
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct VarIntSequence<T>(pub Vec<T>);

impl<T> IntoIterator for VarIntSequence<T> {
    type Item = T;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T> FromIterator<T> for VarIntSequence<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut c = Self(vec![]);
        for i in iter {
            c.0.push(i);
        }
        c
    }
}

impl<T> From<VarIntSequence<T>> for Vec<T> {
    fn from(value: VarIntSequence<T>) -> Self {
        value.0
    }
}

impl<T> From<Vec<T>> for VarIntSequence<T> {
    fn from(value: Vec<T>) -> Self {
        Self(value)
    }
}

impl<T> ByteSize for VarIntSequence<T>
where
    T: ByteSize,
{
    #[instrument(skip_all, ret)]
    fn size_in_bytes(&self) -> Result<usize> {
        self.0.iter().try_fold(
            i32::try_from(self.0.len())
                .map_err(Into::into)
                .map(VarInt)
                .and_then(|vlen| vlen.size_in_bytes())?,
            |acc, t| t.size_in_bytes().map(|tlen| acc + tlen),
        )
    }
}

impl<T> Encode for VarIntSequence<T>
where
    T: Encode + ByteSize,
{
    #[instrument(skip_all)]
    fn encode(&self) -> Result<Bytes> {
        let mut encoded = self.size_in_bytes().map(BytesMut::with_capacity)?;

        let length = VarInt::try_from(self.0.len())?;
        encoded.put(length.encode()?);

        for i in self.0.iter() {
            encoded.put(i.encode()?);
        }

        Ok(encoded.into())
    }
}

impl<T> Decode for VarIntSequence<T>
where
    T: Decode,
{
    #[instrument(skip_all)]
    fn decode(encoded: &mut Bytes) -> Result<Self> {
        debug!(encoded = encoded.len());

        let length = VarInt::decode(encoded)
            .and_then(|length| usize::try_from(length.0).map_err(|_| Error::Overflow))
            .inspect(|length| debug!(length))?;

        // Matches Kafka's own bound in `DefaultRecord.readFrom`
        // (`numHeaders > buffer.remaining()`): a real element needs at
        // least one byte to encode, so a count that exceeds the bytes
        // actually left can never be satisfied without reading past this
        // buffer's true end. Deliberately undivided: a record can pack
        // several null/null headers into very few bytes each, so dividing
        // by anything larger than 1 here would reject real, valid batches.
        // It runs ahead of the decoded-size check so that a count the bytes
        // cannot hold is reported as corrupt, not as too large.
        if length > encoded.len() {
            return Err(Error::Overflow);
        }

        if exceeds_decoded_batch_limit(length, size_of::<T>()) {
            return Err(Error::MessageMaxSizeExceeded(
                length.saturating_mul(size_of::<T>()),
            ));
        }

        let mut items = Vec::with_capacity(length.min(MAX_PREALLOCATED_ELEMENTS));
        for _ in 0..length {
            items.push(T::decode(encoded)?);
        }

        Ok(Self(items))
    }
}

impl<T> VarIntSequence<T> {
    pub(crate) fn serialize<S>(v: &Vec<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        let mut s = serializer.serialize_seq(Some(v.len()))?;

        i32::try_from(v.len())
            .map_err(|e| ser::Error::custom(e.to_string()))
            .map(VarInt)
            .and_then(|length| s.serialize_element(&length))?;

        for i in v {
            s.serialize_element(&i)?;
        }
        s.end()
    }
}

impl<T> VarIntSequence<T>
where
    T: Serialize,
{
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        struct V<T>(PhantomData<T>);

        impl<'de, T> Visitor<'de> for V<T>
        where
            T: Deserialize<'de>,
        {
            type Value = Vec<T>;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(stringify!(Sequence))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                seq.next_element::<VarInt>()?
                    .ok_or_else(|| <A::Error as de::Error>::custom("length"))
                    .map(|v| v.0)
                    .inspect(|length| debug!("length: {length}"))
                    .and_then(|length| {
                        let capacity: usize = length.try_into().map_err(|e| {
                            <A::Error as de::Error>::custom(format!(
                                "length: {length}, caused: {e:?}"
                            ))
                        })?;

                        // No claimed-length pre-check here: `A::Error` can
                        // only be built through `de::Error::custom`, which
                        // would stringify the typed error. The `Decoder`'s
                        // `DecodeBudget` charges each element as it is
                        // produced and returns the typed error directly.
                        (0..length).try_fold(
                            Vec::with_capacity(capacity.min(MAX_PREALLOCATED_ELEMENTS)),
                            |mut acc, _| {
                                seq.next_element::<T>()?
                                    .ok_or_else(|| <A::Error as de::Error>::custom("item"))
                                    .map(|t| {
                                        acc.push(t);
                                        acc
                                    })
                            },
                        )
                    })
            }
        }

        deserializer.deserialize_seq(V(PhantomData))
    }
}

impl<T: Serialize> Serialize for VarIntSequence<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Self::serialize(&self.0, serializer)
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Sequence<T>(pub Vec<T>);

impl<T> Sequence<T> {
    /// Returns an iterator over the sequence's items.
    pub(crate) fn iter(&self) -> slice::Iter<'_, T> {
        self.0.iter()
    }
}

impl<T> IntoIterator for Sequence<T> {
    type Item = T;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T> FromIterator<T> for Sequence<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut c = Self(vec![]);
        for i in iter {
            c.0.push(i);
        }
        c
    }
}

impl<T> From<Vec<T>> for Sequence<T> {
    fn from(value: Vec<T>) -> Self {
        Self(value)
    }
}

impl<T: Serialize> Sequence<T> {
    pub(crate) fn serialize<S>(v: &Vec<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_seq(Some(v.len()))?;

        u32::try_from(v.len())
            .map_err(|e| ser::Error::custom(e.to_string()))
            .and_then(|length| s.serialize_element(&length))?;

        for i in v {
            s.serialize_element(&i)?;
        }
        s.end()
    }
}

impl<T> Sequence<T> {
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        struct V<T>(PhantomData<T>);

        impl<'de, T> Visitor<'de> for V<T>
        where
            T: Deserialize<'de>,
        {
            type Value = Vec<T>;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(stringify!(Sequence))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                seq.next_element::<i32>()?
                    .ok_or_else(|| <A::Error as de::Error>::custom("length"))
                    .inspect(|length| debug!("length: {length}"))
                    .and_then(|length| {
                        (0..length).try_fold(
                            Vec::with_capacity(length.try_into().map_err(|e| {
                                <A::Error as de::Error>::custom(format!(
                                    "length: {length}, caused: {e:?}"
                                ))
                            })?),
                            |mut acc, _| {
                                seq.next_element::<T>()?
                                    .ok_or_else(|| <A::Error as de::Error>::custom("item"))
                                    .map(|t| {
                                        acc.push(t);
                                        acc
                                    })
                            },
                        )
                    })
            }
        }

        deserializer.deserialize_seq(V(PhantomData))
    }
}

impl<T> ByteSize for Sequence<T>
where
    T: ByteSize,
{
    fn size_in_bytes(&self) -> Result<usize> {
        self.0.iter().try_fold(size_of::<u32>(), |acc, i| {
            UnsignedVarInt::size_inclusive(i).map(|sz| acc + sz)
        })
    }
}

impl<T> Extend<T> for Sequence<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for i in iter {
            self.0.push(i);
        }
    }
}

impl<T: Serialize> Serialize for Sequence<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Self::serialize(&self.0, serializer)
    }
}

impl<'de, T> Deserialize<'de> for Sequence<T>
where
    T: Deserialize<'de>,
    T: Default,
{
    #[allow(clippy::too_many_lines)]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::deserialize(deserializer).map(Self)
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn decode_budget_rejects_the_charge_that_overspends_it() {
        let mut budget = super::DecodeBudget::new(10);

        assert!(budget.charge(4).is_ok());
        assert!(budget.charge(6).is_ok());

        // The error carries the decoded size reached, not the limit.
        assert!(matches!(
            budget.charge(1),
            Err(crate::Error::MessageMaxSizeExceeded(11))
        ));
    }

    #[test]
    fn claimed_count_check_saturates_rather_than_overflowing() {
        use super::{MAX_DECODED_BATCH_BYTES, exceeds_decoded_batch_limit};

        assert!(exceeds_decoded_batch_limit(usize::MAX, 64));
        assert!(!exceeds_decoded_batch_limit(0, usize::MAX));
        assert!(!exceeds_decoded_batch_limit(MAX_DECODED_BATCH_BYTES, 1));
        assert!(exceeds_decoded_batch_limit(MAX_DECODED_BATCH_BYTES + 1, 1));
    }
    use std::{fmt::Debug, fs::File, sync::Arc, thread};

    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan};

    use crate::Error;

    use super::*;

    fn init_tracing() -> Result<DefaultGuard, Error> {
        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_span_events(FmtSpan::FULL)
                .with_env_filter(
                    EnvFilter::from_default_env()
                        .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
                )
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

    #[instrument(skip_all)]
    fn encode_decode<T>(expected: T) -> Result<()>
    where
        T: Encode + Decode + Debug + PartialEq,
    {
        debug!(?expected);

        let mut encoded = expected
            .encode()
            .inspect(|encoded| debug!(encoded = ?encoded[..]))?;
        let actual = T::decode(&mut encoded).inspect(|actual| debug!(?actual))?;

        assert_eq!(expected, actual);
        assert_eq!(0, encoded.len());
        Ok(())
    }

    #[test]
    fn octets_none() -> Result<()> {
        let _guard = init_tracing()?;

        let expected = Octets(None);
        assert_eq!(1, expected.size_in_bytes()?);
        encode_decode(expected)
    }

    #[test]
    fn octets_some() -> Result<()> {
        let _guard = init_tracing()?;

        let expected = Octets(Some(Bytes::from_static(b"34543")));
        assert_eq!(6, expected.size_in_bytes()?);
        encode_decode(expected)
    }

    #[test]
    fn empty_var_int_seq() -> Result<()> {
        let _guard = init_tracing()?;

        let expected = VarIntSequence(Vec::<Octets>::new());
        assert_eq!(1, expected.size_in_bytes()?);
        encode_decode(expected)
    }

    #[test]
    fn single_var_int_seq() -> Result<()> {
        let _guard = init_tracing()?;

        let expected = VarIntSequence(vec![Octets(Some(Bytes::from_static(b"34543")))]);
        assert_eq!(7, expected.size_in_bytes()?);
        encode_decode(expected)
    }

    #[test]
    fn multiple_var_int_seq() -> Result<()> {
        let _guard = init_tracing()?;

        let expected = VarIntSequence(vec![
            Octets(Some(Bytes::from_static(b"34543"))),
            Octets(Some(Bytes::from_static(b"32123"))),
        ]);
        assert_eq!(13, expected.size_in_bytes()?);
        encode_decode(expected)
    }

    /// -2 is not the -1 "no value" sentinel: casting it straight to `usize`
    /// sign-extends to a value near `usize::MAX`, which then fails
    /// `split_to`'s own bounds assertion (`at <= len`), panicking instead of
    /// returning `Err`.
    #[test]
    fn octets_decode_length_below_negative_one_sentinel_returns_err() -> Result<()> {
        let _guard = init_tracing()?;

        let mut encoded = VarInt(-2).encode()?;
        assert!(Octets::decode(&mut encoded).is_err());

        Ok(())
    }

    /// A length that is a plausible, positive value, but larger than the
    /// bytes actually remaining: `split_to` panics rather than erroring.
    #[test]
    fn octets_decode_length_exceeding_remaining_returns_err() -> Result<()> {
        let _guard = init_tracing()?;

        let mut encoded = BytesMut::new();
        encoded.put(VarInt(100).encode()?);
        encoded.put_slice(&[1, 2]);
        let mut encoded = encoded.freeze();

        assert!(Octets::decode(&mut encoded).is_err());

        Ok(())
    }

    /// The same negative-count defect as `octets_decode_...`, but for the
    /// header-count-driven `Vec::with_capacity` in `VarIntSequence::decode`.
    #[test]
    fn var_int_sequence_decode_negative_length_returns_err() -> Result<()> {
        let _guard = init_tracing()?;

        let mut encoded = VarInt(-2).encode()?;
        assert!(VarIntSequence::<Octets>::decode(&mut encoded).is_err());

        Ok(())
    }
}
