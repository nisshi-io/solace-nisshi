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

use crate::{ByteSize, Decode, Encode, Error, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, SeqAccess, Visitor},
    ser::SerializeSeq,
};
use std::{
    any::{type_name, type_name_of_val},
    fmt::Formatter,
    ops::Deref,
};
use tracing::{debug, instrument};

const CONTINUATION: u8 = 0b1000_0000;
const MASK: u8 = 0b0111_1111;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct VarInt(pub i32);

impl From<VarInt> for i32 {
    fn from(value: VarInt) -> Self {
        value.0
    }
}

impl Deref for VarInt {
    type Target = i32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Decode for VarInt {
    #[instrument(skip_all, ret)]
    fn decode(encoded: &mut Bytes) -> Result<Self> {
        let mut shift = 0u8;
        let mut accumulator = 0u32;
        let mut done = false;

        while !done {
            let byte = encoded.try_get_u8()?;

            if byte & CONTINUATION == CONTINUATION {
                accumulator = u32::from(byte & MASK)
                    .checked_shl(shift as u32)
                    .and_then(|intermediate| accumulator.checked_add(intermediate))
                    .ok_or(Error::Overflow)?;

                shift = shift.checked_add(7).ok_or(Error::Overflow)?;
            } else {
                accumulator = u32::from(byte)
                    .checked_shl(shift as u32)
                    .and_then(|intermediate| accumulator.checked_add(intermediate))
                    .ok_or(Error::Overflow)?;

                done = true;
            }
        }

        Ok(Self(Self::de_zigzag(accumulator)))
    }
}

impl Encode for VarInt {
    #[instrument]
    fn encode(&self) -> Result<Bytes> {
        let mut encoded = self.size_in_bytes().map(BytesMut::with_capacity)?;

        let mut v = Self::en_zigzag(self.0);

        while v >= u32::from(CONTINUATION) {
            #[allow(clippy::cast_possible_truncation)]
            encoded.put_u8(v as u8 | CONTINUATION);
            v >>= 7;
        }

        #[allow(clippy::cast_possible_truncation)]
        encoded.put_u8(v as u8);

        Ok(encoded.into()).inspect(|encoded: &Bytes| debug!(encoded = ?encoded[..]))
    }
}

impl VarInt {
    #[allow(clippy::cast_sign_loss)]
    fn en_zigzag(decoded: i32) -> u32 {
        ((decoded << 1) ^ (decoded >> 31)) as u32
    }

    #[allow(clippy::cast_possible_wrap)]
    fn de_zigzag(encoded: u32) -> i32 {
        ((encoded >> 1) as i32) ^ -((encoded & 1) as i32)
    }

    pub fn serialize<S>(i: &i32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        debug!(?i);

        let mut v = Self::en_zigzag(*i);
        let mut s = serializer.serialize_seq(None)?;

        while v >= u32::from(CONTINUATION) {
            #[allow(clippy::cast_possible_truncation)]
            s.serialize_element(&(v as u8 | CONTINUATION))?;
            v >>= 7;
        }

        #[allow(clippy::cast_possible_truncation)]
        s.serialize_element(&(v as u8))?;
        s.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<i32, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VarIntVisitor;

        impl<'de> Visitor<'de> for VarIntVisitor {
            type Value = i32;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(stringify!(VarInt))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut shift = 0u8;
                let mut accumulator = 0u32;
                let mut done = false;

                while !done {
                    let byte = seq
                        .next_element::<u8>()?
                        .ok_or_else(|| de::Error::custom("u8"))?;

                    let overflow = || de::Error::custom("varint is too long");

                    if byte & CONTINUATION == CONTINUATION {
                        accumulator = u32::from(byte & MASK)
                            .checked_shl(u32::from(shift))
                            .and_then(|intermediate| accumulator.checked_add(intermediate))
                            .ok_or_else(overflow)?;

                        shift = shift.checked_add(7).ok_or_else(overflow)?;
                    } else {
                        accumulator = u32::from(byte)
                            .checked_shl(u32::from(shift))
                            .and_then(|intermediate| accumulator.checked_add(intermediate))
                            .ok_or_else(overflow)?;

                        done = true;
                    }
                }

                let i = VarInt::de_zigzag(accumulator);
                debug!("i: {i}");
                Ok(i)
            }
        }

        deserializer.deserialize_seq(VarIntVisitor)
    }

    pub fn size_inclusive(bs: &impl ByteSize) -> Result<usize> {
        bs.size_in_bytes().and_then(|size| {
            i32::try_from(size)
                .map_err(Into::into)
                .map(Self)
                .and_then(|vsz| vsz.size_in_bytes().map(|vsz| vsz + size))
        })
    }
}

impl ByteSize for VarInt {
    fn size_in_bytes(&self) -> Result<usize> {
        let mut bytes = 1;
        let mut v = Self::en_zigzag(self.0);
        while v >= u32::from(CONTINUATION) {
            v >>= 7;
            bytes += 1;
        }
        Ok(bytes)
    }
}

impl From<i32> for VarInt {
    fn from(value: i32) -> Self {
        Self(value)
    }
}

impl TryFrom<usize> for VarInt {
    type Error = Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let v = i32::try_from(value)?;
        Ok(Self(v))
    }
}

impl Serialize for VarInt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Self::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for VarInt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::deserialize(deserializer).map(Self)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LongVarInt(pub i64);

impl From<LongVarInt> for i64 {
    fn from(value: LongVarInt) -> Self {
        value.0
    }
}

impl Deref for LongVarInt {
    type Target = i64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Encode for LongVarInt {
    fn encode(&self) -> Result<Bytes> {
        let mut encoded = self.size_in_bytes().map(BytesMut::with_capacity)?;

        let mut v = Self::en_zigzag(self.0);

        while v >= u64::from(CONTINUATION) {
            #[allow(clippy::cast_possible_truncation)]
            encoded.put_u8(v as u8 | CONTINUATION);
            v >>= 7;
        }

        #[allow(clippy::cast_possible_truncation)]
        encoded.put_u8(v as u8);
        Ok(encoded.into())
    }
}

impl Decode for LongVarInt {
    #[instrument(skip_all, ret)]
    fn decode(encoded: &mut Bytes) -> Result<Self> {
        let mut shift = 0u8;
        let mut accumulator = 0u64;
        let mut done = false;

        while !done {
            let byte = encoded.try_get_u8()?;

            if byte & CONTINUATION == CONTINUATION {
                accumulator = u64::from(byte & MASK)
                    .checked_shl(shift as u32)
                    .and_then(|intermediate| accumulator.checked_add(intermediate))
                    .ok_or(Error::Overflow)?;

                shift = shift.checked_add(7).ok_or(Error::Overflow)?;
            } else {
                accumulator = u64::from(byte)
                    .checked_shl(shift as u32)
                    .and_then(|intermediate| accumulator.checked_add(intermediate))
                    .ok_or(Error::Overflow)?;

                done = true;
            }
        }

        Ok(Self(Self::de_zigzag(accumulator)))
    }
}

impl LongVarInt {
    #[allow(clippy::cast_sign_loss)]
    fn en_zigzag(decoded: i64) -> u64 {
        ((decoded << 1) ^ (decoded >> 63)) as u64
    }

    #[allow(clippy::cast_possible_wrap)]
    fn de_zigzag(encoded: u64) -> i64 {
        ((encoded >> 1) as i64) ^ -((encoded & 1) as i64)
    }

    pub fn serialize<S>(i: &i64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut v = Self::en_zigzag(*i);
        let mut s = serializer.serialize_seq(None)?;

        while v >= u64::from(CONTINUATION) {
            #[allow(clippy::cast_possible_truncation)]
            s.serialize_element(&(v as u8 | CONTINUATION))?;
            v >>= 7;
        }

        #[allow(clippy::cast_possible_truncation)]
        s.serialize_element(&(v as u8))?;
        s.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<i64, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct LongVarIntVisitor;

        impl<'de> Visitor<'de> for LongVarIntVisitor {
            type Value = i64;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(stringify!(LongVarInt))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut shift = 0u8;
                let mut accumulator = 0u64;
                let mut done = false;

                while !done {
                    let byte = seq
                        .next_element::<u8>()?
                        .ok_or_else(|| de::Error::custom("u8"))?;

                    let overflow = || de::Error::custom("long varint is too long");

                    if byte & CONTINUATION == CONTINUATION {
                        accumulator = u64::from(byte & MASK)
                            .checked_shl(u32::from(shift))
                            .and_then(|intermediate| accumulator.checked_add(intermediate))
                            .ok_or_else(overflow)?;

                        shift = shift.checked_add(7).ok_or_else(overflow)?;
                    } else {
                        accumulator = u64::from(byte)
                            .checked_shl(u32::from(shift))
                            .and_then(|intermediate| accumulator.checked_add(intermediate))
                            .ok_or_else(overflow)?;

                        done = true;
                    }
                }

                Ok(LongVarInt::de_zigzag(accumulator))
            }
        }

        deserializer.deserialize_seq(LongVarIntVisitor)
    }

    pub fn size_inclusive(bs: &impl ByteSize) -> Result<usize> {
        bs.size_in_bytes().and_then(|size| {
            i64::try_from(size)
                .map_err(Into::into)
                .map(Self)
                .and_then(|vsz| vsz.size_in_bytes().map(|vsz| vsz + size))
        })
    }
}

impl ByteSize for LongVarInt {
    fn size_in_bytes(&self) -> Result<usize> {
        let mut bytes = 1;
        let mut v = Self::en_zigzag(self.0);
        while v >= u64::from(CONTINUATION) {
            v >>= 7;
            bytes += 1;
        }
        Ok(bytes)
    }
}

impl From<i64> for LongVarInt {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl TryFrom<usize> for LongVarInt {
    type Error = Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let v = i64::try_from(value)?;
        Ok(Self(v))
    }
}

impl Serialize for LongVarInt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Self::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for LongVarInt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::deserialize(deserializer).map(Self)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct UnsignedVarInt(pub u32);

impl Deref for UnsignedVarInt {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Encode for UnsignedVarInt {
    fn encode(&self) -> Result<Bytes> {
        let mut encoded = BytesMut::new();

        let mut v = self.0;

        while v >= u32::from(CONTINUATION) {
            encoded.put_u8(v as u8 | CONTINUATION);
            v >>= 7;
        }

        encoded.put_u8(v as u8);

        Ok(Bytes::from(encoded))
    }
}

impl UnsignedVarInt {
    #[instrument(skip(serializer), fields(serializer = type_name::<S>()))]
    pub fn serialize<S>(i: &u32, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut v = *i;
        let mut s = serializer.serialize_seq(None)?;

        while v >= u32::from(CONTINUATION) {
            debug!(v);

            #[allow(clippy::cast_possible_truncation)]
            s.serialize_element(&(v as u8 | CONTINUATION))?;
            v >>= 7;
        }

        debug!(v);

        #[allow(clippy::cast_possible_truncation)]
        s.serialize_element(&(v as u8))?;
        s.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u32, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = u32;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(stringify!(UnsignedVarInt))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                debug!("seq: {}", type_name_of_val(&seq));

                let mut shift = 0u8;
                let mut accumulator = 0u32;
                let mut done = false;

                while !done {
                    let byte = seq
                        .next_element::<u8>()?
                        .ok_or_else(|| de::Error::custom("byte"))?;

                    debug!("byte: {byte}");

                    let overflow = || de::Error::custom("overflow");

                    if byte & CONTINUATION == CONTINUATION {
                        accumulator = u32::from(byte & MASK)
                            .checked_shl(shift as u32)
                            .and_then(|intermediate| accumulator.checked_add(intermediate))
                            .ok_or_else(overflow)?;

                        shift = shift.checked_add(7).ok_or_else(overflow)?;
                    } else {
                        accumulator = u32::from(byte)
                            .checked_shl(shift as u32)
                            .and_then(|intermediate| accumulator.checked_add(intermediate))
                            .ok_or_else(overflow)?;

                        done = true;
                    }
                }

                debug!("accumulator: {accumulator}");

                Ok(accumulator)
            }
        }

        debug!("deserializer: {}", type_name_of_val(&deserializer));

        deserializer.deserialize_seq(V)
    }

    pub fn size_inclusive(bs: &impl ByteSize) -> Result<usize> {
        bs.size_in_bytes().and_then(|size| {
            u32::try_from(size)
                .map_err(Into::into)
                .map(Self)
                .and_then(|vsz| vsz.size_in_bytes().map(|vsz| vsz + size))
        })
    }
}

impl ByteSize for UnsignedVarInt {
    fn size_in_bytes(&self) -> Result<usize> {
        let mut bytes = 1;
        let mut v = self.0;
        while v >= u32::from(CONTINUATION) {
            v >>= 7;
            bytes += 1;
        }
        Ok(bytes)
    }
}

impl From<u32> for UnsignedVarInt {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<UnsignedVarInt> for u32 {
    fn from(value: UnsignedVarInt) -> Self {
        value.0
    }
}

impl From<UnsignedVarInt> for usize {
    fn from(value: UnsignedVarInt) -> Self {
        value.0 as usize
    }
}

impl TryFrom<usize> for UnsignedVarInt {
    type Error = Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        let v = u32::try_from(value)?;
        Ok(Self(v))
    }
}

impl Serialize for UnsignedVarInt {
    #[instrument(skip_all, fields(serializer = type_name::<S>()))]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Self::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for UnsignedVarInt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::deserialize(deserializer).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ser::Encoder;

    // #[test]
    // fn serde_varint() -> Result<()> {
    //     check(&VarInt(-1))?;
    //     check(&VarInt(0))?;
    //     check(&VarInt(1))?;
    //     check(&VarInt(i32::MIN))?;
    //     check(&VarInt(i32::MAX))?;
    //     Ok(())
    // }

    // #[test]
    // fn serde_longvarint() -> Result<()> {
    //     check(&LongVarInt(-1))?;
    //     check(&LongVarInt(0))?;
    //     check(&LongVarInt(1))?;
    //     check(&LongVarInt(i64::from(i32::MIN)))?;
    //     check(&LongVarInt(i64::from(i32::MAX)))?;
    //     check(&LongVarInt(i64::MIN))?;
    //     check(&LongVarInt(i64::MAX))?;
    //     Ok(())
    // }

    // #[test]
    // fn serde_unsigned_varint() -> Result<()> {
    //     check(&UnsignedVarInt(0))?;
    //     check(&UnsignedVarInt(1))?;
    //     check(&UnsignedVarInt(u32::MAX))?;
    //     Ok(())
    // }

    // Untrusted record data: a varint with more continuation bytes than its
    // type can hold must be an error, not an arithmetic overflow panic.
    fn overlong(continuation_bytes: usize) -> std::io::Cursor<Vec<u8>> {
        let mut encoded = vec![0xffu8; continuation_bytes];
        encoded.push(0x01);
        std::io::Cursor::new(encoded)
    }

    #[test]
    fn deserialize_overlong_varint_is_an_error() {
        let mut reader = overlong(6);
        let mut decoder = crate::de::Decoder::new(&mut reader);
        let err = VarInt::deserialize(&mut decoder).expect_err("six continuation bytes");
        assert!(
            matches!(&err, Error::Message(message) if message == "varint is too long"),
            "{err:?}"
        );
    }

    #[test]
    fn deserialize_overlong_long_varint_is_an_error() {
        let mut reader = overlong(11);
        let mut decoder = crate::de::Decoder::new(&mut reader);
        let err = LongVarInt::deserialize(&mut decoder).expect_err("eleven continuation bytes");
        assert!(
            matches!(&err, Error::Message(message) if message == "long varint is too long"),
            "{err:?}"
        );
    }

    fn deserialize_varint(encoded: &[u8]) -> Result<i32> {
        let mut reader = std::io::Cursor::new(encoded.to_vec());
        let mut decoder = crate::de::Decoder::new(&mut reader);
        VarInt::deserialize(&mut decoder)
    }

    fn deserialize_long_varint(encoded: &[u8]) -> Result<i64> {
        let mut reader = std::io::Cursor::new(encoded.to_vec());
        let mut decoder = crate::de::Decoder::new(&mut reader);
        LongVarInt::deserialize(&mut decoder)
    }

    // The extremes of each type take every byte the type allows: five for an
    // i32 and ten for an i64. One continuation byte more is an error.
    #[test]
    fn deserialize_five_byte_varint_extremes() -> Result<()> {
        let min = [0xff, 0xff, 0xff, 0xff, 0x0f];
        assert_eq!(&min, &VarInt(i32::MIN).encode()?[..]);
        assert_eq!(i32::MIN, deserialize_varint(&min)?);

        let max = [0xfe, 0xff, 0xff, 0xff, 0x0f];
        assert_eq!(&max, &VarInt(i32::MAX).encode()?[..]);
        assert_eq!(i32::MAX, deserialize_varint(&max)?);

        Ok(())
    }

    #[test]
    fn deserialize_six_byte_varint_is_an_error() {
        assert!(deserialize_varint(overlong(5).get_ref()).is_err());
    }

    #[test]
    fn deserialize_ten_byte_long_varint_extremes() -> Result<()> {
        let min = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
        assert_eq!(&min, &LongVarInt(i64::MIN).encode()?[..]);
        assert_eq!(i64::MIN, deserialize_long_varint(&min)?);

        let max = [0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01];
        assert_eq!(&max, &LongVarInt(i64::MAX).encode()?[..]);
        assert_eq!(i64::MAX, deserialize_long_varint(&max)?);

        Ok(())
    }

    #[test]
    fn deserialize_eleven_byte_long_varint_is_an_error() {
        assert!(deserialize_long_varint(overlong(10).get_ref()).is_err());
    }

    #[test]
    fn encode_decode() -> Result<()> {
        let expected = 1;

        let mut encoded = VarInt(expected).encode()?;
        assert_eq!(Bytes::from(vec![2u8]), encoded);

        let actual = VarInt::decode(&mut encoded).map(i32::from)?;
        assert_eq!(expected, actual);

        Ok(())
    }

    #[test]
    fn encode_varint_signed_one() -> Result<()> {
        let mut serializer = Encoder::new(BytesMut::new());
        let decoded = VarInt(1);
        decoded.serialize(&mut serializer)?;

        let encoded = Bytes::from(serializer);

        assert_eq!(&[2u8], &encoded[..]);
        Ok(())
    }

    #[test]
    fn encode_varint_unsigned_one() -> Result<()> {
        let mut serializer = Encoder::new(BytesMut::new());
        let decoded = UnsignedVarInt(1);
        decoded.serialize(&mut serializer)?;

        let encoded = Bytes::from(serializer);

        assert_eq!(&[1u8], &encoded[..]);
        Ok(())
    }

    #[test]
    fn encode_varint_signed_zero() -> Result<()> {
        let mut serializer = Encoder::new(BytesMut::new());
        let decoded = VarInt(0);
        decoded.serialize(&mut serializer)?;

        let encoded = Bytes::from(serializer);

        assert_eq!(&[0u8], &encoded[..]);
        Ok(())
    }

    // #[test]
    // fn decode_varint_minus_one() -> Result<()> {
    //     assert_eq!(-1, Decoder::decode::<VarInt>(&[1u8]).map(|v| v.0)?);
    //     Ok(())
    // }
}
