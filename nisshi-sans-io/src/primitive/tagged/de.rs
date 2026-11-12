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

use crate::{Error, Result};
use serde::{
    Deserializer,
    de::{DeserializeSeed, SeqAccess, Visitor},
};
use std::{
    any::{type_name, type_name_of_val},
    fmt,
    io::Read,
};
use tracing::debug;

const MESSAGE_MAX_SIZE: usize = 1024 * 1024 * 1024;

pub struct Decoder<'de> {
    reader: &'de mut dyn Read,
    length: Option<usize>,
    message_max_size: Option<usize>,
}

impl<'de> Decoder<'de> {
    pub fn new(reader: &'de mut dyn Read) -> Self {
        Self {
            reader,
            length: None,
            message_max_size: None,
        }
    }

    /// Bounds every length-prefixed field this decoder reads to at most
    /// `message_max_size`, instead of the much larger `MESSAGE_MAX_SIZE`
    /// fallback. Callers that already know the exact size of the buffer
    /// they're decoding from (for example a tag field's own already-in-memory
    /// bytes) should pass that length here.
    #[must_use]
    pub fn with_message_max_size(self, message_max_size: usize) -> Self {
        Self {
            message_max_size: Some(message_max_size),
            ..self
        }
    }

    pub fn unsigned_varint(&mut self) -> Result<u32> {
        const CONTINUATION: u8 = 0b1000_0000;
        const MASK: u8 = 0b0111_1111;
        let mut shift = 0u8;
        let mut accumulator = 0u32;
        let mut done = false;

        let mut buf = [0u8; 1];

        while !done {
            self.reader.read_exact(&mut buf)?;

            if buf[0] & CONTINUATION == CONTINUATION {
                accumulator = u32::from(buf[0] & MASK)
                    .checked_shl(shift as u32)
                    .and_then(|intermediate| accumulator.checked_add(intermediate))
                    .ok_or(Error::Overflow)?;

                shift = shift.checked_add(7).ok_or(Error::Overflow)?;
            } else {
                accumulator = u32::from(buf[0])
                    .checked_shl(shift as u32)
                    .and_then(|intermediate| accumulator.checked_add(intermediate))
                    .ok_or(Error::Overflow)?;
                done = true;
            }
        }

        Ok(accumulator)
    }
}

impl fmt::Debug for Decoder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Self)).finish()
    }
}

impl<'de> Deserializer<'de> for &mut Decoder<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8];
        self.reader.read_exact(&mut buf)?;
        let v = buf[0] != 0;

        debug!("value: {v}:{}", type_name::<V::Value>(),);
        visitor.visit_bool(v)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8];
        self.reader.read_exact(&mut buf)?;
        let v = i8::from_be_bytes(buf);

        debug!("value: {v}:{}", type_name::<V::Value>(),);
        visitor.visit_i8(v)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 2];
        self.reader.read_exact(&mut buf)?;
        let v = i16::from_be_bytes(buf);

        debug!("value: {v}:{}", type_name::<V::Value>(),);
        visitor.visit_i16(v)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;
        let v = i32::from_be_bytes(buf);

        debug!("value: {v}:{}", type_name::<V::Value>(),);
        visitor.visit_i32(v)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf)?;
        let v = i64::from_be_bytes(buf);

        debug!("value: {v}:{}", type_name::<V::Value>(),);
        visitor.visit_i64(v)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8];
        self.reader.read_exact(&mut buf)?;
        let v = u8::from_be_bytes(buf);

        // A bytes value can decode one u8 at a time, so we don't log the value.
        debug!("{}", type_name::<V::Value>());
        visitor.visit_u8(v)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 2];
        self.reader.read_exact(&mut buf)?;
        let v = u16::from_be_bytes(buf);

        debug!("value: {v}:{}", type_name::<V::Value>(),);
        visitor.visit_u16(v)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;
        let v = u32::from_be_bytes(buf);

        debug!("value: {v}:{}", type_name::<V::Value>(),);
        visitor.visit_u32(v)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf)?;
        let v = u64::from_be_bytes(buf);

        debug!("value: {v}:{}", type_name::<V::Value>(),);
        visitor.visit_u64(v)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;
        let v = f32::from_be_bytes(buf);

        debug!("value: {v}:{}", type_name::<V::Value>(),);
        visitor.visit_f32(v)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf)?;
        let v = f64::from_be_bytes(buf);

        debug!("value: {v}:{}", type_name::<V::Value>(),);
        visitor.visit_f64(v)
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.length
            .take()
            .map_or_else(
                || {
                    self.unsigned_varint()
                        .and_then(|length| length.checked_sub(1).ok_or(Error::Overflow))
                        .and_then(|length| length.try_into().map_err(Into::into))
                },
                Ok,
            )
            .and_then(|length| {
                if length > self.message_max_size.unwrap_or(MESSAGE_MAX_SIZE) {
                    return Err(Error::MessageMaxSizeExceeded(length));
                }

                let mut buf = vec![0u8; length];
                self.reader.read_exact(&mut buf)?;
                std::str::from_utf8(buf.as_slice())
                    .map_err(Into::into)
                    .inspect(|v| debug!("len: {}:{}", v.len(), type_name::<V::Value>()))
                    .and_then(|s| visitor.visit_str(s))
            })
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.length
            .take()
            .map_or_else(
                || {
                    self.unsigned_varint()
                        .and_then(|length| length.checked_sub(1).ok_or(Error::Overflow))
                        .and_then(|length| length.try_into().map_err(Into::into))
                },
                Ok,
            )
            .and_then(|length| {
                if length > self.message_max_size.unwrap_or(MESSAGE_MAX_SIZE) {
                    return Err(Error::MessageMaxSizeExceeded(length));
                }

                let mut buf = vec![0u8; length];
                self.reader.read_exact(&mut buf)?;

                String::from_utf8(buf)
                    .map_err(Into::into)
                    .inspect(|v| debug!("len: {}:{}", v.len(), type_name::<V::Value>()))
                    .and_then(|s| visitor.visit_string(s))
            })
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.unsigned_varint()? {
            0 => visitor.visit_none(),

            length => {
                _ = self.length.replace((length - 1).try_into()?);
                visitor.visit_some(self)
            }
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{name}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let _ = name;
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.unsigned_varint()
            .and_then(|length| length.checked_sub(1).ok_or(Error::Overflow))
            .and_then(|length| length.try_into().map_err(Into::into))
            .and_then(|length| visitor.visit_seq(Seq::new(self, Some(length))))
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(Seq::new(self, Some(len)))
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{name}:{len}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!("name: {name}, fields: {fields:?}");
        visitor.visit_seq(Struct::new(self))
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{name}:{variants:?}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }
}

#[derive(Debug)]
struct Seq<'de, 'a> {
    de: &'a mut Decoder<'de>,
    length: Option<usize>,
}

impl<'de, 'a> Seq<'de, 'a> {
    fn new(de: &'a mut Decoder<'de>, length: Option<usize>) -> Self {
        Self { de, length }
    }
}

impl<'de> SeqAccess<'de> for Seq<'de, '_> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        debug!(
            "seq, next seed: {}, length: {:?}",
            type_name_of_val(&seed),
            self.length
        );

        match self.length {
            Some(0) => Ok(None),

            Some(length) => {
                _ = self.length.replace(length - 1);
                seed.deserialize(&mut *self.de).map(Some)
            }

            None => seed.deserialize(&mut *self.de).map(Some),
        }
    }
}

#[derive(Debug)]
struct Struct<'de, 'a> {
    de: &'a mut Decoder<'de>,
}

impl<'de, 'a> Struct<'de, 'a> {
    fn new(de: &'a mut Decoder<'de>) -> Self {
        Self { de }
    }
}

impl<'de> SeqAccess<'de> for Struct<'de, '_> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        debug!("seed: {}", type_name_of_val(&seed));
        seed.deserialize(&mut *self.de).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Encode, primitive::varint::UnsignedVarInt};
    use serde::de::IgnoredAny;
    use std::io::Cursor;

    /// `deserialize_str` rejects a declared length above the message size limit
    /// before it allocates. The test matches `MessageMaxSizeExceeded`, because
    /// without the guard the decoder still fails, with `Error::Io`, after it
    /// allocates.
    #[test]
    fn deserialize_str_length_exceeding_max_size_returns_err_not_huge_allocation() {
        // Tagged-field string lengths are varint-encoded as length + 1 (0
        // means null); encode MESSAGE_MAX_SIZE + 2 so the decoded length,
        // after the mandatory `- 1`, is MESSAGE_MAX_SIZE + 1 - just past
        // the guard, with no data behind it on the wire.
        let declared = u32::try_from(MESSAGE_MAX_SIZE).unwrap() + 2;
        let encoded = UnsignedVarInt(declared).encode().unwrap();

        let mut cursor = Cursor::new(encoded);
        let mut decoder = Decoder::new(&mut cursor);

        let result = (&mut decoder).deserialize_str(IgnoredAny);

        assert!(matches!(result, Err(Error::MessageMaxSizeExceeded(_))));
    }
}
