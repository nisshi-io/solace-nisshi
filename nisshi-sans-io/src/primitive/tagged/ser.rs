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
    Serialize, Serializer,
    ser::{
        SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
};
use std::{
    any::{type_name, type_name_of_val},
    fmt,
    io::Write,
};
use tracing::{debug, instrument};

pub(crate) struct Encoder<'a> {
    writer: &'a mut dyn Write,
}

impl fmt::Debug for Encoder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Self)).finish()
    }
}

impl<'a> Encoder<'a> {
    pub(crate) fn new(writer: &'a mut dyn Write) -> Self {
        Self { writer }
    }

    #[instrument(skip_all, fields(decoded = type_name::<T>()))]
    pub(crate) fn encode<T>(decoded: &T) -> Result<Vec<u8>>
    where
        T: Serialize,
    {
        let mut encoded = Vec::with_capacity(512);
        let mut serializer = Encoder::new(&mut encoded);
        decoded.serialize(&mut serializer)?;
        Ok(encoded)
    }

    fn unsigned_varint(&mut self, mut v: u32) -> Result<()> {
        const CONTINUATION: u8 = 0b1000_0000;

        while v >= u32::from(CONTINUATION) {
            #[allow(clippy::cast_possible_truncation)]
            self.writer.write_all(&[(v as u8 | CONTINUATION)])?;
            v >>= 7;
        }

        #[allow(clippy::cast_possible_truncation)]
        self.writer.write_all(&[(v as u8)])?;
        Ok(())
    }
}

impl Serializer for &mut Encoder<'_> {
    type Ok = ();

    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    #[instrument(skip(self))]
    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        let buf: [u8; 1] = [u8::from(v); 1];
        self.writer.write_all(&buf).map_err(Into::into)
    }

    #[instrument(skip(self))]
    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    #[instrument(skip(self))]
    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    #[instrument(skip(self))]
    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        debug!(?v);

        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    #[instrument(skip(self))]
    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    #[instrument(skip(self))]
    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    #[instrument(skip(self))]
    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    #[instrument(skip(self))]
    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    #[instrument(skip(self))]
    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    #[instrument(skip(self))]
    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    #[instrument(skip(self))]
    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        let buf = v.to_be_bytes();
        self.writer.write_all(&buf).map_err(Into::into)
    }

    #[instrument(skip(self))]
    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!("{v:?}")))
    }

    #[instrument(skip_all, fields(len = v.len()))]
    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        (v.len() + 1)
            .try_into()
            .map_err(Into::into)
            .and_then(|len| self.unsigned_varint(len))?;
        self.writer.write_all(v.as_bytes()).map_err(Into::into)
    }

    #[instrument(skip_all, fields(len = v.len()))]
    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        (v.len() + 1)
            .try_into()
            .map_err(Into::into)
            .and_then(|len| self.unsigned_varint(len))?;
        self.writer.write_all(v).map_err(Into::into)
    }

    #[instrument(skip(self))]
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.unsigned_varint(0)
    }

    #[instrument(skip_all, fields(value = type_name::<T>()))]
    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        value.serialize(self)
    }

    #[instrument(skip(self))]
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!("{self:?}")))
    }

    #[instrument(skip(self))]
    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(name.into()))
    }

    #[instrument(skip(self))]
    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!(
            "{name}:{variant_index}:{variant}"
        )))
    }

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        value.serialize(self)
    }

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        let _ = value;
        Err(Error::UnexpectedType(format!(
            "{name}:{variant_index}:{variant}"
        )))
    }

    #[instrument(skip(self))]
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        if let Some(len) = len {
            (len + 1)
                .try_into()
                .map_err(Into::into)
                .and_then(|l| self.unsigned_varint(l))?;
        }
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(self)
    }
}

impl SerializeSeq for &mut Encoder<'_> {
    type Ok = ();

    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTuple for &mut Encoder<'_> {
    type Ok = ();

    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTupleStruct for &mut Encoder<'_> {
    type Ok = ();

    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        Err(Error::UnexpectedType(type_name_of_val(value).into()))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!("{self:?}")))
    }
}

impl SerializeTupleVariant for &mut Encoder<'_> {
    type Ok = ();

    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        Err(Error::UnexpectedType(type_name_of_val(value).into()))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!("{self:?}",)))
    }
}

impl SerializeMap for &mut Encoder<'_> {
    type Ok = ();

    type Error = Error;

    #[instrument(skip(self, key), fields(key = type_name::<T>()))]
    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        Err(Error::UnexpectedType(type_name_of_val(key).into()))
    }

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        Err(Error::UnexpectedType(type_name_of_val(value).into()))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(type_name_of_val(self).into()))
    }
}

impl SerializeStruct for &mut Encoder<'_> {
    type Ok = ();
    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        let _ = key;
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeStructVariant for &mut Encoder<'_> {
    type Ok = ();
    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        let _ = key;
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}
