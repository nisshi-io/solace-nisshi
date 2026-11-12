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

use std::{
    any::{type_name, type_name_of_val},
    collections::VecDeque,
    fmt,
};

use bytes::{BufMut, Bytes, BytesMut};
use nisshi_model::{FieldMeta, MessageMeta};
use serde::{
    Serialize, Serializer,
    ser::{
        SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
};
use tracing::{debug, instrument, warn};

use crate::{Encode, Error, Result, RootMessageMeta, primitive::varint::UnsignedVarInt};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Kind {
    Request,
    Response,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Container {
    Struct {
        name: &'static str,
        len: usize,
    },

    StructVariant {
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    },
}

impl Container {
    fn name(&self) -> String {
        match self {
            Self::Struct { name, .. } => (*name).to_string(),
            Self::StructVariant { name, variant, .. } => format!("{name}::{variant}"),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct FieldLookup(&'static [(&'static str, &'static FieldMeta)]);

impl From<&'static [(&'static str, &'static FieldMeta)]> for FieldLookup {
    fn from(value: &'static [(&'static str, &'static FieldMeta)]) -> Self {
        Self(value)
    }
}

impl FieldLookup {
    #[must_use]
    pub(crate) fn field(&self, name: &str) -> Option<&'static FieldMeta> {
        self.0
            .iter()
            .find(|(found, _)| name == *found)
            .map(|(_, meta)| *meta)
    }
}

const PARSE_DEPTH: usize = 6;

#[derive(Clone, Debug, Eq, PartialEq)]
struct Meta {
    message: Option<&'static MessageMeta>,
    field: Option<&'static FieldMeta>,
    parse: VecDeque<FieldLookup>,
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            message: Default::default(),
            field: Default::default(),
            parse: VecDeque::with_capacity(PARSE_DEPTH),
        }
    }
}

/// Serialize the serde data model into the Kafka protocol.
pub struct Encoder {
    working: BytesMut,
    containers: VecDeque<Container>,
    field: Option<&'static str>,
    kind: Option<Kind>,
    api_key: Option<i16>,
    api_version: Option<i16>,
    meta: Meta,
}

impl From<Encoder> for BytesMut {
    fn from(encoder: Encoder) -> Self {
        encoder.working
    }
}

impl From<Encoder> for Bytes {
    fn from(encoder: Encoder) -> Self {
        BytesMut::from(encoder).freeze()
    }
}

impl fmt::Debug for Encoder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Self)).finish()
    }
}

impl Encoder {
    pub(crate) fn request(working: BytesMut) -> Self {
        Self {
            working,
            containers: VecDeque::with_capacity(PARSE_DEPTH),
            kind: Some(Kind::Request),
            field: None,
            api_key: None,
            api_version: None,
            meta: Meta::default(),
        }
    }

    pub(crate) fn response(working: BytesMut, api_key: i16, api_version: i16) -> Self {
        Self {
            working,
            containers: VecDeque::with_capacity(PARSE_DEPTH),
            kind: Some(Kind::Response),
            field: None,
            api_key: Some(api_key),
            api_version: Some(api_version),
            meta: RootMessageMeta::messages()
                .responses()
                .get(&api_key)
                .map_or_else(Meta::default, |meta| {
                    let mut parse = VecDeque::with_capacity(PARSE_DEPTH);
                    parse.push_front(meta.fields.into());

                    Meta {
                        message: Some(*meta),
                        parse,
                        ..Default::default()
                    }
                }),
        }
    }

    pub fn new(working: BytesMut) -> Self {
        Self {
            working,
            containers: VecDeque::with_capacity(PARSE_DEPTH),
            kind: None,
            field: None,
            api_key: None,
            api_version: None,
            meta: Meta::default(),
        }
    }

    #[instrument(skip(self))]
    fn field_meta(&self, name: &str) -> Option<&'static FieldMeta> {
        debug!(
            parse_front = ?self.meta.parse.front().and_then(|front| front.field(name)),
            meta = ?self.meta.message.and_then(|mm| mm.field(name))
        );

        self.meta
            .parse
            .front()
            .and_then(|front| front.field(name))
            .or(self.meta.message.and_then(|mm| mm.field(name)))
    }

    #[allow(dead_code)]
    fn field_name(&self) -> String {
        self.containers.iter().fold(
            self.field.map_or_else(String::new, str::to_owned),
            |acc, container| {
                if acc.is_empty() {
                    container.name()
                } else {
                    format!("{}.{acc}", container.name())
                }
            },
        )
    }

    fn unsigned_varint(&mut self, mut v: u32) -> Result<()> {
        const CONTINUATION: u8 = 0b1000_0000;

        while v >= u32::from(CONTINUATION) {
            self.working.put_u8(v as u8 | CONTINUATION);
            v >>= 7;
        }

        self.working.put_u8(v as u8);
        Ok(())
    }

    fn in_header(&self) -> bool {
        matches!(
            self.containers.front(),
            Some(Container::StructVariant {
                name: "HeaderMezzanine",
                ..
            })
        )
    }

    #[must_use]
    fn is_flexible(&self) -> bool {
        debug!(
            "api_key: {:?}, api_version: {:?}, in_header: {}, is_client_id: {}",
            self.api_key,
            self.api_version,
            self.in_header(),
            self.field.is_some_and(|field| field == "client_id")
        );

        if self.in_header()
            && ((self.kind.is_some_and(|kind| kind == Kind::Request)
                && self.field.is_some_and(|field| field == "client_id"))
                || (self.kind.is_some_and(|kind| kind == Kind::Response)
                    && self.api_key.is_some_and(|api_key| api_key == 18)))
        {
            false
        } else {
            self.meta.message.is_some_and(|meta| {
                self.api_version
                    .is_some_and(|api_version| meta.is_flexible(api_version))
            })
        }
    }

    fn is_nullable(&self) -> bool {
        self.api_version.is_some_and(|api_version| {
            self.meta
                .field
                .is_some_and(|field| field.is_nullable(api_version))
        })
    }

    #[must_use]
    fn is_valid(&self) -> bool {
        self.api_version.is_some_and(|api_version| {
            self.meta
                .field
                .is_some_and(|field| field.version.within(api_version))
        })
    }

    #[must_use]
    fn is_sequence(&self) -> bool {
        self.meta
            .field
            .is_some_and(|field| field.kind.is_sequence())
    }

    #[must_use]
    fn is_structure(&self) -> bool {
        self.meta.field.is_some_and(|field| field.is_structure())
    }

    #[must_use]
    fn is_string(&self) -> bool {
        self.in_header() && self.field.is_some_and(|field| field == "client_id")
            || self.meta.field.is_some_and(|field| field.kind.is_string())
    }

    #[must_use]
    fn is_records(&self) -> bool {
        self.meta.field.is_some_and(|field| field.kind.is_records())
    }
}

impl Serializer for &mut Encoder {
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
        self.working.put_u8(u8::from(v));
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.working.put_i8(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        match (self.containers.front(), self.field) {
            (
                Some(Container::StructVariant {
                    name: "HeaderMezzanine",
                    variant: "Request",
                    ..
                }),
                Some("api_key"),
            ) => {
                _ = self.api_key.replace(v);

                if let Some(meta) = RootMessageMeta::messages().requests().get(&v) {
                    self.meta.message = Some(*meta);
                    self.meta.parse.push_front(meta.fields.into());
                }
            }

            (
                Some(Container::StructVariant {
                    name: "HeaderMezzanine",
                    variant: "Request",
                    ..
                }),
                Some("api_version"),
            ) => {
                _ = self.api_version.replace(v);
            }

            _ => (),
        }

        self.working.put_i16(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.working.put_i32(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        self.working.put_i64(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.working.put_u8(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.working.put_u16(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.working.put_u32(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        self.working.put_u64(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        self.working.put_f32(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        self.working.put_f64(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Err(Error::UnexpectedType(format!("{v}")))
    }

    #[instrument(skip_all, fields(len = v.len()))]
    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        if self.in_header()
            && self.kind.is_some_and(|kind| kind == Kind::Request)
            && self.field.is_some_and(|field| field == "client_id")
        {
            v.len()
                .try_into()
                .map_err(Into::into)
                .and_then(|len| self.serialize_i16(len))?;

            self.working.put_slice(v.as_bytes());
        } else if self.is_valid() {
            if self.is_flexible() {
                (v.len() + 1)
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|len| self.unsigned_varint(len))?;
            } else {
                v.len()
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|len| self.serialize_i16(len))?;
            }

            self.working.put_slice(v.as_bytes());
        }

        Ok(())
    }

    #[instrument(skip_all, fields(len = v.len()))]
    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        debug!(is_valid = self.is_valid(), is_flexible = self.is_flexible());

        if self.is_valid() {
            if self.is_flexible() {
                (v.len() + 1)
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|len| self.unsigned_varint(len))?;
            } else {
                v.len()
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|len| self.serialize_u32(len))?;
            }

            self.working.put_slice(v);
        }

        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        debug!(
            name = self.field_name(),
            is_valid = self.is_valid(),
            is_nullable = self.is_nullable(),
            is_structure = self.is_structure(),
            is_sequence = self.is_sequence(),
            is_flexible = self.is_flexible(),
        );

        if self.in_header()
            && self.kind.is_some_and(|kind| kind == Kind::Request)
            && self.field.is_some_and(|field| field == "client_id")
        {
            self.serialize_i16(-1)
        } else if self.is_valid() && self.is_records() {
            if self.is_flexible() {
                self.unsigned_varint(1)
            } else {
                self.serialize_i32(0)
            }
        } else if self.is_valid() && self.is_nullable() {
            if self.is_structure() && !self.is_sequence() {
                self.serialize_i8(-1)
            } else if self.is_flexible() {
                self.unsigned_varint(0)
            } else if self.is_sequence() {
                self.serialize_i32(-1)
            } else if self.is_string() {
                self.serialize_i16(-1)
            } else {
                Ok(())
            }
        } else if self.is_valid() && self.is_sequence() {
            // Every sequence field is `Option<Vec<T>>` regardless of
            // whether it's actually nullable on the wire (see build.rs's
            // `kind()`), so a *mandatory* array left at its `None` default
            // must still encode as an empty array, not be skipped outright
            // (which desyncs every field written after it). A genuinely
            // nullable sequence never reaches this branch: it's caught by
            // `is_nullable()` above and null is written instead.
            if self.is_flexible() {
                self.unsigned_varint(1)
            } else {
                self.serialize_i32(0)
            }
        } else if self.is_valid() && self.meta.field.is_some_and(|field| field.tag.is_none()) {
            // A scalar, string or struct that exists at this version, is not
            // nullable and is not a tagged field has no encoding for `None`.
            // Skipping it, as this branch used to, drops its bytes and shifts
            // every field after it: the receiver reads garbage with no error
            // here. Kafka's generated `write` also refuses a null in a
            // non-nullable string, bytes or struct field rather than writing
            // a default (`MessageDataGenerator`, "non-nullable field ... was
            // serialized as null"), and its primitives are never null because
            // each starts at its descriptor default. Rust has no such default
            // here, and one guessed for the caller may be wrong, so fail.
            // Tagged fields are encoded elsewhere and never reach this
            // serializer; the `tag` check is a guard.
            Err(Error::MandatoryFieldUnset {
                field: self.field_name(),
                // `is_valid` above is true only when the encoder knows the version.
                api_version: self.api_version.unwrap_or_default(),
            })
        } else {
            Ok(())
        }
    }

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        debug!(
            is_tag_buffer = self.field.is_some_and(|field| field == "tag_buffer"),
            is_flexible = self.is_flexible(),
            is_records = self.is_records(),
        );

        if self.field.is_some_and(|field| field == "tag_buffer") && !self.is_flexible() {
            Ok(())
        } else if self.is_records() {
            if self.working.len() > self.working.capacity() {
                warn!(
                    working_capacity = self.working.capacity(),
                    working_len = self.working.len()
                );
            } else {
                debug!(
                    working_capacity = self.working.capacity(),
                    working_len = self.working.len()
                );
            }

            let records = {
                let records = self.working.split_off(self.working.len());
                debug!(
                    records_capacity = records.capacity(),
                    records_len = records.len(),
                );

                let mut e = RecordBatchEncoder::new(records);
                value.serialize(&mut e)?;
                BytesMut::from(e)
            };

            debug!(
                working_capacity = self.working.capacity(),
                working_len = self.working.len(),
                records_capacity = records.capacity(),
                records_len = records.len(),
            );

            let length = u32::try_from(records.len())?;

            if self.is_flexible() {
                let encoded = UnsignedVarInt(length + 1).encode()?;
                self.working.put(encoded);
            } else {
                self.working.put_u32(length);
            }

            debug!(
                working_capacity = self.working.capacity(),
                working_len = self.working.len(),
            );

            self.working.unsplit(records);

            debug!(
                working_capacity = self.working.capacity(),
                working_len = self.working.len(),
            );

            Ok(())
        } else if self.is_valid()
            && self.is_nullable()
            && self.is_structure()
            && !self.is_sequence()
        {
            // KIP-893 requires a presence marker before a non-null
            // nullable struct's own fields too, mirroring the -1 marker
            // `serialize_none` already writes unconditionally for this same
            // field shape when the value is null. This branch used to fall
            // straight through to the plain `value.serialize(self)` below
            // with no marker at all, desyncing every byte written after it.
            self.serialize_i8(1)?;

            value.serialize(self)
        } else {
            value.serialize(self)
        }
    }

    #[instrument(skip_all)]
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
        value.serialize(self)
    }

    #[instrument(skip(self))]
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        if self.is_valid()
            && let Some(len) = len
        {
            if self.is_flexible() {
                (len + 1)
                    .try_into()
                    .map_err(Into::into)
                    .and_then(|l| self.unsigned_varint(l))?;
            } else {
                len.try_into()
                    .map_err(Into::into)
                    .and_then(|l| self.serialize_i32(l))?;
            }
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
        self.containers.push_front(Container::Struct { name, len });

        if let Some(fm) = self.field_meta(name) {
            self.meta.field = Some(fm);
            self.meta.parse.push_front(fm.fields.into());
        }

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
        self.containers.push_front(Container::StructVariant {
            name,
            variant_index,
            variant,
            len,
        });

        Ok(self)
    }
}

impl SerializeSeq for &mut Encoder {
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

impl SerializeTuple for &mut Encoder {
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

impl SerializeTupleStruct for &mut Encoder {
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

impl SerializeTupleVariant for &mut Encoder {
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

impl SerializeMap for &mut Encoder {
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
        Err(Error::UnexpectedType(format!("{self:?}")))
    }
}

impl SerializeStruct for &mut Encoder {
    type Ok = ();
    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        _ = self.field.replace(key);

        if let Some(fm) = self.field_meta(key) {
            debug!(field = self.field_name(), meta = ?fm, is_valid = self.is_valid());

            _ = self.meta.field.replace(fm);
            self.meta.parse.push_front(fm.fields.into());
            let outcome = if self.is_valid() {
                value.serialize(&mut **self)
            } else {
                Ok(())
            };
            _ = self.meta.parse.pop_front();
            _ = self.meta.field.take();
            outcome
        } else {
            debug!(field = self.field_name());

            _ = self.meta.field.take();
            self.meta.parse.push_front(FieldLookup::default());
            let outcome = value.serialize(&mut **self);
            _ = self.meta.parse.pop_front();
            outcome
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        _ = self.containers.pop_front();
        Ok(())
    }
}

impl SerializeStructVariant for &mut Encoder {
    type Ok = ();
    type Error = Error;

    #[instrument(skip(self, value), fields(value = type_name::<T>()))]
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
        T: ?Sized,
    {
        _ = self.field.replace(key);

        if let Some(fm) = self.field_meta(key) {
            if self
                .api_version
                .is_some_and(|api_version| fm.version.within(api_version))
            {
                debug!(field_name = self.field_name(), meta = ?fm);

                _ = self.meta.field.replace(fm);
                self.meta.parse.push_front(fm.fields.into());
                let outcome = value.serialize(&mut **self);
                _ = self.meta.parse.pop_front();
                _ = self.meta.field.take();
                outcome
            } else {
                debug!(
                    field_name = self.field_name(),
                    api_version = self.api_version
                );
                Ok(())
            }
        } else {
            _ = self.meta.field.take();
            self.meta.parse.push_front(FieldLookup::default());
            let outcome = value.serialize(&mut **self);
            _ = self.meta.parse.pop_front();
            outcome
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        _ = self.containers.pop_front();
        Ok(())
    }
}

pub struct RecordBatchEncoder {
    working: BytesMut,
}

impl RecordBatchEncoder {
    pub fn new(working: BytesMut) -> Self {
        Self { working }
    }
}

impl From<RecordBatchEncoder> for BytesMut {
    fn from(value: RecordBatchEncoder) -> Self {
        value.working
    }
}

impl From<RecordBatchEncoder> for Bytes {
    fn from(value: RecordBatchEncoder) -> Self {
        BytesMut::from(value).into()
    }
}

impl Serializer for &mut RecordBatchEncoder {
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
    fn serialize_bool(self, v: bool) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_u8(u8::from(v));
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i8(self, v: i8) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_i8(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i16(self, v: i16) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_i16(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i32(self, v: i32) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_i32(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_i64(self, v: i64) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_i64(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u8(self, v: u8) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_u8(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u16(self, v: u16) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_u16(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u32(self, v: u32) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_u32(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_u64(self, v: u64) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_u64(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_f32(self, v: f32) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_f32(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_f64(self, v: f64) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put_f64(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_char(self, v: char) -> std::result::Result<Self::Ok, Self::Error> {
        unimplemented!("{v}")
    }

    #[instrument(skip_all, fields(len = v.len()))]
    fn serialize_str(self, v: &str) -> std::result::Result<Self::Ok, Self::Error> {
        unimplemented!("a string of {} bytes", v.len())
    }

    #[instrument(skip_all, fields(len = v.len()))]
    fn serialize_bytes(self, v: &[u8]) -> std::result::Result<Self::Ok, Self::Error> {
        self.working.put(v);
        Ok(())
    }

    #[instrument(skip(self))]
    fn serialize_none(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }

    #[instrument(skip_all, fields(value = type_name::<T>()))]
    fn serialize_some<T>(self, value: &T) -> std::result::Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!("{}", type_name_of_val(value))
    }

    #[instrument(skip_all)]
    fn serialize_unit(self) -> std::result::Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    #[instrument(skip(self))]
    fn serialize_unit_struct(
        self,
        name: &'static str,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        unimplemented!()
    }

    #[instrument(skip(self))]
    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        unimplemented!("{name}, {variant_index}, {variant}")
    }

    #[instrument(skip_all, fields(name, value = type_name::<T>()))]
    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> std::result::Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!("{}, {name}", type_name_of_val(value))
    }

    #[instrument(skip_all, fields(name, variant_index, variant, value = type_name::<T>()))]
    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> std::result::Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        unimplemented!(
            "{}, {name}, {variant_index}, {variant}",
            type_name_of_val(value)
        );
    }

    #[instrument(skip(self))]
    fn serialize_seq(
        self,
        len: Option<usize>,
    ) -> std::result::Result<Self::SerializeSeq, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_tuple(self, len: usize) -> std::result::Result<Self::SerializeTuple, Self::Error> {
        unimplemented!("{len}")
    }

    #[instrument(skip(self))]
    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeTupleStruct, Self::Error> {
        unimplemented!("{name}, {len}")
    }

    #[instrument(skip(self))]
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeTupleVariant, Self::Error> {
        unimplemented!("{name}, {variant_index}, {variant}, {len}")
    }

    #[instrument(skip(self))]
    fn serialize_map(
        self,
        len: Option<usize>,
    ) -> std::result::Result<Self::SerializeMap, Self::Error> {
        unimplemented!("{len:?}")
    }

    #[instrument(skip(self))]
    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeStruct, Self::Error> {
        Ok(self)
    }

    #[instrument(skip(self))]
    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeStructVariant, Self::Error> {
        Ok(self)
    }
}

impl SerializeSeq for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip_all, fields(type_name = type_name::<T>()))]
    fn serialize_element<T>(&mut self, value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTuple for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl SerializeTupleVariant for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl SerializeMap for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_value<T>(&mut self, value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl SerializeStruct for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip_all, fields(key, type_name = type_name::<T>()))]
    fn serialize_field<T>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl SerializeTupleStruct for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl SerializeStructVariant for &mut RecordBatchEncoder {
    type Ok = ();

    type Error = Error;

    #[instrument(skip_all, fields(key, type_name = type_name::<T>()))]
    fn serialize_field<T>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}
