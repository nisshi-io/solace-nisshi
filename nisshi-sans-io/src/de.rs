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

use crate::{Error, Result, RootMessageMeta};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use nisshi_model::{FieldMeta, MessageMeta};
use serde::{
    Deserializer,
    de::{DeserializeSeed, EnumAccess, SeqAccess, VariantAccess, Visitor},
};
use std::{
    any::{type_name, type_name_of_val},
    collections::VecDeque,
    fmt,
    io::Read,
    str::from_utf8,
};
use tracing::{debug, warn};

const MESSAGE_MAX_SIZE: usize = 1024 * 1024 * 1024;

const PARSE_DEPTH: usize = 6;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Kind {
    Request,
    Response,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Container {
    Struct {
        name: &'static str,
        fields: &'static [&'static str],
    },

    Enum {
        name: &'static str,
        variants: &'static [&'static str],
    },
}

impl Container {
    fn name(&self) -> &'static str {
        match self {
            Self::Struct { name, .. } | Self::Enum { name, .. } => name,
        }
    }
}

/// Deserialize the Kafka protocol into the serde data model.
pub struct Decoder<'de> {
    reader: &'de mut dyn Read,
    containers: VecDeque<Container>,
    field: Option<&'static str>,
    kind: Option<Kind>,
    api_key: Option<i16>,
    api_version: Option<i16>,
    meta: Meta,
    length: Option<usize>,
    in_seq_of_primitive: bool,
    path: VecDeque<&'static str>,
    in_records: bool,
    message_max_size: Option<usize>,
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

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Meta {
    message: Option<&'static MessageMeta>,
    structures: Option<Vec<(&'static str, &'static FieldMeta)>>,
    field: Option<&'static FieldMeta>,
    parse: VecDeque<FieldLookup>,
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            message: Default::default(),
            structures: Default::default(),
            field: Default::default(),
            parse: VecDeque::with_capacity(PARSE_DEPTH),
        }
    }
}

impl<'de> Decoder<'de> {
    pub fn new(reader: &'de mut dyn Read) -> Self {
        Self {
            reader,
            containers: VecDeque::with_capacity(PARSE_DEPTH),
            field: None,
            kind: None,
            api_key: None,
            api_version: None,
            meta: Meta::default(),
            length: None,
            in_seq_of_primitive: false,
            path: VecDeque::with_capacity(PARSE_DEPTH),
            in_records: false,
            message_max_size: None,
        }
    }

    pub(crate) fn request(reader: &'de mut dyn Read, message_max_size: Option<usize>) -> Self {
        Self {
            reader,
            containers: VecDeque::with_capacity(PARSE_DEPTH),
            field: None,
            kind: Some(Kind::Request),
            api_key: None,
            api_version: None,
            meta: Meta::default(),
            length: None,
            in_seq_of_primitive: false,
            path: VecDeque::with_capacity(PARSE_DEPTH),
            in_records: false,
            message_max_size,
        }
    }

    /// Bounds every length-prefixed field this decoder reads to at most
    /// `message_max_size`, instead of the much larger `MESSAGE_MAX_SIZE`
    /// fallback. Callers that already hold the whole encoded value in memory
    /// should pass its length here.
    #[must_use]
    pub fn with_message_max_size(self, message_max_size: usize) -> Self {
        Self {
            message_max_size: Some(message_max_size),
            ..self
        }
    }

    pub(crate) fn response(
        reader: &'de mut dyn Read,
        api_key: i16,
        api_version: i16,
        message_max_size: Option<usize>,
    ) -> Self {
        Self {
            reader,
            containers: VecDeque::with_capacity(PARSE_DEPTH),
            field: None,
            kind: Some(Kind::Response),
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
                        structures: Some(meta.structures()),
                        parse,
                        ..Default::default()
                    }
                }),
            length: None,
            in_seq_of_primitive: false,
            path: VecDeque::with_capacity(PARSE_DEPTH),
            in_records: false,
            message_max_size,
        }
    }

    #[must_use]
    fn field_name(&self) -> String {
        self.path.iter().fold(String::new(), |acc, step| {
            if acc.is_empty() {
                (*step).to_string()
            } else {
                format!("{step}.{acc}")
            }
        })
    }

    fn in_header(&self) -> bool {
        self.containers
            .front()
            .is_some_and(|c| c.name() == "HeaderMezzanine")
    }

    #[must_use]
    fn is_flexible(&self) -> bool {
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

    #[must_use]
    fn is_valid(&self) -> bool {
        self.api_version.is_none_or(|api_version| {
            self.meta
                .field
                .is_none_or(|field| field.version.within(api_version))
        })
    }

    fn is_nullable(&self) -> bool {
        // `self.meta.field` describes the sequence field as a whole (e.g. a
        // nullable `[]string`), not its individual elements, which are never
        // themselves nullable. While iterating a sequence of primitives,
        // that field metadata is left in place for every element, so it
        // must not be mistaken for the element's own nullability.
        !self.in_seq_of_primitive
            && self.api_version.is_some_and(|api_version| {
                self.meta
                    .field
                    .is_some_and(|field| field.is_nullable(api_version))
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

    fn is_records(&self) -> bool {
        self.meta.field.is_some_and(|field| field.kind.is_records())
    }

    #[must_use]
    fn is_string(&self) -> bool {
        self.in_header() && self.field.is_some_and(|field| field == "client_id")
            || self.meta.field.is_some_and(|field| field.kind.is_string())
    }

    /// Refuse a length that says null (-1 for a non-flexible array or string, 0 for a flexible
    /// one) at a version where the field is not nullable.
    ///
    /// Kafka's generated reader throws for a negative length outside the field's
    /// `nullableVersions`, for every length-prefixed type, so the request fails to decode and the
    /// broker closes the connection with no response: a Metadata v0 request with a null list of
    /// topics, as one example ([MessageDataGenerator.java#L620-L626],
    /// [RequestContext.java#L111-L131]). The request header's client ID is the exception: it is
    /// nullable at every version and has no field metadata.
    ///
    /// This covers arrays and strings. Bytes and records are not checked here.
    ///
    /// [MessageDataGenerator.java#L620-L626]: https://github.com/apache/kafka/blob/3.9.1/generator/src/main/java/org/apache/kafka/message/MessageDataGenerator.java#L620-L626
    /// [RequestContext.java#L111-L131]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/RequestContext.java#L111-L131
    fn refuse_null_unless_nullable(&self) -> Result<()> {
        if self.in_header() || self.is_nullable() {
            return Ok(());
        }

        Err(Error::NullNotAllowed {
            field: self.field_name(),
            api_version: self.api_version.unwrap_or_default(),
        })
    }

    fn read_mandatory_non_nullable_length(&mut self) -> Result<()> {
        debug!(
            "header mezzanine: {}, nullable: {}, valid: {}",
            self.in_header(),
            self.is_nullable(),
            self.is_valid(),
        );

        if self.in_header() || self.is_nullable() || !self.is_valid() {
            debug!(
                "field: {} is not a mandatory non nullable length",
                self.field_name()
            );
            return Ok(());
        }

        debug!(
            "read_non_nullable_length, field: {}, flexible: {}, string: {}",
            self.field_name(),
            self.is_flexible(),
            self.is_string(),
        );

        if self.is_flexible() {
            self.length = self
                .unsigned_varint()
                .and_then(|length| length.checked_sub(1).ok_or(Error::Overflow))
                .and_then(|length| TryInto::try_into(length).map_err(Into::into))
                .map(Some)?;
        } else if self.is_string()
            || (self.in_seq_of_primitive
                && self.meta.field.is_some_and(|field| {
                    field
                        .kind
                        .kind_of_sequence()
                        .is_some_and(|sk| sk.is_string())
                }))
        {
            let mut buf = [0u8; 2];
            self.reader.read_exact(&mut buf)?;

            let length = i16::from_be_bytes(buf);
            debug!("length: {length}");
            self.length = Some(length.try_into()?);
        } else {
            let mut buf = [0u8; 4];
            self.reader.read_exact(&mut buf)?;

            let length = i32::from_be_bytes(buf);
            debug!("length: {length}");
            self.length = Some(length.try_into()?);
        }

        Ok(())
    }

    fn unsigned_varint(&mut self) -> Result<u32> {
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

        debug!(
            "field: {}, value: {v}:{}",
            self.field_name(),
            type_name::<V::Value>(),
        );
        visitor.visit_bool(v)
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8];
        self.reader.read_exact(&mut buf)?;
        let v = i8::from_be_bytes(buf);

        debug!(
            "field: {}, value: {v}:{}",
            self.field_name(),
            type_name::<V::Value>(),
        );
        visitor.visit_i8(v)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 2];
        self.reader.read_exact(&mut buf)?;
        let v = i16::from_be_bytes(buf);

        match (self.containers.front(), self.field) {
            (
                Some(Container::Struct {
                    name: "HeaderMezzanine",
                    ..
                }),
                Some("api_key"),
            ) => {
                _ = self.api_key.replace(v);

                if let Some(meta) = RootMessageMeta::messages().requests().get(&v) {
                    self.meta.message = Some(*meta);
                    self.meta.structures = Some(meta.structures());
                    self.meta.parse.push_front(meta.fields.into());
                }
            }

            (
                Some(Container::Struct {
                    name: "HeaderMezzanine",
                    ..
                }),
                Some("api_version"),
            ) => {
                _ = self.api_version.replace(v);
            }

            _ => (),
        }

        debug!(
            field = self.field_name(),
            value = v,
            type_name = type_name::<V::Value>(),
        );
        visitor.visit_i16(v)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;
        let v = i32::from_be_bytes(buf);

        debug!(
            field = self.field_name(),
            v,
            type_name = type_name::<V::Value>(),
        );
        visitor.visit_i32(v)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf)?;
        let v = i64::from_be_bytes(buf);

        debug!(
            "field: {}, value: {v}:{}",
            self.field_name(),
            type_name::<V::Value>(),
        );
        visitor.visit_i64(v)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8];
        self.reader.read_exact(&mut buf)?;
        let v = u8::from_be_bytes(buf);

        // A record key, value or header value decodes one u8 at a time, so we
        // log the field but not the value.
        debug!("field: {}, {}", self.field_name(), type_name::<V::Value>());
        visitor.visit_u8(v)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 2];
        self.reader.read_exact(&mut buf)?;
        let v = u16::from_be_bytes(buf);

        debug!(
            "field: {}, value: {v}:{}",
            self.field_name(),
            type_name::<V::Value>(),
        );
        visitor.visit_u16(v)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;
        let v = u32::from_be_bytes(buf);

        debug!(
            "field: {}, value: {v}:{}",
            self.field_name(),
            type_name::<V::Value>(),
        );
        visitor.visit_u32(v)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf)?;
        let v = u64::from_be_bytes(buf);

        debug!(
            "field: {}, value: {v}:{}",
            self.field_name(),
            type_name::<V::Value>(),
        );
        visitor.visit_u64(v)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf)?;
        let v = f32::from_be_bytes(buf);

        debug!(
            "field: {}, value: {v}:{}",
            self.field_name(),
            type_name::<V::Value>(),
        );
        visitor.visit_f32(v)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let mut buf = [0u8; 8];
        self.reader.read_exact(&mut buf)?;
        let v = f64::from_be_bytes(buf);

        debug!(
            "field: {}, value: {v}:{}",
            self.field_name(),
            type_name::<V::Value>(),
        );
        visitor.visit_f64(v)
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Some(field) = self.field {
            debug!("struct: {:?}, field: {}", self.containers.front(), field);
        }
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.read_mandatory_non_nullable_length()?;

        self.length
            .ok_or(Error::StringWithoutLength)
            .and_then(|length| {
                if length > self.message_max_size.unwrap_or(MESSAGE_MAX_SIZE) {
                    return Err(Error::MessageMaxSizeExceeded(length));
                }

                let mut buf = vec![0u8; length];
                self.reader.read_exact(&mut buf)?;
                from_utf8(buf.as_slice())
                    .map_err(Into::into)
                    .inspect(|v| {
                        debug!("visitor: {}, len: {}", type_name_of_val(&visitor), v.len())
                    })
                    .and_then(|s| visitor.visit_str(s))
            })
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!(
            field = self.field_name(),
            is_nullable = self.meta.field.is_some_and(|field| self
                .api_version
                .is_some_and(|api_version| field.is_nullable(api_version)))
        );

        if self.length.is_none() {
            self.read_mandatory_non_nullable_length()?;
        }

        if let Some(length) = self.length.take() {
            if length > self.message_max_size.unwrap_or(MESSAGE_MAX_SIZE) {
                return Err(Error::MessageMaxSizeExceeded(length));
            }

            let mut buf = vec![0u8; length];
            self.reader.read_exact(&mut buf)?;

            String::from_utf8(buf)
                .map_err(Into::into)
                .inspect(|v| {
                    debug!(field = self.field_name(), len = v.len());
                })
                .and_then(|s| visitor.visit_string(s))
        } else {
            Err(Error::StringWithoutLength)
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Some(field) = self.field {
            debug!("struct: {:?}, field: {}", self.containers.front(), field);
        }

        let length = if self.is_flexible() {
            self.unsigned_varint()
                .and_then(|length| usize::try_from(length).map_err(Into::into))
                .and_then(|length| length.checked_sub(1).ok_or(Error::Overflow))?
        } else {
            let mut buf = [0u8; 4];

            self.reader.read_exact(&mut buf)?;
            usize::try_from(u32::from_be_bytes(buf))?
        };

        if length > self.message_max_size.unwrap_or(MESSAGE_MAX_SIZE) {
            return Err(Error::MessageMaxSizeExceeded(length));
        }

        let mut buf = vec![0u8; length];
        self.reader.read_exact(&mut buf)?;
        visitor.visit_bytes(&buf[..])
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Some(field) = self.field {
            debug!("struct: {:?}, field: {}", self.containers.front(), field);
        }

        let length = if self.is_flexible() {
            self.unsigned_varint()
                .and_then(|length| usize::try_from(length).map_err(Into::into))
                .and_then(|length| length.checked_sub(1).ok_or(Error::Overflow))?
        } else {
            let mut buf = [0u8; 4];

            self.reader.read_exact(&mut buf)?;
            usize::try_from(u32::from_be_bytes(buf))?
        };

        if length > self.message_max_size.unwrap_or(MESSAGE_MAX_SIZE) {
            return Err(Error::MessageMaxSizeExceeded(length));
        }

        let mut buf = vec![0u8; length];
        self.reader.read_exact(&mut buf)?;
        visitor.visit_byte_buf(buf)
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!(
            field = self.field_name(),
            is_flexible = self.is_flexible(),
            is_valid = self.is_valid(),
            is_string = self.is_string(),
            is_sequence = self.is_sequence(),
            is_nullable = self.is_nullable(),
            is_structure = self.is_structure(),
            is_records = self.is_records(),
        );

        if self.is_valid() {
            if self.field.is_some_and(|field| field == "tag_buffer") {
                if self.is_flexible() {
                    self.length = None;
                    visitor.visit_some(self)
                } else {
                    visitor.visit_none()
                }
            } else if self.is_records() {
                let length = if self.is_flexible() {
                    self.unsigned_varint()
                        .and_then(|length| length.checked_sub(1).ok_or(Error::Overflow))?
                } else {
                    let mut buf = [0u8; 4];
                    self.reader.read_exact(&mut buf)?;

                    u32::from_be_bytes(buf)
                };

                debug!(length);

                if length == 0 {
                    visitor.visit_none()
                } else {
                    self.length = Some(length.try_into()?);
                    self.in_records = true;
                    visitor.visit_some(self)
                }
            } else if self.is_sequence() {
                if self.is_flexible() {
                    self.unsigned_varint().and_then(|length| {
                        if length == 0 {
                            self.refuse_null_unless_nullable()?;
                            self.length = None;
                            visitor.visit_none()
                        } else {
                            self.length = Some((length - 1).try_into()?);
                            visitor.visit_some(self)
                        }
                    })
                } else {
                    let mut buf = [0u8; 4];
                    self.reader.read_exact(&mut buf)?;

                    let length = i32::from_be_bytes(buf);
                    debug!(length);

                    if length == -1 {
                        self.refuse_null_unless_nullable()?;
                        self.length = None;
                        visitor.visit_none()
                    } else {
                        self.length = Some(length.try_into()?);
                        visitor.visit_some(self)
                    }
                }
            } else if self.is_string() {
                if self.is_flexible() {
                    self.unsigned_varint().and_then(|length| {
                        if length == 0 {
                            self.refuse_null_unless_nullable()?;
                            self.length = None;
                            visitor.visit_none()
                        } else {
                            self.length = Some((length - 1).try_into()?);
                            visitor.visit_some(self)
                        }
                    })
                } else {
                    let mut buf = [0u8; 2];
                    self.reader.read_exact(&mut buf)?;

                    let length = i16::from_be_bytes(buf);
                    debug!(length);

                    if length == -1 {
                        self.refuse_null_unless_nullable()?;
                        self.length = None;
                        visitor.visit_none()
                    } else {
                        self.length = Some(length.try_into()?);
                        visitor.visit_some(self)
                    }
                }
            } else if self.is_nullable() && self.is_structure() {
                let mut buf = [0u8; 1];
                self.reader.read_exact(&mut buf)?;

                if (buf[0] as i8) < 0 {
                    visitor.visit_none()
                } else {
                    self.length = None;
                    visitor.visit_some(self)
                }
            } else {
                self.length = None;
                visitor.visit_some(self)
            }
        } else {
            self.length = None;
            visitor.visit_none()
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!(
            visitor = type_name_of_val(&visitor),
            type_name = type_name::<V::Value>(),
        );
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
        if let Some(field) = self.field {
            debug!(r#struct = ?self.containers.front(), field);
        }

        debug!(name, visitor = type_name_of_val(&visitor));
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
        if let Some(field) = self.field {
            debug!(r#struct = ?self.containers.front(), field);
        }

        debug!(name, visitor = type_name_of_val(&visitor));
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!(
            visitor = type_name::<V>(),
            type_name = type_name::<V::Value>(),
            length = self.length,
            meta_field = self.meta.field.is_some(),
            is_seq_of_primitive = self.meta.field.is_some_and(|field| field
                .kind
                .kind_of_sequence()
                .is_some_and(|seq| seq.is_primitive())),
            is_records = self.is_records(),
        );

        self.in_seq_of_primitive = self.meta.field.is_some_and(|field| {
            field
                .kind
                .kind_of_sequence()
                .is_some_and(|seq| seq.is_primitive())
        });

        match self.length.take() {
            Some(size_in_bytes) if self.in_records => {
                debug!(size_in_bytes);

                if size_in_bytes > self.message_max_size.unwrap_or(MESSAGE_MAX_SIZE) {
                    return Err(Error::MessageMaxSizeExceeded(size_in_bytes));
                }

                let mut buf = vec![0u8; size_in_bytes];
                self.reader.read_exact(&mut buf)?;
                let outcome = visitor.visit_seq(Batch::new(Bytes::from(buf)));
                self.in_seq_of_primitive = false;
                self.in_records = false;
                outcome
            }

            otherwise => {
                let outcome = visitor.visit_seq(Seq::new(self, otherwise));
                self.in_seq_of_primitive = false;
                outcome
            }
        }
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Some(field) = self.field {
            debug!(r#struct = ?self.containers.front(), field);
        }

        debug!(len, visitor = type_name_of_val(&visitor));
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
        debug!(name, len, visitor = type_name_of_val(&visitor));
        Err(Error::UnexpectedType(format!(
            "{name}:{len}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!(visitor = type_name_of_val(&visitor));
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
        debug!(r#struct = name, ?fields);

        self.containers
            .push_front(Container::Struct { name, fields });

        let outcome = if let Some(mm) = self.meta.message {
            if let Some((_, fm)) = self
                .meta
                .structures
                .as_deref()
                .unwrap_or_default()
                .iter()
                .find(|(found, _)| name == *found)
            {
                debug!(r#struct = name);

                _ = self.meta.field.replace(*fm);
                self.meta.parse.push_front(fm.fields.into());
                let outcome = visitor.visit_seq(Struct::new(self, name, fields));
                _ = self.meta.parse.pop_front();
                _ = self.meta.field.take();
                outcome
            } else if name.ends_with("Request") || name.ends_with("Response") {
                self.meta.parse.push_front(mm.fields.into());
                let outcome = visitor.visit_seq(Struct::new(self, name, fields));
                _ = self.meta.parse.pop_front();
                outcome
            } else {
                if !["Frame", "HeaderMezzanine"].contains(&name) {
                    warn!("deserialize_struct, no field meta for struct, name: {name}");
                }
                _ = self.meta.field.take();
                self.meta.parse.push_front(FieldLookup(&[]));
                let outcome = visitor.visit_seq(Struct::new(self, name, fields));
                _ = self.meta.parse.pop_front();
                outcome
            }
        } else {
            if !["Frame", "HeaderMezzanine"].contains(&name) {
                debug!("deserialize_struct, no message meta for struct, name: {name}");
            }

            visitor.visit_seq(Struct::new(self, name, fields))
        };

        _ = self.containers.pop_front();

        outcome
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
        debug!(r#enum = name);

        self.containers
            .push_front(Container::Enum { name, variants });

        let outcome = visitor.visit_enum(Enum::new(self, name));

        _ = self.containers.pop_front();
        outcome
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!(
            front = ?self.containers.front().map(Container::name),
            message_name = self.meta.message.map(|message| message.name)
        );

        visitor.visit_str(match (self.containers.front(), self.meta.message) {
            (
                Some(Container::Enum {
                    name: "HeaderMezzanine",
                    ..
                }),
                _,
            ) if self.kind.is_some_and(|kind| kind == Kind::Request) => Ok("Request"),

            (
                Some(Container::Enum {
                    name: "HeaderMezzanine",
                    ..
                }),
                _,
            ) if self.kind.is_some_and(|kind| kind == Kind::Response) => Ok("Response"),

            (Some(Container::Enum { name: "Body", .. }), Some(meta)) => Ok(meta.name),

            (_, None) => Err(Error::UnknownContainer),

            container => Err(Error::UnexpectedType(format!(
                "{container:?}:{}",
                type_name::<V::Value>()
            ))),
        }?)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }
}

struct Batch {
    encoded: Bytes,
}

impl Batch {
    fn new(encoded: Bytes) -> Self {
        Self { encoded }
    }
}

impl<'de> SeqAccess<'de> for Batch {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        debug!(
            seed = type_name::<T>(),
            value = type_name::<T::Value>(),
            encoded = self.encoded.len()
        );

        if self.encoded.has_remaining() {
            let base_offset = self.encoded.try_get_i64()?;
            let batch_length = self.encoded.try_get_i32()?;
            debug!(base_offset, batch_length);

            let batch_length_usize = usize::try_from(batch_length).map_err(|_| Error::Overflow)?;

            if batch_length_usize > self.encoded.len() {
                return Err(Error::Overflow);
            }

            let mut batch = BytesMut::with_capacity(batch_length_usize);
            batch.put_i64(base_offset);
            batch.put_i32(batch_length);
            batch.put(self.encoded.split_to(batch_length_usize));

            let decoder = BatchDecoder {
                encoded: batch.freeze(),
            };

            seed.deserialize(decoder).map(Some)
        } else {
            Ok(None)
        }
    }
}

pub struct BatchDecoder {
    encoded: Bytes,
}

impl BatchDecoder {
    pub fn new(encoded: Bytes) -> Self {
        Self { encoded }
    }
}

impl<'de> Deserializer<'de> for BatchDecoder {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_bool<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_i8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_i16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_i32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_i64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_u8<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_u16<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_u32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_u64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_f32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_f64<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_char<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_str<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_string<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_bytes<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_bytes(&self.encoded[..])
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_byte_buf(Vec::from(self.encoded))
    }

    fn deserialize_option<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_unit<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
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
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{name}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_seq<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_tuple<V>(
        self,
        len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{len}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{name}:{len}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_map<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
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
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{name}:{fields:?}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{name}:{variants:?}:{}",
            type_name::<V::Value>()
        )))
    }

    fn deserialize_identifier<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(type_name::<V::Value>().into()))
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
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
    name: &'static str,
    fields: &'static [&'static str],
    index: usize,
}

impl<'de, 'a> Struct<'de, 'a> {
    fn new(de: &'a mut Decoder<'de>, name: &'static str, fields: &'static [&'static str]) -> Self {
        Self {
            de,
            name,
            fields,
            index: 0,
        }
    }
}

impl<'de> SeqAccess<'de> for Struct<'de, '_> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        let field = self.fields[self.index];
        self.de.field = Some(field);
        self.index += 1;

        if let Some(fl) = self.de.meta.parse.front() {
            self.de.meta.field = fl.field(field);
        }

        debug!(
            struct = self.name,
            field,
            is_flexible = self.de.is_flexible(),
            type_name = type_name::<T::Value>(),
            meta_field = self.de.meta.field.is_some(),
            is_records = self.de.is_records(),
        );

        self.de.path.push_front(field);
        let outcome = seed.deserialize(&mut *self.de).map(Some);
        _ = self.de.path.pop_front();
        outcome
    }
}

#[derive(Debug)]
struct Enum<'de, 'a> {
    de: &'a mut Decoder<'de>,
    name: &'static str,
}

impl<'de, 'a> Enum<'de, 'a> {
    fn new(de: &'a mut Decoder<'de>, name: &'static str) -> Self {
        Self { de, name }
    }
}

impl<'de> EnumAccess<'de> for Enum<'de, '_> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let val = seed.deserialize(&mut *self.de)?;
        Ok((val, self))
    }
}

impl<'de> VariantAccess<'de> for Enum<'de, '_> {
    type Error = Error;

    fn unit_variant(self) -> Result<(), Self::Error> {
        Err(Error::Message(String::from("expecting string")))
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        debug!(name = self.name, seed = type_name_of_val(&seed));
        seed.deserialize(&mut *self.de)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Err(Error::UnexpectedType(format!(
            "{len}:{}",
            type_name::<V::Value>()
        )))
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        debug!(
            name = self.name,
            ?fields,
            visitor = type_name_of_val(&visitor)
        );
        Deserializer::deserialize_struct(self.de, self.name, fields, visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::marker::PhantomData;

    /// A negative `batch_length` must be rejected before it sizes an
    /// allocation. Cast straight to `usize` it becomes `usize::MAX`, and
    /// `BytesMut::with_capacity` panicked with "capacity overflow".
    #[test]
    fn negative_batch_length_returns_err_not_huge_allocation() {
        let mut encoded = BytesMut::new();
        encoded.put_i64(0); // base_offset
        encoded.put_i32(-1); // batch_length: a wire lie
        let mut seq = Batch::new(encoded.freeze());

        let result = seq.next_element_seed(PhantomData::<crate::record::deflated::Batch>);
        assert!(result.is_err());
    }

    /// A [`Visitor`] that is never actually invoked: every test below expects
    /// the size check to short-circuit before the visitor would be called, so
    /// only `expecting` needs an implementation.
    struct UnreachableVisitor;

    impl<'de> Visitor<'de> for UnreachableVisitor {
        type Value = ();

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "a value (this visitor should never be invoked)")
        }
    }

    /// `deserialize_str` had no `message_max_size` check at all: a claimed
    /// length over the limit must be rejected before the body is read, the
    /// same way `deserialize_string` already does.
    #[test]
    fn deserialize_str_rejects_length_over_message_max_size() {
        let declared_length = 1_000_000i32;
        let mut encoded: Vec<u8> = declared_length.to_be_bytes().to_vec();
        let mut reader: &[u8] = &mut encoded;
        let mut decoder = Decoder::request(&mut reader, Some(10));

        let err = Deserializer::deserialize_str(&mut decoder, UnreachableVisitor)
            .expect_err("a length over message_max_size must be rejected");

        assert!(
            matches!(err, Error::MessageMaxSizeExceeded(length) if length == declared_length as usize),
            "{err:?}"
        );
    }

    /// `deserialize_bytes` had no `message_max_size` check on either the
    /// flexible or non-flexible length-read branch, unlike its sibling
    /// `deserialize_byte_buf`.
    #[test]
    fn deserialize_bytes_rejects_length_over_message_max_size() {
        let declared_length = 1_000_000i32;
        let mut encoded: Vec<u8> = declared_length.to_be_bytes().to_vec();
        let mut reader: &[u8] = &mut encoded;
        let mut decoder = Decoder::request(&mut reader, Some(10));

        let err = Deserializer::deserialize_bytes(&mut decoder, UnreachableVisitor)
            .expect_err("a length over message_max_size must be rejected");

        assert!(
            matches!(err, Error::MessageMaxSizeExceeded(length) if length == declared_length as usize),
            "{err:?}"
        );
    }

    /// The flexible length-read branch of `deserialize_bytes` must also be
    /// rejected over `message_max_size`. `ApiVersionsResponse` (api key 18)
    /// is flexible from v3, so this decoder reads an unsigned varint length.
    #[test]
    fn deserialize_bytes_flexible_rejects_length_over_message_max_size() {
        // Unsigned varint 1_000_001: the flexible encoding of a length of
        // 1_000_000, because the wire value is the length plus one.
        let mut encoded: &[u8] = &[0xc1, 0x84, 0x3d];
        let mut decoder = Decoder::response(&mut encoded, 18, 3, None);
        decoder.message_max_size = Some(10);

        let err = Deserializer::deserialize_bytes(&mut decoder, UnreachableVisitor)
            .expect_err("a length over message_max_size must be rejected");

        assert!(
            matches!(err, Error::MessageMaxSizeExceeded(1_000_000)),
            "{err:?}"
        );
    }

    /// `with_message_max_size` must bound a decoder built with
    /// `Decoder::new`, which otherwise falls back to `MESSAGE_MAX_SIZE`.
    #[test]
    fn with_message_max_size_bounds_a_new_decoder() {
        let declared_length = 1_000_000i32;
        let encoded = declared_length.to_be_bytes();
        let mut reader: &[u8] = &encoded;
        let mut decoder = Decoder::new(&mut reader).with_message_max_size(10);

        let err = Deserializer::deserialize_bytes(&mut decoder, UnreachableVisitor)
            .expect_err("a length over message_max_size must be rejected");

        std::assert_matches!(err, Error::MessageMaxSizeExceeded(1_000_000));
    }

    /// A flexible length prefix of `0` must be rejected with
    /// `Error::Overflow`. Subtracting one from it underflows, and the wrapped
    /// value would size the allocation.
    #[test]
    fn deserialize_bytes_flexible_zero_length_does_not_underflow() {
        // Unsigned varint 0: one byte without the continuation bit.
        let mut encoded: &[u8] = &[0];
        // ApiVersionsResponse (api key 18) is flexible from v3.
        let mut decoder = Decoder::response(&mut encoded, 18, 3, None);

        let err = Deserializer::deserialize_bytes(&mut decoder, UnreachableVisitor)
            .expect_err("a zero flexible length must be rejected");

        assert!(matches!(err, Error::Overflow), "{err:?}");
    }
}
