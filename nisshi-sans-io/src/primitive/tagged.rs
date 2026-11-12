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

mod de;
mod ser;

use super::varint::UnsignedVarInt;
use crate::{ByteSize, Result};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{SeqAccess, Visitor},
    ser::SerializeSeq,
};
use std::{
    any::{type_name, type_name_of_val},
    fmt::Formatter,
    io::Cursor,
    iter::{chain, once},
    ops::Deref,
};
use tracing::{debug, instrument};

const MAXIMUM_TAGGED_FIELDS: usize = 128;

/// Caps the up-front allocation for a single tagged field's data. The real
/// length is an attacker-controlled varint read straight off the wire
/// (`fuzz_request_decode` found a ~3.5GB claim aborting the process via
/// `Vec::with_capacity` before a single data byte was read), so it must not
/// be trusted directly for a pre-allocation size. A legitimate payload
/// larger than this still decodes correctly, just via a few extra
/// reallocations as the vec grows one `push` at a time.
const MAXIMUM_PREALLOCATED_TAG_DATA: usize = 4096;

/// A vec for a tag field's data, sized for the length claimed on the wire but never more than
/// `MAXIMUM_PREALLOCATED_TAG_DATA` up front.
///
/// The decoder must get its vec only from here: the claim is an attacker-controlled varint,
/// and `Vec::with_capacity` on it directly is the allocation the cap exists to prevent.
fn with_claimed_capacity(claimed_length: usize) -> Vec<u8> {
    Vec::with_capacity(claimed_length.min(MAXIMUM_PREALLOCATED_TAG_DATA))
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TagField(pub u32, pub Vec<u8>);

impl TagField {
    pub(crate) fn tag(&self) -> u32 {
        self.0
    }

    pub(crate) fn data(&self) -> &[u8] {
        &self.1[..]
    }

    #[instrument(skip(field))]
    pub fn encode(tag: u32, field: &impl Serialize) -> Result<Self> {
        ser::Encoder::encode(field).map(|encoded| Self(tag, encoded))
    }
}

impl ByteSize for TagField {
    fn size_in_bytes(&self) -> Result<usize> {
        [
            UnsignedVarInt(self.tag()),
            UnsignedVarInt::try_from(self.data().len())?,
        ]
        .iter()
        .try_fold(self.data().len(), |acc, uvi| {
            uvi.size_in_bytes().map(|size_in_bytes| acc + size_in_bytes)
        })
    }
}

impl Serialize for TagField {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        debug!(?self);

        let mut s = serializer.serialize_seq(None)?;

        s.serialize_element(&UnsignedVarInt::from(self.tag()))?;

        UnsignedVarInt::try_from(self.data().len())
            .map_err(|e| serde::ser::Error::custom(format!("length too big: {e:?}")))
            .and_then(|length| s.serialize_element(&length))?;

        for byte in self.data() {
            s.serialize_element(byte)?;
        }

        s.end()
    }
}

impl<'de> Deserialize<'de> for TagField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = TagField;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(stringify!(Tag))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                debug!("seq={}", type_name_of_val(&seq));

                let tag: u32 = seq
                    .next_element::<UnsignedVarInt>()?
                    .ok_or_else(|| serde::de::Error::custom("tag"))?
                    .into();

                let length: usize = seq
                    .next_element::<UnsignedVarInt>()?
                    .ok_or_else(|| serde::de::Error::custom("length"))?
                    .into();

                (0..length)
                    .try_fold(with_claimed_capacity(length), |mut acc, _| {
                        seq.next_element::<u8>()?
                            .ok_or_else(|| serde::de::Error::custom("byte"))
                            .map(|byte| {
                                acc.push(byte);
                                acc
                            })
                    })
                    .inspect(|data| debug!(?tag, ?data))
                    .map(|data| TagField(tag, data))
            }
        }

        debug!("deserializer={}", type_name_of_val(&deserializer));
        deserializer.deserialize_seq(V)
    }
}

impl From<(u32, Vec<u8>)> for TagField {
    fn from(value: (u32, Vec<u8>)) -> Self {
        Self(value.0, value.1)
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TagBuffer(pub Vec<TagField>);

impl TagBuffer {
    #[must_use]
    pub fn empty() -> Self {
        Self(vec![])
    }

    #[must_use]
    pub fn builder() -> TagBufferBuilder {
        TagBufferBuilder::default()
    }

    pub fn decode<T>(&self, tag: &u32) -> Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        debug!("tag={tag} T={}", type_name::<T>());

        self.0
            .iter()
            .find(|TagField(found, _)| found == tag)
            .map_or_else(
                || Ok(None),
                |TagField(_, encoded)| {
                    // `encoded` is already fully in memory (decoded earlier as
                    // part of the parent record), so its own length — not the
                    // 1GiB fallback — is the right bound for whatever it claims
                    // to contain.
                    let message_max_size = encoded.len();
                    let mut r = Cursor::new(encoded);
                    let mut decoder =
                        de::Decoder::new(&mut r).with_message_max_size(message_max_size);
                    T::deserialize(&mut decoder).map(Some)
                },
            )
    }

    pub fn encode(tags: &[(u32, impl Serialize)]) -> Result<Self> {
        tags.iter()
            .map(|(tag, field)| {
                ser::Encoder::encode(field)
                    .map(|encoded| TagField(*tag, encoded))
                    .inspect(|tag_field| debug!(?tag_field))
            })
            .collect::<Result<Vec<_>>>()
            .map(Self)
    }
}

impl Deref for TagBuffer {
    type Target = Vec<TagField>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TagBufferBuilder {
    tags: Vec<TagField>,
}

impl TagBufferBuilder {
    #[must_use]
    pub fn tag(mut self, tag: u32, data: Vec<u8>) -> Self {
        self.tags.push(TagField(tag, data));
        self
    }

    #[must_use]
    pub fn build(self) -> TagBuffer {
        TagBuffer(self.tags)
    }
}

impl From<Vec<TagField>> for TagBuffer {
    fn from(value: Vec<TagField>) -> Self {
        Self(value)
    }
}

impl ByteSize for TagBuffer {
    fn size_in_bytes(&self) -> Result<usize> {
        chain(
            once(UnsignedVarInt::try_from(self.0.len()).and_then(|uvi| uvi.size_in_bytes())),
            self.0.iter().map(|tag| tag.size_in_bytes()),
        )
        .collect::<Result<Vec<_>>>()
        .map(|length| length.iter().sum::<usize>())
    }
}

impl Serialize for TagBuffer {
    #[instrument(skip_all)]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_seq(None)?;

        UnsignedVarInt::try_from(self.0.len())
            .map_err(|e| serde::ser::Error::custom(format!("length too big: {e:?}")))
            .inspect(|length| debug!(?length))
            .and_then(|length| s.serialize_element(&length))?;

        for tagged_field in &self.0 {
            s.serialize_element(tagged_field)?;
        }

        s.end()
    }
}

impl<'de> Deserialize<'de> for TagBuffer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = TagBuffer;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(stringify!(TagBuffer))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                debug!("seq={}", type_name_of_val(&seq));

                let number_of_tagged_fields: usize = seq
                    .next_element::<UnsignedVarInt>()?
                    .ok_or_else(|| serde::de::Error::custom("tag"))?
                    .into();

                debug!(?number_of_tagged_fields);

                if number_of_tagged_fields > MAXIMUM_TAGGED_FIELDS {
                    return Err(serde::de::Error::custom(format!(
                        "maximum tagged fields exceeded {number_of_tagged_fields}"
                    )));
                }

                (0..number_of_tagged_fields)
                    .try_fold(Vec::with_capacity(number_of_tagged_fields), |mut acc, _| {
                        seq.next_element::<TagField>()?
                            .ok_or_else(|| serde::de::Error::custom("tagged field"))
                            .inspect(|tag| debug!(?tag))
                            .map(|tag| {
                                acc.push(tag);
                                acc
                            })
                    })
                    .map(TagBuffer)
            }
        }

        debug!("deserializer={}", type_name_of_val(&deserializer));
        deserializer.deserialize_seq(V)
    }
}

#[cfg(test)]
mod tests {
    use super::{de::Decoder, ser::Encoder, *};
    use crate::Error;

    #[ignore]
    #[test]
    fn empty() -> Result<()> {
        let expected = TagBuffer::builder().build();

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);

        expected.serialize(&mut encoder)?;

        let mut decoder = Decoder::new(&mut c);
        let actual = TagBuffer::deserialize(&mut decoder)?;

        assert_eq!(expected, actual);

        Ok(())
    }

    #[ignore]
    #[test]
    fn buffer_of1() -> Result<()> {
        let expected = TagBuffer::builder().tag(55, b"pqr".into()).build();

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);

        expected.serialize(&mut encoder)?;

        let mut decoder = Decoder::new(&mut c);
        let actual = TagBuffer::deserialize(&mut decoder)?;

        assert_eq!(expected, actual);

        Ok(())
    }

    #[ignore]
    #[test]
    fn buffer_of2() -> Result<()> {
        let expected = TagBuffer::builder()
            .tag(66, vec![5, 4, 3, 2, 1])
            .tag(88, b"abc".into())
            .build();

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);

        expected.serialize(&mut encoder)?;

        let mut decoder = Decoder::new(&mut c);
        let actual = TagBuffer::deserialize(&mut decoder)?;

        assert_eq!(expected, actual);

        Ok(())
    }

    #[ignore]
    #[test]
    fn api_versions_response_v3_000() -> Result<()> {
        let expected = TagBuffer::builder()
            .tag(
                0,
                vec![
                    2, 17, 109, 101, 116, 97, 100, 97, 116, 97, 46, 118, 101, 114, 115, 105, 111,
                    110, 0, 1, 0, 14, 0,
                ],
            )
            .tag(1, vec![0, 0, 0, 0, 0, 0, 0, 76])
            .tag(
                2,
                vec![
                    2, 17, 109, 101, 116, 97, 100, 97, 116, 97, 46, 118, 101, 114, 115, 105, 111,
                    110, 0, 14, 0, 14, 0,
                ],
            )
            .build();

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);

        expected.serialize(&mut encoder)?;

        let mut decoder = Decoder::new(&mut c);
        let actual = TagBuffer::deserialize(&mut decoder)?;

        assert_eq!(expected, actual);

        Ok(())
    }

    #[test]
    fn compact_string() -> Result<()> {
        #[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
        struct Example {
            value: String,
        }

        let expected = Example {
            value: "Hello World!".to_owned(),
        };

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);
        expected.serialize(&mut encoder)?;

        assert_eq!(
            encoded,
            vec![13, 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100, 33]
        );

        let mut c = Cursor::new(&mut encoded);

        let mut decoder = Decoder::new(&mut c);

        assert_eq!(expected, Example::deserialize(&mut decoder)?);

        Ok(())
    }

    #[test]
    fn compact_string_empty() -> Result<()> {
        #[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
        struct Example {
            value: String,
        }

        let expected = Example {
            value: "".to_owned(),
        };

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);
        expected.serialize(&mut encoder)?;

        assert_eq!(encoded, vec![1]);

        let mut c = Cursor::new(&mut encoded);

        let mut decoder = Decoder::new(&mut c);

        assert_eq!(expected, Example::deserialize(&mut decoder)?);

        Ok(())
    }

    /// Unlike a decode, which an overcommitting OS lets succeed even for a ~3.5GB
    /// `Vec::with_capacity`, this fails if the cap is lost: `usize::MAX` bytes can never be
    /// allocated, so an uncapped call panics.
    #[test]
    fn claimed_capacity_is_capped() {
        assert_eq!(0, with_claimed_capacity(0).capacity());
        assert!(with_claimed_capacity(10).capacity() >= 10);
        assert!(with_claimed_capacity(usize::MAX).capacity() <= MAXIMUM_PREALLOCATED_TAG_DATA * 2);
    }

    /// The decoder must allocate through `with_claimed_capacity`. A field one byte over the
    /// cap, decoded with an uncapped `Vec::with_capacity(length)`, would hold exactly its
    /// length; through the cap the vec starts at the cap and grows past the length as the
    /// last byte arrives. This fails if the decoder stops using the helper.
    #[test]
    fn decode_preallocates_no_more_than_the_cap() -> Result<()> {
        let length = MAXIMUM_PREALLOCATED_TAG_DATA + 1;
        let expected = TagField(42, vec![7; length]);

        let mut encoder = crate::ser::Encoder::new(bytes::BytesMut::new());
        expected.serialize(&mut encoder)?;
        let encoded = bytes::Bytes::from(encoder);

        let mut reader = Cursor::new(encoded);
        let actual = TagField::deserialize(&mut crate::de::Decoder::new(&mut reader))?;

        assert_eq!(expected, actual);
        assert!(
            actual.1.capacity() > length,
            "capacity {} is not above the length {length}, so the vec was allocated for the \
             claimed length up front",
            actual.1.capacity()
        );

        Ok(())
    }

    /// The up-front allocation for a tag field's data is capped at
    /// `MAXIMUM_PREALLOCATED_TAG_DATA`, but a legitimate field larger than that must still
    /// decode in full, the vec growing past the cap as bytes arrive.
    #[test]
    fn tag_field_larger_than_preallocation_cap_round_trips() -> Result<()> {
        let data: Vec<u8> = (0..MAXIMUM_PREALLOCATED_TAG_DATA * 3)
            .map(|i| u8::try_from(i % 251).expect("below 251"))
            .collect();
        let expected = TagField(42, data);

        let mut encoder = crate::ser::Encoder::new(bytes::BytesMut::new());
        expected.serialize(&mut encoder)?;
        let encoded = bytes::Bytes::from(encoder);

        let mut reader = Cursor::new(encoded);
        let actual = TagField::deserialize(&mut crate::de::Decoder::new(&mut reader))?;

        assert_eq!(expected, actual);

        Ok(())
    }

    /// `TagBuffer::decode` constructs its decoder from the tag field's own
    /// already-in-memory bytes, not a view onto remaining wire bytes, so the
    /// bound must be the tag field's own length. A claimed length far larger
    /// than the tag field's own encoded size, but still well under the 1GiB
    /// `MESSAGE_MAX_SIZE` fallback, must still be rejected rather than
    /// silently decoded up to that fallback.
    #[test]
    fn decode_rejects_tagged_field_claiming_more_than_its_own_encoded_size() {
        let claimed_length: u32 = 1_000_000;

        // The compact-string length prefix alone (claimed_length + 1, per the
        // compact-string convention), with no string bytes behind it at all:
        // a tiny tag field claiming a length far larger than itself.
        let mut value = claimed_length + 1;
        let mut raw = Vec::new();
        loop {
            let byte = (value & 0x7F) as u8;
            value >>= 7;
            if value == 0 {
                raw.push(byte);
                break;
            }
            raw.push(byte | 0x80);
        }

        let tag_id = 7;
        let buffer = TagBuffer::builder().tag(tag_id, raw).build();

        let err = buffer.decode::<String>(&tag_id).expect_err(
            "a tagged field claiming far more than its own encoded size must be rejected",
        );

        assert!(
            matches!(err, Error::MessageMaxSizeExceeded(length) if length == claimed_length as usize),
            "{err:?}"
        );
    }

    #[test]
    fn compact_nullable_string_none() -> Result<()> {
        #[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
        struct Example {
            value: Option<String>,
        }

        let expected = Example { value: None };

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);
        expected.serialize(&mut encoder)?;

        assert_eq!(encoded, vec![0]);

        let mut c = Cursor::new(&mut encoded);

        let mut decoder = Decoder::new(&mut c);

        assert_eq!(expected, Example::deserialize(&mut decoder)?);

        Ok(())
    }

    #[test]
    fn compact_nullable_string_some() -> Result<()> {
        #[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
        struct Example {
            value: Option<String>,
        }

        let expected = Example {
            value: Some("Hello World!".to_owned()),
        };

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);
        expected.serialize(&mut encoder)?;

        assert_eq!(
            encoded,
            vec![13, 72, 101, 108, 108, 111, 32, 87, 111, 114, 108, 100, 33]
        );

        let mut c = Cursor::new(&mut encoded);

        let mut decoder = Decoder::new(&mut c);

        assert_eq!(expected, Example::deserialize(&mut decoder)?);

        Ok(())
    }

    #[test]
    fn compact_array_of() -> Result<()> {
        #[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
        struct Example {
            value: Vec<i32>,
        }

        let expected = Example {
            value: [12321, 54345, 78987].into(),
        };

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);
        expected.serialize(&mut encoder)?;

        assert_eq!(encoded, vec![4, 0, 0, 48, 33, 0, 0, 212, 73, 0, 1, 52, 139]);

        let mut c = Cursor::new(&mut encoded);

        let mut decoder = Decoder::new(&mut c);

        assert_eq!(expected, Example::deserialize(&mut decoder)?);

        Ok(())
    }

    #[test]
    fn compact_array_of_empty() -> Result<()> {
        #[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
        struct Example {
            value: Vec<i32>,
        }

        let expected = Example { value: [].into() };

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);
        expected.serialize(&mut encoder)?;

        assert_eq!(encoded, vec![1]);

        let mut c = Cursor::new(&mut encoded);

        let mut decoder = Decoder::new(&mut c);

        assert_eq!(expected, Example::deserialize(&mut decoder)?);

        Ok(())
    }

    #[test]
    fn array_of() -> Result<()> {
        #[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
        struct Wrapper {
            value: Vec<Example>,
        }

        #[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
        struct Example {
            node_id: i32,
            host: String,
            port: i32,
            rack: String,
        }

        let expected = Wrapper {
            value: vec![Example {
                node_id: 32123,
                host: "abc".to_owned(),
                port: 98789,
                rack: "pqr".to_owned(),
            }],
        };

        let mut encoded = vec![];
        let mut c = Cursor::new(&mut encoded);
        let mut encoder = Encoder::new(&mut c);
        expected.serialize(&mut encoder)?;

        assert_eq!(
            encoded,
            vec![
                2, 0, 0, 125, 123, 4, 97, 98, 99, 0, 1, 129, 229, 4, 112, 113, 114
            ]
        );

        let mut c = Cursor::new(&mut encoded);

        let mut decoder = Decoder::new(&mut c);

        assert_eq!(expected, Wrapper::deserialize(&mut decoder)?);

        Ok(())
    }
}
