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

use std::{
    fmt::{self, Formatter},
    marker::PhantomData,
};

use bytes::Bytes;
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, SeqAccess, Visitor},
    ser::{self, SerializeSeq as _, SerializeStruct},
};
use tracing::debug;

#[derive(Clone, Eq, Hash, Debug, Ord, PartialEq, PartialOrd)]
pub(crate) struct MemberMetadata(ConsumerProtocolSubscription);

impl AsRef<i16> for MemberMetadata {
    fn as_ref(&self) -> &i16 {
        self.0.as_ref()
    }
}

impl From<MemberMetadata> for super::MemberMetadata {
    fn from(value: MemberMetadata) -> Self {
        let version = *value.0.as_ref();

        Self {
            version,
            subscription: value.0.into(),
        }
    }
}

impl From<super::MemberMetadata> for MemberMetadata {
    fn from(value: super::MemberMetadata) -> Self {
        Self(match value.version {
            0 => ConsumerProtocolSubscription::V0(ConsumerProtocolSubscriptionV0 {
                topics: value.subscription.topics.into(),
                user_data: value.subscription.user_data.into(),
            }),

            1 => ConsumerProtocolSubscription::V1(ConsumerProtocolSubscriptionV1 {
                topics: value.subscription.topics.into(),
                user_data: value.subscription.user_data.into(),
                owned_partitions: value
                    .subscription
                    .owned_partitions
                    .unwrap_or_default()
                    .into(),
            }),

            2 => ConsumerProtocolSubscription::V2(ConsumerProtocolSubscriptionV2 {
                topics: value.subscription.topics.into(),
                user_data: value.subscription.user_data.into(),
                owned_partitions: value
                    .subscription
                    .owned_partitions
                    .unwrap_or_default()
                    .into(),
                generation_id: value.subscription.generation_id.unwrap_or(-1),
            }),

            3 => ConsumerProtocolSubscription::V3(ConsumerProtocolSubscriptionV3 {
                topics: value.subscription.topics.into(),
                user_data: value.subscription.user_data.into(),
                owned_partitions: value
                    .subscription
                    .owned_partitions
                    .unwrap_or_default()
                    .into(),
                generation_id: value.subscription.generation_id.unwrap_or(-1),
                rack_id: value.subscription.rack_id.into(),
            }),

            version => todo!("{version}"),
        })
    }
}

impl<'de> Deserialize<'de> for MemberMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = MemberMetadata;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(stringify!(Bytes))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                seq.next_element::<i16>()
                    .and_then(|version| version.ok_or_else(|| de::Error::custom("length")))
                    .and_then(|version| {
                        match version {
                            0 => seq
                                .next_element::<ConsumerProtocolSubscriptionV0>()
                                .and_then(|version| version.ok_or_else(|| de::Error::custom("v0")))
                                .map(ConsumerProtocolSubscription::V0),

                            1 => seq
                                .next_element::<ConsumerProtocolSubscriptionV1>()
                                .and_then(|version| version.ok_or_else(|| de::Error::custom("v1")))
                                .map(ConsumerProtocolSubscription::V1),

                            2 => seq
                                .next_element::<ConsumerProtocolSubscriptionV2>()
                                .and_then(|version| version.ok_or_else(|| de::Error::custom("v2")))
                                .map(ConsumerProtocolSubscription::V2),

                            3 => seq
                                .next_element::<ConsumerProtocolSubscriptionV3>()
                                .and_then(|version| version.ok_or_else(|| de::Error::custom("v3")))
                                .map(ConsumerProtocolSubscription::V3),

                            version => Err(de::Error::custom(format!("unsupported: {version}"))),
                        }
                        .map(MemberMetadata)
                    })
            }
        }

        deserializer.deserialize_seq(V)
    }
}

impl Serialize for MemberMetadata {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_struct("MemberMetadata", 2)?;

        s.serialize_field(
            "version",
            match self.0 {
                ConsumerProtocolSubscription::V0(_) => &0i16,
                ConsumerProtocolSubscription::V1(_) => &1i16,
                ConsumerProtocolSubscription::V2(_) => &2i16,
                ConsumerProtocolSubscription::V3(_) => &3i16,
            },
        )?;

        match self.0 {
            ConsumerProtocolSubscription::V0(ref subscription) => {
                s.serialize_field("subscription", subscription)?
            }

            ConsumerProtocolSubscription::V1(ref subscription) => {
                s.serialize_field("subscription", subscription)?
            }

            ConsumerProtocolSubscription::V2(ref subscription) => {
                s.serialize_field("subscription", subscription)?
            }

            ConsumerProtocolSubscription::V3(ref subscription) => {
                s.serialize_field("subscription", subscription)?
            }
        }

        s.end()
    }
}

#[derive(Clone, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) enum ConsumerProtocolSubscription {
    V0(ConsumerProtocolSubscriptionV0),
    V1(ConsumerProtocolSubscriptionV1),
    V2(ConsumerProtocolSubscriptionV2),
    V3(ConsumerProtocolSubscriptionV3),
}

impl AsRef<i16> for ConsumerProtocolSubscription {
    fn as_ref(&self) -> &i16 {
        match self {
            ConsumerProtocolSubscription::V0(_) => &super::ConsumerProtocolSubscription::V0,
            ConsumerProtocolSubscription::V1(_) => &super::ConsumerProtocolSubscription::V1,
            ConsumerProtocolSubscription::V2(_) => &super::ConsumerProtocolSubscription::V2,
            ConsumerProtocolSubscription::V3(_) => &super::ConsumerProtocolSubscription::V3,
        }
    }
}

impl From<ConsumerProtocolSubscription> for super::ConsumerProtocolSubscription {
    fn from(value: ConsumerProtocolSubscription) -> Self {
        match value {
            ConsumerProtocolSubscription::V0(subscription) => Self {
                topics: subscription.topics.into(),
                user_data: subscription.user_data.into(),
                owned_partitions: None,
                generation_id: None,
                rack_id: None,
            },

            ConsumerProtocolSubscription::V1(subscription) => Self {
                topics: subscription.topics.into(),
                user_data: subscription.user_data.into(),
                owned_partitions: Some(subscription.owned_partitions.into()),
                generation_id: None,
                rack_id: None,
            },

            ConsumerProtocolSubscription::V2(subscription) => Self {
                topics: subscription.topics.into(),
                user_data: subscription.user_data.into(),
                owned_partitions: Some(subscription.owned_partitions.into()),
                generation_id: Some(subscription.generation_id),
                rack_id: None,
            },

            ConsumerProtocolSubscription::V3(subscription) => Self {
                topics: subscription.topics.into(),
                user_data: subscription.user_data.into(),
                owned_partitions: Some(subscription.owned_partitions.into()),
                generation_id: Some(subscription.generation_id),
                rack_id: subscription.rack_id.into(),
            },
        }
    }
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct ConsumerProtocolSubscriptionV0 {
    topics: Sequence<U16String>,
    user_data: Octets,
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct ConsumerProtocolSubscriptionV1 {
    topics: Sequence<U16String>,
    user_data: Octets,
    owned_partitions: Sequence<TopicPartition>,
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct ConsumerProtocolSubscriptionV2 {
    topics: Sequence<U16String>,
    user_data: Octets,
    owned_partitions: Sequence<TopicPartition>,
    generation_id: i32,
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct ConsumerProtocolSubscriptionV3 {
    topics: Sequence<U16String>,
    user_data: Octets,
    owned_partitions: Sequence<TopicPartition>,
    generation_id: i32,
    rack_id: NullableU16String,
}

impl From<super::ConsumerProtocolSubscription> for ConsumerProtocolSubscription {
    fn from(value: super::ConsumerProtocolSubscription) -> Self {
        todo!()
    }
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct TopicPartition {
    topic: U16String,
    partitions: Sequence<i32>,
}

impl From<super::TopicPartition> for TopicPartition {
    fn from(value: super::TopicPartition) -> Self {
        Self {
            topic: value.topic.into(),
            partitions: value.partitions.into(),
        }
    }
}

impl From<TopicPartition> for super::TopicPartition {
    fn from(value: TopicPartition) -> Self {
        Self {
            topic: value.topic.into(),
            partitions: value.partitions.into(),
        }
    }
}

#[derive(Clone, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub(crate) struct MemberAssignment {
    version: i16,
    assignment: ConsumerProtocolAssignment,
}

impl From<super::MemberAssignment> for MemberAssignment {
    fn from(value: super::MemberAssignment) -> Self {
        Self {
            version: value.version,
            assignment: value.assignment.into(),
        }
    }
}

impl From<MemberAssignment> for super::MemberAssignment {
    fn from(value: MemberAssignment) -> Self {
        Self {
            version: value.version,
            assignment: value.assignment.into(),
        }
    }
}

#[derive(Clone, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
struct ConsumerProtocolAssignment {
    assigned_partitions: Sequence<TopicPartition>,
    user_data: Octets,
}

impl From<super::ConsumerProtocolAssignment> for ConsumerProtocolAssignment {
    fn from(value: super::ConsumerProtocolAssignment) -> Self {
        Self {
            assigned_partitions: value.assigned_partitions.into(),
            user_data: value.user_data.into(),
        }
    }
}

impl From<ConsumerProtocolAssignment> for super::ConsumerProtocolAssignment {
    fn from(value: ConsumerProtocolAssignment) -> Self {
        Self {
            assigned_partitions: value.assigned_partitions.into(),
            user_data: value.user_data.into(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Sequence<T>(Vec<T>);

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

impl<T, U> From<Vec<T>> for Sequence<U>
where
    T: Into<U>,
{
    fn from(value: Vec<T>) -> Self {
        Self(value.into_iter().map(Into::into).collect())
    }
}

impl<T, U> From<Sequence<T>> for Vec<U>
where
    T: Into<U>,
{
    fn from(value: Sequence<T>) -> Self {
        value.0.into_iter().map(Into::into).collect()
    }
}

impl<T> Serialize for Sequence<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_seq(Some(self.0.len()))?;

        u32::try_from(self.0.len())
            .map_err(|e| ser::Error::custom(e.to_string()))
            .and_then(|length| s.serialize_element(&length))?;

        for i in self.0.iter() {
            s.serialize_element(i)?;
        }
        s.end()
    }
}

impl<'de, T> Deserialize<'de> for Sequence<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V<T>(PhantomData<T>);

        impl<'de, T> Visitor<'de> for V<T>
        where
            T: Deserialize<'de>,
        {
            type Value = Sequence<T>;

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
                        if length > 1024 {
                            Err(<A::Error as de::Error>::custom(format!(
                                "consumer maximum array length: {}",
                                length
                            )))
                        } else {
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
                        }
                    })
                    .map(Sequence)
            }
        }

        deserializer
            .deserialize_seq(V(PhantomData))
            .inspect_err(|err| debug!(?err))
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct U16String(String);

impl Serialize for U16String {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_seq(Some(self.0.len()))?;

        u16::try_from(self.0.len())
            .map_err(|e| ser::Error::custom(e.to_string()))
            .and_then(|length| s.serialize_element(&length))?;

        for i in self.0.as_bytes() {
            s.serialize_element(&i)?;
        }

        s.end()
    }
}

impl From<String> for U16String {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<U16String> for String {
    fn from(value: U16String) -> Self {
        value.0
    }
}

impl<'de> Deserialize<'de> for U16String {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = U16String;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(stringify!(Bytes))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                seq.next_element::<i16>()?
                    .ok_or_else(|| de::Error::custom("length"))
                    .inspect(|length| debug!(length))
                    .and_then(|length| {
                        if length > 4096 {
                            Err(<A::Error as de::Error>::custom(format!(
                                "maximum string size: {}",
                                length
                            )))
                        } else {
                            (0..length)
                                .map(|_| {
                                    seq.next_element::<u8>().and_then(|byte| {
                                        byte.ok_or_else(|| <A::Error as de::Error>::custom("byte"))
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()
                        }
                    })
                    .and_then(|bytes| {
                        String::from_utf8(bytes)
                            .map_err(|_| <A::Error as de::Error>::custom("from_utf8"))
                    })
                    .map(U16String)
            }
        }

        deserializer.deserialize_seq(V)
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct NullableU16String(Option<String>);

impl Serialize for NullableU16String {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_seq(self.0.as_ref().map(|bytes| bytes.len()))?;

        if let Some(bytes) = self.0.as_deref().map(|s| s.as_bytes()) {
            u16::try_from(bytes.len())
                .map_err(|e| ser::Error::custom(e.to_string()))
                .and_then(|length| s.serialize_element(&length))?;

            for i in bytes {
                s.serialize_element(&i)?;
            }
        } else {
            s.serialize_element(&-1i16)?;
        }

        s.end()
    }
}

impl<'de> Deserialize<'de> for NullableU16String {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = NullableU16String;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(stringify!(Bytes))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                seq.next_element::<i16>()?
                    .ok_or_else(|| de::Error::custom("length"))
                    .inspect(|length| debug!(length))
                    .and_then(|length| {
                        let q = i16::MAX;

                        if length == -1 {
                            Ok(None)
                        } else if length > 4096 {
                            Err(<A::Error as de::Error>::custom(format!(
                                "maximum string size: {}",
                                length
                            )))
                        } else {
                            (0..length)
                                .map(|_| {
                                    seq.next_element::<u8>().and_then(|byte| {
                                        byte.ok_or_else(|| <A::Error as de::Error>::custom("byte"))
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()
                                .and_then(|bytes| {
                                    String::from_utf8(bytes)
                                        .map_err(|_| <A::Error as de::Error>::custom("from_utf8"))
                                })
                                .map(Some)
                        }
                    })
                    .map(NullableU16String)
            }
        }

        deserializer.deserialize_seq(V)
    }
}

impl From<Option<String>> for NullableU16String {
    fn from(value: Option<String>) -> Self {
        Self(value)
    }
}

impl From<String> for NullableU16String {
    fn from(value: String) -> Self {
        Self(Some(value))
    }
}

impl From<NullableU16String> for Option<String> {
    fn from(value: NullableU16String) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Octets(Option<Bytes>);

impl Serialize for Octets {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_seq(self.0.as_ref().map(|bytes| bytes.len()))?;

        if let Some(ref bytes) = self.0 {
            u32::try_from(bytes.len())
                .map_err(|e| ser::Error::custom(e.to_string()))
                .and_then(|length| s.serialize_element(&length))?;

            for i in bytes {
                s.serialize_element(&i)?;
            }
        } else {
            s.serialize_element(&-1i32)?;
        }

        s.end()
    }
}

impl<'de> Deserialize<'de> for Octets {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = Octets;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(stringify!(Bytes))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                seq.next_element::<i32>()?
                    .ok_or_else(|| de::Error::custom("length"))
                    .inspect(|length| debug!(length))
                    .and_then(|length| {
                        if length == -1 {
                            Ok(None)
                        } else {
                            (0..length)
                                .map(|_| {
                                    seq.next_element::<u8>().and_then(|byte| {
                                        byte.ok_or_else(|| <A::Error as de::Error>::custom("byte"))
                                    })
                                })
                                .collect::<Result<Vec<_>, _>>()
                                .map(Bytes::from)
                                .map(Some)
                        }
                    })
                    .map(Octets)
            }
        }

        deserializer.deserialize_seq(V)
    }
}

impl From<Option<Bytes>> for Octets {
    fn from(value: Option<Bytes>) -> Self {
        Octets(value)
    }
}

impl From<Octets> for Option<Bytes> {
    fn from(value: Octets) -> Self {
        value.0
    }
}
