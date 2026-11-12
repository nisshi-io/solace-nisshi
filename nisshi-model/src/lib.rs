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
//
//! Structures representing Kafka JSON protocol definitions.
//!
//! This crate converts Kafka JSON protocol definitions into structures
//! that can be easily used during the Nisshi Sans I/O build process.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
pub mod error;
pub mod wv;

use convert_case::{Case, Casing};
pub use error::Error;
use lazy_static::lazy_static;
use proc_macro2::{Ident, Span, TokenStream};
use quote::ToTokens;
use regex::Regex;
use serde_json::Value;
use std::{fmt, str::FromStr};
use syn::{Expr, Type};
use tracing::debug;
use wv::{As, AsOption, Wv};

macro_rules! prefix_crate {
    ($e:ident) => {
        format!("{}::{}", env!("CARGO_CRATE_NAME"), stringify!($e))
    };
}

macro_rules! with_crate {
    ($e:expr_2021) => {
        format!("{}::{:?}", env!("CARGO_CRATE_NAME"), $e)
    };

    ($e:expr_2021, $f:expr_2021) => {
        format!("{}::{}::{:?}", env!("CARGO_CRATE_NAME"), $e, $f)
    };
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

fn type_mapping(kind: &str) -> String {
    match kind {
        "bytes" => String::from("bytes::Bytes"),
        "float64" => String::from("f64"),
        "int16" => String::from("i16"),
        "int32" => String::from("i32"),
        "int64" => String::from("i64"),
        "int8" => String::from("i8"),
        "string" => String::from("String"),
        "uint16" => String::from("u16"),
        "uuid" => String::from("[u8; 16]"),
        "records" => String::from("crate::RecordBatch"),

        sequence if sequence.starts_with("[]") => type_mapping(&sequence[2..]),

        s => String::from(s),
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// The Kafka field kind (type).
pub struct Kind(String);

impl ToTokens for Kind {
    #![allow(clippy::unwrap_used)]
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let expr = with_crate!(self);
        syn::parse_str::<Expr>(&expr).unwrap().to_tokens(tokens);
    }
}

impl From<Type> for Kind {
    fn from(value: Type) -> Self {
        Self(value.to_token_stream().to_string())
    }
}

lazy_static! {
    static ref PRIMITIVE: &'static [&'static str] = &[
        "bool", "bytes", "float64", "int16", "int32", "int64", "int8", "records", "string",
        "uint16", "uuid",
    ];
}

impl Kind {
    #[must_use]
    pub fn new(kind: &str) -> Self {
        Self(kind.to_owned())
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.0[..]
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    /// The Rust type name of this kind.
    pub fn type_name(&self) -> Type {
        syn::parse_str::<Type>(&type_mapping(&self.0))
            .unwrap_or_else(|_| panic!("not a type: {self:?}"))
    }

    #[must_use]
    /// Returns true if this kind is a sequence (array)
    pub fn is_sequence(&self) -> bool {
        self.0.starts_with("[]")
    }

    #[must_use]
    /// Returns true is this kind is a Kafka primitive (defined) type
    pub fn is_primitive(&self) -> bool {
        PRIMITIVE.contains(&self.0.as_str())
    }

    #[must_use]
    /// Returns true if this kind is a float64
    pub fn is_float(&self) -> bool {
        self.0.eq("float64")
    }

    #[must_use]
    /// Returns true if this is a sequence of Kafka primitive types
    pub fn is_sequence_of_primitive(&self) -> bool {
        self.is_sequence() && PRIMITIVE.contains(&&self.0[2..])
    }

    #[must_use]
    /// Optionally when the kind is a sequence, returns the kind of the sequence
    pub fn kind_of_sequence(&self) -> Option<Self> {
        if self.is_sequence() {
            Some(Self::new(&self.0[2..]))
        } else {
            None
        }
    }
}

impl FromStr for Kind {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_owned()))
    }
}

impl TryFrom<&Value> for Kind {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        as_str(value, "type").and_then(Self::from_str)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// The listener for this message type.
pub enum Listener {
    ZkBroker,
    #[default]
    Broker,
    Controller,
}

impl TryFrom<&Value> for Listener {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        value
            .as_str()
            .ok_or(Error::Message(String::from(
                "expecting string for listener",
            )))
            .and_then(Self::from_str)
    }
}

impl FromStr for Listener {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "zkBroker" => Ok(Self::ZkBroker),
            "broker" => Ok(Self::Broker),
            "controller" => Ok(Self::Controller),
            s => Err(Error::Message(String::from(s))),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Model request and response types only
pub enum MessageKind {
    #[default]
    Request,
    Response,
}

impl ToTokens for MessageKind {
    #[allow(clippy::unwrap_used)]
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let expr = with_crate!("MessageKind", self);
        syn::parse_str::<Expr>(&expr).unwrap().to_tokens(tokens);
    }
}

impl TryFrom<&Value> for MessageKind {
    type Error = Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        as_str(value, "type").and_then(Self::from_str)
    }
}

impl FromStr for MessageKind {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "request" => Ok(Self::Request),
            "response" => Ok(Self::Response),
            s => Err(Error::Message(String::from(s))),
        }
    }
}

#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// A range of versions.
pub struct VersionRange {
    pub start: i16,
    pub end: i16,
}

impl fmt::Debug for VersionRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(&prefix_crate!(VersionRange))
            .field("start", &self.start)
            .field("end", &self.end)
            .finish()
    }
}

impl VersionRange {
    #[must_use]
    pub fn within(&self, version: i16) -> bool {
        version >= self.start && version <= self.end
    }

    #[must_use]
    pub fn is_mandatory(&self, parent: Option<VersionRange>) -> bool {
        parent.map_or(
            self.start == 0 && self.end == i16::MAX,
            |VersionRange { start, end }| {
                if start > self.start {
                    self.end == end
                } else {
                    self.start == start && self.end == end
                }
            },
        )
    }
}

impl ToTokens for VersionRange {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let expr = format!("{self:?}");
        syn::parse_str::<Expr>(&expr)
            .unwrap_or_else(|_| panic!("an expression: {self:?}"))
            .to_tokens(tokens);
    }
}

impl FromStr for VersionRange {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        #![allow(clippy::expect_used)]
        lazy_static! {
            static ref RX: Regex =
                Regex::new(r"^(?<start>\d+)(-(?<end>\d+)|(?<infinite>\+))?$").expect("regex");
        }

        if s == "none" {
            Ok(VersionRange { start: -2, end: -1 })
        } else {
            RX.captures(s)
                .ok_or(Error::Message(format!("invalid version range format: {s}")))
                .and_then(|captures| {
                    let parse = |name, default: i16| {
                        captures
                            .name(name)
                            .map_or(Ok(&default.to_string()[..]), |s| Ok(s.as_str()))
                            .and_then(str::parse)
                    };

                    let start = parse("start", 0)?;

                    let end = if let Some(end) = captures.name("end") {
                        str::parse(end.as_str())?
                    } else if captures.name("infinite").is_some() {
                        i16::MAX
                    } else {
                        start
                    };

                    Ok(VersionRange { start, end })
                })
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// The validity, deprecation and flexible version ranges of a Kafka API message.
pub struct Version {
    /// The valid version ranges of this Kafka message.
    pub valid: VersionRange,
    /// The deprecated version range of this Kafka message.
    pub deprecated: Option<VersionRange>,
    /// The range of versions where this message uses flexible encoding.
    pub flexible: VersionRange,
}

impl ToTokens for Version {
    #[allow(clippy::unwrap_used)]
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let expr = with_crate!(self);
        syn::parse_str::<Expr>(&expr).unwrap().to_tokens(tokens);
    }
}

impl Version {
    #[must_use]
    pub fn valid(&self) -> VersionRange {
        self.valid
    }

    #[must_use]
    pub fn deprecated(&self) -> Option<VersionRange> {
        self.deprecated
    }

    #[must_use]
    pub fn flexible(&self) -> VersionRange {
        self.flexible
    }
}

impl<'a> TryFrom<&Wv<'a>> for Version {
    type Error = Error;

    fn try_from(value: &Wv<'a>) -> Result<Self, Self::Error> {
        debug!("value: {:?}", value);

        Ok(Self {
            valid: value.as_a("validVersions")?,
            deprecated: value.as_option("deprecatedVersions")?,
            flexible: value.as_a("flexibleVersions")?,
        })
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Kafka message field schema
pub struct Field {
    /// The name of this field.
    name: String,
    /// The Kafka type of this field.
    kind: Kind,
    /// A comment about this field.
    about: Option<String>,
    /// The version range for this field.
    versions: VersionRange,
    /// Whether this field can be used as the key in a map.
    map_key: Option<bool>,
    /// Whether this field can be null.
    nullable: Option<VersionRange>,
    /// The tag ID for this field
    tag: Option<u32>,
    /// The version range in which this field can be tagged.
    tagged: Option<VersionRange>,
    /// The entity type of this field.
    entity_type: Option<String>,
    /// The default value of this field.
    default: Option<String>,
    /// Any fields this field contains.
    fields: Option<Vec<Field>>,
}

impl Field {
    #[must_use]
    pub fn ident(&self) -> Ident {
        match self.name.to_case(Case::Snake) {
            reserved if is_reserved_keyword(&reserved) => {
                Ident::new_raw(&reserved, Span::call_site())
            }

            otherwise => Ident::new(&otherwise, Span::call_site()),
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn kind(&self) -> &Kind {
        &self.kind
    }

    #[must_use]
    pub fn fields(&self) -> Option<&[Field]> {
        self.fields.as_deref()
    }

    #[must_use]
    pub fn versions(&self) -> VersionRange {
        self.versions
    }

    #[must_use]
    pub fn nullable(&self) -> Option<VersionRange> {
        self.nullable
    }

    #[must_use]
    pub fn tagged(&self) -> Option<VersionRange> {
        self.tagged
    }

    #[must_use]
    pub fn tag(&self) -> Option<u32> {
        self.tag
    }

    #[must_use]
    pub fn about(&self) -> Option<&str> {
        self.about.as_deref()
    }

    #[must_use]
    pub fn has_tags(&self) -> bool {
        self.tagged.is_some()
    }

    #[must_use]
    pub fn has_records(&self) -> bool {
        self.kind.0 == "records"
            || self
                .fields()
                .is_some_and(|fields| fields.iter().any(Field::has_records))
    }

    #[must_use]
    pub fn has_float(&self) -> bool {
        self.kind().is_float()
            || self
                .fields()
                .is_some_and(|fields| fields.iter().any(Field::has_float))
    }
}

impl<'a> TryFrom<&Wv<'a>> for Field {
    type Error = Error;

    fn try_from(value: &Wv<'a>) -> Result<Self, Self::Error> {
        let name = value.as_a("name")?;
        let kind = value.as_a("type")?;
        let about = value.as_option("about")?;
        let versions = value.as_a("versions")?;
        let map_key = value.as_option("mapKey")?;
        let nullable = value.as_option("nullableVersions")?;
        let tag = value.as_option("tag")?;
        let tagged = value.as_option("taggedVersions")?;
        let entity_type = value.as_option("entityType")?;
        let default = value.as_option("default")?;

        let fields = value
            .as_option("fields")?
            .map_or(Ok(None), |values: &[Value]| {
                values
                    .iter()
                    .try_fold(Vec::new(), |mut acc, field| {
                        Field::try_from(&Wv::from(field)).map(|f| {
                            acc.push(f);
                            acc
                        })
                    })
                    .map(Some)
            })?;

        Ok(Field {
            name,
            kind,
            about,
            versions,
            map_key,
            nullable,
            tag,
            tagged,
            entity_type,
            default,
            fields,
        })
    }
}

fn is_reserved_keyword(s: &str) -> bool {
    lazy_static! {
        static ref RESERVED: Vec<&'static str> = vec!["as",
        "break",
        "const",
        "continue",
        "crate",
        "else",
        "enum",
        "extern",
        "false",
        "fn",
        "for",
        "if",
        "impl",
        "in",
        "let",
        "loop",
        "match",
        "mod",
        "move",
        "mut",
        "pub",
        "ref",
        "return",
        "self",
        "Self",
        "static",
        "struct",
        "super",
        "trait",
        "true",
        "type",
        "unsafe",
        "use",
        "where",
        "while",

        // 2018 edition
        "async",
        "await",
        "dyn",

        // reserved
        "abstract",
        "become",
        "box",
        "do",
        "final",
        "macro",
        "override",
        "priv",
        "typeof",
        "unsized",
        "virtual",
        "yield",

        // reserved 2018 edition
        "try",
        ];
    }

    RESERVED.contains(&s)
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Message {
    api_key: i16,
    kind: MessageKind,
    listeners: Option<Vec<Listener>>,
    name: String,
    versions: Version,
    fields: Vec<Field>,
    common_structs: Option<Vec<CommonStruct>>,
}

impl Message {
    #[must_use]
    pub fn api_key(&self) -> i16 {
        self.api_key
    }

    #[must_use]
    pub fn kind(&self) -> MessageKind {
        self.kind
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc, clippy::unwrap_used)]
    pub fn type_name(&self) -> Type {
        syn::parse_str::<Type>(&self.name).unwrap()
    }

    #[must_use]
    pub fn version(&self) -> Version {
        self.versions
    }

    #[must_use]
    pub fn listeners(&self) -> Option<&[Listener]> {
        self.listeners.as_deref()
    }

    #[must_use]
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc, clippy::unwrap_used)]
    pub fn wrapper_new_type(&self, field: &Field) -> Type {
        syn::parse_str::<Type>(&format!("{}{}", self.name, field.name).to_case(Case::Pascal))
            .unwrap()
    }

    #[must_use]
    pub fn common_structs(&self) -> Option<&[CommonStruct]> {
        self.common_structs.as_deref()
    }

    #[must_use]
    pub fn has_records(&self) -> bool {
        self.fields().iter().any(Field::has_records)
    }

    #[must_use]
    pub fn has_tags(&self) -> bool {
        self.fields().iter().any(Field::has_tags)
    }

    #[must_use]
    pub fn has_float(&self) -> bool {
        self.fields.iter().any(|field| field.kind().is_float())
            || self
                .common_structs()
                .is_some_and(|structures| structures.iter().any(CommonStruct::has_float))
    }
}

impl<'a> TryFrom<&Wv<'a>> for Message {
    type Error = Error;

    fn try_from(value: &Wv<'a>) -> Result<Self, Self::Error> {
        let api_key = value.as_a("apiKey")?;
        let name = value.as_a("name")?;
        let kind = value.as_a("type")?;
        let listeners = value
            .as_option("listeners")?
            .map_or(Ok(None), |maybe: &[Value]| {
                maybe
                    .iter()
                    .try_fold(Vec::new(), |mut acc, listener| {
                        Listener::try_from(listener).map(|l| {
                            acc.push(l);
                            acc
                        })
                    })
                    .map(Some)
            })?;

        let fields = value.as_a("fields").and_then(|fields: &[Value]| {
            fields.iter().try_fold(Vec::new(), |mut acc, field| {
                Field::try_from(&Wv::from(field)).map(|f| {
                    acc.push(f);
                    acc
                })
            })
        })?;

        let versions = Version::try_from(value)?;
        let common_structs =
            value
                .as_option("commonStructs")?
                .map_or(Ok(None), |maybe: &[Value]| {
                    maybe
                        .iter()
                        .try_fold(Vec::new(), |mut acc, value| {
                            CommonStruct::try_from(&Wv::from(value)).map(|field| {
                                acc.push(field);
                                acc
                            })
                        })
                        .map(Some)
                })?;

        Ok(Self {
            api_key,
            kind,
            listeners,
            name,
            versions,
            fields,
            common_structs,
        })
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CommonStruct {
    name: String,
    fields: Vec<Field>,
}

impl CommonStruct {
    #[must_use]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn type_name(&self) -> Type {
        syn::parse_str::<Type>(&self.name).unwrap_or_else(|_| panic!("not a type: {self:?}"))
    }

    #[must_use]
    pub fn fields(&self) -> &Vec<Field> {
        &self.fields
    }

    #[must_use]
    pub fn has_float(&self) -> bool {
        self.fields.iter().any(|field| field.kind().is_float())
    }
}

impl<'a> TryFrom<&Wv<'a>> for CommonStruct {
    type Error = Error;

    fn try_from(value: &Wv<'a>) -> Result<Self, Self::Error> {
        let name = value.as_a("name")?;
        let fields = value.as_a("fields").and_then(|fields: &[Value]| {
            fields.iter().try_fold(Vec::new(), |mut acc, field| {
                Field::try_from(&Wv::from(field)).map(|f| {
                    acc.push(f);
                    acc
                })
            })
        })?;

        Ok(Self { name, fields })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HeaderMeta {
    pub name: &'static str,
    pub valid: VersionRange,
    pub flexible: VersionRange,
    pub fields: &'static [(&'static str, &'static FieldMeta)],
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Kafka API message metadata.
pub struct MessageMeta {
    /// The name of the Kafka API message.
    pub name: &'static str,
    /// The API key used by this message.
    pub api_key: i16,
    /// The version ranges for this message.
    pub version: Version,
    /// The message kind of this message.
    pub message_kind: MessageKind,
    /// The fields that this message describes.
    pub fields: &'static [(&'static str, &'static FieldMeta)],
}

impl MessageMeta {
    #[must_use]
    pub fn is_flexible(&self, version: i16) -> bool {
        self.version.flexible.within(version)
    }

    #[must_use]
    pub fn structures(&self) -> Vec<(&str, &FieldMeta)> {
        self.fields
            .iter()
            .filter(|(_, fm)| fm.is_structure())
            .flat_map(|(name, fm)| {
                debug!(name = self.name, field = ?name, kind = ?fm.kind.0);

                let mut children = fm.structures();

                if let Some(kind) = fm.kind.kind_of_sequence() {
                    if !kind.is_primitive() {
                        children.push((kind.name(), *fm));
                    }
                } else {
                    children.push((fm.kind.name(), *fm));
                }

                children
            })
            .collect()
    }

    #[must_use]
    pub fn field(&self, name: &str) -> Option<&'static FieldMeta> {
        self.fields
            .iter()
            .find(|(found, _)| name == *found)
            .map(|(_, meta)| *meta)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Kafka API message field metadata
pub struct FieldMeta {
    /// The version range of this field.
    pub version: VersionRange,
    /// The version range where this field may be null.
    pub nullable: Option<VersionRange>,
    /// The kind (type) metadata of this field.
    pub kind: KindMeta,
    /// When present the tag ID of this field.
    pub tag: Option<u32>,
    /// The range of versions where this field is tagged.
    pub tagged: Option<VersionRange>,
    /// The fields contained within this structure.
    pub fields: &'static [(&'static str, &'static FieldMeta)],
}

impl FieldMeta {
    #[must_use]
    pub fn is_nullable(&self, version: i16) -> bool {
        self.nullable.is_some_and(|range| range.within(version))
    }

    #[must_use]
    pub fn is_mandatory(&self, parent: Option<VersionRange>) -> bool {
        self.version.is_mandatory(parent)
    }

    #[must_use]
    pub fn is_structure(&self) -> bool {
        self.kind
            .kind_of_sequence()
            .is_some_and(|sk| !sk.is_primitive())
            || !self.fields.is_empty()
    }

    #[must_use]
    pub fn structures(&self) -> Vec<(&str, &FieldMeta)> {
        self.fields
            .iter()
            .filter(|(_, fm)| fm.is_structure())
            .flat_map(|(_, fm)| {
                let mut children = fm.structures();

                if let Some(kind) = fm.kind.kind_of_sequence()
                    && !kind.is_primitive()
                {
                    children.push((kind.name(), *fm));
                }

                children
            })
            .collect()
    }

    pub fn field(&self, name: &str) -> Option<&FieldMeta> {
        self.fields
            .iter()
            .find(|field| name == field.0)
            .map(|(_, meta)| *meta)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct KindMeta(pub &'static str);

impl KindMeta {
    #[must_use]
    pub fn name(&self) -> &'static str {
        self.0
    }

    #[must_use]
    pub fn is_sequence(&self) -> bool {
        self.0.starts_with("[]")
    }

    #[must_use]
    pub fn is_primitive(&self) -> bool {
        PRIMITIVE.contains(&self.0)
    }

    #[must_use]
    pub fn is_string(&self) -> bool {
        self.0 == "string"
    }

    #[must_use]
    pub fn is_records(&self) -> bool {
        self.0 == "records"
    }

    #[must_use]
    pub fn kind_of_sequence(&self) -> Option<Self> {
        if self.is_sequence() {
            Some(Self(&self.0[2..]))
        } else {
            None
        }
    }
}

fn as_str<'v>(value: &'v Value, name: &str) -> Result<&'v str> {
    value[name]
        .as_str()
        .ok_or(Error::Message(String::from(name)))
}

#[cfg(test)]
mod tests {
    use std::{any::type_name_of_val, collections::HashMap};

    use serde_json::json;

    use super::*;

    const PRIMITIVES: [&str; 10] = [
        "bool", "bytes", "float64", "int16", "int32", "int64", "int8", "string", "uint16", "uuid",
    ];

    #[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
    struct Header {
        name: String,
        valid: VersionRange,
        flexible: VersionRange,
        fields: Vec<Field>,
    }

    impl<'a> TryFrom<&Wv<'a>> for Header {
        type Error = Error;

        fn try_from(value: &Wv<'a>) -> Result<Self, Self::Error> {
            let fields: &[Value] = value.as_a("fields")?;

            Ok(Header {
                name: value.as_a("name")?,
                valid: value.as_a("validVersions")?,
                flexible: value.as_a("flexibleVersions")?,
                fields: fields.iter().try_fold(Vec::new(), |mut acc, field| {
                    Field::try_from(&Wv::from(field)).map(|f| {
                        acc.push(f);
                        acc
                    })
                })?,
            })
        }
    }

    #[test]
    fn type_mapping_test() {
        assert_eq!("bytes::Bytes", type_mapping("bytes"));
        assert_eq!("f64", type_mapping("float64"));
        assert_eq!("i16", type_mapping("int16"));
        assert_eq!("i32", type_mapping("int32"));
        assert_eq!("i64", type_mapping("int64"));
        assert_eq!("i8", type_mapping("int8"));
        assert_eq!("String", type_mapping("string"));
        assert_eq!("u16", type_mapping("uint16"));
        assert_eq!("[u8; 16]", type_mapping("uuid"));

        assert_eq!("i16", type_mapping("[]int16"));
        assert_eq!("SomeType", type_mapping("[]SomeType"));

        assert_eq!("SomeType", type_mapping("SomeType"));
    }

    #[test]
    fn kind_is_sequence() {
        for primitive in PRIMITIVES {
            let k = Kind::new(primitive);
            assert!(!k.is_sequence());
        }

        let sequences = vec!["[]int16", "[]string", "[]SomeType"];

        for sequence in sequences {
            let k = Kind::new(sequence);
            assert!(k.is_sequence());
        }
    }

    #[test]
    fn kind_is_sequence_of_primitive() {
        for primitive in PRIMITIVES {
            let k = Kind::new(primitive);
            assert!(!k.is_sequence_of_primitive());
        }

        let primitive_sequences: Vec<String> = PRIMITIVES
            .iter()
            .map(|primitive| format!("[]{primitive}"))
            .collect();

        for sequence in primitive_sequences {
            let k = Kind::new(sequence.as_str());
            assert!(k.is_sequence_of_primitive());
        }

        let sequences = vec!["[]SomeType", "[]OtherType"];

        for sequence in sequences {
            let k = Kind::new(sequence);
            assert!(!k.is_sequence_of_primitive());
        }
    }

    #[test]
    fn kind_is_primitive() {
        for primitive in PRIMITIVES {
            let k = Kind::new(primitive);
            assert!(k.is_primitive());
        }

        let primitive_sequences: Vec<String> = PRIMITIVES
            .iter()
            .map(|primitive| format!("[]{primitive}"))
            .collect();

        for sequence in primitive_sequences {
            let k = Kind::new(sequence.as_str());
            assert!(!k.is_primitive());
        }
    }

    #[test]
    fn listener_from_value() -> Result<()> {
        assert_eq!(Listener::ZkBroker, Listener::try_from(&json!("zkBroker"))?);

        assert_eq!(Listener::Broker, Listener::try_from(&json!("broker"))?);

        assert_eq!(
            Listener::Controller,
            Listener::try_from(&json!("controller"))?
        );

        Ok(())
    }

    #[test]
    fn message_kind_from_value() -> Result<()> {
        assert_eq!(
            MessageKind::Request,
            MessageKind::try_from(&json!({
                "type": "request"
            }
            ))?
        );

        assert_eq!(
            MessageKind::Response,
            MessageKind::try_from(&json!({
                "type": "response"
            }
            ))?
        );

        Ok(())
    }

    #[test]
    fn version_range_from_str() -> Result<()> {
        assert_eq!(
            VersionRange { start: -2, end: -1 },
            VersionRange::from_str("none")?
        );

        assert_eq!(
            VersionRange {
                start: 3,
                end: i16::MAX
            },
            VersionRange::from_str("3+")?
        );

        assert_eq!(
            VersionRange { start: 6, end: 9 },
            VersionRange::from_str("6-9")?
        );

        assert_eq!(
            VersionRange { start: 1, end: 1 },
            VersionRange::from_str("1")?
        );

        Ok(())
    }

    #[test]
    fn version_range_within() {
        {
            let range = VersionRange { start: 0, end: 0 };
            assert!(!range.within(i16::MIN));
            assert!(range.within(0));
            assert!(!range.within(1));
            assert!(!range.within(i16::MAX));
        }

        {
            let range = VersionRange {
                start: 3,
                end: i16::MAX,
            };
            assert!(!range.within(i16::MIN));
            assert!(!range.within(2));
            assert!(range.within(3));
            assert!(range.within(i16::MAX));
        }

        {
            let range = VersionRange { start: 6, end: 9 };
            assert!(!range.within(i16::MIN));
            assert!(!range.within(5));
            assert!(range.within(6));
            assert!(range.within(7));
            assert!(range.within(8));
            assert!(range.within(9));
            assert!(!range.within(10));
            assert!(!range.within(i16::MAX));
        }
    }

    #[test]
    fn field_from_value() -> Result<()> {
        assert_eq!(
            Field {
                name: String::from("Topics"),
                kind: Kind(String::from("[]CreatableTopic")),
                about: Some(String::from("The topics to create.")),
                versions: VersionRange::from_str("0+")?,
                map_key: None,
                nullable: None,
                tag: None,
                tagged: None,
                entity_type: None,
                default: None,
                fields: Some(vec![Field {
                    name: String::from("Name"),
                    kind: Kind(String::from("string")),
                    versions: VersionRange::from_str("0+")?,
                    map_key: Some(true),
                    entity_type: Some(String::from("topicName")),
                    about: Some(String::from("The topic name.")),
                    default: None,
                    nullable: None,
                    tag: None,
                    tagged: None,
                    fields: None,
                }]),
            },
            serde_json::from_str::<Value>(
                r#"
                {
                    "name": "Topics",
                    "type": "[]CreatableTopic",
                    "versions": "0+",
                    "about": "The topics to create.",
                     "fields": [
                        { "name": "Name",
                          "type": "string",
                          "versions": "0+",
                          "mapKey": true,
                          "entityType": "topicName",
                          "about": "The topic name."
                        }]
                }
                "#
            )
            .map_err(Into::into)
            .and_then(|v| Field::try_from(&Wv::from(&v)))?
        );

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    #[test]
    fn tagged_field_from_value() -> Result<()> {
        assert_eq!(
            Field {
                name: "SupportedFeatures".into(),
                kind: Kind("[]SupportedFeatureKey".into()),
                about: Some("Features supported by the broker.".into()),
                versions: VersionRange {
                    start: 3,
                    end: 32767
                },
                map_key: None,
                nullable: None,
                tag: Some(0),
                tagged: Some(VersionRange {
                    start: 3,
                    end: 32767
                }),
                entity_type: None,
                default: None,
                fields: Some(
                    [
                        Field {
                            name: "Name".into(),
                            kind: Kind("string".into()),
                            about: Some("The name of the feature.".into()),
                            versions: VersionRange {
                                start: 3,
                                end: 32767
                            },
                            map_key: Some(true),
                            nullable: None,
                            tag: None,
                            tagged: None,
                            entity_type: None,
                            default: None,
                            fields: None
                        },
                        Field {
                            name: "MinVersion".into(),
                            kind: Kind("int16".into()),
                            about: Some("The minimum supported version for the feature.".into()),
                            versions: VersionRange {
                                start: 3,
                                end: 32767
                            },
                            map_key: None,
                            nullable: None,
                            tag: None,
                            tagged: None,
                            entity_type: None,
                            default: None,
                            fields: None
                        },
                        Field {
                            name: "MaxVersion".into(),
                            kind: Kind("int16".into()),
                            about: Some("The maximum supported version for the feature.".into()),
                            versions: VersionRange {
                                start: 3,
                                end: 32767
                            },
                            map_key: None,
                            nullable: None,
                            tag: None,
                            tagged: None,
                            entity_type: None,
                            default: None,
                            fields: None
                        }
                    ]
                    .into()
                )
            },
            serde_json::from_str::<Value>(
                r#"
                { "name":  "SupportedFeatures",
                  "type": "[]SupportedFeatureKey",
                  "ignorable": true,
                  "versions":  "3+",
                  "tag": 0,
                  "taggedVersions": "3+",
                  "about": "Features supported by the broker.",
                  "fields":  [
                                { "name": "Name",
                                  "type": "string",
                                  "versions": "3+",
                                  "mapKey": true,
                                  "about": "The name of the feature." },
                                { "name": "MinVersion",
                                  "type": "int16",
                                  "versions": "3+",
                                  "about": "The minimum supported version for the feature." },
                                { "name": "MaxVersion",
                                  "type": "int16",
                                  "versions": "3+",
                                  "about": "The maximum supported version for the feature." }
                             ]
                }
                "#
            )
            .map_err(Into::into)
            .and_then(|v| Field::try_from(&Wv::from(&v)))?
        );

        Ok(())
    }

    #[test]
    fn untagged_message() -> Result<()> {
        let m = serde_json::from_str::<Value>(
            r#"
            {
              "apiKey": 25,
              "type": "request",
              "listeners": ["zkBroker", "broker"],
              "name": "AddOffsetsToTxnRequest",
              "validVersions": "0-3",
              "flexibleVersions": "3+",
              "fields": [
                { "name": "TransactionalId", "type": "string", "versions": "0+", "entityType": "transactionalId",
                  "about": "The transactional id corresponding to the transaction."},
                { "name": "ProducerId", "type": "int64", "versions": "0+", "entityType": "producerId",
                  "about": "Current producer id in use by the transactional id." },
                { "name": "ProducerEpoch", "type": "int16", "versions": "0+",
                  "about": "Current epoch associated with the producer id." },
                { "name": "GroupId", "type": "string", "versions": "0+", "entityType": "groupId",
                  "about": "The unique group identifier." }
              ]
            }
            "#,
        ).map_err(Into::into)
        .and_then(|v| Message::try_from(&Wv::from(&v)))?;

        assert_eq!(MessageKind::Request, m.kind());

        assert!(!m.has_tags());

        Ok(())
    }

    #[test]
    fn tagged_message() -> Result<()> {
        let m = Message::try_from(&Wv::from(&json!(
            {
              "apiKey": 63,
              "type": "request",
              "listeners": ["controller"],
              "name": "BrokerHeartbeatRequest",
              "validVersions": "0-1",
              "flexibleVersions": "0+",
              "fields": [
                { "name": "BrokerId", "type": "int32", "versions": "0+", "entityType": "brokerId",
                  "about": "The broker ID." },
                { "name": "BrokerEpoch", "type": "int64", "versions": "0+", "default": "-1",
                  "about": "The broker epoch." },
                { "name": "CurrentMetadataOffset", "type": "int64", "versions": "0+",
                  "about": "The highest metadata offset which the broker has reached." },
                { "name": "WantFence", "type": "bool", "versions": "0+",
                  "about": "True if the broker wants to be fenced, false otherwise." },
                { "name": "WantShutDown", "type": "bool", "versions": "0+",
                  "about": "True if the broker wants to be shut down, false otherwise." },
                { "name": "OfflineLogDirs", "type":  "[]uuid", "versions": "1+", "taggedVersions": "1+", "tag": "0",
                  "about": "Log directories that failed and went offline." }
              ]
            }
        )))?;

        assert_eq!(MessageKind::Request, m.kind());
        assert!(m.has_tags());

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    #[test]
    fn message_from_value() -> Result<()> {
        assert_eq!(
            Message {
                api_key: 19,
                kind: MessageKind::Request,
                listeners: Some(vec![
                    Listener::ZkBroker,
                    Listener::Broker,
                    Listener::Controller
                ]),
                name: String::from("CreateTopicsRequest"),
                versions: Version {
                    valid: VersionRange::from_str("0-7")?,
                    deprecated: Some(VersionRange::from_str("0-1")?),
                    flexible: VersionRange::from_str("5+")?,
                },
                common_structs: Some(vec![CommonStruct {
                    name: String::from("AddPartitionsToTxnTopic"),
                    fields: vec![
                        Field {
                            name: String::from("Name"),
                            kind: Kind(String::from("string")),
                            versions: VersionRange::from_str("0+")?,
                            map_key: Some(true),
                            nullable: None,
                            tag: None,
                            tagged: None,
                            default: None,
                            fields: None,
                            entity_type: Some(String::from("topicName")),
                            about: Some(String::from("The name of the topic.")),
                        },
                        Field {
                            name: String::from("Partitions"),
                            kind: Kind(String::from("[]int32")),
                            versions: VersionRange::from_str("0+")?,
                            about: Some(String::from(
                                "The partition indexes to add to the transaction"
                            )),
                            map_key: None,
                            nullable: None,
                            tag: None,
                            tagged: None,
                            default: None,
                            fields: None,
                            entity_type: None,
                        }
                    ],
                }]),
                fields: vec![Field {
                    name: String::from("Topics"),
                    kind: Kind(String::from("[]CreatableTopic")),
                    about: Some(String::from("The topics to create.")),
                    versions: VersionRange::from_str("0+")?,
                    map_key: None,
                    nullable: None,
                    tag: None,
                    tagged: None,
                    entity_type: None,
                    default: None,
                    fields: Some(vec![Field {
                        name: String::from("Name"),
                        kind: Kind(String::from("string")),
                        versions: VersionRange::from_str("0+")?,
                        map_key: Some(true),
                        entity_type: Some(String::from("topicName")),
                        about: Some(String::from("The topic name.")),
                        default: None,
                        nullable: None,
                        tag: None,
                        tagged: None,
                        fields: None,
                    }])
                }],
            },
            serde_json::from_str::<Value>(
                r#"
                {
                    "apiKey": 19,
                    "type": "request",
                    "listeners": ["zkBroker", "broker", "controller"],
                    "name": "CreateTopicsRequest",
                    "validVersions": "0-7",
                    "deprecatedVersions": "0-1",
                    "flexibleVersions": "5+",
                    "fields": [
                        {"name": "Topics",
                        "type": "[]CreatableTopic",
                        "versions": "0+",
                        "about": "The topics to create.",
                         "fields": [
                            { "name": "Name",
                              "type": "string",
                              "versions": "0+",
                              "mapKey": true,
                              "entityType": "topicName",
                              "about": "The topic name."
                        }]}],
                        "commonStructs": [
                            { "name": "AddPartitionsToTxnTopic",
                              "versions": "0+",
                              "fields": [
                                { "name": "Name",
                                  "type": "string",
                                  "versions": "0+",
                                  "mapKey": true,
                                  "entityType": "topicName",
                                  "about": "The name of the topic."
                                },
                                { "name": "Partitions",
                                  "type": "[]int32",
                                  "versions": "0+",
                                  "about": "The partition indexes to add to the transaction"
                                }]}]
                }
                "#
            )
            .map_err(Into::into)
            .and_then(|v| Message::try_from(&Wv::from(&v)))?
        );

        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    #[test]
    fn header_from_value() -> Result<()> {
        let v = serde_json::from_str::<Value>(
            r#"
            {
              "type": "header",
              "name": "RequestHeader",
              "validVersions": "0-2",
              "flexibleVersions": "2+",
              "fields": [
                { "name": "RequestApiKey", "type": "int16", "versions": "0+",
                  "about": "The API key of this request." },
                { "name": "RequestApiVersion", "type": "int16", "versions": "0+",
                  "about": "The API version of this request." },
                { "name": "CorrelationId", "type": "int32", "versions": "0+",
                  "about": "The correlation ID of this request." },

                { "name": "ClientId", "type": "string", "versions": "1+", "nullableVersions": "1+", "ignorable": true,
                  "flexibleVersions": "none", "about": "The client ID string." }
              ]
            }
            "#,
        )?;

        let wv = Wv::from(&v);

        assert_eq!(
            Header {
                name: String::from("RequestHeader"),
                valid: VersionRange { start: 0, end: 2 },
                flexible: VersionRange {
                    start: 2,
                    end: i16::MAX
                },
                fields: vec![
                    Field {
                        name: String::from("RequestApiKey"),
                        kind: Kind(String::from("int16")),
                        about: Some(String::from("The API key of this request.")),
                        versions: VersionRange {
                            start: 0,
                            end: 32767
                        },
                        map_key: None,
                        nullable: None,
                        tag: None,
                        tagged: None,
                        entity_type: None,
                        default: None,
                        fields: None
                    },
                    Field {
                        name: String::from("RequestApiVersion"),
                        kind: Kind(String::from("int16")),
                        about: Some(String::from("The API version of this request.")),
                        versions: VersionRange {
                            start: 0,
                            end: 32767
                        },
                        map_key: None,
                        nullable: None,
                        tag: None,
                        tagged: None,
                        entity_type: None,
                        default: None,
                        fields: None
                    },
                    Field {
                        name: String::from("CorrelationId"),
                        kind: Kind(String::from("int32")),
                        about: Some(String::from("The correlation ID of this request.")),
                        versions: VersionRange {
                            start: 0,
                            end: 32767
                        },
                        map_key: None,
                        nullable: None,
                        tag: None,
                        tagged: None,
                        entity_type: None,
                        default: None,
                        fields: None
                    },
                    Field {
                        name: String::from("ClientId"),
                        kind: Kind(String::from("string")),
                        about: Some(String::from("The client ID string.")),
                        versions: VersionRange {
                            start: 1,
                            end: 32767
                        },
                        map_key: None,
                        nullable: Some(VersionRange {
                            start: 1,
                            end: 32767
                        }),
                        tag: None,
                        tagged: None,
                        entity_type: None,
                        default: None,
                        fields: None
                    }
                ],
            },
            Header::try_from(&wv)?
        );
        Ok(())
    }

    #[test]
    fn parse_expression() -> Result<()> {
        let Expr::Array(expression) =
            syn::parse_str::<Expr>(r#"[(one, "a/b/c"), (abc, 123), (pqr, a::b::c)]"#)?
        else {
            return Err(Error::Message(String::from("expecting an array")));
        };

        let mut mappings = HashMap::new();

        for expression in expression.elems {
            let Expr::Tuple(tuple) = expression else {
                return Err(Error::Message(String::from("expecting a tuple")));
            };

            assert_eq!(2, tuple.elems.len());

            println!(
                "i: {}, ty: {}",
                tuple.to_token_stream(),
                type_name_of_val(&tuple)
            );

            let Expr::Path(ref lhs) = tuple.elems[0] else {
                return Err(Error::Message(String::from(
                    "lhs expecting a path expression",
                )));
            };

            let Some(lhs) = lhs.path.get_ident() else {
                return Err(Error::Message(String::from(
                    "lhs expecting a path ident expression",
                )));
            };

            _ = mappings.insert(lhs.clone(), tuple.elems[1].clone());

            println!("lhs: {}", lhs.to_token_stream());
            println!("rhs: {}", tuple.elems[1].to_token_stream());
        }

        let one = syn::parse_str::<Ident>("one")?;
        assert!(mappings.contains_key(&one));

        Ok(())
    }

    #[test]
    fn find_coordinator_request() -> Result<()> {
        let m = serde_json::from_str::<Value>(
            r#"
                {
                    "apiKey": 10,
                    "type": "request",
                    "listeners": ["zkBroker", "broker"],
                    "name": "FindCoordinatorRequest",
                    "validVersions": "0-4",
                    "deprecatedVersions": "0",
                    "flexibleVersions": "3+",
                    "fields": [
                        { "name": "Key", "type": "string", "versions": "0-3",
                        "about": "The coordinator key." },
                        { "name": "KeyType", "type": "int8", "versions": "1+", "default": "0", "ignorable": false,
                        "about": "The coordinator key type. (Group, transaction, etc.)" },
                        { "name": "CoordinatorKeys", "type": "[]string", "versions": "4+",
                        "about": "The coordinator keys." }
                    ]
                }
            "#,
        )
        .map_err(Into::into)
        .and_then(|v| Message::try_from(&Wv::from(&v)))?;

        assert!(!m.has_records());
        assert_eq!(MessageKind::Request, m.kind());
        assert_eq!("FindCoordinatorRequest", m.name());
        assert_eq!(
            Version {
                valid: VersionRange { start: 0, end: 4 },
                deprecated: Some(VersionRange { start: 0, end: 0 }),
                flexible: VersionRange {
                    start: 3,
                    end: i16::MAX
                },
            },
            m.version()
        );

        assert_eq!("Key", m.fields()[0].name());
        assert_eq!(Kind::new("string"), m.fields()[0].kind);
        assert_eq!(VersionRange { start: 0, end: 3 }, m.fields()[0].versions());
        assert_eq!(Some("The coordinator key."), m.fields()[0].about());

        Ok(())
    }

    #[test]
    fn fetch_response() -> Result<()> {
        let m = serde_json::from_str::<Value>(
            r#"
                {
                    "apiKey": 1,
                    "type": "response",
                    "name": "FetchResponse",
                    "validVersions": "0-16",
                    "flexibleVersions": "12+",
                    "fields": [
                        { "name": "NodeEndpoints", "type": "[]NodeEndpoint", "versions": "16+", "taggedVersions": "16+", "tag": 0,
                          "about": "Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.", "fields": [
                          { "name": "NodeId", "type": "int32", "versions": "16+",
                            "mapKey": true, "entityType": "brokerId", "about": "The ID of the associated node."},
                          { "name": "Host", "type": "string", "versions": "16+", "about": "The node's hostname." },
                          { "name": "Port", "type": "int32", "versions": "16+", "about": "The node's port." },
                          { "name": "Rack", "type": "string", "versions": "16+", "nullableVersions": "16+", "default": "null",
                            "about": "The rack of the node, or null if it has not been assigned to a rack." }
                        ]}
                    ]
                }
            "#,
        )
        .map_err(Into::into)
        .and_then(|v| Message::try_from(&Wv::from(&v)))?;

        assert_eq!(MessageKind::Response, m.kind());
        assert_eq!("FetchResponse", m.name());
        assert_eq!(
            Version {
                valid: VersionRange { start: 0, end: 16 },
                deprecated: None,
                flexible: VersionRange {
                    start: 12,
                    end: i16::MAX
                },
            },
            m.version()
        );

        assert_eq!("NodeEndpoints", m.fields()[0].name());
        assert_eq!(Kind::new("[]NodeEndpoint"), m.fields()[0].kind);
        assert_eq!(
            VersionRange {
                start: 16,
                end: i16::MAX
            },
            m.fields()[0].versions()
        );

        let node_id = &m.fields()[0].fields().unwrap_or_default()[0];

        assert_eq!("NodeId", node_id.name());
        assert_eq!(Kind::new("int32"), node_id.kind);
        assert_eq!(
            VersionRange {
                start: 16,
                end: i16::MAX
            },
            node_id.versions()
        );

        Ok(())
    }
}
