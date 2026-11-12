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

// This build script only ever runs at compile time, generating this crate's protocol
// types from the upstream Kafka JSON descriptors in `message/`; a panic here fails the
// build, not a shipped binary, so it follows the same convention as the hand-written
// codegen in `nisshi-model` (see e.g. `nisshi-model/src/lib.rs`'s `ToTokens` impls).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use convert_case::{Case, Casing};
use nisshi_model::{CommonStruct, Field, Listener, Message, MessageKind, wv::Wv};
use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    env, error,
    fmt::{self, Display},
    fs,
    io::{self, BufRead, BufReader, Cursor, Seek, Write},
    path::Path,
};
use syn::{Expr, Type};

/// Fields whose values are secrets, as `(message, struct, field)` using the
/// names in the message descriptors. The generated `Debug` writes these as
/// `[hidden]`, so logging a message, `Body` or `Frame` doesn't log them.
///
/// Kafka 3.9.1 keeps SASL auth bytes out of its logs by never passing them to
/// the request logger ([SaslServerAuthenticator.java#L425-L504]). It does log
/// `SaltedPassword` and delegation token `Hmac` values, since its request
/// logger only redacts configs ([RequestChannel.scala#L186-L212]). We hide
/// those as well, following `DelegationToken.toString`
/// ([DelegationToken.java#L74-L79]).
///
/// Kafka hides an `AlterConfigs` or `IncrementalAlterConfigs` value only when
/// the config it names is sensitive, such as a password. We hide every value,
/// topic configs included, because we keep no list of sensitive config names.
///
/// The build fails if an entry matches no field, so a renamed field can't
/// silently stop being hidden.
///
/// [SaslServerAuthenticator.java#L425-L504]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/security/authenticator/SaslServerAuthenticator.java#L425-L504
/// [RequestChannel.scala#L186-L212]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/network/RequestChannel.scala#L186-L212
/// [DelegationToken.java#L74-L79]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/security/token/delegation/DelegationToken.java#L74-L79
const SENSITIVE_FIELDS: &[(&str, &str, &str)] = &[
    ("AlterConfigsRequest", "AlterableConfig", "Value"),
    ("IncrementalAlterConfigsRequest", "AlterableConfig", "Value"),
    (
        "SaslAuthenticateRequest",
        "SaslAuthenticateRequest",
        "AuthBytes",
    ),
    (
        "SaslAuthenticateResponse",
        "SaslAuthenticateResponse",
        "AuthBytes",
    ),
    (
        "AlterUserScramCredentialsRequest",
        "ScramCredentialUpsertion",
        "SaltedPassword",
    ),
    (
        "CreateDelegationTokenResponse",
        "CreateDelegationTokenResponse",
        "Hmac",
    ),
    (
        "DescribeDelegationTokenResponse",
        "DescribedDelegationToken",
        "Hmac",
    ),
    (
        "ExpireDelegationTokenRequest",
        "ExpireDelegationTokenRequest",
        "Hmac",
    ),
    (
        "RenewDelegationTokenRequest",
        "RenewDelegationTokenRequest",
        "Hmac",
    ),
];

/// `bytes` fields that hold no secret, so the derived `Debug` may log them.
///
/// The build fails on a `bytes` field that is in neither this list nor
/// [`SENSITIVE_FIELDS`], so a new descriptor can't add a secret that we log.
const LOGGABLE_BYTES_FIELDS: &[(&str, &str, &str)] = &[
    // The server sends the salt to the client in the clear during SCRAM.
    (
        "AlterUserScramCredentialsRequest",
        "ScramCredentialUpsertion",
        "Salt",
    ),
    (
        "DescribeGroupsResponse",
        "DescribedGroupMember",
        "MemberMetadata",
    ),
    (
        "DescribeGroupsResponse",
        "DescribedGroupMember",
        "MemberAssignment",
    ),
    ("JoinGroupRequest", "JoinGroupRequestProtocol", "Metadata"),
    ("JoinGroupResponse", "JoinGroupResponseMember", "Metadata"),
    ("PushTelemetryRequest", "PushTelemetryRequest", "Metrics"),
    (
        "SyncGroupRequest",
        "SyncGroupRequestAssignment",
        "Assignment",
    ),
    ("SyncGroupResponse", "SyncGroupResponse", "Assignment"),
];

fn is_sensitive(module: &syn::Path, name: &Type, field: &Field) -> bool {
    let module = module.to_token_stream().to_string();
    let name = name.to_token_stream().to_string();

    SENSITIVE_FIELDS
        .iter()
        .any(|(m, s, f)| m.to_case(Case::Snake) == module && *s == name && *f == field.name())
}

/// The derive attribute for a generated struct. `Debug` is left out when the
/// struct has a sensitive field: [`debug_impl`] writes it instead.
fn derived(module: &syn::Path, name: &Type, fields: &[Field]) -> TokenStream {
    let debug =
        (!fields.iter().any(|field| is_sensitive(module, name, field))).then(|| quote!(Debug,));

    if fields.iter().any(Field::has_float) {
        quote! {
            #[derive(Clone, #debug Default, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
        }
    } else {
        quote! {
            #[derive(Clone, #debug Default, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
        }
    }
}

/// A `Debug` impl for a struct with a sensitive field, matching the derived
/// output except that sensitive fields are written as `[hidden]`.
fn debug_impl(
    module: &syn::Path,
    name: &Type,
    fields: &[Field],
    include_tag: bool,
) -> Option<TokenStream> {
    fields
        .iter()
        .any(|field| is_sensitive(module, name, field))
        .then(|| {
            let label = name.to_token_stream().to_string();

            let entries = fields
                .iter()
                .filter(|field| include_tag || field.tag().is_none())
                .map(|field| {
                    let ident = field.ident();
                    let label = syn::ext::IdentExt::unraw(&ident).to_string();

                    if is_sensitive(module, name, field) {
                        quote! {
                            .field(#label, &crate::Redacted)
                        }
                    } else {
                        quote! {
                            .field(#label, &self.#ident)
                        }
                    }
                })
                .chain((!include_tag).then(|| {
                    quote! {
                        .field("tag_buffer", &self.tag_buffer)
                    }
                }));

            quote! {
                impl ::std::fmt::Debug for #name {
                    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                        f.debug_struct(#label)
                            #(#entries)*
                            .finish()
                    }
                }
            }
        })
}

/// Panics if a [`SENSITIVE_FIELDS`] or [`LOGGABLE_BYTES_FIELDS`] entry matches
/// no field in `messages`, if a `bytes` field is in neither list, or if a
/// [`SENSITIVE_FIELDS`] entry is a tagged field.
fn check_sensitive_fields(messages: &[Message]) {
    struct Found {
        message: String,
        name: String,
        field: String,
        bytes: bool,
        tagged: bool,
    }

    impl Found {
        fn is(&self, (message, name, field): &(&str, &str, &str)) -> bool {
            self.message == *message && self.name == *name && self.field == *field
        }
    }

    fn walk(message: &str, name: &str, fields: &[Field], found: &mut Vec<Found>) {
        for field in fields {
            found.push(Found {
                message: message.into(),
                name: name.into(),
                field: field.name().into(),
                bytes: field.kind().name().trim_start_matches("[]") == "bytes",
                tagged: field.tag().is_some(),
            });

            if let Some(children) = field.fields() {
                let child = field.kind().type_name().to_token_stream().to_string();
                walk(message, &child, children, found);
            }
        }
    }

    let mut found = Vec::new();

    for message in messages {
        walk(message.name(), message.name(), message.fields(), &mut found);

        for cs in message.common_structs().unwrap_or(&[][..]) {
            walk(message.name(), cs.name(), cs.fields(), &mut found);
        }
    }

    for (list, entries) in [
        ("SENSITIVE_FIELDS", SENSITIVE_FIELDS),
        ("LOGGABLE_BYTES_FIELDS", LOGGABLE_BYTES_FIELDS),
    ] {
        for entry in entries {
            let (message, name, field) = entry;

            assert!(
                found.iter().any(|found| found.is(entry)),
                "{list}: no field {field} in struct {name} of message {message}; \
                 update the entry to match the descriptor"
            );
        }
    }

    for found in &found {
        let sensitive = SENSITIVE_FIELDS.iter().any(|entry| found.is(entry));
        let (message, name, field) = (&found.message, &found.name, &found.field);

        assert!(
            !found.bytes || sensitive || LOGGABLE_BYTES_FIELDS.iter().any(|entry| found.is(entry)),
            "bytes field {field} in struct {name} of message {message} is in neither \
             SENSITIVE_FIELDS nor LOGGABLE_BYTES_FIELDS; add it to SENSITIVE_FIELDS if it \
             holds a secret, or to LOGGABLE_BYTES_FIELDS if it may be logged"
        );

        // The internal (mezzanine) struct keeps a tagged field's encoded value
        // in `tag_buffer`, which its `Debug` writes in full, so the generated
        // `Debug` can't hide a tagged field.
        assert!(
            !(sensitive && found.tagged),
            "SENSITIVE_FIELDS: field {field} in struct {name} of message {message} is \
             tagged, and the generated Debug can't hide a tagged field"
        );
    }
}

#[derive(Debug)]
#[allow(dead_code)]
enum Error {
    ExpectingArrayExpr(Box<Expr>),
    ExpectingPathExpr(Box<Expr>),
    ExpectingPathIdentExpr(Box<Expr>),
    ExpectingTupleExpr(Box<Expr>),
    Glob(glob::GlobError),
    Io(io::Error),
    Json(serde_json::Error),
    KafkaModel(nisshi_model::Error),
    Pattern(glob::PatternError),
    Syn(syn::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<glob::GlobError> for Error {
    fn from(value: glob::GlobError) -> Self {
        Error::Glob(value)
    }
}

impl From<glob::PatternError> for Error {
    fn from(value: glob::PatternError) -> Self {
        Error::Pattern(value)
    }
}

impl From<syn::Error> for Error {
    fn from(value: syn::Error) -> Self {
        Error::Syn(value)
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::Io(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Error::Json(value)
    }
}

impl From<nisshi_model::Error> for Error {
    fn from(value: nisshi_model::Error) -> Self {
        Error::KafkaModel(value)
    }
}

impl error::Error for Error {}

type Result<T, E = Error> = std::result::Result<T, E>;

fn read_value<P>(filename: P) -> Result<Value>
where
    P: AsRef<Path>,
{
    fs::File::open(filename)
        .map(BufReader::new)
        .and_then(|r| {
            r.lines()
                .try_fold(Cursor::new(Vec::new()), |mut acc, line| {
                    if let Ok(mut line) = line {
                        if let Some(position) = line.find("//") {
                            line.truncate(position);
                        }
                        acc.write_all(line.as_bytes())?;
                    }
                    Ok(acc)
                })
        })
        .and_then(|mut r| r.rewind().map(|()| r))
        .and_then(|r| serde_json::from_reader(r).map_err(Into::into))
        .map_err(Into::into)
}

fn kind(
    parent: Option<&Field>,
    module: &syn::Path,
    f: &Field,
    dependencies: &[Type],
) -> TokenStream {
    let _ = (module, dependencies);

    #[cfg(feature = "diagnostics")]
    eprintln!(
        "module: {}, field: {}, dependencies: {:?}, primitive: {}, nullable.is_none: {}, \
         versions.is_mandatory: {}",
        module.to_token_stream(),
        f.name(),
        dependencies
            .iter()
            .map(|t| t.to_token_stream().to_string())
            .collect::<Vec<String>>(),
        f.kind().is_primitive(),
        f.nullable().is_none(),
        f.versions()
            .is_mandatory(parent.map(|parent| parent.versions()))
    );

    if f.tag().is_some() {
        let t = f.kind().type_name();

        if f.kind().is_sequence() {
            quote! {
                Option<Vec<#t>>
            }
        } else {
            quote! {
                Option<#t>
            }
        }
    } else if f.kind().is_sequence() {
        let t = f.kind().type_name();
        quote! {
            Option<Vec<#t>>
        }
    } else {
        let t = f.kind().type_name();
        if f.nullable().is_none() && f.versions().is_mandatory(parent.map(Field::versions)) {
            quote! {
                #t
            }
        } else {
            quote! {
                Option<#t>
            }
        }
    }
}

/// Whether a field's generated type is `Option`-wrapped, mirroring the
/// branching in [`kind`] above (needed to pick the right `arbitrary(with =
/// ..)` helper for a scalar field, since that helper must return exactly the
/// field's generated type).
fn is_optional(parent: Option<&Field>, f: &Field) -> bool {
    f.tag().is_some()
        || f.kind().is_sequence()
        || !(f.nullable().is_none() && f.versions().is_mandatory(parent.map(Field::versions)))
}

/// An `#[arbitrary(..)]` field attribute that makes a fuzzed struct match the
/// shape a real request/response can actually have at `latest` (the
/// containing message's highest `validVersions` entry), and, for a field
/// that a later version dropped, at the versions that still have it. The
/// fuzz targets encode at every version of an API, picked by
/// `nisshi_fuzz_harness::api_version`.
///
/// A field whose Rust type is `Option`-wrapped only because it isn't present
/// in every version (see [`kind`]) is otherwise filled in by the derive with
/// no regard for versioning: it can come back `Some` for a field that
/// doesn't exist at version `latest` at all, or `None` for one that is
/// mandatory there, combinations no real encoder/decoder pair would ever
/// produce or accept for that version (e.g. `FetchTopic` getting neither of
/// its version-gated `topic`/`topic_id` fields, or both at once). This
/// function closes that gap:
///
/// - Tagged and not present in `latest`'s version range at all: always
///   `None`, via `#[arbitrary(default)]` (sound regardless of kind, since
///   `Option<T>`'s `Default` is `None` for every `T`).
/// - Untagged, and not nullable at `latest` (which includes every field that
///   a later version dropped): a wire-valid message at a version that has the
///   field always carries a value, so only the `Some` arm is ever generated.
///   A dropped field that was nullable at the older versions is never `None`
///   either, so that no value is generated that a client could not send at
///   some version that has the field. Only `FindCoordinatorResponse`'s
///   `error_message` is such a field, and it is in a response.
/// - Otherwise (tagged, or nullable at `latest`): both arms are legitimate
///   wire shapes at `latest`, so the derive's own `Option` generation is left
///   alone.
///
/// Two kinds need a hand-written generator regardless of the above, since
/// `arbitrary` can't derive them on its own:
///
/// - `bytes` maps to `bytes::Bytes`, a foreign type the orphan rule stops us
///   implementing `Arbitrary` for here, so fuzzed fields are instead built
///   from an arbitrary `Vec<u8>` via `nisshi-sans-io/src/arbitrary_support.rs`.
/// - `records` maps to `crate::RecordBatch`, whose `crc`/`batch_length`
///   fields are checksums/lengths over the rest of the batch that a naive
///   derive would produce inconsistent, invalid values for; `Default` (an
///   empty batch, i.e. `None` when `Option`-wrapped) is used instead. This is
///   sound under the same "nullable at `latest`" rule above: every `records`
///   field in the upstream descriptors declares `nullableVersions` covering
///   its whole `versions` range, so `None` is always a legitimate value
///   wherever the field exists.
///
/// Neither `bytes` nor `records` occurs as a sequence in the upstream Kafka
/// message descriptors (only as a scalar or `tag`/nullable-optional scalar),
/// so only those two shapes are handled here.
fn arbitrary_field_attribute(parent: Option<&Field>, latest: i16, field: &Field) -> TokenStream {
    let is_opt = is_optional(parent, field);

    // A tagged field is `None` unless a tag says otherwise, so the default is sound for it. An
    // untagged field that a later version dropped is not: it is mandatory at the versions that
    // have it, and the fuzzers encode at all of them (see below).
    if is_opt && field.tag().is_some() && !field.versions().within(latest) {
        return quote! {
            #[cfg_attr(feature = "arbitrary", arbitrary(default))]
        };
    }

    match field.kind().name() {
        "bytes" => {
            // Only `crate::arbitrary_support::bytes` is checked in (rather than
            // also a hand-written `bytes_option` counterpart) because no
            // broker-listened message currently has a nullable `bytes` field;
            // a hand-written function nothing generates a call to would be
            // dead code. Should that change, this inline closure calls it.
            let with = if is_opt {
                quote! {
                    |u: &mut arbitrary::Unstructured<'_>| crate::arbitrary_support::bytes(u).map(Some)
                }
            } else {
                quote!(crate::arbitrary_support::bytes)
            };

            quote! {
                #[cfg_attr(feature = "arbitrary", arbitrary(with = #with))]
            }
        }

        "records" => quote! {
            #[cfg_attr(feature = "arbitrary", arbitrary(default))]
        },

        _ if is_opt
            && field.tag().is_none()
            && !field.nullable().is_some_and(|range| range.within(latest)) =>
        {
            let t = field.kind().type_name();

            let with = if field.kind().is_sequence() {
                quote! {
                    |u: &mut arbitrary::Unstructured<'_>| u.arbitrary::<Vec<#t>>().map(Some)
                }
            } else {
                quote! {
                    |u: &mut arbitrary::Unstructured<'_>| u.arbitrary::<#t>().map(Some)
                }
            };

            quote! {
                #[cfg_attr(feature = "arbitrary", arbitrary(with = #with))]
            }
        }

        _ => quote!(),
    }
}

fn tag_kind(
    _parent: Option<&Field>,
    module: &syn::Path,
    f: &Field,
    _dependencies: &[Type],
) -> TokenStream {
    #[cfg(feature = "diagnostics")]
    eprintln!(
        "module: {}, field: {}, primitive: {}, nullable.is_none: {}",
        module.to_token_stream(),
        f.name(),
        f.kind().is_primitive(),
        f.nullable().is_none(),
    );

    if f.kind().is_sequence() {
        let t = f.kind().type_name();
        quote! {
            Vec<#module::#t>
        }
    } else if f.kind().is_primitive() {
        let t = f.kind().type_name();
        quote! {
            #t
        }
    } else {
        let t = f.kind().type_name();
        quote! {
            #module::#t
        }
    }
}

fn body_into_version(messages: &[Message]) -> TokenStream {
    let variants = messages
        .iter()
        .map(|message| {
            let name = message.type_name();

            quote! {
                Self::#name(message) => message.into_version(api_version).into(),
            }
        })
        .collect::<Vec<_>>();

    quote! {
        impl crate::IntoVersion for Body {
            fn into_version(self, api_version: i16) -> Self {
                match self {
                    #(#variants)*
                }
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
fn body_enum(messages: &[Message], include_tag: bool) -> TokenStream {
    let variants = messages.iter().map(|message| {
        let name = message.type_name();
        let module =
            syn::parse_str::<syn::Path>(&name.to_token_stream().to_string().to_case(Case::Snake))
                .unwrap();

        quote! {
            #name(#module::#name)
        }
    });

    if include_tag {
        let from_mezzanine = {
            let conversions = messages.iter().map(|message| {
                let name = message.type_name();
                let module = syn::parse_str::<syn::Path>(
                    &name.to_token_stream().to_string().to_case(Case::Snake),
                )
                .unwrap();

                quote! {
                    mezzanine::Body::#name(inner) => {
                        Body::#name(crate::#module::#name::from(inner))
                    }
                }
            });

            quote! {
                impl From<mezzanine::Body> for Body {
                    fn from(value: mezzanine::Body) -> Self {
                        match value {
                            #(#conversions),*
                        }
                    }
                }
            }
        };

        quote! {
            #[non_exhaustive]
            #[derive(Clone, Debug, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            #[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
            #[serde(from = "mezzanine::Body")]
            #[serde(into = "mezzanine::Body")]
            #[doc = "A Kafka API request or response message body."]
            pub enum Body {
                #(#variants),*
            }

            #from_mezzanine
        }
    } else {
        let from_tagged = {
            let conversions = messages.iter().map(|message| {
                let name = message.type_name();
                let module = syn::parse_str::<syn::Path>(
                    &name.to_token_stream().to_string().to_case(Case::Snake),
                )
                .unwrap();

                quote! {
                    crate::Body::#name(inner) => {
                        Body::#name(#module::#name::from(inner))
                    }
                }
            });

            quote! {
                impl From<crate::Body> for Body {
                    fn from(value: crate::Body) -> Self {
                        match value {
                            #(#conversions),*
                        }
                    }
                }
            }
        };

        quote! {
            #[derive(Clone, Debug, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
            pub(crate) enum Body {
                #(#variants),*
            }

            #from_tagged
        }
    }
}

fn visibility_field_kind(
    parent: Option<&Field>,
    visibility: Option<&TokenStream>,
    fields: &[Field],
    module: &syn::Path,
    dependencies: &[Type],
    include_tag: bool,
    latest: i16,
) -> Vec<TokenStream> {
    fields
        .iter()
        .filter(|field| include_tag || field.tag().is_none())
        .map(|field| {
            let ident = field.ident();
            let kind = kind(parent, module, field, dependencies);
            let arbitrary_attr =
                include_tag.then(|| arbitrary_field_attribute(parent, latest, field));

            field.about().map_or(
                quote! {
                    #arbitrary_attr
                    #visibility #ident: #kind
                },
                |about| {
                    let about = about.replace("[", "\\[").replace("]", "\\]");

                    quote! {
                        #[doc = #about]
                        #arbitrary_attr
                        #visibility #ident: #kind
                    }
                },
            )
        })
        .collect()
}

fn root_message_struct(message: &Message, include_tag: bool) -> TokenStream {
    let name = &message.type_name();
    let api_key = message.api_key();
    let fields = message.fields();
    let common_structs = message.common_structs();
    let message_name = message.name();

    let module =
        syn::parse_str::<syn::Path>(&name.to_token_stream().to_string().to_case(Case::Snake))
            .unwrap();

    let latest = message.version().valid().end;

    let tokens = message_struct(
        &module,
        None,
        name,
        fields,
        common_structs,
        include_tag,
        latest,
    );

    if include_tag {
        quote! {
            pub mod #module {
                use super::*;

                #tokens

                impl From<#name> for Body {
                    fn from(value: #name) -> Body {
                        Body::#name(value)
                    }
                }

                impl TryFrom<Body> for #name {
                    type Error = Error;

                    fn try_from(value: Body) -> Result<Self, Self::Error> {
                        if let Body::#name(inner) = value {
                            Ok(inner)
                        } else {
                            Err(Error::UnexpectedType(format!("{value:?}")))
                        }
                    }
                }

                impl ApiKey for #name {
                    const KEY:i16 = #api_key;
                }

                impl ApiName for #name {
                    const NAME: &'static str = #message_name;
                }

            }

            pub use #module::#name;

        }
    } else {
        quote! {
            pub mod #module {
                #tokens
            }
        }
    }
}

fn message_struct_into_version(name: &Type, fields: &[Field]) -> TokenStream {
    if fields.iter().any(|f| f.tag().is_some()) {
        let none = fields.iter().filter(|f| f.tag().is_some()).map(|field| {
            let f = field.ident();
            let start = field.versions().start;
            let end = field.versions().end;

            quote! {
                if !(#start ..= #end).contains(&api_version) {
                    self.#f = None;
                }
            }
        });

        quote! {
            impl crate::IntoVersion for #name {
                fn into_version(mut self, api_version: i16) -> Self {
                    #(#none)*
                    self
                }
            }
        }
    } else {
        quote! {
            impl crate::IntoVersion for #name {
                fn into_version(self, _api_version: i16) -> Self {
                    self
                }
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
fn message_struct(
    module: &syn::Path,
    parent: Option<&Field>,
    name: &Type,
    fields: &[Field],
    common_structs: Option<&[CommonStruct]>,
    include_tag: bool,
    latest: i16,
) -> TokenStream {
    let dependencies: Vec<Type> = fields
        .iter()
        .filter(|f| f.fields().is_some())
        .map(|f| f.kind().type_name())
        .chain(
            common_structs
                .unwrap_or(&[][..])
                .iter()
                .map(CommonStruct::type_name),
        )
        .collect();

    let token_streams: Vec<TokenStream> = fields
        .iter()
        .filter_map(|f| {
            f.fields().as_ref().map(|children| {
                message_struct(
                    module,
                    Some(f),
                    &f.kind().type_name(),
                    children,
                    None,
                    include_tag,
                    latest,
                )
            })
        })
        .chain(common_structs.unwrap_or(&[][..]).iter().map(|cs| {
            common_struct(
                parent,
                module,
                &cs.type_name(),
                cs.fields(),
                include_tag,
                latest,
            )
        }))
        .collect();

    let vfk = visibility_field_kind(
        parent,
        Some(&quote!(pub)),
        fields,
        module,
        &dependencies,
        include_tag,
        latest,
    );

    let maximum_allocation_size = maximum_allocation_size(name, fields, include_tag);

    if include_tag {
        let tags: Vec<TokenStream> = fields
            .iter()
            .filter(|field| field.tag().is_some())
            .map(|field| {
                let f = field.ident();
                let k = tag_kind(
                    parent,
                    &syn::parse_str::<syn::Path>(&format!("crate::mezzanine::{}",module.to_token_stream())).unwrap(),
                    field,
                    &dependencies,
                );

                let tag = field.tag().unwrap();

                if field.kind().is_primitive() {
                    quote! {
                        let #f = value.tag_buffer.as_ref().and_then(|tag_buffer| tag_buffer.decode::<#k>(&#tag).ok().unwrap_or(None))
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        let #f = if let Some(tag_buffer) = value.tag_buffer.as_ref() {
                            if let Ok(Some(#f)) = tag_buffer.decode::<#k>(&#tag)
                            {
                                Some(#f.into_iter().map(Into::into).collect())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                } else {
                    quote! {
                        let #f = if let Some(tag_buffer) = value.tag_buffer.as_ref() {
                            if let Ok(Some(#f)) =
                                tag_buffer.decode::<#k>(&#tag)
                            {
                                Some(#f.into())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                }

            })
            .collect();

        let assignments = fields
            .iter()
            .map(|field| {
                let f = field.ident();

                if field.tag().is_some() {
                    quote! {
                        #f
                    }
                } else if field.kind().is_primitive() || field.kind().is_sequence_of_primitive() {
                    quote! {
                        #f: value.#f
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        #f: value.#f.map(|v| v.into_iter().map(Into::into).collect())
                    }
                } else if field.nullable().is_some() {
                    quote! {
                        #f: value.#f.map(|#f|#f.into())
                    }
                } else {
                    quote! {
                        #f: value.#f.into()
                    }
                }
            })
            .collect::<Vec<_>>();

        let builders = fields
            .iter()
            .map(|field| {
                let ident = field.ident();
                let kind = kind(parent, module, field, &dependencies);

                quote! {
                    pub fn #ident(mut self, #ident: #kind) -> Self {
                        self.#ident = #ident;
                        self
                    }
                }
            })
            .collect::<Vec<_>>();

        let mezzanine_name = syn::parse_str::<syn::Path>(&format!(
            "crate::mezzanine::{}::{}",
            module.to_token_stream(),
            name.to_token_stream()
        ))
        .unwrap();

        let from_mezzanine = (!fields.is_empty()).then(|| {
            quote! {
                impl From<#mezzanine_name> for #name {
                    fn from(value: #mezzanine_name) -> Self {
                        #(#tags;)*

                        Self {
                            #(#assignments,)*
                        }
                    }
                }
            }
        });

        let derived = derived(module, name, fields);
        let debug_impl = debug_impl(module, name, fields, include_tag);

        let visibility = if include_tag {
            quote! {
                pub
            }
        } else {
            quote! {
                pub(crate)
            }
        };

        let into_version = message_struct_into_version(name, fields);

        quote! {
            #[non_exhaustive]
            #derived
            #[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
            #visibility struct #name {
                #(#vfk,)*
            }

            #debug_impl

            #from_mezzanine

            #maximum_allocation_size

            #into_version

            impl #name {
                #(#builders)*
            }

            #(#token_streams)*
        }
    } else {
        #[cfg(feature = "diagnostics")]
        eprintln!(
            "mezzanine, module: {}, name: {}",
            module.to_token_stream(),
            name.to_token_stream(),
        );

        let tags: Vec<TokenStream> = fields
            .iter()
            .filter(|field| field.tag().is_some())
            .map(|field| {
                let f = field.ident();
                let k = tag_kind(
                    parent,
                    &syn::parse_str::<syn::Path>(&format!(
                        "crate::mezzanine::{}",
                        module.to_token_stream()
                    ))
                    .unwrap(),
                    field,
                    &dependencies,
                );

                let tag = field.tag().unwrap();

                #[cfg(feature = "diagnostics")]
                eprintln!(
                    "mezzanine, module: {}, name: {}, field: {}",
                    module.to_token_stream(),
                    name.to_token_stream(),
                    f.to_token_stream(),
                );

                if field.kind().is_primitive() {
                    quote! {
                        if let Some(#f) = value.#f
                            && let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &#f) {
                            tag_buffer.push(encoded);
                        }
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        if let Some(#f) = value.#f {
                            let mezzanine: #k = #f.into_iter().map(Into::into).collect();

                            if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &mezzanine) {
                                tag_buffer.push(encoded);
                            }
                        }
                    }
                } else {
                    quote! {
                        if let Some(#f) = value.#f {
                            let mezzanine: #k = #f.into();

                            if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &mezzanine) {
                                tag_buffer.push(encoded);
                            }
                        }
                    }
                }
            })
            .collect();

        let tag_capacity = tags.len();

        let assignments: Vec<TokenStream> = fields
            .iter()
            .filter(|field| field.tag().is_none())
            .map(|field| {
                let f = field.ident();

                if field.kind().is_primitive() || field.kind().is_sequence_of_primitive() {
                    quote! {
                        #f: value.#f
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        #f: value.#f.map(|v| v.into_iter().map(Into::into).collect())
                    }
                } else if field.nullable().is_some() {
                    quote! {
                        #f: value.#f.map(|#f|#f.into())
                    }
                } else {
                    quote! {
                        #f: value.#f.into()
                    }
                }
            })
            .collect();

        let tagged_name = syn::parse_str::<syn::Path>(&format!(
            "crate::{}::{}",
            module.to_token_stream(),
            name.to_token_stream()
        ))
        .unwrap();

        let from_tagged = (!fields.is_empty()).then(|| {
            quote! {
                impl From<#tagged_name> for #name {
                    fn from(value: #tagged_name) -> Self {
                        #[allow(unused_mut)]
                        let mut tag_buffer = Vec::with_capacity(#tag_capacity);

                        #(#tags)*

                        Self {
                            #(#assignments,)*
                            tag_buffer: Some(tag_buffer.into()),
                        }
                    }
                }
            }
        });

        let derived = derived(module, name, fields);
        let debug_impl = debug_impl(module, name, fields, include_tag);

        quote! {
            #derived
            pub(crate) struct #name {
                #(#vfk,)*
                pub tag_buffer: Option<crate::primitive::tagged::TagBuffer>,
            }

            #debug_impl

            #from_tagged

            #(#token_streams)*
        }
    }
}

fn maximum_allocation_size(name: &Type, fields: &[Field], include_tag: bool) -> TokenStream {
    let sizes = fields
        .iter()
        .filter(|field| include_tag || field.tag().is_none())
        .map(|field| {
            let f = field.ident();

            quote! {
                total += self.#f.maximum_allocation_size()?
            }
        })
        .collect::<Vec<_>>();

    quote! {
        impl crate::MaximumAllocationSize for #name {
            fn maximum_allocation_size(&self) -> Result<usize, crate::Error> {
                let mut total:usize = 0;

                #(#sizes;)*

                Ok(total)
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
fn common_struct(
    parent: Option<&Field>,
    module: &syn::Path,
    name: &Type,
    fields: &[Field],
    include_tag: bool,
    latest: i16,
) -> TokenStream {
    let vis = quote!(pub);
    let vfk = visibility_field_kind(parent, Some(&vis), fields, module, &[], include_tag, latest);
    let maximum_allocation_size = maximum_allocation_size(name, fields, include_tag);

    if include_tag {
        let assignments: Vec<TokenStream> = fields
            .iter()
            .filter(|field| include_tag || field.tag().is_none())
            .map(|field| {
                let f = field.ident();

                if field.kind().is_primitive() || field.kind().is_sequence_of_primitive() {
                    quote! {
                        #f: value.#f
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        #f: value.#f.map(|v| v.into_iter().map(Into::into).collect())
                    }
                } else {
                    quote! {
                        #f: value.#f.into()
                    }
                }
            })
            .collect();

        let builders = fields
            .iter()
            .map(|field| {
                let ident = field.ident();
                let kind = kind(parent, module, field, &[]);

                quote! {
                    pub fn #ident(mut self, #ident: #kind) -> Self {
                        self.#ident = #ident;
                        self
                    }
                }
            })
            .collect::<Vec<_>>();

        let from = syn::parse_str::<syn::Path>(&format!(
            "crate::mezzanine::{}::{}",
            module.to_token_stream(),
            name.to_token_stream()
        ))
        .unwrap();

        let derived = derived(module, name, fields);
        let debug_impl = debug_impl(module, name, fields, include_tag);

        let visibility = if include_tag {
            quote! {
                pub
            }
        } else {
            quote! {
                pub(crate)
            }
        };

        quote! {
            #[non_exhaustive]
            #derived
            #[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
            #visibility struct #name {
                #(#vfk,)*
            }

            #debug_impl

            impl #name {
                #(#builders)*
            }

            #maximum_allocation_size

            impl From<#from> for #name {
                fn from(value: #from) -> Self {
                    Self {
                        #(#assignments,)*
                    }
                }
            }
        }
    } else {
        let tags: Vec<TokenStream> = fields
            .iter()
            .filter(|field| field.tag().is_some())
            .map(|field| {
                let f = field.ident();
                let k = tag_kind(
                    parent,
                    &syn::parse_str::<syn::Path>(&format!(
                        "crate::mezzanine::{}",
                        module.to_token_stream()
                    ))
                    .unwrap(),
                    field,
                    &[],
                );

                let tag = field.tag().unwrap();

                #[cfg(feature = "diagnostics")]
                eprintln!(
                    "mezzanine, module: {}, name: {}, field: {}",
                    module.to_token_stream(),
                    name.to_token_stream(),
                    f.to_token_stream(),
                );

                if field.kind().is_primitive() {
                    quote! {
                        if let Some(#f) = value.#f {
                            if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &#f) {
                                tag_buffer.push(encoded);
                            }
                        }
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        if let Some(#f) = value.#f {
                            let mezzanine: #k = #f.into_iter().map(Into::into).collect();

                            if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &mezzanine) {
                                tag_buffer.push(encoded);
                            }
                        }
                    }
                } else {
                    quote! {
                        if let Some(#f) = value.#f {
                            let mezzanine: #k = #f.into();

                            if let Ok(encoded) = crate::primitive::tagged::TagField::encode(#tag, &mezzanine) {
                                tag_buffer.push(encoded);
                            }
                        }
                    }
                }
            })
            .collect();

        let tag_capacity = tags.len();

        let assignments: Vec<TokenStream> = fields
            .iter()
            .filter(|field| field.tag().is_none())
            .map(|field| {
                let f = field.ident();

                if field.kind().is_primitive() || field.kind().is_sequence_of_primitive() {
                    quote! {
                        #f: value.#f
                    }
                } else if field.kind().is_sequence() {
                    quote! {
                        #f: value.#f.map(|v| v.into_iter().map(Into::into).collect())
                    }
                } else {
                    quote! {
                        #f: value.#f.into()
                    }
                }
            })
            .collect();

        let from = syn::parse_str::<syn::Path>(&format!(
            "crate::{}::{}",
            module.to_token_stream(),
            name.to_token_stream()
        ))
        .unwrap();

        let derived = derived(module, name, fields);
        let debug_impl = debug_impl(module, name, fields, include_tag);

        quote! {
            #derived
            pub(crate) struct #name {
                #(#vfk,)*
                pub tag_buffer: Option<crate::primitive::tagged::TagBuffer>,
            }

            #debug_impl

            impl From<#from> for #name {
                fn from(value: #from) -> Self {
                    #[allow(unused_mut)]
                    let mut tag_buffer = Vec::with_capacity(#tag_capacity);

                    #(#tags)*

                    Self {
                        #(#assignments,)*
                        tag_buffer: Some(tag_buffer.into()),
                    }
                }
            }

        }
    }
}

fn root(messages: &[Message], include_tag: bool) -> Vec<TokenStream> {
    messages
        .iter()
        .map(|message| root_message_struct(message, include_tag))
        .collect()
}

fn process(messages: &[Message], include_tag: bool) -> TokenStream {
    let body_enum = body_enum(messages, include_tag);
    let root = root(messages, include_tag);

    if include_tag {
        let as_names = messages
            .iter()
            .map(|message| {
                let name = message.type_name();

                let module = syn::parse_str::<syn::Path>(
                    &name.to_token_stream().to_string().to_case(Case::Snake),
                )
                .unwrap_or_else(|_| panic!("module: {}", name.to_token_stream()));

                let as_name = syn::parse_str::<syn::Path>(
                    &format!("As{}", name.to_token_stream()).to_case(Case::Snake),
                )
                .unwrap();

                quote! {
                    pub fn #as_name(self) -> Option<#module::#name> {
                        if let Self::#name(value) = self {
                            Some(value)
                        } else {
                            None
                        }
                    }
                }
            })
            .collect::<Vec<_>>();

        let request_responses = {
            let mapping = {
                let mut mapping: BTreeMap<i16, (Option<Type>, Option<Type>)> = BTreeMap::new();

                for message in messages.iter() {
                    _ = mapping
                        .entry(message.api_key())
                        .and_modify(|entry| match message.kind() {
                            MessageKind::Request => {
                                assert_eq!(entry.0.replace(message.type_name()), None)
                            }

                            MessageKind::Response => {
                                assert_eq!(entry.1.replace(message.type_name()), None)
                            }
                        })
                        .or_insert(match message.kind() {
                            MessageKind::Request => (Some(message.type_name()), None),

                            MessageKind::Response => (None, Some(message.type_name())),
                        });
                }

                mapping
            };

            mapping
                .into_iter()
                .filter(|(_, (request, response))| request.is_some() && response.is_some())
                .map(|(_, (request, response))| {
                    quote! {
                        impl Request for #request {
                            type Response = #response;
                        }

                        impl Response for #response {
                            type Request = #request;
                        }
                    }
                })
                .collect::<Vec<_>>()
        };

        let matchers = messages
            .iter()
            .filter(|message| message.kind() == MessageKind::Request)
            .map(|message| {
                let name = message.type_name();

                quote! {
                    impl rama::matcher::Matcher<Frame> for #name {
                        fn matches(
                            &self,
                            ext: Option<&rama::extensions::Extensions>,
                            input: &Frame,
                        ) -> bool {
                            input.api_key().is_ok_and(|api_key| api_key == Self::KEY)
                        }
                    }

                    impl<T> rama::matcher::Matcher<T> for #name
                    where
                        T: ApiKey,
                    {
                        fn matches(
                            &self,
                            ext: Option<&rama::extensions::Extensions>,
                            input: &T,
                        ) -> bool {
                            T::KEY == Self::KEY
                        }
                    }
                }
            })
            .collect::<Vec<_>>();

        let api_keys = messages
            .iter()
            .map(|message| {
                let name = message.type_name();

                quote! {
                    Self::#name(_) => #name::KEY,
                }
            })
            .collect::<Vec<_>>();

        let api_names = messages
            .iter()
            .map(|message| {
                let name = message.type_name();

                quote! {
                    Self::#name(_) => #name::NAME,
                }
            })
            .collect::<Vec<_>>();

        let maximum_allocation_size = messages
            .iter()
            .map(|message| {
                let name = message.type_name();

                quote! {
                    Self::#name(message) => message.maximum_allocation_size(),
                }
            })
            .collect::<Vec<_>>();

        let api_versions = body_into_version(messages);

        quote! {
            #(#root)*

            #body_enum

            #(#request_responses)*

            #(#matchers)*

            impl Body {
                #(#as_names)*

                pub fn api_key(&self) -> i16 {
                    match self {
                        #(#api_keys)*
                    }
                }

                pub fn api_name(&self) -> &'static str {
                    match self {
                        #(#api_names)*
                    }
                }
            }

            impl crate::MaximumAllocationSize for Body {
                fn maximum_allocation_size(&self) -> Result<usize, crate::Error> {
                    match self {
                        #(#maximum_allocation_size)*
                    }
                }
            }

            #api_versions
        }
    } else {
        quote! {
            mod mezzanine {
                #(#root)*

                #body_enum
            }
        }
    }
}

fn all(pattern: &str) -> Result<Vec<Message>> {
    glob::glob(pattern).map_err(Into::into).and_then(|paths| {
        paths
            .map(|path| {
                path.map_err(Into::into)
                    .inspect(|path| println!("cargo::rerun-if-changed={}", path.display()))
                    .and_then(read_value)
                    .and_then(|v| Message::try_from(&Wv::from(&v)).map_err(Into::into))
            })
            .collect::<Result<Vec<_>>>()
    })
}

fn each_field_meta(
    field: &Field,
    common_structs: &Option<HashMap<Type, &CommonStruct>>,
) -> TokenStream {
    let name = field.name().to_case(Case::Snake);
    let version = field.versions();
    let nullable = OptionWrapper::from(field.nullable());
    let kind = field.kind().name();
    let tag = OptionWrapper::from(field.tag());
    let tagged = OptionWrapper::from(field.tagged());

    let children = field.fields().as_ref().map_or_else(
        || {
            common_structs.as_ref().map_or(Vec::new(), |m| {
                m.get(&field.kind().type_name()).map_or(Vec::new(), |cs| {
                    cs.fields()
                        .iter()
                        .map(|f| each_field_meta(f, common_structs))
                        .collect()
                })
            })
        },
        |fields| {
            fields
                .iter()
                .map(|f| each_field_meta(f, common_structs))
                .collect()
        },
    );

    quote! {
        (#name,
        &nisshi_model::FieldMeta {
            version: #version,
            nullable: #nullable,
            kind: nisshi_model::KindMeta(#kind),
            tag: #tag,
            tagged: #tagged,
            fields: &[#(#children),*],
        })
    }
}

fn each_message_meta(message: &Message) -> TokenStream {
    let name = message.name();
    let api_key = message.api_key();
    let version = message.version();
    let message_kind = message.kind();

    let common_structs = message
        .common_structs()
        .as_ref()
        .map(|v| v.iter().map(|cs| (cs.type_name(), cs)))
        .map(HashMap::from_iter);

    let children = message
        .fields()
        .iter()
        .map(|f| each_field_meta(f, &common_structs));

    quote! {
        (#name,
        &nisshi_model::MessageMeta {
            name: #name,
            api_key: #api_key,
            version: #version,
            message_kind: #message_kind,
            fields: &[#(#children),*],
        })
    }
}

fn message_meta(messages: &[Message]) -> TokenStream {
    let len = messages.len();

    let meta = messages.iter().map(each_message_meta);

    quote! {
        pub static MESSAGE_META : [(&str, &nisshi_model::MessageMeta); #len] = [#(#meta, )*];
    }
}

pub fn main() {
    let files = "message/[A-Z]*Re[qs]*.json";

    let messages = all(files).unwrap_or_else(|e| panic!("all: {e:?}"));

    let broker_api_keys = messages
        .iter()
        .filter(|m| {
            m.listeners()
                .is_some_and(|listeners| listeners.contains(&Listener::Broker))
        })
        .map(|m| m.api_key())
        .collect::<Vec<_>>();

    let broker_messages = messages
        .into_iter()
        .filter(|message| {
            broker_api_keys.contains(&message.api_key()) && !message.fields().is_empty()
        })
        .collect::<Vec<_>>();

    check_sensitive_fields(&broker_messages);

    let tagged = process(&broker_messages, true);
    let untagged = process(&broker_messages, false);

    let message_meta = message_meta(&broker_messages);

    let out_dir = env::var_os("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("generate.rs");

    let q = quote! {
        #tagged
        #untagged
        #message_meta
    };

    let r = syn::parse_file(&q.to_string()).unwrap_or_else(|_| panic!("{}", q.to_string()));

    fs::write(&dest_path, prettyplease::unparse(&r)).unwrap();

    println!("cargo::rerun-if-changed=build.rs");
}

struct OptionWrapper<T>(Option<T>);

impl<T> From<Option<T>> for OptionWrapper<T> {
    fn from(value: Option<T>) -> Self {
        Self(value)
    }
}

impl<T: ToTokens> ToTokens for OptionWrapper<T> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        if let Some(ref v) = self.0 {
            tokens.extend(quote! {
                Some(#v)
            });
        } else {
            tokens.extend(quote! {
                None
            });
        }
    }
}
