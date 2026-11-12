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

#![deny(clippy::todo, clippy::unimplemented)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::todo,
        clippy::unimplemented
    )
)]
use std::{
    collections::HashMap,
    env::vars,
    fmt, io,
    net::{AddrParseError, SocketAddr},
    num::TryFromIntError,
    result,
    str::{FromStr, Utf8Error},
    sync::{Arc, LazyLock, PoisonError},
    time::SystemTimeError,
};

use glob::PatternError;
use jsonschema::ValidationError;
use nisshi_sans_io::ErrorCode;
use opentelemetry::{InstrumentationScope, global, metrics::Meter};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use regex::{Regex, Replacer};
use thiserror::Error;
use tokio::{sync::broadcast::error::SendError, task::JoinError};
use tracing_subscriber::filter::ParseError;

pub mod broker;
pub mod coordinator;
pub mod otel;
pub mod service;

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CancelKind {
    Interrupt,
    Terminate,
}

pub const NODE_ID: i32 = 111;

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

#[derive(Clone, Debug, Error)]
pub enum Error {
    AddrParse(#[from] AddrParseError),
    Api(ErrorCode),
    Auth(#[from] nisshi_auth::Error),
    Custom(String),
    DuplicateApiService(i16),
    EmptyCoordinatorWrapper,
    EmptyJoinGroupRequestProtocol,
    ExpectedJoinGroupRequestProtocol(&'static str),

    Hyper(Arc<hyper::http::Error>),

    /// A sweep interval option in the storage URL has a value that the broker
    /// rejects.
    InvalidStorageOptionValue {
        option: String,
        value: String,
    },

    Io(Arc<io::Error>),
    Join(Arc<JoinError>),
    Json(Arc<serde_json::Error>),
    KafkaProtocol(#[from] nisshi_sans_io::Error),

    /// Another socket already listens on the address that the broker's listener binds.
    ListenerInUse {
        addr: SocketAddr,
    },

    /// The host of the broker's listener URL is a name that did not resolve to an address.
    ListenerHostUnresolved {
        host: String,
        port: u16,
        source: Arc<io::Error>,
    },

    Message(String),
    Model(#[from] nisshi_model::Error),

    Otel(#[from] nisshi_otel::Error),

    ParseFilter(Arc<ParseError>),
    ParseInt(#[from] std::num::ParseIntError),
    Pattern(Arc<PatternError>),
    Poison,

    ProgressStyleTemplate(Arc<indicatif::style::TemplateError>),

    Regex(#[from] regex::Error),

    SchemaRegistry(Arc<nisshi_schema::Error>),
    Service(#[from] nisshi_service::Error),
    Storage(#[from] nisshi_storage::Error),
    SystemTime(#[from] SystemTimeError),

    TryFromInt(#[from] TryFromIntError),

    UnsupportedApiService(i16),
    UnsupportedTracingFormat(String),
    Url(#[from] url::ParseError),
    Utf8(#[from] Utf8Error),
    Uuid(#[from] uuid::Error),
    SchemaValidation,
    Send(Arc<SendError<CancelKind>>),
}

impl From<PatternError> for Error {
    fn from(value: PatternError) -> Self {
        Self::Pattern(Arc::new(value))
    }
}

impl From<SendError<CancelKind>> for Error {
    fn from(value: SendError<CancelKind>) -> Self {
        Self::Send(Arc::new(value))
    }
}

impl From<hyper::http::Error> for Error {
    fn from(value: hyper::http::Error) -> Self {
        Self::Hyper(Arc::new(value))
    }
}

impl From<indicatif::style::TemplateError> for Error {
    fn from(value: indicatif::style::TemplateError) -> Self {
        Self::ProgressStyleTemplate(Arc::new(value))
    }
}

impl From<JoinError> for Error {
    fn from(value: JoinError) -> Self {
        Self::Join(Arc::new(value))
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::from(Arc::new(value))
    }
}

impl From<Arc<serde_json::Error>> for Error {
    fn from(value: Arc<serde_json::Error>) -> Self {
        Self::Json(value)
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        Self::ParseFilter(Arc::new(value))
    }
}

impl From<nisshi_schema::Error> for Error {
    fn from(value: nisshi_schema::Error) -> Self {
        Self::SchemaRegistry(Arc::new(value))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl From<ValidationError<'_>> for Error {
    fn from(_value: ValidationError<'_>) -> Self {
        Self::SchemaValidation
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Copy, Clone, Debug)]
pub enum TracingFormat {
    Text,
    Json,
}

impl FromStr for TracingFormat {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            otherwise => Err(Error::UnsupportedTracingFormat(otherwise.to_owned())),
        }
    }
}

#[derive(Clone, Debug)]
pub struct VarRep(HashMap<String, String>);

impl From<HashMap<String, String>> for VarRep {
    fn from(value: HashMap<String, String>) -> Self {
        Self(value)
    }
}

impl VarRep {
    fn replace(&self, haystack: &str) -> Result<String> {
        Regex::new(r"\$\{(?<var>[^\}]+)\}")
            .map(|re| re.replace(haystack, self).into_owned())
            .map_err(Into::into)
    }
}

impl Replacer for &VarRep {
    fn replace_append(&mut self, caps: &regex::Captures<'_>, dst: &mut String) {
        if let Some(variable) = caps.name("var")
            && let Some(value) = self.0.get(variable.as_str())
        {
            dst.push_str(value);
        }
    }
}

#[derive(Clone, Debug)]
pub struct EnvVarExp<T>(T);

impl<T> EnvVarExp<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> FromStr for EnvVarExp<T>
where
    T: FromStr,
    Error: From<<T as FromStr>::Err>,
{
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        VarRep::from(vars().collect::<HashMap<_, _>>())
            .replace(s)
            .and_then(|s| T::from_str(&s).map_err(Into::into))
            .map(|t| Self(t))
    }
}
