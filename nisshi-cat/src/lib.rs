// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
//! Nisshi Cat
//!
//! Fetch or Produce (with validation when backed by a schema) messages to a topic

use std::{fmt, io, result, sync::Arc};

use consume::Consume;
pub use location::Location;
use nisshi_sans_io::ErrorCode;
use produce::Produce;
use tokio_util::codec::LinesCodecError;

mod consume;
mod location;
mod produce;

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// The broker answered a fetch of `partition` of `topic` with an error code and no records.
    /// A topic that doesn't exist is answered this way, which would otherwise look like an empty
    /// topic.
    Fetch {
        topic: String,
        partition: i32,
        error_code: i16,
    },

    /// The broker answered the metadata lookup of `topic` with an error code. Kafka answers a
    /// fetch by an unknown topic ID with `UNKNOWN_TOPIC_ID` before it checks authorization, so a
    /// topic the user may not read would be reported as unknown. The metadata error code names
    /// the real cause.
    Metadata {
        topic: String,
        error_code: i16,
    },

    Client(#[from] nisshi_client::Error),
    InvalidJson {
        location: Location,
        source: serde_json::Error,
    },

    Io(Arc<io::Error>),
    LinesCodec(#[from] LinesCodecError),
    Message(String),

    /// A message has neither a `key` nor a `value` field, so `cat produce` can't make a record
    /// of it without a schema.
    MessageWithoutKeyOrValue {
        location: Location,
        message: String,
    },

    NoMessages {
        location: Location,
    },

    /// A file holds JSON that isn't an array, so `cat produce` can't read messages from it.
    NotAnArray {
        location: Location,
    },

    Protocol(#[from] nisshi_sans_io::Error),
    Schema(#[from] nisshi_schema::Error),
    SerdeJson(#[from] serde_json::Error),
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJson { location, source } => write!(
                f,
                "{location} is not valid JSON: {source}. {}",
                location.message_format()
            ),
            Self::MessageWithoutKeyOrValue { location, message } => write!(
                f,
                r#"{location} has no "key" or "value" field: {message}. {}"#,
                location.message_format()
            ),
            Self::NotAnArray { location } => write!(
                f,
                "{location} is not a JSON array of messages. {}",
                location.message_format()
            ),
            Self::NoMessages { location } => {
                write!(
                    f,
                    "{location} has no messages. {}",
                    location.message_format()
                )
            }
            Self::Fetch {
                topic,
                partition,
                error_code,
            } => {
                write!(
                    f,
                    "fetching partition {partition} of topic {topic:?} failed: "
                )?;
                write_error_code(f, *error_code)
            }
            Self::Metadata { topic, error_code } => {
                write!(f, "looking up topic {topic:?} failed: ")?;
                write_error_code(f, *error_code)
            }
            _ => write!(f, "{self:?}"),
        }
    }
}

fn write_error_code(f: &mut fmt::Formatter<'_>, error_code: i16) -> fmt::Result {
    match ErrorCode::try_from(error_code) {
        Ok(error_code) => write!(f, "{error_code:?} ({})", i16::from(error_code)),
        Err(_) => write!(f, "unknown error code {error_code}"),
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Cat {
    Consume(Box<consume::Configuration>),
    Produce(Box<produce::Configuration>),
}

impl Cat {
    pub fn produce() -> produce::PhantomBuilder {
        Builder::produce()
    }

    pub fn consume() -> consume::PhantomBuilder {
        Builder::consume()
    }

    pub async fn main(self) -> Result<ErrorCode> {
        match self {
            Self::Produce(configuration) => Produce::try_from(*configuration)?.main().await,
            Self::Consume(configuration) => Consume::try_from(*configuration)?.main().await,
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Builder;

impl Builder {
    pub fn produce() -> produce::PhantomBuilder {
        produce::Builder::default()
    }

    pub fn consume() -> consume::PhantomBuilder {
        consume::Builder::default()
    }
}
