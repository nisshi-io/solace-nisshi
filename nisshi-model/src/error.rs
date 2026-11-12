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
    fmt::{self, Display},
    io, num, str, string,
    sync::Arc,
};

use serde::{de, ser};

#[derive(Clone, Debug)]
pub enum Error {
    DryPop,
    EmptyStack,
    FromUtf8(string::FromUtf8Error),
    Io(Arc<io::Error>),
    Json(Arc<serde_json::Error>),
    Message(String),
    NoCurrentFieldMeta,
    NoMessageMeta,
    NoSuchField(&'static str),
    NoSuchMessage(&'static str),
    ParseBool(str::ParseBoolError),
    ParseInt(num::ParseIntError),
    Syn(syn::Error),
    TryFromInt(num::TryFromIntError),
    Utf8(str::Utf8Error),
}

impl std::error::Error for Error {}

impl ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl From<str::Utf8Error> for Error {
    fn from(value: str::Utf8Error) -> Self {
        Self::Utf8(value)
    }
}

impl From<string::FromUtf8Error> for Error {
    fn from(value: string::FromUtf8Error) -> Self {
        Self::FromUtf8(value)
    }
}

impl From<num::TryFromIntError> for Error {
    fn from(value: num::TryFromIntError) -> Self {
        Self::TryFromInt(value)
    }
}

impl From<str::ParseBoolError> for Error {
    fn from(value: str::ParseBoolError) -> Self {
        Self::ParseBool(value)
    }
}

impl From<num::ParseIntError> for Error {
    fn from(value: num::ParseIntError) -> Self {
        Self::ParseInt(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(Arc::new(value))
    }
}

impl From<syn::Error> for Error {
    fn from(value: syn::Error) -> Self {
        Self::Syn(value)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Message(e) => f.write_str(e),
            e => write!(f, "{e:?}"),
        }
    }
}
