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

/// Resource pattern
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(i8)]
pub enum Pattern {
    #[default]
    /// Represents any PatternType which this client cannot understand, perhaps because this client is too old.
    Unknown = 0,

    /// In a filter, matches any resource pattern type.
    Any = 1,

    Match = 2,

    /// A literal resource name.
    /// A literal name defines the full name of a resource, e.g. topic with name 'foo', or group with name 'bob'.
    /// The special wildcard character `*` can be used to represent a resource with any name.
    Literal = 3,

    /// A prefixed resource name.
    /// A prefixed name defines a prefix for a resource, e.g. topics with names that start with 'foo'.
    Prefixed = 4,
}

impl From<i8> for Pattern {
    fn from(value: i8) -> Self {
        let any = Pattern::Any as i8;

        match value {
            p if p == Pattern::Any as i8 => Pattern::Any,
            p if p == Pattern::Literal as i8 => Pattern::Literal,
            p if p == Pattern::Match as i8 => Pattern::Match,
            p if p == Pattern::Prefixed as i8 => Pattern::Prefixed,

            _ => Pattern::Unknown,
        }
    }
}
