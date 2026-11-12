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

use std::fmt;

/// Where `cat produce` read a message, or the input it read messages from.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Location {
    Stdin,

    /// A line of stdin, counting from 1.
    StdinLine(usize),

    File(String),

    /// An element of the JSON array in a file, counting from 0.
    FileElement {
        file_name: String,
        index: usize,
    },
}

impl Location {
    /// Returns how to write messages in this input, as advice that ends an error message.
    pub fn message_format(&self) -> &'static str {
        match self {
            Self::Stdin | Self::StdinLine(_) => {
                r#"Write one message per line, as a JSON object with a "value" and an optional "key", for example {"key": "1", "value": {"message": "hello world"}}"#
            }
            Self::File(_) | Self::FileElement { .. } => {
                r#"Write the file as a JSON array of messages, each a JSON object with a "value" and an optional "key", for example [{"key": "1", "value": {"message": "hello world"}}]"#
            }
        }
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stdin => f.write_str("stdin"),
            Self::StdinLine(line) => write!(f, "line {line} of stdin"),
            Self::File(file_name) => f.write_str(file_name),
            Self::FileElement { file_name, index } => {
                write!(f, "element {index} of the array in {file_name}")
            }
        }
    }
}
