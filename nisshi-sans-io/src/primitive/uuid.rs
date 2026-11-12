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

use serde::{Deserialize, Serialize};

use crate::{ByteSize, Result};

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct Uuid(pub [u8; 16]);

impl From<[u8; 16]> for Uuid {
    fn from(value: [u8; 16]) -> Self {
        Self(value)
    }
}

impl ByteSize for Uuid {
    fn size_in_bytes(&self) -> Result<usize> {
        Ok(size_of_val(&self.0))
    }
}
