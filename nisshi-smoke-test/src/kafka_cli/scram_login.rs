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

/// A SCRAM user that the tools log in as, on a broker started with `--authentication`.
#[derive(Clone, Debug)]
pub struct ScramLogin {
    pub user: String,
    pub password: String,
    pub mechanism: ScramMechanism,
}

/// A SCRAM mechanism: the hash function that a SCRAM credential and a SCRAM login use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScramMechanism {
    Sha256,
    Sha512,
}

impl ScramMechanism {
    /// Every mechanism that Kafka supports.
    pub const ALL: [Self; 2] = [Self::Sha256, Self::Sha512];

    /// Returns the mechanism's name, as Kafka's user configs and SASL settings write it.
    pub fn name(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }
}
