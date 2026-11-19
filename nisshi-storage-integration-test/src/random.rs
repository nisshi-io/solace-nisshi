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

//! Random names and payloads, so that tests on one shared engine don't collide.

use bytes::Bytes;
use rand::{
    distr::{Alphanumeric, StandardUniform},
    prelude::*,
    rng,
};

pub fn alphanumeric_string(length: usize) -> String {
    rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

pub fn random_bytes(length: usize) -> Bytes {
    rng()
        .sample_iter(StandardUniform)
        .take(length)
        .collect::<Vec<u8>>()
        .into()
}
