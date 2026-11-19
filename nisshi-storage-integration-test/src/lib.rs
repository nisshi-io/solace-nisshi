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

//! Tests of Nisshi's broker running on top of a storage engine that the caller supplies.
//!
//! A case checks Nisshi code above the [`Storage`](nisshi_storage::Storage) trait, such as the
//! group coordinator or a request handler, so any engine can run it: if an engine passes these
//! cases, Nisshi's broker works on it. The crate never builds an engine; each caller passes in
//! the engine that it tests.
//!
//! The crate's API and its test names may change in any commit. A caller updates its call when it
//! moves to a newer Nisshi.

// The cases are tests, and a test fails on an unexpected value, as nisshi-broker's tests do.
#![allow(clippy::unwrap_used, clippy::expect_used)]

pub mod broker;
pub mod group;
pub mod random;
