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

//! Runs `nisshi-storage-integration-test` on each of Nisshi's storage engines.
//!
//! An engine's `without` list names the suite's areas, files and groups of cases that the engine
//! doesn't run.

#[cfg(feature = "dynostore")]
nisshi_storage_integration_test::storage_integration_tests! {
    engine: in_memory,
    storage: crate::common::memory_storage,
    before_each: crate::common::init_tracing,
}

#[cfg(feature = "libsql")]
nisshi_storage_integration_test::storage_integration_tests! {
    engine: lite,
    storage: crate::common::lite_storage,
    before_each: crate::common::init_tracing,
}

#[cfg(feature = "postgres")]
nisshi_storage_integration_test::storage_integration_tests! {
    engine: pg,
    storage: crate::common::postgres_storage,
    before_each: crate::common::init_tracing,
}

// SlateDB is deprecated, and `AGENTS.md` says to keep its test coverage as it is, so it leaves out
// the `not_on_slatedb` group.
#[cfg(feature = "slatedb")]
nisshi_storage_integration_test::storage_integration_tests! {
    engine: slatedb,
    storage: crate::common::slate_storage,
    before_each: crate::common::init_tracing,
    without: [not_on_slatedb],
}

#[cfg(feature = "turso")]
nisshi_storage_integration_test::storage_integration_tests! {
    engine: turso,
    storage: crate::common::turso_storage,
    before_each: crate::common::init_tracing,
    without: [consumer_group],
}
