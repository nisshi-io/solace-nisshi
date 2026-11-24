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

// The in-memory engine answers a DeleteGroups request for a group that it never stored with
// `NONE`, because deleting an absent object store key succeeds, so it leaves out the cases that
// check Kafka's `GROUP_ID_NOT_FOUND`.
#[cfg(feature = "dynostore")]
nisshi_storage_integration_test::storage_integration_tests! {
    engine: in_memory,
    storage: crate::common::memory_storage,
    before_each: crate::common::init_tracing,
    without: [not_on_in_memory],
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

// SlateDB is deprecated, and `AGENTS.md` says to keep its tests and coverage as they are. This list
// keeps that coverage.
#[cfg(feature = "slatedb")]
nisshi_storage_integration_test::storage_integration_tests! {
    engine: slatedb,
    storage: crate::common::slate_storage,
    before_each: crate::common::init_tracing,
    without: [
        not_on_slatedb,
        cg_static,
        consumer_group_describe,
        offset_commit_v1_timestamp,
        offset_fetch_commit_metadata,
        offset_fetch_require_stable,
        offset_metadata_max_size,
        txn_offset_commit_fence,
        txn_offset_fetch_commit_metadata,
        txn_offset_metadata_max_size,
    ],
}

// Turso leaves out every name in this list until [#866] is done. Nisshi has not run these areas
// and files on Turso, so the list records no known failure. When a file passes on Turso, remove
// its name.
//
// [#866]: https://github.com/nisshi-io/solace-nisshi/issues/866
#[cfg(feature = "turso")]
nisshi_storage_integration_test::storage_integration_tests! {
    engine: turso,
    storage: crate::common::turso_storage,
    before_each: crate::common::init_tracing,
    without: [
        cg_dynamic,
        consumer_group_describe,
        describe_groups,
        find_coordinator,
        group_across_brokers,
        group_cache_miss,
        join_invalid_session_timeout,
        list_groups,
        new_cg,
        rebalance_timing,
        offset,
        offset_fetch_require_stable,
        txn_offset_commit_fence,
        txn_offset_fetch_commit_metadata,
        txn_offset_metadata_max_size,
    ],
}
