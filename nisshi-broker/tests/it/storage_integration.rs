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
//! doesn't run. `TEST_FLOORS`, after the macro calls, holds the fewest tests that each engine must
//! get, so a `without` edit that leaves out more cases also lowers that engine's floor.

use std::{env, io, process::Command};

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

/// The fewest tests that the suite must generate for one engine.
struct TestFloor {
    engine: &'static str,
    minimum_tests: usize,
}

/// Each engine's floor: the number of its tests on the day the floor was set, ignored tests
/// included.
///
/// A floor is a minimum, not an exact count, so a new case needs no edit here. If a change
/// removes a case or leaves out more cases for an engine, the author lowers that engine's floor
/// on purpose, in the same change.
const TEST_FLOORS: &[TestFloor] = &[
    #[cfg(feature = "dynostore")]
    TestFloor {
        engine: "in_memory",
        minimum_tests: 97,
    },
    #[cfg(feature = "libsql")]
    TestFloor {
        engine: "lite",
        minimum_tests: 102,
    },
    #[cfg(feature = "postgres")]
    TestFloor {
        engine: "pg",
        minimum_tests: 102,
    },
    #[cfg(feature = "slatedb")]
    TestFloor {
        engine: "slatedb",
        minimum_tests: 47,
    },
    #[cfg(feature = "turso")]
    TestFloor {
        engine: "turso",
        minimum_tests: 33,
    },
];

/// Each engine gets at least its floor of tests from the suite.
///
/// A `without` entry that names more than its author meant, or a broken rule in the macro,
/// compiles to an empty or thinner module for an engine, and the compiler gives no warning. The
/// macro's name check only checks that each name in a `without` list exists.
#[test]
fn every_engine_gets_at_least_its_floor_of_tests() -> io::Result<()> {
    // libtest lists every test of this binary, ignored tests included, as `<path>: test`. The
    // listing runs no test, and it needs no environment variable.
    let listing = Command::new(env::current_exe()?)
        .args(["--list", "--format", "terse"])
        .output()?;
    assert!(
        listing.status.success(),
        "listing the tests of {} failed with {}: {}",
        env::current_exe()?.display(),
        listing.status,
        String::from_utf8_lossy(&listing.stderr)
    );
    let listing = String::from_utf8_lossy(&listing.stdout);

    // `module_path!()` starts with the crate name, and a test path starts after it.
    let module = module_path!()
        .split_once("::")
        .map_or(module_path!(), |(_, path)| path);

    // The listing holds this test itself. If it does not, the listing's format has changed, and
    // the counts below mean nothing.
    let this_test = format!("{module}::every_engine_gets_at_least_its_floor_of_tests: test");
    assert!(
        listing.lines().any(|line| line == this_test),
        "the test listing does not hold `{this_test}`, so this check can't count the suite's \
         tests. The listing starts with: {:?}",
        listing.lines().take(5).collect::<Vec<_>>()
    );

    let mut below_floor = Vec::new();
    for floor in TEST_FLOORS {
        let prefix = format!("{module}::{}::", floor.engine);
        let found = listing
            .lines()
            .filter(|line| line.starts_with(&prefix) && line.ends_with(": test"))
            .count();
        if found < floor.minimum_tests {
            below_floor.push(format!(
                "`{}` has {found} tests, and its floor is {}",
                floor.engine, floor.minimum_tests
            ));
        }
    }
    assert!(
        below_floor.is_empty(),
        "the suite generates fewer tests than the floor for these engines: {below_floor:?}. If \
         cases were left out or removed on purpose, lower the floor in TEST_FLOORS in \
         nisshi-broker/tests/it/storage_integration.rs. If not, check the engine's `without` list \
         and the rules of `__unless_listed!` in nisshi-storage-integration-test"
    );

    Ok(())
}
