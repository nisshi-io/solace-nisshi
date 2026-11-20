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
//! group coordinator or a request handler, so any engine can run it. If an engine passes these
//! cases, the parts of Nisshi's broker that the suite's areas cover work on it. The crate never
//! builds an engine; each caller passes in the engine that it tests.
//!
//! A caller invokes [`storage_integration_tests!`] once for each engine that it tests, and gets
//! one test for each case:
//!
//! ```no_run
//! use nisshi_storage::ArcDynStorage;
//! use uuid::Uuid;
//!
//! async fn my_engine(cluster_id: Uuid, broker_id: i32) -> nisshi_broker::Result<ArcDynStorage> {
//!     todo!("build the engine for {cluster_id} and {broker_id}")
//! }
//!
//! nisshi_storage_integration_test::storage_integration_tests! {
//!     engine: my_engine,
//!     storage: my_engine,
//!     attributes: [ignore = "my engine does not store groups yet"],
//!     without: [transactions],
//! }
//! # fn main() {}
//! ```
//!
//! The crate's API and its test names may change in any commit. A file that moves, splits or is
//! renamed also changes which cases a `without` name leaves out. A caller updates its call and
//! its skip list when it moves to a newer Nisshi.

// The cases are tests, and a test fails on an unexpected value, as nisshi-broker's tests do.
#![allow(clippy::unwrap_used, clippy::expect_used)]

pub mod broker;
mod commit_metadata;
pub mod consumer_group;
pub mod group;
mod init_producer;
mod metadata_max_size;
pub mod offset;
pub mod producer_id;
pub mod random;
pub mod transactions;

#[doc(hidden)]
pub mod __private {
    use rand::{prelude::*, rng};

    pub use uuid::Uuid;

    /// The result of a generated test. A case and the caller's `storage` function each return
    /// their own error type, such as `nisshi_broker::Error` or `anyhow::Error`, and `?` converts
    /// either one into this.
    pub type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Returns a random broker ID, so that the tests that share an engine use different brokers.
    pub fn broker_id() -> i32 {
        rng().random_range(0..i32::MAX)
    }
}

/// Defines a module named after `engine`, with one test for each case.
///
/// - `engine`: the module's name, such as `in_memory`.
/// - `storage`: an async function or closure that takes a cluster ID (a `Uuid`) and a broker ID
///   (an `i32`) and returns `Result<impl Storage + Clone, E>`, where `?` converts `E` into a boxed
///   error. Each test calls it once, so each test gets a fresh engine.
/// - `before_each` (optional): a function that each test calls first. It takes no arguments and
///   returns `Result<T, E>`, where `?` converts `E` into a boxed error. The test keeps the `T`
///   until it ends, such as a tracing guard.
/// - `attributes` (optional): attributes for every test, such as `ignore = "..."`.
/// - `without` (optional): areas, files and groups of cases to leave out. A name that the suite
///   does not have fails the build. The groups, named `not_on_<engine>`, belong to Nisshi's own
///   engines. Another caller leaves out areas and files, and skips a single test by its name,
///   such as with a test filter of its test runner.
///
/// The tests use `#[tokio::test]`, so the caller needs `tokio` as a dev-dependency. The macro
/// searches `without` once for each area, file and group, so a long `without` list needs a
/// higher recursion limit than the compiler's default, such as `#![recursion_limit = "256"]` in
/// the caller's crate.
#[macro_export]
macro_rules! storage_integration_tests {
    (
        engine: $engine:ident,
        storage: $storage:expr
        $(, before_each: $before_each:expr)?
        $(, attributes: [$($attribute:meta),* $(,)?])?
        $(, without: [$($without:ident),* $(,)?])?
        $(,)?
    ) => {
        $($($crate::__check_name!($without);)*)?

        pub mod $engine {
            #[allow(unused_imports)]
            use super::*;

            $crate::__areas!({
                storage: ($storage),
                before_each: ($($before_each)?),
                attributes: [$($($attribute),*)?],
                without: [$($($without),*)?]
            });
        }
    };
}

/// Defines each area, with the config of one engine.
#[doc(hidden)]
#[macro_export]
macro_rules! __areas {
    ($config:tt) => {
        $crate::__area_consumer_group!($config);
        $crate::__area_offset!($config);
        $crate::__area_producer_id!($config);
        $crate::__area_transactions!($config);
    };
}

/// Defines the `consumer_group` area, unless `without` names it. The case lists follow the rules
/// of `__file!`.
#[doc(hidden)]
#[macro_export]
macro_rules! __area_consumer_group {
    ($config:tt) => {
        $crate::__unless_listed!(consumer_group; $config;
            pub mod consumer_group {
                #[allow(unused_imports)]
                use super::*;

                $crate::__file!(consumer_group, cg_dynamic, [
                    reject_empty_member_id_on_join(cluster_id, broker_id, storage),
                    #[ignore = "the second member's SyncGroup answers NONE, not the REBALANCE_IN_PROGRESS that the case expects"]
                    lifecycle(cluster_id, broker_id, storage),
                ], $config);

                $crate::__file!(consumer_group, cg_static, [
                    join_with_empty_member_id(cluster_id, broker_id, storage),
                    rejoin_with_empty_member_id(cluster_id, broker_id, storage),
                ], $config);

                $crate::__file!(consumer_group, consumer_group_describe, [
                    describe_non_existent_group(cluster_id, broker_id, storage),
                    stored_group_answers_group_id_not_found(storage),
                    error_row_omits_authorized_operations(storage),
                ], $config);

                $crate::__file!(consumer_group, delete_groups, [
                    non_empty_group_then_empty(storage),
                    delete_evicts_coordinator_cache(storage),
                    [not_on_slatedb] deleted_group_is_not_found(storage),
                    delete_after_session_expiry(storage),
                    delete_offsets_only_group(storage),
                    cache_miss_with_live_member(storage),
                    stale_cache_on_another_broker(storage),
                    storage_read_error(storage),
                    mixed_request(storage),
                    [not_on_in_memory, not_on_slatedb]
                    unknown_group_in_mixed_request_is_not_found(storage),
                    [not_on_in_memory] empty_group_id_in_mixed_request_is_not_found(storage),
                    delete_non_existent(storage),
                    [not_on_in_memory, not_on_slatedb] unknown_group_is_not_found(storage),
                    [not_on_in_memory] empty_group_id_is_not_found(storage),
                    empty_group_id_mixed_list(storage),
                    [not_on_in_memory] empty_group_id_in_mixed_list_is_not_found(storage),
                    [not_on_turso] slash_and_empty_group_ids_are_distinct_groups(storage),
                    [not_on_slatedb] slash_in_group_id_does_not_break_committed_offset_topitions(storage),
                    [not_on_slatedb] delete_topic_removes_offsets_of_slash_and_empty_group_ids(storage),
                ], $config);

                $crate::__file!(consumer_group, describe_groups, [
                    simple(storage),
                    [not_on_slatedb] operations_omitted_unless_requested(storage),
                    [not_on_slatedb] found_group_answers_supported_operations_when_asked(storage),
                    [not_on_slatedb] unknown_group_answers_dead(storage),
                    [not_on_slatedb] group_with_only_committed_offsets_is_empty(storage),
                    [not_on_slatedb] group_whose_sessions_expired_is_described_as_empty(storage),
                    [not_on_slatedb] stable_group_reports_each_member_and_the_protocol(storage),
                    [not_on_slatedb] rebalancing_group_reports_members_without_metadata(storage),
                ], $config);

                $crate::__file!(consumer_group, find_coordinator, [
                    simple(broker_id, storage),
                ], $config);

                $crate::__file!(consumer_group, group_across_brokers, [
                    rebalances_end_in_one_generation_when_requests_alternate_brokers(storage),
                ], $config);

                $crate::__file!(consumer_group, group_cache_miss, [
                    simultaneous(storage),
                    staggered(storage),
                    restart(storage),
                ], $config);

                $crate::__file!(consumer_group, join_invalid_session_timeout, [
                    reject_invalid_session_timeout_on_join(cluster_id, broker_id, storage),
                    accept_boundary_session_timeout_on_join(cluster_id, broker_id, storage),
                ], $config);

                $crate::__file!(consumer_group, list_groups, [
                    simple(storage),
                    group_without_committed_offsets_is_listed_once(storage),
                    group_with_committed_offsets_is_listed_once(storage),
                    [not_on_slatedb] lists_each_group_with_its_state(storage),
                    [not_on_slatedb] states_filter_lists_only_groups_in_that_state(storage),
                    [not_on_slatedb] group_whose_sessions_expired_is_listed_as_empty(storage),
                    [not_on_slatedb] group_past_its_join_deadline_is_listed_as_empty(storage),
                    [not_on_slatedb] empty_group_id_is_not_surfaced_as_phantom_group(storage),
                    [not_on_slatedb] empty_group_id_without_committed_offsets_is_listed(storage),
                    [not_on_slatedb] state_only_group_ids_are_decoded(storage),
                ], $config);

                $crate::__file!(consumer_group, new_cg, [
                    one_consumer_session_delay_after_initial_join(storage),
                    one_consumer_next_action(storage),
                    two_consumer_next_action(storage),
                    [not_on_slatedb] two_consumer_interleave_join(storage),
                    [not_on_slatedb] consumer_next_action_08c(storage),
                    [not_on_slatedb] consumer_next_action_16c(storage),
                    [not_on_slatedb] consumer_next_action_24c(storage),
                    [not_on_slatedb] consumer_next_action_32c(storage),
                ], $config);

                $crate::__file!(consumer_group, rebalance_timing, [
                    first_join_of_a_new_group_waits_for_the_initial_delay(storage),
                    rebalance_ends_once_every_member_has_rejoined(storage),
                    rebalance_across_brokers_ends_once_the_last_member_rejoins(storage),
                ], $config);
            }
        );
    };
}

/// Defines the `offset` area, unless `without` names it. The case lists follow the rules of
/// `__file!`.
#[doc(hidden)]
#[macro_export]
macro_rules! __area_offset {
    ($config:tt) => {
        $crate::__unless_listed!(offset; $config;
            pub mod offset {
                #[allow(unused_imports)]
                use super::*;

                $crate::__file!(offset, offset_commit_fence, [
                    commit_after_leaving_through_another_broker_is_fenced(
                        cluster_id, broker_id, storage
                    ),
                    commit_whose_member_is_removed_during_the_commit_stores_nothing(
                        cluster_id, broker_id, storage
                    ),
                ], $config);

                $crate::__file!(offset, offset_commit_v1_timestamp, [
                    offset_commit_stores_offsets_with_the_default_commit_timestamp(
                        cluster_id, broker_id, storage
                    ),
                ], $config);

                $crate::__file!(offset, offset_fetch_commit_metadata, [
                    commit_returns_leader_epoch_and_metadata(cluster_id, broker_id, storage),
                    commit_without_epoch_or_metadata_returns_defaults(
                        cluster_id, broker_id, storage
                    ),
                    uncommitted_partition_returns_defaults(cluster_id, broker_id, storage),
                    offset_commit_request_returns_leader_epoch_and_metadata(
                        cluster_id, broker_id, storage
                    ),
                    commit_replaces_leader_epoch_and_metadata(cluster_id, broker_id, storage),
                ], $config);

                $crate::__file!(offset, offset_metadata_max_size, [
                    offset_commit_rejects_metadata_over_the_limit(cluster_id, broker_id, storage),
                    offset_commit_counts_characters_not_bytes(cluster_id, broker_id, storage),
                ], $config);
            }
        );
    };
}

/// Defines the `producer_id` area, unless `without` names it. The case lists follow the rules of
/// `__file!`.
#[doc(hidden)]
#[macro_export]
macro_rules! __area_producer_id {
    ($config:tt) => {
        $crate::__unless_listed!(producer_id; $config;
            pub mod producer_id {
                #[allow(unused_imports)]
                use super::*;

                $crate::__file!(producer_id, init_producer_id, [
                    no_txn_init_producer_id(storage),
                    old_versions_are_fresh(cluster_id, broker_id, storage),
                    no_txn_claim_gets_new_id(cluster_id, broker_id, storage),
                ], $config);
            }
        );
    };
}

/// Defines the `transactions` area, unless `without` names it. A case belongs here when it needs
/// a transactional ID, so that an engine without transactions leaves out the whole area. The case
/// lists follow the rules of `__file!`.
#[doc(hidden)]
#[macro_export]
macro_rules! __area_transactions {
    ($config:tt) => {
        $crate::__unless_listed!(transactions; $config;
            pub mod transactions {
                #[allow(unused_imports)]
                use super::*;

                $crate::__file!(transactions, txn_init_producer_id, [
                    transactional_id_is_refused(cluster_id, broker_id, storage),
                    #[ignore = "InitProducerId refuses a transactional ID until the work in compat/librdkafka/FINDINGS.md item 6 is done"]
                    old_versions_bump_a_transactional_producer(cluster_id, broker_id, storage),
                    txn_claim_current_epoch_bumps(cluster_id, broker_id, storage),
                    txn_claim_stale_epoch_is_fenced(cluster_id, broker_id, storage),
                    txn_claim_other_id_is_fenced(cluster_id, broker_id, storage),
                    txn_claim_unknown_txn_is_fresh(cluster_id, broker_id, storage),
                    mixed_shape_is_invalid_request(cluster_id, broker_id, storage),
                    txn_claim_ongoing_txn_aborts_and_bumps(cluster_id, broker_id, storage),
                    txn_fenced_claim_leaves_ongoing_txn(cluster_id, broker_id, storage),
                ], $config);

                $crate::__file!(transactions, txn_offset_fetch_commit_metadata, [
                    txn_offset_commit_returns_leader_epoch_and_metadata(
                        cluster_id, broker_id, storage
                    ),
                    txn_offset_commit_replaces_leader_epoch_and_metadata(
                        cluster_id, broker_id, storage
                    ),
                ], $config);

                $crate::__file!(transactions, offset_fetch_require_stable, [
                    pending_offset_is_unstable_with_require_stable(cluster_id, broker_id, storage),
                    pending_offset_is_ignored_without_require_stable(
                        cluster_id, broker_id, storage
                    ),
                    committed_transaction_offset_is_stable(cluster_id, broker_id, storage),
                    aborted_transaction_offset_is_stable(cluster_id, broker_id, storage),
                    pending_offset_of_another_group_is_stable(cluster_id, broker_id, storage),
                    pending_offset_without_commit_is_unstable(cluster_id, broker_id, storage),
                    reinitialized_producer_ends_pending_offset(cluster_id, broker_id, storage),
                    deferred_commit_of_reinitialized_producer_keeps_offset_pending(
                        cluster_id, broker_id, storage
                    ),
                ], $config);

                $crate::__file!(transactions, txn_offset_metadata_max_size, [
                    txn_offset_commit_rejects_metadata_over_the_limit(
                        cluster_id, broker_id, storage
                    ),
                ], $config);

                $crate::__file!(transactions, txn_offset_commit_fence, [
                    unknown_member(cluster_id, broker_id, storage),
                    illegal_generation(cluster_id, broker_id, storage),
                    fenced_instance(cluster_id, broker_id, storage),
                    current_member(cluster_id, broker_id, storage),
                    group_completing_rebalance(cluster_id, broker_id, storage),
                    without_member_fields(cluster_id, broker_id, storage),
                    replaced_route_keeps_the_fence(cluster_id, broker_id, storage),
                ], $config);
            }
        );
    };
}

/// Defines the module of one file, with one test for each case, unless `without` names the file.
///
/// Each case lists the arguments that its function takes, from the ones that `__case!` passes:
/// `(cluster_id, broker_id, storage)`, `(broker_id, storage)` or `(storage)`. A case can start
/// with attributes for its test on every engine, such as `#[ignore = "..."]`, and then with
/// group names in brackets. Each group, `not_on_<engine>`, names one of Nisshi's engines that
/// leaves out the case, not what the case tests, and that engine's `without` names the group. A
/// case without a group runs on every engine.
#[doc(hidden)]
#[macro_export]
macro_rules! __file {
    (
        $area:ident,
        $file:ident,
        [
            $(
                $(#[$case_attribute:meta])*
                $([$($group:ident),+ $(,)?])?
                $case:ident $arguments:tt
            ),* $(,)?
        ],
        $config:tt
    ) => {
        $crate::__unless_listed!($file; $config;
            pub mod $file {
                #[allow(unused_imports)]
                use super::*;

                $(
                    $crate::__in_group!(($($($group),+)?); $config;
                        $crate::__case!(
                            $area, $file, [$($case_attribute),*] $case $arguments, $config
                        );
                    );
                )*
            }
        );
    };
}

/// Expands to the items after the second `;`, unless the config's `without` list holds one of
/// the case's groups. A case without a group always expands.
#[doc(hidden)]
#[macro_export]
macro_rules! __in_group {
    ((); $config:tt; $($item:tt)*) => { $($item)* };
    (($group:ident $(, $rest:ident)*); $config:tt; $($item:tt)*) => {
        $crate::__check_name!($group);
        $crate::__unless_listed!($group; $config;
            $crate::__in_group!(($($rest),*); $config; $($item)*);
        );
    };
}

/// Defines one test, which builds a fresh engine and runs one case on it.
///
/// There is one rule for each list of arguments that a case can take. A case with another list
/// fails the build.
#[doc(hidden)]
#[macro_export]
macro_rules! __case {
    (
        $area:ident, $file:ident, $attributes:tt $case:ident (cluster_id, broker_id, storage),
        $config:tt
    ) => {
        $crate::__case!(
            @test $area, $file, $attributes $case, $config,
            |cluster_id, broker_id, storage| {
                $crate::$area::$file::$case(cluster_id, broker_id, storage)
            }
        );
    };
    ($area:ident, $file:ident, $attributes:tt $case:ident (broker_id, storage), $config:tt) => {
        $crate::__case!(
            @test $area, $file, $attributes $case, $config,
            |_cluster_id, broker_id, storage| { $crate::$area::$file::$case(broker_id, storage) }
        );
    };
    ($area:ident, $file:ident, $attributes:tt $case:ident (storage), $config:tt) => {
        $crate::__case!(
            @test $area, $file, $attributes $case, $config,
            |_cluster_id, _broker_id, storage| { $crate::$area::$file::$case(storage) }
        );
    };
    ($area:ident, $file:ident, $attributes:tt $case:ident $arguments:tt, $config:tt) => {
        ::core::compile_error!(::core::concat!(
            "case `",
            ::core::stringify!($case),
            "` lists the arguments `",
            ::core::stringify!($arguments),
            "`; a case takes (cluster_id, broker_id, storage), (broker_id, storage) or (storage)"
        ));
    };
    (
        @test $area:ident, $file:ident, [$($case_attribute:meta),*] $case:ident,
        {
            storage: ($storage:expr),
            before_each: ($($before_each:expr)?),
            attributes: [$($attribute:meta),*],
            without: $without:tt
        },
        $run:expr
    ) => {
        #[::tokio::test]
        $(#[$attribute])*
        $(#[$case_attribute])*
        async fn $case() -> $crate::__private::TestResult {
            $(let _before_each = ($before_each)()?;)?

            let cluster_id = $crate::__private::Uuid::now_v7();
            let broker_id = $crate::__private::broker_id();
            let storage = ($storage)(cluster_id, broker_id).await?;

            ($run)(cluster_id, broker_id, storage).await?;
            ::core::result::Result::Ok(())
        }
    };
}

/// Expands to the items after the second `;`, unless the config's `without` list holds `name`.
///
/// `macro_rules!` can't compare two names that it receives. It can compare a name with a word
/// written into a rule, so each area, file and group has a rule of its own. These rules are the
/// only list of names: `__check_name!` uses them too. Each file name is unique across areas, so
/// that a name leaves out one file.
#[doc(hidden)]
#[macro_export]
macro_rules! __unless_listed {
    (
        $name:ident;
        { storage: $s:tt, before_each: $b:tt, attributes: $a:tt, without: [$($without:ident),*] };
        $($item:tt)*
    ) => {
        $crate::__unless_listed!(@search $name; [$($without),*]; $($item)*);
    };
    (@search consumer_group; [consumer_group $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search offset; [offset $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search producer_id; [producer_id $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search transactions; [transactions $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search init_producer_id; [init_producer_id $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search new_cg; [new_cg $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search not_on_slatedb; [not_on_slatedb $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search not_on_in_memory; [not_on_in_memory $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search not_on_turso; [not_on_turso $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search cg_dynamic; [cg_dynamic $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search cg_static; [cg_static $(, $rest:ident)*]; $($item:tt)*) => {};
    (
        @search consumer_group_describe;
        [consumer_group_describe $(, $rest:ident)*];
        $($item:tt)*
    ) => {};
    (@search delete_groups; [delete_groups $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search describe_groups; [describe_groups $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search find_coordinator; [find_coordinator $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search group_across_brokers; [group_across_brokers $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search group_cache_miss; [group_cache_miss $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search list_groups; [list_groups $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search rebalance_timing; [rebalance_timing $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search offset_commit_fence; [offset_commit_fence $(, $rest:ident)*]; $($item:tt)*) => {};
    (
        @search offset_commit_v1_timestamp;
        [offset_commit_v1_timestamp $(, $rest:ident)*];
        $($item:tt)*
    ) => {};
    (
        @search offset_fetch_commit_metadata;
        [offset_fetch_commit_metadata $(, $rest:ident)*];
        $($item:tt)*
    ) => {};
    (
        @search offset_fetch_require_stable;
        [offset_fetch_require_stable $(, $rest:ident)*];
        $($item:tt)*
    ) => {};
    (
        @search offset_metadata_max_size;
        [offset_metadata_max_size $(, $rest:ident)*];
        $($item:tt)*
    ) => {};
    (
        @search txn_offset_commit_fence;
        [txn_offset_commit_fence $(, $rest:ident)*];
        $($item:tt)*
    ) => {};
    (@search txn_init_producer_id; [txn_init_producer_id $(, $rest:ident)*]; $($item:tt)*) => {};
    (
        @search txn_offset_fetch_commit_metadata;
        [txn_offset_fetch_commit_metadata $(, $rest:ident)*];
        $($item:tt)*
    ) => {};
    (
        @search txn_offset_metadata_max_size;
        [txn_offset_metadata_max_size $(, $rest:ident)*];
        $($item:tt)*
    ) => {};
    (
        @search join_invalid_session_timeout;
        [join_invalid_session_timeout $(, $rest:ident)*];
        $($item:tt)*
    ) => {};
    (@search $name:ident; [$other:ident $(, $rest:ident)*]; $($item:tt)*) => {
        $crate::__unless_listed!(@search $name; [$($rest),*]; $($item)*);
    };
    (@search $name:ident; []; $($item:tt)*) => { $($item)* };
}

/// Fails the build when `name` is not an area, file or group of the suite.
///
/// A known name has a rule in `__unless_listed!` that matches a list holding only that name, so
/// the search expands to nothing. An unknown name reaches the end of the list, and the search
/// expands to the error.
#[doc(hidden)]
#[macro_export]
macro_rules! __check_name {
    ($name:ident) => {
        $crate::__unless_listed!(@search $name; [$name];
            ::core::compile_error!(::core::concat!(
                "`",
                ::core::stringify!($name),
                "` is not an area, file or group of nisshi-storage-integration-test"
            ));
        );
    };
}
