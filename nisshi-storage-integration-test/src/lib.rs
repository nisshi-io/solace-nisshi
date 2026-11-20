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
//! The crate's API and its test names may change in any commit. A caller updates its call and
//! its skip list when it moves to a newer Nisshi.

// The cases are tests, and a test fails on an unexpected value, as nisshi-broker's tests do.
#![allow(clippy::unwrap_used, clippy::expect_used)]

pub mod broker;
pub mod consumer_group;
pub mod group;
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
///   does not have fails the build.
///
/// The tests use `#[tokio::test]`, so the caller needs `tokio` as a dev-dependency.
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
        $crate::__area_producer_id!($config);
        $crate::__area_transactions!($config);
    };
}

/// Defines the `consumer_group` area, unless `without` names it.
///
/// Each case lists the arguments that its function takes, from the ones that `__case!` passes:
/// `(cluster_id, broker_id, storage)`, `(broker_id, storage)` or `(storage)`. A case can start
/// with attributes for its test on every engine, such as `#[ignore = "..."]`, and then with a
/// group name in brackets, so that `without` can leave out the group. A group names the engines
/// that leave it out, not what its cases test: `not_on_slatedb` holds the cases that SlateDB
/// doesn't run, because the repository keeps SlateDB's test coverage as it is.
#[doc(hidden)]
#[macro_export]
macro_rules! __area_consumer_group {
    ($config:tt) => {
        $crate::__unless_listed!(consumer_group; $config;
            pub mod consumer_group {
                #[allow(unused_imports)]
                use super::*;

                $crate::__file!(consumer_group, join_invalid_session_timeout, [
                    reject_invalid_session_timeout_on_join(cluster_id, broker_id, storage),
                    accept_boundary_session_timeout_on_join(cluster_id, broker_id, storage),
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
            }
        );
    };
}

/// Defines the `producer_id` area, unless `without` names it. The case lists follow the rules of
/// `__area_consumer_group!`.
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
/// lists follow the rules of `__area_consumer_group!`.
#[doc(hidden)]
#[macro_export]
macro_rules! __area_transactions {
    ($config:tt) => {
        $crate::__unless_listed!(transactions; $config;
            pub mod transactions {
                #[allow(unused_imports)]
                use super::*;

                $crate::__file!(transactions, init_producer_id, [
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
            }
        );
    };
}

/// Defines the module of one file, with one test for each case, unless `without` names the file.
#[doc(hidden)]
#[macro_export]
macro_rules! __file {
    (
        $area:ident,
        $file:ident,
        [
            $(
                $(#[$case_attribute:meta])*
                $([$group:ident])?
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
                    $crate::__in_group!(($($group)?); $config;
                        $crate::__case!(
                            $area, $file, [$($case_attribute),*] $case $arguments, $config
                        );
                    );
                )*
            }
        );
    };
}

/// Expands to the items after the second `;`, unless the case's group is in the config's
/// `without` list. A case without a group always expands.
#[doc(hidden)]
#[macro_export]
macro_rules! __in_group {
    ((); $config:tt; $($item:tt)*) => { $($item)* };
    (($group:ident); $config:tt; $($item:tt)*) => {
        $crate::__check_name!($group);
        $crate::__unless_listed!($group; $config; $($item)*);
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
/// only list of names: `__check_name!` uses them too. A file name that two areas share, such as
/// `init_producer_id`, leaves out the file in both.
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
    (@search producer_id; [producer_id $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search transactions; [transactions $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search init_producer_id; [init_producer_id $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search new_cg; [new_cg $(, $rest:ident)*]; $($item:tt)*) => {};
    (@search not_on_slatedb; [not_on_slatedb $(, $rest:ident)*]; $($item:tt)*) => {};
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
