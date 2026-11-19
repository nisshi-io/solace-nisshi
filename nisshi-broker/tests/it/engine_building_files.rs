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

//! Keeps the broker tests that run on any engine out of this test binary.
//!
//! A test here runs only on the engines that its file builds. A test of Nisshi's broker code that
//! works on any engine belongs in `nisshi-storage-integration-test`, so that every engine runs
//! it, also an engine outside this repository. Each file here that builds a concrete engine is
//! on one of the lists below, under the reason that it builds one.

use std::{collections::BTreeSet, fs, io, path::Path};

/// Text that shows that a file builds a concrete engine: Nisshi's engine builders in `common`,
/// the storage and broker builders, the engine crates, and the engines' URL schemes.
///
/// When a helper in `common` builds an engine, add its name here. This test reads only the files
/// directly in `tests/it`, so it finds a call of the helper, and it does not find the engine that
/// the helper builds.
const ENGINE_BUILDERS: &[&str] = &[
    "StorageType",
    "storage_container",
    "memory_storage",
    "lite_storage",
    "slate_storage",
    "postgres_storage",
    "EngineFactory",
    "StorageContainer::builder",
    "Broker::builder",
    "Broker::<",
    "spawn_broker",
    "nisshi_storage_dynostore",
    "nisshi_storage_null",
    "nisshi_storage_slatedb",
    "nisshi_storage_sql",
    "memory://",
    "null://",
    "postgres://",
    "slatedb://",
    "sqlite://",
    "turso://",
];

/// Files that test Nisshi's broker code on any engine, and still have to move to
/// `nisshi-storage-integration-test`.
const NOT_YET_MOVED: &[&str] = &[
    "auto_create_topics",
    "cg_dynamic",
    "cg_static",
    "consumer_group_describe",
    "create_topics",
    "create_topics_validate_only",
    "delete_groups",
    "delete_records",
    "delete_records_many_partitions",
    "delete_topics",
    "describe_cluster",
    "describe_configs",
    "describe_configs_broker_logger",
    "describe_configs_topic_defaults",
    "describe_groups",
    "describe_topic_partitions",
    "describe_topic_partitions_invalid_name",
    "fetch",
    "fetch_empty_records",
    "fetch_header_bytes",
    "fetch_max_wait",
    "fetch_unknown_partition",
    "find_coordinator",
    "group_across_brokers",
    "group_cache_miss",
    "incremental_alter_configs",
    "init_producer_id",
    "internal_topic_names",
    "internal_topics",
    "join_invalid_session_timeout",
    "list_groups",
    "list_offsets",
    "list_offsets_after_retention",
    "log_append_time",
    "metadata",
    "new_cg",
    "offset_commit_fence",
    "offset_commit_v1_timestamp",
    "offset_fetch_commit_metadata",
    "offset_fetch_require_stable",
    "offset_metadata_max_size",
    "policy_compact_delete",
    "produce",
    "produce_crc",
    "produce_invalid_acks",
    "produce_invalid_record",
    "produce_max_message_bytes",
    "produce_schema_validation",
    "produce_unknown_partition_order",
    "rebalance_timing",
    "retention_bytes",
    "retention_ms",
    "storage_alter_user_scram_credentials",
    "storage_describe_cluster",
    "storage_describe_configs",
    "storage_list_offsets",
    "storage_metadata",
    "topic_lifecycle",
    "txn_offset_commit_fence",
    "txn_produce_unadded_partition",
    "unknown_topic_id",
];

/// Files that call the engine's own methods to do what they check, so they test the engine
/// itself. Each engine's own tests cover that.
const TESTS_THE_ENGINE: &[&str] = &[
    "cg",
    "describe_groups_round_trip",
    "fetch_after_delete_records",
    "fetch_deadline",
    "offset_commit_unknown_partition",
    "person",
    "produce_fetch",
    "produce_idempotent_duplicate",
    "produce_idempotent_epoch_bump",
    "produce_idempotent_first_sequence",
    "produce_idempotent_sequence_wrap",
    "produce_inflated",
    "replication_factor",
    "storage_group_version",
    "topic",
    "topic_config_aliases",
    "txn",
    "update_group_conditional",
];

/// Files that test the broker's network, authentication, routes, shutdown or wire-level request
/// handling, which work the same on every engine, so one engine is enough.
const ENGINE_DOES_NOT_MATTER: &[&str] = &[
    "decode_error_log",
    "extend_routes",
    "fetch_wire",
    "list_partition_reassignments",
    "log_redaction",
    "metadata_authorized_operations",
    "metadata_empty_topics",
    "metadata_null_topics",
    "metadata_topic_ids",
    "pre_authentication_frame_size",
    "sasl_handshake",
    "sasl_scram_enforcement",
    "shutdown",
    "shutdown_produce_writes",
    "startup_failure",
    "tls",
    "unauthenticated",
    "version_gate",
];

/// Files whose results depend on which of Nisshi's engines they run on: the behaviour of one
/// engine, or how the broker starts on each engine's URL.
const ONE_ENGINE: &[&str] = &[
    "authentication_storage",
    "dynostore_meta_cache",
    "pg_init_producer",
    "pg_txn",
    "produce_idempotent_race",
    "produce_row_locks",
    "storage_capabilities",
    "storage_options",
];

/// Files that wrap an engine, or the object store under it, to make it fail, so they need an
/// engine whose layers they control.
const FAULTY_ENGINE: &[&str] = &[
    "fetch_metadata_storage_error",
    "fetch_storage_error",
    "group_storage_unavailable",
    "list_offsets_storage_error",
    "metadata_known_topics",
    "produce_post_watermark_failure",
    "produce_post_write_advance_failed",
    "topic_config_read_failed",
];

/// Files whose results are the same on every engine, but whose tests build the engine in their own
/// harness: a model checker, a broker that the test starts from a storage URL, or a single test on
/// the in-memory engine. A shared case takes its engine from the caller, so these files stay.
const NO_SHARED_CASE: &[&str] = &[
    "cg_latency",
    "produce_acks_zero",
    "storage_create_acls",
    "storage_delete_acls",
    "storage_describe_acls",
    "storage_fetch",
    "traceforge_delete_topic_object_store_fault",
    "traceforge_group_join",
    "traceforge_offset_commit",
    "traceforge_produce_idempotent",
    "traceforge_produce_object_store_fault",
];

const LISTS: &[&[&str]] = &[
    NOT_YET_MOVED,
    TESTS_THE_ENGINE,
    ENGINE_DOES_NOT_MATTER,
    ONE_ENGINE,
    FAULTY_ENGINE,
    NO_SHARED_CASE,
];

/// Returns the name of each file in `tests/it` that builds a concrete engine.
fn engine_building_files() -> io::Result<BTreeSet<String>> {
    let mut files = BTreeSet::new();

    for entry in fs::read_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/it"))? {
        let path = entry?.path();
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };

        if path.extension().is_none_or(|extension| extension != "rs") || stem == "main" {
            continue;
        }

        // This file holds every text in `ENGINE_BUILDERS`, but it does not build an engine.
        if stem == "engine_building_files" {
            continue;
        }

        let source = fs::read_to_string(&path)?;
        if ENGINE_BUILDERS
            .iter()
            .any(|builder| source.contains(builder))
        {
            _ = files.insert(stem.to_owned());
        }
    }

    Ok(files)
}

/// Each file that builds a concrete engine is on one list, so that a reviewer sees why it builds
/// one, and a test that runs on any engine goes to `nisshi-storage-integration-test` instead.
#[test]
fn every_engine_building_file_is_listed() -> io::Result<()> {
    let building = engine_building_files()?;
    let listed: Vec<&str> = LISTS.iter().flat_map(|list| list.iter().copied()).collect();

    let unlisted: Vec<&String> = building
        .iter()
        .filter(|file| !listed.contains(&file.as_str()))
        .collect();
    assert!(
        unlisted.is_empty(),
        "these files build a concrete engine: {unlisted:?}. If a file's tests check Nisshi's \
         broker code on any engine, move them to nisshi-storage-integration-test; if not, add the \
         file to a list in tests/it/engine_building_files.rs, under the reason that it builds one"
    );

    let stale: Vec<&str> = listed
        .iter()
        .copied()
        .filter(|file| !building.contains(*file))
        .collect();
    assert!(
        stale.is_empty(),
        "tests/it/engine_building_files.rs lists files that no longer exist or no longer build an \
         engine; remove them from its lists: {stale:?}"
    );

    let mut seen = BTreeSet::new();
    let twice: Vec<&str> = listed
        .iter()
        .copied()
        .filter(|file| !seen.insert(*file))
        .collect();
    assert!(
        twice.is_empty(),
        "tests/it/engine_building_files.rs lists these files under more than one reason: {twice:?}"
    );

    Ok(())
}
