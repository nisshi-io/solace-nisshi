// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

#![allow(clippy::unwrap_used, clippy::expect_used)]
mod common;

// `pub` keeps each file's `pub` helpers reachable, as when every file was its own crate.
pub mod auth;
pub mod authentication_storage;
pub mod auto_create_topics;
pub mod cg;
pub mod cg_dynamic;
pub mod cg_latency;
pub mod cg_static;
pub mod consumer_group_describe;
pub mod create_topics;
pub mod create_topics_validate_only;
pub mod decode_error_log;
pub mod delete_groups;
pub mod delete_records;
pub mod delete_topics;
pub mod describe_cluster;
pub mod describe_configs;
pub mod describe_configs_topic_defaults;
pub mod describe_groups;
pub mod describe_groups_round_trip;
pub mod describe_topic_partitions;
pub mod fetch;
pub mod fetch_after_delete_records;
pub mod fetch_max_wait;
pub mod fetch_unknown_partition;
pub mod fetch_wire;
pub mod find_coordinator;
pub mod group_across_brokers;
pub mod group_cache_miss;
pub mod incremental_alter_configs;
pub mod init_producer_id;
pub mod join_invalid_session_timeout;
pub mod list_groups;
pub mod list_offsets;
pub mod list_offsets_after_retention;
pub mod list_offsets_storage_error;
pub mod list_partition_reassignments;
pub mod log_append_time;
pub mod log_redaction;
pub mod metadata;
pub mod new_cg;
pub mod offset_commit_fence;
pub mod offset_commit_unknown_partition;
pub mod offset_fetch_commit_metadata;
pub mod offset_fetch_require_stable;
pub mod offset_fetch_storage_error;
pub mod offset_metadata_max_size;
pub mod person;
pub mod pg_init_producer;
pub mod pg_txn;
pub mod policy_compact_delete;
#[cfg(feature = "libsql")]
pub mod pre_authentication_frame_size;
pub mod produce;
pub mod produce_acks_zero;
pub mod produce_crc;
pub mod produce_fetch;
pub mod produce_idempotent_duplicate;
pub mod produce_idempotent_epoch_bump;
#[cfg(feature = "postgres")]
pub mod produce_idempotent_race;
pub mod produce_invalid_acks;
pub mod produce_invalid_record;
pub mod produce_max_message_bytes;
pub mod produce_post_watermark_failure;
pub mod produce_post_write_advance_failed;
#[cfg(feature = "postgres")]
pub mod produce_row_locks;
pub mod rebalance_timing;
pub mod replication_factor;
pub mod retention_bytes;
pub mod retention_ms;
#[cfg(feature = "libsql")]
pub mod sasl_handshake;
pub mod sasl_scram_enforcement;
pub mod startup_failure;
pub mod storage_alter_user_scram_credentials;
pub mod storage_capabilities;
pub mod storage_create_acls;
pub mod storage_delete_acls;
pub mod storage_describe_acls;
pub mod storage_describe_cluster;
pub mod storage_describe_configs;
pub mod storage_fetch;
pub mod storage_group_version;
pub mod storage_list_offsets;
pub mod storage_metadata;
pub mod storage_options;
pub mod tls;
pub mod topic;
pub mod topic_config_aliases;
pub mod topic_config_read_failed;
pub mod topic_lifecycle;
pub mod traceforge_delete_topic_object_store_fault;
pub mod traceforge_group_join;
pub mod traceforge_offset_commit;
pub mod traceforge_produce_idempotent;
pub mod traceforge_produce_object_store_fault;
pub mod txn;
pub mod txn_offset_commit_fence;
pub mod txn_produce_unadded_partition;
#[cfg(feature = "libsql")]
pub mod unauthenticated;
pub mod update_group_conditional;
pub mod version_gate;

// Cargo only builds the modules declared above, so a file in `tests/it/` without a
// `pub mod` line would never compile or run. Fail instead of skipping it silently.
#[test]
fn every_test_file_is_declared() -> std::io::Result<()> {
    let declared = include_str!("main.rs");
    let mut undeclared = Vec::new();

    for entry in std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/it"))? {
        let path = entry?.path();
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };

        if path.extension().is_some_and(|extension| extension == "rs")
            && stem != "main"
            && !declared.contains(&format!("\npub mod {stem};\n"))
        {
            undeclared.push(stem.to_owned());
        }
    }

    assert!(
        undeclared.is_empty(),
        "add `pub mod <name>;` to tests/it/main.rs for: {undeclared:?}"
    );
    Ok(())
}
