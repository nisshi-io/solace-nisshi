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

use proc_macro2::{Ident, TokenStream, TokenTree};
use syn::{
    Attribute, LitStr, Macro, Meta, MetaList, Path as SynPath, PathSegment,
    visit::{self, Visit},
};

/// Names that show that a file builds a concrete engine: Nisshi's engine builders in `common`,
/// the engine factories, and the engine crates. A name counts when an identifier in the
/// code contains it, as `spawn_broker_on` contains `spawn_broker`, and not when it is only in a
/// comment. A longer name counts because a missed file passes silently, and a false match only
/// asks for the file to be listed.
///
/// When a helper in `common` builds an engine, add its name here. This test reads only the files
/// directly in `tests/it`, so it finds a call of the helper, and it does not find the engine that
/// the helper builds.
const ENGINE_NAMES: &[&str] = &[
    "StorageType",
    "storage_container",
    "memory_storage",
    "lite_storage",
    "slate_storage",
    "postgres_storage",
    "turso_storage",
    "EngineFactory",
    "spawn_broker",
    "nisshi_storage_dynostore",
    "nisshi_storage_null",
    "nisshi_storage_slatedb",
    "nisshi_storage_sql",
];

/// Pairs of consecutive path segments that build an engine, as in `Broker::builder`. A pair
/// counts when the first segment ends with the listed one, as `MemoryBroker::builder` does, for
/// the same reason as in `ENGINE_NAMES`.
const ENGINE_PATHS: &[[&str; 2]] = &[["StorageContainer", "builder"], ["Broker", "builder"]];

/// Types that build an engine when the code gives them generic arguments, as in
/// `Broker::<G, S>::new` or in a type such as `Broker<G, S>`. A type counts when its name ends with
/// the listed one, as in `ENGINE_PATHS`. Inside a macro call, the check sees only the form
/// `Broker::<`.
const ENGINE_GENERIC_TYPES: &[&str] = &["Broker"];

/// URL schemes that name an engine when a string literal contains them.
const ENGINE_URL_SCHEMES: &[&str] = &[
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
    "create_topics",
    "create_topics_validate_only",
    "delete_records",
    "delete_records_many_partitions",
    "delete_topics",
    "describe_cluster",
    "describe_configs",
    "describe_configs_broker_logger",
    "describe_configs_topic_defaults",
    "describe_topic_partitions",
    "describe_topic_partitions_invalid_name",
    "fetch",
    "fetch_empty_records",
    "fetch_header_bytes",
    "fetch_max_wait",
    "fetch_unknown_partition",
    "incremental_alter_configs",
    "internal_topic_names",
    "internal_topics",
    "list_offsets",
    "list_offsets_after_retention",
    "log_append_time",
    "metadata",
    "policy_compact_delete",
    "produce",
    "produce_crc",
    "produce_invalid_acks",
    "produce_invalid_record",
    "produce_max_message_bytes",
    "produce_schema_validation",
    "produce_unknown_partition_order",
    "retention_bytes",
    "retention_ms",
    "storage_alter_user_scram_credentials",
    "storage_describe_cluster",
    "storage_describe_configs",
    "storage_list_offsets",
    "storage_metadata",
    "topic_lifecycle",
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
    "storage_describe_config_keys",
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

/// The file that runs `nisshi-storage-integration-test` on each of Nisshi's engines.
const RUNS_THE_SUITE: &[&str] = &["storage_integration"];

const LISTS: &[&[&str]] = &[
    RUNS_THE_SUITE,
    NOT_YET_MOVED,
    TESTS_THE_ENGINE,
    ENGINE_DOES_NOT_MATTER,
    ONE_ENGINE,
    FAULTY_ENGINE,
    NO_SHARED_CASE,
];

/// Records whether the code it visits builds a concrete engine.
#[derive(Default)]
struct EngineFinder {
    found: bool,
}

impl EngineFinder {
    fn name(&mut self, name: &str) {
        self.found |= ENGINE_NAMES.iter().any(|listed| name.contains(listed));
    }

    fn path(&mut self, first: &str, second: &str) {
        self.found |= ENGINE_PATHS.iter().any(|[listed_first, listed_second]| {
            first.ends_with(listed_first) && second == *listed_second
        });
    }

    fn generic_type(&mut self, name: &str) {
        self.found |= ENGINE_GENERIC_TYPES
            .iter()
            .any(|listed| name.ends_with(listed));
    }

    fn string(&mut self, value: &str) {
        self.found |= ENGINE_URL_SCHEMES
            .iter()
            .any(|scheme| value.contains(scheme));
    }

    /// Scans the tokens of a macro call, which `syn` leaves unparsed.
    fn tokens(&mut self, stream: TokenStream) {
        let trees: Vec<TokenTree> = stream.into_iter().collect();

        for (index, tree) in trees.iter().enumerate() {
            match tree {
                TokenTree::Ident(ident) => {
                    self.name(&ident.to_string());

                    if let [TokenTree::Punct(first), TokenTree::Punct(second), next] =
                        &trees[(index + 1).min(trees.len())..(index + 4).min(trees.len())]
                        && first.as_char() == ':'
                        && second.as_char() == ':'
                    {
                        match next {
                            TokenTree::Ident(next) => {
                                self.path(&ident.to_string(), &next.to_string());
                            }
                            TokenTree::Punct(next) if next.as_char() == '<' => {
                                self.generic_type(&ident.to_string());
                            }
                            _ => {}
                        }
                    }
                }
                TokenTree::Literal(literal) => {
                    if let Ok(literal) = syn::parse_str::<LitStr>(&literal.to_string()) {
                        self.string(&literal.value());
                    }
                }
                TokenTree::Group(group) => self.tokens(group.stream()),
                TokenTree::Punct(_) => {}
            }
        }
    }
}

impl<'ast> Visit<'ast> for EngineFinder {
    fn visit_attribute(&mut self, attribute: &'ast Attribute) {
        // A doc comment reaches `syn` as a `doc` attribute, and its text is not code.
        if attribute.path().is_ident("doc") {
            return;
        }

        // The default visitor does not read the tokens of an attribute's argument list.
        if let Meta::List(MetaList { tokens, .. }) = &attribute.meta {
            self.tokens(tokens.clone());
        }
        visit::visit_attribute(self, attribute);
    }

    fn visit_ident(&mut self, ident: &'ast Ident) {
        self.name(&ident.to_string());
    }

    fn visit_path(&mut self, path: &'ast SynPath) {
        for pair in path.segments.iter().collect::<Vec<_>>().windows(2) {
            self.path(&pair[0].ident.to_string(), &pair[1].ident.to_string());
        }
        visit::visit_path(self, path);
    }

    fn visit_path_segment(&mut self, segment: &'ast PathSegment) {
        if !segment.arguments.is_none() {
            self.generic_type(&segment.ident.to_string());
        }
        visit::visit_path_segment(self, segment);
    }

    fn visit_lit_str(&mut self, literal: &'ast LitStr) {
        self.string(&literal.value());
    }

    fn visit_macro(&mut self, mac: &'ast Macro) {
        self.tokens(mac.tokens.clone());
        visit::visit_macro(self, mac);
    }
}

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

        // This file holds every name in the lists above, but it does not build an engine.
        if stem == "engine_building_files" {
            continue;
        }

        if builds_engine(&fs::read_to_string(&path)?)
            .map_err(|error| io::Error::other(format!("{}: {error}", path.display())))?
        {
            _ = files.insert(stem.to_owned());
        }
    }

    Ok(files)
}

/// Returns whether the Rust source builds a concrete engine.
fn builds_engine(source: &str) -> syn::Result<bool> {
    let file = syn::parse_file(source)?;

    let mut finder = EngineFinder::default();
    finder.visit_file(&file);
    Ok(finder.found)
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
        "tests/it/engine_building_files.rs lists files that no longer exist, or in which the check \
         no longer finds an engine name, path, generic type or URL scheme: {stale:?}. If a file \
         still builds an engine through a new helper, add the helper's name to ENGINE_NAMES. If \
         not, remove the file from its list"
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

#[test]
fn longer_name_that_contains_an_engine_name_builds_an_engine() -> syn::Result<()> {
    assert!(builds_engine(
        "fn f() { spawn_broker_with_authentication(x); }"
    )?);
    Ok(())
}

#[test]
fn path_whose_first_segment_ends_with_a_listed_one_builds_an_engine() -> syn::Result<()> {
    assert!(builds_engine("fn f() { MemoryBroker::builder(); }")?);
    Ok(())
}

#[test]
fn engine_name_in_a_macro_call_builds_an_engine() -> syn::Result<()> {
    assert!(builds_engine("engine_tests!(memory_storage);")?);
    Ok(())
}

#[test]
fn string_that_contains_a_url_scheme_builds_an_engine() -> syn::Result<()> {
    assert!(builds_engine(
        r#"const NAME: &str = "in-memory (memory://)";"#
    )?);
    Ok(())
}

#[test]
fn url_scheme_in_a_doc_comment_does_not_build_an_engine() -> syn::Result<()> {
    assert!(!builds_engine("/// Runs on `null://`.\nfn f() {}")?);
    Ok(())
}

#[test]
fn engine_name_in_an_attribute_argument_builds_an_engine() -> syn::Result<()> {
    assert!(builds_engine("#[test_case(memory_storage)]\nfn f() {}")?);
    Ok(())
}

#[test]
fn engine_name_in_a_comment_does_not_build_an_engine() -> syn::Result<()> {
    assert!(!builds_engine("// memory_storage\nfn f() {}")?);
    Ok(())
}

#[test]
fn file_without_an_engine_name_does_not_build_an_engine() -> syn::Result<()> {
    assert!(!builds_engine("fn f() { let broker = 1; }")?);
    Ok(())
}
