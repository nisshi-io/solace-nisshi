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

//! Runs the suite as a caller outside Nisshi does: on an engine that the caller builds itself,
//! without `nisshi-broker`'s test helpers.
//!
//! `nisshi-broker` already runs the whole suite on the same engine, so this call runs only a few
//! files. It names each file that it leaves out, so that its `without` list is long enough to
//! need the higher recursion limit that the macro's doc tells a caller to set.

use std::sync::Arc;

use nisshi_storage::{ArcDynStorage, StorageContainer};
use url::Url;
use uuid::Uuid;

/// Builds a fresh in-memory engine only through the public API of `nisshi-storage` and the engine
/// crate, as a caller outside Nisshi does.
// The crate's `clippy.toml` bans the builder so that a case never builds an engine. A caller
// builds the engine that the suite tests, so this is the one place in the crate that may call it.
#[allow(clippy::disallowed_methods)]
async fn caller_built_engine(
    cluster_id: Uuid,
    broker_id: i32,
) -> nisshi_storage::Result<ArcDynStorage> {
    let mut builder = StorageContainer::builder();
    builder.with_factory(Arc::new(nisshi_storage_dynostore::MemoryEngineFactory));

    builder
        .cluster_id(cluster_id.to_string())
        .node_id(broker_id)
        .advertised_listener(Url::parse("tcp://127.0.0.1/")?)
        .storage(Url::parse("memory://")?)
        .build()
        .await
}

// The files that the call runs take each of the macro's argument lists. `list_groups` has the
// cases with the most groups, which nest the deepest search of the `without` list. The attribute
// changes nothing: it is here so that a real test build compiles the `attributes` option.
nisshi_storage_integration_test::storage_integration_tests! {
    engine: caller_engine,
    storage: caller_built_engine,
    attributes: [cfg(test)],
    without: [
        cg_dynamic,
        cg_static,
        consumer_group_describe,
        delete_groups,
        describe_groups,
        group_across_brokers,
        group_cache_miss,
        new_cg,
        rebalance_timing,
        offset_commit_fence,
        offset_commit_v1_timestamp,
        offset_fetch_commit_metadata,
        offset_metadata_max_size,
        txn_init_producer_id,
        txn_offset_commit_fence,
        txn_offset_fetch_commit_metadata,
        offset_fetch_require_stable,
        txn_offset_metadata_max_size,
    ],
}
