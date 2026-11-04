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

//! Fuzzes [`CreateTopicsValidationLayer`] wrapping [`CreateTopicsService`] (the
//! [`Storage::create_topic`] request handler) end-to-end against the in-memory storage backend, bypassing the
//! wire protocol entirely: fuzzed bytes are turned directly into a
//! [`CreateTopicsRequest`] via `nisshi-sans-io`'s `arbitrary` feature, then
//! run through the same service the broker routes `CreateTopics` requests
//! to. This targets bugs in the storage/business logic (panics, mismatched
//! invariants) rather than the wire decoder, which `fuzz_request_decode`
//! already covers.

#![no_main]

use std::sync::{Arc, LazyLock};

use libfuzzer_sys::fuzz_target;
use nisshi_fuzz_harness::{api_version, check_request, check_response};
use nisshi_sans_io::CreateTopicsRequest;
use nisshi_storage::{CreateTopicsService, CreateTopicsValidationLayer, StorageContainer};
use nisshi_storage_dynostore::MemoryEngineFactory;
use rama::{Layer as _, Service as _};
use tokio::runtime::Runtime;
use url::Url;

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().expect("tokio runtime"));

// `partitions` and `replication_factor` are deliberately left unclamped: `CreateTopicsValidationLayer`
// rejects out of range values, explicit counts beside manual assignments (`INVALID_REQUEST`), (`INVALID_PARTITIONS`, `INVALID_REPLICATION_FACTOR`) and a request
// totalling more than 10,000 partitions (`POLICY_VIOLATION`) before storage is consulted, so
// an arbitrary `i32::MAX` cannot reach `create_topic`. Fuzzing the raw values checks that guard.
fuzz_target!(|input: (u16, CreateTopicsRequest)| {
    let (selector, request) = input;
    let version = api_version::<CreateTopicsRequest>(selector);

    RUNTIME.block_on(async {
        // Serve what a client could actually have sent: the request as decoded off the wire.
        let Some(request) = check_request(request, version).expect("encode/decode request") else {
            return;
        };

        let mut builder = StorageContainer::builder()
            .cluster_id("fuzz")
            .node_id(111)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092").expect("static url"));
        builder.with_factory(Arc::new(MemoryEngineFactory));

        let storage = builder
            .storage(Url::parse("memory://fuzz").expect("static url"))
            .silent(true)
            .build()
            .await
            .expect("in-memory storage always builds");

        let service = CreateTopicsValidationLayer::new().layer(CreateTopicsService { storage });

        service
            .serve(request)
            .await
            .and_then(|response| check_response(response, version).map_err(Into::into))
            .expect("served and decoded")
    });
});
