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

//! Fuzzes [`FetchValidationLayer`] wrapping [`FetchService`] end-to-end against
//! the in-memory storage backend, bypassing the wire protocol entirely: fuzzed
//! bytes are turned directly into a [`FetchRequest`] via `nisshi-sans-io`'s
//! `arbitrary` feature, then run through the same layering the broker routes
//! `Fetch` requests to. This targets bugs in the validation, storage, and
//! business logic (panics, mismatched invariants) rather than the wire
//! decoder, which `fuzz_request_decode` already covers.
//!
//! Storage starts empty for every execution (no topics/groups/ACLs are
//! pre-seeded), so this mainly exercises not-found/empty-state handling;
//! seeding realistic state first is a possible follow-up.
//!
//! `FetchRequest.max_wait_ms` drives a real `tokio::time::sleep` in
//! `FetchService`'s underlying storage call (it waits for `min_bytes` to
//! become available, up to `max_wait_ms`), independent of whether any
//! requested topic exists. Left unclamped, an arbitrary `i32` there would
//! make individual executions sleep for up to ~24.8 days instead of
//! exercising new logic, so it is capped at a few milliseconds below. Negative values are kept: they
//! are rejected before any wait, so they reach the validation path cheaply.

#![no_main]

use std::sync::{Arc, LazyLock};

use libfuzzer_sys::fuzz_target;
use nisshi_fuzz_harness::{api_version, check_request, check_response};
use nisshi_sans_io::{FetchRequest, RequestInput};
use nisshi_storage::{FetchService, FetchValidationLayer, StorageContainer};
use nisshi_storage_dynostore::MemoryEngineFactory;
use rama::{Layer as _, Service as _, extensions::Extensions};
use tokio::runtime::Runtime;
use url::Url;

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().expect("tokio runtime"));

fuzz_target!(|input: (u16, FetchRequest)| {
    let (selector, request) = input;
    let version = api_version::<FetchRequest>(selector);

    let mut request = request;
    request.max_wait_ms = request.max_wait_ms.min(5);

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

        let service = FetchValidationLayer::new().layer(FetchService { storage });

        service
            .serve(RequestInput {
                request,
                extensions: Extensions::default(),
            })
            .await
            .and_then(|response| check_response(response, version).map_err(Into::into))
            .expect("served and decoded")
    });
});
