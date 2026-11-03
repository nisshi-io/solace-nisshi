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

//! Fuzzes [`MetadataService`] end-to-end against the in-memory storage backend,
//! bypassing the wire protocol entirely: fuzzed bytes are turned directly
//! into a [`MetadataRequest`] via `nisshi-sans-io`'s `arbitrary` feature, then run
//! through the same service the broker routes `Metadata` requests to. This
//! targets bugs in the storage/business logic (panics, mismatched
//! invariants) rather than the wire decoder, which `fuzz_request_decode`
//! already covers.
//!
//! Storage starts empty for every execution (no topics/groups/ACLs are
//! pre-seeded), so this mainly exercises not-found/empty-state handling;
//! seeding realistic state first is a possible follow-up.

#![no_main]

use std::sync::{Arc, LazyLock};

use fuzz::{check_request, check_response, normalize_request};
use libfuzzer_sys::fuzz_target;
use nisshi_sans_io::MetadataRequest;
use nisshi_storage::{AutoCreateRefusals, MetadataService, StorageContainer, TopicCreationConfig};
use nisshi_storage_dynostore::MemoryEngineFactory;
use rama::Service as _;
use tokio::runtime::Runtime;
use url::Url;

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().expect("tokio runtime"));

fuzz_target!(|request: MetadataRequest| {
    RUNTIME.block_on(async {
        check_request(request.clone()).expect("encode/decode request");

        // Serve what a client could actually have sent: the request as decoded off the wire.
        let request = normalize_request(request).expect("normalized request");

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

        let service = MetadataService {
            storage,
            topic_creation: TopicCreationConfig::default(),
            auto_create_refusals: AutoCreateRefusals::default(),
        };

        service
            .serve(request)
            .await
            .and_then(|response| check_response(response).map_err(Into::into))
            .expect("served and decoded")
    });
});
