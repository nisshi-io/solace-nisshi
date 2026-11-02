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

//! Fuzzes [`DeleteAclsService`], the service the broker routes `DeleteAcls` requests to, against
//! the in-memory storage backend. The target builds each [`DeleteAclsRequest`] from the fuzzed
//! bytes with the `arbitrary` feature of `nisshi-sans-io`, so it finds panics and broken
//! invariants in the service, not in the wire decoder.

#![no_main]

use std::sync::{Arc, LazyLock};

use fuzz::{api_version, check_request, check_response};
use libfuzzer_sys::fuzz_target;
use nisshi_sans_io::DeleteAclsRequest;
use nisshi_storage::{DeleteAclsService, StorageContainer};
use nisshi_storage_dynostore::MemoryEngineFactory;
use rama::Service as _;
use tokio::runtime::Runtime;
use url::Url;

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().expect("tokio runtime"));

fuzz_target!(|input: (u16, DeleteAclsRequest)| {
    let (selector, request) = input;
    let version = api_version::<DeleteAclsRequest>(selector);

    RUNTIME.block_on(async {
        // The service gets the request as the broker decodes it, because a client can send only
        // what the wire format encodes.
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

        let service = DeleteAclsService { storage };

        service
            .serve(request)
            .await
            .and_then(|response| check_response(response, version).map_err(Into::into))
            .expect("served and decoded")
    });
});
