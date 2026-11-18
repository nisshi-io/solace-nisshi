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

//! The object store engines answer a produce concurrency of 1: more writers per request only
//! add contention on the cluster's `meta.json`, which every idempotent produce rewrites.

use nisshi_storage::{Result, Storage as _};
use object_store::memory::InMemory;

use crate::dynostore::{DynoStore, tests::init_tracing};

#[test]
fn an_object_store_engine_writes_one_partition_of_a_request_at_a_time() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new("nisshi", 12324, InMemory::new())?;

    assert_eq!(1, storage.produce_concurrency());
    assert!(storage.produce_permits().is_none());

    Ok(())
}
