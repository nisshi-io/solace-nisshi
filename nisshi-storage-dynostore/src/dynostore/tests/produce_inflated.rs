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

//! A produce that hands the store a decoded batch holds that batch only when the schema registry
//! or the lake reads it.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use bytes::Bytes;
use nisshi_sans_io::record::{Record, inflated};
use nisshi_storage::Error;
use object_store::memory::InMemory;

use crate::dynostore::{DynoStore, tests::init_tracing};

/// Sets `dropped` when the last handle to the record bytes that own it goes away.
struct DropFlag {
    bytes: [u8; 1],
    dropped: Arc<AtomicBool>,
}

impl AsRef<[u8]> for DropFlag {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

/// Returns a decoded batch, and the flag that is set when the batch is dropped.
fn batch_with_drop_flag() -> Result<(inflated::Batch, Arc<AtomicBool>), Error> {
    let dropped = Arc::new(AtomicBool::new(false));

    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_owner(DropFlag {
            bytes: [0],
            dropped: dropped.clone(),
        }))))
        .build()?;

    Ok((batch, dropped))
}

#[tokio::test]
async fn a_store_without_a_registry_or_lake_drops_the_decoded_batch() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new("nisshi", 1, InMemory::new())?;
    let (batch, dropped) = batch_with_drop_flag()?;

    // The store drops the batch before the call returns, so a produce does not hold it across the
    // object store request that follows.
    assert!(storage.inflated_if_read(batch).is_none());
    assert!(dropped.load(Ordering::SeqCst));

    Ok(())
}
