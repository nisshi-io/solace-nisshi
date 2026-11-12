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

//! A decoded batch whose drop a test can observe, so that a test can tell whether a wrapper still
//! holds the batch while it waits.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use bytes::Bytes;
use nisshi_sans_io::record::{Record, inflated};

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

/// Returns a decoded batch, and the flag that is set when the last handle to its record bytes
/// goes away.
pub(crate) fn probe() -> (inflated::Batch, Arc<AtomicBool>) {
    let dropped = Arc::new(AtomicBool::new(false));

    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_owner(DropFlag {
            bytes: [0],
            dropped: dropped.clone(),
        }))))
        .build()
        .expect("a batch of one record");

    (batch, dropped)
}
