// Copyright ⓒ Solace Corporation
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

use std::{
    fmt::{self, Debug},
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use nisshi_sans_io::{
    Compression,
    record::{MAX_DECODED_BATCH_BYTES, Record, deflated},
};
use tokio::time::Instant;

use crate::decoded_budget::{self, DecodedBytesBudget, Exhausted, Reservation};

/// The message that goes with `REQUEST_TIMED_OUT` when the decoded-batch budget stays spent.
pub(super) const BUDGET_SPENT: &str =
    "the broker is holding too many decoded produce batches, retry the request";

/// What a Produce request needs to reserve the bytes of a decoded batch: the budget, and how long
/// the whole request may still wait for it.
///
/// The partitions of a request share one `Admission`, and a partition takes from its remaining
/// wait only the time that it spends waiting for the budget. The time that an earlier partition
/// spends decoding or in the storage engine does not count, so a slow storage engine does not
/// make a later partition answer `REQUEST_TIMED_OUT` while the budget is free. A request serves
/// its partitions one after another, so the remaining wait needs no further synchronisation.
#[derive(Clone)]
pub struct Admission {
    budget: Arc<DecodedBytesBudget>,
    remaining: Arc<Mutex<Duration>>,
}

impl Debug for Admission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Admission)).finish()
    }
}

impl Default for Admission {
    /// Returns the admission of a request that uses the process-wide budget, and waits at most
    /// `decoded_budget::WAIT_TIMEOUT` for it.
    fn default() -> Self {
        Self::new(decoded_budget::produce(), decoded_budget::WAIT_TIMEOUT)
    }
}

impl Admission {
    pub(crate) fn new(budget: Arc<DecodedBytesBudget>, wait: Duration) -> Self {
        Self {
            budget,
            remaining: Arc::new(Mutex::new(wait)),
        }
    }

    /// Waits for `bytes` of the budget, for at most the remaining wait, and takes the time that
    /// this took, on success as well as on failure, out of the remaining wait.
    pub(super) async fn reserve(&self, bytes: usize) -> Result<Reservation, Exhausted> {
        let remaining = *self
            .remaining
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        let started = Instant::now();
        let reserved = self.budget.reserve(bytes, started + remaining).await;

        *self
            .remaining
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = remaining.saturating_sub(started.elapsed());

        reserved
    }
}

/// The most that decoding `batch` can charge to the budget.
///
/// A compressed batch can decode to any size up to [`MAX_DECODED_BATCH_BYTES`], and nothing in
/// the batch says which: a Snappy block declares a size that a client controls, and a zstd
/// frame from a streaming encoder, which is what Kafka clients write, carries none. So the
/// reservation is the maximum, and [`Reservation::shrink_to`] returns the excess after the
/// decode.
///
/// An uncompressed batch decodes to its records, whose `Bytes` share the wire buffer, so the
/// decoded size is the `Record` and `Header` structures. A record is at least 7 bytes on the
/// wire and `size_of::<Record>()` decoded, and a header at least 2 bytes on the wire and 64
/// decoded, so 32 times the record data bounds the headers, and each counted record bounds the
/// records.
pub(super) fn decoded_upper_bound(batch: &deflated::Batch) -> usize {
    match Compression::try_from(batch.attributes) {
        Ok(Compression::None) => batch
            .record_data
            .len()
            .saturating_mul(32)
            .saturating_add(
                usize::try_from(batch.record_count)
                    .unwrap_or(usize::MAX)
                    .saturating_mul(size_of::<Record>()),
            )
            .min(MAX_DECODED_BATCH_BYTES),
        _ => MAX_DECODED_BATCH_BYTES,
    }
}
