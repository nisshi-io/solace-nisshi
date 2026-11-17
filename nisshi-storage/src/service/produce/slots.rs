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

use std::sync::Arc;

use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, error::Elapsed, timeout_at},
};

use super::deadline::{Deadline, PRODUCE_REQUEST_DEADLINE};

/// The slots for the storage calls of one Produce request, which its topics share: at most
/// `count` calls of the request run at once, whichever topics they belong to.
///
/// A slot is held for one call, and dropped when the call returns. A write that the deadline
/// left running has returned to its caller, so it holds no slot.
///
/// A storage that limits its writes across requests, see [`Storage::produce_permits`], also gives
/// each write a permit of that pool, which the partition takes after its slot, right before the
/// write starts, see [`take_shared`]. The permit is held as long as the write runs, so a write that
/// the deadline left running keeps it after the request answered and gave back its slot.
///
/// [`Storage::produce_permits`]: crate::Storage::produce_permits
#[derive(Clone, Debug)]
pub(crate) struct Slots {
    count: usize,
    permits: Arc<Semaphore>,
    shared: Option<Arc<Semaphore>>,
}

/// A permit of the pool that the writes of every request on a storage share. The write task
/// holds it until the write ends, so a write that the deadline left running keeps its place.
pub(crate) type SharedPermit = OwnedSemaphorePermit;

impl Slots {
    /// Returns `count` slots, at least one, and the writes of which also take a permit of
    /// `shared`, if there is one.
    pub(super) fn new(count: usize, shared: Option<Arc<Semaphore>>) -> Self {
        let count = count.max(1);

        Self {
            count,
            permits: Arc::new(Semaphore::new(count)),
            shared,
        }
    }

    /// How many calls of the request run at once.
    pub(super) fn count(&self) -> usize {
        self.count
    }

    /// Returns whether `other` is the same set of slots, not another set of the same size.
    #[cfg(test)]
    pub(super) fn is_same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.permits, &other.permits)
    }

    /// Waits for a slot until `deadline`, and returns `None` if the deadline passes first.
    pub(super) async fn take(&self, deadline: &Deadline) -> Option<OwnedSemaphorePermit> {
        // The semaphore is never closed, so the only way to get no permit is the deadline.
        deadline
            .bound(Arc::clone(&self.permits).acquire_owned())
            .await
            .ok()
            .and_then(Result::ok)
    }

    /// The pool of permits that the storage shares across requests, if it has one. A partition
    /// takes a permit of it with [`take_shared`] right before its write starts.
    pub(super) fn shared(&self) -> Option<Arc<Semaphore>> {
        self.shared.clone()
    }
}

/// Waits for a permit of `pool`, if there is one, and returns `None` for a storage without one.
/// The wait ends at `deadline`, or at [`PRODUCE_REQUEST_DEADLINE`] from now for a request that has
/// none (`acks=0`): the request must not park for as long as the writes ahead of it run, and
/// before the pool was shared, an `acks=0` produce waited for a connection at most as long as the
/// engine's own pool wait.
///
/// The permit comes after the request's slot, so a request never holds a permit while it waits
/// for its own slot, and the permits that it holds are the ones of its running writes.
///
/// [`PRODUCE_REQUEST_DEADLINE`]: super::deadline::PRODUCE_REQUEST_DEADLINE
pub(super) async fn take_shared(
    pool: Option<&Arc<Semaphore>>,
    deadline: &Deadline,
) -> Result<Option<SharedPermit>, Elapsed> {
    let Some(pool) = pool else {
        return Ok(None);
    };

    let at = deadline
        .at()
        .unwrap_or_else(|| Instant::now() + PRODUCE_REQUEST_DEADLINE);

    // The semaphore is never closed, so the only way to get no permit is the wait running out.
    timeout_at(at, Arc::clone(pool).acquire_owned())
        .await
        .map(Result::ok)
}
