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

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::deadline::Deadline;

/// The slots for the storage calls of one Produce request, which its topics share: at most
/// `count` calls of the request run at once, whichever topics they belong to.
///
/// A slot is held for one call, and dropped when the call returns. A write that the deadline
/// left running has returned to its caller, so it holds no slot.
///
/// A storage that limits its writes across requests, see [`Storage::produce_permits`], also gives
/// each write a permit of that pool. The permit is held as long as the write runs, so a write that
/// the deadline left running keeps it after the request answered and gave back its slot.
///
/// [`Storage::produce_permits`]: crate::Storage::produce_permits
#[derive(Clone, Debug)]
pub(crate) struct Slots {
    count: usize,
    permits: Arc<Semaphore>,
    shared: Option<Arc<Semaphore>>,
}

/// What a write holds while it runs: its request's slot, and a permit that the storage shares
/// across requests, if it has one.
#[derive(Debug)]
pub(crate) struct Write {
    _slot: OwnedSemaphorePermit,
    _shared: Option<OwnedSemaphorePermit>,
}

impl Write {
    /// Splits the write into its request's slot, and the permit that the storage shares across
    /// requests, if it has one. The write task keeps the permit for as long as the write runs.
    pub(super) fn into_parts(self) -> (OwnedSemaphorePermit, Option<SharedPermit>) {
        (self._slot, self._shared.map(Arc::new))
    }
}

/// A permit of the pool that the writes of every request on a storage share. The request and its
/// write task each hold a clone, so the permit returns to the pool when both are done.
pub(crate) type SharedPermit = Arc<OwnedSemaphorePermit>;

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

    /// Waits until `deadline` for a slot and, if the storage shares permits across requests, one
    /// of them, and returns `None` if the deadline passes first.
    ///
    /// The slot comes first, so a request never holds a shared permit while it waits for its own
    /// slot, and the permits that it holds are the ones of its running writes.
    pub(super) async fn take_write(&self, deadline: &Deadline) -> Option<Write> {
        let slot = self.take(deadline).await?;

        let shared = match &self.shared {
            Some(shared) => Some(
                deadline
                    .bound(Arc::clone(shared).acquire_owned())
                    .await
                    .ok()
                    .and_then(Result::ok)?,
            ),
            None => None,
        };

        Some(Write {
            _slot: slot,
            _shared: shared,
        })
    }
}
