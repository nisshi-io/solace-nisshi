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
    time::error::Elapsed,
};

use super::deadline::Deadline;

/// The slots for the storage calls of one Produce request, which its topics share: at most
/// `count` calls of the request run at once, whichever topics they belong to.
///
/// A slot is held for one call, and dropped when the call returns. A write that the deadline
/// left running has returned to its caller, so it holds no slot.
///
/// A storage that limits its writes across requests, see [`Storage::produce_permits`], gives the
/// request a [`WritePool`] too, which the partition uses after its slot, right before the write
/// starts, see [`take_write`]. What `take_write` returns is held as long as the write runs, so a
/// write that the deadline left running keeps it after the request answered and gave back its
/// slot.
///
/// [`Storage::produce_permits`]: crate::Storage::produce_permits
#[derive(Clone, Debug)]
pub(crate) struct Slots {
    count: usize,
    permits: Arc<Semaphore>,
    pool: Option<WritePool>,
}

/// How the writes of one request take their place among the writes that the storage shares
/// across requests.
///
/// The request's first write takes no permit of the shared pool: it uses the request's own lane,
/// which has one place. The writes beyond it each try for
/// a permit of the shared pool, and a write that finds none free waits for the lane, so a request
/// that gets no permit writes its partitions one at a time. The shared pool therefore caps only
/// the extra writes, and no request writes more slowly than it would without the pool.
#[derive(Clone, Debug)]
pub(crate) struct WritePool {
    shared: Arc<Semaphore>,
    lane: Arc<Semaphore>,
}

/// What a write holds until it ends: a permit of the shared pool or the place of its request's
/// lane, or nothing for a storage without a shared pool. The write task holds it, so a write that
/// the deadline left running keeps its place.
pub(crate) type WritePermit = OwnedSemaphorePermit;

impl Slots {
    /// Returns `count` slots, at least one, whose writes also take their place among `shared`,
    /// if there is a shared pool.
    pub(super) fn new(count: usize, shared: Option<Arc<Semaphore>>) -> Self {
        let count = count.max(1);

        Self {
            count,
            permits: Arc::new(Semaphore::new(count)),
            pool: shared.map(|shared| WritePool {
                shared,
                lane: Arc::new(Semaphore::new(1)),
            }),
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

    /// The pool that the storage shares across requests, if it has one. A partition takes its
    /// place in it with [`take_write`] right before its write starts.
    pub(super) fn pool(&self) -> Option<WritePool> {
        self.pool.clone()
    }
}

/// Takes the place of one write among the writes of a storage with a shared pool, and returns
/// `None` for a storage without one, which needs none.
///
/// The request's lane comes first, so its first write holds no permit of the shared pool. A write
/// that finds the lane taken tries for a permit of the shared pool and takes one if it is free.
/// With none free it waits for the lane, until `deadline` or, for a request that has none
/// (`acks=0`), until the write that holds the lane ends. It waits for no other request's write,
/// so it waits no longer than the request's own earlier write runs.
///
/// The place comes after the request's slot, so a request never holds one while it waits for its
/// own slot, and the places that it holds are the ones of its running writes.
pub(super) async fn take_write(
    pool: Option<&WritePool>,
    deadline: &Deadline,
) -> Result<Option<WritePermit>, Elapsed> {
    let Some(pool) = pool else {
        return Ok(None);
    };

    if let Ok(lane) = Arc::clone(&pool.lane).try_acquire_owned() {
        return Ok(Some(lane));
    }

    if let Ok(permit) = Arc::clone(&pool.shared).try_acquire_owned() {
        return Ok(Some(permit));
    }

    // The semaphore is never closed, so the only way to get no permit is the deadline.
    deadline
        .bound(Arc::clone(&pool.lane).acquire_owned())
        .await
        .map(Result::ok)
}
