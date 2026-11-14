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
#[derive(Clone, Debug)]
pub(crate) struct Slots {
    count: usize,
    permits: Arc<Semaphore>,
}

impl Slots {
    /// Returns `count` slots, at least one.
    pub(super) fn new(count: usize) -> Self {
        let count = count.max(1);

        Self {
            count,
            permits: Arc::new(Semaphore::new(count)),
        }
    }

    /// How many calls of the request run at once.
    pub(super) fn count(&self) -> usize {
        self.count
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
}
