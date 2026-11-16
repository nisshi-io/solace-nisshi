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

use std::{
    collections::{BTreeSet, HashMap},
    future::Future,
    sync::{Arc, LazyLock, Mutex, PoisonError},
    time::Duration,
};

use opentelemetry::metrics::UpDownCounter;
use tokio::time::timeout;
use tokio_util::task::TaskTracker;

use crate::{METER, Topition};

static DETACHED_WRITES: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    METER
        .i64_up_down_counter("nisshi_produce_detached_writes")
        .with_description(
            "Produce writes that run on after the broker answered their request at its deadline",
        )
        .build()
});

/// The set that every service of this process shares, as the decoded-bytes budget is shared. The
/// broker builds a produce service for each connection, and a retry usually arrives on a new one.
static PROCESS: LazyLock<Detached> = LazyLock::new(Detached::default);

/// A partition and the producer that wrote to it. A non-idempotent batch has the producer ID -1.
type Key = (Topition, i64);

/// The writes that the broker left running when it answered their request at its deadline, for
/// each partition and producer, with how many such writes each has.
///
/// An idempotent produce from a producer to a partition where that producer has such a write is
/// not started. The storage engine answers the retry of an idempotent batch as a duplicate only
/// after the first write advanced the producer's sequence, so a retry that starts while the first
/// write runs is stored a second time, and a later batch from the producer could be stored ahead
/// of the one that still runs. Another producer, and a non-idempotent batch, have no sequence to
/// protect and are not held. A client chooses its `timeout_ms`, and it must not hold the
/// partition for the others with it. The set is the process's, so it covers the requests that
/// this process serves, on any connection, and not a retry that reaches another node.
#[derive(Clone, Debug, Default)]
pub(super) struct Detached {
    writes: Arc<Mutex<HashMap<Key, usize>>>,
    /// The task of each write that has started and not ended, whether or not its request still
    /// waits for it.
    tasks: TaskTracker,
}

/// The produce writes that still run when [`wait_for_detached_writes`] ends.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StillRunning {
    /// How many writes still run, whether or not their request still waits for them.
    pub writes: usize,
    /// The partitions with a write whose request already answered at its deadline. A write whose
    /// request was aborted by the shutdown, or that started during the wait, is in `writes` and
    /// not here, so this can name fewer partitions than `writes` counts, or none.
    pub detached_partitions: Vec<Topition>,
}

/// Waits until every produce write that runs in a task of its own has ended, for at most
/// `remaining_grace`. Returns the writes that still run when the wait ends, with zero `writes`
/// when all of them ended in time.
///
/// A write runs in a task of its own so that its request can answer at its deadline, and the
/// task keeps running when its request has answered or its connection has closed. A write that
/// starts during the wait is waited for as well, within the same `remaining_grace`. A write that
/// still runs when the wait ends is left running. The writes are tracked for the whole process,
/// so the wait covers the writes of every broker in it.
pub async fn wait_for_detached_writes(remaining_grace: Duration) -> StillRunning {
    let tasks = &PROCESS.tasks;

    // The tracker completes its wait only once it is closed. A closed tracker still accepts a
    // task, so a write that starts after this point is tracked too.
    _ = tasks.close();

    match timeout(remaining_grace, tasks.wait()).await {
        Ok(()) => StillRunning::default(),
        Err(_) => StillRunning {
            writes: tasks.len(),
            detached_partitions: PROCESS.partitions(),
        },
    }
}

impl Detached {
    /// Returns the set that all services of this process share.
    pub(super) fn process() -> Self {
        PROCESS.clone()
    }

    /// Runs `write` in a task of its own, and tracks the task until it ends, also when it panics.
    pub(super) fn spawn<F>(&self, write: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        _ = self.tasks.spawn(write);
    }

    /// Returns whether a write that the deadline left running is still in flight for `topition`
    /// from `producer_id`. A non-idempotent batch (`producer_id` below 0) is never held.
    pub(super) fn contains(&self, topition: &Topition, producer_id: i64) -> bool {
        producer_id >= 0
            && self
                .writes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains_key(&(topition.clone(), producer_id))
    }

    /// Returns the partitions that have a write which its request no longer waits for, in order.
    fn partitions(&self) -> Vec<Topition> {
        self.writes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .map(|(topition, _)| topition.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Returns the tracker of a write to `topition` from `producer_id`, which starts as a write
    /// that its request still waits for.
    pub(super) fn track(&self, topition: Topition, producer_id: i64) -> Write {
        Write {
            detached: self.clone(),
            key: (topition, producer_id),
            state: Mutex::new(State::Running),
        }
    }

    fn add(&self, key: &Key) {
        *self
            .writes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(key.clone())
            .or_default() += 1;

        DETACHED_WRITES.add(1, &[]);
    }

    fn remove(&self, key: &Key) {
        let mut writes = self.writes.lock().unwrap_or_else(PoisonError::into_inner);

        if let Some(count) = writes.get_mut(key) {
            *count -= 1;

            if *count == 0 {
                _ = writes.remove(key);
            }
        }

        DETACHED_WRITES.add(-1, &[]);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    /// The request that started the write still waits for it.
    Running,
    /// The request answered, and the write runs on.
    Detached,
    /// The write ended.
    Finished,
}

/// One write to a partition, from its start to its end. It lets the request that started the
/// write detach from it, and the write's own task mark its end, in either order.
#[derive(Debug)]
pub(super) struct Write {
    detached: Detached,
    key: Key,
    state: Mutex<State>,
}

impl Write {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Leaves the write running without its request. Returns `false` when the write ended
    /// already, so that nothing runs on.
    pub(super) fn detach(&self) -> bool {
        let mut state = self.state();

        if *state != State::Running {
            return false;
        }

        *state = State::Detached;
        self.detached.add(&self.key);

        true
    }

    /// Returns whether the write runs on without its request.
    pub(super) fn is_detached(&self) -> bool {
        *self.state() == State::Detached
    }

    /// Marks the write as ended, and takes it out of the set if it was detached. The task of the
    /// write calls this when it is dropped, also when the write panics.
    pub(super) fn finish(&self) {
        let mut state = self.state();

        if *state == State::Detached {
            self.detached.remove(&self.key);
        }

        *state = State::Finished;
    }
}
