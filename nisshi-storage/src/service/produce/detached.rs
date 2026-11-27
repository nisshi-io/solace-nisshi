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
    mem,
    sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use opentelemetry::metrics::UpDownCounter;
use tokio::{
    sync::oneshot,
    time::{Instant, timeout, timeout_at},
};
use tokio_util::task::TaskTracker;
use tracing::{Instrument as _, Span};

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
static PROCESS: LazyLock<Detached> = LazyLock::new(Detached::new);

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
#[derive(Clone, Debug)]
pub struct Detached {
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
    PROCESS.wait(remaining_grace).await
}

impl Detached {
    /// Waits until every write of this set has ended, for at most `remaining_grace`, as
    /// [`wait_for_detached_writes`] does for the set of the process.
    pub(super) async fn wait(&self, remaining_grace: Duration) -> StillRunning {
        // The tracker completes its wait only once it is closed. A closed tracker still accepts a
        // task, so a write that starts after this point is tracked too.
        _ = self.tasks.close();

        match timeout(remaining_grace, self.tasks.wait()).await {
            Ok(()) => StillRunning::default(),
            Err(_) => StillRunning {
                writes: self.tasks.len(),
                detached_partitions: self.partitions(),
            },
        }
    }

    /// Returns an empty set of its own. [`wait_for_detached_writes`] waits only for the set of the
    /// process, so code outside the crate gets a set through [`Self::process`].
    pub(crate) fn new() -> Self {
        Self {
            writes: Arc::default(),
            tasks: TaskTracker::new(),
        }
    }

    /// Returns the set that all services of this process share.
    pub fn process() -> Self {
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
    pub fn contains(&self, topition: &Topition, producer_id: i64) -> bool {
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

/// What a caller of [`Detached::write_until`] learns of its write.
#[derive(Debug)]
pub enum WriteOutcome<T> {
    /// The write ended while the caller still waited for it, with its output.
    Ended(T),
    /// The deadline passed first, and the write runs on in its own task.
    Detached,
    /// The write panicked while the caller still waited for it.
    Panicked,
}

impl Detached {
    /// Runs `write` in a task of its own, which the broker's shutdown waits for, and waits for
    /// its output until `at`.
    ///
    /// The write covers `partitions`, each with the producer of its batch. If `at` passes first,
    /// the write runs on, and [`Self::contains`] reports each of those partitions until it
    /// ends, so that a producer's next write to one of them can wait for it.
    ///
    /// The output goes to one place only. If the caller still waits when the write ends, it gets
    /// the output in [`WriteOutcome::Ended`]. Otherwise no request reports the output, and the
    /// task passes it to `on_detached`.
    ///
    /// This method does not check [`Self::contains`]. A caller checks it for each of its
    /// partitions first, so that a retry does not start while an earlier write still runs.
    ///
    /// If the returned future is dropped before `at`, the write runs on and the broker's shutdown
    /// waits for it, but [`Self::contains`] does not report its partitions.
    ///
    /// The task, and `on_detached` with it, run in the span that is current when this method is
    /// called.
    pub async fn write_until<F, D>(
        &self,
        at: Instant,
        partitions: impl IntoIterator<Item = (Topition, i64)>,
        write: F,
        on_detached: D,
    ) -> WriteOutcome<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
        D: FnOnce(F::Output) + Send + 'static,
    {
        let task = Arc::new(Task {
            detached: self.clone(),
            keys: partitions.into_iter().collect(),
            state: Mutex::new(State::Running),
        });
        let (sender, mut receiver) = oneshot::channel();
        let end = End(Arc::clone(&task));

        self.spawn(
            async move {
                let output = write.await;

                let unclaimed = if end.0.finish() {
                    match sender.send(output) {
                        Ok(()) => return,
                        // The caller dropped the future that waited for the output.
                        Err(output) => output,
                    }
                } else {
                    output
                };

                on_detached(unclaimed);
            }
            .instrument(Span::current()),
        );

        match timeout_at(at, &mut receiver).await {
            Ok(output) => output.map_or(WriteOutcome::Panicked, WriteOutcome::Ended),
            Err(_) if task.detach() => WriteOutcome::Detached,

            // The task ended first, so its output is the caller's. The task sends it after it
            // releases the lock, so it can still be on its way. A task that panicked sends none.
            Err(_) => receiver
                .await
                .map_or(WriteOutcome::Panicked, WriteOutcome::Ended),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    /// The caller still waits for the write.
    Running,
    /// The caller stopped waiting, and the write runs on.
    Detached,
    /// The write ended.
    Finished,
}

/// One call to [`Detached::write_until`], which its caller and its task share.
///
/// One lock covers every partition of the write, so that the caller detaches the write from all
/// of them or from none. Whichever side takes the lock first decides where the output goes: the
/// task that finishes first gives it to the caller, and the caller that detaches first leaves it
/// to the task. Neither side does more than that under the lock.
struct Task {
    detached: Detached,
    keys: Vec<Key>,
    state: Mutex<State>,
}

impl Task {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Leaves the write running without its caller, and adds its partitions to the set. Returns
    /// `false` when the write ended already, so that its output is the caller's.
    fn detach(&self) -> bool {
        let mut state = self.state();

        if *state != State::Running {
            return false;
        }

        *state = State::Detached;

        for key in &self.keys {
            self.detached.add(key);
        }

        true
    }

    /// Marks the write as ended, and takes its partitions out of the set if it was detached.
    /// Returns whether the caller still waited for the write, so that the output is the
    /// caller's. A second call changes nothing and returns `false`.
    fn finish(&self) -> bool {
        let mut state = self.state();
        let previous = mem::replace(&mut *state, State::Finished);

        if previous == State::Detached {
            for key in &self.keys {
                self.detached.remove(key);
            }
        }

        previous == State::Running
    }
}

/// Ends the write of a task when the task ends, also when the write panics.
struct End(Arc<Task>);

impl Drop for End {
    fn drop(&mut self) {
        _ = self.0.finish();
    }
}
