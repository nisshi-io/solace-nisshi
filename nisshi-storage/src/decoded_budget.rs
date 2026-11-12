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

//! A byte budget for the decoded batches that Produce requests hold while they wait for storage.
//!
//! [`ProduceService`] decodes every client batch, and then hands the decoded batch to the storage
//! engine, which may wait for a pooled connection or a write lock before it uses it. The decode
//! permit of [`offload`] limits how many decodes run at once, but it is released when the decode
//! returns, so nothing else limits how many decoded batches wait. A batch of a few KB of
//! compressed records decodes to up to [`MAX_DECODED_BATCH_BYTES`], so a client that sends such
//! batches on many connections makes the broker hold that many decoded batches.
//!
//! The budget bounds the bytes of decoded batches that Produce requests hold at once. A request
//! reserves bytes before it decodes its batch, and holds them until storage returns, so a request
//! that cannot get its bytes waits holding only its compressed batch.
//!
//! The budget counts what the decoder charges to a batch (see
//! [`nisshi_sans_io::record::inflated::Batch::decoded_size`]), which is a lower bound of the
//! memory that a decode takes at its peak. The zstd window and spare buffer capacity are not
//! charged, so the memory of the decoded batches is at most the budget plus what the concurrent
//! decodes take above their charge.
//!
//! [`ProduceService`]: crate::ProduceService
//! [`offload`]: crate::offload
//! [`MAX_DECODED_BATCH_BYTES`]: nisshi_sans_io::record::MAX_DECODED_BATCH_BYTES

use std::{
    sync::{
        Arc, LazyLock, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use nisshi_sans_io::record::MAX_DECODED_BATCH_BYTES;
use opentelemetry::metrics::{Counter, Histogram, UpDownCounter};
use tokio::{
    runtime::Handle,
    sync::Semaphore,
    time::{Instant, timeout_at},
};

use crate::METER;

/// The budget is counted in units of this many bytes, so that it fits the `u32` that a
/// semaphore takes for one acquisition and still reaches terabytes.
const UNIT: usize = 1024;

/// The smallest budget that the default applies to: 1 GiB, ten maximum-size decoded batches.
const DEFAULT_MINIMUM_BYTES: usize = 1024 * 1024 * 1024;

/// How long a Produce request waits for its bytes, in total, before its remaining partitions
/// answer `REQUEST_TIMED_OUT`. It matches the 10 s that a request waits for a pooled connection.
pub(crate) const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

static IN_USE: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    METER
        .i64_up_down_counter("nisshi_produce_decoded_bytes_in_use")
        .with_unit("By")
        .with_description(
            "Bytes of the decoded-batch budget that Produce requests hold, from the reservation \
             before a decode until storage returns",
        )
        .build()
});

static WAIT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_produce_decoded_budget_wait_duration")
        .with_unit("ms")
        .with_description("Time a Produce request waits for the decoded-batch budget in ms")
        .build()
});

static EXHAUSTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_produce_decoded_budget_exhausted")
        .with_description(
            "Produce partitions answered REQUEST_TIMED_OUT because the decoded-batch budget \
             stayed spent",
        )
        .build()
});

/// A request that waited until its deadline without getting its bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Exhausted {
    /// The bytes that the request asked for.
    pub(crate) requested: usize,
    /// How long it waited.
    pub(crate) waited: Duration,
}

/// A semaphore sized in decoded bytes. See the [module documentation](self).
#[derive(Debug)]
pub(crate) struct DecodedBytesBudget {
    inner: Arc<Inner>,
    units: u32,
}

#[derive(Debug)]
struct Inner {
    semaphore: Semaphore,
    /// The units that reservations hold. A waiter for a large reservation takes the free units
    /// of the semaphore as they come back, so the semaphore's own count does not say this.
    held: AtomicUsize,
}

impl DecodedBytesBudget {
    /// A budget of `bytes`, rounded up to a whole unit, and at least one unit.
    pub(crate) fn new(bytes: usize) -> Self {
        let units = u32::try_from(bytes.div_ceil(UNIT).max(1)).unwrap_or(u32::MAX);

        Self {
            inner: Arc::new(Inner {
                semaphore: Semaphore::new(units as usize),
                held: AtomicUsize::new(0),
            }),
            units,
        }
    }

    #[cfg(test)]
    pub(crate) fn total_bytes(&self) -> usize {
        self.units as usize * UNIT
    }

    /// The bytes that reservations hold now.
    #[cfg(test)]
    pub(crate) fn in_use_bytes(&self) -> usize {
        self.inner.held.load(Ordering::SeqCst) * UNIT
    }

    /// Wait until `bytes` are free, or `deadline` passes.
    ///
    /// A request for more than the whole budget gets the whole budget, and so runs alone: it
    /// cannot get more, and failing it would fail a batch that the decoder accepts. Such a
    /// request waits for every other reservation to end, and the requests behind it wait for it.
    ///
    /// Outside a Tokio runtime, such as under a model checker that drives a service with its own
    /// executor, there is no timer to wait on, and the reservation is empty, as [`offload`] runs
    /// inline there.
    ///
    /// [`offload`]: crate::offload
    pub(crate) async fn reserve(
        &self,
        bytes: usize,
        deadline: Instant,
    ) -> Result<Reservation, Exhausted> {
        if Handle::try_current().is_err() {
            return Ok(Reservation {
                inner: Arc::clone(&self.inner),
                units: 0,
            });
        }

        let units = u32::try_from(bytes.div_ceil(UNIT).max(1))
            .unwrap_or(u32::MAX)
            .min(self.units);
        let waiting = Instant::now();

        let acquired = timeout_at(deadline, self.inner.semaphore.acquire_many(units)).await;
        let waited = waiting.elapsed();
        WAIT_DURATION.record(u64::try_from(waited.as_millis()).unwrap_or(u64::MAX), &[]);

        match acquired {
            Ok(Ok(permit)) => {
                // The reservation returns the units on drop, so that it can return some of them
                // early.
                permit.forget();
                _ = self.inner.held.fetch_add(units as usize, Ordering::SeqCst);
                IN_USE.add(i64::from(units) * UNIT as i64, &[]);

                Ok(Reservation {
                    inner: Arc::clone(&self.inner),
                    units,
                })
            }

            // The semaphore is never closed, so an error here is a timeout in every case.
            Ok(Err(_)) | Err(_) => {
                EXHAUSTED.add(1, &[]);
                Err(Exhausted {
                    requested: bytes,
                    waited,
                })
            }
        }
    }
}

/// Bytes of a [`DecodedBytesBudget`] held by one Produce partition, returned when it is dropped.
#[derive(Debug)]
pub(crate) struct Reservation {
    inner: Arc<Inner>,
    units: u32,
}

impl Reservation {
    /// The bytes that this reservation holds.
    pub(crate) fn bytes(&self) -> usize {
        self.units as usize * UNIT
    }

    /// Keep `bytes` of the reservation and return the rest, once the decoded size is known. Does
    /// nothing when `bytes` is not smaller than what the reservation holds.
    pub(crate) fn shrink_to(&mut self, bytes: usize) {
        let keep = u32::try_from(bytes.div_ceil(UNIT).max(1)).unwrap_or(u32::MAX);

        if self.units > keep {
            let excess = self.units - keep;
            self.units = keep;
            self.give_back(excess);
        }
    }
}

impl Reservation {
    fn give_back(&self, units: u32) {
        if units > 0 {
            _ = self.inner.held.fetch_sub(units as usize, Ordering::SeqCst);
            self.inner.semaphore.add_permits(units as usize);
            IN_USE.add(-(i64::from(units) * UNIT as i64), &[]);
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.give_back(self.units);
    }
}

static PRODUCE: OnceLock<Arc<DecodedBytesBudget>> = OnceLock::new();

/// Set the process-wide budget of decoded Produce batches to `bytes`. Returns `false` and keeps
/// the budget that is already in place when a Produce request, or an earlier call, fixed it.
///
/// Call this once, at startup, before the broker serves a request. Without a call the budget is
/// [`default_bytes`].
pub fn configure_produce_decoded_bytes(bytes: usize) -> bool {
    PRODUCE
        .set(Arc::new(DecodedBytesBudget::new(bytes)))
        .is_ok()
}

/// The default budget: 1 GiB, or one maximum-size decoded batch for each runtime worker when
/// that is more, so that the budget does not make fewer decodes run at once than the decode
/// permit of [`offload`](crate::offload) allows.
pub fn default_bytes() -> usize {
    let workers = Handle::try_current()
        .map(|handle| handle.metrics().num_workers())
        .unwrap_or(1)
        .max(1);

    DEFAULT_MINIMUM_BYTES.max(workers.saturating_mul(MAX_DECODED_BATCH_BYTES))
}

/// The process-wide budget of decoded Produce batches.
pub(crate) fn produce() -> Arc<DecodedBytesBudget> {
    Arc::clone(PRODUCE.get_or_init(|| Arc::new(DecodedBytesBudget::new(default_bytes()))))
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use tokio::task::yield_now;

    use super::*;

    fn soon() -> Instant {
        Instant::now() + Duration::from_secs(60)
    }

    #[tokio::test]
    async fn reservations_never_hold_more_than_the_budget() {
        let budget = DecodedBytesBudget::new(10 * UNIT);

        let first = budget.reserve(6 * UNIT, soon()).await.unwrap();
        let second = budget.reserve(4 * UNIT, soon()).await.unwrap();
        assert_eq!(10 * UNIT, budget.in_use_bytes());

        drop(first);
        assert_eq!(4 * UNIT, budget.in_use_bytes());
        drop(second);
        assert_eq!(0, budget.in_use_bytes());
    }

    #[tokio::test]
    async fn a_request_over_the_budget_waits_for_bytes_to_come_back() {
        let budget = Arc::new(DecodedBytesBudget::new(10 * UNIT));
        let held = budget.reserve(8 * UNIT, soon()).await.unwrap();

        let waiter = tokio::spawn({
            let budget = Arc::clone(&budget);
            async move { budget.reserve(4 * UNIT, soon()).await.map(|r| r.bytes()) }
        });

        yield_now().await;
        assert!(
            !waiter.is_finished(),
            "the waiter took bytes that were not free"
        );
        assert_eq!(8 * UNIT, budget.in_use_bytes());

        drop(held);
        assert_eq!(4 * UNIT, waiter.await.unwrap().unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_that_waits_past_its_deadline_is_exhausted() {
        let budget = DecodedBytesBudget::new(10 * UNIT);
        let _held = budget.reserve(10 * UNIT, soon()).await.unwrap();

        let outcome = budget
            .reserve(UNIT, Instant::now() + Duration::from_secs(3))
            .await;

        assert_matches!(
            outcome,
            Err(Exhausted { requested, waited }) if requested == UNIT && waited >= Duration::from_secs(3)
        );
        assert_eq!(10 * UNIT, budget.in_use_bytes());
    }

    #[tokio::test]
    async fn shrinking_returns_the_excess_and_never_grows() {
        let budget = DecodedBytesBudget::new(10 * UNIT);
        let mut reservation = budget.reserve(10 * UNIT, soon()).await.unwrap();

        reservation.shrink_to(3 * UNIT);
        assert_eq!(3 * UNIT, reservation.bytes());
        assert_eq!(3 * UNIT, budget.in_use_bytes());

        reservation.shrink_to(9 * UNIT);
        assert_eq!(3 * UNIT, reservation.bytes());

        drop(reservation);
        assert_eq!(0, budget.in_use_bytes());
    }

    #[tokio::test]
    async fn a_request_over_the_whole_budget_gets_the_whole_budget() {
        let budget = DecodedBytesBudget::new(10 * UNIT);

        let reservation = budget.reserve(1000 * UNIT, soon()).await.unwrap();

        assert_eq!(budget.total_bytes(), reservation.bytes());
    }

    #[test]
    fn the_default_is_at_least_a_gibibyte_and_a_batch_for_each_worker() {
        assert_eq!(1024 * 1024 * 1024, default_bytes());
    }
}
