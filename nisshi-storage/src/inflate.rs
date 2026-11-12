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

//! Run CPU-bound batch decoding off the async runtime's worker threads.
//!
//! Inflating a produced batch decompresses and parses every record in it.
//! For a large compressed batch that takes long enough that running it on a
//! runtime worker thread stalls every other connection scheduled on that
//! worker. [`offload`] moves the work onto Tokio's blocking thread pool, and
//! [`inflate`] does so for the common case of inflating one batch.

use std::{
    sync::{Arc, LazyLock, OnceLock},
    time::Instant,
};

use nisshi_sans_io::record::{deflated, inflated};
use opentelemetry::metrics::Histogram;
use tokio::{runtime::Handle, sync::Semaphore, task};

use crate::{METER, Result, inflate_produced};

const DURATION_BOUNDARIES_MS: [f64; 15] = [
    0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0,
];

static OFFLOAD_PERMIT_ACQUIRE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_storage_offload_permit_acquire_duration")
        .with_boundaries(DURATION_BOUNDARIES_MS.into())
        .with_unit("ms")
        .with_description("Time an offloaded decode waits for a permit in ms")
        .build()
});

static OFFLOAD_RUN_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_storage_offload_run_duration")
        .with_boundaries(DURATION_BOUNDARIES_MS.into())
        .with_unit("ms")
        .with_description("Time an offloaded decode runs on the blocking pool in ms")
        .build()
});

/// Bounds how many offloaded decodes run at once, to at most one per runtime
/// worker thread.
///
/// The blocking pool allows up to 512 threads by default, so without this
/// bound a burst of large produce requests would start one decode thread
/// each.
///
/// The semaphore is created once per process, sized from the runtime that
/// first calls [`offload`]. The broker runs a single runtime, so that is its
/// worker count, including any `TOKIO_WORKER_THREADS` override.
static PERMITS: OnceLock<Arc<Semaphore>> = OnceLock::new();

fn permits() -> Arc<Semaphore> {
    PERMITS
        .get_or_init(|| {
            Arc::new(Semaphore::new(
                Handle::current().metrics().num_workers().max(1),
            ))
        })
        .clone()
}

/// Run `f` on Tokio's blocking thread pool, at most one call per runtime
/// worker thread at a time.
///
/// Use this for CPU-bound work, such as decoding a record batch, that would
/// otherwise block a runtime worker thread. A panic in `f` is returned as
/// [`crate::Error::Join`].
///
/// Outside a Tokio runtime, such as under a model checker that drives a
/// service with its own executor, `f` runs inline: there is no worker thread
/// to protect, and no blocking pool to run on.
pub async fn offload<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    if Handle::try_current().is_err() {
        return f();
    }

    offload_with(permits(), f).await
}

async fn offload_with<F, T>(permits: Arc<Semaphore>, f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let waiting = Instant::now();
    let permit = permits.acquire_owned().await?;
    OFFLOAD_PERMIT_ACQUIRE_DURATION.record(elapsed_millis(waiting), &[]);

    task::spawn_blocking(move || {
        let _permit = permit;
        let running = Instant::now();
        let outcome = f();
        OFFLOAD_RUN_DURATION.record(elapsed_millis(running), &[]);
        outcome
    })
    .await?
}

fn elapsed_millis(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Inflate a produced batch on Tokio's blocking thread pool, as [`offload`]
/// does, with the error mapping of [`inflate_produced`].
pub async fn inflate(deflated: deflated::Batch) -> Result<inflated::Batch> {
    offload(move || inflate_produced(deflated)).await
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        thread,
        time::Duration,
    };

    use tokio::task::{JoinSet, yield_now};

    use super::*;
    use crate::Error;

    /// How long the offloaded closure waits for the signal before giving up.
    /// Only reached when the closure runs inline on the runtime thread, where
    /// the signal can never arrive while it waits.
    const SIGNAL_TIMEOUT: Duration = Duration::from_secs(3);

    // The test runtime has a single thread. The offloaded closure waits for
    // a signal that only this task, on that thread, can send. If `offload`
    // ran the closure inline, it would block the only runtime thread, the
    // signal would never be sent while it waited, and it would time out.
    #[tokio::test]
    async fn offload_runs_off_the_runtime_thread() -> Result<()> {
        let (tx, rx) = mpsc::channel::<()>();

        let task = tokio::spawn(offload_with(Arc::new(Semaphore::new(1)), move || {
            Ok(rx.recv_timeout(SIGNAL_TIMEOUT).is_ok())
        }));

        yield_now().await;
        _ = tx.send(());

        assert!(
            task.await??,
            "the offloaded closure blocked the runtime thread"
        );

        Ok(())
    }

    #[tokio::test]
    async fn offload_returns_a_panic_as_join_error() {
        let outcome =
            offload_with::<_, ()>(Arc::new(Semaphore::new(1)), || panic!("decode panicked")).await;

        assert!(matches!(outcome, Err(Error::Join(_))), "{outcome:?}");
    }

    // A plain test has no Tokio runtime, so `offload` runs the closure on
    // the calling thread instead of panicking in `spawn_blocking`.
    #[test]
    fn offload_runs_inline_without_a_runtime() -> Result<()> {
        let ran_on = futures::executor::block_on(offload(|| Ok(thread::current().id())))?;

        assert_eq!(thread::current().id(), ran_on);

        Ok(())
    }

    // Each closure holds its permit long enough for every other spawned
    // closure to start, if a permit were free for it.
    #[tokio::test]
    async fn offload_runs_at_most_one_closure_per_permit() -> Result<()> {
        const PERMITS: usize = 2;
        const HOLD: Duration = Duration::from_millis(100);

        let permits = Arc::new(Semaphore::new(PERMITS));
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let mut closures = JoinSet::new();

        for _ in 0..=PERMITS {
            let running = running.clone();
            let peak = peak.clone();

            _ = closures.spawn(offload_with(permits.clone(), move || {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                _ = peak.fetch_max(now, Ordering::SeqCst);
                thread::sleep(HOLD);
                _ = running.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }));
        }

        while let Some(outcome) = closures.join_next().await {
            outcome??;
        }

        assert_eq!(PERMITS, peak.load(Ordering::SeqCst));

        Ok(())
    }
}
