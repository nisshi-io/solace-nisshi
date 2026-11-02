// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

//! What the broker reports when a storage sweep fails.
//!
//! A sweep that fails stops retention, compaction, the lake sink or the transaction timeout from
//! running, and the broker runs it again on its next tick, so a persistent fault fails every tick.
//! The log carries the first failure and then one line per [`REPEAT_INTERVAL`], with the failures
//! it left out, and the metric counts every one.

use std::{
    fmt::Display,
    sync::{LazyLock, Mutex, PoisonError},
    time::{Duration, Instant},
};

use opentelemetry::{KeyValue, metrics::Counter};
use tracing::warn;

use crate::METER;

/// The least time between two log lines for failures of one sweep.
const REPEAT_INTERVAL: Duration = Duration::from_mins(5);

/// Counts every finished sweep, by outcome, so that an alert can fire on a sweep that stopped
/// succeeding as well as on one that fails: a counter that only counts failures exports no series
/// for a healthy broker, and none for a sweep that never finishes.
static SWEEPS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_storage_maintenance_sweeps")
        .with_description("storage maintenance sweeps that finished, by outcome")
        .build()
});

#[derive(Debug, Default)]
struct State {
    /// Failed sweeps since the last success.
    consecutive: u64,

    /// Failures since the last log line.
    unlogged: u64,

    last_logged: Option<Instant>,
}

/// The failures of one kind of sweep (`maintain` or `maintain_transactions`) of one engine.
#[derive(Debug)]
pub(crate) struct SweepFailures {
    sweep: &'static str,
    engine: String,
    state: Mutex<State>,
}

impl SweepFailures {
    pub(crate) fn new(sweep: &'static str, engine: impl Into<String>) -> Self {
        Self {
            sweep,
            engine: engine.into(),
            state: Mutex::new(State::default()),
        }
    }

    /// Records the outcome of a sweep that ran at `now`.
    pub(crate) fn record<E: Display>(&self, outcome: Result<(), E>, now: Instant) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);

        SWEEPS.add(
            1,
            &[
                KeyValue::new("sweep", self.sweep),
                KeyValue::new("engine", self.engine.clone()),
                KeyValue::new("outcome", if outcome.is_ok() { "ok" } else { "error" }),
            ],
        );

        match outcome {
            Ok(()) => {
                if state.consecutive > 0 {
                    // At warn, the level of the failure, so that an operator at the default level sees
                    // it end. It fires once per run of failures.
                    warn!(
                        sweep = self.sweep,
                        engine = %self.engine,
                        failed_sweeps = state.consecutive,
                        "storage maintenance sweep succeeded again"
                    );
                }

                *state = State::default();
            }

            Err(error) => {
                state.consecutive += 1;
                state.unlogged += 1;

                if state
                    .last_logged
                    .is_some_and(|logged| now.saturating_duration_since(logged) < REPEAT_INTERVAL)
                {
                    return;
                }

                warn!(
                    sweep = self.sweep,
                    engine = %self.engine,
                    consecutive_failures = state.consecutive,
                    failures_since_last_log = state.unlogged,
                    %error,
                    "storage maintenance sweep failed: retention, compaction, the lake sink or \
                     the transaction timeout may have stopped"
                );

                state.unlogged = 0;
                state.last_logged = Some(now);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{Arc, Mutex},
    };

    use tracing::Level;

    use super::*;

    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Capture {
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap_or_else(PoisonError::into_inner))
                .into_owned()
        }
    }

    /// Runs `body` with a subscriber at WARN, the broker's default level.
    fn captured(body: impl FnOnce()) -> String {
        captured_at(Level::WARN, body)
    }

    fn captured_at(level: Level, body: impl FnOnce()) -> String {
        let capture = Capture::default();
        let writer = capture.clone();

        tracing::subscriber::with_default(
            tracing_subscriber::fmt()
                .with_max_level(level)
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .finish(),
            body,
        );

        capture.text()
    }

    #[test]
    fn a_failed_sweep_is_logged_at_warn_with_its_engine() {
        let failures = SweepFailures::new("maintain", "postgres");

        let logs = captured(|| failures.record(Err("connection refused"), Instant::now()));

        assert!(logs.contains("WARN"), "{logs}");
        assert!(
            logs.contains("sweep=\"maintain\"") || logs.contains("sweep=maintain"),
            "{logs}"
        );
        assert!(logs.contains("engine=postgres"), "{logs}");
        assert!(logs.contains("connection refused"), "{logs}");
    }

    #[test]
    fn a_persistent_failure_is_logged_once_per_interval_with_the_count_it_left_out() {
        let failures = SweepFailures::new("maintain_transactions", "sqlite");
        let start = Instant::now();

        let logs = captured(|| {
            for tick in 0..30 {
                failures.record(Err("disk full"), start + Duration::from_secs(10 * tick));
            }
        });

        // 30 ticks over 290 seconds: only the first is logged.
        assert_eq!(1, logs.matches("disk full").count(), "{logs}");

        let logs = captured(|| failures.record(Err("disk full"), start + REPEAT_INTERVAL));

        assert_eq!(1, logs.matches("disk full").count(), "{logs}");
        assert!(logs.contains("consecutive_failures=31"), "{logs}");
        assert!(logs.contains("failures_since_last_log=30"), "{logs}");
    }

    #[test]
    fn a_success_after_failures_is_logged_and_the_next_failure_logs_at_once() {
        let failures = SweepFailures::new("maintain", "memory");
        let start = Instant::now();

        _ = captured(|| failures.record(Err("boom"), start));

        let logs = captured(|| failures.record(Ok::<(), &str>(()), start));
        assert!(logs.contains("succeeded again"), "{logs}");
        assert!(logs.contains("failed_sweeps=1"), "{logs}");

        let logs = captured(|| failures.record(Err("boom"), start));
        assert_eq!(1, logs.matches("boom").count(), "{logs}");

        let logs = captured(|| failures.record(Ok::<(), &str>(()), start));
        assert!(logs.contains("succeeded again"), "{logs}");

        let logs = captured(|| failures.record(Ok::<(), &str>(()), start));
        assert!(logs.is_empty(), "{logs}");
    }
}
