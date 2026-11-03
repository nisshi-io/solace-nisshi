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
//! A sweep that fails leaves its work undone until the broker runs it again on its next tick, so a
//! persistent fault fails every tick. The log carries the first failure and then at most one line
//! per [`REPEAT_INTERVAL`], for failures and recoveries together, and the metric counts every
//! sweep.
//!
//! A recovery always gets its line. When a sweep succeeds before a line is due, the broker keeps
//! the run of failures open and logs the recovery on the first success after the line is due. If
//! failures were left out of the log since its last line, the recovery line carries the latest of
//! them and what the sweep stops, so the newest error text in the log is not older than the last
//! failure.

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
    /// Failed sweeps since the last recovery line. A success that comes before a line is due does
    /// not end the run, so this count can include sweeps that succeeded in between.
    failed_sweeps: u64,

    /// Failures since the last log line.
    unlogged: u64,

    /// The latest failure that no line has carried. `None` when `unlogged` is 0.
    unlogged_error: Option<String>,

    last_logged: Option<Instant>,
}

impl State {
    fn log_is_due(&self, now: Instant) -> bool {
        self.last_logged
            .is_none_or(|logged| now.saturating_duration_since(logged) >= REPEAT_INTERVAL)
    }
}

/// The failures of one kind of sweep (`maintain` or `maintain_transactions`) of one engine.
#[derive(Debug)]
pub(crate) struct SweepFailures {
    sweep: &'static str,

    /// What an operator can expect to stop working while this sweep fails.
    stopped: &'static str,

    engine: String,
    counter: Counter<u64>,
    state: Mutex<State>,
}

impl SweepFailures {
    /// The failures of `maintain`, which runs retention, compaction and the lake sink.
    pub(crate) fn maintain(engine: impl Into<String>) -> Self {
        Self::new(
            "maintain",
            "retention, compaction and the lake sink may have stopped",
            engine,
        )
    }

    /// The failures of `maintain_transactions`, which aborts the transactions that timed out.
    pub(crate) fn maintain_transactions(engine: impl Into<String>) -> Self {
        Self::new(
            "maintain_transactions",
            "timed-out transactions are not aborted, so read_committed consumers may stall",
            engine,
        )
    }

    fn new(sweep: &'static str, stopped: &'static str, engine: impl Into<String>) -> Self {
        Self {
            sweep,
            stopped,
            engine: engine.into(),
            counter: SWEEPS.clone(),
            state: Mutex::new(State::default()),
        }
    }

    /// Records the outcome of a sweep that ran at `now`.
    pub(crate) fn record<E: Display>(&self, outcome: Result<(), E>, now: Instant) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);

        self.counter.add(
            1,
            &[
                KeyValue::new("sweep", self.sweep),
                KeyValue::new("engine", self.engine.clone()),
                KeyValue::new("outcome", if outcome.is_ok() { "ok" } else { "error" }),
            ],
        );

        match outcome {
            Ok(()) => {
                // The recovery line counts against the same limit as the failure lines, because a
                // sweep that alternates between failing and succeeding would otherwise log two
                // lines per cycle. The run of failures stays open until the line is written, so
                // the recovery is not lost and a failure that comes in the meantime continues the
                // run.
                if state.failed_sweeps == 0 || !state.log_is_due(now) {
                    return;
                }

                // At warn, the level of the failure, so that an operator at the default level
                // sees it end. A sweep that flaps can reach here with failures that no line
                // carried, and then the line says what the latest of them was and what the sweep
                // stops, because the failure line before it may be hours old.
                warn!(
                    sweep = self.sweep,
                    engine = %self.engine,
                    failed_sweeps = state.failed_sweeps,
                    failures_since_last_log = state.unlogged,
                    stopped = (state.unlogged > 0).then_some(self.stopped),
                    error = state.unlogged_error.as_deref(),
                    "storage maintenance sweep succeeded again"
                );

                state.failed_sweeps = 0;
                state.unlogged = 0;
                state.unlogged_error = None;
                state.last_logged = Some(now);
            }

            Err(error) => {
                state.failed_sweeps += 1;
                state.unlogged += 1;

                if !state.log_is_due(now) {
                    state.unlogged_error = Some(error.to_string());
                    return;
                }

                warn!(
                    sweep = self.sweep,
                    engine = %self.engine,
                    failed_sweeps = state.failed_sweeps,
                    failures_since_last_log = state.unlogged,
                    stopped = self.stopped,
                    %error,
                    "storage maintenance sweep failed"
                );

                state.unlogged = 0;
                state.unlogged_error = None;
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

    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::{
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        data::{AggregatedMetrics, MetricData, ResourceMetrics, ScopeMetrics, SumDataPoint},
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
        let failures = SweepFailures::maintain("postgres");

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
    fn each_sweep_names_the_work_that_its_failure_leaves_undone() {
        let logs = captured(|| {
            SweepFailures::maintain("memory").record(Err("boom"), Instant::now());
            SweepFailures::maintain_transactions("postgres").record(Err("boom"), Instant::now());
        });

        let mut lines = logs.lines();

        let maintain = lines.next().expect("a line for maintain");
        assert!(maintain.contains("retention, compaction"), "{maintain}");
        assert!(!maintain.contains("read_committed"), "{maintain}");

        let transactions = lines.next().expect("a line for maintain_transactions");
        assert!(transactions.contains("read_committed"), "{transactions}");
        assert!(!transactions.contains("retention"), "{transactions}");
    }

    #[test]
    fn a_persistent_failure_is_logged_once_per_interval_with_the_count_it_left_out() {
        let failures = SweepFailures::maintain_transactions("sqlite");
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
        assert!(logs.contains("failed_sweeps=31"), "{logs}");
        assert!(logs.contains("failures_since_last_log=30"), "{logs}");
    }

    #[test]
    fn a_success_after_failures_is_logged_once_the_interval_has_passed() {
        let failures = SweepFailures::maintain("memory");
        let start = Instant::now();

        _ = captured(|| failures.record(Err("boom"), start));

        let logs = captured(|| failures.record(Ok::<(), &str>(()), start + REPEAT_INTERVAL));
        assert!(logs.contains("succeeded again"), "{logs}");
        assert!(logs.contains("failed_sweeps=1"), "{logs}");

        // The run of failures is over, so a later success has nothing to report.
        let logs = captured(|| {
            failures.record(
                Ok::<(), &str>(()),
                start + REPEAT_INTERVAL + Duration::from_secs(10),
            )
        });
        assert!(logs.is_empty(), "{logs}");
    }

    #[test]
    fn a_sweep_that_fails_once_and_then_succeeds_logs_its_recovery_once() {
        let failures = SweepFailures::maintain_transactions("sqlite");
        let start = Instant::now();

        let logs = captured(|| {
            failures.record(Err("boom"), start);

            // The sweep succeeds on every tick for the next 10 minutes.
            for tick in 1..=60 {
                failures.record(Ok::<(), &str>(()), start + Duration::from_secs(10 * tick));
            }
        });

        // The failure at 0 and the recovery on the first success after the interval.
        assert_eq!(2, logs.lines().count(), "{logs}");
        assert_eq!(1, logs.matches("succeeded again").count(), "{logs}");
        assert!(logs.contains("failed_sweeps=1"), "{logs}");
    }

    #[test]
    fn a_recovery_that_follows_failures_no_line_carried_names_the_latest_and_what_stopped() {
        let failures = SweepFailures::maintain_transactions("postgres");
        let start = Instant::now();

        // The failure at 0 is logged. The failure at 310 seconds is not, because the line at
        // 300 seconds was the recovery.
        let logs = captured(|| {
            failures.record(Err("first"), start);
            failures.record(Ok::<(), &str>(()), start + REPEAT_INTERVAL);
            failures.record(Err("second"), start + Duration::from_secs(310));
        });

        let lines: Vec<_> = logs.lines().collect();
        assert_eq!(2, lines.len(), "{logs}");
        assert!(lines[1].contains("succeeded again"), "{logs}");

        // A recovery with no failure left out is a plain line.
        assert!(!lines[1].contains("stopped"), "{}", lines[1]);
        assert!(!lines[1].contains("error="), "{}", lines[1]);

        // The next due tick is a success: it carries the failure that was left out.
        let logs = captured(|| {
            failures.record(Err("third"), start + Duration::from_secs(400));
            failures.record(Ok::<(), &str>(()), start + Duration::from_secs(600));
        });

        let line = logs.lines().next().expect("the recovery line");
        assert!(line.contains("succeeded again"), "{logs}");
        assert!(line.contains("failures_since_last_log=2"), "{line}");
        assert!(line.contains("third"), "{line}");
        assert!(!line.contains("second"), "{line}");
        assert!(line.contains("read_committed"), "{line}");

        // The recovery line used up its failures, so the next one is plain again.
        let logs = captured(|| failures.record(Err("fourth"), start + Duration::from_secs(900)));
        assert!(logs.contains("failures_since_last_log=1"), "{logs}");
    }

    #[test]
    fn a_failure_that_comes_before_a_held_recovery_is_logged_continues_the_run() {
        let failures = SweepFailures::maintain("memory");
        let start = Instant::now();

        _ = captured(|| failures.record(Err("boom"), start));

        // Both of these are inside the interval, so the success is held and the failure joins the run.
        let logs = captured(|| {
            failures.record(Ok::<(), &str>(()), start + Duration::from_secs(10));
            failures.record(Err("boom"), start + Duration::from_secs(20));
        });
        assert!(logs.is_empty(), "{logs}");

        let logs = captured(|| failures.record(Ok::<(), &str>(()), start + REPEAT_INTERVAL));
        assert!(logs.contains("succeeded again"), "{logs}");
        assert!(logs.contains("failed_sweeps=2"), "{logs}");
        assert!(logs.contains("failures_since_last_log=1"), "{logs}");
    }

    #[test]
    fn a_sweep_that_alternates_between_failing_and_succeeding_stays_within_the_interval() {
        let failures = SweepFailures::maintain_transactions("postgres");
        let start = Instant::now();

        let logs = captured(|| {
            // 61 ticks, 10 seconds apart, which is 10 minutes. The sweep fails on every even tick.
            for tick in 0..=60 {
                let now = start + Duration::from_secs(10 * tick);

                if tick % 2 == 0 {
                    failures.record(Err("pool timed out"), now);
                } else {
                    failures.record(Ok::<(), &str>(()), now);
                }
            }
        });

        // One line at 0, 5 and 10 minutes.
        let lines: Vec<_> = logs.lines().collect();
        assert_eq!(3, lines.len(), "{logs}");

        // The sweep failed on ticks 2, 4 and so on up to 30 since the line at 0: 15 failures. The
        // run counts the failure at 0 as well, and the successes in between do not end it.
        assert!(lines[1].contains("failed_sweeps=16"), "{}", lines[1]);
        assert!(
            lines[1].contains("failures_since_last_log=15"),
            "{}",
            lines[1]
        );
    }

    /// Sums the `nisshi_storage_maintenance_sweeps` counter for `outcome`.
    fn swept(exporter: &InMemoryMetricExporter, outcome: &str) -> u64 {
        exporter
            .get_finished_metrics()
            .expect("finished metrics")
            .iter()
            .flat_map(ResourceMetrics::scope_metrics)
            .flat_map(ScopeMetrics::metrics)
            .filter(|metric| metric.name() == "nisshi_storage_maintenance_sweeps")
            .filter_map(|metric| match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => Some(
                    sum.data_points()
                        .filter(|point| {
                            point.attributes().any(|attribute| {
                                attribute.key.as_str() == "outcome"
                                    && attribute.value.as_str() == outcome
                            })
                        })
                        .map(SumDataPoint::value)
                        .sum::<u64>(),
                ),
                _ => None,
            })
            .sum()
    }

    #[test]
    fn the_counter_counts_every_sweep_by_outcome_including_the_failures_that_are_not_logged() {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();

        let mut failures = SweepFailures::maintain("postgres");
        failures.counter = provider
            .meter("test")
            .u64_counter("nisshi_storage_maintenance_sweeps")
            .build();

        let start = Instant::now();

        _ = captured(|| {
            // Only the first of these 30 failures is logged.
            for tick in 0..30 {
                failures.record(Err("disk full"), start + Duration::from_secs(10 * tick));
            }

            failures.record(Ok::<(), &str>(()), start + Duration::from_secs(300));
        });

        provider.force_flush().expect("flush");

        assert_eq!(30, swept(&exporter, "error"));
        assert_eq!(1, swept(&exporter, "ok"));
    }
}
