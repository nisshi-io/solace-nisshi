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

//! What the broker reports when a storage sweep fails, panics or runs over its deadline.
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
//!
//! A sweep that panics counts as a failure, with the outcome `panic`, and is logged at error
//! through the same limit. Tokio catches the panic in the sweep's task, which would otherwise end
//! unreported at `debug`, and the next tick starts a new sweep.
//!
//! A sweep that is still running when its next tick is due is over its deadline, which is its own
//! interval (`maintenance_interval` and `transaction_maintenance_interval` of the storage URL).
//! The broker skips that tick and calls [`SweepFailures::overdue`] instead, which logs at warn
//! under a limit of its own, one line per [`REPEAT_INTERVAL`], so that a sweep that never finishes
//! keeps being reported, and neither the overdue lines nor the failure and recovery lines hold the
//! other back. The broker never aborts a running sweep on a tick, because a sweep stopped
//! part-way through a compaction could leave its work half done. The gauge
//! `nisshi_storage_maintenance_sweeps_overdue` is 1 while the sweep is over its deadline, and 0
//! once it finishes, however it finishes, and whether or not the limit held an overdue line back.
//! A sweep that logged an overdue line logs a line when it finishes.

use std::{
    fmt::Display,
    panic::AssertUnwindSafe,
    sync::{LazyLock, Mutex, PoisonError},
    time::{Duration, Instant},
};

use futures::FutureExt as _;
use opentelemetry::{
    KeyValue,
    metrics::{Counter, UpDownCounter},
};
use tracing::{error, warn};

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

/// 1 while a sweep is running past the tick that was due to start its next run.
static OVERDUE: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    METER
        .i64_up_down_counter("nisshi_storage_maintenance_sweeps_overdue")
        .with_description("storage maintenance sweeps still running when their next tick is due")
        .build()
});

/// How a sweep ended.
enum Outcome {
    Ok,
    Error(String),
    Panic(String),
}

impl Outcome {
    fn label(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error(_) => "error",
            Self::Panic(_) => "panic",
        }
    }
}

#[derive(Debug, Default)]
struct State {
    /// Whether a sweep has started and not yet finished. A tick that finds the sweep's task
    /// unfinished can come after the sweep recorded its outcome and before its task ended, and
    /// this flag, which `finish` clears under the same lock that `overdue` reads it under, stops
    /// that tick from reporting a sweep that is over.
    in_flight: bool,

    /// When the broker first found the running sweep past its next tick. `None` when the sweep is
    /// not overdue.
    overdue_since: Option<Instant>,

    /// Whether a line reported the current overdue sweep.
    overdue_logged: bool,

    /// When the last overdue line was written. Overdue lines have their own limit, so that a
    /// failure or recovery line cannot hold one back and an overdue line cannot hold those back.
    last_overdue_logged: Option<Instant>,

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

    fn overdue_log_is_due(&self, now: Instant) -> bool {
        self.last_overdue_logged
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
    overdue: UpDownCounter<i64>,
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
            overdue: OVERDUE.clone(),
            state: Mutex::new(State::default()),
        }
    }

    /// Runs `sweep` and records how it ends, including a panic, which would otherwise unwind
    /// into the broker's task set and end unreported at `debug`.
    ///
    /// The panic is caught in the same task, not in a task of its own, so that aborting the
    /// broker's task set still aborts the sweep.
    pub(crate) async fn run<E, F>(&self, sweep: F)
    where
        E: Display,
        F: Future<Output = Result<(), E>>,
    {
        self.begin();

        match AssertUnwindSafe(sweep).catch_unwind().await {
            Ok(outcome) => self.record(outcome, Instant::now()),
            Err(payload) => {
                self.finish(Outcome::Panic(panic_message(&*payload)), Instant::now());
            }
        }
    }

    /// Marks a sweep as running, so that [`SweepFailures::overdue`] can report it.
    fn begin(&self) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .in_flight = true;
    }

    /// Reports that the sweep is still running at `now`, when its next tick is due. A sweep that
    /// has already recorded its outcome is not reported, whatever its task is still doing.
    pub(crate) fn overdue(&self, now: Instant) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);

        if !state.in_flight {
            return;
        }

        let since = *state.overdue_since.get_or_insert_with(|| {
            self.overdue.add(1, &self.attributes());
            now
        });

        if !state.overdue_log_is_due(now) {
            return;
        }

        warn!(
            sweep = self.sweep,
            engine = %self.engine,
            overdue_for_secs = now.saturating_duration_since(since).as_secs(),
            stopped = self.stopped,
            "storage maintenance sweep is still running when its next run is due"
        );

        state.overdue_logged = true;
        state.last_overdue_logged = Some(now);
    }

    fn attributes(&self) -> [KeyValue; 2] {
        [
            KeyValue::new("sweep", self.sweep),
            KeyValue::new("engine", self.engine.clone()),
        ]
    }

    /// Records the outcome of a sweep that ran at `now`.
    pub(crate) fn record<E: Display>(&self, outcome: Result<(), E>, now: Instant) {
        self.finish(
            match outcome {
                Ok(()) => Outcome::Ok,
                Err(error) => Outcome::Error(error.to_string()),
            },
            now,
        );
    }

    fn finish(&self, outcome: Outcome, now: Instant) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);

        state.in_flight = false;

        self.counter.add(
            1,
            &[
                KeyValue::new("sweep", self.sweep),
                KeyValue::new("engine", self.engine.clone()),
                KeyValue::new("outcome", outcome.label()),
            ],
        );

        if let Some(since) = state.overdue_since.take() {
            self.overdue.add(-1, &self.attributes());

            if std::mem::take(&mut state.overdue_logged) {
                // The line carries the error, because the failure line of this sweep can be held
                // back by the limit of an earlier failure line.
                let (error, panic) = match &outcome {
                    Outcome::Ok => (None, None),
                    Outcome::Error(error) => (Some(error.as_str()), None),
                    Outcome::Panic(message) => (None, Some(message.as_str())),
                };

                if panic.is_some() {
                    error!(
                        sweep = self.sweep,
                        engine = %self.engine,
                        overdue_for_secs = now.saturating_duration_since(since).as_secs(),
                        outcome = outcome.label(),
                        panic,
                        "storage maintenance sweep that was over its deadline panicked"
                    );
                } else {
                    warn!(
                        sweep = self.sweep,
                        engine = %self.engine,
                        overdue_for_secs = now.saturating_duration_since(since).as_secs(),
                        outcome = outcome.label(),
                        error,
                        "storage maintenance sweep that was over its deadline has finished"
                    );
                }
            }
        }

        let panicked = matches!(outcome, Outcome::Panic(_));

        match outcome {
            Outcome::Ok => {
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
                // stopped, because the failure line before it may be hours old. The fields are
                // named differently from the failure line's `error` and `stopped`, so that a log
                // query on those two matches only a failure that is happening now.
                warn!(
                    sweep = self.sweep,
                    engine = %self.engine,
                    failed_sweeps = state.failed_sweeps,
                    failures_since_last_log = state.unlogged,
                    was_stopped = (state.unlogged > 0).then_some(self.stopped),
                    last_error = state.unlogged_error.as_deref(),
                    "storage maintenance sweep succeeded again"
                );

                state.failed_sweeps = 0;
                state.unlogged = 0;
                state.unlogged_error = None;
                state.last_logged = Some(now);
            }

            Outcome::Error(error) | Outcome::Panic(error) => {
                state.failed_sweeps += 1;
                state.unlogged += 1;

                if !state.log_is_due(now) {
                    state.unlogged_error = Some(error);
                    return;
                }

                if panicked {
                    error!(
                        sweep = self.sweep,
                        engine = %self.engine,
                        failed_sweeps = state.failed_sweeps,
                        failures_since_last_log = state.unlogged,
                        stopped = self.stopped,
                        panic = %error,
                        "storage maintenance sweep panicked"
                    );
                } else {
                    warn!(
                        sweep = self.sweep,
                        engine = %self.engine,
                        failed_sweeps = state.failed_sweeps,
                        failures_since_last_log = state.unlogged,
                        stopped = self.stopped,
                        %error,
                        "storage maintenance sweep failed"
                    );
                }

                state.unlogged = 0;
                state.unlogged_error = None;
                state.last_logged = Some(now);
            }
        }
    }
}

impl Drop for SweepFailures {
    /// Takes back the overdue report of a sweep that never finished, because the broker aborted
    /// its task at shutdown. The gauge is process-wide, so the report would otherwise stay at 1
    /// for the next broker in the process.
    fn drop(&mut self) {
        let overdue = self
            .state
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .overdue_since
            .take()
            .is_some();

        if overdue {
            self.overdue.add(-1, &self.attributes());
        }
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_owned())
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
        assert!(logs.contains("sweep=\"maintain\""), "{logs}");
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
        assert!(!lines[1].contains("was_stopped"), "{}", lines[1]);
        assert!(!lines[1].contains("last_error"), "{}", lines[1]);

        // The next due tick is a success: it carries the failure that was left out.
        let logs = captured(|| {
            failures.record(Err("third"), start + Duration::from_secs(400));
            failures.record(Ok::<(), &str>(()), start + Duration::from_secs(600));
        });

        let line = logs.lines().next().expect("the recovery line");
        assert!(line.contains("succeeded again"), "{logs}");
        assert!(line.contains("failures_since_last_log=2"), "{line}");
        assert!(line.contains("last_error=\"third\""), "{line}");
        assert!(!line.contains("second"), "{line}");
        assert!(line.contains("was_stopped="), "{line}");
        assert!(line.contains("read_committed"), "{line}");

        // The failure line's own fields do not appear on the recovery line.
        assert!(!line.contains(" error="), "{line}");
        assert!(!line.contains(" stopped="), "{line}");

        // The recovery line used up its failures, so the next one is plain again.
        let logs = captured(|| failures.record(Err("fourth"), start + Duration::from_secs(900)));
        assert!(logs.contains("failures_since_last_log=1"), "{logs}");
    }

    #[test]
    fn a_recovery_does_not_name_a_failure_that_an_earlier_line_carried() {
        let failures = SweepFailures::maintain_transactions("postgres");
        let start = Instant::now();

        let logs = captured(|| {
            // The failure at 0 is logged. The failure at 10 seconds is held, and the failure at
            // 300 seconds is logged with a count that includes it, so the held error is used up.
            failures.record(Err("first"), start);
            failures.record(Err("held"), start + Duration::from_secs(10));
            failures.record(Err("second"), start + REPEAT_INTERVAL);

            failures.record(Ok::<(), &str>(()), start + 2 * REPEAT_INTERVAL);
        });

        let recovery = logs.lines().last().expect("the recovery line");
        assert!(recovery.contains("succeeded again"), "{logs}");
        assert!(recovery.contains("failures_since_last_log=0"), "{recovery}");
        assert!(!recovery.contains("held"), "{recovery}");
        assert!(!recovery.contains("last_error"), "{recovery}");
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

    /// A `SweepFailures` whose counters export to `exporter`.
    fn measured(
        sweep: fn(String) -> SweepFailures,
        engine: &str,
    ) -> (SweepFailures, InMemoryMetricExporter, SdkMeterProvider) {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();

        let mut failures = sweep(engine.to_owned());
        let meter = provider.meter("test");
        failures.counter = meter
            .u64_counter("nisshi_storage_maintenance_sweeps")
            .build();
        failures.overdue = meter
            .i64_up_down_counter("nisshi_storage_maintenance_sweeps_overdue")
            .build();

        (failures, exporter, provider)
    }

    /// The latest value of the overdue gauge, summed over its series.
    fn overdue_now(exporter: &InMemoryMetricExporter, provider: &SdkMeterProvider) -> i64 {
        provider.force_flush().expect("flush");
        exporter.reset();
        provider.force_flush().expect("flush");

        exporter
            .get_finished_metrics()
            .expect("finished metrics")
            .iter()
            .flat_map(ResourceMetrics::scope_metrics)
            .flat_map(ScopeMetrics::metrics)
            .filter(|metric| metric.name() == "nisshi_storage_maintenance_sweeps_overdue")
            .filter_map(|metric| match metric.data() {
                AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                    Some(sum.data_points().map(SumDataPoint::value).sum::<i64>())
                }
                _ => None,
            })
            .sum()
    }

    #[tokio::test]
    async fn a_panicking_sweep_is_logged_at_error_with_its_engine_and_counted() {
        let (failures, exporter, provider) =
            measured(SweepFailures::maintain_transactions, "postgres");

        let capture = Capture::default();
        let writer = capture.clone();

        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_max_level(Level::WARN)
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .finish(),
        );

        failures
            .run(async { panic!("lock poisoned") as Result<(), &'static str> })
            .await;

        // A second panic inside the interval is counted and not logged again.
        failures
            .run(async { panic!("lock poisoned") as Result<(), &'static str> })
            .await;

        let logs = capture.text();
        assert_eq!(1, logs.lines().count(), "{logs}");
        assert!(logs.contains("ERROR"), "{logs}");
        assert!(logs.contains("sweep=\"maintain_transactions\""), "{logs}");
        assert!(logs.contains("engine=postgres"), "{logs}");
        assert!(logs.contains("lock poisoned"), "{logs}");

        provider.force_flush().expect("flush");
        assert_eq!(2, swept(&exporter, "panic"));
        assert_eq!(0, swept(&exporter, "ok"));
    }

    #[tokio::test]
    async fn an_overdue_sweep_that_panics_logs_the_panic_at_error_on_its_finish_line() {
        let failures = SweepFailures::maintain_transactions("postgres");
        let start = Instant::now();

        failures.begin();
        _ = captured(|| failures.overdue(start));

        let capture = Capture::default();
        let writer = capture.clone();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_max_level(Level::WARN)
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .finish(),
        );

        failures
            .run(async { panic!("index out of range") as Result<(), &'static str> })
            .await;

        // The overdue line did not use the failure limit, so the panic is logged on its own line
        // as well as on the finish line.
        let logs = capture.text();
        let lines: Vec<_> = logs.lines().collect();
        assert_eq!(2, lines.len(), "{logs}");
        assert!(
            lines[0].contains("was over its deadline panicked"),
            "{logs}"
        );
        assert!(
            lines[1].contains("storage maintenance sweep panicked"),
            "{logs}"
        );

        for line in lines {
            assert!(line.contains("ERROR"), "{line}");
            assert!(line.contains("index out of range"), "{line}");
        }
    }

    #[tokio::test]
    async fn a_panic_joins_the_run_of_failures() {
        let failures = SweepFailures::maintain("memory");

        failures
            .run(async { panic!("boom") as Result<(), &'static str> })
            .await;

        let state = failures.state.lock().unwrap();
        assert_eq!(1, state.failed_sweeps);
    }

    #[test]
    fn a_sweep_over_its_deadline_is_reported_at_warn_and_again_only_once_per_interval() {
        let (failures, exporter, provider) = measured(SweepFailures::maintain, "sqlite");
        let start = Instant::now();

        failures.begin();

        let logs = captured(|| {
            failures.overdue(start);
            failures.overdue(start + Duration::from_secs(10));
        });

        assert_eq!(1, logs.lines().count(), "{logs}");
        assert!(logs.contains("WARN"), "{logs}");
        assert!(logs.contains("sweep=\"maintain\""), "{logs}");
        assert!(logs.contains("engine=sqlite"), "{logs}");
        assert!(logs.contains("retention, compaction"), "{logs}");
        assert_eq!(1, overdue_now(&exporter, &provider));

        // A sweep that never finishes keeps being reported.
        let logs = captured(|| failures.overdue(start + REPEAT_INTERVAL));
        assert_eq!(1, logs.lines().count(), "{logs}");
        assert!(logs.contains("overdue_for_secs=300"), "{logs}");
    }

    #[test]
    fn the_report_clears_when_the_overdue_sweep_finishes() {
        let (failures, exporter, provider) = measured(SweepFailures::maintain, "sqlite");
        let start = Instant::now();

        failures.begin();
        _ = captured(|| failures.overdue(start));
        assert_eq!(1, overdue_now(&exporter, &provider));

        let logs = captured(|| {
            failures.record(Ok::<(), &str>(()), start + Duration::from_secs(20));
        });
        assert_eq!(1, logs.lines().count(), "{logs}");
        assert!(logs.contains("has finished"), "{logs}");
        assert!(logs.contains("outcome=\"ok\""), "{logs}");
        assert_eq!(0, overdue_now(&exporter, &provider));

        // Nothing is left to clear.
        let logs = captured(|| {
            failures.record(Ok::<(), &str>(()), start + Duration::from_secs(40));
        });
        assert!(logs.is_empty(), "{logs}");
        assert_eq!(0, overdue_now(&exporter, &provider));
    }

    #[test]
    fn an_overdue_sweep_that_fails_clears_the_report_and_logs_the_failure() {
        let (failures, exporter, provider) =
            measured(SweepFailures::maintain_transactions, "postgres");
        let start = Instant::now();

        failures.begin();
        _ = captured(|| failures.overdue(start));

        // The overdue line did not use the failure limit, so the failure line is written as well
        // as the finish line, and both carry the error.
        let logs = captured(|| {
            failures.record(Err("connection reset"), start + Duration::from_secs(10));
        });

        assert_eq!(2, logs.lines().count(), "{logs}");
        assert!(logs.contains("has finished"), "{logs}");
        assert!(logs.contains("storage maintenance sweep failed"), "{logs}");
        assert_eq!(2, logs.matches("connection reset").count(), "{logs}");
        assert_eq!(0, overdue_now(&exporter, &provider));
    }

    #[test]
    fn a_finish_line_follows_only_an_overdue_line_that_was_written() {
        let (failures, exporter, provider) = measured(SweepFailures::maintain, "memory");
        let start = Instant::now();

        // The first overdue sweep is reported, and the second is inside the overdue limit, so
        // its line is held back.
        failures.begin();
        let logs = captured(|| {
            failures.overdue(start);
            failures.record(Ok::<(), &str>(()), start + Duration::from_secs(20));
        });
        assert_eq!(1, logs.matches("has finished").count(), "{logs}");

        failures.begin();
        let logs = captured(|| failures.overdue(start + Duration::from_secs(60)));
        assert!(logs.is_empty(), "{logs}");

        // The held line leaves the gauge at 1, because the gauge does not follow the log limit.
        assert_eq!(1, overdue_now(&exporter, &provider));

        let logs = captured(|| {
            failures.record(Ok::<(), &str>(()), start + Duration::from_secs(80));
        });

        assert!(logs.is_empty(), "{logs}");
        assert_eq!(0, overdue_now(&exporter, &provider));
    }

    #[test]
    fn a_tick_after_the_sweep_recorded_its_outcome_does_not_report_it() {
        let (failures, exporter, provider) = measured(SweepFailures::maintain, "sqlite");
        let start = Instant::now();

        failures.begin();
        failures.record(Ok::<(), &str>(()), start);

        // The sweep's task has not ended yet, so the broker's tick still finds it unfinished.
        let logs = captured(|| failures.overdue(start + Duration::from_secs(10)));

        assert!(logs.is_empty(), "{logs}");
        assert_eq!(0, overdue_now(&exporter, &provider));

        // The next sweep is not reported as late either.
        failures.begin();
        let logs =
            captured(|| failures.record(Ok::<(), &str>(()), start + Duration::from_secs(20)));

        assert!(logs.is_empty(), "{logs}");
    }

    #[test]
    fn a_sweep_that_hangs_right_after_a_failure_is_reported() {
        let failures = SweepFailures::maintain_transactions("postgres");
        let start = Instant::now();

        let logs = captured(|| {
            failures.record(Err("pool timed out"), start);
            failures.begin();
            failures.overdue(start + Duration::from_secs(20));
        });

        let lines: Vec<_> = logs.lines().collect();
        assert_eq!(2, lines.len(), "{logs}");
        assert!(
            lines[0].contains("storage maintenance sweep failed"),
            "{logs}"
        );
        assert!(lines[1].contains("still running"), "{logs}");
    }

    #[test]
    fn a_slow_sweep_that_recovered_still_logs_its_recovery() {
        let failures = SweepFailures::maintain("postgres");
        let start = Instant::now();

        _ = captured(|| failures.record(Err("boom"), start));

        // The sweep runs a little over its interval, so each tick that finds it running is an
        // overdue tick. The overdue line at 300 seconds must not use up the recovery's limit.
        failures.begin();

        let logs = captured(|| {
            failures.overdue(start + REPEAT_INTERVAL);
            failures.record(
                Ok::<(), &str>(()),
                start + REPEAT_INTERVAL + Duration::from_secs(5),
            );
        });

        assert!(logs.contains("succeeded again"), "{logs}");
        assert!(logs.contains("has finished"), "{logs}");
    }

    #[test]
    fn overdue_lines_are_limited_to_one_per_interval_whatever_else_is_logged() {
        let failures = SweepFailures::maintain("memory");
        let start = Instant::now();

        failures.begin();

        let logs = captured(|| {
            failures.overdue(start);
            failures.record(Err("boom"), start + Duration::from_secs(10));
            failures.begin();
            failures.overdue(start + Duration::from_secs(20));
        });

        // The overdue line at 0, the finish line and the failure line at 10 seconds, and nothing
        // for the overdue tick at 20 seconds, which is inside the overdue limit.
        assert_eq!(3, logs.lines().count(), "{logs}");
        assert_eq!(1, logs.matches("still running").count(), "{logs}");
    }

    #[test]
    fn a_sweep_that_never_finishes_takes_back_its_report_when_the_broker_is_dropped() {
        let (failures, exporter, provider) = measured(SweepFailures::maintain, "sqlite");

        failures.begin();
        _ = captured(|| failures.overdue(Instant::now()));
        assert_eq!(1, overdue_now(&exporter, &provider));

        drop(failures);

        assert_eq!(0, overdue_now(&exporter, &provider));
    }
}
