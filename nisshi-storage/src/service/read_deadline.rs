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

//! Deadlines for storage reads made while answering a client request.
//!
//! Each read stops at its deadline, so that a slow storage engine doesn't hold a request after its
//! client has given up on it. A client that gives up sends the request again, and without a
//! deadline the abandoned read would keep running beside the new one.

use std::{
    cell::Cell,
    future::Future,
    pin::{Pin, pin},
    sync::LazyLock,
};

use opentelemetry::{KeyValue, metrics::Counter};
use tokio::time::{Duration, Instant, Sleep, sleep_until};

use crate::{METER, StorageOperation};

/// How long ListOffsets may spend in storage.
///
/// It is half of the shortest default client timeout, so the broker answers before a client with
/// default settings gives up. The defaults are 10s for franz-go, 30s for Java and 60s for
/// librdkafka. A client configured with a shorter timeout gives up before the read ends, and the
/// read still stops at this deadline.
pub(crate) const LIST_OFFSETS_READ_DEADLINE: Duration = Duration::from_secs(5);

/// How long DeleteRecords may spend starting storage calls, for the whole
/// request.
///
/// If a partition's call has not started at this deadline, the partition
/// answers `REQUEST_TIMED_OUT` with a low watermark of -1. That includes a call
/// that is still waiting for a permit that its storage engine shares across
/// requests, see [`DeleteRecordsConcurrency`](crate::DeleteRecordsConcurrency).
/// A call that has started runs to its end, so the request takes longer than
/// the deadline by at most the longest call among those that started before
/// it. A storage engine that runs its writes one at a time lets one call of a
/// request run at a time, so no call waits behind another after the deadline
/// check. A retry is safe, because a cut at or below the log start changes
/// nothing.
///
/// The bound is this constant, not the request's `timeout_ms`. Kafka cuts
/// the logs first and then waits at most `timeout_ms` for its replicas to
/// advance the low watermark ([ReplicaManager.scala#L1317-L1336]), and
/// answers `REQUEST_TIMED_OUT` only for a partition still waiting at the
/// end ([DelayedDeleteRecords.scala#L55-L56]). Nisshi has no replicas, so
/// a client's short `timeout_ms` must not cut work that Kafka completes.
/// The value is the one of [`LIST_OFFSETS_READ_DEADLINE`], under the Java
/// admin client's 30s `request.timeout.ms`.
///
/// [ReplicaManager.scala#L1317-L1336]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1317-L1336
/// [DelayedDeleteRecords.scala#L55-L56]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/DelayedDeleteRecords.scala#L55-L56
pub(crate) const DELETE_RECORDS_DEADLINE: Duration = Duration::from_secs(5);

static READ_DEADLINE_EXCEEDED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_storage_read_deadline_exceeded")
        .with_description("Storage reads stopped at their deadline")
        .build()
});

/// What a read waits for before it reaches storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadWait {
    /// Exclusive access to a storage engine that serves one request at a time.
    ExclusiveEngineAccess,
    /// A permit from the client connection's limit on reads in flight.
    ConnectionReadLimit,
    /// A permit from the read share of the client that made the request, over all its
    /// connections.
    ClientReadLimit,
    /// A permit from the storage engine's limit on reads in flight.
    EngineReadLimit,
    /// A connection from the storage engine's pool of database connections.
    EngineConnectionPool,
    /// A permit from the storage engine's limit on writes in flight.
    EngineWriteLimit,
}

impl ReadWait {
    /// Returns the value of the `wait` attribute for a read stopped during this wait.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ExclusiveEngineAccess => "exclusive_engine_access",
            Self::ConnectionReadLimit => "connection_read_limit",
            Self::ClientReadLimit => "client_read_limit",
            Self::EngineReadLimit => "engine_read_limit",
            Self::EngineConnectionPool => "engine_connection_pool",
            Self::EngineWriteLimit => "engine_write_limit",
        }
    }
}

/// How a read, or one step of a read, timed out: what it was doing when its deadline stopped it.
/// A read that timed out can still have sent data before its deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimedOut {
    /// The deadline had passed before it started, so it made no storage call.
    NotStarted,
    /// It was waiting for a resource before storage, such as a permit or a connection.
    Queued(ReadWait),
    /// It was past every marked wait and in storage.
    Reading,
}

impl TimedOut {
    /// Returns the value of the `stage` attribute for this read.
    pub(crate) fn stage(self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::Queued(_) => "queued",
            Self::Reading => "reading",
        }
    }

    /// Returns the value of the `wait` attribute for a queued read, or `None` for a read in
    /// another stage.
    pub(crate) fn wait(self) -> Option<&'static str> {
        match self {
            Self::Queued(read_wait) => Some(read_wait.as_str()),
            Self::NotStarted | Self::Reading => None,
        }
    }

    /// Returns the attributes of the `nisshi_storage_read_deadline_exceeded` counter for this read
    /// of `operation`.
    pub(crate) fn attributes(self, operation: StorageOperation) -> Vec<KeyValue> {
        let mut attributes = vec![operation.attribute(), KeyValue::new("stage", self.stage())];
        attributes.extend(self.wait().map(|wait| KeyValue::new("wait", wait)));
        attributes
    }
}

/// The deadline of a storage read, with a timer that fires at it.
///
/// One `ReadDeadline` can run several futures, such as the waits for each item of a
/// [`Storage::fetch`] read, and they share its deadline and its timer. When the deadline stops a
/// future, the `nisshi_storage_read_deadline_exceeded` counter counts it once.
///
/// [`Storage::fetch`]: crate::Storage::fetch
#[derive(Debug)]
pub struct ReadDeadline {
    operation: StorageOperation,
    deadline: Instant,
    timer: Pin<Box<Sleep>>,
    /// Whether a future has started under this deadline.
    started: bool,
    /// Whether this deadline has counted a timeout. A read counts at most one.
    counted: bool,
}

impl ReadDeadline {
    /// Returns the deadline of a read of `operation` that must finish by `deadline`.
    ///
    /// Panics outside a Tokio runtime, because the timer needs one. A deadline without a timer
    /// could not stop a read.
    pub fn new(operation: StorageOperation, deadline: Instant) -> Self {
        Self {
            operation,
            deadline,
            timer: Box::pin(sleep_until(deadline)),
            started: false,
            counted: false,
        }
    }

    /// Returns whether the deadline has passed.
    fn has_passed(&self) -> bool {
        Instant::now() >= self.deadline
    }

    /// Runs `future` until the deadline, and returns what it was doing if the deadline passes
    /// first.
    ///
    /// A future does not start once the deadline has passed, because it would send a storage
    /// request only to drop it.
    ///
    /// The deadline limits how long the caller waits, not how long the storage engine works. A
    /// future dropped at the deadline can leave a database server still running a statement that it
    /// sent.
    pub async fn run_until_deadline<F>(&mut self, future: F) -> Result<F::Output, TimedOut>
    where
        F: Future,
    {
        self.check_start()?;

        // `scope` installs this read's cell in `QUEUED` only while it polls the future, so a
        // `QueueWait` marker inside the future writes to this read's cell even when several reads
        // run on one task.
        let mut future = pin!(QUEUED.scope(Cell::new(None), future));

        // The future is polled before the timer, so that a future that finishes in the poll in
        // which the timer fires keeps its output.
        let timed_out = tokio::select! {
            biased;
            output = future.as_mut() => return Ok(output),
            () = self.timer.as_mut() => future
                .as_mut()
                .take_value()
                .and_then(Cell::into_inner)
                // A future that isn't in a marked wait when the timer fires is reading.
                .map_or(TimedOut::Reading, TimedOut::Queued),
        };

        self.count(timed_out);
        Err(timed_out)
    }

    /// Runs `future` to its end if it starts before the deadline, and returns the timeout otherwise.
    /// This never drops `future`, so a write that has started runs to its end.
    pub(crate) async fn start<F>(&mut self, future: F) -> Result<F::Output, TimedOut>
    where
        F: Future,
    {
        self.check_start()?;
        Ok(future.await)
    }

    /// Returns the timeout if the deadline has passed. If the deadline keeps the first step of a
    /// read from starting, the read timed out as [`TimedOut::NotStarted`]. If the deadline stops a
    /// read before a later step, the read timed out as [`TimedOut::Reading`].
    fn check_start(&mut self) -> Result<(), TimedOut> {
        if !self.has_passed() {
            self.started = true;
            return Ok(());
        }

        let timed_out = if self.started {
            TimedOut::Reading
        } else {
            TimedOut::NotStarted
        };

        self.count(timed_out);
        Err(timed_out)
    }

    /// Counts `timed_out` in the `nisshi_storage_read_deadline_exceeded` counter, unless this
    /// deadline has counted a timeout already.
    fn count(&mut self, timed_out: TimedOut) {
        if !self.counted {
            self.counted = true;
            READ_DEADLINE_EXCEEDED.add(1, &timed_out.attributes(self.operation));
        }
    }
}

tokio::task_local! {
    /// What the polled read is waiting for, if anything.
    static QUEUED: Cell<Option<ReadWait>>;
}

/// Marks a future that a read awaits as one of the read's waits before storage.
///
/// The mark reaches [`ReadDeadline::run_until_deadline`] through a task-local, so a wait that runs
/// on a spawned task is not marked, and counts as reading.
pub trait QueueWait: Future + Sized {
    /// Marks this future as a wait for `read_wait`.
    fn queued_for(self, read_wait: ReadWait) -> impl Future<Output = Self::Output>;
}

impl<F: Future> QueueWait for F {
    async fn queued_for(self, read_wait: ReadWait) -> F::Output {
        let wait = self;

        let outer = QUEUED.try_with(|queued| queued.replace(Some(read_wait)));
        let output = wait.await;
        // The wait restores the mark that it replaced instead of clearing it, so a marked wait
        // inside another marked wait leaves the outer wait marked when it ends.
        if let Ok(outer) = outer {
            _ = QUEUED.try_with(|queued| queued.set(outer));
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use opentelemetry::global;
    use opentelemetry_sdk::metrics::{
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        data::{AggregatedMetrics, MetricData, ResourceMetrics, ScopeMetrics, SumDataPoint},
    };
    use tokio::time::sleep;

    use super::*;

    fn fetch_deadline(deadline: Instant) -> ReadDeadline {
        ReadDeadline::new(StorageOperation::Fetch, deadline)
    }

    #[tokio::test(start_paused = true)]
    async fn finishes_before_the_deadline() {
        let mut deadline = fetch_deadline(Instant::now() + Duration::from_secs(1));

        assert_eq!(Ok(7), deadline.run_until_deadline(async { 7 }).await);
    }

    #[test]
    #[should_panic(expected = "Tokio")]
    fn outside_a_runtime_panics() {
        _ = fetch_deadline(Instant::now() + Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_at_the_deadline() {
        let started_at = Instant::now();
        let mut deadline = fetch_deadline(started_at + Duration::from_secs(1));

        assert_eq!(
            Err(TimedOut::Reading),
            deadline.run_until_deadline(pending::<()>()).await
        );
        assert_eq!(Duration::from_secs(1), started_at.elapsed());
    }

    /// Futures run one after another under one `ReadDeadline` share the same deadline.
    #[tokio::test(start_paused = true)]
    async fn does_not_restart_for_each_future() {
        let started_at = Instant::now();
        let mut deadline = fetch_deadline(started_at + Duration::from_secs(1));

        let step = || async { sleep(Duration::from_millis(600)).await };

        assert_eq!(Ok(()), deadline.run_until_deadline(step()).await);
        assert_eq!(
            Err(TimedOut::Reading),
            deadline.run_until_deadline(step()).await
        );
        assert_eq!(Duration::from_secs(1), started_at.elapsed());
    }

    /// A future that finishes at the same instant as the deadline keeps its output.
    #[tokio::test(start_paused = true)]
    async fn keeps_a_future_that_finishes_at_the_deadline() {
        let at = Instant::now() + Duration::from_secs(1);
        let mut deadline = fetch_deadline(at);

        assert_eq!(
            Ok(7),
            deadline
                .run_until_deadline(async {
                    sleep_until(at).await;
                    7
                })
                .await
        );
    }

    /// Once the deadline has passed, the future is never polled.
    #[tokio::test(start_paused = true)]
    async fn does_not_start_after_the_deadline() {
        let mut deadline = fetch_deadline(Instant::now());
        tokio::time::advance(Duration::from_millis(1)).await;

        let polled = Arc::new(AtomicBool::new(false));

        let read = {
            let polled = polled.clone();
            async move { polled.store(true, Ordering::SeqCst) }
        };

        assert!(deadline.has_passed());
        assert_eq!(
            Err(TimedOut::NotStarted),
            deadline.run_until_deadline(read).await
        );
        assert!(!polled.load(Ordering::SeqCst));
    }

    /// `start` runs a future that started before the deadline to its end, past the deadline.
    #[tokio::test(start_paused = true)]
    async fn start_runs_a_started_future_past_the_deadline() {
        let started_at = Instant::now();
        let mut deadline = fetch_deadline(started_at + Duration::from_secs(1));

        assert_eq!(
            Ok(7),
            deadline
                .start(async {
                    sleep(Duration::from_secs(2)).await;
                    7
                })
                .await
        );
        assert_eq!(Duration::from_secs(2), started_at.elapsed());
        assert_eq!(Err(TimedOut::Reading), deadline.start(async { 7 }).await);
    }

    /// A read stopped during a marked wait reports that wait. A read stopped after its wait
    /// finished reports `Reading`.
    #[tokio::test(start_paused = true)]
    async fn tells_a_queued_read_from_a_reading_one() {
        let mut deadline = fetch_deadline(Instant::now() + Duration::from_secs(1));

        assert_eq!(
            Err(TimedOut::Queued(ReadWait::EngineConnectionPool)),
            deadline
                .run_until_deadline(pending::<()>().queued_for(ReadWait::EngineConnectionPool))
                .await
        );

        let mut deadline = fetch_deadline(Instant::now() + Duration::from_secs(1));

        assert_eq!(
            Err(TimedOut::Reading),
            deadline
                .run_until_deadline(async {
                    async {}.queued_for(ReadWait::EngineConnectionPool).await;
                    pending::<()>().await
                })
                .await
        );
    }

    /// A marked wait that ends inside another marked wait leaves the outer wait marked.
    #[tokio::test(start_paused = true)]
    async fn nested_wait_restores_the_outer_wait() {
        let mut deadline = fetch_deadline(Instant::now() + Duration::from_secs(1));

        assert_eq!(
            Err(TimedOut::Queued(ReadWait::EngineConnectionPool)),
            deadline
                .run_until_deadline(
                    async {
                        async {}.queued_for(ReadWait::ConnectionReadLimit).await;
                        pending::<()>().await
                    }
                    .queued_for(ReadWait::EngineConnectionPool)
                )
                .await
        );
    }

    /// Two reads that run at once on one task each report their own stage.
    #[tokio::test(start_paused = true)]
    async fn reads_run_at_once_keep_their_own_stage() {
        let at = Instant::now() + Duration::from_secs(1);
        let (mut queued_deadline, mut reading_deadline) = (fetch_deadline(at), fetch_deadline(at));

        let (queued_read, reading) = tokio::join!(
            queued_deadline
                .run_until_deadline(pending::<()>().queued_for(ReadWait::EngineConnectionPool)),
            reading_deadline.run_until_deadline(pending::<()>())
        );

        assert_eq!(
            Err(TimedOut::Queued(ReadWait::EngineConnectionPool)),
            queued_read
        );
        assert_eq!(Err(TimedOut::Reading), reading);
    }

    /// Installs a process-wide meter provider that exports to the returned exporter. It must run
    /// before the code creates the `nisshi_storage_read_deadline_exceeded` counter, which happens
    /// on the counter's first use. nextest runs each test in a process of its own, so the counter
    /// is created after this in every test that calls it.
    fn install_exporter() -> (SdkMeterProvider, InMemoryMetricExporter) {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        global::set_meter_provider(provider.clone());
        (provider, exporter)
    }

    /// Sums the `nisshi_storage_read_deadline_exceeded` counter over the data points whose
    /// attributes include every pair in `attributes`.
    fn exceeded(exporter: &InMemoryMetricExporter, attributes: &[(&str, &str)]) -> u64 {
        exporter
            .get_finished_metrics()
            .expect("finished metrics")
            .iter()
            .flat_map(ResourceMetrics::scope_metrics)
            .flat_map(ScopeMetrics::metrics)
            .filter(|metric| metric.name() == "nisshi_storage_read_deadline_exceeded")
            .filter_map(|metric| match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => Some(
                    sum.data_points()
                        .filter(|point| {
                            attributes.iter().all(|(key, value)| {
                                point.attributes().any(|attribute| {
                                    attribute.key.as_str() == *key
                                        && attribute.value.as_str() == *value
                                })
                            })
                        })
                        .map(SumDataPoint::value)
                        .sum::<u64>(),
                ),
                _ => None,
            })
            .sum()
    }

    /// Each stopped future adds one to the counter under its operation and stage, and a future that
    /// finishes in time adds nothing.
    #[tokio::test(start_paused = true)]
    async fn counts_each_stopped_future_by_stage() {
        let (provider, exporter) = install_exporter();

        let in_a_second = || Instant::now() + Duration::from_secs(1);

        assert_eq!(
            Ok(7),
            ReadDeadline::new(StorageOperation::ListOffsets, in_a_second())
                .run_until_deadline(async { 7 })
                .await
        );
        assert_eq!(
            Err(TimedOut::Reading),
            fetch_deadline(in_a_second())
                .run_until_deadline(pending::<()>())
                .await
        );
        assert_eq!(
            Err(TimedOut::Reading),
            fetch_deadline(in_a_second())
                .run_until_deadline(pending::<()>())
                .await
        );
        assert_eq!(
            Err(TimedOut::Queued(ReadWait::EngineConnectionPool)),
            fetch_deadline(in_a_second())
                .run_until_deadline(pending::<()>().queued_for(ReadWait::EngineConnectionPool))
                .await
        );
        assert_eq!(
            Err(TimedOut::NotStarted),
            fetch_deadline(Instant::now())
                .run_until_deadline(async {})
                .await
        );

        provider.force_flush().expect("flush");

        let fetch = StorageOperation::Fetch.as_str();
        let list_offsets = StorageOperation::ListOffsets.as_str();

        assert_eq!(
            0,
            exceeded(
                &exporter,
                &[("operation", list_offsets), ("stage", "reading")]
            )
        );
        assert_eq!(
            2,
            exceeded(&exporter, &[("operation", fetch), ("stage", "reading")])
        );
        assert_eq!(
            1,
            exceeded(&exporter, &[("operation", fetch), ("stage", "queued")])
        );
        assert_eq!(
            1,
            exceeded(&exporter, &[("operation", fetch), ("stage", "not_started")])
        );
    }

    /// If its deadline keeps a read from starting a later step, the read counts once, as
    /// `reading`, because it was under way.
    #[tokio::test(start_paused = true)]
    async fn counts_a_read_stopped_before_a_later_step_once_as_reading() {
        let (provider, exporter) = install_exporter();

        let mut deadline = fetch_deadline(Instant::now() + Duration::from_secs(1));

        assert_eq!(Ok(()), deadline.run_until_deadline(async {}).await);

        tokio::time::advance(Duration::from_secs(1)).await;

        assert_eq!(
            Err(TimedOut::Reading),
            deadline.run_until_deadline(async {}).await
        );
        assert_eq!(
            Err(TimedOut::Reading),
            deadline.run_until_deadline(async {}).await
        );

        provider.force_flush().expect("flush");

        assert_eq!(1, exceeded(&exporter, &[("stage", "reading")]));
        assert_eq!(0, exceeded(&exporter, &[("stage", "not_started")]));
    }

    /// Reads stopped during any marked wait count under the `queued` stage, and the `wait`
    /// attribute tells the waits apart.
    #[tokio::test(start_paused = true)]
    async fn counts_queued_reads_by_what_they_waited_for() {
        let (provider, exporter) = install_exporter();

        let in_a_second = || Instant::now() + Duration::from_secs(1);

        assert_eq!(
            Err(TimedOut::Queued(ReadWait::ConnectionReadLimit)),
            fetch_deadline(in_a_second())
                .run_until_deadline(pending::<()>().queued_for(ReadWait::ConnectionReadLimit))
                .await
        );
        assert_eq!(
            Err(TimedOut::Queued(ReadWait::EngineConnectionPool)),
            fetch_deadline(in_a_second())
                .run_until_deadline(pending::<()>().queued_for(ReadWait::EngineConnectionPool))
                .await
        );
        assert_eq!(
            Err(TimedOut::Queued(ReadWait::ClientReadLimit)),
            fetch_deadline(in_a_second())
                .run_until_deadline(pending::<()>().queued_for(ReadWait::ClientReadLimit))
                .await
        );
        assert_eq!(
            Err(TimedOut::Queued(ReadWait::EngineConnectionPool)),
            fetch_deadline(in_a_second())
                .run_until_deadline(pending::<()>().queued_for(ReadWait::EngineConnectionPool))
                .await
        );

        provider.force_flush().expect("flush");

        let fetch = StorageOperation::Fetch.as_str();

        assert_eq!(
            4,
            exceeded(&exporter, &[("operation", fetch), ("stage", "queued")])
        );
        assert_eq!(1, exceeded(&exporter, &[("wait", "connection_read_limit")]));
        assert_eq!(1, exceeded(&exporter, &[("wait", "client_read_limit")]));
        assert_eq!(
            2,
            exceeded(&exporter, &[("wait", "engine_connection_pool")])
        );
    }
}
