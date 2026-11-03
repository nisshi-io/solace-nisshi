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

//! Deadlines for storage reads made while answering a client request.
//!
//! A slow storage engine must not hold a request past the point where the
//! client gives up on it: the client reconnects and sends the request again,
//! while the abandoned one keeps running. Each client abandons a request at
//! its own read deadline:
//!
//! | Client     | ListOffsets | Fetch                          |
//! |------------|-------------|--------------------------------|
//! | Java       | 30s         | 30s (`max_wait` not added)     |
//! | librdkafka | 60s         | 60s + `fetch.wait.max.ms`      |
//! | franz-go   | 10s         | 10s + `max_wait`               |
//!
//! The deadlines here sit under all three with room for the response.

use std::{cell::Cell, future::Future, pin::pin, sync::LazyLock};

use opentelemetry::{KeyValue, metrics::Counter};
use tokio::time::{Duration, Instant, timeout_at};

use crate::METER;

/// How long ListOffsets may spend in storage, under franz-go's flat 10s.
///
/// A partition still unread at this deadline answers `REQUEST_TIMED_OUT`
/// with offset and timestamp -1. Kafka 4.0 sends the same answer for a
/// partition whose remote-storage lookup expires in its list-offsets
/// purgatory ([DelayedRemoteListOffsets.scala#L46], [#L144-L149]), so
/// clients already retry it.
///
/// [DelayedRemoteListOffsets.scala#L46]: https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/DelayedRemoteListOffsets.scala#L46
/// [#L144-L149]: https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/server/DelayedRemoteListOffsets.scala#L144-L149
pub(crate) const LIST_OFFSETS_READ_DEADLINE: Duration = Duration::from_secs(5);

static READ_DEADLINE_EXCEEDED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_storage_read_deadline_exceeded")
        .with_description("Storage reads abandoned at the request deadline")
        .build()
});

/// What a read bounded by [`bounded`] was doing when its deadline passed.
///
/// An operator reads it from the `stage` attribute of the
/// `nisshi_storage_read_deadline_exceeded` counter: `reading` points at a
/// slow storage engine, `queued` at a limit on reads in flight being used
/// up by slow reads, and `not_started` at a request with more partitions
/// than could be read before the deadline. A `queued` read also carries a
/// `limit` attribute: `engine` for its storage engine's limit, which slow
/// reads on any connection use up, and `connection` for its connection's
/// read share, which only that connection's own slow reads use up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Missed {
    /// The deadline had passed before the read was polled, so no storage
    /// call was made.
    NotStarted,
    /// The read was waiting in [`queued`] for its storage engine's limit on
    /// reads in flight, for a pooled connection, or for room on its request
    /// channel.
    Queued,
    /// The read was waiting in [`queued_for_connection_share`] for its
    /// connection's read share, see [`read_share`](super::read_share).
    QueuedForConnectionShare,
    /// The read was in storage.
    Reading,
}

impl Missed {
    pub(crate) fn stage(self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::Queued | Self::QueuedForConnectionShare => "queued",
            Self::Reading => "reading",
        }
    }

    /// Returns the limit that a `queued` read waited for, or `None` for a
    /// read in another stage.
    pub(crate) fn limit(self) -> Option<&'static str> {
        match self {
            Self::Queued => Some("engine"),
            Self::QueuedForConnectionShare => Some("connection"),
            Self::NotStarted | Self::Reading => None,
        }
    }

    /// Returns the attributes of the `nisshi_storage_read_deadline_exceeded`
    /// counter for this read of `operation`.
    pub(crate) fn attributes(self, operation: &'static str) -> Vec<KeyValue> {
        let mut attributes = vec![
            KeyValue::new("operation", operation),
            KeyValue::new("stage", self.stage()),
        ];
        attributes.extend(self.limit().map(|limit| KeyValue::new("limit", limit)));
        attributes
    }
}

tokio::task_local! {
    /// What the read that the enclosing [`bounded`] bounds is waiting for,
    /// while it is in [`queued`] or [`queued_for_connection_share`].
    static QUEUED: Cell<Option<Missed>>;
}

/// Runs `wait`, a storage engine's wait for its own limit on reads in
/// flight, for a pooled connection, or for room on its request channel, so
/// that a read abandoned during it is counted under the `queued` stage
/// rather than `reading`.
///
/// The stage is a task-local of the task that called the [`Storage`]
/// method, so `wait` counts only when it is awaited on that task. A wait on
/// a task the engine spawns, such as the task that serves the request
/// channel in `mpsc` mode, is not seen and counts as `reading`.
///
/// Outside a read that a deadline bounds, this only runs `wait`.
///
/// [`Storage`]: crate::Storage
pub async fn queued<F>(wait: F) -> F::Output
where
    F: Future,
{
    queued_as(Missed::Queued, wait).await
}

/// Runs `wait`, a read's wait for its connection's read share, so that a
/// read abandoned during it is counted under the `queued` stage with the
/// `connection` limit, see [`Missed`].
pub(crate) async fn queued_for_connection_share<F>(wait: F) -> F::Output
where
    F: Future,
{
    queued_as(Missed::QueuedForConnectionShare, wait).await
}

/// Runs `wait`, so that a read abandoned during it is counted as `missed`.
async fn queued_as<F>(missed: Missed, wait: F) -> F::Output
where
    F: Future,
{
    _ = QUEUED.try_with(|queued| queued.set(Some(missed)));
    let output = wait.await;
    _ = QUEUED.try_with(|queued| queued.set(None));
    output
}

/// Runs `read` until `deadline`, returning what it was doing if it did not
/// finish.
///
/// A read is not started at all once the deadline has passed:
/// [`tokio::time::timeout_at`] polls its future once before looking at the
/// clock, which would send a storage request only to drop it.
///
/// Dropping the read cancels nisshi's side of it. Work the storage engine
/// runs on its own tasks, or a statement already sent to a database server,
/// carries on until it finishes.
pub(crate) async fn bounded<F>(deadline: Instant, read: F) -> Result<F::Output, Missed>
where
    F: Future,
{
    if Instant::now() >= deadline {
        return Err(Missed::NotStarted);
    }

    let mut read = pin!(QUEUED.scope(Cell::new(None), timeout_at(deadline, read)));

    match read.as_mut().await {
        Ok(output) => Ok(output),
        Err(_elapsed) => Err(read
            .take_value()
            .and_then(Cell::into_inner)
            .unwrap_or(Missed::Reading)),
    }
}

/// Runs [`bounded`], counting each abandoned read in the
/// `nisshi_storage_read_deadline_exceeded` counter under `operation`.
pub(crate) async fn within<F>(
    operation: &'static str,
    deadline: Instant,
    read: F,
) -> Result<F::Output, Missed>
where
    F: Future,
{
    within_counting(&READ_DEADLINE_EXCEEDED, operation, deadline, read).await
}

/// Runs [`within`], counting each abandoned read in `exceeded`.
async fn within_counting<F>(
    exceeded: &Counter<u64>,
    operation: &'static str,
    deadline: Instant,
    read: F,
) -> Result<F::Output, Missed>
where
    F: Future,
{
    let outcome = bounded(deadline, read).await;

    if let Err(missed) = outcome {
        exceeded.add(1, &missed.attributes(operation));
    }

    outcome
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

    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::{
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        data::{AggregatedMetrics, MetricData, ResourceMetrics, ScopeMetrics, SumDataPoint},
    };

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn finishes_before_the_deadline() {
        let deadline = Instant::now() + Duration::from_secs(1);
        assert_eq!(Ok(7), within("test", deadline, async { 7 }).await);
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_at_the_deadline() {
        let started_at = Instant::now();
        let deadline = started_at + Duration::from_secs(1);

        assert_eq!(
            Err(Missed::Reading),
            within("test", deadline, pending::<()>()).await
        );
        assert_eq!(Duration::from_secs(1), started_at.elapsed());
    }

    /// Once the deadline has passed, the read is never polled.
    #[tokio::test(start_paused = true)]
    async fn does_not_start_after_the_deadline() {
        let deadline = Instant::now();
        tokio::time::advance(Duration::from_millis(1)).await;

        let polled = Arc::new(AtomicBool::new(false));

        let read = {
            let polled = polled.clone();
            async move { polled.store(true, Ordering::SeqCst) }
        };

        assert_eq!(
            Err(Missed::NotStarted),
            within("test", deadline, read).await
        );
        assert!(!polled.load(Ordering::SeqCst));
    }

    /// A read abandoned while it waits in `queued` is a queued miss; one
    /// whose wait finished before the deadline is a reading miss.
    #[tokio::test(start_paused = true)]
    async fn tells_a_queued_read_from_a_reading_one() {
        let deadline = Instant::now() + Duration::from_secs(1);

        assert_eq!(
            Err(Missed::Queued),
            within("test", deadline, queued(pending::<()>())).await
        );

        let deadline = Instant::now() + Duration::from_secs(1);

        assert_eq!(
            Err(Missed::Reading),
            within("test", deadline, async {
                queued(async {}).await;
                pending::<()>().await
            })
            .await
        );
    }

    /// Two reads bounded at once each keep their own stage.
    #[tokio::test(start_paused = true)]
    async fn reads_bounded_at_once_keep_their_own_stage() {
        let deadline = Instant::now() + Duration::from_secs(1);

        let (queued_read, reading) = tokio::join!(
            within("test", deadline, queued(pending::<()>())),
            within("test", deadline, pending::<()>())
        );

        assert_eq!(Err(Missed::Queued), queued_read);
        assert_eq!(Err(Missed::Reading), reading);
    }

    /// Sums the `nisshi_storage_read_deadline_exceeded` counter for
    /// `operation` and `stage`.
    fn exceeded(exporter: &InMemoryMetricExporter, operation: &str, stage: &str) -> u64 {
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
                            let has = |key: &str, value: &str| {
                                point.attributes().any(|attribute| {
                                    attribute.key.as_str() == key
                                        && attribute.value.as_str() == value
                                })
                            };

                            has("operation", operation) && has("stage", stage)
                        })
                        .map(SumDataPoint::value)
                        .sum::<u64>(),
                ),
                _ => None,
            })
            .sum()
    }

    /// Each abandoned read adds one to the counter under its stage, and a
    /// read that finishes in time adds nothing.
    #[tokio::test(start_paused = true)]
    async fn counts_each_abandoned_read_by_stage() {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let counter = provider
            .meter("test")
            .u64_counter("nisshi_storage_read_deadline_exceeded")
            .build();

        let deadline = || Instant::now() + Duration::from_secs(1);

        assert_eq!(
            Ok(7),
            within_counting(&counter, "on_time", deadline(), async { 7 }).await
        );
        assert_eq!(
            Err(Missed::Reading),
            within_counting(&counter, "stalled", deadline(), pending::<()>()).await
        );
        assert_eq!(
            Err(Missed::Reading),
            within_counting(&counter, "stalled", deadline(), pending::<()>()).await
        );
        assert_eq!(
            Err(Missed::Queued),
            within_counting(&counter, "stalled", deadline(), queued(pending::<()>())).await
        );
        assert_eq!(
            Err(Missed::NotStarted),
            within_counting(&counter, "stalled", Instant::now(), async {}).await
        );

        provider.force_flush().expect("flush");

        assert_eq!(0, exceeded(&exporter, "on_time", "reading"));
        assert_eq!(2, exceeded(&exporter, "stalled", "reading"));
        assert_eq!(1, exceeded(&exporter, "stalled", "queued"));
        assert_eq!(1, exceeded(&exporter, "stalled", "not_started"));
    }

    /// A read abandoned while it waits for its connection's read share and
    /// one abandoned while it waits for its engine's limit both count under
    /// the `queued` stage, and the `limit` attribute tells them apart.
    #[tokio::test(start_paused = true)]
    async fn counts_queued_reads_by_the_limit_they_waited_for() {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let counter = provider
            .meter("test")
            .u64_counter("nisshi_storage_read_deadline_exceeded")
            .build();

        let deadline = || Instant::now() + Duration::from_secs(1);

        assert_eq!(
            Err(Missed::QueuedForConnectionShare),
            within_counting(
                &counter,
                "stalled",
                deadline(),
                queued_for_connection_share(pending::<()>())
            )
            .await
        );
        assert_eq!(
            Err(Missed::Queued),
            within_counting(&counter, "stalled", deadline(), queued(pending::<()>())).await
        );
        assert_eq!(
            Err(Missed::Queued),
            within_counting(&counter, "stalled", deadline(), queued(pending::<()>())).await
        );

        provider.force_flush().expect("flush");

        assert_eq!(3, exceeded(&exporter, "stalled", "queued"));
        assert_eq!(1, exceeded_waiting_for(&exporter, "connection"));
        assert_eq!(2, exceeded_waiting_for(&exporter, "engine"));
    }

    /// Sums the `nisshi_storage_read_deadline_exceeded` counter for reads
    /// abandoned while they waited for `limit`.
    fn exceeded_waiting_for(exporter: &InMemoryMetricExporter, limit: &str) -> u64 {
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
                            point.attributes().any(|attribute| {
                                attribute.key.as_str() == "limit"
                                    && attribute.value.as_str() == limit
                            })
                        })
                        .map(SumDataPoint::value)
                        .sum::<u64>(),
                ),
                _ => None,
            })
            .sum()
    }
}
