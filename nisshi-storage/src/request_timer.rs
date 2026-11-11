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

use opentelemetry::{KeyValue, metrics::Histogram};
use tokio::time::Instant;

use crate::StorageOperation;

/// Records how long a request ran in a histogram, also when the caller drops the request's
/// future before it returns.
///
/// A duration recorded after the future returns leaves out every request that a deadline drops,
/// and those are the slowest requests. This timer records such a request when it is dropped, with
/// the attribute `outcome="stopped"`. A request that returns is recorded with
/// `outcome="finished"`, and a request that fails is not recorded.
#[derive(Debug)]
pub struct RequestTimer {
    histogram: &'static Histogram<u64>,
    operation: StorageOperation,
    started: Instant,
    outcome: Option<&'static str>,
}

impl RequestTimer {
    /// Times a request of `operation` that started at `started`, for `histogram`.
    pub fn new(
        histogram: &'static Histogram<u64>,
        operation: StorageOperation,
        started: Instant,
    ) -> Self {
        Self {
            histogram,
            operation,
            started,
            outcome: Some("stopped"),
        }
    }

    /// Ends the timer with the request's `result`: records the duration if the request returned
    /// `Ok`, and records nothing if it failed.
    pub fn end<T, E>(mut self, result: &Result<T, E>) {
        self.outcome = result.is_ok().then_some("finished");
    }
}

impl Drop for RequestTimer {
    fn drop(&mut self) {
        if let Some(outcome) = self.outcome {
            let elapsed_millis =
                u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);

            self.histogram.record(
                elapsed_millis,
                &[
                    self.operation.attribute(),
                    KeyValue::new("outcome", outcome),
                ],
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{future::pending, sync::LazyLock, time::Duration};

    use opentelemetry::global;
    use opentelemetry_sdk::metrics::{
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        data::{AggregatedMetrics, HistogramDataPoint, MetricData, ResourceMetrics, ScopeMetrics},
    };
    use tokio::time::{sleep, timeout};

    use super::*;
    use crate::{Error, METER};

    static DURATION: LazyLock<Histogram<u64>> =
        LazyLock::new(|| METER.u64_histogram("request_timer_test_duration").build());

    /// Installs an in-memory exporter as the global meter provider. A test calls this before its
    /// first use of `DURATION`, which creates the histogram. nextest runs each test in a process of
    /// its own, so the histogram is created after this in every test that calls it.
    fn install_exporter() -> (SdkMeterProvider, InMemoryMetricExporter) {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        global::set_meter_provider(provider.clone());
        (provider, exporter)
    }

    /// Returns the `outcome` and the sum of each data point of `DURATION`.
    fn recorded(
        provider: &SdkMeterProvider,
        exporter: &InMemoryMetricExporter,
    ) -> Vec<(String, u64)> {
        provider.force_flush().expect("flush");

        exporter
            .get_finished_metrics()
            .expect("finished metrics")
            .iter()
            .flat_map(ResourceMetrics::scope_metrics)
            .flat_map(ScopeMetrics::metrics)
            .filter(|metric| metric.name() == "request_timer_test_duration")
            .flat_map(|metric| match metric.data() {
                AggregatedMetrics::U64(MetricData::Histogram(histogram)) => histogram
                    .data_points()
                    .map(|point: &HistogramDataPoint<u64>| {
                        let outcome = point
                            .attributes()
                            .find(|attribute| attribute.key.as_str() == "outcome")
                            .map(|attribute| attribute.value.to_string())
                            .unwrap_or_default();
                        (outcome, point.sum())
                    })
                    .collect::<Vec<_>>(),
                _ => vec![],
            })
            .collect()
    }

    async fn timed_request(result: Result<(), Error>, runs_for: Duration) -> Result<(), Error> {
        let timer = RequestTimer::new(&DURATION, StorageOperation::Fetch, Instant::now());
        sleep(runs_for).await;
        timer.end(&result);
        result
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_that_returns_is_recorded_as_finished() {
        let (provider, exporter) = install_exporter();

        _ = timed_request(Ok(()), Duration::from_millis(30)).await;

        assert_eq!(
            vec![("finished".to_owned(), 30)],
            recorded(&provider, &exporter)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_that_fails_is_not_recorded() {
        let (provider, exporter) = install_exporter();

        _ = timed_request(Err(Error::UnableToSend), Duration::from_millis(30)).await;

        assert_eq!(Vec::<(String, u64)>::new(), recorded(&provider, &exporter));
    }

    /// A request whose caller drops it at a deadline is recorded as stopped, with the time it ran
    /// until the drop.
    #[tokio::test(start_paused = true)]
    async fn a_request_dropped_at_its_deadline_is_recorded_as_stopped() {
        let (provider, exporter) = install_exporter();

        let request = async {
            let _timer = RequestTimer::new(&DURATION, StorageOperation::Fetch, Instant::now());
            pending::<()>().await;
        };
        _ = timeout(Duration::from_millis(50), request).await;

        assert_eq!(
            vec![("stopped".to_owned(), 50)],
            recorded(&provider, &exporter)
        );
    }
}
