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

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
use core::{
    fmt::{self, Debug, Display},
    result,
};
use std::{
    io::{self, Write as _},
    marker::PhantomData,
    num::{NonZero, NonZeroU32},
    ops::AddAssign,
    pin::Pin,
    sync::{Arc, LazyLock, Mutex, PoisonError},
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use governor::{DefaultDirectRateLimiter, InsufficientCapacity, Jitter, Quota, RateLimiter};
use human_units::{
    FormatDuration,
    iec::{Byte, Prefix},
};
use nisshi_client::{Client, ConnectionManager, Security};
use nisshi_sans_io::{
    Ack, ByteSize as _, ErrorCode, ProduceRequest,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated},
};
use nonzero_ext::nonzero;
use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Meter},
};
use opentelemetry_otlp::ExporterBuildError;
use opentelemetry_sdk::{
    error::{OTelSdkError, OTelSdkResult},
    metrics::{
        SdkMeterProvider, Temporality,
        data::{AggregatedMetrics, Histogram, Metric, MetricData, ResourceMetrics},
        exporter::PushMetricExporter,
    },
};
use opentelemetry_semantic_conventions::SCHEMA_URL;
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};
use tokio::{task::JoinSet, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument, warn};
use url::Url;

pub type Result<T, E = Error> = result::Result<T, E>;

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

#[derive(thiserror::Error, Debug)]
pub enum Error {
    Api(ErrorCode),
    Client(#[from] nisshi_client::Error),
    ExporterBuild(#[from] ExporterBuildError),
    InsufficientCapacity(#[from] InsufficientCapacity),
    Io(Arc<io::Error>),
    OtelSdk(#[from] OTelSdkError),
    Random(#[from] getrandom::Error),
    Poison,
    Protocol(#[from] nisshi_sans_io::Error),
    UnknownHost(String),
    Url(#[from] url::ParseError),
}

impl Error {
    /// Whether the broker refused the login, the TLS connection or a produce request, or the
    /// client refused what the broker offered. The run ends with the first refusal, a transient
    /// error code such as `KafkaStorageError` or `RequestTimedOut` included. Kafka's own producer
    /// retries those, and this tool does not: a retry would hide an outage of the broker or its
    /// storage in the numbers of a run that measures the broker. The exit status and the code
    /// on stderr show it instead.
    fn is_refusal(&self) -> bool {
        match self {
            Self::Api(_) => true,
            Self::Client(error) => error.failure_kind() == nisshi_client::FailureKind::Refused,
            _ => false,
        }
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CancelKind {
    Interrupt,
    Terminate,
    Timeout,
}

/// How the report of a run is printed on stdout.
#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ReportFormat {
    /// A line of text for a person to read, for each interval and one for the whole run, which
    /// starts with `summary: `.
    #[default]
    Text,

    /// One JSON object on a line for each interval, with `"type": "interval"`, and a last one
    /// for the whole run, with `"type": "summary"` and the `error_code` that ended it.
    Json,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Perf {
    broker: Url,
    security: Security,
    topic: String,
    partition: i32,
    batch_size: u32,
    record_size: usize,
    per_second: Option<u32>,
    throughput: Option<u32>,
    producers: u32,
    duration: Option<Duration>,
    report: ReportFormat,
}

#[derive(Clone, Debug)]
pub struct Builder<B, T> {
    broker: B,
    security: Security,
    topic: T,
    partition: i32,
    batch_size: u32,
    record_size: usize,
    per_second: Option<u32>,
    throughput: Option<u32>,
    producers: u32,
    duration: Option<Duration>,
    report: ReportFormat,
}

impl Default for Builder<PhantomData<Url>, PhantomData<String>> {
    fn default() -> Self {
        Self {
            broker: Default::default(),
            security: Default::default(),
            topic: Default::default(),
            partition: Default::default(),
            batch_size: 1,
            record_size: 1024,
            per_second: None,
            throughput: None,
            producers: 1,
            duration: None,
            report: ReportFormat::default(),
        }
    }
}

impl<B, T> Builder<B, T> {
    /// How the connection to the broker is secured, by default not at all.
    pub fn security(self, security: Security) -> Self {
        Self { security, ..self }
    }

    pub fn broker(self, broker: impl Into<Url>) -> Builder<Url, T> {
        Builder {
            broker: broker.into(),
            security: self.security,
            topic: self.topic,
            partition: self.partition,
            batch_size: self.batch_size,
            record_size: self.record_size,
            per_second: self.per_second,
            throughput: self.throughput,
            producers: self.producers,
            duration: self.duration,
            report: self.report,
        }
    }

    pub fn topic(self, topic: impl Into<String>) -> Builder<B, String> {
        Builder {
            broker: self.broker,
            security: self.security,
            topic: topic.into(),
            partition: self.partition,
            batch_size: self.batch_size,
            record_size: self.record_size,
            per_second: self.per_second,
            throughput: self.throughput,
            producers: self.producers,
            duration: self.duration,
            report: self.report,
        }
    }

    pub fn partition(self, partition: i32) -> Builder<B, T> {
        Self { partition, ..self }
    }

    pub fn batch_size(self, batch_size: u32) -> Self {
        Self { batch_size, ..self }
    }

    pub fn record_size(self, record_size: usize) -> Self {
        Self {
            record_size,
            ..self
        }
    }

    pub fn per_second(self, per_second: Option<u32>) -> Self {
        Self { per_second, ..self }
    }

    pub fn throughput(self, throughput: Option<u32>) -> Self {
        Self { throughput, ..self }
    }

    pub fn producers(self, producers: u32) -> Self {
        Self { producers, ..self }
    }

    pub fn duration(self, duration: Option<Duration>) -> Self {
        Self { duration, ..self }
    }

    /// How to print the report on stdout, by default as text.
    pub fn report(self, report: ReportFormat) -> Self {
        Self { report, ..self }
    }
}

impl Builder<Url, String> {
    pub fn build(self) -> Perf {
        Perf {
            broker: self.broker,
            security: self.security,
            topic: self.topic,
            partition: self.partition,
            batch_size: self.batch_size,
            record_size: self.record_size,
            per_second: self.per_second,
            throughput: self.throughput,
            producers: self.producers,
            duration: self.duration,
            report: self.report,
        }
    }
}

static RATE_LIMIT_DURATION: LazyLock<opentelemetry::metrics::Histogram<u64>> =
    LazyLock::new(|| {
        METER
            .u64_histogram("rate_limit_duration")
            .with_unit("ms")
            .with_description("Rate limit latencies in milliseconds")
            .build()
    });

static PRODUCE_RECORD_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("produce_record_count")
        .with_description("Produced record count")
        .build()
});

static PRODUCE_API_DURATION: LazyLock<opentelemetry::metrics::Histogram<u64>> =
    LazyLock::new(|| {
        METER
            .u64_histogram("produce_duration")
            .with_unit("ms")
            .with_description("Produce API latencies in milliseconds")
            .build()
    });

impl Perf {
    pub fn builder() -> Builder<PhantomData<Url>, PhantomData<String>> {
        Builder::default()
    }

    pub async fn main(self) -> Result<ErrorCode> {
        let token = CancellationToken::new();
        let started_at = SystemTime::now();
        let totals = Arc::new(Mutex::new(None));

        let meter_provider = {
            let exporter =
                MetricExporter::new(started_at, token.clone(), totals.clone(), self.report);
            let meter_provider = SdkMeterProvider::builder()
                .with_periodic_exporter(exporter)
                .build();
            global::set_meter_provider(meter_provider.clone());

            meter_provider
        };

        #[cfg(unix)]
        let mut interrupt_signal = signal(SignalKind::interrupt())?;
        #[cfg(windows)]
        let mut interrupt_signal = tokio::signal::windows::ctrl_c()?;
        debug!(?interrupt_signal);

        #[cfg(unix)]
        let mut terminate_signal = signal(SignalKind::terminate())?;
        // Windows has no SIGTERM for a console program, so this listens for the nearest event: the
        // user closing the console window, or ending the task in Task Manager. A service manager's
        // stop request doesn't send it. Windows ends the process 5 seconds after the event by
        // default, whether or not shutdown has finished.
        #[cfg(windows)]
        let mut terminate_signal = tokio::signal::windows::ctrl_close()?;
        debug!(?terminate_signal);

        let rate_limiter = self
            .per_second
            .or(self.throughput)
            .inspect(|limit| debug!(?limit))
            .and_then(NonZeroU32::new)
            .map(Quota::per_second)
            .map(RateLimiter::direct)
            .map(Arc::new)
            .inspect(|rate_limiter| debug!(?rate_limiter));

        let record_data = {
            let mut data = vec![0u8; self.record_size];
            getrandom::fill(&mut data)?;
            Bytes::from(data)
        };

        let batch_size = NonZeroU32::new(self.batch_size)
            .inspect(|batch_size| debug!(batch_size = batch_size.get()))
            .unwrap_or(nonzero!(10u32));

        let mut set = JoinSet::new();

        let client = ConnectionManager::builder(self.broker)
            .security(self.security.clone())
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        for id in 0..self.producers {
            let producer = Producer {
                id,
                rate_limiter: rate_limiter.clone(),
                topic: self.topic.clone(),
                partition: self.partition,
                record_data: record_data.clone(),
                token: token.clone(),
                client: client.clone(),
                batch_size,
                throughput: self.throughput,
            };

            _ = set.spawn(async move {
                loop {
                    match producer.rate_limited().await {
                        Ok(false) => return Ok(()),
                        Err(error) if error.is_refusal() => return Err(error),
                        Err(_) => return Ok(()),
                        Ok(true) => continue,
                    }
                }
            });
        }

        // Ends when every producer has ended, or with the error of the first producer that
        // ended with one.
        let join_all = async {
            while !set.is_empty() {
                debug!(len = set.len());

                if let Some(Ok(Err(error))) = set.join_next().await {
                    return Some(error);
                }
            }

            None
        };

        let mut failure = None;

        let duration = self
            .duration
            .map(sleep)
            .map(Box::pin)
            .map(|pinned| pinned as Pin<Box<dyn Future<Output = ()>>>)
            .unwrap_or(Box::pin(std::future::pending()) as Pin<Box<dyn Future<Output = ()>>>);

        let cancellation = tokio::select! {

            timeout = duration => {
                debug!(?timeout);
                token.cancel();
                Some(CancelKind::Timeout)
            }

            completed = join_all => {
                debug!(?completed);

                if completed.is_some() {
                    token.cancel();
                }

                failure = completed;
                None
            }

            interrupt = interrupt_signal.recv() => {
                debug!(?interrupt);
                Some(CancelKind::Interrupt)
            }

            terminate = terminate_signal.recv() => {
                debug!(?terminate);
                Some(CancelKind::Terminate)
            }

        };

        debug!(?cancellation);

        // The producers stop here however the run ended, an interrupt or a signal included, so
        // that the summary counts what was produced up to `ended_at`.
        token.cancel();
        let ended_at = SystemTime::now();

        // The abort ends each producer at its next await, whether or not that await races the
        // token, so this drain can't hang on an await that a later change adds. A producer
        // counts a batch in the same poll as the produce that returned it, so the abort loses
        // no count. The drain waits for the request each producer had in flight to be dropped,
        // and for nothing else.
        set.abort_all();
        while set.join_next().await.is_some() {}

        // The shutdown exports the counters one last time, into `totals`, so the summary below
        // sees every batch that was counted.
        let shut_down = meter_provider
            .shutdown()
            .inspect(|shutdown| debug!(?shutdown));

        let error_code = match &failure {
            Some(Error::Api(code)) => *code,
            _ => ErrorCode::None,
        };

        let summary = summary(
            started_at,
            ended_at,
            totals
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .unwrap_or_default(),
        );
        print_report(self.report, &summary, ReportKind::Summary { error_code });

        // A failed shutdown stands as the run's error only when no refusal is held already,
        // because a refusal names what the broker did, and that is the more specific answer.
        if let Err(error) = shut_down {
            warn!(?error, "the metrics of the run were not exported");

            if failure.is_none() {
                failure = Some(error.into());
            }
        }

        if let Some(CancelKind::Timeout) = cancellation {
            sleep(Duration::from_secs(5)).await;
        }

        failure.map_or(Ok(ErrorCode::None), Err)
    }
}

/// The report of the whole run, from `started_at` to `ended_at`, over the last exported
/// `totals`.
fn summary(started_at: SystemTime, ended_at: SystemTime, totals: ObservationLatency) -> Info {
    let mut info = Info::new(started_at);
    info.current = ObservationLatency {
        observation: Observation {
            taken_at: ended_at,
            ..totals.observation
        },
        latency: totals.latency,
    };
    info
}

/// The line of the report for `info`, as `report` prints it.
fn report_line(report: ReportFormat, info: &Info, kind: ReportKind) -> String {
    match (report, kind) {
        (ReportFormat::Text, ReportKind::Interval) => info.to_string(),
        (ReportFormat::Text, ReportKind::Summary { .. }) => format!("summary: {info}"),
        (ReportFormat::Json, kind) => info.json_line(kind),
    }
}

/// Prints a line of the report. A closed stdout does not fail the run.
fn print_report(report: ReportFormat, info: &Info, kind: ReportKind) {
    _ = writeln!(io::stdout(), "{}", report_line(report, info, kind))
        .inspect_err(|error| debug!(?error));
}

#[derive(Clone, Debug)]
struct Producer {
    id: u32,
    rate_limiter: Option<Arc<DefaultDirectRateLimiter>>,
    topic: String,
    partition: i32,
    record_data: Bytes,
    token: CancellationToken,
    client: Client,
    batch_size: NonZero<u32>,
    throughput: Option<u32>,
}

impl Producer {
    #[instrument(skip_all, fields(record_data_len = self.record_data.len()))]
    fn frame(&self) -> Result<deflated::Frame> {
        let mut batch = inflated::Batch::builder();
        let offset_deltas = 0..(self.batch_size.get() as i32);

        for offset_delta in offset_deltas {
            batch = batch.record(
                Record::builder()
                    .value(Some(self.record_data.clone()))
                    .offset_delta(offset_delta),
            )
        }

        batch
            .last_offset_delta(self.batch_size.get() as i32 - 1)
            .build()
            .map(|batch| inflated::Frame {
                batches: vec![batch],
            })
            .and_then(deflated::Frame::try_from)
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn produce(&self, frame: deflated::Frame) -> Result<()> {
        let req = ProduceRequest::default()
            .acks(Ack::Leader.into())
            .topic_data(Some(
                [TopicProduceData::default()
                    .name(self.topic.clone())
                    .partition_data(Some(
                        [PartitionProduceData::default()
                            .index(self.partition)
                            .records(Some(frame))]
                        .into(),
                    ))]
                .into(),
            ));

        let response = self.client.call(req).await?;

        // The broker answers a produce request that it refuses with an error code in the
        // partition response, not with a failed call.
        for topic in response.responses.unwrap_or_default() {
            for partition in topic.partition_responses.unwrap_or_default() {
                if partition.error_code != i16::from(ErrorCode::None) {
                    let code = ErrorCode::try_from(partition.error_code)
                        .unwrap_or(ErrorCode::UnknownServerError);

                    error!(topic = %topic.name, partition = partition.index, ?code, error_code = partition.error_code, error_message = ?partition.error_message, "the broker refused the produce request");

                    return Err(Error::Api(code));
                }
            }
        }

        Ok(())
    }

    #[instrument(skip_all, fields(id = self.id))]
    async fn rate_limited(&self) -> Result<bool> {
        let attributes = [KeyValue::new("producer", self.id.to_string())];

        let frame = self.frame()?;

        if let Some(ref rate_limiter) = self.rate_limiter {
            let rate_limit_start = SystemTime::now();

            let cells = self
                .throughput
                .and(
                    frame
                        .size_in_bytes()
                        .ok()
                        .and_then(|bytes| NonZeroU32::new(bytes as u32)),
                )
                .unwrap_or(self.batch_size);

            tokio::select! {
                cancelled = self.token.cancelled() => {
                    debug!(?cancelled);
                    return Ok(false)
                },

                Ok(_) = rate_limiter.until_n_ready_with_jitter(cells, Jitter::up_to(Duration::from_millis(10))) => {
                    RATE_LIMIT_DURATION.record(
                    rate_limit_start
                        .elapsed()
                        .inspect(|duration|debug!(rate_limit_duration_ms = duration.as_millis()))
                        .map_or(0, |duration| duration.as_millis() as u64),
                        &attributes)

                },
            }
        }

        let produce_start = SystemTime::now();

        tokio::select! {
            // A produce request still waiting for its answer is dropped here, so the broker may
            // store a batch that the counters never see.
            cancelled = self.token.cancelled() => {
                debug!(?cancelled);
                return Ok(false)
            },

            produced = self.produce(frame) => match produced {
                Ok(()) => {
                    PRODUCE_RECORD_COUNT.add(self.batch_size.get() as u64, &attributes);
                    PRODUCE_API_DURATION.record(produce_start.elapsed().inspect(|duration|debug!(produce_duration_ms = duration.as_millis())).map_or(0, |duration| duration.as_millis() as u64), &attributes);
                }

                // The login, the TLS connection or the produce request was refused: end the run
                // with the error.
                Err(error) if error.is_refusal() => return Err(error),

                // Any other failure of a request leaves this producer idle until the run ends.
                Err(error) => {
                    debug!(?error);
                    self.token.cancelled().await;
                    return Ok(false)
                }
            },
        }

        Ok(!self.token.is_cancelled())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Observation {
    taken_at: SystemTime,
    bytes_sent: u64,
    record_count: u64,
}

impl AddAssign for Observation {
    fn add_assign(&mut self, rhs: Self) {
        self.taken_at = self.taken_at.max(rhs.taken_at);
        self.bytes_sent += rhs.bytes_sent;
        self.record_count += rhs.record_count;
    }
}

impl Default for Observation {
    fn default() -> Self {
        Self {
            taken_at: SystemTime::now(),
            bytes_sent: Default::default(),
            record_count: Default::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
struct Info {
    started_at: SystemTime,
    previous: Option<ObservationLatency>,
    current: ObservationLatency,
}

impl Info {
    fn new(started_at: SystemTime) -> Self {
        Self {
            started_at,
            current: Default::default(),
            previous: Default::default(),
        }
    }

    fn with_previous(self, previous: Option<ObservationLatency>) -> Self {
        Self { previous, ..self }
    }

    fn elapsed(&self) -> Duration {
        self.current
            .observation
            .taken_at
            .duration_since(
                self.previous
                    .map_or(self.started_at, |previous| previous.observation.taken_at),
            )
            .unwrap_or_default()
    }

    fn bytes_sent(&self) -> u64 {
        self.current.observation.bytes_sent
            - self
                .previous
                .map(|previous| previous.observation.bytes_sent)
                .unwrap_or_default()
    }

    /// The records of the batches that the broker accepted.
    fn records_produced(&self) -> u64 {
        self.current.observation.record_count
            - self
                .previous
                .map(|previous| previous.observation.record_count)
                .unwrap_or_default()
    }

    /// The rate over the elapsed time with its fraction, or `None` when no time has elapsed.
    fn records_produced_per_second(&self) -> Option<f64> {
        let seconds = self.elapsed().as_secs_f64();

        (seconds > 0.0).then(|| self.records_produced() as f64 / seconds)
    }

    /// The report as one line of JSON. It has `null` where there is nothing to divide by or
    /// nothing was sampled, never NaN or infinity. The latency is over the run so far, not over
    /// the interval: a minimum or a maximum can't be taken apart by interval.
    fn json_line(&self, kind: ReportKind) -> String {
        let seconds = self.elapsed().as_secs_f64();
        let per_second = |count: u64| (seconds > 0.0).then(|| count as f64 / seconds);
        let milliseconds =
            |duration: Option<Duration>| duration.map(|duration| duration.as_millis() as u64);

        let mut line = serde_json::json!({
            "type": kind.name(),
            "elapsed_ms": self.elapsed().as_millis() as u64,
            "records_produced": self.records_produced(),
            "bytes_sent": self.bytes_sent(),
            "records_per_second": per_second(self.records_produced()),
            "bytes_per_second": per_second(self.bytes_sent()),
            "latency_ms": {
                "min": milliseconds(self.current.latency.min),
                "mean": self.current.latency.mean,
                "max": milliseconds(self.current.latency.max),
            },
        });

        if let ReportKind::Summary { error_code } = kind {
            line["error_code"] = i16::from(error_code).into();
        }

        line.to_string()
    }

    /// The bytes per second over the elapsed time with its fraction, or `None` when no time has
    /// elapsed.
    fn bandwidth(&self) -> Option<Byte> {
        let seconds = self.elapsed().as_secs_f64();

        (seconds > 0.0).then(|| {
            Byte::with_iec_prefix((self.bytes_sent() as f64 / seconds) as u64, Prefix::None)
        })
    }
}

/// Which line of the report this is.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReportKind {
    /// What happened since the previous line.
    Interval,

    /// What happened in the whole run. `error_code` is the code of the produce request that
    /// ended it, [`ErrorCode::None`] when the broker refused none, or
    /// [`ErrorCode::UnknownServerError`] for a code this crate does not know.
    Summary { error_code: ErrorCode },
}

impl ReportKind {
    fn name(self) -> &'static str {
        match self {
            Self::Interval => "interval",
            Self::Summary { .. } => "summary",
        }
    }
}

impl Display for Info {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "elapsed: {}, {} records produced, {} records/s, ({}), latency: {} min, {:.1}ms avg, {} max",
            self.elapsed().format_duration(),
            self.records_produced(),
            self.records_produced_per_second()
                .map_or_else(|| "-".to_owned(), |rate| format!("{rate:.1}")),
            self.bandwidth().map_or_else(
                || "-".to_owned(),
                |bandwidth| format!("{}/s", bandwidth.format_iec())
            ),
            self.current
                .latency
                .min
                .map_or_else(|| "-".to_owned(), |min| min.format_duration().to_string()),
            self.current.latency.mean.unwrap_or_default(),
            self.current
                .latency
                .max
                .map_or_else(|| "-".to_owned(), |max| max.format_duration().to_string()),
        )
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, PartialOrd)]
struct Latency {
    min: Option<Duration>,
    max: Option<Duration>,
    mean: Option<f64>,
}

impl From<&Histogram<u64>> for Latency {
    fn from(histogram: &Histogram<u64>) -> Self {
        let min = histogram
            .data_points()
            .filter_map(|dp| dp.min())
            .min()
            .map(Duration::from_millis);

        let max = histogram
            .data_points()
            .filter_map(|dp| dp.max())
            .max()
            .map(Duration::from_millis);

        let sum = histogram.data_points().map(|dp| dp.sum()).sum::<u64>() as f64;
        let count = histogram.data_points().map(|dp| dp.count()).sum::<u64>() as f64;

        let mean = (count > 0.0).then(|| sum / count);

        Self { min, max, mean }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, PartialOrd)]
struct ObservationLatency {
    observation: Observation,
    latency: Latency,
}

/// Prints an interval line of the report on each export, and keeps the totals of the last export
/// for the summary that the run prints when it ends.
#[derive(Debug)]
struct MetricExporter {
    started_at: SystemTime,
    temporality: Temporality,
    /// The totals of the last export, since the start of the run: the metrics are cumulative.
    /// An interval line is the difference from the export before it.
    totals: Arc<Mutex<Option<ObservationLatency>>>,
    cancellation: CancellationToken,
    report: ReportFormat,
}

impl MetricExporter {
    fn new(
        started_at: SystemTime,
        cancellation: CancellationToken,
        totals: Arc<Mutex<Option<ObservationLatency>>>,
        report: ReportFormat,
    ) -> Self {
        Self {
            started_at,
            temporality: Default::default(),
            totals,
            cancellation,
            report,
        }
    }

    #[instrument(skip_all, fields(scope = scope.name(), metric = metric.name()))]
    fn info(&self, scope: &InstrumentationScope, metric: &Metric, info: &mut Info) {
        match (scope.name(), metric.name(), metric.data()) {
            ("nisshi-client", "tcp_bytes_sent", AggregatedMetrics::U64(MetricData::Sum(sum))) => {
                for (point, data) in sum.data_points().enumerate() {
                    debug!(point, value = ?data.value());
                }

                info.current.observation.bytes_sent =
                    sum.data_points().map(|sum| sum.value()).sum::<u64>();
            }

            (
                "nisshi-perf",
                "produce_record_count",
                AggregatedMetrics::U64(MetricData::Sum(sum)),
            ) => {
                for (point, data) in sum.data_points().enumerate() {
                    debug!(point, value = ?data.value());
                }

                info.current.observation.record_count =
                    sum.data_points().map(|sum| sum.value()).sum::<u64>();
            }

            (
                "nisshi-perf",
                "produce_duration",
                AggregatedMetrics::U64(MetricData::Histogram(histogram)),
            ) => {
                info.current.latency = Latency::from(histogram);
            }

            _ => (),
        }
    }
}

impl PushMetricExporter for MetricExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let mut totals = self
            .totals
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let mut info = Info::new(self.started_at).with_previous(*totals);

        for scope in metrics.scope_metrics() {
            debug!(scope = scope.scope().name());

            for metric in scope.metrics() {
                debug!(scope = scope.scope().name(), metric = metric.name());

                self.info(scope.scope(), metric, &mut info);
            }
        }

        // Once the run has ended, an export only updates the totals for the summary: the last
        // interval is in the summary, and a line here would follow it on a timer export that
        // raced the shutdown.
        if !self.cancellation.is_cancelled() {
            print_report(self.report, &info, ReportKind::Interval);
        }

        _ = totals.replace(info.current);

        Ok(())
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    #[instrument]
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        self.temporality
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn info_display_has_no_rate_without_elapsed_time() {
        let now = SystemTime::now();
        let mut info = Info::new(now);
        info.current.observation.taken_at = now;
        info.current.observation.record_count = 5;
        info.current.observation.bytes_sent = 500;

        assert_eq!(None, info.records_produced_per_second());
        assert_eq!(None, info.bandwidth().map(|bandwidth| bandwidth.0));
        let line = info.to_string();
        assert!(
            line.contains("5 records produced, - records/s, (-)"),
            "{line}"
        );
        assert!(!line.contains("NaN") && !line.contains("inf"), "{line}");
    }

    #[test]
    fn info_display_has_a_rate_for_less_than_a_second() {
        let now = SystemTime::now();
        let mut info = Info::new(now.checked_sub(Duration::from_millis(500)).unwrap());
        info.current.observation.taken_at = now;
        info.current.observation.record_count = 50;
        info.current.observation.bytes_sent = 51_200;

        assert_eq!(Some(100.0), info.records_produced_per_second());
        // Whole seconds would round the half second down to zero and show 0 B/s beside a
        // records/s of 100.
        assert_eq!(Some(102_400), info.bandwidth().map(|bandwidth| bandwidth.0));
        let line = info.to_string();
        assert!(line.contains("100.0 records/s, (100 KiB/s)"), "{line}");
    }

    #[test]
    fn info_display_without_latency_samples() {
        let info = Info::new(SystemTime::now());
        let line = info.to_string();
        assert!(line.contains("latency: - min"), "{line}");
        assert!(line.ends_with("- max"), "{line}");
    }

    #[test]
    fn json_report_is_valid_without_samples_or_elapsed_time() {
        let now = SystemTime::now();
        let mut info = Info::new(now);
        // The default observation is taken when it is built, a moment after `now`.
        info.current.observation.taken_at = now;

        for kind in [
            ReportKind::Interval,
            ReportKind::Summary {
                error_code: ErrorCode::None,
            },
        ] {
            let json: serde_json::Value = serde_json::from_str(&info.json_line(kind)).unwrap();

            assert_eq!(kind.name(), json["type"]);
            assert_eq!(0, json["records_produced"]);
            assert!(json["records_per_second"].is_null(), "{json}");
            assert!(json["latency_ms"]["min"].is_null(), "{json}");
            assert!(json["latency_ms"]["mean"].is_null(), "{json}");
        }
    }

    #[test]
    fn json_report_has_rates_per_second() {
        let now = SystemTime::now();
        let mut info = Info::new(now.checked_sub(Duration::from_secs(4)).unwrap());
        info.current = ObservationLatency {
            observation: Observation {
                taken_at: now,
                bytes_sent: 65_456,
                record_count: 656,
            },
            latency: Latency {
                min: Some(Duration::from_millis(2)),
                max: Some(Duration::from_millis(9)),
                mean: Some(4.5),
            },
        };

        let json: serde_json::Value = serde_json::from_str(&info.json_line(ReportKind::Summary {
            error_code: ErrorCode::None,
        }))
        .unwrap();

        assert_eq!(4000, json["elapsed_ms"]);
        assert_eq!(656, json["records_produced"]);
        assert_eq!(164.0, json["records_per_second"]);
        assert_eq!(16_364.0, json["bytes_per_second"]);
        assert_eq!(2, json["latency_ms"]["min"]);
        assert_eq!(4.5, json["latency_ms"]["mean"]);
        assert_eq!(9, json["latency_ms"]["max"]);
        assert_eq!(0, json["error_code"], "{json}");
    }

    #[test]
    fn the_summary_has_the_error_code_that_ended_the_run() {
        let info = Info::new(SystemTime::now());
        let kind = ReportKind::Summary {
            error_code: ErrorCode::UnknownTopicOrPartition,
        };

        let json: serde_json::Value = serde_json::from_str(&info.json_line(kind)).unwrap();
        assert_eq!("summary", json["type"]);
        assert_eq!(3, json["error_code"], "{json}");

        let interval: serde_json::Value =
            serde_json::from_str(&info.json_line(ReportKind::Interval)).unwrap();
        assert!(interval.get("error_code").is_none(), "{interval}");
    }

    #[test]
    fn the_text_summary_line_says_which_line_it_is() {
        let info = Info::new(SystemTime::now());
        let kind = ReportKind::Summary {
            error_code: ErrorCode::None,
        };

        let line = report_line(ReportFormat::Text, &info, kind);
        assert_eq!(
            Some(info.to_string().as_str()),
            line.strip_prefix("summary: ")
        );

        let interval = report_line(ReportFormat::Text, &info, ReportKind::Interval);
        assert_eq!(info.to_string(), interval);
    }

    #[test]
    fn the_summary_covers_the_run_over_the_last_totals() {
        let now = SystemTime::now();
        let started_at = now.checked_sub(Duration::from_secs(4)).unwrap();
        let totals = ObservationLatency {
            observation: Observation {
                // The export that took the totals was earlier than the run's end.
                taken_at: now.checked_sub(Duration::from_secs(1)).unwrap(),
                bytes_sent: 65_456,
                record_count: 656,
            },
            latency: Latency {
                min: Some(Duration::from_millis(2)),
                max: Some(Duration::from_millis(9)),
                mean: Some(4.5),
            },
        };

        let info = summary(started_at, now, totals);

        assert_eq!(Duration::from_secs(4), info.elapsed());
        assert_eq!(656, info.records_produced());
        assert_eq!(65_456, info.bytes_sent());
        assert_eq!(Some(4.5), info.current.latency.mean);

        // No export at all leaves a summary of zeros over the run's time.
        let empty = summary(started_at, now, ObservationLatency::default());
        assert_eq!(Duration::from_secs(4), empty.elapsed());
        assert_eq!(0, empty.records_produced());
    }

    #[test]
    fn a_refused_produce_is_a_refusal() {
        assert!(Error::Api(ErrorCode::UnknownTopicOrPartition).is_refusal());
        assert!(!Error::Poison.is_refusal());
    }

    #[test]
    fn add_assign_observation() {
        let now = SystemTime::now();
        let delta = Duration::from_secs(5);

        let previous = Observation {
            taken_at: now.checked_sub(delta).expect("previous"),
            bytes_sent: 32_123,
            record_count: 12_321,
        };

        let mut current = Observation {
            taken_at: now,
            bytes_sent: 43_234,
            record_count: 54_345,
        };

        current += previous;

        assert_eq!(now, current.taken_at);
        assert_eq!(75_357, current.bytes_sent);
        assert_eq!(66_666, current.record_count);
    }

    #[test]
    fn middle_observation() {
        let now = SystemTime::now();
        let elapsed = Duration::from_secs(4);

        let previous = {
            let observation = Observation {
                taken_at: now.checked_sub(elapsed).expect("previous"),
                bytes_sent: 43_234,
                record_count: 212,
            };

            ObservationLatency {
                observation,
                latency: Default::default(),
            }
        };

        let mut info = Info::new(now).with_previous(Some(previous));

        info.current = {
            let observation = Observation {
                taken_at: now,
                bytes_sent: 65_456,
                record_count: 656,
            };

            ObservationLatency {
                observation,
                latency: Default::default(),
            }
        };

        assert_eq!(elapsed, info.elapsed());
        assert_eq!(Some(5_555), info.bandwidth().map(|bandwidth| bandwidth.0));
        assert_eq!(Some(111f64), info.records_produced_per_second());
    }

    #[test]
    fn last_or_first_observation() {
        let now = SystemTime::now();
        let elapsed = Duration::from_secs(4);

        let mut info = Info::new(now.checked_sub(elapsed).expect("elapsed"));

        info.current = {
            let observation = Observation {
                taken_at: now,
                bytes_sent: 65_456,
                record_count: 656,
            };

            ObservationLatency {
                observation,
                latency: Default::default(),
            }
        };

        assert_eq!(elapsed, info.elapsed());
        assert_eq!(Some(16_364), info.bandwidth().map(|bandwidth| bandwidth.0));
        assert_eq!(Some(164f64), info.records_produced_per_second());
    }
}
