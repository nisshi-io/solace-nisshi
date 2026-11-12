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
//! Nisshi Message Generator
//!
//! Generate fake data for schema backed topics.

use std::{
    fmt, io,
    marker::PhantomData,
    num::NonZeroU32,
    pin::Pin,
    result,
    sync::{Arc, LazyLock, PoisonError},
    time::{Duration, SystemTime},
};

use governor::{InsufficientCapacity, Jitter, Quota, RateLimiter};
use nisshi_client::{Client, ConnectionManager, Security};
use nisshi_otel::meter_provider;
use nisshi_sans_io::{
    Ack, ByteSize, ErrorCode, ProduceRequest,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{deflated, inflated},
};
use nisshi_schema::{Generator as _, Registry, Schema};
use nonzero_ext::nonzero;
use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Histogram, Meter},
};
use opentelemetry_otlp::ExporterBuildError;
use opentelemetry_sdk::error::OTelSdkError;
use opentelemetry_semantic_conventions::SCHEMA_URL;
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};
use tokio::{task::JoinSet, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Level, debug, span};
use url::Url;

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    Api(ErrorCode),
    Client(#[from] nisshi_client::Error),
    ExporterBuild(#[from] ExporterBuildError),
    InsufficientCapacity(#[from] InsufficientCapacity),
    Io(Arc<io::Error>),
    Otel(#[from] nisshi_otel::Error),
    OtelSdk(#[from] OTelSdkError),
    Poison,
    Protocol(#[from] nisshi_sans_io::Error),
    Schema(Box<nisshi_schema::Error>),
    SchemaNotFoundForTopic(String),
    UnknownHost(String),
    Url(#[from] url::ParseError),
}

impl Error {
    /// Whether the broker refused the login or the TLS connection, or the client refused what
    /// the broker offered. No producer can produce after that, so the run ends with the error.
    fn is_refusal(&self) -> bool {
        matches!(
            self,
            Self::Client(error) if error.failure_kind() == nisshi_client::FailureKind::Refused
        )
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl From<nisshi_schema::Error> for Error {
    fn from(error: nisshi_schema::Error) -> Self {
        Self::Schema(Box::new(error))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl fmt::Display for Error {
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

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Configuration {
    broker: Url,
    security: Security,
    topic: String,
    partition: i32,
    schema_registry: Url,
    batch_size: u32,
    per_second: Option<u32>,
    throughput: Option<u32>,
    producers: u32,
    duration: Option<Duration>,
    otlp_endpoint_url: Option<Url>,
}

#[derive(Clone, Debug)]
pub struct Generate {
    configuration: Configuration,
    registry: Registry,
}

impl TryFrom<Configuration> for Generate {
    type Error = Error;

    fn try_from(configuration: Configuration) -> Result<Self, Self::Error> {
        Registry::builder_try_from_url(&configuration.schema_registry)
            .map(|builder| builder.build())
            .map(|registry| Self {
                configuration,
                registry,
            })
            .map_err(Into::into)
    }
}

static GENERATE_PRODUCE_BATCH_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("generate_produce_batch_duration")
        .with_unit("ms")
        .with_description("Generate a produce batch in milliseconds")
        .build()
});

static PRODUCE_REQUEST_RESPONSE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("produce_request_response_duration")
        .with_unit("ms")
        .with_description("Latency of receiving an produce response in milliseconds")
        .build()
});

fn frame(name: String, index: i32, schema: Schema, batch_size: i32) -> Result<deflated::Frame> {
    let attributes = [
        KeyValue::new("topic", name.clone()),
        KeyValue::new("partition", index.to_string()),
        KeyValue::new("batch_size", batch_size.to_string()),
    ];

    let start = SystemTime::now();

    let mut batch = inflated::Batch::builder();
    let offset_deltas = 0..batch_size;

    for offset_delta in offset_deltas {
        batch = schema
            .generate()
            .map(|record| record.offset_delta(offset_delta))
            .map(|record| batch.record(record))?;
    }

    batch
        .last_offset_delta(batch_size - 1)
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)
        .inspect(|_| {
            GENERATE_PRODUCE_BATCH_DURATION.record(
                start
                    .elapsed()
                    .map_or(0, |duration| duration.as_millis() as u64),
                &attributes,
            )
        })
        .map_err(Into::into)
}

pub async fn produce(
    client: Client,
    name: String,
    index: i32,
    batch_size: i32,
    frame: deflated::Frame,
) -> Result<()> {
    debug!(?client, %name, index, batch_size);

    let attributes = [
        KeyValue::new("topic", name.clone()),
        KeyValue::new("partition", index.to_string()),
        KeyValue::new("batch_size", batch_size.to_string()),
    ];

    let req = ProduceRequest::default()
        .acks(Ack::Leader.into())
        .topic_data(Some(
            [TopicProduceData::default().name(name).partition_data(Some(
                [PartitionProduceData::default()
                    .index(index)
                    .records(Some(frame))]
                .into(),
            ))]
            .into(),
        ));

    let start = SystemTime::now();

    let response = client.call(req).await.inspect(|_| {
        PRODUCE_REQUEST_RESPONSE_DURATION.record(
            start
                .elapsed()
                .map_or(0, |duration| duration.as_millis() as u64),
            &attributes,
        )
    })?;

    assert!(
        response
            .responses
            .unwrap_or_default()
            .into_iter()
            .all(|topic| {
                topic
                    .partition_responses
                    .unwrap_or_default()
                    .iter()
                    .inspect(|partition| debug!(topic = %topic.name, ?partition))
                    .all(|partition| partition.error_code == i16::from(ErrorCode::None))
            })
    );

    Ok(())
}

static RATE_LIMIT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
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

static PRODUCE_API_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("produce_duration")
        .with_unit("ms")
        .with_description("Produce API latencies in milliseconds")
        .build()
});

impl Generate {
    pub async fn main(self) -> Result<ErrorCode> {
        debug!(configuration = ?self.configuration);

        let meter_provider = self
            .configuration
            .otlp_endpoint_url
            .map_or(Ok(None), |otlp_endpoint_url| {
                meter_provider(otlp_endpoint_url, env!("CARGO_PKG_NAME")).map(Some)
            })?;

        let Some(schema) = self.registry.schema(&self.configuration.topic).await? else {
            return Err(Error::SchemaNotFoundForTopic(
                self.configuration.topic.clone(),
            ));
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
            .configuration
            .per_second
            .or(self.configuration.throughput)
            .and_then(NonZeroU32::new)
            .map(Quota::per_second)
            .map(RateLimiter::direct)
            .map(Arc::new)
            .inspect(|rate_limiter| debug!(?rate_limiter));

        let batch_size = NonZeroU32::new(self.configuration.batch_size)
            .inspect(|batch_size| debug!(batch_size = batch_size.get()))
            .unwrap_or(nonzero!(10u32));

        let mut set = JoinSet::new();

        let token = CancellationToken::new();

        let client = ConnectionManager::builder(self.configuration.broker)
            .security(self.configuration.security.clone())
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        for producer in 0..self.configuration.producers {
            let rate_limiter = rate_limiter.clone();
            let schema = schema.clone();
            let topic = self.configuration.topic.clone();
            let partition = self.configuration.partition;
            let token = token.clone();
            let client = client.clone();

            _ = set.spawn(async move {
                    let span = span!(Level::DEBUG, "producer", producer);

                    async move {
                        let attributes = [KeyValue::new("producer", producer.to_string())];

                        loop {
                            debug!(%topic, partition);

                           let Ok(frame) = frame(topic.clone(), partition, schema.clone(), batch_size.get() as i32) else {
                               break
                           };

                           if let Some(ref rate_limiter) = rate_limiter {
                                let rate_limit_start = SystemTime::now();


                                let cells = self.configuration.throughput.and(frame.size_in_bytes().ok().and_then(|bytes|NonZeroU32::new(bytes as u32))).unwrap_or(batch_size);
                                debug!(cells);


                                tokio::select! {
                                    cancelled = token.cancelled() => {
                                        debug!(?cancelled);
                                        break
                                    },

                                    Ok(_) = rate_limiter.until_n_ready_with_jitter(cells, Jitter::up_to(Duration::from_millis(50))) => {
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
                                cancelled = token.cancelled() => {
                                    debug!(?cancelled);
                                    break
                                },

                                produced = produce(client.clone(), topic.clone(), partition,  batch_size.get() as i32, frame) => match produced {
                                    Ok(()) => {
                                        PRODUCE_RECORD_COUNT.add(batch_size.get() as u64, &attributes);
                                        PRODUCE_API_DURATION.record(produce_start.elapsed().inspect(|duration|debug!(produce_duration_ms = duration.as_millis())).map_or(0, |duration| duration.as_millis() as u64), &attributes);
                                    }

                                    // The login or the TLS connection was refused, so no producer
                                    // can ever produce: end the run with the error.
                                    Err(error) if error.is_refusal() => return Err(error),

                                    // Any other failure of a request leaves this producer idle
                                    // until the run ends.
                                    Err(error) => {
                                        debug!(?error);
                                        token.cancelled().await;
                                        break
                                    }
                                },
                            }
                        }

                        Ok::<(), Error>(())
                    }.instrument(span).await
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
            .configuration
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

        if let Some(meter_provider) = meter_provider {
            meter_provider
                .force_flush()
                .inspect(|force_flush| debug!(?force_flush))?;

            meter_provider
                .shutdown()
                .inspect(|shutdown| debug!(?shutdown))?;
        }

        if let Some(CancelKind::Timeout) = cancellation {
            sleep(Duration::from_secs(5)).await;
        }

        debug!(abort = set.len());
        set.abort_all();

        while !set.is_empty() {
            _ = set.join_next().await;
        }

        failure.map_or(Ok(ErrorCode::None), Err)
    }

    pub fn builder()
    -> Builder<PhantomData<Url>, PhantomData<String>, PhantomData<i32>, PhantomData<Url>> {
        Builder::default()
    }
}

#[derive(Clone, Debug)]
pub struct Builder<B, T, P, S> {
    broker: B,
    security: Security,
    topic: T,
    partition: P,
    schema_registry: S,
    batch_size: u32,
    per_second: Option<u32>,
    throughput: Option<u32>,
    producers: u32,
    duration: Option<Duration>,
    otlp_endpoint_url: Option<Url>,
}

impl Default
    for Builder<PhantomData<Url>, PhantomData<String>, PhantomData<i32>, PhantomData<Url>>
{
    fn default() -> Self {
        Self {
            broker: Default::default(),
            security: Default::default(),
            topic: Default::default(),
            partition: Default::default(),
            schema_registry: Default::default(),
            batch_size: 1,
            per_second: None,
            throughput: None,
            producers: 1,
            duration: None,
            otlp_endpoint_url: None,
        }
    }
}

impl<B, T, P, S> Builder<B, T, P, S> {
    /// How the connection to the broker is secured, by default not at all.
    pub fn security(self, security: Security) -> Self {
        Self { security, ..self }
    }

    pub fn broker(self, broker: impl Into<Url>) -> Builder<Url, T, P, S> {
        Builder {
            broker: broker.into(),
            security: self.security,
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            batch_size: self.batch_size,
            per_second: self.per_second,
            throughput: self.throughput,
            producers: self.producers,
            duration: self.duration,
            otlp_endpoint_url: self.otlp_endpoint_url,
        }
    }

    pub fn topic(self, topic: impl Into<String>) -> Builder<B, String, P, S> {
        Builder {
            broker: self.broker,
            security: self.security,
            topic: topic.into(),
            partition: self.partition,
            schema_registry: self.schema_registry,
            batch_size: self.batch_size,
            per_second: self.per_second,
            throughput: self.throughput,
            producers: self.producers,
            duration: self.duration,
            otlp_endpoint_url: self.otlp_endpoint_url,
        }
    }

    pub fn partition(self, partition: i32) -> Builder<B, T, i32, S> {
        Builder {
            broker: self.broker,
            security: self.security,
            topic: self.topic,
            partition,
            schema_registry: self.schema_registry,
            batch_size: self.batch_size,
            per_second: self.per_second,
            throughput: self.throughput,
            producers: self.producers,
            duration: self.duration,
            otlp_endpoint_url: self.otlp_endpoint_url,
        }
    }

    pub fn schema_registry(self, schema_registry: Url) -> Builder<B, T, P, Url> {
        Builder {
            broker: self.broker,
            security: self.security,
            topic: self.topic,
            partition: self.partition,
            schema_registry,
            batch_size: self.batch_size,
            per_second: self.per_second,
            throughput: self.throughput,
            producers: self.producers,
            duration: self.duration,
            otlp_endpoint_url: self.otlp_endpoint_url,
        }
    }

    pub fn batch_size(self, batch_size: u32) -> Self {
        Self { batch_size, ..self }
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

    pub fn otlp_endpoint_url(self, otlp_endpoint_url: Option<Url>) -> Self {
        Self {
            otlp_endpoint_url,
            ..self
        }
    }
}

impl Builder<Url, String, i32, Url> {
    pub fn build(self) -> Result<Generate> {
        Generate::try_from(Configuration {
            broker: self.broker,
            security: self.security,
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            batch_size: self.batch_size,
            per_second: self.per_second,
            throughput: self.throughput,
            producers: self.producers,
            duration: self.duration,
            otlp_endpoint_url: self.otlp_endpoint_url,
        })
    }
}
