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

//! [`ConnectionDrain`](nisshi_broker::broker::ConnectionDrain): a drain closes the connections
//! that a broker has open, each after it has answered the request that it holds, and the broker
//! keeps listening.

use std::{net::SocketAddr, time::Duration};

use anyhow::Result;
use nisshi_sans_io::{
    ApiKey as _, ErrorCode, FetchRequest, FetchResponse, MetadataRequest, MetadataResponse,
    fetch_request::{FetchPartition, FetchTopic},
};
use opentelemetry::global;
use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
    data::{AggregatedMetrics, MetricData},
};
use tokio::{
    io::AsyncReadExt as _,
    net::TcpStream,
    time::{Instant, sleep, timeout},
};

use crate::{
    common::{alphanumeric_string, init_tracing, wire::round_trip},
    fetch_wire::create_topic,
    shutdown::run_broker,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Longer than a test takes when every connection drains, so a shutdown that waits for a
/// connection until this grace ends is told apart from one that does not.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(20);

const METADATA_VERSION: i16 = 12;
const FETCH_VERSION: i16 = 12;
const CORRELATION_ID: i32 = 7;

/// How long a Fetch of [`fetch_request`] waits for a record, in milliseconds, and so how long
/// the broker holds that request when the topic stays empty.
const FETCH_MAX_WAIT_MS: i32 = 2_000;

/// The counter of the bytes of the requests that the broker has read.
const BYTES_RECEIVED: &str = "nisshi_bytes_received";

fn metadata_request() -> MetadataRequest {
    MetadataRequest::default()
        .topics(None)
        .allow_auto_topic_creation(Some(false))
        .include_cluster_authorized_operations(Some(false))
        .include_topic_authorized_operations(Some(false))
}

/// Returns a Fetch of partition 0 of `topic` that waits [`FETCH_MAX_WAIT_MS`] for a record.
fn fetch_request(topic: &str) -> FetchRequest {
    FetchRequest::default()
        .replica_id(Some(-1))
        .max_wait_ms(FETCH_MAX_WAIT_MS)
        .min_bytes(1)
        .max_bytes(Some(50 * 1024))
        .isolation_level(Some(0))
        .session_id(Some(0))
        .session_epoch(Some(-1))
        .topics(Some(
            [FetchTopic::default()
                .topic(Some(topic.into()))
                .partitions(Some(
                    [FetchPartition::default()
                        .partition(0)
                        .current_leader_epoch(Some(-1))
                        .fetch_offset(0)
                        .last_fetched_epoch(Some(-1))
                        .log_start_offset(Some(-1))
                        .partition_max_bytes(50 * 1024)]
                    .into(),
                ))]
            .into(),
        ))
        .forgotten_topics_data(Some([].into()))
        .rack_id(Some("".into()))
}

/// Returns the value of the [`BYTES_RECEIVED`] counter, or 0 before the broker has read a
/// request.
fn bytes_received(provider: &SdkMeterProvider, exporter: &InMemoryMetricExporter) -> Result<u64> {
    provider.force_flush()?;

    // The exporter keeps every export, and the counter only grows, so the largest value is
    // the current one.
    Ok(exporter
        .get_finished_metrics()?
        .iter()
        .flat_map(|resource| resource.scope_metrics())
        .flat_map(|scope| scope.metrics())
        .filter(|metric| metric.name() == BYTES_RECEIVED)
        .filter_map(|metric| match metric.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => Some(sum),
            _ => None,
        })
        .flat_map(|sum| sum.data_points())
        .map(|point| point.value())
        .max()
        .unwrap_or_default())
}

/// Sends a Metadata request on `client` and fails unless the broker answers it.
async fn assert_served(client: &mut TcpStream) -> Result<()> {
    let exchange = round_trip(
        client,
        MetadataRequest::KEY,
        METADATA_VERSION,
        CORRELATION_ID,
        metadata_request().into(),
    );

    let metadata = MetadataResponse::try_from(timeout(TEST_TIMEOUT, exchange).await??.body)?;
    assert_eq!(
        Some(1),
        metadata.brokers.as_ref().map(Vec::len),
        "{metadata:?}"
    );

    Ok(())
}

/// Opens a connection that the broker has accepted.
///
/// The broker has answered a request on it, because a connection that only the listener's
/// backlog holds is not yet one of the broker's open connections.
async fn accepted_connection(addr: SocketAddr) -> Result<TcpStream> {
    let mut client = TcpStream::connect(addr).await?;
    assert_served(&mut client).await?;
    Ok(client)
}

/// Fails unless the next read on `client` is the end of the stream: not data, and not a
/// reset.
async fn assert_end_of_stream(client: &mut TcpStream) -> Result<()> {
    let mut buffer = [0u8; 1];
    let read = timeout(TEST_TIMEOUT, client.read(&mut buffer)).await?;
    assert_eq!(0, read?, "the connection is still open");
    Ok(())
}

#[tokio::test]
async fn a_drain_closes_every_open_connection() -> Result<()> {
    let _guard = init_tracing()?;

    let broker = run_broker(SHUTDOWN_GRACE).await?;

    let mut clients = Vec::new();
    for _ in 0..3 {
        clients.push(accepted_connection(broker.addr).await?);
    }

    broker.connection_drain.drain();

    for client in &mut clients {
        assert_end_of_stream(client).await?;
    }

    Ok(())
}

#[tokio::test]
async fn the_broker_serves_a_connection_opened_after_a_drain() -> Result<()> {
    let _guard = init_tracing()?;

    let broker = run_broker(SHUTDOWN_GRACE).await?;
    let mut drained = accepted_connection(broker.addr).await?;

    broker.connection_drain.drain();

    // The listener has started the drain once this connection ends, so the drain does not
    // reach a connection that opens from here on.
    assert_end_of_stream(&mut drained).await?;

    let mut client = accepted_connection(broker.addr).await?;
    assert_served(&mut client).await?;

    Ok(())
}

#[tokio::test]
async fn each_drain_closes_the_connections_opened_since_the_drain_before_it() -> Result<()> {
    let _guard = init_tracing()?;

    let broker = run_broker(SHUTDOWN_GRACE).await?;

    for _ in 0..3 {
        let mut client = accepted_connection(broker.addr).await?;
        broker.connection_drain.drain();
        assert_end_of_stream(&mut client).await?;
    }

    Ok(())
}

#[tokio::test]
async fn a_request_in_progress_is_answered_before_the_connection_closes() -> Result<()> {
    let _guard = init_tracing()?;

    // The broker's counters report to the provider that is installed when the broker first
    // uses them, so the test installs its provider before it starts the broker.
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    global::set_meter_provider(provider.clone());

    let broker = run_broker(SHUTDOWN_GRACE).await?;
    let mut client = accepted_connection(broker.addr).await?;

    let topic = alphanumeric_string(15);
    create_topic(&mut client, &topic).await?;

    let read_before_the_fetch = bytes_received(&provider, &exporter)?;

    let fetch = round_trip(
        &mut client,
        FetchRequest::KEY,
        FETCH_VERSION,
        CORRELATION_ID,
        fetch_request(&topic).into(),
    );

    let drain_during_the_fetch = async {
        // The broker counts the bytes of a request once it has read all of it, and the Fetch
        // is the only request sent since the count was read. So once the count grows the
        // Fetch is in progress, and it stays so for `FETCH_MAX_WAIT_MS`.
        while bytes_received(&provider, &exporter)? == read_before_the_fetch {
            sleep(Duration::from_millis(10)).await;
        }

        broker.connection_drain.drain();

        anyhow::Ok(())
    };

    let (frame, drained) = timeout(TEST_TIMEOUT, async {
        tokio::join!(fetch, drain_during_the_fetch)
    })
    .await?;
    drained?;

    let response = FetchResponse::try_from(frame?.body)?;
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(response.error_code.unwrap_or_default())?
    );

    assert_end_of_stream(&mut client).await?;

    Ok(())
}

#[tokio::test]
async fn a_connection_opened_after_a_drain_closes_when_the_broker_shuts_down() -> Result<()> {
    let _guard = init_tracing()?;

    let broker = run_broker(SHUTDOWN_GRACE).await?;
    let mut drained = accepted_connection(broker.addr).await?;

    broker.connection_drain.drain();
    assert_end_of_stream(&mut drained).await?;

    let mut client = accepted_connection(broker.addr).await?;

    let cancelled = Instant::now();
    broker.token.cancel();

    assert_end_of_stream(&mut client).await?;
    drop(client);

    assert_eq!(ErrorCode::None, timeout(TEST_TIMEOUT, broker.run).await???);

    // A connection that the shutdown does not drain holds the broker for its whole grace.
    let elapsed = cancelled.elapsed();
    assert!(
        elapsed < SHUTDOWN_GRACE / 2,
        "the shutdown waited {elapsed:?} for a connection opened after a drain"
    );

    Ok(())
}
