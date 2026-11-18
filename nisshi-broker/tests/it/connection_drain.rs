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
use nisshi_sans_io::{ApiKey as _, ErrorCode, Frame, Header, MetadataRequest, MetadataResponse};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
    time::{Instant, sleep, timeout},
};

use crate::{
    common::{init_tracing, wire::round_trip},
    shutdown::run_broker,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Longer than a test takes when every connection drains, so a shutdown that waits for a
/// connection until this grace ends is told apart from one that does not.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(20);

const METADATA_VERSION: i16 = 12;
const CORRELATION_ID: i32 = 7;

fn metadata_request() -> MetadataRequest {
    MetadataRequest::default()
        .topics(None)
        .allow_auto_topic_creation(Some(false))
        .include_cluster_authorized_operations(Some(false))
        .include_topic_authorized_operations(Some(false))
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
async fn a_request_that_has_started_arriving_is_answered_before_the_connection_closes() -> Result<()>
{
    let _guard = init_tracing()?;

    let broker = run_broker(SHUTDOWN_GRACE).await?;
    let mut client = accepted_connection(broker.addr).await?;

    let request = Frame::request(
        Header::Request {
            api_key: MetadataRequest::KEY,
            api_version: METADATA_VERSION,
            correlation_id: CORRELATION_ID,
            client_id: Some(env!("CARGO_PKG_NAME").into()),
        },
        metadata_request().into(),
    )?;

    let (head, tail) = request.split_at(2);
    client.write_all(head).await?;

    // Gives the broker time to read the first byte before the drain starts, and gives the
    // drain time to reach the connection before the rest of the request arrives.
    sleep(Duration::from_millis(100)).await;
    broker.connection_drain.drain();
    sleep(Duration::from_millis(200)).await;

    client.write_all(tail).await?;

    let mut size = [0u8; 4];
    _ = timeout(TEST_TIMEOUT, client.read_exact(&mut size)).await??;
    let mut response = vec![0u8; usize::try_from(i32::from_be_bytes(size))?];
    _ = timeout(TEST_TIMEOUT, client.read_exact(&mut response)).await??;

    // A response starts with the correlation id of its request.
    assert_eq!(CORRELATION_ID.to_be_bytes(), response[..4]);

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
