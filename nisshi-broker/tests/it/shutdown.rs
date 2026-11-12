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

//! A broker that shuts down closes its listener at once, closes its idle connections, and
//! waits for the requests in progress for at most its shutdown grace.

use std::{net::SocketAddr, time::Duration};

use anyhow::{Result, anyhow};
use nisshi_broker::{NODE_ID, broker::Broker, coordinator::group::administrator::Controller};
use nisshi_sans_io::ErrorCode;
use nisshi_storage::ArcDynStorage;
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::common::{init_tracing, wire::free_port};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

struct RunningBroker {
    addr: SocketAddr,
    token: CancellationToken,
    run: JoinHandle<nisshi_broker::Result<ErrorCode>>,
}

/// Starts a memory-backed broker that shuts down when its token is cancelled, and waits until
/// its listener accepts connections.
async fn run_broker(shutdown_grace: Duration) -> Result<RunningBroker> {
    run_broker_on(Url::parse("memory://")?, shutdown_grace).await
}

/// Starts a broker on `storage`, as [`run_broker`] does.
async fn run_broker_on(storage: Url, shutdown_grace: Duration) -> Result<RunningBroker> {
    let addr = SocketAddr::from(([127, 0, 0, 1], free_port().await?));
    let listener = Url::parse(&format!("tcp://{addr}"))?;
    let token = CancellationToken::new();

    let broker = Broker::<Controller<ArcDynStorage>, ArcDynStorage>::builder()
        .node_id(NODE_ID)
        .cluster_id(format!("shutdown-{}", Uuid::now_v7()))
        .incarnation_id(Uuid::now_v7())
        .advertised_listener(listener.clone())
        .storage(storage)
        .listener(listener)
        .silent(true)
        .build()
        .await?
        .with_cancellation(token.clone())
        .with_shutdown_grace(shutdown_grace);

    let run = tokio::spawn(broker.run(Instant::now()));

    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        match TcpStream::connect(addr).await {
            Ok(_) => break,
            Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(50)).await,
            Err(err) => return Err(anyhow!("broker did not start listening on {addr}: {err}")),
        }
    }

    Ok(RunningBroker { addr, token, run })
}

/// Opens a connection that holds a request in progress: the client sends the first byte of a
/// size prefix and nothing more, so the broker waits for the rest until its I/O idle timeout.
async fn stuck_connection(addr: SocketAddr) -> Result<TcpStream> {
    let mut client = TcpStream::connect(addr).await?;
    client.write_all(&[0]).await?;

    // Gives the broker time to read the byte, so that the request has started to arrive when
    // the test cancels the token.
    sleep(Duration::from_millis(100)).await;

    Ok(client)
}

#[tokio::test]
async fn cancelling_the_token_closes_the_listener() -> Result<()> {
    let _guard = init_tracing()?;

    let broker = run_broker(Duration::from_secs(2)).await?;

    // This connection keeps the broker draining for its whole grace, so the listener must close
    // before the connections end.
    let _stuck = stuck_connection(broker.addr).await?;

    broker.token.cancel();

    let deadline = Instant::now() + Duration::from_secs(1);

    loop {
        match TcpStream::connect(broker.addr).await {
            Err(_) => break,
            Ok(_) if Instant::now() < deadline => sleep(Duration::from_millis(20)).await,
            Ok(_) => return Err(anyhow!("the listener still accepts during the drain")),
        }
    }

    assert!(
        !broker.run.is_finished(),
        "the drain ended before the listener closed"
    );
    assert_eq!(ErrorCode::None, timeout(TEST_TIMEOUT, broker.run).await???);

    Ok(())
}

#[tokio::test]
async fn the_grace_deadline_aborts_a_stuck_connection() -> Result<()> {
    let _guard = init_tracing()?;

    let grace = Duration::from_millis(500);
    let broker = run_broker(grace).await?;
    let mut stuck = stuck_connection(broker.addr).await?;

    let cancelled = Instant::now();
    broker.token.cancel();

    assert_eq!(ErrorCode::None, timeout(TEST_TIMEOUT, broker.run).await???);

    let elapsed = cancelled.elapsed();
    assert!(
        elapsed >= grace,
        "the broker stopped before its grace: {elapsed:?}"
    );
    assert!(
        elapsed < grace + Duration::from_secs(2),
        "the broker stopped long after its grace: {elapsed:?}"
    );

    // The abort closes the connection, with a FIN or a reset.
    let mut buffer = [0u8; 1];
    let read = timeout(TEST_TIMEOUT, stuck.read(&mut buffer)).await?;
    assert!(!matches!(read, Ok(1..)), "{read:?}");

    Ok(())
}

#[tokio::test]
async fn a_zero_grace_aborts_a_stuck_connection_at_once() -> Result<()> {
    let _guard = init_tracing()?;

    let broker = run_broker(Duration::ZERO).await?;
    let _stuck = stuck_connection(broker.addr).await?;

    let cancelled = Instant::now();
    broker.token.cancel();

    assert_eq!(ErrorCode::None, timeout(TEST_TIMEOUT, broker.run).await???);

    let elapsed = cancelled.elapsed();
    assert!(
        elapsed < Duration::from_secs(1),
        "the broker took {elapsed:?} to stop"
    );

    Ok(())
}

#[tokio::test]
async fn an_idle_connection_does_not_delay_the_shutdown() -> Result<()> {
    let _guard = init_tracing()?;

    let broker = run_broker(Duration::from_secs(20)).await?;
    let mut idle = TcpStream::connect(broker.addr).await?;

    // Gives the broker time to accept the connection, because a connection still in the
    // listener's backlog is reset when the listener closes.
    sleep(Duration::from_millis(100)).await;

    let cancelled = Instant::now();
    broker.token.cancel();

    let mut buffer = [0u8; 1];
    assert_eq!(0, timeout(TEST_TIMEOUT, idle.read(&mut buffer)).await??);
    drop(idle);

    assert_eq!(ErrorCode::None, timeout(TEST_TIMEOUT, broker.run).await???);

    let elapsed = cancelled.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "an idle connection held the shutdown for {elapsed:?}"
    );

    Ok(())
}

/// The libSQL engine in `mpsc` mode serves its requests from a task that stops when its storage
/// is cancelled, so this test fails if the drain stops the storage before the drained requests
/// are answered.
#[cfg(feature = "libsql")]
#[tokio::test]
async fn a_request_that_has_started_arriving_is_answered_from_storage() -> Result<()> {
    use bytes::Bytes;
    use nisshi_sans_io::{ApiKey as _, Frame, Header, MetadataRequest, MetadataResponse};

    use crate::common::lite_url;

    let _guard = init_tracing()?;

    let mut storage = lite_url().await?;
    storage.set_query(Some("mode=mpsc"));

    let broker = run_broker_on(storage, Duration::from_secs(5)).await?;

    let request = Frame::request(
        Header::Request {
            api_key: MetadataRequest::KEY,
            api_version: 12,
            correlation_id: 7,
            client_id: Some(env!("CARGO_PKG_NAME").into()),
        },
        MetadataRequest::default()
            .topics(None)
            .allow_auto_topic_creation(Some(false))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false))
            .into(),
    )?;

    let mut client = TcpStream::connect(broker.addr).await?;
    let (head, tail) = request.split_at(2);
    client.write_all(head).await?;

    // Gives the broker time to read the first byte before the drain starts, and gives a storage
    // task that stops on the token time to stop before the rest of the request arrives.
    sleep(Duration::from_millis(100)).await;
    broker.token.cancel();
    sleep(Duration::from_millis(200)).await;

    client.write_all(tail).await?;

    let mut size = [0u8; 4];
    _ = timeout(TEST_TIMEOUT, client.read_exact(&mut size)).await??;
    let length = usize::try_from(i32::from_be_bytes(size))?;
    let mut response = vec![0u8; size.len() + length];
    response[..size.len()].copy_from_slice(&size);
    _ = timeout(TEST_TIMEOUT, client.read_exact(&mut response[size.len()..])).await??;

    let frame = Frame::response_from_bytes(Bytes::from(response), MetadataRequest::KEY, 12)?;
    let metadata = MetadataResponse::try_from(frame.body)?;
    assert_eq!(
        Some(1),
        metadata.brokers.as_ref().map(Vec::len),
        "{metadata:?}"
    );

    assert_eq!(ErrorCode::None, timeout(TEST_TIMEOUT, broker.run).await???);

    Ok(())
}
