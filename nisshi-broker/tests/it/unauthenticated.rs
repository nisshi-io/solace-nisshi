// Copyright ⓒ Solace Corporation
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

//! Transport-level tests: on a broker with `--authentication`,
//! a client that sends a request before completing SASL must have its
//! connection closed promptly, the same way `nisshi-broker/tests/it/auth.rs`
//! proves the error type without ever opening a socket. These tests open a
//! real socket, so they can show the connection actually closes.

use std::{net::SocketAddr, time::Duration};

use anyhow::{Context as _, Result, anyhow};
use bytes::Bytes;
use nisshi_broker::{NODE_ID, broker::Broker, coordinator::group::administrator::Controller};
use nisshi_sans_io::{
    ApiKey as _, ApiVersionsRequest, ApiVersionsResponse, Body, Frame, Header, MetadataRequest,
    MetadataResponse,
};
use nisshi_storage::ArcDynStorage;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use tracing::debug;
use url::Url;
use uuid::Uuid;

use crate::common::{init_tracing, lite_url};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Largest response the test client will read; anything bigger is not a
/// Kafka frame.
const MAXIMUM_RESPONSE: usize = 16 * 1024 * 1024;

struct RunningBroker {
    handle: JoinHandle<()>,
    addr: SocketAddr,
}

impl Drop for RunningBroker {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// A plaintext broker with `--authentication` enabled and no credentials
/// configured: every client is unauthenticated. The broker refuses
/// `--authentication` on storage that can't store SCRAM users, so it runs on
/// SQLite.
async fn spawn_authenticating_broker() -> Result<RunningBroker> {
    let port = free_port().await?;
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = Url::parse(&format!("tcp://{addr}"))?;

    let mut broker = Broker::<Controller<ArcDynStorage>, ArcDynStorage>::builder()
        .node_id(NODE_ID)
        .cluster_id(format!("unauthenticated-{}", Uuid::now_v7()))
        .incarnation_id(Uuid::now_v7())
        .advertised_listener(listener.clone())
        .storage(lite_url().await?)
        .listener(listener)
        .authentication(true)
        .silent(true)
        .build()
        .await?;

    let handle = tokio::spawn(async move {
        if let Err(err) = broker.serve(Instant::now()).await {
            debug!(?err);
        }
    });

    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        match TcpStream::connect(addr).await {
            Ok(_) => break,
            Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(50)).await,
            Err(err) => return Err(anyhow!("broker did not start listening on {addr}: {err}")),
        }
    }

    Ok(RunningBroker { handle, addr })
}

/// Send one Kafka request frame and read back the response frame.
async fn round_trip<S>(
    stream: &mut S,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: Body,
) -> Result<Frame>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = Frame::request(
        Header::Request {
            api_key,
            api_version,
            correlation_id,
            client_id: Some(env!("CARGO_PKG_NAME").into()),
        },
        body,
    )?;

    stream.write_all(&request).await?;
    stream.flush().await?;

    let mut size = [0u8; 4];
    _ = stream.read_exact(&mut size).await?;

    let length = usize::try_from(i32::from_be_bytes(size))
        .ok()
        .filter(|length| *length <= MAXIMUM_RESPONSE)
        .ok_or_else(|| anyhow!("not a kafka frame length prefix: {size:02x?}"))?;

    let mut buffer = vec![0u8; length + size.len()];
    buffer[..size.len()].copy_from_slice(&size);
    _ = stream.read_exact(&mut buffer[size.len()..]).await?;

    Frame::response_from_bytes(Bytes::from(buffer), api_key, api_version).map_err(Into::into)
}

async fn api_versions<S>(stream: &mut S) -> Result<ApiVersionsResponse>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let frame = round_trip(
        stream,
        ApiVersionsRequest::KEY,
        3,
        1,
        ApiVersionsRequest::default()
            .client_software_name(Some(env!("CARGO_PKG_NAME").into()))
            .client_software_version(Some(env!("CARGO_PKG_VERSION").into()))
            .into(),
    )
    .await?;

    ApiVersionsResponse::try_from(frame.body).map_err(Into::into)
}

async fn metadata<S>(stream: &mut S) -> Result<MetadataResponse>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let frame = round_trip(
        stream,
        MetadataRequest::KEY,
        12,
        2,
        MetadataRequest::default()
            .topics(Some([].into()))
            .allow_auto_topic_creation(Some(false))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false))
            .into(),
    )
    .await?;

    MetadataResponse::try_from(frame.body).map_err(Into::into)
}

/// `ApiVersions` is exempt from authentication (a client always needs it to
/// negotiate versions before it can even attempt SASL), so it must still
/// succeed on an otherwise-unauthenticated connection.
#[tokio::test]
async fn api_versions_exempt_before_authentication() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_authenticating_broker().await?;

        let mut stream = TcpStream::connect(broker.addr).await?;
        let response = api_versions(&mut stream).await?;

        assert!(
            !response.api_keys.unwrap_or_default().is_empty(),
            "ApiVersions must succeed before authentication"
        );

        Ok(())
    })
    .await?
}

/// A request other than `ApiVersions`/`SaslHandshake`/`SaslAuthenticate`,
/// sent before any SASL exchange, must close the connection promptly rather
/// than hang.
#[tokio::test]
async fn request_before_authentication_closes_connection() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_authenticating_broker().await?;

        let mut stream = TcpStream::connect(broker.addr).await?;
        let outcome = metadata(&mut stream).await;

        assert!(
            outcome.is_err(),
            "a request before authentication must not get a response, got {outcome:?}"
        );

        // The rejected connection must not have taken the listener with it.
        let mut next = TcpStream::connect(broker.addr).await?;
        let response = api_versions(&mut next)
            .await
            .context("a later connection must still be served")?;
        assert!(!response.api_keys.unwrap_or_default().is_empty());

        Ok(())
    })
    .await?
}
