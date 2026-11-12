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

//! A memory-backed broker on a real TCP port, and a minimal Kafka client to
//! exchange frames with it, for tests that need the transport (TLS, or a
//! connection surviving a bad request).

use std::{net::SocketAddr, time::Duration};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use nisshi_broker::{NODE_ID, broker::Broker, coordinator::group::administrator::Controller};
use nisshi_sans_io::{Body, Frame, Header};
use nisshi_schema::Registry;
use nisshi_storage::ArcDynStorage;
use rustls::ServerConfig;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::{Instant, sleep},
};
use tracing::debug;
use url::Url;
use uuid::Uuid;

use super::lite_url;

/// Largest response the test client will read; anything bigger is not a
/// Kafka frame (a TLS alert read as a length prefix decodes to ~350 MiB).
const MAXIMUM_RESPONSE: usize = 16 * 1024 * 1024;

pub(crate) async fn free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

pub(crate) struct RunningBroker {
    handle: JoinHandle<()>,
    pub(crate) addr: SocketAddr,
}

impl Drop for RunningBroker {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

pub(crate) async fn spawn_broker(tls: Option<ServerConfig>) -> Result<RunningBroker> {
    spawn_broker_with_authentication(tls, false).await
}

/// Like [`spawn_broker`], with `--authentication` on or off. With it on, the
/// broker has no credentials, so no client can log in.
///
/// With authentication on, the broker runs on a SQLite database of the test's
/// own, because the in-memory engine can't store the SCRAM users that
/// authentication needs and the broker refuses to start on it.
pub(crate) async fn spawn_broker_with_authentication(
    tls: Option<ServerConfig>,
    authentication: bool,
) -> Result<RunningBroker> {
    let storage = if authentication {
        lite_url().await?
    } else {
        Url::parse("memory://")?
    };

    spawn(tls, authentication, storage, None).await
}

/// Like [`spawn_broker`], with `schemas` as the registry that the broker validates produced
/// records against.
pub(crate) async fn spawn_broker_with_schemas(schemas: Option<Registry>) -> Result<RunningBroker> {
    spawn(None, false, Url::parse("memory://")?, schemas).await
}

/// Like [`spawn_broker_with_authentication`], on the storage at `storage`.
pub(crate) async fn spawn_broker_on(
    tls: Option<ServerConfig>,
    authentication: bool,
    storage: Url,
) -> Result<RunningBroker> {
    spawn(tls, authentication, storage, None).await
}

async fn spawn(
    tls: Option<ServerConfig>,
    authentication: bool,
    storage: Url,
    schemas: Option<Registry>,
) -> Result<RunningBroker> {
    let port = free_port().await?;
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = Url::parse(&format!("tcp://{addr}"))?;

    let mut broker = Broker::<Controller<ArcDynStorage>, ArcDynStorage>::builder()
        .node_id(NODE_ID)
        .cluster_id(format!("wire-{}", Uuid::now_v7()))
        .incarnation_id(Uuid::now_v7())
        .advertised_listener(listener.clone())
        .storage(storage)
        .listener(listener)
        .tls_server_config(tls)
        .authentication(authentication)
        .schema_registry(schemas)
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

/// Send one Kafka request frame and read back the response frame, over any byte stream.
pub(crate) async fn round_trip<S>(
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
