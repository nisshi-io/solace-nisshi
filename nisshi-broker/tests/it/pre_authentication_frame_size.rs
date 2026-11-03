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

//! The pre-authentication frame size limit, over a loopback socket through the
//! broker's own service stack.
//!
//! A SASL listener closes a connection whose frame exceeds
//! [`DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE`] until the client
//! authenticates, and again while the client re-authenticates. A listener
//! without SASL accepts the same frame.

use std::{net::SocketAddr, time::Duration};

use anyhow::{Result, anyhow, ensure};
use bytes::{BufMut as _, Bytes, BytesMut};
use nisshi_broker::{coordinator::group::administrator::Controller, service::services};
use nisshi_sans_io::{
    ApiKey as _, ApiVersionsRequest, ApiVersionsResponse, Body, ErrorCode, Frame, Header,
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
    ScramMechanism,
};
use nisshi_service::DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE;
use nisshi_storage::{
    ArcDynStorage, AutoCreateRefusals, ScramCredential, Storage as _, TopicCreationConfig,
};
use rama::{Service as _, extensions::Extensions, tcp::TcpStream as RamaTcpStream};
use rand::{RngExt as _, rng};
use rsasl::{
    config::SASLConfig,
    prelude::{Mechname, SASLClient, State},
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};
use uuid::Uuid;

use crate::common::{init_tracing, lite_storage, turso_storage};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The time the test client waits for the broker to close a connection.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

const MAXIMUM_RESPONSE: usize = 16 * 1024 * 1024;

const PRINCIPAL: &str = "alice";
const PASSWORD: &str = "secret";
const SASL_API_VERSION: i16 = 1;

/// The SCRAM-SHA-256 credential for [`PRINCIPAL`] with [`PASSWORD`].
fn credential() -> ScramCredential {
    ScramCredential {
        salt: Bytes::from_static(&[
            107, 53, 50, 116, 97, 121, 49, 118, 118, 116, 105, 97, 53, 101, 54, 108, 99, 51, 55,
            103, 110, 51, 102, 51, 104,
        ]),
        iterations: 8192,
        stored_key: Bytes::from_static(&[
            150, 254, 7, 121, 81, 205, 192, 207, 60, 206, 251, 24, 31, 131, 31, 15, 96, 75, 20,
            228, 251, 132, 22, 235, 160, 72, 200, 130, 127, 49, 29, 150,
        ]),
        server_key: Bytes::from_static(&[
            186, 175, 253, 227, 176, 106, 88, 53, 186, 173, 104, 88, 94, 40, 115, 166, 44, 183,
            199, 177, 137, 41, 225, 132, 56, 32, 70, 255, 223, 209, 22, 146,
        ]),
    }
}

struct RunningBroker {
    handle: JoinHandle<()>,
    addr: SocketAddr,
}

impl Drop for RunningBroker {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Serves each accepted connection with the service stack that [`services`]
/// builds, as the broker does.
async fn spawn_broker(sasl: bool, storage: ArcDynStorage) -> Result<RunningBroker> {
    let sasl_config = if sasl {
        storage
            .upsert_user_scram_credential(PRINCIPAL, ScramMechanism::Scram256, credential())
            .await?;

        Some(nisshi_auth::configuration(storage.clone())?)
    } else {
        None
    };

    let coordinator = Controller::with_storage(storage.clone())?;

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let handle = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let Ok(service) = services(
                "pre-authentication",
                coordinator.clone(),
                storage.clone(),
                TopicCreationConfig::default(),
                AutoCreateRefusals::default(),
                sasl_config.clone(),
            ) else {
                return;
            };

            _ = tokio::spawn(async move {
                _ = service
                    .serve(RamaTcpStream::from_tokio_tcp_stream(
                        stream,
                        Extensions::default(),
                    ))
                    .await;
            });
        }
    });

    Ok(RunningBroker { handle, addr })
}

fn request(api_key: i16, api_version: i16, correlation_id: i32, body: Body) -> Result<Bytes> {
    Frame::request(
        Header::Request {
            api_key,
            api_version,
            correlation_id,
            client_id: Some(env!("CARGO_PKG_NAME").into()),
        },
        body,
    )
    .map_err(Into::into)
}

async fn read_response(stream: &mut TcpStream, api_key: i16, api_version: i16) -> Result<Frame> {
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

async fn round_trip(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: Body,
) -> Result<Frame> {
    stream
        .write_all(&request(api_key, api_version, correlation_id, body)?)
        .await?;
    read_response(stream, api_key, api_version).await
}

/// An `ApiVersions` request over the pre-authentication limit and well under
/// the full limit. The broker answers `ApiVersions` before authentication,
/// so only the frame size limit can close the connection.
fn oversized_api_versions(correlation_id: i32) -> Result<Bytes> {
    let frame = request(
        ApiVersionsRequest::KEY,
        3,
        correlation_id,
        ApiVersionsRequest::default()
            .client_software_name(Some(
                "x".repeat(DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE),
            ))
            .client_software_version(Some(env!("CARGO_PKG_VERSION").into()))
            .into(),
    )?;

    ensure!(frame.len() > DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE + 4);
    Ok(frame)
}

/// Sends an oversized `ApiVersions` request and returns whether the broker
/// answered it.
///
/// The function first checks that the broker answers a small `ApiVersions`
/// request on the same connection, so a closed connection after the oversized
/// request can only come from the frame size.
async fn oversized_request_is_answered(
    stream: &mut TcpStream,
    correlation_id: i32,
) -> Result<bool> {
    let frame = round_trip(
        stream,
        ApiVersionsRequest::KEY,
        3,
        correlation_id,
        ApiVersionsRequest::default()
            .client_software_name(Some(env!("CARGO_PKG_NAME").into()))
            .client_software_version(Some(env!("CARGO_PKG_VERSION").into()))
            .into(),
    )
    .await?;
    _ = ApiVersionsResponse::try_from(frame.body)?;

    let correlation_id = correlation_id + 1;

    // The broker can close the connection before it reads the whole frame,
    // so a failed write is one form of a rejection.
    if stream
        .write_all(&oversized_api_versions(correlation_id)?)
        .await
        .is_err()
    {
        return Ok(false);
    }

    match timeout(
        CLOSE_TIMEOUT,
        read_response(stream, ApiVersionsRequest::KEY, 3),
    )
    .await
    {
        Ok(Ok(frame)) => ApiVersionsResponse::try_from(frame.body)
            .map(|_| true)
            .map_err(Into::into),
        Ok(Err(_)) => Ok(false),
        Err(_) => Err(anyhow!(
            "the broker neither answered nor closed the connection"
        )),
    }
}

async fn sasl_handshake(stream: &mut TcpStream, correlation_id: &mut i32) -> Result<()> {
    *correlation_id += 1;

    let frame = round_trip(
        stream,
        SaslHandshakeRequest::KEY,
        SASL_API_VERSION,
        *correlation_id,
        SaslHandshakeRequest::default()
            .mechanism("SCRAM-SHA-256".into())
            .into(),
    )
    .await?;

    let response = SaslHandshakeResponse::try_from(frame.body)?;
    ensure!(ErrorCode::None == ErrorCode::try_from(response.error_code)?);
    Ok(())
}

async fn sasl_authenticate(stream: &mut TcpStream, correlation_id: &mut i32) -> Result<()> {
    // A Kafka client logs in with the mechanism that it requested, not one that
    // it picks from the list.
    let offered = [Mechname::parse(b"SCRAM-SHA-256")?];

    let client = SASLClient::new(SASLConfig::with_credentials(
        None,
        PRINCIPAL.into(),
        PASSWORD.into(),
    )?);

    let mut session = client.start_suggested(&offered)?;
    let mut input: Option<Bytes> = None;

    loop {
        let mut output = BytesMut::new().writer();

        match session.step(input.as_deref(), &mut output)? {
            State::Running => {
                *correlation_id += 1;

                let frame = round_trip(
                    stream,
                    SaslAuthenticateRequest::KEY,
                    SASL_API_VERSION,
                    *correlation_id,
                    SaslAuthenticateRequest::default()
                        .auth_bytes(Bytes::from(output.into_inner()))
                        .into(),
                )
                .await?;

                let response = SaslAuthenticateResponse::try_from(frame.body)?;
                ensure!(ErrorCode::None == ErrorCode::try_from(response.error_code)?);
                input = Some(response.auth_bytes);
            }

            State::Finished(_) => return Ok(()),
        }
    }
}

async fn authenticate(stream: &mut TcpStream, correlation_id: &mut i32) -> Result<()> {
    sasl_handshake(stream, correlation_id).await?;
    sasl_authenticate(stream, correlation_id).await
}

async fn sasl_listener_rejects_oversized_frame_before_authentication(
    storage: ArcDynStorage,
) -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(true, storage).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        ensure!(!oversized_request_is_answered(&mut stream, 1).await?);
        Ok(())
    })
    .await?
}

async fn sasl_listener_accepts_oversized_frame_after_authentication(
    storage: ArcDynStorage,
) -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(true, storage).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;
        let mut correlation_id = 0;

        authenticate(&mut stream, &mut correlation_id).await?;

        ensure!(oversized_request_is_answered(&mut stream, correlation_id + 1).await?);
        Ok(())
    })
    .await?
}

async fn sasl_listener_rejects_oversized_frame_during_reauthentication(
    storage: ArcDynStorage,
) -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(true, storage).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;
        let mut correlation_id = 0;

        authenticate(&mut stream, &mut correlation_id).await?;
        sasl_handshake(&mut stream, &mut correlation_id).await?;

        ensure!(!oversized_request_is_answered(&mut stream, correlation_id + 1).await?);
        Ok(())
    })
    .await?
}

async fn sasl_listener_accepts_oversized_frame_after_reauthentication(
    storage: ArcDynStorage,
) -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(true, storage).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;
        let mut correlation_id = 0;

        authenticate(&mut stream, &mut correlation_id).await?;
        authenticate(&mut stream, &mut correlation_id).await?;

        ensure!(oversized_request_is_answered(&mut stream, correlation_id + 1).await?);
        Ok(())
    })
    .await?
}

async fn listener_without_sasl_accepts_oversized_frame(storage: ArcDynStorage) -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(false, storage).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        ensure!(oversized_request_is_answered(&mut stream, 1).await?);
        Ok(())
    })
    .await?
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage:path) => {
        #[cfg(feature = $feature)]
        mod $name {
            use super::*;

            #[tokio::test]
            async fn sasl_listener_rejects_oversized_frame_before_authentication() -> Result<()> {
                let _guard = init_tracing()?;

                super::sasl_listener_rejects_oversized_frame_before_authentication(
                    $storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await?,
                )
                .await
            }

            #[tokio::test]
            async fn sasl_listener_accepts_oversized_frame_after_authentication() -> Result<()> {
                let _guard = init_tracing()?;

                super::sasl_listener_accepts_oversized_frame_after_authentication(
                    $storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await?,
                )
                .await
            }

            #[tokio::test]
            async fn sasl_listener_rejects_oversized_frame_during_reauthentication() -> Result<()> {
                let _guard = init_tracing()?;

                super::sasl_listener_rejects_oversized_frame_during_reauthentication(
                    $storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await?,
                )
                .await
            }

            #[tokio::test]
            async fn sasl_listener_accepts_oversized_frame_after_reauthentication() -> Result<()> {
                let _guard = init_tracing()?;

                super::sasl_listener_accepts_oversized_frame_after_reauthentication(
                    $storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await?,
                )
                .await
            }

            #[tokio::test]
            async fn listener_without_sasl_accepts_oversized_frame() -> Result<()> {
                let _guard = init_tracing()?;

                super::listener_without_sasl_accepts_oversized_frame(
                    $storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await?,
                )
                .await
            }
        }
    };
}

backend!(lite, "libsql", lite_storage);
backend!(turso, "turso", turso_storage);
