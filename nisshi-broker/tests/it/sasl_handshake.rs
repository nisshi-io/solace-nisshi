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

//! SaslHandshake over a real connection, as Kafka 3.9.1 answers it.
//!
//! A broker with SASL lists the mechanisms that it enables in every response.
//! For a mechanism that it hasn't enabled it answers `UNSUPPORTED_SASL_MECHANISM`
//! and then closes the connection, so that the client fails at once instead of
//! timing out. A broker without SASL answers `ILLEGAL_SASL_STATE` with no
//! mechanisms and keeps the connection open.

use std::{assert_matches, sync::Arc, time::Duration};

use anyhow::{Context as _, Result};
use nisshi_broker::{Error, service::auth};
use nisshi_sans_io::{
    ApiKey as _, ApiVersionsRequest, ApiVersionsResponse, Body, BytesInput, ErrorCode, Frame,
    Header, SaslHandshakeRequest, SaslHandshakeResponse,
};
use nisshi_service::{BytesFrameLayer, FrameRouteService};
use rama::{Layer as _, Service as _, extensions::Extensions};
use rustls::pki_types::ServerName;
use tokio::{
    io::{AsyncRead, AsyncReadExt as _, AsyncWrite},
    net::TcpStream,
    time::timeout,
};
use tokio_rustls::TlsConnector;
use uuid::Uuid;

use crate::{
    common::{
        init_tracing, memory_storage,
        wire::{round_trip, spawn_broker_with_authentication},
    },
    tls::{certified_key, client_config_trusting, server_config_from_pem},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The time the test client waits for the broker to close a connection.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// The mechanisms that a broker with SASL enables, sorted.
const ENABLED: [&str; 2] = ["SCRAM-SHA-256", "SCRAM-SHA-512"];

async fn handshake<S>(
    stream: &mut S,
    api_version: i16,
    mechanism: &str,
) -> Result<SaslHandshakeResponse>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let frame = round_trip(
        stream,
        SaslHandshakeRequest::KEY,
        api_version,
        1,
        SaslHandshakeRequest::default()
            .mechanism(mechanism.into())
            .into(),
    )
    .await?;

    SaslHandshakeResponse::try_from(frame.body).map_err(Into::into)
}

fn sorted_mechanisms(response: &SaslHandshakeResponse) -> Option<Vec<String>> {
    response.mechanisms.clone().map(|mut mechanisms| {
        mechanisms.sort_unstable();
        mechanisms
    })
}

/// Asserts that `response` refuses the mechanism and lists the enabled ones.
fn assert_unsupported(response: &SaslHandshakeResponse) -> Result<()> {
    assert_eq!(
        ErrorCode::UnsupportedSaslMechanism,
        ErrorCode::try_from(response.error_code)?
    );
    assert_eq!(
        Some(ENABLED.map(String::from).to_vec()),
        sorted_mechanisms(response)
    );
    Ok(())
}

/// Asserts that the broker has closed the connection: the next read ends the
/// stream cleanly instead of timing out.
async fn assert_closed<S>(stream: &mut S)
where
    S: AsyncRead + Unpin,
{
    let mut buffer = [0u8; 1];
    let read = timeout(CLOSE_TIMEOUT, stream.read(&mut buffer)).await;

    assert_matches!(read, Ok(Ok(0)), "the broker didn't close the connection");
}

/// Asserts that the broker still serves the connection.
async fn assert_open<S>(stream: &mut S) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let frame = timeout(
        CLOSE_TIMEOUT,
        round_trip(
            stream,
            ApiVersionsRequest::KEY,
            3,
            2,
            ApiVersionsRequest::default()
                .client_software_name(Some(env!("CARGO_PKG_NAME").into()))
                .client_software_version(Some(env!("CARGO_PKG_VERSION").into()))
                .into(),
        ),
    )
    .await
    .context("the broker didn't answer ApiVersions")??;

    let response = ApiVersionsResponse::try_from(frame.body)?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
    Ok(())
}

/// Requests `mechanism` from a broker with SASL, and asserts that the broker
/// refuses it, lists the enabled mechanisms and closes the connection.
async fn refused_then_closed(api_version: i16, mechanism: &str) -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker_with_authentication(None, true).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        let response = handshake(&mut stream, api_version, mechanism).await?;
        assert_unsupported(&response)?;
        assert_closed(&mut stream).await;

        Ok(())
    })
    .await?
}

/// PLAIN isn't enabled.
#[tokio::test]
async fn plain_is_refused_then_closed() -> Result<()> {
    refused_then_closed(1, "PLAIN").await
}

/// OAUTHBEARER isn't enabled: the broker has no way to validate its token.
#[tokio::test]
async fn oauthbearer_is_refused_then_closed() -> Result<()> {
    refused_then_closed(1, "OAUTHBEARER").await
}

/// A name that isn't a valid SASL mechanism name gets the same answer.
#[tokio::test]
async fn invalid_mechanism_name_is_refused_then_closed() -> Result<()> {
    refused_then_closed(1, "not a mechanism").await
}

/// At version 0 a client sends its SASL tokens unframed after an accepted
/// handshake. A refused one is still answered as a Kafka frame.
#[tokio::test]
async fn plain_at_v0_is_refused_then_closed() -> Result<()> {
    refused_then_closed(0, "PLAIN").await
}

/// Over TLS the broker writes the response before it closes the session.
#[tokio::test]
async fn plain_over_tls_is_refused_then_closed() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let certified = certified_key()?;
        let server = server_config_from_pem(&certified.cert, &certified.key)?;
        let broker = spawn_broker_with_authentication(Some(server), true).await?;

        let connector = TlsConnector::from(Arc::new(client_config_trusting(&certified.cert)?));
        let tcp = TcpStream::connect(broker.addr).await?;
        let mut tls = connector
            .connect(ServerName::try_from("localhost")?, tcp)
            .await
            .context("tls handshake with the broker failed")?;

        let response = handshake(&mut tls, 1, "PLAIN").await?;
        assert_unsupported(&response)?;
        assert_closed(&mut tls).await;

        Ok(())
    })
    .await?
}

/// An accepted handshake lists every enabled mechanism, not only the requested
/// one, and the connection stays open for the SASL exchange.
#[tokio::test]
async fn scram_is_accepted_and_lists_the_enabled_mechanisms() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker_with_authentication(None, true).await?;

        for mechanism in ENABLED {
            let mut stream = TcpStream::connect(broker.addr).await?;

            let response = handshake(&mut stream, 1, mechanism).await?;
            assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
            assert_eq!(
                Some(ENABLED.map(String::from).to_vec()),
                sorted_mechanisms(&response),
                "{mechanism}"
            );
            assert_open(&mut stream).await?;
        }

        Ok(())
    })
    .await?
}

/// A broker without SASL answers as a Kafka listener without SASL does: an
/// empty, not null, list of mechanisms, and the connection stays open.
#[tokio::test]
async fn without_sasl_is_illegal_state_with_no_mechanisms() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker_with_authentication(None, false).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        let response = handshake(&mut stream, 1, "SCRAM-SHA-256").await?;
        assert_eq!(
            ErrorCode::IllegalSaslState,
            ErrorCode::try_from(response.error_code)?
        );
        assert_matches!(response.mechanisms.as_deref(), Some([]));
        assert_open(&mut stream).await?;

        Ok(())
    })
    .await?
}

/// A refused handshake at version 0 must not switch the connection to unframed
/// SASL tokens. Served without a connection loop, so that nothing closes the
/// connection, the next request is still read as a Kafka request.
#[tokio::test]
async fn refused_v0_handshake_leaves_the_connection_framed() -> Result<()> {
    let _guard = init_tracing()?;

    let storage = memory_storage(Uuid::now_v7().to_string(), 111).await?;
    let sasl_config = nisshi_auth::configuration(storage)?;

    let broker = auth::services(FrameRouteService::<Error>::builder())
        .and_then(|builder| builder.build().map_err(Into::into))
        .map(|route| {
            BytesFrameLayer::default()
                .with_sasl_config(Some(sasl_config))
                .into_layer(route)
        })?;

    let extensions = Extensions::default();

    let serve = async |api_key, api_version, body: Body| -> Result<Frame> {
        let response = broker
            .serve(BytesInput {
                bytes: Frame::request(
                    Header::Request {
                        api_key,
                        api_version,
                        correlation_id: 1,
                        client_id: Some(env!("CARGO_PKG_NAME").into()),
                    },
                    body,
                )?,
                extensions: extensions.clone(),
            })
            .await?;

        Frame::response_from_bytes(response, api_key, api_version).map_err(Into::into)
    };

    let handshake = serve(
        SaslHandshakeRequest::KEY,
        0,
        SaslHandshakeRequest::default()
            .mechanism("PLAIN".into())
            .into(),
    )
    .await
    .and_then(|frame| SaslHandshakeResponse::try_from(frame.body).map_err(Into::into))?;
    assert_unsupported(&handshake)?;

    let api_versions = serve(
        ApiVersionsRequest::KEY,
        3,
        ApiVersionsRequest::default()
            .client_software_name(Some(env!("CARGO_PKG_NAME").into()))
            .client_software_version(Some(env!("CARGO_PKG_VERSION").into()))
            .into(),
    )
    .await
    .and_then(|frame| ApiVersionsResponse::try_from(frame.body).map_err(Into::into))?;
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(api_versions.error_code)?
    );

    Ok(())
}
