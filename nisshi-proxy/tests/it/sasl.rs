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

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use nisshi_proxy::Proxy;
use nisshi_sans_io::{
    ApiKey as _, ApiVersionsRequest, ApiVersionsResponse, Body, ErrorCode, Frame, FrameInput,
    Header, MetadataRequest, MetadataResponse, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest, SaslHandshakeResponse, api_versions_response::ApiVersion,
};
use nisshi_service::{
    BytesFrameLayer, BytesTcpService, Error as ServiceError, FrameBytesLayer, FrameService,
    TcpListenerInput, TcpListenerLayer, TcpStreamLayer,
};
use rama::{
    Layer as _, Service as _,
    error::BoxError,
    extensions::Extensions,
    tcp::{TcpStream, TokioTcpStream},
};
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;
use url::Url;

/// The API key of each request that reached the origin, in arrival order.
type Received = Arc<Mutex<Vec<i16>>>;

/// Runs an origin that records the API key of each request it receives. It answers
/// SaslHandshake and SaslAuthenticate with success, so a forwarded SASL request also
/// shows up as a wrong error code at the client.
async fn origin(
    listener: TcpListener,
    received: Received,
    cancellation: CancellationToken,
) -> Result<(), BoxError> {
    let service = (
        TcpListenerLayer::new(cancellation),
        TcpStreamLayer,
        BytesFrameLayer::default(),
    )
        .into_layer(FrameService::new(move |input: FrameInput| {
            let api_key = input.frame.api_key()?;
            let correlation_id = input.frame.correlation_id()?;
            received.lock().unwrap().push(api_key);

            let body: Body = match api_key {
                ApiVersionsRequest::KEY => ApiVersionsResponse::default()
                    .error_code(ErrorCode::None.into())
                    .api_keys(Some(
                        [(ApiVersionsRequest::KEY, 3), (MetadataRequest::KEY, 12)]
                            .into_iter()
                            .map(|(api_key, max_version)| {
                                ApiVersion::default()
                                    .api_key(api_key)
                                    .min_version(0)
                                    .max_version(max_version)
                            })
                            .collect(),
                    ))
                    .throttle_time_ms(Some(0))
                    .into(),

                MetadataRequest::KEY => MetadataResponse::default()
                    .brokers(Some([].into()))
                    .topics(Some([].into()))
                    .cluster_id(Some("origin".into()))
                    .controller_id(Some(111))
                    .throttle_time_ms(Some(0))
                    .cluster_authorized_operations(Some(-1))
                    .into(),

                SaslHandshakeRequest::KEY => SaslHandshakeResponse::default()
                    .error_code(ErrorCode::None.into())
                    .mechanisms(Some(["PLAIN".into()].into()))
                    .into(),

                SaslAuthenticateRequest::KEY => SaslAuthenticateResponse::default()
                    .error_code(ErrorCode::None.into())
                    .session_lifetime_ms(Some(0))
                    .into(),

                _ => {
                    return Err(ServiceError::Message(format!(
                        "unexpected api key {api_key}"
                    )));
                }
            };

            Ok::<_, ServiceError>(Frame {
                size: 0,
                header: Header::Response { correlation_id },
                body,
            })
        }));

    service
        .serve(TcpListenerInput {
            listener,
            extensions: Extensions::default(),
        })
        .await
        .map_err(Into::into)
}

/// Starts an origin and a [`Proxy`] in front of it, and returns the proxy's address and
/// the requests that reach the origin.
async fn proxy_with_origin(
    join: &mut JoinSet<Result<(), BoxError>>,
    cancellation: CancellationToken,
) -> Result<(SocketAddr, Received), BoxError> {
    let received = Received::default();

    let origin_listener = TcpListener::bind("127.0.0.1:0").await?;
    let origin_url = Url::parse(&format!("tcp://{}", origin_listener.local_addr()?))?;
    _ = join.spawn(origin(origin_listener, received.clone(), cancellation));

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_addr = proxy_listener.local_addr()?;
    let proxy_url = Url::parse(&format!("tcp://{proxy_addr}"))?;

    let proxy = Proxy::new(proxy_url.clone(), proxy_url, origin_url);
    _ = join.spawn(async move { proxy.serve(proxy_listener).await.map_err(Into::into) });

    Ok((proxy_addr, received))
}

/// Sends one request to the proxy on a new connection, and returns the response.
async fn send(
    proxy_addr: SocketAddr,
    api_key: i16,
    api_version: i16,
    body: Body,
) -> Result<Frame, BoxError> {
    let stream = TcpStream::from_tokio_tcp_stream(
        TokioTcpStream::connect(proxy_addr).await?,
        Extensions::default(),
    );

    FrameBytesLayer
        .into_layer(BytesTcpService::new(stream))
        .serve(FrameInput {
            frame: Frame {
                size: 0,
                header: Header::Request {
                    api_key,
                    api_version,
                    correlation_id: 12321,
                    client_id: Some(env!("CARGO_PKG_NAME").into()),
                },
                body,
            },
            extensions: Extensions::default(),
        })
        .await
        .map_err(Into::into)
}

#[tokio::test]
async fn sasl_requests_never_reach_the_origin() -> Result<(), BoxError> {
    let cancellation = CancellationToken::new();
    let mut join = JoinSet::new();
    let (proxy_addr, received) = proxy_with_origin(&mut join, cancellation.clone()).await?;

    let handshake = send(
        proxy_addr,
        SaslHandshakeRequest::KEY,
        1,
        SaslHandshakeRequest::default()
            .mechanism("PLAIN".into())
            .into(),
    )
    .await
    .and_then(|frame| SaslHandshakeResponse::try_from(frame.body).map_err(Into::into))?;

    assert_eq!(i16::from(ErrorCode::IllegalSaslState), handshake.error_code);

    let authenticate = send(
        proxy_addr,
        SaslAuthenticateRequest::KEY,
        2,
        SaslAuthenticateRequest::default()
            .auth_bytes(b"\0alice\0secret".as_slice().into())
            .into(),
    )
    .await
    .and_then(|frame| SaslAuthenticateResponse::try_from(frame.body).map_err(Into::into))?;

    assert_eq!(
        i16::from(ErrorCode::IllegalSaslState),
        authenticate.error_code
    );

    let received = received.lock().unwrap().clone();
    assert!(
        !received.contains(&SaslHandshakeRequest::KEY),
        "{received:?}"
    );
    assert!(
        !received.contains(&SaslAuthenticateRequest::KEY),
        "{received:?}"
    );

    cancellation.cancel();
    join.abort_all();

    Ok(())
}

#[tokio::test]
async fn metadata_reaches_the_origin() -> Result<(), BoxError> {
    let cancellation = CancellationToken::new();
    let mut join = JoinSet::new();
    let (proxy_addr, received) = proxy_with_origin(&mut join, cancellation.clone()).await?;

    let metadata = send(
        proxy_addr,
        MetadataRequest::KEY,
        12,
        MetadataRequest::default()
            .topics(Some([].into()))
            .allow_auto_topic_creation(Some(false))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false))
            .into(),
    )
    .await
    .and_then(|frame| MetadataResponse::try_from(frame.body).map_err(Into::into))?;

    assert_eq!(Some("origin"), metadata.cluster_id.as_deref());

    let received = received.lock().unwrap().clone();
    assert!(received.contains(&MetadataRequest::KEY), "{received:?}");

    cancellation.cancel();
    join.abort_all();

    Ok(())
}
