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

//! Tests for the version gate on each route registered through `with_capped_route`, and for
//! the `ApiVersions` reply to a request outside the protocol range.

use std::assert_matches;

use bytes::Bytes;
use nisshi_sans_io::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, ApiKey as _, ApiVersionsRequest,
    ApiVersionsResponse, Body, BytesInput, ErrorCode, Frame, FrameInput, Header,
    ListOffsetsRequest, ListOffsetsResponse, ProduceRequest, ProduceResponse, RequestInput,
    api_versions_response::ApiVersion,
};
use nisshi_service::{
    BytesFrameLayer, BytesTcpService, CAPPED_API_VERSIONS, FrameRouteService, ResponseService,
    TcpBytesLayer, TcpListenerInput, TcpListenerLayer,
};
use rama::{
    Layer as _, Service,
    extensions::Extensions,
    tcp::{TcpStream, TokioTcpStream},
};
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::common::Error;

/// A `FrameRouteService` with a stub handler for each capped API.
fn capped_frame_route<E>() -> Result<FrameRouteService<E>, Error>
where
    E: std::error::Error
        + From<nisshi_sans_io::Error>
        + From<nisshi_service::Error>
        + Send
        + Sync
        + 'static,
{
    FrameRouteService::<E>::builder()
        .with_capped_route::<ListOffsetsRequest, _>(ResponseService::new(
            |_: RequestInput<ListOffsetsRequest>| {
                Ok::<_, nisshi_service::Error>(ListOffsetsResponse::default())
            },
        ))?
        .with_capped_route::<ProduceRequest, _>(ResponseService::new(
            |_: RequestInput<ProduceRequest>| {
                Ok::<_, nisshi_service::Error>(ProduceResponse::default())
            },
        ))?
        .with_capped_route::<AddPartitionsToTxnRequest, _>(ResponseService::new(
            |_: RequestInput<AddPartitionsToTxnRequest>| {
                Ok::<_, nisshi_service::Error>(AddPartitionsToTxnResponse::default())
            },
        ))?
        .build()
        .map_err(Error::from)
}

fn default_body(api_key: i16) -> Body {
    match api_key {
        ProduceRequest::KEY => ProduceRequest::default().into(),
        ListOffsetsRequest::KEY => ListOffsetsRequest::default().into(),
        AddPartitionsToTxnRequest::KEY => AddPartitionsToTxnRequest::default().into(),
        otherwise => panic!("no capped API with key {otherwise}"),
    }
}

async fn serve(
    frame_route: &FrameRouteService<Error>,
    api_key: i16,
    api_version: i16,
) -> Result<Frame, Error> {
    frame_route
        .serve(FrameInput {
            frame: Frame {
                size: 0,
                header: Header::Request {
                    api_key,
                    api_version,
                    correlation_id: 0,
                    client_id: None,
                },
                body: default_body(api_key),
            },
            extensions: Extensions::default(),
        })
        .await
}

#[tokio::test]
async fn version_inside_the_cap_reaches_the_handler() -> Result<(), Error> {
    let frame_route = capped_frame_route()?;

    for (api_key, supported) in CAPPED_API_VERSIONS {
        for api_version in [*supported.start(), *supported.end()] {
            let response = serve(&frame_route, api_key, api_version).await?;
            assert!(
                matches!(response.header, Header::Response { .. }),
                "api_key: {api_key}, api_version: {api_version}"
            );
        }
    }

    Ok(())
}

// Each version below is inside the protocol range, so the request passes the protocol-range
// check in `FrameRouteService` and reaches the version gate.
#[tokio::test]
async fn version_outside_the_cap_closes_the_connection() -> Result<(), Error> {
    let frame_route = capped_frame_route()?;

    for (api_key, api_version) in [
        (ProduceRequest::KEY, 2),
        (ListOffsetsRequest::KEY, 7),
        (ListOffsetsRequest::KEY, 9),
        (AddPartitionsToTxnRequest::KEY, 4),
    ] {
        let err = serve(&frame_route, api_key, api_version)
            .await
            .expect_err("a version outside the cap");

        assert!(
            matches!(
                err,
                Error::Service(nisshi_service::Error::UnsupportedVersion {
                    api_key: rejected_key,
                    api_version: rejected_version,
                }) if rejected_key == api_key && rejected_version == api_version
            ),
            "api_key: {api_key}, api_version: {api_version}, err: {err:?}"
        );
    }

    Ok(())
}

/// The correlation id of each `ApiVersions` request above the protocol range below.
const UNSUPPORTED_CORRELATION_ID: i32 = 7;

/// The correlation id of [`API_VERSIONS_V4`].
const V4_CORRELATION_ID: i32 = 8;

/// An `ApiVersions` v4 request, inside the protocol range.
#[rustfmt::skip]
const API_VERSIONS_V4: &[u8] = &[
    0, 0, 0, 16,          // size
    0, 18,                // api_key = ApiVersionsRequest::KEY
    0, 4,                 // api_version = 4
    0, 0, 0, 8,           // correlation_id = V4_CORRELATION_ID
    0xff, 0xff,           // client_id = null
    0x00,                 // header tag buffer: empty
    0x02, b'a',           // client_software_name = "a" (compact string, length + 1)
    0x02, b'1',           // client_software_version = "1"
    0x00,                 // body tag buffer: empty
];

/// Sends `request`, an `ApiVersions` request above the protocol range, to a broker with the
/// capped routes, and checks that the broker answers it as Kafka does
/// ([ApiVersionsRequest.java#L112-L129]): at v0, with `UNSUPPORTED_VERSION`, and with only the
/// `ApiVersions` range that the client retries with.
///
/// Then sends [`API_VERSIONS_V4`] on the same connection and checks that the reply lists every
/// route. This shows that the broker kept the connection open.
///
/// The broker must skip the body of such a request, because a later version adds fields that
/// this build's decoder does not know. `ApiVersionsRequest` v5 adds `ClusterId` and `NodeId` as
/// non-tagged fields ([ApiVersionsRequest.json#L27-L42]).
///
/// [ApiVersionsRequest.java#L112-L129]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsRequest.java#L112-L129
/// [ApiVersionsRequest.json#L27-L42]: https://github.com/apache/kafka/blob/e90d6f42c2957f2b970266e3c5b0ff2ebf972b59/clients/src/main/resources/common/message/ApiVersionsRequest.json#L27-L42
async fn assert_answered_at_v0(request: &'static [u8]) -> Result<(), Error> {
    let _guard = crate::common::init_tracing()?;

    // The capped routes make the full list longer than the `ApiVersions` entry alone.
    let frame_route = capped_frame_route::<nisshi_service::Error>()?;
    let mut route_keys = frame_route.api_keys().collect::<Vec<_>>();
    route_keys.sort_unstable();

    let cancellation = CancellationToken::new();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let local_addr = listener.local_addr()?;

    let mut join = JoinSet::new();
    {
        let cancellation = cancellation.clone();
        let server = (
            TcpListenerLayer::new(cancellation),
            TcpBytesLayer,
            BytesFrameLayer::default(),
        )
            .into_layer(frame_route);

        _ = join.spawn(async move {
            server
                .serve(TcpListenerInput {
                    listener,
                    extensions: Extensions::default(),
                })
                .await
        });
    }

    let stream = TcpStream::from_tokio_tcp_stream(
        TokioTcpStream::connect(local_addr).await?,
        Extensions::default(),
    );
    let client = BytesTcpService::new(stream);

    let unsupported = client
        .serve(BytesInput {
            bytes: Bytes::from_static(request),
            extensions: Extensions::default(),
        })
        .await;
    let in_range = client
        .serve(BytesInput {
            bytes: Bytes::from_static(API_VERSIONS_V4),
            extensions: Extensions::default(),
        })
        .await;

    cancellation.cancel();
    let joined = join.join_all().await;
    debug!(?joined);

    let response = Frame::response_from_bytes(unsupported?, ApiVersionsRequest::KEY, 0)?;
    assert_matches!(
        response.header,
        Header::Response {
            correlation_id: UNSUPPORTED_CORRELATION_ID
        }
    );

    let response = ApiVersionsResponse::try_from(response.body)?;
    assert_eq!(
        ErrorCode::UnsupportedVersion,
        ErrorCode::try_from(response.error_code)?
    );
    let api_versions = response
        .api_keys
        .unwrap_or_default()
        .into_iter()
        .map(|api| (api.api_key, api.min_version, api.max_version))
        .collect::<Vec<_>>();
    assert_eq!(vec![(ApiVersionsRequest::KEY, 0, 4)], api_versions);

    let response = Frame::response_from_bytes(in_range?, ApiVersionsRequest::KEY, 4)?;
    assert_matches!(
        response.header,
        Header::Response {
            correlation_id: V4_CORRELATION_ID
        }
    );

    let response = ApiVersionsResponse::try_from(response.body)?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
    let mut listed_keys = response
        .api_keys
        .unwrap_or_default()
        .into_iter()
        .map(|api| api.api_key)
        .collect::<Vec<_>>();
    listed_keys.sort_unstable();
    assert_eq!(route_keys, listed_keys);

    Ok(())
}

#[tokio::test]
async fn api_versions_v5_with_a_null_cluster_id_is_answered_at_v0() -> Result<(), Error> {
    #[rustfmt::skip]
    let request = &[
        0, 0, 0, 19,          // size
        0, 18,                // api_key = ApiVersionsRequest::KEY
        0, 5,                 // api_version = 5
        0, 0, 0, 7,           // correlation_id = UNSUPPORTED_CORRELATION_ID
        0xff, 0xff,           // client_id = null
        0x00,                 // header tag buffer: empty
        0x00,                 // client_software_name: compact null
        0x00,                 // client_software_version: compact null
        0x00,                 // cluster_id: compact null
        0xff, 0xff, 0xff, 0xff, // node_id = -1
        0x00,                 // body tag buffer: empty
    ];

    assert_answered_at_v0(request).await
}

/// An `ApiVersions` v5 request with `ClusterId` set, as franz-go sends to a broker that it
/// found in Metadata.
#[rustfmt::skip]
const API_VERSIONS_V5: &[u8] = &[
    0, 0, 0, 22,          // size
    0, 18,                // api_key = ApiVersionsRequest::KEY
    0, 5,                 // api_version = 5
    0, 0, 0, 7,           // correlation_id = UNSUPPORTED_CORRELATION_ID
    0xff, 0xff,           // client_id = null
    0x00,                 // header tag buffer: empty
    0x00,                 // client_software_name: compact null
    0x00,                 // client_software_version: compact null
    0x04, b'a', b'b', b'c', // cluster_id = "abc" (compact string, length + 1)
    0, 0, 0, 111,         // node_id = 111
    0x00,                 // body tag buffer: empty
];

#[tokio::test]
async fn api_versions_v5_with_a_cluster_id_is_answered_at_v0() -> Result<(), Error> {
    assert_answered_at_v0(API_VERSIONS_V5).await
}

/// A frame service that answers like a Kafka origin with a newer `ApiVersions` range than
/// this build's, as the origin behind the proxy can be.
#[derive(Clone, Debug)]
struct NewerOrigin;

impl Service<FrameInput> for NewerOrigin {
    type Output = Frame;
    type Error = nisshi_service::Error;

    async fn serve(&self, _req: FrameInput) -> Result<Self::Output, Self::Error> {
        Ok(Frame {
            size: 0,
            header: Header::Response { correlation_id: 0 },
            body: ApiVersionsResponse::default()
                .error_code(ErrorCode::None.into())
                .api_keys(Some(vec![
                    ApiVersion::default()
                        .api_key(ApiVersionsRequest::KEY)
                        .min_version(0)
                        .max_version(5),
                    ApiVersion::default()
                        .api_key(ProduceRequest::KEY)
                        .min_version(3)
                        .max_version(12),
                ]))
                .into(),
        })
    }
}

// The reply must offer a version that this build accepts. franz-go retries only at a version
// lower than the one it sent, and fails the connection otherwise.
#[tokio::test]
async fn api_versions_reply_lists_this_builds_range_not_the_inner_services() -> Result<(), Error> {
    let _guard = crate::common::init_tracing()?;

    let service = BytesFrameLayer::default().into_layer(NewerOrigin);
    let response = service
        .serve(BytesInput {
            bytes: Bytes::from_static(API_VERSIONS_V5),
            extensions: Extensions::default(),
        })
        .await?;

    let response = Frame::response_from_bytes(response, ApiVersionsRequest::KEY, 0)?;
    let response = ApiVersionsResponse::try_from(response.body)?;
    assert_eq!(
        ErrorCode::UnsupportedVersion,
        ErrorCode::try_from(response.error_code)?
    );
    let api_versions = response
        .api_keys
        .unwrap_or_default()
        .into_iter()
        .map(|api| (api.api_key, api.min_version, api.max_version))
        .collect::<Vec<_>>();
    assert_eq!(vec![(ApiVersionsRequest::KEY, 0, 4)], api_versions);

    Ok(())
}

#[tokio::test]
async fn api_versions_v6_with_unknown_fields_is_answered_at_v0() -> Result<(), Error> {
    #[rustfmt::skip]
    let request = &[
        0, 0, 0, 26,          // size
        0, 18,                // api_key = ApiVersionsRequest::KEY
        0, 6,                 // api_version = 6
        0, 0, 0, 7,           // correlation_id = UNSUPPORTED_CORRELATION_ID
        0xff, 0xff,           // client_id = null
        0x00,                 // header tag buffer: empty
        0x00,                 // client_software_name: compact null
        0x00,                 // client_software_version: compact null
        0x04, b'a', b'b', b'c', // cluster_id = "abc" (compact string, length + 1)
        0, 0, 0, 111,         // node_id = 111
        0x00,                 // body tag buffer: empty
        0xde, 0xad, 0xbe, 0xef, // fields that this build does not know
    ];

    assert_answered_at_v0(request).await
}

#[tokio::test]
async fn api_versions_v32767_with_an_empty_body_is_answered_at_v0() -> Result<(), Error> {
    #[rustfmt::skip]
    let request = &[
        0, 0, 0, 11,          // size: the header only
        0, 18,                // api_key = ApiVersionsRequest::KEY
        0x7f, 0xff,           // api_version = 32767
        0, 0, 0, 7,           // correlation_id = UNSUPPORTED_CORRELATION_ID
        0xff, 0xff,           // client_id = null
        0x00,                 // header tag buffer: empty
    ];

    assert_answered_at_v0(request).await
}
