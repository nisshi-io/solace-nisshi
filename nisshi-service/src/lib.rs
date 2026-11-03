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

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
//! Common service layers used in other Nisshi crates.
//!
//! ## Overview
//!
//! This crate provides [Layer][`rama::Layer`] and [Service][`rama::Service`]
//! implementations for operating on [`Frame`], [`Body`], [Request][`nisshi_sans_io::Request`]
//! and [Response][`nisshi_sans_io::Response`].
//!
//! The following transports are provided:
//!
//! - TCP with [`TcpBytesLayer`] and [`BytesTcpService`].
//! - [`MPSC channel`][`tokio::sync::mpsc`] with [`ChannelFrameLayer`] and [`ChannelFrameService`].
//! - [`Bytes`][`bytes::Bytes`] with [`BytesLayer`] (designed primarily for protocol testing)
//!
//! ### Routing
//!
//! Route [`Frame`] to services using [`FrameRouteService`] to automatically
//! implement [`ApiVersionsRequest`][`nisshi_sans_io::ApiVersionsRequest`]
//! with valid protocol ranges:
//!
//! ```
//! # use nisshi_service::Error;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! # use rama::{Layer as _, Service as _};
//! # use nisshi_sans_io::{ApiKey as _, ApiVersionsRequest, MetadataRequest, MetadataResponse};
//! # use nisshi_service::{
//! #     BytesFrameLayer, BytesFrameService, BytesLayer, BytesService, FrameBytesLayer,
//! #     FrameBytesService, FrameRouteService, RequestFrameLayer, RequestFrameService, RequestLayer,
//! #     ResponseService,
//! # };
//! let frame_route = FrameRouteService::<Error>::builder()
//!     .with_service(
//!         RequestLayer::<MetadataRequest>::new().into_layer(ResponseService::new(|_| {
//!             Ok(MetadataResponse::default()
//!                 .brokers(Some([].into()))
//!                 .topics(Some([].into()))
//!                 .cluster_id(Some("nisshi".into()))
//!                 .controller_id(Some(111))
//!                 .throttle_time_ms(Some(0))
//!                 .cluster_authorized_operations(Some(-1)))
//!         })),
//!     )
//!     .and_then(|builder| builder.build())?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Layering
//!
//! Composing [`RequestFrameLayer`], [`FrameBytesLayer`], [`BytesLayer`],
//! [`BytesFrameLayer`] together into `frame_route` to implement a test protocol stack.
//!
//! A "client" [`Frame`] is marshalled into bytes using [`FrameBytesLayer`], with [`BytesLayer`] connecting
//! to a "server" that demarshalls using [`BytesFrameLayer`] back into frames,
//! routing into `frame_route` (above) to [`MetadataRequest`][`nisshi_sans_io::MetadataRequest`]
//! or [`ApiVersionsRequest`][`nisshi_sans_io::ApiVersionsRequest`] depending on the
//! [API key][`Frame#method.api_key`]:
//!
//! ```
//! # use nisshi_service::Error;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! # use rama::{Layer as _, Service as _};
//! # use nisshi_sans_io::{ApiKey as _, ApiVersionsRequest, MetadataRequest, MetadataResponse};
//! # use nisshi_service::{
//! #     BytesFrameLayer, BytesFrameService, BytesLayer, BytesService, FrameBytesLayer,
//! #     FrameBytesService, FrameRouteService, RequestFrameLayer, RequestFrameService, RequestLayer,
//! #     ResponseService,
//! # };
//! # let frame_route = FrameRouteService::<Error>::builder()
//! #     .with_service(
//! #         RequestLayer::<MetadataRequest>::new().into_layer(ResponseService::new(|_| {
//! #             Ok(MetadataResponse::default()
//! #                 .brokers(Some([].into()))
//! #                 .topics(Some([].into()))
//! #                 .cluster_id(Some("nisshi".into()))
//! #                 .controller_id(Some(111))
//! #                 .throttle_time_ms(Some(0))
//! #                 .cluster_authorized_operations(Some(-1)))
//! #         })),
//! #     )
//! #     .and_then(|builder| builder.build())?;
//!   let service = (
//!       // "client" initiator side:
//!       RequestFrameLayer,
//!       FrameBytesLayer,
//!
//!       // transport
//!       BytesLayer,
//!
//!       // "server" side:
//!       BytesFrameLayer::default(),
//!   )
//!       .into_layer(frame_route);
//! # Ok(())
//! # }
//! ```
//!
//! In the broker, proxy and CLI clients, [`BytesLayer`] is replaced with
//! [`TcpBytesLayer`] (server side) or [`BytesTcpService`] (client/initiator side).
//!
//! ### Servicing
//!
//! We construct a [`MetadataRequest`][`nisshi_sans_io::MetadataRequest`] to
//! initiate a request on the `service`. The request passes through the protocol stack
//! and routed into our service. The service responds with a
//! [`MetadataResponse`][`nisshi_sans_io::MetadataResponse`], so that we can
//! verify the expected `response.cluster_id`:
//!
//! ```
//! # use nisshi_service::Error;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! # use rama::{Layer as _, Service as _};
//! # use nisshi_sans_io::{ApiKey as _, ApiVersionsRequest, MetadataRequest, MetadataResponse};
//! # use nisshi_service::{
//! #     BytesFrameLayer, BytesFrameService, BytesLayer, BytesService, FrameBytesLayer,
//! #     FrameBytesService, FrameRouteService, RequestFrameLayer, RequestFrameService, RequestLayer,
//! #     ResponseService,
//! # };
//! # let frame_route = FrameRouteService::<Error>::builder()
//! #     .with_service(
//! #         RequestLayer::<MetadataRequest>::new().into_layer(ResponseService::new(|_| {
//! #             Ok(MetadataResponse::default()
//! #                 .brokers(Some([].into()))
//! #                 .topics(Some([].into()))
//! #                 .cluster_id(Some("nisshi".into()))
//! #                 .controller_id(Some(111))
//! #                 .throttle_time_ms(Some(0))
//! #                 .cluster_authorized_operations(Some(-1)))
//! #         })),
//! #     )
//! #     .and_then(|builder| builder.build())?;
//! # let service = (
//! #      RequestFrameLayer,
//! #      FrameBytesLayer,
//! #      BytesLayer,
//! #      BytesFrameLayer::default(),
//! #  )
//! #      .into_layer(frame_route);
//!   let request = MetadataRequest::default()
//!       .topics(Some([].into()))
//!       .allow_auto_topic_creation(Some(false))
//!       .include_cluster_authorized_operations(Some(false))
//!       .include_topic_authorized_operations(Some(false));
//!
//!   let response = service.serve(request.into()).await?;
//!
//!   assert_eq!(Some("nisshi".into()), response.cluster_id);
//! # Ok(())
//! # }
//! ```
//!
//! The [`FrameRouteService`] automatically implements
//! [`ApiVersionsRequest`][`nisshi_sans_io::ApiVersionsRequest`]
//! with valid protocol ranges for all defined services. An
//! [`ApiVersionsResponse`][`nisshi_sans_io::ApiVersionsResponse`] contains
//! version information for both [`MetadataRequest`][`nisshi_sans_io::MetadataRequest`]
//! and [`ApiVersionsRequest`][`nisshi_sans_io::ApiVersionsRequest`]:
//!
//! ```
//! # use nisshi_service::Error;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! # use rama::{Layer as _, Service as _};
//! # use nisshi_sans_io::{ApiKey as _, ApiVersionsRequest, MetadataRequest, MetadataResponse};
//! # use nisshi_service::{
//! #     BytesFrameLayer, BytesFrameService, BytesLayer, BytesService, FrameBytesLayer,
//! #     FrameBytesService, FrameRouteService, RequestFrameLayer, RequestFrameService, RequestLayer,
//! #     ResponseService,
//! # };
//! # let frame_route = FrameRouteService::<Error>::builder()
//! #     .with_service(
//! #         RequestLayer::<MetadataRequest>::new().into_layer(ResponseService::new(|_| {
//! #             Ok(MetadataResponse::default()
//! #                 .brokers(Some([].into()))
//! #                 .topics(Some([].into()))
//! #                 .cluster_id(Some("nisshi".into()))
//! #                 .controller_id(Some(111))
//! #                 .throttle_time_ms(Some(0))
//! #                 .cluster_authorized_operations(Some(-1)))
//! #         })),
//! #     )
//! #     .and_then(|builder| builder.build())?;
//! # let service = (
//! #      RequestFrameLayer,
//! #      FrameBytesLayer,
//! #      BytesLayer,
//! #      BytesFrameLayer::default(),
//! #  )
//! #      .into_layer(frame_route);
//! let response = service
//!     .serve(
//!         ApiVersionsRequest::default()
//!             .client_software_name(Some("abcba".into()))
//!             .client_software_version(Some("1.2321".into()))
//!             .into(),
//!     )
//!     .await?;
//!
//! let api_versions = response
//!     .api_keys
//!     .unwrap_or_default()
//!     .into_iter()
//!     .map(|api_version| api_version.api_key)
//!     .collect::<Vec<_>>();
//!
//! assert_eq!(2, api_versions.len());
//! assert!(api_versions.contains(&ApiVersionsRequest::KEY));
//! assert!(api_versions.contains(&MetadataRequest::KEY));
//! # Ok(())
//! # }
//! ```

use std::{
    fmt, io,
    net::SocketAddr,
    ops::{Range, RangeInclusive},
    sync::{Arc, LazyLock, Mutex, PoisonError},
    time::{Duration, SystemTime},
};

use indicatif::ProgressBar;
use nisshi_sans_io::{Body, Frame};
use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Histogram, Meter},
};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use rama::{Layer, Service, extensions::Extension};
use rand::{prelude::*, rngs::SmallRng};
use tokio::{net::lookup_host, sync::oneshot, task::JoinError, time::sleep};
use tracing::{debug, instrument};
use url::Url;

mod api;
mod channel;
mod consumer;
mod frame;
mod input;
mod peer_address;
mod stream;
mod transaction_abortable;
mod version_gate;

pub use api::{ApiVersionsService, FrameRouteBuilder, FrameRouteService};

pub use version_gate::{
    CAPPED_API_VERSIONS, VersionGateLayer, VersionGateService, capped_range, routable_max_version,
};

pub use input::TcpListenerInput;

pub use transaction_abortable::{
    TransactionAbortableDowngradeLayer, TransactionAbortableDowngradeService,
};

pub use channel::{
    ChannelFrameLayer, ChannelFrameService, FrameChannelService, FrameReceiver, FrameSender,
    bounded_channel,
};

pub use consumer::{ConsumerGroupLayer, ConsumerGroupService};

pub use peer_address::PeerAddressExtension;

pub use frame::{
    BodyRequestLayer, BytesFrameLayer, BytesFrameService, FrameApiKeyMatcher, FrameBodyLayer,
    FrameBytesLayer, FrameBytesService, FrameRequestLayer, FrameRequestService, FrameService,
    RequestApiKeyMatcher, RequestFrameLayer, RequestFrameService, RequestLayer, ResponseService,
};

pub use stream::{
    BytesLayer, BytesService, BytesTcpService, DEFAULT_CONNECTION_IDLE_TIMEOUT,
    DEFAULT_IO_IDLE_TIMEOUT, DEFAULT_MAXIMUM_FRAME_SIZE,
    DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE, TcpBytesLayer, TcpBytesService, TcpContext,
    TcpContextLayer, TcpContextService, TcpListenerLayer, TcpStreamLayer, TcpStreamService,
};

#[derive(Clone, Debug, Extension)]
pub struct ProgressBarExtension(ProgressBar);

impl ProgressBarExtension {
    pub fn new(progress_bar: ProgressBar) -> Self {
        Self(progress_bar)
    }
}

impl AsRef<ProgressBar> for ProgressBarExtension {
    fn as_ref(&self) -> &ProgressBar {
        &self.0
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    Auth(#[from] nisshi_auth::Error),
    /// An entry in [`CAPPED_API_VERSIONS`] is not a subset of `protocol`, the Kafka protocol's
    /// own valid range for `api_key` (`None` when this build has no protocol metadata for
    /// `api_key`).
    CapRangeExceedsProtocolRange {
        api_key: i16,
        declared: RangeInclusive<i16>,
        protocol: Option<RangeInclusive<i16>>,
    },
    DuplicateRoute(i16),
    FrameTooBig(usize),
    InvalidFrameLength(i32),
    Io(Arc<io::Error>),
    Join(Arc<JoinError>),
    Message(String),
    /// [`FrameRouteBuilder::wrap_route`] found no registered route of `api_key` to wrap.
    NoRouteToWrap(i16),
    OneshotRecv(oneshot::error::RecvError),
    Poison,
    /// A connection that has not authenticated declared a frame larger than
    /// [`TcpContext::pre_authentication_maximum_frame_size`] allows.
    PreAuthenticationFrameTooBig {
        size: usize,
        limit: usize,
    },
    Parse(#[from] url::ParseError),
    Protocol(#[from] nisshi_sans_io::Error),
    UnableToSend(Box<Frame>),
    /// [`FrameRouteBuilder::with_capped_route`] registered an `api_key` that
    /// [`CAPPED_API_VERSIONS`] does not cap.
    UncappedApi(i16),
    UnknownHost(Url),
    UnknownServiceBody(Box<Body>),
    /// No route handles `api_key`. The error names the request, not its
    /// decoded body: the body comes from the client, and this error is logged.
    /// `api_name` is the API's fixed name, so the line identifies the API
    /// without growing with the request.
    UnknownServiceFrame {
        api_key: i16,
        api_name: &'static str,
        api_version: i16,
        correlation_id: i32,
        size: i32,
    },
    /// The broker does not route `api_version` for `api_key`, because the version is outside
    /// the protocol's valid range or outside the range the broker advertises for that API.
    /// The caller closes the connection on this error, as Kafka does for a version it has not
    /// enabled.
    UnsupportedVersion {
        api_key: i16,
        api_version: i16,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<JoinError> for Error {
    fn from(value: JoinError) -> Self {
        Self::Join(Arc::new(value))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

/// Payload size declared by a frame's 4 byte size prefix.
///
/// The prefix is untrusted input from the peer: a negative size is a protocol
/// error, and letting it through would otherwise wrap into a multi-gigabyte
/// allocation.
fn frame_size(encoded: [u8; 4]) -> Result<usize, Error> {
    let size = i32::from_be_bytes(encoded);

    usize::try_from(size).map_err(|_| Error::InvalidFrameLength(size))
}

/// Total length of a frame, the 4 byte size prefix plus the payload it declares.
///
/// Fails with [`Error::InvalidFrameLength`] when the prefix declares a negative size.
pub fn frame_length(encoded: [u8; 4]) -> Result<usize, Error> {
    frame_size(encoded).map(|size| size + encoded.len())
}

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

/// Return the socket address for a given URL
///
/// Recording DNS lookup timings in the `DNS_LOOKUP_DURATION` histogram.
///
/// ```rust
/// # use nisshi_service::{Error, host_port};
/// # use url::Url;
/// # use std::net::Ipv4Addr;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let earl = Url::parse("tcp://localhost:9092")?;
/// let sock_addr = host_port(earl).await?;
/// assert_eq!(sock_addr.ip(), Ipv4Addr::new(127, 0, 0, 1));
/// assert_eq!(sock_addr.port(), 9092);
/// # Ok(())
/// # }
/// ```
pub async fn host_port(url: Url) -> Result<SocketAddr, Error> {
    if let Some(host) = url.host_str()
        && let Some(port) = url.port()
    {
        let attributes = [KeyValue::new("url", url.to_string())];
        let start = SystemTime::now();

        let mut addresses = lookup_host(format!("{host}:{port}"))
            .await
            .inspect(|_| {
                DNS_LOOKUP_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &attributes,
                )
            })?
            .filter(|socket_addr| matches!(socket_addr, SocketAddr::V4(_)));

        if let Some(socket_addr) = addresses.next().inspect(|socket_addr| debug!(?socket_addr)) {
            return Ok(socket_addr);
        }
    }

    Err(Error::UnknownHost(url))
}

pub(crate) static DNS_LOOKUP_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("dns_lookup_duration")
        .with_unit("ms")
        .with_description("DNS lookup latencies")
        .build()
});

pub(crate) static REQUEST_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_request_size")
        .with_unit("By")
        .with_description("The API request size in bytes")
        .build()
});

pub(crate) static RESPONSE_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_response_size")
        .with_unit("By")
        .with_description("The API response size in bytes")
        .build()
});

pub(crate) static REQUEST_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_request_duration")
        .with_unit("ms")
        .with_description("The API request latencies in milliseconds")
        .build()
});

pub(crate) static API_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_api_requests")
        .with_description("The number of API requests made")
        .build()
});

pub(crate) static API_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_api_errors")
        .with_description("The number of API errors")
        .build()
});

pub(crate) static BYTES_SENT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_bytes_sent")
        .with_description("The number of bytes sent")
        .build()
});

pub(crate) static FRAMES_REJECTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_frames_rejected")
        .with_description(
            "The number of frames a listener rejected for their declared size, by limit",
        )
        .build()
});

pub(crate) static BYTES_RECEIVED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_bytes_received")
        .with_description("The number of bytes received")
        .build()
});

#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct LatencyIntroducingLayer {
    seed: u64,
    latency: Range<u64>,
}

impl LatencyIntroducingLayer {
    pub fn with_seed(self, seed: u64) -> Self {
        Self { seed, ..self }
    }

    pub fn with_latency_millis(self, latency: Range<u64>) -> Self {
        Self { latency, ..self }
    }
}

impl<S> Layer<S> for LatencyIntroducingLayer {
    type Service = LatencyIntroducingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service::new(inner)
            .with_seed(self.seed)
            .with_latency(self.latency.clone())
    }
}

#[derive(Clone)]
pub struct LatencyIntroducingService<S> {
    inner: S,
    rng: Arc<Mutex<SmallRng>>,
    latency: Range<u64>,
}

impl<S> fmt::Debug for LatencyIntroducingService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(LatencyIntroducingService))
            .field("latency", &self.latency)
            .finish_non_exhaustive()
    }
}

impl<S> LatencyIntroducingService<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            rng: Arc::new(Mutex::new(SmallRng::seed_from_u64(0))),
            latency: 50..150,
        }
    }

    pub fn with_seed(self, seed: u64) -> Self {
        Self {
            rng: Arc::new(Mutex::new(SmallRng::seed_from_u64(seed))),
            ..self
        }
    }

    pub fn with_latency(self, latency: Range<u64>) -> Self {
        Self { latency, ..self }
    }

    #[instrument(skip_all)]
    async fn introduce_latency(&self) -> Result<(), Error> {
        let latency = self
            .rng
            .lock()
            .map(|mut rng| rng.random_range(self.latency.clone()))
            .map(Duration::from_millis)
            .inspect(|latency| debug!(?latency))?;

        sleep(latency).await;

        Ok(())
    }
}

impl<S, Q> Service<Q> for LatencyIntroducingService<S>
where
    S: Service<Q>,
    Q: Send + 'static,
    S::Error: From<Error>,
{
    type Output = S::Output;

    type Error = S::Error;

    #[instrument(skip_all)]
    async fn serve(&self, req: Q) -> Result<Self::Output, Self::Error> {
        self.introduce_latency().await?;
        let result = self.inner.serve(req).await;
        self.introduce_latency().await?;
        result
    }
}
