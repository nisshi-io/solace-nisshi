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
//! Nisshi Client
//!
//! Nisshi API client.
//!
//! # Simple [`Request`] client
//!
//! ```no_run
//! use nisshi_client::{Client, ConnectionManager, Error};
//! use nisshi_sans_io::MetadataRequest;
//! use rama::{Service as _};
//! use url::Url;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let origin = ConnectionManager::builder(Url::parse("tcp://localhost:9092")?)
//!     .client_id(Some(env!("CARGO_PKG_NAME").into()))
//!     .build()
//!     .await
//!     .map(Client::new)?;
//!
//! let response = origin
//!     .call(
//!         MetadataRequest::default()
//!             .topics(Some([].into()))
//!             .allow_auto_topic_creation(Some(false))
//!             .include_cluster_authorized_operations(Some(false))
//!             .include_topic_authorized_operations(Some(false)),
//!     )
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Proxy: [`Layer`] Composition
//!
//! An example API proxy listening for requests on `tcp://localhost:9092` that
//! forwards each [`Frame`] to an origin broker on `tcp://example.com:9092`.
//!
//! The pool shares each origin connection between the proxy's clients, and the
//! origin binds a SASL login to the connection that carried it. A forwarded login
//! therefore applies to every client that later uses that connection. This example
//! forwards every frame, so it suits only an origin that requires no authentication.
//! A proxy for any other origin must answer SaslHandshake and SaslAuthenticate itself.
//!
//! ```no_run
//! use rama::{Layer as _, Service as _, extensions::Extensions};
//! use nisshi_client::{
//!     BytesConnectionService, ConnectionManager, Error, FrameConnectionLayer,
//!     FramePoolLayer,
//! };
//! use nisshi_service::{
//!     BytesFrameLayer, TcpBytesLayer, TcpContextLayer, TcpListenerInput, TcpListenerLayer,
//!     host_port,
//! };
//! use tokio::net::TcpListener;
//! use tokio_util::sync::CancellationToken;
//! use url::Url;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! // forward protocol frames to the origin using a connection pool:
//! let origin = ConnectionManager::builder(Url::parse("tcp://example.com:9092")?)
//!     .client_id(Some(env!("CARGO_PKG_NAME").into()))
//!     .build()
//!     .await?;
//!
//! // a tcp listener used by the proxy
//! let listener =
//!     TcpListener::bind(host_port(Url::parse("tcp://localhost:9092")?).await?).await?;
//!
//! // listen for requests until cancelled
//! let token = CancellationToken::new();
//!
//! let stack = (
//!     // server layers: reading tcp -> bytes -> frames:
//!     TcpListenerLayer::new(token),
//!     TcpContextLayer::default(),
//!     TcpBytesLayer::default(),
//!     BytesFrameLayer::default(),
//!
//!     // client layers: writing frames -> connection pool -> bytes -> origin:
//!     FramePoolLayer::new(origin),
//!     FrameConnectionLayer,
//! )
//!     .into_layer(BytesConnectionService);
//!
//! stack
//!     .serve(TcpListenerInput {
//!         listener,
//!         extensions: Extensions::default(),
//!     })
//!     .await?;
//!
//! # Ok(())
//! # }
//! ```

use std::{
    cmp,
    collections::BTreeMap,
    error, fmt, io,
    sync::{Arc, LazyLock, PoisonError},
    time::{Instant, SystemTime},
};

use backoff::{ExponentialBackoffBuilder, future::retry};
use bytes::Bytes;
use deadpool::{
    Runtime,
    managed::{self, BuildError, Object, PoolError, RecycleError, TimeoutType},
};
use nisshi_sans_io::{
    ApiKey, ApiVersionsRequest, Body, Frame, FrameInput, Header, Request, RootMessageMeta,
};
use nisshi_service::{frame_length, host_port};
use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Gauge, Histogram, Meter},
};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use rama::{
    Layer, Service,
    extensions::{Extensions, ExtensionsRef},
};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpStream,
    task::JoinError,
    time::{Duration, timeout},
};
use tracing::{Instrument, Level, debug, span};
use tracing_subscriber::filter::ParseError;
use url::Url;

mod consumer;
mod security;

pub use consumer::{ConsumerGroupLayer, ConsumerGroupService};
pub use security::{Sasl, SaslMechanism, Security, Tls, TlsOptions};
use security::{Session, Stream};

/// Client Errors
#[derive(thiserror::Error, Clone, Debug)]
pub enum Error {
    /// The broker refused the SASL login, or does not offer the mechanism. The message says why.
    Authentication(String),
    DeadPoolBuild(#[from] BuildError),
    Io(Arc<io::Error>),
    Join(Arc<JoinError>),
    Message(String),
    ParseFilter(Arc<ParseError>),
    ParseUrl(#[from] url::ParseError),
    Poison,
    Pool(Arc<Box<dyn error::Error + Send + Sync>>),
    Protocol(#[from] nisshi_sans_io::Error),
    Service(#[from] nisshi_service::Error),
    Timeout(Duration),
    /// The TLS options can't be used: a certificate, the private key or the server name. Nothing
    /// has connected yet.
    TlsConfiguration(String),
    /// The TLS handshake with the broker failed: its certificate is not trusted or is not valid
    /// for its name, it wants a client certificate, or its listener does not use TLS.
    Tls(String),
    UnknownApiKey(i16),
    UnknownHost(Url),
}

impl Error {
    fn tls(error: impl fmt::Display) -> Self {
        Self::TlsConfiguration(error.to_string())
    }

    /// Returns whether the client failed to open a connection to the broker, as opposed to
    /// failing after it connected. A pool error that comes from a refused, failed or timed out
    /// connection attempt is a connect failure, and a pool error that comes from waiting for a
    /// free connection is not.
    pub fn is_connect_failure(&self) -> bool {
        match self {
            Self::Pool(cause) => {
                self.login_failure().is_none()
                    && self.tls_failure().is_none()
                    && !matches!(
                        cause.downcast_ref::<PoolError<Error>>(),
                        Some(
                            PoolError::Timeout(TimeoutType::Wait | TimeoutType::Recycle)
                                | PoolError::Closed
                                | PoolError::NoRuntimeSpecified
                        )
                    )
            }
            Self::UnknownHost(_) => true,
            _ => false,
        }
    }

    /// Returns why the TLS handshake with the broker failed, when the client connected and the
    /// handshake then failed: the broker's certificate is not trusted or is not valid for the
    /// broker's name, the broker wants a client certificate, or its listener does not use TLS.
    pub fn tls_failure(&self) -> Option<&str> {
        match self {
            Self::Tls(message) => Some(message),
            Self::Pool(cause) => match cause.downcast_ref::<PoolError<Error>>() {
                Some(PoolError::Backend(Self::Tls(message))) => Some(message),
                _ => None,
            },
            _ => None,
        }
    }

    /// Returns why the broker refused the SASL login, when the client connected and the
    /// broker then refused it or does not offer the mechanism.
    pub fn login_failure(&self) -> Option<&str> {
        match self {
            Self::Authentication(message) => Some(message),
            Self::Pool(cause) => match cause.downcast_ref::<PoolError<Error>>() {
                Some(PoolError::Backend(Self::Authentication(message))) => Some(message),
                _ => None,
            },
            _ => None,
        }
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
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

impl<E> From<PoolError<E>> for Error
where
    E: error::Error + Send + Sync + 'static,
{
    fn from(value: PoolError<E>) -> Self {
        Self::Pool(Arc::new(Box::new(value)))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        Self::ParseFilter(Arc::new(value))
    }
}

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

///  Broker connection stream with [`correlation id`][`Header#variant.Request.field.correlation_id`]
#[derive(Debug)]
pub struct Connection {
    stream: Stream,
    correlation_id: i32,

    /// When the broker stops accepting requests on this connection until it logs in again,
    /// after the session lifetime that its SaslAuthenticate response sets. The client doesn't
    /// re-authenticate: the pool discards the connection a little before then, and a new one
    /// logs in.
    session_expires: Option<Instant>,

    /// True from the write of a request until its complete response is read. A request
    /// that fails or is cancelled leaves it true, and the pool then discards the
    /// connection, because the next read could return that request's late response.
    in_flight: bool,
}

/// Manager of supported API versions for a broker
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectionManager {
    broker: Url,
    client_id: Option<String>,
    security: Security,
    versions: BTreeMap<i16, i16>,
    connect_timeout: Duration,
    max_idle: Duration,
    request_timeout: Duration,
    max_request_wait: Duration,
}

impl ConnectionManager {
    /// Build a manager with a broker endpoint
    pub fn builder(broker: Url) -> Builder {
        Builder::broker(broker)
    }

    /// Client id used in requests to the broker
    pub fn client_id(&self) -> Option<String> {
        self.client_id.clone()
    }

    /// The version supported by the broker for a given api key
    pub fn api_version(&self, api_key: i16) -> Result<i16, Error> {
        self.versions
            .get(&api_key)
            .copied()
            .ok_or(Error::UnknownApiKey(api_key))
    }
}

/// The default of Kafka's `socket.connection.setup.timeout.ms`: the longest that one
/// connection attempt waits. A broker that drops packets otherwise holds an attempt until
/// the operating system gives up, which takes minutes.
const CONNECT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);

impl managed::Manager for ConnectionManager {
    type Type = Connection;
    type Error = Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        debug!(%self.broker);

        let attributes = [KeyValue::new("broker", self.broker.to_string())];
        let start = SystemTime::now();

        let addr = host_port(self.broker.clone()).await?;

        let backoff = ExponentialBackoffBuilder::new()
            .with_max_elapsed_time(Some(self.connect_timeout))
            .build();
        let stream = retry(backoff, || async {
            Ok(timeout(CONNECT_ATTEMPT_TIMEOUT, TcpStream::connect(addr))
                .await
                .unwrap_or_else(|_elapsed| Err(io::ErrorKind::TimedOut.into()))
                .inspect(|_| {
                    TCP_CONNECT_DURATION.record(
                        start
                            .elapsed()
                            .map_or(0, |duration| duration.as_millis() as u64),
                        &attributes,
                    )
                })
                .inspect_err(|err| {
                    debug!(broker = %self.broker, ?err, elapsed = start.elapsed().map_or(0, |duration| duration.as_millis() as u64));
                    TCP_CONNECT_ERRORS.add(1, &attributes);
                })?)
        })
        .await?;

        // A refused TLS handshake or SASL login is not retried: the same credentials fail
        // the same way, so the caller gets the reason at once.
        let mut stream = match self.security.tls_config() {
            Some(tls) => tls.connect(&self.broker, stream).await?,
            None => Stream::Plain(stream),
        };

        let mut correlation_id = 0;
        let mut session = Session::default();

        if let Some(sasl) = self.security.sasl_config() {
            session = sasl
                .login(&mut stream, self.client_id.as_deref(), &mut correlation_id)
                .await?;
        }

        Ok(Connection {
            stream,
            correlation_id,
            session_expires: session.expires(Instant::now()),
            in_flight: false,
        })
    }

    async fn recycle(
        &self,
        obj: &mut Self::Type,
        metrics: &managed::Metrics,
    ) -> managed::RecycleResult<Self::Error> {
        debug!(obj.correlation_id, obj.in_flight, metrics.recycle_count);

        self.reusable(obj, metrics)
            .inspect_err(|reason| {
                debug!(broker = %self.broker, reason);
                CONNECTIONS_DISCARDED.add(1, &[KeyValue::new("reason", *reason)]);
            })
            .map_err(RecycleError::message)
    }
}

impl ConnectionManager {
    /// Returns why the pool must discard this connection, if it must.
    fn reusable(&self, obj: &Connection, metrics: &managed::Metrics) -> Result<(), &'static str> {
        if obj.in_flight {
            return Err("request in flight");
        }

        // A network device between us and the broker can drop an idle connection without
        // telling either end, and the next request on it then waits for its whole
        // deadline. `last_used` counts from when the pool last handed the connection out,
        // so a connection that served a long request goes a little early.
        if metrics.last_used() > self.max_idle {
            return Err("idle");
        }

        if obj
            .session_expires
            .is_some_and(|expires| Instant::now() >= expires)
        {
            return Err("session expiring");
        }

        // An idle connection has nothing to read. Reading one byte is safe, because the
        // pool discards the connection whenever the read returns anything. On a TLS
        // connection it reads from the TCP socket under the TLS session, where an idle
        // connection has nothing either, and a close_notify or an unread record is also
        // a reason to discard.
        match obj.stream.tcp().try_read(&mut [0u8; 1]) {
            Ok(0) => Err("closed by broker"),
            Ok(_) => Err("unread bytes"),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Err(err) => {
                debug!(broker = %self.broker, ?err);
                Err("read error")
            }
        }
    }
}

/// A managed [`Pool`] of broker [`Connection`]s
pub type Pool = managed::Pool<ConnectionManager>;

fn status_update(pool: &Pool) {
    let status = pool.status();
    POOL_AVAILABLE.record(status.available as u64, &[]);
    POOL_CURRENT_SIZE.record(status.size as u64, &[]);
    POOL_MAX_SIZE.record(status.max_size as u64, &[]);
    POOL_WAITING.record(status.waiting as u64, &[]);
}

/// Takes a [`Connection`] from the [`Pool`], and records how long that took or why it
/// failed.
async fn pool_get(pool: &Pool) -> Result<Object<ConnectionManager>, PoolError<Error>> {
    let start = SystemTime::now();

    pool.get()
        .await
        .inspect(|_| {
            POOL_GET_DURATION.record(
                start
                    .elapsed()
                    .map_or(0, |duration| duration.as_millis() as u64),
                &[],
            );
        })
        .inspect_err(|err| {
            let error = match err {
                PoolError::Timeout(TimeoutType::Wait) => "wait timeout",
                PoolError::Timeout(TimeoutType::Create) => "create timeout",
                PoolError::Timeout(TimeoutType::Recycle) => "recycle timeout",
                PoolError::Backend(_) => "backend",
                PoolError::Closed => "closed",
                PoolError::NoRuntimeSpecified => "no runtime",
                PoolError::PostCreateHook(_) => "post create hook",
            };

            POOL_GET_ERRORS.add(1, &[KeyValue::new("error", error)]);
        })
}

/// [Build][`Builder#method.build`] a [`Connection`] [`Pool`] to a [broker][`Builder#method.broker`]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Builder {
    broker: Url,
    client_id: Option<String>,
    security: Security,
    max_size: Option<usize>,
    wait_timeout: Duration,
    connect_timeout: Duration,
    max_idle: Duration,
    request_timeout: Duration,
    max_request_wait: Duration,
}

/// The default of Kafka's `request.timeout.ms`.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The default of Kafka's `max.poll.interval.ms`, which the Java consumer sends as its
/// JoinGroup rebalance timeout.
const DEFAULT_MAX_REQUEST_WAIT: Duration = Duration::from_secs(300);

/// The default of the Java client's `connections.max.idle.ms`, which is a minute below
/// the broker's, so that the client closes an idle connection before the broker does.
const DEFAULT_MAX_IDLE: Duration = Duration::from_secs(540);

const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

impl Builder {
    /// Broker URL
    pub fn broker(broker: Url) -> Self {
        Self {
            broker,
            client_id: None,
            security: Security::default(),
            max_size: None,
            wait_timeout: DEFAULT_WAIT_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            max_idle: DEFAULT_MAX_IDLE,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            max_request_wait: DEFAULT_MAX_REQUEST_WAIT,
        }
    }

    /// Client id used when making requests to the broker
    pub fn client_id(self, client_id: Option<String>) -> Self {
        Self { client_id, ..self }
    }

    /// How the connections are secured, by default not at all. With
    /// [`Tls`] each connection completes a TLS handshake before anything else, and with [`Sasl`]
    /// each connection then logs in before it carries a request, so a pool that logs in with SCRAM
    /// pays for a login whenever it opens a connection.
    pub fn security(self, security: Security) -> Self {
        Self { security, ..self }
    }

    /// Maximum number of connections in the pool, by default twice the number of CPUs
    /// ([`PoolConfig::default`][`managed::PoolConfig::default`])
    pub fn max_size(self, max_size: usize) -> Self {
        Self {
            max_size: Some(max_size),
            ..self
        }
    }

    /// Maximum time a request waits for a free slot in the pool, by default 30s.
    /// A request that waits longer fails with [`Error::Pool`].
    pub fn wait_timeout(self, wait_timeout: Duration) -> Self {
        Self {
            wait_timeout,
            ..self
        }
    }

    /// Maximum time to open a new connection to the broker, including retries, by default
    /// 30s. A request that takes longer fails with [`Error::Pool`].
    pub fn connect_timeout(self, connect_timeout: Duration) -> Self {
        Self {
            connect_timeout,
            ..self
        }
    }

    /// Maximum time a connection stays unused in the pool before the pool closes it, by
    /// default 9 minutes
    pub fn max_idle(self, max_idle: Duration) -> Self {
        Self { max_idle, ..self }
    }

    /// Time the broker has to answer a request, by default 30s. A request that asks the
    /// broker to wait, such as a fetch, gets that wait plus 5s instead when that is
    /// longer, with the wait capped at
    /// [`max_request_wait`][`Builder#method.max_request_wait`]. A request that takes
    /// longer fails with [`Error::Timeout`].
    pub fn request_timeout(self, request_timeout: Duration) -> Self {
        Self {
            request_timeout,
            ..self
        }
    }

    /// Maximum wait that a request can ask the broker for, by default 5 minutes. See
    /// [`request_timeout`][`Builder#method.request_timeout`].
    pub fn max_request_wait(self, max_request_wait: Duration) -> Self {
        Self {
            max_request_wait,
            ..self
        }
    }

    fn pool(&self, versions: BTreeMap<i16, i16>, security: Security) -> Result<Pool, Error> {
        let builder = Pool::builder(ConnectionManager {
            broker: self.broker.clone(),
            client_id: self.client_id.clone(),
            security,
            versions,
            connect_timeout: self.connect_timeout,
            max_idle: self.max_idle,
            request_timeout: self.request_timeout,
            max_request_wait: self.max_request_wait,
        })
        .runtime(Runtime::Tokio1)
        .wait_timeout(Some(self.wait_timeout))
        // The connect retries stop after the connect timeout, but they check it only
        // between attempts. This bounds the whole creation, including the address lookup.
        .create_timeout(Some(self.connect_timeout));

        match self.max_size {
            Some(max_size) => builder.max_size(max_size),
            None => builder,
        }
        .build()
        .map_err(Into::into)
    }

    /// Inquire with the broker supported api versions
    async fn bootstrap(&self) -> Result<BTreeMap<i16, i16>, Error> {
        // Create a temporary pool to establish the API requests
        // and versions supported by the broker
        let versions = BTreeMap::from([(ApiVersionsRequest::KEY, 0)]);

        let req = ApiVersionsRequest::default()
            .client_software_name(Some(env!("CARGO_PKG_NAME").into()))
            .client_software_version(Some(env!("CARGO_PKG_VERSION").into()));

        // A broker answers ApiVersions before a login, so this pool only needs TLS.
        let client = self
            .pool(versions, self.security.without_sasl())
            .map(Client::new)?;

        let supported = RootMessageMeta::messages().requests();

        client.call(req).await.map(|response| {
            response
                .api_keys
                .unwrap_or_default()
                .into_iter()
                .filter_map(|api| {
                    supported.get(&api.api_key).and_then(|supported| {
                        if api.min_version >= supported.version.valid.start {
                            Some((
                                api.api_key,
                                api.max_version.min(supported.version.valid.end),
                            ))
                        } else {
                            None
                        }
                    })
                })
                .collect()
        })
    }

    /// Establish the API versions supported by the broker returning a [`Pool`]
    pub async fn build(self) -> Result<Pool, Error> {
        self.bootstrap()
            .await
            .and_then(|versions| self.pool(versions, self.security.clone()))
    }
}

/// Inject the [`Pool`] into each request of this [`Layer`], as a [`FramePool`], using [`FramePoolService`]
///
/// Every caller of the layer shares the pool's connections. A request that changes the
/// state of its connection, as SaslHandshake and SaslAuthenticate do, therefore affects
/// every later request on that connection, whoever sends it.
#[derive(Clone, Debug)]
pub struct FramePoolLayer {
    pool: Pool,
}

impl FramePoolLayer {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

impl<S> Layer<S> for FramePoolLayer {
    type Service = FramePoolService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        FramePoolService {
            pool: self.pool.clone(),
            inner,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FramePool {
    pub frame: Frame,
    pub pool: Pool,
    pub extensions: Extensions,
}

impl ExtensionsRef for FramePool {
    fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

/// Inject the [`Pool`] into each request to the inner [`Service`], as a [`FramePool`]
#[derive(Clone, Debug)]
pub struct FramePoolService<S> {
    pool: Pool,
    inner: S,
}

impl<S> Service<FrameInput> for FramePoolService<S>
where
    S: Service<FramePool, Output = Frame>,
{
    type Output = Frame;
    type Error = S::Error;

    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        self.inner
            .serve(FramePool {
                frame: req.frame,
                pool: self.pool.clone(),
                extensions: req.extensions,
            })
            .await
    }
}

/// Inject the [`Pool`] into each request of this [`Layer`], as a [`RequestPool`], using [`RequestPoolService`]
#[derive(Clone, Debug)]
pub struct RequestPoolLayer {
    pool: Pool,
}

impl RequestPoolLayer {
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }
}

impl<S> Layer<S> for RequestPoolLayer {
    type Service = RequestPoolService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestPoolService {
            pool: self.pool.clone(),
            inner,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RequestPool<Q> {
    pub request: Q,
    pub pool: Pool,
    pub extensions: Extensions,
}

impl<Q> ExtensionsRef for RequestPool<Q> {
    fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

/// Inject the [`Pool`] into each request to the inner [`Service`], as a [`RequestPool`]
#[derive(Clone, Debug)]
pub struct RequestPoolService<S> {
    pool: Pool,
    inner: S,
}

impl<S, Q> Service<Q> for RequestPoolService<S>
where
    Q: Request,
    S: Service<RequestPool<Q>>,
{
    type Output = S::Output;
    type Error = S::Error;

    /// serve the request, injecting the pool into the request to the inner service
    async fn serve(&self, req: Q) -> Result<Self::Output, Self::Error> {
        self.inner
            .serve(RequestPool {
                request: req,
                pool: self.pool.clone(),
                extensions: Extensions::default(),
            })
            .await
    }
}

/// API client using a [`Connection`] [`Pool`]
#[derive(Clone, Debug)]
pub struct Client {
    service: RequestPoolService<RequestConnectionService<BytesConnectionService>>,
}

impl Client {
    /// Create a new client using the supplied pool
    pub fn new(pool: Pool) -> Self {
        let service = (RequestPoolLayer::new(pool), RequestConnectionLayer)
            .into_layer(BytesConnectionService);

        Self { service }
    }

    /// Make an API request using the connection from the pool
    pub async fn call<Q>(&self, req: Q) -> Result<Q::Response, Error>
    where
        Q: Request,
        Error: From<<<Q as Request>::Response as TryFrom<Body>>::Error>,
    {
        self.service.serve(req).await
    }
}

/// A [`Layer`] that takes a [`Connection`] from the [`Pool`] calling an inner [`Service`] with that [`Connection`] in a [`BytesConnection`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameConnectionLayer;

impl<S> Layer<S> for FrameConnectionLayer {
    type Service = FrameConnectionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

#[derive(Debug)]
pub struct FrameConnection {
    pub frame: Frame,
    pub connection: Object<ConnectionManager>,
    pub extensions: Extensions,
}

impl ExtensionsRef for FrameConnection {
    fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

/// A [`Service`] that takes a [`Connection`] from the [`Pool`] calling an inner [`Service`] with that [`Connection`] in a [`BytesConnection`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameConnectionService<S> {
    inner: S,
}

impl<S> Service<FramePool> for FrameConnectionService<S>
where
    S: Service<BytesConnection, Output = Bytes>,
    S::Error: From<Error> + From<PoolError<Error>> + From<nisshi_sans_io::Error>,
{
    type Output = Frame;
    type Error = S::Error;

    async fn serve(&self, req: FramePool) -> Result<Self::Output, Self::Error> {
        debug!(?req);

        let api_key = req.frame.api_key()?;
        let api_version = req.frame.api_version()?;
        let client_id = req
            .frame
            .client_id()
            .map(|client_id| client_id.map(|client_id| client_id.to_string()))?;

        status_update(&req.pool);

        let connection = pool_get(&req.pool).await?;

        let correlation_id = connection.correlation_id;
        let timeout = response_timeout(req.pool.manager(), &req.frame.body);

        self.inner
            .serve(BytesConnection {
                bytes: Frame::request(
                    Header::Request {
                        api_key,
                        api_version,
                        correlation_id,
                        client_id,
                    },
                    req.frame.body,
                )?,
                connection,
                timeout,
                extensions: req.extensions,
            })
            .await
            .and_then(|response| {
                Frame::response_from_bytes(response, api_key, api_version).map_err(Into::into)
            })
    }
}

/// A [`Layer`] of [`RequestConnectionService`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestConnectionLayer;

impl<S> Layer<S> for RequestConnectionLayer {
    type Service = RequestConnectionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// Take a [`Connection`] from the [`Pool`]. Enclose the [`Request`]
/// in a [`Frame`] using latest API version supported by the broker. Call the
/// inner service with the encoded [`Frame`] and the [`Connection`] in a [`BytesConnection`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestConnectionService<S> {
    inner: S,
}

impl<Q, S> Service<RequestPool<Q>> for RequestConnectionService<S>
where
    Q: Request,
    S: Service<BytesConnection, Output = Bytes>,
    S::Error: From<Error>
        + From<PoolError<Error>>
        + From<nisshi_sans_io::Error>
        + From<<Q::Response as TryFrom<Body>>::Error>,
{
    type Output = Q::Response;
    type Error = S::Error;

    async fn serve(&self, req: RequestPool<Q>) -> Result<Self::Output, Self::Error> {
        debug!(?req);
        status_update(&req.pool);

        let api_key = Q::KEY;
        let api_version = req.pool.manager().api_version(api_key)?;
        let client_id = req.pool.manager().client_id();
        let connection = pool_get(&req.pool).await?;

        let correlation_id = connection.correlation_id;
        let body = req.request.into();
        let timeout = response_timeout(req.pool.manager(), &body);

        let request = Frame::request(
            Header::Request {
                api_key,
                api_version,
                correlation_id,
                client_id,
            },
            body,
        )?;

        let response = self
            .inner
            .serve(BytesConnection {
                bytes: request,
                connection,
                timeout,
                extensions: req.extensions,
            })
            .await?;

        let frame = Frame::response_from_bytes(response, api_key, api_version)?;

        Q::Response::try_from(frame.body)
            .inspect(|response| debug!(?response))
            .map_err(Into::into)
    }
}

#[derive(Debug)]
pub struct BytesConnection {
    bytes: Bytes,
    connection: Object<ConnectionManager>,
    timeout: Duration,
    extensions: Extensions,
}

impl ExtensionsRef for BytesConnection {
    fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

/// The time the Java client allows for a JoinGroup beyond its rebalance timeout. We allow
/// it for every request that asks the broker to wait.
const RESPONSE_MARGIN: Duration = Duration::from_secs(5);

/// Returns how long the broker has to answer a request: the manager's request timeout. A
/// request that asks the broker to wait gets that wait plus [`RESPONSE_MARGIN`] instead,
/// when that is longer. The manager's maximum request wait caps the time the request asks
/// for.
///
/// A proxy forwards requests whose wait another client chose, so this deadline must not
/// end before that client's own deadline. For that reason we apply the Java client's
/// JoinGroup rule, `max(request.timeout.ms, rebalance timeout + 5s)`
/// ([`AbstractCoordinator`][join]), to every request.
///
/// We cap the wait, because every client of a proxy shares the connections in its pool,
/// and a request holds its connection until the broker answers. So we fail a JoinGroup
/// whose rebalance timeout is above the cap, where the Java client would wait (#882).
///
/// [join]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java#L620-L628
fn response_timeout(manager: &ConnectionManager, body: &Body) -> Duration {
    let wait_ms = match body {
        Body::FetchRequest(fetch) => fetch.max_wait_ms,

        // A v0 JoinGroup has no rebalance timeout, and the broker uses the session
        // timeout in its place.
        Body::JoinGroupRequest(join) => join
            .rebalance_timeout_ms
            .filter(|rebalance_timeout_ms| *rebalance_timeout_ms >= 0)
            .unwrap_or(join.session_timeout_ms),

        Body::ProduceRequest(produce) => produce.timeout_ms,

        Body::AlterPartitionReassignmentsRequest(alter) => alter.timeout_ms,
        Body::CreatePartitionsRequest(create) => create.timeout_ms,
        Body::CreateTopicsRequest(create) => create.timeout_ms,
        Body::DeleteRecordsRequest(delete) => delete.timeout_ms,
        Body::DeleteTopicsRequest(delete) => delete.timeout_ms,
        Body::ElectLeadersRequest(elect) => elect.timeout_ms,

        _ => 0,
    };

    let wait = Duration::from_millis(u64::try_from(wait_ms).unwrap_or_default());

    if wait.is_zero() {
        return manager.request_timeout;
    }

    cmp::max(
        manager.request_timeout,
        cmp::min(wait, manager.max_request_wait).saturating_add(RESPONSE_MARGIN),
    )
}

/// A [`Service`] that writes a frame represented by [`Bytes`] to the [`Connection`] in a [`BytesConnection`], returning the [`Bytes`] frame response.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesConnectionService;

impl BytesConnectionService {
    pub(crate) async fn write(
        &self,
        stream: &mut Stream,
        frame: Bytes,
        attributes: &[KeyValue],
    ) -> Result<(), Error> {
        debug!(len = frame.len());

        let start = SystemTime::now();

        stream
            .write_all(&frame[..])
            .await
            .inspect(|_| {
                TCP_SEND_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes,
                );

                TCP_BYTES_SENT.add(frame.len() as u64, attributes);
            })
            .inspect_err(|_| {
                TCP_SEND_ERRORS.add(1, attributes);
            })
            .map_err(Into::into)
    }

    pub(crate) async fn read(
        &self,
        stream: &mut Stream,
        attributes: &[KeyValue],
    ) -> Result<Bytes, Error> {
        let start = SystemTime::now();

        let mut size = [0u8; 4];
        _ = stream.read_exact(&mut size).await?;

        let mut buffer: Vec<u8> = vec![0u8; frame_length(size)?];
        buffer[0..size.len()].copy_from_slice(&size[..]);
        _ = stream
            .read_exact(&mut buffer[4..])
            .await
            .inspect(|_| {
                TCP_RECEIVE_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes,
                );

                TCP_BYTES_RECEIVED.add(buffer.len() as u64, attributes);
            })
            .inspect_err(|_| {
                TCP_RECEIVE_ERRORS.add(1, attributes);
            })?;

        Ok(Bytes::from(buffer)).inspect(|frame| debug!(len = frame.len()))
    }
}

impl Service<BytesConnection> for BytesConnectionService {
    type Output = Bytes;
    type Error = Error;

    async fn serve(&self, req: BytesConnection) -> Result<Self::Output, Self::Error> {
        let BytesConnection {
            bytes,
            mut connection,
            timeout: deadline,
            ..
        } = req;

        let local = connection.stream.tcp().local_addr()?;
        let peer = connection.stream.tcp().peer_addr()?;

        let attributes = [KeyValue::new("peer", peer.to_string())];

        let span = span!(Level::DEBUG, "client", local = %local, peer = %peer);

        async move {
            connection.in_flight = true;

            // The deadline covers the write too, because a write blocks when the broker
            // stops reading.
            let response = timeout(deadline, async {
                self.write(&mut connection.stream, bytes, &attributes)
                    .await?;

                connection.correlation_id += 1;

                self.read(&mut connection.stream, &attributes).await
            })
            .await
            .map_err(|_elapsed| {
                REQUEST_TIMEOUTS.add(1, &attributes);
                Error::Timeout(deadline)
            })
            .inspect_err(|err| debug!(?err))??;

            connection.in_flight = false;
            Ok(response)
        }
        .instrument(span)
        .await
    }
}

static TCP_CONNECT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tcp_connect_duration")
        .with_unit("ms")
        .with_description("The TCP connect latencies in milliseconds")
        .build()
});

static TCP_CONNECT_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_connect_errors")
        .with_description("TCP connect errors")
        .build()
});

static TCP_SEND_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tcp_send_duration")
        .with_unit("ms")
        .with_description("The TCP send latencies in milliseconds")
        .build()
});

static TCP_SEND_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_send_errors")
        .with_description("TCP send errors")
        .build()
});

static TCP_RECEIVE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("tcp_receive_duration")
        .with_unit("ms")
        .with_description("The TCP receive latencies in milliseconds")
        .build()
});

static TCP_RECEIVE_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_receive_errors")
        .with_description("TCP receive errors")
        .build()
});

static TCP_BYTES_SENT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_bytes_sent")
        .with_description("TCP bytes sent")
        .build()
});

static TCP_BYTES_RECEIVED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("tcp_bytes_received")
        .with_description("TCP bytes received")
        .build()
});

static REQUEST_TIMEOUTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("request_timeouts")
        .with_description("Requests that the broker did not answer before their deadline")
        .build()
});

static CONNECTIONS_DISCARDED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("pool_connections_discarded")
        .with_description("Pooled connections closed instead of reused, by reason")
        .build()
});

static POOL_GET_ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("pool_get_errors")
        .with_description("Failures to take a connection from the pool, by error")
        .build()
});

static POOL_GET_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("pool_get_duration")
        .with_unit("ms")
        .with_description("The Pool Get latencies in milliseconds")
        .build()
});

static POOL_MAX_SIZE: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("pool_max_size")
        .with_description("The maximum size of the pool")
        .build()
});

static POOL_CURRENT_SIZE: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("pool_current_size")
        .with_description("The current size of the pool")
        .build()
});

static POOL_AVAILABLE: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("pool_available")
        .with_description("The number of available objects in the pool")
        .build()
});

static POOL_WAITING: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("pool_waiting")
        .with_description("The number of waiting objects in the pool")
        .build()
});

#[cfg(test)]
mod connect_failure_tests {
    use super::{Error, PoolError, TimeoutType};
    use std::{io, sync::Arc};

    fn pool_error(error: PoolError<Error>) -> Error {
        Error::from(error)
    }

    #[test]
    fn create_timeout_and_backend_errors_are_connect_failures() {
        assert!(pool_error(PoolError::Timeout(TimeoutType::Create)).is_connect_failure());
        assert!(
            pool_error(PoolError::Backend(Error::Io(Arc::new(io::Error::from(
                io::ErrorKind::ConnectionRefused
            )))))
            .is_connect_failure()
        );
    }

    #[test]
    fn wait_timeout_is_not_a_connect_failure() {
        assert!(!pool_error(PoolError::Timeout(TimeoutType::Wait)).is_connect_failure());
    }

    #[test]
    fn errors_after_connecting_are_not_connect_failures() {
        assert!(!Error::Timeout(std::time::Duration::from_secs(30)).is_connect_failure());
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, thread};

    use nisshi_sans_io::{
        AlterPartitionReassignmentsRequest, CreatePartitionsRequest, CreateTopicsRequest,
        DeleteRecordsRequest, DeleteTopicsRequest, ElectLeadersRequest, FetchRequest,
        JoinGroupRequest, MetadataRequest, MetadataResponse, ProduceRequest, RequestInput,
    };
    use nisshi_service::{
        BytesFrameLayer, FrameRouteService, RequestLayer, ResponseService, TcpBytesLayer,
        TcpContextLayer, TcpListenerInput, TcpListenerLayer,
    };
    use tokio::{net::TcpListener, task::JoinSet};
    use tokio_util::sync::CancellationToken;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use super::*;

    fn init_tracing() -> Result<DefaultGuard, Error> {
        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(
                    EnvFilter::from_default_env()
                        .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
                )
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    async fn server(cancellation: CancellationToken, listener: TcpListener) -> Result<(), Error> {
        let server = (
            TcpListenerLayer::new(cancellation),
            TcpContextLayer::default(),
            TcpBytesLayer,
            BytesFrameLayer::default(),
        )
            .into_layer(
                FrameRouteService::builder()
                    .with_service(RequestLayer::<MetadataRequest>::new().into_layer(
                        ResponseService::new(|_req: RequestInput<MetadataRequest>| {
                            Ok::<_, Error>(
                                MetadataResponse::default()
                                    .brokers(Some([].into()))
                                    .topics(Some([].into()))
                                    .cluster_id(Some("abc".into()))
                                    .controller_id(Some(111))
                                    .throttle_time_ms(Some(0))
                                    .cluster_authorized_operations(Some(-1)),
                            )
                        }),
                    ))
                    .and_then(|builder| builder.build())?,
            );

        server
            .serve(TcpListenerInput {
                listener,
                extensions: Extensions::default(),
            })
            .await
    }

    #[tokio::test]
    async fn tcp_client_server() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let cancellation = CancellationToken::new();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;

        let mut join = JoinSet::new();

        let _server = {
            let cancellation = cancellation.clone();
            join.spawn(async move { server(cancellation, listener).await })
        };

        let origin = (
            RequestPoolLayer::new(
                ConnectionManager::builder(
                    Url::parse(&format!("tcp://{local_addr}")).inspect(|url| debug!(%url))?,
                )
                .client_id(Some(env!("CARGO_PKG_NAME").into()))
                .build()
                .await
                .inspect(|pool| debug!(?pool))?,
            ),
            RequestConnectionLayer,
        )
            .into_layer(BytesConnectionService);

        let response = origin
            .serve(
                MetadataRequest::default()
                    .topics(Some([].into()))
                    .allow_auto_topic_creation(Some(false))
                    .include_cluster_authorized_operations(Some(false))
                    .include_topic_authorized_operations(Some(false)),
            )
            .await?;

        assert_eq!(Some("abc"), response.cluster_id.as_deref());
        assert_eq!(Some(111), response.controller_id);

        cancellation.cancel();

        let joined = join.join_all().await;
        debug!(?joined);

        Ok(())
    }

    fn manager(
        request_timeout: Duration,
        max_request_wait: Duration,
    ) -> Result<ConnectionManager, Error> {
        Ok(ConnectionManager {
            broker: Url::parse("tcp://localhost:9092")?,
            client_id: None,
            security: Security::default(),
            versions: BTreeMap::new(),
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            max_idle: DEFAULT_MAX_IDLE,
            request_timeout,
            max_request_wait,
        })
    }

    const BASE: Duration = Duration::from_secs(30);
    const CAP: Duration = Duration::from_secs(300);

    #[test]
    fn response_timeout_short_wait_is_base() -> Result<(), Error> {
        let body = FetchRequest::default().max_wait_ms(500).into();
        assert_eq!(BASE, response_timeout(&manager(BASE, CAP)?, &body));
        Ok(())
    }

    #[test]
    fn response_timeout_fetch_waits_max_wait() -> Result<(), Error> {
        let body = FetchRequest::default().max_wait_ms(60_000).into();
        assert_eq!(
            Duration::from_secs(65),
            response_timeout(&manager(BASE, CAP)?, &body)
        );
        Ok(())
    }

    #[test]
    fn response_timeout_join_group_waits_rebalance_timeout() -> Result<(), Error> {
        let body = JoinGroupRequest::default()
            .session_timeout_ms(45_000)
            .rebalance_timeout_ms(Some(60_000))
            .into();
        assert_eq!(
            Duration::from_secs(65),
            response_timeout(&manager(BASE, CAP)?, &body)
        );
        Ok(())
    }

    #[test]
    fn response_timeout_join_group_v0_waits_session_timeout() -> Result<(), Error> {
        let manager = manager(BASE, CAP)?;

        for rebalance_timeout_ms in [None, Some(-1)] {
            let body = JoinGroupRequest::default()
                .session_timeout_ms(45_000)
                .rebalance_timeout_ms(rebalance_timeout_ms)
                .into();
            assert_eq!(Duration::from_secs(50), response_timeout(&manager, &body));
        }
        Ok(())
    }

    #[test]
    fn response_timeout_produce_waits_timeout() -> Result<(), Error> {
        let body = ProduceRequest::default().timeout_ms(30_000).into();
        assert_eq!(
            Duration::from_secs(35),
            response_timeout(&manager(BASE, CAP)?, &body)
        );
        Ok(())
    }

    #[test]
    fn response_timeout_admin_waits_timeout() -> Result<(), Error> {
        let manager = manager(BASE, CAP)?;

        for body in [
            AlterPartitionReassignmentsRequest::default()
                .timeout_ms(60_000)
                .into(),
            CreatePartitionsRequest::default().timeout_ms(60_000).into(),
            CreateTopicsRequest::default().timeout_ms(60_000).into(),
            DeleteRecordsRequest::default().timeout_ms(60_000).into(),
            DeleteTopicsRequest::default().timeout_ms(60_000).into(),
            ElectLeadersRequest::default().timeout_ms(60_000).into(),
        ] {
            assert_eq!(Duration::from_secs(65), response_timeout(&manager, &body));
        }
        Ok(())
    }

    #[test]
    fn response_timeout_ignores_negative_wait() -> Result<(), Error> {
        let manager = manager(BASE, CAP)?;

        for body in [
            FetchRequest::default().max_wait_ms(-60_000).into(),
            ProduceRequest::default().timeout_ms(-60_000).into(),
            JoinGroupRequest::default()
                .session_timeout_ms(-60_000)
                .rebalance_timeout_ms(None)
                .into(),
        ] {
            assert_eq!(BASE, response_timeout(&manager, &body));
        }
        Ok(())
    }

    #[test]
    fn response_timeout_caps_wait() -> Result<(), Error> {
        let body = FetchRequest::default().max_wait_ms(i32::MAX).into();
        assert_eq!(
            Duration::from_secs(65),
            response_timeout(&manager(BASE, Duration::from_secs(60))?, &body)
        );
        Ok(())
    }

    #[test]
    fn response_timeout_saturates() -> Result<(), Error> {
        let body = FetchRequest::default().max_wait_ms(i32::MAX).into();
        assert_eq!(
            Duration::MAX,
            response_timeout(&manager(Duration::MAX, Duration::MAX)?, &body)
        );
        Ok(())
    }

    #[test]
    fn response_timeout_without_wait_is_base() -> Result<(), Error> {
        let base = Duration::from_millis(100);
        let manager = manager(base, CAP)?;

        for body in [
            MetadataRequest::default().into(),
            FetchRequest::default().max_wait_ms(0).into(),
        ] {
            assert_eq!(base, response_timeout(&manager, &body));
        }
        Ok(())
    }
}
