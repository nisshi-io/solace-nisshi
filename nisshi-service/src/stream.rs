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

use std::{
    error::{self},
    fmt::Debug,
    future::Future,
    io,
    net::SocketAddr,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use nanoid::nanoid;
use nisshi_auth::AuthenticationExtension;
use nisshi_sans_io::{BytesInput, CloseAfterResponseExtension, SuppressResponseExtension};
use opentelemetry::KeyValue;
use rama::{
    Layer, Service,
    extensions::{Extension, Extensions, ExtensionsRef},
    tcp::{TcpStream, TokioTcpStream},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Mutex,
    task::JoinSet,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument as _, Level, debug, error, instrument, span};

use crate::{
    BYTES_RECEIVED, BYTES_SENT, Error, FRAMES_REJECTED, REQUEST_DURATION, REQUEST_SIZE,
    RESPONSE_SIZE, TcpListenerInput, frame_length, frame_size,
};

/// The largest request payload a listener accepts unless [`TcpContext::maximum_frame_size`]
/// says otherwise, matching the Apache Kafka default for `socket.request.max.bytes`.
///
/// The size prefix is read before authentication, so an unbounded listener lets
/// a client make it allocate up to 2 GiB per connection by sending 4 bytes.
pub const DEFAULT_MAXIMUM_FRAME_SIZE: usize = 100 * 1024 * 1024;

/// The largest request payload an unauthenticated connection accepts unless
/// [`TcpContext::pre_authentication_maximum_frame_size`] says otherwise,
/// matching the Apache Kafka default for `sasl.server.max.receive.size`.
///
/// `None` (the [`TcpContext`] default) leaves [`DEFAULT_MAXIMUM_FRAME_SIZE`] in
/// effect even before authentication, so a non-SASL listener's behavior is
/// unchanged. Where this is `Some`, the effective limit before authentication
/// is the smaller of this and [`TcpContext::maximum_frame_size`] — never the
/// larger — so configuring the full limit below this can only tighten, not
/// loosen, what an unauthenticated client may send.
pub const DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE: usize = 512 * 1024;

/// How long an otherwise-idle connection may wait for the next request to begin
/// before it's closed, unless [`TcpContext::connection_idle_timeout`] says
/// otherwise, matching the Apache Kafka default for `connections.max.idle.ms`.
///
/// Long by design: a legitimate consumer or producer connection can sit idle
/// between requests for a while.
pub const DEFAULT_CONNECTION_IDLE_TIMEOUT: Duration = Duration::from_secs(600);

/// How long a request or response transfer already in progress may stall
/// between successive chunks before it's abandoned, unless
/// [`TcpContext::io_idle_timeout`] says otherwise.
///
/// Deliberately much shorter than [`DEFAULT_CONNECTION_IDLE_TIMEOUT`]: once a
/// peer has committed to sending or receiving, it shouldn't stall for minutes.
/// Without this, a peer that declares a large frame and then goes fully
/// quiet mid-transfer would hold the connection open indefinitely even
/// though the connection idle timeout never fires (that one only guards the
/// gap *before* a request starts).
///
/// This bounds a stall, not a slow trickle: a peer sending one byte just
/// under this deadline, repeatedly, still resets it every time and can hold
/// a single connection open indefinitely. Closing that fully needs a
/// per-frame minimum-throughput floor or a connection cap; out of scope
/// here, tracked as a follow-up.
pub const DEFAULT_IO_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Bytes read from (or written to) the peer in one chunk while a transfer is
/// in progress, keeping [`TcpBytesService::read`]'s buffer growth bounded and
/// the idle-timeout deadline reset at a reasonable cadence.
const IO_CHUNK_SIZE: usize = 64 * 1024;

/// Reads one chunk into `buf`, bounding the wait by `idle_timeout` (if any).
///
/// Unlike wrapping a whole `read_exact` in one deadline, resetting this timeout
/// on every chunk means a slow-but-progressing transfer is never killed, only
/// a genuinely stalled one.
async fn read_chunk<R>(
    req: &mut R,
    buf: &mut [u8],
    idle_timeout: Option<Duration>,
) -> io::Result<usize>
where
    R: AsyncReadExt + Unpin,
{
    match idle_timeout {
        Some(idle_timeout) => timeout(idle_timeout, req.read(buf))
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?,
        None => req.read(buf).await,
    }
}

/// Writes one chunk from `buf`, bounding the wait by `idle_timeout` (if any).
async fn write_chunk<W>(
    req: &mut W,
    buf: &[u8],
    idle_timeout: Option<Duration>,
) -> io::Result<usize>
where
    W: AsyncWriteExt + Unpin,
{
    match idle_timeout {
        Some(idle_timeout) => timeout(idle_timeout, req.write(buf))
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?,
        None => req.write(buf).await,
    }
}

/// Flushes anything the stream may still be buffering, bounding the wait by
/// `idle_timeout` (if any).
///
/// [`AsyncWriteExt::write`] is allowed to buffer, so a completed write loop
/// alone doesn't guarantee the peer has been sent the bytes; only a flush
/// does. A bare TCP stream flushes as a no-op, but a TLS or buffered wrapper
/// would otherwise leave the tail of a response unsent until the next I/O.
async fn flush_with_idle_timeout<W>(req: &mut W, idle_timeout: Option<Duration>) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    match idle_timeout {
        Some(idle_timeout) => timeout(idle_timeout, req.flush())
            .await
            .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?,
        None => req.flush().await,
    }
}

/// Reads exactly `buf.len()` bytes, chunk by chunk, resetting `idle_timeout`
/// after every chunk of forward progress.
async fn read_exact_with_idle_timeout<R>(
    req: &mut R,
    buf: &mut [u8],
    idle_timeout: Option<Duration>,
) -> io::Result<()>
where
    R: AsyncReadExt + Unpin,
{
    let mut read = 0;

    while read < buf.len() {
        let n = read_chunk(req, &mut buf[read..], idle_timeout).await?;

        if n == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }

        read += n;
    }

    Ok(())
}

/// A [`Layer`] that listens for TCP connections
#[derive(Clone, Debug, Default)]
pub struct TcpListenerLayer {
    cancellation: CancellationToken,
}

impl TcpListenerLayer {
    pub fn new(cancellation: CancellationToken) -> Self {
        Self { cancellation }
    }
}

impl<S> Layer<S> for TcpListenerLayer {
    type Service = TcpListenerService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            cancellation: self.cancellation.clone(),
            inner,
        }
    }
}

/// A [`Service`] that listens for TCP connections
///
/// Each accepted connection gets its own fork of the listener's [`Extensions`]. The
/// connection reads the listener's entries through the fork, and its own inserts land
/// only on the fork, so per-connection state never reaches the listener's store or
/// another connection.
///
/// The fork isolates only the [`Extensions`]. Every connection gets a clone of the inner
/// service, so all connections share any state that the service holds itself. A
/// [`BytesFrameLayer`](crate::BytesFrameLayer) with a SASL config is one example: every
/// clone shares its authentication stage. A stack that holds per-connection state needs
/// a new service for each connection.
#[derive(Clone, Default)]
pub struct TcpListenerService<S> {
    cancellation: CancellationToken,
    inner: S,
}

impl<S> Debug for TcpListenerService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(TcpListenerService)).finish()
    }
}

/// A source of accepted TCP connections.
///
/// Exists so the accept loop in [`TcpListenerService`] is testable against a scripted
/// sequence of accept outcomes (in particular, an `Err` followed by an `Ok`) without
/// depending on triggering a real OS-level `accept()` failure, which is fragile and
/// platform-dependent. Production code always passes the [`TcpListener`] carried by
/// [`TcpListenerInput`]; only tests supply another implementation.
trait Acceptor {
    fn accept(&self) -> impl Future<Output = io::Result<(TokioTcpStream, SocketAddr)>> + Send;
}

impl Acceptor for TcpListener {
    fn accept(&self) -> impl Future<Output = io::Result<(TokioTcpStream, SocketAddr)>> + Send {
        TcpListener::accept(self)
    }
}

/// Backs off after a run of consecutive `accept()` errors that look like resource
/// exhaustion (e.g. `EMFILE`/`ENFILE`), so a persistent failure doesn't spin the loop
/// at 100% CPU. `ConnectionAborted` is a routine, expected per-connection error (a peer
/// reset before we could accept it) and never backs off.
///
/// The backoff sleep runs inside the `select!` arm, blocking the whole `select!` call
/// for its duration -- delaying cancellation and any other periodic branch a caller
/// composes alongside this one -- so it is kept short and capped.
struct AcceptBackoff {
    consecutive_errors: u32,
}

impl AcceptBackoff {
    const CAP: Duration = Duration::from_millis(200);
    const INITIAL: Duration = Duration::from_millis(5);

    const fn new() -> Self {
        Self {
            consecutive_errors: 0,
        }
    }

    fn reset(&mut self) {
        self.consecutive_errors = 0;
    }

    /// Returns the backoff to sleep for, or `None` if this error shouldn't back off.
    fn on_error(&mut self, err: &io::Error) -> Option<Duration> {
        if err.kind() == io::ErrorKind::ConnectionAborted {
            self.reset();
            return None;
        }

        let backoff = Self::INITIAL
            .saturating_mul(1 << self.consecutive_errors.min(6))
            .min(Self::CAP);

        self.consecutive_errors = self.consecutive_errors.saturating_add(1);

        Some(backoff)
    }
}

impl<S> Service<TcpListenerInput> for TcpListenerService<S>
where
    S: Service<TcpStream> + Clone,
    S::Output: Debug,
    S::Error: error::Error,
{
    type Output = ();
    type Error = S::Error;

    #[instrument(skip(req))]
    async fn serve(&self, req: TcpListenerInput) -> Result<Self::Output, Self::Error> {
        self.accept_loop(req.listener, req.extensions).await
    }
}

impl<S> TcpListenerService<S>
where
    S: Service<TcpStream> + Clone,
    S::Output: Debug,
    S::Error: error::Error,
{
    /// Accepts connections from `listener` until cancelled, handing each one to the
    /// inner service with a child forked from `extensions`.
    ///
    /// Generic over [`Acceptor`] (rather than taking [`TcpListenerInput`] directly) so a
    /// test can drive the loop with a scripted accept sequence; see the trait docs.
    async fn accept_loop<A>(&self, listener: A, extensions: Extensions) -> Result<(), S::Error>
    where
        A: Acceptor + Debug + Send + Sync + 'static,
    {
        let mut set = JoinSet::new();
        let mut backoff = AcceptBackoff::new();

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            backoff.reset();
                            debug!(?listener, ?stream, %addr);

                            let service = self.inner.clone();
                            let extensions = extensions.fork();

                            let handle = set.spawn(
                                async move {
                                    match service.serve(TcpStream::from_tokio_tcp_stream(stream, extensions)).await {
                                        Err(error) => {
                                            debug!(%addr, %error);
                                        }

                                        Ok(response) => {
                                            debug!(%addr, ?response)
                                        }
                                    }
                                }
                                .instrument(span!(Level::INFO, "peer", %addr)),
                            );

                            debug!(?handle);
                        }

                        Err(err) => {
                            error!(?err, "accept() failed; continuing to listen");

                            if let Some(backoff) = backoff.on_error(&err) {
                                tokio::time::sleep(backoff).await;
                            }
                        }
                    }

                    continue;
                }

                v = set.join_next(), if !set.is_empty() => {
                    debug!(?v);
                }

                cancelled = self.cancellation.cancelled() => {
                    debug!(?cancelled);
                    break;
                }
            }
        }

        Ok(())
    }
}

/// The connection settings that [`TcpContextLayer`] and [`TcpContextService`] insert into each stream's [`Extensions`]
#[non_exhaustive]
#[derive(Clone, Debug, Extension)]
pub struct TcpContext {
    cluster_id: Option<String>,
    maximum_frame_size: Option<usize>,
    pre_authentication_maximum_frame_size: Option<usize>,
    connection_idle_timeout: Option<Duration>,
    io_idle_timeout: Option<Duration>,
}

impl Default for TcpContext {
    fn default() -> Self {
        Self {
            cluster_id: Default::default(),
            maximum_frame_size: Some(DEFAULT_MAXIMUM_FRAME_SIZE),
            // Not set by default: a listener with no SASL configured must not
            // have its very first frame capped at 512KiB. Callers that do
            // configure SASL are expected to also call
            // `pre_authentication_maximum_frame_size`.
            pre_authentication_maximum_frame_size: None,
            connection_idle_timeout: Some(DEFAULT_CONNECTION_IDLE_TIMEOUT),
            io_idle_timeout: Some(DEFAULT_IO_IDLE_TIMEOUT),
        }
    }
}

#[derive(Clone, Debug, Extension)]
struct ClusterIdExtension(String);

#[derive(Clone, Debug, Extension)]
struct MaximumFrameSizeExtension(usize);

impl Default for MaximumFrameSizeExtension {
    fn default() -> Self {
        Self(DEFAULT_MAXIMUM_FRAME_SIZE)
    }
}

impl From<&MaximumFrameSizeExtension> for usize {
    fn from(value: &MaximumFrameSizeExtension) -> Self {
        value.0
    }
}

/// Per-connection [`TcpContext::pre_authentication_maximum_frame_size`],
/// carried on the stream's [`Extensions`] like [`MaximumFrameSizeExtension`].
/// Absent means no pre-authentication limit is configured, so
/// [`MaximumFrameSizeExtension`] alone governs every frame, authenticated or
/// not.
#[derive(Clone, Debug, Extension)]
struct PreAuthenticationMaximumFrameSizeExtension(usize);

/// Per-connection [`TcpContext::connection_idle_timeout`], carried on the
/// stream's [`Extensions`] like [`MaximumFrameSizeExtension`]. Absent means
/// the timeout is disabled.
#[derive(Clone, Debug, Extension)]
struct ConnectionIdleTimeoutExtension(Duration);

/// Per-connection [`TcpContext::io_idle_timeout`], carried on the stream's
/// [`Extensions`] like [`MaximumFrameSizeExtension`]. Absent means the
/// timeout is disabled.
#[derive(Clone, Debug, Extension)]
struct IoIdleTimeoutExtension(Duration);

impl TcpContext {
    pub fn cluster_id(self, cluster_id: Option<String>) -> Self {
        Self { cluster_id, ..self }
    }

    /// Largest request payload (excluding the 4 byte size prefix) this listener
    /// reads, or `None` for no limit. Defaults to [`DEFAULT_MAXIMUM_FRAME_SIZE`].
    pub fn maximum_frame_size(self, maximum_frame_size: Option<usize>) -> Self {
        Self {
            maximum_frame_size,
            ..self
        }
    }

    /// Largest request payload an unauthenticated connection may send, or
    /// `None` to leave [`maximum_frame_size`][Self::maximum_frame_size] in
    /// effect before authentication too (the default). Where both are `Some`,
    /// the effective pre-authentication limit is the smaller of the two.
    pub fn pre_authentication_maximum_frame_size(
        self,
        pre_authentication_maximum_frame_size: Option<usize>,
    ) -> Self {
        Self {
            pre_authentication_maximum_frame_size,
            ..self
        }
    }

    /// How long an otherwise-idle connection may wait for the next request to
    /// begin, or `None` for no limit. Defaults to
    /// [`DEFAULT_CONNECTION_IDLE_TIMEOUT`].
    pub fn connection_idle_timeout(self, connection_idle_timeout: Option<Duration>) -> Self {
        Self {
            connection_idle_timeout,
            ..self
        }
    }

    /// How long a request or response transfer already in progress may stall
    /// between chunks before it's abandoned, or `None` for no limit. Defaults
    /// to [`DEFAULT_IO_IDLE_TIMEOUT`].
    pub fn io_idle_timeout(self, io_idle_timeout: Option<Duration>) -> Self {
        Self {
            io_idle_timeout,
            ..self
        }
    }
}

/// A [`Layer`] that inserts the [`TcpContext`] settings into each stream's [`Extensions`]
#[derive(Clone, Debug, Default)]
pub struct TcpContextLayer {
    state: TcpContext,
}

impl TcpContextLayer {
    pub fn new(state: TcpContext) -> Self {
        Self { state }
    }
}

impl<S> Layer<S> for TcpContextLayer {
    type Service = TcpContextService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            state: self.state.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpStreamLayer;

impl<S> Layer<S> for TcpStreamLayer {
    type Service = TcpStreamService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpStreamService<S> {
    inner: S,
}

impl<S> Service<TcpStream> for TcpStreamService<S>
where
    S: Service<BytesInput, Output = Bytes>,
    S::Error: Into<Error>,
{
    type Output = TcpStream;
    type Error = Error;

    async fn serve(&self, stream: TcpStream) -> Result<Self::Output, Self::Error> {
        let (frame, stream) = ReadHalfService.serve(stream).await?;

        let extensions = stream.extensions.fork();

        let frame = self
            .inner
            .serve(BytesInput {
                bytes: frame,
                extensions,
            })
            .await
            .map_err(Into::into)?;

        WriteHalfService.serve((frame, stream)).await
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct WriteHalfService;

impl Service<(Bytes, TcpStream)> for WriteHalfService {
    type Output = TcpStream;
    type Error = Error;

    async fn serve(
        &self,
        (frame, mut stream): (Bytes, TcpStream),
    ) -> Result<Self::Output, Self::Error> {
        stream.write_all(&frame[..]).await?;

        BYTES_SENT.add(frame.len() as u64, &[]);

        Ok(stream)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ReadHalfService;

impl Service<TcpStream> for ReadHalfService {
    type Output = (Bytes, TcpStream);
    type Error = Error;

    async fn serve(&self, mut input: TcpStream) -> Result<Self::Output, Self::Error> {
        let mut size = [0u8; 4];
        _ = input.read_exact(&mut size).await?;

        let frame_size = frame_size(size)?;

        if frame_size
            > input
                .extensions()
                .get_ref_or_insert(MaximumFrameSizeExtension::default)
                .into()
        {
            return Err(Into::into(Error::FrameTooBig(frame_size)));
        }

        let mut buffer: Vec<u8> = vec![0u8; frame_length(size)?];
        buffer[0..size.len()].copy_from_slice(&size[..]);
        _ = input.read_exact(&mut buffer[4..]).await?;
        BYTES_RECEIVED.add(buffer.len() as u64, &[]);

        Ok((Bytes::from(buffer), input))
    }
}

/// A [`Service`] that inserts the [`TcpContext`] settings into each stream's [`Extensions`]
///
/// The connection may be any stream type, for example a [`TcpStream`] or a TLS
/// stream wrapping one: this service only inserts the extensions and passes
/// the stream through to the inner service.
#[derive(Clone)]
pub struct TcpContextService<S> {
    inner: S,
    state: TcpContext,
}

impl<S> Debug for TcpContextService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(TcpContextService)).finish()
    }
}

impl<S, Stream> Service<Stream> for TcpContextService<S>
where
    S: Service<Stream>,
    S::Error: From<io::Error>,
    Stream: ExtensionsRef + Send + 'static,
{
    type Output = S::Output;
    type Error = S::Error;

    #[instrument(skip_all)]
    async fn serve(&self, req: Stream) -> Result<Self::Output, Self::Error> {
        if let Some(cluster_id) = self.state.cluster_id.clone() {
            _ = req.extensions().insert(ClusterIdExtension(cluster_id));
        }

        if let Some(maximum_frame_size) = self.state.maximum_frame_size {
            _ = req
                .extensions()
                .insert(MaximumFrameSizeExtension(maximum_frame_size));
        }

        if let Some(pre_authentication_maximum_frame_size) =
            self.state.pre_authentication_maximum_frame_size
        {
            _ = req
                .extensions()
                .insert(PreAuthenticationMaximumFrameSizeExtension(
                    pre_authentication_maximum_frame_size,
                ));
        }

        if let Some(connection_idle_timeout) = self.state.connection_idle_timeout {
            _ = req
                .extensions()
                .insert(ConnectionIdleTimeoutExtension(connection_idle_timeout));
        }

        if let Some(io_idle_timeout) = self.state.io_idle_timeout {
            _ = req
                .extensions()
                .insert(IoIdleTimeoutExtension(io_idle_timeout));
        }

        self.inner.serve(req).await
    }
}

/// A [`Service`] writing [`Bytes`] into a [`TcpStream`], responding with a length delimited frame of [`Bytes`]
pub struct BytesTcpService {
    stream: Mutex<TcpStream>,
}

impl BytesTcpService {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: Mutex::new(stream),
        }
    }
}

impl Debug for BytesTcpService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(BytesTcpService)).finish()
    }
}

impl Service<BytesInput> for BytesTcpService {
    type Output = Bytes;
    type Error = Error;

    #[instrument(skip_all)]
    async fn serve(&self, req: BytesInput) -> Result<Self::Output, Self::Error> {
        let mut stream = self.stream.lock().await;

        stream.write_all(&req.bytes[..]).await?;
        BYTES_SENT.add(req.bytes.len() as u64, &[]);

        let mut size = [0u8; 4];
        _ = stream.read_exact(&mut size).await?;

        let mut buffer: Vec<u8> = vec![0u8; frame_length(size)?];
        buffer[0..size.len()].copy_from_slice(&size[..]);
        _ = stream.read_exact(&mut buffer[4..]).await?;
        BYTES_RECEIVED.add(buffer.len() as u64, &[]);

        Ok(Bytes::from(buffer))
    }
}

/// A [`Layer`] receiving [`Bytes`] from a [`TcpStream`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpBytesLayer;

impl<S> Layer<S> for TcpBytesLayer {
    type Service = TcpBytesService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A [`Service`] receiving [`Bytes`] from a [`TcpStream`], calling an inner [`Service`] and sending [`Bytes`] into the [`TcpStream`]
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TcpBytesService<S> {
    inner: S,
}

impl<S> Debug for TcpBytesService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(TcpBytesService)).finish()
    }
}

impl<S> TcpBytesService<S> {
    fn elapsed_millis(&self, start: SystemTime) -> u64 {
        start
            .elapsed()
            .map_or(0, |duration| duration.as_millis() as u64)
    }
}

/// The [`TcpContext`] limits relevant to a single connection's request/response
/// loop, read from the stream's [`Extensions`] once per
/// [`TcpBytesService::serve`] call instead of growing the positional argument
/// list on [`TcpBytesService::wait`]/`read`/`write`.
///
/// `None` in any field means that limit is disabled, either because the
/// [`TcpContext`] said so or because no [`TcpContextLayer`] set one.
///
/// `maximum_frame_size` here is the full, post-authentication limit snapshotted
/// once at the start of the connection. [`TcpBytesService::req`] re-derives the
/// *effective* limit fresh on every request via [`effective_maximum_frame_size`]
/// — which may be smaller while unauthenticated — and overrides this field with
/// that value before calling [`TcpBytesService::wait`], since authentication
/// state (unlike the other limits) can change over the life of a connection.
#[derive(Clone, Copy, Debug, Default)]
struct ConnectionLimits {
    maximum_frame_size: Option<FrameSizeLimit>,
    connection_idle_timeout: Option<Duration>,
    io_idle_timeout: Option<Duration>,
}

/// A frame-size limit, and the [`TcpContext`] setting that it comes from.
///
/// [`TcpBytesService::wait`] reports a rejection by each setting as a
/// different [`Error`], so that an operator can tell the two limits apart.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FrameSizeLimit {
    /// [`TcpContext::maximum_frame_size`].
    Full(usize),
    /// [`TcpContext::pre_authentication_maximum_frame_size`].
    PreAuthentication(usize),
}

impl FrameSizeLimit {
    fn size(self) -> usize {
        match self {
            Self::Full(size) | Self::PreAuthentication(size) => size,
        }
    }

    fn rejection(self, frame_size: usize) -> Error {
        match self {
            Self::Full(_) => Error::FrameTooBig(frame_size),
            Self::PreAuthentication(limit) => Error::PreAuthenticationFrameTooBig {
                size: frame_size,
                limit,
            },
        }
    }

    fn reason(self) -> &'static str {
        match self {
            Self::Full(_) => "maximum_frame_size",
            Self::PreAuthentication(_) => "pre_authentication_maximum_frame_size",
        }
    }
}

/// The frame-size limit in effect for the *next* frame on this connection.
///
/// Before authentication completes, this is the smaller of
/// [`TcpContext::maximum_frame_size`] and
/// [`TcpContext::pre_authentication_maximum_frame_size`] (clamping, not
/// substituting, so configuring the full limit below the pre-authentication
/// one can only tighten what an unauthenticated client may send, never loosen
/// it). Once [`AuthenticationExtension::is_authenticated`] reports `true`, the
/// full limit alone applies, matching today's behavior exactly.
///
/// An *absent* [`AuthenticationExtension`] is treated the same as one present
/// but not yet authenticated, not as "authenticated" -- `BytesFrameService`
/// only inserts it after a request has already passed the auth check, so the
/// very first frame on every connection (SASL or not) sees no
/// `AuthenticationExtension` at all. Whether "no authentication configured"
/// applies at all is a separate question, answered by whether
/// [`PreAuthenticationMaximumFrameSizeExtension`] is present (see the `match`
/// below): a listener with no SASL configured never has that extension
/// either, so it falls through to the full limit regardless of
/// `AuthenticationExtension`'s presence.
fn effective_maximum_frame_size(extensions: &Extensions) -> Option<FrameSizeLimit> {
    let full = extensions
        .get_ref::<MaximumFrameSizeExtension>()
        .map(|maximum_frame_size| FrameSizeLimit::Full(maximum_frame_size.0));

    let authenticated = extensions
        .get_ref::<AuthenticationExtension>()
        .is_some_and(|authentication| authentication.is_authenticated());

    if authenticated {
        return full;
    }

    match (
        extensions
            .get_ref::<PreAuthenticationMaximumFrameSizeExtension>()
            .map(|pre_authentication_maximum_frame_size| pre_authentication_maximum_frame_size.0),
        full,
    ) {
        // A full limit that is equal or smaller rejects the frame regardless
        // of authentication, so the rejection names the full limit.
        (Some(pre_authentication), Some(full)) if full.size() <= pre_authentication => Some(full),
        (Some(pre_authentication), _) => {
            Some(FrameSizeLimit::PreAuthentication(pre_authentication))
        }
        (None, full) => full,
    }
}

impl ConnectionLimits {
    fn from_extensions(extensions: &Extensions) -> Self {
        Self {
            maximum_frame_size: extensions
                .get_ref::<MaximumFrameSizeExtension>()
                .map(|maximum_frame_size| FrameSizeLimit::Full(maximum_frame_size.0)),
            connection_idle_timeout: extensions
                .get_ref::<ConnectionIdleTimeoutExtension>()
                .map(|connection_idle_timeout| connection_idle_timeout.0),
            io_idle_timeout: extensions
                .get_ref::<IoIdleTimeoutExtension>()
                .map(|io_idle_timeout| io_idle_timeout.0),
        }
    }
}

impl<S> TcpBytesService<S>
where
    S: Service<BytesInput, Output = Bytes>,
    S::Error: From<Error> + From<io::Error> + Debug,
{
    #[instrument(skip_all)]
    async fn wait<R>(&self, req: &mut R, limits: ConnectionLimits) -> Result<[u8; 4], S::Error>
    where
        R: AsyncReadExt + Unpin,
    {
        let mut size = [0u8; 4];

        // The connection idle timeout guards only the gap *before* a request
        // starts, so it covers the first prefix byte alone. Once that byte
        // lands the peer has committed to a request, and the rest of the
        // prefix is a transfer in progress like any other: otherwise a peer
        // trickling one prefix byte per (long) connection idle window would
        // hold the connection four times as long as a quiet one.
        read_exact_with_idle_timeout(req, &mut size[..1], limits.connection_idle_timeout)
            .await
            .inspect_err(|err| debug!(?err))?;

        read_exact_with_idle_timeout(req, &mut size[1..], limits.io_idle_timeout)
            .await
            .inspect_err(|err| debug!(?err))?;

        let frame_size = frame_size(size)?;

        if let Some(limit) = limits.maximum_frame_size
            && frame_size > limit.size()
        {
            FRAMES_REJECTED.add(1, &[KeyValue::new("reason", limit.reason())]);
            return Err(Into::into(limit.rejection(frame_size)));
        }

        Ok(size)
    }

    /// Reads the frame body declared by `size`, growing the buffer as bytes
    /// actually arrive rather than allocating the full declared length up
    /// front.
    ///
    /// Without this, a peer that declares a near-maximum frame and then
    /// stalls (or trickles bytes just under [`TcpContext::io_idle_timeout`])
    /// would still pin the full declared size in memory immediately, even
    /// though the idle timeout bounds *how long* that can go on. Growing
    /// incrementally bounds *how much* an unfinished, stalled transfer can
    /// pin at any point before its own idle timeout catches it. This alone
    /// doesn't bound how many such connections a peer can open at once; a
    /// connection-count limit is the complementary control for that, and is
    /// tracked separately.
    #[instrument(skip_all)]
    async fn read<R>(
        &self,
        req: &mut R,
        size: [u8; 4],
        limits: ConnectionLimits,
    ) -> Result<Bytes, S::Error>
    where
        R: AsyncReadExt + Unpin,
    {
        let declared = frame_length(size)?;

        let mut request: Vec<u8> = Vec::with_capacity(declared.min(size.len() + IO_CHUNK_SIZE));
        request.extend_from_slice(&size[..]);

        while request.len() < declared {
            let start = request.len();
            let want = (declared - start).min(IO_CHUNK_SIZE);
            request.resize(start + want, 0);

            let n = read_chunk(req, &mut request[start..], limits.io_idle_timeout)
                .await
                .inspect_err(|err| debug!(?err))?;

            if n == 0 {
                return Err(Into::into(io::Error::from(io::ErrorKind::UnexpectedEof)));
            }

            request.truncate(start + n);
        }

        BYTES_RECEIVED.add(request.len() as u64, &[]);

        Ok(Bytes::from(request))
    }

    #[instrument(skip_all)]
    async fn process(
        &self,
        attributes: &[KeyValue],
        request: Bytes,
        extensions: Extensions,
    ) -> Result<Bytes, S::Error> {
        REQUEST_SIZE.record(request.len() as u64, attributes);

        let request_start = SystemTime::now();

        self.inner
            .serve(BytesInput {
                bytes: request,
                extensions,
            })
            .await
            .inspect_err(|err| error!(?err))
            .inspect(|response| {
                RESPONSE_SIZE.record(response.len() as u64, attributes);

                let elapsed_millis = self.elapsed_millis(request_start);

                REQUEST_DURATION.record(elapsed_millis, attributes);
            })
    }

    #[instrument(skip_all)]
    async fn write<W>(
        &self,
        req: &mut W,
        frame: Bytes,
        limits: ConnectionLimits,
    ) -> Result<(), S::Error>
    where
        W: AsyncWriteExt + Unpin,
    {
        let mut written = 0;

        while written < frame.len() {
            let n = write_chunk(req, &frame[written..], limits.io_idle_timeout)
                .await
                .inspect_err(|err| debug!(?err))?;

            if n == 0 {
                return Err(Into::into(io::Error::from(io::ErrorKind::WriteZero)));
            }

            written += n;
        }

        flush_with_idle_timeout(req, limits.io_idle_timeout)
            .await
            .inspect_err(|err| debug!(?err))?;

        BYTES_SENT.add(frame.len() as u64, &[]);

        Ok(())
    }

    #[instrument(skip_all, fields(id = nanoid!()))]
    async fn req<R>(
        &self,
        req: &mut R,
        extensions: &Extensions,
        limits: ConnectionLimits,
        attributes: &[KeyValue],
    ) -> Result<(), S::Error>
    where
        R: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        let limits = ConnectionLimits {
            maximum_frame_size: effective_maximum_frame_size(extensions),
            ..limits
        };

        let size = self.wait(req, limits).await?;
        let request = self.read(req, size, limits).await?;
        let response = self
            .process(attributes, request, extensions.clone())
            .await?;

        // An `acks=0` Produce response is suppressed here, after it's already been
        // assembled and metered by `process`: `RESPONSE_SIZE`/`API_REQUESTS` count a
        // response that is never actually written to the peer. That's a known, accepted
        // minor inaccuracy rather than one worth threading a "don't record this" signal
        // through every layer for.
        if SuppressResponseExtension::take(extensions) {
            return Ok(());
        }

        self.write(req, response, limits).await
    }
}

impl<S, Stream> Service<Stream> for TcpBytesService<S>
where
    S: Service<BytesInput, Output = Bytes>,
    S::Error: From<Error> + From<io::Error> + Debug,
    Stream: AsyncReadExt + AsyncWriteExt + ExtensionsRef + Unpin + Send + Sync + 'static,
{
    type Output = ();

    type Error = S::Error;

    #[instrument(skip(req))]
    async fn serve(&self, mut req: Stream) -> Result<Self::Output, Self::Error> {
        let attributes = {
            let mut attributes = vec![];

            if let Some(cluster_id) = req
                .extensions()
                .get_ref::<ClusterIdExtension>()
                .cloned()
                .map(|cluster_id| cluster_id.0)
            {
                attributes.push(KeyValue::new("cluster_id", cluster_id))
            }

            attributes
        };

        let limits = ConnectionLimits::from_extensions(req.extensions());

        // Each connection gets its own scope, because a listener can hand the
        // same store to every connection that it accepts. A request on this
        // connection then sees the listener's settings, and what it inserts
        // stays on this connection.
        let extensions = req.extensions().fork();
        _ = extensions.insert(SuppressResponseExtension::default());
        _ = extensions.insert(CloseAfterResponseExtension::default());

        loop {
            let attributes = attributes.clone();

            self.req(&mut req, &extensions, limits, &attributes[..])
                .await?;

            // The response is already written and flushed, so the peer gets it
            // before the shutdown's FIN (over TLS, before `close_notify` and the
            // FIN). A peer that has already hung up fails the shutdown, which
            // changes nothing: the connection is over either way.
            if CloseAfterResponseExtension::take(&extensions) {
                debug!("closing the connection after the response");
                _ = req.shutdown().await;
                return Ok(());
            }
        }
    }
}

/// A [`Layer`] that handles and responds with [`Bytes`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesLayer;

impl<S> Layer<S> for BytesLayer {
    type Service = BytesService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A [`Service`] that handles and responds with [`Bytes`]
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BytesService<S> {
    inner: S,
}

impl<S> Debug for BytesService<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(BytesService)).finish()
    }
}

impl<S> Service<BytesInput> for BytesService<S>
where
    S: Service<BytesInput, Output = Bytes>,
{
    type Output = Bytes;
    type Error = S::Error;

    #[instrument(skip_all)]
    async fn serve(&self, req: BytesInput) -> Result<Self::Output, Self::Error> {
        debug!(len = req.bytes.len());
        self.inner
            .serve(req)
            .await
            .inspect(|response| debug!(response_len = response.len()))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
    };

    use tokio::{
        io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf, duplex},
        net::{TcpListener, TcpStream as TokioTcpStream},
        spawn,
    };

    use super::*;

    /// Inner service standing in for the frame router: echoes the request bytes.
    #[derive(Clone, Copy, Debug, Default)]
    struct Echo;

    impl Service<BytesInput> for Echo {
        type Output = Bytes;
        type Error = Error;

        async fn serve(&self, req: BytesInput) -> Result<Bytes, Error> {
            Ok(req.bytes)
        }
    }

    fn service() -> TcpBytesService<Echo> {
        TcpBytesLayer.into_layer(Echo)
    }

    fn header(size: i32) -> [u8; 4] {
        size.to_be_bytes()
    }

    /// Limits with only `maximum_frame_size` set; the two idle timeouts stay
    /// disabled (`None`) so frame-size tests aren't sensitive to timing.
    fn limits(maximum_frame_size: Option<usize>) -> ConnectionLimits {
        ConnectionLimits {
            maximum_frame_size: maximum_frame_size.map(FrameSizeLimit::Full),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn frame_within_limit_is_accepted() -> Result<(), Error> {
        let mut reader = &header(12)[..];

        let size = service().wait(&mut reader, limits(Some(1024))).await?;

        assert_eq!(header(12), size);
        Ok(())
    }

    #[tokio::test]
    async fn frame_exactly_at_limit_is_accepted() -> Result<(), Error> {
        let mut reader = &header(1024)[..];

        let size = service().wait(&mut reader, limits(Some(1024))).await?;

        assert_eq!(header(1024), size);
        Ok(())
    }

    #[tokio::test]
    async fn frame_over_limit_is_rejected() {
        let mut reader = &header(1025)[..];

        let err = service()
            .wait(&mut reader, limits(Some(1024)))
            .await
            .expect_err("oversized frame must be rejected before the body is read");

        assert!(matches!(err, Error::FrameTooBig(1025)), "{err:?}");
    }

    #[test]
    fn listeners_are_bounded_by_default() {
        assert_eq!(
            Some(DEFAULT_MAXIMUM_FRAME_SIZE),
            TcpContext::default().maximum_frame_size
        );
    }

    #[test]
    fn idle_timeouts_are_set_by_default() {
        let ctx = TcpContext::default();
        assert_eq!(
            Some(DEFAULT_CONNECTION_IDLE_TIMEOUT),
            ctx.connection_idle_timeout
        );
        assert_eq!(Some(DEFAULT_IO_IDLE_TIMEOUT), ctx.io_idle_timeout);
    }

    #[tokio::test]
    async fn negative_frame_length_is_rejected() {
        let mut reader = &header(-1)[..];

        let err = service()
            .wait(&mut reader, limits(None))
            .await
            .expect_err("negative frame length must be rejected");

        assert!(matches!(err, Error::InvalidFrameLength(-1)), "{err:?}");
    }

    struct DuplexStreamWithExtensions {
        stream: DuplexStream,
        extensions: Extensions,
    }

    impl DuplexStreamWithExtensions {
        /// The server end of a duplex pair, carrying only the extensions
        /// the test inserts: any limit not inserted is disabled, exactly as
        /// when a [`TcpContext`] field is `None`.
        fn new(stream: DuplexStream, extensions: Extensions) -> Self {
            Self { stream, extensions }
        }
    }

    impl AsRef<DuplexStream> for DuplexStreamWithExtensions {
        fn as_ref(&self) -> &DuplexStream {
            &self.stream
        }
    }

    impl AsyncRead for DuplexStreamWithExtensions {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for DuplexStreamWithExtensions {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.stream).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_shutdown(cx)
        }
    }

    impl ExtensionsRef for DuplexStreamWithExtensions {
        fn extensions(&self) -> &Extensions {
            &self.extensions
        }
    }

    /// How many requests a connection has served, inserted by the handler
    /// on the connection's first request.
    #[derive(Clone, Debug, Default, Extension)]
    struct RequestsOnConnection(Arc<AtomicUsize>);

    /// Inner service that answers each request with how many requests its
    /// connection has served, counting this one.
    #[derive(Clone, Copy, Debug, Default)]
    struct CountsRequestsOnConnection;

    impl Service<BytesInput> for CountsRequestsOnConnection {
        type Output = Bytes;
        type Error = Error;

        async fn serve(&self, req: BytesInput) -> Result<Bytes, Error> {
            let served = req
                .extensions
                .get_ref_or_insert(RequestsOnConnection::default)
                .0
                .fetch_add(1, Ordering::SeqCst)
                + 1;

            let mut frame = header(4).to_vec();
            frame.extend(u32::try_from(served).expect("request count").to_be_bytes());
            Ok(Bytes::from(frame))
        }
    }

    /// Sends a request of four zero bytes on `client` and returns the count
    /// that [`CountsRequestsOnConnection`] answers.
    async fn requests_served(client: &mut DuplexStream) -> Result<u32, Error> {
        client.write_all(&header(4)).await?;
        client.write_all(&[0; 4]).await?;

        let mut response = [0; 8];
        _ = client.read_exact(&mut response).await?;
        Ok(u32::from_be_bytes([
            response[4],
            response[5],
            response[6],
            response[7],
        ]))
    }

    /// An extension that a handler inserts on a connection's first request
    /// is there on the connection's later requests, and not on another
    /// connection, even when the listener hands both connections the same
    /// store. Per-connection state, such as the read shares that limit a
    /// connection's storage reads, relies on this.
    #[tokio::test]
    async fn extensions_a_handler_inserts_stay_with_its_connection() -> Result<(), Error> {
        let listener = Extensions::default();

        let mut connections = vec![];
        for _ in 0..2 {
            let (client, server) = duplex(64);
            let extensions = listener.clone();

            _ = spawn(async move {
                TcpBytesLayer
                    .into_layer(CountsRequestsOnConnection)
                    .serve(DuplexStreamWithExtensions::new(server, extensions))
                    .await
            });

            connections.push(client);
        }

        assert_eq!(1, requests_served(&mut connections[0]).await?);
        assert_eq!(2, requests_served(&mut connections[0]).await?);
        assert_eq!(1, requests_served(&mut connections[1]).await?);
        assert_eq!(3, requests_served(&mut connections[0]).await?);
        assert!(!listener.contains::<RequestsOnConnection>());

        Ok(())
    }

    #[tokio::test]
    async fn serve_rejects_oversized_frame_without_reading_body() -> Result<(), Error> {
        let (mut client, server) = duplex(64);

        let handle = spawn(async move {
            let extensions = Extensions::default();
            _ = extensions.insert(MaximumFrameSizeExtension(1_024));

            service()
                .serve(DuplexStreamWithExtensions::new(server, extensions))
                .await
        });

        const SIZE: usize = 65_536;

        // Only the length prefix is sent. If the guard admitted the frame,
        // `read` would block waiting for a body that never arrives, so the
        // timeout is what turns that into a failure.
        client.write_all(&header(SIZE as i32)).await?;

        let outcome = timeout(Duration::from_secs(5), handle)
            .await
            .expect("oversized frame was admitted: serve is blocked reading the body")?;

        assert!(
            matches!(outcome, Err(Error::FrameTooBig(SIZE))),
            "{outcome:?}"
        );
        Ok(())
    }

    /// A fresh, never-handshaken [`AuthenticationExtension`] reports
    /// `is_authenticated() == false` — exactly the state of a connection that
    /// has sent nothing yet. No real SASL exchange is driven; the point is
    /// only to exercise the "present but not yet authenticated" branch of
    /// [`effective_maximum_frame_size`].
    fn unauthenticated_extension() -> AuthenticationExtension {
        let config = rsasl::config::SASLConfig::with_credentials(
            None,
            "principal".into(),
            "password".into(),
        )
        .expect("sasl config");

        AuthenticationExtension::server(config)
    }

    #[tokio::test]
    async fn unauthenticated_frame_over_pre_authentication_limit_is_rejected() -> Result<(), Error>
    {
        let (mut client, server) = duplex(64);

        let handle = spawn(async move {
            let extensions = Extensions::default();
            _ = extensions.insert(MaximumFrameSizeExtension(DEFAULT_MAXIMUM_FRAME_SIZE));
            _ = extensions.insert(PreAuthenticationMaximumFrameSizeExtension(
                DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE,
            ));
            _ = extensions.insert(unauthenticated_extension());

            service()
                .serve(DuplexStreamWithExtensions::new(server, extensions))
                .await
        });

        let size = DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE + 1;

        // Only the length prefix is sent, same reasoning as
        // `serve_rejects_oversized_frame_without_reading_body`.
        client.write_all(&header(size as i32)).await?;

        let outcome = timeout(Duration::from_secs(5), handle)
            .await
            .expect("frame over the pre-authentication limit was admitted")?;

        assert!(
            matches!(
                outcome,
                Err(Error::PreAuthenticationFrameTooBig { size: s, limit })
                    if s == size && limit == DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE
            ),
            "{outcome:?}"
        );
        Ok(())
    }

    /// The very first frame on a SASL-configured connection has no
    /// `AuthenticationExtension` at all yet -- `BytesFrameService` only
    /// inserts one after a request has already passed the auth check, which
    /// happens strictly after this frame's size has already been checked.
    /// This is the actual attack this ticket closes: one frame per
    /// connection, no prior authenticated request needed. Deliberately does
    /// *not* insert an `AuthenticationExtension` at all, unlike the other
    /// tests in this module.
    #[tokio::test]
    async fn pre_authentication_limit_applies_to_the_first_frame_with_no_authentication_extension_yet()
    -> Result<(), Error> {
        let (mut client, server) = duplex(64);

        let handle = spawn(async move {
            let extensions = Extensions::default();
            _ = extensions.insert(MaximumFrameSizeExtension(DEFAULT_MAXIMUM_FRAME_SIZE));
            _ = extensions.insert(PreAuthenticationMaximumFrameSizeExtension(
                DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE,
            ));
            // No `AuthenticationExtension` inserted at all.

            service()
                .serve(DuplexStreamWithExtensions::new(server, extensions))
                .await
        });

        let size = DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE + 1;

        client.write_all(&header(size as i32)).await?;

        let outcome = timeout(Duration::from_secs(5), handle)
            .await
            .expect("the first frame on a SASL connection was admitted over the pre-authentication limit -- an attacker needs only one frame per connection")?;

        assert!(
            matches!(
                outcome,
                Err(Error::PreAuthenticationFrameTooBig { size: s, limit })
                    if s == size && limit == DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE
            ),
            "{outcome:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn pre_authentication_limit_does_not_apply_without_sasl_configured() -> Result<(), Error>
    {
        let (mut client, server) = duplex(2 * 1024 * 1024);

        let handle = spawn(async move {
            let extensions = Extensions::default();
            _ = extensions.insert(MaximumFrameSizeExtension(DEFAULT_MAXIMUM_FRAME_SIZE));
            // No `AuthenticationExtension` and no
            // `PreAuthenticationMaximumFrameSizeExtension` at all: exactly the
            // state of a listener with no SASL configured
            // (`nisshi-broker/src/service.rs` only inserts the latter when
            // `sasl_config` is `Some`).

            service()
                .serve(DuplexStreamWithExtensions::new(server, extensions))
                .await
        });

        let size = DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE + 1;
        let mut frame = header(size as i32).to_vec();
        frame.extend(vec![0u8; size]);

        client.write_all(&frame).await?;

        let mut echoed = vec![0u8; frame.len()];
        _ = timeout(Duration::from_secs(5), client.read_exact(&mut echoed))
            .await
            .expect("a frame over the pre-authentication limit, but within the full limit, must be accepted when no pre-authentication limit is configured")?;

        assert_eq!(frame, echoed);
        drop(client);
        handle.abort();
        Ok(())
    }

    #[tokio::test]
    async fn pre_authentication_limit_clamps_to_the_lower_full_limit_not_substitutes()
    -> Result<(), Error> {
        let (mut client, server) = duplex(64);

        // The full limit is configured *below* the pre-authentication default:
        // the effective pre-authentication limit must be the smaller of the
        // two (clamped), not the pre-authentication value on its own
        // (substituted) -- substituting would let this frame through.
        const LOWER_FULL_LIMIT: usize = 1_024;

        let handle = spawn(async move {
            let extensions = Extensions::default();
            _ = extensions.insert(MaximumFrameSizeExtension(LOWER_FULL_LIMIT));
            _ = extensions.insert(PreAuthenticationMaximumFrameSizeExtension(
                DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE,
            ));
            _ = extensions.insert(unauthenticated_extension());

            service()
                .serve(DuplexStreamWithExtensions::new(server, extensions))
                .await
        });

        let size = LOWER_FULL_LIMIT + 1;
        assert!(
            size < DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE,
            "test is only meaningful below the pre-authentication default"
        );

        client.write_all(&header(size as i32)).await?;

        let outcome = timeout(Duration::from_secs(5), handle)
            .await
            .expect("a frame over the full limit was admitted just because it's under the pre-authentication default: clamp, don't substitute")?;

        assert!(
            matches!(outcome, Err(Error::FrameTooBig(s)) if s == size),
            "{outcome:?}"
        );
        Ok(())
    }

    /// A single `accept()` error must not stop the loop from serving the next,
    /// successful accept -- the bug this guards against is `tokio::select!`
    /// disabling the `Ok((stream, addr)) = req.accept()` arm for the rest of the
    /// macro invocation whenever that future resolves to `Err`, which, combined
    /// with `join_next` being gated on a non-empty `set`, could wedge the loop
    /// until cancellation. This fails against the pre-fix pattern-matched arm and
    /// passes against the `match`-in-the-body fix.
    #[tokio::test]
    async fn accept_error_does_not_wedge_the_loop() -> Result<(), Box<dyn error::Error>> {
        /// Proves a connection was actually handed to the inner service (not just
        /// accepted) by echoing one byte back over the raw stream.
        #[derive(Clone, Copy, Debug, Default)]
        struct RawEcho;

        impl Service<TcpStream> for RawEcho {
            type Output = ();
            type Error = Error;

            async fn serve(&self, mut stream: TcpStream) -> Result<(), Error> {
                let mut buf = [0u8; 1];
                _ = stream.read_exact(&mut buf).await?;
                stream.write_all(&buf).await?;
                Ok(())
            }
        }

        /// Yields a scripted sequence of accept outcomes, then hangs (as a real
        /// listener with nothing pending would) once the script is exhausted.
        #[derive(Debug)]
        struct ScriptedAcceptor {
            results: Mutex<VecDeque<io::Result<(TokioTcpStream, SocketAddr)>>>,
        }

        impl Acceptor for ScriptedAcceptor {
            async fn accept(&self) -> io::Result<(TokioTcpStream, SocketAddr)> {
                match self.results.lock().await.pop_front() {
                    Some(result) => result,
                    None => std::future::pending().await,
                }
            }
        }

        let real_listener = TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = real_listener.local_addr()?;

        let client = spawn(async move {
            let mut stream = TokioTcpStream::connect(local_addr).await?;
            stream.write_all(b"x").await?;

            let mut buf = [0u8; 1];
            _ = stream.read_exact(&mut buf).await?;

            Ok::<_, io::Error>(buf[0])
        });

        let (stream, addr) = real_listener.accept().await?;
        drop(real_listener);

        let mut results = VecDeque::new();
        results.push_back(Err(io::Error::from(io::ErrorKind::ConnectionAborted)));
        results.push_back(Ok((stream, addr)));

        let acceptor = ScriptedAcceptor {
            results: Mutex::new(results),
        };

        let cancellation = CancellationToken::new();
        let service = TcpListenerLayer::new(cancellation.clone()).layer(RawEcho);

        // Drive the loop directly rather than through `Service<TcpListenerInput>`:
        // that input carries a concrete `TcpListener`, and the whole point here is to
        // inject an accept outcome a real listener can't be made to produce on demand.
        let handle =
            spawn(async move { service.accept_loop(acceptor, Extensions::default()).await });

        let echoed = timeout(Duration::from_secs(5), client).await.expect(
            "accept() error wedged the loop: the Ok connection scripted after it \
                 was never served",
        )??;

        assert_eq!(b'x', echoed);

        cancellation.cancel();
        handle.await??;

        Ok(())
    }

    /// A peer that opens a connection and never sends anything must not hold
    /// the connection (and its task) open forever; this is the Slowloris
    /// vector the connection idle timeout closes. `start_paused` lets tokio
    /// fast-forward straight to the timeout deadline instead of a real sleep.
    #[tokio::test(start_paused = true)]
    async fn quiet_connection_is_closed_after_connection_idle_timeout() -> Result<(), Error> {
        let (_client, server) = duplex(64);

        // Only the connection idle timeout is set, so io_idle_timeout is
        // disabled: this test must fail (not hang, thanks to the outer guard
        // below) if wait() is ever wired to the wrong tier.
        let extensions = Extensions::default();
        _ = extensions.insert(ConnectionIdleTimeoutExtension(Duration::from_millis(50)));

        let outcome = timeout(
            Duration::from_secs(5),
            service().serve(DuplexStreamWithExtensions::new(server, extensions)),
        )
        .await
        .expect("wait() did not respect connection_idle_timeout");

        assert!(
            matches!(&outcome, Err(Error::Io(err)) if err.kind() == io::ErrorKind::TimedOut),
            "{outcome:?}"
        );
        Ok(())
    }

    /// A peer that declares a frame and then stops sending mid-body must not
    /// hold the connection open forever either; the connection idle timeout
    /// only guards the gap *before* a request starts, so this is what the
    /// (shorter) io idle timeout closes instead.
    #[tokio::test(start_paused = true)]
    async fn stalled_mid_frame_read_times_out() -> Result<(), Error> {
        let (mut client, server) = duplex(4096);

        // Only the io idle timeout is set, so connection_idle_timeout is
        // disabled: this test must fail (not hang, thanks to the outer guard
        // below) if read() is ever wired to the wrong tier.
        let handle = spawn(async move {
            let extensions = Extensions::default();
            _ = extensions.insert(IoIdleTimeoutExtension(Duration::from_millis(50)));

            service()
                .serve(DuplexStreamWithExtensions::new(server, extensions))
                .await
        });

        // Declare a 100 byte body, send 10 bytes of it, then go quiet.
        client.write_all(&header(100)).await?;
        client.write_all(&[0u8; 10]).await?;

        let outcome = timeout(Duration::from_secs(5), handle)
            .await
            .expect("read() did not respect io_idle_timeout")?;

        assert!(
            matches!(&outcome, Err(Error::Io(err)) if err.kind() == io::ErrorKind::TimedOut),
            "{outcome:?}"
        );
        Ok(())
    }

    /// A peer that sends part of the 4 byte size prefix and then stalls has
    /// already committed to a request, so the remainder of the prefix is
    /// guarded by the (shorter) io idle timeout, not the connection one.
    #[tokio::test(start_paused = true)]
    async fn stalled_mid_prefix_read_times_out() -> Result<(), Error> {
        let (mut client, server) = duplex(64);

        // Only the io idle timeout is set, so connection_idle_timeout is
        // disabled: this test must fail (not hang, thanks to the outer guard
        // below) if the whole prefix is ever read under the connection tier.
        let handle = spawn(async move {
            let extensions = Extensions::default();
            _ = extensions.insert(IoIdleTimeoutExtension(Duration::from_millis(50)));

            service()
                .serve(DuplexStreamWithExtensions::new(server, extensions))
                .await
        });

        // One byte of the prefix, then silence.
        client.write_all(&header(100)[..1]).await?;

        let outcome = timeout(Duration::from_secs(5), handle)
            .await
            .expect("wait() did not read the rest of the prefix under io_idle_timeout")?;

        assert!(
            matches!(&outcome, Err(Error::Io(err)) if err.kind() == io::ErrorKind::TimedOut),
            "{outcome:?}"
        );
        Ok(())
    }

    /// A slow-but-steadily-progressing transfer must not be penalized: each
    /// chunk resets the idle deadline, so a transfer that would exceed a
    /// single flat whole-transfer timeout still succeeds as long as no
    /// individual gap between chunks exceeds `io_idle_timeout`. Uses real
    /// time with a wide margin (chunk gap 20x under the deadline, so a
    /// loaded CI runner overshooting a sleep can't fail it) rather than
    /// paused time, to avoid orchestrating a multi-step manual clock
    /// advance around the two concurrent tasks below.
    #[tokio::test]
    async fn slow_but_steady_transfer_is_not_penalized() -> Result<(), Error> {
        let (mut client, server) = duplex(4096);

        let handle = spawn(async move {
            let extensions = Extensions::default();
            _ = extensions.insert(IoIdleTimeoutExtension(Duration::from_secs(2)));

            service()
                .serve(DuplexStreamWithExtensions::new(server, extensions))
                .await
        });

        let body = vec![7u8; 300];
        client.write_all(&header(body.len() as i32)).await?;

        // Three chunks, ~100ms apart: no single gap comes near the 2s idle
        // timeout, while a single flat whole-transfer deadline would still
        // have been exceeded by a transfer with that many gaps at that size.
        for chunk in body.chunks(100) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            client.write_all(chunk).await?;
        }

        // `serve` loops forever reading requests, so it never returns `Ok`;
        // instead prove the slow send wasn't penalized by reading back the
        // echoed response in full before doing anything else.
        let mut response_size = [0u8; 4];
        _ = client.read_exact(&mut response_size).await?;
        let mut response_body = vec![0u8; frame_length(response_size)? - 4];
        _ = client.read_exact(&mut response_body).await?;
        assert_eq!(body, response_body);

        // Now end the connection; `serve`'s next `wait()` should see a clean
        // EOF, not the timeout a penalized slow transfer would have produced
        // instead.
        drop(client);

        let outcome = timeout(Duration::from_secs(5), handle)
            .await
            .expect("serve should observe EOF promptly after the client disconnects")?;

        assert!(
            matches!(&outcome, Err(Error::Io(err)) if err.kind() == io::ErrorKind::UnexpectedEof),
            "{outcome:?}"
        );
        Ok(())
    }

    /// The duplex tests above insert extensions by hand, so they would stay
    /// green even if [`TcpContextService`] never put the timeouts on the
    /// stream. This runs the production stack (`TcpContextLayer` over
    /// `TcpBytesLayer`, as the broker, proxy and client compose it) over a
    /// real loopback socket to prove the [`TcpContext`] setting reaches
    /// `wait()`.
    #[tokio::test]
    async fn tcp_context_layer_applies_connection_idle_timeout() -> Result<(), Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let _client = TokioTcpStream::connect(listener.local_addr()?).await?;
        let (accepted, _) = listener.accept().await?;

        let service = (
            TcpContextLayer::new(
                TcpContext::default()
                    .connection_idle_timeout(Some(Duration::from_millis(50)))
                    .io_idle_timeout(None),
            ),
            TcpBytesLayer,
        )
            .into_layer(Echo);

        let outcome = timeout(
            Duration::from_secs(5),
            service.serve(TcpStream::from_tokio_tcp_stream(
                accepted,
                Extensions::default(),
            )),
        )
        .await
        .expect("TcpContextService did not wire connection_idle_timeout into the stream");

        assert!(
            matches!(&outcome, Err(Error::Io(err)) if err.kind() == io::ErrorKind::TimedOut),
            "{outcome:?}"
        );
        Ok(())
    }

    /// Same as above for the io tier: the connection idle timeout is
    /// disabled and a frame is declared but never sent, so only a correctly
    /// wired `io_idle_timeout` can end the connection.
    #[tokio::test]
    async fn tcp_context_layer_applies_io_idle_timeout() -> Result<(), Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut client = TokioTcpStream::connect(listener.local_addr()?).await?;
        let (accepted, _) = listener.accept().await?;

        let service = (
            TcpContextLayer::new(
                TcpContext::default()
                    .connection_idle_timeout(None)
                    .io_idle_timeout(Some(Duration::from_millis(50))),
            ),
            TcpBytesLayer,
        )
            .into_layer(Echo);

        client.write_all(&header(100)).await?;

        let outcome = timeout(
            Duration::from_secs(5),
            service.serve(TcpStream::from_tokio_tcp_stream(
                accepted,
                Extensions::default(),
            )),
        )
        .await
        .expect("TcpContextService did not wire io_idle_timeout into the stream");

        assert!(
            matches!(&outcome, Err(Error::Io(err)) if err.kind() == io::ErrorKind::TimedOut),
            "{outcome:?}"
        );
        Ok(())
    }

    /// Same as above for the pre-authentication frame size: the stream starts
    /// with empty extensions, so only a correctly wired
    /// `pre_authentication_maximum_frame_size` can reject a frame that the
    /// full limit admits.
    #[tokio::test]
    async fn tcp_context_layer_applies_pre_authentication_maximum_frame_size() -> Result<(), Error>
    {
        const LIMIT: usize = 1_024;

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut client = TokioTcpStream::connect(listener.local_addr()?).await?;
        let (accepted, _) = listener.accept().await?;

        let service = (
            TcpContextLayer::new(
                TcpContext::default()
                    .pre_authentication_maximum_frame_size(Some(LIMIT))
                    .connection_idle_timeout(None)
                    .io_idle_timeout(None),
            ),
            TcpBytesLayer,
        )
            .into_layer(Echo);

        client.write_all(&header(LIMIT as i32 + 1)).await?;

        let outcome = timeout(
            Duration::from_secs(5),
            service.serve(TcpStream::from_tokio_tcp_stream(
                accepted,
                Extensions::default(),
            )),
        )
        .await
        .expect(
            "TcpContextService did not wire pre_authentication_maximum_frame_size into the stream",
        );

        assert!(
            matches!(
                outcome,
                Err(Error::PreAuthenticationFrameTooBig { size, limit })
                    if size == LIMIT + 1 && limit == LIMIT
            ),
            "{outcome:?}"
        );
        Ok(())
    }
}
