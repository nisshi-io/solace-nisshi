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

pub mod group;
mod maintenance;

use crate::{
    CancelKind, Error, Result,
    coordinator::group::{
        Coordinator,
        administrator::{Controller, GroupCoordinatorConfig},
    },
    otel,
    service::services,
};
use console::Term;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use nisshi_sans_io::{ErrorCode, RootMessageMeta};
use nisshi_schema::{
    Registry,
    lake::{House, LakeHouseType},
    redact_url,
};
use nisshi_service::{PeerAddressExtension, ProgressBarExtension};
use nisshi_storage::{
    ArcDynStorage, AutoCreateRefusals, BrokerRegistrationRequest, Storage, StorageContainer,
    TopicCreationConfig,
};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use rama::{Service, ServiceInput, extensions::Extensions, tcp::TcpStream};
use rsasl::config::SASLConfig;
use rustls::ServerConfig;
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    io::{self, ErrorKind},
    marker::PhantomData,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener as StdTcpListener},
    sync::Arc,
    time::{Duration, SystemTime},
};
#[cfg(unix)]
use tokio::signal::unix::{SignalKind, signal};
use tokio::{
    net::{TcpListener, lookup_host},
    task::{AbortHandle, JoinSet},
    time::{self, Instant, sleep, timeout},
};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Level, debug, error, info, span, warn};
use url::Url;
use uuid::Uuid;

/// How long a client has to complete the TLS handshake once its TCP
/// connection is accepted. Bounds the rustls state held for a peer that
/// connects and never speaks, much like Kafka's `connections.max.idle.ms`.
pub const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Report a failed TLS handshake at a level that matches its cause.
///
/// A peer that connects and hangs up (health checks, port scanners) is
/// routine and logged at debug, mirroring how the plaintext path swallows
/// EOF and reset. Anything else is a client that spoke but could not
/// negotiate: almost always a plaintext client or one that does not trust
/// the broker certificate, so the warning says so.
fn handshake_failed(addr: SocketAddr, err: &io::Error) {
    match err.kind() {
        ErrorKind::UnexpectedEof | ErrorKind::BrokenPipe | ErrorKind::ConnectionReset => {
            debug!(%addr, ?err, "peer closed during tls handshake");
        }

        _ => {
            warn!(
                %addr,
                ?err,
                "tls handshake failed: the client is either plaintext (set security.protocol=SSL) or does not trust the broker certificate"
            );
        }
    }
}

/// Pending-connection backlog passed to `listen(2)`.
///
/// `mio`, which `tokio::net::TcpListener::bind` uses, listens with a backlog of 128. This
/// socket uses a larger queue. The kernel caps the value at `somaxconn`.
const LISTEN_BACKLOG: i32 = 1024;

/// Builds, binds and arms a listening socket for `addr`, ready for
/// [`TcpListener::from_std`](tokio::net::TcpListener::from_std).
fn configure_listener(addr: SocketAddr) -> io::Result<Socket> {
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    arm(socket, addr)
}

/// Binds `socket` to `addr`, listens, and makes it non-blocking.
///
/// macOS and Linux both default a new IPv6 socket to dual-stack (it also accepts
/// IPv4-mapped connections), but that default is a tunable OS setting
/// (`net.inet6.ip6.v6only` on macOS, `bindv6only` on Linux) a host can override. Clearing
/// `IPV6_V6ONLY` here removes the dependency on that tuning, so an IPv6 `addr` always
/// binds dual-stack. Setting `IPV6_V6ONLY` on an IPv4 socket fails, so this is skipped for
/// an IPv4 `addr`.
fn arm(socket: Socket, addr: SocketAddr) -> io::Result<Socket> {
    if addr.is_ipv6() {
        socket.set_only_v6(false)?;
    }

    // `TcpListener::bind` sets this on Unix by default; a bare `socket2::Socket` does not,
    // so a restart while a prior connection is in `TIME_WAIT` would otherwise fail with
    // `EADDRINUSE`.
    socket.set_reuse_address(true)?;
    socket.bind(&addr.into())?;
    socket.listen(LISTEN_BACKLOG)?;

    // `tokio::net::TcpListener::from_std` requires a non-blocking, already-listening
    // socket.
    socket.set_nonblocking(true)?;

    Ok(socket)
}

/// Returns `true` when `err` means the host does not support the address family of the
/// socket, for example IPv6 on a kernel booted with `ipv6.disable=1`.
fn is_address_family_unsupported(err: &io::Error) -> bool {
    err.raw_os_error() == Some(address_family_unsupported_os_error())
}

/// The OS error code for "address family not supported", as `raw_os_error()` reports it
/// on this platform.
///
/// Unix reports it as the C runtime errno `EAFNOSUPPORT`. Windows reports socket errors
/// through `WSAGetLastError`, so `raw_os_error()` there is `WSAEAFNOSUPPORT` (10047), a
/// different number from the C runtime errno that `libc::EAFNOSUPPORT` holds on Windows.
#[cfg(unix)]
fn address_family_unsupported_os_error() -> i32 {
    libc::EAFNOSUPPORT
}

#[cfg(windows)]
fn address_family_unsupported_os_error() -> i32 {
    /// <https://learn.microsoft.com/windows/win32/winsock/windows-sockets-error-codes-2>
    const WSAEAFNOSUPPORT: i32 = 10047;

    WSAEAFNOSUPPORT
}

/// Returns the IPv4 address to bind after binding `addr` failed with `err`, or `None` when
/// the failure stands.
///
/// Only the IPv6 unspecified address falls back, to `0.0.0.0` on the same port, and only
/// when the host does not support IPv6. Apache Kafka's listener with a blank host does the
/// same, because the JDK binds it to `0.0.0.0` on such a host. An explicit IPv6 address
/// does not fall back, because the operator asked for that address.
fn ipv4_fallback(addr: SocketAddr, err: &io::Error) -> Option<SocketAddr> {
    (addr.ip() == IpAddr::V6(Ipv6Addr::UNSPECIFIED) && is_address_family_unsupported(err))
        .then(|| SocketAddr::from((Ipv4Addr::UNSPECIFIED, addr.port())))
}

/// Binds a listening, non-blocking, standard-library socket for `addr`, falling back to
/// IPv4 as [`ipv4_fallback`] decides.
fn bind_listener(addr: SocketAddr) -> io::Result<StdTcpListener> {
    configure_listener(addr)
        .or_else(|err| match ipv4_fallback(addr, &err) {
            Some(fallback) => {
                warn!(%addr, %fallback, ?err, "IPv6 is unavailable on this host, listening on IPv4 only");
                configure_listener(fallback)
            }
            None => Err(err),
        })
        .map(StdTcpListener::from)
}

/// Binds the broker's listener to `addr`, as [`bind_listener`] does.
///
/// # Errors
///
/// Returns [`Error::ListenerInUse`] when another socket already listens on `addr`.
fn bind(addr: SocketAddr) -> Result<TcpListener> {
    bind_listener(addr)
        .and_then(TcpListener::from_std)
        .map_err(|err| {
            if err.kind() == ErrorKind::AddrInUse {
                Error::ListenerInUse { addr }
            } else {
                err.into()
            }
        })
}

/// Returns the address that the broker's listener binds for the `listener` URL.
///
/// A URL without a host listens on `[::]`, and the port defaults to 9092. An IP literal is
/// used as it is: under a non-special scheme such as `tcp`, the `url` crate parses an IPv4
/// literal as a domain, so a domain is parsed as an IP address before it is looked up. Any
/// other host name is resolved, as Apache Kafka resolves the host of a listener, and the
/// listener binds the address that [`preferred_listener_addr`] picks.
///
/// # Errors
///
/// Returns [`Error::ListenerHostUnresolved`] when the host name resolves to no address.
async fn listener_addr(listener: &Url) -> Result<SocketAddr> {
    let port = listener.port().unwrap_or(9092);

    match listener.host() {
        None => Ok(SocketAddr::from((Ipv6Addr::UNSPECIFIED, port))),
        Some(url::Host::Ipv4(ip)) => Ok(SocketAddr::from((ip, port))),
        Some(url::Host::Ipv6(ip)) => Ok(SocketAddr::from((ip, port))),
        Some(url::Host::Domain(domain)) => {
            if let Ok(ip) = domain.parse::<IpAddr>() {
                return Ok(SocketAddr::from((ip, port)));
            }

            let unresolved = |source| Error::ListenerHostUnresolved {
                host: domain.to_owned(),
                port,
                source: Arc::new(source),
            };

            let resolved = lookup_host((domain, port))
                .await
                .map_err(unresolved)?
                .collect::<Vec<_>>();
            debug!(domain, ?resolved);

            preferred_listener_addr(resolved)
                .inspect(|addr| info!(host = domain, %addr, "listener host resolved"))
                .ok_or_else(|| {
                    unresolved(io::Error::new(
                        ErrorKind::NotFound,
                        "the name resolved to no address",
                    ))
                })
        }
    }
}

/// Picks the address that the listener binds from the addresses that its host name resolved
/// to: the first IPv4 address, or the first address when there is none.
///
/// The JDK does the same by default (`java.net.preferIPv6Addresses=false`), so a name such as
/// `localhost`, which macOS resolves to `::1` first, binds `127.0.0.1` as Apache Kafka would.
fn preferred_listener_addr(resolved: impl IntoIterator<Item = SocketAddr>) -> Option<SocketAddr> {
    let mut first = None;

    for addr in resolved {
        if addr.is_ipv4() {
            return Some(addr);
        }

        _ = first.get_or_insert(addr);
    }

    first
}

#[derive(Clone, Debug)]
pub struct Broker<G, S> {
    node_id: i32,
    cluster_id: String,
    incarnation_id: Uuid,
    listener: Url,
    advertised_listener: Url,
    storage: S,
    groups: G,
    topic_creation: TopicCreationConfig,
    auto_create_refusals: AutoCreateRefusals,

    sasl_config: Option<Arc<SASLConfig>>,
    tls_server_config: Option<Arc<ServerConfig>>,
    silent: bool,
    banner: Option<String>,
    maintenance_interval: Option<Duration>,
    transaction_maintenance_interval: Option<Duration>,

    cancellation: CancellationToken,

    /// Present when OTLP metrics are enabled; flushed and shut down when
    /// `main` returns so the last export interval is not lost on exit.
    meter_provider: Option<SdkMeterProvider>,
}

impl<G, S> Broker<G, S>
where
    G: Coordinator,
    S: Storage + Clone + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: i32,
        cluster_id: &str,
        listener: Url,
        advertised_listener: Url,
        storage: S,
        groups: G,
        incarnation_id: Uuid,
    ) -> Self {
        Self {
            node_id,
            cluster_id: cluster_id.to_owned(),
            incarnation_id,
            listener,
            advertised_listener,
            storage,
            groups,
            topic_creation: TopicCreationConfig::default(),
            auto_create_refusals: AutoCreateRefusals::default(),

            sasl_config: None,
            tls_server_config: None,

            silent: false,
            banner: None,

            maintenance_interval: None,
            transaction_maintenance_interval: None,

            cancellation: CancellationToken::new(),

            meter_provider: None,
        }
    }

    pub fn builder() -> PhantomBuilder {
        Builder::default()
    }

    pub async fn main(mut self, started: Instant) -> Result<ErrorCode> {
        {
            let root_meta = RootMessageMeta::messages();
            debug!(
                messages = root_meta
                    .requests()
                    .values()
                    .map(|meta| meta.name)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        let mut set = JoinSet::new();

        #[cfg(unix)]
        let mut interrupt_signal = signal(SignalKind::interrupt())?;
        #[cfg(windows)]
        let mut interrupt_signal = tokio::signal::windows::ctrl_c()?;
        debug!(?interrupt_signal);

        #[cfg(unix)]
        let mut terminate_signal = signal(SignalKind::terminate())?;
        // Windows has no SIGTERM for a console program, so this listens for the nearest event: the
        // user closing the console window, or ending the task in Task Manager. A service manager's
        // stop request doesn't send it. Windows ends the process 5 seconds after the event by
        // default, whether or not shutdown has finished.
        #[cfg(windows)]
        let mut terminate_signal = tokio::signal::windows::ctrl_close()?;
        debug!(?terminate_signal);

        let silent = self.silent;

        let token = self.cancellation.clone();

        let meter_provider = self.meter_provider.take();

        _ = set.spawn(async move { self.serve(started).await });

        // A serve task that ends on its own is a failed (or lost) broker: the
        // process must exit non-zero so a supervisor restarts it.
        let mut failure = None;

        let kind = tokio::select! {
            v = set.join_next() => {
                debug!(?v);
                failure = match v {
                    // The caller reports these errors with the fix, so they are logged
                    // here only at debug, to avoid printing them twice.
                    Some(Ok(Err(
                        err @ (Error::ListenerInUse { .. } | Error::ListenerHostUnresolved { .. }),
                    ))) => {
                        debug!(?err);
                        Some(err)
                    }
                    Some(Ok(Err(err))) => {
                        error!(?err);
                        Some(err)
                    }
                    Some(Err(err)) => {
                        error!(?err);
                        Some(Error::from(err))
                    }
                    Some(Ok(Ok(()))) | None => None,
                };
                None
            }

            interrupt = interrupt_signal.recv() => {
                debug!(?interrupt);
                Some(CancelKind::Interrupt)
            }

            terminate = terminate_signal.recv() => {
                debug!(?terminate);
                Some(CancelKind::Terminate)
            }
        };

        if let Some(kind) = kind {
            token.cancel();

            let cleanup = async {
                while !set.is_empty() {
                    debug!(len = set.len());

                    _ = set.join_next().await;
                }
            };

            let patience = sleep(Duration::from(kind));

            tokio::select! {
                v = cleanup => {
                    debug!(?v)
                }

                _ = patience => {
                    debug!(aborting = set.len());
                    set.abort_all();

                    while !set.is_empty() {
                        _ = set.join_next().await;
                    }
                }
            }

            if !silent {
                let stdout = Term::stdout();

                if stdout.is_term() {
                    _ = stdout.clear_screen().ok();
                }
            }
        }

        // A failed final export should not turn a clean shutdown into an
        // error exit; it is reported and the broker still exits cleanly.
        if let Some(meter_provider) = meter_provider {
            if let Err(err) = meter_provider.force_flush() {
                warn!(?err, "OTLP metrics could not be flushed on shutdown");
            }

            if let Err(err) = meter_provider.shutdown() {
                warn!(?err, "OTLP metrics provider could not be shut down");
            }
        }

        failure.map_or(Ok(ErrorCode::None), Err)
    }

    pub async fn serve(&mut self, started: Instant) -> Result<()> {
        self.register().await?;
        self.listen(started).await
    }

    pub async fn register(&mut self) -> Result<()> {
        self.storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: self.node_id,
                cluster_id: self.cluster_id.clone(),
                incarnation_id: self.incarnation_id,
                rack: None,
            })
            .await
            .map_err(Into::into)
    }

    pub async fn listen(&self, started: Instant) -> Result<()> {
        debug!(%self.listener, %self.advertised_listener);

        let addr = listener_addr(&self.listener).await?;

        let listener = bind(addr)
            .inspect(|listener| debug!(listener = ?listener.local_addr().ok()))
            .inspect_err(|err| match err {
                Error::Io(err) if is_address_family_unsupported(err) => {
                    error!(
                        ?err,
                        %addr,
                        "this host does not support the address family of \
                         --kafka-listener-url (LISTENER_URL), set it to an address this host \
                         supports, such as tcp://0.0.0.0:9092"
                    );
                }
                Error::ListenerInUse { .. } => {}
                _ => {
                    error!(?err, %addr, %self.advertised_listener, "failed to bind the listener");
                }
            })?;

        let mut interval =
            time::interval(self.maintenance_interval.unwrap_or(Duration::from_mins(10)));

        let mut txn_interval = time::interval(
            self.transaction_maintenance_interval
                .unwrap_or(Duration::from_secs(10)),
        );

        // Periodic sweeps: skip missed ticks rather than firing them in a burst.
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        txn_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

        // Track the in-flight sweep per interval so a sweep slower than its
        // interval can't overlap itself or pile up: skip the tick while the
        // previous sweep is still running.
        let mut maintenance_sweep: Option<AbortHandle> = None;
        let mut txn_sweep: Option<AbortHandle> = None;

        let mut set = JoinSet::new();

        let m = MultiProgress::new();

        let spinner_style = ProgressStyle::with_template("{prefix:.bold.dim} {spinner} {msg}")?
            .tick_chars("⠁⠂⠄⡀⢀⠠⠐");

        let ls = if self.silent {
            None
        } else {
            if let Some(banner) = &self.banner {
                print!("{banner}");
            }

            println!("ready in {}ms", started.elapsed().as_millis(),);

            let ls = m.add(ProgressBar::new_spinner());
            ls.set_style(spinner_style.clone());

            if let Ok(local_addr) = listener.local_addr() {
                ls.set_prefix(format!("[{local_addr:?}]"));
            }

            ls.set_message("listening for connection...");

            Some(ls)
        };

        // When a TLS configuration is present every accepted connection must
        // complete a TLS handshake before any Kafka frame is read: the listener
        // is TLS only. Without one the listener stays plain TCP.
        let acceptor = self.tls_server_config.clone().map(TlsAcceptor::from);

        if acceptor.is_some() {
            info!(%self.listener, "listener is tls only");
        }

        let mut connections = 0;

        // Backs off after a run of consecutive accept()-arm errors that look like
        // resource exhaustion (e.g. EMFILE/ENFILE), so a persistent failure doesn't
        // spin the loop at 100% CPU. A routine, expected per-connection error
        // (ConnectionAborted: a peer reset before we could accept it) never backs off.
        // The sleep runs inside the select! arm, blocking the whole select! call, so it
        // is kept short and capped.
        let mut consecutive_accept_errors: u32 = 0;

        loop {
            connections += 1;

            if let Some(ref ls) = ls {
                ls.tick();
            }

            tokio::select! {
                result = listener.accept() => {
                    let (stream, addr) = match result {
                        Ok(accepted) => {
                            consecutive_accept_errors = 0;
                            accepted
                        }

                        Err(err) => {
                            error!(?err, "accept() failed; continuing to listen");

                            if err.kind() != ErrorKind::ConnectionAborted {
                                let backoff = Duration::from_millis(5)
                                    .saturating_mul(1u32 << consecutive_accept_errors.min(6))
                                    .min(Duration::from_millis(200));

                                consecutive_accept_errors = consecutive_accept_errors.saturating_add(1);

                                sleep(backoff).await;
                            } else {
                                consecutive_accept_errors = 0;
                            }

                            continue;
                        }
                    };

                    if let Err(err) = stream.set_nodelay(true) {
                        error!(?err, %addr, "set_nodelay failed; dropping connection");
                        continue;
                    }

                    let extensions = Extensions::default();
                    _ = extensions.insert(PeerAddressExtension::new(addr));

                    let pb = if self.silent {
                        None
                    } else {
                        let pb = m.add(ProgressBar::new_spinner());
                        pb.set_style(spinner_style.clone());
                        pb.set_prefix(format!("[{connections}/{:?}]", addr));
                        pb.set_message("connected");
                        pb.tick();

                        _ = extensions.insert(ProgressBarExtension::new(pb.clone()));
                        Some(pb)
                    };

                    let service = services(
                        self.cluster_id.as_str(),
                        self.groups.clone(),
                        self.storage.clone(),
                        self.topic_creation,
                        self.auto_create_refusals.clone(),
                        self.sasl_config.clone()
                    )?;

                    let acceptor = acceptor.clone();

                    let handle = set.spawn(async move {
                        // The handshake runs inside the connection task, bounded by
                        // TLS_HANDSHAKE_TIMEOUT, so a slow or hostile client can
                        // neither stall the accept loop nor pin rustls state forever.
                        let result = match acceptor {
                            Some(acceptor) => {
                                match timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                                    Ok(Ok(tls)) => {
                                        service.serve(ServiceInput { input: tls, extensions }).await
                                    }
                                    Ok(Err(err)) => {
                                        handshake_failed(addr, &err);
                                        Ok(())
                                    }
                                    Err(elapsed) => {
                                        debug!(%addr, %elapsed, "tls handshake timed out");
                                        Ok(())
                                    }
                                }
                            }
                            None => {
                                service
                                    .serve(TcpStream::from_tokio_tcp_stream(stream, extensions))
                                    .await
                            }
                        };

                        match result {
                            Err(Error::Io(ref io))
                                if io.kind() == ErrorKind::UnexpectedEof
                                    || io.kind() == ErrorKind::BrokenPipe
                                    || io.kind() == ErrorKind::ConnectionReset
                                    // A quiet connection closed by its idle timeout is
                                    // routine (e.g. a consumer polling infrequently),
                                    // not an anomaly worth an `error!` on every occurrence.
                                    || io.kind() == ErrorKind::TimedOut => {}

                            // The broker closes the connection of a client that sends a
                            // request other than ApiVersions or SASL before it
                            // authenticates. This line names the peer at ERROR, the level
                            // of the default filter, because that filter disables the
                            // INFO `peer` span that carries the address.
                            Err(Error::KafkaProtocol(nisshi_sans_io::Error::NotAuthenticated)) => {
                                error!(
                                    %addr,
                                    "closed connection: client sent a request before it authenticated"
                                );
                            }

                            // An acks=0 Produce that fails closes the connection. This arm
                            // logs that close at `info`, as Kafka does ("Closing connection
                            // due to error during produce request"), because the cause is
                            // usually the client's input. `TcpBytesService` has already
                            // logged the same error at `error`; this arm sets the level of
                            // the broker's connection-level line only.
                            Err(Error::Storage(nisshi_storage::Error::AcksZeroProduceFailed {
                                ref topic,
                                partition,
                                error_code,
                            })) => {
                                info!(
                                    topic,
                                    partition,
                                    ?error_code,
                                    "closing connection: acks=0 produce failed"
                                );
                            }

                            Err(error) => {
                                error!(?error);
                            },

                            Ok(response) => {
                                debug!(?response)
                            }
                        }

                        if let Some(ref pb) = pb {
                            pb.finish_and_clear();
                        }
                    }.instrument(span!(Level::INFO, "peer", %addr)));


                    debug!(?handle);

                    continue;
                }

                _ = interval.tick() => {
                    if maintenance_sweep.as_ref().is_some_and(|handle| !handle.is_finished()) {
                        debug!("maintain still in flight; skipping tick");
                    } else {
                        let storage = self.storage.clone();

                        let handle = set.spawn(async move {
                            let span = span!(Level::DEBUG, "maintenance");

                            async move {
                                if let Err(err) = storage.maintain(SystemTime::now()).await {
                                    debug!(?err);
                                }
                            }
                            .instrument(span)
                            .await
                        });

                        debug!(?handle);
                        maintenance_sweep = Some(handle);
                    }
                }

                _ = txn_interval.tick() => {
                    if txn_sweep.as_ref().is_some_and(|handle| !handle.is_finished()) {
                        debug!("maintain_transactions still in flight; skipping tick");
                    } else {
                        let storage = self.storage.clone();

                        let handle = set.spawn(async move {
                            let span = span!(Level::DEBUG, "maintain_transactions");

                            async move {
                                if let Err(err) = storage.maintain_transactions(SystemTime::now()).await {
                                    debug!(?err);
                                }
                            }
                            .instrument(span)
                            .await
                        });

                        debug!(?handle);
                        txn_sweep = Some(handle);
                    }
                }

                v = set.join_next(), if !set.is_empty() => {
                    debug!(?v);
                }

                message = self.cancellation.cancelled() => {
                    debug!(?message);
                    break;
                }
            }
        }

        while !set.is_empty() {
            debug!(len = set.len());

            _ = set.join_next().await;
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct Builder<N, C, I, A, S, L> {
    node_id: N,
    cluster_id: C,
    incarnation_id: I,
    advertised_listener: A,
    storage: S,
    listener: L,
    otlp_endpoint_url: Option<Url>,
    schema_registry: Option<Registry>,
    lake_house: Option<House>,
    authentication: bool,
    tls_server_config: Option<ServerConfig>,
    silent: bool,
    banner: Option<String>,
    group_coordinator: GroupCoordinatorConfig,
    topic_creation: TopicCreationConfig,

    cancellation: CancellationToken,
}

type PhantomBuilder = Builder<
    PhantomData<i32>,
    PhantomData<String>,
    PhantomData<Uuid>,
    PhantomData<Url>,
    PhantomData<Url>,
    PhantomData<Url>,
>;

impl<N, C, I, A, S, L> Builder<N, C, I, A, S, L> {
    pub fn node_id(self, node_id: i32) -> Builder<i32, C, I, A, S, L> {
        Builder {
            node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            banner: self.banner,
            group_coordinator: self.group_coordinator,
            topic_creation: self.topic_creation,
            cancellation: self.cancellation,
        }
    }

    pub fn cluster_id(self, cluster_id: impl Into<String>) -> Builder<N, String, I, A, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: cluster_id.into(),
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            banner: self.banner,
            group_coordinator: self.group_coordinator,
            topic_creation: self.topic_creation,

            cancellation: self.cancellation,
        }
    }

    pub fn incarnation_id(self, incarnation_id: impl Into<Uuid>) -> Builder<N, C, Uuid, A, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: incarnation_id.into(),
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            banner: self.banner,
            group_coordinator: self.group_coordinator,
            topic_creation: self.topic_creation,

            cancellation: self.cancellation,
        }
    }

    pub fn advertised_listener(
        self,
        advertised_listener: impl Into<Url>,
    ) -> Builder<N, C, I, Url, S, L> {
        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: advertised_listener.into(),
            storage: self.storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            banner: self.banner,
            group_coordinator: self.group_coordinator,
            topic_creation: self.topic_creation,

            cancellation: self.cancellation,
        }
    }

    pub fn storage(self, storage: Url) -> Builder<N, C, I, A, Url, L> {
        debug!(storage = %redact_url(&storage));

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage,
            listener: self.listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            banner: self.banner,
            group_coordinator: self.group_coordinator,
            topic_creation: self.topic_creation,

            cancellation: self.cancellation,
        }
    }

    pub fn listener(self, listener: Url) -> Builder<N, C, I, A, S, Url> {
        debug!(%listener);

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            incarnation_id: self.incarnation_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            listener,
            otlp_endpoint_url: self.otlp_endpoint_url,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            authentication: self.authentication,
            tls_server_config: self.tls_server_config,
            silent: self.silent,
            banner: self.banner,
            group_coordinator: self.group_coordinator,
            topic_creation: self.topic_creation,

            cancellation: self.cancellation,
        }
    }

    pub fn schema_registry(self, schema_registry: Option<Registry>) -> Self {
        Self {
            schema_registry,
            ..self
        }
    }

    pub fn lake_house(self, lake_house: Option<House>) -> Self {
        _ = lake_house.as_ref().inspect(|lake_house| {
            debug!(lake_house = ?LakeHouseType::from(*lake_house));
        });

        Self { lake_house, ..self }
    }

    pub fn otlp_endpoint_url(self, otlp_endpoint_url: Option<Url>) -> Self {
        Self {
            otlp_endpoint_url,
            ..self
        }
    }

    pub fn authentication(self, authentication: bool) -> Self {
        Self {
            authentication,
            ..self
        }
    }

    pub fn tls_server_config(self, tls_server_config: Option<ServerConfig>) -> Self {
        Self {
            tls_server_config,
            ..self
        }
    }
    pub fn silent(self, silent: bool) -> Self {
        Self { silent, ..self }
    }

    /// Text that the broker prints to stdout once its listener is bound, before `ready in`,
    /// unless it is silent. A broker that fails to start prints none of it.
    pub fn banner(self, banner: Option<String>) -> Self {
        Self { banner, ..self }
    }

    pub fn group_coordinator(self, group_coordinator: GroupCoordinatorConfig) -> Self {
        Self {
            group_coordinator,
            ..self
        }
    }

    pub fn topic_creation(self, topic_creation: TopicCreationConfig) -> Self {
        Self {
            topic_creation,
            ..self
        }
    }
}

impl Builder<i32, String, Uuid, Url, Url, Url> {
    pub async fn build(self) -> Result<Broker<Controller<ArcDynStorage>, ArcDynStorage>> {
        let meter_provider = self
            .otlp_endpoint_url
            .clone()
            .inspect(|otlp_endpoint_url| debug!(%otlp_endpoint_url))
            .map(otel::metric_exporter)
            .transpose()?;

        let (storage, intervals) = maintenance::take_intervals(self.storage.clone())?;
        debug!(?intervals);

        let builder = {
            let mut builder = StorageContainer::builder();

            builder.with_factory(Arc::new(nisshi_storage_null::EngineFactory));

            #[cfg(feature = "dynostore")]
            builder.with_factory(Arc::new(nisshi_storage_dynostore::MemoryEngineFactory));

            #[cfg(feature = "dynostore")]
            builder.with_factory(Arc::new(
                nisshi_storage_dynostore::S3OptimisticConcurrencyEngineFactory,
            ));

            #[cfg(feature = "dynostore")]
            builder.with_factory(Arc::new(
                nisshi_storage_dynostore::GoogleCloudStorageEngineFactory,
            ));

            #[cfg(feature = "libsql")]
            builder.with_factory(Arc::new(nisshi_storage_sql::LiteEngineFactory));

            #[cfg(feature = "postgres")]
            builder.with_factory(Arc::new(nisshi_storage_sql::PostgresEngineFactory));

            #[cfg(feature = "slatedb")]
            builder.with_factory(Arc::new(nisshi_storage_slatedb::EngineFactory));

            #[cfg(feature = "turso")]
            builder.with_factory(Arc::new(nisshi_storage_sql::LimboEngineFactory));

            builder
        };

        let storage = builder
            .cluster_id(self.cluster_id.clone())
            .node_id(self.node_id)
            .advertised_listener(self.advertised_listener.clone())
            .schema_registry(self.schema_registry.clone())
            .lake_house(self.lake_house.clone())
            .storage(storage)
            .cancellation(self.cancellation.clone())
            .silent(self.silent)
            .user_credentials_required(self.authentication)
            .build()
            .await
            .map(|storage| Arc::new(storage) as ArcDynStorage)?;

        let groups = Controller::new(storage.clone(), self.group_coordinator);

        let sasl_config = if self.authentication {
            nisshi_auth::configuration(storage.clone()).map(Some)?
        } else {
            None
        };

        Ok(Broker {
            node_id: self.node_id,
            cluster_id: self.cluster_id.clone(),
            incarnation_id: self.incarnation_id,
            listener: self.listener,
            advertised_listener: self.advertised_listener,
            storage,
            groups,
            topic_creation: self.topic_creation,
            auto_create_refusals: AutoCreateRefusals::default(),
            sasl_config,
            tls_server_config: self.tls_server_config.map(Arc::new),

            silent: self.silent,
            banner: self.banner,
            maintenance_interval: intervals.maintenance,
            transaction_maintenance_interval: intervals.transaction_maintenance,
            cancellation: self.cancellation,
            meter_provider,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        address_family_unsupported_os_error, arm, bind, configure_listener, ipv4_fallback,
        listener_addr, preferred_listener_addr,
    };
    use crate::Error;
    use socket2::{Domain, Protocol, Socket, Type};
    use std::{
        assert_matches, io,
        net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    };
    use tokio::net::{TcpListener, TcpStream};
    use url::Url;

    async fn listener_addr_of(listener: &str) -> crate::Result<SocketAddr> {
        listener_addr(&Url::parse(listener).expect("listener URL")).await
    }

    /// The `url` crate parses an IPv4 literal under a non-special scheme such as `tcp` as
    /// `Host::Domain`, so this covers the literal path of the domain arm.
    #[tokio::test]
    async fn ipv4_literal_under_tcp_scheme_keeps_its_address() {
        for (listener, expected) in [
            (
                "tcp://127.0.0.1:9092",
                SocketAddr::from((Ipv4Addr::LOCALHOST, 9092)),
            ),
            (
                "tcp://0.0.0.0:9092",
                SocketAddr::from((Ipv4Addr::UNSPECIFIED, 9092)),
            ),
        ] {
            assert_matches!(listener_addr_of(listener).await, Ok(addr) if addr == expected);
        }
    }

    #[tokio::test]
    async fn ipv6_literal_and_missing_host_keep_their_address() {
        assert_matches!(
            listener_addr_of("tcp://[::1]:9092").await,
            Ok(addr) if addr == SocketAddr::from((Ipv6Addr::LOCALHOST, 9092))
        );
        assert_matches!(
            listener_addr_of("tcp:///").await,
            Ok(addr) if addr == SocketAddr::from((Ipv6Addr::UNSPECIFIED, 9092))
        );
    }

    /// `localhost` resolves to `::1` before `127.0.0.1` on macOS. The listener takes the IPv4
    /// address, as the JDK does by default, so it accepts clients of the default advertised
    /// listener, `tcp://127.0.0.1:9092`.
    #[tokio::test]
    async fn host_name_resolves_instead_of_binding_unspecified() {
        assert_matches!(
            listener_addr_of("tcp://localhost:9092").await,
            Ok(addr) if addr == SocketAddr::from((Ipv4Addr::LOCALHOST, 9092))
        );
    }

    /// RFC 6761 reserves `.invalid`, so the lookup goes through the system resolver and
    /// never succeeds. The trailing dot makes the name fully qualified, so the resolver
    /// does not append a search domain, which a wildcard record could resolve.
    #[tokio::test]
    async fn unresolvable_host_fails_naming_the_host_and_port() {
        assert_matches!(
            listener_addr_of("tcp://nisshi-listener.invalid.:9093").await,
            Err(Error::ListenerHostUnresolved { host, port: 9093, .. })
                if host == "nisshi-listener.invalid."
        );
    }

    #[test]
    fn preferred_listener_addr_takes_the_first_ipv4_address() {
        let v6 = SocketAddr::from((Ipv6Addr::LOCALHOST, 9092));
        let v4 = SocketAddr::from((Ipv4Addr::LOCALHOST, 9092));
        let other_v4 = SocketAddr::from((Ipv4Addr::new(10, 0, 0, 1), 9092));

        assert_eq!(Some(v4), preferred_listener_addr([v6, v4, other_v4]));
        assert_eq!(Some(v6), preferred_listener_addr([v6]));
        assert_eq!(None, preferred_listener_addr([]));
    }

    #[tokio::test]
    async fn binding_an_address_in_use_names_the_address() {
        let first = bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).expect("bind a free port");
        let addr = first.local_addr().expect("local_addr");

        assert_matches!(bind(addr), Err(Error::ListenerInUse { addr: in_use }) if in_use == addr);
    }

    /// The socket starts out IPv6-only, as on a host that defaults new IPv6 sockets to
    /// v6-only, so this test fails on any host if [`arm`] stops clearing `IPV6_V6ONLY`.
    #[tokio::test]
    async fn dual_stack_listener_accepts_v4_and_v6_clients() {
        let unspecified = SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0));

        let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))
            .expect("create IPv6 socket");
        socket.set_only_v6(true).expect("start out IPv6-only");

        let socket = arm(socket, unspecified).expect("arm dual-stack listener");
        assert_eq!(
            Some(false),
            socket.only_v6().ok(),
            "IPV6_V6ONLY must be cleared on the dual-stack listener"
        );

        let port = socket
            .local_addr()
            .expect("local_addr")
            .as_socket()
            .expect("socket address")
            .port();

        let listener =
            TcpListener::from_std(socket.into()).expect("hand the bound socket to tokio");

        _ = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("a dual-stack listener must accept an IPv4 client");

        _ = TcpStream::connect(("::1", port))
            .await
            .expect("a dual-stack listener must accept an IPv6 client");

        drop(listener);
    }

    /// Guards the `if addr.is_ipv6()` gate in [`arm`]: setting `IPV6_V6ONLY` on an IPv4
    /// socket fails (`EINVAL` on macOS, `ENOPROTOOPT` on Linux), so an IPv4-literal bind
    /// must skip that call.
    #[tokio::test]
    async fn ipv4_literal_listener_accepts_v4_clients() {
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));

        let socket = configure_listener(loopback).expect("configure IPv4-literal listener");

        let port = socket
            .local_addr()
            .expect("local_addr")
            .as_socket()
            .expect("socket address")
            .port();

        let listener =
            TcpListener::from_std(socket.into()).expect("hand the bound socket to tokio");

        _ = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("an IPv4-literal listener must accept an IPv4 client");

        drop(listener);
    }

    fn os_error(code: i32) -> io::Error {
        io::Error::from_raw_os_error(code)
    }

    #[test]
    fn ipv6_unspecified_falls_back_to_ipv4_without_ipv6_support() {
        let addr = SocketAddr::from((Ipv6Addr::UNSPECIFIED, 9092));

        assert_eq!(
            Some(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 9092))),
            ipv4_fallback(addr, &os_error(address_family_unsupported_os_error())),
        );
    }

    #[test]
    fn explicit_ipv6_address_does_not_fall_back() {
        let addr = SocketAddr::from((Ipv6Addr::LOCALHOST, 9092));

        assert_eq!(
            None,
            ipv4_fallback(addr, &os_error(address_family_unsupported_os_error()))
        );
    }

    #[test]
    fn other_bind_errors_do_not_fall_back() {
        let addr = SocketAddr::from((Ipv6Addr::UNSPECIFIED, 9092));

        assert_eq!(None, ipv4_fallback(addr, &os_error(libc::EADDRINUSE)));
    }

    #[test]
    fn ipv4_address_does_not_fall_back() {
        let addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, 9092));

        assert_eq!(
            None,
            ipv4_fallback(addr, &os_error(address_family_unsupported_os_error()))
        );
    }
}
