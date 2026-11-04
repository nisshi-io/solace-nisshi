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

//! How a [`Builder`][`crate::Builder`] secures the connections that it opens: TLS, and SASL
//! (PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512).
//!
//! The pair is the client side of the `security.protocol` that a Kafka client sets: neither is
//! `PLAINTEXT`, TLS alone is `SSL`, SASL alone is `SASL_PLAINTEXT`, and both are `SASL_SSL`.

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use bytes::{Buf as _, BufMut as _, Bytes, BytesMut};
use nisshi_sans_io::{
    ApiKey as _, Body, ErrorCode, Frame, Header, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest, SaslHandshakeResponse,
};
use opentelemetry::KeyValue;
use rsasl::{
    callback::{Request, SessionCallback, SessionData},
    config::SASLConfig,
    mechanisms::scram::properties::Iterations,
    prelude::{Mechname, SASLClient, SessionError, State},
    property::{AuthId, Password},
};
use rustls::{
    AlertDescription, ClientConfig, DigitallySignedStruct, Error as RustlsError, RootCertStore,
    SignatureScheme,
    client::{
        WebPkiServerVerifier,
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    },
    crypto::{CryptoProvider, aws_lc_rs},
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
use tokio_rustls::{TlsConnector, client::TlsStream};
use url::{Host, Url};
use zeroize::Zeroizing;

use crate::{BytesConnectionService, Error};

/// The version of SaslHandshake that the client sends. Version 1 makes the broker expect
/// SaslAuthenticate requests, where version 0 carries the tokens outside the Kafka protocol.
const HANDSHAKE_VERSION: i16 = 1;

/// The version of SaslAuthenticate that the client sends. Version 1 is the first to answer the
/// session lifetime, and version 2 only makes the header flexible.
const AUTHENTICATE_VERSION: i16 = 1;

/// How the connections of a pool are secured. The default secures nothing, which is a
/// `PLAINTEXT` listener.
#[derive(Clone, Debug, Default)]
pub struct Security {
    tls: Option<Tls>,
    sasl: Option<Sasl>,
}

impl Security {
    /// Connect over TLS, as configured by `tls`.
    pub fn tls(self, tls: Tls) -> Self {
        Self {
            tls: Some(tls),
            ..self
        }
    }

    /// The same security without the SASL login, for the connection that only asks the broker
    /// for its API versions, which a broker answers before a login.
    pub(crate) fn without_sasl(&self) -> Self {
        Self {
            sasl: None,
            ..self.clone()
        }
    }

    /// Log in to each connection with SASL, as configured by `sasl`.
    pub fn sasl(self, sasl: Sasl) -> Self {
        Self {
            sasl: Some(sasl),
            ..self
        }
    }
}

/// The parts that identify a [`Security`]: a TLS configuration by identity, because a rustls
/// configuration can't be compared, and the SASL login.
impl Security {
    fn identity(&self) -> (Option<*const ClientConfig>, Option<&Sasl>) {
        (
            self.tls.as_ref().map(|tls| Arc::as_ptr(&tls.config)),
            self.sasl.as_ref(),
        )
    }
}

impl PartialEq for Security {
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity()
    }
}

impl Eq for Security {}

impl Hash for Security {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.identity().hash(state);
    }
}

impl PartialOrd for Security {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Security {
    fn cmp(&self, other: &Self) -> Ordering {
        self.identity().cmp(&other.identity())
    }
}

/// What [`Tls::new`] builds a TLS configuration from.
#[derive(Debug)]
pub struct TlsOptions {
    /// The certificates that sign the broker's certificate. When empty, the client trusts the
    /// system's certificates, as the broker does for its own outbound connections.
    pub ca_certificates: Vec<CertificateDer<'static>>,

    /// The certificate chain and private key that the client presents to a broker that asks for
    /// them, which is what Kafka calls `ssl.keystore`.
    pub client_identity: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,

    /// The name to check the broker's certificate against, in place of the host of the broker
    /// URL.
    pub server_name: Option<String>,

    /// Whether the broker's certificate must be valid for the server name. Turning this off is
    /// Kafka's empty `ssl.endpoint.identification.algorithm`: the certificate chain is still
    /// checked, but any certificate that the trusted CAs signed is accepted, whichever host it
    /// names.
    pub verify_hostname: bool,
}

impl Default for TlsOptions {
    fn default() -> Self {
        Self {
            ca_certificates: vec![],
            client_identity: None,
            server_name: None,
            verify_hostname: true,
        }
    }
}

/// TLS for the connections of a pool.
#[derive(Clone)]
pub struct Tls {
    config: Arc<ClientConfig>,
    server_name: Option<ServerName<'static>>,
}

impl fmt::Debug for Tls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Tls))
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

fn provider() -> Arc<CryptoProvider> {
    // Both `ring` and `aws-lc-rs` are compiled into the binary through other dependencies, so
    // the provider must be chosen explicitly: rustls panics when asked to pick a default
    // between two.
    Arc::new(aws_lc_rs::default_provider())
}

impl Tls {
    /// Build a TLS configuration, failing when a certificate or the private key can't be used.
    pub fn new(options: TlsOptions) -> Result<Self, Error> {
        let TlsOptions {
            ca_certificates,
            client_identity,
            server_name,
            verify_hostname,
        } = options;

        let mut roots = RootCertStore::empty();

        if ca_certificates.is_empty() {
            let native = rustls_native_certs::load_native_certs();

            for error in &native.errors {
                tracing::debug!(%error, "system certificate not loaded");
            }

            let (added, ignored) = roots.add_parsable_certificates(native.certs);

            tracing::debug!(added, ignored, "system certificates");
        } else {
            for certificate in ca_certificates {
                roots.add(certificate).map_err(Error::tls)?;
            }
        }

        let verifier = WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider())
            .build()
            .map_err(Error::tls)?;

        let builder = ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(Error::tls)?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(Verifier {
                inner: verifier,
                verify_hostname,
            }));

        let config = match client_identity {
            Some((chain, key)) => builder
                .with_client_auth_cert(chain, key)
                .map_err(Error::tls)?,
            None => builder.with_no_client_auth(),
        };

        let server_name = server_name
            .map(|name| {
                ServerName::try_from(name.clone()).map_err(|_invalid| {
                    Error::TlsConfiguration(format!("{name:?} is not a valid server name"))
                })
            })
            .transpose()?;

        Ok(Self {
            config: Arc::new(config),
            server_name,
        })
    }

    /// The name that the broker's certificate must be valid for: the configured server name, or
    /// else the host of the broker URL.
    fn server_name(&self, broker: &Url) -> Result<ServerName<'static>, Error> {
        if let Some(server_name) = &self.server_name {
            return Ok(server_name.clone());
        }

        match broker.host() {
            Some(Host::Domain(domain)) => ServerName::try_from(domain.to_owned())
                .map_err(|_invalid| Error::Tls(format!("{domain:?} is not a valid server name"))),
            Some(Host::Ipv4(address)) => Ok(ServerName::IpAddress(address.into())),
            Some(Host::Ipv6(address)) => Ok(ServerName::IpAddress(address.into())),
            None => Err(Error::UnknownHost(broker.clone())),
        }
    }

    pub(crate) async fn connect(&self, broker: &Url, stream: TcpStream) -> Result<Stream, Error> {
        let server_name = self.server_name(broker)?;

        TlsConnector::from(self.config.clone())
            .connect(server_name, stream)
            .await
            .map(|stream| Stream::Tls(Box::new(stream)))
            .map_err(handshake_failure)
    }
}

/// Returns the error that rustls reports through `error`, when it does.
///
/// A broker that asks for a client certificate and gets none sends its `CertificateRequired`
/// alert after the client finishes a TLS 1.3 handshake, so it reaches the client on its first
/// read, not in the handshake.
pub(crate) fn rustls_cause(error: &io::Error) -> Option<&RustlsError> {
    error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<RustlsError>())
}

/// Why the broker rejected a TLS connection: the alert that it sent.
///
/// Marked `non_exhaustive`: a later release may give another alert its own variant.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TlsRejection {
    /// The broker asks for a client certificate, and the client has none.
    CertificateRequired,

    /// The broker does not accept the client's certificate. The alert's name says why: for
    /// example, `UnknownCA` means that the broker does not trust the authority that signed it.
    ClientCertificate(String),

    /// The broker found nothing to agree on with the client, and the alert's name says which:
    /// `HandshakeFailure` is no shared cipher suite, and also what a Java broker negotiating
    /// TLS 1.2 sends to a client that gave no certificate when it requires one (a TLS 1.3
    /// broker sends `CertificateRequired`). `ProtocolVersion` and `InsufficientSecurity` mean no
    /// shared version or cipher suite.
    Negotiation(String),

    /// Any other alert, by name.
    Other(String),
}

impl TlsRejection {
    /// Returns the rejection that `cause` reports, when it is an alert from the broker.
    pub(crate) fn from_rustls(cause: &RustlsError) -> Option<Self> {
        let RustlsError::AlertReceived(alert) = cause else {
            return None;
        };

        Some(match alert {
            AlertDescription::CertificateRequired => Self::CertificateRequired,

            AlertDescription::BadCertificate
            | AlertDescription::UnsupportedCertificate
            | AlertDescription::CertificateRevoked
            | AlertDescription::CertificateExpired
            | AlertDescription::CertificateUnknown
            | AlertDescription::UnknownCA
            | AlertDescription::AccessDenied => Self::ClientCertificate(format!("{alert:?}")),

            AlertDescription::HandshakeFailure
            | AlertDescription::ProtocolVersion
            | AlertDescription::InsufficientSecurity => Self::Negotiation(format!("{alert:?}")),

            _ => Self::Other(format!("{alert:?}")),
        })
    }
}

/// The error of a failed TLS handshake. An alert from the broker is a rejection, and a
/// certificate or protocol error that rustls reports, or a peer that closes the connection
/// during the handshake, as a listener that does not speak TLS does, is a TLS failure. Anything
/// else is an I/O error.
fn handshake_failure(error: io::Error) -> Error {
    if let Some(cause) = rustls_cause(&error) {
        return TlsRejection::from_rustls(cause)
            .map_or_else(|| Error::Tls(cause.to_string()), Error::TlsRejected);
    }

    match error.kind() {
        io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset => Error::Tls(format!(
            "the broker closed the connection during the TLS handshake ({error}): is its listener TLS?"
        )),
        _ => error.into(),
    }
}

/// Checks the broker's certificate chain as rustls does, and its name unless told not to.
#[derive(Debug)]
struct Verifier {
    inner: Arc<WebPkiServerVerifier>,
    verify_hostname: bool,
}

impl ServerCertVerifier for Verifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            // webpki checks the chain before the name, so a name error means that the chain
            // is valid.
            Err(RustlsError::InvalidCertificate(
                rustls::CertificateError::NotValidForName
                | rustls::CertificateError::NotValidForNameContext { .. },
            )) if !self.verify_hostname => Ok(ServerCertVerified::assertion()),
            result => result,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

/// The SASL mechanisms that the client can log in with.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    #[default]
    ScramSha512,
}

impl SaslMechanism {
    /// The name that the SaslHandshake request carries.
    pub fn name(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

/// A SASL login for each connection of a pool.
#[derive(Clone)]
pub struct Sasl {
    mechanism: SaslMechanism,
    username: String,
    password: Zeroizing<String>,
}

impl Sasl {
    pub fn new(
        mechanism: SaslMechanism,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            mechanism,
            username: username.into(),
            password: Zeroizing::new(password.into()),
        }
    }
}

impl Sasl {
    fn key(&self) -> (SaslMechanism, &str, &str) {
        (self.mechanism, &self.username, &self.password)
    }
}

impl PartialEq for Sasl {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for Sasl {}

impl Hash for Sasl {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key().hash(state);
    }
}

impl PartialOrd for Sasl {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Sasl {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

/// Leaves the password out, because a pool, a request and a manager print their fields at debug
/// level.
impl fmt::Debug for Sasl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Sasl))
            .field("mechanism", &self.mechanism)
            .field("username", &self.username)
            .finish_non_exhaustive()
    }
}

/// The fewest PBKDF2 iterations that a SCRAM login accepts from the broker: the minimum that
/// [Kafka's client enforces][Kafka's check] for SCRAM-SHA-256 and SCRAM-SHA-512. A smaller count lets whoever answers for the broker
/// recover the password from the client's proof with little work, and a SASL_PLAINTEXT
/// connection lets an attacker on the network answer for the broker.
///
/// [Kafka's check]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/security/scram/internals/ScramSaslClient.java#L127-L128
const SCRAM_MIN_ITERATIONS: u32 = 4096;

/// The most PBKDF2 iterations that a SCRAM login accepts from the broker. The client derives the
/// key on the tokio worker that runs the login, and the pool's create timeout can't interrupt
/// it, so a count that the broker's answer sets without limit would hold the worker. Kafka's
/// brokers set at most 16384, and Kafka's client has no maximum, so this is four times what a
/// Kafka broker asks for.
const SCRAM_MAX_ITERATIONS: u32 = 1 << 16;

/// The iteration count that the broker asked for is outside the range that the client accepts.
#[derive(Debug, thiserror::Error)]
#[error("the broker asked for {0} SCRAM iterations")]
struct ScramIterationsOutOfRange(u32);

/// Returns the error for a failed step of a SCRAM login. An iteration count outside the range
/// that the client accepts is the client's refusal, and not the broker's, so it has an error of
/// its own.
fn scram_step_error(error: SessionError) -> Error {
    if let SessionError::Boxed(cause) = &error
        && let Some(&ScramIterationsOutOfRange(iterations)) = cause.downcast_ref()
    {
        return Error::ScramIterations {
            iterations,
            accepted: SCRAM_MIN_ITERATIONS..=SCRAM_MAX_ITERATIONS,
        };
    }

    Error::Authentication(error.to_string())
}

/// Gives a SCRAM login its credentials, and refuses an iteration count outside the range that the
/// client accepts before the client derives a key from the password.
struct ScramCredentials {
    username: String,
    password: Zeroizing<String>,
}

impl SessionCallback for ScramCredentials {
    fn callback(
        &self,
        _session_data: &SessionData,
        context: &rsasl::callback::Context<'_>,
        request: &mut Request<'_>,
    ) -> Result<(), SessionError> {
        if let Some(&iterations) = context.get_ref::<Iterations>()
            && !(SCRAM_MIN_ITERATIONS..=SCRAM_MAX_ITERATIONS).contains(&iterations)
        {
            return Err(SessionError::Boxed(Box::new(ScramIterationsOutOfRange(
                iterations,
            ))));
        }

        _ = request
            .satisfy::<AuthId>(self.username.as_str())?
            .satisfy::<Password>(self.password.as_bytes())?;

        Ok(())
    }
}

/// What a login tells the pool about the connection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Session {
    /// The milliseconds after which the broker accepts only a re-authentication on this
    /// connection, or none when the broker sets no limit.
    pub(crate) lifetime_ms: Option<u64>,
}

impl Session {
    /// When the connection must stop carrying requests, if `now` is when it logged in. That is
    /// at 85% of the lifetime, as the Java client re-authenticates at 85 to 95% of it.
    pub(crate) fn expires(self, now: Instant) -> Option<Instant> {
        self.lifetime_ms
            .map(|lifetime| Duration::from_millis(lifetime).mul_f64(0.85))
            .and_then(|lifetime| now.checked_add(lifetime))
    }
}

impl Sasl {
    /// Logs in to `stream`, which has sent `correlation_id` requests, so that it carries the
    /// first request that the caller sends.
    pub(crate) async fn login(
        &self,
        stream: &mut Stream,
        client_id: Option<&str>,
        correlation_id: &mut i32,
    ) -> Result<Session, Error> {
        let attributes = [KeyValue::new("mechanism", self.mechanism.name())];
        let exchange = Exchange {
            stream,
            client_id,
            correlation_id,
            attributes: &attributes,
        };

        exchange.run(self).await
    }
}

struct Exchange<'a> {
    stream: &'a mut Stream,
    client_id: Option<&'a str>,
    correlation_id: &'a mut i32,
    attributes: &'a [KeyValue],
}

impl Exchange<'_> {
    async fn call(&mut self, api_key: i16, api_version: i16, body: Body) -> Result<Frame, Error> {
        let request = Frame::request(
            Header::Request {
                api_key,
                api_version,
                correlation_id: *self.correlation_id,
                client_id: self.client_id.map(str::to_owned),
            },
            body,
        )?;

        *self.correlation_id += 1;

        BytesConnectionService
            .write(self.stream, request, self.attributes)
            .await?;

        let response = BytesConnectionService
            .read(self.stream, self.attributes)
            .await?;

        Frame::response_from_bytes(response, api_key, api_version).map_err(Into::into)
    }

    async fn run(mut self, sasl: &Sasl) -> Result<Session, Error> {
        let handshake = self
            .call(
                SaslHandshakeRequest::KEY,
                HANDSHAKE_VERSION,
                SaslHandshakeRequest::default()
                    .mechanism(sasl.mechanism.name().into())
                    .into(),
            )
            .await
            .and_then(|frame| SaslHandshakeResponse::try_from(frame.body).map_err(Into::into))?;

        match ErrorCode::try_from(handshake.error_code) {
            Ok(ErrorCode::None) => {}
            Ok(code @ ErrorCode::UnsupportedSaslMechanism) => {
                return Err(Error::Authentication(format!(
                    "the broker does not enable {} ({code}), it enables {}",
                    sasl.mechanism.name(),
                    handshake.mechanisms.unwrap_or_default().join(", ")
                )));
            }
            Ok(ErrorCode::IllegalSaslState) => return Err(Error::SaslNotUsed),
            Ok(code) => return Err(Error::Authentication(format!("SaslHandshake: {code}"))),
            Err(_) => {
                return Err(Error::Authentication(format!(
                    "SaslHandshake: error code {}",
                    handshake.error_code
                )));
            }
        }

        match sasl.mechanism {
            SaslMechanism::Plain => {
                // RFC 4616: an empty authorization identity, the user name and the password,
                // each after a NUL.
                let mut token = Zeroizing::new(Vec::with_capacity(
                    2 + sasl.username.len() + sasl.password.len(),
                ));
                token.push(0);
                token.extend_from_slice(sasl.username.as_bytes());
                token.push(0);
                token.extend_from_slice(sasl.password.as_bytes());

                self.authenticate(Bytes::copy_from_slice(&token))
                    .await
                    .map(|(_, session)| session)
            }

            SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512 => self.scram(sasl).await,
        }
    }

    /// Sends one SaslAuthenticate request, returning the bytes that the broker answers with and
    /// what the login says about the connection.
    async fn authenticate(&mut self, auth_bytes: Bytes) -> Result<(Bytes, Session), Error> {
        let response = self
            .call(
                SaslAuthenticateRequest::KEY,
                AUTHENTICATE_VERSION,
                SaslAuthenticateRequest::default()
                    .auth_bytes(auth_bytes)
                    .into(),
            )
            .await
            .and_then(|frame| SaslAuthenticateResponse::try_from(frame.body).map_err(Into::into))?;

        match ErrorCode::try_from(response.error_code) {
            Ok(ErrorCode::None) => Ok((
                response.auth_bytes,
                Session {
                    lifetime_ms: response
                        .session_lifetime_ms
                        .and_then(|lifetime| u64::try_from(lifetime).ok())
                        .filter(|lifetime| *lifetime > 0),
                },
            )),

            Ok(code) => Err(Error::Authentication(match response.error_message {
                Some(message) if !message.is_empty() => format!("{code}: {message}"),
                _ => code.to_string(),
            })),

            Err(_) => Err(Error::Authentication(format!(
                "SaslAuthenticate: error code {}",
                response.error_code
            ))),
        }
    }

    async fn scram(&mut self, sasl: &Sasl) -> Result<Session, Error> {
        let mechanism = Mechname::parse(sasl.mechanism.name().as_bytes())
            .map_err(|error| Error::Authentication(error.to_string()))?;

        let config = SASLConfig::builder()
            .with_default_mechanisms()
            .with_callback(ScramCredentials {
                username: sasl.username.clone(),
                password: sasl.password.clone(),
            })
            .map_err(|error| Error::Authentication(error.to_string()))?;

        let mut session = SASLClient::new(config)
            .start_suggested(&[mechanism])
            .map_err(|error| Error::Authentication(error.to_string()))?;

        let mut input: Option<Bytes> = None;
        let mut outcome = Session::default();

        loop {
            let mut output = BytesMut::new().writer();

            let state = session
                .step(input.as_deref(), &mut output)
                .map_err(scram_step_error)?;

            let output = output.into_inner().freeze();

            match state {
                State::Running => {
                    let (received, session) = self.authenticate(output).await?;
                    outcome = session;
                    input = Some(received);
                }

                State::Finished(_) => {
                    // SCRAM ends with the broker's message, which the step above checked, so
                    // the client has nothing more to send.
                    if output.has_remaining() {
                        outcome = self.authenticate(output).await?.1;
                    }

                    return Ok(outcome);
                }
            }
        }
    }
}

/// A broker connection, with TLS or without.
#[derive(Debug)]
pub(crate) enum Stream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl Stream {
    /// The TCP connection under the stream.
    pub(crate) fn tcp(&self) -> &TcpStream {
        match self {
            Self::Plain(stream) => stream,
            Self::Tls(stream) => stream.get_ref().0,
        }
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
            Self::Tls(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain(stream) => stream.is_write_vectored(),
            Self::Tls(stream) => stream.is_write_vectored(),
        }
    }
}

impl Security {
    pub(crate) fn tls_config(&self) -> Option<&Tls> {
        self.tls.as_ref()
    }

    pub(crate) fn sasl_config(&self) -> Option<&Sasl> {
        self.sasl.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alert_during_handshake(alert: AlertDescription) -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            RustlsError::AlertReceived(alert),
        )
    }

    /// A Java broker sends `HandshakeFailure` on TLS 1.2 when it requires a client certificate
    /// and gets none, and when it shares no cipher suite with the client.
    #[test]
    fn handshake_failure_alert_during_the_handshake_is_a_negotiation_rejection() {
        for alert in [
            AlertDescription::HandshakeFailure,
            AlertDescription::ProtocolVersion,
            AlertDescription::InsufficientSecurity,
        ] {
            let error = handshake_failure(alert_during_handshake(alert));

            assert_eq!(
                Some(&TlsRejection::Negotiation(format!("{alert:?}"))),
                error.tls_rejection(),
                "{error}"
            );
        }
    }

    #[test]
    fn alert_without_a_cause_of_its_own_is_other() {
        let error = handshake_failure(alert_during_handshake(AlertDescription::InternalError));

        assert_eq!(
            Some(&TlsRejection::Other("InternalError".to_owned())),
            error.tls_rejection(),
            "{error}"
        );
    }
}
