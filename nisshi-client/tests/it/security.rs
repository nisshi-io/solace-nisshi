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

//! The TLS and SASL that a pool applies to its connections, against a fake broker that serves
//! TLS (and asks for a client certificate), or SASL PLAIN, or both.
//!
//! SCRAM against Nisshi's own broker is covered by `nisshi/tests/client_security.rs`, which runs
//! each command-line tool against a SASL_SSL listener.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use nisshi_client::{
    Builder, Client, ConnectionManager, Error, Sasl, SaslMechanism, Security, Tls, TlsOptions,
};
use nisshi_sans_io::{
    ApiKey as _, ApiVersionsRequest, ApiVersionsResponse, Body, ErrorCode, Frame, Header,
    MetadataRequest, MetadataResponse, RootMessageMeta, SaslAuthenticateRequest,
    SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
    api_versions_response::ApiVersion,
};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
};
use rustls::{
    RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    server::WebPkiClientVerifier,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _},
    net::TcpListener,
    task::JoinSet,
    time::{sleep, timeout},
};
use tokio_rustls::TlsAcceptor;
use url::Url;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

const USERNAME: &str = "alice";
const PASSWORD: &str = "correct horse battery staple";

/// A certificate authority, and what it signed.
struct Authority {
    issuer: Issuer<'static, KeyPair>,
    certificate: CertificateDer<'static>,
}

struct Identity {
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

impl Authority {
    fn new() -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![]).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        // Each authority needs a name of its own, or a certificate that another one signed
        // names this one as its issuer, and fails on its signature instead.
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        params.distinguished_name.push(
            DnType::CommonName,
            format!(
                "nisshi test authority {}",
                NEXT.fetch_add(1, Ordering::SeqCst)
            ),
        );

        let certificate = params.self_signed(&key).unwrap().der().clone();

        Self {
            issuer: Issuer::new(params, key),
            certificate,
        }
    }

    /// A certificate for `names`, which `purpose` allows to be used as a server or a client.
    fn sign(&self, names: &[&str], purpose: ExtendedKeyUsagePurpose) -> Identity {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(
            names
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        params.extended_key_usages = vec![purpose];

        Identity {
            chain: vec![params.signed_by(&key, &self.issuer).unwrap().der().clone()],
            key: PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        }
    }

    fn server(&self, names: &[&str]) -> Identity {
        self.sign(names, ExtendedKeyUsagePurpose::ServerAuth)
    }

    fn client(&self) -> Identity {
        self.sign(&["alice"], ExtendedKeyUsagePurpose::ClientAuth)
    }
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

fn server_config(
    server: Identity,
    require_client_certificate_from: Option<&Authority>,
) -> Arc<ServerConfig> {
    let builder = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .unwrap();

    let builder = match require_client_certificate_from {
        Some(authority) => {
            let mut roots = RootCertStore::empty();
            roots.add(authority.certificate.clone()).unwrap();

            builder.with_client_cert_verifier(
                WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider())
                    .build()
                    .unwrap(),
            )
        }
        None => builder.with_no_client_auth(),
    };

    Arc::new(builder.with_single_cert(server.chain, server.key).unwrap())
}

/// How the fake broker handles SASL.
#[derive(Clone, Debug)]
struct SaslPolicy {
    /// The mechanisms that the broker lists in its SaslHandshake responses.
    enabled: Vec<&'static str>,

    /// The SaslAuthenticate token that logs in: PLAIN's.
    token: Vec<u8>,

    session_lifetime_ms: i64,
}

impl SaslPolicy {
    fn plain() -> Self {
        Self {
            enabled: vec!["PLAIN"],
            token: [&[0][..], USERNAME.as_bytes(), &[0], PASSWORD.as_bytes()].concat(),
            session_lifetime_ms: 0,
        }
    }
}

#[derive(Default)]
struct Seen {
    /// The SaslAuthenticate tokens, in the order that they arrived.
    tokens: Mutex<Vec<Bytes>>,
    accepted: AtomicUsize,
    metadata: AtomicUsize,
}

/// A fake broker that answers ApiVersions, SaslHandshake, SaslAuthenticate and Metadata.
struct Broker {
    url: Url,
    seen: Arc<Seen>,
    _tasks: JoinSet<()>,
}

impl Broker {
    async fn start(tls: Option<Arc<ServerConfig>>, sasl: Option<SaslPolicy>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("tcp://{}", listener.local_addr().unwrap())).unwrap();
        let seen = Arc::new(Seen::default());

        let mut tasks = JoinSet::new();
        let accepting = seen.clone();

        _ = tasks.spawn(async move {
            let mut connections = JoinSet::new();

            while let Ok((stream, _)) = listener.accept().await {
                _ = accepting.accepted.fetch_add(1, Ordering::SeqCst);

                let (tls, sasl, seen) = (tls.clone(), sasl.clone(), accepting.clone());

                _ = connections.spawn(async move {
                    _ = match tls {
                        Some(config) => match TlsAcceptor::from(config).accept(stream).await {
                            Ok(mut stream) => serve(&mut stream, sasl, seen).await,
                            // A client that refuses the certificate ends the handshake.
                            Err(_) => Ok(()),
                        },
                        None => {
                            let mut stream = stream;
                            serve(&mut stream, sasl, seen).await
                        }
                    }
                    .ok();
                });
            }
        });

        Self {
            url,
            seen,
            _tasks: tasks,
        }
    }

    fn builder(&self, security: Security) -> Builder {
        ConnectionManager::builder(self.url.clone())
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .security(security)
    }
}

async fn serve<S>(stream: &mut S, sasl: Option<SaslPolicy>, seen: Arc<Seen>) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut authenticated = sasl.is_none();

    loop {
        let mut size = [0u8; 4];
        _ = stream.read_exact(&mut size).await?;

        let mut buffer = vec![0u8; size.len() + u32::from_be_bytes(size) as usize];
        buffer[..size.len()].copy_from_slice(&size);
        _ = stream.read_exact(&mut buffer[size.len()..]).await?;

        let frame = Frame::request_from_bytes(Bytes::from(buffer)).map_err(io::Error::other)?;
        let api_key = frame.api_key().map_err(io::Error::other)?;
        let api_version = frame.api_version().map_err(io::Error::other)?;
        let header = Header::Response {
            correlation_id: frame.correlation_id().map_err(io::Error::other)?,
        };

        let body: Body = match frame.body {
            Body::ApiVersionsRequest(_) => api_versions().into(),

            Body::SaslHandshakeRequest(SaslHandshakeRequest { mechanism, .. }) => match &sasl {
                None => SaslHandshakeResponse::default()
                    .error_code(ErrorCode::IllegalSaslState.into())
                    .mechanisms(Some(vec![]))
                    .into(),

                Some(policy) if policy.enabled.contains(&mechanism.as_str()) => {
                    SaslHandshakeResponse::default()
                        .error_code(ErrorCode::None.into())
                        .mechanisms(Some(policy.enabled.iter().map(|m| (*m).into()).collect()))
                        .into()
                }

                Some(policy) => SaslHandshakeResponse::default()
                    .error_code(ErrorCode::UnsupportedSaslMechanism.into())
                    .mechanisms(Some(policy.enabled.iter().map(|m| (*m).into()).collect()))
                    .into(),
            },

            Body::SaslAuthenticateRequest(SaslAuthenticateRequest { auth_bytes, .. }) => {
                let Some(policy) = &sasl else {
                    return Ok(());
                };

                seen.tokens.lock().unwrap().push(auth_bytes.clone());

                if auth_bytes[..] == policy.token[..] {
                    authenticated = true;

                    SaslAuthenticateResponse::default()
                        .error_code(ErrorCode::None.into())
                        .error_message(None)
                        .auth_bytes(Bytes::new())
                        .session_lifetime_ms(Some(policy.session_lifetime_ms))
                        .into()
                } else {
                    SaslAuthenticateResponse::default()
                        .error_code(ErrorCode::SaslAuthenticationFailed.into())
                        .error_message(Some("Authentication failed: invalid credentials".into()))
                        .auth_bytes(Bytes::new())
                        .session_lifetime_ms(Some(0))
                        .into()
                }
            }

            Body::MetadataRequest(_) => {
                // A broker with SASL closes a connection that sends anything else before it
                // logs in.
                if !authenticated {
                    return Ok(());
                }

                _ = seen.metadata.fetch_add(1, Ordering::SeqCst);

                MetadataResponse::default()
                    .brokers(Some([].into()))
                    .topics(Some([].into()))
                    .cluster_id(Some("fake".into()))
                    .controller_id(Some(111))
                    .throttle_time_ms(Some(0))
                    .cluster_authorized_operations(Some(-1))
                    .into()
            }

            _ => return Ok(()),
        };

        let response =
            Frame::response(header, body, api_key, api_version).map_err(io::Error::other)?;
        stream.write_all(&response).await?;
    }
}

fn api_versions() -> ApiVersionsResponse {
    ApiVersionsResponse::default()
        .error_code(ErrorCode::None.into())
        .api_keys(Some(
            RootMessageMeta::messages()
                .requests()
                .iter()
                .filter(|(api_key, _)| {
                    [
                        ApiVersionsRequest::KEY,
                        MetadataRequest::KEY,
                        SaslHandshakeRequest::KEY,
                        SaslAuthenticateRequest::KEY,
                    ]
                    .contains(api_key)
                })
                .map(|(_, meta)| {
                    ApiVersion::default()
                        .api_key(meta.api_key)
                        .min_version(meta.version.valid.start)
                        .max_version(meta.version.valid.end)
                })
                .collect(),
        ))
        .throttle_time_ms(Some(0))
}

fn metadata_request() -> MetadataRequest {
    MetadataRequest::default()
        .topics(Some([].into()))
        .allow_auto_topic_creation(Some(false))
        .include_cluster_authorized_operations(Some(false))
        .include_topic_authorized_operations(Some(false))
}

/// Builds the pool, and sends one Metadata request through it.
async fn metadata(builder: Builder) -> Result<Option<String>, Error> {
    let client = timeout(TEST_TIMEOUT, builder.build())
        .await
        .expect("the pool builds in time")
        .map(Client::new)?;

    timeout(TEST_TIMEOUT, client.call(metadata_request()))
        .await
        .expect("the request answers in time")
        .map(|response| response.cluster_id)
}

fn tls(authority: &Authority, identity: Option<Identity>) -> Tls {
    Tls::new(TlsOptions {
        ca_certificates: vec![authority.certificate.clone()],
        client_identity: identity.map(|identity| (identity.chain, identity.key)),
        ..TlsOptions::default()
    })
    .unwrap()
}

#[tokio::test]
async fn a_pool_with_tls_trusts_the_ca_that_signed_the_broker() {
    let authority = Authority::new();
    let broker = Broker::start(
        Some(server_config(
            authority.server(&["localhost", "127.0.0.1"]),
            None,
        )),
        None,
    )
    .await;

    let security = Security::default().tls(tls(&authority, None));

    assert_eq!(
        Some("fake".to_owned()),
        metadata(broker.builder(security)).await.unwrap()
    );
}

#[tokio::test]
async fn a_pool_without_tls_cannot_use_a_tls_broker() {
    let authority = Authority::new();
    let broker = Broker::start(
        Some(server_config(authority.server(&["127.0.0.1"]), None)),
        None,
    )
    .await;

    assert!(
        timeout(TEST_TIMEOUT, broker.builder(Security::default()).build())
            .await
            .unwrap()
            .is_err()
    );
}

#[tokio::test]
async fn a_pool_refuses_a_broker_whose_ca_it_does_not_trust() {
    let authority = Authority::new();
    let other = Authority::new();
    let broker = Broker::start(
        Some(server_config(authority.server(&["127.0.0.1"]), None)),
        None,
    )
    .await;

    let error = metadata(broker.builder(Security::default().tls(tls(&other, None))))
        .await
        .unwrap_err();

    assert!(error.to_string().contains("UnknownIssuer"), "{error}");
}

#[tokio::test]
async fn a_pool_refuses_a_certificate_for_another_name_unless_told_not_to_check() {
    let authority = Authority::new();
    let broker = Broker::start(
        Some(server_config(authority.server(&["broker.example"]), None)),
        None,
    )
    .await;

    // The URL has the address, and the certificate has only a name.
    let error = metadata(broker.builder(Security::default().tls(tls(&authority, None))))
        .await
        .unwrap_err();

    assert!(error.to_string().contains("not valid for name"), "{error}");

    let unchecked = Tls::new(TlsOptions {
        ca_certificates: vec![authority.certificate.clone()],
        verify_hostname: false,
        ..TlsOptions::default()
    })
    .unwrap();

    assert_eq!(
        Some("fake".to_owned()),
        metadata(broker.builder(Security::default().tls(unchecked)))
            .await
            .unwrap()
    );

    let named = Tls::new(TlsOptions {
        ca_certificates: vec![authority.certificate.clone()],
        server_name: Some("broker.example".into()),
        ..TlsOptions::default()
    })
    .unwrap();

    assert_eq!(
        Some("fake".to_owned()),
        metadata(broker.builder(Security::default().tls(named)))
            .await
            .unwrap()
    );
}

/// Skipping the name check does not skip the chain check.
#[tokio::test]
async fn a_pool_that_skips_the_name_check_still_checks_the_ca() {
    let authority = Authority::new();
    let other = Authority::new();
    let broker = Broker::start(
        Some(server_config(authority.server(&["broker.example"]), None)),
        None,
    )
    .await;

    let unchecked = Tls::new(TlsOptions {
        ca_certificates: vec![other.certificate.clone()],
        verify_hostname: false,
        ..TlsOptions::default()
    })
    .unwrap();

    let error = metadata(broker.builder(Security::default().tls(unchecked)))
        .await
        .unwrap_err();

    assert!(error.to_string().contains("UnknownIssuer"), "{error}");
}

#[tokio::test]
async fn a_pool_presents_its_client_certificate_to_a_broker_that_asks() {
    let authority = Authority::new();
    let broker = Broker::start(
        Some(server_config(
            authority.server(&["127.0.0.1"]),
            Some(&authority),
        )),
        None,
    )
    .await;

    assert!(
        metadata(broker.builder(Security::default().tls(tls(&authority, None))))
            .await
            .is_err(),
        "the broker accepted a client without a certificate"
    );

    assert_eq!(
        Some("fake".to_owned()),
        metadata(
            broker.builder(Security::default().tls(tls(&authority, Some(authority.client()))))
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn a_pool_logs_in_with_plain_before_its_first_request() {
    let broker = Broker::start(None, Some(SaslPolicy::plain())).await;

    let security = Security::default().sasl(Sasl::new(SaslMechanism::Plain, USERNAME, PASSWORD));

    // The fake broker closes a connection that sends Metadata before it logs in.
    assert_eq!(
        Some("fake".to_owned()),
        metadata(broker.builder(security)).await.unwrap()
    );

    assert_eq!(
        vec![Bytes::from(
            [&[0][..], USERNAME.as_bytes(), &[0], PASSWORD.as_bytes()].concat()
        )],
        broker
            .seen
            .tokens
            .lock()
            .unwrap()
            .iter()
            .take(1)
            .cloned()
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn a_pool_logs_in_over_tls() {
    let authority = Authority::new();
    let broker = Broker::start(
        Some(server_config(authority.server(&["127.0.0.1"]), None)),
        Some(SaslPolicy::plain()),
    )
    .await;

    let security = Security::default()
        .tls(tls(&authority, None))
        .sasl(Sasl::new(SaslMechanism::Plain, USERNAME, PASSWORD));

    assert_eq!(
        Some("fake".to_owned()),
        metadata(broker.builder(security)).await.unwrap()
    );
}

#[tokio::test]
async fn a_refused_login_names_the_reason_and_is_not_a_connect_failure() {
    let broker = Broker::start(None, Some(SaslPolicy::plain())).await;

    let security = Security::default().sasl(Sasl::new(SaslMechanism::Plain, USERNAME, "wrong"));

    let error = metadata(broker.builder(security)).await.unwrap_err();

    assert!(
        error
            .login_failure()
            .is_some_and(|reason| reason.contains("Authentication failed: invalid credentials")),
        "{error}"
    );
    assert!(!error.is_connect_failure(), "{error}");
    assert!(!error.to_string().contains("wrong"), "{error}");

    // The pool does not retry a refused login, which would lock the account of a broker that
    // counts failures.
    assert_eq!(1, broker.seen.tokens.lock().unwrap().len());
}

#[tokio::test]
async fn a_login_with_a_mechanism_the_broker_does_not_enable_lists_the_enabled_ones() {
    let broker = Broker::start(None, Some(SaslPolicy::plain())).await;

    let security =
        Security::default().sasl(Sasl::new(SaslMechanism::ScramSha512, USERNAME, PASSWORD));

    let error = metadata(broker.builder(security)).await.unwrap_err();

    let reason = error.login_failure().unwrap_or_default();
    assert!(reason.contains("SCRAM-SHA-512"), "{reason}");
    assert!(reason.contains("it enables PLAIN"), "{reason}");
}

#[tokio::test]
async fn a_login_to_a_broker_without_sasl_says_so() {
    let broker = Broker::start(None, None).await;

    let security = Security::default().sasl(Sasl::new(SaslMechanism::Plain, USERNAME, PASSWORD));

    let error = metadata(broker.builder(security)).await.unwrap_err();

    assert!(
        error
            .login_failure()
            .is_some_and(|reason| reason.contains("does not use SASL")),
        "{error}"
    );
}

/// The broker closes a connection whose session lifetime has run out, unless the client logs
/// in again. The pool opens a new connection, which logs in, before that.
#[tokio::test]
async fn a_connection_is_replaced_before_its_session_lifetime_ends() {
    let broker = Broker::start(
        None,
        Some(SaslPolicy {
            session_lifetime_ms: 400,
            ..SaslPolicy::plain()
        }),
    )
    .await;

    let security = Security::default().sasl(Sasl::new(SaslMechanism::Plain, USERNAME, PASSWORD));

    let client = broker
        .builder(security)
        .build()
        .await
        .map(Client::new)
        .unwrap();

    drop(client.call(metadata_request()).await.unwrap());
    let before = broker.seen.tokens.lock().unwrap().len();

    sleep(Duration::from_millis(450)).await;

    drop(client.call(metadata_request()).await.unwrap());
    assert_eq!(before + 1, broker.seen.tokens.lock().unwrap().len());
}

#[test]
fn the_password_is_not_in_what_a_pool_prints() {
    let security = Security::default().sasl(Sasl::new(SaslMechanism::Plain, USERNAME, PASSWORD));

    let builder = ConnectionManager::builder(Url::parse("tcp://127.0.0.1:9092").unwrap())
        .security(security.clone());

    for printed in [format!("{security:?}"), format!("{builder:?}")] {
        assert!(!printed.contains(PASSWORD), "{printed}");
        assert!(printed.contains(USERNAME), "{printed}");
    }
}
