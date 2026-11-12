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

//! Transport level tests for the broker listener: with a TLS configuration
//! every accepted connection must complete a TLS handshake before any Kafka
//! frame is read, and without one the listener stays plain TCP.
//!
//! TLS is independent of the storage backend, so only the memory backend is used.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context as _, Result};
use nisshi_broker::NODE_ID;
use nisshi_sans_io::{
    ApiKey as _, ApiVersionsRequest, ApiVersionsResponse, MetadataRequest, MetadataResponse,
};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject as _},
};
use tempfile::TempDir;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::timeout,
};
use tokio_rustls::TlsConnector;

use crate::common::{
    init_tracing,
    wire::{round_trip, spawn_broker},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct CertifiedKey {
    _dir: TempDir,
    pub(crate) cert: PathBuf,
    pub(crate) key: PathBuf,
}

/// A self-signed certificate for `localhost`, written as PEM into a temporary directory.
pub(crate) fn certified_key() -> Result<CertifiedKey> {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(["localhost".to_owned(), "127.0.0.1".to_owned()])?;

    let dir = tempfile::tempdir()?;
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");

    std::fs::write(&cert_path, cert.pem())?;
    std::fs::write(&key_path, signing_key.serialize_pem())?;

    Ok(CertifiedKey {
        _dir: dir,
        cert: cert_path,
        key: key_path,
    })
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

pub(crate) fn server_config_from_pem(cert: &Path, key: &Path) -> Result<ServerConfig> {
    let certs = CertificateDer::pem_file_iter(cert)?.collect::<Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file(key)?;

    ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(Into::into)
}

pub(crate) fn client_config_trusting(cert: &Path) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();

    for cert in CertificateDer::pem_file_iter(cert)? {
        roots.add(cert?)?;
    }

    ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map(|config| config.with_root_certificates(roots))
        .map(|config| config.with_no_client_auth())
        .map_err(Into::into)
}

async fn api_versions<S>(stream: &mut S) -> Result<ApiVersionsResponse>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let frame = round_trip(
        stream,
        ApiVersionsRequest::KEY,
        3,
        1,
        ApiVersionsRequest::default()
            .client_software_name(Some(env!("CARGO_PKG_NAME").into()))
            .client_software_version(Some(env!("CARGO_PKG_VERSION").into()))
            .into(),
    )
    .await?;

    ApiVersionsResponse::try_from(frame.body).map_err(Into::into)
}

async fn metadata<S>(stream: &mut S) -> Result<MetadataResponse>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let frame = round_trip(
        stream,
        MetadataRequest::KEY,
        12,
        2,
        MetadataRequest::default()
            .topics(Some([].into()))
            .allow_auto_topic_creation(Some(false))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false))
            .into(),
    )
    .await?;

    MetadataResponse::try_from(frame.body).map_err(Into::into)
}

/// Without a TLS configuration the listener keeps speaking plain TCP.
#[tokio::test]
async fn plaintext_listener_without_tls() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(None).await?;

        let mut stream = TcpStream::connect(broker.addr).await?;
        let response = api_versions(&mut stream).await?;

        assert!(
            !response.api_keys.unwrap_or_default().is_empty(),
            "expected a non-empty api_keys list over plaintext"
        );

        Ok(())
    })
    .await?
}

/// With a TLS configuration a TLS client completes the handshake and Kafka
/// requests are served over the encrypted stream.
#[tokio::test]
async fn tls_handshake_and_kafka_request() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let certified = certified_key()?;
        let server = server_config_from_pem(&certified.cert, &certified.key)?;
        let broker = spawn_broker(Some(server)).await?;

        let connector = TlsConnector::from(Arc::new(client_config_trusting(&certified.cert)?));
        let tcp = TcpStream::connect(broker.addr).await?;
        let mut tls = connector
            .connect(ServerName::try_from("localhost")?, tcp)
            .await
            .context("tls handshake with the broker failed")?;

        let response = api_versions(&mut tls).await?;
        assert!(
            !response.api_keys.unwrap_or_default().is_empty(),
            "expected a non-empty api_keys list over tls"
        );

        let response = metadata(&mut tls).await?;
        assert_eq!(Some(NODE_ID), response.controller_id);
        assert_eq!(
            1,
            response.brokers.unwrap_or_default().len(),
            "expected the single broker in metadata over tls"
        );

        Ok(())
    })
    .await?
}

/// With a TLS configuration a plaintext client must not get a Kafka response:
/// the listener is TLS only, so the handshake fails and the connection closes.
#[tokio::test]
async fn plaintext_rejected_on_tls_listener() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let certified = certified_key()?;
        let server = server_config_from_pem(&certified.cert, &certified.key)?;
        let broker = spawn_broker(Some(server)).await?;

        let mut stream = TcpStream::connect(broker.addr).await?;
        let outcome = api_versions(&mut stream).await;

        assert!(
            outcome.is_err(),
            "plaintext request on a tls listener must fail, got {outcome:?}"
        );

        // The rejected connection must not have taken the listener with it:
        // a proper TLS client is still served afterwards.
        let connector = TlsConnector::from(Arc::new(client_config_trusting(&certified.cert)?));
        let tcp = TcpStream::connect(broker.addr).await?;
        let mut tls = connector
            .connect(ServerName::try_from("localhost")?, tcp)
            .await
            .context("tls handshake after a rejected plaintext peer failed")?;

        let response = api_versions(&mut tls).await?;
        assert!(!response.api_keys.unwrap_or_default().is_empty());

        Ok(())
    })
    .await?
}
