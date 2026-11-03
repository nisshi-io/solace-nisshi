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
    env,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::{EnvVarExp, Error, Result, cli::storage_engines, insecure_tls};

use super::DEFAULT_BROKER;
use clap::{ArgAction, Parser};
use nisshi_broker::{
    NODE_ID,
    broker::Broker,
    coordinator::group::administrator::{Controller, GroupCoordinatorConfig},
};
use nisshi_sans_io::ErrorCode;
use nisshi_schema::{Registry, redact_url};
use nisshi_storage::{ArcDynStorage, DEFAULT_NUM_PARTITIONS, TopicCreationConfig};
use owo_colors::{OwoColorize as _, Stream, Style};
use rustls::{
    ServerConfig,
    pki_types::{
        CertificateDer, PrivateKeyDer,
        pem::{Error as TlsPkiPemError, PemObject as _},
    },
};
use tokio::time::Instant;
use tracing::{debug, warn};
use url::Url;
use uuid::Uuid;
use zeroize::Zeroizing;

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use clap::Subcommand;

/// Parses a duration that is longer than zero. A held `JoinGroup` reads storage once per poll
/// interval, so a zero interval would read storage in a loop.
fn positive_duration(value: &str) -> std::result::Result<Duration, String> {
    match humantime::parse_duration(value) {
        Ok(duration) if duration.is_zero() => Err("must be longer than zero".into()),
        Ok(duration) => Ok(duration),
        Err(error) => Err(error.to_string()),
    }
}

#[derive(Clone, Debug, Parser)]
pub(super) struct Arg {
    #[command(subcommand)]
    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    command: Option<Lake>,

    /// All members of the same cluster should use the same id
    #[arg(
        long,
        env = "CLUSTER_ID",
        default_value = "nisshi_cluster",
        visible_alias = "kafka-cluster-id"
    )]
    cluster_id: String,

    /// The broker will listen on this address
    //
    // `[::]`, not `0.0.0.0`. This default requires the broker to bind an IPv6 address
    // with `IPV6_V6ONLY` cleared, so that it also accepts IPv4 connections.
    #[arg(
        long,
        env = "LISTENER_URL",
        default_value = "tcp://[::]:9092",
        visible_alias = "kafka-listener-url"
    )]
    listener_url: EnvVarExp<Url>,

    /// This location is advertised to clients in metadata
    #[arg(
        long,
        env = "ADVERTISED_LISTENER_URL",
        default_value = DEFAULT_BROKER,
        visible_alias = "kafka-advertised-listener-url"
    )]
    advertised_listener_url: EnvVarExp<Url>,

    /// Storage engine examples are: postgres://postgres:postgres@localhost, memory://nisshi/ or s3://nisshi/
    #[arg(long, env = "STORAGE_ENGINE", default_value = "memory://nisshi/")]
    storage_engine: EnvVarExp<Url>,

    /// Schema registry examples are: file://./etc/schema or s3://nisshi/, containing: topic.json, topic.proto or topic.avsc
    #[arg(long, env = "SCHEMA_REGISTRY", global = true)]
    schema_registry: Option<EnvVarExp<Url>>,

    /// Schema registry cache expiry duration
    #[arg(long,value_parser = humantime::parse_duration)]
    schema_registry_cache_expiry: Option<Duration>,

    /// How long the first rebalance of an empty consumer group waits for more members, as Kafka's group.initial.rebalance.delay.ms
    #[arg(long, env = "GROUP_INITIAL_REBALANCE_DELAY", default_value = "3s", value_parser = humantime::parse_duration)]
    group_initial_rebalance_delay: Duration,

    /// How often a held JoinGroup or SyncGroup reads its group from storage, to see changes by other brokers
    #[arg(long, env = "GROUP_POLL_INTERVAL", default_value = "500ms", value_parser = positive_duration)]
    group_poll_interval: Duration,

    /// Whether Metadata creates a topic that it does not know, as Kafka's auto.create.topics.enable
    #[arg(long, env = "AUTO_CREATE_TOPICS_ENABLE", default_value_t = true, action = ArgAction::Set)]
    auto_create_topics_enable: bool,

    /// The partitions of an auto-created topic, or of one created with -1 partitions, as Kafka's num.partitions
    #[arg(long, env = "NUM_PARTITIONS", default_value_t = DEFAULT_NUM_PARTITIONS, value_parser = clap::value_parser!(i32).range(1..))]
    num_partitions: i32,

    /// OTEL Exporter OTLP endpoint
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    otlp_endpoint_url: Option<EnvVarExp<Url>>,

    /// When present, client authentication is required
    #[arg(long)]
    authentication: bool,

    /// Transport Layer Security certificate chain (PEM), requires --key.
    /// When present the listener only accepts TLS connections
    #[arg(long, requires = "key")]
    cert: Option<PathBuf>,

    /// Transport Layer Security private key (PKCS#8, SEC1 or RSA PEM; encrypted PKCS#8 with --key-passphrase-file), requires --cert
    #[arg(long, requires = "cert")]
    key: Option<PathBuf>,

    /// File containing the passphrase of an encrypted PKCS#8 private key (trailing newline ignored), requires --key
    #[arg(long, requires = "key")]
    key_passphrase_file: Option<PathBuf>,

    /// Silent
    #[arg(long)]
    silent: bool,
}

fn load_certs(filename: &Path) -> Result<Vec<CertificateDer<'static>>> {
    CertificateDer::pem_file_iter(filename)
        .and_then(|der| der.collect::<Result<Vec<_>, TlsPkiPemError>>())
        .and_then(|certs| {
            // A file with no certificate blocks (garbage, or the key file by
            // mistake) iterates to an empty chain rather than an error.
            if certs.is_empty() {
                Err(TlsPkiPemError::NoItemsFound)
            } else {
                Ok(certs)
            }
        })
        .map_err(|source| Error::TlsCertificate {
            path: filename.to_path_buf(),
            source,
        })
}

/// Reads the passphrase file, if any. A trailing newline (as left by most
/// editors and `echo`) is not part of the passphrase. An empty file means
/// "no passphrase", matching how OpenSSL reads a `file:` password source and
/// how mounted secrets behave.
fn load_passphrase(filename: Option<&Path>) -> Result<Option<Zeroizing<Vec<u8>>>> {
    let Some(filename) = filename else {
        return Ok(None);
    };

    let mut passphrase =
        fs::read(filename)
            .map(Zeroizing::new)
            .map_err(|source| Error::TlsKeyPassphraseFile {
                path: filename.to_path_buf(),
                source,
            })?;

    while passphrase
        .last()
        .is_some_and(|b| *b == b'\n' || *b == b'\r')
    {
        _ = passphrase.pop();
    }

    Ok((!passphrase.is_empty()).then_some(passphrase))
}

const ENCRYPTED_PKCS8_BEGIN: &str = "-----BEGIN ENCRYPTED PRIVATE KEY-----";
const ENCRYPTED_PKCS8_END: &str = "-----END ENCRYPTED PRIVATE KEY-----";

/// The encrypted PKCS#8 section of `pem`, which may be a certificate and key
/// bundle, from its BEGIN line through its END line.
fn encrypted_pkcs8_section(pem: &str) -> Option<&str> {
    let start = pem.find(ENCRYPTED_PKCS8_BEGIN)?;
    let end = pem[start..].find(ENCRYPTED_PKCS8_END)? + start + ENCRYPTED_PKCS8_END.len();
    Some(&pem[start..end])
}

fn decrypt_pkcs8(section: &str, passphrase: &[u8]) -> pkcs8::Result<PrivateKeyDer<'static>> {
    use pkcs8::{EncryptedPrivateKeyInfo, SecretDocument, der::pem::PemLabel as _};

    let (label, encrypted) = SecretDocument::from_pem(section)?;
    EncryptedPrivateKeyInfo::validate_pem_label(label)?;

    let decrypted = EncryptedPrivateKeyInfo::try_from(encrypted.as_bytes())?.decrypt(passphrase)?;

    // `decrypted` is zeroized on drop; the copy handed to rustls is not,
    // because `PrivatePkcs8KeyDer` has no zeroize-on-drop. The unencrypted
    // path (`from_pem_slice`) has the same property.
    Ok(PrivateKeyDer::Pkcs8(decrypted.as_bytes().to_vec().into()))
}

/// The name of the encryption algorithm a decrypt error says this build does
/// not support, if that is what it says. A wrong passphrase is reported
/// differently, so the operator is not sent to retype a passphrase when the
/// key needs re-encrypting.
fn unsupported_encryption(error: &pkcs8::Error) -> Option<String> {
    use pkcs8::pkcs5::Error as Pkcs5Error;

    const HMAC_WITH_SHA1: &str = "1.2.840.113549.2.7";
    const DES_CBC: &str = "1.3.14.3.2.7";

    match error {
        pkcs8::Error::EncryptedPrivateKey(Pkcs5Error::UnsupportedAlgorithm { oid }) => {
            Some(match oid.to_string().as_str() {
                HMAC_WITH_SHA1 => "PBKDF2 with HMAC-SHA1".to_owned(),
                DES_CBC => "DES-CBC".to_owned(),
                other => format!("algorithm {other}"),
            })
        }
        pkcs8::Error::EncryptedPrivateKey(Pkcs5Error::NoPbes1CryptSupport) => {
            Some("PBES1".to_owned())
        }
        _ => None,
    }
}

fn load_private_key(filename: &Path, passphrase: Option<&[u8]>) -> Result<PrivateKeyDer<'static>> {
    let key_error = |source| Error::TlsPrivateKey {
        path: filename.to_path_buf(),
        source,
    };

    let pem = fs::read_to_string(filename)
        .map(Zeroizing::new)
        .map_err(TlsPkiPemError::Io)
        .map_err(key_error)?;

    // Legacy OpenSSL PEM encryption (`Proc-Type: 4,ENCRYPTED` on an RSA/EC
    // key) derives its key with a single round of MD5: not supported, and
    // `openssl pkcs8 -topk8` converts it in place, so say so rather than
    // letting rustls report "no private key found".
    if pem.contains("Proc-Type: 4,ENCRYPTED") {
        return Err(Error::TlsKeyLegacyEncrypted {
            path: filename.to_path_buf(),
        });
    }

    if let Some(section) = encrypted_pkcs8_section(&pem) {
        let Some(passphrase) = passphrase else {
            return Err(Error::TlsKeyPassphraseRequired {
                path: filename.to_path_buf(),
            });
        };

        // A wrong passphrase may still decrypt to well-padded garbage; the
        // certificate and key are compared by `with_single_cert` afterwards.
        return decrypt_pkcs8(section, passphrase).map_err(|source| {
            unsupported_encryption(&source).map_or_else(
                || Error::TlsKeyDecrypt {
                    path: filename.to_path_buf(),
                    source,
                },
                |algorithm| Error::TlsKeyUnsupportedEncryption {
                    path: filename.to_path_buf(),
                    algorithm,
                },
            )
        });
    }

    if passphrase.is_some() {
        warn!(
            "TLS private key {} is not encrypted; --key-passphrase-file ignored",
            filename.display()
        );
    }

    PrivateKeyDer::from_pem_slice(pem.as_bytes()).map_err(key_error)
}

fn server_config(
    certs: &Path,
    private_key: &Path,
    passphrase_file: Option<&Path>,
) -> Result<ServerConfig> {
    let passphrase = load_passphrase(passphrase_file)?;

    // Both `ring` and `aws-lc-rs` are compiled into this binary (via other
    // dependencies), so the provider must be chosen explicitly: rustls panics
    // when asked to pick a default between two.
    ServerConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(
            load_certs(certs)?,
            load_private_key(private_key, passphrase.as_deref().map(Vec::as_slice))?,
        )
        .map_err(Into::into)
}

#[derive(Clone, Debug, Subcommand)]
#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
pub(super) enum Lake {
    /// Schema topics are written as Apache Iceberg tables
    #[cfg(feature = "iceberg")]
    Iceberg {
        /// Apache Parquet files are written to this location, examples are: file://./lake or s3://lake/
        #[arg(long, env = "DATA_LAKE")]
        location: EnvVarExp<Url>,

        /// Apache Iceberg Catalog, examples are: http://localhost:8181/
        #[arg(long, env = "ICEBERG_CATALOG")]
        catalog: EnvVarExp<Url>,

        /// Iceberg namespace
        #[arg(long, env = "ICEBERG_NAMESPACE", default_value = "nisshi")]
        namespace: Option<String>,

        /// Iceberg warehouse
        #[arg(long, env = "ICEBERG_WAREHOUSE")]
        warehouse: Option<String>,
    },

    /// Schema topics are written as Delta Lake tables
    #[cfg(feature = "delta")]
    Delta {
        /// Apache Parquet files are written to this location, examples are: file://./lake or s3://lake/
        #[arg(long, env = "DATA_LAKE")]
        location: EnvVarExp<Url>,

        /// Delta database
        #[arg(long, env = "DELTA_DATABASE", default_value = "nisshi")]
        database: Option<String>,

        /// Throttle the maximum number of records per second
        #[clap(long)]
        records_per_second: Option<u32>,
    },

    /// Schema topics are written in Parquet format
    #[cfg(feature = "parquet")]
    Parquet {
        /// Apache Parquet files are written to this location, examples are: file://./lake or s3://lake/
        #[arg(long, env = "DATA_LAKE")]
        location: EnvVarExp<Url>,
    },
}

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
impl Lake {
    fn name(&self) -> &'static str {
        match self {
            #[cfg(feature = "iceberg")]
            Self::Iceberg { .. } => "iceberg",

            #[cfg(feature = "delta")]
            Self::Delta { .. } => "delta",

            #[cfg(feature = "parquet")]
            Self::Parquet { .. } => "parquet",
        }
    }
}

/// Requires a schema registry whenever a data lake subcommand is selected.
///
/// clap cannot enforce this itself without making `--schema-registry`
/// required on each lake subcommand individually, which rejects the flag
/// given before the subcommand (`broker --schema-registry y iceberg ...`):
/// the only ordering this repo's own `justfile`, `compose.yaml` and
/// `docs/schema-registry.md` use. This check runs instead, after clap has
/// already resolved the value regardless of where the flag appeared.
#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
fn require_schema_registry(command: &Option<Lake>, has_schema_registry: bool) -> Result<()> {
    match command {
        Some(lake) if !has_schema_registry => {
            Err(Error::LakeRequiresSchemaRegistry { lake: lake.name() })
        }
        _ => Ok(()),
    }
}

impl Arg {
    pub(super) async fn main(self) -> Result<ErrorCode> {
        let started = Instant::now();
        self.build()
            .await?
            .main(started)
            .await
            .inspect(|result| debug!(?result))
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
    }

    async fn build(self) -> Result<Broker<Controller<ArcDynStorage>, ArcDynStorage>> {
        // A bad TLS configuration must fail startup loudly rather than silently
        // falling back to a plaintext listener. It is checked first: it is a
        // cheap local validation, so it fails before any registry, lake or
        // storage connection is attempted.
        let tls_server_config = match (self.cert.as_deref(), self.key.as_deref()) {
            (Some(cert), Some(key)) => Some(server_config(
                cert,
                key,
                self.key_passphrase_file.as_deref(),
            )?),
            (None, None) => None,
            // clap enforces this pairing already; keep the invariant if the
            // arguments are ever constructed another way.
            _ => return Err(Error::TlsRequiresCertAndKey),
        };

        // The broker warns instead of refusing to start, so that a test setup
        // that sets one of these variables keeps working after an upgrade.
        for variable in insecure_tls::env_variables(env::vars_os()) {
            warn!(
                "{variable} turns off certificate checks on connections to object stores, \
                 which is unsupported and lets anyone on the network path read and change \
                 the data: https://github.com/nisshi-io/nisshi#outbound-tls"
            );
        }

        let cluster_id = self.cluster_id;
        let incarnation_id = Uuid::now_v7();
        let otlp_endpoint_url = self
            .otlp_endpoint_url
            .map(|env_var_exp| env_var_exp.into_inner());

        let storage_engine = self.storage_engine.into_inner();
        let advertised_listener = self.advertised_listener_url.into_inner();
        let listener = self.listener_url.into_inner();

        let schema_registry_url = self
            .schema_registry
            .map(|env_var_exp| env_var_exp.into_inner());

        let schema_registry = schema_registry_url
            .clone()
            .map(|object_store| {
                Registry::builder_try_from_url(&object_store).map(|registry| {
                    registry
                        .with_cache_expiry_after(self.schema_registry_cache_expiry)
                        .build()
                })
            })
            .transpose()?;

        #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
        require_schema_registry(&self.command, schema_registry.is_some())?;

        #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
        let lake_house = match self.command {
            #[cfg(feature = "iceberg")]
            Some(Lake::Iceberg {
                location,
                catalog,
                namespace,
                warehouse,
            }) => Some(
                nisshi_schema::lake::House::iceberg()
                    .location(location.into_inner())
                    .catalog(catalog.into_inner())
                    .schema_registry(
                        schema_registry
                            .clone()
                            .ok_or(Error::LakeRequiresSchemaRegistry { lake: "iceberg" })?,
                    )
                    .namespace(namespace)
                    .warehouse(warehouse)
                    .build()
                    .await?,
            ),

            #[cfg(feature = "delta")]
            Some(Lake::Delta {
                location,
                database,
                records_per_second,
            }) => Some(
                nisshi_schema::lake::House::delta()
                    .location(location.into_inner())
                    .schema_registry(
                        schema_registry
                            .clone()
                            .ok_or(Error::LakeRequiresSchemaRegistry { lake: "delta" })?,
                    )
                    .database(database)
                    .records_per_second(records_per_second)
                    .build()?,
            ),

            #[cfg(feature = "parquet")]
            Some(Lake::Parquet { location }) => Some(
                nisshi_schema::lake::House::parquet()
                    .location(location.into_inner())
                    .schema_registry(
                        schema_registry
                            .clone()
                            .ok_or(Error::LakeRequiresSchemaRegistry { lake: "parquet" })?,
                    )
                    .build()?,
            ),

            None => None,
        };

        let broker = Broker::<Controller<ArcDynStorage>, ArcDynStorage>::builder()
            .node_id(NODE_ID)
            .cluster_id(cluster_id)
            .incarnation_id(incarnation_id)
            .advertised_listener(advertised_listener.clone())
            .otlp_endpoint_url(otlp_endpoint_url)
            .schema_registry(schema_registry.clone())
            .storage(storage_engine.clone())
            .listener(listener.clone())
            .authentication(self.authentication)
            .tls_server_config(tls_server_config)
            .silent(self.silent)
            .group_coordinator(GroupCoordinatorConfig {
                initial_rebalance_delay: self.group_initial_rebalance_delay,
                poll_interval: self.group_poll_interval,
            })
            .topic_creation(TopicCreationConfig {
                auto_create_topics_enable: self.auto_create_topics_enable,
                num_partitions: self.num_partitions,
            });

        #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
        let broker = broker.lake_house(lake_house);

        // The broker prints the banner once its listener is bound, so that a broker that
        // fails to start prints only the error.
        let banner = (!self.silent).then(|| {
            banner(
                &listener,
                &advertised_listener,
                &storage_engine,
                schema_registry_url.as_ref(),
                self.cert.as_deref(),
            )
        });

        broker.banner(banner).build().await.map_err(Into::into)
    }
}

/// Returns the lines that the broker prints when it starts listening: its version, listener,
/// storage, and the schema registry and TLS certificate when it has them.
fn banner(
    listener: &Url,
    advertised_listener: &Url,
    storage_engine: &Url,
    schema_registry: Option<&Url>,
    cert: Option<&Path>,
) -> String {
    let sheet = Sheet::default();
    let mut banner = String::new();

    // Writing to a String can't fail.
    _ = writeln!(
        banner,
        "nisshi {} {}",
        "broker".if_supports_color(Stream::Stdout, |text| text.style(sheet.headline)),
        env!("CARGO_PKG_VERSION")
            .if_supports_color(Stream::Stdout, |text| text.style(sheet.version))
    );

    _ = writeln!(
        banner,
        "listener: {} (advertised: {})",
        listener.if_supports_color(Stream::Stdout, |text| text.style(sheet.listener)),
        advertised_listener
            .if_supports_color(Stream::Stdout, |text| text.style(sheet.advertised_listener))
    );

    _ = writeln!(
        banner,
        "storage: {} {:?}",
        redact_url(storage_engine)
            .if_supports_color(Stream::Stdout, |text| text.style(sheet.storage)),
        storage_engines()
            .iter()
            .map(|storage_engine| storage_engine
                .if_supports_color(Stream::Stdout, |text| text.style(sheet.storage)))
            .collect::<Vec<_>>()
    );

    if let Some(schema_registry) = schema_registry {
        _ = writeln!(
            banner,
            "schema registry: {}",
            redact_url(schema_registry)
                .if_supports_color(Stream::Stdout, |text| text.style(sheet.schema_registry))
        );
    }

    if let Some(cert) = cert {
        _ = writeln!(
            banner,
            "tls: {} ({})",
            "enabled".if_supports_color(Stream::Stdout, |text| text.style(sheet.tls)),
            cert.display()
                .if_supports_color(Stream::Stdout, |text| text.style(sheet.tls))
        );
    }

    banner
}

struct Sheet {
    advertised_listener: Style,
    headline: Style,
    listener: Style,
    schema_registry: Style,
    storage: Style,
    tls: Style,
    version: Style,
}

impl Default for Sheet {
    fn default() -> Self {
        Self {
            advertised_listener: Style::new().magenta().bold(),
            headline: Style::new().green().bold(),
            listener: Style::new().magenta().bold(),
            schema_registry: Style::new().magenta().bold(),
            storage: Style::new().magenta().bold(),
            tls: Style::new().magenta().bold(),
            version: Style::new().magenta().bold(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use tempfile::TempDir;

    use super::*;
    use crate::Error;

    struct Pem {
        _dir: TempDir,
        cert: PathBuf,
        key: PathBuf,
    }

    /// A freshly generated self-signed certificate and its private key as PEM files.
    fn pem() -> Pem {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(["localhost".to_owned()]).expect("self-signed");

        let dir = tempfile::tempdir().expect("tempdir");
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");

        fs::write(&cert_path, cert.pem()).expect("write cert");
        fs::write(&key_path, signing_key.serialize_pem()).expect("write key");

        Pem {
            _dir: dir,
            cert: cert_path,
            key: key_path,
        }
    }

    fn parse(args: &[&str]) -> Result<Arg, clap::Error> {
        Arg::try_parse_from(std::iter::once("nisshi").chain(args.iter().copied()))
    }

    /// With neither flag nor environment variable overriding them, the listener binds
    /// every interface dual-stack and the advertised listener resolves to the IPv4
    /// loopback address, not `localhost`: a client whose resolver returns `::1` first
    /// must not be sent back to an address this build cannot always serve.
    ///
    /// `LISTENER_URL` and `ADVERTISED_LISTENER_URL` are unset for the duration of this
    /// test because CI and a developer's own `.env` both set `ADVERTISED_LISTENER_URL`,
    /// which would otherwise mask the default this test exists to pin.
    #[test]
    fn defaults_resolve_listener_and_advertised_listener() {
        temp_env::with_vars_unset(["LISTENER_URL", "ADVERTISED_LISTENER_URL"], || {
            let arg = parse(&[]).expect("defaults parse");

            assert_eq!(
                Some(url::Host::Ipv6(std::net::Ipv6Addr::UNSPECIFIED)),
                arg.listener_url.into_inner().host(),
            );

            assert_eq!(
                Some("127.0.0.1"),
                arg.advertised_listener_url.into_inner().host_str(),
            );
        });
    }

    #[test]
    fn topic_creation_defaults_match_kafka() {
        temp_env::with_vars_unset(["AUTO_CREATE_TOPICS_ENABLE", "NUM_PARTITIONS"], || {
            let arg = parse(&[]).expect("defaults parse");

            assert!(arg.auto_create_topics_enable);
            assert_eq!(1, arg.num_partitions);
        });
    }

    #[test]
    fn auto_create_topics_enable_can_be_turned_off() {
        let arg = parse(&["--auto-create-topics-enable=false"]).expect("false parses");

        assert!(!arg.auto_create_topics_enable);
    }

    #[test]
    fn num_partitions_sets_the_default() {
        let arg = parse(&["--num-partitions=4"]).expect("4 parses");

        assert_eq!(4, arg.num_partitions);
    }

    #[test]
    fn num_partitions_below_one_is_rejected() {
        for num_partitions in ["--num-partitions=0", "--num-partitions=-1"] {
            let err = parse(&[num_partitions]).expect_err("num.partitions must be at least 1");

            assert_eq!(
                clap::error::ErrorKind::ValueValidation,
                err.kind(),
                "{num_partitions}"
            );
        }
    }

    #[test]
    fn cert_requires_key() {
        let pem = pem();

        let err = parse(&["--cert", pem.cert.to_str().unwrap()])
            .expect_err("--cert without --key must be rejected");

        assert_eq!(clap::error::ErrorKind::MissingRequiredArgument, err.kind());
    }

    #[test]
    fn key_requires_cert() {
        let pem = pem();

        let err = parse(&["--key", pem.key.to_str().unwrap()])
            .expect_err("--key without --cert must be rejected");

        assert_eq!(clap::error::ErrorKind::MissingRequiredArgument, err.kind());
    }

    #[test]
    fn cert_and_key_together_parse() {
        let pem = pem();

        let arg = parse(&[
            "--cert",
            pem.cert.to_str().unwrap(),
            "--key",
            pem.key.to_str().unwrap(),
        ])
        .expect("--cert and --key together must parse");

        assert_eq!(Some(pem.cert.as_path()), arg.cert.as_deref());
        assert_eq!(Some(pem.key.as_path()), arg.key.as_deref());
    }

    #[test]
    fn valid_pem_builds_server_config() {
        let pem = pem();

        _ = server_config(&pem.cert, &pem.key, None).expect("valid cert and key");
    }

    #[test]
    fn missing_files_fail() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist.pem");

        assert!(server_config(&missing, &missing, None).is_err());
    }

    #[test]
    fn invalid_pem_fails() {
        let dir = tempfile::tempdir().unwrap();
        let garbage = dir.path().join("garbage.pem");
        fs::write(&garbage, "not a pem file").unwrap();

        assert!(server_config(&garbage, &garbage, None).is_err());
    }

    #[test]
    fn mismatched_key_fails() {
        let a = pem();
        let b = pem();

        let err =
            server_config(&a.cert, &b.key, None).expect_err("key from another pair must fail");

        assert!(matches!(err, Error::Tls(_)), "{err:?}");
    }

    #[test]
    fn encrypted_key_rejected() {
        let pem = pem();
        let encrypted = pem.key.with_file_name("encrypted.pem");
        fs::write(
            &encrypted,
            "-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIBvTBXBgkqhkiG9w0BBQ0wSjApBgkqhkiG9w0BBQwwHAQI\n-----END ENCRYPTED PRIVATE KEY-----\n",
        )
        .unwrap();

        let err = server_config(&pem.cert, &encrypted, None).expect_err("encrypted key must fail");

        assert!(
            matches!(err, Error::TlsKeyPassphraseRequired { ref path } if *path == encrypted),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn build_fails_on_bad_tls() {
        let dir = tempfile::tempdir().unwrap();
        let garbage = dir.path().join("garbage.pem");
        fs::write(&garbage, "not a pem file").unwrap();

        let arg = parse(&[
            "--storage-engine",
            "memory://nisshi/",
            "--silent",
            "--cert",
            garbage.to_str().unwrap(),
            "--key",
            garbage.to_str().unwrap(),
        ])
        .expect("arguments parse");

        let err = arg
            .build()
            .await
            .expect_err("build must fail with an unreadable certificate");

        assert!(
            matches!(err, Error::TlsCertificate { ref path, .. } if *path == garbage),
            "expected the certificate error naming its path before anything else, got {err:?}"
        );
    }

    /// A common operator slip: pointing `--cert` at the key file yields an
    /// empty certificate chain, which must be rejected rather than served.
    #[test]
    fn key_file_as_cert_fails() {
        let pem = pem();

        let err = server_config(&pem.key, &pem.key, None).expect_err("empty certificate chain");

        assert!(
            matches!(
                err,
                Error::TlsCertificate {
                    ref path,
                    source: TlsPkiPemError::NoItemsFound
                } if *path == pem.key
            ),
            "{err:?}"
        );
    }

    #[test]
    fn missing_key_names_its_path() {
        let pem = pem();
        let missing = pem.key.with_file_name("missing.pem");

        let err = server_config(&pem.cert, &missing, None).expect_err("missing key file");

        assert!(
            matches!(err, Error::TlsPrivateKey { ref path, .. } if *path == missing),
            "{err:?}"
        );
    }

    #[test]
    fn legacy_encrypted_key_rejected() {
        let pem = pem();
        let encrypted = write(&pem, "legacy.pem", EC_KEY_LEGACY_ENCRYPTED);
        let passphrase = write(&pem, "passphrase", FIXTURE_PASSPHRASE);

        let err = server_config(&pem.cert, &encrypted, Some(&passphrase))
            .expect_err("legacy encrypted key must fail even with the right passphrase");

        assert!(
            matches!(err, Error::TlsKeyLegacyEncrypted { ref path } if *path == encrypted),
            "{err:?}"
        );
    }

    // OpenSSL-generated fixtures (`tests/fixtures/tls/`): interop guards so
    // the in-process encryption used by `encrypted_key_with_passphrase_builds`
    // cannot mask a mismatch with what `openssl pkcs8 -topk8` actually emits.
    // Every encrypted key uses the passphrase `correct-horse`. Neither key
    // protects anything. `with_single_cert` does not check validity dates,
    // so short fixture lifetimes do not matter.
    const FIXTURE_PASSPHRASE: &[u8] = b"correct-horse";

    /// Self-signed P-256 certificate:
    /// `openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj /CN=localhost -days 1`
    const EC_CERT: &str = include_str!("../../tests/fixtures/tls/ec-cert.pem");

    /// The P-256 key as PBES2 / PBKDF2-HMAC-SHA256 (100 000 iterations) / AES-256-CBC:
    /// `openssl pkcs8 -topk8 -v2 aes-256-cbc -v2prf hmacWithSHA256 -iter 100000`
    const EC_KEY_ENCRYPTED: &str =
        include_str!("../../tests/fixtures/tls/ec-key-pkcs8-encrypted.pem");

    /// The P-256 key with legacy OpenSSL PEM encryption: `openssl ec -aes256`
    const EC_KEY_LEGACY_ENCRYPTED: &str =
        include_str!("../../tests/fixtures/tls/ec-key-legacy-encrypted.pem");

    /// Self-signed RSA-2048 certificate:
    /// `openssl req -x509 -newkey rsa:2048 -nodes -subj /CN=localhost -days 397`
    const RSA_CERT: &str = include_str!("../../tests/fixtures/tls/rsa-cert.pem");

    /// The RSA key as PBES2 / PBKDF2-HMAC-SHA256 (2048 iterations) / AES-256-CBC:
    /// `openssl pkcs8 -topk8 -v2 aes-256-cbc -v2prf hmacWithSHA256 -iter 2048`
    const RSA_KEY_ENCRYPTED_AES: &str =
        include_str!("../../tests/fixtures/tls/rsa-key-pkcs8-aes.pem");

    /// The same RSA key as PBES2 / PBKDF2-HMAC-SHA256 (2048 iterations) / Triple DES CBC:
    /// `openssl pkcs8 -topk8 -v2 des3 -v2prf hmacWithSHA256 -iter 2048`
    const RSA_KEY_ENCRYPTED_3DES: &str =
        include_str!("../../tests/fixtures/tls/rsa-key-pkcs8-3des.pem");

    /// The same RSA key as PBES2 / scrypt / AES-256-CBC: `openssl pkcs8 -topk8 -scrypt`
    const RSA_KEY_ENCRYPTED_SCRYPT: &str =
        include_str!("../../tests/fixtures/tls/rsa-key-pkcs8-scrypt.pem");

    /// The same RSA key as PBES2 / PBKDF2-HMAC-SHA1 (2048 iterations) / AES-256-CBC,
    /// older OpenSSL releases' default PRF, which this build does not support:
    /// `openssl pkcs8 -topk8 -v2 aes-256-cbc -v2prf hmacWithSHA1 -iter 2048`
    const RSA_KEY_ENCRYPTED_SHA1_PRF: &str =
        include_str!("../../tests/fixtures/tls/rsa-key-pkcs8-sha1-prf.pem");

    /// Writes `contents` next to the generated PEM files and returns its path.
    fn write(pem: &Pem, name: &str, contents: impl AsRef<[u8]>) -> PathBuf {
        let path = pem.key.with_file_name(name);
        fs::write(&path, contents).expect("write fixture");
        path
    }

    /// The freshly generated key, re-encoded as an encrypted PKCS#8 PEM
    /// (PBES2 / PBKDF2-HMAC-SHA256 / AES-256-CBC) under `passphrase`.
    fn encrypt_key(pem: &Pem, passphrase: &[u8]) -> PathBuf {
        use pkcs8::{LineEnding, PrivateKeyInfo, SecretDocument, pkcs5::pbes2::Parameters};

        let plain = fs::read_to_string(&pem.key).expect("read key");
        let (label, der) = SecretDocument::from_pem(&plain).expect("pkcs8 pem");
        assert_eq!("PRIVATE KEY", label);

        let salt = [7u8; 16];
        let iv = [9u8; 16];
        let params = Parameters::pbkdf2_sha256_aes256cbc(10_000, &salt, &iv).expect("pbes2");

        let encrypted = PrivateKeyInfo::try_from(der.as_bytes())
            .expect("private key info")
            .encrypt_with_params(params, passphrase)
            .expect("encrypt");

        write(
            pem,
            "encrypted.pem",
            encrypted
                .to_pem("ENCRYPTED PRIVATE KEY", LineEnding::LF)
                .expect("pem")
                .as_bytes(),
        )
    }

    #[test]
    fn encrypted_key_with_passphrase_builds() {
        let pem = pem();
        let encrypted = encrypt_key(&pem, b"pw");
        // Editors and `echo` leave a trailing newline; it is not part of the passphrase.
        let passphrase = write(&pem, "passphrase", "pw\n");

        _ = server_config(&pem.cert, &encrypted, Some(&passphrase))
            .expect("encrypted key with its passphrase");
    }

    /// A passphrase file written on Windows, or with a blank line after the
    /// passphrase: every trailing CR and LF is dropped, not just one byte.
    #[test]
    fn encrypted_key_with_crlf_passphrase_builds() {
        let pem = pem();
        let encrypted = encrypt_key(&pem, b"pw");
        let passphrase = write(&pem, "passphrase", "pw\r\n\n");

        _ = server_config(&pem.cert, &encrypted, Some(&passphrase))
            .expect("trailing line endings are not part of the passphrase");
    }

    #[test]
    fn encrypted_key_wrong_passphrase_fails() {
        let pem = pem();
        let encrypted = encrypt_key(&pem, b"pw");
        let passphrase = write(&pem, "passphrase", "not-pw\n");

        let err = server_config(&pem.cert, &encrypted, Some(&passphrase))
            .expect_err("wrong passphrase must fail");

        assert!(
            matches!(err, Error::TlsKeyDecrypt { ref path, .. } if *path == encrypted),
            "{err:?}"
        );
    }

    #[test]
    fn encrypted_key_without_passphrase_fails() {
        let pem = pem();
        let encrypted = encrypt_key(&pem, b"pw");

        let err = server_config(&pem.cert, &encrypted, None)
            .expect_err("encrypted key without a passphrase must fail");

        assert!(
            matches!(err, Error::TlsKeyPassphraseRequired { ref path } if *path == encrypted),
            "{err:?}"
        );
    }

    #[test]
    fn openssl_pkcs8_ec_fixture_decrypts() {
        let pem = pem();
        let cert = write(&pem, "ec-cert.pem", EC_CERT);
        let key = write(&pem, "ec-key.pem", EC_KEY_ENCRYPTED);
        let passphrase = write(&pem, "passphrase", FIXTURE_PASSPHRASE);

        _ = server_config(&cert, &key, Some(&passphrase)).expect("openssl pkcs8 ec key");
    }

    #[test]
    fn openssl_pkcs8_rsa_aes_fixture_decrypts() {
        let pem = pem();
        let cert = write(&pem, "rsa-cert.pem", RSA_CERT);
        let key = write(&pem, "rsa-key.pem", RSA_KEY_ENCRYPTED_AES);
        let passphrase = write(&pem, "passphrase", FIXTURE_PASSPHRASE);

        _ = server_config(&cert, &key, Some(&passphrase)).expect("openssl pkcs8 rsa aes key");
    }

    #[test]
    fn openssl_pkcs8_rsa_3des_fixture_decrypts() {
        let pem = pem();
        let cert = write(&pem, "rsa-cert.pem", RSA_CERT);
        let key = write(&pem, "rsa-key.pem", RSA_KEY_ENCRYPTED_3DES);
        let passphrase = write(&pem, "passphrase", FIXTURE_PASSPHRASE);

        _ = server_config(&cert, &key, Some(&passphrase)).expect("openssl pkcs8 rsa 3des key");
    }

    #[test]
    fn openssl_pkcs8_rsa_scrypt_fixture_decrypts() {
        let pem = pem();
        let cert = write(&pem, "rsa-cert.pem", RSA_CERT);
        let key = write(&pem, "rsa-key.pem", RSA_KEY_ENCRYPTED_SCRYPT);
        let passphrase = write(&pem, "passphrase", FIXTURE_PASSPHRASE);

        _ = server_config(&cert, &key, Some(&passphrase)).expect("openssl pkcs8 rsa scrypt key");
    }

    /// A SHA-1 PRF key with the right passphrase must name the algorithm, not
    /// blame the passphrase.
    #[test]
    fn openssl_pkcs8_sha1_prf_fixture_names_the_algorithm() {
        let pem = pem();
        let cert = write(&pem, "rsa-cert.pem", RSA_CERT);
        let key = write(&pem, "rsa-key.pem", RSA_KEY_ENCRYPTED_SHA1_PRF);
        let passphrase = write(&pem, "passphrase", FIXTURE_PASSPHRASE);

        let err =
            server_config(&cert, &key, Some(&passphrase)).expect_err("sha-1 prf is not supported");

        assert!(
            matches!(
                err,
                Error::TlsKeyUnsupportedEncryption { ref path, ref algorithm }
                    if *path == key && algorithm == "PBKDF2 with HMAC-SHA1"
            ),
            "{err:?}"
        );
    }

    /// A common deployment shape: certificate chain and encrypted key in one
    /// PEM bundle, with `--cert` and `--key` both pointing at it.
    #[test]
    fn cert_and_key_in_one_bundle() {
        let pem = pem();
        let bundle = write(
            &pem,
            "bundle.pem",
            format!("{RSA_CERT}{RSA_KEY_ENCRYPTED_AES}"),
        );
        let passphrase = write(&pem, "passphrase", FIXTURE_PASSPHRASE);

        _ = server_config(&bundle, &bundle, Some(&passphrase)).expect("cert and key bundle");
    }

    #[test]
    fn unencrypted_key_ignores_passphrase() {
        let pem = pem();
        let passphrase = write(&pem, "passphrase", "unused\n");

        _ = server_config(&pem.cert, &pem.key, Some(&passphrase))
            .expect("a passphrase for an unencrypted key is ignored");
    }

    #[test]
    fn unencrypted_key_with_empty_passphrase_file() {
        let pem = pem();
        let passphrase = write(&pem, "passphrase", "");

        _ = server_config(&pem.cert, &pem.key, Some(&passphrase))
            .expect("an empty passphrase file means no passphrase");
    }

    #[test]
    fn encrypted_key_with_empty_passphrase_file_fails() {
        let pem = pem();
        let encrypted = encrypt_key(&pem, b"pw");
        let passphrase = write(&pem, "passphrase", "\n");

        let err = server_config(&pem.cert, &encrypted, Some(&passphrase))
            .expect_err("an empty passphrase file is no passphrase");

        assert!(
            matches!(err, Error::TlsKeyPassphraseRequired { ref path } if *path == encrypted),
            "{err:?}"
        );
    }

    #[test]
    fn missing_passphrase_file_fails() {
        let pem = pem();
        let encrypted = encrypt_key(&pem, b"pw");
        let missing = pem.key.with_file_name("missing-passphrase");

        let err = server_config(&pem.cert, &encrypted, Some(&missing))
            .expect_err("missing passphrase file");

        assert!(
            matches!(err, Error::TlsKeyPassphraseFile { ref path, .. } if *path == missing),
            "{err:?}"
        );
    }

    #[test]
    fn key_passphrase_file_requires_key() {
        let pem = pem();
        let passphrase = write(&pem, "passphrase", "pw");

        let err = parse(&["--key-passphrase-file", passphrase.to_str().unwrap()])
            .expect_err("--key-passphrase-file without --key must be rejected");

        assert_eq!(clap::error::ErrorKind::MissingRequiredArgument, err.kind());
    }

    #[test]
    fn key_passphrase_file_with_cert_and_key_parses() {
        let pem = pem();
        let passphrase = write(&pem, "passphrase", "pw");

        let arg = parse(&[
            "--cert",
            pem.cert.to_str().unwrap(),
            "--key",
            pem.key.to_str().unwrap(),
            "--key-passphrase-file",
            passphrase.to_str().unwrap(),
        ])
        .expect("--cert, --key and --key-passphrase-file together must parse");

        assert_eq!(
            Some(passphrase.as_path()),
            arg.key_passphrase_file.as_deref()
        );
    }

    #[tokio::test]
    async fn build_with_encrypted_bundle_succeeds() {
        let pem = pem();
        let bundle = write(
            &pem,
            "bundle.pem",
            format!("{RSA_CERT}{RSA_KEY_ENCRYPTED_AES}"),
        );
        let passphrase = write(&pem, "passphrase", FIXTURE_PASSPHRASE);

        let arg = parse(&[
            "--storage-engine",
            "memory://nisshi/",
            "--silent",
            "--cert",
            bundle.to_str().unwrap(),
            "--key",
            bundle.to_str().unwrap(),
            "--key-passphrase-file",
            passphrase.to_str().unwrap(),
        ])
        .expect("arguments parse");

        // `--schema-registry` also reads SCHEMA_REGISTRY, which CI exports
        // as a path relative to the workspace root; this test is about TLS,
        // not the registry, so it must not depend on the environment.
        let arg = Arg {
            schema_registry: None,
            ..arg
        };

        _ = arg
            .build()
            .await
            .expect("build with an encrypted cert and key bundle");
    }

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    mod lake_schema_registry {
        use super::*;

        /// Whichever lake feature this build enables, so the test runs
        /// regardless of which combination `--all-features` turns on.
        fn lake() -> Lake {
            #[cfg(feature = "parquet")]
            {
                Lake::Parquet {
                    location: "file://./lake".parse().expect("valid url"),
                }
            }

            #[cfg(all(not(feature = "parquet"), feature = "delta"))]
            {
                Lake::Delta {
                    location: "file://./lake".parse().expect("valid url"),
                    database: None,
                    records_per_second: None,
                }
            }

            #[cfg(all(not(feature = "parquet"), not(feature = "delta"), feature = "iceberg"))]
            {
                Lake::Iceberg {
                    location: "file://./lake".parse().expect("valid url"),
                    catalog: "http://localhost:8181/".parse().expect("valid url"),
                    namespace: None,
                    warehouse: None,
                }
            }
        }

        #[test]
        fn no_lake_no_registry_is_ok() {
            require_schema_registry(&None, false).expect("no lake command needs no registry");
        }

        #[test]
        fn no_lake_with_registry_is_ok() {
            require_schema_registry(&None, true)
                .expect("a registry present without a lake command is fine");
        }

        #[test]
        fn lake_without_registry_is_rejected() {
            let err = require_schema_registry(&Some(lake()), false)
                .expect_err("a lake command without a schema registry must be rejected");

            assert!(
                matches!(err, Error::LakeRequiresSchemaRegistry { .. }),
                "{err:?}"
            );
        }

        #[test]
        fn lake_with_registry_is_ok() {
            require_schema_registry(&Some(lake()), true)
                .expect("a lake command with a schema registry must be accepted");
        }
    }

    #[cfg(feature = "parquet")]
    #[tokio::test]
    async fn parquet_lake_without_schema_registry_fails_build() {
        let arg = parse(&[
            "--storage-engine",
            "memory://nisshi/",
            "--silent",
            "parquet",
            "--location",
            "file://./lake",
        ])
        .expect("arguments parse");

        // Never depend on a SCHEMA_REGISTRY the environment happens to export.
        let arg = Arg {
            schema_registry: None,
            ..arg
        };

        let err = arg
            .build()
            .await
            .expect_err("a lake command without a schema registry must fail build");

        assert!(
            matches!(err, Error::LakeRequiresSchemaRegistry { lake: "parquet" }),
            "{err:?}"
        );
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn schema_registry_before_subcommand_parses() {
        let arg = parse(&[
            "--schema-registry",
            "file://./etc/schema",
            "parquet",
            "--location",
            "file://./lake",
        ])
        .expect("--schema-registry given before the subcommand must still parse");

        assert!(arg.schema_registry.is_some());
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn schema_registry_after_subcommand_parses() {
        let arg = parse(&[
            "parquet",
            "--location",
            "file://./lake",
            "--schema-registry",
            "file://./etc/schema",
        ])
        .expect("--schema-registry given after the subcommand must parse");

        assert!(arg.schema_registry.is_some());
    }
}
