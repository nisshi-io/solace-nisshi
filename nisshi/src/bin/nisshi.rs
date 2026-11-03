// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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
use dotenv::dotenv;
use nisshi_broker::{TracingFormat, otel};
use nisshi_cli::{Cli, Outcome};
use nisshi_sans_io::ErrorCode;
use std::{
    io,
    net::SocketAddr,
    process::{ExitCode, Termination},
};
use tracing::{debug, error};

/// The option and the environment variable that give `cat`, `generator` and `perf` their
/// broker.
const BROKER_OPTION: &str = "--broker (or ADVERTISED_LISTENER_URL)";

/// The option that gives `topic` its broker. `topic` reads no environment variable for it.
const TOPIC_BROKER_OPTION: &str = "--broker";

/// The option and the environment variable that give `proxy` its origin broker.
const ORIGIN_OPTION: &str = "--origin-url (or ORIGIN_URL)";

/// Returns the error message for a command whose Kafka client failed. `broker_option` names
/// the option and the environment variable that give the command its broker.
fn client_error_message(error: &nisshi_client::Error, broker_option: &str) -> String {
    match error {
        nisshi_client::Error::Pool(cause) if error.is_connect_failure() => format!(
            "Could not connect to the broker: {cause}. Check that it is running (nisshi broker) \
            and that {broker_option} points at it."
        ),
        // A broker that serves only TLS, or that requires authentication, accepts the connection
        // and then closes it when the plaintext request arrives.
        nisshi_client::Error::Io(cause)
            if matches!(
                cause.kind(),
                io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
            ) =>
        {
            format!(
                "Connected to the broker, but it closed the connection: {cause}. This command \
                connects in plaintext and does not authenticate, so it can't use a broker that \
                serves only TLS (--cert and --key) or that requires authentication \
                (--authentication)."
            )
        }
        _ => format!("The request to the broker failed: {error}"),
    }
}

/// Returns the error message for an error that `cat` returned.
fn cat_error_message(error: &nisshi_cat::Error) -> String {
    match error {
        nisshi_cat::Error::Client(error) => client_error_message(error, BROKER_OPTION),
        nisshi_cat::Error::InvalidJson { .. }
        | nisshi_cat::Error::MessageWithoutKeyOrValue { .. }
        | nisshi_cat::Error::NoMessages { .. }
        | nisshi_cat::Error::NotAnArray { .. } => error.to_string(),
        _ => format!("Unknown error occurred during command: {error}"),
    }
}

/// Returns the error message for a storage URL whose engine this build doesn't have.
/// `scheme` is the URL's scheme, and `storage` is the URL with its credentials redacted.
fn feature_not_enabled_message(scheme: &str, storage: &str) -> String {
    let feature = match scheme {
        "memory" | "s3" | "gs" => "dynostore",
        "postgres" | "postgresql" => "postgres",
        "sqlite" => "libsql",
        "slatedb" => "slatedb",
        "turso" => "turso",
        _ => {
            return format!(
                "storage {storage} uses the {scheme}:// scheme, which no storage engine in this \
                build supports"
            );
        }
    };

    format!(
        "storage {storage} needs the \"{feature}\" feature, which this build doesn't have. \
        Install a release binary, or reinstall with cargo install nisshi --features {feature}"
    )
}

/// Returns advice for a storage startup check failure, for the storage URL
/// scheme.
fn startup_check_hint(scheme: &str) -> &'static str {
    match scheme {
        "s3" => {
            ". Check that the bucket exists, that AWS_ENDPOINT and AWS_REGION are correct, and \
            that the AWS credentials allow listing the cluster's prefix in the bucket."
        }
        "gs" => {
            ". Check that the bucket exists, and that the Google Cloud credentials allow listing \
            the cluster's prefix in the bucket."
        }
        _ => "",
    }
}

/// Returns the error message for a broker that can't listen on `addr`, because another
/// socket already listens on it.
fn listener_in_use_message(addr: SocketAddr) -> String {
    format!(
        "{addr} is already in use, so the broker can't listen on it. Stop the process that uses \
        it (perhaps another broker), or start this broker on another port, with --listener-url \
        and --advertised-listener-url both set to tcp://<host>:<port>"
    )
}

/// Returns the error message for a broker whose listener URL names a host that did not
/// resolve to an address.
fn listener_host_unresolved_message(host: &str, port: u16, source: &io::Error) -> String {
    format!(
        "the --listener-url host {host} could not be resolved: {source}. Set --listener-url to \
        an IP address, or to a host name that this machine resolves, for example \
        tcp://127.0.0.1:{port} for local clients only, or tcp://<interface address>:{port}"
    )
}

/// The message for a storage option that the engine reads, given a value that it can't parse.
fn invalid_storage_option_value_message(
    scheme: &str,
    option: &str,
    value: &str,
    expected: &str,
) -> String {
    format!("storage option {option}={value} is not valid for the {scheme} engine: give {expected}")
}

/// Runs the command, and exits with failure when it returns an error or a Kafka error code.
///
/// `main` reports an error and then returns [`ExitCode::FAILURE`], instead of returning the
/// error, because Rust prints the `Debug` form of an error that `main` returns, after the
/// message that this function has already reported.
#[tokio::main]
async fn main() -> ExitCode {
    _ = dotenv().ok();

    let _guard = match otel::init(TracingFormat::Text) {
        Ok(guard) => guard,
        Err(err) => {
            eprintln!("Error: {err:?}");
            return ExitCode::FAILURE;
        }
    };

    match Cli::run().await {
        Ok(outcome) => {
            match outcome {
                Outcome {
                    error_code: ErrorCode::None,
                    ..
                }
                | Outcome { reported: true, .. } => debug!("{}", outcome.error_code),
                _ => error!("{}", outcome.error_code),
            }

            outcome.error_code.report()
        }

        Err(err) => {
            report_error(&err);
            ExitCode::FAILURE
        }
    }
}

/// Writes a failed command's final message to stderr after the prefix `error: `, whatever
/// `RUST_LOG` is set to. A script that captures stdout (`topics=$(nisshi topic list)`) then gets
/// no message in its output.
///
/// The prefix is the one that `rustc` and `clap` print, so a reader or a script finds the failure
/// line by it. The smoke tests' `error_lines` rule relies on it, because other lines of the
/// broker's output can also name the storage URL, so a test can't search the whole log for a bad
/// value.
macro_rules! report_failure {
    ($($argument:tt)*) => {
        eprintln!("{}", failure_line(format_args!($($argument)*)))
    };
}

/// Returns the line that `report_failure!` writes for the message: the message after `error: `.
fn failure_line(message: std::fmt::Arguments<'_>) -> String {
    format!("error: {message}")
}

/// Writes the message for an error that a command returned: what went wrong and how to fix it
/// when the error is a known one, or the error itself otherwise.
fn report_error(err: &nisshi_cli::Error) {
    match err {
        nisshi_cli::Error::Cat(error) => report_failure!("{}", cat_error_message(error)),
        nisshi_cli::Error::Generate(error) => match error {
            nisshi_generator::Error::Client(error) => {
                report_failure!("{}", client_error_message(error, BROKER_OPTION))
            }
            _ => report_failure!("Unknown error occurred during command: {}", error),
        },
        nisshi_cli::Error::Perf(error) => match error {
            nisshi_perf::Error::Client(error) => {
                report_failure!("{}", client_error_message(error, BROKER_OPTION))
            }
            _ => report_failure!("Unknown error occurred during command: {}", error),
        },
        nisshi_cli::Error::Proxy(error) => match error {
            nisshi_proxy::Error::Client(error) => {
                report_failure!("{}", client_error_message(error, ORIGIN_OPTION))
            }
            _ => report_failure!("Unknown error occurred during command: {}", error),
        },
        nisshi_cli::Error::Topic(error) => match error {
            nisshi_topic::Error::Client(error) => {
                report_failure!("{}", client_error_message(error, TOPIC_BROKER_OPTION))
            }
            _ => report_failure!("Unknown error occurred during command: {}", error),
        },
        nisshi_cli::Error::Client(error) => {
            report_failure!("{}", client_error_message(error, TOPIC_BROKER_OPTION))
        }
        nisshi_cli::Error::Server(error) => match &**error {
            #[cfg(any(feature = "dynostore", feature = "slatedb"))]
            nisshi_broker::Error::Storage(nisshi_storage::Error::NoCredentials(source)) => {
                report_failure!(
                    "could not get AWS credentials from the configured provider: {source}. AWS \
                        credentials come from static keys (AWS_ACCESS_KEY_ID and \
                        AWS_SECRET_ACCESS_KEY), a web identity token (AWS_WEB_IDENTITY_TOKEN_FILE \
                        and AWS_ROLE_ARN), an ECS or EKS container credential endpoint, or the EC2 \
                        instance metadata service."
                )
            }
            nisshi_broker::Error::Storage(nisshi_storage::Error::StartupFailed {
                storage,
                failure,
                source,
            }) => match failure {
                nisshi_storage::StartupFailure::StorageUnreachable => report_failure!(
                    "storage {storage} failed its startup check: {source}{}",
                    startup_check_hint(storage.scheme())
                ),
                nisshi_storage::StartupFailure::SchemaSetup => match &**source {
                    nisshi_storage::Error::IncompatibleSchema {
                        recorded_version,
                        oldest_compatible_version,
                        schema_version,
                    } => report_failure!(
                        "storage {storage} records schema version {recorded_version}, which a \
                            release with schema version {oldest_compatible_version} or later can \
                            use. This release has schema version {schema_version}: upgrade Nisshi."
                    ),
                    nisshi_storage::Error::OutdatedSchema {
                        recorded_version,
                        schema_version,
                    } => report_failure!(
                        "storage {storage} records schema version {recorded_version}, which is \
                            older than this release's schema version {schema_version}. This \
                            release can't upgrade the schema: run the release that set up this \
                            database."
                    ),
                    #[cfg(feature = "postgres")]
                    nisshi_storage::Error::TokioPostgres(err)
                        if err.code().map(|code| code.code()) == Some("42501") =>
                    {
                        report_failure!(
                            "storage {storage} failed its schema setup: {source}. The \
                                database records no schema version that this release can use, so \
                                the broker must change the schema, and its role isn't allowed \
                                to. Start one broker once with the credentials of the role that \
                                owns Nisshi's tables, then start the others with this role. See \
                                the PostgreSQL section of docs/storage.md."
                        )
                    }
                    _ => report_failure!("storage {storage} failed its schema setup: {source}"),
                },
            },
            nisshi_broker::Error::Storage(nisshi_storage::Error::AuthenticationUnsupported {
                engine,
                supported,
            }) => report_failure!(
                "{}",
                nisshi_storage::authentication_unsupported_message(engine, supported)
            ),
            nisshi_broker::Error::ListenerInUse { addr } => {
                report_failure!("{}", listener_in_use_message(*addr))
            }
            nisshi_broker::Error::ListenerHostUnresolved { host, port, source } => {
                report_failure!("{}", listener_host_unresolved_message(host, *port, source))
            }
            nisshi_broker::Error::Storage(nisshi_storage::Error::NoEngineForScheme {
                scheme,
                storage,
            }) => report_failure!("{}", feature_not_enabled_message(scheme, storage)),
            nisshi_broker::Error::InvalidStorageOptionValue { option, value } => report_failure!(
                "storage option {option}={value} is not a valid interval: give a non-zero \
                    duration with a unit, up to 365d (e.g. 500ms, 90s, 10m, 1h30m)"
            ),
            nisshi_broker::Error::Storage(nisshi_storage::Error::UnrecognizedStorageOption {
                scheme,
                option,
            }) => {
                report_failure!("storage option {option} is not recognised by the {scheme} engine")
            }
            nisshi_broker::Error::Storage(nisshi_storage::Error::InvalidStorageOptionValue {
                scheme,
                option,
                value,
                expected,
            }) => report_failure!(
                "{}",
                invalid_storage_option_value_message(scheme, option, value, expected)
            ),
            _ => report_failure!("Unknown error occurred during command: {}", err),
        },
        nisshi_cli::Error::TlsCertificate { path, source } => report_failure!(
            "TLS certificate {} could not be loaded: {source}. Expected one or more PEM \
                certificates (--cert).",
            path.display()
        ),
        nisshi_cli::Error::TlsPrivateKey { path, source } => report_failure!(
            "TLS private key {} could not be loaded: {source}. Expected a PKCS#8, SEC1 or RSA \
                PEM key (--key).",
            path.display()
        ),
        nisshi_cli::Error::TlsKeyPassphraseRequired { path } => report_failure!(
            "TLS private key {} is encrypted: pass --key-passphrase-file <file>.",
            path.display()
        ),
        nisshi_cli::Error::TlsKeyDecrypt { path, source } => report_failure!(
            "TLS private key {} could not be decrypted: {source}. Check the passphrase in \
                --key-passphrase-file.",
            path.display()
        ),
        nisshi_cli::Error::TlsKeyUnsupportedEncryption { path, algorithm } => report_failure!(
            "TLS private key {} is encrypted with {algorithm}, which is not supported. \
                Supported: PKCS#8 PBES2 with PBKDF2-HMAC-SHA2 or scrypt and AES-CBC or Triple DES. \
                Re-encrypt it: openssl pkcs8 -topk8 -in key.pem -out key-pkcs8.pem -v2 aes-256-cbc \
                -v2prf hmacWithSHA256",
            path.display()
        ),
        nisshi_cli::Error::TlsKeyLegacyEncrypted { path } => report_failure!(
            "TLS private key {} uses legacy OpenSSL PEM encryption, which is not supported. \
                Convert it: openssl pkcs8 -topk8 -in key.pem -out key-pkcs8.pem",
            path.display()
        ),
        nisshi_cli::Error::TlsKeyPassphraseFile { path, source } => report_failure!(
            "TLS key passphrase file {} could not be read: {source}.",
            path.display()
        ),
        nisshi_cli::Error::Tls(error) => report_failure!(
            "TLS configuration rejected: {error}. Check that --key is the private key for the \
                certificate in --cert and, for an encrypted key, that the passphrase is correct."
        ),
        nisshi_cli::Error::TlsRequiresCertAndKey => {
            report_failure!("TLS requires both --cert and --key.")
        }
        nisshi_cli::Error::LakeRequiresSchemaRegistry { lake } => report_failure!(
            "{lake} requires --schema-registry: a data lake writer validates every record \
                against a schema."
        ),
        _ => report_failure!("Unknown error occurred during command: {}", err),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BROKER_OPTION, TOPIC_BROKER_OPTION, cat_error_message, client_error_message, failure_line,
        feature_not_enabled_message, invalid_storage_option_value_message,
        listener_host_unresolved_message, listener_in_use_message,
    };

    /// The smoke tests find a failed start by the `error: ` prefix.
    #[test]
    fn failure_line_starts_with_the_error_prefix() {
        assert_eq!(
            "error: storage option a=b is not a valid interval",
            failure_line(format_args!(
                "storage option {}=b is not a valid interval",
                "a"
            ))
        );
    }

    /// The smoke tests find the bad value by `option=value`, and the user needs the hint
    /// to fix it.
    #[test]
    fn invalid_storage_option_value_message_names_the_value_engine_and_hint() {
        assert_eq!(
            "storage option mode=Direct is not valid for the sqlite engine: give semaphore, mpsc \
                or direct",
            invalid_storage_option_value_message(
                "sqlite",
                "mode",
                "Direct",
                "semaphore, mpsc or direct"
            )
        );
    }

    use std::{
        io,
        net::{Ipv4Addr, SocketAddr},
        sync::Arc,
        time::Duration,
    };

    #[test]
    fn cat_input_errors_print_without_the_unknown_error_prefix() {
        for error in [
            nisshi_cat::Error::NoMessages {
                location: nisshi_cat::Location::Stdin,
            },
            nisshi_cat::Error::NotAnArray {
                location: nisshi_cat::Location::File("messages.json".into()),
            },
        ] {
            assert_eq!(error.to_string(), cat_error_message(&error));
        }
    }

    #[test]
    fn other_cat_errors_keep_the_unknown_error_prefix() {
        let error = nisshi_cat::Error::Message("boom".into());

        assert_eq!(
            "Unknown error occurred during command: Message(\"boom\")",
            cat_error_message(&error)
        );
    }

    #[test]
    fn feature_not_enabled_message_names_the_storage_and_the_cargo_feature() {
        assert_eq!(
            "storage memory://nisshi/ needs the \"dynostore\" feature, which this build doesn't \
            have. Install a release binary, or reinstall with cargo install nisshi --features \
            dynostore",
            feature_not_enabled_message("memory", "memory://nisshi/")
        );

        for (scheme, feature) in [
            ("s3", "dynostore"),
            ("gs", "dynostore"),
            ("postgres", "postgres"),
            ("postgresql", "postgres"),
            ("sqlite", "libsql"),
            ("slatedb", "slatedb"),
            ("turso", "turso"),
        ] {
            assert!(
                feature_not_enabled_message(scheme, "storage://")
                    .contains(&format!("needs the \"{feature}\" feature")),
                "{scheme}"
            );
        }
    }

    #[test]
    fn feature_not_enabled_message_for_an_unknown_scheme_names_the_scheme() {
        assert_eq!(
            "storage foo://bar/ uses the foo:// scheme, which no storage engine in this build \
            supports",
            feature_not_enabled_message("foo", "foo://bar/")
        );
    }

    #[test]
    fn connect_failure_names_the_cause_and_the_option_that_gives_the_broker() {
        let cause = io::Error::from(io::ErrorKind::ConnectionRefused);
        let expected = format!(
            "Could not connect to the broker: {cause}. Check that it is running (nisshi broker) \
            and that --broker (or ADVERTISED_LISTENER_URL) points at it."
        );

        let error = nisshi_client::Error::Pool(Arc::new(Box::new(cause)));

        assert_eq!(expected, client_error_message(&error, BROKER_OPTION));
    }

    #[test]
    fn broker_that_closes_the_connection_gets_the_tls_and_authentication_hint() {
        let error =
            nisshi_client::Error::Io(Arc::new(io::Error::from(io::ErrorKind::UnexpectedEof)));

        let message = client_error_message(&error, BROKER_OPTION);

        assert!(
            message.starts_with("Connected to the broker, but it closed"),
            "{message}"
        );
        assert!(message.contains("--cert and --key"), "{message}");
        assert!(message.contains("--authentication"), "{message}");
    }

    #[test]
    fn request_failure_after_connecting_is_not_reported_as_a_connect_failure() {
        let error = nisshi_client::Error::Timeout(Duration::from_secs(30));

        assert_eq!(
            "The request to the broker failed: Timeout(30s)",
            client_error_message(&error, BROKER_OPTION)
        );
    }

    #[test]
    fn topic_connect_failure_names_only_the_option() {
        let error = nisshi_client::Error::Pool(Arc::new(Box::new(io::Error::from(
            io::ErrorKind::ConnectionRefused,
        ))));

        let message = client_error_message(&error, TOPIC_BROKER_OPTION);

        assert!(message.contains("that --broker points at it"), "{message}");
    }

    #[test]
    fn listener_in_use_message_names_the_address_and_the_fix() {
        assert_eq!(
            "127.0.0.1:9092 is already in use, so the broker can't listen on it. Stop the \
            process that uses it (perhaps another broker), or start this broker on another port, \
            with --listener-url and --advertised-listener-url both set to tcp://<host>:<port>",
            listener_in_use_message(SocketAddr::from((Ipv4Addr::LOCALHOST, 9092)))
        );
    }

    #[test]
    fn listener_host_unresolved_message_names_the_host_and_the_fix() {
        let source = io::Error::other("nodename nor servname provided, or not known");

        assert_eq!(
            "the --listener-url host eth1.internal could not be resolved: nodename nor servname \
            provided, or not known. Set --listener-url to an IP address, or to a host name that \
            this machine resolves, for example tcp://127.0.0.1:9093 for local clients only, or \
            tcp://<interface address>:9093",
            listener_host_unresolved_message("eth1.internal", 9093, &source)
        );
    }
}
