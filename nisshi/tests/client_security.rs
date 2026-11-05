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

//! `nisshi user`, `topic`, `cat`, `generator` and `perf` against a broker whose listener serves
//! TLS, and against one that serves TLS and requires a SASL SCRAM login (SASL_SSL).

#![cfg(all(feature = "dynostore", feature = "libsql"))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::{
    fs,
    path::{Path, PathBuf},
    process::Output,
};

use common::{Broker, stdout};
use tempfile::TempDir;

const USERNAME: &str = "alice";
const PASSWORD: &str = "correct horse battery staple";

/// A self-signed certificate for `localhost` and `127.0.0.1`, which is the CA of the tools too.
struct Certificate {
    directory: TempDir,
}

impl Certificate {
    fn new() -> Self {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(["localhost".to_owned(), "127.0.0.1".to_owned()])
                .expect("certificate");

        let directory = tempfile::tempdir().expect("temporary directory");
        fs::write(directory.path().join("cert.pem"), cert.pem()).expect("certificate written");
        fs::write(
            directory.path().join("key.pem"),
            signing_key.serialize_pem(),
        )
        .expect("key written");

        Self { directory }
    }

    fn cert(&self) -> PathBuf {
        self.directory.path().join("cert.pem")
    }

    fn key(&self) -> PathBuf {
        self.directory.path().join("key.pem")
    }

    /// The `nisshi broker` options that serve TLS.
    fn broker_args(&self) -> Vec<String> {
        vec![
            "--cert".into(),
            path(&self.cert()),
            "--key".into(),
            path(&self.key()),
        ]
    }

    /// The tool option that trusts the broker's certificate.
    fn ca_args(&self) -> Vec<String> {
        vec!["--tls-ca-file".into(), path(&self.cert())]
    }
}

fn path(path: &Path) -> String {
    path.display().to_string()
}

fn strs(args: &[String]) -> Vec<&str> {
    args.iter().map(String::as_str).collect()
}

/// A broker that serves TLS on `memory://`, without SASL.
fn tls_broker(certificate: &Certificate) -> Broker {
    Broker::start_on("memory://nisshi/", &strs(&certificate.broker_args()))
}

/// What a tool needs to log in over TLS: the CA of the broker, and the user, whose password
/// comes from a file.
struct Login {
    certificate: Certificate,
    directory: TempDir,
}

impl Login {
    fn password_file(&self) -> PathBuf {
        self.directory.path().join("password")
    }

    fn args(&self) -> Vec<String> {
        let mut args = self.certificate.ca_args();
        args.extend([
            "--sasl-username".into(),
            USERNAME.into(),
            "--sasl-password-file".into(),
            path(&self.password_file()),
            "--sasl-mechanism".into(),
            "scram-sha-256".into(),
        ]);
        args
    }
}

/// A broker that serves TLS and requires SCRAM-SHA-256 logins on SQLite, with the user `alice`.
///
/// A broker with `--authentication` accepts no request before a login, so the user is created
/// on a broker that serves TLS without `--authentication`, over the same database file, which
/// is then restarted with it.
fn sasl_ssl_broker() -> (Broker, Login) {
    let login = Login {
        certificate: Certificate::new(),
        directory: tempfile::tempdir().expect("temporary directory"),
    };

    // A trailing newline, as an editor leaves, is not part of the password.
    fs::write(login.password_file(), format!("{PASSWORD}\n")).expect("password written");

    let database = login.directory.path().join("nisshi.db");
    let storage = format!("sqlite:///{}", database.display());

    let mut broker_args = login.certificate.broker_args();

    let bootstrap = Broker::start_on(&storage, &strs(&broker_args));

    let mut args = vec![
        "user".to_owned(),
        "create".into(),
        USERNAME.into(),
        PASSWORD.into(),
        "--mechanism".into(),
        "scram256".into(),
    ];
    args.extend(login.certificate.ca_args());

    // Creating the user is also `nisshi user` over TLS.
    _ = stdout(&bootstrap.run(&strs(&args), ""));

    // The broker stores a credential for each mechanism, so the default one (SCRAM-SHA-512)
    // needs its own.
    args[5] = "scram512".into();
    _ = stdout(&bootstrap.run(&strs(&args), ""));
    drop(bootstrap);

    broker_args.push("--authentication".into());

    (Broker::start_on(&storage, &strs(&broker_args)), login)
}

fn with(args: &[&str], security: &[String]) -> Vec<String> {
    args.iter()
        .map(|arg| (*arg).to_owned())
        .chain(security.iter().cloned())
        .collect()
}

/// Runs `args` with `security` added, and returns its stdout. Panics if it failed.
fn succeeds(broker: &Broker, args: &[&str], security: &[String], stdin: &str) -> String {
    let output = broker.run(&strs(&with(args, security)), stdin);
    stdout(&output)
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// Every tool, one after another, against `broker`, which needs `security` to be reached. Each
/// step depends on the one before it, so a step that failed to reach the broker fails the next.
fn every_tool_works(broker: &Broker, security: &[String]) {
    // `generator` takes the schema of the topic from the registry. A `file://` registry is
    // relative to the directory that the command runs in, which is the temporary one.
    let schemas = tempfile::Builder::new()
        .prefix("schemas-")
        .tempdir_in(std::env::temp_dir())
        .expect("schema directory");
    _ = fs::copy(
        concat!(env!("CARGO_MANIFEST_DIR"), "/../etc/schema/taxi.proto"),
        schemas.path().join("taxi.proto"),
    )
    .expect("schema copied");
    let registry = format!(
        "file://./{}",
        schemas
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .expect("directory name")
    );

    _ = succeeds(
        broker,
        &["topic", "create", "taxi", "--partitions", "1"],
        security,
        "",
    );
    _ = succeeds(
        broker,
        &["topic", "create", "notes", "--partitions", "1"],
        security,
        "",
    );

    assert!(
        succeeds(broker, &["topic", "list"], security, "").contains("taxi"),
        "topic list"
    );

    _ = succeeds(
        broker,
        &["cat", "produce", "notes"],
        security,
        "{\"value\": \"hello\"}\n",
    );
    assert_eq!(
        "[{\"key\":null,\"value\":\"hello\"}]\n",
        succeeds(broker, &["cat", "consume", "notes"], security, ""),
    );

    let generated = broker.run(
        &strs(&with(
            &[
                "generator",
                "taxi",
                "--schema-registry",
                &registry,
                "--partition",
                "0",
                "--per-second",
                "5",
                "--duration-seconds",
                "1",
            ],
            security,
        )),
        "",
    );
    assert!(generated.status.success(), "generator: {generated:?}");

    assert!(
        !succeeds(broker, &["cat", "consume", "taxi"], security, "").is_empty(),
        "generator wrote no messages: {generated:?}"
    );

    // `perf` takes `--broker` before its subcommand.
    let mut perf = vec!["perf".to_owned(), "notes".into()];
    perf.extend(security.iter().cloned());
    perf.extend(
        [
            "--broker",
            broker.url(),
            "produce",
            "--duration",
            "1s",
            "--record-size",
            "100",
        ]
        .map(str::to_owned),
    );
    _ = stdout(&broker.run_unaided(&strs(&perf), &[]));

    _ = succeeds(broker, &["topic", "delete", "notes"], security, "");
    assert!(
        !succeeds(broker, &["topic", "list"], security, "").contains("notes"),
        "topic delete"
    );
}

#[test]
fn every_tool_reaches_a_tls_listener() {
    let certificate = Certificate::new();
    let broker = tls_broker(&certificate);

    every_tool_works(&broker, &certificate.ca_args());
}

#[test]
fn every_tool_reaches_a_sasl_ssl_listener() {
    let (broker, login) = sasl_ssl_broker();

    every_tool_works(&broker, &login.args());

    // `user` logs in too: it is how a user creates the next one.
    let mut args = vec![
        "user",
        "create",
        "bob",
        "bob-secret",
        "--mechanism",
        "scram256",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    args.extend(login.args());

    _ = stdout(&broker.run(&strs(&args), ""));
}

/// The password can come from `SASL_PASSWORD` instead of a file, and the user and mechanism
/// from the environment too.
#[test]
fn the_login_comes_from_the_environment() {
    let (broker, login) = sasl_ssl_broker();
    let ca = login.certificate.ca_args();

    let output = broker.run_with_env(
        &strs(&with(&["topic", "list"], &ca)),
        "",
        &[
            ("SASL_USERNAME", USERNAME),
            ("SASL_PASSWORD", PASSWORD),
            ("SASL_MECHANISM", "scram-sha-256"),
        ],
    );

    _ = stdout(&output);
}

/// `--sasl-mechanism` defaults to SCRAM-SHA-512, as `nisshi user create --mechanism` does.
#[test]
fn the_default_mechanism_is_scram_sha_512() {
    let (broker, login) = sasl_ssl_broker();

    let output = broker.run_with_env(
        &strs(&with(
            &["topic", "list", "--sasl-username", USERNAME],
            &login.certificate.ca_args(),
        )),
        "",
        &[("SASL_PASSWORD", PASSWORD)],
    );

    _ = stdout(&output);
}

#[test]
fn a_wrong_password_is_reported_as_a_refused_login() {
    let (broker, login) = sasl_ssl_broker();
    let ca = login.certificate.ca_args();

    let output = broker.run_with_env(
        &strs(&with(&["topic", "list", "--sasl-username", USERNAME], &ca)),
        "",
        &[("SASL_PASSWORD", "wrong")],
    );

    assert_eq!(Some(4), output.status.code(), "{output:?}");
    assert!(
        stderr(&output)
            .lines()
            .any(|line| line.starts_with("error: The broker refused the login: ")),
        "{output:?}"
    );
    assert!(!stderr(&output).contains("wrong"), "{output:?}");
}

#[test]
fn the_password_comes_from_one_place() {
    let (broker, login) = sasl_ssl_broker();

    let output = broker.run_with_env(
        &strs(&with(&["topic", "list"], &login.args())),
        "",
        &[("SASL_PASSWORD", PASSWORD)],
    );

    assert_eq!(Some(2), output.status.code(), "{output:?}");
    assert!(
        stderr(&output).contains("Both --sasl-password-file and the SASL_PASSWORD"),
        "{output:?}"
    );

    let output = broker.run(
        &strs(&with(
            &["topic", "list", "--sasl-username", USERNAME],
            &login.certificate.ca_args(),
        )),
        "",
    );

    assert_eq!(Some(2), output.status.code(), "{output:?}");
    assert!(
        stderr(&output).contains("--sasl-username needs a password"),
        "{output:?}"
    );
}

#[test]
fn a_tool_without_tls_is_told_how_to_connect() {
    let certificate = Certificate::new();
    let broker = tls_broker(&certificate);

    let output = broker.run(&["topic", "list"], "");

    assert!(!output.status.success(), "{output:?}");
    assert!(
        stderr(&output).lines().any(|line| {
            line.starts_with("error: Connected to the broker, but it closed the connection")
                && line.contains("--tls")
        }),
        "{output:?}"
    );
}

#[test]
fn a_tool_that_does_not_trust_the_broker_certificate_is_told_how_to_trust_it() {
    let certificate = Certificate::new();
    let broker = tls_broker(&certificate);

    // The system's CA certificates don't include the test certificate.
    let output = broker.run(&["topic", "list", "--tls"], "");

    assert_eq!(Some(4), output.status.code(), "{output:?}");
    assert!(
        stderr(&output).lines().any(|line| {
            line.starts_with("error: The TLS handshake with the broker failed: ")
                && line.contains("UnknownIssuer")
                && line.contains("--tls-ca-file")
        }),
        "{output:?}"
    );
}

#[test]
fn a_certificate_for_another_name_is_refused_unless_the_name_check_is_off() {
    let certificate = Certificate::new();
    let broker = tls_broker(&certificate);
    let ca = certificate.ca_args();

    let output = broker.run(
        &strs(&with(
            &["topic", "list", "--tls-server-name", "other.example"],
            &ca,
        )),
        "",
    );

    assert_eq!(Some(4), output.status.code(), "{output:?}");
    assert!(
        stderr(&output).contains("not valid for name \"other.example\""),
        "{output:?}"
    );

    _ = stdout(&broker.run(
        &strs(&with(
            &[
                "topic",
                "list",
                "--tls-server-name",
                "other.example",
                "--tls-no-hostname-check",
            ],
            &ca,
        )),
        "",
    ));
}

#[test]
fn tls_against_a_plaintext_listener_says_so() {
    let broker = Broker::start();

    let output = broker.run(&["topic", "list", "--tls"], "");

    assert_eq!(Some(4), output.status.code(), "{output:?}");
    assert!(
        stderr(&output).starts_with("error: The TLS handshake with the broker failed: "),
        "{output:?}"
    );
}

/// A broker without SASL answers a login with `ILLEGAL_SASL_STATE`.
#[test]
fn a_login_to_a_listener_without_sasl_says_so() {
    let certificate = Certificate::new();
    let broker = tls_broker(&certificate);

    let output = broker.run_with_env(
        &strs(&with(
            &["topic", "list", "--sasl-username", USERNAME],
            &certificate.ca_args(),
        )),
        "",
        &[("SASL_PASSWORD", PASSWORD)],
    );

    assert_eq!(Some(4), output.status.code(), "{output:?}");
    assert!(stderr(&output).contains("does not use SASL"), "{output:?}");
}
