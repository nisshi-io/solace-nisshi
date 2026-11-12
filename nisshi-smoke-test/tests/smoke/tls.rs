// Copyright (c) Solace Corporation
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

//! Tests of a broker that serves TLS and requires a SCRAM login, as a user deploys it. The broker
//! must serve a client that connects with TLS and logs in. It must refuse a client that connects
//! without TLS, because that client sends its password and its records in plaintext.

// Each test creates its user on a broker that doesn't require a login, and then restarts the
// broker with `--authentication`. Only PostgreSQL and SQLite store users.
#![cfg(any(feature = "postgres", feature = "sqlite"))]

use std::time::Duration;

use nisshi_smoke_test::{
    Broker, KafkaCli, LaunchOptions, ScramLogin, ScramMechanism, TlsCertificate, settings, unique,
};

/// A broker that serves TLS with its own certificate, and requires a login as `login`.
struct TlsBrokerRequiringLogin {
    broker: Broker,
    certificate: TlsCertificate,
    login: ScramLogin,
}

impl TlsBrokerRequiringLogin {
    /// Launches a broker that serves TLS, creates a SCRAM-SHA-256 user on it with `kafka-configs`
    /// over TLS, and restarts the broker with `--authentication`.
    #[track_caller]
    fn launch() -> Self {
        let certificate = TlsCertificate::self_signed_for_localhost();
        let login = ScramLogin {
            user: unique::name("user"),
            password: unique::password(),
            mechanism: ScramMechanism::Sha256,
        };

        let mut options = LaunchOptions::new(settings::storage_url_under_test());
        options.tls = Some(certificate.clone());
        let broker = Broker::launch(options);

        _ = KafkaCli::new(broker.bootstrap())
            .over_tls(&certificate)
            .configs()
            .add_scram_user(&login.user, &login.password)
            .succeeded();

        Self {
            broker: broker.restart_requiring_login(&login),
            certificate,
            login,
        }
    }

    /// Returns tools that connect with TLS and log in as the broker's user.
    #[track_caller]
    fn logged_in_cli_over_tls(&self) -> KafkaCli {
        KafkaCli::logged_in_as(self.broker.bootstrap(), &self.login).over_tls(&self.certificate)
    }
}

/// A user who logs in with SCRAM over TLS must be able to create a topic, produce to it and consume
/// from it. A broker that can't serve a SCRAM login over TLS leaves the user a choice between
/// plaintext and no access.
#[test]
fn scram_user_produces_and_consumes_over_tls() {
    let tls_broker = TlsBrokerRequiringLogin::launch();
    let cli = tls_broker.logged_in_cli_over_tls();
    let topic = cli.topics().create_unique(1, &[]);

    _ = cli
        .console_producer()
        .produce_values(&topic, &["over-tls-0", "over-tls-1"]);
    let consumed = cli
        .console_consumer()
        .consume(&topic, &unique::name("group"), 2);

    assert_eq!(
        consumed.succeeded().consumed_values(),
        ["over-tls-0", "over-tls-1"],
        "{consumed}"
    );
}

/// The broker must refuse a client that connects without TLS, even with the right password, and
/// keep serving the clients that connect with TLS. A broker that serves the plaintext client lets
/// that client's password and records cross the network in plaintext.
///
/// The client retries until its API timeout, because the broker closes each connection that
/// doesn't start a TLS handshake. A broker that stops answering every client also makes the tool
/// fail, so the test then checks that the broker still serves a client over TLS.
#[test]
fn plaintext_client_with_the_right_password_is_refused() {
    /// Shorter than the Java client's default API timeout of 60 seconds, so that the tool gives up
    /// on its own, before the harness stops it.
    const CLIENT_API_TIMEOUT: Duration = Duration::from_secs(5);

    let tls_broker = TlsBrokerRequiringLogin::launch();

    let listed_in_plaintext =
        KafkaCli::logged_in_as(tls_broker.broker.bootstrap(), &tls_broker.login)
            .with_api_timeout(CLIENT_API_TIMEOUT)
            .topics()
            .list();

    assert!(
        listed_in_plaintext.timeout.is_none(),
        "the tool didn't give up within its API timeout: {listed_in_plaintext}"
    );
    assert_ne!(
        listed_in_plaintext.code,
        Some(0),
        "the broker served a client without TLS: {listed_in_plaintext}"
    );

    let listed_over_tls = tls_broker.logged_in_cli_over_tls().topics().list();

    assert_eq!(
        listed_over_tls.code,
        Some(0),
        "the broker refused a client over TLS after it refused a plaintext client: \
         {listed_over_tls}"
    );
}
