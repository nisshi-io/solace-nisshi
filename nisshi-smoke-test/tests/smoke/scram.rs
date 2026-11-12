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

//! Tests that a user can create SCRAM users with `kafka-configs`, and that clients log in as them.
//! On a broker with `--authentication`, a client that can't log in can't do anything, and a broker
//! that lets in a client with a wrong password or none protects nothing.

/// Creating and describing users, on a broker that doesn't require a login. Only PostgreSQL and
/// SQLite store users: the other engines answer each user with an error.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
mod user_creation {
    use nisshi_smoke_test::{KafkaCli, ScramMechanism, unique};

    /// `kafka-configs --add-config` creates credentials with both mechanisms, and `kafka-configs
    /// --describe` must list both for the user. A user checks the list before giving a client
    /// the mechanism to log in with.
    #[test]
    #[ignore = "kafka-configs describes users' quotas too, with DescribeClientQuotas, which the \
                broker doesn't support (#922)"]
    fn kafka_configs_user_is_described_with_both_mechanisms() {
        let cli = KafkaCli::shared();
        let user = unique::name("user");
        _ = cli
            .configs()
            .add_scram_user(&user, &unique::password())
            .succeeded();

        let described = cli.configs().describe_users();

        assert_eq!(
            described
                .succeeded()
                .scram_mechanisms(&user)
                .map(|mut mechanisms| {
                    mechanisms.sort_unstable();
                    mechanisms
                }),
            Some(ScramMechanism::ALL.map(ScramMechanism::name).to_vec()),
            "{described}"
        );
    }
}

/// Logging in, on a broker with `--authentication`.
///
/// A broker with `--authentication` refuses to create a user for a client that hasn't logged in,
/// so each test creates its user on a broker without `--authentication`, and restarts the broker
/// with it. Each test therefore also checks that the user, and its password, survive a restart.
/// Only PostgreSQL and SQLite keep the user across that restart, so the tests run on them.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
mod login {
    use std::time::Duration;

    use nisshi_smoke_test::{
        Broker, KafkaCli, SASL_AUTHENTICATION_EXCEPTION, SASL_MECHANISM_NOT_ENABLED, ScramLogin,
        ScramMechanism, unique,
    };

    fn login_for_new_user(mechanism: ScramMechanism) -> ScramLogin {
        ScramLogin {
            user: unique::name("user"),
            password: unique::password(),
            mechanism,
        }
    }

    /// Creates `login`'s user with `kafka-configs` on a new broker, and restarts the broker with
    /// `--authentication`.
    #[track_caller]
    fn broker_requiring_kafka_configs_user(login: &ScramLogin) -> Broker {
        let broker = Broker::isolated();
        _ = KafkaCli::new(broker.bootstrap())
            .configs()
            .add_scram_user(&login.user, &login.password)
            .succeeded();

        broker.restart_requiring_login(login)
    }

    /// Asserts that a client that logs in as `login` can list the broker's topics.
    #[track_caller]
    fn assert_logs_in(broker: &Broker, login: &ScramLogin) {
        let listed = KafkaCli::logged_in_as(broker.bootstrap(), login)
            .topics()
            .list();

        assert_eq!(
            listed.code,
            Some(0),
            "{} can't log in with {}: {listed}",
            login.user,
            login.mechanism.name()
        );
    }

    /// A client must be able to log in with SCRAM-SHA-256 as a user that `kafka-configs` created.
    /// A broker that can't check those credentials locks every SCRAM-SHA-256 client out.
    #[test]
    fn kafka_configs_user_logs_in_with_scram_sha_256() {
        let login = login_for_new_user(ScramMechanism::Sha256);
        let broker = broker_requiring_kafka_configs_user(&login);

        assert_logs_in(&broker, &login);
    }

    /// As for SCRAM-SHA-256, with SCRAM-SHA-512, which uses another hash function and so other
    /// credentials.
    #[test]
    fn kafka_configs_user_logs_in_with_scram_sha_512() {
        let login = login_for_new_user(ScramMechanism::Sha512);
        let broker = broker_requiring_kafka_configs_user(&login);

        assert_logs_in(&broker, &login);
    }

    /// The broker must refuse a client that logs in with the wrong password, and the tool must
    /// report why. A broker that accepts any password lets anyone log in as the user.
    #[test]
    fn wrong_password_is_refused() {
        let login = login_for_new_user(ScramMechanism::Sha256);
        let broker = broker_requiring_kafka_configs_user(&login);
        let wrong_login = ScramLogin {
            password: unique::password(),
            ..login
        };

        let listed = KafkaCli::logged_in_as(broker.bootstrap(), &wrong_login)
            .topics()
            .list();

        assert!(
            listed.code != Some(0) && listed.mentions(SASL_AUTHENTICATION_EXCEPTION),
            "the broker didn't refuse the wrong password: {listed}"
        );
    }

    /// The broker must refuse a client without credentials, and keep serving the clients that log
    /// in. A broker that serves the client lets anyone use the broker without a password.
    ///
    /// The broker closes the connection of a client that sends a request before it logs in, and
    /// the client retries until its API timeout. A broker that stops answering every client also
    /// makes the tool fail, so the test then checks that a client that logs in is still served.
    #[test]
    fn client_without_credentials_is_refused() {
        /// Shorter than the Java client's default API timeout of 60 seconds, so that the tool
        /// gives up on its own, before the harness stops it.
        const CLIENT_API_TIMEOUT: Duration = Duration::from_secs(5);

        let login = login_for_new_user(ScramMechanism::Sha256);
        let broker = broker_requiring_kafka_configs_user(&login);

        let listed = KafkaCli::new(broker.bootstrap())
            .with_api_timeout(CLIENT_API_TIMEOUT)
            .topics()
            .list();

        assert!(
            listed.timeout.is_none() && listed.code != Some(0),
            "the broker served a client without credentials: {listed}"
        );
        assert_logs_in(&broker, &login);
    }

    /// The broker must refuse a client that logs in with a mechanism that it hasn't enabled, and
    /// the tool must fail at once, naming the broker's mechanisms. A broker that drops the
    /// connection without an answer leaves the client retrying until its API timeout, with an
    /// error that doesn't point at the mechanism.
    #[test]
    fn plain_login_is_refused_at_once() {
        /// Long enough that a tool which fails only on its API timeout stands out from one that the
        /// broker refused.
        const CLIENT_API_TIMEOUT: Duration = Duration::from_secs(30);

        let login = login_for_new_user(ScramMechanism::Sha256);
        let broker = broker_requiring_kafka_configs_user(&login);

        let listed =
            KafkaCli::logged_in_with_plain(broker.bootstrap(), &login.user, &login.password)
                .with_api_timeout(CLIENT_API_TIMEOUT)
                .topics()
                .list();

        assert!(
            listed.timeout.is_none()
                && listed.code != Some(0)
                && listed.mentions(SASL_MECHANISM_NOT_ENABLED),
            "the broker didn't refuse the PLAIN login: {listed}"
        );
        assert_logs_in(&broker, &login);
    }
}
