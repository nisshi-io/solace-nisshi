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

//! The TLS and SASL options that every tool connecting to a broker shares.

use std::{env, fs, path::PathBuf};

use clap::{Args, ValueEnum};
use nisshi_client::{Sasl, SaslMechanism, Security, Tls, TlsOptions};
use zeroize::Zeroizing;

use super::broker::{KeyOptions, load_certs, load_passphrase, load_private_key};
use crate::{Error, Result};

/// The environment variable that holds the SASL password, as an alternative to
/// `--sasl-password-file`. There is no option for the password itself, because an option lands
/// in the shell history and in the process list.
const SASL_PASSWORD: &str = "SASL_PASSWORD";

const CLIENT_KEY_OPTIONS: KeyOptions = KeyOptions {
    key: "--tls-key-file",
    passphrase_file: "--tls-key-passphrase-file",
};

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, PartialOrd, Ord, ValueEnum)]
pub(super) enum Mechanism {
    /// PLAIN
    Plain,

    /// SCRAM-SHA-256
    #[value(name = "scram-sha-256")]
    ScramSha256,

    /// SCRAM-SHA-512
    #[default]
    #[value(name = "scram-sha-512")]
    ScramSha512,
}

impl From<Mechanism> for SaslMechanism {
    fn from(value: Mechanism) -> Self {
        match value {
            Mechanism::Plain => Self::Plain,
            Mechanism::ScramSha256 => Self::ScramSha256,
            Mechanism::ScramSha512 => Self::ScramSha512,
        }
    }
}

/// How the tool secures its connection to the broker: TLS, SASL, or both, which is Kafka's
/// `SASL_SSL`. Without any of these options the connection is plaintext and not logged in.
#[derive(Args, Clone, Debug, Default)]
pub(super) struct ClientSecurity {
    /// Connect over TLS, trusting the system's CA certificates (set SSL_CERT_FILE or SSL_CERT_DIR
    /// to trust others). Every other --tls-* option also turns TLS on
    #[arg(long)]
    tls: bool,

    /// PEM file of the CA certificates that sign the broker's certificate, in place of the
    /// system's
    #[arg(long, env = "TLS_CA_FILE")]
    tls_ca_file: Option<PathBuf>,

    /// PEM file of the client certificate chain, for a broker that asks for one, requires
    /// --tls-key-file
    #[arg(long, env = "TLS_CERT_FILE")]
    tls_cert_file: Option<PathBuf>,

    /// PEM file of the client private key (PKCS#8, SEC1 or RSA; encrypted PKCS#8 with
    /// --tls-key-passphrase-file), requires --tls-cert-file
    #[arg(long, env = "TLS_KEY_FILE")]
    tls_key_file: Option<PathBuf>,

    /// File with the passphrase of an encrypted private key (trailing newline ignored), requires
    /// --tls-key-file
    #[arg(long, env = "TLS_KEY_PASSPHRASE_FILE")]
    tls_key_passphrase_file: Option<PathBuf>,

    /// Check the broker's certificate against this name, in place of the host of the broker URL
    #[arg(long, env = "TLS_SERVER_NAME")]
    tls_server_name: Option<String>,

    /// Accept a broker certificate that is not valid for the broker's host name. The
    /// certificate chain is still checked. Kafka's empty ssl.endpoint.identification.algorithm
    #[arg(long)]
    tls_no_hostname_check: bool,

    /// Log in with SASL as this user, requires a password in the SASL_PASSWORD environment
    /// variable or in --sasl-password-file
    #[arg(long, env = "SASL_USERNAME")]
    sasl_username: Option<String>,

    /// File with the SASL password (trailing newline ignored), in place of the SASL_PASSWORD
    /// environment variable, requires --sasl-username
    #[arg(long, env = "SASL_PASSWORD_FILE")]
    sasl_password_file: Option<PathBuf>,

    /// The SASL mechanism to log in with, requires --sasl-username
    #[arg(long, env = "SASL_MECHANISM", value_enum, ignore_case = true)]
    sasl_mechanism: Option<Mechanism>,
}

impl ClientSecurity {
    fn tls_enabled(&self) -> bool {
        self.tls
            || self.tls_ca_file.is_some()
            || self.tls_cert_file.is_some()
            || self.tls_key_file.is_some()
            || self.tls_key_passphrase_file.is_some()
            || self.tls_server_name.is_some()
            || self.tls_no_hostname_check
    }

    /// Whether the name check is off while the system's CA certificates sign the broker's
    /// certificate, so that a certificate which a public CA issued for any name passes.
    fn accepts_any_public_certificate(&self) -> bool {
        self.tls_no_hostname_check && self.tls_ca_file.is_none()
    }

    fn tls_config(&self) -> Result<Option<Tls>> {
        if !self.tls_enabled() {
            return Ok(None);
        }

        let ca_certificates = self
            .tls_ca_file
            .as_deref()
            .map(|ca| load_certs(ca, "--tls-ca-file"))
            .transpose()?
            .unwrap_or_default();

        let client_identity = match (self.tls_cert_file.as_deref(), self.tls_key_file.as_deref()) {
            (Some(cert), Some(key)) => {
                let passphrase = load_passphrase(
                    self.tls_key_passphrase_file.as_deref(),
                    CLIENT_KEY_OPTIONS.passphrase_file,
                )?;

                Some((
                    load_certs(cert, "--tls-cert-file")?,
                    load_private_key(
                        key,
                        passphrase.as_deref().map(Vec::as_slice),
                        CLIENT_KEY_OPTIONS,
                    )?,
                ))
            }
            (None, None) => None,
            // Not enforced by clap, because these options also come from the environment, and
            // a `requires` would reject a `.env` that sets one for tools that don't use TLS.
            _ => return Err(Error::TlsRequiresCertAndKey),
        };

        Tls::new(TlsOptions {
            ca_certificates,
            client_identity,
            server_name: self.tls_server_name.clone(),
            verify_hostname: !self.tls_no_hostname_check,
        })
        .map(Some)
        .map_err(Into::into)
    }

    fn sasl_config(&self) -> Result<Option<Sasl>> {
        let Some(username) = self.sasl_username.as_deref() else {
            return Ok(None);
        };

        let password = match (
            self.sasl_password_file.as_deref(),
            env::var_os(SASL_PASSWORD),
        ) {
            (Some(_), Some(_)) => return Err(Error::SaslPasswordAmbiguous),
            (Some(path), None) => fs::read_to_string(path)
                .map(Zeroizing::new)
                .map(|password| Zeroizing::new(password.trim_end_matches(['\n', '\r']).to_owned()))
                .map_err(|cause| Error::SaslPasswordFile {
                    path: path.to_path_buf(),
                    cause,
                })?,
            (None, Some(password)) => password
                .into_string()
                .map(Zeroizing::new)
                .map_err(|_| Error::SaslPasswordNotUnicode)?,
            (None, None) => return Err(Error::SaslPasswordMissing),
        };

        Ok(Some(Sasl::new(
            self.sasl_mechanism.unwrap_or_default().into(),
            username,
            password.as_str(),
        )))
    }

    /// The security that the options ask for. It fails before any connection is attempted, when
    /// a file can't be read or is not what the option names.
    pub(super) fn build(&self) -> Result<Security> {
        let security = Security::default();

        if self.sasl_username.is_some()
            && self.sasl_mechanism == Some(Mechanism::Plain)
            && !self.tls_enabled()
        {
            eprintln!(
                "warning: SASL PLAIN without TLS sends the password in clear text; add --tls"
            );
        }

        if self.accepts_any_public_certificate() {
            eprintln!(
                "warning: --tls-no-hostname-check without --tls-ca-file accepts a certificate that \
                 any public CA issued for any name; add --tls-ca-file"
            );
        }

        let security = match self.tls_config()? {
            Some(tls) => security.tls(tls),
            None => security,
        };

        Ok(match self.sasl_config()? {
            Some(sasl) => security.sasl(sasl),
            None => security,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use clap::{CommandFactory as _, Parser};
    use nisshi_client::{Sasl, SaslMechanism, Security};

    use super::{ClientSecurity, Mechanism, SASL_PASSWORD};
    use crate::Error;

    /// Every environment variable that the options read, so that no test sees the developer's
    /// `.env`, which the justfile loads.
    const ENVIRONMENT: [&str; 9] = [
        SASL_PASSWORD,
        "SASL_USERNAME",
        "SASL_PASSWORD_FILE",
        "SASL_MECHANISM",
        "TLS_CA_FILE",
        "TLS_CERT_FILE",
        "TLS_KEY_FILE",
        "TLS_KEY_PASSPHRASE_FILE",
        "TLS_SERVER_NAME",
    ];

    /// Runs `run` with each variable in `ENVIRONMENT` unset, except the ones in `set`.
    fn with_environment<T>(set: &[(&str, &str)], run: impl FnOnce() -> T) -> T {
        temp_env::with_vars(
            ENVIRONMENT.map(|name| {
                (
                    name,
                    set.iter()
                        .find(|(variable, _)| *variable == name)
                        .map(|(_, value)| *value),
                )
            }),
            run,
        )
    }

    #[derive(Parser)]
    struct Wrapper {
        #[command(flatten)]
        security: ClientSecurity,
    }

    fn parse_arguments(args: &[&str]) -> Result<ClientSecurity, clap::Error> {
        Wrapper::try_parse_from(std::iter::once("tool").chain(args.iter().copied()))
            .map(|wrapper| wrapper.security)
    }

    fn parse(args: &[&str]) -> Result<ClientSecurity, clap::Error> {
        with_environment(&[], || parse_arguments(args))
    }

    fn build(args: &[&str], password: Option<&str>) -> crate::Result<Security> {
        let set: Vec<(&str, &str)> = password
            .map(|password| (SASL_PASSWORD, password))
            .into_iter()
            .collect();

        with_environment(&set, || {
            parse_arguments(args).expect("arguments parse").build()
        })
    }

    fn sasl(mechanism: SaslMechanism, password: &str) -> Security {
        Security::default().sasl(Sasl::new(mechanism, "alice", password))
    }

    #[test]
    fn no_options_connect_in_plaintext() {
        assert_eq!(Security::default(), build(&[], None).unwrap());
        // A password in the environment does nothing without a user.
        assert_eq!(Security::default(), build(&[], Some("secret")).unwrap());
    }

    #[test]
    fn the_password_comes_from_the_environment() {
        assert_eq!(
            sasl(SaslMechanism::ScramSha512, "secret"),
            build(&["--sasl-username", "alice"], Some("secret")).unwrap()
        );
    }

    #[test]
    fn the_password_comes_from_a_file_without_its_trailing_newline() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("password");
        fs::write(&file, "secret\r\n").unwrap();

        assert_eq!(
            sasl(SaslMechanism::ScramSha256, "secret"),
            build(
                &[
                    "--sasl-username",
                    "alice",
                    "--sasl-password-file",
                    file.to_str().unwrap(),
                    "--sasl-mechanism",
                    "scram-sha-256",
                ],
                None
            )
            .unwrap()
        );
    }

    #[test]
    fn the_password_is_in_one_place() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("password");
        fs::write(&file, "secret").unwrap();

        let args = [
            "--sasl-username",
            "alice",
            "--sasl-password-file",
            file.to_str().unwrap(),
        ];

        assert!(matches!(
            build(&args, Some("secret")),
            Err(Error::SaslPasswordAmbiguous)
        ));
        assert!(matches!(
            build(&["--sasl-username", "alice"], None),
            Err(Error::SaslPasswordMissing)
        ));
        assert!(matches!(
            build(
                &[
                    "--sasl-username",
                    "alice",
                    "--sasl-password-file",
                    "/no/such/file"
                ],
                None
            ),
            Err(Error::SaslPasswordFile { .. })
        ));
    }

    #[test]
    fn there_is_no_option_for_the_password() {
        assert!(parse(&["--sasl-username", "alice", "--sasl-password", "secret"]).is_err());
    }

    #[test]
    fn a_mechanism_or_password_file_without_a_user_is_ignored() {
        // A `.env` file can set them for every tool, including one that logs in to nothing.
        assert_eq!(
            Security::default(),
            build(&["--sasl-mechanism", "plain"], Some("secret")).unwrap()
        );
    }

    #[test]
    fn any_tls_option_turns_tls_on() {
        assert_ne!(Security::default(), build(&["--tls"], None).unwrap());
        assert_ne!(
            Security::default(),
            build(&["--tls-no-hostname-check"], None).unwrap()
        );
    }

    /// The message must send the user to the flag they gave, not the broker's `--cert`.
    #[test]
    fn a_bad_tls_file_error_names_the_client_option() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("ca.pem");
        fs::write(&file, "not a certificate").unwrap();

        let message = build(&["--tls-ca-file", file.to_str().unwrap()], None)
            .expect_err("a file with no certificate")
            .to_string();

        assert!(message.ends_with("(--tls-ca-file)."), "{message}");
        assert!(!message.contains("--cert"), "{message}");
    }

    /// Each client option, not just the CA file, must reach its message.
    #[test]
    fn every_tls_file_error_names_the_client_option_that_was_given() {
        let directory = tempfile::tempdir().unwrap();
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(["localhost".to_owned()]).unwrap();
        let good_cert = directory.path().join("cert.pem");
        fs::write(&good_cert, cert.pem()).unwrap();
        let good_key = directory.path().join("key.pem");
        fs::write(&good_key, signing_key.serialize_pem()).unwrap();
        let garbage = directory.path().join("garbage.pem");
        fs::write(&garbage, "not a pem file").unwrap();
        let encrypted = directory.path().join("encrypted.pem");
        fs::write(
            &encrypted,
            "-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIBvTBXBgkqhkiG9w0BBQ0wSjApBgkqhkiG9w0BBQwwHAQI\n-----END ENCRYPTED PRIVATE KEY-----\n",
        )
        .unwrap();
        let missing = directory.path().join("missing.txt");

        let message = |cert: &Path, key: &Path, passphrase: Option<&Path>| {
            let mut args = vec![
                "--tls-cert-file",
                cert.to_str().unwrap(),
                "--tls-key-file",
                key.to_str().unwrap(),
            ];
            if let Some(passphrase) = passphrase {
                args.extend(["--tls-key-passphrase-file", passphrase.to_str().unwrap()]);
            }
            build(&args, None).expect_err("a bad file").to_string()
        };

        let text = message(&garbage, &good_key, None);
        assert!(text.ends_with("(--tls-cert-file)."), "{text}");
        let text = message(&good_cert, &garbage, None);
        assert!(text.ends_with("(--tls-key-file)."), "{text}");
        let text = message(&good_cert, &encrypted, None);
        assert!(
            text.ends_with("pass --tls-key-passphrase-file <file>."),
            "{text}"
        );
        let text = message(&good_cert, &encrypted, Some(&missing));
        assert!(text.ends_with("(--tls-key-passphrase-file)."), "{text}");
    }

    #[test]
    fn a_file_that_is_not_what_the_option_names_is_refused() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("ca.pem");
        fs::write(&file, "not a certificate").unwrap();

        assert!(matches!(
            build(&["--tls-ca-file", file.to_str().unwrap()], None),
            Err(Error::TlsCertificate { .. })
        ));
        assert!(matches!(
            build(&["--tls-ca-file", "/no/such/file"], None),
            Err(Error::TlsCertificate { .. })
        ));
    }

    #[test]
    fn the_test_environment_covers_every_variable_that_an_option_reads() {
        let options = Wrapper::command()
            .get_arguments()
            .filter_map(|argument| argument.get_env())
            .map(|name| name.to_str().unwrap().to_owned())
            .collect::<Vec<_>>();

        assert!(!options.is_empty());

        for name in options {
            assert!(ENVIRONMENT.contains(&name.as_str()), "{name}");
        }
    }

    #[test]
    fn a_mechanism_may_be_spelled_as_kafka_spells_it() {
        for (spelling, mechanism) in [
            ("PLAIN", Mechanism::Plain),
            ("SCRAM-SHA-256", Mechanism::ScramSha256),
            ("SCRAM-SHA-512", Mechanism::ScramSha512),
        ] {
            assert_eq!(
                Some(mechanism),
                parse(&["--sasl-mechanism", spelling])
                    .unwrap()
                    .sasl_mechanism
            );

            // A `.env` file that the other Kafka tools read sets it by the variable.
            assert_eq!(
                Some(mechanism),
                with_environment(&[("SASL_MECHANISM", spelling)], || {
                    parse_arguments(&[]).unwrap().sasl_mechanism
                })
            );
        }
    }

    #[test]
    fn no_hostname_check_warns_unless_a_ca_file_limits_the_trust() {
        assert!(
            parse(&["--tls-no-hostname-check"])
                .unwrap()
                .accepts_any_public_certificate()
        );
        assert!(
            !parse(&["--tls-no-hostname-check", "--tls-ca-file", "ca.pem"])
                .unwrap()
                .accepts_any_public_certificate()
        );
        assert!(!parse(&["--tls"]).unwrap().accepts_any_public_certificate());
    }

    #[test]
    fn a_client_certificate_needs_its_key() {
        assert!(matches!(
            build(&["--tls-cert-file", "cert.pem"], None),
            Err(Error::TlsRequiresCertAndKey)
        ));
    }
}
