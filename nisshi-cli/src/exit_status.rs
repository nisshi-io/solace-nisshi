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

//! The exit status of a `nisshi` command.
//!
//! A script or a coding agent that drives a client tool (`topic`, `cat`, `user`, `generator`,
//! `perf`) tells "I called it wrong", "the broker isn't there" and "the broker said no" apart by
//! the exit status, without parsing the message on stderr. docs/commands.md documents the table.

use std::{io, process::ExitCode};

use nisshi_sans_io::ErrorCode;

use crate::Error;

/// How a `nisshi` command ended.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ExitStatus {
    /// The command did what it was asked to.
    Success = 0,

    /// The command failed for a reason that none of the statuses below names: an input file it
    /// can't read, a storage failure, or a response it can't make sense of.
    Failure = 1,

    /// The command line is wrong: an unknown option, a missing argument, a value that doesn't
    /// parse, or a TLS or SASL option that can't be used. Nothing was sent to a broker. `clap`
    /// ends a command with this status on its own, so the two agree.
    Usage = 2,

    /// The command could not reach the broker: no connection opened, or the broker did not
    /// answer a request in time. The broker may be down, or the address may be wrong. Retrying
    /// later can succeed.
    Unreachable = 3,

    /// The command connected, and the connection or the request was refused: the broker
    /// answered with a Kafka error code, refused the login, rejected the TLS connection or
    /// closed the connection, or the client refused what the broker offered (a certificate it
    /// does not trust, a SCRAM iteration count out of range). Retrying the same command
    /// won't help: change the options or the request.
    Refused = 4,
}

impl ExitStatus {
    /// The number the process exits with.
    pub const fn code(self) -> u8 {
        self as u8
    }

    /// The status of a command that returned this Kafka error code in a broker's response.
    pub fn of_error_code(error_code: ErrorCode) -> Self {
        match error_code {
            ErrorCode::None => Self::Success,
            _ => Self::Refused,
        }
    }

    /// The status of a command that returned this error.
    pub fn of_error(error: &Error) -> Self {
        match error {
            Error::Client(error) => Self::of_client_error(error),
            Error::Cat(error) => match &**error {
                nisshi_cat::Error::Client(error) => Self::of_client_error(error),
                // The broker answered the fetch or the metadata lookup with an error code.
                nisshi_cat::Error::Fetch { .. } | nisshi_cat::Error::Metadata { .. } => {
                    Self::Refused
                }
                _ => Self::Failure,
            },
            Error::Generate(nisshi_generator::Error::Client(error))
            | Error::Perf(nisshi_perf::Error::Client(error))
            | Error::Proxy(nisshi_proxy::Error::Client(error)) => Self::of_client_error(error),
            Error::Topic(error) => match error {
                nisshi_topic::Error::Client(error) => Self::of_client_error(error),
                nisshi_topic::Error::Api(_) => Self::Refused,
                _ => Self::Failure,
            },
            Error::LakeRequiresSchemaRegistry { .. }
            | Error::SaslPasswordAmbiguous
            | Error::SaslPasswordFile { .. }
            | Error::SaslPasswordMissing
            | Error::SaslPasswordNotUnicode
            | Error::Tls(_)
            | Error::TlsCertificate { .. }
            | Error::TlsKeyDecrypt { .. }
            | Error::TlsKeyLegacyEncrypted { .. }
            | Error::TlsKeyPassphraseFile { .. }
            | Error::TlsKeyPassphraseRequired { .. }
            | Error::TlsKeyUnsupportedEncryption { .. }
            | Error::TlsPrivateKey { .. }
            | Error::TlsRequiresCertAndKey
            | Error::Url(_) => Self::Usage,
            _ => Self::Failure,
        }
    }

    /// The status of a failed Kafka client. Keep this in step with `client_error_message` in
    /// `nisshi/src/bin/nisshi.rs`, which explains the same failures to the user.
    pub fn of_client_error(error: &nisshi_client::Error) -> Self {
        if matches!(
            error,
            nisshi_client::Error::TlsConfiguration(_) | nisshi_client::Error::ParseUrl(_)
        ) {
            return Self::Usage;
        }

        if error.is_connect_failure() || matches!(error, nisshi_client::Error::Timeout(_)) {
            return Self::Unreachable;
        }

        let refused = error.scram_iterations().is_some()
            || error.sasl_not_used()
            || error.login_failure().is_some()
            || error.tls_failure().is_some()
            || error.tls_rejection().is_some()
            // A broker that serves only TLS, or that requires authentication, accepts the
            // connection and closes it when the plaintext request arrives.
            || matches!(
                error,
                nisshi_client::Error::Io(cause)
                    if matches!(
                        cause.kind(),
                        io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset
                    )
            );

        if refused {
            Self::Refused
        } else {
            Self::Failure
        }
    }
}

impl From<ExitStatus> for ExitCode {
    fn from(status: ExitStatus) -> Self {
        ExitCode::from(status.code())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_numbers_are_the_documented_ones() {
        assert_eq!(
            [0, 1, 2, 3, 4],
            [
                ExitStatus::Success,
                ExitStatus::Failure,
                ExitStatus::Usage,
                ExitStatus::Unreachable,
                ExitStatus::Refused
            ]
            .map(ExitStatus::code)
        );
    }

    #[test]
    fn a_kafka_error_code_is_a_refusal_and_none_is_success() {
        assert_eq!(
            ExitStatus::Success,
            ExitStatus::of_error_code(ErrorCode::None)
        );
        assert_eq!(
            ExitStatus::Refused,
            ExitStatus::of_error_code(ErrorCode::TopicAlreadyExists)
        );
    }

    fn client(error: nisshi_client::Error) -> Error {
        Error::from(error)
    }

    #[test]
    fn options_that_cannot_be_used_are_usage_errors() {
        for error in [
            client(nisshi_client::Error::TlsConfiguration("bad".into())),
            Error::SaslPasswordMissing,
            Error::TlsRequiresCertAndKey,
        ] {
            assert_eq!(ExitStatus::Usage, ExitStatus::of_error(&error), "{error}");
        }
    }

    #[test]
    fn a_connection_that_does_not_open_or_never_answers_is_unreachable() {
        let timeout = client(nisshi_client::Error::Timeout(
            std::time::Duration::from_secs(30),
        ));
        assert_eq!(ExitStatus::Unreachable, ExitStatus::of_error(&timeout));
    }

    #[test]
    fn a_login_a_tls_alert_or_a_closed_connection_is_a_refusal() {
        for error in [
            client(nisshi_client::Error::SaslNotUsed),
            client(nisshi_client::Error::Authentication("bad password".into())),
            client(nisshi_client::Error::Tls("untrusted".into())),
            client(nisshi_client::Error::Io(
                io::Error::from(io::ErrorKind::UnexpectedEof).into(),
            )),
            Error::Cat(Box::new(nisshi_cat::Error::Fetch {
                topic: "taxi".into(),
                partition: 0,
                error_code: 3,
            })),
            Error::Topic(nisshi_topic::Error::Api(ErrorCode::TopicAlreadyExists)),
        ] {
            assert_eq!(ExitStatus::Refused, ExitStatus::of_error(&error), "{error}");
        }
    }

    #[test]
    fn a_refused_login_ends_generator_and_perf_with_4() {
        let login = || nisshi_client::Error::Authentication("bad password".into());

        for error in [
            Error::Generate(nisshi_generator::Error::Client(login())),
            Error::Perf(nisshi_perf::Error::Client(login())),
        ] {
            assert_eq!(ExitStatus::Refused, ExitStatus::of_error(&error), "{error}");
        }
    }

    #[test]
    fn anything_else_is_a_plain_failure() {
        for error in [
            client(nisshi_client::Error::Message("boom".into())),
            Error::Cat(Box::new(nisshi_cat::Error::Message("boom".into()))),
        ] {
            assert_eq!(ExitStatus::Failure, ExitStatus::of_error(&error), "{error}");
        }
    }
}
