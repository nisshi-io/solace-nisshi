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

//! The exit status of a `nisshi` command.
//!
//! A script or a coding agent that drives a client tool (`topic`, `cat`, `user`, `generator`,
//! `perf`) tells "I called it wrong", "the broker isn't there" and "the broker said no" apart by
//! the exit status, without parsing the message on stderr. docs/commands.md documents the table.

use std::process::ExitCode;

use nisshi_client::FailureKind;
use nisshi_sans_io::ErrorCode;

use crate::Error;

/// How a `nisshi` command ended.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ExitStatus {
    /// The command did what it was asked to.
    Success = 0,

    /// The command failed for a reason that none of the statuses below names: an input file it
    /// can't read, a storage failure, a response it can't make sense of, a connection that the
    /// broker closed, or a request that got no answer in time. After a closed connection or a
    /// missing answer the broker may have applied the request, so check before you retry.
    Failure = 1,

    /// The command line is wrong: an unknown option, a missing argument, a value that doesn't
    /// parse, or a TLS or SASL option that can't be used. Nothing was sent to a broker. `clap`
    /// ends a command with this status on its own, so the two agree.
    Usage = 2,

    /// The command could not reach the broker: no connection opened. The broker may be down, or
    /// the address may be wrong.
    Unreachable = 3,

    /// The command connected, and the broker answered with a Kafka error code, refused the
    /// login, or rejected the TLS connection, or the client refused what the broker offered (a
    /// certificate it does not trust, a SCRAM iteration count out of range). The exit status
    /// does not say whether a retry can succeed: for an error code, read the code, which
    /// `--output json` prints.
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

    /// The status of a command that returned this error. The match has no wildcard arm, so a new
    /// variant of one of these errors does not compile until its author has chosen its status.
    pub fn of_error(error: &Error) -> Self {
        match error {
            Error::Client(error) => Self::of_client_error(error),
            Error::Cat(error) => match &**error {
                nisshi_cat::Error::Client(error) => Self::of_client_error(error),
                // The broker answered the fetch or the metadata lookup with an error code.
                nisshi_cat::Error::Fetch { .. } | nisshi_cat::Error::Metadata { .. } => {
                    Self::Refused
                }
                nisshi_cat::Error::InvalidJson { .. }
                | nisshi_cat::Error::Io(_)
                | nisshi_cat::Error::LinesCodec(_)
                | nisshi_cat::Error::Message(_)
                | nisshi_cat::Error::MessageWithoutKeyOrValue { .. }
                | nisshi_cat::Error::NoMessages { .. }
                | nisshi_cat::Error::NotAnArray { .. }
                | nisshi_cat::Error::Protocol(_)
                | nisshi_cat::Error::RecordOffsetOverflow { .. }
                | nisshi_cat::Error::Schema(_)
                | nisshi_cat::Error::SerdeJson(_) => Self::Failure,
            },
            Error::Generate(error) => match error {
                nisshi_generator::Error::Client(error) => Self::of_client_error(error),
                nisshi_generator::Error::Api(_) => Self::Refused,
                nisshi_generator::Error::ExporterBuild(_)
                | nisshi_generator::Error::InsufficientCapacity(_)
                | nisshi_generator::Error::Io(_)
                | nisshi_generator::Error::Otel(_)
                | nisshi_generator::Error::OtelSdk(_)
                | nisshi_generator::Error::Poison
                | nisshi_generator::Error::Protocol(_)
                | nisshi_generator::Error::Schema(_)
                | nisshi_generator::Error::SchemaNotFoundForTopic(_)
                | nisshi_generator::Error::UnknownHost(_) => Self::Failure,
                nisshi_generator::Error::Url(_) => Self::Usage,
            },
            Error::Perf(error) => match error {
                nisshi_perf::Error::Client(error) => Self::of_client_error(error),
                nisshi_perf::Error::Api(_) => Self::Refused,
                nisshi_perf::Error::ExporterBuild(_)
                | nisshi_perf::Error::InsufficientCapacity(_)
                | nisshi_perf::Error::Io(_)
                | nisshi_perf::Error::OtelSdk(_)
                | nisshi_perf::Error::Random(_)
                | nisshi_perf::Error::Poison
                | nisshi_perf::Error::Protocol(_)
                | nisshi_perf::Error::UnknownHost(_) => Self::Failure,
                nisshi_perf::Error::Url(_) => Self::Usage,
            },
            Error::Proxy(error) => match error {
                nisshi_proxy::Error::Client(error) => Self::of_client_error(error),
                nisshi_proxy::Error::ExporterBuild(_)
                | nisshi_proxy::Error::FrameTooBig(_)
                | nisshi_proxy::Error::Io(_)
                | nisshi_proxy::Error::Join(_)
                | nisshi_proxy::Error::Message(_)
                | nisshi_proxy::Error::Otel(_)
                | nisshi_proxy::Error::OtelSdk(_)
                | nisshi_proxy::Error::ParseFilter(_)
                | nisshi_proxy::Error::Protocol(_)
                | nisshi_proxy::Error::ResourceLock { .. }
                | nisshi_proxy::Error::Service(_)
                | nisshi_proxy::Error::UnknownHost(_) => Self::Failure,
            },
            Error::Topic(error) => match error {
                nisshi_topic::Error::Client(error) => Self::of_client_error(error),
                nisshi_topic::Error::Api(_) => Self::Refused,
                nisshi_topic::Error::Io(_)
                | nisshi_topic::Error::Message(_)
                | nisshi_topic::Error::Protocol(_)
                | nisshi_topic::Error::SerdeJson(_) => Self::Failure,
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
            | Error::UnsetVariable(_)
            | Error::Url(_) => Self::Usage,
            Error::Box(_)
            | Error::DotEnv(_)
            | Error::InvalidLength(_)
            | Error::Regex(_)
            | Error::SansIo(_)
            | Error::Schema(_)
            | Error::Server(_) => Self::Failure,
        }
    }

    /// The status of a failed Kafka client. `client_error_message` in `nisshi/src/bin/nisshi.rs`
    /// explains the same failure to the user, from the same [`nisshi_client::FailureKind`].
    pub fn of_client_error(error: &nisshi_client::Error) -> Self {
        match error.failure_kind() {
            FailureKind::Options => Self::Usage,
            FailureKind::Unreachable => Self::Unreachable,
            FailureKind::Refused => Self::Refused,
            FailureKind::Closed | FailureKind::TimedOut | FailureKind::Other => Self::Failure,
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
    use std::io;

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
    fn a_connection_that_does_not_open_is_unreachable() {
        for error in [
            client(nisshi_client::Error::Io(
                io::Error::from(io::ErrorKind::ConnectionRefused).into(),
            )),
            client(nisshi_client::Error::UnknownHost(
                "tcp://nowhere:9092".parse().expect("url"),
            )),
        ] {
            assert_eq!(
                ExitStatus::Unreachable,
                ExitStatus::of_error(&error),
                "{error}"
            );
        }
    }

    /// The request may have been applied, so the status makes no promise that a retry is safe.
    #[test]
    fn a_request_without_an_answer_is_a_plain_failure() {
        let timeout = client(nisshi_client::Error::Timeout(
            std::time::Duration::from_secs(30),
        ));
        assert_eq!(ExitStatus::Failure, ExitStatus::of_error(&timeout));
    }

    /// A closed connection ends in 1, and so does one that the pool wrapped while it opened the
    /// connection (`nisshi-client` tests that case).
    #[test]
    fn a_closed_connection_is_a_plain_failure() {
        let closed = client(nisshi_client::Error::Io(
            io::Error::from(io::ErrorKind::UnexpectedEof).into(),
        ));

        assert_eq!(ExitStatus::Failure, ExitStatus::of_error(&closed));
    }

    #[test]
    fn a_login_or_a_tls_alert_is_a_refusal() {
        for error in [
            client(nisshi_client::Error::SaslNotUsed),
            client(nisshi_client::Error::Authentication("bad password".into())),
            client(nisshi_client::Error::Tls("untrusted".into())),
            Error::Cat(Box::new(nisshi_cat::Error::Fetch {
                topic: "taxi".into(),
                partition: 0,
                error_code: 3,
            })),
            Error::Topic(nisshi_topic::Error::Api(ErrorCode::TopicAlreadyExists)),
            Error::Generate(nisshi_generator::Error::Api(
                ErrorCode::UnknownTopicOrPartition,
            )),
            Error::Perf(nisshi_perf::Error::Api(ErrorCode::UnknownTopicOrPartition)),
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
