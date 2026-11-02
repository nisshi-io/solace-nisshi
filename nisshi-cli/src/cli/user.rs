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

use crate::Result;
use bytes::{Bytes, BytesMut};
use clap::{Subcommand, ValueEnum};
use nisshi_client::{Client, ConnectionManager};
use nisshi_sans_io::{
    AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse, ErrorCode, ScramMechanism,
    alter_user_scram_credentials_request::{ScramCredentialDeletion, ScramCredentialUpsertion},
};
use pbkdf2::{hmac::Hmac, pbkdf2};
use rand::{Rng as _, rng};
use sha2::{Sha256, Sha512};
use std::io::{self, Write};
use tracing::debug;
use url::Url;

use super::{DEFAULT_BROKER, Outcome};

#[derive(Clone, Debug, Subcommand)]
pub(super) enum Command {
    /// Create a user
    Create {
        /// Broker URL
        #[arg(long, default_value = DEFAULT_BROKER)]
        broker: Url,

        /// The name of the user
        name: String,

        /// Password
        password: String,

        // Iterations
        #[arg(long, default_value = "8192")]
        iterations: Option<u32>,

        // Mode
        #[arg(long, value_enum, default_value = "scram512")]
        mechanism: Mechanism,
    },

    /// Delete a user
    Delete {
        /// Broker URL
        #[arg(long, default_value = DEFAULT_BROKER)]
        broker: Url,

        /// The name of the user
        name: String,

        // Mode
        #[arg(long, value_enum, default_value = "scram512")]
        mechanism: Mechanism,
    },
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, PartialOrd, Ord, ValueEnum)]
pub(super) enum Mechanism {
    /// Scram 256
    Scram256,

    /// Scram 512
    #[default]
    Scram512,
}

impl Mechanism {
    fn salted_password(&self, password: &[u8], iterations: u32, salt: &[u8]) -> Result<Bytes> {
        match self {
            Mechanism::Scram256 => {
                let mut buf = BytesMut::zeroed(32);
                pbkdf2::<Hmac<Sha256>>(password, salt, iterations, &mut buf)?;
                Ok(buf.into())
            }
            Mechanism::Scram512 => {
                let mut buf = BytesMut::zeroed(64);
                pbkdf2::<Hmac<Sha512>>(password, salt, iterations, &mut buf)?;
                Ok(buf.into())
            }
        }
    }
}

impl From<&Mechanism> for i8 {
    fn from(value: &Mechanism) -> Self {
        match value {
            Mechanism::Scram256 => ScramMechanism::Scram256.into(),
            Mechanism::Scram512 => ScramMechanism::Scram512.into(),
        }
    }
}

impl Command {
    const DEFAULT_SALT_LEN: usize = 32;
    const DEFAULT_ITERATIONS: u32 = 2u32.pow(14);

    fn broker(&self) -> Url {
        match self {
            Command::Create { broker, .. } => broker.to_owned(),
            Command::Delete { broker, .. } => broker.to_owned(),
        }
    }

    fn upsertions(&self) -> Option<Vec<ScramCredentialUpsertion>> {
        match self {
            Command::Create {
                name,
                password,
                iterations,
                mechanism,
                ..
            } => {
                let mut salt = BytesMut::zeroed(Self::DEFAULT_SALT_LEN);
                rng().fill_bytes(&mut salt);

                let iterations = iterations.unwrap_or(Self::DEFAULT_ITERATIONS);

                mechanism
                    .salted_password(password.as_bytes(), iterations, &salt[..])
                    .ok()
                    .map(|salted_password| {
                        [ScramCredentialUpsertion::default()
                            .name(name.into())
                            .mechanism(mechanism.into())
                            .iterations(iterations as i32)
                            .salt(salt.into())
                            .salted_password(salted_password)]
                        .into()
                    })
            }

            _ => Some([].into()),
        }
    }

    fn deletions(&self) -> Option<Vec<ScramCredentialDeletion>> {
        match self {
            Command::Create { .. } => Some([].into()),
            Command::Delete {
                name,
                mechanism: mode,
                ..
            } => Some(
                [ScramCredentialDeletion::default()
                    .name(name.into())
                    .mechanism(mode.into())]
                .into(),
            ),
        }
    }

    pub(super) async fn main(self) -> Result<Outcome> {
        let client = ConnectionManager::builder(self.broker())
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        let req = AlterUserScramCredentialsRequest::default()
            .deletions(self.deletions())
            .upsertions(self.upsertions());

        let response = client
            .call(req)
            .await
            .inspect(|response| debug!(?response))?;

        Ok(Outcome {
            error_code: report(&response, &mut io::stderr()),
            reported: true,
        })
    }
}

/// Writes a line to `out` for each user that the broker failed to alter, with the broker's
/// message, and returns the first failure's code, or [`ErrorCode::None`] when every user was
/// altered.
///
/// Each line starts with `error: `, as the error line of every other failed command does, so a
/// script finds both kinds of failure by the same prefix.
fn report(response: &AlterUserScramCredentialsResponse, out: &mut impl Write) -> ErrorCode {
    let mut first = ErrorCode::None;

    for result in response.results.as_deref().unwrap_or_default() {
        let error_code =
            ErrorCode::try_from(result.error_code).unwrap_or(ErrorCode::UnknownServerError);

        if error_code == ErrorCode::None {
            continue;
        }

        match result.error_message.as_deref() {
            Some(message) if !message.is_empty() => {
                _ = writeln!(
                    out,
                    "error: user {}: {message} ({error_code:?})",
                    result.user
                );
            }
            _ => {
                _ = writeln!(
                    out,
                    "error: user {}: {error_code} ({error_code:?})",
                    result.user
                )
            }
        }

        if first == ErrorCode::None {
            first = error_code;
        }
    }

    first
}

#[cfg(test)]
mod tests {
    use super::*;
    use nisshi_sans_io::alter_user_scram_credentials_response::AlterUserScramCredentialsResult;

    fn result(user: &str, error_code: ErrorCode, message: &str) -> AlterUserScramCredentialsResult {
        AlterUserScramCredentialsResult::default()
            .user(user.into())
            .error_code(error_code.into())
            .error_message(Some(message.into()))
    }

    fn reported(results: Vec<AlterUserScramCredentialsResult>) -> (ErrorCode, String) {
        let mut out = Vec::new();
        let error_code = report(
            &AlterUserScramCredentialsResponse::default().results(Some(results)),
            &mut out,
        );
        (error_code, String::from_utf8_lossy(&out).into_owned())
    }

    #[test]
    fn report_success_writes_nothing() {
        assert_eq!(
            (ErrorCode::None, String::new()),
            reported(vec![result("alice", ErrorCode::None, "")])
        );
    }

    #[test]
    fn report_writes_the_broker_message_and_returns_the_first_failure() {
        let (error_code, out) = reported(vec![
            result("alice", ErrorCode::None, ""),
            result(
                "bob",
                ErrorCode::UnsupportedVersion,
                "can't store SCRAM credentials",
            ),
            result("carol", ErrorCode::UnsupportedSaslMechanism, ""),
        ]);

        assert_eq!(ErrorCode::UnsupportedVersion, error_code);
        assert_eq!(
            format!(
                "error: user bob: can't store SCRAM credentials (UnsupportedVersion)\n\
                error: user carol: {} (UnsupportedSaslMechanism)\n",
                ErrorCode::UnsupportedSaslMechanism
            ),
            out
        );
    }

    #[test]
    fn test_salted_password_256() -> Result<()> {
        let password = "password";
        let salt = b"abcdef";
        let iterations = 1000;
        let mechanism = Mechanism::Scram256;

        let result = mechanism.salted_password(password.as_bytes(), iterations, salt)?;
        assert_eq!(
            [
                145, 219, 38, 255, 206, 134, 237, 218, 6, 231, 82, 1, 148, 149, 161, 210, 185, 243,
                46, 76, 94, 133, 112, 217, 144, 162, 201, 91, 29, 255, 5, 19
            ],
            &result[..]
        );
        Ok(())
    }

    #[test]
    fn test_salted_password_512() -> Result<()> {
        let password = "password";
        let salt = b"abcdef";
        let iterations = 1000;
        let mechanism = Mechanism::Scram512;

        let result = mechanism.salted_password(password.as_bytes(), iterations, salt)?;
        assert_eq!(
            [
                154, 35, 153, 145, 17, 161, 139, 24, 204, 40, 101, 29, 139, 51, 136, 125, 228, 84,
                18, 240, 169, 203, 123, 34, 18, 167, 45, 226, 1, 215, 102, 172, 150, 75, 234, 71,
                238, 187, 194, 46, 5, 38, 119, 248, 202, 110, 44, 39, 62, 227, 46, 210, 208, 201,
                180, 183, 91, 212, 148, 57, 64, 96, 115, 175
            ],
            &result[..]
        );
        Ok(())
    }
}
