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

use crate::{Error, Result, ScramCredential, Storage, user_credentials_unsupported_message};
use bytes::Bytes;
use nisshi_sans_io::{
    AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse, ApiKey, ErrorCode,
    RequestInput, ScramMechanism,
    alter_user_scram_credentials_response::AlterUserScramCredentialsResult,
};
use rama::Service;
use rsasl::mechanisms::scram::tools::derive_keys;
use sha2::{Digest, Sha256, Sha512};
use tracing::{debug, instrument, warn};

/// The answer for `user` once storage has upserted or deleted its credential.
///
/// An engine that can't store credentials is answered `UNSUPPORTED_VERSION` with a message
/// that names the engine, as Kafka answers each user when its cluster can't store SCRAM
/// credentials. Kafka's clients show that message to the user.
fn result(user: String, outcome: Result<()>) -> AlterUserScramCredentialsResult {
    let (error_code, error_message) = match outcome {
        Ok(()) => (ErrorCode::None, String::new()),

        Err(Error::UserCredentialsUnsupported { engine }) => (
            ErrorCode::UnsupportedVersion,
            user_credentials_unsupported_message(&engine),
        ),

        Err(error) => {
            warn!(?error, user, "storage failed to alter a SCRAM credential");
            (ErrorCode::UnsupportedSaslMechanism, String::new())
        }
    };

    AlterUserScramCredentialsResult::default()
        .user(user)
        .error_code(error_code.into())
        .error_message(Some(error_message))
}

#[derive(Clone, Debug)]
pub struct AlterUserScramCredentialsService<G> {
    pub storage: G,
}

impl<G> ApiKey for AlterUserScramCredentialsService<G> {
    const KEY: i16 = AlterUserScramCredentialsRequest::KEY;
}

impl<G, I> Service<I> for AlterUserScramCredentialsService<G>
where
    G: Storage,
    I: Into<RequestInput<AlterUserScramCredentialsRequest>> + Send + 'static,
{
    type Output = AlterUserScramCredentialsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let mut results = vec![];

        let input = input.into();

        if let Some(deletions) = input.request.deletions {
            for deletion in deletions {
                let mechanism = match ScramMechanism::try_from(deletion.mechanism) {
                    Ok(mechanism) => mechanism,
                    Err(error) => {
                        warn!(
                            ?error,
                            mechanism = deletion.mechanism,
                            "invalid scram mechanism, answering UNSUPPORTED_SASL_MECHANISM"
                        );

                        results.push(
                            AlterUserScramCredentialsResult::default()
                                .user(deletion.name.clone())
                                .error_code(ErrorCode::UnsupportedSaslMechanism.into())
                                .error_message(Some("Unknown SCRAM mechanism".into())),
                        );

                        continue;
                    }
                };

                results.push(result(
                    deletion.name.clone(),
                    self.storage
                        .delete_user_scram_credential(&deletion.name, mechanism)
                        .await,
                ));
            }
        }

        if let Some(upsertions) = input.request.upsertions {
            for upsertion in upsertions {
                let mechanism = match ScramMechanism::try_from(upsertion.mechanism)
                    .inspect(|mechanism| debug!(?mechanism))
                {
                    Ok(mechanism) => mechanism,
                    Err(error) => {
                        warn!(
                            ?error,
                            mechanism = upsertion.mechanism,
                            "invalid scram mechanism, answering UNSUPPORTED_SASL_MECHANISM"
                        );

                        results.push(
                            AlterUserScramCredentialsResult::default()
                                .user(upsertion.name.clone())
                                .error_code(ErrorCode::UnsupportedSaslMechanism.into())
                                .error_message(Some("Unknown SCRAM mechanism".into())),
                        );

                        continue;
                    }
                };

                let (stored_key, server_key) = if mechanism == ScramMechanism::Scram256 {
                    let (client_key, server_key) =
                        derive_keys::<Sha256>(&upsertion.salted_password);

                    (
                        Bytes::copy_from_slice(&Sha256::digest(client_key)[..]),
                        Bytes::copy_from_slice(&server_key[..]),
                    )
                } else {
                    let (client_key, server_key) =
                        derive_keys::<Sha512>(&upsertion.salted_password);

                    (
                        Bytes::copy_from_slice(&Sha512::digest(client_key)[..]),
                        Bytes::copy_from_slice(&server_key[..]),
                    )
                };

                let credential = ScramCredential {
                    salt: upsertion.salt,
                    iterations: upsertion.iterations,
                    stored_key,
                    server_key,
                };

                results.push(result(
                    upsertion.name.clone(),
                    self.storage
                        .upsert_user_scram_credential(
                            upsertion.name.as_str(),
                            mechanism,
                            credential,
                        )
                        .await,
                ));
            }
        }

        Ok(AlterUserScramCredentialsResponse::default()
            .throttle_time_ms(0)
            .results(Some(results)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_is_none() -> Result<()> {
        let answered = result("alice".into(), Ok(()));

        assert_eq!("alice", answered.user);
        assert_eq!(ErrorCode::None, ErrorCode::try_from(answered.error_code)?);
        Ok(())
    }

    #[test]
    fn unsupported_engine_is_unsupported_version_naming_the_engine() -> Result<()> {
        let answered = result(
            "alice".into(),
            Err(Error::UserCredentialsUnsupported {
                engine: "in-memory (memory://)".into(),
            }),
        );

        assert_eq!(
            ErrorCode::UnsupportedVersion,
            ErrorCode::try_from(answered.error_code)?
        );
        assert_eq!(
            Some(
                "The in-memory (memory://) storage engine can't store SCRAM credentials, so \
                nothing was changed. SCRAM users need PostgreSQL or SQLite storage."
            ),
            answered.error_message.as_deref()
        );
        Ok(())
    }

    /// Any other storage failure is answered as before this engine check existed.
    #[test]
    fn other_failure_is_unchanged() -> Result<()> {
        let answered = result("alice".into(), Err(Error::Message("broken".into())));

        assert_eq!(
            ErrorCode::UnsupportedSaslMechanism,
            ErrorCode::try_from(answered.error_code)?
        );
        assert_eq!(Some(""), answered.error_message.as_deref());
        Ok(())
    }
}
