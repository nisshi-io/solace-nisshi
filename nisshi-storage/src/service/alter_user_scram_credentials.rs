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
    RequestInput, ScramMechanism, alter_user_scram_credentials_request::ScramCredentialUpsertion,
    alter_user_scram_credentials_response::AlterUserScramCredentialsResult,
};
use rama::Service;
use rsasl::mechanisms::scram::tools::derive_keys;
use sha2::{Digest, Sha256, Sha512};
use std::collections::{BTreeMap, btree_map::Entry};
use tracing::{debug, instrument, warn};

/// The smallest iteration count in a SCRAM-SHA-256 or SCRAM-SHA-512 credential that
/// [Kafka 3.9.1 accepts][kafka-minimum].
///
/// [kafka-minimum]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/security/scram/internals/ScramMechanism.java#L32-L33
pub const MIN_SCRAM_ITERATIONS: i32 = 4096;

/// The largest iteration count in a SCRAM-SHA-256 or SCRAM-SHA-512 credential that
/// [Kafka 3.9.1 accepts][kafka-maximum].
///
/// [kafka-maximum]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ScramControlManager.java#L58
pub const MAX_SCRAM_ITERATIONS: i32 = 16384;

/// Returns Kafka's message for an iteration count outside `MIN_SCRAM_ITERATIONS` to
/// `MAX_SCRAM_ITERATIONS`, and `None` for a count inside it.
fn unacceptable_iterations_message(iterations: i32) -> Option<&'static str> {
    if iterations < MIN_SCRAM_ITERATIONS {
        Some("Too few iterations")
    } else if iterations > MAX_SCRAM_ITERATIONS {
        Some("Too many iterations")
    } else {
        None
    }
}

/// Returns, for each user, the message for the first upsertion of that user that has a known
/// mechanism and an iteration count outside Kafka's range.
///
/// Kafka orders its checks as empty name, unknown mechanism, then iteration count, so an item
/// with an unknown mechanism keeps its mechanism error and never counts here.
fn iterations_refusals(upsertions: &[ScramCredentialUpsertion]) -> BTreeMap<String, &'static str> {
    let mut refusals = BTreeMap::new();

    for upsertion in upsertions {
        let Ok(mechanism) = ScramMechanism::try_from(upsertion.mechanism) else {
            continue;
        };

        let Some(message) = unacceptable_iterations_message(upsertion.iterations) else {
            continue;
        };

        if let Entry::Vacant(refusal) = refusals.entry(upsertion.name.clone()) {
            warn!(
                user = upsertion.name.as_str(),
                ?mechanism,
                iterations = upsertion.iterations,
                "iteration count out of range, answering UNACCEPTABLE_CREDENTIAL"
            );

            _ = refusal.insert(message);
        }
    }

    refusals
}

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
            // [Kafka 3.9.1 refuses][kafka-range-check] an iteration count outside its range, and
            // then stores none of the upsertions of that user in the request. So this loop
            // refuses each upsertion of such a user with the message of the first refusal, and
            // stores the upsertions of every other user.
            //
            // [kafka-range-check]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ScramControlManager.java#L284-L296
            //
            // Two differences from Kafka remain. Kafka answers once per user, and this loop
            // answers once per upsertion. Kafka answers DUPLICATE_RESOURCE for a user that the
            // request names more than once, and runs none of that user's items. This loop
            // handles such a user as follows:
            //
            // - A deletion and an out-of-range upsertion: this service deletes the credential
            //   and refuses the new one, so a delete-then-add rotation with a bad count ends
            //   with no credential.
            // - A valid upsertion and a refused one: this service answers both with the refused
            //   message.
            // - Two valid upsertions with different mechanisms: this service stores both.
            let refusals = iterations_refusals(&upsertions);

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

                if let Some(message) = refusals.get(&upsertion.name) {
                    results.push(
                        AlterUserScramCredentialsResult::default()
                            .user(upsertion.name.clone())
                            .error_code(ErrorCode::UnacceptableCredential.into())
                            .error_message(Some((*message).into())),
                    );

                    continue;
                }

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
mod tests;
