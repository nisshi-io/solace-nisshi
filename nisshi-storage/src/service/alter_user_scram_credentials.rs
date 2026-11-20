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
use std::collections::BTreeMap;
use tracing::{debug, instrument, warn};

/// The smallest iteration count in a SCRAM-SHA-256 or SCRAM-SHA-512 credential that
/// [Kafka 3.9.1 accepts][kafka-minimum].
///
/// [kafka-minimum]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/security/scram/internals/ScramMechanism.java#L32-L33
const MIN_SCRAM_ITERATIONS: i32 = 4096;

/// The largest iteration count in a SCRAM-SHA-256 or SCRAM-SHA-512 credential that
/// [Kafka 3.9.1 accepts][kafka-maximum].
///
/// [kafka-maximum]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ScramControlManager.java#L58
const MAX_SCRAM_ITERATIONS: i32 = 16384;

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
        if ScramMechanism::try_from(upsertion.mechanism).is_err() {
            continue;
        }

        if let Some(message) = unacceptable_iterations_message(upsertion.iterations) {
            warn!(
                iterations = upsertion.iterations,
                "iteration count out of range, answering UNACCEPTABLE_CREDENTIAL"
            );

            _ = refusals.entry(upsertion.name.clone()).or_insert(message);
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
            // Three differences from Kafka remain. Kafka answers once per user, and this loop
            // answers once per upsertion. Kafka answers DUPLICATE_RESOURCE for a user that the
            // request names twice, and this loop does not. Kafka refuses a deletion for a refused
            // user, and this loop runs it.
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
mod tests {
    use super::*;
    use crate::storage_double::storage_double;
    use rama::extensions::Extensions;

    const SCRAM_SHA_256: i8 = 1;
    const SCRAM_SHA_512: i8 = 2;
    const UNKNOWN_MECHANISM: i8 = 111;
    const MECHANISMS: [i8; 2] = [SCRAM_SHA_256, SCRAM_SHA_512];

    /// A storage double that stores nothing. A call to `upsert_user_scram_credential` or
    /// `delete_user_scram_credential` panics, so a test that makes the service call storage
    /// fails.
    #[derive(Clone, Debug)]
    struct Unreachable;

    storage_double!(Unreachable, async fn fetch(&self, _fetched_sender) {
        unimplemented!()
    });

    fn upsertion(name: &str, mechanism: i8, iterations: i32) -> ScramCredentialUpsertion {
        ScramCredentialUpsertion::default()
            .name(name.into())
            .mechanism(mechanism)
            .iterations(iterations)
            .salt(Bytes::from_static(b"salt"))
            .salted_password(Bytes::from_static(b"salted password"))
    }

    /// Serves a request with the given upsertions on a storage that panics if the service calls
    /// it.
    async fn serve(
        upsertions: Vec<ScramCredentialUpsertion>,
    ) -> Result<Vec<AlterUserScramCredentialsResult>> {
        let response = AlterUserScramCredentialsService {
            storage: Unreachable,
        }
        .serve(RequestInput {
            request: AlterUserScramCredentialsRequest::default()
                .deletions(Some(vec![]))
                .upsertions(Some(upsertions)),
            extensions: Extensions::default(),
        })
        .await?;

        Ok(response.results.unwrap_or_default())
    }

    fn assert_refused(
        answered: &AlterUserScramCredentialsResult,
        user: &str,
        message: &str,
    ) -> Result<()> {
        assert_eq!(user, answered.user);
        assert_eq!(
            ErrorCode::UnacceptableCredential,
            ErrorCode::try_from(answered.error_code)?
        );
        assert_eq!(Some(message), answered.error_message.as_deref());
        Ok(())
    }

    #[test]
    fn counts_below_the_minimum_are_too_few() {
        for iterations in [4095, 1, 0, -1, i32::MIN] {
            assert_eq!(
                Some("Too few iterations"),
                unacceptable_iterations_message(iterations),
                "{iterations}"
            );
        }
    }

    #[test]
    fn counts_above_the_maximum_are_too_many() {
        for iterations in [16385, 65536, i32::MAX] {
            assert_eq!(
                Some("Too many iterations"),
                unacceptable_iterations_message(iterations),
                "{iterations}"
            );
        }
    }

    #[test]
    fn counts_from_the_minimum_to_the_maximum_are_acceptable() {
        for iterations in [4096, 4097, 8192, 16383, 16384] {
            assert_eq!(
                None,
                unacceptable_iterations_message(iterations),
                "{iterations}"
            );
        }
    }

    #[tokio::test]
    async fn too_few_iterations_are_refused_before_storage_for_each_mechanism() -> Result<()> {
        for mechanism in MECHANISMS {
            for iterations in [4095, 0, -1, i32::MIN] {
                let results = serve(vec![upsertion("alice", mechanism, iterations)]).await?;

                assert_eq!(1, results.len());
                assert_refused(&results[0], "alice", "Too few iterations")?;
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn too_many_iterations_are_refused_before_storage_for_each_mechanism() -> Result<()> {
        for mechanism in MECHANISMS {
            for iterations in [16385, i32::MAX] {
                let results = serve(vec![upsertion("alice", mechanism, iterations)]).await?;

                assert_eq!(1, results.len());
                assert_refused(&results[0], "alice", "Too many iterations")?;
            }
        }
        Ok(())
    }

    /// Each user is refused with the message for that user's own count.
    #[tokio::test]
    async fn each_user_is_refused_for_its_own_count() -> Result<()> {
        let results = serve(vec![
            upsertion("alice", SCRAM_SHA_256, 4095),
            upsertion("bob", SCRAM_SHA_512, 16385),
        ])
        .await?;

        assert_eq!(2, results.len());
        assert_refused(&results[0], "alice", "Too few iterations")?;
        assert_refused(&results[1], "bob", "Too many iterations")?;
        Ok(())
    }

    /// Kafka stores nothing for a user when any one of the user's upsertions is refused.
    #[tokio::test]
    async fn a_refused_upsertion_refuses_the_valid_upsertion_after_it() -> Result<()> {
        let results = serve(vec![
            upsertion("alice", SCRAM_SHA_256, 20000),
            upsertion("alice", SCRAM_SHA_512, 8192),
        ])
        .await?;

        assert_eq!(2, results.len());
        assert_refused(&results[0], "alice", "Too many iterations")?;
        assert_refused(&results[1], "alice", "Too many iterations")?;
        Ok(())
    }

    #[tokio::test]
    async fn a_refused_upsertion_refuses_the_valid_upsertion_before_it() -> Result<()> {
        let results = serve(vec![
            upsertion("alice", SCRAM_SHA_512, 8192),
            upsertion("alice", SCRAM_SHA_256, 20000),
        ])
        .await?;

        assert_eq!(2, results.len());
        assert_refused(&results[0], "alice", "Too many iterations")?;
        assert_refused(&results[1], "alice", "Too many iterations")?;
        Ok(())
    }

    /// Every upsertion of the user gets the message of the user's first refused upsertion.
    #[tokio::test]
    async fn a_user_with_two_refused_upsertions_gets_the_first_message_for_both() -> Result<()> {
        let results = serve(vec![
            upsertion("alice", SCRAM_SHA_256, 100),
            upsertion("alice", SCRAM_SHA_512, 20000),
        ])
        .await?;

        assert_eq!(2, results.len());
        assert_refused(&results[0], "alice", "Too few iterations")?;
        assert_refused(&results[1], "alice", "Too few iterations")?;
        Ok(())
    }

    /// The mechanism is checked before the count, as Kafka does.
    #[tokio::test]
    async fn an_unknown_mechanism_keeps_its_error_when_its_count_is_also_refused() -> Result<()> {
        let results = serve(vec![upsertion("alice", UNKNOWN_MECHANISM, 4095)]).await?;

        assert_eq!(1, results.len());
        assert_eq!("alice", results[0].user);
        assert_eq!(
            ErrorCode::UnsupportedSaslMechanism,
            ErrorCode::try_from(results[0].error_code)?
        );
        assert_eq!(
            Some("Unknown SCRAM mechanism"),
            results[0].error_message.as_deref()
        );
        Ok(())
    }

    /// An item with an unknown mechanism keeps its mechanism error, and its count does not
    /// change the answer for the same user's other items.
    #[tokio::test]
    async fn an_unknown_mechanism_item_keeps_its_own_error_beside_a_refused_item() -> Result<()> {
        let results = serve(vec![
            upsertion("alice", UNKNOWN_MECHANISM, 20000),
            upsertion("alice", SCRAM_SHA_256, 4095),
        ])
        .await?;

        assert_eq!(2, results.len());
        assert_eq!(
            ErrorCode::UnsupportedSaslMechanism,
            ErrorCode::try_from(results[0].error_code)?
        );
        assert_refused(&results[1], "alice", "Too few iterations")?;
        Ok(())
    }
}
