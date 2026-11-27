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

use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    AlterUserScramCredentialsRequest, ErrorCode, RequestInput, ScramMechanism,
    alter_user_scram_credentials_request::{ScramCredentialDeletion, ScramCredentialUpsertion},
    alter_user_scram_credentials_response::AlterUserScramCredentialsResult,
};
use nisshi_storage::{
    AlterUserScramCredentialsService, ArcDynStorage, Storage, user_credentials_unsupported_message,
};
use rama::{Service, extensions::Extensions};
use rand::{prelude::*, rng};
use uuid::Uuid;

use crate::common::{alphanumeric_string, init_tracing};

const SCRAM_SHA_256: i8 = 1;
const SCRAM_SHA_512: i8 = 2;
const UNKNOWN_MECHANISM: i8 = 111;

/// Both mechanisms, as the wire value of the request and the value that storage keys on.
const MECHANISMS: [(i8, ScramMechanism); 2] = [
    (SCRAM_SHA_256, ScramMechanism::Scram256),
    (SCRAM_SHA_512, ScramMechanism::Scram512),
];

const TOO_FEW: &str = "Too few iterations";
const TOO_MANY: &str = "Too many iterations";

/// Counts that Kafka 3.9.1 refuses, with the message that it answers for each.
const REFUSED_COUNTS: [(i32, &str); 6] = [
    (4095, TOO_FEW),
    (0, TOO_FEW),
    (-1, TOO_FEW),
    (i32::MIN, TOO_FEW),
    (16385, TOO_MANY),
    (i32::MAX, TOO_MANY),
];

fn upsertion(name: &str, mechanism: i8, iterations: i32) -> ScramCredentialUpsertion {
    ScramCredentialUpsertion::default()
        .name(name.into())
        .mechanism(mechanism)
        .iterations(iterations)
        .salt(Bytes::from_static(b"salt"))
        .salted_password(Bytes::from_static(b"salted password"))
}

async fn alter(
    storage: &ArcDynStorage,
    deletions: Vec<ScramCredentialDeletion>,
    upsertions: Vec<ScramCredentialUpsertion>,
) -> Result<Vec<AlterUserScramCredentialsResult>> {
    let response = AlterUserScramCredentialsService {
        storage: storage.clone(),
    }
    .serve(RequestInput {
        request: AlterUserScramCredentialsRequest::default()
            .deletions(Some(deletions))
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

fn assert_stored(answered: &AlterUserScramCredentialsResult, user: &str) -> Result<()> {
    assert_eq!(user, answered.user);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(answered.error_code)?);
    Ok(())
}

/// Returns the iteration count that storage holds for the user's credential, if it holds one.
async fn stored_iterations(
    storage: &ArcDynStorage,
    user: &str,
    mechanism: ScramMechanism,
) -> Result<Option<i32>> {
    Ok(storage
        .user_scram_credential(user, mechanism)
        .await?
        .map(|credential| credential.iterations))
}

async fn refused_counts_store_nothing(storage: ArcDynStorage) -> Result<()> {
    for (wire, mechanism) in MECHANISMS {
        for (iterations, message) in REFUSED_COUNTS {
            let user = alphanumeric_string(15);

            let results = alter(&storage, vec![], vec![upsertion(&user, wire, iterations)]).await?;

            assert_eq!(1, results.len(), "{mechanism:?} {iterations}");
            assert_refused(&results[0], &user, message)?;
            assert_eq!(None, stored_iterations(&storage, &user, mechanism).await?);
        }
    }

    Ok(())
}

async fn the_bounds_are_stored(storage: ArcDynStorage) -> Result<()> {
    for (wire, mechanism) in MECHANISMS {
        for iterations in [4096, 16384] {
            let user = alphanumeric_string(15);

            let results = alter(&storage, vec![], vec![upsertion(&user, wire, iterations)]).await?;

            assert_eq!(1, results.len(), "{mechanism:?} {iterations}");
            assert_stored(&results[0], &user)?;
            assert_eq!(
                Some(iterations),
                stored_iterations(&storage, &user, mechanism).await?
            );
        }
    }

    Ok(())
}

async fn a_refused_upsertion_leaves_the_stored_credential_unchanged(
    storage: ArcDynStorage,
) -> Result<()> {
    for (wire, mechanism) in MECHANISMS {
        let user = alphanumeric_string(15);

        let results = alter(&storage, vec![], vec![upsertion(&user, wire, 8192)]).await?;
        assert_stored(&results[0], &user)?;

        let results = alter(&storage, vec![], vec![upsertion(&user, wire, 4095)]).await?;
        assert_refused(&results[0], &user, TOO_FEW)?;

        assert_eq!(
            Some(8192),
            stored_iterations(&storage, &user, mechanism).await?
        );
    }

    Ok(())
}

/// Kafka stores nothing for a user when any one of the user's upsertions is refused, and stores
/// the other users of the request.
async fn a_refused_upsertion_stores_nothing_for_its_user_but_stores_other_users(
    storage: ArcDynStorage,
) -> Result<()> {
    // The refused upsertion comes first for one user and last for the other.
    let refused_first = alphanumeric_string(15);
    let refused_last = alphanumeric_string(15);
    let valid = alphanumeric_string(15);

    let results = alter(
        &storage,
        vec![],
        vec![
            upsertion(&refused_first, SCRAM_SHA_256, 20000),
            upsertion(&refused_first, SCRAM_SHA_512, 8192),
            upsertion(&refused_last, SCRAM_SHA_512, 8192),
            upsertion(&refused_last, SCRAM_SHA_256, 100),
            upsertion(&valid, SCRAM_SHA_256, 8192),
        ],
    )
    .await?;

    assert_eq!(5, results.len());
    assert_refused(&results[0], &refused_first, TOO_MANY)?;
    assert_refused(&results[1], &refused_first, TOO_MANY)?;
    assert_refused(&results[2], &refused_last, TOO_FEW)?;
    assert_refused(&results[3], &refused_last, TOO_FEW)?;
    assert_stored(&results[4], &valid)?;

    for user in [&refused_first, &refused_last] {
        for (_, mechanism) in MECHANISMS {
            assert_eq!(None, stored_iterations(&storage, user, mechanism).await?);
        }
    }
    assert_eq!(
        Some(8192),
        stored_iterations(&storage, &valid, ScramMechanism::Scram256).await?
    );

    Ok(())
}

/// A deletion for the same user as a refused upsertion still deletes, as the deletion is not
/// checked against the iteration range.
async fn a_refused_upsertion_does_not_stop_a_deletion_for_its_user(
    storage: ArcDynStorage,
) -> Result<()> {
    let user = alphanumeric_string(15);

    let results = alter(
        &storage,
        vec![],
        vec![upsertion(&user, SCRAM_SHA_256, 8192)],
    )
    .await?;
    assert_stored(&results[0], &user)?;

    let results = alter(
        &storage,
        vec![
            ScramCredentialDeletion::default()
                .name(user.clone())
                .mechanism(SCRAM_SHA_256),
        ],
        vec![upsertion(&user, SCRAM_SHA_512, 99999)],
    )
    .await?;

    assert_eq!(2, results.len());
    assert_stored(&results[0], &user)?;
    assert_refused(&results[1], &user, TOO_MANY)?;
    assert_eq!(
        None,
        stored_iterations(&storage, &user, ScramMechanism::Scram256).await?
    );
    assert_eq!(
        None,
        stored_iterations(&storage, &user, ScramMechanism::Scram512).await?
    );

    Ok(())
}

/// An item with an unknown mechanism is answered with the mechanism error, and its count is not
/// checked, so it does not refuse the user's valid upsertion.
async fn an_unknown_mechanism_does_not_refuse_a_valid_upsertion_for_its_user(
    storage: ArcDynStorage,
) -> Result<()> {
    let user = alphanumeric_string(15);

    let results = alter(
        &storage,
        vec![],
        vec![
            upsertion(&user, UNKNOWN_MECHANISM, 20000),
            upsertion(&user, SCRAM_SHA_256, 8192),
        ],
    )
    .await?;

    assert_eq!(2, results.len());
    assert_eq!(
        ErrorCode::UnsupportedSaslMechanism,
        ErrorCode::try_from(results[0].error_code)?
    );
    assert_eq!(
        Some("Unknown SCRAM mechanism"),
        results[0].error_message.as_deref()
    );
    assert_stored(&results[1], &user)?;
    assert_eq!(
        Some(8192),
        stored_iterations(&storage, &user, ScramMechanism::Scram256).await?
    );

    Ok(())
}

/// The in-memory engine answers every upsertion `UNSUPPORTED_VERSION`. A refused count is
/// answered `UNACCEPTABLE_CREDENTIAL` instead, so the refusal comes before the storage call.
async fn memory_refuses_counts_before_storage(storage: ArcDynStorage) -> Result<()> {
    for (wire, _) in MECHANISMS {
        for (iterations, message) in REFUSED_COUNTS {
            let user = alphanumeric_string(15);

            let results = alter(&storage, vec![], vec![upsertion(&user, wire, iterations)]).await?;

            assert_eq!(1, results.len());
            assert_refused(&results[0], &user, message)?;
        }

        let user = alphanumeric_string(15);
        let results = alter(&storage, vec![], vec![upsertion(&user, wire, 4096)]).await?;

        assert_eq!(
            ErrorCode::UnsupportedVersion,
            ErrorCode::try_from(results[0].error_code)?
        );
        assert_eq!(
            Some(user_credentials_unsupported_message("in-memory (memory://)").as_str()),
            results[0].error_message.as_deref()
        );
    }

    Ok(())
}

macro_rules! stored_backend {
    ($name:ident, $feature:literal, $storage:path) => {
        #[cfg(feature = $feature)]
        mod $name {
            use super::*;

            async fn storage() -> Result<ArcDynStorage> {
                $storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await
            }

            #[tokio::test]
            async fn refused_counts_store_nothing() -> Result<()> {
                let _guard = init_tracing()?;

                super::refused_counts_store_nothing(storage().await?).await
            }

            #[tokio::test]
            async fn the_bounds_are_stored() -> Result<()> {
                let _guard = init_tracing()?;

                super::the_bounds_are_stored(storage().await?).await
            }

            #[tokio::test]
            async fn a_refused_upsertion_leaves_the_stored_credential_unchanged() -> Result<()> {
                let _guard = init_tracing()?;

                super::a_refused_upsertion_leaves_the_stored_credential_unchanged(storage().await?)
                    .await
            }

            #[tokio::test]
            async fn a_refused_upsertion_stores_nothing_for_its_user_but_stores_other_users()
            -> Result<()> {
                let _guard = init_tracing()?;

                super::a_refused_upsertion_stores_nothing_for_its_user_but_stores_other_users(
                    storage().await?,
                )
                .await
            }

            #[tokio::test]
            async fn a_refused_upsertion_does_not_stop_a_deletion_for_its_user() -> Result<()> {
                let _guard = init_tracing()?;

                super::a_refused_upsertion_does_not_stop_a_deletion_for_its_user(storage().await?)
                    .await
            }

            #[tokio::test]
            async fn an_unknown_mechanism_does_not_refuse_a_valid_upsertion_for_its_user()
            -> Result<()> {
                let _guard = init_tracing()?;

                super::an_unknown_mechanism_does_not_refuse_a_valid_upsertion_for_its_user(
                    storage().await?,
                )
                .await
            }
        }
    };
}

stored_backend!(lite, "libsql", crate::common::lite_storage);
stored_backend!(pg, "postgres", crate::common::postgres_storage);

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;

    #[tokio::test]
    async fn refused_counts_are_answered_before_storage() -> Result<()> {
        let _guard = init_tracing()?;

        let storage =
            crate::common::memory_storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await?;

        memory_refuses_counts_before_storage(storage).await
    }
}
