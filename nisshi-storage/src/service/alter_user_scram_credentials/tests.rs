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

use super::*;
use crate::storage_double::storage_double;
use rama::extensions::Extensions;

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

/// The same user and mechanism named twice is not Kafka's DUPLICATE_RESOURCE here: both
/// items get the refused message, and neither reaches storage.
#[tokio::test]
async fn the_same_user_and_mechanism_twice_with_one_refused_count_refuses_both() -> Result<()> {
    let results = serve(vec![
        upsertion("alice", SCRAM_SHA_256, 4095),
        upsertion("alice", SCRAM_SHA_256, 8192),
    ])
    .await?;

    assert_eq!(2, results.len());
    assert_refused(&results[0], "alice", "Too few iterations")?;
    assert_refused(&results[1], "alice", "Too few iterations")?;
    Ok(())
}

/// The fields of each WARN event that this layer saw.
#[derive(Clone, Default)]
struct Fields(std::sync::Arc<std::sync::Mutex<Vec<BTreeMap<String, String>>>>);

struct Recorded(BTreeMap<String, String>);

impl tracing::field::Visit for Recorded {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        _ = self.0.insert(field.name().into(), format!("{value:?}"));
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Fields {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != tracing::Level::WARN {
            return;
        }

        let mut recorded = Recorded(BTreeMap::new());
        event.record(&mut recorded);
        self.0
            .lock()
            .expect("the lock is not poisoned")
            .push(recorded.0);
    }
}

/// Serves `upsertions` and returns the fields of each WARN event that the service logged.
async fn logged_fields(
    upsertions: Vec<ScramCredentialUpsertion>,
) -> Result<Vec<BTreeMap<String, String>>> {
    use tracing_subscriber::layer::SubscriberExt as _;

    let fields = Fields::default();
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(fields.clone()));

    _ = serve(upsertions).await?;

    let logged = fields.0.lock().expect("the lock is not poisoned").clone();
    Ok(logged)
}

#[tokio::test]
async fn a_user_with_two_refused_upsertions_is_logged_once_with_the_first_refusal() -> Result<()> {
    let logged = logged_fields(vec![
        upsertion("alice", SCRAM_SHA_256, 100),
        upsertion("alice", SCRAM_SHA_512, 20000),
        upsertion("bob", SCRAM_SHA_512, 20000),
    ])
    .await?;

    assert_eq!(2, logged.len(), "{logged:?}");
    assert_eq!(Some("\"alice\""), logged[0].get("user").map(String::as_str));
    assert_eq!(
        Some("Scram256"),
        logged[0].get("mechanism").map(String::as_str)
    );
    assert_eq!(Some("100"), logged[0].get("iterations").map(String::as_str));
    assert_eq!(Some("\"bob\""), logged[1].get("user").map(String::as_str));
    Ok(())
}
