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

//! Every route that the broker registers answers with a response that encodes at every version a
//! client can reach.
//!
//! A response that leaves a field unset that the client's version requires has no encoding, and
//! the encoder refuses it with `MandatoryFieldUnset` (SOL-157119). The broker then has nothing to
//! send, so the connection closes where Kafka would have answered. Each handler fills in the fields
//! of the version it serves, so a handler can miss one at one version and nowhere else. This test
//! serves requests at every one of those versions, so that the next miss fails here and not on a
//! client.
//!
//! The routes come from [`nisshi_broker::service::routes`], the list that the broker serves, and
//! the versions from [`routable_range`], the range that the broker routes, so a
//! service that is added to the broker is covered with no change here.
//!
//! The requests are arbitrary ones against empty storage, so most of them end in an error response,
//! and a field that only a success path fills is checked only where a request happens to succeed.
//! What the test guarantees is one answered request at every version of every route. A route that
//! needs connection state, the SASL ones, is served with none, so only its refusal is checked.
//!
//! The test lives in this crate, which is not published, because it needs the harness and the
//! broker together, and a test of the broker's own crate that imports the harness would not compile
//! from the published package.

#![allow(clippy::expect_used)]

use std::{
    any::Any,
    assert_matches,
    collections::BTreeMap,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
    time::Duration,
};

use arbitrary::{Arbitrary as _, Unstructured};
use nisshi_broker::{
    NODE_ID,
    coordinator::group::administrator::{Controller, GroupCoordinatorConfig},
    service::routes,
};
use nisshi_fuzz_harness::{
    RequestAtVersion, check_response_body, pseudo_random_bytes, request_at_version,
};
use nisshi_sans_io::{ApiKey as _, Body, EndTxnRequest, Error, Frame, Header};
use nisshi_service::routable_range;
use nisshi_storage::{ArcDynStorage, AutoCreateRefusals, StorageContainer, TopicCreationConfig};
use nisshi_storage_dynostore::MemoryEngineFactory;
use rama::Service as _;
use tokio::time::timeout;
use url::Url;

/// The requests of each API that are served at each of its versions. The harness's own tests use
/// 200 for each version of one API, and this serves every API, so it is smaller.
const REQUESTS_PER_VERSION: usize = 60;

/// How long a request may wait for an answer, in virtual time: the test pauses the clock, so a
/// request that waits for a rebalance or for data that never comes ends when the clock runs ahead.
const ANSWER_WITHIN: Duration = Duration::from_secs(24 * 60 * 60);

/// The routes that answer none of the requests this test builds, with the reason.
///
/// A route has to answer at least one request at each version, or this test has checked no
/// response of it. An entry that answers now fails the test, so the list only shrinks.
fn unanswered() -> BTreeMap<i16, &'static str> {
    BTreeMap::from([(
        EndTxnRequest::KEY,
        // A random transactional ID is never one that `InitProducerId` registered, and
        // `InitProducerId` refuses every transactional ID, so this test cannot register one. The
        // storage refuses every request with `TransactionalIdNotFound`, and the route closes the
        // connection. Kafka 3.9.1 answers an unknown transactional ID with
        // `INVALID_PRODUCER_ID_MAPPING` in the response:
        // https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala#L511-L513
        "the route closes the connection for an unknown transactional ID, where Kafka answers INVALID_PRODUCER_ID_MAPPING",
    )])
}

/// Arbitrary requests for every API key in `api_keys`, [`REQUESTS_PER_VERSION`] each, from fixed
/// bytes so that a failure repeats.
///
/// A [`Body`] is a request or a response of any API, so this draws bodies and keeps the requests.
fn requests(api_keys: &[i16]) -> BTreeMap<i16, Vec<Body>> {
    let mut found: BTreeMap<i16, Vec<Body>> = api_keys.iter().map(|key| (*key, vec![])).collect();

    for seed in 0..200_000 {
        if found
            .values()
            .all(|bodies| bodies.len() >= REQUESTS_PER_VERSION)
        {
            break;
        }

        let data = pseudo_random_bytes(seed, 4096);

        let Ok(body) = Body::arbitrary(&mut Unstructured::new(&data)) else {
            continue;
        };

        if !body.api_name().ends_with("Request") {
            continue;
        }

        if let Some(bodies) = found.get_mut(&body.api_key())
            && bodies.len() < REQUESTS_PER_VERSION
        {
            bodies.push(body);
        }
    }

    found
}

/// What became of the requests served at one version of one API.
#[derive(Debug, Default)]
struct Tally {
    /// Requests that the encoder refused at this version, so that none was served, and the first
    /// field that it refused.
    no_encoding: usize,
    first_refused_field: Option<String>,

    /// Requests that the route failed, which closes the connection, and the first error.
    route_failed: usize,
    first_route_error: Option<String>,

    /// Requests that the route did not answer within [`ANSWER_WITHIN`].
    timed_out: usize,

    /// Requests that the route answered.
    answered: usize,
}

impl Tally {
    fn describe(&self) -> String {
        format!(
            "answered {}, no encoding {} (first field: {}), route failed {} (first error: {}), timed out {}",
            self.answered,
            self.no_encoding,
            self.first_refused_field.as_deref().unwrap_or("none"),
            self.route_failed,
            self.first_route_error.as_deref().unwrap_or("none"),
            self.timed_out,
        )
    }
}

/// The message of a panic, whatever its payload: a `String` from a formatted message, a `&str` from
/// a literal one, or a note that there was none.
fn panic_message(panic: &(dyn Any + Send)) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            panic
                .downcast_ref::<&str>()
                .map(|message| (*message).to_string())
        })
        .unwrap_or_else(|| String::from("panicked with no message"))
}

async fn memory_storage() -> ArcDynStorage {
    let mut builder = StorageContainer::builder()
        .cluster_id("response-encoding")
        .node_id(NODE_ID)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092").expect("static url"));
    builder.with_factory(Arc::new(MemoryEngineFactory));

    builder
        .storage(Url::parse("memory://response-encoding").expect("static url"))
        .silent(true)
        .build()
        .await
        .expect("in-memory storage always builds")
}

#[tokio::test(start_paused = true)]
async fn every_routed_response_encodes_at_every_routed_version() {
    let storage = memory_storage().await;

    let route = routes(
        Controller::new(storage.clone(), GroupCoordinatorConfig::default()),
        storage,
        TopicCreationConfig::default(),
        AutoCreateRefusals::default(),
    )
    .expect("the broker's routes build")
    .build()
    .expect("the broker's routes build");

    let api_keys: Vec<i16> = route.api_keys().collect();
    let requests = requests(&api_keys);

    let unanswered = unanswered();
    let mut failures = vec![];
    let mut tallies = BTreeMap::<(i16, i16), Tally>::new();

    for api_key in &api_keys {
        let versions = routable_range(*api_key).expect("a routed API has protocol metadata");

        for api_version in versions {
            let tally = tallies.entry((*api_key, api_version)).or_default();

            for body in &requests[api_key] {
                let body = match request_at_version(body.clone(), *api_key, api_version) {
                    Ok(RequestAtVersion::Encoded(body)) => body,

                    Ok(RequestAtVersion::Refused { field }) => {
                        tally.no_encoding += 1;
                        if tally.first_refused_field.is_none() {
                            tally.first_refused_field = Some(field);
                        }
                        continue;
                    }

                    Err(error) => {
                        failures.push(format!("{error}"));
                        continue;
                    }
                };

                let frame = Frame {
                    size: 0,
                    header: Header::Request {
                        api_key: *api_key,
                        api_version,
                        correlation_id: 1,
                        client_id: Some("response-encoding".into()),
                    },
                    body,
                };

                // A route that fails the request closes the connection, or never answers: there
                // is no response to encode.
                let answer = match timeout(ANSWER_WITHIN, route.serve(frame)).await {
                    Ok(Ok(answer)) => answer,

                    Ok(Err(error)) => {
                        tally.route_failed += 1;
                        if tally.first_route_error.is_none() {
                            tally.first_route_error = Some(format!("{error:?}"));
                        }
                        continue;
                    }

                    Err(_elapsed) => {
                        tally.timed_out += 1;
                        continue;
                    }
                };

                tally.answered += 1;

                let encoded = catch_unwind(AssertUnwindSafe(|| {
                    check_response_body(answer.body, *api_key, api_version)
                }));

                match encoded {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => failures.push(format!("{error}")),
                    Err(panic) => failures.push(format!(
                        "api key {api_key} v{api_version}: {}",
                        panic_message(panic.as_ref())
                    )),
                }
            }
        }
    }

    assert!(
        failures.is_empty(),
        "a route answered with a response that does not encode:\n{}",
        failures.join("\n")
    );

    let silent: Vec<String> = tallies
        .iter()
        .filter(|(_, tally)| tally.answered == 0)
        .filter(|((api_key, _), _)| !unanswered.contains_key(api_key))
        .map(|((api_key, version), tally)| {
            format!("api key {api_key} v{version}: {}", tally.describe())
        })
        .collect();

    assert!(
        silent.is_empty(),
        "no request was answered, so no response was checked:\n{}",
        silent.join("\n")
    );

    for (api_key, reason) in &unanswered {
        assert!(
            !tallies
                .iter()
                .any(|((key, _), tally)| key == api_key && tally.answered > 0),
            "api key {api_key} answers now, so remove it from the list of unanswered routes ({reason})"
        );
    }
}

/// The check has to fail for a response that leaves a required field unset, or the test above
/// passes whatever the routes answer. `JoinGroupResponse.ProtocolName` is mandatory below v7.
#[test]
fn a_response_that_leaves_a_required_field_unset_fails_the_check() {
    use nisshi_sans_io::JoinGroupResponse;

    assert_matches!(
        check_response_body(
            JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .protocol_name(None)
                .into(),
            JoinGroupResponse::KEY,
            6,
        ),
        Err(Error::Message(message)) if message.contains("protocol_name")
    );
}

/// A panic with a literal message has a `&str` payload, and any panic must keep its message.
#[test]
fn a_panic_keeps_its_message_whatever_its_payload() {
    let literal = catch_unwind(|| panic!("a literal message")).unwrap_err();
    let formatted = catch_unwind(|| panic!("a message with {}", 1)).unwrap_err();
    let bare = catch_unwind(|| std::panic::panic_any(7_u8)).unwrap_err();

    assert_eq!("a literal message", panic_message(literal.as_ref()));
    assert_eq!("a message with 1", panic_message(formatted.as_ref()));
    assert_eq!("panicked with no message", panic_message(bare.as_ref()));
}

/// A request that no client could send at a version is counted as refused, with the field that the
/// encoder refused, and not dropped without a trace.
#[test]
fn a_request_with_no_encoding_at_a_version_reports_the_field() {
    use nisshi_sans_io::{MetadataRequest, metadata_request::MetadataRequestTopic};

    // A topic's `Name` is nullable from v10, so a request at v9 that leaves it unset has no
    // encoding.
    let request: Body = MetadataRequest::default()
        .topics(Some(vec![MetadataRequestTopic::default().name(None)]))
        .into();

    assert_matches!(
        request_at_version(request, MetadataRequest::KEY, 9),
        Ok(RequestAtVersion::Refused { field }) if field.contains("name")
    );
}
