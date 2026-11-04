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

use std::{
    assert_matches,
    collections::BTreeMap,
    panic::{AssertUnwindSafe, catch_unwind},
    time::Duration,
};

use arbitrary::{Arbitrary as _, Unstructured};
use nisshi_broker::{
    NODE_ID,
    coordinator::group::administrator::{Controller, GroupCoordinatorConfig},
    service::routes,
};
use nisshi_fuzz_harness::{check_response_body, pseudo_random_bytes, request_at_version};
use nisshi_sans_io::{ApiKey as _, Body, EndTxnRequest, Error, Frame, Header};
use nisshi_service::routable_range;
use nisshi_storage::{ArcDynStorage, AutoCreateRefusals, TopicCreationConfig};
use rama::Service as _;
use tokio::time::timeout;
use url::Url;

use crate::common::{StorageType, storage_container};

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
        // Transactions are out of scope for Nisshi, so nobody fixes or tracks anything here. A
        // random transactional ID is never one that `InitProducerId` registered, so the storage
        // refuses every request with `TransactionalIdNotFound`, and the route closes the
        // connection instead of answering.
        "transactions are out of scope: the route fails every request for an unknown transactional ID",
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

#[tokio::test(start_paused = true)]
async fn every_routed_response_encodes_at_every_routed_version() {
    let listener = Url::parse("tcp://127.0.0.1:9092").unwrap();

    let storage: ArcDynStorage = storage_container(
        StorageType::InMemory,
        "response-encoding",
        NODE_ID,
        listener,
        None,
    )
    .await
    .unwrap();

    let route = routes(
        Controller::new(storage.clone(), GroupCoordinatorConfig::default()),
        storage,
        TopicCreationConfig::default(),
        AutoCreateRefusals::default(),
    )
    .unwrap()
    .build()
    .unwrap();

    let api_keys: Vec<i16> = route.api_keys().collect();
    let requests = requests(&api_keys);

    let unanswered = unanswered();
    let mut failures = vec![];
    let mut served = BTreeMap::<(i16, i16), usize>::new();

    for api_key in &api_keys {
        let versions = routable_range(*api_key).expect("a routed API has protocol metadata");

        for api_version in versions {
            for body in &requests[api_key] {
                let Some(body) = request_at_version(body.clone(), *api_key, api_version).unwrap()
                else {
                    continue;
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
                let Ok(Ok(answer)) = timeout(ANSWER_WITHIN, route.serve(frame)).await else {
                    continue;
                };

                *served.entry((*api_key, api_version)).or_default() += 1;

                let encoded = catch_unwind(AssertUnwindSafe(|| {
                    check_response_body(answer.body, *api_key, api_version)
                }));

                match encoded {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => failures.push(format!("{error}")),
                    Err(panic) => {
                        failures.push(panic.downcast_ref::<String>().cloned().unwrap_or_else(
                            || format!("api key {api_key} v{api_version}: panicked"),
                        ))
                    }
                }
            }
        }
    }

    assert!(
        failures.is_empty(),
        "a route answered with a response that does not encode:\n{}",
        failures.join("\n")
    );

    let silent: Vec<String> = api_keys
        .iter()
        .flat_map(|api_key| {
            routable_range(*api_key)
                .expect("a routed API has protocol metadata")
                .map(move |version| (*api_key, version))
        })
        .filter(|pair| !served.contains_key(pair))
        .filter(|(api_key, _)| !unanswered.contains_key(api_key))
        .map(|(api_key, version)| format!("api key {api_key} v{version}"))
        .collect();

    assert!(
        silent.is_empty(),
        "no request was answered, so no response was checked: {silent:?}"
    );

    for (api_key, reason) in &unanswered {
        assert!(
            !served.keys().any(|(key, _)| key == api_key),
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
