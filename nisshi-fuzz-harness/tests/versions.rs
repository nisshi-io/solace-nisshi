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

//! Drives the harness's check functions the way a fuzz target does, over arbitrary requests built
//! from fixed pseudo-random bytes, at every version of each API. This runs without libFuzzer, so
//! a change to the harness, to the encoder or to the `arbitrary` generator that makes a version
//! unreachable (every input skipped) or breaks the byte check fails here.

#![allow(clippy::expect_used)]

use std::{
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    sync::Arc,
};

use arbitrary::{Arbitrary, Unstructured};
use nisshi_fuzz_harness::{api_version, check_request, check_response};
use nisshi_sans_io::{
    AddPartitionsToTxnRequest, AlterUserScramCredentialsRequest, ApiKey,
    ConsumerGroupDescribeRequest, CreateAclsRequest, CreateTopicsRequest, DeleteAclsRequest,
    DeleteGroupsRequest, DeleteRecordsRequest, DeleteTopicsRequest, DescribeAclsRequest,
    DescribeClusterRequest, DescribeConfigsRequest, DescribeGroupsRequest,
    DescribeTopicPartitionsRequest, DescribeUserScramCredentialsRequest, FetchRequest,
    FindCoordinatorRequest, IncrementalAlterConfigsRequest, InitProducerIdRequest,
    ListGroupsRequest, ListOffsetsRequest, ListPartitionReassignmentsRequest, MetadataRequest,
    ProduceRequest, Request, metadata_request::MetadataRequestTopic,
};
use nisshi_service::{CAPPED_API_VERSIONS, routable_range};
use nisshi_storage::{AutoCreateRefusals, MetadataService, StorageContainer, TopicCreationConfig};
use nisshi_storage_dynostore::MemoryEngineFactory;
use rama::Service as _;
use url::Url;

/// A xorshift64 stream: the same bytes on every run, so a failure reproduces.
fn bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;

    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// Checks `T` at every version a client can reach, with `inputs` arbitrary requests each.
///
/// The harness decides which unset fields the encoder may refuse, and fails on any other refusal,
/// so an input that is skipped here is one that the harness allows to be skipped. At least one
/// input per version has to reach the byte comparison, or that version isn't fuzzed.
fn check_every_version<T>(inputs: u64)
where
    T: Request + for<'a> Arbitrary<'a>,
{
    let routed = routable_range(T::KEY).expect("protocol metadata");
    let count = u16::try_from(routed.end() - routed.start() + 1).expect("a few versions");

    let reached: Vec<i16> = (0..count).map(api_version::<T>).collect();
    assert_eq!(
        routed.clone().collect::<Vec<_>>(),
        reached,
        "api key {}: the selectors don't reach the routed versions",
        T::KEY,
    );

    for version in reached {
        let mut built = 0;
        let mut skipped = 0;

        for seed in 0..inputs {
            let data = bytes(seed, 4096);

            let Ok(request) = T::arbitrary(&mut Unstructured::new(&data)) else {
                continue;
            };

            built += 1;

            let checked = catch_unwind(AssertUnwindSafe(|| check_request(request, version)))
                .unwrap_or_else(|panic| {
                    eprintln!("api key {} v{version}: seed {seed}", T::KEY);
                    resume_unwind(panic)
                })
                .unwrap_or_else(|error| panic!("seed {seed}: {error}"));

            if checked.is_none() {
                skipped += 1;
            }
        }

        assert!(
            built > skipped,
            "api key {} v{version}: no input reached the byte comparison ({built} built, {skipped} skipped)",
            T::KEY,
        );
    }
}

/// The APIs that nisshi routes at fewer versions than the protocol has are fuzzed at the routed
/// ones only.
#[test]
fn selector_stays_in_a_capped_range() {
    for (api_key, range) in CAPPED_API_VERSIONS {
        let versions: Vec<i16> = (0..64)
            .map(|selector| match api_key {
                k if k == ProduceRequest::KEY => api_version::<ProduceRequest>(selector),
                k if k == ListOffsetsRequest::KEY => api_version::<ListOffsetsRequest>(selector),
                k if k == AddPartitionsToTxnRequest::KEY => {
                    api_version::<AddPartitionsToTxnRequest>(selector)
                }
                k => panic!("a new capped API {k}: add it here"),
            })
            .collect();

        assert_eq!(versions.iter().min(), Some(range.start()));
        assert_eq!(versions.iter().max(), Some(range.end()));
    }
}

/// Serves `request` with the Metadata service, over an empty in-memory storage.
async fn serve_metadata(
    request: MetadataRequest,
) -> Result<nisshi_sans_io::MetadataResponse, nisshi_storage::Error> {
    let mut builder = StorageContainer::builder()
        .cluster_id("fuzz")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092").expect("static url"));
    builder.with_factory(Arc::new(MemoryEngineFactory));

    let storage = builder
        .storage(Url::parse("memory://versions").expect("static url"))
        .silent(true)
        .build()
        .await
        .expect("in-memory storage always builds");

    MetadataService {
        storage,
        topic_creation: TopicCreationConfig::default(),
        auto_create_refusals: AutoCreateRefusals::default(),
    }
    .serve(request)
    .await
}

/// A request for `topic` that sets the fields that Metadata requires at some version, so that it
/// encodes at every one.
fn metadata_request_for(topic: MetadataRequestTopic) -> MetadataRequest {
    MetadataRequest::default()
        .topics(Some(vec![topic]))
        .allow_auto_topic_creation(Some(false))
        .include_cluster_authorized_operations(Some(false))
        .include_topic_authorized_operations(Some(false))
}

/// A response has to encode at the version of its request, which only a service that fills in
/// the fields of that version can ensure, so this serves a request at every version.
#[tokio::test]
async fn metadata_response_encodes_at_every_routed_version() {
    for version in routable_range(MetadataRequest::KEY).expect("protocol metadata") {
        let request = check_request(
            metadata_request_for(
                MetadataRequestTopic::default()
                    .topic_id(Some([0; 16]))
                    .name(Some("orders".into())),
            ),
            version,
        )
        .unwrap_or_else(|error| panic!("{error}"))
        .expect("a named topic encodes at every version");

        let response = serve_metadata(request)
            .await
            .unwrap_or_else(|error| panic!("v{version}: {error}"));

        check_response(response, version).unwrap_or_else(|error| panic!("{error}"));
    }
}

/// A topic `Name` is nullable from Metadata v10, but a response's topic `Name` only from v12. The
/// memory engine answers an unnamed topic with an unnamed topic, which v10 and v11 can't encode.
/// Kafka 3.9.1 rejects such a request with `InvalidRequestException`.
#[tokio::test]
#[ignore = "the Metadata service answers an unnamed topic at v10 and v11 with a response that those versions can't encode"]
async fn metadata_unnamed_topic_response_encodes_at_v10_and_v11() {
    for version in 10..=11 {
        let request = check_request(
            metadata_request_for(
                MetadataRequestTopic::default()
                    .topic_id(Some([0; 16]))
                    .name(None),
            ),
            version,
        )
        .unwrap_or_else(|error| panic!("{error}"))
        .expect("an unnamed topic encodes from v10");

        let response = serve_metadata(request)
            .await
            .unwrap_or_else(|error| panic!("v{version}: {error}"));

        check_response(response, version).unwrap_or_else(|error| panic!("{error}"));
    }
}

#[test]
fn alter_user_scram_credentials() {
    check_every_version::<AlterUserScramCredentialsRequest>(200);
}

#[test]
fn consumer_group_describe() {
    check_every_version::<ConsumerGroupDescribeRequest>(200);
}

#[test]
fn create_acls() {
    check_every_version::<CreateAclsRequest>(200);
}

#[test]
fn create_topics() {
    check_every_version::<CreateTopicsRequest>(200);
}

#[test]
fn delete_acls() {
    check_every_version::<DeleteAclsRequest>(200);
}

#[test]
fn delete_groups() {
    check_every_version::<DeleteGroupsRequest>(200);
}

#[test]
fn delete_records() {
    check_every_version::<DeleteRecordsRequest>(200);
}

#[test]
fn delete_topics() {
    check_every_version::<DeleteTopicsRequest>(200);
}

#[test]
fn describe_acls() {
    check_every_version::<DescribeAclsRequest>(200);
}

#[test]
fn describe_cluster() {
    check_every_version::<DescribeClusterRequest>(200);
}

#[test]
fn describe_configs() {
    check_every_version::<DescribeConfigsRequest>(200);
}

#[test]
fn describe_groups() {
    check_every_version::<DescribeGroupsRequest>(200);
}

#[test]
fn describe_topic_partitions() {
    check_every_version::<DescribeTopicPartitionsRequest>(200);
}

#[test]
fn describe_user_scram_credentials() {
    check_every_version::<DescribeUserScramCredentialsRequest>(200);
}

#[test]
fn fetch() {
    check_every_version::<FetchRequest>(200);
}

#[test]
fn find_coordinator() {
    check_every_version::<FindCoordinatorRequest>(200);
}

#[test]
fn incremental_alter_configs() {
    check_every_version::<IncrementalAlterConfigsRequest>(200);
}

#[test]
fn init_producer_id() {
    check_every_version::<InitProducerIdRequest>(200);
}

#[test]
fn list_groups() {
    check_every_version::<ListGroupsRequest>(200);
}

#[test]
fn list_offsets() {
    check_every_version::<ListOffsetsRequest>(200);
}

#[test]
fn list_partition_reassignments() {
    check_every_version::<ListPartitionReassignmentsRequest>(200);
}

#[test]
fn metadata() {
    check_every_version::<MetadataRequest>(200);
}

#[test]
fn produce() {
    check_every_version::<ProduceRequest>(200);
}
