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

//! Tests for the API version ranges that the broker's route table advertises and routes.

use bytes::Bytes;
use nisshi_broker::{
    Error, Result,
    coordinator::group::administrator::Controller,
    service::{auth, coordinator, storage},
};
use nisshi_sans_io::{
    AddPartitionsToTxnRequest, ApiKey as _, ApiVersionsRequest, ApiVersionsResponse, Body,
    BytesInput, CreateTopicsRequest, CreateTopicsResponse, ErrorCode, Frame, FrameInput, Header,
    IsolationLevel, ListOffset, ListOffsetsRequest, ListOffsetsResponse, ProduceRequest,
    RootMessageMeta,
    api_versions_response::ApiVersion,
    create_topics_request::CreatableTopic,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
};
use nisshi_service::{BytesFrameLayer, CAPPED_API_VERSIONS, FrameRouteService};
use nisshi_storage::{AutoCreateRefusals, TopicCreationConfig};
use rama::{Layer as _, Service as _, extensions::Extensions};
use uuid::Uuid;

use crate::common::{self, StorageType};

/// Builds the route table that the broker serves, without the TCP layers.
async fn route_table() -> Result<FrameRouteService<Error>> {
    let storage = common::storage_container(
        StorageType::InMemory,
        Uuid::new_v4().to_string(),
        111,
        "tcp://localhost:9092".parse()?,
        None,
    )
    .await?;

    let coordinator = Controller::with_storage(storage.clone())?;

    let builder = storage::services(
        FrameRouteService::<Error>::builder(),
        storage,
        TopicCreationConfig::default(),
        AutoCreateRefusals::default(),
    )?;
    let builder = coordinator::services(builder, coordinator)?;
    let builder = auth::services(builder)?;

    builder.build().map_err(Error::from)
}

async fn serve(
    route_table: &FrameRouteService<Error>,
    api_key: i16,
    api_version: i16,
    body: Body,
) -> Result<Frame> {
    route_table
        .serve(FrameInput {
            frame: Frame {
                size: 0,
                header: Header::Request {
                    api_key,
                    api_version,
                    correlation_id: 0,
                    client_id: None,
                },
                body,
            },
            extensions: Extensions::default(),
        })
        .await
}

fn protocol_range(api_key: i16) -> (i16, i16) {
    let valid = RootMessageMeta::messages()
        .requests()
        .get(&api_key)
        .expect("protocol metadata")
        .version
        .valid;

    (valid.start, valid.end)
}

#[test]
fn capped_range_snapshot() {
    assert_eq!(
        (0, 11),
        protocol_range(ProduceRequest::KEY),
        "Produce's protocol range moved; check that the Produce cap still excludes only the \
         versions before RecordBatch v2"
    );
    assert_eq!(
        (0, 9),
        protocol_range(ListOffsetsRequest::KEY),
        "ListOffsets' protocol range moved; check that the ListOffsets cap still excludes only \
         the versions with unimplemented sentinel timestamps (v7+)"
    );
    assert_eq!(
        (0, 5),
        protocol_range(AddPartitionsToTxnRequest::KEY),
        "AddPartitionsToTxn's protocol range moved; check that the AddPartitionsToTxn cap still \
         excludes only the multi-transaction (v4+) shape"
    );

    let caps = CAPPED_API_VERSIONS
        .iter()
        .map(|(api_key, range)| (*api_key, *range.start(), *range.end()))
        .collect::<Vec<_>>();

    assert_eq!(
        vec![
            (ProduceRequest::KEY, 3, 11),
            (ListOffsetsRequest::KEY, 0, 6),
            (AddPartitionsToTxnRequest::KEY, 0, 3),
        ],
        caps
    );
}

#[tokio::test]
async fn full_route_table_has_the_expected_route_count() -> Result<()> {
    let storage = common::storage_container(
        StorageType::InMemory,
        Uuid::new_v4().to_string(),
        111,
        "tcp://localhost:9092".parse()?,
        None,
    )
    .await?;

    let coordinator = Controller::with_storage(storage.clone())?;

    let with_storage_routes = storage::services(
        FrameRouteService::<Error>::builder(),
        storage,
        TopicCreationConfig::default(),
        AutoCreateRefusals::default(),
    )?;
    // The broker does not register a route for GetTelemetrySubscriptions, so it does not
    // advertise the client telemetry APIs.
    assert_eq!(24, with_storage_routes.len());

    let with_coordinator_routes = coordinator::services(with_storage_routes, coordinator)?;
    assert_eq!(24 + 9, with_coordinator_routes.len());

    let with_auth_routes = auth::services(with_coordinator_routes)?;
    assert_eq!(24 + 9 + 2, with_auth_routes.len());

    let route_table = with_auth_routes.build().map_err(Error::from)?;
    // `build` adds the `ApiVersions` route itself.
    assert_eq!(24 + 9 + 2 + 1, route_table.len());

    Ok(())
}

// `ApiVersions` advertises a cap only for a route registered through `with_capped_route`, so
// this test fails when a capped API is registered without its version gate.
#[tokio::test]
async fn api_versions_advertises_each_cap() -> Result<()> {
    let route_table = route_table().await?;

    let response = serve(
        &route_table,
        ApiVersionsRequest::KEY,
        3,
        ApiVersionsRequest::default().into(),
    )
    .await?;

    let advertised = ApiVersionsResponse::try_from(response.body)?
        .api_keys
        .unwrap_or_default();

    let range = |api_key: i16| {
        advertised
            .iter()
            .find(|api: &&ApiVersion| api.api_key == api_key)
            .map(|api| (api.min_version, api.max_version))
    };

    assert_eq!(Some((3, 11)), range(ProduceRequest::KEY));
    assert_eq!(Some((0, 6)), range(ListOffsetsRequest::KEY));
    assert_eq!(Some((0, 3)), range(AddPartitionsToTxnRequest::KEY));

    for (api_key, supported) in CAPPED_API_VERSIONS {
        assert_eq!(
            Some((*supported.start(), *supported.end())),
            range(api_key),
            "api_key: {api_key}"
        );
    }

    Ok(())
}

// A literal table, not `protocol_range`, because the protocol metadata is what changes when
// the Kafka message descriptors are updated. An API that is not capped advertises its whole
// protocol range, so a descriptor update that adds a version nisshi does not implement fails
// here until someone caps the API or updates its row.
#[tokio::test]
async fn api_versions_advertises_the_expected_table() -> Result<()> {
    let route_table = route_table().await?;

    let response = serve(
        &route_table,
        ApiVersionsRequest::KEY,
        3,
        ApiVersionsRequest::default().into(),
    )
    .await?;

    let mut advertised = ApiVersionsResponse::try_from(response.body)?
        .api_keys
        .unwrap_or_default()
        .iter()
        .map(|api: &ApiVersion| (api.api_key, api.min_version, api.max_version))
        .collect::<Vec<_>>();
    advertised.sort_unstable();

    // `(api_key, min_version, max_version)`
    let expected: Vec<(i16, i16, i16)> = vec![
        (0, 3, 11), // Produce
        (1, 0, 17), // Fetch
        (2, 0, 6),  // ListOffsets
        (3, 0, 12), // Metadata
        (8, 0, 9),  // OffsetCommit
        (9, 0, 9),  // OffsetFetch
        (10, 0, 6), // FindCoordinator
        (11, 0, 9), // JoinGroup
        (12, 0, 4), // Heartbeat
        (13, 0, 5), // LeaveGroup
        (14, 0, 5), // SyncGroup
        (15, 0, 5), // DescribeGroups
        (16, 0, 5), // ListGroups
        (17, 0, 1), // SaslHandshake
        (18, 0, 4), // ApiVersions
        (19, 0, 7), // CreateTopics
        (20, 0, 6), // DeleteTopics
        (21, 0, 2), // DeleteRecords
        (22, 0, 5), // InitProducerId
        (24, 0, 3), // AddPartitionsToTxn
        (25, 0, 4), // AddOffsetsToTxn
        (26, 0, 4), // EndTxn
        (28, 0, 4), // TxnOffsetCommit
        (29, 0, 3), // DescribeAcls
        (30, 0, 3), // CreateAcls
        (31, 0, 3), // DeleteAcls
        (32, 0, 4), // DescribeConfigs
        (36, 0, 2), // SaslAuthenticate
        (42, 0, 2), // DeleteGroups
        (44, 0, 1), // IncrementalAlterConfigs
        (46, 0, 0), // ListPartitionReassignments
        (50, 0, 0), // DescribeUserScramCredentials
        (51, 0, 0), // AlterUserScramCredentials
        (60, 0, 1), // DescribeCluster
        (69, 0, 0), // ConsumerGroupDescribe
        (75, 0, 0), // DescribeTopicPartitions
    ];

    assert_eq!(expected, advertised);

    Ok(())
}

// `OldStyleOffsets` is on the wire only at v0, so this test encodes the request and decodes
// the response at v0.
#[tokio::test]
async fn list_offsets_v0_round_trip() -> Result<()> {
    let route_table = route_table().await?;
    let topic = "list-offsets-v0";

    let created = serve(
        &route_table,
        CreateTopicsRequest::KEY,
        7,
        CreateTopicsRequest::default()
            .validate_only(Some(false))
            .timeout_ms(5_000)
            .topics(Some(vec![
                CreatableTopic::default()
                    .name(topic.into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
            ]))
            .into(),
    )
    .await?;

    let created = CreateTopicsResponse::try_from(created.body)?;
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(created.topics.unwrap_or_default()[0].error_code)?
    );

    let bytes_frame = BytesFrameLayer::default().into_layer(route_table);

    let request = Frame::request(
        Header::Request {
            api_key: ListOffsetsRequest::KEY,
            api_version: 0,
            correlation_id: 3,
            client_id: Some("list-offsets-v0".into()),
        },
        ListOffsetsRequest::default()
            .replica_id(-1)
            .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
            .topics(Some(vec![
                ListOffsetsTopic::default()
                    .name(topic.into())
                    .partitions(Some(vec![
                        ListOffsetsPartition::default()
                            .partition_index(0)
                            .current_leader_epoch(Some(-1))
                            .max_num_offsets(Some(1))
                            .timestamp(ListOffset::Latest.try_into()?),
                    ])),
            ]))
            .into(),
    )?;

    let response: Bytes = bytes_frame
        .serve(BytesInput {
            bytes: request,
            extensions: Extensions::default(),
        })
        .await?;

    let response = Frame::response_from_bytes(response, ListOffsetsRequest::KEY, 0)?;
    let response = ListOffsetsResponse::try_from(response.body)?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());

    let partitions = topics[0].partitions.clone().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert_eq!(Some(vec![0]), partitions[0].old_style_offsets);

    Ok(())
}
