// Copyright ⓒ Solace Corporation
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

use crate::common::init_tracing;
use bytes::Bytes;
use nisshi_sans_io::{
    Body, ErrorCode, Frame, Header, Result,
    join_group_response::{JoinGroupResponse, JoinGroupResponseMember},
    offset_fetch_response::{
        OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
        OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
    },
};
use pretty_assertions::assert_eq;
use tracing::debug;

#[test]
fn join_group_response() -> Result<()> {
    let _guard = init_tracing()?;

    let throttle_time_ms = Some(0);
    let error_code = ErrorCode::None.into();
    let generation_id = 1;
    let protocol_type = Some("consumer".into());
    let protocol_name = Some("range".into());
    let leader = "rdkafka-499e5770-375e-4990-bf84-a39634e3bfe4".to_owned();
    let skip_assignment = Some(true);
    let member_id = leader.clone();
    let metadata =
        Bytes::from_static(b"\0\x03\0\0\0\x01\0\tbenchmark\0\0\0\0\0\0\0\0\xff\xff\xff\xff\0\0");
    let members = Some(
        [JoinGroupResponseMember::default()
            .member_id(member_id.clone())
            .group_instance_id(Some("group_instance_id".into()))
            .metadata(metadata.clone())]
        .into(),
    );

    let correlation_id = 4;

    let header = Header::Response { correlation_id };

    let join_group_response = JoinGroupResponse::default()
        .throttle_time_ms(throttle_time_ms)
        .error_code(error_code)
        .generation_id(generation_id)
        .protocol_type(protocol_type)
        .protocol_name(protocol_name.clone())
        .leader(leader.clone())
        .skip_assignment(skip_assignment)
        .member_id(member_id.clone())
        .members(members.clone());

    let api_key = 11;
    let api_version = 5;

    let encoded = Frame::response(header, join_group_response.into(), api_key, api_version)?;

    debug!(?encoded);

    match Frame::response_from_bytes(&encoded[..], api_key, api_version) {
        Ok(Frame {
            header: Header::Response { .. },
            body:
                Body::JoinGroupResponse(JoinGroupResponse {
                    throttle_time_ms: encoded_throttle_time_ms,
                    error_code: encoded_error_code,
                    generation_id: encoded_generation_id,
                    protocol_type: encoded_protocol_type,
                    protocol_name: encoded_protocol_name,
                    leader: encoded_leader,
                    skip_assignment: encoded_skip_assignment,
                    member_id: encoded_member_id,
                    members: encoded_members,
                    ..
                }),
            ..
        }) => {
            assert_eq!(throttle_time_ms, encoded_throttle_time_ms);
            assert_eq!(error_code, encoded_error_code);
            assert_eq!(generation_id, encoded_generation_id);
            assert_eq!(None, encoded_protocol_type);
            assert_eq!(protocol_name, encoded_protocol_name);
            assert_eq!(leader, encoded_leader);
            assert_eq!(None, encoded_skip_assignment);
            assert_eq!(member_id, encoded_member_id);
            assert_eq!(members, encoded_members);
        }
        otherwise => panic!("{otherwise:?}"),
    }

    Ok(())
}

#[test]
fn offset_fetch_response() -> Result<()> {
    let _guard = init_tracing()?;

    let correlation_id = 8;
    let header = Header::Response { correlation_id };

    let encoded_throttle_time_ms = Some(0);
    let encoded_error_code = Some(ErrorCode::None.into());
    let encoded_groups = Some(
        [OffsetFetchResponseGroup::default()
            .group_id("example_consumer_group_id".into())
            .topics(Some(
                [OffsetFetchResponseTopics::default()
                    .name("topic-name-a".into())
                    .partitions(Some(
                        [OffsetFetchResponsePartitions::default()
                            .partition_index(32123)
                            .committed_offset(4343)
                            .committed_leader_epoch(87678)
                            .metadata(Some("".into()))
                            .error_code(ErrorCode::None.into())]
                        .into(),
                    ))]
                .into(),
            ))
            .error_code(ErrorCode::None.into())]
        .into(),
    );

    let encoded_topics = Some(
        [OffsetFetchResponseTopic::default()
            .name("benchmark".into())
            .partitions(Some(
                [1, 0, 6, 5, 4, 3, 2]
                    .into_iter()
                    .map(|partition_index| {
                        OffsetFetchResponsePartition::default()
                            .partition_index(partition_index)
                            .committed_offset(-1)
                            .committed_leader_epoch(Some(-1))
                            .metadata(Some("".into()))
                            .error_code(ErrorCode::None.into())
                    })
                    .collect(),
            ))]
        .into(),
    );

    let offset_fetch_response = OffsetFetchResponse::default()
        .throttle_time_ms(encoded_throttle_time_ms)
        .error_code(encoded_error_code)
        .groups(encoded_groups.clone())
        .topics(encoded_topics.clone());

    let api_key = 9;
    let api_version = 7;

    let encoded = Frame::response(header, offset_fetch_response.into(), api_key, api_version)?;

    match Frame::response_from_bytes(&encoded[..], api_key, api_version) {
        Ok(Frame {
            header: Header::Response { .. },
            body:
                Body::OffsetFetchResponse(OffsetFetchResponse {
                    throttle_time_ms,
                    topics,
                    error_code,
                    groups,
                    ..
                }),
            ..
        }) => {
            assert_eq!(throttle_time_ms, encoded_throttle_time_ms);
            assert_eq!(topics, encoded_topics);
            assert_eq!(error_code, encoded_error_code);
            assert_eq!(groups, None);
        }

        otherwise => panic!("{otherwise:?}"),
    }

    debug!(?encoded);

    Ok(())
}
