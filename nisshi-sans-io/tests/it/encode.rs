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
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, Body, CreateTopicsResponse,
    DeleteTopicsRequest, DescribeClusterRequest, DescribeConfigsRequest, DescribeConfigsResponse,
    DescribeGroupsRequest, DescribeGroupsResponse, DescribeTopicPartitionsRequest,
    DescribeTopicPartitionsResponse, FetchRequest, FetchResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, Frame, Header, HeartbeatRequest, InitProducerIdRequest,
    JoinGroupRequest, JoinGroupResponse, ListGroupsRequest, ListOffsetsResponse,
    ListPartitionReassignmentsRequest, MaximumAllocationSize, MetadataRequest, MetadataResponse,
    OffsetFetchRequest, OffsetForLeaderEpochRequest, ProduceRequest, ProduceResponse, Result,
    fetch_request::{FetchPartition, FetchTopic},
    fetch_response::NodeEndpoint,
    join_group_request::JoinGroupRequestProtocol,
    join_group_response::JoinGroupResponseMember,
    record::{
        Record, deflated,
        inflated::{self, Batch},
    },
};
use tracing::debug;

#[test]
fn api_versions_request_v3_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 18,
        api_version: 3,
        correlation_id: 3,
        client_id: Some("console-producer".into()),
    };

    let body = ApiVersionsRequest::default()
        .client_software_name(Some("apache-kafka-java".into()))
        .client_software_version(Some("3.6.1".into()))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 52, 0, 18, 0, 3, 0, 0, 0, 3, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
            114, 111, 100, 117, 99, 101, 114, 0, 18, 97, 112, 97, 99, 104, 101, 45, 107, 97, 102,
            107, 97, 45, 106, 97, 118, 97, 6, 51, 46, 54, 46, 49, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn api_versions_response_v1_000() -> Result<()> {
    use nisshi_sans_io::api_versions_response::ApiVersion;

    let _guard = init_tracing()?;

    let header = Header::Response { correlation_id: 0 };

    let body = ApiVersionsResponse::default()
        .finalized_features(None)
        .finalized_features_epoch(None)
        .supported_features(None)
        .zk_migration_ready(None)
        .error_code(0)
        .api_keys(Some(
            [
                ApiVersion::default()
                    .api_key(0)
                    .min_version(0)
                    .max_version(5),
                ApiVersion::default()
                    .api_key(1)
                    .min_version(0)
                    .max_version(6),
                ApiVersion::default()
                    .api_key(2)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(3)
                    .min_version(0)
                    .max_version(5),
                ApiVersion::default()
                    .api_key(4)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(5)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(6)
                    .min_version(0)
                    .max_version(4),
                ApiVersion::default()
                    .api_key(7)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(8)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(9)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(10)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(11)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(12)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(13)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(14)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(15)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(16)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(17)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(18)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(19)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(20)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(21)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(22)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(23)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(24)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(25)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(26)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(27)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(28)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(29)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(30)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(31)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(32)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(33)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(34)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(35)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(36)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(37)
                    .min_version(0)
                    .max_version(0),
            ]
            .into(),
        ))
        .throttle_time_ms(Some(0))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 242, 0, 0, 0, 0, 0, 0, 0, 0, 0, 38, 0, 0, 0, 0, 0, 5, 0, 1, 0, 0, 0, 6, 0, 2,
            0, 0, 0, 2, 0, 3, 0, 0, 0, 5, 0, 4, 0, 0, 0, 1, 0, 5, 0, 0, 0, 0, 0, 6, 0, 0, 0, 4, 0,
            7, 0, 0, 0, 1, 0, 8, 0, 0, 0, 3, 0, 9, 0, 0, 0, 3, 0, 10, 0, 0, 0, 1, 0, 11, 0, 0, 0,
            2, 0, 12, 0, 0, 0, 1, 0, 13, 0, 0, 0, 1, 0, 14, 0, 0, 0, 1, 0, 15, 0, 0, 0, 1, 0, 16,
            0, 0, 0, 1, 0, 17, 0, 0, 0, 1, 0, 18, 0, 0, 0, 1, 0, 19, 0, 0, 0, 2, 0, 20, 0, 0, 0, 1,
            0, 21, 0, 0, 0, 0, 0, 22, 0, 0, 0, 0, 0, 23, 0, 0, 0, 0, 0, 24, 0, 0, 0, 0, 0, 25, 0,
            0, 0, 0, 0, 26, 0, 0, 0, 0, 0, 27, 0, 0, 0, 0, 0, 28, 0, 0, 0, 0, 0, 29, 0, 0, 0, 0, 0,
            30, 0, 0, 0, 0, 0, 31, 0, 0, 0, 0, 0, 32, 0, 0, 0, 0, 0, 33, 0, 0, 0, 0, 0, 34, 0, 0,
            0, 0, 0, 35, 0, 0, 0, 0, 0, 36, 0, 0, 0, 0, 0, 37, 0, 0, 0, 0, 0, 0, 0, 0,
        ],
        Frame::response(header, body, ApiVersionsResponse::KEY, 1)?,
    );

    Ok(())
}

#[test]
fn api_versions_response_v3_000() -> Result<()> {
    use nisshi_sans_io::api_versions_response::{
        ApiVersion, FinalizedFeatureKey, SupportedFeatureKey,
    };

    let _guard = init_tracing()?;

    let header = Header::Response { correlation_id: 0 };
    let body = ApiVersionsResponse::default()
        .finalized_features(Some(vec![
            FinalizedFeatureKey::default()
                .name("metadata.version".into())
                .min_version_level(14)
                .max_version_level(14),
        ]))
        .finalized_features_epoch(Some(76i64))
        .supported_features(Some(vec![
            SupportedFeatureKey::default()
                .name("metadata.version".into())
                .min_version(1)
                .max_version(14),
        ]))
        .zk_migration_ready(None)
        .error_code(0)
        .api_keys(Some(
            [
                ApiVersion::default()
                    .api_key(0)
                    .min_version(0)
                    .max_version(9),
                ApiVersion::default()
                    .api_key(1)
                    .min_version(0)
                    .max_version(15),
                ApiVersion::default()
                    .api_key(2)
                    .min_version(0)
                    .max_version(8),
                ApiVersion::default()
                    .api_key(3)
                    .min_version(0)
                    .max_version(12),
                ApiVersion::default()
                    .api_key(8)
                    .min_version(0)
                    .max_version(8),
                ApiVersion::default()
                    .api_key(9)
                    .min_version(0)
                    .max_version(8),
                ApiVersion::default()
                    .api_key(10)
                    .min_version(0)
                    .max_version(4),
                ApiVersion::default()
                    .api_key(11)
                    .min_version(0)
                    .max_version(9),
                ApiVersion::default()
                    .api_key(12)
                    .min_version(0)
                    .max_version(4),
                ApiVersion::default()
                    .api_key(13)
                    .min_version(0)
                    .max_version(5),
                ApiVersion::default()
                    .api_key(14)
                    .min_version(0)
                    .max_version(5),
                ApiVersion::default()
                    .api_key(15)
                    .min_version(0)
                    .max_version(5),
                ApiVersion::default()
                    .api_key(16)
                    .min_version(0)
                    .max_version(4),
                ApiVersion::default()
                    .api_key(17)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(18)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(19)
                    .min_version(0)
                    .max_version(7),
                ApiVersion::default()
                    .api_key(20)
                    .min_version(0)
                    .max_version(6),
                ApiVersion::default()
                    .api_key(21)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(22)
                    .min_version(0)
                    .max_version(4),
                ApiVersion::default()
                    .api_key(23)
                    .min_version(0)
                    .max_version(4),
                ApiVersion::default()
                    .api_key(24)
                    .min_version(0)
                    .max_version(4),
                ApiVersion::default()
                    .api_key(25)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(26)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(27)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(28)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(29)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(30)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(31)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(32)
                    .min_version(0)
                    .max_version(4),
                ApiVersion::default()
                    .api_key(33)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(34)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(35)
                    .min_version(0)
                    .max_version(4),
                ApiVersion::default()
                    .api_key(36)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(37)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(38)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(39)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(40)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(41)
                    .min_version(0)
                    .max_version(3),
                ApiVersion::default()
                    .api_key(42)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(43)
                    .min_version(0)
                    .max_version(2),
                ApiVersion::default()
                    .api_key(44)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(45)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(46)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(47)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(48)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(49)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(50)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(51)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(55)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(57)
                    .min_version(0)
                    .max_version(1),
                ApiVersion::default()
                    .api_key(60)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(61)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(64)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(65)
                    .min_version(0)
                    .max_version(0),
                ApiVersion::default()
                    .api_key(66)
                    .min_version(0)
                    .max_version(0),
            ]
            .into(),
        ))
        .throttle_time_ms(Some(0))
        .into();

    assert_eq!(
        vec![
            0, 0, 1, 201, 0, 0, 0, 0, 0, 0, 56, 0, 0, 0, 0, 0, 9, 0, 0, 1, 0, 0, 0, 15, 0, 0, 2, 0,
            0, 0, 8, 0, 0, 3, 0, 0, 0, 12, 0, 0, 8, 0, 0, 0, 8, 0, 0, 9, 0, 0, 0, 8, 0, 0, 10, 0,
            0, 0, 4, 0, 0, 11, 0, 0, 0, 9, 0, 0, 12, 0, 0, 0, 4, 0, 0, 13, 0, 0, 0, 5, 0, 0, 14, 0,
            0, 0, 5, 0, 0, 15, 0, 0, 0, 5, 0, 0, 16, 0, 0, 0, 4, 0, 0, 17, 0, 0, 0, 1, 0, 0, 18, 0,
            0, 0, 3, 0, 0, 19, 0, 0, 0, 7, 0, 0, 20, 0, 0, 0, 6, 0, 0, 21, 0, 0, 0, 2, 0, 0, 22, 0,
            0, 0, 4, 0, 0, 23, 0, 0, 0, 4, 0, 0, 24, 0, 0, 0, 4, 0, 0, 25, 0, 0, 0, 3, 0, 0, 26, 0,
            0, 0, 3, 0, 0, 27, 0, 0, 0, 1, 0, 0, 28, 0, 0, 0, 3, 0, 0, 29, 0, 0, 0, 3, 0, 0, 30, 0,
            0, 0, 3, 0, 0, 31, 0, 0, 0, 3, 0, 0, 32, 0, 0, 0, 4, 0, 0, 33, 0, 0, 0, 2, 0, 0, 34, 0,
            0, 0, 2, 0, 0, 35, 0, 0, 0, 4, 0, 0, 36, 0, 0, 0, 2, 0, 0, 37, 0, 0, 0, 3, 0, 0, 38, 0,
            0, 0, 3, 0, 0, 39, 0, 0, 0, 2, 0, 0, 40, 0, 0, 0, 2, 0, 0, 41, 0, 0, 0, 3, 0, 0, 42, 0,
            0, 0, 2, 0, 0, 43, 0, 0, 0, 2, 0, 0, 44, 0, 0, 0, 1, 0, 0, 45, 0, 0, 0, 0, 0, 0, 46, 0,
            0, 0, 0, 0, 0, 47, 0, 0, 0, 0, 0, 0, 48, 0, 0, 0, 1, 0, 0, 49, 0, 0, 0, 1, 0, 0, 50, 0,
            0, 0, 0, 0, 0, 51, 0, 0, 0, 0, 0, 0, 55, 0, 0, 0, 1, 0, 0, 57, 0, 0, 0, 1, 0, 0, 60, 0,
            0, 0, 0, 0, 0, 61, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 65, 0, 0, 0, 0, 0, 0, 66, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 23, 2, 17, 109, 101, 116, 97, 100, 97, 116, 97, 46, 118,
            101, 114, 115, 105, 111, 110, 0, 1, 0, 14, 0, 1, 8, 0, 0, 0, 0, 0, 0, 0, 76, 2, 23, 2,
            17, 109, 101, 116, 97, 100, 97, 116, 97, 46, 118, 101, 114, 115, 105, 111, 110, 0, 14,
            0, 14, 0,
        ],
        Frame::response(header, body, ApiVersionsResponse::KEY, 3)?,
    );

    Ok(())
}

#[test]
fn create_topics_request_v7_000() -> Result<()> {
    use nisshi_sans_io::{
        ApiKey as _, CreateTopicsRequest, Frame, Header,
        create_topics_request::{CreatableTopic, CreatableTopicConfig},
    };

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: CreateTopicsRequest::KEY,
        api_version: 7,
        correlation_id: 298,
        client_id: Some("adminclient-1".into()),
    };

    let body = CreateTopicsRequest::default()
        .topics(Some(
            [CreatableTopic::default()
                .name("balances".into())
                .num_partitions(-1)
                .replication_factor(-1)
                .assignments(Some([].into()))
                .configs(Some(
                    [CreatableTopicConfig::default()
                        .name("cleanup.policy".into())
                        .value(Some("compact".into()))]
                    .into(),
                ))]
            .into(),
        ))
        .timeout_ms(30_000)
        .validate_only(Some(false))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 73, 0, 19, 0, 7, 0, 0, 1, 42, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105,
            101, 110, 116, 45, 49, 0, 2, 9, 98, 97, 108, 97, 110, 99, 101, 115, 255, 255, 255, 255,
            255, 255, 1, 2, 15, 99, 108, 101, 97, 110, 117, 112, 46, 112, 111, 108, 105, 99, 121,
            8, 99, 111, 109, 112, 97, 99, 116, 0, 0, 0, 0, 117, 48, 0, 0,
        ],
        Frame::request(header, body)?
    );

    Ok(())
}

#[test]
fn create_topics_response_v7_000() -> Result<()> {
    use nisshi_sans_io::create_topics_response::{CreatableTopicConfigs, CreatableTopicResult};

    let _guard = init_tracing()?;

    let api_key = 19;
    let api_version = 7;

    let header = Header::Response {
        correlation_id: 298,
    };

    let body = CreateTopicsResponse::default()
        .throttle_time_ms(Some(0))
        .topics(Some(
            [CreatableTopicResult::default()
                .name("balances".into())
                .topic_id(Some([
                    222, 159, 182, 217, 102, 152, 68, 189, 174, 152, 214, 59, 29, 216, 240, 198,
                ]))
                .error_code(0)
                .error_message(None)
                .topic_config_error_code(None)
                .num_partitions(Some(1))
                .replication_factor(Some(1))
                .configs(Some(
                    [
                        CreatableTopicConfigs::default()
                            .name("cleanup.policy".into())
                            .value(Some("compact".into()))
                            .read_only(false)
                            .config_source(1)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("compression.type".into())
                            .value(Some("producer".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("delete.retention.ms".into())
                            .value(Some("86400000".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("file.delete.delay.ms".into())
                            .value(Some("60000".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("flush.messages".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("flush.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("follower.replication.throttled.replicas".into())
                            .value(Some("".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("index.interval.bytes".into())
                            .value(Some("4096".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("leader.replication.throttled.replicas".into())
                            .value(Some("".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("local.retention.bytes".into())
                            .value(Some("-2".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("local.retention.ms".into())
                            .value(Some("-2".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("max.compaction.lag.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("max.message.bytes".into())
                            .value(Some("1048588".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("message.downconversion.enable".into())
                            .value(Some("true".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("message.format.version".into())
                            .value(Some("3.0-IV1".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("message.timestamp.after.max.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("message.timestamp.before.max.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("message.timestamp.difference.max.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("message.timestamp.type".into())
                            .value(Some("CreateTime".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("min.cleanable.dirty.ratio".into())
                            .value(Some("0.5".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("min.compaction.lag.ms".into())
                            .value(Some("0".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("min.insync.replicas".into())
                            .value(Some("1".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("preallocate".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("remote.storage.enable".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("retention.bytes".into())
                            .value(Some("-1".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("retention.ms".into())
                            .value(Some("604800000".into()))
                            .read_only(false)
                            .config_source(4)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("segment.bytes".into())
                            .value(Some("1073741824".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("segment.index.bytes".into())
                            .value(Some("10485760".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("segment.jitter.ms".into())
                            .value(Some("0".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("segment.ms".into())
                            .value(Some("604800000".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                        CreatableTopicConfigs::default()
                            .name("unclean.leader.election.enable".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .config_source(5)
                            .is_sensitive(false),
                    ]
                    .into(),
                ))]
            .into(),
        ))
        .into();

    let encoded = vec![
        0, 0, 4, 92, 0, 0, 1, 42, 0, 0, 0, 0, 0, 2, 9, 98, 97, 108, 97, 110, 99, 101, 115, 222,
        159, 182, 217, 102, 152, 68, 189, 174, 152, 214, 59, 29, 216, 240, 198, 0, 0, 0, 0, 0, 0,
        1, 0, 1, 32, 15, 99, 108, 101, 97, 110, 117, 112, 46, 112, 111, 108, 105, 99, 121, 8, 99,
        111, 109, 112, 97, 99, 116, 0, 1, 0, 0, 17, 99, 111, 109, 112, 114, 101, 115, 115, 105,
        111, 110, 46, 116, 121, 112, 101, 9, 112, 114, 111, 100, 117, 99, 101, 114, 0, 5, 0, 0, 20,
        100, 101, 108, 101, 116, 101, 46, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 109,
        115, 9, 56, 54, 52, 48, 48, 48, 48, 48, 0, 5, 0, 0, 21, 102, 105, 108, 101, 46, 100, 101,
        108, 101, 116, 101, 46, 100, 101, 108, 97, 121, 46, 109, 115, 6, 54, 48, 48, 48, 48, 0, 5,
        0, 0, 15, 102, 108, 117, 115, 104, 46, 109, 101, 115, 115, 97, 103, 101, 115, 20, 57, 50,
        50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 0, 5, 0, 0, 9, 102,
        108, 117, 115, 104, 46, 109, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52,
        55, 55, 53, 56, 48, 55, 0, 5, 0, 0, 40, 102, 111, 108, 108, 111, 119, 101, 114, 46, 114,
        101, 112, 108, 105, 99, 97, 116, 105, 111, 110, 46, 116, 104, 114, 111, 116, 116, 108, 101,
        100, 46, 114, 101, 112, 108, 105, 99, 97, 115, 1, 0, 5, 0, 0, 21, 105, 110, 100, 101, 120,
        46, 105, 110, 116, 101, 114, 118, 97, 108, 46, 98, 121, 116, 101, 115, 5, 52, 48, 57, 54,
        0, 5, 0, 0, 38, 108, 101, 97, 100, 101, 114, 46, 114, 101, 112, 108, 105, 99, 97, 116, 105,
        111, 110, 46, 116, 104, 114, 111, 116, 116, 108, 101, 100, 46, 114, 101, 112, 108, 105, 99,
        97, 115, 1, 0, 5, 0, 0, 22, 108, 111, 99, 97, 108, 46, 114, 101, 116, 101, 110, 116, 105,
        111, 110, 46, 98, 121, 116, 101, 115, 3, 45, 50, 0, 5, 0, 0, 19, 108, 111, 99, 97, 108, 46,
        114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 109, 115, 3, 45, 50, 0, 5, 0, 0, 22, 109,
        97, 120, 46, 99, 111, 109, 112, 97, 99, 116, 105, 111, 110, 46, 108, 97, 103, 46, 109, 115,
        20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 0, 5, 0, 0,
        18, 109, 97, 120, 46, 109, 101, 115, 115, 97, 103, 101, 46, 98, 121, 116, 101, 115, 8, 49,
        48, 52, 56, 53, 56, 56, 0, 5, 0, 0, 30, 109, 101, 115, 115, 97, 103, 101, 46, 100, 111,
        119, 110, 99, 111, 110, 118, 101, 114, 115, 105, 111, 110, 46, 101, 110, 97, 98, 108, 101,
        5, 116, 114, 117, 101, 0, 5, 0, 0, 23, 109, 101, 115, 115, 97, 103, 101, 46, 102, 111, 114,
        109, 97, 116, 46, 118, 101, 114, 115, 105, 111, 110, 8, 51, 46, 48, 45, 73, 86, 49, 0, 5,
        0, 0, 31, 109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109, 101, 115, 116, 97, 109, 112,
        46, 97, 102, 116, 101, 114, 46, 109, 97, 120, 46, 109, 115, 20, 57, 50, 50, 51, 51, 55, 50,
        48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 0, 5, 0, 0, 32, 109, 101, 115, 115, 97,
        103, 101, 46, 116, 105, 109, 101, 115, 116, 97, 109, 112, 46, 98, 101, 102, 111, 114, 101,
        46, 109, 97, 120, 46, 109, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55,
        55, 53, 56, 48, 55, 0, 5, 0, 0, 36, 109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109,
        101, 115, 116, 97, 109, 112, 46, 100, 105, 102, 102, 101, 114, 101, 110, 99, 101, 46, 109,
        97, 120, 46, 109, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53,
        56, 48, 55, 0, 5, 0, 0, 23, 109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109, 101, 115,
        116, 97, 109, 112, 46, 116, 121, 112, 101, 11, 67, 114, 101, 97, 116, 101, 84, 105, 109,
        101, 0, 5, 0, 0, 26, 109, 105, 110, 46, 99, 108, 101, 97, 110, 97, 98, 108, 101, 46, 100,
        105, 114, 116, 121, 46, 114, 97, 116, 105, 111, 4, 48, 46, 53, 0, 5, 0, 0, 22, 109, 105,
        110, 46, 99, 111, 109, 112, 97, 99, 116, 105, 111, 110, 46, 108, 97, 103, 46, 109, 115, 2,
        48, 0, 5, 0, 0, 20, 109, 105, 110, 46, 105, 110, 115, 121, 110, 99, 46, 114, 101, 112, 108,
        105, 99, 97, 115, 2, 49, 0, 5, 0, 0, 12, 112, 114, 101, 97, 108, 108, 111, 99, 97, 116,
        101, 6, 102, 97, 108, 115, 101, 0, 5, 0, 0, 22, 114, 101, 109, 111, 116, 101, 46, 115, 116,
        111, 114, 97, 103, 101, 46, 101, 110, 97, 98, 108, 101, 6, 102, 97, 108, 115, 101, 0, 5, 0,
        0, 16, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 98, 121, 116, 101, 115, 3, 45, 49,
        0, 5, 0, 0, 13, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 109, 115, 10, 54, 48, 52,
        56, 48, 48, 48, 48, 48, 0, 4, 0, 0, 14, 115, 101, 103, 109, 101, 110, 116, 46, 98, 121,
        116, 101, 115, 11, 49, 48, 55, 51, 55, 52, 49, 56, 50, 52, 0, 5, 0, 0, 20, 115, 101, 103,
        109, 101, 110, 116, 46, 105, 110, 100, 101, 120, 46, 98, 121, 116, 101, 115, 9, 49, 48, 52,
        56, 53, 55, 54, 48, 0, 5, 0, 0, 18, 115, 101, 103, 109, 101, 110, 116, 46, 106, 105, 116,
        116, 101, 114, 46, 109, 115, 2, 48, 0, 5, 0, 0, 11, 115, 101, 103, 109, 101, 110, 116, 46,
        109, 115, 10, 54, 48, 52, 56, 48, 48, 48, 48, 48, 0, 5, 0, 0, 31, 117, 110, 99, 108, 101,
        97, 110, 46, 108, 101, 97, 100, 101, 114, 46, 101, 108, 101, 99, 116, 105, 111, 110, 46,
        101, 110, 97, 98, 108, 101, 6, 102, 97, 108, 115, 101, 0, 5, 0, 0, 0, 0,
    ];

    assert_eq!(
        encoded,
        Frame::response(header, body, api_key, api_version)?
    );

    Ok(())
}

#[test]
fn delete_topics_request_v6_000() -> Result<()> {
    use nisshi_sans_io::delete_topics_request::DeleteTopicState;

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 20,
        api_version: 6,
        correlation_id: 4,
        client_id: Some("adminclient-1".into()),
    };

    let body = DeleteTopicsRequest::default()
        .topics(Some(
            [DeleteTopicState::default()
                .name(Some("test".into()))
                .topic_id([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])]
            .into(),
        ))
        .topic_names(None)
        .timeout_ms(30000)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 52, 0, 20, 0, 6, 0, 0, 0, 4, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
            110, 116, 45, 49, 0, 2, 5, 116, 101, 115, 116, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 117, 48, 0,
        ],
        Frame::request(header, body)?,
    );
    Ok(())
}

#[test]
fn describe_cluster_request_v1_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 60,
        api_version: 1,
        correlation_id: 7,
        client_id: Some("adminclient-1".into()),
    };

    let body = DescribeClusterRequest::default()
        .include_cluster_authorized_operations(false)
        .endpoint_type(Some(1))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 27, 0, 60, 0, 1, 0, 0, 0, 7, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
            110, 116, 45, 49, 0, 0, 1, 0,
        ],
        Frame::request(header, body)?,
    );
    Ok(())
}

#[test]
fn describe_configs_request_v4_000() -> Result<()> {
    use nisshi_sans_io::describe_configs_request::DescribeConfigsResource;

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 32,
        api_version: 4,
        correlation_id: 5,
        client_id: Some("adminclient-1".into()),
    };

    let body = DescribeConfigsRequest::default()
        .resources(Some(
            [DescribeConfigsResource::default()
                .resource_type(2)
                .resource_name("test".into())
                .configuration_keys(None)]
            .into(),
        ))
        .include_synonyms(Some(false))
        .include_documentation(Some(false))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 36, 0, 32, 0, 4, 0, 0, 0, 5, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
            110, 116, 45, 49, 0, 2, 2, 5, 116, 101, 115, 116, 0, 0, 0, 0, 0,
        ],
        Frame::request(header, body)?,
    );
    Ok(())
}

#[test]
fn describe_configs_request_v4_001() -> Result<()> {
    use nisshi_sans_io::describe_configs_request::DescribeConfigsResource;

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 32,
        api_version: 4,
        correlation_id: 3,
        client_id: Some("adminclient-1".into()),
    };

    let body = DescribeConfigsRequest::default()
        .resources(Some(
            [DescribeConfigsResource::default()
                .resource_type(2)
                .resource_name("_schemas".into())
                .configuration_keys(None)]
            .into(),
        ))
        .include_synonyms(Some(true))
        .include_documentation(Some(false))
        .into();

    let v = vec![
        0, 0, 0, 40, 0, 32, 0, 4, 0, 0, 0, 3, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
        110, 116, 45, 49, 0, 2, 2, 9, 95, 115, 99, 104, 101, 109, 97, 115, 0, 0, 1, 0, 0,
    ];

    assert_eq!(v, Frame::request(header, body)?);

    Ok(())
}

#[test]
fn describe_configs_request_v4_002() -> Result<()> {
    use nisshi_sans_io::describe_configs_request::DescribeConfigsResource;

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 32,
        api_version: 4,
        correlation_id: 6,
        client_id: Some("adminclient-1".into()),
    };

    let body = DescribeConfigsRequest::default()
        .resources(Some(
            [DescribeConfigsResource::default()
                .resource_type(2)
                .resource_name("test".into())
                .configuration_keys(None)]
            .into(),
        ))
        .include_synonyms(Some(false))
        .include_documentation(Some(false))
        .into();

    let v = vec![
        0, 0, 0, 36, 0, 32, 0, 4, 0, 0, 0, 6, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
        110, 116, 45, 49, 0, 2, 2, 5, 116, 101, 115, 116, 0, 0, 0, 0, 0,
    ];

    assert_eq!(v, Frame::request(header, body)?);

    Ok(())
}

#[test]
fn describe_configs_response_v4_001() -> Result<()> {
    use nisshi_sans_io::describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsResult, DescribeConfigsSynonym,
    };

    let _guard = init_tracing()?;

    let expected = vec![
        0, 0, 9, 26, 0, 0, 0, 3, 0, 0, 0, 0, 0, 2, 0, 0, 1, 2, 9, 95, 115, 99, 104, 101, 109, 97,
        115, 35, 17, 99, 111, 109, 112, 114, 101, 115, 115, 105, 111, 110, 46, 116, 121, 112, 101,
        9, 112, 114, 111, 100, 117, 99, 101, 114, 0, 5, 0, 2, 17, 99, 111, 109, 112, 114, 101, 115,
        115, 105, 111, 110, 46, 116, 121, 112, 101, 9, 112, 114, 111, 100, 117, 99, 101, 114, 5, 0,
        2, 0, 0, 38, 108, 101, 97, 100, 101, 114, 46, 114, 101, 112, 108, 105, 99, 97, 116, 105,
        111, 110, 46, 116, 104, 114, 111, 116, 116, 108, 101, 100, 46, 114, 101, 112, 108, 105, 99,
        97, 115, 1, 0, 5, 0, 1, 7, 0, 0, 22, 114, 101, 109, 111, 116, 101, 46, 115, 116, 111, 114,
        97, 103, 101, 46, 101, 110, 97, 98, 108, 101, 6, 102, 97, 108, 115, 101, 0, 5, 0, 1, 1, 0,
        0, 30, 109, 101, 115, 115, 97, 103, 101, 46, 100, 111, 119, 110, 99, 111, 110, 118, 101,
        114, 115, 105, 111, 110, 46, 101, 110, 97, 98, 108, 101, 5, 116, 114, 117, 101, 0, 5, 0, 2,
        34, 108, 111, 103, 46, 109, 101, 115, 115, 97, 103, 101, 46, 100, 111, 119, 110, 99, 111,
        110, 118, 101, 114, 115, 105, 111, 110, 46, 101, 110, 97, 98, 108, 101, 5, 116, 114, 117,
        101, 5, 0, 1, 0, 0, 20, 109, 105, 110, 46, 105, 110, 115, 121, 110, 99, 46, 114, 101, 112,
        108, 105, 99, 97, 115, 2, 49, 0, 5, 0, 2, 20, 109, 105, 110, 46, 105, 110, 115, 121, 110,
        99, 46, 114, 101, 112, 108, 105, 99, 97, 115, 2, 49, 5, 0, 3, 0, 0, 18, 115, 101, 103, 109,
        101, 110, 116, 46, 106, 105, 116, 116, 101, 114, 46, 109, 115, 2, 48, 0, 5, 0, 1, 5, 0, 0,
        19, 108, 111, 99, 97, 108, 46, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 109, 115,
        3, 45, 50, 0, 5, 0, 2, 23, 108, 111, 103, 46, 108, 111, 99, 97, 108, 46, 114, 101, 116,
        101, 110, 116, 105, 111, 110, 46, 109, 115, 3, 45, 50, 5, 0, 5, 0, 0, 15, 99, 108, 101, 97,
        110, 117, 112, 46, 112, 111, 108, 105, 99, 121, 7, 100, 101, 108, 101, 116, 101, 0, 5, 0,
        2, 19, 108, 111, 103, 46, 99, 108, 101, 97, 110, 117, 112, 46, 112, 111, 108, 105, 99, 121,
        7, 100, 101, 108, 101, 116, 101, 5, 0, 7, 0, 0, 9, 102, 108, 117, 115, 104, 46, 109, 115,
        20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 0, 5, 0, 1,
        5, 0, 0, 40, 102, 111, 108, 108, 111, 119, 101, 114, 46, 114, 101, 112, 108, 105, 99, 97,
        116, 105, 111, 110, 46, 116, 104, 114, 111, 116, 116, 108, 101, 100, 46, 114, 101, 112,
        108, 105, 99, 97, 115, 1, 0, 5, 0, 1, 7, 0, 0, 22, 99, 111, 109, 112, 114, 101, 115, 115,
        105, 111, 110, 46, 108, 122, 52, 46, 108, 101, 118, 101, 108, 2, 57, 0, 5, 0, 2, 22, 99,
        111, 109, 112, 114, 101, 115, 115, 105, 111, 110, 46, 108, 122, 52, 46, 108, 101, 118, 101,
        108, 2, 57, 5, 0, 3, 0, 0, 14, 115, 101, 103, 109, 101, 110, 116, 46, 98, 121, 116, 101,
        115, 11, 49, 48, 55, 51, 55, 52, 49, 56, 50, 52, 0, 4, 0, 3, 18, 108, 111, 103, 46, 115,
        101, 103, 109, 101, 110, 116, 46, 98, 121, 116, 101, 115, 11, 49, 48, 55, 51, 55, 52, 49,
        56, 50, 52, 4, 0, 18, 108, 111, 103, 46, 115, 101, 103, 109, 101, 110, 116, 46, 98, 121,
        116, 101, 115, 11, 49, 48, 55, 51, 55, 52, 49, 56, 50, 52, 5, 0, 3, 0, 0, 13, 114, 101,
        116, 101, 110, 116, 105, 111, 110, 46, 109, 115, 10, 54, 48, 52, 56, 48, 48, 48, 48, 48, 0,
        5, 0, 1, 5, 0, 0, 23, 99, 111, 109, 112, 114, 101, 115, 115, 105, 111, 110, 46, 103, 122,
        105, 112, 46, 108, 101, 118, 101, 108, 3, 45, 49, 0, 5, 0, 2, 23, 99, 111, 109, 112, 114,
        101, 115, 115, 105, 111, 110, 46, 103, 122, 105, 112, 46, 108, 101, 118, 101, 108, 3, 45,
        49, 5, 0, 3, 0, 0, 15, 102, 108, 117, 115, 104, 46, 109, 101, 115, 115, 97, 103, 101, 115,
        2, 49, 0, 1, 0, 3, 15, 102, 108, 117, 115, 104, 46, 109, 101, 115, 115, 97, 103, 101, 115,
        2, 49, 1, 0, 28, 108, 111, 103, 46, 102, 108, 117, 115, 104, 46, 105, 110, 116, 101, 114,
        118, 97, 108, 46, 109, 101, 115, 115, 97, 103, 101, 115, 20, 57, 50, 50, 51, 51, 55, 50,
        48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 5, 0, 5, 0, 0, 23, 99, 111, 109, 112, 114,
        101, 115, 115, 105, 111, 110, 46, 122, 115, 116, 100, 46, 108, 101, 118, 101, 108, 2, 51,
        0, 5, 0, 2, 23, 99, 111, 109, 112, 114, 101, 115, 115, 105, 111, 110, 46, 122, 115, 116,
        100, 46, 108, 101, 118, 101, 108, 2, 51, 5, 0, 3, 0, 0, 23, 109, 101, 115, 115, 97, 103,
        101, 46, 102, 111, 114, 109, 97, 116, 46, 118, 101, 114, 115, 105, 111, 110, 8, 51, 46, 48,
        45, 73, 86, 49, 0, 5, 0, 2, 27, 108, 111, 103, 46, 109, 101, 115, 115, 97, 103, 101, 46,
        102, 111, 114, 109, 97, 116, 46, 118, 101, 114, 115, 105, 111, 110, 8, 51, 46, 48, 45, 73,
        86, 49, 5, 0, 2, 0, 0, 22, 109, 97, 120, 46, 99, 111, 109, 112, 97, 99, 116, 105, 111, 110,
        46, 108, 97, 103, 46, 109, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55,
        55, 53, 56, 48, 55, 0, 5, 0, 2, 34, 108, 111, 103, 46, 99, 108, 101, 97, 110, 101, 114, 46,
        109, 97, 120, 46, 99, 111, 109, 112, 97, 99, 116, 105, 111, 110, 46, 108, 97, 103, 46, 109,
        115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 5, 0,
        5, 0, 0, 21, 102, 105, 108, 101, 46, 100, 101, 108, 101, 116, 101, 46, 100, 101, 108, 97,
        121, 46, 109, 115, 6, 54, 48, 48, 48, 48, 0, 5, 0, 2, 28, 108, 111, 103, 46, 115, 101, 103,
        109, 101, 110, 116, 46, 100, 101, 108, 101, 116, 101, 46, 100, 101, 108, 97, 121, 46, 109,
        115, 6, 54, 48, 48, 48, 48, 5, 0, 5, 0, 0, 18, 109, 97, 120, 46, 109, 101, 115, 115, 97,
        103, 101, 46, 98, 121, 116, 101, 115, 6, 54, 52, 48, 48, 48, 0, 1, 0, 3, 18, 109, 97, 120,
        46, 109, 101, 115, 115, 97, 103, 101, 46, 98, 121, 116, 101, 115, 6, 54, 52, 48, 48, 48, 1,
        0, 18, 109, 101, 115, 115, 97, 103, 101, 46, 109, 97, 120, 46, 98, 121, 116, 101, 115, 8,
        49, 48, 52, 56, 53, 56, 56, 5, 0, 3, 0, 0, 22, 109, 105, 110, 46, 99, 111, 109, 112, 97,
        99, 116, 105, 111, 110, 46, 108, 97, 103, 46, 109, 115, 2, 48, 0, 5, 0, 2, 34, 108, 111,
        103, 46, 99, 108, 101, 97, 110, 101, 114, 46, 109, 105, 110, 46, 99, 111, 109, 112, 97, 99,
        116, 105, 111, 110, 46, 108, 97, 103, 46, 109, 115, 2, 48, 5, 0, 5, 0, 0, 23, 109, 101,
        115, 115, 97, 103, 101, 46, 116, 105, 109, 101, 115, 116, 97, 109, 112, 46, 116, 121, 112,
        101, 11, 67, 114, 101, 97, 116, 101, 84, 105, 109, 101, 0, 5, 0, 2, 27, 108, 111, 103, 46,
        109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109, 101, 115, 116, 97, 109, 112, 46, 116,
        121, 112, 101, 11, 67, 114, 101, 97, 116, 101, 84, 105, 109, 101, 5, 0, 2, 0, 0, 22, 108,
        111, 99, 97, 108, 46, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 98, 121, 116, 101,
        115, 3, 45, 50, 0, 5, 0, 2, 26, 108, 111, 103, 46, 108, 111, 99, 97, 108, 46, 114, 101,
        116, 101, 110, 116, 105, 111, 110, 46, 98, 121, 116, 101, 115, 3, 45, 50, 5, 0, 5, 0, 0,
        12, 112, 114, 101, 97, 108, 108, 111, 99, 97, 116, 101, 6, 102, 97, 108, 115, 101, 0, 5, 0,
        2, 16, 108, 111, 103, 46, 112, 114, 101, 97, 108, 108, 111, 99, 97, 116, 101, 6, 102, 97,
        108, 115, 101, 5, 0, 1, 0, 0, 26, 109, 105, 110, 46, 99, 108, 101, 97, 110, 97, 98, 108,
        101, 46, 100, 105, 114, 116, 121, 46, 114, 97, 116, 105, 111, 4, 48, 46, 53, 0, 5, 0, 2,
        32, 108, 111, 103, 46, 99, 108, 101, 97, 110, 101, 114, 46, 109, 105, 110, 46, 99, 108,
        101, 97, 110, 97, 98, 108, 101, 46, 114, 97, 116, 105, 111, 4, 48, 46, 53, 5, 0, 6, 0, 0,
        21, 105, 110, 100, 101, 120, 46, 105, 110, 116, 101, 114, 118, 97, 108, 46, 98, 121, 116,
        101, 115, 5, 52, 48, 57, 54, 0, 5, 0, 2, 25, 108, 111, 103, 46, 105, 110, 100, 101, 120,
        46, 105, 110, 116, 101, 114, 118, 97, 108, 46, 98, 121, 116, 101, 115, 5, 52, 48, 57, 54,
        5, 0, 3, 0, 0, 31, 117, 110, 99, 108, 101, 97, 110, 46, 108, 101, 97, 100, 101, 114, 46,
        101, 108, 101, 99, 116, 105, 111, 110, 46, 101, 110, 97, 98, 108, 101, 6, 102, 97, 108,
        115, 101, 0, 5, 0, 2, 31, 117, 110, 99, 108, 101, 97, 110, 46, 108, 101, 97, 100, 101, 114,
        46, 101, 108, 101, 99, 116, 105, 111, 110, 46, 101, 110, 97, 98, 108, 101, 6, 102, 97, 108,
        115, 101, 5, 0, 1, 0, 0, 16, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 98, 121, 116,
        101, 115, 3, 45, 49, 0, 5, 0, 2, 20, 108, 111, 103, 46, 114, 101, 116, 101, 110, 116, 105,
        111, 110, 46, 98, 121, 116, 101, 115, 3, 45, 49, 5, 0, 5, 0, 0, 20, 100, 101, 108, 101,
        116, 101, 46, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 109, 115, 9, 56, 54, 52, 48,
        48, 48, 48, 48, 0, 5, 0, 2, 32, 108, 111, 103, 46, 99, 108, 101, 97, 110, 101, 114, 46,
        100, 101, 108, 101, 116, 101, 46, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 109,
        115, 9, 56, 54, 52, 48, 48, 48, 48, 48, 5, 0, 5, 0, 0, 31, 109, 101, 115, 115, 97, 103,
        101, 46, 116, 105, 109, 101, 115, 116, 97, 109, 112, 46, 97, 102, 116, 101, 114, 46, 109,
        97, 120, 46, 109, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53,
        56, 48, 55, 0, 5, 0, 2, 35, 108, 111, 103, 46, 109, 101, 115, 115, 97, 103, 101, 46, 116,
        105, 109, 101, 115, 116, 97, 109, 112, 46, 97, 102, 116, 101, 114, 46, 109, 97, 120, 46,
        109, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55,
        5, 0, 5, 0, 0, 32, 109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109, 101, 115, 116, 97,
        109, 112, 46, 98, 101, 102, 111, 114, 101, 46, 109, 97, 120, 46, 109, 115, 20, 57, 50, 50,
        51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 0, 5, 0, 2, 36, 108, 111,
        103, 46, 109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109, 101, 115, 116, 97, 109, 112,
        46, 98, 101, 102, 111, 114, 101, 46, 109, 97, 120, 46, 109, 115, 20, 57, 50, 50, 51, 51,
        55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 5, 0, 5, 0, 0, 11, 115, 101, 103,
        109, 101, 110, 116, 46, 109, 115, 10, 54, 48, 52, 56, 48, 48, 48, 48, 48, 0, 5, 0, 1, 5, 0,
        0, 36, 109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109, 101, 115, 116, 97, 109, 112,
        46, 100, 105, 102, 102, 101, 114, 101, 110, 99, 101, 46, 109, 97, 120, 46, 109, 115, 20,
        57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 0, 5, 0, 2, 40,
        108, 111, 103, 46, 109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109, 101, 115, 116, 97,
        109, 112, 46, 100, 105, 102, 102, 101, 114, 101, 110, 99, 101, 46, 109, 97, 120, 46, 109,
        115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 5, 0,
        5, 0, 0, 20, 115, 101, 103, 109, 101, 110, 116, 46, 105, 110, 100, 101, 120, 46, 98, 121,
        116, 101, 115, 9, 49, 48, 52, 56, 53, 55, 54, 48, 0, 5, 0, 2, 25, 108, 111, 103, 46, 105,
        110, 100, 101, 120, 46, 115, 105, 122, 101, 46, 109, 97, 120, 46, 98, 121, 116, 101, 115,
        9, 49, 48, 52, 56, 53, 55, 54, 48, 5, 0, 3, 0, 0, 0, 0,
    ];

    let api_key = 32;
    let api_version = 4;

    let header = Header::Response { correlation_id: 3 };
    let body = DescribeConfigsResponse::default()
        .throttle_time_ms(0)
        .results(Some(
            [DescribeConfigsResult::default()
                .error_code(0)
                .error_message(Some("".into()))
                .resource_type(2)
                .resource_name("_schemas".into())
                .configs(Some(
                    [
                        DescribeConfigsResourceResult::default()
                            .name("compression.type".into())
                            .value(Some("producer".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("compression.type".into())
                                    .value(Some("producer".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(2))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("leader.replication.throttled.replicas".into())
                            .value(Some("".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(7))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("remote.storage.enable".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.downconversion.enable".into())
                            .value(Some("true".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.message.downconversion.enable".into())
                                    .value(Some("true".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("min.insync.replicas".into())
                            .value(Some("1".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("min.insync.replicas".into())
                                    .value(Some("1".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("segment.jitter.ms".into())
                            .value(Some("0".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("local.retention.ms".into())
                            .value(Some("-2".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.local.retention.ms".into())
                                    .value(Some("-2".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("cleanup.policy".into())
                            .value(Some("delete".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.cleanup.policy".into())
                                    .value(Some("delete".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(7))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("flush.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("follower.replication.throttled.replicas".into())
                            .value(Some("".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(7))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("compression.lz4.level".into())
                            .value(Some("9".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("compression.lz4.level".into())
                                    .value(Some("9".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("segment.bytes".into())
                            .value(Some("1073741824".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(4))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [
                                    DescribeConfigsSynonym::default()
                                        .name("log.segment.bytes".into())
                                        .value(Some("1073741824".into()))
                                        .source(4),
                                    DescribeConfigsSynonym::default()
                                        .name("log.segment.bytes".into())
                                        .value(Some("1073741824".into()))
                                        .source(5),
                                ]
                                .into(),
                            ))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("retention.ms".into())
                            .value(Some("604800000".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("compression.gzip.level".into())
                            .value(Some("-1".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("compression.gzip.level".into())
                                    .value(Some("-1".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("flush.messages".into())
                            .value(Some("1".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(1))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [
                                    DescribeConfigsSynonym::default()
                                        .name("flush.messages".into())
                                        .value(Some("1".into()))
                                        .source(1),
                                    DescribeConfigsSynonym::default()
                                        .name("log.flush.interval.messages".into())
                                        .value(Some("9223372036854775807".into()))
                                        .source(5),
                                ]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("compression.zstd.level".into())
                            .value(Some("3".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("compression.zstd.level".into())
                                    .value(Some("3".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.format.version".into())
                            .value(Some("3.0-IV1".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.message.format.version".into())
                                    .value(Some("3.0-IV1".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(2))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("max.compaction.lag.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.cleaner.max.compaction.lag.ms".into())
                                    .value(Some("9223372036854775807".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("file.delete.delay.ms".into())
                            .value(Some("60000".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.segment.delete.delay.ms".into())
                                    .value(Some("60000".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("max.message.bytes".into())
                            .value(Some("64000".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(1))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [
                                    DescribeConfigsSynonym::default()
                                        .name("max.message.bytes".into())
                                        .value(Some("64000".into()))
                                        .source(1),
                                    DescribeConfigsSynonym::default()
                                        .name("message.max.bytes".into())
                                        .value(Some("1048588".into()))
                                        .source(5),
                                ]
                                .into(),
                            ))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("min.compaction.lag.ms".into())
                            .value(Some("0".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.cleaner.min.compaction.lag.ms".into())
                                    .value(Some("0".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.timestamp.type".into())
                            .value(Some("CreateTime".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.message.timestamp.type".into())
                                    .value(Some("CreateTime".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(2))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("local.retention.bytes".into())
                            .value(Some("-2".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.local.retention.bytes".into())
                                    .value(Some("-2".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("preallocate".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.preallocate".into())
                                    .value(Some("false".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("min.cleanable.dirty.ratio".into())
                            .value(Some("0.5".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.cleaner.min.cleanable.ratio".into())
                                    .value(Some("0.5".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(6))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("index.interval.bytes".into())
                            .value(Some("4096".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.index.interval.bytes".into())
                                    .value(Some("4096".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("unclean.leader.election.enable".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("unclean.leader.election.enable".into())
                                    .value(Some("false".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("retention.bytes".into())
                            .value(Some("-1".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.retention.bytes".into())
                                    .value(Some("-1".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("delete.retention.ms".into())
                            .value(Some("86400000".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.cleaner.delete.retention.ms".into())
                                    .value(Some("86400000".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.timestamp.after.max.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.message.timestamp.after.max.ms".into())
                                    .value(Some("9223372036854775807".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.timestamp.before.max.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.message.timestamp.before.max.ms".into())
                                    .value(Some("9223372036854775807".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("segment.ms".into())
                            .value(Some("604800000".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.timestamp.difference.max.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.message.timestamp.difference.max.ms".into())
                                    .value(Some("9223372036854775807".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("segment.index.bytes".into())
                            .value(Some("10485760".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some(
                                [DescribeConfigsSynonym::default()
                                    .name("log.index.size.max.bytes".into())
                                    .value(Some("10485760".into()))
                                    .source(5)]
                                .into(),
                            ))
                            .config_type(Some(3))
                            .documentation(None),
                    ]
                    .into(),
                ))]
            .into(),
        ))
        .into();

    assert_eq!(
        expected,
        Frame::response(header, body, api_key, api_version)?
    );

    Ok(())
}

#[test]
fn describe_configs_response_v4_002() -> Result<()> {
    use nisshi_sans_io::describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsResult,
    };

    let _guard = init_tracing()?;

    let expected = vec![
        0, 0, 5, 79, 0, 0, 0, 6, 0, 0, 0, 0, 0, 2, 0, 0, 1, 2, 5, 116, 101, 115, 116, 37, 17, 99,
        111, 109, 112, 114, 101, 115, 115, 105, 111, 110, 46, 116, 121, 112, 101, 9, 112, 114, 111,
        100, 117, 99, 101, 114, 0, 5, 0, 1, 2, 0, 0, 29, 114, 101, 109, 111, 116, 101, 46, 108,
        111, 103, 46, 100, 101, 108, 101, 116, 101, 46, 111, 110, 46, 100, 105, 115, 97, 98, 108,
        101, 6, 102, 97, 108, 115, 101, 0, 5, 0, 1, 1, 0, 0, 38, 108, 101, 97, 100, 101, 114, 46,
        114, 101, 112, 108, 105, 99, 97, 116, 105, 111, 110, 46, 116, 104, 114, 111, 116, 116, 108,
        101, 100, 46, 114, 101, 112, 108, 105, 99, 97, 115, 1, 0, 5, 0, 1, 7, 0, 0, 22, 114, 101,
        109, 111, 116, 101, 46, 115, 116, 111, 114, 97, 103, 101, 46, 101, 110, 97, 98, 108, 101,
        6, 102, 97, 108, 115, 101, 0, 5, 0, 1, 1, 0, 0, 30, 109, 101, 115, 115, 97, 103, 101, 46,
        100, 111, 119, 110, 99, 111, 110, 118, 101, 114, 115, 105, 111, 110, 46, 101, 110, 97, 98,
        108, 101, 5, 116, 114, 117, 101, 0, 5, 0, 1, 1, 0, 0, 20, 109, 105, 110, 46, 105, 110, 115,
        121, 110, 99, 46, 114, 101, 112, 108, 105, 99, 97, 115, 2, 49, 0, 5, 0, 1, 3, 0, 0, 18,
        115, 101, 103, 109, 101, 110, 116, 46, 106, 105, 116, 116, 101, 114, 46, 109, 115, 2, 48,
        0, 5, 0, 1, 5, 0, 0, 24, 114, 101, 109, 111, 116, 101, 46, 108, 111, 103, 46, 99, 111, 112,
        121, 46, 100, 105, 115, 97, 98, 108, 101, 6, 102, 97, 108, 115, 101, 0, 5, 0, 1, 1, 0, 0,
        19, 108, 111, 99, 97, 108, 46, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 109, 115,
        3, 45, 50, 0, 5, 0, 1, 5, 0, 0, 15, 99, 108, 101, 97, 110, 117, 112, 46, 112, 111, 108,
        105, 99, 121, 8, 99, 111, 109, 112, 97, 99, 116, 0, 1, 0, 1, 7, 0, 0, 9, 102, 108, 117,
        115, 104, 46, 109, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53,
        56, 48, 55, 0, 5, 0, 1, 5, 0, 0, 40, 102, 111, 108, 108, 111, 119, 101, 114, 46, 114, 101,
        112, 108, 105, 99, 97, 116, 105, 111, 110, 46, 116, 104, 114, 111, 116, 116, 108, 101, 100,
        46, 114, 101, 112, 108, 105, 99, 97, 115, 1, 0, 5, 0, 1, 7, 0, 0, 22, 99, 111, 109, 112,
        114, 101, 115, 115, 105, 111, 110, 46, 108, 122, 52, 46, 108, 101, 118, 101, 108, 2, 57, 0,
        5, 0, 1, 3, 0, 0, 14, 115, 101, 103, 109, 101, 110, 116, 46, 98, 121, 116, 101, 115, 11,
        49, 48, 55, 51, 55, 52, 49, 56, 50, 52, 0, 4, 0, 1, 3, 0, 0, 13, 114, 101, 116, 101, 110,
        116, 105, 111, 110, 46, 109, 115, 10, 54, 48, 52, 56, 48, 48, 48, 48, 48, 0, 5, 0, 1, 5, 0,
        0, 23, 99, 111, 109, 112, 114, 101, 115, 115, 105, 111, 110, 46, 103, 122, 105, 112, 46,
        108, 101, 118, 101, 108, 3, 45, 49, 0, 5, 0, 1, 3, 0, 0, 15, 102, 108, 117, 115, 104, 46,
        109, 101, 115, 115, 97, 103, 101, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53,
        52, 55, 55, 53, 56, 48, 55, 0, 5, 0, 1, 5, 0, 0, 23, 99, 111, 109, 112, 114, 101, 115, 115,
        105, 111, 110, 46, 122, 115, 116, 100, 46, 108, 101, 118, 101, 108, 2, 51, 0, 5, 0, 1, 3,
        0, 0, 23, 109, 101, 115, 115, 97, 103, 101, 46, 102, 111, 114, 109, 97, 116, 46, 118, 101,
        114, 115, 105, 111, 110, 8, 51, 46, 48, 45, 73, 86, 49, 0, 5, 0, 1, 2, 0, 0, 22, 109, 97,
        120, 46, 99, 111, 109, 112, 97, 99, 116, 105, 111, 110, 46, 108, 97, 103, 46, 109, 115, 20,
        57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 0, 5, 0, 1, 5,
        0, 0, 21, 102, 105, 108, 101, 46, 100, 101, 108, 101, 116, 101, 46, 100, 101, 108, 97, 121,
        46, 109, 115, 6, 54, 48, 48, 48, 48, 0, 5, 0, 1, 5, 0, 0, 18, 109, 97, 120, 46, 109, 101,
        115, 115, 97, 103, 101, 46, 98, 121, 116, 101, 115, 8, 49, 48, 52, 56, 53, 56, 56, 0, 5, 0,
        1, 3, 0, 0, 22, 109, 105, 110, 46, 99, 111, 109, 112, 97, 99, 116, 105, 111, 110, 46, 108,
        97, 103, 46, 109, 115, 2, 48, 0, 5, 0, 1, 5, 0, 0, 23, 109, 101, 115, 115, 97, 103, 101,
        46, 116, 105, 109, 101, 115, 116, 97, 109, 112, 46, 116, 121, 112, 101, 11, 67, 114, 101,
        97, 116, 101, 84, 105, 109, 101, 0, 5, 0, 1, 2, 0, 0, 22, 108, 111, 99, 97, 108, 46, 114,
        101, 116, 101, 110, 116, 105, 111, 110, 46, 98, 121, 116, 101, 115, 3, 45, 50, 0, 5, 0, 1,
        5, 0, 0, 12, 112, 114, 101, 97, 108, 108, 111, 99, 97, 116, 101, 6, 102, 97, 108, 115, 101,
        0, 5, 0, 1, 1, 0, 0, 26, 109, 105, 110, 46, 99, 108, 101, 97, 110, 97, 98, 108, 101, 46,
        100, 105, 114, 116, 121, 46, 114, 97, 116, 105, 111, 4, 48, 46, 53, 0, 5, 0, 1, 6, 0, 0,
        21, 105, 110, 100, 101, 120, 46, 105, 110, 116, 101, 114, 118, 97, 108, 46, 98, 121, 116,
        101, 115, 5, 52, 48, 57, 54, 0, 5, 0, 1, 3, 0, 0, 31, 117, 110, 99, 108, 101, 97, 110, 46,
        108, 101, 97, 100, 101, 114, 46, 101, 108, 101, 99, 116, 105, 111, 110, 46, 101, 110, 97,
        98, 108, 101, 6, 102, 97, 108, 115, 101, 0, 5, 0, 1, 1, 0, 0, 16, 114, 101, 116, 101, 110,
        116, 105, 111, 110, 46, 98, 121, 116, 101, 115, 3, 45, 49, 0, 5, 0, 1, 5, 0, 0, 20, 100,
        101, 108, 101, 116, 101, 46, 114, 101, 116, 101, 110, 116, 105, 111, 110, 46, 109, 115, 9,
        56, 54, 52, 48, 48, 48, 48, 48, 0, 5, 0, 1, 5, 0, 0, 31, 109, 101, 115, 115, 97, 103, 101,
        46, 116, 105, 109, 101, 115, 116, 97, 109, 112, 46, 97, 102, 116, 101, 114, 46, 109, 97,
        120, 46, 109, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56,
        48, 55, 0, 5, 0, 1, 5, 0, 0, 32, 109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109, 101,
        115, 116, 97, 109, 112, 46, 98, 101, 102, 111, 114, 101, 46, 109, 97, 120, 46, 109, 115,
        20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 55, 0, 5, 0, 1,
        5, 0, 0, 11, 115, 101, 103, 109, 101, 110, 116, 46, 109, 115, 10, 54, 48, 52, 56, 48, 48,
        48, 48, 48, 0, 5, 0, 1, 5, 0, 0, 36, 109, 101, 115, 115, 97, 103, 101, 46, 116, 105, 109,
        101, 115, 116, 97, 109, 112, 46, 100, 105, 102, 102, 101, 114, 101, 110, 99, 101, 46, 109,
        97, 120, 46, 109, 115, 20, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53,
        56, 48, 55, 0, 5, 0, 1, 5, 0, 0, 20, 115, 101, 103, 109, 101, 110, 116, 46, 105, 110, 100,
        101, 120, 46, 98, 121, 116, 101, 115, 9, 49, 48, 52, 56, 53, 55, 54, 48, 0, 5, 0, 1, 3, 0,
        0, 0, 0,
    ];

    let api_key = 32;
    let api_version = 4;

    let header = Header::Response { correlation_id: 6 };
    let body = DescribeConfigsResponse::default()
        .throttle_time_ms(0)
        .results(Some(
            [DescribeConfigsResult::default()
                .error_code(0)
                .error_message(Some("".into()))
                .resource_type(2)
                .resource_name("test".into())
                .configs(Some(
                    [
                        DescribeConfigsResourceResult::default()
                            .name("compression.type".into())
                            .value(Some("producer".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(2))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("remote.log.delete.on.disable".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("leader.replication.throttled.replicas".into())
                            .value(Some("".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(7))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("remote.storage.enable".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.downconversion.enable".into())
                            .value(Some("true".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("min.insync.replicas".into())
                            .value(Some("1".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("segment.jitter.ms".into())
                            .value(Some("0".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("remote.log.copy.disable".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("local.retention.ms".into())
                            .value(Some("-2".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("cleanup.policy".into())
                            .value(Some("compact".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(1))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(7))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("flush.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("follower.replication.throttled.replicas".into())
                            .value(Some("".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(7))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("compression.lz4.level".into())
                            .value(Some("9".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("segment.bytes".into())
                            .value(Some("1073741824".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(4))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("retention.ms".into())
                            .value(Some("604800000".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("compression.gzip.level".into())
                            .value(Some("-1".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("flush.messages".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("compression.zstd.level".into())
                            .value(Some("3".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.format.version".into())
                            .value(Some("3.0-IV1".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(2))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("max.compaction.lag.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("file.delete.delay.ms".into())
                            .value(Some("60000".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("max.message.bytes".into())
                            .value(Some("1048588".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("min.compaction.lag.ms".into())
                            .value(Some("0".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.timestamp.type".into())
                            .value(Some("CreateTime".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(2))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("local.retention.bytes".into())
                            .value(Some("-2".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("preallocate".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("min.cleanable.dirty.ratio".into())
                            .value(Some("0.5".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(6))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("index.interval.bytes".into())
                            .value(Some("4096".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(3))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("unclean.leader.election.enable".into())
                            .value(Some("false".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(1))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("retention.bytes".into())
                            .value(Some("-1".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("delete.retention.ms".into())
                            .value(Some("86400000".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.timestamp.after.max.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.timestamp.before.max.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("segment.ms".into())
                            .value(Some("604800000".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("message.timestamp.difference.max.ms".into())
                            .value(Some("9223372036854775807".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(5))
                            .documentation(None),
                        DescribeConfigsResourceResult::default()
                            .name("segment.index.bytes".into())
                            .value(Some("10485760".into()))
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(5))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(3))
                            .documentation(None),
                    ]
                    .into(),
                ))]
            .into(),
        ))
        .into();

    assert_eq!(
        expected,
        Frame::response(header, body, api_key, api_version)?
    );

    Ok(())
}

#[test]
fn describe_configs_response_v4_003() -> Result<()> {
    use nisshi_sans_io::describe_configs_response::{
        DescribeConfigsResourceResult, DescribeConfigsResult,
    };

    let _guard = init_tracing()?;

    let expected = vec![
        0, 0, 0, 61, 0, 0, 0, 7, 0, 0, 0, 0, 0, 2, 0, 0, 10, 78, 111, 32, 101, 114, 114, 111, 114,
        46, 2, 5, 116, 101, 115, 116, 2, 15, 99, 108, 101, 97, 110, 117, 112, 46, 112, 111, 108,
        105, 99, 121, 8, 99, 111, 109, 112, 97, 99, 116, 0, 5, 0, 1, 2, 1, 0, 0, 0,
    ];

    let api_key = 32;
    let api_version = 4;

    let header = Header::Response { correlation_id: 7 };
    let body = DescribeConfigsResponse::default()
        .throttle_time_ms(0)
        .results(Some(
            [DescribeConfigsResult::default()
                .error_code(0)
                .error_message(Some("No error.".into()))
                .resource_type(2)
                .resource_name("test".into())
                .configs(Some(
                    [DescribeConfigsResourceResult::default()
                        .name("cleanup.policy".into())
                        .value(Some("compact".into()))
                        .read_only(false)
                        .is_default(None)
                        .config_source(Some(5))
                        .is_sensitive(false)
                        .synonyms(Some([].into()))
                        .config_type(Some(2))
                        .documentation(Some("".into()))]
                    .into(),
                ))]
            .into(),
        ))
        .into();

    assert_eq!(
        expected,
        Frame::response(header, body, api_key, api_version)?
    );

    Ok(())
}

#[test]
fn describe_groups_request_v1_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 15,
        api_version: 1,
        correlation_id: 0,
        client_id: None,
    };

    let body = DescribeGroupsRequest::default()
        .groups(Some(["abcabc".into()].into()))
        .include_authorized_operations(None)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 22, 0, 15, 0, 1, 0, 0, 0, 0, 255, 255, 0, 0, 0, 1, 0, 6, 97, 98, 99, 97, 98,
            99,
        ],
        Frame::request(header, body)?,
    );
    Ok(())
}

#[test]
fn describe_groups_response_v1_000() -> Result<()> {
    use nisshi_sans_io::describe_groups_response::DescribedGroup;

    let _guard = init_tracing()?;
    let header = Header::Response { correlation_id: 0 };
    let body = DescribeGroupsResponse::default()
        .throttle_time_ms(Some(0))
        .groups(Some(
            [DescribedGroup::default()
                .error_code(16)
                .group_id("abcabc".into())
                .group_state("".into())
                .protocol_type("".into())
                .protocol_data("".into())
                .members(Some([].into()))
                .authorized_operations(None)]
            .into(),
        ))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 16, 0, 6, 97, 98, 99, 97, 98, 99,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ],
        Frame::response(header, body, DescribeGroupsResponse::KEY, 1)?,
    );
    Ok(())
}

#[test]
fn describe_groups_response_v2_drops_authorized_operations() -> Result<()> {
    use nisshi_sans_io::describe_groups_response::DescribedGroup;

    let _guard = init_tracing()?;
    let header = Header::Response { correlation_id: 0 };
    let body = DescribeGroupsResponse::default()
        .throttle_time_ms(Some(0))
        .groups(Some(
            [DescribedGroup::default()
                .error_code(0)
                .group_id("abc".into())
                .group_state("".into())
                .protocol_type("".into())
                .protocol_data("".into())
                .members(Some([].into()))
                .authorized_operations(Some(i32::MIN))]
            .into(),
        ))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 29, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 3, 97, 98, 99, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
        ],
        Frame::response(header, body, DescribeGroupsResponse::KEY, 2)?,
    );
    Ok(())
}

#[test]
fn describe_groups_response_v5_writes_authorized_operations() -> Result<()> {
    use nisshi_sans_io::describe_groups_response::DescribedGroup;

    let _guard = init_tracing()?;
    let header = Header::Response { correlation_id: 0 };
    let body = DescribeGroupsResponse::default()
        .throttle_time_ms(Some(0))
        .groups(Some(
            [DescribedGroup::default()
                .error_code(0)
                .group_id("abc".into())
                .group_state("".into())
                .protocol_type("".into())
                .protocol_data("".into())
                .members(Some([].into()))
                .authorized_operations(Some(328))]
            .into(),
        ))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 26, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 4, 97, 98, 99, 1, 1, 1, 1, 0, 0, 1,
            72, 0, 0,
        ],
        Frame::response(header, body, DescribeGroupsResponse::KEY, 5)?,
    );
    Ok(())
}

#[test]
fn fetch_request_v6_000() -> Result<()> {
    use nisshi_sans_io::fetch_request::{FetchPartition, FetchTopic};

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 1,
        api_version: 6,
        correlation_id: 0,
        client_id: None,
    };

    let body = FetchRequest::default()
        .cluster_id(None)
        .replica_state(None)
        .replica_id(Some(-1))
        .max_wait_ms(5000)
        .min_bytes(1024)
        .max_bytes(Some(4096))
        .isolation_level(Some(1))
        .session_id(None)
        .session_epoch(None)
        .topics(Some(
            [FetchTopic::default()
                .topic(Some("abcabcabcab".into()))
                .topic_id(None)
                .partitions(Some(
                    [FetchPartition::default()
                        .partition(0)
                        .current_leader_epoch(None)
                        .fetch_offset(0)
                        .last_fetched_epoch(None)
                        .log_start_offset(Some(0))
                        .partition_max_bytes(4096)
                        .replica_directory_id(None)]
                    .into(),
                ))]
            .into(),
        ))
        .forgotten_topics_data(None)
        .rack_id(None)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 72, 0, 1, 0, 6, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 0, 0, 19, 136, 0, 0,
            4, 0, 0, 0, 16, 0, 1, 0, 0, 0, 1, 0, 11, 97, 98, 99, 97, 98, 99, 97, 98, 99, 97, 98, 0,
            0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn fetch_request_v16_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 1,
        api_version: 16,
        correlation_id: 12,
        client_id: Some("console-consumer".into()),
    };

    let body = FetchRequest::default()
        .cluster_id(None)
        .replica_id(None)
        .replica_state(None)
        .max_wait_ms(500)
        .min_bytes(1)
        .max_bytes(Some(52428800))
        .isolation_level(Some(0))
        .session_id(Some(0))
        .session_epoch(Some(-1))
        .topics(Some([].into()))
        .forgotten_topics_data(Some([].into()))
        .rack_id(Some("".into()))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 52, 0, 1, 0, 16, 0, 0, 0, 12, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99,
            111, 110, 115, 117, 109, 101, 114, 0, 0, 0, 1, 244, 0, 0, 0, 1, 3, 32, 0, 0, 0, 0, 0,
            0, 0, 255, 255, 255, 255, 1, 1, 1, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn fetch_request_v17_fuzz_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 1,
        api_version: 17,
        correlation_id: 12,
        client_id: Some("console-consumer".into()),
    };

    let body = FetchRequest::default()
        .cluster_id(Some(
            "\0\u{6}uuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu\0\0\0\0\u{6}\0".into()
        ))
        .replica_id(None)
        .replica_state(None)
        .max_wait_ms(603980776)
        .min_bytes(605416440)
        .max_bytes(Some(930559488))
        .isolation_level(Some(0))
        .session_id(Some(605416440))
        .session_epoch(Some(922812031))
        .topics(Some([
            FetchTopic::default()
                .topic(None)
                .topic_id(Some([
                    55, 127, 127, 254, 255, 255, 127, 127, 222, 255, 255, 255, 255, 255, 127, 255,
                ]))
                .partitions(Some([].into()))
            ,
            FetchTopic::default()
                .topic(None)
                .topic_id(Some([
                    190, 0, 0, 0, 6, 0, 248, 235, 21, 36, 255, 255, 255, 255, 43, 43,
                ]))
                .partitions(Some([].into()))
            ,
            FetchTopic::default()
                .topic(None)
                .topic_id(Some([
                    43, 0, 248, 235, 21, 36, 0, 127, 21, 10, 12, 156, 144, 46, 96, 0,
                ]))
                .partitions(Some([].into()))
            ,
            FetchTopic::default()
                .topic(None)
                .topic_id(Some([
                    255, 127, 255, 61, 190, 0, 0, 0, 6, 0, 248, 235, 21, 36, 255, 255,
                ]))
                .partitions(Some([FetchPartition::default()
                    .partition(-1613442047)
                    .current_leader_epoch(Some(134206676))
                    .fetch_offset(-2021827061141964821)
                    .last_fetched_epoch(Some(21))
                    .log_start_offset(Some(0))
                    .partition_max_bytes(0)
                    .replica_directory_id(None)
                ].into()))

            ].into()))
        .forgotten_topics_data(Some([].into()))
        .rack_id(Some("".into()))
        .into();

    assert_eq!(
        &[
            0, 0, 1, 35, 0, 1, 0, 17, 0, 0, 0, 12, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 99,
            111, 110, 115, 117, 109, 101, 114, 0, 36, 0, 3, 232, 36, 21, 235, 248, 55, 119, 54, 0,
            0, 36, 21, 235, 248, 55, 0, 254, 127, 5, 55, 127, 127, 254, 255, 255, 127, 127, 222,
            255, 255, 255, 255, 255, 127, 255, 1, 0, 190, 0, 0, 0, 6, 0, 248, 235, 21, 36, 255,
            255, 255, 255, 43, 43, 1, 0, 43, 0, 248, 235, 21, 36, 0, 127, 21, 10, 12, 156, 144, 46,
            96, 0, 1, 0, 255, 127, 255, 61, 190, 0, 0, 0, 6, 0, 248, 235, 21, 36, 255, 255, 2, 159,
            212, 212, 1, 7, 255, 212, 212, 227, 241, 6, 255, 255, 92, 119, 235, 0, 0, 0, 21, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 131, 1, 130, 1, 0, 6, 117, 117, 117,
            117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117,
            117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117,
            117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117,
            117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117,
            117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117,
            117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117,
            117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 117, 0, 0,
            0, 0, 6, 0,
        ],
        &Frame::request(header, body)?[..],
    );

    Ok(())
}

#[test]
fn fetch_request_v17_fuzz_001() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 1,
        api_version: 17,
        correlation_id: 12,
        client_id: Some("console-consumer".into()),
    };

    let body: Body = FetchRequest::default()
        .cluster_id(None)
        .replica_id(None)
        .replica_state(None)
        .max_wait_ms(5632)
        .min_bytes(33024)
        .max_bytes(Some(0))
        .isolation_level(Some(25))
        .session_id(Some(926351396))
        .session_epoch(Some(3618615))
        .topics(Some(
            [
                FetchTopic::default()
                    .topic(None)
                    .topic_id(Some([
                        55, 55, 55, 55, 55, 55, 55, 0, 0, 0, 0, 0, 0, 235, 21, 36,
                    ]))
                    .partitions(Some([].into())),
                FetchTopic::default()
                    .topic(None)
                    .topic_id(Some([
                        162, 55, 55, 55, 0, 0, 0, 0, 0, 251, 251, 251, 251, 251, 251, 251,
                    ]))
                    .partitions(Some(
                        [FetchPartition::default()
                            .partition(-67372037)
                            .current_leader_epoch(Some(-67372037))
                            .fetch_offset(-289360691352306693)
                            .last_fetched_epoch(Some(-67372037))
                            .log_start_offset(Some(-289360691352306693))
                            .partition_max_bytes(-67372037)
                            .replica_directory_id(Some([
                                251, 251, 251, 251, 251, 251, 251, 251, 251, 0, 235, 21, 36, 0, 55,
                                162,
                            ]))]
                        .into(),
                    )),
            ]
            .into(),
        ))
        .forgotten_topics_data(Some([].into()))
        .rack_id(Some("".into()))
        .into();

    let round_trip = Frame::request(header, body.clone())
        .and_then(Frame::request_from_bytes)
        .map(|frame| frame.body)?;

    assert_eq!(body, round_trip);

    Ok(())
}

#[test]
fn fetch_response_v12_000() -> Result<()> {
    use nisshi_sans_io::fetch_response::{FetchableTopicResponse, PartitionData};

    let _guard = init_tracing()?;

    let api_key = 1;
    let api_version = 12;

    let header = Header::Response { correlation_id: 8 };
    let body = FetchResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(Some(0))
        .session_id(Some(888121338))
        .responses(Some(
            [FetchableTopicResponse::default()
                .topic(Some("test".into()))
                .topic_id(None)
                .partitions(Some(
                    [
                        PartitionData::default()
                            .partition_index(1)
                            .error_code(3)
                            .high_watermark(-1)
                            .last_stable_offset(Some(-1))
                            .log_start_offset(Some(-1))
                            .diverging_epoch(None)
                            .current_leader(None)
                            .snapshot_id(None)
                            .aborted_transactions(Some([].into()))
                            .preferred_read_replica(Some(-1))
                            .records(None),
                        PartitionData::default()
                            .partition_index(0)
                            .error_code(3)
                            .high_watermark(-1)
                            .last_stable_offset(Some(-1))
                            .log_start_offset(Some(-1))
                            .diverging_epoch(None)
                            .current_leader(None)
                            .snapshot_id(None)
                            .aborted_transactions(Some([].into()))
                            .preferred_read_replica(Some(-1))
                            .records(None),
                        PartitionData::default()
                            .partition_index(2)
                            .error_code(3)
                            .high_watermark(-1)
                            .last_stable_offset(Some(-1))
                            .log_start_offset(Some(-1))
                            .diverging_epoch(None)
                            .current_leader(None)
                            .snapshot_id(None)
                            .aborted_transactions(Some([].into()))
                            .preferred_read_replica(Some(-1))
                            .records(None),
                    ]
                    .into(),
                ))]
            .into(),
        ))
        .node_endpoints(None)
        .into();

    let expected = vec![
        0, 0, 0, 135, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 52, 239, 167, 250, 2, 5, 116, 101, 115, 116,
        4, 0, 0, 0, 1, 0, 3, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0,
        0, 3, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 1, 255, 255, 255, 255, 1, 0, 0, 0, 0, 2, 0, 3, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 1, 255, 255, 255, 255, 1, 0, 0, 0,
    ];

    assert_eq!(
        expected,
        Frame::response(header, body, api_key, api_version)?
    );
    Ok(())
}

#[test]
fn fetch_response_v12_000_with_node_endpoints() -> Result<()> {
    use nisshi_sans_io::fetch_response::{FetchableTopicResponse, PartitionData};

    let _guard = init_tracing()?;

    let api_key = 1;
    let api_version = 12;

    let header = Header::Response { correlation_id: 8 };
    let body = FetchResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(Some(0))
        .session_id(Some(888121338))
        .responses(Some(
            [FetchableTopicResponse::default()
                .topic(Some("test".into()))
                .topic_id(None)
                .partitions(Some(
                    [
                        PartitionData::default()
                            .partition_index(1)
                            .error_code(3)
                            .high_watermark(-1)
                            .last_stable_offset(Some(-1))
                            .log_start_offset(Some(-1))
                            .diverging_epoch(None)
                            .current_leader(None)
                            .snapshot_id(None)
                            .aborted_transactions(Some([].into()))
                            .preferred_read_replica(Some(-1))
                            .records(None),
                        PartitionData::default()
                            .partition_index(0)
                            .error_code(3)
                            .high_watermark(-1)
                            .last_stable_offset(Some(-1))
                            .log_start_offset(Some(-1))
                            .diverging_epoch(None)
                            .current_leader(None)
                            .snapshot_id(None)
                            .aborted_transactions(Some([].into()))
                            .preferred_read_replica(Some(-1))
                            .records(None),
                        PartitionData::default()
                            .partition_index(2)
                            .error_code(3)
                            .high_watermark(-1)
                            .last_stable_offset(Some(-1))
                            .log_start_offset(Some(-1))
                            .diverging_epoch(None)
                            .current_leader(None)
                            .snapshot_id(None)
                            .aborted_transactions(Some([].into()))
                            .preferred_read_replica(Some(-1))
                            .records(None),
                    ]
                    .into(),
                ))]
            .into(),
        ))
        .node_endpoints(Some(
            [NodeEndpoint::default()
                .host("abc".into())
                .node_id(12321)
                .port(32123)
                .rack(Some("production-A".into()))]
            .into(),
        ))
        .into();

    let expected = vec![
        0, 0, 0, 135, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 52, 239, 167, 250, 2, 5, 116, 101, 115, 116,
        4, 0, 0, 0, 1, 0, 3, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0,
        0, 3, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 1, 255, 255, 255, 255, 1, 0, 0, 0, 0, 2, 0, 3, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 1, 255, 255, 255, 255, 1, 0, 0, 0,
    ];

    assert_eq!(
        expected,
        Frame::response(header, body, api_key, api_version)?
    );
    Ok(())
}

#[test]
fn fetch_response_v12_001() -> Result<()> {
    use nisshi_sans_io::fetch_response::{FetchableTopicResponse, PartitionData};

    let _guard = init_tracing()?;

    let api_key = 1;
    let api_version = 12;

    let header = Header::Response { correlation_id: 8 };
    let body = FetchResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(Some(0))
        .session_id(Some(2086460889))
        .responses(Some(
            [FetchableTopicResponse::default()
                .topic(Some("test".into()))
                .topic_id(None)
                .partitions(Some(
                    [
                        PartitionData::default()
                            .partition_index(1)
                            .error_code(0)
                            .high_watermark(0)
                            .last_stable_offset(Some(0))
                            .log_start_offset(Some(0))
                            .diverging_epoch(None)
                            .current_leader(None)
                            .snapshot_id(None)
                            .aborted_transactions(None)
                            .preferred_read_replica(Some(-1))
                            .records(None),
                        PartitionData::default()
                            .partition_index(0)
                            .error_code(0)
                            .high_watermark(2)
                            .last_stable_offset(Some(2))
                            .log_start_offset(Some(0))
                            .diverging_epoch(None)
                            .current_leader(None)
                            .snapshot_id(None)
                            .aborted_transactions(None)
                            .preferred_read_replica(Some(-1))
                            .records(Some(
                                inflated::Frame {
                                    batches: [
                                        Batch {
                                            base_offset: 0,
                                            batch_length: 62,
                                            partition_leader_epoch: 0,
                                            magic: 2,
                                            crc: 2915995653,
                                            attributes: 0,
                                            last_offset_delta: 0,
                                            base_timestamp: 1707058170165,
                                            max_timestamp: 1707058170165,
                                            producer_id: 1,
                                            producer_epoch: 0,
                                            base_sequence: 1,
                                            records: [Record {
                                                length: 12,
                                                attributes: 0,
                                                timestamp_delta: 0,
                                                offset_delta: 0,
                                                key: Some(Bytes::from_static(&[97, 98, 99])),
                                                value: Some(Bytes::from_static(&[112, 113, 114])),
                                                headers: [].into(),
                                            }]
                                            .into(),
                                        },
                                        Batch {
                                            base_offset: 1,
                                            batch_length: 62,
                                            partition_leader_epoch: 0,
                                            magic: 2,
                                            crc: 2915995653,
                                            attributes: 0,
                                            last_offset_delta: 0,
                                            base_timestamp: 1707058170165,
                                            max_timestamp: 1707058170165,
                                            producer_id: 1,
                                            producer_epoch: 0,
                                            base_sequence: 1,
                                            records: [Record {
                                                length: 12,
                                                attributes: 0,
                                                timestamp_delta: 0,
                                                offset_delta: 0,
                                                key: Some(Bytes::from_static(&[97, 98, 99])),
                                                value: Some(Bytes::from_static(&[112, 113, 114])),
                                                headers: [].into(),
                                            }]
                                            .into(),
                                        },
                                    ]
                                    .into(),
                                }
                                .try_into()?,
                            )),
                        PartitionData::default()
                            .partition_index(2)
                            .error_code(0)
                            .high_watermark(0)
                            .last_stable_offset(Some(0))
                            .log_start_offset(Some(0))
                            .diverging_epoch(None)
                            .current_leader(None)
                            .snapshot_id(None)
                            .aborted_transactions(None)
                            .preferred_read_replica(Some(-1))
                            .records(None),
                    ]
                    .into(),
                ))]
            .into(),
        ))
        .node_endpoints(None)
        .into();

    let expected = vec![
        0, 0, 1, 28, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 124, 92, 221, 217, 2, 5, 116, 101, 115, 116,
        4, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 255, 255, 255, 255, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0,
        0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 149, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 62, 0, 0, 0, 0, 2, 173, 206, 144, 5, 0, 0, 0, 0, 0, 0, 0, 0, 1, 141, 116, 152, 137, 53,
        0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 24,
        0, 0, 0, 6, 97, 98, 99, 6, 112, 113, 114, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 62, 0, 0, 0,
        0, 2, 173, 206, 144, 5, 0, 0, 0, 0, 0, 0, 0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 1, 141,
        116, 152, 137, 53, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 24, 0, 0, 0, 6,
        97, 98, 99, 6, 112, 113, 114, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 1, 0, 0, 0,
    ];

    assert_eq!(
        expected,
        Frame::response(header, body, api_key, api_version)?
    );
    Ok(())
}

#[test]
fn fetch_response_v16_001() -> Result<()> {
    use nisshi_sans_io::fetch_response::{
        EpochEndOffset, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData, SnapshotId,
    };

    let _guard = init_tracing()?;

    let api_key = 1;
    let api_version = 16;

    let header = Header::Response { correlation_id: 8 };
    let body = FetchResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(Some(0))
        .session_id(Some(0))
        .responses(Some(
            [FetchableTopicResponse::default()
                .topic(None)
                .topic_id(Some([
                    28, 205, 172, 195, 142, 19, 71, 71, 182, 128, 13, 18, 65, 142, 210, 222,
                ]))
                .partitions(Some(
                    [PartitionData::default()
                        .partition_index(0)
                        .error_code(0)
                        .high_watermark(0)
                        .last_stable_offset(Some(1))
                        .log_start_offset(Some(-1))
                        .diverging_epoch(Some(EpochEndOffset::default().epoch(-1).end_offset(-1)))
                        .current_leader(Some(
                            LeaderIdAndEpoch::default().leader_id(0).leader_epoch(0),
                        ))
                        .snapshot_id(Some(SnapshotId::default().end_offset(-1).epoch(-1)))
                        .aborted_transactions(Some([].into()))
                        .preferred_read_replica(Some(0))
                        .records(Some(
                            inflated::Frame {
                                batches: [Batch {
                                    base_offset: 0,
                                    batch_length: 61,
                                    partition_leader_epoch: -1,
                                    magic: 2,
                                    crc: 2576291984,
                                    attributes: 0,
                                    last_offset_delta: 0,
                                    base_timestamp: 1721989616694,
                                    max_timestamp: 1721989616694,
                                    producer_id: 1,
                                    producer_epoch: 0,
                                    base_sequence: 0,
                                    records: [Record {
                                        length: 11,
                                        attributes: 0,
                                        timestamp_delta: 0,
                                        offset_delta: 0,
                                        key: None,
                                        value: Some(Bytes::from_static(&[112, 111, 105, 117, 121])),
                                        headers: [].into(),
                                    }]
                                    .into(),
                                }]
                                .into(),
                            }
                            .try_into()?,
                        ))]
                    .into(),
                ))]
            .into(),
        ))
        .node_endpoints(Some([].into()))
        .into();

    let expected = vec![
        0, 0, 0, 189, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 28, 205, 172, 195, 142, 19,
        71, 71, 182, 128, 13, 18, 65, 142, 210, 222, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 1, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 74, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 61, 255, 255, 255, 255, 2, 153, 143, 24, 144, 0, 0, 0, 0, 0, 0, 0,
        0, 1, 144, 238, 148, 84, 54, 0, 0, 1, 144, 238, 148, 84, 54, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 1, 22, 0, 0, 0, 1, 10, 112, 111, 105, 117, 121, 0, 3, 0, 13, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 1, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
        13, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 1, 0, 1, 1,
    ];

    assert_eq!(
        expected,
        Frame::response(header, body, api_key, api_version)?
    );
    Ok(())
}

#[test]
fn fetch_response_v16_002() -> Result<()> {
    use nisshi_sans_io::fetch_response::{
        EpochEndOffset, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData, SnapshotId,
    };

    let _guard = init_tracing()?;

    let api_key = 1;
    let api_version = 16;

    let header = Header::Response { correlation_id: 8 };
    let body = FetchResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(Some(0))
        .session_id(Some(0))
        .responses(Some(
            [FetchableTopicResponse::default()
                .topic(None)
                .topic_id(Some([
                    28, 205, 172, 195, 142, 19, 71, 71, 182, 128, 13, 18, 65, 142, 210, 222,
                ]))
                .partitions(Some(
                    [PartitionData::default()
                        .partition_index(0)
                        .error_code(0)
                        .high_watermark(0)
                        .last_stable_offset(Some(1))
                        .log_start_offset(Some(-1))
                        .diverging_epoch(Some(EpochEndOffset::default().epoch(-1).end_offset(-1)))
                        .current_leader(Some(
                            LeaderIdAndEpoch::default().leader_id(0).leader_epoch(0),
                        ))
                        .snapshot_id(Some(SnapshotId::default().end_offset(-1).epoch(-1)))
                        .aborted_transactions(Some([].into()))
                        .preferred_read_replica(Some(0))
                        .records(Some(
                            inflated::Frame {
                                batches: [Batch {
                                    base_offset: 0,
                                    batch_length: 61,
                                    partition_leader_epoch: -1,
                                    magic: 2,
                                    crc: 2576291984,
                                    attributes: 0,
                                    last_offset_delta: 0,
                                    base_timestamp: 1721989616694,
                                    max_timestamp: 1721989616694,
                                    producer_id: 1,
                                    producer_epoch: 0,
                                    base_sequence: 0,
                                    records: [Record {
                                        length: 11,
                                        attributes: 0,
                                        timestamp_delta: 0,
                                        offset_delta: 0,
                                        key: None,
                                        value: Some(Bytes::from_static(&[112, 111, 105, 117, 121])),
                                        headers: [].into(),
                                    }]
                                    .into(),
                                }]
                                .into(),
                            }
                            .try_into()?,
                        ))]
                    .into(),
                ))]
            .into(),
        ))
        .node_endpoints(None)
        .into();

    let frame = Frame {
        size: 0,
        header,
        body,
    };

    let expected = vec![
        0, 0, 0, 186, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 28, 205, 172, 195, 142, 19,
        71, 71, 182, 128, 13, 18, 65, 142, 210, 222, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 1, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 74, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 61, 255, 255, 255, 255, 2, 153, 143, 24, 144, 0, 0, 0, 0, 0, 0, 0,
        0, 1, 144, 238, 148, 84, 54, 0, 0, 1, 144, 238, 148, 84, 54, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 1, 22, 0, 0, 0, 1, 10, 112, 111, 105, 117, 121, 0, 3, 0, 13, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 1, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
        13, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0,
    ];

    let allocation_size = frame
        .maximum_allocation_size()
        .inspect(|allocation_size| debug!(allocation_size, expected_len = expected.len()))?;

    assert!(allocation_size >= expected.len());

    assert_eq!(
        expected,
        Frame::response(frame.header, frame.body, api_key, api_version)?
    );
    Ok(())
}

#[test]
fn fetch_response_v17_001() -> Result<()> {
    use nisshi_sans_io::fetch_response::{FetchableTopicResponse, PartitionData};

    let _guard = init_tracing()?;

    let api_key = FetchResponse::KEY;
    let api_version = 17;

    #[allow(clippy::octal_escapes)]
    let frame = Frame {
      size: 0,
      header: Header::Response { correlation_id: 8 },
      body: Body::FetchResponse(FetchResponse::default()
    .throttle_time_ms(Some(0))
    .error_code(Some(0))
    .session_id(Some(0))
    .responses(Some([FetchableTopicResponse::default()
    .topic(None)
    .topic_id(Some([1, 158, 203, 118, 29, 132, 120, 131, 157, 49, 191, 78, 45, 167, 97, 240]))
    .partitions(Some([PartitionData::default()
    .partition_index(0)
    .error_code(0)
    .high_watermark(7037)
    .last_stable_offset(Some(7037))
    .log_start_offset(Some(0))
    .diverging_epoch(None)
    .current_leader(None)
    .snapshot_id(None)
    .aborted_transactions(Some([].into()))
    .preferred_read_replica(Some(-1))
    .records(Some(deflated::Frame {batches:[deflated::Batch {base_offset: 6906,
    batch_length: 966,
    partition_leader_epoch: -1,
    magic: 2,
    crc: 2247564047,
    attributes: 2,
    last_offset_delta: 65,
    base_timestamp: 1781529986827,
    max_timestamp: 1781529986827,
    producer_id: 1,
    producer_epoch: 0,
    base_sequence: 6906,
    record_count: 66,
    record_data: Bytes::from_static(b"\x9e(\xf0X\x98\x01\0\0\0\n75476\x80\x019cd65b48408fce39dcbba2272da5847204ba3e102cc29dcb5a12d341f932b494\0\x98\x01\0\0\x02\n75477\xeeN\0\x1dN\x18\x04\n75482\xee\x9c\0\x1d\x9c\x18\x06\n75495\xeeN\0\x1dN\x14\x08\n7550\xeeN\0.N\0\0\n\x05N\09\xeeN\0\x1dN\0\x0c\x01N\x0460\xeeN\0\x1dN\x18\x0e\n75598\xeeN\0\x1dN\x14\x10\n7560\xee\xd4\x01.\xd4\x01\x14\x12\n7561\xeeN\0.N\0\x14\x14\n7562\xeeN\0.N\0\0\x16\x01N\03\xee\x86\x01.\x86\x01\x18\x18\n75654\xeeN\0\x1dN\x18\x1a\n75663\xeeN\0\x1dN\0\x1c\x05N\05\xeeN\0\x1dN\0\x1e\x01N\xeeD\x042D\x04\x14 \n7568\xeeZ\x03.Z\x03\x14\"\n7570\xeeN\0.N\0\x14$\n7571\xee|\x05.|\x05\x14&\n7571\xee\x9c\0.\x9c\0\x10(\n757\xee\x0c\x032\x0c\x03\x14*\n7575\xee\x0c\x03.\x0c\x03\x14,\n7575\xee\x9c\0.\x9c\0\x14.\n7577\xee\x9c\0.\x9c\0\00\x01\x9c\x0471\xee\x86\x01=\x86\x142\n7577\xee\xa8\x03.\xa8\x03\x144\n7579\xee\xea\0.\xea\0\x146\n7579\xee\x92\x04.\x92\x04\x148\n7579\xee\x0c\x03.\x0c\x03\x14:\n7580\xee\xb4\x06.\xb4\x06\x14<\n7582\xee\x9c\0.\x9c\0\x14>\n7583\xeeN\0.N\0\x14@\n7584\xee\xea\0.\xea\0\x14B\n7587\xee\xca\x05.\xca\x05\x14D\n7587\xee\x92\x04.\x92\x04\x10F\n758\xeep\x022p\x02\x10H\n759\xee:\x082:\x08\x14J\n7591\xee\x86\x01.\x86\x01\x10L\n759\xee|\x052|\x05\x14N\n7593\xee\xd4\x01.\xd4\x01\x14P\n7593\xee\xd4\x01.\xd4\x01\x14R\n7595\xee\x0c\x03.\x0c\x03\x14T\n7595\xee:\x08.:\x08\x14V\n7595\xee:\x08.:\x08\x14X\n7596\xee\x18\x06.\x18\x06\x14Z\n7596\xee\xd4\x01.\xd4\x01\x14\\\n7597\xeeZ\x03.Z\x03\x14^\n7599\xee\x86\x01.\x86\x01\x14`\n7599\xee\x86\x01.\x86\x01\x10b\n760\xee\xb6\r2\xb6\r\x14d\n7601\xeeP\x07.P\x07\x14f\n7601\xee\"\x02.\"\x02\x14h\n7601\xee:\x08.:\x08\x14j\n7602\xee:\x08.:\x08\x14l\n7602\xee\xd4\x01.\xd4\x01\x10n\n760\xeeh\r2h\r\x10p\n760\xee\xe0\x042\xe0\x04\x14r\n7604\xee\xca\x05.\xca\x05\x14t\n7606\xeeZ\x03.Z\x03\x10v\n760\xee\xf8\n2\xf8\n\x14x\n7611\xee\xf8\n.\xf8\n\x10z\n761\xee\x1a\r2\x1a\r\x14|\n7614\xee\x0c\x03.\x0c\x03\x14~\n7615\xeeD\x04\x91D$\x9a\x01\0\0\x80\x01\n761\xee\x03\x07\xf5\x03(\x9a\x01\0\0\x82\x01\n7616\xeer\x02Qr")
    },deflated::Batch {
    base_offset: 6972,
    batch_length: 948,
    partition_leader_epoch: -1,
    magic: 2,
    crc: 1516087299,
    attributes: 2,
    last_offset_delta: 64,
    base_timestamp: 1781529987035,
    max_timestamp: 1781529987035,
    producer_id: 1,
    producer_epoch: 0,
    base_sequence: 6972,
    record_count: 65,
    record_data: Bytes::from_static(b"\xcf'\xf0X\x98\x01\0\0\0\n76187\x80\x019cd65b48408fce39dcbba2272da5847204ba3e102cc29dcb5a12d341f932b494\0\x98\x01\0\0\x02\n76196\xeeN\0\x1dN\x18\x04\n76221\xee\x9c\0\x1d\x9c\x18\x06\n76224\xeeN\0\x1dN\x18\x08\n76245\xeeN\0\x1dN\x18\n\n76262\xeeN\0\x1dN\x14\x0c\n7630\xee8\x01.8\x01\x14\x0e\n7630\xee\x9c\0.\x9c\0\x14\x10\n7631\xee\x9c\0.\x9c\0\0\x12\x01\x9c\01\xee\x86\x01.\x86\x01\x18\x14\n76329\xeeN\0\x1dN\x14\x16\n7635\xee8\x01.8\x01\x14\x18\n7635\xee\xbe\x02.\xbe\x02\x14\x1a\n7639\xee\x86\x01.\x86\x01\x10\x1c\n763\xee\xf6\x032\xf6\x03\x18\x1e\n76400\xee\x9c\0\x1d\x9c\x10 \n764\xee\"\x022\"\x02\x14\"\n7643\xee\xd4\x01.\xd4\x01\x14$\n7643\xee8\x01.8\x01\x14&\n7644\xee\"\x02.\"\x02\x14(\n7644\xee\x9c\0.\x9c\0\x10*\n764\xee\x0c\x032\x0c\x03\x14,\n7646\xee\xbe\x02.\xbe\x02\x18.\n76463\xeeN\0\x1dN\00\x05N\08\xeeN\0\x1dN\02\x01N\07\xeeN\0.N\0\04\x01N\09\xee\x86\x01.\x86\x01\x106\n765\xee\x18\x062\x18\x06\x108\n765\xee\x18\x062\x18\x06\x14:\n7651\xee8\x01.8\x01\x14<\n7652\xee$\t.$\t\x10>\n765\xeeD\x042D\x04\x14@\n7653\xee\x9c\0.\x9c\0\x14B\n7655\xeeN\0.N\0\x14D\n7656\xeeD\x04.D\x04\x14F\n7658\xee\x18\x06.\x18\x06\x14H\n7658\xee\x86\x01.\x86\x01\x14J\n7658\xee\xea\0.\xea\0\0L\x05\xea\09\xee\xea\0\x1d\xea\x14N\n7661\xee8\x01.8\x01\x14P\n7663\xee.\x05..\x05\x10R\n766\xeeF\x0b2F\x0b\x14T\n7664\xee\xbe\x02.\xbe\x02\x10V\n766\xeef\x062f\x06\x10X\n766\xee|\x052|\x05\x14Z\n7671\xee\x86\x01.\x86\x01\x14\\\n7673\xee\xea\0.\xea\0\x14^\n7674\xeeN\0.N\0\0`\x05N\08\xeeN\0\x1dN\0b\x01N\07\xee\x0c\x03.\x0c\x03\x14d\n7677\xee\x0c\x03.\x0c\x03\x10f\n767\xee\xec\x072\xec\x07\x14h\n7678\xeeZ\x03.Z\x03\x10j\n767\xee0\x0c20\x0c\x14l\n7680\xee\xaa\n.\xaa\n\x10n\n768\xee\xe0\x042\xe0\x04\x14p\n7681\xeeD\x04.D\x04\x10r\n768\xee.\x052.\x05\x14t\n7684\xeeN\0.N\0\0v\x05N\04\xeeN\0\x1dN\0x\x01N\06\xee\x86\x01.\x86\x01\x10z\n768\xee:\x082:\x08\x10|\n768\xee\x9e\x072\x9e\x07\x14~\n7689\xee\x9e\x07\xf1\x9e,\x9a\x01\0\0\x80\x01\n76905\xee9\x01\x1832b494\0")
    }].into() })),
    PartitionData::default()
    .partition_index(1)
    .error_code(0)
    .high_watermark(7105)
    .last_stable_offset(Some(7105))
    .log_start_offset(Some(0))
    .diverging_epoch(None)
    .current_leader(None)
    .snapshot_id(None)
    .aborted_transactions(Some([].into()))
    .preferred_read_replica(Some(-1))
    .records(Some(deflated::Frame { batches: [
    deflated::Batch {
    base_offset: 7040,
    batch_length: 939,
    partition_leader_epoch: -1,
    magic: 2,
    crc: 4263650927,
    attributes: 2,
    last_offset_delta: 64,
    base_timestamp: 1781529987035,
    max_timestamp: 1781529987035,
    producer_id: 1,
    producer_epoch: 0,
    base_sequence: 7040,
    record_count: 65,
    record_data: Bytes::from_static(b"\xcf'\xf0X\x98\x01\0\0\0\n76201\x80\x019cd65b48408fce39dcbba2272da5847204ba3e102cc29dcb5a12d341f932b494\0\x98\x01\0\0\x02\n76230\xeeN\0\x1dN\x18\x04\n76258\xee\x9c\0\x1d\x9c\x18\x06\n76266\xeeN\0\x1dN\x18\x08\n76282\xeeN\0\x1dN\0\n\x05N\08\xeeN\0\x1dN\x18\x0c\n76314\xeeN\0\x1dN\0\x0e\x05N\08\xeeN\0\x1dN\0\x10\x01N\02\xeeN\0.N\0\0\x12\x01N\03\xee\xbe\x02.\xbe\x02\x18\x14\n76337\xeeN\0\x1dN\x14\x16\n7634\xee\x9c\0.\x9c\0\x18\x18\n76343\xeeN\0\x1dN\x14\x1a\n7635\xeeN\0.N\0\0\x1c\x05N\05\xeeN\0\x1dN\0\x1e\x01N\07\xee\xbe\x02.\xbe\x02\x14 \n7639\xeeN\0.N\0\x10\"\n764\xee.\x052.\x05\x10$\n764\xee\xbe\x022\xbe\x02\x14&\n7644\xeeZ\x03.Z\x03\x18(\n76449\xeeN\0\x1dN\x14*\n7645\xee|\x05.|\x05\x14,\n7646\xeef\x06.f\x06\x14.\n7647\xee\xf6\x03.\xf6\x03\x140\n7651\xee\x9c\0.\x9c\0\x142\n7651\xee8\x01.8\x01\x144\n7651\xee\xd4\x01.\xd4\x01\x146\n7652\xee\xbe\x02.\xbe\x02\x148\n7653\xee\xea\0.\xea\0\x14:\n7654\xee\x92\x04.\x92\x04\x14<\n7655\xeeN\0.N\0\x14>\n7656\xee\x86\x01.\x86\x01\x10@\n765\xee.\x052.\x05\x10B\n765\xee\x0c\x032\x0c\x03\x14D\n7660\xee\x9c\0.\x9c\0\x14F\n7661\xeep\x02.p\x02\x14H\n7661\xee\x02\x07.\x02\x07\x10J\n766\xee:\x082:\x08\x10L\n766\xeep\x022p\x02\x10N\n766\xee.\x052.\x05\x14P\n7666\xee.\x05..\x05\x14R\n7668\xee\x86\x01.\x86\x01\x14T\n7670\xee\xea\0.\xea\0\x10V\n767\xee\xbe\x022\xbe\x02\x14X\n7671\xee\xd4\x01.\xd4\x01\x10Z\n767\xee\x18\x062\x18\x06\x10\\\n767\xee\x18\x062\x18\x06\x10^\n767\xee\x04\x0e2\x04\x0e\x10`\n767\xeeZ\x032Z\x03\x14b\n7673\xee\xea\0.\xea\0\x14d\n7674\xee\"\x02.\"\x02\x14f\n7675\xeeN\0.N\0\0h\x05N\08\xeeN\0\x1dN\0j\x01N\xee\xee\x0e2\xee\x0e\x10l\n767\xee\x94\x0b2\x94\x0b\x14n\n7679\xee\"\x02.\"\x02\x14p\n7679\xee\"\x02.\"\x02\x14r\n7680\xee\xa8\x03.\xa8\x03\x14t\n7681\xee\x86\x01.\x86\x01\x14v\n7682\xee\x92\x04.\x92\x04\x14x\n7684\xee\xf6\x03.\xf6\x03\x10z\n768\xee<\x0f2<\x0f\x10|\n768\xeeZ\x032Z\x03\x10~\n768\xee\x9e\x07\xf5\x9e(\x9a\x01\0\0\x80\x01\n7688\xee\x9d\0\x11\x9d")
    }].into() })) ].into()))].into()))
    .node_endpoints(Some([].into()))) };

    let expected = vec![
        0, 0, 11, 187, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 158, 203, 118, 29, 132,
        120, 131, 157, 49, 191, 78, 45, 167, 97, 240, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 27,
        125, 0, 0, 0, 0, 0, 0, 27, 125, 0, 0, 0, 0, 0, 0, 0, 0, 1, 255, 255, 255, 255, 147, 15, 0,
        0, 0, 0, 0, 0, 26, 250, 0, 0, 3, 198, 255, 255, 255, 255, 2, 133, 247, 27, 15, 0, 2, 0, 0,
        0, 65, 0, 0, 1, 158, 203, 118, 71, 11, 0, 0, 1, 158, 203, 118, 71, 11, 0, 0, 0, 0, 0, 0, 0,
        1, 0, 0, 0, 0, 26, 250, 0, 0, 0, 66, 158, 40, 240, 88, 152, 1, 0, 0, 0, 10, 55, 53, 52, 55,
        54, 128, 1, 57, 99, 100, 54, 53, 98, 52, 56, 52, 48, 56, 102, 99, 101, 51, 57, 100, 99, 98,
        98, 97, 50, 50, 55, 50, 100, 97, 53, 56, 52, 55, 50, 48, 52, 98, 97, 51, 101, 49, 48, 50,
        99, 99, 50, 57, 100, 99, 98, 53, 97, 49, 50, 100, 51, 52, 49, 102, 57, 51, 50, 98, 52, 57,
        52, 0, 152, 1, 0, 0, 2, 10, 55, 53, 52, 55, 55, 238, 78, 0, 29, 78, 24, 4, 10, 55, 53, 52,
        56, 50, 238, 156, 0, 29, 156, 24, 6, 10, 55, 53, 52, 57, 53, 238, 78, 0, 29, 78, 20, 8, 10,
        55, 53, 53, 48, 238, 78, 0, 46, 78, 0, 0, 10, 5, 78, 0, 57, 238, 78, 0, 29, 78, 0, 12, 1,
        78, 4, 54, 48, 238, 78, 0, 29, 78, 24, 14, 10, 55, 53, 53, 57, 56, 238, 78, 0, 29, 78, 20,
        16, 10, 55, 53, 54, 48, 238, 212, 1, 46, 212, 1, 20, 18, 10, 55, 53, 54, 49, 238, 78, 0,
        46, 78, 0, 20, 20, 10, 55, 53, 54, 50, 238, 78, 0, 46, 78, 0, 0, 22, 1, 78, 0, 51, 238,
        134, 1, 46, 134, 1, 24, 24, 10, 55, 53, 54, 53, 52, 238, 78, 0, 29, 78, 24, 26, 10, 55, 53,
        54, 54, 51, 238, 78, 0, 29, 78, 0, 28, 5, 78, 0, 53, 238, 78, 0, 29, 78, 0, 30, 1, 78, 238,
        68, 4, 50, 68, 4, 20, 32, 10, 55, 53, 54, 56, 238, 90, 3, 46, 90, 3, 20, 34, 10, 55, 53,
        55, 48, 238, 78, 0, 46, 78, 0, 20, 36, 10, 55, 53, 55, 49, 238, 124, 5, 46, 124, 5, 20, 38,
        10, 55, 53, 55, 49, 238, 156, 0, 46, 156, 0, 16, 40, 10, 55, 53, 55, 238, 12, 3, 50, 12, 3,
        20, 42, 10, 55, 53, 55, 53, 238, 12, 3, 46, 12, 3, 20, 44, 10, 55, 53, 55, 53, 238, 156, 0,
        46, 156, 0, 20, 46, 10, 55, 53, 55, 55, 238, 156, 0, 46, 156, 0, 0, 48, 1, 156, 4, 55, 49,
        238, 134, 1, 61, 134, 20, 50, 10, 55, 53, 55, 55, 238, 168, 3, 46, 168, 3, 20, 52, 10, 55,
        53, 55, 57, 238, 234, 0, 46, 234, 0, 20, 54, 10, 55, 53, 55, 57, 238, 146, 4, 46, 146, 4,
        20, 56, 10, 55, 53, 55, 57, 238, 12, 3, 46, 12, 3, 20, 58, 10, 55, 53, 56, 48, 238, 180, 6,
        46, 180, 6, 20, 60, 10, 55, 53, 56, 50, 238, 156, 0, 46, 156, 0, 20, 62, 10, 55, 53, 56,
        51, 238, 78, 0, 46, 78, 0, 20, 64, 10, 55, 53, 56, 52, 238, 234, 0, 46, 234, 0, 20, 66, 10,
        55, 53, 56, 55, 238, 202, 5, 46, 202, 5, 20, 68, 10, 55, 53, 56, 55, 238, 146, 4, 46, 146,
        4, 16, 70, 10, 55, 53, 56, 238, 112, 2, 50, 112, 2, 16, 72, 10, 55, 53, 57, 238, 58, 8, 50,
        58, 8, 20, 74, 10, 55, 53, 57, 49, 238, 134, 1, 46, 134, 1, 16, 76, 10, 55, 53, 57, 238,
        124, 5, 50, 124, 5, 20, 78, 10, 55, 53, 57, 51, 238, 212, 1, 46, 212, 1, 20, 80, 10, 55,
        53, 57, 51, 238, 212, 1, 46, 212, 1, 20, 82, 10, 55, 53, 57, 53, 238, 12, 3, 46, 12, 3, 20,
        84, 10, 55, 53, 57, 53, 238, 58, 8, 46, 58, 8, 20, 86, 10, 55, 53, 57, 53, 238, 58, 8, 46,
        58, 8, 20, 88, 10, 55, 53, 57, 54, 238, 24, 6, 46, 24, 6, 20, 90, 10, 55, 53, 57, 54, 238,
        212, 1, 46, 212, 1, 20, 92, 10, 55, 53, 57, 55, 238, 90, 3, 46, 90, 3, 20, 94, 10, 55, 53,
        57, 57, 238, 134, 1, 46, 134, 1, 20, 96, 10, 55, 53, 57, 57, 238, 134, 1, 46, 134, 1, 16,
        98, 10, 55, 54, 48, 238, 182, 13, 50, 182, 13, 20, 100, 10, 55, 54, 48, 49, 238, 80, 7, 46,
        80, 7, 20, 102, 10, 55, 54, 48, 49, 238, 34, 2, 46, 34, 2, 20, 104, 10, 55, 54, 48, 49,
        238, 58, 8, 46, 58, 8, 20, 106, 10, 55, 54, 48, 50, 238, 58, 8, 46, 58, 8, 20, 108, 10, 55,
        54, 48, 50, 238, 212, 1, 46, 212, 1, 16, 110, 10, 55, 54, 48, 238, 104, 13, 50, 104, 13,
        16, 112, 10, 55, 54, 48, 238, 224, 4, 50, 224, 4, 20, 114, 10, 55, 54, 48, 52, 238, 202, 5,
        46, 202, 5, 20, 116, 10, 55, 54, 48, 54, 238, 90, 3, 46, 90, 3, 16, 118, 10, 55, 54, 48,
        238, 248, 10, 50, 248, 10, 20, 120, 10, 55, 54, 49, 49, 238, 248, 10, 46, 248, 10, 16, 122,
        10, 55, 54, 49, 238, 26, 13, 50, 26, 13, 20, 124, 10, 55, 54, 49, 52, 238, 12, 3, 46, 12,
        3, 20, 126, 10, 55, 54, 49, 53, 238, 68, 4, 145, 68, 36, 154, 1, 0, 0, 128, 1, 10, 55, 54,
        49, 238, 3, 7, 245, 3, 40, 154, 1, 0, 0, 130, 1, 10, 55, 54, 49, 54, 238, 114, 2, 81, 114,
        0, 0, 0, 0, 0, 0, 27, 60, 0, 0, 3, 180, 255, 255, 255, 255, 2, 90, 93, 168, 3, 0, 2, 0, 0,
        0, 64, 0, 0, 1, 158, 203, 118, 71, 219, 0, 0, 1, 158, 203, 118, 71, 219, 0, 0, 0, 0, 0, 0,
        0, 1, 0, 0, 0, 0, 27, 60, 0, 0, 0, 65, 207, 39, 240, 88, 152, 1, 0, 0, 0, 10, 55, 54, 49,
        56, 55, 128, 1, 57, 99, 100, 54, 53, 98, 52, 56, 52, 48, 56, 102, 99, 101, 51, 57, 100, 99,
        98, 98, 97, 50, 50, 55, 50, 100, 97, 53, 56, 52, 55, 50, 48, 52, 98, 97, 51, 101, 49, 48,
        50, 99, 99, 50, 57, 100, 99, 98, 53, 97, 49, 50, 100, 51, 52, 49, 102, 57, 51, 50, 98, 52,
        57, 52, 0, 152, 1, 0, 0, 2, 10, 55, 54, 49, 57, 54, 238, 78, 0, 29, 78, 24, 4, 10, 55, 54,
        50, 50, 49, 238, 156, 0, 29, 156, 24, 6, 10, 55, 54, 50, 50, 52, 238, 78, 0, 29, 78, 24, 8,
        10, 55, 54, 50, 52, 53, 238, 78, 0, 29, 78, 24, 10, 10, 55, 54, 50, 54, 50, 238, 78, 0, 29,
        78, 20, 12, 10, 55, 54, 51, 48, 238, 56, 1, 46, 56, 1, 20, 14, 10, 55, 54, 51, 48, 238,
        156, 0, 46, 156, 0, 20, 16, 10, 55, 54, 51, 49, 238, 156, 0, 46, 156, 0, 0, 18, 1, 156, 0,
        49, 238, 134, 1, 46, 134, 1, 24, 20, 10, 55, 54, 51, 50, 57, 238, 78, 0, 29, 78, 20, 22,
        10, 55, 54, 51, 53, 238, 56, 1, 46, 56, 1, 20, 24, 10, 55, 54, 51, 53, 238, 190, 2, 46,
        190, 2, 20, 26, 10, 55, 54, 51, 57, 238, 134, 1, 46, 134, 1, 16, 28, 10, 55, 54, 51, 238,
        246, 3, 50, 246, 3, 24, 30, 10, 55, 54, 52, 48, 48, 238, 156, 0, 29, 156, 16, 32, 10, 55,
        54, 52, 238, 34, 2, 50, 34, 2, 20, 34, 10, 55, 54, 52, 51, 238, 212, 1, 46, 212, 1, 20, 36,
        10, 55, 54, 52, 51, 238, 56, 1, 46, 56, 1, 20, 38, 10, 55, 54, 52, 52, 238, 34, 2, 46, 34,
        2, 20, 40, 10, 55, 54, 52, 52, 238, 156, 0, 46, 156, 0, 16, 42, 10, 55, 54, 52, 238, 12, 3,
        50, 12, 3, 20, 44, 10, 55, 54, 52, 54, 238, 190, 2, 46, 190, 2, 24, 46, 10, 55, 54, 52, 54,
        51, 238, 78, 0, 29, 78, 0, 48, 5, 78, 0, 56, 238, 78, 0, 29, 78, 0, 50, 1, 78, 0, 55, 238,
        78, 0, 46, 78, 0, 0, 52, 1, 78, 0, 57, 238, 134, 1, 46, 134, 1, 16, 54, 10, 55, 54, 53,
        238, 24, 6, 50, 24, 6, 16, 56, 10, 55, 54, 53, 238, 24, 6, 50, 24, 6, 20, 58, 10, 55, 54,
        53, 49, 238, 56, 1, 46, 56, 1, 20, 60, 10, 55, 54, 53, 50, 238, 36, 9, 46, 36, 9, 16, 62,
        10, 55, 54, 53, 238, 68, 4, 50, 68, 4, 20, 64, 10, 55, 54, 53, 51, 238, 156, 0, 46, 156, 0,
        20, 66, 10, 55, 54, 53, 53, 238, 78, 0, 46, 78, 0, 20, 68, 10, 55, 54, 53, 54, 238, 68, 4,
        46, 68, 4, 20, 70, 10, 55, 54, 53, 56, 238, 24, 6, 46, 24, 6, 20, 72, 10, 55, 54, 53, 56,
        238, 134, 1, 46, 134, 1, 20, 74, 10, 55, 54, 53, 56, 238, 234, 0, 46, 234, 0, 0, 76, 5,
        234, 0, 57, 238, 234, 0, 29, 234, 20, 78, 10, 55, 54, 54, 49, 238, 56, 1, 46, 56, 1, 20,
        80, 10, 55, 54, 54, 51, 238, 46, 5, 46, 46, 5, 16, 82, 10, 55, 54, 54, 238, 70, 11, 50, 70,
        11, 20, 84, 10, 55, 54, 54, 52, 238, 190, 2, 46, 190, 2, 16, 86, 10, 55, 54, 54, 238, 102,
        6, 50, 102, 6, 16, 88, 10, 55, 54, 54, 238, 124, 5, 50, 124, 5, 20, 90, 10, 55, 54, 55, 49,
        238, 134, 1, 46, 134, 1, 20, 92, 10, 55, 54, 55, 51, 238, 234, 0, 46, 234, 0, 20, 94, 10,
        55, 54, 55, 52, 238, 78, 0, 46, 78, 0, 0, 96, 5, 78, 0, 56, 238, 78, 0, 29, 78, 0, 98, 1,
        78, 0, 55, 238, 12, 3, 46, 12, 3, 20, 100, 10, 55, 54, 55, 55, 238, 12, 3, 46, 12, 3, 16,
        102, 10, 55, 54, 55, 238, 236, 7, 50, 236, 7, 20, 104, 10, 55, 54, 55, 56, 238, 90, 3, 46,
        90, 3, 16, 106, 10, 55, 54, 55, 238, 48, 12, 50, 48, 12, 20, 108, 10, 55, 54, 56, 48, 238,
        170, 10, 46, 170, 10, 16, 110, 10, 55, 54, 56, 238, 224, 4, 50, 224, 4, 20, 112, 10, 55,
        54, 56, 49, 238, 68, 4, 46, 68, 4, 16, 114, 10, 55, 54, 56, 238, 46, 5, 50, 46, 5, 20, 116,
        10, 55, 54, 56, 52, 238, 78, 0, 46, 78, 0, 0, 118, 5, 78, 0, 52, 238, 78, 0, 29, 78, 0,
        120, 1, 78, 0, 54, 238, 134, 1, 46, 134, 1, 16, 122, 10, 55, 54, 56, 238, 58, 8, 50, 58, 8,
        16, 124, 10, 55, 54, 56, 238, 158, 7, 50, 158, 7, 20, 126, 10, 55, 54, 56, 57, 238, 158, 7,
        241, 158, 44, 154, 1, 0, 0, 128, 1, 10, 55, 54, 57, 48, 53, 238, 57, 1, 24, 51, 50, 98, 52,
        57, 52, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 27, 193, 0, 0, 0, 0, 0, 0, 27, 193, 0, 0,
        0, 0, 0, 0, 0, 0, 1, 255, 255, 255, 255, 184, 7, 0, 0, 0, 0, 0, 0, 27, 128, 0, 0, 3, 171,
        255, 255, 255, 255, 2, 254, 34, 38, 111, 0, 2, 0, 0, 0, 64, 0, 0, 1, 158, 203, 118, 71,
        219, 0, 0, 1, 158, 203, 118, 71, 219, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 27, 128, 0, 0, 0,
        65, 207, 39, 240, 88, 152, 1, 0, 0, 0, 10, 55, 54, 50, 48, 49, 128, 1, 57, 99, 100, 54, 53,
        98, 52, 56, 52, 48, 56, 102, 99, 101, 51, 57, 100, 99, 98, 98, 97, 50, 50, 55, 50, 100, 97,
        53, 56, 52, 55, 50, 48, 52, 98, 97, 51, 101, 49, 48, 50, 99, 99, 50, 57, 100, 99, 98, 53,
        97, 49, 50, 100, 51, 52, 49, 102, 57, 51, 50, 98, 52, 57, 52, 0, 152, 1, 0, 0, 2, 10, 55,
        54, 50, 51, 48, 238, 78, 0, 29, 78, 24, 4, 10, 55, 54, 50, 53, 56, 238, 156, 0, 29, 156,
        24, 6, 10, 55, 54, 50, 54, 54, 238, 78, 0, 29, 78, 24, 8, 10, 55, 54, 50, 56, 50, 238, 78,
        0, 29, 78, 0, 10, 5, 78, 0, 56, 238, 78, 0, 29, 78, 24, 12, 10, 55, 54, 51, 49, 52, 238,
        78, 0, 29, 78, 0, 14, 5, 78, 0, 56, 238, 78, 0, 29, 78, 0, 16, 1, 78, 0, 50, 238, 78, 0,
        46, 78, 0, 0, 18, 1, 78, 0, 51, 238, 190, 2, 46, 190, 2, 24, 20, 10, 55, 54, 51, 51, 55,
        238, 78, 0, 29, 78, 20, 22, 10, 55, 54, 51, 52, 238, 156, 0, 46, 156, 0, 24, 24, 10, 55,
        54, 51, 52, 51, 238, 78, 0, 29, 78, 20, 26, 10, 55, 54, 51, 53, 238, 78, 0, 46, 78, 0, 0,
        28, 5, 78, 0, 53, 238, 78, 0, 29, 78, 0, 30, 1, 78, 0, 55, 238, 190, 2, 46, 190, 2, 20, 32,
        10, 55, 54, 51, 57, 238, 78, 0, 46, 78, 0, 16, 34, 10, 55, 54, 52, 238, 46, 5, 50, 46, 5,
        16, 36, 10, 55, 54, 52, 238, 190, 2, 50, 190, 2, 20, 38, 10, 55, 54, 52, 52, 238, 90, 3,
        46, 90, 3, 24, 40, 10, 55, 54, 52, 52, 57, 238, 78, 0, 29, 78, 20, 42, 10, 55, 54, 52, 53,
        238, 124, 5, 46, 124, 5, 20, 44, 10, 55, 54, 52, 54, 238, 102, 6, 46, 102, 6, 20, 46, 10,
        55, 54, 52, 55, 238, 246, 3, 46, 246, 3, 20, 48, 10, 55, 54, 53, 49, 238, 156, 0, 46, 156,
        0, 20, 50, 10, 55, 54, 53, 49, 238, 56, 1, 46, 56, 1, 20, 52, 10, 55, 54, 53, 49, 238, 212,
        1, 46, 212, 1, 20, 54, 10, 55, 54, 53, 50, 238, 190, 2, 46, 190, 2, 20, 56, 10, 55, 54, 53,
        51, 238, 234, 0, 46, 234, 0, 20, 58, 10, 55, 54, 53, 52, 238, 146, 4, 46, 146, 4, 20, 60,
        10, 55, 54, 53, 53, 238, 78, 0, 46, 78, 0, 20, 62, 10, 55, 54, 53, 54, 238, 134, 1, 46,
        134, 1, 16, 64, 10, 55, 54, 53, 238, 46, 5, 50, 46, 5, 16, 66, 10, 55, 54, 53, 238, 12, 3,
        50, 12, 3, 20, 68, 10, 55, 54, 54, 48, 238, 156, 0, 46, 156, 0, 20, 70, 10, 55, 54, 54, 49,
        238, 112, 2, 46, 112, 2, 20, 72, 10, 55, 54, 54, 49, 238, 2, 7, 46, 2, 7, 16, 74, 10, 55,
        54, 54, 238, 58, 8, 50, 58, 8, 16, 76, 10, 55, 54, 54, 238, 112, 2, 50, 112, 2, 16, 78, 10,
        55, 54, 54, 238, 46, 5, 50, 46, 5, 20, 80, 10, 55, 54, 54, 54, 238, 46, 5, 46, 46, 5, 20,
        82, 10, 55, 54, 54, 56, 238, 134, 1, 46, 134, 1, 20, 84, 10, 55, 54, 55, 48, 238, 234, 0,
        46, 234, 0, 16, 86, 10, 55, 54, 55, 238, 190, 2, 50, 190, 2, 20, 88, 10, 55, 54, 55, 49,
        238, 212, 1, 46, 212, 1, 16, 90, 10, 55, 54, 55, 238, 24, 6, 50, 24, 6, 16, 92, 10, 55, 54,
        55, 238, 24, 6, 50, 24, 6, 16, 94, 10, 55, 54, 55, 238, 4, 14, 50, 4, 14, 16, 96, 10, 55,
        54, 55, 238, 90, 3, 50, 90, 3, 20, 98, 10, 55, 54, 55, 51, 238, 234, 0, 46, 234, 0, 20,
        100, 10, 55, 54, 55, 52, 238, 34, 2, 46, 34, 2, 20, 102, 10, 55, 54, 55, 53, 238, 78, 0,
        46, 78, 0, 0, 104, 5, 78, 0, 56, 238, 78, 0, 29, 78, 0, 106, 1, 78, 238, 238, 14, 50, 238,
        14, 16, 108, 10, 55, 54, 55, 238, 148, 11, 50, 148, 11, 20, 110, 10, 55, 54, 55, 57, 238,
        34, 2, 46, 34, 2, 20, 112, 10, 55, 54, 55, 57, 238, 34, 2, 46, 34, 2, 20, 114, 10, 55, 54,
        56, 48, 238, 168, 3, 46, 168, 3, 20, 116, 10, 55, 54, 56, 49, 238, 134, 1, 46, 134, 1, 20,
        118, 10, 55, 54, 56, 50, 238, 146, 4, 46, 146, 4, 20, 120, 10, 55, 54, 56, 52, 238, 246, 3,
        46, 246, 3, 16, 122, 10, 55, 54, 56, 238, 60, 15, 50, 60, 15, 16, 124, 10, 55, 54, 56, 238,
        90, 3, 50, 90, 3, 16, 126, 10, 55, 54, 56, 238, 158, 7, 245, 158, 40, 154, 1, 0, 0, 128, 1,
        10, 55, 54, 56, 56, 238, 157, 0, 17, 157, 0, 0, 1, 0, 1, 1,
    ];

    assert_eq!(
        expected,
        Frame::response(frame.header, frame.body, api_key, api_version)?
    );
    Ok(())
}

#[test]
fn find_coordinator_request_v1_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 10,
        api_version: 1,
        correlation_id: 0,
        client_id: None,
    };

    let body = FindCoordinatorRequest::default()
        .key(Some("abcdef".into()))
        .key_type(Some(0))
        .coordinator_keys(None)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 19, 0, 10, 0, 1, 0, 0, 0, 0, 255, 255, 0, 6, 97, 98, 99, 100, 101, 102, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn find_coordinator_response_v1_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Response { correlation_id: 0 };
    let body = FindCoordinatorResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(Some(0))
        .error_message(None)
        .node_id(Some(1002))
        .host(Some("ip-10-2-91-66.eu-west-1.compute.internal".into()))
        .port(Some(9092))
        .coordinators(None)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 62, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 0, 0, 3, 234, 0, 40, 105, 112, 45,
            49, 48, 45, 50, 45, 57, 49, 45, 54, 54, 46, 101, 117, 45, 119, 101, 115, 116, 45, 49,
            46, 99, 111, 109, 112, 117, 116, 101, 46, 105, 110, 116, 101, 114, 110, 97, 108, 0, 0,
            35, 132,
        ],
        Frame::response(header, body, FindCoordinatorResponse::KEY, 1)?,
    );

    Ok(())
}

#[test]
fn heartbeat_request_v4_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 12,
        api_version: 4,
        correlation_id: 10288,
        client_id: Some("console-consumer".into()),
    };

    let body = HeartbeatRequest::default()
        .group_id("test-consumer-group".into())
        .generation_id(0)
        .member_id("1000".into())
        .group_instance_id(None)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 58, 0, 12, 0, 4, 0, 0, 40, 48, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45,
            99, 111, 110, 115, 117, 109, 101, 114, 0, 20, 116, 101, 115, 116, 45, 99, 111, 110,
            115, 117, 109, 101, 114, 45, 103, 114, 111, 117, 112, 0, 0, 0, 0, 5, 49, 48, 48, 48, 0,
            0,
        ],
        Frame::request(header, body)?,
    );
    Ok(())
}

#[test]
fn init_producer_id_request_v4_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 22,
        api_version: 4,
        correlation_id: 2,
        client_id: Some("console-producer".into()),
    };

    let body = InitProducerIdRequest::default()
        .transactional_id(None)
        .transaction_timeout_ms(2147483647)
        .producer_id(Some(-1))
        .producer_epoch(Some(-1))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 43, 0, 22, 0, 4, 0, 0, 0, 2, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
            114, 111, 100, 117, 99, 101, 114, 0, 0, 127, 255, 255, 255, 255, 255, 255, 255, 255,
            255, 255, 255, 255, 255, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn join_group_request_v5_000() -> Result<()> {
    let _guard = init_tracing()?;

    let range_metadata =
        Bytes::from_static(b"\0\x03\0\0\0\x01\0\tbenchmark\0\0\0\0\0\0\0\0\xff\xff\xff\xff\0\0");
    let roundrobin_metadata =
        Bytes::from_static(b"\0\x03\0\0\0\x01\0\tbenchmark\0\0\0\0\0\0\0\0\xff\xff\xff\xff\0\0");

    let header = Header::Request {
        api_key: 11,
        api_version: 5,
        correlation_id: 3,
        client_id: Some("rdkafka".into()),
    };

    let body = JoinGroupRequest::default()
        .group_id("example_consumer_group_id".into())
        .session_timeout_ms(6000)
        .rebalance_timeout_ms(Some(300000))
        .member_id("".into())
        .group_instance_id(None)
        .protocol_type("consumer".into())
        .protocols(Some(
            [
                JoinGroupRequestProtocol::default()
                    .name("range".into())
                    .metadata(range_metadata),
                JoinGroupRequestProtocol::default()
                    .name("roundrobin".into())
                    .metadata(roundrobin_metadata),
            ]
            .into(),
        ))
        .reason(None)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 159, 0, 11, 0, 5, 0, 0, 0, 3, 0, 7, 114, 100, 107, 97, 102, 107, 97, 0, 25,
            101, 120, 97, 109, 112, 108, 101, 95, 99, 111, 110, 115, 117, 109, 101, 114, 95, 103,
            114, 111, 117, 112, 95, 105, 100, 0, 0, 23, 112, 0, 4, 147, 224, 0, 0, 255, 255, 0, 8,
            99, 111, 110, 115, 117, 109, 101, 114, 0, 0, 0, 2, 0, 5, 114, 97, 110, 103, 101, 0, 0,
            0, 31, 0, 3, 0, 0, 0, 1, 0, 9, 98, 101, 110, 99, 104, 109, 97, 114, 107, 0, 0, 0, 0, 0,
            0, 0, 0, 255, 255, 255, 255, 0, 0, 0, 10, 114, 111, 117, 110, 100, 114, 111, 98, 105,
            110, 0, 0, 0, 31, 0, 3, 0, 0, 0, 1, 0, 9, 98, 101, 110, 99, 104, 109, 97, 114, 107, 0,
            0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 0, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn join_group_response_v5_000() -> Result<()> {
    let _guard = init_tracing()?;

    let api_key = 11;
    let api_version = 5;

    let metadata =
        Bytes::from_static(b"\0\x03\0\0\0\x01\0\tbenchmark\0\0\0\0\0\0\0\0\xff\xff\xff\xff\0\0");

    let header = Header::Response { correlation_id: 4 };

    let body = JoinGroupResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(0)
        .generation_id(1)
        .protocol_type(None)
        .protocol_name(Some("range".into()))
        .leader("rdkafka-499e5770-375e-4990-bf84-a39634e3bfe4".into())
        .skip_assignment(None)
        .member_id("rdkafka-499e5770-375e-4990-bf84-a39634e3bfe4".into())
        .members(Some(
            [JoinGroupResponseMember::default()
                .member_id("rdkafka-499e5770-375e-4990-bf84-a39634e3bfe4".into())
                .group_instance_id(None)
                .metadata(metadata)]
            .into(),
        ))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 200, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 5, 114, 97, 110, 103, 101,
            0, 44, 114, 100, 107, 97, 102, 107, 97, 45, 52, 57, 57, 101, 53, 55, 55, 48, 45, 51,
            55, 53, 101, 45, 52, 57, 57, 48, 45, 98, 102, 56, 52, 45, 97, 51, 57, 54, 51, 52, 101,
            51, 98, 102, 101, 52, 0, 44, 114, 100, 107, 97, 102, 107, 97, 45, 52, 57, 57, 101, 53,
            55, 55, 48, 45, 51, 55, 53, 101, 45, 52, 57, 57, 48, 45, 98, 102, 56, 52, 45, 97, 51,
            57, 54, 51, 52, 101, 51, 98, 102, 101, 52, 0, 0, 0, 1, 0, 44, 114, 100, 107, 97, 102,
            107, 97, 45, 52, 57, 57, 101, 53, 55, 55, 48, 45, 51, 55, 53, 101, 45, 52, 57, 57, 48,
            45, 98, 102, 56, 52, 45, 97, 51, 57, 54, 51, 52, 101, 51, 98, 102, 101, 52, 255, 255,
            0, 0, 0, 31, 0, 3, 0, 0, 0, 1, 0, 9, 98, 101, 110, 99, 104, 109, 97, 114, 107, 0, 0, 0,
            0, 0, 0, 0, 0, 255, 255, 255, 255, 0, 0,
        ],
        Frame::response(header, body, api_key, api_version)?,
    );

    Ok(())
}

#[test]
fn list_groups_request_v4_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 16,
        api_version: 4,
        correlation_id: 84,
        client_id: Some("adminclient-1".into()),
    };

    let body = ListGroupsRequest::default()
        .states_filter(Some([].into()))
        .types_filter(Some([].into()))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 26, 0, 16, 0, 4, 0, 0, 0, 84, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105,
            101, 110, 116, 45, 49, 0, 1, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn list_offsets_response_v0_000() -> Result<()> {
    use nisshi_sans_io::list_offsets_response::{
        ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
    };

    let _guard = init_tracing()?;

    let header = Header::Response { correlation_id: 0 };

    let body = ListOffsetsResponse::default()
        .throttle_time_ms(None)
        .topics(Some(
            [ListOffsetsTopicResponse::default()
                .name("abcabcabcab".into())
                .partitions(Some(
                    [ListOffsetsPartitionResponse::default()
                        .partition_index(1)
                        .error_code(0)
                        .old_style_offsets(Some([1189284, 1174012, 1164900, 0].into()))
                        .timestamp(None)
                        .offset(None)
                        .leader_epoch(None)]
                    .into(),
                ))]
            .into(),
        ))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 67, 0, 0, 0, 0, 0, 0, 0, 1, 0, 11, 97, 98, 99, 97, 98, 99, 97, 98, 99, 97, 98,
            0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 18, 37, 164, 0, 0, 0, 0, 0,
            17, 233, 252, 0, 0, 0, 0, 0, 17, 198, 100, 0, 0, 0, 0, 0, 0, 0, 0,
        ],
        Frame::response(header, body, ListOffsetsResponse::KEY, 0)?,
    );

    Ok(())
}

#[test]
fn list_partition_reassignments_request_v0_000() -> Result<()> {
    use nisshi_sans_io::list_partition_reassignments_request::ListPartitionReassignmentsTopics;

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 46,
        api_version: 0,
        correlation_id: 7,
        client_id: Some("adminclient-1".into()),
    };

    let body = ListPartitionReassignmentsRequest::default()
        .timeout_ms(30_000)
        .topics(Some(
            [ListPartitionReassignmentsTopics::default()
                .name("test".into())
                .partition_indexes(Some([1, 0, 2].into()))]
            .into(),
        ))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 49, 0, 46, 0, 0, 0, 0, 0, 7, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
            110, 116, 45, 49, 0, 0, 0, 117, 48, 2, 5, 116, 101, 115, 116, 4, 0, 0, 0, 1, 0, 0, 0,
            0, 0, 0, 0, 2, 0, 0,
        ],
        Frame::request(header, body)?,
    );
    Ok(())
}

#[test]
fn metadata_request_v12_000() -> Result<()> {
    use nisshi_sans_io::metadata_request::MetadataRequestTopic;

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 3,
        api_version: 12,
        correlation_id: 5,
        client_id: Some("console-producer".into()),
    };

    let body = MetadataRequest::default()
        .topics(Some(
            [MetadataRequestTopic::default()
                .topic_id(Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]))
                .name(Some("test".into()))]
            .into(),
        ))
        .allow_auto_topic_creation(Some(true))
        .include_cluster_authorized_operations(None)
        .include_topic_authorized_operations(Some(false))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 53, 0, 3, 0, 12, 0, 0, 0, 5, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
            114, 111, 100, 117, 99, 101, 114, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            5, 116, 101, 115, 116, 0, 1, 0, 0,
        ],
        Frame::request(header, body)?,
    );
    Ok(())
}

#[test]
fn metadata_request_v7_000() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 3,
        api_version: 7,
        correlation_id: 0,
        client_id: Some("sarama".into()),
    };

    let body = MetadataRequest::default()
        .topics(None)
        .allow_auto_topic_creation(Some(false))
        .include_cluster_authorized_operations(None)
        .include_topic_authorized_operations(None)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 21, 0, 3, 0, 7, 0, 0, 0, 0, 0, 6, 115, 97, 114, 97, 109, 97, 255, 255, 255,
            255, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn metadata_response_v7_0000() -> Result<()> {
    use nisshi_sans_io::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };

    let _guard = init_tracing()?;

    let header = Header::Response { correlation_id: 0 };
    let body = MetadataResponse::default()
        .throttle_time_ms(Some(0))
        .brokers(Some(
            [MetadataResponseBroker::default()
                .node_id(1)
                .host("localhost".into())
                .port(9092)
                .rack(None)]
            .into(),
        ))
        .cluster_id(Some("5L6g3nShT-eMCtK--X86sw".into()))
        .controller_id(Some(1))
        .topics(Some(
            [MetadataResponseTopic::default()
                .error_code(0)
                .name(Some("test".into()))
                .topic_id(None)
                .is_internal(Some(false))
                .partitions(Some(
                    [
                        MetadataResponsePartition::default()
                            .error_code(0)
                            .partition_index(1)
                            .leader_id(1)
                            .leader_epoch(Some(0))
                            .replica_nodes(Some([1].into()))
                            .isr_nodes(Some([1].into()))
                            .offline_replicas(Some([].into())),
                        MetadataResponsePartition::default()
                            .error_code(0)
                            .partition_index(2)
                            .leader_id(1)
                            .leader_epoch(Some(0))
                            .replica_nodes(Some([1].into()))
                            .isr_nodes(Some([1].into()))
                            .offline_replicas(Some([].into())),
                        MetadataResponsePartition::default()
                            .error_code(0)
                            .partition_index(0)
                            .leader_id(1)
                            .leader_epoch(Some(0))
                            .replica_nodes(Some([1].into()))
                            .isr_nodes(Some([1].into()))
                            .offline_replicas(Some([].into())),
                    ]
                    .into(),
                ))
                .topic_authorized_operations(None)]
            .into(),
        ))
        .cluster_authorized_operations(None)
        .into();

    let api_key = 3;
    let api_version = 7;

    assert_eq!(
        vec![
            0, 0, 0, 180, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 9, 108, 111, 99, 97,
            108, 104, 111, 115, 116, 0, 0, 35, 132, 255, 255, 0, 22, 53, 76, 54, 103, 51, 110, 83,
            104, 84, 45, 101, 77, 67, 116, 75, 45, 45, 88, 56, 54, 115, 119, 0, 0, 0, 1, 0, 0, 0,
            1, 0, 0, 0, 4, 116, 101, 115, 116, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0,
            0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0,
            0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0,
            0, 0, 0,
        ],
        Frame::response(header, body, MetadataResponse::KEY, api_version)?,
    );

    Ok(())
}

#[test]
fn metadata_response_v7_0001() -> Result<()> {
    use nisshi_sans_io::metadata_response::{
        MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
    };

    let _guard = init_tracing()?;

    let header = Header::Response { correlation_id: 0 };

    // note that topic_id is not part of a v7 response
    let body = MetadataResponse::default()
        .throttle_time_ms(Some(0))
        .brokers(Some(
            [MetadataResponseBroker::default()
                .node_id(1)
                .host("localhost".into())
                .port(9092)
                .rack(None)]
            .into(),
        ))
        .cluster_id(Some("5L6g3nShT-eMCtK--X86sw".into()))
        .controller_id(Some(1))
        .topics(Some(
            [MetadataResponseTopic::default()
                .error_code(0)
                .name(Some("test".into()))
                .topic_id(Some([
                    118, 154, 146, 249, 19, 231, 73, 33, 136, 41, 108, 64, 151, 75, 30, 65,
                ]))
                .is_internal(Some(false))
                .partitions(Some(
                    [
                        MetadataResponsePartition::default()
                            .error_code(0)
                            .partition_index(1)
                            .leader_id(1)
                            .leader_epoch(Some(0))
                            .replica_nodes(Some([1].into()))
                            .isr_nodes(Some([1].into()))
                            .offline_replicas(Some([].into())),
                        MetadataResponsePartition::default()
                            .error_code(0)
                            .partition_index(2)
                            .leader_id(1)
                            .leader_epoch(Some(0))
                            .replica_nodes(Some([1].into()))
                            .isr_nodes(Some([1].into()))
                            .offline_replicas(Some([].into())),
                        MetadataResponsePartition::default()
                            .error_code(0)
                            .partition_index(0)
                            .leader_id(1)
                            .leader_epoch(Some(0))
                            .replica_nodes(Some([1].into()))
                            .isr_nodes(Some([1].into()))
                            .offline_replicas(Some([].into())),
                    ]
                    .into(),
                ))
                .topic_authorized_operations(None)]
            .into(),
        ))
        .cluster_authorized_operations(None)
        .into();

    let api_key = 3;
    let api_version = 7;

    assert_eq!(
        vec![
            0, 0, 0, 180, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 9, 108, 111, 99, 97,
            108, 104, 111, 115, 116, 0, 0, 35, 132, 255, 255, 0, 22, 53, 76, 54, 103, 51, 110, 83,
            104, 84, 45, 101, 77, 67, 116, 75, 45, 45, 88, 56, 54, 115, 119, 0, 0, 0, 1, 0, 0, 0,
            1, 0, 0, 0, 4, 116, 101, 115, 116, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0,
            0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0,
            0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0,
            0, 0, 0,
        ],
        Frame::response(header, body, api_key, api_version)?,
    );

    Ok(())
}

#[test]
fn metadata_request_v12_001() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 3,
        api_version: 12,
        correlation_id: 1,
        client_id: Some("console-producer".into()),
    };

    let body = MetadataRequest::default()
        .topics(Some([].into()))
        .allow_auto_topic_creation(Some(true))
        .include_cluster_authorized_operations(None)
        .include_topic_authorized_operations(Some(false))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 31, 0, 3, 0, 12, 0, 0, 0, 1, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
            114, 111, 100, 117, 99, 101, 114, 0, 1, 1, 0, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn metadata_request_v12_002() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 3,
        api_version: 12,
        correlation_id: 1,
        client_id: Some("console-producer".into()),
    };

    let body = MetadataRequest::default()
        .topics(Some([].into()))
        .allow_auto_topic_creation(Some(true))
        .include_cluster_authorized_operations(Some(false))
        .include_topic_authorized_operations(Some(false))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 31, 0, 3, 0, 12, 0, 0, 0, 1, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
            114, 111, 100, 117, 99, 101, 114, 0, 1, 1, 0, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn metadata_response_v12_0000() -> Result<()> {
    use nisshi_sans_io::metadata_response::{MetadataResponseBroker, MetadataResponseTopic};

    let _guard = init_tracing()?;

    let header = Header::Response { correlation_id: 5 };

    let body = MetadataResponse::default()
        .throttle_time_ms(Some(0))
        .brokers(Some(vec![
            MetadataResponseBroker::default()
                .node_id(0)
                .host("kafka-server".into())
                .port(9092)
                .rack(None),
        ]))
        .cluster_id(Some("RvQwrYegSUCkIPkaiAZQlQ".into()))
        .controller_id(Some(0))
        .topics(Some(vec![
            MetadataResponseTopic::default()
                .error_code(3)
                .name(Some("test".into()))
                .topic_id(Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]))
                .is_internal(Some(false))
                .partitions(Some(vec![]))
                .topic_authorized_operations(Some(-2147483648)),
        ]))
        .cluster_authorized_operations(None)
        .into();

    let api_key = 3;
    let api_version = 12;

    assert_eq!(
        vec![
            0, 0, 0, 92, 0, 0, 0, 5, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 13, 107, 97, 102, 107, 97, 45,
            115, 101, 114, 118, 101, 114, 0, 0, 35, 132, 0, 0, 23, 82, 118, 81, 119, 114, 89, 101,
            103, 83, 85, 67, 107, 73, 80, 107, 97, 105, 65, 90, 81, 108, 81, 0, 0, 0, 0, 2, 0, 3,
            5, 116, 101, 115, 116, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 128, 0, 0,
            0, 0, 0
        ],
        Frame::response(header, body, api_key, api_version)?,
    );

    Ok(())
}

#[test]
fn metadata_response_v12_0001() -> Result<()> {
    use nisshi_sans_io::metadata_response::{MetadataResponseBroker, MetadataResponseTopic};

    let _guard = init_tracing()?;

    let header = Header::Response { correlation_id: 5 };

    let body = MetadataResponse::default()
        .throttle_time_ms(Some(0))
        .brokers(Some(vec![
            MetadataResponseBroker::default()
                .node_id(0)
                .host("kafka-server".into())
                .port(9092)
                .rack(None),
        ]))
        .cluster_id(Some("RvQwrYegSUCkIPkaiAZQlQ".into()))
        .controller_id(Some(0))
        .topics(Some(vec![
            MetadataResponseTopic::default()
                .error_code(3)
                .name(Some("test".into()))
                .topic_id(Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]))
                .is_internal(Some(false))
                .partitions(Some(vec![]))
                .topic_authorized_operations(Some(-2147483648)),
        ]))
        .cluster_authorized_operations(Some(-1))
        .into();

    let api_key = 3;
    let api_version = 12;

    assert_eq!(
        vec![
            0, 0, 0, 92, 0, 0, 0, 5, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 13, 107, 97, 102, 107, 97, 45,
            115, 101, 114, 118, 101, 114, 0, 0, 35, 132, 0, 0, 23, 82, 118, 81, 119, 114, 89, 101,
            103, 83, 85, 67, 107, 73, 80, 107, 97, 105, 65, 90, 81, 108, 81, 0, 0, 0, 0, 2, 0, 3,
            5, 116, 101, 115, 116, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 128, 0, 0,
            0, 0, 0
        ],
        Frame::response(header, body, api_key, api_version)?,
    );

    Ok(())
}

#[test]
fn offset_fetch_request_v3_000() -> Result<()> {
    use nisshi_sans_io::offset_fetch_request::OffsetFetchRequestTopic;

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 9,
        api_version: 3,
        correlation_id: 0,
        client_id: None,
    };

    let body = OffsetFetchRequest::default()
        .group_id(Some("abc".into()))
        .topics(Some(
            [
                OffsetFetchRequestTopic::default()
                    .name("test2".into())
                    .partition_indexes(Some([3, 4, 5].into())),
                OffsetFetchRequestTopic::default()
                    .name("test1".into())
                    .partition_indexes(Some([0, 1, 2].into())),
            ]
            .into(),
        ))
        .groups(None)
        .require_stable(None)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 65, 0, 9, 0, 3, 0, 0, 0, 0, 255, 255, 0, 3, 97, 98, 99, 0, 0, 0, 2, 0, 5, 116,
            101, 115, 116, 50, 0, 0, 0, 3, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5, 0, 5, 116, 101, 115,
            116, 49, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn offset_for_leader_request_v0_000() -> Result<()> {
    use nisshi_sans_io::offset_for_leader_epoch_request::OffsetForLeaderTopic;

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 23,
        api_version: 0,
        correlation_id: 0,
        client_id: None,
    };

    let body = OffsetForLeaderEpochRequest::default()
        .replica_id(None)
        .topics(Some(
            [OffsetForLeaderTopic::default()
                .topic("abcabcabcab".into())
                .partitions(Some([].into()))]
            .into(),
        ))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 31, 0, 23, 0, 0, 0, 0, 0, 0, 255, 255, 0, 0, 0, 1, 0, 11, 97, 98, 99, 97, 98,
            99, 97, 98, 99, 97, 98, 0, 0, 0, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn produce_request_v9_000() -> Result<()> {
    use nisshi_sans_io::produce_request::{PartitionProduceData, TopicProduceData};

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 0,
        api_version: 9,
        correlation_id: 6,
        client_id: Some("console-producer".into()),
    };

    let body = ProduceRequest::default()
        .transactional_id(None)
        .acks(-1)
        .timeout_ms(1500)
        .topic_data(Some(
            [TopicProduceData::default()
                .name("test".into())
                .partition_data(Some(
                    [PartitionProduceData::default().index(0).records(Some(
                        inflated::Frame {
                            batches: [Batch {
                                base_offset: 0,
                                batch_length: 59,
                                partition_leader_epoch: -1,
                                magic: 2,
                                crc: 1126819645,
                                attributes: 0,
                                last_offset_delta: 0,
                                base_timestamp: 1707058170165,
                                max_timestamp: 1707058170165,
                                producer_id: 1,
                                producer_epoch: 0,
                                base_sequence: 1,
                                records: [Record {
                                    length: 9,
                                    attributes: 0,
                                    timestamp_delta: 0,
                                    offset_delta: 0,
                                    key: None,
                                    value: Some(Bytes::from_static(&[100, 101, 102])),
                                    headers: [].into(),
                                }]
                                .into(),
                            }]
                            .into(),
                        }
                        .try_into()?,
                    ))]
                    .into(),
                ))]
            .into(),
        ))
        .into();

    assert_eq!(
        &[
            0, 0, 0, 120, 0, 0, 0, 9, 0, 0, 0, 6, 0, 16, 99, 111, 110, 115, 111, 108, 101, 45, 112,
            114, 111, 100, 117, 99, 101, 114, 0, 0, 255, 255, 0, 0, 5, 220, 2, 5, 116, 101, 115,
            116, 2, 0, 0, 0, 0, 72, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 59, 255, 255, 255, 255, 2, 67,
            41, 231, 61, 0, 0, 0, 0, 0, 0, 0, 0, 1, 141, 116, 152, 137, 53, 0, 0, 1, 141, 116, 152,
            137, 53, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 18, 0, 0, 0, 1, 6, 100,
            101, 102, 0, 0, 0, 0,
        ],
        &Frame::request(header, body)?[..],
    );

    Ok(())
}

#[test]
fn produce_response_v9_000() -> Result<()> {
    use nisshi_sans_io::produce_response::{PartitionProduceResponse, TopicProduceResponse};

    let _guard = init_tracing()?;

    let header = Header::Response { correlation_id: 6 };
    let body = ProduceResponse::default()
        .responses(Some(
            [TopicProduceResponse::default()
                .name("test".into())
                .partition_responses(Some(
                    [PartitionProduceResponse::default()
                        .index(0)
                        .error_code(0)
                        .base_offset(2)
                        .log_append_time_ms(Some(-1))
                        .log_start_offset(Some(0))
                        .record_errors(Some([].into()))
                        .error_message(None)
                        .current_leader(None)]
                    .into(),
                ))]
            .into(),
        ))
        .node_endpoints(None)
        .throttle_time_ms(Some(0))
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 51, 0, 0, 0, 6, 0, 2, 5, 116, 101, 115, 116, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 2, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
            0, 0, 0, 0, 0,
        ],
        Frame::response(header, body, 0, 9)?
    );

    Ok(())
}

#[test]
fn describe_topic_partitions_request_v0_000() -> Result<()> {
    use nisshi_sans_io::describe_topic_partitions_request::TopicRequest;

    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 75,
        api_version: 0,
        correlation_id: 5,
        client_id: Some("adminclient-1".into()),
    };

    let body = DescribeTopicPartitionsRequest::default()
        .topics(Some([TopicRequest::default().name("test".into())].into()))
        .response_partition_limit(2000)
        .cursor(None)
        .into();

    assert_eq!(
        vec![
            0, 0, 0, 37, 0, 75, 0, 0, 0, 0, 0, 5, 0, 13, 97, 100, 109, 105, 110, 99, 108, 105, 101,
            110, 116, 45, 49, 0, 2, 5, 116, 101, 115, 116, 0, 0, 0, 7, 208, 255, 0,
        ],
        Frame::request(header, body)?,
    );

    Ok(())
}

#[test]
fn describe_topic_partitions_response_v0_000() -> Result<()> {
    use nisshi_sans_io::describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    };

    let _guard = init_tracing()?;

    let v = vec![
        0, 0, 0, 126, 0, 0, 0, 5, 0, 0, 0, 0, 0, 2, 0, 0, 5, 116, 101, 115, 116, 113, 142, 248, 9,
        90, 152, 68, 142, 161, 218, 25, 210, 166, 234, 204, 62, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
        0, 0, 0, 0, 2, 0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0,
        0, 0, 2, 0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0,
        2, 0, 0, 0, 1, 2, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 13, 248, 0, 255, 0,
    ];

    let api_key = 75;
    let api_version = 0;

    let header = Header::Response { correlation_id: 5 };

    let body = DescribeTopicPartitionsResponse::default()
        .throttle_time_ms(0)
        .topics(Some(
            [DescribeTopicPartitionsResponseTopic::default()
                .error_code(0)
                .name(Some("test".into()))
                .topic_id([
                    113, 142, 248, 9, 90, 152, 68, 142, 161, 218, 25, 210, 166, 234, 204, 62,
                ])
                .is_internal(false)
                .partitions(Some(
                    [
                        DescribeTopicPartitionsResponsePartition::default()
                            .error_code(0)
                            .partition_index(0)
                            .leader_id(1)
                            .leader_epoch(0)
                            .replica_nodes(Some([1].into()))
                            .isr_nodes(Some([1].into()))
                            .eligible_leader_replicas(Some([].into()))
                            .last_known_elr(Some([].into()))
                            .offline_replicas(Some([].into())),
                        DescribeTopicPartitionsResponsePartition::default()
                            .error_code(0)
                            .partition_index(1)
                            .leader_id(1)
                            .leader_epoch(0)
                            .replica_nodes(Some([1].into()))
                            .isr_nodes(Some([1].into()))
                            .eligible_leader_replicas(Some([].into()))
                            .last_known_elr(Some([].into()))
                            .offline_replicas(Some([].into())),
                        DescribeTopicPartitionsResponsePartition::default()
                            .error_code(0)
                            .partition_index(2)
                            .leader_id(1)
                            .leader_epoch(0)
                            .replica_nodes(Some([1].into()))
                            .isr_nodes(Some([1].into()))
                            .eligible_leader_replicas(Some([].into()))
                            .last_known_elr(Some([].into()))
                            .offline_replicas(Some([].into())),
                    ]
                    .into(),
                ))
                .topic_authorized_operations(3576)]
            .into(),
        ))
        .next_cursor(None)
        .into();

    assert_eq!(v, Frame::response(header, body, api_key, api_version)?);

    Ok(())
}
