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

//! A field that exists at the encoded version, is not nullable and is not tagged has no wire
//! encoding for `None`. The encoder used to write nothing for it, which shifted every later field:
//! `cat consume` left Fetch v17's `current_leader_epoch`, `last_fetched_epoch` and `topic_id`
//! unset, lost 8 bytes, and the broker read `fetch_offset` as 4294967295. The encoder now refuses.

use std::assert_matches;

use crate::common::init_tracing;
use nisshi_sans_io::{
    Body, Error, Frame, Header, JoinGroupResponse, Result,
    fetch_request::{FetchPartition, FetchRequest, FetchTopic},
};

const FETCH: i16 = 1;
const JOIN_GROUP: i16 = 11;

fn header(api_version: i16) -> Header {
    Header::Request {
        api_key: FETCH,
        api_version,
        correlation_id: 0,
        client_id: Some("test".into()),
    }
}

fn fetch(partition: FetchPartition) -> FetchRequest {
    FetchRequest::default()
        .cluster_id(Some("".into()))
        .replica_id(Some(-1))
        .replica_state(Some(Default::default()))
        .max_wait_ms(0)
        .min_bytes(0)
        .max_bytes(Some(1024))
        .isolation_level(Some(0))
        .session_id(Some(-1))
        .session_epoch(Some(-1))
        .topics(Some(vec![
            FetchTopic::default()
                .topic(Some("t".into()))
                .topic_id(Some([1; 16]))
                .partitions(Some(vec![partition])),
        ]))
        .forgotten_topics_data(Some(vec![]))
        .rack_id(Some("".into()))
}

fn complete_partition() -> FetchPartition {
    FetchPartition::default()
        .partition(0)
        .current_leader_epoch(Some(-1))
        .fetch_offset(42)
        .last_fetched_epoch(Some(-1))
        .log_start_offset(Some(-1))
        .partition_max_bytes(1024)
}

#[test]
fn unset_mandatory_scalar_is_an_error() -> Result<()> {
    let _guard = init_tracing()?;

    // v17 needs `current_leader_epoch` (v9+) and `last_fetched_epoch` (v12+).
    let partition = FetchPartition::default()
        .partition(0)
        .fetch_offset(42)
        .partition_max_bytes(1024);

    assert_matches!(
        Frame::request(header(17), fetch(partition).into()),
        Err(Error::MandatoryFieldUnset { .. })
    );

    Ok(())
}

#[test]
fn error_names_the_field() -> Result<()> {
    let _guard = init_tracing()?;

    let partition = complete_partition().current_leader_epoch(None);

    let Err(Error::MandatoryFieldUnset { field, api_version }) =
        Frame::request(header(17), fetch(partition).into())
    else {
        panic!("expected MandatoryFieldUnset");
    };

    assert!(field.ends_with("current_leader_epoch"), "{field}");
    assert_eq!(17, api_version);

    Ok(())
}

#[test]
fn unset_field_outside_its_version_range_is_skipped() -> Result<()> {
    let _guard = init_tracing()?;

    // `current_leader_epoch` starts at v9, so v8 doesn't have it and `None` is right.
    let request = FetchRequest::default()
        .replica_id(Some(-1))
        .max_wait_ms(0)
        .min_bytes(0)
        .max_bytes(Some(1024))
        .isolation_level(Some(0))
        .session_id(Some(-1))
        .session_epoch(Some(-1))
        .topics(Some(vec![
            FetchTopic::default()
                .topic(Some("t".into()))
                .partitions(Some(vec![
                    FetchPartition::default()
                        .partition(0)
                        .fetch_offset(42)
                        .log_start_offset(Some(-1))
                        .partition_max_bytes(1024),
                ])),
        ]))
        .forgotten_topics_data(Some(vec![]));

    let decoded = Frame::request(header(8), request.into()).and_then(Frame::request_from_bytes)?;

    let Body::FetchRequest(decoded) = decoded.body else {
        panic!("wrong body variant: {decoded:?}");
    };

    let partition = &decoded.topics.as_ref().unwrap()[0]
        .partitions
        .as_ref()
        .unwrap()[0];
    assert_eq!(42, partition.fetch_offset);

    Ok(())
}

#[test]
fn fully_set_request_round_trips() -> Result<()> {
    let _guard = init_tracing()?;

    let decoded = Frame::request(header(17), fetch(complete_partition()).into())
        .and_then(Frame::request_from_bytes)?;

    let Body::FetchRequest(decoded) = decoded.body else {
        panic!("wrong body variant: {decoded:?}");
    };

    let partition = &decoded.topics.as_ref().unwrap()[0]
        .partitions
        .as_ref()
        .unwrap()[0];
    assert_eq!(42, partition.fetch_offset);
    assert_eq!(Some(-1), partition.last_fetched_epoch);

    Ok(())
}

#[test]
fn unset_mandatory_field_in_a_response_is_an_error() -> Result<()> {
    use nisshi_sans_io::{
        DescribeConfigsResponse,
        describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    };

    let _guard = init_tracing()?;

    // `is_default` exists only at v0, where it is not nullable.
    let response = |is_default| {
        DescribeConfigsResponse::default()
            .throttle_time_ms(0)
            .results(Some(vec![DescribeConfigsResult::default().configs(Some(
                vec![
                DescribeConfigsResourceResult::default()
                    .name("n".into())
                    .value(Some("v".into()))
                    .read_only(false)
                    .is_default(is_default)
                    .is_sensitive(false),
            ],
            ))]))
    };

    assert_matches!(
        Frame::response(
            Header::Response { correlation_id: 0 },
            response(None).into(),
            32,
            0
        ),
        Err(Error::MandatoryFieldUnset { .. })
    );

    _ = Frame::response(
        Header::Response { correlation_id: 0 },
        response(Some(false)).into(),
        32,
        0,
    )?;

    Ok(())
}

fn join_group_response_without_protocol_name() -> JoinGroupResponse {
    JoinGroupResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(0)
        .generation_id(-1)
        .protocol_type(None)
        .protocol_name(None)
        .leader("".into())
        .skip_assignment(Some(false))
        .member_id("m".into())
        .members(Some(vec![]))
}

#[test]
fn field_nullable_only_from_some_version_errors_before_it_and_encodes_null_from_it() -> Result<()> {
    let _guard = init_tracing()?;

    // JoinGroupResponse `ProtocolName` exists from v0 and is nullable only from v7.
    let encode = |api_version| {
        Frame::response(
            Header::Response { correlation_id: 0 },
            join_group_response_without_protocol_name().into(),
            JOIN_GROUP,
            api_version,
        )
    };

    assert_matches!(
        encode(6),
        Err(Error::MandatoryFieldUnset { api_version: 6, .. })
    );

    let decoded = encode(7).and_then(|bytes| Frame::response_from_bytes(bytes, JOIN_GROUP, 7))?;

    let Body::JoinGroupResponse(decoded) = decoded.body else {
        panic!("wrong body variant: {decoded:?}");
    };
    assert_eq!(None, decoded.protocol_name);

    Ok(())
}
