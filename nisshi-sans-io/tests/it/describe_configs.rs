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
use nisshi_model::{MessageKind, VersionRange};
use nisshi_sans_io::{
    Body, DescribeConfigsRequest, Frame, Header, MESSAGE_META, Result,
    describe_configs_request::DescribeConfigsResource,
};
use std::collections::BTreeMap;

// Regression test for a decoder bug found by `fuzz_describe_configs_storage`:
// a non-empty, non-null `ConfigurationKeys` (a nullable `[]string`) failed to
// decode because the decoder mistook the nullability of the array itself for
// the nullability of its individual string elements, and so skipped reading
// their length prefix.
fn request_round_trips(api_version: i16) -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 32,
        api_version,
        correlation_id: 0,
        client_id: Some("test".into()),
    };

    let mut request = DescribeConfigsRequest::default().resources(Some(
        [DescribeConfigsResource::default()
            .resource_type(2)
            .resource_name("test".into())
            .configuration_keys(Some(["retention.ms".into(), "".into()].into()))]
        .into(),
    ));

    if api_version >= 1 {
        request = request.include_synonyms(Some(false));
    }

    if api_version >= 3 {
        request = request.include_documentation(Some(false));
    }

    let body: Body = request.into();

    let decoded = Frame::request(header, body.clone()).and_then(Frame::request_from_bytes)?;

    assert_eq!(body, decoded.body);

    Ok(())
}

#[test]
fn request_non_empty_configuration_keys_v0() -> Result<()> {
    request_round_trips(0)
}

#[test]
fn request_non_empty_configuration_keys_v4() -> Result<()> {
    request_round_trips(4)
}

#[test]
fn response() {
    let _guard = init_tracing().unwrap();

    let message_name = "DescribeConfigsResponse";
    assert!(BTreeMap::from(MESSAGE_META).contains_key(message_name));

    let meta = BTreeMap::from(MESSAGE_META);

    let message = meta.get(message_name).unwrap();
    assert_eq!(32, message.api_key);
    assert_eq!(MessageKind::Response, message.message_kind);

    let structures = message.structures();
    let mut keys: Vec<_> = structures.into_iter().map(|(name, _)| name).collect();
    keys.sort();

    assert_eq!(
        vec![
            "DescribeConfigsResourceResult",
            "DescribeConfigsResult",
            "DescribeConfigsSynonym",
        ],
        keys
    );

    assert_eq!(
        "DescribeConfigsResult",
        message
            .field("results")
            .and_then(|field| field.kind.kind_of_sequence())
            .map(|kind_meta| kind_meta.0)
            .unwrap()
    );

    assert_eq!(
        "DescribeConfigsResourceResult",
        message
            .field("results")
            .and_then(|results| results.field("configs"))
            .and_then(|field| field.kind.kind_of_sequence())
            .map(|kind_meta| kind_meta.0)
            .unwrap()
    );

    assert_eq!(
        "bool",
        message
            .field("results")
            .and_then(|results| results.field("configs"))
            .and_then(|configs| configs.field("read_only"))
            .map(|read_only| read_only.kind.0)
            .unwrap()
    );

    assert_eq!(
        VersionRange {
            start: 0,
            end: i16::MAX
        },
        message
            .field("results")
            .and_then(|results| results.field("configs"))
            .and_then(|configs| configs.field("read_only"))
            .map(|read_only| read_only.version)
            .unwrap()
    );

    assert_eq!(
        "bool",
        message
            .field("results")
            .and_then(|results| results.field("configs"))
            .and_then(|configs| configs.field("is_default"))
            .map(|read_only| read_only.kind.0)
            .unwrap()
    );

    assert_eq!(
        VersionRange { start: 0, end: 0 },
        message
            .field("results")
            .and_then(|results| results.field("configs"))
            .and_then(|configs| configs.field("is_default"))
            .map(|read_only| read_only.version)
            .unwrap()
    );
}
