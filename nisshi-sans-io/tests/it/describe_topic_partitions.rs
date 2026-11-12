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
use nisshi_model::MessageKind;
use nisshi_sans_io::{
    Body, DescribeTopicPartitionsRequest, Frame, Header, MESSAGE_META, Result,
    describe_topic_partitions_request::{Cursor, TopicRequest},
};
use std::collections::BTreeMap;

// Regression test for an encoder bug found by `fuzz_describe_topic_partitions_storage`:
// a populated (non-null) nullable struct field encoded no presence marker
// at all, even though `serialize_none` already wrote one for the null
// case, desyncing every field written after it (KIP-893 requires one for
// both: https://cwiki.apache.org/confluence/display/KAFKA/KIP-893).
#[test]
fn request_non_null_cursor_round_trips() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 75,
        api_version: 0,
        correlation_id: 0,
        client_id: Some("test".into()),
    };

    let body: Body = DescribeTopicPartitionsRequest::default()
        .topics(Some([TopicRequest::default().name("test".into())].into()))
        .response_partition_limit(2000)
        .cursor(Some(
            Cursor::default()
                .topic_name("test".into())
                .partition_index(3),
        ))
        .into();

    let decoded = Frame::request(header, body.clone()).and_then(Frame::request_from_bytes)?;

    assert_eq!(body, decoded.body);

    Ok(())
}

#[test]
fn request() {
    let _guard = init_tracing().unwrap();

    assert!(BTreeMap::from(MESSAGE_META).contains_key("DescribeTopicPartitionsRequest"));

    let meta = BTreeMap::from(MESSAGE_META);

    let message = meta.get("DescribeTopicPartitionsRequest").unwrap();
    assert_eq!(75, message.api_key);
    assert_eq!(MessageKind::Request, message.message_kind);

    let structures = message.structures();
    let mut keys: Vec<_> = structures.into_iter().map(|(name, _)| name).collect();
    keys.sort();

    assert_eq!(vec!["Cursor", "TopicRequest",], keys);

    assert!(
        message
            .field("topics")
            .map(|field| field.kind.is_sequence())
            .unwrap()
    );

    assert!(
        message
            .field("topics")
            .map(|field| field.is_structure())
            .unwrap()
    );

    assert!(
        !message
            .field("response_partition_limit")
            .map(|field| field.kind.is_sequence())
            .unwrap()
    );

    assert!(
        !message
            .field("response_partition_limit")
            .map(|field| field.is_structure())
            .unwrap()
    );

    assert!(
        !message
            .field("cursor")
            .map(|field| field.kind.is_sequence())
            .unwrap()
    );

    assert!(
        message
            .field("cursor")
            .map(|field| field.is_nullable(i16::MAX))
            .unwrap()
    );

    assert!(
        message
            .field("cursor")
            .map(|field| field.is_structure())
            .unwrap()
    );
}
