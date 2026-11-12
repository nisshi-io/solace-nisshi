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
use nisshi_sans_io::MESSAGE_META;
use std::collections::BTreeMap;

const FETCH_RESPONSE: &str = "FetchResponse";

#[test]
fn check_message_meta() {
    let _guard = init_tracing().unwrap();

    assert!(BTreeMap::from(MESSAGE_META).contains_key(FETCH_RESPONSE));

    let meta = BTreeMap::from(MESSAGE_META);

    let message = meta.get(FETCH_RESPONSE).unwrap();
    assert_eq!(1, message.api_key);
    assert_eq!(MessageKind::Response, message.message_kind);
    assert_eq!(VersionRange { start: 0, end: 17 }, message.version.valid);
    assert_eq!(
        VersionRange {
            start: 12,
            end: i16::MAX
        },
        message.version.flexible
    );
}

#[test]
fn throttle_time_ms() {
    let meta = BTreeMap::from(MESSAGE_META);
    let message = meta.get(FETCH_RESPONSE).unwrap();
    let message_fields = BTreeMap::from_iter(message.fields.iter().copied());

    let throttle_time_ms = message_fields["throttle_time_ms"];
    assert_eq!(
        VersionRange {
            start: 1,
            end: i16::MAX
        },
        throttle_time_ms.version
    );
    assert!(!throttle_time_ms.is_mandatory(None));
}

#[test]
fn responses() {
    let meta = BTreeMap::from(MESSAGE_META);
    let message = meta.get(FETCH_RESPONSE).unwrap();
    let message_fields = BTreeMap::from_iter(message.fields.iter().copied());
    let responses = message_fields["responses"];
    assert_eq!(
        VersionRange {
            start: 0,
            end: i16::MAX
        },
        responses.version
    );
    assert!(responses.is_mandatory(None));
}

#[test]
fn responses_topic() {
    let meta = BTreeMap::from(MESSAGE_META);
    let message = meta.get(FETCH_RESPONSE).unwrap();
    let message_fields = BTreeMap::from_iter(message.fields.iter().copied());
    let responses = message_fields["responses"];
    let responses_fields = BTreeMap::from_iter(responses.fields.iter().copied());

    let topic = responses_fields["topic"];
    assert_eq!(VersionRange { start: 0, end: 12 }, topic.version);
    assert!(!topic.is_mandatory(Some(responses.version)));
}

#[test]
fn responses_partitions() {
    let meta = BTreeMap::from(MESSAGE_META);
    let message = meta.get(FETCH_RESPONSE).unwrap();
    let message_fields = BTreeMap::from_iter(message.fields.iter().copied());
    let responses = message_fields["responses"];
    let responses_fields = BTreeMap::from_iter(responses.fields.iter().copied());
    let partitions = responses_fields["partitions"];

    assert_eq!(
        VersionRange {
            start: 0,
            end: i16::MAX
        },
        partitions.version
    );
    assert!(partitions.is_mandatory(Some(responses.version)));
}

#[test]
fn responses_partitions_partition_index() {
    let meta = BTreeMap::from(MESSAGE_META);
    let message = meta.get(FETCH_RESPONSE).unwrap();
    let message_fields = BTreeMap::from_iter(message.fields.iter().copied());

    let responses = message_fields["responses"];
    let responses_fields = BTreeMap::from_iter(responses.fields.iter().copied());

    let partitions = responses_fields["partitions"];
    let partitions_fields = BTreeMap::from_iter(partitions.fields.iter().copied());

    let partition_index = partitions_fields["partition_index"];
    assert_eq!(
        VersionRange {
            start: 0,
            end: i16::MAX
        },
        partition_index.version
    );
    assert!(partition_index.is_mandatory(Some(partitions.version)));
}

#[test]
fn responses_partitions_error_code() {
    let meta = BTreeMap::from(MESSAGE_META);
    let message = meta.get(FETCH_RESPONSE).unwrap();
    let message_fields = BTreeMap::from_iter(message.fields.iter().copied());
    let responses = message_fields["responses"];
    let responses_fields = BTreeMap::from_iter(responses.fields.iter().copied());

    let partitions = responses_fields["partitions"];
    let partitions_fields = BTreeMap::from_iter(partitions.fields.iter().copied());

    let error_code = partitions_fields["error_code"];
    assert_eq!(
        VersionRange {
            start: 0,
            end: i16::MAX
        },
        error_code.version
    );
    assert!(error_code.is_mandatory(Some(partitions.version)));
}

#[test]
fn responses_partitions_high_watermark() {
    let meta = BTreeMap::from(MESSAGE_META);
    let message = meta.get(FETCH_RESPONSE).unwrap();
    let message_fields = BTreeMap::from_iter(message.fields.iter().copied());
    let responses = message_fields["responses"];
    let responses_fields = BTreeMap::from_iter(responses.fields.iter().copied());

    let partitions = responses_fields["partitions"];
    let partitions_fields = BTreeMap::from_iter(partitions.fields.iter().copied());

    let high_watermark = partitions_fields["high_watermark"];
    assert_eq!(
        VersionRange {
            start: 0,
            end: i16::MAX
        },
        high_watermark.version
    );
    assert!(high_watermark.is_mandatory(Some(partitions.version)));
}

#[test]
fn responses_partitions_last_stable_offset() {
    let meta = BTreeMap::from(MESSAGE_META);
    let message = meta.get(FETCH_RESPONSE).unwrap();
    let message_fields = BTreeMap::from_iter(message.fields.iter().copied());
    let responses = message_fields["responses"];
    let responses_fields = BTreeMap::from_iter(responses.fields.iter().copied());

    let partitions = responses_fields["partitions"];
    let partitions_fields = BTreeMap::from_iter(partitions.fields.iter().copied());

    let last_stable_offset = partitions_fields["last_stable_offset"];
    assert_eq!(
        VersionRange {
            start: 4,
            end: i16::MAX
        },
        last_stable_offset.version
    );
    assert!(!last_stable_offset.is_mandatory(Some(partitions.version)));
}

#[test]
fn responses_partitions_diverging_epoch() {
    let meta = BTreeMap::from(MESSAGE_META);
    let message = meta.get(FETCH_RESPONSE).unwrap();
    let message_fields = BTreeMap::from_iter(message.fields.iter().copied());
    let responses = message_fields["responses"];
    let responses_fields = BTreeMap::from_iter(responses.fields.iter().copied());
    let partitions = responses_fields["partitions"];
    let partitions_fields = BTreeMap::from_iter(partitions.fields.iter().copied());

    let diverging_epoch = partitions_fields["diverging_epoch"];
    assert_eq!(
        VersionRange {
            start: 12,
            end: i16::MAX
        },
        diverging_epoch.version
    );
    assert!(!diverging_epoch.is_mandatory(Some(partitions.version)));

    let diverging_epoch_fields = BTreeMap::from_iter(diverging_epoch.fields.iter().copied());
    let epoch = diverging_epoch_fields["epoch"];
    assert_eq!(
        VersionRange {
            start: 12,
            end: i16::MAX
        },
        epoch.version
    );
    assert!(epoch.is_mandatory(Some(diverging_epoch.version)));

    let end_offset = diverging_epoch_fields["end_offset"];
    assert_eq!(
        VersionRange {
            start: 12,
            end: i16::MAX
        },
        end_offset.version
    );
    assert!(end_offset.is_mandatory(Some(diverging_epoch.version)));
}

#[test]
fn responses_partitions_snapshot_id() {
    let meta = BTreeMap::from(MESSAGE_META);
    let message = meta.get(FETCH_RESPONSE).unwrap();
    let message_fields = BTreeMap::from_iter(message.fields.iter().copied());
    let responses = message_fields["responses"];
    let responses_fields = BTreeMap::from_iter(responses.fields.iter().copied());
    let partitions = responses_fields["partitions"];
    let partitions_fields = BTreeMap::from_iter(partitions.fields.iter().copied());

    let snapshot_id = partitions_fields["snapshot_id"];
    assert_eq!(
        VersionRange {
            start: 12,
            end: i16::MAX
        },
        snapshot_id.version
    );
    assert!(!snapshot_id.is_mandatory(Some(partitions.version)));

    let snapshot_id_fields = BTreeMap::from_iter(snapshot_id.fields.iter().copied());
    let end_offset = snapshot_id_fields["end_offset"];
    assert_eq!(
        VersionRange {
            start: 0,
            end: i16::MAX
        },
        end_offset.version
    );
    assert!(end_offset.is_mandatory(Some(snapshot_id.version)));

    let epoch = snapshot_id_fields["epoch"];
    assert_eq!(
        VersionRange {
            start: 0,
            end: i16::MAX
        },
        epoch.version
    );
    assert!(epoch.is_mandatory(Some(snapshot_id.version)));
}

#[test]
fn node_endpoints() {
    let meta = BTreeMap::from(MESSAGE_META);
    let message = meta.get(FETCH_RESPONSE).unwrap();
    let message_fields = BTreeMap::from_iter(message.fields.iter().copied());

    let node_endpoints = message_fields["node_endpoints"];
    assert_eq!(
        VersionRange {
            start: 16,
            end: i16::MAX
        },
        node_endpoints.version
    );
}
