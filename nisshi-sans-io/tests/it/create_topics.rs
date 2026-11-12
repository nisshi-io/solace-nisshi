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
use nisshi_sans_io::MESSAGE_META;
use std::collections::BTreeMap;

#[test]
fn check_message_meta() {
    let _guard = init_tracing().unwrap();

    assert!(BTreeMap::from(MESSAGE_META).contains_key("CreateTopicsRequest"));

    let meta = BTreeMap::from(MESSAGE_META);

    let message = meta.get("CreateTopicsRequest").unwrap();
    assert_eq!(19, message.api_key);
    assert_eq!(MessageKind::Request, message.message_kind);

    let structures = message.structures();
    let mut keys: Vec<_> = structures.into_iter().map(|(name, _)| name).collect();
    keys.sort();

    assert_eq!(
        vec![
            "CreatableReplicaAssignment",
            "CreatableTopic",
            "CreatableTopicConfig"
        ],
        keys
    );

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
            .field("topics")
            .map(|field| field.kind.is_primitive())
            .unwrap()
    );

    assert_eq!(
        "CreatableTopic",
        message
            .field("topics")
            .and_then(|field| field.kind.kind_of_sequence())
            .map(|kind_meta| kind_meta.0)
            .unwrap()
    );

    assert!(
        !message
            .field("timeout_ms")
            .map(|field| field.kind.is_sequence())
            .unwrap()
    );

    assert!(
        !message
            .field("timeout_ms")
            .map(|field| field.is_structure())
            .unwrap()
    );

    assert!(
        !message
            .field("validate_only")
            .map(|field| field.kind.is_sequence())
            .unwrap()
    );

    assert!(
        !message
            .field("validate_only")
            .map(|field| field.is_structure())
            .unwrap()
    );
}
