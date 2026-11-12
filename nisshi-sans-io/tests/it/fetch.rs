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
fn request() {
    let _guard = init_tracing().unwrap();

    assert!(BTreeMap::from(MESSAGE_META).contains_key("FetchRequest"));

    let meta = BTreeMap::from(MESSAGE_META);

    let message = meta.get("FetchRequest").unwrap();
    assert_eq!(1, message.api_key);
    assert_eq!(MessageKind::Request, message.message_kind);

    let structures = message.structures();
    let mut keys: Vec<_> = structures.into_iter().map(|(name, _)| name).collect();
    keys.sort();

    assert_eq!(
        vec![
            "FetchPartition",
            "FetchTopic",
            "ForgottenTopic",
            "ReplicaState"
        ],
        keys
    );

    assert_eq!(
        "ForgottenTopic",
        message
            .field("forgotten_topics_data")
            .and_then(|field| field.kind.kind_of_sequence())
            .map(|kind_meta| kind_meta.0)
            .unwrap()
    );

    let partitions = message
        .field("forgotten_topics_data")
        .and_then(|forgotten_topic_data| forgotten_topic_data.field("partitions"))
        .unwrap();

    assert!(!partitions.is_structure());
    assert!(partitions.kind.is_sequence());
    assert!(
        partitions
            .kind
            .kind_of_sequence()
            .map(|sk| sk.is_primitive())
            .unwrap()
    );
}
