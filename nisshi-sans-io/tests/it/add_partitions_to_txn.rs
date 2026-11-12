// Copyright (c) Solace Corporation
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

#[test]
fn check_message_meta() {
    let _guard = init_tracing().unwrap();

    assert!(BTreeMap::from(MESSAGE_META).contains_key("AddPartitionsToTxnRequest"));

    let meta = BTreeMap::from(MESSAGE_META);

    let message = meta.get("AddPartitionsToTxnRequest").unwrap();
    assert_eq!(24, message.api_key);
    assert_eq!(MessageKind::Request, message.message_kind);
    assert_eq!(VersionRange { start: 0, end: 5 }, message.version.valid);
    assert_eq!(
        VersionRange {
            start: 3,
            end: i16::MAX
        },
        message.version.flexible
    );

    let fields = BTreeMap::from_iter(message.fields.iter().copied());
    assert!(fields.contains_key("transactions"));

    assert_eq!(
        VersionRange {
            start: 4,
            end: i16::MAX
        },
        fields["transactions"].version
    );

    let transactions = BTreeMap::from_iter(fields["transactions"].fields.iter().copied());
    assert!(transactions.contains_key("transactional_id"));
    assert_eq!(0, transactions["transactional_id"].fields.len());

    assert!(transactions.contains_key("producer_id"));
    assert_eq!(0, transactions["producer_id"].fields.len());

    assert!(transactions.contains_key("producer_epoch"));
    assert_eq!(0, transactions["producer_epoch"].fields.len());

    assert!(transactions.contains_key("verify_only"));
    assert_eq!(0, transactions["verify_only"].fields.len());

    assert!(transactions.contains_key("topics"));
    assert_eq!(2, transactions["topics"].fields.len());

    let topics = BTreeMap::from_iter(transactions["topics"].fields.iter().copied());
    assert!(topics.contains_key("name"));
    assert!(topics.contains_key("partitions"));
}
