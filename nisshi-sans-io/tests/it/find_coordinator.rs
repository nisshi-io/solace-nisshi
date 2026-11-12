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

#[test]
fn check_message_meta() {
    let _guard = init_tracing().unwrap();

    assert!(BTreeMap::from(MESSAGE_META).contains_key("FindCoordinatorRequest"));

    let meta = BTreeMap::from(MESSAGE_META);

    let message = meta.get("FindCoordinatorRequest").unwrap();
    assert_eq!(10, message.api_key);
    assert_eq!(MessageKind::Request, message.message_kind);

    assert_eq!(VersionRange { start: 0, end: 6 }, message.version.valid);

    assert_eq!(
        Some(VersionRange { start: 0, end: 3 }),
        message.field("key").map(|field| field.version)
    );

    assert_eq!(
        Some(VersionRange {
            start: 1,
            end: i16::MAX
        }),
        message.field("key_type").map(|field| field.version)
    );
    assert!(
        !message
            .field("key_type")
            .is_some_and(|field| field.is_mandatory(None))
    );

    assert_eq!(
        Some(VersionRange {
            start: 4,
            end: i16::MAX
        }),
        message.field("coordinator_keys").map(|field| field.version)
    );
    assert!(
        !message
            .field("coordinator_keys")
            .is_some_and(|field| field.is_mandatory(None))
    );
}
