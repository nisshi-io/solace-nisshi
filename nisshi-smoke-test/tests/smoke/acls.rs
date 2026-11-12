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

//! Tests of the broker's ACL APIs, which `kafka-acls` calls. The broker does not have an
//! authorizer, so it must answer as Apache Kafka does without one: it refuses each request to
//! create, delete or list ACLs with `SECURITY_DISABLED`. A user who sees the broker accept an ACL,
//! or list none, believes that ACLs are supported.

use nisshi_smoke_test::{KafkaCli, SECURITY_DISABLED_EXCEPTION, unique};

/// The principal of every ACL in these tests.
const PRINCIPAL: &str = "User:alice";
/// The operation of every ACL in these tests.
const OPERATION: &str = "Read";

/// Adding an ACL must fail with `SecurityDisabledException`, not succeed without effect.
#[test]
fn adding_an_acl_fails_as_security_disabled() {
    let cli = KafkaCli::shared();

    let added = cli
        .acls()
        .allow(PRINCIPAL, OPERATION, &unique::name("topic"));

    assert!(added.mentions(SECURITY_DISABLED_EXCEPTION), "{added}");
    assert_ne!(added.code, Some(0), "{added}");
}

/// Removing one ACL must fail with `SecurityDisabledException`, not as an unsupported API.
#[test]
fn removing_an_acl_fails_as_security_disabled() {
    let cli = KafkaCli::shared();

    let removed = cli
        .acls()
        .remove_allowed(PRINCIPAL, OPERATION, &unique::name("topic"));

    assert!(removed.mentions(SECURITY_DISABLED_EXCEPTION), "{removed}");
    assert_ne!(removed.code, Some(0), "{removed}");
}

/// Removing every ACL on a topic sends a filter that matches any principal and operation. The
/// broker must refuse it with `SecurityDisabledException`, as it does a filter for one ACL.
#[test]
fn removing_every_acl_on_a_topic_fails_as_security_disabled() {
    let cli = KafkaCli::shared();

    let removed = cli.acls().remove_all_on_topic(&unique::name("topic"));

    assert!(removed.mentions(SECURITY_DISABLED_EXCEPTION), "{removed}");
    assert_ne!(removed.code, Some(0), "{removed}");
}

/// Listing ACLs must fail with `SecurityDisabledException`, for a topic and for the whole cluster,
/// not succeed with an empty list.
#[test]
fn listing_acls_fails_as_security_disabled() {
    let cli = KafkaCli::shared();

    let for_topic = cli.acls().list_for_topic(&unique::name("topic"));
    let all = cli.acls().list_all();

    assert!(
        for_topic.mentions(SECURITY_DISABLED_EXCEPTION),
        "{for_topic}"
    );
    assert_ne!(for_topic.code, Some(0), "{for_topic}");
    assert!(all.mentions(SECURITY_DISABLED_EXCEPTION), "{all}");
    assert_ne!(all.code, Some(0), "{all}");
}
