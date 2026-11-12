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

use nisshi_sans_io::topic::{has_collision_chars, is_valid_topic_name, unify_collision_chars};

#[test]
fn accepts_legal_names() {
    assert!(is_valid_topic_name("a"));
    assert!(is_valid_topic_name(&"a".repeat(249)));
    assert!(is_valid_topic_name("a.b_c-d"));

    // Kafka rejects a name only when it is exactly "." or ".."; three or
    // more dots is a legal name.
    assert!(is_valid_topic_name("..."));
}

#[test]
fn rejects_illegal_names() {
    assert!(!is_valid_topic_name(""));
    assert!(!is_valid_topic_name("."));
    assert!(!is_valid_topic_name(".."));
    assert!(!is_valid_topic_name(&"a".repeat(250)));
    assert!(!is_valid_topic_name("a/b"));
    assert!(!is_valid_topic_name("a b"));

    // The rule checks bytes, not chars, so every multi-byte character is
    // rejected; swapping the check to `.chars()` would silently widen it.
    assert!(!is_valid_topic_name("é"));
}

#[test]
fn collision_chars_are_period_and_underscore() {
    assert!(has_collision_chars("a.b"));
    assert!(has_collision_chars("a_b"));
    assert!(!has_collision_chars("a-b"));
    assert!(!has_collision_chars("ab"));
}

#[test]
fn unify_collision_chars_maps_period_to_underscore() {
    assert_eq!("a_b_c", unify_collision_chars("a.b_c"));
    assert_eq!(unify_collision_chars("a_b"), unify_collision_chars("a.b"));
    assert_eq!("a-b", unify_collision_chars("a-b"));
    assert_eq!("ab", unify_collision_chars("ab"));
}
