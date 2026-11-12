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

//! Rules that Apache Kafka applies to a topic.

/// Returns whether `name` is a legal Kafka topic name: 1 to 249 characters
/// from `[a-zA-Z0-9._-]`, and not `.` or `..`.
pub fn is_valid_topic_name(name: &str) -> bool {
    !name.is_empty()
        && name != "."
        && name != ".."
        && name.len() <= 249
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
}

/// Returns whether `name` has a collision character: a period or an underscore.
///
/// Kafka maps '.' to '_' in per-topic metric names, so two topic names that differ only in these
/// characters share their metrics ([`Topic.hasCollisionChars`]).
///
/// [`Topic.hasCollisionChars`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/internals/Topic.java#L81-L89
pub fn has_collision_chars(name: &str) -> bool {
    name.contains(['.', '_'])
}

/// Returns `name` with its collision characters unified: each '.' replaced with '_'.
///
/// Two names collide when their unified names are equal ([`Topic.unifyCollisionChars`]). The
/// unified name is only for that comparison, and never names a topic.
///
/// [`Topic.unifyCollisionChars`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/internals/Topic.java#L91-L100
pub fn unify_collision_chars(name: &str) -> String {
    name.replace('.', "_")
}
