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

//! The authorized operations that a describe response reports for a resource: a bitfield with
//! bit `n` set for each permitted operation whose ACL operation code is `n`.
//!
//! The broker has no authorizer, so it reports every operation that the resource supports, as
//! Kafka does without an authorizer ([`AuthHelper.authorizedOperations`]). Kafka reports the
//! operations only when the request asks for them. Which rows carry them differs by resource, and
//! each function says which.
//!
//! [`AuthHelper.authorizedOperations`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/AuthHelper.scala#L62-L76

use nisshi_sans_io::{ErrorCode, acl::Operation};

/// The value that tells a client the response omits the authorized operations. It is the schema
/// default of each `AuthorizedOperations` field. A client reads any other value as a bitfield, so
/// a row must carry this value, not -1, when the operations are not reported.
pub(crate) const OMITTED: i32 = i32::MIN;

/// The operations that a group supports, as Kafka's [`AclEntry.supportedOperations`] lists them.
///
/// [`AclEntry.supportedOperations`]: https://github.com/apache/kafka/blob/3.9.1/server/src/main/java/org/apache/kafka/security/authorizer/AclEntry.java#L150-L167
pub(crate) const GROUP: i32 =
    1 << Operation::Read as i32 | 1 << Operation::Delete as i32 | 1 << Operation::Describe as i32;

/// The operations that the cluster supports, as Kafka's [`AclEntry.supportedOperations`] lists
/// them.
///
/// [`AclEntry.supportedOperations`]: https://github.com/apache/kafka/blob/3.9.1/server/src/main/java/org/apache/kafka/security/authorizer/AclEntry.java#L150-L167
pub(crate) const CLUSTER: i32 = 1 << Operation::Create as i32
    | 1 << Operation::Alter as i32
    | 1 << Operation::Describe as i32
    | 1 << Operation::ClusterAction as i32
    | 1 << Operation::DescribeConfigs as i32
    | 1 << Operation::AlterConfigs as i32
    | 1 << Operation::IdempotentWrite as i32;

/// The operations that a topic supports, as Kafka's [`AclEntry.supportedOperations`] lists them.
///
/// [`AclEntry.supportedOperations`]: https://github.com/apache/kafka/blob/3.9.1/server/src/main/java/org/apache/kafka/security/authorizer/AclEntry.java#L150-L167
pub(crate) const TOPIC: i32 = 1 << Operation::Read as i32
    | 1 << Operation::Write as i32
    | 1 << Operation::Create as i32
    | 1 << Operation::Delete as i32
    | 1 << Operation::Alter as i32
    | 1 << Operation::Describe as i32
    | 1 << Operation::DescribeConfigs as i32
    | 1 << Operation::AlterConfigs as i32;

/// Returns the cluster operations of a Metadata response: [`CLUSTER`] when the request asked for
/// them, and [`OMITTED`] otherwise ([`KafkaApis` Metadata]).
///
/// `include_cluster_authorized_operations` is `Some` only for the versions that carry it, 8 to 10,
/// and a response of any other version has no field to put it in.
///
/// [`KafkaApis` Metadata]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1396-L1406
pub(crate) fn cluster(include_cluster_authorized_operations: Option<bool>) -> i32 {
    if include_cluster_authorized_operations == Some(true) {
        CLUSTER
    } else {
        OMITTED
    }
}

/// Returns the authorized operations of a Metadata topic row: [`TOPIC`] when the request asked
/// for them and the row has a name, and [`OMITTED`] otherwise.
///
/// Unlike a group row, a topic row carries them whatever its error, so an unknown or invalid name
/// gets them too. Kafka sets them on every row that it looked up by name, and it leaves the rows
/// that answer an unknown topic ID, which have no name, at the default
/// ([`KafkaApis` Metadata]).
///
/// [`KafkaApis` Metadata]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1409-L1419
pub(crate) fn topic(include_topic_authorized_operations: Option<bool>, name: Option<&str>) -> i32 {
    if include_topic_authorized_operations == Some(true) && name.is_some() {
        TOPIC
    } else {
        OMITTED
    }
}

/// Returns the authorized operations of a described group row: [`GROUP`] when the request asked
/// for them and the row's `error_code` is NONE, and [`OMITTED`] otherwise ([`KafkaApis`
/// DescribeGroups], [`KafkaApis` ConsumerGroupDescribe]).
///
/// [`KafkaApis` DescribeGroups]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1772-L1781
/// [`KafkaApis` ConsumerGroupDescribe]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L3907-L3916
pub(crate) fn group(include_authorized_operations: bool, error_code: i16) -> i32 {
    if include_authorized_operations && error_code == i16::from(ErrorCode::None) {
        GROUP
    } else {
        OMITTED
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_bitfield_matches_kafka_without_an_authorizer() {
        assert_eq!(328, GROUP);
    }

    #[test]
    fn cluster_bitfield_matches_kafka_without_an_authorizer() {
        assert_eq!(8096, CLUSTER);
    }

    #[test]
    fn topic_bitfield_matches_kafka_without_an_authorizer() {
        assert_eq!(3576, TOPIC);
    }

    #[test]
    fn cluster_operations_reported_only_when_requested() {
        assert_eq!(OMITTED, cluster(None));
        assert_eq!(OMITTED, cluster(Some(false)));
        assert_eq!(CLUSTER, cluster(Some(true)));
    }

    #[test]
    fn topic_operations_reported_only_when_requested_for_a_named_row() {
        for include in [None, Some(false)] {
            assert_eq!(OMITTED, topic(include, Some("orders")));
        }

        assert_eq!(TOPIC, topic(Some(true), Some("orders")));
        assert_eq!(OMITTED, topic(Some(true), None));
    }

    #[test]
    fn group_operations_omitted_unless_requested() {
        assert_eq!(i32::MIN, group(false, ErrorCode::None.into()));
    }

    #[test]
    fn group_operations_reported_when_requested() {
        assert_eq!(328, group(true, ErrorCode::None.into()));
    }

    #[test]
    fn error_row_omits_group_operations() {
        for error_code in [
            ErrorCode::GroupIdNotFound,
            ErrorCode::CoordinatorNotAvailable,
            ErrorCode::UnknownServerError,
        ] {
            assert_eq!(i32::MIN, group(true, error_code.into()));
            assert_eq!(i32::MIN, group(false, error_code.into()));
        }
    }

    #[test]
    fn group_conversions_omit_authorized_operations() {
        use nisshi_sans_io::{consumer_group_describe_response, describe_groups_response};

        use crate::{GroupDetail, NamedGroupDetail};

        let rows = [
            NamedGroupDetail::found("found".into(), GroupDetail::default()),
            NamedGroupDetail::error_code("error".into(), ErrorCode::GroupIdNotFound),
        ];

        for row in &rows {
            assert_eq!(
                Some(i32::MIN),
                describe_groups_response::DescribedGroup::from(row).authorized_operations
            );
            assert_eq!(
                i32::MIN,
                consumer_group_describe_response::DescribedGroup::from(row).authorized_operations
            );
        }
    }
}
