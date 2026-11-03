// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

//! The `operation` attribute that storage metrics record.

use std::fmt::{self, Display, Formatter};

use opentelemetry::KeyValue;

/// A [`Storage`](crate::Storage) method, as the `operation` attribute of a storage metric names
/// it.
///
/// Each variant names the method of the same name, and [`StorageOperation::as_str`] returns that
/// method's name. Dashboards and alerts select on these values, so a renamed value is a breaking
/// change for them.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StorageOperation {
    RegisterBroker,
    CreateTopic,
    IncrementalAlterResource,
    DeleteRecords,
    DeleteTopic,
    Brokers,
    Produce,
    Fetch,
    OffsetStage,
    CurrentOffsetStage,
    ListOffsets,
    OffsetCommit,
    OffsetFetch,
    CommittedOffsetTopitions,
    PendingTxnOffsets,
    Metadata,
    UpsertUserScramCredential,
    DeleteUserScramCredential,
    UserScramCredential,
    DescribeConfig,
    ListGroups,
    DeleteGroups,
    DescribeGroups,
    DescribeTopicPartitions,
    GroupWithVersion,
    UpdateGroup,
    InitProducer,
    TxnAddOffsets,
    TxnAddPartitions,
    TxnOffsetCommit,
    TxnEnd,
    Maintain,
    MaintainTransactions,
    AbortedTransactions,
    ClusterId,
    Node,
    AdvertisedListener,
    Ping,
}

impl StorageOperation {
    /// Returns the name of the [`Storage`](crate::Storage) method that this operation names.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RegisterBroker => "register_broker",
            Self::CreateTopic => "create_topic",
            Self::IncrementalAlterResource => "incremental_alter_resource",
            Self::DeleteRecords => "delete_records",
            Self::DeleteTopic => "delete_topic",
            Self::Brokers => "brokers",
            Self::Produce => "produce",
            Self::Fetch => "fetch",
            Self::OffsetStage => "offset_stage",
            Self::CurrentOffsetStage => "current_offset_stage",
            Self::ListOffsets => "list_offsets",
            Self::OffsetCommit => "offset_commit",
            Self::OffsetFetch => "offset_fetch",
            Self::CommittedOffsetTopitions => "committed_offset_topitions",
            Self::PendingTxnOffsets => "pending_txn_offsets",
            Self::Metadata => "metadata",
            Self::UpsertUserScramCredential => "upsert_user_scram_credential",
            Self::DeleteUserScramCredential => "delete_user_scram_credential",
            Self::UserScramCredential => "user_scram_credential",
            Self::DescribeConfig => "describe_config",
            Self::ListGroups => "list_groups",
            Self::DeleteGroups => "delete_groups",
            Self::DescribeGroups => "describe_groups",
            Self::DescribeTopicPartitions => "describe_topic_partitions",
            Self::GroupWithVersion => "group_with_version",
            Self::UpdateGroup => "update_group",
            Self::InitProducer => "init_producer",
            Self::TxnAddOffsets => "txn_add_offsets",
            Self::TxnAddPartitions => "txn_add_partitions",
            Self::TxnOffsetCommit => "txn_offset_commit",
            Self::TxnEnd => "txn_end",
            Self::Maintain => "maintain",
            Self::MaintainTransactions => "maintain_transactions",
            Self::AbortedTransactions => "aborted_transactions",
            Self::ClusterId => "cluster_id",
            Self::Node => "node",
            Self::AdvertisedListener => "advertised_listener",
            Self::Ping => "ping",
        }
    }

    /// Returns this operation as the `operation` attribute of a metric.
    pub fn attribute(self) -> KeyValue {
        KeyValue::new("operation", self.as_str())
    }
}

impl Display for StorageOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins each label value, because dashboards and alerts select on it.
    #[test]
    fn as_str_is_the_label_value() {
        let expected = [
            (StorageOperation::RegisterBroker, "register_broker"),
            (StorageOperation::CreateTopic, "create_topic"),
            (
                StorageOperation::IncrementalAlterResource,
                "incremental_alter_resource",
            ),
            (StorageOperation::DeleteRecords, "delete_records"),
            (StorageOperation::DeleteTopic, "delete_topic"),
            (StorageOperation::Brokers, "brokers"),
            (StorageOperation::Produce, "produce"),
            (StorageOperation::Fetch, "fetch"),
            (StorageOperation::OffsetStage, "offset_stage"),
            (StorageOperation::CurrentOffsetStage, "current_offset_stage"),
            (StorageOperation::ListOffsets, "list_offsets"),
            (StorageOperation::OffsetCommit, "offset_commit"),
            (StorageOperation::OffsetFetch, "offset_fetch"),
            (
                StorageOperation::CommittedOffsetTopitions,
                "committed_offset_topitions",
            ),
            (StorageOperation::PendingTxnOffsets, "pending_txn_offsets"),
            (StorageOperation::Metadata, "metadata"),
            (
                StorageOperation::UpsertUserScramCredential,
                "upsert_user_scram_credential",
            ),
            (
                StorageOperation::DeleteUserScramCredential,
                "delete_user_scram_credential",
            ),
            (
                StorageOperation::UserScramCredential,
                "user_scram_credential",
            ),
            (StorageOperation::DescribeConfig, "describe_config"),
            (StorageOperation::ListGroups, "list_groups"),
            (StorageOperation::DeleteGroups, "delete_groups"),
            (StorageOperation::DescribeGroups, "describe_groups"),
            (
                StorageOperation::DescribeTopicPartitions,
                "describe_topic_partitions",
            ),
            (StorageOperation::GroupWithVersion, "group_with_version"),
            (StorageOperation::UpdateGroup, "update_group"),
            (StorageOperation::InitProducer, "init_producer"),
            (StorageOperation::TxnAddOffsets, "txn_add_offsets"),
            (StorageOperation::TxnAddPartitions, "txn_add_partitions"),
            (StorageOperation::TxnOffsetCommit, "txn_offset_commit"),
            (StorageOperation::TxnEnd, "txn_end"),
            (StorageOperation::Maintain, "maintain"),
            (
                StorageOperation::MaintainTransactions,
                "maintain_transactions",
            ),
            (
                StorageOperation::AbortedTransactions,
                "aborted_transactions",
            ),
            (StorageOperation::ClusterId, "cluster_id"),
            (StorageOperation::Node, "node"),
            (StorageOperation::AdvertisedListener, "advertised_listener"),
            (StorageOperation::Ping, "ping"),
        ];

        for (operation, label) in expected {
            assert_eq!(label, operation.as_str());
        }
    }
}
