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

//! The broker settings and limits that apply when a request creates topics, by
//! `CreateTopics` or by `Metadata` auto-creation.

/// The most partitions that one request may create, by `CreateTopics` or by `Metadata`
/// auto-creation.
///
/// Kafka 3.9.1 rejects a whole `CreateTopics` request whose topics add up to more than
/// `MAX_PARTITIONS_PER_BATCH` (10,000) partitions, before any per-topic validation runs,
/// including the name check ([`ReplicationControlManager.validateTotalNumberOfPartitions`]).
///
/// nisshi counts partitions only, so it is looser than Kafka on the number of topics. Kafka
/// 3.9.1 also caps the metadata records that one request produces at 10,000
/// ([`ReplicationControlManager.createTopics`]): one `TopicRecord` per topic, one
/// `ConfigRecord` per config and one `PartitionRecord` per partition. So Kafka creates at
/// most 5,000 single-partition topics in one request, and it rejects a request for exactly
/// 10,000 partitions. nisshi has no metadata records, so it allows 10,000 partitions in any
/// number of topics.
///
/// [`ReplicationControlManager.validateTotalNumberOfPartitions`]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java#L1152-L1169
/// [`ReplicationControlManager.createTopics`]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java#L590-L594
pub const MAX_PARTITIONS_PER_REQUEST: i64 = 10_000;

/// Kafka's default for `num.partitions`: the partitions of a topic that is auto-created, or
/// that `CreateTopics` creates with `num_partitions` of `-1`.
pub const DEFAULT_NUM_PARTITIONS: i32 = 1;

/// The name of Kafka's KRaft metadata log, which no request may create as a topic.
pub(crate) const CLUSTER_METADATA_TOPIC_NAME: &str = "__cluster_metadata";

/// The names of the topics that hold Kafka's coordinator state: committed offsets, transaction
/// state and share group state ([`Topic.INTERNAL_TOPICS`]). This broker keeps that state in its
/// storage engine.
///
/// [`Topic.INTERNAL_TOPICS`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/internals/Topic.java#L39-L40
const INTERNAL_TOPIC_NAMES: [&str; 3] = [
    "__consumer_offsets",
    "__transaction_state",
    "__share_group_state",
];

/// Returns whether `Metadata` may auto-create the topic `name`. If it may not, the unknown name
/// keeps `UNKNOWN_TOPIC_OR_PARTITION`, and no `CreateTopics` runs for it.
///
/// Kafka's broker sends `__cluster_metadata` to its controller, which refuses it, so the topic
/// never exists and each retry of the client gets `UNKNOWN_TOPIC_OR_PARTITION` again
/// ([`AutoTopicCreationManager.sendCreateTopicRequest`]). We skip the `CreateTopics` instead
/// of sending one, because the refusal store remembers only a colliding name, and each retry
/// would then list every topic for the collision check. Kafka auto-creates `__consumer_offsets`
/// and `__transaction_state` with their coordinators' settings, to hold the coordinators'
/// state, and `__share_group_state` with the broker defaults
/// ([`AutoTopicCreationManager.creatableTopic`]). This broker keeps that state in its storage
/// engine, so there is no topic to create.
///
/// [`AutoTopicCreationManager.sendCreateTopicRequest`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/AutoTopicCreationManager.scala#L214-L222
/// [`AutoTopicCreationManager.creatableTopic`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/AutoTopicCreationManager.scala#L232-L250
pub(crate) fn auto_creatable(name: &str) -> bool {
    name != CLUSTER_METADATA_TOPIC_NAME && !INTERNAL_TOPIC_NAMES.contains(&name)
}

/// The broker settings for topic creation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TopicCreationConfig {
    /// Whether `Metadata` creates a topic that it does not know, as Kafka's
    /// `auto.create.topics.enable` sets it. A client can still ask for no creation with
    /// `allow_auto_topic_creation`.
    pub auto_create_topics_enable: bool,

    /// The partitions of a topic that is auto-created, or that `CreateTopics` creates with
    /// `num_partitions` of `-1`, as Kafka's `num.partitions` sets it. It must be at least 1.
    pub num_partitions: i32,
}

impl Default for TopicCreationConfig {
    fn default() -> Self {
        Self {
            auto_create_topics_enable: true,
            num_partitions: DEFAULT_NUM_PARTITIONS,
        }
    }
}
