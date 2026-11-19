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

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use nisshi_sans_io::{ErrorCode, NULL_TOPIC_ID, metadata_response::MetadataResponseTopic};

use crate::{MetadataResponse, TopicId};

/// The partitions of each topic that a Metadata read last answered, and the brokers of the last
/// read. Clones share one store, so a broker shares it across its connections.
///
/// A Kafka 3.9.1 broker answers Metadata from its metadata image, so a topic that it knows
/// keeps its partitions when their leader is down. It answers each such partition with
/// `LEADER_NOT_AVAILABLE`, leader -1, its leader epoch, and its replicas and ISR
/// ([`KRaftMetadataCache.getPartitionMetadata`]), and the topic with `NONE`
/// ([`KRaftMetadataCache.getTopicMetadata`]). A stateless broker has no image. So when a read of
/// the storage fails with an error that a retry may clear, the Metadata handler answers each
/// topic from this store in that shape, and clients wait for a leader as they do during a
/// Kafka leader election.
///
/// An answer of a topic-level error without partitions breaks clients instead. librdkafka before
/// 2.10.0 takes it as a permanent error, and fails the queued messages and every new produce of
/// the topic ([`rd_kafka_topic_metadata_update`]).
///
/// A row that answers the topic as unknown removes the topic, and a read of every topic keeps
/// only the topics that it answers. So the store holds at most the topics that existed when the
/// broker last read them.
///
/// [`KRaftMetadataCache.getPartitionMetadata`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/metadata/KRaftMetadataCache.scala#L102-L119
/// [`KRaftMetadataCache.getTopicMetadata`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/metadata/KRaftMetadataCache.scala#L245-L260
/// [`rd_kafka_topic_metadata_update`]: https://github.com/confluentinc/librdkafka/blob/v2.3.0/src/rdkafka_topic.c#L1300-L1306
#[derive(Clone, Debug, Default)]
pub struct KnownTopics(Arc<Mutex<Layouts>>);

#[derive(Debug, Default)]
struct Layouts {
    by_name: BTreeMap<String, MetadataResponseTopic>,

    /// The name of each topic in `by_name` that has an ID. Each write of `by_name` keeps it
    /// equal to the IDs of the rows there.
    names_by_id: BTreeMap<[u8; 16], String>,

    /// The last successful read, without its topics.
    brokers: Option<MetadataResponse>,
}

impl KnownTopics {
    /// Records a successful read of `topics`, where `None` reads every topic.
    pub(crate) fn record(&self, topics: Option<&[TopicId]>, response: &MetadataResponse) {
        let brokers = MetadataResponse {
            cluster: response.cluster.clone(),
            controller: response.controller,
            brokers: response.brokers.clone(),
            topics: Vec::new(),
        };

        let known = response
            .topics
            .iter()
            .filter(|row| row.error_code == i16::from(ErrorCode::None) && row.name.is_some())
            .cloned()
            .collect::<Vec<_>>();

        let mut layouts = self.layouts();
        layouts.brokers = Some(brokers);

        if topics.is_none() {
            let listed = known
                .iter()
                .filter_map(|row| row.name.as_deref())
                .collect::<BTreeSet<_>>();

            let Layouts {
                by_name,
                names_by_id,
                ..
            } = &mut *layouts;
            by_name.retain(|name, _| listed.contains(name.as_str()));
            names_by_id.retain(|_, name| by_name.contains_key(name));
        }

        for row in &response.topics {
            if matches!(
                ErrorCode::try_from(row.error_code),
                Ok(ErrorCode::UnknownTopicOrPartition | ErrorCode::UnknownTopicId)
            ) {
                layouts.remove(row);
            }
        }

        for row in known {
            layouts.insert(row);
        }
    }

    /// Returns each of `topics` without a leader, in order, or `None` if any of them is not
    /// known.
    pub(crate) fn without_leader(&self, topics: &[TopicId]) -> Option<Vec<MetadataResponseTopic>> {
        let rows = {
            let layouts = self.layouts();

            topics
                .iter()
                .map(|topic| layouts.get(topic).cloned())
                .collect::<Option<Vec<_>>>()?
        };

        Some(rows.into_iter().map(without_leader).collect())
    }

    /// Returns the brokers, cluster ID and controller of the last successful read, without
    /// topics.
    pub(crate) fn brokers(&self) -> Option<MetadataResponse> {
        self.layouts().brokers.clone()
    }

    fn layouts(&self) -> MutexGuard<'_, Layouts> {
        // Each write leaves both maps valid, so a panic while it was held leaves nothing to undo.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Layouts {
    /// Inserts `row`, which has a name.
    fn insert(&mut self, row: MetadataResponseTopic) {
        let Some(name) = row.name.clone() else {
            return;
        };

        let id = row.topic_id.filter(|id| *id != NULL_TOPIC_ID);

        if let Some(previous) = self.by_name.insert(name.clone(), row)
            && let Some(previous_id) = previous.topic_id
            && Some(previous_id) != id
        {
            _ = self.names_by_id.remove(&previous_id);
        }

        if let Some(id) = id {
            _ = self.names_by_id.insert(id, name);
        }
    }

    fn remove(&mut self, row: &MetadataResponseTopic) {
        if let Some(removed) = row.name.as_ref().and_then(|name| self.by_name.remove(name))
            && let Some(id) = removed.topic_id
        {
            _ = self.names_by_id.remove(&id);
        }

        if let Some(id) = row.topic_id.filter(|id| *id != NULL_TOPIC_ID)
            && let Some(name) = self.names_by_id.remove(&id)
        {
            _ = self.by_name.remove(&name);
        }
    }

    fn get(&self, topic: &TopicId) -> Option<&MetadataResponseTopic> {
        match topic {
            TopicId::Name(name) => self.by_name.get(name),
            TopicId::Id(id) => self
                .names_by_id
                .get(id.as_bytes())
                .and_then(|name| self.by_name.get(name)),
        }
    }
}

/// Returns `row` with the topic code `NONE` and each partition without a leader, keeping its
/// leader epoch, replicas and ISR. The leader epoch stays, because a client discards metadata
/// whose epoch is older than one it has seen. Kafka lists a replica on a fenced broker as
/// offline. The broker that answers is alive, so this lists no offline replica.
fn without_leader(row: MetadataResponseTopic) -> MetadataResponseTopic {
    let partitions = row.partitions.clone().map(|partitions| {
        partitions
            .into_iter()
            .map(|partition| {
                partition
                    .error_code(ErrorCode::LeaderNotAvailable.into())
                    .leader_id(-1)
                    .offline_replicas(Some(vec![]))
            })
            .collect()
    });

    row.error_code(ErrorCode::None.into())
        .partitions(partitions)
}

#[cfg(test)]
mod tests {
    use nisshi_sans_io::{
        ErrorCode, NULL_TOPIC_ID,
        metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
    };
    use uuid::Uuid;

    use super::KnownTopics;
    use crate::{MetadataResponse, TopicId};

    fn existing(name: &str, id: Uuid) -> MetadataResponseTopic {
        MetadataResponseTopic::default()
            .error_code(ErrorCode::None.into())
            .name(Some(name.into()))
            .topic_id(Some(id.into_bytes()))
            .partitions(Some(vec![
                MetadataResponsePartition::default()
                    .partition_index(0)
                    .leader_id(111)
                    .leader_epoch(Some(4))
                    .replica_nodes(Some(vec![111]))
                    .isr_nodes(Some(vec![111])),
            ]))
    }

    /// A read that answers `rows`.
    fn read(rows: &[MetadataResponseTopic]) -> MetadataResponse {
        MetadataResponse {
            topics: rows.to_vec(),
            ..MetadataResponse::default()
        }
    }

    fn unknown(name: &str) -> MetadataResponseTopic {
        MetadataResponseTopic::default()
            .error_code(ErrorCode::UnknownTopicOrPartition.into())
            .name(Some(name.into()))
            .topic_id(Some(NULL_TOPIC_ID))
    }

    #[test]
    fn a_topic_is_known_by_name_and_by_id() {
        let known = KnownTopics::default();
        let id = Uuid::new_v4();

        known.record(Some(&["a".into()]), &read(&[existing("a", id)]));

        let by_name = known.without_leader(&["a".into()]);
        let by_id = known.without_leader(&[TopicId::Id(id)]);

        assert!(by_name.is_some());
        assert_eq!(by_name, by_id);
    }

    #[test]
    fn a_topic_answered_as_unknown_is_forgotten() {
        let known = KnownTopics::default();
        let id = Uuid::new_v4();

        known.record(Some(&["a".into()]), &read(&[existing("a", id)]));
        known.record(Some(&["a".into()]), &read(&[unknown("a")]));

        assert_eq!(None, known.without_leader(&["a".into()]));
        assert_eq!(None, known.without_leader(&[TopicId::Id(id)]));
    }

    /// A row of an unknown ID has no name, so the topic is found through its ID.
    #[test]
    fn a_topic_id_answered_as_unknown_is_forgotten() {
        let known = KnownTopics::default();
        let id = Uuid::new_v4();

        known.record(Some(&["a".into()]), &read(&[existing("a", id)]));
        known.record(
            Some(&[TopicId::Id(id)]),
            &read(&[MetadataResponseTopic::default()
                .error_code(ErrorCode::UnknownTopicId.into())
                .name(None)
                .topic_id(Some(id.into_bytes()))]),
        );

        assert_eq!(None, known.without_leader(&["a".into()]));
    }

    #[test]
    fn a_read_of_every_topic_forgets_the_topics_it_does_not_list() {
        let known = KnownTopics::default();
        let deleted = Uuid::new_v4();

        known.record(
            Some(&["a".into(), "b".into()]),
            &read(&[existing("a", Uuid::new_v4()), existing("b", deleted)]),
        );
        known.record(None, &read(&[existing("a", Uuid::new_v4())]));

        assert!(known.without_leader(&["a".into()]).is_some());
        assert_eq!(None, known.without_leader(&["b".into()]));
        assert_eq!(None, known.without_leader(&[TopicId::Id(deleted)]));
    }

    /// A topic created again under its name gets a new ID, and the old ID no longer finds it.
    #[test]
    fn a_recreated_topic_drops_its_old_id() {
        let known = KnownTopics::default();
        let old = Uuid::new_v4();
        let new = Uuid::new_v4();

        known.record(Some(&["a".into()]), &read(&[existing("a", old)]));
        known.record(Some(&["a".into()]), &read(&[existing("a", new)]));

        assert_eq!(None, known.without_leader(&[TopicId::Id(old)]));
        assert!(known.without_leader(&[TopicId::Id(new)]).is_some());
    }

    #[test]
    fn one_unknown_topic_answers_none() {
        let known = KnownTopics::default();

        known.record(Some(&["a".into()]), &read(&[existing("a", Uuid::new_v4())]));

        assert_eq!(None, known.without_leader(&["a".into(), "b".into()]));
    }
}
