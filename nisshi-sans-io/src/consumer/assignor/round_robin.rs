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

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use tracing::instrument;

use crate::{
    Error, MetadataResponse,
    consumer::{
        ConsumerAssignor, ConsumerProtocolAssignment, MemberAssignment, MemberMetadata,
        TopicPartition,
    },
    join_group_response::JoinGroupResponseMember,
    sync_group_request::SyncGroupRequestAssignment,
};

use super::Topition;

#[derive(Clone, Default, Eq, Hash, Debug, Ord, PartialEq, PartialOrd)]
pub struct RoundRobinAssignor;

impl ConsumerAssignor for RoundRobinAssignor {
    #[instrument(skip(members, metadata))]
    fn assign(
        &self,
        members: &[JoinGroupResponseMember],
        metadata: &MetadataResponse,
    ) -> Result<Vec<SyncGroupRequestAssignment>, Error> {
        let mut sorted: Vec<&JoinGroupResponseMember> = members.iter().collect();
        sorted.sort_by_key(|m| m.member_id.as_str());

        let member_subs: Vec<BTreeSet<String>> = sorted
            .iter()
            .map(|m| {
                MemberMetadata::try_from(m.metadata.clone())
                    .map(|mm| mm.subscription.topics.into_iter().collect())
            })
            .collect::<Result<_, _>>()?;

        let n = sorted.len();
        let mut assignments: Vec<Vec<Topition>> = vec![vec![]; n];
        let mut counter: usize = 0;

        'outer: for topition in BTreeSet::<Topition>::from(metadata) {
            for _ in 0..n {
                let idx = counter % n;
                counter += 1;
                if member_subs[idx].contains(topition.topic.as_str()) {
                    assignments[idx].push(topition);
                    continue 'outer;
                }
            }
        }

        sorted
            .iter()
            .zip(assignments)
            .map(|(member, topitions)| {
                let mut topic_map: BTreeMap<String, Vec<i32>> = BTreeMap::new();
                for t in topitions {
                    topic_map.entry(t.topic).or_default().push(t.partition);
                }
                let topic_partitions: Vec<TopicPartition> = topic_map
                    .into_iter()
                    .map(|(topic, partitions)| TopicPartition { topic, partitions })
                    .collect();

                Bytes::try_from(
                    &MemberAssignment::default().version(3).assignment(
                        ConsumerProtocolAssignment::default()
                            .assigned_partitions(topic_partitions.into_iter()),
                    ),
                )
                .map(|assignment| {
                    SyncGroupRequestAssignment::default()
                        .member_id(member.member_id.clone())
                        .assignment(assignment)
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use bytes::Bytes;

    use crate::{
        Error, MetadataResponse,
        consumer::{
            ConsumerProtocolSubscription, MemberAssignment, MemberMetadata, TopicPartition,
        },
        join_group_response::JoinGroupResponseMember,
        metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
    };

    use super::*;

    fn metadata_response_topic(
        topic: impl Into<String>,
        partitions: Range<i32>,
    ) -> MetadataResponseTopic {
        MetadataResponseTopic::default()
            .name(Some(topic.into()))
            .partitions(Some(
                partitions
                    .into_iter()
                    .map(|p| MetadataResponsePartition::default().partition_index(p))
                    .collect(),
            ))
    }

    fn member_metadata(topics: &[&str]) -> Result<Bytes, Error> {
        Bytes::try_from(&MemberMetadata::default().version(3).subscription(
            ConsumerProtocolSubscription::default().topics(topics.iter().map(|t| t.to_string())),
        ))
    }

    // 3 members, 1 topic, 3 partitions → one partition per member
    #[test]
    fn even_single_topic() -> Result<(), Error> {
        const C0: &str = "C0";
        const C1: &str = "C1";
        const C2: &str = "C2";
        const T0: &str = "t0";

        let md = member_metadata(&[T0])?;
        let members = [
            JoinGroupResponseMember::default()
                .member_id(C0.into())
                .metadata(md.clone()),
            JoinGroupResponseMember::default()
                .member_id(C1.into())
                .metadata(md.clone()),
            JoinGroupResponseMember::default()
                .member_id(C2.into())
                .metadata(md.clone()),
        ];

        let metadata =
            MetadataResponse::default().topics(Some([metadata_response_topic(T0, 0..3)].into()));

        let assignments = RoundRobinAssignor.assign(&members, &metadata)?;
        assert_eq!(3, assignments.len());

        for (i, id) in [C0, C1, C2].into_iter().enumerate() {
            assert_eq!(id, assignments[i].member_id);
            assert_eq!(
                vec![TopicPartition {
                    topic: T0.into(),
                    partitions: vec![i as i32],
                }],
                MemberAssignment::try_from(assignments[i].clone().assignment)?
                    .assignment
                    .assigned_partitions
            );
        }

        Ok(())
    }

    // 3 members, 2 topics × 3 partitions → each member gets one partition from each topic
    #[test]
    fn even_two_topics() -> Result<(), Error> {
        const C0: &str = "C0";
        const C1: &str = "C1";
        const C2: &str = "C2";
        const T0: &str = "t0";
        const T1: &str = "t1";

        let md = member_metadata(&[T0, T1])?;
        let members = [
            JoinGroupResponseMember::default()
                .member_id(C0.into())
                .metadata(md.clone()),
            JoinGroupResponseMember::default()
                .member_id(C1.into())
                .metadata(md.clone()),
            JoinGroupResponseMember::default()
                .member_id(C2.into())
                .metadata(md.clone()),
        ];

        let metadata = MetadataResponse::default().topics(Some(
            [
                metadata_response_topic(T0, 0..3),
                metadata_response_topic(T1, 0..3),
            ]
            .into(),
        ));

        let assignments = RoundRobinAssignor.assign(&members, &metadata)?;
        assert_eq!(3, assignments.len());

        for (i, id) in [C0, C1, C2].into_iter().enumerate() {
            assert_eq!(id, assignments[i].member_id);
            assert_eq!(
                [T0, T1]
                    .into_iter()
                    .map(|topic| TopicPartition {
                        topic: topic.into(),
                        partitions: vec![i as i32],
                    })
                    .collect::<Vec<_>>(),
                MemberAssignment::try_from(assignments[i].clone().assignment)?
                    .assignment
                    .assigned_partitions
            );
        }

        Ok(())
    }

    // 2 members, 1 topic, 3 partitions → C0 gets [0,2], C1 gets [1]
    #[test]
    fn uneven_single_topic() -> Result<(), Error> {
        const C0: &str = "C0";
        const C1: &str = "C1";
        const T0: &str = "t0";

        let md = member_metadata(&[T0])?;
        let members = [
            JoinGroupResponseMember::default()
                .member_id(C0.into())
                .metadata(md.clone()),
            JoinGroupResponseMember::default()
                .member_id(C1.into())
                .metadata(md.clone()),
        ];

        let metadata =
            MetadataResponse::default().topics(Some([metadata_response_topic(T0, 0..3)].into()));

        let assignments = RoundRobinAssignor.assign(&members, &metadata)?;
        assert_eq!(2, assignments.len());

        assert_eq!(C0, assignments[0].member_id);
        assert_eq!(
            vec![TopicPartition {
                topic: T0.into(),
                partitions: vec![0, 2],
            }],
            MemberAssignment::try_from(assignments[0].clone().assignment)?
                .assignment
                .assigned_partitions
        );

        assert_eq!(C1, assignments[1].member_id);
        assert_eq!(
            vec![TopicPartition {
                topic: T0.into(),
                partitions: vec![1],
            }],
            MemberAssignment::try_from(assignments[1].clone().assignment)?
                .assignment
                .assigned_partitions
        );

        Ok(())
    }

    // 2 members, 2 topics × 3 partitions → partitions interleaved across topics
    #[test]
    fn interleaved_two_topics() -> Result<(), Error> {
        const C0: &str = "C0";
        const C1: &str = "C1";
        const T0: &str = "t0";
        const T1: &str = "t1";

        let md = member_metadata(&[T0, T1])?;
        let members = [
            JoinGroupResponseMember::default()
                .member_id(C0.into())
                .metadata(md.clone()),
            JoinGroupResponseMember::default()
                .member_id(C1.into())
                .metadata(md.clone()),
        ];

        let metadata = MetadataResponse::default().topics(Some(
            [
                metadata_response_topic(T0, 0..3),
                metadata_response_topic(T1, 0..3),
            ]
            .into(),
        ));

        let assignments = RoundRobinAssignor.assign(&members, &metadata)?;
        assert_eq!(2, assignments.len());

        // t0:[0,2] t1:[1]
        assert_eq!(C0, assignments[0].member_id);
        assert_eq!(
            vec![
                TopicPartition {
                    topic: T0.into(),
                    partitions: vec![0, 2],
                },
                TopicPartition {
                    topic: T1.into(),
                    partitions: vec![1],
                },
            ],
            MemberAssignment::try_from(assignments[0].clone().assignment)?
                .assignment
                .assigned_partitions
        );

        // t0:[1] t1:[0,2]
        assert_eq!(C1, assignments[1].member_id);
        assert_eq!(
            vec![
                TopicPartition {
                    topic: T0.into(),
                    partitions: vec![1],
                },
                TopicPartition {
                    topic: T1.into(),
                    partitions: vec![0, 2],
                },
            ],
            MemberAssignment::try_from(assignments[1].clone().assignment)?
                .assignment
                .assigned_partitions
        );

        Ok(())
    }

    // C0 subscribes to [t0, t1], C1 only to [t1]; t0 2p + t1 2p
    // t0 partitions skip C1; C0 ends up with t0:[0,1] t1:[1], C1 gets t1:[0]
    #[test]
    fn different_subscriptions() -> Result<(), Error> {
        const C0: &str = "C0";
        const C1: &str = "C1";
        const T0: &str = "t0";
        const T1: &str = "t1";

        let md_both = member_metadata(&[T0, T1])?;
        let md_t1 = member_metadata(&[T1])?;
        let members = [
            JoinGroupResponseMember::default()
                .member_id(C0.into())
                .metadata(md_both),
            JoinGroupResponseMember::default()
                .member_id(C1.into())
                .metadata(md_t1),
        ];

        let metadata = MetadataResponse::default().topics(Some(
            [
                metadata_response_topic(T0, 0..2),
                metadata_response_topic(T1, 0..2),
            ]
            .into(),
        ));

        let assignments = RoundRobinAssignor.assign(&members, &metadata)?;
        assert_eq!(2, assignments.len());

        // C0: t0:[0,1] t1:[1]  (t0 skips C1 both times; t1:0 lands on C1, t1:1 lands on C0)
        assert_eq!(C0, assignments[0].member_id);
        assert_eq!(
            vec![
                TopicPartition {
                    topic: T0.into(),
                    partitions: vec![0, 1],
                },
                TopicPartition {
                    topic: T1.into(),
                    partitions: vec![1],
                },
            ],
            MemberAssignment::try_from(assignments[0].clone().assignment)?
                .assignment
                .assigned_partitions
        );

        // C1: t1:[0]
        assert_eq!(C1, assignments[1].member_id);
        assert_eq!(
            vec![TopicPartition {
                topic: T1.into(),
                partitions: vec![0],
            }],
            MemberAssignment::try_from(assignments[1].clone().assignment)?
                .assignment
                .assigned_partitions
        );

        Ok(())
    }
}
