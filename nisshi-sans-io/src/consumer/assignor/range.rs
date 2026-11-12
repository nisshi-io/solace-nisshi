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

use std::collections::BTreeSet;

use bytes::Bytes;
use tracing::{debug, instrument};

use crate::{
    Error, MetadataResponse,
    consumer::{
        ConsumerAssignor, ConsumerProtocolAssignment, MemberAssignment, MemberMetadata,
        TopicPartition,
    },
    join_group_response::JoinGroupResponseMember,
    sync_group_request::SyncGroupRequestAssignment,
};

#[derive(Clone, Default, Eq, Hash, Debug, Ord, PartialEq, PartialOrd)]
pub struct RangeAssignor;

impl ConsumerAssignor for RangeAssignor {
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
                    .inspect(|mm| debug!(?mm))
                    .map(|mm| mm.subscription.topics.into_iter().collect())
            })
            .collect::<Result<_, _>>()?;

        let n = sorted.len();
        let mut assignments: Vec<Vec<TopicPartition>> = vec![vec![]; n];

        for topic in metadata.topics.as_deref().unwrap_or_default() {
            let topic_name = match topic.name.as_deref() {
                Some(name) => name,
                None => continue,
            };

            // Indices into `sorted` for members subscribed to this topic
            let subscribed: Vec<usize> = (0..n)
                .filter(|&i| member_subs[i].contains(topic_name))
                .collect();

            if subscribed.is_empty() {
                continue;
            }

            // Partitions for this topic, sorted by index
            let partitions: Vec<i32> = topic
                .partitions
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|p| p.partition_index)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();

            let sub_n = subscribed.len();
            let total = partitions.len();
            let allocation = total / sub_n;
            let remainder = total % sub_n;
            let mut offset = 0;

            for (j, &sorted_idx) in subscribed.iter().enumerate() {
                // First `remainder` members get one extra partition
                let count = allocation + usize::from(j < remainder);
                if count == 0 {
                    continue;
                }
                let assigned = partitions[offset..offset + count].to_vec();
                offset += count;

                debug!(
                    member_id = sorted[sorted_idx].member_id,
                    topic_name,
                    ?assigned
                );

                assignments[sorted_idx].push(TopicPartition {
                    topic: topic_name.into(),
                    partitions: assigned,
                });
            }
        }

        sorted
            .iter()
            .zip(assignments)
            .map(|(member, topic_partitions)| {
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
                    .map(|partition| {
                        MetadataResponsePartition::default().partition_index(partition)
                    })
                    .collect(),
            ))
    }

    fn member_metadata(topics: &[&str]) -> Result<Bytes, Error> {
        Bytes::try_from(&MemberMetadata::default().version(3).subscription(
            ConsumerProtocolSubscription::default().topics(topics.iter().map(|t| t.to_string())),
        ))
    }

    // 3 members × 2 topics × 3 partitions → each member gets exactly 1 partition per topic
    #[test]
    fn evenly_divided() -> Result<(), Error> {
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
                .metadata(md),
        ];

        let metadata = MetadataResponse::default().topics(Some(
            [
                metadata_response_topic(T0, 0..3),
                metadata_response_topic(T1, 0..3),
            ]
            .into(),
        ));

        let assignments = RangeAssignor.assign(&members[..], &metadata)?;
        assert_eq!(3, assignments.len());

        for (index, id) in [C0, C1, C2].into_iter().enumerate() {
            assert_eq!(id, assignments[index].member_id);
            assert_eq!(
                [T0, T1]
                    .into_iter()
                    .map(|topic| TopicPartition {
                        topic: topic.into(),
                        partitions: vec![index as i32],
                    })
                    .collect::<Vec<_>>(),
                MemberAssignment::try_from(assignments[index].clone().assignment)?
                    .assignment
                    .assigned_partitions
            );
        }

        Ok(())
    }

    // 2 members × 2 topics × 3 partitions → C0 gets [0,1] per topic, C1 gets [2]
    #[test]
    fn remainder() -> Result<(), Error> {
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
                .metadata(md),
        ];

        let metadata = MetadataResponse::default().topics(Some(
            [
                metadata_response_topic(T0, 0..3),
                metadata_response_topic(T1, 0..3),
            ]
            .into(),
        ));

        let assignments = RangeAssignor.assign(&members[..], &metadata)?;
        assert_eq!(2, assignments.len());

        assert_eq!(C0, assignments[0].member_id);
        assert_eq!(
            [T0, T1]
                .into_iter()
                .map(|topic| TopicPartition {
                    topic: topic.into(),
                    partitions: vec![0, 1],
                })
                .collect::<Vec<_>>(),
            MemberAssignment::try_from(assignments[0].clone().assignment)?
                .assignment
                .assigned_partitions
        );

        assert_eq!(C1, assignments[1].member_id);
        assert_eq!(
            [T0, T1]
                .into_iter()
                .map(|topic| TopicPartition {
                    topic: topic.into(),
                    partitions: vec![2],
                })
                .collect::<Vec<_>>(),
            MemberAssignment::try_from(assignments[1].clone().assignment)?
                .assignment
                .assigned_partitions
        );

        Ok(())
    }

    // 2 members × t0(4 partitions) + t1(2 partitions) → per-topic ranges are independent.
    // The old code collapsed all partition indices into one set {0,1,2,3}, so C1 would have
    // been assigned t1 partitions [2,3] which don't exist.
    #[test]
    fn different_partition_counts() -> Result<(), Error> {
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
                .metadata(md),
        ];

        let metadata = MetadataResponse::default().topics(Some(
            [
                metadata_response_topic(T0, 0..4), // 4 partitions
                metadata_response_topic(T1, 0..2), // 2 partitions
            ]
            .into(),
        ));

        let assignments = RangeAssignor.assign(&members[..], &metadata)?;
        assert_eq!(2, assignments.len());

        // t0: allocation=2, remainder=0 → C0:[0,1], C1:[2,3]
        // t1: allocation=1, remainder=0 → C0:[0],   C1:[1]
        assert_eq!(C0, assignments[0].member_id);
        assert_eq!(
            vec![
                TopicPartition {
                    topic: T0.into(),
                    partitions: vec![0, 1],
                },
                TopicPartition {
                    topic: T1.into(),
                    partitions: vec![0],
                },
            ],
            MemberAssignment::try_from(assignments[0].clone().assignment)?
                .assignment
                .assigned_partitions
        );

        assert_eq!(C1, assignments[1].member_id);
        assert_eq!(
            vec![
                TopicPartition {
                    topic: T0.into(),
                    partitions: vec![2, 3],
                },
                TopicPartition {
                    topic: T1.into(),
                    partitions: vec![1],
                },
            ],
            MemberAssignment::try_from(assignments[1].clone().assignment)?
                .assignment
                .assigned_partitions
        );

        Ok(())
    }

    // C0 subscribes to [t0, t1], C1 subscribes to only [t0].
    // t1 partitions are divided only among t1 subscribers (C0 gets all of them).
    #[test]
    fn partial_subscription() -> Result<(), Error> {
        const C0: &str = "C0";
        const C1: &str = "C1";
        const T0: &str = "t0";
        const T1: &str = "t1";

        let md_both = member_metadata(&[T0, T1])?;
        let md_t0 = member_metadata(&[T0])?;
        let members = [
            JoinGroupResponseMember::default()
                .member_id(C0.into())
                .metadata(md_both),
            JoinGroupResponseMember::default()
                .member_id(C1.into())
                .metadata(md_t0),
        ];

        let metadata = MetadataResponse::default().topics(Some(
            [
                metadata_response_topic(T0, 0..4),
                metadata_response_topic(T1, 0..2),
            ]
            .into(),
        ));

        let assignments = RangeAssignor.assign(&members[..], &metadata)?;
        assert_eq!(2, assignments.len());

        // t0: both members, 4p → C0:[0,1], C1:[2,3]
        // t1: only C0, 2p   → C0:[0,1]
        assert_eq!(C0, assignments[0].member_id);
        assert_eq!(
            vec![
                TopicPartition {
                    topic: T0.into(),
                    partitions: vec![0, 1],
                },
                TopicPartition {
                    topic: T1.into(),
                    partitions: vec![0, 1],
                },
            ],
            MemberAssignment::try_from(assignments[0].clone().assignment)?
                .assignment
                .assigned_partitions
        );

        assert_eq!(C1, assignments[1].member_id);
        assert_eq!(
            vec![TopicPartition {
                topic: T0.into(),
                partitions: vec![2, 3],
            }],
            MemberAssignment::try_from(assignments[1].clone().assignment)?
                .assignment
                .assigned_partitions
        );

        Ok(())
    }
}
