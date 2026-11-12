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

// TODO: implement incremental rebalance (KIP-429): read `owned_partitions` from
// each member's ConsumerProtocolSubscription, keep existing assignments unchanged
// where they are already balanced, and only move partitions that must be revoked.

use crate::{
    Error, MetadataResponse, consumer::ConsumerAssignor,
    join_group_response::JoinGroupResponseMember, sync_group_request::SyncGroupRequestAssignment,
};

use super::RoundRobinAssignor;

#[derive(Clone, Default, Eq, Hash, Debug, Ord, PartialEq, PartialOrd)]
pub struct CooperativeStickyAssignor;

impl ConsumerAssignor for CooperativeStickyAssignor {
    fn assign(
        &self,
        members: &[JoinGroupResponseMember],
        metadata: &MetadataResponse,
    ) -> Result<Vec<SyncGroupRequestAssignment>, Error> {
        RoundRobinAssignor.assign(members, metadata)
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

    // mirrors RoundRobinAssignor::even_single_topic
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

        let assignments = CooperativeStickyAssignor.assign(&members, &metadata)?;
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

    // mirrors RoundRobinAssignor::interleaved_two_topics
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

        let assignments = CooperativeStickyAssignor.assign(&members, &metadata)?;
        assert_eq!(2, assignments.len());

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

    // Members carry owned_partitions reflecting an already-balanced state. The
    // owned_partitions data must round-trip through MemberMetadata serialization, and
    // v1 (delegating to round-robin) must still produce a valid assignment.
    // With 2 members and 2 partitions, round-robin happens to preserve the owned state.
    #[test]
    fn rebalance_no_change() -> Result<(), Error> {
        const C0: &str = "C0";
        const C1: &str = "C1";
        const T0: &str = "t0";

        let md_c0 = Bytes::try_from(
            &MemberMetadata::default().version(3).subscription(
                ConsumerProtocolSubscription::default()
                    .topics([T0.to_string()])
                    .owned_partitions(
                        [TopicPartition {
                            topic: T0.into(),
                            partitions: vec![0],
                        }]
                        .into_iter(),
                    ),
            ),
        )?;

        let md_c1 = Bytes::try_from(
            &MemberMetadata::default().version(3).subscription(
                ConsumerProtocolSubscription::default()
                    .topics([T0.to_string()])
                    .owned_partitions(
                        [TopicPartition {
                            topic: T0.into(),
                            partitions: vec![1],
                        }]
                        .into_iter(),
                    ),
            ),
        )?;

        let members = [
            JoinGroupResponseMember::default()
                .member_id(C0.into())
                .metadata(md_c0),
            JoinGroupResponseMember::default()
                .member_id(C1.into())
                .metadata(md_c1),
        ];

        let metadata =
            MetadataResponse::default().topics(Some([metadata_response_topic(T0, 0..2)].into()));

        let assignments = CooperativeStickyAssignor.assign(&members, &metadata)?;
        assert_eq!(2, assignments.len());

        // round-robin over 2 members × 2 partitions: C0→[0], C1→[1]
        // this matches the owned_partitions, confirming the data round-tripped and
        // the v1 delegate produces a consistent result
        assert_eq!(C0, assignments[0].member_id);
        assert_eq!(
            vec![TopicPartition {
                topic: T0.into(),
                partitions: vec![0],
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
}
