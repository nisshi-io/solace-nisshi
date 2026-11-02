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

use std::{
    assert_matches,
    collections::BTreeMap,
    slice,
    time::{Duration, SystemTime},
};

use crate::common::{
    alphanumeric_string, init_tracing, lite_storage, memory_storage, postgres_storage,
    slate_storage,
};
use bytes::Bytes;
use nisshi_broker::{
    Error, Result,
    coordinator::group::{Coordinator, administrator::Controller},
};
use nisshi_sans_io::{
    DescribeGroupsRequest, DescribeGroupsResponse, ErrorCode, RequestInput,
    create_topics_request::CreatableTopic,
    describe_groups_response::{DescribedGroup, DescribedGroupMember},
    join_group_response::JoinGroupResponseMember,
};
use nisshi_storage::{
    ArcDynStorage, DescribeGroupsService, GroupDetail, GroupMember, GroupRebalance, GroupState,
    OffsetCommitRequest, PendingSync, Storage, Topition,
};
use rama::{Service, extensions::Extensions};
use rand::{RngExt as _, rng};
use uuid::Uuid;

/// The authorized operations that Kafka 3.9.1 answers for a group when the broker has no
/// authorizer: the group's supported operations READ (3), DELETE (6) and DESCRIBE (8), as the
/// bitfield `1 << 3 | 1 << 6 | 1 << 8` ([`AuthHelper.authorizedOperations`],
/// [`AclEntry.supportedOperations`]).
///
/// [`AuthHelper.authorizedOperations`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/AuthHelper.scala#L62-L76
/// [`AclEntry.supportedOperations`]: https://github.com/apache/kafka/blob/3.9.1/server/src/main/java/org/apache/kafka/security/authorizer/AclEntry.java#L150-L167
const SUPPORTED_GROUP_OPERATIONS: i32 = 328;

async fn create_group(storage: &impl Storage, group_id: &str) -> Result<()> {
    _ = storage
        .update_group(group_id, GroupDetail::default(), None)
        .await
        .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;
    Ok(())
}

/// Returns each group in `group_ids` as the group coordinator describes it.
async fn describe(
    storage: impl Storage + Clone,
    group_ids: &[String],
    include_authorized_operations: bool,
) -> Result<Vec<DescribedGroup>> {
    let response: DescribeGroupsResponse = Controller::with_storage(storage)?
        .describe_groups(Some(group_ids), include_authorized_operations)
        .await
        .and_then(|body| TryInto::try_into(body).map_err(Into::into))?;

    Ok(response.groups.unwrap_or_default())
}

async fn operations_omitted_unless_requested(storage: impl Storage + Clone) -> Result<()> {
    let group_id = alphanumeric_string(15);
    create_group(&storage, &group_id).await?;

    let groups = describe(storage, &[group_id], false).await?;
    assert_eq!(1, groups.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(groups[0].error_code)?);
    assert_eq!(Some(i32::MIN), groups[0].authorized_operations);

    Ok(())
}

async fn found_group_answers_supported_operations_when_asked(
    storage: impl Storage + Clone,
) -> Result<()> {
    let group_id = alphanumeric_string(15);
    create_group(&storage, &group_id).await?;

    let groups = describe(storage, &[group_id], true).await?;
    assert_eq!(1, groups.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(groups[0].error_code)?);
    assert_eq!(
        Some(SUPPORTED_GROUP_OPERATIONS),
        groups[0].authorized_operations
    );

    Ok(())
}

/// `DescribeGroups` v0 to v5 describes a group that does not exist with the error NONE and the
/// state `Dead`, as Kafka does. A client reads the group as missing from that state, but fails on
/// `GROUP_ID_NOT_FOUND`. Requires an engine that stores the groups that it describes.
#[cfg(any(feature = "dynostore", feature = "libsql", feature = "postgres"))]
async fn unknown_group_answers_dead(storage: impl Storage + Clone) -> Result<()> {
    for include_authorized_operations in [false, true] {
        let groups = describe(
            storage.clone(),
            &[alphanumeric_string(15)],
            include_authorized_operations,
        )
        .await?;
        assert_eq!(1, groups.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(groups[0].error_code)?);
        assert_eq!("Dead", groups[0].group_state);
        assert!(groups[0].members.as_deref().unwrap_or_default().is_empty());

        let expected = if include_authorized_operations {
            SUPPORTED_GROUP_OPERATIONS
        } else {
            i32::MIN
        };
        assert_eq!(Some(expected), groups[0].authorized_operations);
    }

    Ok(())
}

/// A group that only committed offsets, and never joined, exists: the broker must answer it as
/// an empty group, not with `GROUP_ID_NOT_FOUND`.
#[cfg(any(feature = "dynostore", feature = "libsql", feature = "postgres"))]
async fn group_with_only_committed_offsets_is_empty(storage: impl Storage + Clone) -> Result<()> {
    let topic_name = alphanumeric_string(15);
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let group_id = alphanumeric_string(15);
    let topition = Topition::new(topic_name, 0);
    let committed = storage
        .offset_commit(
            &group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(rng().random_range(0..i64::MAX)),
            )],
        )
        .await?;
    assert_eq!(vec![(topition, ErrorCode::None)], committed);

    let groups = describe(storage, slice::from_ref(&group_id), false).await?;
    assert_eq!(1, groups.len());
    assert_eq!(group_id, groups[0].group_id);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(groups[0].error_code)?);
    assert_eq!("Empty", groups[0].group_state);

    Ok(())
}

/// A group whose consumers stopped without sending `LeaveGroup` must be described as `Empty` once
/// their sessions expire, as Kafka describes it, so that tools that need an empty group, such as
/// an offset reset, accept it. The broker must remove each member whose session has expired before
/// it describes the group, also when no member has sent a request since. The broker answers no
/// protocol for the group, as Kafka does: Kafka forgets the protocol when the group becomes
/// `Empty`, and answers a protocol only for a `Stable` group.
async fn group_whose_sessions_expired_is_described_as_empty(
    storage: impl Storage + Clone,
) -> Result<()> {
    let group_id = alphanumeric_string(15);
    let member_id = alphanumeric_string(15);

    let session_timeout =
        Duration::from_millis(u64::try_from(GroupDetail::default().session_timeout_ms)?);

    let detail = GroupDetail {
        members: [(
            member_id.clone(),
            GroupMember {
                join_response: JoinGroupResponseMember::default().member_id(member_id.clone()),
                last_contact: Some(SystemTime::now() - session_timeout - Duration::from_secs(1)),
                ..GroupMember::default()
            },
        )]
        .into(),
        generation_id: 1,
        state: GroupState::Formed {
            protocol_type: "consumer".into(),
            protocol_name: "range".into(),
            leader: member_id.clone(),
            assignments: [(member_id, Bytes::new())].into(),
        },
        ..GroupDetail::default()
    };

    _ = storage
        .update_group(&group_id, detail, None)
        .await
        .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;

    let groups = describe(storage, &[group_id], false).await?;
    assert_eq!(1, groups.len());
    assert_eq!("Empty", groups[0].group_state);
    assert_eq!("", groups[0].protocol_data);
    assert_eq!(Some(vec![]), groups[0].members);

    Ok(())
}

/// A member of a test group, with the values that `DescribeGroups` reports for it.
struct TestMember {
    member_id: String,
    group_instance_id: Option<&'static str>,
    client_id: &'static str,
    client_host: &'static str,
    metadata: Bytes,
    assignment: Bytes,
}

impl TestMember {
    fn new(client_id: &'static str, client_host: &'static str) -> Self {
        Self {
            member_id: alphanumeric_string(15),
            group_instance_id: None,
            client_id,
            client_host,
            metadata: Bytes::from(format!("{client_id} subscription")),
            assignment: Bytes::from(format!("{client_id} assignment")),
        }
    }

    /// Returns the member as a static member, with the given group instance id.
    fn with_group_instance_id(self, group_instance_id: &'static str) -> Self {
        Self {
            group_instance_id: Some(group_instance_id),
            ..self
        }
    }

    /// Returns the member as the group coordinator stores it, with `last_contact` set to the
    /// current time. Before it describes a group, the coordinator removes each member whose
    /// session timeout has expired, so it describes only a member whose `last_contact` is within
    /// its session timeout.
    fn stored(&self) -> (String, GroupMember) {
        (
            self.member_id.clone(),
            GroupMember {
                join_response: JoinGroupResponseMember::default()
                    .member_id(self.member_id.clone())
                    .group_instance_id(self.group_instance_id.map(Into::into))
                    .metadata(self.metadata.clone()),
                last_contact: Some(SystemTime::now()),
                client_id: self.client_id.into(),
                client_host: self.client_host.into(),
            },
        )
    }

    /// Returns the member as `DescribeGroups` reports it in a `Stable` group.
    fn described(&self) -> DescribedGroupMember {
        DescribedGroupMember::default()
            .member_id(self.member_id.clone())
            .group_instance_id(self.group_instance_id.map(Into::into))
            .client_id(self.client_id.into())
            .client_host(self.client_host.into())
            .member_metadata(self.metadata.clone())
            .member_assignment(self.assignment.clone())
    }
}

/// The parts of a `DescribeGroups` answer that show who reads which partitions.
#[derive(Debug, PartialEq)]
struct GroupMembership {
    group_state: String,
    /// The protocol that the group selected. For a consumer group, the protocol is the partition
    /// assignor.
    protocol_data: String,
    members_by_id: BTreeMap<String, DescribedGroupMember>,
}

impl From<DescribedGroup> for GroupMembership {
    fn from(group: DescribedGroup) -> Self {
        Self {
            group_state: group.group_state,
            protocol_data: group.protocol_data,
            members_by_id: group
                .members
                .unwrap_or_default()
                .into_iter()
                .map(|member| (member.member_id.clone(), member))
                .collect(),
        }
    }
}

/// Stores `detail` as the group `group_id`, and returns the group's membership as the coordinator
/// describes it.
async fn store_and_describe(
    storage: impl Storage + Clone,
    group_id: &str,
    detail: GroupDetail,
) -> Result<GroupMembership> {
    _ = storage
        .update_group(group_id, detail, None)
        .await
        .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;

    let mut groups = describe(storage, &[group_id.into()], false).await?;
    assert_eq!(1, groups.len());
    Ok(GroupMembership::from(groups.remove(0)))
}

/// Describing a running group must show who reads which partitions. The broker must answer
/// `DescribeGroups` for a `Stable` group with each member's group instance id, client id, host,
/// subscription and assignment, and the group's assignor.
async fn stable_group_reports_each_member_and_the_protocol(
    storage: impl Storage + Clone,
) -> Result<()> {
    let members = [
        TestMember::new("first-client", "/192.0.2.1"),
        TestMember::new("second-client", "/192.0.2.2").with_group_instance_id("second-instance"),
    ];

    let detail = GroupDetail {
        members: members.iter().map(TestMember::stored).collect(),
        generation_id: 1,
        state: GroupState::Formed {
            protocol_type: "consumer".into(),
            protocol_name: "range".into(),
            leader: members[0].member_id.clone(),
            assignments: members
                .iter()
                .map(|member| (member.member_id.clone(), member.assignment.clone()))
                .collect(),
        },
        ..GroupDetail::default()
    };

    assert_eq!(
        GroupMembership {
            group_state: "Stable".into(),
            protocol_data: "range".into(),
            members_by_id: members
                .iter()
                .map(|member| (member.member_id.clone(), member.described()))
                .collect(),
        },
        store_and_describe(storage, &alphanumeric_string(15), detail).await?
    );

    Ok(())
}

/// During a rebalance, a member's stored subscription can be for a protocol that the group has not
/// selected yet, and an admin client that decodes it with the wrong assignor shows a wrong
/// subscription. The broker must answer `DescribeGroups` for a rebalancing group with each
/// member's group instance id, client id and host, and with an empty subscription, assignment and
/// protocol, as Kafka does.
async fn rebalancing_group_reports_members_without_metadata(
    storage: impl Storage + Clone,
) -> Result<()> {
    let members = [
        TestMember::new("first-client", "/192.0.2.1"),
        TestMember::new("second-client", "/192.0.2.2").with_group_instance_id("second-instance"),
    ];

    let detail = GroupDetail {
        members: members.iter().map(TestMember::stored).collect(),
        generation_id: 1,
        state: GroupState::Forming {
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
            leader: Some(members[0].member_id.clone()),
        },
        rebalance: Box::new(GroupRebalance {
            pending_sync: Some(PendingSync {
                deadline: SystemTime::now() + Duration::from_secs(300),
                members: members
                    .iter()
                    .map(|member| member.member_id.clone())
                    .collect(),
            }),
            ..GroupRebalance::default()
        }),
        ..GroupDetail::default()
    };

    assert_eq!(
        GroupMembership {
            group_state: "CompletingRebalance".into(),
            protocol_data: "".into(),
            members_by_id: members
                .iter()
                .map(|member| {
                    let without_subscription_or_assignment = member
                        .described()
                        .member_metadata(Bytes::new())
                        .member_assignment(Bytes::new());
                    (member.member_id.clone(), without_subscription_or_assignment)
                })
                .collect(),
        },
        store_and_describe(storage, &alphanumeric_string(15), detail).await?
    );

    Ok(())
}

async fn simple(storage: impl Storage + Clone) -> Result<()> {
    let service = DescribeGroupsService {
        storage: storage.clone(),
    };

    let group_id = &alphanumeric_string(15)[..];

    let response = service
        .serve(RequestInput {
            request: DescribeGroupsRequest::default()
                .groups(Some([group_id.into()].into()))
                .include_authorized_operations(Some(false)),
            extensions: Extensions::default(),
        })
        .await?;

    let groups = response.groups.unwrap_or_default();
    assert_eq!(1, groups.len());

    assert_eq!(ErrorCode::None, ErrorCode::try_from(groups[0].error_code)?);
    assert_eq!(group_id, groups[0].group_id.as_str());
    assert_matches!(groups[0].group_state.as_str(), "Empty" | "Dead");

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        memory_storage(cluster, node).await
    }

    #[tokio::test]
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage).await?;

        Ok(())
    }

    async fn storage() -> Result<ArcDynStorage> {
        storage_container(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await
    }

    #[tokio::test]
    async fn operations_omitted_unless_requested() -> Result<()> {
        let _guard = init_tracing()?;
        super::operations_omitted_unless_requested(storage().await?).await
    }

    #[tokio::test]
    async fn found_group_answers_supported_operations_when_asked() -> Result<()> {
        let _guard = init_tracing()?;
        super::found_group_answers_supported_operations_when_asked(storage().await?).await
    }

    #[tokio::test]
    async fn unknown_group_answers_dead() -> Result<()> {
        let _guard = init_tracing()?;
        super::unknown_group_answers_dead(storage().await?).await
    }

    #[tokio::test]
    async fn group_with_only_committed_offsets_is_empty() -> Result<()> {
        let _guard = init_tracing()?;
        super::group_with_only_committed_offsets_is_empty(storage().await?).await
    }

    #[tokio::test]
    async fn group_whose_sessions_expired_is_described_as_empty() -> Result<()> {
        let _guard = init_tracing()?;
        super::group_whose_sessions_expired_is_described_as_empty(storage().await?).await
    }

    #[tokio::test]
    async fn stable_group_reports_each_member_and_the_protocol() -> Result<()> {
        let _guard = init_tracing()?;
        super::stable_group_reports_each_member_and_the_protocol(storage().await?).await
    }

    #[tokio::test]
    async fn rebalancing_group_reports_members_without_metadata() -> Result<()> {
        let _guard = init_tracing()?;
        super::rebalancing_group_reports_members_without_metadata(storage().await?).await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        lite_storage(cluster, node).await
    }

    #[tokio::test]
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage).await?;

        Ok(())
    }

    async fn storage() -> Result<ArcDynStorage> {
        storage_container(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await
    }

    #[tokio::test]
    async fn operations_omitted_unless_requested() -> Result<()> {
        let _guard = init_tracing()?;
        super::operations_omitted_unless_requested(storage().await?).await
    }

    #[tokio::test]
    async fn found_group_answers_supported_operations_when_asked() -> Result<()> {
        let _guard = init_tracing()?;
        super::found_group_answers_supported_operations_when_asked(storage().await?).await
    }

    #[tokio::test]
    async fn unknown_group_answers_dead() -> Result<()> {
        let _guard = init_tracing()?;
        super::unknown_group_answers_dead(storage().await?).await
    }

    #[tokio::test]
    async fn group_with_only_committed_offsets_is_empty() -> Result<()> {
        let _guard = init_tracing()?;
        super::group_with_only_committed_offsets_is_empty(storage().await?).await
    }

    #[tokio::test]
    async fn group_whose_sessions_expired_is_described_as_empty() -> Result<()> {
        let _guard = init_tracing()?;
        super::group_whose_sessions_expired_is_described_as_empty(storage().await?).await
    }

    #[tokio::test]
    async fn stable_group_reports_each_member_and_the_protocol() -> Result<()> {
        let _guard = init_tracing()?;
        super::stable_group_reports_each_member_and_the_protocol(storage().await?).await
    }

    #[tokio::test]
    async fn rebalancing_group_reports_members_without_metadata() -> Result<()> {
        let _guard = init_tracing()?;
        super::rebalancing_group_reports_members_without_metadata(storage().await?).await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        slate_storage(cluster, node).await
    }

    #[tokio::test]
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        postgres_storage(cluster, node).await
    }

    #[tokio::test]
    async fn simple() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::simple(storage).await?;

        Ok(())
    }

    async fn storage() -> Result<ArcDynStorage> {
        storage_container(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await
    }

    #[tokio::test]
    async fn operations_omitted_unless_requested() -> Result<()> {
        let _guard = init_tracing()?;
        super::operations_omitted_unless_requested(storage().await?).await
    }

    #[tokio::test]
    async fn found_group_answers_supported_operations_when_asked() -> Result<()> {
        let _guard = init_tracing()?;
        super::found_group_answers_supported_operations_when_asked(storage().await?).await
    }

    #[tokio::test]
    async fn unknown_group_answers_dead() -> Result<()> {
        let _guard = init_tracing()?;
        super::unknown_group_answers_dead(storage().await?).await
    }

    #[tokio::test]
    async fn group_with_only_committed_offsets_is_empty() -> Result<()> {
        let _guard = init_tracing()?;
        super::group_with_only_committed_offsets_is_empty(storage().await?).await
    }

    #[tokio::test]
    async fn group_whose_sessions_expired_is_described_as_empty() -> Result<()> {
        let _guard = init_tracing()?;
        super::group_whose_sessions_expired_is_described_as_empty(storage().await?).await
    }

    #[tokio::test]
    async fn stable_group_reports_each_member_and_the_protocol() -> Result<()> {
        let _guard = init_tracing()?;
        super::stable_group_reports_each_member_and_the_protocol(storage().await?).await
    }

    #[tokio::test]
    async fn rebalancing_group_reports_members_without_metadata() -> Result<()> {
        let _guard = init_tracing()?;
        super::rebalancing_group_reports_members_without_metadata(storage().await?).await
    }
}
