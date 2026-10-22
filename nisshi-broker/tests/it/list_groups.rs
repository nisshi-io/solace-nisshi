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

use crate::common::{
    alphanumeric_string, init_tracing, lite_storage, memory_storage, postgres_storage,
    slate_storage,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use nisshi_broker::{
    Error, Result,
    coordinator::group::{Coordinator, administrator::Controller},
};
use nisshi_sans_io::{
    ErrorCode, ListGroupsRequest, ListGroupsResponse, RequestInput,
    create_topics_request::CreatableTopic, join_group_response::JoinGroupResponseMember,
};
use nisshi_storage::{
    ArcDynStorage, GroupDetail, GroupMember, GroupRebalance, GroupState, JoinPhase,
    ListGroupsService, OffsetCommitRequest, PendingSync, Storage, Topition,
};
use rama::{Service, extensions::Extensions};
use rand::{prelude::*, rng};
use uuid::Uuid;

async fn simple(storage: impl Storage + Clone) -> Result<()> {
    let service = ListGroupsService { storage };

    let response = service
        .serve(RequestInput {
            request: ListGroupsRequest::default().states_filter(Some(["Empty".into()].into())),
            extensions: Extensions::default(),
        })
        .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
    assert_eq!(Some([].into()), response.groups);

    Ok(())
}

/// A group that the coordinator has stored, but that has not committed an offset, must be listed
/// exactly once, because Kafka lists every group the coordinator knows. A group has no committed
/// offset until its first commit, and an operator must be able to find it before then.
async fn group_without_committed_offsets_is_listed_once(
    storage: impl Storage + Clone,
) -> Result<()> {
    let group_id = alphanumeric_string(15);

    _ = storage
        .update_group(&group_id, GroupDetail::default(), None)
        .await
        .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;

    let groups = storage.list_groups().await?;

    assert_eq!(
        1,
        groups.iter().filter(|group| group.name == group_id).count(),
        "expected {group_id:?} exactly once in {groups:?}"
    );

    Ok(())
}

/// A group that the coordinator has stored, and that has committed an offset, must be listed
/// once, not once for its state and once for its offsets.
async fn group_with_committed_offsets_is_listed_once(storage: impl Storage + Clone) -> Result<()> {
    let topic_name = alphanumeric_string(15);
    let group_id = alphanumeric_string(15);

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

    _ = storage
        .update_group(&group_id, GroupDetail::default(), None)
        .await
        .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;

    let commit = storage
        .offset_commit(
            &group_id,
            None,
            &[(
                Topition::new(topic_name, 0),
                OffsetCommitRequest::default().offset(rng().random_range(0..i64::MAX)),
            )],
        )
        .await?;
    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let groups = storage.list_groups().await?;

    assert_eq!(
        1,
        groups.iter().filter(|group| group.name == group_id).count(),
        "expected {group_id:?} exactly once in {groups:?}"
    );

    Ok(())
}

/// Returns a detail with one member, in the given group state and rebalance.
fn detail_with_one_member(
    member_id: &str,
    state: GroupState,
    rebalance: GroupRebalance,
) -> GroupDetail {
    GroupDetail {
        members: [(
            member_id.into(),
            GroupMember {
                join_response: JoinGroupResponseMember::default().member_id(member_id.into()),
                last_contact: Some(SystemTime::now()),
                ..GroupMember::default()
            },
        )]
        .into(),
        generation_id: 1,
        state,
        rebalance: Box::new(rebalance),
        ..GroupDetail::default()
    }
}

/// Returns a forming group state with the `consumer` protocol type and `range` protocol.
fn forming_group_state(leader: Option<String>) -> GroupState {
    GroupState::Forming {
        protocol_type: Some("consumer".into()),
        protocol_name: Some("range".into()),
        leader,
    }
}

/// Commits offset 1 on partition 0 of `topic_name` for `group_id`.
async fn commit_offset(
    storage: &impl Storage,
    topic_name: &str,
    group_id: &str,
) -> nisshi_storage::Result<Vec<(Topition, ErrorCode)>> {
    storage
        .offset_commit(
            group_id,
            None,
            &[(
                Topition::new(topic_name.to_owned(), 0),
                OffsetCommitRequest::default().offset(1),
            )],
        )
        .await
}

/// The ids of the groups that [`store_group_in_each_state`] stores, one for each state that
/// `ListGroups` reports.
struct StoredGroupIds {
    /// A group with a formed state and committed offsets.
    stable: String,
    /// A group with the default detail.
    empty: String,
    /// A forming group without a leader, in a join phase.
    preparing_rebalance: String,
    /// A forming group with a leader, waiting for its members to sync.
    completing_rebalance: String,
    /// A group with committed offsets and no stored detail.
    offsets_only: String,
}

/// Stores `detail` under a new group id, and returns that id.
async fn store_group(storage: &impl Storage, detail: GroupDetail) -> Result<String> {
    let group_id = alphanumeric_string(15);

    _ = storage
        .update_group(&group_id, detail, None)
        .await
        .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;

    Ok(group_id)
}

/// Stores a group in each state that `ListGroups` reports, and returns their ids.
async fn store_group_in_each_state(storage: &impl Storage) -> Result<StoredGroupIds> {
    let member_id = alphanumeric_string(15);

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

    let stable = store_group(
        storage,
        detail_with_one_member(
            &member_id,
            GroupState::Formed {
                protocol_type: "consumer".into(),
                protocol_name: "range".into(),
                leader: member_id.clone(),
                assignments: [(member_id.clone(), Bytes::new())].into(),
            },
            GroupRebalance::default(),
        ),
    )
    .await?;

    // The stable group also has committed offsets. A storage engine can store a group's detail
    // and its offsets apart, and `ListGroups` must still list the group once.
    _ = commit_offset(storage, &topic_name, &stable).await?;

    let empty = store_group(storage, GroupDetail::default()).await?;

    let preparing_rebalance = store_group(
        storage,
        detail_with_one_member(
            &member_id,
            forming_group_state(None),
            GroupRebalance {
                join_phase: Some(JoinPhase {
                    deadline: SystemTime::now() + Duration::from_secs(60),
                    joined: BTreeSet::new(),
                    initial_delay: None,
                }),
                ..GroupRebalance::default()
            },
        ),
    )
    .await?;

    let completing_rebalance = store_group(
        storage,
        detail_with_one_member(
            &member_id,
            forming_group_state(Some(member_id.clone())),
            GroupRebalance {
                pending_sync: Some(PendingSync {
                    deadline: SystemTime::now() + Duration::from_secs(60),
                    members: [member_id.clone()].into(),
                }),
                ..GroupRebalance::default()
            },
        ),
    )
    .await?;

    let offsets_only = alphanumeric_string(15);
    _ = commit_offset(storage, &topic_name, &offsets_only).await?;

    Ok(StoredGroupIds {
        stable,
        empty,
        preparing_rebalance,
        completing_rebalance,
        offsets_only,
    })
}

/// Returns the state of each group that the group coordinator lists for `states_filter`.
async fn listed_state_by_group(
    storage: impl Storage + Clone,
    states_filter: Option<Vec<String>>,
) -> Result<BTreeMap<String, String>> {
    let response: ListGroupsResponse = Controller::with_storage(storage)?
        .list_groups(states_filter.as_deref())
        .await
        .and_then(|body| TryInto::try_into(body).map_err(Into::into))?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

    let listed_groups = response.groups.unwrap_or_default();

    let state_by_group = listed_groups
        .iter()
        .map(|group| {
            (
                group.group_id.clone(),
                group.group_state.clone().unwrap_or_default(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    assert_eq!(
        listed_groups.len(),
        state_by_group.len(),
        "a group is listed more than once: {listed_groups:?}"
    );

    Ok(state_by_group)
}

/// Listing the groups must show each group's state, so an operator can see which groups are
/// running, idle or rebalancing. The broker must answer `ListGroups` with the state of each
/// group, from the group's stored detail.
async fn lists_each_group_with_its_state(storage: impl Storage + Clone) -> Result<()> {
    let groups = store_group_in_each_state(&storage).await?;

    // `ListGroups` reports a group that has committed offsets and no stored detail as `Empty`.
    let expected = BTreeMap::from([
        (groups.stable, "Stable".into()),
        (groups.empty, "Empty".into()),
        (groups.preparing_rebalance, "PreparingRebalance".into()),
        (groups.completing_rebalance, "CompletingRebalance".into()),
        (groups.offsets_only, "Empty".into()),
    ]);

    assert_eq!(expected, listed_state_by_group(storage, None).await?);

    Ok(())
}

/// Listing the groups in a state must show only the groups in that state, so an operator can find
/// the groups that are running. The broker must leave out of `ListGroups` each group whose state
/// is not in the request's state filter.
async fn states_filter_lists_only_groups_in_that_state(
    storage: impl Storage + Clone,
) -> Result<()> {
    let groups = store_group_in_each_state(&storage).await?;

    let expected = BTreeMap::from([(groups.stable, "Stable".into())]);

    assert_eq!(
        expected,
        listed_state_by_group(storage, Some(vec!["Stable".into()])).await?
    );

    Ok(())
}

/// A group whose consumers stopped without sending `LeaveGroup` must be listed as `Empty` once
/// their sessions expire, as Kafka lists it, so an operator can find the abandoned group. The
/// broker must remove each member whose session has expired before it reports the group's state,
/// also when no member has sent a request since.
async fn group_whose_sessions_expired_is_listed_as_empty(
    storage: impl Storage + Clone,
) -> Result<()> {
    let member_id = alphanumeric_string(15);

    let mut detail = detail_with_one_member(
        &member_id,
        GroupState::Formed {
            protocol_type: "consumer".into(),
            protocol_name: "range".into(),
            leader: member_id.clone(),
            assignments: [(member_id.clone(), Bytes::new())].into(),
        },
        GroupRebalance::default(),
    );

    let session_timeout = Duration::from_millis(u64::try_from(detail.session_timeout_ms)?);

    for member in detail.members.values_mut() {
        member.last_contact = Some(SystemTime::now() - session_timeout - Duration::from_secs(1));
    }

    let group_id = store_group(&storage, detail).await?;

    let listed = listed_state_by_group(storage, None).await?;

    assert_eq!(Some(&"Empty".into()), listed.get(&group_id));

    Ok(())
}

/// A group whose join phase has passed its deadline must not be listed as rebalancing, because
/// Kafka ends the phase at its deadline. A group whose last member leaves during the initial
/// rebalance delay keeps its join phase in storage until a request ends it. The broker must end
/// the phase before it reports the group's state, so the group is listed as `Empty`.
async fn group_past_its_join_deadline_is_listed_as_empty(
    storage: impl Storage + Clone,
) -> Result<()> {
    let group_id = store_group(
        &storage,
        GroupDetail {
            state: forming_group_state(None),
            rebalance: Box::new(GroupRebalance {
                join_phase: Some(JoinPhase {
                    deadline: SystemTime::now() - Duration::from_secs(1),
                    joined: BTreeSet::new(),
                    initial_delay: None,
                }),
                ..GroupRebalance::default()
            }),
            ..GroupDetail::default()
        },
    )
    .await?;

    let listed = listed_state_by_group(storage, None).await?;

    assert_eq!(Some(&"Empty".into()), listed.get(&group_id));

    Ok(())
}

/// Regression coverage for the specific symptom `EMPTY_GROUP_SENTINEL`
/// exists to prevent, surfaced at the `list_groups` layer rather than at the
/// sentinel's own encode/decode unit tests.
///
/// On dynostore, `list_groups` lists via `list_with_delimiter`, a different
/// `object_store` operation to the plain prefix `list` every other group
/// operation uses. Without the sentinel, the empty group id would produce a
/// key containing a doubled slash (`consumers//offsets/...`), and
/// `list_with_delimiter`'s common-prefix computation normalizes that doubled
/// slash away - surfacing a phantom group under the wrong name instead of
/// the real empty-id group.
///
/// This commits an offset under group id `""`, then asserts `list_groups`
/// reports a group with id exactly `""` and does not report any phantom
/// group under a name a doubled-slash key could plausibly decode to.
#[cfg(feature = "dynostore")]
async fn empty_group_id_is_not_surfaced_as_phantom_group(
    storage: impl Storage + Clone,
) -> Result<()> {
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

    let topition = Topition::new(topic_name, 0);
    let offset = rng().random_range(0..i64::MAX);

    let commit = storage
        .offset_commit(
            "",
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;
    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let groups = storage.list_groups().await?;

    assert!(
        groups.iter().any(|group| group.name.is_empty()),
        "expected a group with id \"\" in {groups:?}"
    );

    for phantom in ["consumers", "offsets", "%empty"] {
        assert!(
            !groups.iter().any(|group| group.name == phantom),
            "list_groups surfaced a phantom group named {phantom:?}: {groups:?}"
        );
    }

    Ok(())
}

/// A group with the empty id and only a state object must be listed once, as `""`. On dynostore
/// that state object is `%empty.json`, because the empty id is stored as the sentinel `%empty`,
/// and `list_groups` must decode the sentinel instead of listing a group named `%empty`.
#[cfg(feature = "dynostore")]
async fn empty_group_id_without_committed_offsets_is_listed(
    storage: impl Storage + Clone,
) -> Result<()> {
    _ = storage
        .update_group("", GroupDetail::default(), None)
        .await
        .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;

    let groups = storage.list_groups().await?;

    assert_eq!(
        1,
        groups.iter().filter(|group| group.name.is_empty()).count(),
        "expected a group with id \"\" exactly once in {groups:?}"
    );

    for phantom in ["%empty", "%empty.json"] {
        assert!(
            !groups.iter().any(|group| group.name == phantom),
            "list_groups surfaced a phantom group named {phantom:?}: {groups:?}"
        );
    }

    Ok(())
}

/// On dynostore, a group's state object is its encoded id followed by `.json`, so `list_groups`
/// must remove the `.json` before it decodes the id. A group named `foo.json` must stay distinct
/// from the state object of a group named `foo`, and an id with reserved characters must be
/// listed as it was stored.
#[cfg(feature = "dynostore")]
async fn state_only_group_ids_are_decoded(storage: impl Storage + Clone) -> Result<()> {
    let base = alphanumeric_string(15);
    let group_ids = [base.clone(), format!("{base}.json"), format!("{base}/a%b")];

    for group_id in &group_ids {
        _ = storage
            .update_group(group_id, GroupDetail::default(), None)
            .await
            .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;
    }

    let groups = storage.list_groups().await?;

    for group_id in &group_ids {
        assert_eq!(
            1,
            groups
                .iter()
                .filter(|group| &group.name == group_id)
                .count(),
            "expected {group_id:?} exactly once in {groups:?}"
        );
    }

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

    #[tokio::test]
    async fn group_without_committed_offsets_is_listed_once() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_without_committed_offsets_is_listed_once(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn group_with_committed_offsets_is_listed_once() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_with_committed_offsets_is_listed_once(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn lists_each_group_with_its_state() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::lists_each_group_with_its_state(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn states_filter_lists_only_groups_in_that_state() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::states_filter_lists_only_groups_in_that_state(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn group_whose_sessions_expired_is_listed_as_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_whose_sessions_expired_is_listed_as_empty(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn group_past_its_join_deadline_is_listed_as_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_past_its_join_deadline_is_listed_as_empty(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn empty_group_id_is_not_surfaced_as_phantom_group() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::empty_group_id_is_not_surfaced_as_phantom_group(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn empty_group_id_without_committed_offsets_is_listed() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::empty_group_id_without_committed_offsets_is_listed(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn state_only_group_ids_are_decoded() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::state_only_group_ids_are_decoded(storage).await?;

        Ok(())
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

    #[tokio::test]
    async fn group_without_committed_offsets_is_listed_once() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_without_committed_offsets_is_listed_once(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn group_with_committed_offsets_is_listed_once() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_with_committed_offsets_is_listed_once(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn lists_each_group_with_its_state() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::lists_each_group_with_its_state(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn states_filter_lists_only_groups_in_that_state() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::states_filter_lists_only_groups_in_that_state(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn group_whose_sessions_expired_is_listed_as_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_whose_sessions_expired_is_listed_as_empty(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn group_past_its_join_deadline_is_listed_as_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_past_its_join_deadline_is_listed_as_empty(storage).await?;

        Ok(())
    }
}

#[cfg(feature = "turso")]
mod turso {
    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        crate::common::turso_storage(cluster, node).await
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

    #[tokio::test]
    async fn group_without_committed_offsets_is_listed_once() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_without_committed_offsets_is_listed_once(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn group_with_committed_offsets_is_listed_once() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_with_committed_offsets_is_listed_once(storage).await?;

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

    #[tokio::test]
    async fn group_without_committed_offsets_is_listed_once() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_without_committed_offsets_is_listed_once(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn group_with_committed_offsets_is_listed_once() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_with_committed_offsets_is_listed_once(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn lists_each_group_with_its_state() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::lists_each_group_with_its_state(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn states_filter_lists_only_groups_in_that_state() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::states_filter_lists_only_groups_in_that_state(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn group_whose_sessions_expired_is_listed_as_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_whose_sessions_expired_is_listed_as_empty(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn group_past_its_join_deadline_is_listed_as_empty() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::group_past_its_join_deadline_is_listed_as_empty(storage).await?;

        Ok(())
    }
}
