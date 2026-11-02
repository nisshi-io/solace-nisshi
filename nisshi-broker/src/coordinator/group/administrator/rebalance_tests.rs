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

//! The rebalance protocol of Kafka's classic group coordinator, driven through the group's state
//! transitions at an explicit time, so that each test is deterministic and never sleeps.

use std::{assert_matches, sync::Arc};

use nisshi_sans_io::{
    heartbeat_response::HeartbeatResponse, join_group_response::JoinGroupResponse,
    sync_group_response::SyncGroupResponse,
};
use nisshi_storage::{ArcDynStorage, GroupMember, StorageContainer};
use pretty_assertions::assert_eq;
use url::Url;

use super::*;

const GROUP: &str = "group";
const PROTOCOL_TYPE: &str = "consumer";
const CLIENT_ID: &str = "client";
const CLIENT_HOST: &str = "/192.0.2.7";
const SESSION_TIMEOUT_MS: i32 = 45_000;
const REBALANCE_TIMEOUT_MS: i32 = 20_000;
const INITIAL_REBALANCE_DELAY: Duration = Duration::from_secs(3);
const LATEST_JOIN_GROUP_VERSION: i16 = 9;

/// Returns the time `millis` after the start of a test.
fn at(millis: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000) + Duration::from_millis(millis)
}

fn protocols(metadata: &'static [u8]) -> Vec<JoinGroupRequestProtocol> {
    vec![
        JoinGroupRequestProtocol::default()
            .name("range".into())
            .metadata(Bytes::from_static(metadata)),
    ]
}

pub(super) async fn null_storage() -> Result<ArcDynStorage> {
    let mut builder = StorageContainer::builder();
    builder.with_factory(Arc::new(nisshi_storage_null::EngineFactory));

    builder
        .cluster_id("cluster")
        .node_id(111)
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
        .schema_registry(None)
        .storage(Url::parse("null://")?)
        .build()
        .await
        .map_err(Into::into)
}

#[track_caller]
fn join_response(reply: &Reply) -> JoinGroupResponse {
    match reply {
        Reply::Ready(body) => JoinGroupResponse::try_from(body.clone()).expect("a join response"),
        Reply::Held { member_id } => panic!("the join of {member_id} is held"),
    }
}

#[track_caller]
fn sync_response(reply: &Reply) -> SyncGroupResponse {
    match reply {
        Reply::Ready(body) => SyncGroupResponse::try_from(body.clone()).expect("a sync response"),
        Reply::Held { member_id } => panic!("the sync of {member_id} is held"),
    }
}

#[track_caller]
fn error_code(code: i16) -> ErrorCode {
    ErrorCode::try_from(code).expect("an error code")
}

fn held(member_id: &str) -> Reply {
    Reply::Held {
        member_id: member_id.to_owned(),
    }
}

/// A group that each method settles at the given time and then sends a request to, as
/// [`Controller`] does.
struct Harness {
    group: Option<Wrapper<ArcDynStorage>>,
    rebalance_timeout_ms: i32,
}

impl Harness {
    async fn new() -> Result<Self> {
        Ok(Self {
            group: Some(Wrapper::Forming(Inner::new(null_storage().await?))),
            rebalance_timeout_ms: REBALANCE_TIMEOUT_MS,
        })
    }

    fn group(&self) -> &Wrapper<ArcDynStorage> {
        self.group.as_ref().expect("a group")
    }

    fn settled(&mut self, now: SystemTime) -> Wrapper<ArcDynStorage> {
        self.group.take().expect("a group").settle(GROUP, now)
    }

    fn settle(&mut self, now: SystemTime) {
        self.group = Some(self.settled(now));
    }

    async fn join_as(
        &mut self,
        now: SystemTime,
        api_version: i16,
        member_id: &str,
        group_instance_id: Option<&str>,
        metadata: &'static [u8],
    ) -> Reply {
        let protocols = protocols(metadata);
        let request = JoinRequest {
            api_version,
            client_id: Some(CLIENT_ID),
            client_host: Some(CLIENT_HOST),
            group_id: GROUP,
            session_timeout_ms: SESSION_TIMEOUT_MS,
            rebalance_timeout_ms: self.rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type: PROTOCOL_TYPE,
            protocols: Some(&protocols),
            reason: None,
        };

        let (group, reply) = self
            .settled(now)
            .join(now, &request, INITIAL_REBALANCE_DELAY)
            .await;
        self.group = Some(group);
        reply
    }

    async fn join(&mut self, now: SystemTime, member_id: &str) -> Reply {
        self.join_as(now, LATEST_JOIN_GROUP_VERSION, member_id, None, b"m")
            .await
    }

    /// Returns the member id that `MEMBER_ID_REQUIRED` gives a new dynamic member.
    async fn new_member_id(&mut self, now: SystemTime) -> String {
        let response = join_response(&self.join(now, "").await);
        assert_eq!(ErrorCode::MemberIdRequired, error_code(response.error_code));
        response.member_id
    }

    /// Adds a dynamic member, and returns its id and the reply to its join.
    async fn add_member(&mut self, now: SystemTime) -> (String, Reply) {
        let member_id = self.new_member_id(now).await;
        let reply = self.join(now, &member_id).await;
        (member_id, reply)
    }

    /// Adds a static member, and returns its id.
    async fn add_static_member(&mut self, now: SystemTime, group_instance_id: &str) -> String {
        match self
            .join_as(
                now,
                LATEST_JOIN_GROUP_VERSION,
                "",
                Some(group_instance_id),
                b"m",
            )
            .await
        {
            Reply::Held { member_id } => member_id,
            Reply::Ready(body) => panic!("the static member's join was answered: {body:?}"),
        }
    }

    async fn sync(
        &mut self,
        now: SystemTime,
        member_id: &str,
        generation_id: i32,
        assignments: &[SyncGroupRequestAssignment],
    ) -> Reply {
        let request = SyncRequest {
            group_id: GROUP,
            generation_id,
            member_id,
            group_instance_id: None,
            protocol_type: Some(PROTOCOL_TYPE),
            protocol_name: Some("range"),
            assignments: Some(assignments),
        };

        let (group, reply) = self.settled(now).sync(now, &request).await;
        self.group = Some(group);
        reply
    }

    async fn heartbeat_as(
        &mut self,
        now: SystemTime,
        member_id: &str,
        group_instance_id: Option<&str>,
        generation_id: i32,
    ) -> ErrorCode {
        let (group, body) = self
            .settled(now)
            .heartbeat(now, GROUP, generation_id, member_id, group_instance_id)
            .await;
        self.group = Some(group);
        error_code(
            HeartbeatResponse::try_from(body)
                .expect("a heartbeat response")
                .error_code,
        )
    }

    async fn heartbeat(
        &mut self,
        now: SystemTime,
        member_id: &str,
        generation_id: i32,
    ) -> ErrorCode {
        self.heartbeat_as(now, member_id, None, generation_id).await
    }

    async fn leave(&mut self, now: SystemTime, member_id: &str) {
        let (group, _) = self
            .settled(now)
            .leave(now, GROUP, Some(member_id), None)
            .await;
        self.group = Some(group);
    }

    /// Returns whether the group is in Kafka's `PreparingRebalance`.
    fn is_preparing_rebalance(&self) -> bool {
        self.group().join_phase().is_some()
    }

    fn is_stable(&self) -> bool {
        matches!(self.group(), Wrapper::Formed(_))
    }

    fn deadline(&self) -> Option<SystemTime> {
        self.group()
            .join_phase()
            .map(|join_phase| join_phase.deadline)
    }

    fn member_ids(&self) -> BTreeSet<String> {
        self.group()
            .members()
            .into_iter()
            .map(|member| member.member_id)
            .collect()
    }

    fn assignment(member_id: &str) -> Bytes {
        Bytes::from(format!("assigned to {member_id}"))
    }

    /// Syncs every member after a join phase, the leader first, and returns the members' ids with
    /// the leader first.
    async fn sync_all(&mut self, now: SystemTime) -> Vec<String> {
        let leader = self.group().leader().expect("a leader").to_owned();
        let generation_id = self.group().generation_id();
        let member_ids = self.member_ids();

        let assignments = member_ids
            .iter()
            .map(|member_id| {
                SyncGroupRequestAssignment::default()
                    .member_id(member_id.clone())
                    .assignment(Self::assignment(member_id))
            })
            .collect::<Vec<_>>();

        let reply = self.sync(now, &leader, generation_id, &assignments).await;
        assert_eq!(
            ErrorCode::None,
            error_code(sync_response(&reply).error_code)
        );

        let mut ordered = vec![leader.clone()];

        for member_id in member_ids
            .into_iter()
            .filter(|member_id| *member_id != leader)
        {
            let reply = self.sync(now, &member_id, generation_id, &[]).await;
            assert_eq!(
                Self::assignment(&member_id),
                sync_response(&reply).assignment
            );
            ordered.push(member_id);
        }

        assert!(self.is_stable());
        ordered
    }

    /// Forms a stable group, at generation 1 and 6 s, of `dynamic` members and a static member for each
    /// of `group_instance_ids`, which all join at the start. Returns the members' ids, the leader
    /// first.
    async fn stable_group(&mut self, dynamic: usize, group_instance_ids: &[&str]) -> Vec<String> {
        for _ in 0..dynamic {
            let (_, reply) = self.add_member(at(0)).await;
            assert_matches!(reply, Reply::Held { .. });
        }

        for group_instance_id in group_instance_ids {
            _ = self.add_static_member(at(0), group_instance_id).await;
        }

        // The later members joined during the first delay, so the join phase gets a second.
        self.settle(at(6_000));
        assert_eq!(1, self.group().generation_id());
        self.sync_all(at(6_000)).await
    }
}

#[tokio::test]
async fn initial_join_phase_is_extended_by_new_members_up_to_the_rebalance_timeout() -> Result<()> {
    let mut harness = Harness::new().await?;
    harness.rebalance_timeout_ms = 7_000;

    let (first, reply) = harness.add_member(at(0)).await;
    assert_eq!(held(&first), reply);
    assert_eq!(Some(at(3_000)), harness.deadline());

    _ = harness.add_member(at(1_000)).await;

    harness.settle(at(2_999));
    assert_eq!(Some(at(3_000)), harness.deadline());

    // A member joined during the first delay, so the phase gets another delay.
    harness.settle(at(3_000));
    assert_eq!(Some(at(6_000)), harness.deadline());

    _ = harness.add_member(at(4_000)).await;

    // Only 1 s of the rebalance timeout remains after the first two delays.
    harness.settle(at(6_000));
    assert_eq!(Some(at(7_000)), harness.deadline());

    _ = harness.add_member(at(6_500)).await;

    harness.settle(at(7_000));
    assert!(!harness.is_preparing_rebalance());
    assert_eq!(1, harness.group().generation_id());

    let response = join_response(&harness.join(at(7_000), &first).await);
    assert_eq!(ErrorCode::None, error_code(response.error_code));
    assert_eq!(first, response.leader);
    assert_eq!(4, response.members.unwrap_or_default().len());
    Ok(())
}

#[tokio::test]
async fn initial_join_phase_ends_after_one_delay_without_new_members() -> Result<()> {
    let mut harness = Harness::new().await?;

    let (member_id, _) = harness.add_member(at(0)).await;
    assert_eq!(held(&member_id), harness.join(at(2_999), &member_id).await);

    let response = join_response(&harness.join(at(3_000), &member_id).await);
    assert_eq!(1, response.generation_id);
    assert_eq!(member_id, response.leader);
    Ok(())
}

#[tokio::test]
async fn empty_group_at_a_later_generation_also_waits_for_the_initial_delay() -> Result<()> {
    let mut harness = Harness::new().await?;
    let members = harness.stable_group(1, &[]).await;

    harness.leave(at(5_000), &members[0]).await;
    assert!(harness.member_ids().is_empty());
    assert!(!harness.is_preparing_rebalance());
    assert_eq!(2, harness.group().generation_id());

    let (member_id, reply) = harness.add_member(at(10_000)).await;
    assert_eq!(held(&member_id), reply);
    assert_matches!(
        harness.group().join_phase(),
        Some(JoinPhase {
            initial_delay: Some(_),
            ..
        })
    );

    harness.settle(at(12_999));
    assert!(harness.is_preparing_rebalance());

    harness.settle(at(13_000));
    assert_eq!(3, harness.group().generation_id());
    Ok(())
}

#[tokio::test]
async fn rebalance_ends_once_every_member_has_rejoined() -> Result<()> {
    let mut harness = Harness::new().await?;
    let members = harness.stable_group(2, &[]).await;
    let (leader, follower) = (&members[0], &members[1]);

    // The leader's rejoin starts a rebalance, even with the same metadata.
    assert_eq!(held(leader), harness.join(at(10_000), leader).await);
    assert!(harness.is_preparing_rebalance());
    assert_eq!(1, harness.group().generation_id());

    assert_eq!(
        ErrorCode::RebalanceInProgress,
        harness.heartbeat(at(10_100), follower, 1).await
    );

    // The follower's rejoin ends the phase at once.
    let response = join_response(&harness.join(at(10_200), follower).await);
    assert_eq!(ErrorCode::None, error_code(response.error_code));
    assert_eq!(2, response.generation_id);
    assert_eq!(*leader, response.leader);

    let response = join_response(&harness.join(at(10_300), leader).await);
    assert_eq!(2, response.generation_id);
    assert_eq!(2, response.members.unwrap_or_default().len());
    Ok(())
}

#[tokio::test]
async fn rebalance_waits_for_a_pending_member_id() -> Result<()> {
    let mut harness = Harness::new().await?;
    let members = harness.stable_group(2, &[]).await;

    let pending = harness.new_member_id(at(10_000)).await;
    assert!(
        harness.is_stable(),
        "a pending member id doesn't start a rebalance"
    );

    _ = harness.join(at(10_100), &members[0]).await;
    assert_eq!(
        held(&members[1]),
        harness.join(at(10_200), &members[1]).await
    );

    let response = join_response(&harness.join(at(10_300), &pending).await);
    assert_eq!(2, response.generation_id);
    assert_eq!(3, harness.member_ids().len());
    Ok(())
}

#[tokio::test]
async fn rebalance_timeout_removes_dynamic_members_that_did_not_rejoin_and_keeps_static_ones()
-> Result<()> {
    let mut harness = Harness::new().await?;
    let members = harness.stable_group(2, &["instance"]).await;
    let leader = &members[0];

    let static_member = members
        .iter()
        .find(|member_id| member_id.starts_with("instance-"))
        .expect("the static member")
        .clone();
    let dynamic_follower = members
        .iter()
        .find(|member_id| *member_id != leader && **member_id != static_member)
        .expect("a dynamic follower")
        .clone();

    assert!(!leader.starts_with("instance-"), "the first member leads");

    assert_eq!(held(leader), harness.join(at(10_000), leader).await);
    assert_eq!(Some(at(30_000)), harness.deadline());

    harness.settle(at(29_999));
    assert!(harness.is_preparing_rebalance());

    harness.settle(at(30_000));
    assert_eq!(
        BTreeSet::from([leader.clone(), static_member.clone()]),
        harness.member_ids()
    );
    assert!(!harness.member_ids().contains(&dynamic_follower));
    assert_eq!(2, harness.group().generation_id());

    let response = join_response(&harness.join(at(30_000), leader).await);
    assert_eq!(2, response.members.unwrap_or_default().len());
    Ok(())
}

#[tokio::test]
async fn member_whose_session_expires_during_a_rebalance_is_removed_and_the_rebalance_ends()
-> Result<()> {
    let mut harness = Harness::new().await?;
    harness.rebalance_timeout_ms = 300_000;
    let members = harness.stable_group(2, &[]).await;
    let (leader, crashed) = (&members[0], &members[1]);

    assert_eq!(held(leader), harness.join(at(10_000), leader).await);

    // The crashed member's last contact was its sync at 6 s, and its session timeout is 45 s.
    harness.settle(at(51_000));
    assert!(harness.is_preparing_rebalance());

    harness.settle(at(51_001));
    assert!(!harness.member_ids().contains(crashed));
    assert_eq!(2, harness.group().generation_id());
    Ok(())
}

#[tokio::test]
async fn leave_during_a_rebalance_ends_it_when_every_remaining_member_has_rejoined() -> Result<()> {
    let mut harness = Harness::new().await?;
    let members = harness.stable_group(3, &[]).await;

    _ = harness.join(at(10_000), &members[0]).await;
    assert_eq!(
        held(&members[1]),
        harness.join(at(10_100), &members[1]).await
    );

    harness.leave(at(10_200), &members[2]).await;
    assert!(!harness.is_preparing_rebalance());
    assert_eq!(2, harness.group().generation_id());
    Ok(())
}

#[tokio::test]
async fn pending_member_that_leaves_lets_the_join_phase_end() -> Result<()> {
    let mut harness = Harness::new().await?;
    let members = harness.stable_group(2, &[]).await;

    let pending = harness.new_member_id(at(10_000)).await;
    _ = harness.join(at(10_100), &members[0]).await;
    _ = harness.join(at(10_200), &members[1]).await;
    assert!(harness.is_preparing_rebalance());

    harness.leave(at(10_300), &pending).await;
    assert!(!harness.is_preparing_rebalance());
    assert_eq!(2, harness.group().generation_id());
    Ok(())
}

#[tokio::test]
async fn leave_from_a_stable_group_starts_a_rebalance() -> Result<()> {
    let mut harness = Harness::new().await?;
    let members = harness.stable_group(2, &[]).await;

    harness.leave(at(10_000), &members[1]).await;
    assert!(harness.is_preparing_rebalance());
    assert_eq!(1, harness.group().generation_id());
    assert_eq!(
        ErrorCode::RebalanceInProgress,
        harness.heartbeat(at(10_100), &members[0], 1).await
    );

    let response = join_response(&harness.join(at(10_200), &members[0]).await);
    assert_eq!(2, response.generation_id);
    Ok(())
}

#[tokio::test]
async fn follower_rejoin_with_the_same_metadata_in_a_stable_group_is_answered_at_once() -> Result<()>
{
    let mut harness = Harness::new().await?;
    let members = harness.stable_group(2, &[]).await;
    let before = harness.group().clone();

    let response = join_response(&harness.join(at(10_000), &members[1]).await);
    assert_eq!(ErrorCode::None, error_code(response.error_code));
    assert_eq!(1, response.generation_id);
    assert_eq!(members[0], response.leader);
    assert!(response.members.unwrap_or_default().is_empty());
    assert_eq!(&before, harness.group());
    Ok(())
}

#[tokio::test]
async fn follower_rejoin_with_new_metadata_starts_a_rebalance() -> Result<()> {
    let mut harness = Harness::new().await?;
    let members = harness.stable_group(2, &[]).await;

    assert_eq!(
        held(&members[1]),
        harness
            .join_as(
                at(10_000),
                LATEST_JOIN_GROUP_VERSION,
                &members[1],
                None,
                b"new"
            )
            .await
    );
    assert!(harness.is_preparing_rebalance());
    Ok(())
}

#[tokio::test]
async fn rejoin_with_the_same_metadata_while_awaiting_the_assignment_is_answered_at_once()
-> Result<()> {
    let mut harness = Harness::new().await?;
    let (leader, _) = harness.add_member(at(0)).await;
    let (follower, _) = harness.add_member(at(0)).await;
    harness.settle(at(6_000));
    let before = harness.group().clone();

    for member_id in [&leader, &follower] {
        let response = join_response(&harness.join(at(7_000), member_id).await);
        assert_eq!(1, response.generation_id);
        assert_eq!(leader, response.leader);
    }

    assert_eq!(&before, harness.group());
    Ok(())
}

#[tokio::test]
async fn static_member_restart_takes_its_place_without_a_rebalance() -> Result<()> {
    let mut harness = Harness::new().await?;
    let members = harness.stable_group(1, &["instance"]).await;
    let (leader, old_member_id) = (&members[0], &members[1]);

    let response = join_response(
        &harness
            .join_as(
                at(10_000),
                LATEST_JOIN_GROUP_VERSION,
                "",
                Some("instance"),
                b"m",
            )
            .await,
    );
    assert_eq!(ErrorCode::None, error_code(response.error_code));
    assert_ne!(*old_member_id, response.member_id);
    assert_eq!(1, response.generation_id);
    assert_eq!(*leader, response.leader);
    assert_eq!(Some(false), response.skip_assignment);
    assert!(harness.is_stable());

    let sync = sync_response(&harness.sync(at(10_100), &response.member_id, 1, &[]).await);
    assert_eq!(Harness::assignment(old_member_id), sync.assignment);

    assert_eq!(
        ErrorCode::FencedInstanceId,
        harness
            .heartbeat_as(at(10_200), old_member_id, Some("instance"), 1)
            .await
    );
    Ok(())
}

#[tokio::test]
async fn restarted_static_leader_skips_the_assignment_from_version_9() -> Result<()> {
    for (api_version, skips_assignment) in [(9, true), (8, false)] {
        let mut harness = Harness::new().await?;
        let members = harness.stable_group(0, &["leader", "follower"]).await;
        let old_leader = &members[0];
        assert!(old_leader.starts_with("leader-"));

        let response = join_response(
            &harness
                .join_as(at(10_000), api_version, "", Some("leader"), b"m")
                .await,
        );
        assert!(harness.is_stable());

        if skips_assignment {
            assert_eq!(response.member_id, response.leader);
            assert_eq!(Some(true), response.skip_assignment);
            assert_eq!(2, response.members.unwrap_or_default().len());
        } else {
            assert_eq!(*old_leader, response.leader);
            assert_eq!(Some(false), response.skip_assignment);
            assert!(response.members.unwrap_or_default().is_empty());
        }
    }

    Ok(())
}

#[tokio::test]
async fn follower_sync_is_answered_once_the_leader_assignment_is_stored() -> Result<()> {
    let mut harness = Harness::new().await?;
    let (leader, _) = harness.add_member(at(0)).await;
    let (follower, _) = harness.add_member(at(0)).await;
    harness.settle(at(6_000));

    assert_eq!(
        held(&follower),
        harness.sync(at(6_100), &follower, 1, &[]).await
    );

    let assignments = [&leader, &follower].map(|member_id| {
        SyncGroupRequestAssignment::default()
            .member_id(member_id.clone())
            .assignment(Harness::assignment(member_id))
    });
    let reply = harness.sync(at(6_200), &leader, 1, &assignments).await;
    assert_eq!(
        Harness::assignment(&leader),
        sync_response(&reply).assignment
    );
    assert!(harness.is_stable());

    let reply = harness.sync(at(6_300), &follower, 1, &[]).await;
    assert_eq!(
        Harness::assignment(&follower),
        sync_response(&reply).assignment
    );
    Ok(())
}

#[tokio::test]
async fn leader_assignment_gives_an_unassigned_member_an_empty_assignment() -> Result<()> {
    let mut harness = Harness::new().await?;
    let (leader, _) = harness.add_member(at(0)).await;
    let (follower, _) = harness.add_member(at(0)).await;
    harness.settle(at(6_000));

    let reply = harness.sync(at(6_100), &leader, 1, &[]).await;
    assert_eq!(
        ErrorCode::None,
        error_code(sync_response(&reply).error_code)
    );

    let reply = harness.sync(at(6_200), &follower, 1, &[]).await;
    assert_eq!(
        ErrorCode::None,
        error_code(sync_response(&reply).error_code)
    );
    assert!(sync_response(&reply).assignment.is_empty());
    Ok(())
}

#[tokio::test]
async fn member_that_never_syncs_is_removed_at_the_rebalance_timeout() -> Result<()> {
    let mut harness = Harness::new().await?;
    let (leader, _) = harness.add_member(at(0)).await;
    let (follower, _) = harness.add_member(at(0)).await;
    harness.settle(at(6_000));

    let assignments = [SyncGroupRequestAssignment::default()
        .member_id(leader.clone())
        .assignment(Harness::assignment(&leader))];
    _ = harness.sync(at(6_000), &leader, 1, &assignments).await;
    assert!(harness.is_stable());

    // The follower keeps heartbeating, so only the missing sync removes it.
    assert_eq!(
        ErrorCode::None,
        harness.heartbeat(at(10_000), &follower, 1).await
    );
    assert_eq!(
        ErrorCode::None,
        harness.heartbeat(at(25_999), &follower, 1).await
    );
    assert!(harness.is_stable());

    harness.settle(at(26_000));
    assert!(!harness.member_ids().contains(&follower));
    assert!(harness.is_preparing_rebalance());
    assert_eq!(
        ErrorCode::RebalanceInProgress,
        harness.heartbeat(at(26_100), &leader, 1).await
    );
    Ok(())
}

#[tokio::test]
async fn forming_group_stored_without_a_rebalance_waits_for_every_member_to_rejoin() -> Result<()> {
    let storage = null_storage().await?;

    let member = |member_id: &str| {
        (
            member_id.to_owned(),
            GroupMember {
                join_response: JoinGroupResponseMember::default()
                    .member_id(member_id.to_owned())
                    .metadata(Bytes::from_static(b"m")),
                last_contact: Some(at(1_000)),
                ..GroupMember::default()
            },
        )
    };

    let detail = GroupDetail {
        session_timeout_ms: SESSION_TIMEOUT_MS,
        rebalance_timeout_ms: Some(REBALANCE_TIMEOUT_MS),
        members: BTreeMap::from([member("leader"), member("follower")]),
        generation_id: 3,
        state: GroupState::Forming {
            protocol_type: Some(PROTOCOL_TYPE.into()),
            protocol_name: Some("range".into()),
            leader: Some("leader".into()),
        },
        ..GroupDetail::default()
    };

    let mut harness = Harness {
        group: Some(Wrapper::with_storage_group_detail(storage, detail)),
        rebalance_timeout_ms: REBALANCE_TIMEOUT_MS,
    };

    assert_eq!(Some(at(21_000)), harness.deadline());

    assert_eq!(held("leader"), harness.join(at(2_000), "leader").await);
    let response = join_response(&harness.join(at(2_100), "follower").await);
    assert_eq!(4, response.generation_id);
    Ok(())
}

#[tokio::test]
async fn followers_waiting_for_a_slow_leader_keep_their_sessions() -> Result<()> {
    let mut harness = Harness::new().await?;
    harness.rebalance_timeout_ms = 300_000;
    let (leader, _) = harness.add_member(at(0)).await;
    let (follower, _) = harness.add_member(at(0)).await;
    harness.settle(at(6_000));

    assert_eq!(
        held(&follower),
        harness.sync(at(6_100), &follower, 1, &[]).await
    );

    // The leader heartbeats while it computes the assignment, and the follower can't, because
    // its sync is held.
    assert_eq!(
        ErrorCode::None,
        harness.heartbeat(at(40_000), &leader, 1).await
    );

    let assignments = [&leader, &follower].map(|member_id| {
        SyncGroupRequestAssignment::default()
            .member_id(member_id.clone())
            .assignment(Harness::assignment(member_id))
    });
    _ = harness.sync(at(56_000), &leader, 1, &assignments).await;
    assert!(harness.is_stable());

    let reply = harness.sync(at(56_100), &follower, 1, &[]).await;
    assert_eq!(
        Harness::assignment(&follower),
        sync_response(&reply).assignment
    );
    Ok(())
}

#[tokio::test]
async fn followers_waiting_for_a_leader_that_never_syncs_survive_the_next_rebalance() -> Result<()>
{
    let mut harness = Harness::new().await?;
    let (_, followers) = followers_waiting_for_the_leader(&mut harness).await;

    // The leader never sends SyncGroup, so its session expires 45 s after the join phase ended,
    // and the group rebalances. The followers' syncs are still held, so they haven't heard of the
    // rebalance yet, and they can't heartbeat.
    harness.settle(at(51_001));
    assert!(harness.is_preparing_rebalance());
    assert_eq!(followers, harness.member_ids());

    // A request just after the followers' last contact plus their session timeout must not remove
    // them: each still has a SyncGroup in progress, which Kafka answers at once with
    // REBALANCE_IN_PROGRESS.
    harness.settle(at(51_200));
    assert_eq!(followers, harness.member_ids());

    for follower in &followers {
        let reply = harness.sync(at(51_300), follower, 1, &[]).await;
        assert_eq!(
            ErrorCode::RebalanceInProgress,
            error_code(sync_response(&reply).error_code)
        );
    }

    for follower in &followers {
        _ = harness.join(at(51_400), follower).await;
    }
    assert_eq!(2, harness.group().generation_id());
    assert_eq!(followers, harness.member_ids());
    Ok(())
}

/// Forms a group of three dynamic members whose join phase ends at 6 s, and has each follower
/// send SyncGroup at 6.1 s, where it is held for the leader's assignment. Returns the leader and
/// the followers.
async fn followers_waiting_for_the_leader(harness: &mut Harness) -> (String, BTreeSet<String>) {
    harness.rebalance_timeout_ms = 300_000;
    for _ in 0..3 {
        _ = harness.add_member(at(0)).await;
    }
    harness.settle(at(6_000));

    let leader = harness.group().leader().expect("a leader").to_owned();
    let followers = harness
        .member_ids()
        .into_iter()
        .filter(|member_id| *member_id != leader)
        .collect::<BTreeSet<_>>();

    for follower in &followers {
        assert_eq!(
            held(follower),
            harness.sync(at(6_100), follower, 1, &[]).await
        );
    }

    (leader, followers)
}

#[tokio::test]
async fn followers_waiting_for_a_leader_that_leaves_survive_the_next_rebalance() -> Result<()> {
    let mut harness = Harness::new().await?;
    let (leader, followers) = followers_waiting_for_the_leader(&mut harness).await;

    // The leader keeps its session alive, then leaves without sending SyncGroup, as a Java
    // leader does when its poll interval expires while it computes the assignment.
    assert_eq!(
        ErrorCode::None,
        harness.heartbeat(at(40_000), &leader, 1).await
    );
    harness.leave(at(60_000), &leader).await;
    assert!(harness.is_preparing_rebalance());

    harness.settle(at(60_100));
    assert_eq!(followers, harness.member_ids());
    Ok(())
}

#[tokio::test]
async fn followers_waiting_for_a_leader_that_misses_the_sync_deadline_survive_the_next_rebalance()
-> Result<()> {
    let mut harness = Harness::new().await?;
    let (leader, followers) = followers_waiting_for_the_leader(&mut harness).await;

    // The leader keeps its session alive but never sends SyncGroup, so the coordinator removes
    // it at the sync deadline, a rebalance timeout after the join phase ended.
    for heartbeat_at in (40_000..=280_000).step_by(40_000) {
        assert_eq!(
            ErrorCode::None,
            harness.heartbeat(at(heartbeat_at), &leader, 1).await
        );
    }
    harness.settle(at(306_001));
    assert!(harness.is_preparing_rebalance());
    assert_eq!(followers, harness.member_ids());

    harness.settle(at(306_100));
    assert_eq!(followers, harness.member_ids());
    Ok(())
}

#[tokio::test]
async fn joined_member_outlasts_its_session_timeout_in_a_long_join_phase() -> Result<()> {
    let mut harness = Harness::new().await?;
    harness.rebalance_timeout_ms = 300_000;
    let members = harness.stable_group(2, &[]).await;
    let (leader, follower) = (&members[0], &members[1]);

    assert_eq!(held(leader), harness.join(at(10_000), leader).await);
    assert_eq!(
        ErrorCode::RebalanceInProgress,
        harness.heartbeat(at(30_000), follower, 1).await
    );

    // The leader's last contact was its join at 10 s, and its session timeout is 45 s, but it
    // waits in the join phase, so its session doesn't expire.
    harness.settle(at(56_000));
    assert!(harness.member_ids().contains(leader));
    assert!(harness.is_preparing_rebalance());
    Ok(())
}

#[tokio::test]
async fn leave_during_the_initial_delay_does_not_end_it() -> Result<()> {
    let mut harness = Harness::new().await?;
    let (member_id, _) = harness.add_member(at(0)).await;

    harness.leave(at(1_000), "unknown").await;
    assert!(harness.is_preparing_rebalance());

    let pending = harness.new_member_id(at(1_100)).await;
    harness.leave(at(1_200), &pending).await;
    assert!(harness.is_preparing_rebalance());

    assert_eq!(held(&member_id), harness.join(at(2_999), &member_id).await);
    Ok(())
}

#[tokio::test]
async fn group_whose_members_expire_while_awaiting_the_assignment_becomes_empty() -> Result<()> {
    let mut harness = Harness::new().await?;
    harness.rebalance_timeout_ms = 300_000;
    _ = harness.add_member(at(0)).await;
    _ = harness.add_member(at(0)).await;
    harness.settle(at(6_000));
    assert_eq!(1, harness.group().generation_id());

    // Neither member syncs nor heartbeats, so both sessions expire 45 s after the join phase
    // ended.
    harness.settle(at(51_001));
    assert!(harness.member_ids().is_empty());
    assert!(!harness.is_preparing_rebalance());
    assert_eq!(2, harness.group().generation_id());
    assert_eq!(None, harness.group().protocol_name());
    Ok(())
}

#[tokio::test]
async fn pending_member_id_that_expires_while_awaiting_the_assignment_does_not_rebalance()
-> Result<()> {
    let mut harness = Harness::new().await?;
    harness.rebalance_timeout_ms = 300_000;
    let (leader, _) = harness.add_member(at(0)).await;
    let (follower, _) = harness.add_member(at(0)).await;
    harness.settle(at(6_000));

    _ = harness.new_member_id(at(7_000)).await;

    for member_id in [&leader, &follower] {
        assert_eq!(
            ErrorCode::None,
            harness.heartbeat(at(30_000), member_id, 1).await
        );
        assert_eq!(
            ErrorCode::None,
            harness.heartbeat(at(50_000), member_id, 1).await
        );
    }

    harness.settle(at(52_001));
    assert!(!harness.is_preparing_rebalance());
    assert_eq!(1, harness.group().generation_id());
    Ok(())
}

#[tokio::test]
async fn follower_waiting_for_the_assignment_is_told_to_rejoin_when_the_leader_leaves() -> Result<()>
{
    let mut harness = Harness::new().await?;
    let (leader, _) = harness.add_member(at(0)).await;
    let (follower, _) = harness.add_member(at(0)).await;
    harness.settle(at(6_000));

    assert_eq!(
        held(&follower),
        harness.sync(at(6_100), &follower, 1, &[]).await
    );

    harness.leave(at(6_200), &leader).await;
    assert!(harness.is_preparing_rebalance());

    let reply = harness.sync(at(6_300), &follower, 1, &[]).await;
    assert_eq!(
        ErrorCode::RebalanceInProgress,
        error_code(sync_response(&reply).error_code)
    );
    Ok(())
}

#[tokio::test]
async fn heartbeat_error_codes_follow_the_group_state() -> Result<()> {
    let mut harness = Harness::new().await?;
    let (member_id, _) = harness.add_member(at(0)).await;

    assert_eq!(
        ErrorCode::RebalanceInProgress,
        harness.heartbeat(at(1_000), &member_id, 0).await,
        "the join phase"
    );

    harness.settle(at(3_000));
    assert_eq!(
        ErrorCode::None,
        harness.heartbeat(at(3_100), &member_id, 1).await,
        "awaiting the assignment"
    );
    assert_eq!(
        ErrorCode::IllegalGeneration,
        harness.heartbeat(at(3_200), &member_id, 0).await
    );
    assert_eq!(
        ErrorCode::UnknownMemberId,
        harness.heartbeat(at(3_300), "unknown", 1).await
    );

    _ = harness.sync_all(at(3_400)).await;
    assert_eq!(
        ErrorCode::None,
        harness.heartbeat(at(3_500), &member_id, 1).await,
        "stable"
    );
    Ok(())
}

#[tokio::test]
async fn sync_error_codes_follow_the_group_state() -> Result<()> {
    let mut harness = Harness::new().await?;
    let members = harness.stable_group(2, &[]).await;

    _ = harness.join(at(10_000), &members[0]).await;

    assert_eq!(
        ErrorCode::RebalanceInProgress,
        error_code(sync_response(&harness.sync(at(10_100), &members[1], 1, &[]).await).error_code)
    );
    assert_eq!(
        ErrorCode::IllegalGeneration,
        error_code(sync_response(&harness.sync(at(10_200), &members[1], 0, &[]).await).error_code)
    );
    assert_eq!(
        ErrorCode::UnknownMemberId,
        error_code(sync_response(&harness.sync(at(10_300), "unknown", 1, &[]).await).error_code)
    );
    Ok(())
}

fn offset_commit(generation_id: Option<i32>, member_id: Option<&str>) -> OffsetCommit<'_> {
    OffsetCommit {
        group_id: GROUP,
        generation_id_or_member_epoch: generation_id,
        member_id,
        group_instance_id: None,
        retention_time_ms: None,
        topics: None,
        is_transactional: false,
    }
}

fn offset_commit_fence(
    group: &Wrapper<ArcDynStorage>,
    generation_id: Option<i32>,
    member_id: Option<&str>,
) -> Option<ErrorCode> {
    let detail = offset_commit(generation_id, member_id);

    match group.clone() {
        Wrapper::Forming(mut inner) => {
            let is_completing_rebalance = inner.is_completing_rebalance();
            inner.offset_commit_fence(at(0), &detail, is_completing_rebalance)
        }

        Wrapper::Formed(mut inner) => inner.offset_commit_fence(at(0), &detail, false),
    }
}

#[tokio::test]
async fn offset_commit_is_fenced_as_kafka_fences_it() -> Result<()> {
    let mut harness = Harness::new().await?;
    assert_eq!(
        None,
        offset_commit_fence(harness.group(), None, None),
        "empty"
    );

    let members = harness.stable_group(2, &[]).await;
    let member = Some(members[1].as_str());

    assert_eq!(None, offset_commit_fence(harness.group(), Some(1), member));
    assert_eq!(
        Some(ErrorCode::IllegalGeneration),
        offset_commit_fence(harness.group(), Some(0), member)
    );
    assert_eq!(
        Some(ErrorCode::UnknownMemberId),
        offset_commit_fence(harness.group(), None, None)
    );

    // A member commits the offsets of its revoked partitions in the join phase, at the
    // generation that the join phase hasn't changed yet.
    _ = harness.join(at(10_000), &members[0]).await;
    assert_eq!(None, offset_commit_fence(harness.group(), Some(1), member));

    _ = harness.join(at(10_100), &members[1]).await;
    assert_eq!(
        Some(ErrorCode::RebalanceInProgress),
        offset_commit_fence(harness.group(), Some(2), member),
        "awaiting the assignment"
    );
    Ok(())
}

#[tokio::test]
async fn dynamic_member_of_version_3_joins_without_member_id_required() -> Result<()> {
    let mut harness = Harness::new().await?;

    let Reply::Held { member_id } = harness.join_as(at(0), 3, "", None, b"m").await else {
        panic!("the join of a version 3 member was answered at once");
    };

    assert!(member_id.starts_with("client-"));
    assert_eq!(BTreeSet::from([member_id]), harness.member_ids());
    Ok(())
}

#[tokio::test]
async fn version_0_join_uses_the_session_timeout_as_the_rebalance_timeout() -> Result<()> {
    let storage = null_storage().await?;
    let controller = Controller::new(
        storage.clone(),
        GroupCoordinatorConfig {
            initial_rebalance_delay: Duration::from_millis(10),
            poll_interval: Duration::from_millis(10),
        },
    );

    let protocols = protocols(b"m");
    let body = controller
        .join(
            0,
            Some(CLIENT_ID),
            Some(CLIENT_HOST),
            GROUP,
            SESSION_TIMEOUT_MS,
            None,
            "",
            None,
            PROTOCOL_TYPE,
            Some(&protocols),
            None,
        )
        .await?;

    let response = JoinGroupResponse::try_from(body)?;
    assert_eq!(ErrorCode::None, error_code(response.error_code));
    assert_eq!(response.member_id, response.leader);

    let (detail, _) = storage
        .group_with_version(GROUP)
        .await?
        .expect("the stored group");
    assert_eq!(Some(SESSION_TIMEOUT_MS), detail.rebalance_timeout_ms);
    Ok(())
}

/// `DescribeGroups` reports each member's client id and host from the stored group, so the
/// coordinator must store them when the member joins.
#[tokio::test]
async fn join_stores_the_member_client_id_and_host() -> Result<()> {
    let storage = null_storage().await?;
    let controller = Controller::new(
        storage.clone(),
        GroupCoordinatorConfig {
            initial_rebalance_delay: Duration::from_millis(10),
            poll_interval: Duration::from_millis(10),
        },
    );

    let protocols = protocols(b"m");
    let body = controller
        .join(
            LATEST_JOIN_GROUP_VERSION,
            Some(CLIENT_ID),
            Some(CLIENT_HOST),
            GROUP,
            SESSION_TIMEOUT_MS,
            Some(REBALANCE_TIMEOUT_MS),
            "",
            Some("instance"),
            PROTOCOL_TYPE,
            Some(&protocols),
            None,
        )
        .await?;

    let response = JoinGroupResponse::try_from(body)?;
    assert_eq!(ErrorCode::None, error_code(response.error_code));

    let (detail, _) = storage
        .group_with_version(GROUP)
        .await?
        .expect("the stored group");
    let member = &detail.members[&response.member_id];
    assert_eq!(
        (CLIENT_ID, CLIENT_HOST),
        (member.client_id.as_str(), member.client_host.as_str())
    );
    Ok(())
}

#[tokio::test]
async fn next_deadline_is_the_earliest_change_that_settling_makes() -> Result<()> {
    let storage = null_storage().await?;

    let deadline = |join_phase: Option<JoinPhase>, pending_sync: Option<PendingSync>| {
        let mut group = Inner::new(storage.clone());
        group.state.join_phase = join_phase;
        group.pending_sync = pending_sync;
        _ = group
            .pending_members
            .insert("pending".to_owned(), at(50_000));
        group.next_deadline()
    };

    assert_eq!(Some(at(50_000)), deadline(None, None));
    assert_eq!(
        Some(at(20_000)),
        deadline(
            Some(JoinPhase {
                deadline: at(20_000),
                joined: BTreeSet::new(),
                initial_delay: None,
            }),
            None,
        )
    );
    assert_eq!(
        Some(at(30_000)),
        deadline(
            None,
            Some(PendingSync {
                deadline: at(30_000),
                members: BTreeSet::new(),
            }),
        )
    );
    Ok(())
}
