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

//! The classic group coordinator, which follows the rebalance protocol of Kafka's Java group
//! coordinator ([`GroupMetadataManager`] at 3.9.1), the coordinator of every Kafka 4 broker. A
//! Kafka 3.9.1 broker runs its Scala `GroupCoordinator` by default, which differs in when it
//! extends the initial delay (see `Inner::prepare_initial_rebalance`).
//!
//! A group is Empty, PreparingRebalance, CompletingRebalance or Stable, as in Kafka. [`Forming`]
//! covers the first three, and [`Formed`] is Stable. Kafka keeps a rebalance in progress in the
//! memory of the one broker that coordinates the group. Any Nisshi broker that shares the storage
//! can coordinate a group, so the coordinator stores the rebalance with the group, as
//! [`GroupRebalance`], and the broker that first sees a phase end makes the transition with a
//! conditional write. Kafka runs each timer when it fires. Nisshi makes each change that is due
//! when a request reads the group, so a change can come later than its deadline, by up to the
//! time until the group's next request.
//!
//! [`GroupMetadataManager`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt::{self, Debug},
    hash::{Hash, Hasher},
    marker::PhantomData,
    ops::Deref,
    sync::{Arc, LazyLock, Mutex},
    time::{Instant, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use nisshi_sans_io::{
    Body, DeleteGroupsResponse, ErrorCode,
    consumer::{MemberAssignment, MemberMetadata},
    delete_groups_response::DeletableGroupResult,
    heartbeat_response::HeartbeatResponse,
    join_group_request::JoinGroupRequestProtocol,
    join_group_response::{JoinGroupResponse, JoinGroupResponseMember},
    leave_group_request::MemberIdentity,
    leave_group_response::{LeaveGroupResponse, MemberResponse},
    offset_commit_response::{
        OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
    },
    offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopic},
    offset_fetch_response::{
        OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
        OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
    },
    sync_group_request::SyncGroupRequestAssignment,
    sync_group_response::SyncGroupResponse,
};
use nisshi_storage::{
    CommittedOffset, GroupDetail, GroupDetailResponse, GroupMember, GroupRebalance, GroupState,
    InitialDelay, JoinPhase, NamedGroupDetail, OffsetCommitRequest, PendingSync, Storage, Topition,
    UpdateError, Version, describe_groups_response, list_groups_response, offset_metadata,
};
use opentelemetry::{KeyValue, metrics::Counter};
use tokio::{
    sync::Notify,
    time::{Duration, sleep},
};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::{Error, METER, Result};

use super::{Coordinator, OffsetCommit};

mod subscription;

use subscription::{ChangeMap, Subscription};

// Kafka's own `group.max.session.timeout.ms` default. There is no corresponding
// nisshi config, so we hardcode it. We deliberately do NOT enforce Kafka's
// `group.min.session.timeout.ms` default (6000): librdkafka's own compat test
// suite (e.g. 0106-cgrp_sess_timeout.c, 0113-cooperative_rebalance.cpp) uses
// session.timeout.ms as low as 5000. The actual bug class is non-positive
// values, so the floor here is simply "greater than zero".
const MAX_SESSION_TIMEOUT_MS: i32 = 1_800_000;

/// The session timeout that the coordinator uses for a group stored with a session timeout that
/// is not positive. `JoinGroup` rejects such a timeout, but a group stored by an older broker can
/// still carry one.
const FALLBACK_SESSION_TIMEOUT: Duration = Duration::from_secs(45);

/// The first `JoinGroup` version that answers a dynamic member without a member id with
/// `MEMBER_ID_REQUIRED`, as Kafka's [`requiresKnownMemberId`] decides. An older client
/// gets its member id with the response that ends the join phase.
///
/// [`requiresKnownMemberId`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/JoinGroupRequest.java#L100
const MEMBER_ID_REQUIRED_VERSION: i16 = 4;

/// The first `JoinGroup` version whose response can tell a restarted static leader to skip the
/// assignment, as Kafka's [`supportsSkippingAssignment`] decides.
///
/// [`supportsSkippingAssignment`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/JoinGroupRequest.java#L154
const SKIP_ASSIGNMENT_VERSION: i16 = 9;

static COORDINATOR_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_group_coordinator_requests")
        .with_description("consumer group coordinator requests")
        .build()
});

/// The settings of the group coordinator.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GroupCoordinatorConfig {
    /// How long the first join phase of an empty group lasts, so that other members can join the
    /// same generation, as Kafka's `group.initial.rebalance.delay.ms` sets it.
    pub initial_rebalance_delay: Duration,

    /// How long a held `JoinGroup` or `SyncGroup` waits at most before it reads the group from
    /// storage again. A change by this broker wakes the request at once, and a change by another
    /// broker that shares the storage reaches it only by this read.
    pub poll_interval: Duration,
}

impl Default for GroupCoordinatorConfig {
    fn default() -> Self {
        Self {
            initial_rebalance_delay: Duration::from_secs(3),
            poll_interval: Duration::from_millis(500),
        }
    }
}

/// A `JoinGroup` request, with the values that the coordinator has resolved.
#[derive(Clone, Copy, Debug)]
pub struct JoinRequest<'a> {
    pub api_version: i16,
    pub client_id: Option<&'a str>,
    /// The client's address, as `DescribeGroups` reports it.
    pub client_host: Option<&'a str>,
    pub group_id: &'a str,
    pub session_timeout_ms: i32,
    /// The request's rebalance timeout, or its session timeout for version 0, which has no
    /// rebalance timeout.
    pub rebalance_timeout_ms: i32,
    pub member_id: &'a str,
    pub group_instance_id: Option<&'a str>,
    pub protocol_type: &'a str,
    pub protocols: Option<&'a [JoinGroupRequestProtocol]>,
    pub reason: Option<&'a str>,
}

/// A `SyncGroup` request.
#[derive(Clone, Copy, Debug)]
pub struct SyncRequest<'a> {
    pub group_id: &'a str,
    pub generation_id: i32,
    pub member_id: &'a str,
    pub group_instance_id: Option<&'a str>,
    pub protocol_type: Option<&'a str>,
    pub protocol_name: Option<&'a str>,
    pub assignments: Option<&'a [SyncGroupRequestAssignment]>,
}

/// The coordinator's answer to a `JoinGroup` or a `SyncGroup`.
#[derive(Clone, Debug, PartialEq)]
pub enum Reply {
    /// The response to send now.
    Ready(Body),

    /// The coordinator holds the request until the group's phase changes, and then runs it again
    /// for `member_id`. A `JoinGroup` without a member id gets one here.
    Held { member_id: String },
}

#[async_trait]
pub trait Group: Debug + Send {
    type JoinState;
    type SyncState;
    type HeartbeatState;
    type LeaveState;
    type OffsetCommitState;
    type OffsetFetchState;

    async fn join(
        self,
        now: SystemTime,
        request: &JoinRequest<'_>,
        initial_rebalance_delay: Duration,
    ) -> (Self::JoinState, Reply);

    async fn sync(self, now: SystemTime, request: &SyncRequest<'_>) -> (Self::SyncState, Reply);

    async fn heartbeat(
        self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Self::HeartbeatState, Body);

    async fn leave(
        self,
        now: SystemTime,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Self::LeaveState, Body);

    /// Checks an `OffsetCommit` against the group, and records the committing member's contact.
    /// Returns the error code that rejects the commit, or `None` when the commit may store its
    /// offsets. The check stores no offsets: the coordinator stores them only after its
    /// conditional write of the group lands, so that it never stores offsets of a member that
    /// another broker has removed.
    fn check_offset_commit(
        self,
        now: SystemTime,
        detail: &OffsetCommit<'_>,
    ) -> (Self::OffsetCommitState, Option<ErrorCode>);

    async fn offset_fetch(
        self,
        now: SystemTime,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Self::OffsetFetchState, Body);
}

#[derive(Clone, Debug)]
pub enum Wrapper<O> {
    Forming(Inner<O, Forming>),
    Formed(Inner<O, Formed>),
}

impl<O> fmt::Display for Wrapper<O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Wrapper::Forming(inner) => write!(f, "wrap: {}", inner),
            Wrapper::Formed(inner) => write!(f, "wrap: {}", inner),
        }
    }
}

impl<O> PartialEq for Wrapper<O> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Forming(sinner), Self::Forming(oinner)) => sinner == oinner,
            (Self::Formed(sinner), Self::Formed(oinner)) => sinner == oinner,
            _ => false,
        }
    }
}

impl<O> Hash for Wrapper<O>
where
    O: Storage,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Forming(inner) => {
                state.write_u8(3);
                inner.hash(state)
            }
            Self::Formed(inner) => {
                state.write_u8(5);
                inner.hash(state)
            }
        }
    }
}

impl<O> From<Inner<O, Forming>> for Wrapper<O>
where
    O: Storage,
{
    fn from(value: Inner<O, Forming>) -> Self {
        Self::Forming(value)
    }
}

impl<O> From<Inner<O, Formed>> for Wrapper<O>
where
    O: Storage,
{
    fn from(value: Inner<O, Formed>) -> Self {
        Self::Formed(value)
    }
}

impl<O> From<&Wrapper<O>> for GroupDetail
where
    O: Storage,
{
    fn from(value: &Wrapper<O>) -> Self {
        match value {
            Wrapper::Forming(inner) => GroupDetail {
                session_timeout_ms: inner.session_timeout_ms,
                rebalance_timeout_ms: inner.rebalance_timeout_ms,
                members: inner
                    .members
                    .iter()
                    .map(|(member_id, member)| (member_id.clone(), member.into()))
                    .collect(),
                generation_id: inner.generation_id,
                skip_assignment: inner.skip_assignment,
                inception: inner.inception,
                state: GroupState::Forming {
                    protocol_type: inner.state.protocol_type().map(ToOwned::to_owned),
                    protocol_name: inner.state.protocol_name().map(ToOwned::to_owned),
                    leader: inner.state.leader().map(ToOwned::to_owned),
                },
                rebalance: Box::new(GroupRebalance {
                    join_phase: inner.state.join_phase.clone(),
                    pending_sync: inner.pending_sync.clone(),
                    pending_members: inner.pending_members.clone(),
                }),
            },

            Wrapper::Formed(inner) => GroupDetail {
                session_timeout_ms: inner.session_timeout_ms,
                rebalance_timeout_ms: inner.rebalance_timeout_ms,
                members: inner
                    .members
                    .iter()
                    .map(|(member_id, member)| (member_id.clone(), member.into()))
                    .collect(),
                generation_id: inner.generation_id,
                skip_assignment: inner.skip_assignment,
                inception: inner.inception,
                state: GroupState::Formed {
                    protocol_type: inner.state.protocol_type.clone(),
                    protocol_name: inner.state.protocol_name.clone(),
                    leader: inner.state.leader.clone(),
                    assignments: inner.state.assignments.clone(),
                },
                rebalance: Box::new(GroupRebalance {
                    join_phase: None,
                    pending_sync: inner.pending_sync.clone(),
                    pending_members: inner.pending_members.clone(),
                }),
            },
        }
    }
}

impl<O> Wrapper<O>
where
    O: Storage,
{
    pub fn with_storage_group_detail(storage: O, gd: GroupDetail) -> Self {
        let GroupRebalance {
            join_phase,
            pending_sync,
            pending_members,
        } = *gd.rebalance;

        match gd.state {
            GroupState::Forming {
                protocol_type,
                protocol_name,
                mut leader,
            } => {
                if let Some(ref leader_id) = leader
                    && !gd.members.contains_key(leader_id)
                {
                    _ = leader.take();
                }

                let mut inner = Inner {
                    session_timeout_ms: gd.session_timeout_ms,
                    rebalance_timeout_ms: gd.rebalance_timeout_ms,
                    members: gd
                        .members
                        .iter()
                        .map(|(member_id, member)| (member_id.clone(), member.into()))
                        .collect(),
                    generation_id: gd.generation_id,
                    state: Forming {
                        join_phase,
                        ..Forming::from_parts(protocol_type, protocol_name, leader)
                    },
                    storage,
                    skip_assignment: gd.skip_assignment,
                    inception: gd.inception,
                    pending_members,
                    pending_sync,
                };

                // A broker that stored the group without its rebalance state held each request
                // for a fixed time. Its forming group with members has no deadline, so the
                // coordinator gives it a join phase that every member must rejoin. The deadline
                // comes from the stored group, so that each read of the group finds the same one.
                if inner.state.join_phase.is_none()
                    && inner.pending_sync.is_none()
                    && !inner.members.is_empty()
                {
                    let last_contact = inner
                        .members
                        .values()
                        .filter_map(|member| member.last_contact)
                        .max()
                        .unwrap_or(inner.inception);

                    inner.state.join_phase = Some(JoinPhase {
                        deadline: last_contact + inner.rebalance_timeout(),
                        joined: BTreeSet::new(),
                        initial_delay: None,
                    });
                }

                Self::Forming(inner)
            }

            GroupState::Formed {
                protocol_type,
                protocol_name,
                leader,
                assignments,
            } => Self::Formed(Inner {
                session_timeout_ms: gd.session_timeout_ms,
                rebalance_timeout_ms: gd.rebalance_timeout_ms,
                members: gd
                    .members
                    .iter()
                    .map(|(member_id, member)| (member_id.clone(), member.into()))
                    .collect(),
                generation_id: gd.generation_id,
                state: Formed {
                    protocol_type,
                    protocol_name,
                    leader,
                    assignments,
                },
                storage,
                skip_assignment: gd.skip_assignment,
                inception: gd.inception,
                pending_members,
                pending_sync,
            }),
        }
    }

    pub fn generation_id(&self) -> i32 {
        match self {
            Self::Forming(inner) => inner.generation_id,
            Self::Formed(inner) => inner.generation_id,
        }
    }

    pub fn inception(&self) -> SystemTime {
        match self {
            Self::Forming(inner) => inner.inception,
            Self::Formed(inner) => inner.inception,
        }
    }

    pub fn session_timeout_ms(&self) -> i32 {
        match self {
            Self::Forming(inner) => inner.session_timeout_ms,
            Self::Formed(inner) => inner.session_timeout_ms,
        }
    }

    pub fn rebalance_timeout_ms(&self) -> Option<i32> {
        match self {
            Self::Forming(inner) => inner.rebalance_timeout_ms,
            Self::Formed(inner) => inner.rebalance_timeout_ms,
        }
    }

    pub fn protocol_type(&self) -> Option<&str> {
        match self {
            Self::Forming(inner) => inner.state.protocol_type(),
            Self::Formed(inner) => Some(inner.state.protocol_type.as_str()),
        }
    }

    pub fn protocol_name(&self) -> Option<&str> {
        match self {
            Self::Forming(inner) => inner.state.protocol_name(),
            Self::Formed(inner) => Some(inner.state.protocol_name.as_str()),
        }
    }

    pub fn leader(&self) -> Option<&str> {
        match self {
            Self::Forming(inner) => inner.state.leader(),
            Self::Formed(inner) => Some(inner.state.leader.as_str()),
        }
    }

    pub fn skip_assignment(&self) -> Option<&bool> {
        match self {
            Self::Forming(inner) => inner.skip_assignment.as_ref(),
            Self::Formed(inner) => inner.skip_assignment.as_ref(),
        }
    }

    fn rebalance_timeout(&self) -> Duration {
        match self {
            Self::Forming(inner) => inner.rebalance_timeout(),
            Self::Formed(inner) => inner.rebalance_timeout(),
        }
    }

    /// Returns the hold limit of a request to this group: the longest time that the coordinator
    /// holds the request, twice the larger of the group's rebalance timeout and
    /// `rebalance_timeout_ms`. Each phase of a rebalance ends by its deadline, within the
    /// rebalance timeout, so a request that waits twice as long shows a group that a fault left
    /// without a deadline. The coordinator then answers the request, so that its client rejoins,
    /// instead of holding it forever.
    fn hold_limit(&self, rebalance_timeout_ms: Option<i32>) -> Duration {
        rebalance_timeout_ms
            .and_then(positive_millis)
            .unwrap_or_default()
            .max(self.rebalance_timeout())
            .saturating_mul(2)
    }

    /// Returns the members and the pending member ids that the join phase waits for.
    fn waiting_for(&self) -> Vec<String> {
        match self {
            Self::Forming(inner) => inner
                .state
                .join_phase
                .as_ref()
                .map(|join_phase| {
                    inner
                        .members
                        .keys()
                        .filter(|member_id| !join_phase.joined.contains(*member_id))
                        .chain(inner.pending_members.keys())
                        .cloned()
                        .collect()
                })
                .unwrap_or_default(),

            Self::Formed(_) => Vec::new(),
        }
    }

    /// Returns the join phase in progress, if the group is in Kafka's `PreparingRebalance`.
    pub fn join_phase(&self) -> Option<&JoinPhase> {
        match self {
            Self::Forming(inner) => inner.state.join_phase.as_ref(),
            Self::Formed(_) => None,
        }
    }

    fn members(&self) -> Vec<JoinGroupResponseMember> {
        match self {
            Self::Forming(inner) => inner.member_list(),
            Self::Formed(inner) => inner.member_list(),
        }
    }

    /// Makes each change that the passing of time causes by `now`: the coordinator forgets each
    /// pending member id and removes each member whose session timeout has expired, ends a join
    /// phase whose deadline has passed, and removes each member that has not sent `SyncGroup` by
    /// the sync deadline.
    ///
    /// Kafka makes these changes with a timer on the broker that coordinates the group. Nisshi has
    /// no such broker, so each request makes the changes that are due when it reads the group.
    #[instrument(skip(self, now))]
    pub fn settle(self, group_id: &str, now: SystemTime) -> Self {
        match self {
            Self::Forming(mut inner) => {
                let was_completing_rebalance = inner.is_completing_rebalance();
                let forgot_pending = inner.forget_pending_members(now);
                let removed_members = inner.remove_expired(now);

                if inner.state.join_phase.is_some() {
                    // Kafka ends the join phase when an expiry leaves only joined members, also
                    // during the initial delay.
                    if forgot_pending || removed_members {
                        inner.complete_join_if_all_joined(now);
                    }

                    inner.end_join_phase_after_deadline(now);
                } else if was_completing_rebalance
                    && (removed_members || inner.remove_members_pending_sync(now))
                {
                    // A group whose last member is removed ends the rebalance at once, and becomes
                    // empty at the next generation.
                    inner.forget_removed_members();
                    inner.prepare_rebalance(now);
                    inner.complete_join_if_all_joined(now);
                }

                Self::Forming(inner)
            }

            Self::Formed(mut inner) => {
                _ = inner.forget_pending_members(now);

                if inner.remove_expired(now) | inner.remove_members_pending_sync(now) {
                    let mut forming = inner.into_preparing_rebalance(now);
                    forming.complete_join_if_all_joined(now);
                    Self::Forming(forming)
                } else {
                    Self::Formed(inner)
                }
            }
        }
    }

    /// Returns the earliest time at which [`Wrapper::settle`] would change the group.
    fn next_deadline(&self) -> Option<SystemTime> {
        match self {
            Self::Forming(inner) => inner.next_deadline(),
            Self::Formed(inner) => inner.next_deadline(),
        }
    }

    /// Stores the offsets of `detail`, a commit that [`Group::check_offset_commit`] accepted, and
    /// returns the response.
    async fn commit_offset(&mut self, detail: &OffsetCommit<'_>) -> Result<Body> {
        match self {
            Self::Forming(inner) => inner.commit_offset(detail).await,
            Self::Formed(inner) => inner.commit_offset(detail).await,
        }
    }
}

#[async_trait]
impl<O> Group for Wrapper<O>
where
    O: Storage,
{
    type JoinState = Wrapper<O>;
    type SyncState = Wrapper<O>;
    type HeartbeatState = Wrapper<O>;
    type LeaveState = Wrapper<O>;
    type OffsetCommitState = Wrapper<O>;
    type OffsetFetchState = Wrapper<O>;

    async fn join(
        self,
        now: SystemTime,
        request: &JoinRequest<'_>,
        initial_rebalance_delay: Duration,
    ) -> (Wrapper<O>, Reply) {
        match self {
            Self::Forming(inner) => {
                let (state, reply) = inner.join(now, request, initial_rebalance_delay).await;
                (state.into(), reply)
            }

            Self::Formed(inner) => inner.join(now, request, initial_rebalance_delay).await,
        }
    }

    async fn sync(self, now: SystemTime, request: &SyncRequest<'_>) -> (Wrapper<O>, Reply) {
        match self {
            Wrapper::Forming(inner) => inner.sync(now, request).await,

            Wrapper::Formed(inner) => {
                let (state, reply) = inner.sync(now, request).await;
                (state.into(), reply)
            }
        }
    }

    async fn leave(
        self,
        now: SystemTime,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Wrapper<O>, Body) {
        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner.leave(now, group_id, member_id, members).await;
                (state.into(), body)
            }

            Wrapper::Formed(inner) => inner.leave(now, group_id, member_id, members).await,
        }
    }

    fn check_offset_commit(
        self,
        now: SystemTime,
        detail: &OffsetCommit<'_>,
    ) -> (Wrapper<O>, Option<ErrorCode>) {
        match self {
            Wrapper::Forming(inner) => {
                let (state, error_code) = inner.check_offset_commit(now, detail);
                (state.into(), error_code)
            }

            Wrapper::Formed(inner) => {
                let (state, error_code) = inner.check_offset_commit(now, detail);
                (state.into(), error_code)
            }
        }
    }

    async fn offset_fetch(
        self,
        now: SystemTime,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Wrapper<O>, Body) {
        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner
                    .offset_fetch(now, group_id, topics, groups, require_stable)
                    .await;
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner
                    .offset_fetch(now, group_id, topics, groups, require_stable)
                    .await;
                (state.into(), body)
            }
        }
    }

    async fn heartbeat(
        self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Wrapper<O>, Body) {
        debug!(
            ?now,
            ?group_id,
            ?generation_id,
            ?member_id,
            ?group_instance_id
        );

        match self {
            Wrapper::Forming(inner) => {
                let (state, body) = inner
                    .heartbeat(now, group_id, generation_id, member_id, group_instance_id)
                    .await;
                (state.into(), body)
            }

            Wrapper::Formed(inner) => {
                let (state, body) = inner
                    .heartbeat(now, group_id, generation_id, member_id, group_instance_id)
                    .await;
                (state.into(), body)
            }
        }
    }
}

/// A group as this coordinator last learned it from storage, by a read or a write.
#[derive(Clone, Debug)]
struct Learned<O> {
    group: Wrapper<O>,
    version: Option<Version>,
    at: Instant,
}

type WrapperMap<O> = Arc<Mutex<BTreeMap<String, Learned<O>>>>;

/// Where a request found the group that it runs on.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Source {
    Cache,
    Storage,
}

#[derive(Clone, Debug)]
pub struct Controller<O> {
    storage: O,
    config: GroupCoordinatorConfig,
    wrappers: WrapperMap<O>,
    /// Wakes the requests that this coordinator holds for a group, after each change that it
    /// stores for the group.
    changes: ChangeMap,
}

impl<O> Controller<O>
where
    O: Storage + Clone,
{
    pub fn new(storage: O, config: GroupCoordinatorConfig) -> Self {
        Self {
            storage,
            config,
            wrappers: Arc::new(Mutex::new(BTreeMap::new())),
            changes: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn with_storage(storage: O) -> Result<Self> {
        Ok(Self::new(storage, GroupCoordinatorConfig::default()))
    }

    /// Returns a hold on the group's notifier, for a request that waits for a change of the group.
    fn subscribe(&self, group_id: &str) -> Result<Subscription> {
        self.changes
            .lock()
            .map(|mut changes| {
                let notify = changes
                    .entry(group_id.to_owned())
                    .or_insert_with(|| Arc::new(Notify::new()))
                    .clone();

                Subscription::new(&self.changes, group_id, notify)
            })
            .map_err(Into::into)
    }

    /// Wakes the requests that wait on the group. A group without an entry has no waiting request,
    /// because each request adds the entry before it waits.
    fn wake_waiters(&self, group_id: &str) -> Result<()> {
        self.changes
            .lock()
            .map(|changes| {
                if let Some(notify) = changes.get(group_id) {
                    notify.notify_waiters();
                }
            })
            .map_err(Into::into)
    }

    /// Returns the cached group if this coordinator learned it less than `max_age` ago.
    fn cached(
        &self,
        group_id: &str,
        max_age: Duration,
    ) -> Result<Option<(Wrapper<O>, Option<Version>)>> {
        self.wrappers
            .lock()
            .map(|wrappers| {
                wrappers
                    .get(group_id)
                    .filter(|learned| learned.at.elapsed() < max_age)
                    .map(|learned| (learned.group.clone(), learned.version.clone()))
            })
            .map_err(Into::into)
    }

    fn cache(&self, group_id: &str, group: Wrapper<O>, version: Option<Version>) -> Result<()> {
        self.wrappers
            .lock()
            .map(|mut wrappers| {
                _ = wrappers.insert(
                    group_id.to_owned(),
                    Learned {
                        group,
                        version,
                        at: Instant::now(),
                    },
                )
            })
            .map_err(Into::into)
    }

    /// Returns the group as storage holds it, or an empty group if storage holds none. The
    /// coordinator caches only a group that storage holds, because a client chooses the group id.
    async fn stored_group(&self, group_id: &str) -> Result<(Wrapper<O>, Option<Version>)> {
        let stored = match self.storage.group_with_version(group_id).await? {
            Some((detail, version)) => (
                Wrapper::with_storage_group_detail(self.storage.clone(), detail),
                Some(version),
            ),

            None => return Ok((Wrapper::Forming(Inner::new(self.storage.clone())), None)),
        };

        self.cache(group_id, stored.0.clone(), stored.1.clone())?;
        Ok(stored)
    }

    /// Returns the group as this coordinator learned it less than `max_age` ago, or as storage
    /// holds it.
    async fn recent_group(
        &self,
        group_id: &str,
        max_age: Duration,
    ) -> Result<(Wrapper<O>, Option<Version>, Source)> {
        match self.cached(group_id, max_age)? {
            Some((group, version)) => Ok((group, version, Source::Cache)),
            None => self
                .stored_group(group_id)
                .await
                .map(|(group, version)| (group, version, Source::Storage)),
        }
    }

    /// Stores `wrapper` as the group's state if storage still holds `version`. Returns `false`,
    /// and caches the group that storage holds, if it holds another version.
    async fn store_group(
        &self,
        group_id: &str,
        wrapper: &Wrapper<O>,
        version: Option<Version>,
        method: &'static str,
    ) -> Result<bool> {
        match self
            .storage
            .update_group(group_id, GroupDetail::from(wrapper), version)
            .await
        {
            Ok(version) => {
                debug!(group_id, ?version, method);
                self.cache(group_id, wrapper.clone(), Some(version))?;
                self.wake_waiters(group_id)?;
                Ok(true)
            }

            Err(UpdateError::Outdated { current, version }) => {
                debug!(group_id, ?current, ?version, method);
                COORDINATOR_REQUESTS
                    .add(1, &[KeyValue::new("method", format!("{method}_outdated"))]);

                self.cache(
                    group_id,
                    Wrapper::with_storage_group_detail(self.storage.clone(), *current),
                    Some(version),
                )?;

                Ok(false)
            }

            Err(UpdateError::Error(error)) => Err(error.into()),
            Err(UpdateError::SerdeJson(error)) => Err(error.into()),
            Err(UpdateError::MissingEtag) => Err(Error::Message(String::from("missing e-tag"))),
            Err(UpdateError::Uuid(uuid)) => Err(Error::Message(format!("uuid: {uuid}"))),
        }
    }

    /// Waits until this coordinator stores a change to the group, until the group's next
    /// deadline, or for the poll interval, whichever comes first.
    ///
    /// A held request then runs on the group that this coordinator learned within the poll
    /// interval, if it has one, so that the held requests of a group on one broker share a read
    /// of storage instead of each making one, and a change stored here reaches them without a
    /// read. A change by another broker reaches them by the next read, within the poll interval.
    async fn wait_for_change(&self, changed: impl Future<Output = ()>, group: &Wrapper<O>) {
        let now = SystemTime::now();
        let until_deadline = group
            .next_deadline()
            .map(|deadline| deadline.duration_since(now).unwrap_or_default())
            .unwrap_or(self.config.poll_interval);

        tokio::select! {
            () = changed => {}
            () = sleep(until_deadline.min(self.config.poll_interval)) => {}
        }
    }
}

#[async_trait]
impl<O> Coordinator for Controller<O>
where
    O: Storage + Clone,
{
    #[instrument(skip(
        self,
        client_id,
        client_host,
        session_timeout_ms,
        rebalance_timeout_ms,
        group_instance_id,
        protocol_type,
        protocols,
        reason
    ))]
    async fn join(
        &self,
        api_version: i16,
        client_id: Option<&str>,
        client_host: Option<&str>,
        group_id: &str,
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: Option<&[JoinGroupRequestProtocol]>,
        reason: Option<&str>,
    ) -> Result<Body> {
        debug!(
            api_version,
            ?client_id,
            ?client_host,
            ?session_timeout_ms,
            ?rebalance_timeout_ms,
            ?group_instance_id,
            ?protocol_type,
            protocols = ?protocols
                .map(|protocols| {
                    protocols
                        .iter()
                        .filter_map(|protocol| {
                            MemberMetadata::try_from(protocol.metadata.clone())
                                .ok()
                                .map(|metadata| (protocol.name.clone(), metadata.to_string()))
                        })
                        .collect::<BTreeMap<_, _>>()
                })
                .unwrap_or_default(),
            ?reason,
        );

        // Validate before touching any group state or storage: a session timeout outside
        // this range must never be persisted, because nothing downstream re-validates it.
        if session_timeout_ms <= 0 || session_timeout_ms > MAX_SESSION_TIMEOUT_MS {
            debug!(join_outcome = ?ErrorCode::InvalidSessionTimeout, session_timeout_ms);

            COORDINATOR_REQUESTS.add(
                1,
                &[KeyValue::new("method", "join_invalid_session_timeout")],
            );

            let join_group_response = JoinGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::InvalidSessionTimeout.into())
                .generation_id(-1)
                .protocol_type(None)
                .protocol_name(Some("".into()))
                .leader("".into())
                .skip_assignment(Some(false))
                .member_id(member_id.to_owned())
                .members(Some([].into()));

            return Ok(join_group_response.into());
        }

        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join")]);

        let mut member_id = member_id.to_owned();
        let mut held_since = None;
        let subscription = self.subscribe(group_id)?;
        let mut max_age = Duration::ZERO;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join_loop")]);

            // The request registers for a wake-up before it reads the group, so that it can't
            // miss a change stored between the read and the wait.
            let changed = subscription.notified();
            tokio::pin!(changed);
            _ = changed.as_mut().enable();

            let (stored, version, _) = self.recent_group(group_id, max_age).await?;
            let now = SystemTime::now();

            let request = JoinRequest {
                api_version,
                client_id,
                client_host,
                group_id,
                session_timeout_ms,
                rebalance_timeout_ms: rebalance_timeout_ms.unwrap_or(session_timeout_ms),
                member_id: &member_id,
                group_instance_id,
                protocol_type,
                protocols,
                reason,
            };

            let (updated, reply) = stored
                .clone()
                .settle(group_id, now)
                .join(now, &request, self.config.initial_rebalance_delay)
                .await;

            debug!(%updated, ?version, ?reply);

            if updated != stored
                && !self
                    .store_group(group_id, &updated, version, "join")
                    .await?
            {
                max_age = self.config.poll_interval;
                continue;
            }

            match reply {
                Reply::Ready(body) => return Ok(body),

                Reply::Held { member_id: held } => {
                    COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "join_held")]);

                    if member_id != held || held_since.is_none() {
                        info!(
                            group_id,
                            member_id = held,
                            waiting_for = ?updated.waiting_for(),
                            deadline = ?updated.join_phase().map(|join_phase| join_phase.deadline),
                            "holding the join until the join phase ends"
                        );
                    }

                    let held_since = *held_since.get_or_insert_with(Instant::now);

                    if held_since.elapsed() > updated.hold_limit(rebalance_timeout_ms) {
                        warn!(
                            group_id,
                            member_id = held,
                            "the join phase didn't end in time"
                        );
                        return Ok(rebalance_in_progress_join(&held));
                    }

                    member_id = held;
                    self.wait_for_change(changed, &updated).await;
                    max_age = self.config.poll_interval;
                }
            }
        }
    }

    #[instrument(skip(self, group_instance_id, protocol_type, protocol_name, assignments))]
    async fn sync(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: Option<&str>,
        protocol_name: Option<&str>,
        assignments: Option<&[SyncGroupRequestAssignment]>,
    ) -> Result<Body> {
        debug!(
            ?group_instance_id,
            ?protocol_type,
            ?protocol_name,
            assignments = ?assignments
                .map(|assignments| {
                    assignments
                        .iter()
                        .filter_map(|request| {
                            MemberAssignment::try_from(request.assignment.clone())
                                .ok()
                                .map(|member_assignment| {
                                    (request.member_id.clone(), member_assignment.to_string())
                                })
                        })
                        .collect::<BTreeMap<_, _>>()
                })
                .unwrap_or_default(),
        );

        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "sync")]);

        if let Some(assignments) = assignments
            && !assignments.is_empty()
            && !has_unique_elements(
                assignments
                    .iter()
                    .map(|assignment| assignment.assignment.clone())
                    .filter_map(|assignment| MemberAssignment::try_from(assignment).ok())
                    .map(|ma| ma.assignment)
                    .map(|cpa| cpa.assigned_partitions)
                    .flat_map(|tp| tp.into_iter())
                    .map(|tp| (tp.topic, tp.partitions))
                    .flat_map(|(topic, partitions)| {
                        partitions
                            .into_iter()
                            .map(move |partition| (topic.clone(), partition))
                    }),
            )
        {
            warn!(
                group_id,
                generation_id,
                member_count = assignments.len(),
                non_unique_assignment = assignments
                    .iter()
                    .map(|assignment| (assignment.member_id.clone(), assignment.assignment.clone()))
                    .filter_map(|(member_id, assignment)| {
                        MemberAssignment::try_from(assignment)
                            .ok()
                            .map(|ma| (member_id, ma))
                    })
                    .map(|(member, ma)| format!("{member}: {ma}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        let request = SyncRequest {
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            protocol_type,
            protocol_name,
            assignments,
        };

        let subscription = self.subscribe(group_id)?;
        let mut max_age = Duration::ZERO;
        let held_since = Instant::now();

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "sync_loop")]);

            let changed = subscription.notified();
            tokio::pin!(changed);
            _ = changed.as_mut().enable();

            let (stored, version, _) = self.recent_group(group_id, max_age).await?;
            let now = SystemTime::now();

            let (updated, reply) = stored
                .clone()
                .settle(group_id, now)
                .sync(now, &request)
                .await;

            debug!(group_id, %updated, ?version, ?reply);

            if updated != stored
                && !self
                    .store_group(group_id, &updated, version, "sync")
                    .await?
            {
                max_age = self.config.poll_interval;
                continue;
            }

            match reply {
                Reply::Ready(body) => return Ok(body),

                Reply::Held { .. } => {
                    COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "sync_held")]);

                    if held_since.elapsed() > updated.hold_limit(None) {
                        warn!(
                            group_id,
                            member_id, "the leader's assignment didn't arrive in time"
                        );
                        return Ok(rebalance_in_progress_sync());
                    }

                    self.wait_for_change(changed, &updated).await;
                    max_age = self.config.poll_interval;
                }
            }
        }
    }

    #[instrument(skip(self, members))]
    async fn leave(
        &self,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> Result<Body> {
        debug!(?members);

        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "leave")]);

        let mut max_age = Duration::MAX;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "leave_loop")]);

            let (original, version, source) = self.recent_group(group_id, max_age).await?;
            let now = SystemTime::now();

            let (wrapper, body) = original
                .clone()
                .settle(group_id, now)
                .leave(now, group_id, member_id, members)
                .await;

            if wrapper == original {
                // An answer that changes nothing needs no write. The cache can be older than
                // another broker's change, so an answer from the cache is checked on storage.
                match source {
                    Source::Cache => {
                        max_age = Duration::ZERO;
                        continue;
                    }

                    Source::Storage => return Ok(body),
                }
            }

            if self
                .store_group(group_id, &wrapper, version, "leave")
                .await?
            {
                return Ok(body);
            }
        }
    }

    #[instrument(skip(self, offset_commit), fields(group_id = offset_commit.group_id, generation_id = offset_commit.generation_id_or_member_epoch))]
    async fn offset_commit(&self, offset_commit: OffsetCommit<'_>) -> Result<Body> {
        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "offset_commit")]);

        let group_id = offset_commit.group_id;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "offset_commit_loop")]);

            // The commit is checked against the group in storage, not the cache: a cached group
            // can be older than another broker's rebalance that removed this member.
            let (original, version) = self.stored_group(group_id).await?;
            let now = SystemTime::now();

            let (mut wrapper, error_code) = original
                .clone()
                .settle(group_id, now)
                .check_offset_commit(now, &offset_commit);

            // The coordinator stores the group before the offsets, and stores the offsets only
            // if the group's conditional write lands. A lost write means that another broker
            // changed the group after this read, and may have removed the member, so the commit
            // is checked again. A member's accepted commit refreshes its contact, so it always
            // writes the group. A check that changes nothing needs no write, unless storage
            // holds no group yet.
            let is_unchanged = wrapper == original && version.is_some();

            if !is_unchanged
                && !self
                    .store_group(group_id, &wrapper, version, "offset_commit")
                    .await?
            {
                continue;
            }

            return Ok(match error_code {
                Some(error_code) => offset_commit_response(&offset_commit, error_code),

                None => wrapper
                    .commit_offset(&offset_commit)
                    .await
                    .unwrap_or_else(|reason| {
                        debug!(?reason);
                        offset_commit_response(&offset_commit, ErrorCode::UnknownMemberId)
                    }),
            });
        }
    }

    #[instrument(skip(self))]
    async fn txn_offset_commit_fence(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<Option<ErrorCode>> {
        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "txn_offset_commit_fence")]);

        let detail = OffsetCommit {
            group_id,
            generation_id_or_member_epoch: Some(generation_id),
            member_id: Some(member_id),
            group_instance_id,
            retention_time_ms: None,
            topics: None,
            is_transactional: true,
        };

        loop {
            // As `offset_commit` does: against the group in storage. The offsets are not stored
            // here, so a lost write only means the fence is checked again.
            let (original, version) = self.stored_group(group_id).await?;
            let now = SystemTime::now();

            let (wrapper, error_code) = original
                .clone()
                .settle(group_id, now)
                .check_offset_commit(now, &detail);

            let is_unchanged = wrapper == original && version.is_some();

            if is_unchanged
                || self
                    .store_group(group_id, &wrapper, version, "txn_offset_commit_fence")
                    .await?
            {
                return Ok(error_code);
            }
        }
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body> {
        debug!(?group_id, ?topics, ?groups, ?require_stable);

        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "offset_fetch")]);

        let wrapper = Wrapper::Forming(Inner::new(self.storage.clone()));

        let now = SystemTime::now();
        let (_wrapper, body) = wrapper
            .offset_fetch(now, group_id, topics, groups, require_stable)
            .await;
        Ok(body)
    }

    #[instrument(skip(self, group_instance_id))]
    async fn heartbeat(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<Body> {
        debug!(?group_id, ?generation_id, ?member_id, ?group_instance_id);
        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "heartbeat")]);

        let mut max_age = Duration::MAX;

        loop {
            COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "heartbeat_loop")]);

            let (original, version, source) = self.recent_group(group_id, max_age).await?;
            let now = SystemTime::now();

            let (wrapper, body) = original
                .clone()
                .settle(group_id, now)
                .heartbeat(now, group_id, generation_id, member_id, group_instance_id)
                .await;

            if wrapper == original {
                // An answer that changes nothing needs no write. The cache can be older than
                // another broker's change, so an answer from the cache is checked on storage.
                match source {
                    Source::Cache => {
                        max_age = Duration::ZERO;
                        continue;
                    }

                    Source::Storage => return Ok(body),
                }
            }

            if self
                .store_group(group_id, &wrapper, version, "heartbeat")
                .await?
            {
                return Ok(body);
            }
        }
    }

    /// Deletes every named group that has no members, and refuses each other
    /// group with `NON_EMPTY_GROUP`.
    ///
    /// The decision comes from storage, not from this broker's cache, because
    /// another broker that shares the storage can hold the group's members.
    /// This broker forgets its cached state for each group that it deletes.
    /// A known race remains: a concurrent request for the same group can
    /// write the group's detail again after the delete, because the
    /// `Storage` delete does not check the version of the group's detail.
    #[instrument(skip(self))]
    async fn delete_groups(&self, group_ids: &[String]) -> Result<Body> {
        debug!(?group_ids);
        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "delete_groups")]);

        // Kafka processes each distinct group id once, and answers it once:
        // [KafkaApis.scala#L1891].
        //
        // [KafkaApis.scala#L1891]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1891
        let mut seen = HashSet::with_capacity(group_ids.len());
        let group_ids = group_ids
            .iter()
            .filter(|group_id| seen.insert(group_id.as_str()))
            .cloned()
            .collect::<Vec<_>>();

        let mut described = self
            .storage
            .describe_groups(Some(&group_ids), false)
            .await?
            .into_iter()
            .map(|named| (named.name, named.response))
            .collect::<BTreeMap<_, _>>();

        let now = SystemTime::now();
        let mut results = Vec::with_capacity(group_ids.len());
        let mut deletable = Vec::with_capacity(group_ids.len());

        for group_id in group_ids {
            let refusal = match described.remove(&group_id) {
                Some(GroupDetailResponse::Found(detail)) => {
                    // The coordinator removes each member whose session
                    // timeout has expired, so that a member that never sent
                    // `LeaveGroup` does not keep the group alive.
                    let wrapper = Wrapper::with_storage_group_detail(self.storage.clone(), detail)
                        .settle(&group_id, now);

                    (!wrapper.members().is_empty()).then_some(ErrorCode::NonEmptyGroup)
                }

                // `Storage::delete_groups` answers for a group that does not
                // exist and for an invalid group id.
                Some(GroupDetailResponse::ErrorCode(
                    ErrorCode::GroupIdNotFound | ErrorCode::InvalidGroupId,
                )) => None,

                // The coordinator keeps a group whose state it cannot read,
                // because an unread group can still have members.
                Some(GroupDetailResponse::ErrorCode(error_code)) => {
                    warn!(
                        group_id,
                        ?error_code,
                        "describe groups failed, not deleting"
                    );
                    Some(error_code)
                }

                None => {
                    warn!(group_id, "describe groups omitted group, not deleting");
                    Some(ErrorCode::UnknownServerError)
                }
            };

            match refusal {
                None => deletable.push(group_id),
                Some(error_code) => results.push(
                    DeletableGroupResult::default()
                        .group_id(group_id)
                        .error_code(error_code.into()),
                ),
            }
        }

        if !deletable.is_empty() {
            let deleted = self.storage.delete_groups(Some(&deletable)).await?;

            _ = self.wrappers.lock().map(|mut wrappers| {
                for result in &deleted {
                    if result.error_code == i16::from(ErrorCode::None) {
                        _ = wrappers.remove(&result.group_id);
                    }
                }
            })?;

            results.extend(deleted);
        }

        Ok(Body::DeleteGroupsResponse(
            DeleteGroupsResponse::default()
                .throttle_time_ms(0)
                .results(Some(results)),
        ))
    }

    /// Lists each group with its state at the current time, after the changes that the passing
    /// of time makes, as Kafka's timers would have made them.
    #[instrument(skip(self))]
    async fn list_groups(&self, states_filter: Option<&[String]>) -> Result<Body> {
        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "list_groups")]);

        let listed = self
            .storage
            .list_groups()
            .await
            .map(|groups| settled_groups(&self.storage, groups, SystemTime::now()));

        Ok(Body::ListGroupsResponse(list_groups_response(
            listed,
            states_filter,
        )))
    }

    /// Describes each group as it is at the current time, after the changes that the passing of
    /// time makes, as Kafka's timers would have made them.
    #[instrument(skip(self))]
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Body> {
        COORDINATOR_REQUESTS.add(1, &[KeyValue::new("method", "describe_groups")]);

        let described = self
            .storage
            .describe_groups(group_ids, include_authorized_operations)
            .await
            .map(|groups| settled_groups(&self.storage, groups, SystemTime::now()));

        Ok(Body::DescribeGroupsResponse(describe_groups_response(
            described,
            group_ids,
            include_authorized_operations,
        )))
    }
}

/// Returns each group in `groups` as it is at `now`. A request reads a group to report its state,
/// so the group gets each change that is due by then, as for any other request: a member whose
/// session has expired is removed, and a join phase whose deadline has passed ends. The settled
/// group is not stored.
fn settled_groups<O>(
    storage: &O,
    groups: Vec<NamedGroupDetail>,
    now: SystemTime,
) -> Vec<NamedGroupDetail>
where
    O: Storage + Clone,
{
    groups
        .into_iter()
        .map(|NamedGroupDetail { name, response }| match response {
            GroupDetailResponse::Found(detail) => {
                let settled =
                    Wrapper::with_storage_group_detail(storage.clone(), detail).settle(&name, now);

                NamedGroupDetail::found(name, GroupDetail::from(&settled))
            }

            GroupDetailResponse::ErrorCode(error_code) => {
                NamedGroupDetail::error_code(name, error_code)
            }
        })
        .collect()
}

/// Kafka's Empty, PreparingRebalance and CompletingRebalance states. A group with a join phase is
/// in PreparingRebalance. Without one, a group with members is in CompletingRebalance, and a group
/// without members is Empty.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Forming {
    protocol: Option<Protocol>,
    join_phase: Option<JoinPhase>,
}

/// The protocol a forming group has settled on, and its leader if it has one.
///
/// A group takes its protocol from the first member to join, and only then
/// chooses a leader, so a leader without a protocol cannot be represented.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Protocol {
    protocol_type: String,
    protocol_name: String,
    leader: Option<String>,
}

impl Forming {
    /// Build from the stored group record, which keeps the three parts
    /// separately. A record without both a type and a name has no protocol,
    /// and so no leader.
    fn from_parts(
        protocol_type: Option<String>,
        protocol_name: Option<String>,
        leader: Option<String>,
    ) -> Self {
        Self {
            protocol: protocol_type
                .zip(protocol_name)
                .map(|(protocol_type, protocol_name)| Protocol {
                    protocol_type,
                    protocol_name,
                    leader,
                }),
            join_phase: None,
        }
    }

    fn protocol_type(&self) -> Option<&str> {
        self.protocol
            .as_ref()
            .map(|protocol| protocol.protocol_type.as_str())
    }

    fn protocol_name(&self) -> Option<&str> {
        self.protocol
            .as_ref()
            .map(|protocol| protocol.protocol_name.as_str())
    }

    fn leader(&self) -> Option<&str> {
        self.protocol
            .as_ref()
            .and_then(|protocol| protocol.leader.as_deref())
    }

    fn set_leader(&mut self, leader: Option<String>) {
        if let Some(protocol) = self.protocol.as_mut() {
            protocol.leader = leader;
        }
    }
}

impl fmt::Display for Forming {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Forming({}/{}/{}/{})",
            self.protocol_type().unwrap_or("?"),
            self.protocol_name().unwrap_or("?"),
            self.leader().unwrap_or("?"),
            self.join_phase
                .as_ref()
                .map_or("-".to_owned(), |join_phase| join_phase
                    .joined
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", "))
        )
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Formed {
    protocol_type: String,
    protocol_name: String,
    leader: String,
    assignments: BTreeMap<String, Bytes>,
}

impl fmt::Display for Formed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Formed({}/{}/{}/[{}])",
            self.protocol_type,
            self.protocol_name,
            self.leader,
            self.assignments
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[derive(Clone, Debug)]
pub struct Inner<O, S> {
    /// The session timeout of the member that formed the group. Kafka keeps a session timeout for
    /// each member. Nisshi applies one to the whole group, as it did before it stored rebalances.
    session_timeout_ms: i32,
    /// The largest rebalance timeout of a member that joined since the group was last empty.
    /// Kafka uses the largest rebalance timeout of the current members. Nisshi doesn't store a
    /// timeout for each member, so a member that leaves can't shorten the group's timeout until
    /// the group is empty again.
    rebalance_timeout_ms: Option<i32>,
    members: BTreeMap<String, Member>,
    generation_id: i32,
    state: S,
    storage: O,
    skip_assignment: Option<bool>,
    inception: SystemTime,
    pending_members: BTreeMap<String, SystemTime>,
    pending_sync: Option<PendingSync>,
}

impl<O, S> fmt::Display for Inner<O, S>
where
    S: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "state: {}, generation: {}, members: [{}]",
            self.state,
            self.generation_id,
            self.members.keys().cloned().collect::<Vec<_>>().join(", ")
        )
    }
}

impl<O, S> PartialEq for Inner<O, S>
where
    S: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.session_timeout_ms == other.session_timeout_ms
            && self.rebalance_timeout_ms == other.rebalance_timeout_ms
            && self.members == other.members
            && self.generation_id == other.generation_id
            && self.state == other.state
            && self.skip_assignment == other.skip_assignment
            && self.inception == other.inception
            && self.pending_members == other.pending_members
            && self.pending_sync == other.pending_sync
    }
}

impl<O, S> Hash for Inner<O, S>
where
    S: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.session_timeout_ms.hash(state);
        self.rebalance_timeout_ms.hash(state);
        self.members.hash(state);
        self.generation_id.hash(state);
        self.state.hash(state);
        self.skip_assignment.hash(state);
        self.inception.hash(state);
        self.pending_members.hash(state);
        self.pending_sync.hash(state);
    }
}

impl<O> Inner<O, PhantomData<Forming>>
where
    O: Storage,
{
    pub fn new(storage: O) -> Inner<O, Forming> {
        Inner {
            session_timeout_ms: Default::default(),
            rebalance_timeout_ms: Default::default(),
            members: Default::default(),
            generation_id: 0,
            state: Forming::default(),
            skip_assignment: Some(false),
            storage,
            inception: SystemTime::now(),
            pending_members: Default::default(),
            pending_sync: None,
        }
    }
}

/// Returns `milliseconds` as a duration, or `None` if it is not positive.
fn positive_millis(milliseconds: i32) -> Option<Duration> {
    u64::try_from(milliseconds)
        .ok()
        .filter(|milliseconds| *milliseconds > 0)
        .map(Duration::from_millis)
}

fn new_member_id(prefix: Option<&str>) -> String {
    match prefix {
        Some(prefix) => format!("{prefix}-{}", Uuid::new_v4()),
        None => format!("{}", Uuid::new_v4()),
    }
}

/// Returns a `JoinGroup` response that reports `error_code` to `member_id`.
fn join_error(member_id: &str, error_code: ErrorCode) -> Reply {
    debug!(join_outcome = ?error_code);

    Reply::Ready(
        JoinGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(error_code.into())
            .generation_id(-1)
            .protocol_type(None)
            // ProtocolName is non-nullable before version 7, so encoding None would omit the
            // length prefix and truncate the frame.
            .protocol_name(Some("".into()))
            .leader("".into())
            .skip_assignment(Some(false))
            .member_id(member_id.to_owned())
            .members(Some([].into()))
            .into(),
    )
}

fn sync_error(error_code: ErrorCode) -> Reply {
    debug!(sync_outcome = ?error_code);

    Reply::Ready(
        SyncGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(error_code.into())
            .protocol_type(None)
            .protocol_name(None)
            .assignment(Bytes::from_static(b""))
            .into(),
    )
}

fn rebalance_in_progress_join(member_id: &str) -> Body {
    match join_error(member_id, ErrorCode::RebalanceInProgress) {
        Reply::Ready(body) => body,
        Reply::Held { .. } => unreachable!("join_error answers at once"),
    }
}

fn rebalance_in_progress_sync() -> Body {
    match sync_error(ErrorCode::RebalanceInProgress) {
        Reply::Ready(body) => body,
        Reply::Held { .. } => unreachable!("sync_error answers at once"),
    }
}

fn heartbeat_response(error_code: ErrorCode) -> Body {
    debug!(heartbeat_outcome = ?error_code);

    HeartbeatResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(error_code.into())
        .into()
}

impl<O, S> Inner<O, S>
where
    O: Storage,
    S: Debug,
{
    /// Returns the partitions that an `OffsetFetch` with `require_stable` answers with
    /// `UNSTABLE_OFFSET_COMMIT`: those for which an open transaction holds an offset of the
    /// group. Without `require_stable`, storage is not read and no partition is unstable.
    ///
    /// The caller reads the unstable partitions before the committed offsets. A transaction
    /// that ends between the two reads is then answered as unstable, which the client
    /// retries, and not as the offset from before its end. PostgreSQL, SQLite and Turso
    /// commit a transaction's offsets and drop its pending ones in one database
    /// transaction. DynoStore does not yet: its `txn_end` clears the pending offsets before
    /// it writes the committed ones, so a read in between gets the earlier offset as stable.
    async fn unstable_offsets(
        &self,
        group_id: Option<&str>,
        require_stable: Option<bool>,
    ) -> nisshi_storage::Result<BTreeSet<Topition>> {
        match (group_id, require_stable) {
            (Some(group_id), Some(true)) => self.storage.pending_txn_offsets(group_id).await,
            _ => Ok(BTreeSet::new()),
        }
    }

    async fn fetch_offset(
        &mut self,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> Result<Body> {
        debug!(?group_id, ?topics, ?groups, ?require_stable);

        let topics = if let Some(topics) = topics {
            let topics: Vec<Topition> = topics
                .iter()
                .flat_map(|topic| {
                    topic
                        .partition_indexes
                        .as_ref()
                        .map(|partition_indexes| {
                            partition_indexes
                                .iter()
                                .map(|partition_index| {
                                    Topition::new(topic.name.clone(), *partition_index)
                                })
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                })
                .collect();

            let unstable = self.unstable_offsets(group_id, require_stable).await?;

            self.storage
                .offset_fetch(group_id, topics.deref(), require_stable)
                .await
                .map(|offsets| fetched_offsets(offsets, &unstable))
                .map(|offsets| {
                    offsets
                        .iter()
                        .fold(BTreeSet::new(), |mut topics, (topition, _)| {
                            _ = topics.insert(topition.topic());
                            topics
                        })
                        .iter()
                        .map(|topic_name| {
                            OffsetFetchResponseTopic::default()
                                .name((*topic_name).into())
                                .partitions(Some(
                                    offsets
                                        .iter()
                                        .filter_map(|(topition, (committed, error_code))| {
                                            if topition.topic() == *topic_name {
                                                Some(
                                                    OffsetFetchResponsePartition::default()
                                                        .partition_index(topition.partition())
                                                        .committed_offset(committed.offset)
                                                        .committed_leader_epoch(Some(
                                                            fetched_leader_epoch(committed),
                                                        ))
                                                        .metadata(Some(fetched_metadata(committed)))
                                                        .error_code((*error_code).into()),
                                                )
                                            } else {
                                                None
                                            }
                                        })
                                        .collect(),
                                ))
                        })
                        .collect()
                })
                .map(Some)?
        } else {
            None
        };

        let groups = if let Some(groups) = groups {
            let mut responses = vec![];

            for group in groups {
                debug!(?group);

                let topics = group.topics.as_ref().map(|topics| {
                    topics
                        .iter()
                        .flat_map(|topic| {
                            topic
                                .partition_indexes
                                .as_ref()
                                .map(|partition_indexes| {
                                    partition_indexes
                                        .iter()
                                        .map(|partition_index| {
                                            Topition::new(topic.name.clone(), *partition_index)
                                        })
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default()
                        })
                        .collect::<Vec<_>>()
                });

                let response = async {
                    let unstable = self
                        .unstable_offsets(Some(group.group_id.as_str()), require_stable)
                        .await?;

                    if let Some(topics) = topics {
                        self.storage
                            .offset_fetch(
                                Some(group.group_id.as_str()),
                                topics.deref(),
                                require_stable,
                            )
                            .await
                    } else {
                        self.storage
                            .committed_offset_topitions(&group.group_id)
                            .await
                    }
                    .map(|offsets| fetched_offsets(offsets, &unstable))
                }
                .await
                .inspect(|offsets| debug!(?offsets))
                .inspect_err(|err| error!(?err, ?group))
                .map(|offsets| {
                    OffsetFetchResponseGroup::default()
                        .group_id(group.group_id.clone())
                        .topics(Some(
                            offsets
                                .iter()
                                .fold(BTreeSet::new(), |mut topics, (topition, _)| {
                                    _ = topics.insert(topition.topic());
                                    topics
                                })
                                .iter()
                                .map(|topic_name| {
                                    OffsetFetchResponseTopics::default()
                                        .name((*topic_name).into())
                                        .partitions(Some(
                                            offsets
                                                .iter()
                                                .filter_map(
                                                    |(topition, (committed, error_code))| {
                                                        if topition.topic() == *topic_name {
                                                            Some(
                                                            OffsetFetchResponsePartitions::default(
                                                            )
                                                            .partition_index(topition.partition())
                                                            .committed_offset(committed.offset)
                                                            .committed_leader_epoch(
                                                                fetched_leader_epoch(committed),
                                                            )
                                                            .metadata(Some(fetched_metadata(
                                                                committed,
                                                            )))
                                                            .error_code((*error_code).into()),
                                                        )
                                                        } else {
                                                            None
                                                        }
                                                    },
                                                )
                                                .collect(),
                                        ))
                                })
                                .collect(),
                        ))
                        .error_code(ErrorCode::None.into())
                })
                // Kafka reports an error per group from version 8, so a group
                // whose offsets the storage can't read fails alone, and the
                // other groups still get their offsets.
                .unwrap_or_else(|error| {
                    OffsetFetchResponseGroup::default()
                        .group_id(group.group_id.clone())
                        .topics(Some([].into()))
                        .error_code(offset_fetch_error_code(&error.into()).into())
                });

                responses.push(response);
            }

            Some(responses)
        } else {
            None
        };

        Ok(OffsetFetchResponse::default()
            .throttle_time_ms(Some(0))
            .topics(topics)
            .error_code(Some(ErrorCode::None.into()))
            .groups(groups)
            .into())
    }

    async fn commit_offset(&mut self, detail: &OffsetCommit<'_>) -> Result<Body> {
        let retention_time_ms = detail.retention_time_ms.map_or(Ok(None), |ms| {
            u64::try_from(ms)
                .map(Duration::from_millis)
                .map_err(Error::from)
                .map(Some)
        })?;

        if let Some(topics) = detail.topics {
            let mut offsets = vec![];
            let mut too_large = vec![];

            for topic in topics {
                if let Some(ref partitions) = topic.partitions {
                    for partition in partitions {
                        let topition = Topition::new(topic.name.clone(), partition.partition_index);

                        if offset_metadata::is_too_large(partition.committed_metadata.as_deref()) {
                            too_large.push((topition, ErrorCode::OffsetMetadataTooLarge));
                            continue;
                        }

                        let offset = OffsetCommitRequest::try_from(partition)?;

                        offsets.push((topition, offset));
                    }
                }
            }

            self.storage
                .offset_commit(detail.group_id, retention_time_ms, offsets.deref())
                .await
                .map(|mut value| {
                    value.append(&mut too_large);

                    let topics = value
                        .iter()
                        .fold(BTreeSet::new(), |mut topics, (topition, _)| {
                            _ = topics.insert(topition.topic());
                            topics
                        })
                        .iter()
                        .map(|topic_name| {
                            OffsetCommitResponseTopic::default()
                                .name((*topic_name).into())
                                .partitions(Some(
                                    value
                                        .iter()
                                        .filter_map(|(topition, error_code)| {
                                            if topition.topic() == *topic_name {
                                                Some(
                                                    OffsetCommitResponsePartition::default()
                                                        .partition_index(topition.partition())
                                                        .error_code(i16::from(*error_code)),
                                                )
                                            } else {
                                                None
                                            }
                                        })
                                        .collect(),
                                ))
                        })
                        .collect();

                    OffsetCommitResponse::default()
                        .throttle_time_ms(Some(0))
                        .topics(Some(topics))
                        .into()
                })
                .inspect_err(|err| error!(?err))
                .map_err(Into::into)
        } else {
            Ok(OffsetCommitResponse::default()
                .throttle_time_ms(Some(0))
                .topics(detail.topics.map(|topics| {
                    topics
                        .as_ref()
                        .iter()
                        .map(|topic| {
                            OffsetCommitResponseTopic::default()
                                .name(topic.name.clone())
                                .partitions(topic.partitions.as_ref().map(|partitions| {
                                    partitions
                                        .iter()
                                        .map(|partition| {
                                            OffsetCommitResponsePartition::default()
                                                .partition_index(partition.partition_index)
                                                .error_code(ErrorCode::UnknownMemberId.into())
                                        })
                                        .collect()
                                }))
                        })
                        .collect()
                }))
                .into())
        }
    }

    fn session_timeout(&self) -> Duration {
        positive_millis(self.session_timeout_ms).unwrap_or(FALLBACK_SESSION_TIMEOUT)
    }

    /// The time that the coordinator gives each phase of a rebalance.
    fn rebalance_timeout(&self) -> Duration {
        self.rebalance_timeout_ms
            .and_then(positive_millis)
            .unwrap_or_else(|| self.session_timeout())
    }

    fn member_list(&self) -> Vec<JoinGroupResponseMember> {
        self.members
            .values()
            .map(|member| member.join_response.clone())
            .collect()
    }

    fn static_member_id(&self, group_instance_id: &str) -> Option<&str> {
        self.members.iter().find_map(|(member_id, member)| {
            (member.join_response.group_instance_id.as_deref() == Some(group_instance_id))
                .then_some(member_id.as_str())
        })
    }

    /// Checks that `member_id` is a member, and that it is the member that holds
    /// `group_instance_id`, as Kafka's [`validateMember`] does.
    ///
    /// [`validateMember`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/classic/ClassicGroup.java#L793
    fn validate_member(
        &self,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<(), ErrorCode> {
        if let Some(group_instance_id) = group_instance_id {
            match self.static_member_id(group_instance_id) {
                None => return Err(ErrorCode::UnknownMemberId),
                Some(existing) if existing != member_id => {
                    info!(
                        member_id,
                        group_instance_id, existing, "fenced by the existing static member"
                    );
                    return Err(ErrorCode::FencedInstanceId);
                }
                Some(_) => {}
            }
        }

        if self.members.contains_key(member_id) {
            Ok(())
        } else {
            Err(ErrorCode::UnknownMemberId)
        }
    }

    fn has_joined_with(&self, member_id: &str, metadata: &Bytes) -> bool {
        self.members
            .get(member_id)
            .is_some_and(|member| member.join_response.metadata == *metadata)
    }

    /// Raises the group's rebalance timeout to `rebalance_timeout_ms` if that is larger.
    fn raise_rebalance_timeout(&mut self, rebalance_timeout_ms: i32) {
        self.rebalance_timeout_ms = Some(
            self.rebalance_timeout_ms
                .map_or(rebalance_timeout_ms, |current| {
                    current.max(rebalance_timeout_ms)
                }),
        );
    }

    /// Adds or updates the member `member_id` from `request`.
    fn upsert_member(
        &mut self,
        now: SystemTime,
        member_id: &str,
        request: &JoinRequest<'_>,
        metadata: &Bytes,
    ) {
        self.raise_rebalance_timeout(request.rebalance_timeout_ms);

        let member = self
            .members
            .entry(member_id.to_owned())
            .or_insert_with(|| Member {
                join_response: JoinGroupResponseMember::default()
                    .member_id(member_id.to_owned())
                    .group_instance_id(request.group_instance_id.map(ToOwned::to_owned)),
                ..Member::default()
            });

        member.join_response.metadata = metadata.clone();
        member.last_contact = Some(now);
        member.client_id = request.client_id.unwrap_or_default().to_owned();
        member.client_host = request.client_host.unwrap_or_default().to_owned();
    }

    /// Forgets each pending member id whose session timeout has expired. Returns whether it forgot
    /// one.
    fn forget_pending_members(&mut self, now: SystemTime) -> bool {
        let before = self.pending_members.len();
        self.pending_members.retain(|member_id, expires| {
            let keep = *expires > now;
            if !keep {
                info!(
                    member_id,
                    "forgot pending member id after its session timeout"
                );
            }
            keep
        });
        before > self.pending_members.len()
    }

    /// Removes the members whose session timeout expired by `now`, except those for which
    /// `is_waiting` is true. Kafka treats a member that waits in a join or a sync as heartbeating,
    /// because a client doesn't heartbeat while it waits. Returns whether it removed a member.
    fn remove_expired_members(
        &mut self,
        now: SystemTime,
        is_waiting: impl Fn(&str) -> bool,
    ) -> bool {
        let session_timeout = self.session_timeout();
        let generation_id = self.generation_id;
        let before = self.members.len();

        self.members.retain(|member_id, member| {
            let expired = !is_waiting(member_id)
                && member.last_contact.is_none_or(|last_contact| {
                    now.duration_since(last_contact).unwrap_or_default() > session_timeout
                });

            if expired {
                info!(
                    member_id,
                    generation_id, "removed after its session timeout"
                );
            }

            !expired
        });

        before > self.members.len()
    }

    /// Removes `member_ids` from the members waiting to sync.
    fn forget_pending_sync(&mut self, member_ids: &[&str]) {
        if let Some(pending_sync) = self.pending_sync.as_mut() {
            for member_id in member_ids {
                _ = pending_sync.members.remove(*member_id);
            }

            if pending_sync.members.is_empty() {
                self.pending_sync = None;
            }
        }
    }

    /// Removes each member that has not sent `SyncGroup` by the sync deadline, static members
    /// too, as Kafka's [`expirePendingSync`] does. Returns whether it removed a member.
    ///
    /// [`expirePendingSync`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java#L4854
    fn remove_members_pending_sync(&mut self, now: SystemTime) -> bool {
        match self.pending_sync.take() {
            Some(pending_sync) if pending_sync.deadline <= now => {
                info!(members = ?pending_sync.members, "removed members that didn't sync");

                let before = self.members.len();
                self.members
                    .retain(|member_id, _| !pending_sync.members.contains(member_id));
                before > self.members.len()
            }

            pending_sync => {
                self.pending_sync = pending_sync;
                false
            }
        }
    }

    /// Answers a member that is leaving, and removes it or its pending member id. Returns whether
    /// the leave was valid.
    fn remove_leaving(
        &mut self,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (MemberResponse, bool) {
        let response = MemberResponse::default()
            .member_id(member_id.to_owned())
            .group_instance_id(group_instance_id.map(ToOwned::to_owned));

        let removing = match group_instance_id {
            Some(group_instance_id) => match self.static_member_id(group_instance_id) {
                None => Err(ErrorCode::UnknownMemberId),
                Some(existing) if !member_id.is_empty() && existing != member_id => {
                    Err(ErrorCode::FencedInstanceId)
                }
                Some(existing) => Ok(existing.to_owned()),
            },

            None if self.pending_members.remove(member_id).is_some() => {
                return (response.error_code(ErrorCode::None.into()), true);
            }

            None if self.members.contains_key(member_id) => Ok(member_id.to_owned()),

            None => Err(ErrorCode::UnknownMemberId),
        };

        match removing {
            Ok(removing) => {
                _ = self.members.remove(&removing);
                self.forget_pending_sync(&[removing.as_str()]);
                info!(member_id = removing, "left the group");
                (response.error_code(ErrorCode::None.into()), true)
            }

            Err(error_code) => (response.error_code(error_code.into()), false),
        }
    }

    /// Answers each leaving member, and removes it or its pending member id. Returns whether a
    /// leave was valid.
    fn remove_leaving_members(
        &mut self,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Vec<MemberResponse>, bool) {
        let mut any_left = false;

        let responses = match member_id {
            Some(member_id) => {
                let (response, left) = self.remove_leaving(member_id, None);
                any_left |= left;
                vec![response]
            }

            None => members
                .unwrap_or_default()
                .iter()
                .map(|member| {
                    let (response, left) =
                        self.remove_leaving(&member.member_id, member.group_instance_id.as_deref());
                    any_left |= left;
                    response
                })
                .collect(),
        };

        (responses, any_left)
    }

    /// Fences an offset commit as Kafka's [`validateOffsetCommit`] does. A commit without a
    /// generation or a member, from a simple consumer or an admin client, needs an empty
    /// group. Any other commit must come from a member in the group's current generation. A
    /// member that commits while the group waits for the leader's assignment gets
    /// `REBALANCE_IN_PROGRESS`, unless the commit is transactional, which Kafka does not hold
    /// to the group's state.
    ///
    /// [`validateOffsetCommit`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/classic/ClassicGroup.java#L825
    #[instrument(skip(self, now, detail), fields(generation_id = detail.generation_id_or_member_epoch, member_id = detail.member_id))]
    fn offset_commit_fence(
        &mut self,
        now: SystemTime,
        detail: &OffsetCommit<'_>,
        is_completing_rebalance: bool,
    ) -> Option<ErrorCode> {
        let generation_id = detail.generation_id_or_member_epoch.unwrap_or(-1);
        let member_id = detail.member_id.unwrap_or_default();

        if generation_id < 0 && self.members.is_empty() {
            return None;
        }

        if generation_id >= 0 || !member_id.is_empty() || detail.group_instance_id.is_some() {
            if let Err(error_code) = self.validate_member(member_id, detail.group_instance_id) {
                return Some(error_code);
            }

            if generation_id != self.generation_id {
                return Some(ErrorCode::IllegalGeneration);
            }
        } else if !self.members.is_empty() {
            return Some(ErrorCode::UnknownMemberId);
        }

        if is_completing_rebalance && !detail.is_transactional {
            return Some(ErrorCode::RebalanceInProgress);
        }

        if !detail.is_transactional
            && let Some(member) = self.members.get_mut(member_id)
        {
            member.last_contact = Some(now);
        }

        None
    }

    fn earliest_pending_member_expiry(&self) -> Option<SystemTime> {
        self.pending_members.values().min().copied()
    }

    /// Returns when the session of the first member for which `is_waiting` is false expires.
    fn earliest_session_expiry(&self, is_waiting: impl Fn(&str) -> bool) -> Option<SystemTime> {
        let session_timeout = self.session_timeout();

        self.members
            .iter()
            .filter(|(member_id, _)| !is_waiting(member_id))
            .filter_map(|(_, member)| member.last_contact)
            .map(|last_contact| last_contact + session_timeout)
            .min()
    }
}

impl<O> Inner<O, Forming>
where
    O: Storage,
{
    /// Returns whether `member_id` waits in the join phase or for the leader's assignment, so
    /// that its session can't expire.
    ///
    /// Kafka removes a new member that waits in a join phase for more than 5 minutes
    /// (`CLASSIC_GROUP_NEW_MEMBER_JOIN_TIMEOUT_MS`). Nisshi doesn't, because the join phase ends
    /// by the rebalance timeout, which clients set to 5 minutes by default.
    fn is_waiting(&self, member_id: &str) -> bool {
        match self.state.join_phase.as_ref() {
            Some(join_phase) => join_phase.joined.contains(member_id),
            None => self
                .pending_sync
                .as_ref()
                .is_none_or(|pending_sync| !pending_sync.members.contains(member_id)),
        }
    }

    fn remove_expired(&mut self, now: SystemTime) -> bool {
        let waiting = self
            .members
            .keys()
            .filter(|member_id| self.is_waiting(member_id))
            .cloned()
            .collect::<BTreeSet<_>>();

        let removed = self.remove_expired_members(now, |member_id| waiting.contains(member_id));

        if removed {
            self.forget_removed_members();
        }

        removed
    }

    /// Keeps the leader, the join phase and the pending syncs consistent with the members, after
    /// the coordinator removes some.
    fn forget_removed_members(&mut self) {
        if self
            .state
            .leader()
            .is_some_and(|leader| !self.members.contains_key(leader))
        {
            let leader = self.members.keys().next().cloned();
            self.state.set_leader(leader);
        }

        if let Some(join_phase) = self.state.join_phase.as_mut() {
            join_phase
                .joined
                .retain(|member_id| self.members.contains_key(member_id));
        }

        let members = &self.members;
        if let Some(pending_sync) = self.pending_sync.as_mut() {
            pending_sync
                .members
                .retain(|member_id| members.contains_key(member_id));
        }
    }

    fn next_deadline(&self) -> Option<SystemTime> {
        let waiting = |member_id: &str| self.is_waiting(member_id);

        [
            self.state
                .join_phase
                .as_ref()
                .map(|join_phase| join_phase.deadline),
            self.pending_sync
                .as_ref()
                .map(|pending_sync| pending_sync.deadline),
            self.earliest_pending_member_expiry(),
            self.earliest_session_expiry(waiting),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    fn is_completing_rebalance(&self) -> bool {
        self.state.join_phase.is_none() && !self.members.is_empty()
    }

    /// Starts a join phase in a group with members, which ends when every member has rejoined, or
    /// at the rebalance timeout, as Kafka's [`prepareRebalance`] does. Each member waiting to
    /// sync gets `REBALANCE_IN_PROGRESS`, and its session starts again.
    ///
    /// [`prepareRebalance`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java#L4627
    fn prepare_rebalance(&mut self, now: SystemTime) {
        // A member with a SyncGroup in progress couldn't heartbeat while it waited. Kafka
        // answers the request when the rebalance starts and then reschedules the member's
        // heartbeat, so the member gets a full session timeout to rejoin.
        if self.is_completing_rebalance() {
            self.restart_sessions_of_synced_members(now);
        }

        self.start_join_phase(now);
    }

    /// Restarts the session of each member that has sent `SyncGroup` in this rebalance: each
    /// member that `pending_sync` doesn't list, which is every member when it is `None`. Such a
    /// member waits for the leader's assignment and can't heartbeat while it waits, so its
    /// session starts again when its `SyncGroup` is answered, as Kafka's
    /// [`propagateAssignment`] reschedules each member's heartbeat.
    ///
    /// [`propagateAssignment`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java#L4787-L4796
    fn restart_sessions_of_synced_members(&mut self, now: SystemTime) {
        let pending_sync = self
            .pending_sync
            .as_ref()
            .map(|pending_sync| &pending_sync.members);

        for (member_id, member) in &mut self.members {
            if pending_sync.is_none_or(|pending_sync| !pending_sync.contains(member_id)) {
                member.last_contact = Some(now);
            }
        }
    }

    /// Starts a join phase that ends when every member has rejoined, or at the rebalance timeout.
    fn start_join_phase(&mut self, now: SystemTime) {
        info!(generation_id = self.generation_id, "preparing to rebalance");

        self.pending_sync = None;
        self.state.join_phase = Some(JoinPhase {
            deadline: now + self.rebalance_timeout(),
            joined: BTreeSet::new(),
            initial_delay: None,
        });
    }

    /// Starts the join phase of an empty group, which lasts for `initial_rebalance_delay`, so that
    /// other members can join the same generation.
    ///
    /// Kafka's Scala coordinator, the default of a 3.9.1 broker, also delays each rebalance of an
    /// empty group, but extends the delay for new members only at generation 0. The Java
    /// coordinator, which every Kafka 4 broker uses, extends it at every generation, and so does
    /// Nisshi.
    fn prepare_initial_rebalance(&mut self, now: SystemTime, initial_rebalance_delay: Duration) {
        info!(
            generation_id = self.generation_id,
            "preparing the initial rebalance"
        );

        self.pending_sync = None;
        self.state.join_phase = Some(JoinPhase {
            deadline: now + initial_rebalance_delay,
            joined: BTreeSet::new(),
            initial_delay: Some(InitialDelay {
                delay_ms: u64::try_from(initial_rebalance_delay.as_millis()).unwrap_or(u64::MAX),
                remaining_ms: u64::try_from(
                    self.rebalance_timeout()
                        .saturating_sub(initial_rebalance_delay)
                        .as_millis(),
                )
                .unwrap_or(u64::MAX),
                new_member_added: false,
            }),
        });
    }

    fn all_members_joined(&self) -> bool {
        self.pending_members.is_empty()
            && self.state.join_phase.as_ref().is_some_and(|join_phase| {
                self.members
                    .keys()
                    .all(|member_id| join_phase.joined.contains(member_id))
            })
    }

    /// Ends the join phase if every member has joined and no member id is pending, as Kafka does
    /// when a member joins, except in the join phase of an empty group, which lasts for its delay.
    fn complete_join_if_all_joined_after_join(&mut self, now: SystemTime) {
        if self
            .state
            .join_phase
            .as_ref()
            .is_some_and(|join_phase| join_phase.initial_delay.is_none())
        {
            self.complete_join_if_all_joined(now);
        }
    }

    /// Ends the join phase if every member has joined and no member id is pending, as Kafka does
    /// when a member's session or a pending member id expires, also in the join phase of an empty
    /// group.
    fn complete_join_if_all_joined(&mut self, now: SystemTime) {
        if self.all_members_joined() {
            self.complete_join(now);
        }
    }

    /// Ends the join phase if its deadline has passed. The join phase of an empty group gets
    /// another delay instead if a new member joined during the last one and the rebalance timeout
    /// allows it, as Kafka's [`tryCompleteInitialRebalanceElseSchedule`] does.
    ///
    /// A request can read the group after more than one deadline has passed, so the coordinator
    /// handles each of them in turn.
    ///
    /// [`tryCompleteInitialRebalanceElseSchedule`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java#L4696
    fn end_join_phase_after_deadline(&mut self, now: SystemTime) {
        while let Some(join_phase) = self.state.join_phase.as_mut()
            && join_phase.deadline <= now
        {
            if let Some(initial_delay) = join_phase.initial_delay.as_mut()
                && initial_delay.new_member_added
                && initial_delay.remaining_ms > 0
            {
                let delay_ms = initial_delay.delay_ms.min(initial_delay.remaining_ms);

                info!(delay_ms, "extending the initial rebalance for new members");

                initial_delay.remaining_ms = initial_delay
                    .remaining_ms
                    .saturating_sub(initial_delay.delay_ms);
                initial_delay.delay_ms = delay_ms;
                initial_delay.new_member_added = false;
                join_phase.deadline += Duration::from_millis(delay_ms);
            } else {
                self.complete_join(now);
            }
        }
    }

    /// Ends the join phase, as Kafka's [`completeClassicGroupJoin`] does. The coordinator
    /// removes each dynamic member that has not rejoined, and keeps each static member with its
    /// old subscription. If a member joined, the group starts its next generation and waits for
    /// the leader's assignment. A group without members becomes empty.
    ///
    /// [`completeClassicGroupJoin`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java#L4306
    fn complete_join(&mut self, now: SystemTime) {
        let Some(join_phase) = self.state.join_phase.take() else {
            return;
        };

        let generation_id = self.generation_id;

        self.members.retain(|member_id, member| {
            let keep = join_phase.joined.contains(member_id)
                || member.join_response.group_instance_id.is_some();

            if !keep {
                info!(member_id, generation_id, "removed after it didn't rejoin");
            }

            keep
        });

        if !self.members.is_empty()
            && !self
                .members
                .keys()
                .any(|member_id| join_phase.joined.contains(member_id))
        {
            // Only static members that didn't rejoin remain. Kafka waits for another rebalance
            // timeout, until their sessions expire.
            info!(
                generation_id,
                "no member rejoined, waiting another rebalance timeout"
            );

            self.state.join_phase = Some(JoinPhase {
                deadline: now + self.rebalance_timeout(),
                joined: BTreeSet::new(),
                initial_delay: None,
            });

            return;
        }

        if !self
            .state
            .leader()
            .is_some_and(|leader| join_phase.joined.contains(leader))
        {
            let leader = self
                .members
                .keys()
                .find(|member_id| join_phase.joined.contains(*member_id))
                .cloned();

            self.state.set_leader(leader);
        }

        self.generation_id += 1;

        if self.members.is_empty() {
            info!(generation_id = self.generation_id, "the group is now empty");

            self.state.protocol = None;
            self.pending_sync = None;
        } else {
            info!(
                generation_id = self.generation_id,
                members = self.members.len(),
                leader = self.state.leader(),
                "completed the join phase"
            );

            for member in self.members.values_mut() {
                member.last_contact = Some(now);
            }

            self.pending_sync = Some(PendingSync {
                deadline: now + self.rebalance_timeout(),
                members: self.members.keys().cloned().collect(),
            });
        }
    }

    /// Records that `member_id` joined in the join phase.
    fn mark_joined(&mut self, member_id: &str) {
        if let Some(join_phase) = self.state.join_phase.as_mut() {
            _ = join_phase.joined.insert(member_id.to_owned());
        }
    }

    /// Answers a member that has joined: at once if the join phase has ended, otherwise when it
    /// ends.
    fn reply_to_joined(&self, member_id: &str) -> Reply {
        if self.state.join_phase.is_none() && self.members.contains_key(member_id) {
            Reply::Ready(self.join_response(member_id))
        } else {
            Reply::Held {
                member_id: member_id.to_owned(),
            }
        }
    }

    fn join_response(&self, member_id: &str) -> Body {
        let leader = self.state.leader().unwrap_or_default();

        debug!(join_outcome = ?ErrorCode::None, member_id, leader, generation_id = self.generation_id);

        JoinGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .generation_id(self.generation_id)
            .protocol_type(self.state.protocol_type().map(ToOwned::to_owned))
            .protocol_name(self.state.protocol_name().map(ToOwned::to_owned))
            .leader(leader.to_owned())
            .skip_assignment(self.skip_assignment)
            .member_id(member_id.to_owned())
            .members(Some(if leader == member_id {
                self.member_list()
            } else {
                [].into()
            }))
            .into()
    }

    /// Adds a member, and starts a rebalance or ends the join phase, as Kafka's
    /// [`addMemberThenRebalanceOrCompleteJoin`] does.
    ///
    /// [`addMemberThenRebalanceOrCompleteJoin`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java#L4557
    fn add_member(
        mut self,
        now: SystemTime,
        member_id: &str,
        request: &JoinRequest<'_>,
        protocol: &JoinGroupRequestProtocol,
        initial_rebalance_delay: Duration,
    ) -> (Self, Reply) {
        let is_empty = self.members.is_empty() && self.state.join_phase.is_none();

        if self.state.protocol.is_none() {
            self.state.protocol = Some(Protocol {
                protocol_type: request.protocol_type.to_owned(),
                protocol_name: protocol.name.clone(),
                leader: None,
            });

            self.session_timeout_ms = request.session_timeout_ms;
            self.rebalance_timeout_ms = None;
        }

        info!(
            member_id,
            generation_id = self.generation_id,
            "adding member"
        );

        self.upsert_member(now, member_id, request, &protocol.metadata);

        if self.state.leader().is_none() {
            self.state.set_leader(Some(member_id.to_owned()));
        }

        match self.state.join_phase.as_mut() {
            Some(join_phase) => {
                if let Some(initial_delay) = join_phase.initial_delay.as_mut() {
                    initial_delay.new_member_added = true;
                }
            }

            None if is_empty => self.prepare_initial_rebalance(now, initial_rebalance_delay),
            None => self.prepare_rebalance(now),
        }

        self.mark_joined(member_id);
        self.complete_join_if_all_joined_after_join(now);
        let reply = self.reply_to_joined(member_id);
        (self, reply)
    }

    /// Updates an existing member that rejoins, and starts a rebalance or ends the join phase.
    fn update_member(
        mut self,
        now: SystemTime,
        member_id: &str,
        request: &JoinRequest<'_>,
        metadata: &Bytes,
    ) -> (Self, Reply) {
        let is_waiting = self
            .state
            .join_phase
            .as_ref()
            .is_some_and(|join_phase| join_phase.joined.contains(member_id))
            && self.has_joined_with(member_id, metadata);

        if !is_waiting {
            self.upsert_member(now, member_id, request, metadata);

            if self.state.join_phase.is_none() {
                self.prepare_rebalance(now);
            }

            self.mark_joined(member_id);
            self.complete_join_if_all_joined_after_join(now);
        }

        let reply = self.reply_to_joined(member_id);
        (self, reply)
    }

    /// Gives the member that holds `group_instance_id` the member id `new_member_id`, so that a
    /// restarted static member takes the place of its earlier instance, as Kafka's
    /// [`updateStaticMemberThenRebalanceOrCompleteJoin`] does. A forming group then rebalances,
    /// because the leader may already hold the old member id.
    ///
    /// [`updateStaticMemberThenRebalanceOrCompleteJoin`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java#L4938
    fn replace_static_member(
        mut self,
        now: SystemTime,
        old_member_id: &str,
        new_member_id: &str,
        request: &JoinRequest<'_>,
        metadata: &Bytes,
    ) -> (Self, Reply) {
        info!(old_member_id, new_member_id, "replacing static member");

        if let Some(mut member) = self.members.remove(old_member_id) {
            member.join_response.member_id = new_member_id.to_owned();
            _ = self.members.insert(new_member_id.to_owned(), member);
        }

        if self.state.leader() == Some(old_member_id) {
            self.state.set_leader(Some(new_member_id.to_owned()));
        }

        let was_joined = self
            .state
            .join_phase
            .as_mut()
            .is_some_and(|join_phase| join_phase.joined.remove(old_member_id));

        self.upsert_member(now, new_member_id, request, metadata);

        if self.state.join_phase.is_none() {
            self.prepare_rebalance(now);
        } else if was_joined {
            debug!(new_member_id, "the replaced member had joined");
        }

        self.mark_joined(new_member_id);
        self.complete_join_if_all_joined_after_join(now);
        let reply = self.reply_to_joined(new_member_id);
        (self, reply)
    }
}

impl<O> Inner<O, Formed>
where
    O: Storage,
{
    fn remove_expired(&mut self, now: SystemTime) -> bool {
        self.remove_expired_members(now, |_| false)
    }

    fn next_deadline(&self) -> Option<SystemTime> {
        [
            self.pending_sync
                .as_ref()
                .map(|pending_sync| pending_sync.deadline),
            self.earliest_pending_member_expiry(),
            self.earliest_session_expiry(|_| false),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// Returns the group in a join phase that starts now, with its protocol and its leader if the
    /// leader is still a member.
    fn into_preparing_rebalance(self, now: SystemTime) -> Inner<O, Forming> {
        let leader = self
            .members
            .contains_key(&self.state.leader)
            .then_some(self.state.leader)
            .or_else(|| self.members.keys().next().cloned());

        let mut forming = Inner {
            session_timeout_ms: self.session_timeout_ms,
            rebalance_timeout_ms: self.rebalance_timeout_ms,
            members: self.members,
            generation_id: self.generation_id,
            state: Forming {
                protocol: Some(Protocol {
                    protocol_type: self.state.protocol_type,
                    protocol_name: self.state.protocol_name,
                    leader,
                }),
                join_phase: None,
            },
            storage: self.storage,
            skip_assignment: self.skip_assignment,
            inception: self.inception,
            pending_members: self.pending_members,
            pending_sync: None,
        };

        // A stable group holds no SyncGroup, so no member's session starts again, as Kafka's
        // `prepareRebalance` answers held syncs only in COMPLETING_REBALANCE.
        forming.start_join_phase(now);
        forming
    }

    fn join_response(&self, member_id: &str, leader: &str, skip_assignment: bool) -> Body {
        debug!(join_outcome = ?ErrorCode::None, member_id, leader, generation_id = self.generation_id);

        JoinGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .generation_id(self.generation_id)
            .protocol_type(Some(self.state.protocol_type.clone()))
            .protocol_name(Some(self.state.protocol_name.clone()))
            .leader(leader.to_owned())
            .skip_assignment(Some(skip_assignment))
            .member_id(member_id.to_owned())
            .members(Some(if leader == member_id {
                self.member_list()
            } else {
                [].into()
            }))
            .into()
    }

    fn sync_response(&self, member_id: &str) -> Reply {
        debug!(sync_outcome = ?ErrorCode::None, sync_assignment = self.state.assignments.contains_key(member_id));

        Reply::Ready(
            SyncGroupResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::None.into())
                .protocol_type(Some(self.state.protocol_type.clone()))
                .protocol_name(Some(self.state.protocol_name.clone()))
                .assignment(
                    self.state
                        .assignments
                        .get(member_id)
                        .cloned()
                        .unwrap_or(Bytes::from_static(b"")),
                )
                .into(),
        )
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Member {
    join_response: JoinGroupResponseMember,
    last_contact: Option<SystemTime>,
    client_id: String,
    client_host: String,
}

impl From<&Member> for GroupMember {
    fn from(member: &Member) -> Self {
        Self {
            join_response: member.join_response.clone(),
            last_contact: member.last_contact,
            client_id: member.client_id.clone(),
            client_host: member.client_host.clone(),
        }
    }
}

impl From<&GroupMember> for Member {
    fn from(member: &GroupMember) -> Self {
        Self {
            join_response: member.join_response.clone(),
            last_contact: member.last_contact,
            client_id: member.client_id.clone(),
            client_host: member.client_host.clone(),
        }
    }
}

/// The protocol type and name that a group has settled on.
#[derive(Clone, Copy, Debug)]
struct GroupProtocol<'a> {
    protocol_type: &'a str,
    protocol_name: &'a str,
}

impl<'a> JoinRequest<'a> {
    /// Returns the joining protocol: the protocol of this request that the member joins with.
    /// It is the request's protocol named as the group's protocol, or the request's first for a
    /// group without a protocol.
    fn joining_protocol(
        &self,
        group_protocol: Option<GroupProtocol<'_>>,
    ) -> Result<&'a JoinGroupRequestProtocol, ErrorCode> {
        let protocols = self.protocols.ok_or(ErrorCode::InvalidRequest)?;

        match group_protocol {
            Some(group_protocol) => {
                if self.protocol_type != group_protocol.protocol_type {
                    return Err(ErrorCode::InconsistentGroupProtocol);
                }

                protocols
                    .iter()
                    .find(|protocol| protocol.name == group_protocol.protocol_name)
                    .ok_or(ErrorCode::InconsistentGroupProtocol)
            }

            None if self.protocol_type.is_empty() => Err(ErrorCode::InconsistentGroupProtocol),
            None => protocols
                .first()
                .ok_or(ErrorCode::InconsistentGroupProtocol),
        }
    }
}

/// How a `JoinGroup` without a member id joins.
enum NewMember {
    /// A dynamic member gets a member id with `MEMBER_ID_REQUIRED`, and joins with it.
    Pending(String),
    /// A dynamic member of a client too old for `MEMBER_ID_REQUIRED`, or a static member whose
    /// group instance id the group doesn't hold, joins at once.
    Added(String),
    /// A static member whose group instance id the group holds takes that member's place.
    Replacing { old: String, new: String },
}

impl NewMember {
    /// Decides how `request`, a `JoinGroup` without a member id, joins, as Kafka's
    /// [`classicGroupJoinNewDynamicMember`] and `classicGroupJoinNewStaticMember` decide.
    /// `static_member_id` is the member that holds the request's group instance id.
    ///
    /// [`classicGroupJoinNewDynamicMember`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java#L4084
    fn of(request: &JoinRequest<'_>, static_member_id: Option<&str>) -> Self {
        match request.group_instance_id {
            Some(group_instance_id) => {
                let new = new_member_id(Some(group_instance_id));

                match static_member_id {
                    Some(old) => NewMember::Replacing {
                        old: old.to_owned(),
                        new,
                    },
                    None => NewMember::Added(new),
                }
            }

            None if request.api_version >= MEMBER_ID_REQUIRED_VERSION => {
                NewMember::Pending(new_member_id(request.client_id))
            }

            None => NewMember::Added(new_member_id(request.client_id)),
        }
    }
}

fn member_id_required(member_id: String) -> Reply {
    debug!(member_id, join_outcome = ?ErrorCode::MemberIdRequired);

    Reply::Ready(
        JoinGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::MemberIdRequired.into())
            .generation_id(-1)
            .protocol_type(None)
            .protocol_name(Some("".into()))
            .leader("".into())
            .skip_assignment(Some(false))
            .member_id(member_id)
            .members(Some([].into()))
            .into(),
    )
}

#[async_trait::async_trait]
impl<O> Group for Inner<O, Forming>
where
    O: Storage,
{
    type JoinState = Inner<O, Forming>;
    type SyncState = Wrapper<O>;
    type HeartbeatState = Inner<O, Forming>;
    type LeaveState = Inner<O, Forming>;
    type OffsetCommitState = Inner<O, Forming>;
    type OffsetFetchState = Inner<O, Forming>;

    /// Handles `JoinGroup` as Kafka's [`classicGroupJoinExistingMember`] and the methods it calls
    /// do, in Kafka's Empty, PreparingRebalance and CompletingRebalance states.
    ///
    /// [`classicGroupJoinExistingMember`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java#L4134
    async fn join(
        mut self,
        now: SystemTime,
        request: &JoinRequest<'_>,
        initial_rebalance_delay: Duration,
    ) -> (Self::JoinState, Reply) {
        debug!(?request);

        let protocol =
            match request.joining_protocol(self.state.protocol.as_ref().map(|protocol| {
                GroupProtocol {
                    protocol_type: &protocol.protocol_type,
                    protocol_name: &protocol.protocol_name,
                }
            })) {
                Ok(protocol) => protocol,
                Err(error_code) => return (self, join_error(request.member_id, error_code)),
            };

        if request.member_id.is_empty() {
            return match NewMember::of(
                request,
                request
                    .group_instance_id
                    .and_then(|group_instance_id| self.static_member_id(group_instance_id)),
            ) {
                NewMember::Pending(member_id) => {
                    _ = self.pending_members.insert(
                        member_id.clone(),
                        now + positive_millis(request.session_timeout_ms)
                            .unwrap_or(FALLBACK_SESSION_TIMEOUT),
                    );
                    (self, member_id_required(member_id))
                }

                NewMember::Added(member_id) => {
                    self.add_member(now, &member_id, request, protocol, initial_rebalance_delay)
                }

                NewMember::Replacing { old, new } => {
                    self.replace_static_member(now, &old, &new, request, &protocol.metadata)
                }
            };
        }

        if self.pending_members.remove(request.member_id).is_some() {
            return self.add_member(
                now,
                request.member_id,
                request,
                protocol,
                initial_rebalance_delay,
            );
        }

        if let Err(error_code) = self.validate_member(request.member_id, request.group_instance_id)
        {
            return (self, join_error(request.member_id, error_code));
        }

        if self.is_completing_rebalance()
            && self.has_joined_with(request.member_id, &protocol.metadata)
        {
            // A member that rejoins with the same metadata while the group waits for the leader's
            // assignment, perhaps because it lost the response, gets the current generation.
            let body = self.join_response(request.member_id);
            return (self, Reply::Ready(body));
        }

        self.update_member(now, request.member_id, request, &protocol.metadata)
    }

    /// Handles `SyncGroup` as Kafka's [`classicGroupSyncToClassicGroup`] does. The leader's
    /// assignment ends the rebalance, and each other member waits for it.
    ///
    /// [`classicGroupSyncToClassicGroup`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java#L5099
    async fn sync(
        mut self,
        now: SystemTime,
        request: &SyncRequest<'_>,
    ) -> (Self::SyncState, Reply) {
        debug!(?request);

        if let Err(error_code) = self.validate_member(request.member_id, request.group_instance_id)
        {
            return (self.into(), sync_error(error_code));
        }

        if request.generation_id != self.generation_id {
            return (self.into(), sync_error(ErrorCode::IllegalGeneration));
        }

        if request
            .protocol_type
            .zip(self.state.protocol_type())
            .is_some_and(|(requested, group)| requested != group)
            || request
                .protocol_name
                .zip(self.state.protocol_name())
                .is_some_and(|(requested, group)| requested != group)
        {
            return (
                self.into(),
                sync_error(ErrorCode::InconsistentGroupProtocol),
            );
        }

        if self.state.join_phase.is_some() {
            return (self.into(), sync_error(ErrorCode::RebalanceInProgress));
        }

        self.forget_pending_sync(&[request.member_id]);

        let Some(Protocol {
            protocol_type,
            protocol_name,
            leader: Some(leader),
        }) = self.state.protocol.clone()
        else {
            return (self.into(), sync_error(ErrorCode::RebalanceInProgress));
        };

        if leader != request.member_id {
            return (
                self.into(),
                Reply::Held {
                    member_id: request.member_id.to_owned(),
                },
            );
        }

        // Kafka gives each member that the leader didn't assign to an empty assignment.
        let mut assignments = request
            .assignments
            .unwrap_or_default()
            .iter()
            .filter(|assignment| self.members.contains_key(&assignment.member_id))
            .map(|assignment| (assignment.member_id.clone(), assignment.assignment.clone()))
            .collect::<BTreeMap<_, _>>();

        for member_id in self.members.keys() {
            _ = assignments
                .entry(member_id.clone())
                .or_insert_with(|| Bytes::from_static(b""));
        }

        self.restart_sessions_of_synced_members(now);

        info!(
            generation_id = self.generation_id,
            leader, "the group is stable"
        );

        let formed = Inner {
            session_timeout_ms: self.session_timeout_ms,
            rebalance_timeout_ms: self.rebalance_timeout_ms,
            members: self.members,
            generation_id: self.generation_id,
            state: Formed {
                protocol_type,
                protocol_name,
                leader,
                assignments,
            },
            storage: self.storage,
            skip_assignment: self.skip_assignment,
            inception: self.inception,
            pending_members: self.pending_members,
            pending_sync: self.pending_sync,
        };

        let reply = formed.sync_response(request.member_id);
        (formed.into(), reply)
    }

    /// Handles `Heartbeat` as Kafka's [`classicGroupHeartbeatToClassicGroup`] does: a heartbeat
    /// during the join phase tells the member to rejoin.
    ///
    /// [`classicGroupHeartbeatToClassicGroup`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java#L5366
    async fn heartbeat(
        mut self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Self::HeartbeatState, Body) {
        debug!(group_id, generation_id, member_id, group_instance_id);

        if let Err(error_code) = self.validate_member(member_id, group_instance_id) {
            return (self, heartbeat_response(error_code));
        }

        if generation_id != self.generation_id {
            return (self, heartbeat_response(ErrorCode::IllegalGeneration));
        }

        if let Some(member) = self.members.get_mut(member_id) {
            member.last_contact = Some(now);
        }

        let error_code = if self.state.join_phase.is_some() {
            ErrorCode::RebalanceInProgress
        } else {
            ErrorCode::None
        };

        (self, heartbeat_response(error_code))
    }

    async fn leave(
        mut self,
        now: SystemTime,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Self::LeaveState, Body) {
        debug!(?group_id, member_id, ?members);

        let was_completing_rebalance = self.is_completing_rebalance();
        let (members, any_left) = self.remove_leaving_members(member_id, members);

        // A valid leave starts a rebalance of a group that waits for the leader's assignment, and
        // can end a join phase, except during the initial delay, as Kafka's
        // `classicGroupLeaveToClassicGroup` does. The leave of a pending member id counts too.
        if any_left {
            self.forget_removed_members();

            if self.state.join_phase.is_some() {
                self.complete_join_if_all_joined_after_join(now);
            } else if was_completing_rebalance {
                self.prepare_rebalance(now);
                self.complete_join_if_all_joined(now);
            }
        }

        let body = LeaveGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .members(Some(members))
            .into();

        (self, body)
    }

    #[instrument(skip(self, now, detail), fields(generation_id = detail.generation_id_or_member_epoch, member_id = detail.member_id))]
    fn check_offset_commit(
        mut self,
        now: SystemTime,
        detail: &OffsetCommit<'_>,
    ) -> (Self::OffsetCommitState, Option<ErrorCode>) {
        let is_completing_rebalance = self.is_completing_rebalance();
        let error_code = self.offset_commit_fence(now, detail, is_completing_rebalance);
        debug!(offset_commit_outcome = ?error_code);
        (self, error_code)
    }

    async fn offset_fetch(
        mut self,
        now: SystemTime,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Self::OffsetFetchState, Body) {
        let _ = now;
        debug!(group_id, ?topics, ?groups, ?require_stable);
        match self
            .fetch_offset(group_id, topics, groups, require_stable)
            .await
        {
            Ok(body) => (self, body),
            Err(error) => {
                warn!(group_id, ?topics, ?error);
                (
                    self,
                    offset_fetch_error_response(topics, groups, offset_fetch_error_code(&error)),
                )
            }
        }
    }
}

#[async_trait::async_trait]
impl<O> Group for Inner<O, Formed>
where
    O: Storage,
{
    type JoinState = Wrapper<O>;
    type SyncState = Inner<O, Formed>;
    type HeartbeatState = Inner<O, Formed>;
    type LeaveState = Wrapper<O>;
    type OffsetCommitState = Inner<O, Formed>;
    type OffsetFetchState = Inner<O, Formed>;

    /// Handles `JoinGroup` in Kafka's Stable state. A follower that rejoins with the same metadata
    /// gets the current generation. The leader, or a member with new metadata, starts a rebalance,
    /// and so does a new member.
    async fn join(
        mut self,
        now: SystemTime,
        request: &JoinRequest<'_>,
        initial_rebalance_delay: Duration,
    ) -> (Self::JoinState, Reply) {
        debug!(?request);

        let protocol = match request.joining_protocol(Some(GroupProtocol {
            protocol_type: &self.state.protocol_type,
            protocol_name: &self.state.protocol_name,
        })) {
            Ok(protocol) => protocol,
            Err(error_code) => return (self.into(), join_error(request.member_id, error_code)),
        };

        if request.member_id.is_empty() {
            return match NewMember::of(
                request,
                request
                    .group_instance_id
                    .and_then(|group_instance_id| self.static_member_id(group_instance_id)),
            ) {
                NewMember::Pending(member_id) => {
                    _ = self.pending_members.insert(
                        member_id.clone(),
                        now + positive_millis(request.session_timeout_ms)
                            .unwrap_or(FALLBACK_SESSION_TIMEOUT),
                    );
                    (self.into(), member_id_required(member_id))
                }

                NewMember::Added(member_id) => {
                    let (forming, reply) = self.into_preparing_rebalance(now).add_member(
                        now,
                        &member_id,
                        request,
                        protocol,
                        initial_rebalance_delay,
                    );
                    (forming.into(), reply)
                }

                NewMember::Replacing { old, new } => {
                    // A restarted static member takes its earlier instance's place, and gets that
                    // instance's assignment, without a rebalance.
                    let current_leader = self.state.leader.clone();

                    if let Some(mut member) = self.members.remove(&old) {
                        member.join_response.member_id = new.clone();
                        _ = self.members.insert(new.clone(), member);
                    }

                    if let Some(assignment) = self.state.assignments.remove(&old) {
                        _ = self.state.assignments.insert(new.clone(), assignment);
                    }

                    if self.state.leader == old {
                        self.state.leader = new.clone();
                    }

                    if let Some(pending_sync) = self.pending_sync.as_mut()
                        && pending_sync.members.remove(&old)
                    {
                        _ = pending_sync.members.insert(new.clone());
                    }

                    self.upsert_member(now, &new, request, &protocol.metadata);

                    info!(old, new, "replaced static member without a rebalance");

                    // From version 9, the response tells a restarted leader to skip the
                    // assignment. An older leader learns the old leader id, so that it syncs as a
                    // follower and does not assign again.
                    let body = if request.api_version >= SKIP_ASSIGNMENT_VERSION {
                        let is_leader = self.state.leader == new;
                        self.join_response(&new, &self.state.leader, is_leader)
                    } else {
                        self.join_response(&new, &current_leader, false)
                    };

                    (self.into(), Reply::Ready(body))
                }
            };
        }

        if self.pending_members.remove(request.member_id).is_some() {
            let (forming, reply) = self.into_preparing_rebalance(now).add_member(
                now,
                request.member_id,
                request,
                protocol,
                initial_rebalance_delay,
            );
            return (forming.into(), reply);
        }

        if let Err(error_code) = self.validate_member(request.member_id, request.group_instance_id)
        {
            return (self.into(), join_error(request.member_id, error_code));
        }

        if self.state.leader != request.member_id
            && self.has_joined_with(request.member_id, &protocol.metadata)
        {
            let body = self.join_response(request.member_id, &self.state.leader, false);
            return (self.into(), Reply::Ready(body));
        }

        info!(
            member_id = request.member_id,
            leader = self.state.leader,
            "rejoin starts a rebalance"
        );

        let (forming, reply) = self.into_preparing_rebalance(now).update_member(
            now,
            request.member_id,
            request,
            &protocol.metadata,
        );

        (forming.into(), reply)
    }

    async fn sync(
        mut self,
        now: SystemTime,
        request: &SyncRequest<'_>,
    ) -> (Self::SyncState, Reply) {
        debug!(?request);

        if let Err(error_code) = self.validate_member(request.member_id, request.group_instance_id)
        {
            return (self, sync_error(error_code));
        }

        if request.generation_id != self.generation_id {
            return (self, sync_error(ErrorCode::IllegalGeneration));
        }

        if request
            .protocol_type
            .is_some_and(|protocol_type| protocol_type != self.state.protocol_type)
            || request
                .protocol_name
                .is_some_and(|protocol_name| protocol_name != self.state.protocol_name)
        {
            return (self, sync_error(ErrorCode::InconsistentGroupProtocol));
        }

        if self
            .pending_sync
            .as_ref()
            .is_some_and(|pending_sync| pending_sync.members.contains(request.member_id))
        {
            self.forget_pending_sync(&[request.member_id]);

            if let Some(member) = self.members.get_mut(request.member_id) {
                member.last_contact = Some(now);
            }
        }

        let reply = self.sync_response(request.member_id);
        (self, reply)
    }

    async fn heartbeat(
        mut self,
        now: SystemTime,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> (Self::HeartbeatState, Body) {
        debug!(group_id, generation_id, member_id, group_instance_id);

        if let Err(error_code) = self.validate_member(member_id, group_instance_id) {
            return (self, heartbeat_response(error_code));
        }

        if generation_id != self.generation_id {
            return (self, heartbeat_response(ErrorCode::IllegalGeneration));
        }

        if let Some(member) = self.members.get_mut(member_id) {
            member.last_contact = Some(now);
        }

        (self, heartbeat_response(ErrorCode::None))
    }

    async fn leave(
        mut self,
        now: SystemTime,
        group_id: &str,
        member_id: Option<&str>,
        members: Option<&[MemberIdentity]>,
    ) -> (Self::LeaveState, Body) {
        debug!(?group_id, member_id, ?members);

        let (members, any_left) = self.remove_leaving_members(member_id, members);

        let body = LeaveGroupResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(ErrorCode::None.into())
            .members(Some(members))
            .into();

        if any_left {
            let mut forming = self.into_preparing_rebalance(now);
            forming.complete_join_if_all_joined(now);
            (forming.into(), body)
        } else {
            (self.into(), body)
        }
    }

    #[instrument(skip(self, now, detail), fields(generation_id = detail.generation_id_or_member_epoch, member_id = detail.member_id))]
    fn check_offset_commit(
        mut self,
        now: SystemTime,
        detail: &OffsetCommit<'_>,
    ) -> (Self::OffsetCommitState, Option<ErrorCode>) {
        let error_code = self.offset_commit_fence(now, detail, false);
        debug!(offset_commit_outcome = ?error_code);
        (self, error_code)
    }

    async fn offset_fetch(
        mut self,
        now: SystemTime,
        group_id: Option<&str>,
        topics: Option<&[OffsetFetchRequestTopic]>,
        groups: Option<&[OffsetFetchRequestGroup]>,
        require_stable: Option<bool>,
    ) -> (Self::OffsetFetchState, Body) {
        let _ = now;

        match self
            .fetch_offset(group_id, topics, groups, require_stable)
            .await
        {
            Ok(body) => (self, body),
            Err(error) => {
                warn!(group_id, ?topics, ?error);
                (
                    self,
                    offset_fetch_error_response(topics, groups, offset_fetch_error_code(&error)),
                )
            }
        }
    }
}

fn has_unique_elements<T>(iter: T) -> bool
where
    T: IntoIterator,
    T::Item: Eq + Hash,
{
    let mut uniq = HashSet::new();
    iter.into_iter().all(|x| uniq.insert(x))
}

fn offset_commit_response(detail: &OffsetCommit<'_>, error_code: ErrorCode) -> Body {
    OffsetCommitResponse::default()
        .throttle_time_ms(Some(0))
        .topics(detail.topics.map(|topics| {
            topics
                .iter()
                .map(|topic| {
                    OffsetCommitResponseTopic::default()
                        .name(topic.name.clone())
                        .partitions(topic.partitions.as_ref().map(|partitions| {
                            partitions
                                .iter()
                                .map(|partition| {
                                    OffsetCommitResponsePartition::default()
                                        .partition_index(partition.partition_index)
                                        .error_code(error_code.into())
                                })
                                .collect()
                        }))
                })
                .collect()
        }))
        .into()
}

/// Returns the leader epoch that `OffsetFetch` answers for a committed offset: the
/// committed epoch, or -1 when the commit did not give one, as Kafka does
/// ([`OffsetMetadataManager.fetchOffsets`]).
///
/// [`OffsetMetadataManager.fetchOffsets`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java#L775-L780
fn fetched_leader_epoch(committed: &CommittedOffset) -> i32 {
    committed.leader_epoch.unwrap_or(-1)
}

/// Returns the metadata that `OffsetFetch` answers for a committed offset: the
/// committed metadata, or `""` when the commit did not give any.
///
/// Kafka never answers a null metadata. It stores a null commit metadata as `""`
/// ([`OffsetAndMetadata.fromRequest`]), and it answers `""` for a partition without a
/// committed offset ([`OffsetMetadataManager.fetchOffsets`]).
///
/// [`OffsetAndMetadata.fromRequest`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetAndMetadata.java#L159-L171
/// [`OffsetMetadataManager.fetchOffsets`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java#L769-L774
fn fetched_metadata(committed: &CommittedOffset) -> String {
    committed.metadata.clone().unwrap_or_default()
}

/// Pairs each committed offset with the error code that `OffsetFetch` answers for its
/// partition.
///
/// Kafka answers a partition in `unstable` with `UNSTABLE_OFFSET_COMMIT`, offset -1, leader
/// epoch -1 and metadata `""`, in place of its committed offset ([`GroupMetadataManager.getOffsets`],
/// [`OffsetMetadataManager.fetchOffsets`], [`OffsetMetadataManager.fetchAllOffsets`]). A
/// request for every committed offset of the group lists only the partitions with a committed
/// offset, so a partition that only an open transaction has an offset for is not listed.
///
/// [`GroupMetadataManager.getOffsets`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala#L526-L531
/// [`OffsetMetadataManager.fetchOffsets`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java#L762-L768
/// [`OffsetMetadataManager.fetchAllOffsets`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java#L829-L835
fn fetched_offsets(
    offsets: BTreeMap<Topition, CommittedOffset>,
    unstable: &BTreeSet<Topition>,
) -> BTreeMap<Topition, (CommittedOffset, ErrorCode)> {
    offsets
        .into_iter()
        .map(|(topition, committed)| {
            let fetched = if unstable.contains(&topition) {
                (
                    CommittedOffset::uncommitted(),
                    ErrorCode::UnstableOffsetCommit,
                )
            } else {
                (committed, ErrorCode::None)
            };

            (topition, fetched)
        })
        .collect()
}

/// Returns the error code that an `OffsetFetch` reports when storage fails
/// with `error`.
///
/// A storage error that carries a code keeps that code. Each storage engine
/// reports a committed offset that it can't decode as `UNKNOWN_SERVER_ERROR`,
/// because a retry can't fix it. Any other storage error is treated as
/// transient, so the response reports `COORDINATOR_NOT_AVAILABLE`, which Kafka
/// clients [retry].
///
/// [retry]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java#L1460-L1464
fn offset_fetch_error_code(error: &Error) -> ErrorCode {
    match error {
        Error::Storage(nisshi_storage::Error::Api(error_code)) => *error_code,
        _ => ErrorCode::CoordinatorNotAvailable,
    }
}

/// Returns an `OffsetFetch` response that reports `error_code` for every
/// requested partition and group.
///
/// Each version reads the error from a different field: version 0 and 1 from
/// each partition, versions 2 to 7 from the top level, and version 8 onward
/// from each group. The response sets all of them, and the encoder writes only
/// the fields of the negotiated version.
fn offset_fetch_error_response(
    topics: Option<&[OffsetFetchRequestTopic]>,
    groups: Option<&[OffsetFetchRequestGroup]>,
    error_code: ErrorCode,
) -> Body {
    OffsetFetchResponse::default()
        .throttle_time_ms(Some(0))
        .topics(topics.map(|topics| {
            topics
                .iter()
                .map(|topic| {
                    OffsetFetchResponseTopic::default()
                        .name(topic.name.clone())
                        .partitions(Some(
                            topic
                                .partition_indexes
                                .as_deref()
                                .unwrap_or_default()
                                .iter()
                                .map(|partition_index| {
                                    OffsetFetchResponsePartition::default()
                                        .partition_index(*partition_index)
                                        .committed_offset(-1)
                                        .committed_leader_epoch(Some(-1))
                                        .metadata(Some("".into()))
                                        .error_code(error_code.into())
                                })
                                .collect(),
                        ))
                })
                .collect()
        }))
        .error_code(Some(error_code.into()))
        .groups(groups.map(|groups| {
            groups
                .iter()
                .map(|group| {
                    OffsetFetchResponseGroup::default()
                        .group_id(group.group_id.clone())
                        .topics(Some([].into()))
                        .error_code(error_code.into())
                })
                .collect()
        }))
        .into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forming_without_a_protocol_has_no_leader() {
        let forming = Forming::from_parts(None, None, Some("member".into()));
        assert_eq!(None, forming.leader());
        assert_eq!(None, forming.protocol_type());

        let forming = Forming::from_parts(Some("consumer".into()), None, Some("member".into()));
        assert_eq!(None, forming.leader());
    }

    #[test]
    fn forming_keeps_the_leader_with_its_protocol() {
        let forming = Forming::from_parts(
            Some("consumer".into()),
            Some("range".into()),
            Some("member".into()),
        );

        assert_eq!(Some("consumer"), forming.protocol_type());
        assert_eq!(Some("range"), forming.protocol_name());
        assert_eq!(Some("member"), forming.leader());
    }
    use nisshi_sans_io::consumer::CONSUMER;
    use nisshi_storage::{ArcDynStorage, StorageContainer};
    use pretty_assertions::assert_eq;
    use tracing::subscriber::DefaultGuard;
    use url::Url;

    #[cfg(miri)]
    fn init_tracing() -> Result<()> {
        Ok(())
    }

    #[cfg(not(miri))]
    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        use tracing::Level;
        use tracing_subscriber::fmt::format::FmtSpan;

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_max_level(Level::DEBUG)
                .with_span_events(FmtSpan::ACTIVE)
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    async fn storage(cluster: &str, node: i32) -> Result<ArcDynStorage> {
        let builder = {
            let mut builder = StorageContainer::builder();

            builder.with_factory(Arc::new(nisshi_storage_null::EngineFactory));

            #[cfg(feature = "dynostore")]
            builder.with_factory(Arc::new(nisshi_storage_dynostore::MemoryEngineFactory));

            #[cfg(feature = "dynostore")]
            builder.with_factory(Arc::new(
                nisshi_storage_dynostore::S3OptimisticConcurrencyEngineFactory,
            ));

            #[cfg(feature = "dynostore")]
            builder.with_factory(Arc::new(
                nisshi_storage_dynostore::GoogleCloudStorageEngineFactory,
            ));

            #[cfg(feature = "libsql")]
            builder.with_factory(Arc::new(nisshi_storage_sql::LiteEngineFactory));

            #[cfg(feature = "postgres")]
            builder.with_factory(Arc::new(nisshi_storage_sql::PostgresEngineFactory));

            #[cfg(feature = "slatedb")]
            builder.with_factory(Arc::new(nisshi_storage_slatedb::EngineFactory));

            #[cfg(feature = "turso")]
            builder.with_factory(Arc::new(nisshi_storage_sql::LimboEngineFactory));

            builder
        };

        builder
            .cluster_id(cluster)
            .node_id(node)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092/")?)
            .schema_registry(None)
            .storage(Url::parse("memory://")?)
            .build()
            .await
            .map_err(Into::into)
    }

    #[tokio::test]
    async fn join_fresh_group_with_empty_protocols_is_inconsistent_group_protocol() -> Result<()> {
        let _guard = init_tracing()?;

        let session_timeout_ms = 45_000;
        let rebalance_timeout_ms = Some(300_000);
        let group_instance_id = None;
        let reason = None;

        let cluster = "abc";
        let node = 12321;

        const CLIENT_ID: &str = "console-consumer";
        const GROUP_ID: &str = "test-consumer-group-empty-protocols";

        let s = Controller::with_storage(storage(cluster, node).await?)?;

        // A fresh group (never joined before) receiving a JoinGroupRequest whose
        // `protocols` array is present but empty must not panic indexing `protocols[0]`;
        // it should be rejected the same way the already-negotiated-group path rejects
        // an unmatched/absent protocol: InconsistentGroupProtocol.
        assert_eq!(
            Body::from(
                JoinGroupResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(ErrorCode::InconsistentGroupProtocol.into())
                    .generation_id(-1)
                    .protocol_type(None)
                    .protocol_name(Some("".into()))
                    .leader("".into())
                    .skip_assignment(Some(false))
                    .member_id("".into())
                    .members(Some([].into()))
            ),
            s.join(
                9,
                Some(CLIENT_ID),
                None,
                GROUP_ID,
                session_timeout_ms,
                rebalance_timeout_ms,
                "",
                group_instance_id,
                CONSUMER,
                Some(&[]),
                reason,
            )
            .await?
        );

        Ok(())
    }
}

#[cfg(test)]
mod cache_tests;

#[cfg(test)]
mod rebalance_tests;
