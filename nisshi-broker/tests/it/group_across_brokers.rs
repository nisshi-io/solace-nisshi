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

//! A consumer group whose members send each request to a different broker, as a load balancer
//! in front of brokers that share storage can.
//!
//! Each member sends its requests to two brokers in turn, so the two requests of a join (the
//! `MEMBER_ID_REQUIRED` round and the rejoin), the `SyncGroup` and each heartbeat land on
//! different brokers. A real load balancer spreads connections rather than requests, so this is
//! the harsher case. Each rebalance must end in one generation that every member joined, and each
//! member must get the assignment that the leader made for it in that generation.

use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

use bytes::Bytes;
use nisshi_broker::{
    Result,
    coordinator::group::administrator::{Controller, GroupCoordinatorConfig},
};
use nisshi_sans_io::{
    ErrorCode, join_group_request::JoinGroupRequestProtocol,
    sync_group_request::SyncGroupRequestAssignment,
};
use nisshi_storage::Storage;
use rand::{prelude::*, rng};
use tokio::{task::JoinHandle, time::sleep};
use url::Url;
use uuid::Uuid;

use crate::common::{
    self, CLIENT_ID, PROTOCOL_TYPE, RANGE, StorageType, alphanumeric_string, heartbeat,
    init_tracing, join_group, leave, random_bytes, sync_group,
};

const SESSION_TIMEOUT_MS: i32 = 45_000;

const REBALANCE_TIMEOUT_MS: i32 = 300_000;

/// A short poll interval, so that each rebalance ends quickly. The initial delay must outlast
/// the four members' first joins, two requests each, on a slow storage: a member that joins after
/// the first join phase ends starts another generation, and the test expects one.
const GROUP_COORDINATOR: GroupCoordinatorConfig = GroupCoordinatorConfig {
    initial_rebalance_delay: Duration::from_secs(1),
    poll_interval: Duration::from_millis(100),
};

/// How long a member sends heartbeats for, at most, until the coordinator reports a rebalance.
const HEARTBEAT_WAIT: Duration = Duration::from_secs(5);

/// Two brokers that share one storage.
struct Brokers<G> {
    brokers: [Controller<G>; 2],
}

impl<G> Clone for Brokers<G>
where
    G: Storage + Clone,
{
    fn clone(&self) -> Self {
        Self {
            brokers: self.brokers.clone(),
        }
    }
}

impl<G> Brokers<G>
where
    G: Storage + Clone,
{
    fn new(storage: G) -> Self {
        Self {
            brokers: [
                Controller::new(storage.clone(), GROUP_COORDINATOR),
                Controller::new(storage, GROUP_COORDINATOR),
            ],
        }
    }

    /// Returns the broker for a member's request number `request`: each request of a member goes
    /// to the other broker than its request before.
    fn for_request(&self, request: usize) -> Controller<G> {
        self.brokers[request % 2].clone()
    }
}

/// A member of the group, as its client knows it after a join.
#[derive(Clone, Debug)]
struct Member {
    id: String,
    generation: i32,
    is_leader: bool,
    /// The member ids that the leader's join response lists, empty for a follower.
    member_ids: BTreeSet<String>,
    protocols: Vec<JoinGroupRequestProtocol>,
    /// How many requests the member has sent, which picks the broker of its next request.
    requests: usize,
}

/// Returns the assignment that the leader gives `member_id` in `generation`, so that a member can
/// check that its assignment belongs to the generation that it joined.
fn assignment(generation: i32, member_id: &str) -> Bytes {
    Bytes::from(format!("generation {generation}, member {member_id}"))
}

/// Sends `JoinGroup` as `member_id`, or as a new member when it is empty, as request number
/// `request` of the member, and returns the response's error code and member id with the member
/// that the response describes.
async fn join_request<G>(
    brokers: &Brokers<G>,
    group: &str,
    member_id: &str,
    protocols: Vec<JoinGroupRequestProtocol>,
    request: usize,
) -> Result<(ErrorCode, Member)>
where
    G: Storage + Clone,
{
    let response = join_group(
        &mut brokers.for_request(request),
        Some(CLIENT_ID),
        group,
        SESSION_TIMEOUT_MS,
        Some(REBALANCE_TIMEOUT_MS),
        member_id,
        None,
        PROTOCOL_TYPE,
        Some(&protocols),
        None,
    )
    .await?;

    Ok((
        ErrorCode::try_from(response.error_code)?,
        Member {
            is_leader: !response.leader.is_empty() && response.member_id == response.leader,
            member_ids: response
                .members
                .unwrap_or_default()
                .into_iter()
                .map(|member| member.member_id)
                .collect(),
            id: response.member_id,
            generation: response.generation_id,
            protocols,
            requests: request + 1,
        },
    ))
}

/// Joins `group` as a new member: the `MEMBER_ID_REQUIRED` round through one broker, and the
/// rejoin with the member id through the other. `first_request` picks the broker of the first.
async fn join_new<G>(brokers: Brokers<G>, group: String, first_request: usize) -> Result<Member>
where
    G: Storage + Clone,
{
    let protocols = vec![
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(random_bytes(15)),
    ];

    let (error_code, required) =
        join_request(&brokers, &group, "", protocols.clone(), first_request).await?;
    assert_eq!(ErrorCode::MemberIdRequired, error_code);

    rejoin(brokers, group, required).await
}

/// Rejoins `group` as `member`, with its member id and protocols, through the next broker.
async fn rejoin<G>(brokers: Brokers<G>, group: String, member: Member) -> Result<Member>
where
    G: Storage + Clone,
{
    let (error_code, joined) = join_request(
        &brokers,
        &group,
        &member.id,
        member.protocols,
        member.requests,
    )
    .await?;
    assert_eq!(ErrorCode::None, error_code, "join of {}", member.id);

    Ok(joined)
}

/// Sends `SyncGroup` as `member` through the next broker, and returns the member with its
/// assignment. The leader assigns each member that its join response lists.
async fn sync<G>(brokers: Brokers<G>, group: String, mut member: Member) -> Result<(Member, Bytes)>
where
    G: Storage + Clone,
{
    let assignments = member
        .member_ids
        .iter()
        .map(|member_id| {
            SyncGroupRequestAssignment::default()
                .member_id(member_id.clone())
                .assignment(assignment(member.generation, member_id))
        })
        .collect::<Vec<_>>();

    let response = sync_group(
        &mut brokers.for_request(member.requests),
        &group,
        member.generation,
        &member.id,
        None,
        PROTOCOL_TYPE,
        RANGE,
        &assignments,
    )
    .await?;
    member.requests += 1;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(response.error_code)?,
        "sync of {}",
        member.id
    );

    Ok((member, response.assignment))
}

/// Sends heartbeats as `member`, each through the next broker, until the coordinator reports a
/// rebalance.
async fn wait_for_rebalance<G>(brokers: &Brokers<G>, group: &str, member: &mut Member) -> Result<()>
where
    G: Storage + Clone,
{
    let started = Instant::now();

    loop {
        let response = heartbeat(
            &mut brokers.for_request(member.requests),
            group,
            member.generation,
            &member.id,
            None,
        )
        .await?;
        member.requests += 1;

        match ErrorCode::try_from(response.error_code)? {
            ErrorCode::RebalanceInProgress => return Ok(()),
            ErrorCode::None if started.elapsed() < HEARTBEAT_WAIT => {
                sleep(Duration::from_millis(50)).await
            }
            otherwise => panic!("heartbeat of {} answered {otherwise:?}", member.id),
        }
    }
}

/// Checks one completed join phase: every member joined the same generation, one later than
/// `previous_generation`, and the leader's response lists exactly these members. Then every
/// member syncs, and each gets the assignment that the leader made for it in that generation.
/// Returns the members.
async fn complete_rebalance<G>(
    brokers: &Brokers<G>,
    group: &str,
    joins: Vec<JoinHandle<Result<Member>>>,
    previous_generation: i32,
) -> Result<Vec<Member>>
where
    G: Storage + Clone + 'static,
{
    let mut members = Vec::new();
    for join in joins {
        members.push(join.await??);
    }

    let generation = members[0].generation;
    assert_eq!(previous_generation + 1, generation);
    assert!(
        members.iter().all(|member| member.generation == generation),
        "members joined different generations: {members:?}"
    );

    let leaders = members
        .iter()
        .filter(|member| member.is_leader)
        .collect::<Vec<_>>();
    assert_eq!(1, leaders.len(), "{members:?}");
    assert_eq!(
        members
            .iter()
            .map(|member| member.id.clone())
            .collect::<BTreeSet<_>>(),
        leaders[0].member_ids
    );

    let syncs = members
        .into_iter()
        .map(|member| tokio::spawn(sync(brokers.clone(), group.to_owned(), member)))
        .collect::<Vec<_>>();

    let mut synced = Vec::new();
    for sync in syncs {
        let (member, assigned) = sync.await??;
        assert_eq!(assignment(generation, &member.id), assigned);
        synced.push(member);
    }

    Ok(synced)
}

/// Rejoins each of `members` with its member id, each through the other broker than its last
/// request.
fn rejoin_all<G>(
    brokers: &Brokers<G>,
    group: &str,
    members: &[Member],
) -> Vec<JoinHandle<Result<Member>>>
where
    G: Storage + Clone + 'static,
{
    members
        .iter()
        .map(|member| tokio::spawn(rejoin(brokers.clone(), group.to_owned(), member.clone())))
        .collect()
}

/// Four members form a group, one leaves, and a new one joins, with each request of each member
/// sent to the other broker than its request before. Each rebalance ends in one generation that
/// every member joined, with each member's assignment from that generation.
async fn rebalances_end_in_one_generation_when_requests_alternate_brokers<G>(
    storage: G,
) -> Result<()>
where
    G: Storage + Clone + 'static,
{
    const MEMBERS: usize = 4;

    let brokers = Brokers::new(storage);
    let group = alphanumeric_string(15);

    let joins = (0..MEMBERS)
        .map(|first_request| tokio::spawn(join_new(brokers.clone(), group.clone(), first_request)))
        .collect();
    let mut members = complete_rebalance(&brokers, &group, joins, 0).await?;

    let leaving = members.pop().expect("a member");
    let left = leave(
        &mut brokers.for_request(leaving.requests),
        &group,
        &leaving.id,
        None,
    )
    .await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(left.error_code)?);

    for member in &mut members {
        wait_for_rebalance(&brokers, &group, member).await?;
    }
    let generation = members[0].generation;
    let joins = rejoin_all(&brokers, &group, &members);
    let mut members = complete_rebalance(&brokers, &group, joins, generation).await?;

    let generation = members[0].generation;
    let mut joins = vec![tokio::spawn(join_new(brokers.clone(), group.clone(), 0))];
    for member in &mut members {
        wait_for_rebalance(&brokers, &group, member).await?;
    }
    joins.extend(rejoin_all(&brokers, &group, &members));
    let members = complete_rebalance(&brokers, &group, joins, generation).await?;

    assert_eq!(MEMBERS, members.len());

    Ok(())
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage_type:expr) => {
        #[cfg(feature = $feature)]
        mod $name {
            use super::*;

            #[tokio::test]
            async fn rebalances_end_in_one_generation_when_requests_alternate_brokers()
            -> Result<()> {
                let _guard = init_tracing()?;

                super::rebalances_end_in_one_generation_when_requests_alternate_brokers(
                    common::storage_container(
                        $storage_type,
                        Uuid::now_v7(),
                        rng().random_range(0..i32::MAX),
                        Url::parse("tcp://127.0.0.1/")?,
                        None,
                    )
                    .await?,
                )
                .await
            }
        }
    };
}

backend!(in_memory, "dynostore", StorageType::InMemory);
backend!(lite, "libsql", StorageType::Lite);
backend!(turso, "turso", StorageType::Turso);
backend!(pg, "postgres", StorageType::Postgres);
backend!(slatedb, "slatedb", StorageType::SlateDb);
