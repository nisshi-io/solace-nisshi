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

//! How long the group coordinator holds `JoinGroup` and `SyncGroup` in a rebalance, compared with
//! Kafka's classic group coordinator.
//!
//! Kafka holds the first join of a new group for `group.initial.rebalance.delay.ms`, so that the
//! other members can join the same generation. A rebalance of a group with members ends as soon as
//! every member has rejoined. Each test measures the time on the wall clock, because a client sees
//! that time.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use nisshi_broker::{Error, Result, coordinator::group::administrator::Controller};
use nisshi_sans_io::{ErrorCode, sync_group_request::SyncGroupRequestAssignment};
use nisshi_storage::{
    LatencyIntroducingStorage, Request, RequestChannelService, RequestStorageService, Storage,
    bounded_channel,
};
use rama::Service as _;
use rand::{prelude::*, rng};
use tokio::{task::JoinHandle, time::sleep};
use url::Url;
use uuid::Uuid;

use crate::common::{
    self, JoinResponse, PROTOCOL_TYPE, RANGE, StorageType, alphanumeric_string, heartbeat,
    init_tracing, join, sync_group,
};

/// The Java client's default `session.timeout.ms`.
const SESSION_TIMEOUT_MS: i32 = 45_000;

/// The Java client's default `max.poll.interval.ms`, which it sends as the rebalance timeout.
const REBALANCE_TIMEOUT_MS: i32 = 300_000;

/// Kafka's default `group.initial.rebalance.delay.ms`.
const INITIAL_REBALANCE_DELAY: Duration = Duration::from_secs(3);

/// How much later than Kafka the coordinator may answer, for its round trips to storage and for a
/// loaded CI runner.
const ALLOWED_DELAY: Duration = Duration::from_millis(1_500);

/// How long a test sends heartbeats for, at most, until the coordinator reports a rebalance.
const HEARTBEAT_WAIT: Duration = Duration::from_secs(5);

/// How long each member of the shared-storage test waits after the previous one before it
/// rejoins, so that the rebalance can end only when the last member rejoins.
const REJOIN_STAGGER: Duration = Duration::from_millis(300);

/// How long the shared-storage test holds a join while nothing changes: three of the
/// coordinator's default poll intervals.
const IDLE_HOLD: Duration = Duration::from_millis(1_500);

/// Returns `storage` behind a channel that counts each `update_group`, with 50 ms to 150 ms of
/// latency on every storage call, as two brokers would see a shared database.
fn counting_storage<G>(storage: G, writes: Arc<AtomicUsize>) -> RequestChannelService
where
    G: Storage + Clone + 'static,
{
    let (sender, mut receiver) = bounded_channel(64);
    let service = RequestStorageService::new(LatencyIntroducingStorage::new(storage));

    _ = tokio::spawn(async move {
        while let Some((request, reply)) = receiver.recv().await {
            if matches!(request, Request::UpdateGroup { .. }) {
                _ = writes.fetch_add(1, Ordering::SeqCst);
            }

            let service = service.clone();

            _ = tokio::spawn(async move {
                if let Ok(response) = service.serve(request).await {
                    _ = reply.send(response);
                }
            });
        }
    });

    RequestChannelService::new(sender)
}

/// Joins `group` as a new dynamic member, in a task, so that several members can wait in the same
/// join phase.
fn spawn_join<G>(controller: &Controller<G>, group: &str) -> JoinHandle<Result<JoinResponse>>
where
    G: Storage + Clone,
{
    let mut controller = controller.clone();
    let group = group.to_owned();

    tokio::spawn(async move {
        join(
            &mut controller,
            &group,
            None,
            None,
            None,
            SESSION_TIMEOUT_MS,
            Some(REBALANCE_TIMEOUT_MS),
        )
        .await
    })
}

/// Rejoins `group` as `member`, with the protocols that it joined with, in a task.
fn spawn_rejoin<G>(
    controller: &Controller<G>,
    group: &str,
    member: &JoinResponse,
) -> JoinHandle<Result<JoinResponse>>
where
    G: Storage + Clone,
{
    let mut controller = controller.clone();
    let group = group.to_owned();
    let member_id = member.id().to_owned();
    let protocols = member.protocols().to_vec();

    tokio::spawn(async move {
        join(
            &mut controller,
            &group,
            Some(&member_id),
            None,
            Some(protocols),
            SESSION_TIMEOUT_MS,
            Some(REBALANCE_TIMEOUT_MS),
        )
        .await
    })
}

/// Sends `SyncGroup` for `member`, in a task. The leader's request assigns a partition of its own
/// to each of `members`.
fn spawn_sync<G>(
    controller: &Controller<G>,
    group: &str,
    member: &JoinResponse,
    members: &[&JoinResponse],
) -> JoinHandle<Result<Bytes>>
where
    G: Storage + Clone,
{
    let mut controller = controller.clone();
    let group = group.to_owned();
    let member = member.clone();
    let assignments = if member.is_leader() {
        members
            .iter()
            .enumerate()
            .map(|(partition, member)| {
                SyncGroupRequestAssignment::default()
                    .member_id(member.id().to_owned())
                    .assignment(Bytes::from(format!("partition-{partition}")))
            })
            .collect()
    } else {
        Vec::new()
    };

    tokio::spawn(async move {
        let response = sync_group(
            &mut controller,
            &group,
            member.generation(),
            member.id(),
            None,
            PROTOCOL_TYPE,
            RANGE,
            &assignments,
        )
        .await?;

        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(response.error_code)?,
            "sync of {}",
            member.id()
        );

        Ok(response.assignment)
    })
}

/// Joins two dynamic members to a new group and syncs them, and returns the leader and the
/// follower.
async fn form_group_of_two<G>(
    controller: &Controller<G>,
    group: &str,
) -> Result<(JoinResponse, JoinResponse)>
where
    G: Storage + Clone,
{
    let first = spawn_join(controller, group);
    let second = spawn_join(controller, group);

    let (first, second) = (first.await??, second.await??);
    assert_eq!(
        first.generation(),
        second.generation(),
        "the two members joined different generations"
    );

    let (leader, follower) = if first.is_leader() {
        (first, second)
    } else {
        (second, first)
    };
    assert!(!follower.is_leader(), "both members lead the group");

    let leader_sync = spawn_sync(controller, group, &leader, &[&leader, &follower]);
    let follower_sync = spawn_sync(controller, group, &follower, &[]);

    assert!(
        !leader_sync.await??.is_empty(),
        "the leader got an empty assignment"
    );
    assert!(
        !follower_sync.await??.is_empty(),
        "the follower got an empty assignment"
    );

    Ok((leader, follower))
}

/// Sends heartbeats for `member` until the coordinator answers `REBALANCE_IN_PROGRESS`.
async fn wait_for_rebalance_in_progress<G>(
    controller: &Controller<G>,
    group: &str,
    member: &JoinResponse,
) -> Result<()>
where
    G: Storage + Clone,
{
    let mut controller = controller.clone();
    let deadline = Instant::now() + HEARTBEAT_WAIT;

    loop {
        let response = heartbeat(
            &mut controller,
            group,
            member.generation(),
            member.id(),
            None,
        )
        .await?;

        match ErrorCode::try_from(response.error_code)? {
            ErrorCode::RebalanceInProgress => return Ok(()),
            ErrorCode::None if Instant::now() < deadline => sleep(Duration::from_millis(50)).await,
            otherwise => {
                return Err(Error::Message(format!(
                    "heartbeat of {} answered {otherwise:?}, not REBALANCE_IN_PROGRESS",
                    member.id()
                )));
            }
        }
    }
}

/// The first member of a new group waits for `group.initial.rebalance.delay.ms`, so that other
/// members can join the same generation, and no longer.
async fn first_join_of_a_new_group_waits_for_the_initial_delay<G>(storage: G) -> Result<()>
where
    G: Storage + Clone,
{
    let controller = Controller::with_storage(storage)?;
    let group = alphanumeric_string(15);

    let started_at = Instant::now();
    let member = spawn_join(&controller, &group).await??;
    let elapsed = started_at.elapsed();

    assert!(member.is_leader());
    assert!(
        elapsed >= INITIAL_REBALANCE_DELAY,
        "the first join was answered after {elapsed:?}, before the initial delay of \
         {INITIAL_REBALANCE_DELAY:?}"
    );
    assert!(
        elapsed <= INITIAL_REBALANCE_DELAY + ALLOWED_DELAY,
        "the first join was answered after {elapsed:?}, allowed {:?}: the initial delay of \
         {INITIAL_REBALANCE_DELAY:?} and {ALLOWED_DELAY:?}",
        INITIAL_REBALANCE_DELAY + ALLOWED_DELAY
    );

    Ok(())
}

/// The leader of a stable group rejoins, which starts a rebalance. The follower learns of it from
/// its heartbeat and rejoins. The join phase ends as soon as both have rejoined, without a fixed
/// wait, and starts the next generation.
async fn rebalance_ends_once_every_member_has_rejoined<G>(storage: G) -> Result<()>
where
    G: Storage + Clone,
{
    let controller = Controller::with_storage(storage)?;
    let group = alphanumeric_string(15);

    let (leader, follower) = form_group_of_two(&controller, &group).await?;

    let started_at = Instant::now();
    let leader_rejoin = spawn_rejoin(&controller, &group, &leader);
    wait_for_rebalance_in_progress(&controller, &group, &follower).await?;
    let follower_rejoin = spawn_rejoin(&controller, &group, &follower);

    let (leader_rejoined, follower_rejoined) = (leader_rejoin.await??, follower_rejoin.await??);
    let elapsed = started_at.elapsed();

    assert!(
        elapsed <= ALLOWED_DELAY,
        "the rebalance ended {elapsed:?} after it started, although every member rejoined at \
         once, allowed {ALLOWED_DELAY:?}"
    );
    assert_eq!(leader.generation() + 1, leader_rejoined.generation());
    assert_eq!(leader_rejoined.generation(), follower_rejoined.generation());

    Ok(())
}

/// Four members of a group are split across two brokers that share storage. When the leader
/// rejoins, the others rejoin one after another, each on its own broker. The join phase ends as
/// soon as the last member rejoins, on whichever broker it uses. While a join is held and nothing
/// changes, the brokers only read the group: a held request doesn't write it on each poll.
async fn rebalance_across_brokers_ends_once_the_last_member_rejoins<G>(storage: G) -> Result<()>
where
    G: Storage + Clone + 'static,
{
    const MEMBERS: usize = 4;

    let writes = Arc::new(AtomicUsize::new(0));
    let shared = counting_storage(storage, writes.clone());
    let brokers = [
        Controller::with_storage(shared.clone())?,
        Controller::with_storage(shared)?,
    ];
    let group = alphanumeric_string(15);

    let joins = (0..MEMBERS)
        .map(|index| spawn_join(&brokers[index % 2], &group))
        .collect::<Vec<_>>();

    let mut members = Vec::new();
    for (index, join) in joins.into_iter().enumerate() {
        members.push((join.await??, &brokers[index % 2]));
    }

    let leader = members
        .iter()
        .position(|(member, _)| member.is_leader())
        .expect("a leader");
    let all = members.iter().map(|(member, _)| member).collect::<Vec<_>>();

    let syncs = members
        .iter()
        .map(|(member, broker)| spawn_sync(broker, &group, member, &all))
        .collect::<Vec<_>>();

    for sync in syncs {
        assert!(!sync.await??.is_empty());
    }

    let (leader_member, leader_broker) = &members[leader];
    let mut rejoins = vec![spawn_rejoin(leader_broker, &group, leader_member)];

    let followers = members
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != leader)
        .map(|(_, member)| member)
        .collect::<Vec<_>>();

    for (member, broker) in &followers {
        wait_for_rebalance_in_progress(broker, &group, member).await?;
    }

    // The leader's join is held while nothing else happens, so its broker only reads the group.
    let writes_before_idle = writes.load(Ordering::SeqCst);
    sleep(IDLE_HOLD).await;
    let writes_while_idle = writes.load(Ordering::SeqCst) - writes_before_idle;
    assert_eq!(
        0, writes_while_idle,
        "the brokers wrote the group {writes_while_idle} times while a join was held for \
         {IDLE_HOLD:?} and nothing changed"
    );

    for (member, broker) in &followers {
        sleep(REJOIN_STAGGER).await;
        rejoins.push(spawn_rejoin(broker, &group, member));
    }

    let last_rejoined_at = Instant::now();

    for rejoin in rejoins {
        assert_eq!(leader_member.generation() + 1, rejoin.await??.generation());
    }

    let elapsed = last_rejoined_at.elapsed();
    println!("the rebalance ended {elapsed:?} after the last rejoin");

    assert!(
        elapsed <= ALLOWED_DELAY,
        "the rebalance ended {elapsed:?} after the last member rejoined, allowed {ALLOWED_DELAY:?}"
    );

    Ok(())
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage_type:expr) => {
        #[cfg(feature = $feature)]
        mod $name {
            use nisshi_storage::ArcDynStorage;

            use super::*;

            async fn storage_container() -> Result<ArcDynStorage> {
                common::storage_container(
                    $storage_type,
                    Uuid::now_v7(),
                    rng().random_range(0..i32::MAX),
                    Url::parse("tcp://127.0.0.1/")?,
                    None,
                )
                .await
            }

            #[tokio::test]
            async fn first_join_of_a_new_group_waits_for_the_initial_delay() -> Result<()> {
                let _guard = init_tracing()?;
                super::first_join_of_a_new_group_waits_for_the_initial_delay(
                    storage_container().await?,
                )
                .await
            }

            #[tokio::test]
            async fn rebalance_ends_once_every_member_has_rejoined() -> Result<()> {
                let _guard = init_tracing()?;
                super::rebalance_ends_once_every_member_has_rejoined(storage_container().await?)
                    .await
            }

            #[tokio::test]
            async fn rebalance_across_brokers_ends_once_the_last_member_rejoins() -> Result<()> {
                let _guard = init_tracing()?;
                super::rebalance_across_brokers_ends_once_the_last_member_rejoins(
                    storage_container().await?,
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
