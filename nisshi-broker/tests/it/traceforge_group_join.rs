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

//! Phase 1: three consumer-group members
//! joining a fresh group, in every order, each independently racing a
//! simulated "another concurrent request already emptied the local cache"
//! condition, against the real `Wrapper`/`Inner` group state machine and the
//! real dynostore `Storage::update_group`.
//!
//! This applies the same nondet()-sequencing technique where it earns its
//! keep: with 3 members there are `3! = 6` join orders and 2 independent
//! cache-hit/miss decisions per member (`2^3 = 8`), 48 combinations in total.
//! Hand-writing a test per combination the way
//! `update_group_conditional::concurrent_same_version` does for 2 writers
//! would not scale; exposing "join order" and "did this member's request see
//! a stale/empty cache" as `nondet()` choices lets TraceForge exhaust all of
//! them automatically while every step still calls the genuine `Wrapper::
//! join` and `Storage::update_group` implementations.
//!
//! What's modeled and what isn't: this drives `Wrapper<O>::join` and
//! `Storage::update_group` directly, skipping `Controller::join`'s outer
//! request-handler loop (which holds a request until the group's phase ends,
//! with real `tokio` waits, which TraceForge cannot fork the search on, so
//! exhaustive `verify()` would just run the loop to completion instead of
//! exploring its interleavings). Each member is pre-assigned a member id, so
//! the separate empty-member-id round trip (`MemberIdRequired`) isn't
//! exercised here; that path has no shared mutable state and isn't part of
//! this hazard. Each test stores the member ids as pending member ids, as
//! `MemberIdRequired` would, before the members join with them.
//!
//! The invariant checked is not just "exactly one leader" (the code trivially
//! enforces that per call) but "the leader is always whichever member's join
//! actually reached storage first" - which depends on the CAS retry loop
//! correctly re-reading the true persisted state after every simulated cache
//! miss, rather than on any single call's local logic.
//!
//! `sync_forms_group_with_races` continues past the join phase into the
//! deferred `Forming -> Formed` transition: the same 3 members call `sync`
//! in a second, independently `nondet()`-chosen order (with the same
//! per-call cache-miss race), where only the leader submits real
//! assignments and every other member submits none, as real clients do.
//! This exercises Kafka's `SyncGroupRequest` fencing: a follower's sync
//! before the leader's must be held until the leader's assignment, the
//! leader's sync must be the one that commits the assignment map, and a
//! follower syncing after the leader must simply be handed its own stored
//! assignment (`Inner<Formed>::sync`) - across every one of the 6 x 8 = 48
//! sync-side combinations, crossed with the 48 already explored on the join
//! side.
//!
//! `heartbeats_succeed_regardless_of_node_or_order` and
//! `members_leave_regardless_of_node_or_order` cover the remaining two
//! operations differently: `Controller::heartbeat` and `Controller::leave`
//! never hold a request (unlike join/sync, every path returns
//! immediately) - so, unlike the hand-rolled `join_member`/`sync_member`
//! above, they can be driven through the real public `Coordinator` trait
//! directly, which is strictly stronger evidence. Two independent
//! `Controller` instances sharing the same real storage - each with its own
//! private local cache - model nisshi's stateless, multi-node design more
//! faithfully than a hand-rolled cache-miss flag ever could: routing a
//! member's request to whichever node's cache happens to be stale *is*
//! "another concurrent request landed on a different node" here, without
//! needing to simulate it.

#![cfg(feature = "dynostore")]

use crate::common::{CLIENT_ID, PROTOCOL_TYPE, RANGE, alphanumeric_string, memory_storage};
use bytes::Bytes;
use nisshi_broker::coordinator::group::{
    Coordinator,
    administrator::{Controller, Group, Inner, JoinRequest, Reply, SyncRequest, Wrapper},
};
use nisshi_sans_io::{
    Body, ErrorCode, HeartbeatResponse, LeaveGroupResponse,
    join_group_request::JoinGroupRequestProtocol, leave_group_request::MemberIdentity,
    sync_group_request::SyncGroupRequestAssignment, sync_group_response::SyncGroupResponse,
};
use nisshi_storage::{
    ArcDynStorage, GroupDetail, GroupDetailResponse, GroupRebalance, GroupState, NamedGroupDetail,
    Storage, UpdateError, Version,
};
use std::time::{Duration, SystemTime};
use traceforge::{Config, Nondet, TypeNondet, cover, future, verify};

const MEMBER_IDS: [&str; 3] = ["m0", "m1", "m2"];

/// One member's join, replaying the CAS-retry loop of `Controller::join`
/// without holding the request until the join phase ends: retry on
/// `Outdated`, return as soon as the write commits.
fn join_member(
    storage: &ArcDynStorage,
    group_id: &str,
    member_id: &str,
    protocols: &[JoinGroupRequestProtocol],
    cache: &mut Option<(Wrapper<ArcDynStorage>, Option<Version>)>,
    simulate_cache_miss: bool,
) {
    let mut first_attempt = true;

    loop {
        let (original, version) = if first_attempt && simulate_cache_miss {
            (Wrapper::Forming(Inner::new(storage.clone())), None)
        } else {
            cache
                .take()
                .unwrap_or_else(|| (Wrapper::Forming(Inner::new(storage.clone())), None))
        };
        first_attempt = false;

        let (updated, result) = future::block_on(async {
            let request = JoinRequest {
                api_version: 9,
                client_id: Some(CLIENT_ID),
                client_host: None,
                group_id,
                session_timeout_ms: 45_000,
                rebalance_timeout_ms: 300_000,
                member_id,
                group_instance_id: None,
                protocol_type: PROTOCOL_TYPE,
                protocols: Some(protocols),
                reason: None,
            };

            let (updated, _reply) = original
                .join(SystemTime::now(), &request, Duration::from_secs(3))
                .await;

            let detail = GroupDetail::from(&updated);
            let result = storage.update_group(group_id, detail, version).await;

            (updated, result)
        });

        match result {
            Ok(new_version) => {
                *cache = Some((updated, Some(new_version)));
                return;
            }

            Err(UpdateError::Outdated { current, version }) => {
                cover!("join_cas_retry_occurred");

                *cache = Some((
                    Wrapper::with_storage_group_detail(storage.clone(), *current),
                    Some(version),
                ));
                continue;
            }

            Err(other) => panic!("unexpected update_group error: {other:?}"),
        }
    }
}

/// One member's sync, replaying the CAS-retry loop of `Controller::sync`
/// without holding the request, the same way `join_member` mirrors
/// `Controller::join`. Returns the reply so the caller can check the
/// Kafka-level fencing outcome.
#[allow(clippy::too_many_arguments)]
fn sync_member(
    storage: &ArcDynStorage,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    protocol_type: &str,
    protocol_name: &str,
    assignments: Option<&[SyncGroupRequestAssignment]>,
    cache: &mut Option<(Wrapper<ArcDynStorage>, Option<Version>)>,
    simulate_cache_miss: bool,
) -> Reply {
    let mut first_attempt = true;

    loop {
        let (original, version) = if first_attempt && simulate_cache_miss {
            (Wrapper::Forming(Inner::new(storage.clone())), None)
        } else {
            cache
                .take()
                .unwrap_or_else(|| (Wrapper::Forming(Inner::new(storage.clone())), None))
        };
        first_attempt = false;

        let (updated, body, result) = future::block_on(async {
            let request = SyncRequest {
                group_id,
                generation_id,
                member_id,
                group_instance_id: None,
                protocol_type: Some(protocol_type),
                protocol_name: Some(protocol_name),
                assignments,
            };

            let (updated, body) = original.sync(SystemTime::now(), &request).await;

            let detail = GroupDetail::from(&updated);
            let result = storage.update_group(group_id, detail, version).await;

            (updated, body, result)
        });

        match result {
            Ok(new_version) => {
                *cache = Some((updated, Some(new_version)));
                return body;
            }

            Err(UpdateError::Outdated { current, version }) => {
                cover!("sync_cas_retry_occurred");

                *cache = Some((
                    Wrapper::with_storage_group_detail(storage.clone(), *current),
                    Some(version),
                ));
                continue;
            }

            Err(other) => panic!("unexpected update_group error: {other:?}"),
        }
    }
}

/// Stores `MEMBER_IDS` as pending member ids of a new group, as
/// `MemberIdRequired` would, so that each member can join with its id.
fn create_group_with_pending_members(storage: &ArcDynStorage, group_id: &str) {
    let expires = SystemTime::now() + Duration::from_secs(3_600);

    let detail = GroupDetail {
        rebalance: Box::new(GroupRebalance {
            pending_members: MEMBER_IDS
                .iter()
                .map(|member_id| ((*member_id).to_owned(), expires))
                .collect(),
            ..GroupRebalance::default()
        }),
        ..GroupDetail::default()
    };

    _ = future::block_on(storage.update_group(group_id, detail, None)).expect("create the group");
}

/// Ends the group's join phase, as the first request after its deadline does.
fn end_join_phase(
    storage: &ArcDynStorage,
    group_id: &str,
    cache: &mut Option<(Wrapper<ArcDynStorage>, Option<Version>)>,
) {
    let (detail, version) = future::block_on(storage.group_with_version(group_id))
        .expect("read the group")
        .expect("the group");

    let ended = Wrapper::with_storage_group_detail(storage.clone(), detail)
        .settle(group_id, SystemTime::now() + Duration::from_secs(3_600));

    let version =
        future::block_on(storage.update_group(group_id, GroupDetail::from(&ended), Some(version)))
            .expect("end the join phase");

    *cache = Some((ended, Some(version)));
}

#[test]
fn three_members_join_with_simulated_cache_misses() {
    let stats = verify(Config::builder().build(), || {
        let storage = future::block_on(memory_storage("spike", 111)).expect("storage");
        let group_id = alphanumeric_string(15);
        create_group_with_pending_members(&storage, &group_id);

        let protocols = [JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(Bytes::from_static(b"meta"))];

        // Pick a join order by repeatedly choosing an index among whoever's
        // left, rather than enumerating all 6 permutations by hand.
        let mut remaining: Vec<usize> = (0..MEMBER_IDS.len()).collect();
        let mut order = Vec::new();
        while !remaining.is_empty() {
            let i = (0..remaining.len()).nondet();
            order.push(remaining.remove(i));
        }

        let mut cache: Option<(Wrapper<ArcDynStorage>, Option<Version>)> = None;

        for &member_index in &order {
            let simulate_cache_miss = bool::nondet();

            join_member(
                &storage,
                &group_id,
                MEMBER_IDS[member_index],
                &protocols,
                &mut cache,
                simulate_cache_miss,
            );
        }

        let described =
            future::block_on(storage.describe_groups(Some(std::slice::from_ref(&group_id)), false))
                .expect("describe_groups");

        let detail = match &described[..] {
            [
                NamedGroupDetail {
                    response: GroupDetailResponse::Found(detail),
                    ..
                },
            ] => detail,
            otherwise => panic!("expected exactly one described group, got: {otherwise:?}"),
        };

        assert_eq!(
            detail.members.len(),
            MEMBER_IDS.len(),
            "every member's join must be durable despite simulated cache misses, got: {:?}",
            detail.members
        );

        let GroupState::Forming { leader, .. } = &detail.state else {
            panic!(
                "group should still be Forming (no sync yet): {:?}",
                detail.state
            );
        };

        let expected_leader = MEMBER_IDS[order[0]];
        assert_eq!(
            leader.as_deref(),
            Some(expected_leader),
            "leader must be whichever member's join actually reached storage first, \
             regardless of any later member's simulated cache miss"
        );

        cover!("all_three_members_joined");
    });

    println!(
        "traceforge stats: execs={} blocked={}",
        stats.execs, stats.block
    );

    assert!(
        stats
            .coverage
            .is_covered("join_cas_retry_occurred".to_string())
    );
    assert!(
        stats
            .coverage
            .is_covered("all_three_members_joined".to_string())
    );
}

/// Joins 3 members exactly like `three_members_join_with_simulated_cache_misses`
/// (reusing `join_member` for setup, not exhaustively re-exploring it - the
/// join phase's invariants are already proven there), then has all 3 `sync`
/// in a second, independently `nondet()`-chosen order with the same per-call
/// cache-miss race, asserting Kafka's `SyncGroupRequest` fencing holds no
/// matter how a follower's sync interleaves with the leader's.
#[test]
fn sync_forms_group_with_races() {
    let stats = verify(Config::builder().build(), || {
        let storage = future::block_on(memory_storage("spike", 111)).expect("storage");
        let group_id = alphanumeric_string(15);
        create_group_with_pending_members(&storage, &group_id);

        let protocols = [JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(Bytes::from_static(b"meta"))];

        let mut join_remaining: Vec<usize> = (0..MEMBER_IDS.len()).collect();
        let mut join_order = Vec::new();
        while !join_remaining.is_empty() {
            let i = (0..join_remaining.len()).nondet();
            join_order.push(join_remaining.remove(i));
        }

        let mut cache: Option<(Wrapper<ArcDynStorage>, Option<Version>)> = None;

        for &member_index in &join_order {
            join_member(
                &storage,
                &group_id,
                MEMBER_IDS[member_index],
                &protocols,
                &mut cache,
                bool::nondet(),
            );
        }

        end_join_phase(&storage, &group_id, &mut cache);

        // The join phase's own invariants (durability, leader election) are
        // already exhaustively checked in `three_members_join_with_simulated_
        // cache_misses`; here the leader is just whichever member joined
        // first, taken as given.
        let leader_id = MEMBER_IDS[join_order[0]];

        let leader_assignments: Vec<SyncGroupRequestAssignment> = MEMBER_IDS
            .iter()
            .map(|member_id| {
                SyncGroupRequestAssignment::default()
                    .member_id((*member_id).into())
                    .assignment(Bytes::from(format!("assignment-for-{member_id}")))
            })
            .collect();

        let mut sync_remaining: Vec<usize> = (0..MEMBER_IDS.len()).collect();
        let mut sync_order = Vec::new();
        while !sync_remaining.is_empty() {
            let i = (0..sync_remaining.len()).nondet();
            sync_order.push(sync_remaining.remove(i));
        }

        let leader_position = sync_order
            .iter()
            .position(|&index| MEMBER_IDS[index] == leader_id)
            .expect("leader must be among the syncing members");

        for (position, &member_index) in sync_order.iter().enumerate() {
            let member_id = MEMBER_IDS[member_index];
            let is_leader = member_id == leader_id;
            let simulate_cache_miss = bool::nondet();

            let reply = sync_member(
                &storage,
                &group_id,
                1,
                member_id,
                PROTOCOL_TYPE,
                RANGE,
                is_leader.then_some(&leader_assignments[..]),
                &mut cache,
                simulate_cache_miss,
            );

            if position < leader_position {
                assert_eq!(
                    reply,
                    Reply::Held {
                        member_id: member_id.to_owned()
                    },
                    "member={member_id} synced before the leader (position {position} < \
                     {leader_position}) and must wait for the leader's assignment"
                );
                cover!("follower_held_until_leader_syncs");
            } else {
                let Reply::Ready(Body::SyncGroupResponse(SyncGroupResponse {
                    error_code,
                    assignment,
                    ..
                })) = reply
                else {
                    panic!("expected a SyncGroupResponse, got: {reply:?}");
                };

                assert_eq!(
                    error_code,
                    i16::from(ErrorCode::None),
                    "member={member_id} at position={position}, leader at \
                     {leader_position}, unexpected error"
                );
                assert_eq!(
                    assignment,
                    Bytes::from(format!("assignment-for-{member_id}")),
                    "member={member_id} received the wrong assignment"
                );

                cover!(if position == leader_position {
                    "leader_sync_forms_group"
                } else {
                    "late_follower_fetches_assignment"
                });
            }
        }

        let described =
            future::block_on(storage.describe_groups(Some(std::slice::from_ref(&group_id)), false))
                .expect("describe_groups");

        let detail = match &described[..] {
            [
                NamedGroupDetail {
                    response: GroupDetailResponse::Found(detail),
                    ..
                },
            ] => detail,
            otherwise => panic!("expected exactly one described group, got: {otherwise:?}"),
        };

        let GroupState::Formed {
            leader,
            assignments,
            ..
        } = &detail.state
        else {
            panic!(
                "group should be Formed once the leader has synced: {:?}",
                detail.state
            );
        };

        assert_eq!(leader, leader_id);
        assert_eq!(assignments.len(), MEMBER_IDS.len());
        for member_id in MEMBER_IDS {
            assert_eq!(
                assignments.get(member_id),
                Some(&Bytes::from(format!("assignment-for-{member_id}"))),
                "persisted assignment for {member_id} is wrong"
            );
        }

        cover!("group_formed_in_storage");
    });

    println!(
        "traceforge stats: execs={} blocked={}",
        stats.execs, stats.block
    );

    assert!(
        stats
            .coverage
            .is_covered("sync_cas_retry_occurred".to_string())
    );
    assert!(
        stats
            .coverage
            .is_covered("follower_held_until_leader_syncs".to_string())
    );
    assert!(
        stats
            .coverage
            .is_covered("leader_sync_forms_group".to_string())
    );
    assert!(
        stats
            .coverage
            .is_covered("late_follower_fetches_assignment".to_string())
    );
    assert!(
        stats
            .coverage
            .is_covered("group_formed_in_storage".to_string())
    );
}

/// Forms the group deterministically (the join/sync race combinatorics are
/// already exhaustively covered above; every `nondet()` choice here is spent
/// on the heartbeat-routing race instead), then has all 3 members heartbeat
/// in a `nondet()`-chosen order, each routed to one of two independent
/// `Controller` instances sharing the same storage.
#[test]
fn heartbeats_succeed_regardless_of_node_or_order() {
    let stats = verify(Config::builder().build(), || {
        let storage = future::block_on(memory_storage("spike", 111)).expect("storage");
        let group_id = alphanumeric_string(15);
        create_group_with_pending_members(&storage, &group_id);

        let protocols = [JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(Bytes::from_static(b"meta"))];

        let mut cache = None;
        for member_id in MEMBER_IDS {
            join_member(
                &storage, &group_id, member_id, &protocols, &mut cache, false,
            );
        }

        end_join_phase(&storage, &group_id, &mut cache);

        let leader_assignments: Vec<SyncGroupRequestAssignment> = MEMBER_IDS
            .iter()
            .map(|member_id| {
                SyncGroupRequestAssignment::default()
                    .member_id((*member_id).into())
                    .assignment(Bytes::from(format!("assignment-for-{member_id}")))
            })
            .collect();

        for member_id in MEMBER_IDS {
            let is_leader = member_id == MEMBER_IDS[0];
            let _ = sync_member(
                &storage,
                &group_id,
                1,
                member_id,
                PROTOCOL_TYPE,
                RANGE,
                is_leader.then_some(&leader_assignments[..]),
                &mut cache,
                false,
            );
        }

        let node_a = Controller::with_storage(storage.clone()).expect("controller");
        let node_b = Controller::with_storage(storage.clone()).expect("controller");

        let mut remaining: Vec<usize> = (0..MEMBER_IDS.len()).collect();
        let mut order = Vec::new();
        while !remaining.is_empty() {
            let i = (0..remaining.len()).nondet();
            order.push(remaining.remove(i));
        }

        for &member_index in &order {
            let member_id = MEMBER_IDS[member_index];
            let on_node_a = bool::nondet();

            let body = if on_node_a {
                future::block_on(node_a.heartbeat(&group_id, 1, member_id, None))
            } else {
                future::block_on(node_b.heartbeat(&group_id, 1, member_id, None))
            }
            .expect("heartbeat");

            let Body::HeartbeatResponse(HeartbeatResponse { error_code, .. }) = body else {
                panic!("expected a HeartbeatResponse, got: {body:?}");
            };

            assert_eq!(
                error_code,
                i16::from(ErrorCode::None),
                "member={member_id} on_node_a={on_node_a}"
            );

            cover!(if on_node_a {
                "heartbeat_on_node_a"
            } else {
                "heartbeat_on_node_b"
            });
        }
    });

    println!(
        "traceforge stats: execs={} blocked={}",
        stats.execs, stats.block
    );

    assert!(stats.coverage.is_covered("heartbeat_on_node_a".to_string()));
    assert!(stats.coverage.is_covered("heartbeat_on_node_b".to_string()));
}

/// All 3 members leave (while still `Forming` - no sync in this test) in a
/// `nondet()`-chosen order, each routed to one of two independent
/// `Controller` instances sharing the same storage; asserts every leave
/// succeeds and the group ends up with no members regardless of routing or
/// order.
#[test]
fn members_leave_regardless_of_node_or_order() {
    let stats = verify(Config::builder().build(), || {
        let storage = future::block_on(memory_storage("spike", 111)).expect("storage");
        let group_id = alphanumeric_string(15);
        create_group_with_pending_members(&storage, &group_id);

        let protocols = [JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(Bytes::from_static(b"meta"))];

        let mut cache = None;
        for member_id in MEMBER_IDS {
            join_member(
                &storage, &group_id, member_id, &protocols, &mut cache, false,
            );
        }
        drop(cache);

        let node_a = Controller::with_storage(storage.clone()).expect("controller");
        let node_b = Controller::with_storage(storage.clone()).expect("controller");

        let mut remaining: Vec<usize> = (0..MEMBER_IDS.len()).collect();
        let mut order = Vec::new();
        while !remaining.is_empty() {
            let i = (0..remaining.len()).nondet();
            order.push(remaining.remove(i));
        }

        for &member_index in &order {
            let member_id = MEMBER_IDS[member_index];
            let on_node_a = bool::nondet();

            let members = [MemberIdentity::default()
                .member_id(member_id.into())
                .group_instance_id(None)];

            let body = if on_node_a {
                future::block_on(node_a.leave(&group_id, None, Some(&members)))
            } else {
                future::block_on(node_b.leave(&group_id, None, Some(&members)))
            }
            .expect("leave");

            let Body::LeaveGroupResponse(LeaveGroupResponse {
                members: results, ..
            }) = body
            else {
                panic!("expected a LeaveGroupResponse, got: {body:?}");
            };

            let results = results.expect("leave response should list the departing member");
            assert_eq!(results.len(), 1);
            assert_eq!(
                results[0].error_code,
                i16::from(ErrorCode::None),
                "member={member_id} on_node_a={on_node_a}"
            );

            cover!(if on_node_a {
                "leave_on_node_a"
            } else {
                "leave_on_node_b"
            });
        }

        let described =
            future::block_on(storage.describe_groups(Some(std::slice::from_ref(&group_id)), false))
                .expect("describe_groups");

        let detail = match &described[..] {
            [
                NamedGroupDetail {
                    response: GroupDetailResponse::Found(detail),
                    ..
                },
            ] => detail,
            otherwise => panic!("expected exactly one described group, got: {otherwise:?}"),
        };

        assert!(
            detail.members.is_empty(),
            "all members left, got: {:?}",
            detail.members
        );

        cover!("all_members_left");
    });

    println!(
        "traceforge stats: execs={} blocked={}",
        stats.execs, stats.block
    );

    assert!(stats.coverage.is_covered("leave_on_node_a".to_string()));
    assert!(stats.coverage.is_covered("leave_on_node_b".to_string()));
    assert!(stats.coverage.is_covered("all_members_left".to_string()));
}
