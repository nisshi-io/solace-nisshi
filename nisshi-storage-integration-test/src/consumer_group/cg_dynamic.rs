// Copyright (c) Solace Corporation
//
// Contributors outside Solace:
//   Enwei Jiao
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

use crate::{
    broker,
    group::{self, CLIENT_ID, COOPERATIVE_STICKY, PROTOCOL_TYPE, RANGE},
    random,
};
use bytes::Bytes;
use nisshi_broker::{Result, coordinator::group::administrator::Controller};
use nisshi_sans_io::{
    ErrorCode, HeartbeatResponse, join_group_request::JoinGroupRequestProtocol,
    sync_group_request::SyncGroupRequestAssignment,
};
use nisshi_storage::Storage;
use tracing::debug;

pub async fn reject_empty_member_id_on_join<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    broker::register_broker(cluster_id, broker_id, &sc).await?;

    let mut controller = Controller::with_storage(sc.clone())?;

    let session_timeout_ms = 45_000;
    let rebalance_timeout_ms = Some(300_000);
    let group_instance_id = None;
    let reason = None;

    let group_id: String = random::alphanumeric_string(15);
    debug!(?group_id);

    let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
    let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

    let protocols = [
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(first_member_range_meta.clone()),
        JoinGroupRequestProtocol::default()
            .name(COOPERATIVE_STICKY.into())
            .metadata(first_member_sticky_meta),
    ];

    // join dynamic group without a member id
    //
    let member_id_required = group::join_group(
        &mut controller,
        Some(CLIENT_ID),
        group_id.as_str(),
        session_timeout_ms,
        rebalance_timeout_ms,
        "",
        group_instance_id,
        PROTOCOL_TYPE,
        Some(&protocols[..]),
        reason,
    )
    .await?;

    // join rejected as member id is required
    //
    assert_eq!(
        ErrorCode::MemberIdRequired,
        ErrorCode::try_from(member_id_required.error_code)?
    );
    // Kafka leaves the protocol type null in this response.
    assert_eq!(None, member_id_required.protocol_type);
    assert_eq!(Some("".into()), member_id_required.protocol_name);
    assert!(member_id_required.leader.is_empty());
    assert!(member_id_required.member_id.starts_with(CLIENT_ID));
    assert_eq!(0, member_id_required.members.unwrap().len());

    Ok(())
}

pub async fn lifecycle<G>(cluster_id: impl Into<String>, broker_id: i32, sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    broker::register_broker(cluster_id, broker_id, &sc).await?;

    let mut controller = Controller::with_storage(sc.clone())?;

    let session_timeout_ms = 45_000;
    let rebalance_timeout_ms = Some(300_000);
    let group_instance_id = None;

    let group_id: String = random::alphanumeric_string(15);
    debug!(?group_id);

    // 1st member
    //
    let first_member = group::join(
        &mut controller,
        group_id.as_str(),
        None,
        None,
        None,
        session_timeout_ms,
        rebalance_timeout_ms,
    )
    .await?;
    debug!(?first_member);

    let first_member_assignment_01 = random::random_bytes(15);

    let assignments = [SyncGroupRequestAssignment::default()
        .member_id(first_member.id().into())
        .assignment(first_member_assignment_01.clone())];

    // sync to form the group
    //
    let sync_response = group::sync_group(
        &mut controller,
        group_id.as_str(),
        first_member.generation(),
        first_member.id(),
        group_instance_id,
        PROTOCOL_TYPE,
        RANGE,
        &assignments,
    )
    .await?;
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(sync_response.error_code)?
    );
    assert_eq!(PROTOCOL_TYPE, sync_response.protocol_type.unwrap());
    assert_eq!(RANGE, sync_response.protocol_name.unwrap());
    assert_eq!(first_member_assignment_01, sync_response.assignment);

    // heartbeat establishing leadership of current generation
    //
    let HeartbeatResponse { error_code, .. } = group::heartbeat(
        &mut controller,
        group_id.as_str(),
        first_member.generation(),
        first_member.id(),
        group_instance_id,
    )
    .await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(error_code)?);

    // 2nd member joins
    //
    let second_member = group::join(
        &mut controller,
        group_id.as_str(),
        None,
        None,
        None,
        session_timeout_ms,
        rebalance_timeout_ms,
    )
    .await?;
    debug!(?second_member);

    // 2nd member on sync is told that the group is rebalancing
    //
    let sync_response = group::sync_group(
        &mut controller,
        group_id.as_str(),
        second_member.generation(),
        second_member.id(),
        group_instance_id,
        PROTOCOL_TYPE,
        RANGE,
        &assignments,
    )
    .await?;
    assert_eq!(
        ErrorCode::RebalanceInProgress,
        ErrorCode::try_from(sync_response.error_code)?
    );
    assert_eq!(PROTOCOL_TYPE, sync_response.protocol_type.unwrap());
    assert_eq!(RANGE, sync_response.protocol_name.unwrap());

    // rebalance in progress on heartbeat from leader with previous generation
    //
    let HeartbeatResponse { error_code, .. } = group::heartbeat(
        &mut controller,
        group_id.as_str(),
        first_member.generation(),
        first_member.id(),
        group_instance_id,
    )
    .await?;
    assert_eq!(
        ErrorCode::RebalanceInProgress,
        ErrorCode::try_from(error_code)?
    );

    // 2nd member rejoins due to rebalance
    //
    let second_member = group::join(
        &mut controller,
        group_id.as_str(),
        Some(second_member.id()),
        None,
        Some(second_member.protocols().into()),
        session_timeout_ms,
        rebalance_timeout_ms,
    )
    .await?;
    debug!(?second_member);
    assert_eq!(first_member.id(), second_member.leader());

    // first member rejoins as leader
    //
    let first_member = group::join(
        &mut controller,
        group_id.as_str(),
        Some(first_member.id()),
        None,
        Some(first_member.protocols().into()),
        session_timeout_ms,
        rebalance_timeout_ms,
    )
    .await?;
    debug!(?first_member);
    assert!(first_member.is_leader());

    // both have joined the same generation
    //
    assert_eq!(first_member.generation(), second_member.generation());

    let first_member_assignment_02 = random::random_bytes(15);
    let second_member_assignment_02 = random::random_bytes(15);

    let assignments = [
        SyncGroupRequestAssignment::default()
            .member_id(first_member.id().into())
            .assignment(first_member_assignment_02.clone()),
        SyncGroupRequestAssignment::default()
            .member_id(second_member.id().into())
            .assignment(second_member_assignment_02.clone()),
    ];

    // 1st member leader sync to form and assign the group
    //
    let sync_response = group::sync_group(
        &mut controller,
        group_id.as_str(),
        first_member.generation(),
        first_member.id(),
        group_instance_id,
        PROTOCOL_TYPE,
        RANGE,
        &assignments,
    )
    .await?;
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(sync_response.error_code)?
    );
    assert_eq!(PROTOCOL_TYPE, sync_response.protocol_type.unwrap());
    assert_eq!(RANGE, sync_response.protocol_name.unwrap());
    assert_eq!(first_member_assignment_02, sync_response.assignment);

    // 2st member receives group assignments
    //
    let sync_response = group::sync_group(
        &mut controller,
        group_id.as_str(),
        second_member.generation(),
        second_member.id(),
        group_instance_id,
        PROTOCOL_TYPE,
        RANGE,
        &assignments,
    )
    .await?;
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(sync_response.error_code)?
    );
    assert_eq!(PROTOCOL_TYPE, sync_response.protocol_type.unwrap());
    assert_eq!(RANGE, sync_response.protocol_name.unwrap());
    assert_eq!(second_member_assignment_02, sync_response.assignment);

    // 1st member heartbeat
    //
    let HeartbeatResponse { error_code, .. } = group::heartbeat(
        &mut controller,
        group_id.as_str(),
        first_member.generation(),
        first_member.id(),
        group_instance_id,
    )
    .await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(error_code)?);

    // 2nd member heartbeat
    //
    let HeartbeatResponse { error_code, .. } = group::heartbeat(
        &mut controller,
        group_id.as_str(),
        second_member.generation(),
        second_member.id(),
        group_instance_id,
    )
    .await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(error_code)?);

    // 1st member leaves the group
    //
    let leave_response = group::leave(
        &mut controller,
        group_id.as_str(),
        first_member.id(),
        group_instance_id,
    )
    .await?;
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(leave_response.error_code)?
    );

    // 2nd member heartbeat resulting in rebalance in progress
    //
    let HeartbeatResponse { error_code, .. } = group::heartbeat(
        &mut controller,
        group_id.as_str(),
        second_member.generation(),
        second_member.id(),
        group_instance_id,
    )
    .await?;
    assert_eq!(
        ErrorCode::RebalanceInProgress,
        ErrorCode::try_from(error_code)?
    );

    // 2nd member rejoins due to rebalance as leader
    //
    let second_member = group::join(
        &mut controller,
        group_id.as_str(),
        Some(second_member.id()),
        None,
        Some(second_member.protocols().into()),
        session_timeout_ms,
        rebalance_timeout_ms,
    )
    .await?;
    debug!(?second_member);
    assert!(second_member.is_leader());

    let second_member_assignment_03 = random::random_bytes(15);

    let assignments = [SyncGroupRequestAssignment::default()
        .member_id(second_member.id().into())
        .assignment(second_member_assignment_03.clone())];

    // 2nd member leader sync to reform and assign the group
    //
    let sync_response = group::sync_group(
        &mut controller,
        group_id.as_str(),
        second_member.generation(),
        second_member.id(),
        group_instance_id,
        PROTOCOL_TYPE,
        RANGE,
        &assignments,
    )
    .await?;
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(sync_response.error_code)?
    );
    assert_eq!(PROTOCOL_TYPE, sync_response.protocol_type.unwrap());
    assert_eq!(RANGE, sync_response.protocol_name.unwrap());
    assert_eq!(second_member_assignment_03, sync_response.assignment);

    // 2nd member heartbeat
    //
    let HeartbeatResponse { error_code, .. } = group::heartbeat(
        &mut controller,
        group_id.as_str(),
        second_member.generation(),
        second_member.id(),
        group_instance_id,
    )
    .await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(error_code)?);

    Ok(())
}
