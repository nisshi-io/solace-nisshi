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

//! Drives Nisshi's group coordinator as a consumer does: join, sync, heartbeat and leave.

use crate::random::{alphanumeric_string, random_bytes};
use nisshi_broker::{
    Error, Result,
    coordinator::group::{Coordinator, administrator::Controller},
    service::coordinator::services,
};
use nisshi_sans_io::{
    Body, ErrorCode, HeartbeatResponse, JoinGroupResponse, LeaveGroupResponse, MetadataResponse,
    SyncGroupResponse,
    join_group_request::JoinGroupRequestProtocol,
    join_group_response::JoinGroupResponseMember,
    leave_group_request::MemberIdentity,
    metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
    sync_group_request::SyncGroupRequestAssignment,
};
use nisshi_service::{BytesFrameLayer, ConsumerGroupLayer, FrameBytesLayer, FrameRouteService};
use nisshi_storage::Storage;
use rama::{Layer as _, Service};

#[allow(clippy::too_many_arguments)]
pub async fn join_group<G>(
    controller: &mut Controller<G>,
    client_id: Option<&str>,
    group_id: &str,
    session_timeout_ms: i32,
    rebalance_timeout_ms: Option<i32>,
    member_id: &str,
    group_instance_id: Option<&str>,
    protocol_type: &str,
    protocols: Option<&[JoinGroupRequestProtocol]>,
    reason: Option<&str>,
) -> Result<JoinGroupResponse>
where
    G: Storage + Clone,
{
    controller
        .join(
            JOIN_GROUP_VERSION,
            client_id,
            None,
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            protocols,
            reason,
        )
        .await
        .and_then(|body| TryInto::try_into(body).map_err(Into::into))
}

#[allow(clippy::too_many_arguments)]
pub async fn sync_group<G>(
    controller: &mut Controller<G>,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    group_instance_id: Option<&str>,
    protocol_type: &str,
    protocol_name: &str,
    assignments: &[SyncGroupRequestAssignment],
) -> Result<SyncGroupResponse>
where
    G: Storage + Clone,
{
    controller
        .sync(
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            Some(protocol_type),
            Some(protocol_name),
            Some(assignments),
        )
        .await
        .and_then(|body| TryInto::try_into(body).map_err(Into::into))
}

pub async fn heartbeat<G>(
    controller: &mut Controller<G>,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    group_instance_id: Option<&str>,
) -> Result<HeartbeatResponse>
where
    G: Storage + Clone,
{
    controller
        .heartbeat(group_id, generation_id, member_id, group_instance_id)
        .await
        .and_then(|body| TryInto::try_into(body).map_err(Into::into))
}

pub async fn leave<G>(
    controller: &mut Controller<G>,
    group_id: &str,
    member_id: &str,
    group_instance_id: Option<&str>,
) -> Result<LeaveGroupResponse>
where
    G: Storage + Clone,
{
    controller
        .leave(
            group_id,
            None,
            Some(&[MemberIdentity::default()
                .member_id(member_id.into())
                .group_instance_id(group_instance_id.map(|s| s.to_owned()))
                .reason(Some("the consumer is being closed".into()))]),
        )
        .await
        .and_then(|body| TryInto::try_into(body).map_err(Into::into))
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum JoinResponse {
    Leader {
        id: String,
        generation: i32,
        members: Vec<JoinGroupResponseMember>,
        protocols: Vec<JoinGroupRequestProtocol>,
    },
    Follower {
        leader: String,
        id: String,
        generation: i32,
        protocols: Vec<JoinGroupRequestProtocol>,
    },
}

impl JoinResponse {
    #[allow(dead_code)]
    pub fn leader(&self) -> &str {
        match self {
            Self::Leader { id: leader, .. } | Self::Follower { leader, .. } => leader.as_str(),
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Self::Leader { id, .. } | Self::Follower { id, .. } => id.as_str(),
        }
    }

    pub fn generation(&self) -> i32 {
        match self {
            Self::Leader { generation, .. } | Self::Follower { generation, .. } => *generation,
        }
    }

    pub fn is_leader(&self) -> bool {
        matches!(self, Self::Leader { .. })
    }

    pub fn protocols(&self) -> &[JoinGroupRequestProtocol] {
        match self {
            Self::Leader { protocols, .. } | Self::Follower { protocols, .. } => &protocols[..],
        }
    }
}

/// The newest `JoinGroup` version, which the test helpers send.
pub const JOIN_GROUP_VERSION: i16 = 9;

pub const CLIENT_ID: &str = "console-consumer";
pub const RANGE: &str = "range";
pub const COOPERATIVE_STICKY: &str = "cooperative-sticky";
pub const PROTOCOL_TYPE: &str = "consumer";

#[allow(clippy::too_many_arguments)]
pub async fn join<G>(
    controller: &mut Controller<G>,
    group_id: &str,
    member_id: Option<&str>,
    group_instance_id: Option<&str>,
    protocols: Option<Vec<JoinGroupRequestProtocol>>,
    session_timeout_ms: i32,
    rebalance_timeout_ms: Option<i32>,
) -> Result<JoinResponse>
where
    G: Storage + Clone,
{
    let reason = None;

    let protocols = protocols.unwrap_or_else(|| {
        [
            JoinGroupRequestProtocol::default()
                .name(RANGE.into())
                .metadata(random_bytes(15)),
            JoinGroupRequestProtocol::default()
                .name(COOPERATIVE_STICKY.into())
                .metadata(random_bytes(15)),
        ]
        .into()
    });

    let join_response = join_group(
        controller,
        Some(CLIENT_ID),
        group_id,
        session_timeout_ms,
        rebalance_timeout_ms,
        member_id.unwrap_or_default(),
        group_instance_id,
        PROTOCOL_TYPE,
        Some(&protocols[..]),
        reason,
    )
    .await?;

    if member_id.is_none() && group_instance_id.is_none() {
        // join rejected as member id is required for a dynamic group
        //
        assert_eq!(
            ErrorCode::MemberIdRequired,
            ErrorCode::try_from(join_response.error_code)?
        );
        assert_eq!(Some("".into()), join_response.protocol_name);
        assert!(join_response.leader.is_empty());
        assert!(join_response.member_id.starts_with(CLIENT_ID));
        assert_eq!(0, join_response.members.unwrap_or_default().len());

        Box::pin(join(
            controller,
            group_id,
            Some(join_response.member_id.as_str()),
            group_instance_id,
            Some(protocols),
            session_timeout_ms,
            rebalance_timeout_ms,
        ))
        .await
    } else if join_response.member_id == join_response.leader {
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(join_response.error_code)?
        );
        assert_eq!(Some(PROTOCOL_TYPE.into()), join_response.protocol_type);
        assert_eq!(Some(RANGE.into()), join_response.protocol_name);

        let id = join_response.leader;
        let generation = join_response.generation_id;
        let members = join_response.members.unwrap_or_default();

        Ok(JoinResponse::Leader {
            id,
            generation,
            members,
            protocols,
        })
    } else {
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(join_response.error_code)?
        );
        assert_eq!(Some(PROTOCOL_TYPE.into()), join_response.protocol_type);
        assert_eq!(Some(RANGE.into()), join_response.protocol_name);
        assert_ne!(join_response.member_id, join_response.leader);
        assert_eq!(0, join_response.members.unwrap_or_default().len());

        let id = join_response.member_id;
        let leader = join_response.leader;
        let generation = join_response.generation_id;

        Ok(JoinResponse::Follower {
            leader,
            id,
            generation,
            protocols,
        })
    }
}

async fn serve<S>(service: &S, input: Option<Body>) -> anyhow::Result<Body>
where
    S: Service<Option<Body>, Output = Body>,
    S::Error: Into<anyhow::Error>,
{
    service.serve(input).await.map_err(Into::into)
}

/// A group with one member, formed through a coordinator: member id required,
/// join, sync.
#[derive(Debug)]
pub struct Formed {
    pub group: String,
    pub generation_id: i32,
    pub member_id: String,
}

pub async fn form_group<S>(coordinator: &Controller<S>) -> anyhow::Result<Formed>
where
    S: Storage + Clone,
{
    use anyhow::anyhow;

    let group = alphanumeric_string(15);

    let metadata = MetadataResponse::default().topics(Some(vec![
        MetadataResponseTopic::default()
            .name(Some("t".into()))
            .partitions(Some(
                (0..3)
                    .map(|partition_index| {
                        MetadataResponsePartition::default().partition_index(partition_index)
                    })
                    .collect(),
            )),
    ]));

    let route = services(FrameRouteService::<Error>::builder(), coordinator.clone())
        .and_then(|builder| builder.build().map_err(Into::into))?;

    let consumer = (
        ConsumerGroupLayer::new(group.clone(), ["t"], metadata),
        FrameBytesLayer,
        BytesFrameLayer::default(),
    )
        .into_layer(route);

    let member_id_required = serve(&consumer, None).await?;
    let joined = serve(&consumer, Some(member_id_required)).await?;

    let Body::JoinGroupResponse(JoinGroupResponse {
        error_code,
        generation_id,
        ref member_id,
        ..
    }) = joined
    else {
        return Err(anyhow!("expecting join response: {joined:?}"));
    };
    assert_eq!(i16::from(ErrorCode::None), error_code);
    let member_id = member_id.clone();

    let synced = serve(&consumer, Some(joined)).await?;
    assert!(matches!(synced, Body::SyncGroupResponse(_)), "{synced:?}");

    Ok(Formed {
        group,
        generation_id,
        member_id,
    })
}
