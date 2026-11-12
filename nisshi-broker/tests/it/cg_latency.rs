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

use std::sync::Arc;

use crate::common::init_tracing;
use nisshi_broker::{
    Error, NODE_ID, coordinator::group::administrator::Controller, service::coordinator::services,
};
use nisshi_sans_io::{
    ErrorCode, HeartbeatRequest, JoinGroupRequest, MetadataResponse, RequestInput,
    SyncGroupRequest,
    consumer::{GroupConsumer, MemberAssignment},
    metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
};
use nisshi_service::{
    BytesFrameLayer, FrameBytesLayer, FrameRouteService, LatencyIntroducingLayer, RequestFrameLayer,
};
use rama::{Layer, Service, extensions::Extensions};
use tracing::debug;
use url::Url;

use crate::common::{StorageType, storage_container};

#[tokio::test]
async fn stack() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let seed = 0;
    let latency = 50..150;

    let cluster = "nisshi";

    let storage = storage_container(
        StorageType::InMemory,
        cluster,
        NODE_ID,
        Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await?;

    let coordinator = Controller::with_storage(storage)?;

    let route = services(FrameRouteService::<Error>::builder(), coordinator)
        .and_then(|builder| builder.build().map_err(Into::into))?;

    let latency_introducing = LatencyIntroducingLayer::default()
        .with_seed(seed)
        .with_latency_millis(latency);

    let sut = (
        RequestFrameLayer,
        latency_introducing,
        FrameBytesLayer,
        BytesFrameLayer::default(),
    )
        .into_layer(route);

    let group_id = "abc";
    let topic = "pqr";

    let metadata = MetadataResponse::default().topics(Some(
        [MetadataResponseTopic::default()
            .name(Some(topic.into()))
            .partitions(Some(
                (0..3)
                    .map(|partition_index| {
                        MetadataResponsePartition::default().partition_index(partition_index)
                    })
                    .collect(),
            ))]
        .into(),
    ));

    let mut consumer = GroupConsumer::builder(group_id)
        .topics([topic])
        .metadata(metadata)
        .on_assignment(Some(Arc::new(
            |group: String, assignment: MemberAssignment| debug!(group, %assignment),
        )))
        .build();

    let initial = consumer
        .next_action(None)
        .and_then(JoinGroupRequest::try_from)?;
    assert!(initial.member_id.is_empty());

    let extensions = Extensions::default();

    let r0 = sut
        .serve(RequestInput {
            request: initial,
            extensions: extensions.clone(),
        })
        .await?;
    assert_eq!(i16::from(ErrorCode::MemberIdRequired), r0.error_code);
    assert!(!r0.member_id.is_empty());

    let next_action = consumer.next_action(Some(r0.into()))?;

    let join = JoinGroupRequest::try_from(next_action)?;
    assert!(!join.member_id.is_empty());

    let r1 = sut
        .serve(RequestInput {
            request: join,
            extensions: extensions.clone(),
        })
        .await?;
    assert_eq!(i16::from(ErrorCode::None), r1.error_code);
    assert_eq!(1, r1.generation_id);
    assert_eq!(r1.leader, r1.member_id);
    assert_eq!(1, r1.members.as_deref().unwrap_or_default().len());

    let next_action = consumer.next_action(Some(r1.into()))?;
    let sync = SyncGroupRequest::try_from(next_action)?;
    assert_eq!(group_id, sync.group_id);
    assert_eq!(1, sync.generation_id);
    assert_eq!(1, sync.assignments.as_deref().unwrap_or_default().len());

    let r2 = sut
        .serve(RequestInput {
            request: sync,
            extensions: extensions.clone(),
        })
        .await?;
    assert_eq!(i16::from(ErrorCode::None), r2.error_code);

    let member_assignment = MemberAssignment::try_from(r2.assignment.clone())
        .inspect(|member_assignment| debug!(?member_assignment))?;
    assert!(!member_assignment.assignment.assigned_partitions.is_empty());

    let next_action = consumer.next_action(Some(r2.into()))?;
    let heartbeat = HeartbeatRequest::try_from(next_action)?;
    assert_eq!(1, heartbeat.generation_id);

    Ok(())
}
