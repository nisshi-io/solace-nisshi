// Copyright ⓒ Solace Corporation
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

use nisshi_sans_io::{
    ApiKey as _, DeleteGroupsRequest, DescribeGroupsRequest, HeartbeatRequest, JoinGroupRequest,
    LeaveGroupRequest, ListGroupsRequest, OffsetCommitRequest, OffsetFetchRequest,
    SyncGroupRequest,
};
use nisshi_service::FrameRouteBuilder;
use rama::{Layer as _, Service as _, layer::MapErrLayer};

use crate::{
    Error,
    broker::group::{
        delete::DeleteGroupsService, describe::DescribeGroupsService, heartbeat::HeartbeatService,
        join::JoinGroupService, leave::LeaveGroupService, list::ListGroupsService,
        offset_commit::OffsetCommitService, offset_fetch::OffsetFetchService,
        sync::SyncGroupService,
    },
    coordinator::group::Coordinator,
};

pub fn services<C>(
    builder: FrameRouteBuilder<Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    C: Coordinator,
{
    [
        delete_groups,
        describe_groups,
        heartbeat,
        join_group,
        leave_group,
        list_groups,
        offset_commit,
        offset_fetch,
        sync_group,
    ]
    .iter()
    .try_fold(builder, |builder, service| {
        service(builder, coordinator.clone())
    })
}

pub fn delete_groups<C>(
    builder: FrameRouteBuilder<Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    C: Coordinator,
{
    builder
        .with_route(
            DeleteGroupsRequest::KEY,
            (MapErrLayer::new(Error::from),)
                .into_layer(DeleteGroupsService { coordinator })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn describe_groups<C>(
    builder: FrameRouteBuilder<Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    C: Coordinator,
{
    builder
        .with_route(
            DescribeGroupsRequest::KEY,
            (MapErrLayer::new(Error::from),)
                .into_layer(DescribeGroupsService { coordinator })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn heartbeat<C>(
    builder: FrameRouteBuilder<Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    C: Coordinator,
{
    builder
        .with_route(
            HeartbeatRequest::KEY,
            (MapErrLayer::new(Error::from),)
                .into_layer(HeartbeatService { coordinator })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn join_group<C>(
    builder: FrameRouteBuilder<Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    C: Coordinator,
{
    builder
        .with_route(
            JoinGroupRequest::KEY,
            (MapErrLayer::new(Error::from),)
                .into_layer(JoinGroupService { coordinator })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn leave_group<C>(
    builder: FrameRouteBuilder<Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    C: Coordinator,
{
    builder
        .with_route(
            LeaveGroupRequest::KEY,
            (MapErrLayer::new(Error::from),)
                .into_layer(LeaveGroupService { coordinator })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn list_groups<C>(
    builder: FrameRouteBuilder<Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    C: Coordinator,
{
    builder
        .with_route(
            ListGroupsRequest::KEY,
            (MapErrLayer::new(Error::from),)
                .into_layer(ListGroupsService { coordinator })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn offset_commit<C>(
    builder: FrameRouteBuilder<Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    C: Coordinator,
{
    builder
        .with_route(
            OffsetCommitRequest::KEY,
            (MapErrLayer::new(Error::from),)
                .into_layer(OffsetCommitService { coordinator })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn offset_fetch<C>(
    builder: FrameRouteBuilder<Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    C: Coordinator,
{
    builder
        .with_route(
            OffsetFetchRequest::KEY,
            (MapErrLayer::new(Error::from),)
                .into_layer(OffsetFetchService { coordinator })
                .boxed(),
        )
        .map_err(Into::into)
}

pub fn sync_group<C>(
    builder: FrameRouteBuilder<Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    C: Coordinator,
{
    builder
        .with_route(
            SyncGroupRequest::KEY,
            (MapErrLayer::new(Error::from),)
                .into_layer(SyncGroupService { coordinator })
                .boxed(),
        )
        .map_err(Into::into)
}
