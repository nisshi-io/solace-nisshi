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

use nisshi_sans_io::{ApiKey, DeleteGroupsRequest, Frame, FrameInput, Header};
use rama::Service;
use tracing::instrument;

use crate::{Error, Result, coordinator::group::Coordinator};

/// Routes `DeleteGroups` through the [`Coordinator`] (not straight to
/// storage): deleting a group's storage state without checking whether it
/// still has members would both wrongly discard a live group's committed
/// offsets, and, because of how the conditional group-detail write upserts
/// a missing row, let the next heartbeat from a member of that group
/// silently recreate what was just deleted.
#[derive(Clone, Debug)]
pub struct DeleteGroupsService<C> {
    pub coordinator: C,
}

impl<C> ApiKey for DeleteGroupsService<C> {
    const KEY: i16 = DeleteGroupsRequest::KEY;
}

impl<C> Service<FrameInput> for DeleteGroupsService<C>
where
    C: Coordinator,
{
    type Output = Frame;
    type Error = Error;

    #[instrument(skip(self, req))]
    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        let correlation_id = req.frame.correlation_id()?;

        let req = DeleteGroupsRequest::try_from(req.frame.body)?;

        self.coordinator
            .delete_groups(req.groups_names.unwrap_or_default().as_slice())
            .await
            .map(|body| Frame {
                size: 0,
                header: Header::Response { correlation_id },
                body,
            })
    }
}
