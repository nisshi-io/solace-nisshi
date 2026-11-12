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

use nisshi_sans_io::{ApiKey, Frame, FrameInput, Header, ListGroupsRequest};
use rama::Service;
use tracing::instrument;

use crate::{Error, Result, coordinator::group::Coordinator};

/// Routes `ListGroups` through the [`Coordinator`] (not straight to storage), so that each group
/// is listed in its state at the current time. The stored state of a group whose members stopped
/// without leaving still has those members until a request settles the group.
#[derive(Clone, Debug)]
pub struct ListGroupsService<C> {
    pub coordinator: C,
}

impl<C> ApiKey for ListGroupsService<C> {
    const KEY: i16 = ListGroupsRequest::KEY;
}

impl<C> Service<FrameInput> for ListGroupsService<C>
where
    C: Coordinator,
{
    type Output = Frame;
    type Error = Error;

    #[instrument(skip(self, req))]
    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        let correlation_id = req.frame.correlation_id()?;

        let req = ListGroupsRequest::try_from(req.frame.body)?;

        self.coordinator
            .list_groups(req.states_filter.as_deref())
            .await
            .map(|body| Frame {
                size: 0,
                header: Header::Response { correlation_id },
                body,
            })
    }
}
