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

use nisshi_sans_io::{ApiKey, Frame, FrameInput, Header, JoinGroupRequest};
use nisshi_service::PeerAddressExtension;
use rama::Service;
use tracing::instrument;

use crate::{Error, Result, coordinator::group::Coordinator};

#[derive(Clone, Debug)]
pub struct JoinGroupService<C> {
    pub coordinator: C,
}

impl<C> ApiKey for JoinGroupService<C> {
    const KEY: i16 = JoinGroupRequest::KEY;
}

impl<C> Service<FrameInput> for JoinGroupService<C>
where
    C: Coordinator,
{
    type Output = Frame;
    type Error = Error;

    #[instrument(skip(self, req))]
    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        let correlation_id = req.frame.correlation_id()?;
        let api_version = req.frame.api_version()?;

        let client_id = req
            .frame
            .client_id()
            .map(|client_id| client_id.map(|client_id| client_id.to_owned()))?;

        let client_host = req
            .extensions
            .get_ref::<PeerAddressExtension>()
            .map(PeerAddressExtension::client_host);

        let join_group = JoinGroupRequest::try_from(req.frame.body)?;

        self.coordinator
            .join(
                api_version,
                client_id.as_deref(),
                client_host.as_deref(),
                join_group.group_id.as_str(),
                join_group.session_timeout_ms,
                join_group.rebalance_timeout_ms,
                join_group.member_id.as_str(),
                join_group.group_instance_id.as_deref(),
                join_group.protocol_type.as_str(),
                join_group.protocols.as_deref(),
                join_group.reason.as_deref(),
            )
            .await
            .map(|body| Frame {
                size: 0,
                header: Header::Response { correlation_id },
                body,
            })
    }
}
