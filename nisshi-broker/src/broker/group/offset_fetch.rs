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

use nisshi_sans_io::{ApiKey, Frame, FrameInput, Header, OffsetFetchRequest};
use rama::Service;
use tracing::instrument;

use crate::{Error, Result, coordinator::group::Coordinator};

#[derive(Clone, Debug)]
pub struct OffsetFetchService<C> {
    pub coordinator: C,
}

impl<C> ApiKey for OffsetFetchService<C> {
    const KEY: i16 = OffsetFetchRequest::KEY;
}

impl<C> Service<FrameInput> for OffsetFetchService<C>
where
    C: Coordinator,
{
    type Output = Frame;
    type Error = Error;

    #[instrument(skip(self, req))]
    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        let correlation_id = req.frame.correlation_id()?;
        let offset_fetch = OffsetFetchRequest::try_from(req.frame.body)?;

        self.coordinator
            .offset_fetch(
                offset_fetch.group_id.as_deref(),
                offset_fetch.topics.as_deref(),
                offset_fetch.groups.as_deref(),
                offset_fetch.require_stable,
            )
            .await
            .map(|body| Frame {
                size: 0,
                header: Header::Response { correlation_id },
                body,
            })
    }
}
