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

use nisshi_sans_io::{ApiKey, Frame, FrameInput, Header, OffsetCommitRequest};
use rama::Service;
use tracing::instrument;

use crate::{
    Error, Result,
    coordinator::group::{Coordinator, OffsetCommit},
};

#[derive(Clone, Debug)]
pub struct OffsetCommitService<C> {
    pub coordinator: C,
}

impl<C> ApiKey for OffsetCommitService<C> {
    const KEY: i16 = OffsetCommitRequest::KEY;
}

impl<C> Service<FrameInput> for OffsetCommitService<C>
where
    C: Coordinator,
{
    type Output = Frame;
    type Error = Error;

    #[instrument(skip(self, req))]
    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        let correlation_id = req.frame.correlation_id()?;

        let mut offset_commit = OffsetCommitRequest::try_from(req.frame.body)?;

        _ = offset_commit
            .retention_time_ms
            .take_if(|retention_ms| retention_ms.is_negative());

        self.coordinator
            .offset_commit(OffsetCommit {
                group_id: offset_commit.group_id.as_str(),
                generation_id_or_member_epoch: offset_commit.generation_id_or_member_epoch,
                member_id: offset_commit.member_id.as_deref(),
                group_instance_id: offset_commit.group_instance_id.as_deref(),
                retention_time_ms: offset_commit.retention_time_ms,
                topics: offset_commit.topics.as_deref(),
                is_transactional: false,
            })
            .await
            .map(|body| Frame {
                size: 0,
                header: Header::Response { correlation_id },
                body,
            })
    }
}
