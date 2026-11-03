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

//! `TxnOffsetCommit`: the group's member, generation and group instance ID are checked by the
//! group coordinator, which holds the group. The storage checks the producer.

use nisshi_sans_io::{
    ApiKey as _, Body, Frame, FrameInput, Header, TxnOffsetCommitRequest, TxnOffsetCommitResponse,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use nisshi_service::FrameRouteBuilder;
use rama::{Service, service::BoxService};

use crate::{Error, Result, coordinator::group::Coordinator};

/// Wraps the registered route of `TxnOffsetCommit`: the group coordinator fences the commit, then
/// the storage's route commits it. Fails with [`nisshi_service::Error::NoRouteToWrap`] when the
/// route is not registered yet, so that a route table cannot leave the commit unfenced.
pub fn fence_txn_offset_commit<C>(
    builder: FrameRouteBuilder<Error>,
    coordinator: C,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    C: Coordinator,
{
    builder
        .wrap_route(TxnOffsetCommitRequest::KEY, |inner| {
            TxnOffsetCommitFenceService { coordinator, inner }.boxed()
        })
        .map_err(Into::into)
}

#[derive(Clone, Debug)]
pub struct TxnOffsetCommitFenceService<C> {
    coordinator: C,
    inner: BoxService<FrameInput, Frame, Error>,
}

impl<C> Service<FrameInput> for TxnOffsetCommitFenceService<C>
where
    C: Coordinator,
{
    type Output = Frame;
    type Error = Error;

    async fn serve(&self, input: FrameInput) -> Result<Self::Output, Self::Error> {
        let correlation_id = input.frame.correlation_id()?;

        // The request is decoded here to read the group's fields, and again by the route.
        let request = TxnOffsetCommitRequest::try_from(input.frame.body.clone())?;

        let generation_id = request.generation_id.unwrap_or(-1);
        let member_id = request.member_id.as_deref().unwrap_or_default();

        // A commit without a generation, a member or a group instance ID (versions before 3
        // have none) is not checked against the group, whatever its state, as in Kafka's
        // `validateOffsetCommit` for a transactional commit.
        if generation_id < 0 && member_id.is_empty() && request.group_instance_id.is_none() {
            return self.inner.serve(input).await;
        }

        match self
            .coordinator
            .txn_offset_commit_fence(
                &request.group_id,
                generation_id,
                member_id,
                request.group_instance_id.as_deref(),
            )
            .await?
        {
            None => self.inner.serve(input).await,

            Some(error_code) => Ok(Frame {
                size: 0,
                header: Header::Response { correlation_id },
                body: Body::from(
                    TxnOffsetCommitResponse::default()
                        .throttle_time_ms(0)
                        .topics(Some(
                            request
                                .topics
                                .iter()
                                .flatten()
                                .map(|topic| {
                                    TxnOffsetCommitResponseTopic::default()
                                        .name(topic.name.clone())
                                        .partitions(Some(
                                            topic
                                                .partitions
                                                .iter()
                                                .flatten()
                                                .map(|partition| {
                                                    TxnOffsetCommitResponsePartition::default()
                                                        .partition_index(partition.partition_index)
                                                        .error_code(i16::from(error_code))
                                                })
                                                .collect(),
                                        ))
                                })
                                .collect(),
                        )),
                ),
            }),
        }
    }
}
