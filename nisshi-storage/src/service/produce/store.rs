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

use std::convert::Infallible;

use nisshi_sans_io::{
    ErrorCode, TimestampType,
    produce_response::PartitionProduceResponse,
    record::{deflated, inflated},
};
use rama::Service;
use tracing::instrument;

use super::{
    deadline::{Deadline, Stage},
    partition::PartitionProduce,
    response,
    timestamps::NO_TIMESTAMP,
};
use crate::{Storage, Topition};

/// A [`Service`] using its [`Storage`] taking a [`PartitionProduce`] of a validated batch and its
/// decoded records, returning the partition's [`PartitionProduceResponse`].
///
/// The service stores the batch as it is, and gives the storage engine the decoded records through
/// [`Storage::produce_inflated`], so that the engine does not decode the batch again. The steps
/// before it validate the batch and set its timestamps.
///
/// A partition that the request's deadline reaches before its write starts is not written. It
/// answers `REQUEST_TIMED_OUT`. Compared with Kafka this is looser on the start (no write begins
/// after the deadline).
#[derive(Clone, Debug)]
pub struct StorePartitionService<G> {
    storage: G,
}

impl<G> StorePartitionService<G>
where
    G: Storage,
{
    pub fn new(storage: G) -> Self {
        Self { storage }
    }
}

impl<G> Service<PartitionProduce<(deflated::Batch, inflated::Batch)>> for StorePartitionService<G>
where
    G: Storage,
{
    type Output = PartitionProduceResponse;
    type Error = Infallible;

    #[instrument(skip_all)]
    async fn serve(
        &self,
        request: PartitionProduce<(deflated::Batch, inflated::Batch)>,
    ) -> Result<Self::Output, Self::Error> {
        let (request, (batch, inflated)) = request.take_batch();

        // The decode and the wait for the decoded-bytes budget take time too.
        if request.deadline.has_passed() {
            return Ok(Deadline::timed_out(Stage::NotStarted, request.index));
        }

        // Kafka returns -1 as `log_append_time_ms` for a `CreateTime` topic. For a `LogAppendTime`
        // topic the timestamp step stamped the batch with the time of the append, as its
        // `max_timestamp`.
        let log_append_time_ms = match request.configuration.timestamp_type() {
            TimestampType::CreateTime => NO_TIMESTAMP,
            TimestampType::LogAppendTime => batch.max_timestamp,
        };

        let producer_id = batch.producer_id;
        let producer_epoch = batch.producer_epoch;
        let base_sequence = batch.base_sequence;

        let topition = Topition::new(request.topic.as_str(), request.index);

        Ok(
            match self
                .storage
                .produce_inflated(
                    request.transaction_id.as_deref(),
                    &topition,
                    batch,
                    inflated,
                )
                .await
            {
                Ok(base_offset) => PartitionProduceResponse::default()
                    .index(request.index)
                    .error_code(ErrorCode::None.into())
                    .base_offset(base_offset)
                    .log_append_time_ms(Some(log_append_time_ms))
                    .log_start_offset(Some(0))
                    .record_errors(Some([].into()))
                    .error_message(None)
                    .current_leader(None),

                Err(error) => response::produce_failure(
                    &request.topic,
                    request.index,
                    producer_id,
                    producer_epoch,
                    base_sequence,
                    error,
                ),
            },
        )
    }
}
