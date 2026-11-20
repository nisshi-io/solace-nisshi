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

use std::{convert::Infallible, pin::pin, time::Duration};

use nisshi_sans_io::{
    ErrorCode, TimestampType,
    produce_response::PartitionProduceResponse,
    record::{deflated, inflated},
};
use rama::Service;
use tokio::time::{Instant, sleep_until};
use tracing::{info, instrument, warn};

use super::{
    deadline::{Deadline, Stage},
    detached::{self, Detached},
    partition::PartitionProduce,
    response,
    slots::{WritePermit, take_write},
    timestamps::NO_TIMESTAMP,
};
use crate::{Error, Result, Storage, Topition};

/// How long past the request's deadline a write runs before it is logged as stuck. A producer
/// whose write to a partition never ends gets no further idempotent write to it through this
/// process until it restarts, so the engine must bound its calls, and this line tells an
/// operator which write did not.
const STUCK_AFTER: Duration = Duration::from_secs(60);

/// A [`Service`] using its [`Storage`] taking a [`PartitionProduce`] of a validated batch and its
/// decoded records, returning the partition's [`PartitionProduceResponse`].
///
/// The service stores the batch as it is, and gives the storage engine the decoded records through
/// [`Storage::produce_inflated`], so that the engine does not decode the batch again. The steps
/// before it validate the batch and set its timestamps.
///
/// A request with a deadline waits for the write only until the deadline. A write that has
/// started is not dropped then: it runs on in a task of its own, and the partition answers
/// `REQUEST_TIMED_OUT`. A write that its caller drops can leave its engine half done. The object
/// store engine, for one, has taken the batch's offsets and then skips recording them as a failed
/// produce, so a fetch stops at the gap, and it can have stored the batch without advancing the
/// producer's sequence, so that the retry stores it again. Compared with Kafka this answer is
/// looser on the start (no write begins after the deadline) and matches it on meaning:
/// `REQUEST_TIMED_OUT` says that the data may be stored.
#[derive(Clone, Debug)]
pub struct StorePartitionService<G> {
    storage: G,
    detached: Detached,
}

impl<G> StorePartitionService<G>
where
    G: Storage,
{
    pub fn new(storage: G) -> Self {
        Self::with_detached(storage, Detached::process())
    }

    /// Returns a service that tracks the writes that its requests leave running in `detached`,
    /// not in the set of the process. A test uses this so that it does not share state with the
    /// others that run in its process.
    pub(super) fn with_detached(storage: G, detached: Detached) -> Self {
        Self { storage, detached }
    }
}

/// The result of waiting for a write until the deadline.
enum Waited {
    /// The write ended, with this result.
    Ended(Result<i64>),
    /// The deadline passed first, and the write runs on.
    Detached,
}

impl<G> StorePartitionService<G>
where
    G: Storage + Clone,
{
    /// Writes the batch in a task of its own, and waits for its result until `at`.
    ///
    /// The task owns the batch, the request's share of the decoded-bytes budget, and `permit`, the
    /// place of the write among the writes that the storage shares across requests. The budget
    /// keeps counting the memory that the write holds after the request answered, and the permit
    /// keeps counting the connection.
    /// The task runs under the span of the request, and logs its own result when the request has
    /// left it.
    async fn write_until(
        &self,
        at: Instant,
        request: &PartitionProduce<()>,
        permit: Option<WritePermit>,
        topition: Topition,
        batch: deflated::Batch,
        inflated: inflated::Batch,
    ) -> Waited {
        let storage = self.storage.clone();
        let transaction_id = request.transaction_id.clone();
        let reservation = request.reservation.clone();
        let partitions = [(topition.clone(), batch.producer_id)];

        let waited = self
            .detached
            .write_until(at, partitions, |detachment| async move {
                // Both live as long as the write holds the engine's connection, not as long as
                // its request. Dropping the task drops them, on a panic and on an abort too.
                let _reservation = reservation;
                let _permit = permit;

                let result = run(
                    &storage,
                    at,
                    transaction_id.as_deref(),
                    &topition,
                    batch,
                    inflated,
                )
                .await;

                if detachment.is_detached() {
                    match &result {
                        Ok(base_offset) => {
                            info!(base_offset, "a write that outlived its request ended");
                        }
                        Err(error) => {
                            warn!(?error, "a write that outlived its request failed");
                        }
                    }
                }

                result
            })
            .await;

        match waited {
            detached::WriteOutcome::Ended(result) => Waited::Ended(result),
            detached::WriteOutcome::Detached => Waited::Detached,
            detached::WriteOutcome::Panicked => Waited::Ended(Err(Error::Message(
                "the produce task ended without a result".into(),
            ))),
        }
    }
}

/// Runs the write, and logs it once when it runs `STUCK_AFTER` past `at`.
async fn run<G>(
    storage: &G,
    at: Instant,
    transaction_id: Option<&str>,
    topition: &Topition,
    batch: deflated::Batch,
    inflated: inflated::Batch,
) -> Result<i64>
where
    G: Storage,
{
    let mut write = pin!(storage.produce_inflated(transaction_id, topition, batch, inflated));

    tokio::select! {
        result = &mut write => result,
        () = sleep_until(at + STUCK_AFTER) => {
            warn!(
                ?topition,
                stuck_after = ?STUCK_AFTER,
                "a produce write still runs long after its request's deadline, and takes no \
                 other idempotent write from its producer to its partition until it ends"
            );

            write.await
        }
    }
}

impl<G> Service<PartitionProduce<(deflated::Batch, inflated::Batch)>> for StorePartitionService<G>
where
    G: Storage + Clone,
{
    type Output = PartitionProduceResponse;
    type Error = Infallible;

    #[instrument(
        skip_all,
        fields(
            topic = %request.topic,
            partition = request.index,
            producer_id = request.batch.0.producer_id,
        )
    )]
    async fn serve(
        &self,
        request: PartitionProduce<(deflated::Batch, inflated::Batch)>,
    ) -> Result<Self::Output, Self::Error> {
        let (request, (batch, inflated)) = request.take_batch();

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

        let stored = match request.deadline.at() {
            // A request that does not wait for an answer has no deadline, and its write is
            // waited for as it always was. Its wait for the request's lane ends when the write
            // that holds the lane ends, so it needs no bound of its own.
            None => {
                // Without a deadline the wait cannot time out, so this is `Ok`.
                let _permit = take_write(request.pool.as_ref(), &request.deadline)
                    .await
                    .ok()
                    .flatten();

                self.storage
                    .produce_inflated(
                        request.transaction_id.as_deref(),
                        &topition,
                        batch,
                        inflated,
                    )
                    .await
            }

            Some(at) => {
                if request.deadline.has_passed() {
                    return Ok(Deadline::timed_out(Stage::NotStarted, request.index));
                }

                if self.detached.contains(&topition, producer_id) {
                    return Ok(Deadline::timed_out(Stage::InFlight, request.index));
                }

                // After the checks above and the request's slot, so a partition that is not
                // written holds no place, and the place is held as long as the write runs.
                let Ok(permit) = take_write(request.pool.as_ref(), &request.deadline).await else {
                    warn!(
                        ?topition,
                        remaining = ?request.deadline.remaining(),
                        "produce partition waited for the write lane of its request until the \
                         request's deadline"
                    );

                    return Ok(Deadline::timed_out(Stage::SlotWait, request.index));
                };

                match self
                    .write_until(at, &request, permit, topition, batch, inflated)
                    .await
                {
                    Waited::Ended(stored) => stored,
                    Waited::Detached => {
                        return Ok(Deadline::timed_out(Stage::Writing, request.index));
                    }
                }
            }
        };

        Ok(match stored {
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
        })
    }
}
