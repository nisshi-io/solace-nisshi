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

use std::{convert::Infallible, pin::pin, sync::Arc, time::Duration};

use nisshi_sans_io::{
    ErrorCode, TimestampType,
    produce_response::PartitionProduceResponse,
    record::{deflated, inflated},
};
use rama::Service;
use tokio::{
    sync::oneshot,
    time::{Instant, sleep_until, timeout_at},
};
use tracing::{Instrument as _, Span, info, instrument, warn};

use super::{
    deadline::{Deadline, Stage},
    detached::{Detached, Write},
    partition::PartitionProduce,
    response,
    timestamps::NO_TIMESTAMP,
};
use crate::{Error, Result, Storage, Topition};

/// How long past the request's deadline a write runs before it is logged as stuck. A partition
/// whose write never ends takes no writes from this process until it restarts, so the engine
/// must bound its calls, and this line tells an operator which write did not.
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
        Self {
            storage,
            detached: Detached::default(),
        }
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
    /// The task owns the batch and the request's share of the decoded-bytes budget, so the budget
    /// keeps counting the memory that the write holds after the request answered. The task runs
    /// under the span of the request, and logs its own result when the request has left it.
    async fn write_until(
        &self,
        at: Instant,
        request: &PartitionProduce<()>,
        topition: Topition,
        batch: deflated::Batch,
        inflated: inflated::Batch,
    ) -> Waited {
        let write = Arc::new(self.detached.track(topition.clone()));
        let (sender, receiver) = oneshot::channel();

        let task = {
            let storage = self.storage.clone();
            let transaction_id = request.transaction_id.clone();
            let reservation = request.reservation.clone();
            let write = Arc::clone(&write);

            async move {
                let _reservation = reservation;
                let _end = End(Arc::clone(&write));

                let result = run(
                    &storage,
                    at,
                    transaction_id.as_deref(),
                    &topition,
                    batch,
                    inflated,
                )
                .await;

                if write.is_detached() {
                    match &result {
                        Ok(base_offset) => {
                            info!(base_offset, "a write that outlived its request ended");
                        }
                        Err(error) => {
                            warn!(?error, "a write that outlived its request failed");
                        }
                    }
                }

                // The request has stopped waiting when this fails, and the result is logged.
                _ = sender.send(result);
            }
            .instrument(Span::current())
        };

        _ = tokio::spawn(task);

        let mut receiver = receiver;

        // The task drops its sender without a result only when the write panicked.
        let panicked = || Error::Message("the produce task ended without a result".into());

        match timeout_at(at, &mut receiver).await {
            Ok(result) => Waited::Ended(result.unwrap_or_else(|_| Err(panicked()))),

            Err(_) => {
                // A write that ended in the same instant has nothing to detach from, and its
                // result is waiting.
                if write.detach() {
                    Waited::Detached
                } else {
                    Waited::Ended(receiver.try_recv().unwrap_or_else(|_| Err(panicked())))
                }
            }
        }
    }
}

/// Marks a write as ended when its task ends, whether it returned or panicked.
struct End(Arc<Write>);

impl Drop for End {
    fn drop(&mut self) {
        self.0.finish();
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
                 other write to its partition until it ends"
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
            // waited for as it always was.
            None => {
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

                if self.detached.contains(&topition) {
                    return Ok(Deadline::in_flight(request.index));
                }

                match self
                    .write_until(at, &request, topition, batch, inflated)
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
