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

use std::fmt::{self, Debug};

use nisshi_sans_io::{
    Ack, ErrorCode, ProduceRequest, ProduceResponse, RequestInput,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::deflated,
};
use rama::{Layer, Service};
use tracing::{instrument, warn};

use super::{batch_validation, rejection::Rejection, response};
use crate::{Error, Result};

/// A [`ProduceRequest`] whose `acks` is valid, with each partition's batch checked for the
/// faults that need no storage to find.
///
/// [`ProduceService`](super::ProduceService) takes this instead of a [`ProduceRequest`], so it
/// never parses those fields itself.
#[derive(Clone, Debug)]
pub struct ValidatedProduceRequest {
    pub(super) topics: Vec<ValidatedTopic>,
}

/// A topic of a [`ProduceRequest`], with the transaction that the request writes it in, if any.
///
/// [`ProduceTopicService`](super::ProduceTopicService) takes this, so that it needs nothing else
/// from the request.
#[derive(Clone, Debug)]
pub struct ValidatedTopic {
    pub(super) name: String,
    pub(super) transaction_id: Option<String>,
    pub(super) partitions: Vec<ValidatedPartition>,
}

/// A partition of a [`ProduceRequest`], with its batch or the reason to reject it.
#[derive(Clone, Debug)]
pub(super) struct ValidatedPartition {
    pub(super) index: i32,
    pub(super) batch: Result<deflated::Batch, Rejection>,
}

impl ValidatedPartition {
    /// Returns `partition` of `topic` with the batch that the partition carries, or the
    /// [`Rejection`] that the partition gets, which this function logs.
    fn new(topic: &str, partition: PartitionProduceData) -> Self {
        let index = partition.index;

        // The decoder turns a zero-length records field into `None`, so it
        // counts as zero batches.
        let batches = partition
            .records
            .map(|records| records.batches)
            .unwrap_or_default();

        // Kafka allows exactly one batch per partition from Produce v3, the
        // first version that carries record batches, which is the only format
        // this broker decodes. Kafka checks the count before it validates any
        // batch, so this check runs before `batch_validation::rejection`. See
        // [ProduceRequest.java#L225-L246].
        //
        // [ProduceRequest.java#L225-L246]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/ProduceRequest.java#L225-L246
        let batch_count = batches.len();
        let Ok([batch]) = <[deflated::Batch; 1]>::try_from(batches) else {
            const REASON: &str =
                "a produce request must contain exactly one record batch per partition";

            warn!(
                topic,
                partition = index,
                batch_count,
                reason = REASON,
                "rejecting produce request",
            );

            return Self {
                index,
                batch: Err(Rejection::new(ErrorCode::InvalidRecord, REASON)),
            };
        };

        let batch = match batch_validation::rejection(&batch) {
            Some(rejection) => {
                let rejection = Rejection::from(rejection);
                rejection.log(topic, index, &batch);
                Err(rejection)
            }
            None => Ok(batch),
        };

        Self { index, batch }
    }
}

impl ValidatedTopic {
    fn new(topic: TopicProduceData, transaction_id: Option<String>) -> Self {
        let partitions = topic
            .partition_data
            .unwrap_or_default()
            .into_iter()
            .map(|partition| ValidatedPartition::new(&topic.name, partition))
            .collect();

        Self {
            name: topic.name,
            transaction_id,
            partitions,
        }
    }
}

/// A [`Layer`] that gives its inner [`Service`] a [`ValidatedProduceRequest`], and answers a
/// [`ProduceRequest`] with an invalid `acks` itself.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProduceRequestValidationLayer;

impl ProduceRequestValidationLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for ProduceRequestValidationLayer {
    type Service = ProduceRequestValidationService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ProduceRequestValidationService { inner }
    }
}

/// A [`Service`] passing a [`ValidatedProduceRequest`] to `inner`, or answering
/// `INVALID_REQUIRED_ACKS` for each partition of a [`ProduceRequest`] whose `acks` is not valid.
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProduceRequestValidationService<S> {
    inner: S,
}

impl<S> Debug for ProduceRequestValidationService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(ProduceRequestValidationService))
            .finish()
    }
}

impl<S, I> Service<I> for ProduceRequestValidationService<S>
where
    S: Service<ValidatedProduceRequest, Output = ProduceResponse, Error = Error>,
    I: Into<RequestInput<ProduceRequest>> + Send + 'static,
{
    type Output = ProduceResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output> {
        let request = input.into().request;
        let topics = request.topic_data.unwrap_or_default();

        // Kafka refuses a Produce whose `acks` is not -1, 0 or 1 before it appends anything, and
        // answers every partition `INVALID_REQUIRED_ACKS` ([`ReplicaManager.appendRecords`]).
        //
        // Kafka first answers an unknown partition `UNKNOWN_TOPIC_OR_PARTITION`, and a partition
        // without exactly one batch `INVALID_RECORD`. We answer every partition
        // `INVALID_REQUIRED_ACKS` instead, because we find an unknown partition only inside
        // `storage.produce`, and this check must run before any storage call. Each such partition
        // still fails, only with another error code.
        //
        // [`ReplicaManager.appendRecords`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L762-L765
        if Ack::try_from(request.acks).is_err() {
            warn!(
                acks = request.acks,
                topics = topics.len(),
                "rejecting produce request: acks must be -1, 0 or 1"
            );

            return Ok(response::produce_response(
                topics
                    .into_iter()
                    .map(|topic| response::topic_error(topic, ErrorCode::InvalidRequiredAcks))
                    .collect(),
            ));
        }

        self.inner
            .serve(ValidatedProduceRequest {
                topics: topics
                    .into_iter()
                    .map(|topic| ValidatedTopic::new(topic, request.transactional_id.clone()))
                    .collect(),
            })
            .await
    }
}
