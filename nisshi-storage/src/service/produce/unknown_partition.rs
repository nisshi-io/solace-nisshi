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

use std::{
    convert::Infallible,
    fmt::{self, Debug},
};

use nisshi_sans_io::{ErrorCode, produce_response::PartitionProduceResponse, record::deflated};
use rama::{Layer, Service};
use tracing::{debug, instrument};

use super::{partition::PartitionProduce, rejection::is_rejection, response};
use crate::{Storage, Topition};

/// A [`Layer`] that answers a rejected batch for a partition that does not exist with
/// `UNKNOWN_TOPIC_OR_PARTITION`, using the metadata of a [`Storage`].
#[derive(Clone, Debug)]
pub struct UnknownPartitionLayer<G> {
    storage: G,
}

impl<G> UnknownPartitionLayer<G> {
    pub fn new(storage: G) -> Self {
        Self { storage }
    }
}

impl<G, S> Layer<S> for UnknownPartitionLayer<G>
where
    G: Clone,
{
    type Service = UnknownPartitionService<G, S>;

    fn layer(&self, inner: S) -> Self::Service {
        UnknownPartitionService {
            storage: self.storage.clone(),
            inner,
        }
    }
}

/// A [`Service`] using its [`Storage`] taking a [`PartitionProduce`] of the batches that the
/// client sent, returning the partition's [`PartitionProduceResponse`] from `inner`, except that a
/// batch that `inner` rejects for a partition that does not exist gets
/// `UNKNOWN_TOPIC_OR_PARTITION`.
///
/// Kafka looks up the partition before it validates a batch, so a batch sent to a partition that
/// does not exist gets `UNKNOWN_TOPIC_OR_PARTITION`, whatever is wrong with the batch
/// ([`ReplicaManager.appendToLocalLog`] reads the partition, and only [`UnifiedLog.append`]
/// validates the batch). The steps of a partition validate the batch before they call storage, so
/// this service looks up the topic's metadata only when `inner` answers a rejection. A batch that
/// is stored costs no extra call. A lookup that fails, or that does not say whether the partition
/// exists, leaves the rejection as it was.
///
/// The lookup is the one that Fetch makes: every engine numbers a topic's partitions from 0, so
/// the count of partitions bounds the valid indexes.
///
/// [`ReplicaManager.appendToLocalLog`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1406-L1409
/// [`UnifiedLog.append`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/log/UnifiedLog.scala#L780-L813
#[derive(Clone)]
pub struct UnknownPartitionService<G, S> {
    storage: G,
    inner: S,
}

impl<G, S> Debug for UnknownPartitionService<G, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(UnknownPartitionService)).finish()
    }
}

impl<G, S> UnknownPartitionService<G, S>
where
    G: Storage,
{
    /// Returns whether `topition` is known not to exist.
    async fn partition_is_missing(&self, topition: &Topition) -> bool {
        let Ok(metadata) = self
            .storage
            .metadata(Some(&[topition.into()]))
            .await
            .inspect_err(|error| {
                debug!(
                    ?error,
                    topic = topition.topic(),
                    "the metadata lookup for a rejected batch failed"
                )
            })
        else {
            return false;
        };

        // The null engine lists every topic whatever the lookup names, so only the entry with
        // this topic's name answers for it.
        let Some(topic) = metadata
            .topics()
            .iter()
            .find(|topic| topic.name.as_deref() == Some(topition.topic()))
        else {
            debug!(
                topic = topition.topic(),
                "the metadata lookup for a rejected batch did not answer for the topic"
            );

            return false;
        };

        match ErrorCode::try_from(topic.error_code) {
            Ok(ErrorCode::UnknownTopicOrPartition) => true,
            Ok(ErrorCode::None) => {
                let count = topic.partitions.as_deref().map_or(0, <[_]>::len);

                !usize::try_from(topition.partition()).is_ok_and(|index| index < count)
            }
            error_code => {
                debug!(
                    ?error_code,
                    topic = topition.topic(),
                    "the metadata lookup for a rejected batch answered an error code that does not say whether the partition exists"
                );

                false
            }
        }
    }
}

impl<G, S> Service<PartitionProduce<Vec<deflated::Batch>>> for UnknownPartitionService<G, S>
where
    G: Storage,
    S: Service<
            PartitionProduce<Vec<deflated::Batch>>,
            Output = PartitionProduceResponse,
            Error = Infallible,
        >,
{
    type Output = PartitionProduceResponse;
    type Error = Infallible;

    #[instrument(skip_all)]
    async fn serve(
        &self,
        request: PartitionProduce<Vec<deflated::Batch>>,
    ) -> Result<Self::Output, Self::Error> {
        let topition = Topition::new(request.topic.as_str(), request.index);

        let Ok(response) = self.inner.serve(request).await;

        if !ErrorCode::try_from(response.error_code).is_ok_and(is_rejection)
            || !self.partition_is_missing(&topition).await
        {
            return Ok(response);
        }

        // The client gets the replacement, so the log keeps the reason that the batch was
        // rejected. Once the partition exists, the same batch is rejected with that reason.
        debug!(
            topic = topition.topic(),
            partition = topition.partition(),
            error_code = response.error_code,
            reason = response.error_message,
            "the rejection is replaced, because the partition does not exist"
        );

        Ok(
            response::partition_error(topition.partition(), ErrorCode::UnknownTopicOrPartition)
                .error_message(Some(
                    "this server does not host this topic-partition".into(),
                )),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::service::{
        counting_storage::{CountingStorage, Listing, Lookup},
        produce::tests::support::{INDEX, error_code, partition_produce},
    };

    /// A partition service that answers every partition with `error_code`, and counts the
    /// requests that it serves.
    #[derive(Clone, Debug)]
    struct Answering {
        error_code: ErrorCode,
        served: Arc<Mutex<usize>>,
    }

    impl Answering {
        fn new(error_code: ErrorCode) -> Self {
            Self {
                error_code,
                served: Arc::default(),
            }
        }
    }

    impl Service<PartitionProduce<Vec<deflated::Batch>>> for Answering {
        type Output = PartitionProduceResponse;
        type Error = Infallible;

        async fn serve(
            &self,
            request: PartitionProduce<Vec<deflated::Batch>>,
        ) -> Result<Self::Output, Self::Error> {
            *self.served.lock().expect("a test thread panicked") += 1;

            Ok(response::partition_error(request.index, self.error_code))
        }
    }

    /// Serves a request for partition [`INDEX`] of the topic `person`, which `inner` answers.
    async fn serve(storage: &CountingStorage, inner: Answering) -> PartitionProduceResponse {
        let Ok(response) = UnknownPartitionLayer::new(storage.clone())
            .layer(inner)
            .serve(partition_produce(Vec::new(), &[]))
            .await;

        response
    }

    #[tokio::test]
    async fn a_rejection_for_a_missing_topic_answers_unknown_topic_or_partition() {
        let storage = CountingStorage::new(Listing::Topics, &[]);

        let response = serve(&storage, Answering::new(ErrorCode::InvalidRecord)).await;

        assert_eq!(ErrorCode::UnknownTopicOrPartition, error_code(&response));
        assert_eq!(INDEX, response.index);
    }

    #[tokio::test]
    async fn a_rejection_for_a_missing_partition_of_an_existing_topic_is_replaced() {
        let storage = CountingStorage::new(Listing::Topics, &["person"]);

        let response = serve(&storage, Answering::new(ErrorCode::MessageTooLarge)).await;

        assert_eq!(ErrorCode::UnknownTopicOrPartition, error_code(&response));
    }

    /// The lookup costs a call only when a batch is rejected.
    #[tokio::test]
    async fn a_batch_that_is_not_rejected_makes_no_lookup() {
        let storage = CountingStorage::new(Listing::Topics, &[]);

        let response = serve(&storage, Answering::new(ErrorCode::None)).await;

        assert_eq!(ErrorCode::None, error_code(&response));
        assert_eq!(0, storage.lookups());
    }

    /// A lookup that fails does not say whether the partition exists, so the rejection stands.
    #[tokio::test]
    async fn a_failed_metadata_lookup_keeps_the_rejection() {
        // The topic does not exist, so a lookup that worked would replace the rejection.
        let storage = CountingStorage::new(Listing::Topics, &[]).with_lookup(Lookup::Error);

        let response = serve(&storage, Answering::new(ErrorCode::InvalidRecord)).await;

        assert_eq!(ErrorCode::InvalidRecord, error_code(&response));
        assert_eq!(1, storage.lookups());
    }

    /// A lookup that answers for another topic does not say whether this partition exists, so the
    /// rejection stands.
    #[tokio::test]
    async fn a_metadata_lookup_that_answers_for_another_topic_keeps_the_rejection() {
        // The topic exists. The lookup lists one topic with no name and an unknown-topic error,
        // as the null engine's listing does, which the service takes for this topic if it does
        // not match by name.
        let storage =
            CountingStorage::new(Listing::NamelessTopic, &["person"]).with_lookup(Lookup::Listed);

        let response = serve(&storage, Answering::new(ErrorCode::InvalidRecord)).await;

        assert_eq!(ErrorCode::InvalidRecord, error_code(&response));
    }

    #[tokio::test]
    async fn the_inner_service_serves_the_request() {
        let storage = CountingStorage::new(Listing::Topics, &[]);
        let inner = Answering::new(ErrorCode::InvalidRecord);

        _ = serve(&storage, inner.clone()).await;

        assert_eq!(1, *inner.served.lock().expect("a test thread panicked"));
    }
}
