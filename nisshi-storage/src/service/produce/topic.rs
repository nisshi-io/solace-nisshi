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

use std::{convert::Infallible, sync::Arc};

use futures::{StreamExt as _, stream};
use nisshi_sans_io::{
    produce_request::PartitionProduceData,
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    record::deflated,
};
use rama::Service;
use tracing::{error, instrument};

use super::{
    deadline::{Deadline, Stage},
    grouping,
    partition::PartitionProduce,
    rejection::storage_error_code,
    request_validation::ValidatedTopic,
    response,
    topic_configuration::TopicConfiguration,
};
use crate::Storage;

/// A [`Service`] using its [`Storage`] taking [`ValidatedTopic`] returning
/// [`TopicProduceResponse`].
///
/// The service reads the topic's configuration, and answers each partition of the topic with what
/// `partitions` answers for the partition, in the order of the request. It serves the partitions
/// that have different indexes at once, within the request's slots, and the entries of one index
/// one after another. When the service cannot read the configuration it answers every partition
/// with the error code of the failure, so the service itself does not fail.
#[derive(Clone, Debug)]
pub struct ProduceTopicService<G, S> {
    storage: G,
    partitions: S,
}

impl<G, S> ProduceTopicService<G, S>
where
    G: Storage,
    S: Service<
            PartitionProduce<Vec<deflated::Batch>>,
            Output = PartitionProduceResponse,
            Error = Infallible,
        >,
{
    /// Returns a service that reads the configuration of a topic from `storage`, and serves each
    /// partition of the topic with `partitions`.
    pub fn new(storage: G, partitions: S) -> Self {
        Self {
            storage,
            partitions,
        }
    }

    /// Serves `partition` with `partitions` once the request has a slot for it, and answers it
    /// `REQUEST_TIMED_OUT` without a write if the deadline passes first.
    async fn serve_partition(
        &self,
        topic: &ValidatedTopic,
        configuration: &Arc<TopicConfiguration>,
        partition: PartitionProduceData,
    ) -> PartitionProduceResponse {
        // The slot is held until the partition answers. If the deadline leaves a write running,
        // the partition has answered, so the write holds no slot. The deadline can pass while the
        // partition waits for a slot, or just after it got one. The partition answers
        // `REQUEST_TIMED_OUT` either way, and the ones that were written keep their offsets.
        let Some(_slot) = topic.slots.take(&topic.deadline).await else {
            return Deadline::timed_out(Stage::NotStarted, partition.index);
        };

        if topic.deadline.has_passed() {
            return Deadline::timed_out(Stage::NotStarted, partition.index);
        }

        // The decoder turns a zero-length records field into `None`, so it counts as zero
        // batches.
        let batches = partition
            .records
            .map(|records| records.batches)
            .unwrap_or_default();

        let Ok(response) = self
            .partitions
            .serve(PartitionProduce {
                topic: topic.name.clone(),
                index: partition.index,
                transaction_id: topic.transaction_id.clone(),
                configuration: Arc::clone(configuration),
                admission: topic.admission.clone(),
                deadline: topic.deadline,
                reservation: None,
                batch: batches,
            })
            .await;

        response
    }
}

impl<G, S> Service<ValidatedTopic> for ProduceTopicService<G, S>
where
    G: Storage,
    S: Service<
            PartitionProduce<Vec<deflated::Batch>>,
            Output = PartitionProduceResponse,
            Error = Infallible,
        >,
{
    type Output = TopicProduceResponse;
    type Error = Infallible;

    #[instrument(skip_all)]
    async fn serve(&self, topic: ValidatedTopic) -> Result<Self::Output, Self::Error> {
        let mut topic = topic;
        let partitions = std::mem::take(&mut topic.partitions);

        // A read is safe to drop, so the deadline stops it. A partition that the deadline stopped
        // answers `REQUEST_TIMED_OUT` and is not written. The slot of the read is given back
        // before the partitions are served, so a topic never holds a slot while it waits for
        // another one.
        let read = match topic.slots.take(&topic.deadline).await {
            Some(_slot) => topic
                .deadline
                .bound(TopicConfiguration::read(&self.storage, &topic.name))
                .await
                .ok(),
            None => None,
        };

        let responses = match read {
            None => partitions
                .iter()
                .map(|partition| Deadline::timed_out(Stage::NotStarted, partition.index))
                .collect(),

            Some(Ok(configuration)) => {
                let configuration = Arc::new(configuration);

                // If a request names a partition twice, the entries of that index are one group,
                // and a group is served one entry after another, so the writes to a partition
                // keep the request's order.
                let groups = grouping::by_key(partitions, |partition| partition.index)
                    .into_iter()
                    .map(|group| {
                        let topic = &topic;
                        let configuration = &configuration;

                        async move {
                            let mut answered = Vec::with_capacity(group.len());

                            for (position, partition) in group {
                                let response =
                                    self.serve_partition(topic, configuration, partition).await;
                                answered.push((position, response));
                            }

                            answered
                        }
                    })
                    .collect::<Vec<_>>();

                // Unordered, because an ordered buffer keeps a finished group's answer behind an
                // unfinished earlier one and counts it against the window, so one slow partition
                // would hold the later ones back. The slots bound the storage calls.
                let answered = stream::iter(groups)
                    .buffer_unordered(topic.slots.count())
                    .collect::<Vec<_>>()
                    .await;

                grouping::in_request_order(answered)
            }

            Some(Err(error)) => {
                error!(
                    topic = topic.name,
                    ?error,
                    "topic configuration read failed"
                );

                let error_code = storage_error_code(&error);

                partitions
                    .iter()
                    .map(|partition| response::partition_error(partition.index, error_code))
                    .collect()
            }
        };

        Ok(TopicProduceResponse::default()
            .name(topic.name)
            .partition_responses(Some(responses)))
    }
}
