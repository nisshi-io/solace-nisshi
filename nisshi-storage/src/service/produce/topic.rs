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

use nisshi_sans_io::{
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    record::deflated,
};
use rama::Service;
use tracing::{error, instrument};

use super::{
    deadline::{Deadline, Stage},
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
/// The service reads the topic's configuration, and answers each partition of the topic, in order,
/// with what `partitions` answers for the partition. When the service cannot read the
/// configuration it answers every partition with the error code of the failure, so the service
/// itself does not fail.
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
        let mut responses = Vec::with_capacity(topic.partitions.len());

        // A read is safe to drop, so the deadline stops it. A partition that the deadline stopped
        // answers `REQUEST_TIMED_OUT` and is not written.
        let read = topic
            .deadline
            .bound(TopicConfiguration::read(&self.storage, &topic.name))
            .await;

        match read {
            Err(_) => responses.extend(
                topic
                    .partitions
                    .iter()
                    .map(|partition| Deadline::timed_out(Stage::NotStarted, partition.index)),
            ),

            Ok(Ok(configuration)) => {
                let configuration = Arc::new(configuration);

                for partition in topic.partitions {
                    // The partitions are written one after another, so the deadline can pass
                    // while an earlier one is written. A partition that is not started then
                    // answers `REQUEST_TIMED_OUT`, and the ones that were written keep their
                    // offsets.
                    if topic.deadline.has_passed() {
                        responses.push(Deadline::timed_out(Stage::NotStarted, partition.index));
                        continue;
                    }

                    // The decoder turns a zero-length records field into `None`, so it counts as
                    // zero batches.
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
                            configuration: Arc::clone(&configuration),
                            admission: topic.admission.clone(),
                            deadline: topic.deadline,
                            batch: batches,
                        })
                        .await;

                    responses.push(response);
                }
            }

            Ok(Err(error)) => {
                error!(
                    topic = topic.name,
                    ?error,
                    "topic configuration read failed"
                );

                let error_code = storage_error_code(&error);

                responses.extend(
                    topic
                        .partitions
                        .iter()
                        .map(|partition| response::partition_error(partition.index, error_code)),
                );
            }
        }

        Ok(TopicProduceResponse::default()
            .name(topic.name)
            .partition_responses(Some(responses)))
    }
}
