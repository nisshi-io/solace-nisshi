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
    time::{SystemTime, UNIX_EPOCH},
};

use nisshi_sans_io::{
    ErrorCode, TimestampType,
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
};
use nisshi_schema::Registry;
use rama::Service;
use tracing::{debug, error, instrument, warn};

use super::{
    rejection::storage_error_code,
    request_validation::{ValidatedPartition, ValidatedTopic},
    response,
    timestamps::{NO_TIMESTAMP, validate_timestamps_offloaded},
    topic_configuration::TopicConfiguration,
};
use crate::{Error, Storage, Topition};

/// A [`Service`] using its [`Storage`] taking [`ValidatedTopic`] returning
/// [`TopicProduceResponse`].
///
/// The service answers each partition of the topic, in order, with its own response. A failure
/// is the response of the partition that it happened to, or of every partition when the service
/// cannot read the topic's configuration, so the service itself does not fail.
#[derive(Clone, Debug)]
pub struct ProduceTopicService<G> {
    storage: G,
    schemas: Option<Registry>,
}

impl<G> ProduceTopicService<G>
where
    G: Storage,
{
    /// Returns a service that stores batches in `storage`, and does not validate them against a
    /// schema.
    pub fn new(storage: G) -> Self {
        Self {
            storage,
            schemas: None,
        }
    }

    /// Returns this service with `schemas`: the registry that holds the schema of each topic. A
    /// batch for a topic that has a schema must match it before the service stores the batch,
    /// unless the topic's `schema.validation` is `false`.
    pub fn with_schemas(self, schemas: Option<Registry>) -> Self {
        Self { schemas, ..self }
    }

    #[instrument(skip_all)]
    async fn partition(
        &self,
        transaction_id: Option<&str>,
        name: &str,
        configuration: &TopicConfiguration,
        partition: ValidatedPartition,
    ) -> PartitionProduceResponse {
        let ValidatedPartition { index, batch } = partition;

        let batch = match batch {
            Ok(batch) => batch,
            Err(rejection) => return rejection.into_response(index),
        };

        let timestamp_type = configuration.timestamp_type;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_default();

        let (batch, inflated) =
            match validate_timestamps_offloaded(batch, timestamp_type, now).await {
                Ok((validated, Ok(inflated))) => (validated, inflated),
                Ok((validated, Err(rejection))) => {
                    rejection.log(name, index, &validated);
                    return rejection.into_response(index);
                }
                Err(err) => {
                    error!(?err);
                    return response::partition_error(index, ErrorCode::UnknownServerError);
                }
            };

        let producer_id = batch.producer_id;
        let producer_epoch = batch.producer_epoch;
        let base_sequence = batch.base_sequence;

        // The batch is validated here, on the records that the timestamp step decoded, and before
        // storage moves the high watermark, so that a batch rejected here leaves no state behind.
        // Storage does not validate the schema, because it would decode the batch a second time.
        if let Some(schemas) = &self.schemas
            && configuration.schema_validation()
            && let Err(error) = schemas
                .validate(name, &inflated)
                .await
                .inspect(|validation| debug!(?validation))
                .inspect_err(|err| debug!(?err))
        {
            return produce_failure(
                name,
                index,
                producer_id,
                producer_epoch,
                base_sequence,
                error.into(),
            );
        }

        // Kafka returns -1 as `log_append_time_ms` for a `CreateTime` topic.
        let log_append_time_ms = match timestamp_type {
            TimestampType::CreateTime => NO_TIMESTAMP,
            TimestampType::LogAppendTime => now,
        };

        let tp = Topition::new(name, index);

        match self.storage.produce(transaction_id, &tp, batch).await {
            Ok(base_offset) => PartitionProduceResponse::default()
                .index(index)
                .error_code(ErrorCode::None.into())
                .base_offset(base_offset)
                .log_append_time_ms(Some(log_append_time_ms))
                .log_start_offset(Some(0))
                .record_errors(Some([].into()))
                .error_message(None)
                .current_leader(None),

            Err(error) => produce_failure(
                name,
                index,
                producer_id,
                producer_epoch,
                base_sequence,
                error,
            ),
        }
    }
}

/// Returns the response for a batch that `storage.produce()` or the schema validation failed with
/// `error`, and logs the failure once, with the partition: a rejection that the client caused is
/// a warning, and an internal failure is an error.
fn produce_failure(
    name: &str,
    index: i32,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    error: Error,
) -> PartitionProduceResponse {
    let error_code = storage_error_code(&error);

    if error_code == ErrorCode::UnknownServerError {
        error!(
            topic = name,
            partition = index,
            producer_id,
            producer_epoch,
            base_sequence,
            ?error,
            "produce failed"
        );
    } else if let Error::MessageTooLarge { size, limit } = error {
        warn!(
            topic = name,
            partition = index,
            producer_id,
            size,
            limit,
            "rejecting produce batch larger than max.message.bytes"
        );
    } else {
        warn!(
            topic = name,
            partition = index,
            producer_id,
            producer_epoch,
            base_sequence,
            ?error_code,
            %error,
            "rejecting produce batch"
        );
    }

    let response = response::partition_error(index, error_code);

    // Kafka sends the same message, so that the producer's `RecordTooLargeException` names both
    // sizes.
    if let Error::MessageTooLarge { size, limit, .. } = error {
        response.error_message(Some(format!(
            "The record batch size in the append to {name}-{index} is {size} bytes which exceeds \
             the maximum configured value of {limit}."
        )))
    } else {
        response
    }
}

impl<G> Service<ValidatedTopic> for ProduceTopicService<G>
where
    G: Storage,
{
    type Output = TopicProduceResponse;
    type Error = Infallible;

    #[instrument(skip_all)]
    async fn serve(&self, topic: ValidatedTopic) -> Result<Self::Output, Self::Error> {
        let mut partitions = Vec::with_capacity(topic.partitions.len());

        match TopicConfiguration::read(&self.storage, &topic.name).await {
            Ok(configuration) => {
                for partition in topic.partitions {
                    partitions.push(
                        self.partition(
                            topic.transaction_id.as_deref(),
                            &topic.name,
                            &configuration,
                            partition,
                        )
                        .await,
                    )
                }
            }

            Err(error) => {
                error!(
                    topic = topic.name,
                    ?error,
                    "topic configuration read failed"
                );

                let error_code = storage_error_code(&error);

                partitions.extend(
                    topic
                        .partitions
                        .iter()
                        .map(|partition| response::partition_error(partition.index, error_code)),
                );
            }
        }

        Ok(TopicProduceResponse::default()
            .name(topic.name)
            .partition_responses(Some(partitions)))
    }
}
