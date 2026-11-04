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

mod acks_zero;
mod batch_validation;
mod rejection;
mod request_validation;
mod response;
mod timestamps;
mod topic_configuration;

use std::time::{SystemTime, UNIX_EPOCH};

use nisshi_sans_io::{
    ApiKey, ErrorCode, ProduceRequest, ProduceResponse, TimestampType,
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    record::inflated,
};
use nisshi_schema::Registry;
use rama::{Layer as _, Service};
use tracing::{debug, error, instrument, warn};

pub use self::{
    acks_zero::{AcksZeroLayer, AcksZeroService},
    request_validation::{
        ProduceRequestValidationLayer, ProduceRequestValidationService, ValidatedProduceRequest,
    },
};
use self::{
    rejection::storage_error_code,
    request_validation::{ValidatedPartition, ValidatedTopic},
    timestamps::{NO_TIMESTAMP, validate_timestamps_offloaded},
    topic_configuration::TopicConfiguration,
};
use crate::{Error, Result, Storage, Topition};

/// Inflate a batch that a client sent in a Produce request, mapping a
/// decode failure to `INVALID_RECORD`.
///
/// Accepts both a `deflated::Batch` and a `&deflated::Batch`. The two
/// conversions take different decode paths and fail with different
/// [`nisshi_sans_io::Error`] variants for the same bytes: by value, an
/// uncompressed batch is parsed directly and a short record fails with
/// `Overflow` or `TryGet`. By reference, every batch goes through the serde
/// decoder and the same record fails with `Io(UnexpectedEof)`. Mapping every
/// error keeps the client-visible code the same on every backend, whichever
/// path it uses.
///
/// Every error is safe to treat as the client's fault, because decoding only
/// reads bytes already in memory: decompression, varints, lengths and record
/// fields all come from the request. Retrying the same bytes fails the same
/// way, so the client needs `INVALID_RECORD` rather than
/// `UNKNOWN_SERVER_ERROR`.
///
/// For a compressed batch this differs from Kafka on purpose. Kafka answers
/// `INVALID_RECORD` for a truncated uncompressed record, but it wraps a
/// decompression failure, including record data that is not a stream of the
/// declared codec, in a `KafkaException`, and the client gets
/// `UNKNOWN_SERVER_ERROR`. That code invites a retry of the same bytes, which
/// cannot succeed. See [DefaultRecordBatch.java#L633-L641].
///
/// The decode error is logged at debug: [`ProduceService`] logs each
/// rejection once, at warn, with the topic and partition, and a client can
/// send any number of batches that do not decode.
///
/// A batch over the decoded-size limit keeps its
/// [`nisshi_sans_io::Error::MessageMaxSizeExceeded`], so that
/// [`ProduceService`] answers `MESSAGE_TOO_LARGE`, which a producer can
/// recover from by splitting the batch.
///
/// Only use this on a batch from a client Produce request. A stored batch
/// that fails to decode is a broker fault, and must keep its own error.
///
/// [DefaultRecordBatch.java#L633-L641]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L633-L641
pub fn inflate_produced<B>(batch: B) -> Result<inflated::Batch>
where
    inflated::Batch: TryFrom<B, Error = nisshi_sans_io::Error>,
{
    inflated::Batch::try_from(batch).map_err(|err| match err {
        nisshi_sans_io::Error::MessageMaxSizeExceeded(_) => Error::SansIo(err),
        err => {
            debug!(?err, "produce batch does not decode");
            Error::Api(ErrorCode::InvalidRecord)
        }
    })
}

/// A [`Service`] using its [`Storage`] taking [`ValidatedProduceRequest`] returning
/// [`ProduceResponse`].
///
/// Call [`ProduceService::with_validation`] to get a service that takes a [`ProduceRequest`].
/// ```no_run
/// use bytes::Bytes;
/// use rama::{Layer as _, Service as _};
/// use nisshi_sans_io::{
///     CreateTopicsRequest, ErrorCode, ProduceRequest,
///     create_topics_request::CreatableTopic,
///     produce_request::{PartitionProduceData, TopicProduceData},
///     record::{Record, deflated::Frame, inflated},
/// };
/// use nisshi_storage::{CreateTopicsService, CreateTopicsValidationLayer, Error, ProduceService, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// const CLUSTER_ID: &str = "nisshi";
/// const NODE_ID: i32 = 111;
/// const HOST: &str = "localhost";
/// const PORT: i32 = 9092;
///
/// let storage = StorageContainer::builder()
///     .cluster_id(CLUSTER_ID)
///     .node_id(NODE_ID)
///     .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
///     storage: storage.clone(),
/// });
///
/// let name = "abcba";
///
/// let response = create_topic
///     .serve(
///         CreateTopicsRequest::default()
///             .topics(Some(vec![
///                 CreatableTopic::default()
///                     .name(name.into())
///                     .num_partitions(5)
///                     .replication_factor(3)
///                     .assignments(Some([].into()))
///                     .configs(Some([].into())),
///             ]))
///             .validate_only(Some(false)),
///     )
///     .await?;
///
/// let topics = response.topics.unwrap_or_default();
/// assert_eq!(1, topics.len());
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
///
/// let produce = ProduceService::new(storage.clone()).with_validation();
///
/// let partition = 0;
///
/// let response = produce
///     .serve(
///         ProduceRequest::default().topic_data(Some(
///             [TopicProduceData::default()
///                 .name(name.into())
///                 .partition_data(Some(
///                     [PartitionProduceData::default()
///                         .index(partition)
///                         .records(Some(Frame {
///                             batches: vec![
///                                 inflated::Batch::builder()
///                                     .record(
///                                         Record::builder().value(
///                                             Bytes::from_static(
///                                                 b"Lorem ipsum dolor sit amet",
///                                             )
///                                             .into(),
///                                         ),
///                                     )
///                                     .build()
///                                     .and_then(TryInto::try_into)?,
///                             ],
///                         }))]
///                     .into(),
///                 ))]
///             .into(),
///         )),
///     )
///     .await?;
///
/// let topics = response.responses.as_deref().unwrap_or_default();
/// assert_eq!(1, topics.len());
/// let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
/// assert_eq!(1, partitions.len());
/// assert_eq!(
///     ErrorCode::None,
///     ErrorCode::try_from(partitions[0].error_code)?
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct ProduceService<G> {
    storage: G,
    schemas: Option<Registry>,
}

impl<G> ApiKey for ProduceService<G> {
    const KEY: i16 = ProduceRequest::KEY;
}

impl<G> ProduceService<G>
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

    /// Returns this service behind the layers that give it a [`ValidatedProduceRequest`] and that
    /// close the connection of a failed `acks=0` request: [`AcksZeroLayer`] around
    /// [`ProduceRequestValidationLayer`].
    pub fn with_validation(
        self,
    ) -> AcksZeroService<ProduceRequestValidationService<ProduceService<G>>> {
        AcksZeroLayer::new().layer(ProduceRequestValidationLayer::new().layer(self))
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

    #[instrument(skip_all)]
    async fn topic(
        &self,
        transaction_id: Option<&str>,
        topic: ValidatedTopic,
    ) -> TopicProduceResponse {
        let mut partitions = Vec::with_capacity(topic.partitions.len());

        match TopicConfiguration::read(&self.storage, &topic.name).await {
            Ok(configuration) => {
                for partition in topic.partitions {
                    partitions.push(
                        self.partition(transaction_id, &topic.name, &configuration, partition)
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

        TopicProduceResponse::default()
            .name(topic.name)
            .partition_responses(Some(partitions))
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

impl<G> Service<ValidatedProduceRequest> for ProduceService<G>
where
    G: Storage,
{
    type Output = ProduceResponse;
    type Error = Error;

    #[instrument(skip(self, request))]
    async fn serve(&self, request: ValidatedProduceRequest) -> Result<Self::Output, Self::Error> {
        let mut responses = Vec::with_capacity(request.topics.len());

        for topic in request.topics {
            responses.push(self.topic(request.transactional_id.as_deref(), topic).await)
        }

        Ok(response::produce_response(responses))
    }
}

#[cfg(test)]
mod inflate_produced_tests {
    use nisshi_sans_io::{BatchAttribute, Compression, record::deflated};

    use super::*;
    use crate::service::produce::rejection::storage_error_code;

    /// The batch is compressed: on the by-value path an uncompressed batch
    /// whose `record_count` exceeds what its `record_data` could hold is
    /// reported as corrupt before the decoded-size limit is checked.
    #[test]
    fn a_produced_batch_over_the_decoded_size_limit_is_message_too_large() {
        let oversized = || deflated::Batch {
            attributes: BatchAttribute::default()
                .compression(Compression::Gzip)
                .into(),
            record_count: 1_000_000,
            last_offset_delta: 999_999,
            ..Default::default()
        };

        for result in [
            inflate_produced(&oversized()),
            inflate_produced(oversized()),
        ] {
            let error = result.expect_err("an oversized batch must not decode");
            assert_eq!(ErrorCode::MessageTooLarge, storage_error_code(&error));
        }
    }

    #[test]
    fn a_produced_batch_that_does_not_decode_is_an_invalid_record() {
        let truncated = || deflated::Batch {
            record_count: 1,
            last_offset_delta: 0,
            ..Default::default()
        };

        for result in [
            inflate_produced(&truncated()),
            inflate_produced(truncated()),
        ] {
            let error = result.expect_err("a batch without record data must not decode");
            assert_eq!(ErrorCode::InvalidRecord, storage_error_code(&error));
        }
    }
}
