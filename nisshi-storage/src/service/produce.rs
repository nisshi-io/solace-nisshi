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

mod batch_validation;
mod rejection;
mod timestamps;

use std::{
    sync::LazyLock,
    time::{SystemTime, UNIX_EPOCH},
};

use nisshi_sans_io::{
    Ack, ApiKey, ConfigResource, ErrorCode, ProduceRequest, ProduceResponse, RequestInput,
    SuppressResponseExtension, TimestampType,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    record::{deflated, inflated},
};
use opentelemetry::{KeyValue, metrics::Counter};
use rama::Service;
use tracing::{debug, error, instrument, warn};

use self::{
    batch_validation::rejection,
    rejection::{Rejection, storage_error_code},
    timestamps::{NO_TIMESTAMP, validate_timestamps_offloaded},
};
use crate::{Error, METER, Result, Storage, Topition, message_timestamp_type};

/// Counts the connections closed by a failed `acks=0` Produce, by the error
/// code of the first failing partition.
///
/// The failed request does not reach the request and error counts, because
/// it ends the connection before a response is encoded. A producer that
/// keeps failing shows here as a reconnect loop.
static ACKS_ZERO_PRODUCE_FAILED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_produce_acks_zero_failed")
        .with_description("Connections closed by a failed acks=0 Produce")
        .build()
});

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

/// The first partition in `responses` carrying a non-[`ErrorCode::None`]
/// error code, for the message an `acks=0` closed connection logs: the
/// client gets no response at all, so an operator needs to know which
/// partition actually failed.
fn first_error(responses: &[TopicProduceResponse]) -> Option<(String, i32, ErrorCode)> {
    responses.iter().find_map(|topic| {
        topic
            .partition_responses
            .as_deref()
            .unwrap_or_default()
            .iter()
            .find(|partition| partition.error_code != i16::from(ErrorCode::None))
            .map(|partition| {
                (
                    topic.name.clone(),
                    partition.index,
                    ErrorCode::try_from(partition.error_code)
                        .unwrap_or(ErrorCode::UnknownServerError),
                )
            })
    })
}

/// A [`Service`] using its [`Storage`] taking [`ProduceRequest`] returning [`ProduceResponse`].
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
/// let produce = ProduceService {
///     storage: storage.clone(),
/// };
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
    pub storage: G,
}

impl<G> ApiKey for ProduceService<G> {
    const KEY: i16 = ProduceRequest::KEY;
}

impl<G> ProduceService<G>
where
    G: Storage,
{
    fn error(&self, index: i32, error_code: ErrorCode) -> PartitionProduceResponse {
        PartitionProduceResponse::default()
            .index(index)
            .error_code(error_code.into())
            .base_offset(-1)
            .log_append_time_ms(Some(-1))
            .log_start_offset(Some(0))
            .record_errors(Some([].into()))
            .error_message(None)
            .current_leader(None)
    }

    /// Returns the response for `topic` that answers each of its partitions with `error_code`.
    fn topic_error(&self, topic: TopicProduceData, error_code: ErrorCode) -> TopicProduceResponse {
        let partitions = topic
            .partition_data
            .unwrap_or_default()
            .iter()
            .map(|partition| self.error(partition.index, error_code))
            .collect();

        TopicProduceResponse::default()
            .name(topic.name)
            .partition_responses(Some(partitions))
    }

    fn reject(
        &self,
        name: &str,
        index: i32,
        batch: &deflated::Batch,
        rejection: Rejection,
    ) -> PartitionProduceResponse {
        warn!(
            topic = name,
            partition = index,
            producer_id = batch.producer_id,
            attributes = batch.attributes,
            record_count = batch.record_count,
            last_offset_delta = batch.last_offset_delta,
            base_timestamp = batch.base_timestamp,
            max_timestamp = batch.max_timestamp,
            error_code = ?rejection.error_code,
            reason = %rejection.message,
            record_error = rejection
                .record_error
                .as_ref()
                .and_then(|record_error| record_error.batch_index_error_message.as_deref()),
            "rejecting produce batch",
        );

        self.error(index, rejection.error_code)
            .error_message(Some(rejection.message.into_owned()))
            .record_errors(Some(rejection.record_error.into_iter().collect()))
    }

    #[instrument(skip_all)]
    async fn partition(
        &self,
        transaction_id: Option<&str>,
        name: &str,
        timestamp_type: TimestampType,
        partition: PartitionProduceData,
    ) -> PartitionProduceResponse {
        // The decoder turns a zero-length records field into `None`, so it
        // counts as zero batches.
        let batches = partition
            .records
            .map(|records| records.batches)
            .unwrap_or_default();

        // Kafka allows exactly one batch per partition from Produce v3, the
        // first version that carries record batches, which is the only format
        // this broker decodes. Kafka checks the count before it validates any
        // batch, so this check runs before `rejection`. See
        // [ProduceRequest.java#L225-L246].
        //
        // [ProduceRequest.java#L225-L246]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/ProduceRequest.java#L225-L246
        let batch_count = batches.len();
        let Ok([mut batch]) = <[deflated::Batch; 1]>::try_from(batches) else {
            const REASON: &str =
                "a produce request must contain exactly one record batch per partition";

            warn!(
                topic = name,
                partition = partition.index,
                batch_count,
                reason = REASON,
                "rejecting produce request",
            );

            return self
                .error(partition.index, ErrorCode::InvalidRecord)
                .error_message(Some(REASON.into()));
        };

        if let Some(rejection) = rejection(&batch) {
            return self.reject(name, partition.index, &batch, rejection.into());
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_default();

        match validate_timestamps_offloaded(batch, timestamp_type, now).await {
            Ok((validated, None)) => batch = validated,
            Ok((validated, Some(rejection))) => {
                return self.reject(name, partition.index, &validated, rejection);
            }
            Err(err) => {
                error!(?err);
                return self.error(partition.index, ErrorCode::UnknownServerError);
            }
        }

        // Kafka returns -1 as `log_append_time_ms` for a `CreateTime` topic.
        let log_append_time_ms = match timestamp_type {
            TimestampType::CreateTime => NO_TIMESTAMP,
            TimestampType::LogAppendTime => now,
        };

        let tp = Topition::new(name, partition.index);
        let producer_id = batch.producer_id;
        let producer_epoch = batch.producer_epoch;
        let base_sequence = batch.base_sequence;

        match self.storage.produce(transaction_id, &tp, batch).await {
            Ok(base_offset) => PartitionProduceResponse::default()
                .index(partition.index)
                .error_code(ErrorCode::None.into())
                .base_offset(base_offset)
                .log_append_time_ms(Some(log_append_time_ms))
                .log_start_offset(Some(0))
                .record_errors(Some([].into()))
                .error_message(None)
                .current_leader(None),

            Err(error) => {
                let error_code = storage_error_code(&error);

                // Logged once, here, with the partition: a rejection
                // the client caused is a warning, an internal failure
                // is an error.
                if error_code == ErrorCode::UnknownServerError {
                    error!(
                        topic = name,
                        partition = partition.index,
                        producer_id,
                        producer_epoch,
                        base_sequence,
                        ?error,
                        "produce failed"
                    );
                } else if let Error::MessageTooLarge { size, limit } = error {
                    warn!(
                        topic = name,
                        partition = partition.index,
                        producer_id,
                        size,
                        limit,
                        "rejecting produce batch larger than max.message.bytes"
                    );
                } else {
                    warn!(
                        topic = name,
                        partition = partition.index,
                        producer_id,
                        producer_epoch,
                        base_sequence,
                        ?error_code,
                        %error,
                        "rejecting produce batch"
                    );
                }

                let response = self.error(partition.index, error_code);

                // Kafka sends the same message, so that the producer's `RecordTooLargeException`
                // names both sizes.
                if let Error::MessageTooLarge { size, limit, .. } = error {
                    response.error_message(Some(format!(
                        "The record batch size in the append to {name}-{} is {size} bytes \
                         which exceeds the maximum configured value of {limit}.",
                        partition.index
                    )))
                } else {
                    response
                }
            }
        }
    }

    /// Returns the `message.timestamp.type` of the topic `name`, as
    /// [`message_timestamp_type::described`] reads it from the topic's configs.
    ///
    /// The lookup and `storage.produce()` run in separate transactions. If the topic's
    /// `message.timestamp.type` changes between them, the batch keeps the type that this function
    /// read. The batch stores its type in its attributes, so a consumer still reads its timestamps
    /// correctly. Kafka also applies a config change asynchronously, and an append that is in
    /// progress can use the old type.
    async fn timestamp_type(&self, name: &str) -> Result<TimestampType> {
        let described = self
            .storage
            .describe_config(
                name,
                ConfigResource::Topic,
                Some(&[message_timestamp_type::KEY.to_owned()]),
            )
            .await?;

        message_timestamp_type::described(name, described)
    }

    #[instrument(skip_all)]
    async fn topic(
        &self,
        transaction_id: Option<&str>,
        topic: TopicProduceData,
    ) -> TopicProduceResponse {
        let partition_data = topic.partition_data.unwrap_or_default();
        let mut partitions = Vec::with_capacity(partition_data.len());

        match self.timestamp_type(&topic.name).await {
            Ok(timestamp_type) => {
                for partition in partition_data {
                    partitions.push(
                        self.partition(transaction_id, &topic.name, timestamp_type, partition)
                            .await,
                    )
                }
            }

            Err(error) => {
                error!(
                    topic = topic.name,
                    ?error,
                    "message.timestamp.type lookup failed"
                );

                let error_code = storage_error_code(&error);

                partitions.extend(
                    partition_data
                        .iter()
                        .map(|partition| self.error(partition.index, error_code)),
                );
            }
        }

        TopicProduceResponse::default()
            .name(topic.name)
            .partition_responses(Some(partitions))
    }
}

impl<G, I> Service<I> for ProduceService<G>
where
    G: Storage,
    I: Into<RequestInput<ProduceRequest>> + Send + 'static,
{
    type Output = ProduceResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        let topics = input.request.topic_data.unwrap_or_default();
        let mut responses = Vec::with_capacity(topics.len());

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
        if Ack::try_from(input.request.acks).is_err() {
            warn!(
                acks = input.request.acks,
                topics = topics.len(),
                "rejecting produce request: acks must be -1, 0 or 1"
            );

            responses.extend(
                topics
                    .into_iter()
                    .map(|topic| self.topic_error(topic, ErrorCode::InvalidRequiredAcks)),
            );
        } else {
            for topic in topics {
                responses.push(
                    self.topic(input.request.transactional_id.as_deref(), topic)
                        .await,
                )
            }
        }

        // Kafka's `KafkaApis.handleProduceRequest` closes the connection for an
        // `acks=0` request whenever any partition in the assembled response map
        // carries an error, not only a storage failure: `unauthorizedTopicResponses`,
        // `nonExistingTopicResponses` and `invalidRequestResponses` are merged into
        // that map before the check runs. The client reads no response under
        // `acks=0` regardless, so closing on any error (rather than silently
        // dropping the batch) matches that behaviour and gives a real Kafka
        // producer a retriable `NetworkException` instead of nothing at all.
        if input.request.acks == i16::from(Ack::None) {
            if let Some((topic, partition, error_code)) = first_error(&responses) {
                ACKS_ZERO_PRODUCE_FAILED
                    .add(1, &[KeyValue::new("error_code", format!("{error_code:?}"))]);

                return Err(Error::AcksZeroProduceFailed {
                    topic,
                    partition,
                    error_code,
                });
            }

            SuppressResponseExtension::mark(&input.extensions);
        }

        Ok(ProduceResponse::default()
            .responses(Some(responses))
            .throttle_time_ms(Some(0))
            .node_endpoints(None))
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
