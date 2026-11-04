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
mod topic;
mod topic_configuration;

use std::convert::Infallible;

use nisshi_sans_io::{
    ApiKey, ErrorCode, ProduceRequest, ProduceResponse, produce_response::TopicProduceResponse,
    record::inflated,
};
use nisshi_schema::Registry;
use rama::{Layer as _, Service};
use tracing::{debug, instrument};

pub use self::{
    acks_zero::{AcksZeroLayer, AcksZeroService},
    request_validation::{
        ProduceRequestValidationLayer, ProduceRequestValidationService, ValidatedProduceRequest,
        ValidatedTopic,
    },
    topic::ProduceTopicService,
};
use crate::{Error, Result, Storage};

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

/// A [`Service`] taking [`ValidatedProduceRequest`] returning [`ProduceResponse`], that serves each
/// topic of the request, in order, with `topics`.
///
/// [`ProduceService::new`] gives it a [`ProduceTopicService`] that uses a [`Storage`]. Call
/// [`ProduceService::with_validation`] to get a service that takes a [`ProduceRequest`].
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
pub struct ProduceService<S> {
    topics: S,
}

impl<S> ApiKey for ProduceService<S> {
    const KEY: i16 = ProduceRequest::KEY;
}

impl<G> ProduceService<ProduceTopicService<G>>
where
    G: Storage,
{
    /// Returns a service that stores batches in `storage`, and does not validate them against a
    /// schema.
    pub fn new(storage: G) -> Self {
        Self {
            topics: ProduceTopicService::new(storage),
        }
    }

    /// Returns this service with `schemas`: the registry that holds the schema of each topic. A
    /// batch for a topic that has a schema must match it before the service stores the batch,
    /// unless the topic's `schema.validation` is `false`.
    pub fn with_schemas(self, schemas: Option<Registry>) -> Self {
        Self {
            topics: self.topics.with_schemas(schemas),
        }
    }
}

impl<S> ProduceService<S>
where
    S: Service<ValidatedTopic, Output = TopicProduceResponse, Error = Infallible>,
{
    /// Returns this service behind the layers that give it a [`ValidatedProduceRequest`] and that
    /// close the connection of a failed `acks=0` request: [`AcksZeroLayer`] around
    /// [`ProduceRequestValidationLayer`].
    pub fn with_validation(
        self,
    ) -> AcksZeroService<ProduceRequestValidationService<ProduceService<S>>> {
        AcksZeroLayer::new().layer(ProduceRequestValidationLayer::new().layer(self))
    }
}

impl<S> Service<ValidatedProduceRequest> for ProduceService<S>
where
    S: Service<ValidatedTopic, Output = TopicProduceResponse, Error = Infallible>,
{
    type Output = ProduceResponse;
    type Error = Error;

    #[instrument(skip(self, request))]
    async fn serve(&self, request: ValidatedProduceRequest) -> Result<Self::Output, Self::Error> {
        let mut responses = Vec::with_capacity(request.topics.len());

        for topic in request.topics {
            let Ok(response) = self.topics.serve(topic).await;
            responses.push(response);
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

#[cfg(test)]
mod fan_out_tests {
    use std::sync::{Arc, Mutex};

    use nisshi_sans_io::{
        RequestInput,
        produce_request::{PartitionProduceData, TopicProduceData},
    };
    use rama::extensions::Extensions;

    use super::*;

    /// A topic that [`RecordingTopics`] served.
    #[derive(Clone, Debug, PartialEq)]
    struct ServedTopic {
        name: String,
        transaction_id: Option<String>,
    }

    impl ServedTopic {
        fn new(name: &str, transaction_id: Option<&str>) -> Self {
            Self {
                name: name.to_owned(),
                transaction_id: transaction_id.map(Into::into),
            }
        }
    }

    /// A topic service that records each topic that it serves, and answers the topic with its name
    /// and no partitions.
    #[derive(Clone, Debug, Default)]
    struct RecordingTopics {
        served: Arc<Mutex<Vec<ServedTopic>>>,
    }

    impl RecordingTopics {
        fn served(&self) -> Vec<ServedTopic> {
            self.served
                .lock()
                .expect("a test thread panicked while it held the lock")
                .clone()
        }
    }

    impl Service<ValidatedTopic> for RecordingTopics {
        type Output = TopicProduceResponse;
        type Error = Infallible;

        async fn serve(&self, topic: ValidatedTopic) -> Result<Self::Output, Self::Error> {
            self.served
                .lock()
                .expect("a test thread panicked while it held the lock")
                .push(ServedTopic {
                    name: topic.name.clone(),
                    transaction_id: topic.transaction_id,
                });

            Ok(TopicProduceResponse::default().name(topic.name))
        }
    }

    /// Serves a request for each of `topics` with `topics_service`, through the layer that builds
    /// each [`ValidatedTopic`] from the request.
    async fn serve(
        topics_service: &RecordingTopics,
        transactional_id: Option<&str>,
        topics: &[&str],
    ) -> Result<Vec<String>> {
        let topic_data = topics
            .iter()
            .map(|name| {
                TopicProduceData::default()
                    .name((*name).into())
                    .partition_data(Some(vec![PartitionProduceData::default().index(0)]))
            })
            .collect();

        let response = ProduceRequestValidationLayer::new()
            .layer(ProduceService {
                topics: topics_service.clone(),
            })
            .serve(RequestInput {
                request: ProduceRequest::default()
                    .acks(-1)
                    .transactional_id(transactional_id.map(Into::into))
                    .topic_data(Some(topic_data)),
                extensions: Extensions::default(),
            })
            .await?;

        Ok(response
            .responses
            .unwrap_or_default()
            .into_iter()
            .map(|topic| topic.name)
            .collect())
    }

    #[tokio::test]
    async fn topics_are_served_and_answered_in_the_order_of_the_request() -> Result<()> {
        let topics_service = RecordingTopics::default();

        let answered = serve(&topics_service, None, &["c", "a", "b"]).await?;

        assert_eq!(["c", "a", "b"], answered.as_slice());
        assert_eq!(
            ["c", "a", "b"],
            topics_service
                .served()
                .iter()
                .map(|served| served.name.as_str())
                .collect::<Vec<_>>()
                .as_slice()
        );

        Ok(())
    }

    #[tokio::test]
    async fn each_topic_is_served_in_the_transaction_of_the_request() -> Result<()> {
        let topics_service = RecordingTopics::default();

        _ = serve(&topics_service, Some("transaction"), &["a", "b"]).await?;

        assert_eq!(
            [
                ServedTopic::new("a", Some("transaction")),
                ServedTopic::new("b", Some("transaction")),
            ],
            topics_service.served().as_slice()
        );

        Ok(())
    }

    #[tokio::test]
    async fn a_request_outside_a_transaction_serves_each_topic_without_one() -> Result<()> {
        let topics_service = RecordingTopics::default();

        _ = serve(&topics_service, None, &["a"]).await?;

        assert_eq!(
            [ServedTopic::new("a", None)],
            topics_service.served().as_slice()
        );

        Ok(())
    }

    #[tokio::test]
    async fn a_request_without_topics_serves_no_topic() -> Result<()> {
        let topics_service = RecordingTopics::default();

        let answered = serve(&topics_service, None, &[]).await?;

        assert!(answered.is_empty());
        assert!(topics_service.served().is_empty());

        Ok(())
    }
}
