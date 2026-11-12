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
mod admission;
mod batch_validation;
mod partition;
mod rejection;
mod request_validation;
mod response;
mod schema;
mod store;
#[cfg(test)]
mod tests;
mod timestamps;
mod topic;
mod topic_configuration;
mod unknown_partition;

use std::{convert::Infallible, sync::Arc, time::Duration};

use nisshi_sans_io::{
    ApiKey, ErrorCode, ProduceRequest, ProduceResponse, produce_response::TopicProduceResponse,
    record::inflated,
};
use nisshi_schema::Registry;
use rama::{Layer as _, Service};
use tracing::{debug, instrument};

pub use self::{
    acks_zero::{AcksZeroLayer, AcksZeroService},
    admission::Admission,
    batch_validation::{BatchValidationLayer, BatchValidationService},
    partition::PartitionProduce,
    request_validation::{
        ProduceRequestValidationLayer, ProduceRequestValidationService, ValidatedProduceRequest,
        ValidatedTopic,
    },
    schema::{SchemaValidationLayer, SchemaValidationService},
    store::StorePartitionService,
    timestamps::{TimestampLayer, TimestampService},
    topic::ProduceTopicService,
    topic_configuration::TopicConfiguration,
    unknown_partition::{UnknownPartitionLayer, UnknownPartitionService},
};
use crate::{
    Error, Result, Storage,
    decoded_budget::{self, DecodedBytesBudget},
};

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
/// # #![recursion_limit = "256"]
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
    budget: Arc<DecodedBytesBudget>,
    wait: Duration,
}

impl<S> ApiKey for ProduceService<S> {
    const KEY: i16 = ProduceRequest::KEY;
}

/// The steps that [`ProduceService::new`] applies to each partition of a topic, in order: the
/// batch checks, the timestamp validation, the schema validation, and the write to storage. The
/// outermost step also answers a rejected batch for a partition that does not exist with
/// `UNKNOWN_TOPIC_OR_PARTITION`.
pub type StoredPartitions<G> = UnknownPartitionService<
    G,
    BatchValidationService<TimestampService<SchemaValidationService<StorePartitionService<G>>>>,
>;

impl<G> ProduceService<ProduceTopicService<G, StoredPartitions<G>>>
where
    G: Storage + Clone,
{
    /// Returns a service that validates the batches of a request, and stores them in `storage`.
    /// The service does not validate them against a schema.
    pub fn new(storage: G) -> Self {
        Self::with_schemas(storage, None)
    }

    /// Returns a service like [`ProduceService::new`] that also validates each batch against the
    /// schema of its topic in `schemas`, the registry that holds the schema of each topic, unless
    /// the topic's `schema.validation` is `false`.
    pub fn with_schemas(storage: G, schemas: Option<Registry>) -> Self {
        let partitions = (
            UnknownPartitionLayer::new(storage.clone()),
            BatchValidationLayer::new(),
            TimestampLayer::new(),
            SchemaValidationLayer::new(schemas),
        )
            .into_layer(StorePartitionService::new(storage.clone()));

        Self::from_topics(ProduceTopicService::new(storage, partitions))
    }
}

impl<S> ProduceService<S> {
    /// Returns a service that serves each topic with `topics`, and holds the decoded batches of a
    /// request against the process-wide budget, for which a request waits at most
    /// [`decoded_budget::WAIT_TIMEOUT`].
    fn from_topics(topics: S) -> Self {
        Self {
            topics,
            budget: decoded_budget::produce(),
            wait: decoded_budget::WAIT_TIMEOUT,
        }
    }

    /// Returns this service holding the decoded batches of a request against `budget`, for which
    /// a request waits at most `wait`.
    #[cfg(test)]
    pub(super) fn with_decoded_budget(
        self,
        budget: Arc<DecodedBytesBudget>,
        wait: Duration,
    ) -> Self {
        Self {
            budget,
            wait,
            ..self
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

        // One wait for the whole request, so that a request with many partitions does not wait
        // the full time for each one.
        let admission = Admission::new(Arc::clone(&self.budget), self.wait);

        for topic in request.topics {
            let topic = ValidatedTopic {
                admission: admission.clone(),
                ..topic
            };

            let Ok(response) = self.topics.serve(topic).await;
            responses.push(response);
        }

        Ok(response::produce_response(responses))
    }
}
