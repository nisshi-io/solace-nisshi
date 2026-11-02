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
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Debug, Display, Formatter},
    ops::DerefMut as _,
    pin::Pin,
    sync::{Arc, LazyLock, Mutex, MutexGuard},
    task::{self, Poll, Waker},
    time::{Duration, SystemTime},
    vec::IntoIter,
};

use nisshi_sans_io::topic_config;
use nisshi_sans_io::{
    Ack, ErrorCode, ProduceRequest, ProduceResponse, RequestInput, TimestampType,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    record::{
        deflated::{self, Frame},
        inflated,
    },
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use rama::{Layer, Service};
use tokio::time::sleep;
use tracing::{debug, error};
use uuid::Uuid;

use crate::{Error, METER, topic::ResourceConfig};

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Topition {
    topic: String,
    partition: i32,
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct BatchRequest {
    id: Uuid,
    request: ProduceRequest,
}
impl BatchRequest {
    fn number_of_records(&self) -> usize {
        number_of_records(&self.request)
    }
}

#[derive(Clone, Debug)]
enum BatchResponse {
    Waker(Waker),
    Response(ProduceResponse),
}

#[derive(Clone, Default)]
pub(crate) struct BatchProduceService<S> {
    service: S,
    requests: Arc<Mutex<Vec<BatchRequest>>>,
    responses: Arc<Mutex<BTreeMap<Uuid, BatchResponse>>>,
    resource_config: ResourceConfig,
}

static SEND_PENDING_BATCH_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("send_pending_batch")
        .with_description("The number times we send a pending batch")
        .build()
});

static SEND_PENDING_EMPTY_BATCH_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("send_pending_empty_batch")
        .with_description("The number times we tried to send an empty pending batch")
        .build()
});

static SEND_PENDING_PRODUCE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("send_pending_produce_duration")
        .with_unit("ms")
        .with_description("The send pending produce latency in milliseconds")
        .build()
});

static SEND_PENDING_FOUND_OWNER_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("send_pending_found_owner")
        .with_description("The number of owners found after sending a pending batch")
        .build()
});

static SEND_PENDING_OWNER_WAKE_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("send_pending_owner_wake")
        .with_description(
            "The number of owners we have called wake on after sending a pending batch",
        )
        .build()
});

pub(crate) fn topic_names(req: &ProduceRequest) -> Vec<String> {
    let mut topics = req.topic_data.as_deref().map_or(vec![], |topics| {
        topics
            .iter()
            .map(|topic| topic.name.clone())
            .collect::<Vec<_>>()
    });

    topics.sort();
    topics.dedup();
    topics
}

fn size_of(req: &ProduceRequest, unit: fn(&deflated::Batch) -> usize) -> usize {
    req.topic_data.as_ref().map_or(0, |topics| {
        topics
            .iter()
            .map(|topic| {
                topic.partition_data.as_ref().map_or(0, |partitions| {
                    partitions
                        .iter()
                        .map(|partition| {
                            partition
                                .records
                                .as_ref()
                                .map_or(0, |frame| frame.batches.iter().map(unit).sum())
                        })
                        .sum()
                })
            })
            .sum()
    })
}

pub(crate) fn number_of_records(req: &ProduceRequest) -> usize {
    size_of(req, |batch| batch.record_count as usize)
}

#[allow(dead_code)]
pub(crate) fn number_of_bytes(req: &ProduceRequest) -> usize {
    size_of(req, |batch| batch.record_data.len())
}

/// Returns true if every batch of `req` may be merged with the batches of other requests: it has
/// no producer id, and is neither transactional nor control, nor stamped with a log append time.
///
/// A merged batch carries one header, the first owner's, for everyone's records. A batch with a
/// producer id has a producer epoch and a base sequence that the origin checks per producer, and
/// a transactional or control batch belongs to one transaction. Merging such a batch would give
/// another owner's records that header, and fail an idempotent producer with
/// `OUT_OF_ORDER_SEQUENCE_NUMBER` or pull the records into a transaction. The origin stores
/// each such batch as sent.
fn mergeable(req: &ProduceRequest) -> bool {
    req.topic_data
        .as_deref()
        .unwrap_or_default()
        .iter()
        .flat_map(|topic| topic.partition_data.as_deref().unwrap_or_default())
        .filter_map(|partition| partition.records.as_ref())
        .flat_map(|frame| &frame.batches)
        .all(|batch| {
            batch.producer_id == -1
                && !batch.is_transactional()
                && !batch.is_control()
                && TimestampType::from(batch.attributes) == TimestampType::CreateTime
        })
}

/// Lock `mutex`, aborting the process if a panic poisoned it.
///
/// A panic part way through updating the pending requests or responses leaves
/// tickets that never get an answer, and recovering the lock cannot repair
/// that: those requests would wait forever while the proxy kept accepting
/// more. The proxy is stateless, so it is better to stop and let a supervisor
/// restart it.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| {
        error!(?poisoned, "produce batch lock poisoned, aborting");
        std::process::abort()
    })
}

impl<S> BatchProduceService<S>
where
    S: Clone + Debug,
{
    async fn send_pending_batch(&self, id: &Uuid) -> Result<Option<ProduceResponse>, Error>
    where
        S: Service<RequestInput<ProduceRequest>, Output = ProduceResponse> + Clone + Debug,
        S::Error: Into<Error> + Send + Debug + 'static,
    {
        debug!(%id);

        SEND_PENDING_BATCH_COUNTER.add(1, &[]);

        let requests = {
            let mut guard = lock(&self.requests);
            std::mem::take(guard.deref_mut())
        };
        debug!(?requests);

        if requests.is_empty() {
            SEND_PENDING_EMPTY_BATCH_COUNTER.add(1, &[]);
            return Ok(None);
        }

        let mut owners = Owner::from(&requests[..]);
        let (produce_request, rejections) = produce_request(requests);

        for rejection in rejections {
            owners.reject(rejection);
        }

        let produce_response = if produce_request
            .topic_data
            .as_deref()
            .unwrap_or_default()
            .is_empty()
        {
            ProduceResponse::default()
                .responses(Some([].into()))
                .throttle_time_ms(Some(0))
                .node_endpoints(None)
        } else {
            let start = SystemTime::now();

            self.service
                .serve(produce_request.into())
                .await
                .inspect(|response| debug!(?response))
                .inspect_err(|err| debug!(?err))
                .map_err(Into::into)
                .inspect(|_| {
                    SEND_PENDING_PRODUCE_DURATION.record(
                        start
                            .elapsed()
                            .map_or(0, |duration| duration.as_millis() as u64),
                        &[],
                    )
                })?
        };

        let mut responses = lock(&self.responses);

        let mut owners = owners.split(produce_response);
        debug!(?owners);
        SEND_PENDING_FOUND_OWNER_COUNTER.add(owners.len() as u64, &[]);

        if let Some(produce_response) = owners
            .remove(id)
            .inspect(|produce_response| debug!(%id, ?produce_response))
        {
            _ = responses
                .remove(id)
                .inspect(|response| debug!(%id, ?response));

            for (owner, response) in owners {
                debug!(?owner, ?response);

                if let Some(BatchResponse::Waker(waker)) =
                    responses.insert(owner, BatchResponse::Response(response))
                {
                    SEND_PENDING_OWNER_WAKE_COUNTER.add(1, &[]);
                    waker.wake();
                }
            }

            Ok(Some(produce_response))
        } else {
            for (owner, response) in owners {
                debug!(%owner, ?response);
                if let Some(BatchResponse::Waker(waker)) =
                    responses.insert(owner, BatchResponse::Response(response))
                {
                    SEND_PENDING_OWNER_WAKE_COUNTER.add(1, &[]);
                    waker.wake();
                }
            }

            Ok(None)
        }
    }
}

static BATCH_OVERFLOW_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_record_overflow")
        .with_description("The number of overflows while producing a batch")
        .build()
});

static BATCH_REJECTED_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_rejected")
        .with_description(
            "The number of partitions of a request that the proxy answers with an error, \
             because their batches do not merge",
        )
        .build()
});

static BATCH_BYPASSED_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_bypassed")
        .with_description(
            "The number of requests that the proxy forwards at once, because their batches \
             carry a producer id, a transaction or a log append time and do not merge",
        )
        .build()
});

static TIMEOUT_EXPIRED_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_timeout_expired")
        .with_description("The timeouts while preparing a batch")
        .build()
});

static TICKET_READY_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_ticket_ready")
        .with_description("The ticket was ready")
        .build()
});

impl<S> Service<RequestInput<ProduceRequest>> for BatchProduceService<S>
where
    S: Service<RequestInput<ProduceRequest>, Output = ProduceResponse> + Clone + Debug,
    S::Error: Clone + Into<Error> + Send + Debug + 'static,
{
    type Output = ProduceResponse;

    type Error = Error;

    async fn serve(
        &self,
        request: RequestInput<ProduceRequest>,
    ) -> Result<Self::Output, Self::Error> {
        debug!(?request);

        // A request that cannot merge gains nothing from waiting for others, so it goes to the
        // origin as it is, at once.
        if !mergeable(&request.request) {
            BATCH_BYPASSED_COUNTER.add(1, &[]);

            return self.service.serve(request).await.map_err(Into::into);
        }

        let batch_timeout_ms = Duration::from_millis(
            topic_names(&request.request)
                .iter()
                .filter_map(|topic_name| {
                    self.resource_config
                        .get(topic_name, topic_config::BATCH_TIMEOUT_MS)
                        .and_then(|value| value.as_u64())
                })
                .min()
                .inspect(|batch_timeout_ms| debug!(?batch_timeout_ms))
                .unwrap_or(10_000),
        );

        let max_records = topic_names(&request.request)
            .iter()
            .filter_map(|topic_name| {
                self.resource_config
                    .get(topic_name, topic_config::BATCH_MAX_RECORDS)
                    .and_then(|value| value.as_usize())
            })
            .min()
            .inspect(|max_records| debug!(?max_records))
            .unwrap_or(1_000);

        let ticket = {
            let mut requests = lock(&self.requests);
            let ticket = Ticket::new(self.clone());
            requests.push(BatchRequest {
                id: ticket.id,
                request: request.request,
            });
            ticket
        };

        loop {
            let id = ticket.id;

            let num_records = lock(&self.requests)
                .iter()
                .map(|batch_request| batch_request.number_of_records())
                .sum::<usize>();
            debug!(num_records, max_records);

            if num_records >= max_records {
                BATCH_OVERFLOW_COUNTER.add(1, &[]);

                if let Ok(Some(produce_response)) =
                    self.send_pending_batch(&id).await.inspect(|r| debug!(?r))
                {
                    return Ok(produce_response);
                }
            }

            let patience = sleep(batch_timeout_ms);
            let ticket = ticket.clone();
            debug!(?patience);

            tokio::select! {
                response = ticket  => {
                    TICKET_READY_COUNTER.add(1, &[]);

                    return response
                    .inspect(|response|debug!(?response))
                    .inspect_err(|err|debug!(?err))
                }

                expired = patience => {
                    debug!(?expired);

                    TIMEOUT_EXPIRED_COUNTER.add(1, &[]);

                    if let Ok(Some(produce_response)) = self.send_pending_batch(&id).await.inspect(|r|debug!(?r)) {
                        return Ok(produce_response)
                    }
                }
            }
        }
    }
}

impl<S> BatchProduceService<S> {
    fn new(resource_config: ResourceConfig, service: S) -> Self {
        Self {
            service,
            requests: Arc::new(Mutex::new(Vec::new())),
            responses: Arc::new(Mutex::new(BTreeMap::new())),
            resource_config,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct BatchProduceLayer {
    resource_config: ResourceConfig,
}

impl BatchProduceLayer {
    pub(crate) fn new(resource_config: ResourceConfig) -> Self {
        Self { resource_config }
    }
}

impl<S> Layer<S> for BatchProduceLayer
where
    S: Debug + Clone,
{
    type Service = BatchProduceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service::new(self.resource_config.clone(), inner)
    }
}

type Topic = String;
type Partition = i32;

struct Owner {
    topition: BTreeMap<Topition, BTreeSet<Uuid>>,
    rejections: Vec<Rejection>,
}

/// A partition of one request that the proxy answers itself, because the request's batches for it
/// do not merge.
#[derive(Clone, Debug)]
struct Rejection {
    owner: Uuid,
    topition: Topition,
    response: PartitionProduceResponse,
}

impl From<&[BatchRequest]> for Owner {
    fn from(requests: &[BatchRequest]) -> Self {
        debug!(?requests);

        let mut topition = BTreeMap::<Topition, BTreeSet<Uuid>>::new();

        for request in requests {
            debug!(?request);

            for topic in request.request.topic_data.as_deref().unwrap_or_default() {
                debug!(?topic);

                for partition in topic.partition_data.as_deref().unwrap_or_default() {
                    debug!(?partition);

                    _ = topition
                        .entry(Topition {
                            topic: topic.name.clone(),
                            partition: partition.index,
                        })
                        .or_default()
                        .insert(request.id);
                }
            }
        }

        Self {
            topition,
            rejections: Vec::new(),
        }
    }
}

impl Owner {
    /// Answers the rejected owner's partition with the rejection's response, in place of the
    /// origin's response for that partition.
    fn reject(&mut self, rejection: Rejection) {
        if let Some(owners) = self.topition.get_mut(&rejection.topition) {
            _ = owners.remove(&rejection.owner);
        }

        self.rejections.push(rejection);
    }

    fn split(self, produce_response: ProduceResponse) -> BTreeMap<Uuid, ProduceResponse> {
        let mut responses = BTreeMap::<Uuid, BatchTopicProduceResponse>::new();

        for rejection in self.rejections {
            _ = responses
                .entry(rejection.owner)
                .or_default()
                .0
                .entry(rejection.topition.topic)
                .or_default()
                .0
                .insert(rejection.topition.partition, rejection.response);
        }

        for topic in produce_response.responses.unwrap_or_default() {
            debug!(?topic);

            for partition in topic.partition_responses.unwrap_or_default() {
                let topition = Topition {
                    topic: topic.name.clone(),
                    partition: partition.index,
                };

                debug!(?topition);

                for owner in self.topition.get(&topition).cloned().unwrap_or_default() {
                    debug!(?owner);

                    _ = responses
                        .entry(owner)
                        .or_default()
                        .0
                        .entry(topic.name.clone())
                        .or_default()
                        .0
                        .insert(partition.index, partition.clone())
                        .inspect(|partition_produce_response| debug!(?partition_produce_response));
                }
            }
        }

        responses
            .into_iter()
            .inspect(|(owner, batch_topic_produce_response)| {
                debug!(?owner, ?batch_topic_produce_response)
            })
            .map(|(owner, batch_topic_produce_response)| {
                (
                    owner,
                    ProduceResponse::default()
                        .responses(Some(batch_topic_produce_response.into_iter().collect()))
                        .throttle_time_ms(produce_response.throttle_time_ms)
                        .node_endpoints(produce_response.node_endpoints.clone()),
                )
            })
            .inspect(|(owner, produce_response)| debug!(?owner, ?produce_response))
            .collect()
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct BatchTopicProduceResponse(BTreeMap<Topic, BatchPartitionProduceResponse>);

impl AsRef<BTreeMap<Topic, BatchPartitionProduceResponse>> for BatchTopicProduceResponse {
    fn as_ref(&self) -> &BTreeMap<Topic, BatchPartitionProduceResponse> {
        &self.0
    }
}

impl IntoIterator for BatchTopicProduceResponse {
    type Item = TopicProduceResponse;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0
            .into_iter()
            .map(|(name, partitions)| {
                TopicProduceResponse::default()
                    .name(name)
                    .partition_responses(Some(partitions.into_iter().collect()))
            })
            .collect::<Vec<_>>()
            .into_iter()
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct BatchPartitionProduceResponse(BTreeMap<Partition, PartitionProduceResponse>);

impl AsRef<BTreeMap<Partition, PartitionProduceResponse>> for BatchPartitionProduceResponse {
    fn as_ref(&self) -> &BTreeMap<Partition, PartitionProduceResponse> {
        &self.0
    }
}

impl IntoIterator for BatchPartitionProduceResponse {
    type Item = PartitionProduceResponse;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_values().collect::<Vec<_>>().into_iter()
    }
}

/// Returns the request that the proxy forwards to the origin for `requests`, and the partitions
/// that it rejects and answers itself.
fn produce_request(requests: Vec<BatchRequest>) -> (ProduceRequest, Vec<Rejection>) {
    debug!(?requests);

    let acks = batch_acks(&requests);

    let mut run = TopicPartitionBatch::default();
    for request in requests {
        debug!(?request);

        for topic in request.request.topic_data.unwrap_or_default() {
            debug!(?topic);

            for partition in topic.partition_data.unwrap_or_default() {
                debug!(?partition);

                if let Some(records) = partition.records {
                    run.topics
                        .entry(topic.name.clone())
                        .or_default()
                        .partitions
                        .entry(partition.index)
                        .or_default()
                        .extend(records.batches.into_iter().map(|batch| OwnedBatch {
                            owner: request.id,
                            batch,
                        }));
                }
            }
        }
    }

    let (topic_data, rejections) = run.into_topic_data();

    (
        ProduceRequest::default()
            .acks(acks.into())
            .topic_data(Some(topic_data))
            .timeout_ms(5_000),
        rejections,
    )
}

/// Returns the `acks` for a batch: the strictest that any of its requests
/// asked for, and at least [`Ack::Leader`].
///
/// The proxy waits for the origin's response to each batch, and an origin
/// does not answer an `acks=0` Produce, so a batch never goes out at
/// `acks=0`.
fn batch_acks(requests: &[BatchRequest]) -> Ack {
    if requests
        .iter()
        .any(|request| request.request.acks == i16::from(Ack::FullIsr))
    {
        Ack::FullIsr
    } else {
        Ack::Leader
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TopicPartitionBatch {
    topics: BTreeMap<Topic, PartitionBatch>,
}

impl TopicPartitionBatch {
    /// Returns the topics that the proxy forwards, with the batches of each partition combined,
    /// and the partitions that it rejects. A topic or a partition with no batch left to forward
    /// is left out.
    fn into_topic_data(self) -> (Vec<TopicProduceData>, Vec<Rejection>) {
        let mut topic_data = Vec::new();
        let mut rejections = Vec::new();

        for (name, partitions) in self.topics {
            let mut partition_data = Vec::new();

            for (index, batches) in partitions.partitions {
                let combined = combine(batches);

                for (owner, rejection) in combined.rejected {
                    let error_code = rejection.error_code();
                    debug!(
                        topic = name,
                        partition = index,
                        %owner,
                        %rejection,
                        ?error_code,
                        "produce batch rejected"
                    );

                    BATCH_REJECTED_COUNTER
                        .add(1, &[KeyValue::new("error_code", format!("{error_code:?}"))]);

                    rejections.push(Rejection {
                        owner,
                        topition: Topition {
                            topic: name.clone(),
                            partition: index,
                        },
                        response: rejected_partition_response(index, error_code),
                    });
                }

                if !combined.forwarded.is_empty() {
                    partition_data.push(PartitionProduceData::default().index(index).records(
                        Some(Frame {
                            batches: combined.forwarded,
                        }),
                    ));
                }
            }

            if !partition_data.is_empty() {
                topic_data.push(
                    TopicProduceData::default()
                        .name(name)
                        .partition_data(Some(partition_data)),
                );
            }
        }

        (topic_data, rejections)
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct PartitionBatch {
    partitions: BTreeMap<Partition, Vec<OwnedBatch>>,
}

/// A batch, with the id of the request that sent it.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct OwnedBatch {
    owner: Uuid,
    batch: deflated::Batch,
}

/// The batch of one owner for one partition, as sent and decoded.
struct OwnerBatch {
    owner: Uuid,
    as_sent: deflated::Batch,
    decoded: inflated::Batch,
}

/// Why the proxy rejects an owner's batches for a partition.
#[derive(Debug)]
enum BatchRejection {
    /// `last_offset_delta + 1` does not equal `record_count`.
    OffsetRange {
        last_offset_delta: i32,
        record_count: u32,
    },

    /// The request has more than one batch for the partition.
    MultipleBatches(usize),

    /// The records do not decode.
    Decode(nisshi_sans_io::Error),

    /// An offset delta overflows an `i32` when the batch is appended to the other batches.
    Append(nisshi_sans_io::Error),

    /// The merged batch does not encode.
    Encode(nisshi_sans_io::Error),
}

impl Display for BatchRejection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::OffsetRange {
                last_offset_delta,
                record_count,
            } => write!(
                f,
                "last_offset_delta {last_offset_delta} + 1 does not equal record_count {record_count}"
            ),
            Self::MultipleBatches(count) => {
                write!(f, "{count} batches for one partition, where one is allowed")
            }
            Self::Decode(error) => write!(f, "records do not decode: {error}"),
            Self::Append(error) => write!(f, "batch does not append: {error}"),
            Self::Encode(error) => write!(f, "merged batch does not encode: {error}"),
        }
    }
}

impl BatchRejection {
    /// Returns the error code that the proxy answers for the rejected partition.
    ///
    /// For a batch that does not decode, Kafka 3.9.1 answers INVALID_RECORD when the records do
    /// not decode ([`DefaultRecordBatch`]), and UNKNOWN_SERVER_ERROR when they do not decompress.
    /// The proxy answers as a Nisshi broker does: MESSAGE_TOO_LARGE for a batch over the decoded
    /// size limit, and INVALID_RECORD for every other decode error.
    ///
    /// Kafka 3.9.1 answers INVALID_RECORD for a partition with more than one batch in a Produce
    /// request of version 3 or later ([`ProduceRequest.validateRecords`]), for that partition only
    /// ([`KafkaApis.handleProduceRequest`]).
    ///
    /// Kafka does not check a record's offset delta, so it stores a batch that the proxy rejects
    /// with [`BatchRejection::Append`]. Only a malformed batch has an offset delta that overflows
    /// when it is appended.
    ///
    /// [`DefaultRecordBatch`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L633-L642
    /// [`ProduceRequest.validateRecords`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/ProduceRequest.java#L243-L245
    /// [`KafkaApis.handleProduceRequest`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L637-L643
    fn error_code(&self) -> ErrorCode {
        match self {
            Self::Decode(nisshi_sans_io::Error::MessageMaxSizeExceeded(_)) => {
                ErrorCode::MessageTooLarge
            }
            Self::MultipleBatches(_)
            | Self::OffsetRange { .. }
            | Self::Decode(_)
            | Self::Append(_) => ErrorCode::InvalidRecord,
            Self::Encode(_) => ErrorCode::UnknownServerError,
        }
    }
}

/// One partition's batches after [`combine`]: the batches that the proxy forwards, and the
/// owners that it rejects.
#[derive(Debug)]
struct Combined {
    forwarded: Vec<deflated::Batch>,
    rejected: BTreeMap<Uuid, BatchRejection>,
}

/// Merges one partition's batches into one batch, and rejects each owner with a batch that does
/// not merge.
///
/// Kafka never merges two requests, so one owner's batch must not fail another owner's batch. The
/// proxy forwards one batch for the partition, because Kafka rejects a Produce request with more
/// than one batch for a partition ([`ProduceRequest.validateRecords`]). For the same reason, an
/// owner with more than one batch for the partition is rejected with INVALID_RECORD, as Kafka
/// answers it, and none of its batches is merged. A partition with batches of one owner only is
/// forwarded as sent, for the origin to check.
///
/// Two differences from Kafka are deliberate. The count does not look at the Produce version,
/// although Kafka applies the rule only from version 3, because the proxy decodes magic v2
/// batches only. A request that lists the same partition twice puts both entries' batches under
/// one owner, so the proxy rejects it with INVALID_RECORD, where Kafka keeps the last entry for
/// the partition ([`KafkaApis.handleProduceRequest`]). No known client repeats a partition.
///
/// [`ProduceRequest.validateRecords`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/ProduceRequest.java#L225-L246
/// [`KafkaApis.handleProduceRequest`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L626-L643
fn combine(batches: Vec<OwnedBatch>) -> Combined {
    debug!(len = batches.len());

    if batches
        .windows(2)
        .all(|pair| pair[0].owner == pair[1].owner)
    {
        return Combined {
            forwarded: batches.into_iter().map(|owned| owned.batch).collect(),
            rejected: BTreeMap::new(),
        };
    }

    let mut rejected = BTreeMap::new();

    // Kafka refuses a partition with more than one batch, so the owner of such a partition is
    // rejected before any of its batches is decoded.
    let mut counts = BTreeMap::<Uuid, usize>::new();
    for owned in &batches {
        *counts.entry(owned.owner).or_default() += 1;
    }

    for (owner, count) in counts {
        if count > 1 {
            _ = rejected.insert(owner, BatchRejection::MultipleBatches(count));
        }
    }

    let mut owners = Vec::<OwnerBatch>::new();

    for OwnedBatch { owner, batch } in batches {
        if rejected.contains_key(&owner) {
            continue;
        }

        match decode(&batch) {
            Ok(decoded) => owners.push(OwnerBatch {
                owner,
                as_sent: batch,
                decoded,
            }),
            Err(rejection) => _ = rejected.insert(owner, rejection),
        }
    }

    let forwarded = merge(owners, &mut rejected);

    Combined {
        forwarded,
        rejected,
    }
}

/// Returns `batch` decoded, after the check of its offset range that a Kafka broker makes.
fn decode(batch: &deflated::Batch) -> Result<inflated::Batch, BatchRejection> {
    if i32::try_from(batch.record_count).ok() != batch.last_offset_delta.checked_add(1) {
        return Err(BatchRejection::OffsetRange {
            last_offset_delta: batch.last_offset_delta,
            record_count: batch.record_count,
        });
    }

    // A clone of a batch shares its record bytes, so it costs no copy.
    inflated::Batch::try_from(batch.clone()).map_err(BatchRejection::Decode)
}

/// Returns the batches of `owners` merged into one batch, and adds each owner whose batch does
/// not merge to `rejected`. The batch of a single owner is forwarded as sent.
fn merge(
    owners: Vec<OwnerBatch>,
    rejected: &mut BTreeMap<Uuid, BatchRejection>,
) -> Vec<deflated::Batch> {
    let mut owners = owners.into_iter();

    let Some(first) = owners.next() else {
        return Vec::new();
    };

    let mut sink = first.decoded;
    let mut merged_owners = vec![first.owner];
    let first_as_sent = first.as_sent;

    for owner_batch in owners {
        // `append_rebased` leaves the sink unchanged when it fails, so the batches of the owners
        // merged before stay as they are.
        match sink.append_rebased(owner_batch.decoded) {
            Ok(()) => merged_owners.push(owner_batch.owner),
            Err(error) => {
                _ = rejected.insert(owner_batch.owner, BatchRejection::Append(error));
            }
        }
    }

    if merged_owners.len() == 1 {
        return vec![first_as_sent];
    }

    match deflated::Batch::try_from(sink) {
        Ok(merged) => vec![merged],
        Err(error) => {
            for owner in merged_owners {
                _ = rejected.insert(owner, BatchRejection::Encode(error.clone()));
            }

            Vec::new()
        }
    }
}

/// Returns the response for a rejected partition, with the values of Kafka's
/// [`PartitionResponse(Errors)`]. Kafka does not set an error message.
///
/// [`PartitionResponse(Errors)`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/ProduceResponse.java#L168-L170
fn rejected_partition_response(
    index: Partition,
    error_code: ErrorCode,
) -> PartitionProduceResponse {
    PartitionProduceResponse::default()
        .index(index)
        .error_code(error_code.into())
        .base_offset(-1)
        .log_append_time_ms(Some(-1))
        .log_start_offset(Some(-1))
        .record_errors(Some([].into()))
        .error_message(None)
        .current_leader(None)
}

#[derive(Clone)]
struct Ticket<S: Clone + Debug> {
    id: Uuid,
    batch: BatchProduceService<S>,
}

impl<S> Ticket<S>
where
    S: Clone + Debug,
{
    fn new(batch: BatchProduceService<S>) -> Self {
        Self {
            id: Uuid::now_v7(),
            batch,
        }
    }
}

impl<S> AsRef<Uuid> for Ticket<S>
where
    S: Clone + Debug,
{
    fn as_ref(&self) -> &Uuid {
        &self.id
    }
}

impl<S> Future for Ticket<S>
where
    S: Clone + Debug,
{
    type Output = Result<ProduceResponse, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        let mut responses = lock(&self.batch.responses);
        debug!(?responses);

        match responses
            .remove(&self.id)
            .inspect(|response| debug!(id = ?self.id, ?response))
        {
            Some(BatchResponse::Response(response)) => Poll::Ready(Ok(response)),
            Some(_) => Poll::Pending,
            None => {
                _ = responses.insert(self.id, BatchResponse::Waker(cx.waker().clone()));
                Poll::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{assert_matches, fs::File, thread};

    use bytes::Bytes;
    use nisshi_sans_io::{
        BatchAttribute, Compression,
        produce_response::{LeaderIdAndEpoch, PartitionProduceResponse, TopicProduceResponse},
        record::Record,
    };
    use tokio::{task::yield_now, time::advance};
    use tracing::{Instrument, Level, debug, span, subscriber::DefaultGuard};
    use tracing_subscriber::{EnvFilter, fmt::format::FmtSpan};

    use super::*;

    const POISON_CHILD: &str = "NISSHI_PROXY_POISON_CHILD";

    #[test]
    fn poisoned_lock_aborts_the_process() {
        if std::env::var_os(POISON_CHILD).is_some() {
            let mutex = Arc::new(Mutex::new(()));

            let poisoner = mutex.clone();
            _ = thread::spawn(move || {
                let _guard = poisoner.lock();
                panic!("poison the lock");
            })
            .join();

            drop(lock(&mutex));
            return;
        }

        let status = std::process::Command::new(std::env::current_exe().expect("current exe"))
            .args([
                "--exact",
                "produce::tests::poisoned_lock_aborts_the_process",
            ])
            .env(POISON_CHILD, "1")
            .status()
            .expect("child");

        assert!(!status.success(), "{status:?}");

        #[cfg(unix)]
        assert_eq!(
            Some(6),
            std::os::unix::process::ExitStatusExt::signal(&status)
        );
    }

    #[derive(Clone, Default, Debug)]
    struct MockProduceService {
        reqs: Arc<Mutex<Vec<ProduceRequest>>>,
    }

    impl Service<RequestInput<ProduceRequest>> for MockProduceService {
        type Output = ProduceResponse;
        type Error = Error;

        async fn serve(
            &self,
            req: RequestInput<ProduceRequest>,
        ) -> Result<Self::Output, Self::Error> {
            debug!(?req);

            let produce_response = ProduceResponse::default()
                .responses(req.request.topic_data.as_ref().map(|topic_data| {
                    topic_data
                        .iter()
                        .map(|topic_produce_data| {
                            TopicProduceResponse::default()
                                .name(topic_produce_data.name.clone())
                                .partition_responses(
                                    topic_produce_data.partition_data.as_ref().map(
                                        |partition_produce_data| {
                                            partition_produce_data
                                                .iter()
                                                .map(|partition_produce| {
                                                    PartitionProduceResponse::default()
                                                        .index(partition_produce.index)
                                                        .error_code(ErrorCode::None.into())
                                                        .base_offset(65456)
                                                        .log_append_time_ms(Some(0))
                                                        .log_start_offset(Some(0))
                                                        .record_errors(Some([].into()))
                                                        .error_message(Some("none".into()))
                                                        .current_leader(Some(
                                                            LeaderIdAndEpoch::default()
                                                                .leader_id(12321)
                                                                .leader_epoch(23432),
                                                        ))
                                                })
                                                .collect()
                                        },
                                    ),
                                )
                        })
                        .collect()
                }))
                .throttle_time_ms(Some(5_000))
                .node_endpoints(Some([].into()));

            let mut guard = self.reqs.lock().expect("poison");
            guard.push(req.request);

            Ok(produce_response)
        }
    }

    fn init_tracing() -> Result<DefaultGuard, Error> {
        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_ids(false)
                .with_span_events(FmtSpan::NONE)
                .with_env_filter(
                    EnvFilter::from_default_env()
                        .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
                )
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    fn record_data_batch(value: &'static [u8]) -> Result<deflated::Batch, Error> {
        inflated::Batch::builder()
            .base_timestamp(1_234_567_890 * 1_000)
            .max_timestamp(1_234_567_890 * 1_000)
            .record(Record::builder().value(Bytes::from_static(value).into()))
            .build()
            .and_then(deflated::Batch::try_from)
            .map_err(Into::into)
    }

    fn multi_record_batch(values: &[&'static [u8]]) -> Result<deflated::Batch, Error> {
        values
            .iter()
            .enumerate()
            .fold(
                inflated::Batch::builder()
                    .base_timestamp(1_234_567_890 * 1_000)
                    .max_timestamp(1_234_567_890 * 1_000)
                    .last_offset_delta(i32::try_from(values.len() - 1).expect("last offset delta")),
                |builder, (delta, value)| {
                    builder.record(
                        Record::builder()
                            .offset_delta(i32::try_from(delta).expect("offset delta"))
                            .value(Bytes::from_static(value).into()),
                    )
                },
            )
            .build()
            .and_then(deflated::Batch::try_from)
            .map_err(Into::into)
    }

    /// Returns `batches` with a different owner for each.
    fn owned_by_separate_requests(batches: Vec<deflated::Batch>) -> Vec<OwnedBatch> {
        batches
            .into_iter()
            .map(|batch| OwnedBatch {
                owner: Uuid::now_v7(),
                batch,
            })
            .collect()
    }

    /// Combines `batches`, each from a different request, and fails if `combine` rejects any.
    fn combine_accepted(batches: Vec<deflated::Batch>) -> Result<Vec<deflated::Batch>, Error> {
        let combined = combine(owned_by_separate_requests(batches));

        if combined.rejected.is_empty() {
            Ok(combined.forwarded)
        } else {
            Err(Error::Message(format!("rejected: {:?}", combined.rejected)))
        }
    }

    #[test]
    fn combine_rejects_the_owner_of_a_batch_that_does_not_decode() -> Result<(), Error> {
        let rejected_owner = Uuid::now_v7();

        let [first, second] =
            owned_by_separate_requests(vec![record_data_batch(b"a")?, record_data_batch(b"d")?])
                .try_into()
                .expect("two owned batches");

        let combined = combine(vec![
            first,
            OwnedBatch {
                owner: rejected_owner,
                batch: undecodable_batch()?,
            },
            second,
        ]);

        assert_eq!(
            vec![rejected_owner],
            combined.rejected.into_keys().collect::<Vec<_>>()
        );

        assert_eq!(1, combined.forwarded.len());
        assert_eq!(
            vec![
                Some(Bytes::from_static(b"a")),
                Some(Bytes::from_static(b"d"))
            ],
            Vec::<Record>::try_from(combined.forwarded[0].clone())?
                .into_iter()
                .map(|record| record.value)
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    #[test]
    fn combine_keeps_offset_deltas_contiguous() -> Result<(), Error> {
        let combined = combine_accepted(vec![
            multi_record_batch(&[b"a", b"b"])?,
            multi_record_batch(&[b"c", b"d", b"e"])?,
        ])?;

        assert_eq!(1, combined.len());
        assert_eq!(5, combined[0].record_count);
        assert_eq!(4, combined[0].last_offset_delta);

        let inflated = inflated::Batch::try_from(combined[0].clone())?;
        assert_eq!(
            vec![0, 1, 2, 3, 4],
            inflated
                .records
                .iter()
                .map(|record| record.offset_delta)
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    /// A record offset delta of the second batch overflows once that batch is appended to the
    /// first, so `combine` rejects the second batch's owner and forwards the first batch as sent.
    #[test]
    fn combine_rejects_the_owner_of_a_batch_whose_offset_delta_overflows() -> Result<(), Error> {
        let batch_with_maximum_offset_delta = inflated::Batch::builder()
            .record(
                Record::builder()
                    .offset_delta(i32::MAX)
                    .value(Bytes::from_static(b"b").into()),
            )
            .build()
            .and_then(deflated::Batch::try_from)?;

        let first = record_data_batch(b"a")?;

        let owned =
            owned_by_separate_requests(vec![first.clone(), batch_with_maximum_offset_delta]);
        let overflowing_owner = owned[1].owner;

        let combined = combine(owned);

        assert_eq!(vec![first], combined.forwarded);
        assert_eq!(1, combined.rejected.len());
        assert_matches!(
            combined.rejected.get(&overflowing_owner),
            Some(BatchRejection::Append(
                nisshi_sans_io::Error::OffsetDeltaOverflow(_)
            ))
        );

        Ok(())
    }

    /// An owner with two batches for the partition is rejected, as Kafka rejects its request, and
    /// neither of its batches joins the other owners' merged batch.
    #[test]
    fn combine_rejects_an_owner_with_two_batches_for_the_partition() -> Result<(), Error> {
        let rejected_owner = Uuid::now_v7();

        let [first, second] =
            owned_by_separate_requests(vec![record_data_batch(b"a")?, record_data_batch(b"d")?])
                .try_into()
                .expect("two owned batches");

        let combined = combine(vec![
            first,
            OwnedBatch {
                owner: rejected_owner,
                batch: record_data_batch(b"b")?,
            },
            OwnedBatch {
                owner: rejected_owner,
                batch: record_data_batch(b"c")?,
            },
            second,
        ]);

        assert_eq!(1, combined.rejected.len());
        assert_matches!(
            combined.rejected.get(&rejected_owner),
            Some(BatchRejection::MultipleBatches(2))
        );
        assert_eq!(
            ErrorCode::InvalidRecord,
            combined.rejected[&rejected_owner].error_code()
        );

        assert_eq!(1, combined.forwarded.len());
        assert_eq!(
            vec![
                Some(Bytes::from_static(b"a")),
                Some(Bytes::from_static(b"d"))
            ],
            Vec::<Record>::try_from(combined.forwarded[0].clone())?
                .into_iter()
                .map(|record| record.value)
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    /// The second batch's record is rebuilt with `offset_delta` 64, one byte
    /// longer as a varint than the 0 it was encoded with, so the combined
    /// batch must still decode record by record.
    #[test]
    fn combine_offset_delta_crossing_varint_width_decodes_by_value() -> Result<(), Error> {
        let combined = combine_accepted(vec![
            multi_record_batch(&[b"a".as_slice(); 64])?,
            multi_record_batch(&[b"b"])?,
        ])?;

        assert_eq!(1, combined.len());

        let records = Vec::<Record>::try_from(combined[0].clone())?;
        assert_eq!(65, records.len());
        assert_eq!(64, records[64].offset_delta);
        assert_eq!(Some(Bytes::from_static(b"b")), records[64].value);

        Ok(())
    }

    // The batches have different base timestamps, so `combine` must rebase the
    // second batch's timestamp deltas onto the first batch's base.
    #[test]
    fn combine_preserves_timestamps_across_different_base_timestamps() -> Result<(), Error> {
        let first = inflated::Batch::builder()
            .producer_id(1)
            .producer_epoch(0)
            .base_offset(0)
            .last_offset_delta(1)
            .base_sequence(0)
            .base_timestamp(1_000)
            .max_timestamp(1_005)
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"a").into())
                    .offset_delta(0)
                    .timestamp_delta(0),
            )
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"b").into())
                    .offset_delta(1)
                    .timestamp_delta(5),
            )
            .build()
            .and_then(deflated::Batch::try_from)?;

        let second = inflated::Batch::builder()
            .producer_id(1)
            .producer_epoch(0)
            .base_offset(2)
            .last_offset_delta(1)
            .base_sequence(2)
            .base_timestamp(1_010)
            .max_timestamp(1_013)
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"c").into())
                    .offset_delta(0)
                    .timestamp_delta(0),
            )
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"d").into())
                    .offset_delta(1)
                    .timestamp_delta(3),
            )
            .build()
            .and_then(deflated::Batch::try_from)?;

        let original_times: BTreeMap<&'static [u8], i64> = BTreeMap::from([
            (&b"a"[..], 1_000),
            (&b"b"[..], 1_005),
            (&b"c"[..], 1_010),
            (&b"d"[..], 1_013),
        ]);

        let combined = combine_accepted(vec![first, second])?;
        assert_eq!(1, combined.len());

        let combined = inflated::Batch::try_from(combined[0].clone())?;

        assert_eq!(4, combined.records.len());

        assert_eq!(1_000, combined.base_timestamp);
        assert_eq!(1_013, combined.max_timestamp);

        for record in &combined.records {
            let value = record.value.as_deref().expect("value");
            let expected = original_times[value];
            let actual = combined.base_timestamp + record.timestamp_delta;
            assert_eq!(
                expected, actual,
                "record {value:?} should keep its original absolute timestamp"
            );
        }

        Ok(())
    }

    // The second batch's base timestamp is earlier than the first's, so the
    // rebased deltas are negative and the first batch's maximum stays the
    // combined maximum.
    #[test]
    fn combine_preserves_timestamps_when_second_base_timestamp_is_earlier() -> Result<(), Error> {
        let first = inflated::Batch::builder()
            .producer_id(1)
            .producer_epoch(0)
            .base_offset(0)
            .last_offset_delta(1)
            .base_sequence(0)
            .base_timestamp(1_000)
            .max_timestamp(1_005)
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"a").into())
                    .offset_delta(0)
                    .timestamp_delta(0),
            )
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"b").into())
                    .offset_delta(1)
                    .timestamp_delta(5),
            )
            .build()
            .and_then(deflated::Batch::try_from)?;

        let second = inflated::Batch::builder()
            .producer_id(1)
            .producer_epoch(0)
            .base_offset(2)
            .last_offset_delta(1)
            .base_sequence(2)
            .base_timestamp(990)
            .max_timestamp(993)
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"c").into())
                    .offset_delta(0)
                    .timestamp_delta(0),
            )
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"d").into())
                    .offset_delta(1)
                    .timestamp_delta(3),
            )
            .build()
            .and_then(deflated::Batch::try_from)?;

        let original_times: BTreeMap<&'static [u8], i64> = BTreeMap::from([
            (&b"a"[..], 1_000),
            (&b"b"[..], 1_005),
            (&b"c"[..], 990),
            (&b"d"[..], 993),
        ]);

        let combined = combine_accepted(vec![first, second])?;
        assert_eq!(1, combined.len());

        let combined = inflated::Batch::try_from(combined[0].clone())?;

        assert_eq!(4, combined.records.len());

        assert_eq!(1_000, combined.base_timestamp);
        assert_eq!(1_005, combined.max_timestamp);

        assert_eq!(
            vec![0, 5, -10, -7],
            combined
                .records
                .iter()
                .map(|record| record.timestamp_delta)
                .collect::<Vec<_>>()
        );

        for record in &combined.records {
            let value = record.value.as_deref().expect("value");
            let expected = original_times[value];
            let actual = combined.base_timestamp + record.timestamp_delta;
            assert_eq!(
                expected, actual,
                "record {value:?} should keep its original absolute timestamp"
            );
        }

        Ok(())
    }

    fn produce_request(topic: &str, record_data: &'static [u8]) -> Result<ProduceRequest, Error> {
        record_data_batch(record_data).map(|batch| {
            ProduceRequest::default().topic_data(Some(
                [TopicProduceData::default()
                    .name(String::from(topic))
                    .partition_data(Some(
                        [PartitionProduceData::default()
                            .index(0)
                            .records(Some(Frame {
                                batches: vec![batch],
                            }))]
                        .into(),
                    ))]
                .into(),
            ))
        })
    }

    #[tokio::test(start_paused = true)]
    async fn batch_timeout_key_and_its_alias_hold_a_batch_for_the_same_time() -> Result<(), Error> {
        let _guard = init_tracing()?;

        for timeout_key in ["tansu.batch.timeout_ms", "nisshi.batch.timeout_ms"] {
            let mock = MockProduceService::default();

            let configuration = ResourceConfig::default();
            configuration.put("a", timeout_key, "5000")?;

            let bp = (BatchProduceLayer::new(configuration)).into_layer(mock.clone());

            let _batch = tokio::spawn({
                let bp = bp.clone();

                async move {
                    bp.serve(produce_request("a", b"foo").map(Into::into)?)
                        .await
                }
            });

            yield_now().await;
            advance(Duration::from_secs(1)).await;
            yield_now().await;

            let len = mock.reqs.lock().map(|guard| guard.len()).expect("poison");
            assert_eq!(0, len, "{timeout_key} should hold the batch");

            // The default batch timeout is 10 seconds, so a request sent after 6 seconds
            // shows that the 5 second timeout applies.
            advance(Duration::from_secs(5)).await;
            yield_now().await;

            let len = mock.reqs.lock().map(|guard| guard.len()).expect("poison");
            assert_eq!(1, len, "{timeout_key} should release the batch");
        }

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn single_request_in_batch() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let mock = MockProduceService::default();

        let configuration = ResourceConfig::default();

        let topic_a = "a";
        configuration.put(topic_a, "tansu.batch", "true")?;
        configuration.put(topic_a, "tansu.batch.timeout_ms", "5000")?;

        let bp = (BatchProduceLayer::new(configuration)).into_layer(mock.clone());

        let batch_a = {
            let bp = bp.clone();

            tokio::spawn(async move {
                let span = span!(Level::DEBUG, "batch", topic_a);

                async move {
                    bp.serve(produce_request(topic_a, b"foo").map(Into::into)?)
                        .await
                }
                .instrument(span)
                .await
            })
        };

        yield_now().await;
        advance(Duration::from_secs(1)).await;
        yield_now().await;

        let len = bp.requests.lock().map(|guard| guard.len()).expect("poison");
        assert_eq!(1, len);

        let len = mock.reqs.lock().map(|guard| guard.len()).expect("poison");
        assert_eq!(0, len);

        yield_now().await;
        advance(Duration::from_secs(5)).await;
        yield_now().await;

        let requests = mock.reqs.lock().map(|guard| guard.clone()).expect("poison");
        assert_eq!(
            vec![
                ProduceRequest::default()
                    .transactional_id(None)
                    .acks(1)
                    .timeout_ms(5000)
                    .topic_data(Some(
                        [TopicProduceData::default()
                            .name("a".into())
                            .partition_data(Some(
                                [PartitionProduceData::default()
                                    .index(0)
                                    .records(Some(Frame {
                                        batches: [deflated::Batch {
                                            base_offset: 0,
                                            batch_length: 59,
                                            partition_leader_epoch: -1,
                                            magic: 2,
                                            crc: 1920191007,
                                            attributes: 0,
                                            last_offset_delta: 0,
                                            base_timestamp: 1234567890000,
                                            max_timestamp: 1234567890000,
                                            producer_id: -1,
                                            producer_epoch: 0,
                                            base_sequence: 0,
                                            record_count: 1,
                                            record_data: Bytes::from_static(
                                                b"\x12\0\0\0\x01\x06foo\0"
                                            )
                                        }]
                                        .into()
                                    }))]
                                .into()
                            ))]
                        .into()
                    ))
            ],
            requests
        );

        let response_a = batch_a
            .await
            .expect("a_await")
            .inspect(|produce_response| debug!(?produce_response))?;

        assert_eq!(
            ProduceResponse::default()
                .throttle_time_ms(Some(5_000))
                .node_endpoints(Some([].into()))
                .responses(Some(
                    [TopicProduceResponse::default()
                        .name("a".into())
                        .partition_responses(Some(
                            [PartitionProduceResponse::default()
                                .index(0)
                                .error_code(0)
                                .base_offset(65456)
                                .log_append_time_ms(Some(0))
                                .log_start_offset(Some(0))
                                .record_errors(Some([].into()))
                                .error_message(Some("none".into()))
                                .current_leader(Some(
                                    LeaderIdAndEpoch::default()
                                        .leader_id(12321)
                                        .leader_epoch(23432)
                                ))]
                            .into()
                        ))]
                    .into()
                )),
            response_a
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn two_requests_in_batch() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let mock = MockProduceService::default();

        let configuration = ResourceConfig::default();

        let topic_a = "a";
        let topic_b = "b";
        configuration.put(topic_a, "tansu.batch", "true")?;
        configuration.put(topic_a, "tansu.batch.timeout_ms", "5000")?;

        let bp = (BatchProduceLayer::new(configuration)).into_layer(mock.clone());

        let batch_a = {
            let bp = bp.clone();

            tokio::spawn(async move {
                let span = span!(Level::DEBUG, "batch", topic_a);

                async move {
                    bp.serve(produce_request(topic_a, b"foo").map(Into::into)?)
                        .await
                }
                .instrument(span)
                .await
            })
        };

        advance(Duration::from_secs(1)).await;
        yield_now().await;

        let len = bp.requests.lock().map(|guard| guard.len()).expect("poison");
        assert_eq!(1, len);

        let len = mock.reqs.lock().map(|guard| guard.len()).expect("poison");
        assert_eq!(0, len);

        let batch_b = {
            let bp = bp.clone();

            tokio::spawn(async move {
                let span = span!(Level::DEBUG, "batch", topic_b);

                async move {
                    bp.serve(produce_request(topic_b, b"bar").map(Into::into)?)
                        .await
                }
                .instrument(span)
                .await
            })
        };

        advance(Duration::from_secs(1)).await;
        yield_now().await;

        let len = bp.requests.lock().map(|guard| guard.len()).expect("poison");
        assert_eq!(2, len);

        let len = mock.reqs.lock().map(|guard| guard.len()).expect("poison");
        assert_eq!(0, len);

        advance(Duration::from_secs(5)).await;
        yield_now().await;

        let requests = mock.reqs.lock().map(|guard| guard.clone()).expect("poison");
        assert_eq!(
            vec![
                ProduceRequest::default()
                    .transactional_id(None)
                    .acks(1)
                    .timeout_ms(5000)
                    .topic_data(Some(
                        [
                            TopicProduceData::default()
                                .name("a".into())
                                .partition_data(Some(
                                    [PartitionProduceData::default().index(0).records(Some(
                                        Frame {
                                            batches: [deflated::Batch {
                                                base_offset: 0,
                                                batch_length: 59,
                                                partition_leader_epoch: -1,
                                                magic: 2,
                                                crc: 1920191007,
                                                attributes: 0,
                                                last_offset_delta: 0,
                                                base_timestamp: 1234567890000,
                                                max_timestamp: 1234567890000,
                                                producer_id: -1,
                                                producer_epoch: 0,
                                                base_sequence: 0,
                                                record_count: 1,
                                                record_data: Bytes::from_static(
                                                    b"\x12\0\0\0\x01\x06foo\0"
                                                )
                                            }]
                                            .into()
                                        }
                                    ))]
                                    .into()
                                )),
                            TopicProduceData::default()
                                .name("b".into())
                                .partition_data(Some(
                                    [PartitionProduceData::default().index(0).records(Some(
                                        Frame {
                                            batches: [deflated::Batch {
                                                base_offset: 0,
                                                batch_length: 59,
                                                partition_leader_epoch: -1,
                                                magic: 2,
                                                crc: 524875948,
                                                attributes: 0,
                                                last_offset_delta: 0,
                                                base_timestamp: 1234567890000,
                                                max_timestamp: 1234567890000,
                                                producer_id: -1,
                                                producer_epoch: 0,
                                                base_sequence: 0,
                                                record_count: 1,
                                                record_data: Bytes::from_static(
                                                    b"\x12\0\0\0\x01\x06bar\0"
                                                )
                                            }]
                                            .into()
                                        }
                                    ))]
                                    .into()
                                ))
                        ]
                        .into()
                    ))
            ],
            requests
        );

        let response_a = batch_a
            .await?
            .inspect(|produce_response| debug!(?produce_response))?;

        assert_eq!(
            ProduceResponse::default()
                .throttle_time_ms(Some(5_000))
                .node_endpoints(Some([].into()))
                .responses(Some(
                    [TopicProduceResponse::default()
                        .name("a".into())
                        .partition_responses(Some(
                            [PartitionProduceResponse::default()
                                .index(0)
                                .error_code(0)
                                .base_offset(65456)
                                .log_append_time_ms(Some(0))
                                .log_start_offset(Some(0))
                                .record_errors(Some([].into()))
                                .error_message(Some("none".into()))
                                .current_leader(Some(
                                    LeaderIdAndEpoch::default()
                                        .leader_id(12321)
                                        .leader_epoch(23432)
                                ))]
                            .into()
                        ))]
                    .into()
                )),
            response_a
        );

        let response_b = batch_b
            .await?
            .inspect(|produce_response| debug!(?produce_response))?;

        assert_eq!(
            ProduceResponse::default()
                .throttle_time_ms(Some(5_000))
                .node_endpoints(Some([].into()))
                .responses(Some(
                    [TopicProduceResponse::default()
                        .name("b".into())
                        .partition_responses(Some(
                            [PartitionProduceResponse::default()
                                .index(0)
                                .error_code(0)
                                .base_offset(65456)
                                .log_append_time_ms(Some(0))
                                .log_start_offset(Some(0))
                                .record_errors(Some([].into()))
                                .error_message(Some("none".into()))
                                .current_leader(Some(
                                    LeaderIdAndEpoch::default()
                                        .leader_id(12321)
                                        .leader_epoch(23432)
                                ))]
                            .into()
                        ))]
                    .into()
                )),
            response_b
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn two_requests_into_single_topition() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let produce_service = MockProduceService::default();

        let configuration = ResourceConfig::default();

        let topic_a = "a";
        configuration.put(topic_a, "tansu.batch", "true")?;
        configuration.put(topic_a, "tansu.batch.timeout_ms", "5000")?;

        let bp = (BatchProduceLayer::new(configuration)).into_layer(produce_service.clone());

        let batch_a = {
            let bp = bp.clone();

            tokio::spawn(async move {
                let span = span!(Level::DEBUG, "batch", topic_a);

                async move {
                    bp.serve(produce_request(topic_a, b"foo").map(Into::into)?)
                        .await
                }
                .instrument(span)
                .await
            })
        };

        advance(Duration::from_secs(1)).await;
        yield_now().await;

        let len = bp.requests.lock().map(|guard| guard.len()).expect("poison");
        assert_eq!(1, len);

        let len = produce_service
            .reqs
            .lock()
            .map(|guard| guard.len())
            .expect("poison");
        assert_eq!(0, len);

        let batch_b = {
            let bp = bp.clone();

            tokio::spawn(async move {
                let span = span!(Level::DEBUG, "batch", topic_a);

                async move {
                    bp.serve(produce_request(topic_a, b"bar").map(Into::into)?)
                        .await
                }
                .instrument(span)
                .await
            })
        };

        advance(Duration::from_secs(1)).await;
        yield_now().await;

        let len = bp.requests.lock().map(|guard| guard.len()).expect("poison");
        assert_eq!(2, len);

        let len = produce_service
            .reqs
            .lock()
            .map(|guard| guard.len())
            .expect("poison");
        assert_eq!(0, len);

        advance(Duration::from_secs(5)).await;
        yield_now().await;

        let requests = produce_service
            .reqs
            .lock()
            .map(|guard| guard.clone())
            .expect("poison");

        assert_eq!(
            vec![
                ProduceRequest::default()
                    .transactional_id(None)
                    .acks(1)
                    .timeout_ms(5000)
                    .topic_data(Some(
                        [TopicProduceData::default()
                            .name("a".into())
                            .partition_data(Some(
                                [PartitionProduceData::default()
                                    .index(0)
                                    .records(Some(Frame {
                                        batches: [deflated::Batch {
                                            base_offset: 0,
                                            batch_length: 69,
                                            partition_leader_epoch: -1,
                                            magic: 2,
                                            crc: 2530424301,
                                            attributes: 0,
                                            last_offset_delta: 1,
                                            base_timestamp: 1234567890000,
                                            max_timestamp: 1234567890000,
                                            producer_id: -1,
                                            producer_epoch: 0,
                                            base_sequence: 0,
                                            record_count: 2,
                                            record_data: Bytes::from_static(
                                                b"\x12\0\0\0\x01\x06foo\0\x12\0\0\x02\x01\x06bar\0"
                                            )
                                        }]
                                        .into()
                                    }))]
                                .into()
                            ))]
                        .into()
                    ))
            ],
            requests
        );

        let response_a = batch_a
            .await?
            .inspect(|produce_response| debug!(?produce_response))?;

        assert_eq!(
            ProduceResponse::default()
                .throttle_time_ms(Some(5_000))
                .node_endpoints(Some([].into()))
                .responses(Some(
                    [TopicProduceResponse::default()
                        .name("a".into())
                        .partition_responses(Some(
                            [PartitionProduceResponse::default()
                                .index(0)
                                .error_code(0)
                                .base_offset(65456)
                                .log_append_time_ms(Some(0))
                                .log_start_offset(Some(0))
                                .record_errors(Some([].into()))
                                .error_message(Some("none".into()))
                                .current_leader(Some(
                                    LeaderIdAndEpoch::default()
                                        .leader_id(12321)
                                        .leader_epoch(23432)
                                ))]
                            .into()
                        ))]
                    .into()
                )),
            response_a
        );

        let response_b = batch_b
            .await?
            .inspect(|produce_response| debug!(?produce_response))?;

        assert_eq!(
            ProduceResponse::default()
                .throttle_time_ms(Some(5_000))
                .node_endpoints(Some([].into()))
                .responses(Some(
                    [TopicProduceResponse::default()
                        .name("a".into())
                        .partition_responses(Some(
                            [PartitionProduceResponse::default()
                                .index(0)
                                .error_code(0)
                                .base_offset(65456)
                                .log_append_time_ms(Some(0))
                                .log_start_offset(Some(0))
                                .record_errors(Some([].into()))
                                .error_message(Some("none".into()))
                                .current_leader(Some(
                                    LeaderIdAndEpoch::default()
                                        .leader_id(12321)
                                        .leader_epoch(23432)
                                ))]
                            .into()
                        ))]
                    .into()
                )),
            response_b
        );

        Ok(())
    }

    fn produce_request_to_partitions(
        topic: &str,
        batches: Vec<(i32, deflated::Batch)>,
    ) -> ProduceRequest {
        ProduceRequest::default().topic_data(Some(
            [TopicProduceData::default()
                .name(String::from(topic))
                .partition_data(Some(
                    batches
                        .into_iter()
                        .map(|(index, batch)| {
                            PartitionProduceData::default()
                                .index(index)
                                .records(Some(Frame {
                                    batches: vec![batch],
                                }))
                        })
                        .collect(),
                ))]
            .into(),
        ))
    }

    /// Returns a batch with a valid header whose records do not decode.
    fn undecodable_batch() -> Result<deflated::Batch, Error> {
        record_data_batch(b"bar").map(|batch| deflated::Batch {
            record_data: batch.record_data.slice(..3),
            ..batch
        })
    }

    fn batching_service(
        origin: MockProduceService,
    ) -> Result<BatchProduceService<MockProduceService>, Error> {
        let configuration = ResourceConfig::default();
        configuration.put("a", "tansu.batch", "true")?;
        configuration.put("a", "tansu.batch.timeout_ms", "5000")?;
        Ok(BatchProduceLayer::new(configuration).into_layer(origin))
    }

    fn spawn_produce(
        service: &BatchProduceService<MockProduceService>,
        request: ProduceRequest,
    ) -> tokio::task::JoinHandle<Result<ProduceResponse, Error>> {
        let service = service.clone();
        tokio::spawn(async move { service.serve(request.into()).await })
    }

    /// Queues `requests` in order, one second apart, then lets the batch timeout expire.
    async fn produce_in_one_batch(
        service: &BatchProduceService<MockProduceService>,
        requests: Vec<ProduceRequest>,
    ) -> Result<Vec<ProduceResponse>, Error> {
        let mut handles = vec![];
        for request in requests {
            handles.push(spawn_produce(service, request));
            advance(Duration::from_secs(1)).await;
            yield_now().await;
        }

        advance(Duration::from_secs(5)).await;
        yield_now().await;

        let mut responses = vec![];
        for handle in handles {
            responses.push(handle.await??);
        }
        Ok(responses)
    }

    /// Returns the partition rows of `response`, keyed by topic and partition.
    fn partition_responses(
        response: &ProduceResponse,
    ) -> BTreeMap<(String, i32), PartitionProduceResponse> {
        response
            .responses
            .iter()
            .flatten()
            .flat_map(|topic| {
                topic
                    .partition_responses
                    .iter()
                    .flatten()
                    .map(|partition| ((topic.name.clone(), partition.index), partition.clone()))
            })
            .collect()
    }

    /// The row that Kafka's `PartitionResponse(Errors)` writes for a partition that failed.
    fn rejected_partition(index: i32, error_code: ErrorCode) -> PartitionProduceResponse {
        PartitionProduceResponse::default()
            .index(index)
            .error_code(error_code.into())
            .base_offset(-1)
            .log_append_time_ms(Some(-1))
            .log_start_offset(Some(-1))
            .record_errors(Some([].into()))
            .error_message(None)
            .current_leader(None)
    }

    /// Returns the batches that the origin received for each topic and partition.
    fn forwarded_batches(
        origin: &MockProduceService,
    ) -> Vec<BTreeMap<(String, i32), Vec<deflated::Batch>>> {
        origin
            .reqs
            .lock()
            .expect("poison")
            .iter()
            .map(|request| {
                request
                    .topic_data
                    .iter()
                    .flatten()
                    .flat_map(|topic| {
                        topic.partition_data.iter().flatten().map(|partition| {
                            (
                                (topic.name.clone(), partition.index),
                                partition
                                    .records
                                    .as_ref()
                                    .map(|frame| frame.batches.clone())
                                    .unwrap_or_default(),
                            )
                        })
                    })
                    .collect()
            })
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn undecodable_batch_fails_only_its_own_request() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let origin = MockProduceService::default();
        let service = batching_service(origin.clone())?;

        let good = record_data_batch(b"foo")?;

        let responses = produce_in_one_batch(
            &service,
            vec![
                produce_request_to_partitions("a", vec![(0, good.clone())]),
                produce_request_to_partitions("a", vec![(0, undecodable_batch()?)]),
            ],
        )
        .await?;

        assert_eq!(
            vec![BTreeMap::from([((String::from("a"), 0), vec![good])])],
            forwarded_batches(&origin)
        );

        let good_rows = partition_responses(&responses[0]);
        assert_eq!(1, good_rows.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(good_rows[&("a".into(), 0)].error_code)?
        );
        assert_eq!(65456, good_rows[&("a".into(), 0)].base_offset);

        assert_eq!(
            BTreeMap::from([(
                (String::from("a"), 0),
                rejected_partition(0, ErrorCode::InvalidRecord)
            )]),
            partition_responses(&responses[1])
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn undecodable_batch_leaves_other_partitions_forwarded() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let origin = MockProduceService::default();
        let service = batching_service(origin.clone())?;

        let good = record_data_batch(b"foo")?;
        let other_partition = record_data_batch(b"baz")?;

        let responses = produce_in_one_batch(
            &service,
            vec![
                produce_request_to_partitions("a", vec![(0, good.clone())]),
                produce_request_to_partitions(
                    "a",
                    vec![(0, undecodable_batch()?), (1, other_partition.clone())],
                ),
            ],
        )
        .await?;

        assert_eq!(
            vec![BTreeMap::from([
                ((String::from("a"), 0), vec![good]),
                ((String::from("a"), 1), vec![other_partition]),
            ])],
            forwarded_batches(&origin)
        );

        let rows = partition_responses(&responses[1]);
        assert_eq!(2, rows.len());
        assert_eq!(
            rejected_partition(0, ErrorCode::InvalidRecord),
            rows[&("a".into(), 0)]
        );
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(rows[&("a".into(), 1)].error_code)?
        );
        assert_eq!(65456, rows[&("a".into(), 1)].base_offset);

        assert_eq!(
            vec![(String::from("a"), 0)],
            partition_responses(&responses[0])
                .into_keys()
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn batch_over_the_decoded_size_limit_fails_only_its_own_request() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let origin = MockProduceService::default();
        let service = batching_service(origin.clone())?;

        let good = record_data_batch(b"foo")?;

        // The record count alone puts the decoded batch over the limit, so it fails before any
        // decompression.
        let too_large = record_data_batch(b"bar").map(|batch| deflated::Batch {
            attributes: BatchAttribute::default()
                .compression(Compression::Zstd)
                .into(),
            last_offset_delta: 99_999_999,
            record_count: 100_000_000,
            ..batch
        })?;

        let responses = produce_in_one_batch(
            &service,
            vec![
                produce_request_to_partitions("a", vec![(0, good.clone())]),
                produce_request_to_partitions("a", vec![(0, too_large)]),
            ],
        )
        .await?;

        assert_eq!(
            vec![BTreeMap::from([((String::from("a"), 0), vec![good])])],
            forwarded_batches(&origin)
        );

        assert_eq!(
            BTreeMap::from([(
                (String::from("a"), 0),
                rejected_partition(0, ErrorCode::MessageTooLarge)
            )]),
            partition_responses(&responses[1])
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn batch_whose_offset_delta_overflows_fails_only_its_own_request() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let origin = MockProduceService::default();
        let service = batching_service(origin.clone())?;

        let good = record_data_batch(b"foo")?;

        // The record's offset delta overflows an `i32` when the batch is appended to another.
        let overflowing = inflated::Batch::builder()
            .record(
                Record::builder()
                    .offset_delta(i32::MAX)
                    .value(Bytes::from_static(b"bar").into()),
            )
            .build()
            .and_then(deflated::Batch::try_from)?;

        let responses = produce_in_one_batch(
            &service,
            vec![
                produce_request_to_partitions("a", vec![(0, good.clone())]),
                produce_request_to_partitions("a", vec![(0, overflowing)]),
            ],
        )
        .await?;

        assert_eq!(
            vec![BTreeMap::from([((String::from("a"), 0), vec![good])])],
            forwarded_batches(&origin)
        );

        assert_eq!(
            BTreeMap::from([(
                (String::from("a"), 0),
                rejected_partition(0, ErrorCode::InvalidRecord)
            )]),
            partition_responses(&responses[1])
        );

        Ok(())
    }

    /// Kafka answers INVALID_RECORD for a partition with two batches in one request, and handles
    /// the request's other partitions as usual. The other request's batch for the partition is
    /// forwarded as sent, merged with neither batch.
    #[tokio::test(start_paused = true)]
    async fn two_batches_for_one_partition_fail_only_that_partition_of_their_request()
    -> Result<(), Error> {
        let _guard = init_tracing()?;

        let origin = MockProduceService::default();
        let service = batching_service(origin.clone())?;

        let good = record_data_batch(b"foo")?;
        let other_partition = record_data_batch(b"qux")?;

        let two_batches = ProduceRequest::default().topic_data(Some(
            [TopicProduceData::default()
                .name(String::from("a"))
                .partition_data(Some(
                    [
                        PartitionProduceData::default()
                            .index(0)
                            .records(Some(Frame {
                                batches: vec![
                                    record_data_batch(b"bar")?,
                                    record_data_batch(b"baz")?,
                                ],
                            })),
                        PartitionProduceData::default()
                            .index(1)
                            .records(Some(Frame {
                                batches: vec![other_partition.clone()],
                            })),
                    ]
                    .into(),
                ))]
            .into(),
        ));

        let responses = produce_in_one_batch(
            &service,
            vec![
                produce_request_to_partitions("a", vec![(0, good.clone())]),
                two_batches,
            ],
        )
        .await?;

        assert_eq!(
            vec![BTreeMap::from([
                ((String::from("a"), 0), vec![good]),
                ((String::from("a"), 1), vec![other_partition]),
            ])],
            forwarded_batches(&origin)
        );

        let two_batches_responses = partition_responses(&responses[1]);

        assert_eq!(
            Some(&rejected_partition(0, ErrorCode::InvalidRecord)),
            two_batches_responses.get(&(String::from("a"), 0))
        );

        assert_eq!(
            Some(i16::from(ErrorCode::None)),
            two_batches_responses
                .get(&(String::from("a"), 1))
                .map(|partition| partition.error_code)
        );

        assert_eq!(
            Some(i16::from(ErrorCode::None)),
            partition_responses(&responses[0])
                .get(&(String::from("a"), 0))
                .map(|partition| partition.error_code)
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn batch_whose_offset_range_does_not_match_its_records_fails_only_its_own_request()
    -> Result<(), Error> {
        let _guard = init_tracing()?;

        let origin = MockProduceService::default();
        let service = batching_service(origin.clone())?;

        let good = record_data_batch(b"foo")?;

        let inconsistent = record_data_batch(b"bar").map(|batch| deflated::Batch {
            last_offset_delta: 1_000,
            ..batch
        })?;

        let responses = produce_in_one_batch(
            &service,
            vec![
                produce_request_to_partitions("a", vec![(0, good.clone())]),
                produce_request_to_partitions("a", vec![(0, inconsistent)]),
            ],
        )
        .await?;

        assert_eq!(
            vec![BTreeMap::from([((String::from("a"), 0), vec![good])])],
            forwarded_batches(&origin)
        );

        assert_eq!(
            BTreeMap::from([(
                (String::from("a"), 0),
                rejected_partition(0, ErrorCode::InvalidRecord)
            )]),
            partition_responses(&responses[1])
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn lone_undecodable_batch_is_forwarded_for_the_origin_to_answer() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let origin = MockProduceService::default();
        let service = batching_service(origin.clone())?;

        let undecodable = undecodable_batch()?;

        let responses = produce_in_one_batch(
            &service,
            vec![produce_request_to_partitions(
                "a",
                vec![(0, undecodable.clone())],
            )],
        )
        .await?;

        assert_eq!(
            vec![BTreeMap::from([(
                (String::from("a"), 0),
                vec![undecodable]
            )])],
            forwarded_batches(&origin)
        );

        assert_eq!(
            65456,
            partition_responses(&responses[0])[&("a".into(), 0)].base_offset
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn every_batch_rejected_answers_without_calling_the_origin() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let origin = MockProduceService::default();
        let service = batching_service(origin.clone())?;

        let responses = produce_in_one_batch(
            &service,
            vec![
                produce_request_to_partitions("a", vec![(0, undecodable_batch()?)]),
                produce_request_to_partitions("a", vec![(0, undecodable_batch()?)]),
            ],
        )
        .await?;

        assert!(forwarded_batches(&origin).is_empty());

        for response in responses {
            assert_eq!(
                BTreeMap::from([(
                    (String::from("a"), 0),
                    rejected_partition(0, ErrorCode::InvalidRecord)
                )]),
                partition_responses(&response)
            );
        }

        Ok(())
    }

    fn batch_request(acks: Ack) -> BatchRequest {
        BatchRequest {
            id: Uuid::now_v7(),
            request: ProduceRequest::default().acks(acks.into()),
        }
    }

    #[test]
    fn batch_acks_is_at_least_leader() {
        assert_eq!(Ack::Leader, batch_acks(&[batch_request(Ack::None)]));
        assert_eq!(
            Ack::Leader,
            batch_acks(&[batch_request(Ack::None), batch_request(Ack::Leader)])
        );
    }

    #[test]
    fn batch_acks_is_the_strictest_in_the_batch() {
        assert_eq!(
            Ack::FullIsr,
            batch_acks(&[
                batch_request(Ack::None),
                batch_request(Ack::FullIsr),
                batch_request(Ack::Leader),
            ])
        );
    }

    /// A one record batch with the given producer header.
    fn producer_batch(
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        attributes: BatchAttribute,
        value: &'static [u8],
    ) -> Result<deflated::Batch, Error> {
        inflated::Batch::builder()
            .base_timestamp(1_234_567_890 * 1_000)
            .max_timestamp(1_234_567_890 * 1_000)
            .attributes(attributes.into())
            .producer_id(producer_id)
            .producer_epoch(producer_epoch)
            .base_sequence(base_sequence)
            .record(Record::builder().value(Bytes::from_static(value).into()))
            .build()
            .and_then(deflated::Batch::try_from)
            .map_err(Into::into)
    }

    #[test]
    fn mergeable_only_for_batches_without_a_producer_header() -> Result<(), Error> {
        let request = |batch| produce_request_to_partitions("a", vec![(0, batch)]);

        assert!(mergeable(&request(record_data_batch(b"plain")?)));

        assert!(!mergeable(&request(producer_batch(
            7,
            0,
            0,
            BatchAttribute::default(),
            b"idempotent"
        )?)));

        assert!(!mergeable(&request(producer_batch(
            7,
            0,
            0,
            BatchAttribute::default().transaction(true),
            b"transactional"
        )?)));

        assert!(!mergeable(&request(producer_batch(
            -1,
            -1,
            -1,
            BatchAttribute::default().transaction(true),
            b"transaction bit only"
        )?)));

        assert!(!mergeable(&request(producer_batch(
            -1,
            -1,
            -1,
            BatchAttribute::default().control(true),
            b"control"
        )?)));

        assert!(!mergeable(&request(producer_batch(
            -1,
            -1,
            -1,
            BatchAttribute::default().timestamp(TimestampType::LogAppendTime),
            b"log append time"
        )?)));

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn idempotent_producers_on_one_partition_keep_their_own_headers() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let origin = MockProduceService::default();
        let service = batching_service(origin.clone())?;

        let first = producer_batch(7, 0, 0, BatchAttribute::default(), b"first")?;
        let second = producer_batch(9, 3, 42, BatchAttribute::default(), b"second")?;

        let started = tokio::time::Instant::now();

        let handles = [&first, &second].map(|batch| {
            spawn_produce(
                &service,
                produce_request_to_partitions("a", vec![(0, batch.clone())]),
            )
        });

        let mut responses = vec![];
        for handle in handles {
            responses.push(handle.await??);
        }

        // Neither request waits for a batch it cannot join.
        assert_eq!(Duration::ZERO, started.elapsed());

        // Each producer is answered by its own request to the origin, with its own header.
        let mut forwarded = forwarded_batches(&origin)
            .into_iter()
            .map(|mut request| request.remove(&(String::from("a"), 0)).unwrap_or_default())
            .collect::<Vec<_>>();
        forwarded.sort_by_key(|batches| batches.first().map(|batch| batch.producer_id));

        assert_eq!(vec![vec![first], vec![second]], forwarded);

        for response in responses {
            assert_eq!(
                ErrorCode::None,
                ErrorCode::try_from(
                    partition_responses(&response)[&(String::from("a"), 0)].error_code
                )?
            );
        }

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn transactional_producer_does_not_pull_other_clients_records_into_its_transaction()
    -> Result<(), Error> {
        let _guard = init_tracing()?;

        let origin = MockProduceService::default();
        let service = batching_service(origin.clone())?;

        let transactional = producer_batch(
            7,
            0,
            0,
            BatchAttribute::default().transaction(true),
            b"transactional",
        )?;
        let plain = record_data_batch(b"plain")?;

        // The origin must receive this request whole, so it carries a transactional id and acks
        // that a rebuilt request would lose.
        let transactional_request =
            produce_request_to_partitions("a", vec![(0, transactional.clone())])
                .transactional_id(Some(String::from("transaction")))
                .acks(Ack::FullIsr.into());

        let responses = produce_in_one_batch(
            &service,
            vec![
                transactional_request.clone(),
                produce_request_to_partitions("a", vec![(0, plain.clone())]),
            ],
        )
        .await?;

        assert_eq!(2, responses.len());

        assert!(
            origin
                .reqs
                .lock()
                .expect("poison")
                .contains(&transactional_request)
        );

        let mut forwarded = forwarded_batches(&origin)
            .into_iter()
            .map(|mut request| request.remove(&(String::from("a"), 0)).unwrap_or_default())
            .collect::<Vec<_>>();
        forwarded.sort_by_key(|batches| batches.first().map(|batch| batch.producer_id));

        assert_eq!(vec![vec![plain], vec![transactional]], forwarded);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn request_with_one_idempotent_partition_bypasses_whole() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let origin = MockProduceService::default();
        let service = batching_service(origin.clone())?;

        let idempotent = producer_batch(7, 0, 0, BatchAttribute::default(), b"idempotent")?;

        // Partition 1 holds a plain batch that could merge on its own. It stays in this request
        // because the request has an idempotent batch on partition 0.
        let mixed = produce_request_to_partitions(
            "a",
            vec![(0, idempotent), (1, record_data_batch(b"plain one")?)],
        );

        let responses = produce_in_one_batch(
            &service,
            vec![
                mixed.clone(),
                produce_request_to_partitions("a", vec![(1, record_data_batch(b"plain two")?)]),
            ],
        )
        .await?;

        assert_eq!(2, responses.len());

        let requests = origin.reqs.lock().expect("poison");
        assert_eq!(2, requests.len());
        assert!(requests.contains(&mixed));

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn plain_batches_on_one_partition_still_merge() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let origin = MockProduceService::default();
        let service = batching_service(origin.clone())?;

        let responses = produce_in_one_batch(
            &service,
            vec![
                produce_request_to_partitions("a", vec![(0, record_data_batch(b"one")?)]),
                produce_request_to_partitions("a", vec![(0, record_data_batch(b"two")?)]),
            ],
        )
        .await?;

        assert_eq!(2, responses.len());

        let forwarded = forwarded_batches(&origin);
        assert_eq!(1, forwarded.len());
        assert_eq!(2, forwarded[0][&(String::from("a"), 0)][0].record_count);

        Ok(())
    }
}
