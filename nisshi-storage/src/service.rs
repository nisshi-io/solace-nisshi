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

mod alter_user_scram_credentials;
mod consumer_group_describe;
#[cfg(test)]
mod counting_storage;
mod create_acls;
mod create_topics;
mod delete_acls;
mod delete_groups;
mod delete_records;
mod delete_topics;
mod describe_acls;
mod describe_cluster;
mod describe_configs;
mod describe_groups;
mod describe_topic_partitions;
mod describe_user_scram_credentials;
mod fetch;
mod find_coordinator;
mod incremental_alter_configs;
mod init_producer_id;
mod list_groups;
mod list_offsets;
mod list_partition_reassignments;
mod metadata;
mod produce;
#[cfg(test)]
mod produce_inflated_forwarding;
pub mod read_deadline;
pub mod read_share;
mod txn;

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Debug, Display, Formatter},
    sync::{Arc, LazyLock},
    time::{Duration, SystemTime},
};

pub use alter_user_scram_credentials::AlterUserScramCredentialsService;
use async_trait::async_trait;
pub use consumer_group_describe::ConsumerGroupDescribeService;
pub use create_acls::CreateAclsService;
pub use create_topics::{
    CreateTopicsService,
    validation::{
        CreateTopicsValidationLayer, CreateTopicsValidationService, MalformedCreateTopicsRequest,
        ValidatedCreateTopicsRequest,
    },
};
pub use delete_acls::DeleteAclsService;
pub use delete_groups::DeleteGroupsService;
pub use delete_records::{
    DELETE_RECORDS_CONCURRENCY, DeleteRecordsConcurrency, DeleteRecordsService,
};
pub use delete_topics::DeleteTopicsService;
pub use describe_acls::DescribeAclsService;
pub use describe_cluster::DescribeClusterService;
pub use describe_configs::DescribeConfigsService;
pub use describe_groups::{DescribeGroupsService, describe_groups_response};
pub use describe_topic_partitions::DescribeTopicPartitionsService;
pub use describe_user_scram_credentials::DescribeUserScramCredentialsService;
pub use fetch::{
    FetchService,
    validation::{
        FetchValidationLayer, FetchValidationService, MalformedFetchRequest, ValidatedFetchRequest,
    },
};
pub use find_coordinator::FindCoordinatorService;
pub use incremental_alter_configs::IncrementalAlterConfigsService;
pub use init_producer_id::InitProducerIdService;
pub use list_groups::{ListGroupsService, list_groups_response};
pub use list_offsets::{LIST_OFFSETS_CONCURRENCY, ListOffsetsService};
pub use list_partition_reassignments::ListPartitionReassignmentsService;
pub use metadata::{AutoCreateRefusals, MetadataService};
use nisshi_sans_io::{
    ConfigResource, ErrorCode, IsolationLevel, ListOffset, ScramMechanism,
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    fetch_response::AbortedTransaction,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    record::{deflated, inflated},
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Histogram},
};
pub use produce::{ProduceService, inflate_produced};
use rama::{Layer, Service};
use tokio::{
    sync::{
        mpsc::{self, error::SendError},
        oneshot,
    },
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument, warn};
pub use txn::add_offsets::AddOffsetsService as TxnAddOffsetsService;
pub use txn::add_partitions::AddPartitionService as TxnAddPartitionService;
pub use txn::end::EndService as TxnEndService;
pub use txn::offset_commit::OffsetCommitService as TxnOffsetCommitService;
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, CommittedOffset, Error, GroupDetail, ListOffsetResponse, METER,
    MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse,
    Result, ScramCredential, Storage, StorageOperation, TopicId, Topition, TxnAddPartitionsRequest,
    TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError, Version,
    service::read_deadline::{QueueWait as _, ReadDeadline, ReadWait},
};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Request {
    RegisterBroker(BrokerRegistrationRequest),
    IncrementalAlterResource(AlterConfigsResource),
    CreateTopic {
        topic: CreatableTopic,
        validate_only: bool,
    },
    DeleteRecords(Vec<DeleteRecordsTopic>),
    DeleteTopic(TopicId),
    Brokers,
    Produce {
        transaction_id: Option<String>,
        topition: Topition,
        batch: deflated::Batch,
        /// `batch` decoded, when the caller has done that: see [`Storage::produce_inflated`].
        inflated: Option<inflated::Batch>,
    },
    Fetch {
        topition: Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        deadline: Instant,
    },
    OffsetStage(Topition),
    CurrentOffsetStage(Topition),
    ListOffsets {
        isolation_level: IsolationLevel,
        offsets: Vec<(Topition, ListOffset)>,
    },
    OffsetCommit {
        group_id: String,
        retention_time_ms: Option<Duration>,
        offsets: Vec<(Topition, OffsetCommitRequest)>,
    },
    CommittedOffsetTopitions(String),
    OffsetFetch {
        group_id: Option<String>,
        topics: Vec<Topition>,
        require_stable: Option<bool>,
    },
    PendingTxnOffsets(String),
    Metadata(Option<Vec<TopicId>>),
    DescribeConfig {
        name: String,
        resource: ConfigResource,
        keys: Option<Vec<String>>,
    },
    DescribeTopicPartitions {
        topics: Option<Vec<TopicId>>,
        partition_limit: i32,
        cursor: Option<Topition>,
    },
    ListGroups,
    DeleteGroups(Option<Vec<String>>),
    DescribeGroups {
        group_ids: Option<Vec<String>>,
        include_authorized_operations: bool,
    },
    GroupWithVersion(String),
    UpdateGroup {
        group_id: String,
        detail: GroupDetail,
        version: Option<Version>,
    },
    InitProducer {
        transaction_id: Option<String>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    },
    TxnAddOffsets {
        transaction_id: String,
        producer_id: i64,
        producer_epoch: i16,
        group_id: String,
    },
    TxnAddPartitions(TxnAddPartitionsRequest),
    TxnOffsetCommit(TxnOffsetCommitRequest),
    TxnEnd {
        transaction_id: String,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    },
    Maintain(SystemTime),
    MaintainTransactions(SystemTime),
    AbortedTransactions {
        topition: Topition,
        offset: i64,
        last_stable_offset: i64,
    },
    ClusterId,
    Node,
    AdvertisedListener,
    DeleteUserScramCredential {
        user: String,
        mechanism: ScramMechanism,
    },
    UpsertUserScramCredential {
        user: String,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    },
    UserScramCredential {
        user: String,
        mechanism: ScramMechanism,
    },
    Ping,
}

impl Request {
    /// Returns the [`Storage`] method that this request calls.
    pub fn operation(&self) -> StorageOperation {
        match self {
            Self::AdvertisedListener => StorageOperation::AdvertisedListener,
            Self::Brokers => StorageOperation::Brokers,
            Self::ClusterId => StorageOperation::ClusterId,
            Self::CommittedOffsetTopitions(_) => StorageOperation::CommittedOffsetTopitions,
            Self::CreateTopic { .. } => StorageOperation::CreateTopic,
            Self::DeleteGroups(_) => StorageOperation::DeleteGroups,
            Self::DeleteRecords(_) => StorageOperation::DeleteRecords,
            Self::DeleteTopic(_) => StorageOperation::DeleteTopic,
            Self::DescribeConfig { .. } => StorageOperation::DescribeConfig,
            Self::DescribeGroups { .. } => StorageOperation::DescribeGroups,
            Self::DescribeTopicPartitions { .. } => StorageOperation::DescribeTopicPartitions,
            Self::Fetch { .. } => StorageOperation::Fetch,
            Self::GroupWithVersion(_) => StorageOperation::GroupWithVersion,
            Self::IncrementalAlterResource(_) => StorageOperation::IncrementalAlterResource,
            Self::InitProducer { .. } => StorageOperation::InitProducer,
            Self::ListGroups => StorageOperation::ListGroups,
            Self::ListOffsets { .. } => StorageOperation::ListOffsets,
            Self::Maintain(_) => StorageOperation::Maintain,
            Self::MaintainTransactions(_) => StorageOperation::MaintainTransactions,
            Self::AbortedTransactions { .. } => StorageOperation::AbortedTransactions,
            Self::Metadata(_) => StorageOperation::Metadata,
            Self::Node => StorageOperation::Node,
            Self::OffsetCommit { .. } => StorageOperation::OffsetCommit,
            Self::OffsetFetch { .. } => StorageOperation::OffsetFetch,
            Self::OffsetStage(_) => StorageOperation::OffsetStage,
            Self::CurrentOffsetStage(_) => StorageOperation::CurrentOffsetStage,
            Self::PendingTxnOffsets(_) => StorageOperation::PendingTxnOffsets,
            Self::Produce { .. } => StorageOperation::Produce,
            Self::RegisterBroker(_) => StorageOperation::RegisterBroker,
            Self::TxnAddOffsets { .. } => StorageOperation::TxnAddOffsets,
            Self::TxnAddPartitions(_) => StorageOperation::TxnAddPartitions,
            Self::TxnEnd { .. } => StorageOperation::TxnEnd,
            Self::TxnOffsetCommit(_) => StorageOperation::TxnOffsetCommit,
            Self::UpdateGroup { .. } => StorageOperation::UpdateGroup,
            Self::DeleteUserScramCredential { .. } => StorageOperation::DeleteUserScramCredential,
            Self::UpsertUserScramCredential { .. } => StorageOperation::UpsertUserScramCredential,
            Self::UserScramCredential { .. } => StorageOperation::UserScramCredential,
            Self::Ping => StorageOperation::Ping,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Response {
    AdvertisedListener(Result<Url>),
    Brokers(Result<Vec<DescribeClusterBroker>>),
    ClusterId(Result<String>),
    CommittedOffsetTopitions(Result<BTreeMap<Topition, CommittedOffset>>),
    CreateTopic(Result<Uuid>),
    DeleteGroups(Result<Vec<DeletableGroupResult>>),
    DeleteRecords(Result<Vec<DeleteRecordsTopicResult>>),
    DeleteTopic(Result<ErrorCode>),
    DeleteUserScramCredential(Result<()>),
    DescribeConfig(Result<DescribeConfigsResult>),
    DescribeGroups(Result<Vec<NamedGroupDetail>>),
    DescribeTopicPartitions(Result<Vec<DescribeTopicPartitionsResponseTopic>>),
    Fetch(Result<Vec<deflated::Batch>>),
    GroupWithVersion(Result<Option<(GroupDetail, Version)>>),
    IncrementalAlterResponse(Result<AlterConfigsResourceResponse>),
    InitProducer(Result<ProducerIdResponse>),
    ListGroups(Result<Vec<NamedGroupDetail>>),
    ListOffsets(Result<Vec<(Topition, ListOffsetResponse)>>),
    Maintain(Result<()>),
    MaintainTransactions(Result<()>),
    AbortedTransactions(Result<Vec<AbortedTransaction>>),
    Metadata(Result<MetadataResponse>),
    Node(Result<i32>),
    OffsetCommit(Result<Vec<(Topition, ErrorCode)>>),
    OffsetFetch(Result<BTreeMap<Topition, CommittedOffset>>),
    OffsetStage(Result<OffsetStage>),
    CurrentOffsetStage(Result<OffsetStage>),
    PendingTxnOffsets(Result<BTreeSet<Topition>>),
    Ping(Result<()>),
    Produce(Result<i64>),
    RegisterBroker(Result<()>),
    TxnAddOffsets(Result<ErrorCode>),
    TxnAddPartitions(Result<TxnAddPartitionsResponse>),
    TxnEnd(Result<ErrorCode>),
    TxnOffsetCommit(Result<Vec<TxnOffsetCommitResponseTopic>>),
    UpdateGroup(Result<Version, UpdateError<GroupDetail>>),
    UpsertUserScramCredential(Result<()>),
    UserScramCredential(Result<Option<ScramCredential>>),
}

/// Returns the error code that a partition answers when storage fails for
/// it: [`Error::error_code`], with `KAFKA_STORAGE_ERROR` for an error that a
/// retry may fix.
///
/// Kafka answers `KAFKA_STORAGE_ERROR` for a log that it cannot read, and
/// `UNKNOWN_SERVER_ERROR` for an exception that it does not map to a code
/// ([KafkaApis.scala#L1228-L1251]). A transient storage error, such as a
/// database or object store that times out or is unavailable, is the first,
/// and a client retries it after a metadata refresh. Any other error is the
/// second: a retry does not fix it.
///
/// [KafkaApis.scala#L1228-L1251]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1228-L1251
pub(crate) fn storage_error_code(error: &Error) -> ErrorCode {
    error.error_code(ErrorCode::KafkaStorageError)
}

/// The sender of a request's outcome: the response, or the error that its storage task failed
/// with. The error keeps its type, so [`Error::is_transient`] and [`Error::error_code`] give
/// the same answer on both sides of the channel.
pub type OutcomeSender = oneshot::Sender<Result<Response>>;

/// A [`Request`] sent on the storage channel, with a sender for its outcome and a sender for
/// the signal that its service started it.
#[derive(Debug)]
pub struct ChannelRequest {
    pub request: Request,
    /// Takes the outcome when the service has served the request.
    pub response: OutcomeSender,
    /// Takes a signal when the service starts to serve the request. The sender of the request
    /// treats a dropped `started` as a start.
    pub started: oneshot::Sender<()>,
}

pub type RequestSender = mpsc::Sender<ChannelRequest>;
pub type RequestReceiver = mpsc::Receiver<ChannelRequest>;

pub fn bounded_channel(buffer: usize) -> (RequestSender, RequestReceiver) {
    mpsc::channel::<ChannelRequest>(buffer)
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum ServiceError {
    Storage(Error),
    UpdateGroupDetail(UpdateError<GroupDetail>),
}

impl Display for ServiceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<SendError<()>> for ServiceError {
    fn from(_value: SendError<()>) -> Self {
        Self::Storage(Error::UnableToSend)
    }
}

impl From<Error> for ServiceError {
    fn from(value: Error) -> Self {
        Self::Storage(value)
    }
}

impl From<UpdateError<GroupDetail>> for ServiceError {
    fn from(value: UpdateError<GroupDetail>) -> Self {
        Self::UpdateGroupDetail(value)
    }
}

impl From<ServiceError> for Error {
    fn from(value: ServiceError) -> Self {
        if let ServiceError::Storage(error) = value {
            error
        } else {
            unreachable!()
        }
    }
}

impl From<ServiceError> for UpdateError<GroupDetail> {
    fn from(value: ServiceError) -> Self {
        if let ServiceError::UpdateGroupDetail(error) = value {
            error
        } else {
            unreachable!()
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestLayer;

impl<S> Layer<S> for RequestLayer {
    type Service = RequestService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestService<S> {
    inner: S,
}

impl<S> Service<Request> for RequestService<S>
where
    S: Service<Request>,
{
    type Output = S::Output;
    type Error = S::Error;

    async fn serve(&self, input: Request) -> Result<Self::Output, Self::Error> {
        debug!(?input);
        self.inner.serve(input).await
    }
}

/// A [`Service`] sending [`Request`]s over a [`RequestSender`] channel
#[derive(Clone, Debug)]
pub struct RequestChannelService {
    tx: RequestSender,
}

impl RequestChannelService {
    pub fn new(tx: RequestSender) -> Self {
        Self { tx }
    }

    /// Sends `input` on the channel once the channel has room for it.
    async fn send(&self, input: Request) -> Result<SentRequest, ServiceError> {
        let start = Instant::now();

        let operation = input.operation();
        let attributes = [operation.attribute()];

        let capacity = self.tx.capacity();
        STORAGE_CHANNEL_CAPACITY.record(capacity as u64, &attributes);
        debug!(%operation, capacity);

        let permit = self
            .tx
            .reserve()
            .queued_for(ReadWait::ExclusiveEngineAccess)
            .await
            .inspect_err(|err| {
                error!(%operation, ?err);
                STORAGE_CHANNEL_ERROR.add(1, &attributes);
            })?;

        Ok(Self::send_reserved(permit, input, start))
    }

    /// Sends `input` through `permit`, which reserved a slot in the channel at `start`.
    fn send_reserved(
        permit: mpsc::Permit<'_, ChannelRequest>,
        input: Request,
        start: Instant,
    ) -> SentRequest {
        let (resp_tx, resp_rx) = oneshot::channel();
        let (started_tx, started_rx) = oneshot::channel();

        let operation = input.operation();
        let attributes = [operation.attribute()];

        permit.send(ChannelRequest {
            request: input,
            response: resp_tx,
            started: started_tx,
        });

        let permit_elapsed = start.elapsed().as_millis() as u64;
        STORAGE_CHANNEL_PERMIT_DURATION.record(permit_elapsed, &attributes);
        debug!(%operation, permit_elapsed);

        SentRequest {
            response: resp_rx,
            started: started_rx,
            start,
            operation,
        }
    }
}

/// A [`Request`] that [`RequestChannelService`] has sent on its channel, waiting for its
/// [`Response`].
struct SentRequest {
    response: oneshot::Receiver<Result<Response>>,
    started: oneshot::Receiver<()>,
    start: Instant,
    operation: StorageOperation,
}

impl SentRequest {
    /// Returns whether the service on the other end of the channel started the request by
    /// `deadline`. If the deadline ends the wait, the wait counts as a `queued` miss of the
    /// request's operation.
    async fn started_by(&mut self, deadline: Instant) -> bool {
        ReadDeadline::new(self.operation, deadline)
            .timeout((&mut self.started).queued_for(ReadWait::ExclusiveEngineAccess))
            .await
            .is_ok()
    }

    async fn response(self) -> Result<Response, ServiceError> {
        let operation = self.operation;
        let attributes = [operation.attribute()];

        self.response
            .await
            .map_err(|_| Error::OneshotRecv)
            .and_then(|outcome| outcome)
            .map_err(ServiceError::from)
            .inspect(|_| {
                let elapsed_millis = self.start.elapsed().as_millis() as u64;
                STORAGE_CHANNEL_REQUEST_DURATION.record(elapsed_millis, &attributes);
                debug!(%operation, elapsed_millis);
            })
            .inspect_err(|err| {
                error!(%operation, ?err);
                STORAGE_CHANNEL_ERROR.add(1, &attributes);
            })
    }
}

static STORAGE_CHANNEL_CAPACITY: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    METER
        .u64_gauge("nisshi_storage_channel_capacity")
        .with_description("Storage channel capacity")
        .build()
});

impl Service<Request> for RequestChannelService {
    type Output = Response;
    type Error = ServiceError;

    #[instrument(skip_all)]
    async fn serve(&self, input: Request) -> Result<Self::Output, Self::Error> {
        self.send(input).await?.response().await
    }
}

static STORAGE_CHANNEL_REQUEST_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_storage_channel_request_duration")
        .with_unit("ms")
        .with_description("Storage channel request latency in milliseconds")
        .build()
});

static STORAGE_CHANNEL_PERMIT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_storage_channel_permit_duration")
        .with_unit("ms")
        .with_description("Storage channel permit latency in milliseconds")
        .build()
});

static STORAGE_CHANNEL_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_storage_channel_error")
        .with_description("Storage channel error count")
        .build()
});

/// Sends `$request` to the storage channel and extracts the `Response::$variant` payload,
/// erroring if the channel responded with an unexpected variant.
macro_rules! serve_and_extract {
    ($self:ident, $request:expr, $variant:ident) => {
        $self
            .serve($request)
            .await
            .and_then(|response| {
                if let Response::$variant(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    };
}

#[async_trait]
impl Storage for RequestChannelService {
    #[instrument(skip_all)]
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        serve_and_extract!(
            self,
            Request::RegisterBroker(broker_registration),
            RegisterBroker
        )
    }

    #[instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        serve_and_extract!(
            self,
            Request::IncrementalAlterResource(resource),
            IncrementalAlterResponse
        )
    }

    #[instrument(skip_all)]
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        serve_and_extract!(
            self,
            Request::CreateTopic {
                topic,
                validate_only,
            },
            CreateTopic
        )
    }

    #[instrument(skip_all)]
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        serve_and_extract!(
            self,
            Request::DeleteRecords(Vec::from(topics)),
            DeleteRecords
        )
    }

    /// The server of a channel serves one request after another, see
    /// [`ChannelRequestService`], so a second call waits for the first.
    /// This answers without a request over the channel, which a call could
    /// not make before the deadline check.
    fn delete_records_concurrency(&self) -> DeleteRecordsConcurrency {
        DeleteRecordsConcurrency::serial()
    }

    #[instrument(skip_all)]
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        serve_and_extract!(self, Request::DeleteTopic(topic.to_owned()), DeleteTopic)
    }

    #[instrument(skip_all)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        serve_and_extract!(self, Request::Brokers, Brokers)
    }

    #[instrument(skip_all)]
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        let transaction_id = transaction_id.map(|s| s.to_string());
        let topition = topition.to_owned();

        serve_and_extract!(
            self,
            Request::Produce {
                transaction_id,
                topition,
                batch,
                inflated: None,
            },
            Produce
        )
    }

    #[instrument(skip_all)]
    async fn produce_inflated(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        inflated: inflated::Batch,
    ) -> Result<i64> {
        // A produce that waits for a slot holds only its compressed bytes, so the number of decoded
        // batches the channel holds is the number of slots, not the number of waiting produces.
        // The produce then reaches the storage without the decoded batch, which decodes it again.
        let start = Instant::now();
        let Ok(permit) = self.tx.try_reserve() else {
            drop(inflated);

            return self.produce(transaction_id, topition, deflated).await;
        };

        let request = Request::Produce {
            transaction_id: transaction_id.map(|s| s.to_string()),
            topition: topition.to_owned(),
            batch: deflated,
            inflated: Some(inflated),
        };

        Self::send_reserved(permit, request, start)
            .response()
            .await
            .and_then(|response| {
                if let Response::Produce(inner) = response {
                    inner.map_err(Into::into)
                } else {
                    Err(Error::UnexpectedServiceResponse(Box::new(response)).into())
                }
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        deadline: Instant,
    ) -> Result<Vec<deflated::Batch>> {
        let request = Request::Fetch {
            topition: topition.to_owned(),
            offset,
            min_bytes,
            max_bytes,
            isolation,
            deadline,
        };

        // Until the read starts, `fetch` gives up at the deadline, because nothing has been read.
        let Ok(sent) = ReadDeadline::new(StorageOperation::Fetch, deadline)
            .timeout(self.send(request))
            .await
        else {
            return Ok(vec![]);
        };

        let mut sent = sent?;

        if !sent.started_by(deadline).await {
            return Ok(vec![]);
        }

        // Once the read has started, `fetch` waits for the response without a time limit,
        // because the response holds the batches that the read finished. The engine stops the
        // read at the deadline itself.
        match sent.response().await? {
            Response::Fetch(fetched) => fetched,
            response => Err(Error::UnexpectedServiceResponse(Box::new(response))),
        }
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        serve_and_extract!(self, Request::OffsetStage(topition.to_owned()), OffsetStage)
    }

    #[instrument(skip_all)]
    async fn current_offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        serve_and_extract!(
            self,
            Request::CurrentOffsetStage(topition.to_owned()),
            CurrentOffsetStage
        )
    }

    #[instrument(skip_all)]
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let offsets = Vec::from(offsets);

        serve_and_extract!(
            self,
            Request::ListOffsets {
                isolation_level,
                offsets,
            },
            ListOffsets
        )
    }

    #[instrument(skip_all)]
    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let group_id = group_id.to_string();
        let offsets = Vec::from(offsets);

        serve_and_extract!(
            self,
            Request::OffsetCommit {
                group_id,
                retention_time_ms,
                offsets,
            },
            OffsetCommit
        )
    }

    #[instrument(skip_all)]
    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        let group_id = group_id.to_string();

        serve_and_extract!(
            self,
            Request::CommittedOffsetTopitions(group_id),
            CommittedOffsetTopitions
        )
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        let group_id = group_id.map(|s| s.to_string());
        let topics = Vec::from(topics);

        serve_and_extract!(
            self,
            Request::OffsetFetch {
                group_id,
                topics,
                require_stable,
            },
            OffsetFetch
        )
    }

    #[instrument(skip_all)]
    async fn pending_txn_offsets(&self, group_id: &str) -> Result<BTreeSet<Topition>> {
        let group_id = group_id.to_string();

        serve_and_extract!(
            self,
            Request::PendingTxnOffsets(group_id),
            PendingTxnOffsets
        )
    }

    #[instrument(skip_all)]
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let topics = topics.map(Vec::from);

        serve_and_extract!(self, Request::Metadata(topics), Metadata)
    }

    #[instrument(skip_all)]
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let name = name.to_string();
        let keys = keys.map(Vec::from);

        serve_and_extract!(
            self,
            Request::DescribeConfig {
                name,
                resource,
                keys,
            },
            DescribeConfig
        )
    }

    #[instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let topics = topics.map(Vec::from);

        serve_and_extract!(
            self,
            Request::DescribeTopicPartitions {
                topics,
                partition_limit,
                cursor,
            },
            DescribeTopicPartitions
        )
    }

    #[instrument(skip_all)]
    async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
        serve_and_extract!(self, Request::ListGroups, ListGroups)
    }

    #[instrument(skip_all)]
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let group_ids = group_ids.map(Vec::from);

        serve_and_extract!(self, Request::DeleteGroups(group_ids), DeleteGroups)
    }

    #[instrument(skip_all)]
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let group_ids = group_ids.map(Vec::from);

        serve_and_extract!(
            self,
            Request::DescribeGroups {
                group_ids,
                include_authorized_operations,
            },
            DescribeGroups
        )
    }

    #[instrument(skip_all)]
    async fn group_with_version(&self, group_id: &str) -> Result<Option<(GroupDetail, Version)>> {
        serve_and_extract!(
            self,
            Request::GroupWithVersion(group_id.to_owned()),
            GroupWithVersion
        )
    }

    #[instrument(skip_all)]
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let group_id = group_id.to_string();

        serve_and_extract!(
            self,
            Request::UpdateGroup {
                group_id,
                detail,
                version,
            },
            UpdateGroup
        )
    }

    #[instrument(skip_all)]
    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        let transaction_id = transaction_id.map(|transaction_id| transaction_id.to_owned());

        serve_and_extract!(
            self,
            Request::InitProducer {
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            },
            InitProducer
        )
    }

    #[instrument(skip_all)]
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        let transaction_id = transaction_id.to_string();
        let group_id = group_id.to_string();

        serve_and_extract!(
            self,
            Request::TxnAddOffsets {
                transaction_id,
                producer_id,
                producer_epoch,
                group_id,
            },
            TxnAddOffsets
        )
    }

    #[instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        serve_and_extract!(
            self,
            Request::TxnAddPartitions(partitions),
            TxnAddPartitions
        )
    }

    #[instrument(skip_all)]
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        serve_and_extract!(self, Request::TxnOffsetCommit(offsets), TxnOffsetCommit)
    }

    #[instrument(skip_all)]
    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let transaction_id = transaction_id.to_string();

        serve_and_extract!(
            self,
            Request::TxnEnd {
                transaction_id,
                producer_id,
                producer_epoch,
                committed,
            },
            TxnEnd
        )
    }

    #[instrument(skip_all)]
    async fn maintain(&self, now: SystemTime) -> Result<()> {
        serve_and_extract!(self, Request::Maintain(now), Maintain)
    }

    #[instrument(skip_all)]
    async fn maintain_transactions(&self, now: SystemTime) -> Result<()> {
        serve_and_extract!(
            self,
            Request::MaintainTransactions(now),
            MaintainTransactions
        )
    }

    #[instrument(skip_all)]
    async fn aborted_transactions(
        &self,
        topition: &Topition,
        offset: i64,
        last_stable_offset: i64,
    ) -> Result<Vec<AbortedTransaction>> {
        serve_and_extract!(
            self,
            Request::AbortedTransactions {
                topition: topition.clone(),
                offset,
                last_stable_offset,
            },
            AbortedTransactions
        )
    }

    #[instrument(skip_all)]
    async fn cluster_id(&self) -> Result<String> {
        serve_and_extract!(self, Request::ClusterId, ClusterId)
    }

    #[instrument(skip_all)]
    async fn node(&self) -> Result<i32> {
        serve_and_extract!(self, Request::Node, Node)
    }

    #[instrument(skip_all)]
    async fn advertised_listener(&self) -> Result<Url> {
        serve_and_extract!(self, Request::AdvertisedListener, AdvertisedListener)
    }

    #[instrument(skip_all)]
    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        let user = user.to_string();

        serve_and_extract!(
            self,
            Request::DeleteUserScramCredential { user, mechanism },
            DeleteUserScramCredential
        )
    }

    #[instrument(skip_all)]
    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let user = user.to_string();

        serve_and_extract!(
            self,
            Request::UpsertUserScramCredential {
                user,
                mechanism,
                credential,
            },
            UpsertUserScramCredential
        )
    }

    #[instrument(skip_all)]
    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        let user = user.to_string();

        serve_and_extract!(
            self,
            Request::UserScramCredential { user, mechanism },
            UserScramCredential
        )
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        serve_and_extract!(self, Request::Ping, Ping)
    }
}

#[derive(Clone, Debug, Default)]
pub struct ChannelRequestLayer {
    cancellation: CancellationToken,
}

impl ChannelRequestLayer {
    pub fn new(cancellation: CancellationToken) -> Self {
        Self { cancellation }
    }
}

impl<S> Layer<S> for ChannelRequestLayer {
    type Service = ChannelRequestService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            cancellation: self.cancellation.clone(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ChannelRequestService<S> {
    inner: S,
    cancellation: CancellationToken,
}

static STORAGE_CHANNEL_SERVER_FAILURE: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_storage_channel_server_failure")
        .with_description(
            "Count of per-request storage task failures in the mpsc server loop, by kind: a \
             panic, a task cancelled at runtime shutdown, or a storage error; the loop itself \
             continues serving other requests in every case",
        )
        .build()
});

static STORAGE_CHANNEL_RESPONSE_DISCARDED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_storage_channel_response_discarded")
        .with_description(
            "Count of mpsc storage responses discarded because the requester dropped its \
             response receiver (a client disconnect or a read deadline); normal client churn",
        )
        .build()
});

impl<S> Service<RequestReceiver> for ChannelRequestService<S>
where
    S: Service<Request, Output = Response, Error = Error> + Clone,
{
    type Output = ();
    type Error = Error;

    /// Runs the mpsc-mode storage supervisor loop.
    ///
    /// Each request's storage work runs in its own `tokio::spawn`'d task so that a panic
    /// there is caught by Tokio's own per-task unwind handling (surfaced as
    /// `JoinError::is_panic`) instead of unwinding this loop and ending storage for every
    /// other caller sharing this channel. `spawn` is immediately followed by `.await` on
    /// the resulting `JoinHandle`, which keeps request processing strictly sequential:
    /// mpsc mode's single-writer discipline against the underlying SQLite connection
    /// depends on staying sequential here, so this must not become fire-and-forget.
    ///
    /// Note that aborting this task (e.g. by dropping a `JoinSet` that owns it) only
    /// cancels whichever child `JoinHandle` it happens to be awaiting at that moment; the
    /// child keeps running detached to completion. That is harmless for correctness (its
    /// `tx` is simply dropped, and the pooled connection still returns to the pool
    /// normally through the child's own unwind/return path), but abort is not transitive.
    async fn serve(&self, mut req: RequestReceiver) -> Result<Self::Output, Self::Error> {
        loop {
            tokio::select! {
                received = req.recv() => {
                    let Some(ChannelRequest { request, response: tx, started }) = received else {
                        debug!("storage channel closed; ending supervisor loop");
                        break;
                    };

                    // An expired fetch is answered with no batches instead of being served. Its
                    // requester has already given up and counted the miss, so serving it would
                    // only delay the requests queued behind it.
                    if let Request::Fetch { deadline, .. } = &request
                        && Instant::now() >= *deadline
                    {
                        _ = tx.send(Ok(Response::Fetch(Ok(vec![]))));
                        continue;
                    }

                    // The requester may have stopped waiting for the start already.
                    _ = started.send(());

                    let operation = request.operation();
                    let inner = self.inner.clone();

                    match tokio::spawn(async move { inner.serve(request).await }).await {
                        Ok(Ok(response)) => {
                            if tx.send(Ok(response)).is_err() {
                                debug!(
                                    %operation,
                                    "requester dropped its response receiver; discarding response"
                                );

                                STORAGE_CHANNEL_RESPONSE_DISCARDED
                                    .add(1, &[operation.attribute()]);
                            }
                        }

                        Ok(Err(error)) => {
                            error!(%operation, ?error, "storage request failed");

                            STORAGE_CHANNEL_SERVER_FAILURE.add(
                                1,
                                &[
                                    operation.attribute(),
                                    KeyValue::new("kind", "error"),
                                ],
                            );

                            // The requester may have dropped its receiver, and the error is
                            // already logged and counted.
                            _ = tx.send(Err(error));
                        }

                        Err(join_error) => {
                            let kind = if join_error.is_panic() {
                                error!(
                                    %operation,
                                    ?join_error,
                                    "storage request task panicked; isolated, loop continues"
                                );
                                "panic"
                            } else {
                                warn!(
                                    %operation,
                                    ?join_error,
                                    "storage request task cancelled"
                                );
                                "cancelled"
                            };

                            STORAGE_CHANNEL_SERVER_FAILURE.add(
                                1,
                                &[
                                    operation.attribute(),
                                    KeyValue::new("kind", kind),
                                ],
                            );

                            _ = tx.send(Err(Error::Join(Arc::new(join_error))));
                        }
                    }
                }

                cancelled = self.cancellation.cancelled() => {
                    debug!(?cancelled);
                    break;
                }
            }
        }

        Ok(())
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestStorageService<G> {
    storage: G,
}

impl<G> RequestStorageService<G>
where
    G: Storage,
{
    pub fn new(storage: G) -> Self {
        Self { storage }
    }
}

impl<G> Service<Request> for RequestStorageService<G>
where
    G: Storage,
{
    type Output = Response;
    type Error = Error;

    async fn serve(&self, req: Request) -> Result<Self::Output, Self::Error> {
        match req {
            Request::RegisterBroker(broker_registration) => Ok(Response::RegisterBroker(
                self.storage.register_broker(broker_registration).await,
            )),
            Request::IncrementalAlterResource(alter_configs_resource) => {
                Ok(Response::IncrementalAlterResponse(
                    self.storage
                        .incremental_alter_resource(alter_configs_resource)
                        .await,
                ))
            }
            Request::CreateTopic {
                topic,
                validate_only,
            } => Ok(Response::CreateTopic(
                self.storage.create_topic(topic, validate_only).await,
            )),
            Request::DeleteRecords(delete_records_topics) => Ok(Response::DeleteRecords(
                self.storage
                    .delete_records(&delete_records_topics[..])
                    .await,
            )),
            Request::DeleteTopic(topic_id) => Ok(Response::DeleteTopic(
                self.storage.delete_topic(&topic_id).await,
            )),
            Request::Brokers => Ok(Response::Brokers(self.storage.brokers().await)),
            Request::Produce {
                transaction_id,
                topition,
                batch,
                inflated,
            } => Ok(Response::Produce(match inflated {
                Some(inflated) => {
                    self.storage
                        .produce_inflated(transaction_id.as_deref(), &topition, batch, inflated)
                        .await
                }
                None => {
                    self.storage
                        .produce(transaction_id.as_deref(), &topition, batch)
                        .await
                }
            })),
            Request::Fetch {
                topition,
                offset,
                min_bytes,
                max_bytes,
                isolation,
                deadline,
            } => Ok(Response::Fetch(
                self.storage
                    .fetch(&topition, offset, min_bytes, max_bytes, isolation, deadline)
                    .await,
            )),
            Request::OffsetStage(topition) => Ok(Response::OffsetStage(
                self.storage.offset_stage(&topition).await,
            )),
            Request::CurrentOffsetStage(topition) => Ok(Response::CurrentOffsetStage(
                self.storage.current_offset_stage(&topition).await,
            )),
            Request::ListOffsets {
                isolation_level,
                offsets,
            } => Ok(Response::ListOffsets(
                self.storage
                    .list_offsets(isolation_level, &offsets[..])
                    .await,
            )),
            Request::OffsetCommit {
                group_id,
                retention_time_ms,
                offsets,
            } => Ok(Response::OffsetCommit(
                self.storage
                    .offset_commit(&group_id, retention_time_ms, &offsets[..])
                    .await,
            )),
            Request::CommittedOffsetTopitions(group_id) => Ok(Response::CommittedOffsetTopitions(
                self.storage.committed_offset_topitions(&group_id).await,
            )),
            Request::OffsetFetch {
                group_id,
                topics,
                require_stable,
            } => Ok(Response::OffsetFetch(
                self.storage
                    .offset_fetch(group_id.as_deref(), &topics[..], require_stable)
                    .await,
            )),
            Request::PendingTxnOffsets(group_id) => Ok(Response::PendingTxnOffsets(
                self.storage.pending_txn_offsets(&group_id).await,
            )),
            Request::Metadata(topic_ids) => Ok(Response::Metadata(
                self.storage.metadata(topic_ids.as_deref()).await,
            )),
            Request::DescribeConfig {
                name,
                resource,
                keys,
            } => Ok(Response::DescribeConfig(
                self.storage
                    .describe_config(&name, resource, keys.as_deref())
                    .await,
            )),
            Request::DescribeTopicPartitions {
                topics,
                partition_limit,
                cursor,
            } => Ok(Response::DescribeTopicPartitions(
                self.storage
                    .describe_topic_partitions(topics.as_deref(), partition_limit, cursor)
                    .await,
            )),
            Request::ListGroups => Ok(Response::ListGroups(self.storage.list_groups().await)),
            Request::DeleteGroups(items) => Ok(Response::DeleteGroups(
                self.storage.delete_groups(items.as_deref()).await,
            )),
            Request::DescribeGroups {
                group_ids,
                include_authorized_operations,
            } => Ok(Response::DescribeGroups(
                self.storage
                    .describe_groups(group_ids.as_deref(), include_authorized_operations)
                    .await,
            )),
            Request::GroupWithVersion(group_id) => Ok(Response::GroupWithVersion(
                self.storage.group_with_version(&group_id).await,
            )),
            Request::UpdateGroup {
                group_id,
                detail,
                version,
            } => Ok(Response::UpdateGroup(
                self.storage.update_group(&group_id, detail, version).await,
            )),
            Request::InitProducer {
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            } => Ok(Response::InitProducer(
                self.storage
                    .init_producer(
                        transaction_id.as_deref(),
                        transaction_timeout_ms,
                        producer_id,
                        producer_epoch,
                    )
                    .await,
            )),
            Request::TxnAddOffsets {
                transaction_id,
                producer_id,
                producer_epoch,
                group_id,
            } => Ok(Response::TxnAddOffsets(
                self.storage
                    .txn_add_offsets(&transaction_id, producer_id, producer_epoch, &group_id)
                    .await,
            )),
            Request::TxnAddPartitions(txn_add_partitions_request) => {
                Ok(Response::TxnAddPartitions(
                    self.storage
                        .txn_add_partitions(txn_add_partitions_request)
                        .await,
                ))
            }
            Request::TxnOffsetCommit(txn_offset_commit_request) => Ok(Response::TxnOffsetCommit(
                self.storage
                    .txn_offset_commit(txn_offset_commit_request)
                    .await,
            )),
            Request::TxnEnd {
                transaction_id,
                producer_id,
                producer_epoch,
                committed,
            } => Ok(Response::TxnEnd(
                self.storage
                    .txn_end(&transaction_id, producer_id, producer_epoch, committed)
                    .await,
            )),
            Request::Maintain(now) => Ok(Response::Maintain(self.storage.maintain(now).await)),
            Request::MaintainTransactions(now) => Ok(Response::MaintainTransactions(
                self.storage.maintain_transactions(now).await,
            )),
            Request::AbortedTransactions {
                topition,
                offset,
                last_stable_offset,
            } => Ok(Response::AbortedTransactions(
                self.storage
                    .aborted_transactions(&topition, offset, last_stable_offset)
                    .await,
            )),
            Request::ClusterId => Ok(Response::ClusterId(self.storage.cluster_id().await)),
            Request::Node => Ok(Response::Node(self.storage.node().await)),
            Request::AdvertisedListener => Ok(Response::AdvertisedListener(
                self.storage.advertised_listener().await,
            )),
            Request::DeleteUserScramCredential { user, mechanism } => {
                Ok(Response::DeleteUserScramCredential(
                    self.storage
                        .delete_user_scram_credential(&user[..], mechanism)
                        .await,
                ))
            }
            Request::UpsertUserScramCredential {
                user,
                mechanism,
                credential,
            } => Ok(Response::UpsertUserScramCredential(
                self.storage
                    .upsert_user_scram_credential(&user[..], mechanism, credential)
                    .await,
            )),
            Request::UserScramCredential { user, mechanism } => Ok(Response::UserScramCredential(
                self.storage
                    .user_scram_credential(&user[..], mechanism)
                    .await,
            )),
            Request::Ping => Ok(Response::Ping(self.storage.ping().await)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nisshi_sans_io::record::inflated;
    use tokio::{
        sync::oneshot,
        time::{Duration, Instant, sleep_until},
    };

    /// Pins the `operation` label each request is counted under on the
    /// `nisshi_storage_channel_*` metrics: the name of the [`Storage`] method it calls.
    #[test]
    fn operation_is_the_storage_method() {
        let topition = Topition::new("t", 0);
        let topic = TopicId::Name("t".into());

        let expected = [
            (
                Request::RegisterBroker(BrokerRegistrationRequest {
                    broker_id: 111,
                    cluster_id: "c".into(),
                    incarnation_id: Uuid::nil(),
                    rack: None,
                }),
                "register_broker",
            ),
            (
                Request::IncrementalAlterResource(AlterConfigsResource::default()),
                "incremental_alter_resource",
            ),
            (
                Request::CreateTopic {
                    topic: CreatableTopic::default(),
                    validate_only: false,
                },
                "create_topic",
            ),
            (Request::DeleteRecords(vec![]), "delete_records"),
            (Request::DeleteTopic(topic.clone()), "delete_topic"),
            (Request::Brokers, "brokers"),
            (
                Request::Produce {
                    transaction_id: None,
                    topition: topition.clone(),
                    batch: deflated::Batch::default(),
                },
                "produce",
            ),
            (
                Request::Fetch {
                    topition: topition.clone(),
                    offset: 0,
                    min_bytes: 0,
                    max_bytes: 0,
                    isolation: IsolationLevel::default(),
                    deadline: Instant::now(),
                },
                "fetch",
            ),
            (Request::OffsetStage(topition.clone()), "offset_stage"),
            (
                Request::CurrentOffsetStage(topition.clone()),
                "current_offset_stage",
            ),
            (
                Request::ListOffsets {
                    isolation_level: IsolationLevel::default(),
                    offsets: vec![],
                },
                "list_offsets",
            ),
            (
                Request::OffsetCommit {
                    group_id: "g".into(),
                    retention_time_ms: None,
                    offsets: vec![],
                },
                "offset_commit",
            ),
            (
                Request::CommittedOffsetTopitions("g".into()),
                "committed_offset_topitions",
            ),
            (
                Request::OffsetFetch {
                    group_id: None,
                    topics: vec![],
                    require_stable: None,
                },
                "offset_fetch",
            ),
            (
                Request::PendingTxnOffsets("g".into()),
                "pending_txn_offsets",
            ),
            (Request::Metadata(None), "metadata"),
            (
                Request::DescribeConfig {
                    name: "t".into(),
                    resource: ConfigResource::Topic,
                    keys: None,
                },
                "describe_config",
            ),
            (
                Request::DescribeTopicPartitions {
                    topics: None,
                    partition_limit: 0,
                    cursor: None,
                },
                "describe_topic_partitions",
            ),
            (Request::ListGroups, "list_groups"),
            (Request::DeleteGroups(None), "delete_groups"),
            (
                Request::DescribeGroups {
                    group_ids: None,
                    include_authorized_operations: false,
                },
                "describe_groups",
            ),
            (Request::GroupWithVersion("g".into()), "group_with_version"),
            (
                Request::UpdateGroup {
                    group_id: "g".into(),
                    detail: GroupDetail::default(),
                    version: None,
                },
                "update_group",
            ),
            (
                Request::InitProducer {
                    transaction_id: None,
                    transaction_timeout_ms: 0,
                    producer_id: None,
                    producer_epoch: None,
                },
                "init_producer",
            ),
            (
                Request::TxnAddOffsets {
                    transaction_id: "x".into(),
                    producer_id: 0,
                    producer_epoch: 0,
                    group_id: "g".into(),
                },
                "txn_add_offsets",
            ),
            (
                Request::TxnAddPartitions(TxnAddPartitionsRequest::VersionFourPlus {
                    transactions: vec![],
                }),
                "txn_add_partitions",
            ),
            (
                Request::TxnOffsetCommit(TxnOffsetCommitRequest {
                    transaction_id: "x".into(),
                    group_id: "g".into(),
                    producer_id: 0,
                    producer_epoch: 0,
                    generation_id: None,
                    member_id: None,
                    group_instance_id: None,
                    topics: vec![],
                }),
                "txn_offset_commit",
            ),
            (
                Request::TxnEnd {
                    transaction_id: "x".into(),
                    producer_id: 0,
                    producer_epoch: 0,
                    committed: true,
                },
                "txn_end",
            ),
            (Request::Maintain(SystemTime::UNIX_EPOCH), "maintain"),
            (
                Request::MaintainTransactions(SystemTime::UNIX_EPOCH),
                "maintain_transactions",
            ),
            (
                Request::AbortedTransactions {
                    topition,
                    offset: 0,
                    last_stable_offset: 0,
                },
                "aborted_transactions",
            ),
            (Request::ClusterId, "cluster_id"),
            (Request::Node, "node"),
            (Request::AdvertisedListener, "advertised_listener"),
            (
                Request::DeleteUserScramCredential {
                    user: "u".into(),
                    mechanism: ScramMechanism::Scram256,
                },
                "delete_user_scram_credential",
            ),
            (
                Request::UpsertUserScramCredential {
                    user: "u".into(),
                    mechanism: ScramMechanism::Scram256,
                    credential: ScramCredential::default(),
                },
                "upsert_user_scram_credential",
            ),
            (
                Request::UserScramCredential {
                    user: "u".into(),
                    mechanism: ScramMechanism::Scram256,
                },
                "user_scram_credential",
            ),
            (Request::Ping, "ping"),
        ];

        for (request, label) in expected {
            assert_eq!(label, request.operation().as_str(), "{request:?}");
        }
    }

    async fn fetch(storage: &RequestChannelService, deadline: Instant) -> Result<usize> {
        storage
            .fetch(
                &Topition::new("abc", 0),
                0,
                1,
                1024,
                IsolationLevel::ReadUncommitted,
                deadline,
            )
            .await
            .map(|batches| batches.len())
    }

    #[tokio::test(start_paused = true)]
    async fn fetch_stops_waiting_for_room_on_the_channel_at_its_deadline() -> Result<()> {
        let (tx, _rx) = bounded_channel(1);

        // The request fills the channel, and nothing receives it, so the fetch never gets room.
        let (response, _) = oneshot::channel();
        let (started, _) = oneshot::channel();
        tx.send(ChannelRequest {
            request: Request::Brokers,
            response,
            started,
        })
        .await
        .expect("room on the channel");

        let storage = RequestChannelService::new(tx);

        let started_at = Instant::now();
        let deadline = started_at + Duration::from_millis(500);

        assert_eq!(0, fetch(&storage, deadline).await?);
        assert_eq!(Duration::from_millis(500), started_at.elapsed());

        Ok(())
    }

    /// Once the service has started the read, the fetch waits for its response, and keeps its
    /// batches even when the response arrives after the deadline.
    #[tokio::test(start_paused = true)]
    async fn fetch_waits_for_the_response_of_a_read_that_started_by_its_deadline() -> Result<()> {
        let (tx, mut rx) = bounded_channel(1);
        let storage = RequestChannelService::new(tx);

        let deadline = Instant::now() + Duration::from_millis(500);

        let answer = async {
            let ChannelRequest {
                response: reply,
                started,
                ..
            } = rx.recv().await.expect("a fetch request");

            started.send(()).expect("the reader");

            sleep_until(deadline + Duration::from_millis(100)).await;

            let batch = inflated::Batch::builder()
                .build()
                .and_then(deflated::Batch::try_from)
                .expect("a batch");

            reply
                .send(Ok(Response::Fetch(Ok(vec![batch]))))
                .expect("the reader");
        };

        let (fetched, ()) = tokio::join!(fetch(&storage, deadline), answer);

        assert_eq!(1, fetched?);

        Ok(())
    }

    /// The fetch stops waiting at the deadline when the service has not started the read by then,
    /// and answers with no batches.
    #[tokio::test(start_paused = true)]
    async fn fetch_stops_waiting_for_a_read_that_has_not_started_by_its_deadline() -> Result<()> {
        let (tx, mut rx) = bounded_channel(1);
        let storage = RequestChannelService::new(tx);

        let started_at = Instant::now();
        let deadline = started_at + Duration::from_millis(500);

        // The service receives the read but never starts it, as when it serves an earlier
        // request until after the deadline.
        let held = async { rx.recv().await.expect("a fetch request") };

        let (fetched, _held) = tokio::join!(fetch(&storage, deadline), held);

        assert_eq!(0, fetched?);
        assert_eq!(Duration::from_millis(500), started_at.elapsed());

        Ok(())
    }
}
