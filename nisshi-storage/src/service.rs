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
pub mod deadline;
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
pub mod read_share;
mod txn;

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Debug, Display, Formatter},
    sync::LazyLock,
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
pub use delete_records::DeleteRecordsService;
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
    create_topics_request::CreatableTopic, delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic, delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    fetch_response::AbortedTransaction, incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse, record::deflated,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Histogram},
};
pub use produce::{ProduceService, inflate_produced};
use rama::{Layer, Service};
use tokio::sync::{
    mpsc::{self, error::SendError},
    oneshot,
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
    Result, ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest,
    TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError, Version,
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
    },
    Fetch {
        topition: Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        max_wait: Duration,
    },
    OffsetStage(Topition),
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

impl Display for Request {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::AdvertisedListener => f.write_str("AdvertisedListener"),
            Self::Brokers => f.write_str("Brokers"),
            Self::ClusterId => f.write_str("ClusterId"),
            Self::CommittedOffsetTopitions(_) => f.write_str("CommittedOffsetTopitions"),
            Self::CreateTopic { .. } => f.write_str("CreateTopic"),
            Self::DeleteGroups(_) => f.write_str("DeleteGroups"),
            Self::DeleteRecords(_) => f.write_str("DeleteRecords"),
            Self::DeleteTopic(_) => f.write_str("DeleteTopic"),
            Self::DescribeConfig { .. } => f.write_str("DescribeConfig"),
            Self::DescribeGroups { .. } => f.write_str("DescribeGroups"),
            Self::DescribeTopicPartitions { .. } => f.write_str("DescribeTopicPartitions"),
            Self::Fetch { .. } => f.write_str("Fetch"),
            Self::GroupWithVersion(_) => f.write_str("GroupWithVersion"),
            Self::IncrementalAlterResource(_) => f.write_str("IncrementalAlterResource"),
            Self::InitProducer { .. } => f.write_str("InitProducer"),
            Self::ListGroups => f.write_str("ListGroups"),
            Self::ListOffsets { .. } => f.write_str("ListOffsets"),
            Self::Maintain(_) => f.write_str("Maintain"),
            Self::MaintainTransactions(_) => f.write_str("MaintainTransactions"),
            Self::AbortedTransactions { .. } => f.write_str("AbortedTransactions"),
            Self::Metadata(_) => f.write_str("Metadata"),
            Self::Node => f.write_str("Node"),
            Self::OffsetCommit { .. } => f.write_str("OffsetCommit"),
            Self::OffsetFetch { .. } => f.write_str("OffsetFetch"),
            Self::OffsetStage(_) => f.write_str("OffsetStage"),
            Self::PendingTxnOffsets(_) => f.write_str("PendingTxnOffsets"),
            Self::Produce { .. } => f.write_str("Produce"),
            Self::RegisterBroker(_) => f.write_str("RegisterBroker"),
            Self::TxnAddOffsets { .. } => f.write_str("TxnAddOffsets"),
            Self::TxnAddPartitions(_) => f.write_str("TxnAddPartitions"),
            Self::TxnEnd { .. } => f.write_str("TxnEnd"),
            Self::TxnOffsetCommit(_) => f.write_str("TxnOffsetCommit"),
            Self::UpdateGroup { .. } => f.write_str("UpdateGroup"),
            Self::DeleteUserScramCredential { .. } => f.write_str("DeleteUserScramCredential"),
            Self::UpsertUserScramCredential { .. } => f.write_str("UpsertUserScramCredential"),
            Self::UserScramCredential { .. } => f.write_str("UserScramCredential"),
            Self::Ping => f.write_str("Ping"),
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

pub type RequestSender = mpsc::Sender<(Request, oneshot::Sender<Response>)>;
pub type RequestReceiver = mpsc::Receiver<(Request, oneshot::Sender<Response>)>;

pub fn bounded_channel(buffer: usize) -> (RequestSender, RequestReceiver) {
    mpsc::channel::<(Request, oneshot::Sender<Response>)>(buffer)
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

    fn elapsed_millis(&self, start: SystemTime) -> u64 {
        start
            .elapsed()
            .map_or(0, |duration| duration.as_millis() as u64)
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
        let (resp_tx, resp_rx) = oneshot::channel();

        let start = SystemTime::now();

        let operation = input.to_string();
        let attributes = [KeyValue::new("operation", operation.clone())];

        let capacity = self.tx.capacity();
        STORAGE_CHANNEL_CAPACITY.record(capacity as u64, &attributes);
        debug!(operation, capacity);

        deadline::queued(self.tx.reserve())
            .await
            .map(|permit| permit.send((input, resp_tx)))
            .inspect(|_| {
                let permit_elapsed = self.elapsed_millis(start);
                STORAGE_CHANNEL_PERMIT_DURATION.record(permit_elapsed, &attributes);
                debug!(operation, permit_elapsed);
            })
            .inspect_err(|err| {
                error!(operation, ?err);
                STORAGE_CHANNEL_ERROR.add(1, &attributes);
            })?;

        resp_rx
            .await
            .map_err(|_| Error::OneshotRecv.into())
            .inspect(|_| {
                let elapsed_millis = self.elapsed_millis(start);
                STORAGE_CHANNEL_REQUEST_DURATION.record(elapsed_millis, &attributes);
                debug!(operation, elapsed_millis);
            })
            .inspect_err(|err| {
                error!(operation, ?err);
                STORAGE_CHANNEL_ERROR.add(1, &attributes);
            })
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
            },
            Produce
        )
    }

    #[instrument(skip_all)]
    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        let topition = topition.to_owned();

        serve_and_extract!(
            self,
            Request::Fetch {
                topition,
                offset,
                min_bytes,
                max_bytes,
                isolation,
                max_wait,
            },
            Fetch
        )
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        serve_and_extract!(self, Request::OffsetStage(topition.to_owned()), OffsetStage)
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
                    let Some((request, tx)) = received else {
                        debug!("storage channel closed; ending supervisor loop");
                        break;
                    };

                    let operation = request.to_string();
                    let inner = self.inner.clone();

                    match tokio::spawn(async move { inner.serve(request).await }).await {
                        Ok(Ok(response)) => {
                            if tx.send(response).is_err() {
                                debug!(
                                    operation,
                                    "requester dropped its response receiver; discarding response"
                                );

                                STORAGE_CHANNEL_RESPONSE_DISCARDED
                                    .add(1, &[KeyValue::new("operation", operation)]);
                            }
                        }

                        Ok(Err(error)) => {
                            error!(operation, ?error, "storage request failed");

                            STORAGE_CHANNEL_SERVER_FAILURE.add(
                                1,
                                &[
                                    KeyValue::new("operation", operation),
                                    KeyValue::new("kind", "error"),
                                ],
                            );
                        }

                        Err(join_error) => {
                            let kind = if join_error.is_panic() {
                                error!(
                                    operation,
                                    ?join_error,
                                    "storage request task panicked; isolated, loop continues"
                                );
                                "panic"
                            } else {
                                warn!(
                                    operation,
                                    ?join_error,
                                    "storage request task cancelled"
                                );
                                "cancelled"
                            };

                            STORAGE_CHANNEL_SERVER_FAILURE.add(
                                1,
                                &[
                                    KeyValue::new("operation", operation),
                                    KeyValue::new("kind", kind),
                                ],
                            );
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
            } => Ok(Response::Produce(
                self.storage
                    .produce(transaction_id.as_deref(), &topition, batch)
                    .await,
            )),
            Request::Fetch {
                topition,
                offset,
                min_bytes,
                max_bytes,
                isolation,
                max_wait,
            } => Ok(Response::Fetch(
                self.storage
                    .fetch(&topition, offset, min_bytes, max_bytes, isolation, max_wait)
                    .await,
            )),
            Request::OffsetStage(topition) => Ok(Response::OffsetStage(
                self.storage.offset_stage(&topition).await,
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
