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
//
//! Nisshi Storage Abstraction
//!
//! [`StorageContainer`] provides an abstraction over [`Storage`] and can
//! be configured to use memory, [S3](https://en.wikipedia.org/wiki/Amazon_S3),
//! [PostgreSQL](https://postgresql.org/),
//! [libSQL](https://github.com/tursodatabase/libsql) and
//! [Turso](https://github.com/tursodatabase/turso) (alpha: currently feature locked).
//!
//! ## Memory
//!
//! ```no_run
//! # use nisshi_storage::{Error, StorageContainer};
//! # use url::Url;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let storage = StorageContainer::builder()
//!     .cluster_id("nisshi")
//!     .node_id(111)
//!     .advertised_listener(Url::parse("tcp://localhost:9092")?)
//!     .storage(Url::parse("memory://nisshi/")?)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## S3
//!
//! ```no_run
//! # use nisshi_storage::{Error, StorageContainer};
//! # use url::Url;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let storage = StorageContainer::builder()
//!     .cluster_id("nisshi")
//!     .node_id(111)
//!     .advertised_listener(Url::parse("tcp://localhost:9092")?)
//!     .storage(Url::parse("s3://nisshi/")?)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## PostgreSQL
//!
//! ```no_run
//! # use nisshi_storage::{Error, StorageContainer};
//! # use url::Url;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let storage = StorageContainer::builder()
//!     .cluster_id("nisshi")
//!     .node_id(111)
//!     .advertised_listener(Url::parse("tcp://localhost:9092")?)
//!     .storage(Url::parse("postgres://postgres:postgres@localhost")?)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## libSQL (SQLite)
//!
//! ```no_run
//! # use nisshi_storage::{Error, StorageContainer};
//! # use url::Url;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let storage = StorageContainer::builder()
//!     .cluster_id("nisshi")
//!     .node_id(111)
//!     .advertised_listener(Url::parse("tcp://localhost:9092")?)
//!     .storage(Url::parse("sqlite://nisshi.db")?)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Turso
//!
//! ```no_run
//! # use nisshi_storage::{Error, StorageContainer};
//! # use url::Url;
//! # #[tokio::main]
//! # async fn main() -> Result<(), Error> {
//! let storage = StorageContainer::builder()
//!     .cluster_id("nisshi")
//!     .node_id(111)
//!     .advertised_listener(Url::parse("tcp://localhost:9092")?)
//!     .storage(Url::parse("turso://nisshi.db")?)
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!

#![deny(clippy::todo, clippy::unimplemented)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::todo,
        clippy::unimplemented
    )
)]
use async_trait::async_trait;
use bytes::{Bytes, TryGetError};

use console::Emoji;
#[cfg(any(feature = "libsql", feature = "postgres"))]
use deadpool::managed::PoolError;

use glob::{GlobError, PatternError};

use indicatif::{ProgressBar, ProgressStyle};

use opentelemetry::{InstrumentationScope, global, metrics::Meter};
use opentelemetry_semantic_conventions::SCHEMA_URL;

use governor::InsufficientCapacity;
use nisshi_sans_io::{
    Body, ConfigResource, ErrorCode, IsolationLevel, ListOffset, NULL_TOPIC_ID, ScramMechanism,
    add_partitions_to_txn_request::{
        AddPartitionsToTxnRequest, AddPartitionsToTxnTopic, AddPartitionsToTxnTransaction,
    },
    add_partitions_to_txn_response::{AddPartitionsToTxnResult, AddPartitionsToTxnTopicResult},
    consumer_group_describe_response,
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::DeleteRecordsTopicResult,
    delete_topics_request::DeleteTopicState,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_groups_response,
    describe_topic_partitions_request::{Cursor, TopicRequest},
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    fetch_request::FetchTopic,
    fetch_response::AbortedTransaction,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    join_group_response::JoinGroupResponseMember,
    list_groups_response::ListedGroup,
    metadata_request::MetadataRequestTopic,
    metadata_response::{MetadataResponseBroker, MetadataResponseTopic},
    offset_commit_request::OffsetCommitRequestPartition,
    record::deflated,
    to_system_time, to_timestamp,
    txn_offset_commit_request::TxnOffsetCommitRequestTopic,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use nisshi_schema::{
    Registry,
    lake::{House, LakeHouseType},
    redact_url,
};
use rama::error::BoxError;
use regex::Regex;
use serde::{Deserialize, Serialize};
#[cfg(any(feature = "libsql", feature = "postgres"))]
use std::error;
use std::{
    array::TryFromSliceError,
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fmt::{self, Debug, Display, Formatter},
    fs::DirEntry,
    io,
    marker::PhantomData,
    num::{ParseIntError, TryFromIntError},
    path::PathBuf,
    result,
    str::FromStr,
    sync::{Arc, LazyLock, PoisonError},
    time::{Duration, SystemTime, SystemTimeError},
};
use tokio::{sync::AcquireError, task::JoinError};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};
use tracing_subscriber::filter::ParseError;
use url::Url;
use uuid::Uuid;

mod assignment;
mod authorized_operations;
mod batch;
pub mod config;
mod inflate;
mod latency;
pub mod max_message_bytes;
pub mod message_timestamp_type;
pub mod offset_metadata;
mod producer;
mod proxy;
mod service;
mod topic_creation;
mod topic_defaults;

pub use assignment::{MAX_REPLICATION_FACTOR, assign_replicas, single_node_replicas};
pub use batch::ProduceRequestBatcher;
pub use inflate::{inflate, offload};
pub use latency::LatencyIntroducingStorage;
pub use producer::{
    OLDER_EPOCH_ERROR, ProduceEpoch, StoredEpochs, check_claim, epoch_is_exhausted, producer_claim,
};
pub use proxy::SemaphoreProxy;
pub use service::deadline;
pub use service::read_share;
pub use topic_creation::{DEFAULT_NUM_PARTITIONS, MAX_PARTITIONS_PER_REQUEST, TopicCreationConfig};

pub use service::{
    AlterUserScramCredentialsService, AutoCreateRefusals, ChannelRequestLayer,
    ChannelRequestService, ConsumerGroupDescribeService, CreateAclsService, CreateTopicsService,
    CreateTopicsValidationLayer, CreateTopicsValidationService, DeleteAclsService,
    DeleteGroupsService, DeleteRecordsService, DeleteTopicsService, DescribeAclsService,
    DescribeClusterService, DescribeConfigsService, DescribeGroupsService,
    DescribeTopicPartitionsService, DescribeUserScramCredentialsService, FetchService,
    FetchValidationLayer, FetchValidationService, FindCoordinatorService,
    IncrementalAlterConfigsService, InitProducerIdService, LIST_OFFSETS_CONCURRENCY,
    ListGroupsService, ListOffsetsService, ListPartitionReassignmentsService,
    MalformedCreateTopicsRequest, MalformedFetchRequest, MetadataService, ProduceService, Request,
    RequestChannelService, RequestLayer, RequestReceiver, RequestSender, RequestService,
    RequestStorageService, Response, TxnAddOffsetsService, TxnAddPartitionService, TxnEndService,
    TxnOffsetCommitService, ValidatedCreateTopicsRequest, ValidatedFetchRequest, bounded_channel,
    describe_groups_response, inflate_produced, list_groups_response,
};

#[cfg(feature = "dynostore")]
mod os;

/// The ways that the broker's storage can fail at startup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupFailure {
    /// The storage failed the broker's startup [`Storage::ping`].
    StorageUnreachable,
    /// The broker failed to check or create its tables in the storage.
    SchemaSetup,
}

/// Storage Errors
#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    Acquire(Arc<AcquireError>),

    Api(ErrorCode),

    /// The broker was asked for `--authentication` on the `engine` storage engine, which can't
    /// store SCRAM credentials. `supported` names the storage engines of this build that can,
    /// and is empty when there are none. See [`authentication_unsupported_message`].
    AuthenticationUnsupported {
        engine: String,
        supported: Vec<String>,
    },

    /// The `engine` storage engine can't store SCRAM credentials, so a credential upsert or
    /// deletion changed nothing. See [`user_credentials_unsupported_message`].
    UserCredentialsUnsupported {
        engine: String,
    },

    /// An `acks=0` Produce request had at least one non-`None` error code
    /// among its partition responses. The client reads no response either
    /// way under `acks=0`, so the caller closes the connection instead of
    /// silently dropping the batch, carrying the first failing partition's
    /// context for an operator reading the log.
    AcksZeroProduceFailed {
        topic: String,
        partition: i32,
        error_code: ErrorCode,
    },

    ChronoParse(#[from] chrono::ParseError),

    #[cfg(any(feature = "postgres", feature = "libsql"))]
    DeadPoolBuild(#[from] deadpool::managed::BuildError),

    Decode(Bytes),

    Boxed(Arc<BoxError>),

    FeatureNotEnabled {
        feature: String,
        message: String,
    },

    Glob(Arc<GlobError>),

    /// No storage factory registered with the builder accepts the `scheme` of the `storage` URL,
    /// which has its credentials redacted. A build that leaves out the engine's feature has no
    /// factory for it.
    NoEngineForScheme {
        scheme: String,
        storage: String,
    },

    /// The database records schema `recorded_version`, which a broker of `schema_version` can't
    /// use. Only a broker of `oldest_compatible_version` or later can use it.
    IncompatibleSchema {
        recorded_version: i32,
        oldest_compatible_version: i32,
        schema_version: i32,
    },

    /// The database records schema `recorded_version`, which is older than `schema_version`. A
    /// broker of `schema_version` can't upgrade the recorded schema.
    OutdatedSchema {
        recorded_version: i32,
        schema_version: i32,
    },

    InsufficientCapacity(#[from] InsufficientCapacity),
    Io(Arc<io::Error>),

    /// An [`offload`]ed task panicked or was cancelled.
    Join(Arc<JoinError>),

    LessThanBaseOffset {
        offset: i64,
        base_offset: i64,
    },
    LessThanLastOffset {
        offset: i64,
        last_offset: Option<i64>,
    },

    #[cfg(feature = "libsql")]
    LibSql(Arc<libsql::Error>),

    LessThanMaxTime {
        time: i64,
        max_time: Option<i64>,
    },
    LessThanMinTime {
        time: i64,
        min_time: Option<i64>,
    },
    /// A produced batch of `size` bytes is larger than its topic's `max.message.bytes` of `limit`
    /// bytes. Produce answers it with `MESSAGE_TOO_LARGE`.
    MessageTooLarge {
        size: usize,
        limit: usize,
    },
    Message(String),
    NoSuchEntry {
        nth: u32,
    },
    NoSuchOffset(i64),
    OsString(OsString),

    /// The error when the storage at `storage` fails at startup. `failure` names what failed.
    /// `source` is the error that the storage returned.
    StartupFailed {
        /// The storage URL, with any password removed.
        storage: Arc<Url>,
        failure: StartupFailure,
        source: Arc<Error>,
    },

    #[cfg(any(feature = "dynostore", feature = "slatedb"))]
    ObjectStore(Arc<object_store::Error>),

    /// The configured AWS credential provider did not return a credential.
    /// Distinct from [`Error::ObjectStore`], which covers a request that
    /// failed, including a request signed with a credential that the store
    /// rejected.
    #[cfg(any(feature = "dynostore", feature = "slatedb"))]
    NoCredentials(Arc<object_store::Error>),

    ParseFilter(Arc<ParseError>),
    Pattern(Arc<PatternError>),
    ParseInt(#[from] ParseIntError),
    PhantomCached(),
    Poison,

    #[cfg(any(feature = "libsql", feature = "postgres"))]
    Pool(Arc<Box<dyn error::Error + Send + Sync>>),

    #[cfg(feature = "slatedb")]
    Postcard(#[from] postcard::Error),

    ProgressStyleTemplate(Arc<indicatif::style::TemplateError>),

    Regex(#[from] regex::Error),

    SansIo(#[from] nisshi_sans_io::Error),

    Schema(Arc<nisshi_schema::Error>),

    Rustls(#[from] rustls::Error),

    SegmentEmpty(Topition),

    SegmentMissing {
        topition: Topition,
        offset: Option<i64>,
    },

    SerdeJson(Arc<serde_json::Error>),

    #[cfg(feature = "slatedb")]
    Slate(Arc<slatedb::Error>),

    SystemTime(#[from] SystemTimeError),

    #[cfg(feature = "postgres")]
    TokioPostgres(Arc<tokio_postgres::error::Error>),
    TryFromInt(#[from] TryFromIntError),
    TryFromSlice(#[from] TryFromSliceError),

    TryGet(Arc<TryGetError>),

    #[cfg(feature = "turso")]
    Turso(Arc<turso::Error>),

    UnexpectedBody(Box<Body>),

    UnexpectedServiceResponse(Box<Response>),

    #[cfg(feature = "turso")]
    UnexpectedValue(turso::Value),

    #[cfg(feature = "libsql")]
    UnexpectedLibSqlValue(libsql::Value),

    UnknownCacheKey(String),

    /// A storage URL gave an option that the engine reads, with a value that the
    /// engine cannot parse.
    InvalidStorageOptionValue {
        scheme: String,
        option: String,
        value: String,

        /// The values that the option takes, for example `"single or multi"`.
        /// The engine that defines the option writes this hint, so a message
        /// can tell the user what to give without knowing every engine's
        /// options.
        expected: &'static str,
    },

    /// A storage URL named a query option that scheme's engine does not
    /// recognise (for example a typo, or an option that belongs to a
    /// different engine).
    UnrecognizedStorageOption {
        scheme: String,
        option: String,
    },

    /// The v4+ (multi-transaction) field shape of `AddPartitionsToTxn` is not implemented on
    /// this backend.
    UnsupportedTxnAddPartitionsVersion,
    UnexpectedAddPartitionsToTxnRequest(Box<AddPartitionsToTxnRequest>),
    Url(#[from] url::ParseError),
    UnknownTxnState(String),

    Uuid(#[from] uuid::Error),

    UnableToSend,
    OneshotRecv,
}

impl Error {
    /// Returns the error code that a group request, such as `ListGroups` or `DescribeGroups`,
    /// reports when storage fails with this error.
    ///
    /// A storage error that carries a code keeps that code. Any other storage error is treated as
    /// transient, so the response reports `COORDINATOR_NOT_AVAILABLE`, which Kafka clients
    /// [retry].
    ///
    /// [retry]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java#L3614-L3620
    pub fn group_request_error_code(&self) -> ErrorCode {
        match self {
            Self::Api(error_code) => *error_code,
            _ => ErrorCode::CoordinatorNotAvailable,
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<TryGetError> for Error {
    fn from(value: TryGetError) -> Self {
        Self::TryGet(Arc::new(value))
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl From<JoinError> for Error {
    fn from(value: JoinError) -> Self {
        Self::Join(Arc::new(value))
    }
}

impl From<AcquireError> for Error {
    fn from(value: AcquireError) -> Self {
        Self::Acquire(Arc::new(value))
    }
}

impl From<indicatif::style::TemplateError> for Error {
    fn from(value: indicatif::style::TemplateError) -> Self {
        Self::ProgressStyleTemplate(Arc::new(value))
    }
}

#[cfg(any(feature = "libsql", feature = "postgres"))]
impl<E> From<PoolError<E>> for Error
where
    E: error::Error + Send + Sync + 'static,
{
    fn from(value: PoolError<E>) -> Self {
        // a pool-acquisition timeout is transient load, not a backend failure: map it to a
        // retriable Kafka error code so callers (e.g. produce.rs's `Error::Api(_)` branch)
        // don't turn it into a non-retriable `UnknownServerError` for the client.
        if matches!(value, PoolError::Timeout(_)) {
            Self::Api(ErrorCode::RequestTimedOut)
        } else {
            Self::Pool(Arc::new(Box::new(value)))
        }
    }
}

#[cfg(feature = "libsql")]
impl From<libsql::Error> for Error {
    fn from(value: libsql::Error) -> Self {
        Self::LibSql(Arc::new(value))
    }
}

#[cfg(feature = "slatedb")]
impl From<slatedb::Error> for Error {
    fn from(value: slatedb::Error) -> Self {
        Self::Slate(Arc::new(value))
    }
}

#[cfg(feature = "turso")]
impl From<turso::Error> for Error {
    fn from(value: turso::Error) -> Self {
        Self::Turso(Arc::new(value))
    }
}

impl From<GlobError> for Error {
    fn from(value: GlobError) -> Self {
        Self::Glob(Arc::new(value))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

#[cfg(any(feature = "dynostore", feature = "slatedb"))]
impl From<Arc<object_store::Error>> for Error {
    fn from(value: Arc<object_store::Error>) -> Self {
        Self::ObjectStore(value)
    }
}

#[cfg(any(feature = "dynostore", feature = "slatedb"))]
impl From<object_store::Error> for Error {
    fn from(value: object_store::Error) -> Self {
        Self::from(Arc::new(value))
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        Self::ParseFilter(Arc::new(value))
    }
}

impl From<PatternError> for Error {
    fn from(value: PatternError) -> Self {
        Self::Pattern(Arc::new(value))
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::from(Arc::new(value))
    }
}

impl From<Arc<serde_json::Error>> for Error {
    fn from(value: Arc<serde_json::Error>) -> Self {
        Self::SerdeJson(value)
    }
}

#[cfg(feature = "postgres")]
impl From<tokio_postgres::error::Error> for Error {
    fn from(value: tokio_postgres::error::Error) -> Self {
        // Postgres reports SQLSTATE 57014 (query_canceled) when `statement_timeout` aborts a
        // stalled query. That's transient, not a real backend failure, so map it to a
        // retriable Kafka error code the same way a pool-acquisition timeout is (see
        // `From<PoolError<E>>` above) rather than the generic `Error::TokioPostgres`.
        if value
            .code()
            .is_some_and(|code| *code == tokio_postgres::error::SqlState::QUERY_CANCELED)
        {
            Self::Api(ErrorCode::RequestTimedOut)
        } else {
            Self::from(Arc::new(value))
        }
    }
}

#[cfg(feature = "postgres")]
impl From<Arc<tokio_postgres::error::Error>> for Error {
    fn from(value: Arc<tokio_postgres::error::Error>) -> Self {
        Self::TokioPostgres(value)
    }
}

impl From<nisshi_schema::Error> for Error {
    fn from(value: nisshi_schema::Error) -> Self {
        if let nisshi_schema::Error::Api(error_code) = value {
            Self::Api(error_code)
        } else {
            Self::Schema(Arc::new(value))
        }
    }
}

impl From<BoxError> for Error {
    fn from(value: BoxError) -> Self {
        Self::Boxed(Arc::new(value))
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;

/// Topic Partition (topition)
///
/// A topic partition pair.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Topition {
    pub topic: String,
    pub partition: i32,
}

impl Topition {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        let topic = topic.into();
        Self { topic, partition }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }
}

impl From<Cursor> for Topition {
    fn from(value: Cursor) -> Self {
        Self {
            topic: value.topic_name,
            partition: value.partition_index,
        }
    }
}

impl TryFrom<&DirEntry> for Topition {
    type Error = Error;

    fn try_from(value: &DirEntry) -> result::Result<Self, Self::Error> {
        Regex::new(r"^(?<topic>.+)-(?<partition>\d{10})$")
            .map_err(Into::into)
            .and_then(|re| {
                value
                    .file_name()
                    .into_string()
                    .map_err(Error::OsString)
                    .and_then(|ref file_name| {
                        re.captures(file_name)
                            .ok_or(Error::Message(format!("no captures for {file_name}")))
                            .and_then(|ref captures| {
                                let topic = captures
                                    .name("topic")
                                    .ok_or(Error::Message(format!("missing topic for {file_name}")))
                                    .map(|s| s.as_str().to_owned())?;

                                let partition = captures
                                    .name("partition")
                                    .ok_or(Error::Message(format!(
                                        "missing partition for: {file_name}"
                                    )))
                                    .map(|s| s.as_str())
                                    .and_then(|s| str::parse(s).map_err(Into::into))?;

                                Ok(Self { topic, partition })
                            })
                    })
            })
    }
}

impl FromStr for Topition {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        i32::from_str(&s[s.len() - 10..])
            .map(|partition| {
                let topic = String::from(&s[..s.len() - 11]);

                Self { topic, partition }
            })
            .map_err(Into::into)
    }
}

impl From<&Topition> for PathBuf {
    fn from(value: &Topition) -> Self {
        let topic = value.topic.as_str();
        let partition = value.partition;
        PathBuf::from(format!("{topic}-{partition:0>10}"))
    }
}

/// Topic Partition Offset
///
/// A topic partition with an offset.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TopitionOffset {
    topition: Topition,
    offset: i64,
}

impl TopitionOffset {
    pub fn new(topition: Topition, offset: i64) -> Self {
        Self { topition, offset }
    }

    pub fn topition(&self) -> &Topition {
        &self.topition
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }
}

impl From<&TopitionOffset> for PathBuf {
    fn from(value: &TopitionOffset) -> Self {
        let offset = value.offset;
        PathBuf::from(value.topition()).join(format!("{offset:0>20}"))
    }
}

pub type ListOffsetRequest = ListOffset;

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ListOffsetResponse {
    pub error_code: ErrorCode,
    /// The timestamp of the record that a [`ListOffset::Timestamp`] lookup
    /// matched. `ListOffsetsService` sends -1 for [`ListOffset::Earliest`]
    /// and [`ListOffset::Latest`] whatever this holds, as Kafka does, so a
    /// storage engine need not set it for them.
    pub timestamp: Option<SystemTime>,
    /// `None` when a [`ListOffset::Timestamp`] lookup matches no record, or
    /// when `error_code` is not [`ErrorCode::None`]; `ListOffsetsService`
    /// sends `None` as -1, as Kafka does. [`ListOffset::Earliest`] and
    /// [`ListOffset::Latest`] with [`ErrorCode::None`] are always `Some`.
    pub offset: Option<i64>,
}

impl Default for ListOffsetResponse {
    fn default() -> Self {
        Self {
            error_code: ErrorCode::None,
            timestamp: None,
            offset: None,
        }
    }
}

impl ListOffsetResponse {
    pub fn offset(&self) -> Option<i64> {
        self.offset
    }

    pub fn timestamp(&self) -> Result<Option<i64>> {
        self.timestamp.map_or(Ok(None), |system_time| {
            to_timestamp(&system_time).map(Some).map_err(Into::into)
        })
    }

    pub fn error_code(&self) -> ErrorCode {
        self.error_code
    }
}

/// Offset Commit Request
///
/// A structure representing an [`nisshi_sans_io::OffsetCommitRequestPartition](OffsetCommitRequestPartition).
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct OffsetCommitRequest {
    pub offset: i64,
    pub leader_epoch: Option<i32>,
    pub timestamp: Option<SystemTime>,
    pub metadata: Option<String>,
}

impl OffsetCommitRequest {
    pub fn offset(self, offset: i64) -> Self {
        Self { offset, ..self }
    }
}

impl TryFrom<&OffsetCommitRequestPartition> for OffsetCommitRequest {
    type Error = Error;

    fn try_from(value: &OffsetCommitRequestPartition) -> Result<Self, Self::Error> {
        value
            .commit_timestamp
            .map_or(Ok(None), |commit_timestamp| {
                to_system_time(commit_timestamp)
                    .map(Some)
                    .map_err(Into::into)
            })
            .map(|timestamp| Self {
                offset: value.committed_offset,
                leader_epoch: value.committed_leader_epoch,
                timestamp,
                metadata: value.committed_metadata.clone(),
            })
    }
}

/// Committed Offset
///
/// The offset that a consumer group committed for a partition, with the leader
/// epoch and the metadata of that commit.
///
/// The type does not implement `Default`, because an offset of 0 tells a consumer to
/// read the partition from its start. [`CommittedOffset::uncommitted`] is the value for
/// a partition without a committed offset.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CommittedOffset {
    pub offset: i64,
    pub leader_epoch: Option<i32>,
    pub metadata: Option<String>,
}

impl CommittedOffset {
    /// Returns the committed offset of a partition that the group has not
    /// committed: offset -1, with no leader epoch and no metadata.
    pub fn uncommitted() -> Self {
        Self {
            offset: -1,
            leader_epoch: None,
            metadata: None,
        }
    }
}

impl From<OffsetCommitRequest> for CommittedOffset {
    fn from(value: OffsetCommitRequest) -> Self {
        Self {
            offset: value.offset,
            leader_epoch: value.leader_epoch,
            metadata: value.metadata,
        }
    }
}

/// Topic Id
///
/// An enumeration of either the name or UUID of a topic.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TopicId {
    Name(String),
    Id(Uuid),
}

impl FromStr for TopicId {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        Ok(Self::Name(s.into()))
    }
}

impl From<&str> for TopicId {
    fn from(value: &str) -> Self {
        Self::Name(value.to_owned())
    }
}

impl From<String> for TopicId {
    fn from(value: String) -> Self {
        Self::Name(value)
    }
}

impl From<Uuid> for TopicId {
    fn from(value: Uuid) -> Self {
        Self::Id(value)
    }
}

impl From<[u8; 16]> for TopicId {
    fn from(value: [u8; 16]) -> Self {
        Self::Id(Uuid::from_bytes(value))
    }
}

impl From<&TopicId> for [u8; 16] {
    fn from(value: &TopicId) -> Self {
        match value {
            TopicId::Id(id) => id.into_bytes(),
            TopicId::Name(_) => NULL_TOPIC_ID,
        }
    }
}

impl From<&FetchTopic> for TopicId {
    fn from(value: &FetchTopic) -> Self {
        if let Some(ref name) = value.topic {
            Self::Name(name.into())
        } else if let Some(ref id) = value.topic_id {
            Self::Id(Uuid::from_bytes(*id))
        } else {
            // A malformed/adversarial request naming neither a topic nor a
            // topic id: fall back to the nil UUID sentinel already used
            // elsewhere for "no such topic", so storage lookups resolve to
            // an ordinary "not found" instead of panicking.
            Self::Id(Uuid::from_bytes(NULL_TOPIC_ID))
        }
    }
}

impl From<&MetadataRequestTopic> for TopicId {
    fn from(value: &MetadataRequestTopic) -> Self {
        if let Some(ref name) = value.name {
            Self::Name(name.into())
        } else if let Some(ref id) = value.topic_id {
            Self::Id(Uuid::from_bytes(*id))
        } else {
            Self::Id(Uuid::from_bytes(NULL_TOPIC_ID))
        }
    }
}

impl From<DeleteTopicState> for TopicId {
    fn from(value: DeleteTopicState) -> Self {
        match value {
            DeleteTopicState {
                name: Some(name),
                topic_id,
                ..
            } if topic_id == NULL_TOPIC_ID => name.into(),

            DeleteTopicState { topic_id, .. } => topic_id.into(),
        }
    }
}

impl From<&TopicRequest> for TopicId {
    fn from(value: &TopicRequest) -> Self {
        value.name.to_owned().into()
    }
}

impl From<&Topition> for TopicId {
    fn from(value: &Topition) -> Self {
        value.topic.to_owned().into()
    }
}

/// Broker Registration Request
///
/// A broker will register with storage using this structure.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct BrokerRegistrationRequest {
    pub broker_id: i32,
    pub cluster_id: String,
    pub incarnation_id: Uuid,
    pub rack: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct MetadataResponse {
    pub cluster: Option<String>,
    pub controller: Option<i32>,
    pub brokers: Vec<MetadataResponseBroker>,
    pub topics: Vec<MetadataResponseTopic>,
}

impl MetadataResponse {
    pub fn cluster(&self) -> Option<&str> {
        self.cluster.as_deref()
    }

    pub fn controller(&self) -> Option<i32> {
        self.controller
    }

    pub fn brokers(&self) -> &[MetadataResponseBroker] {
        self.brokers.as_ref()
    }

    pub fn topics(&self) -> &[MetadataResponseTopic] {
        self.topics.as_ref()
    }
}

/// Offset Stage
///
/// An offset stage structure represents the `last_stable`, `high_watermark` and `log_start` offsets.
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct OffsetStage {
    pub last_stable: i64,
    pub high_watermark: i64,
    pub log_start: i64,
}

impl OffsetStage {
    pub fn last_stable(&self) -> i64 {
        self.last_stable
    }

    pub fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    pub fn log_start(&self) -> i64 {
        self.log_start
    }
}

/// The `low_watermark` of a `DeleteRecords` partition result that carries an
/// error, as Kafka's `DeleteRecordsResponse.INVALID_LOW_WATERMARK`.
pub const DELETE_RECORDS_INVALID_LOW_WATERMARK: i64 = -1;

/// Validate a client-requested `DeleteRecords` offset against a partition's
/// current [`OffsetStage`], and resolve it to the new low watermark (log
/// start offset).
///
/// Mirrors the validation Kafka applies in `Partition.deleteRecordsOnLeader`
/// / `UnifiedLog.maybeIncrementLogStartOffset`:
///
/// - `requested == -1` means "delete everything up to the high watermark"
///   (what `kafka-delete-records.sh` sends by default): resolves to
///   `stage.high_watermark`.
/// - Any other negative offset, or one above the high watermark, is
///   [`ErrorCode::OffsetOutOfRange`].
/// - An offset at or below the current log start is a no-op: it resolves to
///   the *existing* `stage.log_start`, not the requested value, so the log
///   start never moves backward.
/// - Otherwise the requested offset becomes the new log start.
///
/// Every backend must call this before mutating any watermark or deleting
/// any record/object, so the five storage engines cannot drift from one
/// another on what counts as a valid request.
pub fn delete_records_cutoff(requested: i64, stage: &OffsetStage) -> Result<i64, ErrorCode> {
    if requested == -1 {
        return Ok(stage.high_watermark);
    }

    if requested < -1 || requested > stage.high_watermark {
        return Err(ErrorCode::OffsetOutOfRange);
    }

    if requested <= stage.log_start {
        return Ok(stage.log_start);
    }

    Ok(requested)
}

/// Group Member
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct GroupMember {
    pub join_response: JoinGroupResponseMember,
    pub last_contact: Option<SystemTime>,
    /// The client id in the header of the member's last `JoinGroup`. It is empty for a member
    /// whose stored group lacks the field, until the member joins again.
    #[serde(default)]
    pub client_id: String,
    /// The address that the member's last `JoinGroup` came from, as Kafka writes it: `/` and the
    /// IP address. It is empty for a member whose stored group lacks the field, until the member
    /// joins again.
    #[serde(default)]
    pub client_host: String,
}

/// Group State
///
/// A group is either in the process of [`GroupState::Forming`] or has [`GroupState::Formed`].
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum GroupState {
    Forming {
        protocol_type: Option<String>,
        protocol_name: Option<String>,
        leader: Option<String>,
    },

    Formed {
        protocol_type: String,
        protocol_name: String,
        leader: String,
        assignments: BTreeMap<String, Bytes>,
    },
}

impl GroupState {
    pub fn protocol_type(&self) -> Option<String> {
        match self {
            Self::Forming { protocol_type, .. } => protocol_type.clone(),
            Self::Formed { protocol_type, .. } => Some(protocol_type.clone()),
        }
    }

    pub fn protocol_name(&self) -> Option<String> {
        match self {
            Self::Forming { protocol_name, .. } => protocol_name.clone(),
            Self::Formed { protocol_name, .. } => Some(protocol_name.clone()),
        }
    }

    pub fn leader(&self) -> Option<String> {
        match self {
            Self::Forming { leader, .. } => leader.clone(),
            Self::Formed { leader, .. } => Some(leader.clone()),
        }
    }

    pub fn assignments(&self) -> BTreeMap<String, Bytes> {
        match self {
            Self::Forming { .. } => BTreeMap::new(),
            Self::Formed { assignments, .. } => assignments.clone(),
        }
    }
}

impl Default for GroupState {
    fn default() -> Self {
        Self::Forming {
            protocol_type: None,
            protocol_name: Some("".into()),
            leader: None,
        }
    }
}

/// Consumer Group State
///
/// A helper type for conversion into [`consumer_group_describe_response::DescribedGroup`].
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum ConsumerGroupState {
    Unknown,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
    Empty,
    Assigning,
    Reconciling,
}

impl Display for ConsumerGroupState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => f.write_str("Unknown"),
            Self::PreparingRebalance => f.write_str("PreparingRebalance"),
            Self::CompletingRebalance => f.write_str("CompletingRebalance"),
            Self::Stable => f.write_str("Stable"),
            Self::Dead => f.write_str("Dead"),
            Self::Empty => f.write_str("Empty"),
            Self::Assigning => f.write_str("Assigning"),
            Self::Reconciling => f.write_str("Reconciling"),
        }
    }
}

/// Group Detail
///
/// A helper type that can be easily converted into [`consumer_group_describe_response::DescribedGroup`].
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct GroupDetail {
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: Option<i32>,
    pub members: BTreeMap<String, GroupMember>,
    pub generation_id: i32,
    pub skip_assignment: Option<bool>,
    pub inception: SystemTime,
    pub state: GroupState,
    /// A group stored before this field existed has no rebalance in progress. The field is
    /// boxed to keep `GroupDetail` small in the enums that carry it.
    #[serde(default)]
    pub rebalance: Box<GroupRebalance>,
}

impl Default for GroupDetail {
    fn default() -> Self {
        Self {
            session_timeout_ms: 45_000,
            rebalance_timeout_ms: Default::default(),
            members: Default::default(),
            generation_id: Default::default(),
            skip_assignment: Some(false),
            inception: SystemTime::now(),
            state: GroupState::default(),
            rebalance: Box::default(),
        }
    }
}

/// The progress of a group's rebalance, which the group coordinator stores with the group so that
/// any broker sharing the storage can coordinate the group.
///
/// Kafka keeps this state in the memory of the one broker that coordinates the group. Nisshi has
/// no such broker, so each deadline is a wall-clock time that every broker can compare with its own
/// clock.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct GroupRebalance {
    /// The join phase in progress, which Kafka calls `PreparingRebalance`.
    pub join_phase: Option<JoinPhase>,
    /// The members that must still send `SyncGroup` for the current generation.
    pub pending_sync: Option<PendingSync>,
    /// The member ids that `MEMBER_ID_REQUIRED` gave out and that have not joined yet, each with
    /// the time after which the coordinator forgets it.
    pub pending_members: BTreeMap<String, SystemTime>,
}

/// A join phase: the coordinator holds each `JoinGroup` until the phase ends.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct JoinPhase {
    /// When the phase ends whether or not every member has rejoined.
    pub deadline: SystemTime,
    /// The members that have joined in this phase.
    pub joined: BTreeSet<String>,
    /// Set while the group forms from empty, when the phase lasts for a delay instead of ending
    /// once every member has joined.
    pub initial_delay: Option<InitialDelay>,
}

/// The delay of a join phase that forms a group from empty, as Kafka's
/// `group.initial.rebalance.delay.ms` gives it.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct InitialDelay {
    /// The length of the delay that ends at the phase's deadline.
    pub delay_ms: u64,
    /// How much longer than the deadline the phase may still be extended.
    pub remaining_ms: u64,
    /// Whether a new member joined during the delay that ends at the deadline.
    pub new_member_added: bool,
}

/// The members that must still send `SyncGroup`, and when the coordinator removes those that have
/// not.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PendingSync {
    pub deadline: SystemTime,
    pub members: BTreeSet<String>,
}

impl ConsumerGroupState {
    /// Returns whether `states_filter` matches this state. The filter is the list of state names
    /// in a `ListGroups` request. As in Kafka, a missing or empty filter matches every state,
    /// and each name in the filter matches without regard to case or surrounding white space.
    pub fn matches(&self, states_filter: Option<&[String]>) -> bool {
        states_filter.is_none_or(|states_filter| {
            let state = self.to_string();

            states_filter.is_empty()
                || states_filter
                    .iter()
                    .any(|name| name.trim().eq_ignore_ascii_case(&state))
        })
    }
}

/// A classic group's state in Kafka, from the group's stored detail.
impl From<&GroupDetail> for ConsumerGroupState {
    fn from(value: &GroupDetail) -> Self {
        match value {
            // A join phase continues after every member sends LeaveGroup during the initial delay,
            // and the group stays in PreparingRebalance until the phase ends, as in Kafka.
            GroupDetail {
                state: GroupState::Forming { .. },
                rebalance,
                ..
            } if rebalance.join_phase.is_some() => Self::PreparingRebalance,

            GroupDetail { members, .. } if members.is_empty() => Self::Empty,

            GroupDetail {
                state: GroupState::Formed { .. },
                ..
            } => Self::Stable,

            GroupDetail {
                state: GroupState::Forming { .. },
                rebalance,
                ..
            } if rebalance.pending_sync.is_some() => Self::CompletingRebalance,

            // A broker that stored the group without its rebalance state held each request for a
            // fixed time. The group coordinator gives such a group a join phase, so it is in
            // PreparingRebalance.
            GroupDetail {
                state: GroupState::Forming { .. },
                ..
            } => Self::PreparingRebalance,
        }
    }
}

/// Returns the `ConsumerGroupDescribe` row for the stored group `group_id`.
///
/// `ConsumerGroupDescribe` describes only a group of the KIP-848 consumer protocol, whose members
/// send `ConsumerGroupHeartbeat`. Nisshi forms each group through `JoinGroup` and `SyncGroup`, so
/// each group is a classic group. This function answers `GROUP_ID_NOT_FOUND` for it, as Kafka's
/// group coordinator does for a classic group ([`GroupMetadataManager.consumerGroup`]), and an
/// admin client then describes the group with `DescribeGroups`. If the broker answered `NONE`
/// with an empty member list, the admin client would not fall back to `DescribeGroups`, and it
/// would report the group without members.
///
/// [`GroupMetadataManager.consumerGroup`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java
fn classic_group_consumer_group_describe_row(
    group_id: String,
) -> consumer_group_describe_response::DescribedGroup {
    let error_message = format!("Group {group_id} is not a consumer group.");
    consumer_group_describe_error_row(group_id, ErrorCode::GroupIdNotFound, error_message)
}

/// Returns the `ConsumerGroupDescribe` row that answers `error_code` for the group `group_id`.
fn consumer_group_describe_error_row(
    group_id: String,
    error_code: ErrorCode,
    error_message: String,
) -> consumer_group_describe_response::DescribedGroup {
    consumer_group_describe_response::DescribedGroup::default()
        .error_code(error_code.into())
        .error_message(Some(error_message))
        .group_id(group_id)
        .group_state(ConsumerGroupState::Unknown.to_string())
        .group_epoch(-1)
        .assignment_epoch(-1)
        .assignor_name("".into())
        .members(Some([].into()))
        .authorized_operations(authorized_operations::OMITTED)
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum GroupDetailResponse {
    ErrorCode(ErrorCode),
    Found(GroupDetail),
}

/// NamedGroupDetail
///
/// A helper type that can be easily converted into [`consumer_group_describe_response::DescribedGroup`]
/// or [`describe_groups_response::DescribedGroup`].
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct NamedGroupDetail {
    pub name: String,
    pub response: GroupDetailResponse,
}

impl NamedGroupDetail {
    pub fn error_code(name: String, error_code: ErrorCode) -> Self {
        Self {
            name,
            response: GroupDetailResponse::ErrorCode(error_code),
        }
    }

    pub fn found(name: String, found: GroupDetail) -> Self {
        Self {
            name,
            response: GroupDetailResponse::Found(found),
        }
    }

    /// Returns a group from its stored detail, as the storage read and decoded it. A group
    /// without a stored detail has only committed offsets, so it is empty. A group whose detail
    /// can't be read or decoded has an error instead of a detail, so that a list of groups still
    /// includes the other groups.
    pub fn decoded<E>(name: String, stored: Option<Result<GroupDetail, E>>) -> Self
    where
        E: Debug,
    {
        match stored {
            Some(Ok(detail)) => Self::found(name, detail),
            Some(Err(err)) => {
                error!(?err, name, "the stored group can't be read or decoded");
                Self::error_code(name, ErrorCode::UnknownServerError)
            }
            None => Self::found(name, GroupDetail::default()),
        }
    }
}

impl From<&NamedGroupDetail> for consumer_group_describe_response::DescribedGroup {
    fn from(value: &NamedGroupDetail) -> Self {
        match &value.response {
            GroupDetailResponse::Found(_) => {
                classic_group_consumer_group_describe_row(value.name.clone())
            }

            GroupDetailResponse::ErrorCode(error_code) => consumer_group_describe_error_row(
                value.name.clone(),
                *error_code,
                error_code.to_string(),
            ),
        }
    }
}

impl From<&NamedGroupDetail> for describe_groups_response::DescribedGroup {
    fn from(value: &NamedGroupDetail) -> Self {
        match value {
            NamedGroupDetail {
                name,
                response: GroupDetailResponse::Found(group_detail),
            } => {
                let group_state = ConsumerGroupState::from(group_detail);

                // Kafka reports the group's protocol, and each member's metadata and assignment,
                // only for a Stable group ([`GroupMetadata.summary`]). In another state, the
                // members' metadata may be for a protocol the group has not selected yet.
                //
                // [`GroupMetadata.summary`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/coordinator/group/GroupMetadata.scala
                let is_stable = group_state == ConsumerGroupState::Stable;
                let assignments = group_detail.state.assignments();

                let members = group_detail
                    .members
                    .iter()
                    .map(|(member_id, member)| {
                        let (member_metadata, member_assignment) = if is_stable {
                            (
                                member.join_response.metadata.clone(),
                                assignments.get(member_id).cloned().unwrap_or_default(),
                            )
                        } else {
                            (Bytes::new(), Bytes::new())
                        };

                        describe_groups_response::DescribedGroupMember::default()
                            .member_id(member_id.into())
                            .group_instance_id(member.join_response.group_instance_id.clone())
                            .client_id(member.client_id.clone())
                            .client_host(member.client_host.clone())
                            .member_metadata(member_metadata)
                            .member_assignment(member_assignment)
                    })
                    .collect::<Vec<_>>();

                let protocol_data = if is_stable {
                    group_detail.state.protocol_name().unwrap_or_default()
                } else {
                    String::new()
                };

                Self::default()
                    .error_code(ErrorCode::None.into())
                    .group_id(name.clone())
                    .group_state(group_state.to_string())
                    .protocol_type(group_detail.state.protocol_type().unwrap_or_default())
                    .protocol_data(protocol_data)
                    .members(Some(members))
                    .authorized_operations(Some(authorized_operations::OMITTED))
            }

            // `DescribeGroups` v0 to v5 describes a group that does not exist with the error
            // NONE and the state `Dead`. Kafka adds `GROUP_ID_NOT_FOUND` to `DescribeGroups` from
            // v6 ([KIP-1043]), and Nisshi supports v5 at most. An admin client reads the group
            // as missing from `Dead`, but fails on `GROUP_ID_NOT_FOUND` as an unexpected error
            // ([`GroupCoordinator.handleDescribeGroup`]).
            //
            // [KIP-1043]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-1043%3A+Administration+of+groups
            // [`GroupCoordinator.handleDescribeGroup`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/coordinator/group/GroupCoordinator.scala
            NamedGroupDetail {
                name,
                response: GroupDetailResponse::ErrorCode(ErrorCode::GroupIdNotFound),
            } => Self::default()
                .error_code(ErrorCode::None.into())
                .group_id(name.clone())
                .group_state(ConsumerGroupState::Dead.to_string())
                .protocol_type("".into())
                .protocol_data("".into())
                .members(Some(vec![]))
                .authorized_operations(Some(authorized_operations::OMITTED)),

            NamedGroupDetail {
                name,
                response: GroupDetailResponse::ErrorCode(error_code),
            } => Self::default()
                .error_code((*error_code).into())
                .group_id(name.clone())
                .group_state(ConsumerGroupState::from(value).to_string())
                .protocol_type("".into())
                .protocol_data("".into())
                .members(Some(vec![]))
                .authorized_operations(Some(authorized_operations::OMITTED)),
        }
    }
}

/// A group's state from its stored detail. A group whose detail the storage could not read is in
/// the `Unknown` state.
impl From<&NamedGroupDetail> for ConsumerGroupState {
    fn from(value: &NamedGroupDetail) -> Self {
        match &value.response {
            GroupDetailResponse::Found(group_detail) => Self::from(group_detail),
            GroupDetailResponse::ErrorCode(_) => Self::Unknown,
        }
    }
}

impl From<&NamedGroupDetail> for ListedGroup {
    fn from(value: &NamedGroupDetail) -> Self {
        Self::default()
            .group_id(value.name.clone())
            .protocol_type("consumer".into())
            .group_state(Some(ConsumerGroupState::from(value).to_string()))
            .group_type(Some("classic".into()))
    }
}

/// Topition (topic partition) Detail
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TopitionDetail {
    error: ErrorCode,
    topic: TopicId,
    partitions: Option<Vec<PartitionDetail>>,
}

/// Partition Detail
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PartitionDetail {
    error: ErrorCode,
    partition_index: i32,
}

/// Version representing an `e_tag` and `version` used in conditional writes to an object store.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Version {
    pub e_tag: Option<String>,
    pub version: Option<String>,
}

impl From<&Uuid> for Version {
    fn from(value: &Uuid) -> Self {
        Self {
            e_tag: Some(value.to_string()),
            version: None,
        }
    }
}

/// Producer Id Response
#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ProducerIdResponse {
    pub error: ErrorCode,
    pub id: i64,
    pub epoch: i16,
}

impl Default for ProducerIdResponse {
    fn default() -> Self {
        Self {
            error: ErrorCode::None,
            id: 1,
            epoch: 0,
        }
    }
}

/// For protocol versions 0..=3 using [`AddPartitionsToTxnTopic`],
/// thereafter using [`AddPartitionsToTxnTransaction`].
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TxnAddPartitionsRequest {
    VersionZeroToThree {
        transaction_id: String,
        producer_id: i64,
        producer_epoch: i16,
        topics: Vec<AddPartitionsToTxnTopic>,
    },

    VersionFourPlus {
        transactions: Vec<AddPartitionsToTxnTransaction>,
    },
}

impl TryFrom<AddPartitionsToTxnRequest> for TxnAddPartitionsRequest {
    type Error = Error;

    fn try_from(value: AddPartitionsToTxnRequest) -> result::Result<Self, Self::Error> {
        match value {
            AddPartitionsToTxnRequest {
                transactions: None,
                v_3_and_below_transactional_id: Some(transactional_id),
                v_3_and_below_producer_id: Some(producer_id),
                v_3_and_below_producer_epoch: Some(producer_epoch),
                v_3_and_below_topics: Some(topics),
                ..
            } => Ok(Self::VersionZeroToThree {
                transaction_id: transactional_id,
                producer_id,
                producer_epoch,
                topics,
            }),

            AddPartitionsToTxnRequest {
                transactions: Some(transactions),
                v_3_and_below_transactional_id: None,
                v_3_and_below_producer_id: None,
                v_3_and_below_producer_epoch: None,
                v_3_and_below_topics: None,
                ..
            } => Ok(Self::VersionFourPlus { transactions }),

            unexpected => Err(Error::UnexpectedAddPartitionsToTxnRequest(Box::new(
                unexpected,
            ))),
        }
    }
}

/// Transaction Add Partitions Response
///
/// For protocol versions 0..=3 using `AddPartitionsToTxnTopic`, thereafter using `AddPartitionsToTxnTransaction`.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TxnAddPartitionsResponse {
    VersionZeroToThree(Vec<AddPartitionsToTxnTopicResult>),
    VersionFourPlus(Vec<AddPartitionsToTxnResult>),
}

impl TxnAddPartitionsResponse {
    pub fn zero_to_three(&self) -> &[AddPartitionsToTxnTopicResult] {
        match self {
            Self::VersionZeroToThree(result) => result.as_slice(),
            Self::VersionFourPlus(_) => &[][..],
        }
    }

    pub fn four_plus(&self) -> &[AddPartitionsToTxnResult] {
        match self {
            Self::VersionZeroToThree(_) => &[][..],
            Self::VersionFourPlus(result) => result.as_slice(),
        }
    }
}

/// Transaction Offset Commit Request
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TxnOffsetCommitRequest {
    pub transaction_id: String,
    pub group_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub generation_id: Option<i32>,
    pub member_id: Option<String>,
    pub group_instance_id: Option<String>,
    pub topics: Vec<TxnOffsetCommitRequestTopic>,
}

/// Transaction State
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum TxnState {
    Begin,
    PrepareCommit,
    PrepareAbort,
    Committed,
    Aborted,
}

impl TxnState {
    pub fn is_prepared(&self) -> bool {
        match self {
            Self::PrepareAbort | Self::PrepareCommit => true,
            _otherwise => false,
        }
    }

    /// Whether the transaction has finished: it committed or aborted, and its
    /// markers are written.
    pub fn is_finished(&self) -> bool {
        matches!(self, Self::Committed | Self::Aborted)
    }
}

impl FromStr for TxnState {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ABORTED" => Ok(TxnState::Aborted),
            "BEGIN" => Ok(TxnState::Begin),
            "COMMITTED" => Ok(TxnState::Committed),
            "PREPARE_ABORT" => Ok(TxnState::PrepareAbort),
            "PREPARE_COMMIT" => Ok(TxnState::PrepareCommit),
            otherwise => Err(Error::UnknownTxnState(otherwise.to_owned())),
        }
    }
}

impl TryFrom<String> for TxnState {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<TxnState> for String {
    fn from(value: TxnState) -> Self {
        match value {
            TxnState::Begin => "BEGIN".into(),
            TxnState::PrepareCommit => "PREPARE_COMMIT".into(),
            TxnState::PrepareAbort => "PREPARE_ABORT".into(),
            TxnState::Committed => "COMMITTED".into(),
            TxnState::Aborted => "ABORTED".into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct StorageFactoryConfiguration {
    pub node_id: i32,
    pub cluster: String,
    pub advertised_listener: Url,
    pub storage: Url,
    pub schema_registry: Option<Registry>,
    pub lake_house: Option<House>,
    pub cancellation: CancellationToken,
}

/// What a storage engine can do beyond the core [`Storage`] methods, so that the broker can
/// refuse a configuration the engine can't serve before it starts, instead of failing request
/// by request.
///
/// A new capability is a new field. An engine with no capability returns [`Default`]. An
/// engine with any lists its capabilities in full, so adding a field makes the compiler name
/// each engine that has to decide about it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StorageCapabilities {
    /// Whether the engine stores SCRAM credentials, so that a broker on it can require
    /// authentication. An engine that can't must fail
    /// [`Storage::upsert_user_scram_credential`] and [`Storage::delete_user_scram_credential`]
    /// instead of reporting success.
    pub user_credentials: bool,
}

/// A storage engine's factory.
///
/// [`Display`] is the engine's name for people, with its URL scheme, e.g.
/// `PostgreSQL (postgres://)`.
#[async_trait]
pub trait StorageFactory: Debug + Display + Send + Sync + 'static {
    fn scheme(&self) -> Result<Regex>;

    /// What the engine can do. See [`StorageCapabilities`].
    fn capabilities(&self) -> StorageCapabilities;

    /// Returns the notice that [`Builder::build`] logs as a warning when this factory is
    /// selected, or `None` for an engine that is not deprecated.
    ///
    /// The notice is logged as it is written, so it must not hold a storage URL, which
    /// can carry credentials.
    fn deprecation(&self) -> Option<&'static str> {
        None
    }

    async fn build(&self, configuration: StorageFactoryConfiguration) -> Result<ArcDynStorage>;
}

pub type DynStorageFactory = dyn StorageFactory;
pub type ArcDynStorageFactory = Arc<DynStorageFactory>;

// The existence of this function makes the compiler catch if the StorageFactory
// trait is "object-safe" or not.
fn _assert_storage_factory_trait_object(_s: &dyn StorageFactory) {}

/// Storage
///
/// The Core storage abstraction. All storage engines implement this type.
#[async_trait]
pub trait Storage: Debug + Send + Sync + 'static {
    /// On startup a broker will register with storage.
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()>;

    /// Create a topic on this storage.
    ///
    /// An implementation creates the topic whatever `validate_only` says.
    /// [`CreateTopicsService`] answers a `validate_only` request itself and
    /// calls this method with `false`.
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid>;

    /// Incrementally alter a resource on this storage.
    ///
    /// A topic resource applies all of its changes or none of them. Its
    /// `APPEND` and `SUBTRACT` changes follow [`config::apply_op`].
    ///
    /// [`IncrementalAlterConfigsService`] checks each resource the way Kafka
    /// does before it calls this method. A caller that skips the service gets
    /// only the checks that `apply_op` makes.
    ///
    /// # Errors
    ///
    /// - [`Error::Api`] with [`ErrorCode::UnknownTopicOrPartition`] for a topic
    ///   that doesn't exist;
    /// - [`Error::Api`] with [`ErrorCode::InvalidConfig`] for `APPEND` or
    ///   `SUBTRACT` on a key that isn't a list;
    /// - any other error when storage fails, or when a change has an unknown
    ///   operation.
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse>;

    /// Delete records on this storage.
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>>;

    /// Delete a topic from this storage.
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode>;

    /// Query the brokers registered with this storage.
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>>;

    /// Produce a deflated batch to this storage.
    ///
    /// Trusts the batch attributes as given: a batch with the control bit set
    /// is written as a transaction marker, skipping schema validation and lake
    /// writes. Only the broker may write control batches, so client Produce
    /// batches must reach storage through [`ProduceService`], which rejects
    /// them.
    ///
    /// A backend that decodes the batch does so with [`inflate_produced`], so
    /// records that do not decode fail with `INVALID_RECORD`.
    ///
    /// A backend returns [`Error::MessageTooLarge`] when [`max_message_bytes::ensure_batch_fits`]
    /// rejects the batch. The backend must return that error before it changes any state.
    /// [`ProduceRequestBatcher`] then writes the same records again, one batch at a time.
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64>;

    /// Fetch deflated batches from storage.
    ///
    /// The batches hold at most `max_bytes`, except that a read can exceed it by
    /// one batch. A read returns the first batch at or after `offset` even when
    /// that batch alone is larger than `max_bytes`, so that a consumer gets past
    /// it (KIP-74). The fetch service relies on this: without it, a consumer
    /// whose fetch limit is smaller than a record never reads past that record.
    ///
    /// For a topic or partition that does not exist, see
    /// [`Storage::offset_stage`].
    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>>;

    /// Query the offset stage for a topic partition.
    ///
    /// The partition index comes from the client, so an engine must not
    /// keep state for a partition that does not exist: a client can send
    /// any number of indexes.
    ///
    /// # Errors
    ///
    /// An engine should return [`Error::Api`] with
    /// [`ErrorCode::UnknownTopicOrPartition`] for a topic or partition that
    /// does not exist. Fetch reads the metadata before storage, so it sees
    /// this error only when the topic is deleted, or recreated with fewer
    /// partitions, in between, and it then answers the partition with that
    /// error in the response. Any other error closes the client's
    /// connection.
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage>;

    /// Query the offsets for one or more topic partitions. See
    /// [`ListOffsetResponse::offset`] for when an answer has no offset.
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>>;

    /// Commit offsets for one or more topic partitions in a consumer group.
    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>>;

    /// Fetch committed offsets for one or more topic partitions in a consumer group.
    ///
    /// The group coordinator answers `require_stable` with
    /// [`Storage::pending_txn_offsets`], so an engine may ignore it here.
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, CommittedOffset>>;

    /// Fetch all committed offsets in a consumer group.
    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>>;

    /// Returns the partitions for which an open transaction has committed an offset of the
    /// consumer group with `TxnOffsetCommit`, and has not yet ended.
    ///
    /// The current epoch of each transactional producer counts, and an earlier epoch only
    /// while its end is prepared and waits behind an older open transaction: an
    /// `InitProducerId` that moves the producer to a new epoch ends its earlier open
    /// transaction in Kafka. An `OffsetFetch` with `require_stable` answers
    /// `UNSTABLE_OFFSET_COMMIT` for each of these partitions.
    ///
    /// A transaction that only commits offsets, with no `AddPartitionsToTxn`, is not timed
    /// out: its partitions stay unstable until its `EndTxn`, or until an `InitProducerId`
    /// moves its producer to a new epoch.
    async fn pending_txn_offsets(&self, group_id: &str) -> Result<BTreeSet<Topition>>;

    /// Query broker and topic metadata.
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse>;

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()>;

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()>;

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>>;

    /// Query the configuration of a resource in this storage.
    ///
    /// [`DescribeConfigsService`] answers a [`ConfigResource::Broker`] resource itself, so an
    /// implementation needs to answer only the other resource types.
    ///
    /// For a topic, an implementation returns only the configs that the topic sets, and answers
    /// [`ErrorCode::UnknownTopicOrPartition`] for a topic that it doesn't store.
    /// [`DescribeConfigsService`] adds Kafka's default for each other topic config when the error
    /// code is [`ErrorCode::None`]. The broker also calls this method, and reads a config that the
    /// topic doesn't set as Nisshi's own default.
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult>;

    /// Returns every group in this storage with its stored detail. A group without a stored
    /// detail (e.g. a group with only committed offsets) has the default detail, which is empty.
    async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>>;

    /// Delete one or more groups from storage.
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>>;

    /// Describe the groups found in this storage.
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>>;

    /// Describe the topic partitions found in this storage.
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>>;

    /// Returns the stored state of a group and the version that a following
    /// [`Storage::update_group`] must pass, or `None` when no state is stored for the
    /// group.
    ///
    /// The read does not change the stored version, so the returned version stays
    /// valid until the stored group changes.
    async fn group_with_version(&self, group_id: &str) -> Result<Option<(GroupDetail, Version)>>;

    /// Conditionally update the state of a group in this storage.
    ///
    /// `version` is the version of the group the caller last read. `None`
    /// only succeeds when no state is stored for the group yet, and a version
    /// for a stored group only succeeds when it matches the stored version.
    /// Otherwise the update fails with [`UpdateError::Outdated`], carrying the
    /// stored group and its version for the caller to retry against.
    ///
    /// Backends do not yet agree on a version for a group with no stored
    /// state: some accept it, others report the group as outdated.
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>>;

    /// Initialise a transactional or idempotent producer in this storage.
    ///
    /// `producer_id` and `producer_epoch` are fresh or a claim, as [`producer_claim`]
    /// reads them. A backend checks a claim with [`check_claim`] when the transactional
    /// ID has a producer, and ignores it otherwise, as Kafka does.
    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse>;

    /// Add offsets to a transaction for a producer.
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode>;

    /// Add partitions to a transaction.
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse>;

    /// Commit an offset within a transaction.
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>>;

    /// Commit or abort a running transaction.
    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode>;

    /// Run periodic maintenance on this storage.
    async fn maintain(&self, _now: SystemTime) -> Result<()>;

    /// Abort transactions whose timeout has elapsed, releasing the Last Stable Offset they pin.
    async fn maintain_transactions(&self, _now: SystemTime) -> Result<()>;

    /// Aborted transactions overlapping `[offset, last_stable_offset)`, reporting which producer
    /// ranges a `read_committed` Fetch client should discard. No engine implements this yet:
    /// every backend, Postgres included, returns `Ok(vec![])` (wrapper layers such as the proxy
    /// forward to their inner storage), so a `read_committed` consumer would read aborted records.
    async fn aborted_transactions(
        &self,
        _topition: &Topition,
        _offset: i64,
        _last_stable_offset: i64,
    ) -> Result<Vec<AbortedTransaction>>;

    async fn cluster_id(&self) -> Result<String>;

    async fn node(&self) -> Result<i32>;

    async fn advertised_listener(&self) -> Result<Url>;

    async fn ping(&self) -> Result<()>;
}

// The existence of this function makes the compiler catch if the Storage
// trait is "object-safe" or not.
fn _assert_trait_object(_s: &dyn Storage) {}

#[async_trait]
impl<T> Storage for Arc<T>
where
    T: Storage + ?Sized,
{
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        self.as_ref().register_broker(broker_registration).await
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        self.as_ref().create_topic(topic, validate_only).await
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        self.as_ref().incremental_alter_resource(resource).await
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        self.as_ref().delete_records(topics).await
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        self.as_ref().delete_topic(topic).await
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        self.as_ref().brokers().await
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        self.as_ref().produce(transaction_id, topition, batch).await
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        self.as_ref()
            .fetch(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            .await
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        self.as_ref().offset_stage(topition).await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        self.as_ref().list_offsets(isolation_level, offsets).await
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        self.as_ref()
            .offset_commit(group_id, retention_time_ms, offsets)
            .await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        self.as_ref()
            .offset_fetch(group_id, topics, require_stable)
            .await
    }

    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        self.as_ref().committed_offset_topitions(group_id).await
    }

    async fn pending_txn_offsets(&self, group_id: &str) -> Result<BTreeSet<Topition>> {
        self.as_ref().pending_txn_offsets(group_id).await
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        self.as_ref().metadata(topics).await
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        self.as_ref()
            .upsert_user_scram_credential(user, mechanism, credential)
            .await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        self.as_ref()
            .delete_user_scram_credential(user, mechanism)
            .await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        self.as_ref().user_scram_credential(user, mechanism).await
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        self.as_ref().describe_config(name, resource, keys).await
    }

    async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
        self.as_ref().list_groups().await
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        self.as_ref().delete_groups(group_ids).await
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        self.as_ref()
            .describe_groups(group_ids, include_authorized_operations)
            .await
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        self.as_ref()
            .describe_topic_partitions(topics, partition_limit, cursor)
            .await
    }

    async fn group_with_version(&self, group_id: &str) -> Result<Option<(GroupDetail, Version)>> {
        self.as_ref().group_with_version(group_id).await
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        self.as_ref().update_group(group_id, detail, version).await
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        self.as_ref()
            .init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            )
            .await
    }

    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        self.as_ref()
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        self.as_ref().txn_add_partitions(partitions).await
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.as_ref().txn_offset_commit(offsets).await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        self.as_ref()
            .txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
    }

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        self.as_ref().maintain(now).await
    }

    async fn maintain_transactions(&self, now: SystemTime) -> Result<()> {
        self.as_ref().maintain_transactions(now).await
    }

    async fn aborted_transactions(
        &self,
        topition: &Topition,
        offset: i64,
        last_stable_offset: i64,
    ) -> Result<Vec<AbortedTransaction>> {
        self.as_ref()
            .aborted_transactions(topition, offset, last_stable_offset)
            .await
    }

    async fn cluster_id(&self) -> Result<String> {
        self.as_ref().cluster_id().await
    }

    async fn node(&self) -> Result<i32> {
        self.as_ref().node().await
    }

    async fn advertised_listener(&self) -> Result<Url> {
        self.as_ref().advertised_listener().await
    }

    async fn ping(&self) -> Result<()> {
        self.as_ref().ping().await
    }
}

#[async_trait]
impl<T> Storage for Box<T>
where
    T: Storage + ?Sized,
{
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        self.as_ref().register_broker(broker_registration).await
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        self.as_ref().create_topic(topic, validate_only).await
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        self.as_ref().incremental_alter_resource(resource).await
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        self.as_ref().delete_records(topics).await
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        self.as_ref().delete_topic(topic).await
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        self.as_ref().brokers().await
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> Result<i64> {
        self.as_ref().produce(transaction_id, topition, batch).await
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        self.as_ref()
            .fetch(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            .await
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        self.as_ref().offset_stage(topition).await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        self.as_ref().list_offsets(isolation_level, offsets).await
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        self.as_ref()
            .offset_commit(group_id, retention_time_ms, offsets)
            .await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        self.as_ref()
            .offset_fetch(group_id, topics, require_stable)
            .await
    }

    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        self.as_ref().committed_offset_topitions(group_id).await
    }

    async fn pending_txn_offsets(&self, group_id: &str) -> Result<BTreeSet<Topition>> {
        self.as_ref().pending_txn_offsets(group_id).await
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        self.as_ref().metadata(topics).await
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        self.as_ref()
            .upsert_user_scram_credential(user, mechanism, credential)
            .await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        self.as_ref()
            .delete_user_scram_credential(user, mechanism)
            .await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        self.as_ref().user_scram_credential(user, mechanism).await
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        self.as_ref().describe_config(name, resource, keys).await
    }

    async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
        self.as_ref().list_groups().await
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        self.as_ref().delete_groups(group_ids).await
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        self.as_ref()
            .describe_groups(group_ids, include_authorized_operations)
            .await
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        self.as_ref()
            .describe_topic_partitions(topics, partition_limit, cursor)
            .await
    }

    async fn group_with_version(&self, group_id: &str) -> Result<Option<(GroupDetail, Version)>> {
        self.as_ref().group_with_version(group_id).await
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        self.as_ref().update_group(group_id, detail, version).await
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        self.as_ref()
            .init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            )
            .await
    }

    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        self.as_ref()
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        self.as_ref().txn_add_partitions(partitions).await
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.as_ref().txn_offset_commit(offsets).await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        self.as_ref()
            .txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
    }

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        self.as_ref().maintain(now).await
    }

    async fn maintain_transactions(&self, now: SystemTime) -> Result<()> {
        self.as_ref().maintain_transactions(now).await
    }

    async fn aborted_transactions(
        &self,
        topition: &Topition,
        offset: i64,
        last_stable_offset: i64,
    ) -> Result<Vec<AbortedTransaction>> {
        self.as_ref()
            .aborted_transactions(topition, offset, last_stable_offset)
            .await
    }

    async fn cluster_id(&self) -> Result<String> {
        self.as_ref().cluster_id().await
    }

    async fn node(&self) -> Result<i32> {
        self.as_ref().node().await
    }

    async fn advertised_listener(&self) -> Result<Url> {
        self.as_ref().advertised_listener().await
    }

    async fn ping(&self) -> Result<()> {
        self.as_ref().ping().await
    }
}

pub type DynStorage = dyn Storage;
pub type ArcDynStorage = Arc<DynStorage>;

/// Conditional Update Errors
#[derive(Clone, Debug, thiserror::Error)]
pub enum UpdateError<T> {
    Error(#[from] Error),

    MissingEtag,

    Outdated { current: Box<T>, version: Version },

    SerdeJson(Arc<serde_json::Error>),

    Uuid(#[from] uuid::Error),
}

#[cfg(feature = "libsql")]
impl<T> From<libsql::Error> for UpdateError<T> {
    fn from(value: libsql::Error) -> Self {
        Self::Error(Error::from(value))
    }
}

#[cfg(feature = "turso")]
impl<T> From<turso::Error> for UpdateError<T> {
    fn from(value: turso::Error) -> Self {
        Self::Error(Error::from(value))
    }
}

#[cfg(any(feature = "dynostore", feature = "slatedb"))]
impl<T> From<object_store::Error> for UpdateError<T> {
    fn from(value: object_store::Error) -> Self {
        Self::Error(Error::from(value))
    }
}

impl<T> From<serde_json::Error> for UpdateError<T> {
    fn from(value: serde_json::Error) -> Self {
        Self::SerdeJson(Arc::new(value))
    }
}

#[cfg(feature = "postgres")]
impl<T> From<tokio_postgres::error::Error> for UpdateError<T> {
    fn from(value: tokio_postgres::error::Error) -> Self {
        Self::Error(Error::from(value))
    }
}

/// Storage Container
#[derive(Clone, Copy, Debug)]
pub struct StorageContainer;

impl StorageContainer {
    pub fn builder() -> PhantomBuilder {
        PhantomBuilder::default()
    }
}

/// A [`StorageContainer`] builder
#[derive(Clone, Debug, Default)]
pub struct Builder<N, C, A, S> {
    node_id: N,
    cluster_id: C,
    advertised_listener: A,
    storage: S,
    schema_registry: Option<Registry>,
    lake_house: Option<House>,
    silent: bool,
    user_credentials_required: bool,

    cancellation: CancellationToken,

    factories: Vec<ArcDynStorageFactory>,
}

type PhantomBuilder =
    Builder<PhantomData<i32>, PhantomData<String>, PhantomData<Url>, PhantomData<Url>>;

/// Returns [`Error::UnrecognizedStorageOption`] for the first of `storage`'s query keys
/// that is not in `recognized`.
///
/// A [`StorageFactory::build`] passes the options that its engine reads, so a misspelt
/// option, or an option of a different engine, stops startup instead of being ignored.
pub fn reject_unrecognized_options(storage: &Url, recognized: &[&str]) -> Result<()> {
    storage
        .query_pairs()
        .find(|(k, _)| !recognized.contains(&k.as_ref()))
        .map_or(Ok(()), |(k, _)| {
            Err(Error::UnrecognizedStorageOption {
                scheme: storage.scheme().to_owned(),
                option: k.into_owned(),
            })
        })
}

/// Returns the value of the query option `option` in `storage`, parsed with `parse`, or
/// `None` when the URL doesn't give the option.
///
/// Every occurrence of the option must parse, and the last one is used, as the broker
/// does for its own options. A [`StorageFactory::build`] calls this for each option that
/// its engine reads, so a value that doesn't parse stops startup instead of being
/// replaced by the default.
///
/// # Errors
///
/// [`Error::InvalidStorageOptionValue`], with `expected` as its hint, for the first
/// occurrence whose value `parse` rejects.
pub fn parse_storage_option<T>(
    storage: &Url,
    option: &str,
    expected: &'static str,
    parse: impl Fn(&str) -> Option<T>,
) -> Result<Option<T>> {
    let mut parsed = None;

    for (key, value) in storage.query_pairs() {
        if key == option {
            parsed = Some(
                parse(&value).ok_or_else(|| Error::InvalidStorageOptionValue {
                    scheme: storage.scheme().to_owned(),
                    option: option.to_owned(),
                    value: value.into_owned(),
                    expected,
                })?,
            );
        }
    }

    Ok(parsed)
}

impl<N, C, A, S> Builder<N, C, A, S> {
    pub fn node_id(self, node_id: i32) -> Builder<i32, C, A, S> {
        Builder {
            node_id,
            cluster_id: self.cluster_id,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            silent: self.silent,
            user_credentials_required: self.user_credentials_required,
            cancellation: self.cancellation,
            factories: self.factories,
        }
    }

    pub fn cluster_id(self, cluster_id: impl Into<String>) -> Builder<N, String, A, S> {
        Builder {
            node_id: self.node_id,
            cluster_id: cluster_id.into(),
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            silent: self.silent,
            user_credentials_required: self.user_credentials_required,
            cancellation: self.cancellation,
            factories: self.factories,
        }
    }

    pub fn advertised_listener(self, advertised_listener: impl Into<Url>) -> Builder<N, C, Url, S> {
        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            advertised_listener: advertised_listener.into(),
            storage: self.storage,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            silent: self.silent,
            user_credentials_required: self.user_credentials_required,
            cancellation: self.cancellation,
            factories: self.factories,
        }
    }

    pub fn storage(self, storage: Url) -> Builder<N, C, A, Url> {
        debug!(storage = %redact_url(&storage));

        Builder {
            node_id: self.node_id,
            cluster_id: self.cluster_id,
            advertised_listener: self.advertised_listener,
            storage,
            schema_registry: self.schema_registry,
            lake_house: self.lake_house,
            silent: self.silent,
            user_credentials_required: self.user_credentials_required,
            cancellation: self.cancellation,
            factories: self.factories,
        }
    }

    pub fn schema_registry(self, schema_registry: Option<Registry>) -> Self {
        _ = schema_registry
            .as_ref()
            .inspect(|schema_registry| debug!(?schema_registry));

        Self {
            schema_registry,
            ..self
        }
    }

    pub fn lake_house(self, lake_house: Option<House>) -> Self {
        _ = lake_house.as_ref().inspect(|lake_house| {
            debug!(lake_house = ?LakeHouseType::from(*lake_house));
        });

        Self { lake_house, ..self }
    }

    pub fn cancellation(self, cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            ..self
        }
    }

    pub fn silent(self, silent: bool) -> Self {
        Self { silent, ..self }
    }

    /// When `true`, [`build`](Builder::build) fails with [`Error::AuthenticationUnsupported`]
    /// if the storage engine can't store SCRAM credentials, before it connects to the storage.
    /// A broker that requires authentication sets it.
    pub fn user_credentials_required(self, user_credentials_required: bool) -> Self {
        Self {
            user_credentials_required,
            ..self
        }
    }

    pub fn with_factory(&mut self, factory: ArcDynStorageFactory) {
        self.factories.push(factory);
    }
}

impl From<Builder<i32, String, Url, Url>> for StorageFactoryConfiguration {
    fn from(value: Builder<i32, String, Url, Url>) -> Self {
        Self {
            node_id: value.node_id,
            cluster: value.cluster_id,
            advertised_listener: value.advertised_listener,
            storage: value.storage,
            schema_registry: value.schema_registry,
            lake_house: value.lake_house,
            cancellation: value.cancellation,
        }
    }
}

impl Builder<i32, String, Url, Url> {
    pub async fn build(self) -> Result<ArcDynStorage> {
        let Some(factory) = self
            .factories
            .iter()
            .find(|factory| {
                factory
                    .scheme()
                    .is_ok_and(|scheme| scheme.is_match(self.storage.scheme()))
            })
            .cloned()
        else {
            return Err(Error::NoEngineForScheme {
                scheme: self.storage.scheme().into(),
                storage: redact_url(&self.storage).to_string(),
            });
        };

        if self.user_credentials_required && !factory.capabilities().user_credentials {
            return Err(Error::AuthenticationUnsupported {
                engine: factory.to_string(),
                supported: self
                    .factories
                    .iter()
                    .filter(|factory| factory.capabilities().user_credentials)
                    .map(|factory| factory.to_string())
                    .collect(),
            });
        }

        if let Some(notice) = factory.deprecation() {
            warn!("{notice}");
        }

        let silent = self.silent;
        let redacted = redact_url(&self.storage);

        let storage = factory.build(self.into()).await?;

        let pb = if silent {
            None
        } else {
            let pb = ProgressBar::new(1);
            pb.set_style(
                ProgressStyle::with_template("[{elapsed}] {bar:40.cyan/blue} {msg}")?
                    .progress_chars("##-"),
            );

            pb.set_message("connecting to storage");

            Some(pb)
        };

        storage
            .ping()
            .await
            .map_err(|source| Error::StartupFailed {
                storage: Arc::new(redacted),
                failure: StartupFailure::StorageUnreachable,
                source: Arc::new(source),
            })?;

        if let Some(pb) = pb {
            pb.inc(1);
            pb.finish_with_message(format!("{} connected to storage", Emoji("✅", ""),));
        }

        Ok(storage)
    }
}

/// The message for [`Error::AuthenticationUnsupported`]: `engine` can't store SCRAM users,
/// and `supported` names the engines of this build that can.
pub fn authentication_unsupported_message(engine: &str, supported: &[impl AsRef<str>]) -> String {
    let choices = match supported.split_last() {
        None => {
            return format!(
                "the {engine} storage engine can't store SCRAM users, which --authentication \
                needs, and this build has no storage engine that can: rebuild with the libsql or \
                postgres feature, or start without --authentication, which lets any client \
                connect without logging in"
            );
        }
        Some((last, [])) => last.as_ref().to_owned(),
        Some((last, rest)) => format!(
            "{} or {}",
            rest.iter()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>()
                .join(", "),
            last.as_ref()
        ),
    };

    format!(
        "the {engine} storage engine can't store SCRAM users, which --authentication needs: use \
        {choices}, or start without --authentication, which lets any client connect without \
        logging in"
    )
}

/// The per-user message that AlterUserScramCredentials answers for
/// [`Error::UserCredentialsUnsupported`].
pub fn user_credentials_unsupported_message(engine: &str) -> String {
    format!(
        "The {engine} storage engine can't store SCRAM credentials, so nothing was changed. \
        SCRAM users need PostgreSQL or SQLite storage."
    )
}

pub static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

/// A stored SCRAM credential (RFC 5802).
///
/// `Debug` writes `stored_key` and `server_key` as `[hidden]`. With the salt and
/// iteration count, the stored key allows an offline dictionary attack on the
/// password, and the server key allows a client to impersonate the server.
#[derive(Clone, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ScramCredential {
    pub salt: Bytes,
    pub iterations: i32,
    pub stored_key: Bytes,
    pub server_key: Bytes,
}

impl Debug for ScramCredential {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScramCredential")
            .field("salt", &self.salt)
            .field("iterations", &self.iterations)
            .field("stored_key", &format_args!("[hidden]"))
            .field("server_key", &format_args!("[hidden]"))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{assert_matches, fs::File, sync::Arc, thread};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    #[test]
    fn authentication_unsupported_message_names_the_engines_that_can() {
        assert_eq!(
            "the in-memory (memory://) storage engine can't store SCRAM users, which \
            --authentication needs: use SQLite (sqlite://) or PostgreSQL (postgres://), or start \
            without --authentication, which lets any client connect without logging in",
            authentication_unsupported_message(
                "in-memory (memory://)",
                &["SQLite (sqlite://)", "PostgreSQL (postgres://)"]
            )
        );

        assert_eq!(
            "the S3 (s3://) storage engine can't store SCRAM users, which --authentication needs: \
            use A, B or C, or start without --authentication, which lets any client connect \
            without logging in",
            authentication_unsupported_message("S3 (s3://)", &["A", "B", "C"])
        );
    }

    #[test]
    fn authentication_unsupported_message_without_an_engine_that_can() {
        assert_eq!(
            "the in-memory (memory://) storage engine can't store SCRAM users, which \
            --authentication needs, and this build has no storage engine that can: rebuild with \
            the libsql or postgres feature, or start without --authentication, which lets any \
            client connect without logging in",
            authentication_unsupported_message("in-memory (memory://)", &[] as &[&str])
        );
    }

    #[test]
    fn topition_from_str() -> Result<()> {
        let topition = Topition::from_str("qwerty-2147483647")?;
        assert_eq!("qwerty", topition.topic());
        assert_eq!(i32::MAX, topition.partition());
        Ok(())
    }

    #[test]
    fn topic_with_dashes_in_name() -> Result<()> {
        let topition = Topition::from_str("test-topic-0000000-eFC79C8-2147483647")?;
        assert_eq!("test-topic-0000000-eFC79C8", topition.topic());
        assert_eq!(i32::MAX, topition.partition());
        Ok(())
    }

    #[test]
    fn scram_credential_debug_hides_keys() {
        let credential = ScramCredential {
            salt: Bytes::from_static(b"salt-marker"),
            iterations: 4096,
            stored_key: Bytes::from_static(b"stored-key-secret"),
            server_key: Bytes::from_static(b"server-key-secret"),
        };

        let debug = format!("{credential:?}");

        assert!(!debug.contains("secret"), "{debug}");
        assert!(debug.contains("stored_key: [hidden]"), "{debug}");
        assert!(debug.contains("server_key: [hidden]"), "{debug}");
        assert!(debug.contains("salt-marker"), "{debug}");
        assert!(debug.contains("iterations: 4096"), "{debug}");
    }

    /// Every engine except SlateDB stores a group as JSON. After an upgrade, the broker reads
    /// groups that an earlier version stored, whose members do not have a `client_id` or
    /// `client_host` field. If the broker fails to decode such a group, `DescribeGroups` answers
    /// `UNKNOWN_SERVER_ERROR` for it. Decoding must give such a member an empty client id and
    /// host.
    #[test]
    fn member_stored_without_client_id_and_host_reads_with_empty_ones() -> serde_json::Result<()> {
        let mut stored = serde_json::to_value(detail_with_one_member(
            GroupState::default(),
            GroupRebalance::default(),
        ))?;
        let member = stored["members"]["member"]
            .as_object_mut()
            .expect("a stored member is a JSON object");
        assert!(member.remove("client_id").is_some());
        assert!(member.remove("client_host").is_some());

        let detail = serde_json::from_value::<GroupDetail>(stored)?;

        let member = &detail.members["member"];
        assert_eq!(
            ("", ""),
            (member.client_id.as_str(), member.client_host.as_str())
        );
        Ok(())
    }

    /// Returns a detail with one member, in the given group state and rebalance.
    fn detail_with_one_member(state: GroupState, rebalance: GroupRebalance) -> GroupDetail {
        GroupDetail {
            members: [(
                "member".into(),
                GroupMember {
                    join_response: JoinGroupResponseMember::default().member_id("member".into()),
                    last_contact: None,
                    ..GroupMember::default()
                },
            )]
            .into(),
            state,
            rebalance: Box::new(rebalance),
            ..GroupDetail::default()
        }
    }

    /// Returns a forming group state with the `consumer` protocol type and `range` protocol.
    fn forming_group_state(leader: Option<&str>) -> GroupState {
        GroupState::Forming {
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
            leader: leader.map(Into::into),
        }
    }

    #[test]
    fn group_state_is_the_kafka_classic_group_state() {
        let join_phase = JoinPhase {
            deadline: SystemTime::now(),
            joined: BTreeSet::new(),
            initial_delay: None,
        };

        let formed = GroupState::Formed {
            protocol_type: "consumer".into(),
            protocol_name: "range".into(),
            leader: "member".into(),
            assignments: BTreeMap::new(),
        };

        let in_join_phase = GroupRebalance {
            join_phase: Some(join_phase),
            ..GroupRebalance::default()
        };

        let pending_sync = GroupRebalance {
            pending_sync: Some(PendingSync {
                deadline: SystemTime::now(),
                members: ["member".into()].into(),
            }),
            ..GroupRebalance::default()
        };

        let without_members_in_join_phase = GroupDetail {
            state: forming_group_state(None),
            rebalance: Box::new(in_join_phase.clone()),
            ..GroupDetail::default()
        };

        assert_eq!(
            [
                ConsumerGroupState::Empty,
                ConsumerGroupState::PreparingRebalance,
                ConsumerGroupState::PreparingRebalance,
                ConsumerGroupState::CompletingRebalance,
                ConsumerGroupState::PreparingRebalance,
                ConsumerGroupState::Stable,
            ],
            [
                GroupDetail::default(),
                without_members_in_join_phase,
                detail_with_one_member(forming_group_state(None), in_join_phase),
                detail_with_one_member(forming_group_state(Some("member")), pending_sync),
                detail_with_one_member(
                    forming_group_state(Some("member")),
                    GroupRebalance::default()
                ),
                detail_with_one_member(formed, GroupRebalance::default()),
            ]
            .each_ref()
            .map(ConsumerGroupState::from)
        );
    }

    #[test]
    fn states_filter_ignores_case_and_surrounding_white_space() {
        assert!(ConsumerGroupState::Stable.matches(Some(&[" stable ".into()])));
    }

    #[test]
    fn states_filter_leaves_out_other_states() {
        assert!(!ConsumerGroupState::Empty.matches(Some(&["Stable".into()])));
    }

    #[test]
    fn states_filter_of_unknown_names_matches_no_state() {
        assert!(!ConsumerGroupState::Empty.matches(Some(&["NoSuchState".into()])));
    }

    #[test]
    fn missing_or_empty_states_filter_matches_every_state() {
        assert_eq!(
            (true, true),
            (
                ConsumerGroupState::Empty.matches(None),
                ConsumerGroupState::Empty.matches(Some(&[]))
            )
        );
    }

    #[test]
    fn storage_error_with_a_code_keeps_that_code() {
        assert_eq!(
            ErrorCode::UnknownServerError,
            Error::Api(ErrorCode::UnknownServerError).group_request_error_code()
        );
    }

    #[test]
    fn other_storage_error_answers_coordinator_not_available() {
        assert_eq!(
            ErrorCode::CoordinatorNotAvailable,
            Error::Message("connection refused".into()).group_request_error_code()
        );
    }

    #[test]
    fn group_that_storage_could_not_read_is_listed_as_unknown() {
        let listed = ListedGroup::from(&NamedGroupDetail::error_code(
            "group".into(),
            ErrorCode::UnknownServerError,
        ));

        assert_eq!(Some("Unknown".into()), listed.group_state);
    }

    #[test]
    fn redact_url_strips_password_only() -> Result<()> {
        let url = Url::parse("postgres://user:secret@host/db")?;
        let redacted = redact_url(&url);

        assert_eq!(None, redacted.password());
        assert_eq!("user", redacted.username());
        assert_eq!(Some("host"), redacted.host_str());
        assert_eq!("/db", redacted.path());
        assert!(!redacted.as_str().contains("secret"));

        // the original is untouched: it's the value still used to connect
        assert_eq!(Some("secret"), url.password());

        Ok(())
    }

    #[test]
    fn redact_url_leaves_passwordless_url_unchanged() -> Result<()> {
        let url = Url::parse("memory://nisshi/")?;
        assert_eq!(url, redact_url(&url));
        Ok(())
    }

    #[test]
    fn delete_records_cutoff_minus_one_means_high_watermark() {
        let stage = OffsetStage {
            last_stable: 100,
            high_watermark: 100,
            log_start: 10,
        };

        assert_eq!(Ok(100), delete_records_cutoff(-1, &stage));
    }

    #[test]
    fn delete_records_cutoff_rejects_other_negative_offsets() {
        let stage = OffsetStage {
            last_stable: 100,
            high_watermark: 100,
            log_start: 10,
        };

        assert_eq!(
            Err(ErrorCode::OffsetOutOfRange),
            delete_records_cutoff(-2, &stage)
        );

        assert_eq!(
            Err(ErrorCode::OffsetOutOfRange),
            delete_records_cutoff(i64::MIN, &stage)
        );
    }

    #[test]
    fn delete_records_cutoff_rejects_offset_above_high_watermark() {
        let stage = OffsetStage {
            last_stable: 100,
            high_watermark: 100,
            log_start: 10,
        };

        assert_eq!(
            Err(ErrorCode::OffsetOutOfRange),
            delete_records_cutoff(101, &stage)
        );
    }

    #[test]
    fn delete_records_cutoff_at_or_below_log_start_is_a_noop() {
        let stage = OffsetStage {
            last_stable: 100,
            high_watermark: 100,
            log_start: 10,
        };

        // Exactly at the current log start: unchanged.
        assert_eq!(Ok(10), delete_records_cutoff(10, &stage));

        // Below the current log start: the log start never moves backward.
        assert_eq!(Ok(10), delete_records_cutoff(0, &stage));
    }

    #[test]
    fn delete_records_cutoff_within_range_uses_requested_offset() {
        let stage = OffsetStage {
            last_stable: 100,
            high_watermark: 100,
            log_start: 10,
        };

        assert_eq!(Ok(50), delete_records_cutoff(50, &stage));
    }

    #[test]
    fn delete_records_cutoff_allows_explicit_high_watermark() {
        let stage = OffsetStage {
            last_stable: 100,
            high_watermark: 100,
            log_start: 10,
        };

        assert_eq!(Ok(100), delete_records_cutoff(100, &stage));
    }

    #[test]
    fn delete_records_cutoff_on_empty_partition() {
        // A brand new partition: high_watermark == log_start == 0.
        let stage = OffsetStage {
            last_stable: 0,
            high_watermark: 0,
            log_start: 0,
        };

        assert_eq!(Ok(0), delete_records_cutoff(-1, &stage));
        assert_eq!(Ok(0), delete_records_cutoff(0, &stage));
        assert_eq!(
            Err(ErrorCode::OffsetOutOfRange),
            delete_records_cutoff(1, &stage)
        );
    }

    fn log_file_path() -> Result<String> {
        thread::current()
            .name()
            .ok_or(Error::Message(String::from("unnamed thread")))
            .map(|name| format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
    }

    fn init_tracing() -> Result<DefaultGuard> {
        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(
                    EnvFilter::from_default_env()
                        .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
                )
                .with_writer(
                    log_file_path()
                        .and_then(|path| File::create(path).map_err(Into::into))
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[test]
    fn builder_storage_does_not_log_password() -> Result<()> {
        let _guard = init_tracing()?;

        // Exercises `Builder::storage`, which used to `debug!(%storage)`
        // the raw URL (nisshi-storage/src/lib.rs).
        _ = StorageContainer::builder().storage(Url::parse("postgres://user:secret@host/db")?);

        let log = std::fs::read_to_string(log_file_path()?)?;
        assert!(!log.contains("secret"));
        assert!(log.contains("user@host"));

        Ok(())
    }

    #[tokio::test]
    async fn feature_not_enabled_error_does_not_leak_password() -> Result<()> {
        // No factories are registered on a bare `StorageContainer::builder()`
        // (those come from the storage backend crates, e.g.
        // `nisshi-storage-sql`), so this always falls into the
        // `Error::NoEngineForScheme` branch regardless of scheme, which used
        // to embed the raw, unredacted URL in its `message` field.
        let err = StorageContainer::builder()
            .node_id(1)
            .cluster_id("test")
            .advertised_listener(Url::parse("tcp://localhost:9092")?)
            .storage(Url::parse("postgres://user:secret@host/db")?)
            .build()
            .await
            .unwrap_err();

        let message = err.to_string();
        assert!(!message.contains("secret"));
        assert!(message.contains("user@host"));

        Ok(())
    }

    /// An engine that keeps no credentials, and panics if the builder asks it to build.
    #[derive(Debug)]
    struct PanicsOnBuild;

    impl Display for PanicsOnBuild {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            f.write_str("stub (stub://)")
        }
    }

    #[async_trait]
    impl StorageFactory for PanicsOnBuild {
        fn scheme(&self) -> Result<Regex> {
            Regex::new("stub").map_err(Into::into)
        }

        fn capabilities(&self) -> StorageCapabilities {
            StorageCapabilities::default()
        }

        async fn build(&self, _: StorageFactoryConfiguration) -> Result<ArcDynStorage> {
            panic!("the engine was built before the capability check");
        }
    }

    /// The check that refuses an engine without user credentials runs before the factory
    /// connects, so a refused engine is never built: a check moved after `build` panics here
    /// on every machine, with or without a reachable storage service.
    #[tokio::test]
    async fn credentials_check_runs_before_the_engine_is_built() -> Result<()> {
        let mut builder = StorageContainer::builder();
        builder.with_factory(Arc::new(PanicsOnBuild));

        let refused = builder
            .node_id(1)
            .cluster_id("test")
            .advertised_listener(Url::parse("tcp://localhost:9092")?)
            .storage(Url::parse("stub://bucket")?)
            .user_credentials_required(true)
            .silent(true)
            .build()
            .await;

        assert_matches!(
            &refused,
            Err(Error::AuthenticationUnsupported { engine, supported })
                if engine == "stub (stub://)" && supported.is_empty(),
            "{refused:?}"
        );

        Ok(())
    }

    #[test]
    fn reject_unrecognized_options_accepts_listed_keys() -> Result<()> {
        reject_unrecognized_options(&Url::parse("sqlite://nisshi.db")?, &[])?;
        reject_unrecognized_options(
            &Url::parse("sqlite://nisshi.db?vacuum_into=/tmp/x&mode=memory")?,
            &["vacuum_into", "mode"],
        )
    }

    #[test]
    fn reject_unrecognized_options_names_unknown_key_and_scheme() -> Result<()> {
        let error = reject_unrecognized_options(
            &Url::parse("sqlite://nisshi.db?vacuum_onto=/tmp/x")?,
            &["vacuum_into"],
        )
        .unwrap_err();

        assert!(
            matches!(
                &error,
                Error::UnrecognizedStorageOption { scheme, option }
                    if scheme == "sqlite" && option == "vacuum_onto"
            ),
            "{error:?}"
        );

        Ok(())
    }

    fn parse_mode(storage: &str) -> Result<Option<String>> {
        parse_storage_option(&Url::parse(storage)?, "mode", "fast or slow", |value| {
            ["fast", "slow"].contains(&value).then(|| value.to_owned())
        })
    }

    #[test]
    fn parse_storage_option_without_the_option_returns_none() -> Result<()> {
        assert_eq!(None, parse_mode("sqlite://nisshi.db")?);
        assert_eq!(None, parse_mode("sqlite://nisshi.db?other=fast")?);
        Ok(())
    }

    #[test]
    fn parse_storage_option_returns_the_parsed_value() -> Result<()> {
        assert_eq!(
            Some("fast".to_owned()),
            parse_mode("sqlite://nisshi.db?mode=fast")?
        );
        Ok(())
    }

    #[test]
    fn parse_storage_option_uses_the_last_occurrence() -> Result<()> {
        assert_eq!(
            Some("slow".to_owned()),
            parse_mode("sqlite://nisshi.db?mode=fast&mode=slow")?
        );
        Ok(())
    }

    #[test]
    fn parse_storage_option_names_the_scheme_option_value_and_hint() {
        assert_matches!(
            parse_mode("sqlite://nisshi.db?mode=Fast"),
            Err(Error::InvalidStorageOptionValue { scheme, option, value, expected })
                if scheme == "sqlite"
                    && option == "mode"
                    && value == "Fast"
                    && expected == "fast or slow"
        );
    }

    /// A value that doesn't parse is an error wherever it is, even when another
    /// occurrence of the option parses.
    #[test]
    fn parse_storage_option_rejects_any_occurrence_that_does_not_parse() {
        for storage in [
            "sqlite://nisshi.db?mode=Fast&mode=slow",
            "sqlite://nisshi.db?mode=slow&mode=Fast",
        ] {
            assert_matches!(
                parse_mode(storage),
                Err(Error::InvalidStorageOptionValue { value, .. }) if value == "Fast",
                "{storage}"
            );
        }
    }
}
