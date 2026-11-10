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

//! PostgreSQL Storage engine

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    hash::Hash,
    io,
    marker::PhantomData,
    ops::RangeInclusive,
    pin::Pin,
    str::FromStr,
    sync::Arc,
    task::{Context, Poll, ready},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use deadpool::managed;
use deadpool_postgres::{
    Hook, HookError, Manager, ManagerConfig, Object, Pool, PoolError, RecyclingMethod, Runtime,
    TimeoutType, Transaction,
};
use futures::pin_mut;
use nisshi_sans_io::topic_config;
use nisshi_sans_io::{
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, ControlBatch, EndTransactionMarker,
    ErrorCode, IsolationLevel, ListOffset, NULL_TOPIC_ID, OpType, ScramMechanism,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::CreatableTopic,
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
    fetch_response::AbortedTransaction,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    metadata_response::{MetadataResponseBroker, MetadataResponseTopic},
    record::{Record, deflated, inflated::Batch},
    to_system_time, to_timestamp,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use nisshi_schema::{
    Registry,
    lake::{House, LakeHouse as _},
};
use nisshi_storage::{
    BrokerRegistrationRequest, CommittedOffset, DELETE_RECORDS_CONCURRENCY,
    DELETE_RECORDS_INVALID_LOW_WATERMARK, DeleteRecordsConcurrency, Error, FetchedSender,
    GroupDetail, LIST_OFFSETS_CONCURRENCY, ListOffsetResponse, MetadataResponse, NamedGroupDetail,
    OffsetCommitRequest, OffsetStage, ProduceEpoch, ProducerIdResponse, Result, ScramCredential,
    Storage, StorageOperation, StoredEpochs, TopicId, Topition, TxnAddPartitionsRequest,
    TxnAddPartitionsResponse, TxnOffsetCommitRequest, TxnState, UpdateError, Version, check_claim,
    config::{self, apply_op},
    delete_records_cutoff, epoch_is_exhausted, inflate, max_message_bytes, parse_storage_option,
    producer_claim,
    read_deadline::{QueueWait as _, ReadWait},
    read_share::{ClientAdmission, ClientReadShares, FetchReadShare, ListOffsetsReadShare},
};
use opentelemetry::{KeyValue, metrics::Counter};
use rand::{prelude::*, rng};
use serde_json::Value;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    runtime::Handle,
    sync::{AcquireError, OwnedSemaphorePermit, Semaphore},
    time::{Instant, timeout, timeout_at},
};
use tokio_postgres::{
    CancelToken, Config, Row, SimpleQueryMessage,
    binary_copy::BinaryCopyInWriter,
    config::Host,
    error::SqlState,
    tls::MakeTlsConnect,
    types::{ToSql, Type},
};
use tokio_postgres_rustls::MakeRustlsConnect;
#[cfg(unix)]
use tokio_util::either::Either;
use tracing::{Instrument as _, debug, error, info, instrument, warn};
use url::{Url, form_urlencoded};
use uuid::Uuid;

use self::metrics::{FetchStatementMetrics, POOL_REPLACED};
use crate::{
    schema::{RecordedSchema, should_create_schema},
    sql::{
        consumer_group_select, consumer_offset_select, consumer_offset_select_by_group,
        default_hash, idempotent_epoch_check, idempotent_sequence_check, pg_record_fetch,
        producer_epochs_select, topic_configuration_select, topic_configuration_select_keys,
        topic_select, txn_offset_commit_tp_select_pending, txn_produce_partition_check,
        txn_produce_partition_select, watermark_select_for_write,
    },
};

mod connect_rate_limit;
pub mod factory;
mod metrics;

use connect_rate_limit::{
    CheckoutError, ConnectRateLimit, RATE_LIMITED_CONNECT_INTERVAL, RateLimitedPool,
};

/// PostgreSQL Storage Engine
#[derive(Clone, Debug)]
pub struct Postgres {
    cluster: String,
    node: i32,
    advertised_listener: Url,
    pool: RateLimitedPool,
    cancel: Option<CancelRequests>,
    /// Permits for the ListOffsets partition reads in flight, see
    /// [`ReadLimits::list_offsets`].
    list_offsets_reads: Arc<Semaphore>,
    /// Permits for the Fetch partition reads in flight, see
    /// [`ReadLimits::fetch`].
    fetch_reads: Arc<Semaphore>,
    /// How many of `list_offsets_reads` one connection may hold, see
    /// [`ReadLimits::list_offsets_per_connection`].
    list_offsets_reads_per_connection: usize,
    /// How many of `fetch_reads` one connection may hold, see
    /// [`ReadLimits::fetch_per_connection`].
    fetch_reads_per_connection: usize,
    /// Permits for the DeleteRecords calls in flight across every request,
    /// see [`delete_records_limit`].
    delete_records_writes: Arc<Semaphore>,
    /// How many DeleteRecords calls one request makes at once, at most
    /// `delete_records_writes`, see
    /// [`Storage::delete_records_concurrency`].
    delete_records_per_request: usize,
    /// The shares of `list_offsets_reads` that clients hold, see
    /// [`ReadLimits::list_offsets_per_client`].
    list_offsets_client_shares: ClientReadShares,
    /// The shares of `fetch_reads` that clients hold, see
    /// [`ReadLimits::fetch_per_client`].
    fetch_client_shares: ClientReadShares,
    /// How many of `list_offsets_reads` one client may hold, over all its
    /// connections, while another client waits for them, see
    /// [`ReadLimits::list_offsets_per_client`].
    list_offsets_reads_per_client: usize,
    /// How many of `fetch_reads` one client may hold, over all its
    /// connections, while another client waits for them, see
    /// [`ReadLimits::fetch_per_client`].
    fetch_reads_per_client: usize,
    /// How long an abandoned statement's connection stays out of the pool
    /// before it is closed, see [`cancel_and_return`].
    abandoned_statement_bound: Duration,
    schemas: Option<Registry>,
    lake: Option<House>,
}

/// PostgreSQL Storage Builder
#[derive(Clone, Default, Debug)]
pub struct Builder<C, N, L, P> {
    cluster: C,
    node: N,
    advertised_listener: L,
    pool: P,
    read_limits: ReadLimits,
    cancel: Option<CancelRequests>,
    abandoned_statement_bound: Duration,
    schemas: Option<Registry>,
    lake: Option<House>,
}

impl<C, N, L, P> Builder<C, N, L, P> {
    pub fn cluster(self, cluster: impl Into<String>) -> Builder<String, N, L, P> {
        Builder {
            cluster: cluster.into(),
            node: self.node,
            advertised_listener: self.advertised_listener,
            pool: self.pool,
            read_limits: self.read_limits,
            cancel: self.cancel,
            abandoned_statement_bound: self.abandoned_statement_bound,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub fn node(self, node: i32) -> Builder<C, i32, L, P> {
        Builder {
            cluster: self.cluster,
            node,
            advertised_listener: self.advertised_listener,
            pool: self.pool,
            read_limits: self.read_limits,
            cancel: self.cancel,
            abandoned_statement_bound: self.abandoned_statement_bound,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub fn advertised_listener(self, advertised_listener: Url) -> Builder<C, N, Url, P> {
        Builder {
            cluster: self.cluster,
            node: self.node,
            advertised_listener,
            pool: self.pool,
            read_limits: self.read_limits,
            cancel: self.cancel,
            abandoned_statement_bound: self.abandoned_statement_bound,
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub fn schemas(self, schemas: Option<Registry>) -> Builder<C, N, L, P> {
        Self { schemas, ..self }
    }

    pub fn lake(self, lake: Option<House>) -> Self {
        Self { lake, ..self }
    }
}

/// What the builder needs to open the pool: the connection config, the TLS connector and the
/// pool's size, kept until [`Builder::build`] knows the cluster and the node that name the
/// connections. Building a pool does not connect, so the pool opens its first connection later.
#[derive(Clone)]
pub struct PoolParts {
    /// The operator's settings, without a default `application_name`.
    config: Config,
    tls: MakeRustlsConnect,
    pool_max_size: usize,
    /// Whether the storage URL, in its query or in `options`, sets `application_name`.
    named_by_operator: bool,
    /// The value of [`READ_ONLY_CHECK_EVERY`], which [`ReadOnlyCheck`] runs on.
    read_only_check_every: u32,
    /// The machine's name, which [`application_name`] puts in the name of each connection.
    host: Option<String>,
    /// The pool with the plain [`APPLICATION_NAME_PREFIX`] as its name, built where the storage
    /// URL is parsed, so that an unusable setting fails there.
    pool: RateLimitedPool,
}

impl Debug for PoolParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolParts")
            .field("config", &self.config)
            .field("pool_max_size", &self.pool_max_size)
            .field("named_by_operator", &self.named_by_operator)
            .field("read_only_check_every", &self.read_only_check_every)
            .field("host", &self.host)
            .finish_non_exhaustive()
    }
}

impl PoolParts {
    /// Opens a pool of connections that use `config`, and that run `check` on them unless
    /// `check.every` is 0. The pool has a rate limit of its own, which `check` starts and stops.
    fn open(
        config: Config,
        tls: MakeRustlsConnect,
        pool_max_size: usize,
        check: &ReadOnlyCheck,
    ) -> std::result::Result<RateLimitedPool, deadpool_postgres::BuildError> {
        let rate_limit = Arc::new(ConnectRateLimit::default());
        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };

        let builder = Pool::builder(Manager::from_config(config, tls, mgr_config))
            .max_size(pool_max_size)
            .runtime(Runtime::Tokio1)
            .wait_timeout(Some(DEFAULT_POOL_WAIT_TIMEOUT))
            .create_timeout(Some(DEFAULT_POOL_CREATE_TIMEOUT))
            .recycle_timeout(Some(DEFAULT_POOL_RECYCLE_TIMEOUT));

        if check.every == 0 {
            builder.build()
        } else {
            builder
                .post_create(check.post_create(rate_limit.clone()))
                .pre_recycle(check.pre_recycle())
                .build()
        }
        .map(|pool| RateLimitedPool::new(pool, rate_limit))
    }

    /// The pool for the broker of `cluster`: its connections are named by
    /// [`application_name`] unless the operator named them, and the check that it runs counts in
    /// `cluster`.
    fn pool_of(&self, cluster: &str, node: i32) -> RateLimitedPool {
        let mut config = self.config.clone();

        if !self.named_by_operator {
            _ = config.application_name(application_name(self.host.as_deref(), cluster, node));
        }

        let check = ReadOnlyCheck::new(self.read_only_check_every, cluster);

        // The only way that opening a pool fails is a timeout without a runtime, and `open` sets
        // both. The pool that `from_str` built with the same settings stands in, so that this
        // can never stop the broker.
        Self::open(config, self.tls.clone(), self.pool_max_size, &check).unwrap_or_else(|err| {
            error!(?err, "opening the pool for the cluster failed");
            self.pool.clone()
        })
    }
}

impl<C, N, L> Builder<C, N, L, PoolParts> {
    /// Names the connections of the pool `nisshi-<host>-<cluster>`, with `host` instead of the
    /// name of the machine that this process runs on. A storage URL that sets
    /// `application_name` still wins.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.pool.host = Some(host.into());
        self
    }
}

impl Builder<String, i32, Url, PoolParts> {
    pub fn build(self) -> Postgres {
        let pool = self.pool.pool_of(&self.cluster, self.node);
        let delete_records_limit = delete_records_limit(pool.status().max_size);

        Postgres {
            cluster: self.cluster,
            node: self.node,
            advertised_listener: self.advertised_listener,
            pool,
            cancel: self.cancel,
            list_offsets_reads: Arc::new(Semaphore::new(self.read_limits.list_offsets)),
            fetch_reads: Arc::new(Semaphore::new(self.read_limits.fetch)),
            list_offsets_reads_per_connection: self.read_limits.list_offsets_per_connection(),
            fetch_reads_per_connection: self.read_limits.fetch_per_connection(),
            delete_records_writes: Arc::new(Semaphore::new(delete_records_limit)),
            delete_records_per_request: DELETE_RECORDS_CONCURRENCY.min(delete_records_limit),
            list_offsets_client_shares: ClientReadShares::default(),
            fetch_client_shares: ClientReadShares::default(),
            list_offsets_reads_per_client: self.read_limits.list_offsets_per_client(),
            fetch_reads_per_client: self.read_limits.fetch_per_client(),
            abandoned_statement_bound: self.abandoned_statement_bound,
            schemas: self.schemas,
            lake: self.lake,
        }
    }
}

/// How many connections the pool opens at most when the storage URL does not
/// set [`POOL_MAX_SIZE`].
const DEFAULT_POOL_MAX_SIZE: usize = 16;

/// The storage URL option that sets how many connections the pool opens at
/// most.
const POOL_MAX_SIZE: &str = "pool_max_size";

/// The range of [`POOL_MAX_SIZE`]. Below 2, [`ReadLimits::for_pool`] gives a
/// limit of 0, and no read of that kind ever runs. 262143 is PostgreSQL's
/// `MAX_BACKENDS`, the most connections that a server accepts. The pool and
/// the read limits each make a [`Semaphore`] of this size, and
/// [`Semaphore::new`] panics above [`Semaphore::MAX_PERMITS`], which is much
/// larger.
const POOL_MAX_SIZE_RANGE: RangeInclusive<usize> = 2..=262_143;

/// The values that [`POOL_MAX_SIZE`] takes, as an error message gives them.
const POOL_MAX_SIZE_EXPECTED: &str = "a whole number from 2 to 262143";

/// The storage URL option that sets how many of each read limit one client may
/// hold while another client waits for it, as a percentage, see
/// [`ReadLimits::list_offsets_per_client`].
const CLIENT_READ_SHARE: &str = "client_read_share";

/// The range of [`CLIENT_READ_SHARE`]: a percentage.
const CLIENT_READ_SHARE_RANGE: RangeInclusive<usize> = 1..=100;

/// The values that [`CLIENT_READ_SHARE`] takes, as an error message gives
/// them.
const CLIENT_READ_SHARE_EXPECTED: &str = "a whole number from 1 to 100";

/// How many partition reads of each kind this engine has in flight at once,
/// across every request, counting an abandoned read until its statement has
/// ended, its connection has failed, or the bound on it has passed, see
/// [`cancel_and_return`].
#[derive(Clone, Copy, Debug)]
struct ReadLimits {
    /// How many ListOffsets partition reads are in flight at once.
    ///
    /// Each read holds a pooled connection, so ListOffsets reads alone take
    /// at most half the pool. Together with the Fetch reads, they can take
    /// all of it, see [`ReadLimits::fetch`]. A read past this limit waits for
    /// a permit, and [`ListOffsetsService`]'s deadline drops it if the wait
    /// lasts too long. One connection holds at most
    /// [`ReadLimits::list_offsets_per_connection`] of these permits.
    ///
    /// [`ListOffsetsService`]: nisshi_storage::ListOffsetsService
    list_offsets: usize,

    /// How many Fetch partition reads, of records or of an offset stage, are
    /// in flight at once.
    ///
    /// Each read holds a pooled connection, so Fetch reads alone take at most
    /// half the pool. Fetch reads have their own limit instead of sharing
    /// [`ReadLimits::list_offsets`], so slow fetches cannot hold up the
    /// ListOffsets reads that a consumer needs to start or reset. The two
    /// limits together equal the pool, so when slow reads use up both, other
    /// requests wait for a connection. A smaller limit would slow down
    /// healthy consumers, because a read for a Fetch can hold its connection
    /// until its deadline. A read past this limit waits for a permit until its
    /// deadline. One connection holds at most
    /// [`ReadLimits::fetch_per_connection`] of these permits.
    fetch: usize,

    /// The percentage of each limit that one client may hold while another
    /// client waits for it, from [`CLIENT_READ_SHARE`], or `None` for the
    /// default, which leaves a newcomer a full connection's share. A client
    /// that is alone may hold the whole limit.
    client_percent: Option<usize>,
}

impl ReadLimits {
    /// Returns the read limits for a pool of `max_size` connections: half
    /// the pool for each kind of read.
    fn for_pool(max_size: usize) -> Self {
        let limits = Self {
            list_offsets: max_size / 2,
            fetch: max_size / 2,
            client_percent: None,
        };
        debug_assert!(limits.list_offsets + limits.fetch <= max_size);
        limits
    }

    /// Returns how many of the [`ReadLimits::list_offsets`] permits one client
    /// connection holds at once: [`LIST_OFFSETS_READS_PER_CONNECTION`], or the
    /// whole limit when the pool is too small for that.
    fn list_offsets_per_connection(self) -> usize {
        LIST_OFFSETS_READS_PER_CONNECTION.min(self.list_offsets)
    }

    /// Returns how many of the [`ReadLimits::fetch`] permits one client
    /// connection holds at once: [`FETCH_READS_PER_CONNECTION`], or the whole
    /// limit when the pool is too small for that.
    fn fetch_per_connection(self) -> usize {
        FETCH_READS_PER_CONNECTION.min(self.fetch)
    }

    /// Returns how many of the [`ReadLimits::list_offsets`] permits one
    /// client holds at once, over all its connections, while another client
    /// waits for them.
    fn list_offsets_per_client(self) -> usize {
        self.per_client(self.list_offsets, self.list_offsets_per_connection())
    }

    /// Returns how many of the [`ReadLimits::fetch`] permits one client holds
    /// at once, over all its connections, while another client waits for
    /// them.
    fn fetch_per_client(self) -> usize {
        self.per_client(self.fetch, self.fetch_per_connection())
    }

    /// Returns how many of `limit` one client holds: [`CLIENT_READ_SHARE`]
    /// percent of it, or by default all but a connection's share, so another
    /// client's connection always finds `per_connection` permits left. The
    /// result is never below `per_connection`, so a lone client's single
    /// request is not slowed, and never above `limit`.
    fn per_client(self, limit: usize, per_connection: usize) -> usize {
        self.client_percent
            .map_or_else(
                || limit.saturating_sub(per_connection),
                |percent| limit * percent / 100,
            )
            .clamp(per_connection, limit)
    }
}

/// How many DeleteRecords calls this engine has in flight at once, across every
/// request, for a pool of `max_size` connections: a quarter of the pool, and at
/// least one.
///
/// Each call holds a pooled connection for its watermark transaction and its
/// record `DELETE`, which only the statement timeout bounds. Without this
/// limit, requests that each make [`DELETE_RECORDS_CONCURRENCY`] calls take the
/// whole pool with a few requests, and produce and offset commits then wait for
/// a connection. Kafka Streams sends such requests from every stream thread at
/// each purge interval. The calls of a request wait for a permit, and a call
/// still waiting at the request's deadline is not started.
fn delete_records_limit(max_size: usize) -> usize {
    (max_size / 4).max(1)
}

impl Default for ReadLimits {
    fn default() -> Self {
        Self::for_pool(DEFAULT_POOL_MAX_SIZE)
    }
}

/// How many ListOffsets reads one client connection holds at once when the
/// pool allows it, see [`read_share`](nisshi_storage::read_share) and
/// [`ReadLimits::list_offsets_per_connection`].
///
/// The share equals [`LIST_OFFSETS_CONCURRENCY`], the number of partitions
/// that one ListOffsets request reads at once, so a single request is not
/// slowed. A client retries a `REQUEST_TIMED_OUT` partition at once, and the
/// retry's reads wait for the share while the abandoned reads of the request
/// before it still run, so one connection cannot take the whole limit. The
/// share follows the request, not the pool, so a larger pool does not let
/// one connection hold more.
const LIST_OFFSETS_READS_PER_CONNECTION: usize = LIST_OFFSETS_CONCURRENCY;

/// How many Fetch reads, of records or of an offset stage, one client
/// connection holds at once when the pool allows it, see
/// [`read_share`](nisshi_storage::read_share) and
/// [`ReadLimits::fetch_per_connection`].
///
/// A Fetch gives each partition half of the time left, so one wide Fetch
/// can abandon several partitions in a row, and each abandoned read holds
/// its permit until its statement ends. Without a share, one connection
/// could hold most of the limit that way, and every other consumer's reads
/// would wait and come back empty.
///
/// A Fetch reads its partitions one after another, so a connection holds
/// one live read at a time. The second permit lets that read run while one
/// abandoned read of the connection still holds its permit. The share
/// follows how a Fetch reads, not the pool, so a larger pool does not let
/// one connection hold more.
const FETCH_READS_PER_CONNECTION: usize = 2;

/// How much longer than its connection's `statement_timeout` an abandoned
/// statement is waited for before its connection is closed, see
/// [`cancel_and_return`].
///
/// The server ends the statement itself at `statement_timeout`, and this
/// margin gives its answer time to arrive.
const ABANDONED_STATEMENT_MARGIN: Duration = Duration::from_secs(5);

/// The statements that create the schema of [`SCHEMA_VERSION`].
const SCHEMA: &str = include_str!("pg/schema.sql");

/// The version of the schema that [`SCHEMA`] creates. A database records the version of its
/// schema in its `nisshi_schema_version` table.
const SCHEMA_VERSION: i32 = 1;

// The build fails here when someone changes `SCHEMA_VERSION`. In a database with tables but no
// recorded version, `ensure_schema` creates only the missing tables, and then records
// `SCHEMA_VERSION`. A new version needs an upgrade step there, because such a database has the
// tables of version 1.
const _: () = assert!(SCHEMA_VERSION == 1);

/// The oldest schema version whose broker can use the schema that [`SCHEMA`] creates.
const OLDEST_COMPATIBLE_SCHEMA_VERSION: i32 = 1;

/// The key of the advisory lock that [`Postgres::ensure_schema`] holds. Advisory locks share one
/// key space per database. The key spells `nisshi`, so that it is unlikely to match another
/// application's key.
const SCHEMA_LOCK: i64 = i64::from_be_bytes(*b"\0\0nisshi");

/// Postgres statement timeout applied to every connection by default, so a stalled query
/// (e.g. a lock wait) is aborted server-side instead of holding a pooled connection open
/// indefinitely. An operator's own `?options=-c%20statement_timeout%3D...` in the connection
/// URL still wins over this default; see [`merge_statement_timeout_option`]. Maintenance
/// sweeps (`policy_compact`, `policy_delete`) explicitly opt out via `SET LOCAL` since their
/// full-table scans can legitimately run longer than a request-path query should.
const DEFAULT_STATEMENT_TIMEOUT_MS: u64 = 30_000;

/// Settings for PostgreSQL's query planner in the transaction of a fetch. They turn off bitmap
/// scans, sequential scans, sorts, hash aggregation, nested loop joins and hash joins, so that
/// PostgreSQL reads `record` and `header` in offset order through their primary keys, and joins
/// each record to its headers with a merge join.
const FETCH_PLAN_SETTINGS: &str = "SET LOCAL enable_bitmapscan = off; \
     SET LOCAL enable_seqscan = off; \
     SET LOCAL enable_sort = off; \
     SET LOCAL enable_hashagg = off; \
     SET LOCAL enable_nestloop = off; \
     SET LOCAL enable_hashjoin = off";

/// The most rows that one fetch reads from `record`, bound as the limit of the fetch queries. The
/// fetch service reads again from the last offset it received, so a limit that ends a read early
/// costs one more query. Without it, one fetch costs as much as the whole backlog between the fetch
/// offset and the high watermark.
const ROWS_PER_FETCH: i64 = 10_000;

/// How the records statement of a fetch ended on its connection.
///
/// A read returns an error instead when no statement runs on the connection: it failed before its
/// records statement started, or the server ended the statement with an error.
#[derive(Debug)]
enum RecordsStatement {
    /// The statement sent all of its rows, and its transaction committed, so no statement runs on
    /// the connection.
    Ended,
    /// The read stopped before its statement ended, because a row failed to decode or the caller
    /// stopped taking records. The statement can still be running on the connection.
    Abandoned(Error),
}

/// How long a caller waits for a free pooled connection before failing fast with a retriable
/// error, rather than queuing indefinitely while the pool is exhausted.
const DEFAULT_POOL_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// How long establishing a brand new connection to Postgres may take when the pool needs to
/// grow, both at the deadpool level (which bounds the full TCP + TLS + auth handshake via
/// `Manager::create`) and, as defense in depth, directly on `tokio_postgres::Config`'s own
/// `connect_timeout` (which in this version only bounds the socket-level TCP connect per
/// resolved address, not the TLS/auth steps that follow it).
const DEFAULT_POOL_CREATE_TIMEOUT: Duration = Duration::from_secs(10);

/// The start of the `application_name` of the broker's connections when the storage URL sets
/// none, so that `application_name LIKE 'nisshi%'` finds every Nisshi connection in
/// `pg_stat_activity`. [`application_name`] adds the host and the cluster.
const APPLICATION_NAME_PREFIX: &str = "nisshi";

/// The longest `application_name` that the server keeps. It cuts a longer name to
/// `NAMEDATALEN - 1`, which is 63 bytes, without an error.
const APPLICATION_NAME_MAX_LEN: usize = 63;

/// The name of the machine that this process runs on, from the `HOSTNAME` environment variable
/// or, where that is not set, from `/etc/hostname`. On Kubernetes it is the name of the pod, and
/// in a container the container's id or its `--hostname`. `None` when neither gives a name.
fn local_host_name() -> Option<String> {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|host| host.trim().to_owned())
        .filter(|host| !host.is_empty())
}

/// Keeps the ASCII letters, digits, `.`, `_` and `-` of `text` and replaces every other
/// character with `_`, because the server replaces a byte outside printable ASCII with `?` and a
/// name with a space or a quote is hard to filter on.
fn sanitised(text: &str) -> impl Iterator<Item = char> + '_ {
    text.chars().map(|c| {
        if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
            c
        } else {
            '_'
        }
    })
}

/// The `application_name` of the broker's connections when the storage URL sets none:
/// `nisshi-<host>-<cluster>`, or `nisshi-<node>-<cluster>` where the host is not known. A row in
/// `pg_stat_activity` then names the machine of the broker, which on Kubernetes is its pod, and
/// the cluster, also where a pooler or proxy hides `client_addr`.
///
/// Both parts go through [`sanitised`]. The host comes before the cluster and the name is cut at
/// [`APPLICATION_NAME_MAX_LEN`], so the cut removes the end of the cluster first, and the end of
/// the host only when the host alone is longer than the limit allows.
fn application_name(host: Option<&str>, cluster: &str, node: i32) -> String {
    let mut name = match host {
        Some(host) if !host.is_empty() => format!("{APPLICATION_NAME_PREFIX}-"),
        _ => format!("{APPLICATION_NAME_PREFIX}-{node}"),
    };

    if let Some(host) = host.filter(|host| !host.is_empty()) {
        name.extend(sanitised(host));
    }

    name.push('-');
    name.extend(sanitised(cluster));
    name.truncate(APPLICATION_NAME_MAX_LEN);

    name
}

/// How long [`ReadOnlyCheck::run`] may take for one connection. A connection whose peer vanished
/// without a reset passes the pool's `is_closed` check, and without a bound the query would wait
/// for TCP to give up. On a timeout the pool drops that connection and tries the next idle one,
/// which gets its own timeout, so the pool opens a new connection only when no idle one is left. A
/// checkout can therefore take up to this timeout for each idle connection, plus
/// [`DEFAULT_POOL_CREATE_TIMEOUT`] for a new connection, plus up to this timeout again for the
/// check of that new connection, and `connection()` puts no other bound around the checkout.
///
/// The pool applies the same value to its own recycle step, which only checks that the socket is
/// open.
const DEFAULT_POOL_RECYCLE_TIMEOUT: Duration = Duration::from_secs(5);

/// The storage URL option that sets how often the pool checks a connection for a read-only
/// session, see [`ReadOnlyCheck`].
const READ_ONLY_CHECK_EVERY: &str = "read_only_check_every";

/// How many checkouts of an idle connection pass between two checks when the storage URL does not
/// set [`READ_ONLY_CHECK_EVERY`].
///
/// A check on every checkout adds a round trip to each one. Against a local PostgreSQL it halved
/// the throughput of a pool that runs statements as short as the check, and cost 8% to 16% with a
/// statement of a few milliseconds, where every fourth checkout cost nothing that the noise of the
/// runs hid. A connection that turns read-only then serves at most 3 checkouts before the check
/// finds it, and each of those fails its writes with a transient error that the client retries.
const DEFAULT_READ_ONLY_CHECK_EVERY: u32 = 4;

/// The range of [`READ_ONLY_CHECK_EVERY`]. 0 turns the check off.
const READ_ONLY_CHECK_EVERY_RANGE: RangeInclusive<u32> = 0..=1_000_000;

/// The values that [`READ_ONLY_CHECK_EVERY`] takes, as an error message gives them.
const READ_ONLY_CHECK_EVERY_EXPECTED: &str =
    "a whole number from 0 to 1000000, where 0 turns the check off";

/// Run on a pooled connection to find out whether its session is read-only. It fails when it is,
/// with a failed integer cast whose message says so.
///
/// After a failover, the old primary can stay up and turn read-only. A connection to it still
/// looks open, so [`RecyclingMethod::Fast`] and [`RecyclingMethod::Verified`] keep it, and
/// every write on it fails with SQLSTATE 25006 until the broker restarts. The check costs one
/// round trip, as `Verified` does.
///
/// The failure is a failed integer cast, not `RAISE` in a `DO` block, so the check needs no
/// privilege on a procedural language: a role without `USAGE` on plpgsql would otherwise
/// fail every check and open a connection for every checkout. The message is concatenated
/// with a stable function so the planner doesn't fold the cast into an error for every
/// session.
const RECYCLE_QUERY: &str = "SELECT CASE WHEN pg_is_in_recovery() \
    OR current_setting('transaction_read_only')::bool \
    THEN ('connection is read-only, transaction_read_only=' \
    || current_setting('transaction_read_only'))::int END";

/// The text that [`RECYCLE_QUERY`] puts in its error when the session is read-only. The server's
/// message quotes it inside an `invalid input syntax for type integer` error.
const READ_ONLY_MESSAGE: &str = "connection is read-only";

/// Where a pooled connection failed [`ReadOnlyCheck::run`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CheckStage {
    /// A new connection, before its first use.
    Create,
    /// An idle connection, at its checkout.
    Recycle,
}

impl CheckStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Recycle => "recycle",
        }
    }
}

/// Why a pooled connection failed [`ReadOnlyCheck::run`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CheckFailure {
    /// The session is read-only.
    ReadOnly,
    /// The connection is closed.
    Closed,
    /// The check took longer than [`DEFAULT_POOL_RECYCLE_TIMEOUT`].
    Timeout,
    /// Any other error from the server or the socket.
    Error,
}

impl CheckFailure {
    fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::Closed => "closed",
            Self::Timeout => "timeout",
            Self::Error => "error",
        }
    }

    fn of(error: &tokio_postgres::Error) -> Self {
        match error.as_db_error() {
            Some(db) if db.message().contains(READ_ONLY_MESSAGE) => Self::ReadOnly,
            Some(_) => Self::Error,
            None if error.is_closed() => Self::Closed,
            None => Self::Error,
        }
    }
}

/// The check that keeps a pooled connection to a read-only server out of use, set up by
/// [`PoolParts::open`] unless the storage URL sets [`READ_ONLY_CHECK_EVERY`] to 0.
///
/// It runs [`RECYCLE_QUERY`] on every new connection before its first use, and on every
/// `every`-th checkout of an idle connection. A connection that fails is dropped, and the pool
/// tries the next idle one. A new connection that fails makes the checkout fail with a
/// [`PoolError::PostCreateHook`], which is a transient error to the caller, so a broker whose
/// endpoint still reaches a read-only server fails each checkout fast instead of using the
/// connection for one request and dropping it at the next. A new connection to a read-only
/// server also turns on the pool's rate limit, so the pool opens about one connection a second
/// until a new connection passes, see [`ConnectRateLimit`].
///
/// The pool logs a failed recycle only as the error's `Display`, which tokio-postgres limits to
/// `db error`, and logs nothing for a timeout, so this check logs each failure itself, with the
/// server's message, and counts it in [`POOL_REPLACED`].
#[derive(Clone, Debug)]
struct ReadOnlyCheck {
    /// Check every `every`-th checkout of an idle connection. 1 checks every checkout.
    every: u32,
    cluster: String,
    replaced: Counter<u64>,
}

impl ReadOnlyCheck {
    fn new(every: u32, cluster: &str) -> Self {
        Self::counting(every, cluster, POOL_REPLACED.clone())
    }

    fn counting(every: u32, cluster: &str, replaced: Counter<u64>) -> Self {
        Self {
            every,
            cluster: cluster.to_owned(),
            replaced,
        }
    }

    /// Checks `client`, and logs and counts a failure.
    async fn run(
        &self,
        stage: CheckStage,
        client: &deadpool_postgres::ClientWrapper,
    ) -> std::result::Result<(), CheckFailure> {
        let failure = if client.is_closed() {
            warn!(
                cluster = self.cluster,
                stage = stage.as_str(),
                reason = CheckFailure::Closed.as_str(),
                "a pooled connection failed its check and is dropped"
            );
            CheckFailure::Closed
        } else {
            match timeout(
                DEFAULT_POOL_RECYCLE_TIMEOUT,
                client.simple_query(RECYCLE_QUERY),
            )
            .await
            {
                Ok(Ok(_)) => return Ok(()),
                Ok(Err(error)) => {
                    let failure = CheckFailure::of(&error);
                    warn!(
                        cluster = self.cluster,
                        stage = stage.as_str(),
                        reason = failure.as_str(),
                        ?error,
                        "a pooled connection failed its check and is dropped"
                    );
                    failure
                }
                Err(_elapsed) => {
                    warn!(
                        cluster = self.cluster,
                        stage = stage.as_str(),
                        reason = CheckFailure::Timeout.as_str(),
                        timeout = ?DEFAULT_POOL_RECYCLE_TIMEOUT,
                        "a pooled connection failed its check and is dropped"
                    );
                    CheckFailure::Timeout
                }
            }
        };

        self.replaced.add(
            1,
            &[
                KeyValue::new("cluster_id", self.cluster.clone()),
                KeyValue::new("stage", stage.as_str()),
                KeyValue::new("reason", failure.as_str()),
            ],
        );

        Err(failure)
    }

    /// Returns the `post_create` hook, which runs this [`ReadOnlyCheck`] on each new connection.
    /// The hook starts or restarts `rate_limit` when the session is read-only, and stops it when
    /// the connection passes the check. It logs when the limit turns on or off.
    fn post_create(&self, rate_limit: Arc<ConnectRateLimit>) -> Hook {
        let check = self.clone();

        Hook::async_fn(move |client, _metrics| {
            let check = check.clone();
            let rate_limit = rate_limit.clone();

            Box::pin(async move {
                match check.run(CheckStage::Create, client).await {
                    Ok(()) => {
                        if rate_limit.stop() {
                            info!(
                                cluster = check.cluster,
                                "a new Postgres connection passed its check, so the pool no \
                                longer limits new connections"
                            );
                        }
                        Ok(())
                    }

                    Err(failure) => {
                        // Only a read-only session starts the rate limit, instead of every
                        // failure. A read-only server accepts each connection before the check
                        // runs, so each attempt costs the server a handshake and a new backend.
                        // A timeout slows each attempt by itself, and a rate limit after another
                        // error hides that error behind the rate-limited error, which logs only
                        // at `debug`.
                        if failure == CheckFailure::ReadOnly && rate_limit.start_or_restart() {
                            warn!(
                                cluster = check.cluster,
                                interval = ?RATE_LIMITED_CONNECT_INTERVAL,
                                "a new Postgres connection reached a read-only server, so the \
                                pool opens at most one new connection per interval until a new \
                                connection passes its check"
                            );
                        }

                        Err(HookError::message(format!(
                            "a new connection failed its check: {}",
                            failure.as_str()
                        )))
                    }
                }
            })
        })
    }

    /// The hook that checks an idle connection at its checkout, on every `every`-th one.
    fn pre_recycle(&self) -> Hook {
        let check = self.clone();

        Hook::async_fn(move |client, metrics| {
            let check = check.clone();

            Box::pin(async move {
                if metrics.recycle_count % usize::try_from(check.every).unwrap_or(usize::MAX) != 0 {
                    return Ok(());
                }

                check
                    .run(CheckStage::Recycle, client)
                    .await
                    .map_err(|failure| {
                        HookError::message(format!(
                            "an idle connection failed its check: {}",
                            failure.as_str()
                        ))
                    })
            })
        })
    }
}

/// Merges a default `statement_timeout` into an existing libpq `options` startup string.
///
/// Postgres applies `-c name=value` startup options in order with last-one-wins semantics,
/// so the default is prepended: an operator's own explicit `statement_timeout` (if present in
/// `existing`) is appended after it and therefore takes precedence.
fn merge_statement_timeout_option(existing: Option<&str>, default_ms: u64) -> String {
    let default_option = format!("-c statement_timeout={default_ms}");

    match existing {
        Some(existing) if !existing.is_empty() => format!("{default_option} {existing}"),
        _ => default_option,
    }
}

/// Returns the `name=value` settings of `options`, a libpq options startup
/// string, in order. The server reads `-c name=value`, `-cname=value` and
/// `--name=value`, treats `-` in a name as `_` and ignores its case, so the names
/// returned are lowercase with that done.
fn option_settings(options: &str) -> impl Iterator<Item = (String, &str)> {
    let mut tokens = options.split_whitespace();

    std::iter::from_fn(move || {
        loop {
            let token = tokens.next()?;
            let setting = match token {
                "-c" => tokens.next(),
                short if short.starts_with("-c") => short.strip_prefix("-c"),
                long => long.strip_prefix("--"),
            };

            if let Some((name, value)) = setting.and_then(|setting| setting.split_once('=')) {
                return Some((name.replace('-', "_").to_ascii_lowercase(), value));
            }
        }
    })
}

/// Reads the `statement_timeout` that `options`, a libpq options startup
/// string, sets on each connection: the last one wins, as on the server.
///
/// Returns `None` when `options` sets no timeout, disables it with `0`, or
/// sets a value the server rejects, in which case no connection opens.
fn statement_timeout_option(options: &str) -> Option<Duration> {
    option_settings(options)
        .filter(|(name, _)| name == "statement_timeout")
        .last()
        .and_then(|(_, value)| statement_timeout_value(value))
        .filter(|timeout| !timeout.is_zero())
}

/// Whether `options`, a libpq options startup string, sets `application_name`.
fn options_set_application_name(options: &str) -> bool {
    option_settings(options).any(|(name, _)| name == "application_name")
}

/// Parses a `statement_timeout` value as the server does: a number of
/// milliseconds, or a number with one of the server's time units, with a
/// fraction allowed either way.
fn statement_timeout_value(value: &str) -> Option<Duration> {
    let number = value.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    let unit = &value[number.len()..];
    let count = number.parse::<f64>().ok()?;

    let seconds = match unit {
        "us" => count / 1_000_000.0,
        "" | "ms" => count / 1_000.0,
        "s" => count,
        "min" => count * 60.0,
        "h" => count * 3_600.0,
        "d" => count * 86_400.0,
        _ => return None,
    };

    Duration::try_from_secs_f64(seconds).ok()
}

/// Removes every [`POOL_MAX_SIZE`] option from `config`, a connection URL,
/// and returns the pool size that the last one sets, or
/// [`DEFAULT_POOL_MAX_SIZE`] when the URL sets none.
///
/// Returns [`Error::InvalidStorageOptionValue`] when any of them is not a
/// number in [`POOL_MAX_SIZE_RANGE`], as [`parse_storage_option`] does for
/// every storage option.
fn take_pool_max_size(config: &mut String) -> Result<usize> {
    take_option(config, POOL_MAX_SIZE, POOL_MAX_SIZE_EXPECTED, |value| {
        value
            .parse()
            .ok()
            .filter(|size| POOL_MAX_SIZE_RANGE.contains(size))
    })
    .map(|size| size.unwrap_or(DEFAULT_POOL_MAX_SIZE))
}

/// Removes every [`CLIENT_READ_SHARE`] option from `config`, a connection
/// URL, and returns the percentage that the last one sets, or `None` when the
/// URL sets none.
///
/// Returns [`Error::InvalidStorageOptionValue`] when any of them is not a
/// number in [`CLIENT_READ_SHARE_RANGE`].
fn take_client_read_share(config: &mut String) -> Result<Option<usize>> {
    take_option(
        config,
        CLIENT_READ_SHARE,
        CLIENT_READ_SHARE_EXPECTED,
        |value| {
            value
                .parse()
                .ok()
                .filter(|percent| CLIENT_READ_SHARE_RANGE.contains(percent))
        },
    )
}

/// Removes every [`READ_ONLY_CHECK_EVERY`] option from `config`, a connection URL, and returns
/// the value that the last one sets, or [`DEFAULT_READ_ONLY_CHECK_EVERY`] when the URL sets none.
///
/// Returns [`Error::InvalidStorageOptionValue`] when any of them is not a number in
/// [`READ_ONLY_CHECK_EVERY_RANGE`].
fn take_read_only_check_every(config: &mut String) -> Result<u32> {
    take_option(
        config,
        READ_ONLY_CHECK_EVERY,
        READ_ONLY_CHECK_EVERY_EXPECTED,
        |value| {
            value
                .parse()
                .ok()
                .filter(|every| READ_ONLY_CHECK_EVERY_RANGE.contains(every))
        },
    )
    .map(|every| every.unwrap_or(DEFAULT_READ_ONLY_CHECK_EVERY))
}

/// Removes every `option` from `config`, a connection URL, and returns what
/// `parse` makes of the last one's value, or `None` when the URL sets none.
///
/// tokio-postgres rejects an option that it does not read, so a storage
/// option must be removed before it parses the URL.
fn take_option<T>(
    config: &mut String,
    option: &str,
    expected: &'static str,
    parse: impl Fn(&str) -> Option<T>,
) -> Result<Option<T>> {
    // A key/value connection string, such as `host=db user=nisshi`, is not a
    // URL, and sets no storage URL options.
    let Ok(storage) = Url::parse(config) else {
        return Ok(None);
    };

    let Some(value) = parse_storage_option(&storage, option, expected, parse)? else {
        return Ok(None);
    };

    let Some((base, query)) = config.split_once('?') else {
        return Ok(Some(value));
    };

    // The retained options keep their bytes, instead of going through
    // `Url::query_pairs_mut`, because that writes a space as `+`, and
    // tokio-postgres reads a `+` as itself.
    let retained = query
        .split('&')
        .filter(|candidate| {
            form_urlencoded::parse(candidate.as_bytes())
                .next()
                .is_none_or(|(key, _)| key != option)
        })
        .collect::<Vec<_>>();

    *config = if retained.is_empty() {
        base.to_owned()
    } else {
        format!("{base}?{}", retained.join("&"))
    };

    Ok(Some(value))
}

impl<C, N> FromStr for Builder<C, N, Url, PoolParts>
where
    C: Default,
    N: Default,
{
    type Err = Error;

    fn from_str(config: &str) -> Result<Self, Self::Err> {
        let mut config = config.to_owned();
        let pool_max_size = take_pool_max_size(&mut config)?;
        let read_only_check_every = take_read_only_check_every(&mut config)?;
        let client_percent = take_client_read_share(&mut config)?;
        let read_limits = ReadLimits {
            client_percent,
            ..ReadLimits::for_pool(pool_max_size)
        };
        info!(
            pool_max_size,
            list_offsets_reads = read_limits.list_offsets,
            fetch_reads = read_limits.fetch,
            list_offsets_reads_per_client = read_limits.list_offsets_per_client(),
            fetch_reads_per_client = read_limits.fetch_per_client(),
        );

        let mut pg_config = Config::from_str(&config).inspect(|pg_config| debug!(?pg_config))?;

        let options =
            merge_statement_timeout_option(pg_config.get_options(), DEFAULT_STATEMENT_TIMEOUT_MS);
        let abandoned_statement_bound = statement_timeout_option(&options)
            .unwrap_or(Duration::from_millis(DEFAULT_STATEMENT_TIMEOUT_MS))
            + ABANDONED_STATEMENT_MARGIN;

        // An operator's own `application_name` wins over the default, whether it is in the URL
        // or in `options`. The server applies a named `application_name` after `options`, so a
        // default set here would override one that only `options` sets.
        let named_by_operator =
            pg_config.get_application_name().is_some() || options_set_application_name(&options);

        _ = pg_config.options(options);
        _ = pg_config.connect_timeout(DEFAULT_POOL_CREATE_TIMEOUT);

        let root_store = {
            let mut roots = rustls::RootCertStore::empty();
            for cert in rustls_native_certs::load_native_certs().certs {
                roots.add(cert).inspect_err(|err| debug!(?err))?;
            }
            roots
        };

        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map(|config| config.with_root_certificates(root_store))
        .map(|config| config.with_no_client_auth())?;

        let tls = MakeRustlsConnect::new(config);

        // The cluster and the node are not known here, so this pool carries the plain prefix as
        // its name. `Builder::build` opens the pool that names them.
        let mut default_config = pg_config.clone();

        if !named_by_operator {
            _ = default_config.application_name(APPLICATION_NAME_PREFIX);
        }

        let cancel = Some(CancelRequests {
            config: default_config.clone(),
            tls: tls.clone(),
        });

        let pool = PoolParts::open(
            default_config,
            tls.clone(),
            pool_max_size,
            &ReadOnlyCheck::new(read_only_check_every, ""),
        )?;
        let advertised_listener = Url::parse("tcp://127.0.0.1/")?;

        Ok(Self {
            pool: PoolParts {
                config: pg_config,
                tls,
                pool_max_size,
                named_by_operator,
                read_only_check_every,
                host: local_host_name(),
                pool,
            },
            read_limits,
            cancel,
            abandoned_statement_bound,
            advertised_listener,
            node: N::default(),
            cluster: C::default(),
            schemas: None,
            lake: None,
        })
    }
}

/// What a cancel request needs to reach the server on a connection of its
/// own: the connection config, and the TLS connector.
#[derive(Clone)]
struct CancelRequests {
    config: Config,
    tls: MakeRustlsConnect,
}

impl Debug for CancelRequests {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CancelRequests")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// A cancel request that did not reach the server, and the host it was
/// sent to.
#[derive(Debug)]
struct CancelFailed {
    host: String,
    error: Error,
}

/// The transport a cancel socket connects over: a Unix socket is a second
/// variant only where [`Host::Unix`] can occur, since Postgres accepts one
/// only on a Unix domain socket.
#[cfg(unix)]
type CancelTransport = Either<TcpStream, UnixStream>;
#[cfg(not(unix))]
type CancelTransport = TcpStream;

/// Wraps a TCP connection as a [`CancelTransport`], matching whichever
/// variant that type is on this platform.
#[cfg(unix)]
fn tcp_cancel_transport(stream: TcpStream) -> CancelTransport {
    Either::Left(stream)
}
#[cfg(not(unix))]
fn tcp_cancel_transport(stream: TcpStream) -> CancelTransport {
    stream
}

/// A socket to one configured host, with the host name that TLS verifies
/// and the name a log line gives the host.
struct CancelSocket {
    stream: CancelTransport,
    tls_hostname: Option<String>,
    host: String,
}

impl CancelRequests {
    /// Asks the server to cancel the statement that `token` identifies, and
    /// returns the host it asked once the server has passed the request on
    /// to its backend.
    ///
    /// [`CancelToken::cancel_query`] returns as soon as it has written the
    /// request, so a statement sent on the connection after it could be the
    /// one cancelled instead. The server closes the cancel connection once
    /// it has signalled the backend, and libpq's `PQcancel` waits for that
    /// close; so does this, through [`ClosedByPeer`].
    ///
    /// This relies on a direct connection to PostgreSQL: a proxy that
    /// closes the cancel connection before forwarding the request breaks
    /// the ordering, for libpq as well.
    ///
    /// The request goes to the first configured host that accepts a
    /// connection, as a new connection would. A server other than the one
    /// running the statement ignores it, and the statement then runs until
    /// it ends on its own or at `statement_timeout`.
    async fn cancel(&self, token: &CancelToken) -> std::result::Result<String, CancelFailed> {
        let CancelSocket {
            stream,
            tls_hostname,
            host,
        } = self.connect().await?;

        let tls = match MakeTlsConnect::<CancelStream>::make_tls_connect(
            &mut self.tls.clone(),
            tls_hostname.as_deref().unwrap_or_default(),
        ) {
            Ok(tls) => tls,
            Err(err) => {
                return Err(CancelFailed {
                    host,
                    error: io::Error::other(err).into(),
                });
            }
        };

        match timeout(
            DEFAULT_POOL_CREATE_TIMEOUT,
            token.cancel_query_raw(ClosedByPeer::new(stream), tls),
        )
        .await
        {
            Ok(Ok(())) => Ok(host),
            Ok(Err(err)) => Err(CancelFailed {
                host,
                error: err.into(),
            }),
            Err(elapsed) => Err(CancelFailed {
                host,
                error: io::Error::from(elapsed).into(),
            }),
        }
    }

    /// Opens a socket to the first configured host that accepts one. The
    /// error names the last host tried.
    async fn connect(&self) -> std::result::Result<CancelSocket, CancelFailed> {
        let hosts = self.config.get_hosts();
        let ports = self.config.get_ports();
        let hostaddrs = self.config.get_hostaddrs();
        let connect_timeout = self
            .config
            .get_connect_timeout()
            .copied()
            .unwrap_or(DEFAULT_POOL_CREATE_TIMEOUT);

        let mut last = CancelFailed {
            host: "none".into(),
            error: io::Error::new(io::ErrorKind::NotFound, "no host configured").into(),
        };

        for (index, host) in hosts.iter().enumerate() {
            let port = ports.get(index).or(ports.first()).copied().unwrap_or(5432);

            let name = match (hostaddrs.get(index), host) {
                (Some(addr), _) => format!("{addr}:{port}"),
                (None, Host::Tcp(host)) => format!("{host}:{port}"),
                #[cfg(unix)]
                (None, Host::Unix(dir)) => {
                    dir.join(format!(".s.PGSQL.{port}")).display().to_string()
                }
            };

            let connect = async {
                match (hostaddrs.get(index), host) {
                    (Some(addr), _) => TcpStream::connect((*addr, port))
                        .await
                        .map(tcp_cancel_transport),

                    (None, Host::Tcp(host)) => TcpStream::connect((host.as_str(), port))
                        .await
                        .map(tcp_cancel_transport),

                    #[cfg(unix)]
                    (None, Host::Unix(dir)) => {
                        UnixStream::connect(dir.join(format!(".s.PGSQL.{port}")))
                            .await
                            .map(Either::Right)
                    }
                }
            };

            match timeout(connect_timeout, connect).await {
                Ok(Ok(stream)) => {
                    let tls_hostname = match host {
                        Host::Tcp(hostname) => Some(hostname.clone()),
                        #[cfg(unix)]
                        Host::Unix(_) => None,
                    };

                    return Ok(CancelSocket {
                        stream,
                        tls_hostname,
                        host: name,
                    });
                }

                Ok(Err(err)) => {
                    last = CancelFailed {
                        host: name,
                        error: err.into(),
                    }
                }

                Err(_) => {
                    last = CancelFailed {
                        host: name,
                        error: io::Error::new(io::ErrorKind::TimedOut, "connect timed out").into(),
                    }
                }
            }
        }

        Err(last)
    }
}

/// The stream a cancel request travels on.
type CancelStream = ClosedByPeer<CancelTransport>;

/// A stream whose shutdown also waits for the peer to close its side.
struct ClosedByPeer<S> {
    stream: S,
    writes_shut: bool,
}

impl<S> ClosedByPeer<S> {
    fn new(stream: S) -> Self {
        Self {
            stream,
            writes_shut: false,
        }
    }
}

impl<S> AsyncRead for ClosedByPeer<S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for ClosedByPeer<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.writes_shut {
            match ready!(Pin::new(&mut self.stream).poll_shutdown(cx)) {
                // A peer that has already closed makes some systems refuse
                // the shutdown, and the close is what this waits for.
                Ok(()) | Err(_) => self.writes_shut = true,
            }
        }

        let mut scratch = [0; 64];

        loop {
            let mut buf = ReadBuf::new(&mut scratch);

            match ready!(Pin::new(&mut self.stream).poll_read(cx, &mut buf)) {
                Ok(()) if buf.filled().is_empty() => return Poll::Ready(Ok(())),
                Ok(()) => {}
                // An abrupt close is still a close.
                Err(err)
                    if matches!(
                        err.kind(),
                        io::ErrorKind::ConnectionReset
                            | io::ErrorKind::ConnectionAborted
                            | io::ErrorKind::BrokenPipe
                            | io::ErrorKind::UnexpectedEof
                    ) =>
                {
                    return Poll::Ready(Ok(()));
                }
                Err(err) => return Poll::Ready(Err(err)),
            }
        }
    }
}

/// A pooled object that goes back to its pool only through
/// [`Abandonable::release`].
///
/// The pool recycles a connection after checking only that it is open and
/// writable ([`RECYCLE_QUERY`]), not that it is idle. A connection dropped while a statement runs
/// on the server would go back to the pool still busy, and the next caller
/// would wait behind that statement. Dropping this guard instead passes the
/// object, still checked out of its pool, to `abandon`, which decides when
/// the object may go back.
struct Abandonable<M, F>
where
    M: managed::Manager,
    F: FnOnce(managed::Object<M>),
{
    object: Option<managed::Object<M>>,
    abandon: Option<F>,
}

impl<M, F> Abandonable<M, F>
where
    M: managed::Manager,
    F: FnOnce(managed::Object<M>),
{
    fn new(object: managed::Object<M>, abandon: F) -> Self {
        Self {
            object: Some(object),
            abandon: Some(abandon),
        }
    }

    fn object(&self) -> Result<&managed::Object<M>> {
        self.object
            .as_ref()
            .ok_or_else(|| Error::Message("the abandonable object has been released".into()))
    }

    fn object_mut(&mut self) -> Result<&mut managed::Object<M>> {
        self.object
            .as_mut()
            .ok_or_else(|| Error::Message("the abandonable object has been released".into()))
    }

    /// Returns the object to its pool, once its work has finished.
    fn release(mut self) {
        drop(self.object.take());
    }
}

impl<M, F> Drop for Abandonable<M, F>
where
    M: managed::Manager,
    F: FnOnce(managed::Object<M>),
{
    fn drop(&mut self) {
        if let (Some(object), Some(abandon)) = (self.object.take(), self.abandon.take()) {
            abandon(object);
        }
    }
}

impl<M, F> Debug for Abandonable<M, F>
where
    M: managed::Manager,
    F: FnOnce(managed::Object<M>),
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Abandonable")
            .field("held", &self.object.is_some())
            .finish()
    }
}

/// The query that [`cancel_and_return`] runs on an abandoned read's
/// connection. It answers only after the read's own statement has ended,
/// and it answers `t` only outside a transaction block.
///
/// Outside a block, the server runs the query in a transaction of its own,
/// so the transaction and the statement start at the same time. Inside a
/// block, the transaction started with an earlier statement, so the query
/// answers `f`. Inside a failed block, the server rejects the query.
///
/// A read can leave a block open: tokio-postgres queues a `ROLLBACK` when a
/// `Transaction` is dropped, but the cancel request ends that `ROLLBACK`
/// instead of the read's statement when the statement finishes just before
/// the cancel request arrives. The query detects the block without writing
/// to the server log, which a second `ROLLBACK` would do on every abandoned
/// read that left no block open.
const SETTLE_QUERY: &str = "SELECT transaction_timestamp() = statement_timestamp()";

/// The permits that a read holds while it is in flight: one from its
/// connection's share, see [`read_share`](nisshi_storage::read_share), and the
/// ones from its client's share and the engine's limit on reads in flight
/// ([`ReadLimits::list_offsets`] or [`ReadLimits::fetch`]).
///
/// An abandoned read keeps them until its statement ends, see
/// [`cancel_and_return`].
#[derive(Debug)]
// Nothing reads the permits: holding them is the point, and dropping
// `ReadPermits` returns them.
#[allow(dead_code)]
struct ReadPermits {
    /// `None` for a read that runs outside a connection's read share.
    connection_share: Option<OwnedSemaphorePermit>,
    /// The permit of the limit, and the one of the client's share. A read
    /// that runs outside a client's read share holds only the first, and still
    /// returns it through the client shares, which wake a read that waits for
    /// a permit of the limit.
    engine: ClientAdmission,
}

impl ReadPermits {
    /// Waits for `connection_share`, a permit from the connection's share,
    /// then for a permit from `limit`, and from the share of its client, with
    /// `client_share` permits, when the read has one, see
    /// [`ClientReadShares::acquire`].
    ///
    /// The connection's share comes first, so a connection never waits on
    /// `limit` for more reads than its share. `limit` is a FIFO queue, and
    /// those reads would hold places in it ahead of other clients' reads. A
    /// client's reads beyond its share do not queue for `limit` either, and
    /// the order is the same for every read, so reads cannot wait on each
    /// other in a cycle. A permit is returned when it is dropped.
    async fn acquire(
        connection_share: impl Future<Output = Result<Option<OwnedSemaphorePermit>, AcquireError>>,
        client_shares: &ClientReadShares,
        client_share: usize,
        limit: &Arc<Semaphore>,
    ) -> Result<Self> {
        let connection_share = connection_share.await?;
        let engine = client_shares.acquire(client_share, limit).await?;

        Ok(Self {
            connection_share,
            engine,
        })
    }
}

/// Waits for the statement on `connection` to end, then returns whether the
/// connection is outside any transaction block, see [`SETTLE_QUERY`].
async fn settled_outside_a_transaction(connection: &Object) -> Result<bool, tokio_postgres::Error> {
    connection.simple_query(SETTLE_QUERY).await.map(|messages| {
        messages.iter().any(
            |message| matches!(message, SimpleQueryMessage::Row(row) if row.get(0) == Some("t")),
        )
    })
}

/// Cancels the statement running on an abandoned `connection`, and returns
/// the connection to its pool once that statement has ended, so the pool
/// opens no more server connections than its maximum size.
///
/// The connection and `permits` are held until the statement has ended,
/// whether or not the cancel request reached the server, so the statement
/// counts against the limit that `permits` come from
/// ([`ReadLimits::list_offsets`] or [`ReadLimits::fetch`]), and against its
/// connection's share of that limit, meanwhile. The share keeps a connection
/// whose reads are abandoned in a row from holding the whole limit. A cancel
/// request that misses its statement (a host other than the one running the
/// statement, or a proxy that does not forward it) then shows as reads that
/// wait for a permit, under the `queued` stage, rather than as server
/// connections beyond the pool.
///
/// The connection goes back to the pool only when [`SETTLE_QUERY`] shows it
/// outside any transaction block. The connection is closed and taken out of
/// the pool instead, and the pool opens a replacement, when that query
/// fails, shows a transaction block, or has not answered within `bound`. A
/// connection inside a block would fail every statement of the next caller
/// that checks it out. The bound sits past the connection's
/// `statement_timeout`
/// ([`ABANDONED_STATEMENT_MARGIN`]), at which the server ends the statement
/// itself unless an operator has disabled the timeout. deadpool-postgres
/// closes a dropped connection's socket at once, and a backend notices a
/// closed socket only when it next sends a result, so a statement still
/// running then keeps its backend until it ends.
async fn cancel_and_return(
    connection: Object,
    cancel: Option<CancelRequests>,
    bound: Duration,
    permits: ReadPermits,
) {
    let _permits = permits;
    let settle_by = Instant::now() + bound;

    let host = match cancel {
        Some(cancel) => match cancel.cancel(&connection.cancel_token()).await {
            Ok(host) => host,

            Err(CancelFailed { host, error }) => {
                warn!(
                    host,
                    ?error,
                    ?bound,
                    "the cancel request for an abandoned statement did not reach the server; waiting for the statement to end"
                );
                host
            }
        },

        None => {
            warn!(
                ?bound,
                "no cancel request is configured for an abandoned statement; waiting for it to end"
            );
            "none".into()
        }
    };

    match timeout_at(settle_by, settled_outside_a_transaction(&connection)).await {
        Ok(Ok(true)) => drop(connection),

        Ok(Ok(false)) => {
            warn!(
                host,
                "an abandoned statement left its connection inside a transaction; closing it"
            );
            drop(Object::take(connection));
        }

        Ok(Err(error)) => {
            warn!(
                host,
                ?error,
                "an abandoned statement's connection failed its settle query; closing it"
            );
            drop(Object::take(connection));
        }

        Err(_elapsed) => {
            warn!(
                host,
                ?bound,
                "an abandoned statement did not end within the bound; closing its connection"
            );
            drop(Object::take(connection));
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Txn {
    name: String,
    producer_id: i64,
    producer_epoch: i16,
    status: TxnState,
}

impl TryFrom<Row> for Txn {
    type Error = Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let name = row
            .try_get::<_, String>(0)
            .inspect_err(|err| error!(?err))?;
        let producer_id = row.try_get::<_, i64>(1).inspect_err(|err| error!(?err))?;
        let producer_epoch = row.try_get::<_, i16>(2).inspect_err(|err| error!(?err))?;
        let status = row
            .try_get::<_, Option<String>>(3)
            .map_err(Into::into)
            .and_then(|status| status.map_or(Ok(TxnState::Begin), TxnState::try_from))
            .inspect_err(|err| error!(?err))?;

        Ok(Self {
            name,
            producer_id,
            producer_epoch,
            status,
        })
    }
}

/// Returns the `DescribeConfigs` entry for a config that a topic sets: a dynamic topic config of
/// type `STRING`. A null stored value is described as an empty string.
fn dynamic_topic_config(name: String, value: Option<String>) -> DescribeConfigsResourceResult {
    DescribeConfigsResourceResult::default()
        .name(name)
        .value(Some(value.unwrap_or_default()))
        .read_only(false)
        .is_default(Some(false))
        .config_source(Some(ConfigSource::DynamicTopicConfig.into()))
        .is_sensitive(false)
        .synonyms(Some([].into()))
        .config_type(Some(ConfigType::String.into()))
        .documentation(Some("".into()))
}

impl Postgres {
    /// Applies every change in `resource` to its topic in one transaction, so
    /// that a failed change leaves the topic's configuration unchanged.
    async fn alter_topic(&self, resource: &AlterConfigsResource) -> Result<()> {
        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let topic = &resource.resource_name;

        if self
            .tx_prepare_query_opt(
                &tx,
                "topic_select_name_for_update.sql",
                &[&self.cluster, topic],
            )
            .await?
            .is_none()
        {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        for config in resource.configs.as_deref().unwrap_or_default() {
            let op = OpType::try_from(config.config_operation)?;

            let current = if matches!(op, OpType::Append | OpType::Subtract) {
                self.tx_prepare_query_opt(
                    &tx,
                    "topic_configuration_select_name.sql",
                    &[&self.cluster, topic, &config.name],
                )
                .await?
                .map(|row| row.try_get::<_, Option<String>>(0))
                .transpose()?
                .flatten()
            } else {
                None
            };

            match apply_op(
                ConfigResource::Topic,
                &config.name,
                current.as_deref(),
                op,
                config.value.as_deref(),
            )? {
                Some(value) => {
                    _ = self
                        .tx_prepare_query_opt(
                            &tx,
                            "topic_configuration_upsert.sql",
                            &[&self.cluster, topic, &config.name, &value],
                        )
                        .await?;
                }

                None => {
                    _ = self
                        .tx_prepare_execute(
                            &tx,
                            "topic_configuration_delete.sql",
                            &[&self.cluster, topic, &config.name],
                        )
                        .await?;
                }
            }
        }

        tx.commit().await.map_err(Into::into)
    }

    pub fn builder(
        connection: &str,
    ) -> Result<Builder<PhantomData<String>, PhantomData<i32>, Url, PoolParts>> {
        // Do not log `connection` here: it is the raw, unparsed connection
        // string (e.g. `postgres://user:password@host/db`) and would leak
        // the password in plaintext. `Builder::from_str` below logs the
        // parsed `tokio_postgres::Config`, whose `Debug` impl redacts the
        // password.
        Builder::from_str(connection)
    }

    async fn connection(&self) -> Result<Object> {
        self.pool
            .get()
            .await
            .inspect_err(|err| match err {
                // routine under load: the pool is momentarily saturated, not broken.
                CheckoutError::Pool(PoolError::Timeout(TimeoutType::Wait)) => {
                    debug!(?err, cluster = ?self.cluster)
                }

                // The `post_create` hook logged at `warn` when the rate limit turned on, so each
                // checkout that the limit refuses logs only at `debug`.
                CheckoutError::RateLimited => {
                    debug!(?err, cluster = ?self.cluster);
                    metrics::pool_failed(&self.cluster);
                }

                // likely means Postgres itself is unreachable or misbehaving.
                CheckoutError::Pool(_) => {
                    error!(?err, cluster = ?self.cluster);
                    metrics::pool_failed(&self.cluster);
                }
            })
            .map_err(Into::into)
    }

    /// Checks out a connection for a read that a caller may drop
    /// mid-statement, holding the read's `permits`.
    ///
    /// Dropping the guard before [`Abandonable::release`] asks the server to
    /// cancel the statement, so it stops instead of running until
    /// `statement_timeout`, and keeps the connection and the permits until
    /// the statement has ended, see [`cancel_and_return`]. The drop counts
    /// in `nisshi_sql_statements_abandoned` under `operation`.
    async fn abandonable_connection(
        &self,
        permits: ReadPermits,
        operation: StorageOperation,
    ) -> Result<Abandonable<Manager, impl FnOnce(Object) + use<>>> {
        let cancel = self.cancel.clone();
        let bound = self.abandoned_statement_bound;
        let cluster = self.cluster.clone();

        self.connection().await.map(|connection| {
            Abandonable::new(connection, move |connection: Object| {
                metrics::abandoned(&cluster, operation);

                match Handle::try_current() {
                    // The task keeps the request's span, so its warnings
                    // name the peer, the correlation id and the partition.
                    Ok(runtime) => {
                        _ = runtime.spawn(
                            cancel_and_return(connection, cancel, bound, permits).in_current_span(),
                        );
                    }

                    // Without a runtime the cancel request cannot be sent.
                    // Closing the connection still keeps the pool from
                    // handing out one that is busy.
                    Err(_) => drop(Object::take(connection)),
                }
            })
        })
    }

    /// Waits for the permits of a Fetch read, of records or of an offset
    /// stage, see [`ReadLimits::fetch`].
    async fn fetch_read_permits(&self) -> Result<ReadPermits> {
        ReadPermits::acquire(
            FetchReadShare::acquire(self.fetch_reads_per_connection),
            &self.fetch_client_shares,
            self.fetch_reads_per_client,
            &self.fetch_reads,
        )
        .await
    }

    /// Waits for the permits of a ListOffsets read, see
    /// [`ReadLimits::list_offsets`].
    async fn list_offsets_read_permits(&self) -> Result<ReadPermits> {
        ReadPermits::acquire(
            ListOffsetsReadShare::acquire(self.list_offsets_reads_per_connection),
            &self.list_offsets_client_shares,
            self.list_offsets_reads_per_client,
            &self.list_offsets_reads,
        )
        .await
    }

    /// Reads the records of `topition` from `offset` on `c`, in one transaction, up to the
    /// partition's high watermark or, for `ReadCommitted`, its last stable offset. It sends the
    /// offset stage through `fetched_sender` first, then each record as its row arrives.
    ///
    /// The stage is read before the records and only grows, so it bounds every record sent. Each
    /// row holds one complete record, so a caller that drops the read keeps every record that it
    /// received.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_on(
        &self,
        c: &mut Object,
        topition: &Topition,
        base_topic: &str,
        key_filter: Option<&str>,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
        fetched_sender: FetchedSender,
    ) -> Result<RecordsStatement> {
        let offset_stage = self
            .offset_stage_on(c, base_topic, topition.partition())
            .await?;

        let high_watermark = if isolation_level == IsolationLevel::ReadCommitted {
            offset_stage.last_stable
        } else {
            offset_stage.high_watermark
        };

        debug!(
            cluster = self.cluster,
            ?topition,
            offset,
            ?isolation_level,
            high_watermark,
            min_bytes,
            max_bytes
        );

        let mut fetched_data = fetched_sender.send_offsets(offset_stage).await?;

        let tx = c.transaction().await?;

        tx.batch_execute(FETCH_PLAN_SETTINGS)
            .await
            .inspect_err(|err| error!(?err))?;

        let query = if key_filter.is_some() {
            pg_record_fetch::KEYED_QUERY
        } else {
            pg_record_fetch::QUERY
        };
        let sql = self.sql_lookup(query)?;

        let partition = topition.partition();
        let max_bytes = i64::from(max_bytes);
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![
            &self.cluster,
            &base_topic,
            &partition,
            &offset,
            &max_bytes,
            &high_watermark,
        ];
        params.extend(key_filter.as_ref().map(|key| key as &(dyn ToSql + Sync)));
        params.push(&ROWS_PER_FETCH);

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        // `query_raw` returns once the server has accepted the statement, and its stream yields
        // each row as the server sends it, so the read sends each record before the next row
        // arrives.
        let started_at = Instant::now();
        let rows = tx
            .query_raw(&prepared, params.iter().copied())
            .await
            .inspect_err(|err| metrics::failed(sql, &self.cluster, err))?;
        let mut statement = FetchStatementMetrics::started(sql, &self.cluster, started_at);
        pin_mut!(rows);

        while let Some(row) = statement.next_row(&mut rows).await {
            let row = match row {
                Ok(row) => row,
                // An error from the server ends the statement.
                Err(error) => {
                    metrics::failed(sql, &self.cluster, &error);
                    statement.failed();
                    return Err(error.into());
                }
            };

            let record = match pg_record_fetch::Row::try_from(&row) {
                Ok(record) => record,
                Err(error) => {
                    error!(?error);
                    metrics::decode_failed(sql, &self.cluster);
                    statement.failed();
                    return Ok(RecordsStatement::Abandoned(error));
                }
            };

            // A send fails when the caller has stopped taking records, which is a stop like the
            // drop of the read at its deadline, so the guard counts the statement as stopped.
            if let Err(error) = fetched_data.send_record(record.into()).await {
                debug!(?error);
                drop(statement);
                return Ok(RecordsStatement::Abandoned(error));
            }
        }

        statement.completed();

        // The transaction only reads, so a rollback has the same effect as a commit.
        tx.commit().await?;
        Ok(RecordsStatement::Ended)
    }

    /// Reads the log start, high watermark and last stable offset of
    /// `base_topic`'s `partition` on `c`.
    async fn offset_stage_on(
        &self,
        c: &Object,
        base_topic: &str,
        partition: i32,
    ) -> Result<OffsetStage> {
        let row = self
            .prepare_query_one(
                c,
                "watermark_select.sql",
                &[&self.cluster, &base_topic, &partition],
            )
            .await
            .inspect_err(|err| error!(base_topic, partition, ?err))?;

        let log_start = row
            .try_get::<_, Option<i64>>(0)
            .inspect_err(|err| error!(base_topic, partition, ?err))?
            .unwrap_or_default();

        let high_watermark = row
            .try_get::<_, Option<i64>>(1)
            .inspect_err(|err| error!(base_topic, partition, ?err))?
            .unwrap_or_default();

        let last_stable = row
            .try_get::<_, Option<i64>>(2)
            .inspect_err(|err| error!(base_topic, partition, ?err))?
            .unwrap_or(high_watermark);

        debug!(
            cluster = self.cluster,
            base_topic, partition, log_start, high_watermark,
        );

        Ok(OffsetStage {
            last_stable,
            high_watermark,
            log_start,
        })
    }

    /// Reads `offsets` one statement at a time on `c`.
    async fn list_offsets_on(
        &self,
        c: &Object,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let mut responses = vec![];

        for (topition, offset_type) in offsets {
            let query = match (offset_type, isolation_level) {
                (ListOffset::Earliest, _) => "list_earliest_offset.sql",
                (ListOffset::Latest, IsolationLevel::ReadCommitted) => {
                    "list_latest_offset_committed.sql"
                }
                (ListOffset::Latest, IsolationLevel::ReadUncommitted) => {
                    "list_latest_offset_uncommitted.sql"
                }
                (ListOffset::Timestamp(_), _) => "list_latest_offset_timestamp.sql",
            };

            debug!(?query);

            let list_offset = match offset_type {
                ListOffset::Earliest | ListOffset::Latest => self
                    .prepare_query_opt(
                        c,
                        query,
                        &[&self.cluster, &topition.topic(), &topition.partition()],
                    )
                    .await
                    .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition)),

                ListOffset::Timestamp(timestamp) => self
                    .prepare_query_opt(
                        c,
                        query,
                        &[
                            &self.cluster.as_str(),
                            &topition.topic(),
                            &topition.partition(),
                            timestamp,
                        ],
                    )
                    .await
                    .inspect_err(|err| error!(?err)),
            }
            .inspect_err(|err| {
                error!(?err, cluster = self.cluster, ?topition);
            })
            .inspect(|result| debug!(?result))?
            .map_or_else(
                || {
                    // Earliest and Latest find no row only when the partition does not exist,
                    // because creating a partition also creates its watermark row. A Timestamp
                    // lookup finds no row when no record matches, and also when the partition
                    // does not exist. Both answer no offset (see `ListOffsetResponse::offset`).
                    let error_code = match offset_type {
                        ListOffset::Earliest | ListOffset::Latest => {
                            ErrorCode::UnknownTopicOrPartition
                        }
                        ListOffset::Timestamp(_) => ErrorCode::None,
                    };
                    debug!(cluster = self.cluster, ?topition, ?offset_type, ?error_code);

                    Ok(ListOffsetResponse {
                        error_code,
                        ..Default::default()
                    })
                },
                |row| {
                    debug!(?row);

                    row.try_get::<_, i64>(0).map(Some).and_then(|offset| {
                        // A record without a timestamp is stored before the epoch, which
                        // `ListOffsetResponse::timestamp` cannot convert. The service answers -1
                        // for Earliest and Latest, as Kafka does, so that is harmless here.
                        row.try_get::<_, Option<SystemTime>>(1).map(|timestamp| {
                            debug!(
                                cluster = self.cluster,
                                ?topition,
                                ?offset_type,
                                offset,
                                ?timestamp
                            );

                            ListOffsetResponse {
                                timestamp,
                                offset,
                                ..Default::default()
                            }
                        })
                    })
                },
            )?;

            responses.push((topition.clone(), list_offset));
        }

        Ok(responses)
    }

    /// Makes sure that the database has the broker's schema before the broker serves a request.
    ///
    /// When the database records no schema version, this function creates the missing tables and
    /// records [`SCHEMA_VERSION`]. It leaves existing tables and their data as they are.
    ///
    /// This function runs no DDL on a database that records a version. A database user that cannot
    /// create tables can therefore start a broker on a database that records a version.
    pub(crate) async fn ensure_schema(&self) -> Result<()> {
        let mut c = self.connection().await?;

        // The broker reads the version without the lock first, because a broker that holds the
        // lock and then stops answering blocks every broker that waits for it. A recorded version
        // means a complete schema, because the transaction that creates the schema also records
        // the version.
        {
            let tx = c.transaction().await?;
            let recorded = self.recorded_schema(&tx).await?;
            if !should_create_schema(recorded, SCHEMA_VERSION)? {
                return tx.commit().await.map_err(Into::into);
            }
        }

        // This transaction is READ COMMITTED, regardless of the server's default isolation level.
        // A broker that waited for the lock must see the schema that another broker committed
        // during the wait. Each statement of a READ COMMITTED transaction sees every commit made
        // before the statement started. A REPEATABLE READ transaction sees only the commits made
        // before its first statement, and that statement ran before the wait.
        let tx = c
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
            .start()
            .await?;

        // This transaction sets its own `statement_timeout`, because an operator's short timeout
        // bounds a request and can abort this setup. The setup can wait for another broker's lock
        // below. A broker whose lock wait reaches 5 minutes exits. Its orchestrator then restarts
        // it.
        tx.batch_execute("SET LOCAL statement_timeout = '5min'")
            .await?;

        // The server ends this session when the session stays idle in this transaction for a
        // minute. The lock of a broker that stops answering is then released after a minute,
        // instead of when TCP keepalive notices that the broker is gone.
        tx.batch_execute("SET LOCAL idle_in_transaction_session_timeout = '1min'")
            .await?;

        // This lock makes a second broker wait until the first broker commits the schema. The
        // second broker then finds the recorded version. Without the lock, PostgreSQL fails one of
        // two concurrent `create table if not exists` statements with a unique violation.
        let locked_at_once = tx
            .query_one(
                &format!("select pg_try_advisory_xact_lock({SCHEMA_LOCK})"),
                &[],
            )
            .await?
            .try_get::<_, bool>(0)?;
        if !locked_at_once {
            info!("waiting for another broker to set up the schema");
            tx.batch_execute(&format!("select pg_advisory_xact_lock({SCHEMA_LOCK})"))
                .await?;
        }

        let recorded = self.recorded_schema(&tx).await?;
        if !should_create_schema(recorded, SCHEMA_VERSION)? {
            return tx.commit().await.map_err(Into::into);
        }

        // This query puts only the namespace that holds the existing `cluster` table on the search
        // path, for this transaction. PostgreSQL calls a namespace a schema. An unqualified `create
        // table` creates its table in the first namespace on the search path. That namespace can
        // differ from the one that holds the existing tables. The broker then finds new, empty
        // tables instead of its data. The query returns a row only when `cluster` exists.
        let has_tables = tx
            .query_opt(
                self.sql_lookup("pg/search_path_set_to_cluster_namespace.sql")?,
                &[],
            )
            .await?
            .is_some();

        tx.batch_execute(self.sql_lookup("nisshi_schema_version_create.sql")?)
            .await?;
        tx.batch_execute(SCHEMA).await?;
        _ = tx
            .execute(
                self.sql_lookup("nisshi_schema_version_insert.sql")?,
                &[&SCHEMA_VERSION, &OLDEST_COMPATIBLE_SCHEMA_VERSION],
            )
            .await?;

        tx.commit().await?;

        if has_tables {
            warn!(
                schema_version = SCHEMA_VERSION,
                "recorded the schema version in a database whose tables an earlier release created"
            );
        } else {
            info!(schema_version = SCHEMA_VERSION, "created the schema");
        }

        Ok(())
    }

    /// Returns the schema version that the database records, or `None` when it records none.
    async fn recorded_schema(&self, tx: &Transaction<'_>) -> Result<Option<RecordedSchema>> {
        let version_table_exists = tx
            .query_one(self.sql_lookup("pg/nisshi_schema_version_exists.sql")?, &[])
            .await?
            .try_get::<_, bool>(0)?;
        if !version_table_exists {
            return Ok(None);
        }

        let recorded = tx
            .query_opt(self.sql_lookup("nisshi_schema_version_select.sql")?, &[])
            .await?
            .map(|row| {
                Ok::<_, Error>(RecordedSchema {
                    version: row.try_get(0)?,
                    oldest_compatible_version: row.try_get(1)?,
                })
            })
            .transpose()?;
        debug!(?recorded);
        Ok(recorded)
    }

    fn sql_lookup(&self, key: &str) -> Result<&str> {
        crate::sql::SQL.get(key)
    }

    async fn idempotent_message_check(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
        tx: &Transaction<'_>,
    ) -> Result<()> {
        debug!(transaction_id, ?deflated);

        if let Some(row) = self
            .tx_prepare_query_opt(
                tx,
                producer_epochs_select::QUERY,
                &[
                    &self.cluster,
                    &deflated.producer_id,
                    &topition.topic(),
                    &topition.partition(),
                ],
            )
            .await
            .inspect_err(|err| error!(?err))?
        {
            let stored = StoredEpochs {
                transactional: row
                    .try_get::<_, i32>(producer_epochs_select::TRANSACTIONAL)
                    .inspect_err(|err| error!(?err))?
                    != 0,
                newest: row
                    .try_get(producer_epochs_select::NEWEST_EPOCH)
                    .inspect_err(|err| error!(?err))?,
                partition: row
                    .try_get(producer_epochs_select::PARTITION_EPOCH)
                    .inspect_err(|err| error!(?err))?,
            };

            if idempotent_epoch_check(transaction_id, stored, deflated)? == ProduceEpoch::Start {
                _ = self
                    .tx_prepare_execute(
                        tx,
                        "producer_epoch_insert_given.sql",
                        &[
                            &self.cluster,
                            &deflated.producer_id,
                            &deflated.producer_epoch,
                        ],
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;
            }

            let row = self
                .tx_prepare_query_one(
                    tx,
                    "producer_select_for_update.sql",
                    &[
                        &self.cluster,
                        &topition.topic(),
                        &topition.partition(),
                        &deflated.producer_id,
                        &deflated.producer_epoch,
                    ],
                )
                .await
                .inspect_err(|err| {
                    error!(
                        self.cluster,
                        ?topition,
                        deflated.producer_id,
                        deflated.producer_epoch,
                        ?err
                    )
                })?;

            let sequence = row.try_get::<_, i32>(0).inspect_err(|err| error!(?err))?;

            debug!(
                self.cluster,
                ?topition,
                deflated.producer_id,
                deflated.producer_epoch,
                ?stored,
                sequence,
            );

            let increment = idempotent_sequence_check(sequence, deflated)?;

            debug!(increment);

            assert_eq!(
                1,
                self.tx_prepare_execute(
                    tx,
                    "producer_detail_insert.sql",
                    &[
                        &self.cluster,
                        &topition.topic(),
                        &topition.partition(),
                        &deflated.producer_id,
                        &deflated.producer_epoch,
                        &increment,
                    ],
                )
                .await?
            );

            Ok(())
        } else {
            Err(Error::Api(ErrorCode::UnknownProducerId))
        }
    }

    /// Locks the partition's watermark row until `tx` ends. Returns the partition's low and high
    /// watermarks and its topic's stored `max.message.bytes`. Fails with
    /// `UNKNOWN_TOPIC_OR_PARTITION` when the partition does not exist.
    async fn watermark_select_for_write(
        &self,
        topition: &Topition,
        tx: &Transaction<'_>,
    ) -> Result<watermark_select_for_write::Row> {
        let Some(row) = self
            .tx_prepare_query_opt(
                tx,
                watermark_select_for_write::QUERY,
                &[&self.cluster, &topition.topic(), &topition.partition()],
            )
            .await
            .inspect_err(|err| error!(?err, cluster = ?self.cluster, ?topition))?
        else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        Ok(watermark_select_for_write::Row {
            low: row
                .try_get(watermark_select_for_write::LOW)
                .inspect_err(|err| error!(?err))?,
            high: row
                .try_get(watermark_select_for_write::HIGH)
                .inspect_err(|err| error!(?err))?,
            max_message_bytes: row
                .try_get(watermark_select_for_write::MAX_MESSAGE_BYTES)
                .inspect_err(|err| error!(?err))?,
        })
    }

    /// Advances one partition's log start for DeleteRecords, answering the
    /// error code and new low watermark.
    ///
    /// The watermark advance is the delete: it commits first, in a short
    /// transaction that holds the watermark row lock. Removing the records
    /// below it follows in a separate transaction under the default statement
    /// timeout. That removal is best effort: a failure is logged, and
    /// `maintain` removes the rows on its next run.
    async fn delete_records_partition(
        &self,
        c: &mut Object,
        topition: &Topition,
        offset: i64,
    ) -> Result<(ErrorCode, i64)> {
        let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

        let watermark_select_for_write::Row { low, high, .. } =
            self.watermark_select_for_write(topition, &tx).await?;

        let stage = OffsetStage {
            last_stable: high.unwrap_or_default(),
            high_watermark: high.unwrap_or_default(),
            log_start: low.unwrap_or_default(),
        };

        let cutoff = match delete_records_cutoff(offset, &stage) {
            Ok(cutoff) => cutoff,
            Err(error_code) => return Ok((error_code, DELETE_RECORDS_INVALID_LOW_WATERMARK)),
        };

        _ = self
            .tx_prepare_execute(
                &tx,
                "watermark_update.sql",
                &[
                    &self.cluster,
                    &topition.topic(),
                    &topition.partition(),
                    &cutoff,
                    &high.unwrap_or_default(),
                ],
            )
            .await
            .inspect_err(|err| error!(?err))?;

        tx.commit().await.inspect_err(|err| error!(?err))?;

        // A produce only inserts at or above the high watermark, so these
        // rows need no watermark lock.
        if cutoff > stage.log_start
            && let Err(err) = self.record_delete_below(c, topition, cutoff).await
        {
            warn!(?err, cluster = ?self.cluster, ?topition, cutoff);
        }

        Ok((ErrorCode::None, cutoff))
    }

    async fn record_delete_below(
        &self,
        c: &mut Object,
        topition: &Topition,
        cutoff: i64,
    ) -> Result<u64> {
        let tx = c.transaction().await?;

        let deleted = self
            .tx_prepare_execute(
                &tx,
                "record_delete_by_offset.sql",
                &[
                    &self.cluster,
                    &topition.topic(),
                    &topition.partition(),
                    &cutoff,
                ],
            )
            .await?;

        tx.commit().await?;

        Ok(deleted)
    }

    #[instrument(skip(self, c, params))]
    async fn prepare_execute(
        &self,
        c: &Object,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = c
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();
        c.execute(&prepared, params)
            .await
            .inspect(|_| metrics::completed(sql, &self.cluster, execute_start))
            .inspect_err(|err| metrics::failed(sql, &self.cluster, err))
            .map_err(Into::into)
    }

    /// Returns the configs among `keys` that the topic `name` sets, or `None` when the topic does
    /// not exist.
    async fn topic_configs_by_key(
        &self,
        c: &Object,
        name: &str,
        keys: &[String],
    ) -> Result<Option<Vec<DescribeConfigsResourceResult>>> {
        // This function runs one query instead of the two that `all_topic_configs` runs, because
        // the broker reads `message.timestamp.type` through here for each topic of each produce
        // request.
        let rows = self
            .prepare_query(
                c,
                topic_configuration_select_keys::QUERY,
                &[&self.cluster.as_str(), &name, &keys],
            )
            .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let mut configs = vec![];

        for row in rows {
            // The query returns a row with a null name for a topic that sets none of `keys`, and
            // that row is not a config.
            let Some(name) = row
                .try_get::<_, Option<String>>(topic_configuration_select_keys::NAME)
                .inspect_err(|err| error!(?err))?
            else {
                continue;
            };
            let value = row
                .try_get::<_, Option<String>>(topic_configuration_select_keys::VALUE)
                .inspect_err(|err| error!(?err))?;

            configs.push(dynamic_topic_config(name, value));
        }

        Ok(Some(configs))
    }

    /// Returns every config that the topic `name` sets, or `None` when the topic does not exist.
    async fn all_topic_configs(
        &self,
        c: &Object,
        name: &str,
    ) -> Result<Option<Vec<DescribeConfigsResourceResult>>> {
        if self
            .prepare_query(c, topic_select::QUERY, &[&self.cluster.as_str(), &name])
            .await?
            .is_empty()
        {
            return Ok(None);
        }

        let rows = self
            .prepare_query(
                c,
                topic_configuration_select::QUERY,
                &[&self.cluster.as_str(), &name],
            )
            .await?;

        let mut configs = vec![];

        for row in rows {
            let name = row
                .try_get::<_, String>(topic_configuration_select::NAME)
                .inspect_err(|err| error!(?err))?;
            let value = row
                .try_get::<_, Option<String>>(topic_configuration_select::VALUE)
                .inspect_err(|err| error!(?err))?;

            configs.push(dynamic_topic_config(name, value));
        }

        Ok(Some(configs))
    }

    #[instrument(skip(self, c, params))]
    async fn prepare_query(
        &self,
        c: &Object,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = c
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();

        c.query(&prepared, params)
            .await
            .inspect(|_| metrics::completed(sql, &self.cluster, execute_start))
            .inspect_err(|err| metrics::failed(sql, &self.cluster, err))
            .map_err(Into::into)
    }

    #[instrument(skip(self, c, params))]
    async fn prepare_query_one(
        &self,
        c: &Object,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = c
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();

        c.query_one(&prepared, params)
            .await
            .inspect(|_| metrics::completed(sql, &self.cluster, execute_start))
            .inspect_err(|err| {
                debug!(?err);
                metrics::failed(sql, &self.cluster, err);
            })
            .map_err(Into::into)
    }

    #[instrument(skip(self, c, params))]
    async fn prepare_query_opt(
        &self,
        c: &Object,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = c
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();

        c.query_opt(&prepared, params)
            .await
            .inspect(|_| metrics::completed(sql, &self.cluster, execute_start))
            .inspect_err(|err| metrics::failed(sql, &self.cluster, err))
            .map_err(Into::into)
    }

    #[instrument(skip(self, tx, params))]
    async fn tx_prepare_execute(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();

        tx.execute(&prepared, params)
            .await
            .inspect(|_| metrics::completed(sql, &self.cluster, execute_start))
            .inspect_err(|err| metrics::failed(sql, &self.cluster, err))
            .map_err(Into::into)
    }

    #[instrument(skip(self, tx, params))]
    async fn tx_prepare_query(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();
        tx.query(&prepared, params)
            .await
            .inspect(|_| metrics::completed(sql, &self.cluster, execute_start))
            .inspect_err(|err| metrics::failed(sql, &self.cluster, err))
            .map_err(Into::into)
    }

    #[instrument(skip(self, tx, params))]
    async fn tx_prepare_query_one(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();
        tx.query_one(&prepared, params)
            .await
            .inspect(|_| metrics::completed(sql, &self.cluster, execute_start))
            .inspect_err(|err| metrics::failed(sql, &self.cluster, err))
            .map_err(Into::into)
    }

    #[instrument(skip(self, tx, params))]
    async fn tx_prepare_query_opt(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error> {
        let sql = self.sql_lookup(sql)?;

        let prepared = tx
            .prepare_cached(sql)
            .await
            .inspect_err(|err| error!(?err))?;

        let execute_start = SystemTime::now();
        tx.query_opt(&prepared, params)
            .await
            .inspect(|_| metrics::completed(sql, &self.cluster, execute_start))
            .inspect_err(|err| metrics::failed(sql, &self.cluster, err))
            .map_err(Into::into)
    }

    /// Refuses a client's transactional batch for a partition that is not in its producer's
    /// ongoing transaction (see [`txn_produce_partition_check`]). EndTxn deletes a transaction's
    /// `txn_topition` rows, so a row is present only while the transaction is ongoing.
    async fn txn_produce_partition_check(
        &self,
        transaction_id: &str,
        topition: &Topition,
        deflated: &deflated::Batch,
        tx: &Transaction<'_>,
    ) -> Result<()> {
        let row = self
            .tx_prepare_query_opt(
                tx,
                txn_produce_partition_select::QUERY,
                &[
                    &self.cluster,
                    &transaction_id,
                    &deflated.producer_id,
                    &deflated.producer_epoch,
                    &topition.topic(),
                    &topition.partition(),
                ],
            )
            .await
            .inspect_err(|err| error!(?err))?
            .map(|row| {
                Ok::<_, Error>((
                    row.try_get::<_, Option<i64>>(txn_produce_partition_select::PRODUCER)?,
                    row.try_get::<_, i32>(txn_produce_partition_select::ADDED)?,
                ))
            })
            .transpose()
            .inspect_err(|err| error!(?err))?;

        txn_produce_partition_check(row, deflated.producer_id)
            .inspect_err(|err| debug!(transaction_id, ?topition, deflated.producer_id, ?err))
    }

    #[instrument(skip_all)]
    async fn produce_in_tx(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        inflated: Batch,
        tx: &Transaction<'_>,
    ) -> Result<i64> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?topition, ?deflated);

        let topic = topition.topic();
        let partition = topition.partition();

        let Some(row) = self
            .tx_prepare_query_opt(
                tx,
                "topition_select_id.sql",
                &[&self.cluster, &topic, &partition],
            )
            .await
            .inspect_err(|err| debug!(?err))?
        else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        let topition_id = row.try_get::<_, i32>(0).inspect_err(|err| error!(?err))?;
        debug!(topition_id);

        let watermarks = self.watermark_select_for_write(topition, tx).await?;

        // The batch's producer epoch and sequence are checked under the partition's watermark
        // lock, which every produce to the partition holds until it commits, as Kafka checks
        // them under the partition's log lock. The check then sees every batch stored before
        // this one. Checked before the lock, a retry of a batch still in flight passed and was
        // stored a second time, and so did a batch at an older epoch than one stored meanwhile.
        if deflated.is_idempotent() {
            self.idempotent_message_check(transaction_id, topition, &deflated, tx)
                .await
                .inspect_err(|err| error!(?err))?;
        }

        if let Some(transaction_id) = transaction_id
            && deflated.is_transactional()
            && !deflated.is_control()
        {
            self.txn_produce_partition_check(transaction_id, topition, &deflated, tx)
                .await?;
        }

        let limit =
            max_message_bytes::limit(topition.topic(), watermarks.max_message_bytes.as_deref());
        max_message_bytes::ensure_batch_fits(&deflated, limit)?;
        let watermark_select_for_write::Row { low, high, .. } = watermarks;

        debug!(?low, ?high);

        // The producer's batch attributes, validated and normalised. Inflating
        // clears the compression codec (the records are now plaintext), so the
        // codec is kept from here: fetch rebuilds the batch from its rows and
        // re-deflates with the codec the producer used, as Kafka does with
        // compression.type=producer.
        let produced_attributes = BatchAttribute::try_from(deflated.attributes).map(i16::from)?;

        let attributes = BatchAttribute::try_from(inflated.attributes)?;

        if !attributes.control
            && let Some(ref schemas) = self.schemas
            && self
                .describe_config(topic, ConfigResource::Topic, None)
                .await
                .map(|resources| {
                    resources
                        .configs
                        .as_ref()
                        .and_then(|configs| {
                            topic_config::lookup(
                                configs.iter().inspect(|config| debug!(?config)).filter_map(
                                    |config| Some((config.name.as_str(), config.value.as_deref()?)),
                                ),
                                topic_config::SCHEMA_VALIDATION,
                            )
                            .and_then(|value| bool::from_str(value).ok())
                        })
                        .unwrap_or(true)
                })
                .inspect(|schema_validation| debug!(schema_validation))?
        {
            schemas.validate(topition.topic(), &inflated).await?;
        }

        let last_offset_delta = i64::from(inflated.last_offset_delta);

        if self.schemas.is_none()
            || self.lake.is_none()
            || (self.lake.is_some()
                && !self
                    .describe_config(topic, ConfigResource::Topic, None)
                    .await
                    .inspect(|resources| debug!(?resources))
                    .map(|resources| {
                        resources
                            .configs
                            .as_ref()
                            .and_then(|configs| {
                                topic_config::lookup(
                                    configs.iter().inspect(|config| debug!(?config)).filter_map(
                                        |config| {
                                            Some((config.name.as_str(), config.value.as_deref()?))
                                        },
                                    ),
                                    topic_config::LAKE_SINK,
                                )
                                .and_then(|value| bool::from_str(value).ok())
                            })
                            .unwrap_or_default()
                    })
                    .inspect(|nisshi_lake_sink| debug!(nisshi_lake_sink))?)
        {
            {
                let record_sink = tx.copy_in(self.sql_lookup("record_copy.sql")?).await?;

                let record_column_types = [
                    Type::INT4,
                    Type::INT8,
                    Type::INT2,
                    Type::INT8,
                    Type::INT2,
                    Type::TIMESTAMPTZ,
                    Type::BYTEA,
                    Type::BYTEA,
                ];

                let record_writer = BinaryCopyInWriter::new(record_sink, &record_column_types);
                pin_mut!(record_writer);

                for (delta, record) in inflated.records.iter().enumerate() {
                    let delta = i64::try_from(delta)?;
                    let offset = high.unwrap_or_default() + delta;
                    let attributes = produced_attributes;
                    let key = record.key.as_deref();
                    let value = record.value.as_deref();

                    let producer_id = transaction_id.and(Some(inflated.producer_id));
                    let producer_epoch = transaction_id.and(Some(inflated.producer_epoch));
                    let ts = to_sql_timestamp(inflated.record_timestamp(record))?;

                    let mut row: Vec<&(dyn ToSql + Sync)> =
                        Vec::with_capacity(record_column_types.len());

                    row.push(&topition_id);
                    row.push(&offset);
                    row.push(&attributes);
                    row.push(&producer_id);
                    row.push(&producer_epoch);
                    row.push(&ts);
                    row.push(&key);
                    row.push(&value);

                    record_writer
                        .as_mut()
                        .write(&row)
                        .await
                        .inspect_err(|err| {
                            error!(?err, ?topic, ?partition, ?offset, key_len = ?key.map(<[u8]>::len), value_len = ?value.map(<[u8]>::len))
                        })?;
                }

                _ = record_writer
                    .finish()
                    .await
                    .inspect(|record_row_count| debug!(?record_row_count))
                    .inspect_err(|err| error!(?err))?;
            }

            {
                let header_sink = tx.copy_in(self.sql_lookup("header_copy.sql")?).await?;
                let header_column_types =
                    [Type::INT4, Type::INT8, Type::INT4, Type::BYTEA, Type::BYTEA];
                let header_writer = BinaryCopyInWriter::new(header_sink, &header_column_types);
                pin_mut!(header_writer);

                for (delta, record) in inflated.records.iter().enumerate() {
                    let delta = i64::try_from(delta)?;
                    let offset = high.unwrap_or_default() + delta;

                    for (ordinal, header) in record.headers.iter().enumerate() {
                        let ordinal = i32::try_from(ordinal)?;
                        let key = header.key.as_deref();
                        let value = header.value.as_deref();

                        let mut row: Vec<&(dyn ToSql + Sync)> =
                            Vec::with_capacity(header_column_types.len());

                        row.push(&topition_id);
                        row.push(&offset);
                        row.push(&ordinal);
                        row.push(&key);
                        row.push(&value);

                        header_writer
                            .as_mut()
                            .write(&row)
                            .await
                            .inspect_err(|err| {
                                error!(?err, ?topic, ?partition, ?offset, key_len = ?key.map(<[u8]>::len), value_len = ?value.map(<[u8]>::len))
                            })?;
                    }
                }

                _ = header_writer
                    .finish()
                    .await
                    .inspect(|header_row_count| debug!(?header_row_count))
                    .inspect_err(|err| error!(?err))?;
            }

            if let Some(transaction_id) = transaction_id
                && attributes.transaction
            {
                let offset_start = high.unwrap_or_default();
                let offset_end = high.map_or(last_offset_delta, |high| high + last_offset_delta);

                _ = self
                .tx_prepare_execute(tx,
                    "txn_produce_offset_insert.sql",
                    &[
                        &self.cluster,
                        &transaction_id,
                        &inflated.producer_id,
                        &inflated.producer_epoch,
                        &topic,
                        &partition,
                        &offset_start,
                        &offset_end,
                    ],
                )
                .await
                .inspect(|n| debug!(cluster = ?self.cluster, ?transaction_id, ?inflated.producer_id, ?inflated.producer_epoch, ?topic, ?partition, ?offset_start, ?offset_end, ?n))
                .inspect_err(|err| error!(?err))?;
            }
        }

        _ = self
            .tx_prepare_execute(
                tx,
                "watermark_update.sql",
                &[
                    &self.cluster,
                    &topic,
                    &partition,
                    &low.unwrap_or_default(),
                    &high.map_or(last_offset_delta + 1, |high| high + last_offset_delta + 1),
                ],
            )
            .await
            .inspect(|n| debug!(?n))
            .inspect_err(|err| error!(?err))?;

        self.lake_store(&attributes, topition, high, &inflated)
            .await?;

        Ok(high.unwrap_or_default())
    }

    #[instrument(skip_all)]
    async fn end_in_tx(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        fence: bool,
        tx: &Transaction<'_>,
    ) -> Result<ErrorCode> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?producer_id, ?producer_epoch, ?committed, fence);

        // Check the producer's identity before touching this specific transaction's state
        // at all: a request carrying a stale epoch is not this transaction's problem, it's
        // an identity problem, and must be reported as such (ProducerFenced) regardless of
        // what the stale epoch's own txn_detail row says.
        let Some(row) = self
            .tx_prepare_query_opt(
                tx,
                "producer_epoch_current_for_producer.sql",
                &[&self.cluster, &producer_id],
            )
            .await?
        else {
            return Ok(ErrorCode::UnknownProducerId);
        };
        let current_epoch = row.try_get::<_, i16>(0)?;

        match producer_epoch.cmp(&current_epoch) {
            Ordering::Less => return Ok(ErrorCode::ProducerFenced),
            Ordering::Greater => return Ok(ErrorCode::InvalidProducerEpoch),
            Ordering::Equal => {}
        }

        // Lock the row before touching anything else: if a concurrent caller (a real
        // EndTxn racing the maintain_transactions sweep, or a retried EndTxn) already
        // finalized this transaction, this blocks until it commits, then sees the
        // terminal status below and no-ops instead of writing a second marker.
        let status = match self
            .tx_prepare_query_opt(
                tx,
                "txn_detail_select_status_for_update.sql",
                &[
                    &self.cluster,
                    &transaction_id,
                    &producer_id,
                    &producer_epoch,
                ],
            )
            .await?
        {
            Some(row) => row
                .try_get::<_, Option<String>>(0)? // nullable transaction status column -> Option<String>
                .map(TxnState::try_from) // parse if present -> Option<Result<TxnState, _>>
                .transpose()?, // Option<Result<_>> -> Result<Option<_>>, then `?` -> Option<TxnState>
            None => None, // no txn_detail row found
        };

        // Outcome-aware idempotency: a retry (or a race between a real EndTxn and the sweep)
        // must only no-op when it agrees with what's already staged or finalized for this
        // transaction. A conflicting request -- e.g. a real commit arriving after the sweep
        // already staged/finalized an abort -- is a genuine protocol error (InvalidTxnState is
        // exactly Kafka's error code for "transactional operation attempted in an invalid
        // state"), not something to silently paper over by claiming success either way.
        //
        // PREPARE_COMMIT/PREPARE_ABORT means an earlier call already wrote this transaction's
        // control marker and is only waiting on an older, still-open transaction on the same
        // partition(s) to resolve first -- so a matching retry must NOT write a second marker,
        // it should just re-check whether those older transactions have since resolved.
        let write_marker = match status {
            Some(TxnState::Committed) => {
                debug!(transaction_id, producer_id, producer_epoch, ?status);
                return Ok(if committed {
                    ErrorCode::None
                } else {
                    ErrorCode::InvalidTxnState
                });
            }
            Some(TxnState::Aborted) => {
                debug!(transaction_id, producer_id, producer_epoch, ?status);
                return Ok(if committed {
                    ErrorCode::InvalidTxnState
                } else {
                    ErrorCode::None
                });
            }
            Some(TxnState::PrepareCommit) => {
                if !committed {
                    debug!(transaction_id, producer_id, producer_epoch, ?status);
                    return Ok(ErrorCode::InvalidTxnState);
                }
                false
            }
            Some(TxnState::PrepareAbort) => {
                if committed {
                    debug!(transaction_id, producer_id, producer_epoch, ?status);
                    return Ok(ErrorCode::InvalidTxnState);
                }
                false
            }
            None | Some(TxnState::Begin) => true,
        };

        // A broker-initiated timeout abort (the sweep) must fence the producer, not just
        // clean up this transaction's own bookkeeping: the producer might still be alive
        // and about to send more data under this same epoch. Bump it now, before doing any
        // of the actual abort work below (which can be deferred behind an older still-open
        // transaction) -- check_produce_epoch already rejects a stale epoch with
        // ProducerFenced, this just needs to make the current one stale. Only on the first
        // call that actually touches this transaction (write_marker), not a matching retry.
        //
        // A producer at epoch i16::MAX gets no new epoch, and is fenced already.
        if fence && write_marker {
            _ = self
                .tx_prepare_query_opt(
                    tx,
                    "producer_epoch_insert.sql",
                    &[&self.cluster, &producer_id],
                )
                .await?;
        }

        let mut overlaps = vec![];

        let rows = self
            .tx_prepare_query(
                tx,
                "txn_select_produced_topitions.sql",
                &[
                    &self.cluster,
                    &transaction_id,
                    &producer_id,
                    &producer_epoch,
                ],
            )
            .await?;

        for row in rows {
            let topic = row.try_get::<_, String>(0)?;
            let partition = row.try_get::<_, i32>(1)?;

            let topition = Topition::new(topic.clone(), partition);

            debug!(?topition);

            // Only write the control marker the first time this transaction is finalized or
            // deferred -- a matching retry while already staged in PREPARE_COMMIT/
            // PREPARE_ABORT must not write a second one (see the status match above).
            if write_marker {
                let control_batch: Bytes = if committed {
                    ControlBatch::default().commit().try_into()?
                } else {
                    ControlBatch::default().abort().try_into()?
                };
                let end_transaction_marker: Bytes = EndTransactionMarker::default().try_into()?;

                let batch = Batch::builder()
                    .record(
                        Record::builder()
                            .key(control_batch.into())
                            .value(end_transaction_marker.into()),
                    )
                    .attributes(
                        BatchAttribute::default()
                            .control(true)
                            .transaction(true)
                            .into(),
                    )
                    .producer_id(producer_id)
                    .producer_epoch(producer_epoch)
                    .base_sequence(-1)
                    .build()
                    .and_then(TryInto::try_into)
                    .inspect(|deflated| debug!(?deflated))?;

                // A control batch holds one uncompressed record, so it is
                // inflated inline. `inflate` would wait for a decode permit
                // while this transaction holds its watermark row locks.
                let inflated = Batch::try_from(&batch)?;

                let offset = self
                    .produce_in_tx(Some(transaction_id), &topition, batch, inflated, tx)
                    .await?;

                debug!(offset, ?topition);
            }

            let row = self
                .tx_prepare_query_one(
                    tx,
                    "txn_produce_offset_select_offset_range.sql",
                    &[
                        &self.cluster,
                        &transaction_id,
                        &producer_id,
                        &producer_epoch,
                        &topic,
                        &partition,
                    ],
                )
                .await?;

            let offset_start = row.try_get::<_, i64>(0)?;
            let offset_end = row.try_get::<_, i64>(1)?;
            debug!(offset_start, offset_end);

            let rows = self
                .tx_prepare_query(
                    tx,
                    "txn_produce_offset_select_overlapping_txn.sql",
                    &[
                        &self.cluster,
                        &transaction_id,
                        &producer_id,
                        &producer_epoch,
                        &topic,
                        &partition,
                        &offset_end,
                    ],
                )
                .await?;

            for row in rows {
                overlaps.push(Txn::try_from(row).inspect(|txn| debug!(?txn))?);
            }
        }

        if overlaps.iter().all(|txn| txn.status.is_prepared()) {
            let txns = {
                let mut txns = Vec::with_capacity(overlaps.len() + 1);

                txns.append(&mut overlaps);

                txns.push(Txn {
                    name: transaction_id.into(),
                    producer_id,
                    producer_epoch,
                    status: if committed {
                        TxnState::PrepareCommit
                    } else {
                        TxnState::PrepareAbort
                    },
                });

                txns
            };

            debug!(?txns);

            for txn in txns {
                debug!(?txn);

                _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_produce_offset_delete_by_txn.sql",
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                        ],
                    )
                    .await?;

                _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_topition_delete_by_txn.sql",
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                        ],
                    )
                    .await?;

                if txn.status == TxnState::PrepareCommit {
                    _ = self
                        .tx_prepare_execute(
                            tx,
                            "consumer_offset_insert_from_txn.sql",
                            &[
                                &self.cluster,
                                &txn.name,
                                &txn.producer_id,
                                &txn.producer_epoch,
                            ],
                        )
                        .await?;
                }

                _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_offset_commit_tp_delete_by_txn.sql",
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                        ],
                    )
                    .await?;

                _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_offset_commit_delete_by_txn.sql",
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                        ],
                    )
                    .await?;

                let outcome = if txn.status == TxnState::PrepareCommit {
                    String::from(TxnState::Committed)
                } else if txn.status == TxnState::PrepareAbort {
                    String::from(TxnState::Aborted)
                } else {
                    String::from(txn.status)
                };

                _ = self
                    .tx_prepare_execute(
                        tx,
                        "txn_status_update.sql",
                        &[
                            &self.cluster,
                            &txn.name,
                            &txn.producer_id,
                            &txn.producer_epoch,
                            &outcome,
                        ],
                    )
                    .await?;
            }
        } else {
            debug!(?overlaps);

            let outcome = if committed {
                String::from(TxnState::PrepareCommit)
            } else {
                String::from(TxnState::PrepareAbort)
            };

            _ = self
                .tx_prepare_execute(
                    tx,
                    "txn_status_update.sql",
                    &[
                        &self.cluster,
                        &transaction_id,
                        &producer_id,
                        &producer_epoch,
                        &outcome,
                    ],
                )
                .await
                .inspect(|n| {
                    debug!(
                        cluster = self.cluster,
                        transaction_id, producer_id, producer_epoch, outcome, n
                    )
                })?;
        }

        Ok(ErrorCode::None)
    }

    /// Used only by maintain_transactions: aborts a transaction the sweep has decided is
    /// timed out, AND fences the producer's epoch -- unlike a real client's own EndTxn, the
    /// producer here might still be alive and about to send more data, so the broker must
    /// unilaterally invalidate its current epoch rather than just cleaning up bookkeeping.
    #[instrument(skip(self))]
    async fn abort_timed_out(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<ErrorCode> {
        let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
        let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

        let error_code = self
            .end_in_tx(
                transaction_id,
                producer_id,
                producer_epoch,
                false,
                true,
                &tx,
            )
            .await?;

        tx.commit().await?;

        Ok(error_code)
    }

    /// Moves `transaction_id` from the `previous` producer ID, whose epoch is exhausted, to a
    /// new producer ID at epoch 0, and fences `previous`. Returns the new producer ID and
    /// epoch.
    async fn move_to_new_producer(
        &self,
        tx: &Transaction<'_>,
        transaction_id: &str,
        previous: i64,
    ) -> Result<(i64, i16)> {
        let producer: i64 = self
            .tx_prepare_query_one(tx, "producer_insert.sql", &[&self.cluster])
            .await?
            .try_get(0)?;

        let epoch: i16 = self
            .tx_prepare_query_one(tx, "producer_epoch_insert.sql", &[&self.cluster, &producer])
            .await?
            .try_get(0)?;

        // A produce or an EndTxn with an epoch below the producer's newest is fenced, so
        // epoch i16::MAX fences the previous producer ID. A producer at epoch i16::MAX
        // gets no new epoch, and is fenced already.
        _ = self
            .tx_prepare_query_opt(tx, "producer_epoch_insert.sql", &[&self.cluster, &previous])
            .await?;

        let updated = self
            .tx_prepare_execute(
                tx,
                "txn_update_producer.sql",
                &[&self.cluster, &transaction_id, &producer],
            )
            .await?;

        if updated == 1 {
            info!(
                cluster = self.cluster,
                transaction_id,
                previous,
                producer,
                "producer epoch exhausted, moved to a new producer ID"
            );

            Ok((producer, epoch))
        } else {
            Err(Error::Message(format!(
                "{updated} transactions named {transaction_id}"
            )))
        }
    }

    /// Mints a fresh producer, or bumps an existing transactional.id's producer to its next
    /// epoch.
    ///
    /// When the producer's epoch is exhausted, the transactional ID moves to a new producer
    /// ID at epoch 0 instead, as in Kafka, or the answer is
    /// [`ErrorCode::ConcurrentTransactions`] while the transaction of its current epoch has
    /// not finished.
    ///
    /// A `claim` must match the transactional ID's current producer and epoch, or the
    /// answer is [`ErrorCode::ProducerFenced`]. Without a transactional ID, or for one with
    /// no producer yet, the claim is ignored, as in Kafka.
    async fn bump_or_create_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        claim: Option<(i64, i16)>,
    ) -> Result<ProducerIdResponse> {
        if let Some(transaction_id) = transaction_id {
            let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
            let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

            // At read committed, two requests for this transactional ID can both read the
            // same epoch before either bumps it, and both pass the claim check. The lock
            // makes the second request wait, and its next statement then reads the
            // committed bump. The unique transactional ID name covers a txn row that does
            // not exist yet.
            _ = self
                .tx_prepare_query_opt(
                    &tx,
                    "txn_select_name_for_update.sql",
                    &[&self.cluster, &transaction_id],
                )
                .await
                .inspect_err(|err| error!(?err))?;

            let mut rolled_back = false;

            if let Some(row) = self
                .tx_prepare_query_opt(
                    &tx,
                    "producer_epoch_for_current_txn.sql",
                    &[&self.cluster, &transaction_id],
                )
                .await
                .inspect_err(|err| error!(?err))?
            {
                let id: i64 = row.try_get(0).inspect_err(|err| error!(?err))?;
                let epoch: i16 = row.try_get(1).inspect_err(|err| error!(?err))?;
                let status = row
                    .try_get::<_, Option<String>>(2)
                    .inspect_err(|err| error!(?err))?
                    .map_or(Ok(None), |status| {
                        TxnState::from_str(status.as_str()).map(Some)
                    })?;

                debug!(transaction_id, id, epoch, ?status, ?claim);

                if let Some(claim) = claim {
                    // The claim must match the producer's newest epoch, not the newest
                    // epoch with a transaction. The timeout sweep fences a producer by
                    // adding an epoch without a transaction, and the fenced producer must
                    // not get the transactional ID back with its old epoch.
                    let current = self
                        .tx_prepare_query_opt(
                            &tx,
                            "producer_epoch_current_for_producer.sql",
                            &[&self.cluster, &id],
                        )
                        .await
                        .inspect_err(|err| error!(?err))?
                        .map(|row| row.try_get::<_, i16>(0))
                        .transpose()
                        .inspect_err(|err| error!(?err))?
                        .unwrap_or(epoch);

                    let error = check_claim((id, current), claim);

                    if error != ErrorCode::None {
                        _ = tx
                            .rollback()
                            .await
                            .inspect_err(|err| error!(?err, ?transaction_id, id, epoch));

                        return Ok(ProducerIdResponse::failed(error));
                    }
                }

                if let Some(TxnState::Begin) = status {
                    let error = self
                        .end_in_tx(transaction_id, id, epoch, false, false, &tx)
                        .await?;

                    if error != ErrorCode::None {
                        _ = tx
                            .rollback()
                            .await
                            .inspect_err(|err| error!(?err, ?transaction_id, id, epoch));

                        return Ok(ProducerIdResponse { error, id, epoch });
                    }

                    rolled_back = true;
                }
            }

            let (producer, epoch) = if let Some(row) = self
                .tx_prepare_query_opt(
                    &tx,
                    "txn_select_name.sql",
                    &[&self.cluster, &transaction_id],
                )
                .await
                .inspect_err(|err| error!(?err))?
            {
                let producer: i64 = row.try_get(0).inspect_err(|err| error!(?err))?;

                // The producer's newest epoch, not the newest epoch with a transaction,
                // because the timeout sweep fences a producer with an epoch that has no
                // transaction.
                let newest = self
                    .tx_prepare_query_opt(
                        &tx,
                        "producer_epoch_current_for_producer.sql",
                        &[&self.cluster, &producer],
                    )
                    .await
                    .inspect_err(|err| error!(?err))?
                    .map(|row| row.try_get::<_, i16>(0))
                    .transpose()
                    .inspect_err(|err| error!(?err))?;

                if newest.is_some_and(epoch_is_exhausted) {
                    // The status after the rollback above, if it ran.
                    let status = self
                        .tx_prepare_query_opt(
                            &tx,
                            "producer_epoch_for_current_txn.sql",
                            &[&self.cluster, &transaction_id],
                        )
                        .await
                        .inspect_err(|err| error!(?err))?
                        .map(|row| row.try_get::<_, Option<String>>(2))
                        .transpose()
                        .inspect_err(|err| error!(?err))?
                        .flatten()
                        .map(|status| TxnState::from_str(status.as_str()))
                        .transpose()?;

                    // As in Kafka, only the transaction of the current epoch holds back the
                    // move: the client retries CONCURRENT_TRANSACTIONS until it finishes.
                    // This commits the rollback above, if it ran.
                    if status.is_some_and(|status| !status.is_finished()) {
                        // The rollback leaves its abort prepared behind an older open
                        // transaction. Kafka fences the producer at that point, so that it
                        // cannot add to the aborted transaction, and so does this. A producer
                        // at epoch i16::MAX gets no new epoch, and is fenced already.
                        if rolled_back {
                            _ = self
                                .tx_prepare_query_opt(
                                    &tx,
                                    "producer_epoch_insert.sql",
                                    &[&self.cluster, &producer],
                                )
                                .await
                                .inspect_err(|err| error!(?err))?;
                        }

                        let error = match tx.commit().await.inspect_err(|err| {
                            error!(?err, cluster = self.cluster, transaction_id, producer)
                        }) {
                            Ok(()) => ErrorCode::ConcurrentTransactions,
                            Err(err) => return Err(Error::from(err)),
                        };

                        return Ok(ProducerIdResponse::failed(error));
                    }

                    self.move_to_new_producer(&tx, transaction_id, producer)
                        .await
                        .inspect_err(|err| error!(self.cluster, transaction_id, producer, ?err))?
                } else {
                    let row = self
                        .tx_prepare_query_one(
                            &tx,
                            "producer_epoch_insert.sql",
                            &[&self.cluster, &producer],
                        )
                        .await
                        .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                    let epoch: i16 = row.try_get(0)?;

                    (producer, epoch)
                }
            } else {
                let row = self
                    .tx_prepare_query_one(&tx, "producer_insert.sql", &[&self.cluster])
                    .await
                    .inspect_err(|err| error!(?err))?;

                let producer: i64 = row.try_get(0).inspect_err(|err| error!(?err))?;

                let row = self
                    .tx_prepare_query_one(
                        &tx,
                        "producer_epoch_insert.sql",
                        &[&self.cluster, &producer],
                    )
                    .await
                    .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                let epoch: i16 = row.try_get(0)?;

                assert_eq!(
                    1,
                    self.tx_prepare_execute(
                        &tx,
                        "txn_insert.sql",
                        &[&self.cluster, &transaction_id, &producer],
                    )
                    .await
                    .inspect_err(|err| error!(
                        self.cluster,
                        transaction_id,
                        producer,
                        ?err
                    ))?
                );

                (producer, epoch)
            };

            debug!(transaction_id, producer, epoch);

            assert_eq!(
                1,
                self.tx_prepare_execute(
                    &tx,
                    "txn_detail_insert.sql",
                    &[
                        &self.cluster,
                        &transaction_id,
                        &producer,
                        &epoch,
                        &transaction_timeout_ms
                    ],
                )
                .await
                .inspect_err(|err| error!(
                    self.cluster,
                    transaction_id,
                    producer,
                    epoch,
                    transaction_timeout_ms,
                    ?err
                ))?
            );

            let error = match tx.commit().await.inspect_err(|err| {
                error!(
                    ?err,
                    cluster = self.cluster,
                    transaction_id,
                    producer,
                    epoch
                )
            }) {
                Ok(()) => ErrorCode::None,
                Err(err) => return Err(Error::from(err)),
            };

            Ok(ProducerIdResponse {
                error,
                id: producer,
                epoch,
            })
        } else {
            let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
            let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

            let row = self
                .tx_prepare_query_one(&tx, "producer_insert.sql", &[&self.cluster])
                .await
                .inspect_err(|err| error!(self.cluster, ?err))?;

            let producer: i64 = row.try_get(0)?;

            let row = self
                .tx_prepare_query_one(
                    &tx,
                    "producer_epoch_insert.sql",
                    &[&self.cluster, &producer],
                )
                .await
                .inspect_err(|err| error!(self.cluster, producer, ?err))?;

            let epoch: i16 = row.try_get(0)?;

            let error = match tx
                .commit()
                .await
                .inspect_err(|err| error!(?err, ?transaction_id, producer, epoch))
            {
                Ok(()) => ErrorCode::None,
                Err(err) => return Err(Error::from(err)),
            };

            Ok(ProducerIdResponse {
                error,
                id: producer,
                epoch,
            })
        }
    }

    #[instrument(skip_all)]
    async fn lake_store(
        &self,
        attributes: &BatchAttribute,
        topition: &Topition,
        high: Option<i64>,
        inflated: &Batch,
    ) -> Result<()> {
        if !attributes.control
            && let Some(ref lake) = self.lake
        {
            let config = self
                .describe_config(topition.topic(), ConfigResource::Topic, None)
                .await?;

            lake.store(
                topition.topic(),
                topition.partition(),
                high.unwrap_or_default(),
                inflated,
                config,
            )
            .await?;
        }

        Ok(())
    }

    #[instrument(skip(self), ret)]
    async fn policy_compact(&self) -> Result<u64> {
        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        // exempt this sweep from DEFAULT_STATEMENT_TIMEOUT_MS: its full-table scan can
        // legitimately outrun a request-path query's timeout, and re-aborting it on every
        // maintenance tick would only let the backlog it exists to clear keep growing.
        tx.batch_execute("SET LOCAL statement_timeout = 0")
            .await
            .inspect_err(|err| error!(?err))?;

        let compacted = self
            .tx_prepare_execute(&tx, "policy_compact.sql", &[&self.cluster])
            .await?;

        tx.commit().await.map_err(Into::into).and(Ok(compacted))
    }

    #[instrument(skip(self), ret)]
    async fn policy_delete(&self, now: SystemTime) -> Result<u64> {
        let retention_secs = i64::try_from(config::DEFAULT_RETENTION.as_secs())?;

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        // see policy_compact: exempt this sweep from the default statement_timeout.
        tx.batch_execute("SET LOCAL statement_timeout = 0")
            .await
            .inspect_err(|err| error!(?err))?;

        let deleted = self
            .tx_prepare_execute(
                &tx,
                "policy_delete.sql",
                &[&self.cluster, &now, &retention_secs],
            )
            .await?;

        let deleted_by_bytes = self
            .tx_prepare_execute(&tx, "policy_delete_bytes.sql", &[&self.cluster])
            .await?;

        tx.commit()
            .await
            .map_err(Into::into)
            .and(Ok(deleted + deleted_by_bytes))
    }

    /// Removes the records that DeleteRecords left below each partition's log
    /// start, when its own removal failed or timed out.
    #[instrument(skip(self), ret)]
    async fn record_delete_below_log_start(&self) -> Result<u64> {
        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        // see policy_compact: exempt this sweep from the default statement_timeout.
        tx.batch_execute("SET LOCAL statement_timeout = 0")
            .await
            .inspect_err(|err| error!(?err))?;

        let deleted = self
            .tx_prepare_execute(&tx, "record_delete_below_log_start.sql", &[&self.cluster])
            .await?;

        tx.commit().await.map_err(Into::into).and(Ok(deleted))
    }

    async fn topic_with_key<'a>(&self, topic: &'a str) -> Result<(&'a str, Option<&'a str>)> {
        if let Some((base, key)) = topic.split_once('/')
            && self
                .describe_config(base, ConfigResource::Topic, None)
                .await
                .map(|configs| {
                    topic_config::lookup(
                        configs
                            .configs
                            .as_deref()
                            .unwrap_or_default()
                            .iter()
                            .filter_map(|config| {
                                Some((config.name.as_str(), config.value.as_deref()?))
                            }),
                        topic_config::VIRTUAL,
                    )
                    .and_then(|value| bool::from_str(value).ok())
                    .unwrap_or_default()
                })?
        {
            Ok((base, Some(key)))
        } else {
            Ok((topic, None))
        }
    }

    #[instrument(skip(self), ret)]
    async fn base_topic<'a>(&self, topic: &'a str) -> Result<&'a str> {
        self.topic_with_key(topic).await.map(|(topic, _key)| topic)
    }

    #[instrument(skip(self), ret)]
    async fn virtual_topic_id(&self, topic: &str, key: &str) -> Result<Uuid> {
        let uuid = Uuid::new_v5(
            &Uuid::NAMESPACE_URL,
            format!("tag:nisshi.io,2026-04:virtual:{topic}:{key}",).as_bytes(),
        );

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let row = self
            .prepare_query_one(
                &c,
                "virtual_topic_upsert.sql",
                &[&self.cluster, &topic, &key.as_bytes(), &uuid],
            )
            .await?;

        row.try_get::<_, Uuid>(0)
            .inspect_err(|err| error!(?err))
            .map_err(Into::into)
            .inspect(|vt| debug!(%vt))
    }
}

/// Reads a `consumer_group_detail.sql` row as the stored group and the version that
/// `update_group` compares against.
fn group_detail_with_version(row: &Row) -> Result<(GroupDetail, Version)> {
    let version = row
        .try_get::<_, Uuid>(0)
        .inspect_err(|err| error!(?err))
        .map(|uuid| Version {
            e_tag: Some(uuid.to_string()),
            version: None,
        })
        .inspect(|version| debug!(?version))?;

    let value = row.try_get::<_, Value>(1)?;
    let current =
        serde_json::from_value::<GroupDetail>(value).inspect(|current| debug!(?current))?;

    Ok((current, version))
}

impl Postgres {
    /// Writes `deflated` to `topition` in one transaction, with `inflated` as its decoded records,
    /// or with the records that this decodes from `deflated` when the caller passed none. Both
    /// produce methods call this, so that each produce decodes and writes in one place.
    #[instrument(name = "produce", skip_all)]
    async fn produce_batch(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        inflated: Option<Batch>,
    ) -> Result<i64> {
        // The batch is inflated before the transaction starts, so the wait
        // for a decode permit and the decode do not hold a pooled connection
        // or the partition's watermark row lock.
        let inflated = match inflated {
            Some(inflated) => inflated,
            None => inflate(deflated.clone())
                .await
                .inspect_err(|err| debug!(?err))?,
        };

        debug!(cluster = self.cluster, transaction_id, ?topition, ?deflated);

        let mut c = self.connection().await?;

        // This transaction is READ COMMITTED, regardless of the server's default isolation level.
        // `produce_in_tx` checks an idempotent batch after it waits for the partition's watermark
        // lock, and the check must see the batch that the lock's previous holder committed during
        // the wait. Each statement of a READ COMMITTED transaction sees every commit made before
        // the statement started. A REPEATABLE READ transaction sees only the commits made before
        // its first statement, which ran before the wait.
        let tx = c
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
            .start()
            .await?;

        let high = self
            .produce_in_tx(transaction_id, topition, deflated, inflated, &tx)
            .await?;

        tx.commit().await?;

        Ok(high)
    }
}

#[async_trait]
impl Storage for Postgres {
    #[instrument(skip_all)]
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        debug!(cluster = self.cluster, ?broker_registration);

        let c = self.connection().await?;

        _ = self
            .prepare_execute(
                &c,
                "register_broker.sql",
                &[&broker_registration.cluster_id],
            )
            .await
            .inspect(|n| debug!(cluster = self.cluster, n))?;

        Ok(())
    }

    #[instrument(skip_all)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        debug!(cluster = self.cluster);

        let broker_id = self.node;
        let host = self
            .advertised_listener
            .host_str()
            .unwrap_or("0.0.0.0")
            .into();
        let port = self.advertised_listener.port().unwrap_or(9092).into();
        let rack = None;

        Ok(vec![
            DescribeClusterBroker::default()
                .broker_id(broker_id)
                .host(host)
                .port(port)
                .rack(rack),
        ])
    }

    #[instrument(skip_all)]
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        debug!(cluster = self.cluster, ?topic, validate_only);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let uuid = Uuid::new_v4();

        let topic_uuid = self
            .tx_prepare_query_one(
                &tx,
                "topic_insert.sql",
                &[
                    &self.cluster,
                    &topic.name,
                    &uuid,
                    &topic.num_partitions,
                    &(topic.replication_factor as i32),
                ],
            )
            .await
            .inspect_err(|err| debug!(?err, ?topic, ?validate_only))
            .map(|row| row.get(0))
            .map_err(|error| {
                if let Error::TokioPostgres(ref error) = error
                    && error
                        .code()
                        .is_some_and(|code| *code == SqlState::UNIQUE_VIOLATION)
                {
                    Error::Api(ErrorCode::TopicAlreadyExists)
                } else {
                    error
                }
            })?;

        debug!(?topic_uuid, cluster = self.cluster, ?topic);

        // The watermark insert selects the topition rows that the topition insert creates, so
        // every topition insert must finish before the first watermark insert is sent. Each
        // statement is awaited before the next one is sent on this transaction's connection,
        // which keeps that order. A watermark insert that finds no topition row matches no
        // rows without an error, leaving a topic that rejects every produce.
        for sql in ["topition_insert.sql", "watermark_insert.sql"] {
            for partition in 0..topic.num_partitions {
                let inserted = self
                    .tx_prepare_execute(&tx, sql, &[&self.cluster, &topic.name, &partition])
                    .await?;

                if inserted != 1 {
                    return Err(Error::Message(format!(
                        "{sql} did not insert one row per partition of {} in {}",
                        topic.name, self.cluster
                    )));
                }
            }
        }

        if let Some(configs) = topic.configs {
            for config in configs {
                debug!(?config);

                _ = self
                    .tx_prepare_execute(
                        &tx,
                        "topic_configuration_upsert.sql",
                        &[
                            &self.cluster,
                            &topic.name,
                            &config.name,
                            &config.value.as_deref(),
                        ],
                    )
                    .await
                    .inspect_err(|err| error!(?err, ?config));
            }
        }

        tx.commit().await.inspect_err(|err| error!(?err))?;

        Ok(topic_uuid)
    }

    fn delete_records_concurrency(&self) -> DeleteRecordsConcurrency {
        DeleteRecordsConcurrency::shared(
            self.delete_records_per_request,
            Arc::clone(&self.delete_records_writes),
        )
    }

    #[instrument(skip_all)]
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        debug!(cluster = self.cluster, ?topics);

        let mut c = self.connection().await?;
        let mut responses = Vec::with_capacity(topics.len());

        // Each partition commits on its own, so a call holds at most one
        // watermark row lock at a time. It cannot then deadlock with EndTxn
        // or with the other calls of the same request, which the service runs
        // concurrently on their own connections, and a produce waits only
        // while its own partition advances.
        for topic in topics {
            let mut partitions = Vec::with_capacity(topic.partitions.as_ref().map_or(0, Vec::len));

            for partition in topic.partitions.iter().flatten() {
                let topition = Topition::new(topic.name.as_str(), partition.partition_index);

                let (error_code, low_watermark) = self
                    .delete_records_partition(&mut c, &topition, partition.offset)
                    .await
                    .unwrap_or_else(|err| match err {
                        Error::Api(error_code) => {
                            (error_code, DELETE_RECORDS_INVALID_LOW_WATERMARK)
                        }

                        // Retriable, as Kafka answers a storage failure. A retry
                        // is safe: at or below the log start it changes nothing.
                        err => {
                            error!(?err, cluster = ?self.cluster, ?topition);
                            (
                                ErrorCode::KafkaStorageError,
                                DELETE_RECORDS_INVALID_LOW_WATERMARK,
                            )
                        }
                    });

                partitions.push(
                    DeleteRecordsPartitionResult::default()
                        .partition_index(partition.partition_index)
                        .low_watermark(low_watermark)
                        .error_code(error_code.into()),
                );
            }

            responses.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partitions)),
            );
        }

        Ok(responses)
    }

    #[instrument(skip_all)]
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        debug!(cluster = self.cluster, ?topic);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let row = match topic {
            TopicId::Id(id) => {
                self.tx_prepare_query_opt(&tx, "topic_select_uuid.sql", &[&self.cluster, &id])
                    .await?
            }

            TopicId::Name(name) => {
                self.tx_prepare_query_opt(&tx, "topic_select_name.sql", &[&self.cluster, name])
                    .await?
            }
        };

        let Some(row) = row else {
            return Ok(ErrorCode::UnknownTopicOrPartition);
        };

        let topic_name = row.try_get::<_, String>(1)?;

        _ = self
            .tx_prepare_query_opt(
                &tx,
                "topic_select_name_for_update.sql",
                &[&self.cluster, &topic_name],
            )
            .await?;

        // The foreign keys on `record` and `header` cascade from `topition`, so deleting
        // the topitions below deletes the records and headers. Deleting them here with a
        // join lets the planner scan the whole `record` table when its statistics are
        // skewed (#982).
        for (description, sql) in [
            ("consumer_offsets", "consumer_offset_delete_by_topic.sql"),
            (
                "topic_configuration",
                "topic_configuration_delete_by_topic.sql",
            ),
            ("watermarks", "watermark_delete_by_topic.sql"),
            (
                "txn_offset_commit_tp",
                "txn_offset_commit_tp_delete_by_topic.sql",
            ),
            (
                "txn_produce_offset_delete",
                "txn_produce_offset_delete_by_topic.sql",
            ),
            ("txn_topition", "txn_topition_delete_by_topic.sql"),
            ("producer_detail", "producer_detail_delete_by_topic.sql"),
            ("topitions", "topition_delete_by_topic.sql"),
        ] {
            let rows = self
                .tx_prepare_execute(&tx, sql, &[&self.cluster, &topic_name])
                .await
                .inspect_err(|err| {
                    debug!(?description, ?err);
                })?;

            debug!(?topic, ?rows, ?description);
        }

        _ = self
            .tx_prepare_execute(&tx, "topic_delete_by.sql", &[&self.cluster, &topic_name])
            .await?;

        tx.commit().await.inspect_err(|err| error!(?err))?;

        Ok(ErrorCode::None)
    }

    #[instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        match ConfigResource::from(resource.resource_type) {
            ConfigResource::Group => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::ClientMetric => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::BrokerLogger => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::Broker => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::Topic => {
                self.alter_topic(&resource).await?;

                Ok(AlterConfigsResourceResponse::default()
                    .error_code(ErrorCode::None.into())
                    .error_message(Some("".into()))
                    .resource_type(resource.resource_type)
                    .resource_name(resource.resource_name))
            }
            ConfigResource::Unknown => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
        }
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        self.produce_batch(transaction_id, topition, deflated, None)
            .await
    }

    async fn produce_inflated(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        inflated: Batch,
    ) -> Result<i64> {
        self.produce_batch(transaction_id, topition, deflated, Some(inflated))
            .await
    }

    #[instrument(skip_all)]
    async fn fetch(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
        fetched_sender: FetchedSender,
    ) -> Result<()> {
        // A `/` in the name checks out a connection of its own, so this
        // runs before the read holds its connection.
        let (base_topic, key_filter): (&str, Option<&str>) =
            self.topic_with_key(topition.topic()).await?;

        let permits = self.fetch_read_permits().await?;
        let mut connection = self
            .abandonable_connection(permits, StorageOperation::Fetch)
            .queued_for(ReadWait::EngineConnectionPool)
            .await?;

        // The read sends each record while its statement runs, so that a caller that stops the
        // read at its deadline keeps the records already sent. The read holds the connection and
        // the permits until it has put the last record in the channel, so when the channel fills
        // it waits for the caller with both held. A caller that drops the read mid-statement drops
        // the connection, which cancels the statement.
        let read = self
            .fetch_on(
                connection.object_mut()?,
                topition,
                base_topic,
                key_filter,
                offset,
                min_bytes,
                max_bytes,
                isolation_level,
                fetched_sender,
            )
            .await;

        match read {
            Ok(RecordsStatement::Ended) => {
                connection.release();
                Ok(())
            }
            // The statement may still be running on the connection. Dropping the connection
            // cancels it.
            Ok(RecordsStatement::Abandoned(error)) => {
                drop(connection);
                Err(error)
            }
            // No statement is running, so the connection goes straight back to the pool.
            Err(error) => {
                connection.release();
                Err(error)
            }
        }
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        debug!(cluster = self.cluster, ?topition);

        let base_topic = self.base_topic(topition.topic()).await?;

        let permits = self.fetch_read_permits().await?;
        let connection = self
            .abandonable_connection(permits, StorageOperation::OffsetStage)
            .queued_for(ReadWait::EngineConnectionPool)
            .await?;
        let offset_stage = self
            .offset_stage_on(connection.object()?, base_topic, topition.partition())
            .await;
        connection.release();

        offset_stage
    }

    #[instrument(skip_all)]
    async fn offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        debug!(cluster = self.cluster, ?group, ?retention);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let mut cg_inserted = false;

        let mut responses = vec![];

        for (topition, offset) in offsets {
            debug!(?topition, ?offset);

            if self
                .tx_prepare_query_opt(
                    &tx,
                    "topition_select.sql",
                    &[
                        &self.cluster,
                        &self.base_topic(topition.topic()).await?,
                        &topition.partition(),
                    ],
                )
                .await
                .inspect_err(|err| error!(?err))?
                .is_some()
            {
                if !cg_inserted {
                    let rows = self
                        .tx_prepare_execute(
                            &tx,
                            "consumer_group_insert.sql",
                            &[&self.cluster, &group],
                        )
                        .await?;
                    debug!(rows);

                    cg_inserted = true;
                }

                let rows = self
                    .tx_prepare_execute(
                        &tx,
                        "consumer_offset_insert.sql",
                        &[
                            &self.cluster,
                            &self.base_topic(topition.topic()).await?,
                            &topition.partition(),
                            &group,
                            &offset.offset,
                            &offset.leader_epoch,
                            &offset.timestamp,
                            &offset.metadata,
                        ],
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;

                debug!(?rows);

                responses.push((
                    topition.to_owned(),
                    if rows == 0 {
                        ErrorCode::UnknownTopicOrPartition
                    } else {
                        ErrorCode::None
                    },
                ));
            } else {
                responses.push((topition.to_owned(), ErrorCode::UnknownTopicOrPartition))
            }
        }

        tx.commit().await.inspect_err(|err| error!(?err))?;

        Ok(responses)
    }

    #[instrument(skip_all)]
    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        debug!(group_id);

        let mut results = BTreeMap::new();

        let c = self.connection().await?;

        for row in self
            .prepare_query(
                &c,
                consumer_offset_select_by_group::QUERY,
                &[&self.cluster, &group_id],
            )
            .await
            .inspect_err(|err| error!(?err))?
        {
            let topic = row.try_get::<_, String>(consumer_offset_select_by_group::TOPIC)?;
            let partition = row.try_get::<_, i32>(consumer_offset_select_by_group::PARTITION)?;
            let committed = CommittedOffset {
                offset: row.try_get(consumer_offset_select_by_group::COMMITTED_OFFSET)?,
                leader_epoch: row.try_get(consumer_offset_select_by_group::LEADER_EPOCH)?,
                metadata: row.try_get(consumer_offset_select_by_group::METADATA)?,
            };

            debug!(group_id, topic, partition, ?committed);

            assert_eq!(
                None,
                results.insert(Topition::new(topic, partition), committed)
            );
        }

        Ok(results)
    }

    #[instrument(skip_all)]
    async fn pending_txn_offsets(&self, group_id: &str) -> Result<BTreeSet<Topition>> {
        debug!(cluster = self.cluster, group_id);

        let c = self.connection().await?;

        self.prepare_query(
            &c,
            txn_offset_commit_tp_select_pending::QUERY,
            &[&self.cluster, &group_id],
        )
        .await
        .inspect_err(|err| error!(?err, cluster = self.cluster, group_id))?
        .into_iter()
        .map(|row| {
            Ok(Topition::new(
                row.try_get::<_, String>(txn_offset_commit_tp_select_pending::TOPIC)?,
                row.try_get::<_, i32>(txn_offset_commit_tp_select_pending::PARTITION)?,
            ))
        })
        .collect::<Result<BTreeSet<_>>>()
        .inspect(|pending| debug!(cluster = self.cluster, group_id, ?pending))
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        debug!(cluster = self.cluster, ?group_id, ?topics, ?require_stable);

        let c = self.connection().await?;

        let mut offsets = BTreeMap::new();

        for topic in topics {
            let committed = self
                .prepare_query_opt(
                    &c,
                    consumer_offset_select::QUERY,
                    &[
                        &self.cluster,
                        &group_id,
                        &self.base_topic(topic.topic()).await?,
                        &topic.partition(),
                    ],
                )
                .await
                .and_then(|maybe| {
                    maybe.map_or(Ok(CommittedOffset::uncommitted()), |row| {
                        Ok(CommittedOffset {
                            offset: row.try_get(consumer_offset_select::COMMITTED_OFFSET)?,
                            leader_epoch: row.try_get(consumer_offset_select::LEADER_EPOCH)?,
                            metadata: row.try_get(consumer_offset_select::METADATA)?,
                        })
                    })
                })
                .inspect(|committed| {
                    debug!(
                        cluster = self.cluster,
                        group_id,
                        topic = topic.topic,
                        partition = topic.partition,
                        ?committed
                    )
                })
                .inspect_err(|err| {
                    error!(
                        ?err,
                        cluster = self.cluster,
                        group_id,
                        topic = topic.topic,
                        partition = topic.partition
                    )
                })?;

            assert_eq!(None, offsets.insert(topic.to_owned(), committed));
        }

        Ok(offsets)
    }

    #[instrument(skip_all, fields(?offsets))]
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        debug!(cluster = self.cluster, ?isolation_level);

        let permits = self.list_offsets_read_permits().await?;
        let connection = self
            .abandonable_connection(permits, StorageOperation::ListOffsets)
            .queued_for(ReadWait::EngineConnectionPool)
            .await?;
        let responses = self
            .list_offsets_on(connection.object()?, isolation_level, offsets)
            .await;
        connection.release();

        responses.inspect(|r| debug!(?r))
    }

    #[instrument(skip_all)]
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        debug!(cluster = self.cluster, ?topics);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let brokers = vec![
            MetadataResponseBroker::default()
                .node_id(self.node)
                .host(
                    self.advertised_listener
                        .host_str()
                        .unwrap_or("0.0.0.0")
                        .into(),
                )
                .port(self.advertised_listener.port().unwrap_or(9092).into())
                .rack(None),
        ];

        debug!(?brokers);

        let responses = match topics {
            Some(topics) if !topics.is_empty() => {
                let mut responses = vec![];

                for topic in topics {
                    responses.push(match topic {
                        TopicId::Name(name) => {
                            let (base_topic, key) = self
                                .topic_with_key(name.as_str())
                                .await
                                .inspect(|(base_topic, key)| debug!(base_topic, key))?;

                            let vtid = if let Some(key) = key {
                                self.virtual_topic_id(base_topic, key)
                                    .await
                                    .map(|uuid| uuid.into_bytes())
                                    .map(Some)
                            } else {
                                Ok(None)
                            }?;

                            match self
                                .prepare_query_opt(
                                    &c,
                                    "topic_select_name.sql",
                                    &[&self.cluster, &base_topic],
                                )
                                .await
                                .inspect_err(|err| error!(?err))
                            {
                                Ok(Some(row)) => {
                                    let error_code = ErrorCode::None.into();

                                    let topic_id = vtid.or(row
                                        .try_get::<_, Uuid>(0)
                                        .map(|uuid| uuid.into_bytes())
                                        .map(Some)?);

                                    let is_internal = row.try_get::<_, bool>(2).map(Some)?;
                                    let partitions = row.try_get::<_, i32>(3)?;
                                    let replication_factor = row.try_get::<_, i32>(4)?;

                                    debug!(
                                        ?error_code,
                                        ?topic_id,
                                        ?name,
                                        ?is_internal,
                                        ?partitions,
                                        ?replication_factor
                                    );

                                    let mut rng = rng();
                                    let mut broker_ids: Vec<_> =
                                        brokers.iter().map(|broker| broker.node_id).collect();
                                    broker_ids.shuffle(&mut rng);

                                    let partitions = Some(nisshi_storage::assign_replicas(
                                        &broker_ids,
                                        partitions,
                                        replication_factor,
                                        error_code,
                                        0,
                                    )?);

                                    MetadataResponseTopic::default()
                                        .error_code(error_code)
                                        .name(Some(name.to_owned()))
                                        .topic_id(topic_id)
                                        .is_internal(is_internal)
                                        .partitions(partitions)
                                        .topic_authorized_operations(Some(-2147483648))
                                }

                                Ok(None) => MetadataResponseTopic::default()
                                    .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                    .name(Some(name.into()))
                                    .topic_id(Some(NULL_TOPIC_ID))
                                    .is_internal(Some(false))
                                    .partitions(Some([].into()))
                                    .topic_authorized_operations(Some(-2147483648)),

                                Err(reason) => {
                                    debug!(?reason);
                                    MetadataResponseTopic::default()
                                        .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                        .name(Some(name.into()))
                                        .topic_id(Some(NULL_TOPIC_ID))
                                        .is_internal(Some(false))
                                        .partitions(Some([].into()))
                                        .topic_authorized_operations(Some(-2147483648))
                                }
                            }
                        }
                        TopicId::Id(id) => {
                            debug!(?id);
                            match self
                                .prepare_query_one(
                                    &c,
                                    "pg/topic_select_uuid.sql",
                                    &[&self.cluster, &id],
                                )
                                .await
                            {
                                Ok(row) => {
                                    let error_code = ErrorCode::None.into();
                                    let topic_id = row
                                        .try_get::<_, Uuid>(0)
                                        .map(|uuid| uuid.into_bytes())
                                        .map(Some)?;
                                    let name = row.try_get::<_, String>(1).map(Some)?;
                                    let is_internal = row.try_get::<_, bool>(2).map(Some)?;
                                    let partitions = row.try_get::<_, i32>(3)?;
                                    let replication_factor = row.try_get::<_, i32>(4)?;

                                    debug!(
                                        ?error_code,
                                        ?topic_id,
                                        ?name,
                                        ?is_internal,
                                        ?partitions,
                                        ?replication_factor
                                    );

                                    let mut rng = rng();
                                    let mut broker_ids: Vec<_> =
                                        brokers.iter().map(|broker| broker.node_id).collect();
                                    broker_ids.shuffle(&mut rng);

                                    let partitions = Some(nisshi_storage::assign_replicas(
                                        &broker_ids,
                                        partitions,
                                        replication_factor,
                                        error_code,
                                        0,
                                    )?);

                                    MetadataResponseTopic::default()
                                        .error_code(error_code)
                                        .name(name)
                                        .topic_id(topic_id)
                                        .is_internal(is_internal)
                                        .partitions(partitions)
                                        .topic_authorized_operations(Some(-2147483648))
                                }
                                Err(reason) => {
                                    debug!(?reason);
                                    MetadataResponseTopic::default()
                                        .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                        .name(None)
                                        .topic_id(Some(id.into_bytes()))
                                        .is_internal(Some(false))
                                        .partitions(Some([].into()))
                                        .topic_authorized_operations(Some(-2147483648))
                                }
                            }
                        }
                    });
                }

                responses
            }

            _ => {
                let mut responses = vec![];

                match self
                    .prepare_query(&c, "topic_by_cluster.sql", &[&self.cluster])
                    .await
                    .inspect_err(|err| error!(?err))
                {
                    Ok(rows) => {
                        for row in rows {
                            let error_code = ErrorCode::None.into();
                            let topic_id = row
                                .try_get::<_, Uuid>(0)
                                .map(|uuid| uuid.into_bytes())
                                .map(Some)?;
                            let name = row.try_get::<_, String>(1).map(Some)?;
                            let is_internal = row.try_get::<_, bool>(2).map(Some)?;
                            let partitions = row.try_get::<_, i32>(3)?;
                            let replication_factor = row.try_get::<_, i32>(4)?;

                            debug!(
                                ?error_code,
                                ?topic_id,
                                ?name,
                                ?is_internal,
                                ?partitions,
                                ?replication_factor
                            );

                            let mut rng = rng();
                            let mut broker_ids: Vec<_> =
                                brokers.iter().map(|broker| broker.node_id).collect();
                            broker_ids.shuffle(&mut rng);

                            let partitions = Some(nisshi_storage::assign_replicas(
                                &broker_ids,
                                partitions,
                                replication_factor,
                                error_code,
                                0,
                            )?);

                            responses.push(
                                MetadataResponseTopic::default()
                                    .error_code(error_code)
                                    .name(name)
                                    .topic_id(topic_id)
                                    .is_internal(is_internal)
                                    .partitions(partitions)
                                    .topic_authorized_operations(Some(-2147483648)),
                            );
                        }
                    }
                    Err(reason) => {
                        debug!(?reason);
                        responses.push(
                            MetadataResponseTopic::default()
                                .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                .name(None)
                                .topic_id(Some(NULL_TOPIC_ID))
                                .is_internal(Some(false))
                                .partitions(Some([].into()))
                                .topic_authorized_operations(Some(-2147483648)),
                        );
                    }
                }

                responses
            }
        };

        Ok(MetadataResponse {
            cluster: Some(self.cluster.clone()),
            controller: Some(self.node),
            brokers,
            topics: responses,
        })
    }

    #[instrument(skip_all)]
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        debug!(cluster = self.cluster, name, ?resource, ?keys);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let configs = match keys.filter(|keys| !keys.is_empty()) {
            Some(keys) => self.topic_configs_by_key(&c, name, keys).await?,
            None => self.all_topic_configs(&c, name).await?,
        };

        if let Some(configs) = configs {
            let error_code = ErrorCode::None;

            Ok(DescribeConfigsResult::default()
                .error_code(error_code.into())
                .error_message(Some(error_code.to_string()))
                .resource_type(i8::from(resource))
                .resource_name(name.into())
                .configs(Some(configs)))
        } else {
            let error_code = ErrorCode::UnknownTopicOrPartition;

            Ok(DescribeConfigsResult::default()
                .error_code(error_code.into())
                .error_message(Some(error_code.to_string()))
                .resource_type(i8::from(resource))
                .resource_name(name.into())
                .configs(Some([].into())))
        }
    }

    #[instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let _ = (topics, partition_limit, cursor);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let mut responses =
            Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or_default());

        for topic in topics.unwrap_or_default() {
            debug!(?topic);

            responses.push(match topic {
                TopicId::Name(name) => {
                    match self
                        .prepare_query_opt(
                            &c,
                            "topic_select_name.sql",
                            &[&self.cluster, &name.as_str()],
                        )
                        .await
                        .inspect_err(|err| error!(?err))
                    {
                        Ok(Some(row)) => {
                            let topic_id =
                                row.try_get::<_, Uuid>(0).map(|uuid| uuid.into_bytes())?;
                            let name = row.try_get::<_, String>(1).map(Some)?;
                            let is_internal = row.try_get::<_, bool>(2).map(Some)?;
                            let partitions = row.try_get::<_, i32>(3)?;
                            let replication_factor = row.try_get::<_, i32>(4)?;

                            debug!(
                                ?topic_id,
                                ?name,
                                ?is_internal,
                                ?partitions,
                                ?replication_factor
                            );

                            DescribeTopicPartitionsResponseTopic::default()
                                .error_code(ErrorCode::None.into())
                                .name(name)
                                .topic_id(topic_id)
                                .is_internal(false)
                                .partitions(Some(
                                    (0..partitions)
                                        .map(|partition_index| {
                                            DescribeTopicPartitionsResponsePartition::default()
                                                .error_code(ErrorCode::None.into())
                                                .partition_index(partition_index)
                                                .leader_id(self.node)
                                                .leader_epoch(0)
                                                .replica_nodes(Some(
                                                    nisshi_storage::single_node_replicas(
                                                        self.node,
                                                        replication_factor,
                                                    ),
                                                ))
                                                .isr_nodes(Some(
                                                    nisshi_storage::single_node_replicas(
                                                        self.node,
                                                        replication_factor,
                                                    ),
                                                ))
                                                .eligible_leader_replicas(Some(vec![]))
                                                .last_known_elr(Some(vec![]))
                                                .offline_replicas(Some(vec![]))
                                        })
                                        .collect(),
                                ))
                                .topic_authorized_operations(-2147483648)
                        }

                        Ok(None) => DescribeTopicPartitionsResponseTopic::default()
                            .error_code(ErrorCode::UnknownTopicOrPartition.into())
                            .name(match topic {
                                TopicId::Name(name) => Some(name.into()),
                                TopicId::Id(_) => None,
                            })
                            .topic_id(match topic {
                                TopicId::Name(_) => NULL_TOPIC_ID,
                                TopicId::Id(id) => id.into_bytes(),
                            })
                            .is_internal(false)
                            .partitions(Some([].into()))
                            .topic_authorized_operations(-2147483648),

                        Err(reason) => {
                            debug!(?reason);
                            DescribeTopicPartitionsResponseTopic::default()
                                .error_code(ErrorCode::UnknownServerError.into())
                                .name(match topic {
                                    TopicId::Name(name) => Some(name.into()),
                                    TopicId::Id(_) => None,
                                })
                                .topic_id(match topic {
                                    TopicId::Name(_) => NULL_TOPIC_ID,
                                    TopicId::Id(id) => id.into_bytes(),
                                })
                                .is_internal(false)
                                .partitions(Some([].into()))
                                .topic_authorized_operations(-2147483648)
                        }
                    }
                }
                TopicId::Id(id) => {
                    debug!(?id);
                    match self
                        .prepare_query_one(&c, "topic_select_uuid.sql", &[&self.cluster, &id])
                        .await
                    {
                        Ok(row) => {
                            let topic_id =
                                row.try_get::<_, Uuid>(0).map(|uuid| uuid.into_bytes())?;
                            let name = row.try_get::<_, String>(1).map(Some)?;
                            let is_internal = row.try_get::<_, bool>(2).map(Some)?;
                            let partitions = row.try_get::<_, i32>(3)?;
                            let replication_factor = row.try_get::<_, i32>(4)?;

                            debug!(
                                ?topic_id,
                                ?name,
                                ?is_internal,
                                ?partitions,
                                ?replication_factor
                            );

                            DescribeTopicPartitionsResponseTopic::default()
                                .error_code(ErrorCode::None.into())
                                .name(name)
                                .topic_id(topic_id)
                                .is_internal(false)
                                .partitions(Some(
                                    (0..partitions)
                                        .map(|partition_index| {
                                            DescribeTopicPartitionsResponsePartition::default()
                                                .error_code(ErrorCode::None.into())
                                                .partition_index(partition_index)
                                                .leader_id(self.node)
                                                .leader_epoch(0)
                                                .replica_nodes(Some(
                                                    nisshi_storage::single_node_replicas(
                                                        self.node,
                                                        replication_factor,
                                                    ),
                                                ))
                                                .isr_nodes(Some(
                                                    nisshi_storage::single_node_replicas(
                                                        self.node,
                                                        replication_factor,
                                                    ),
                                                ))
                                                .eligible_leader_replicas(Some(vec![]))
                                                .last_known_elr(Some(vec![]))
                                                .offline_replicas(Some(vec![]))
                                        })
                                        .collect(),
                                ))
                                .topic_authorized_operations(-2147483648)
                        }

                        Err(reason) => {
                            debug!(?reason);
                            DescribeTopicPartitionsResponseTopic::default()
                                .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                .name(match topic {
                                    TopicId::Name(name) => Some(name.into()),
                                    TopicId::Id(_) => None,
                                })
                                .topic_id(match topic {
                                    TopicId::Name(_) => NULL_TOPIC_ID,
                                    TopicId::Id(id) => id.into_bytes(),
                                })
                                .is_internal(false)
                                .partitions(Some([].into()))
                                .topic_authorized_operations(-2147483648)
                        }
                    }
                }
            });
        }

        Ok(responses)
    }

    #[instrument(skip_all)]
    async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let mut listed_groups = vec![];

        for row in self
            .prepare_query(&c, consumer_group_select::QUERY, &[&self.cluster])
            .await
            .inspect_err(|err| error!(?err))?
        {
            let group_id = row.try_get::<_, String>(consumer_group_select::NAME)?;

            let stored = row
                .try_get::<_, Option<Value>>(consumer_group_select::DETAIL)?
                .map(serde_json::from_value::<GroupDetail>);

            listed_groups.push(NamedGroupDetail::decoded(group_id, stored));
        }

        Ok(listed_groups)
    }

    #[instrument(skip_all)]
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        debug!(?group_ids);

        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            let c = self.connection().await?;

            let consumer_offset = c
                .prepare_cached(self.sql_lookup("consumer_offset_delete_by_cg.sql")?)
                .await
                .inspect_err(|err| error!(?err))?;

            let group_detail = c
                .prepare_cached(self.sql_lookup("consumer_group_detail_delete_by_cg.sql")?)
                .await
                .inspect_err(|err| error!(?err))?;

            let group = c
                .prepare_cached(self.sql_lookup("consumer_group_delete.sql")?)
                .await
                .inspect_err(|err| error!(?err))?;

            for group_id in group_ids {
                _ = c
                    .execute(&consumer_offset, &[&self.cluster, &group_id])
                    .await
                    .inspect_err(|err| error!(?err))?;

                _ = c
                    .execute(&group_detail, &[&self.cluster, &group_id])
                    .await
                    .inspect_err(|err| error!(?err))?;

                let rows = c
                    .execute(&group, &[&self.cluster, &group_id])
                    .await
                    .inspect_err(|err| error!(?err))?;

                results.push(
                    DeletableGroupResult::default()
                        .group_id(group_id.into())
                        .error_code(
                            if rows == 0 {
                                ErrorCode::GroupIdNotFound
                            } else {
                                ErrorCode::None
                            }
                            .into(),
                        ),
                );
            }
        }

        Ok(results)
    }

    #[instrument(skip_all)]
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        debug!(?group_ids, include_authorized_operations);

        let mut results = vec![];
        let c = self.connection().await.inspect_err(|err| error!(?err));

        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                // The engine answers a group whose read fails with an error code, as Apache Kafka's
                // [`GroupCoordinatorService`] does, instead of failing the request, because the
                // broker closes the client's connection on a failed request and sends no response.
                // Kafka clients retry `COORDINATOR_NOT_AVAILABLE`, so a transient error answers
                // it, and any other error answers `UNKNOWN_SERVER_ERROR`.
                //
                // [`GroupCoordinatorService`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorService.java#L1139-L1184
                let c = match &c {
                    Ok(c) => c,
                    Err(err) => {
                        results.push(NamedGroupDetail::error_code(
                            group_id.into(),
                            err.error_code(ErrorCode::CoordinatorNotAvailable),
                        ));
                        continue;
                    }
                };

                match self
                    .prepare_query_opt(
                        c,
                        "consumer_group_select_by_name.sql",
                        &[&self.cluster, group_id],
                    )
                    .await
                    .inspect_err(|err| error!(?err, group_id))
                {
                    Ok(Some(row)) => {
                        // A group row with no detail row (e.g. a group that only ever committed
                        // offsets, never a JoinGroup) joins to a NULL `detail` column here: that
                        // group exists and is empty, not an error.
                        let stored = row
                            .try_get::<_, Option<Value>>(1)
                            .map_err(Error::from)
                            .transpose()
                            .map(|stored| {
                                stored.and_then(|value| {
                                    serde_json::from_value::<GroupDetail>(value)
                                        .map_err(Error::from)
                                        .inspect(|current| debug!(?current))
                                })
                            });

                        results.push(NamedGroupDetail::decoded(group_id.into(), stored));
                    }

                    Ok(None) => results.push(NamedGroupDetail::error_code(
                        group_id.into(),
                        ErrorCode::GroupIdNotFound,
                    )),

                    Err(err) => results.push(NamedGroupDetail::error_code(
                        group_id.into(),
                        err.error_code(ErrorCode::CoordinatorNotAvailable),
                    )),
                }
            }
        }

        Ok(results)
    }

    #[instrument(skip_all)]
    async fn group_with_version(&self, group_id: &str) -> Result<Option<(GroupDetail, Version)>> {
        debug!(cluster = self.cluster, group_id);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        self.prepare_query_opt(
            &c,
            "consumer_group_detail.sql",
            &[&group_id, &self.cluster.as_str()],
        )
        .await
        .inspect_err(|err| error!(?err, group_id))?
        .as_ref()
        .map(group_detail_with_version)
        .transpose()
        .inspect_err(|err| error!(?err, group_id, "the stored group can't be decoded"))
    }

    #[instrument(skip_all)]
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        debug!(cluster = self.cluster, group_id, ?detail, ?version);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        _ = self
            .tx_prepare_execute(
                &tx,
                "consumer_group_insert.sql",
                &[&self.cluster, &group_id],
            )
            .await?;

        let existing_e_tag = version
            .as_ref()
            .map_or(Ok(Uuid::from_u128(0)), |version| {
                version
                    .e_tag
                    .as_ref()
                    .map_or(Err(UpdateError::MissingEtag::<GroupDetail>), |e_tag| {
                        Uuid::from_str(e_tag.as_str()).map_err(Into::into)
                    })
            })
            .inspect_err(|err| error!(?err))
            .inspect(|existing_e_tag| debug!(?existing_e_tag))?;

        let new_e_tag = default_hash(&detail);
        debug!(?new_e_tag);

        let detail = serde_json::to_value(detail).inspect(|detail| debug!(?detail))?;

        let outcome = if let Some(row) = self
            .tx_prepare_query_opt(
                &tx,
                "consumer_group_detail_insert.sql",
                &[
                    &self.cluster,
                    &group_id,
                    &existing_e_tag,
                    &new_e_tag,
                    &detail,
                ],
            )
            .await
            .inspect(|row| debug!(?row))
            .inspect_err(|err| error!(?err))?
        {
            row.try_get::<_, Uuid>(2)
                .inspect_err(|err| error!(?err))
                .map_err(Into::into)
                .map(|uuid| uuid.to_string())
                .map(Some)
                .map(|e_tag| Version {
                    e_tag,
                    version: None,
                })
                .inspect(|version| debug!(?version))
        } else {
            let row = self
                .tx_prepare_query_one(
                    &tx,
                    "consumer_group_detail.sql",
                    &[&group_id, &self.cluster.as_str()],
                )
                .await
                .inspect(|row| debug!(?row))
                .inspect_err(|err| error!(?err))?;

            let (current, version) = group_detail_with_version(&row)?;

            Err(UpdateError::Outdated {
                current: Box::new(current),
                version,
            })
        };

        tx.commit().await.inspect_err(|err| error!(?err))?;

        debug!(?outcome);

        outcome
    }

    #[instrument(skip_all)]
    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        debug!(
            cluster = self.cluster,
            transaction_id, producer_id, producer_epoch
        );

        match producer_claim(producer_id, producer_epoch) {
            Ok(claim) => {
                self.bump_or_create_producer(transaction_id, transaction_timeout_ms, claim)
                    .await
            }

            Err(error) => Ok(ProducerIdResponse::failed(error)),
        }
    }

    #[instrument(skip_all)]
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        debug!(
            cluster = self.cluster,
            transaction_id, producer_id, producer_epoch, group_id
        );

        Ok(ErrorCode::None)
    }

    #[instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        debug!(cluster = self.cluster, ?partitions);

        match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id,
                producer_id,
                producer_epoch,
                topics,
            } => {
                debug!(?transaction_id, ?producer_id, ?producer_epoch, ?topics);

                let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
                let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

                let mut results = vec![];

                for topic in topics {
                    let mut results_by_partition = vec![];

                    for partition_index in topic.partitions.unwrap_or(vec![]) {
                        _ = self
                            .tx_prepare_execute(
                                &tx,
                                "txn_topition_insert.sql",
                                &[
                                    &self.cluster,
                                    &topic.name,
                                    &partition_index,
                                    &transaction_id,
                                    &producer_id,
                                    &producer_epoch,
                                ],
                            )
                            .await
                            .inspect_err(|err| {
                                error!(
                                    ?err,
                                    cluster = self.cluster,
                                    topic = topic.name,
                                    ?partition_index,
                                    transaction_id
                                )
                            })?;

                        results_by_partition.push(
                            AddPartitionsToTxnPartitionResult::default()
                                .partition_index(partition_index)
                                .partition_error_code(i16::from(ErrorCode::None)),
                        );
                    }

                    results.push(
                        AddPartitionsToTxnTopicResult::default()
                            .name(topic.name)
                            .results_by_partition(Some(results_by_partition)),
                    )
                }

                _ = self
                    .tx_prepare_execute(
                        &tx,
                        "txn_detail_update_started_at.sql",
                        &[
                            &self.cluster,
                            &transaction_id,
                            &producer_id,
                            &producer_epoch,
                        ],
                    )
                    .await
                    .inspect_err(|err| {
                        error!(
                            ?err,
                            cluster = self.cluster,
                            transaction_id,
                            producer_id,
                            producer_epoch,
                        )
                    })?;

                tx.commit().await?;

                Ok(TxnAddPartitionsResponse::VersionZeroToThree(results))
            }

            TxnAddPartitionsRequest::VersionFourPlus { .. } => {
                Err(Error::UnsupportedTxnAddPartitionsVersion)
            }
        }
    }

    #[instrument(skip_all)]
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        debug!(cluster = self.cluster, ?offsets);

        let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
        let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

        let (producer_id, producer_epoch) = if let Some(row) = self
            .tx_prepare_query_opt(
                &tx,
                "producer_epoch_for_current_txn.sql",
                &[&self.cluster, &offsets.transaction_id],
            )
            .await
            .inspect_err(|err| error!(?err))?
        {
            let producer_id = row
                .try_get::<_, i64>(0)
                .map(Some)
                .inspect_err(|err| error!(?err))?;

            let epoch = row
                .try_get::<_, i16>(1)
                .map(Some)
                .inspect_err(|err| error!(?err))?;

            (producer_id, epoch)
        } else {
            (None, None)
        };

        _ = self
            .tx_prepare_execute(
                &tx,
                "consumer_group_insert.sql",
                &[&self.cluster, &offsets.group_id],
            )
            .await?;

        debug!(?producer_id, ?producer_epoch);

        _ = self
            .tx_prepare_execute(
                &tx,
                "txn_offset_commit_insert.sql",
                &[
                    &self.cluster,
                    &offsets.transaction_id,
                    &offsets.group_id,
                    &offsets.producer_id,
                    &offsets.producer_epoch,
                    &offsets.generation_id,
                    &offsets.member_id,
                ],
            )
            .await
            .inspect_err(|err| error!(?err))?;

        let mut topics = vec![];

        for topic in offsets.topics {
            let mut partitions = vec![];

            for partition in topic.partitions.unwrap_or(vec![]) {
                if producer_id.is_some_and(|producer_id| producer_id == offsets.producer_id) {
                    if producer_epoch
                        .is_some_and(|producer_epoch| producer_epoch == offsets.producer_epoch)
                    {
                        _ = self
                            .tx_prepare_execute(
                                &tx,
                                "txn_offset_commit_tp_insert.sql",
                                &[
                                    &self.cluster,
                                    &offsets.transaction_id,
                                    &offsets.group_id,
                                    &offsets.producer_id,
                                    &offsets.producer_epoch,
                                    &topic.name,
                                    &partition.partition_index,
                                    &partition.committed_offset,
                                    &partition.committed_leader_epoch,
                                    &partition.committed_metadata,
                                ],
                            )
                            .await
                            .inspect_err(|err| error!(?err))?;

                        partitions.push(
                            TxnOffsetCommitResponsePartition::default()
                                .partition_index(partition.partition_index)
                                .error_code(i16::from(ErrorCode::None)),
                        );
                    } else {
                        partitions.push(
                            TxnOffsetCommitResponsePartition::default()
                                .partition_index(partition.partition_index)
                                .error_code(i16::from(ErrorCode::InvalidProducerEpoch)),
                        );
                    }
                } else {
                    partitions.push(
                        TxnOffsetCommitResponsePartition::default()
                            .partition_index(partition.partition_index)
                            .error_code(i16::from(ErrorCode::UnknownProducerId)),
                    );
                }
            }

            topics.push(
                TxnOffsetCommitResponseTopic::default()
                    .name(topic.name)
                    .partitions(Some(partitions)),
            );
        }

        tx.commit().await?;

        Ok(topics)
    }

    #[instrument(skip_all)]
    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        debug!(cluster = ?self.cluster, transaction_id, producer_id, producer_epoch, committed);

        let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
        let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

        let error_code = self
            .end_in_tx(
                transaction_id,
                producer_id,
                producer_epoch,
                committed,
                false,
                &tx,
            )
            .await?;

        tx.commit().await?;

        Ok(error_code)
    }

    #[instrument(skip_all)]
    /// Runs each maintenance step, whether or not the steps before it
    /// failed: retention, the removal of the records that DeleteRecords left
    /// below the log start, compaction and the lake. No step depends on
    /// another, so one that fails, such as a retention sweep that hits a
    /// deadlock or a `lock_timeout` set in the connection options, does not
    /// hold back the others until the next run. Each failure is logged, and
    /// the first is returned.
    async fn maintain(&self, now: SystemTime) -> Result<()> {
        let deleted = self
            .policy_delete(now)
            .await
            .inspect(|deleted| debug!(deleted))
            .inspect_err(|err| warn!(?err, "retention failed"))
            .map(|_| ());

        let below_log_start = self
            .record_delete_below_log_start()
            .await
            .inspect(|below_log_start| debug!(below_log_start))
            .inspect_err(|err| warn!(?err, "removing records below the log start failed"))
            .map(|_| ());

        let compacted = self
            .policy_compact()
            .await
            .inspect(|compacted| debug!(compacted))
            .inspect_err(|err| warn!(?err, "compaction failed"))
            .map(|_| ());

        let lake = match self.lake {
            Some(ref lake) => lake
                .maintain()
                .await
                .inspect_err(|err| warn!(?err, "lake maintenance failed"))
                .map_err(Into::into),
            None => Ok(()),
        };

        deleted.and(below_log_start).and(compacted).and(lake)
    }

    #[instrument(skip(self), ret)]
    async fn maintain_transactions(&self, now: SystemTime) -> Result<()> {
        let c = self.connection().await?;

        let rows = self
            .prepare_query(
                &c,
                "txn_detail_select_timed_out.sql",
                &[&self.cluster, &now],
            )
            .await?;

        // release the listing connection before aborting: each abort_timed_out below checks
        // out its own connection from the same pool, and the pool can be small
        // (or exactly 1), so holding this one idle risks starving/deadlocking them.
        drop(c);

        for row in rows {
            let transaction_id = row.try_get::<_, String>(0)?;
            let producer_id = row.try_get::<_, i64>(1)?;
            let producer_epoch = row.try_get::<_, i16>(2)?;

            match self
                .abort_timed_out(&transaction_id, producer_id, producer_epoch)
                .await
            {
                Ok(ErrorCode::None) => {}
                // Benign, expected outcome: the sweep lost a race against a real client that
                // already finalized this transaction between the SELECT above and this call.
                // Not an operational problem, so debug! rather than error!.
                Ok(error_code) => debug!(
                    ?error_code,
                    transaction_id,
                    producer_id,
                    producer_epoch,
                    "maintain_transactions: abort rejected"
                ),
                Err(ref err) => error!(
                    ?err,
                    transaction_id, producer_id, producer_epoch, "maintain_transactions"
                ),
            }
        }

        Ok(())
    }

    async fn aborted_transactions(
        &self,
        _topition: &Topition,
        _offset: i64,
        _last_stable_offset: i64,
    ) -> Result<Vec<AbortedTransaction>> {
        Ok(vec![])
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        let c = self.connection().await?;

        self.prepare_execute(
            &c,
            "scram_credential_delete.sql",
            &[&self.cluster, &user, &i32::from(mechanism)],
        )
        .await
        .inspect_err(|err| error!(?err, ?user, ?mechanism,))
        .and(Ok(()))
    }

    async fn upsert_user_scram_credential(
        &self,
        username: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let c = self.connection().await?;

        self.prepare_execute(
            &c,
            "scram_credential_insert.sql",
            &[
                &self.cluster,
                &username,
                &i32::from(mechanism),
                &&credential.salt[..],
                &credential.iterations,
                &&credential.stored_key[..],
                &&credential.server_key[..],
            ],
        )
        .await
        .inspect_err(|err| error!(?err, ?username, ?mechanism,))
        .and(Ok(()))
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        let c = self.connection().await?;

        self.prepare_query_opt(
            &c,
            "scram_credential_select.sql",
            &[&self.cluster, &user, &i32::from(mechanism)],
        )
        .await
        .and_then(|maybe| {
            if let Some(row) = maybe {
                let salt = row.try_get::<_, &[u8]>(0).map(Bytes::copy_from_slice)?;
                let iterations = row.try_get::<_, i32>(1)?;
                let stored_key = row.try_get::<_, &[u8]>(2).map(Bytes::copy_from_slice)?;
                let server_key = row.try_get::<_, &[u8]>(3).map(Bytes::copy_from_slice)?;

                Ok(Some(ScramCredential {
                    salt,
                    iterations,
                    stored_key,
                    server_key,
                }))
            } else {
                Ok(None)
            }
        })
        .inspect_err(|err| error!(?err, ?user, ?mechanism,))
    }

    async fn cluster_id(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    async fn node(&self) -> Result<i32> {
        Ok(self.node)
    }

    async fn advertised_listener(&self) -> Result<Url> {
        Ok(self.advertised_listener.clone())
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        let c = self.pool.get().await?;
        let _ = self.prepare_query(&c, "ping.sql", &[]).await?;
        Ok(())
    }
}

/// The earliest Kafka timestamp, in milliseconds since the epoch, that a PostgreSQL `timestamp`
/// can hold: 4713 BC.
const MIN_SQL_TIMESTAMP_MS: i64 = -210_866_803_200_000;

/// Returns the instant that the record `timestamp`, in milliseconds since the epoch, is stored as.
///
/// A producer can send a record with no timestamp, Kafka's `NO_TIMESTAMP`, -1, and Kafka's broker
/// accepts it: `LogValidator` skips the range check for it, and a `TimestampBeforeMaxMs` of
/// `Long.MAX_VALUE` leaves no lower bound for any other negative value either. [`to_system_time`]
/// rejects a negative timestamp, so a negative one is stored as the instant before the epoch that
/// it names, which a `timestamp` column and a [`SystemTime`] both hold. It is not stored as null,
/// though the column allows null, because retention tests the timestamp of each record: a null
/// never expires, while an instant in 1969 expires with the record's append time, as Kafka's does
/// by the segment's modification time when it has no timestamp.
///
/// <https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogValidator.java#L583-L602>
///
/// Returns an error for a timestamp that PostgreSQL cannot hold.
///
/// Only this write side is checked. The read side of a timestamp before 1601 is unchecked: the
/// `postgres-types` `FromSql<SystemTime>` conversion and the fetch paths can panic a Windows
/// broker on a row that a Linux broker wrote (SOL-157762).
fn to_sql_timestamp(timestamp: i64) -> Result<SystemTime> {
    if timestamp >= 0 {
        to_system_time(timestamp).map_err(Into::into)
    } else if timestamp < MIN_SQL_TIMESTAMP_MS {
        Err(Error::Message(format!(
            "timestamp {timestamp} is before the earliest that PostgreSQL stores"
        )))
    } else {
        // `SystemTime - Duration` panics on overflow, and on Windows a `SystemTime` cannot be
        // earlier than 1601, so a timestamp between `MIN_SQL_TIMESTAMP_MS` and that year must
        // be an error there. Kafka 3.9.1 accepts such a timestamp on a CreateTime topic with
        // the default `message.timestamp.before.max.ms`; this broker answers
        // UNKNOWN_SERVER_ERROR for it, a deliberate difference on Windows only.
        SystemTime::UNIX_EPOCH
            .checked_sub(Duration::from_millis(timestamp.unsigned_abs()))
            .ok_or_else(|| {
                Error::Message(format!(
                    "timestamp {timestamp} is before the earliest that this broker stores"
                ))
            })
    }
}

/// Returns the Kafka timestamp, in milliseconds since the epoch, of a stored `system_time`.
/// The inverse of [`to_sql_timestamp`]: an instant before the epoch is a negative timestamp, as
/// -1 is for a record that was produced without one.
pub(crate) fn from_sql_timestamp(system_time: &SystemTime) -> Result<i64> {
    match system_time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(_) => to_timestamp(system_time).map_err(Into::into),
        Err(before) => i64::try_from(before.duration().as_millis())
            .map(|millis| -millis)
            .map_err(Into::into),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures_util::future;
    use nisshi_sans_io::{
        add_partitions_to_txn_request::AddPartitionsToTxnTopic,
        record::{Header, header},
    };
    use nisshi_storage::read_share::ClientIdentity;
    use nisshi_storage::{
        ArcDynStorage, StartupFailure, StorageFactory as _, StorageFactoryConfiguration,
    };
    use nisshi_storage::{
        fetched::{self, FetchUntilDeadline as _},
        read_deadline::TimedOut,
    };
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::metrics::{
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        data::{AggregatedMetrics, MetricData, ResourceMetrics, ScopeMetrics, SumDataPoint},
    };
    use rand::distr::Alphanumeric;
    use std::{
        assert_matches,
        sync::{
            LazyLock,
            atomic::{self, AtomicBool},
        },
    };
    use tokio::task::JoinHandle;

    #[cfg(not(windows))]
    #[test]
    fn sql_timestamp_round_trips_a_negative_timestamp() -> Result<()> {
        for timestamp in [
            -1,
            -2,
            -86_400_000,
            0,
            1,
            1_700_000_000_000,
            MIN_SQL_TIMESTAMP_MS,
        ] {
            assert_eq!(
                timestamp,
                from_sql_timestamp(&to_sql_timestamp(timestamp)?)?,
                "{timestamp}"
            );
        }

        Ok(())
    }

    /// A Windows `SystemTime` cannot be earlier than 1601, so the earliest timestamp that
    /// PostgreSQL holds is an error there, where it round-trips on other platforms.
    #[cfg(windows)]
    #[test]
    fn sql_timestamp_round_trips_a_negative_timestamp() -> Result<()> {
        for timestamp in [-1, -2, -86_400_000, 0, 1, 1_700_000_000_000] {
            assert_eq!(
                timestamp,
                from_sql_timestamp(&to_sql_timestamp(timestamp)?)?,
                "{timestamp}"
            );
        }
        assert_matches!(
            to_sql_timestamp(MIN_SQL_TIMESTAMP_MS),
            Err(Error::Message(_))
        );

        Ok(())
    }

    #[test]
    fn sql_timestamp_refuses_what_postgres_cannot_hold() {
        assert_matches!(
            to_sql_timestamp(MIN_SQL_TIMESTAMP_MS - 1),
            Err(Error::Message(_))
        );
        assert_matches!(to_sql_timestamp(i64::MIN), Err(Error::Message(_)));
    }

    /// The PostgreSQL server that the tests use: `NISSHI_TEST_DATABASE_URL`, or else the `db`
    /// service of compose.yaml on its default port.
    static CONNECTION: LazyLock<String> = LazyLock::new(|| {
        std::env::var("NISSHI_TEST_DATABASE_URL")
            .unwrap_or_else(|_| String::from("postgres://postgres:postgres@localhost"))
    });

    fn alphanumeric_string(length: usize) -> String {
        rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }

    /// A change to the schema must come with a new [`SCHEMA_VERSION`], and with an upgrade step for
    /// databases of the earlier version. A database that records a version keeps the tables that
    /// it has, so a change without a new version reaches only new databases. Update the digest
    /// here once both are in place.
    #[test]
    fn schema_matches_its_version() -> Result<()> {
        let digest = crate::schema::schema_digest([
            crate::sql::remove_comments(SCHEMA).as_str(),
            crate::sql::SQL.get("nisshi_schema_version_create.sql")?,
        ]);

        assert_eq!(
            (SCHEMA_VERSION, digest.as_str()),
            (
                1,
                "c453a24e053100c8c68e7f31694fa6f9920d20225b1d245aac671b9c198c9dad"
            )
        );
        Ok(())
    }

    /// Returns a [`Postgres`] storage engine for `cluster` on the server at `url`, with the schema
    /// created. Returns `None` when the server can't be reached. The caller then skips `test`.
    async fn storage_with_schema(test: &str, url: &str, cluster: &str) -> Result<Option<Postgres>> {
        let storage = Postgres::builder(url)?
            .cluster(cluster)
            .node(rng().random_range(0..i32::MAX))
            .build();

        // The test skips only when the server can't be reached. Any later error fails it.
        if let Err(err) = storage.connection().await {
            eprintln!("skipping {test}: {err:?}");
            return Ok(None);
        }

        storage.ensure_schema().await?;
        Ok(Some(storage))
    }

    #[test]
    fn merge_statement_timeout_option_no_existing_options() {
        assert_eq!(
            merge_statement_timeout_option(None, 30_000),
            "-c statement_timeout=30000"
        );
        assert_eq!(
            merge_statement_timeout_option(Some(""), 30_000),
            "-c statement_timeout=30000"
        );
    }

    /// Postgres applies startup `-c name=value` options in order with last-one-wins
    /// semantics, so an operator's own explicit `statement_timeout` must appear after (and
    /// therefore override) our default.
    #[test]
    fn merge_statement_timeout_option_preserves_operator_override() {
        assert_eq!(
            merge_statement_timeout_option(Some("-c statement_timeout=120000"), 30_000),
            "-c statement_timeout=30000 -c statement_timeout=120000"
        );
    }

    #[test]
    fn merge_statement_timeout_option_preserves_unrelated_existing_options() {
        assert_eq!(
            merge_statement_timeout_option(Some("-c search_path=foo"), 30_000),
            "-c statement_timeout=30000 -c search_path=foo"
        );
    }

    /// The last `statement_timeout` in the options wins, as on the server,
    /// in either spelling and with the server's time units.
    #[test]
    fn statement_timeout_option_reads_the_last_setting() {
        assert_eq!(
            Some(Duration::from_secs(30)),
            statement_timeout_option("-c statement_timeout=30000")
        );
        assert_eq!(
            Some(Duration::from_secs(120)),
            statement_timeout_option("-c statement_timeout=30000 -c statement_timeout=120s")
        );
        assert_eq!(
            Some(Duration::from_secs(120)),
            statement_timeout_option("-c statement_timeout=30000 --statement_timeout=2min")
        );
        assert_eq!(
            Some(Duration::from_millis(100)),
            statement_timeout_option(
                "-c statement_timeout=30000 -c search_path=foo -c statement_timeout=100"
            )
        );
        assert_eq!(
            Some(Duration::from_secs(120)),
            statement_timeout_option("-c statement_timeout=30000 -cstatement_timeout=120s")
        );
        assert_eq!(
            Some(Duration::from_secs(150)),
            statement_timeout_option("-c statement_timeout=2.5min")
        );
        assert_eq!(
            Some(Duration::from_micros(500)),
            statement_timeout_option("-c statement_timeout=500us")
        );
    }

    /// The server reads `-` in a setting name as `_`, so a dashed spelling
    /// is the same setting.
    #[test]
    fn option_settings_read_every_spelling() {
        assert!(options_set_application_name("-c application_name=a"));
        assert!(options_set_application_name("-capplication_name=a"));
        assert!(options_set_application_name("--application_name=a"));
        assert!(options_set_application_name("-c application-name=a"));
        assert!(options_set_application_name("--application-name=a"));
        assert!(options_set_application_name("-c Application_Name=a"));
        assert!(options_set_application_name(
            "-c search_path=foo -c application_name=a"
        ));
        assert!(!options_set_application_name(""));
        assert!(!options_set_application_name(
            "-c search_path=application_name"
        ));
        assert_eq!(
            Some(Duration::from_secs(2)),
            statement_timeout_option("-c statement-timeout=2s")
        );
    }

    /// The machine's name has no surrounding white space and is not empty,
    /// or is not known.
    #[test]
    fn local_host_name_is_trimmed_or_unknown() {
        assert!(local_host_name().is_none_or(|host| !host.is_empty() && host == host.trim()));
    }

    /// The name has the host first and the cluster after it, and the node
    /// only where the host is not known.
    #[test]
    fn application_name_names_the_host_then_the_cluster() {
        assert_eq!(
            "nisshi-pod-0.ns-cluster.a_b-c",
            application_name(Some("pod-0.ns"), "cluster.a_b-c", 111)
        );
        assert_eq!("nisshi-111-c", application_name(None, "c", 111));
        assert_eq!("nisshi-111-c", application_name(Some(""), "c", 111));
    }

    /// A character that the server would turn into `?`, or that a filter would
    /// have to quote, becomes `_`, in the host and in the cluster.
    #[test]
    fn application_name_replaces_characters_a_filter_cannot_use() {
        assert_eq!(
            "nisshi-h_t-a_b_c_d_e_",
            application_name(Some("h t"), "a b'c\"d\u{e9}e\n", 1)
        );
    }

    /// The server keeps 63 bytes of a name. The cluster is cut first, so the
    /// host stays whole, and the host is cut only where it alone is too long.
    #[test]
    fn application_name_is_cut_at_the_servers_limit_and_keeps_the_host() {
        let host = "h".repeat(40);
        let name = application_name(Some(&host), &"c".repeat(200), 1);

        assert_eq!(APPLICATION_NAME_MAX_LEN, name.len());
        assert!(name.starts_with(&format!("nisshi-{host}-c")), "{name}");

        let exact = application_name(Some("h"), &"c".repeat(APPLICATION_NAME_MAX_LEN), 1);
        assert_eq!(APPLICATION_NAME_MAX_LEN, exact.len());

        let long_host = application_name(Some(&"h".repeat(200)), "cluster", 1);
        assert_eq!(APPLICATION_NAME_MAX_LEN, long_host.len());
        assert!(long_host.starts_with("nisshi-hhh"), "{long_host}");
    }

    /// Reads the `application_name` that the server shows for the connection of
    /// `storage`.
    async fn connection_name(storage: &Postgres) -> Result<String> {
        let connection = storage.connection().await?;

        connection
            .query_one(
                "select application_name from pg_stat_activity where pid = pg_backend_pid()",
                &[],
            )
            .await
            .map(|row| row.get::<_, String>(0))
            .map_err(Into::into)
    }

    /// Every connection of the pool is named in `pg_stat_activity` by its host
    /// and cluster, and a name that the operator sets, in the URL or in
    /// `options`, wins.
    #[tokio::test]
    async fn connections_are_named_in_pg_stat_activity() -> Result<()> {
        // Only an unreachable database skips the test. A case that fails to connect after
        // this probe did is a regression of the `options` merge, so it fails the test.
        let probe = Postgres::builder(&CONNECTION)?
            .cluster(alphanumeric_string(15))
            .node(rng().random_range(0..i32::MAX))
            .build();

        if let Err(err) = probe.connection().await {
            eprintln!("skipping connections_are_named_in_pg_stat_activity: {err:?}");
            return Ok(());
        }

        let cluster = alphanumeric_string(15);

        let cases = [
            ("", format!("nisshi-pod-0-{cluster}")),
            ("?application_name=from_url", "from_url".to_owned()),
            (
                "?options=-c%20application_name%3Dfrom_options",
                "from_options".to_owned(),
            ),
            (
                "?options=-c%20application-name%3Dfrom_dashes",
                "from_dashes".to_owned(),
            ),
        ];

        for (query, expected) in cases {
            let storage = Postgres::builder(&format!("{}{query}", *CONNECTION))?
                .host("pod-0")
                .cluster(cluster.clone())
                .node(111)
                .build();

            assert_eq!(expected, connection_name(&storage).await?, "for {query:?}");
        }

        Ok(())
    }

    /// Brokers of one database that differ in host or in cluster show
    /// different names, and the name stays within what the server keeps, so
    /// the server shows exactly the name that was set. A broker whose host is
    /// not known shows its node.
    #[tokio::test]
    async fn brokers_are_told_apart_in_pg_stat_activity() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let brokers = [
            (Some("pod-0"), cluster.clone()),
            (Some("pod-1"), cluster.clone()),
            (Some("pod-0"), alphanumeric_string(15)),
            (Some("pod-0"), "c".repeat(200)),
            (None, cluster.clone()),
        ];

        let mut names = Vec::new();

        for (host, cluster) in brokers {
            let mut builder = Postgres::builder(&CONNECTION)?;

            if let Some(host) = host {
                builder = builder.host(host);
            } else {
                // The machine that runs the test may have a host name, so leave it unknown.
                builder.pool.host = None;
            }

            let storage = builder.cluster(cluster).node(111).build();

            // Only the first case may mean that there is no database. A later case that fails
            // to connect fails the test.
            let name = match connection_name(&storage).await {
                Ok(name) => name,
                Err(err) if names.is_empty() => {
                    eprintln!("skipping brokers_are_told_apart_in_pg_stat_activity: {err:?}");
                    return Ok(());
                }
                Err(err) => return Err(err),
            };

            assert!(name.starts_with("nisshi-"), "{name}");
            assert!(name.len() <= APPLICATION_NAME_MAX_LEN, "{name}");
            names.push(name);
        }

        let distinct = names.iter().collect::<std::collections::HashSet<_>>();
        assert_eq!(names.len(), distinct.len(), "{names:?}");
        assert_eq!(format!("nisshi-pod-0-{cluster}"), names[0]);
        assert_eq!(format!("nisshi-pod-1-{cluster}"), names[1]);
        assert_eq!(APPLICATION_NAME_MAX_LEN, names[3].len());
        assert_eq!(format!("nisshi-111-{cluster}"), names[4]);

        Ok(())
    }

    /// No setting, a disabled timeout and a value the server would reject
    /// all leave the default in place.
    #[test]
    fn statement_timeout_option_without_a_usable_setting() {
        assert_eq!(None, statement_timeout_option(""));
        assert_eq!(None, statement_timeout_option("-c search_path=foo"));
        assert_eq!(
            None,
            statement_timeout_option("-c statement_timeout=30000 -c statement_timeout=0")
        );
        assert_eq!(None, statement_timeout_option("-c statement_timeout=soon"));
        assert_eq!(
            None,
            statement_timeout_option("-c statement_timeout=5fortnights")
        );
        assert_eq!(None, statement_timeout_option("-c statement_timeout=-1"));
    }

    /// The bound on an abandoned statement follows the connection's
    /// `statement_timeout`: the default, or an operator's own in the URL.
    #[test]
    fn abandoned_statement_bound_follows_the_statement_timeout() -> Result<()> {
        let storage = Postgres::builder(&CONNECTION)?
            .cluster("default")
            .node(0)
            .build();
        assert_eq!(
            Duration::from_millis(DEFAULT_STATEMENT_TIMEOUT_MS) + ABANDONED_STATEMENT_MARGIN,
            storage.abandoned_statement_bound
        );

        let storage = Postgres::builder(&format!(
            "{}?options=-c%20statement_timeout%3D120000",
            *CONNECTION
        ))?
        .cluster("longer")
        .node(0)
        .build();
        assert_eq!(
            Duration::from_secs(120) + ABANDONED_STATEMENT_MARGIN,
            storage.abandoned_statement_bound
        );

        Ok(())
    }

    /// Regression test for a real blocker found in review: deadpool's `PoolBuilder::build()`
    /// returns `Err(BuildError::NoRuntimeSpecified)` if `wait_timeout`/`create_timeout` are
    /// set without also configuring `.runtime(..)`. This doesn't need a live Postgres:
    /// `Config::from_str` and `Pool::builder(..).build()` only parse/construct, they don't
    /// connect.
    #[test]
    fn pool_builds_with_configured_timeouts() -> Result<()> {
        let builder = Postgres::builder(&CONNECTION)?;

        let timeouts = builder.pool.pool.timeouts();
        assert_eq!(timeouts.wait, Some(DEFAULT_POOL_WAIT_TIMEOUT));
        assert_eq!(timeouts.create, Some(DEFAULT_POOL_CREATE_TIMEOUT));
        assert_eq!(timeouts.recycle, Some(DEFAULT_POOL_RECYCLE_TIMEOUT));

        Ok(())
    }

    /// A storage URL option that tokio-postgres does not read (`vacuum_into` is an
    /// option of the sqlite engine) stops the build before any connection, and the
    /// error names the option.
    #[test]
    fn unrecognized_query_option_rejected_without_connecting() {
        let error = Postgres::builder(&format!("{}?vacuum_into=/tmp/does-not-matter", *CONNECTION))
            .expect_err("an option that tokio-postgres does not read must be rejected");

        assert!(error.to_string().contains("vacuum_into"), "{error}");
    }

    /// DeleteRecords calls across every request take a quarter of the pool,
    /// and at least one connection, and one request makes at most
    /// [`DELETE_RECORDS_CONCURRENCY`] of them at once. The pool of 16 leaves 12
    /// connections to other work, which four requests that each made four calls
    /// would take.
    #[test]
    fn delete_records_calls_take_a_quarter_of_the_pool() -> Result<()> {
        for (pool_max_size, limit, per_request) in [(16, 4, 4), (32, 8, 4), (8, 2, 2), (2, 1, 1)] {
            let storage =
                Postgres::builder(&format!("{}?pool_max_size={pool_max_size}", *CONNECTION))?
                    .cluster("sized")
                    .node(0)
                    .build();

            let concurrency = storage.delete_records_concurrency();

            assert_eq!(per_request, concurrency.per_request(), "{pool_max_size}");
            assert_eq!(
                limit,
                concurrency
                    .permits()
                    .map(|permits| permits.available_permits())
                    .expect("permits shared across requests"),
                "{pool_max_size}"
            );
        }

        Ok(())
    }

    /// Two requests, which ask the same engine, get the same permits, so the
    /// calls of both together stay within the limit. A clone of the engine, as
    /// each client connection has, shares them too.
    #[test]
    fn delete_records_permits_are_shared_across_requests() -> Result<()> {
        let storage = Postgres::builder(&CONNECTION)?
            .cluster("shared")
            .node(0)
            .build();

        let first = storage.delete_records_concurrency();
        let second = storage.clone().delete_records_concurrency();

        let (first, second) = (
            first.permits().expect("permits"),
            second.permits().expect("permits"),
        );

        assert!(Arc::ptr_eq(first, second));

        // the calls of one request take permits that another request then lacks
        let taken = (0..first.available_permits())
            .map(|_| Arc::clone(first).try_acquire_owned())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| Error::Message(error.to_string()))?;

        assert_eq!(0, second.available_permits());
        assert_eq!(taken.len(), 4);

        Ok(())
    }

    /// Without `pool_max_size`, the pool opens at most 16 connections, and
    /// each kind of read takes half of them.
    #[test]
    fn pool_max_size_defaults_to_sixteen() -> Result<()> {
        let storage = Postgres::builder(&CONNECTION)?
            .cluster("default")
            .node(0)
            .build();

        assert_eq!(16, storage.pool.status().max_size);
        assert_eq!(8, storage.list_offsets_reads.available_permits());
        assert_eq!(8, storage.fetch_reads.available_permits());

        Ok(())
    }

    /// `pool_max_size` sets how many connections the pool opens at most, and
    /// each kind of read takes half of them, rounded down. The other options
    /// in the URL still reach tokio-postgres.
    #[test]
    fn pool_max_size_sets_the_pool_and_its_read_limits() -> Result<()> {
        for (pool_max_size, read_limit) in [(32, 16), (33, 16), (2, 1)] {
            let storage = Postgres::builder(&format!(
                "{}?options=-c%20statement_timeout%3D120000&pool_max_size={pool_max_size}",
                *CONNECTION
            ))?
            .cluster("sized")
            .node(0)
            .build();

            assert_eq!(pool_max_size, storage.pool.status().max_size);
            assert_eq!(read_limit, storage.list_offsets_reads.available_permits());
            assert_eq!(read_limit, storage.fetch_reads.available_permits());
            assert_eq!(
                Duration::from_secs(120) + ABANDONED_STATEMENT_MARGIN,
                storage.abandoned_statement_bound
            );
        }

        Ok(())
    }

    /// `client_read_share` in the storage URL reaches the storage's client
    /// shares. Without the option, a client holds 6 of 8 Fetch reads and 4 of 8
    /// ListOffsets reads.
    #[test]
    fn client_read_share_sets_the_client_shares_of_the_storage() -> Result<()> {
        for (percent, fetch, list_offsets) in [(50, 4, 4), (100, 8, 8)] {
            let storage =
                Postgres::builder(&format!("{}?client_read_share={percent}", *CONNECTION))?
                    .cluster("shared")
                    .node(0)
                    .build();

            assert_eq!(fetch, storage.fetch_reads_per_client, "{percent}%");
            assert_eq!(
                list_offsets, storage.list_offsets_reads_per_client,
                "{percent}%"
            );
        }

        Ok(())
    }

    /// Each connection's share of each kind of read is at least one read and
    /// at most the engine's limit, whatever `pool_max_size` is. A pool too
    /// small for the usual share gives each connection the whole limit, and
    /// a larger pool does not give a connection more than its usual share.
    #[test]
    fn pool_max_size_clamps_each_connection_share_to_its_read_limit() -> Result<()> {
        for (pool_max_size, list_offsets_share, fetch_share) in [
            (2, 1, 1),
            (4, 2, 2),
            (8, LIST_OFFSETS_CONCURRENCY, 2),
            (64, LIST_OFFSETS_CONCURRENCY, 2),
        ] {
            let storage =
                Postgres::builder(&format!("{}?pool_max_size={pool_max_size}", *CONNECTION))?
                    .cluster("shared")
                    .node(0)
                    .build();

            assert_eq!(
                list_offsets_share, storage.list_offsets_reads_per_connection,
                "pool_max_size={pool_max_size}"
            );
            assert_eq!(
                fetch_share, storage.fetch_reads_per_connection,
                "pool_max_size={pool_max_size}"
            );
            assert!(
                (1..=storage.list_offsets_reads.available_permits())
                    .contains(&storage.list_offsets_reads_per_connection),
                "pool_max_size={pool_max_size}"
            );
            assert!(
                (1..=storage.fetch_reads.available_permits())
                    .contains(&storage.fetch_reads_per_connection),
                "pool_max_size={pool_max_size}"
            );
        }

        Ok(())
    }

    /// A `pool_max_size` that is not a number in [`POOL_MAX_SIZE_RANGE`]
    /// stops the build before any connection, and the error names the option
    /// and its value.
    #[test]
    fn pool_max_size_rejects_invalid_values() {
        for value in [
            "banana",
            "",
            "-1",
            "0",
            "1",
            "262144",
            "18446744073709551616",
        ] {
            let error = Postgres::builder(&format!("{}?pool_max_size={value}", *CONNECTION))
                .expect_err(value);

            assert_matches!(
                &error,
                Error::InvalidStorageOptionValue { scheme, option, value: rejected, expected }
                    if scheme == "postgres"
                        && option == POOL_MAX_SIZE
                        && rejected == value
                        && *expected == "a whole number from 2 to 262143",
                "{value}"
            );
        }
    }

    /// An invalid `pool_max_size` stops the build wherever it is in the URL,
    /// even when a later one is valid, as every storage option does.
    #[test]
    fn pool_max_size_rejects_an_invalid_earlier_occurrence() {
        let error = Postgres::builder(&format!(
            "{}?pool_max_size=banana&pool_max_size=4",
            *CONNECTION
        ))
        .expect_err("an invalid pool_max_size must be rejected");

        assert_matches!(
            &error,
            Error::InvalidStorageOptionValue { option, value, .. }
                if option == POOL_MAX_SIZE && value == "banana"
        );
    }

    /// [`take_pool_max_size`] removes every `pool_max_size` from the URL, and
    /// keeps the bytes of every other option: a re-encoded `%20` would reach
    /// tokio-postgres as a `+`. The last `pool_max_size` wins.
    #[test]
    fn take_pool_max_size_removes_only_its_option() -> Result<()> {
        for (config, expected) in [
            ("postgres://h/db?pool_max_size=4", "postgres://h/db"),
            (
                "postgres://h/db?pool_max_size=4&sslmode=require",
                "postgres://h/db?sslmode=require",
            ),
            (
                "postgres://h/db?sslmode=require&pool_max_size=4&options=-c%20search_path%3Dfoo",
                "postgres://h/db?sslmode=require&options=-c%20search_path%3Dfoo",
            ),
            (
                "postgres://h/db?options=-c%20search_path%3Dfoo&pool_max_size=4",
                "postgres://h/db?options=-c%20search_path%3Dfoo",
            ),
            (
                "postgres://h/db?pool_max_size=8&sslmode=require&pool_max_size=4",
                "postgres://h/db?sslmode=require",
            ),
        ] {
            let mut stripped = config.to_owned();
            assert_eq!(4, take_pool_max_size(&mut stripped)?, "{config}");
            assert_eq!(expected, stripped, "{config}");
        }

        Ok(())
    }

    /// A URL without `pool_max_size` keeps every byte, and the pool gets
    /// [`DEFAULT_POOL_MAX_SIZE`].
    #[test]
    fn take_pool_max_size_defaults_without_its_option() -> Result<()> {
        for config in [
            "postgres://h/db",
            "postgres://h/db?sslmode=require&options=-c%20search_path%3Dfoo",
        ] {
            let mut stripped = config.to_owned();
            assert_eq!(
                DEFAULT_POOL_MAX_SIZE,
                take_pool_max_size(&mut stripped)?,
                "{config}"
            );
            assert_eq!(config, stripped);
        }

        Ok(())
    }

    /// Both ends of [`POOL_MAX_SIZE_RANGE`] are accepted.
    #[test]
    fn take_pool_max_size_accepts_its_range() -> Result<()> {
        for size in [*POOL_MAX_SIZE_RANGE.start(), *POOL_MAX_SIZE_RANGE.end()] {
            let mut config = format!("postgres://h/db?pool_max_size={size}");
            assert_eq!(size, take_pool_max_size(&mut config)?);
        }

        Ok(())
    }

    /// Hands out numbered objects, so a test can tell a fresh object from a
    /// recycled one.
    #[derive(Debug, Default)]
    struct Numbered(atomic::AtomicU32);

    impl managed::Manager for Numbered {
        type Type = u32;
        type Error = std::convert::Infallible;

        async fn create(&self) -> std::result::Result<u32, Self::Error> {
            Ok(self.0.fetch_add(1, atomic::Ordering::SeqCst))
        }

        async fn recycle(
            &self,
            _object: &mut u32,
            _metrics: &managed::Metrics,
        ) -> managed::RecycleResult<Self::Error> {
            Ok(())
        }
    }

    /// A released object goes back to the pool. A dropped one reaches
    /// `abandon` still checked out, so the pool cannot hand it out until
    /// `abandon` lets it go.
    #[tokio::test]
    async fn abandoned_object_is_handed_to_abandon_still_checked_out() -> Result<()> {
        let pool = managed::Pool::<Numbered>::builder(Numbered::default())
            .max_size(1)
            .build()
            .map_err(|err| Error::Message(err.to_string()))?;

        let abandoned = Arc::new(std::sync::Mutex::new(Vec::new()));

        let get = async || {
            let abandoned = abandoned.clone();
            pool.get()
                .await
                .map(|object| {
                    Abandonable::new(object, move |object| {
                        abandoned.lock().expect("abandoned").push(object)
                    })
                })
                .map_err(|err| Error::Message(err.to_string()))
        };

        let first = get().await?;
        assert_eq!(0, **first.object()?);
        first.release();
        assert_eq!(1, pool.status().available);

        let second = get().await?;
        assert_eq!(0, **second.object()?);
        drop(second);

        assert_eq!(1, pool.status().size);
        assert_eq!(0, pool.status().available);
        assert!(
            timeout(Duration::from_millis(50), pool.get())
                .await
                .is_err(),
            "the abandoned object is still checked out"
        );

        let held = abandoned
            .lock()
            .expect("abandoned")
            .pop()
            .expect("abandon received the object");
        assert_eq!(0, *held);
        drop(held);

        let third = get().await?;
        assert_eq!(0, **third.object()?);
        third.release();

        Ok(())
    }

    /// Shutting down a `ClosedByPeer` stream finishes only once the peer
    /// has closed its side, and everything the peer sent meanwhile is
    /// discarded.
    #[tokio::test]
    async fn closed_by_peer_shutdown_waits_for_the_peer() -> Result<()> {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let (ours, mut peer) = tokio::io::duplex(64);
        let mut ours = ClosedByPeer::new(ours);

        ours.write_all(b"cancel").await?;
        ours.flush().await?;

        let mut shutdown = tokio::spawn(async move { ours.shutdown().await.map(|()| ours) });

        let mut request = [0; 6];
        _ = peer.read_exact(&mut request).await?;
        assert_eq!(b"cancel", &request);
        peer.write_all(b"busy").await?;

        assert!(
            timeout(Duration::from_millis(100), &mut shutdown)
                .await
                .is_err(),
            "shutdown finished before the peer closed"
        );

        drop(peer);

        let ours = timeout(Duration::from_secs(1), shutdown)
            .await
            .map_err(|_| Error::Message("shutdown did not finish after the peer closed".into()))?
            .map_err(|err| Error::Message(err.to_string()))??;
        assert!(ours.writes_shut);

        Ok(())
    }

    /// Returns a storage whose pool has no permits at all, so each read that
    /// has its read permits waits in the pool, where the pool counts it,
    /// without a server.
    fn storage_without_connections() -> Result<Postgres> {
        let pool = Pool::builder(Manager::new(
            Config::from_str(&CONNECTION)?,
            tokio_postgres::NoTls,
        ))
        .max_size(0)
        .runtime(Runtime::Tokio1)
        .wait_timeout(Some(DEFAULT_POOL_WAIT_TIMEOUT))
        .build()?;

        Ok(Postgres {
            pool: RateLimitedPool::new(pool, Arc::default()),
            ..Postgres::builder(&CONNECTION)?
                .cluster(alphanumeric_string(15).as_str())
                .node(rng().random_range(0..i32::MAX))
                .build()
        })
    }

    /// Waits until at least `waiting` reads wait in `storage`'s pool, gives
    /// other reads time to reach it too, and returns how many wait.
    async fn reads_waiting_in_the_pool(storage: &Postgres, waiting: usize) -> usize {
        let settled = Instant::now() + Duration::from_secs(5);
        while storage.pool.status().waiting < waiting {
            assert!(Instant::now() < settled, "{:?}", storage.pool.status());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        storage.pool.status().waiting
    }

    /// Aborts each of `reads` and waits for it to end.
    async fn abort_reads<T: Debug>(reads: Vec<JoinHandle<T>>) {
        for read in &reads {
            read.abort();
        }
        for read in reads {
            let outcome = read.await;
            assert!(
                outcome.as_ref().is_err_and(|err| err.is_cancelled()),
                "{outcome:?}"
            );
        }
    }

    /// Spawns a ListOffsets read of `topition` on a connection with `share`.
    fn spawn_list_offsets(
        storage: &Postgres,
        topition: &Topition,
        share: ListOffsetsReadShare,
    ) -> JoinHandle<Result<Vec<(Topition, ListOffsetResponse)>>> {
        let storage = storage.clone();
        let offsets = [(topition.clone(), ListOffset::Latest)];

        tokio::spawn(share.scope(async move {
            storage
                .list_offsets(IsolationLevel::ReadUncommitted, &offsets)
                .await
        }))
    }

    /// Spawns a read of `topition`'s offset stage on a connection with
    /// `share`.
    fn spawn_offset_stage(
        storage: &Postgres,
        topition: &Topition,
        share: FetchReadShare,
    ) -> JoinHandle<Result<OffsetStage>> {
        let storage = storage.clone();
        let topition = topition.clone();

        tokio::spawn(share.scope(async move { storage.offset_stage(&topition).await }))
    }

    /// One connection's ListOffsets reads reach the pool only up to its
    /// share, `ReadLimits::list_offsets_per_connection`, and leave the rest of
    /// `ReadLimits::list_offsets` to other connections.
    #[tokio::test]
    async fn one_connection_holds_at_most_its_share_of_list_offsets_reads() -> Result<()> {
        let storage = storage_without_connections()?;
        let topition = Topition::new("shared", 0);

        let connection = ListOffsetsReadShare::default();
        let mut reads = (0..ReadLimits::default().list_offsets)
            .map(|_| spawn_list_offsets(&storage, &topition, connection.clone()))
            .collect::<Vec<_>>();

        assert_eq!(
            ReadLimits::default().list_offsets_per_connection(),
            reads_waiting_in_the_pool(
                &storage,
                ReadLimits::default().list_offsets_per_connection()
            )
            .await
        );

        reads.push(spawn_list_offsets(
            &storage,
            &topition,
            ListOffsetsReadShare::default(),
        ));

        assert_eq!(
            ReadLimits::default().list_offsets_per_connection() + 1,
            reads_waiting_in_the_pool(
                &storage,
                ReadLimits::default().list_offsets_per_connection() + 1
            )
            .await
        );

        abort_reads(reads).await;
        assert_eq!(
            ReadLimits::default().list_offsets,
            storage.list_offsets_reads.available_permits()
        );

        Ok(())
    }

    /// One connection's Fetch reads reach the pool only up to its share,
    /// `ReadLimits::fetch_per_connection`, and leave the rest of `ReadLimits::fetch` to
    /// other connections.
    #[tokio::test]
    async fn one_connection_holds_at_most_its_share_of_fetch_reads() -> Result<()> {
        let storage = storage_without_connections()?;
        let topition = Topition::new("shared", 0);

        let connection = FetchReadShare::default();
        let mut reads = (0..ReadLimits::default().fetch)
            .map(|_| spawn_offset_stage(&storage, &topition, connection.clone()))
            .collect::<Vec<_>>();

        assert_eq!(
            ReadLimits::default().fetch_per_connection(),
            reads_waiting_in_the_pool(&storage, ReadLimits::default().fetch_per_connection()).await
        );

        reads.push(spawn_offset_stage(
            &storage,
            &topition,
            FetchReadShare::default(),
        ));

        assert_eq!(
            ReadLimits::default().fetch_per_connection() + 1,
            reads_waiting_in_the_pool(&storage, ReadLimits::default().fetch_per_connection() + 1)
                .await
        );

        abort_reads(reads).await;
        assert_eq!(
            ReadLimits::default().fetch,
            storage.fetch_reads.available_permits()
        );

        Ok(())
    }

    /// One client's reads, over many connections, may hold the whole of
    /// `ReadLimits::fetch` while no other client waits for it, and the next
    /// read waits for a permit of its own client's share, not in the limit's
    /// queue.
    #[tokio::test]
    async fn a_lone_client_may_hold_the_whole_fetch_limit() -> Result<()> {
        let storage = storage_without_connections()?;
        let topition = Topition::new("shared", 0);
        let limits = ReadLimits::default();
        let client = ClientIdentity::new(Some("alice"), Some("greedy"));

        // Each read has a connection of its own, and no connection's share
        // is used up.
        let reads = (0..limits.fetch + 1)
            .map(|_| {
                let storage = storage.clone();
                let topition = topition.clone();

                tokio::spawn(
                    client.clone().scope(
                        FetchReadShare::default()
                            .scope(async move { storage.offset_stage(&topition).await }),
                    ),
                )
            })
            .collect::<Vec<_>>();

        assert!(limits.fetch_per_client() < limits.fetch);
        assert_eq!(
            limits.fetch,
            reads_waiting_in_the_pool(&storage, limits.fetch).await
        );
        assert_eq!(0, storage.fetch_reads.available_permits());

        abort_reads(reads).await;
        assert_eq!(limits.fetch, storage.fetch_reads.available_permits());

        Ok(())
    }

    /// The same, for ListOffsets reads: `ReadLimits::list_offsets`.
    #[tokio::test]
    async fn a_lone_client_may_hold_the_whole_list_offsets_limit() -> Result<()> {
        let storage = storage_without_connections()?;
        let topition = Topition::new("shared", 0);
        let limits = ReadLimits::default();
        let client = ClientIdentity::new(None, Some("greedy"));

        let reads = (0..limits.list_offsets + 1)
            .map(|_| {
                let storage = storage.clone();
                let offsets = [(topition.clone(), ListOffset::Latest)];

                tokio::spawn(client.clone().scope(ListOffsetsReadShare::default().scope(
                    async move {
                        storage
                            .list_offsets(IsolationLevel::ReadUncommitted, &offsets)
                            .await
                    },
                )))
            })
            .collect::<Vec<_>>();

        assert!(limits.list_offsets_per_client() < limits.list_offsets);
        assert_eq!(
            limits.list_offsets,
            reads_waiting_in_the_pool(&storage, limits.list_offsets).await
        );
        assert_eq!(0, storage.list_offsets_reads.available_permits());

        abort_reads(reads).await;
        assert_eq!(
            limits.list_offsets,
            storage.list_offsets_reads.available_permits()
        );

        Ok(())
    }

    /// A client's share is all of a limit but a connection's share by
    /// default, never less than a connection's share, and `client_read_share`
    /// sets a percentage of the limit, clamped the same way.
    #[test]
    fn client_share_follows_the_limit_and_the_setting() {
        let default = ReadLimits::default();
        assert_eq!(6, default.fetch_per_client());
        assert_eq!(
            default.list_offsets - LIST_OFFSETS_CONCURRENCY,
            default.list_offsets_per_client()
        );

        // A pool too small for a spare connection share gives a client the
        // whole limit.
        assert_eq!(1, ReadLimits::for_pool(2).fetch_per_client());
        assert_eq!(4, ReadLimits::for_pool(8).list_offsets_per_client());

        for (percent, fetch) in [(1, 2), (25, 2), (50, 4), (75, 6), (100, 8)] {
            let limits = ReadLimits {
                client_percent: Some(percent),
                ..ReadLimits::default()
            };
            assert_eq!(fetch, limits.fetch_per_client(), "{percent}%");
            assert!(limits.list_offsets_per_client() >= LIST_OFFSETS_CONCURRENCY);
            assert!(limits.list_offsets_per_client() <= limits.list_offsets);
        }
    }

    /// `client_read_share` is removed from the URL like `pool_max_size`, and
    /// only a percentage is accepted.
    #[test]
    fn client_read_share_is_taken_from_the_url() -> Result<()> {
        let mut config =
            "postgres://localhost/db?options=a&client_read_share=25&pool_max_size=4".to_owned();
        assert_eq!(Some(25), take_client_read_share(&mut config)?);
        assert_eq!("postgres://localhost/db?options=a&pool_max_size=4", config);

        let mut config = "postgres://localhost/db".to_owned();
        assert_eq!(None, take_client_read_share(&mut config)?);

        for value in ["0", "101", "banana", ""] {
            let mut config = format!("postgres://localhost/db?client_read_share={value}");
            assert_matches!(
                take_client_read_share(&mut config),
                Err(Error::InvalidStorageOptionValue { option, .. }) if option == CLIENT_READ_SHARE,
                "{value}"
            );
        }

        Ok(())
    }

    /// A connection's abandoned Fetch reads keep their permits from its
    /// share, as well as from `ReadLimits::fetch`, until their statements end. The
    /// connection's next read waits meanwhile, and another connection's
    /// read goes ahead.
    #[tokio::test]
    async fn abandoned_fetch_reads_count_against_their_connection() -> Result<()> {
        let storage = Postgres::builder(&CONNECTION)?
            .cluster(alphanumeric_string(15).as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        let monitor = match storage.connection().await {
            Ok(monitor) => monitor,
            Err(err) => {
                eprintln!("skipping abandoned_fetch_reads_count_against_their_connection: {err:?}");
                return Ok(());
            }
        };

        let storage = with_cancel_requests_refused(storage).await?;
        let marker = alphanumeric_string(15);
        let connection = FetchReadShare::default();

        for _ in 0..ReadLimits::default().fetch_per_connection() {
            let permits = connection
                .clone()
                .scope(storage.fetch_read_permits())
                .await?;
            abandon_sleeping_statement(&storage, permits, &marker).await?;
        }

        assert_eq!(
            i64::try_from(ReadLimits::default().fetch_per_connection())?,
            backends_running(&monitor, &marker).await?
        );
        assert_eq!(
            ReadLimits::default().fetch - ReadLimits::default().fetch_per_connection(),
            storage.fetch_reads.available_permits()
        );

        assert!(
            timeout(
                Duration::from_millis(200),
                connection.clone().scope(storage.fetch_read_permits())
            )
            .await
            .is_err(),
            "the connection read beyond its share while its abandoned reads ran"
        );

        let other_connection = timeout(
            Duration::from_millis(200),
            FetchReadShare::default().scope(storage.fetch_read_permits()),
        )
        .await
        .map_err(|_| Error::Message("another connection's read waited".into()))??;
        drop(other_connection);

        assert_eq!(
            ReadLimits::default().fetch_per_connection(),
            cancel_backends(&monitor, &marker).await?
        );

        wait_until(&storage, |storage| {
            storage.fetch_reads.available_permits() == ReadLimits::default().fetch
        })
        .await;

        _ = timeout(
            Duration::from_secs(1),
            connection.scope(storage.fetch_read_permits()),
        )
        .await
        .map_err(|_| Error::Message("the connection's share did not come back".into()))??;

        Ok(())
    }

    /// A client's abandoned Fetch reads keep their permits, from the client's
    /// share and from `ReadLimits::fetch`, until their statements end, and a
    /// client past its share waits for it, behind another client that waits
    /// for the limit. This is the reconnect that the client share exists for:
    /// a client that is alone may use the whole limit, but once another
    /// client waits, a permit that frees goes to that client and not to the
    /// next read of the one that abandons its reads.
    #[tokio::test]
    async fn abandoned_fetch_reads_of_a_client_give_way_to_a_waiting_client() -> Result<()> {
        let storage = Postgres::builder(&CONNECTION)?
            .cluster(alphanumeric_string(15).as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        let monitor = match storage.connection().await {
            Ok(monitor) => monitor,
            Err(err) => {
                eprintln!(
                    "skipping abandoned_fetch_reads_of_a_client_give_way_to_a_waiting_client: {err:?}"
                );
                return Ok(());
            }
        };

        let storage = with_cancel_requests_refused(storage).await?;
        let limits = ReadLimits::default();
        let client = ClientIdentity::new(Some("alice"), Some("reconnecting"));
        let share_marker = alphanumeric_string(15);
        let first_extra_marker = alphanumeric_string(15);
        let second_extra_marker = alphanumeric_string(15);

        // Each read comes from a new connection, so that no connection's share
        // is used up, only the client's. The reads past the client's share take
        // the rest of the limit, because nobody waits for it.
        let abandoned = [
            (limits.fetch_per_client(), &share_marker),
            (1, &first_extra_marker),
            (
                limits.fetch - limits.fetch_per_client() - 1,
                &second_extra_marker,
            ),
        ];
        for (reads, marker) in abandoned {
            for _ in 0..reads {
                let permits = client
                    .clone()
                    .scope(FetchReadShare::default().scope(storage.fetch_read_permits()))
                    .await?;
                abandon_sleeping_statement(&storage, permits, marker).await?;
            }
        }

        assert_eq!(0, storage.fetch_reads.available_permits());

        // The client's next read waits for its share, and then another
        // client's read waits for the limit.
        let next = tokio::spawn({
            let storage = storage.clone();

            client.clone().scope(
                FetchReadShare::default()
                    .scope(async move { storage.fetch_read_permits().await.map(drop) }),
            )
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!next.is_finished(), "the read went ahead of a full limit");

        let other = tokio::spawn({
            let storage = storage.clone();

            ClientIdentity::new(Some("bob"), Some("polite")).scope(
                FetchReadShare::default().scope(async move { storage.fetch_read_permits().await }),
            )
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!other.is_finished(), "the read went ahead of a full limit");

        // One permit frees. It goes to the client that waits for the limit, and
        // the read of the client past its share does not take it.
        assert_eq!(1, cancel_backends(&monitor, &first_extra_marker).await?);
        // It keeps the permit, so that the limit stays used up.
        let other_permits = timeout(Duration::from_secs(5), other)
            .await
            .map_err(|_| Error::Message("the waiting client did not get the permit".into()))?
            .map_err(|err| Error::Message(err.to_string()))??;
        assert!(
            !next.is_finished(),
            "the read of the client past its share took the permit that freed"
        );

        // The rest frees, and so the reads of the client go ahead again.
        assert_eq!(
            limits.fetch_per_client(),
            cancel_backends(&monitor, &share_marker).await?
        );
        assert_eq!(
            limits.fetch - limits.fetch_per_client() - 1,
            cancel_backends(&monitor, &second_extra_marker).await?
        );
        timeout(Duration::from_secs(5), next)
            .await
            .map_err(|_| Error::Message("the client's read did not go ahead".into()))?
            .map_err(|err| Error::Message(err.to_string()))??;

        drop(other_permits);
        wait_until(&storage, |storage| {
            storage.fetch_reads.available_permits() == limits.fetch
        })
        .await;

        Ok(())
    }

    /// A Fetch that reaches its deadline while it waits for its client's share answers with no
    /// records, and reports that wait, so that the Fetch service can name the client.
    #[tokio::test]
    async fn fetch_timing_out_at_the_client_share_reports_that_wait() -> Result<()> {
        let storage = Postgres::builder(&CONNECTION)?
            .cluster(alphanumeric_string(15).as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        let monitor = match storage.connection().await {
            Ok(monitor) => monitor,
            Err(err) => {
                eprintln!(
                    "skipping fetch_timing_out_at_the_client_share_reports_that_wait: \
                     {err:?}"
                );
                return Ok(());
            }
        };

        let storage = with_cancel_requests_refused(storage).await?;
        let limits = ReadLimits::default();
        let client = ClientIdentity::new(Some("alice"), Some("reporting"));
        let marker = alphanumeric_string(15);

        // The client abandons as many reads as the limit allows, each from a new connection, so
        // that its share and the limit are used up.
        for _ in 0..limits.fetch {
            let permits = client
                .clone()
                .scope(FetchReadShare::default().scope(storage.fetch_read_permits()))
                .await?;
            abandon_sleeping_statement(&storage, permits, &marker).await?;
        }
        assert_eq!(0, storage.fetch_reads.available_permits());

        let read = client
            .scope(
                FetchReadShare::default().scope(storage.fetch_until_deadline(
                    &Topition::new("orders", 0),
                    0,
                    1,
                    1024,
                    IsolationLevel::ReadUncommitted,
                    Instant::now() + Duration::from_millis(300),
                )),
            )
            .await?;

        assert!(read.batches.is_empty(), "{read:?}");
        assert_eq!(
            Some(TimedOut::Queued(ReadWait::ClientReadLimit)),
            read.timed_out
        );

        _ = cancel_backends(&monitor, &marker).await?;

        Ok(())
    }

    /// A Fetch holds its read permits while it waits for its caller to take its records, and
    /// returns them when the caller drops the read.
    #[tokio::test]
    async fn fetch_dropped_while_it_waits_to_send_returns_its_permits() -> Result<()> {
        const RECORDS: i32 = 200;

        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "fetch_dropped_while_it_waits_to_send_returns_its_permits",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name, 0);

        let batch = (0..RECORDS)
            .fold(Batch::builder(), |batch, offset_delta| {
                batch.record(
                    Record::builder()
                        .value(Bytes::from_static(b"v").into())
                        .offset_delta(offset_delta),
                )
            })
            .last_offset_delta(RECORDS - 1)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage.produce(None, &topition, batch).await?;

        // The test keeps the receiving end and never reads it. The read has more records than the
        // channel holds, so it fills the channel and then waits for room that never comes.
        let (fetched_sender, _fetched_receiver) = fetched::channel();

        let mut read = storage.fetch(
            &topition,
            0,
            1,
            1024 * 1024,
            IsolationLevel::ReadUncommitted,
            fetched_sender,
        );

        assert!(
            timeout(Duration::from_secs(2), &mut read).await.is_err(),
            "the read ended without waiting to send"
        );

        assert_eq!(
            ReadLimits::default().fetch - 1,
            storage.fetch_reads.available_permits()
        );

        drop(read);

        // A dropped read cancels its statement before it returns its permits, which takes round
        // trips to the server.
        let give_up = Instant::now() + Duration::from_secs(10);
        while storage.fetch_reads.available_permits() < ReadLimits::default().fetch {
            assert!(
                Instant::now() < give_up,
                "the dropped read kept its permits"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        Ok(())
    }

    /// At most `ReadLimits::list_offsets` ListOffsets reads reach the pool at
    /// once; the rest wait for a permit, and the permits come back when the
    /// reads are dropped.
    ///
    /// The pool has no permits at all, so each read that has a ListOffsets
    /// permit waits in the pool, where the pool counts it, without a server.
    #[tokio::test]
    async fn list_offsets_reads_are_limited_before_the_pool() -> Result<()> {
        let storage = storage_without_connections()?;

        let topition = Topition::new("limited", 0);
        let reads = (0..ReadLimits::default().list_offsets + 3)
            .map(|_| {
                let storage = storage.clone();
                let topition = topition.clone();

                tokio::spawn(async move {
                    storage
                        .list_offsets(
                            IsolationLevel::ReadUncommitted,
                            &[(topition, ListOffset::Latest)],
                        )
                        .await
                })
            })
            .collect::<Vec<_>>();

        let settled = Instant::now() + Duration::from_secs(5);
        while storage.pool.status().waiting < ReadLimits::default().list_offsets {
            assert!(Instant::now() < settled, "{:?}", storage.pool.status());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(
            ReadLimits::default().list_offsets,
            storage.pool.status().waiting
        );
        assert_eq!(0, storage.list_offsets_reads.available_permits());

        for read in &reads {
            read.abort();
        }
        for read in reads {
            let outcome = read.await;
            assert!(
                outcome.as_ref().is_err_and(|err| err.is_cancelled()),
                "{outcome:?}"
            );
        }

        assert_eq!(0, storage.pool.status().waiting);
        assert_eq!(
            ReadLimits::default().list_offsets,
            storage.list_offsets_reads.available_permits()
        );

        Ok(())
    }

    /// Dropping an abandonable connection mid-statement stops the statement
    /// on the server and returns the connection to the pool, with its
    /// permit.
    #[tokio::test]
    async fn abandoned_statement_is_cancelled_and_its_connection_returned() -> Result<()> {
        let storage = Postgres::builder(&CONNECTION)?
            .cluster(alphanumeric_string(15).as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        let monitor = match storage.connection().await {
            Ok(monitor) => monitor,
            Err(err) => {
                eprintln!(
                    "skipping abandoned_statement_is_cancelled_and_its_connection_returned: {err:?}"
                );
                return Ok(());
            }
        };

        let marker = alphanumeric_string(15);
        let sleep = format!("SELECT pg_sleep(30) /* {marker} */");

        let permits = storage.list_offsets_read_permits().await?;
        let connection = storage
            .abandonable_connection(permits, StorageOperation::Fetch)
            .await?;
        assert_eq!(
            ReadLimits::default().list_offsets - 1,
            storage.list_offsets_reads.available_permits()
        );

        assert!(
            timeout(
                Duration::from_millis(200),
                connection.object()?.batch_execute(&sleep)
            )
            .await
            .is_err(),
            "pg_sleep(30) is still running"
        );
        drop(connection);

        // The cancel task holds the permit, so the statement still counts
        // against the limit while it runs.
        assert_eq!(
            ReadLimits::default().list_offsets - 1,
            storage.list_offsets_reads.available_permits()
        );

        let settled = Instant::now() + Duration::from_secs(10);
        loop {
            let running = monitor
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity WHERE state <> 'idle' AND query LIKE $1",
                    &[&format!("%{marker}%")],
                )
                .await?
                .get::<_, i64>(0);
            let status = storage.pool.status();

            if running == 0 && status.available == 1 {
                break;
            }

            assert!(
                Instant::now() < settled,
                "statement still running on {running} backend(s), pool {status:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert_eq!(2, storage.pool.status().size);
        assert_eq!(
            ReadLimits::default().list_offsets,
            storage.list_offsets_reads.available_permits()
        );

        Ok(())
    }

    /// How many backends run a statement marked `marker`, other than
    /// `monitor`'s own.
    async fn backends_running(monitor: &Object, marker: &str) -> Result<i64> {
        monitor
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE state <> 'idle' AND query LIKE $1 AND pid <> pg_backend_pid()",
                &[&format!("%{marker}%")],
            )
            .await
            .map(|row| row.get::<_, i64>(0))
            .map_err(Into::into)
    }

    /// Ends every statement marked `marker` through the server, as the
    /// cancel request would have, and returns how many it ended.
    async fn cancel_backends(monitor: &Object, marker: &str) -> Result<usize> {
        monitor
            .query(
                "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE state <> 'idle' AND query LIKE $1 AND pid <> pg_backend_pid()",
                &[&format!("%{marker}%")],
            )
            .await
            .map(|rows| rows.len())
            .map_err(Into::into)
    }

    /// Points `storage`'s cancel requests at a port nothing listens on, so
    /// each one fails without reaching the server, as one sent to a host
    /// other than the one running the statement would.
    async fn with_cancel_requests_refused(storage: Postgres) -> Result<Postgres> {
        let closed_port = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await?
            .local_addr()?
            .port();

        let tls = storage
            .cancel
            .as_ref()
            .expect("cancel requests are configured from a URL")
            .tls
            .clone();

        let mut config = Config::new();
        _ = config.host("127.0.0.1").port(closed_port);

        Ok(Postgres {
            cancel: Some(CancelRequests { config, tls }),
            ..storage
        })
    }

    /// Runs `SELECT pg_sleep(30)` marked `marker` on an abandonable
    /// connection holding `permits` and drops the connection mid-statement,
    /// returning once the cancel task has had time to send its cancel
    /// request.
    async fn abandon_sleeping_statement(
        storage: &Postgres,
        permits: ReadPermits,
        marker: &str,
    ) -> Result<()> {
        let sleep = format!("SELECT pg_sleep(30) /* {marker} */");

        let connection = storage
            .abandonable_connection(permits, StorageOperation::Fetch)
            .await?;

        assert!(
            timeout(
                Duration::from_millis(200),
                connection.object()?.batch_execute(&sleep)
            )
            .await
            .is_err(),
            "pg_sleep(30) is still running"
        );
        drop(connection);

        tokio::time::sleep(Duration::from_millis(500)).await;

        Ok(())
    }

    /// Waits until `settled` holds, failing after 10 seconds.
    async fn wait_until(storage: &Postgres, settled: impl Fn(&Postgres) -> bool) {
        let give_up = Instant::now() + Duration::from_secs(10);

        while !settled(storage) {
            assert!(
                Instant::now() < give_up,
                "pool {:?}, ListOffsets permits {}, Fetch permits {}",
                storage.pool.status(),
                storage.list_offsets_reads.available_permits(),
                storage.fetch_reads.available_permits()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// A cancel request that does not reach the server leaves the
    /// statement running, and the connection and its permit stay held
    /// until the statement ends; then both come back.
    #[tokio::test]
    async fn abandoned_statement_outliving_its_cancel_keeps_its_connection_and_permit() -> Result<()>
    {
        let storage = Postgres::builder(&CONNECTION)?
            .cluster(alphanumeric_string(15).as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        let monitor = match storage.connection().await {
            Ok(monitor) => monitor,
            Err(err) => {
                eprintln!(
                    "skipping abandoned_statement_outliving_its_cancel_keeps_its_connection_and_permit: {err:?}"
                );
                return Ok(());
            }
        };

        let storage = with_cancel_requests_refused(storage).await?;
        let marker = alphanumeric_string(15);

        let permits = storage.list_offsets_read_permits().await?;
        abandon_sleeping_statement(&storage, permits, &marker).await?;

        assert_eq!(1, backends_running(&monitor, &marker).await?);
        assert_eq!(
            ReadLimits::default().list_offsets - 1,
            storage.list_offsets_reads.available_permits()
        );
        assert_eq!(0, storage.pool.status().available);
        assert_eq!(2, storage.pool.status().size);

        assert_eq!(1, cancel_backends(&monitor, &marker).await?);

        wait_until(&storage, |storage| storage.pool.status().available == 1).await;

        assert_eq!(0, backends_running(&monitor, &marker).await?);
        assert_eq!(2, storage.pool.status().size);
        assert_eq!(
            ReadLimits::default().list_offsets,
            storage.list_offsets_reads.available_permits()
        );

        Ok(())
    }

    /// A statement still running at the bound has its connection closed
    /// and taken out of the pool, and its permit comes back; the backend
    /// runs on until the statement ends.
    #[tokio::test]
    async fn abandoned_statement_still_running_at_the_bound_loses_its_connection() -> Result<()> {
        let storage = Postgres::builder(&CONNECTION)?
            .cluster(alphanumeric_string(15).as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        let monitor = match storage.connection().await {
            Ok(monitor) => monitor,
            Err(err) => {
                eprintln!(
                    "skipping abandoned_statement_still_running_at_the_bound_loses_its_connection: {err:?}"
                );
                return Ok(());
            }
        };

        let storage = Postgres {
            abandoned_statement_bound: Duration::from_secs(2),
            ..with_cancel_requests_refused(storage).await?
        };
        let marker = alphanumeric_string(15);

        let permits = storage.list_offsets_read_permits().await?;
        abandon_sleeping_statement(&storage, permits, &marker).await?;

        assert_eq!(
            ReadLimits::default().list_offsets - 1,
            storage.list_offsets_reads.available_permits()
        );
        assert_eq!(2, storage.pool.status().size);

        wait_until(&storage, |storage| {
            storage.list_offsets_reads.available_permits() == ReadLimits::default().list_offsets
        })
        .await;

        assert_eq!(1, storage.pool.status().size);
        assert_eq!(0, storage.pool.status().available);
        assert_eq!(1, backends_running(&monitor, &marker).await?);

        assert_eq!(1, cancel_backends(&monitor, &marker).await?);

        Ok(())
    }

    /// A connection that fails while its abandoned statement runs is taken
    /// out of the pool, and its permit comes back.
    #[tokio::test]
    async fn abandoned_statement_whose_connection_fails_loses_it() -> Result<()> {
        let storage = Postgres::builder(&CONNECTION)?
            .cluster(alphanumeric_string(15).as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();

        let monitor = match storage.connection().await {
            Ok(monitor) => monitor,
            Err(err) => {
                eprintln!("skipping abandoned_statement_whose_connection_fails_loses_it: {err:?}");
                return Ok(());
            }
        };

        let storage = with_cancel_requests_refused(storage).await?;
        let marker = alphanumeric_string(15);

        let permits = storage.list_offsets_read_permits().await?;
        abandon_sleeping_statement(&storage, permits, &marker).await?;

        assert_eq!(
            ReadLimits::default().list_offsets - 1,
            storage.list_offsets_reads.available_permits()
        );

        let terminated = monitor
            .query(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state <> 'idle' AND query LIKE $1 AND pid <> pg_backend_pid()",
                &[&format!("%{marker}%")],
            )
            .await?
            .len();
        assert_eq!(1, terminated);

        wait_until(&storage, |storage| {
            storage.list_offsets_reads.available_permits() == ReadLimits::default().list_offsets
        })
        .await;

        assert_eq!(1, storage.pool.status().size);
        assert_eq!(0, storage.pool.status().available);
        assert_eq!(0, backends_running(&monitor, &marker).await?);

        Ok(())
    }

    /// At most `ReadLimits::fetch` Fetch reads, of records and of offset stages
    /// alike, reach the pool at once; the rest wait for a permit, and the
    /// permits come back when the reads are dropped.
    ///
    /// The pool has no permits at all, so each read that has a Fetch
    /// permit waits in the pool, where the pool counts it, without a server.
    #[tokio::test]
    async fn fetch_reads_are_limited_before_the_pool() -> Result<()> {
        let storage = storage_without_connections()?;

        let topition = Topition::new("limited", 0);
        let reads = (0..ReadLimits::default().fetch + 3)
            .map(|read| {
                let storage = storage.clone();
                let topition = topition.clone();

                tokio::spawn(async move {
                    if read % 2 == 0 {
                        storage
                            .fetch_until_deadline(
                                &topition,
                                0,
                                1,
                                8 * 1024,
                                IsolationLevel::ReadUncommitted,
                                Instant::now() + Duration::from_secs(5),
                            )
                            .await
                            .map(|read| read.batches)
                            .map(drop)
                    } else {
                        storage.offset_stage(&topition).await.map(drop)
                    }
                })
            })
            .collect::<Vec<_>>();

        let settled = Instant::now() + Duration::from_secs(5);
        while storage.pool.status().waiting < ReadLimits::default().fetch {
            assert!(Instant::now() < settled, "{:?}", storage.pool.status());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(ReadLimits::default().fetch, storage.pool.status().waiting);
        assert_eq!(0, storage.fetch_reads.available_permits());
        assert_eq!(
            ReadLimits::default().list_offsets,
            storage.list_offsets_reads.available_permits()
        );

        for read in &reads {
            read.abort();
        }
        for read in reads {
            let outcome = read.await;
            assert!(
                outcome.as_ref().is_err_and(|err| err.is_cancelled()),
                "{outcome:?}"
            );
        }

        assert_eq!(0, storage.pool.status().waiting);
        assert_eq!(
            ReadLimits::default().fetch,
            storage.fetch_reads.available_permits()
        );

        Ok(())
    }

    /// An abandoned read that leaves its connection inside a transaction
    /// block, open or failed, loses that connection, so the pool never
    /// hands it out. The permit still comes back.
    ///
    /// The read opens its block with a plain `BEGIN`, so dropping it queues
    /// no `ROLLBACK`, as when the cancel request ends the `ROLLBACK` that
    /// tokio-postgres queues for a dropped `Transaction`.
    #[tokio::test]
    async fn abandoned_read_left_inside_a_transaction_loses_its_connection() -> Result<()> {
        let storage = Postgres::builder(&CONNECTION)?
            .cluster(alphanumeric_string(15).as_str())
            .node(rng().random_range(0..i32::MAX))
            .build();
        storage.pool.resize(1);

        if let Err(err) = storage.connection().await {
            eprintln!(
                "skipping abandoned_read_left_inside_a_transaction_loses_its_connection: {err:?}"
            );
            return Ok(());
        }

        let marker = alphanumeric_string(15);
        let open = "BEGIN; SET LOCAL enable_seqscan = off".to_owned();
        let failed = format!("BEGIN; SELECT pg_sleep(30) /* {marker} */");

        for read in [open, failed] {
            let permits = storage.fetch_read_permits().await?;
            let connection = storage
                .abandonable_connection(permits, StorageOperation::Fetch)
                .await?;
            let backend = connection
                .object()?
                .query_one("SELECT pg_backend_pid()", &[])
                .await?
                .get::<_, i32>(0);

            // The open block finishes at once, and the cancel request then
            // finds no statement to end. The sleep is still running at the
            // timeout, and the cancel request ends it, which fails the block.
            _ = timeout(
                Duration::from_millis(200),
                connection.object()?.batch_execute(&read),
            )
            .await;
            drop(connection);

            wait_until(&storage, |storage| {
                storage.pool.status().size == 0
                    && storage.fetch_reads.available_permits() == ReadLimits::default().fetch
            })
            .await;

            let c = storage.connection().await?;
            assert_ne!(
                backend,
                c.query_one("SELECT pg_backend_pid()", &[])
                    .await?
                    .get::<_, i32>(0),
                "the pool handed out the connection left by {read:?}"
            );
            assert_eq!(
                "on",
                c.query_one("SELECT current_setting('enable_seqscan')", &[])
                    .await?
                    .get::<_, String>(0)
            );
        }

        Ok(())
    }

    /// Registers a broker for `cluster`, creates a topic of one partition,
    /// and produces one record with a header to it.
    async fn topic_with_one_record(storage: &Postgres, cluster: &str) -> Result<Topition> {
        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.into(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name, 0);

        let batch = Batch::builder()
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"a").into())
                    .header(
                        Header::builder()
                            .key(Bytes::from_static(b"k"))
                            .value(Bytes::from_static(b"v")),
                    )
                    .offset_delta(0),
            )
            .last_offset_delta(0)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage.produce(None, &topition, batch).await?;

        Ok(topition)
    }

    /// Reads `topition` from offset 0 with a 5 second budget.
    async fn fetch_from_start(storage: &Postgres, topition: &Topition) -> Result<()> {
        storage
            .fetch_until_deadline(
                topition,
                0,
                1,
                8 * 1024,
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_secs(5),
            )
            .await
            .map(|read| read.batches)
            .map(drop)
    }

    /// More fetches than `ReadLimits::fetch` at once all finish: each fetch reads
    /// its watermark on the connection it already holds, so no fetch waits
    /// for a second permit while it holds one.
    #[tokio::test]
    async fn fetches_beyond_the_limit_all_finish() -> Result<()> {
        let cluster = alphanumeric_string(15);
        let Some(storage) =
            storage_with_schema("fetches_beyond_the_limit_all_finish", &CONNECTION, &cluster)
                .await?
        else {
            return Ok(());
        };

        let topition = topic_with_one_record(&storage, &cluster).await?;

        let fetches = (0..ReadLimits::default().fetch + 2)
            .map(|_| {
                let storage = storage.clone();
                let topition = topition.clone();
                tokio::spawn(async move { fetch_from_start(&storage, &topition).await })
            })
            .collect::<Vec<_>>();

        for fetch in fetches {
            timeout(Duration::from_secs(10), fetch)
                .await
                .map_err(|_| Error::Message("a fetch did not finish".into()))?
                .map_err(|err| Error::Message(err.to_string()))??;
        }

        assert_eq!(
            ReadLimits::default().fetch,
            storage.fetch_reads.available_permits()
        );

        Ok(())
    }

    /// Runs `read` while `monitor` holds an exclusive lock on `table`, and
    /// aborts `read` once its backend waits for that lock in a statement
    /// that contains `statement`. While the lock is still held, checks that
    /// the backend ends up idle after [`SETTLE_QUERY`], with its connection
    /// back in `storage`'s pool and its Fetch permit returned. Returns the
    /// backend's process id.
    ///
    /// `storage` must have a pool of one connection, and connect with
    /// `application_name`. Other tests that use `table` wait for the lock
    /// too, for about a second.
    async fn abandon_read_waiting_for_a_lock(
        storage: &Postgres,
        monitor: &Postgres,
        application_name: &str,
        table: &str,
        statement: &str,
        read: impl Future<Output = Result<()>> + Send + 'static,
    ) -> Result<i32> {
        let mut locking = monitor.connection().await?;

        // A transaction reads one snapshot of `pg_stat_activity`, so the
        // test watches the read from outside the locking transaction.
        let watching = monitor.connection().await?;

        // The test releases the lock before it returns. The timeouts bound
        // how long the lock holds up other tests when this test stops early.
        let lock = locking.transaction().await?;
        lock.batch_execute(&format!(
            "SET LOCAL lock_timeout = '5s'; SET LOCAL idle_in_transaction_session_timeout = '10s'; LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE"
        ))
        .await?;

        let reading = tokio::spawn(read);

        let give_up = Instant::now() + Duration::from_secs(10);
        let backend = loop {
            if let Some(row) = watching
                .query_opt(
                    "SELECT pid FROM pg_stat_activity WHERE application_name = $1 AND wait_event_type = 'Lock' AND query LIKE $2",
                    &[&application_name, &format!("%{statement}%")],
                )
                .await?
            {
                break row.get::<_, i32>(0);
            }

            assert!(
                Instant::now() < give_up,
                "the read did not wait for the lock on {table}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };

        reading.abort();
        let outcome = reading.await;
        assert!(
            outcome.as_ref().is_err_and(|err| err.is_cancelled()),
            "{outcome:?}"
        );

        let give_up = Instant::now() + Duration::from_secs(10);
        let last_query = loop {
            let activity = watching
                .query_one(
                    "SELECT state, query FROM pg_stat_activity WHERE pid = $1",
                    &[&backend],
                )
                .await?;
            let state = activity.get::<_, Option<String>>(0);
            let status = storage.pool.status();

            if state.as_deref() == Some("idle") && status.available == 1 {
                break activity.get::<_, Option<String>>(1);
            }

            assert!(
                Instant::now() < give_up,
                "backend {backend} is {state:?} while the lock on {table} is held, pool {status:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        };

        assert_eq!(Some(SETTLE_QUERY), last_query.as_deref());
        assert_eq!(
            ReadLimits::default().fetch,
            storage.fetch_reads.available_permits()
        );

        lock.rollback().await?;

        Ok(backend)
    }

    /// A pooled connection that turns read-only, as one to a demoted primary does, is dropped
    /// at its next checkout and replaced, while a connection that stays writable is kept.
    #[tokio::test]
    async fn read_only_pooled_connection_is_replaced() -> Result<()> {
        let cluster = alphanumeric_string(15);
        let storage = Postgres::builder(&format!("{}?read_only_check_every=1", *CONNECTION))?
            .cluster(&cluster)
            .node(rng().random_range(0..i32::MAX))
            .build();
        storage.pool.resize(1);

        let Some(_probe) = storage_with_schema(
            "read_only_pooled_connection_is_replaced",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        async fn pid_and_read_only(storage: &Postgres) -> Result<(i32, String)> {
            let connection = storage.connection().await?;
            let row = connection
                .query_one(
                    "SELECT pg_backend_pid(), current_setting('transaction_read_only')",
                    &[],
                )
                .await?;
            Ok((row.get(0), row.get(1)))
        }

        let (pid, read_only) = pid_and_read_only(&storage).await?;
        assert_eq!("off", read_only);

        // The control: a writable connection is reused.
        assert_eq!(
            (pid, String::from("off")),
            pid_and_read_only(&storage).await?
        );

        storage
            .connection()
            .await?
            .batch_execute("SET default_transaction_read_only = on")
            .await?;

        let (replacement, read_only) = pid_and_read_only(&storage).await?;
        assert_ne!(pid, replacement);
        assert_eq!("off", read_only);
        assert_eq!(1, storage.pool.status().size);

        Ok(())
    }

    /// The URL of a server that every session of is read-only, as the old primary
    /// of a failover is, with `suffix` after it.
    fn read_only_server(suffix: &str) -> String {
        format!(
            "{}?options=-c%20default_transaction_read_only%3Don{suffix}",
            *CONNECTION
        )
    }

    /// While the endpoint still reaches a read-only server, each checkout fails
    /// fast with a transient pool error and the pool does not grow, instead of
    /// each checkout using a new connection once and dropping it at the next.
    #[tokio::test]
    async fn read_only_server_fails_each_checkout_fast_without_growing_the_pool() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(_probe) = storage_with_schema(
            "read_only_server_fails_each_checkout_fast",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        let storage = Postgres::builder(&read_only_server(""))?
            .cluster(&cluster)
            .node(rng().random_range(0..i32::MAX))
            .build();

        for _ in 0..3 {
            let started = Instant::now();
            let error = storage
                .connection()
                .await
                .expect_err("a connection to a read-only server must not be handed out");

            assert!(started.elapsed() < Duration::from_secs(5), "{error:?}");
            assert_matches!(&error, Error::Pool(_));
            assert_eq!(0, storage.pool.status().size);
        }

        Ok(())
    }

    /// While new connections reach a read-only server, the pool starts one attempt to open a
    /// connection per [`RATE_LIMITED_CONNECT_INTERVAL`], and each other checkout fails without one.
    #[tokio::test]
    async fn read_only_server_gets_one_connection_attempt_per_interval() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(_probe) = storage_with_schema(
            "read_only_server_gets_one_connection_attempt",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        let read_only = Postgres::builder(&read_only_server(""))?;
        let pool = PoolParts::open(
            read_only.pool.config.clone(),
            read_only.pool.tls.clone(),
            2,
            &ReadOnlyCheck::new(1, &cluster),
        )?;

        // Every session on this server is read-only, so each connection that the pool opens
        // fails its check, and no checkout succeeds. A failed `post_create` hook shows that the
        // checkout opened a connection.
        assert_matches!(
            pool.get().await,
            Err(CheckoutError::Pool(PoolError::PostCreateHook(_)))
        );

        for _ in 0..3 {
            assert_matches!(pool.get().await, Err(CheckoutError::RateLimited));
        }

        tokio::time::sleep(RATE_LIMITED_CONNECT_INTERVAL).await;

        assert_matches!(
            pool.get().await,
            Err(CheckoutError::Pool(PoolError::PostCreateHook(_)))
        );
        assert_matches!(pool.get().await, Err(CheckoutError::RateLimited));

        Ok(())
    }

    /// Returns a pool of connections to `CONNECTION`, limited by `rate_limit`, with `post_create`
    /// as its hook if it has one.
    fn writable_rate_limited_pool(
        rate_limit: &Arc<ConnectRateLimit>,
        post_create: Option<Hook>,
    ) -> Result<RateLimitedPool> {
        let writable = Postgres::builder(&CONNECTION)?;
        let builder = Pool::builder(Manager::from_config(
            writable.pool.config.clone(),
            writable.pool.tls.clone(),
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        ))
        .max_size(2)
        .runtime(Runtime::Tokio1);

        let pool = match post_create {
            Some(hook) => builder.post_create(hook).build()?,
            None => builder.build()?,
        };

        Ok(RateLimitedPool::new(pool, rate_limit.clone()))
    }

    /// A rate-limited pool hands out an idle connection, because a read-only server still serves
    /// reads, and fails a checkout only when it would open a connection.
    #[tokio::test]
    async fn rate_limited_pool_hands_out_an_idle_connection() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(_probe) = storage_with_schema(
            "rate_limited_pool_hands_out_an_idle_connection",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        let rate_limit = Arc::new(ConnectRateLimit::default());
        let pool = writable_rate_limited_pool(&rate_limit, None)?;

        // The checkout opens a connection, and dropping it returns the connection to the pool,
        // where it stays open and idle.
        drop(pool.get().await?);
        assert_eq!(1, pool.status().available);

        // The `post_create` hook does this when a new connection finds a read-only server.
        assert!(rate_limit.start_or_restart());

        let idle = pool.get().await?;
        assert_matches!(pool.get().await, Err(CheckoutError::RateLimited));
        assert_eq!(1, pool.status().size);

        drop(idle);

        Ok(())
    }

    /// A checkout of a rate-limited pool that is full waits for a connection to come back,
    /// because the pool would not open a connection for it.
    #[tokio::test]
    async fn rate_limited_full_pool_waits_for_a_connection() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(_probe) = storage_with_schema(
            "rate_limited_full_pool_waits_for_a_connection",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        let rate_limit = Arc::new(ConnectRateLimit::default());
        let pool = writable_rate_limited_pool(&rate_limit, None)?;

        let first = pool.get().await?;
        let second = pool.get().await?;
        assert_eq!(0, pool.status().available);

        // The `post_create` hook does this when a new connection finds a read-only server.
        assert!(rate_limit.start_or_restart());

        let waiting = tokio::spawn({
            let pool = pool.clone();
            async move { pool.get().await.map(drop) }
        });

        // The checkout waits in the pool's queue, instead of failing as rate limited.
        let settled = Instant::now() + Duration::from_secs(5);
        while pool.status().waiting == 0 {
            assert!(Instant::now() < settled, "{:?}", pool.status());
            assert!(!waiting.is_finished());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        drop(first);

        assert_matches!(waiting.await, Ok(Ok(())));
        assert_eq!(2, pool.status().size);

        drop(second);

        Ok(())
    }

    /// A new connection that passes its check stops the rate limit, so the pool opens connections
    /// without a limit again.
    #[tokio::test]
    async fn new_connection_that_passes_its_check_stops_the_rate_limit() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(_probe) = storage_with_schema(
            "new_connection_that_passes_its_check_stops_the_rate_limit",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        let rate_limit = Arc::new(ConnectRateLimit::default());
        let check = ReadOnlyCheck::new(1, &cluster);
        let pool =
            writable_rate_limited_pool(&rate_limit, Some(check.post_create(rate_limit.clone())))?;

        // The `post_create` hook does this when a new connection finds a read-only server.
        assert!(rate_limit.start_or_restart());

        // The limit refuses a checkout before the interval ends, so the checkout after it shows
        // that the connection which passes its check stops the limit.
        assert_matches!(pool.get().await, Err(CheckoutError::RateLimited));

        tokio::time::sleep(RATE_LIMITED_CONNECT_INTERVAL).await;

        let first = pool.get().await?;
        assert!(!rate_limit.is_limiting());

        let second = pool.get().await?;
        assert_eq!(2, pool.status().size);

        drop((first, second));

        Ok(())
    }

    /// A read that holds its read permits and whose checkout fails the check on
    /// a new connection gives the permits back, so the failed checkouts of a
    /// read-only server do not use up the limits of the reads.
    #[tokio::test]
    async fn read_whose_checkout_fails_the_check_gives_its_permits_back() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(_probe) =
            storage_with_schema("read_whose_checkout_fails_the_check", &CONNECTION, &cluster)
                .await?
        else {
            return Ok(());
        };

        let storage = Postgres::builder(&read_only_server(""))?
            .cluster(&cluster)
            .node(rng().random_range(0..i32::MAX))
            .build();
        let available = storage.fetch_reads.available_permits();

        for _ in 0..3 {
            let permits = storage.fetch_read_permits().await?;
            assert_eq!(available - 1, storage.fetch_reads.available_permits());

            assert!(
                storage
                    .abandonable_connection(permits, StorageOperation::Fetch)
                    .await
                    .is_err()
            );
            assert_eq!(available, storage.fetch_reads.available_permits());
        }

        Ok(())
    }

    /// `read_only_check_every=0` turns the check off, and a checkout of a
    /// connection to a read-only server succeeds as it did before the check.
    #[tokio::test]
    async fn read_only_check_every_zero_hands_out_a_read_only_connection() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(_probe) = storage_with_schema(
            "read_only_check_every_zero_hands_out",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        let storage = Postgres::builder(&read_only_server("&read_only_check_every=0"))?
            .cluster(&cluster)
            .node(rng().random_range(0..i32::MAX))
            .build();

        let connection = storage.connection().await?;
        let row = connection
            .query_one("SELECT current_setting('transaction_read_only')", &[])
            .await?;

        assert_eq!("on", row.get::<_, String>(0));
        assert_eq!(1, storage.pool.status().size);

        Ok(())
    }

    /// With `read_only_check_every=2`, a connection that turns read-only is
    /// replaced at the checkout that checks it, and the checkout between two
    /// checks keeps it.
    #[tokio::test]
    async fn read_only_check_every_skips_the_checkouts_between_two_checks() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(_probe) = storage_with_schema(
            "read_only_check_every_skips_checkouts",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        let storage = Postgres::builder(&format!("{}?read_only_check_every=2", *CONNECTION))?
            .cluster(&cluster)
            .node(rng().random_range(0..i32::MAX))
            .build();
        storage.pool.resize(1);

        async fn pid_and_read_only(storage: &Postgres) -> Result<(i32, String)> {
            let connection = storage.connection().await?;
            let row = connection
                .query_one(
                    "SELECT pg_backend_pid(), current_setting('transaction_read_only')",
                    &[],
                )
                .await?;
            Ok((row.get(0), row.get(1)))
        }

        // Checkout 1 creates the connection.
        let (pid, _) = pid_and_read_only(&storage).await?;

        // Checkout 2 is its first recycle, which is checked and passes. The connection then
        // turns read-only.
        storage
            .connection()
            .await?
            .batch_execute("SET default_transaction_read_only = on")
            .await?;

        // Checkout 3 is its second recycle: it is not checked, so the read-only connection is
        // kept.
        assert_eq!(
            (pid, String::from("on")),
            pid_and_read_only(&storage).await?
        );

        // Checkout 4 is its third recycle, which is checked, and replaces it.
        let (replacement, read_only) = pid_and_read_only(&storage).await?;
        assert_ne!(pid, replacement);
        assert_eq!("off", read_only);

        Ok(())
    }

    /// The counter of dropped connections counts by stage and reason: a new
    /// connection to a read-only server, and an idle one that turned
    /// read-only.
    #[tokio::test]
    async fn dropped_connections_are_counted_by_stage_and_reason() -> Result<()> {
        use opentelemetry::metrics::MeterProvider as _;
        use opentelemetry_sdk::metrics::{
            InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
            data::{AggregatedMetrics, MetricData},
        };

        let cluster = alphanumeric_string(15);

        let Some(_probe) =
            storage_with_schema("dropped_connections_are_counted", &CONNECTION, &cluster).await?
        else {
            return Ok(());
        };

        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let counter = provider
            .meter("test")
            .u64_counter("nisshi_sql_pool_replaced")
            .build();
        let check = ReadOnlyCheck::counting(1, &cluster, counter);

        let counted = |stage: &str, reason: &str| -> u64 {
            provider.force_flush().expect("flush");

            exporter
                .get_finished_metrics()
                .expect("finished metrics")
                .iter()
                .flat_map(|resource| resource.scope_metrics())
                .flat_map(|scope| scope.metrics())
                .filter_map(|metric| match metric.data() {
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => Some(sum),
                    _ => None,
                })
                .flat_map(|sum| sum.data_points())
                .filter(|point| {
                    let has = |key: &str, value: &str| {
                        point
                            .attributes()
                            .any(|kv| kv.key.as_str() == key && kv.value.as_str() == value)
                    };

                    has("cluster_id", &cluster) && has("stage", stage) && has("reason", reason)
                })
                .map(|point| point.value())
                .max()
                .unwrap_or(0)
        };

        let read_only = Postgres::builder(&read_only_server(""))?;
        let pool = PoolParts::open(
            read_only.pool.config.clone(),
            read_only.pool.tls.clone(),
            2,
            &check,
        )?;

        assert_matches!(
            pool.get().await,
            Err(CheckoutError::Pool(PoolError::PostCreateHook(_)))
        );
        assert_eq!(1, counted("create", "read_only"));
        assert_eq!(0, counted("recycle", "read_only"));

        let writable = Postgres::builder(&CONNECTION)?;
        let pool = PoolParts::open(
            writable.pool.config.clone(),
            writable.pool.tls.clone(),
            1,
            &check,
        )?;

        pool.get()
            .await?
            .batch_execute("SET default_transaction_read_only = on")
            .await?;
        let replacement = pool.get().await?;

        assert_eq!(
            "off",
            replacement
                .query_one("SELECT current_setting('transaction_read_only')", &[])
                .await?
                .get::<_, String>(0)
        );
        assert_eq!(1, counted("recycle", "read_only"));
        assert_eq!(1, counted("create", "read_only"));

        Ok(())
    }

    /// A `Write` that appends to a buffer that the test reads back.
    #[derive(Clone, Default)]
    struct LoggedBytes(Arc<std::sync::Mutex<Vec<u8>>>);

    impl io::Write for LoggedBytes {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// An idle connection that the server ended is replaced at its checkout,
    /// counted under the recycle stage with the reason `closed`, and the broker
    /// logs it, because the pool no longer logs a closed connection.
    #[tokio::test]
    async fn closed_idle_connection_is_replaced_counted_and_logged() -> Result<()> {
        use opentelemetry::metrics::MeterProvider as _;
        use opentelemetry_sdk::metrics::{
            InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
            data::{AggregatedMetrics, MetricData},
        };

        let cluster = alphanumeric_string(15);

        let Some(_probe) =
            storage_with_schema("closed_idle_connection_is_replaced", &CONNECTION, &cluster)
                .await?
        else {
            return Ok(());
        };

        let logged = LoggedBytes::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_writer({
                    let logged = logged.clone();
                    move || logged.clone()
                })
                .with_ansi(false)
                .finish(),
        );

        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let counter = provider
            .meter("test")
            .u64_counter("nisshi_sql_pool_replaced")
            .build();
        let check = ReadOnlyCheck::counting(1, &cluster, counter);

        let writable = Postgres::builder(&CONNECTION)?;
        let pool = PoolParts::open(
            writable.pool.config.clone(),
            writable.pool.tls.clone(),
            1,
            &check,
        )?;

        let pid = {
            let connection = pool.get().await?;
            connection
                .query_one("SELECT pg_backend_pid()", &[])
                .await?
                .get::<_, i32>(0)
        };

        // The server ends the idle connection, as a restart or a failover does.
        let monitor = Postgres::builder(&CONNECTION)?
            .cluster(&cluster)
            .node(rng().random_range(0..i32::MAX))
            .build();
        _ = monitor
            .connection()
            .await?
            .query("SELECT pg_terminate_backend($1)", &[&pid])
            .await?;

        // The connection task needs a moment to see the end of the socket.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let replacement = pool.get().await?;
        let replacement_pid = replacement
            .query_one("SELECT pg_backend_pid()", &[])
            .await?
            .get::<_, i32>(0);

        assert_ne!(pid, replacement_pid);

        provider.force_flush().expect("flush");
        let counted = exporter
            .get_finished_metrics()
            .expect("finished metrics")
            .iter()
            .flat_map(|resource| resource.scope_metrics())
            .flat_map(|scope| scope.metrics())
            .filter_map(|metric| match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => Some(sum),
                _ => None,
            })
            .flat_map(|sum| sum.data_points())
            .filter(|point| {
                let has = |key: &str, value: &str| {
                    point
                        .attributes()
                        .any(|kv| kv.key.as_str() == key && kv.value.as_str() == value)
                };

                has("stage", "recycle") && has("reason", "closed")
            })
            .map(|point| point.value())
            .sum::<u64>();
        assert_eq!(1, counted);

        let log = String::from_utf8(
            logged
                .0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
        )
        .expect("utf8 log");
        assert!(
            log.lines().any(|line| line.contains("WARN")
                && line.contains("a pooled connection failed its check and is dropped")
                && line.contains("recycle")
                && line.contains("closed")),
            "{log}"
        );

        Ok(())
    }

    /// [`CheckFailure::of`] tells a read-only session from any other server
    /// error.
    #[tokio::test]
    async fn check_failure_names_the_read_only_session() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(_probe) =
            storage_with_schema("check_failure_names_read_only", &CONNECTION, &cluster).await?
        else {
            return Ok(());
        };

        let storage = Postgres::builder(&CONNECTION)?
            .cluster(&cluster)
            .node(rng().random_range(0..i32::MAX))
            .build();
        let connection = storage.connection().await?;

        let other = connection
            .simple_query("SELECT 1/0")
            .await
            .expect_err("a division by zero is an error");
        assert_eq!(CheckFailure::Error, CheckFailure::of(&other));

        connection
            .batch_execute("SET default_transaction_read_only = on")
            .await?;
        let read_only = connection
            .simple_query(RECYCLE_QUERY)
            .await
            .expect_err("a read-only session fails the check");
        assert_eq!(CheckFailure::ReadOnly, CheckFailure::of(&read_only));

        Ok(())
    }

    /// Without `read_only_check_every`, the check runs on every
    /// [`DEFAULT_READ_ONLY_CHECK_EVERY`]-th checkout of an idle connection, and
    /// the option takes the numbers in [`READ_ONLY_CHECK_EVERY_RANGE`]. The
    /// other options in the URL still reach tokio-postgres.
    #[test]
    fn read_only_check_every_is_read_from_the_url() -> Result<()> {
        let default = Postgres::builder(&CONNECTION)?;
        assert_eq!(
            DEFAULT_READ_ONLY_CHECK_EVERY,
            default.pool.read_only_check_every
        );

        for every in [0, 1, 7, 1_000_000] {
            let builder = Postgres::builder(&format!(
                "{}?options=-c%20statement_timeout%3D120000&read_only_check_every={every}",
                *CONNECTION
            ))?;

            assert_eq!(every, builder.pool.read_only_check_every);
        }

        Ok(())
    }

    /// A `read_only_check_every` that is not a number in
    /// [`READ_ONLY_CHECK_EVERY_RANGE`] stops the build before any connection,
    /// and the error names the option and its value.
    #[test]
    fn read_only_check_every_rejects_invalid_values() {
        for value in ["banana", "", "-1", "1000001", "18446744073709551616"] {
            let error =
                Postgres::builder(&format!("{}?read_only_check_every={value}", *CONNECTION))
                    .expect_err(value);

            assert_matches!(
                &error,
                Error::InvalidStorageOptionValue { scheme, option, value: rejected, expected }
                    if scheme == "postgres"
                        && option == READ_ONLY_CHECK_EVERY
                        && rejected == value
                        && *expected == READ_ONLY_CHECK_EVERY_EXPECTED,
                "{value}"
            );
        }
    }

    /// Returns storage on a pool of one connection that connects with a
    /// unique `application_name`, a monitor on a pool of its own, and that
    /// name, or `None` when no server answers. The schema is in place.
    async fn storage_on_one_connection(
        cluster: &str,
    ) -> Result<Option<(Postgres, Postgres, String)>> {
        let application_name = format!("abandoned_read_{}", alphanumeric_string(15));

        let storage = Postgres::builder(&format!(
            "{}?application_name={application_name}",
            *CONNECTION
        ))?
        .cluster(cluster)
        .node(rng().random_range(0..i32::MAX))
        .build();
        storage.pool.resize(1);

        // The tests of this module share one database, and a test that
        // starts before another one has created the schema fails without
        // this.
        Ok(
            storage_with_schema("storage_on_one_connection", &CONNECTION, cluster)
                .await?
                .map(|monitor| (storage, monitor, application_name)),
        )
    }

    /// Installs a process-wide meter provider that exports to the returned exporter. nextest runs
    /// each test in a process of its own, so the provider belongs to the test that installs it.
    fn install_exporter() -> (SdkMeterProvider, InMemoryMetricExporter) {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        opentelemetry::global::set_meter_provider(provider.clone());
        (provider, exporter)
    }

    /// Returns the total of the counter `name` for `cluster`, after `provider` exports what it
    /// holds.
    ///
    /// The counters are cumulative, so the total comes from the latest export only. A sum over
    /// every export would count a value once for each export.
    fn counter_total(
        provider: &SdkMeterProvider,
        exporter: &InMemoryMetricExporter,
        name: &str,
        cluster: &str,
    ) -> Result<u64> {
        provider
            .force_flush()
            .map_err(|err| Error::Message(err.to_string()))?;

        let metrics = exporter.get_finished_metrics().unwrap_or_default();

        Ok(metrics
            .last()
            .into_iter()
            .flat_map(ResourceMetrics::scope_metrics)
            .flat_map(ScopeMetrics::metrics)
            .filter(|metric| metric.name() == name)
            .map(|metric| match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                    .data_points()
                    .filter(|point| {
                        point.attributes().any(|attribute| {
                            attribute.key.as_str() == "cluster_id"
                                && attribute.value.as_str() == cluster
                        })
                    })
                    .map(SumDataPoint::value)
                    .sum(),
                _ => 0,
            })
            .sum())
    }

    /// A fetch dropped while its statement waits has that statement
    /// cancelled, and its connection goes back to the pool idle and outside
    /// any transaction, while the statement would still be waiting. A fetch
    /// or an offset stage that finishes returns its connection and its
    /// permit before it returns.
    ///
    /// The fetch waits for a lock on `header`, so it waits in its records
    /// statement, inside its transaction.
    #[tokio::test]
    async fn abandoned_fetch_is_cancelled_and_its_connection_returned() -> Result<()> {
        // The exporter must be installed before the storage creates its first SQL metric.
        let (provider, exporter) = install_exporter();

        let cluster = alphanumeric_string(15);
        let Some((storage, monitor, application_name)) =
            storage_on_one_connection(&cluster).await?
        else {
            return Ok(());
        };

        let topition = topic_with_one_record(&storage, &cluster).await?;

        fetch_from_start(&storage, &topition).await?;
        assert_eq!(1, storage.pool.status().available);
        assert_eq!(
            ReadLimits::default().fetch,
            storage.fetch_reads.available_permits()
        );

        _ = storage.offset_stage(&topition).await?;
        assert_eq!(1, storage.pool.status().available);
        assert_eq!(
            ReadLimits::default().fetch,
            storage.fetch_reads.available_permits()
        );
        assert_eq!(
            0,
            counter_total(
                &provider,
                &exporter,
                "nisshi_sql_statements_abandoned",
                &cluster
            )?
        );

        let fetching = {
            let storage = storage.clone();
            let topition = topition.clone();
            async move { fetch_from_start(&storage, &topition).await }
        };

        let backend = abandon_read_waiting_for_a_lock(
            &storage,
            &monitor,
            &application_name,
            "header",
            "from header",
            fetching,
        )
        .await?;

        assert_eq!(
            1,
            counter_total(
                &provider,
                &exporter,
                "nisshi_sql_statements_abandoned",
                &cluster
            )?
        );

        let mut c = storage.connection().await?;
        assert_eq!(1, storage.pool.status().size);
        assert_eq!(
            backend,
            c.query_one("SELECT pg_backend_pid()", &[])
                .await?
                .get::<_, i32>(0),
            "the pool kept the abandoned fetch's connection"
        );

        let tx = c.transaction().await?;
        assert_eq!(1, tx.query_one("SELECT 1", &[]).await?.get::<_, i32>(0));
        tx.commit().await?;

        Ok(())
    }

    /// A fetch whose deadline passes while its statement waits returns at that deadline with no
    /// batches. The engine cancels the statement, so the connection goes back to the pool idle, as
    /// [`SETTLE_QUERY`] shows, while the lock is still held.
    ///
    /// The fetch waits for a lock on `header`, so it waits in its records statement, inside its
    /// transaction.
    #[tokio::test]
    async fn fetch_stopped_at_its_deadline_cancels_its_statement() -> Result<()> {
        // The exporter must be installed before the storage creates its first SQL metric.
        let (provider, exporter) = install_exporter();

        let cluster = alphanumeric_string(15);
        let Some((storage, monitor, application_name)) =
            storage_on_one_connection(&cluster).await?
        else {
            return Ok(());
        };

        let topition = topic_with_one_record(&storage, &cluster).await?;

        let mut locking = monitor.connection().await?;

        // A transaction reads one snapshot of `pg_stat_activity`, so the
        // test watches the read from outside the locking transaction.
        let watching = monitor.connection().await?;

        // The test releases the lock before it returns. The timeouts bound
        // how long the lock holds up other tests when this test stops early.
        let lock = locking.transaction().await?;
        lock.batch_execute(concat!(
            "SET LOCAL lock_timeout = '5s'; ",
            "SET LOCAL idle_in_transaction_session_timeout = '10s'; ",
            "LOCK TABLE header IN ACCESS EXCLUSIVE MODE",
        ))
        .await?;

        let read_time = Duration::from_millis(500);
        let started_at = Instant::now();

        let fetched = storage
            .fetch_until_deadline(
                &topition,
                0,
                1,
                8 * 1024,
                IsolationLevel::ReadUncommitted,
                started_at + read_time,
            )
            .await
            .map(|read| read.batches)?;

        assert!(fetched.is_empty(), "{fetched:?}");

        // The lock holds for up to 10s, so this bound fails for a fetch that waits for its
        // statement to end.
        assert!(
            started_at.elapsed() < read_time + Duration::from_secs(1),
            "{:?}",
            started_at.elapsed()
        );

        // The loop passes only when the pool's one connection is idle after `SETTLE_QUERY` while
        // the lock is still held. A connection released with its statement still waiting is back
        // in the pool while its backend is still active.
        let give_up = Instant::now() + Duration::from_secs(10);
        loop {
            let activity = watching
                .query_opt(
                    "SELECT state, query FROM pg_stat_activity WHERE application_name = $1",
                    &[&application_name],
                )
                .await?
                .map(|row| {
                    (
                        row.get::<_, Option<String>>(0),
                        row.get::<_, Option<String>>(1),
                    )
                });
            let status = storage.pool.status();

            if let Some((Some(state), query)) = &activity
                && state == "idle"
                && status.available == 1
            {
                assert_eq!(Some(SETTLE_QUERY), query.as_deref());
                break;
            }

            assert!(
                Instant::now() < give_up,
                "{activity:?} while the lock on header is held, pool {status:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert_eq!(
            ReadLimits::default().fetch,
            storage.fetch_reads.available_permits()
        );
        assert_eq!(
            1,
            counter_total(
                &provider,
                &exporter,
                "nisshi_sql_statements_abandoned",
                &cluster
            )?
        );

        lock.rollback().await?;

        Ok(())
    }

    /// A fetch that fails before its records statement starts returns its connection to the pool
    /// at once. The fetch does not cancel a statement, because none runs.
    #[tokio::test]
    async fn fetch_failed_before_its_records_statement_releases_its_connection() -> Result<()> {
        // The exporter must be installed before the storage creates its first SQL metric.
        let (provider, exporter) = install_exporter();

        let cluster = alphanumeric_string(15);
        let Some((storage, _, _)) = storage_on_one_connection(&cluster).await? else {
            return Ok(());
        };

        let topition = topic_with_one_record(&storage, &cluster).await?;

        // The topic's only partition is partition 0, so partition 1 has no offset stage, and
        // the fetch fails before its records statement.
        let unknown = Topition::new(topition.topic(), 1);

        let fetched = storage
            .fetch_until_deadline(
                &unknown,
                0,
                1,
                8 * 1024,
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_secs(30),
            )
            .await;
        assert_matches!(fetched, Err(_));

        assert_eq!(1, storage.pool.status().available);
        assert_eq!(
            ReadLimits::default().fetch,
            storage.fetch_reads.available_permits()
        );

        // The setup's statements count in `nisshi_sql_requests`, so the exporter is live, and a
        // count of 0 abandoned statements is not a missing export.
        assert!(counter_total(&provider, &exporter, "nisshi_sql_requests", &cluster)? > 0);
        assert_eq!(
            0,
            counter_total(
                &provider,
                &exporter,
                "nisshi_sql_statements_abandoned",
                &cluster
            )?
        );

        Ok(())
    }

    /// An offset stage dropped while its statement waits has that statement
    /// cancelled, and its connection goes back to the pool idle, while the
    /// statement would still be waiting.
    ///
    /// The offset stage waits for a lock on `txn_produce_offset`, which only
    /// transactional produces write.
    #[tokio::test]
    async fn abandoned_offset_stage_is_cancelled_and_its_connection_returned() -> Result<()> {
        let cluster = alphanumeric_string(15);
        let Some((storage, monitor, application_name)) =
            storage_on_one_connection(&cluster).await?
        else {
            return Ok(());
        };

        let topition = topic_with_one_record(&storage, &cluster).await?;

        let reading = {
            let storage = storage.clone();
            let topition = topition.clone();
            async move { storage.offset_stage(&topition).await.map(drop) }
        };

        let backend = abandon_read_waiting_for_a_lock(
            &storage,
            &monitor,
            &application_name,
            "txn_produce_offset",
            "txn_produce_offset",
            reading,
        )
        .await?;

        let c = storage.connection().await?;
        assert_eq!(
            backend,
            c.query_one("SELECT pg_backend_pid()", &[])
                .await?
                .get::<_, i32>(0),
            "the pool kept the abandoned offset stage's connection"
        );

        Ok(())
    }

    #[test]
    fn pool_error_timeout_maps_to_retriable_error_code() {
        let error = Error::from(PoolError::Timeout(TimeoutType::Wait));
        assert!(
            matches!(error, Error::Api(ErrorCode::RequestTimedOut)),
            "expected a retriable RequestTimedOut, got {error:?}"
        );

        let error = Error::from(PoolError::Timeout(TimeoutType::Create));
        assert!(
            matches!(error, Error::Api(ErrorCode::RequestTimedOut)),
            "expected a retriable RequestTimedOut, got {error:?}"
        );

        // a non-timeout pool error is a real backend failure, not transient load, and must
        // stay the generic (non-retriable) variant.
        let error = Error::from(PoolError::Closed);
        assert!(
            matches!(error, Error::Pool(_)),
            "expected the generic Pool error, got {error:?}"
        );
    }

    /// Proves the exact mechanism `policy_compact`/`policy_delete` now rely on: `SET LOCAL
    /// statement_timeout = 0` inside a transaction must survive a query that would otherwise
    /// be aborted by a short connection-level default, and the abort must map to a retriable
    /// error when the override is absent.
    #[tokio::test]
    async fn set_local_statement_timeout_exempts_a_transaction() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let storage = Postgres::builder(&format!(
            "{}?options=-c%20statement_timeout%3D100",
            *CONNECTION
        ))?
        .cluster(cluster.as_str())
        .node(rng().random_range(0..i32::MAX))
        .build();

        if let Err(err) = storage.connection().await {
            eprintln!("skipping set_local_statement_timeout_exempts_a_transaction: {err:?}");
            return Ok(());
        }

        // baseline: the connection's 100ms default aborts a 1s sleep with a retriable error.
        let mut c = storage.connection().await?;
        let tx = c.transaction().await?;
        let started = SystemTime::now();
        let result = tx.query("select pg_sleep(1)", &[]).await;
        let elapsed = started.elapsed().unwrap_or_default();
        _ = tx.rollback().await;

        assert!(
            elapsed < Duration::from_millis(900),
            "expected the 100ms default to abort quickly, took {elapsed:?}"
        );
        match result {
            Ok(_) => panic!("expected pg_sleep(1) to be cancelled by the 100ms default"),
            Err(err) => {
                let error = Error::from(err);
                assert!(
                    matches!(error, Error::Api(ErrorCode::RequestTimedOut)),
                    "expected a retriable RequestTimedOut, got {error:?}"
                );
            }
        }

        // with the SET LOCAL override, the same sleep must complete.
        let mut c = storage.connection().await?;
        let tx = c.transaction().await?;
        tx.batch_execute("SET LOCAL statement_timeout = 0").await?;
        let started = SystemTime::now();
        _ = tx
            .query("select pg_sleep(1)", &[])
            .await
            .inspect_err(|err| error!(?err))
            .expect("SET LOCAL statement_timeout = 0 should exempt this query");
        let elapsed = started.elapsed().unwrap_or_default();
        tx.commit().await?;

        assert!(
            elapsed >= Duration::from_millis(900),
            "expected the exempted query to actually sleep ~1s, took {elapsed:?}"
        );

        Ok(())
    }

    /// End-to-end smoke test: `policy_compact`/`policy_delete` must still complete
    /// successfully even when the connection's global `statement_timeout` is set far below
    /// what a real (if fast, in this test) sweep could take, proving the `SET LOCAL` guard
    /// added to both doesn't break their normal, fast-completing path.
    #[tokio::test]
    async fn maintenance_sweeps_are_exempt_from_statement_timeout() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "maintenance_sweeps_are_exempt_from_statement_timeout",
            &format!("{}?options=-c%20statement_timeout%3D100", *CONNECTION),
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        _ = storage.policy_compact().await?;
        _ = storage.policy_delete(SystemTime::now()).await?;
        _ = storage.record_delete_below_log_start().await?;

        Ok(())
    }

    /// The `retention.bytes` sweep reads a partition from its newest record only until the bytes
    /// pass the limit, so its cost follows what a partition may keep and not how many records it
    /// holds, and it deletes the older records of every partition that has the limit.
    #[tokio::test]
    async fn retention_bytes_sweep_stops_reading_at_the_limit() -> Result<()> {
        use nisshi_sans_io::create_topics_request::CreatableTopicConfig;

        const PARTITIONS: i32 = 2;
        const ROWS: i64 = 20_000;
        const VALUE_BYTES: i64 = 1_000;
        const KEPT: i64 = 100;

        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "retention_bytes_sweep_stops_reading_at_the_limit",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let configs = [
            ("cleanup.policy", "delete".to_owned()),
            ("retention.bytes", (KEPT * VALUE_BYTES).to_string()),
        ];

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(PARTITIONS)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some(
                        configs
                            .iter()
                            .map(|(name, value)| {
                                CreatableTopicConfig::default()
                                    .name((*name).into())
                                    .value(Some(value.as_str().into()))
                            })
                            .collect(),
                    )),
                false,
            )
            .await?;

        let mut c = storage.connection().await?;
        let tx = c.transaction().await?;

        // fill the partitions directly, which is far faster than producing
        _ = tx
            .execute(
                "insert into record (topition, offset_id, v) \
             select tp.id, g, decode(repeat('00', $2), 'hex') \
             from topition tp \
             join topic t on t.id = tp.topic \
             cross join generate_series(0::bigint, $3) g \
             where t.name = $1",
                &[&topic_name, &i32::try_from(VALUE_BYTES)?, &(ROWS - 1)],
            )
            .await?;
        _ = tx
            .execute(
                "update watermark set low = 0, high = $2 \
             where topition in ( \
                 select tp.id from topition tp join topic t on t.id = tp.topic where t.name = $1)",
                &[&topic_name, &ROWS],
            )
            .await?;
        tx.batch_execute("analyze record").await?;

        // On a table this small, with no visibility map yet, the planner prefers to scan and sort.
        // On the large partitions that a limit is for it walks the primary key, which is the case
        // that this test is about.
        tx.batch_execute("set local enable_seqscan = off; set local enable_bitmapscan = off")
            .await?;

        let sql = crate::sql::SQL.get("policy_delete_bytes.sql")?;
        let plan = tx
            .query(
                &format!("explain (analyze, costs off, timing off, buffers off) {sql}"),
                &[&cluster],
            )
            .await?
            .iter()
            .map(|row| row.get::<_, String>(0))
            .collect::<Vec<_>>();

        // the scan that reads each partition from its newest record, as the planner counts it:
        // rows per loop times loops
        let read = plan
            .iter()
            .find(|line| line.contains("Index Scan Backward"))
            .and_then(|line| {
                let rows = line.split("rows=").nth(1)?.split_whitespace().next()?;
                let loops = line.split("loops=").nth(1)?.split(')').next()?;
                Some(rows.parse::<f64>().ok()? * loops.parse::<f64>().ok()?)
            })
            .unwrap_or_else(|| panic!("no backward scan of the primary key in {plan:#?}"));

        let remaining = tx
            .query(
                "select count(*) from record r \
                 join topition tp on tp.id = r.topition \
                 join topic t on t.id = tp.topic where t.name = $1 group by tp.id",
                &[&topic_name],
            )
            .await?
            .iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<Vec<_>>();

        _ = tx.rollback().await;

        // each partition keeps its newest KEPT records
        assert_eq!(vec![KEPT; PARTITIONS as usize], remaining, "{plan:#?}");

        // and the sweep read about KEPT records of each, where it held ROWS
        assert!(
            read <= (PARTITIONS as f64) * (KEPT as f64 + 2.0),
            "read {read} records of {} in {plan:#?}",
            PARTITIONS as i64 * ROWS
        );

        Ok(())
    }

    /// DeleteRecords commits the new log start before it removes the records
    /// below it. When that removal fails, `maintain` removes them.
    #[tokio::test]
    async fn maintain_removes_records_left_below_log_start() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "maintain_removes_records_left_below_log_start",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);
        let record_count = 6i64;

        for _ in 0..record_count {
            let batch = Batch::builder()
                .record(Record::builder().value(Bytes::from_static(b"r").into()))
                .build()
                .and_then(TryInto::try_into)?;

            _ = storage.produce(None, &topition, batch).await?;
        }

        let stored = async || -> Result<i64> {
            let c = storage.connection().await?;
            let row = c
                .query_one(
                    "select count(*) from record r \
                     join topition tp on r.topition = tp.id \
                     join topic t on tp.topic = t.id \
                     join cluster c on t.cluster = c.id \
                     where c.name = $1 and t.name = $2",
                    &[&cluster, &topic_name],
                )
                .await?;
            row.try_get::<_, i64>(0).map_err(Into::into)
        };

        // the log start advances, as a DeleteRecords whose removal failed leaves it
        let advance_log_start = async |low: i64| -> Result<()> {
            let mut c = storage.connection().await?;
            let tx = c.transaction().await?;

            _ = storage
                .tx_prepare_execute(
                    &tx,
                    "watermark_update.sql",
                    &[&cluster, &topic_name, &0i32, &low, &record_count],
                )
                .await?;

            tx.commit().await.map_err(Into::into)
        };

        advance_log_start(3).await?;
        assert_eq!(record_count, stored().await?);

        storage.maintain(SystemTime::now()).await?;
        assert_eq!(record_count - 3, stored().await?);

        advance_log_start(record_count).await?;
        storage.maintain(SystemTime::now()).await?;
        assert_eq!(0, stored().await?);

        let latest = storage
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[(topition.clone(), ListOffset::Latest)],
            )
            .await?;

        assert_eq!(Some(record_count), latest[0].1.offset);

        Ok(())
    }

    /// A retention sweep that fails, here on a lock it waits for past
    /// `lock_timeout`, does not stop `maintain` from removing the records
    /// below the log start, or from compacting. `maintain` still returns the
    /// failure.
    #[tokio::test]
    async fn maintain_runs_every_step_when_retention_fails() -> Result<()> {
        with_empty_database(
            "maintain_runs_every_step_when_retention_fails",
            async |database| {
                use nisshi_sans_io::create_topics_request::CreatableTopicConfig;
                use tokio_postgres::NoTls;

                // The schema comes from a storage without the lock timeout, so that the timeout applies
                // to the statements of the test only.
                database.storage()?.ensure_schema().await?;

                // Every connection of this storage gives up waiting for a lock after 500ms. The option is
                // written by hand, because `Url::query_pairs_mut` writes a space as `+`, which
                // tokio-postgres reads as itself.
                let mut url = database.url()?;
                let query = match url.query() {
                    Some(query) => format!("{query}&options=-c%20lock_timeout%3D500"),
                    None => "options=-c%20lock_timeout%3D500".to_owned(),
                };
                url.set_query(Some(&query));

                let storage = database.storage_at(&url)?;
                let cluster = storage.cluster.clone();

                storage
                    .register_broker(BrokerRegistrationRequest {
                        broker_id: 111,
                        cluster_id: cluster.clone(),
                        incarnation_id: Uuid::now_v7(),
                        rack: None,
                    })
                    .await?;

                let create_topic =
                    async |name: &str, configs: Vec<CreatableTopicConfig>| -> Result<()> {
                        storage
                            .create_topic(
                                CreatableTopic::default()
                                    .name(name.into())
                                    .num_partitions(1)
                                    .replication_factor(0)
                                    .assignments(Some([].into()))
                                    .configs(Some(configs)),
                                false,
                            )
                            .await
                            .map(|_| ())
                    };

                let produce = async |name: &str, key: Option<Bytes>| -> Result<i64> {
                    let batch = Batch::builder()
                        .record(
                            Record::builder()
                                .key(key)
                                .value(Bytes::from_static(b"r").into()),
                        )
                        .build()
                        .and_then(TryInto::try_into)?;

                    storage.produce(None, &Topition::new(name, 0), batch).await
                };

                let stored = async |name: &str| -> Result<i64> {
                    let c = storage.connection().await?;
                    let row = c
                        .query_one(
                            "select count(*) from record r \
                     join topition tp on r.topition = tp.id \
                     join topic t on tp.topic = t.id \
                     join cluster c on t.cluster = c.id \
                     where c.name = $1 and t.name = $2",
                            &[&cluster, &name],
                        )
                        .await?;
                    row.try_get::<_, i64>(0).map_err(Into::into)
                };

                // retained has the default cleanup.policy, delete
                let retained = alphanumeric_string(15);
                create_topic(&retained, vec![]).await?;

                let record_count = 6i64;
                for _ in 0..record_count {
                    _ = produce(&retained, None).await?;
                }

                // the log start moves to 3, as a DeleteRecords whose removal failed leaves it
                {
                    let mut c = storage.connection().await?;
                    let tx = c.transaction().await?;

                    _ = storage
                        .tx_prepare_execute(
                            &tx,
                            "watermark_update.sql",
                            &[&cluster, &retained, &0i32, &3i64, &record_count],
                        )
                        .await?;

                    tx.commit().await?;
                }

                // compacted holds three records with one key, of which compaction keeps the last
                let compacted = alphanumeric_string(15);
                create_topic(
                    &compacted,
                    vec![
                        CreatableTopicConfig::default()
                            .name("cleanup.policy".into())
                            .value(Some("compact".into())),
                    ],
                )
                .await?;

                for _ in 0..3 {
                    _ = produce(&compacted, Some(Bytes::from_static(b"k"))).await?;
                }

                assert_eq!(record_count, stored(&retained).await?);
                assert_eq!(3, stored(&compacted).await?);

                // Another session locks the record at offset 4, above the log start. Retention, a month
                // after now, deletes every record of retained, so it waits on that lock and fails.
                let (mut locker, connection) =
                    tokio_postgres::connect(database.url()?.as_str(), NoTls).await?;
                let connection = tokio::spawn(connection);

                let lock = locker.transaction().await?;
                _ = lock
                    .query_one(
                        "select r.offset_id from record r \
                 join topition tp on r.topition = tp.id \
                 join topic t on tp.topic = t.id \
                 join cluster c on t.cluster = c.id \
                 where c.name = $1 and t.name = $2 and r.offset_id = 4 \
                 for update of r",
                        &[&cluster, &retained],
                    )
                    .await?;

                let a_month_later = SystemTime::now() + Duration::from_hours(30 * 24);

                assert!(storage.maintain(a_month_later).await.is_err());

                // the records below the log start are removed, and compaction ran
                assert_eq!(record_count - 3, stored(&retained).await?);
                assert_eq!(1, stored(&compacted).await?);

                lock.rollback().await?;
                drop(locker);
                _ = connection.await;

                // with the lock released, retention deletes the rest
                storage.maintain(a_month_later).await?;
                assert_eq!(0, stored(&retained).await?);

                Ok(())
            },
        )
        .await
    }

    /// An open (uncommitted) transaction must pin the read_committed last stable
    /// offset below the high watermark. Regressed when `offset_stage` read the
    /// high-watermark column instead of the stable-offset column, so
    /// `read_committed` consumers saw uncommitted records.
    #[tokio::test]
    async fn read_committed_last_stable_offset_pinned_by_open_transaction() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "read_committed_last_stable_offset_pinned_by_open_transaction",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        // a transactional producer begins a transaction and produces, never commits
        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"uncommitted").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        let stage = storage.offset_stage(&topition).await?;

        assert!(
            stage.high_watermark > 0,
            "the uncommitted record should advance the high watermark"
        );
        assert!(
            stage.last_stable < stage.high_watermark,
            "an open transaction must pin last_stable below the high watermark \
             (last_stable={}, high_watermark={})",
            stage.last_stable,
            stage.high_watermark,
        );

        Ok(())
    }

    /// DeleteRecords can move the log start past the first offset of an open transaction. The
    /// last stable offset then answers the log start, as Kafka raises it, and not the
    /// transaction's first offset below it.
    #[tokio::test]
    async fn delete_records_past_open_transaction_raises_last_stable_to_log_start() -> Result<()> {
        use nisshi_sans_io::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};

        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "delete_records_past_open_transaction_raises_last_stable_to_log_start",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        // a transaction opens at offset 0 and stays open
        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some(vec![0])),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"uncommitted").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        assert_eq!(
            0,
            storage
                .produce(Some(transaction_id.as_str()), &topition, batch)
                .await?
        );

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"after").into()))
            .build()
            .and_then(TryInto::try_into)?;
        assert_eq!(1, storage.produce(None, &topition, batch).await?);

        let log_start = 1;

        let results = storage
            .delete_records(&[DeleteRecordsTopic::default()
                .name(topic_name.clone())
                .partitions(Some(vec![
                    DeleteRecordsPartition::default()
                        .partition_index(0)
                        .offset(log_start),
                ]))])
            .await?;
        assert_eq!(
            log_start,
            results[0].partitions.as_deref().unwrap_or_default()[0].low_watermark
        );

        assert_eq!(
            OffsetStage {
                last_stable: log_start,
                high_watermark: 2,
                log_start,
            },
            storage.offset_stage(&topition).await?
        );

        Ok(())
    }

    /// A transaction whose timeout has elapsed without the client calling `EndTxn` (a crashed
    /// or abandoned producer) must be aborted by `maintain_transactions`, releasing the
    /// `read_committed` last stable offset it was pinning.
    #[tokio::test]
    async fn maintain_transactions_aborts_timed_out_transaction() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "maintain_transactions_aborts_timed_out_transaction",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        // a transactional producer begins a transaction and produces, never commits
        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"abandoned").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        let stage = storage.offset_stage(&topition).await?;

        assert!(
            stage.last_stable < stage.high_watermark,
            "an open transaction must pin last_stable below the high watermark \
             (last_stable={}, high_watermark={})",
            stage.last_stable,
            stage.high_watermark,
        );

        // no need to actually sleep past the timeout: `now` is a plain parameter,
        // so pass a `now` far enough ahead that the 10s timeout has "elapsed".
        storage
            .maintain_transactions(SystemTime::now() + Duration::from_secs(3600))
            .await?;

        let stage = storage.offset_stage(&topition).await?;

        assert_eq!(
            stage.high_watermark, stage.last_stable,
            "the timed-out transaction should have been aborted, releasing last_stable \
             (last_stable={}, high_watermark={})",
            stage.last_stable, stage.high_watermark,
        );

        Ok(())
    }

    /// A transaction that hasn't reached its own `transaction_timeout_ms` yet must be left
    /// alone by the sweep -- it should not be aborted just because maintain_transactions ran.
    #[tokio::test]
    async fn maintain_transactions_leaves_transaction_before_timeout() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "maintain_transactions_leaves_transaction_before_timeout",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"still-active").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        let before = storage.offset_stage(&topition).await?;

        // `now` is barely past `started_at`, nowhere near the 10s timeout.
        storage
            .maintain_transactions(SystemTime::now() + Duration::from_millis(50))
            .await?;

        let after = storage.offset_stage(&topition).await?;

        assert_eq!(
            before.last_stable, after.last_stable,
            "a transaction within its timeout must not be touched by the sweep \
             (before={}, after={})",
            before.last_stable, after.last_stable,
        );
        assert!(
            after.last_stable < after.high_watermark,
            "the still-open transaction must remain pinned \
             (last_stable={}, high_watermark={})",
            after.last_stable,
            after.high_watermark,
        );

        Ok(())
    }

    /// When a timed-out transaction overlaps an older, still-open transaction on the same
    /// partition, the sweep must not finalize it out of order: it should stage to
    /// PREPARE_ABORT and leave the last stable offset exactly where the older transaction
    /// pins it, until that older transaction resolves too.
    #[tokio::test]
    async fn maintain_transactions_defers_when_older_overlapping_transaction_still_open()
    -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "maintain_transactions_defers_when_older_overlapping_transaction_still_open",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        // producer A: opens first, given a long timeout, and never resolves -- it's the
        // oldest open transaction on this partition, so it pins the last stable offset.
        let txn_a = alphanumeric_string(10);
        let producer_a = storage
            .init_producer(Some(txn_a.as_str()), 10_000_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_a.clone(),
                producer_id: producer_a.id,
                producer_epoch: producer_a.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch_a = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"a").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer_a.id)
            .producer_epoch(producer_a.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn_a.as_str()), &topition, batch_a)
            .await?;

        // producer B: opens second, on the same partition, with a short timeout -- it will
        // be picked up by the sweep, but must defer to A.
        let txn_b = alphanumeric_string(10);
        let producer_b = storage
            .init_producer(Some(txn_b.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_b.clone(),
                producer_id: producer_b.id,
                producer_epoch: producer_b.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch_b = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"b").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer_b.id)
            .producer_epoch(producer_b.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn_b.as_str()), &topition, batch_b)
            .await?;

        let before = storage.offset_stage(&topition).await?;

        // far enough ahead for B's 10s timeout to have elapsed, nowhere near A's ~10,000s one.
        storage
            .maintain_transactions(SystemTime::now() + Duration::from_secs(20))
            .await?;

        let after = storage.offset_stage(&topition).await?;

        assert_eq!(
            before.last_stable, after.last_stable,
            "B must not advance last_stable while A (older, still open) remains pinned \
             (before={}, after={})",
            before.last_stable, after.last_stable,
        );
        assert!(
            after.last_stable < after.high_watermark,
            "A is still open, so last_stable must remain pinned below the high watermark \
             (last_stable={}, high_watermark={})",
            after.last_stable,
            after.high_watermark,
        );

        Ok(())
    }

    /// The status guard added to end_in_tx must reject a second, stale call that conflicts
    /// with what already happened -- this is the exact race maintain_transactions and a
    /// genuinely concurrent client EndTxn could hit: a candidate found by the sweep's query
    /// that a real client commits first, then the sweep's stale abort arrives late. The guard
    /// must not silently ack the sweep's abort as success (that would tell the caller their
    /// request "worked" when the transaction was actually already committed).
    #[tokio::test]
    async fn txn_end_after_already_finalized_with_conflicting_outcome_is_rejected() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "txn_end_after_already_finalized_is_a_no_op",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"raced").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        // the "real client" wins the race and commits first.
        _ = storage
            .txn_end(&transaction_id, producer.id, producer.epoch, true)
            .await?;

        let after_commit = storage.offset_stage(&topition).await?;

        assert_eq!(
            after_commit.last_stable, after_commit.high_watermark,
            "the committed transaction should have released last_stable \
             (last_stable={}, high_watermark={})",
            after_commit.last_stable, after_commit.high_watermark,
        );

        // the sweep's stale view still thinks it should abort the same transaction -- but the
        // real client already committed it. This must be rejected, not silently acked.
        let error_code = storage
            .txn_end(&transaction_id, producer.id, producer.epoch, false)
            .await?;

        assert_eq!(
            ErrorCode::InvalidTxnState,
            error_code,
            "a stale abort arriving after a real commit must be rejected, not acked as \
             success -- the caller has no other way to learn their request didn't apply"
        );

        let after_conflict = storage.offset_stage(&topition).await?;

        assert_eq!(
            after_commit.high_watermark, after_conflict.high_watermark,
            "the rejected call must not append a second control-batch marker \
             (high_watermark before={}, after={})",
            after_commit.high_watermark, after_conflict.high_watermark,
        );

        Ok(())
    }

    /// A retry (or a genuine race between a real EndTxn and the sweep) that lands while a
    /// transaction is deferred (PREPARE_ABORT/PREPARE_COMMIT -- an older, still-open
    /// transaction on the same partition hasn't resolved yet) must not write a second control
    /// marker. Deferral means the marker was already written on the first call; only the
    /// overlap check should re-run.
    #[tokio::test]
    async fn deferred_txn_end_retry_does_not_duplicate_marker() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "deferred_txn_end_retry_does_not_duplicate_marker",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        // producer A: opens first, never resolves -- pins B behind it.
        let txn_a = alphanumeric_string(10);
        let producer_a = storage
            .init_producer(Some(txn_a.as_str()), 10_000_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_a.clone(),
                producer_id: producer_a.id,
                producer_epoch: producer_a.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch_a = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"a").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer_a.id)
            .producer_epoch(producer_a.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn_a.as_str()), &topition, batch_a)
            .await?;

        // producer B: opens second, overlapping A -- must defer.
        let txn_b = alphanumeric_string(10);
        let producer_b = storage
            .init_producer(Some(txn_b.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_b.clone(),
                producer_id: producer_b.id,
                producer_epoch: producer_b.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch_b = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"b").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer_b.id)
            .producer_epoch(producer_b.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn_b.as_str()), &topition, batch_b)
            .await?;

        // first call: B defers (A is still open), writing its abort marker but only reaching
        // PREPARE_ABORT, not the terminal ABORTED the guard checks for.
        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&txn_b, producer_b.id, producer_b.epoch, false)
                .await?
        );

        // second call: simulates a retry, or the sweep racing a real client's EndTxn, while B
        // is still sitting in PREPARE_ABORT. The guard should recognize this as already
        // handled and no-op -- if it doesn't, a second marker gets written.
        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&txn_b, producer_b.id, producer_b.epoch, false)
                .await?
        );

        // Two calls while deferred must still result in exactly one control-batch marker
        // record for producer B on this partition -- a real Kafka client seeing two
        // conflicting markers for the same producer is undefined/nonsensical behavior. Counted
        // directly against the record table (aborted_transactions isn't wired up yet at this
        // point in the stack).
        let c = storage.connection().await?;
        let marker_count: i64 = c
            .query_one(
                "select count(*) from cluster c \
                 join topic t on t.cluster = c.id \
                 join topition tp on tp.topic = t.id \
                 join record r on r.topition = tp.id \
                 where c.name = $1 and t.name = $2 and tp.partition = $3 \
                 and r.producer_id = $4 and (r.attributes & 32) = 32",
                &[&cluster, &topic_name, &0i32, &producer_b.id],
            )
            .await?
            .try_get(0)?;

        assert_eq!(
            1, marker_count,
            "expected exactly one control-batch marker for producer B, got {marker_count}",
        );

        Ok(())
    }

    /// Once maintain_transactions sweep-aborts a timed-out transaction, it also fences the
    /// producer's epoch -- so a LATE real EndTxn(commit=true) for that same transaction (e.g.
    /// a slow producer that was declared dead but is actually still alive and finally calls
    /// commit) must be rejected with ProducerFenced, not acked as success -- the data was
    /// already irreversibly reported as aborted, so telling the caller their commit "worked"
    /// would be a lie, and ProducerFenced (rather than a generic InvalidTxnState) is what
    /// tells a real Kafka client library to stop retrying and rebuild its producer.
    #[tokio::test]
    async fn late_commit_after_sweep_abort_is_rejected() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "late_commit_after_sweep_abort_is_rejected",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"slow-producer").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        // the sweep declares this transaction dead and aborts it.
        storage
            .maintain_transactions(SystemTime::now() + Duration::from_secs(3600))
            .await?;

        let stage = storage.offset_stage(&topition).await?;
        assert_eq!(
            stage.high_watermark, stage.last_stable,
            "the sweep should have aborted the timed-out transaction"
        );

        // the "slow" producer, unaware it's been declared dead, finally calls commit for real.
        let late_commit_result = storage
            .txn_end(&transaction_id, producer.id, producer.epoch, true)
            .await?;

        assert_eq!(
            ErrorCode::ProducerFenced,
            late_commit_result,
            "a late commit() after a sweep-abort must be rejected as ProducerFenced (the \
             sweep fences the producer's epoch on timeout-abort), not acked as success",
        );

        Ok(())
    }

    async fn txn_status(
        storage: &Postgres,
        cluster: &str,
        transaction_id: &str,
        producer: &ProducerIdResponse,
    ) -> Result<Option<String>> {
        let c = storage.connection().await?;

        let row = c
            .query_one(
                "select txn_d.status \
                 from cluster c \
                 join producer p on p.cluster = c.id \
                 join producer_epoch pe on pe.producer = p.id \
                 join txn on txn.cluster = c.id and txn.producer = p.id \
                 join txn_detail txn_d on txn_d.\"transaction\" = txn.id \
                 and txn_d.producer_epoch = pe.id \
                 where c.name = $1 and txn.name = $2 and p.id = $3 and pe.epoch = $4",
                &[
                    &cluster.to_owned(),
                    &transaction_id.to_owned(),
                    &producer.id,
                    &producer.epoch,
                ],
            )
            .await?;

        row.try_get::<_, Option<String>>(0).map_err(Into::into)
    }

    /// Finalizing one epoch's transaction must delete only that epoch's txn_topition and
    /// txn_produce_offset bookkeeping. The delete_by_txn queries once matched every epoch of
    /// the (transaction, producer). Reproduced via an epoch-0 abort deferred in PREPARE_ABORT
    /// (re-init leaves non-BEGIN transactions alone, so the epoch bump keeps it deferred):
    /// its retained bookkeeping is what eventually tells the deferred abort which partitions
    /// get markers, so epoch 1's finalize wiping it loses those markers entirely.
    #[tokio::test]
    async fn txn_end_scopes_bookkeeping_delete_to_its_own_epoch() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "txn_end_scopes_bookkeeping_delete_to_its_own_epoch",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(2)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let partition_0 = Topition::new(topic_name.clone(), 0);
        let partition_1 = Topition::new(topic_name.clone(), 1);

        // pinning producer: an open transaction on partition 0 that epoch 0's abort will
        // defer behind.
        let txn_pin = alphanumeric_string(10);
        let producer_pin = storage
            .init_producer(Some(txn_pin.as_str()), 10_000_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_pin.clone(),
                producer_id: producer_pin.id,
                producer_epoch: producer_pin.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some(vec![0])),
                ],
            })
            .await?;

        let batch_pin = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"pin").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer_pin.id)
            .producer_epoch(producer_pin.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn_pin.as_str()), &partition_0, batch_pin)
            .await?;

        // epoch 0: produce on partition 0 behind the pin, then abort -- defers to
        // PREPARE_ABORT, keeping its bookkeeping rows.
        let txn_a = alphanumeric_string(10);
        let epoch0 = storage
            .init_producer(Some(txn_a.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_a.clone(),
                producer_id: epoch0.id,
                producer_epoch: epoch0.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some(vec![0])),
                ],
            })
            .await?;

        let batch_epoch0 = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"a").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(epoch0.id)
            .producer_epoch(epoch0.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn_a.as_str()), &partition_0, batch_epoch0)
            .await?;

        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&txn_a, epoch0.id, epoch0.epoch, false)
                .await?
        );
        assert_eq!(
            Some("PREPARE_ABORT".to_owned()),
            txn_status(&storage, &cluster, &txn_a, &epoch0).await?,
            "epoch 0's abort should defer behind the pinning transaction",
        );

        // reconnect: PREPARE_ABORT is not BEGIN, so re-init bumps the epoch without touching
        // the deferred transaction.
        let epoch1 = storage
            .init_producer(Some(txn_a.as_str()), 10_000, Some(-1), Some(-1))
            .await?;
        assert_eq!(epoch0.id, epoch1.id);
        assert_eq!(epoch0.epoch + 1, epoch1.epoch);

        // epoch 1: transact on partition 1 (no overlap with the pin) and abort -- finalizes
        // immediately, running the bookkeeping deletes.
        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_a.clone(),
                producer_id: epoch1.id,
                producer_epoch: epoch1.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some(vec![1])),
                ],
            })
            .await?;

        let batch_epoch1 = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"b").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(epoch1.id)
            .producer_epoch(epoch1.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn_a.as_str()), &partition_1, batch_epoch1)
            .await?;

        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&txn_a, epoch1.id, epoch1.epoch, false)
                .await?
        );
        assert_eq!(
            Some("ABORTED".to_owned()),
            txn_status(&storage, &cluster, &txn_a, &epoch1).await?,
            "epoch 1 has no overlap and must finalize immediately, running the deletes",
        );

        let bookkeeping = |epoch: &ProducerIdResponse| {
            let storage = &storage;
            let cluster = &cluster;
            let txn_a = &txn_a;
            let (id, epoch) = (epoch.id, epoch.epoch);
            async move {
                let c = storage.connection().await?;
                let row = c
                    .query_one(
                        "select \
                         count(distinct txn_tp.id) as topitions, \
                         count(txn_po.txn_topition) as produce_offsets \
                         from cluster c \
                         join producer p on p.cluster = c.id \
                         join producer_epoch pe on pe.producer = p.id \
                         join txn on txn.cluster = c.id and txn.producer = p.id \
                         join txn_detail txn_d on txn_d.\"transaction\" = txn.id \
                         and txn_d.producer_epoch = pe.id \
                         join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id \
                         left join txn_produce_offset txn_po on txn_po.txn_topition = txn_tp.id \
                         where c.name = $1 and txn.name = $2 and p.id = $3 and pe.epoch = $4",
                        &[cluster, txn_a, &id, &epoch],
                    )
                    .await?;
                Ok::<_, Error>((row.try_get::<_, i64>(0)?, row.try_get::<_, i64>(1)?))
            }
        };

        assert_eq!(
            (0, 0),
            bookkeeping(&epoch1).await?,
            "epoch 1's own bookkeeping should be deleted by its finalize",
        );
        assert_eq!(
            (1, 1),
            bookkeeping(&epoch0).await?,
            "epoch 1's finalize must not delete the deferred epoch 0 transaction's \
             bookkeeping -- it is what tells the deferred abort which partitions get markers",
        );

        Ok(())
    }

    /// Stress the race end_in_tx's status guard exists to close: a genuinely concurrent
    /// live commit (as a real client's EndTxn) and the maintain_transactions sweep's abort,
    /// both racing txn_end on the exact same transaction. Recovered and adapted from an
    /// earlier, unmerged exploration (orhayat/fix/pg-txn-timeout-abort) that found this race
    /// before the guard existed; run here to confirm the guard actually holds under real
    /// concurrency, not just the sequential simulation above.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn race_concurrent_commit_and_abort() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) =
            storage_with_schema("race_concurrent_commit_and_abort", &CONNECTION, &cluster).await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let iterations = 100;
        let mut commit_wins = 0;
        let mut abort_wins = 0;
        let mut both_ok = 0;
        let mut both_err = 0;
        let mut anomalies: Vec<String> = vec![];

        for i in 0..iterations {
            let transaction_id = alphanumeric_string(10);

            let producer = storage
                .init_producer(Some(transaction_id.as_str()), 60_000, Some(-1), Some(-1))
                .await?;

            _ = storage
                .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                    transaction_id: transaction_id.clone(),
                    producer_id: producer.id,
                    producer_epoch: producer.epoch,
                    topics: vec![
                        AddPartitionsToTxnTopic::default()
                            .name(topic_name.clone())
                            .partitions(Some((0..num_partitions).collect())),
                    ],
                })
                .await?;

            let batch = Batch::builder()
                .record(Record::builder().value(Bytes::from_static(b"v").into()))
                .attributes(BatchAttribute::default().transaction(true).into())
                .producer_id(producer.id)
                .producer_epoch(producer.epoch)
                .base_sequence(0)
                .build()
                .and_then(TryInto::try_into)?;

            _ = storage
                .produce(Some(transaction_id.as_str()), &topition, batch)
                .await?;

            let wm_before = storage.offset_stage(&topition).await?.high_watermark;

            let (s1, s2) = (storage.clone(), storage.clone());
            let (t1, t2) = (transaction_id.clone(), transaction_id.clone());
            let (pid, ep) = (producer.id, producer.epoch);

            let commit = tokio::spawn(async move { s1.txn_end(&t1, pid, ep, true).await });
            let abort = tokio::spawn(async move { s2.txn_end(&t2, pid, ep, false).await });

            let r_commit = commit.await.expect("commit task panicked");
            let r_abort = abort.await.expect("abort task panicked");

            let wm_after = storage.offset_stage(&topition).await?.high_watermark;
            let markers = wm_after - wm_before;
            let status = txn_status(&storage, &cluster, &transaction_id, &producer).await?;

            match (r_commit.is_ok(), r_abort.is_ok()) {
                (true, false) => commit_wins += 1,
                (false, true) => abort_wins += 1,
                (true, true) => both_ok += 1,
                (false, false) => both_err += 1,
            }

            // The end_in_tx status guard makes the loser of the race a clean
            // Ok(ErrorCode::None) no-op too (matching how Kafka treats a duplicate
            // EndTxn), so both calls returning Ok is expected -- it no longer signals
            // which side "won". The real invariants: exactly one control marker ever
            // lands, and the transaction settles into a definite terminal state.
            let consistent =
                status.as_deref() == Some("COMMITTED") || status.as_deref() == Some("ABORTED");
            if markers != 1 || !consistent {
                anomalies.push(format!(
                    "iter {i}: markers={markers} commit={r_commit:?} abort={r_abort:?} status={status:?}"
                ));
            }
        }

        eprintln!(
            "summary over {iterations}: commit_wins={commit_wins} abort_wins={abort_wins} both_ok={both_ok} both_err={both_err} anomalies={}",
            anomalies.len()
        );

        assert!(
            anomalies.is_empty(),
            "{} anomalies (expected exactly 1 marker + status matching the winner every time):\n{}",
            anomalies.len(),
            anomalies.join("\n"),
        );

        Ok(())
    }

    /// The actual data-safety guarantee fencing exists for: once maintain_transactions
    /// sweep-aborts a timed-out transaction, a still-alive "zombie" producer trying to send
    /// MORE data under its old epoch must be rejected outright -- not silently accepted and
    /// later delivered to read_committed consumers as if it were ordinary committed data.
    #[tokio::test]
    async fn sweep_fenced_producer_zombie_produce_is_rejected() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "sweep_fenced_producer_zombie_produce_is_rejected",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"before-sweep").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        // the sweep declares this producer's transaction dead -- and must fence it.
        storage
            .maintain_transactions(SystemTime::now() + Duration::from_secs(3600))
            .await?;

        let stage_before_zombie_write = storage.offset_stage(&topition).await?;

        // the "zombie" producer, still alive, tries to send more data under its old epoch.
        let zombie_batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"zombie-write").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(1)
            .build()
            .and_then(TryInto::try_into)?;

        let zombie_result = storage
            .produce(Some(transaction_id.as_str()), &topition, zombie_batch)
            .await;

        assert!(
            matches!(
                zombie_result,
                Err(Error::Api(ErrorCode::InvalidProducerEpoch))
            ),
            "a zombie produce under the pre-sweep epoch must be rejected as \
             InvalidProducerEpoch, got {zombie_result:?}",
        );

        let stage_after_zombie_write = storage.offset_stage(&topition).await?;

        assert_eq!(
            stage_before_zombie_write.high_watermark, stage_after_zombie_write.high_watermark,
            "the rejected zombie write must not have been appended to the log",
        );

        Ok(())
    }

    /// A producer fenced by the sweep must still be able to recover normally -- reconnecting
    /// via InitProducerId(-1, -1) for the same transactional.id gets a fresh epoch strictly
    /// after the sweep's fence, and can then produce/commit normally under it.
    #[tokio::test]
    async fn sweep_fenced_producer_can_reconnect_and_produce_normally() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "sweep_fenced_producer_can_reconnect_and_produce_normally",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"before-sweep").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        storage
            .maintain_transactions(SystemTime::now() + Duration::from_secs(3600))
            .await?;

        let reconnected = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        assert_eq!(
            producer.id, reconnected.id,
            "reconnect must keep the same stable producer_id"
        );
        assert!(
            reconnected.epoch > producer.epoch,
            "reconnect must return an epoch strictly after the sweep's fence (was {}, now {})",
            producer.epoch,
            reconnected.epoch,
        );

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: reconnected.id,
                producer_epoch: reconnected.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some((0..num_partitions).collect())),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"after-reconnect").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(reconnected.id)
            .producer_epoch(reconnected.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&transaction_id, reconnected.id, reconnected.epoch, true)
                .await?,
            "the reconnected producer's transaction must commit normally",
        );

        Ok(())
    }

    /// EndTxn for a producer id that was never issued must be UnknownProducerId -- the
    /// identity check runs before any transaction-state lookup, so this cannot be
    /// misreported as a transaction-state problem.
    #[tokio::test]
    async fn txn_end_for_unknown_producer_is_unknown_producer_id() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "txn_end_for_unknown_producer_is_unknown_producer_id",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        assert_eq!(
            ErrorCode::UnknownProducerId,
            storage
                .txn_end(alphanumeric_string(10).as_str(), i64::MAX, 0, false)
                .await?
        );

        Ok(())
    }

    /// EndTxn carrying an epoch NEWER than the broker ever issued is a protocol violation
    /// (the client invented an epoch), reported as InvalidProducerEpoch -- distinct from
    /// ProducerFenced, which tells a stale producer a NEWER epoch exists and it should
    /// rebuild; here nothing newer exists and retrying with a rebuilt producer won't help.
    #[tokio::test]
    async fn txn_end_with_future_epoch_is_invalid_producer_epoch() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "txn_end_with_future_epoch_is_invalid_producer_epoch",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        assert_eq!(
            ErrorCode::InvalidProducerEpoch,
            storage
                .txn_end(&transaction_id, producer.id, producer.epoch + 1, true)
                .await?
        );

        Ok(())
    }

    /// A retried sweep-abort (the next maintain tick, or another broker instance sharing
    /// the database) carries the victim's original epoch, which the first call's fencing
    /// made stale -- so the retry must be stopped at the identity check as ProducerFenced,
    /// bumping no second epoch and writing no second control marker.
    #[tokio::test]
    async fn sweep_abort_retry_is_fenced_and_does_not_double_bump() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "sweep_abort_retry_is_fenced_and_does_not_double_bump",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        // pinning producer: an older open transaction the victim's abort will defer behind,
        // so the victim sits in PREPARE_ABORT when the retry arrives.
        let txn_pin = alphanumeric_string(10);
        let producer_pin = storage
            .init_producer(Some(txn_pin.as_str()), 10_000_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_pin.clone(),
                producer_id: producer_pin.id,
                producer_epoch: producer_pin.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some(vec![0])),
                ],
            })
            .await?;

        let batch_pin = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"pin").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer_pin.id)
            .producer_epoch(producer_pin.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn_pin.as_str()), &topition, batch_pin)
            .await?;

        let txn_victim = alphanumeric_string(10);
        let victim = storage
            .init_producer(Some(txn_victim.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: txn_victim.clone(),
                producer_id: victim.id,
                producer_epoch: victim.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some(vec![0])),
                ],
            })
            .await?;

        let batch_victim = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"v").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(victim.id)
            .producer_epoch(victim.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn_victim.as_str()), &topition, batch_victim)
            .await?;

        assert_eq!(
            ErrorCode::None,
            storage
                .abort_timed_out(&txn_victim, victim.id, victim.epoch)
                .await?
        );
        assert_eq!(
            Some("PREPARE_ABORT".to_owned()),
            txn_status(&storage, &cluster, &txn_victim, &victim).await?,
            "the victim's abort should defer behind the pinning transaction",
        );

        assert_eq!(
            ErrorCode::ProducerFenced,
            storage
                .abort_timed_out(&txn_victim, victim.id, victim.epoch)
                .await?,
            "the retry carries the epoch the first call fenced",
        );

        let c = storage.connection().await?;

        let current_epoch: i16 = c
            .query_one(
                "select max(pe.epoch) from cluster c \
                 join producer p on p.cluster = c.id \
                 join producer_epoch pe on pe.producer = p.id \
                 where c.name = $1 and p.id = $2",
                &[&cluster, &victim.id],
            )
            .await?
            .try_get(0)?;

        assert_eq!(
            victim.epoch + 1,
            current_epoch,
            "two sweep-abort calls must fence exactly once",
        );

        let marker_count: i64 = c
            .query_one(
                "select count(*) from cluster c \
                 join topic t on t.cluster = c.id \
                 join topition tp on tp.topic = t.id \
                 join record r on r.topition = tp.id \
                 where c.name = $1 and t.name = $2 and tp.partition = $3 \
                 and r.producer_id = $4 and (r.attributes & 32) = 32",
                &[&cluster, &topic_name, &0i32, &victim.id],
            )
            .await?
            .try_get(0)?;

        assert_eq!(
            1, marker_count,
            "the fenced retry must not write a second control marker",
        );

        Ok(())
    }

    /// InitProducerId API versions <= 2 have no wire representation for producer_id/epoch at
    /// all, so they decode as (None, None) -- must be treated as a fresh-epoch request, not
    /// panic.
    #[tokio::test]
    async fn init_producer_old_api_version_is_treated_as_fresh() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "init_producer_old_api_version_is_treated_as_fresh",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let transaction_id = alphanumeric_string(10);

        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
            .await?;

        assert_eq!(ErrorCode::None, producer.error);

        Ok(())
    }

    /// A v3+ client presenting its current, still-valid (producer_id, producer_epoch) -- the
    /// KIP-360 recovery shape -- must be validated against the record and, once confirmed,
    /// bumped to a new epoch exactly like a fresh (-1, -1) request.
    #[tokio::test]
    async fn init_producer_recovery_with_current_epoch_succeeds() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "init_producer_recovery_with_current_epoch_succeeds",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let transaction_id = alphanumeric_string(10);

        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        let recovered = storage
            .init_producer(
                Some(transaction_id.as_str()),
                10_000,
                Some(producer.id),
                Some(producer.epoch),
            )
            .await?;

        assert_eq!(ErrorCode::None, recovered.error);
        assert_eq!(producer.id, recovered.id);
        assert!(
            recovered.epoch > producer.epoch,
            "a validated recovery request must still bump the epoch (was {}, now {})",
            producer.epoch,
            recovered.epoch,
        );

        Ok(())
    }

    /// A v3+ client presenting a STALE (producer_id, producer_epoch) -- exactly what happens
    /// after maintain_transactions' sweep fences a timed-out producer -- must be rejected as
    /// ProducerFenced, not silently granted a new epoch (and not panic).
    #[tokio::test]
    async fn init_producer_recovery_with_stale_epoch_is_fenced() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "init_producer_recovery_with_stale_epoch_is_fenced",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let transaction_id = alphanumeric_string(10);

        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        // someone else bumps the epoch (a reconnect, or the sweep fencing this producer),
        // making `producer`'s epoch stale.
        _ = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        let stale_recovery = storage
            .init_producer(
                Some(transaction_id.as_str()),
                10_000,
                Some(producer.id),
                Some(producer.epoch),
            )
            .await?;

        assert_eq!(
            ErrorCode::ProducerFenced,
            stale_recovery.error,
            "a recovery request carrying a stale epoch must be rejected as ProducerFenced"
        );

        Ok(())
    }

    /// The timeout sweep fences a producer by adding an epoch that has no transaction. A
    /// claim of the fenced epoch must be rejected, not bumped past the sweep's epoch.
    #[tokio::test]
    async fn init_producer_recovery_after_sweep_is_fenced() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "init_producer_recovery_after_sweep_is_fenced",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic_name.clone())
                        .partitions(Some(vec![0])),
                ],
            })
            .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"abandoned").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        storage
            .maintain_transactions(SystemTime::now() + Duration::from_secs(3600))
            .await?;

        let fenced = storage
            .init_producer(
                Some(transaction_id.as_str()),
                10_000,
                Some(producer.id),
                Some(producer.epoch),
            )
            .await?;

        assert_eq!(ErrorCode::ProducerFenced, fenced.error);
        assert_eq!((-1, -1), (fenced.id, fenced.epoch));

        let fresh = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        assert_eq!(ErrorCode::None, fresh.error);
        assert_eq!(producer.id, fresh.id);
        assert_eq!(producer.epoch + 2, fresh.epoch);

        Ok(())
    }

    /// Returns a [`Postgres`] storage engine for a new cluster with a registered broker, and
    /// one topic with one partition. Returns `None` when the server can't be reached.
    async fn storage_with_topic(test: &str) -> Result<Option<(Postgres, Topition)>> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(test, &CONNECTION, &cluster).await? else {
            return Ok(None);
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        Ok(Some((storage, Topition::new(topic_name, 0))))
    }

    /// Sets the epoch of each `producer_epoch` row of `producer` to `epoch`. A producer
    /// that has one epoch then has only `epoch`, without 32k InitProducerId requests.
    async fn seed_producer_epoch(storage: &Postgres, producer: i64, epoch: i16) -> Result<()> {
        _ = storage
            .connection()
            .await?
            .execute(
                "update producer_epoch set epoch = $1 where producer = $2",
                &[&epoch, &producer],
            )
            .await?;

        Ok(())
    }

    fn idempotent_batch(producer: &ProducerIdResponse) -> Result<deflated::Batch> {
        Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"idempotent").into()))
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)
            .map_err(Into::into)
    }

    /// An epoch below the limit bumps. At the limit, a fresh InitProducerId moves the
    /// transactional ID to a new producer ID at epoch 0, and the previous producer ID is
    /// fenced, both for a claim and for a produce.
    #[tokio::test]
    async fn init_producer_exhausted_epoch_moves_to_a_new_producer_id() -> Result<()> {
        let Some((storage, topition)) =
            storage_with_topic("init_producer_exhausted_epoch_moves_to_a_new_producer_id").await?
        else {
            return Ok(());
        };

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
            .await?;
        seed_producer_epoch(&storage, producer.id, 32765).await?;

        let bumped = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
            .await?;
        assert_eq!(
            (ErrorCode::None, producer.id, 32766),
            (bumped.error, bumped.id, bumped.epoch)
        );

        let rotated = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
            .await?;
        assert_eq!((ErrorCode::None, 0), (rotated.error, rotated.epoch));
        assert_ne!(producer.id, rotated.id);

        assert_eq!(
            ProducerIdResponse::failed(ErrorCode::ProducerFenced),
            storage
                .init_producer(
                    Some(transaction_id.as_str()),
                    10_000,
                    Some(bumped.id),
                    Some(bumped.epoch),
                )
                .await?
        );

        assert_matches!(
            storage
                .produce(None, &topition, idempotent_batch(&bumped)?)
                .await,
            Err(Error::Api(ErrorCode::ProducerFenced))
        );
        assert_eq!(
            0,
            storage
                .produce(None, &topition, idempotent_batch(&rotated)?)
                .await?
        );

        let claimed = storage
            .init_producer(
                Some(transaction_id.as_str()),
                10_000,
                Some(rotated.id),
                Some(rotated.epoch),
            )
            .await?;
        assert_eq!(
            (ErrorCode::None, rotated.id, 1),
            (claimed.error, claimed.id, claimed.epoch)
        );

        Ok(())
    }

    /// A claim of an exhausted epoch moves the transactional ID to a new producer ID.
    #[tokio::test]
    async fn init_producer_exhausted_claim_moves_to_a_new_producer_id() -> Result<()> {
        let Some((storage, _)) =
            storage_with_topic("init_producer_exhausted_claim_moves_to_a_new_producer_id").await?
        else {
            return Ok(());
        };

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
            .await?;
        seed_producer_epoch(&storage, producer.id, 32766).await?;

        let rotated = storage
            .init_producer(
                Some(transaction_id.as_str()),
                10_000,
                Some(producer.id),
                Some(32766),
            )
            .await?;
        assert_eq!((ErrorCode::None, 0), (rotated.error, rotated.epoch));
        assert_ne!(producer.id, rotated.id);

        Ok(())
    }

    /// An exhausted epoch with a prepared transaction answers ConcurrentTransactions, and
    /// keeps the producer ID.
    #[tokio::test]
    async fn init_producer_exhausted_epoch_with_a_prepared_transaction_is_concurrent() -> Result<()>
    {
        let Some((storage, _)) = storage_with_topic(
            "init_producer_exhausted_epoch_with_a_prepared_transaction_is_concurrent",
        )
        .await?
        else {
            return Ok(());
        };

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
            .await?;
        seed_producer_epoch(&storage, producer.id, 32766).await?;

        _ = storage
            .connection()
            .await?
            .execute(
                "update txn_detail set status = 'PREPARE_ABORT' \
                 from cluster c, txn \
                 where c.name = $1 and txn.cluster = c.id and txn.name = $2 \
                 and txn_detail.\"transaction\" = txn.id",
                &[&storage.cluster, &transaction_id],
            )
            .await?;

        assert_eq!(
            ProducerIdResponse::failed(ErrorCode::ConcurrentTransactions),
            storage
                .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
                .await?
        );

        // The producer keeps its ID and epoch: its claim is not fenced.
        assert_eq!(
            ProducerIdResponse::failed(ErrorCode::ConcurrentTransactions),
            storage
                .init_producer(
                    Some(transaction_id.as_str()),
                    10_000,
                    Some(producer.id),
                    Some(32766),
                )
                .await?
        );

        Ok(())
    }

    /// The timeout sweep fences a producer at epoch 32766 with epoch 32767, the largest
    /// epoch. The next InitProducerId moves the transactional ID to a new producer ID.
    #[tokio::test]
    async fn init_producer_after_a_sweep_fence_at_the_limit_moves_to_a_new_producer_id()
    -> Result<()> {
        let Some((storage, topition)) = storage_with_topic(
            "init_producer_after_a_sweep_fence_at_the_limit_moves_to_a_new_producer_id",
        )
        .await?
        else {
            return Ok(());
        };

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
            .await?;

        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.clone(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topition.topic().to_owned())
                        .partitions(Some(vec![0])),
                ],
            })
            .await?;

        seed_producer_epoch(&storage, producer.id, 32766).await?;

        let batch = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"abandoned").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(32766)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(transaction_id.as_str()), &topition, batch)
            .await?;

        storage
            .maintain_transactions(SystemTime::now() + Duration::from_secs(3600))
            .await?;

        let rotated = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
            .await?;
        assert_eq!((ErrorCode::None, 0), (rotated.error, rotated.epoch));
        assert_ne!(producer.id, rotated.id);

        Ok(())
    }

    fn transactional_batch(producer: &ProducerIdResponse) -> Result<deflated::Batch> {
        Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"transactional").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)
            .map_err(Into::into)
    }

    /// Opens a transaction for `producer` on `topition`, and produces one record in it.
    async fn open_transaction(
        storage: &Postgres,
        transaction_id: &str,
        producer: &ProducerIdResponse,
        topition: &Topition,
    ) -> Result<()> {
        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: transaction_id.to_owned(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topition.topic().to_owned())
                        .partitions(Some(vec![topition.partition()])),
                ],
            })
            .await?;

        _ = storage
            .produce(
                Some(transaction_id),
                topition,
                transactional_batch(producer)?,
            )
            .await?;

        Ok(())
    }

    /// Only the transaction of the current epoch holds back the move, as in Kafka. A
    /// transaction left prepared on an older epoch does not.
    #[tokio::test]
    async fn init_producer_older_prepared_transaction_does_not_hold_back_the_move() -> Result<()> {
        let Some((storage, _)) = storage_with_topic(
            "init_producer_older_prepared_transaction_does_not_hold_back_the_move",
        )
        .await?
        else {
            return Ok(());
        };

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
            .await?;
        _ = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
            .await?;

        let connection = storage.connection().await?;

        _ = connection
            .execute(
                "update producer_epoch \
                 set epoch = case when epoch = 0 then 100 else 32766 end \
                 where producer = $1",
                &[&producer.id],
            )
            .await?;

        _ = connection
            .execute(
                "update txn_detail set status = 'PREPARE_ABORT' \
                 from producer_epoch pe \
                 where txn_detail.producer_epoch = pe.id and pe.producer = $1 and pe.epoch = 100",
                &[&producer.id],
            )
            .await?;

        let moved = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
            .await?;
        assert_eq!((ErrorCode::None, 0), (moved.error, moved.epoch));
        assert_ne!(producer.id, moved.id);

        Ok(())
    }

    /// A producer that restarts in the middle of a transaction at the exhausted epoch:
    /// InitProducerId aborts the transaction, then moves to a new producer ID.
    #[tokio::test]
    async fn init_producer_open_transaction_at_the_exhausted_epoch_is_aborted_then_moved()
    -> Result<()> {
        let Some((storage, topition)) = storage_with_topic(
            "init_producer_open_transaction_at_the_exhausted_epoch_is_aborted_then_moved",
        )
        .await?
        else {
            return Ok(());
        };

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
            .await?;
        seed_producer_epoch(&storage, producer.id, 32766).await?;

        let exhausted = ProducerIdResponse {
            epoch: 32766,
            ..producer
        };
        open_transaction(&storage, &transaction_id, &exhausted, &topition).await?;

        let moved = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
            .await?;
        assert_eq!((ErrorCode::None, 0), (moved.error, moved.epoch));
        assert_ne!(producer.id, moved.id);

        let stage = storage.offset_stage(&topition).await?;
        assert_eq!(stage.high_watermark, stage.last_stable);

        Ok(())
    }

    /// When the abort of the open transaction waits behind an older open transaction on
    /// the same partition, the answer is CONCURRENT_TRANSACTIONS, and the producer is
    /// fenced, as in Kafka. The move happens once the older transaction ends, which
    /// finishes the abort.
    #[tokio::test]
    async fn init_producer_deferred_abort_at_the_exhausted_epoch_fences_then_moves() -> Result<()> {
        let Some((storage, topition)) = storage_with_topic(
            "init_producer_deferred_abort_at_the_exhausted_epoch_fences_then_moves",
        )
        .await?
        else {
            return Ok(());
        };

        let older_transaction_id = alphanumeric_string(10);
        let older = storage
            .init_producer(Some(older_transaction_id.as_str()), 10_000, None, None)
            .await?;
        open_transaction(&storage, &older_transaction_id, &older, &topition).await?;

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
            .await?;
        seed_producer_epoch(&storage, producer.id, 32766).await?;

        let exhausted = ProducerIdResponse {
            epoch: 32766,
            ..producer
        };
        open_transaction(&storage, &transaction_id, &exhausted, &topition).await?;

        assert_eq!(
            ProducerIdResponse::failed(ErrorCode::ConcurrentTransactions),
            storage
                .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
                .await?
        );

        let zombie = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"zombie").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(exhausted.id)
            .producer_epoch(exhausted.epoch)
            .base_sequence(1)
            .build()
            .and_then(TryInto::try_into)?;

        assert_matches!(
            storage
                .produce(Some(transaction_id.as_str()), &topition, zombie)
                .await,
            Err(Error::Api(ErrorCode::ProducerFenced))
        );

        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&older_transaction_id, older.id, older.epoch, true)
                .await?
        );

        let moved = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
            .await?;
        assert_eq!((ErrorCode::None, 0), (moved.error, moved.epoch));
        assert_ne!(producer.id, moved.id);

        let stage = storage.offset_stage(&topition).await?;
        assert_eq!(stage.high_watermark, stage.last_stable);

        Ok(())
    }

    /// After a move, a whole transaction runs on the new producer ID, and a transactional
    /// produce from the previous producer ID is fenced.
    #[tokio::test]
    async fn init_producer_transaction_after_a_move() -> Result<()> {
        let Some((storage, topition)) =
            storage_with_topic("init_producer_transaction_after_a_move").await?
        else {
            return Ok(());
        };

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
            .await?;
        seed_producer_epoch(&storage, producer.id, 32766).await?;

        let moved = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, None, None)
            .await?;
        assert_eq!((ErrorCode::None, 0), (moved.error, moved.epoch));

        let previous = ProducerIdResponse {
            epoch: 32766,
            ..producer
        };
        assert_matches!(
            storage
                .produce(
                    Some(transaction_id.as_str()),
                    &topition,
                    transactional_batch(&previous)?
                )
                .await,
            Err(Error::Api(ErrorCode::ProducerFenced))
        );

        open_transaction(&storage, &transaction_id, &moved, &topition).await?;

        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(&transaction_id, moved.id, moved.epoch, true)
                .await?
        );

        let stage = storage.offset_stage(&topition).await?;
        assert_eq!(stage.high_watermark, stage.last_stable);

        Ok(())
    }

    /// Concurrent claims of the same producer and epoch bump the epoch once: one claim
    /// succeeds, and every other claim is fenced against the bumped epoch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn init_producer_concurrent_claims_bump_once() -> Result<()> {
        const CLAIMS: usize = 8;
        const ROUNDS: usize = 10;

        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "init_producer_concurrent_claims_bump_once",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        // Open a pooled connection for each claim first, so that the claims run at the
        // same time instead of one after another while the pool connects.
        drop(future::try_join_all((0..CLAIMS).map(|_| storage.connection())).await?);

        let transaction_id = alphanumeric_string(10);

        let mut producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        for _ in 0..ROUNDS {
            let claims = (0..CLAIMS)
                .map(|_| {
                    let storage = storage.clone();
                    let transaction_id = transaction_id.clone();

                    tokio::spawn(async move {
                        storage
                            .init_producer(
                                Some(transaction_id.as_str()),
                                10_000,
                                Some(producer.id),
                                Some(producer.epoch),
                            )
                            .await
                    })
                })
                .collect::<Vec<_>>();

            let mut responses = vec![];

            for claim in claims {
                responses.push(claim.await.expect("claim task panicked"));
            }

            let bumped = responses
                .iter()
                .filter_map(|response| response.as_ref().ok())
                .filter(|response| response.error == ErrorCode::None)
                .copied()
                .collect::<Vec<_>>();

            let fenced = responses
                .iter()
                .filter_map(|response| response.as_ref().ok())
                .filter(|response| response.error == ErrorCode::ProducerFenced)
                .count();

            assert_eq!(1, bumped.len(), "{responses:?}");
            assert_eq!(CLAIMS - 1, fenced, "{responses:?}");
            assert_eq!(
                (producer.id, producer.epoch + 1),
                (bumped[0].id, bumped[0].epoch)
            );

            producer = bumped[0];
        }

        Ok(())
    }

    /// A malformed InitProducerId request -- one of producer_id/producer_epoch present
    /// without the other -- is not a valid shape under any protocol version and must be
    /// rejected, not panic.
    #[tokio::test]
    async fn init_producer_malformed_partial_fields_is_rejected() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "init_producer_malformed_partial_fields_is_rejected",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let transaction_id = alphanumeric_string(10);

        let response = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(42), None)
            .await?;

        assert_eq!(ErrorCode::InvalidRequest, response.error);

        Ok(())
    }

    /// AddPartitionsToTxn is idempotent in Kafka: a partition already in the transaction is
    /// a no-op, not an error. Clients do re-send it -- a retry, or a produce racing the
    /// first add -- and the unique (txn_detail, topition) constraint turned that into a
    /// database error surfacing as a broken connection mid-transaction.
    #[tokio::test]
    async fn txn_add_partitions_is_idempotent() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) =
            storage_with_schema("txn_add_partitions_is_idempotent", &CONNECTION, &cluster).await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        let num_partitions = 1;

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(num_partitions)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;

        let request = || TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: transaction_id.clone(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: vec![
                AddPartitionsToTxnTopic::default()
                    .name(topic_name.clone())
                    .partitions(Some((0..num_partitions).collect())),
            ],
        };

        for attempt in 1..=2 {
            let response = storage
                .txn_add_partitions(request())
                .await
                .inspect_err(|err| {
                    panic!("add #{attempt} of the same partition must succeed, got {err:?}")
                })?;

            for topic in response.zero_to_three() {
                for partition in topic.results_by_partition.as_deref().unwrap_or_default() {
                    assert_eq!(
                        i16::from(ErrorCode::None),
                        partition.partition_error_code,
                        "add #{attempt} of the same partition must report no error",
                    );
                }
            }
        }

        Ok(())
    }

    /// Fetch must serve committed records even while an unrelated database transaction is
    /// open. Produce serializes offset allocation per partition through the watermark row
    /// lock (held to commit), so every visible row below the committed high watermark is
    /// final -- there is no in-flight insert a fetch could incorrectly stream past, and no
    /// reason to withhold rows waiting on the global transaction horizon (xmin).
    #[tokio::test]
    async fn fetch_serves_committed_records_despite_concurrent_transaction() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "fetch_serves_committed_records_despite_concurrent_transaction",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        // an unrelated transaction with an assigned xid, held open across the produce
        // and fetch -- as any concurrent produce to another partition, sweep tick, or
        // slow client elsewhere in the database would hold one
        let mut held_connection = storage.connection().await?;
        let held = held_connection.transaction().await?;
        _ = held.query_one("select pg_current_xact_id()", &[]).await?;

        let batch = Batch::builder()
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"a").into())
                    .offset_delta(0),
            )
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"b").into())
                    .offset_delta(1),
            )
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"c").into())
                    .offset_delta(2),
            )
            .last_offset_delta(2)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage.produce(None, &topition, batch).await?;

        let batches = storage
            .fetch_until_deadline(
                &topition,
                0,
                1,
                8 * 1024,
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_secs(5),
            )
            .await
            .map(|read| read.batches)?;

        held.rollback().await?;

        let records = batches.into_iter().try_fold(Vec::new(), |mut acc, batch| {
            Batch::try_from(batch).map(|inflated| {
                acc.extend(inflated.records);
                acc
            })
        })?;

        assert_eq!(
            3,
            records.len(),
            "all committed records must be served regardless of unrelated open transactions",
        );

        Ok(())
    }

    /// The watermark insert selects the topition row that the topition insert creates, so
    /// `create_topic` must finish the second before sending the first. When it sent both
    /// together, a connection holding only the watermark statement in its statement cache
    /// sent the watermark insert first; it matched no rows without an error, and the topic
    /// had no watermark, so every produce to it failed with `UnknownTopicOrPartition`. No
    /// current caller leaves the cache in that state; this test sets it up directly.
    #[tokio::test]
    async fn create_topic_writes_the_watermark_when_only_its_statement_is_cached() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "create_topic_writes_the_watermark_when_only_its_statement_is_cached",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        {
            let c = storage.connection().await?;
            _ = c
                .prepare_cached(storage.sql_lookup("watermark_insert.sql")?)
                .await?;
        }

        assert_eq!(
            1,
            storage.pool.status().size,
            "one connection holds the cache"
        );

        let topic_name = alphanumeric_string(15);

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let stage = storage.offset_stage(&Topition::new(topic_name, 0)).await?;

        assert_eq!(0, stage.high_watermark);

        Ok(())
    }

    /// Deleting one topic must leave every other topic in the cluster untouched.
    /// `topic_delete_by.sql` narrowed the delete on `topic.cluster` instead of
    /// `topic.id`, so the subquery only established that the named topic existed
    /// and the delete then matched every topic in the cluster -- taking their
    /// topitions, records and headers with it through `on delete cascade`.
    #[tokio::test]
    async fn delete_topic_leaves_the_other_topics_in_the_cluster_intact() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "delete_topic_leaves_the_other_topics_in_the_cluster_intact",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let doomed = alphanumeric_string(15);
        let survivor = alphanumeric_string(15);

        for name in [doomed.clone(), survivor.clone()] {
            _ = storage
                .create_topic(
                    CreatableTopic::default()
                        .name(name)
                        .num_partitions(1)
                        .replication_factor(0)
                        .assignments(Some([].into()))
                        .configs(Some([].into())),
                    false,
                )
                .await?;
        }

        // the survivor holds a record, so the cascade is observable as data loss
        // and not just as a missing topic row
        let topition = Topition::new(survivor.clone(), 0);

        let batch = Batch::builder()
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"a").into())
                    .offset_delta(0),
            )
            .last_offset_delta(0)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage.produce(None, &topition, batch).await?;

        assert_eq!(
            ErrorCode::None,
            storage.delete_topic(&TopicId::Name(doomed.clone())).await?,
        );

        let batches = storage
            .fetch_until_deadline(
                &topition,
                0,
                1,
                8 * 1024,
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_secs(5),
            )
            .await
            .map(|read| read.batches)?;

        let records = batches.into_iter().try_fold(Vec::new(), |mut acc, batch| {
            Batch::try_from(batch).map(|inflated| {
                acc.extend(inflated.records);
                acc
            })
        })?;

        assert_eq!(
            1,
            records.len(),
            "deleting {doomed} must not cascade into the records of {survivor}",
        );

        assert_eq!(
            ErrorCode::None,
            storage
                .delete_topic(&TopicId::Name(survivor.clone()))
                .await?,
            "deleting {doomed} must leave {survivor} present and deletable",
        );

        Ok(())
    }

    /// Record headers are an ordered list and may repeat a key. header_fetch.sql
    /// had no `order by` and the only index was the primary key
    /// (topition, offset_id, k), so postgres served headers from that index in
    /// key order, and the key being part of the primary key meant a repeated key
    /// could not be stored at all.
    ///
    /// The produce order below is the one librdkafka 0073_headers uses; sorted by
    /// key it starts with "empty", which is exactly what 0073 reported at index 0.
    #[tokio::test]
    async fn headers_keep_produce_order_and_allow_repeated_keys() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "headers_keep_produce_order_and_allow_repeated_keys",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic_name.clone(), 0);

        let keys = [
            "msgid", "static", "multi", "multi", "multi", "null", "empty",
        ];

        let batch = Batch::builder()
            .record(
                keys.iter().enumerate().fold(
                    Record::builder()
                        .value(Bytes::from_static(b"a").into())
                        .offset_delta(0),
                    |builder, (ordinal, key)| {
                        builder.header(
                            Header::builder()
                                .key(Bytes::copy_from_slice(key.as_bytes()))
                                .value(Bytes::from(format!("{key}-{ordinal}"))),
                        )
                    },
                ),
            )
            .last_offset_delta(0)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage.produce(None, &topition, batch).await?;

        let batches = storage
            .fetch_until_deadline(
                &topition,
                0,
                1,
                8 * 1024,
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_secs(5),
            )
            .await
            .map(|read| read.batches)?;

        let records = batches.into_iter().try_fold(Vec::new(), |mut acc, batch| {
            Batch::try_from(batch).map(|inflated| {
                acc.extend(inflated.records);
                acc
            })
        })?;

        let fetched = records
            .first()
            .map(|record| {
                record
                    .headers
                    .iter()
                    .map(|header| {
                        header
                            .key
                            .as_deref()
                            .map(|key| String::from_utf8_lossy(key).into_owned())
                            .unwrap_or_default()
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        assert_eq!(
            keys.iter().map(|key| key.to_string()).collect::<Vec<_>>(),
            fetched,
            "headers must be served in produce order, repeats included",
        );

        Ok(())
    }

    /// A storage on a new cluster holding one empty single-partition topic, or `None` when
    /// there is no postgres to connect to.
    async fn storage_with_empty_topic() -> Result<Option<(Postgres, Topition)>> {
        let cluster = alphanumeric_string(15);

        let Some(storage) =
            storage_with_schema("storage_with_empty_topic", &CONNECTION, &cluster).await?
        else {
            return Ok(None);
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster,
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);

        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        Ok(Some((storage, Topition::new(topic_name, 0))))
    }

    /// The records of every batch, as `(offset, record)`.
    fn records_with_offsets(batches: Vec<deflated::Batch>) -> Result<Vec<(i64, Record)>> {
        batches.into_iter().try_fold(Vec::new(), |mut acc, batch| {
            let inflated = Batch::try_from(batch)?;

            acc.extend(inflated.records.into_iter().map(|record| {
                (
                    inflated.base_offset + i64::from(record.offset_delta),
                    record,
                )
            }));

            Ok(acc)
        })
    }

    /// The fetch query reads at most 10,000 rows per call. Without that bound each fetch
    /// reads every row between the fetch offset and the high watermark, so a consumer behind
    /// a fast producer slows down as its backlog grows. The fetch service reads again from
    /// the last offset it received, so a bounded read must neither skip nor repeat a record.
    #[tokio::test]
    async fn fetch_reads_a_bounded_number_of_rows_and_resumes_after_them() -> Result<()> {
        const RECORDS: i64 = ROWS_PER_FETCH + 50;

        let Some((storage, topition)) = storage_with_empty_topic().await? else {
            return Ok(());
        };

        let batch = (0..RECORDS)
            .fold(Batch::builder(), |builder, delta| {
                builder.record(
                    Record::builder()
                        .value(Bytes::from_static(b"a").into())
                        .offset_delta(i32::try_from(delta).unwrap_or(i32::MAX)),
                )
            })
            .last_offset_delta(i32::try_from(RECORDS - 1)?)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage.produce(None, &topition, batch).await?;

        let fetch_from = |offset| {
            storage.fetch_until_deadline(
                &topition,
                offset,
                1,
                1024 * 1024,
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_secs(5),
            )
        };

        let first = records_with_offsets(fetch_from(0).await?.batches)?;

        assert_eq!(
            (0..ROWS_PER_FETCH).collect::<Vec<_>>(),
            first.iter().map(|(offset, _)| *offset).collect::<Vec<_>>(),
            "one fetch must stop at the row limit",
        );

        let rest = records_with_offsets(fetch_from(ROWS_PER_FETCH).await?.batches)?;

        assert_eq!(
            (ROWS_PER_FETCH..RECORDS).collect::<Vec<_>>(),
            rest.iter().map(|(offset, _)| *offset).collect::<Vec<_>>(),
            "reading on from the last offset must return the rest, with no gap or repeat",
        );

        Ok(())
    }

    /// A read with the largest `max_wait` that a client can send still reads at most
    /// `ROWS_PER_FETCH` rows, and then returns its connection and its Fetch permit. The
    /// budget only stops the assembly of records, so the row limit is the bound that keeps
    /// one read over a large backlog from holding a permit for minutes.
    #[tokio::test]
    async fn fetch_with_the_largest_max_wait_returns_its_permit_within_a_bounded_read() -> Result<()>
    {
        const RECORDS: i64 = 3 * ROWS_PER_FETCH;

        // Measured reads of `ROWS_PER_FETCH` rows take about 300 ms.
        const READ_BOUND: Duration = Duration::from_secs(10);

        let Some((storage, topition)) = storage_with_empty_topic().await? else {
            return Ok(());
        };

        let batch = (0..RECORDS)
            .fold(Batch::builder(), |builder, delta| {
                builder.record(
                    Record::builder()
                        .value(Bytes::from_static(b"a").into())
                        .offset_delta(i32::try_from(delta).unwrap_or(i32::MAX)),
                )
            })
            .last_offset_delta(i32::try_from(RECORDS - 1)?)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage.produce(None, &topition, batch).await?;

        let largest_max_wait = Duration::from_millis(u64::try_from(i32::MAX)?);

        let batches = timeout(
            READ_BOUND,
            storage.fetch_until_deadline(
                &topition,
                0,
                1,
                5 * 1024 * 1024,
                IsolationLevel::ReadUncommitted,
                Instant::now() + largest_max_wait,
            ),
        )
        .await
        .map_err(|_elapsed| {
            Error::Message(format!("the read took longer than {READ_BOUND:?}"))
        })??;

        assert_eq!(
            (0..ROWS_PER_FETCH).collect::<Vec<_>>(),
            records_with_offsets(batches.batches)?
                .iter()
                .map(|(offset, _)| *offset)
                .collect::<Vec<_>>(),
            "one read must stop at the row limit, whatever its budget",
        );

        assert_eq!(
            ReadLimits::default().fetch,
            storage.fetch_reads.available_permits()
        );

        Ok(())
    }

    /// Gives the topic a delete policy with a retention of one minute.
    async fn retain_for_one_minute(storage: &Postgres, topition: &Topition) -> Result<()> {
        let c = storage.connection().await?;

        _ = c
            .execute(
                "insert into topic_configuration (topic, name, value) \
                 select t.id, v.name, v.value \
                 from topic t \
                 join cluster c on c.id = t.cluster, \
                 (values ('cleanup.policy', 'delete'), ('retention.ms', '60000')) v(name, value) \
                 where c.name = $1 and t.name = $2",
                &[&storage.cluster, &topition.topic()],
            )
            .await?;

        Ok(())
    }

    /// Produces one record per timestamp, in milliseconds since the epoch, at consecutive offsets.
    async fn produce_at(storage: &Postgres, topition: &Topition, timestamps: &[i64]) -> Result<()> {
        let base = timestamps.iter().copied().min().unwrap_or_default();

        let batch = timestamps
            .iter()
            .enumerate()
            .fold(Batch::builder(), |builder, (offset, timestamp)| {
                builder.record(
                    Record::builder()
                        .value(Bytes::from_static(b"a").into())
                        .offset_delta(i32::try_from(offset).unwrap_or(i32::MAX))
                        .timestamp_delta(timestamp - base),
                )
            })
            .base_timestamp(base)
            .max_timestamp(timestamps.iter().copied().max().unwrap_or_default())
            .last_offset_delta(i32::try_from(timestamps.len() - 1)?)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage.produce(None, topition, batch).await?;

        Ok(())
    }

    /// Sets when each record was appended, in milliseconds since the epoch, for the records at
    /// consecutive offsets from `first_offset`. Records are appended now, and retention reads
    /// the append time to find where the expired records end.
    async fn append_times(
        storage: &Postgres,
        topition: &Topition,
        first_offset: i64,
        appended: &[i64],
    ) -> Result<()> {
        let c = storage.connection().await?;

        let updated = c
            .execute(
                "update record r \
                 set created_at = cast(to_timestamp(a.ms / 1000.0) as timestamp) \
                 from unnest($4::bigint[]) with ordinality as a(ms, n) \
                 where r.topition = ( \
                     select tp.id \
                     from cluster c \
                     join topic t on t.cluster = c.id \
                     join topition tp on tp.topic = t.id \
                     where c.name = $1 and t.name = $2 and tp.partition = $3) \
                 and r.offset_id = $5 + a.n - 1",
                &[
                    &storage.cluster,
                    &topition.topic(),
                    &topition.partition(),
                    &appended,
                    &first_offset,
                ],
            )
            .await?;

        assert_eq!(u64::try_from(appended.len())?, updated);

        Ok(())
    }

    /// Removes the records at the offsets, as compaction leaves a gap.
    async fn remove_offsets(
        storage: &Postgres,
        topition: &Topition,
        offsets: std::ops::Range<i64>,
    ) -> Result<()> {
        let c = storage.connection().await?;

        _ = c
            .execute(
                "delete from record r \
                 where r.topition = ( \
                     select tp.id \
                     from cluster c \
                     join topic t on t.cluster = c.id \
                     join topition tp on tp.topic = t.id \
                     where c.name = $1 and t.name = $2 and tp.partition = $3) \
                 and r.offset_id >= $4 and r.offset_id < $5",
                &[
                    &storage.cluster,
                    &topition.topic(),
                    &topition.partition(),
                    &offsets.start,
                    &offsets.end,
                ],
            )
            .await?;

        Ok(())
    }

    /// The offsets that are still stored for the partition, in order.
    async fn stored_offsets(storage: &Postgres, topition: &Topition) -> Result<Vec<i64>> {
        let c = storage.connection().await?;

        c.query(
            "select r.offset_id \
             from record r \
             join topition tp on tp.id = r.topition \
             join topic t on t.id = tp.topic \
             join cluster c on c.id = t.cluster \
             where c.name = $1 and t.name = $2 and tp.partition = $3 \
             order by r.offset_id",
            &[&storage.cluster, &topition.topic(), &topition.partition()],
        )
        .await?
        .iter()
        .map(|row| row.try_get::<_, i64>(0).map_err(Into::into))
        .collect()
    }

    fn now_ms() -> Result<i64> {
        Ok(i64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_millis(),
        )?)
    }

    /// Retention deletes the records below an offset that a search over the append times finds,
    /// in place of testing every record. When the append times rise with offsets, that must
    /// delete every expired record and no other.
    #[tokio::test]
    async fn retention_deletes_every_expired_record_when_timestamps_rise_with_offsets() -> Result<()>
    {
        const EXPIRED: i64 = 1_200;
        const RECORDS: i64 = 2_000;

        let Some((storage, topition)) = storage_with_empty_topic().await? else {
            return Ok(());
        };

        retain_for_one_minute(&storage, &topition).await?;

        let now = now_ms()?;

        let timestamps = (0..RECORDS)
            .map(|offset| {
                if offset < EXPIRED {
                    now - 600_000 + offset
                } else {
                    now - 10_000 + (offset - EXPIRED)
                }
            })
            .collect::<Vec<_>>();

        produce_at(&storage, &topition, &timestamps).await?;
        append_times(&storage, &topition, 0, &timestamps).await?;

        let deleted = storage.policy_delete(SystemTime::now()).await?;

        assert_eq!(u64::try_from(EXPIRED)?, deleted);
        assert_eq!(
            (EXPIRED..RECORDS).collect::<Vec<_>>(),
            stored_offsets(&storage, &topition).await?
        );

        Ok(())
    }

    /// A record with a timestamp ahead of the records around it, from a producer clock that is
    /// wrong, has not expired and is kept. It must not hold back the deletion of the records after
    /// it.
    #[tokio::test]
    async fn retention_keeps_a_record_that_has_not_expired_and_deletes_the_rest() -> Result<()> {
        const RECORDS: i64 = 1_000;
        const AHEAD: [i64; 2] = [100, 500];

        let Some((storage, topition)) = storage_with_empty_topic().await? else {
            return Ok(());
        };

        retain_for_one_minute(&storage, &topition).await?;

        let now = now_ms()?;

        let timestamps = (0..RECORDS)
            .map(|offset| {
                if AHEAD.contains(&offset) {
                    now + 86_400_000
                } else {
                    now - 600_000 + offset
                }
            })
            .collect::<Vec<_>>();

        produce_at(&storage, &topition, &timestamps).await?;
        append_times(
            &storage,
            &topition,
            0,
            &(0..RECORDS)
                .map(|offset| now - 600_000 + offset)
                .collect::<Vec<_>>(),
        )
        .await?;

        let deleted = storage.policy_delete(SystemTime::now()).await?;

        assert_eq!(u64::try_from(RECORDS - 2)?, deleted);
        assert_eq!(AHEAD.to_vec(), stored_offsets(&storage, &topition).await?);

        Ok(())
    }

    /// Retention does not advance the low watermark, so a sweep after the first searches a range
    /// that starts with offsets that are no longer stored. It must end, and delete exactly the
    /// records that have expired since.
    #[tokio::test]
    async fn retention_sweeps_again_after_more_records_arrive() -> Result<()> {
        let Some((storage, topition)) = storage_with_empty_topic().await? else {
            return Ok(());
        };

        retain_for_one_minute(&storage, &topition).await?;

        let now = now_ms()?;

        let first = (0..600)
            .map(|offset| now - 600_000 + offset)
            .collect::<Vec<_>>();
        produce_at(&storage, &topition, &first).await?;
        append_times(&storage, &topition, 0, &first).await?;

        assert_eq!(600, storage.policy_delete(SystemTime::now()).await?);
        assert_eq!(
            Vec::<i64>::new(),
            stored_offsets(&storage, &topition).await?
        );

        let second = (0..400)
            .map(|offset| {
                if offset < 200 {
                    now - 300_000 + offset
                } else {
                    now - 10_000 + offset
                }
            })
            .collect::<Vec<_>>();
        produce_at(&storage, &topition, &second).await?;
        append_times(&storage, &topition, 600, &second).await?;

        assert_eq!(200, storage.policy_delete(SystemTime::now()).await?);
        assert_eq!(
            (800..1_000).collect::<Vec<_>>(),
            stored_offsets(&storage, &topition).await?
        );

        assert_eq!(0, storage.policy_delete(SystemTime::now()).await?);

        Ok(())
    }

    /// Compaction leaves gaps in the offsets of a partition. A search whose probes land in a gap
    /// must still find the boundary and delete every expired record on both sides of the gap.
    #[tokio::test]
    async fn retention_deletes_expired_records_on_both_sides_of_a_gap() -> Result<()> {
        const EXPIRED: i64 = 800;
        const RECORDS: i64 = 1_000;

        let Some((storage, topition)) = storage_with_empty_topic().await? else {
            return Ok(());
        };

        retain_for_one_minute(&storage, &topition).await?;

        let now = now_ms()?;

        let timestamps = (0..RECORDS)
            .map(|offset| {
                if offset < EXPIRED {
                    now - 600_000 + offset
                } else {
                    now - 10_000 + offset
                }
            })
            .collect::<Vec<_>>();

        produce_at(&storage, &topition, &timestamps).await?;
        append_times(&storage, &topition, 0, &timestamps).await?;
        remove_offsets(&storage, &topition, 300..700).await?;

        assert_eq!(400, storage.policy_delete(SystemTime::now()).await?);
        assert_eq!(
            (EXPIRED..RECORDS).collect::<Vec<_>>(),
            stored_offsets(&storage, &topition).await?
        );

        Ok(())
    }

    /// A record produced without a timestamp stores NO_TIMESTAMP, -1, which is an instant before
    /// the epoch. Retention reads the append time first, so the record is kept while that has not
    /// expired, and deleted when it has, as a record with any other old timestamp is.
    #[tokio::test]
    async fn retention_keeps_a_record_without_a_timestamp_until_its_append_time_expires()
    -> Result<()> {
        let Some((storage, topition)) = storage_with_empty_topic().await? else {
            return Ok(());
        };

        retain_for_one_minute(&storage, &topition).await?;

        let now = now_ms()?;

        produce_at(&storage, &topition, &[-1, -1]).await?;
        append_times(&storage, &topition, 0, &[now, now]).await?;

        assert_eq!(0, storage.policy_delete(SystemTime::now()).await?);
        assert_eq!(vec![0, 1], stored_offsets(&storage, &topition).await?);

        append_times(&storage, &topition, 0, &[now - 600_000, now - 600_000]).await?;

        assert_eq!(2, storage.policy_delete(SystemTime::now()).await?);
        assert_eq!(
            Vec::<i64>::new(),
            stored_offsets(&storage, &topition).await?
        );

        Ok(())
    }

    /// A lookup by timestamp never answers a record without one: Kafka's time index holds no entry
    /// for it either.
    #[tokio::test]
    async fn list_offsets_by_timestamp_skips_a_record_without_a_timestamp() -> Result<()> {
        let Some((storage, topition)) = storage_with_empty_topic().await? else {
            return Ok(());
        };

        let now = now_ms()?;

        produce_at(&storage, &topition, &[-1, now]).await?;

        let responses = storage
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[(
                    topition.clone(),
                    ListOffset::Timestamp(SystemTime::UNIX_EPOCH),
                )],
            )
            .await?;

        assert_eq!(Some(1), responses[0].1.offset);

        Ok(())
    }

    /// A keyed fetch returns each record with its own headers, and pairs each header key with its
    /// value.
    #[tokio::test]
    async fn keyed_fetch_attaches_headers_of_the_records_it_returns() -> Result<()> {
        let Some((storage, topition)) = storage_with_empty_topic().await? else {
            return Ok(());
        };

        let c = storage.connection().await?;
        _ = c
            .execute(
                "insert into topic_configuration (topic, name, value) \
                 select t.id, 'tansu.virtual', 'true' \
                 from topic t join cluster c on c.id = t.cluster \
                 where c.name = $1 and t.name = $2",
                &[&storage.cluster, &topition.topic()],
            )
            .await?;

        let batch = (0..6)
            .fold(Batch::builder(), |builder, delta| {
                let key = if delta % 2 == 0 { "key-a" } else { "key-b" };

                builder.record(
                    keyed_record_headers(delta).into_iter().fold(
                        Record::builder()
                            .key(Bytes::copy_from_slice(key.as_bytes()).into())
                            .value(Bytes::from_static(b"a").into())
                            .offset_delta(delta),
                        |record, header| record.header(header),
                    ),
                )
            })
            .last_offset_delta(5)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage.produce(None, &topition, batch).await?;

        let records = records_with_offsets(
            storage
                .fetch_until_deadline(
                    &Topition::new(format!("{}/key-a", topition.topic()), 0),
                    0,
                    1,
                    8 * 1024,
                    IsolationLevel::ReadUncommitted,
                    Instant::now() + Duration::from_secs(5),
                )
                .await
                .map(|read| read.batches)?,
        )?;

        assert_eq!(
            [0, 2, 4]
                .into_iter()
                .map(|delta| (
                    i64::from(delta),
                    keyed_record_headers(delta)
                        .map(|header| header.build())
                        .to_vec()
                ))
                .collect::<Vec<_>>(),
            records
                .into_iter()
                .map(|(offset, record)| (offset, record.headers))
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    /// Returns the headers of the record at `offset_delta`: one whose key differs from its value,
    /// and one with a null value. Both name the record, so a header on the wrong record shows.
    fn keyed_record_headers(offset_delta: i32) -> [header::Builder; 2] {
        [
            Header::builder()
                .key(Bytes::from(format!("key-{offset_delta}")))
                .value(Bytes::from(format!("value-{offset_delta}"))),
            Header::builder().key(Bytes::from(format!("null-value-{offset_delta}"))),
        ]
    }

    /// Headers are read for the whole range of a fetch at once, and each must end up on the
    /// record at its own offset: records without headers stay without, the first and last record of
    /// the range get theirs, and a record keeps its headers in produce order.
    #[tokio::test]
    async fn fetch_attaches_each_header_to_its_own_record() -> Result<()> {
        let Some((storage, topition)) = storage_with_empty_topic().await? else {
            return Ok(());
        };

        let headers: [&[&str]; 5] = [&["zero"], &["one"], &[], &["two-a", "two-b"], &["four"]];

        let batch = headers
            .iter()
            .enumerate()
            .fold(Batch::builder(), |builder, (delta, keys)| {
                builder.record(
                    keys.iter().fold(
                        Record::builder()
                            .value(Bytes::from_static(b"a").into())
                            .offset_delta(i32::try_from(delta).unwrap_or(i32::MAX)),
                        |record, key| {
                            record.header(
                                Header::builder()
                                    .key(Bytes::copy_from_slice(key.as_bytes()))
                                    .value(Bytes::copy_from_slice(key.as_bytes())),
                            )
                        },
                    ),
                )
            })
            .last_offset_delta(i32::try_from(headers.len() - 1)?)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage.produce(None, &topition, batch).await?;

        let records = records_with_offsets(
            storage
                .fetch_until_deadline(
                    &topition,
                    0,
                    1,
                    8 * 1024,
                    IsolationLevel::ReadUncommitted,
                    Instant::now() + Duration::from_secs(5),
                )
                .await
                .map(|read| read.batches)?,
        )?;

        let fetched = records
            .iter()
            .map(|(_, record)| {
                record
                    .headers
                    .iter()
                    .map(|header| {
                        header
                            .key
                            .as_deref()
                            .map(|key| String::from_utf8_lossy(key).into_owned())
                            .unwrap_or_default()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(
            headers
                .iter()
                .map(|keys| keys.iter().map(|key| key.to_string()).collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            fetched,
        );

        Ok(())
    }

    /// Under [`FETCH_PLAN_SETTINGS`], each fetch query reads its records in offset order and sends
    /// its first row before it has read every row: the plain query merge-joins the records with
    /// their headers, and the keyed query looks up the headers of each returned record. Both the
    /// generic plan and a custom plan, built for real parameters, keep that shape.
    #[tokio::test]
    async fn fetch_query_plans_stream_records_with_headers() -> Result<()> {
        let Some((storage, topition)) = storage_with_empty_topic().await? else {
            return Ok(());
        };

        let mut c = storage.connection().await?;
        let tx = c.transaction().await?;

        // The statistics of a nearly empty table make the planner expect few rows in any offset
        // range, which is when it would choose a plan that reads every row.
        tx.batch_execute("ANALYZE record; ANALYZE header;").await?;
        tx.batch_execute(FETCH_PLAN_SETTINGS).await?;

        let cluster = storage.cluster.as_str();
        let topic = topition.topic();

        for (query, arguments) in [
            (
                pg_record_fetch::QUERY,
                format!("'{cluster}', '{topic}', 0, 0, 1048576, 100000, {ROWS_PER_FETCH}"),
            ),
            (
                pg_record_fetch::KEYED_QUERY,
                format!("'{cluster}', '{topic}', 0, 0, 1048576, 100000, 'k', {ROWS_PER_FETCH}"),
            ),
        ] {
            let sql = storage.sql_lookup(query)?;

            // `generic_plan` explains the plan that PostgreSQL builds without the parameters'
            // values, which a prepared statement can switch to. `explain execute` explains the
            // custom plan that it builds from the values of its first executions.
            tx.batch_execute(&format!("prepare fetch_plan as {sql}"))
                .await?;

            for explain in [
                format!("explain (costs off, generic_plan) {sql}"),
                format!("explain (costs off) execute fetch_plan({arguments})"),
            ] {
                // The simple query protocol sends the statement without binding its
                // parameters, which `generic_plan` needs.
                let plan = tx
                    .simple_query(&explain)
                    .await?
                    .into_iter()
                    .filter_map(|message| match message {
                        SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
                        _ => None,
                    })
                    .collect::<Vec<_>>();

                if query == pg_record_fetch::QUERY {
                    assert!(plan[0].contains("Merge Left Join"), "{explain}: {plan:#?}");
                } else {
                    assert!(
                        plan.iter().all(|line| !line.contains("Join")),
                        "{explain}: {plan:#?}"
                    );
                    assert!(
                        plan.iter().any(|line| line.contains("SubPlan")),
                        "{explain}: {plan:#?}"
                    );
                }

                for step in ["Sort", "Materialize", "Nested Loop", "Hash"] {
                    assert!(
                        plan.iter().all(|line| !line.contains(step)),
                        "{explain}: {step} in {plan:#?}"
                    );
                }
            }

            tx.batch_execute("deallocate fetch_plan").await?;
        }

        _ = tx.rollback().await;

        Ok(())
    }

    /// Produces `ROWS_PER_FETCH` records of 1 KiB to `topition`, from offset 0, each with one
    /// header whose key is the record's offset.
    ///
    /// Records of 1 KiB make a fetch statement send rows for long enough that a deadline can fall
    /// inside it.
    async fn produce_records_with_headers(storage: &Postgres, topition: &Topition) -> Result<()> {
        // A produce request holds at most 1 MiB, so the records go in batches of 1,000.
        const RECORDS_PER_BATCH: i64 = 1_000;

        let value = Bytes::from(vec![b'v'; 1024]);

        for base_offset in (0..ROWS_PER_FETCH).step_by(usize::try_from(RECORDS_PER_BATCH)?) {
            let batch = (0..RECORDS_PER_BATCH)
                .fold(Batch::builder(), |builder, delta| {
                    builder.record(
                        Record::builder()
                            .value(value.clone().into())
                            .offset_delta(i32::try_from(delta).unwrap_or(i32::MAX))
                            .header(
                                Header::builder()
                                    .key(Bytes::from((base_offset + delta).to_string()))
                                    .value(Bytes::from_static(b"h")),
                            ),
                    )
                })
                .last_offset_delta(i32::try_from(RECORDS_PER_BATCH - 1)?)
                .build()
                .and_then(TryInto::try_into)?;

            _ = storage.produce(None, topition, batch).await?;
        }

        Ok(())
    }

    /// How often `nisshi_sql_requests` counted a statement, and how many durations
    /// `nisshi_sql_duration` recorded for it, with their total in milliseconds.
    #[derive(Debug)]
    struct StatementCounts {
        requests: u64,
        durations: u64,
        duration_millis: u64,
    }

    /// Returns the counts that `exporter` holds for the statement `sql` with the `outcome`
    /// attribute `outcome`.
    fn statement_counts(
        exporter: &InMemoryMetricExporter,
        sql: &str,
        outcome: &str,
    ) -> StatementCounts {
        let matches = |attributes: &mut dyn Iterator<Item = &KeyValue>| {
            let attributes = attributes.collect::<Vec<_>>();
            [("sql", sql), ("outcome", outcome)]
                .iter()
                .all(|(key, value)| {
                    attributes.iter().any(|attribute| {
                        attribute.key.as_str() == *key && attribute.value.as_str() == *value
                    })
                })
        };

        let mut counts = StatementCounts {
            requests: 0,
            durations: 0,
            duration_millis: 0,
        };

        let metrics = exporter.get_finished_metrics().unwrap_or_default();

        // The counts are cumulative, so they come from the latest export only.
        for metric in metrics
            .last()
            .into_iter()
            .flat_map(ResourceMetrics::scope_metrics)
            .flat_map(ScopeMetrics::metrics)
        {
            match (metric.name(), metric.data()) {
                ("nisshi_sql_requests", AggregatedMetrics::U64(MetricData::Sum(sum))) => {
                    counts.requests += sum
                        .data_points()
                        .filter(|point| matches(&mut point.attributes()))
                        .map(SumDataPoint::value)
                        .sum::<u64>();
                }
                (
                    "nisshi_sql_duration",
                    AggregatedMetrics::U64(MetricData::Histogram(histogram)),
                ) => {
                    for point in histogram
                        .data_points()
                        .filter(|point| matches(&mut point.attributes()))
                    {
                        counts.durations += point.count();
                        counts.duration_millis += point.sum();
                    }
                }
                _ => {}
            }
        }

        counts
    }

    /// The records statement of a fetch counts in `nisshi_sql_requests` and `nisshi_sql_duration`
    /// with the `outcome` `completed` when it sends all of its rows, and `stopped` when the caller
    /// drops the read before then. The duration leaves out the time that the read waits for room in
    /// its channel.
    #[tokio::test]
    async fn fetch_statement_counts_its_outcome() -> Result<()> {
        // The exporter must be installed before the storage creates its first SQL metric.
        let (provider, exporter) = install_exporter();

        let Some((storage, topition)) = storage_with_empty_topic().await? else {
            return Ok(());
        };

        produce_records_with_headers(&storage, &topition).await?;

        let read = storage
            .fetch_until_deadline(
                &topition,
                0,
                1,
                u32::MAX,
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_secs(30),
            )
            .await?;
        assert_eq!(
            usize::try_from(ROWS_PER_FETCH)?,
            records_with_offsets(read.batches)?.len()
        );

        // The test keeps the receiving end and never reads it. The read has more records than the
        // channel holds, so it never finishes, and it waits in its statement for room in the
        // channel when the test drops it.
        let (fetched_sender, _fetched_receiver) = fetched::channel();
        let mut stopped = storage.fetch(
            &topition,
            0,
            1,
            u32::MAX,
            IsolationLevel::ReadUncommitted,
            fetched_sender,
        );
        assert!(timeout(Duration::from_secs(5), &mut stopped).await.is_err());
        drop(stopped);

        provider
            .force_flush()
            .map_err(|err| Error::Message(err.to_string()))?;

        let sql = storage.sql_lookup(pg_record_fetch::QUERY)?;

        let completed = statement_counts(&exporter, sql, "completed");
        assert_eq!((1, 1), (completed.requests, completed.durations));

        // The stopped read waited 5 seconds for room in its channel, and none of that wait is
        // database time.
        let stopped = statement_counts(&exporter, sql, "stopped");
        assert_eq!((1, 1), (stopped.requests, stopped.durations));
        assert!(
            stopped.duration_millis < 5_000,
            "{} ms",
            stopped.duration_millis
        );

        Ok(())
    }

    /// A fetch whose caller stops taking records returns an error. It counts its statement as
    /// stopped and its read as abandoned, and returns its permits once the statement has ended.
    #[tokio::test]
    async fn fetch_whose_caller_stops_taking_records_abandons_its_statement() -> Result<()> {
        // The exporter must be installed before the storage creates its first SQL metric.
        let (provider, exporter) = install_exporter();

        let Some((storage, topition)) = storage_with_empty_topic().await? else {
            return Ok(());
        };

        produce_records_with_headers(&storage, &topition).await?;

        let (fetched_sender, fetched_receiver) = fetched::channel();
        let mut read = storage.fetch(
            &topition,
            0,
            1,
            u32::MAX,
            IsolationLevel::ReadUncommitted,
            fetched_sender,
        );

        // The read has more records than the channel holds, so it waits for room in the channel
        // until the test drops the receiving end.
        assert!(timeout(Duration::from_secs(5), &mut read).await.is_err());
        drop(fetched_receiver);
        assert_matches!(read.await, Err(Error::UnableToSend));

        // An abandoned read returns its permits only after a cancel request and `SETTLE_QUERY`,
        // which take round trips to the server.
        let give_up = Instant::now() + Duration::from_secs(10);
        while storage.fetch_reads.available_permits() < ReadLimits::default().fetch {
            assert!(
                Instant::now() < give_up,
                "the abandoned read kept its permits"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert_eq!(
            1,
            counter_total(
                &provider,
                &exporter,
                "nisshi_sql_statements_abandoned",
                &storage.cluster
            )?
        );

        let sql = storage.sql_lookup(pg_record_fetch::QUERY)?;
        assert_eq!(1, statement_counts(&exporter, sql, "stopped").requests);

        Ok(())
    }

    /// A fetch whose rows end in an error from its connection returns the records that arrived
    /// before the error, with the error. Its connection goes back to the pool without a cancel
    /// request, because no statement runs on it.
    #[tokio::test]
    async fn fetch_whose_rows_end_in_an_error_releases_its_connection() -> Result<()> {
        // The exporter must be installed before the storage creates its first SQL metric.
        let (provider, exporter) = install_exporter();

        let mut url = Url::parse(&CONNECTION)?;
        let upstream = (
            url.host_str().unwrap_or("localhost").to_owned(),
            url.port().unwrap_or(5432),
        );
        let (port, cut) = proxy_that_cuts(upstream).await?;
        _ = url.set_host(Some("127.0.0.1"));
        _ = url.set_port(Some(port));

        let cluster = alphanumeric_string(15);
        let Some(storage) = storage_with_schema(
            "fetch_whose_rows_end_in_an_error_releases_its_connection",
            url.as_str(),
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };
        storage.pool.resize(1);

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        let topic_name = alphanumeric_string(15);
        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(topic_name.clone())
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;
        let topition = Topition::new(topic_name, 0);

        produce_records_with_headers(&storage, &topition).await?;

        // The fetch's rows are about 10 MiB, and every statement before them returns far less than
        // the 1 MiB after which the proxy cuts, so the cut falls partway through the rows.
        cut.store(true, atomic::Ordering::Relaxed);

        let fetched = storage
            .fetch_until_deadline_or_failure(
                &topition,
                0,
                1,
                u32::MAX,
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_secs(30),
            )
            .await?;
        assert_matches!(fetched.failed, Some(_));

        let offsets = records_with_offsets(fetched.batches)?
            .into_iter()
            .map(|(offset, _)| offset)
            .collect::<Vec<_>>();
        assert!(!offsets.is_empty(), "the fetch kept no records");
        assert!(
            offsets.len() < usize::try_from(ROWS_PER_FETCH)?,
            "the cut did not fall partway through the rows"
        );
        assert_eq!((0..).take(offsets.len()).collect::<Vec<i64>>(), offsets);

        assert_eq!(1, storage.pool.status().available);
        assert_eq!(
            ReadLimits::default().fetch,
            storage.fetch_reads.available_permits()
        );

        // The setup's statements count in `nisshi_sql_requests`, so the exporter is live, and a
        // count of 0 abandoned statements is not a missing export.
        assert!(counter_total(&provider, &exporter, "nisshi_sql_requests", &cluster)? > 0);
        assert_eq!(
            0,
            counter_total(
                &provider,
                &exporter,
                "nisshi_sql_statements_abandoned",
                &cluster
            )?
        );

        Ok(())
    }

    /// Starts a TCP proxy to the server at `upstream`, and returns its port and a switch. Once the
    /// switch is set, the proxy closes each connection after it has forwarded more than 1 MiB from
    /// the server, which ends a large result partway through its rows.
    async fn proxy_that_cuts(upstream: (String, u16)) -> Result<(u16, Arc<AtomicBool>)> {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        const CUT_AFTER: usize = 1024 * 1024;

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let port = listener.local_addr()?.port();
        let cut = Arc::new(AtomicBool::new(false));

        let switch = cut.clone();
        _ = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let upstream = upstream.clone();
                let cut = cut.clone();

                _ = tokio::spawn(async move {
                    let Ok(server) = TcpStream::connect(upstream).await else {
                        return;
                    };

                    let (mut client_read, mut client_write) = client.into_split();
                    let (mut server_read, mut server_write) = server.into_split();

                    let to_server = tokio::io::copy(&mut client_read, &mut server_write);

                    let to_client = async {
                        let mut buffer = vec![0; 64 * 1024];
                        let mut forwarded_since_cut = 0;

                        loop {
                            let read = server_read.read(&mut buffer).await?;
                            if read == 0 {
                                return Ok::<_, io::Error>(());
                            }

                            client_write.write_all(&buffer[..read]).await?;

                            if cut.load(atomic::Ordering::Relaxed) {
                                forwarded_since_cut += read;
                                if forwarded_since_cut > CUT_AFTER {
                                    return Ok(());
                                }
                            }
                        }
                    };

                    // Returning drops both halves of each connection, which closes them.
                    tokio::select! {
                        _ = to_server => {}
                        _ = to_client => {}
                    }
                });
            }
        });

        Ok((port, switch))
    }

    /// A header with a null key or a null value comes back null, not empty, in its own position.
    #[tokio::test]
    async fn fetch_keeps_null_header_keys_and_values() -> Result<()> {
        let Some((storage, topition)) = storage_with_empty_topic().await? else {
            return Ok(());
        };

        let headers = [
            Header::builder()
                .key(Bytes::from_static(b"key"))
                .value(Bytes::from_static(b"value")),
            Header::builder().key(Bytes::from_static(b"null-value")),
            Header::builder().value(Bytes::from_static(b"null-key")),
            Header::builder(),
        ];

        let batch = Batch::builder()
            .record(
                headers.iter().cloned().fold(
                    Record::builder()
                        .value(Bytes::from_static(b"a").into())
                        .offset_delta(0),
                    |record, header| record.header(header),
                ),
            )
            .last_offset_delta(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage.produce(None, &topition, batch).await?;

        let read = storage
            .fetch_until_deadline(
                &topition,
                0,
                1,
                8 * 1024,
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_secs(5),
            )
            .await?;

        assert_eq!(
            vec![headers.map(|header| header.build()).to_vec()],
            records_with_offsets(read.batches)?
                .into_iter()
                .map(|(_, record)| record.headers)
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    /// A fetched row whose header arrays differ in length fails to decode, with an error that
    /// names the record's offset.
    #[tokio::test]
    async fn fetched_row_with_unpaired_headers_names_its_offset() -> Result<()> {
        let Some((storage, _)) = storage_with_empty_topic().await? else {
            return Ok(());
        };

        let row = storage
            .connection()
            .await?
            .query_one(
                "SELECT 42::bigint AS offset_id, \
                 NULL::smallint AS attributes, \
                 now() AS timestamp, \
                 NULL::bytea AS k, \
                 NULL::bytea AS v, \
                 NULL::bigint AS producer_id, \
                 NULL::smallint AS producer_epoch, \
                 ARRAY['\\x01'::bytea] AS header_keys, \
                 ARRAY[]::bytea[] AS header_values",
                &[],
            )
            .await?;

        assert_matches!(
            pg_record_fetch::Row::try_from(&row),
            Err(Error::Message(message)) if message.contains("offset 42")
        );

        Ok(())
    }

    /// An empty database that one test creates on the server at [`CONNECTION`].
    struct EmptyDatabase {
        name: String,
        admin: Postgres,
    }

    impl EmptyDatabase {
        /// Creates the database. Returns `None` when the server isn't reachable.
        async fn create(test: &str) -> Result<Option<Self>> {
            let admin = Postgres::builder(&CONNECTION)?
                .cluster(alphanumeric_string(15).as_str())
                .node(0)
                .build();

            let c = match admin.connection().await {
                Ok(c) => c,
                Err(err) => {
                    eprintln!("skipping {test}: {err:?}");
                    return Ok(None);
                }
            };

            let name = format!("nisshi_test_{}", alphanumeric_string(12).to_lowercase());
            c.batch_execute(&format!("create database {name}")).await?;

            Ok(Some(Self { name, admin }))
        }

        fn url(&self) -> Result<Url> {
            let mut url = Url::parse(&CONNECTION)?;
            url.set_path(&self.name);
            Ok(url)
        }

        /// Returns a [`Postgres`] storage engine for a new cluster on this database, as the user
        /// in [`CONNECTION`].
        fn storage(&self) -> Result<Postgres> {
            self.storage_at(&self.url()?)
        }

        fn storage_at(&self, url: &Url) -> Result<Postgres> {
            Ok(Postgres::builder(url.as_str())?
                .cluster(alphanumeric_string(15).as_str())
                .node(111)
                .build())
        }

        /// Runs `sql` on this database as the user in [`CONNECTION`].
        async fn execute(&self, sql: &str) -> Result<()> {
            self.storage()?
                .connection()
                .await?
                .batch_execute(sql)
                .await
                .map_err(Into::into)
        }

        /// Records `version` and `oldest_compatible_version` in the database's
        /// `nisshi_schema_version` table.
        async fn record_schema_version(
            &self,
            version: i32,
            oldest_compatible_version: i32,
        ) -> Result<()> {
            self.execute(&format!(
                "update nisshi_schema_version set version = {version}, \
                 oldest_compatible_version = {oldest_compatible_version}"
            ))
            .await
        }

        /// Creates a login role that can't create anything in the `public` namespace, and returns
        /// the URL that logs in to this database as the role. [`EmptyDatabase::remove`] drops
        /// the role.
        async fn create_role(&self) -> Result<Url> {
            let role = format!("{}_role", self.name);
            let password = alphanumeric_string(12);

            self.admin
                .connection()
                .await?
                .batch_execute(&format!("create role {role} login password '{password}'"))
                .await?;
            self.execute("revoke create on schema public from public")
                .await?;

            let mut url = self.url()?;
            url.set_username(&role)
                .and_then(|()| url.set_password(Some(&password)))
                .map_err(|()| Error::Message(format!("{url} can't hold a user")))?;
            Ok(url)
        }

        /// Drops the database, and the role of [`EmptyDatabase::create_role`] if there is one.
        async fn remove(self) -> Result<()> {
            let c = self.admin.connection().await?;
            c.batch_execute(&format!("drop database {} with (force)", self.name))
                .await?;
            c.batch_execute(&format!("drop role if exists {}_role", self.name))
                .await
                .map_err(Into::into)
        }
    }

    /// Runs `body` on an empty database of its own, then drops the database when `body` returns.
    /// Skips `test` when the server can't be reached.
    async fn with_empty_database(
        test: &str,
        body: impl AsyncFnOnce(&EmptyDatabase) -> Result<()>,
    ) -> Result<()> {
        let Some(database) = EmptyDatabase::create(test).await? else {
            return Ok(());
        };

        let outcome = body(&database).await;
        let removed = database.remove().await;
        outcome.and(removed)
    }

    /// Returns the version that the `nisshi_schema_version` table in `storage`'s database records,
    /// or `None` when it records none.
    async fn recorded_version(storage: &Postgres) -> Result<Option<i32>> {
        storage
            .connection()
            .await?
            .query_opt("select version from nisshi_schema_version", &[])
            .await?
            .map(|row| row.try_get(0))
            .transpose()
            .map_err(Into::into)
    }

    async fn register(storage: &Postgres) -> Result<()> {
        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: storage.node,
                cluster_id: storage.cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await
    }

    /// Returns whether `storage`'s cluster is stored in its database.
    async fn cluster_is_stored(storage: &Postgres) -> Result<bool> {
        storage
            .connection()
            .await?
            .query_one(
                "select count(*) from cluster where name = $1",
                &[&storage.cluster],
            )
            .await?
            .try_get::<_, i64>(0)
            .map(|count| count == 1)
            .map_err(Into::into)
    }

    /// Builds a storage on `storage` through the factory, as a broker does at startup.
    ///
    /// The server reserves some connections for superusers. The other tests connect as a superuser
    /// and can hold every other connection. When `storage` logs in as a role that isn't a
    /// superuser, this function then waits up to 30 seconds for a free connection.
    async fn build_with_factory(storage: Url) -> Result<ArcDynStorage> {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);

        loop {
            let built = factory::PostgresFactory
                .build(StorageFactoryConfiguration {
                    node_id: 111,
                    cluster: alphanumeric_string(15),
                    advertised_listener: Url::parse("tcp://127.0.0.1:9092")?,
                    storage: storage.clone(),
                    schema_registry: None,
                    lake_house: None,
                    cancellation: Default::default(),
                })
                .await;

            match built {
                Err(Error::StartupFailed {
                    failure: StartupFailure::StorageUnreachable,
                    ref source,
                    ..
                }) if server_has_no_free_connection(source)
                    && std::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(500)).await
                }
                built => return built,
            }
        }
    }

    /// Returns whether the server refused a connection because it had no free connection.
    fn server_has_no_free_connection(error: &Error) -> bool {
        let Error::Pool(pool_error) = error else {
            return false;
        };

        matches!(
            pool_error.downcast_ref::<PoolError>(),
            Some(PoolError::Backend(err)) if err.code() == Some(&SqlState::TOO_MANY_CONNECTIONS)
        )
    }

    /// In an empty database, `ensure_schema` must create every table and record [`SCHEMA_VERSION`].
    #[tokio::test]
    async fn ensure_schema_creates_the_schema_on_an_empty_database() -> Result<()> {
        with_empty_database(
            "ensure_schema_creates_the_schema_on_an_empty_database",
            async |database| {
                let storage = database.storage()?;

                storage.ensure_schema().await?;

                assert_eq!(recorded_version(&storage).await?, Some(SCHEMA_VERSION));
                register(&storage).await?;
                assert!(cluster_is_stored(&storage).await?);
                Ok(())
            },
        )
        .await
    }

    /// A newer release set up this database with a schema that this release can use.
    /// `ensure_schema` must leave the tables, the data and the recorded version as they are.
    #[tokio::test]
    async fn ensure_schema_keeps_a_database_that_records_a_version() -> Result<()> {
        with_empty_database(
            "ensure_schema_keeps_a_database_that_records_a_version",
            async |database| {
                let storage = database.storage()?;
                storage.ensure_schema().await?;
                register(&storage).await?;
                database.record_schema_version(2, 1).await?;

                storage.ensure_schema().await?;

                assert_eq!(recorded_version(&storage).await?, Some(2));
                assert!(cluster_is_stored(&storage).await?);
                Ok(())
            },
        )
        .await
    }

    /// An earlier release created the tables in this database without recording a version.
    /// `ensure_schema` must record [`SCHEMA_VERSION`] and keep the data.
    #[tokio::test]
    async fn ensure_schema_records_the_version_of_a_database_created_without_one() -> Result<()> {
        with_empty_database(
            "ensure_schema_records_the_version_of_a_database_created_without_one",
            async |database| {
                let storage = database.storage()?;
                database.execute(SCHEMA).await?;
                register(&storage).await?;

                storage.ensure_schema().await?;

                assert_eq!(recorded_version(&storage).await?, Some(SCHEMA_VERSION));
                assert!(cluster_is_stored(&storage).await?);
                Ok(())
            },
        )
        .await
    }

    /// When an existing deployment keeps its tables in PostgreSQL's `public` namespace (which
    /// PostgreSQL calls a schema), `ensure_schema` must add the version table there too, even when
    /// the broker's search path puts another, empty namespace first. It must create nothing in that
    /// other namespace, because reads would then find empty copies of the tables instead of data.
    #[tokio::test]
    async fn ensure_schema_uses_the_namespace_that_holds_the_existing_tables() -> Result<()> {
        with_empty_database(
            "ensure_schema_uses_the_namespace_that_holds_the_existing_tables",
            async |database| {
                database.execute(SCHEMA).await?;
                database.execute("create schema first_on_path").await?;

                // tokio-postgres reads a `+` in the URL as a `+`, so the space is written as `%20`.
                let mut url = database.url()?;
                url.set_query(Some("options=-c%20search_path%3Dfirst_on_path%2Cpublic"));
                let storage = database.storage_at(&url)?;
                register(&storage).await?;

                storage.ensure_schema().await?;

                let tables_in_first_on_path = storage
                    .connection()
                    .await?
                    .query_one(
                        "select count(*) from pg_tables where schemaname = 'first_on_path'",
                        &[],
                    )
                    .await?
                    .try_get::<_, i64>(0)?;
                assert_eq!(tables_in_first_on_path, 0);
                assert_eq!(recorded_version(&storage).await?, Some(SCHEMA_VERSION));
                assert!(cluster_is_stored(&storage).await?);
                Ok(())
            },
        )
        .await
    }

    /// `ensure_schema` must wait for the schema lock on a database that records no version, so that
    /// two brokers never create the schema at the same time.
    #[tokio::test]
    async fn ensure_schema_waits_for_the_schema_lock() -> Result<()> {
        with_empty_database(
            "ensure_schema_waits_for_the_schema_lock",
            async |database| {
                let holder_storage = database.storage()?;
                let holder = holder_storage.connection().await?;
                holder
                    .batch_execute(&format!("select pg_advisory_lock({SCHEMA_LOCK})"))
                    .await?;

                let storage = database.storage()?;
                let setup = tokio::spawn(async move { storage.ensure_schema().await });
                tokio::time::sleep(Duration::from_millis(500)).await;
                assert!(!setup.is_finished());

                holder
                    .batch_execute(&format!("select pg_advisory_unlock({SCHEMA_LOCK})"))
                    .await?;
                setup.await??;

                assert_eq!(
                    recorded_version(&database.storage()?).await?,
                    Some(SCHEMA_VERSION)
                );
                Ok(())
            },
        )
        .await
    }

    /// Four brokers run `ensure_schema` at the same time on one empty database. Each one must
    /// succeed, and the database must record [`SCHEMA_VERSION`].
    #[tokio::test]
    async fn ensure_schema_succeeds_for_brokers_that_start_together() -> Result<()> {
        const BROKERS: usize = 4;

        with_empty_database(
            "ensure_schema_succeeds_for_brokers_that_start_together",
            async |database| {
                let storages = (0..BROKERS)
                    .map(|_| database.storage())
                    .collect::<Result<Vec<_>>>()?;

                for result in
                    future::join_all(storages.iter().map(|storage| storage.ensure_schema())).await
                {
                    result?;
                }

                assert_eq!(recorded_version(&storages[0]).await?, Some(SCHEMA_VERSION));
                Ok(())
            },
        )
        .await
    }

    /// A broker finds no schema, and then waits for the setup lock while another broker records
    /// the schema version. The broker must find the recorded version after the wait, and must not
    /// create the schema again.
    ///
    /// The database's transactions are REPEATABLE READ unless they ask for another level. The test
    /// therefore also fails when the setup transaction uses the server's default isolation level.
    #[tokio::test]
    async fn ensure_schema_finds_the_version_recorded_while_it_waits_for_the_lock() -> Result<()> {
        with_empty_database(
            "ensure_schema_finds_the_version_recorded_while_it_waits_for_the_lock",
            async |database| {
                database
                    .execute(&format!(
                        "alter database {} set default_transaction_isolation = 'repeatable read'",
                        database.name
                    ))
                    .await?;

                let other = database.storage()?;
                let mut c = other.connection().await?;
                let tx = c.transaction().await?;
                tx.batch_execute(&format!("select pg_advisory_xact_lock({SCHEMA_LOCK})"))
                    .await?;

                let waiting = database.storage()?;
                let (waited, created) = future::join(waiting.ensure_schema(), async {
                    // The version must be recorded only after the waiting broker has read that
                    // there is none, which it does before it asks for the lock. The wait has a
                    // limit, so that the test fails instead of hanging when `ensure_schema` fails
                    // before it asks for the lock.
                    let observer = database.storage()?.connection().await?;
                    timeout(Duration::from_secs(30), async {
                        while !observer
                            .query_one(
                                "select exists (select from pg_locks \
                                 where locktype = 'advisory' and not granted \
                                 and database = (select oid from pg_database \
                                 where datname = current_database()))",
                                &[],
                            )
                            .await?
                            .try_get::<_, bool>(0)?
                        {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        Ok::<_, Error>(())
                    })
                    .await
                    .map_err(|_| {
                        Error::Message("the broker didn't wait for the setup lock".into())
                    })??;

                    // The other broker creates only the version table, not the whole schema. A
                    // commit that changes many catalog entries makes the server refresh the waiting
                    // broker's cache of table names, which hides a check that reads that cache.
                    tx.batch_execute(crate::sql::SQL.get("nisshi_schema_version_create.sql")?)
                        .await?;
                    _ = tx
                        .execute(
                            crate::sql::SQL.get("nisshi_schema_version_insert.sql")?,
                            &[&SCHEMA_VERSION, &OLDEST_COMPATIBLE_SCHEMA_VERSION],
                        )
                        .await?;
                    tx.commit().await.map_err(Error::from)
                })
                .await;

                created?;
                waited?;
                assert_eq!(recorded_version(&waiting).await?, Some(SCHEMA_VERSION));
                Ok(())
            },
        )
        .await
    }

    /// The database is already set up, and the broker's database user can read and write the tables
    /// but can't create them. The broker must start.
    #[tokio::test]
    async fn factory_starts_without_create_permission_on_a_database_that_is_set_up() -> Result<()> {
        with_empty_database(
            "factory_starts_without_create_permission_on_a_database_that_is_set_up",
            async |database| {
                database.storage()?.ensure_schema().await?;
                let role = database.create_role().await?;
                database
                    .execute(&format!(
                        "grant select, insert, update, delete on all tables in schema public to {0}; \
                         grant usage on all sequences in schema public to {0}",
                        role.username()
                    ))
                    .await?;

                _ = build_with_factory(role).await?;
                Ok(())
            },
        )
        .await
    }

    /// The database is empty, and the broker's database user can't create tables. The broker must
    /// stop at startup with a `SchemaSetup` failure.
    #[tokio::test]
    async fn factory_fails_with_schema_setup_when_the_user_cannot_create_tables() -> Result<()> {
        with_empty_database(
            "factory_fails_with_schema_setup_when_the_user_cannot_create_tables",
            async |database| {
                let role = database.create_role().await?;

                let Err(Error::StartupFailed {
                    storage: reported,
                    failure,
                    source,
                }) = build_with_factory(role.clone()).await
                else {
                    panic!("expected a startup failure");
                };

                assert_eq!(failure, StartupFailure::SchemaSetup);
                assert_eq!(*reported, nisshi_schema::redact_url(&role));
                assert_eq!(reported.password(), None);
                assert_matches!(
                    &*source,
                    Error::TokioPostgres(err) if err.code() == Some(&SqlState::INSUFFICIENT_PRIVILEGE)
                );
                Ok(())
            },
        )
        .await
    }

    /// A newer release set up this database with a schema that this release can't use. The broker
    /// must stop at startup with an `IncompatibleSchema` error.
    #[tokio::test]
    async fn factory_refuses_a_database_whose_schema_this_release_cannot_use() -> Result<()> {
        with_empty_database(
            "factory_refuses_a_database_whose_schema_this_release_cannot_use",
            async |database| {
                database.storage()?.ensure_schema().await?;
                database.record_schema_version(2, 2).await?;

                let Err(Error::StartupFailed {
                    failure, source, ..
                }) = build_with_factory(database.url()?).await
                else {
                    panic!("expected a startup failure");
                };

                assert_eq!(failure, StartupFailure::SchemaSetup);
                assert_matches!(*source, Error::IncompatibleSchema { .. });
                Ok(())
            },
        )
        .await
    }

    /// A DescribeGroups whose storage can't get a connection must answer each group with
    /// `COORDINATOR_NOT_AVAILABLE`, which clients retry. A failed request instead closes the
    /// client's connection without a response.
    #[tokio::test]
    async fn describe_groups_answers_coordinator_not_available_without_a_connection() -> Result<()>
    {
        // The listener closes when this statement ends, so the engine's connection attempt to its
        // port fails at once. The test then runs without a PostgreSQL server.
        let closed_port = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await?
            .local_addr()?
            .port();

        let storage = Postgres::builder(&format!(
            "postgres://postgres:postgres@127.0.0.1:{closed_port}"
        ))?
        .cluster(alphanumeric_string(15))
        .node(111)
        .build();

        let group_ids = ["abc", "pqr"].map(String::from);

        assert_eq!(
            vec![
                NamedGroupDetail::error_code("abc".into(), ErrorCode::CoordinatorNotAvailable),
                NamedGroupDetail::error_code("pqr".into(), ErrorCode::CoordinatorNotAvailable),
            ],
            storage.describe_groups(Some(&group_ids), false).await?
        );

        Ok(())
    }

    /// A DescribeGroups must answer a stored group that can't be decoded with
    /// `UNKNOWN_SERVER_ERROR`, which clients report at once, and answer the other groups in the
    /// request as usual.
    #[tokio::test]
    async fn describe_groups_answers_unknown_server_error_for_an_undecodable_group() -> Result<()> {
        let cluster = alphanumeric_string(15);

        let Some(storage) = storage_with_schema(
            "describe_groups_answers_unknown_server_error_for_an_undecodable_group",
            &CONNECTION,
            &cluster,
        )
        .await?
        else {
            return Ok(());
        };

        storage
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.clone(),
                incarnation_id: Uuid::now_v7(),
                rack: None,
            })
            .await?;

        // `GroupDetail::default` records the current time as the group's inception, so a second
        // call returns a different detail. The test compares the answer with this value.
        let stored = GroupDetail::default();

        for group_id in ["healthy", "undecodable"] {
            _ = storage
                .update_group(group_id, stored.clone(), None)
                .await
                .map_err(|err| Error::Message(format!("{err:?}")))?;
        }

        // `42` is valid JSON, so the database accepts it, but it doesn't decode to a `GroupDetail`.
        let changed = storage
            .connection()
            .await?
            .execute(
                "update consumer_group_detail cgd set detail = '42'
                from consumer_group cg join cluster c on c.id = cg.cluster
                where cgd.consumer_group = cg.id and c.name = $1 and cg.name = $2",
                &[&cluster, &"undecodable"],
            )
            .await?;
        // The update must change the group's row. An update that changes no row leaves the group
        // decodable, and the test then fails for that reason instead of testing the engine.
        assert_eq!(1, changed);

        let group_ids = ["healthy", "undecodable", "missing"].map(String::from);

        assert_eq!(
            vec![
                NamedGroupDetail::found("healthy".into(), stored),
                NamedGroupDetail::error_code("undecodable".into(), ErrorCode::UnknownServerError),
                NamedGroupDetail::error_code("missing".into(), ErrorCode::GroupIdNotFound),
            ],
            storage.describe_groups(Some(&group_ids), false).await?
        );

        Ok(())
    }
}
