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

use crate::{
    database_file,
    schema::{RecordedSchema, should_create_schema},
    sql::{
        self, Cache, ColumnErrorCount, Query, QueryRow, QueryRowCursor, ROWS_PER_FETCH,
        consumer_group_select, default_hash, driver::KafkaTimestamp, driver::sqlite::StoredTime,
        header_fetch, idempotent_epoch_check, idempotent_sequence_check, list_offset,
        producer_epoch_current_for_producer, producer_epoch_for_current_txn, producer_epoch_insert,
        producer_epochs_select, producer_insert, producer_select_for_update, record_fetch,
        remove_comments, result_columns::ResultColumns, row::topic_name, topic_by_cluster,
        topic_configuration_select, topic_configuration_select_name, topic_configuration_upsert,
        topic_insert, topic_select, topic_select_name, topic_select_uuid, topition_insert,
        txn_offset_commit_tp_select_pending, txn_produce_offset_select_offset_range,
        txn_produce_offset_select_overlapping_txn, txn_produce_partition_check,
        txn_produce_partition_select, txn_select_name, txn_select_produced_topitions,
        virtual_topic_upsert, watermark_insert, watermark_select, watermark_select_for_write,
    },
};
use async_trait::async_trait;
use bytes::Bytes;
use deadpool::managed;
use libsql::{
    Connection, Database, Row, Rows, Statement, Transaction, TransactionBehavior,
    ffi::SQLITE_CONSTRAINT_UNIQUE,
    params::{IntoParams, Params},
};
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
    record::{Header, Record, deflated, inflated},
    to_timestamp,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use nisshi_schema::{
    Registry,
    lake::{House, LakeHouse as _},
    redact_url,
};
use nisshi_storage::{
    ArcDynStorage, BrokerRegistrationRequest, ChannelRequestLayer, CommittedOffset,
    DELETE_RECORDS_INVALID_LOW_WATERMARK, DeleteRecordsConcurrency, Error, FetchedRecord,
    FetchedSender, GroupDetail, ListOffsetResponse, METER, MetadataResponse, NamedGroupDetail,
    OffsetCommitRequest, OffsetStage, ProduceEpoch, ProducerIdResponse, RequestChannelService,
    RequestStorageService, RequestTimer, Result, ScramCredential, SemaphoreProxy, StartupFailure,
    Storage, StorageOperation, StoredEpochs, TopicId, Topition, TxnAddPartitionsRequest,
    TxnAddPartitionsResponse, TxnOffsetCommitRequest, TxnState, UpdateError, Version,
    bounded_channel, check_claim,
    config::{self, apply_op},
    delete_records_cutoff, epoch_is_exhausted, inflate, max_message_bytes, parse_storage_option,
    producer_claim,
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use policy_operation::PolicyOperation;
use rama::{Layer as _, Service as _};
use rand::{rng, seq::SliceRandom as _};
use regex::Regex;
use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fmt::Debug,
    marker::PhantomData,
    path::PathBuf,
    result,
    str::FromStr,
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, SystemTime},
};
use tokio::{fs::rename, sync::Semaphore, task::JoinSet, time::Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};
use url::Url;
use uuid::Uuid;

pub mod factory;
mod policy_operation;

macro_rules! include_sql {
    ($e: expr) => {
        remove_comments(include_str!($e))
    };
}

/// The query keys that [`PoolConnection::cached_statement`] prepares afresh on each call, because
/// they are rare and carry credentials that a cached statement would keep bound.
const UNCACHED_PREFIXES: &[&str] = &["scram_credential_"];

static SQL_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_sqlite_duration")
        .with_unit("ms")
        .with_description("The SQL request latencies in milliseconds")
        .build()
});

static CONNECT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_sqlite_connect_duration")
        .with_unit("ms")
        .with_description("The connection latencies in milliseconds")
        .build()
});

static PRODUCE_IN_TX_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_sqlite_produce_in_tx_duration")
        .with_unit("ms")
        .with_description("The produce in TX latencies in milliseconds")
        .build()
});

static TRANSACTION_WITH_BEHAVIOR_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_sqlite_transaction_with_behavior_duration")
        .with_unit("ms")
        .with_description("The transaction with behavior latencies in milliseconds")
        .build()
});

static TRANSACTION_COMMIT_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_sqlite_transaction_commit_duration")
        .with_unit("ms")
        .with_description("The transaction commit latencies in milliseconds")
        .build()
});

static ENGINE_REQUEST_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_sqlite_engine_request_duration")
        .with_unit("ms")
        .with_description("The engine latencies in milliseconds")
        .build()
});

static DELEGATE_REQUEST_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_sqlite_delegate_request_duration")
        .with_boundaries(
            [
                0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0,
                1000.0,
            ]
            .into(),
        )
        .with_unit("ms")
        .with_description("The engine latencies in milliseconds")
        .build()
});

static SQL_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_sqlite_requests")
        .with_description("The number of SQL requests made")
        .build()
});

static SQL_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_sqlite_error")
        .with_description("The SQL error count")
        .build()
});

fn elapsed_millis(start: SystemTime) -> u64 {
    start
        .elapsed()
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Txn {
    name: String,
    producer_id: i64,
    producer_epoch: i16,
    status: TxnState,
}

impl TryFrom<txn_produce_offset_select_overlapping_txn::Row> for Txn {
    type Error = Error;

    fn try_from(row: txn_produce_offset_select_overlapping_txn::Row) -> Result<Self, Self::Error> {
        let txn_produce_offset_select_overlapping_txn::Row {
            name,
            producer_id,
            producer_epoch,
            status,
        } = row;
        let status = status
            .map_or(Ok(TxnState::Begin), TxnState::try_from)
            .inspect_err(|err| error!(?err))?;

        Ok(Self {
            name,
            producer_id,
            producer_epoch,
            status,
        })
    }
}

fn is_unique_constraint(error: &libsql::Error) -> bool {
    matches!(
        error,
        libsql::Error::SqliteFailure(SQLITE_CONSTRAINT_UNIQUE, _)
    )
}

/// LibSQL/SQLite storage engine
///
#[derive(Clone, Debug)]
pub(crate) struct Delegate {
    cluster: String,
    node: i32,
    advertised_listener: Url,
    pool: Pool,

    schemas: Option<Registry>,

    lake: Option<House>,

    vacuum_into: Option<PathBuf>,

    maintenance: Arc<Semaphore>,
    compaction: CompactionMode,
}

#[derive(Clone, Debug)]
pub(crate) struct ConnectionManager {
    db: Arc<Mutex<Database>>,
    busy_timeout: Duration,
}

pub(crate) struct PoolConnection {
    connection: Connection,

    /// The statements that this connection has prepared, by query key.
    statements: Mutex<BTreeMap<String, Arc<Statement>>>,
}

/// A statement that a connection shares between calls, which can only be executed.
///
/// It offers no way to read rows: a `Row` or `Rows` reads its values from the statement, so a
/// reset under a live reader would change what the caller sees. It is reset when dropped, so
/// that a statement that stopped before its last row does not keep a read transaction open on
/// the connection.
struct CachedStatement(Arc<Statement>);

impl CachedStatement {
    fn parameter_count(&self) -> usize {
        self.0.parameter_count()
    }

    async fn execute(&self, params: Params) -> result::Result<usize, libsql::Error> {
        self.0.execute(params).await
    }
}

impl Drop for CachedStatement {
    fn drop(&mut self) {
        self.0.reset();
    }
}

impl Debug for PoolConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolConnection")
            .field("connection", &self.connection)
            .finish()
    }
}

impl PoolConnection {
    async fn journal_mode(connection: &Connection) -> Result<()> {
        let mut rows = connection.query("PRAGMA journal_mode = WAL", ()).await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            debug!(journal_mode = row.get_str(0)?);
        }

        Ok(())
    }

    async fn synchronous(connection: &Connection) -> Result<()> {
        _ = connection
            .execute("PRAGMA synchronous = normal", ())
            .await
            .inspect(|rows| debug!(rows))?;

        let mut rows = connection.query("PRAGMA synchronous", ()).await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            debug!(synchronous = row.get_str(0)?);
        }

        Ok(())
    }

    async fn wal_autocheckpoint(connection: &Connection) -> Result<()> {
        let mut rows = connection.query("PRAGMA wal_autocheckpoint", ()).await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            debug!(wal_autocheckpoint = row.get_str(0)?);
        }

        Ok(())
    }

    async fn journal_size_limit(connection: &Connection) -> Result<()> {
        let mut rows = connection.query("PRAGMA journal_size_limit", ()).await?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            debug!(journal_size_limit = row.get_str(0)?);
        }

        Ok(())
    }

    async fn foreign_keys(connection: &Connection) -> Result<()> {
        connection
            .execute("PRAGMA foreign_keys = ON", ())
            .await
            .map_err(Into::into)
            .and(Ok(()))
    }

    async fn init(connection: &Connection) -> Result<()> {
        Self::journal_mode(connection).await?;
        Self::synchronous(connection).await?;
        Self::wal_autocheckpoint(connection).await?;
        Self::journal_size_limit(connection).await?;
        Self::foreign_keys(connection).await?;
        Ok(())
    }

    async fn new(connection: Connection) -> Result<Self> {
        Self::init(&connection).await?;

        Ok(Self {
            connection,
            statements: Mutex::default(),
        })
    }

    #[instrument(skip(self))]
    async fn prepared_statement(&self, key: &str) -> Result<Statement, libsql::Error> {
        let sql = SQL
            .0
            .get(key)
            .ok_or(libsql::Error::Misuse(format!("Unknown cache key: {}", key)))?;

        self.connection.prepare(sql).await
    }

    /// Returns the connection's statement for the query key, preparing it on first use.
    ///
    /// libsql's `Connection::prepare` parses and plans the SQL on every call and keeps no
    /// cache. The statement is reset when the returned guard drops, and the guard can only
    /// execute, so only a caller that returns no row may use this method. A caller that returns
    /// a row uses [`Self::prepared_statement`].
    ///
    /// A reset keeps the values last bound, and libsql binds blobs and text by copying them, so
    /// a cached statement holds its last record, header or credential until its next use or
    /// until the connection is dropped. The SCRAM credential keys are rare administrative
    /// calls, so they are prepared afresh each time rather than cached.
    async fn cached_statement(&self, key: &str) -> Result<CachedStatement, libsql::Error> {
        if UNCACHED_PREFIXES
            .iter()
            .any(|prefix| key.starts_with(prefix))
        {
            return Ok(CachedStatement(Arc::new(
                self.prepared_statement(key).await?,
            )));
        }

        let cached = self
            .statements
            .lock()
            .map_err(|_| libsql::Error::Misuse("statement cache lock poisoned".into()))?
            .get(key)
            .cloned();

        let statement = match cached {
            Some(statement) => statement,
            None => {
                let statement = Arc::new(self.prepared_statement(key).await?);

                _ = self
                    .statements
                    .lock()
                    .map_err(|_| libsql::Error::Misuse("statement cache lock poisoned".into()))?
                    .insert(key.to_owned(), statement.clone());

                statement
            }
        };

        statement.reset();

        Ok(CachedStatement(statement))
    }

    fn attributes_for_error(&self, key: Option<&str>, error: &libsql::Error) -> Vec<KeyValue> {
        debug!(key, ?error);

        let mut attributes = if let Some(sql) = key {
            vec![KeyValue::new("key", sql.to_owned())]
        } else {
            vec![]
        };

        if let libsql::Error::SqliteFailure(code, _) = error {
            attributes.push(KeyValue::new("code", format!("{code}")));
        }

        attributes
    }

    #[instrument(skip_all)]
    async fn transaction(&self) -> Result<Transaction> {
        let start = SystemTime::now();

        self.connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await
            .inspect(|_tx| TRANSACTION_WITH_BEHAVIOR_DURATION.record(elapsed_millis(start), &[]))
            .inspect_err(|err| {
                error!(?err, elapsed_millis = elapsed_millis(start));

                SQL_ERROR.add(1, &self.attributes_for_error(None, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn commit(&self, tx: Transaction) -> Result<()> {
        let start = SystemTime::now();

        tx.commit()
            .await
            .inspect(|_| TRANSACTION_COMMIT_DURATION.record(elapsed_millis(start), &[]))
            .inspect_err(|err| {
                error!(?err, elapsed_millis = elapsed_millis(start));
                SQL_ERROR.add(1, &self.attributes_for_error(None, err)[..]);
            })
            .map_err(Into::into)
    }

    #[instrument(skip_all)]
    async fn query<P>(&self, sql: &str, params: P) -> result::Result<Rows, libsql::Error>
    where
        P: IntoParams,
    {
        // The params hold record data and SCRAM credentials, so we log only the SQL.
        debug!(sql);

        let start = SystemTime::now();

        let statement = self.prepared_statement(sql).await?;

        statement
            .query(params)
            .await
            .inspect(|rows| {
                debug!(?rows);

                SQL_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("sql", sql.to_owned())],
                );

                SQL_REQUESTS.add(1, &[KeyValue::new("sql", sql.to_owned())]);
            })
            .inspect_err(|err| {
                error!(?err, elapsed_millis = elapsed_millis(start));

                SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
            })
    }

    /// Runs `query`, and returns its rows, each read as `T` when the caller reaches it.
    async fn query_rows<T: QueryRow>(
        &self,
        query: Query<T>,
        params: impl IntoParams,
    ) -> Result<QueryRowCursor<Rows, T>> {
        let query = query.key();
        let rows = self.query(query, params).await?;
        QueryRowCursor::new(
            ResultColumns::new(query, &rows),
            rows,
            self.column_error_count(query),
        )
    }

    /// Returns the count of `query`'s column errors in `SQL_ERROR`, with the labels that
    /// [`Self::attributes_for_error`] gives the query.
    fn column_error_count(&self, query: &str) -> ColumnErrorCount {
        ColumnErrorCount::new(&SQL_ERROR, vec![KeyValue::new("key", query.to_owned())])
    }

    /// Runs `query`, and returns the first row of its result as `T`, or `None` when the result
    /// has no rows.
    ///
    /// The function drops the result before it returns, because SQLite can't commit while a
    /// `returning` statement is still open.
    async fn query_first<T: QueryRow>(
        &self,
        query: Query<T>,
        params: impl IntoParams,
    ) -> Result<Option<T>> {
        let mut rows = self.query_leading_rows(query, params, 1).await?;
        Ok(rows.pop())
    }

    /// Runs `query`, and returns at most `limit` of its rows, in order, each read as `T`. It
    /// records the query's SQL metrics, and drops the result before it returns.
    #[instrument(skip_all)]
    async fn query_leading_rows<T: QueryRow>(
        &self,
        query: Query<T>,
        params: impl IntoParams,
        limit: usize,
    ) -> Result<Vec<T>> {
        let query = query.key();

        // The params hold record data and SCRAM credentials, so we log only the SQL.
        debug!(sql = query);

        let start = SystemTime::now();

        let statement = self.prepared_statement(query).await?;

        let mut rows = statement
            .query(params)
            .await
            .inspect(|rows| debug!(?rows))
            .inspect_err(|err| self.record_query_error(query, start, err))?;

        let reader = T::reader(&ResultColumns::new(query, &rows))
            .inspect_err(|err| self.column_error_count(query).record(err))?;

        let mut leading = Vec::with_capacity(limit);

        // libSQL reports an error of the statement's first step from the first `next`, not from
        // `query`.
        while leading.len() < limit
            && let Some(row) = rows
                .next()
                .await
                .inspect(|row| debug!(?row))
                .inspect_err(|err| self.record_query_error(query, start, err))?
        {
            // A libSQL `Row` reads its values from the statement's current row, and the next
            // step replaces them, so the loop reads each row before it steps again.
            leading.push(
                T::read(&reader, &row)
                    .inspect_err(|err| self.column_error_count(query).record(err))?,
            );
        }

        let attributes = [KeyValue::new("sql", query.to_owned())];

        SQL_DURATION.record(elapsed_millis(start), &attributes);
        SQL_REQUESTS.add(1, &attributes);

        Ok(leading)
    }

    /// Logs `err` from `query`, started at `start`, and counts it in `SQL_ERROR`.
    ///
    /// A unique-constraint violation is logged at debug level and not counted, because some
    /// callers expect one, as a topic insert does for an existing topic.
    fn record_query_error(&self, query: &str, start: SystemTime, err: &libsql::Error) {
        if is_unique_constraint(err) {
            debug!(?err);
        } else {
            error!(?err, query, elapsed_millis = elapsed_millis(start));
            SQL_ERROR.add(1, &self.attributes_for_error(Some(query), err)[..]);
        }
    }

    #[instrument(skip_all)]
    async fn execute<P>(&self, sql: &str, params: P) -> result::Result<usize, libsql::Error>
    where
        P: IntoParams,
    {
        // The params hold record data and SCRAM credentials, so we log only the SQL.
        debug!(sql);
        let start = SystemTime::now();

        let statement = self.cached_statement(sql).await?;

        // A reset keeps the values last bound, so a placeholder that the caller did not bind
        // would take the previous call's value instead of failing as NULL.
        let params = params.into_params()?;
        let supplied = match &params {
            Params::None => 0,
            Params::Positional(values) => values.len(),
            Params::Named(values) => values.len(),
        };

        if supplied != statement.parameter_count() {
            return Err(libsql::Error::Misuse(format!(
                "sql={sql}: bound {supplied} parameters, statement has {}",
                statement.parameter_count()
            )));
        }

        statement
            .execute(params)
            .await
            .inspect(|rows| {
                let elapsed_millis = elapsed_millis(start);
                debug!(rows, elapsed_millis);

                SQL_DURATION.record(elapsed_millis, &[KeyValue::new("sql", sql.to_owned())]);
                SQL_REQUESTS.add(1, &[KeyValue::new("sql", sql.to_owned())]);
            })
            .inspect_err(|err| {
                error!(?err, sql, elapsed_millis = elapsed_millis(start));

                SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
            })
    }

    #[instrument(skip_all)]
    async fn query_opt<P>(&self, sql: &str, params: P) -> result::Result<Option<Row>, libsql::Error>
    where
        P: IntoParams,
    {
        // The params hold record data and SCRAM credentials, so we log only the SQL.
        debug!(sql);

        let start = SystemTime::now();

        let statement = self.prepared_statement(sql).await?;

        let mut rows = statement.query(params).await.inspect_err(|err| {
            error!(?err, elapsed_millis = elapsed_millis(start));
            SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
        })?;

        let row = rows.next().await.inspect_err(|err| {
            error!(?err, elapsed_millis = elapsed_millis(start));
            SQL_ERROR.add(1, &self.attributes_for_error(Some(sql), err)[..]);
        })?;

        let attributes = [KeyValue::new("sql", sql.to_owned())];

        SQL_DURATION.record(elapsed_millis(start), &attributes);
        SQL_REQUESTS.add(1, &attributes);

        Ok(row)
    }

    /// Runs `query`, and returns the one row of its result as `T`.
    ///
    /// Returns [`Error::LibSql`] with [`libsql::Error::Misuse`] when the result has no rows or more
    /// than one row.
    async fn query_one<T: QueryRow>(&self, query: Query<T>, params: impl IntoParams) -> Result<T> {
        let mut rows = self.query_leading_rows(query, params, 2).await?;
        let query = query.key();

        match (rows.pop(), rows.pop()) {
            (Some(row), None) => Ok(row),
            (None, _) => Err(libsql::Error::Misuse(format!("sql={query}: no rows")).into()),
            (Some(_), Some(_)) => {
                Err(libsql::Error::Misuse(format!("sql={query}: more than one row")).into())
            }
        }
    }

    /// Returns the newest epoch of `producer` in `cluster`, or `None` when the producer does not
    /// exist or has no epoch.
    async fn producer_epoch_current(&self, cluster: &str, producer: i64) -> Result<Option<i16>> {
        self.query_first(
            producer_epoch_current_for_producer::QUERY,
            (cluster, producer),
        )
        .await
        .map(|row| row.map(|row| row.epoch))
    }

    /// Returns the producer that owns `transaction_id` in `cluster`, the newest of its epochs
    /// with a transaction, and that transaction's status, or `None` when there is no such epoch.
    async fn producer_epoch_for_current_txn(
        &self,
        cluster: &str,
        transaction_id: &str,
    ) -> Result<Option<producer_epoch_for_current_txn::Row>> {
        self.query_first(
            producer_epoch_for_current_txn::QUERY,
            (cluster, transaction_id),
        )
        .await
    }

    /// Adds a producer to `cluster`, and returns its ID.
    async fn producer_insert(&self, cluster: &str) -> Result<i64> {
        self.query_one(producer_insert::QUERY, &[cluster])
            .await
            .map(|row| row.id)
    }

    /// Adds the next epoch to `producer` in `cluster`, and returns it.
    ///
    /// Returns [`Error::LibSql`] with [`libsql::Error::Misuse`] when the producer does not exist,
    /// or when its newest epoch is `i16::MAX`.
    async fn producer_epoch_insert(&self, cluster: &str, producer: i64) -> Result<i16> {
        self.query_one(producer_epoch_insert::QUERY, (cluster, producer))
            .await
            .map(|row| row.epoch)
    }
}

impl managed::Manager for ConnectionManager {
    type Type = PoolConnection;
    type Error = Error;

    #[instrument(skip_all)]
    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let start = SystemTime::now();

        let connection = {
            let db = self.db.lock()?;
            db.connect()
        }?;

        connection.busy_timeout(self.busy_timeout)?;

        PoolConnection::new(connection)
            .await
            .inspect(|_| CONNECT_DURATION.record(elapsed_millis(start), &[]))
    }

    /// Rejects a connection still inside a transaction, for example one whose
    /// rollback failed when its `Transaction` was dropped. Such a connection
    /// holds SQLite's write lock. Rejecting it makes the pool drop it, which
    /// closes the handle and rolls the transaction back, and hand out a fresh
    /// connection instead.
    async fn recycle(
        &self,
        obj: &mut Self::Type,
        _metrics: &managed::Metrics,
    ) -> managed::RecycleResult<Self::Error> {
        if obj.connection.is_autocommit() {
            Ok(())
        } else {
            // deadpool discards a recycle error without logging it.
            warn!("discarding a pooled connection left inside an open transaction");

            Err(managed::RecycleError::message(
                "connection is inside an open transaction",
            ))
        }
    }
}

pub(crate) type Pool = managed::Pool<ConnectionManager>;

impl Delegate {
    /// Applies every change in `resource` to its topic in one transaction, so
    /// that a failed change leaves the topic's configuration unchanged.
    async fn alter_topic(&self, resource: &AlterConfigsResource) -> Result<()> {
        let c = self.connection().await?;
        let tx = c.transaction().await?;

        let topic = resource.resource_name.as_str();

        if c.query_opt(
            topic_select_name::QUERY.key(),
            (self.cluster.as_str(), topic),
        )
        .await?
        .is_none()
        {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        for config in resource.configs.as_deref().unwrap_or_default() {
            let op = OpType::try_from(config.config_operation)?;
            let name = config.name.as_str();

            let current = if matches!(op, OpType::Append | OpType::Subtract) {
                c.query_first(
                    topic_configuration_select_name::QUERY,
                    (self.cluster.as_str(), topic, name),
                )
                .await?
                .and_then(|row| row.value)
            } else {
                None
            };

            match apply_op(
                ConfigResource::Topic,
                name,
                current.as_deref(),
                op,
                config.value.as_deref(),
            )? {
                Some(value) => {
                    _ = c
                        .query_opt(
                            topic_configuration_upsert::QUERY.key(),
                            (self.cluster.as_str(), topic, name, value.as_str()),
                        )
                        .await?;
                }

                None => {
                    _ = c
                        .query_opt(
                            sql::TOPIC_CONFIGURATION_DELETE,
                            (self.cluster.as_str(), topic, name),
                        )
                        .await?;
                }
            }
        }

        c.commit(tx).await
    }

    #[instrument(skip_all)]
    async fn connection(&self) -> Result<managed::Object<ConnectionManager>> {
        let start = SystemTime::now();

        self.pool.get().await.map_err(Into::into).inspect(|_| {
            CONNECT_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("cluster_id", self.cluster.clone())],
            )
        })
    }

    #[instrument(skip_all)]
    async fn idempotent_message_check(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
        connection: &PoolConnection,
    ) -> Result<()> {
        let epochs = connection
            .query_first(
                producer_epochs_select::QUERY,
                (
                    self.cluster.as_str(),
                    deflated.producer_id,
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await
            .inspect_err(|err| error!(?err))?;

        if let Some(epochs) = epochs {
            let stored = StoredEpochs::from(epochs);

            if idempotent_epoch_check(transaction_id, stored, deflated)? == ProduceEpoch::Start {
                _ = connection
                    .execute(
                        sql::PRODUCER_EPOCH_INSERT_GIVEN,
                        (
                            self.cluster.as_str(),
                            deflated.producer_id,
                            deflated.producer_epoch,
                        ),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;
            }

            let producer_select_for_update::Row { sequence } = connection
                .query_one(
                    producer_select_for_update::QUERY,
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        deflated.producer_id,
                        deflated.producer_epoch,
                    ),
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
                connection
                    .execute(
                        sql::PRODUCER_DETAIL_INSERT,
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                            deflated.producer_id,
                            deflated.producer_epoch,
                            increment,
                        ),
                    )
                    .await?
            );

            Ok(())
        } else {
            Err(Error::Api(ErrorCode::UnknownProducerId))
        }
    }

    /// Returns what a write to the partition reads first: the partition's low and high watermarks,
    /// and its topic's stored `max.message.bytes`. Fails with `UNKNOWN_TOPIC_OR_PARTITION` when the
    /// partition does not exist.
    #[instrument(skip_all)]
    async fn watermark_select_for_write(
        &self,
        topition: &Topition,
        connection: &PoolConnection,
    ) -> Result<watermark_select_for_write::Row> {
        debug!(?topition);

        connection
            .query_first(
                watermark_select_for_write::LITE_QUERY,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await
            .inspect_err(|err| error!(?err, cluster = ?self.cluster, ?topition))?
            .ok_or(Error::Api(ErrorCode::UnknownTopicOrPartition))
    }

    /// Refuses a client's transactional batch for a partition that is not in its producer's
    /// ongoing transaction (see [`txn_produce_partition_check`]). EndTxn deletes a transaction's
    /// `txn_topition` rows, so a row is present only while the transaction is ongoing.
    async fn txn_produce_partition_check(
        &self,
        transaction_id: &str,
        topition: &Topition,
        deflated: &deflated::Batch,
        connection: &PoolConnection,
    ) -> Result<()> {
        let row = connection
            .query_first(
                txn_produce_partition_select::QUERY,
                (
                    self.cluster.as_str(),
                    transaction_id,
                    deflated.producer_id,
                    deflated.producer_epoch,
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await
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
        inflated: inflated::Batch,
        connection: &PoolConnection,
    ) -> Result<i64> {
        let start = SystemTime::now();

        let topic = topition.topic();
        let partition = topition.partition();

        if deflated.is_idempotent() {
            self.idempotent_message_check(transaction_id, topition, &deflated, connection)
                .await
                .inspect_err(|err| error!(?err))?;
        }

        if let Some(transaction_id) = transaction_id
            && deflated.is_transactional()
            && !deflated.is_control()
        {
            self.txn_produce_partition_check(transaction_id, topition, &deflated, connection)
                .await?;
        }

        debug!(after_idempotent_check = elapsed_millis(start));

        let watermarks = self
            .watermark_select_for_write(topition, connection)
            .await
            .inspect_err(|err| debug!(?err))?;

        debug!(after_watermark_select_for_write = elapsed_millis(start));

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

        debug!(after_attributes = elapsed_millis(start));

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

        debug!(after_validation = elapsed_millis(start));

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
            for (delta, record) in inflated.records.iter().enumerate() {
                debug!(delta, elapsed = elapsed_millis(start));

                let delta = i64::try_from(delta)?;
                let offset = high.unwrap_or_default() + delta;
                let key = record.key.as_deref();
                let value = record.value.as_deref();

                debug!(?delta, ?offset);

                _ = connection
                    .execute(
                        sql::RECORD_INSERT,
                        (
                            self.cluster.as_str(),
                            topic,
                            partition,
                            offset,
                            produced_attributes,
                            if transaction_id.is_none() {
                                None
                            } else {
                                Some(inflated.producer_id)
                            },
                            if transaction_id.is_none() {
                                None
                            } else {
                                Some(inflated.producer_epoch)
                            },
                            inflated.record_timestamp(record),
                            key,
                            value,
                        ),
                    )
                    .await
                    .inspect_err(|err| error!(?err, ?topic, ?partition, ?offset, key_len = ?key.map(<[u8]>::len), value_len = ?value.map(<[u8]>::len)))
                    .map_err(unique_constraint(ErrorCode::UnknownServerError))?;

                debug!(delta, after_record_insert = elapsed_millis(start));

                for (ordinal, header) in record.headers.iter().enumerate() {
                    let ordinal = i32::try_from(ordinal)?;
                    let key = header.key.as_deref();
                    let value = header.value.as_deref();

                    _ = connection
                        .execute(
                            sql::HEADER_INSERT,
                            (
                                self.cluster.as_str(),
                                topic,
                                partition,
                                offset,
                                ordinal,
                                key,
                                value,
                            ),
                        )
                        .await
                        .inspect_err(|err| {
                            error!(?err, ?topic, ?partition, ?offset, key_len = ?key.map(<[u8]>::len), value_len = ?value.map(<[u8]>::len));
                        });
                }

                debug!(delta, after_header_insert = elapsed_millis(start));
            }

            debug!(after_record_insert = elapsed_millis(start));

            if let Some(transaction_id) = transaction_id
                && attributes.transaction
            {
                let offset_start = high.unwrap_or_default();
                let offset_end = high.map_or(last_offset_delta, |high| high + last_offset_delta);

                _ = connection
                        .execute(
                            sql::TXN_PRODUCE_OFFSET_INSERT,
                            (
                                self.cluster.as_str(),
                                transaction_id,
                                inflated.producer_id,
                                inflated.producer_epoch,
                                topic,
                                partition,
                                offset_start,
                                offset_end,
                            ),
                        )
                        .await
                        .inspect(|n| debug!(cluster = ?self.cluster, ?transaction_id, ?inflated.producer_id, ?inflated.producer_epoch, ?topic, ?partition, ?offset_start, ?offset_end, ?n))
                        .inspect_err(|err| error!(?err))?;
            }

            debug!(after_some_transaction_id = elapsed_millis(start));
        }

        _ = connection
            .execute(
                sql::WATERMARK_UPDATE,
                (
                    self.cluster.as_str(),
                    topic,
                    partition,
                    low.unwrap_or_default(),
                    high.map_or(last_offset_delta + 1, |high| high + last_offset_delta + 1),
                ),
            )
            .await
            .inspect(|n| debug!(?n, after_watermark_update = elapsed_millis(start)))
            .inspect_err(|err| error!(?err))?;

        if !attributes.control
            && let Some(ref lake) = self.lake
        {
            let config = self
                .describe_config(topition.topic(), ConfigResource::Topic, None)
                .await
                .inspect_err(|err| error!(?err))?;

            lake.store(
                topition.topic(),
                topition.partition(),
                high.unwrap_or_default(),
                &inflated,
                config,
            )
            .await
            .inspect_err(|err| error!(?err))?;
        }

        debug!(after_all_done = elapsed_millis(start));

        Ok(high.unwrap_or_default()).inspect(|_| {
            PRODUCE_IN_TX_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("cluster_id", self.cluster.clone())],
            );
        })
    }

    async fn end_in_tx(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        connection: &PoolConnection,
    ) -> Result<ErrorCode> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?producer_id, ?producer_epoch, ?committed);

        let mut overlaps = vec![];

        let mut rows = connection
            .query_rows(
                txn_select_produced_topitions::QUERY,
                (
                    self.cluster.as_str(),
                    transaction_id,
                    producer_id,
                    producer_epoch,
                ),
            )
            .await?;

        while let Some(txn_select_produced_topitions::Row { topic, partition }) =
            rows.next().await?
        {
            let topition = Topition::new(topic.clone(), partition);

            debug!(?topition);

            let control_batch: Bytes = if committed {
                ControlBatch::default().commit().try_into()?
            } else {
                ControlBatch::default().abort().try_into()?
            };
            let end_transaction_marker: Bytes = EndTransactionMarker::default().try_into()?;

            let batch = inflated::Batch::builder()
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
            // while this transaction holds the database write lock.
            let inflated = inflated::Batch::try_from(&batch)?;

            let offset = self
                .produce_in_tx(Some(transaction_id), &topition, batch, inflated, connection)
                .await?;

            debug!(offset, ?topition);

            let row = connection
                .query_first(
                    txn_produce_offset_select_offset_range::QUERY,
                    (
                        self.cluster.as_str(),
                        transaction_id,
                        producer_id,
                        producer_epoch,
                        topic.as_str(),
                        partition,
                    ),
                )
                .await?;

            if let Some(txn_produce_offset_select_offset_range::Row {
                offset_start,
                offset_end,
            }) = row
            {
                debug!(offset_start, offset_end);

                let mut rows = connection
                    .query_rows(
                        txn_produce_offset_select_overlapping_txn::QUERY,
                        (
                            self.cluster.as_str(),
                            transaction_id,
                            producer_id,
                            producer_epoch,
                            topic.as_str(),
                            partition,
                            offset_end,
                        ),
                    )
                    .await?;

                while let Some(row) = rows.next().await? {
                    overlaps.push(Txn::try_from(row).inspect(|txn| debug!(?txn))?);
                }
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

                _ = connection
                    .execute(
                        sql::TXN_PRODUCE_OFFSET_DELETE_BY_TXN,
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                _ = connection
                    .execute(
                        sql::TXN_TOPITION_DELETE_BY_TXN,
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                if txn.status == TxnState::PrepareCommit {
                    _ = connection
                        .execute(
                            sql::CONSUMER_OFFSET_INSERT_FROM_TXN,
                            (
                                self.cluster.as_str(),
                                txn.name.as_str(),
                                txn.producer_id,
                                txn.producer_epoch,
                            ),
                        )
                        .await?;
                }

                _ = connection
                    .execute(
                        sql::TXN_OFFSET_COMMIT_TP_DELETE_BY_TXN,
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                _ = connection
                    .execute(
                        sql::TXN_OFFSET_COMMIT_DELETE_BY_TXN,
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                let outcome = if txn.status == TxnState::PrepareCommit {
                    String::from(TxnState::Committed)
                } else if txn.status == TxnState::PrepareAbort {
                    String::from(TxnState::Aborted)
                } else {
                    String::from(txn.status)
                };

                _ = connection
                    .execute(
                        sql::TXN_STATUS_UPDATE,
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                            outcome,
                        ),
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

            _ = connection
                .execute(
                    sql::TXN_STATUS_UPDATE,
                    (
                        self.cluster.as_str(),
                        transaction_id,
                        producer_id,
                        producer_epoch,
                        outcome.as_str(),
                    ),
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

    #[instrument(skip(self), ret)]
    async fn policy_compact_delete(&self, topition: i64, offset_id: i64) -> Result<u64> {
        let pc = self.connection().await?;

        pc.execute(sql::LITE_POLICY_COMPACT_DELETE, (topition, offset_id))
            .await
            .map(|rows| rows as u64)
            .inspect(|rows| debug!(rows))
            .map_err(Into::into)
    }

    #[instrument(skip(self, key))]
    async fn policy_compact_compaction(
        &self,
        topition: i64,
        key: &[u8],
        max_offset_id: i64,
    ) -> Result<Vec<i64>> {
        let pc = self.connection().await?;

        let mut rows = pc
            .query_rows(
                sql::lite_policy_compact_compaction::QUERY,
                (topition, key, max_offset_id),
            )
            .await?;

        let mut offsets = Vec::new();

        while let Some(sql::lite_policy_compact_compaction::Row { offset_id: offset }) =
            rows.next().await?
        {
            offsets.push(offset);
        }

        Ok(offsets)
    }

    #[instrument(skip(self, key))]
    async fn policy_compact_max_offset_id(&self, topition: i64, key: &[u8]) -> Result<Option<i64>> {
        let pc = self.connection().await?;

        let row = pc
            .query_first(
                sql::lite_policy_compact_max_offset_id::QUERY,
                (topition, key),
            )
            .await?;

        let max_offset = row.map(|row| row.max_offset_id);
        debug!(max_offset);
        Ok(max_offset)
    }

    #[instrument(skip(self))]
    async fn policy_compact_distinct_k(&self, topition: i64) -> Result<BTreeSet<Vec<u8>>> {
        let pc = self.connection().await?;

        let mut rows = pc
            .query_rows(sql::lite_policy_compact_distinct_k::QUERY, [topition])
            .await?;

        let mut keys = BTreeSet::new();

        while let Some(row) = rows.next().await? {
            if let Some(key) = row.key {
                _ = keys.insert(Vec::from(key));
            }
        }

        debug!(keys = keys.len());

        Ok(keys)
    }

    #[instrument(skip(self))]
    async fn policy_compact_topitions(&self) -> Result<BTreeSet<i64>> {
        let pc = self.connection().await?;

        let mut rows = pc
            .query_rows(
                sql::lite_policy_compact_topitions::QUERY,
                [self.cluster.as_str()],
            )
            .await?;

        let mut topitions = BTreeSet::new();

        while let Some(sql::lite_policy_compact_topitions::Row { topition }) = rows.next().await? {
            _ = topitions.insert(topition);
        }

        Ok(topitions)
    }

    #[instrument(skip(self))]
    async fn policy_compact(&self) -> Result<u64> {
        let start = SystemTime::now();

        match self.compaction {
            CompactionMode::Single => {
                let pc = self.connection().await?;

                pc.execute(sql::POLICY_COMPACT, [self.cluster.as_str()])
                    .await
                    .map(|compacted| compacted as u64)
                    .inspect(|_| {
                        DELEGATE_REQUEST_DURATION.record(
                            elapsed_millis(start),
                            &[PolicyOperation::CompactSingle.attribute()],
                        )
                    })
                    .map_err(Into::into)
            }
            CompactionMode::Multi => {
                let mut compacted = 0;

                for topition in self.policy_compact_topitions().await? {
                    debug!(topition);

                    for key in self.policy_compact_distinct_k(topition).await? {
                        debug!(key_len = key.len());

                        if let Some(max_offset_id) = self
                            .policy_compact_max_offset_id(topition, &key[..])
                            .await?
                        {
                            debug!(max_offset_id);

                            for offset in self
                                .policy_compact_compaction(topition, &key[..], max_offset_id)
                                .await?
                            {
                                debug!(offset);

                                compacted += self.policy_compact_delete(topition, offset).await?;
                            }
                        }
                    }
                }

                Ok(compacted).inspect(|_| {
                    DELEGATE_REQUEST_DURATION.record(
                        elapsed_millis(start),
                        &[PolicyOperation::CompactMulti.attribute()],
                    )
                })
            }
        }
    }

    #[instrument(skip(self))]
    async fn vacuum_into(&self) -> Result<()> {
        if let Some(vacuum_into) = self
            .vacuum_into
            .as_deref()
            .inspect(|vacuum_into| debug!(vacuum_into = vacuum_into.to_str()))
        {
            let mut temporary = PathBuf::from(vacuum_into);
            if temporary.add_extension("temporary") {
                debug!(temporary = temporary.to_str());

                if let Some(vacuum_temporary) = temporary.to_str() {
                    let pc = self.connection().await?;
                    let rows = pc
                        .execute(sql::LITE_VACUUM_INTO, [vacuum_temporary])
                        .await? as u64;

                    rename(temporary, vacuum_into).await?;
                    debug!(vacuum_into = vacuum_into.to_str(), rows);
                }
            }
        }

        Ok(())
    }

    #[instrument(skip(self, now), ret)]
    async fn policy_delete(&self, now: SystemTime) -> Result<u64> {
        let start = SystemTime::now();

        let now = to_timestamp(&now)?;
        let retention_ms = u64::try_from(config::DEFAULT_RETENTION.as_millis())?;

        let pc = self.connection().await?;

        let by_age = pc
            .execute(
                sql::LITE_POLICY_DELETE,
                (self.cluster.as_str(), now, retention_ms),
            )
            .await?;

        pc.execute(sql::LITE_POLICY_DELETE_BYTES, [self.cluster.as_str()])
            .await
            .map(|by_bytes| (by_age + by_bytes) as u64)
            .map_err(Into::into)
            .inspect(|_| {
                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[PolicyOperation::Delete.attribute()],
                )
            })
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

        c.query_one(
            virtual_topic_upsert::QUERY,
            (self.cluster.as_str(), topic, key, uuid.to_string()),
        )
        .await
        .map(|row| row.uuid)
        .inspect(|vt| debug!(%vt))
    }
}

#[derive(Clone, Default, Debug)]
pub struct Builder<C, N, L, D> {
    cluster: C,
    node: N,
    advertised_listener: L,
    storage: D,
    schemas: Option<Registry>,
    lake: Option<House>,
    cancellation: CancellationToken,
}

impl<C, N, L, D> Builder<C, N, L, D> {
    pub(crate) fn cluster<T>(self, cluster: T) -> Builder<String, N, L, D>
    where
        T: Into<String>,
    {
        Builder {
            cluster: cluster.into(),
            node: self.node,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            schemas: self.schemas,
            lake: self.lake,
            cancellation: self.cancellation,
        }
    }

    pub(crate) fn node(self, node: i32) -> Builder<C, i32, L, D> {
        debug!(node);
        Builder {
            cluster: self.cluster,
            node,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            schemas: self.schemas,
            lake: self.lake,
            cancellation: self.cancellation,
        }
    }

    pub(crate) fn advertised_listener(self, advertised_listener: Url) -> Builder<C, N, Url, D> {
        debug!(%advertised_listener);
        Builder {
            cluster: self.cluster,
            node: self.node,
            advertised_listener,
            storage: self.storage,
            schemas: self.schemas,
            lake: self.lake,
            cancellation: self.cancellation,
        }
    }

    pub(crate) fn storage(self, storage: Url) -> Builder<C, N, L, Url> {
        debug!(storage = %redact_url(&storage));
        Builder {
            cluster: self.cluster,
            node: self.node,
            advertised_listener: self.advertised_listener,
            storage,
            schemas: self.schemas,
            lake: self.lake,
            cancellation: self.cancellation,
        }
    }

    pub(crate) fn schemas(self, schemas: Option<Registry>) -> Builder<C, N, L, D> {
        Self { schemas, ..self }
    }

    pub(crate) fn lake(self, lake: Option<House>) -> Self {
        Self { lake, ..self }
    }

    pub(crate) fn cancellation(self, cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            ..self
        }
    }
}

/// The version of the schema that [`DDL`] creates. A database records the version of its schema
/// in its `nisshi_schema_version` table.
const SCHEMA_VERSION: i32 = 1;

// The build fails here when someone changes `SCHEMA_VERSION`. In a file with tables but no
// recorded version, `ensure_schema` creates only the missing tables, and then records
// `SCHEMA_VERSION`. A new version needs an upgrade step there, because such a file has the
// tables of version 1.
const _: () = assert!(SCHEMA_VERSION == 1);

/// The oldest schema version whose broker can use the schema that [`DDL`] creates.
const OLDEST_COMPATIBLE_SCHEMA_VERSION: i32 = 1;

static DDL: LazyLock<Cache> = LazyLock::new(|| {
    let mapping = [
        ("010-cluster.sql", include_sql!("ddl/010-cluster.sql")),
        (
            "020-consumer-group.sql",
            include_sql!("ddl/020-consumer-group.sql"),
        ),
        ("020-producer.sql", include_sql!("ddl/020-producer.sql")),
        (
            "020-scram-credential.sql",
            include_sql!("ddl/020-scram-credential.sql"),
        ),
        ("020-topic.sql", include_sql!("ddl/020-topic.sql")),
        (
            "030-consumer-group-detail.sql",
            include_sql!("ddl/030-consumer-group-detail.sql"),
        ),
        (
            "030-producer-epoch.sql",
            include_sql!("ddl/030-producer-epoch.sql"),
        ),
        (
            "030-topic-configuration.sql",
            include_sql!("ddl/030-topic-configuration.sql"),
        ),
        ("030-topition.sql", include_sql!("ddl/030-topition.sql")),
        ("030-txn.sql", include_sql!("ddl/030-txn.sql")),
        (
            "030-virtual-topic.sql",
            include_sql!("ddl/030-virtual-topic.sql"),
        ),
        (
            "040-consumer-offset.sql",
            include_sql!("ddl/040-consumer-offset.sql"),
        ),
        ("040-header.sql", include_sql!("ddl/040-header.sql")),
        (
            "040-producer-detail.sql",
            include_sql!("ddl/040-producer-detail.sql"),
        ),
        ("040-record.sql", include_sql!("ddl/040-record.sql")),
        ("040-txn-detail.sql", include_sql!("ddl/040-txn-detail.sql")),
        ("040-watermark.sql", include_sql!("ddl/040-watermark.sql")),
        (
            "050-txn-offset-commit.sql",
            include_sql!("ddl/050-txn-offset-commit.sql"),
        ),
        (
            "050-txn-topition.sql",
            include_sql!("ddl/050-txn-topition.sql"),
        ),
        (
            "060-txn-offset-commit-tp.sql",
            include_sql!("ddl/060-txn-offset-commit-tp.sql"),
        ),
        (
            "060-txn-produce-offset.sql",
            include_sql!("ddl/060-txn-produce-offset.sql"),
        ),
    ];

    Cache::new(BTreeMap::from(mapping))
});

/// Makes sure that the database has the broker's schema before the broker serves a request.
///
/// When the database records no schema version, this function creates the missing tables and
/// records [`SCHEMA_VERSION`]. It leaves existing tables and their data as they are.
async fn ensure_schema(db: &Database, busy_timeout: Duration) -> Result<()> {
    let connection = db.connect()?;

    // Another broker on the same file can hold the write lock while it creates the schema. The
    // busy timeout makes this broker wait for that lock, instead of failing at once.
    connection.busy_timeout(busy_timeout)?;

    // An immediate transaction takes the write lock before it reads the version. Two brokers that
    // start together then read the version one at a time, and only the first one applies the
    // schema.
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await?;

    _ = tx
        .execute(SQL.get(sql::NISSHI_SCHEMA_VERSION_CREATE)?, ())
        .await?;

    let mut rows = tx
        .query(SQL.get(sql::nisshi_schema_version_select::QUERY.key())?, ())
        .await?;
    let reader = sql::nisshi_schema_version_select::RowReader::new(&ResultColumns::new(
        sql::nisshi_schema_version_select::QUERY.key(),
        &rows,
    ))?;
    let recorded = match rows.next().await? {
        Some(row) => {
            let sql::nisshi_schema_version_select::Row {
                version,
                oldest_compatible_version,
            } = reader.read_row(&row)?;

            Some(RecordedSchema {
                version,
                oldest_compatible_version,
            })
        }
        None => None,
    };
    debug!(?recorded);

    if !should_create_schema(recorded, SCHEMA_VERSION)? {
        return tx.commit().await.map_err(Into::into);
    }

    let has_tables = tx
        .query(
            "select 1 from sqlite_schema where type = 'table' and name = 'cluster'",
            (),
        )
        .await?
        .next()
        .await?
        .is_some();

    for (name, ddl) in DDL.iter() {
        _ = tx
            .execute(ddl.as_str(), ())
            .await
            .inspect(|rows| debug!(name, rows))
            .inspect_err(|err| error!(name, ?err))?;
    }

    _ = tx
        .execute(
            SQL.get(sql::NISSHI_SCHEMA_VERSION_INSERT)?,
            [SCHEMA_VERSION, OLDEST_COMPATIBLE_SCHEMA_VERSION],
        )
        .await?;

    tx.commit().await?;

    if has_tables {
        warn!(
            schema_version = SCHEMA_VERSION,
            "recorded the schema version in a file whose tables an earlier release created"
        );
    } else {
        info!(schema_version = SCHEMA_VERSION, "created the schema");
    }

    Ok(())
}

pub(crate) static SQL: LazyLock<Cache> = LazyLock::new(|| {
    Cache::new(
        sql::SQL
            .iter()
            .map(|(name, sql)| fix_parameters(sql).map(|sql| (*name, sql)))
            .collect::<Result<BTreeMap<_, _>>>()
            .unwrap_or_default(),
    )
});

fn fix_parameters(sql: &str) -> Result<String> {
    Regex::new(r"\$(?<i>\d+)")
        .map(|re| re.replace_all(sql, "?$i").into_owned())
        .map_err(Into::into)
}

#[derive(Clone, Debug)]
pub struct Engine {
    inner: RequestChannelService,
}

impl Engine {
    pub fn builder()
    -> Builder<PhantomData<String>, PhantomData<i32>, PhantomData<Url>, PhantomData<Url>> {
        Builder::default()
    }

    /// Sends a produce of `deflated` to the delegate, and records how long it took. Both produce
    /// methods call this, so that each produce opens its span and records its duration in one
    /// place.
    #[instrument(name = "produce", skip_all)]
    async fn produce_batch(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        inflated: Option<inflated::Batch>,
    ) -> Result<i64> {
        let start = SystemTime::now();

        match inflated {
            Some(inflated) => {
                self.inner
                    .produce_inflated(transaction_id, topition, deflated, inflated)
                    .await
            }

            None => self.inner.produce(transaction_id, topition, deflated).await,
        }
        .inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::Produce.attribute()],
            )
        })
    }
}

#[async_trait]
impl Storage for Engine {
    #[instrument(skip_all)]
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        let start = SystemTime::now();
        self.inner
            .register_broker(broker_registration)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::RegisterBroker.attribute()],
                )
            })
    }

    #[instrument(skip_all)]
    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let start = SystemTime::now();
        self.inner.brokers().await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::Brokers.attribute()],
            )
        })
    }

    #[instrument(skip_all)]
    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        let start = SystemTime::now();
        self.inner
            .create_topic(topic, validate_only)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::CreateTopic.attribute()],
                )
            })
    }

    fn delete_records_concurrency(&self) -> DeleteRecordsConcurrency {
        self.inner.delete_records_concurrency()
    }

    #[instrument(skip_all)]
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let start = SystemTime::now();
        self.inner.delete_records(topics).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::DeleteRecords.attribute()],
            )
        })
    }

    #[instrument(skip_all)]
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        let start = SystemTime::now();
        self.inner.delete_topic(topic).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::DeleteTopic.attribute()],
            )
        })
    }

    #[instrument(skip_all)]
    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let start = SystemTime::now();
        self.inner
            .incremental_alter_resource(resource)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::IncrementalAlterResource.attribute()],
                )
            })
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
        inflated: inflated::Batch,
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
        let timer = RequestTimer::new(
            &ENGINE_REQUEST_DURATION,
            StorageOperation::Fetch,
            Instant::now(),
        );

        let fetched = self
            .inner
            .fetch(
                topition,
                offset,
                min_bytes,
                max_bytes,
                isolation_level,
                fetched_sender,
            )
            .await;

        timer.end(&fetched);
        fetched
    }

    #[instrument(skip_all)]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let start = SystemTime::now();
        self.inner.offset_stage(topition).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::OffsetStage.attribute()],
            )
        })
    }

    #[instrument(skip_all)]
    async fn offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let start = SystemTime::now();
        self.inner
            .offset_commit(group, retention, offsets)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::OffsetCommit.attribute()],
                )
            })
    }

    #[instrument(skip_all)]
    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        let start = SystemTime::now();
        self.inner
            .committed_offset_topitions(group_id)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::CommittedOffsetTopitions.attribute()],
                )
            })
    }

    #[instrument(skip_all)]
    async fn pending_txn_offsets(&self, group_id: &str) -> Result<BTreeSet<Topition>> {
        let start = SystemTime::now();
        self.inner.pending_txn_offsets(group_id).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::PendingTxnOffsets.attribute()],
            )
        })
    }

    #[instrument(skip_all)]
    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        let start = SystemTime::now();
        self.inner
            .offset_fetch(group_id, topics, require_stable)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::OffsetFetch.attribute()],
                )
            })
    }

    #[instrument(skip_all)]
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let start = SystemTime::now();
        self.inner
            .list_offsets(isolation_level, offsets)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::ListOffsets.attribute()],
                )
            })
    }

    #[instrument(skip_all)]
    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let start = SystemTime::now();
        self.inner.metadata(topics).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::Metadata.attribute()],
            )
        })
    }

    #[instrument(skip_all)]
    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let start = SystemTime::now();
        self.inner
            .describe_config(name, resource, keys)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::DescribeConfig.attribute()],
                )
            })
    }

    #[instrument(skip_all)]
    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let start = SystemTime::now();
        self.inner
            .describe_topic_partitions(topics, partition_limit, cursor)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::DescribeTopicPartitions.attribute()],
                )
            })
    }

    #[instrument(skip_all)]
    async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
        let start = SystemTime::now();
        self.inner.list_groups().await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::ListGroups.attribute()],
            )
        })
    }

    #[instrument(skip_all)]
    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let start = SystemTime::now();
        self.inner.delete_groups(group_ids).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::DeleteGroups.attribute()],
            )
        })
    }

    #[instrument(skip_all)]
    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let start = SystemTime::now();
        self.inner
            .describe_groups(group_ids, include_authorized_operations)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::DescribeGroups.attribute()],
                )
            })
    }

    #[instrument(skip_all)]
    async fn group_with_version(&self, group_id: &str) -> Result<Option<(GroupDetail, Version)>> {
        let start = SystemTime::now();
        self.inner.group_with_version(group_id).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::GroupWithVersion.attribute()],
            )
        })
    }

    #[instrument(skip_all)]
    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let start = SystemTime::now();
        self.inner
            .update_group(group_id, detail, version)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::UpdateGroup.attribute()],
                )
            })
    }

    #[instrument(skip_all)]
    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        let start = SystemTime::now();
        self.inner
            .init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            )
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::InitProducer.attribute()],
                )
            })
    }

    #[instrument(skip_all)]
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        let start = SystemTime::now();
        self.inner
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::TxnAddOffsets.attribute()],
                )
            })
    }

    #[instrument(skip_all)]
    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        let start = SystemTime::now();
        self.inner
            .txn_add_partitions(partitions)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::TxnAddPartitions.attribute()],
                )
            })
    }

    #[instrument(skip_all)]
    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let start = SystemTime::now();
        self.inner.txn_offset_commit(offsets).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::TxnOffsetCommit.attribute()],
            )
        })
    }

    #[instrument(skip_all)]
    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let start = SystemTime::now();
        self.inner
            .txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::TxnEnd.attribute()],
                )
            })
    }

    #[instrument(skip_all)]
    async fn maintain(&self, now: SystemTime) -> Result<()> {
        let start = SystemTime::now();
        self.inner.maintain(now).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::Maintain.attribute()],
            )
        })
    }

    #[instrument(skip_all)]
    async fn maintain_transactions(&self, now: SystemTime) -> Result<()> {
        let start = SystemTime::now();
        self.inner.maintain_transactions(now).await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::MaintainTransactions.attribute()],
            )
        })
    }

    #[instrument(skip_all)]
    async fn aborted_transactions(
        &self,
        topition: &Topition,
        offset: i64,
        last_stable_offset: i64,
    ) -> Result<Vec<AbortedTransaction>> {
        let start = SystemTime::now();
        self.inner
            .aborted_transactions(topition, offset, last_stable_offset)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::AbortedTransactions.attribute()],
                )
            })
    }

    #[instrument(skip_all)]
    async fn cluster_id(&self) -> Result<String> {
        let start = SystemTime::now();
        self.inner.cluster_id().await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::ClusterId.attribute()],
            )
        })
    }

    #[instrument(skip_all)]
    async fn node(&self) -> Result<i32> {
        let start = SystemTime::now();
        self.inner.node().await.inspect(|_| {
            ENGINE_REQUEST_DURATION
                .record(elapsed_millis(start), &[StorageOperation::Node.attribute()])
        })
    }

    #[instrument(skip_all)]
    async fn advertised_listener(&self) -> Result<Url> {
        let start = SystemTime::now();
        self.inner.advertised_listener().await.inspect(|_| {
            ENGINE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::AdvertisedListener.attribute()],
            )
        })
    }

    #[instrument(skip_all)]
    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        let start = SystemTime::now();
        self.inner
            .delete_user_scram_credential(user, mechanism)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::DeleteUserScramCredential.attribute()],
                )
            })
    }

    #[instrument(skip_all)]
    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let start = SystemTime::now();
        self.inner
            .upsert_user_scram_credential(user, mechanism, credential)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::UpsertUserScramCredential.attribute()],
                )
            })
    }

    #[instrument(skip_all)]
    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        let start = SystemTime::now();
        self.inner
            .user_scram_credential(user, mechanism)
            .await
            .inspect(|_| {
                ENGINE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::UserScramCredential.attribute()],
                )
            })
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        let start = SystemTime::now();
        self.inner.ping().await.inspect(|_| {
            ENGINE_REQUEST_DURATION
                .record(elapsed_millis(start), &[StorageOperation::Ping.attribute()])
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum CompactionMode {
    Single,
    #[default]
    Multi,
}

impl FromStr for CompactionMode {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "single" => Ok(Self::Single),
            "multi" => Ok(Self::Multi),
            otherwise => Err(Error::Message(otherwise.to_owned())),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum CommunicationMode {
    Mpsc,
    Direct,
    #[default]
    Semaphore,
}

impl FromStr for CommunicationMode {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "direct" => Ok(Self::Direct),
            "mpsc" => Ok(Self::Mpsc),
            "semaphore" => Ok(Self::Semaphore),
            otherwise => Err(Error::Message(otherwise.to_owned())),
        }
    }
}

const BUSY_TIMEOUT: &str = "busy_timeout";
const COMPACTION: &str = "compaction";
const MODE: &str = "mode";

/// What `busy_timeout` takes. Every example here must parse (see the tests).
const BUSY_TIMEOUT_HINT: &str = "a duration, such as 5s or 500ms";

/// What `compaction` takes. Every value here must parse (see the tests).
const COMPACTION_HINT: &str = "single or multi";

/// What `mode` takes. Every value here must parse (see the tests).
const MODE_HINT: &str = "semaphore, mpsc or direct";

/// The storage URL options of the SQLite engine that take a parsed value. An option
/// that the URL doesn't give takes its default.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LiteOptions {
    busy_timeout: Duration,
    compaction: CompactionMode,
    mode: CommunicationMode,
}

impl Default for LiteOptions {
    fn default() -> Self {
        Self {
            busy_timeout: Duration::from_secs(5),
            compaction: CompactionMode::default(),
            mode: CommunicationMode::default(),
        }
    }
}

impl LiteOptions {
    /// Reads the options from `storage`.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidStorageOptionValue`] when an option's value doesn't parse.
    fn parse(storage: &Url) -> Result<Self> {
        let defaults = Self::default();

        Ok(Self {
            busy_timeout: parse_storage_option(
                storage,
                BUSY_TIMEOUT,
                BUSY_TIMEOUT_HINT,
                |value| {
                    human_units::Duration::from_str(value)
                        .ok()
                        .map(|duration| duration.0)
                },
            )?
            .unwrap_or(defaults.busy_timeout),

            compaction: parse_storage_option(storage, COMPACTION, COMPACTION_HINT, |value| {
                CompactionMode::from_str(value).ok()
            })?
            .unwrap_or(defaults.compaction),

            mode: parse_storage_option(storage, MODE, MODE_HINT, |value| {
                CommunicationMode::from_str(value).ok()
            })?
            .unwrap_or(defaults.mode),
        })
    }
}

impl Builder<String, i32, Url, Url> {
    pub(crate) async fn build(self) -> Result<ArcDynStorage> {
        debug!(domain = self.storage.domain(), path = self.storage.path());

        // Read the options before the database file is opened, so that a value that
        // doesn't parse stops startup before the file is created.
        let LiteOptions {
            busy_timeout,
            compaction,
            mode,
        } = LiteOptions::parse(&self.storage)?;

        let path = env::current_dir()
            .inspect(|current_dir| debug!(?current_dir))
            .map(|current_dir| database_file::path(current_dir, &self.storage))?;

        debug!(?path);

        let vacuum_into = self.storage.query_pairs().find_map(|(k, v)| {
            if k == "vacuum_into" {
                Some(PathBuf::from(v.as_ref()))
            } else {
                None
            }
        });

        let db = libsql::Builder::new_local(path).build().await?;

        ensure_schema(&db, busy_timeout)
            .await
            .map_err(|source| Error::StartupFailed {
                storage: Arc::new(redact_url(&self.storage)),
                failure: StartupFailure::SchemaSetup,
                source: Arc::new(source),
            })?;

        match mode {
            CommunicationMode::Mpsc => {
                let (sender, receiver) = bounded_channel(1);
                let mut server = JoinSet::new();

                let _ = {
                    let cancellation = self.cancellation.clone();

                    let storage = Delegate {
                        cluster: self.cluster,
                        node: self.node,
                        advertised_listener: self.advertised_listener,
                        pool: Pool::builder(ConnectionManager {
                            db: Arc::new(Mutex::new(db)),
                            busy_timeout,
                        })
                        .build()?,
                        schemas: self.schemas,
                        lake: self.lake,
                        vacuum_into,
                        maintenance: Arc::new(Semaphore::new(1)),
                        compaction,
                    };

                    server.spawn(async move {
                        let server = ChannelRequestLayer::new(cancellation)
                            .into_layer(RequestStorageService::new(storage));

                        server.serve(receiver).await
                    })
                };

                // The supervisor loop above isolates each request's storage work in its
                // own child task (see `ChannelRequestService::serve`), so a panic handling
                // one request does not end this loop. Nothing should ordinarily end it
                // either: it runs until the channel closes (every `RequestSender`, held by
                // `Engine.inner`, is dropped) or `cancellation` fires. This JoinSet holds
                // exactly one task; rather than leave it unread for the life of `Engine`,
                // this watches it and logs if it ever ends - belt and braces for a future
                // bug, not something this should ever observe in practice.
                drop(tokio::spawn(async move {
                    match server.join_next().await {
                        Some(Ok(Ok(()))) => {
                            debug!("mpsc storage supervisor ended (channel closed or cancelled)")
                        }
                        Some(Ok(Err(error))) => error!(
                            ?error,
                            "mpsc storage supervisor ended with an error; storage unavailable for this engine"
                        ),
                        Some(Err(join_error)) => error!(
                            ?join_error,
                            "mpsc storage supervisor panicked; storage unavailable for this engine"
                        ),
                        None => unreachable!("exactly one task was ever spawned onto this JoinSet"),
                    }
                }));

                let inner = RequestChannelService::new(sender);

                Ok(Arc::new(Box::new(Engine { inner }) as Box<dyn Storage>))
            }

            CommunicationMode::Direct => Ok(Arc::new(Box::new(Delegate {
                cluster: self.cluster,
                node: self.node,
                advertised_listener: self.advertised_listener,
                pool: Pool::builder(ConnectionManager {
                    db: Arc::new(Mutex::new(db)),
                    busy_timeout,
                })
                .build()?,
                schemas: self.schemas,
                lake: self.lake,
                vacuum_into,
                maintenance: Arc::new(Semaphore::new(1)),
                compaction,
            }) as Box<dyn Storage>)),

            CommunicationMode::Semaphore => Ok(Arc::new(Box::new(SemaphoreProxy::new(Delegate {
                cluster: self.cluster,
                node: self.node,
                advertised_listener: self.advertised_listener,
                pool: Pool::builder(ConnectionManager {
                    db: Arc::new(Mutex::new(db)),
                    busy_timeout,
                })
                .build()?,
                schemas: self.schemas,
                lake: self.lake,
                vacuum_into,
                maintenance: Arc::new(Semaphore::new(1)),
                compaction,
            })) as Box<dyn Storage>)),
        }
    }
}

/// Returns a function that maps a unique-constraint violation to [`Error::Api`] with
/// `error_code`, and returns every other error as it is.
fn unique_constraint<E: Into<Error>>(error_code: ErrorCode) -> impl Fn(E) -> Error {
    move |err| match err.into() {
        Error::LibSql(err) if is_unique_constraint(&err) => {
            debug!(?err);
            Error::Api(error_code)
        }

        err => err,
    }
}

/// Reads a `consumer_group_detail.sql` row as the stored group and the version that
/// `update_group` compares against.
fn group_detail_with_version(
    row: sql::consumer_group_detail::Row,
) -> Result<(GroupDetail, Version)> {
    let version = Version {
        e_tag: Some(row.e_tag.to_string()),
        version: None,
    };
    debug!(?version);

    let current = row
        .detail
        .map_err(Error::from)
        .inspect(|value| debug!(%value))
        .and_then(|value| serde_json::from_value::<GroupDetail>(value).map_err(Into::into))
        .inspect(|current| debug!(?current))
        .inspect_err(|err| error!(?err))?;

    Ok((current, version))
}

impl Delegate {
    /// Writes `deflated` to `topition` in one transaction, with `inflated` as its decoded records,
    /// or with the records that this decodes from `deflated` when the caller passed none. Both
    /// produce methods call this, so that each produce decodes, writes and records its duration in
    /// one place.
    async fn produce_batch(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        inflated: Option<inflated::Batch>,
    ) -> Result<i64> {
        // This runs while the delegate serves the request, so the wait for a decode permit holds
        // the engine's single permit. A caller that passes `inflated` skips this decode.
        //
        // The batch is inflated before the transaction starts, so the wait
        // for a decode permit and the decode do not hold a connection or the
        // database write lock.
        let inflated = match inflated {
            Some(inflated) => inflated,
            None => inflate(deflated.clone())
                .await
                .inspect_err(|err| debug!(?err))?,
        };

        let start = SystemTime::now();

        let pc = self.connection().await?;

        let tx = pc.transaction().await.inspect(|_| {
            debug!(after_produce_transaction = elapsed_millis(start));
        })?;

        let high = self
            .produce_in_tx(transaction_id, topition, deflated, inflated, &pc)
            .await
            .inspect(|_| {
                debug!(after_produce_in_tx = elapsed_millis(start));
            })
            .inspect_err(|err| match err {
                // A rejection the client caused: ProduceService logs it once,
                // at warn, with the topic and partition.
                Error::Api(_)
                | Error::MessageTooLarge { .. }
                | Error::SansIo(nisshi_sans_io::Error::MessageMaxSizeExceeded(_)) => {
                    debug!(?err)
                }
                err => error!(?err),
            })?;

        pc.commit(tx)
            .await
            .and(Ok(high))
            .inspect_err(|err| error!(?err))
            .inspect(|_| {
                debug!(after_produce_commit = elapsed_millis(start));

                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::Produce.attribute()],
                )
            })
    }
}

#[async_trait]
impl Storage for Delegate {
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        let start = SystemTime::now();

        debug!(?broker_registration);

        let connection = self.connection().await?;

        connection
            .execute(
                sql::REGISTER_BROKER,
                &[broker_registration.cluster_id.as_str()],
            )
            .await
            .map_err(Into::into)
            .and(Ok(()))
            .inspect(|_| {
                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::RegisterBroker.attribute()],
                )
            })
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let start = SystemTime::now();

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
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::Brokers.attribute()],
            )
        })
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?topic, validate_only);

        let pc = self.connection().await?;
        let tx = pc.transaction().await?;

        let uuid = {
            let uuid = Uuid::new_v4();

            let parameters = (
                self.cluster.as_str(),
                topic.name.as_str(),
                uuid.to_string(),
                topic.num_partitions,
                (topic.replication_factor as i32),
            );

            pc.query_one(topic_insert::QUERY, parameters.clone())
                .await
                .map_err(unique_constraint(ErrorCode::TopicAlreadyExists))
                .inspect(|row| debug!(?parameters, ?row))
                .map(|row| row.uuid)
        }
        .inspect(|uuid| debug!(?uuid))
        .inspect_err(|err| error!(?err))?;

        for partition in 0..topic.num_partitions {
            let params = (self.cluster.as_str(), topic.name.as_str(), partition);

            let topition = pc.query_first(topition_insert::QUERY, params).await?;
            debug!(?topition);

            let watermark = pc.query_first(watermark_insert::QUERY, params).await?;
            debug!(?watermark);
        }

        if let Some(configs) = topic.configs {
            for config in configs {
                debug!(?config);

                let params = (
                    self.cluster.as_str(),
                    topic.name.as_str(),
                    config.name.as_str(),
                    config.value.as_deref(),
                );

                let id = pc
                    .query_one(topic_configuration_upsert::QUERY, params)
                    .await
                    .inspect_err(|err| error!(?err, ?config))?;
                debug!(?id, ?config);
            }
        }

        pc.commit(tx).await.and(Ok(uuid)).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::CreateTopic.attribute()],
            )
        })
    }

    /// SQLite has one writer. A call that finds the write lock held waits in the
    /// connection's busy handler, after the service's deadline check, so a
    /// second call at once gains nothing. This applies in direct mode too, where
    /// no proxy or channel serialises the calls.
    fn delete_records_concurrency(&self) -> DeleteRecordsConcurrency {
        DeleteRecordsConcurrency::serial()
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        debug!(?topics);

        let pc = self.connection().await?;
        let tx = pc.transaction().await?;

        let mut responses = vec![];

        for topic in topics {
            let mut partition_responses = vec![];

            if let Some(ref partitions) = topic.partitions {
                for partition in partitions {
                    let topition = Topition::new(topic.name.as_str(), partition.partition_index);

                    let (error_code, low_watermark) = match self
                        .watermark_select_for_write(&topition, &pc)
                        .await
                    {
                        Err(Error::Api(error_code)) => {
                            (error_code, DELETE_RECORDS_INVALID_LOW_WATERMARK)
                        }
                        Err(err) => return Err(err),

                        Ok(watermark_select_for_write::Row { low, high, .. }) => {
                            let stage = OffsetStage {
                                last_stable: high.unwrap_or_default(),
                                high_watermark: high.unwrap_or_default(),
                                log_start: low.unwrap_or_default(),
                            };

                            match delete_records_cutoff(partition.offset, &stage) {
                                Err(error_code) => {
                                    (error_code, DELETE_RECORDS_INVALID_LOW_WATERMARK)
                                }

                                Ok(cutoff) => {
                                    if cutoff > stage.log_start {
                                        _ = pc
                                            .execute(
                                                sql::RECORD_DELETE_BY_OFFSET,
                                                (
                                                    self.cluster.as_str(),
                                                    topic.name.as_str(),
                                                    partition.partition_index,
                                                    cutoff,
                                                ),
                                            )
                                            .await
                                            .inspect_err(|err| error!(?err, ?topition, cutoff))?;
                                    }

                                    _ = pc
                                        .execute(
                                            sql::WATERMARK_UPDATE,
                                            (
                                                self.cluster.as_str(),
                                                topic.name.as_str(),
                                                partition.partition_index,
                                                cutoff,
                                                high.unwrap_or_default(),
                                            ),
                                        )
                                        .await
                                        .inspect_err(|err| error!(?err, ?topition))?;

                                    (ErrorCode::None, cutoff)
                                }
                            }
                        }
                    };

                    partition_responses.push(
                        DeleteRecordsPartitionResult::default()
                            .partition_index(partition.partition_index)
                            .low_watermark(low_watermark)
                            .error_code(error_code.into()),
                    );
                }
            }

            responses.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_responses)),
            );
        }

        pc.commit(tx).await.inspect_err(|err| error!(?err))?;

        Ok(responses)
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        let start = SystemTime::now();
        debug!(cluster = self.cluster, ?topic);

        let pc = self.connection().await?;
        let tx = pc.transaction().await?;

        let topic_name = match topic {
            TopicId::Id(id) => pc
                .query_first(
                    topic_select_uuid::NAME_QUERY,
                    (self.cluster.as_str(), id.to_string().as_str()),
                )
                .await?
                .map(|row| row.name),

            TopicId::Name(name) => pc
                .query_first(
                    topic_select_name::NAME_QUERY,
                    (self.cluster.as_str(), name.as_str()),
                )
                .await?
                .map(|row| row.name),
        };

        let Some(topic_name) = topic_name else {
            return Ok(ErrorCode::UnknownTopicOrPartition);
        };

        let topic_name = topic_name.as_str();

        for sql in [
            sql::CONSUMER_OFFSET_DELETE_BY_TOPIC,
            sql::TOPIC_CONFIGURATION_DELETE_BY_TOPIC,
            sql::WATERMARK_DELETE_BY_TOPIC,
            sql::HEADER_DELETE_BY_TOPIC,
            sql::RECORD_DELETE_BY_TOPIC,
            sql::TXN_OFFSET_COMMIT_TP_DELETE_BY_TOPIC,
            sql::TXN_PRODUCE_OFFSET_DELETE_BY_TOPIC,
            sql::TXN_TOPITION_DELETE_BY_TOPIC,
            sql::PRODUCER_DETAIL_DELETE_BY_TOPIC,
            sql::TOPITION_DELETE_BY_TOPIC,
        ] {
            let rows = pc.execute(sql, (self.cluster.as_str(), topic_name)).await?;

            debug!(?topic, rows, sql)
        }

        _ = pc
            .execute(sql::TOPIC_DELETE_BY, (self.cluster.as_str(), topic_name))
            .await?;

        pc.commit(tx).await.and(Ok(ErrorCode::None)).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::DeleteTopic.attribute()],
            )
        })
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let start = SystemTime::now();
        debug!(?resource);

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
                .inspect(|_| {
                    DELEGATE_REQUEST_DURATION.record(
                        elapsed_millis(start),
                        &[StorageOperation::IncrementalAlterResource.attribute()],
                    )
                })
            }
            ConfigResource::Unknown => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name))
            .inspect(|_| {
                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::IncrementalAlterResource.attribute()],
                )
            }),
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
        inflated: inflated::Batch,
    ) -> Result<i64> {
        self.produce_batch(transaction_id, topition, deflated, Some(inflated))
            .await
    }

    async fn fetch(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
        fetched_sender: FetchedSender,
    ) -> Result<()> {
        let timer = RequestTimer::new(
            &DELEGATE_REQUEST_DURATION,
            StorageOperation::Fetch,
            Instant::now(),
        );

        let fetched = async {
            debug!(?topition, offset, min_bytes, max_bytes, ?isolation_level);

            let (base_topic, key_filter): (&str, Option<&str>) =
                self.topic_with_key(topition.topic()).await?;

            let offset_stage = self.offset_stage(topition).await?;

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

            let c = self.connection().await?;

            let mut records = if let Some(key) = key_filter {
                let key_bytes = key.as_bytes().to_vec();
                c.query_rows(
                    record_fetch::KEYED_QUERY,
                    (
                        self.cluster.as_str(),
                        base_topic,
                        topition.partition(),
                        offset,
                        (max_bytes as i64),
                        high_watermark,
                        key_bytes,
                        ROWS_PER_FETCH,
                    ),
                )
                .await
            } else {
                c.query_rows(
                    record_fetch::QUERY,
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                        offset,
                        (max_bytes as i64),
                        high_watermark,
                        ROWS_PER_FETCH,
                    ),
                )
                .await
            }
            .inspect_err(|err| error!(?err))?;

            let mut data = fetched_sender.send_offsets(offset_stage).await?;

            // This loop can run past the read's deadline by the time that it takes to read one full
            // channel of records, with the header query of each. The caller stops the read at its
            // deadline only while the read waits. libSQL reads each row synchronously inside `next`,
            // without waiting, so the read waits only when its channel is full.
            //
            // A step that waits for a lock blocks for up to the connection's busy timeout, so the
            // read can overrun its deadline by that much. In WAL mode a read waits for a lock only
            // rarely, such as while a checkpoint restarts the log. The busy timeout stays as
            // configured instead of shrinking to the time left, because it applies to each wait and
            // is shared with every later user of the pooled connection.
            while let Some(record_fetch::Row {
                offset_id: offset,
                attributes,
                timestamp,
                key,
                value,
                producer_id,
                producer_epoch,
            }) = records.next().await?
            {
                debug!(offset, ?timestamp);
                debug!(key_len = ?key.as_ref().map(Bytes::len));
                debug!(value_len = ?value.as_ref().map(Bytes::len));

                let mut headers = vec![];

                let mut header_rows = c
                    .query_rows(
                        header_fetch::QUERY,
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                            offset,
                        ),
                    )
                    .await?;

                while let Some(header_fetch::Row { key, value }) = header_rows.next().await? {
                    headers.push(Header { key, value }.into());
                }

                data.send_record(FetchedRecord {
                    offset,
                    timestamp,
                    attributes: attributes.unwrap_or(0),
                    producer_id: producer_id.unwrap_or(-1),
                    producer_epoch: producer_epoch.unwrap_or(-1),
                    key,
                    value,
                    headers,
                })
                .await?;
            }

            Ok::<_, Error>(())
        }
        .await;

        timer.end(&fetched);
        fetched
    }

    #[instrument(skip(self))]
    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?topition);

        let c = self.connection().await?;

        let watermark_select::Row { low, high, stable } = c
            .query_one(
                watermark_select::QUERY,
                (
                    self.cluster.as_str(),
                    self.base_topic(topition.topic()).await?,
                    topition.partition(),
                ),
            )
            .await
            .inspect_err(|err| error!(?topition, ?err))?;

        let log_start = low.unwrap_or_default();
        let high_watermark = high.unwrap_or_default();
        let last_stable = stable.unwrap_or(high_watermark);

        debug!(cluster = self.cluster, ?topition, log_start, high_watermark,);

        Ok(OffsetStage {
            last_stable,
            high_watermark,
            log_start,
        })
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::OffsetStage.attribute()],
            )
        })
    }

    async fn offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?group, ?retention, ?offsets);

        let c = self.connection().await?;
        let tx = c.transaction().await?;

        let mut cg_inserted = false;

        let mut responses = vec![];

        for (topition, offset) in offsets {
            debug!(?topition, ?offset);

            let mut rows = c
                .query(
                    sql::TOPITION_SELECT,
                    (
                        self.cluster.as_str(),
                        self.base_topic(topition.topic()).await?,
                        topition.partition(),
                    ),
                )
                .await
                .inspect_err(|err| error!(?err))?;

            if rows.next().await.inspect_err(|err| error!(?err))?.is_some() {
                if !cg_inserted {
                    let rows = c
                        .execute(sql::CONSUMER_GROUP_INSERT, (self.cluster.as_str(), group))
                        .await?;
                    debug!(rows);

                    cg_inserted = true;
                }

                let rows = c
                    .execute(
                        sql::CONSUMER_OFFSET_INSERT,
                        (
                            self.cluster.as_str(),
                            self.base_topic(topition.topic()).await?,
                            topition.partition(),
                            group,
                            offset.offset,
                            offset.leader_epoch,
                            offset.timestamp.map(StoredTime::from),
                            offset.metadata.as_deref(),
                        ),
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

        c.commit(tx).await.inspect_err(|err| error!(?err))?;

        Ok(responses).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::OffsetCommit.attribute()],
            )
        })
    }

    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        let start = SystemTime::now();

        debug!(group_id);

        let mut results = BTreeMap::new();

        let c = self.connection().await?;

        let mut rows = c
            .query_rows(
                sql::consumer_offset_select_by_group::QUERY,
                (self.cluster.as_str(), group_id),
            )
            .await?;

        while let Some(sql::consumer_offset_select_by_group::Row {
            topic,
            partition,
            committed_offset,
            leader_epoch,
            metadata,
        }) = rows.next().await?
        {
            let committed = CommittedOffset {
                offset: committed_offset,
                leader_epoch,
                metadata,
            };

            debug!(group_id, topic, partition, ?committed);

            assert_eq!(
                None,
                results.insert(Topition::new(topic, partition), committed)
            );
        }

        Ok(results).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::CommittedOffsetTopitions.attribute()],
            )
        })
    }

    async fn pending_txn_offsets(&self, group_id: &str) -> Result<BTreeSet<Topition>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, group_id);

        let c = self.connection().await?;

        let mut rows = c
            .query_rows(
                txn_offset_commit_tp_select_pending::QUERY,
                (self.cluster.as_str(), group_id),
            )
            .await
            .inspect_err(|err| error!(?err, cluster = self.cluster, group_id))?;

        let mut pending = BTreeSet::new();

        while let Some(txn_offset_commit_tp_select_pending::Row { topic, partition }) =
            rows.next().await?
        {
            _ = pending.insert(Topition::new(topic, partition));
        }

        debug!(cluster = self.cluster, group_id, ?pending);

        Ok(pending).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::PendingTxnOffsets.attribute()],
            )
        })
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?group_id, ?topics, ?require_stable);

        let c = self.connection().await?;

        let mut offsets = BTreeMap::new();

        for topic in topics {
            let mut rows = c
                .query_rows(
                    sql::consumer_offset_select::QUERY,
                    (
                        self.cluster.as_str(),
                        group_id,
                        self.base_topic(topic.topic()).await?,
                        topic.partition(),
                    ),
                )
                .await
                .inspect_err(|err| {
                    error!(
                        ?err,
                        cluster = self.cluster,
                        group_id,
                        topic = topic.topic,
                        partition = topic.partition
                    )
                })?;

            let committed = rows
                .next()
                .await
                .map(|row| {
                    row.map_or(CommittedOffset::uncommitted(), |row| CommittedOffset {
                        offset: row.committed_offset,
                        leader_epoch: row.leader_epoch,
                        metadata: row.metadata,
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

        Ok(offsets).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::OffsetFetch.attribute()],
            )
        })
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?isolation_level, ?offsets);

        let c = self.connection().await?;

        let mut responses = vec![];

        for (topition, offset_type) in offsets {
            let query = match (offset_type, isolation_level) {
                (ListOffset::Earliest, _) => list_offset::EARLIEST_QUERY,
                (ListOffset::Latest, IsolationLevel::ReadCommitted) => {
                    list_offset::LATEST_COMMITTED_QUERY
                }
                (ListOffset::Latest, IsolationLevel::ReadUncommitted) => {
                    list_offset::LATEST_UNCOMMITTED_QUERY
                }
                (ListOffset::Timestamp(_), _) => list_offset::LATEST_TIMESTAMP_QUERY,
            };

            debug!(?query);

            let list_offset = match offset_type {
                ListOffset::Earliest | ListOffset::Latest => c
                    .query_first(
                        query,
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                        ),
                    )
                    .await
                    .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition)),

                ListOffset::Timestamp(timestamp) => c
                    .query_first(
                        query,
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                            StoredTime::from(*timestamp),
                        ),
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
                |list_offset::Row {
                     offset_id,
                     timestamp,
                 }|
                 -> Result<ListOffsetResponse> {
                    let offset = Some(offset_id);

                    // Only a Timestamp lookup converts the timestamp, because the service drops
                    // it for Earliest and Latest. Converting it for those two would fail a
                    // request on an instant that this platform's `SystemTime` cannot hold,
                    // although the request never uses it.
                    let timestamp = match offset_type {
                        ListOffset::Timestamp(_) => {
                            timestamp.map(KafkaTimestamp::system_time).transpose()?
                        }
                        ListOffset::Earliest | ListOffset::Latest => None,
                    };

                    debug!(
                        cluster = self.cluster,
                        ?topition,
                        ?offset_type,
                        offset,
                        ?timestamp
                    );

                    Ok(ListOffsetResponse {
                        timestamp,
                        offset,
                        ..Default::default()
                    })
                },
            )?;

            responses.push((topition.clone(), list_offset));
        }

        Ok(responses).inspect(|r| {
            debug!(?r);
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::ListOffsets.attribute()],
            )
        })
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let start = SystemTime::now();

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

                            let mut rows = c
                                .query_rows(
                                    topic_select_name::QUERY,
                                    (self.cluster.as_str(), base_topic),
                                )
                                .await?;

                            match rows.next().await {
                                Ok(Some(topic_select_name::Row {
                                    uuid,
                                    is_internal,
                                    partitions,
                                    replication_factor,
                                    ..
                                })) => {
                                    let error_code = ErrorCode::None.into();
                                    let topic_id = vtid.or(Some(uuid.into_bytes()));
                                    let is_internal = Some(is_internal);

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
                                    error!(?reason, ?topic);
                                    MetadataResponseTopic::default()
                                        .error_code(sql::TOPIC_READ_ERROR_CODE.into())
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

                            let mut rows = c
                                .query_rows(
                                    topic_select_uuid::QUERY,
                                    (self.cluster.as_str(), id.to_string().as_str()),
                                )
                                .await?;

                            match rows.next().await {
                                Ok(Some(topic_select_uuid::Row {
                                    uuid,
                                    name,
                                    is_internal,
                                    partitions,
                                    replication_factor,
                                })) => {
                                    let error_code = ErrorCode::None.into();
                                    let topic_id = Some(uuid.into_bytes());
                                    let name = Some(name);
                                    let is_internal = Some(is_internal);

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
                                Ok(None) => MetadataResponseTopic::default()
                                    .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                    .name(None)
                                    .topic_id(Some(id.into_bytes()))
                                    .is_internal(Some(false))
                                    .partitions(Some([].into()))
                                    .topic_authorized_operations(Some(-2147483648)),
                                Err(reason) => {
                                    error!(?reason, ?topic);
                                    MetadataResponseTopic::default()
                                        .error_code(sql::TOPIC_READ_ERROR_CODE.into())
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

                let mut rows = c
                    .query_rows(topic_by_cluster::QUERY, &[self.cluster.as_str()])
                    .await?;

                // The loop skips each row with an unreadable column, so that one corrupt row
                // does not hide the cluster's other topics. It leaves the topic out instead of
                // listing it with `UNKNOWN_SERVER_ERROR`, because the Java consumer's `listTopics`
                // fails for the whole cluster when one topic of the response has an error that it
                // can't retry ([`TopicMetadataFetcher.getTopicMetadata`]).
                //
                // [`TopicMetadataFetcher.getTopicMetadata`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/TopicMetadataFetcher.java#L117-L138
                while let Some(topic_by_cluster::Row {
                    uuid,
                    name,
                    is_internal,
                    partitions,
                    replication_factor,
                }) = rows.next_readable::<topic_name::Row>().await?
                {
                    let error_code = ErrorCode::None.into();
                    let topic_id = Some(uuid.into_bytes());
                    let name = Some(name);
                    let is_internal = Some(is_internal);

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

                responses
            }
        };

        Ok(MetadataResponse {
            cluster: Some(self.cluster.clone()),
            controller: Some(self.node),
            brokers,
            topics: responses,
        })
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::Metadata.attribute()],
            )
        })
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, name, ?resource, ?keys);

        let c = self.connection().await?;

        let mut rows = c
            .query(topic_select::QUERY, (self.cluster.as_str(), name))
            .await?;

        if rows.next().await?.is_some() {
            let mut rows = c
                .query_rows(
                    topic_configuration_select::QUERY,
                    (self.cluster.as_str(), name),
                )
                .await?;

            let mut configs = vec![];

            while let Some(topic_configuration_select::Row { name, value }) = rows.next().await? {
                configs.push(
                    DescribeConfigsResourceResult::default()
                        .name(name)
                        .value(Some(value.unwrap_or_default()))
                        .read_only(false)
                        .is_default(Some(false))
                        .config_source(Some(ConfigSource::DynamicTopicConfig.into()))
                        .is_sensitive(false)
                        .synonyms(Some([].into()))
                        .config_type(Some(ConfigType::String.into()))
                        .documentation(Some("".into())),
                );
            }

            let error_code = ErrorCode::None;

            Ok(DescribeConfigsResult::default()
                .error_code(error_code.into())
                .error_message(Some(error_code.to_string()))
                .resource_type(i8::from(resource))
                .resource_name(name.into())
                .configs(Some(configs)))
            .inspect(|_| {
                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::DescribeConfig.attribute()],
                )
            })
        } else {
            let error_code = ErrorCode::UnknownTopicOrPartition;

            Ok(DescribeConfigsResult::default()
                .error_code(error_code.into())
                .error_message(Some(error_code.to_string()))
                .resource_type(i8::from(resource))
                .resource_name(name.into())
                .configs(Some([].into())))
            .inspect(|_| {
                DELEGATE_REQUEST_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::DescribeConfig.attribute()],
                )
            })
        }
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let start = SystemTime::now();

        debug!(?topics, partition_limit, ?cursor);

        let c = self.connection().await?;

        let mut responses =
            Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or_default());

        for topic in topics.unwrap_or_default() {
            responses.push(match topic {
                TopicId::Name(name) => {
                    let selected = c
                        .query_first(
                            topic_select_name::QUERY,
                            (self.cluster.as_str(), name.as_str()),
                        )
                        .await;

                    match selected {
                        Ok(Some(topic_select_name::Row {
                            uuid,
                            name,
                            is_internal,
                            partitions,
                            replication_factor,
                        })) => {
                            let topic_id = uuid.into_bytes();
                            let name = Some(name);
                            let is_internal = Some(is_internal);

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
                            error!(?reason, ?topic);
                            DescribeTopicPartitionsResponseTopic::default()
                                .error_code(sql::TOPIC_READ_ERROR_CODE.into())
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
                    let selected = c
                        .query_first(
                            topic_select_uuid::QUERY,
                            (self.cluster.as_str(), id.to_string().as_str()),
                        )
                        .await;

                    match selected {
                        Ok(Some(topic_select_uuid::Row {
                            uuid,
                            name,
                            is_internal,
                            partitions,
                            replication_factor,
                        })) => {
                            let topic_id = uuid.into_bytes();
                            let name = Some(name);
                            let is_internal = Some(is_internal);

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
                            error!(?reason, ?topic);
                            DescribeTopicPartitionsResponseTopic::default()
                                .error_code(sql::TOPIC_READ_ERROR_CODE.into())
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

        Ok(responses).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::DescribeTopicPartitions.attribute()],
            )
        })
    }

    async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
        let start = SystemTime::now();

        let c = self.connection().await?;

        let mut listed_groups = vec![];

        let mut rows = c
            .query_rows(consumer_group_select::QUERY, &[self.cluster.as_str()])
            .await?;

        while let Some(consumer_group_select::Row { name, detail }) = rows.next().await? {
            let stored =
                detail.map(|detail| detail.and_then(serde_json::from_value::<GroupDetail>));

            listed_groups.push(NamedGroupDetail::decoded(name, stored));
        }

        Ok(listed_groups).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::ListGroups.attribute()],
            )
        })
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let start = SystemTime::now();

        debug!(?group_ids);

        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            let c = self.connection().await?;

            for group_id in group_ids {
                _ = c
                    .execute(
                        sql::CONSUMER_OFFSET_DELETE_BY_CG,
                        (self.cluster.as_str(), group_id.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;

                _ = c
                    .execute(
                        sql::CONSUMER_GROUP_DETAIL_DELETE_BY_CG,
                        (self.cluster.as_str(), group_id.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;

                let rows = c
                    .execute(
                        sql::CONSUMER_GROUP_DELETE,
                        (self.cluster.as_str(), group_id.as_str()),
                    )
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

        Ok(results).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::DeleteGroups.attribute()],
            )
        })
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let start = SystemTime::now();

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

                match c
                    .query_first(
                        sql::consumer_group_select_by_name::QUERY,
                        (self.cluster.as_str(), group_id.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err, group_id))
                {
                    Ok(Some(row)) => {
                        // A group row with no detail row (e.g. a group that only ever committed
                        // offsets, never a JoinGroup) joins to a NULL `detail` column here: that
                        // group exists and is empty, not an error.
                        let stored = row.detail.map(|detail| {
                            detail
                                .and_then(serde_json::from_value::<GroupDetail>)
                                .map_err(Error::from)
                                .inspect(|current| debug!(?current))
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

        Ok(results).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::DescribeGroups.attribute()],
            )
        })
    }

    async fn group_with_version(&self, group_id: &str) -> Result<Option<(GroupDetail, Version)>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, group_id);

        let c = self.connection().await?;

        c.query_first(
            sql::consumer_group_detail::QUERY,
            (group_id, self.cluster.as_str()),
        )
        .await
        .inspect_err(|err| error!(?err, group_id))?
        .map(group_detail_with_version)
        .transpose()
        .inspect_err(|err| error!(?err, group_id, "the stored group can't be decoded"))
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::GroupWithVersion.attribute()],
            )
        })
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, group_id, ?detail, ?version);

        let pc = self.connection().await?;
        let tx = pc.transaction().await?;

        _ = pc
            .execute(
                sql::CONSUMER_GROUP_INSERT,
                (self.cluster.as_str(), group_id),
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

        let detail = serde_json::to_value(detail).inspect(|detail| debug!(%detail))?;

        let row = pc
            .query_first(
                sql::consumer_group_detail_insert::QUERY,
                (
                    self.cluster.as_str(),
                    group_id,
                    existing_e_tag.to_string().as_str(),
                    new_e_tag.to_string().as_str(),
                    detail.to_string().as_str(),
                ),
            )
            .await
            .inspect_err(|err| error!(?err))?;

        let outcome = if let Some(row) = row {
            let version = Version {
                e_tag: Some(row.e_tag.to_string()),
                version: None,
            };
            debug!(?version);
            Ok(version)
        } else {
            let (current, version) = pc
                .query_one(
                    sql::consumer_group_detail::QUERY,
                    (group_id, self.cluster.as_str()),
                )
                .await
                .inspect_err(|err| error!(?err))
                .and_then(group_detail_with_version)?;

            Err(UpdateError::Outdated {
                current: Box::new(current),
                version,
            })
        };

        pc.commit(tx).await?;

        debug!(?outcome);

        outcome.inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::UpdateGroup.attribute()],
            )
        })
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        let start = SystemTime::now();

        debug!(
            cluster = self.cluster,
            transaction_id, transaction_timeout_ms, producer_id, producer_epoch
        );

        let claim = match producer_claim(producer_id, producer_epoch) {
            Ok(claim) => claim,
            Err(error) => {
                return Ok(ProducerIdResponse::failed(error)).inspect(|_| {
                    DELEGATE_REQUEST_DURATION.record(
                        elapsed_millis(start),
                        &[StorageOperation::InitProducer.attribute()],
                    )
                });
            }
        };

        match transaction_id {
            Some(transaction_id) => {
                let pc = self.connection().await?;
                let tx = pc.transaction().await?;

                let mut rolled_back = false;

                if let Some(producer_epoch_for_current_txn::Row {
                    producer: id,
                    epoch,
                    status,
                }) = pc
                    .producer_epoch_for_current_txn(self.cluster.as_str(), transaction_id)
                    .await
                    .inspect_err(|err| error!(?err, transaction_id))?
                {
                    let status = status.map_or(Ok(None), |status| {
                        TxnState::from_str(status.as_str()).map(Some)
                    })?;

                    debug!(transaction_id, id, epoch, ?status, ?claim);

                    if let Some(claim) = claim {
                        let error = check_claim((id, epoch), claim);

                        if error != ErrorCode::None {
                            _ = tx
                                .rollback()
                                .await
                                .inspect_err(|err| error!(?err, ?transaction_id, id, epoch));

                            return Ok(ProducerIdResponse::failed(error)).inspect(|_| {
                                DELEGATE_REQUEST_DURATION.record(
                                    elapsed_millis(start),
                                    &[StorageOperation::InitProducer.attribute()],
                                )
                            });
                        }
                    }

                    if let Some(TxnState::Begin) = status {
                        let error = self
                            .end_in_tx(transaction_id, id, epoch, false, &pc)
                            .await?;

                        if error != ErrorCode::None {
                            _ = tx
                                .rollback()
                                .await
                                .inspect_err(|err| error!(?err, ?transaction_id, id, epoch));

                            return Ok(ProducerIdResponse { error, id, epoch }).inspect(|_| {
                                DELEGATE_REQUEST_DURATION.record(
                                    elapsed_millis(start),
                                    &[StorageOperation::InitProducer.attribute()],
                                )
                            });
                        }

                        rolled_back = true;
                    }
                }

                let txn_producer = pc
                    .query_first(
                        txn_select_name::QUERY,
                        (self.cluster.as_str(), transaction_id),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?
                    .map(|row| row.producer_id);

                let (producer, epoch) = if let Some(producer) = txn_producer {
                    debug!(producer);

                    // A bump gives the producer its newest epoch plus one, so the limit
                    // applies to that epoch, not to the newest epoch with a transaction.
                    let newest = pc
                        .producer_epoch_current(self.cluster.as_str(), producer)
                        .await
                        .inspect_err(|err| error!(?err, transaction_id, producer))?;

                    if newest.is_some_and(epoch_is_exhausted) {
                        // The status after the rollback above, if it ran.
                        let status = pc
                            .producer_epoch_for_current_txn(self.cluster.as_str(), transaction_id)
                            .await
                            .inspect_err(|err| error!(?err))?
                            .and_then(|row| row.status)
                            .map(|status| TxnState::from_str(status.as_str()))
                            .transpose()?;

                        // As in Kafka, only the transaction of the current epoch holds back
                        // the move: the client retries CONCURRENT_TRANSACTIONS until it
                        // finishes. This commits the rollback above, if it ran.
                        if status.is_some_and(|status| !status.is_finished()) {
                            // The rollback leaves its abort prepared behind an older open
                            // transaction. Kafka fences the producer at that point, so that
                            // it cannot add to the aborted transaction, and so does this. A
                            // producer at epoch i16::MAX gets no new epoch, and is fenced
                            // already.
                            if rolled_back {
                                _ = pc
                                    .query_opt(
                                        producer_epoch_insert::QUERY.key(),
                                        (self.cluster.as_str(), producer),
                                    )
                                    .await
                                    .inspect_err(|err| error!(self.cluster, producer, ?err))?;
                            }

                            let error = match pc.commit(tx).await.inspect_err(|err| {
                                error!(?err, cluster = self.cluster, transaction_id, producer)
                            }) {
                                Ok(()) => ErrorCode::ConcurrentTransactions,
                                Err(err) => return Err(err),
                            };

                            return Ok(ProducerIdResponse::failed(error)).inspect(|_| {
                                DELEGATE_REQUEST_DURATION.record(
                                    elapsed_millis(start),
                                    &[StorageOperation::InitProducer.attribute()],
                                )
                            });
                        }

                        let previous = producer;

                        let producer = pc
                            .producer_insert(self.cluster.as_str())
                            .await
                            .inspect_err(|err| error!(?err))?;

                        let epoch = pc
                            .producer_epoch_insert(self.cluster.as_str(), producer)
                            .await
                            .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                        // A produce with an epoch below the producer's newest is fenced, so
                        // epoch i16::MAX fences the previous producer ID. A producer at epoch
                        // i16::MAX gets no new epoch, and is fenced already.
                        _ = pc
                            .query_opt(
                                producer_epoch_insert::QUERY.key(),
                                (self.cluster.as_str(), previous),
                            )
                            .await
                            .inspect_err(|err| error!(self.cluster, previous, ?err))?;

                        let updated = pc
                            .execute(
                                sql::TXN_UPDATE_PRODUCER,
                                (self.cluster.as_str(), transaction_id, producer),
                            )
                            .await
                            .inspect_err(|err| error!(self.cluster, transaction_id, ?err))?;

                        if updated != 1 {
                            return Err(Error::Message(format!(
                                "{updated} transactions named {transaction_id}"
                            )));
                        }

                        info!(
                            cluster = self.cluster,
                            transaction_id,
                            previous,
                            producer,
                            "producer epoch exhausted, moved to a new producer ID"
                        );

                        (producer, epoch)
                    } else {
                        let epoch = pc
                            .producer_epoch_insert(self.cluster.as_str(), producer)
                            .await
                            .inspect(|epoch| debug!(epoch))
                            .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                        (producer, epoch)
                    }
                } else {
                    let producer = pc
                        .producer_insert(self.cluster.as_str())
                        .await
                        .inspect_err(|err| error!(?err))?;

                    let epoch = pc
                        .producer_epoch_insert(self.cluster.as_str(), producer)
                        .await
                        .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                    assert_eq!(
                        1,
                        pc.execute(
                            sql::TXN_INSERT,
                            (self.cluster.as_str(), transaction_id, producer),
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
                    pc.execute(
                        sql::TXN_DETAIL_INSERT,
                        (
                            self.cluster.as_str(),
                            transaction_id,
                            producer,
                            epoch,
                            transaction_timeout_ms
                        ),
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

                let error = match pc.commit(tx).await.inspect_err(|err| {
                    error!(
                        ?err,
                        cluster = self.cluster,
                        transaction_id,
                        producer,
                        epoch
                    )
                }) {
                    Ok(()) => ErrorCode::None,
                    Err(err) => return Err(err),
                };

                Ok(ProducerIdResponse {
                    error,
                    id: producer,
                    epoch,
                })
            }

            None => {
                let pc = self.connection().await?;
                let tx = pc.transaction().await?;

                let mut rows = pc
                    .query_rows(producer_insert::QUERY, &[self.cluster.as_str()])
                    .await?;

                if let Some(producer_insert::Row { id: producer }) = rows.next().await? {
                    debug!(producer);

                    while let Some(row) = rows.next().await? {
                        debug!(?row)
                    }

                    let mut rows = pc
                        .query_rows(
                            producer_epoch_insert::QUERY,
                            (self.cluster.as_str(), producer),
                        )
                        .await?;

                    if let Some(producer_epoch_insert::Row { epoch }) = rows.next().await? {
                        debug!(epoch);

                        while let Some(row) = rows.next().await? {
                            debug!(?row)
                        }

                        let error = match pc
                            .commit(tx)
                            .await
                            .inspect_err(|err| error!(?err, ?transaction_id, producer, epoch))
                        {
                            Ok(()) => ErrorCode::None,
                            Err(err) => return Err(err),
                        };

                        Ok(ProducerIdResponse {
                            error,
                            id: producer,
                            epoch,
                        })
                        .inspect(|response| debug!(?response))
                        .inspect(|_| {
                            DELEGATE_REQUEST_DURATION.record(
                                elapsed_millis(start),
                                &[StorageOperation::InitProducer.attribute()],
                            )
                        })
                    } else {
                        Ok(ProducerIdResponse {
                            error: ErrorCode::UnknownServerError,
                            id: producer,
                            epoch: -1,
                        })
                        .inspect(|response| debug!(?response))
                        .inspect(|_| {
                            DELEGATE_REQUEST_DURATION.record(
                                elapsed_millis(start),
                                &[StorageOperation::InitProducer.attribute()],
                            )
                        })
                    }
                } else {
                    Ok(ProducerIdResponse {
                        error: ErrorCode::UnknownServerError,
                        id: -1,
                        epoch: -1,
                    })
                    .inspect(|response| debug!(?response))
                    .inspect(|_| {
                        DELEGATE_REQUEST_DURATION.record(
                            elapsed_millis(start),
                            &[StorageOperation::InitProducer.attribute()],
                        )
                    })
                }
            }
        }
    }

    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        let start = SystemTime::now();

        debug!(
            cluster = self.cluster,
            transaction_id, producer_id, producer_epoch, group_id
        );

        Ok(ErrorCode::None).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::TxnAddOffsets.attribute()],
            )
        })
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?partitions);

        match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id,
                producer_id,
                producer_epoch,
                topics,
            } => {
                debug!(?transaction_id, ?producer_id, ?producer_epoch, ?topics);

                let pc = self.connection().await?;
                let tx = pc.transaction().await?;

                let mut results = vec![];

                for topic in topics {
                    let mut results_by_partition = vec![];

                    for partition_index in topic.partitions.unwrap_or(vec![]) {
                        _ = pc
                            .execute(
                                sql::TXN_TOPITION_INSERT,
                                (
                                    self.cluster.as_str(),
                                    topic.name.as_str(),
                                    partition_index,
                                    transaction_id.as_str(),
                                    producer_id,
                                    producer_epoch,
                                ),
                            )
                            .await
                            .inspect_err(|err| {
                                error!(
                                    ?err,
                                    cluster = self.cluster,
                                    topic = topic.name,
                                    partition_index,
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

                _ = pc
                    .execute(
                        sql::TXN_DETAIL_UPDATE_STARTED_AT,
                        (
                            self.cluster.as_str(),
                            transaction_id.as_str(),
                            producer_id,
                            producer_epoch,
                        ),
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

                pc.commit(tx).await?;

                Ok(TxnAddPartitionsResponse::VersionZeroToThree(results)).inspect(|_| {
                    DELEGATE_REQUEST_DURATION.record(
                        elapsed_millis(start),
                        &[StorageOperation::TxnAddPartitions.attribute()],
                    )
                })
            }

            TxnAddPartitionsRequest::VersionFourPlus { .. } => {
                Err(Error::UnsupportedTxnAddPartitionsVersion)
            }
        }
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let start = SystemTime::now();

        debug!(cluster = self.cluster, ?offsets);

        let pc = self.connection().await?;
        let tx = pc.transaction().await?;

        let (producer_id, producer_epoch) = if let Some(row) = pc
            .producer_epoch_for_current_txn(self.cluster.as_str(), offsets.transaction_id.as_str())
            .await
            .inspect_err(|err| error!(?err))?
        {
            (Some(row.producer), Some(row.epoch))
        } else {
            (None, None)
        };

        _ = pc
            .execute(
                sql::CONSUMER_GROUP_INSERT,
                (self.cluster.as_str(), offsets.group_id.as_str()),
            )
            .await?;

        debug!(?producer_id, ?producer_epoch);

        _ = pc
            .execute(
                sql::TXN_OFFSET_COMMIT_INSERT,
                (
                    self.cluster.as_str(),
                    offsets.transaction_id.as_str(),
                    offsets.group_id.as_str(),
                    offsets.producer_id,
                    offsets.producer_epoch,
                    offsets.generation_id,
                    offsets.member_id,
                ),
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
                        _ = pc
                            .execute(
                                sql::TXN_OFFSET_COMMIT_TP_INSERT,
                                (
                                    self.cluster.as_str(),
                                    offsets.transaction_id.as_str(),
                                    offsets.group_id.as_str(),
                                    offsets.producer_id,
                                    offsets.producer_epoch,
                                    topic.name.as_str(),
                                    partition.partition_index,
                                    partition.committed_offset,
                                    partition.committed_leader_epoch,
                                    partition.committed_metadata,
                                ),
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

        pc.commit(tx).await?;

        Ok(topics).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::TxnOffsetCommit.attribute()],
            )
        })
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let start = SystemTime::now();

        debug!(cluster = ?self.cluster, transaction_id, producer_id, producer_epoch, committed);

        let pc = self.connection().await?;
        let tx = pc.transaction().await?;

        let error_code = self
            .end_in_tx(transaction_id, producer_id, producer_epoch, committed, &pc)
            .await?;

        pc.commit(tx).await.and(Ok(error_code)).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::TxnEnd.attribute()],
            )
        })
    }

    async fn maintain_transactions(&self, _now: SystemTime) -> Result<()> {
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

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        self.vacuum_into().await?;

        let Ok(_permit) = self.maintenance.try_acquire() else {
            return Ok(());
        };

        let start = SystemTime::now();

        let deleted = self.policy_delete(now).await?;
        debug!(deleted);

        let compacted = self.policy_compact().await?;
        debug!(compacted);

        {
            let connection = self.pool.get().await?;

            let mut rows = connection
                .query_rows(sql::maintain_vacuum::QUERY, ())
                .await?;

            if let Some(sql::maintain_vacuum::Row {
                freelist_count,
                page_size,
            }) = rows.next().await.inspect_err(|err| error!(?err))?
            {
                debug!(freelist_count, page_size);
            }
        }

        Ok(()).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::Maintain.attribute()],
            )
        })
    }

    async fn cluster_id(&self) -> Result<String> {
        let start = SystemTime::now();

        Ok(self.cluster.clone()).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::ClusterId.attribute()],
            )
        })
    }

    async fn node(&self) -> Result<i32> {
        let start = SystemTime::now();

        Ok(self.node).inspect(|_| {
            DELEGATE_REQUEST_DURATION
                .record(elapsed_millis(start), &[StorageOperation::Node.attribute()])
        })
    }

    async fn advertised_listener(&self) -> Result<Url> {
        let start = SystemTime::now();

        Ok(self.advertised_listener.clone()).inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::AdvertisedListener.attribute()],
            )
        })
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        let start = SystemTime::now();
        let pc = self.connection().await?;

        pc.execute(
            sql::SCRAM_CREDENTIAL_DELETE,
            (self.cluster.as_str(), user, i32::from(mechanism)),
        )
        .await
        .inspect_err(|err| error!(?err))
        .map_err(Into::into)
        .and(Ok(()))
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::DeleteUserScramCredential.attribute()],
            )
        })
    }

    #[instrument(skip_all)]
    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let start = SystemTime::now();
        let pc = self.connection().await?;

        pc.execute(
            sql::SCRAM_CREDENTIAL_INSERT,
            (
                self.cluster.as_str(),
                user,
                i32::from(mechanism),
                &credential.salt[..],
                credential.iterations,
                &credential.stored_key[..],
                &credential.server_key[..],
            ),
        )
        .await
        .inspect_err(|err| error!(?err))
        .map_err(Into::into)
        .and(Ok(()))
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::UpsertUserScramCredential.attribute()],
            )
        })
    }

    #[instrument(skip_all)]
    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        let start = SystemTime::now();
        let pc = self.connection().await?;

        let row = pc
            .query_first(
                sql::scram_credential_select::QUERY,
                (self.cluster.as_str(), user, i32::from(mechanism)),
            )
            .await
            .inspect_err(|err| error!(?err))?;

        Ok(row.map(
            |sql::scram_credential_select::Row {
                 salt,
                 iterations,
                 stored_key,
                 server_key,
             }| ScramCredential {
                salt,
                iterations,
                stored_key,
                server_key,
            },
        ))
        .inspect(|_| {
            DELEGATE_REQUEST_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::UpsertUserScramCredential.attribute()],
            )
        })
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        let start = SystemTime::now();
        let c = self.pool.get().await?;
        let _ = c.query(sql::PING, ()).await?;
        DELEGATE_REQUEST_DURATION
            .record(elapsed_millis(start), &[StorageOperation::Ping.attribute()]);
        Ok(())
    }
}

#[cfg(test)]
mod options_tests;

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::thread;

    use nisshi_sans_io::{Compression, create_topics_request::CreatableTopicConfig};
    use nisshi_storage::{ArcDynStorage, StorageContainer, StorageFactory as _};
    use tempfile::{tempdir, tempdir_in};
    use tokio::{fs::remove_file, task::yield_now, time::Instant};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use super::*;
    use nisshi_storage::fetched::FetchUntilDeadline as _;

    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(EnvFilter::from_default_env().add_directive(
                    format!("{}=debug", env!("CARGO_PKG_NAME").replace("-", "_")).parse()?,
                ))
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

    #[tokio::test]
    async fn insert_with_returning() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");
        let db = libsql::Builder::new_local(file_path).build().await?;
        let connection = db.connect()?;

        let sql = "create table xyz (
            id integer primary key autoincrement,
            name text not null
            )";

        _ = connection.execute(sql, ()).await?;

        let sql = "insert into xyz (name) values (?1) returning xyz.name";

        let statement = connection.prepare(sql).await?;
        let mut rows = statement.query(&["abc"]).await?;
        let row = rows.next().await.inspect(|row| debug!(?row))?.unwrap();
        let name = row.get_str(0).inspect(|name| debug!(name))?;
        assert_eq!("abc", name);

        Ok(())
    }

    #[tokio::test]
    async fn insert_select_with_returning() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");
        let db = libsql::Builder::new_local(file_path).build().await?;
        let connection = db.connect()?;

        let sql = "create table pqr (
            id integer primary key autoincrement,
            name text not null
            )";

        _ = connection.execute(sql, ()).await?;

        let sql = "insert into pqr (name) values (?1)";
        let statement = connection.prepare(sql).await?;
        assert_eq!(1, statement.execute(&["fgh"]).await?);

        let sql = "create table xyz (
            id integer primary key autoincrement,
            pqr integer references pqr (id) not null,
            name text not null
            )";

        _ = connection.execute(sql, ()).await?;

        let sql = "insert into xyz (pqr, name)
            select pqr.id, ?2
            from pqr
            where pqr.name = ?1
            returning xyz.name";

        let expected = "abc";
        let mut rows = connection.query(sql, &["fgh", expected]).await?;
        let row = rows.next().await.inspect(|row| debug!(?row))?.unwrap();
        let actual = row.get_str(0).inspect(|name| debug!(name))?;
        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn cte_insert_with_returning() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");
        let db = libsql::Builder::new_local(file_path).build().await?;
        let connection = db.connect()?;

        let sql = "create table pqr (
            id integer primary key autoincrement,
            name text not null
            )";

        _ = connection.execute(sql, ()).await?;

        let sql = "insert into pqr (name) values (?1)";
        let statement = connection.prepare(sql).await?;
        assert_eq!(1, statement.execute(&["fgh"]).await?);

        let sql = "create table xyz (
            id integer primary key autoincrement,
            pqr integer references pqr (id) not null,
            name text not null
            )";

        _ = connection.execute(sql, ()).await?;

        let sql = "with qwe as (
                select pqr.id, ?2
                from pqr
                where pqr.name = ?1
            )
            insert into xyz (pqr, name)
            select * from qwe
            returning xyz.name";

        let expected = "abc";
        let mut rows = connection.query(sql, &["fgh", expected]).await?;
        let row = rows.next().await.inspect(|row| debug!(?row))?.unwrap();
        let actual = row.get_str(0).inspect(|name| debug!(name))?;
        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn simpler_insert_select_with_returning() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");
        let db = libsql::Builder::new_local(file_path).build().await?;
        let connection = db.connect()?;

        let sql = "create table xyz (
            id integer primary key autoincrement,
            pqr integer not null,
            name text not null
            )";

        _ = connection.execute(sql, ()).await?;

        let expected = "abc";

        let sql = "insert into xyz (pqr, name)
            select 41 + 1, ?1
            returning xyz.pqr, xyz.name";

        let mut rows = connection.query(sql, &[expected]).await?;
        let row = rows.next().await.inspect(|row| debug!(?row))?.unwrap();

        assert_eq!(42, row.get::<i32>(0)?);

        let actual = row.get_str(1).inspect(|name| debug!(name))?;
        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn create_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");
        let db = libsql::Builder::new_local(file_path).build().await?;
        let connection = db.connect()?;

        assert_eq!(
            0,
            connection
                .execute(&include_sql!("ddl/010-cluster.sql"), ())
                .await?
        );

        assert_eq!(
            0,
            connection
                .execute(&include_sql!("ddl/020-topic.sql"), ())
                .await?
        );

        let cluster = "nisshi";

        assert_eq!(
            1,
            connection
                .execute(
                    &fix_parameters(&include_sql!("sql/register_broker.sql"))?,
                    &[cluster]
                )
                .await?
        );

        let name = "test";
        let uuid = Uuid::new_v4();
        let partitions = 3;
        let replication_factor = 3;

        let mut rows = connection
            .query(
                &fix_parameters(&include_sql!("sql/topic_insert.sql"))?,
                (
                    cluster,
                    name,
                    uuid.to_string(),
                    partitions,
                    replication_factor,
                ),
            )
            .await?;

        let row = rows.next().await.inspect(|row| debug!(?row))?.unwrap();
        assert_eq!(uuid.to_string().as_str(), row.get_str(0)?);
        Ok(())
    }

    #[tokio::test]
    async fn create_topic_in_tx() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let db = libsql::Builder::new_local(file_path).build().await?;
        let tx = db.connect()?.transaction().await?;

        assert_eq!(
            0,
            tx.execute(&include_sql!("ddl/010-cluster.sql"), ()).await?
        );

        assert_eq!(0, tx.execute(&include_sql!("ddl/020-topic.sql"), ()).await?);

        let cluster = "nisshi";

        assert_eq!(
            1,
            tx.execute(
                &fix_parameters(&include_sql!("sql/register_broker.sql"))?,
                &[cluster]
            )
            .await?
        );

        let name = "test";
        let uuid = Uuid::new_v4();
        let partitions = 3;
        let replication_factor = 3;

        let mut rows = tx
            .query(
                &fix_parameters(&include_sql!("sql/topic_insert.sql"))?,
                (
                    cluster,
                    name,
                    uuid.to_string(),
                    partitions,
                    replication_factor,
                ),
            )
            .await?;

        let row = rows.next().await.inspect(|row| debug!(?row))?.unwrap();
        assert_eq!(uuid.to_string().as_str(), row.get_str(0)?);
        Ok(())
    }

    #[tokio::test]
    async fn lite_system_time() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let db = libsql::Builder::new_local(file_path).build().await?;
        let connection = db.connect()?;

        assert_eq!(
            0,
            connection
                .execute(&include_sql!("ddl/010-cluster.sql"), ())
                .await?
        );

        let name = "lite";

        _ = connection
            .execute(&include_sql!("sql/register_broker.sql"), &[name])
            .await?;

        let mut rows = connection
            .query("select last_updated from cluster where name = ?1", &[name])
            .await?;
        let row = rows.next().await?.unwrap();
        let last_updated = row.get::<String>(0)?;
        _ = chrono::NaiveDateTime::parse_from_str(&last_updated, "%Y-%m-%d %H:%M:%S%.f")?;

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn register_broker() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let storage = Url::parse(&format!("file://{}", file_path.display()))?;
        let cluster = "nisshi";
        let node = 12321;

        {
            let engine = Engine::builder()
                .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
                .cluster(cluster.to_owned())
                .storage(storage)
                .node(node)
                .build()
                .await?;

            engine
                .register_broker(BrokerRegistrationRequest {
                    broker_id: node,
                    cluster_id: cluster.to_owned(),
                    incarnation_id: Uuid::new_v4(),
                    rack: None,
                })
                .await?;
        }

        let db = libsql::Builder::new_local(file_path).build().await?;
        let connection = db.connect()?;

        let mut rows = connection
            .query("select id, name from cluster where name = ?1", &[cluster])
            .await?;

        let row = rows.next().await?.unwrap();
        assert_eq!(cluster, row.get_str(1)?);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn storage_create_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let storage = Url::parse(&format!("file://{}", file_path.display()))?;
        let cluster = "nisshi";
        let node = 12321;

        let topic = "test";
        let num_partitions = 5;
        let replication_factor = 3;

        let uuid = {
            let engine = Engine::builder()
                .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
                .cluster(cluster.to_owned())
                .storage(storage)
                .node(node)
                .build()
                .await?;

            engine
                .register_broker(BrokerRegistrationRequest {
                    broker_id: node,
                    cluster_id: cluster.to_owned(),
                    incarnation_id: Uuid::new_v4(),
                    rack: None,
                })
                .await?;

            let creatable_topic = CreatableTopic::default()
                .name(topic.to_owned())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(Some([].into()))
                .configs(Some([].into()));

            engine
                .create_topic(creatable_topic, false)
                .await
                .inspect(|uuid| debug!(?uuid))
        }?;

        let db = libsql::Builder::new_local(file_path).build().await?;
        let connection = db.connect()?;

        let mut rows = connection
            .query(
                "select t.uuid, t.partitions, t.replication_factor from
                cluster c
                join topic t on t.cluster = c.id
                where c.name = ?1
                and t.name = ?2",
                &[cluster, topic],
            )
            .await?;

        let row = rows.next().await?.unwrap();
        assert_eq!(uuid, Uuid::parse_str(row.get_str(0)?)?);
        assert_eq!(num_partitions, row.get::<i32>(1)?);
        assert_eq!(replication_factor, row.get::<i32>(2)? as i16);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn produce() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let storage = Url::parse(&format!("file://{}", file_path.display()))?;
        let cluster = "nisshi";
        let node = 12321;

        let topic = "test";
        let num_partitions = 5;
        let replication_factor = 3;

        let engine = Engine::builder()
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
            .cluster(cluster.to_owned())
            .storage(storage)
            .node(node)
            .build()
            .await?;

        engine
            .register_broker(BrokerRegistrationRequest {
                broker_id: node,
                cluster_id: cluster.to_owned(),
                incarnation_id: Uuid::new_v4(),
                rack: None,
            })
            .await?;

        let creatable_topic = CreatableTopic::default()
            .name(topic.to_owned())
            .num_partitions(num_partitions)
            .replication_factor(replication_factor)
            .assignments(Some([].into()))
            .configs(Some([].into()));

        let _uuid = engine
            .create_topic(creatable_topic, false)
            .await
            .inspect(|uuid| debug!(?uuid))?;

        Ok(())
    }

    /// Smoke test for the `?mode=mpsc` wiring on the ordinary (non-panicking,
    /// non-cancelled) path. It does not cover the panic isolation in
    /// `ChannelRequestService::serve` (`nisshi-storage/tests/channel.rs` does), and it
    /// does not prove the mpsc engine was selected: an unrecognised mode falls back to
    /// Semaphore, and this test would pass there too.
    #[tokio::test]
    async fn mpsc_mode_produce_and_fetch_round_trip() -> Result<()> {
        let _guard = init_tracing()?;

        // lite's build() resolves the URL path relative to the crate dir (it strips
        // the leading "/" and pushes onto cwd), so use a crate-local db file rather
        // than a tempdir (see `read_committed_last_stable_offset_pinned_by_open_transaction`
        // below for the same note). The guard removes the file (and its WAL/SHM
        // sidecars) on drop, including on panic.
        struct DbFileGuard(&'static str);
        impl Drop for DbFileGuard {
            fn drop(&mut self) {
                for suffix in ["", "-wal", "-shm"] {
                    let _ = std::fs::remove_file(format!("{}{suffix}", self.0));
                }
            }
        }

        let db_file = "mpsc-mode-produce-and-fetch-round-trip-test.db";
        let _db_file_guard = DbFileGuard(db_file);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{db_file}{suffix}"));
        }

        let storage = Url::parse(&format!("file:///{db_file}?mode=mpsc"))?;
        let cluster = "nisshi";
        let node = 12321;

        let topic = "test";
        let num_partitions = 1;
        let replication_factor = 1;

        let engine = Engine::builder()
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
            .cluster(cluster.to_owned())
            .storage(storage)
            .node(node)
            .build()
            .await?;

        engine
            .register_broker(BrokerRegistrationRequest {
                broker_id: node,
                cluster_id: cluster.to_owned(),
                incarnation_id: Uuid::new_v4(),
                rack: None,
            })
            .await?;

        let creatable_topic = CreatableTopic::default()
            .name(topic.to_owned())
            .num_partitions(num_partitions)
            .replication_factor(replication_factor)
            .assignments(Some([].into()))
            .configs(Some([].into()));

        _ = engine
            .create_topic(creatable_topic, false)
            .await
            .inspect(|uuid| debug!(?uuid))?;

        let topition = Topition::new(topic, 0);

        let batch: deflated::Batch = inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"sol-155270").into()))
            .build()
            .and_then(TryInto::try_into)?;

        let offset = engine
            .produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(offset))?;
        assert_eq!(0, offset);

        let fetched = engine
            .fetch_until_deadline(
                &topition,
                0,
                0,
                1_000_000,
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .map(|read| read.batches)
            .inspect(|fetched| debug!(?fetched))?;

        assert_eq!(1, fetched.len());

        Ok(())
    }

    /// After its deadline, a fetch returns no batches, and the same fetch before its deadline
    /// returns the record.
    #[tokio::test]
    async fn fetch_after_its_deadline_returns_no_batches() -> Result<()> {
        let _guard = init_tracing()?;

        // The builder resolves the storage path against the current
        // directory, so the database lives in a temporary directory there.
        let current_dir = env::current_dir()?;
        let temp_dir = tempdir_in(&current_dir).inspect(|temporary| debug!(?temporary))?;
        let relative = temp_dir
            .path()
            .strip_prefix(&current_dir)
            .map_err(|err| Error::Message(err.to_string()))?
            .join("nisshi.db");

        let storage = Url::parse(&format!("file:///{}?mode=direct", relative.display()))?;
        let cluster = "nisshi";
        let node = 12321;
        let topic = "test";

        let engine = Engine::builder()
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
            .cluster(cluster.to_owned())
            .storage(storage)
            .node(node)
            .build()
            .await?;

        engine
            .register_broker(BrokerRegistrationRequest {
                broker_id: node,
                cluster_id: cluster.to_owned(),
                incarnation_id: Uuid::new_v4(),
                rack: None,
            })
            .await?;

        _ = engine
            .create_topic(
                CreatableTopic::default()
                    .name(topic.to_owned())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic, 0);

        let batch: deflated::Batch = inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"value").into()))
            .build()
            .and_then(TryInto::try_into)?;

        _ = engine.produce(None, &topition, batch).await?;

        let fetch = |deadline| {
            engine.fetch_until_deadline(
                &topition,
                0,
                0,
                1_000_000,
                IsolationLevel::ReadUncommitted,
                deadline,
            )
        };

        assert!(fetch(Instant::now()).await?.batches.is_empty());

        // The record is there, so the empty fetch above comes from its deadline.
        assert_eq!(
            1,
            fetch(Instant::now() + Duration::from_secs(1))
                .await?
                .batches
                .len()
        );

        Ok(())
    }

    /// Returns the number of records in a fetch of `topition` from offset 0 that stops at
    /// `deadline`.
    async fn fetched_records(
        storage: &dyn Storage,
        topition: &Topition,
        deadline: Instant,
    ) -> Result<u32> {
        storage
            .fetch_until_deadline(
                topition,
                0,
                0,
                u32::MAX,
                IsolationLevel::ReadUncommitted,
                deadline,
            )
            .await
            .map(|read| read.batches)
            .map(|fetched| fetched.iter().map(|batch| batch.record_count).sum())
    }

    /// A fetch stops at its deadline partway through a partition, and returns the records that it
    /// read by then.
    #[tokio::test]
    async fn fetch_stops_at_its_deadline_partway_through_a_partition() -> Result<()> {
        // A read of every record runs a header query for each one, so a full read takes long
        // enough for a deadline to stop it partway.
        const RECORDS: u32 = 5_000;

        let _guard = init_tracing()?;

        // The builder resolves the storage path against the current
        // directory, so the database lives in a temporary directory there.
        let current_dir = env::current_dir()?;
        let temp_dir = tempdir_in(&current_dir).inspect(|temporary| debug!(?temporary))?;
        let relative = temp_dir
            .path()
            .strip_prefix(&current_dir)
            .map_err(|err| Error::Message(err.to_string()))?
            .join("nisshi.db");

        let storage = Url::parse(&format!("file:///{}?mode=direct", relative.display()))?;
        let cluster = "nisshi";
        let node = 12321;
        let topic = "test";

        let engine = Engine::builder()
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
            .cluster(cluster.to_owned())
            .storage(storage)
            .node(node)
            .build()
            .await?;

        engine
            .register_broker(BrokerRegistrationRequest {
                broker_id: node,
                cluster_id: cluster.to_owned(),
                incarnation_id: Uuid::new_v4(),
                rack: None,
            })
            .await?;

        _ = engine
            .create_topic(
                CreatableTopic::default()
                    .name(topic.to_owned())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new(topic, 0);

        let batch: deflated::Batch = (0..RECORDS)
            .fold(
                inflated::Batch::builder().last_offset_delta(RECORDS as i32 - 1),
                |builder, offset_delta| {
                    builder.record(
                        Record::builder()
                            .value(Bytes::from_static(b"value").into())
                            .offset_delta(offset_delta as i32),
                    )
                },
            )
            .build()
            .and_then(TryInto::try_into)?;

        _ = engine.produce(None, &topition, batch).await?;

        // The full read measures how long this machine takes to read the partition, so that the
        // deadline below falls partway through a read on a fast machine and a slow one alike.
        let started_at = Instant::now();

        assert_eq!(
            RECORDS,
            fetched_records(&*engine, &topition, started_at + Duration::from_secs(60)).await?
        );

        let full_read = started_at.elapsed();

        let records = fetched_records(&*engine, &topition, Instant::now() + full_read / 10).await?;

        assert!(
            records < RECORDS,
            "the deadline didn't stop the fetch: it returned all {records} of {RECORDS} records"
        );
        assert!(
            records > 0,
            "the fetch kept none of the records that it read before its deadline"
        );

        Ok(())
    }

    /// Opens an engine on a database file in a temporary directory and creates a topic of one
    /// partition on it, a virtual topic when `keyed`. Returns the engine, the partition that
    /// records are produced to and the one that a fetch reads: a keyed fetch reads the records
    /// with the key `k` of the virtual topic.
    async fn fetch_engine_with_topic(
        keyed: bool,
    ) -> Result<(tempfile::TempDir, ArcDynStorage, Topition, Topition)> {
        let current_dir = env::current_dir()?;
        let temp_dir = tempdir_in(&current_dir)?;
        let relative = temp_dir
            .path()
            .strip_prefix(&current_dir)
            .map_err(|err| Error::Message(err.to_string()))?
            .join("nisshi.db");

        let cluster = "nisshi";
        let node = 12321;
        let topic = "test";

        let engine = Engine::builder()
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
            .cluster(cluster.to_owned())
            .storage(Url::parse(&format!(
                "file:///{}?mode=direct",
                relative.display()
            ))?)
            .node(node)
            .build()
            .await?;

        engine
            .register_broker(BrokerRegistrationRequest {
                broker_id: node,
                cluster_id: cluster.to_owned(),
                incarnation_id: Uuid::new_v4(),
                rack: None,
            })
            .await?;

        _ = engine
            .create_topic(
                CreatableTopic::default()
                    .name(topic.to_owned())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some(
                        keyed
                            .then(|| {
                                CreatableTopicConfig::default()
                                    .name(topic_config::VIRTUAL.to_owned())
                                    .value(Some("true".into()))
                            })
                            .into_iter()
                            .collect(),
                    )),
                false,
            )
            .await?;

        let produced = Topition::new(topic, 0);
        let fetched = if keyed {
            Topition::new(format!("{topic}/k"), 0)
        } else {
            produced.clone()
        };

        Ok((temp_dir, engine, produced, fetched))
    }

    /// Stores `count` records, 1,000 to a batch because a batch of headers can exceed the
    /// message size limit. Each has the key `k`, or the key `j` when `alternate_keys` and its
    /// offset is odd. Each has the value `a`, and a 1 byte header key and a 99 byte header value
    /// when `with_header`. A record then costs 111 bytes against a fetch's `max_bytes`: 7 for every
    /// record, 1 for the key, 1 for the value, 2 for the header's framing and 100 for the header.
    async fn produce_keyed_records(
        engine: &ArcDynStorage,
        topition: &Topition,
        count: i64,
        with_header: bool,
        alternate_keys: bool,
    ) -> Result<()> {
        const RECORDS_PER_BATCH: i64 = 1_000;

        for first in (0..count).step_by(usize::try_from(RECORDS_PER_BATCH)?) {
            let in_batch = RECORDS_PER_BATCH.min(count - first);

            let batch = (0..in_batch)
                .fold(inflated::Batch::builder(), |builder, delta| {
                    let key: &'static [u8] = if alternate_keys && (first + delta) % 2 == 1 {
                        b"j"
                    } else {
                        b"k"
                    };

                    let record = Record::builder()
                        .key(Some(Bytes::from_static(key)))
                        .value(Some(Bytes::from_static(b"a")))
                        .offset_delta(i32::try_from(delta).unwrap_or(i32::MAX));

                    builder.record(if with_header {
                        record.header(
                            Header::builder()
                                .key(Bytes::from_static(b"h"))
                                .value(Bytes::from(vec![0; 99])),
                        )
                    } else {
                        record
                    })
                })
                .last_offset_delta(i32::try_from(in_batch - 1)?)
                .build()
                .and_then(TryInto::try_into)?;

            _ = engine.produce(None, topition, batch).await?;
        }

        Ok(())
    }

    /// Returns the offsets of the records that one engine read of `topition` from `offset`
    /// returns within `max_bytes`.
    async fn fetched_offsets(
        engine: &ArcDynStorage,
        topition: &Topition,
        offset: i64,
        max_bytes: u32,
    ) -> Result<Vec<i64>> {
        engine
            .fetch_until_deadline(
                topition,
                offset,
                1,
                max_bytes,
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_secs(60),
            )
            .await?
            .batches
            .into_iter()
            .try_fold(Vec::new(), |mut offsets, batch| {
                let batch = inflated::Batch::try_from(batch)?;

                offsets.extend(
                    batch
                        .records
                        .iter()
                        .map(|record| batch.base_offset + i64::from(record.offset_delta)),
                );

                Ok(offsets)
            })
    }

    /// One engine read returns at most `ROWS_PER_FETCH` rows, and a read from the last offset that
    /// it returned gets the rest, with no gap and no repeat. Without the limit, each read covers
    /// every row between its offset and the high watermark, so a consumer that is far behind pays
    /// for its whole backlog on every read. The test covers the statement of an ordinary topic and
    /// the statement of a virtual topic.
    #[tokio::test]
    async fn fetch_reads_a_bounded_number_of_rows_and_resumes_after_them() -> Result<()> {
        const RECORDS: i64 = ROWS_PER_FETCH + 50;

        let _guard = init_tracing()?;

        for keyed in [false, true] {
            let (_temp_dir, engine, produced, fetched) = fetch_engine_with_topic(keyed).await?;

            produce_keyed_records(&engine, &produced, RECORDS, false, false).await?;

            assert_eq!(
                (0..ROWS_PER_FETCH).collect::<Vec<_>>(),
                fetched_offsets(&engine, &fetched, 0, u32::MAX).await?,
                "one read must stop at the row limit, keyed: {keyed}",
            );

            assert_eq!(
                (ROWS_PER_FETCH..RECORDS).collect::<Vec<_>>(),
                fetched_offsets(&engine, &fetched, ROWS_PER_FETCH, u32::MAX).await?,
                "a read from the last offset must return the rest, keyed: {keyed}",
            );
        }

        Ok(())
    }

    /// A keyed read applies the key filter before the row limit, so a read from offset 0 returns
    /// every record with the key, also when the limit is smaller than the number of records
    /// that it scans. Every second record has another key, so a filter that runs after the limit
    /// returns fewer.
    #[tokio::test]
    async fn keyed_fetch_filters_by_key_before_it_applies_the_row_limit() -> Result<()> {
        const RECORDS: i64 = ROWS_PER_FETCH + 50;

        let _guard = init_tracing()?;

        let (_temp_dir, engine, produced, fetched) = fetch_engine_with_topic(true).await?;

        produce_keyed_records(&engine, &produced, RECORDS, false, true).await?;

        // The records at the even offsets have the key `k`, which is 5,025 of them. The limit
        // of 10,000 rows covers 5,000 of them when it applies first.
        assert_eq!(
            (0..RECORDS).step_by(2).collect::<Vec<_>>(),
            fetched_offsets(&engine, &fetched, 0, u32::MAX).await?,
        );

        Ok(())
    }

    /// The row limit applies to records that have headers, and a record's headers still count
    /// toward `max_bytes` when the limit ends the read. A record costs 111 bytes with the 7 of
    /// every record, so a budget of 1111 bytes returns 10 of them.
    #[tokio::test]
    async fn fetch_of_records_with_headers_stops_at_the_row_limit_or_the_byte_budget() -> Result<()>
    {
        const RECORDS: i64 = ROWS_PER_FETCH + 50;

        let _guard = init_tracing()?;

        for keyed in [false, true] {
            let (_temp_dir, engine, produced, fetched) = fetch_engine_with_topic(keyed).await?;

            produce_keyed_records(&engine, &produced, RECORDS, true, false).await?;

            assert_eq!(
                (40..40 + ROWS_PER_FETCH).collect::<Vec<_>>(),
                fetched_offsets(&engine, &fetched, 40, u32::MAX).await?,
                "the row limit counts from the fetch offset, keyed: {keyed}",
            );

            assert_eq!(
                (40..50).collect::<Vec<_>>(),
                fetched_offsets(&engine, &fetched, 40, 1111).await?,
                "the byte budget must count the headers, keyed: {keyed}",
            );
        }

        Ok(())
    }

    /// A read over more rows than the limit still returns the first record when that record
    /// alone is larger than `max_bytes` (KIP-74), so that a consumer gets past it.
    #[tokio::test]
    async fn fetch_returns_the_first_record_larger_than_max_bytes_within_a_bounded_read()
    -> Result<()> {
        const RECORDS: i64 = ROWS_PER_FETCH + 50;

        let _guard = init_tracing()?;

        for keyed in [false, true] {
            let (_temp_dir, engine, produced, fetched) = fetch_engine_with_topic(keyed).await?;

            produce_keyed_records(&engine, &produced, RECORDS, true, false).await?;

            for offset in [0, ROWS_PER_FETCH - 1, ROWS_PER_FETCH] {
                assert_eq!(
                    vec![offset],
                    fetched_offsets(&engine, &fetched, offset, 50).await?,
                    "fetch at {offset}, keyed: {keyed}",
                );
            }
        }

        Ok(())
    }

    /// Returns the offset of each row of a fetch query's result.
    async fn offsets_of(mut rows: Rows) -> Result<Vec<i64>> {
        let mut offsets = vec![];

        while let Some(row) = rows.next().await? {
            offsets.push(row.get::<i64>(0)?);
        }

        Ok(offsets)
    }

    /// A fetch from the start of a backlog of large records runs within a heap of a few MB: the
    /// window no longer copies the key and the value of every record into its temporary table. The
    /// keyed query still reads through each value on libSQL (see `record_fetch_keyed.sql`), but
    /// walking overflow pages uses little heap, so this test does not see it.
    #[tokio::test]
    async fn fetch_over_large_records_runs_within_a_small_heap() -> Result<()> {
        const RECORDS: i64 = 40;
        const VALUE_BYTES: usize = 500_000;
        const KEY: &[u8] = b"k";

        let _guard = init_tracing()?;

        let cluster = "nisshi";
        let topic = "test";

        let (_temp_dir, connection) = scratch_connection().await?;

        _ = connection
            .connection
            .execute("insert into cluster (name) values (?1)", [cluster])
            .await?;

        _ = connection
            .connection
            .execute(
                "insert into topic (cluster, name, uuid, partitions, replication_factor) \
                 select id, ?2, ?3, 1, 1 from cluster where name = ?1",
                (cluster, topic, Uuid::new_v4().to_string()),
            )
            .await?;

        _ = connection
            .connection
            .execute(
                "insert into topition (topic, partition) select id, 0 from topic where name = ?1",
                [topic],
            )
            .await?;

        let value = vec![b'x'; VALUE_BYTES];

        for offset in 0..RECORDS {
            _ = connection
                .connection
                .execute(
                    "insert into record (topition, offset_id, k, v) \
                     select id, ?1, ?2, ?3 from topition",
                    (offset, KEY.to_vec(), value.clone()),
                )
                .await?;
        }

        // With the default temp store, SQLite spills the window's temporary table to a file, and
        // the heap limit below would not see it.
        _ = connection
            .connection
            .query("PRAGMA temp_store = memory", ())
            .await?
            .next()
            .await?;

        // The page cache of this connection counts toward the heap limit below. A small cache
        // keeps the pages of the inserts above out of what the limit measures.
        _ = connection
            .connection
            .query("PRAGMA cache_size = -256", ())
            .await?
            .next()
            .await?;

        // Each record is 500,001 bytes, more than the budget, so each fetch returns only its
        // first record (KIP-74).
        let max_bytes = 100_000_i64;
        let high_watermark = RECORDS;

        // The heap limit covers every SQLite connection in this process, and SQLite lets a later
        // call only lower it, so nothing in this process can raise it again. The test is safe
        // because nextest runs each test in a process of its own. Under `cargo test`, every test
        // that runs after this one in the same binary would run within this limit.
        //
        // With the key and the value of each record in the window, these fetches need more than
        // 6 MB, and without them they run within 3 MB. The edge depends on how SQLite buffers the
        // rows of a window, so an update of libsql can move it.
        _ = connection
            .connection
            .query("PRAGMA hard_heap_limit = 4000000", ())
            .await?
            .next()
            .await?;

        let plain = connection
            .query(
                "record_fetch.sql",
                (
                    cluster,
                    topic,
                    0,
                    0,
                    max_bytes,
                    high_watermark,
                    ROWS_PER_FETCH,
                ),
            )
            .await?;

        assert_eq!(vec![0], offsets_of(plain).await?);

        let keyed = connection
            .query(
                "record_fetch_keyed.sql",
                (
                    cluster,
                    topic,
                    0,
                    0,
                    max_bytes,
                    high_watermark,
                    KEY.to_vec(),
                    ROWS_PER_FETCH,
                ),
            )
            .await?;

        assert_eq!(vec![0], offsets_of(keyed).await?);

        Ok(())
    }

    /// `produce_inflated` stores the batch that the caller decoded and does not decode the bytes
    /// again, also through the channel to the engine's single writer.
    ///
    /// The test sends record bytes that do not decode, next to the batch that does. `produce`
    /// decodes them and fails. `produce_inflated` stores the decoded records, so it never reads
    /// the bytes.
    #[tokio::test]
    async fn produce_inflated_does_not_decode_again() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir_in(env::current_dir()?).inspect(|temporary| debug!(?temporary))?;
        let relative = temp_dir
            .path()
            .strip_prefix(env::current_dir()?)
            .map_err(|err| Error::Message(err.to_string()))?
            .join("nisshi.db");

        let storage = Url::parse(&format!("file:///{}?mode=mpsc", relative.display()))?;
        let cluster = "nisshi";
        let node = 12321;
        let topic = "test";

        let engine = Engine::builder()
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
            .cluster(cluster.to_owned())
            .storage(storage)
            .node(node)
            .build()
            .await?;

        engine
            .register_broker(BrokerRegistrationRequest {
                broker_id: node,
                cluster_id: cluster.to_owned(),
                incarnation_id: Uuid::new_v4(),
                rack: None,
            })
            .await?;

        _ = engine
            .create_topic(
                CreatableTopic::default()
                    .name(topic.to_owned())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let inflated = inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"decoded once").into()))
            .build()?;
        let mut undecodable: deflated::Batch = inflated.clone().try_into()?;
        undecodable.record_data = Bytes::from_static(b"\xff\xff\xff");

        let topition = Topition::new(topic, 0);

        assert_matches!(
            engine.produce(None, &topition, undecodable.clone()).await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        );

        assert_eq!(
            0,
            engine
                .produce_inflated(None, &topition, undecodable, inflated)
                .await?
        );

        let fetched = engine
            .fetch_until_deadline(
                &topition,
                0,
                0,
                1_000_000,
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .map(|read| read.batches)?;
        let records: Vec<_> = fetched
            .into_iter()
            .map(inflated::Batch::try_from)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flat_map(|batch| batch.records)
            .collect();

        assert_eq!(1, records.len());
        assert_eq!(
            Some(Bytes::from_static(b"decoded once")),
            records[0].value.clone()
        );

        Ok(())
    }

    /// The storage URL has `?mode=direct`, so `produce` runs on the test
    /// task itself, with no channel to another task in between.
    ///
    /// The test runtime has a single thread. If `produce` inflated the batch
    /// inline, the produce task would decode and store it within its first
    /// poll, so it would already be finished when `yield_now` returns.
    /// Offloaded, it is still waiting on the decode.
    #[tokio::test]
    async fn produce_inflates_off_the_runtime_thread() -> Result<()> {
        const RECORDS: usize = 1_000;
        const RECORD_SIZE: usize = 1_024;

        let _guard = init_tracing()?;

        // The builder resolves the storage path against the current
        // directory, so the database lives in a temporary directory there.
        let current_dir = env::current_dir()?;
        let temp_dir = tempdir_in(&current_dir).inspect(|temporary| debug!(?temporary))?;
        let relative = temp_dir
            .path()
            .strip_prefix(&current_dir)
            .map_err(|err| Error::Message(err.to_string()))?
            .join("nisshi.db");

        let storage = Url::parse(&format!("file:///{}?mode=direct", relative.display()))?;
        let cluster = "nisshi";
        let node = 12321;
        let topic = "test";

        let engine = Engine::builder()
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
            .cluster(cluster.to_owned())
            .storage(storage)
            .node(node)
            .build()
            .await?;

        engine
            .register_broker(BrokerRegistrationRequest {
                broker_id: node,
                cluster_id: cluster.to_owned(),
                incarnation_id: Uuid::new_v4(),
                rack: None,
            })
            .await?;

        _ = engine
            .create_topic(
                CreatableTopic::default()
                    .name(topic.to_owned())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        // Pseudo-random values, so gzip has real work to do on decode.
        let mut seed = 0x2545_f491_u32;
        let mut builder = inflated::Batch::builder()
            .attributes(
                BatchAttribute::default()
                    .compression(Compression::Gzip)
                    .into(),
            )
            .producer_id(-1)
            .producer_epoch(-1)
            .base_sequence(-1)
            .last_offset_delta(RECORDS as i32 - 1);

        for offset_delta in 0..RECORDS {
            let value = (0..RECORD_SIZE)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 17;
                    seed ^= seed << 5;
                    seed as u8
                })
                .collect::<Bytes>();

            builder = builder.record(
                Record::builder()
                    .value(value.into())
                    .offset_delta(offset_delta as i32),
            );
        }

        let batch: deflated::Batch = builder.build().and_then(TryInto::try_into)?;

        let topition = Topition::new(topic, 0);

        let produce = {
            let engine = engine.clone();
            let topition = topition.clone();

            tokio::spawn(async move { engine.produce(None, &topition, batch).await })
        };

        yield_now().await;

        assert!(
            !produce.is_finished(),
            "the batch was inflated on the runtime thread"
        );

        assert_eq!(0, produce.await??);

        Ok(())
    }

    #[allow(dead_code)]
    async fn clean_up() -> Result<()> {
        let relative = db_path().map(|path| format!("{path}*"))?;

        let mut path = env::current_dir()?;
        path.push(relative);
        debug!(?path);

        if let Some(pattern) = path.to_str() {
            debug!(pattern);

            let paths = glob::glob(pattern)?;

            for path in paths.flatten() {
                debug!(?path);

                remove_file(path).await?;
            }
        }

        Ok(())
    }

    #[allow(dead_code)]
    async fn storage_container(cluster: &str, node: i32) -> Result<ArcDynStorage> {
        StorageContainer::builder()
            .cluster_id(cluster)
            .node_id(node)
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
            .schema_registry(None)
            .storage(
                db_path()
                    .map(|path| format!("sqlite://{path}"))
                    .inspect(|url| debug!(url))
                    .and_then(|url| Url::parse(&url).map_err(Into::into))?,
            )
            .build()
            .await
    }

    #[allow(dead_code)]
    fn db_path() -> Result<String> {
        thread::current()
            .name()
            .ok_or(Error::Message(String::from("unnamed thread")))
            .map(|name| {
                format!(
                    "../logs/{}/{}::{name}.db",
                    env!("CARGO_PKG_NAME"),
                    env!("CARGO_CRATE_NAME")
                )
            })
    }

    fn txn_batch(producer_id: i64) -> deflated::Batch {
        inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"x").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer_id)
            .producer_epoch(0)
            .base_sequence(0)
            .last_offset_delta(0)
            .build()
            .and_then(TryInto::try_into)
            .expect("deflated txn batch")
    }

    /// A crate-local database file that is removed, with its WAL and SHM
    /// sidecars, when the guard is created and when it is dropped, including
    /// on a panic.
    ///
    /// lite's `build()` resolves the URL path relative to the crate directory
    /// (it strips the leading "/" and pushes onto cwd), so a test that reads
    /// the database file itself uses a crate-local file. A tempdir does not
    /// work here.
    struct DbFileGuard(&'static str);

    impl DbFileGuard {
        fn new(db_file: &'static str) -> Self {
            let guard = Self(db_file);
            guard.remove();
            guard
        }

        fn remove(&self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", self.0));
            }
        }
    }

    impl Drop for DbFileGuard {
        fn drop(&mut self) {
            self.remove();
        }
    }

    /// An open (uncommitted) transaction must pin the read_committed last stable
    /// offset below the high watermark. Regressed when `offset_stage` read the
    /// high-watermark column instead of the stable-offset column, so
    /// `read_committed` consumers saw uncommitted records.
    #[tokio::test]
    async fn read_committed_last_stable_offset_pinned_by_open_transaction() -> Result<()> {
        use nisshi_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

        let _guard = init_tracing()?;

        let db_file = "read-committed-last-stable-offset-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);
        let storage = Url::parse(&format!("file:///{db_file}"))?;
        let cluster = "nisshi";
        let node = 12321;

        let engine = Engine::builder()
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
            .cluster(cluster.to_owned())
            .storage(storage)
            .node(node)
            .build()
            .await?;

        engine
            .register_broker(BrokerRegistrationRequest {
                broker_id: node,
                cluster_id: cluster.to_owned(),
                incarnation_id: Uuid::new_v4(),
                rack: None,
            })
            .await?;

        _ = engine
            .create_topic(
                CreatableTopic::default()
                    .name("t".into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        // a transactional producer begins a transaction and produces, never commits
        let producer = engine
            .init_producer(Some("tx"), 0, Some(-1), Some(-1))
            .await?;
        let pid = producer.id;

        _ = engine
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: "tx".into(),
                producer_id: pid,
                producer_epoch: 0,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name("t".into())
                        .partitions(Some(vec![0])),
                ],
            })
            .await?;

        _ = engine
            .produce(Some("tx"), &Topition::new("t", 0), txn_batch(pid))
            .await?;

        let stage = engine.offset_stage(&Topition::new("t", 0)).await?;

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

    /// DeleteRecords can move the log start past the first offset of an open
    /// transaction. The last stable offset then answers the log start, as
    /// Kafka raises it, and not the transaction's first offset below it.
    #[tokio::test]
    async fn delete_records_past_open_transaction_raises_last_stable_to_log_start() -> Result<()> {
        use nisshi_sans_io::{
            add_partitions_to_txn_request::AddPartitionsToTxnTopic,
            delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
        };

        let _guard = init_tracing()?;

        let db_file = "delete-records-past-open-transaction-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);
        let storage = Url::parse(&format!("file:///{db_file}"))?;
        let cluster = "nisshi";
        let node = 12321;

        let engine = Engine::builder()
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
            .cluster(cluster.to_owned())
            .storage(storage)
            .node(node)
            .build()
            .await?;

        engine
            .register_broker(BrokerRegistrationRequest {
                broker_id: node,
                cluster_id: cluster.to_owned(),
                incarnation_id: Uuid::new_v4(),
                rack: None,
            })
            .await?;

        _ = engine
            .create_topic(
                CreatableTopic::default()
                    .name("t".into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new("t", 0);

        // a transaction opens at offset 0 and stays open
        let producer = engine
            .init_producer(Some("tx"), 0, Some(-1), Some(-1))
            .await?;

        _ = engine
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: "tx".into(),
                producer_id: producer.id,
                producer_epoch: 0,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name("t".into())
                        .partitions(Some(vec![0])),
                ],
            })
            .await?;

        assert_eq!(
            0,
            engine
                .produce(Some("tx"), &topition, txn_batch(producer.id))
                .await?
        );

        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"after").into()))
            .build()
            .and_then(TryInto::try_into)?;
        assert_eq!(1, engine.produce(None, &topition, batch).await?);

        let log_start = 1;

        let results = engine
            .delete_records(&[DeleteRecordsTopic::default()
                .name("t".into())
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
            engine.offset_stage(&topition).await?
        );

        Ok(())
    }

    /// A DeleteRecords cut removes the rows below it from the `record` table.
    /// The watermark alone does not show this, so the test counts the rows in
    /// the database file.
    #[tokio::test]
    async fn delete_records_removes_rows_below_the_cut() -> Result<()> {
        use nisshi_sans_io::delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic};

        let _guard = init_tracing()?;

        let db_file = "delete-records-removes-rows-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);
        let storage = Url::parse(&format!("file:///{db_file}"))?;
        let cluster = "nisshi";
        let node = 12321;

        let engine = Engine::builder()
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
            .cluster(cluster.to_owned())
            .storage(storage)
            .node(node)
            .build()
            .await?;

        engine
            .register_broker(BrokerRegistrationRequest {
                broker_id: node,
                cluster_id: cluster.to_owned(),
                incarnation_id: Uuid::new_v4(),
                rack: None,
            })
            .await?;

        _ = engine
            .create_topic(
                CreatableTopic::default()
                    .name("t".into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let topition = Topition::new("t", 0);
        let record_count = 6i64;

        for n in 0..record_count {
            let batch = inflated::Batch::builder()
                .record(Record::builder().value(Some(Bytes::from(format!("record-{n}")))))
                .build()
                .and_then(TryInto::try_into)?;

            _ = engine.produce(None, &topition, batch).await?;
        }

        // the engine opened `file:///{db_file}` as `{cwd}/{db_file}`
        let connection = libsql::Builder::new_local(db_file)
            .build()
            .await?
            .connect()?;

        // the stored rows, as (count, lowest offset)
        let stored_rows = async || -> Result<(i64, Option<i64>)> {
            let mut rows = connection
                .query("select count(*), min(offset_id) from record", ())
                .await?;

            let row = rows
                .next()
                .await?
                .ok_or_else(|| Error::Message(String::from("no count row")))?;

            Ok((row.get::<i64>(0)?, row.get::<Option<i64>>(1)?))
        };

        let delete_up_to = async |offset: i64| -> Result<(ErrorCode, i64)> {
            let results = engine
                .delete_records(&[DeleteRecordsTopic::default()
                    .name("t".into())
                    .partitions(Some(
                        [DeleteRecordsPartition::default()
                            .partition_index(0)
                            .offset(offset)]
                        .into(),
                    ))])
                .await?;

            let partitions = results[0].partitions.clone().unwrap_or_default();

            Ok((
                ErrorCode::try_from(partitions[0].error_code)?,
                partitions[0].low_watermark,
            ))
        };

        assert_eq!((record_count, Some(0)), stored_rows().await?);

        let cutoff = 3;
        assert_eq!((ErrorCode::None, cutoff), delete_up_to(cutoff).await?);
        assert_eq!((record_count - cutoff, Some(cutoff)), stored_rows().await?);

        assert_eq!((ErrorCode::None, record_count), delete_up_to(-1).await?);
        assert_eq!((0, None), stored_rows().await?);

        Ok(())
    }

    /// A record without a timestamp stores NO_TIMESTAMP, -1. Retention ages it from when the
    /// broker appended it, so a sweep deletes it once `retention.ms` has passed since then, and
    /// keeps a record appended within `retention.ms` in the same sweep.
    #[tokio::test]
    async fn retention_ages_a_record_without_a_timestamp_from_its_append_time() -> Result<()> {
        let _guard = init_tracing()?;
        let db_file = "retention-record-without-timestamp-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);
        let (engine, topition) = engine_with_topic(db_file).await?;

        execute_on(
            db_file,
            "insert into topic_configuration (topic, name, value) \
             select id, 'retention.ms', '600000' from topic where name = 't'",
            (),
        )
        .await?;

        for _ in 0..2 {
            let batch = inflated::Batch::builder()
                .base_timestamp(-1)
                .max_timestamp(-1)
                .record(Record::builder().value(Some(Bytes::from_static(b"a"))))
                .build()
                .and_then(TryInto::try_into)?;

            _ = engine.produce(None, &topition, batch).await?;
        }

        // Offset 0 was appended an hour ago, which is longer than the retention. A sweep that
        // aged it by the sweep time, which is also its age now, would keep it.
        execute_on(
            db_file,
            "update record set created_at = datetime('now', '-1 hour') where offset_id = 0",
            (),
        )
        .await?;

        engine.maintain(SystemTime::now()).await?;

        let mut rows = libsql::Builder::new_local(db_file)
            .build()
            .await?
            .connect()?
            .query("select offset_id from record order by offset_id", ())
            .await?;

        let mut stored = vec![];
        while let Some(row) = rows.next().await? {
            stored.push(row.get::<i64>(0)?);
        }

        assert_eq!(vec![1], stored);

        Ok(())
    }

    async fn scratch_connection() -> Result<(tempfile::TempDir, managed::Object<ConnectionManager>)>
    {
        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let db = libsql::Builder::new_local(temp_dir.path().join("nisshi.db"))
            .build()
            .await?;

        ensure_schema(&db, Duration::from_secs(5)).await?;

        let pool = Pool::builder(ConnectionManager {
            db: Arc::new(Mutex::new(db)),
            busy_timeout: Duration::from_millis(100),
        })
        .build()?;

        Ok((temp_dir, pool.get().await?))
    }

    async fn cluster_names(connection: &PoolConnection) -> Result<Vec<String>> {
        let mut rows = connection
            .connection
            .query("select name from cluster order by name", ())
            .await?;

        let mut names = vec![];
        while let Some(row) = rows.next().await? {
            names.push(row.get::<String>(0)?);
        }
        Ok(names)
    }

    /// `PoolConnection::execute` runs a parameterised write key through the cache. A second call
    /// reuses the first call's statement and binds its own values.
    #[tokio::test]
    async fn execute_reuses_the_cached_statement_and_binds_each_call() -> Result<()> {
        let _guard = init_tracing()?;

        let key = "register_broker.sql";
        let (_directory, connection) = scratch_connection().await?;

        assert_eq!(1, connection.execute(key, ["alpha"]).await?);
        let first_statement = connection.statements.lock().unwrap().get(key).cloned();
        assert!(first_statement.is_some());

        assert_eq!(1, connection.execute(key, ["beta"]).await?);
        let second_statement = connection.statements.lock().unwrap().get(key).cloned();
        assert!(Arc::ptr_eq(
            &first_statement.unwrap(),
            &second_statement.unwrap()
        ));

        assert_eq!(1, connection.statements.lock().unwrap().len());
        assert_eq!(vec!["alpha", "beta"], cluster_names(&connection).await?);

        Ok(())
    }

    /// A reset statement keeps the values last bound, so a call that binds fewer parameters than
    /// the statement has is refused, not run with the previous call's value.
    #[tokio::test]
    async fn execute_refuses_a_call_that_binds_the_wrong_number_of_parameters() -> Result<()> {
        let _guard = init_tracing()?;

        let key = "register_broker.sql";
        let (_directory, connection) = scratch_connection().await?;

        assert_eq!(1, connection.execute(key, ["alpha"]).await?);

        let err = connection.execute(key, ()).await.unwrap_err();
        assert_matches!(&err, libsql::Error::Misuse(message)
            if message.contains(key) && message.contains("bound 0") && message.contains("has 1"));

        let err = connection.execute(key, ["a", "b"]).await.unwrap_err();
        assert_matches!(&err, libsql::Error::Misuse(message) if message.contains("bound 2"));

        assert_eq!(vec!["alpha"], cluster_names(&connection).await?);

        Ok(())
    }

    /// The SCRAM credential keys carry salts and keys, so a connection prepares them afresh and
    /// keeps no statement bound to the last credential.
    #[tokio::test]
    async fn execute_does_not_cache_scram_credential_statements() -> Result<()> {
        let _guard = init_tracing()?;

        let (_directory, connection) = scratch_connection().await?;

        assert_eq!(1, connection.execute("register_broker.sql", ["c"]).await?);
        assert_eq!(
            1,
            connection
                .execute(
                    "scram_credential_insert.sql",
                    (
                        "c",
                        "alice",
                        1_i32,
                        &b"salt"[..],
                        4096_i32,
                        &b"stored"[..],
                        &b"server"[..],
                    ),
                )
                .await?
        );

        let cached = connection
            .statements
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(vec!["register_broker.sql"], cached);

        Ok(())
    }

    /// A connection returned to the pool inside an open transaction holds
    /// SQLite's write lock. The pool discards it on its next checkout of that
    /// object, not on return: `recycle` rejects it, dropping it closes the
    /// handle and rolls the transaction back, and the pool hands out a fresh
    /// connection instead. Until that checkout, a writer that pops another
    /// idle connection first can still see `SQLITE_BUSY` (the queue is FIFO,
    /// so this is bounded by the number of idle connections ahead of it).
    #[tokio::test]
    async fn pool_discards_connection_left_in_open_transaction() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let db = libsql::Builder::new_local(temp_dir.path().join("nisshi.db"))
            .build()
            .await?;

        let pool = Pool::builder(ConnectionManager {
            db: Arc::new(Mutex::new(db)),
            busy_timeout: Duration::from_millis(100),
        })
        .build()?;

        {
            let stale = pool.get().await?;
            _ = stale.connection.execute("BEGIN IMMEDIATE", ()).await?;
            assert!(!stale.connection.is_autocommit());
        }

        let next = pool.get().await?;
        assert!(next.connection.is_autocommit());
        assert_eq!(0, managed::Object::metrics(&next).recycle_count);
        assert_eq!(1, pool.status().size);

        let other = pool.get().await?;
        let tx = other.transaction().await?;
        other.commit(tx).await?;

        Ok(())
    }

    /// A change to the schema must come with a new [`SCHEMA_VERSION`], and with an upgrade step for
    /// databases of the earlier version. A database that records a version keeps the tables that
    /// it has, so a change without a new version reaches only new databases. Update the digest
    /// here once both are in place.
    #[test]
    fn schema_matches_its_version() -> Result<()> {
        let digest = crate::schema::schema_digest(
            DDL.values()
                .map(|statement| statement.as_str())
                .chain([SQL.get(sql::NISSHI_SCHEMA_VERSION_CREATE)?]),
        );

        assert_eq!(
            (SCHEMA_VERSION, digest.as_str()),
            (
                1,
                "4b97d00984999b16629d9d0a7a6da18e8458adc6a57cd0affa36f407b819d1dd"
            )
        );
        Ok(())
    }

    /// Each query that libSQL runs returns every column that its module's `RowReader` reads, once.
    /// The broker reads a query's rows only on the paths that run it, so this test finds a query
    /// that doesn't match its row before a rare path does.
    #[tokio::test]
    async fn every_row_query_returns_the_columns_of_its_row() -> Result<()> {
        let directory = tempdir()?;
        let db = libsql::Builder::new_local(directory.path().join("nisshi.db"))
            .build()
            .await?;
        ensure_schema(&db, Duration::from_secs(5)).await?;
        let connection = db.connect()?;

        let mut failures = Vec::new();
        for row_query in sql::row_queries(sql::SqlEngine::Libsql) {
            let checked = match connection.prepare(SQL.get(row_query.query)?).await {
                Ok(statement) => {
                    (row_query.check_columns)(&ResultColumns::new(row_query.query, &statement))
                }
                Err(error) => Err(error.into()),
            };

            if let Err(error) = checked {
                failures.push(format!("{}: {error:?}", row_query.query));
            }
        }

        assert!(failures.is_empty(), "{failures:#?}");
        Ok(())
    }

    /// Returns the version that `db`'s `nisshi_schema_version` table records, or `None` when it
    /// records none.
    async fn recorded_version(db: &Database) -> Result<Option<i32>> {
        db.connect()?
            .query("select version from nisshi_schema_version", ())
            .await?
            .next()
            .await?
            .map(|row| row.get::<i32>(0))
            .transpose()
            .map_err(Into::into)
    }

    /// In a new file, `ensure_schema` must create every table and record [`SCHEMA_VERSION`].
    #[tokio::test]
    async fn ensure_schema_creates_the_schema_in_a_new_file() -> Result<()> {
        let directory = tempdir()?;
        let db = libsql::Builder::new_local(directory.path().join("nisshi.db"))
            .build()
            .await?;

        ensure_schema(&db, Duration::from_secs(5)).await?;

        assert_eq!(recorded_version(&db).await?, Some(SCHEMA_VERSION));
        for table in ["cluster", "topic", "record", "scram_credential"] {
            _ = db
                .connect()?
                .query(&format!("select count(*) from {table}"), ())
                .await?;
        }

        Ok(())
    }

    /// Brokers that start together on one new file must all start, and the file must record
    /// [`SCHEMA_VERSION`].
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ensure_schema_succeeds_for_brokers_that_start_together() -> Result<()> {
        const BROKERS: usize = 4;

        let directory = tempdir()?;
        let path = directory.path().join("nisshi.db");
        let barrier = Arc::new(tokio::sync::Barrier::new(BROKERS));

        let mut setups = Vec::with_capacity(BROKERS);
        for _ in 0..BROKERS {
            let db = libsql::Builder::new_local(&path).build().await?;
            let barrier = Arc::clone(&barrier);
            setups.push(tokio::spawn(async move {
                _ = barrier.wait().await;
                ensure_schema(&db, Duration::from_secs(5)).await
            }));
        }
        for setup in setups {
            setup.await??;
        }

        let db = libsql::Builder::new_local(&path).build().await?;
        assert_eq!(recorded_version(&db).await?, Some(SCHEMA_VERSION));
        Ok(())
    }

    /// A newer release set up this file with a schema that this release can use. `ensure_schema`
    /// must leave the tables and the recorded version as they are.
    #[tokio::test]
    async fn ensure_schema_keeps_a_file_that_records_a_version() -> Result<()> {
        let directory = tempdir()?;
        let db = libsql::Builder::new_local(directory.path().join("nisshi.db"))
            .build()
            .await?;
        ensure_schema(&db, Duration::from_secs(5)).await?;

        // This makes the file look as if a newer release set it up.
        _ = db
            .connect()?
            .execute("update nisshi_schema_version set version = 2", ())
            .await?;

        ensure_schema(&db, Duration::from_secs(5)).await?;

        assert_eq!(recorded_version(&db).await?, Some(2));

        Ok(())
    }

    /// A file whose tables an earlier release created records no version. `ensure_schema` must
    /// record version 1 in it.
    #[tokio::test]
    async fn ensure_schema_records_the_version_of_a_file_created_without_one() -> Result<()> {
        let directory = tempdir()?;
        let db = libsql::Builder::new_local(directory.path().join("nisshi.db"))
            .build()
            .await?;
        ensure_schema(&db, Duration::from_secs(5)).await?;

        // This makes the file look as if a release before 0.7.0 set it up.
        _ = db
            .connect()?
            .execute("drop table nisshi_schema_version", ())
            .await?;

        ensure_schema(&db, Duration::from_secs(5)).await?;

        assert_eq!(recorded_version(&db).await?, Some(SCHEMA_VERSION));
        Ok(())
    }

    /// The storage file isn't a SQLite database. The broker must stop at startup with a
    /// `SchemaSetup` failure, instead of running without tables.
    #[tokio::test]
    async fn factory_fails_with_schema_setup_on_a_file_that_is_not_a_database() -> Result<()> {
        let directory = tempdir()?;
        let path = directory.path().join("nisshi.db");
        std::fs::write(&path, [0x5a_u8; 8192])?;

        let storage = Url::parse(&format!("sqlite:///{}", path.display()))?;

        let built = factory::LiteFactory
            .build(nisshi_storage::StorageFactoryConfiguration {
                node_id: 111,
                cluster: String::from("nisshi"),
                advertised_listener: Url::parse("tcp://127.0.0.1:9092")?,
                storage: storage.clone(),
                schema_registry: None,
                lake_house: None,
                cancellation: CancellationToken::new(),
            })
            .await;

        match built {
            Err(Error::StartupFailed {
                storage: reported,
                failure: StartupFailure::SchemaSetup,
                ..
            }) => assert_eq!(*reported, storage),
            Err(err) => panic!("expected the SchemaSetup failure, got {err:?}"),
            Ok(_) => panic!("expected the SchemaSetup failure, got a storage"),
        }

        Ok(())
    }

    /// A newer release set up this file with a schema that this release can't use. The broker must
    /// stop at startup with an `IncompatibleSchema` error.
    #[tokio::test]
    async fn factory_refuses_a_file_whose_schema_this_release_cannot_use() -> Result<()> {
        let directory = tempdir()?;
        let path = directory.path().join("nisshi.db");
        let db = libsql::Builder::new_local(&path).build().await?;
        ensure_schema(&db, Duration::from_secs(5)).await?;

        // This makes the file look as if a newer release set it up with a schema that this
        // release can't use.
        _ = db
            .connect()?
            .execute(
                "update nisshi_schema_version set version = 2, oldest_compatible_version = 2",
                (),
            )
            .await?;
        drop(db);

        let built = factory::LiteFactory
            .build(nisshi_storage::StorageFactoryConfiguration {
                node_id: 111,
                cluster: String::from("nisshi"),
                advertised_listener: Url::parse("tcp://127.0.0.1:9092")?,
                storage: Url::parse(&format!("sqlite:///{}", path.display()))?,
                schema_registry: None,
                lake_house: None,
                cancellation: CancellationToken::new(),
            })
            .await;

        match built {
            Err(Error::StartupFailed {
                failure: StartupFailure::SchemaSetup,
                source,
                ..
            }) => assert_matches!(*source, Error::IncompatibleSchema { .. }),
            Err(err) => panic!("expected the SchemaSetup failure, got {err:?}"),
            Ok(_) => panic!("expected the SchemaSetup failure, got a storage"),
        }

        Ok(())
    }

    /// Returns an engine on `db_file` with a registered broker, and one topic with one
    /// partition.
    async fn engine_with_topic(db_file: &str) -> Result<(ArcDynStorage, Topition)> {
        let cluster = "nisshi";
        let node = 12321;

        let engine = Engine::builder()
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
            .cluster(cluster.to_owned())
            .storage(Url::parse(&format!("file:///{db_file}"))?)
            .node(node)
            .build()
            .await?;

        engine
            .register_broker(BrokerRegistrationRequest {
                broker_id: node,
                cluster_id: cluster.to_owned(),
                incarnation_id: Uuid::new_v4(),
                rack: None,
            })
            .await?;

        _ = engine
            .create_topic(
                CreatableTopic::default()
                    .name("t".into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        Ok((engine, Topition::new("t", 0)))
    }

    /// Runs `sql` with `params` on `db_file`, through a connection of its own. The
    /// engine opened `file:///{db_file}` as `{cwd}/{db_file}`.
    async fn execute_on(db_file: &str, sql: &str, params: impl IntoParams) -> Result<()> {
        _ = libsql::Builder::new_local(db_file)
            .build()
            .await?
            .connect()?
            .execute(sql, params)
            .await?;

        Ok(())
    }

    /// Sets the epoch of each `producer_epoch` row of `producer` to `epoch`. A producer
    /// that has one epoch then has only `epoch`, without 32k InitProducerId requests.
    async fn seed_producer_epoch(db_file: &str, producer: i64, epoch: i32) -> Result<()> {
        execute_on(
            db_file,
            "update producer_epoch set epoch = ?1 where producer = ?2",
            (epoch, producer),
        )
        .await
    }

    fn idempotent_batch(producer: &ProducerIdResponse) -> Result<deflated::Batch> {
        inflated::Batch::builder()
            .record(Record::builder().value(Some(Bytes::from_static(b"idempotent"))))
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
        let _guard = init_tracing()?;
        let db_file = "init-producer-exhausted-epoch-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);
        let (engine, topition) = engine_with_topic(db_file).await?;

        let producer = engine
            .init_producer(Some("txn"), 10_000, None, None)
            .await?;
        seed_producer_epoch(db_file, producer.id, 32765).await?;

        let bumped = engine
            .init_producer(Some("txn"), 10_000, None, None)
            .await?;
        assert_eq!(
            (ErrorCode::None, producer.id, 32766),
            (bumped.error, bumped.id, bumped.epoch)
        );

        let rotated = engine
            .init_producer(Some("txn"), 10_000, None, None)
            .await?;
        assert_eq!((ErrorCode::None, 0), (rotated.error, rotated.epoch));
        assert_ne!(producer.id, rotated.id);

        assert_eq!(
            ProducerIdResponse::failed(ErrorCode::ProducerFenced),
            engine
                .init_producer(Some("txn"), 10_000, Some(bumped.id), Some(bumped.epoch))
                .await?
        );

        assert_matches!(
            engine
                .produce(None, &topition, idempotent_batch(&bumped)?)
                .await,
            Err(Error::Api(ErrorCode::ProducerFenced))
        );
        assert_eq!(
            0,
            engine
                .produce(None, &topition, idempotent_batch(&rotated)?)
                .await?
        );

        let claimed = engine
            .init_producer(Some("txn"), 10_000, Some(rotated.id), Some(rotated.epoch))
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
        let _guard = init_tracing()?;
        let db_file = "init-producer-exhausted-claim-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);
        let (engine, _) = engine_with_topic(db_file).await?;

        let producer = engine
            .init_producer(Some("txn"), 10_000, None, None)
            .await?;
        seed_producer_epoch(db_file, producer.id, 32766).await?;

        let rotated = engine
            .init_producer(Some("txn"), 10_000, Some(producer.id), Some(32766))
            .await?;
        assert_eq!((ErrorCode::None, 0), (rotated.error, rotated.epoch));
        assert_ne!(producer.id, rotated.id);

        Ok(())
    }

    /// An exhausted epoch with a prepared transaction answers ConcurrentTransactions.
    #[tokio::test]
    async fn init_producer_exhausted_epoch_with_a_prepared_transaction_is_concurrent() -> Result<()>
    {
        let _guard = init_tracing()?;
        let db_file = "init-producer-exhausted-prepared-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);
        let (engine, _) = engine_with_topic(db_file).await?;

        let producer = engine
            .init_producer(Some("txn"), 10_000, None, None)
            .await?;
        seed_producer_epoch(db_file, producer.id, 32766).await?;
        execute_on(
            db_file,
            "update txn_detail set status = 'PREPARE_ABORT'",
            (),
        )
        .await?;

        assert_eq!(
            ProducerIdResponse::failed(ErrorCode::ConcurrentTransactions),
            engine
                .init_producer(Some("txn"), 10_000, None, None)
                .await?
        );

        Ok(())
    }

    /// sqlite does not limit an integer column to `i16`. A stored epoch beyond `i16::MAX`
    /// is an error, not an epoch that wraps to a negative value.
    #[tokio::test]
    async fn init_producer_epoch_beyond_i16_is_an_error() -> Result<()> {
        let _guard = init_tracing()?;
        let db_file = "init-producer-epoch-beyond-i16-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);
        let (engine, _) = engine_with_topic(db_file).await?;

        let producer = engine
            .init_producer(Some("txn"), 10_000, None, None)
            .await?;
        seed_producer_epoch(db_file, producer.id, 40_000).await?;

        assert_matches!(
            engine.init_producer(Some("txn"), 10_000, None, None).await,
            Err(Error::UnreadableColumn { column: "epoch", source, .. })
                if matches!(*source, Error::TryFromInt(_))
        );

        Ok(())
    }

    /// Returns a connection of its own on `db_file`, which holds the topics `t` and `u`.
    async fn connection_with_two_topics(db_file: &str) -> Result<PoolConnection> {
        let (engine, _) = engine_with_topic(db_file).await?;

        _ = engine
            .create_topic(
                CreatableTopic::default()
                    .name("u".into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        let connection = libsql::Builder::new_local(db_file)
            .build()
            .await?
            .connect()?;

        PoolConnection::new(connection).await
    }

    /// A one-row read of a result with two rows is an error, instead of the first row.
    #[tokio::test]
    async fn query_one_of_two_rows_is_an_error() -> Result<()> {
        let _guard = init_tracing()?;
        let db_file = "query-one-of-two-rows-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);
        let connection = connection_with_two_topics(db_file).await?;

        assert_matches!(
            connection
                .query_one(topic_by_cluster::QUERY, ["nisshi"])
                .await,
            Err(Error::LibSql(err))
                if matches!(&*err, libsql::Error::Misuse(message) if message.contains("more than one row"))
        );

        Ok(())
    }

    /// Each row that a read returns holds its own values. A libSQL `Row` reads from the
    /// statement's current row, so a row read after the next step would hold the next row's
    /// values.
    #[tokio::test]
    async fn query_leading_rows_keeps_each_rows_values() -> Result<()> {
        let _guard = init_tracing()?;
        let db_file = "query-leading-rows-values-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);
        let connection = connection_with_two_topics(db_file).await?;

        let mut names: Vec<String> = connection
            .query_leading_rows(topic_by_cluster::QUERY, ["nisshi"], 2)
            .await?
            .into_iter()
            .map(|topic| topic.name)
            .collect();
        // The query does not order its rows.
        names.sort();

        assert_eq!(vec!["t".to_owned(), "u".to_owned()], names);

        Ok(())
    }

    fn transactional_batch(
        producer: &ProducerIdResponse,
        sequence: i32,
    ) -> Result<deflated::Batch> {
        inflated::Batch::builder()
            .record(Record::builder().value(Some(Bytes::from_static(b"transactional"))))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(sequence)
            .build()
            .and_then(TryInto::try_into)
            .map_err(Into::into)
    }

    /// Opens a transaction for `producer` on `topition`, and produces one record in it.
    async fn open_transaction(
        engine: &ArcDynStorage,
        transaction_id: &str,
        producer: &ProducerIdResponse,
        topition: &Topition,
    ) -> Result<()> {
        use nisshi_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

        _ = engine
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

        _ = engine
            .produce(
                Some(transaction_id),
                topition,
                transactional_batch(producer, 0)?,
            )
            .await?;

        Ok(())
    }

    /// Only the transaction of the current epoch holds back the move, as in Kafka. A
    /// transaction left prepared on an older epoch does not.
    #[tokio::test]
    async fn init_producer_older_prepared_transaction_does_not_hold_back_the_move() -> Result<()> {
        let _guard = init_tracing()?;
        let db_file = "init-producer-older-prepared-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);
        let (engine, _) = engine_with_topic(db_file).await?;

        let producer = engine
            .init_producer(Some("txn"), 10_000, None, None)
            .await?;
        _ = engine
            .init_producer(Some("txn"), 10_000, None, None)
            .await?;

        execute_on(
            db_file,
            "update producer_epoch \
             set epoch = case when epoch = 0 then 100 else 32766 end \
             where producer = ?1",
            [producer.id],
        )
        .await?;
        execute_on(
            db_file,
            "update txn_detail set status = 'PREPARE_ABORT' \
             where producer_epoch = \
             (select id from producer_epoch where producer = ?1 and epoch = 100)",
            [producer.id],
        )
        .await?;

        let moved = engine
            .init_producer(Some("txn"), 10_000, None, None)
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
        let _guard = init_tracing()?;
        let db_file = "init-producer-open-transaction-exhausted-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);
        let (engine, topition) = engine_with_topic(db_file).await?;

        let producer = engine
            .init_producer(Some("txn"), 10_000, None, None)
            .await?;
        seed_producer_epoch(db_file, producer.id, 32766).await?;

        let exhausted = ProducerIdResponse {
            epoch: 32766,
            ..producer
        };
        open_transaction(&engine, "txn", &exhausted, &topition).await?;

        let moved = engine
            .init_producer(Some("txn"), 10_000, None, None)
            .await?;
        assert_eq!((ErrorCode::None, 0), (moved.error, moved.epoch));
        assert_ne!(producer.id, moved.id);

        let stage = engine.offset_stage(&topition).await?;
        assert_eq!(stage.high_watermark, stage.last_stable);

        Ok(())
    }

    /// When the abort of the open transaction waits behind an older open transaction on
    /// the same partition, the answer is CONCURRENT_TRANSACTIONS, and the producer is
    /// fenced, as in Kafka. The move happens once the older transaction ends, which
    /// finishes the abort.
    #[tokio::test]
    async fn init_producer_deferred_abort_at_the_exhausted_epoch_fences_then_moves() -> Result<()> {
        let _guard = init_tracing()?;
        let db_file = "init-producer-deferred-abort-exhausted-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);
        let (engine, topition) = engine_with_topic(db_file).await?;

        let older = engine
            .init_producer(Some("older"), 10_000, None, None)
            .await?;
        open_transaction(&engine, "older", &older, &topition).await?;

        let producer = engine
            .init_producer(Some("txn"), 10_000, None, None)
            .await?;
        seed_producer_epoch(db_file, producer.id, 32766).await?;

        let exhausted = ProducerIdResponse {
            epoch: 32766,
            ..producer
        };
        open_transaction(&engine, "txn", &exhausted, &topition).await?;

        assert_eq!(
            ProducerIdResponse::failed(ErrorCode::ConcurrentTransactions),
            engine
                .init_producer(Some("txn"), 10_000, None, None)
                .await?
        );

        assert_matches!(
            engine
                .produce(Some("txn"), &topition, transactional_batch(&exhausted, 1)?)
                .await,
            Err(Error::Api(ErrorCode::ProducerFenced))
        );

        assert_eq!(
            ErrorCode::None,
            engine.txn_end("older", older.id, older.epoch, true).await?
        );

        let moved = engine
            .init_producer(Some("txn"), 10_000, None, None)
            .await?;
        assert_eq!((ErrorCode::None, 0), (moved.error, moved.epoch));
        assert_ne!(producer.id, moved.id);

        let stage = engine.offset_stage(&topition).await?;
        assert_eq!(stage.high_watermark, stage.last_stable);

        Ok(())
    }

    /// Returns an engine on `db_file` with the groups `healthy` and `undecodable` stored, each with
    /// the returned detail, and a connection to the same file, separate from the engine's own.
    async fn engine_with_stored_groups(
        db_file: &'static str,
        cluster: &str,
    ) -> Result<(ArcDynStorage, Connection, GroupDetail)> {
        let engine = Engine::builder()
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
            .cluster(cluster.to_owned())
            .storage(Url::parse(&format!("file:///{db_file}"))?)
            .node(111)
            .build()
            .await?;

        engine
            .register_broker(BrokerRegistrationRequest {
                broker_id: 111,
                cluster_id: cluster.to_owned(),
                incarnation_id: Uuid::new_v4(),
                rack: None,
            })
            .await?;

        // `GroupDetail::default` records the current time as the group's inception, so a second
        // call returns a different detail. The helper therefore returns the detail that it stored.
        let stored = GroupDetail::default();

        for group_id in ["healthy", "undecodable"] {
            _ = engine
                .update_group(group_id, stored.clone(), None)
                .await
                .map_err(|err| Error::Message(format!("{err:?}")))?;
        }

        // the engine opened `file:///{db_file}` as `{cwd}/{db_file}`
        let connection = libsql::Builder::new_local(db_file)
            .build()
            .await?
            .connect()?;

        Ok((engine, connection, stored))
    }

    /// A DescribeGroups whose query fails must answer each group with `COORDINATOR_NOT_AVAILABLE`,
    /// which clients retry. A failed request instead closes the client's connection without a
    /// response.
    #[tokio::test]
    async fn describe_groups_answers_coordinator_not_available_when_the_query_fails() -> Result<()>
    {
        let _guard = init_tracing()?;

        let db_file = "describe-groups-query-fails-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);

        let (engine, connection, _) = engine_with_stored_groups(db_file, "nisshi").await?;

        // The test drops the table that the engine reads, so the engine's query fails. A local
        // SQLite file can't be made to refuse a connection, so the test makes the query fail
        // instead.
        _ = connection
            .execute("drop table consumer_group_detail", ())
            .await?;

        let group_ids = ["healthy", "undecodable"].map(String::from);

        assert_eq!(
            vec![
                NamedGroupDetail::error_code("healthy".into(), ErrorCode::CoordinatorNotAvailable),
                NamedGroupDetail::error_code(
                    "undecodable".into(),
                    ErrorCode::CoordinatorNotAvailable
                ),
            ],
            engine.describe_groups(Some(&group_ids), false).await?
        );

        Ok(())
    }

    /// A DescribeGroups must answer a stored group that can't be decoded with
    /// `UNKNOWN_SERVER_ERROR`, which clients report at once, and answer the other groups in the
    /// request as usual.
    #[tokio::test]
    async fn describe_groups_answers_unknown_server_error_for_an_undecodable_group() -> Result<()> {
        let _guard = init_tracing()?;

        let db_file = "describe-groups-undecodable-group-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);

        let cluster = "nisshi";
        let (engine, connection, stored) = engine_with_stored_groups(db_file, cluster).await?;

        // `42` is valid JSON, so the database accepts it, but it doesn't decode to a `GroupDetail`.
        let changed = connection
            .execute(
                "update consumer_group_detail set detail = '42'
                where consumer_group = (
                    select cg.id from consumer_group cg join cluster c on c.id = cg.cluster
                    where c.name = ?1 and cg.name = ?2
                )",
                (cluster, "undecodable"),
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
            engine.describe_groups(Some(&group_ids), false).await?
        );

        Ok(())
    }

    /// A ListGroups must answer a stored group whose detail isn't text with
    /// `UNKNOWN_SERVER_ERROR`, as it answers a detail that isn't JSON, and list the other groups
    /// as usual.
    #[tokio::test]
    async fn list_groups_answers_unknown_server_error_for_a_detail_that_is_not_text() -> Result<()>
    {
        let _guard = init_tracing()?;

        let db_file = "list-groups-detail-not-text-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);

        let cluster = "nisshi";
        let (engine, connection, stored) = engine_with_stored_groups(db_file, cluster).await?;

        // SQLite never converts a blob to text for the column's affinity, so the detail stays a
        // value that isn't text.
        let changed = connection
            .execute(
                "update consumer_group_detail set detail = x'7b7d'
                where consumer_group = (
                    select cg.id from consumer_group cg join cluster c on c.id = cg.cluster
                    where c.name = ?1 and cg.name = ?2
                )",
                (cluster, "undecodable"),
            )
            .await?;
        // The update must change the group's row. An update that changes no row leaves the group
        // readable, and the test then fails for that reason instead of testing the engine.
        assert_eq!(1, changed);

        let mut listed = engine.list_groups().await?;
        // The query doesn't order its rows.
        listed.sort_by(|left, right| left.name.cmp(&right.name));

        assert_eq!(
            vec![
                NamedGroupDetail::found("healthy".into(), stored),
                NamedGroupDetail::error_code("undecodable".into(), ErrorCode::UnknownServerError),
            ],
            listed
        );

        Ok(())
    }

    /// Returns an engine on `db_file` with a readable topic, and the topic `unreadable`, whose
    /// stored row can't be read. Returns the readable topic's partition and the ID of
    /// `unreadable`.
    async fn engine_with_unreadable_topic(
        db_file: &str,
    ) -> Result<(ArcDynStorage, Topition, Uuid)> {
        let (engine, healthy) = engine_with_topic(db_file).await?;

        let unreadable = engine
            .create_topic(
                CreatableTopic::default()
                    .name("unreadable".into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        // SQLite stores text that isn't a number as text in an `int` column, and the reader of
        // `partitions` then can't read it as an integer.
        execute_on(
            db_file,
            "update topic set partitions = 'x' where name = ?1",
            ["unreadable"],
        )
        .await?;

        Ok((engine, healthy, unreadable))
    }

    /// A topic whose stored row can't be read is a server error in Metadata, by name and by ID,
    /// not an unknown topic, so that a client does not try to create it again. A list of every
    /// topic leaves it out, and keeps the cluster's other topics.
    #[tokio::test]
    async fn metadata_answers_unknown_server_error_for_an_unreadable_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let db_file = "metadata-unreadable-topic-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);

        let (engine, healthy, unreadable) = engine_with_unreadable_topic(db_file).await?;

        let named = engine
            .metadata(Some(&[
                TopicId::from("unreadable"),
                TopicId::from(unreadable),
                TopicId::from(healthy.topic()),
            ]))
            .await?;

        assert_eq!(
            vec![
                (
                    Some("unreadable".to_owned()),
                    i16::from(ErrorCode::UnknownServerError)
                ),
                (None, i16::from(ErrorCode::UnknownServerError)),
                (Some(healthy.topic().to_owned()), i16::from(ErrorCode::None)),
            ],
            named
                .topics
                .iter()
                .map(|topic| (topic.name.clone(), topic.error_code))
                .collect::<Vec<_>>()
        );

        let listed = engine.metadata(None).await?;

        assert_eq!(
            vec![Some(healthy.topic().to_owned())],
            listed
                .topics
                .iter()
                .map(|topic| topic.name.clone())
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    /// A topic whose stored row can't be read is a server error in DescribeTopicPartitions, by
    /// name and by ID, and the response still describes the other topics.
    #[tokio::test]
    async fn describe_topic_partitions_answers_unknown_server_error_for_an_unreadable_topic()
    -> Result<()> {
        let _guard = init_tracing()?;

        let db_file = "describe-topic-partitions-unreadable-topic-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);

        let (engine, healthy, unreadable) = engine_with_unreadable_topic(db_file).await?;

        let described = engine
            .describe_topic_partitions(
                Some(&[
                    TopicId::from("unreadable"),
                    TopicId::from(unreadable),
                    TopicId::from(healthy.topic()),
                ]),
                i32::MAX,
                None,
            )
            .await?;

        assert_eq!(
            vec![
                (
                    Some("unreadable".to_owned()),
                    i16::from(ErrorCode::UnknownServerError)
                ),
                (None, i16::from(ErrorCode::UnknownServerError)),
                (Some(healthy.topic().to_owned()), i16::from(ErrorCode::None)),
            ],
            described
                .iter()
                .map(|topic| (topic.name.clone(), topic.error_code))
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    /// DeleteTopics deletes a topic whose stored row can't be read, because it reads only the
    /// topic's name, so that an operator can remove a corrupt topic without editing the database.
    #[tokio::test]
    async fn delete_topic_deletes_a_topic_with_an_unreadable_row() -> Result<()> {
        let _guard = init_tracing()?;

        let db_file = "delete-topic-unreadable-row-test.db";
        let _db_file_guard = DbFileGuard::new(db_file);

        let (engine, topition) = engine_with_topic(db_file).await?;

        // A UUID that doesn't parse makes the topic's row unreadable as a whole topic.
        execute_on(
            db_file,
            "update topic set uuid = 'not-a-uuid' where name = ?1",
            [topition.topic()],
        )
        .await?;

        assert_eq!(
            ErrorCode::None,
            engine
                .delete_topic(&TopicId::from(topition.topic()))
                .await?
        );

        // A deleted topic is unknown, where a topic that is still stored with its unreadable row
        // is a server error.
        assert_eq!(
            vec![i16::from(ErrorCode::UnknownTopicOrPartition)],
            engine
                .metadata(Some(&[TopicId::from(topition.topic())]))
                .await?
                .topics
                .iter()
                .map(|topic| topic.error_code)
                .collect::<Vec<_>>()
        );

        Ok(())
    }
}
