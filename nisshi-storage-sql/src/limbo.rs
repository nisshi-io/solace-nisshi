// Copyright (c) Solace Corporation
//
// Contributors outside Solace:
//   Erik Wickstrom
//   Or Hayat
//   Shane Kennedy
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
    env,
    fmt::Debug,
    marker::PhantomData,
    ops::Deref,
    result,
    str::FromStr as _,
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, SystemTime},
};

use crate::{
    database_file,
    sql::{
        self, Cache, ColumnErrorCount, Query, QueryRow, QueryRowCursor, ROWS_PER_FETCH,
        consumer_group_select, default_hash, driver::KafkaTimestamp, driver::sqlite::StoredTime,
        header_fetch, idempotent_epoch_check, idempotent_sequence_check, list_offset,
        producer_epoch_current_for_producer, producer_epoch_for_current_txn, producer_epoch_insert,
        producer_epochs_select, producer_insert, producer_select_for_update, record_fetch,
        remove_comments, result_columns::ResultColumns, topic_by_cluster,
        topic_configuration_select, topic_configuration_select_name, topic_configuration_upsert,
        topic_insert, topic_select_name, topic_select_uuid, topition_insert,
        txn_offset_commit_tp_select_pending, txn_produce_offset_select_offset_range,
        txn_produce_offset_select_overlapping_txn, txn_produce_partition_check,
        txn_produce_partition_select, txn_select_name, txn_select_produced_topitions,
        watermark_insert, watermark_select, watermark_select_for_write,
    },
};
use async_trait::async_trait;
use bytes::Bytes;
use nisshi_sans_io::{
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, ControlBatch, EndTransactionMarker,
    ErrorCode, IsolationLevel, NULL_TOPIC_ID, OpType, ScramMechanism,
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
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use nisshi_schema::{
    lake::{House, LakeHouse as _},
    redact_url,
};
use nisshi_storage::{
    BrokerRegistrationRequest, CommittedOffset, DELETE_RECORDS_INVALID_LOW_WATERMARK,
    DeleteRecordsConcurrency, Error, FetchedRecord, FetchedSender, GroupDetail, ListOffsetRequest,
    ListOffsetResponse, METER, MetadataResponse, NamedGroupDetail, OffsetCommitRequest,
    OffsetStage, ProduceEpoch, ProducerIdResponse, Result, ScramCredential, Storage, StoredEpochs,
    TopicConfiguration, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, TxnState, UpdateError, Version, check_claim, config::apply_op,
    delete_records_cutoff, epoch_is_exhausted, inflate, lake_skip, max_message_bytes,
    producer_claim,
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use rand::{rng, seq::SliceRandom as _};
use regex::Regex;
use tracing::{debug, error, info};
use turso::{
    Connection, Database, Row, params::IntoParams, transaction::Transaction,
    transaction::TransactionBehavior,
};
use url::Url;
use uuid::Uuid;

pub mod factory;

macro_rules! include_sql {
    ($e: expr) => {
        remove_comments(include_str!($e))
    };
}

static SQL_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_sqlite_duration")
        .with_unit("ms")
        .with_description("The SQL request latencies in milliseconds")
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

/// Turso storage engine
///
#[derive(Clone, Debug)]
pub struct Engine {
    cluster: String,
    node: i32,
    advertised_listener: Url,
    db: Arc<Mutex<Database>>,
    lake: Option<House>,
}

impl Engine {
    /// Applies every change in `resource` to its topic in one transaction, so
    /// that a failed change leaves the topic's configuration unchanged.
    async fn alter_topic(&self, resource: &AlterConfigsResource) -> Result<()> {
        let mut connection = self.connection().await?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;

        let topic = resource.resource_name.as_str();

        if tx
            .query(
                &sql_lookup(topic_select_name::QUERY.key())?,
                (self.cluster.as_str(), topic),
            )
            .await?
            .next()
            .await?
            .is_none()
        {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        for config in resource.configs.as_deref().unwrap_or_default() {
            let op = OpType::try_from(config.config_operation)?;
            let name = config.name.as_str();

            let current = if matches!(op, OpType::Append | OpType::Subtract) {
                self.query_first(
                    &tx,
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
                    _ = tx
                        .query(
                            &sql_lookup(topic_configuration_upsert::QUERY.key())?,
                            (self.cluster.as_str(), topic, name, value.as_str()),
                        )
                        .await?
                        .next()
                        .await?;
                }

                None => {
                    _ = tx
                        .execute(
                            &sql_lookup(sql::TOPIC_CONFIGURATION_DELETE)?,
                            (self.cluster.as_str(), topic, name),
                        )
                        .await?;
                }
            }
        }

        tx.commit().await.map_err(Into::into)
    }

    pub fn builder()
    -> Builder<PhantomData<String>, PhantomData<i32>, PhantomData<Url>, PhantomData<Url>> {
        Builder::default()
    }

    async fn connection(&self) -> Result<Connection> {
        let db = self.db.lock()?;
        db.connect().map_err(Into::into)
    }

    /// Returns the headers of the record at `offset` in `topition`.
    async fn record_headers(
        &self,
        c: &Connection,
        topition: &Topition,
        offset: i64,
    ) -> Result<Vec<Header>> {
        let mut rows = self
            .query_rows(
                c,
                header_fetch::QUERY,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                    offset,
                ),
            )
            .await?;

        let mut headers = vec![];

        while let Some(header_fetch::Row { key, value }) = rows.next().await? {
            headers.push(Header { key, value });
        }

        Ok(headers)
    }

    fn attributes_for_error(&self, sql: &str, error: &turso::Error) -> Vec<KeyValue> {
        debug!(sql, ?error);

        vec![
            KeyValue::new("sql", sql.to_owned()),
            KeyValue::new("cluster_id", self.cluster.clone()),
            KeyValue::new("kind", error_kind(error)),
        ]
    }

    async fn prepare_execute<P>(
        &self,
        connection: &Connection,
        sql: &str,
        params: P,
    ) -> result::Result<u64, turso::Error>
    where
        P: IntoParams,
    {
        // The params hold record data, so we log only the SQL.
        debug!(?connection, sql);

        let mut statement = connection.prepare(sql).await?;

        let execute_start = SystemTime::now();

        statement
            .execute(params)
            .await
            .inspect(|rows| {
                debug!(rows);

                SQL_DURATION.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );

                SQL_REQUESTS.add(
                    1,
                    &[
                        KeyValue::new("sql", sql.to_owned()),
                        KeyValue::new("cluster_id", self.cluster.clone()),
                    ],
                );
            })
            .inspect_err(|err| {
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })
    }

    async fn prepare_query_opt<P>(
        &self,
        connection: &Connection,
        sql: &str,
        params: P,
    ) -> result::Result<Option<Row>, turso::Error>
    where
        P: IntoParams,
    {
        // The params hold record data, so we log only the SQL.
        debug!(?connection, sql);

        let mut statement = connection.prepare(sql).await?;

        let execute_start = SystemTime::now();

        let mut rows = statement.query(params).await.inspect_err(|err| {
            SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
        })?;

        let row = rows.next().await.inspect_err(|err| {
            SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
        })?;

        let attributes = [
            KeyValue::new("sql", sql.to_owned()),
            KeyValue::new("cluster_id", self.cluster.clone()),
        ];

        SQL_DURATION.record(
            execute_start
                .elapsed()
                .map_or(0, |duration| duration.as_millis() as u64),
            &attributes,
        );

        SQL_REQUESTS.add(1, &attributes);

        Ok(row)
    }

    /// Runs `query` on `connection`, and returns its rows, each read as `T` when the caller
    /// reaches it.
    async fn query_rows<T: QueryRow>(
        &self,
        connection: &Connection,
        query: Query<T>,
        params: impl IntoParams,
    ) -> Result<QueryRowCursor<turso::Rows, T>> {
        let query = query.key();
        let sql = sql_lookup(query)?;
        let rows = connection.query(&sql, params).await?;
        QueryRowCursor::new(
            ResultColumns::new(query, &rows),
            rows,
            self.column_error_count(&sql),
        )
    }

    /// Returns the count of `sql`'s column errors in `SQL_ERROR`, with the labels that
    /// [`Self::attributes_for_error`] gives the query.
    fn column_error_count(&self, sql: &str) -> ColumnErrorCount {
        ColumnErrorCount::new(
            &SQL_ERROR,
            vec![
                KeyValue::new("sql", sql.to_owned()),
                KeyValue::new("cluster_id", self.cluster.clone()),
            ],
        )
    }

    /// Runs `query` on `connection`, and returns the first row of its result as `T`, or `None`
    /// when the result has no rows.
    ///
    /// The function drops the result before it returns, because SQLite can't commit while a
    /// `returning` statement is still open.
    async fn query_first<T: QueryRow>(
        &self,
        connection: &Connection,
        query: Query<T>,
        params: impl IntoParams,
    ) -> Result<Option<T>> {
        let mut rows = self
            .query_leading_rows(connection, query, params, 1)
            .await?;
        Ok(rows.pop())
    }

    /// Runs `query` on `connection`, and returns at most `limit` of its rows, in order, each read
    /// as `T`. It records the query's SQL metrics, and drops the result before it returns.
    async fn query_leading_rows<T: QueryRow>(
        &self,
        connection: &Connection,
        query: Query<T>,
        params: impl IntoParams,
        limit: usize,
    ) -> Result<Vec<T>> {
        let query = query.key();
        let sql = sql_lookup(query)?;

        // The params hold record data, so we log only the SQL.
        debug!(?connection, sql);

        let mut statement = connection
            .prepare(&sql)
            .await
            .inspect_err(|err| error!(?err, sql))?;

        let execute_start = SystemTime::now();

        let mut rows = statement.query(params).await.inspect_err(|err| {
            error!(?err, sql);
            SQL_ERROR.add(1, &self.attributes_for_error(&sql, err)[..]);
        })?;

        let reader = T::reader(&ResultColumns::new(query, &rows))
            .inspect_err(|err| self.column_error_count(&sql).record(err))?;

        let mut leading = Vec::with_capacity(limit);

        while leading.len() < limit
            && let Some(row) = rows
                .next()
                .await
                .inspect(|row| debug!(?row))
                .inspect_err(|err| {
                    error!(?err, sql);
                    SQL_ERROR.add(1, &self.attributes_for_error(&sql, err)[..]);
                })?
        {
            leading.push(
                T::read(&reader, &row)
                    .inspect_err(|err| self.column_error_count(&sql).record(err))?,
            );
        }

        let attributes = [
            KeyValue::new("sql", sql),
            KeyValue::new("cluster_id", self.cluster.clone()),
        ];

        SQL_DURATION.record(
            execute_start
                .elapsed()
                .map_or(0, |duration| duration.as_millis() as u64),
            &attributes,
        );

        SQL_REQUESTS.add(1, &attributes);

        Ok(leading)
    }

    /// Runs `query` on `connection`, and returns the first row of its result as `T`.
    ///
    /// Returns Turso's `QueryReturnedNoRows` error when the result has no rows.
    async fn query_one<T: QueryRow>(
        &self,
        connection: &Connection,
        query: Query<T>,
        params: impl IntoParams,
    ) -> Result<T> {
        match self.query_first(connection, query, params).await? {
            Some(row) => Ok(row),
            None => {
                error!(sql = query.key(), "query returned no rows");
                Err(turso::Error::QueryReturnedNoRows.into())
            }
        }
    }

    /// Returns the newest epoch of `producer`, or `None` when the producer does not exist or has
    /// no epoch.
    async fn producer_epoch_current(
        &self,
        connection: &Connection,
        producer: i64,
    ) -> Result<Option<i16>> {
        self.query_first(
            connection,
            producer_epoch_current_for_producer::QUERY,
            (self.cluster.as_str(), producer),
        )
        .await
        .map(|row| row.map(|row| row.epoch))
    }

    /// Returns the producer that owns `transaction_id`, the newest of its epochs with a
    /// transaction, and that transaction's status, or `None` when there is no such epoch.
    async fn producer_epoch_for_current_txn(
        &self,
        connection: &Connection,
        transaction_id: &str,
    ) -> Result<Option<producer_epoch_for_current_txn::Row>> {
        self.query_first(
            connection,
            producer_epoch_for_current_txn::QUERY,
            (self.cluster.as_str(), transaction_id),
        )
        .await
    }

    /// Adds a producer to the cluster, and returns its ID.
    async fn producer_insert(&self, connection: &Connection) -> Result<i64> {
        self.query_one(connection, producer_insert::QUERY, &[self.cluster.as_str()])
            .await
            .map(|row| row.id)
    }

    /// Adds the next epoch to `producer`, and returns it.
    async fn producer_epoch_insert(&self, connection: &Connection, producer: i64) -> Result<i16> {
        self.query_one(
            connection,
            producer_epoch_insert::QUERY,
            (self.cluster.as_str(), producer),
        )
        .await
        .map(|row| row.epoch)
    }

    async fn idempotent_message_check(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
        connection: &Connection,
    ) -> Result<()> {
        debug!(transaction_id, ?deflated);

        let epochs = self
            .query_first(
                connection,
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
                _ = self
                    .prepare_execute(
                        connection,
                        &sql_lookup(sql::PRODUCER_EPOCH_INSERT_GIVEN)?,
                        (
                            self.cluster.as_str(),
                            deflated.producer_id,
                            deflated.producer_epoch,
                        ),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;
            }

            let producer_select_for_update::Row { sequence } = self
                .query_one(
                    connection,
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
                self.prepare_execute(
                    connection,
                    &sql_lookup(sql::PRODUCER_DETAIL_INSERT)?,
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
    async fn watermark_select_for_write(
        &self,
        topition: &Topition,
        tx: &Connection,
    ) -> Result<watermark_select_for_write::Row> {
        debug!(?topition, ?tx);

        self.query_first(
            tx,
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
        connection: &Connection,
    ) -> Result<()> {
        let row = self
            .query_first(
                connection,
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

    async fn produce_in_tx<'conn>(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        inflated: inflated::Batch,
        tx: &Transaction<'conn>,
    ) -> Result<i64> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?topition, ?deflated);

        let topic = topition.topic();
        let partition = topition.partition();

        if deflated.is_idempotent() {
            self.idempotent_message_check(transaction_id, topition, &deflated, tx.deref())
                .await
                .inspect_err(|err| error!(?err))?;
        }

        if let Some(transaction_id) = transaction_id
            && deflated.is_transactional()
            && !deflated.is_control()
        {
            self.txn_produce_partition_check(transaction_id, topition, &deflated, tx.deref())
                .await?;
        }

        let watermarks = self.watermark_select_for_write(topition, tx).await?;
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

        let last_offset_delta = i64::from(inflated.last_offset_delta);

        for (delta, record) in inflated.records.iter().enumerate() {
            let delta = i64::try_from(delta)?;
            let offset = high.unwrap_or_default() + delta;
            let key = record.key.as_deref();
            let value = record.value.as_deref();

            debug!(?delta, ?offset);

            _ = self
                .prepare_execute(
                    tx,
                    &sql_lookup(sql::RECORD_INSERT)?,
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

            for (ordinal, header) in record.headers.iter().enumerate() {
                let ordinal = i32::try_from(ordinal)?;
                let key = header.key.as_deref();
                let value = header.value.as_deref();

                _ = self
                    .prepare_execute(
                        tx,
                        &sql_lookup(sql::HEADER_INSERT)?,
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
        }

        if let Some(transaction_id) = transaction_id
            && attributes.transaction
        {
            let offset_start = high.unwrap_or_default();
            let offset_end = high.map_or(last_offset_delta, |high| high + last_offset_delta);

            _ = self
                    .prepare_execute(tx,
                        &sql_lookup(sql::TXN_PRODUCE_OFFSET_INSERT)?,
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

        _ = self
            .prepare_execute(
                tx,
                &sql_lookup(sql::WATERMARK_UPDATE)?,
                (
                    self.cluster.as_str(),
                    topic,
                    partition,
                    low.unwrap_or_default(),
                    high.map_or(last_offset_delta + 1, |high| high + last_offset_delta + 1),
                ),
            )
            .await
            .inspect(|n| debug!(?n))
            .inspect_err(|err| error!(?err))?;

        if !attributes.control
            && let Some(ref lake) = self.lake
        {
            let config = self
                .describe_config(topition.topic(), ConfigResource::Topic, None)
                .await?;

            if TopicConfiguration::described(topition.topic(), config.clone())?.validates_schema() {
                lake.store(
                    topition.topic(),
                    topition.partition(),
                    high.unwrap_or_default(),
                    &inflated,
                    config,
                )
                .await
                .inspect(|store| debug!(?store))
                .inspect_err(|err| debug!(?err))?;
            } else {
                lake_skip::warn_once(topition.topic());
            }
        }

        Ok(high.unwrap_or_default())
    }

    async fn end_in_tx<'conn>(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
        tx: &Transaction<'conn>,
    ) -> Result<ErrorCode> {
        debug!(cluster = ?self.cluster, ?transaction_id, ?producer_id, ?producer_epoch, ?committed);

        let mut overlaps = vec![];

        let mut rows = self
            .query_rows(
                tx,
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
                .produce_in_tx(Some(transaction_id), &topition, batch, inflated, tx)
                .await?;

            debug!(offset, ?topition);

            let row = self
                .query_first(
                    tx,
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

                let mut rows = self
                    .query_rows(
                        tx,
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

                _ = tx
                    .execute(
                        &sql_lookup(sql::TXN_PRODUCE_OFFSET_DELETE_BY_TXN)?,
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                _ = tx
                    .execute(
                        &sql_lookup(sql::TXN_TOPITION_DELETE_BY_TXN)?,
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                if txn.status == TxnState::PrepareCommit {
                    _ = tx
                        .execute(
                            &sql_lookup(sql::CONSUMER_OFFSET_INSERT_FROM_TXN)?,
                            (
                                self.cluster.as_str(),
                                txn.name.as_str(),
                                txn.producer_id,
                                txn.producer_epoch,
                            ),
                        )
                        .await?;
                }

                _ = tx
                    .execute(
                        &sql_lookup(sql::TXN_OFFSET_COMMIT_TP_DELETE_BY_TXN)?,
                        (
                            self.cluster.as_str(),
                            txn.name.as_str(),
                            txn.producer_id,
                            txn.producer_epoch,
                        ),
                    )
                    .await?;

                _ = tx
                    .execute(
                        &sql_lookup(sql::TXN_OFFSET_COMMIT_DELETE_BY_TXN)?,
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

                _ = tx
                    .execute(
                        &sql_lookup(sql::TXN_STATUS_UPDATE)?,
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

            _ = tx
                .execute(
                    &sql_lookup(sql::TXN_STATUS_UPDATE)?,
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
}

#[derive(Clone, Default, Debug)]
pub struct Builder<C, N, L, D> {
    cluster: C,
    node: N,
    advertised_listener: L,
    storage: D,
    lake: Option<House>,
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
            lake: self.lake,
        }
    }

    pub(crate) fn node(self, node: i32) -> Builder<C, i32, L, D> {
        debug!(node);
        Builder {
            cluster: self.cluster,
            node,
            advertised_listener: self.advertised_listener,
            storage: self.storage,
            lake: self.lake,
        }
    }

    pub(crate) fn advertised_listener(self, advertised_listener: Url) -> Builder<C, N, Url, D> {
        debug!(%advertised_listener);
        Builder {
            cluster: self.cluster,
            node: self.node,
            advertised_listener,
            storage: self.storage,
            lake: self.lake,
        }
    }

    pub(crate) fn storage(self, storage: Url) -> Builder<C, N, L, Url> {
        debug!(storage = %redact_url(&storage));
        Builder {
            cluster: self.cluster,
            node: self.node,
            advertised_listener: self.advertised_listener,
            storage,
            lake: self.lake,
        }
    }

    pub(crate) fn lake(self, lake: Option<House>) -> Self {
        Self { lake, ..self }
    }
}

static DDL: LazyLock<Cache> = LazyLock::new(|| {
    let mapping = [
        ("010-cluster.sql", include_sql!("ddl/010-cluster.sql")),
        (
            "020-consumer-group.sql",
            include_sql!("limbo/ddl/020-consumer-group.sql"),
        ),
        (
            "020-producer.sql",
            include_sql!("limbo/ddl/020-producer.sql"),
        ),
        ("020-topic.sql", include_sql!("limbo/ddl/020-topic.sql")),
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

fn fix_parameters(sql: &str) -> Result<String> {
    Regex::new(r"\$(?<i>\d+)")
        .map(|re| re.replace_all(sql, "?$i").into_owned())
        .map_err(Into::into)
}

fn sql_lookup(key: &str) -> Result<String> {
    sql::SQL
        .get(key)
        .and_then(|sql| fix_parameters(sql).inspect(|sql| debug!(key, sql)))
}

impl Builder<String, i32, Url, Url> {
    pub(crate) async fn build(self) -> Result<Engine> {
        debug!(domain = self.storage.domain(), path = self.storage.path());

        let path = env::current_dir()
            .inspect(|current_dir| debug!(?current_dir))
            .map(|current_dir| database_file::path(current_dir, &self.storage))?;

        debug!(?path);

        let db = turso::Builder::new_local(
            path.to_str()
                .ok_or_else(|| Error::Message(format!("non-UTF-8 database path: {path:?}")))?,
        )
        .build()
        .await?;

        let connection = db.connect()?;

        for (name, ddl) in DDL.iter() {
            _ = connection
                .execute(ddl.as_str(), ())
                .await
                .inspect(|rows| debug!(name, rows))
                .inspect_err(|err| error!(name, ?err))
                .map_err(|err| Error::Message(format!("{name}: {err}")))?;
        }

        Ok(Engine {
            cluster: self.cluster,
            node: self.node,
            advertised_listener: self.advertised_listener,
            db: Arc::new(Mutex::new(db)),
            lake: self.lake,
        })
    }
}

/// Returns a mapper that turns a unique constraint failure into [`Error::Api`] with
/// `error_code`, and passes any other error through.
///
/// turso reports a unique or primary key failure as [`turso::Error::Constraint`] with a
/// message, and does not give its SQLite extended result code, so the mapper matches the
/// message's prefix.
fn unique_constraint<E: Into<Error>>(error_code: ErrorCode) -> impl Fn(E) -> Error {
    move |err| match err.into() {
        Error::Turso(turso_error) => match &*turso_error {
            turso::Error::Constraint(reason) if reason.starts_with("UNIQUE constraint failed") => {
                debug!(reason);
                Error::Api(error_code)
            }

            _ => Error::Turso(turso_error),
        },

        otherwise => otherwise,
    }
}

/// Returns the name of `error`'s variant, as the `kind` attribute of the SQL error metric.
fn error_kind(error: &turso::Error) -> &'static str {
    match error {
        turso::Error::ToSqlConversionFailure(_) => "to_sql_conversion_failure",
        turso::Error::QueryReturnedNoRows => "query_returned_no_rows",
        turso::Error::ConversionFailure(_) => "conversion_failure",
        turso::Error::Busy(_) => "busy",
        turso::Error::BusySnapshot(_) => "busy_snapshot",
        turso::Error::Interrupt(_) => "interrupt",
        turso::Error::Error(_) => "error",
        turso::Error::Misuse(_) => "misuse",
        turso::Error::Constraint(_) => "constraint",
        turso::Error::Readonly(_) => "readonly",
        turso::Error::DatabaseFull(_) => "database_full",
        turso::Error::NotAdb(_) => "not_a_db",
        turso::Error::Corrupt(_) => "corrupt",
        turso::Error::IoError(..) => "io_error",
        turso::Error::BatchStatementFailed { .. } => "batch_statement_failed",
        turso::Error::BatchRollbackFailed { .. } => "batch_rollback_failed",
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
        .and_then(serde_json::from_value::<GroupDetail>)
        .map_err(Error::from)
        .inspect(|current| debug!(?current))?;

    Ok((current, version))
}

impl Engine {
    /// Writes `deflated` to `topition` in one transaction, with `inflated` as its decoded records,
    /// or with the records that this decodes from `deflated` when the caller passed none. Both
    /// produce methods call this, so that each produce decodes and writes in one place.
    async fn produce_batch(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        inflated: Option<inflated::Batch>,
    ) -> Result<i64> {
        // The batch is inflated before the transaction starts, so the wait
        // for a decode permit and the decode do not hold a connection or the
        // database write lock.
        let inflated = match inflated {
            Some(inflated) => inflated,
            None => inflate(deflated.clone())
                .await
                .inspect_err(|err| debug!(?err))?,
        };

        debug!(cluster = self.cluster, transaction_id, ?topition, ?deflated);

        let mut connection = self.connection().await?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;

        let high = self
            .produce_in_tx(transaction_id, topition, deflated, inflated, &tx)
            .await?;

        tx.commit().await.map_err(Into::into).and(Ok(high))
    }
}

#[async_trait]
impl Storage for Engine {
    /// A Turso connection has no busy handler, so a write that finds another
    /// connection's write lock held fails at once with `Busy`. The factory puts a
    /// `nisshi_storage::SemaphoreProxy` in front of the engine, which runs one write at a time,
    /// so a second call at once gains nothing.
    fn delete_records_concurrency(&self) -> DeleteRecordsConcurrency {
        DeleteRecordsConcurrency::serial()
    }

    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        debug!(?broker_registration);

        let connection = self.connection().await?;

        self.prepare_execute(
            &connection,
            &sql_lookup(sql::REGISTER_BROKER)?,
            &[broker_registration.cluster_id],
        )
        .await
        .map_err(Into::into)
        .and(Ok(()))
    }

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

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        debug!(cluster = self.cluster, ?topic, validate_only);

        let mut connection = self.connection().await.inspect_err(|err| error!(?err))?;

        let tx = connection.transaction().await?;

        let uuid = {
            let uuid = Uuid::new_v4();

            let parameters = (
                self.cluster.as_str(),
                topic.name.as_str(),
                uuid.to_string(),
                topic.num_partitions,
                (topic.replication_factor as i32),
            );

            self.query_one(&tx, topic_insert::QUERY, parameters.clone())
                .await
                .inspect_err(|err| error!(?err))
                .map_err(unique_constraint(ErrorCode::TopicAlreadyExists))
                .inspect(|row| debug!(?parameters, ?row))
                .map(|row| row.uuid)
        }
        .inspect(|uuid| debug!(?uuid))
        .inspect_err(|err| error!(?err))?;

        for partition in 0..topic.num_partitions {
            let params = (self.cluster.as_str(), topic.name.as_str(), partition);

            let topition = self.query_one(&tx, topition_insert::QUERY, params).await?;
            debug!(?topition);

            let watermark = self.query_one(&tx, watermark_insert::QUERY, params).await?;
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

                let id = self
                    .query_one(&tx, topic_configuration_upsert::QUERY, params)
                    .await
                    .inspect_err(|err| error!(?err, ?config))?;
                debug!(?id, ?config);
            }
        }

        tx.commit().await.map_err(Into::into).and(Ok(uuid))
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        debug!(?topics);

        let mut connection = self.connection().await?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;

        let mut responses = vec![];

        for topic in topics {
            let mut partition_responses = vec![];

            if let Some(ref partitions) = topic.partitions {
                for partition in partitions {
                    let topition = Topition::new(topic.name.as_str(), partition.partition_index);

                    let (error_code, low_watermark) = match self
                        .watermark_select_for_write(&topition, &tx)
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
                                        _ = self
                                            .prepare_execute(
                                                &tx,
                                                &sql_lookup(sql::RECORD_DELETE_BY_OFFSET)?,
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

                                    _ = self
                                        .prepare_execute(
                                            &tx,
                                            &sql_lookup(sql::WATERMARK_UPDATE)?,
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

        tx.commit().await.inspect_err(|err| error!(?err))?;

        Ok(responses)
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        debug!(cluster = self.cluster, ?topic);

        let mut connection = self.connection().await?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;

        let topic_name = match topic {
            TopicId::Id(id) => self
                .query_first(
                    &tx,
                    topic_select_uuid::NAME_QUERY,
                    (self.cluster.as_str(), id.to_string().as_str()),
                )
                .await?
                .map(|row| row.name),

            TopicId::Name(name) => self
                .query_first(
                    &tx,
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
            let rows = self
                .prepare_execute(&tx, &sql_lookup(sql)?, (self.cluster.as_str(), topic_name))
                .await?;

            debug!(?topic, rows, sql)
        }

        _ = self
            .prepare_execute(
                &tx,
                &sql_lookup(sql::TOPIC_DELETE_BY)?,
                (self.cluster.as_str(), topic_name),
            )
            .await?;

        tx.commit()
            .await
            .map_err(Into::into)
            .and(Ok(ErrorCode::None))
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
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
        debug!(?topition, offset, min_bytes, max_bytes, ?isolation_level);

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

        let mut records = self
            .query_rows(
                &c,
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
            .inspect_err(|err| error!(?err))?;

        let mut data = fetched_sender.send_offsets(offset_stage).await?;

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

            let headers = self
                .record_headers(&c, topition, offset)
                .await?
                .into_iter()
                .map(Into::into)
                .collect();

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

        Ok(())
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        debug!(cluster = self.cluster, ?topition);
        let c = self.connection().await?;

        let watermark_select::Row { low, high, stable } = self
            .query_one(
                &c,
                watermark_select::QUERY,
                (
                    self.cluster.as_str(),
                    topition.topic(),
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
    }

    async fn offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        debug!(cluster = self.cluster, ?group, ?retention, ?offsets);
        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        let mut cg_inserted = false;

        let mut responses = vec![];

        for (topition, offset) in offsets {
            debug!(?topition, ?offset);

            let mut rows = tx
                .query(
                    &sql_lookup(sql::TOPITION_SELECT)?,
                    (
                        self.cluster.as_str(),
                        topition.topic(),
                        topition.partition(),
                    ),
                )
                .await
                .inspect_err(|err| error!(?err))?;

            if rows.next().await.inspect_err(|err| error!(?err))?.is_some() {
                if !cg_inserted {
                    let rows = self
                        .prepare_execute(
                            &tx,
                            &sql_lookup(sql::CONSUMER_GROUP_INSERT)?,
                            (self.cluster.as_str(), group),
                        )
                        .await?;
                    debug!(rows);

                    cg_inserted = true;
                }

                let rows = self
                    .prepare_execute(
                        &tx,
                        &sql_lookup(sql::CONSUMER_OFFSET_INSERT)?,
                        (
                            self.cluster.as_str(),
                            topition.topic(),
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

        tx.commit().await.inspect_err(|err| error!(?err))?;

        Ok(responses)
    }

    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        debug!(group_id);

        let mut results = BTreeMap::new();

        let c = self.connection().await?;

        let mut rows = self
            .query_rows(
                &c,
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

        Ok(results)
    }

    async fn pending_txn_offsets(&self, group_id: &str) -> Result<BTreeSet<Topition>> {
        debug!(cluster = self.cluster, group_id);

        let c = self.connection().await?;

        let mut rows = self
            .query_rows(
                &c,
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

        Ok(pending)
    }

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
            let mut rows = self
                .query_rows(
                    &c,
                    sql::consumer_offset_select::QUERY,
                    (
                        self.cluster.as_str(),
                        group_id,
                        topic.topic(),
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

        Ok(offsets)
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffsetRequest)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        debug!(cluster = self.cluster, ?isolation_level, ?offsets);
        let c = self.connection().await?;

        let mut responses = vec![];

        for (topition, offset_type) in offsets {
            let query = match (offset_type, isolation_level) {
                (ListOffsetRequest::Earliest, _) => list_offset::EARLIEST_QUERY,
                (ListOffsetRequest::Latest, IsolationLevel::ReadCommitted) => {
                    list_offset::LATEST_COMMITTED_QUERY
                }
                (ListOffsetRequest::Latest, IsolationLevel::ReadUncommitted) => {
                    list_offset::LATEST_UNCOMMITTED_QUERY
                }
                (ListOffsetRequest::Timestamp(_), _) => list_offset::LATEST_TIMESTAMP_QUERY,
            };

            debug!(?query);

            let list_offset = match offset_type {
                ListOffsetRequest::Earliest | ListOffsetRequest::Latest => self
                    .query_first(
                        &c,
                        query,
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                        ),
                    )
                    .await
                    .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition)),

                ListOffsetRequest::Timestamp(timestamp) => self
                    .query_first(
                        &c,
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
                        ListOffsetRequest::Earliest | ListOffsetRequest::Latest => {
                            ErrorCode::UnknownTopicOrPartition
                        }
                        ListOffsetRequest::Timestamp(_) => ErrorCode::None,
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
                        ListOffsetRequest::Timestamp(_) => {
                            timestamp.map(KafkaTimestamp::system_time).transpose()?
                        }
                        ListOffsetRequest::Earliest | ListOffsetRequest::Latest => None,
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

        Ok(responses).inspect(|r| debug!(?r))
    }

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
            Some(topics) => {
                let mut responses = vec![];

                for topic in topics {
                    responses.push(match topic {
                        TopicId::Name(name) => {
                            let mut rows = self
                                .query_rows(
                                    &c,
                                    topic_select_name::QUERY,
                                    (self.cluster.as_str(), name.as_str()),
                                )
                                .await?;

                            match rows.next().await {
                                Ok(Some(topic_select_name::Row {
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
                                        -1,
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
                                    .name(Some(name.into()))
                                    .topic_id(Some(NULL_TOPIC_ID))
                                    .is_internal(Some(false))
                                    .partitions(Some([].into()))
                                    .topic_authorized_operations(Some(-2147483648)),

                                // A row that can't be read fails the whole request, not only this topic.
                                Err(reason) if sql::is_column_error(&reason) => return Err(reason),

                                Err(reason) => {
                                    error!(?reason, ?topic);
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
                            let mut rows = self
                                .query_rows(
                                    &c,
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
                                        -1,
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
                                // A row that can't be read fails the whole request, not only this topic.
                                Err(reason) if sql::is_column_error(&reason) => return Err(reason),

                                Err(reason) => {
                                    error!(?reason, ?topic, topic_id = %id, "topic ID query failed, answering UNKNOWN_TOPIC_OR_PARTITION");
                                    MetadataResponseTopic::default()
                                        .error_code(
                                            nisshi_storage::topic_id_query_error_code(&reason)
                                                .into(),
                                        )
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

            None => {
                let mut responses = vec![];

                let mut rows = self
                    .query_rows(&c, topic_by_cluster::QUERY, &[self.cluster.as_str()])
                    .await?;

                while let Some(topic_by_cluster::Row {
                    uuid,
                    name,
                    is_internal,
                    partitions,
                    replication_factor,
                }) = rows.next().await?
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
                        -1,
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
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        debug!(cluster = self.cluster, name, ?resource, ?keys);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let mut rows = c
            .query(
                &sql_lookup(sql::TOPIC_SELECT)?,
                (self.cluster.as_str(), name),
            )
            .await?;

        if rows.next().await?.is_some() {
            let mut rows = self
                .query_rows(
                    &c,
                    topic_configuration_select::QUERY,
                    (self.cluster.as_str(), name),
                )
                .await?;

            let mut configs = vec![];

            while let Some(topic_configuration_select::Row { name, value }) = rows.next().await? {
                configs.push(
                    DescribeConfigsResourceResult::default()
                        .name(name)
                        .value(value)
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

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        debug!(?topics, partition_limit, ?cursor);
        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let mut responses =
            Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or_default());

        for topic in topics.unwrap_or_default() {
            responses.push(match topic {
                TopicId::Name(name) => {
                    let selected = self
                        .query_first(
                            &c,
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
                                                .leader_epoch(-1)
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

                        // A row that can't be read fails the whole request, not only this topic.
                        Err(reason) if sql::is_column_error(&reason) => return Err(reason),

                        Err(reason) => {
                            error!(?reason, ?topic);
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
                    let selected = self
                        .query_first(
                            &c,
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
                                                .leader_epoch(-1)
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

                        // A row that can't be read fails the whole request, not only this topic.
                        Err(reason) if sql::is_column_error(&reason) => return Err(reason),

                        Err(reason) => {
                            error!(?reason, ?topic);
                            DescribeTopicPartitionsResponseTopic::default()
                                .error_code(
                                    nisshi_storage::topic_id_query_error_code(&reason).into(),
                                )
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

    async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let mut listed_groups = vec![];

        let mut rows = self
            .query_rows(&c, consumer_group_select::QUERY, &[self.cluster.as_str()])
            .await?;

        while let Some(consumer_group_select::Row { name, detail }) = rows.next().await? {
            let stored =
                detail.map(|detail| detail.and_then(serde_json::from_value::<GroupDetail>));

            listed_groups.push(NamedGroupDetail::decoded(name, stored));
        }

        Ok(listed_groups)
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        debug!(?group_ids);
        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            let c = self.connection().await?;

            let mut consumer_offset = c
                .prepare(&sql_lookup(sql::CONSUMER_OFFSET_DELETE_BY_CG)?)
                .await
                .inspect_err(|err| error!(?err))?;

            let mut group_detail = c
                .prepare(&sql_lookup(sql::CONSUMER_GROUP_DETAIL_DELETE_BY_CG)?)
                .await
                .inspect_err(|err| error!(?err))?;

            let mut group = c
                .prepare(&sql_lookup(sql::CONSUMER_GROUP_DELETE)?)
                .await
                .inspect_err(|err| error!(?err))?;

            for group_id in group_ids {
                _ = consumer_offset
                    .execute((self.cluster.as_str(), group_id.as_str()))
                    .await
                    .inspect_err(|err| error!(?err))?;

                _ = group_detail
                    .execute((self.cluster.as_str(), group_id.as_str()))
                    .await
                    .inspect_err(|err| error!(?err))?;

                let rows = group
                    .execute((self.cluster.as_str(), group_id.as_str()))
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

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        debug!(?group_ids, include_authorized_operations);

        let mut results = vec![];
        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                let row = self
                    .query_first(
                        &c,
                        sql::consumer_group_select_by_name::QUERY,
                        (self.cluster.as_str(), group_id.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err, group_id))?;

                if let Some(row) = row {
                    // A group row with no detail row (e.g. a group that only ever
                    // committed offsets, never a JoinGroup) joins to a NULL
                    // `detail` column here: that group exists and is empty, not
                    // an error.
                    let stored = row.detail.map(|detail| {
                        detail
                            .and_then(serde_json::from_value::<GroupDetail>)
                            .map_err(Error::from)
                            .inspect(|current| debug!(?current))
                    });

                    results.push(NamedGroupDetail::decoded(group_id.into(), stored));
                } else {
                    results.push(NamedGroupDetail::error_code(
                        group_id.into(),
                        ErrorCode::GroupIdNotFound,
                    ));
                }
            }
        }

        Ok(results)
    }

    async fn group_with_version(&self, group_id: &str) -> Result<Option<(GroupDetail, Version)>> {
        debug!(cluster = self.cluster, group_id);

        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        self.query_first(
            &c,
            sql::consumer_group_detail::QUERY,
            (group_id, self.cluster.as_str()),
        )
        .await
        .inspect_err(|err| error!(?err, group_id))?
        .map(group_detail_with_version)
        .transpose()
        .inspect_err(|err| error!(?err, group_id, "the stored group can't be decoded"))
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        debug!(cluster = self.cluster, group_id, ?detail, ?version);
        debug!(cluster = self.cluster, group_id, ?detail, ?version);

        let mut c = self.connection().await?;
        let tx = c.transaction().await?;

        _ = self
            .prepare_execute(
                &tx,
                &sql_lookup(sql::CONSUMER_GROUP_INSERT)?,
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

        let detail = serde_json::to_value(detail).inspect(|detail| debug!(?detail))?;

        let row = self
            .query_first(
                &tx,
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
            let (current, version) = self
                .query_one(
                    &tx,
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

        tx.commit().await.inspect_err(|err| error!(?err))?;

        debug!(?outcome);

        outcome
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        debug!(
            cluster = self.cluster,
            transaction_id, transaction_timeout_ms, producer_id, producer_epoch
        );
        let claim = match producer_claim(producer_id, producer_epoch) {
            Ok(claim) => claim,
            Err(error) => return Ok(ProducerIdResponse::failed(error)),
        };

        match transaction_id {
            Some(transaction_id) => {
                let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
                let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

                let mut rolled_back = false;

                if let Some(producer_epoch_for_current_txn::Row {
                    producer: id,
                    epoch,
                    status,
                }) = self
                    .producer_epoch_for_current_txn(&tx, transaction_id)
                    .await
                    .inspect_err(|err| error!(?err, transaction_id))?
                {
                    let status = status
                        .map_or(Ok(None), |status| {
                            TxnState::from_str(status.as_str()).map(Some)
                        })
                        .inspect_err(|err| error!(?err))?;

                    debug!(transaction_id, id, epoch, ?status, ?claim);

                    if let Some(claim) = claim {
                        let error = check_claim((id, epoch), claim);

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
                            .end_in_tx(transaction_id, id, epoch, false, &tx)
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

                let txn_producer = self
                    .query_first(
                        &tx,
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
                    let newest = self
                        .producer_epoch_current(&tx, producer)
                        .await
                        .inspect_err(|err| error!(?err, transaction_id, producer))?;

                    if newest.is_some_and(epoch_is_exhausted) {
                        // The status after the rollback above, if it ran.
                        let status = self
                            .producer_epoch_for_current_txn(&tx, transaction_id)
                            .await
                            .and_then(|row| {
                                row.and_then(|row| row.status)
                                    .map(|status| TxnState::from_str(status.as_str()))
                                    .transpose()
                            })
                            .inspect_err(|err| error!(?err))?;

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
                                _ = self
                                    .prepare_query_opt(
                                        &tx,
                                        &sql_lookup(producer_epoch_insert::QUERY.key())?,
                                        (self.cluster.as_str(), producer),
                                    )
                                    .await
                                    .inspect_err(|err| error!(self.cluster, producer, ?err))?;
                            }

                            let error = match tx.commit().await.inspect_err(|err| {
                                error!(?err, cluster = self.cluster, transaction_id, producer)
                            }) {
                                Ok(()) => ErrorCode::ConcurrentTransactions,
                                Err(err) => return Err(Error::from(err)),
                            };

                            return Ok(ProducerIdResponse::failed(error));
                        }

                        let previous = producer;

                        let producer = self
                            .producer_insert(&tx)
                            .await
                            .inspect_err(|err| error!(?err))?;

                        let epoch = self
                            .producer_epoch_insert(&tx, producer)
                            .await
                            .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                        // A produce with an epoch below the producer's newest is fenced, so
                        // epoch i16::MAX fences the previous producer ID. A producer at epoch
                        // i16::MAX gets no new epoch, and is fenced already.
                        _ = self
                            .prepare_query_opt(
                                &tx,
                                &sql_lookup(producer_epoch_insert::QUERY.key())?,
                                (self.cluster.as_str(), previous),
                            )
                            .await
                            .inspect_err(|err| error!(self.cluster, previous, ?err))?;

                        let updated = self
                            .prepare_execute(
                                &tx,
                                &sql_lookup(sql::TXN_UPDATE_PRODUCER)?,
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
                        let epoch = self
                            .producer_epoch_insert(&tx, producer)
                            .await
                            .inspect(|epoch| debug!(epoch))
                            .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                        (producer, epoch)
                    }
                } else {
                    let producer = self
                        .producer_insert(&tx)
                        .await
                        .inspect_err(|err| error!(?err))?;

                    let epoch = self
                        .producer_epoch_insert(&tx, producer)
                        .await
                        .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                    assert_eq!(
                        1,
                        self.prepare_execute(
                            &tx,
                            &sql_lookup(sql::TXN_INSERT)?,
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
                    self.prepare_execute(
                        &tx,
                        &sql_lookup(sql::TXN_DETAIL_INSERT)?,
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
            }

            None => {
                let mut connection = self.connection().await.inspect_err(|err| error!(?err))?;
                let tx = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .await?;

                let mut rows = self
                    .query_rows(&tx, producer_insert::QUERY, &[self.cluster.as_str()])
                    .await?;

                if let Some(producer_insert::Row { id: producer }) =
                    rows.next().await.inspect_err(|err| error!(?err))?
                {
                    debug!(producer);

                    while let Some(row) = rows
                        .next()
                        .await
                        .inspect(|row| debug!(?row))
                        .inspect_err(|err| error!(?err))?
                    {
                        debug!(?row)
                    }

                    let mut rows = self
                        .query_rows(
                            &tx,
                            producer_epoch_insert::QUERY,
                            (self.cluster.as_str(), producer),
                        )
                        .await
                        .inspect_err(|err| error!(?err, cluster = self.cluster, producer))?;

                    if let Some(producer_epoch_insert::Row { epoch }) = rows.next().await? {
                        debug!(epoch);

                        while let Some(row) = rows.next().await? {
                            debug!(?row)
                        }

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
                        .inspect(|response| debug!(?response))
                    } else {
                        Ok(ProducerIdResponse {
                            error: ErrorCode::UnknownServerError,
                            id: producer,
                            epoch: -1,
                        })
                        .inspect(|response| debug!(?response))
                    }
                } else {
                    Ok(ProducerIdResponse {
                        error: ErrorCode::UnknownServerError,
                        id: -1,
                        epoch: -1,
                    })
                    .inspect(|response| debug!(?response))
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
        debug!(
            cluster = self.cluster,
            transaction_id, producer_id, producer_epoch, group_id
        );

        Ok(ErrorCode::None)
    }

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
                            .prepare_execute(
                                &tx,
                                &sql_lookup(sql::TXN_TOPITION_INSERT)?,
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

                _ = self
                    .prepare_execute(
                        &tx,
                        &sql_lookup(sql::TXN_DETAIL_UPDATE_STARTED_AT)?,
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

                tx.commit().await?;

                Ok(TxnAddPartitionsResponse::VersionZeroToThree(results))
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
        debug!(cluster = self.cluster, ?offsets);

        let mut c = self.connection().await.inspect_err(|err| error!(?err))?;
        let tx = c.transaction().await.inspect_err(|err| error!(?err))?;

        let (producer_id, producer_epoch) = if let Some(row) = self
            .producer_epoch_for_current_txn(&tx, offsets.transaction_id.as_str())
            .await
            .inspect_err(|err| error!(?err))?
        {
            (Some(row.producer), Some(row.epoch))
        } else {
            (None, None)
        };

        _ = self
            .prepare_execute(
                &tx,
                &sql_lookup(sql::CONSUMER_GROUP_INSERT)?,
                (self.cluster.as_str(), offsets.group_id.as_str()),
            )
            .await?;

        debug!(?producer_id, ?producer_epoch);

        _ = self
            .prepare_execute(
                &tx,
                &sql_lookup(sql::TXN_OFFSET_COMMIT_INSERT)?,
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
                        _ = self
                            .prepare_execute(
                                &tx,
                                &sql_lookup(sql::TXN_OFFSET_COMMIT_TP_INSERT)?,
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

        tx.commit().await?;

        Ok(topics)
    }

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
            .end_in_tx(transaction_id, producer_id, producer_epoch, committed, &tx)
            .await?;

        tx.commit().await?;

        Ok(error_code)
    }

    async fn maintain(&self, _now: SystemTime) -> Result<()> {
        Ok(())
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

    async fn cluster_id(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    async fn node(&self) -> Result<i32> {
        Ok(self.node)
    }

    async fn advertised_listener(&self) -> Result<Url> {
        Ok(self.advertised_listener.clone())
    }

    /// Turso keeps no SCRAM credentials, so it refuses the change instead of panicking.
    async fn delete_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<()> {
        Err(Error::UserCredentialsUnsupported {
            engine: factory::NAME.into(),
        })
    }

    /// See [`Self::delete_user_scram_credential`].
    async fn upsert_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> Result<()> {
        Err(Error::UserCredentialsUnsupported {
            engine: factory::NAME.into(),
        })
    }

    /// No user has a credential here: [`Self::upsert_user_scram_credential`] stores none.
    async fn user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        Ok(None)
    }

    async fn ping(&self) -> Result<()> {
        let c = self.connection().await?;
        let _ = c.query(&sql_lookup(sql::PING)?, ()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use tempfile::{TempDir, tempdir, tempdir_in};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;
    use turso::Value;

    use super::*;

    /// Each query that Turso runs returns every column that its module's `RowReader` reads, once.
    /// The broker reads a query's rows only on the paths that run it, so this test finds a query
    /// that doesn't match its row before a rare path does.
    #[tokio::test]
    async fn every_row_query_returns_the_columns_of_its_row() -> Result<()> {
        use crate::sql::{
            nisshi_schema_version_select, row_check::RowCheck, scram_credential_select,
            txn_detail_select_status_for_update, txn_detail_select_timed_out,
            watermark_select_for_write,
        };

        let directory = tempdir()?;
        let path = directory.path().join("nisshi.db");
        let db = turso::Builder::new_local(
            path.to_str()
                .ok_or_else(|| Error::Message(format!("non-UTF-8 database path: {path:?}")))?,
        )
        .build()
        .await?;
        let connection = db.connect()?;

        let mut failures = Vec::new();
        for (name, ddl) in DDL.iter() {
            if let Err(error) = connection.execute(ddl.as_str(), ()).await {
                failures.push(format!("{name}: {error:?}"));
            }
        }

        // Turso doesn't run these queries. The first three use PostgreSQL's syntax, and Turso runs
        // `watermark_select_for_write::LITE_QUERY` instead of `watermark_select_for_write::QUERY`.
        // The other two read tables that Turso's schema doesn't have.
        let skipped = [
            txn_detail_select_status_for_update::QUERY.key(),
            txn_detail_select_timed_out::QUERY.key(),
            watermark_select_for_write::QUERY.key(),
            nisshi_schema_version_select::QUERY.key(),
            scram_credential_select::QUERY.key(),
        ];
        let checks = RowCheck::in_folders(&["sql", "lite"], &skipped);

        for check in &checks {
            let checked = match connection.prepare(sql::SQL.get(check.query())?).await {
                Ok(statement) => {
                    check.check_columns(&ResultColumns::new(check.query(), &statement))
                }
                Err(error) => Err(error.into()),
            };

            if let Err(error) = checked {
                failures.push(format!("{}: {error:?}", check.query()));
            }
        }

        assert!(failures.is_empty(), "{failures:#?}");
        Ok(())
    }

    /// A unique constraint failure becomes the given error code, and any other constraint
    /// failure stays a turso error.
    #[tokio::test]
    async fn unique_constraint_answers_its_error_code() -> Result<()> {
        let _guard = init_tracing()?;

        let db = turso::Builder::new_local(":memory:").build().await?;
        let connection = db.connect()?;

        _ = connection
            .execute(
                "create table xyz (
                    cluster text not null,
                    name text not null,
                    unique (cluster, name)
                )",
                (),
            )
            .await?;

        let insert = "insert into xyz (cluster, name) values (?1, ?2) returning name";

        assert!(insert_returning(&connection, insert, ("nisshi", "abc")).await?);

        assert_matches!(
            insert_returning(&connection, insert, ("nisshi", "abc"))
                .await
                .map_err(unique_constraint(ErrorCode::TopicAlreadyExists)),
            Err(Error::Api(ErrorCode::TopicAlreadyExists))
        );

        assert_matches!(
            insert_returning(&connection, insert, ("nisshi", Value::Null))
                .await
                .map_err(unique_constraint(ErrorCode::TopicAlreadyExists)),
            Err(Error::Turso(_))
        );

        Ok(())
    }

    /// Turso accepts the statement shape of `topic_insert.sql`: an `insert ... select ...
    /// returning`.
    #[tokio::test]
    async fn insert_select_with_returning() -> Result<()> {
        let _guard = init_tracing()?;

        let db = turso::Builder::new_local(":memory:").build().await?;
        let connection = db.connect()?;

        _ = connection
            .execute(
                "create table pqr (
                    id integer primary key autoincrement,
                    name text not null
                )",
                (),
            )
            .await?;
        _ = connection
            .execute("insert into pqr (name) values (?1)", &["fgh"])
            .await?;
        _ = connection
            .execute(
                "create table xyz (
                    id integer primary key autoincrement,
                    pqr integer not null references pqr (id),
                    name text not null
                )",
                (),
            )
            .await?;

        let mut rows = connection
            .query(
                "insert into xyz (pqr, name)
                select pqr.id, ?2
                from pqr
                where pqr.name = ?1
                returning xyz.name",
                &["fgh", "abc"],
            )
            .await?;

        let row = rows.next().await?.unwrap();
        assert_eq!(Some("abc"), row.get_value(0)?.as_text().map(String::as_str));

        Ok(())
    }

    /// Turso accepts the statement shape of `producer_epoch_insert.sql`: a common table
    /// expression that feeds an `insert ... select ... returning`.
    #[tokio::test]
    async fn cte_insert_with_returning() -> Result<()> {
        let _guard = init_tracing()?;

        let db = turso::Builder::new_local(":memory:").build().await?;
        let connection = db.connect()?;

        _ = connection
            .execute(
                "create table pqr (
                    id integer primary key autoincrement,
                    name text not null
                )",
                (),
            )
            .await?;
        _ = connection
            .execute("insert into pqr (name) values (?1)", &["fgh"])
            .await?;
        _ = connection
            .execute(
                "create table xyz (
                    id integer primary key autoincrement,
                    pqr integer not null references pqr (id),
                    name text not null
                )",
                (),
            )
            .await?;

        let mut rows = connection
            .query(
                "with qwe as (
                    select pqr.id, ?2
                    from pqr
                    where pqr.name = ?1
                )
                insert into xyz (pqr, name)
                select * from qwe
                returning xyz.name",
                &["fgh", "abc"],
            )
            .await?;

        let row = rows.next().await?.unwrap();
        assert_eq!(Some("abc"), row.get_value(0)?.as_text().map(String::as_str));

        Ok(())
    }

    /// A fetch statement returns at most `ROWS_PER_FETCH` rows, and a read from the last offset
    /// that it returned gets the rest, with no gap and no repeat. Each record has a header of 99
    /// bytes, so the statement runs its header probe for the rows that it keeps, and costs a
    /// record 110 bytes with its key and the 7 of every record. The statements run here, on the
    /// Turso engine, because it rejects some SQL that SQLite takes, and a row limit inside a
    /// derived table is one such construct.
    #[tokio::test]
    async fn fetch_reads_a_bounded_number_of_rows_and_resumes_after_them() -> Result<()> {
        const RECORDS: i64 = ROWS_PER_FETCH + 50;

        let _guard = init_tracing()?;

        let db = turso::Builder::new_local(":memory:").build().await?;
        let connection = db.connect()?;

        create_fetch_tables(&connection).await?;
        insert_numbered_records(&connection, RECORDS).await?;

        for (statement, key) in FETCH_STATEMENTS {
            let fetch_from = |offset, max_bytes| {
                fetched_offsets_from(&connection, statement, key, offset, max_bytes, RECORDS)
            };

            assert_eq!(
                (0..ROWS_PER_FETCH).collect::<Vec<_>>(),
                fetch_from(0, i64::MAX).await?,
                "{statement}: one read must stop at the row limit",
            );

            assert_eq!(
                (ROWS_PER_FETCH..RECORDS).collect::<Vec<_>>(),
                fetch_from(ROWS_PER_FETCH, i64::MAX).await?,
                "{statement}: a read from the last offset must return the rest",
            );

            assert_eq!(
                (40..40 + ROWS_PER_FETCH).collect::<Vec<_>>(),
                fetch_from(40, i64::MAX).await?,
                "{statement}: the row limit counts from the fetch offset",
            );

            // A record costs 110 bytes, so a budget of 1101 bytes holds 10 of them.
            assert_eq!(
                (40..50).collect::<Vec<_>>(),
                fetch_from(40, 1101).await?,
                "{statement}: the byte budget must count the headers",
            );

            // The first record is exempt from the budget (KIP-74), also when the row limit
            // cuts the read short.
            for offset in [0, ROWS_PER_FETCH - 1, ROWS_PER_FETCH] {
                assert_eq!(
                    vec![offset],
                    fetch_from(offset, 50).await?,
                    "{statement}: a first record larger than the budget, at {offset}",
                );
            }
        }

        // The records at the odd offsets take another key, so the keyed statement matches the
        // 5,025 records at the even offsets. A statement that applies the row limit before the
        // key filter returns 5,000 of them from offset 0.
        _ = connection
            .execute("update record set k = x'6a' where offset_id % 2 = 1", ())
            .await?;

        assert_eq!(
            (0..RECORDS).step_by(2).collect::<Vec<_>>(),
            fetched_offsets_from(
                &connection,
                "record_fetch_keyed.sql",
                Some(b"k"),
                0,
                i64::MAX,
                RECORDS
            )
            .await?,
            "record_fetch_keyed.sql: the key filter must apply before the row limit",
        );

        Ok(())
    }

    /// The fetch statements count a record's headers toward `max_bytes`: three records with a
    /// one byte key, no value and a 100 byte header each cost 110 bytes (the 7 bytes of every
    /// record, the header's 2 bytes of framing, 100 of its own, and the key). The statements run
    /// here, on the Turso engine, because it rejects some SQL that SQLite takes.
    #[tokio::test]
    async fn fetch_counts_header_bytes() -> Result<()> {
        let _guard = init_tracing()?;

        let db = turso::Builder::new_local(":memory:").build().await?;
        let connection = db.connect()?;

        create_fetch_tables(&connection).await?;
        insert_records_with_headers(
            &connection,
            &[(Value::Blob(b"h".to_vec()), Value::Blob(vec![0; 99]))],
        )
        .await?;

        for (statement, key) in FETCH_STATEMENTS {
            for (max_bytes, expected) in [(250, vec![0, 1]), (50, vec![0]), (1_000, vec![0, 1, 2])]
            {
                assert_eq!(
                    expected,
                    fetched_offsets(&connection, statement, key, max_bytes).await?,
                    "{statement} within {max_bytes}"
                );
            }
        }

        Ok(())
    }

    /// Each header costs 2 bytes of framing, even when its key is empty and its value is null: 50
    /// such headers and a one byte key cost a record 108 bytes, so a budget of 150 bytes returns
    /// one record. A fetch that dropped the framing, or that took the largest header of a record
    /// instead of the sum of its headers, would count 8 to 10 bytes for a record and return all
    /// three.
    #[tokio::test]
    async fn fetch_counts_the_framing_of_every_header() -> Result<()> {
        let _guard = init_tracing()?;

        let db = turso::Builder::new_local(":memory:").build().await?;
        let connection = db.connect()?;

        create_fetch_tables(&connection).await?;
        insert_records_with_headers(&connection, &vec![(Value::Blob(vec![]), Value::Null); 50])
            .await?;

        for (statement, key) in FETCH_STATEMENTS {
            assert_eq!(
                vec![0],
                fetched_offsets(&connection, statement, key, 150).await?,
                "{statement}"
            );
        }

        Ok(())
    }

    /// A header costs the length of its key and the length of its value. Each record has one
    /// header with a 40 byte key and a null value, and one with a null key and a 40 byte value,
    /// and so costs 92 bytes with its own key, and a budget of 110 bytes returns one record. A
    /// fetch that left out the key, or the value, or took the larger header of the two, would
    /// count 50 to 52 bytes and return two.
    #[tokio::test]
    async fn fetch_counts_the_key_and_the_value_of_every_header() -> Result<()> {
        let _guard = init_tracing()?;

        let db = turso::Builder::new_local(":memory:").build().await?;
        let connection = db.connect()?;

        create_fetch_tables(&connection).await?;
        insert_records_with_headers(
            &connection,
            &[
                (Value::Blob(vec![0; 40]), Value::Null),
                (Value::Null, Value::Blob(vec![0; 40])),
            ],
        )
        .await?;

        for (statement, key) in FETCH_STATEMENTS {
            assert_eq!(
                vec![0],
                fetched_offsets(&connection, statement, key, 110).await?,
                "{statement}"
            );
        }

        Ok(())
    }

    /// A record with no value and no headers still costs the 7 bytes of the smallest record, and
    /// the key of 1 byte, so 8 bytes. A budget of 40 bytes returns the 4 records whose running
    /// total stays under it, where a statement that counts 1 byte for each returns all 10.
    #[tokio::test]
    async fn fetch_counts_a_fixed_cost_for_every_record() -> Result<()> {
        let _guard = init_tracing()?;

        let db = turso::Builder::new_local(":memory:").build().await?;
        let connection = db.connect()?;

        create_fetch_tables(&connection).await?;
        for offset in 0..10 {
            _ = connection
                .execute(
                    "insert into record (topition, offset_id, timestamp, k) values (1, ?1, 0, x'6b')",
                    [offset],
                )
                .await?;
        }

        for (statement, key) in FETCH_STATEMENTS {
            assert_eq!(
                vec![0, 1, 2, 3],
                fetched_offsets_from(&connection, statement, key, 0, 40, 10).await?,
                "{statement}"
            );

            // the first record is returned whatever its size (KIP-74)
            assert_eq!(
                vec![0],
                fetched_offsets_from(&connection, statement, key, 0, 1, 10).await?,
                "{statement}"
            );
        }

        Ok(())
    }

    /// The fetch statements, each with the key that it filters on: `record_fetch_keyed.sql` reads
    /// the records of a virtual topic by their key.
    const FETCH_STATEMENTS: [(&str, Option<&[u8]>); 2] = [
        ("record_fetch.sql", None),
        ("record_fetch_keyed.sql", Some(b"k")),
    ];

    /// Creates the tables that the fetch statements read, and partition 0 of topic `t` in
    /// cluster `c`.
    async fn create_fetch_tables(connection: &Connection) -> Result<()> {
        for ddl in [
            "010-cluster.sql",
            "030-topition.sql",
            "040-record.sql",
            "040-header.sql",
        ] {
            _ = connection
                .execute(&DDL.get(ddl)?, ())
                .await
                .inspect_err(|err| error!(ddl, ?err))?;
        }

        // The Turso engine does not take the DDL of the topic table (`default false not null`)
        for sql in [
            "create table topic (
                id integer primary key autoincrement,
                cluster int not null,
                name text not null
            )",
            "insert into cluster (name) values ('c')",
            "insert into topic (cluster, name) values (1, 't')",
            "insert into topition (topic, partition) values (1, 0)",
        ] {
            _ = connection.execute(sql, ()).await?;
        }

        Ok(())
    }

    /// Stores three records at offsets 0 to 2 of partition 0, each with the key `k`, no value and
    /// the given headers as `(key, value)` pairs.
    async fn insert_records_with_headers(
        connection: &Connection,
        headers: &[(Value, Value)],
    ) -> Result<()> {
        for offset in 0..3 {
            _ = connection
                .execute(
                    "insert into record (topition, offset_id, timestamp, k) values (1, ?1, 0, x'6b')",
                    [offset],
                )
                .await?;

            for (ordinal, (key, value)) in headers.iter().enumerate() {
                _ = connection
                    .execute(
                        "insert into header (topition, offset_id, ordinal, k, v)
                            values (1, ?1, ?2, ?3, ?4)",
                        vec![
                            Value::from(offset),
                            Value::from(i64::try_from(ordinal)?),
                            key.clone(),
                            value.clone(),
                        ],
                    )
                    .await?;
            }
        }

        Ok(())
    }

    /// Stores `count` records at offsets 0 and up of partition 0, each with the key `k`, no value
    /// and one header with a 1 byte key and a 99 byte value.
    async fn insert_numbered_records(connection: &Connection, count: i64) -> Result<()> {
        _ = connection.execute("begin", ()).await?;

        for offset in 0..count {
            _ = connection
                .execute(
                    "insert into record (topition, offset_id, timestamp, k) values (1, ?1, 0, x'6b')",
                    [offset],
                )
                .await?;

            _ = connection
                .execute(
                    "insert into header (topition, offset_id, ordinal, k, v)
                        values (1, ?1, 0, x'68', ?2)",
                    vec![Value::from(offset), Value::Blob(vec![0; 99])],
                )
                .await?;
        }

        _ = connection.execute("commit", ()).await?;

        Ok(())
    }

    /// Returns the offsets that `statement` selects when it reads partition 0 from offset 0
    /// within `max_bytes`, up to a high watermark of 3. `key` is the key that a keyed statement
    /// filters on.
    async fn fetched_offsets(
        connection: &Connection,
        statement: &str,
        key: Option<&[u8]>,
        max_bytes: i64,
    ) -> Result<Vec<i64>> {
        fetched_offsets_from(connection, statement, key, 0, max_bytes, 3).await
    }

    /// Returns the offsets that `statement` selects when it reads partition 0 from `offset`
    /// within `max_bytes`, up to `high_watermark`, with the row limit that a fetch binds. `key`
    /// is the key that a keyed statement filters on.
    async fn fetched_offsets_from(
        connection: &Connection,
        statement: &str,
        key: Option<&[u8]>,
        offset: i64,
        max_bytes: i64,
        high_watermark: i64,
    ) -> Result<Vec<i64>> {
        let mut params = vec![
            Value::from("c"),
            Value::from("t"),
            Value::from(0),
            Value::from(offset),
            Value::from(max_bytes),
            Value::from(high_watermark),
        ];
        params.extend(key.map(|key| Value::Blob(key.to_vec())));
        params.push(Value::from(ROWS_PER_FETCH));

        let sql = sql_lookup(statement)?;
        let mut rows = connection.prepare(&sql).await?.query(params).await?;
        let mut offsets = vec![];
        while let Some(row) = rows.next().await? {
            offsets.push(row.get::<i64>(0)?);
        }

        Ok(offsets)
    }

    /// Runs `sql` as the engine runs an `insert ... returning`: prepare, query, then read the
    /// first row. Returns whether it returned a row.
    async fn insert_returning(
        connection: &Connection,
        sql: &str,
        params: impl IntoParams,
    ) -> result::Result<bool, turso::Error> {
        let mut statement = connection.prepare(sql).await?;
        let mut rows = statement.query(params).await?;
        Ok(rows.next().await?.is_some())
    }

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

    #[ignore]
    #[tokio::test]
    async fn lite_system_time() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let db = turso::Builder::new_local(file_path.to_str().unwrap())
            .build()
            .await?;
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

    #[tokio::test]
    async fn register_broker() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir_in(env::current_dir()?).inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let storage = storage_url(&temp_dir)?;
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

        let db = turso::Builder::new_local(file_path.to_str().unwrap())
            .build()
            .await?;
        let connection = db.connect()?;

        let mut rows = connection
            .query("select id, name from cluster where name = ?1", &[cluster])
            .await?;

        let row = rows.next().await?.unwrap();
        assert_eq!(
            Some(cluster),
            row.get_value(1)?.as_text().map(String::as_str)
        );

        Ok(())
    }

    #[tokio::test]
    async fn storage_create_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir_in(env::current_dir()?).inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let storage = storage_url(&temp_dir)?;
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

        let db = turso::Builder::new_local(file_path.to_str().unwrap())
            .build()
            .await?;
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
        let stored = row.get_value(0)?;
        assert_eq!(
            uuid,
            Uuid::parse_str(stored.as_text().expect("uuid is text"))?
        );
        assert_eq!(
            Some(&i64::from(num_partitions)),
            row.get_value(1)?.as_integer()
        );
        assert_eq!(
            Some(&i64::from(replication_factor)),
            row.get_value(2)?.as_integer()
        );

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn incremental_alter_resource() -> Result<()> {
        use nisshi_sans_io::incremental_alter_configs_request::AlterableConfig;

        let _guard = init_tracing()?;

        // The engine resolves the path of its URL against the working
        // directory, so the database lives in a temporary directory there.
        let temp_dir = tempdir_in(env::current_dir()?).inspect(|temporary| debug!(?temporary))?;
        let relative = temp_dir
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();

        let storage = Url::parse(&format!("file:///{relative}/nisshi.db"))?;
        let cluster = "nisshi";
        let node = 12321;
        let topic = "test";
        let cleanup_policy = "cleanup.policy";

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

        let alter = |name: &str, op: OpType, value: &str| {
            AlterConfigsResource::default()
                .resource_type(ConfigResource::Topic.into())
                .resource_name(name.to_owned())
                .configs(Some(vec![
                    AlterableConfig::default()
                        .name(cleanup_policy.to_owned())
                        .config_operation(op.into())
                        .value(Some(value.to_owned())),
                ]))
        };

        for (op, value, expected) in [
            (OpType::Append, "compact", "delete,compact"),
            (OpType::Append, "delete", "delete,compact"),
            (OpType::Subtract, "compact", "delete"),
        ] {
            let response = engine
                .incremental_alter_resource(alter(topic, op, value))
                .await?;
            assert_eq!(i16::from(ErrorCode::None), response.error_code);

            let described = engine
                .describe_config(
                    topic,
                    ConfigResource::Topic,
                    Some(&[cleanup_policy.to_owned()]),
                )
                .await?;
            let configs = described.configs.unwrap_or_default();
            assert_eq!(1, configs.len());
            assert_eq!(Some(expected), configs[0].value.as_deref());
        }

        assert!(matches!(
            engine
                .incremental_alter_resource(alter("missing", OpType::Append, "compact"))
                .await,
            Err(Error::Api(ErrorCode::UnknownTopicOrPartition))
        ));

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

    /// An open (uncommitted) transaction must pin the read_committed last stable
    /// offset below the high watermark. Regressed when `offset_stage` read the
    /// high-watermark column instead of the stable-offset column, so
    /// `read_committed` consumers saw uncommitted records.
    ///
    /// `#[ignore]`d like every other engine-level test here: turso/limbo cannot
    /// yet run the full produce flow in CI. Run manually against turso with
    /// `cargo nextest run --features turso -- --ignored`.
    #[ignore]
    #[tokio::test]
    async fn read_committed_last_stable_offset_pinned_by_open_transaction() -> Result<()> {
        use nisshi_sans_io::add_partitions_to_txn_request::AddPartitionsToTxnTopic;

        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let storage = Url::parse(&format!("file://{}", file_path.display()))?;
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

        // a transactional producer begins a transaction and produces, never commits
        let producer = engine
            .init_producer(Some("tx"), 10_000, Some(-1), Some(-1))
            .await?;

        _ = engine
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: "tx".into(),
                producer_id: producer.id,
                producer_epoch: producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name("t".into())
                        .partitions(Some(vec![0])),
                ],
            })
            .await?;

        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"uncommitted").into()))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = engine.produce(Some("tx"), &topition, batch).await?;

        let stage = engine.offset_stage(&topition).await?;

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

    /// `#[ignore]`d like every other engine-level test here: turso/limbo
    /// cannot yet run the full produce flow in CI. Run manually against
    /// turso with `cargo nextest run --features turso -- --ignored`.
    #[ignore]
    #[tokio::test]
    async fn delete_records_advances_log_start() -> Result<()> {
        use nisshi_sans_io::delete_records_request::DeleteRecordsPartition;

        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let storage = Url::parse(&format!("file://{}", file_path.display()))?;
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

        let record_count = 6;
        for n in 0..record_count {
            let batch = inflated::Batch::builder()
                .record(Record::builder().value(Some(Bytes::from(format!("record-{n}")))))
                .build()
                .and_then(TryInto::try_into)?;

            _ = engine.produce(None, &topition, batch).await?;
        }

        let cutoff = 3;

        let results = engine
            .delete_records(&[DeleteRecordsTopic::default()
                .name("t".into())
                .partitions(Some(
                    [DeleteRecordsPartition::default()
                        .partition_index(0)
                        .offset(cutoff)]
                    .into(),
                ))])
            .await?;

        assert_eq!(1, results.len());
        let partitions = results[0].partitions.clone().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(cutoff, partitions[0].low_watermark);

        let stage = engine.offset_stage(&topition).await?;
        assert_eq!(cutoff, stage.log_start);
        assert_eq!(record_count, stage.high_watermark);

        Ok(())
    }

    /// `offset == -1` ("delete everything up to the high watermark") moves the
    /// log start to the high watermark, and leaves the high watermark, and so
    /// `ListOffsets(Latest)`, unchanged.
    ///
    /// `#[ignore]`d like every other engine-level test here: turso/limbo
    /// cannot yet run the full produce flow in CI. Run manually against
    /// turso with `cargo nextest run --features turso -- --ignored`.
    #[ignore]
    #[tokio::test]
    async fn delete_records_to_high_watermark_keeps_latest() -> Result<()> {
        use nisshi_sans_io::delete_records_request::DeleteRecordsPartition;

        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let storage = Url::parse(&format!("file://{}", file_path.display()))?;
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

        let record_count = 6;
        for n in 0..record_count {
            let batch = inflated::Batch::builder()
                .record(Record::builder().value(Some(Bytes::from(format!("record-{n}")))))
                .build()
                .and_then(TryInto::try_into)?;

            _ = engine.produce(None, &topition, batch).await?;
        }

        let results = engine
            .delete_records(&[DeleteRecordsTopic::default()
                .name("t".into())
                .partitions(Some(
                    [DeleteRecordsPartition::default()
                        .partition_index(0)
                        .offset(-1)]
                    .into(),
                ))])
            .await?;

        let partitions = results[0].partitions.clone().unwrap_or_default();
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(record_count, partitions[0].low_watermark);

        let stage = engine.offset_stage(&topition).await?;
        assert_eq!(record_count, stage.log_start);
        assert_eq!(record_count, stage.high_watermark);

        // `offset_stage` reads the watermark only, so these check what
        // `ListOffsets` answers.
        let offsets = engine
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[
                    (topition.clone(), ListOffsetRequest::Latest),
                    (topition.clone(), ListOffsetRequest::Earliest),
                ],
            )
            .await?;

        assert_eq!(Some(record_count), offsets[0].1.offset, "{offsets:?}");
        assert_eq!(Some(record_count), offsets[1].1.offset, "{offsets:?}");

        Ok(())
    }

    /// An unknown topic in a `DeleteRecords` request must never fail the
    /// whole call: it reports `UnknownTopicOrPartition` for that one
    /// partition while a sibling topic in the same request still succeeds.
    ///
    /// `#[ignore]`d like every other engine-level test here: turso/limbo
    /// cannot yet run the full produce flow in CI. Run manually against
    /// turso with `cargo nextest run --features turso -- --ignored`.
    #[ignore]
    #[tokio::test]
    async fn delete_records_unknown_topic_does_not_fail_sibling() -> Result<()> {
        use nisshi_sans_io::delete_records_request::DeleteRecordsPartition;

        let _guard = init_tracing()?;

        let temp_dir = tempdir().inspect(|temporary| debug!(?temporary))?;
        let file_path = temp_dir.path().join("nisshi.db");

        let storage = Url::parse(&format!("file://{}", file_path.display()))?;
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

        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Some(Bytes::from_static(b"one"))))
            .build()
            .and_then(TryInto::try_into)?;

        _ = engine.produce(None, &topition, batch).await?;

        let results = engine
            .delete_records(&[
                DeleteRecordsTopic::default()
                    .name("missing".into())
                    .partitions(Some(
                        [DeleteRecordsPartition::default()
                            .partition_index(0)
                            .offset(0)]
                        .into(),
                    )),
                DeleteRecordsTopic::default()
                    .name("t".into())
                    .partitions(Some(
                        [DeleteRecordsPartition::default()
                            .partition_index(0)
                            .offset(1)]
                        .into(),
                    )),
            ])
            .await?;

        assert_eq!(2, results.len());

        let missing = results
            .iter()
            .find(|topic| topic.name == "missing")
            .expect("missing topic result");
        let missing_partitions = missing.partitions.clone().unwrap_or_default();
        assert_eq!(
            ErrorCode::UnknownTopicOrPartition,
            ErrorCode::try_from(missing_partitions[0].error_code)?
        );

        let known = results
            .iter()
            .find(|topic| topic.name == "t")
            .expect("known topic result");
        let known_partitions = known.partitions.clone().unwrap_or_default();
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(known_partitions[0].error_code)?
        );
        assert_eq!(1, known_partitions[0].low_watermark);

        Ok(())
    }

    /// The storage that the factory builds runs one write at a time, so calls that write at
    /// the same time, as the DeleteRecords service makes one call for each partition, all
    /// succeed. Turso fails a write that finds another connection's write lock held with
    /// `Busy`, so this fails if the factory does not put a proxy in front of the engine. It
    /// runs in CI: a database file in a temporary directory needs no service.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn factory_storage_runs_concurrent_delete_records_calls_one_at_a_time() -> Result<()> {
        use std::sync::Arc as Shared;

        use nisshi_sans_io::delete_records_request::DeleteRecordsPartition;
        use nisshi_storage::{StorageFactory as _, StorageFactoryConfiguration};
        use tokio::{sync::Barrier, task::JoinSet};
        use tokio_util::sync::CancellationToken;

        let _guard = init_tracing()?;
        let temp_dir = tempdir_in(env::current_dir()?)?;

        let node = 12321;
        let cluster = "nisshi";

        let engine = factory::LimboFactory
            .build(StorageFactoryConfiguration {
                node_id: node,
                cluster: cluster.to_owned(),
                advertised_listener: Url::parse("tcp://127.0.0.1:9092")?,
                storage: storage_url(&temp_dir)?,
                schema_registry: None,
                lake_house: None,
                cancellation: CancellationToken::new(),
            })
            .await?;

        engine
            .register_broker(BrokerRegistrationRequest {
                broker_id: node,
                cluster_id: cluster.to_owned(),
                incarnation_id: Uuid::new_v4(),
                rack: None,
            })
            .await?;

        assert_eq!(1, engine.delete_records_concurrency().per_request());

        let partitions = 4;

        _ = engine
            .create_topic(
                CreatableTopic::default()
                    .name("t".into())
                    .num_partitions(partitions)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;

        // A call holds the write lock for a short time, so one round of calls can miss
        // each other. Many rounds, each started together, make them meet.
        for _ in 0..50 {
            let start = Shared::new(Barrier::new(usize::try_from(partitions)?));
            let mut calls = JoinSet::new();

            for partition in 0..partitions {
                let engine = engine.clone();
                let start = Shared::clone(&start);

                _ = calls.spawn(async move {
                    _ = start.wait().await;

                    engine
                        .delete_records(&[DeleteRecordsTopic::default()
                            .name("t".into())
                            .partitions(Some(vec![
                                DeleteRecordsPartition::default()
                                    .partition_index(partition)
                                    .offset(-1),
                            ]))])
                        .await
                });
            }

            while let Some(call) = calls.join_next().await {
                let topics = call??;

                assert_eq!(
                    vec![ErrorCode::None],
                    topics
                        .iter()
                        .flat_map(|topic| topic.partitions.iter().flatten())
                        .map(|partition| ErrorCode::try_from(partition.error_code))
                        .collect::<Result<Vec<_>, _>>()?
                );
            }
        }

        Ok(())
    }

    /// Returns the URL of `nisshi.db` in `temp_dir`. `build()` opens the URL path relative to
    /// the current directory, so `temp_dir` must be in it.
    fn storage_url(temp_dir: &TempDir) -> Result<Url> {
        let relative = temp_dir
            .path()
            .strip_prefix(env::current_dir()?)
            .map_err(|err| Error::Message(err.to_string()))?
            .join("nisshi.db");

        Ok(Url::parse(&format!("file:///{}", relative.display()))?)
    }

    /// Returns an engine on a new database in `temp_dir`, with a registered broker.
    async fn registered_engine(temp_dir: &TempDir) -> Result<Engine> {
        let cluster = "nisshi";
        let node = 12321;

        let engine = Engine::builder()
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
            .cluster(cluster.to_owned())
            .storage(storage_url(temp_dir)?)
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

        Ok(engine)
    }

    /// Returns an engine on a new database in `temp_dir`, with the topics `t` and `unreadable`.
    /// The stored row of `unreadable` can't be read.
    async fn engine_with_unreadable_topic(temp_dir: &TempDir) -> Result<Engine> {
        let engine = registered_engine(temp_dir).await?;

        for name in ["t", "unreadable"] {
            _ = engine
                .create_topic(
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(1)
                        .replication_factor(1)
                        .assignments(Some([].into()))
                        .configs(Some([].into())),
                    false,
                )
                .await?;
        }

        // Turso stores text that isn't a number as text in an `int` column, and the reader of
        // `partitions` then can't read it as an integer.
        _ = engine
            .connection()
            .await?
            .execute(
                "update topic set partitions = 'x' where name = ?1",
                ["unreadable"],
            )
            .await?;

        Ok(engine)
    }

    /// Returns an engine on a new database in `temp_dir`, with the groups `healthy` and
    /// `undecodable`, and the detail that it stored for `healthy`. The stored detail of
    /// `undecodable` isn't text.
    async fn engine_with_group_detail_not_text(
        temp_dir: &TempDir,
    ) -> Result<(Engine, GroupDetail)> {
        let engine = registered_engine(temp_dir).await?;

        // `GroupDetail::default` records the current time as the group's inception, so the helper
        // returns the detail that it stores.
        let stored = GroupDetail::default();

        for group_id in ["healthy", "undecodable"] {
            _ = engine
                .update_group(group_id, stored.clone(), None)
                .await
                .map_err(|err| Error::Message(format!("{err:?}")))?;
        }

        // SQLite never converts a blob to text for the column's affinity, so the detail stays a
        // value that isn't text.
        let changed = engine
            .connection()
            .await?
            .execute(
                "update consumer_group_detail set detail = x'7b7d'
                where consumer_group = (
                    select cg.id from consumer_group cg join cluster c on c.id = cg.cluster
                    where c.name = ?1 and cg.name = ?2
                )",
                ["nisshi", "undecodable"],
            )
            .await?;
        // The update must change the group's row. An update that changes no row leaves the group
        // readable, and a test then fails for that reason instead of testing the engine.
        assert_eq!(1, changed);

        Ok((engine, stored))
    }

    /// A ListGroups must answer a stored group whose detail isn't text with `UNKNOWN_SERVER_ERROR`
    /// as it answers a detail that isn't JSON, and list the other groups as usual.
    #[tokio::test]
    async fn list_groups_answers_unknown_server_error_for_a_detail_that_is_not_text() -> Result<()>
    {
        let _guard = init_tracing()?;

        let temp_dir = tempdir_in(env::current_dir()?)?;
        let (engine, stored) = engine_with_group_detail_not_text(&temp_dir).await?;

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

    /// A DescribeGroups must answer a stored group whose detail isn't text with
    /// `UNKNOWN_SERVER_ERROR`, which clients report at once, and answer the other groups in the
    /// request as usual.
    #[tokio::test]
    async fn describe_groups_answers_unknown_server_error_for_a_detail_that_is_not_text()
    -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir_in(env::current_dir()?)?;
        let (engine, stored) = engine_with_group_detail_not_text(&temp_dir).await?;

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

    /// A topic whose stored row can't be read fails each Metadata and DescribeTopicPartitions
    /// request that reads it by name, and a Metadata request for every topic.
    #[tokio::test]
    async fn a_request_that_reads_an_unreadable_topic_fails() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir_in(env::current_dir()?)?;
        let engine = engine_with_unreadable_topic(&temp_dir).await?;

        let by_name = TopicId::from("unreadable");

        for topics in [Some(vec![by_name.clone()]), None] {
            assert_matches!(
                engine.metadata(topics.as_deref()).await,
                Err(Error::UnreadableColumn { .. }),
                "{topics:?}"
            );
        }

        assert_matches!(
            engine
                .describe_topic_partitions(Some(&[by_name]), i32::MAX, None)
                .await,
            Err(Error::UnreadableColumn { .. })
        );

        Ok(())
    }

    /// DeleteTopics deletes a topic whose stored row can't be read, because it reads only the
    /// topic's name, so that an operator can remove a corrupt topic without editing the database.
    #[tokio::test]
    async fn delete_topic_deletes_a_topic_with_an_unreadable_row() -> Result<()> {
        let _guard = init_tracing()?;

        let temp_dir = tempdir_in(env::current_dir()?)?;
        let engine = engine_with_unreadable_topic(&temp_dir).await?;

        assert_eq!(
            ErrorCode::None,
            engine.delete_topic(&TopicId::from("unreadable")).await?
        );

        // A deleted topic is unknown, where a topic that is still stored with its unreadable row
        // is a server error.
        assert_eq!(
            vec![i16::from(ErrorCode::UnknownTopicOrPartition)],
            engine
                .metadata(Some(&[TopicId::from("unreadable")]))
                .await?
                .topics
                .iter()
                .map(|topic| topic.error_code)
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    /// Sets the epoch of each `producer_epoch` row of `producer` to `epoch`. A producer
    /// that has one epoch then has only `epoch`, without 32k InitProducerId requests.
    async fn seed_producer_epoch(engine: &Engine, producer: i64, epoch: i64) -> Result<()> {
        _ = engine
            .connection()
            .await?
            .execute(
                "update producer_epoch set epoch = ?1 where producer = ?2",
                (epoch, producer),
            )
            .await?;

        Ok(())
    }

    /// An epoch below the limit bumps. At the limit, a fresh InitProducerId moves the
    /// transactional ID to a new producer ID at epoch 0, and a claim of the previous
    /// producer ID is fenced.
    #[ignore]
    #[tokio::test]
    async fn init_producer_exhausted_epoch_moves_to_a_new_producer_id() -> Result<()> {
        let _guard = init_tracing()?;
        let temp_dir = tempdir_in(env::current_dir()?)?;
        let engine = registered_engine(&temp_dir).await?;

        let producer = engine
            .init_producer(Some("txn"), 10_000, None, None)
            .await?;
        seed_producer_epoch(&engine, producer.id, 32765).await?;

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

        Ok(())
    }

    /// A claim of an exhausted epoch moves the transactional ID to a new producer ID.
    #[ignore]
    #[tokio::test]
    async fn init_producer_exhausted_claim_moves_to_a_new_producer_id() -> Result<()> {
        let _guard = init_tracing()?;
        let temp_dir = tempdir_in(env::current_dir()?)?;
        let engine = registered_engine(&temp_dir).await?;

        let producer = engine
            .init_producer(Some("txn"), 10_000, None, None)
            .await?;
        seed_producer_epoch(&engine, producer.id, 32766).await?;

        let rotated = engine
            .init_producer(Some("txn"), 10_000, Some(producer.id), Some(32766))
            .await?;
        assert_eq!((ErrorCode::None, 0), (rotated.error, rotated.epoch));
        assert_ne!(producer.id, rotated.id);

        Ok(())
    }

    /// An exhausted epoch with a prepared transaction answers ConcurrentTransactions.
    #[ignore]
    #[tokio::test]
    async fn init_producer_exhausted_epoch_with_a_prepared_transaction_is_concurrent() -> Result<()>
    {
        let _guard = init_tracing()?;
        let temp_dir = tempdir_in(env::current_dir()?)?;
        let engine = registered_engine(&temp_dir).await?;

        let producer = engine
            .init_producer(Some("txn"), 10_000, None, None)
            .await?;
        seed_producer_epoch(&engine, producer.id, 32766).await?;
        _ = engine
            .connection()
            .await?
            .execute("update txn_detail set status = 'PREPARE_ABORT'", ())
            .await?;

        assert_eq!(
            ProducerIdResponse::failed(ErrorCode::ConcurrentTransactions),
            engine
                .init_producer(Some("txn"), 10_000, None, None)
                .await?
        );

        Ok(())
    }

    /// The database does not limit an integer column to `i16`. A stored epoch beyond
    /// `i16::MAX` is an error, not an epoch that wraps to a negative value.
    #[ignore]
    #[tokio::test]
    async fn init_producer_epoch_beyond_i16_is_an_error() -> Result<()> {
        let _guard = init_tracing()?;
        let temp_dir = tempdir_in(env::current_dir()?)?;
        let engine = registered_engine(&temp_dir).await?;

        let producer = engine
            .init_producer(Some("txn"), 10_000, None, None)
            .await?;
        seed_producer_epoch(&engine, producer.id, 40_000).await?;

        assert!(matches!(
            engine.init_producer(Some("txn"), 10_000, None, None).await,
            Err(Error::UnreadableColumn { column: "epoch", source, .. })
                if matches!(*source, Error::TryFromInt(_))
        ));

        Ok(())
    }
}
