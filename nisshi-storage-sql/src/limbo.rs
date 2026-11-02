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
        Cache, consumer_group_select, consumer_offset_select, consumer_offset_select_by_group,
        default_hash, idempotent_epoch_check, idempotent_sequence_check, producer_epochs_select,
        remove_comments, result_columns::ResultColumns, txn_offset_commit_tp_select_pending,
        txn_produce_partition_check, txn_produce_partition_select, watermark_select_for_write,
    },
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::NaiveDateTime;
use nisshi_sans_io::{
    BatchAttribute, ConfigResource, ConfigSource, ConfigType, ControlBatch, EndTransactionMarker,
    ErrorCode, IsolationLevel, NULL_TOPIC_ID, OpType, ScramMechanism, TimestampType,
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
    to_system_time, to_timestamp,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use nisshi_schema::{
    Registry,
    lake::{House, LakeHouse as _},
    redact_url,
};
use nisshi_storage::{
    BrokerRegistrationRequest, CommittedOffset, DELETE_RECORDS_INVALID_LOW_WATERMARK, Error,
    GroupDetail, ListOffsetRequest, ListOffsetResponse, METER, MetadataResponse, NamedGroupDetail,
    OffsetCommitRequest, OffsetStage, ProduceEpoch, ProducerIdResponse, Result, ScramCredential,
    Storage, StoredEpochs, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, TxnState, UpdateError, Version, check_claim, config::apply_op,
    delete_records_cutoff, epoch_is_exhausted, inflate, max_message_bytes, producer_claim,
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use rand::{rng, seq::SliceRandom as _};
use regex::Regex;
use tracing::{debug, error, info};
use turso::{
    Connection, Database, Row, Value, params::IntoParams, transaction::Transaction,
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

impl TryFrom<Row> for Txn {
    type Error = Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let name = row
            .get_value(0)
            .map_err(Into::into)
            .and_then(|value| {
                value
                    .as_text()
                    .cloned()
                    .ok_or(Error::UnexpectedValue(value))
            })
            .inspect_err(|err| error!(?err))?;

        let producer_id = row
            .get_value(1)
            .map_err(Into::into)
            .and_then(|value| {
                value
                    .as_integer()
                    .copied()
                    .ok_or(Error::UnexpectedValue(value))
            })
            .inspect_err(|err| error!(?err))?;

        let producer_epoch = row
            .get_value(2)
            .map_err(Into::into)
            .and_then(|value| {
                value
                    .as_integer()
                    .copied()
                    .ok_or(Error::UnexpectedValue(value))
                    .and_then(|value| i16::try_from(value).map_err(Error::from))
            })
            .inspect_err(|err| error!(?err))?;

        let status = row
            .get_value(3)
            .map(|value| value.as_text().cloned())
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

/// Turso storage engine
///
#[derive(Clone, Debug)]
pub struct Engine {
    cluster: String,
    node: i32,
    advertised_listener: Url,
    db: Arc<Mutex<Database>>,

    schemas: Option<Registry>,
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
                &sql_lookup("topic_select_name.sql")?,
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
                tx.query(
                    &sql_lookup("topic_configuration_select_name.sql")?,
                    (self.cluster.as_str(), topic, name),
                )
                .await?
                .next()
                .await?
                .map(|row| row.get_value(0).map(|value| value.as_text().cloned()))
                .transpose()?
                .flatten()
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
                            &sql_lookup("topic_configuration_upsert.sql")?,
                            (self.cluster.as_str(), topic, name, value.as_str()),
                        )
                        .await?
                        .next()
                        .await?;
                }

                None => {
                    _ = tx
                        .execute(
                            &sql_lookup("topic_configuration_delete.sql")?,
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

    async fn prepare_query_one<P>(
        &self,
        connection: &Connection,
        sql: &str,
        params: P,
    ) -> result::Result<Row, turso::Error>
    where
        P: IntoParams,
    {
        // The params hold record data, so we log only the SQL.
        debug!(?connection, sql);

        let mut statement = connection
            .prepare(sql)
            .await
            .inspect_err(|err| error!(?err, sql))?;

        let execute_start = SystemTime::now();

        let mut rows = statement.query(params).await.inspect_err(|err| {
            error!(?err, sql);
            SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
        })?;

        if let Some(row) = rows
            .next()
            .await
            .inspect(|row| debug!(?row))
            .inspect_err(|err| {
                error!(?err, sql);
                SQL_ERROR.add(1, &self.attributes_for_error(sql, err)[..]);
            })?
        {
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

            Ok(row).inspect(|row| debug!(?row))
        } else {
            error!(sql, "query returned no rows");
            Err(turso::Error::QueryReturnedNoRows)
        }
    }

    async fn idempotent_message_check(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
        connection: &Connection,
    ) -> Result<()> {
        debug!(transaction_id, ?deflated);

        let mut rows = connection
            .query(
                &sql_lookup(producer_epochs_select::QUERY)?,
                (
                    self.cluster.as_str(),
                    deflated.producer_id,
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await?;

        let columns = ResultColumns::turso(producer_epochs_select::QUERY, &rows);
        let newest_epoch = columns.index(producer_epochs_select::NEWEST_EPOCH)?;
        let partition_epoch = columns.index(producer_epochs_select::PARTITION_EPOCH)?;
        let transactional = columns.index(producer_epochs_select::TRANSACTIONAL)?;

        if let Some(row) = rows.next().await.inspect_err(|err| error!(?err))? {
            let stored = StoredEpochs {
                transactional: integer_value(&row, transactional)
                    .inspect_err(|err| error!(?err))?
                    != 0,
                newest: epoch_value(&row, newest_epoch).inspect_err(|err| error!(?err))?,
                partition: match row.get_value(partition_epoch)? {
                    Value::Null => None,
                    _ => Some(epoch_value(&row, partition_epoch).inspect_err(|err| error!(?err))?),
                },
            };

            if idempotent_epoch_check(transaction_id, stored, deflated)? == ProduceEpoch::Start {
                _ = self
                    .prepare_execute(
                        connection,
                        &sql_lookup("producer_epoch_insert_given.sql")?,
                        (
                            self.cluster.as_str(),
                            deflated.producer_id,
                            deflated.producer_epoch,
                        ),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?;
            }

            let row = self
                .prepare_query_one(
                    connection,
                    &sql_lookup("producer_select_for_update.sql")?,
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

            let sequence = row
                .get_value(0)
                .inspect_err(|err| error!(self.cluster, deflated.producer_id, ?err))
                .map_err(Into::into)
                .and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .map(|value| value as i32)
                        .ok_or(Error::UnexpectedValue(value))
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
                    &sql_lookup("producer_detail_insert.sql")?,
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

        let mut rows = tx
            .query(
                &sql_lookup(watermark_select_for_write::LITE_QUERY)?,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await?;

        let columns = ResultColumns::turso(watermark_select_for_write::LITE_QUERY, &rows);
        let low = columns.index(watermark_select_for_write::LOW)?;
        let high = columns.index(watermark_select_for_write::HIGH)?;
        let max_message_bytes = columns.index(watermark_select_for_write::MAX_MESSAGE_BYTES)?;

        let Some(row) = rows
            .next()
            .await
            .inspect_err(|err| error!(?err, cluster = ?self.cluster, ?topition))?
        else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        Ok(watermark_select_for_write::Row {
            low: row
                .get_value(low)
                .map(|value| value.as_integer().copied())
                .inspect_err(|err| error!(?err))?,
            high: row
                .get_value(high)
                .map(|value| value.as_integer().copied())
                .inspect_err(|err| error!(?err))?,
            max_message_bytes: row
                .get_value(max_message_bytes)
                .map(|value| value.as_text().cloned())
                .inspect_err(|err| error!(?err))?,
        })
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
        let mut rows = connection
            .query(
                &sql_lookup(txn_produce_partition_select::QUERY)?,
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

        let columns = ResultColumns::turso(txn_produce_partition_select::QUERY, &rows);
        let producer = columns.index(txn_produce_partition_select::PRODUCER)?;
        let added = columns.index(txn_produce_partition_select::ADDED)?;

        let row = match rows.next().await.inspect_err(|err| error!(?err))? {
            Some(row) => Some((
                match row.get_value(producer)? {
                    Value::Null => None,
                    _ => Some(integer_value(&row, producer).inspect_err(|err| error!(?err))?),
                },
                integer_value(&row, added).inspect_err(|err| error!(?err))? as i32,
            )),
            None => None,
        };

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

        if !attributes.control
            && let Some(ref schemas) = self.schemas
        {
            schemas.validate(topition.topic(), &inflated).await?;
        }

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
                    &sql_lookup("record_insert.sql")?,
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
                        &sql_lookup("header_insert.sql")?,
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
                        &sql_lookup("txn_produce_offset_insert.sql")?,
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
                &sql_lookup("watermark_update.sql")?,
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

        let mut rows = tx
            .deref()
            .query(
                &sql_lookup("txn_select_produced_topitions.sql")?,
                (
                    self.cluster.as_str(),
                    transaction_id,
                    producer_id,
                    producer_epoch,
                ),
            )
            .await?;

        while let Some(row) = rows.next().await? {
            let topic = row.get_value(0).map_err(Into::into).and_then(|value| {
                value
                    .as_text()
                    .cloned()
                    .ok_or(Error::UnexpectedValue(value))
            })?;

            let partition = row.get_value(1).map_err(Into::into).and_then(|value| {
                value
                    .as_integer()
                    .map(|i| *i as i32)
                    .ok_or(Error::UnexpectedValue(value))
            })?;

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

            let mut rows = tx
                .query(
                    &sql_lookup("txn_produce_offset_select_offset_range.sql")?,
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

            if let Some(row) = rows.next().await? {
                let offset_start = row.get_value(0).map_err(Into::into).and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .ok_or(Error::UnexpectedValue(value))
                })?;

                let offset_end = row.get_value(1).map_err(Into::into).and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .ok_or(Error::UnexpectedValue(value))
                })?;

                debug!(offset_start, offset_end);

                let mut rows = tx
                    .query(
                        &sql_lookup("txn_produce_offset_select_overlapping_txn.sql")?,
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
                        &sql_lookup("txn_produce_offset_delete_by_txn.sql")?,
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
                        &sql_lookup("txn_topition_delete_by_txn.sql")?,
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
                            &sql_lookup("consumer_offset_insert_from_txn.sql")?,
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
                        &sql_lookup("txn_offset_commit_tp_delete_by_txn.sql")?,
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
                        &sql_lookup("txn_offset_commit_delete_by_txn.sql")?,
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
                        &sql_lookup("txn_status_update.sql")?,
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
                    &sql_lookup("txn_status_update.sql")?,
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
    schemas: Option<Registry>,
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
            schemas: self.schemas,
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
            schemas: self.schemas,
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
            schemas: self.schemas,
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
            schemas: self.schemas,
            lake: self.lake,
        }
    }

    pub(crate) fn schemas(self, schemas: Option<Registry>) -> Builder<C, N, L, D> {
        Self { schemas, ..self }
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
            include_sql!("ddl/020-consumer-group.sql"),
        ),
        ("020-producer.sql", include_sql!("ddl/020-producer.sql")),
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
    crate::sql::SQL
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
                .inspect_err(|err| error!(name, ?err));
        }

        Ok(Engine {
            cluster: self.cluster,
            node: self.node,
            advertised_listener: self.advertised_listener,
            db: Arc::new(Mutex::new(db)),
            schemas: self.schemas,
            lake: self.lake,
        })
    }
}

/// Returns the integer in column `index` of `row`.
fn integer_value(row: &Row, index: usize) -> Result<i64> {
    row.get_value(index).map_err(Error::from).and_then(|value| {
        value
            .as_integer()
            .copied()
            .ok_or(Error::UnexpectedValue(value))
    })
}

/// Returns the producer epoch in column `index` of `row`, or an error when the stored
/// integer is outside `i16`.
fn epoch_value(row: &Row, index: usize) -> Result<i16> {
    integer_value(row, index).and_then(|epoch| i16::try_from(epoch).map_err(Error::from))
}

/// Returns the leader epoch in column `index` of `row`, or `None` when the column is null.
/// Returns an error when the stored integer is outside `i32`.
fn leader_epoch_value(row: &Row, index: usize) -> Result<Option<i32>> {
    row.get_value(index).map_err(Error::from).and_then(|value| {
        if value.is_null() {
            Ok(None)
        } else {
            value
                .as_integer()
                .copied()
                .ok_or(Error::UnexpectedValue(value))
                .and_then(|epoch| i32::try_from(epoch).map(Some).map_err(Error::from))
        }
    })
}

/// Returns the text in column `index` of `row`, or `None` when the column is null.
fn optional_text_value(row: &Row, index: usize) -> Result<Option<String>> {
    row.get_value(index).map_err(Error::from).and_then(|value| {
        if value.is_null() {
            Ok(None)
        } else {
            value
                .as_text()
                .cloned()
                .map(Some)
                .ok_or(Error::UnexpectedValue(value))
        }
    })
}

/// Returns a mapper that turns a unique constraint failure into [`Error::Api`] with
/// `error_code`, and passes any other error through.
///
/// turso reports a unique or primary key failure as [`turso::Error::Constraint`] with a
/// message, and does not give its SQLite extended result code, so the mapper matches the
/// message's prefix.
fn unique_constraint(error_code: ErrorCode) -> impl Fn(turso::Error) -> Error {
    move |err| match err {
        turso::Error::Constraint(ref reason) if reason.starts_with("UNIQUE constraint failed") => {
            debug!(reason);
            Error::Api(error_code)
        }

        otherwise => otherwise.into(),
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
fn group_detail_with_version(row: &Row) -> Result<(GroupDetail, Version)> {
    let version = row
        .get_value(0)
        .map_err(Error::from)
        .and_then(|value| {
            value
                .as_text()
                .ok_or(Error::UnexpectedValue(value.clone()))
                .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
        })
        .inspect_err(|err| error!(?err))
        .map(|uuid| Version {
            e_tag: Some(uuid.to_string()),
            version: None,
        })
        .inspect(|version| debug!(?version))?;

    let current = row
        .get_value(1)
        .map_err(Error::from)
        .and_then(|value| {
            value
                .as_text()
                .map(|v| v.as_str())
                .ok_or(Error::UnexpectedValue(value.clone()))
                .and_then(|s| serde_json::from_str::<GroupDetail>(s).map_err(Into::into))
        })
        .inspect(|current| debug!(?current))?;

    Ok((current, version))
}

#[async_trait]
impl Storage for Engine {
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        debug!(?broker_registration);

        let connection = self.connection().await?;

        self.prepare_execute(
            &connection,
            &sql_lookup("register_broker.sql")?,
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

            self.prepare_query_one(&tx, &sql_lookup("topic_insert.sql")?, parameters.clone())
                .await
                .inspect_err(|err| error!(?err))
                .map_err(unique_constraint(ErrorCode::TopicAlreadyExists))
                .inspect(|row| debug!(?parameters, ?row))
                .and_then(|row| {
                    row.get_value(0)
                        .map_err(Into::into)
                        .and_then(|value| {
                            value.as_text().cloned().ok_or_else(|| {
                                Error::Message("expected a text column value".into())
                            })
                        })
                        .inspect_err(|err| error!(?err))
                })
                .and_then(|id| Uuid::parse_str(id.as_str()).map_err(Into::into))
        }
        .inspect(|uuid| debug!(?uuid))
        .inspect_err(|err| error!(?err))?;

        for partition in 0..topic.num_partitions {
            let params = (self.cluster.as_str(), topic.name.as_str(), partition);

            _ = self
                .prepare_query_one(&tx, &sql_lookup("topition_insert.sql")?, params)
                .await
                .map(|row| row.get_value(0))
                .inspect(|topition| debug!(?topition))?;

            _ = self
                .prepare_query_one(&tx, &sql_lookup("watermark_insert.sql")?, params)
                .await
                .map(|row| row.get_value(0))
                .inspect(|watermark| debug!(?watermark))?;
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

                _ = self
                    .prepare_query_one(&tx, &sql_lookup("topic_configuration_upsert.sql")?, params)
                    .await
                    .map(|row| row.get_value(0))
                    .inspect_err(|err| error!(?err, ?config))
                    .inspect(|id| debug!(?id, ?config))?;
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
                                                &sql_lookup("record_delete_by_offset.sql")?,
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
                                            &sql_lookup("watermark_update.sql")?,
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

        let mut rows = match topic {
            TopicId::Id(id) => {
                tx.query(
                    &sql_lookup("topic_select_uuid.sql")?,
                    (self.cluster.as_str(), id.to_string().as_str()),
                )
                .await?
            }

            TopicId::Name(name) => {
                tx.query(
                    &sql_lookup("topic_select_name.sql")?,
                    (self.cluster.as_str(), name.as_str()),
                )
                .await?
            }
        };

        let Some(row) = rows.next().await? else {
            return Ok(ErrorCode::UnknownTopicOrPartition);
        };

        let value = row.get_value(1)?;
        let topic_name = value
            .as_text()
            .map(|topic_name| topic_name.as_str())
            .ok_or(Error::UnexpectedValue(value.clone()))?;

        for sql in [
            "consumer_offset_delete_by_topic.sql",
            "topic_configuration_delete_by_topic.sql",
            "watermark_delete_by_topic.sql",
            "header_delete_by_topic.sql",
            "record_delete_by_topic.sql",
            "txn_offset_commit_tp_delete_by_topic.sql",
            "txn_produce_offset_delete_by_topic.sql",
            "txn_topition_delete_by_topic.sql",
            "producer_detail_delete_by_topic.sql",
            "topition_delete_by_topic.sql",
        ] {
            let rows = self
                .prepare_execute(&tx, &sql_lookup(sql)?, (self.cluster.as_str(), topic_name))
                .await?;

            debug!(?topic, rows, sql)
        }

        _ = self
            .prepare_execute(
                &tx,
                &sql_lookup("topic_delete_by.sql")?,
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
        debug!(cluster = self.cluster, transaction_id, ?topition, ?deflated);

        // The batch is inflated before the transaction starts, so the wait
        // for a decode permit and the decode do not hold a connection or the
        // database write lock.
        let inflated = inflate(deflated.clone())
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut connection = self.connection().await?;
        let tx = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;

        let high = self
            .produce_in_tx(transaction_id, topition, deflated, inflated, &tx)
            .await?;

        tx.commit().await.map_err(Into::into).and(Ok(high))
    }

    async fn fetch(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
        _max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        debug!(?topition, offset, min_bytes, max_bytes, ?isolation_level);
        let high_watermark = self.offset_stage(topition).await.map(|offset_stage| {
            if isolation_level == IsolationLevel::ReadCommitted {
                offset_stage.last_stable
            } else {
                offset_stage.high_watermark
            }
        })?;

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

        let mut records = c
            .query(
                &sql_lookup("record_fetch.sql")?,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                    offset,
                    (max_bytes as i64),
                    high_watermark,
                ),
            )
            .await
            .inspect_err(|err| error!(?err))?;

        let mut batches = vec![];

        if let Some(row) = records.next().await? {
            let offset_delta = 0;
            let timestamp_delta = 0;

            let base_offset = row
                .get_value(0)
                .map_err(Into::into)
                .and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .ok_or(Error::UnexpectedValue(value))
                })
                .inspect(|base_offset| debug!(base_offset))
                .inspect_err(|err| error!(?err))?;

            let record_builder = {
                let mut record_builder = Record::builder()
                    .offset_delta(offset_delta)
                    .timestamp_delta(timestamp_delta)
                    .key(
                        row.get_value(3)
                            .map(|o| o.as_blob().map(|blob| Bytes::copy_from_slice(blob)))
                            .inspect(|k| debug!(key_len = ?k.as_ref().map(Bytes::len)))
                            .inspect_err(|err| error!(?err))?,
                    )
                    .value(
                        row.get_value(4)
                            .map(|o| o.as_blob().map(|blob| Bytes::copy_from_slice(blob)))
                            .inspect(|v| debug!(value_len = ?v.as_ref().map(Bytes::len)))
                            .inspect_err(|err| error!(?err))?,
                    );

                let mut headers = c
                    .query(
                        &sql_lookup("header_fetch.sql")?,
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                            base_offset,
                        ),
                    )
                    .await?;

                while let Some(header) = headers.next().await? {
                    let mut header_builder = Header::builder();

                    if let Some(k) = header
                        .get_value(0)
                        .map(|value| value.as_blob().cloned())
                        .inspect_err(|err| error!(?err))?
                    {
                        header_builder = header_builder.key(Bytes::from(k));
                    }

                    if let Some(v) = header
                        .get_value(1)
                        .map(|value| value.as_blob().cloned())
                        .inspect_err(|err| error!(?err))?
                    {
                        header_builder = header_builder.value(Bytes::from(v));
                    }

                    record_builder = record_builder.header(header_builder);
                }

                record_builder
            };

            let mut batch_builder = inflated::Batch::builder()
                .base_offset(base_offset)
                .attributes(
                    row.get_value(1)
                        .map(|value| {
                            value
                                .as_integer()
                                .copied()
                                .map(|attributes| attributes as i32)
                        })
                        .map(|attributes| attributes.unwrap_or(0))
                        .inspect_err(|err| error!(?err))? as i16,
                )
                .base_timestamp(
                    row.get_value(2)
                        .map_err(Error::from)
                        .and_then(LiteTimestamp::try_from)
                        .and_then(|system_time| to_timestamp(&system_time.0).map_err(Into::into))
                        .inspect_err(|err| error!(?err))?,
                )
                .producer_id(
                    row.get_value(6)
                        .map(|value| value.as_integer().copied())
                        .map(|producer_id| producer_id.unwrap_or(-1))
                        .inspect_err(|err| error!(?err))?,
                )
                .producer_epoch(i16::try_from(
                    row.get_value(7)
                        .map(|value| value.as_integer().copied())
                        .map(|producer_epoch| producer_epoch.unwrap_or(-1))
                        .inspect_err(|err| error!(?err))?,
                )?)
                .record(record_builder)
                .last_offset_delta(offset_delta);

            while let Some(row) = records.next().await? {
                let attributes = row
                    .get_value(1)
                    .map(|value| {
                        value
                            .as_integer()
                            .copied()
                            .map(|attributes| attributes as i16)
                    })
                    .map(|attributes| attributes.unwrap_or(0))
                    .inspect_err(|err| error!(?err))?;

                let producer_id = row
                    .get_value(6)
                    .map(|value| value.as_integer().copied())
                    .map(|producer_id| producer_id.unwrap_or(-1))
                    .inspect_err(|err| error!(?err))?;

                let producer_epoch = i16::try_from(
                    row.get_value(7)
                        .map(|value| value.as_integer().copied())
                        .map(|producer_epoch| producer_epoch.unwrap_or(-1))
                        .inspect_err(|err| error!(?err))?,
                )?;

                let timestamp = row
                    .get_value(2)
                    .map_err(Error::from)
                    .and_then(LiteTimestamp::try_from)
                    .and_then(|system_time| to_timestamp(&system_time.0).map_err(Into::into))
                    .inspect(|timestamp| debug!(?timestamp))
                    .inspect_err(|err| error!(?err))?;

                // A `LogAppendTime` record whose timestamp differs from the batch's starts a new
                // batch. A consumer reads one timestamp, `max_timestamp`, for every record of a
                // `LogAppendTime` batch. Each stored record holds the broker's time of its own
                // produce request, so one batch cannot hold two such times.
                if batch_builder.attributes != attributes
                    || batch_builder.producer_id != producer_id
                    || batch_builder.producer_epoch != producer_epoch
                    || (TimestampType::from(attributes) == TimestampType::LogAppendTime
                        && timestamp != batch_builder.base_timestamp)
                {
                    batches.push(
                        batch_builder
                            .max_timestamp_of_records()
                            .build()
                            .and_then(TryInto::try_into)?,
                    );

                    batch_builder = inflated::Batch::builder()
                        .base_offset(
                            row.get_value(0)
                                .map_err(Into::into)
                                .and_then(|value| {
                                    value
                                        .as_integer()
                                        .copied()
                                        .ok_or(Error::UnexpectedValue(value))
                                })
                                .inspect(|base_offset| debug!(base_offset))
                                .inspect_err(|err| error!(?err))?,
                        )
                        .base_timestamp(timestamp)
                        .attributes(attributes)
                        .producer_id(producer_id)
                        .producer_epoch(producer_epoch);
                }

                let offset = row
                    .get_value(0)
                    .map_err(Into::into)
                    .and_then(|value| {
                        value
                            .as_integer()
                            .copied()
                            .ok_or(Error::UnexpectedValue(value))
                    })
                    .inspect(|offset| debug!(offset))
                    .inspect_err(|err| error!(?err))?;

                let offset_delta = i32::try_from(offset - batch_builder.base_offset)?;

                let timestamp_delta = timestamp - batch_builder.base_timestamp;

                let record_builder = {
                    let mut record_builder = Record::builder()
                        .offset_delta(offset_delta)
                        .timestamp_delta(timestamp_delta)
                        .key(
                            row.get_value(3)
                                .map(|value| value.as_blob().cloned())
                                .map(|o| o.map(Bytes::from))
                                .inspect(|k| debug!(key_len = ?k.as_ref().map(Bytes::len)))
                                .inspect_err(|err| error!(?err))?,
                        )
                        .value(
                            row.get_value(4)
                                .map(|value| value.as_blob().cloned())
                                .map(|o| o.map(Bytes::from))
                                .inspect(|v| debug!(value_len = ?v.as_ref().map(Bytes::len)))
                                .inspect_err(|err| error!(?err))?,
                        );

                    let mut headers = c
                        .query(
                            &sql_lookup("header_fetch.sql")?,
                            (
                                self.cluster.as_str(),
                                topition.topic(),
                                topition.partition(),
                                offset,
                            ),
                        )
                        .await?;

                    while let Some(header) = headers.next().await? {
                        let mut header_builder = Header::builder();

                        if let Some(k) = header
                            .get_value(0)
                            .map(|value| value.as_blob().cloned())
                            .map(|o| o.map(Bytes::from))
                            .inspect_err(|err| error!(?err))?
                        {
                            header_builder = header_builder.key(k);
                        }

                        if let Some(v) = header
                            .get_value(1)
                            .map(|value| value.as_blob().cloned())
                            .map(|o| o.map(Bytes::from))
                            .inspect_err(|err| error!(?err))?
                        {
                            header_builder = header_builder.value(v);
                        }

                        record_builder = record_builder.header(header_builder);
                    }

                    record_builder
                };

                batch_builder = batch_builder
                    .record(record_builder)
                    .last_offset_delta(offset_delta);
            }

            batches.push(
                batch_builder
                    .max_timestamp_of_records()
                    .build()
                    .and_then(TryInto::try_into)?,
            );
        } else {
            batches.push(
                inflated::Batch::builder()
                    .build()
                    .and_then(TryInto::try_into)?,
            );
        }

        Ok(batches)
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        debug!(cluster = self.cluster, ?topition);
        let c = self.connection().await?;

        let row = self
            .prepare_query_one(
                &c,
                &sql_lookup("watermark_select.sql")?,
                (
                    self.cluster.as_str(),
                    topition.topic(),
                    topition.partition(),
                ),
            )
            .await
            .inspect_err(|err| error!(?topition, ?err))?;

        let log_start = row
            .get_value(0)
            .map(|value| value.as_integer().copied())
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or_default();

        let high_watermark = row
            .get_value(1)
            .map(|value| value.as_integer().copied())
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or_default();

        let last_stable = row
            .get_value(2)
            .map(|value| value.as_integer().copied())
            .inspect_err(|err| error!(?topition, ?err))?
            .unwrap_or(high_watermark);

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
                    &sql_lookup("topition_select.sql")?,
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
                            &sql_lookup("consumer_group_insert.sql")?,
                            (self.cluster.as_str(), group),
                        )
                        .await?;
                    debug!(rows);

                    cg_inserted = true;
                }

                let rows = self
                    .prepare_execute(
                        &tx,
                        &sql_lookup("consumer_offset_insert.sql")?,
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                            group,
                            offset.offset,
                            offset.leader_epoch,
                            offset.timestamp.map(LiteTimestamp),
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

        let mut rows = c
            .query(
                &sql_lookup(consumer_offset_select_by_group::QUERY)?,
                (self.cluster.as_str(), group_id),
            )
            .await?;

        let columns = ResultColumns::turso(consumer_offset_select_by_group::QUERY, &rows);
        let topic = columns.index(consumer_offset_select_by_group::TOPIC)?;
        let partition = columns.index(consumer_offset_select_by_group::PARTITION)?;
        let committed_offset = columns.index(consumer_offset_select_by_group::COMMITTED_OFFSET)?;
        let leader_epoch = columns.index(consumer_offset_select_by_group::LEADER_EPOCH)?;
        let metadata = columns.index(consumer_offset_select_by_group::METADATA)?;

        while let Some(row) = rows.next().await? {
            let topic = row.get_value(topic).map_err(Into::into).and_then(|value| {
                value
                    .as_text()
                    .cloned()
                    .ok_or(Error::UnexpectedValue(value))
            })?;

            let partition = row
                .get_value(partition)
                .map_err(Into::into)
                .and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .map(|partition| partition as i32)
                        .ok_or(Error::UnexpectedValue(value))
                })?;

            let committed = CommittedOffset {
                offset: integer_value(&row, committed_offset)?,
                leader_epoch: leader_epoch_value(&row, leader_epoch)?,
                metadata: optional_text_value(&row, metadata)?,
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

        let mut rows = c
            .query(
                &sql_lookup(txn_offset_commit_tp_select_pending::QUERY)?,
                (self.cluster.as_str(), group_id),
            )
            .await
            .inspect_err(|err| error!(?err, cluster = self.cluster, group_id))?;

        let columns = ResultColumns::turso(txn_offset_commit_tp_select_pending::QUERY, &rows);
        let topic = columns.index(txn_offset_commit_tp_select_pending::TOPIC)?;
        let partition = columns.index(txn_offset_commit_tp_select_pending::PARTITION)?;

        let mut pending = BTreeSet::new();

        while let Some(row) = rows.next().await? {
            let topic = row.get_value(topic).map_err(Into::into).and_then(|value| {
                value
                    .as_text()
                    .cloned()
                    .ok_or(Error::UnexpectedValue(value))
            })?;

            let partition = row
                .get_value(partition)
                .map_err(Into::into)
                .and_then(|value| {
                    value
                        .as_integer()
                        .copied()
                        .map(|partition| partition as i32)
                        .ok_or(Error::UnexpectedValue(value))
                })?;

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
            let mut rows = c
                .query(
                    &sql_lookup(consumer_offset_select::QUERY)?,
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

            let columns = ResultColumns::turso(consumer_offset_select::QUERY, &rows);
            let committed_offset = columns.index(consumer_offset_select::COMMITTED_OFFSET)?;
            let leader_epoch = columns.index(consumer_offset_select::LEADER_EPOCH)?;
            let metadata = columns.index(consumer_offset_select::METADATA)?;

            let committed = rows
                .next()
                .await
                .map_err(Error::from)
                .and_then(|row| {
                    row.map_or(Ok(CommittedOffset::uncommitted()), |row| {
                        Ok(CommittedOffset {
                            offset: integer_value(&row, committed_offset)?,
                            leader_epoch: leader_epoch_value(&row, leader_epoch)?,
                            metadata: optional_text_value(&row, metadata)?,
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
                (ListOffsetRequest::Earliest, _) => sql_lookup("list_earliest_offset.sql")?,
                (ListOffsetRequest::Latest, IsolationLevel::ReadCommitted) => {
                    sql_lookup("list_latest_offset_committed.sql")?
                }
                (ListOffsetRequest::Latest, IsolationLevel::ReadUncommitted) => {
                    sql_lookup("list_latest_offset_uncommitted.sql")?
                }
                (ListOffsetRequest::Timestamp(_), _) => {
                    sql_lookup("list_latest_offset_timestamp.sql")?
                }
            };

            debug!(?query);

            let list_offset = match offset_type {
                ListOffsetRequest::Earliest | ListOffsetRequest::Latest => self
                    .prepare_query_opt(
                        &c,
                        query.as_str(),
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                        ),
                    )
                    .await
                    .inspect_err(|err| error!(?err, cluster = self.cluster, ?topition)),

                ListOffsetRequest::Timestamp(timestamp) => self
                    .prepare_query_opt(
                        &c,
                        query.as_str(),
                        (
                            self.cluster.as_str(),
                            topition.topic(),
                            topition.partition(),
                            LiteTimestamp::from(timestamp),
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
                |row| {
                    debug!(?row);

                    row.get_value(0)
                        .map_err(Into::into)
                        .map(|value| value.as_integer().copied())
                        .and_then(|offset| {
                            row.get_value(1)
                                .map_err(Into::into)
                                .and_then(|value| match value {
                                    Value::Null => Ok(None),
                                    value => LiteTimestamp::try_from(value)
                                        .map(SystemTime::from)
                                        .map(Some),
                                })
                                .map(|timestamp| {
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
            Some(topics) if !topics.is_empty() => {
                let mut responses = vec![];

                for topic in topics {
                    responses.push(match topic {
                        TopicId::Name(name) => {
                            let mut rows = c
                                .query(
                                    &sql_lookup("topic_select_name.sql")?,
                                    (self.cluster.as_str(), name.as_str()),
                                )
                                .await?;

                            match rows.next().await.inspect_err(|err| error!(?err)) {
                                Ok(Some(row)) => {
                                    let error_code = ErrorCode::None.into();

                                    let topic_id = row.get_value(0).map_err(Error::from).and_then(
                                        |value| {
                                            value
                                                .as_text()
                                                .map(|value| {
                                                    Uuid::parse_str(value)
                                                        .map(|uuid| uuid.into_bytes())
                                                        .map_err(Into::into)
                                                })
                                                .transpose()
                                        },
                                    )?;

                                    let name =
                                        row.get_value(1).map(|value| value.as_text().cloned())?;

                                    let is_internal =
                                        row.get_value(2).map_err(Into::into).and_then(|value| {
                                            value
                                                .as_integer()
                                                .map(|i| match *i {
                                                    0 => Ok(false),
                                                    1 => Ok(true),
                                                    _ => Err(Error::UnexpectedValue(value.clone())),
                                                })
                                                .transpose()
                                        })?;

                                    let partitions =
                                        row.get_value(3).map_err(Into::into).and_then(|value| {
                                            value
                                                .as_integer()
                                                .map(|i| *i as i32)
                                                .ok_or(Error::UnexpectedValue(value))
                                        })?;

                                    let replication_factor =
                                        row.get_value(4).map_err(Into::into).and_then(|value| {
                                            value
                                                .as_integer()
                                                .map(|i| *i as i32)
                                                .ok_or(Error::UnexpectedValue(value))
                                        })?;

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
                            let mut rows = c
                                .query(
                                    &sql_lookup("topic_select_uuid.sql")?,
                                    (self.cluster.as_str(), id.to_string().as_str()),
                                )
                                .await?;

                            match rows.next().await {
                                Ok(Some(row)) => {
                                    let error_code = ErrorCode::None.into();
                                    let topic_id = row.get_value(0).map_err(Error::from).and_then(
                                        |value| {
                                            value
                                                .as_text()
                                                .map(|value| {
                                                    Uuid::parse_str(value)
                                                        .map(|uuid| uuid.into_bytes())
                                                        .map_err(Into::into)
                                                })
                                                .transpose()
                                        },
                                    )?;

                                    let name =
                                        row.get_value(1).map(|value| value.as_text().cloned())?;

                                    let is_internal =
                                        row.get_value(2).map_err(Into::into).and_then(|value| {
                                            value
                                                .as_integer()
                                                .map(|i| match *i {
                                                    0 => Ok(false),
                                                    1 => Ok(true),
                                                    _ => Err(Error::UnexpectedValue(value.clone())),
                                                })
                                                .transpose()
                                        })?;

                                    let partitions =
                                        row.get_value(3).map_err(Into::into).and_then(|value| {
                                            value
                                                .as_integer()
                                                .map(|i| *i as i32)
                                                .ok_or(Error::UnexpectedValue(value))
                                        })?;

                                    let replication_factor =
                                        row.get_value(4).map_err(Into::into).and_then(|value| {
                                            value
                                                .as_integer()
                                                .map(|i| *i as i32)
                                                .ok_or(Error::UnexpectedValue(value))
                                        })?;

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

                let mut rows = c
                    .query(
                        &sql_lookup("topic_by_cluster.sql")?,
                        &[self.cluster.as_str()],
                    )
                    .await?;

                while let Some(row) = rows.next().await? {
                    let error_code = ErrorCode::None.into();
                    let topic_id = row.get_value(0).map_err(Error::from).and_then(|value| {
                        value
                            .as_text()
                            .map(|value| {
                                Uuid::parse_str(value)
                                    .map(|uuid| uuid.into_bytes())
                                    .map_err(Into::into)
                            })
                            .transpose()
                    })?;

                    let name = row.get_value(1).map(|value| value.as_text().cloned())?;

                    let is_internal = row.get_value(2).map_err(Into::into).and_then(|value| {
                        value
                            .as_integer()
                            .map(|i| match *i {
                                0 => Ok(false),
                                1 => Ok(true),
                                _ => Err(Error::UnexpectedValue(value.clone())),
                            })
                            .transpose()
                    })?;

                    let partitions = row.get_value(3).map_err(Into::into).and_then(|value| {
                        value
                            .as_integer()
                            .map(|i| *i as i32)
                            .ok_or(Error::UnexpectedValue(value))
                    })?;

                    let replication_factor =
                        row.get_value(4).map_err(Into::into).and_then(|value| {
                            value
                                .as_integer()
                                .map(|i| *i as i32)
                                .ok_or(Error::UnexpectedValue(value))
                        })?;

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
                &sql_lookup("topic_select.sql")?,
                (self.cluster.as_str(), name),
            )
            .await?;

        if rows.next().await?.is_some() {
            let mut rows = c
                .query(
                    &sql_lookup("topic_configuration_select.sql")?,
                    (self.cluster.as_str(), name),
                )
                .await?;

            let mut configs = vec![];

            while let Some(row) = rows.next().await? {
                let name = row
                    .get_value(0)
                    .map_err(Into::into)
                    .and_then(|value| {
                        value
                            .as_text()
                            .cloned()
                            .ok_or(Error::UnexpectedValue(value))
                    })
                    .inspect_err(|err| error!(?err))?;

                let value = row
                    .get_value(1)
                    .map(|value| value.as_text().cloned())
                    .inspect_err(|err| error!(?err))?;

                configs.push(
                    DescribeConfigsResourceResult::default()
                        .name(name)
                        .value(value)
                        .read_only(false)
                        .is_default(None)
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
                    match self
                        .prepare_query_opt(
                            &c,
                            &sql_lookup("topic_select_name.sql")?,
                            (self.cluster.as_str(), name.as_str()),
                        )
                        .await
                        .inspect_err(|err| error!(?err))
                    {
                        Ok(Some(row)) => {
                            let topic_id =
                                row.get_value(0).map_err(Error::from).and_then(|value| {
                                    value.as_text().map_or(
                                        Err(Error::UnexpectedValue(value.clone())),
                                        |value| {
                                            Uuid::parse_str(value)
                                                .map(|uuid| uuid.into_bytes())
                                                .map_err(Into::into)
                                        },
                                    )
                                })?;

                            let name = row.get_value(1).map(|value| value.as_text().cloned())?;

                            let is_internal =
                                row.get_value(2).map_err(Into::into).and_then(|value| {
                                    value
                                        .as_integer()
                                        .map(|i| match *i {
                                            0 => Ok(false),
                                            1 => Ok(true),
                                            _ => Err(Error::UnexpectedValue(value.clone())),
                                        })
                                        .transpose()
                                })?;

                            let partitions =
                                row.get_value(3).map_err(Into::into).and_then(|value| {
                                    value
                                        .as_integer()
                                        .map(|i| *i as i32)
                                        .ok_or(Error::UnexpectedValue(value))
                                })?;

                            let replication_factor =
                                row.get_value(4).map_err(Into::into).and_then(|value| {
                                    value
                                        .as_integer()
                                        .map(|i| *i as i32)
                                        .ok_or(Error::UnexpectedValue(value))
                                })?;

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
                        .prepare_query_one(
                            &c,
                            &sql_lookup("topic_select_uuid.sql")?,
                            (self.cluster.as_str(), id.to_string().as_str()),
                        )
                        .await
                    {
                        Ok(row) => {
                            let topic_id =
                                row.get_value(0).map_err(Error::from).and_then(|value| {
                                    value.as_text().map_or(
                                        Err(Error::UnexpectedValue(value.clone())),
                                        |value| {
                                            Uuid::parse_str(value)
                                                .map(|uuid| uuid.into_bytes())
                                                .map_err(Into::into)
                                        },
                                    )
                                })?;

                            let name = row.get_value(1).map(|value| value.as_text().cloned())?;

                            let is_internal =
                                row.get_value(2).map_err(Into::into).and_then(|value| {
                                    value
                                        .as_integer()
                                        .map(|i| match *i {
                                            0 => Ok(false),
                                            1 => Ok(true),
                                            _ => Err(Error::UnexpectedValue(value.clone())),
                                        })
                                        .transpose()
                                })?;

                            let partitions =
                                row.get_value(3).map_err(Into::into).and_then(|value| {
                                    value
                                        .as_integer()
                                        .map(|i| *i as i32)
                                        .ok_or(Error::UnexpectedValue(value))
                                })?;

                            let replication_factor =
                                row.get_value(4).map_err(Into::into).and_then(|value| {
                                    value
                                        .as_integer()
                                        .map(|i| *i as i32)
                                        .ok_or(Error::UnexpectedValue(value))
                                })?;

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

    async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
        let c = self.connection().await.inspect_err(|err| error!(?err))?;

        let mut listed_groups = vec![];

        let mut rows = c
            .query(
                &sql_lookup(consumer_group_select::QUERY)?,
                &[self.cluster.as_str()],
            )
            .await?;

        let columns = ResultColumns::turso(consumer_group_select::QUERY, &rows);
        let name = columns.index(consumer_group_select::NAME)?;
        let detail = columns.index(consumer_group_select::DETAIL)?;

        while let Some(row) = rows.next().await? {
            let group_id = row.get_value(name).map_err(Into::into).and_then(|value| {
                value
                    .as_text()
                    .cloned()
                    .ok_or(Error::UnexpectedValue(value.clone()))
            })?;

            let detail = row.get_value(detail).map_err(Error::from)?;

            let stored = if detail.is_null() {
                None
            } else {
                Some(
                    detail
                        .as_text()
                        .ok_or(Error::UnexpectedValue(detail.clone()))
                        .and_then(|text| serde_json::from_str(text).map_err(Error::from)),
                )
            };

            listed_groups.push(NamedGroupDetail::decoded(group_id, stored));
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
                .prepare(&sql_lookup("consumer_offset_delete_by_cg.sql")?)
                .await
                .inspect_err(|err| error!(?err))?;

            let mut group_detail = c
                .prepare(&sql_lookup("consumer_group_detail_delete_by_cg.sql")?)
                .await
                .inspect_err(|err| error!(?err))?;

            let mut group = c
                .prepare(&sql_lookup("consumer_group_delete.sql")?)
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
                if let Some(row) = self
                    .prepare_query_opt(
                        &c,
                        &sql_lookup("consumer_group_select_by_name.sql")?,
                        (self.cluster.as_str(), group_id.as_str()),
                    )
                    .await
                    .inspect_err(|err| error!(?err, group_id))?
                {
                    // A group row with no detail row (e.g. a group that only ever
                    // committed offsets, never a JoinGroup) joins to a NULL
                    // `detail` column here: that group exists and is empty, not
                    // an error.
                    let value = row
                        .get_value(1)
                        .map_err(Error::from)
                        .inspect_err(|err| error!(?err, group_id))?;

                    let current = if value.is_null() {
                        GroupDetail::default()
                    } else {
                        value
                            .as_text()
                            .cloned()
                            .ok_or(Error::UnexpectedValue(value.clone()))
                            .and_then(|s| {
                                serde_json::from_str::<GroupDetail>(&s).map_err(Into::into)
                            })
                            .inspect(|current| debug!(?current))
                            .inspect_err(|err| error!(?err, group_id))?
                    };

                    results.push(NamedGroupDetail::found(group_id.into(), current));
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

        self.prepare_query_opt(
            &c,
            &sql_lookup("consumer_group_detail.sql")?,
            (group_id, self.cluster.as_str()),
        )
        .await
        .map_err(Error::from)
        .inspect_err(|err| error!(?err, group_id))?
        .as_ref()
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
                &sql_lookup("consumer_group_insert.sql")?,
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

        let outcome = if let Some(row) = self
            .prepare_query_opt(
                &tx,
                &sql_lookup("consumer_group_detail_insert.sql")?,
                (
                    self.cluster.as_str(),
                    group_id,
                    existing_e_tag.to_string().as_str(),
                    new_e_tag.to_string().as_str(),
                    detail.to_string().as_str(),
                ),
            )
            .await
            .inspect(|row| debug!(?row))
            .inspect_err(|err| error!(?err))?
        {
            row.get_value(2)
                .map_err(Error::from)
                .and_then(|value| {
                    value
                        .as_text()
                        .ok_or(Error::UnexpectedValue(value.clone()))
                        .and_then(|str| Uuid::parse_str(str).map_err(Into::into))
                })
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
                .prepare_query_one(
                    &tx,
                    &sql_lookup("consumer_group_detail.sql")?,
                    (group_id, self.cluster.as_str()),
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

                if let Some(row) = self
                    .prepare_query_opt(
                        &tx,
                        &sql_lookup("producer_epoch_for_current_txn.sql")?,
                        (self.cluster.as_str(), transaction_id),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?
                {
                    let id = row
                        .get_value(0)
                        .map_err(Into::into)
                        .and_then(|value| {
                            value
                                .as_integer()
                                .copied()
                                .ok_or(Error::UnexpectedValue(value))
                        })
                        .inspect_err(|err| error!(?err))?;
                    let epoch = row
                        .get_value(1)
                        .map_err(Into::into)
                        .and_then(|value| {
                            value
                                .as_integer()
                                .copied()
                                .ok_or(Error::UnexpectedValue(value))
                                .and_then(|i| i16::try_from(i).map_err(Error::from))
                        })
                        .inspect_err(|err| error!(?err, transaction_id, id))?;
                    let status = row
                        .get_value(2)
                        .map_err(Error::from)
                        .and_then(|value| {
                            value.as_text().map_or(Ok(None), |status| {
                                TxnState::from_str(status.as_str()).map(Some)
                            })
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

                let (producer, epoch) = if let Some(row) = self
                    .prepare_query_opt(
                        &tx,
                        &sql_lookup("txn_select_name.sql")?,
                        (self.cluster.as_str(), transaction_id),
                    )
                    .await
                    .inspect_err(|err| error!(?err))?
                {
                    let producer = row
                        .get_value(0)
                        .map_err(Into::into)
                        .and_then(|value| {
                            value
                                .as_integer()
                                .cloned()
                                .ok_or(Error::UnexpectedValue(value.clone()))
                        })
                        .inspect_err(|err| error!(?err))
                        .inspect(|producer| debug!(producer))?;

                    // A bump gives the producer its newest epoch plus one, so the limit
                    // applies to that epoch, not to the newest epoch with a transaction.
                    let newest = self
                        .prepare_query_opt(
                            &tx,
                            &sql_lookup("producer_epoch_current_for_producer.sql")?,
                            (self.cluster.as_str(), producer),
                        )
                        .await
                        .inspect_err(|err| error!(?err))?
                        .map(|row| epoch_value(&row, 0))
                        .transpose()
                        .inspect_err(|err| error!(?err, transaction_id, producer))?;

                    if newest.is_some_and(epoch_is_exhausted) {
                        // The status after the rollback above, if it ran.
                        let status = self
                            .prepare_query_opt(
                                &tx,
                                &sql_lookup("producer_epoch_for_current_txn.sql")?,
                                (self.cluster.as_str(), transaction_id),
                            )
                            .await
                            .map_err(Error::from)
                            .and_then(|row| {
                                row.map(|row| row.get_value(2).map_err(Error::from))
                                    .transpose()
                            })
                            .and_then(|value| {
                                value
                                    .as_ref()
                                    .and_then(Value::as_text)
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
                                        &sql_lookup("producer_epoch_insert.sql")?,
                                        (self.cluster.as_str(), producer),
                                    )
                                    .await
                                    .inspect_err(|err| error!(self.cluster, producer, ?err))?;
                            }

                            let error = match tx.commit().await.inspect_err(|err| {
                                error!(?err, cluster = self.cluster, transaction_id, producer)
                            }) {
                                Ok(()) => ErrorCode::ConcurrentTransactions,
                                Err(_) => ErrorCode::UnknownServerError,
                            };

                            return Ok(ProducerIdResponse::failed(error));
                        }

                        let previous = producer;

                        let producer = self
                            .prepare_query_one(
                                &tx,
                                &sql_lookup("producer_insert.sql")?,
                                &[self.cluster.as_str()],
                            )
                            .await
                            .map_err(Error::from)
                            .and_then(|row| integer_value(&row, 0))
                            .inspect_err(|err| error!(?err))?;

                        let epoch = self
                            .prepare_query_one(
                                &tx,
                                &sql_lookup("producer_epoch_insert.sql")?,
                                (self.cluster.as_str(), producer),
                            )
                            .await
                            .map_err(Error::from)
                            .and_then(|row| epoch_value(&row, 0))
                            .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                        // A produce with an epoch below the producer's newest is fenced, so
                        // epoch i16::MAX fences the previous producer ID. A producer at epoch
                        // i16::MAX gets no new epoch, and is fenced already.
                        _ = self
                            .prepare_query_opt(
                                &tx,
                                &sql_lookup("producer_epoch_insert.sql")?,
                                (self.cluster.as_str(), previous),
                            )
                            .await
                            .inspect_err(|err| error!(self.cluster, previous, ?err))?;

                        let updated = self
                            .prepare_execute(
                                &tx,
                                &sql_lookup("txn_update_producer.sql")?,
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
                            .prepare_query_one(
                                &tx,
                                &sql_lookup("producer_epoch_insert.sql")?,
                                (self.cluster.as_str(), producer),
                            )
                            .await
                            .map_err(Error::from)
                            .and_then(|row| epoch_value(&row, 0))
                            .inspect(|epoch| debug!(epoch))
                            .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                        (producer, epoch)
                    }
                } else {
                    let row = self
                        .prepare_query_one(
                            &tx,
                            &sql_lookup("producer_insert.sql")?,
                            &[self.cluster.as_str()],
                        )
                        .await
                        .inspect_err(|err| error!(?err))?;

                    let producer = row
                        .get_value(0)
                        .map_err(Into::into)
                        .and_then(|value| {
                            value
                                .as_integer()
                                .copied()
                                .ok_or(Error::UnexpectedValue(value))
                        })
                        .inspect_err(|err| error!(?err))?;

                    let row = self
                        .prepare_query_one(
                            &tx,
                            &sql_lookup("producer_epoch_insert.sql")?,
                            (self.cluster.as_str(), producer),
                        )
                        .await
                        .inspect_err(|err| error!(self.cluster, producer, ?err))?;

                    let epoch = row.get_value(0).map_err(Into::into).and_then(|value| {
                        value
                            .as_integer()
                            .copied()
                            .ok_or(Error::UnexpectedValue(value))
                            .and_then(|i| i16::try_from(i).map_err(Error::from))
                    })?;

                    assert_eq!(
                        1,
                        self.prepare_execute(
                            &tx,
                            &sql_lookup("txn_insert.sql")?,
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
                        &sql_lookup("txn_detail_insert.sql")?,
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
                    Err(_) => ErrorCode::UnknownServerError,
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

                let mut rows = tx
                    .query(
                        &sql_lookup("producer_insert.sql")?,
                        &[self.cluster.as_str()],
                    )
                    .await?;

                if let Some(row) = rows.next().await? {
                    let producer = row
                        .get_value(0)
                        .map_err(Into::into)
                        .and_then(|value| {
                            value
                                .as_integer()
                                .copied()
                                .ok_or(Error::UnexpectedValue(value))
                        })
                        .inspect(|producer| debug!(producer))
                        .inspect_err(|err| error!(?err))?;

                    while let Some(row) = rows
                        .next()
                        .await
                        .inspect(|row| debug!(?row))
                        .inspect_err(|err| error!(?err))?
                    {
                        debug!(?row)
                    }

                    let mut rows = tx
                        .query(
                            &sql_lookup("producer_epoch_insert.sql")?,
                            (self.cluster.as_str(), producer),
                        )
                        .await
                        .inspect_err(|err| error!(?err, cluster = self.cluster, producer))?;

                    if let Some(row) = rows.next().await? {
                        let epoch = row
                            .get_value(0)
                            .map_err(Into::into)
                            .and_then(|value| {
                                value
                                    .as_integer()
                                    .copied()
                                    .ok_or(Error::UnexpectedValue(value))
                                    .and_then(|i| i16::try_from(i).map_err(Error::from))
                            })
                            .inspect(|epoch| debug!(epoch))?;

                        while let Some(row) = rows.next().await? {
                            debug!(?row)
                        }

                        let error = match tx
                            .commit()
                            .await
                            .inspect_err(|err| error!(?err, ?transaction_id, producer, epoch))
                        {
                            Ok(()) => ErrorCode::None,
                            Err(_) => ErrorCode::UnknownServerError,
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
                                &sql_lookup("txn_topition_insert.sql")?,
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
                        &sql_lookup("txn_detail_update_started_at.sql")?,
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
            .prepare_query_opt(
                &tx,
                &sql_lookup("producer_epoch_for_current_txn.sql")?,
                (self.cluster.as_str(), offsets.transaction_id.as_str()),
            )
            .await
            .inspect_err(|err| error!(?err))?
        {
            let producer_id = row
                .get_value(0)
                .map(|value| value.as_integer().copied())
                .inspect_err(|err| error!(?err))?;

            let epoch = row
                .get_value(1)
                .map_err(Error::from)
                .and_then(|value| {
                    value
                        .as_integer()
                        .map(|i| i16::try_from(*i))
                        .transpose()
                        .map_err(Error::from)
                })
                .inspect_err(|err| error!(?err))?;

            (producer_id, epoch)
        } else {
            (None, None)
        };

        _ = self
            .prepare_execute(
                &tx,
                &sql_lookup("consumer_group_insert.sql")?,
                (self.cluster.as_str(), offsets.group_id.as_str()),
            )
            .await?;

        debug!(?producer_id, ?producer_epoch);

        _ = self
            .prepare_execute(
                &tx,
                &sql_lookup("txn_offset_commit_insert.sql")?,
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
                                &sql_lookup("txn_offset_commit_tp_insert.sql")?,
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
        let _ = c.query("ping.sql", ()).await?;
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct LiteTimestamp(SystemTime);

impl Deref for LiteTimestamp {
    type Target = SystemTime;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<SystemTime> for LiteTimestamp {
    fn from(value: SystemTime) -> Self {
        Self(value)
    }
}

impl From<&SystemTime> for LiteTimestamp {
    fn from(value: &SystemTime) -> Self {
        Self(*value)
    }
}

impl From<LiteTimestamp> for SystemTime {
    fn from(value: LiteTimestamp) -> Self {
        value.0
    }
}

impl From<LiteTimestamp> for Value {
    fn from(value: LiteTimestamp) -> Self {
        Value::Integer(to_timestamp(&value.0).unwrap_or_default())
    }
}

impl TryFrom<Value> for LiteTimestamp {
    type Error = Error;

    fn try_from(value: Value) -> result::Result<Self, Self::Error> {
        match value {
            Value::Integer(timestamp) => to_system_time(timestamp)
                .map_err(Into::into)
                .map(LiteTimestamp::from),

            Value::Text(text) => NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S%.f")
                .map(|date_time| date_time.and_utc())
                .inspect(|dt| debug!(?dt))
                .map(SystemTime::from)
                .map(LiteTimestamp::from)
                .map_err(Into::into),

            Value::Real(_) | Value::Null | Value::Blob(_) => Err(Error::UnexpectedValue(value)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use tempfile::{TempDir, tempdir, tempdir_in};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use super::*;

    #[test]
    fn timestamp_from_null_is_an_error() {
        assert_matches!(
            LiteTimestamp::try_from(Value::Null),
            Err(Error::UnexpectedValue(Value::Null))
        );
    }

    #[test]
    fn timestamp_round_trips_through_a_value() -> Result<()> {
        let timestamp = LiteTimestamp::from(SystemTime::UNIX_EPOCH + Duration::from_millis(1_234));

        assert_eq!(timestamp, LiteTimestamp::try_from(Value::from(timestamp))?);

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
        let _timestamp = LiteTimestamp::try_from(row.get_value(0)?)?;

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

    /// Returns an engine on a new database in `temp_dir`, with a registered broker.
    /// `build()` opens the URL path relative to the current directory, so `temp_dir` must
    /// be in it.
    async fn registered_engine(temp_dir: &TempDir) -> Result<Engine> {
        let current_dir = env::current_dir()?;
        let relative = temp_dir
            .path()
            .strip_prefix(&current_dir)
            .map_err(|err| Error::Message(err.to_string()))?
            .join("nisshi.db");

        let cluster = "nisshi";
        let node = 12321;

        let engine = Engine::builder()
            .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
            .cluster(cluster.to_owned())
            .storage(Url::parse(&format!("file:///{}", relative.display()))?)
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
            Err(Error::TryFromInt(_))
        ));

        Ok(())
    }
}
