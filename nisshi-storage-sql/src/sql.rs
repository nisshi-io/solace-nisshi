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

/// Declares `Row`, one row of a query's result, and `RowReader`, which reads it, in the query's
/// module. The macro takes one line for each column, such as `low: Option<i64>`: the field and its
/// type. The field's name is the column's name in the query's result, so a query that needs another
/// Rust name renames the column with `as`.
///
/// `RowReader::new` finds each column's index by its name once, before the first row, and
/// `RowReader::read_row` reads each row by those indexes through any
/// [`DriverRow`](driver::DriverRow). Each field's type implements [`FromColumn`](driver::FromColumn).
/// A read error names the query and the column. The macro implements [`QueryRow`] for `Row`, so
/// that an engine's query helpers read each row of a result as `Row`.
macro_rules! sql_row {
    (
        $(#[$row_attribute:meta])*
        pub(crate) struct Row {
            $(
                $(#[$field_attribute:meta])*
                $field:ident: $type:ty
            ),+ $(,)?
        }
    ) => {
        $(#[$row_attribute])*
        pub(crate) struct Row {
            $(
                $(#[$field_attribute])*
                pub(crate) $field: $type,
            )+
        }

        /// Reads each [`Row`] of the query's result by the index of each of its columns.
        #[derive(Clone, Debug)]
        pub(crate) struct RowReader {
            query: &'static str,
            $($field: usize,)+
        }

        impl RowReader {
            /// Returns a reader for the result with `columns`, or
            /// [`Error::MissingColumn`](nisshi_storage::Error::MissingColumn) for the first
            /// column of [`Row`] that the result does not have.
            pub(crate) fn new(
                columns: &$crate::sql::result_columns::ResultColumns,
            ) -> ::nisshi_storage::Result<Self> {
                Ok(Self {
                    query: columns.query(),
                    $($field: columns.index(stringify!($field))?,)+
                })
            }

            /// Reads `row`, or returns [`Error::UnreadableColumn`](nisshi_storage::Error::UnreadableColumn) for the
            /// first column that the reader can't read as its field's type.
            pub(crate) fn read_row<R>(&self, row: &R) -> ::nisshi_storage::Result<Row>
            where
                R: $crate::sql::driver::DriverRow,
            {
                Ok(Row {
                    $(
                        $field: <$type as $crate::sql::driver::FromColumn>::from_column(
                            row,
                            self.$field,
                        )
                            .map_err(|source| ::nisshi_storage::Error::UnreadableColumn {
                                query: self.query,
                                column: stringify!($field),
                                source: Box::new(source),
                            })?,
                    )+
                })
            }
        }

        impl $crate::sql::QueryRow for Row {
            type Reader = RowReader;

            fn reader(
                columns: &$crate::sql::result_columns::ResultColumns,
            ) -> ::nisshi_storage::Result<RowReader> {
                RowReader::new(columns)
            }

            fn read<R>(reader: &RowReader, row: &R) -> ::nisshi_storage::Result<Self>
            where
                R: $crate::sql::driver::DriverRow,
            {
                reader.read_row(row)
            }
        }
    };
}

/// A query whose result an engine reads as rows of `R`: the query's name in the SQL cache, typed
/// with its row.
///
/// The query's module declares it as `QUERY`, beside the row. An engine's query helpers take a
/// `Query<T>` and return rows of `T`, so a call site can't pass one query with another query's
/// row, which compiles and reads the wrong columns when both have columns of the same names.
pub(crate) struct Query<R> {
    key: &'static str,
    row: PhantomData<fn() -> R>,
}

impl<R> Query<R> {
    /// Returns the query that the SQL cache holds under `key`.
    pub(crate) const fn new(key: &'static str) -> Self {
        Self {
            key,
            row: PhantomData,
        }
    }

    /// Returns the query's name in the SQL cache.
    pub(crate) const fn key(&self) -> &'static str {
        self.key
    }
}

// A derive would require `R: Clone` and `R: Copy`, which a row is not.
impl<R> Clone for Query<R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<R> Copy for Query<R> {}

impl<R> fmt::Debug for Query<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Query").field(&self.key).finish()
    }
}

/// A row of one query's result, which [`sql_row!`] declares, read by column name. An engine's query
/// helpers take the row's [`Query`], as in `query_first(topic_select_name::QUERY, params)`, and
/// return each row read as it.
pub(crate) trait QueryRow: Sized {
    /// The reader that finds each column of a result by its name, before the first row.
    type Reader;

    /// Returns the reader for the result with `columns`.
    fn reader(columns: &result_columns::ResultColumns) -> Result<Self::Reader>;

    /// Reads `row` with `reader`.
    fn read<R: driver::DriverRow>(reader: &Self::Reader, row: &R) -> Result<Self>;
}

/// Returns the kind of `error` when a row reader returned it: `missing_column`,
/// `duplicate_column` or `unreadable_column`. Returns `None` for any other error.
///
/// Each engine counts a column error in its SQL error counter with this kind. The query has run
/// by then, so the driver reports no error, and a count of the driver's errors alone shows a
/// query that doesn't match its row, or a corrupt stored row, as healthy.
pub(crate) fn column_error_kind(error: &Error) -> Option<&'static str> {
    match error {
        Error::MissingColumn { .. } => Some("missing_column"),
        Error::DuplicateColumn { .. } => Some("duplicate_column"),
        Error::UnreadableColumn { .. } => Some("unreadable_column"),
        _ => None,
    }
}

/// Counts each column error of one query in an engine's SQL error counter, with the labels that
/// the engine gives the query and the error's [`column_error_kind`] as `kind`.
pub(crate) struct ColumnErrorCount {
    counter: &'static Counter<u64>,
    attributes: Vec<KeyValue>,
}

impl ColumnErrorCount {
    /// Returns a count in `counter`, with the query's labels `attributes`.
    pub(crate) fn new(counter: &'static Counter<u64>, attributes: Vec<KeyValue>) -> Self {
        Self {
            counter,
            attributes,
        }
    }

    /// Counts `error` when a row reader returned it, and ignores any other error.
    pub(crate) fn record(&self, error: &Error) {
        if let Some(kind) = column_error_kind(error) {
            let mut attributes = self.attributes.clone();
            attributes.push(KeyValue::new("kind", kind));
            self.counter.add(1, &attributes);
        }
    }
}

/// A cursor over one query's result on libSQL or Turso, which returns each of its rows as a
/// [`QueryRow`], in order. It reads a row only when the caller reaches it.
pub(crate) struct QueryRowCursor<R: driver::DriverRowCursor, T: QueryRow> {
    driver_cursor: R,
    reader: T::Reader,
    column_errors: ColumnErrorCount,
}

impl<R: driver::DriverRowCursor, T: QueryRow> QueryRowCursor<R, T> {
    /// Returns a cursor over the result with `columns`, which reads each row from
    /// `driver_cursor` and counts each column error in `column_errors`.
    pub(crate) fn new(
        columns: &result_columns::ResultColumns,
        driver_cursor: R,
        column_errors: ColumnErrorCount,
    ) -> Result<Self> {
        let reader = T::reader(columns).inspect_err(|err| column_errors.record(err))?;

        Ok(Self {
            driver_cursor,
            reader,
            column_errors,
        })
    }

    /// Returns the next row as `T`, or `None` after the last row.
    pub(crate) async fn next(&mut self) -> Result<Option<T>> {
        self.driver_cursor
            .next()
            .await?
            .map(|row| T::read(&self.reader, &row))
            .transpose()
            .inspect_err(|err| self.column_errors.record(err))
    }
}

pub(crate) mod consumer_group_select;
pub(crate) mod consumer_offset_select;
pub(crate) mod consumer_offset_select_by_group;
pub(crate) mod driver;
pub(crate) mod pg_record_fetch;
pub(crate) mod producer_epochs_select;
pub(crate) mod result_columns;
pub(crate) mod topic_configuration_select;
pub(crate) mod topic_configuration_select_keys;
pub(crate) mod topic_select;
pub(crate) mod txn_offset_commit_tp_select_pending;
pub(crate) mod txn_produce_partition_select;
pub(crate) mod watermark_select_for_write;

use nisshi_sans_io::{ErrorCode, record::deflated};
use nisshi_storage::{Error, ProduceEpoch, Result, StoredEpochs};
use opentelemetry::{KeyValue, metrics::Counter};
use std::{
    cmp::Ordering,
    fmt,
    hash::{DefaultHasher, Hash, Hasher as _},
    marker::PhantomData,
};
use tracing::debug;
use uuid::Uuid;

use std::{collections::BTreeMap, ops::Deref, sync::LazyLock};

/// The most rows that one fetch reads from `record`, bound as the limit of the fetch queries. The
/// fetch service reads again from the last offset it received, so a limit that ends a read early
/// costs one more query. Without it, one fetch costs as much as the whole backlog between the fetch
/// offset and the high watermark.
pub(crate) const ROWS_PER_FETCH: i64 = 10_000;

pub(crate) struct Cache(pub BTreeMap<&'static str, String>);

impl Deref for Cache {
    type Target = BTreeMap<&'static str, String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Cache {
    pub(crate) fn new(inner: BTreeMap<&'static str, String>) -> Self {
        Self(inner)
    }

    pub(crate) fn get(&self, key: &str) -> Result<&str> {
        self.0
            .get(key)
            .map(|s| s.as_str())
            .ok_or(Error::UnknownCacheKey(key.to_owned()))
    }
}

macro_rules! include_sql {
    ($e: expr) => {
        remove_comments(include_str!($e))
    };
}

pub(crate) static SQL: LazyLock<Cache> = LazyLock::new(|| {
    let mapping = [
        (
            "maintain-vacuum.sql",
            include_sql!("sql/maintain-vacuum.sql"),
        ),
        (
            "consumer_group_delete.sql",
            include_sql!("sql/consumer_group_delete.sql"),
        ),
        (
            "consumer_group_detail_delete_by_cg.sql",
            include_sql!("sql/consumer_group_detail_delete_by_cg.sql"),
        ),
        (
            "consumer_group_detail_insert.sql",
            include_sql!("sql/consumer_group_detail_insert.sql"),
        ),
        (
            "consumer_group_detail.sql",
            include_sql!("sql/consumer_group_detail.sql"),
        ),
        (
            "consumer_group_insert.sql",
            include_sql!("sql/consumer_group_insert.sql"),
        ),
        (
            "consumer_group_select_by_name.sql",
            include_sql!("sql/consumer_group_select_by_name.sql"),
        ),
        (
            consumer_group_select::QUERY.key(),
            include_sql!("sql/consumer_group_select.sql"),
        ),
        (
            "consumer_offset_delete_by_cg.sql",
            include_sql!("sql/consumer_offset_delete_by_cg.sql"),
        ),
        (
            "consumer_offset_delete_by_topic.sql",
            include_sql!("sql/consumer_offset_delete_by_topic.sql"),
        ),
        (
            "consumer_offset_insert_from_txn.sql",
            include_sql!("sql/consumer_offset_insert_from_txn.sql"),
        ),
        (
            "consumer_offset_insert.sql",
            include_sql!("sql/consumer_offset_insert.sql"),
        ),
        (
            "consumer_offset_select_by_group.sql",
            include_sql!("sql/consumer_offset_select_by_group.sql"),
        ),
        (
            "consumer_offset_select.sql",
            include_sql!("sql/consumer_offset_select.sql"),
        ),
        ("header_copy.sql", include_sql!("sql/header_copy.sql")),
        (
            "header_delete_by_topic.sql",
            include_sql!("sql/header_delete_by_topic.sql"),
        ),
        ("header_fetch.sql", include_sql!("sql/header_fetch.sql")),
        ("header_insert.sql", include_sql!("sql/header_insert.sql")),
        (
            "list_earliest_offset.sql",
            include_sql!("sql/list_earliest_offset.sql"),
        ),
        (
            "list_latest_offset_committed.sql",
            include_sql!("sql/list_latest_offset_committed.sql"),
        ),
        (
            "list_latest_offset_timestamp.sql",
            include_sql!("sql/list_latest_offset_timestamp.sql"),
        ),
        (
            "list_latest_offset_uncommitted.sql",
            include_sql!("sql/list_latest_offset_uncommitted.sql"),
        ),
        (
            "lite/policy_compact_compaction.sql",
            include_sql!("lite/policy_compact_compaction.sql"),
        ),
        (
            "lite/policy_compact_delete.sql",
            include_sql!("lite/policy_compact_delete.sql"),
        ),
        (
            "lite/policy_compact_distinct_k.sql",
            include_sql!("lite/policy_compact_distinct_k.sql"),
        ),
        (
            "lite/policy_compact_max_offset_id.sql",
            include_sql!("lite/policy_compact_max_offset_id.sql"),
        ),
        (
            "lite/policy_compact_topitions.sql",
            include_sql!("lite/policy_compact_topitions.sql"),
        ),
        (
            "lite/policy_delete.sql",
            include_sql!("lite/policy_delete.sql"),
        ),
        (
            "lite/policy_delete_bytes.sql",
            include_sql!("lite/policy_delete_bytes.sql"),
        ),
        ("lite/vacuum_into.sql", include_sql!("lite/vacuum_into.sql")),
        (
            "nisshi_schema_version_create.sql",
            include_sql!("sql/nisshi_schema_version_create.sql"),
        ),
        (
            "nisshi_schema_version_insert.sql",
            include_sql!("sql/nisshi_schema_version_insert.sql"),
        ),
        (
            "nisshi_schema_version_select.sql",
            include_sql!("sql/nisshi_schema_version_select.sql"),
        ),
        (
            "pg/nisshi_schema_version_exists.sql",
            include_sql!("pg/nisshi_schema_version_exists.sql"),
        ),
        (
            "pg/search_path_set_to_cluster_namespace.sql",
            include_sql!("pg/search_path_set_to_cluster_namespace.sql"),
        ),
        ("policy_compact.sql", include_sql!("sql/policy_compact.sql")),
        ("policy_delete.sql", include_sql!("sql/policy_delete.sql")),
        (
            "policy_delete_bytes.sql",
            include_sql!("sql/policy_delete_bytes.sql"),
        ),
        ("ping.sql", "select 1 + 1".to_string()),
        (
            "producer_detail_delete_by_topic.sql",
            include_sql!("sql/producer_detail_delete_by_topic.sql"),
        ),
        (
            "producer_detail_insert.sql",
            include_sql!("sql/producer_detail_insert.sql"),
        ),
        (
            "producer_epoch_current_for_producer.sql",
            include_sql!("sql/producer_epoch_current_for_producer.sql"),
        ),
        (
            "producer_epoch_for_current_txn.sql",
            include_sql!("sql/producer_epoch_for_current_txn.sql"),
        ),
        (
            "producer_epoch_insert.sql",
            include_sql!("sql/producer_epoch_insert.sql"),
        ),
        (
            "producer_epoch_insert_given.sql",
            include_sql!("sql/producer_epoch_insert_given.sql"),
        ),
        (
            producer_epochs_select::QUERY.key(),
            include_sql!("sql/producer_epochs_select.sql"),
        ),
        (
            "producer_insert.sql",
            include_sql!("sql/producer_insert.sql"),
        ),
        (
            "producer_select_for_update.sql",
            include_sql!("sql/producer_select_for_update.sql"),
        ),
        (
            "producer_update_epoch_with_txn.sql",
            include_sql!("sql/producer_update_epoch_with_txn.sql"),
        ),
        (
            "producer_update_sequence.sql",
            include_sql!("sql/producer_update_sequence.sql"),
        ),
        ("record_copy.sql", include_sql!("sql/record_copy.sql")),
        (
            "record_delete_by_offset.sql",
            include_sql!("sql/record_delete_by_offset.sql"),
        ),
        (
            "record_delete_below_log_start.sql",
            include_sql!("sql/record_delete_below_log_start.sql"),
        ),
        (
            "record_delete_by_topic.sql",
            include_sql!("sql/record_delete_by_topic.sql"),
        ),
        ("record_fetch.sql", include_sql!("sql/record_fetch.sql")),
        (
            "record_fetch_keyed.sql",
            include_sql!("sql/record_fetch_keyed.sql"),
        ),
        (pg_record_fetch::QUERY, include_sql!("pg/record_fetch.sql")),
        (
            pg_record_fetch::KEYED_QUERY,
            include_sql!("pg/record_fetch_keyed.sql"),
        ),
        ("record_insert.sql", include_sql!("sql/record_insert.sql")),
        (
            "register_broker.sql",
            include_sql!("sql/register_broker.sql"),
        ),
        (
            "scram_credential_delete.sql",
            include_sql!("sql/scram_credential_delete.sql"),
        ),
        (
            "scram_credential_insert.sql",
            include_sql!("sql/scram_credential_insert.sql"),
        ),
        (
            "scram_credential_select.sql",
            include_sql!("sql/scram_credential_select.sql"),
        ),
        (
            "topic_by_cluster.sql",
            include_sql!("sql/topic_by_cluster.sql"),
        ),
        ("topic_by_uuid.sql", include_sql!("sql/topic_by_uuid.sql")),
        (
            "topic_configuration_delete_by_topic.sql",
            include_sql!("sql/topic_configuration_delete_by_topic.sql"),
        ),
        (
            "topic_configuration_delete.sql",
            include_sql!("sql/topic_configuration_delete.sql"),
        ),
        (
            topic_configuration_select::QUERY.key(),
            include_sql!("sql/topic_configuration_select.sql"),
        ),
        (
            "topic_configuration_select_name.sql",
            include_sql!("sql/topic_configuration_select_name.sql"),
        ),
        (
            "topic_configuration_upsert.sql",
            include_sql!("sql/topic_configuration_upsert.sql"),
        ),
        (
            "topic_delete_by.sql",
            include_sql!("sql/topic_delete_by.sql"),
        ),
        ("topic_insert.sql", include_sql!("sql/topic_insert.sql")),
        (topic_select::QUERY, include_sql!("sql/topic_select.sql")),
        (
            "topic_select_name_for_update.sql",
            include_sql!("sql/topic_select_name_for_update.sql"),
        ),
        (
            "topic_select_name.sql",
            include_sql!("sql/topic_select_name.sql"),
        ),
        (
            "topic_select_uuid.sql",
            include_sql!("sql/topic_select_uuid.sql"),
        ),
        (
            "pg/topic_select_uuid.sql",
            include_sql!("pg/topic_select_uuid.sql"),
        ),
        (
            topic_configuration_select_keys::QUERY.key(),
            include_sql!("pg/topic_configuration_select_keys.sql"),
        ),
        (
            "topition_delete_by_topic.sql",
            include_sql!("sql/topition_delete_by_topic.sql"),
        ),
        (
            "topition_insert.sql",
            include_sql!("sql/topition_insert.sql"),
        ),
        (
            "topition_select.sql",
            include_sql!("sql/topition_select.sql"),
        ),
        (
            "topition_select_id.sql",
            include_sql!("sql/topition_select_id.sql"),
        ),
        (
            "txn_detail_insert.sql",
            include_sql!("sql/txn_detail_insert.sql"),
        ),
        (
            "txn_detail_select_current.sql",
            include_sql!("sql/txn_detail_select_current.sql"),
        ),
        (
            "txn_detail_select_for_update.sql",
            include_sql!("sql/txn_detail_select_for_update.sql"),
        ),
        (
            "txn_detail_select_status_for_update.sql",
            include_sql!("sql/txn_detail_select_status_for_update.sql"),
        ),
        (
            "txn_detail_select_timed_out.sql",
            include_sql!("sql/txn_detail_select_timed_out.sql"),
        ),
        (
            "txn_detail_select.sql",
            include_sql!("sql/txn_detail_select.sql"),
        ),
        (
            "txn_detail_update_sequence.sql",
            include_sql!("sql/txn_detail_update_sequence.sql"),
        ),
        (
            "txn_detail_update_started_at.sql",
            include_sql!("sql/txn_detail_update_started_at.sql"),
        ),
        ("txn_insert.sql", include_sql!("sql/txn_insert.sql")),
        (
            "txn_offset_commit_delete_by_txn.sql",
            include_sql!("sql/txn_offset_commit_delete_by_txn.sql"),
        ),
        (
            "txn_offset_commit_insert.sql",
            include_sql!("sql/txn_offset_commit_insert.sql"),
        ),
        (
            "txn_offset_commit_tp_delete_by_topic.sql",
            include_sql!("sql/txn_offset_commit_tp_delete_by_topic.sql"),
        ),
        (
            "txn_offset_commit_tp_delete_by_txn.sql",
            include_sql!("sql/txn_offset_commit_tp_delete_by_txn.sql"),
        ),
        (
            "txn_offset_commit_tp_insert.sql",
            include_sql!("sql/txn_offset_commit_tp_insert.sql"),
        ),
        (
            "txn_offset_commit_tp_select_pending.sql",
            include_sql!("sql/txn_offset_commit_tp_select_pending.sql"),
        ),
        (
            "txn_produce_offset_delete_by_topic.sql",
            include_sql!("sql/txn_produce_offset_delete_by_topic.sql"),
        ),
        (
            "txn_produce_offset_delete_by_txn.sql",
            include_sql!("sql/txn_produce_offset_delete_by_txn.sql"),
        ),
        (
            "txn_produce_offset_insert.sql",
            include_sql!("sql/txn_produce_offset_insert.sql"),
        ),
        (
            txn_produce_partition_select::QUERY,
            include_sql!("sql/txn_produce_partition_select.sql"),
        ),
        (
            "txn_produce_offset_select_offset_range.sql",
            include_sql!("sql/txn_produce_offset_select_offset_range.sql"),
        ),
        (
            "txn_produce_offset_select_overlapping_txn.sql",
            include_sql!("sql/txn_produce_offset_select_overlapping_txn.sql"),
        ),
        (
            "txn_select_name.sql",
            include_sql!("sql/txn_select_name.sql"),
        ),
        (
            "txn_select_name_for_update.sql",
            include_sql!("sql/txn_select_name_for_update.sql"),
        ),
        (
            "txn_select_produced_topitions.sql",
            include_sql!("sql/txn_select_produced_topitions.sql"),
        ),
        (
            "txn_select_producer_epoch.sql",
            include_sql!("sql/txn_select_producer_epoch.sql"),
        ),
        (
            "txn_status_update.sql",
            include_sql!("sql/txn_status_update.sql"),
        ),
        (
            "txn_topition_delete_by_topic.sql",
            include_sql!("sql/txn_topition_delete_by_topic.sql"),
        ),
        (
            "txn_topition_delete_by_txn.sql",
            include_sql!("sql/txn_topition_delete_by_txn.sql"),
        ),
        (
            "txn_topition_insert.sql",
            include_sql!("sql/txn_topition_insert.sql"),
        ),
        (
            "txn_topition_select_txns.sql",
            include_sql!("sql/txn_topition_select_txns.sql"),
        ),
        (
            "txn_topition_select.sql",
            include_sql!("sql/txn_topition_select.sql"),
        ),
        (
            "txn_update_producer.sql",
            include_sql!("sql/txn_update_producer.sql"),
        ),
        (
            "virtual_topic_upsert.sql",
            include_sql!("sql/virtual_topic_upsert.sql"),
        ),
        (
            "watermark_delete_by_topic.sql",
            include_sql!("sql/watermark_delete_by_topic.sql"),
        ),
        (
            "watermark_insert_from_txn.sql",
            include_sql!("sql/watermark_insert_from_txn.sql"),
        ),
        (
            "watermark_insert.sql",
            include_sql!("sql/watermark_insert.sql"),
        ),
        (
            watermark_select_for_write::QUERY.key(),
            include_sql!("sql/watermark_select_for_write.sql"),
        ),
        (
            watermark_select_for_write::LITE_QUERY.key(),
            include_sql!("lite/watermark_select_for_write.sql"),
        ),
        (
            "watermark_select_stable.sql",
            include_sql!("sql/watermark_select_stable.sql"),
        ),
        (
            "watermark_select.sql",
            include_sql!("sql/watermark_select.sql"),
        ),
        (
            "watermark_update.sql",
            include_sql!("sql/watermark_update.sql"),
        ),
    ];

    Cache::new(BTreeMap::from(mapping))
});

pub(crate) fn remove_comments(commented: &str) -> String {
    commented.lines().fold(String::new(), |uncommented, line| {
        if let Some(position) = line.find("--") {
            match line.split_at(position) {
                ("", _) => uncommented,
                (before, _) => {
                    if uncommented.is_empty() {
                        before.trim().into()
                    } else {
                        format!("{uncommented} {before}")
                    }
                }
            }
        } else if line.trim().is_empty() {
            uncommented
        } else if uncommented.is_empty() {
            line.trim().into()
        } else {
            format!("{uncommented} {}", line.trim())
        }
    })
}

/// Checks a batch's epoch against the epochs stored for its producer, read with
/// [`producer_epochs_select`]. The batch's producer is transactional when `stored` says so,
/// when the request has a transactional ID, or when the batch is transactional.
pub(crate) fn idempotent_epoch_check(
    transaction_id: Option<&str>,
    stored: StoredEpochs,
    deflated: &deflated::Batch,
) -> Result<ProduceEpoch> {
    let stored = StoredEpochs {
        transactional: stored.transactional
            || transaction_id.is_some()
            || deflated.is_transactional(),
        ..stored
    };

    stored
        .check_batch(deflated.producer_epoch, deflated.base_sequence)
        .inspect_err(|error| debug!(?error, ?stored, deflated.producer_epoch))
        .map_err(Error::Api)
}

/// Checks that a client's transactional batch goes to a partition in its producer's ongoing
/// transaction, from the row that [`txn_produce_partition_select`] read: `None` when the
/// transactional ID does not exist, otherwise the producer ID that owns it and whether the
/// partition was added at the batch's producer ID and epoch (the query's `added` column).
///
/// Kafka's transaction coordinator answers an unknown transactional ID, or one that another
/// producer ID owns, `INVALID_PRODUCER_ID_MAPPING`, a transaction that is preparing to commit
/// or abort `CONCURRENT_TRANSACTIONS`, and a partition that is not in the transaction
/// `TRANSACTION_ABORTABLE` ([TransactionCoordinator.scala#L357-L402]). A preparing transaction
/// has its marker written and its partitions still listed, while it waits for an older
/// overlapping transaction, so a batch stored then would land after the marker. Storing the
/// batch anyway would leave records that no transaction marker ever closes, holding back the
/// partition's last stable offset.
///
/// [TransactionCoordinator.scala#L357-L402]: https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala#L357-L402
pub(crate) fn txn_produce_partition_check(
    row: Option<(Option<i64>, i32)>,
    producer_id: i64,
) -> Result<()> {
    match row {
        Some((Some(producer), txn_produce_partition_select::ADDED_IN_TRANSACTION))
            if producer == producer_id =>
        {
            Ok(())
        }
        Some((Some(producer), txn_produce_partition_select::ADDED_IN_PREPARING_TRANSACTION))
            if producer == producer_id =>
        {
            Err(Error::Api(ErrorCode::ConcurrentTransactions))
        }
        Some((Some(producer), _not_added)) if producer == producer_id => {
            Err(Error::Api(ErrorCode::TransactionAbortable))
        }
        None | Some(_) => Err(Error::Api(ErrorCode::InvalidProducerIdMapping)),
    }
}

/// Checks a batch's first sequence against `sequence`, the next sequence stored for its
/// producer's epoch on the partition, and returns how far the batch advances it.
///
/// The caller checks the batch's epoch first, with [`idempotent_epoch_check`].
pub(crate) fn idempotent_sequence_check(sequence: i32, deflated: &deflated::Batch) -> Result<i32> {
    match sequence.cmp(&deflated.base_sequence) {
        Ordering::Equal => Ok(deflated.last_offset_delta + 1),

        Ordering::Greater => {
            debug!(?sequence, ?deflated.base_sequence);
            Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
        }

        Ordering::Less => {
            debug!(?sequence, ?deflated.base_sequence);
            Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
        }
    }
}

pub(crate) fn default_hash<H>(h: &H) -> Uuid
where
    H: Hash,
{
    let mut s = DefaultHasher::new();
    h.hash(&mut s);
    Uuid::from_u128(s.finish() as u128)
}

#[cfg(test)]
mod tests {
    use nisshi_storage::{Error, Result};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

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

    #[test]
    fn txn_produce_partition_check() {
        use nisshi_sans_io::ErrorCode;
        use std::assert_matches;

        assert_matches!(
            super::txn_produce_partition_check(Some((Some(7), 1)), 7),
            Ok(())
        );

        assert_matches!(
            super::txn_produce_partition_check(Some((Some(7), 0)), 7),
            Err(Error::Api(ErrorCode::TransactionAbortable))
        );

        assert_matches!(
            super::txn_produce_partition_check(Some((Some(7), 2)), 7),
            Err(Error::Api(ErrorCode::ConcurrentTransactions))
        );

        for row in [
            None,
            Some((None, 1)),
            Some((Some(8), 1)),
            Some((Some(8), 0)),
            Some((Some(8), 2)),
        ] {
            assert_matches!(
                super::txn_produce_partition_check(row, 7),
                Err(Error::Api(ErrorCode::InvalidProducerIdMapping)),
                "{row:?}"
            );
        }
    }

    #[test]
    fn remove_comments() -> Result<()> {
        let _guard = init_tracing()?;

        assert_eq!(String::from(""), super::remove_comments(""));
        assert_eq!(String::from("pqr"), super::remove_comments("-- abc\npqr"));
        assert_eq!(String::from("abc"), super::remove_comments("abc -- def"));
        assert_eq!(String::from("abc def"), super::remove_comments("abc\ndef"));
        assert_eq!(
            String::from("abc def"),
            super::remove_comments("abc\n\ndef")
        );
        assert_eq!(
            String::from("abc def"),
            super::remove_comments("abc \ndef ")
        );

        Ok(())
    }

    /// Each error that a row reader returns has a kind for the SQL error counters, and a
    /// driver's error has none, because the engine counts it where the driver returns it.
    #[test]
    fn column_error_kind() {
        assert_eq!(
            Some("missing_column"),
            super::column_error_kind(&Error::MissingColumn {
                query: "q.sql",
                column: "c",
                found: vec![],
            })
        );
        assert_eq!(
            Some("duplicate_column"),
            super::column_error_kind(&Error::DuplicateColumn {
                query: "q.sql",
                column: "c",
            })
        );
        assert_eq!(
            Some("unreadable_column"),
            super::column_error_kind(&Error::UnreadableColumn {
                query: "q.sql",
                column: "c",
                source: Box::new(Error::UnexpectedNull),
            })
        );
        assert_eq!(None, super::column_error_kind(&Error::UnexpectedNull));
    }
}
