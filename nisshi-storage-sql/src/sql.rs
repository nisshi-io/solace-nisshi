// Copyright (c) Solace Corporation
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

/// Returns whether `error` is a column error that a row reader returns: a column that the
/// result lacks, has twice, or holds a value that its field can't read.
pub(crate) fn is_column_error(error: &Error) -> bool {
    column_error_kind(error).is_some()
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

    /// Counts and logs `error` when a row reader returned it, and ignores any other error.
    ///
    /// The log names the query and the column, which the error holds. The engines log a driver's
    /// error where their query helpers receive it, and this log does the same for a column error,
    /// so that each failed read has a log whichever caller receives the error.
    pub(crate) fn record(&self, error: &Error) {
        if let Some(kind) = column_error_kind(error) {
            error!(?error, "a column of the result can't be read");

            let mut attributes = self.attributes.clone();
            attributes.push(KeyValue::new("kind", kind));
            self.counter.add(1, &attributes);
        }
    }
}

/// Reads each row of one query's result as `T`, and counts each column error in the query's
/// [`ColumnErrorCount`].
pub(crate) struct ResultRowReader<T: QueryRow> {
    reader: T::Reader,
    column_errors: ColumnErrorCount,
}

impl<T: QueryRow> ResultRowReader<T> {
    /// Returns the reader of `T` for the result with `columns`, which counts each column error in
    /// `column_errors`.
    pub(crate) fn new(
        columns: result_columns::ResultColumns,
        column_errors: ColumnErrorCount,
    ) -> Result<Self> {
        let reader = T::reader(&columns).inspect_err(|err| column_errors.record(err))?;

        Ok(Self {
            reader,
            column_errors,
        })
    }

    pub(crate) fn read<R: driver::DriverRow>(&self, row: &R) -> Result<T> {
        T::read(&self.reader, row).inspect_err(|err| self.column_errors.record(err))
    }
}

/// A cursor over one query's result on libSQL or Turso, which returns each of its rows as a
/// [`QueryRow`], in order. It reads a row only when the caller reaches it.
#[cfg(any(feature = "libsql", feature = "turso"))]
pub(crate) struct QueryRowCursor<R: driver::DriverRowCursor, T: QueryRow> {
    driver_cursor: R,
    rows: ResultRowReader<T>,
}

#[cfg(any(feature = "libsql", feature = "turso"))]
impl<R: driver::DriverRowCursor, T: QueryRow> QueryRowCursor<R, T> {
    /// Returns a cursor over the result with `columns`, which reads each row from
    /// `driver_cursor` and counts each column error in `column_errors`.
    pub(crate) fn new(
        columns: result_columns::ResultColumns,
        driver_cursor: R,
        column_errors: ColumnErrorCount,
    ) -> Result<Self> {
        Ok(Self {
            driver_cursor,
            rows: ResultRowReader::new(columns, column_errors)?,
        })
    }

    /// Returns the next row as `T`, or `None` after the last row.
    pub(crate) async fn next(&mut self) -> Result<Option<T>> {
        self.driver_cursor
            .next()
            .await?
            .map(|row| self.rows.read(&row))
            .transpose()
    }
}

pub(crate) mod consumer_group_detail;
pub(crate) mod consumer_group_detail_insert;
pub(crate) mod consumer_group_select;
pub(crate) mod consumer_group_select_by_name;
pub(crate) mod consumer_offset_select;
pub(crate) mod consumer_offset_select_by_group;
pub(crate) mod driver;
pub(crate) mod header_fetch;
pub(crate) mod list_offset;
pub(crate) mod lite_policy_compact_compaction;
pub(crate) mod lite_policy_compact_distinct_k;
pub(crate) mod lite_policy_compact_max_offset_id;
pub(crate) mod lite_policy_compact_topitions;
pub(crate) mod maintain_vacuum;
pub(crate) mod nisshi_schema_version_select;
pub(crate) mod pg_nisshi_schema_version_exists;
pub(crate) mod pg_record_fetch;
pub(crate) mod producer_epoch_current_for_producer;
pub(crate) mod producer_epoch_for_current_txn;
pub(crate) mod producer_epoch_insert;
pub(crate) mod producer_epochs_select;
pub(crate) mod producer_insert;
pub(crate) mod producer_select_for_update;
pub(crate) mod record_fetch;
pub(crate) mod result_columns;
pub(crate) mod row;
#[cfg(test)]
pub(crate) mod row_check;
pub(crate) mod scram_credential_select;
pub(crate) mod topic_by_cluster;
pub(crate) mod topic_configuration_select;
pub(crate) mod topic_configuration_select_keys;
pub(crate) mod topic_configuration_select_name;
pub(crate) mod topic_configuration_upsert;
pub(crate) mod topic_insert;
pub(crate) mod topic_select_name;
pub(crate) mod topic_select_uuid;
pub(crate) mod topition_insert;
pub(crate) mod topition_select_id;
pub(crate) mod txn_detail_select_status_for_update;
pub(crate) mod txn_detail_select_timed_out;
pub(crate) mod txn_offset_commit_tp_select_pending;
pub(crate) mod txn_produce_offset_select_offset_range;
pub(crate) mod txn_produce_offset_select_overlapping_txn;
pub(crate) mod txn_produce_partition_select;
pub(crate) mod txn_select_name;
pub(crate) mod txn_select_produced_topitions;
pub(crate) mod virtual_topic_upsert;
pub(crate) mod watermark_insert;
pub(crate) mod watermark_select;
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
use tracing::{debug, error};
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

/// Returns the SQL cache key of the file `$file` in the folder `$folder` of this crate's `src`: the
/// file's name for a file in `sql`, and its path for a file in another folder.
macro_rules! cache_key {
    ("sql" / $file:literal) => {
        $file
    };
    ($folder:literal / $file:literal) => {
        concat!($folder, "/", $file)
    };
}

/// Declares the SQL cache, [`SQL`], which holds every query that the engines run, keyed by
/// [`cache_key!`].
///
/// Each line names a query by its folder and file in this crate's `src`. A line in `rows` names
/// the constant that a query's module declares, because an engine reads the query's rows through
/// that module's `RowReader`, and a compile-time assertion checks that the constant holds the
/// query's key. A line in `plain` declares the constant here, for a query whose rows no engine
/// reads a column of.
macro_rules! queries {
    (
        rows {
            $(
                $module:ident :: $module_constant:ident = $row_folder:tt / $row_file:literal,
            )+
        }
        plain {
            $($constant:ident = $folder:tt / $file:literal,)+
        }
    ) => {
        $(
            #[doc = concat!("The `", $folder, "/", $file, "` query's name in the SQL cache.")]
            pub(crate) const $constant: &str = cache_key!($folder / $file);
        )+

        $(
            const _: () = assert!(
                const_str_eq(
                    $module::$module_constant.key(),
                    cache_key!($row_folder / $row_file)
                ),
                concat!(
                    stringify!($module::$module_constant),
                    " must be the cache key of ",
                    $row_folder,
                    "/",
                    $row_file
                ),
            );
        )+

        /// Returns the check of each query in `rows`.
        #[cfg(test)]
        pub(crate) fn row_checks() -> Vec<row_check::RowCheck> {
            vec![$(row_check::RowCheck::rows($module::$module_constant),)+]
        }

        pub(crate) static SQL: LazyLock<Cache> = LazyLock::new(|| {
            Cache::new(BTreeMap::from([
                $(
                    (
                        $module::$module_constant.key(),
                        include_sql!(concat!($row_folder, "/", $row_file)),
                    ),
                )+
                $(($constant, include_sql!(concat!($folder, "/", $file))),)+
            ]))
        });
    };
}

/// Returns whether `left` and `right` hold the same text. `str`'s `==` can't run in a constant.
const fn const_str_eq(left: &str, right: &str) -> bool {
    let (left, right) = (left.as_bytes(), right.as_bytes());

    if left.len() != right.len() {
        return false;
    }

    let mut index = 0;
    while index < left.len() {
        if left[index] != right[index] {
            return false;
        }
        index += 1;
    }

    true
}

queries! {
    rows {
        consumer_group_detail::QUERY = "sql" / "consumer_group_detail.sql",
        consumer_group_detail_insert::QUERY = "sql" / "consumer_group_detail_insert.sql",
        consumer_group_select::QUERY = "sql" / "consumer_group_select.sql",
        consumer_group_select_by_name::QUERY = "sql" / "consumer_group_select_by_name.sql",
        consumer_offset_select::QUERY = "sql" / "consumer_offset_select.sql",
        consumer_offset_select_by_group::QUERY = "sql" / "consumer_offset_select_by_group.sql",
        header_fetch::QUERY = "sql" / "header_fetch.sql",
        list_offset::EARLIEST_QUERY = "sql" / "list_earliest_offset.sql",
        list_offset::LATEST_COMMITTED_QUERY = "sql" / "list_latest_offset_committed.sql",
        list_offset::LATEST_TIMESTAMP_QUERY = "sql" / "list_latest_offset_timestamp.sql",
        list_offset::LATEST_UNCOMMITTED_QUERY = "sql" / "list_latest_offset_uncommitted.sql",
        lite_policy_compact_compaction::QUERY = "lite" / "policy_compact_compaction.sql",
        lite_policy_compact_distinct_k::QUERY = "lite" / "policy_compact_distinct_k.sql",
        lite_policy_compact_max_offset_id::QUERY = "lite" / "policy_compact_max_offset_id.sql",
        lite_policy_compact_topitions::QUERY = "lite" / "policy_compact_topitions.sql",
        maintain_vacuum::QUERY = "sql" / "maintain-vacuum.sql",
        nisshi_schema_version_select::QUERY = "sql" / "nisshi_schema_version_select.sql",
        pg_nisshi_schema_version_exists::QUERY = "pg" / "nisshi_schema_version_exists.sql",
        pg_record_fetch::KEYED_QUERY = "pg" / "record_fetch_keyed.sql",
        pg_record_fetch::QUERY = "pg" / "record_fetch.sql",
        producer_epoch_current_for_producer::QUERY = "sql" / "producer_epoch_current_for_producer.sql",
        producer_epoch_for_current_txn::QUERY = "sql" / "producer_epoch_for_current_txn.sql",
        producer_epoch_insert::QUERY = "sql" / "producer_epoch_insert.sql",
        producer_epochs_select::QUERY = "sql" / "producer_epochs_select.sql",
        producer_insert::QUERY = "sql" / "producer_insert.sql",
        producer_select_for_update::QUERY = "sql" / "producer_select_for_update.sql",
        record_fetch::KEYED_QUERY = "sql" / "record_fetch_keyed.sql",
        record_fetch::QUERY = "sql" / "record_fetch.sql",
        scram_credential_select::QUERY = "sql" / "scram_credential_select.sql",
        topic_by_cluster::QUERY = "sql" / "topic_by_cluster.sql",
        topic_configuration_select::QUERY = "sql" / "topic_configuration_select.sql",
        topic_configuration_select_keys::QUERY = "pg" / "topic_configuration_select_keys.sql",
        topic_configuration_select_name::QUERY = "sql" / "topic_configuration_select_name.sql",
        topic_configuration_upsert::QUERY = "sql" / "topic_configuration_upsert.sql",
        topic_insert::QUERY = "sql" / "topic_insert.sql",
        topic_select_name::QUERY = "sql" / "topic_select_name.sql",
        topic_select_uuid::PG_QUERY = "pg" / "topic_select_uuid.sql",
        topic_select_uuid::QUERY = "sql" / "topic_select_uuid.sql",
        topition_insert::QUERY = "sql" / "topition_insert.sql",
        topition_select_id::QUERY = "sql" / "topition_select_id.sql",
        txn_detail_select_status_for_update::QUERY = "sql" / "txn_detail_select_status_for_update.sql",
        txn_detail_select_timed_out::QUERY = "sql" / "txn_detail_select_timed_out.sql",
        txn_offset_commit_tp_select_pending::QUERY = "sql" / "txn_offset_commit_tp_select_pending.sql",
        txn_produce_offset_select_offset_range::QUERY = "sql" / "txn_produce_offset_select_offset_range.sql",
        txn_produce_offset_select_overlapping_txn::QUERY = "sql" / "txn_produce_offset_select_overlapping_txn.sql",
        txn_produce_partition_select::QUERY = "sql" / "txn_produce_partition_select.sql",
        txn_select_name::QUERY = "sql" / "txn_select_name.sql",
        txn_select_produced_topitions::QUERY = "sql" / "txn_select_produced_topitions.sql",
        virtual_topic_upsert::QUERY = "sql" / "virtual_topic_upsert.sql",
        watermark_insert::QUERY = "sql" / "watermark_insert.sql",
        watermark_select::QUERY = "sql" / "watermark_select.sql",
        watermark_select_for_write::LITE_QUERY = "lite" / "watermark_select_for_write.sql",
        watermark_select_for_write::QUERY = "sql" / "watermark_select_for_write.sql",
    }
    plain {
        CONSUMER_GROUP_DELETE = "sql" / "consumer_group_delete.sql",
        CONSUMER_GROUP_DETAIL_DELETE_BY_CG = "sql" / "consumer_group_detail_delete_by_cg.sql",
        CONSUMER_GROUP_INSERT = "sql" / "consumer_group_insert.sql",
        CONSUMER_OFFSET_DELETE_BY_CG = "sql" / "consumer_offset_delete_by_cg.sql",
        CONSUMER_OFFSET_DELETE_BY_TOPIC = "sql" / "consumer_offset_delete_by_topic.sql",
        CONSUMER_OFFSET_INSERT = "sql" / "consumer_offset_insert.sql",
        CONSUMER_OFFSET_INSERT_FROM_TXN = "sql" / "consumer_offset_insert_from_txn.sql",
        HEADER_COPY = "sql" / "header_copy.sql",
        HEADER_DELETE_BY_TOPIC = "sql" / "header_delete_by_topic.sql",
        HEADER_INSERT = "sql" / "header_insert.sql",
        LITE_POLICY_COMPACT_DELETE = "lite" / "policy_compact_delete.sql",
        LITE_POLICY_DELETE = "lite" / "policy_delete.sql",
        LITE_POLICY_DELETE_BYTES = "lite" / "policy_delete_bytes.sql",
        LITE_VACUUM_INTO = "lite" / "vacuum_into.sql",
        NISSHI_SCHEMA_VERSION_CREATE = "sql" / "nisshi_schema_version_create.sql",
        NISSHI_SCHEMA_VERSION_INSERT = "sql" / "nisshi_schema_version_insert.sql",
        PG_SEARCH_PATH_SET_TO_CLUSTER_NAMESPACE = "pg" / "search_path_set_to_cluster_namespace.sql",
        PING = "sql" / "ping.sql",
        POLICY_COMPACT = "sql" / "policy_compact.sql",
        POLICY_DELETE = "sql" / "policy_delete.sql",
        POLICY_DELETE_BYTES = "sql" / "policy_delete_bytes.sql",
        PRODUCER_DETAIL_DELETE_BY_TOPIC = "sql" / "producer_detail_delete_by_topic.sql",
        PRODUCER_DETAIL_INSERT = "sql" / "producer_detail_insert.sql",
        PRODUCER_EPOCH_INSERT_GIVEN = "sql" / "producer_epoch_insert_given.sql",
        PRODUCER_UPDATE_EPOCH_WITH_TXN = "sql" / "producer_update_epoch_with_txn.sql",
        PRODUCER_UPDATE_SEQUENCE = "sql" / "producer_update_sequence.sql",
        RECORD_COPY = "sql" / "record_copy.sql",
        RECORD_DELETE_BELOW_LOG_START = "sql" / "record_delete_below_log_start.sql",
        RECORD_DELETE_BY_OFFSET = "sql" / "record_delete_by_offset.sql",
        RECORD_DELETE_BY_TOPIC = "sql" / "record_delete_by_topic.sql",
        RECORD_INSERT = "sql" / "record_insert.sql",
        REGISTER_BROKER = "sql" / "register_broker.sql",
        SCRAM_CREDENTIAL_DELETE = "sql" / "scram_credential_delete.sql",
        SCRAM_CREDENTIAL_INSERT = "sql" / "scram_credential_insert.sql",
        TOPIC_BY_UUID = "sql" / "topic_by_uuid.sql",
        TOPIC_CONFIGURATION_DELETE = "sql" / "topic_configuration_delete.sql",
        TOPIC_CONFIGURATION_DELETE_BY_TOPIC = "sql" / "topic_configuration_delete_by_topic.sql",
        TOPIC_DELETE_BY = "sql" / "topic_delete_by.sql",
        TOPIC_SELECT = "sql" / "topic_select.sql",
        TOPIC_SELECT_NAME_FOR_UPDATE = "sql" / "topic_select_name_for_update.sql",
        TOPITION_DELETE_BY_TOPIC = "sql" / "topition_delete_by_topic.sql",
        TOPITION_SELECT = "sql" / "topition_select.sql",
        TXN_DETAIL_INSERT = "sql" / "txn_detail_insert.sql",
        TXN_DETAIL_SELECT = "sql" / "txn_detail_select.sql",
        TXN_DETAIL_SELECT_CURRENT = "sql" / "txn_detail_select_current.sql",
        TXN_DETAIL_SELECT_FOR_UPDATE = "sql" / "txn_detail_select_for_update.sql",
        TXN_DETAIL_UPDATE_SEQUENCE = "sql" / "txn_detail_update_sequence.sql",
        TXN_DETAIL_UPDATE_STARTED_AT = "sql" / "txn_detail_update_started_at.sql",
        TXN_INSERT = "sql" / "txn_insert.sql",
        TXN_OFFSET_COMMIT_DELETE_BY_TXN = "sql" / "txn_offset_commit_delete_by_txn.sql",
        TXN_OFFSET_COMMIT_INSERT = "sql" / "txn_offset_commit_insert.sql",
        TXN_OFFSET_COMMIT_TP_DELETE_BY_TOPIC = "sql" / "txn_offset_commit_tp_delete_by_topic.sql",
        TXN_OFFSET_COMMIT_TP_DELETE_BY_TXN = "sql" / "txn_offset_commit_tp_delete_by_txn.sql",
        TXN_OFFSET_COMMIT_TP_INSERT = "sql" / "txn_offset_commit_tp_insert.sql",
        TXN_PRODUCE_OFFSET_DELETE_BY_TOPIC = "sql" / "txn_produce_offset_delete_by_topic.sql",
        TXN_PRODUCE_OFFSET_DELETE_BY_TXN = "sql" / "txn_produce_offset_delete_by_txn.sql",
        TXN_PRODUCE_OFFSET_INSERT = "sql" / "txn_produce_offset_insert.sql",
        TXN_SELECT_NAME_FOR_UPDATE = "sql" / "txn_select_name_for_update.sql",
        TXN_SELECT_PRODUCER_EPOCH = "sql" / "txn_select_producer_epoch.sql",
        TXN_STATUS_UPDATE = "sql" / "txn_status_update.sql",
        TXN_TOPITION_DELETE_BY_TOPIC = "sql" / "txn_topition_delete_by_topic.sql",
        TXN_TOPITION_DELETE_BY_TXN = "sql" / "txn_topition_delete_by_txn.sql",
        TXN_TOPITION_INSERT = "sql" / "txn_topition_insert.sql",
        TXN_TOPITION_SELECT = "sql" / "txn_topition_select.sql",
        TXN_TOPITION_SELECT_TXNS = "sql" / "txn_topition_select_txns.sql",
        TXN_UPDATE_PRODUCER = "sql" / "txn_update_producer.sql",
        WATERMARK_DELETE_BY_TOPIC = "sql" / "watermark_delete_by_topic.sql",
        WATERMARK_INSERT_FROM_TXN = "sql" / "watermark_insert_from_txn.sql",
        WATERMARK_SELECT_STABLE = "sql" / "watermark_select_stable.sql",
        WATERMARK_UPDATE = "sql" / "watermark_update.sql",
    }
}

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
/// transaction, from the row that [`txn_produce_partition_select`] read, or `None` when the
/// transactional ID does not exist.
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
    row: Option<txn_produce_partition_select::Row>,
    producer_id: i64,
) -> Result<()> {
    use txn_produce_partition_select::{ADDED_IN_PREPARING_TRANSACTION, ADDED_IN_TRANSACTION, Row};

    match row {
        Some(Row {
            producer: Some(producer),
            added: ADDED_IN_TRANSACTION,
        }) if producer == producer_id => Ok(()),
        Some(Row {
            producer: Some(producer),
            added: ADDED_IN_PREPARING_TRANSACTION,
        }) if producer == producer_id => Err(Error::Api(ErrorCode::ConcurrentTransactions)),
        Some(Row {
            producer: Some(producer),
            added: _not_added,
        }) if producer == producer_id => Err(Error::Api(ErrorCode::TransactionAbortable)),
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

    /// A failed topic read answers a code that a client retries only when a retry can succeed.
    #[test]
    fn txn_produce_partition_check() {
        use super::txn_produce_partition_select::{
            ADDED_IN_PREPARING_TRANSACTION, ADDED_IN_TRANSACTION, Row,
        };
        use nisshi_sans_io::ErrorCode;
        use std::assert_matches;

        const NOT_ADDED: i32 = 0;

        fn row(producer: Option<i64>, added: i32) -> Option<Row> {
            Some(Row { producer, added })
        }

        assert_matches!(
            super::txn_produce_partition_check(row(Some(7), ADDED_IN_TRANSACTION), 7),
            Ok(())
        );

        assert_matches!(
            super::txn_produce_partition_check(row(Some(7), NOT_ADDED), 7),
            Err(Error::Api(ErrorCode::TransactionAbortable))
        );

        assert_matches!(
            super::txn_produce_partition_check(row(Some(7), ADDED_IN_PREPARING_TRANSACTION), 7),
            Err(Error::Api(ErrorCode::ConcurrentTransactions))
        );

        for row in [
            None,
            row(None, ADDED_IN_TRANSACTION),
            row(Some(8), ADDED_IN_TRANSACTION),
            row(Some(8), NOT_ADDED),
            row(Some(8), ADDED_IN_PREPARING_TRANSACTION),
        ] {
            assert_matches!(
                super::txn_produce_partition_check(row.clone(), 7),
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
