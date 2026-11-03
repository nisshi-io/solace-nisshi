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
                            })
                            // The reader logs the error at error level, because the error means
                            // that the row is corrupt or that the query does not match its row,
                            // and a caller can answer it with a Kafka error code that does not
                            // show it.
                            .inspect_err(|err| ::tracing::error!(?err))?,
                    )+
                })
            }
        }

        #[cfg(test)]
        impl Row {
            /// Each column of the row, with the kind of value that its field reads.
            #[allow(dead_code, reason = "a test probe's row is in no query's line of `queries!`")]
            pub(crate) const COLUMNS: &[$crate::sql::driver::DeclaredColumn] = &[
                $(
                    $crate::sql::driver::DeclaredColumn {
                        name: stringify!($field),
                        kind: <$type as $crate::sql::driver::FromColumn>::KIND,
                    },
                )+
            ];
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

/// Returns the error code that a Metadata or DescribeTopicPartitions response gives a topic whose
/// read failed with `error`. `query_error_code` is the code for any error other than a column
/// error, such as a failed query.
///
/// A column error means that the topic's stored row is corrupt, or that the query doesn't match
/// its row, and a retry fixes neither. The topic then gets `UNKNOWN_SERVER_ERROR`, which a client
/// does not retry, instead of `UNKNOWN_TOPIC_OR_PARTITION`. That code tells a client that the
/// topic does not exist, and a client that may create topics then asks the broker to create it,
/// which fails because the topic exists, on each metadata refresh.
pub(crate) fn topic_read_error_code(error: &Error, query_error_code: ErrorCode) -> ErrorCode {
    if column_error_kind(error).is_some() {
        ErrorCode::UnknownServerError
    } else {
        query_error_code
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

    /// Returns the next row that the reader can read as `T`, or `None` after the last row.
    ///
    /// It skips each row with an unreadable column, which the reader logs and counts, so that one
    /// corrupt row does not hide the rows after it. Use it only where a result without that row
    /// is still a correct answer, such as a list of the cluster's topics.
    pub(crate) async fn next_readable(&mut self) -> Result<Option<T>> {
        loop {
            match self.next().await {
                Err(Error::UnreadableColumn { .. }) => continue,
                next => return next,
            }
        }
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

pub(crate) mod consumer_group_detail;
pub(crate) mod consumer_group_detail_insert;
pub(crate) mod consumer_group_select;
pub(crate) mod consumer_group_select_by_name;
pub(crate) mod consumer_offset_select;
pub(crate) mod consumer_offset_select_by_group;
pub(crate) mod driver;
pub(crate) mod header_fetch;
pub(crate) mod header_fetch_range;
pub(crate) mod list_offset;
pub(crate) mod lite_policy_compact_compaction;
pub(crate) mod lite_policy_compact_distinct_k;
pub(crate) mod lite_policy_compact_max_offset_id;
pub(crate) mod lite_policy_compact_topitions;
pub(crate) mod maintain_vacuum;
pub(crate) mod nisshi_schema_version_select;
pub(crate) mod pg_nisshi_schema_version_exists;
pub(crate) mod producer_epoch_current_for_producer;
pub(crate) mod producer_epoch_for_current_txn;
pub(crate) mod producer_epoch_insert;
pub(crate) mod producer_epochs_select;
pub(crate) mod producer_insert;
pub(crate) mod producer_select_for_update;
pub(crate) mod record_fetch;
pub(crate) mod result_columns;
pub(crate) mod row;
pub(crate) mod scram_credential_select;
pub(crate) mod topic_by_cluster;
pub(crate) mod topic_configuration_select;
pub(crate) mod topic_configuration_select_keys;
pub(crate) mod topic_configuration_select_name;
pub(crate) mod topic_configuration_upsert;
pub(crate) mod topic_insert;
pub(crate) mod topic_select;
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
use tracing::debug;
use uuid::Uuid;

use std::{collections::BTreeMap, ops::Deref, sync::LazyLock};

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
/// that module's `RowReader`. A line in `without_columns` names a module's constant too, for a
/// query whose rows an engine counts but reads no column of. A compile-time assertion checks that
/// each such constant holds the query's key. A line in `plain` declares the constant here.
///
/// A line in `rows` or `without_columns` starts with the engines that run the query, and
/// `ROW_QUERIES` lists those queries for a test that prepares each one on each of its engines.
macro_rules! queries {
    (
        rows {
            $(
                [$($row_engine:ident),+]
                $module:ident :: $module_constant:ident = $row_folder:tt / $row_file:literal,
            )+
        }
        without_columns {
            $(
                [$($counted_engine:ident),+]
                $counted_module:ident :: $counted_constant:ident
                    = $counted_folder:tt / $counted_file:literal,
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

        $(
            const _: () = assert!(
                const_str_eq(
                    $counted_module::$counted_constant,
                    cache_key!($counted_folder / $counted_file)
                ),
                concat!(
                    stringify!($counted_module::$counted_constant),
                    " must be the cache key of ",
                    $counted_folder,
                    "/",
                    $counted_file
                ),
            );
        )+

        pub(crate) static SQL: LazyLock<Cache> = LazyLock::new(|| {
            Cache::new(BTreeMap::from([
                $(
                    (
                        $module::$module_constant.key(),
                        include_sql!(concat!($row_folder, "/", $row_file)),
                    ),
                )+
                $(
                    (
                        $counted_module::$counted_constant,
                        include_sql!(concat!($counted_folder, "/", $counted_file)),
                    ),
                )+
                $(($constant, include_sql!(concat!($folder, "/", $file))),)+
            ]))
        });

        #[cfg(test)]
        pub(crate) static ROW_QUERIES: &[RowQuery] = &[
            $(
                RowQuery {
                    query: $module::$module_constant.key(),
                    engines: &[$(SqlEngine::$row_engine),+],
                    check_columns: |columns| {
                        query_reader($module::$module_constant, columns).map(|_| ())
                    },
                    columns: $module::Row::COLUMNS,
                },
            )+
            $(
                RowQuery {
                    query: $counted_module::$counted_constant,
                    engines: &[$(SqlEngine::$counted_engine),+],
                    check_columns: |_| Ok(()),
                    columns: &[],
                },
            )+
        ];
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

/// An engine that runs the queries of [`SQL`].
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SqlEngine {
    Postgres,
    Libsql,
    Turso,
}

/// A query in [`ROW_QUERIES`]: one whose rows an engine reads.
#[cfg(test)]
pub(crate) struct RowQuery {
    /// The query's name in [`SQL`].
    pub(crate) query: &'static str,
    /// The engines that run the query.
    pub(crate) engines: &'static [SqlEngine],
    /// Returns the error of the query's `RowReader` for the columns of the query's result.
    pub(crate) check_columns: fn(&result_columns::ResultColumns) -> Result<()>,
    /// Each column of the query's row, with the kind of value that its field reads.
    #[cfg_attr(
        not(feature = "postgres"),
        allow(
            dead_code,
            reason = "only the PostgreSQL test reads a column's declared kind"
        )
    )]
    pub(crate) columns: &'static [driver::DeclaredColumn],
}

/// Returns the reader of `T`, the row of `query`, for the result with `columns`.
#[cfg(test)]
fn query_reader<T: QueryRow>(
    _query: Query<T>,
    columns: &result_columns::ResultColumns,
) -> Result<T::Reader> {
    T::reader(columns)
}

/// Returns each query of [`ROW_QUERIES`] that `engine` runs.
#[cfg(test)]
pub(crate) fn row_queries(engine: SqlEngine) -> impl Iterator<Item = &'static RowQuery> {
    ROW_QUERIES
        .iter()
        .filter(move |row_query| row_query.engines.contains(&engine))
}

queries! {
    rows {
        [Postgres, Libsql, Turso] consumer_group_detail::QUERY = "sql" / "consumer_group_detail.sql",
        [Postgres, Libsql, Turso] consumer_group_detail_insert::QUERY = "sql" / "consumer_group_detail_insert.sql",
        [Postgres, Libsql, Turso] consumer_group_select::QUERY = "sql" / "consumer_group_select.sql",
        [Postgres, Libsql, Turso] consumer_group_select_by_name::QUERY = "sql" / "consumer_group_select_by_name.sql",
        [Postgres, Libsql, Turso] consumer_offset_select::QUERY = "sql" / "consumer_offset_select.sql",
        [Postgres, Libsql, Turso] consumer_offset_select_by_group::QUERY = "sql" / "consumer_offset_select_by_group.sql",
        [Libsql, Turso] header_fetch::QUERY = "sql" / "header_fetch.sql",
        [Postgres] header_fetch_range::QUERY = "sql" / "header_fetch_range.sql",
        [Postgres, Libsql, Turso] list_offset::EARLIEST_QUERY = "sql" / "list_earliest_offset.sql",
        [Postgres, Libsql, Turso] list_offset::LATEST_COMMITTED_QUERY = "sql" / "list_latest_offset_committed.sql",
        [Postgres, Libsql, Turso] list_offset::LATEST_TIMESTAMP_QUERY = "sql" / "list_latest_offset_timestamp.sql",
        [Postgres, Libsql, Turso] list_offset::LATEST_UNCOMMITTED_QUERY = "sql" / "list_latest_offset_uncommitted.sql",
        [Libsql] lite_policy_compact_compaction::QUERY = "lite" / "policy_compact_compaction.sql",
        [Libsql] lite_policy_compact_distinct_k::QUERY = "lite" / "policy_compact_distinct_k.sql",
        [Libsql] lite_policy_compact_max_offset_id::QUERY = "lite" / "policy_compact_max_offset_id.sql",
        [Libsql] lite_policy_compact_topitions::QUERY = "lite" / "policy_compact_topitions.sql",
        [Libsql] maintain_vacuum::QUERY = "sql" / "maintain-vacuum.sql",
        [Postgres, Libsql] nisshi_schema_version_select::QUERY = "sql" / "nisshi_schema_version_select.sql",
        [Postgres] pg_nisshi_schema_version_exists::QUERY = "pg" / "nisshi_schema_version_exists.sql",
        [Postgres, Libsql, Turso] producer_epoch_current_for_producer::QUERY = "sql" / "producer_epoch_current_for_producer.sql",
        [Postgres, Libsql, Turso] producer_epoch_for_current_txn::QUERY = "sql" / "producer_epoch_for_current_txn.sql",
        [Postgres, Libsql, Turso] producer_epoch_insert::QUERY = "sql" / "producer_epoch_insert.sql",
        [Postgres, Libsql, Turso] producer_epochs_select::QUERY = "sql" / "producer_epochs_select.sql",
        [Postgres, Libsql, Turso] producer_insert::QUERY = "sql" / "producer_insert.sql",
        [Postgres, Libsql, Turso] producer_select_for_update::QUERY = "sql" / "producer_select_for_update.sql",
        [Libsql] record_fetch::KEYED_QUERY = "sql" / "record_fetch_keyed.sql",
        [Postgres] record_fetch::PG_KEYED_QUERY = "pg" / "record_fetch_keyed.sql",
        [Postgres] record_fetch::PG_QUERY = "pg" / "record_fetch.sql",
        [Libsql, Turso] record_fetch::QUERY = "sql" / "record_fetch.sql",
        [Postgres, Libsql] scram_credential_select::QUERY = "sql" / "scram_credential_select.sql",
        [Postgres, Libsql, Turso] topic_by_cluster::QUERY = "sql" / "topic_by_cluster.sql",
        [Postgres, Libsql, Turso] topic_configuration_select::QUERY = "sql" / "topic_configuration_select.sql",
        [Postgres] topic_configuration_select_keys::QUERY = "pg" / "topic_configuration_select_keys.sql",
        [Postgres, Libsql, Turso] topic_configuration_select_name::QUERY = "sql" / "topic_configuration_select_name.sql",
        [Postgres, Libsql, Turso] topic_configuration_upsert::QUERY = "sql" / "topic_configuration_upsert.sql",
        [Postgres, Libsql, Turso] topic_insert::QUERY = "sql" / "topic_insert.sql",
        [Postgres, Libsql, Turso] topic_select_name::QUERY = "sql" / "topic_select_name.sql",
        [Postgres] topic_select_uuid::PG_QUERY = "pg" / "topic_select_uuid.sql",
        [Postgres, Libsql, Turso] topic_select_uuid::QUERY = "sql" / "topic_select_uuid.sql",
        [Postgres, Libsql, Turso] topition_insert::QUERY = "sql" / "topition_insert.sql",
        [Postgres] topition_select_id::QUERY = "sql" / "topition_select_id.sql",
        [Postgres] txn_detail_select_status_for_update::QUERY = "sql" / "txn_detail_select_status_for_update.sql",
        [Postgres] txn_detail_select_timed_out::QUERY = "sql" / "txn_detail_select_timed_out.sql",
        [Postgres, Libsql, Turso] txn_offset_commit_tp_select_pending::QUERY = "sql" / "txn_offset_commit_tp_select_pending.sql",
        [Postgres, Libsql, Turso] txn_produce_offset_select_offset_range::QUERY = "sql" / "txn_produce_offset_select_offset_range.sql",
        [Postgres, Libsql, Turso] txn_produce_offset_select_overlapping_txn::QUERY = "sql" / "txn_produce_offset_select_overlapping_txn.sql",
        [Postgres, Libsql, Turso] txn_produce_partition_select::QUERY = "sql" / "txn_produce_partition_select.sql",
        [Postgres, Libsql, Turso] txn_select_name::QUERY = "sql" / "txn_select_name.sql",
        [Postgres, Libsql, Turso] txn_select_produced_topitions::QUERY = "sql" / "txn_select_produced_topitions.sql",
        [Postgres, Libsql] virtual_topic_upsert::QUERY = "sql" / "virtual_topic_upsert.sql",
        [Postgres, Libsql, Turso] watermark_insert::QUERY = "sql" / "watermark_insert.sql",
        [Postgres, Libsql, Turso] watermark_select::QUERY = "sql" / "watermark_select.sql",
        [Libsql, Turso] watermark_select_for_write::LITE_QUERY = "lite" / "watermark_select_for_write.sql",
        [Postgres] watermark_select_for_write::QUERY = "sql" / "watermark_select_for_write.sql",
    }
    without_columns {
        [Postgres, Libsql, Turso] topic_select::QUERY = "sql" / "topic_select.sql",
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
