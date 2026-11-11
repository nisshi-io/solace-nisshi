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

//! The interface through which a query module's `RowReader` reads a column's value from a row of
//! any SQL driver. Each driver implements it in a module of its own.

use bytes::Bytes;
use nisshi_storage::{Error, Result};
use serde_json::Value;
use std::time::{Duration, SystemTime};
use uuid::Uuid;

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(any(feature = "libsql", feature = "turso"))]
pub(crate) mod sqlite;

/// A row of one SQL driver's result, read by column index.
///
/// Each method reads one kind of value that the queries store, and returns `None` for a null and
/// an error for a value of another kind. Each method reads the value from the row once, so that a
/// large value, such as a record's key, is copied at most once. [`FromColumn`] reads each Rust type
/// that a query module declares through these methods.
pub(crate) trait DriverRow {
    /// Returns the integer at `index`, widened to `i64`.
    fn integer(&self, index: usize) -> Result<Option<i64>>;

    /// Returns the boolean at `index`.
    fn boolean(&self, index: usize) -> Result<Option<bool>>;

    /// Returns the text at `index`.
    fn text(&self, index: usize) -> Result<Option<String>>;

    /// Returns the bytes at `index`.
    fn bytes(&self, index: usize) -> Result<Option<Bytes>>;

    /// Returns the array of bytes at `index`, each element of which can be null.
    fn bytes_array(&self, index: usize) -> Result<Option<Vec<Option<Bytes>>>>;

    /// Returns the UUID at `index`.
    fn uuid(&self, index: usize) -> Result<Option<Uuid>>;

    /// Returns the time at `index` as a Kafka timestamp.
    fn kafka_timestamp(&self, index: usize) -> Result<Option<KafkaTimestamp>>;

    /// Returns the JSON document at `index`, or the error from parsing it.
    ///
    /// A driver that can return a document that isn't JSON returns the parse error as a value,
    /// not a read error, so that the engine decides what an undecodable document does to the
    /// rest of the result.
    fn json(&self, index: usize) -> Result<Option<serde_json::Result<Value>>>;
}

/// A time read as a Kafka timestamp: milliseconds since the Unix epoch, negative before it.
///
/// The read does not go through [`SystemTime`], because a Windows [`SystemTime`] cannot hold an
/// instant before 1601, and a broker on another platform can have stored one. On PostgreSQL a
/// fraction of a millisecond is dropped toward zero, and an infinity reads as the finite value of
/// its eight bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct KafkaTimestamp(pub(crate) i64);

impl KafkaTimestamp {
    /// Returns the instant that this timestamp names.
    ///
    /// A record that its producer sent without a timestamp stores Kafka's `NO_TIMESTAMP`, -1. A
    /// negative count is the instant before the epoch that it names, so each engine reads the same
    /// record as the same instant. Returns an error for an instant that a `SystemTime` can't hold
    /// on this platform, such as one before 1601 or after 30828 on Windows.
    pub(crate) fn system_time(self) -> Result<SystemTime> {
        let Self(millis) = self;
        let offset = Duration::from_millis(millis.unsigned_abs());

        if millis >= 0 {
            SystemTime::UNIX_EPOCH.checked_add(offset)
        } else {
            SystemTime::UNIX_EPOCH.checked_sub(offset)
        }
        .ok_or_else(|| {
            Error::Message(format!(
                "timestamp {millis} is outside the range that this platform holds"
            ))
        })
    }
}

/// A cursor over one SQL driver's result, which returns each of its rows as a [`DriverRow`], in
/// order.
#[cfg(any(feature = "libsql", feature = "turso"))]
pub(crate) trait DriverRowCursor {
    /// A row of the result.
    type Row: DriverRow;

    /// Returns the next row, or `None` after the last row.
    async fn next(&mut self) -> Result<Option<Self::Row>>;
}

/// A type that a field of a query's row can have, read from one column of a [`DriverRow`].
///
/// A field of type `T` returns [`Error::UnexpectedNull`] for a null, and a field of type
/// `Option<T>` reads a null as `None`. A row's reader calls [`from_column`](Self::from_column) for
/// each field. Each kind of value, such as `i64`, implements only
/// [`from_nullable_column`](Self::from_nullable_column), and `Option<T>` overrides `from_column`.
pub(crate) trait FromColumn: Sized {
    /// Reads the value at `index` of `row`, or `None` for a null.
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>>;

    /// Reads the value at `index` of `row` as a field of this type. Returns
    /// [`Error::UnexpectedNull`] for a null.
    fn from_column<R: DriverRow>(row: &R, index: usize) -> Result<Self> {
        Self::from_nullable_column(row, index)?.ok_or(Error::UnexpectedNull)
    }
}

impl<T: FromColumn> FromColumn for Option<T> {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        T::from_nullable_column(row, index).map(Some)
    }

    fn from_column<R: DriverRow>(row: &R, index: usize) -> Result<Self> {
        T::from_nullable_column(row, index)
    }
}

impl FromColumn for i64 {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.integer(index)
    }
}

impl FromColumn for i32 {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.integer(index)?
            .map(Self::try_from)
            .transpose()
            .map_err(Into::into)
    }
}

impl FromColumn for i16 {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.integer(index)?
            .map(Self::try_from)
            .transpose()
            .map_err(Into::into)
    }
}

impl FromColumn for bool {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.boolean(index)
    }
}

impl FromColumn for String {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.text(index)
    }
}

impl FromColumn for Bytes {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.bytes(index)
    }
}

impl FromColumn for Vec<Option<Bytes>> {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.bytes_array(index)
    }
}

impl FromColumn for Uuid {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.uuid(index)
    }
}

impl FromColumn for KafkaTimestamp {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.kafka_timestamp(index)
    }
}

impl FromColumn for serde_json::Result<Value> {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.json(index)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::{QueryRow, result_columns::ResultColumns},
        FromColumn, KafkaTimestamp,
    };
    use bytes::Bytes;
    #[cfg(any(feature = "libsql", feature = "turso"))]
    use nisshi_storage::SqliteValueSummary;
    use nisshi_storage::{Error, Result};
    use serde_json::json;
    use std::{
        assert_matches,
        time::{Duration, SystemTime},
    };

    /// A query that reads one column of each kind that [`DriverRow`](super::DriverRow) reads.
    mod probe {
        /// The query's name in the errors that its reader returns.
        pub(super) const QUERY: &str = "probe.sql";

        sql_row! {
            #[derive(Debug)]
            pub(crate) struct Row {
                epoch: i16,
                high: Option<i64>,
                name: String,
                detail: Option<serde_json::Result<serde_json::Value>>,
            }
        }
    }

    /// The probe queries in one driver's SQL dialect.
    struct ProbeSql {
        /// Returns one row, with the columns in the order of [`probe::Row`].
        in_order: &'static str,
        /// Returns the row of `in_order`, with its columns in the reverse order.
        reordered: &'static str,
        /// Returns no rows, and names the `name` column `nom`.
        renamed_without_rows: &'static str,
        /// Returns text in the `epoch` column.
        epoch_is_text: &'static str,
        /// Returns null in the `name` column.
        name_is_null: &'static str,
        /// Returns an `epoch` larger than `i16::MAX`.
        epoch_overflows: &'static str,
        /// Returns two columns named `name`.
        name_twice: &'static str,
    }

    #[cfg(any(feature = "libsql", feature = "turso"))]
    const SQLITE: ProbeSql = ProbeSql {
        in_order: r#"select 3 as epoch, 7 as high, 'a' as name, '{"k":1}' as detail"#,
        reordered: r#"select '{"k":1}' as detail, 'a' as name, 7 as high, 3 as epoch"#,
        renamed_without_rows: "select 3 as epoch, 7 as high, 'a' as nom, null as detail \
                               where 1 = 0",
        epoch_is_text: "select 'x' as epoch, 7 as high, 'a' as name, null as detail",
        name_is_null: "select 3 as epoch, 7 as high, null as name, null as detail",
        epoch_overflows: "select 40000 as epoch, 7 as high, 'a' as name, null as detail",
        name_twice: "select 3 as epoch, 7 as high, 'a' as name, 'b' as name, null as detail",
    };

    #[cfg(feature = "postgres")]
    const POSTGRES: ProbeSql = ProbeSql {
        in_order: r#"select 3::int2 as epoch, 7::int4 as high, 'a'::text as name,
                     '{"k":1}'::json as detail"#,
        reordered: r#"select '{"k":1}'::json as detail, 'a'::text as name, 7::int4 as high,
                      3::int2 as epoch"#,
        renamed_without_rows: "select 3::int2 as epoch, 7::int8 as high, 'a'::text as nom, \
                               null::json as detail where 1 = 0",
        epoch_is_text: "select 'x'::text as epoch, 7::int8 as high, 'a'::text as name, \
                        null::json as detail",
        name_is_null: "select 3::int2 as epoch, 7::int8 as high, null::text as name, \
                       null::json as detail",
        epoch_overflows: "select 40000::int4 as epoch, 7::int8 as high, 'a'::text as name, \
                          null::json as detail",
        name_twice: "select 3::int2 as epoch, 7::int8 as high, 'a'::text as name, \
                     'b'::text as name, null::json as detail",
    };

    /// Runs probe queries through one driver.
    trait ProbeDriver {
        /// Runs `sql`, and reads each row of its result as `T`.
        async fn probe_rows<T: QueryRow>(sql: &str) -> Result<Vec<T>>;

        /// Runs `sql`, and reads the first column of its one row as `T`.
        async fn read_value<T: FromColumn>(sql: &str) -> Result<T>;
    }

    #[cfg(feature = "postgres")]
    struct Postgres;

    #[cfg(feature = "postgres")]
    impl Postgres {
        /// Returns whether the PostgreSQL server that the tests use can be reached. When it
        /// can't, this prints that `test` is skipped, as the engine's PostgreSQL tests do, so
        /// that a run without the server passes.
        async fn is_reachable(test: &str) -> bool {
            tokio_postgres::connect(&crate::pg::tests::CONNECTION, tokio_postgres::NoTls)
                .await
                .inspect_err(|err| eprintln!("skipping {test}: {err:?}"))
                .is_ok()
        }
    }

    #[cfg(feature = "postgres")]
    impl ProbeDriver for Postgres {
        async fn probe_rows<T: QueryRow>(sql: &str) -> Result<Vec<T>> {
            let (client, connection) =
                tokio_postgres::connect(&crate::pg::tests::CONNECTION, tokio_postgres::NoTls)
                    .await?;
            let connection = tokio::spawn(connection);

            let statement = client.prepare(sql).await?;
            let reader = T::reader(&ResultColumns::new(probe::QUERY, &statement))?;
            let rows = client
                .query(&statement, &[])
                .await?
                .iter()
                .map(|row| T::read(&reader, row))
                .collect();

            drop(client);
            _ = connection.await;
            rows
        }

        async fn read_value<T: FromColumn>(sql: &str) -> Result<T> {
            let (client, connection) =
                tokio_postgres::connect(&crate::pg::tests::CONNECTION, tokio_postgres::NoTls)
                    .await?;
            let connection = tokio::spawn(connection);

            let row = client.query_one(sql, &[]).await?;
            let value = T::from_column(&row, 0);

            drop(client);
            _ = connection.await;
            value
        }
    }

    #[cfg(feature = "libsql")]
    struct Libsql;

    #[cfg(feature = "libsql")]
    impl ProbeDriver for Libsql {
        async fn probe_rows<T: QueryRow>(sql: &str) -> Result<Vec<T>> {
            let connection = libsql::Builder::new_local(":memory:")
                .build()
                .await?
                .connect()?;
            let mut rows = connection.query(sql, ()).await?;
            let reader = T::reader(&ResultColumns::new(probe::QUERY, &rows))?;

            let mut probes = vec![];
            while let Some(row) = rows.next().await? {
                probes.push(T::read(&reader, &row)?);
            }
            Ok(probes)
        }

        async fn read_value<T: FromColumn>(sql: &str) -> Result<T> {
            let connection = libsql::Builder::new_local(":memory:")
                .build()
                .await?
                .connect()?;
            let mut rows = connection.query(sql, ()).await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| Error::Message(format!("{sql} returned no rows")))?;
            T::from_column(&row, 0)
        }
    }

    #[cfg(feature = "turso")]
    struct Turso;

    #[cfg(feature = "turso")]
    impl ProbeDriver for Turso {
        async fn probe_rows<T: QueryRow>(sql: &str) -> Result<Vec<T>> {
            let connection = turso::Builder::new_local(":memory:")
                .build()
                .await?
                .connect()?;
            let mut rows = connection.query(sql, ()).await?;
            let reader = T::reader(&ResultColumns::new(probe::QUERY, &rows))?;

            let mut probes = vec![];
            while let Some(row) = rows.next().await? {
                probes.push(T::read(&reader, &row)?);
            }
            Ok(probes)
        }

        async fn read_value<T: FromColumn>(sql: &str) -> Result<T> {
            let connection = turso::Builder::new_local(":memory:")
                .build()
                .await?
                .connect()?;
            let mut rows = connection.query(sql, ()).await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| Error::Message(format!("{sql} returned no rows")))?;
            T::from_column(&row, 0)
        }
    }

    /// Returns the one row of `sql`'s result.
    async fn probe_row<D: ProbeDriver, T: QueryRow>(sql: &str) -> Result<T> {
        let mut rows = D::probe_rows::<T>(sql).await?;
        assert_eq!(1, rows.len(), "{sql}");
        Ok(rows.remove(0))
    }

    /// The reader finds each column by its name, so a query that returns its columns in another
    /// order gives the engine the same values.
    async fn reordered_select_reads_the_same_values<D: ProbeDriver>(sql: &ProbeSql) -> Result<()> {
        for query in [sql.in_order, sql.reordered] {
            let row = probe_row::<D, probe::Row>(query).await?;

            assert_eq!(3, row.epoch, "{query}");
            assert_eq!(Some(7), row.high, "{query}");
            assert_eq!("a", row.name, "{query}");
            assert_matches!(row.detail, Some(Ok(detail)) if detail == json!({"k": 1}));
        }

        Ok(())
    }

    /// A query whose result lacks a column fails before the first row, so a renamed column is
    /// found even when the result is empty.
    async fn renamed_column_is_missing_without_rows<D: ProbeDriver>(sql: &ProbeSql) -> Result<()> {
        assert_matches!(
            D::probe_rows::<probe::Row>(sql.renamed_without_rows).await,
            Err(Error::MissingColumn { query: probe::QUERY, column: "name", found })
                if found.contains(&"nom".to_owned())
        );
        Ok(())
    }

    /// A result with two columns of one name is an error, instead of a read of the first of them.
    async fn duplicate_column_is_an_error<D: ProbeDriver>(sql: &ProbeSql) -> Result<()> {
        assert_matches!(
            D::probe_rows::<probe::Row>(sql.name_twice).await,
            Err(Error::DuplicateColumn {
                query: probe::QUERY,
                column: "name"
            })
        );
        Ok(())
    }

    /// A value that the reader can't read as its field's type is an error that names the query
    /// and the column, instead of a value read as missing.
    async fn unreadable_value_names_its_column<D: ProbeDriver>(sql: &ProbeSql) -> Result<()> {
        for (query, column) in [
            (sql.epoch_is_text, "epoch"),
            (sql.name_is_null, "name"),
            (sql.epoch_overflows, "epoch"),
        ] {
            assert_matches!(
                D::probe_rows::<probe::Row>(query).await,
                Err(Error::UnreadableColumn { query: probe::QUERY, column: found, .. }) if found == column,
                "{query}"
            );
        }
        Ok(())
    }

    /// A document that isn't JSON is kept in the row as a parse error, so that the engine decides
    /// what it does to the rest of the result.
    #[cfg(any(feature = "libsql", feature = "turso"))]
    async fn invalid_json_is_kept_in_the_row<D: ProbeDriver>() -> Result<()> {
        let row =
            probe_row::<D, probe::Row>("select 3 as epoch, 7 as high, 'a' as name, '{' as detail")
                .await?;
        assert_matches!(row.detail, Some(Err(_)));
        Ok(())
    }

    /// On libSQL and Turso, a document stored as a value that isn't text is kept in the row as a
    /// parse error, as a document that isn't JSON is, instead of failing the read of the result.
    #[cfg(any(feature = "libsql", feature = "turso"))]
    async fn sqlite_json_that_is_not_text_is_kept_in_the_row<D: ProbeDriver>() -> Result<()> {
        // The blob holds the bytes of `{}`, which is valid JSON, so the read fails only because
        // the value isn't text.
        let row = probe_row::<D, probe::Row>(
            "select 3 as epoch, 7 as high, 'a' as name, x'7b7d' as detail",
        )
        .await?;
        assert_matches!(row.detail, Some(Err(_)));
        Ok(())
    }

    /// On libSQL and Turso, a boolean other than 0 or 1 is an error, instead of a value read as
    /// true. The broker stores only 0 and 1.
    #[cfg(any(feature = "libsql", feature = "turso"))]
    async fn sqlite_rejects_other_stored_forms<D: ProbeDriver>() -> Result<()> {
        assert_matches!(
            D::read_value::<bool>("select 2").await,
            Err(Error::UnexpectedSqliteValue(SqliteValueSummary::Integer(2)))
        );
        Ok(())
    }

    #[cfg(feature = "libsql")]
    #[tokio::test]
    async fn libsql_rejects_other_stored_forms() -> Result<()> {
        sqlite_rejects_other_stored_forms::<Libsql>().await
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn turso_rejects_other_stored_forms() -> Result<()> {
        sqlite_rejects_other_stored_forms::<Turso>().await
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_reordered_select_reads_the_same_values() -> Result<()> {
        if !Postgres::is_reachable("postgres_reordered_select_reads_the_same_values").await {
            return Ok(());
        }

        reordered_select_reads_the_same_values::<Postgres>(&POSTGRES).await
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_renamed_column_is_missing_without_rows() -> Result<()> {
        if !Postgres::is_reachable("postgres_renamed_column_is_missing_without_rows").await {
            return Ok(());
        }

        renamed_column_is_missing_without_rows::<Postgres>(&POSTGRES).await
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_duplicate_column_is_an_error() -> Result<()> {
        if !Postgres::is_reachable("postgres_duplicate_column_is_an_error").await {
            return Ok(());
        }

        duplicate_column_is_an_error::<Postgres>(&POSTGRES).await
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_unreadable_value_names_its_column() -> Result<()> {
        if !Postgres::is_reachable("postgres_unreadable_value_names_its_column").await {
            return Ok(());
        }

        unreadable_value_names_its_column::<Postgres>(&POSTGRES).await
    }

    #[cfg(feature = "libsql")]
    #[tokio::test]
    async fn libsql_reordered_select_reads_the_same_values() -> Result<()> {
        reordered_select_reads_the_same_values::<Libsql>(&SQLITE).await
    }

    #[cfg(feature = "libsql")]
    #[tokio::test]
    async fn libsql_renamed_column_is_missing_without_rows() -> Result<()> {
        renamed_column_is_missing_without_rows::<Libsql>(&SQLITE).await
    }

    #[cfg(feature = "libsql")]
    #[tokio::test]
    async fn libsql_duplicate_column_is_an_error() -> Result<()> {
        duplicate_column_is_an_error::<Libsql>(&SQLITE).await
    }

    #[cfg(feature = "libsql")]
    #[tokio::test]
    async fn libsql_unreadable_value_names_its_column() -> Result<()> {
        unreadable_value_names_its_column::<Libsql>(&SQLITE).await
    }

    #[cfg(feature = "libsql")]
    #[tokio::test]
    async fn libsql_invalid_json_is_kept_in_the_row() -> Result<()> {
        invalid_json_is_kept_in_the_row::<Libsql>().await
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn turso_reordered_select_reads_the_same_values() -> Result<()> {
        reordered_select_reads_the_same_values::<Turso>(&SQLITE).await
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn turso_renamed_column_is_missing_without_rows() -> Result<()> {
        renamed_column_is_missing_without_rows::<Turso>(&SQLITE).await
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn turso_duplicate_column_is_an_error() -> Result<()> {
        duplicate_column_is_an_error::<Turso>(&SQLITE).await
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn turso_unreadable_value_names_its_column() -> Result<()> {
        unreadable_value_names_its_column::<Turso>(&SQLITE).await
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn turso_invalid_json_is_kept_in_the_row() -> Result<()> {
        invalid_json_is_kept_in_the_row::<Turso>().await
    }

    #[cfg(feature = "libsql")]
    #[tokio::test]
    async fn libsql_json_that_is_not_text_is_kept_in_the_row() -> Result<()> {
        sqlite_json_that_is_not_text_is_kept_in_the_row::<Libsql>().await
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn turso_json_that_is_not_text_is_kept_in_the_row() -> Result<()> {
        sqlite_json_that_is_not_text_is_kept_in_the_row::<Turso>().await
    }

    /// PostgreSQL reads each element of a `bytea[]`, a null element as `None`.
    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_bytes_array_reads_each_element() -> Result<()> {
        if !Postgres::is_reachable("postgres_bytes_array_reads_each_element").await {
            return Ok(());
        }

        assert_eq!(
            Some(vec![Some(Bytes::from_static(&[1, 2])), None]),
            Postgres::read_value::<Option<Vec<Option<Bytes>>>>(
                "select array['\\x0102'::bytea, null]"
            )
            .await?
        );
        Ok(())
    }

    /// On libSQL and Turso, an array of bytes reads a null as `None`, and any other value as an
    /// error that names the column, because SQLite stores no arrays.
    #[cfg(any(feature = "libsql", feature = "turso"))]
    async fn sqlite_bytes_array_is_only_null<D: ProbeDriver>() -> Result<()> {
        assert_matches!(
            D::read_value::<Option<Vec<Option<Bytes>>>>("select null").await,
            Ok(None)
        );
        assert_matches!(
            D::read_value::<Option<Vec<Option<Bytes>>>>("select x'0102'").await,
            Err(Error::UnexpectedSqliteValue(SqliteValueSummary::Blob {
                length: 2
            }))
        );
        Ok(())
    }

    #[cfg(feature = "libsql")]
    #[tokio::test]
    async fn libsql_bytes_array_is_only_null() -> Result<()> {
        sqlite_bytes_array_is_only_null::<Libsql>().await
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn turso_bytes_array_is_only_null() -> Result<()> {
        sqlite_bytes_array_is_only_null::<Turso>().await
    }

    /// A stored -1, a record without a timestamp, reads as the millisecond before the epoch, and
    /// a timestamp after the epoch reads as itself.
    #[test]
    fn kafka_timestamp_reads_a_negative_timestamp_before_the_epoch() -> Result<()> {
        assert_eq!(
            SystemTime::UNIX_EPOCH - Duration::from_millis(1),
            KafkaTimestamp(-1).system_time()?
        );
        assert_eq!(
            SystemTime::UNIX_EPOCH + Duration::from_millis(1_234),
            KafkaTimestamp(1_234).system_time()?
        );

        Ok(())
    }

    /// A timestamp after the end of the platform's `SystemTime`, as on Windows after the year
    /// 30828, reads as an error and not a panic.
    #[test]
    fn kafka_timestamp_after_the_platform_range_is_an_error() {
        let read = KafkaTimestamp(i64::MAX).system_time();

        if cfg!(windows) {
            assert_matches!(read, Err(Error::Message(_)));
        } else {
            assert_matches!(read, Ok(_));
        }
    }
}
