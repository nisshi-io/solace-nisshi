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

//! Reads a column's value from a row of each SQL driver: the only driver-specific code that a
//! query module's `RowReader` uses.

use bytes::Bytes;
use nisshi_storage::{Error, Result};
use serde_json::Value;
use std::time::SystemTime;
use uuid::Uuid;

/// A row of one SQL driver's result, read by column index.
///
/// Each method reads one kind of value that the queries store, and returns `None` for a null and
/// an error for a value of another kind. Each method reads the value from the row once, so that a
/// large value, such as a record's key, is copied at most once. [`FromColumn`] reads each Rust type
/// that a query module declares through these methods.
pub(crate) trait DriverRow {
    /// Returns the integer at `index`, widened to `i64`.
    fn integer(&self, index: usize) -> Result<Option<i64>>;

    /// Returns the boolean at `index`. SQLite stores a boolean as the integer 0 or 1.
    fn boolean(&self, index: usize) -> Result<Option<bool>>;

    /// Returns the text at `index`.
    fn text(&self, index: usize) -> Result<Option<String>>;

    /// Returns the bytes at `index`.
    fn bytes(&self, index: usize) -> Result<Option<Bytes>>;

    /// Returns the UUID at `index`. SQLite stores a UUID as its text.
    fn uuid(&self, index: usize) -> Result<Option<Uuid>>;

    /// Returns the time at `index`. SQLite stores a time as milliseconds since the Unix epoch.
    fn timestamp(&self, index: usize) -> Result<Option<SystemTime>>;

    /// Returns the JSON document at `index`, or the error from parsing it.
    ///
    /// On libSQL and Turso, which store the document as text, the parse error is a value, not a
    /// read error, so that the engine decides what an undecodable document does to the rest of
    /// the result. PostgreSQL rejects an invalid `json` value when it is stored, and
    /// tokio-postgres decodes the document itself, so a decode failure there is a read error.
    fn json(&self, index: usize) -> Result<Option<serde_json::Result<Value>>>;
}

/// A cursor over one SQL driver's result, which returns each of its rows as a [`DriverRow`], in
/// order.
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

    /// The kind of stored value that this type reads.
    #[cfg(test)]
    const KIND: ColumnKind;
}

/// A kind of stored value, which one [`DriverRow`] method reads.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ColumnKind {
    Integer,
    Boolean,
    Text,
    Bytes,
    Uuid,
    Timestamp,
    Json,
}

/// A column of a row that [`sql_row!`] declares: the column's name, and the kind of value that
/// its field reads.
#[cfg(test)]
#[cfg_attr(
    not(feature = "postgres"),
    allow(
        dead_code,
        reason = "only the PostgreSQL test reads a column's declared kind"
    )
)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct DeclaredColumn {
    pub(crate) name: &'static str,
    pub(crate) kind: ColumnKind,
}

impl<T: FromColumn> FromColumn for Option<T> {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        T::from_nullable_column(row, index).map(Some)
    }

    fn from_column<R: DriverRow>(row: &R, index: usize) -> Result<Self> {
        T::from_nullable_column(row, index)
    }

    #[cfg(test)]
    const KIND: ColumnKind = T::KIND;
}

impl FromColumn for i64 {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.integer(index)
    }

    #[cfg(test)]
    const KIND: ColumnKind = ColumnKind::Integer;
}

impl FromColumn for i32 {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.integer(index)?
            .map(Self::try_from)
            .transpose()
            .map_err(Into::into)
    }

    #[cfg(test)]
    const KIND: ColumnKind = ColumnKind::Integer;
}

impl FromColumn for i16 {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.integer(index)?
            .map(Self::try_from)
            .transpose()
            .map_err(Into::into)
    }

    #[cfg(test)]
    const KIND: ColumnKind = ColumnKind::Integer;
}

impl FromColumn for bool {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.boolean(index)
    }

    #[cfg(test)]
    const KIND: ColumnKind = ColumnKind::Boolean;
}

impl FromColumn for String {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.text(index)
    }

    #[cfg(test)]
    const KIND: ColumnKind = ColumnKind::Text;
}

impl FromColumn for Bytes {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.bytes(index)
    }

    #[cfg(test)]
    const KIND: ColumnKind = ColumnKind::Bytes;
}

impl FromColumn for Uuid {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.uuid(index)
    }

    #[cfg(test)]
    const KIND: ColumnKind = ColumnKind::Uuid;
}

impl FromColumn for SystemTime {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.timestamp(index)
    }

    #[cfg(test)]
    const KIND: ColumnKind = ColumnKind::Timestamp;
}

impl FromColumn for serde_json::Result<Value> {
    fn from_nullable_column<R: DriverRow>(row: &R, index: usize) -> Result<Option<Self>> {
        row.json(index)
    }

    #[cfg(test)]
    const KIND: ColumnKind = ColumnKind::Json;
}

#[cfg(feature = "postgres")]
mod postgres {
    use super::DriverRow;
    use bytes::Bytes;
    use nisshi_storage::Result;
    use serde_json::Value;
    use std::{error::Error as StdError, time::SystemTime};
    use tokio_postgres::{
        Row,
        types::{FromSql, Type},
    };
    use uuid::Uuid;

    /// A `smallint`, `integer` or `bigint`, widened to `i64`.
    ///
    /// tokio-postgres reads each width only as its own Rust type, and a query's integer columns
    /// have different widths, such as a `smallint` epoch and a `bigint` offset.
    struct Integer(i64);

    impl<'a> FromSql<'a> for Integer {
        fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn StdError + Sync + Send>> {
            match *ty {
                Type::INT2 => i16::from_sql(ty, raw).map(i64::from),
                Type::INT4 => i32::from_sql(ty, raw).map(i64::from),
                _ => i64::from_sql(ty, raw),
            }
            .map(Self)
        }

        fn accepts(ty: &Type) -> bool {
            matches!(*ty, Type::INT2 | Type::INT4 | Type::INT8)
        }
    }

    #[cfg(test)]
    impl super::ColumnKind {
        /// Returns whether the [`DriverRow`] method for this kind reads a column of type `ty`.
        pub(crate) fn postgres_accepts(self, ty: &Type) -> bool {
            match self {
                Self::Integer => Integer::accepts(ty),
                Self::Boolean => bool::accepts(ty),
                Self::Text => String::accepts(ty),
                Self::Bytes => <&[u8]>::accepts(ty),
                Self::Uuid => Uuid::accepts(ty),
                Self::Timestamp => SystemTime::accepts(ty),
                Self::Json => Value::accepts(ty),
            }
        }
    }

    impl DriverRow for Row {
        fn integer(&self, index: usize) -> Result<Option<i64>> {
            self.try_get::<_, Option<Integer>>(index)
                .map(|integer| integer.map(|Integer(integer)| integer))
                .map_err(Into::into)
        }

        fn boolean(&self, index: usize) -> Result<Option<bool>> {
            self.try_get(index).map_err(Into::into)
        }

        fn text(&self, index: usize) -> Result<Option<String>> {
            self.try_get(index).map_err(Into::into)
        }

        fn bytes(&self, index: usize) -> Result<Option<Bytes>> {
            self.try_get::<_, Option<&[u8]>>(index)
                .map(|bytes| bytes.map(Bytes::copy_from_slice))
                .map_err(Into::into)
        }

        fn uuid(&self, index: usize) -> Result<Option<Uuid>> {
            self.try_get(index).map_err(Into::into)
        }

        fn timestamp(&self, index: usize) -> Result<Option<SystemTime>> {
            self.try_get(index).map_err(Into::into)
        }

        fn json(&self, index: usize) -> Result<Option<serde_json::Result<Value>>> {
            self.try_get::<_, Option<Value>>(index)
                .map(|json| json.map(Ok))
                .map_err(Into::into)
        }
    }
}

#[cfg(feature = "libsql")]
mod libsql {
    use super::{DriverRow, DriverRowCursor};
    use bytes::Bytes;
    use libsql::{Row, Rows, Value};
    use nisshi_sans_io::to_system_time;
    use nisshi_storage::{Error, Result, SqliteValueSummary};
    use std::time::SystemTime;
    use uuid::Uuid;

    /// Returns the value at `index` of `row`, or `None` for a null.
    ///
    /// This reads the value with `get_value` and each caller matches its kind, instead of the
    /// typed `Row::get`, because libSQL 0.10's typed `get` panics on a value of another kind.
    fn nullable_value(row: &Row, index: usize) -> Result<Option<Value>> {
        match row.get_value(i32::try_from(index)?)? {
            Value::Null => Ok(None),
            value => Ok(Some(value)),
        }
    }

    /// Returns the error for `value`, which has the wrong kind for its column.
    fn unexpected_value(value: Value) -> Error {
        Error::UnexpectedSqliteValue(match value {
            Value::Null => SqliteValueSummary::Null,
            Value::Integer(integer) => SqliteValueSummary::Integer(integer),
            Value::Real(real) => SqliteValueSummary::Real(real),
            Value::Text(text) => SqliteValueSummary::Text { length: text.len() },
            Value::Blob(blob) => SqliteValueSummary::Blob { length: blob.len() },
        })
    }

    /// Returns the integer in `value`, or an error for a value of another kind.
    fn integer_of(value: Value) -> Result<i64> {
        match value {
            Value::Integer(integer) => Ok(integer),
            value => Err(unexpected_value(value)),
        }
    }

    /// Returns the text in `value`, or an error for a value of another kind.
    fn text_of(value: Value) -> Result<String> {
        match value {
            Value::Text(text) => Ok(text),
            value => Err(unexpected_value(value)),
        }
    }

    impl DriverRowCursor for Rows {
        type Row = Row;

        async fn next(&mut self) -> Result<Option<Row>> {
            Rows::next(self).await.map_err(Into::into)
        }
    }

    impl DriverRow for Row {
        fn integer(&self, index: usize) -> Result<Option<i64>> {
            nullable_value(self, index)?.map(integer_of).transpose()
        }

        fn boolean(&self, index: usize) -> Result<Option<bool>> {
            self.integer(index)?
                .map(|integer| match integer {
                    0 => Ok(false),
                    1 => Ok(true),
                    integer => Err(unexpected_value(Value::Integer(integer))),
                })
                .transpose()
        }

        fn text(&self, index: usize) -> Result<Option<String>> {
            nullable_value(self, index)?.map(text_of).transpose()
        }

        fn bytes(&self, index: usize) -> Result<Option<Bytes>> {
            nullable_value(self, index)?
                .map(|value| match value {
                    Value::Blob(blob) => Ok(Bytes::from(blob)),
                    value => Err(unexpected_value(value)),
                })
                .transpose()
        }

        fn uuid(&self, index: usize) -> Result<Option<Uuid>> {
            self.text(index)?
                .map(|text| Uuid::parse_str(&text).map_err(Into::into))
                .transpose()
        }

        fn timestamp(&self, index: usize) -> Result<Option<SystemTime>> {
            self.integer(index)?
                .map(|millis| to_system_time(millis).map_err(Into::into))
                .transpose()
        }

        fn json(&self, index: usize) -> Result<Option<serde_json::Result<serde_json::Value>>> {
            self.text(index)
                .map(|text| text.map(|text| serde_json::from_str(&text)))
        }
    }
}

#[cfg(feature = "turso")]
mod turso {
    use super::{DriverRow, DriverRowCursor};
    use bytes::Bytes;
    use nisshi_sans_io::to_system_time;
    use nisshi_storage::{Error, Result, SqliteValueSummary};
    use std::time::SystemTime;
    use turso::{Row, Rows, Value};
    use uuid::Uuid;

    /// Returns the value at `index` of `row`, or `None` for a null.
    fn nullable_value(row: &Row, index: usize) -> Result<Option<Value>> {
        match row.get_value(index)? {
            Value::Null => Ok(None),
            value => Ok(Some(value)),
        }
    }

    /// Returns the error for `value`, which has the wrong kind for its column.
    fn unexpected_value(value: Value) -> Error {
        Error::UnexpectedSqliteValue(match value {
            Value::Null => SqliteValueSummary::Null,
            Value::Integer(integer) => SqliteValueSummary::Integer(integer),
            Value::Real(real) => SqliteValueSummary::Real(real),
            Value::Text(text) => SqliteValueSummary::Text { length: text.len() },
            Value::Blob(blob) => SqliteValueSummary::Blob { length: blob.len() },
        })
    }

    /// Returns the integer in `value`, or an error for a value of another kind.
    fn integer_of(value: Value) -> Result<i64> {
        match value {
            Value::Integer(integer) => Ok(integer),
            value => Err(unexpected_value(value)),
        }
    }

    /// Returns the text in `value`, or an error for a value of another kind.
    fn text_of(value: Value) -> Result<String> {
        match value {
            Value::Text(text) => Ok(text),
            value => Err(unexpected_value(value)),
        }
    }

    impl DriverRowCursor for Rows {
        type Row = Row;

        async fn next(&mut self) -> Result<Option<Row>> {
            Rows::next(self).await.map_err(Into::into)
        }
    }

    impl DriverRow for Row {
        fn integer(&self, index: usize) -> Result<Option<i64>> {
            nullable_value(self, index)?.map(integer_of).transpose()
        }

        fn boolean(&self, index: usize) -> Result<Option<bool>> {
            self.integer(index)?
                .map(|integer| match integer {
                    0 => Ok(false),
                    1 => Ok(true),
                    integer => Err(unexpected_value(Value::Integer(integer))),
                })
                .transpose()
        }

        fn text(&self, index: usize) -> Result<Option<String>> {
            nullable_value(self, index)?.map(text_of).transpose()
        }

        fn bytes(&self, index: usize) -> Result<Option<Bytes>> {
            nullable_value(self, index)?
                .map(|value| match value {
                    Value::Blob(blob) => Ok(Bytes::from(blob)),
                    value => Err(unexpected_value(value)),
                })
                .transpose()
        }

        fn uuid(&self, index: usize) -> Result<Option<Uuid>> {
            self.text(index)?
                .map(|text| Uuid::parse_str(&text).map_err(Into::into))
                .transpose()
        }

        fn timestamp(&self, index: usize) -> Result<Option<SystemTime>> {
            self.integer(index)?
                .map(|millis| to_system_time(millis).map_err(Into::into))
                .transpose()
        }

        fn json(&self, index: usize) -> Result<Option<serde_json::Result<serde_json::Value>>> {
            self.text(index)
                .map(|text| text.map(|text| serde_json::from_str(&text)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{QueryRow, result_columns::ResultColumns};
    use bytes::Bytes;
    use nisshi_storage::{Error, Result, SqliteValueSummary};
    use serde_json::json;
    use std::{
        assert_matches,
        time::{Duration, SystemTime},
    };
    use uuid::Uuid;

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

    /// A query that reads one column of each kind that SQLite stores in another form than
    /// PostgreSQL does.
    mod converted_probe {
        sql_row! {
            #[derive(Debug)]
            pub(crate) struct Row {
                flag: bool,
                payload: bytes::Bytes,
                id: uuid::Uuid,
                time: std::time::SystemTime,
            }
        }
    }

    /// The UUID in the `id` column of each converted probe query.
    const CONVERTED_PROBE_ID: &str = "67e55044-10b1-426f-9247-bb680e5fe0c8";

    /// The converted probe queries in one driver's SQL dialect.
    struct ConvertedProbeSql {
        /// Returns one row: true, the bytes 1 and 2, [`CONVERTED_PROBE_ID`], and 1.5 seconds after
        /// the Unix epoch.
        in_order: &'static str,
    }

    #[cfg(any(feature = "libsql", feature = "turso"))]
    const SQLITE_CONVERTED: ConvertedProbeSql = ConvertedProbeSql {
        in_order: "select 1 as flag, x'0102' as payload, \
                   '67e55044-10b1-426f-9247-bb680e5fe0c8' as id, 1500 as time",
    };

    #[cfg(feature = "postgres")]
    const POSTGRES_CONVERTED: ConvertedProbeSql = ConvertedProbeSql {
        in_order: r"select true as flag, '\x0102'::bytea as payload,
                    '67e55044-10b1-426f-9247-bb680e5fe0c8'::uuid as id,
                    '1970-01-01 00:00:01.5'::timestamp as time",
    };

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
            let reader = T::reader(&ResultColumns::postgres(probe::QUERY, &statement))?;
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
            let reader = T::reader(&ResultColumns::libsql(probe::QUERY, &rows))?;

            let mut probes = vec![];
            while let Some(row) = rows.next().await? {
                probes.push(T::read(&reader, &row)?);
            }
            Ok(probes)
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
            let reader = T::reader(&ResultColumns::turso(probe::QUERY, &rows))?;

            let mut probes = vec![];
            while let Some(row) = rows.next().await? {
                probes.push(T::read(&reader, &row)?);
            }
            Ok(probes)
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
    async fn invalid_json_is_kept_in_the_row<D: ProbeDriver>() -> Result<()> {
        let row =
            probe_row::<D, probe::Row>("select 3 as epoch, 7 as high, 'a' as name, '{' as detail")
                .await?;
        assert_matches!(row.detail, Some(Err(_)));
        Ok(())
    }

    /// Each driver reads a boolean, bytes, a UUID and a time from the form that its engine stores
    /// them in, so a field has one Rust type on every engine.
    async fn converted_columns_read_their_stored_forms<D: ProbeDriver>(
        sql: &ConvertedProbeSql,
    ) -> Result<()> {
        let row = probe_row::<D, converted_probe::Row>(sql.in_order).await?;

        assert!(row.flag);
        assert_eq!(Bytes::from_static(&[1, 2]), row.payload);
        assert_eq!(Uuid::parse_str(CONVERTED_PROBE_ID)?, row.id);
        assert_eq!(
            SystemTime::UNIX_EPOCH + Duration::from_millis(1500),
            row.time
        );
        Ok(())
    }

    /// On libSQL and Turso, a boolean other than 0 or 1, and a time stored as text, are errors
    /// that name the column, instead of values read as some other boolean or time. The broker
    /// stores only 0, 1 and integer milliseconds. The error keeps a text value's length, not the
    /// text.
    async fn sqlite_rejects_other_stored_forms<D: ProbeDriver>() -> Result<()> {
        for (query, column, summary) in [
            (
                "select 2 as flag, x'0102' as payload, \
                 '67e55044-10b1-426f-9247-bb680e5fe0c8' as id, 1500 as time",
                "flag",
                SqliteValueSummary::Integer(2),
            ),
            (
                "select 1 as flag, x'0102' as payload, \
                 '67e55044-10b1-426f-9247-bb680e5fe0c8' as id, \
                 '1970-01-01 00:00:01.500' as time",
                "time",
                // The length of '1970-01-01 00:00:01.500'.
                SqliteValueSummary::Text { length: 23 },
            ),
        ] {
            assert_matches!(
                D::probe_rows::<converted_probe::Row>(query).await,
                Err(Error::UnreadableColumn { column: found, source, .. })
                    if found == column
                        && matches!(*source, Error::UnexpectedSqliteValue(found) if found == summary),
                "{query}"
            );
        }
        Ok(())
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_converted_columns_read_their_stored_forms() -> Result<()> {
        if !Postgres::is_reachable("postgres_converted_columns_read_their_stored_forms").await {
            return Ok(());
        }

        converted_columns_read_their_stored_forms::<Postgres>(&POSTGRES_CONVERTED).await
    }

    #[cfg(feature = "libsql")]
    #[tokio::test]
    async fn libsql_converted_columns_read_their_stored_forms() -> Result<()> {
        converted_columns_read_their_stored_forms::<Libsql>(&SQLITE_CONVERTED).await
    }

    #[cfg(feature = "libsql")]
    #[tokio::test]
    async fn libsql_rejects_other_stored_forms() -> Result<()> {
        sqlite_rejects_other_stored_forms::<Libsql>().await
    }

    #[cfg(feature = "turso")]
    #[tokio::test]
    async fn turso_converted_columns_read_their_stored_forms() -> Result<()> {
        converted_columns_read_their_stored_forms::<Turso>(&SQLITE_CONVERTED).await
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
}
