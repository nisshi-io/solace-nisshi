// Copyright ⓒ Solace Corporation
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

//! Reads and writes the values of libSQL and Turso, the two SQLite drivers, in the form that
//! SQLite stores them: each value is one of SQLite's storage classes.

use super::{DriverRow, KafkaTimestamp};
use bytes::Bytes;
use nisshi_storage::{Error, Result, SqliteValueSummary};
use std::time::SystemTime;
use uuid::Uuid;

/// A value that libSQL or Turso returned, in one of SQLite's storage classes.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SqliteValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl SqliteValue {
    /// Returns the error for this value, which has the wrong storage class for its column.
    ///
    /// The error keeps only the length of a text or blob value, because the value can hold record
    /// data or SCRAM credentials, and the broker logs the error.
    fn unexpected(self) -> Error {
        Error::UnexpectedSqliteValue(match self {
            Self::Null => SqliteValueSummary::Null,
            Self::Integer(integer) => SqliteValueSummary::Integer(integer),
            Self::Real(real) => SqliteValueSummary::Real(real),
            Self::Text(text) => SqliteValueSummary::Text { length: text.len() },
            Self::Blob(blob) => SqliteValueSummary::Blob { length: blob.len() },
        })
    }

    /// Returns the integer in this value, or an error for a value of another storage class.
    fn into_integer(self) -> Result<i64> {
        match self {
            Self::Integer(integer) => Ok(integer),
            value => Err(value.unexpected()),
        }
    }

    /// Returns the text in this value, or an error for a value of another storage class.
    fn into_text(self) -> Result<String> {
        match self {
            Self::Text(text) => Ok(text),
            value => Err(value.unexpected()),
        }
    }
}

/// A row of a libSQL or Turso result, which returns each column as a [`SqliteValue`].
pub(crate) trait SqliteRow {
    /// Returns the value at `index`.
    fn value(&self, index: usize) -> Result<SqliteValue>;

    /// Returns the value at `index`, or `None` for a null.
    fn nullable_value(&self, index: usize) -> Result<Option<SqliteValue>> {
        match self.value(index)? {
            SqliteValue::Null => Ok(None),
            value => Ok(Some(value)),
        }
    }
}

/// SQLite stores a boolean as the integer 0 or 1, a UUID and a JSON document as text, and a time
/// as a [`StoredTime`]. It stores no arrays, so an array of bytes reads only a null.
impl<R: SqliteRow> DriverRow for R {
    fn integer(&self, index: usize) -> Result<Option<i64>> {
        self.nullable_value(index)?
            .map(SqliteValue::into_integer)
            .transpose()
    }

    fn boolean(&self, index: usize) -> Result<Option<bool>> {
        self.integer(index)?
            .map(|integer| match integer {
                0 => Ok(false),
                1 => Ok(true),
                integer => Err(SqliteValue::Integer(integer).unexpected()),
            })
            .transpose()
    }

    fn text(&self, index: usize) -> Result<Option<String>> {
        self.nullable_value(index)?
            .map(SqliteValue::into_text)
            .transpose()
    }

    fn bytes(&self, index: usize) -> Result<Option<Bytes>> {
        self.nullable_value(index)?
            .map(|value| match value {
                SqliteValue::Blob(blob) => Ok(Bytes::from(blob)),
                value => Err(value.unexpected()),
            })
            .transpose()
    }

    fn bytes_array(&self, index: usize) -> Result<Option<Vec<Option<Bytes>>>> {
        self.nullable_value(index)?
            .map(|value| Err(value.unexpected()))
            .transpose()
    }

    fn uuid(&self, index: usize) -> Result<Option<Uuid>> {
        self.text(index)?
            .map(|text| Uuid::parse_str(&text).map_err(Into::into))
            .transpose()
    }

    fn kafka_timestamp(&self, index: usize) -> Result<Option<KafkaTimestamp>> {
        self.integer(index).map(|millis| millis.map(KafkaTimestamp))
    }

    /// A document that isn't JSON, and a value that isn't text, are parse errors in the value,
    /// not read errors, because SQLite accepts any value in any column. The engine then decides
    /// what one undecodable document does to the rest of the result.
    fn json(&self, index: usize) -> Result<Option<serde_json::Result<serde_json::Value>>> {
        Ok(self.nullable_value(index)?.map(|value| match value {
            SqliteValue::Text(text) => serde_json::from_str(&text),
            value => Err(serde::de::Error::custom(format!(
                "a JSON document is stored as text, not as {:?}",
                value.unexpected()
            ))),
        }))
    }
}

/// A time as libSQL and Turso store it: an integer count of milliseconds since the Unix epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StoredTime(SystemTime);

impl StoredTime {
    /// Returns the count of milliseconds since the Unix epoch that SQLite stores for this time.
    /// A time that the count can't hold, which includes any time before the epoch, stores 0.
    fn millis(self) -> i64 {
        nisshi_sans_io::to_timestamp(&self.0).unwrap_or_default()
    }
}

impl From<SystemTime> for StoredTime {
    fn from(time: SystemTime) -> Self {
        Self(time)
    }
}

#[cfg(feature = "libsql")]
mod libsql {
    use super::{SqliteRow, SqliteValue, StoredTime};
    use crate::sql::{driver::DriverRowCursor, result_columns::ColumnNames};
    use libsql::{Row, Rows, Statement, Value};
    use nisshi_storage::Result;

    impl From<Value> for SqliteValue {
        fn from(value: Value) -> Self {
            match value {
                Value::Null => Self::Null,
                Value::Integer(integer) => Self::Integer(integer),
                Value::Real(real) => Self::Real(real),
                Value::Text(text) => Self::Text(text),
                Value::Blob(blob) => Self::Blob(blob),
            }
        }
    }

    impl From<StoredTime> for Value {
        fn from(time: StoredTime) -> Self {
            Self::Integer(time.millis())
        }
    }

    /// This reads the value with `get_value`, instead of the typed `Row::get`, because libSQL
    /// 0.10's typed `get` panics on a value of another kind.
    impl SqliteRow for Row {
        fn value(&self, index: usize) -> Result<SqliteValue> {
            Ok(self.get_value(i32::try_from(index)?)?.into())
        }
    }

    impl DriverRowCursor for Rows {
        type Row = Row;

        async fn next(&mut self) -> Result<Option<Row>> {
            Rows::next(self).await.map_err(Into::into)
        }
    }

    impl ColumnNames for Rows {
        fn column_names(&self) -> Vec<String> {
            // Each index is below the column count, so libSQL returns a name for each.
            (0..self.column_count())
                .map(|index| self.column_name(index).unwrap_or_default().to_owned())
                .collect()
        }
    }

    impl ColumnNames for Statement {
        fn column_names(&self) -> Vec<String> {
            self.columns()
                .iter()
                .map(|column| column.name().to_owned())
                .collect()
        }
    }
}

#[cfg(feature = "turso")]
mod turso {
    use super::{SqliteRow, SqliteValue, StoredTime};
    use crate::sql::{driver::DriverRowCursor, result_columns::ColumnNames};
    use nisshi_storage::Result;
    use turso::{Row, Rows, Statement, Value};

    impl From<Value> for SqliteValue {
        fn from(value: Value) -> Self {
            match value {
                Value::Null => Self::Null,
                Value::Integer(integer) => Self::Integer(integer),
                Value::Real(real) => Self::Real(real),
                Value::Text(text) => Self::Text(text),
                Value::Blob(blob) => Self::Blob(blob),
            }
        }
    }

    impl From<StoredTime> for Value {
        fn from(time: StoredTime) -> Self {
            Self::Integer(time.millis())
        }
    }

    impl SqliteRow for Row {
        fn value(&self, index: usize) -> Result<SqliteValue> {
            Ok(self.get_value(index)?.into())
        }
    }

    impl DriverRowCursor for Rows {
        type Row = Row;

        async fn next(&mut self) -> Result<Option<Row>> {
            Rows::next(self).await.map_err(Into::into)
        }
    }

    impl ColumnNames for Rows {
        fn column_names(&self) -> Vec<String> {
            Rows::column_names(self)
        }
    }

    impl ColumnNames for Statement {
        fn column_names(&self) -> Vec<String> {
            Statement::column_names(self)
        }
    }
}
