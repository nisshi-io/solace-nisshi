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

//! Reads the rows of tokio-postgres, the PostgreSQL driver.

use super::{DriverRow, KafkaTimestamp};
use crate::{pg::POSTGRES_EPOCH_SECONDS, sql::result_columns::ColumnNames};
use bytes::Bytes;
use nisshi_storage::Result;
use serde_json::Value;
use std::error::Error as StdError;
use tokio_postgres::{
    Row, Statement,
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

impl<'a> FromSql<'a> for KafkaTimestamp {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn StdError + Sync + Send>> {
        let microseconds = i64::from_be_bytes(raw.try_into()?);

        // The sum is an `i128`, because the microseconds of a late timestamp plus the epoch
        // offset exceed `i64::MAX`.
        let milliseconds =
            (i128::from(microseconds) + i128::from(POSTGRES_EPOCH_SECONDS) * 1_000_000) / 1_000;

        Ok(Self(i64::try_from(milliseconds)?))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::TIMESTAMP | Type::TIMESTAMPTZ)
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

    fn bytes_array(&self, index: usize) -> Result<Option<Vec<Option<Bytes>>>> {
        self.try_get::<_, Option<Vec<Option<&[u8]>>>>(index)
            .map(|array| {
                array.map(|array| {
                    array
                        .into_iter()
                        .map(|bytes| bytes.map(Bytes::copy_from_slice))
                        .collect()
                })
            })
            .map_err(Into::into)
    }

    fn uuid(&self, index: usize) -> Result<Option<Uuid>> {
        self.try_get(index).map_err(Into::into)
    }

    fn kafka_timestamp(&self, index: usize) -> Result<Option<KafkaTimestamp>> {
        self.try_get(index).map_err(Into::into)
    }

    /// PostgreSQL rejects an invalid `json` value when it is stored, and tokio-postgres decodes
    /// the document itself, so a decode failure here is a read error, not a value.
    fn json(&self, index: usize) -> Result<Option<serde_json::Result<Value>>> {
        self.try_get::<_, Option<Value>>(index)
            .map(|json| json.map(Ok))
            .map_err(Into::into)
    }
}

/// PostgreSQL names a statement's columns before it runs, so a result without rows has them too.
impl ColumnNames for Statement {
    fn column_names(&self) -> Vec<String> {
        self.columns()
            .iter()
            .map(|column| column.name().to_owned())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::KafkaTimestamp;
    use crate::pg::MIN_SQL_TIMESTAMP_MS;
    use nisshi_storage::{Error, Result};
    use std::{assert_matches, time::SystemTime};
    use tokio_postgres::types::{FromSql, Type};

    /// The Kafka timestamp that a `timestamp` column of `microseconds` from 2000-01-01 reads as.
    fn read_kafka_timestamp(microseconds: i64) -> Result<i64> {
        KafkaTimestamp::from_sql(&Type::TIMESTAMP, &microseconds.to_be_bytes())
            .map(|KafkaTimestamp(timestamp)| timestamp)
            .map_err(|err| Error::Message(err.to_string()))
    }

    #[test]
    fn kafka_timestamp_counts_milliseconds_from_the_unix_epoch() -> Result<()> {
        const EPOCH_MICROSECONDS: i64 = 946_684_800_000_000;

        // 2000-01-01 is 946_684_800_000 milliseconds after the Unix epoch.
        assert_eq!(946_684_800_000, read_kafka_timestamp(0)?);
        assert_eq!(946_684_801_000, read_kafka_timestamp(1_000_000)?);
        // The Unix epoch itself, and one millisecond on each side of it.
        assert_eq!(0, read_kafka_timestamp(-EPOCH_MICROSECONDS)?);
        assert_eq!(1, read_kafka_timestamp(-EPOCH_MICROSECONDS + 1_000)?);
        assert_eq!(-1, read_kafka_timestamp(-EPOCH_MICROSECONDS - 1_000)?);

        Ok(())
    }

    #[test]
    fn kafka_timestamp_truncates_toward_zero() -> Result<()> {
        const EPOCH_MICROSECONDS: i64 = 946_684_800_000_000;

        assert_eq!(0, read_kafka_timestamp(-EPOCH_MICROSECONDS + 999)?);
        // Before the Unix epoch the result rounds up, where a floor division gives -1 and -2.
        assert_eq!(0, read_kafka_timestamp(-EPOCH_MICROSECONDS - 1)?);
        assert_eq!(0, read_kafka_timestamp(-EPOCH_MICROSECONDS - 999)?);
        assert_eq!(-1, read_kafka_timestamp(-EPOCH_MICROSECONDS - 1_999)?);
        assert_eq!(-2, read_kafka_timestamp(-EPOCH_MICROSECONDS - 2_000)?);

        Ok(())
    }

    #[test]
    fn kafka_timestamp_reads_the_bounds_of_the_timestamp_column() -> Result<()> {
        // 4713 BC and 294276 AD, the bounds of PostgreSQL's `timestamp`.
        assert_eq!(
            MIN_SQL_TIMESTAMP_MS,
            read_kafka_timestamp(-211_813_488_000_000_000)?
        );
        assert_eq!(
            9_224_318_015_999_999,
            read_kafka_timestamp(9_223_371_331_199_999_999)?
        );

        // -infinity and infinity are `i64::MIN` and `i64::MAX`, and read as ordinary values.
        // The sum with the epoch offset of `i64::MAX` does not fit in an `i64`.
        assert_eq!(-9_222_425_352_054_775, read_kafka_timestamp(i64::MIN)?);
        assert_eq!(9_224_318_721_654_775, read_kafka_timestamp(i64::MAX)?);

        Ok(())
    }

    /// Reads the same value as the `postgres-types` conversion to a `SystemTime` and back, on a
    /// platform where that conversion does not panic.
    #[cfg(not(windows))]
    #[test]
    fn kafka_timestamp_equals_the_read_through_system_time() -> Result<()> {
        fn through_system_time(microseconds: i64) -> Result<i64> {
            let system_time = SystemTime::from_sql(&Type::TIMESTAMP, &microseconds.to_be_bytes())
                .map_err(|err| Error::Message(err.to_string()))?;

            match system_time.duration_since(SystemTime::UNIX_EPOCH) {
                Ok(after) => Ok(i64::try_from(after.as_millis())?),
                Err(before) => Ok(-i64::try_from(before.duration().as_millis())?),
            }
        }

        for microseconds in [
            i64::MIN,
            i64::MIN + 1,
            -211_813_488_000_000_000,
            -12_591_158_400_000_001,
            -12_591_158_400_000_000,
            -946_684_800_000_001,
            -946_684_800_000_000,
            -946_684_799_999_999,
            -1_999,
            -1,
            0,
            1,
            999,
            1_000,
            1_700_000_000_000_000,
            9_223_371_331_199_999_999,
            i64::MAX - 1,
            i64::MAX,
        ] {
            assert_eq!(
                through_system_time(microseconds)?,
                read_kafka_timestamp(microseconds)?,
                "{microseconds}"
            );
        }

        Ok(())
    }

    #[test]
    fn kafka_timestamp_refuses_a_value_that_is_not_eight_bytes() {
        assert_matches!(KafkaTimestamp::from_sql(&Type::TIMESTAMP, &[0; 4]), Err(_));
        assert_matches!(KafkaTimestamp::from_sql(&Type::TIMESTAMP, &[0; 9]), Err(_));
    }
}
