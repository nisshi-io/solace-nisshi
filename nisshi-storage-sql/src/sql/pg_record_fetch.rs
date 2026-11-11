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

//! The PostgreSQL queries that read the records of a fetch with their headers, in offset order, and
//! the names of the columns that PostgreSQL reads from their result. The two versions return the
//! same columns. A change to either version's `select` list must keep these names.
//!
//! Each version takes seven parameters: `$1` is the cluster's name, `$2` the topic's name, `$3` the
//! partition's index, `$4` the fetch offset, `$5` the fetch's byte limit, and `$6` the offset that
//! ends the read, which the read does not include. The last parameter is the most rows that the
//! read returns: `$7`, or `$8` in the keyed version, which takes the key as `$7`.

#[cfg(feature = "postgres")]
use bytes::Bytes;
#[cfg(feature = "postgres")]
use nisshi_sans_io::record::Header;
#[cfg(feature = "postgres")]
use nisshi_storage::{Error, FetchedRecord, Result};
#[cfg(feature = "postgres")]
use tokio_postgres::types::FromSql;

#[cfg(feature = "postgres")]
use crate::pg::KafkaTimestamp;

/// The query's name in the SQL cache.
pub(crate) const QUERY: &str = "pg/record_fetch.sql";

/// The keyed version's name in the SQL cache. This version reads only the records with the key.
pub(crate) const KEYED_QUERY: &str = "pg/record_fetch_keyed.sql";

/// The record's offset.
#[cfg(feature = "postgres")]
const OFFSET_ID: &str = "offset_id";

/// The attributes of the record's batch.
#[cfg(feature = "postgres")]
const ATTRIBUTES: &str = "attributes";

/// The record's timestamp.
#[cfg(feature = "postgres")]
const TIMESTAMP: &str = "timestamp";

/// The record's key.
#[cfg(feature = "postgres")]
const KEY: &str = "k";

/// The record's value.
#[cfg(feature = "postgres")]
const VALUE: &str = "v";

/// The ID of the producer that wrote the record.
#[cfg(feature = "postgres")]
const PRODUCER_ID: &str = "producer_id";

/// The epoch of the producer that wrote the record.
#[cfg(feature = "postgres")]
const PRODUCER_EPOCH: &str = "producer_epoch";

/// The keys of the record's headers, in produce order. It is null for a record without headers.
#[cfg(feature = "postgres")]
const HEADER_KEYS: &str = "header_keys";

/// The values of the record's headers, in the order of [`HEADER_KEYS`]. It is null for a record
/// without headers.
#[cfg(feature = "postgres")]
const HEADER_VALUES: &str = "header_values";

/// One record of the partition, with its headers.
#[cfg(feature = "postgres")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Row {
    /// The record's offset.
    offset_id: i64,
    /// The attributes of the record's batch, or 0 when the record stores none.
    attributes: i16,
    /// The record's timestamp, in milliseconds since the epoch.
    timestamp: i64,
    /// The record's key.
    key: Option<Bytes>,
    /// The record's value.
    value: Option<Bytes>,
    /// The ID of the producer that wrote the record, or -1 when the record stores none.
    producer_id: i64,
    /// The epoch of the producer that wrote the record, or -1 when the record stores none.
    producer_epoch: i16,
    /// The record's headers, in produce order.
    headers: Vec<Header>,
}

#[cfg(feature = "postgres")]
impl TryFrom<&tokio_postgres::Row> for Row {
    type Error = Error;

    /// Reads one row of the query's result. An error names the record's offset, once that column
    /// has been read, and the column that failed.
    fn try_from(row: &tokio_postgres::Row) -> Result<Self> {
        let offset_id = row.try_get::<_, i64>(OFFSET_ID).map_err(|error| {
            Error::Message(format!("fetched record: column {OFFSET_ID}: {error}"))
        })?;

        let header_keys =
            column::<Option<Vec<Option<&[u8]>>>>(row, offset_id, HEADER_KEYS)?.unwrap_or_default();
        let header_values = column::<Option<Vec<Option<&[u8]>>>>(row, offset_id, HEADER_VALUES)?
            .unwrap_or_default();

        // Both arrays aggregate the same header rows, so a difference in length means that the
        // query no longer pairs each key with its value.
        if header_keys.len() != header_values.len() {
            return Err(Error::Message(format!(
                "fetched record at offset {offset_id}: {HEADER_KEYS} has {} entries, \
                 {HEADER_VALUES} has {}",
                header_keys.len(),
                header_values.len()
            )));
        }

        Ok(Self {
            offset_id,
            attributes: column::<Option<i16>>(row, offset_id, ATTRIBUTES)?.unwrap_or(0),
            timestamp: column::<KafkaTimestamp>(row, offset_id, TIMESTAMP)?.0,
            key: column::<Option<&[u8]>>(row, offset_id, KEY)?.map(Bytes::copy_from_slice),
            value: column::<Option<&[u8]>>(row, offset_id, VALUE)?.map(Bytes::copy_from_slice),
            producer_id: column::<Option<i64>>(row, offset_id, PRODUCER_ID)?.unwrap_or(-1),
            producer_epoch: column::<Option<i16>>(row, offset_id, PRODUCER_EPOCH)?.unwrap_or(-1),
            headers: header_keys
                .into_iter()
                .zip(header_values)
                .map(|(key, value)| Header {
                    key: key.map(Bytes::copy_from_slice),
                    value: value.map(Bytes::copy_from_slice),
                })
                .collect(),
        })
    }
}

#[cfg(feature = "postgres")]
impl From<Row> for FetchedRecord {
    fn from(row: Row) -> Self {
        Self {
            offset: row.offset_id,
            timestamp: row.timestamp,
            attributes: row.attributes,
            producer_id: row.producer_id,
            producer_epoch: row.producer_epoch,
            key: row.key,
            value: row.value,
            headers: row.headers.into_iter().map(Into::into).collect(),
        }
    }
}

/// Returns the column `name` of `row`, the record at `offset_id`, with an error that names both.
#[cfg(feature = "postgres")]
fn column<'a, T>(row: &'a tokio_postgres::Row, offset_id: i64, name: &str) -> Result<T>
where
    T: FromSql<'a>,
{
    row.try_get(name).map_err(|error| {
        Error::Message(format!(
            "fetched record at offset {offset_id}: column {name}: {error}"
        ))
    })
}
