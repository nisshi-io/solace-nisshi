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

//! The PostgreSQL queries that read the records of a fetch with their headers, in offset order.
//! The two versions return the same columns.
//!
//! Each version takes seven parameters: `$1` is the cluster's name, `$2` the topic's name, `$3` the
//! partition's index, `$4` the fetch offset, `$5` the fetch's byte limit, and `$6` the offset that
//! ends the read, which the read does not include. The last parameter is the most rows that the
//! read returns: `$7`, or `$8` in the keyed version, which takes the key as `$7`.

use super::{Query, driver::KafkaTimestamp};

#[cfg(feature = "postgres")]
use nisshi_sans_io::record::Header;
#[cfg(feature = "postgres")]
use nisshi_storage::{Error, FetchedRecord, Result};

/// The query's name in the SQL cache.
pub(crate) const QUERY: Query<Row> = Query::new("pg/record_fetch.sql");

/// The keyed version's name in the SQL cache. This version reads only the records with the key.
pub(crate) const KEYED_QUERY: Query<Row> = Query::new("pg/record_fetch_keyed.sql");

sql_row! {
    /// One record of the partition, with its headers.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The record's offset.
        offset_id: i64,
        /// The attributes of the record's batch.
        attributes: Option<i16>,
        /// The record's timestamp. A record without a timestamp reads as -1.
        timestamp: KafkaTimestamp,
        /// The record's key.
        key: Option<bytes::Bytes>,
        /// The record's value.
        value: Option<bytes::Bytes>,
        /// The ID of the producer that wrote the record.
        producer_id: Option<i64>,
        /// The epoch of the producer that wrote the record.
        producer_epoch: Option<i16>,
        /// The keys of the record's headers, in produce order. It is null for a record without
        /// headers.
        header_keys: Option<Vec<Option<bytes::Bytes>>>,
        /// The values of the record's headers, in the order of `header_keys`. It is null for a
        /// record without headers.
        header_values: Option<Vec<Option<bytes::Bytes>>>,
    }
}

/// A record's offset, read from a result that returns a whole [`Row`].
///
/// A fetch reads it from a row that it can't read as a [`Row`], so that the fetch's log names the
/// record, which the column error does not.
#[cfg(feature = "postgres")]
pub(crate) mod offset {
    sql_row! {
        /// One record's offset.
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(crate) struct Row {
            /// The record's offset.
            offset_id: i64,
        }
    }
}

#[cfg(feature = "postgres")]
impl TryFrom<Row> for FetchedRecord {
    type Error = Error;

    /// Returns the record that `row` stores. An error names the record's offset.
    fn try_from(row: Row) -> Result<Self> {
        let offset = row.offset_id;
        let header_keys = row.header_keys.unwrap_or_default();
        let header_values = row.header_values.unwrap_or_default();

        // Both arrays aggregate the same header rows, so a difference in length means that the
        // query no longer pairs each key with its value.
        if header_keys.len() != header_values.len() {
            return Err(Error::Message(format!(
                "fetched record at offset {offset}: header_keys has {} entries, \
                 header_values has {}",
                header_keys.len(),
                header_values.len()
            )));
        }

        Ok(Self {
            offset,
            timestamp: row.timestamp.0,
            attributes: row.attributes.unwrap_or(0),
            producer_id: row.producer_id.unwrap_or(-1),
            producer_epoch: row.producer_epoch.unwrap_or(-1),
            key: row.key,
            value: row.value,
            headers: header_keys
                .into_iter()
                .zip(header_values)
                .map(|(key, value)| Header { key, value }.into())
                .collect(),
        })
    }
}
