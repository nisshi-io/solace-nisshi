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

//! The libSQL and Turso queries that read the records of a fetch, in offset order. The two versions
//! return the same columns.
//!
//! Each version takes at least six parameters: `$1` is the cluster's name, `$2` the topic's name,
//! `$3` the partition's index, `$4` the fetch offset, `$5` the fetch's byte limit, and `$6` the
//! offset that ends the read, which the read does not include. The keyed version takes the key as
//! `$7`.

use super::Query;

/// The query's name in the SQL cache.
pub(crate) const QUERY: Query<Row> = Query::new("record_fetch.sql");

/// The keyed version's name in the SQL cache. This version reads only the records with the key.
pub(crate) const KEYED_QUERY: Query<Row> = Query::new("record_fetch_keyed.sql");

sql_row! {
    /// One record of the partition.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The record's offset.
        offset_id: i64,
        /// The attributes of the record's batch.
        attributes: Option<i16>,
        /// The record's timestamp, in milliseconds since the epoch. It is -1, Kafka's
        /// `NO_TIMESTAMP`, for a record that its producer sent without a timestamp, which a
        /// `SystemTime` cannot hold.
        timestamp: i64,
        /// The record's key.
        key: Option<bytes::Bytes>,
        /// The record's value.
        value: Option<bytes::Bytes>,
        /// The ID of the producer that wrote the record.
        producer_id: Option<i64>,
        /// The epoch of the producer that wrote the record.
        producer_epoch: Option<i16>,
    }
}
