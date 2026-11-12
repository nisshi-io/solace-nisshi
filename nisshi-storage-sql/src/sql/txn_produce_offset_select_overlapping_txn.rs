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

//! The query that finds each other transaction with a status that produced to a partition below
//! an offset, ordered by the last offset that each transaction produced.
//!
//! The query takes seven parameters: `$1` is the cluster's name, `$2` the transactional ID, `$3`
//! the producer ID, `$4` the producer's epoch, `$5` the topic's name, `$6` the partition's index,
//! and `$7` the offset. The result does not include the transaction that `$2`, `$3` and `$4`
//! name.

use super::Query;

/// The query's name in the SQL cache.
pub(crate) const QUERY: Query<Row> = Query::new("txn_produce_offset_select_overlapping_txn.sql");

sql_row! {
    /// One transaction that produced to the partition below the offset.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The transactional ID.
        name: String,
        /// The producer ID.
        producer_id: i64,
        /// The producer's epoch of the transaction.
        producer_epoch: i16,
        /// The transaction's status.
        status: Option<String>,
    }
}
