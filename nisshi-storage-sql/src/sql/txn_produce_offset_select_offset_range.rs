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

//! The query that reads the range of offsets that a transaction produced to a partition. It
//! returns no row when the transaction did not produce to the partition.
//!
//! The query takes six parameters: `$1` is the cluster's name, `$2` the transactional ID, `$3`
//! the producer ID, `$4` the producer's epoch, `$5` the topic's name, and `$6` the partition's
//! index.

use super::Query;

/// The query's name in the SQL cache.
pub(crate) const QUERY: Query<Row> = Query::new("txn_produce_offset_select_offset_range.sql");

sql_row! {
    /// The transaction's offsets on the partition.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The first offset that the transaction produced.
        offset_start: i64,
        /// The last offset that the transaction produced.
        offset_end: i64,
    }
}
