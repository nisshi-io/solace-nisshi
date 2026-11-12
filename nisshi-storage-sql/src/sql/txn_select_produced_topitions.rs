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

//! The query that finds each partition that a transaction produced to, ordered by topic and
//! partition.
//!
//! The query takes four parameters: `$1` is the cluster's name, `$2` the transactional ID, `$3`
//! the producer ID, and `$4` the producer's epoch.

use super::Query;

/// The query's name in the SQL cache.
pub(crate) const QUERY: Query<Row> = Query::new("txn_select_produced_topitions.sql");

sql_row! {
    /// One partition that the transaction produced to.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The topic's name.
        topic: String,
        /// The partition's index.
        partition: i32,
    }
}
