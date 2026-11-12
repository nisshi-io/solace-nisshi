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

//! The query that reads the next sequence of a producer's epoch on a partition. It returns no row
//! when the partition or the producer's epoch does not exist.
//!
//! The query takes five parameters: `$1` is the cluster's name, `$2` the topic's name, `$3` the
//! partition's index, `$4` the producer ID, and `$5` the producer's epoch.

use super::Query;

/// The query's name in the SQL cache.
pub(crate) const QUERY: Query<Row> = Query::new("producer_select_for_update.sql");

sql_row! {
    /// The next sequence of the producer's epoch on the partition.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The next sequence, or 0 when the epoch has not written to the partition.
        sequence: i32,
    }
}
