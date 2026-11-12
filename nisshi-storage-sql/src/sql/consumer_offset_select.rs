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

//! The query that reads a consumer group's committed offset on one partition. It returns no row
//! when the group has not committed an offset on the partition.
//!
//! The query takes four parameters: `$1` is the cluster's name, `$2` the group's name, `$3` the
//! topic's name, and `$4` the partition's index.

use super::Query;

/// The query's name in the SQL cache.
pub(crate) const QUERY: Query<Row> = Query::new("consumer_offset_select.sql");

sql_row! {
    /// The committed offset of a consumer group on a partition.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The committed offset.
        committed_offset: i64,
        /// The leader epoch of the commit, or `None` when the commit did not give one.
        leader_epoch: Option<i32>,
        /// The metadata of the commit, or `None` when the commit did not give any.
        metadata: Option<String>,
    }
}
