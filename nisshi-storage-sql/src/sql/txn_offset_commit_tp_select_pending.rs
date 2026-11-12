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

//! The query that reads each partition for which a transaction holds an offset of a consumer group
//! that it has not yet committed or aborted: in its current epoch, or in an earlier epoch whose end
//! is prepared and waits behind an older open transaction. The end of a transaction deletes its
//! rows.
//!
//! The query takes two parameters: `$1` is the cluster's name, and `$2` the group's name.

use super::Query;

/// The query's name in the SQL cache.
pub(crate) const QUERY: Query<Row> = Query::new("txn_offset_commit_tp_select_pending.sql");

sql_row! {
    /// A partition with a pending transactional offset of the group.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The topic's name.
        topic: String,
        /// The partition's index.
        partition: i32,
    }
}
