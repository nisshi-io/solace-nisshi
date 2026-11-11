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

//! The SQLite query that reads the offset of each record of a key in one partition below a given
//! offset, which compaction deletes.
//!
//! The query takes three parameters: `$1` is the partition's ID, `$2` the key, and `$3` the
//! offset, which is the key's newest offset.

use super::Query;

/// The query's name in the SQL cache.
pub(crate) const QUERY: Query<Row> = Query::new("lite/policy_compact_compaction.sql");

sql_row! {
    /// One record that compaction deletes.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The record's offset.
        offset_id: i64,
    }
}
