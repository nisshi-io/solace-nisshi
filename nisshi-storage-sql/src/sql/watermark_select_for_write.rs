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

//! The query that reads a partition's watermarks and its topic's `max.message.bytes` before a write
//! to the partition.
//!
//! The query takes three parameters: `$1` is the cluster's name, `$2` the topic's name, and `$3`
//! the partition's index.

use super::Query;

/// The PostgreSQL version's name in the SQL cache. This version locks the partition's watermark
/// row.
pub(crate) const QUERY: Query<Row> = Query::new("watermark_select_for_write.sql");

/// The libSQL and Turso version's name in the SQL cache. This version does not lock rows, because
/// SQLite does not lock rows.
pub(crate) const LITE_QUERY: Query<Row> = Query::new("lite/watermark_select_for_write.sql");

sql_row! {
    /// A partition's watermarks and its topic's stored `max.message.bytes`.
    #[derive(Clone, Debug, Default, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The partition's low watermark.
        low: Option<i64>,
        /// The partition's high watermark.
        high: Option<i64>,
        /// The topic's stored `max.message.bytes`, or `None` when the topic does not set the key.
        max_message_bytes: Option<String>,
    }
}
