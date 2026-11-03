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

//! The query that reads the headers of several records of a partition, ordered by the record's
//! offset and then by the header's order in the record.
//!
//! The query takes four parameters: `$1` is the cluster's name, `$2` the topic's name, `$3` the
//! partition's index, and `$4` the array of the records' offsets.

use super::Query;

/// The query's name in the SQL cache.
pub(crate) const QUERY: Query<Row> = Query::new("header_fetch_range.sql");

sql_row! {
    /// One header of a record.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The offset of the header's record.
        offset_id: i64,
        /// The header's key.
        key: Option<bytes::Bytes>,
        /// The header's value.
        value: Option<bytes::Bytes>,
    }
}
