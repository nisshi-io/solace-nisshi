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

//! The query that reads a partition's low and high watermarks, and the first offset of its oldest
//! unfinished transaction. That offset is null when the partition has no unfinished transaction. It
//! returns no row when the partition does not exist.
//!
//! The query takes three parameters: `$1` is the cluster's name, `$2` the topic's name, and `$3`
//! the partition's index.

use super::Query;

/// The query's name in the SQL cache.
pub(crate) const QUERY: Query<Row> = Query::new("watermark_select.sql");

sql_row! {
    /// A partition's watermarks.
    #[derive(Clone, Debug, Default, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The partition's low watermark.
        low: Option<i64>,
        /// The partition's high watermark.
        high: Option<i64>,
        /// The first offset of the partition's oldest open transaction, or `None` when the
        /// partition has no open transaction.
        stable: Option<i64>,
    }
}
