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

//! The query that reads a consumer group's stored detail and its entity tag. It returns no row
//! when the group has no stored detail.
//!
//! The query takes two parameters: `$1` is the group's name, and `$2` the cluster's name.

use super::Query;

/// The query's name in the SQL cache.
pub(crate) const QUERY: Query<Row> = Query::new("consumer_group_detail.sql");

sql_row! {
    /// The stored detail of a consumer group.
    #[derive(Debug)]
    pub(crate) struct Row {
        /// The entity tag of the stored detail, which an update compares against.
        e_tag: uuid::Uuid,
        /// The group's stored detail, or the error from parsing it as JSON.
        detail: serde_json::Result<serde_json::Value>,
    }
}
