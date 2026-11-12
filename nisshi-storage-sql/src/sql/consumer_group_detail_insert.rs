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

//! The query that stores a consumer group's detail, unless the group already has a stored detail
//! with another entity tag than the expected one. It returns the stored row, and no row when it
//! stores nothing.
//!
//! The query takes five parameters: `$1` is the cluster's name, `$2` the group's name, `$3` the
//! expected entity tag, `$4` the new entity tag, and `$5` the new detail.

use super::Query;

/// The query's name in the SQL cache.
pub(crate) const QUERY: Query<Row> = Query::new("consumer_group_detail_insert.sql");

sql_row! {
    /// The stored detail of a consumer group.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The entity tag of the stored detail.
        e_tag: uuid::Uuid,
    }
}
