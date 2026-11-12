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

//! The PostgreSQL query that reads whether a namespace on the search path holds the
//! `nisshi_schema_version` table. It returns one row.
//!
//! The query takes no parameters.

use super::Query;

/// The query's name in the SQL cache.
pub(crate) const QUERY: Query<Row> = Query::new("pg/nisshi_schema_version_exists.sql");

sql_row! {
    /// Whether the `nisshi_schema_version` table exists.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// Whether a namespace on the search path holds the `nisshi_schema_version` table.
        version_table_exists: bool,
    }
}
