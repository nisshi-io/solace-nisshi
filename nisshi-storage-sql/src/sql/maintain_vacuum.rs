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

//! The SQLite query that reads the database's `freelist_count` and `page_size` pragmas. It
//! returns one row.
//!
//! The query takes no parameters.

use super::Query;

/// The query's name in the SQL cache.
pub(crate) const QUERY: Query<Row> = Query::new("maintain-vacuum.sql");

sql_row! {
    /// The database's count of unused pages and its page size.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The count of unused pages in the database file.
        freelist_count: i64,
        /// The size of a page, in bytes.
        page_size: i64,
    }
}
