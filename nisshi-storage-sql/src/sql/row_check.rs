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

//! The check that a query returns the columns that its row reads.

use super::{Query, QueryRow, result_columns::ResultColumns};
use nisshi_storage::Result;

/// A query, with the check of its prepared result's columns against the query's row.
pub(crate) struct RowCheck {
    query: &'static str,
    check_columns: fn(&ResultColumns) -> Result<()>,
}

impl RowCheck {
    /// Returns the check of `query`, whose result an engine reads as rows of `T`. The check builds
    /// the reader of `T` through [`QueryRow::reader`], as the engine does.
    pub(crate) fn rows<T: QueryRow>(query: Query<T>) -> Self {
        Self {
            query: query.key(),
            check_columns: |columns| T::reader(columns).map(|_| ()),
        }
    }

    /// Returns the check of each row query in `folders`, the folders of this crate's `src` whose
    /// queries an engine runs, except the queries in `skipped`.
    ///
    /// Panics if a query in `skipped` is not a row query in `folders`, so that the list of an
    /// engine's skipped queries can't name a query that no longer exists.
    pub(crate) fn in_folders(folders: &[&str], skipped: &[&str]) -> Vec<Self> {
        let checks: Vec<Self> = super::row_checks()
            .into_iter()
            .filter(|check| folders.contains(&check.folder()))
            .collect();

        for query in skipped {
            assert!(
                checks.iter().any(|check| check.query == *query),
                "{query} is not a row query in {folders:?}"
            );
        }

        checks
            .into_iter()
            .filter(|check| !skipped.contains(&check.query))
            .collect()
    }

    /// Returns the folder of the query's file in this crate's `src`. A cache key starts with the
    /// folder and a `/` for each folder except `sql`.
    fn folder(&self) -> &'static str {
        self.query
            .split_once('/')
            .map_or("sql", |(folder, _)| folder)
    }

    /// Returns the query's name in the SQL cache.
    pub(crate) fn query(&self) -> &'static str {
        self.query
    }

    /// Returns the error of the query's reader for the prepared result's `columns`.
    pub(crate) fn check_columns(&self, columns: &ResultColumns) -> Result<()> {
        (self.check_columns)(columns)
    }
}
