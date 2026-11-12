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

//! The index of each column in a query's result, found by the column's name.

use nisshi_storage::{Error, Result};

/// A driver's result or prepared statement, which names the columns of a query's result.
pub(crate) trait ColumnNames {
    /// Returns the name of each column, in order.
    fn column_names(&self) -> Vec<String>;
}

/// The column names of one query's result, in order, as the engine's driver reports them.
#[derive(Clone, Debug)]
pub(crate) struct ResultColumns {
    query: &'static str,
    names: Vec<String>,
}

impl ResultColumns {
    /// Returns the column names of `source`, the result or the prepared statement of `query`.
    pub(crate) fn new(query: &'static str, source: &impl ColumnNames) -> Self {
        Self {
            query,
            names: source.column_names(),
        }
    }

    /// Returns the query's name in the SQL cache.
    pub(crate) fn query(&self) -> &'static str {
        self.query
    }

    /// Returns the index of the column `name`. Returns [`Error::MissingColumn`] if the result does
    /// not have that column, and [`Error::DuplicateColumn`] if it has more than one.
    pub(crate) fn index(&self, name: &'static str) -> Result<usize> {
        let mut indexes = self
            .names
            .iter()
            .enumerate()
            .filter(|(_, found)| *found == name)
            .map(|(index, _)| index);

        let index = indexes.next().ok_or_else(|| Error::MissingColumn {
            query: self.query,
            column: name,
            found: self.names.clone(),
        })?;

        if indexes.next().is_some() {
            return Err(Error::DuplicateColumn {
                query: self.query,
                column: name,
            });
        }

        Ok(index)
    }
}
