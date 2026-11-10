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

/// The column names of one query's result, in order, as the engine's driver reports them.
#[derive(Clone, Debug)]
pub(crate) struct ResultColumns {
    query: &'static str,
    names: Vec<String>,
}

impl ResultColumns {
    fn new(query: &'static str, names: Vec<String>) -> Self {
        Self { query, names }
    }

    /// Returns the column names of `statement`, PostgreSQL's prepared statement of `query`.
    ///
    /// The names come from the statement, not from a row, so a result without rows has them too.
    #[cfg(feature = "postgres")]
    pub(crate) fn postgres(query: &'static str, statement: &tokio_postgres::Statement) -> Self {
        Self::new(
            query,
            statement
                .columns()
                .iter()
                .map(|column| column.name().to_owned())
                .collect(),
        )
    }

    /// Returns the column names of `rows`, libSQL's result of `query`.
    #[cfg(feature = "libsql")]
    pub(crate) fn libsql(query: &'static str, rows: &libsql::Rows) -> Self {
        Self::new(
            query,
            (0..rows.column_count())
                .map(|index| rows.column_name(index).unwrap_or_default().to_owned())
                .collect(),
        )
    }

    /// Returns the column names of `statement`, libSQL's prepared statement of `query`.
    #[cfg(all(test, feature = "libsql"))]
    pub(crate) fn libsql_statement(query: &'static str, statement: &libsql::Statement) -> Self {
        Self::new(
            query,
            statement
                .columns()
                .iter()
                .map(|column| column.name().to_owned())
                .collect(),
        )
    }

    /// Returns the column names of `statement`, Turso's prepared statement of `query`.
    #[cfg(all(test, feature = "turso"))]
    pub(crate) fn turso_statement(query: &'static str, statement: &turso::Statement) -> Self {
        Self::new(query, statement.column_names())
    }

    /// Returns the column names of `rows`, Turso's result of `query`.
    #[cfg(feature = "turso")]
    pub(crate) fn turso(query: &'static str, rows: &turso::Rows) -> Self {
        Self::new(query, rows.column_names())
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
