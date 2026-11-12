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

use std::fmt;

/// The text the harness passes to the broker's `--storage-engine` option. The harness doesn't
/// parse it, because the tests check how the broker parses it.
#[derive(Clone, Debug)]
pub struct StorageUrl(String);

impl StorageUrl {
    /// Returns `text` as a storage URL. The harness passes it to the broker unchanged, so a test
    /// can give the broker a URL that isn't valid.
    pub fn as_given(text: impl Into<String>) -> Self {
        Self(text.into())
    }

    /// Returns this URL with `option`, a `name=value` pair, added to its query.
    pub fn with_query_option(&self, option: &str) -> Self {
        let separator = if self.0.contains('?') { '&' } else { '?' };
        Self(format!("{}{separator}{option}", self.0))
    }

    pub fn is_sqlite(&self) -> bool {
        self.0.starts_with("sqlite:")
    }

    /// Returns the path of a SQLite URL's database, as the broker reads it: the text after
    /// `sqlite://`, without the query. Returns `None` for a URL of another engine.
    pub fn sqlite_database_path(&self) -> Option<&str> {
        let path = self.0.strip_prefix("sqlite://")?;
        Some(path.split_once('?').map_or(path, |(path, _)| path))
    }
}

impl fmt::Display for StorageUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::StorageUrl;

    #[test]
    fn sqlite_database_path_leaves_out_the_query() {
        let storage = StorageUrl::as_given("sqlite://nisshi.db").with_query_option("vacuum_into=x");

        assert_eq!(storage.sqlite_database_path(), Some("nisshi.db"));
    }

    #[test]
    fn sqlite_database_path_is_none_for_another_engine() {
        let storage = StorageUrl::as_given("postgres://postgres:postgres@localhost");

        assert_eq!(storage.sqlite_database_path(), None);
    }
}
