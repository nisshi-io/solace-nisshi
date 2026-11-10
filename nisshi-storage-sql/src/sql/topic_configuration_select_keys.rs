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

//! The PostgreSQL query that reads the configs among some keys that a topic sets. It returns no row
//! when the topic does not exist, and one row with a null name when the topic sets none of the
//! keys.
//!
//! The query takes three parameters: `$1` is the cluster's name, `$2` the topic's name, and `$3`
//! the array of config keys to read.

use super::Query;

/// The query's name in the SQL cache.
pub(crate) const QUERY: Query<Row> = Query::new("pg/topic_configuration_select_keys.sql");

sql_row! {
    /// One config among the requested keys that the topic sets, or the one row with a null name
    /// for a topic that sets none of them.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The config's key, or `None` in the row for a topic that sets none of the keys.
        name: Option<String>,
        /// The config's stored value.
        value: Option<String>,
    }
}

/// One config that the topic sets.
#[cfg(feature = "postgres")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Config {
    /// The config's key.
    pub(crate) name: String,
    /// The config's stored value.
    pub(crate) value: Option<String>,
}

#[cfg(feature = "postgres")]
impl Row {
    /// Returns the row's config, or `None` for the row of a topic that sets none of the keys,
    /// which is not a config.
    pub(crate) fn config(self) -> Option<Config> {
        self.name.map(|name| Config {
            name,
            value: self.value,
        })
    }
}
