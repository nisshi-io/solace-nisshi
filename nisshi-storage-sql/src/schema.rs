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

//! What a SQL engine does at startup with the schema version that its database records.

use nisshi_storage::{Error, Result};
use tracing::info;

/// The schema version that a database records in its `nisshi_schema_version` table.
///
/// An older broker reads this row to decide whether it can use the database, so every release
/// must keep the table. An older broker that finds no `nisshi_schema_version` table creates its
/// own schema there. A release that moves to another way of tracking the schema can stop
/// updating the row once it has raised `oldest_compatible_version` to its own version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecordedSchema {
    /// The version of the database's schema.
    pub(crate) version: i32,
    /// The oldest schema version whose broker can use the database's schema.
    pub(crate) oldest_compatible_version: i32,
}

/// Returns whether to create the schema of `schema_version` in a database of `recorded`. The
/// function returns `true` only when the database records no version.
///
/// Returns [`Error::IncompatibleSchema`] when a broker of `schema_version` can't use the recorded
/// schema. Returns [`Error::OutdatedSchema`] when the recorded schema is older than
/// `schema_version`, because a broker can't upgrade a schema.
pub(crate) fn should_create_schema(
    recorded: Option<RecordedSchema>,
    schema_version: i32,
) -> Result<bool> {
    let Some(recorded) = recorded else {
        return Ok(true);
    };

    // The broker refuses an older schema instead of using it. The broker can't tell whether the
    // older schema has every table and column that it uses. It also can't upgrade the schema.
    if recorded.version < schema_version {
        return Err(Error::OutdatedSchema {
            recorded_version: recorded.version,
            schema_version,
        });
    }

    if recorded.oldest_compatible_version > schema_version {
        return Err(Error::IncompatibleSchema {
            recorded_version: recorded.version,
            oldest_compatible_version: recorded.oldest_compatible_version,
            schema_version,
        });
    }

    if recorded.version > schema_version {
        info!(
            recorded_version = recorded.version,
            schema_version, "the database records a newer schema that this release can use"
        );
    }

    Ok(false)
}

/// Returns the SHA-256 digest of `statements`, in hex.
#[cfg(test)]
pub(crate) fn schema_digest<'a>(statements: impl IntoIterator<Item = &'a str>) -> String {
    use sha2::{Digest as _, Sha256};

    let mut hasher = Sha256::new();
    for statement in statements {
        hasher.update(statement.as_bytes());
        hasher.update(b"\0");
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;

    const SCHEMA_VERSION: i32 = 2;

    fn recorded(version: i32, oldest_compatible_version: i32) -> Option<RecordedSchema> {
        Some(RecordedSchema {
            version,
            oldest_compatible_version,
        })
    }

    #[test]
    fn schema_is_created_in_a_database_that_records_no_version() {
        assert_matches!(should_create_schema(None, SCHEMA_VERSION), Ok(true));
    }

    #[test]
    fn schema_of_the_same_version_is_kept() {
        assert_matches!(
            should_create_schema(recorded(2, 1), SCHEMA_VERSION),
            Ok(false)
        );
    }

    #[test]
    fn newer_schema_that_this_version_can_use_is_kept() {
        assert_matches!(
            should_create_schema(recorded(3, 2), SCHEMA_VERSION),
            Ok(false)
        );
    }

    #[test]
    fn older_schema_is_refused() {
        assert_matches!(
            should_create_schema(recorded(1, 1), SCHEMA_VERSION),
            Err(Error::OutdatedSchema {
                recorded_version: 1,
                schema_version: SCHEMA_VERSION,
            })
        );
    }

    #[test]
    fn newer_schema_that_this_version_cannot_use_is_refused() {
        assert_matches!(
            should_create_schema(recorded(3, 3), SCHEMA_VERSION),
            Err(Error::IncompatibleSchema {
                recorded_version: 3,
                oldest_compatible_version: 3,
                schema_version: SCHEMA_VERSION,
            })
        );
    }
}
