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

/// One step that upgrades a database from the schema version before `to_version` to `to_version`.
///
/// An engine keeps its steps in an ordered list, one for each version after 1, and runs the
/// steps that a database needs in one transaction with the update of its recorded version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Migration {
    /// The schema version that the database has after this step.
    pub(crate) to_version: i32,
    /// The oldest schema version whose broker can use the schema that this step leaves.
    pub(crate) oldest_compatible_version: i32,
    /// The statements that change the schema.
    pub(crate) sql: &'static str,
}

/// The schema that a release creates, and the steps that upgrade each earlier schema to it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Schema {
    /// The version of the schema that the release creates in a new database.
    pub(crate) version: i32,
    /// The oldest schema version whose broker can use the schema that the release creates.
    pub(crate) oldest_compatible_version: i32,
    /// The steps from version 1 to `version`, in order. See [`Schema::is_consistent`].
    pub(crate) migrations: &'static [Migration],
}

impl Schema {
    /// Returns whether `migrations` has exactly one step for each version from 2 to `version`, in
    /// order, and the last step leaves `oldest_compatible_version`. A release whose list fails
    /// this check can't upgrade every database that it accepts. Each engine asserts it at build
    /// time.
    pub(crate) const fn is_consistent(&self) -> bool {
        let mut previous_compatible = 1;
        let mut index = 0;
        while index < self.migrations.len() {
            let step = &self.migrations[index];
            // `index` is a position in a list that the check below bounds, so it fits in `i32`.
            // A step can't need a release newer than itself, and a later step can't make an
            // earlier release usable again: the broker records the last step's value.
            if step.to_version != index as i32 + 2
                || step.oldest_compatible_version > step.to_version
                || step.oldest_compatible_version < previous_compatible
            {
                return false;
            }
            previous_compatible = step.oldest_compatible_version;
            index += 1;
        }

        let steps = self.migrations.len() as i32;
        steps + 1 == self.version
            && match self.migrations.last() {
                Some(last) => last.oldest_compatible_version == self.oldest_compatible_version,
                None => self.oldest_compatible_version <= self.version,
            }
    }
}

/// What a broker must do to the schema of a database before it serves a request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Action {
    /// The database records no version: create the schema.
    Create,
    /// The database records a schema that the release can use: change nothing.
    Keep,
    /// The database records an older schema: run these steps, then record the last one's version
    /// and the larger of its `oldest_compatible_version` and the recorded one.
    Upgrade(&'static [Migration]),
}

/// Returns what a broker of `schema` must do to a database of `recorded`.
///
/// Returns [`Error::IncompatibleSchema`] when a broker of `schema.version` can't use the recorded
/// schema. Returns [`Error::OutdatedSchema`] when the recorded schema is older than `schema` and
/// has no step to upgrade it, which happens only when the recorded version is below 1.
pub(crate) fn plan(recorded: Option<RecordedSchema>, schema: &Schema) -> Result<Action> {
    let Some(recorded) = recorded else {
        return Ok(Action::Create);
    };

    // A newer broker records the oldest version that can use what it left, and this check comes
    // first so that no broker upgrades a schema that it can't read.
    if recorded.oldest_compatible_version > schema.version {
        return Err(Error::IncompatibleSchema {
            recorded_version: recorded.version,
            oldest_compatible_version: recorded.oldest_compatible_version,
            schema_version: schema.version,
        });
    }

    if recorded.version < schema.version {
        // The list starts at version 2, so the steps that a recorded version needs start at
        // index `recorded.version - 1`.
        return usize::try_from(recorded.version)
            .ok()
            .filter(|version| *version >= 1)
            .and_then(|version| schema.migrations.get(version - 1..))
            .map(Action::Upgrade)
            .ok_or(Error::OutdatedSchema {
                recorded_version: recorded.version,
                schema_version: schema.version,
            });
    }

    if recorded.version > schema.version {
        info!(
            recorded_version = recorded.version,
            schema_version = schema.version,
            "the database records a newer schema that this release can use"
        );
    }

    Ok(Action::Keep)
}

/// Returns the `oldest_compatible_version` to record after `steps` ran on a database of
/// `recorded`. An upgrade never lowers it, so a database that a newer broker marked as needing a
/// newer release keeps that mark.
pub(crate) fn upgraded_compatibility(recorded: RecordedSchema, steps: &[Migration]) -> i32 {
    steps
        .last()
        .map_or(recorded.oldest_compatible_version, |last| {
            last.oldest_compatible_version
                .max(recorded.oldest_compatible_version)
        })
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

    const STEP_2: Migration = Migration {
        to_version: 2,
        oldest_compatible_version: 1,
        sql: "select 2",
    };
    const STEP_3: Migration = Migration {
        to_version: 3,
        oldest_compatible_version: 2,
        sql: "select 3",
    };
    static STEPS: [Migration; 2] = [STEP_2, STEP_3];

    static SCHEMA: Schema = Schema {
        version: 3,
        oldest_compatible_version: 2,
        migrations: &STEPS,
    };

    fn recorded(version: i32, oldest_compatible_version: i32) -> Option<RecordedSchema> {
        Some(RecordedSchema {
            version,
            oldest_compatible_version,
        })
    }

    #[test]
    fn schema_is_created_in_a_database_that_records_no_version() {
        assert_matches!(plan(None, &SCHEMA), Ok(Action::Create));
    }

    #[test]
    fn schema_of_the_same_version_is_kept() {
        assert_matches!(plan(recorded(3, 2), &SCHEMA), Ok(Action::Keep));
    }

    #[test]
    fn newer_schema_that_this_version_can_use_is_kept() {
        assert_matches!(plan(recorded(4, 3), &SCHEMA), Ok(Action::Keep));
    }

    #[test]
    fn schema_one_version_back_runs_the_last_step() {
        assert_eq!(
            plan(recorded(2, 1), &SCHEMA).unwrap(),
            Action::Upgrade(&[STEP_3])
        );
    }

    #[test]
    fn schema_of_version_1_runs_every_step_in_order() {
        assert_eq!(
            plan(recorded(1, 1), &SCHEMA).unwrap(),
            Action::Upgrade(&[STEP_2, STEP_3])
        );
    }

    #[test]
    fn schema_without_a_valid_version_has_no_upgrade() {
        assert_matches!(
            plan(recorded(0, 1), &SCHEMA),
            Err(Error::OutdatedSchema {
                recorded_version: 0,
                schema_version: 3,
            })
        );
    }

    #[test]
    fn newer_schema_that_this_version_cannot_use_is_refused() {
        assert_matches!(
            plan(recorded(4, 4), &SCHEMA),
            Err(Error::IncompatibleSchema {
                recorded_version: 4,
                oldest_compatible_version: 4,
                schema_version: 3,
            })
        );
    }

    #[test]
    fn upgrade_keeps_a_stricter_recorded_compatibility() {
        let recorded = RecordedSchema {
            version: 1,
            oldest_compatible_version: 3,
        };
        assert_eq!(upgraded_compatibility(recorded, &[STEP_2, STEP_3]), 3);
    }

    #[test]
    fn upgrade_records_the_compatibility_of_the_last_step() {
        let recorded = RecordedSchema {
            version: 1,
            oldest_compatible_version: 1,
        };
        assert_eq!(upgraded_compatibility(recorded, &[STEP_2, STEP_3]), 2);
    }

    #[test]
    fn schema_is_consistent_when_it_has_one_step_for_each_version() {
        assert!(SCHEMA.is_consistent());
        assert!(
            Schema {
                version: 1,
                oldest_compatible_version: 1,
                migrations: &[],
            }
            .is_consistent()
        );
    }

    #[test]
    fn schema_is_inconsistent_when_a_step_lowers_or_overshoots_compatibility() {
        const LOWERS: [Migration; 2] = [
            Migration {
                to_version: 2,
                oldest_compatible_version: 2,
                sql: "select 2",
            },
            Migration {
                to_version: 3,
                oldest_compatible_version: 1,
                sql: "select 3",
            },
        ];
        assert!(
            !Schema {
                oldest_compatible_version: 1,
                migrations: &LOWERS,
                ..SCHEMA
            }
            .is_consistent()
        );

        const OVERSHOOTS: [Migration; 1] = [Migration {
            to_version: 2,
            oldest_compatible_version: 3,
            sql: "select 2",
        }];
        assert!(
            !Schema {
                version: 2,
                oldest_compatible_version: 3,
                migrations: &OVERSHOOTS,
            }
            .is_consistent()
        );
    }

    #[test]
    fn schema_is_inconsistent_with_a_missing_or_misplaced_step() {
        assert!(
            !Schema {
                version: 4,
                ..SCHEMA
            }
            .is_consistent()
        );
        assert!(
            !Schema {
                migrations: &[STEP_3, STEP_2],
                ..SCHEMA
            }
            .is_consistent()
        );
        assert!(
            !Schema {
                oldest_compatible_version: 1,
                ..SCHEMA
            }
            .is_consistent()
        );
    }
}
