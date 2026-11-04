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

//! The Nisshi topic configuration keys (`nisshi.*`) and their deprecated `tansu.*` aliases.
//!
//! Every reader of a Nisshi topic configuration key goes through [`lookup`] or [`resolve`], so
//! this module is the only place that knows both names. A topic keeps the key name that its
//! creator set: nothing rewrites a stored configuration, and a topic configuration that the
//! previous release stored under a `tansu.*` name keeps working.
//!
//! When a topic sets both names of one key, the `nisshi.*` value applies and the `tansu.*` value
//! is ignored.

use std::{
    borrow::Cow,
    collections::HashSet,
    sync::{
        LazyLock,
        atomic::{AtomicBool, Ordering},
    },
};

use tracing::warn;

/// Batches the Produce requests that a proxy receives for a topic when the value is `true`.
pub const BATCH: &str = "nisshi.batch";

/// The longest time in milliseconds that a proxy holds a batch before it sends it.
pub const BATCH_TIMEOUT_MS: &str = "nisshi.batch.timeout_ms";

/// The number of records at which a proxy sends a batch.
pub const BATCH_MAX_RECORDS: &str = "nisshi.batch.max_records";

/// Writes the records of a topic to the data lake when the value is `true`.
pub const LAKE_SINK: &str = "nisshi.lake.sink";

/// The comma separated columns that a lake table is partitioned by.
pub const LAKE_PARTITION: &str = "nisshi.lake.partition";

/// The comma separated columns that a lake table is Z-ordered by.
pub const LAKE_Z_ORDER: &str = "nisshi.lake.z_order";

/// Flattens nested fields into top level lake table columns when the value is `true`.
pub const LAKE_NORMALIZE: &str = "nisshi.lake.normalize";

/// The separator that joins the names of the nested fields that [`LAKE_NORMALIZE`] flattens.
pub const LAKE_NORMALIZE_SEPARATOR: &str = "nisshi.lake.normalize.separator";

/// The prefix of the keys that define a generated lake table column: the rest of the key names
/// the column, and the value is the `CAST` expression that computes it.
pub const LAKE_GENERATE_PREFIX: &str = "nisshi.lake.generate.";

/// Validates each produced batch against the topic's schema when the value is `true`, which is
/// the default.
pub const SCHEMA_VALIDATION: &str = "nisshi.schema.validation";

/// Makes `topic/key` a view of the records of `topic` that have the record key `key` when the
/// value is `true`.
pub const VIRTUAL: &str = "nisshi.virtual";

/// A `nisshi.*` key, with the `tansu.*` alias that it replaces.
///
/// A `family` is a prefix: every key that starts with `canonical` is a canonical key, and every
/// key that starts with `alias` is the alias of the key with the same suffix.
struct Key {
    canonical: &'static str,
    alias: &'static str,
    family: bool,
}

const KEYS: [Key; 11] = [
    Key {
        canonical: BATCH,
        alias: "tansu.batch",
        family: false,
    },
    Key {
        canonical: BATCH_TIMEOUT_MS,
        alias: "tansu.batch.timeout_ms",
        family: false,
    },
    Key {
        canonical: BATCH_MAX_RECORDS,
        alias: "tansu.batch.max_records",
        family: false,
    },
    Key {
        canonical: LAKE_SINK,
        alias: "tansu.lake.sink",
        family: false,
    },
    Key {
        canonical: LAKE_PARTITION,
        alias: "tansu.lake.partition",
        family: false,
    },
    Key {
        canonical: LAKE_Z_ORDER,
        alias: "tansu.lake.z_order",
        family: false,
    },
    Key {
        canonical: LAKE_NORMALIZE,
        alias: "tansu.lake.normalize",
        family: false,
    },
    Key {
        canonical: LAKE_NORMALIZE_SEPARATOR,
        alias: "tansu.lake.normalize.separator",
        family: false,
    },
    Key {
        canonical: LAKE_GENERATE_PREFIX,
        alias: "tansu.lake.generate.",
        family: true,
    },
    Key {
        canonical: SCHEMA_VALIDATION,
        alias: "tansu.schema.validation",
        family: false,
    },
    Key {
        canonical: VIRTUAL,
        alias: "tansu.virtual",
        family: false,
    },
];

impl Key {
    /// Returns the key that `name` is the alias of, or `None` when `name` is not an alias.
    ///
    /// Only the keys in [`KEYS`] have an alias: any other `tansu.*` name is an unknown key, as it
    /// was before the `nisshi.*` names existed.
    fn aliased_by(name: &str) -> Option<&'static Self> {
        KEYS.iter().find(|key| {
            if key.family {
                name.starts_with(key.alias)
            } else {
                name == key.alias
            }
        })
    }

    /// Returns the canonical name of the alias `name` of this key.
    fn canonical_name(&self, name: &str) -> String {
        format!("{}{}", self.canonical, &name[self.alias.len()..])
    }
}

/// Logs one deprecation warning for each alias, however often the alias is used.
///
/// A proxy reads the topic configuration on every Produce request, and a broker reads it on
/// every produce and fetch, so a warning for each use would flood the log. The warning names the
/// registered key, never the name or the value in the topic configuration, because a client
/// sets those.
#[derive(Debug, Default)]
pub struct AliasWarnings {
    warned: [AtomicBool; KEYS.len()],
}

impl AliasWarnings {
    /// Returns the value of the entry named `key`, or else of the entry named by the alias of
    /// `key`, so that an entry named `key` wins when both exist.
    ///
    /// Logs a deprecation warning, once for this `AliasWarnings`, when the entry that supplies
    /// the value is an alias.
    pub fn lookup<'a, V>(
        &self,
        entries: impl IntoIterator<Item = (&'a str, V)>,
        key: &str,
    ) -> Option<V> {
        let mut aliased = None;

        for (name, value) in entries {
            if name == key {
                return Some(value);
            }

            if aliased.is_none()
                && let Some(alias_key) = Key::aliased_by(name)
                && !alias_key.family
                && alias_key.canonical == key
            {
                aliased = Some((alias_key, value));
            }
        }

        aliased.map(|(alias_key, value)| {
            self.warn(alias_key);
            value
        })
    }

    /// Returns what `get` returns for `key`, or else for the alias of `key`, so that `key` wins
    /// when `get` finds both. Use it instead of [`AliasWarnings::lookup`] on a keyed collection,
    /// which `get` reads in one step for each name.
    ///
    /// Logs a deprecation warning, once for this `AliasWarnings`, when the alias supplies the
    /// value.
    pub fn lookup_by<V>(&self, key: &str, get: impl Fn(&str) -> Option<V>) -> Option<V> {
        get(key).or_else(|| {
            let alias_key = KEYS
                .iter()
                .find(|alias_key| !alias_key.family && alias_key.canonical == key)?;
            let value = get(alias_key.alias)?;
            self.warn(alias_key);
            Some(value)
        })
    }

    /// Returns `entries` with the name of each alias replaced by the canonical name, and without
    /// each alias for which `entries` also has an entry with the canonical name.
    ///
    /// Logs a deprecation warning, once for this `AliasWarnings`, for each alias in `entries`.
    pub fn resolve<'a, V>(
        &self,
        entries: impl IntoIterator<Item = (&'a str, V)>,
    ) -> Vec<(Cow<'a, str>, V)> {
        let entries = entries.into_iter().collect::<Vec<_>>();
        let names = entries
            .iter()
            .map(|(name, _)| *name)
            .collect::<HashSet<&str>>();

        entries
            .into_iter()
            .filter_map(|(name, value)| match Key::aliased_by(name) {
                Some(key) => {
                    self.warn(key);

                    let canonical_name = key.canonical_name(name);
                    (!names.contains(canonical_name.as_str()))
                        .then_some((Cow::Owned(canonical_name), value))
                }

                None => Some((Cow::Borrowed(name), value)),
            })
            .collect()
    }

    fn warn(&self, key: &Key) {
        let Some(index) = KEYS
            .iter()
            .position(|registered| registered.canonical == key.canonical)
        else {
            return;
        };

        // The load keeps the steady state, where the alias has warned already, free of
        // a write that every connection would contend for.
        let first_use = !self.warned[index].load(Ordering::Relaxed)
            && !self.warned[index].swap(true, Ordering::Relaxed);

        if first_use {
            let deprecated = if key.family {
                format!("{}*", key.alias)
            } else {
                key.alias.to_owned()
            };

            warn!(
                deprecated,
                replacement = key.canonical,
                "deprecated topic configuration key"
            );
        }
    }
}

static PROCESS: LazyLock<AliasWarnings> = LazyLock::new(AliasWarnings::default);

/// Returns the value of the entry named `key`, or else of the entry named by the `tansu.*` alias
/// of `key`, and logs a deprecation warning, once for the process, when the alias supplies it.
///
/// `key` is one of the `nisshi.*` constants of this module.
pub fn lookup<'a, V>(entries: impl IntoIterator<Item = (&'a str, V)>, key: &str) -> Option<V> {
    PROCESS.lookup(entries, key)
}

/// Returns what `get` returns for `key`, or else for the `tansu.*` alias of `key`, and logs a
/// deprecation warning, once for the process, when the alias supplies the value.
pub fn lookup_by<V>(key: &str, get: impl Fn(&str) -> Option<V>) -> Option<V> {
    PROCESS.lookup_by(key, get)
}

/// Returns `entries` with each `tansu.*` alias renamed to its `nisshi.*` key and without the
/// aliases that `entries` overrides, and logs a deprecation warning, once for the process, for
/// each alias that `entries` has.
pub fn resolve<'a, V>(entries: impl IntoIterator<Item = (&'a str, V)>) -> Vec<(Cow<'a, str>, V)> {
    PROCESS.resolve(entries)
}

/// Returns `key`, and the `tansu.*` alias of `key` when it has one: the names under which a topic
/// can store the value that [`lookup`] reads for `key`.
///
/// A caller that asks a storage engine for named configs, instead of every config, asks for each of
/// these names.
pub fn names(key: &str) -> Vec<String> {
    let alias = KEYS
        .iter()
        .find(|candidate| !candidate.family && candidate.canonical == key)
        .map(|candidate| candidate.alias);

    std::iter::once(key)
        .chain(alias)
        .map(str::to_owned)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_with_an_alias_is_named_by_both() {
        assert_eq!(
            vec![
                SCHEMA_VALIDATION.to_owned(),
                "tansu.schema.validation".to_owned()
            ],
            names(SCHEMA_VALIDATION)
        );
    }

    #[test]
    fn a_key_without_an_alias_is_named_by_itself() {
        assert_eq!(
            vec!["max.message.bytes".to_owned()],
            names("max.message.bytes")
        );
    }

    #[test]
    fn each_alias_is_its_key_with_the_old_prefix() {
        for key in &KEYS {
            assert_eq!(
                Some(key.alias),
                key.canonical
                    .strip_prefix("nisshi.")
                    .map(|rest| format!("tansu.{rest}"))
                    .as_deref()
            );
        }
    }
}
