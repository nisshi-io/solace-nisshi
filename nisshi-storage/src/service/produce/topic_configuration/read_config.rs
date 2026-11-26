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

use nisshi_sans_io::{describe_configs_response::DescribeConfigsResourceResult, topic_config};

use crate::{max_message_bytes, message_timestamp_type};

/// Defines [`ReadConfig`] from one line for each config that a Produce reads.
///
/// The enum, `ALL` and `key` come from the same lines, so each variant is in `ALL` and has a key.
macro_rules! read_configs {
    ($($variant:ident => $key:expr),+ $(,)?) => {
        /// A config that a Produce reads from a topic.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub(super) enum ReadConfig {
            $($variant),+
        }

        impl ReadConfig {
            /// Every variant of this enum.
            const ALL: &'static [Self] = &[$(Self::$variant),+];

            /// Returns the canonical name of the config.
            fn key(self) -> &'static str {
                match self {
                    $(Self::$variant => $key),+
                }
            }
        }
    };
}

read_configs! {
    MessageTimestampType => message_timestamp_type::KEY,
    MaxMessageBytes => max_message_bytes::KEY,
    LakeSink => topic_config::LAKE_SINK,
    SchemaValidation => topic_config::SCHEMA_VALIDATION,
}

impl ReadConfig {
    /// Returns the key of each config, and its `tansu.*` alias when it has one.
    pub(super) fn all_names() -> Vec<String> {
        Self::ALL
            .iter()
            .flat_map(|config| topic_config::names(config.key()))
            .collect()
    }
}

/// Holds the configs that a storage engine returned for a topic.
///
/// `stored_value` takes a [`ReadConfig`], so a getter reads a config by the names that
/// [`ReadConfig::all_names`] requests.
#[derive(Clone, Debug)]
pub(super) struct ReadConfigs {
    configs: Vec<DescribeConfigsResourceResult>,
}

impl ReadConfigs {
    pub(super) fn new(configs: Vec<DescribeConfigsResourceResult>) -> Self {
        Self { configs }
    }

    /// Returns the value that the topic stores for `config`, under its key or else under its
    /// `tansu.*` alias, and `None` when the topic stores no value for it.
    pub(super) fn stored_value(&self, config: ReadConfig) -> Option<&str> {
        topic_config::lookup(
            self.configs
                .iter()
                .filter_map(|stored| Some((stored.name.as_str(), stored.value.as_deref()?))),
            config.key(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored_config(name: &str, value: Option<&str>) -> DescribeConfigsResourceResult {
        DescribeConfigsResourceResult::default()
            .name(name.into())
            .value(value.map(Into::into))
    }

    #[test]
    fn every_read_config_is_named_once_with_its_key_and_alias() {
        let names = ReadConfig::all_names();

        let expected: Vec<String> = ReadConfig::ALL
            .iter()
            .flat_map(|config| topic_config::names(config.key()))
            .collect();
        assert_eq!(expected, names);

        let mut unique = names.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(names.len(), unique.len(), "{names:?}");

        for name in [
            topic_config::LAKE_SINK,
            "tansu.lake.sink",
            topic_config::SCHEMA_VALIDATION,
            "tansu.schema.validation",
        ] {
            assert!(
                names.iter().any(|listed| listed == name),
                "{name}: {names:?}"
            );
        }
    }

    #[test]
    fn a_value_is_read_from_the_key_or_from_the_alias() {
        let by_key = ReadConfigs::new(vec![stored_config(topic_config::LAKE_SINK, Some("true"))]);
        let by_alias = ReadConfigs::new(vec![stored_config("tansu.lake.sink", Some("true"))]);

        assert_eq!(Some("true"), by_key.stored_value(ReadConfig::LakeSink));
        assert_eq!(Some("true"), by_alias.stored_value(ReadConfig::LakeSink));
    }

    #[test]
    fn the_key_wins_over_the_alias_when_the_topic_stores_both() {
        let configs = ReadConfigs::new(vec![
            stored_config("tansu.lake.sink", Some("false")),
            stored_config(topic_config::LAKE_SINK, Some("true")),
        ]);

        assert_eq!(Some("true"), configs.stored_value(ReadConfig::LakeSink));
    }

    #[test]
    fn a_config_that_the_topic_does_not_store_has_no_value() {
        let configs = ReadConfigs::new(vec![stored_config(max_message_bytes::KEY, Some("1234"))]);

        assert_eq!(None, configs.stored_value(ReadConfig::LakeSink));
        assert_eq!(
            Some("1234"),
            configs.stored_value(ReadConfig::MaxMessageBytes)
        );
    }

    #[test]
    fn a_stored_config_without_a_value_has_no_value() {
        let configs = ReadConfigs::new(vec![stored_config(max_message_bytes::KEY, None)]);

        assert_eq!(None, configs.stored_value(ReadConfig::MaxMessageBytes));
    }
}
