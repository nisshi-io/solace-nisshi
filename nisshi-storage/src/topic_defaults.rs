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

//! Kafka 3.9.1's topic configs with their defaults, which `DescribeConfigs` lists for each config
//! that a topic doesn't set, as Kafka does ([`ConfigHelper.scala`]).
//!
//! The broker applies only some of these configs, and a storage engine applies only some of those
//! (see `docs/compatibility.md`). We list Kafka's default for every config anyway, because a Kafka
//! client expects a topic to have each one. A listed default therefore states Kafka's value, such
//! as the 7-day `retention.ms` on the in-memory engine, which keeps every record.
//!
//! [`ConfigHelper.scala`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ConfigHelper.scala#L101-L111

use nisshi_sans_io::{
    ConfigSource, ConfigType, ErrorCode,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
};

/// A topic config that Kafka defines, with the default that Kafka 3.9.1 lists for a topic that
/// doesn't set it.
#[derive(Clone, Copy, Debug)]
struct TopicConfig {
    name: &'static str,
    default: &'static str,
    config_type: ConfigType,
}

impl TopicConfig {
    const fn new(name: &'static str, default: &'static str, config_type: ConfigType) -> Self {
        Self {
            name,
            default,
            config_type,
        }
    }

    /// Returns the `DescribeConfigs` entry of this config for a topic that doesn't set it.
    ///
    /// Kafka 3.9.1 leaves `IsDefault` false, and a v0 client then reads the entry as a topic
    /// config ([`DescribeConfigsResponse`]). We set it, so that a v0 client reads the entry as a
    /// default. A later version doesn't encode the field.
    ///
    /// Kafka lists the broker configs that set a topic default as synonyms when the request sets
    /// `IncludeSynonyms`. Nisshi has no broker configs, so the entry has no synonyms.
    ///
    /// [`DescribeConfigsResponse`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/DescribeConfigsResponse.java#L226-L246
    fn default_entry(&self) -> DescribeConfigsResourceResult {
        DescribeConfigsResourceResult::default()
            .name(self.name.into())
            .value(Some(self.default.into()))
            .read_only(false)
            .is_default(Some(true))
            .config_source(Some(ConfigSource::DefaultConfig.into()))
            .is_sensitive(false)
            .synonyms(Some([].into()))
            .config_type(Some(self.config_type.into()))
            .documentation(Some("".into()))
    }
}

/// Kafka 3.9.1's topic configs, sorted by name, with the defaults that a 3.9.1 broker lists for a
/// topic created without configs (`kafka-configs --describe --all`), and the types that
/// [`LogConfig`] defines.
///
/// The `apache/kafka:3.9.1` image sets `log.retention.hours=168` and
/// `log.segment.bytes=1073741824` in its `server.properties`. Both are equal to Kafka's code
/// defaults, so every value here is a code default.
///
/// [`LogConfig`]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java#L262-L333
const TOPIC_CONFIGS: [TopicConfig; 36] = [
    TopicConfig::new("cleanup.policy", "delete", ConfigType::List),
    TopicConfig::new("compression.gzip.level", "-1", ConfigType::Int),
    TopicConfig::new("compression.lz4.level", "9", ConfigType::Int),
    TopicConfig::new("compression.type", "producer", ConfigType::String),
    TopicConfig::new("compression.zstd.level", "3", ConfigType::Int),
    TopicConfig::new("delete.retention.ms", "86400000", ConfigType::Long),
    TopicConfig::new("file.delete.delay.ms", "60000", ConfigType::Long),
    TopicConfig::new("flush.messages", "9223372036854775807", ConfigType::Long),
    TopicConfig::new("flush.ms", "9223372036854775807", ConfigType::Long),
    TopicConfig::new(
        "follower.replication.throttled.replicas",
        "",
        ConfigType::List,
    ),
    TopicConfig::new("index.interval.bytes", "4096", ConfigType::Int),
    TopicConfig::new(
        "leader.replication.throttled.replicas",
        "",
        ConfigType::List,
    ),
    TopicConfig::new("local.retention.bytes", "-2", ConfigType::Long),
    TopicConfig::new("local.retention.ms", "-2", ConfigType::Long),
    TopicConfig::new(
        "max.compaction.lag.ms",
        "9223372036854775807",
        ConfigType::Long,
    ),
    TopicConfig::new("max.message.bytes", "1048588", ConfigType::Int),
    TopicConfig::new("message.downconversion.enable", "true", ConfigType::Boolean),
    TopicConfig::new("message.format.version", "3.0-IV1", ConfigType::String),
    TopicConfig::new(
        "message.timestamp.after.max.ms",
        "9223372036854775807",
        ConfigType::Long,
    ),
    TopicConfig::new(
        "message.timestamp.before.max.ms",
        "9223372036854775807",
        ConfigType::Long,
    ),
    TopicConfig::new(
        "message.timestamp.difference.max.ms",
        "9223372036854775807",
        ConfigType::Long,
    ),
    TopicConfig::new("message.timestamp.type", "CreateTime", ConfigType::String),
    TopicConfig::new("min.cleanable.dirty.ratio", "0.5", ConfigType::Double),
    TopicConfig::new("min.compaction.lag.ms", "0", ConfigType::Long),
    TopicConfig::new("min.insync.replicas", "1", ConfigType::Int),
    TopicConfig::new("preallocate", "false", ConfigType::Boolean),
    TopicConfig::new("remote.log.copy.disable", "false", ConfigType::Boolean),
    TopicConfig::new("remote.log.delete.on.disable", "false", ConfigType::Boolean),
    TopicConfig::new("remote.storage.enable", "false", ConfigType::Boolean),
    TopicConfig::new("retention.bytes", "-1", ConfigType::Long),
    TopicConfig::new("retention.ms", "604800000", ConfigType::Long),
    TopicConfig::new("segment.bytes", "1073741824", ConfigType::Int),
    TopicConfig::new("segment.index.bytes", "10485760", ConfigType::Int),
    TopicConfig::new("segment.jitter.ms", "0", ConfigType::Long),
    TopicConfig::new("segment.ms", "604800000", ConfigType::Long),
    TopicConfig::new(
        "unclean.leader.election.enable",
        "false",
        ConfigType::Boolean,
    ),
];

/// Returns Kafka's topic config `name`, or `None` when Kafka doesn't define it.
fn topic_config(name: &str) -> Option<&'static TopicConfig> {
    TOPIC_CONFIGS
        .binary_search_by(|config| config.name.cmp(name))
        .ok()
        .map(|index| &TOPIC_CONFIGS[index])
}

/// Whether the request selects the config `name`. An absent or empty `keys` selects every config,
/// as in Kafka.
fn is_selected(name: &str, keys: Option<&[String]>) -> bool {
    keys.is_none_or(|keys| keys.is_empty() || keys.iter().any(|key| key == name))
}

/// Adds Kafka's topic defaults to `result`, the configs that a storage engine describes for a
/// topic, so that `result` lists every topic config as Kafka 3.9.1 does.
///
/// It keeps only the configs that `keys` selects, and sorts them by name. A config that the topic
/// sets keeps its value and its source, and takes its type from Kafka, such as `LONG` for
/// `retention.ms`. A config that Kafka doesn't define keeps the type that the engine gives it.
/// Each Kafka config that the topic doesn't set is added as a [`ConfigSource::DefaultConfig`].
///
/// It leaves `result` unchanged when its error code isn't [`ErrorCode::None`], such as for an
/// unknown topic.
pub(crate) fn add_to(result: &mut DescribeConfigsResult, keys: Option<&[String]>) {
    if result.error_code != i16::from(ErrorCode::None) {
        return;
    }

    let mut configs: Vec<DescribeConfigsResourceResult> = result
        .configs
        .take()
        .unwrap_or_default()
        .into_iter()
        .filter(|config| is_selected(&config.name, keys))
        .map(|config| match topic_config(&config.name) {
            Some(topic_config) => config.config_type(Some(topic_config.config_type.into())),
            None => config,
        })
        .collect();

    let defaults: Vec<DescribeConfigsResourceResult> = TOPIC_CONFIGS
        .iter()
        .filter(|topic_config| is_selected(topic_config.name, keys))
        .filter(|topic_config| {
            !configs
                .iter()
                .any(|config| config.name == topic_config.name)
        })
        .map(TopicConfig::default_entry)
        .collect();

    configs.extend(defaults);
    configs.sort_by(|a, b| a.name.cmp(&b.name));
    result.configs = Some(configs);
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;
    use crate::{config, max_message_bytes, message_timestamp_type};

    #[test]
    fn topic_configs_are_sorted_and_unique() {
        assert!(
            TOPIC_CONFIGS
                .windows(2)
                .all(|pair| pair[0].name < pair[1].name)
        );
    }

    #[test]
    fn every_list_config_is_typed_list() {
        for name in config::TOPIC_LIST_CONFIGS {
            assert_matches!(
                topic_config(name),
                Some(TopicConfig {
                    config_type: ConfigType::List,
                    ..
                }),
                "{name}"
            );
        }
    }

    #[test]
    fn max_message_bytes_default_is_the_limit_that_produce_enforces() {
        assert_eq!(
            Some(max_message_bytes::DEFAULT.to_string().as_str()),
            topic_config(max_message_bytes::KEY).map(|config| config.default)
        );
    }

    #[test]
    fn retention_ms_default_is_the_retention_that_maintenance_enforces() {
        assert_eq!(
            Some(config::DEFAULT_RETENTION.as_millis().to_string().as_str()),
            topic_config("retention.ms").map(|config| config.default)
        );
    }

    #[test]
    fn cleanup_policy_default_is_the_policy_that_alter_appends_to() {
        assert_eq!(
            Some(config::DEFAULT_CLEANUP_POLICY),
            topic_config("cleanup.policy").map(|config| config.default)
        );
    }

    #[test]
    fn message_timestamp_type_default_is_the_type_that_produce_applies() {
        let listed = topic_config(message_timestamp_type::KEY).map(|config| config.default);

        assert_eq!(
            Some(Ok(message_timestamp_type::timestamp_type("topic", None))),
            listed.map(message_timestamp_type::parse)
        );
    }

    #[test]
    fn unknown_topic_is_unchanged() {
        let mut result = DescribeConfigsResult::default()
            .error_code(ErrorCode::UnknownTopicOrPartition.into())
            .configs(Some([].into()));
        let expected = result.clone();

        add_to(&mut result, None);

        assert_eq!(expected, result);
    }

    #[test]
    fn stored_config_takes_kafka_type_and_keeps_its_source() {
        let mut result = DescribeConfigsResult::default()
            .error_code(ErrorCode::None.into())
            .configs(Some(vec![
                DescribeConfigsResourceResult::default()
                    .name("retention.ms".into())
                    .value(Some("1".into()))
                    .config_source(Some(ConfigSource::DynamicTopicConfig.into()))
                    .config_type(Some(ConfigType::String.into())),
            ]));

        add_to(&mut result, Some(&["retention.ms".into()]));

        assert_eq!(
            Some(vec![
                DescribeConfigsResourceResult::default()
                    .name("retention.ms".into())
                    .value(Some("1".into()))
                    .config_source(Some(ConfigSource::DynamicTopicConfig.into()))
                    .config_type(Some(ConfigType::Long.into())),
            ]),
            result.configs
        );
    }
}
