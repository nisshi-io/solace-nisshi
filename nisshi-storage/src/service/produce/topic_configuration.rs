// Copyright ⓒ Solace Corporation
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

use std::str::FromStr as _;

use nisshi_sans_io::{
    ConfigResource, ErrorCode, TimestampType,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    topic_config,
};
use tracing::debug;

use crate::{Result, Storage, max_message_bytes, message_timestamp_type};

/// The configuration of a topic that a Produce reads before it validates a partition.
#[derive(Clone, Debug)]
pub struct TopicConfiguration {
    topic: String,
    exists: bool,
    timestamp_type: TimestampType,
    configs: Vec<DescribeConfigsResourceResult>,
}

impl TopicConfiguration {
    /// Reads the configuration of the topic `name` from `storage`.
    ///
    /// The read and `storage.produce_inflated()` run in separate transactions. If the topic's
    /// `message.timestamp.type` changes between them, the batch keeps the type that this function
    /// read. The batch stores its type in its attributes, so a consumer still reads its timestamps
    /// correctly. Kafka also applies a config change asynchronously, and an append that is in
    /// progress can use the old type.
    ///
    /// The read names the configs that this type reads, with the `tansu.*` alias of each. The
    /// PostgreSQL engine answers a read of named configs with one query, and a read of every
    /// config with two, and a Produce reads the configuration of every topic that it writes.
    pub async fn read<G>(storage: &G, name: &str) -> Result<Self>
    where
        G: Storage,
    {
        let keys = [
            vec![message_timestamp_type::KEY.to_owned()],
            topic_config::names(topic_config::SCHEMA_VALIDATION),
            topic_config::names(topic_config::LAKE_SINK),
            vec![max_message_bytes::KEY.to_owned()],
        ]
        .concat();

        let described = storage
            .describe_config(name, ConfigResource::Topic, Some(&keys))
            .await?;

        Self::described(name, described)
    }

    /// Returns the configuration of the topic `name` from `described`, its `DescribeConfigs`
    /// result for every config, with the errors of [`message_timestamp_type::described`].
    pub fn described(name: &str, described: DescribeConfigsResult) -> Result<Self> {
        let configs = described.configs.clone().unwrap_or_default();

        let exists = ErrorCode::try_from(described.error_code)? == ErrorCode::None;

        message_timestamp_type::described(name, described).map(|timestamp_type| Self {
            topic: name.to_owned(),
            exists,
            timestamp_type,
            configs,
        })
    }

    /// Returns whether the storage holds the topic. The configuration of a topic that it does not
    /// hold has the default of each config.
    pub fn exists(&self) -> bool {
        self.exists
    }

    /// Returns the topic's `max.message.bytes`: the size of the largest batch that the topic
    /// stores, as [`max_message_bytes::limit`] reads it.
    pub fn max_message_bytes(&self) -> usize {
        max_message_bytes::limit(
            &self.topic,
            self.configs
                .iter()
                .find(|config| config.name == max_message_bytes::KEY)
                .and_then(|config| config.value.as_deref()),
        )
    }

    /// Returns whether the topic's `lake.sink` is `true`. A topic that does not set the key, or
    /// sets a value that is not a boolean, has no lake sink.
    pub fn lake_sink(&self) -> bool {
        topic_config::lookup(
            self.configs
                .iter()
                .filter_map(|config| Some((config.name.as_str(), config.value.as_deref()?))),
            topic_config::LAKE_SINK,
        )
        .and_then(|value| bool::from_str(value).ok())
        .unwrap_or(false)
    }

    /// Returns the topic's `message.timestamp.type`, as [`message_timestamp_type::described`]
    /// reads it.
    pub fn timestamp_type(&self) -> TimestampType {
        self.timestamp_type
    }

    /// Returns whether the topic's `schema.validation` turns validation against the topic's
    /// schema on. A topic that does not set the key, or sets a value that is not a boolean,
    /// validates.
    pub fn schema_validation(&self) -> bool {
        let schema_validation = topic_config::lookup(
            self.configs
                .iter()
                .filter_map(|config| Some((config.name.as_str(), config.value.as_deref()?))),
            topic_config::SCHEMA_VALIDATION,
        )
        .and_then(|value| bool::from_str(value).ok())
        .unwrap_or(true);

        debug!(schema_validation);

        schema_validation
    }

    /// Returns whether the topic's records are validated against its schema. A topic with a lake
    /// sink validates whatever its `schema.validation` is.
    pub fn validates_schema(&self) -> bool {
        self.schema_validation() || self.lake_sink()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::produce::tests::support::configuration;

    #[test]
    fn a_topic_validates_unless_it_turns_schema_validation_off() {
        assert!(configuration(&[]).validates_schema());
        assert!(configuration(&[(topic_config::SCHEMA_VALIDATION, "true")]).validates_schema());
        assert!(!configuration(&[(topic_config::SCHEMA_VALIDATION, "false")]).validates_schema());

        // A value that is not a boolean leaves validation on.
        assert!(configuration(&[(topic_config::SCHEMA_VALIDATION, "off")]).validates_schema());
    }

    #[test]
    fn a_topic_with_a_lake_sink_validates_whatever_its_schema_validation_is() {
        assert!(
            configuration(&[
                (topic_config::SCHEMA_VALIDATION, "false"),
                (topic_config::LAKE_SINK, "true"),
            ])
            .validates_schema()
        );
    }

    #[test]
    fn the_tansu_alias_turns_schema_validation_off() {
        assert!(!configuration(&[("tansu.schema.validation", "false")]).validates_schema());
    }
}
