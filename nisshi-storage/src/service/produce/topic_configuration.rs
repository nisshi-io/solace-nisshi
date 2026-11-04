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

use std::str::FromStr as _;

use nisshi_sans_io::{
    ConfigResource, TimestampType,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    topic_config,
};
use tracing::debug;

use crate::{Result, Storage, message_timestamp_type};

/// The configuration of a topic that a Produce reads before it validates a partition.
#[derive(Clone, Debug)]
pub struct TopicConfiguration {
    timestamp_type: TimestampType,
    configs: Vec<DescribeConfigsResourceResult>,
}

impl TopicConfiguration {
    /// Reads the configuration of the topic `name` from `storage`.
    ///
    /// The read and `storage.produce()` run in separate transactions. If the topic's
    /// `message.timestamp.type` changes between them, the batch keeps the type that this function
    /// read. The batch stores its type in its attributes, so a consumer still reads its timestamps
    /// correctly. Kafka also applies a config change asynchronously, and an append that is in
    /// progress can use the old type.
    ///
    /// The read asks for every config, because a stored `tansu.*` alias of a key that
    /// [`TopicConfiguration::schema_validation`] reads is not named by that key.
    pub async fn read<G>(storage: &G, name: &str) -> Result<Self>
    where
        G: Storage,
    {
        let described = storage
            .describe_config(name, ConfigResource::Topic, None)
            .await?;

        Self::described(name, described)
    }

    /// Returns the configuration of the topic `name` from `described`, its `DescribeConfigs`
    /// result for every config, with the errors of [`message_timestamp_type::described`].
    pub fn described(name: &str, described: DescribeConfigsResult) -> Result<Self> {
        let configs = described.configs.clone().unwrap_or_default();

        message_timestamp_type::described(name, described).map(|timestamp_type| Self {
            timestamp_type,
            configs,
        })
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
}
