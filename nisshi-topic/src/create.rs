// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::{
    collections::HashMap,
    fmt::{self, Display},
    io::{self, Write},
};

use nisshi_client::{Client, ConnectionManager, Security};
use nisshi_sans_io::{
    CreateTopicsRequest, CreateTopicsResponse, ErrorCode,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    create_topics_response::CreatableTopicResult,
};
use tracing::debug;
use url::Url;

use crate::{Error, Result};

use super::Topic;

#[derive(Clone, Debug, Default)]
pub struct Builder<B, N, P> {
    broker: B,
    security: Security,
    name: N,
    partitions: P,
    configs: HashMap<String, String>,
    confirm: bool,
}

impl<B, N, P> Builder<B, N, P> {
    /// How the connection to the broker is secured, by default not at all.
    pub fn security(self, security: Security) -> Self {
        Self { security, ..self }
    }

    pub fn broker(self, broker: Url) -> Builder<Url, N, P> {
        Builder {
            broker,
            security: self.security,
            name: self.name,
            partitions: self.partitions,
            configs: self.configs,
            confirm: self.confirm,
        }
    }

    pub fn name(self, name: impl Into<String>) -> Builder<B, String, P> {
        Builder {
            broker: self.broker,
            security: self.security,
            name: name.into(),
            partitions: self.partitions,
            configs: self.configs,
            confirm: self.confirm,
        }
    }

    pub fn partitions(self, partitions: i32) -> Builder<B, N, i32> {
        Builder {
            broker: self.broker,
            security: self.security,
            name: self.name,
            partitions,
            configs: self.configs,
            confirm: self.confirm,
        }
    }

    pub fn config(self, configs: HashMap<String, String>) -> Builder<B, N, P> {
        Self { configs, ..self }
    }

    /// Print a confirmation line to stdout on success. Off by default, so a library caller gets no output.
    pub fn confirm(self, confirm: bool) -> Builder<B, N, P> {
        Self { confirm, ..self }
    }
}

impl Builder<Url, String, i32> {
    pub fn build(self) -> Topic {
        Topic::Create(Configuration {
            broker: self.broker,
            security: self.security,
            name: self.name,
            partitions: self.partitions,
            configs: self.configs,
            confirm: self.confirm,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Configuration {
    broker: Url,
    security: Security,
    name: String,
    partitions: i32,
    configs: HashMap<String, String>,
    confirm: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Create {
    configuration: Configuration,
}

impl TryFrom<Configuration> for Create {
    type Error = Error;

    fn try_from(configuration: Configuration) -> Result<Self, Self::Error> {
        Ok(Create { configuration })
    }
}

impl Create {
    pub(crate) async fn main(self) -> Result<ErrorCode> {
        let client = ConnectionManager::builder(self.configuration.broker.clone())
            .security(self.configuration.security.clone())
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        let timeout_ms = 30_000;
        let validate_only = Some(false);

        let req = CreateTopicsRequest::default()
            .topics(Some(
                [CreatableTopic::default()
                    .name(self.configuration.name.clone())
                    .num_partitions(self.configuration.partitions)
                    .replication_factor(-1)
                    .assignments(Some([].into()))
                    .configs(Some(
                        self.configuration
                            .configs
                            .into_iter()
                            .map(|(name, value)| {
                                CreatableTopicConfig::default()
                                    .name(name)
                                    .value(Some(value))
                            })
                            .collect(),
                    ))]
                .into(),
            ))
            .timeout_ms(timeout_ms)
            .validate_only(validate_only);

        let CreateTopicsResponse { topics, .. } = client
            .call(req)
            .await
            .inspect(|response| debug!(?response))?;

        let topics = topics.unwrap_or_default();
        assert_eq!(1, topics.len());

        let CreatableTopicResult {
            error_code,
            num_partitions,
            ..
        } = topics
            .first()
            .ok_or_else(|| Error::Message(format!("expected exactly one response: {topics:?}")))?;

        let error_code = ErrorCode::try_from(error_code)?;

        if error_code == ErrorCode::None && self.configuration.confirm {
            // A response before version 5 omits the partition count, and a request may leave the
            // count to the broker with -1, so the line names the count only when one is positive.
            let partitions = num_partitions
                .filter(|partitions| *partitions > 0)
                .or(Some(self.configuration.partitions).filter(|partitions| *partitions > 0));

            // The line names the topic that the user asked for, not the name in the response,
            // because the response comes from the network and could carry terminal escape
            // sequences.
            let created = Created {
                name: self.configuration.name,
                partitions,
            };

            // The broker has already created the topic. A failure to print the line, such as a
            // closed pipe, is ignored instead of panicking in `println!`, because a non-zero exit
            // would send a caller that retries into "topic already exists".
            _ = writeln!(io::stdout(), "{created}").inspect_err(|error| debug!(?error));
        }

        Ok(error_code)
    }
}

/// The line `topic create` prints when the broker has created the topic.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Created {
    name: String,
    /// The topic's partition count, `None` when neither the response nor the request gives one.
    partitions: Option<i32>,
}

impl Display for Created {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.partitions {
            Some(1) => write!(f, "Created topic {} with 1 partition.", self.name),
            Some(partitions) => write!(
                f,
                "Created topic {} with {partitions} partitions.",
                self.name
            ),
            None => write!(f, "Created topic {}.", self.name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Created;

    fn created(partitions: Option<i32>) -> String {
        Created {
            name: "taxi".into(),
            partitions,
        }
        .to_string()
    }

    #[test]
    fn created_names_the_topic_and_its_partitions() {
        assert_eq!("Created topic taxi with 3 partitions.", created(Some(3)));
    }

    #[test]
    fn created_with_one_partition_is_singular() {
        assert_eq!("Created topic taxi with 1 partition.", created(Some(1)));
    }

    #[test]
    fn created_without_a_partition_count_names_only_the_topic() {
        assert_eq!("Created topic taxi.", created(None));
    }
}
