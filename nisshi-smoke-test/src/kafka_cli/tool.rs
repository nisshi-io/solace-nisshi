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

use serde::de::DeserializeOwned;

/// A Kafka command-line tool in `/opt/kafka/bin` that the suite runs.
#[derive(Clone, Copy, Debug)]
pub(super) enum Tool {
    Acls,
    BrokerApiVersions,
    Cluster,
    Configs,
    ConsoleConsumer,
    ConsoleProducer,
    ConsumerGroups,
    DeleteRecords,
    GetOffsets,
    ProducerPerfTest,
    ReassignPartitions,
    Topics,
    VerifiableConsumer,
    VerifiableProducer,
}

impl Tool {
    /// Returns the tool's name, which is also its script's file name in `/opt/kafka/bin` without
    /// `.sh`.
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Acls => "kafka-acls",
            Self::BrokerApiVersions => "kafka-broker-api-versions",
            Self::Cluster => "kafka-cluster",
            Self::Configs => "kafka-configs",
            Self::ConsoleConsumer => "kafka-console-consumer",
            Self::ConsoleProducer => "kafka-console-producer",
            Self::ConsumerGroups => "kafka-consumer-groups",
            Self::DeleteRecords => "kafka-delete-records",
            Self::GetOffsets => "kafka-get-offsets",
            Self::ProducerPerfTest => "kafka-producer-perf-test",
            Self::ReassignPartitions => "kafka-reassign-partitions",
            Self::Topics => "kafka-topics",
            Self::VerifiableConsumer => "kafka-verifiable-consumer",
            Self::VerifiableProducer => "kafka-verifiable-producer",
        }
    }

    /// Returns the option that gives the tool a client config file, such as the one that logs in.
    /// The tools don't share one name for it.
    pub(super) fn client_config_option(self) -> &'static str {
        match self {
            Self::ConsoleProducer | Self::ProducerPerfTest | Self::VerifiableProducer => {
                "--producer.config"
            }
            Self::ConsoleConsumer | Self::VerifiableConsumer => "--consumer.config",
            Self::Cluster => "--config",
            Self::Acls
            | Self::BrokerApiVersions
            | Self::Configs
            | Self::ConsumerGroups
            | Self::DeleteRecords
            | Self::GetOffsets
            | Self::ReassignPartitions
            | Self::Topics => "--command-config",
        }
    }

    /// Returns the arguments that give the tool the broker's address.
    pub(super) fn bootstrap_arguments(self, bootstrap: &str) -> [String; 2] {
        match self {
            // `kafka-producer-perf-test` in Kafka 3.9 and earlier does not accept
            // `--bootstrap-server`. Kafka 4.3 accepts it, and marks `--producer-props` and
            // `--producer.config` deprecated.
            Self::ProducerPerfTest => [
                "--producer-props".to_owned(),
                format!("bootstrap.servers={bootstrap}"),
            ],
            _ => ["--bootstrap-server".to_owned(), bootstrap.to_owned()],
        }
    }

    /// Reads an event from one line that the tool printed as JSON, or returns `None` for a line
    /// that isn't a JSON object, such as a warning that Kafka 4 tools print on stdout.
    ///
    /// Panics with the line if it is a JSON object that doesn't read as an `Event`, because then
    /// the tool prints an event in a layout this reader doesn't know. Returning `None` would
    /// make a test report a broker that lost the event.
    #[track_caller]
    pub(super) fn read_json_event<Event: DeserializeOwned>(self, line: &str) -> Option<Event> {
        let Ok(object @ serde_json::Value::Object(_)) = serde_json::from_str(line) else {
            return None;
        };

        match serde_json::from_value(object) {
            Ok(event) => Some(event),
            Err(err) => panic!(
                "{} printed an event that this reader doesn't know: {err}\n{line}",
                self.name()
            ),
        }
    }
}
