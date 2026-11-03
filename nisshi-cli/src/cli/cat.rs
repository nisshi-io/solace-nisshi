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

use super::DEFAULT_BROKER;
use crate::Result;
use clap::Subcommand;
use nisshi_cat::Cat;
use nisshi_sans_io::ErrorCode;
use url::Url;

#[derive(Clone, Debug, Subcommand)]
pub(super) enum Command {
    /// Produce Avro/JSON/Protobuf messages to a topic
    Produce {
        /// The URL of the broker to produce messages into
        #[arg(long, default_value = DEFAULT_BROKER, env = "ADVERTISED_LISTENER_URL")]
        broker: Url,

        /// The topic to produce messages into
        #[clap(value_parser)]
        topic: String,

        /// Input file, or '-' for stdin. Without --schema-registry, each message is a JSON object with a "value" and an optional "key", one per line on stdin, or in a JSON array in a file
        #[clap(value_parser, default_value = "-")]
        file: String,

        /// The partition to produce messages into
        #[arg(long, default_value = "0")]
        partition: i32,

        /// Schema registry examples are: file://./etc/schema or s3://nisshi/, containing: topic.json, topic.proto or topic.avsc
        #[arg(long, env = "SCHEMA_REGISTRY")]
        schema_registry: Option<Url>,

        /// Print no confirmation on success (errors still print)
        #[arg(long)]
        silent: bool,
    },

    /// Consume Avro/JSON/Protobuf messages from a topic
    Consume {
        /// The URL of the broker to consume messages from
        #[arg(long, default_value = DEFAULT_BROKER, env = "ADVERTISED_LISTENER_URL")]
        broker: Url,

        /// The topic to consume messages from
        #[clap(value_parser)]
        topic: String,

        /// The partition to consume messages from
        #[arg(long, default_value = "0")]
        partition: i32,

        /// Schema registry examples are: file://./etc/schema or s3://nisshi/, containing: topic.json, topic.proto or topic.avsc
        #[arg(long, env = "SCHEMA_REGISTRY")]
        schema_registry: Option<Url>,

        /// The maximum time in milliseconds to wait for a message
        #[arg(long, default_value = "5000")]
        max_wait_time_ms: i32,

        /// The minimum number of bytes to wait for
        #[arg(long, default_value = "1")]
        min_bytes: i32,

        /// The maximum bytes to wait for
        #[arg(long, default_value = "52428800")]
        max_bytes: Option<i32>,

        /// The fetch offset to start from
        #[arg(long, default_value = "0")]
        fetch_offset: i64,

        /// The maximum bytes to fetch from the partition
        #[arg(long, default_value = "1048576")]
        partition_max_bytes: i32,
    },
}

impl From<Command> for Cat {
    fn from(value: Command) -> Self {
        match value {
            Command::Produce {
                broker,
                topic,
                file,
                partition,
                schema_registry,
                silent,
            } => Cat::produce()
                .broker(broker)
                .topic(topic)
                .partition(partition)
                .schema_registry(schema_registry)
                .file_name(file)
                .confirm(!silent)
                .build(),

            Command::Consume {
                broker,
                topic,
                partition,
                schema_registry,
                max_wait_time_ms,
                min_bytes,
                max_bytes,
                fetch_offset,
                partition_max_bytes,
            } => Cat::consume()
                .broker(broker)
                .topic(topic)
                .partition(partition)
                .schema_registry(schema_registry)
                .max_wait_time_ms(max_wait_time_ms)
                .min_bytes(min_bytes)
                .max_bytes(max_bytes)
                .fetch_offset(fetch_offset)
                .partition_max_bytes(partition_max_bytes)
                .build(),
        }
    }
}

impl Command {
    pub(super) async fn main(self) -> Result<ErrorCode> {
        Cat::from(self).main().await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::Command;
    use clap::Parser;

    #[derive(Parser)]
    struct Wrapper {
        #[command(subcommand)]
        command: Command,
    }

    fn broker_default(args: &[&str]) -> url::Url {
        let Wrapper { command } = temp_env::with_vars_unset(["ADVERTISED_LISTENER_URL"], || {
            Wrapper::try_parse_from(std::iter::once("cat").chain(args.iter().copied()))
                .expect("defaults parse")
        });

        match command {
            Command::Produce { broker, .. } | Command::Consume { broker, .. } => broker,
        }
    }

    /// `cat produce`/`cat consume` share `DEFAULT_BROKER` with the broker's own
    /// advertised listener, so with no override they resolve to the same IPv4 loopback
    /// address, not `localhost`.
    #[test]
    fn produce_and_consume_broker_defaults_resolve_to_loopback() {
        for args in [["produce", "test"], ["consume", "test"]] {
            assert_eq!(Some("127.0.0.1"), broker_default(&args).host_str());
        }
    }

    fn produce_is_silent(args: &[&str]) -> bool {
        let Wrapper { command } =
            Wrapper::try_parse_from(std::iter::once("cat").chain(args.iter().copied()))
                .expect("arguments parse");

        match command {
            Command::Produce { silent, .. } => silent,
            Command::Consume { .. } => panic!("expected produce: {args:?}"),
        }
    }

    #[test]
    fn produce_prints_its_confirmation_unless_silent() {
        assert!(!produce_is_silent(&["produce", "test"]));
        assert!(produce_is_silent(&["produce", "test", "--silent"]));
    }
}
