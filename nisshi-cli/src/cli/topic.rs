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

use std::{collections::HashMap, error::Error, str::FromStr};

use crate::Result;
use clap::Subcommand;
use nisshi_sans_io::ErrorCode;
use nisshi_topic::{Format, Topic};
use url::Url;

use super::{DEFAULT_BROKER, output::Output};

#[derive(Clone, Debug, Subcommand)]
pub(super) enum Command {
    /// Create a topic
    Create {
        /// Broker URL
        #[arg(long, default_value = DEFAULT_BROKER)]
        broker: Url,

        /// The name of the topic to create
        #[clap(value_parser)]
        name: String,

        /// The number of partitions to create
        #[arg(long, default_value = "3")]
        partitions: i32,

        #[arg(long, value_parser = parse_key_val::<String, String>)]
        config: Vec<(String, String)>,

        /// Print no confirmation on success (errors still print)
        #[arg(long)]
        silent: bool,
    },

    /// Delete an existing topic
    Delete {
        /// Broker URL
        #[arg(long, default_value = DEFAULT_BROKER)]
        broker: Url,

        /// The name of the topic to delete
        #[clap(value_parser)]
        name: String,
    },

    /// List existing topics
    List {
        /// Broker URL
        #[arg(long, default_value = DEFAULT_BROKER)]
        broker: Url,

        /// Print a table of topics and partition counts, or the broker's metadata as JSON
        #[arg(long, value_enum, default_value_t = Output::Table)]
        output: Output,
    },
}

impl From<Command> for Topic {
    fn from(value: Command) -> Self {
        match value {
            Command::Create {
                broker,
                name,
                partitions,
                config,
                silent,
            } => Topic::create()
                .broker(broker)
                .name(name)
                .partitions(partitions)
                .config(HashMap::from_iter(config))
                .confirm(!silent)
                .build(),

            Command::Delete { broker, name } => Topic::delete().broker(broker).name(name).build(),

            Command::List { broker, output } => Topic::list()
                .broker(broker)
                .format(match output {
                    Output::Table => Format::Table,
                    Output::Json => Format::Json,
                })
                .build(),
        }
    }
}

impl Command {
    pub(super) async fn main(self) -> Result<ErrorCode> {
        Topic::from(self).main().await.map_err(Into::into)
    }
}

/// Parse a single key-value pair
fn parse_key_val<T, U>(s: &str) -> Result<(T, U), Box<dyn Error + Send + Sync + 'static>>
where
    T: FromStr,
    T::Err: Error + Send + Sync + 'static,
    U: FromStr,
    U::Err: Error + Send + Sync + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}

#[cfg(test)]
mod tests {
    use super::{Command, Output};
    use clap::Parser;

    #[derive(Parser)]
    struct Wrapper {
        #[command(subcommand)]
        command: Command,
    }

    fn create_is_silent(args: &[&str]) -> bool {
        let Wrapper { command } =
            Wrapper::try_parse_from(std::iter::once("topic").chain(args.iter().copied()))
                .expect("arguments parse");

        match command {
            Command::Create { silent, .. } => silent,
            Command::Delete { .. } | Command::List { .. } => panic!("expected create: {args:?}"),
        }
    }

    #[test]
    fn create_prints_its_confirmation_unless_silent() {
        assert!(!create_is_silent(&["create", "test"]));
        assert!(create_is_silent(&["create", "test", "--silent"]));
    }

    fn list_output(args: &[&str]) -> Result<Output, clap::Error> {
        let Wrapper { command } =
            Wrapper::try_parse_from(std::iter::once("topic").chain(args.iter().copied()))?;

        match command {
            Command::List { output, .. } => Ok(output),
            Command::Create { .. } | Command::Delete { .. } => panic!("expected list: {args:?}"),
        }
    }

    #[test]
    fn list_prints_a_table_unless_asked_for_json() {
        assert_eq!(Output::Table, list_output(&["list"]).unwrap());
        assert_eq!(
            Output::Json,
            list_output(&["list", "--output", "json"]).unwrap()
        );
    }

    #[test]
    fn list_rejects_an_unknown_output() {
        assert_eq!(
            clap::error::ErrorKind::InvalidValue,
            list_output(&["list", "--output", "yaml"])
                .unwrap_err()
                .kind()
        );
    }
}
