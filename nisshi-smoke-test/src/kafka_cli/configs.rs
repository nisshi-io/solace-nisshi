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

//! `kafka-configs`, which describes and changes the configs of topics, brokers and users.

use super::{KafkaCli, Output, ScramMechanism, Tool};

/// How `kafka-configs --describe --entity-type users` starts the line that lists a user's SCRAM
/// mechanisms, before the user's name.
const SCRAM_CREDENTIALS_LINE_PREFIX: &str = "SCRAM credential configs for user-principal '";
/// What `kafka-configs --describe --entity-type users` prints between the user's name and the
/// mechanisms, which it separates with [`SCRAM_MECHANISM_SEPARATOR`].
const SCRAM_CREDENTIALS_LINE_INFIX: &str = "' are ";
/// What `kafka-configs --describe --entity-type users` prints between a user's mechanisms.
const SCRAM_MECHANISM_SEPARATOR: &str = ", ";

/// The command of the [`Output`] of `kafka-configs --describe`, from
/// [`KafkaConfigs::describe_topic`] or [`KafkaConfigs::describe_broker_defaults`].
#[derive(Clone, Copy, Debug)]
pub enum DescribeConfigs {}

/// The command of [`KafkaConfigs::describe_users`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum DescribeUsers {}

/// Runs `kafka-configs` against the broker of a [`KafkaCli`].
#[derive(Clone, Copy, Debug)]
pub struct KafkaConfigs<'cli> {
    cli: &'cli KafkaCli,
}

impl<'cli> KafkaConfigs<'cli> {
    pub(super) fn new(cli: &'cli KafkaCli) -> Self {
        Self { cli }
    }
    /// Describes `topic`'s config overrides, one `<name>=<value> sensitive=<bool> synonyms={...}`
    /// line each. With `all_configs`, it describes every config the topic has, defaults included.
    ///
    /// Sends `DescribeConfigs`.
    #[track_caller]
    pub fn describe_topic(&self, topic: &str, all_configs: bool) -> Output<DescribeConfigs> {
        let mut args = vec![
            "--entity-type",
            "topics",
            "--entity-name",
            topic,
            "--describe",
        ];

        if all_configs {
            args.push("--all");
        }

        self.cli.run(Tool::Configs, &args)
    }

    /// Sets `configs`, each a `name=value` topic config, on `topic`.
    ///
    /// Sends `IncrementalAlterConfigs`.
    #[track_caller]
    pub fn add_to_topic(&self, topic: &str, configs: &[&str]) -> Output {
        let configs = configs.join(",");

        self.cli.run(
            Tool::Configs,
            &[
                "--entity-type",
                "topics",
                "--entity-name",
                topic,
                "--alter",
                "--add-config",
                &configs,
            ],
        )
    }

    /// Removes the topic configs `names` from `topic`, so the topic uses the default values of
    /// those configs.
    ///
    /// Sends `IncrementalAlterConfigs`.
    #[track_caller]
    pub fn delete_from_topic(&self, topic: &str, names: &[&str]) -> Output {
        let names = names.join(",");

        self.cli.run(
            Tool::Configs,
            &[
                "--entity-type",
                "topics",
                "--entity-name",
                topic,
                "--alter",
                "--delete-config",
                &names,
            ],
        )
    }

    /// Describes the cluster-wide broker defaults that `--entity-default` selects.
    ///
    /// Sends `DescribeConfigs`.
    #[track_caller]
    pub fn describe_broker_defaults(&self) -> Output<DescribeConfigs> {
        self.cli.run(
            Tool::Configs,
            &["--entity-type", "brokers", "--entity-default", "--describe"],
        )
    }

    /// Describes each broker that the cluster lists, by its id, and then the broker defaults.
    ///
    /// Sends `DescribeCluster`, then `DescribeConfigs`.
    #[track_caller]
    pub fn describe_brokers(&self) -> Output<DescribeConfigs> {
        self.cli
            .run(Tool::Configs, &["--entity-type", "brokers", "--describe"])
    }

    /// Creates credentials for `user` with each mechanism in [`ScramMechanism::ALL`].
    ///
    /// Sends `AlterUserScramCredentials`.
    #[track_caller]
    pub fn add_scram_user(&self, user: &str, password: &str) -> Output {
        let credentials = ScramMechanism::ALL
            .map(|mechanism| format!("{}=[password={password}]", mechanism.name()))
            .join(",");

        self.cli.run(
            Tool::Configs,
            &[
                "--entity-type",
                "users",
                "--entity-name",
                user,
                "--alter",
                "--add-config",
                &credentials,
            ],
        )
    }

    /// Describes every user's SCRAM credentials, with one
    /// `SCRAM credential configs for user-principal '<user>' are <mechanism>=iterations=<n>, ...`
    /// line per user.
    ///
    /// Sends `DescribeUserScramCredentials`.
    #[track_caller]
    pub fn describe_users(&self) -> Output<DescribeUsers> {
        self.cli
            .run(Tool::Configs, &["--entity-type", "users", "--describe"])
    }
}

impl Output<DescribeUsers> {
    /// Returns the names of the SCRAM mechanisms that `user` has credentials for, such as
    /// `SCRAM-SHA-256`, or `None` if the output doesn't list `user`.
    pub fn scram_mechanisms(&self, user: &str) -> Option<Vec<&str>> {
        let user_line_start =
            format!("{SCRAM_CREDENTIALS_LINE_PREFIX}{user}{SCRAM_CREDENTIALS_LINE_INFIX}");

        self.lines().iter().find_map(|line| {
            let mechanisms = line.strip_prefix(&user_line_start)?;

            Some(
                mechanisms
                    .split(SCRAM_MECHANISM_SEPARATOR)
                    .filter_map(|mechanism| Some(mechanism.split_once('=')?.0))
                    .collect(),
            )
        })
    }
}

impl Output<DescribeConfigs> {
    /// Returns whether the output shows `config`, a `name=value` pair.
    pub fn describes_config(&self, config: &str) -> bool {
        self.lines()
            .iter()
            .any(|line| line.trim_start().starts_with(&format!("{config} ")))
    }
}
