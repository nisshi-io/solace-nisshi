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

//! Nisshi CLI
//!
//! The CLI is a single statically linked binary that contains:
//! - Broker
//! - Cat: produce, validate (if backed by a schema) and fetch messages
//! - Generator: use fake data generators to produce messages with a rate limit
//! - Proxy: a Kafka API proxy
//! - Topic: Topic administration

use std::process;

use crate::Result;
use clap::{CommandFactory as _, Parser, Subcommand};
use nisshi_sans_io::ErrorCode;
use tracing::debug;

mod broker;
mod cat;
mod client_security;
mod generator;
mod missing_feature;
mod output;
mod perf;
mod proxy;
mod topic;
mod user;

// `127.0.0.1`, not `localhost`, because the broker's default advertised listener uses
// this value, and a client connects to the address it receives in Metadata. A client
// whose resolver returns `::1` first would otherwise try IPv6, which a broker that
// listens on IPv4 only refuses.
const DEFAULT_BROKER: &str = "tcp://127.0.0.1:9092";

fn storage_engines() -> Vec<&'static str> {
    vec![
        #[cfg(feature = "dynostore")]
        "dynostore",
        #[cfg(feature = "libsql")]
        "libsql",
        #[cfg(feature = "postgres")]
        "postgres",
        #[cfg(feature = "slatedb")]
        "slatedb",
        #[cfg(feature = "turso")]
        "turso",
    ]
}

fn lakes() -> Vec<&'static str> {
    vec![
        #[cfg(feature = "delta")]
        "delta",
        #[cfg(feature = "iceberg")]
        "iceberg",
    ]
}

fn after_help() -> String {
    [
        format!(
            "Storage engines: {}",
            storage_engines()
                .into_iter()
                .map(|engine| match engine {
                    // This list is built from feature names and does not read the factories, so a
                    // deprecation is also marked here by hand.
                    "slatedb" => "slatedb (deprecated)",
                    engine => engine,
                })
                .collect::<Vec<_>>()
                .join(", ")
        ),
        format!("Data lakes: {}", lakes().join(", ")),
    ]
    .join("\n")
}

#[derive(Clone, Debug, Parser)]
#[command(
    name = "nisshi",
    version,
    about,
    long_about = None,
    after_help = after_help(),
    args_conflicts_with_subcommands = true
)]
pub struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    #[clap(flatten)]
    broker: broker::Arg,
}

/// The `--help` note of each tool that connects to a broker as a Kafka client.
const CLIENT_TOOL_NOTE: &str = "Connects in plaintext and does not log in, unless given the --tls options (TLS) or --sasl options (SASL PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512). Without them it can't reach a broker listener that requires TLS or SASL. The SASL password comes from the SASL_PASSWORD environment variable or --sasl-password-file, not from an option.";

/// The `--help` note of `nisshi proxy`.
const PROXY_NOTE: &str = "For development and demos, not production.";

#[derive(Clone, Debug, Subcommand)]
enum Command {
    /// Apache Kafka®-compatible broker with Avro, JSON, Protobuf schema validation [default if no command supplied]
    Broker(Box<broker::Arg>),

    /// Consume or produce Avro, JSON or Protobuf messages to a topic
    #[command(after_help = CLIENT_TOOL_NOTE)]
    Cat {
        #[command(subcommand)]
        command: cat::Command,
    },

    /// Generate fake messages for schema-backed topics (development and demos)
    #[command(after_help = CLIENT_TOOL_NOTE)]
    Generator(Box<generator::Arg>),

    /// Measure produce and consume throughput against a broker (development and demos)
    #[command(after_help = CLIENT_TOOL_NOTE)]
    Perf(Box<perf::Arg>),

    /// Apache Kafka®-compatible proxy (development and demos)
    #[command(after_help = PROXY_NOTE)]
    Proxy(Box<proxy::Arg>),

    /// Create, list or delete topics managed by the broker
    #[command(after_help = CLIENT_TOOL_NOTE)]
    Topic {
        #[command(subcommand)]
        command: topic::Command,
    },

    /// Create, list or delete users managed by the broker
    #[command(after_help = CLIENT_TOOL_NOTE)]
    User {
        #[command(subcommand)]
        command: user::Command,
    },
}

/// How a subcommand finished.
#[derive(Clone, Copy, Debug)]
pub struct Outcome {
    pub error_code: ErrorCode,

    /// The subcommand already told the user what went wrong, so the caller does not log the
    /// code again as an error.
    pub reported: bool,
}

impl From<ErrorCode> for Outcome {
    fn from(error_code: ErrorCode) -> Self {
        Self {
            error_code,
            reported: false,
        }
    }
}

impl Cli {
    pub async fn main() -> Result<ErrorCode> {
        Self::run().await.map(|outcome| outcome.error_code)
    }

    pub async fn run() -> Result<Outcome> {
        debug!(
            pid = process::id(),
            storage = ?storage_engines(),
            lakes = ?lakes()
        );

        let cli = Cli::try_parse().unwrap_or_else(|error| {
            missing_feature::explain(
                error,
                &missing_feature::built_features(),
                &mut Cli::command(),
            )
            .exit()
        });

        match cli.command.unwrap_or(Command::Broker(Box::new(cli.broker))) {
            Command::Broker(arg) => arg.main().await.map(Outcome::from),
            Command::Cat { command } => command.main().await.map(Outcome::from),
            Command::Generator(arg) => arg.main().await.map(Outcome::from),
            Command::Perf(arg) => arg.main().await.map(Outcome::from),
            Command::Proxy(arg) => nisshi_proxy::Proxy::main(
                arg.listener_url.into_inner(),
                arg.advertised_listener_url.into_inner(),
                arg.origin_url.into_inner(),
                arg.otlp_endpoint_url
                    .map(|otlp_endpoint_url| otlp_endpoint_url.into_inner()),
            )
            .await
            .map(Outcome::from)
            .map_err(Into::into),
            Command::Topic { command } => command.main().await.map(Outcome::from),
            Command::User { command } => command.main().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EnvVarExp;
    use clap::CommandFactory;
    use url::Url;

    fn long_help(tool: &str) -> String {
        Cli::command()
            .find_subcommand_mut(tool)
            .expect(tool)
            .render_long_help()
            .to_string()
    }

    #[test]
    fn each_tool_help_states_its_role() {
        for tool in ["cat", "generator", "perf", "topic", "user"] {
            let help = long_help(tool);
            assert!(help.contains(CLIENT_TOOL_NOTE), "{tool}: {help}");
        }

        let proxy = long_help("proxy");
        assert!(proxy.contains(PROXY_NOTE), "{proxy}");

        let broker = long_help("broker");
        assert!(!broker.contains(CLIENT_TOOL_NOTE), "{broker}");
        assert!(!broker.contains(PROXY_NOTE), "{broker}");
    }

    const REGISTRY: &str = "file://./etc/schema";
    const OTHER_REGISTRY: &str = "s3://nisshi/";

    /// Parses `nisshi <args>` with `SCHEMA_REGISTRY` set to `env`, or unset.
    fn parse(env: Option<&str>, args: &[&str]) -> Cli {
        temp_env::with_var("SCHEMA_REGISTRY", env, || {
            Cli::try_parse_from(std::iter::once("nisshi").chain(args.iter().copied()))
                .unwrap_or_else(|e| panic!("{args:?} with SCHEMA_REGISTRY={env:?}: {e}"))
        })
    }

    /// Asserts that `nisshi <args>` parses, with `SCHEMA_REGISTRY` set to `env`, or unset.
    fn parses(env: Option<&str>, args: &[&str]) {
        let _ = parse(env, args);
    }

    /// Returns the schema registry that `nisshi cat <subcommand> t <args>` hands to `cat`.
    fn cat_schema_registry(subcommand: &str, env: Option<&str>, args: &[&str]) -> Option<Url> {
        let args = [&["cat", subcommand, "t"], args].concat();
        match parse(env, &args).command {
            Some(Command::Cat {
                command:
                    cat::Command::Produce {
                        schema_registry, ..
                    }
                    | cat::Command::Consume {
                        schema_registry, ..
                    },
            }) => schema_registry.map(EnvVarExp::into_inner),
            other => panic!("{args:?}: expected a cat produce or consume, got {other:?}"),
        }
    }

    fn registry(url: &str) -> Option<Url> {
        Some(Url::parse(url).expect(url))
    }

    /// The broker's `--schema-registry` is `global`, so clap merges the matches of a tool that
    /// declares its own into the flattened broker arguments. Parsing only `cat`'s arguments
    /// hides that: it has to go through `Cli`.
    #[test]
    fn cat_gets_its_schema_registry_from_flag_and_environment() {
        for subcommand in ["produce", "consume"] {
            let flag = ["--schema-registry", OTHER_REGISTRY];
            assert_eq!(cat_schema_registry(subcommand, None, &[]), None);
            assert_eq!(
                cat_schema_registry(subcommand, Some(REGISTRY), &[]),
                registry(REGISTRY),
            );
            assert_eq!(
                cat_schema_registry(subcommand, None, &flag),
                registry(OTHER_REGISTRY),
            );
            // The flag wins over the environment variable.
            assert_eq!(
                cat_schema_registry(subcommand, Some(REGISTRY), &flag),
                registry(OTHER_REGISTRY),
            );
        }
    }

    #[test]
    fn cat_expands_a_variable_in_its_schema_registry() {
        temp_env::with_var("REGISTRY_FROM_VARIABLE", Some(REGISTRY), || {
            for subcommand in ["produce", "consume"] {
                assert_eq!(
                    cat_schema_registry(
                        subcommand,
                        None,
                        &["--schema-registry", "${REGISTRY_FROM_VARIABLE}"],
                    ),
                    registry(REGISTRY),
                );
            }
        });
    }

    /// An argument that clap merges into a `global` one must have the global's type, or reading
    /// the merged value panics at run time. Walks every command, so a tool added later is covered.
    #[test]
    fn an_argument_with_a_global_arguments_name_has_its_type() {
        fn check(command: &clap::Command, globals: &[&clap::Arg], path: &str) {
            for arg in command.get_arguments() {
                if let Some(global) = globals
                    .iter()
                    .find(|global| global.get_id() == arg.get_id())
                {
                    assert_eq!(
                        arg.get_value_parser().type_id(),
                        global.get_value_parser().type_id(),
                        "`{path}` declares `--{}` with another type than the global argument",
                        arg.get_id(),
                    );
                }
            }
            let mut inherited = globals.to_vec();
            inherited.extend(command.get_arguments().filter(|arg| arg.is_global_set()));
            for subcommand in command.get_subcommands() {
                check(
                    subcommand,
                    &inherited,
                    &format!("{path} {}", subcommand.get_name()),
                );
            }
        }

        let cli = Cli::command();
        check(&cli, &[], cli.get_name());
    }

    #[test]
    fn generator_parses_schema_registry_from_flag_and_environment() {
        parses(None, &["generator", "t", "--schema-registry", REGISTRY]);
        parses(Some(REGISTRY), &["generator", "t"]);
    }

    #[test]
    fn broker_parses_schema_registry_before_and_after_a_subcommand() {
        parses(None, &["--schema-registry", REGISTRY]);
        parses(None, &["broker", "--schema-registry", REGISTRY]);
        parses(Some(REGISTRY), &["broker"]);
    }
}
