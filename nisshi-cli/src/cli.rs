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

//! Nisshi CLI
//!
//! The CLI is a single statically linked binary that contains:
//! - Broker
//! - Cat: produce, validate (if backed by a schema) and fetch messages
//! - Generator: use fake data generators to produce messages with a rate limit
//! - Proxy: a Kafka API proxy
//! - Topic: Topic administration

use std::{
    env,
    ffi::{OsStr, OsString},
    process,
};

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
        "With no command, nisshi runs the broker. `nisshi broker --help` lists its options."
            .to_owned(),
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
    after_help = after_help()
)]
pub struct Cli {
    // The broker's options are not flattened into `Cli`: clap reads the environment variable of
    // every flattened option, so a bad `STORAGE_ENGINE` would fail `nisshi topic list`, which
    // never uses it. `with_default_command` makes `broker` the command instead.
    #[command(subcommand)]
    command: Command,
}

/// Returns `args` (the command line, program name first) with `broker` inserted as the command
/// when the user gave none, so that `nisshi`, `nisshi --listener-url ...` and `nisshi iceberg ...`
/// still run the broker. The rule is as narrow as it can be: an unknown word such as
/// `nisshi foo` is left alone, so clap reports it as an unrecognized subcommand, and so are
/// `--help`, `-h`, `--version` and `-V`, which describe `nisshi` and not the broker.
fn with_default_command(mut args: Vec<OsString>) -> Vec<OsString> {
    let default_to_broker = match args.get(1).map(OsString::as_os_str) {
        None => true,
        Some(first) if first == OsStr::new("-h") => false,
        Some(first) if first == OsStr::new("--help") => false,
        Some(first) if first == OsStr::new("-V") => false,
        Some(first) if first == OsStr::new("--version") => false,
        Some(first) => {
            first.to_string_lossy().starts_with('-') || missing_feature::is_gated_subcommand(first)
        }
    };

    if default_to_broker {
        args.insert(1.min(args.len()), OsString::from("broker"));
    }

    args
}

/// The `--help` note of each tool that connects to a broker as a Kafka client.
const CLIENT_TOOL_NOTE: &str = "Connects in plaintext and does not log in, unless given the --tls options (TLS) or --sasl options (SASL PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512). Without them it can't reach a broker listener that requires TLS or SASL. The SASL password comes from the SASL_PASSWORD environment variable or --sasl-password-file, not from an option.";

/// The exit statuses of the client tools, as [`crate::ExitStatus`] numbers them. Part of every client
/// tool's `--help`, and of docs/commands.md.
const EXIT_STATUS_NOTE: &str = "Exit status: 0 success, 1 other failure (including a connection that the broker closed and a request that got no answer in time: the broker may have applied the request, so check before you retry), 2 usage error (a wrong command line or an unusable TLS or SASL option), 3 broker unreachable (no connection opened), 4 broker refused (an error code in its answer, which --output json prints, a refused login, or a rejected TLS connection). generator and perf keep running when the broker refuses a produce request, so that does not end them with 4, but a refused login or TLS connection does.";

fn client_tool_note() -> String {
    format!("{CLIENT_TOOL_NOTE}\n\n{EXIT_STATUS_NOTE}")
}

/// The `--help` note of `nisshi proxy`.
const PROXY_NOTE: &str = "For development and demos, not production.";

#[derive(Clone, Debug, Subcommand)]
enum Command {
    /// Apache Kafka®-compatible broker with Avro, JSON, Protobuf schema validation [default if no command supplied]
    Broker(Box<broker::Arg>),

    /// Consume or produce Avro, JSON or Protobuf messages to a topic
    #[command(after_help = client_tool_note())]
    Cat {
        #[command(subcommand)]
        command: cat::Command,
    },

    /// Generate fake messages for schema-backed topics (development and demos)
    #[command(after_help = client_tool_note())]
    Generator(Box<generator::Arg>),

    /// Measure produce and consume throughput against a broker (development and demos)
    #[command(after_help = client_tool_note())]
    Perf(Box<perf::Arg>),

    /// Apache Kafka®-compatible proxy (development and demos)
    #[command(after_help = PROXY_NOTE)]
    Proxy(Box<proxy::Arg>),

    /// Create, list or delete topics managed by the broker
    #[command(after_help = client_tool_note())]
    Topic {
        #[command(subcommand)]
        command: topic::Command,
    },

    /// Create, list or delete users managed by the broker
    #[command(after_help = client_tool_note())]
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
        Self::from_command_line()
            .run()
            .await
            .map(|outcome| outcome.error_code)
    }

    /// Parses the command line, or explains why it can't be parsed and exits.
    pub fn from_command_line() -> Self {
        Cli::try_parse_from(with_default_command(env::args_os().collect())).unwrap_or_else(
            |error| {
                missing_feature::explain(
                    error,
                    &missing_feature::built_features(),
                    &mut Cli::command(),
                )
                .exit()
            },
        )
    }

    /// Whether the command is a client tool: one that connects to a broker, prints its result on
    /// stdout and ends with an [`crate::ExitStatus`]. Its log lines go to stderr, so a script that
    /// reads its output does not read them. The broker and the proxy are servers, and log to
    /// stdout. The match has no wildcard arm, so a new subcommand does not compile until its
    /// author has chosen.
    pub fn is_client_tool(&self) -> bool {
        match &self.command {
            Command::Broker(_) | Command::Proxy(_) => false,
            Command::Cat { .. }
            | Command::Generator(_)
            | Command::Perf(_)
            | Command::Topic { .. }
            | Command::User { .. } => true,
        }
    }

    pub async fn run(self) -> Result<Outcome> {
        debug!(
            pid = process::id(),
            storage = ?storage_engines(),
            lakes = ?lakes()
        );

        let cli = self;

        match cli.command {
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
            assert!(
                help.contains("Exit status: 0 success, 1 other failure"),
                "{tool}: {help}"
            );
        }

        let proxy = long_help("proxy");
        assert!(proxy.contains(PROXY_NOTE), "{proxy}");

        let broker = long_help("broker");
        assert!(!broker.contains(CLIENT_TOOL_NOTE), "{broker}");
        assert!(!broker.contains(PROXY_NOTE), "{broker}");
    }

    /// A client tool is the command whose `--help` carries the client tool note, and the one whose
    /// log lines go to stderr and whose failure has an exit status. The table must name every
    /// subcommand, so a new one fails this test until its author adds it.
    #[test]
    fn a_client_tool_is_the_command_with_the_client_tool_note() {
        let command_lines: &[(&str, &[&str])] = &[
            ("broker", &["broker"]),
            ("cat", &["cat", "consume", "taxi"]),
            ("generator", &["generator", "taxi"]),
            ("perf", &["perf", "taxi", "produce"]),
            ("proxy", &["proxy"]),
            ("topic", &["topic", "list"]),
            ("user", &["user", "delete", "alice"]),
        ];

        // Every subcommand whose `--help` carries the note is in the table.
        let mut noted = Vec::new();
        for subcommand in Cli::command().get_subcommands_mut() {
            if subcommand
                .render_long_help()
                .to_string()
                .contains(CLIENT_TOOL_NOTE)
            {
                noted.push(subcommand.get_name().to_owned());
            }
        }
        let tools: Vec<_> = command_lines
            .iter()
            .map(|(name, _)| *name)
            .filter(|name| noted.iter().any(|noted| noted == name))
            .collect();
        assert_eq!(noted, tools, "add each client tool to the table");

        for (name, args) in command_lines {
            let noted = long_help(name).contains(CLIENT_TOOL_NOTE);

            assert_eq!(
                noted,
                parse(Some(REGISTRY), args).is_client_tool(),
                "{name}"
            );
        }

        assert!(!parse(Some(REGISTRY), &[]).is_client_tool(), "no command");
        assert!(
            !parse(Some(REGISTRY), &["--listener-url", "tcp://[::]:1"]).is_client_tool(),
            "broker options"
        );
    }

    const REGISTRY: &str = "file://./etc/schema";
    const OTHER_REGISTRY: &str = "s3://nisshi/";

    /// Parses `nisshi <args>` as the binary does, with the environment `vars`.
    fn try_parse(vars: &[(&str, Option<&str>)], args: &[&str]) -> Result<Cli, clap::Error> {
        temp_env::with_vars(vars, || {
            Cli::try_parse_from(with_default_command(
                std::iter::once("nisshi")
                    .chain(args.iter().copied())
                    .map(OsString::from)
                    .collect(),
            ))
        })
    }

    /// Parses `nisshi <args>` with `SCHEMA_REGISTRY` set to `env`, or unset.
    fn parse(env: Option<&str>, args: &[&str]) -> Cli {
        try_parse(&[("SCHEMA_REGISTRY", env)], args)
            .unwrap_or_else(|e| panic!("{args:?} with SCHEMA_REGISTRY={env:?}: {e}"))
    }

    /// Asserts that `nisshi <args>` parses, with `SCHEMA_REGISTRY` set to `env`, or unset.
    fn parses(env: Option<&str>, args: &[&str]) {
        let _ = parse(env, args);
    }

    /// Returns the schema registry that `nisshi cat <subcommand> t <args>` hands to `cat`.
    fn cat_schema_registry(subcommand: &str, env: Option<&str>, args: &[&str]) -> Option<Url> {
        let args = [&["cat", subcommand, "t"], args].concat();
        match parse(env, &args).command {
            Command::Cat {
                command:
                    cat::Command::Produce {
                        schema_registry, ..
                    }
                    | cat::Command::Consume {
                        schema_registry, ..
                    },
            } => schema_registry.map(EnvVarExp::into_inner),
            other => panic!("{args:?}: expected a cat produce or consume, got {other:?}"),
        }
    }

    fn registry(url: &str) -> Option<Url> {
        Some(Url::parse(url).expect(url))
    }

    /// Parses `cat` through `Cli`, as the binary does, with its own `--schema-registry`.
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

    fn words(args: &[&str]) -> Vec<OsString> {
        args.iter().map(OsString::from).collect()
    }

    #[test]
    fn a_command_line_without_a_command_runs_the_broker() {
        for (given, expected) in [
            (&["nisshi"][..], &["nisshi", "broker"][..]),
            (
                &["nisshi", "--listener-url", "tcp://[::]:1"],
                &["nisshi", "broker", "--listener-url", "tcp://[::]:1"],
            ),
            (
                &["nisshi", "--kafka-cluster-id=x"],
                &["nisshi", "broker", "--kafka-cluster-id=x"],
            ),
            // A lake subcommand is the broker's, whether this binary was built with it or not.
            (
                &["nisshi", "iceberg", "--help"],
                &["nisshi", "broker", "iceberg", "--help"],
            ),
            (&["nisshi", "parquet"], &["nisshi", "broker", "parquet"]),
            // A command, and what describes `nisshi` itself, are left as given.
            (&["nisshi", "broker"], &["nisshi", "broker"]),
            (&["nisshi", "topic", "list"], &["nisshi", "topic", "list"]),
            (&["nisshi", "help"], &["nisshi", "help"]),
            (&["nisshi", "--help"], &["nisshi", "--help"]),
            (&["nisshi", "-h"], &["nisshi", "-h"]),
            (&["nisshi", "--version"], &["nisshi", "--version"]),
            (&["nisshi", "-V"], &["nisshi", "-V"]),
            // An unknown word is clap's to report.
            (&["nisshi", "foo"], &["nisshi", "foo"]),
        ] {
            assert_eq!(
                with_default_command(words(given)),
                words(expected),
                "{given:?}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_command_line_that_is_not_utf8_does_not_panic() {
        use std::os::unix::ffi::OsStringExt as _;

        let odd = OsString::from_vec(vec![b'-', 0xff]);
        let args = with_default_command(vec![OsString::from("nisshi"), odd.clone()]);
        assert_eq!(
            args,
            vec![OsString::from("nisshi"), OsString::from("broker"), odd]
        );
    }

    /// Returns the environment variables that `command` and its subcommands read.
    fn environment(command: &clap::Command) -> Vec<String> {
        command
            .get_arguments()
            .filter_map(|arg| arg.get_env())
            .map(|name| name.to_string_lossy().into_owned())
            .chain(command.get_subcommands().flat_map(environment))
            .collect()
    }

    /// The command lines of every tool other than the broker.
    const TOOLS: &[&[&str]] = &[
        &["topic", "list"],
        &["user", "delete", "alice"],
        &["cat", "consume", "t"],
        &["cat", "produce", "t"],
        &["perf", "t", "produce"],
        &["generator", "t", "--schema-registry", REGISTRY],
        &["proxy"],
    ];

    /// A value that none of the broker's options accepts: not a URL, a number, a duration or a
    /// boolean.
    const BAD_VALUE: &str = "0 bad:";

    /// A broker option that is wrong in the environment must not fail a tool that does not read
    /// that variable. clap reads the variable of every option that it parses, so this checks that
    /// `Cli` parses none of the broker's options for a tool. It walks the broker's variables, so
    /// an option added later is covered.
    #[test]
    fn a_bad_broker_environment_variable_does_not_affect_a_tool() {
        let broker = environment(&broker::Arg::command());
        assert!(
            broker.iter().any(|name| name == "STORAGE_ENGINE"),
            "{broker:?}"
        );

        for tool in TOOLS {
            let own = environment(
                Cli::command()
                    .find_subcommand(tool[0])
                    .expect("the tool is a subcommand"),
            );

            for name in broker.iter().filter(|name| !own.contains(name)) {
                let result = try_parse(&[(name, Some(BAD_VALUE))], tool);
                assert!(
                    result.is_ok(),
                    "{tool:?} with {name}={BAD_VALUE}: {result:?}"
                );
            }
        }
    }

    /// The reverse: the broker, named or not, still reads its variables.
    #[test]
    fn a_bad_broker_environment_variable_fails_the_broker() {
        for args in [&["broker"][..], &[], &["--cluster-id", "x"]] {
            for name in ["STORAGE_ENGINE", "NUM_PARTITIONS", "GROUP_POLL_INTERVAL"] {
                let error = try_parse(&[(name, Some(BAD_VALUE))], args)
                    .expect_err(&format!("{args:?} with {name}"));
                assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
            }
        }
    }

    /// A tool still fails on a bad value of a variable that it reads.
    #[test]
    fn a_bad_tool_environment_variable_fails_the_tool() {
        assert!(
            try_parse(
                &[("ADVERTISED_LISTENER_URL", Some(BAD_VALUE))],
                &["cat", "consume", "t"]
            )
            .is_err()
        );
        assert!(try_parse(&[("ORIGIN_URL", Some(BAD_VALUE))], &["proxy"]).is_err());
    }

    #[test]
    fn a_command_line_with_an_unknown_command_is_rejected() {
        let error = try_parse(&[], &["foo"]).expect_err("foo is not a command");
        assert_eq!(error.kind(), clap::error::ErrorKind::InvalidSubcommand);
    }

    #[test]
    fn broker_options_parse_with_and_without_the_broker_command() {
        for args in [
            &["--listener-url", "tcp://127.0.0.1:1"][..],
            &["broker", "--listener-url", "tcp://127.0.0.1:1"],
            &[],
        ] {
            assert!(
                matches!(parse(None, args).command, Command::Broker(_)),
                "{args:?}"
            );
        }
    }

    /// A lake subcommand is the broker's, with or without the word `broker` before it, and takes
    /// the broker's `--schema-registry` on either side of its own name.
    #[cfg(feature = "parquet")]
    #[test]
    fn a_lake_subcommand_runs_in_the_broker() {
        for args in [
            &["parquet", "--location", "file:///x"][..],
            &["broker", "parquet", "--location", "file:///x"],
            &[
                "broker",
                "--schema-registry",
                REGISTRY,
                "parquet",
                "--location",
                "file:///x",
            ],
            &[
                "parquet",
                "--location",
                "file:///x",
                "--schema-registry",
                REGISTRY,
            ],
        ] {
            assert!(
                matches!(parse(None, args).command, Command::Broker(_)),
                "{args:?}"
            );
        }
    }
}
