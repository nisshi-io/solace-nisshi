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

//! Runs the Apache Kafka command-line tools against a broker.
//!
//! [`KafkaCli`] runs each tool that [`Tool`] lists. Each tool has a module with a type named after
//! the tool, such as [`KafkaTopics`], which a method of [`KafkaCli`] returns, such as
//! [`KafkaCli::topics`]. The type's methods build the tool's command-line arguments, so a test
//! names an action, such as creating a topic, and does not list flags. Methods on [`Output`] read
//! what the tool printed. A method returns an [`Output`] typed by its command, so a test can call
//! only the readers for that command's output.
//!
//! Each method's doc names the Kafka APIs that the command is for. A command sends other requests
//! too, such as `ApiVersions` and `Metadata` first, and which ones it sends can change between
//! Kafka versions.
//!
//! The tools in `/opt/kafka/bin` run inside a container started from a Kafka image. The container
//! uses the host network, so the tools reach the broker at the address the broker advertises. Each
//! call returns the tool's exit code and output.
//!
//! Every time limit in the harness is a watchdog for a test that has already failed. A passing test
//! must stop when it reaches the result it expects, such as a record count, and must not wait for a
//! limit to expire. Each limit can then be well above the slowest passing run.
//!
//! Each call has two time limits. GNU `timeout` stops the tool inside the container, because
//! killing `docker exec` would leave the tool running there. [`timed_command::run`] kills a `docker
//! exec` that still hasn't returned [`GRACE`] later.

use std::{
    net::{TcpStream, ToSocketAddrs},
    process::Command,
    thread,
    time::{Duration, Instant},
};

use crate::{
    TlsCertificate,
    common::{docker, timed_command, unique},
    settings,
};

mod acls;
mod background_tool;
mod broker_api_versions;
mod cluster;
mod configs;
mod console_consumer;
mod console_producer;
mod consumer_groups;
mod delete_records;
mod get_offsets;
mod output;
mod producer_perf_test;
mod reassign_partitions;
mod record;
mod scram_login;
mod tool;
mod topics;
mod verifiable_consumer;
mod verifiable_producer;

/// The name the tools print for the broker's `TOPIC_ALREADY_EXISTS` error.
pub const TOPIC_EXISTS_EXCEPTION: &str = "TopicExistsException";
/// The name the tools print for the broker's `SECURITY_DISABLED` error.
pub const SECURITY_DISABLED_EXCEPTION: &str = "SecurityDisabledException";
/// The name the tools print for the broker's `MESSAGE_TOO_LARGE` error.
pub const RECORD_TOO_LARGE_EXCEPTION: &str = "RecordTooLargeException";
/// The name the tools print when the broker doesn't list a version of an API that a tool needs.
pub const UNSUPPORTED_VERSION_EXCEPTION: &str = "UnsupportedVersionException";
/// The name the tools print when the broker refuses a login.
pub const SASL_AUTHENTICATION_EXCEPTION: &str = "SaslAuthenticationException";
/// What the tools print when the broker answers a SaslHandshake with
/// `UNSUPPORTED_SASL_MECHANISM`, ahead of the broker's list of enabled mechanisms.
pub const SASL_MECHANISM_NOT_ENABLED: &str = "not enabled in the server, enabled mechanisms are";
/// The name the tools print for the broker's `NON_EMPTY_GROUP` error.
pub const GROUP_NOT_EMPTY_EXCEPTION: &str = "GroupNotEmptyException";
/// The name the tools print for the broker's `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` error.
pub const TRANSACTIONAL_ID_AUTHORIZATION_EXCEPTION: &str = "TransactionalIdAuthorizationException";

pub use acls::KafkaAcls;
pub use background_tool::{BackgroundTool, PrintedLine, StdoutLines};
pub use broker_api_versions::{ApiVersions, KafkaBrokerApiVersions};
pub use cluster::{ClusterId, KafkaCluster};
pub use configs::{DescribeConfigs, DescribeUsers, KafkaConfigs};
pub use console_consumer::{
    Consume, ConsumeMatching, ConsumedRecord, KafkaConsoleConsumer, LOG_APPEND_TIME, PRINTED_NULL,
    PrintsPartitionLines,
};
pub use console_producer::KafkaConsoleProducer;
pub use consumer_groups::{
    DescribeGroup, DescribeGroupMembers, DescribeGroupState, DescribedMember, GroupOffsets,
    GroupState, KafkaConsumerGroups, ListGroups, ListGroupsInState,
};
pub use delete_records::{DeleteRecords, KafkaDeleteRecords};
pub use get_offsets::{GetOffsets, KafkaGetOffsets};
pub use output::{AnyCommand, Output};
pub use producer_perf_test::KafkaProducerPerfTest;
pub use reassign_partitions::{KafkaReassignPartitions, NO_REASSIGNMENTS};
pub use record::Record;
pub use scram_login::{ScramLogin, ScramMechanism};
use tool::Tool;
pub use topics::{CreateTopic, DescribeTopic, KafkaTopics, ListTopics, PartitionReplicas};
pub use verifiable_consumer::{
    CommittedOffset, ConsumedOffsets, ConsumerEvent, KafkaVerifiableConsumer, PartitionAssignor,
    TopicPartition, VerifiableConsume,
};
pub use verifiable_producer::{
    Acks, KafkaVerifiableProducer, SendResult, StoredRecord, VerifiableProduce,
};

/// How long a tool a test runs has to finish.
const TOOL_TIMEOUT: Duration = Duration::from_secs(60);

/// How much longer than a tool's own timeout the harness waits for
/// `docker exec` to return.
const GRACE: Duration = Duration::from_secs(15);
/// How long `timeout` waits after SIGTERM before it sends SIGKILL.
const KILL_AFTER: Duration = Duration::from_secs(5);

// The harness must wait longer than `timeout` takes to send SIGKILL, or it
// kills `docker exec` before `timeout` can report why the tool stopped.
const _: () = assert!(GRACE.as_secs() > KILL_AFTER.as_secs());

/// The Kafka CLI tools, run with `docker exec` in a container on the host
/// network, so they reach a broker at the address it advertises.
#[derive(Debug)]
pub struct KafkaCli {
    container: String,
    bootstrap: String,
    /// The client properties that every tool gets, such as the ones that log in. It is empty for
    /// a broker that doesn't require a login.
    client_properties: String,
    /// The path in the container of the file that holds `client_properties`, or `None` when
    /// `client_properties` is empty.
    client_config: Option<String>,
    /// Whether every tool logs in with SASL. [`KafkaCli::over_tls`] reads it to choose `SASL_SSL`
    /// or `SSL`. It must be true exactly when `client_properties` sets a `SASL_*` security
    /// protocol.
    logs_in_with_sasl: bool,
}

impl KafkaCli {
    /// Tools for the broker at `bootstrap`, in the `NISSHI_SMOKE_KAFKA`
    /// container that `just smoke` starts for the run.
    pub fn new(bootstrap: &str) -> Self {
        Self {
            container: settings::kafka_container(),
            bootstrap: bootstrap.to_owned(),
            client_properties: String::new(),
            client_config: None,
            logs_in_with_sasl: false,
        }
    }

    /// Tools for the broker that all tests share, at [`settings::shared_broker_bootstrap`].
    pub fn shared() -> Self {
        Self::new(&settings::shared_broker_bootstrap())
    }

    /// Like [`KafkaCli::new`], but every tool logs in as `login`.
    ///
    /// Each tool sends `SaslHandshake` and `SaslAuthenticate` before its other requests.
    #[track_caller]
    pub fn logged_in_as(bootstrap: &str, login: &ScramLogin) -> Self {
        Self::marked_as_logging_in_with_sasl(bootstrap).with_client_properties(&format!(
            "security.protocol=SASL_PLAINTEXT\n\
             sasl.mechanism={}\n\
             sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
             username=\"{}\" password=\"{}\";\n",
            login.mechanism.name(),
            login.user,
            login.password
        ))
    }

    /// Like [`KafkaCli::logged_in_as`], but every tool logs in with SASL PLAIN as `user`.
    #[track_caller]
    pub fn logged_in_with_plain(bootstrap: &str, user: &str, password: &str) -> Self {
        Self::marked_as_logging_in_with_sasl(bootstrap).with_client_properties(&format!(
            "security.protocol=SASL_PLAINTEXT\n\
             sasl.mechanism=PLAIN\n\
             sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
             username=\"{user}\" password=\"{password}\";\n"
        ))
    }

    /// Like [`KafkaCli::new`], but marked as logging in with SASL. The caller adds the login's
    /// client properties.
    fn marked_as_logging_in_with_sasl(bootstrap: &str) -> Self {
        Self {
            logs_in_with_sasl: true,
            ..Self::new(bootstrap)
        }
    }

    /// Returns a copy of this `KafkaCli` whose tools connect to the broker with TLS and trust
    /// `certificate` as the broker's certificate. A tool that logs in sends its login over TLS.
    #[track_caller]
    pub fn over_tls(&self, certificate: &TlsCertificate) -> Self {
        let truststore =
            self.write_file_in_container("tls-certificate", "pem", &certificate.certificate_pem);

        // The tools load the client config file with `java.util.Properties`, which keeps the last
        // value of a repeated key. This `security.protocol` therefore replaces the
        // `security.protocol=SASL_PLAINTEXT` that a login sets.
        let security_protocol = if self.logs_in_with_sasl {
            "SASL_SSL"
        } else {
            "SSL"
        };

        self.with_client_properties(&format!(
            "security.protocol={security_protocol}\n\
             ssl.truststore.type=PEM\n\
             ssl.truststore.location={truststore}\n"
        ))
    }

    /// Returns `kafka-acls`, run against this broker.
    pub fn acls(&self) -> KafkaAcls<'_> {
        KafkaAcls::new(self)
    }

    /// Returns `kafka-broker-api-versions`, run against this broker.
    pub fn broker_api_versions(&self) -> KafkaBrokerApiVersions<'_> {
        KafkaBrokerApiVersions::new(self)
    }

    /// Returns `kafka-cluster`, run against this broker.
    pub fn cluster(&self) -> KafkaCluster<'_> {
        KafkaCluster::new(self)
    }

    /// Returns `kafka-configs`, run against this broker.
    pub fn configs(&self) -> KafkaConfigs<'_> {
        KafkaConfigs::new(self)
    }

    /// Returns `kafka-console-consumer`, run against this broker.
    pub fn console_consumer(&self) -> KafkaConsoleConsumer<'_> {
        KafkaConsoleConsumer::new(self)
    }

    /// Returns `kafka-console-producer`, run against this broker.
    pub fn console_producer(&self) -> KafkaConsoleProducer<'_> {
        KafkaConsoleProducer::new(self)
    }

    /// Returns `kafka-consumer-groups`, run against this broker.
    pub fn consumer_groups(&self) -> KafkaConsumerGroups<'_> {
        KafkaConsumerGroups::new(self)
    }

    /// Returns `kafka-delete-records`, run against this broker.
    pub fn delete_records(&self) -> KafkaDeleteRecords<'_> {
        KafkaDeleteRecords::new(self)
    }

    /// Returns `kafka-get-offsets`, run against this broker.
    pub fn get_offsets(&self) -> KafkaGetOffsets<'_> {
        KafkaGetOffsets::new(self)
    }

    /// Returns `kafka-producer-perf-test`, run against this broker.
    pub fn producer_perf_test(&self) -> KafkaProducerPerfTest<'_> {
        KafkaProducerPerfTest::new(self)
    }

    /// Returns `kafka-reassign-partitions`, run against this broker.
    pub fn reassign_partitions(&self) -> KafkaReassignPartitions<'_> {
        KafkaReassignPartitions::new(self)
    }

    /// Returns `kafka-topics`, run against this broker.
    pub fn topics(&self) -> KafkaTopics<'_> {
        KafkaTopics::new(self)
    }

    /// Returns `kafka-verifiable-consumer`, run against this broker.
    pub fn verifiable_consumer(&self) -> KafkaVerifiableConsumer<'_> {
        KafkaVerifiableConsumer::new(self)
    }

    /// Returns `kafka-verifiable-producer`, run against this broker.
    pub fn verifiable_producer(&self) -> KafkaVerifiableProducer<'_> {
        KafkaVerifiableProducer::new(self)
    }

    /// Returns a copy of this `KafkaCli` with its API timeout set to `timeout`, instead of the Java
    /// client's default of 60 seconds. The API timeout limits how long a tool waits for the broker
    /// to answer a call.
    #[track_caller]
    pub fn with_api_timeout(&self, timeout: Duration) -> Self {
        let timeout_millis = timeout.as_millis();

        // The admin client raises `default.api.timeout.ms` to `request.timeout.ms`, 30 seconds by
        // default, when it is lower, so the tools set both.
        self.with_client_properties(&format!(
            "default.api.timeout.ms={timeout_millis}\nrequest.timeout.ms={timeout_millis}\n"
        ))
    }

    /// Returns a copy of this `KafkaCli` with its client timeouts set to `timeout`. A client
    /// timeout limits how long a tool's Kafka client waits for the broker.
    #[track_caller]
    pub fn with_client_timeouts(&self, timeout: Duration) -> Self {
        let timeout_millis = timeout.as_millis();

        // A producer rejects a `delivery.timeout.ms` below `request.timeout.ms` plus `linger.ms`.
        let delivery_timeout_millis = 2 * timeout_millis;

        // Each tool warns about the other tools' client properties, and ignores them.
        self.with_api_timeout(timeout)
            .with_client_properties(&format!(
                "delivery.timeout.ms={delivery_timeout_millis}\nmax.block.ms={timeout_millis}\n"
            ))
    }

    /// Returns tools for the same broker whose client config file holds these tools' client
    /// properties, followed by `properties`.
    #[track_caller]
    fn with_client_properties(&self, properties: &str) -> Self {
        let client_properties = format!("{}{properties}", self.client_properties);
        let path = self.write_file_in_container("client", "properties", &client_properties);

        Self {
            container: self.container.clone(),
            bootstrap: self.bootstrap.clone(),
            client_properties,
            client_config: Some(path),
            logs_in_with_sasl: self.logs_in_with_sasl,
        }
    }

    /// Writes `contents` to a new file in the tools' container, and returns the file's path there.
    /// The file is named `<name_prefix>-<unique part>.<extension>`.
    #[track_caller]
    fn write_file_in_container(
        &self,
        name_prefix: &str,
        extension: &str,
        contents: &str,
    ) -> String {
        let path = format!("/tmp/{}.{extension}", unique::name(name_prefix));

        let written = timed_command::run(
            Command::new("docker").args([
                "exec",
                "--interactive",
                &self.container,
                "sh",
                "-c",
                &format!("cat > {path}"),
            ]),
            Some(contents),
            TOOL_TIMEOUT,
        );

        assert!(
            matches!(&written, Ok(written) if written.code == Some(0)),
            "could not write {path} in {}: {written:?}",
            self.container
        );

        path
    }

    /// Runs `/opt/kafka/bin/<tool>.sh <args>`, followed by the broker's address in the arguments
    /// that [`Tool::bootstrap_arguments`] returns. The address follows `<args>`, so that a
    /// subcommand such as `kafka-cluster.sh cluster-id` comes before the address and reads it as
    /// the subcommand's own option.
    ///
    /// Panics if the tool does not finish within `TOOL_TIMEOUT`.
    #[track_caller]
    fn run<PrintedBy>(&self, tool: Tool, args: &[&str]) -> Output<PrintedBy> {
        self.exec(tool, args, None, TOOL_TIMEOUT)
    }

    /// Like [`KafkaCli::run`], with `input` on the tool's stdin.
    #[track_caller]
    fn run_with_input<PrintedBy>(
        &self,
        tool: Tool,
        args: &[&str],
        input: &str,
    ) -> Output<PrintedBy> {
        self.exec(tool, args, Some(input), TOOL_TIMEOUT)
    }

    /// Waits until the broker answers [`KafkaCluster::id`], and returns that answer. Returns an
    /// error soon after `check_running` reports that the broker has exited, and also if the broker
    /// does not answer within `timeout`.
    ///
    /// The harness waits in Rust until the broker's port accepts connections, and only then starts
    /// the tool. Each tool starts a JVM, which takes seconds, and a tool that can't connect keeps
    /// trying until its time limit.
    pub fn wait_until_ready(
        &self,
        timeout: Duration,
        mut check_running: impl FnMut() -> Result<(), String>,
    ) -> Result<Output<ClusterId>, String> {
        /// How long one readiness probe has to finish.
        const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
        /// How often the harness checks whether the broker has exited, and whether its port
        /// accepts connections, before it starts a tool.
        const PORT_POLL_INTERVAL: Duration = Duration::from_millis(100);

        let deadline = Instant::now() + timeout;

        loop {
            check_running()?;

            if !self.port_accepts_connections(PORT_POLL_INTERVAL) {
                if Instant::now() >= deadline {
                    return Err(format!(
                        "{} did not accept connections within {timeout:?}",
                        self.bootstrap
                    ));
                }

                thread::sleep(PORT_POLL_INTERVAL);
                continue;
            }

            // Each attempt gets at most the time left before the deadline, and at
            // least a second, because `timeout 0s` means no time limit.
            let left = deadline.saturating_duration_since(Instant::now());
            let limit = ATTEMPT_TIMEOUT.min(left).max(Duration::from_secs(1));

            let args = ["cluster-id"];
            let attempt = timed_command::run(
                &mut self.command(Tool::Cluster, &args, false, None, limit),
                None,
                limit + GRACE,
            );

            match attempt {
                Ok(finished) if finished.code == Some(0) => {
                    return Ok(Output::of_tool(Tool::Cluster, &args, finished, None));
                }

                attempt if Instant::now() >= deadline => {
                    return Err(format!("did not answer within {timeout:?}: {attempt:?}"));
                }

                _ => thread::sleep(Duration::from_millis(500)),
            }
        }
    }

    fn port_accepts_connections(&self, timeout: Duration) -> bool {
        self.bootstrap
            .to_socket_addrs()
            .into_iter()
            .flatten()
            .any(|address| TcpStream::connect_timeout(&address, timeout).is_ok())
    }

    /// Runs `tool` as [`KafkaCli::run`] does, with `tool_timeout`. Panics if the tool doesn't
    /// finish in time.
    #[track_caller]
    fn exec<PrintedBy>(
        &self,
        tool: Tool,
        args: &[&str],
        input: Option<&str>,
        tool_timeout: Duration,
    ) -> Output<PrintedBy> {
        let output = self.exec_until_timeout(tool, args, input, tool_timeout);

        if let Some(reason) = &output.timeout {
            panic!(
                "`{}` {reason}\nstdout:\n{}\nstderr:\n{}",
                output.command, output.stdout, output.stderr
            );
        }

        output
    }

    /// Returns the time now on the clock of the tools' container, in milliseconds since the Unix
    /// epoch. The tools stamp the lines they print with this clock.
    ///
    /// A test compares a printed timestamp with this time, instead of with this machine's clock.
    /// This machine's clock can differ from the container's, such as when Docker runs in a virtual
    /// machine.
    #[track_caller]
    pub fn now_in_millis(&self) -> i64 {
        let printed = match docker::execute(&["exec", &self.container, "date", "+%s%3N"]) {
            Ok(printed) => printed,
            Err(err) => panic!("{err}"),
        };

        match printed.stdout.trim().parse() {
            Ok(now_in_millis) => now_in_millis,
            Err(err) => panic!("can't read the time in {printed:?}: {err}"),
        }
    }

    /// Runs `tool` as [`KafkaCli::exec`] does, but returns the output of a tool that doesn't
    /// finish in time instead of panicking, with [`Output::timeout`] set.
    #[track_caller]
    fn exec_until_timeout<PrintedBy>(
        &self,
        tool: Tool,
        args: &[&str],
        input: Option<&str>,
        tool_timeout: Duration,
    ) -> Output<PrintedBy> {
        /// The exit status of GNU `timeout` when it stops the tool with SIGTERM.
        const TIMEOUT_TERM_EXIT: i32 = 124;
        /// The exit status of GNU `timeout` when it sends SIGKILL, because the tool was still
        /// running `KILL_AFTER` after SIGTERM. `timeout` reports a signal as a shell does, as 128
        /// plus the signal number, and SIGKILL is 9.
        const TIMEOUT_KILL_EXIT: i32 = 128 + 9;

        let mut command = self.command(tool, args, input.is_some(), None, tool_timeout);

        let finished = match timed_command::run(&mut command, input, tool_timeout + GRACE) {
            Ok(finished) => finished,
            Err(err) => panic!("{err}"),
        };

        let timeout = match finished.code {
            Some(TIMEOUT_TERM_EXIT) => Some(format!("did not finish within {tool_timeout:?}")),
            Some(TIMEOUT_KILL_EXIT) => Some(format!(
                "did not finish within {tool_timeout:?}, nor within {KILL_AFTER:?} of SIGTERM"
            )),
            _ => None,
        };

        Output::of_tool(tool, args, finished, timeout)
    }

    /// Builds the `docker exec` command that runs `tool`. The command puts `jvm_tag` on the tool's
    /// JVM command line, so that [`BackgroundTool`] signals only this tool.
    fn command(
        &self,
        tool: Tool,
        args: &[&str],
        interactive: bool,
        jvm_tag: Option<&str>,
        timeout: Duration,
    ) -> Command {
        let mut command = Command::new("docker");
        _ = command.arg("exec");

        if interactive {
            _ = command.arg("--interactive");
        }

        _ = command.arg(&self.container);

        // The Kafka scripts pass `KAFKA_OPTS` to `java`. The shell appends the tag to the
        // container's `KAFKA_OPTS`, which `docker exec --env` would replace. The shell's own
        // arguments also contain the tag, so it starts the tool with `exec`, which ends the shell,
        // and `pkill` then matches only the tool's JVM.
        if let Some(jvm_tag) = jvm_tag {
            _ = command.args([
                "sh",
                "-c",
                r#"export KAFKA_OPTS="${KAFKA_OPTS:+$KAFKA_OPTS }$0"; exec "$@""#,
                &background_tool::jvm_tag_option(jvm_tag),
            ]);
        }

        // The tool runs under `timeout` in the container, because killing `docker exec` leaves
        // the tool running there, still a member of its group.
        //
        // The image needs GNU `timeout`, which exits with 124 or 137 when it stops the tool, as
        // `exec` expects. BusyBox's `timeout` exits with the tool's own status, so a timeout would
        // look like an ordinary failure.
        _ = command
            .arg("timeout")
            .arg(format!("--kill-after={}s", KILL_AFTER.as_secs()))
            .arg(format!("{}s", timeout.as_secs()))
            .arg(format!("/opt/kafka/bin/{}.sh", tool.name()))
            .args(args)
            .args(tool.bootstrap_arguments(&self.bootstrap));

        if let Some(client_config) = &self.client_config {
            _ = command.args([tool.client_config_option(), client_config]);
        }

        command
    }
}
