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

//! `kafka-verifiable-producer`, which reports each record the broker acknowledges.

use std::time::Duration;

use serde::Deserialize;

use super::{BackgroundTool, KafkaCli, Output, TOOL_TIMEOUT, Tool};

/// The command of [`KafkaVerifiableProducer::produce`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum VerifiableProduce {}

/// A record's value, partition and offset, as the broker acknowledged it to a producer or as a
/// consumer read it. Records sort by partition, then offset.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StoredRecord {
    pub partition: u32,
    pub offset: i64,
    pub value: String,
}

/// What `kafka-verifiable-producer` reported for one record it sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SendResult {
    /// The broker acknowledged the record.
    Acknowledged(StoredRecord),
    /// The send failed, or the record timed out.
    Failed { value: String },
}

/// A JSON line that `kafka-verifiable-producer` prints. The `name` field selects the variant, and
/// `Other` matches the lines that the tests don't read.
#[derive(Debug, Deserialize)]
#[serde(tag = "name", rename_all = "snake_case")]
enum ProducerEvent {
    ProducerSendSuccess {
        timestamp: i64,
        value: String,
        partition: u32,
        offset: i64,
    },
    ProducerSendError {
        timestamp: i64,
        value: String,
    },
    /// The line the tool prints when it finishes, with how many records it sent.
    ToolData {
        sent: usize,
    },
    #[serde(other)]
    Other,
}

/// Returns the arguments of `kafka-verifiable-producer`. Without `max_messages`, the producer sends
/// until it is stopped.
fn verifiable_producer_args(
    topic: &str,
    max_messages: Option<usize>,
    acks: Acks,
    value_prefix: u32,
    more_args: &[&str],
) -> Vec<String> {
    // The tool reads -1 as no limit.
    let max_messages = max_messages.map_or_else(|| "-1".to_owned(), |max| max.to_string());

    [
        "--topic",
        topic,
        "--max-messages",
        &max_messages,
        "--acks",
        acks.setting(),
        "--value-prefix",
        &value_prefix.to_string(),
    ]
    .into_iter()
    .chain(more_args.iter().copied())
    .map(ToOwned::to_owned)
    .collect()
}

/// When a producer counts a record as sent. The names match the broker's `Ack`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Acks {
    /// When the record is written to the network, without a response from the broker.
    None,
    /// When the partition leader has stored the record.
    Leader,
    /// When every in-sync replica has stored the record.
    FullIsr,
}

impl Acks {
    /// The value of the producer's `acks` setting.
    fn setting(self) -> &'static str {
        match self {
            Self::None => "0",
            Self::Leader => "1",
            Self::FullIsr => "-1",
        }
    }
}

/// Runs `kafka-verifiable-producer` against the broker of a [`KafkaCli`].
#[derive(Clone, Copy, Debug)]
pub struct KafkaVerifiableProducer<'cli> {
    cli: &'cli KafkaCli,
}

impl<'cli> KafkaVerifiableProducer<'cli> {
    pub(super) fn new(cli: &'cli KafkaCli) -> Self {
        Self { cli }
    }
    /// Produces `max_messages` records to `topic` with `kafka-verifiable-producer`, which prints
    /// one JSON line per record it sends, with `"name":"producer_send_success"` for each record
    /// the broker acknowledges. Record `n`'s value is `<value_prefix>.<n>`.
    ///
    /// Sends `Produce`. The tool sets `retries=0`, and the Java client then turns idempotence off,
    /// whatever `acks` is. [`KafkaVerifiableProducer::produce_idempotent`] turns it on.
    #[track_caller]
    pub fn produce(
        &self,
        topic: &str,
        max_messages: usize,
        acks: Acks,
        value_prefix: u32,
    ) -> Output<VerifiableProduce> {
        self.run_verifiable_producer(topic, max_messages, acks, value_prefix, &[], TOOL_TIMEOUT)
    }

    /// Produces `max_messages` records to `topic` with [`Acks::FullIsr`], at most `throughput`
    /// records a second. Record `n`'s value is `<value_prefix>.<n>`. Panics if the producer
    /// doesn't finish within `tool_timeout`.
    #[track_caller]
    pub fn produce_throttled(
        &self,
        topic: &str,
        max_messages: usize,
        throughput: u32,
        value_prefix: u32,
        tool_timeout: Duration,
    ) -> Output<VerifiableProduce> {
        let throughput = throughput.to_string();

        self.run_verifiable_producer(
            topic,
            max_messages,
            Acks::FullIsr,
            value_prefix,
            &["--throughput", &throughput],
            tool_timeout,
        )
    }

    /// Starts a producer that sends `throughput` records a second to `topic` with
    /// [`Acks::FullIsr`], until the test stops it. Record `n`'s value is `<value_prefix>.<n>`.
    ///
    /// The producer repeats a fixed set of keys, so that the records spread over the topic's
    /// partitions. A producer without keys writes to one partition until it fills a batch, which
    /// takes minutes at a few records a second.
    #[track_caller]
    pub fn start(
        &self,
        topic: &str,
        throughput: u32,
        value_prefix: u32,
    ) -> BackgroundTool<VerifiableProduce> {
        let throughput = throughput.to_string();
        let args = verifiable_producer_args(
            topic,
            None,
            Acks::FullIsr,
            value_prefix,
            &["--throughput", &throughput, "--repeating-keys", "30"],
        );

        BackgroundTool::start(
            self.cli,
            Tool::VerifiableProducer,
            &args.iter().map(String::as_str).collect::<Vec<_>>(),
        )
    }

    /// Produces `max_messages` records timestamped about `create_time_millis`, so a test can
    /// choose how old a topic's retention finds them. The broker rejects a timestamp more than an
    /// hour ahead of its clock.
    ///
    /// Record `n`'s value is `<value_prefix>.<n>`. With `repeating_keys` of 2, the keys are `0`,
    /// `1`, `0`, `1`, and so on; without it, the records have no keys.
    #[track_caller]
    pub fn produce_created_at(
        &self,
        topic: &str,
        max_messages: usize,
        create_time_millis: i64,
        value_prefix: u32,
        repeating_keys: Option<usize>,
    ) -> Output<VerifiableProduce> {
        let create_time_millis = create_time_millis.to_string();
        let mut args = vec!["--message-create-time", &create_time_millis];

        let repeating_keys = repeating_keys.map(|keys| keys.to_string());
        if let Some(repeating_keys) = &repeating_keys {
            args.extend(["--repeating-keys", repeating_keys]);
        }

        self.run_verifiable_producer(
            topic,
            max_messages,
            Acks::FullIsr,
            value_prefix,
            &args,
            TOOL_TIMEOUT,
        )
    }

    /// Runs `kafka-verifiable-producer` until it has sent `max_messages` records. Panics if the
    /// producer doesn't finish within `tool_timeout`.
    #[track_caller]
    fn run_verifiable_producer(
        &self,
        topic: &str,
        max_messages: usize,
        acks: Acks,
        value_prefix: u32,
        more_args: &[&str],
        tool_timeout: Duration,
    ) -> Output<VerifiableProduce> {
        let args =
            verifiable_producer_args(topic, Some(max_messages), acks, value_prefix, more_args);

        self.cli.exec(
            Tool::VerifiableProducer,
            &args.iter().map(String::as_str).collect::<Vec<_>>(),
            None,
            tool_timeout,
        )
    }

    /// Like [`KafkaVerifiableProducer::produce`] with [`Acks::FullIsr`], but the producer is
    /// idempotent: it numbers its batches, so the broker can drop a batch that it gets twice.
    ///
    /// Sends `InitProducerId` and `Produce`.
    #[track_caller]
    pub fn produce_idempotent(
        &self,
        topic: &str,
        max_messages: usize,
        value_prefix: u32,
    ) -> Output<VerifiableProduce> {
        // The tool sets `retries=0`, and the Java client refuses `enable.idempotence=true` with
        // it, so the producer is idempotent or the tool fails. The tool reads this file after its
        // own settings, so the file's `retries` replaces that 0.
        self.cli
            .with_client_properties("enable.idempotence=true\nretries=2147483647\n")
            .verifiable_producer()
            .produce(topic, max_messages, Acks::FullIsr, value_prefix)
    }

    /// Returns the values of the first `count` records that [`KafkaVerifiableProducer::produce`]
    /// sends with `value_prefix`: `<value_prefix>.<n>`.
    pub fn values(value_prefix: u32, count: usize) -> Vec<String> {
        (0..count).map(|n| format!("{value_prefix}.{n}")).collect()
    }
}

impl Output<VerifiableProduce> {
    /// Returns how many records the producer says the broker acknowledged.
    #[track_caller]
    pub fn acknowledged_record_count(&self) -> usize {
        // The function parses each line in its body, not in a closure, so that a line that
        // `ProducerEvent::parse` doesn't know reports the test's line.
        let mut acknowledged_record_count = 0;

        for line in self.succeeded().lines() {
            if matches!(
                ProducerEvent::parse(line),
                Some(ProducerEvent::ProducerSendSuccess { .. })
            ) {
                acknowledged_record_count += 1;
            }
        }

        acknowledged_record_count
    }

    /// Returns each record that the producer says the broker acknowledged, in the order it printed
    /// them. Accepts the output of a producer that failed or that the harness stopped.
    #[track_caller]
    pub fn acknowledged_records(&self) -> Vec<StoredRecord> {
        self.send_results_with_report_times()
            .into_iter()
            .filter_map(|(_, result)| match result {
                SendResult::Acknowledged(record) => Some(record),
                SendResult::Failed { .. } => None,
            })
            .collect()
    }

    /// Returns what the producer reported for each record it sent, in the order it printed them.
    /// Each result comes with the time the producer reported it (ms since the Unix epoch).
    #[track_caller]
    pub fn send_results_with_report_times(&self) -> Vec<(i64, SendResult)> {
        self.json_events::<ProducerEvent>(Tool::VerifiableProducer)
            .filter_map(|event| match event {
                ProducerEvent::ProducerSendSuccess {
                    timestamp,
                    value,
                    partition,
                    offset,
                } => Some((
                    timestamp,
                    SendResult::Acknowledged(StoredRecord {
                        partition,
                        offset,
                        value,
                    }),
                )),
                ProducerEvent::ProducerSendError { timestamp, value } => {
                    Some((timestamp, SendResult::Failed { value }))
                }
                ProducerEvent::ToolData { .. } | ProducerEvent::Other => None,
            })
            .collect()
    }

    /// Returns how many records the producer sent, from the `tool_data` line that it prints when it
    /// exits. Returns `None` when that line is missing.
    #[track_caller]
    pub fn sent_count(&self) -> Option<usize> {
        self.json_events::<ProducerEvent>(Tool::VerifiableProducer)
            .find_map(|event| match event {
                ProducerEvent::ToolData { sent } => Some(sent),
                _ => None,
            })
    }
}

impl ProducerEvent {
    /// Reads an event from one of the producer's lines, as [`Tool::read_json_event`] does.
    #[track_caller]
    fn parse(line: &str) -> Option<Self> {
        Tool::VerifiableProducer.read_json_event(line)
    }
}

#[cfg(test)]
mod tests {
    use super::{Output, SendResult, StoredRecord, VerifiableProduce};

    /// What `kafka-verifiable-producer` 3.9 printed when the broker stopped during the run, plus a
    /// warning line of the kind that Kafka 4 tools print on stdout.
    const PRINTED: &str = r#"{"timestamp":1791572831101,"name":"startup_complete"}
WARN some warning
{"timestamp":1791572842010,"name":"producer_send_success","key":null,"value":"1091","partition":1,"topic":"t","offset":363}
{"timestamp":1791572842030,"name":"producer_send_error","topic":"t","key":null,"value":"1092","exception":"class org.apache.kafka.common.errors.NetworkException","message":"Disconnected from node 111"}
{"timestamp":1791572862097,"name":"shutdown_complete"}
{"timestamp":1791572862098,"name":"tool_data","sent":2,"acked":1,"target_throughput":100,"avg_throughput":35.2}
"#;

    #[test]
    fn send_results_are_read_in_order() {
        assert_eq!(
            Output::<VerifiableProduce>::printed(PRINTED)
                .send_results_with_report_times()
                .into_iter()
                .map(|(_, result)| result)
                .collect::<Vec<_>>(),
            [
                SendResult::Acknowledged(StoredRecord {
                    partition: 1,
                    offset: 363,
                    value: "1091".to_owned(),
                }),
                SendResult::Failed {
                    value: "1092".to_owned(),
                },
            ]
        );
    }

    #[test]
    fn acknowledged_records_leave_out_failed_sends() {
        assert_eq!(
            Output::<VerifiableProduce>::printed(PRINTED).acknowledged_records(),
            [StoredRecord {
                partition: 1,
                offset: 363,
                value: "1091".to_owned(),
            }]
        );
    }

    #[test]
    fn send_results_carry_the_time_the_producer_reported_them() {
        assert_eq!(
            Output::<VerifiableProduce>::printed(PRINTED)
                .send_results_with_report_times()
                .into_iter()
                .map(|(reported_at, _)| reported_at)
                .collect::<Vec<_>>(),
            [1791572842010, 1791572842030]
        );
    }

    #[test]
    fn sent_count_is_read_from_the_tool_data() {
        assert_eq!(
            Output::<VerifiableProduce>::printed(PRINTED).sent_count(),
            Some(2)
        );
    }

    #[test]
    fn sent_count_is_none_without_the_tool_data_line() {
        assert_eq!(
            Output::<VerifiableProduce>::printed(
                r#"{"timestamp":1791572831101,"name":"startup_complete"}"#
            )
            .sent_count(),
            None
        );
    }
}
