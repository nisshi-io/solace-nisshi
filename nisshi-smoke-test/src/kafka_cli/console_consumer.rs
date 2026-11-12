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

//! `kafka-console-consumer`, which prints each record it consumes.

use std::{collections::BTreeMap, time::Duration};

use super::{KafkaCli, Output, Tool};

/// How the console consumer starts each record's partition field, with `print.partition`.
const PARTITION_FIELD_PREFIX: &str = "Partition:";
/// How the console consumer starts each record's offset field, with `print.offset`.
const OFFSET_FIELD_PREFIX: &str = "Offset:";
/// The timestamp type the console consumer prints for a record whose timestamp the broker set.
pub const LOG_APPEND_TIME: &str = "LogAppendTime";
/// What the console consumer prints for a record's key or value when it is null, such as a
/// tombstone's value.
pub const PRINTED_NULL: &str = "null";

/// The command of [`KafkaConsoleConsumer::consume`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum Consume {}

/// The command of [`KafkaConsoleConsumer::consume_matching`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum ConsumeMatching {}

/// A console consumer command that prints each record on a line that starts with
/// `Partition:<n>\t` and ends with the record's value.
pub trait PrintsPartitionLines {}

impl PrintsPartitionLines for Consume {}
impl PrintsPartitionLines for ConsumeMatching {}

/// Runs `kafka-console-consumer` against the broker of a [`KafkaCli`].
#[derive(Clone, Copy, Debug)]
pub struct KafkaConsoleConsumer<'cli> {
    cli: &'cli KafkaCli,
}

impl<'cli> KafkaConsoleConsumer<'cli> {
    pub(super) fn new(cli: &'cli KafkaCli) -> Self {
        Self { cli }
    }
    /// Consumes `topic` from the beginning as a member of `group`, and stops after `max_messages`
    /// records or 30 seconds without one.
    ///
    /// Sends `FindCoordinator`, `JoinGroup`, `SyncGroup`, `Fetch`, `OffsetCommit` and `LeaveGroup`.
    ///
    /// The consumer prints each record as `Partition:<n>\tOffset:<n>\t` followed by
    /// [`Record::console_line`](crate::Record::console_line).
    #[track_caller]
    pub fn consume(&self, topic: &str, group: &str, max_messages: usize) -> Output<Consume> {
        let max_messages = max_messages.to_string();

        self.cli.run(
            Tool::ConsoleConsumer,
            &[
                "--timeout-ms",
                "30000",
                "--max-messages",
                &max_messages,
                "--consumer-property",
                "fetch.max.wait.ms=15000",
                "--consumer-property",
                "session.timeout.ms=6000",
                "--group",
                group,
                "--topic",
                topic,
                "--from-beginning",
                "--property",
                "print.key=true",
                "--property",
                "print.offset=true",
                "--property",
                "print.partition=true",
                "--property",
                "print.headers=true",
                "--property",
                "print.value=true",
            ],
        )
    }

    /// Reads `count` records from `topic`'s `partition`, starting at `offset`, without a group.
    /// The consumer stops after `count` records or 30 seconds without one.
    ///
    /// Sends `ListOffsets` and `Fetch`.
    ///
    /// The consumer prints each record as `<type>:<timestamp>\tOffset:<n>\t<key>\t<value>`,
    /// which [`ConsumedRecord::parse`](crate::ConsumedRecord::parse) reads.
    ///
    /// Panics if it fails or reads a number of records other than `count`.
    #[track_caller]
    pub fn read_records(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        count: usize,
    ) -> Vec<ConsumedRecord> {
        self.read_records_from(topic, partition, &offset.to_string(), count)
    }

    /// Like [`KafkaConsoleConsumer::read_records`], but starts at the partition's earliest offset,
    /// for a test that doesn't know which records retention or compaction left.
    #[track_caller]
    pub fn read_records_from_earliest(
        &self,
        topic: &str,
        partition: u32,
        count: usize,
    ) -> Vec<ConsumedRecord> {
        self.read_records_from(topic, partition, "earliest", count)
    }

    /// Reads `count` records as [`KafkaConsoleConsumer::read_records`] does, from `offset`, which
    /// is an offset or `earliest`.
    #[track_caller]
    fn read_records_from(
        &self,
        topic: &str,
        partition: u32,
        offset: &str,
        count: usize,
    ) -> Vec<ConsumedRecord> {
        let partition = partition.to_string();
        let max_messages = count.to_string();

        let read: Output = self.cli.run(
            Tool::ConsoleConsumer,
            &[
                "--timeout-ms",
                "30000",
                "--max-messages",
                &max_messages,
                "--topic",
                topic,
                "--partition",
                &partition,
                "--offset",
                offset,
                "--property",
                "print.timestamp=true",
                "--property",
                "print.offset=true",
                "--property",
                "print.key=true",
                "--property",
                "print.value=true",
            ],
        );

        let records = read
            .succeeded()
            .stdout
            .lines()
            .filter_map(ConsumedRecord::parse)
            .collect::<Vec<_>>();

        assert_eq!(records.len(), count, "{read}");
        records
    }

    /// Consumes every topic whose name matches `pattern` as a member of `group`, from the
    /// beginning, and stops after `max_messages` records, or after two minutes pass without a
    /// record. The consumer looks for new matching topics every second. It prints
    /// each record as `Partition:<n>\t<value>`, which [`Output::consumed_values`] reads.
    ///
    /// Returns the output even if the consumer doesn't finish in time, with [`Output::timeout`]
    /// set, because a test runs it on a thread while it waits for the consumer's group.
    ///
    /// Sends `Metadata`, `FindCoordinator`, `JoinGroup`, `SyncGroup`, `Fetch`, `OffsetCommit` and
    /// `LeaveGroup`.
    #[track_caller]
    pub fn consume_matching(
        &self,
        pattern: &str,
        group: &str,
        max_messages: usize,
    ) -> Output<ConsumeMatching> {
        /// How long the consumer waits for a record before it stops. The consumer runs while its
        /// test creates a topic and produces to it, and a passing test stops when the consumer
        /// reaches the record count.
        const IDLE_TIMEOUT: Duration = Duration::from_secs(120);
        /// How long the tool has to finish: its idle timeout, and time to read the first record
        /// and to leave its group.
        const TOOL_TIMEOUT: Duration = Duration::from_secs(150);

        let max_messages = max_messages.to_string();
        let idle_timeout = IDLE_TIMEOUT.as_millis().to_string();

        self.cli.exec_until_timeout(
            Tool::ConsoleConsumer,
            &[
                "--timeout-ms",
                &idle_timeout,
                "--max-messages",
                &max_messages,
                "--consumer-property",
                "metadata.max.age.ms=1000",
                "--consumer-property",
                "session.timeout.ms=6000",
                "--group",
                group,
                "--include",
                pattern,
                "--from-beginning",
                "--property",
                "print.partition=true",
                // The consumer puts a tab after the partition only when told to print the value,
                // though it prints the value either way.
                "--property",
                "print.value=true",
            ],
            None,
            TOOL_TIMEOUT,
        )
    }
}

impl<PrintedBy: PrintsPartitionLines> Output<PrintedBy> {
    /// Returns the record lines that the consumer printed, each as tab-separated fields starting
    /// with `Partition:` and ending with the value. Other lines, such as the warnings that Kafka 4
    /// tools print on stdout, are left out.
    pub fn consumed_lines(&self) -> Vec<&str> {
        self.lines()
            .into_iter()
            .filter(|line| line.starts_with(PARTITION_FIELD_PREFIX))
            .collect()
    }

    /// Returns the values of the records that the consumer printed.
    pub fn consumed_values(&self) -> Vec<&str> {
        self.consumed_lines()
            .into_iter()
            .filter_map(|line| line.rsplit('\t').next())
            .collect()
    }
}

impl Output<Consume> {
    /// Returns the offsets of the records, grouped by partition, in the order the consumer printed
    /// them. Panics if a record line doesn't start with `Partition:<n>\tOffset:<n>\t`, because
    /// then the tool prints a layout this reader doesn't know.
    #[track_caller]
    pub fn consumed_offsets_by_partition(&self) -> BTreeMap<u32, Vec<i64>> {
        let mut offsets = BTreeMap::<u32, Vec<i64>>::new();

        for line in self.consumed_lines() {
            let mut fields = line.split('\t');
            let partition = fields
                .next()
                .and_then(|field| field.strip_prefix(PARTITION_FIELD_PREFIX)?.parse().ok());
            let offset = fields
                .next()
                .and_then(|field| field.strip_prefix(OFFSET_FIELD_PREFIX)?.parse().ok());

            let (Some(partition), Some(offset)) = (partition, offset) else {
                panic!("can't read a partition and an offset from {line:?}: {self}");
            };

            offsets.entry(partition).or_default().push(offset);
        }

        offsets
    }
}

/// A record as [`KafkaConsoleConsumer::read_records`] reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumedRecord {
    /// `CreateTime` if the producer set the timestamp, `LogAppendTime` if the broker set it.
    pub timestamp_type: String,
    pub timestamp: i64,
    pub offset: i64,
    /// [`PRINTED_NULL`] for a record that does not have a key.
    pub key: String,
    /// [`PRINTED_NULL`] for a record that does not have a value, such as a tombstone.
    pub value: String,
}

impl ConsumedRecord {
    /// Reads a `<type>:<timestamp>\tOffset:<n>\t<key>\t<value>` line, or returns `None` for a
    /// line that isn't a record, such as a warning that Kafka 4 tools print on stdout.
    pub fn parse(line: &str) -> Option<Self> {
        let mut fields = line.splitn(4, '\t');
        let (timestamp_type, timestamp) = fields.next()?.split_once(':')?;
        let offset = fields.next()?.strip_prefix(OFFSET_FIELD_PREFIX)?;

        Some(Self {
            timestamp_type: timestamp_type.to_owned(),
            timestamp: timestamp.parse().ok()?,
            offset: offset.parse().ok()?,
            key: fields.next()?.to_owned(),
            value: fields.next()?.to_owned(),
        })
    }
}
