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

//! `kafka-console-producer`, which produces a record for each line it reads.

use super::{KafkaCli, Output, Tool};
use crate::Record;

/// What the console producer logs on stderr for each record that the broker rejects.
const CONSOLE_PRODUCER_SEND_ERROR: &str = "Error when sending message";

/// The text the console producer reads as a null value, for a tombstone.
const NULL_MARKER: &str = "<null>";

/// Runs `kafka-console-producer` against the broker of a [`KafkaCli`].
#[derive(Clone, Copy, Debug)]
pub struct KafkaConsoleProducer<'cli> {
    cli: &'cli KafkaCli,
}

impl<'cli> KafkaConsoleProducer<'cli> {
    pub(super) fn new(cli: &'cli KafkaCli) -> Self {
        Self { cli }
    }
    /// Produces `records` to `topic`, keys and headers included, with the console producer.
    ///
    /// Sends `InitProducerId`, because the producer is idempotent, and `Produce`.
    ///
    /// Panics if the producer fails, or if the broker rejects a record.
    #[track_caller]
    pub fn produce(&self, topic: &str, records: &[Record<'_>]) -> Output {
        let produced = self.cli.run_with_input(
            Tool::ConsoleProducer,
            &[
                "--topic",
                topic,
                "--property",
                "parse.headers=true",
                "--property",
                "parse.key=true",
            ],
            &Record::console_input(records),
        );

        assert_every_record_accepted(&produced);
        produced
    }

    /// Produces `values` to `topic` without keys, one record per value, with the console producer.
    ///
    /// Sends `InitProducerId` and `Produce`.
    ///
    /// Panics if the producer fails, or if the broker rejects a record.
    #[track_caller]
    pub fn produce_values(&self, topic: &str, values: &[impl AsRef<str>]) -> Output {
        self.produce_values_with(topic, values, &[])
    }

    /// Like [`KafkaConsoleProducer::produce_values`], with each of `properties` as a `name=value`
    /// producer config. The producer sends `InitProducerId` unless a property turns idempotence
    /// off.
    #[track_caller]
    pub fn produce_values_with(
        &self,
        topic: &str,
        values: &[impl AsRef<str>],
        properties: &[&str],
    ) -> Output {
        let produced = self.run_console_producer(topic, values, properties);
        assert_every_record_accepted(&produced);
        produced
    }

    /// Like [`KafkaConsoleProducer::produce_values`], but returns the producer's output without
    /// checking it, for a test that expects the broker to reject a record.
    #[track_caller]
    pub fn try_produce_values(&self, topic: &str, values: &[impl AsRef<str>]) -> Output {
        self.run_console_producer(topic, values, &[])
    }

    #[track_caller]
    fn run_console_producer(
        &self,
        topic: &str,
        values: &[impl AsRef<str>],
        properties: &[&str],
    ) -> Output {
        let mut args = vec!["--topic", topic];

        for property in properties {
            args.extend(["--producer-property", property]);
        }

        let input = values
            .iter()
            .map(|value| format!("{}\n", value.as_ref()))
            .collect::<String>();
        self.cli
            .run_with_input(Tool::ConsoleProducer, &args, &input)
    }

    /// Produces `records` to `topic`, each a key and a value, with the console producer. A
    /// `None` value is a tombstone.
    ///
    /// Sends `InitProducerId` and `Produce`.
    ///
    /// Panics if the producer fails, or if the broker rejects a record.
    #[track_caller]
    pub fn produce_keyed(&self, topic: &str, records: &[(&str, Option<&str>)]) -> Output {
        let null_marker = format!("null.marker={NULL_MARKER}");
        let input = records
            .iter()
            .map(|(key, value)| format!("{key}\t{}\n", value.unwrap_or(NULL_MARKER)))
            .collect::<String>();

        let produced = self.cli.run_with_input(
            Tool::ConsoleProducer,
            &[
                "--topic",
                topic,
                "--property",
                "parse.key=true",
                "--property",
                &null_marker,
            ],
            &input,
        );

        assert_every_record_accepted(&produced);
        produced
    }
}

/// Asserts that a console producer exited with 0 and did not log an error for a record. The
/// console producer exits with 0 even if the broker rejects a record, and only logs the error.
#[track_caller]
fn assert_every_record_accepted(produced: &Output) {
    assert!(
        !produced
            .succeeded()
            .stderr
            .contains(CONSOLE_PRODUCER_SEND_ERROR),
        "the broker rejected a record: {produced}"
    );
}
