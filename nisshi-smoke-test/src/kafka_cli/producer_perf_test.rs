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

//! `kafka-producer-perf-test`, which produces generated records, in transactions if it gets a
//! transactional ID.

use super::{KafkaCli, Output, Tool};

/// Runs `kafka-producer-perf-test` against the broker of a [`KafkaCli`].
#[derive(Clone, Copy, Debug)]
pub struct KafkaProducerPerfTest<'cli> {
    cli: &'cli KafkaCli,
}

impl<'cli> KafkaProducerPerfTest<'cli> {
    pub(super) fn new(cli: &'cli KafkaCli) -> Self {
        Self { cli }
    }

    /// Produces `record_count` records of 100 bytes to `topic` as a transactional producer with
    /// `transactional_id`, committing a transaction every second.
    ///
    /// The tool's producer sends `InitProducerId` with the transactional ID before it sends any
    /// record.
    #[track_caller]
    pub fn produce_in_transactions(
        &self,
        topic: &str,
        record_count: u32,
        transactional_id: &str,
    ) -> Output {
        self.cli.run(
            Tool::ProducerPerfTest,
            &[
                "--topic",
                topic,
                "--num-records",
                &record_count.to_string(),
                "--throughput",
                "-1",
                "--record-size",
                "100",
                "--transactional-id",
                transactional_id,
                // Kafka 3.9 and earlier start transactions only if `--transaction-duration-ms` is
                // positive ([ProducerPerformance.java#L72-L73]).
                //
                // [ProducerPerformance.java#L72-L73]: https://github.com/apache/kafka/blob/3.9.1/tools/src/main/java/org/apache/kafka/tools/ProducerPerformance.java#L72-L73
                "--transaction-duration-ms",
                "1000",
            ],
        )
    }
}
