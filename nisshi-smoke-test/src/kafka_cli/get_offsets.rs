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

//! `kafka-get-offsets`, which finds a topic's offsets.

use super::{KafkaCli, Output, Tool};

/// The command of [`KafkaGetOffsets::at`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum GetOffsets {}

/// Runs `kafka-get-offsets` against the broker of a [`KafkaCli`].
#[derive(Clone, Copy, Debug)]
pub struct KafkaGetOffsets<'cli> {
    cli: &'cli KafkaCli,
}

impl<'cli> KafkaGetOffsets<'cli> {
    pub(super) fn new(cli: &'cli KafkaCli) -> Self {
        Self { cli }
    }
    /// Reads each partition's offset of `topic` at `time`, which is `earliest`, `latest` or a
    /// timestamp in milliseconds. The tool prints one `<topic>:<partition>:<offset>` line per
    /// partition.
    ///
    /// Sends `ListOffsets`.
    #[track_caller]
    pub fn at(&self, topic: &str, time: &str) -> Output<GetOffsets> {
        self.cli
            .run(Tool::GetOffsets, &["--topic", topic, "--time", time])
    }

    /// Returns each partition's offset of `topic` at `time`, as [`KafkaGetOffsets::at`] reads
    /// it. Panics, showing the tool's output, if the tool fails.
    #[track_caller]
    pub fn partition_offsets(&self, topic: &str, time: &str) -> Vec<i64> {
        self.at(topic, time).succeeded().partition_offsets(topic)
    }
}

impl Output<GetOffsets> {
    /// Returns each of `topic`'s partition offsets, in the order the tool printed them. The tool
    /// prints one `<topic>:<partition>:<offset>` line per partition, and Kafka 4 tools also print
    /// warnings on stdout, so the reader reads only the lines that start with `topic`.
    pub fn partition_offsets(&self, topic: &str) -> Vec<i64> {
        self.lines()
            .iter()
            .filter_map(|line| line.strip_prefix(topic)?.strip_prefix(':'))
            .filter_map(|partition_and_offset| {
                partition_and_offset.rsplit(':').next()?.parse().ok()
            })
            .collect()
    }
}
