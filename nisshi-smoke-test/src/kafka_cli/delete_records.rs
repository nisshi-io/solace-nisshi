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

//! `kafka-delete-records`, which deletes a partition's records before an offset.

use super::{KafkaCli, Output, Tool};

/// How `kafka-delete-records` starts the line for each partition, before `<topic>-<partition>`.
const PARTITION_LINE_PREFIX: &str = "partition: ";
/// How `kafka-delete-records` starts the field that holds a partition's new low watermark.
const LOW_WATERMARK_FIELD_PREFIX: &str = "low_watermark: ";

/// The command of [`KafkaDeleteRecords::before`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum DeleteRecords {}

/// Runs `kafka-delete-records` against the broker of a [`KafkaCli`].
#[derive(Clone, Copy, Debug)]
pub struct KafkaDeleteRecords<'cli> {
    cli: &'cli KafkaCli,
}

impl<'cli> KafkaDeleteRecords<'cli> {
    pub(super) fn new(cli: &'cli KafkaCli) -> Self {
        Self { cli }
    }
    /// Deletes the records of `topic`'s `partition` before `offset`. The tool prints a
    /// `partition: <topic>-<partition>\tlow_watermark: <offset>` line for the partition.
    ///
    /// Sends `DeleteRecords`.
    #[track_caller]
    pub fn before(&self, topic: &str, partition: u32, offset: i64) -> Output<DeleteRecords> {
        let offsets = format!(
            r#"{{"partitions":[{{"topic":"{topic}","partition":{partition},"offset":{offset}}}],"#
        ) + r#""version":1}"#;
        let offset_json_file = self
            .cli
            .write_file_in_container("delete-records", "json", &offsets);

        self.cli.run(
            Tool::DeleteRecords,
            &["--offset-json-file", &offset_json_file],
        )
    }
}

impl Output<DeleteRecords> {
    /// Returns the low watermark that the tool printed for `topic`'s `partition`, or `None` if it
    /// printed none, such as when the broker returned an error for the partition.
    pub fn low_watermark(&self, topic: &str, partition: u32) -> Option<i64> {
        let partition_line_start = format!("{PARTITION_LINE_PREFIX}{topic}-{partition}\t");

        self.lines().iter().find_map(|line| {
            line.strip_prefix(&partition_line_start)?
                .strip_prefix(LOW_WATERMARK_FIELD_PREFIX)?
                .trim()
                .parse()
                .ok()
        })
    }
}
