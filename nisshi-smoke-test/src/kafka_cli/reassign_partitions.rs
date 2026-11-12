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

//! `kafka-reassign-partitions`, which moves partitions between brokers and lists the moves in
//! progress.

use super::{KafkaCli, Output, Tool};

/// What `kafka-reassign-partitions --list` prints when no reassignment is in progress.
pub const NO_REASSIGNMENTS: &str = "No partition reassignments found.";

/// Runs `kafka-reassign-partitions` against the broker of a [`KafkaCli`].
#[derive(Clone, Copy, Debug)]
pub struct KafkaReassignPartitions<'cli> {
    cli: &'cli KafkaCli,
}

impl<'cli> KafkaReassignPartitions<'cli> {
    pub(super) fn new(cli: &'cli KafkaCli) -> Self {
        Self { cli }
    }
    /// Lists the partition reassignments in progress.
    ///
    /// Sends `ListPartitionReassignments`.
    #[track_caller]
    pub fn list(&self) -> Output {
        self.cli.run(Tool::ReassignPartitions, &["--list"])
    }
}
