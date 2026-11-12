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

//! `kafka-cluster`, which describes the cluster.

use super::{KafkaCli, Output, Tool};

/// How `kafka-cluster cluster-id` starts the line that holds the cluster's id.
const CLUSTER_ID_LINE_PREFIX: &str = "Cluster ID: ";

/// The command of [`KafkaCluster::id`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum ClusterId {}

/// Runs `kafka-cluster` against the broker of a [`KafkaCli`].
#[derive(Clone, Copy, Debug)]
pub struct KafkaCluster<'cli> {
    cli: &'cli KafkaCli,
}

impl<'cli> KafkaCluster<'cli> {
    pub(super) fn new(cli: &'cli KafkaCli) -> Self {
        Self { cli }
    }

    /// Prints the cluster's id, as `Cluster ID: <id>`.
    ///
    /// Sends `DescribeCluster`.
    #[track_caller]
    pub fn id(&self) -> Output<ClusterId> {
        self.cli.run(Tool::Cluster, &["cluster-id"])
    }
}

impl Output<ClusterId> {
    /// Returns the cluster's id.
    pub fn cluster_id(&self) -> Option<&str> {
        self.stdout
            .lines()
            .find_map(|line| line.strip_prefix(CLUSTER_ID_LINE_PREFIX))
    }
}
