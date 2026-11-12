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

//! `kafka-broker-api-versions`, which lists the APIs each broker supports.

use super::{KafkaCli, Output, Tool};

/// What `kafka-broker-api-versions` prints before the node id, on its first line.
const NODE_ID_MARKER: &str = "(id: ";

/// The command of [`KafkaBrokerApiVersions::list`]'s [`Output`].
#[derive(Clone, Copy, Debug)]
pub enum ApiVersions {}

/// Runs `kafka-broker-api-versions` against the broker of a [`KafkaCli`].
#[derive(Clone, Copy, Debug)]
pub struct KafkaBrokerApiVersions<'cli> {
    cli: &'cli KafkaCli,
}

impl<'cli> KafkaBrokerApiVersions<'cli> {
    pub(super) fn new(cli: &'cli KafkaCli) -> Self {
        Self { cli }
    }

    /// Lists the API versions the broker supports, one `<name>(<key>): <min> to <max> [usable:
    /// <version>]` line per API, after a line naming the broker.
    ///
    /// Sends `ApiVersions` and `Metadata`.
    #[track_caller]
    pub fn list(&self) -> Output<ApiVersions> {
        self.cli.run(Tool::BrokerApiVersions, &[])
    }
}

impl Output<ApiVersions> {
    /// Returns the node id that the listing names on its first line,
    /// `<host>:<port> (id: <node> rack: ...) -> (`.
    pub fn node_id(&self) -> Option<i32> {
        self.stdout
            .lines()
            .next()?
            .split_once(NODE_ID_MARKER)?
            .1
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    }
}
