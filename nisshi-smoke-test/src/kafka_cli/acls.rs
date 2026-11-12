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

//! `kafka-acls`, which adds, lists and removes ACLs.

use super::{KafkaCli, Output, Tool};

/// Runs `kafka-acls` against the broker of a [`KafkaCli`].
#[derive(Clone, Copy, Debug)]
pub struct KafkaAcls<'cli> {
    cli: &'cli KafkaCli,
}

impl<'cli> KafkaAcls<'cli> {
    pub(super) fn new(cli: &'cli KafkaCli) -> Self {
        Self { cli }
    }

    /// Adds an ACL that allows `principal`, such as `User:alice`, to do `operation`, such as
    /// `Read`, on `topic`.
    ///
    /// Sends `CreateAcls`.
    #[track_caller]
    pub fn allow(&self, principal: &str, operation: &str, topic: &str) -> Output {
        self.cli.run(
            Tool::Acls,
            &[
                "--add",
                "--allow-principal",
                principal,
                "--operation",
                operation,
                "--topic",
                topic,
            ],
        )
    }

    /// Removes the ACL that allows `principal` to do `operation` on `topic`, without asking for
    /// confirmation.
    ///
    /// Sends `DeleteAcls`.
    #[track_caller]
    pub fn remove_allowed(&self, principal: &str, operation: &str, topic: &str) -> Output {
        self.cli.run(
            Tool::Acls,
            &[
                "--remove",
                "--force",
                "--allow-principal",
                principal,
                "--operation",
                operation,
                "--topic",
                topic,
            ],
        )
    }

    /// Removes every ACL on `topic`, without asking for confirmation.
    ///
    /// Sends `DeleteAcls`.
    #[track_caller]
    pub fn remove_all_on_topic(&self, topic: &str) -> Output {
        self.cli
            .run(Tool::Acls, &["--remove", "--force", "--topic", topic])
    }

    /// Lists the ACLs on `topic`.
    ///
    /// Sends `DescribeAcls`.
    #[track_caller]
    pub fn list_for_topic(&self, topic: &str) -> Output {
        self.cli.run(Tool::Acls, &["--list", "--topic", topic])
    }

    /// Lists every ACL in the cluster.
    ///
    /// Sends `DescribeAcls`.
    #[track_caller]
    pub fn list_all(&self) -> Output {
        self.cli.run(Tool::Acls, &["--list"])
    }
}
