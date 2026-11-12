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

use std::{fmt, marker::PhantomData};

use serde::de::DeserializeOwned;

use super::Tool;
use crate::common::timed_command;

/// What a Kafka CLI tool printed, and how it exited. Its `Display` form shows the command, the exit
/// code, and the full stdout and stderr, for use in a failure message.
///
/// `PrintedBy` names the tool method that ran the tool, such as
/// [`DescribeTopic`](super::DescribeTopic) for
/// [`KafkaTopics::describe`](super::KafkaTopics::describe). The readers for a command's output are
/// methods of `Output<ThatCommand>`, so the compiler rejects a reader called on another command's
/// output.
#[derive(Debug)]
pub struct Output<PrintedBy = AnyCommand> {
    pub command: String,
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    /// Why the harness stopped the tool, if it didn't finish in time.
    pub timeout: Option<String>,
    printed_by: PhantomData<PrintedBy>,
}

/// The `PrintedBy` of an [`Output`] that no reader reads, beyond its exit code and its text.
#[derive(Clone, Copy, Debug)]
pub enum AnyCommand {}

impl<PrintedBy> Output<PrintedBy> {
    /// Returns the output of `tool`, run with `args`, that finished as `finished`. `timeout` says
    /// why the harness stopped the tool, if it didn't finish in time.
    pub(super) fn of_tool(
        tool: Tool,
        args: &[&str],
        finished: timed_command::Finished,
        timeout: Option<String>,
    ) -> Self {
        Self::new(
            format!("{} {}", tool.name(), args.join(" ")),
            finished.code,
            finished.stdout,
            finished.stderr,
            timeout,
        )
    }

    pub(super) fn new(
        command: String,
        code: Option<i32>,
        stdout: String,
        stderr: String,
        timeout: Option<String>,
    ) -> Self {
        Self {
            command,
            code,
            stdout,
            stderr,
            timeout,
            printed_by: PhantomData,
        }
    }
}

impl<PrintedBy> Output<PrintedBy> {
    /// Returns what a tool printed on `stdout` when it exited with 0, for a reader's unit test.
    #[cfg(test)]
    pub(super) fn printed(stdout: &str) -> Self {
        Self {
            command: "sample".to_owned(),
            code: Some(0),
            stdout: stdout.to_owned(),
            stderr: String::new(),
            timeout: None,
            printed_by: PhantomData,
        }
    }

    /// Reads each event that `tool`, a verifiable tool, printed on stdout, as
    /// [`Tool::read_json_event`] does.
    #[track_caller]
    pub(super) fn json_events<Event: DeserializeOwned>(
        &self,
        tool: Tool,
    ) -> impl Iterator<Item = Event> {
        self.stdout
            .lines()
            .filter_map(move |line| tool.read_json_event(line))
    }

    /// Asserts the tool exited with `code`, showing its output if not.
    #[track_caller]
    pub fn exited(&self, code: i32) -> &Self {
        assert_eq!(self.code, Some(code), "{self}");
        self
    }

    /// Asserts the tool exited with 0.
    #[track_caller]
    pub fn succeeded(&self) -> &Self {
        self.exited(0)
    }

    pub fn lines(&self) -> Vec<&str> {
        self.stdout.lines().collect()
    }

    /// Returns whether the tool printed `text` on stdout or stderr. A tool prints an exception on
    /// one or the other, depending on where it caught it.
    pub fn mentions(&self, text: &str) -> bool {
        self.stdout.contains(text) || self.stderr.contains(text)
    }
}

impl<PrintedBy> fmt::Display for Output<PrintedBy> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.timeout {
            Some(reason) => write!(formatter, "`{}` {reason}", self.command)?,
            None => write!(formatter, "`{}` exited with {:?}", self.command, self.code)?,
        }

        write!(
            formatter,
            "\nstdout:\n{}\nstderr:\n{}",
            self.stdout, self.stderr
        )
    }
}
