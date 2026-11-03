// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::{fmt, fs, path::Path};

use super::stop_failure;

/// A broker that didn't start: how it was launched, how it exited, and what it printed.
#[derive(Debug)]
pub struct FailedStart {
    /// The `nisshi broker` command line.
    command: String,
    /// The broker's exit code, or `None` when the harness killed a broker that didn't answer.
    exit_code: Option<i32>,
    /// What the broker printed, stdout and stderr together.
    log: String,
    /// Why the harness counts the start as failed.
    reason: String,
}

impl FailedStart {
    /// Returns a failed start of the broker launched with `command`, with the log that the broker
    /// wrote to `log`, without its colour codes.
    pub(super) fn from_log(
        command: String,
        exit_code: Option<i32>,
        log: &Path,
        reason: String,
    ) -> Self {
        let log = fs::read_to_string(log).unwrap_or_default();

        Self {
            command,
            exit_code,
            log: log
                .lines()
                .map(|line| stop_failure::without_colour(line) + "\n")
                .collect(),
            reason,
        }
    }

    /// Returns whether the broker refused to start as a user expects: it exited with a code other
    /// than 0, and didn't panic.
    pub(super) fn exited_with_error(&self) -> bool {
        self.exit_code.is_some_and(|code| code != 0) && !self.log.contains("panicked at")
    }

    /// Returns the error lines the broker printed: each line that starts with `Error: ` or
    /// `error: `, or that tracing logged at level `ERROR`.
    ///
    /// A test checks these lines rather than the whole log, because other lines can name the
    /// storage URL: the banner, once the broker listens, and its logs. A bad value in the URL can
    /// therefore be in the log whatever the error says.
    fn error_lines(&self) -> impl Iterator<Item = &str> {
        self.log.lines().filter(|line| {
            line.starts_with("Error: ")
                || line.starts_with("error: ")
                || line.split_whitespace().nth(1) == Some("ERROR")
        })
    }

    /// Asserts the broker exited with an error line that contains `text`.
    #[track_caller]
    pub fn assert_error_names(&self, text: &str) {
        assert!(
            self.error_lines().any(|line| line.contains(text)),
            "the broker didn't exit with an error that names {text}: {self}"
        );
    }
}

impl fmt::Display for FailedStart {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let exit_code = self.exit_code.map_or_else(
            || "none, the harness killed it".to_owned(),
            |code| code.to_string(),
        );

        write!(
            formatter,
            "`{}` didn't start: {}\nexit code: {exit_code}\nlog:\n{}",
            self.command, self.reason, self.log
        )
    }
}

#[cfg(test)]
mod tests {
    use super::FailedStart;

    fn failed_start_with_log(log: &str) -> FailedStart {
        FailedStart {
            command: "nisshi broker".to_owned(),
            exit_code: Some(1),
            log: log.to_owned(),
            reason: "exited with 1".to_owned(),
        }
    }

    #[test]
    fn error_lines_leave_out_the_storage_url_the_broker_prints_at_startup() {
        let failed_start = failed_start_with_log(
            "storage: sqlite:////proc/nisshi/nisshi.db [\"sqlite\"]\nError: Io(PermissionDenied)\n\
             storage: sqlite:////proc/nisshi/nisshi.db\n",
        );

        assert_eq!(
            failed_start.error_lines().collect::<Vec<_>>(),
            ["Error: Io(PermissionDenied)"]
        );
    }

    #[test]
    fn error_lines_include_an_argument_error_from_clap() {
        let failed_start = failed_start_with_log(
            "error: invalid value 'not a url' for '--storage-engine <STORAGE_ENGINE>'\n\nFor more \
             information, try '--help'.\n",
        );

        failed_start.assert_error_names("not a url");
    }

    #[test]
    fn error_lines_include_an_error_that_tracing_logged() {
        let failed_start = failed_start_with_log(
            "storage: postgres://localhost?maintenance_interval=0s [\"postgres\"]\n\
             2026-10-08T21:09:59.160193Z ERROR nisshi: 91: storage option maintenance_interval=0s \
             is not a valid interval\n\
             Error: Server(InvalidStorageOptionValue { option: \"maintenance_interval\" })\n",
        );

        failed_start.assert_error_names("maintenance_interval=0s");
    }

    #[test]
    fn error_lines_are_empty_when_the_broker_printed_no_error() {
        let failed_start =
            failed_start_with_log("thread 'main' panicked at src/main.rs:1:1:\nsome message\n");

        assert_eq!(failed_start.error_lines().count(), 0);
    }
}
