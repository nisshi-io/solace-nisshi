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

use std::{
    collections::VecDeque,
    fmt,
    fs::File,
    io::{self, BufRead, BufReader},
    path::{Path, PathBuf},
};

/// Why a broker failed its checks when it stopped, with the last lines of its log, which show what
/// the broker was doing.
#[derive(Debug)]
pub struct StopFailure {
    reasons: Vec<String>,
    log: PathBuf,
    last_log_lines: VecDeque<String>,
}

impl StopFailure {
    /// How many of the broker log's last lines a stop failure quotes.
    const QUOTED_LOG_LINES: usize = 40;

    /// Checks the log at `log` of a broker that has stopped. Returns a `StopFailure` with
    /// `reasons`, and with the first panic in the log, unless there are neither.
    pub(super) fn check(mut reasons: Vec<String>, log: &Path) -> Result<(), Self> {
        let mut last_log_lines = VecDeque::with_capacity(Self::QUOTED_LOG_LINES);

        match first_panic_in_log(log, &mut last_log_lines) {
            Ok(Some(panic)) => reasons.push(format!("broker panicked: {panic}")),
            Ok(None) => {}
            Err(err) => reasons.push(format!(
                "couldn't read {} to check for a panic: {err}",
                log.display()
            )),
        }

        if reasons.is_empty() {
            Ok(())
        } else {
            Err(Self {
                reasons,
                log: log.to_owned(),
                last_log_lines,
            })
        }
    }
}

impl fmt::Display for StopFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "{}", self.reasons.join("\n"))?;
        write!(formatter, "last lines of {}:", self.log.display())?;

        for line in &self.last_log_lines {
            write!(formatter, "\n{}", without_colour(line))?;
        }

        Ok(())
    }
}

/// Returns the first line of the broker's log at `log` that reports a panic, and keeps the log's
/// last [`StopFailure::QUOTED_LOG_LINES`] lines in `last_log_lines`. The function reads the log
/// one line at a time instead of all at once, because a broker that logs at debug level writes
/// gigabytes in one run.
fn first_panic_in_log(
    log: &Path,
    last_log_lines: &mut VecDeque<String>,
) -> io::Result<Option<String>> {
    let mut reader = BufReader::new(File::open(log)?);
    let mut first_panic = None;
    let mut bytes = Vec::new();

    while reader.read_until(b'\n', &mut bytes)? > 0 {
        // The broker can log bytes that aren't UTF-8, such as a record's value.
        let line = String::from_utf8_lossy(&bytes).trim_end().to_owned();
        bytes.clear();

        if first_panic.is_none() && line.contains("panicked at") {
            first_panic = Some(line.clone());
        }

        if last_log_lines.len() == StopFailure::QUOTED_LOG_LINES {
            _ = last_log_lines.pop_front();
        }
        last_log_lines.push_back(line);
    }

    Ok(first_panic)
}

/// Removes the terminal colour codes the broker writes into its log.
pub(super) fn without_colour(line: &str) -> String {
    let mut plain = String::with_capacity(line.len());
    let mut chars = line.chars();

    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            // An SGR sequence: ESC, '[', parameters, then 'm'.
            for c in chars.by_ref() {
                if c == 'm' {
                    break;
                }
            }
        } else {
            plain.push(c);
        }
    }

    plain
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, env, fs, path::PathBuf};

    use super::{StopFailure, first_panic_in_log, without_colour};
    use crate::common::unique;

    /// Writes `log` to a new file in the system's temporary directory, and returns its path.
    fn log_file(log: &str) -> PathBuf {
        let path = env::temp_dir().join(format!("{}.log", unique::name("broker")));
        fs::write(&path, log).expect("the test writes its log file");
        path
    }

    #[test]
    fn clean_log_has_no_panic() {
        let log = log_file("started\nstopped\n");
        let mut last_log_lines = VecDeque::new();

        assert_eq!(
            first_panic_in_log(&log, &mut last_log_lines).expect("the test reads its log file"),
            None
        );
        assert_eq!(last_log_lines, ["started", "stopped"]);

        _ = fs::remove_file(log);
    }

    #[test]
    fn first_panic_is_found_and_only_the_last_lines_are_kept() {
        let mut lines = vec!["thread 'a' panicked at src/a.rs:1:1:".to_owned()];
        lines.extend((0..StopFailure::QUOTED_LOG_LINES).map(|index| format!("line {index}")));
        lines.push("thread 'b' panicked at src/b.rs:2:2:".to_owned());
        let log = log_file(&lines.join("\n"));
        let mut last_log_lines = VecDeque::new();

        assert_eq!(
            first_panic_in_log(&log, &mut last_log_lines)
                .expect("the test reads its log file")
                .as_deref(),
            Some("thread 'a' panicked at src/a.rs:1:1:")
        );
        assert_eq!(last_log_lines.len(), StopFailure::QUOTED_LOG_LINES);
        assert_eq!(
            last_log_lines.back().map(String::as_str),
            Some("thread 'b' panicked at src/b.rs:2:2:")
        );

        _ = fs::remove_file(log);
    }

    #[test]
    fn colour_codes_are_removed() {
        assert_eq!(
            without_colour("\u{1b}[2m2026\u{1b}[0m \u{1b}[34mDEBUG\u{1b}[0m ready"),
            "2026 DEBUG ready"
        );
    }
}
