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

//! A Kafka CLI tool that runs while its test does other work, such as a consumer that stays in
//! its group while the test starts and stops other members. The test reads the tool's output as
//! it arrives, and stops the tool cleanly with SIGTERM or kills it with SIGKILL.

use std::{
    io::{BufRead as _, BufReader, Read},
    marker::PhantomData,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use super::{GRACE, KafkaCli, Output, Tool};
use crate::common::{timed_command, unique};

/// How long a background tool may run before `timeout` stops it inside the container. A test stops
/// its tools long before this, so the limit only ends a tool whose test has already failed.
///
/// nextest kills a test that runs too long without running `Drop`, so only this limit stops the
/// tools of a killed test, and they would otherwise keep loading the shared container.
const BACKGROUND_TOOL_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// How long a tool has to exit after SIGTERM or SIGKILL.
const STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// How long `pkill` has to signal a tool.
const SIGNAL_TIMEOUT: Duration = Duration::from_secs(15);

/// The Java system property that tags a background tool's JVM.
const JVM_TAG_PROPERTY: &str = "nisshi.smoke.tool";

/// Returns the JVM option that tags a tool's JVM with `jvm_tag`.
pub(super) fn jvm_tag_option(jvm_tag: &str) -> String {
    format!("-D{JVM_TAG_PROPERTY}={jvm_tag}")
}

/// A line that a background tool printed, and when the harness read it.
#[derive(Clone, Debug)]
pub struct PrintedLine {
    pub read_at: Instant,
    pub line: String,
}

/// The stdout lines of a [`BackgroundTool`], in the order it printed them. `PrintedBy` names the
/// tool method that started the tool, as it does for [`Output`], and the readers for the
/// lines are methods of `StdoutLines<ThatCommand>`.
#[derive(Clone, Debug)]
pub struct StdoutLines<PrintedBy> {
    pub lines: Vec<PrintedLine>,
    printed_by: PhantomData<PrintedBy>,
}

impl<PrintedBy> StdoutLines<PrintedBy> {
    fn new(lines: Vec<PrintedLine>) -> Self {
        Self {
            lines,
            printed_by: PhantomData,
        }
    }

    /// Returns the lines at index `first_line` and later, which are the lines that the tool
    /// printed after its first `first_line` lines.
    pub fn since(&self, first_line: usize) -> Self {
        Self::new(self.lines.get(first_line..).unwrap_or_default().to_vec())
    }
}

/// A Kafka CLI tool that runs in the background. Dropping it kills the tool.
#[derive(Debug)]
pub struct BackgroundTool<PrintedBy> {
    command_line: String,
    container: String,
    /// The tag on the tool's JVM command line, which `pkill` matches so that it signals only this
    /// tool in the container.
    jvm_tag: String,
    started: Instant,
    child: Option<Child>,
    stdout_lines: Arc<Mutex<Vec<PrintedLine>>>,
    stdout_reader: Option<JoinHandle<()>>,
    /// The tool's stderr lines so far, which a failure message shows while the tool runs. The
    /// Kafka tools log their errors and exceptions on stderr.
    stderr_lines: Arc<Mutex<Vec<PrintedLine>>>,
    stderr_reader: Option<JoinHandle<()>>,
    printed_by: PhantomData<PrintedBy>,
}

impl<PrintedBy> BackgroundTool<PrintedBy> {
    /// Starts `/opt/kafka/bin/<tool>.sh <args> --bootstrap-server <broker>` in the background,
    /// with `cli`'s settings.
    ///
    /// Panics if `docker exec` can't start.
    #[track_caller]
    pub(super) fn start(cli: &KafkaCli, tool: Tool, args: &[&str]) -> Self {
        let jvm_tag = unique::name(tool.name());
        let mut command = cli.command(tool, args, false, Some(&jvm_tag), BACKGROUND_TOOL_TIMEOUT);

        let mut child = match command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(err) => panic!("{command:?} could not start: {err}"),
        };

        let stdout_lines = Arc::new(Mutex::new(Vec::new()));
        let stdout_reader = child.stdout.take().map(|stdout| {
            let stdout_lines = Arc::clone(&stdout_lines);
            thread::spawn(move || record_lines(stdout, &stdout_lines))
        });

        let stderr_lines = Arc::new(Mutex::new(Vec::new()));
        let stderr_reader = child.stderr.take().map(|stderr| {
            let stderr_lines = Arc::clone(&stderr_lines);
            thread::spawn(move || record_lines(stderr, &stderr_lines))
        });

        Self {
            command_line: format!("{} {}", tool.name(), args.join(" ")),
            container: cli.container.clone(),
            jvm_tag,
            started: Instant::now(),
            child: Some(child),
            stdout_lines,
            stdout_reader,
            stderr_lines,
            stderr_reader,
            printed_by: PhantomData,
        }
    }
}

impl<PrintedBy> BackgroundTool<PrintedBy> {
    /// Returns the time at which the harness started the tool.
    pub fn started(&self) -> Instant {
        self.started
    }

    /// Returns the lines the tool has printed on stdout so far.
    pub fn stdout_lines(&self) -> StdoutLines<PrintedBy> {
        StdoutLines::new(locked(&self.stdout_lines).clone())
    }

    /// Returns how many lines the tool has printed on stdout so far.
    pub fn line_count(&self) -> usize {
        locked(&self.stdout_lines).len()
    }

    /// Waits until the tool prints a stdout line at or after index `from_line` for which `pick`
    /// returns a value, and returns that value. Returns an error with everything the tool printed
    /// if the tool does not print such a line within `timeout`, or exits before it does.
    pub fn wait_for_line<Picked>(
        &mut self,
        from_line: usize,
        timeout: Duration,
        pick: impl Fn(&PrintedLine) -> Option<Picked>,
    ) -> Result<Picked, String> {
        let deadline = Instant::now() + timeout;
        let mut lines_checked = from_line;

        loop {
            // The tool can exit after it prints the wanted line, so the harness checks whether it
            // has exited before it reads the lines. The reader thread can still hold the last
            // lines when the tool exits, so the tool counts as exited only once the thread has read
            // all of stdout.
            let exit_status = self
                .child
                .as_mut()
                .and_then(|child| child.try_wait().ok().flatten());
            let exited = (self.child.is_none() || exit_status.is_some())
                && self
                    .stdout_reader
                    .as_ref()
                    .is_none_or(JoinHandle::is_finished);

            {
                let lines = locked(&self.stdout_lines);

                if let Some(picked) = lines
                    .get(lines_checked..)
                    .unwrap_or_default()
                    .iter()
                    .find_map(&pick)
                {
                    return Ok(picked);
                }

                lines_checked = lines_checked.max(lines.len());
            }

            if exited || Instant::now() >= deadline {
                let reason = match exit_status {
                    Some(status) if exited => {
                        format!(
                            "exited with {status} before it printed the line the test waits for"
                        )
                    }
                    _ if exited => {
                        "exited before it printed the line the test waits for".to_owned()
                    }
                    _ => format!("did not print the line the test waits for within {timeout:?}"),
                };

                return Err(format!(
                    "`{}` {reason}\nstdout:\n{}\nstderr:\n{}",
                    self.command_line,
                    joined(&locked(&self.stdout_lines)),
                    joined(&locked(&self.stderr_lines))
                ));
            }

            thread::sleep(Duration::from_millis(100));
        }
    }

    /// Stops the tool as [`BackgroundTool::terminate`] does, and returns its [`Output`]. The tool
    /// prints its last lines, such as a producer's `tool_data`, as it stops.
    ///
    /// Panics if the tool exited before the test stopped it, or does not exit within
    /// `STOP_TIMEOUT` of the signal.
    #[track_caller]
    pub fn stop(mut self) -> Output<PrintedBy> {
        let exited = self
            .child
            .as_mut()
            .is_none_or(|child| matches!(child.try_wait(), Ok(Some(_))));

        if exited {
            let (output, _) = self.collect_exit();
            panic!("the tool exited before the test stopped it: {output}");
        }

        let (output, _) = self.terminate();
        output
    }

    /// Stops the tool with SIGTERM, which a Kafka client handles by closing cleanly, so a
    /// consumer leaves its group. Returns the tool's [`Output`], and its stdout lines with the time
    /// at which the harness read each.
    ///
    /// Panics if the tool does not exit within `STOP_TIMEOUT` of the signal.
    #[track_caller]
    pub fn terminate(self) -> (Output<PrintedBy>, StdoutLines<PrintedBy>) {
        self.signal_and_wait("TERM")
    }

    /// Kills the tool with SIGKILL, so a consumer stays in its group until its session times out.
    /// Returns the tool's [`Output`], and its stdout lines with the time at which the harness read
    /// each.
    ///
    /// Panics if the tool does not exit within `STOP_TIMEOUT` of the signal.
    #[track_caller]
    pub fn kill(self) -> (Output<PrintedBy>, StdoutLines<PrintedBy>) {
        self.signal_and_wait("KILL")
    }

    #[track_caller]
    fn signal_and_wait(mut self, signal: &str) -> (Output<PrintedBy>, StdoutLines<PrintedBy>) {
        if let Err(reason) = self.signal(signal) {
            panic!("{reason}");
        }

        self.collect_exit()
    }

    /// Sends `signal` to the tool's JVM inside the container. Signalling the `docker exec`
    /// client instead would leave the tool running in the container.
    fn signal(&self, signal: &str) -> Result<(), String> {
        // The tag is followed by the next JVM option or ends the command line, so one tag
        // doesn't match a longer tag that starts with it.
        let pattern = format!("{}( |$)", jvm_tag_option(&self.jvm_tag));

        let signalled = timed_command::run(
            Command::new("docker").args([
                "exec",
                &self.container,
                "pkill",
                &format!("-{signal}"),
                "-f",
                "--",
                &pattern,
            ]),
            None,
            SIGNAL_TIMEOUT,
        );

        // The function ignores `pkill`'s exit status. `pkill` exits with 1 if the tool has already
        // exited, and `collect_exit` reports how the tool exited.
        signalled.map(drop).map_err(|reason| {
            format!(
                "could not send SIG{signal} to `{}`: {reason}",
                self.command_line
            )
        })
    }

    #[track_caller]
    fn collect_exit(&mut self) -> (Output<PrintedBy>, StdoutLines<PrintedBy>) {
        let mut child = self.child.take().expect("the tool is running");

        let status = timed_command::wait(&mut child, STOP_TIMEOUT + GRACE);

        // `wait` kills only the `docker exec` client, which leaves the tool running in the
        // container. The function reports a failure to kill it instead of panicking, because the
        // assert below panics with the tool's output.
        if status.is_none()
            && let Err(reason) = self.signal("KILL")
        {
            eprintln!("{reason}");
        }

        for reader in [self.stdout_reader.take(), self.stderr_reader.take()]
            .into_iter()
            .flatten()
        {
            _ = reader.join();
        }

        let lines = self.stdout_lines();

        let output = Output::new(
            self.command_line.clone(),
            status.and_then(|status| status.code()),
            joined(&lines.lines),
            joined(&locked(&self.stderr_lines)),
            status
                .is_none()
                .then(|| format!("did not exit within {STOP_TIMEOUT:?} of the signal")),
        );

        assert!(output.timeout.is_none(), "{output}");

        (output, lines)
    }
}

impl<PrintedBy> Drop for BackgroundTool<PrintedBy> {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            // A second panic while the test is already failing would abort the test binary, so a
            // tool that can't be signalled is only reported.
            if matches!(child.try_wait(), Ok(None))
                && let Err(reason) = self.signal("KILL")
            {
                eprintln!("{reason}");
            }

            _ = timed_command::wait(&mut child, STOP_TIMEOUT);
        }

        // A test that panics drops the tools it still runs without reading their output, and a
        // tool's stderr can hold the client error that caused the failure. The harness prints
        // stderr and not stdout, because a consumer's stdout has a line for each poll.
        if thread::panicking() {
            eprintln!(
                "`{}` stderr, printed because the test failed:\n{}",
                self.command_line,
                joined(&locked(&self.stderr_lines))
            );
        }
    }
}

fn record_lines(pipe: impl Read, lines: &Mutex<Vec<PrintedLine>>) {
    for line in BufReader::new(pipe).lines() {
        let Ok(line) = line else {
            return;
        };

        locked(lines).push(PrintedLine {
            read_at: Instant::now(),
            line,
        });
    }
}

/// Locks `lines`, also after a test panicked while it held the lock, so that `Drop` and a failure
/// message still read them.
fn locked(lines: &Mutex<Vec<PrintedLine>>) -> MutexGuard<'_, Vec<PrintedLine>> {
    lines.lock().unwrap_or_else(PoisonError::into_inner)
}

fn joined(lines: &[PrintedLine]) -> String {
    lines
        .iter()
        .map(|printed_line| format!("{}\n", printed_line.line))
        .collect()
}
