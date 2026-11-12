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
    fmt, fs,
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, Instant},
};

use super::{STOP_TIMEOUT, Shutdown};
use crate::{
    broker::{LaunchOptions, files::BrokerFiles},
    common::{docker, timed_command, unique},
};

/// How long each of `docker create` and `docker start` has to finish for a broker container.
/// `run.sh` pulls the image before the tests run, so this covers only creating or starting the
/// container, and stays well under the time nextest gives a test before it kills it.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_secs(60);

/// How many times to ask `docker logs` for the log of a broker that exited without printing, and
/// how long to wait between asks, and when to stop asking whatever the count. Each ask gets only
/// the time left in the window, so one hung `docker logs` cannot hold the test for its usual
/// minute: the test must reach its failure report before nextest kills it.
const LOG_ATTEMPTS: u32 = 20;
const LOG_WINDOW: Duration = Duration::from_secs(3);
const LOG_RETRY_DELAY: Duration = Duration::from_millis(100);

/// The least time that one `docker logs` gets, however little of the window is left.
const LOG_MIN_TIMEOUT: Duration = Duration::from_secs(1);

/// How long the `docker inspect` that describes a lost log has to finish.
const LOG_CONTEXT_TIMEOUT: Duration = Duration::from_secs(5);

/// What `docker cp` prints when the file to copy isn't in the container.
const DOCKER_CP_MISSING_FILE: &str = "Could not find the file";

/// A broker that runs in a container from the Docker image in `NISSHI_SMOKE_IMAGE`.
#[derive(Debug)]
pub(in crate::broker) struct BrokerContainer {
    name: String,
    /// The broker's own directory on this machine, where the harness copies a file out of the
    /// container to read it.
    dir: PathBuf,
}

impl BrokerContainer {
    /// Starts `image` in a container, with a SQLite broker's volume in `files` as its working
    /// directory. A TLS broker reads its certificate and private key from a copy of `files`' TLS
    /// directory in [`BrokerContainer::tls_directory`].
    ///
    /// Returns an error if the harness can't create the container, copy the TLS directory into it,
    /// or start it.
    pub(super) fn launch(
        image: &str,
        options: &LaunchOptions,
        files: &BrokerFiles,
    ) -> Result<Self, String> {
        let name = unique::name("nisshi-smoke-broker");

        let mut command = Command::new("docker");

        _ = command.args([
            "create",
            "--name",
            &name,
            &docker::label(),
            "--network=host",
        ]);

        // The image sets its own PATH and HOME.
        for (variable, _) in super::broker_environment() {
            if variable != "PATH" && variable != "HOME" {
                _ = command.args(["--env", &variable]);
            }
        }

        if let Some(volume) = files.volume() {
            _ = command.args(["--volume", &format!("{volume}:/data"), "--workdir=/data"]);
        }

        _ = command
            .arg(image)
            .args(options.broker_args(Self::tls_directory()));

        let created = timed_command::run(&mut command, None, CONTAINER_START_TIMEOUT);

        if !matches!(&created, Ok(created) if created.code == Some(0)) {
            remove_container(&name);
            return Err(format!(
                "could not create a container from {image}: {created:?}"
            ));
        }

        // The harness copies the TLS directory in with `docker cp` instead of bind-mounting it,
        // because the Docker daemon can run in a virtual machine that doesn't see this machine's
        // files. `docker cp` creates `/tls` in the container.
        if options.tls.is_some() {
            let source = files.tls_directory().display().to_string();
            let destination = format!("{name}:{}", Self::tls_directory().display());
            let copied = docker::execute(&["cp", &source, &destination]);

            if !matches!(&copied, Ok(copied) if copied.code == Some(0)) {
                remove_container(&name);
                return Err(format!(
                    "could not copy {source} to {destination}: {copied:?}"
                ));
            }
        }

        let started = timed_command::run(
            Command::new("docker").args(["start", &name]),
            None,
            CONTAINER_START_TIMEOUT,
        );

        if !matches!(&started, Ok(started) if started.code == Some(0)) {
            remove_container(&name);
            return Err(format!("could not start {name} from {image}: {started:?}"));
        }

        Ok(Self {
            name,
            dir: files.dir().to_owned(),
        })
    }

    /// Returns the directory in the container where the broker reads its TLS certificate and
    /// private key, a copy of [`BrokerFiles::tls_directory`].
    pub(super) fn tls_directory() -> &'static Path {
        Path::new("/tls")
    }

    /// An error if the broker has exited.
    pub(super) fn check_running(&self) -> Result<(), String> {
        match docker::inspect(&self.name, "{{.State.Running}} {{.State.ExitCode}}")?.split_once(' ')
        {
            Some(("true", _)) => Ok(()),
            Some((_, code)) => Err(format!("exited with {code}")),
            None => Err(format!("container {} has no state", self.name)),
        }
    }

    /// Stops the container as a broker process is stopped, saves what it printed to `log`, and
    /// removes it. Adds each check that it fails to `failures`.
    pub(super) fn stop(self, shutdown: Shutdown, log: &Path, failures: &mut Vec<String>) {
        if let Err(reason) = self.check_running() {
            failures.push(format!("broker {reason} during the run"));
        } else {
            self.signal_and_check_exit(shutdown, failures);
        }

        failures.extend(self.save_logs(log).err());
        remove_container(&self.name);
    }

    fn signal_and_check_exit(&self, shutdown: Shutdown, failures: &mut Vec<String>) {
        let name = &self.name;

        match shutdown {
            Shutdown::Graceful => {
                let stopped = timed_command::run(
                    Command::new("docker").args([
                        "stop",
                        &format!("--time={}", STOP_TIMEOUT.as_secs()),
                        name,
                    ]),
                    None,
                    STOP_TIMEOUT + Duration::from_secs(30),
                );

                if let Err(reason) = stopped {
                    failures.push(reason);
                }

                match docker::inspect(name, "{{.State.ExitCode}}") {
                    Ok(code) if code == "0" => {}
                    Ok(code) => failures.push(format!("broker exited with {code} on SIGTERM")),
                    Err(reason) => failures.push(reason),
                }
            }

            Shutdown::Kill => match docker::execute(&["kill", "--signal=KILL", name]) {
                Ok(killed) if killed.code == Some(0) => {}
                killed => failures.push(format!("couldn't kill {name}: {killed:?}")),
            },
        }
    }

    /// Kills a broker container that didn't start, if it is still running, saves what it printed
    /// to `log`, and removes it. Returns its exit code, or `None` if the harness killed it, and
    /// why the log is missing or empty, if it is.
    pub(super) fn stop_after_failed_start(&self, log: &Path) -> (Option<i32>, Option<String>) {
        let (killed, code) = match self.check_running() {
            Ok(()) => {
                _ = docker::execute(&["kill", &self.name]);
                (true, None)
            }

            Err(_) => (
                false,
                docker::inspect(&self.name, "{{.State.ExitCode}}")
                    .ok()
                    .and_then(|code| code.parse().ok()),
            ),
        };

        // Save the container's output before removing the container deletes it.
        // `FailedStart` reads the broker's error from this log, so without it a test
        // that expects a refusal fails. A broker that exited has nothing more to print, so an
        // empty log is a lost one: ask again for a moment, in case `docker logs` failed
        // transiently, also when `docker inspect` could not read the exit code. A broker that the
        // harness killed may print nothing, so ask once.
        let log_problem = if killed {
            self.save_logs(log).err()
        } else {
            collect_log(LOG_ATTEMPTS, LOG_RETRY_DELAY, LOG_WINDOW, |timeout| {
                self.save_logs_expecting_output(log, timeout)
            })
            .err()
            .map(|problem| format!("{problem}; {}", self.log_context()))
        };

        remove_container(&self.name);
        (code, log_problem)
    }

    /// Describes how the container logs and how long it lived, to tell a log lost in a
    /// short-lived container from one that `docker logs` couldn't read.
    fn log_context(&self) -> String {
        match docker::inspect_within(
            &self.name,
            "{{.HostConfig.LogConfig.Type}} started {{.State.StartedAt}} finished {{.State.FinishedAt}}",
            LOG_CONTEXT_TIMEOUT,
        ) {
            Ok(context) => format!("log driver {context}"),
            Err(reason) => reason,
        }
    }

    #[track_caller]
    pub(super) fn file_metadata(&self, path: &str) -> Option<fs::Metadata> {
        let name = &self.name;

        // The broker's image holds only the broker, without a shell or `stat`, so the
        // harness copies the file out with `docker cp`, which keeps its modification time.
        // `docker cp` reads a relative path from the container's root, not from its
        // working directory.
        let working_dir = match docker::inspect(name, "{{.Config.WorkingDir}}") {
            Ok(working_dir) => working_dir,
            Err(reason) => panic!("{reason}"),
        };
        let path_in_container = Path::new("/").join(working_dir).join(path);

        let copy = self.dir.join(unique::name("copied"));
        let source = format!("{name}:{}", path_in_container.display());

        let copied = match docker::execute(&["cp", &source, &copy.display().to_string()]) {
            Ok(copied) => copied,
            Err(reason) => panic!("{reason}"),
        };

        let metadata = match copied.code {
            Some(0) => match fs::metadata(&copy) {
                Ok(metadata) => Some(metadata),
                Err(err) => panic!("{}: {err}", copy.display()),
            },
            _ if copied.stderr.contains(DOCKER_CP_MISSING_FILE) => None,
            _ => panic!("docker cp {source}: {}", copied.stderr.trim()),
        };

        _ = fs::remove_file(&copy);
        metadata
    }

    /// Writes what the container printed to `log`.
    fn save_logs(&self, log: &Path) -> Result<(), String> {
        self.write_logs(log, Duration::from_secs(60)).map(drop)
    }

    /// Writes what the container printed to `log`, and fails when it printed nothing: a broker
    /// that exited after a failed start always printed its error. `docker logs` gets `timeout`.
    fn save_logs_expecting_output(&self, log: &Path, timeout: Duration) -> Result<(), String> {
        if self.write_logs(log, timeout)? {
            Ok(())
        } else {
            Err(format!("docker logs {} printed nothing", self.name))
        }
    }

    /// Writes what the container printed to `log`, and gives `docker logs` `timeout` to finish.
    /// Returns whether the container printed anything.
    fn write_logs(&self, log: &Path, timeout: Duration) -> Result<bool, String> {
        let logs = docker::execute_within(&["logs", &self.name], timeout)?;

        if logs.code != Some(0) {
            return Err(format!("docker logs {}: {}", self.name, logs.stderr.trim()));
        }

        let printed = logs.stdout + logs.stderr.as_str();

        fs::write(log, &printed)
            .map_err(|err| format!("couldn't write {}: {err}", log.display()))?;

        Ok(!printed.is_empty())
    }
}

/// Calls `fetch` up to `attempts` times, `delay` apart, until it succeeds, and stops asking once
/// `window` has passed. Each call gets the time left in the window as its timeout.
///
/// Returns the first and the last failure, with the number of attempts, if none succeeds. When a
/// call succeeds after a failure, prints the failures to stderr, which nextest keeps for a passing
/// test, so that a recovery leaves a trace.
fn collect_log(
    attempts: u32,
    delay: Duration,
    window: Duration,
    mut fetch: impl FnMut(Duration) -> Result<(), String>,
) -> Result<(), String> {
    let started = Instant::now();
    let mut first = None;
    let mut last = String::new();
    let mut made = 0;

    for attempt in 1..=attempts {
        made = attempt;
        match fetch(time_left(window, started.elapsed())) {
            Ok(()) => {
                if let Some(first) = first {
                    eprintln!(
                        "the broker's log was lost on {} attempt(s) and then collected on attempt \
                         {attempt}: {first}; then {last}",
                        attempt - 1
                    );
                }
                return Ok(());
            }
            Err(reason) => {
                _ = first.get_or_insert_with(|| reason.clone());
                last = reason;
            }
        }

        if started.elapsed() >= window {
            break;
        }

        if attempt < attempts {
            thread::sleep(delay);
        }
    }

    let first = first.unwrap_or_default();

    if first == last {
        Err(format!("{last} after {made} attempt(s)"))
    } else {
        Err(format!("{first}; then {last} after {made} attempt(s)"))
    }
}

/// Returns how long the next ask has: what is left of `window` after `elapsed`, and at least
/// [`LOG_MIN_TIMEOUT`].
fn time_left(window: Duration, elapsed: Duration) -> Duration {
    window.saturating_sub(elapsed).max(LOG_MIN_TIMEOUT)
}

impl fmt::Display for BrokerContainer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "container {}", self.name)
    }
}

/// Removes a container that the harness started, but not a broker's volume, which
/// [`BrokerFiles`] removes.
fn remove_container(name: &str) {
    _ = docker::execute(&["rm", "--force", "--volumes", name]);
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use super::{LOG_MIN_TIMEOUT, collect_log, time_left};

    const WINDOW: Duration = Duration::from_secs(60);

    #[test]
    fn collect_log_asks_again_until_the_log_is_there() {
        let mut asked = 0;

        let collected = collect_log(5, Duration::ZERO, WINDOW, |_| {
            asked += 1;
            if asked < 3 {
                Err("printed nothing".to_owned())
            } else {
                Ok(())
            }
        });

        assert_eq!(collected, Ok(()));
        assert_eq!(asked, 3);
    }

    #[test]
    fn collect_log_names_the_first_and_last_failure_and_the_attempts() {
        let mut asked = 0;

        let collected = collect_log(3, Duration::ZERO, WINDOW, |_| {
            asked += 1;
            Err(format!("failure {asked}"))
        });

        assert_eq!(
            collected,
            Err("failure 1; then failure 3 after 3 attempt(s)".to_owned())
        );
    }

    #[test]
    fn collect_log_names_a_repeated_failure_once() {
        let collected = collect_log(3, Duration::ZERO, WINDOW, |_| {
            Err("printed nothing".to_owned())
        });

        assert_eq!(
            collected,
            Err("printed nothing after 3 attempt(s)".to_owned())
        );
    }

    #[test]
    fn collect_log_stops_asking_when_the_window_has_passed() {
        let mut asked = 0;

        let collected = collect_log(20, Duration::ZERO, Duration::ZERO, |_| {
            asked += 1;
            Err("timed out".to_owned())
        });

        assert_eq!(asked, 1);
        assert_eq!(collected, Err("timed out after 1 attempt(s)".to_owned()));
    }

    #[test]
    fn collect_log_asks_once_when_given_one_attempt() {
        let mut asked = 0;

        _ = collect_log(1, Duration::ZERO, WINDOW, |_| {
            asked += 1;
            Err("printed nothing".to_owned())
        });

        assert_eq!(asked, 1);
    }

    #[test]
    fn collect_log_gives_each_ask_what_is_left_of_the_window() {
        const ASK_DURATION: Duration = Duration::from_millis(20);
        let mut timeouts = vec![];

        _ = collect_log(2, Duration::ZERO, WINDOW, |timeout| {
            timeouts.push(timeout);
            // The first ask takes this long, so the second ask has at most `WINDOW` minus this
            // long. A `collect_log` that passed `WINDOW` itself would give the second ask `WINDOW`.
            thread::sleep(ASK_DURATION);
            Err("timed out".to_owned())
        });

        assert_eq!(timeouts.len(), 2);
        assert!(timeouts[0] > WINDOW - ASK_DURATION, "{timeouts:?}");
        assert!(timeouts[1] <= WINDOW - ASK_DURATION, "{timeouts:?}");
    }

    #[test]
    fn time_left_is_what_remains_of_the_window() {
        assert_eq!(
            time_left(Duration::from_secs(3), Duration::from_secs(1)),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn time_left_is_never_less_than_the_minimum() {
        assert_eq!(
            time_left(Duration::from_secs(3), Duration::from_millis(2900)),
            LOG_MIN_TIMEOUT
        );
        assert_eq!(
            time_left(Duration::from_secs(3), Duration::from_secs(10)),
            LOG_MIN_TIMEOUT
        );
    }
}
