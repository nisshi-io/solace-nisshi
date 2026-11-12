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

use std::{
    fmt,
    fs::{self, File},
    io,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

use super::{STOP_TIMEOUT, Shutdown};
use crate::{
    broker::{LaunchOptions, files::TLS_DIRECTORY},
    common::timed_command,
    settings,
};

/// The file in a process broker's directory that holds its process id until the harness has
/// stopped it. nextest kills a test that runs too long with SIGKILL, which skips `Drop` and leaves
/// the broker running; `run.sh` stops every broker whose file is still there when the run ends.
const PID_FILE: &str = "broker.pid";

/// A broker that runs as a process of the `nisshi` binary in `NISSHI_SMOKE_BIN`.
#[derive(Debug)]
pub(in crate::broker) struct BrokerProcess {
    child: Child,
    /// The broker's working directory, which holds its [`PID_FILE`].
    dir: PathBuf,
}

impl BrokerProcess {
    /// Starts the broker in `dir`, and writes its stdout and stderr to `log`. Returns an error if
    /// the binary can't start.
    #[track_caller]
    pub(super) fn launch(options: &LaunchOptions, dir: &Path, log: &Path) -> Result<Self, String> {
        let binary = settings::broker_binary();

        // The broker runs in its own directory, where a relative path would no
        // longer find the binary.
        let binary = match fs::canonicalize(&binary) {
            Ok(binary) => binary,
            Err(err) => panic!("NISSHI_SMOKE_BIN {binary}: {err}"),
        };

        let output = File::create(log).expect("broker log");
        let errors = output.try_clone().expect("broker log");

        let mut command = Command::new(&binary);

        _ = command
            .args(options.broker_args(Self::tls_directory()))
            // The broker resolves a relative sqlite:// path against its working
            // directory, and loads `.env` from it; there is none here.
            .current_dir(dir)
            .env_clear()
            .envs(super::broker_environment())
            .stdin(Stdio::null())
            .stdout(output)
            .stderr(errors);

        let child = command
            .spawn()
            .map_err(|err| format!("could not start {}: {err}", binary.display()))?;

        let process = Self {
            child,
            dir: dir.to_owned(),
        };

        if let Err(err) = fs::write(process.pid_file(), process.child.id().to_string()) {
            panic!("{}: {err}", process.pid_file().display());
        }

        Ok(process)
    }

    /// Returns the directory where the broker reads its TLS certificate and private key:
    /// [`BrokerFiles::tls_directory`], relative to the broker's working directory.
    ///
    /// [`BrokerFiles::tls_directory`]: crate::broker::files::BrokerFiles::tls_directory
    pub(super) fn tls_directory() -> &'static Path {
        Path::new(TLS_DIRECTORY)
    }

    /// An error if the broker has exited.
    pub(super) fn check_running(&mut self) -> Result<(), String> {
        match self.child.try_wait() {
            Ok(Some(status)) => Err(format!("exited with {status}")),
            _ => Ok(()),
        }
    }

    /// Stops the broker with SIGTERM and checks that it exits cleanly in time, or kills it with
    /// SIGKILL and waits for it to exit. Adds each check that it fails to `failures`.
    pub(super) fn stop(mut self, shutdown: Shutdown, failures: &mut Vec<String>) {
        if let Ok(Some(status)) = self.child.try_wait() {
            failures.push(format!("broker exited during the run with {status}"));
        } else {
            match shutdown {
                Shutdown::Graceful => {
                    _ = Command::new("kill")
                        .args(["-TERM", &self.child.id().to_string()])
                        .status();

                    match timed_command::wait(&mut self.child, STOP_TIMEOUT) {
                        Some(status) if status.success() => {}
                        Some(status) => {
                            failures.push(format!("broker exited with {status} on SIGTERM"));
                        }
                        None => failures.push(format!(
                            "broker did not exit within {STOP_TIMEOUT:?} of SIGTERM"
                        )),
                    }
                }

                Shutdown::Kill => {
                    if let Err(err) = self.child.kill() {
                        failures.push(format!("couldn't kill the broker: {err}"));
                    }

                    _ = self.child.wait();
                }
            }
        }

        self.remove_pid_file();
    }

    /// Kills a broker that didn't start, if it is still running, and returns its exit code, or
    /// `None` if the harness killed it.
    pub(super) fn stop_after_failed_start(&mut self) -> Option<i32> {
        if let Ok(None) = self.child.try_wait() {
            _ = self.child.kill();
        }

        let code = self.child.wait().ok().and_then(|status| status.code());
        self.remove_pid_file();
        code
    }

    #[track_caller]
    pub(super) fn file_metadata(&self, path: &str) -> Option<fs::Metadata> {
        match fs::metadata(self.dir.join(path)) {
            Ok(metadata) => Some(metadata),
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            Err(err) => panic!("{path}: {err}"),
        }
    }

    fn pid_file(&self) -> PathBuf {
        self.dir.join(PID_FILE)
    }

    fn remove_pid_file(&self) {
        _ = fs::remove_file(self.pid_file());
    }
}

impl fmt::Display for BrokerProcess {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "process {}", self.child.id())
    }
}
