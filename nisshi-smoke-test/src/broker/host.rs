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

mod container;
mod process;

use std::{fmt, fs, path::Path, time::Duration};

use self::{container::BrokerContainer, process::BrokerProcess};
use super::{LaunchOptions, files::BrokerFiles};
use crate::settings;

/// How long a broker has to exit after SIGTERM.
const STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// How the harness stops a broker.
#[derive(Clone, Copy, Debug)]
pub(super) enum Shutdown {
    /// SIGTERM, after which the broker must exit with 0 within [`STOP_TIMEOUT`].
    Graceful,
    /// SIGKILL. The broker stops at once, as in a crash, and can't finish a write in progress.
    Kill,
}

/// What a launched broker runs in.
#[derive(Debug)]
pub(super) enum Host {
    Process(BrokerProcess),
    Container(BrokerContainer),
}

impl Host {
    /// Launches `NISSHI_SMOKE_IMAGE` in a container when it is set, otherwise `NISSHI_SMOKE_BIN` as
    /// a process. Returns an error if the container or the process can't start.
    #[track_caller]
    pub(super) fn launch(
        options: &LaunchOptions,
        files: &BrokerFiles,
        log: &Path,
    ) -> Result<Self, String> {
        if let Some(certificate) = &options.tls {
            files.write_tls_certificate(certificate);
        }

        match settings::broker_image() {
            Some(image) => BrokerContainer::launch(&image, options, files).map(Self::Container),
            None => BrokerProcess::launch(options, files.dir(), log).map(Self::Process),
        }
    }

    /// An error if the broker has exited.
    pub(super) fn check_running(&mut self) -> Result<(), String> {
        match self {
            Self::Process(process) => process.check_running(),
            Self::Container(container) => container.check_running(),
        }
    }

    /// Stops the broker, and adds each check that it fails to `failures`. Saves a container's
    /// output to `log` before it removes the container.
    pub(super) fn stop(self, shutdown: Shutdown, log: &Path, failures: &mut Vec<String>) {
        match self {
            Self::Process(process) => process.stop(shutdown, failures),
            Self::Container(container) => container.stop(shutdown, log, failures),
        }
    }

    /// Stops a broker that didn't start, if it is still running, and returns its exit code, or
    /// `None` if the harness killed it, and why a container's log is missing or empty, if it is.
    /// Saves a container's output to `log` before it removes the container.
    pub(super) fn stop_after_failed_start(&mut self, log: &Path) -> (Option<i32>, Option<String>) {
        match self {
            Self::Process(process) => (process.stop_after_failed_start(), None),
            Self::Container(container) => container.stop_after_failed_start(log),
        }
    }

    /// Returns the directory where the broker reads its TLS certificate and private key.
    pub(super) fn tls_directory(&self) -> &'static Path {
        match self {
            Self::Process(_) => BrokerProcess::tls_directory(),
            Self::Container(_) => BrokerContainer::tls_directory(),
        }
    }

    /// Returns the metadata of the file at `path` where the broker runs, or `None` if the file
    /// doesn't exist. A relative `path` starts at the broker's working directory.
    ///
    /// Panics if the harness can't read the file for another reason, so that a test never takes
    /// such a failure for a missing file.
    #[track_caller]
    pub(super) fn file_metadata(&self, path: &str) -> Option<fs::Metadata> {
        match self {
            Self::Process(process) => process.file_metadata(path),
            Self::Container(container) => container.file_metadata(path),
        }
    }
}

impl fmt::Display for Host {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Process(process) => process.fmt(formatter),
            Self::Container(container) => container.fmt(formatter),
        }
    }
}

/// Returns the environment variables a broker is launched with: `PATH`,
/// `HOME`, the log settings, and every `AWS_` variable, which the S3 engine
/// reads its endpoint and credentials from.
///
/// The broker reads much of its configuration from environment variables,
/// and `just` loads a developer's `.env` into the environment. The broker gets
/// only these variables, so a local `.env` can't make it differ from the CI tests.
fn broker_environment() -> impl Iterator<Item = (String, String)> {
    std::env::vars().filter(|(name, _)| {
        name.starts_with("AWS_")
            || ["HOME", "PATH", "RUST_BACKTRACE", "RUST_LOG"].contains(&name.as_str())
    })
}
