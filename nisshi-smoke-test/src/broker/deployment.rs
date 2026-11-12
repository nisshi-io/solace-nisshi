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

use std::{fs, path::PathBuf};

use super::{
    FailedStart, LaunchOptions, StopFailure,
    files::BrokerFiles,
    host::{Host, Shutdown},
};

/// A broker that this harness launched.
#[derive(Debug)]
pub(super) struct Deployment {
    host: Host,
    files: BrokerFiles,
    log: PathBuf,
    options: LaunchOptions,
}

impl Deployment {
    /// Launches a broker with `options`, in `files`. The broker logs to `options.log` if it is
    /// set, otherwise to a new log in `files`. Panics if the broker's process or container can't
    /// start.
    #[track_caller]
    pub(super) fn launch(options: LaunchOptions, mut files: BrokerFiles) -> Self {
        let log = options.log.clone().unwrap_or_else(|| files.next_log());

        let host = match Host::launch(&options, &files, &log) {
            Ok(host) => host,
            Err(reason) => panic!("{reason}"),
        };

        // nextest shows a test's stderr only when the test fails, or when nextest kills it for
        // running too long. That kill skips `Drop`, so this line is the only place that names the
        // test's broker.
        eprintln!(
            "launched the broker with cluster id {} on port {} in {host}, logging to {}",
            options.cluster_id,
            options.port.number(),
            log.display()
        );

        Self {
            host,
            files,
            log,
            options,
        }
    }

    pub(super) fn options(&self) -> &LaunchOptions {
        &self.options
    }

    /// An error if the broker has exited.
    pub(super) fn check_running(&mut self) -> Result<(), String> {
        self.host.check_running()
    }

    /// Returns the metadata of the file at `path` where the broker runs, or `None` if the file
    /// doesn't exist. A relative `path` starts at the broker's working directory.
    ///
    /// Panics if the harness can't read the file for another reason, so that a test never takes
    /// such a failure for a missing file.
    #[track_caller]
    pub(super) fn file_metadata(&self, path: &str) -> Option<fs::Metadata> {
        self.host.file_metadata(path)
    }

    /// Stops the broker and checks it. Returns the files and options too, for a restart.
    pub(super) fn stop(
        self,
        shutdown: Shutdown,
    ) -> (Result<(), StopFailure>, BrokerFiles, LaunchOptions) {
        let mut failures = Vec::new();
        self.host.stop(shutdown, &self.log, &mut failures);

        (
            StopFailure::check(failures, &self.log),
            self.files,
            self.options,
        )
    }

    /// Stops a broker that didn't start, if it is still running, and removes it and its files. The
    /// returned [`FailedStart`] holds its whole log, so nothing is lost.
    pub(super) fn stop_and_collect_failed_start(mut self, reason: String) -> FailedStart {
        let (exit_code, log_problem) = self.host.stop_after_failed_start(&self.log);

        FailedStart::from_log(
            format!(
                "nisshi {}",
                self.options
                    .broker_args(self.host.tls_directory())
                    .join(" ")
            ),
            exit_code,
            &self.log,
            log_problem,
            reason,
        )
    }
}
