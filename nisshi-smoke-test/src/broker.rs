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

//! Finds, starts and stops the broker that a test talks to.
//!
//! A test either uses the broker that all tests share, at the address in `NISSHI_SMOKE_BOOTSTRAP`,
//! or launches a broker of its own: from the Docker image in `NISSHI_SMOKE_IMAGE` when that is
//! set, otherwise from the `nisshi` binary in `NISSHI_SMOKE_BIN`. A launched broker is checked
//! when it stops: the check fails if the broker exited before then, didn't exit with
//! 0 within 30 seconds of SIGTERM, or wrote `panicked at` to its log.
//!
//! A test can restart a launched broker on the same storage and port. The harness first stops the
//! broker with SIGTERM, or kills it with SIGKILL. A test that expects the broker to refuse its
//! configuration gets a [`FailedStart`] instead of a broker.
//!
//! Each launched broker keeps its files, and a log per start, in a directory of its own under
//! `NISSHI_SMOKE_WORK_DIR`. The directory is removed when the broker passes its checks and the
//! test hasn't failed.

mod deployment;
mod failed_start;
mod files;
mod host;
mod stop_failure;

use std::{
    path::{Path, PathBuf},
    thread,
    time::{Duration, SystemTime},
};

pub use failed_start::FailedStart;
pub use stop_failure::StopFailure;

use self::{deployment::Deployment, files::BrokerFiles, host::Shutdown};
use crate::{
    KafkaCli, ReservedPort, ScramLogin, StorageUrl, TlsCertificate, common::unique, settings,
};

/// The storage URL option that makes a broker run maintenance every 2 seconds instead of every 10
/// minutes, for a test that waits for a maintenance run.
pub const FREQUENT_MAINTENANCE: &str = "maintenance_interval=2s";

/// A broker the tests talk to: the shared one `just smoke` started, or one
/// of their own from [`Broker::isolated`].
#[derive(Debug)]
pub struct Broker {
    /// The broker's bootstrap address; see [`Broker::bootstrap`].
    bootstrap: String,
    /// The broker that this `Broker` launched, and so stops and checks. It is
    /// `None` for the shared broker, which `smoke-broker` stops.
    deployment: Option<Deployment>,
}

/// How to launch a broker.
#[derive(Debug)]
pub struct LaunchOptions {
    /// The port the broker listens on and advertises, on 127.0.0.1. The broker keeps the port
    /// when it restarts.
    pub port: ReservedPort,
    pub cluster_id: String,
    /// The user every client logs in as. When it is set, the broker runs with `--authentication`,
    /// and the harness's own tools log in as this user.
    pub login: Option<ScramLogin>,
    /// Where to keep the broker's log. Otherwise the log goes in the broker's temporary
    /// directory, which is removed when the broker passes its checks.
    pub log: Option<PathBuf>,
    /// The storage engine URL, usually [`settings::storage_url_under_test`].
    pub storage: StorageUrl,
    /// The certificate the broker serves TLS with. When it is set, the broker accepts only TLS
    /// connections, and the harness's own Kafka CLI tools connect with TLS and trust this
    /// certificate.
    pub tls: Option<TlsCertificate>,
}

impl LaunchOptions {
    /// Launch options for a broker on `storage`. The broker gets a cluster id of its own, so its
    /// data stays apart from other brokers' data on a shared storage engine.
    #[track_caller]
    pub fn new(storage: StorageUrl) -> Self {
        Self {
            port: ReservedPort::reserve(),
            cluster_id: unique::name("cluster"),
            login: None,
            log: None,
            storage,
            tls: None,
        }
    }

    /// Returns the broker's command-line arguments. A TLS broker reads its certificate and private
    /// key from `tls_directory`, where the harness puts them before the broker starts.
    fn broker_args(&self, tls_directory: &Path) -> Vec<String> {
        let url = format!("tcp://127.0.0.1:{}", self.port.number());

        let mut args = vec![
            "broker".to_owned(),
            format!("--cluster-id={}", self.cluster_id),
            format!("--listener-url={url}"),
            format!("--advertised-listener-url={url}"),
            format!("--storage-engine={}", self.storage),
        ];

        if self.login.is_some() {
            args.push("--authentication".to_owned());
        }

        if self.tls.is_some() {
            let certificate = tls_directory.join(files::TLS_CERTIFICATE_FILE);
            let private_key = tls_directory.join(files::TLS_PRIVATE_KEY_FILE);
            args.push(format!("--cert={}", certificate.display()));
            args.push(format!("--key={}", private_key.display()));
        }

        args
    }
}

impl Broker {
    /// A broker of this test's own, for a test that needs its own configuration or breaks its
    /// broker. It runs on [`settings::storage_url_under_test`].
    #[track_caller]
    pub fn isolated() -> Self {
        Self::launch(LaunchOptions::new(settings::storage_url_under_test()))
    }

    /// Launches a broker of this test's own, as [`Broker::isolated`] does, that runs maintenance
    /// every 2 seconds instead of every 10 minutes, for a test that waits for a maintenance run.
    #[track_caller]
    pub fn isolated_with_frequent_maintenance() -> Self {
        Self::launch(LaunchOptions::new(
            settings::storage_url_under_test().with_query_option(FREQUENT_MAINTENANCE),
        ))
    }

    /// Launches `NISSHI_SMOKE_IMAGE` when set, otherwise `NISSHI_SMOKE_BIN`, and waits until it
    /// answers. Panics if it doesn't start, or if something else already listens on the port.
    #[track_caller]
    pub fn launch(options: LaunchOptions) -> Self {
        let cluster_id = options.cluster_id.clone();

        match Self::try_launch(options) {
            Ok(broker) => broker,
            Err(failed_start) => {
                panic!("the broker with cluster id {cluster_id} didn't start: {failed_start}")
            }
        }
    }

    /// Like [`Broker::launch`], but returns a [`FailedStart`] when the broker exits or doesn't
    /// answer, for a test that expects the broker to refuse its configuration. Panics, as `launch`
    /// does, when the binary or container can't start at all.
    #[track_caller]
    fn try_launch(options: LaunchOptions) -> Result<Self, FailedStart> {
        options.port.assert_free();

        let files = BrokerFiles::new(&options.storage);
        Self::start_with_files(options, files)
    }

    /// Launches a broker that must refuse its configuration, and returns how it exited. Panics if
    /// the broker starts, exits with 0, or panics, because a broker that panics gives the user
    /// only a backtrace.
    #[track_caller]
    pub fn launch_expecting_refusal(options: LaunchOptions) -> FailedStart {
        let refused = match Self::try_launch(options) {
            Ok(broker) => panic!("the broker started at {}", broker.bootstrap()),
            Err(refused) => refused,
        };

        assert!(
            refused.exited_with_error(),
            "the broker didn't exit with an error: {refused}"
        );

        refused
    }

    /// Stops the broker as [`Broker::stop`] does, and starts it again on the same storage and
    /// port. Panics if it fails its checks or doesn't start again.
    #[track_caller]
    pub fn restart(self) -> Self {
        self.restart_with(Shutdown::Graceful, |_| {}, &[] as &[&str])
    }

    /// Kills the broker with SIGKILL, as a crash would, and restarts it on the same storage and
    /// port. Panics if the broker had already exited, logged a panic, or doesn't start again.
    #[track_caller]
    pub fn kill_and_restart(self) -> Self {
        self.restart_with(Shutdown::Kill, |_| {}, &[] as &[&str])
    }

    /// Like [`Broker::restart`], but the broker starts again with `--authentication`, so every
    /// client must log in. The harness's readiness check logs in as `login`, so the broker starts
    /// again only if it still has that user's credentials.
    #[track_caller]
    pub fn restart_requiring_login(self, login: &ScramLogin) -> Self {
        self.restart_with(
            Shutdown::Graceful,
            |options| options.login = Some(login.clone()),
            &[] as &[&str],
        )
    }

    /// Like [`Broker::restart`], but the broker starts again on `storage`, with the same cluster
    /// id and working directory. A relative SQLite path in `storage` therefore names a file next
    /// to the broker's own database.
    ///
    /// Each of `files_to_delete`, a path relative to the working directory, is deleted while the
    /// broker is stopped, so a test can make sure that the broker can't read them after the
    /// restart. A file that doesn't exist is skipped.
    #[track_caller]
    pub fn restart_on_storage(
        self,
        storage: StorageUrl,
        files_to_delete: &[impl AsRef<str>],
    ) -> Self {
        self.restart_with(
            Shutdown::Graceful,
            |options| options.storage = storage,
            files_to_delete,
        )
    }

    #[track_caller]
    fn restart_with(
        mut self,
        shutdown: Shutdown,
        change_options: impl FnOnce(&mut LaunchOptions),
        files_to_delete: &[impl AsRef<str>],
    ) -> Self {
        let Some(deployment) = self.deployment.take() else {
            panic!("only a broker this test launched can restart");
        };

        let (stopped, mut files, mut options) = deployment.stop(shutdown);

        if let Err(reason) = stopped {
            panic!("{reason}");
        }

        files.delete(files_to_delete);
        change_options(&mut options);
        files.create_volume_for(&options.storage);
        let cluster_id = options.cluster_id.clone();

        match Self::start_with_files(options, files) {
            Ok(broker) => broker,
            Err(failed_start) => {
                panic!("the broker with cluster id {cluster_id} didn't start again: {failed_start}")
            }
        }
    }

    #[track_caller]
    fn start_with_files(options: LaunchOptions, files: BrokerFiles) -> Result<Self, FailedStart> {
        let bootstrap = format!("127.0.0.1:{}", options.port.number());

        // The harness finds the tools' container before it launches the broker, so a missing
        // `NISSHI_SMOKE_KAFKA` panics while no broker is running.
        let cli = match &options.login {
            Some(login) => KafkaCli::logged_in_as(&bootstrap, login),
            None => KafkaCli::new(&bootstrap),
        };
        let cli = match &options.tls {
            Some(certificate) => cli.over_tls(certificate),
            None => cli,
        };

        /// How long a launched broker has to answer after it starts.
        const READY_TIMEOUT: Duration = Duration::from_secs(60);

        let mut deployment = Deployment::launch(options, files);

        let answered = match cli.wait_until_ready(READY_TIMEOUT, || deployment.check_running()) {
            Ok(answered) => answered,
            Err(reason) => return Err(deployment.stop_and_collect_failed_start(reason)),
        };

        let expected = deployment.options().cluster_id.clone();

        let broker = Self {
            bootstrap,
            deployment: Some(deployment),
        };

        // The harness also checks the cluster id in the answer. Another test's broker can bind
        // this port after `assert_port_is_free` and before this broker does, and any broker on the
        // port answers the readiness check.
        assert_eq!(
            answered.cluster_id(),
            Some(expected.as_str()),
            "the broker at {} is not the one launched with cluster id {expected}: {answered}",
            broker.bootstrap,
        );

        Ok(broker)
    }

    /// Returns the broker's bootstrap address, `host:port`, which [`KafkaCli::new`] takes.
    ///
    /// A Kafka client connects to its bootstrap address first, and asks that broker for the
    /// addresses of the cluster's brokers. It then connects to the address each broker
    /// advertises. Here each cluster has one broker, which listens on and advertises the
    /// same `127.0.0.1` address, so the bootstrap address is also the broker's address.
    pub fn bootstrap(&self) -> &str {
        &self.bootstrap
    }

    /// Returns whether the file at `path` exists where the broker runs: on this machine for a
    /// broker process, and in its container for a broker container. A relative `path` starts at
    /// the broker's working directory.
    ///
    /// Panics for the shared broker. `smoke-broker` launches that broker, so this `Broker` doesn't
    /// know where the broker runs.
    #[track_caller]
    pub fn file_exists(&self, path: &str) -> bool {
        self.launched_deployment().file_metadata(path).is_some()
    }

    /// Returns when the file at `path`, where the broker runs, was last written, or `None` if the
    /// file doesn't exist. A relative `path` starts at the broker's working directory.
    ///
    /// Panics for the shared broker. `smoke-broker` launches that broker, so this `Broker` doesn't
    /// know where the broker runs.
    #[track_caller]
    pub fn file_modification_time(&self, path: &str) -> Option<SystemTime> {
        self.launched_deployment()
            .file_metadata(path)?
            .modified()
            .ok()
    }

    /// Returns the broker that this `Broker` launched. Panics for the shared broker.
    #[track_caller]
    fn launched_deployment(&self) -> &Deployment {
        match &self.deployment {
            Some(deployment) => deployment,
            None => panic!("only a broker this test launched has files the test can check"),
        }
    }

    /// Stops a broker this test launched, failing if it exited before now, didn't exit with 0 on
    /// SIGTERM, or logged a panic. Dropping the broker does the same.
    pub fn stop(mut self) -> Result<(), StopFailure> {
        self.shutdown()
    }

    fn shutdown(&mut self) -> Result<(), StopFailure> {
        let Some(deployment) = self.deployment.take() else {
            return Ok(());
        };

        let (stopped, mut files, _) = deployment.stop(Shutdown::Graceful);

        if stopped.is_err() {
            files.keep_directory();
        }

        stopped
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let result = self.shutdown();

        // A second panic while the test is already failing would abort the
        // whole test binary and hide the first one, so the reason is printed instead.
        if let Err(reason) = result {
            if thread::panicking() {
                eprintln!("{reason}");
            } else {
                panic!("{reason}");
            }
        }
    }
}
