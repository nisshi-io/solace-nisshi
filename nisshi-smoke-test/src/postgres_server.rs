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

//! A PostgreSQL server that one test owns, so that the test can stop or pause it and start it
//! again.
//!
//! Every other test and the shared broker use the server that `just smoke postgres` starts. A test
//! must never stop that server.
//!
//! An outage test uses a server of its own, instead of a database on the shared server that it
//! closes to connections. Only a server of its own can stop answering on open connections. A
//! mistake in a test's outage then also can't reach the shared broker.

use std::time::Duration;

use crate::{
    ReservedPort, StorageUrl,
    common::{docker, polling, unique},
    settings,
};

/// How long a server has to accept connections after it starts.
const READY_TIMEOUT: Duration = Duration::from_secs(60);

/// A PostgreSQL server in a container, with an empty database. The broker creates its tables there
/// at startup. Dropping the server removes the container and its data.
#[derive(Debug)]
pub struct PostgresServer {
    container: String,
    /// The port on 127.0.0.1 where the server accepts connections. The server keeps the port
    /// while it is stopped.
    port: ReservedPort,
}

impl PostgresServer {
    /// Starts a server from the image in `NISSHI_SMOKE_POSTGRES_IMAGE`, and waits until it accepts
    /// connections. Panics if it doesn't within a minute.
    #[track_caller]
    pub fn start() -> Self {
        let server = Self {
            container: unique::name("nisshi-smoke-postgres"),
            port: ReservedPort::reserve(),
        };

        let started = docker::execute(&[
            "run",
            "--detach",
            "--name",
            &server.container,
            &docker::label(),
            "--publish",
            &format!("127.0.0.1:{}:5432", server.port.number()),
            "--env",
            "POSTGRES_PASSWORD=postgres",
            "--env",
            "PGUSER=postgres",
            &settings::postgres_image(),
        ]);

        assert!(
            matches!(&started, Ok(started) if started.code == Some(0)),
            "couldn't start {}: {started:?}",
            server.container
        );

        server.wait_until_ready();
        server
    }

    /// Returns the storage URL of a broker that keeps its data on this server. The URL holds the
    /// password of the `postgres` user.
    pub fn storage_url(&self) -> StorageUrl {
        StorageUrl::as_given(format!("postgres://postgres:postgres@{}", self.address()))
    }

    /// Returns the host and port where the server accepts connections.
    pub fn address(&self) -> String {
        format!("127.0.0.1:{}", self.port.number())
    }

    /// Stops the server with `docker stop`. The server keeps its data and port for
    /// [`PostgresServer::start_again`].
    #[track_caller]
    pub fn stop(&self) {
        self.docker_or_panic(&["stop", &self.container]);
    }

    /// Starts the server again after [`PostgresServer::stop`], and waits until it accepts
    /// connections.
    #[track_caller]
    pub fn start_again(&self) {
        self.docker_or_panic(&["start", &self.container]);
        self.wait_until_ready();
    }

    /// Freezes the server's processes with `docker pause`. The server keeps its connections open
    /// and stops answering on them, as a server behind a network partition does.
    #[track_caller]
    pub fn pause(&self) {
        self.docker_or_panic(&["pause", &self.container]);
    }

    /// Lets the server run again after [`PostgresServer::pause`]. The server then answers on the
    /// connections it kept.
    #[track_caller]
    pub fn unpause(&self) {
        self.docker_or_panic(&["unpause", &self.container]);
    }

    /// Waits until the server accepts connections over TCP. The image's entrypoint first sets up
    /// the database on a temporary server that listens only on its socket. A check over the socket
    /// passes during that time, so this check connects over TCP.
    #[track_caller]
    fn wait_until_ready(&self) {
        polling::wait_until(READY_TIMEOUT, || {
            match docker::execute(&[
                "exec",
                &self.container,
                "pg_isready",
                "--host=localhost",
                "--quiet",
            ])? {
                ready if ready.code == Some(0) => Ok(()),
                not_ready => Err(format!("{} isn't ready: {not_ready:?}", self.container)),
            }
        });
    }

    #[track_caller]
    fn docker_or_panic(&self, args: &[&str]) {
        match docker::execute(args) {
            Ok(finished) if finished.code == Some(0) => {}
            finished => panic!("docker {}: {finished:?}", args.join(" ")),
        }
    }
}

impl Drop for PostgresServer {
    fn drop(&mut self) {
        _ = docker::execute(&["rm", "--force", "--volumes", &self.container]);
    }
}
