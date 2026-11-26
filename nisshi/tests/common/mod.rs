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

//! A `nisshi broker` run from the built binary, and the helpers that run `nisshi` commands
//! against it.

// Each test binary includes this module and uses only some of its items.
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    process::{Child, Command, Output, Stdio},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a command may run before the test kills it. A test that waits without a limit hangs
/// a local run, and CI's nextest timeout kills the test with SIGKILL, which skips [`Broker`]'s
/// `Drop` and leaves the broker running.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(60);

/// A `nisshi broker`, killed when it is dropped.
pub(crate) struct Broker {
    process: Child,
    url: String,
}

impl Broker {
    /// Starts a broker on `memory://`.
    pub(crate) fn start() -> Self {
        Self::start_on("memory://nisshi/", &[])
    }

    /// Starts a broker on the `storage` engine, with `broker_args` added to its command line.
    pub(crate) fn start_on(storage: &str, broker_args: &[&str]) -> Self {
        Self::launch(storage, broker_args, true)
    }

    /// Starts a broker on `memory://` without `--silent`, so that it prints its banner.
    pub(crate) fn start_with_banner() -> Self {
        Self::launch("memory://nisshi/", &[], false)
    }

    fn launch(storage: &str, broker_args: &[&str], silent: bool) -> Self {
        let port = TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .map(|address| address.port())
            .expect("free port");

        let url = format!("tcp://127.0.0.1:{port}");

        // The broker owns the process from here, so a failed start below kills it on the way out.
        let mut broker = Self {
            url: url.clone(),
            process: nisshi()
                .args([
                    "broker",
                    "--listener-url",
                    &url,
                    "--advertised-listener-url",
                    &url,
                    "--storage-engine",
                    storage,
                ])
                .args(silent.then_some("--silent"))
                .args(broker_args)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("broker starts"),
        };

        let started = Instant::now();

        while TcpStream::connect(("127.0.0.1", port)).is_err() {
            if broker.process.try_wait().expect("broker status").is_some()
                || started.elapsed() > STARTUP_TIMEOUT
            {
                let output = broker.stop();
                panic!("broker did not listen on {url} within {STARTUP_TIMEOUT:?}: {output}");
            }

            thread::sleep(Duration::from_millis(50));
        }

        // Another process can take the port between the probe above and the broker's bind. The
        // broker then exits, and the connect succeeded against the other process.
        thread::sleep(Duration::from_millis(50));

        if broker.process.try_wait().expect("broker status").is_some() {
            panic!("broker exited after it started: {}", broker.stop());
        }

        broker
    }

    /// Stops the broker and returns how it exited and what it wrote, for a failure message.
    fn stop(&mut self) -> String {
        let output = self.kill();

        format!(
            "{:?}, stdout: {}, stderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    }

    /// Kills the broker and returns what it wrote.
    pub(crate) fn kill(&mut self) -> Output {
        _ = self.process.kill();
        let status = self.process.wait().expect("broker status");
        let stdout = self.process.stdout.take().map(read_to_end);
        let stderr = self.process.stderr.take().map(read_to_end);

        Output {
            status,
            stdout: joined(stdout),
            stderr: joined(stderr),
        }
    }

    /// Starts `nisshi` with `args` and `--broker` set to this broker, with `env` as its environment.
    fn spawn(&self, args: &[&str], env: &[(&str, &str)]) -> Child {
        nisshi()
            .envs(env.iter().copied())
            .args(args)
            .args(["--broker", &self.url])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("command starts")
    }

    /// Runs `nisshi` with `args` and `--broker` set to this broker, with `stdin` as its input.
    pub(crate) fn run(&self, args: &[&str], stdin: &str) -> Output {
        self.run_with_env(args, stdin, &[])
    }

    /// Runs `nisshi` as [`Broker::run`] does, with `env` as its environment.
    pub(crate) fn run_with_env(&self, args: &[&str], stdin: &str, env: &[(&str, &str)]) -> Output {
        let mut process = self.spawn(args, env);
        write_stdin(&mut process, stdin);
        finish(process)
    }

    /// The URL of the broker, for a command that takes `--broker` before its subcommand, where
    /// [`Broker::run`] would add it too late.
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    /// Runs `nisshi` with `args` and `env`, which name the broker themselves.
    pub(crate) fn run_unaided(&self, args: &[&str], env: &[(&str, &str)]) -> Output {
        finish(self.spawn_unaided(args, env))
    }

    /// Starts `nisshi` with `args` and `env`, which name the broker themselves, and leaves it
    /// running.
    pub(crate) fn spawn_unaided(&self, args: &[&str], env: &[(&str, &str)]) -> Child {
        nisshi()
            .envs(env.iter().copied())
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("command starts")
    }

    /// Waits until `topic` holds a record, by fetching it with `cat consume` until a fetch returns
    /// one. A fetch waits up to 5 s for a record, so the wait ends soon after the broker stores
    /// the first batch.
    pub(crate) fn wait_for_a_record(&self, topic: &str) {
        let started = Instant::now();

        while self.run(&["cat", "consume", topic], "").stdout.is_empty() {
            assert!(
                started.elapsed() < STARTUP_TIMEOUT,
                "no record in {topic} within {STARTUP_TIMEOUT:?}"
            );
        }
    }

    /// Runs `nisshi` as [`Broker::run`] does, with the read end of its stdout closed before it
    /// starts, as `nisshi ... | true` does.
    pub(crate) fn run_with_closed_stdout(&self, args: &[&str], stdin: &str) -> Output {
        let mut process = self.spawn(args, &[]);
        drop(process.stdout.take());
        write_stdin(&mut process, stdin);
        finish(process)
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        _ = self.process.kill();
        _ = self.process.wait();
    }
}

/// A TCP port with nothing listening on it.
pub(crate) fn closed_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|address| address.port())
        .expect("free port")
}

/// Runs `nisshi` with `args` and no input, and returns its output.
pub(crate) fn run(args: &[&str]) -> Output {
    finish(
        nisshi()
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("command starts"),
    )
}

/// Runs `nisshi` as [`run`] does, with `env` as its environment.
pub(crate) fn run_with_env(args: &[&str], env: &[(&str, &str)]) -> Output {
    finish(
        nisshi()
            .envs(env.iter().copied())
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("command starts"),
    )
}

/// The `nisshi` binary, with an empty environment, run from a directory without a `.env`, so
/// that neither the caller's environment nor a developer's `.env` changes the broker or the
/// command output.
fn nisshi() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_nisshi"));
    _ = command.env_clear().current_dir(std::env::temp_dir());
    command
}

/// Writes `stdin` to the process and closes its stdin.
fn write_stdin(process: &mut Child, stdin: &str) {
    process
        .stdin
        .take()
        .expect("stdin")
        .write_all(stdin.as_bytes())
        .expect("stdin written");
}

/// Sends SIGINT to `process`, as Ctrl-C does.
#[cfg(unix)]
pub(crate) fn interrupt(process: &Child) {
    let status = Command::new("kill")
        .args(["-INT", &process.id().to_string()])
        .status()
        .expect("kill runs");
    assert!(status.success(), "kill -INT: {status}");
}

/// Waits for `process` to exit and returns its output. Kills it after [`COMMAND_TIMEOUT`].
pub(crate) fn finish(process: Child) -> Output {
    finish_timed(process).0
}

/// Waits for `process` to exit as [`finish`] does, and also returns how long after this call
/// the first line of stdout was complete, if one was.
pub(crate) fn finish_timed(mut process: Child) -> (Output, Option<Duration>) {
    // Threads drain stdout and stderr while the process runs, so that a full pipe can't stop it.
    let started = Instant::now();
    let stdout = process
        .stdout
        .take()
        .map(|pipe| read_to_end_timed(pipe, started));
    let stderr = process.stderr.take().map(read_to_end);

    loop {
        if let Some(status) = process.try_wait().expect("command status") {
            let (stdout, first_line) = stdout
                .map(|reader| reader.join().expect("pipe reader"))
                .unwrap_or_default();

            return (
                Output {
                    status,
                    stdout,
                    stderr: joined(stderr),
                },
                first_line,
            );
        }

        if started.elapsed() > COMMAND_TIMEOUT {
            _ = process.kill();
            _ = process.wait();

            let stdout = stdout
                .map(|reader| reader.join().expect("pipe reader").0)
                .unwrap_or_default();

            panic!(
                "command did not exit within {COMMAND_TIMEOUT:?}, stdout: {}, stderr: {}",
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&joined(stderr))
            );
        }

        thread::sleep(Duration::from_millis(10));
    }
}

/// Reads `pipe` to its end on a thread of its own.
fn read_to_end(mut pipe: impl Read + Send + 'static) -> JoinHandle<Vec<u8>> {
    thread::spawn(move || {
        let mut bytes = vec![];
        _ = pipe.read_to_end(&mut bytes);
        bytes
    })
}

/// Reads `pipe` to its end on a thread of its own, and notes how long after `started` the first
/// newline arrived.
fn read_to_end_timed(
    mut pipe: impl Read + Send + 'static,
    started: Instant,
) -> JoinHandle<(Vec<u8>, Option<Duration>)> {
    thread::spawn(move || {
        let mut bytes = vec![];
        let mut first_line = None;
        let mut chunk = [0u8; 4096];

        loop {
            match pipe.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    bytes.extend_from_slice(&chunk[..read]);

                    if first_line.is_none() && bytes.contains(&b'\n') {
                        first_line = Some(started.elapsed());
                    }
                }
            }
        }

        (bytes, first_line)
    })
}

fn joined(reader: Option<JoinHandle<Vec<u8>>>) -> Vec<u8> {
    reader
        .map(|reader| reader.join().expect("pipe reader"))
        .unwrap_or_default()
}

/// Returns the stdout of a command that succeeded. Panics if it failed.
pub(crate) fn stdout(output: &Output) -> String {
    assert!(output.status.success(), "{output:?}");
    String::from_utf8(output.stdout.clone()).expect("stdout is UTF-8")
}

/// A schema registry directory with the `taxi` schema in it, which `generator` needs, and the
/// `file://` URL that names it relative to the directory that the command runs in.
pub(crate) fn schema_registry(name: &str) -> (std::path::PathBuf, String) {
    let registry =
        std::env::temp_dir().join(format!("nisshi-exit-codes-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&registry).expect("registry directory");
    _ = std::fs::copy("../etc/schema/taxi.proto", registry.join("taxi.proto")).expect("schema");
    let url = format!(
        "file://./{}",
        registry.file_name().unwrap().to_str().unwrap()
    );

    (registry, url)
}
