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

    /// Starts `nisshi` with `args` and `--broker` set to this broker.
    fn spawn(&self, args: &[&str]) -> Child {
        nisshi()
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
        let mut process = self.spawn(args);
        write_stdin(&mut process, stdin);
        finish(process)
    }

    /// Runs `nisshi` as [`Broker::run`] does, with the read end of its stdout closed before it
    /// starts, as `nisshi ... | true` does.
    pub(crate) fn run_with_closed_stdout(&self, args: &[&str], stdin: &str) -> Output {
        let mut process = self.spawn(args);
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

/// Waits for `process` to exit and returns its output. Kills it after [`COMMAND_TIMEOUT`].
fn finish(mut process: Child) -> Output {
    // Threads drain stdout and stderr while the process runs, so that a full pipe can't stop it.
    let stdout = process.stdout.take().map(read_to_end);
    let stderr = process.stderr.take().map(read_to_end);
    let started = Instant::now();

    loop {
        if let Some(status) = process.try_wait().expect("command status") {
            return Output {
                status,
                stdout: joined(stdout),
                stderr: joined(stderr),
            };
        }

        if started.elapsed() > COMMAND_TIMEOUT {
            _ = process.kill();
            _ = process.wait();

            panic!(
                "command did not exit within {COMMAND_TIMEOUT:?}, stdout: {}, stderr: {}",
                String::from_utf8_lossy(&joined(stdout)),
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
