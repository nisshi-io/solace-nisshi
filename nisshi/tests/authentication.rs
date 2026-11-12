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

//! The error that `nisshi topic` and `nisshi user` print against a broker started with
//! `--authentication`. Neither command authenticates, so the broker closes the connection at
//! the first request after `ApiVersions`.

#![cfg(feature = "libsql")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::{
    process::Output,
    time::{Duration, Instant},
};

use common::Broker;
use tempfile::TempDir;

/// How long a command may take to fail. The broker closes the connection at once, so the
/// command must fail well before the client's 30 second request timeout. A command that waits
/// for a response instead fails only at that timeout.
const FAILURE_DEADLINE: Duration = Duration::from_secs(10);

/// Starts a broker with `--authentication` on a SQLite file in `directory`, without any users.
fn start_authenticating_broker(directory: &TempDir) -> Broker {
    let storage = format!("sqlite:///{}", directory.path().join("nisshi.db").display());

    Broker::start_on(&storage, &["--authentication"])
}

/// Asserts that `output` is a failure whose error line says that the broker closed the
/// connection and names `--authentication`.
fn assert_names_authentication(output: &Output) {
    assert!(!output.status.success(), "{output:?}");

    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        stderr.lines().any(|line| {
            line.starts_with("error: Connected to the broker, but it closed the connection")
                && line.contains("--authentication")
        }),
        "{output:?}"
    );
}

#[test]
fn topic_list_without_credentials_names_authentication() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let broker = start_authenticating_broker(&directory);

    let started = Instant::now();
    let output = broker.run(&["topic", "list"], "");

    assert!(started.elapsed() < FAILURE_DEADLINE, "{output:?}");
    assert_names_authentication(&output);
}

#[test]
fn user_create_without_credentials_names_authentication() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let broker = start_authenticating_broker(&directory);

    let started = Instant::now();
    let output = broker.run(
        &[
            "user",
            "create",
            "alice",
            "secret",
            "--mechanism",
            "scram256",
        ],
        "",
    );

    assert!(started.elapsed() < FAILURE_DEADLINE, "{output:?}");
    assert_names_authentication(&output);
}
