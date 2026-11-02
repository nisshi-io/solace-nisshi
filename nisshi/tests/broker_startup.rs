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

//! What `nisshi broker` prints on stdout as it starts: its banner once it listens, and nothing
//! when it fails to start.

#![cfg(feature = "dynostore")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::{net::TcpListener, process::Output};

use common::{Broker, run, stdout};

/// Runs `nisshi broker` on `storage`, listening on `listener`, without `--silent`.
fn broker(listener: &str, storage: &str) -> Output {
    run(&[
        "broker",
        "--listener-url",
        listener,
        "--advertised-listener-url",
        listener,
        "--storage-engine",
        storage,
    ])
}

/// Asserts that a broker failed to start, and printed only an error, to stderr.
fn assert_failed_without_banner(output: &Output, error: &str) {
    assert!(!output.status.success(), "{output:?}");
    assert_eq!("", String::from_utf8_lossy(&output.stdout), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(error),
        "{output:?}"
    );
}

#[test]
fn port_in_use_prints_no_banner() {
    let taken = TcpListener::bind("127.0.0.1:0").expect("bind a free port");
    let listener = format!("tcp://{}", taken.local_addr().expect("local address"));

    assert_failed_without_banner(&broker(&listener, "memory://nisshi/"), "is already in use");
}

#[test]
fn storage_without_an_engine_prints_no_banner() {
    let port = TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|address| address.port())
        .expect("free port");

    assert_failed_without_banner(
        &broker(&format!("tcp://127.0.0.1:{port}"), "foo://bar/"),
        "uses the foo:// scheme",
    );
}

/// The banner moved from the CLI into the broker's listener. This checks that the move kept it,
/// in order and before `ready in`; it passes with or without the fix that the tests above cover.
#[test]
fn banner_still_prints_before_ready() {
    let mut broker = Broker::start_with_banner();

    // The broker prints its banner before it accepts a connection, so it has printed it by the
    // time it has answered a request.
    _ = stdout(&broker.run(&["topic", "list"], ""));

    let output = broker.kill();
    let printed = String::from_utf8_lossy(&output.stdout);
    let lines = printed.lines().collect::<Vec<_>>();

    let position = |prefix: &str| {
        lines
            .iter()
            .position(|line| line.starts_with(prefix))
            .unwrap_or_else(|| panic!("no {prefix:?} line in {printed:?}"))
    };

    let headline = position("nisshi broker ");
    let listener = position("listener: ");
    let storage = position("storage: memory://nisshi/");
    let ready = position("ready in ");

    assert!(
        headline < listener && listener < storage && storage < ready,
        "{printed:?}"
    );
}
