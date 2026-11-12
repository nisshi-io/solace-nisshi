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

//! `--output json` of the client tools, against the `nisshi` binary: stdout is JSON, the broker's
//! per-item results as it sent them.

#![cfg(feature = "dynostore")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::Broker;
use serde_json::Value;
use std::process::Output;

/// Parses the stdout of a command as JSON, whatever its exit status.
fn json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("stdout is not JSON ({error}): {output:?}"))
}

#[test]
fn topic_create_prints_the_brokers_result() {
    let broker = Broker::start();

    let created = broker.run(&["topic", "create", "taxi", "--output", "json"], "");
    assert_eq!(Some(0), created.status.code(), "{created:?}");

    let created = json(&created);
    assert_eq!("taxi", created[0]["name"], "{created}");
    assert_eq!(0, created[0]["error_code"], "{created}");
    assert_eq!(1, created.as_array().unwrap().len());

    // The result is printed when the broker refuses too, with its error code.
    let duplicate = broker.run(&["topic", "create", "taxi", "--output", "json"], "");
    assert_eq!(Some(4), duplicate.status.code(), "{duplicate:?}");
    assert_ne!(0, json(&duplicate)[0]["error_code"]);
}

#[test]
fn topic_create_prints_the_json_even_when_silent() {
    let broker = Broker::start();

    let created = broker.run(
        &["topic", "create", "taxi", "--silent", "--output", "json"],
        "",
    );

    assert_eq!("taxi", json(&created)[0]["name"], "{created:?}");
}

#[test]
fn topic_delete_prints_the_brokers_result() {
    let broker = Broker::start();
    _ = broker.run(&["topic", "create", "taxi"], "");

    let deleted = broker.run(&["topic", "delete", "taxi", "--output", "json"], "");
    assert_eq!(Some(0), deleted.status.code(), "{deleted:?}");
    assert_eq!(0, json(&deleted)[0]["error_code"], "{deleted:?}");

    let missing = broker.run(&["topic", "delete", "taxi", "--output", "json"], "");
    assert_eq!(Some(4), missing.status.code(), "{missing:?}");
    assert_ne!(0, json(&missing)[0]["error_code"]);

    // Without --output, delete prints nothing, as before.
    _ = broker.run(&["topic", "create", "taxi"], "");
    let quiet = broker.run(&["topic", "delete", "taxi"], "");
    assert!(quiet.stdout.is_empty(), "{quiet:?}");
}

#[test]
fn user_create_prints_the_brokers_results() {
    // `memory://` stores no SCRAM credentials, so it refuses every user with
    // `UnsupportedVersion`.
    let broker = Broker::start();

    let output = broker.run(
        &[
            "user",
            "create",
            "alice",
            "secret",
            "--mechanism",
            "scram256",
            "--output",
            "json",
        ],
        "",
    );

    assert_eq!(Some(4), output.status.code(), "{output:?}");

    let results = json(&output);
    assert_eq!("alice", results[0]["user"], "{results}");
    assert_ne!(0, results[0]["error_code"], "{results}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("error: user alice: "),
        "{output:?}"
    );
}

#[test]
fn user_delete_prints_the_brokers_results() {
    let broker = Broker::start();

    let output = broker.run(&["user", "delete", "alice", "--output", "json"], "");

    assert_eq!("alice", json(&output)[0]["user"], "{output:?}");
}

#[test]
fn cat_produce_prints_the_brokers_partition_result() {
    let broker = Broker::start();
    _ = broker.run(&["topic", "create", "taxi"], "");

    let produced = broker.run(
        &["cat", "produce", "taxi", "--output", "json"],
        "{\"value\": \"hello\"}\n{\"value\": \"world\"}\n",
    );
    assert_eq!(Some(0), produced.status.code(), "{produced:?}");

    let produced = json(&produced);
    assert_eq!(0, produced[0]["index"], "{produced}");
    assert_eq!(0, produced[0]["base_offset"], "{produced}");
    assert_eq!(0, produced[0]["error_code"], "{produced}");

    // A refused produce prints the result too, and exits 4.
    let refused = broker.run(
        &[
            "cat",
            "produce",
            "taxi",
            "--partition",
            "7",
            "--output",
            "json",
        ],
        "{\"value\": \"hello\"}\n",
    );
    assert_eq!(Some(4), refused.status.code(), "{refused:?}");
    assert_ne!(0, json(&refused)[0]["error_code"]);
}

#[test]
fn cat_consume_accepts_json_and_prints_a_json_array_on_each_line() {
    let broker = Broker::start();
    _ = broker.run(&["topic", "create", "taxi"], "");
    _ = broker.run(&["cat", "produce", "taxi"], "{\"value\": \"hello\"}\n");

    let consumed = broker.run(&["cat", "consume", "taxi", "--output", "json"], "");
    assert_eq!(Some(0), consumed.status.code(), "{consumed:?}");

    let stdout = String::from_utf8_lossy(&consumed.stdout);
    assert!(stdout.lines().count() >= 1, "{consumed:?}");
    for line in stdout.lines() {
        let batch: Value = serde_json::from_str(line).expect(line);
        assert_eq!("hello", batch[0]["value"], "{line}");
    }
}
