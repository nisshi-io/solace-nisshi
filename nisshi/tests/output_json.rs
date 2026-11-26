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

#[cfg(unix)]
use common::interrupt;
use common::{Broker, closed_port, finish, finish_timed, schema_registry};
use serde_json::Value;
use std::process::Output;

/// Parses the stdout of a command as JSON, whatever its exit status.
fn json(output: &Output) -> Value {
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("stdout is not JSON ({error}): {output:?}"))
}

/// Parses each line of the stdout of a command as JSON, whatever its exit status.
fn json_lines(output: &Output) -> Vec<Value> {
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap_or_else(|error| panic!("{line}: {error}")))
        .collect()
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

#[test]
fn perf_prints_a_json_object_for_each_interval_and_a_summary() {
    let broker = Broker::start();
    _ = broker.run(&["topic", "create", "taxi"], "");

    let output = broker.run_unaided(
        &[
            "perf",
            "taxi",
            "--broker",
            broker.url(),
            "produce",
            "--duration",
            "3s",
            "--output",
            "json",
        ],
        // Export every half second, not every minute, so that the run has intervals.
        &[("OTEL_METRIC_EXPORT_INTERVAL", "500")],
    );
    assert_eq!(Some(0), output.status.code(), "{output:?}");

    let lines = json_lines(&output);
    let (summary, intervals) = lines.split_last().expect("a summary");
    assert_eq!("summary", summary["type"], "{output:?}");
    assert!(
        summary["records_produced"].as_u64().unwrap() > 0,
        "{summary}"
    );
    assert!(!intervals.is_empty(), "{output:?}");
    for interval in intervals {
        assert_eq!("interval", interval["type"], "{interval}");
        assert!(interval["records_produced"].is_u64(), "{interval}");
    }

    // The summary covers the whole run, not the last interval: it holds at least what the
    // intervals add up to, and a run of 3 s, not of the 500 ms of an interval.
    let interval_records: u64 = intervals
        .iter()
        .map(|interval| interval["records_produced"].as_u64().unwrap())
        .sum();
    assert!(
        summary["records_produced"].as_u64().unwrap() >= interval_records,
        "{output:?}"
    );
    let elapsed_ms = summary["elapsed_ms"].as_u64().unwrap();
    assert!((2_500..10_000).contains(&elapsed_ms), "{summary}");
}

#[test]
fn perf_prints_a_json_summary_when_the_run_is_shorter_than_an_interval() {
    let broker = Broker::start();
    _ = broker.run(&["topic", "create", "taxi"], "");

    let output = broker.run_unaided(
        &[
            "perf",
            "taxi",
            "--broker",
            broker.url(),
            "produce",
            "--duration",
            "1s",
            "--output",
            "json",
        ],
        // An interval of a minute, whatever the runner sets.
        &[("OTEL_METRIC_EXPORT_INTERVAL", "60000")],
    );

    assert_eq!(Some(0), output.status.code(), "{output:?}");
    let summary = json(&output);
    assert_eq!("summary", summary["type"], "{output:?}");
    // No interval export ran, so only the export at the shutdown of the meter provider can
    // supply the count. A summary read before that export would show 0.
    assert!(
        summary["records_produced"].as_u64().unwrap() > 0,
        "{summary}"
    );
}

#[test]
fn generator_json_survives_a_closed_stdout() {
    let broker = Broker::start();
    _ = broker.run(&["topic", "create", "taxi"], "");
    let (registry, schema_registry) = schema_registry("json-closed");

    let output = broker.run_with_closed_stdout(
        &[
            "generator",
            "taxi",
            "--schema-registry",
            &schema_registry,
            "--duration-seconds",
            "1",
            "--output",
            "json",
        ],
        "",
    );
    _ = std::fs::remove_dir_all(&registry);

    assert_eq!(Some(0), output.status.code(), "{output:?}");
}

#[test]
fn generator_prints_one_json_summary_when_the_run_ends() {
    let broker = Broker::start();
    _ = broker.run(&["topic", "create", "taxi"], "");
    let (registry, schema_registry) = schema_registry("json");

    let output = broker.run(
        &[
            "generator",
            "taxi",
            "--schema-registry",
            &schema_registry,
            "--per-second",
            "10",
            "--duration-seconds",
            "2",
            "--output",
            "json",
        ],
        "",
    );
    _ = std::fs::remove_dir_all(&registry);
    assert_eq!(Some(0), output.status.code(), "{output:?}");

    let summary = json(&output);
    assert_eq!("summary", summary["type"], "{summary}");
    assert!(
        summary["records_produced"].as_u64().unwrap() > 0,
        "{summary}"
    );
    assert_eq!(
        summary["batches_produced"], summary["records_produced"],
        "the default batch holds one record: {summary}"
    );
    assert!(summary["elapsed_ms"].as_u64().unwrap() >= 2000, "{summary}");
}

#[test]
fn a_refused_produce_still_prints_the_json_summary_and_exits_4() {
    let broker = Broker::start();
    _ = broker.run(&["topic", "create", "taxi"], "");
    let (registry, schema_registry) = schema_registry("json-refused");

    let output = broker.run(
        &[
            "generator",
            "taxi",
            "--schema-registry",
            &schema_registry,
            "--partition",
            "7",
            "--output",
            "json",
        ],
        "",
    );
    _ = std::fs::remove_dir_all(&registry);

    assert_eq!(Some(4), output.status.code(), "{output:?}");
    let summary = json(&output);
    assert_eq!(0, summary["records_produced"], "{output:?}");
    // UNKNOWN_TOPIC_OR_PARTITION is 3.
    assert_eq!(3, summary["error_code"], "{output:?}");
}

/// A refused perf run prints its summary from the run itself, so it prints one whether or not
/// any metric was recorded before the refusal.
#[test]
fn a_refused_perf_produce_prints_one_json_summary_and_exits_4() {
    let broker = Broker::start();
    _ = broker.run(&["topic", "create", "taxi"], "");

    let output = broker.run_unaided(
        &[
            "perf",
            "taxi",
            "--broker",
            broker.url(),
            "produce",
            "--partition",
            "7",
            "--duration",
            "30s",
            "--output",
            "json",
        ],
        &[],
    );
    assert_eq!(Some(4), output.status.code(), "{output:?}");

    let lines = json_lines(&output);
    assert_eq!(1, lines.len(), "{output:?}");
    assert_eq!("summary", lines[0]["type"], "{output:?}");
    assert_eq!(0, lines[0]["records_produced"], "{output:?}");
    // UNKNOWN_TOPIC_OR_PARTITION is 3.
    assert_eq!(3, lines[0]["error_code"], "{output:?}");
}

/// Ctrl-C ends a perf run with its summary as the last line, after the intervals, and with 0.
#[cfg(unix)]
#[test]
fn an_interrupted_perf_run_ends_with_one_json_summary_and_exits_0() {
    let broker = Broker::start();
    _ = broker.run(&["topic", "create", "taxi"], "");

    let process = broker.spawn_unaided(
        &[
            "perf",
            "taxi",
            "--broker",
            broker.url(),
            "produce",
            "--duration",
            "30s",
            "--output",
            "json",
        ],
        // Export every half second, so that the run has interval lines before the interrupt.
        &[("OTEL_METRIC_EXPORT_INTERVAL", "500")],
    );
    broker.wait_for_a_record("taxi");
    let started = std::time::Instant::now();
    interrupt(&process);

    let output = finish(process);
    assert_eq!(Some(0), output.status.code(), "{output:?}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(20),
        "the interrupt ends the run before its duration: {:?}",
        started.elapsed()
    );

    let lines = json_lines(&output);
    let (summary, intervals) = lines.split_last().expect("a summary");
    assert_eq!("summary", summary["type"], "{output:?}");
    assert_eq!(0, summary["error_code"], "{output:?}");
    assert!(
        summary["records_produced"].as_u64().unwrap() > 0,
        "{summary}"
    );
    for interval in intervals {
        assert_eq!("interval", interval["type"], "{output:?}");
    }
}

/// Ctrl-C ends a generator run with its one summary and with 0.
#[cfg(unix)]
#[test]
fn an_interrupted_generator_run_ends_with_one_json_summary_and_exits_0() {
    let broker = Broker::start();
    _ = broker.run(&["topic", "create", "taxi"], "");
    let (registry, schema_registry) = schema_registry("json-interrupted");

    let process = broker.spawn_unaided(
        &[
            "generator",
            "taxi",
            "--broker",
            broker.url(),
            "--schema-registry",
            &schema_registry,
            "--per-second",
            "50",
            "--duration-seconds",
            "30",
            "--output",
            "json",
        ],
        &[],
    );
    broker.wait_for_a_record("taxi");
    let started = std::time::Instant::now();
    interrupt(&process);

    let output = finish(process);
    _ = std::fs::remove_dir_all(&registry);
    assert_eq!(Some(0), output.status.code(), "{output:?}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(20),
        "the interrupt ends the run before its duration: {:?}",
        started.elapsed()
    );

    let lines = json_lines(&output);
    assert_eq!(1, lines.len(), "{output:?}");
    assert_eq!("summary", lines[0]["type"], "{output:?}");
    assert_eq!(0, lines[0]["error_code"], "{output:?}");
    assert!(
        lines[0]["records_produced"].as_u64().unwrap() > 0,
        "{output:?}"
    );
}

/// The summary is final once the producers have stopped, so it prints then, before the metrics
/// flush and the grace period that follow the run. A run of 1 s prints it within a few seconds,
/// although the process exits 5 s later.
#[test]
fn the_generator_summary_prints_when_the_run_ends_not_when_the_process_exits() {
    let broker = Broker::start();
    _ = broker.run(&["topic", "create", "taxi"], "");
    let (registry, schema_registry) = schema_registry("json-prompt");

    let process = broker.spawn_unaided(
        &[
            "generator",
            "taxi",
            "--broker",
            broker.url(),
            "--schema-registry",
            &schema_registry,
            "--per-second",
            "10",
            "--duration-seconds",
            "1",
            "--output",
            "json",
        ],
        &[],
    );
    let (output, summary_after) = finish_timed(process);
    _ = std::fs::remove_dir_all(&registry);

    assert_eq!(Some(0), output.status.code(), "{output:?}");
    assert_eq!("summary", json(&output)["type"], "{output:?}");
    let summary_after = summary_after.expect("a summary line");
    assert!(
        // The old order printed it after the 5 s grace period, so at 6 s or later.
        summary_after < std::time::Duration::from_secs(5),
        "the summary came {summary_after:?} after the start of a 1 s run"
    );
}

/// The metrics are flushed when the run ends. A collector that is down then must neither drop
/// the summary nor turn the refusal's 4 into the export error's 1.
#[test]
fn a_refused_produce_prints_the_json_summary_and_exits_4_when_the_collector_is_down() {
    let broker = Broker::start();
    _ = broker.run(&["topic", "create", "taxi"], "");
    let (registry, schema_registry) = schema_registry("json-refused-collector-down");

    let output = broker.run(
        &[
            "generator",
            "taxi",
            "--schema-registry",
            &schema_registry,
            "--partition",
            "7",
            "--otlp-endpoint-url",
            &format!("http://127.0.0.1:{}", closed_port()),
            "--output",
            "json",
        ],
        "",
    );
    _ = std::fs::remove_dir_all(&registry);

    assert_eq!(Some(4), output.status.code(), "{output:?}");
    assert_eq!("summary", json(&output)["type"], "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("UnknownTopicOrPartition"),
        "{output:?}"
    );
}
