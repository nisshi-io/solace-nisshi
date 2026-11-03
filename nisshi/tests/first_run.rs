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

//! The first commands a new user runs, against the `nisshi` binary: `topic create`, `cat produce`
//! and `cat consume` on an in-memory broker.

#![cfg(feature = "dynostore")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::{Broker, stdout};

#[test]
fn topic_create_and_cat_produce_confirm_success() {
    let broker = Broker::start();

    assert_eq!(
        "Created topic taxi with 3 partitions.\n",
        stdout(&broker.run(&["topic", "create", "taxi"], ""))
    );

    assert_eq!(
        "Produced 2 messages to taxi partition 0, offsets 0 to 1.\n",
        stdout(&broker.run(
            &["cat", "produce", "taxi"],
            "{\"value\": \"hello\"}\n{\"value\": \"world\"}\n"
        ))
    );

    assert_eq!(
        "Produced 1 message to taxi partition 0, offset 2.\n",
        stdout(&broker.run(&["cat", "produce", "taxi"], "{\"value\": \"again\"}\n"))
    );

    let consumed = stdout(&broker.run(&["cat", "consume", "taxi"], ""));

    for value in ["hello", "world", "again"] {
        assert!(consumed.contains(value), "{value} not in {consumed}");
    }
}

#[test]
fn topic_list_prints_a_table_or_json() {
    let broker = Broker::start();

    assert_eq!("No topics.\n", stdout(&broker.run(&["topic", "list"], "")));
    assert_eq!(
        "[]\n",
        stdout(&broker.run(&["topic", "list", "--output", "json"], ""))
    );

    _ = stdout(&broker.run(&["topic", "create", "taxi"], ""));
    _ = stdout(&broker.run(&["topic", "create", "hello", "--partitions", "1"], ""));

    assert_eq!(
        "TOPIC  PARTITIONS\nhello  1\ntaxi   3\n",
        stdout(&broker.run(&["topic", "list"], ""))
    );

    let json: serde_json::Value = serde_json::from_str(&stdout(
        &broker.run(&["topic", "list", "--output", "json"], ""),
    ))
    .expect("topic list --output json prints JSON");

    let taxi = json
        .as_array()
        .expect("an array of topics")
        .iter()
        .find(|topic| topic["name"] == "taxi")
        .expect("taxi is listed");

    assert_eq!(3, taxi["partitions"].as_array().expect("partitions").len());
}

#[test]
fn silent_prints_nothing_on_success() {
    let broker = Broker::start();

    assert_eq!(
        "",
        stdout(&broker.run(&["topic", "create", "taxi", "--silent"], ""))
    );

    assert_eq!(
        "",
        stdout(&broker.run(
            &["cat", "produce", "taxi", "--silent"],
            "{\"value\": \"hello\"}\n"
        ))
    );
}

#[test]
fn failed_topic_create_prints_no_confirmation() {
    let broker = Broker::start();

    _ = stdout(&broker.run(&["topic", "create", "taxi"], ""));

    let duplicate = broker.run(&["topic", "create", "taxi"], "");

    assert_eq!(Some(1), duplicate.status.code(), "{duplicate:?}");
    assert!(
        !String::from_utf8_lossy(&duplicate.stdout).contains("Created topic"),
        "{duplicate:?}"
    );
}

#[test]
fn failed_cat_produce_prints_no_confirmation() {
    let broker = Broker::start();

    _ = stdout(&broker.run(&["topic", "create", "taxi", "--partitions", "3"], ""));

    let missing_partition = broker.run(
        &["cat", "produce", "taxi", "--partition", "7"],
        "{\"value\": \"hello\"}\n",
    );

    assert_eq!(
        Some(1),
        missing_partition.status.code(),
        "{missing_partition:?}"
    );
    assert!(
        !String::from_utf8_lossy(&missing_partition.stdout).contains("Produced"),
        "{missing_partition:?}"
    );
}

/// A command whose stdout reader has gone, as in `nisshi topic create taxi | true`, has still
/// done its work. It must exit 0, because a caller that retries a failed `cat produce` writes
/// the messages again.
#[test]
fn closed_stdout_does_not_fail_a_command_that_succeeded() {
    let broker = Broker::start();

    let created = broker.run_with_closed_stdout(&["topic", "create", "taxi"], "");
    assert!(created.status.success(), "{created:?}");

    let produced =
        broker.run_with_closed_stdout(&["cat", "produce", "taxi"], "{\"value\": \"hello\"}\n");
    assert!(produced.status.success(), "{produced:?}");

    assert_eq!(
        "[{\"key\":null,\"value\":\"hello\"}]\n",
        stdout(&broker.run(&["cat", "consume", "taxi"], ""))
    );
}
