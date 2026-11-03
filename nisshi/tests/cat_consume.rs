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

//! The fetch offset of `cat consume`, against the `nisshi` binary on an in-memory broker.

#![cfg(feature = "dynostore")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::{Broker, stdout};

/// A broker with the topic `taxi`, whose partition 0 holds `hello` and `world` in one batch at
/// offsets 0 and 1, then `again` at offset 2 and `more` at offset 3, each in a batch of its own.
fn taxi() -> Broker {
    let broker = Broker::start();

    _ = stdout(&broker.run(&["topic", "create", "taxi"], ""));

    for messages in [
        "{\"value\": \"hello\"}\n{\"value\": \"world\"}\n",
        "{\"value\": \"again\"}\n",
        "{\"value\": \"more\"}\n",
    ] {
        _ = stdout(&broker.run(&["cat", "produce", "taxi"], messages));
    }

    broker
}

#[test]
fn fetch_offset_starts_at_that_offset() {
    let broker = taxi();

    assert_eq!(
        "[{\"key\":null,\"value\":\"again\"}]\n[{\"key\":null,\"value\":\"more\"}]\n",
        stdout(&broker.run(&["cat", "consume", "taxi", "--fetch-offset", "2"], ""))
    );
}

/// Fetch answers with whole batches, so the offset inside a batch has the records before it
/// skipped, as a Kafka consumer does.
#[test]
fn fetch_offset_inside_a_batch_skips_the_records_before_it() {
    let broker = taxi();

    assert_eq!(
        "[{\"key\":null,\"value\":\"world\"}]\n[{\"key\":null,\"value\":\"again\"}]\n[{\"key\":null,\"value\":\"more\"}]\n",
        stdout(&broker.run(&["cat", "consume", "taxi", "--fetch-offset", "1"], ""))
    );
}

/// Fetch answers an offset below the log start with `OFFSET_OUT_OF_RANGE` and no records, and
/// `-1` is below it. That must not look like a valid offset with nothing to read to the caller.
/// An offset above the high watermark is answered with no error by design, so it is not covered
/// here.
#[test]
fn fetch_offset_below_the_log_start_fails_with_the_error() {
    let broker = taxi();

    let output = broker.run(&["cat", "consume", "taxi", "--fetch-offset=-1"], "");

    assert!(!output.status.success(), "{output:?}");
    assert_eq!("", String::from_utf8(output.stdout.clone()).unwrap());

    let stderr = String::from_utf8(output.stderr.clone()).unwrap();
    assert!(stderr.contains("OffsetOutOfRange"), "{stderr}");
}
