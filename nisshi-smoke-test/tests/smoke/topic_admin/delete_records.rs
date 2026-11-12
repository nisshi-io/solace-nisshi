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

//! Tests that `kafka-delete-records` deletes a partition's records before an offset. A user runs it
//! to drop records that consumers must never read, such as bad data, without deleting the topic.
//! The broker must delete exactly the records before the offset, and report the partition's new
//! earliest offset.

use nisshi_smoke_test::{Broker, KafkaCli};

/// How many records each test produces, at offsets 0 to 4.
const PRODUCED_RECORD_COUNT: usize = 5;

/// The offset that each test deletes the records before.
const DELETE_BEFORE_OFFSET: i64 = 3;

/// Launches a broker, and creates a topic on it with one partition that holds
/// [`PRODUCED_RECORD_COUNT`] records.
///
/// Each test launches its own broker instead of using the shared one, because a broker that
/// panics on `DeleteRecords` would fail every other test on the shared broker too.
#[track_caller]
fn create_topic_with_records() -> (Broker, KafkaCli, String) {
    let broker = Broker::isolated();
    let cli = KafkaCli::new(broker.bootstrap());
    let topic = cli.topics().create_unique(1, &[]);
    let values = (0..PRODUCED_RECORD_COUNT)
        .map(|n| format!("value-{n}"))
        .collect::<Vec<_>>();
    _ = cli.console_producer().produce_values(&topic, &values);

    (broker, cli, topic)
}

/// The tool prints the low watermark that the broker returns, the partition's new earliest offset.
/// A user reads it to confirm what the deletion did.
#[test]
fn delete_records_returns_the_new_low_watermark() {
    let (_broker, cli, topic) = create_topic_with_records();

    let deleted = cli.delete_records().before(&topic, 0, DELETE_BEFORE_OFFSET);

    assert_eq!(
        deleted.succeeded().low_watermark(&topic, 0),
        Some(DELETE_BEFORE_OFFSET),
        "{deleted}"
    );
}

/// After the deletion, the partition's earliest offset must be the offset that the records were
/// deleted before. A consumer that starts from the beginning then reads only the records the user
/// kept.
#[test]
fn earliest_offset_moves_to_the_offset_deleted_before() {
    let (_broker, cli, topic) = create_topic_with_records();
    _ = cli
        .delete_records()
        .before(&topic, 0, DELETE_BEFORE_OFFSET)
        .succeeded();

    assert_eq!(
        cli.get_offsets().partition_offsets(&topic, "earliest"),
        [DELETE_BEFORE_OFFSET]
    );
}

/// The deletion must not change the partition's latest offset. A broker that numbers new records
/// from a lower offset gives two records one offset, and consumers skip or repeat records.
#[test]
fn latest_offset_is_kept_after_delete_records() {
    let (_broker, cli, topic) = create_topic_with_records();
    _ = cli
        .delete_records()
        .before(&topic, 0, DELETE_BEFORE_OFFSET)
        .succeeded();

    assert_eq!(
        cli.get_offsets().partition_offsets(&topic, "latest"),
        [PRODUCED_RECORD_COUNT as i64]
    );
}
