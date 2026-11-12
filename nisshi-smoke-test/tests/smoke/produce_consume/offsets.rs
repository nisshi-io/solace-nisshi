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

//! Tests that `kafka-get-offsets` finds offsets by time, which users need to replay a topic from a
//! point in time. A lookup must return the first record at or after the time, -1 when there is
//! none, and a clear error when the broker can't do the lookup.

use nisshi_smoke_test::{KafkaCli, UNSUPPORTED_VERSION_EXCEPTION};

/// A topic with one record produced before the returned time, at offset 0, and two after it, at
/// offsets 1 and 2.
///
/// The returned time is halfway between the timestamps of records 0 and 1, which the producer
/// sets. The test's own clock would also give a time between them, but the producer runs in a
/// container, and a container's clock can differ from the host's by more than a short gap.
#[track_caller]
fn create_topic_with_records_before_and_after() -> (KafkaCli, String, i64) {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(1, &[]);

    _ = cli.console_producer().produce_values(&topic, &["before"]);
    _ = cli
        .console_producer()
        .produce_values(&topic, &["after-0", "after-1"]);

    let records = cli.console_consumer().read_records(&topic, 0, 0, 3);
    let time_between_records = records[0].timestamp.midpoint(records[1].timestamp);

    (cli, topic, time_between_records)
}

/// A lookup by time must return the offset of the first record at or after that time, so a replay
/// from that time starts there and misses no record. Two records follow the time, so a lookup
/// that returns the last of them instead of the first fails this test.
#[test]
fn timestamp_lookup_returns_first_record_at_or_after_it() {
    let (cli, topic, time_between_records) = create_topic_with_records_before_and_after();

    let offsets = cli
        .get_offsets()
        .at(&topic, &time_between_records.to_string());

    assert_eq!(
        offsets.succeeded().partition_offsets(&topic),
        [1],
        "{offsets}"
    );
}

/// When no record is at or after the time, the broker must answer -1, and `kafka-get-offsets` then
/// prints no offset for the partition. Offset 0 would make a replay read the whole partition again.
#[test]
fn timestamp_after_last_record_finds_no_offset() {
    let (cli, topic, _) = create_topic_with_records_before_and_after();
    let hour_after_last_record = cli.now_in_millis() + 3_600_000;

    let offsets = cli
        .get_offsets()
        .at(&topic, &hour_after_last_record.to_string());

    assert_eq!(
        offsets.succeeded().partition_offsets(&topic),
        [] as [i64; 0],
        "{offsets}"
    );
}

/// `max-timestamp` needs ListOffsets version 7. Until the broker supports it, it must not
/// advertise it, so the tool fails at once with `UnsupportedVersionException` instead of waiting
/// for an answer that never comes.
#[test]
fn max_timestamp_lookup_fails_at_once_as_unsupported() {
    let (cli, topic, _) = create_topic_with_records_before_and_after();

    let offsets = cli.get_offsets().at(&topic, "max-timestamp");

    assert!(offsets.mentions(UNSUPPORTED_VERSION_EXCEPTION), "{offsets}");
}
