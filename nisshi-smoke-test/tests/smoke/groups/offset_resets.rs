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

//! Tests that `kafka-consumer-groups --reset-offsets` moves a group to where an operator asks,
//! which is how they replay records or skip past them. In every mode, the broker must keep the
//! group's committed offset through `--dry-run`, and store the new offset on `--execute`, so that
//! the group's next consumer starts at that record. A reset that moves the group one record too
//! far or too short makes its consumers read records twice or skip them. A reset of a group that
//! has members must be refused, because the members would commit their own position over it.
//!
//! The tool finds the new offset with `ListOffsets`, by time or at the partition's ends, and moves
//! the group with `OffsetCommit`, so the broker must answer the first and store the second. The
//! tests read the committed offset with `OffsetFetch`, through [`BrokerStateReader`].

use std::time::{Duration, SystemTime};

use nisshi_smoke_test::{BrokerStateReader, KafkaCli, KafkaVerifiableProducer, unique};

/// The records that each test produces.
const RECORDS: [&str; 10] = ["r0", "r1", "r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9"];

/// The time between the two batches of a test of a reset by time, which the producer sets as the
/// records' timestamps. A reset by time looks up the middle of the gap, so the half hour on each
/// side leaves room for the seconds that the tools take to start.
const GAP_BETWEEN_BATCHES: Duration = Duration::from_secs(60 * 60);

/// How many records each batch of a test of a reset by time has.
const RECORDS_PER_BATCH: usize = 5;

/// The session timeout of the member in the test of a group with members.
const SESSION_TIMEOUT: Duration = Duration::from_secs(6);

/// How long the member in the test of a group with members has to join the group.
const JOIN_TIMEOUT: Duration = Duration::from_secs(60);

/// A topic with one partition, and a group that has read some of its records and committed its
/// offset.
struct ConsumerGroup {
    cli: KafkaCli,
    broker_state_reader: BrokerStateReader,
    topic: String,
    group: String,
    committed_offset: i64,
}

impl ConsumerGroup {
    /// Produces `records` in one batch, and returns a group that has read the first `records_read`
    /// of them.
    #[track_caller]
    fn after_reading(records: &[&str], records_read: usize) -> Self {
        Self::produced_with(records, records_read, |cli, topic| {
            _ = cli.console_producer().produce_values(topic, records);
        })
    }

    /// Produces a batch of `records_per_batch` records for each time in `created_at_millis`,
    /// timestamped about that time, and returns a group that has read the first `records_read` of
    /// them. The records of batch `n` have the values `<n>.0`, `<n>.1`, and so on.
    #[track_caller]
    fn after_reading_batches_created_at(
        created_at_millis: &[i64],
        records_per_batch: usize,
        records_read: usize,
    ) -> Self {
        let values = (0..)
            .zip(created_at_millis)
            .flat_map(|(batch, _)| KafkaVerifiableProducer::values(batch, records_per_batch))
            .collect::<Vec<_>>();

        Self::produced_with(&as_strs(&values), records_read, |cli, topic| {
            for (batch, created_at_millis) in (0..).zip(created_at_millis) {
                let produced = cli.verifiable_producer().produce_created_at(
                    topic,
                    records_per_batch,
                    *created_at_millis,
                    batch,
                    None,
                );
                assert_eq!(
                    produced.acknowledged_record_count(),
                    records_per_batch,
                    "{produced}"
                );
            }
        })
    }

    /// Returns a group that has read two records of two batches: one timestamped two
    /// [`GAP_BETWEEN_BATCHES`] ago, and one a gap ago. The group reads two records, so a reset to
    /// the second batch moves it forward, and a reset that the broker doesn't store leaves it where
    /// it was.
    #[track_caller]
    fn with_two_batches_an_hour_apart() -> Self {
        let gap_millis =
            i64::try_from(GAP_BETWEEN_BATCHES.as_millis()).expect("the gap fits in an i64");
        let now_millis = KafkaCli::shared().now_in_millis();

        Self::after_reading_batches_created_at(
            &[now_millis - 2 * gap_millis, now_millis - gap_millis],
            RECORDS_PER_BATCH,
            2,
        )
    }

    #[track_caller]
    fn produced_with(
        records: &[&str],
        records_read: usize,
        produce: impl FnOnce(&KafkaCli, &str),
    ) -> Self {
        let cli = KafkaCli::shared();
        let topic = cli.topics().create_unique(1, &[]);
        let group = unique::name("group");

        produce(&cli, &topic);

        let consumed = cli.console_consumer().consume(&topic, &group, records_read);
        assert_eq!(
            consumed.succeeded().consumed_values(),
            records[..records_read],
            "{consumed}"
        );

        Self {
            cli,
            broker_state_reader: BrokerStateReader::shared(),
            topic,
            group,
            committed_offset: offset_of(records_read),
        }
    }

    /// Returns the offset that the broker stores for the group.
    #[track_caller]
    fn stored_offset(&self) -> Option<i64> {
        self.broker_state_reader
            .committed_offset(&self.group, &self.topic, 0)
    }

    /// Checks that the broker keeps the group's offset through a dry run with `reset_option`,
    /// stores `expected_offset` after a reset with `reset_option`, and that the group's next
    /// consumer starts at the record `expected_first_value`.
    #[track_caller]
    fn assert_reset_moves_group_to(
        &self,
        reset_option: &[&str],
        expected_offset: i64,
        expected_first_value: &str,
    ) {
        self.assert_reset_stores(reset_option, expected_offset);

        let consumed = self
            .cli
            .console_consumer()
            .consume(&self.topic, &self.group, 1);
        assert_eq!(
            consumed.succeeded().consumed_values(),
            [expected_first_value],
            "{consumed}"
        );
    }

    /// Checks that the broker keeps the group's offset through a dry run with `reset_option`, and
    /// stores `expected_offset` after a reset with it.
    #[track_caller]
    fn assert_reset_stores(&self, reset_option: &[&str], expected_offset: i64) {
        let planned =
            self.cli
                .consumer_groups()
                .plan_offset_reset(&self.group, &self.topic, reset_option);
        _ = planned.succeeded();
        assert_eq!(
            self.stored_offset(),
            Some(self.committed_offset),
            "the broker stored another offset after a dry run: {planned}"
        );

        let executed =
            self.cli
                .consumer_groups()
                .reset_offsets(&self.group, &self.topic, reset_option);
        _ = executed.succeeded();
        assert_eq!(
            self.stored_offset(),
            Some(expected_offset),
            "the broker stored another offset than the reset's: {executed}"
        );
    }
}

/// A reset to the earliest offset must move the group to the partition's first record, so that
/// its next consumer reads every record again.
#[test]
fn reset_to_earliest_replays_from_the_first_record() {
    ConsumerGroup::after_reading(&RECORDS, 5).assert_reset_moves_group_to(
        &["--to-earliest"],
        0,
        "r0",
    );
}

/// A reset to the latest offset must skip every record so far, so the group's next consumer reads
/// only records produced after the reset.
#[test]
fn reset_to_latest_skips_every_record_so_far() {
    let consumer_group = ConsumerGroup::after_reading(&RECORDS, 5);
    consumer_group.assert_reset_stores(&["--to-latest"], offset_of(RECORDS.len()));

    let ConsumerGroup {
        cli, topic, group, ..
    } = &consumer_group;

    _ = cli
        .console_producer()
        .produce_values(topic, &["after-reset"]);

    let consumed = cli.console_consumer().consume(topic, group, 1);
    assert_eq!(
        consumed.succeeded().consumed_values(),
        ["after-reset"],
        "{consumed}"
    );
}

/// A reset to an offset must move the group to that offset, so that its next consumer starts at
/// that record.
#[test]
fn reset_to_offset_starts_at_that_record() {
    ConsumerGroup::after_reading(&RECORDS, 5).assert_reset_moves_group_to(
        &["--to-offset", "2"],
        2,
        "r2",
    );
}

/// A reset that shifts by a negative count must move the group back from its committed offset by
/// that many records, so that its next consumer reads those records again.
#[test]
fn reset_shift_by_moves_back_from_the_committed_offset() {
    ConsumerGroup::after_reading(&RECORDS, 5).assert_reset_moves_group_to(
        &["--shift-by", "-3"],
        2,
        "r2",
    );
}

/// A reset to a time must move the group to the first record at or after that time. The broker
/// must find that record by the records' timestamps.
#[test]
fn reset_to_datetime_starts_at_first_record_after_that_time() {
    let consumer_group = ConsumerGroup::with_two_batches_an_hour_apart();
    let middle_of_gap = SystemTime::now() - GAP_BETWEEN_BATCHES * 3 / 2;
    let datetime = humantime::format_rfc3339_millis(middle_of_gap).to_string();

    consumer_group.assert_reset_moves_group_to(
        &["--to-datetime", &datetime],
        offset_of(RECORDS_PER_BATCH),
        "1.0",
    );
}

/// A reset by duration must move the group to the first record produced within that duration
/// before now. The broker must find that record by the records' timestamps.
#[test]
fn reset_by_duration_starts_at_first_record_within_it() {
    let consumer_group = ConsumerGroup::with_two_batches_an_hour_apart();
    let back_to_middle_of_gap = format!("PT{}S", (GAP_BETWEEN_BATCHES * 3 / 2).as_secs());

    consumer_group.assert_reset_moves_group_to(
        &["--by-duration", &back_to_middle_of_gap],
        offset_of(RECORDS_PER_BATCH),
        "1.0",
    );
}

/// A reset of a group that has members must be refused, and the broker must keep the group's
/// offset. The members keep their position in memory and commit it again, so a reset under them
/// would be lost without a word. The tool refuses the reset when the broker answers
/// `DescribeGroups` with a state other than `Empty`, so the broker must report the group's members.
#[test]
fn reset_of_a_group_with_members_is_refused() {
    // The group reads every record, so that its member has nothing left to read, and commits keep
    // the offset at the end.
    let consumer_group = ConsumerGroup::after_reading(&RECORDS, RECORDS.len());
    let ConsumerGroup {
        cli, topic, group, ..
    } = &consumer_group;

    let (member, _) =
        cli.verifiable_consumer()
            .join_group(topic, group, SESSION_TIMEOUT, None, JOIN_TIMEOUT);

    let refused = cli
        .consumer_groups()
        .reset_offsets(group, topic, &["--to-earliest"]);
    let stored_offset = consumer_group.stored_offset();

    drop(member.terminate());

    // The tool exits with 0 when it refuses, so the test checks the offset that the broker stores.
    // A broker that reported the group as `Empty` would let the reset store offset 0.
    assert_eq!(
        stored_offset,
        Some(consumer_group.committed_offset),
        "the broker stored another offset after a reset of a group with members: {refused}"
    );
}

/// Returns the offset of the record at `index` in a topic's only partition.
fn offset_of(index: usize) -> i64 {
    i64::try_from(index).expect("a record's index fits in an i64")
}

/// Borrows each of `words`, for the tool methods that take `&[&str]`.
fn as_strs(words: &[String]) -> Vec<&str> {
    words.iter().map(String::as_str).collect()
}
