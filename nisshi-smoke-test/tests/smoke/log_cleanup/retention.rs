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

//! Tests that the broker's maintenance run deletes the records that a topic's retention configs
//! expire, and keeps the others. A user sets `retention.ms` or `retention.bytes` to bound how much
//! a topic stores. A broker that deletes too little fills its storage, and a broker that deletes
//! too much loses records that consumers haven't read yet.
//!
//! These tests run on PostgreSQL and SQLite, the engines that apply retention. Memory storage is
//! for development, and it doesn't apply retention. Each test launches a broker that runs
//! maintenance every 2 seconds. A test that needs expired records gives the topic a `retention.ms`
//! of 30 seconds and waits for it to pass, instead of producing records with an old timestamp,
//! because the broker can also count a record's age from the time that it stored the record.

#![cfg(any(feature = "postgres", feature = "sqlite"))]

use std::{thread, time::Duration};

use nisshi_smoke_test::{Broker, KafkaCli, polling};

/// How long a test waits for a maintenance run to give the result it expects.
const MAINTENANCE_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a record lives in a topic with [`SHORT_RETENTION`].
const SHORT_RETENTION_PERIOD: Duration = Duration::from_secs(30);

/// How far ahead of the clock a test stamps the records that retention must keep. Retention deletes
/// a record only once its timestamp is older than `retention.ms`, so these records outlast the
/// test, however long its checks take. The broker rejects a timestamp more than an hour ahead.
const KEPT_RECORD_LEAD: Duration = Duration::from_secs(600);

/// The value prefix of the records that a test stamps [`KEPT_RECORD_LEAD`] ahead.
const KEPT_VALUE_PREFIX: u32 = 1;

/// The `retention.ms` of [`SHORT_RETENTION_PERIOD`], which must change with it.
const SHORT_RETENTION: &str = "retention.ms=30000";

const DELETE_POLICY: &str = "cleanup.policy=delete";

/// How many records a test will let expire, at offsets 0, 1 and 2.
const EXPIRED_RECORD_COUNT: usize = 3;

#[track_caller]
fn produce_records(cli: &KafkaCli, topic: &str, count: usize) {
    let values = (0..count).map(|n| format!("old-{n}")).collect::<Vec<_>>();
    _ = cli.console_producer().produce_values(topic, &values);
}

/// Produces `count` records stamped [`KEPT_RECORD_LEAD`] ahead, which retention keeps for the rest
/// of the test. Record `n`'s value is `<KEPT_VALUE_PREFIX>.<n>`.
#[track_caller]
fn produce_kept_records(cli: &KafkaCli, topic: &str, count: usize) {
    let ahead = cli.now_in_millis() + KEPT_RECORD_LEAD.as_millis() as i64;
    let produced =
        cli.verifiable_producer()
            .produce_created_at(topic, count, ahead, KEPT_VALUE_PREFIX, None);

    assert_eq!(produced.acknowledged_record_count(), count, "{produced}");
}

/// Waits until the records that a test produced before it called this have expired under
/// [`SHORT_RETENTION`].
fn wait_for_records_to_expire() {
    thread::sleep(SHORT_RETENTION_PERIOD);
}

#[track_caller]
fn earliest_offset(cli: &KafkaCli, topic: &str) -> i64 {
    cli.get_offsets().partition_offsets(topic, "earliest")[0]
}

#[track_caller]
fn latest_offset(cli: &KafkaCli, topic: &str) -> i64 {
    cli.get_offsets().partition_offsets(topic, "latest")[0]
}

/// Waits until `topic`'s earliest offset is `expected`, which shows that a maintenance run deleted
/// the records before it. Panics if that doesn't happen within [`MAINTENANCE_TIMEOUT`].
#[track_caller]
fn wait_until_earliest_offset_is(cli: &KafkaCli, topic: &str, expected: i64) {
    polling::wait_until(MAINTENANCE_TIMEOUT, || match earliest_offset(cli, topic) {
        earliest if earliest == expected => Ok(()),
        earliest => Err(format!(
            "{topic}'s earliest offset is {earliest}, not {expected}"
        )),
    });
}

/// Asserts that `topic` still holds the `count` records produced to it, at offsets from 0.
#[track_caller]
fn assert_every_record_kept(cli: &KafkaCli, topic: &str, count: usize) {
    let offsets = cli
        .console_consumer()
        .read_records(topic, 0, 0, count)
        .into_iter()
        .map(|record| record.offset)
        .collect::<Vec<_>>();

    assert_eq!(offsets, (0..count as i64).collect::<Vec<_>>());
}

/// A topic with a short `retention.ms`, whose deleted record shows that a maintenance run has
/// applied retention since the record expired.
struct MaintenanceWitness {
    topic: String,
}

impl MaintenanceWitness {
    /// A test creates the witness after it produces the records that it lets expire, so that the
    /// witness's record expires last, and its deletion comes from a run after theirs expired.
    #[track_caller]
    fn create(cli: &KafkaCli) -> Self {
        let topic = cli
            .topics()
            .create_unique(1, &[DELETE_POLICY, SHORT_RETENTION]);
        produce_records(cli, &topic, 1);
        Self { topic }
    }

    #[track_caller]
    fn wait_for_maintenance_run(&self, cli: &KafkaCli) {
        produce_kept_records(cli, &self.topic, 1);
        wait_until_earliest_offset_is(cli, &self.topic, 1);
    }
}

/// Retention must delete the records older than `retention.ms`, and keep the newer ones that
/// consumers still need. The earliest offset moves to the first record it keeps.
#[test]
fn expired_records_are_deleted_and_newer_records_kept() {
    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let topic = cli
        .topics()
        .create_unique(1, &[DELETE_POLICY, SHORT_RETENTION]);
    produce_records(&cli, &topic, EXPIRED_RECORD_COUNT);
    wait_for_records_to_expire();
    produce_kept_records(&cli, &topic, 2);

    wait_until_earliest_offset_is(&cli, &topic, EXPIRED_RECORD_COUNT as i64);

    let kept_records = cli
        .console_consumer()
        .read_records_from_earliest(&topic, 0, 2);
    let kept_offsets = kept_records.iter().map(|record| record.offset);
    let kept_values = kept_records.iter().map(|record| record.value.clone());
    assert_eq!(kept_offsets.collect::<Vec<_>>(), [3, 4]);
    assert_eq!(
        kept_values.collect::<Vec<_>>(),
        [
            format!("{KEPT_VALUE_PREFIX}.0"),
            format!("{KEPT_VALUE_PREFIX}.1")
        ]
    );
}

/// After retention deletes every record of a partition, the broker must give the next record the
/// offset after the last one it stored. A broker that numbers from the records that remain starts
/// again at 0, and a consumer that already read offsets 0 to 2 skips the new records.
#[test]
fn record_produced_after_retention_gets_the_next_offset() {
    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let topic = cli
        .topics()
        .create_unique(1, &[DELETE_POLICY, SHORT_RETENTION]);
    produce_records(&cli, &topic, EXPIRED_RECORD_COUNT);
    let witness = MaintenanceWitness::create(&cli);
    wait_for_records_to_expire();
    witness.wait_for_maintenance_run(&cli);

    _ = cli
        .console_producer()
        .produce_values(&topic, &["after-retention"]);

    // The read starts at the earliest offset, so it also shows that retention deleted the
    // expired records, and the partition was empty before the new record.
    assert_eq!(
        cli.console_consumer()
            .read_records_from_earliest(&topic, 0, 1)
            .into_iter()
            .map(|record| (record.offset, record.value))
            .collect::<Vec<_>>(),
        [(EXPIRED_RECORD_COUNT as i64, "after-retention".to_owned())]
    );
}

/// When retention deletes every record of a partition, the earliest and the latest offset must
/// both be the offset after the last deleted record, as Kafka reports them. A consumer group's
/// committed offset is at that offset, and a latest offset that goes back makes the group's
/// offset look out of range, so the group starts again from the beginning or the end.
#[test]
fn offsets_are_kept_when_retention_empties_a_partition() {
    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let topic = cli
        .topics()
        .create_unique(1, &[DELETE_POLICY, SHORT_RETENTION]);
    produce_records(&cli, &topic, EXPIRED_RECORD_COUNT);
    let witness = MaintenanceWitness::create(&cli);
    wait_for_records_to_expire();

    witness.wait_for_maintenance_run(&cli);

    assert_eq!(
        (earliest_offset(&cli, &topic), latest_offset(&cli, &topic)),
        (EXPIRED_RECORD_COUNT as i64, EXPIRED_RECORD_COUNT as i64)
    );
}

/// A `retention.ms` added to an existing topic with `kafka-configs` must apply from the next
/// maintenance run. A user lowers it to free storage at once. The topic keeps its records under
/// the default retention of 7 days first, so the test shows that the added config deleted them.
#[test]
fn added_retention_ms_deletes_older_records() {
    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let topic = cli.topics().create_unique(1, &[DELETE_POLICY]);
    produce_records(&cli, &topic, EXPIRED_RECORD_COUNT);
    let witness = MaintenanceWitness::create(&cli);
    wait_for_records_to_expire();
    witness.wait_for_maintenance_run(&cli);
    assert_every_record_kept(&cli, &topic, EXPIRED_RECORD_COUNT);
    // The added retention keeps this record, so retention never empties the topic.
    produce_kept_records(&cli, &topic, 1);

    _ = cli
        .configs()
        .add_to_topic(&topic, &[SHORT_RETENTION])
        .succeeded();

    wait_until_earliest_offset_is(&cli, &topic, EXPIRED_RECORD_COUNT as i64);
}

/// Deleting a topic's `retention.ms` with `kafka-configs` must restore the default of 7 days, which
/// keeps a record that has passed the deleted value. A broker that keeps applying the deleted value
/// deletes records the user meant to keep.
#[test]
fn deleted_retention_ms_restores_the_default() {
    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let topic = cli
        .topics()
        .create_unique(1, &[DELETE_POLICY, SHORT_RETENTION]);
    _ = cli
        .configs()
        .delete_from_topic(&topic, &["retention.ms"])
        .succeeded();
    produce_records(&cli, &topic, EXPIRED_RECORD_COUNT);
    let witness = MaintenanceWitness::create(&cli);
    wait_for_records_to_expire();

    witness.wait_for_maintenance_run(&cli);

    assert_every_record_kept(&cli, &topic, EXPIRED_RECORD_COUNT);
}

/// Retention must keep a partition within its topic's `retention.bytes`, by deleting the oldest
/// records, and keep the newest ones. A user sets it to bound a topic's storage, whatever the
/// records' age.
#[test]
fn retention_bytes_keeps_partition_under_the_cap() {
    /// The size of each record's value. The broker stores more bytes than this per record, so a
    /// partition under the cap holds at most `RETENTION_BYTES / VALUE_BYTES` records.
    const VALUE_BYTES: usize = 1000;
    const RETENTION_BYTES: usize = 5 * VALUE_BYTES;
    const PRODUCED_RECORD_COUNT: usize = 20;

    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let retention_bytes = format!("retention.bytes={RETENTION_BYTES}");
    let topic = cli
        .topics()
        .create_unique(1, &[DELETE_POLICY, &retention_bytes]);
    let values = (0..PRODUCED_RECORD_COUNT)
        .map(|n| format!("{n:0>VALUE_BYTES$}"))
        .collect::<Vec<_>>();
    _ = cli.console_producer().produce_values(&topic, &values);

    // A broker can delete the records in several runs, as Kafka does a segment at a time, so the
    // test waits until the partition is under the cap, not for the first deletion.
    polling::wait_until(MAINTENANCE_TIMEOUT, || {
        let kept_record_count = PRODUCED_RECORD_COUNT as i64 - earliest_offset(&cli, &topic);

        if kept_record_count <= (RETENTION_BYTES / VALUE_BYTES) as i64 {
            Ok(())
        } else {
            Err(format!(
                "{topic} keeps {kept_record_count} records of {VALUE_BYTES} bytes, with \
                 retention.bytes of {RETENTION_BYTES}"
            ))
        }
    });

    let newest_offset = PRODUCED_RECORD_COUNT as u64 - 1;
    assert_eq!(
        cli.console_consumer()
            .read_records(&topic, 0, newest_offset, 1)[0]
            .value,
        values[PRODUCED_RECORD_COUNT - 1]
    );
}
