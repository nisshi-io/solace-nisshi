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

//! Tests that the broker's maintenance run compacts a topic with `cleanup.policy=compact`: it
//! keeps the latest record of each key, and deletes the older ones. A user reads such a topic as a
//! table of each key's current value. A broker that keeps old values grows without bound, and a
//! broker that deletes the latest value loses the table's contents.
//!
//! These tests run on PostgreSQL and SQLite, the engines that compact a topic. Memory storage is
//! for development, and it doesn't compact. Each test launches a broker that runs maintenance
//! every 2 seconds.

#![cfg(any(feature = "postgres", feature = "sqlite"))]

use std::{thread, time::Duration};

use nisshi_smoke_test::{Broker, KafkaCli, PRINTED_NULL, polling};

/// How long a test waits for a maintenance run to give the result it expects.
const MAINTENANCE_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a record lives in a topic with [`SHORT_RETENTION`].
const SHORT_RETENTION_PERIOD: Duration = Duration::from_secs(30);

/// How far ahead of the clock a test stamps the records that retention must keep. Retention deletes
/// a record only once its timestamp is older than `retention.ms`, so these records outlast the
/// test, however long its checks take. The broker rejects a timestamp more than an hour ahead.
const KEPT_RECORD_LEAD: Duration = Duration::from_secs(600);

/// The `retention.ms` of [`SHORT_RETENTION_PERIOD`], which must change with it.
const SHORT_RETENTION: &str = "retention.ms=30000";

/// A record that a partition holds: its offset, and its key and value as the console consumer
/// prints them.
#[derive(Debug, PartialEq, Eq)]
struct StoredRecord<'a> {
    offset: i64,
    key: &'a str,
    value: &'a str,
}

/// Waits until the records from `topic`'s earliest offset are `expected`. Panics if that doesn't
/// happen within [`MAINTENANCE_TIMEOUT`].
///
/// Until a maintenance run compacts the topic, the topic holds at least as many records as
/// `expected`, so each read returns at once.
#[track_caller]
fn wait_until_records_are(cli: &KafkaCli, topic: &str, expected: &[StoredRecord<'_>]) {
    polling::wait_until(MAINTENANCE_TIMEOUT, || {
        let consumed = cli
            .console_consumer()
            .read_records_from_earliest(topic, 0, expected.len());
        let records = consumed
            .iter()
            .map(|record| StoredRecord {
                offset: record.offset,
                key: &record.key,
                value: &record.value,
            })
            .collect::<Vec<_>>();

        if records == expected {
            Ok(())
        } else {
            Err(format!(
                "{topic}'s records are {records:?}, not {expected:?}"
            ))
        }
    });
}

/// Compaction must keep only the latest value of each key, at the offset it was produced at, in
/// offset order. A consumer that reads the topic from the beginning then ends with each key's
/// current value.
#[test]
fn compaction_keeps_the_latest_value_of_each_key() {
    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let topic = cli.topics().create_unique(1, &["cleanup.policy=compact"]);
    _ = cli.console_producer().produce_keyed(
        &topic,
        &[
            ("k0", Some("v0")),
            ("k1", Some("v1")),
            ("k0", Some("v2")),
            ("k1", Some("v3")),
            ("k2", Some("v4")),
        ],
    );

    wait_until_records_are(
        &cli,
        &topic,
        &[
            StoredRecord {
                offset: 2,
                key: "k0",
                value: "v2",
            },
            StoredRecord {
                offset: 3,
                key: "k1",
                value: "v3",
            },
            StoredRecord {
                offset: 4,
                key: "k2",
                value: "v4",
            },
        ],
    );
}

/// A tombstone, a record with a null value, deletes its key. Compaction must delete the key's
/// earlier values, and keep the tombstone itself, as Kafka does for `delete.retention.ms`, 24
/// hours by default. A consumer that starts later still reads the tombstone, and deletes the key
/// from its own copy of the table.
#[test]
fn tombstone_is_the_only_record_left_for_its_key() {
    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let topic = cli.topics().create_unique(1, &["cleanup.policy=compact"]);
    _ = cli.console_producer().produce_keyed(
        &topic,
        &[("k0", Some("v0")), ("k1", Some("v1")), ("k0", None)],
    );

    wait_until_records_are(
        &cli,
        &topic,
        &[
            StoredRecord {
                offset: 1,
                key: "k1",
                value: "v1",
            },
            StoredRecord {
                offset: 2,
                key: "k0",
                value: PRINTED_NULL,
            },
        ],
    );
}

/// With `cleanup.policy=compact,delete`, maintenance must both compact the topic and delete the
/// records that `retention.ms` has expired. The expired records have keys of their own, so only
/// retention deletes them, and the new records share a key, so only compaction deletes the older
/// one.
#[test]
fn compact_delete_policy_compacts_and_deletes_expired_records() {
    let broker = Broker::isolated_with_frequent_maintenance();
    let cli = KafkaCli::new(broker.bootstrap());
    let topic = cli
        .topics()
        .create_unique(1, &["cleanup.policy=compact,delete", SHORT_RETENTION]);
    _ = cli
        .console_producer()
        .produce_keyed(&topic, &[("old-0", Some("v0")), ("old-1", Some("v1"))]);
    // The test waits for the records to expire instead of producing them with an old timestamp,
    // because the broker can also count a record's age from the time that it stored the record.
    thread::sleep(SHORT_RETENTION_PERIOD);
    // Both new records have the key `0`, so compaction deletes the first. They are stamped
    // `KEPT_RECORD_LEAD` ahead, so retention keeps them.
    let ahead = cli.now_in_millis() + KEPT_RECORD_LEAD.as_millis() as i64;
    let produced = cli
        .verifiable_producer()
        .produce_created_at(&topic, 2, ahead, 0, Some(1));
    assert_eq!(produced.acknowledged_record_count(), 2, "{produced}");

    wait_until_records_are(
        &cli,
        &topic,
        &[StoredRecord {
            offset: 3,
            key: "0",
            value: "0.1",
        }],
    );
}
