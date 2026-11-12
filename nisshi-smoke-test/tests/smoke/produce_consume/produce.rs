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

//! Tests that the broker stores every record a producer sends once, and returns it exactly as it
//! was sent, and that it refuses a record larger than the topic allows. A record that is lost,
//! changed or stored twice gives every consumer wrong data. Also tests that the broker sets record
//! timestamps itself on a topic that asks for it.

use std::{
    collections::BTreeSet,
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

use nisshi_smoke_test::{
    Acks, ConsumedRecord, KafkaCli, KafkaVerifiableProducer, LOG_APPEND_TIME, PRINTED_NULL,
    RECORD_TOO_LARGE_EXCEPTION,
};

/// A topic's `max.message.bytes`, the largest record batch the broker accepts for it.
const MAX_MESSAGE_BYTES: usize = 10_000;

fn record_values(records: &[ConsumedRecord]) -> Vec<&str> {
    records.iter().map(|record| record.value.as_str()).collect()
}

/// Returns the `count` records in partition 0 of `topic`, and panics if the partition holds more.
///
/// A read of `count` records stops before any extra record, so this function checks the latest
/// offset. A read of one more record would also find an extra record, but on a correct broker
/// that read waits for its timeout.
#[track_caller]
fn read_all_records(cli: &KafkaCli, topic: &str, count: usize) -> Vec<ConsumedRecord> {
    let records = cli.console_consumer().read_records(topic, 0, 0, count);

    assert_eq!(
        cli.get_offsets().partition_offsets(topic, "latest"),
        [count as i64],
        "the partition holds more than the {count} records produced"
    );

    records
}

/// A producer that sends values without keys must get them back in order, without keys.
#[test]
fn values_without_keys_read_back() {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(1, &[]);
    let values = ["one", "two", "three"];

    _ = cli.console_producer().produce_values(&topic, &values);

    assert_eq!(
        cli.console_consumer()
            .read_records(&topic, 0, 0, values.len())
            .iter()
            .map(|record| (record.key.as_str(), record.value.as_str()))
            .collect::<Vec<_>>(),
        [
            (PRINTED_NULL, "one"),
            (PRINTED_NULL, "two"),
            (PRINTED_NULL, "three")
        ]
    );
}

/// A tombstone, a key with a null value, marks the key as deleted, so its value must come back null
/// and not empty. The console consumer prints a null value as `null`.
#[test]
fn tombstone_reads_back_as_null() {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(1, &[]);

    _ = cli
        .console_producer()
        .produce_keyed(&topic, &[("k0", Some("v0")), ("k1", None)]);

    assert_eq!(
        cli.console_consumer()
            .read_records(&topic, 0, 0, 2)
            .iter()
            .map(|record| (record.key.as_str(), record.value.as_str()))
            .collect::<Vec<_>>(),
        [("k0", "v0"), ("k1", PRINTED_NULL)]
    );
}

/// `acks` sets when the producer counts a record as sent: when it is written to the network, when
/// the broker has stored it, or when every replica has. Whichever it is, the broker must store
/// every record.
#[track_caller]
fn stores_every_record(acks: Acks) {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(1, &[]);
    let count = 100;

    let produced = cli.verifiable_producer().produce(&topic, count, acks, 0);

    // With `acks=0` the producer counts a record as sent without an answer from the broker, so the
    // count says nothing about the broker.
    if acks != Acks::None {
        assert_eq!(produced.acknowledged_record_count(), count, "{produced}");
    }

    assert_eq!(
        record_values(&read_all_records(&cli, &topic, count)),
        KafkaVerifiableProducer::values(0, count)
    );
}

#[test]
fn acks_0_stores_every_record() {
    stores_every_record(Acks::None);
}

#[test]
fn acks_1_stores_every_record() {
    stores_every_record(Acks::Leader);
}

#[test]
fn acks_all_stores_every_record() {
    stores_every_record(Acks::FullIsr);
}

/// Producers compress record batches to save network and storage. The broker must keep a
/// compressed batch so that consumers get back the same records.
#[track_caller]
fn compressed_records_read_back(compression: &str) {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(1, &[]);
    let produced_values = (0..10)
        .map(|n| format!("{compression}-{n}"))
        .collect::<Vec<_>>();

    _ = cli.console_producer().produce_values_with(
        &topic,
        &produced_values,
        &[&format!("compression.type={compression}")],
    );

    assert_eq!(
        record_values(
            &cli.console_consumer()
                .read_records(&topic, 0, 0, produced_values.len())
        ),
        produced_values
    );
}

#[test]
fn gzip_records_read_back() {
    compressed_records_read_back("gzip");
}

#[test]
fn snappy_records_read_back() {
    compressed_records_read_back("snappy");
}

#[test]
fn lz4_records_read_back() {
    compressed_records_read_back("lz4");
}

#[test]
fn zstd_records_read_back() {
    compressed_records_read_back("zstd");
}

/// A record just under the topic's limit must be accepted whole. A record batch holds a record and
/// about 70 bytes besides, so a 9,800-byte value fits under 10,000.
#[test]
fn record_under_max_message_bytes_reads_back() {
    let cli = KafkaCli::shared();
    let topic = cli
        .topics()
        .create_unique(1, &[&format!("max.message.bytes={MAX_MESSAGE_BYTES}")]);
    let value = "a".repeat(MAX_MESSAGE_BYTES - 200);

    _ = cli.console_producer().produce_values(&topic, &[&value]);

    assert_eq!(
        record_values(&cli.console_consumer().read_records(&topic, 0, 0, 1)),
        [value.as_str()]
    );
}

/// A record over the limit must be refused with `RecordTooLargeException`, which tells the producer
/// that retrying won't help.
#[test]
fn record_over_max_message_bytes_is_rejected() {
    let cli = KafkaCli::shared();
    let topic = cli
        .topics()
        .create_unique(1, &[&format!("max.message.bytes={MAX_MESSAGE_BYTES}")]);
    let value = "a".repeat(MAX_MESSAGE_BYTES + 200);

    let produced = cli.console_producer().try_produce_values(&topic, &[&value]);

    assert!(
        produced.succeeded().mentions(RECORD_TOO_LARGE_EXCEPTION),
        "{produced}"
    );
}

/// An idempotent producer numbers its batches, so the broker can drop a batch sent twice. The
/// broker must give the producer a producer id, accept its numbered batches and store each record
/// once, with no gaps. This test doesn't make the producer retry, so it doesn't check that a
/// retried batch is dropped.
#[test]
fn idempotent_producer_stores_each_record_once() {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(1, &[]);
    let count = 1_000;

    let produced = cli
        .verifiable_producer()
        .produce_idempotent(&topic, count, 0);
    assert_eq!(produced.acknowledged_record_count(), count, "{produced}");

    let records = read_all_records(&cli, &topic, count);

    assert_eq!(
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        (0..count as i64).collect::<Vec<_>>()
    );
    assert_eq!(
        record_values(&records),
        KafkaVerifiableProducer::values(0, count)
    );
}

/// Several producers writing to one partition at once must each get offsets of their own, with
/// none skipped or given twice, or consumers would miss records or read them twice.
#[test]
fn concurrent_producers_get_unique_gapless_offsets() {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(1, &[]);
    let producers = 5;
    let records_per_producer = 200;

    thread::scope(|scope| {
        let producer_threads = (0..producers)
            .map(|producer| {
                let (cli, topic) = (&cli, &topic);
                scope.spawn(move || {
                    cli.verifiable_producer().produce(
                        topic,
                        records_per_producer,
                        Acks::FullIsr,
                        producer,
                    )
                })
            })
            .collect::<Vec<_>>();

        for producer in producer_threads {
            let produced = producer.join().expect("producer thread");
            assert_eq!(
                produced.acknowledged_record_count(),
                records_per_producer,
                "{produced}"
            );
        }
    });

    let total_records = producers as usize * records_per_producer;
    let records = read_all_records(&cli, &topic, total_records);

    assert_eq!(
        records
            .iter()
            .map(|record| record.offset)
            .collect::<Vec<_>>(),
        (0..total_records as i64).collect::<Vec<_>>()
    );
    assert_eq!(
        records
            .into_iter()
            .map(|record| record.value)
            .collect::<BTreeSet<_>>(),
        (0..producers)
            .flat_map(|producer| KafkaVerifiableProducer::values(producer, records_per_producer))
            .collect::<BTreeSet<_>>()
    );
}

/// Returns the time now on this machine's clock, in milliseconds since the Unix epoch. A broker
/// that the harness launches runs on this machine, and stamps a `LogAppendTime` record with this
/// clock, not with the clock of the Kafka tools' container.
fn now_in_millis_on_this_machine() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}

/// The broker sets each record's timestamp to when it stored it, and marks it `LogAppendTime`.
#[test]
fn log_append_time_gives_broker_timestamps() {
    let cli = KafkaCli::shared();
    let topic = cli
        .topics()
        .create_unique(1, &["message.timestamp.type=LogAppendTime"]);

    let before = now_in_millis_on_this_machine();
    _ = cli.console_producer().produce_values(&topic, &["stamped"]);
    let after = now_in_millis_on_this_machine();

    let [record] = &cli.console_consumer().read_records(&topic, 0, 0, 1)[..] else {
        unreachable!("read_records checks the record count")
    };

    assert!(
        record.timestamp_type == LOG_APPEND_TIME && (before..=after).contains(&record.timestamp),
        "not between {before} and {after}: {record:?}"
    );
}
