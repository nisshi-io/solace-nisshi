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

//! Tests that a topic created with `kafka-topics` can be listed, described, produced to, consumed
//! through a group and deleted, and that the tools report the right offsets at each step. A new
//! user runs these commands first, so a failure here is the first thing they see.

use std::collections::BTreeSet;

use nisshi_smoke_test::{GroupOffsets, KafkaCli, Record, unique};

const RECORDS_WITH_KEYS_AND_HEADERS: [Record<'static>; 3] = [
    Record {
        headers: &[("h1", "pqr"), ("h2", "jkl"), ("h3", "uio")],
        key: "qwerty",
        value: "poiuy",
    },
    Record {
        headers: &[("h1", "def"), ("h2", "lmn"), ("h3", "xyz")],
        key: "asdfgh",
        value: "lkj",
    },
    Record {
        headers: &[("h1", "stu"), ("h2", "fgh"), ("h3", "ijk")],
        key: "zxcvbn",
        value: "mnbvc",
    },
];

/// Each test topic's partition count.
const TEST_TOPIC_PARTITIONS: u32 = 3;

/// Each test topic's configs.
const TEST_TOPIC_CONFIGS: &[&str] = &["cleanup.policy=compact"];

/// Creates a test topic with a name no other test uses.
#[track_caller]
fn create_test_topic(cli: &KafkaCli) -> String {
    cli.topics()
        .create_unique(TEST_TOPIC_PARTITIONS, TEST_TOPIC_CONFIGS)
}

/// Creates a test topic and produces [`RECORDS_WITH_KEYS_AND_HEADERS`] to it.
#[track_caller]
fn create_test_topic_with_records(cli: &KafkaCli) -> String {
    let topic = create_test_topic(cli);
    produce_test_records(cli, &topic);
    topic
}

/// Produces [`RECORDS_WITH_KEYS_AND_HEADERS`] to `topic`.
#[track_caller]
fn produce_test_records(cli: &KafkaCli, topic: &str) {
    _ = cli
        .console_producer()
        .produce(topic, &RECORDS_WITH_KEYS_AND_HEADERS);
}

/// `offset` for each of a test topic's partitions.
fn every_partition_at_offset(offset: i64) -> Vec<i64> {
    vec![offset; TEST_TOPIC_PARTITIONS as usize]
}

/// `kafka-topics --delete` with a mistyped topic name must fail, and must not create the topic.
/// `kafka-topics` finds the topic missing in `Metadata` before it sends `DeleteTopics`, so the
/// broker must answer `Metadata` without the topic, and without creating it.
#[test]
fn delete_missing_topic_fails() {
    let cli = KafkaCli::shared();
    let topic = unique::name("topic");

    _ = cli.topics().delete(&topic).exited(1);

    let listed = cli.topics().list();
    assert!(!listed.succeeded().lists_topic(&topic), "{listed}");
}

#[test]
fn list_shows_created_topic() {
    let cli = KafkaCli::shared();
    let topic = create_test_topic(&cli);

    let listed = cli.topics().list();

    assert!(
        listed.succeeded().lists_topic(&topic),
        "{topic} is not listed: {listed}"
    );
}

#[test]
fn create_duplicate_topic_fails() {
    let cli = KafkaCli::shared();
    let topic = create_test_topic(&cli);

    cli.topics()
        .create(&topic, TEST_TOPIC_PARTITIONS, TEST_TOPIC_CONFIGS)
        .assert_topic_already_exists();
}

#[test]
fn describe_shows_partition_count() {
    let cli = KafkaCli::shared();
    let topic = create_test_topic(&cli);

    let described = cli.topics().describe(&topic);

    assert_eq!(
        described.succeeded().partition_count(),
        Some(TEST_TOPIC_PARTITIONS),
        "{described}"
    );
}

/// `kafka-topics --describe` shows only the configs that a topic sets itself, not defaults.
#[test]
fn describe_shows_topic_configs() {
    let cli = KafkaCli::shared();
    let topic = create_test_topic(&cli);

    let described = cli.topics().describe(&topic);

    assert_eq!(
        described.succeeded().topic_configs(),
        TEST_TOPIC_CONFIGS,
        "{described}"
    );
}

#[test]
fn earliest_offsets_are_0_before_produce() {
    let cli = KafkaCli::shared();
    let topic = create_test_topic(&cli);

    assert_eq!(
        cli.get_offsets().partition_offsets(&topic, "earliest"),
        every_partition_at_offset(0)
    );
}

#[test]
fn latest_offsets_are_0_before_produce() {
    let cli = KafkaCli::shared();
    let topic = create_test_topic(&cli);

    assert_eq!(
        cli.get_offsets().partition_offsets(&topic, "latest"),
        every_partition_at_offset(0)
    );
}

#[test]
fn produce_with_keys_and_headers_succeeds() {
    let cli = KafkaCli::shared();
    let topic = create_test_topic(&cli);

    produce_test_records(&cli, &topic);
}

#[test]
fn earliest_offsets_stay_0_after_produce() {
    let cli = KafkaCli::shared();
    let topic = create_test_topic_with_records(&cli);

    assert_eq!(
        cli.get_offsets().partition_offsets(&topic, "earliest"),
        every_partition_at_offset(0)
    );
}

/// The latest offsets must count every record produced. The client's partitioner chooses each
/// record's partition, so the test checks the total over the partitions.
#[test]
fn latest_offsets_count_records_after_produce() {
    let cli = KafkaCli::shared();
    let topic = create_test_topic_with_records(&cli);

    let latest = cli.get_offsets().partition_offsets(&topic, "latest");

    assert_eq!(
        latest.iter().sum::<i64>(),
        RECORDS_WITH_KEYS_AND_HEADERS.len() as i64,
        "{latest:?}"
    );
}

/// A group member must receive every record that was produced, each with the key, headers and
/// value it was produced with. The broker must number each partition's records from 0, without a
/// gap or a repeat, or a consumer that resumes from a committed offset skips or rereads records.
#[test]
fn group_consumes_every_record() {
    let cli = KafkaCli::shared();
    let topic = create_test_topic_with_records(&cli);

    let consumed = cli.console_consumer().consume(
        &topic,
        &unique::name("group"),
        RECORDS_WITH_KEYS_AND_HEADERS.len(),
    );

    // Kafka orders records within a partition, not across partitions, so the records are compared
    // as a set. The set leaves out the partition and offset fields at the start of each line,
    // because the client's partitioner chooses each record's partition.
    assert_eq!(
        consumed
            .succeeded()
            .consumed_lines()
            .into_iter()
            .filter_map(|line| line.splitn(3, '\t').nth(2))
            .map(str::to_owned)
            .collect::<BTreeSet<_>>(),
        RECORDS_WITH_KEYS_AND_HEADERS
            .iter()
            .map(Record::console_line)
            .collect::<BTreeSet<_>>(),
        "{consumed}"
    );

    for (partition, offsets) in consumed.consumed_offsets_by_partition() {
        assert_eq!(
            offsets,
            (0..offsets.len() as i64).collect::<Vec<_>>(),
            "partition {partition}: {consumed}"
        );
    }
}

/// After a member consumes every record, the group's committed offset on each partition that holds
/// a record must be that partition's end offset, so a member that joins later has nothing left to
/// read.
#[test]
fn committed_offset_reaches_end_after_consuming() {
    let cli = KafkaCli::shared();
    let topic = create_test_topic_with_records(&cli);
    let group = unique::name("group");

    let consumed =
        cli.console_consumer()
            .consume(&topic, &group, RECORDS_WITH_KEYS_AND_HEADERS.len());
    let described = cli.consumer_groups().describe(&group);

    let consumed_offsets = consumed.succeeded().consumed_offsets_by_partition();

    // The console consumer exits with 0 when it stops at its timeout, so the test checks that the
    // member read every record. The partitions it read from are then every partition that holds a
    // record.
    assert_eq!(
        consumed_offsets.values().map(Vec::len).sum::<usize>(),
        RECORDS_WITH_KEYS_AND_HEADERS.len(),
        "{consumed}"
    );

    // The client's partitioner chooses each record's partition, so the test takes the number of
    // records on each partition from what the member consumed.
    for (partition, offsets) in consumed_offsets {
        let records = offsets.len() as u64;

        assert_eq!(
            described.group_offsets(&group, &topic, partition),
            Some(GroupOffsets {
                committed: records,
                end: records,
            }),
            "partition {partition}: {described}"
        );
    }
}

#[test]
fn deleted_topic_is_not_listed() {
    let cli = KafkaCli::shared();
    let topic = create_test_topic(&cli);

    _ = cli.topics().delete(&topic).succeeded();

    let listed = cli.topics().list();

    assert!(
        !listed.succeeded().lists_topic(&topic),
        "{topic} is still listed: {listed}"
    );
}
