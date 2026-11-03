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

//! Tests that `kafka-topics` describes and changes a topic as it does against Apache Kafka, so that
//! admin and monitoring scripts written for Kafka work unchanged.

use nisshi_smoke_test::{KafkaCli, PartitionReplicas, unique};

/// The partition count of a topic the broker creates for a producer.
const AUTO_CREATE_PARTITIONS: u32 = 1;

/// Every record on each partition, as offset and value.
#[track_caller]
fn records_on_each_partition(cli: &KafkaCli, topic: &str) -> Vec<Vec<(i64, String)>> {
    let mut records_on_each_partition = Vec::new();

    for (end, partition) in cli
        .get_offsets()
        .partition_offsets(topic, "latest")
        .into_iter()
        .zip(0..)
    {
        let records = if end == 0 {
            Vec::new()
        } else {
            cli.console_consumer()
                .read_records(topic, partition, 0, end as usize)
        };

        records_on_each_partition.push(
            records
                .into_iter()
                .map(|record| (record.offset, record.value))
                .collect(),
        );
    }

    records_on_each_partition
}

/// Nisshi is one broker, node 111, which holds the only replica of every partition. Monitoring
/// tools read the leader, replicas and in-sync replicas from `--describe` to find partitions that
/// have no leader or too few replicas, so all three must be 111.
#[test]
fn describe_shows_node_111_as_leader_replica_and_isr() {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(3, &[]);

    let described = cli.topics().describe(&topic);

    for partition in 0..3 {
        assert_eq!(
            described.succeeded().partition_replicas(partition),
            Some(PartitionReplicas {
                leader: "111",
                replicas: "111",
                isr: "111",
            }),
            "partition {partition}: {described}"
        );
    }
}

/// `kafka-topics --describe` with a mistyped topic name must fail without hanging, and must not
/// create the topic. The broker must answer `Metadata` without the topic, and without creating it.
#[test]
fn describing_missing_topic_fails() {
    let cli = KafkaCli::shared();
    let topic = unique::name("topic");

    _ = cli.topics().describe(&topic).exited(1);

    let listed = cli.topics().list();
    assert!(!listed.succeeded().lists_topic(&topic), "{listed}");
}

/// A topic deleted and created again under the same name is a new topic: none of the old records
/// come back, and its offsets start again at 0.
#[test]
fn recreated_topic_starts_at_offset_0() {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(1, &[]);

    _ = cli
        .console_producer()
        .produce_values(&topic, &["old-0", "old-1"]);
    _ = cli.topics().delete(&topic).succeeded();
    _ = cli.topics().create(&topic, 1, &[]).succeeded();
    _ = cli.console_producer().produce_values(&topic, &["new-0"]);

    assert_eq!(
        records_on_each_partition(&cli, &topic),
        [[(0, "new-0".to_owned())]]
    );
}

/// Producing to a deleted topic recreates it, as Apache Kafka does by default. The new topic must
/// hold only the records produced after the delete, starting at offset 0.
#[test]
fn producing_to_deleted_topic_recreates_it_empty() {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(1, &[]);

    _ = cli
        .console_producer()
        .produce_values(&topic, &["old-0", "old-1"]);
    _ = cli.topics().delete(&topic).succeeded();
    _ = cli.console_producer().produce_values(&topic, &["new-0"]);

    assert_eq!(
        records_on_each_partition(&cli, &topic)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>(),
        [(0, "new-0".to_owned())]
    );
}

/// Like Apache Kafka with its default settings, the broker creates a topic when a producer writes
/// to one that doesn't exist, and gives it [`AUTO_CREATE_PARTITIONS`] partition, as Kafka's
/// default `num.partitions` does.
#[test]
fn produce_to_missing_topic_creates_it() {
    let cli = KafkaCli::shared();
    let topic = unique::name("topic");

    _ = cli.console_producer().produce_values(&topic, &["created"]);

    let described = cli.topics().describe(&topic);

    assert_eq!(
        described.succeeded().partition_count(),
        Some(AUTO_CREATE_PARTITIONS),
        "{described}"
    );

    assert_eq!(
        records_on_each_partition(&cli, &topic)
            .into_iter()
            .flatten()
            .collect::<Vec<_>>(),
        [(0, "created".to_owned())]
    );
}
