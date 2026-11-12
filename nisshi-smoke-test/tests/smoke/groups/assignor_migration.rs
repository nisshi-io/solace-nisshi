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

//! Tests that a consumer group can change its partition assignor while its consumers keep running.

use std::time::Duration;

use nisshi_smoke_test::{BackgroundTool, KafkaCli, PartitionAssignor, VerifiableConsume, unique};

/// How long a consumer has to join its group and get its partitions, and how long a member has to
/// get its partitions after another member leaves.
const JOIN_TIMEOUT: Duration = Duration::from_secs(60);

/// How long the restarted consumer has to consume a record after it joins.
const CONSUME_TIMEOUT: Duration = Duration::from_secs(60);

/// The session timeout of the consumers, so that the broker removes a member that stops without
/// sending `LeaveGroup` within seconds, and the member doesn't hold up a rebalance.
const SESSION_TIMEOUT: Duration = Duration::from_secs(6);

/// How many records a second the producer writes. The producer keeps writing during the whole test,
/// so that the restarted consumer finds new records on the partition that it gets.
const RECORDS_PER_SECOND: u32 = 20;

/// The Java client's default `partition.assignment.strategy`. `kafka-verifiable-consumer` replaces
/// that default with `range` alone, so the test passes this list to give the tool's consumers the
/// client's default.
const CLIENT_DEFAULT_ASSIGNORS: [PartitionAssignor; 2] = [
    PartitionAssignor::Range,
    PartitionAssignor::CooperativeSticky,
];

/// Starts a consumer in `group` that lists the protocols of `assignors`, and waits until it joins
/// the group.
#[track_caller]
fn join_with_assignors(
    cli: &KafkaCli,
    topic: &str,
    group: &str,
    assignors: &[PartitionAssignor],
) -> BackgroundTool<VerifiableConsume> {
    let mut member =
        cli.verifiable_consumer()
            .assignors(assignors)
            .start(topic, group, SESSION_TIMEOUT, None);

    if let Err(reason) = member.wait_for_partitions_assigned(0, JOIN_TIMEOUT) {
        panic!("the consumer with the assignors {assignors:?} didn't join {group}: {reason}");
    }

    member
}

/// The Java client's default assignors are `range` and `cooperative-sticky`, and [Kafka documents]
/// that a group moves to `cooperative-sticky` with one rolling restart that removes `range` from
/// each consumer's list. A user whose first restarted consumer fails to join can't finish the
/// restart. The broker must choose the group's protocol again at each rebalance, from the
/// protocols that every member lists. A consumer that lists only `cooperative-sticky` must then
/// join a group whose other member lists the client's default. Both members must finish the
/// rebalance, and the restarted consumer must consume a record.
///
/// [Kafka documents]: https://kafka.apache.org/39/documentation.html#consumerconfigs_partition.assignment.strategy
#[test]
#[ignore = "the broker keeps the protocol of the group's first member until the group is empty, \
            and answers a consumer that lists only cooperative-sticky with \
            INCONSISTENT_GROUP_PROTOCOL (#1051)"]
fn restarted_consumer_with_only_cooperative_sticky_joins_a_group_on_the_default_assignors() {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(2, &[]);
    let group = unique::name("group");

    let producer = cli
        .verifiable_producer()
        .start(&topic, RECORDS_PER_SECOND, 0);

    let mut staying = join_with_assignors(&cli, &topic, &group, &CLIENT_DEFAULT_ASSIGNORS);
    let staying_lines_before_second_join = staying.line_count();
    let restarting = join_with_assignors(&cli, &topic, &group, &CLIENT_DEFAULT_ASSIGNORS);

    if let Err(reason) =
        staying.wait_for_partitions_assigned(staying_lines_before_second_join, JOIN_TIMEOUT)
    {
        panic!("the first member didn't get its partitions after the second joined: {reason}");
    }

    let staying_lines_before_leave = staying.line_count();
    drop(restarting.terminate());

    if let Err(reason) =
        staying.wait_for_partitions_assigned(staying_lines_before_leave, JOIN_TIMEOUT)
    {
        panic!(
            "the remaining member didn't get its partitions after the other member stopped: \
             {reason}"
        );
    }

    let staying_lines_before_restart = staying.line_count();
    let mut restarted = join_with_assignors(
        &cli,
        &topic,
        &group,
        &[PartitionAssignor::CooperativeSticky],
    );

    if let Err(reason) =
        staying.wait_for_partitions_assigned(staying_lines_before_restart, JOIN_TIMEOUT)
    {
        panic!(
            "the member on the default assignors didn't finish the rebalance after the restarted \
             member joined: {reason}"
        );
    }

    if let Err(reason) = restarted.wait_for_records_consumed(0, CONSUME_TIMEOUT) {
        panic!("the restarted member with only cooperative-sticky didn't consume: {reason}");
    }

    drop(restarted.terminate());
    drop(staying.terminate());
    drop(producer.terminate());
}
