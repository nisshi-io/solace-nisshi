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

//! Tests that a consumer group keeps consuming while members leave, join and crash. When a member
//! leaves, joins or crashes, the broker's group coordinator must rebalance the group and hand the
//! member's partitions to the other members, so that every record is consumed and no partition is
//! left without a consumer. A static member that restarts within its session timeout must get its
//! partitions back without a rebalance, which would pause every other member.
//!
//! The tests print how long consuming paused at each rebalance, so that the cost of a rebalance
//! stays visible. One test also asserts the pauses of a group with the client's default session
//! timeout, against the pauses that Kafka's own broker gives.

use std::{
    collections::BTreeSet,
    thread,
    time::{Duration, Instant},
};

use nisshi_smoke_test::{
    BackgroundTool, ConsumerEvent, KafkaCli, Output, StdoutLines, TopicPartition,
    VerifiableConsume, unique,
};

/// How long a consumer has to join its group and get its partitions, and how long the members
/// of a group have to get their partitions after a rebalance.
const JOIN_TIMEOUT: Duration = Duration::from_secs(60);

/// The session timeout of the consumers in the rebalance test, so that the broker removes a killed
/// member from the group within seconds.
const SHORT_SESSION_TIMEOUT: Duration = Duration::from_secs(6);

/// The session timeout of the static members. It must be longer than a consumer takes to restart.
const STATIC_MEMBER_SESSION_TIMEOUT: Duration = Duration::from_secs(30);

/// The session timeout of the members in the test of a clean leave. The broker removes a member
/// that stops without `LeaveGroup` only after this time, so a reassignment within it shows that the
/// broker acted on `LeaveGroup`.
const LEAVE_GROUP_SESSION_TIMEOUT: Duration = Duration::from_secs(30);

/// How long both static members keep running after the restarted member rejoins. A member learns
/// of a rebalance only from the answer to its next heartbeat, and the Java client sends one every
/// 3 s by default, so a rebalance shows in the other member's events within this time.
const AFTER_STATIC_MEMBER_REJOINS: Duration = Duration::from_secs(10);

/// How long the consumers have to read every record after the producer stops.
const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(60);

/// How long each member has to consume a record after a change to the group's members.
const CONSUME_TIMEOUT: Duration = Duration::from_secs(60);

/// The Java client's default `session.timeout.ms`. The timing test uses it, because a broker that
/// holds a join for a part of the session timeout pauses a group with this default for many
/// seconds.
const DEFAULT_SESSION_TIMEOUT: Duration = Duration::from_secs(45);

/// Kafka's default `group.initial.rebalance.delay.ms`. Kafka holds the first join of an empty
/// group for this delay, and holds it for another delay after each delay in which a member joined.
const INITIAL_REBALANCE_DELAY: Duration = Duration::from_secs(3);

/// The Java client's default `heartbeat.interval.ms`. A member learns of a rebalance from the
/// answer to its next heartbeat.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(3);

/// How long the members of the timing test take, on Kafka, to revoke, rejoin, sync and fetch once
/// each knows of a rebalance.
const REBALANCE_DURATION: Duration = Duration::from_secs(1);

/// How much later than Kafka the broker may end each step of the timing test, for its round trips
/// to storage and for a CI runner that is slower than the machine that measured Kafka.
const ALLOWED_DELAY_OVER_KAFKA: Duration = Duration::from_secs(2);

/// How long the timing test's group may take from the start of its last member to its first
/// record. The last member joins during the initial delay that is running, and Kafka then holds
/// the join for one more delay.
const FIRST_RECORD_BOUND: Duration = INITIAL_REBALANCE_DELAY
    .saturating_mul(2)
    .saturating_add(REBALANCE_DURATION)
    .saturating_add(ALLOWED_DELAY_OVER_KAFKA);

/// How long the timing test's group may pause after a member leaves or joins: until the other
/// members' next heartbeat, and then for the rebalance.
const PAUSE_BOUND: Duration = HEARTBEAT_INTERVAL
    .saturating_add(REBALANCE_DURATION)
    .saturating_add(ALLOWED_DELAY_OVER_KAFKA);

const PARTITIONS: u32 = 3;
const RECORDS_PER_SECOND: u32 = 20;

type ConsumerLines = StdoutLines<VerifiableConsume>;
type GroupMember = BackgroundTool<VerifiableConsume>;

/// Returns the acknowledged offsets that no consumer read.
fn unconsumed_offsets(
    acknowledged: &BTreeSet<(u32, i64)>,
    consumers_lines: &[ConsumerLines],
) -> BTreeSet<(u32, i64)> {
    let consumed_ranges = consumers_lines
        .iter()
        .flat_map(ConsumerLines::consumer_events)
        .flat_map(|(_, event)| match event {
            ConsumerEvent::RecordsConsumed { partitions } => partitions,
            _ => Vec::new(),
        })
        .collect::<Vec<_>>();

    acknowledged
        .iter()
        .filter(|(partition, offset)| {
            !consumed_ranges.iter().any(|range| {
                range.partition == *partition
                    && (range.min_offset..=range.max_offset).contains(offset)
            })
        })
        .copied()
        .collect()
}

/// Returns the partitions that a member consumed at or after `change`.
fn partitions_consumed_after(change: Instant, consumers_lines: &[ConsumerLines]) -> BTreeSet<u32> {
    consumers_lines
        .iter()
        .flat_map(ConsumerLines::consumer_events)
        .filter(|(read_at, _)| *read_at >= change)
        .flat_map(|(_, event)| match event {
            ConsumerEvent::RecordsConsumed { partitions } => partitions,
            _ => Vec::new(),
        })
        .map(|consumed| consumed.partition)
        .collect()
}

/// Returns how long consuming paused after `change`: the longest time that a partition went
/// without a consumed record, from `change` to the partition's last record before `next_change`. A
/// member keeps consuming until it learns of the rebalance, so the pause starts after `change` and
/// shows as a gap between two records of a partition, not only as the time to the first record
/// after `change`. Returns `None` if a partition was not consumed between the two changes.
fn pause_after(
    change: Instant,
    next_change: Instant,
    consumers_lines: &[ConsumerLines],
) -> Option<Duration> {
    let events = consumers_lines
        .iter()
        .flat_map(ConsumerLines::consumer_events)
        .filter(|(read_at, _)| (change..next_change).contains(read_at))
        .collect::<Vec<_>>();

    (0..PARTITIONS)
        .map(|partition| {
            let mut consumed_at = events
                .iter()
                .filter(|(_, event)| {
                    matches!(
                        event,
                        ConsumerEvent::RecordsConsumed { partitions }
                            if partitions.iter().any(|range| range.partition == partition)
                    )
                })
                .map(|(read_at, _)| *read_at)
                .collect::<Vec<_>>();

            if consumed_at.is_empty() {
                return None;
            }

            // The members' events come from several lines, so the times are sorted before
            // the gaps are measured.
            consumed_at.sort();

            [change]
                .into_iter()
                .chain(consumed_at)
                .collect::<Vec<_>>()
                .windows(2)
                .map(|pair| pair[1].duration_since(pair[0]))
                .max()
        })
        .collect::<Option<Vec<_>>>()
        .and_then(|pauses| pauses.into_iter().max())
}

/// A member of the rebalance test's group, with the name that failure messages give it.
struct NamedMember {
    name: &'static str,
    consumer: GroupMember,
}

impl NamedMember {
    /// Returns the partitions that the member holds now: those of its last `partitions_assigned`
    /// event, or none if it revoked them after that event.
    ///
    /// The verifiable consumer's default assignor, `RangeAssignor`, is eager: the member revokes
    /// all its partitions at the start of each rebalance, and each `partitions_assigned` event
    /// lists all the partitions the member holds after it.
    fn held_partitions(&self) -> BTreeSet<u32> {
        let mut held_partitions = BTreeSet::new();

        for (_, event) in self.consumer.stdout_lines().consumer_events() {
            match event {
                ConsumerEvent::PartitionsAssigned { partitions } => {
                    held_partitions = partitions
                        .into_iter()
                        .map(|assigned| assigned.partition)
                        .collect();
                }
                ConsumerEvent::PartitionsRevoked { .. } => held_partitions.clear(),
                _ => {}
            }
        }

        held_partitions
    }
}

/// Waits until each of `members` prints a `partitions_assigned` event after the line count at its
/// position in `line_counts_before`, and until the members hold every partition between them. A
/// member prints that event after the group finishes the rebalance that `change` caused.
#[track_caller]
fn wait_until_reassigned(
    change: &str,
    members: &mut [&mut NamedMember],
    line_counts_before: &[usize],
) {
    for (member, line_count_before) in members.iter_mut().zip(line_counts_before) {
        if let Err(reason) = member
            .consumer
            .wait_for_partitions_assigned(*line_count_before, JOIN_TIMEOUT)
        {
            panic!(
                "the {} member didn't get its partitions after {change}: {reason}",
                member.name
            );
        }
    }

    // A member can print its assignment from a rebalance that the group then repeats, so the
    // test waits until the members' current assignments cover every partition.
    let deadline = Instant::now() + JOIN_TIMEOUT;

    loop {
        let held_partitions = members
            .iter()
            .map(|member| (member.name, member.held_partitions()))
            .collect::<Vec<_>>();

        let all_held = held_partitions
            .iter()
            .flat_map(|(_, partitions)| partitions.iter().copied())
            .collect::<BTreeSet<_>>();

        if all_held == (0..PARTITIONS).collect() {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "the members didn't hold every partition within {JOIN_TIMEOUT:?} after {change}, \
             each member's partitions: {held_partitions:?}"
        );

        thread::sleep(Duration::from_millis(100));
    }
}

/// Waits until each of `members` consumes a record, so that the group consumes between two
/// changes to its members.
#[track_caller]
fn wait_until_each_consumes(after: &str, members: &mut [&mut NamedMember]) {
    for member in members.iter_mut() {
        let line_count = member.consumer.line_count();

        if let Err(reason) = member
            .consumer
            .wait_for_records_consumed(line_count, CONSUME_TIMEOUT)
        {
            panic!(
                "the {} member didn't consume after {after}: {reason}",
                member.name
            );
        }
    }
}

fn line_counts(members: &[&mut NamedMember]) -> Vec<usize> {
    members
        .iter()
        .map(|member| member.consumer.line_count())
        .collect()
}

/// Starts the member `name` of `group`, and waits until it and every member in `others` have
/// their partitions after the rebalance it causes.
#[track_caller]
fn start_member_and_wait(
    cli: &KafkaCli,
    topic: &str,
    group: &str,
    name: &'static str,
    others: &mut [&mut NamedMember],
) -> NamedMember {
    let mut line_counts_before = line_counts(others);
    let consumer = cli
        .verifiable_consumer()
        .start(topic, group, SHORT_SESSION_TIMEOUT, None);
    let mut started = NamedMember { name, consumer };

    let mut members = others
        .iter_mut()
        .map(|other| &mut **other)
        .collect::<Vec<_>>();
    members.push(&mut started);
    line_counts_before.push(0);

    wait_until_reassigned(
        &format!("the {name} member joined"),
        &mut members,
        &line_counts_before,
    );

    started
}

fn print_pause(change: &str, pause: Option<Duration>) {
    match pause {
        Some(pause) => println!("consuming paused for {pause:?} after {change}"),
        None => println!("a partition wasn't consumed again after {change}"),
    }
}

/// Returns each member's stderr, where the client logs errors such as a failed commit, for a
/// failure message.
fn members_stderr(members: &[(&str, &Output<VerifiableConsume>)]) -> String {
    members
        .iter()
        .map(|(name, output)| format!("the {name} member's stderr:\n{}\n", output.stderr))
        .collect()
}

/// While a producer writes, one member leaves, a new member joins, and another member is killed.
/// The broker's group coordinator must give the partitions to the other members after each change,
/// so that the members hold every partition after each rebalance, and the group consumes every
/// record the broker acknowledged.
#[test]
fn group_consumes_every_record_while_members_leave_join_and_crash() {
    let test_started = Instant::now();
    // The test prints each step as it reaches it, so that the output of a test that nextest stops
    // shows the step that didn't finish.
    let print_step = |step: &str| println!("{:?}: {step}", test_started.elapsed());

    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(PARTITIONS, &[]);
    let group = unique::name("group");

    let producer = cli
        .verifiable_producer()
        .start(&topic, RECORDS_PER_SECOND, 0);

    let mut staying = start_member_and_wait(&cli, &topic, &group, "staying", &mut []);
    let mut leaving = start_member_and_wait(&cli, &topic, &group, "leaving", &mut [&mut staying]);
    let mut crashing = start_member_and_wait(
        &cli,
        &topic,
        &group,
        "crashing",
        &mut [&mut staying, &mut leaving],
    );
    print_step("three members joined");

    wait_until_each_consumes(
        "three members joined",
        &mut [&mut staying, &mut leaving, &mut crashing],
    );

    let line_counts_before_leave = line_counts(&[&mut staying, &mut crashing]);
    let left_at = Instant::now();
    let (left_output, left_lines) = leaving.consumer.terminate();
    wait_until_reassigned(
        "the leaving member left",
        &mut [&mut staying, &mut crashing],
        &line_counts_before_leave,
    );
    print_step("partitions reassigned after the leaving member left");

    wait_until_each_consumes(
        "the leaving member left",
        &mut [&mut staying, &mut crashing],
    );

    let rejoined_at = Instant::now();
    let mut rejoined = start_member_and_wait(
        &cli,
        &topic,
        &group,
        "rejoined",
        &mut [&mut staying, &mut crashing],
    );
    print_step("partitions reassigned after a member rejoined");

    wait_until_each_consumes(
        "a member rejoined",
        &mut [&mut staying, &mut crashing, &mut rejoined],
    );

    let line_counts_before_crash = line_counts(&[&mut staying, &mut rejoined]);
    let crashed_at = Instant::now();
    let (crashed_output, crashed_lines) = crashing.consumer.kill();
    wait_until_reassigned(
        "the crashing member was killed",
        &mut [&mut staying, &mut rejoined],
        &line_counts_before_crash,
    );
    let reassigned_after_crash_at = Instant::now();
    print_step("partitions reassigned after the crashing member was killed");

    wait_until_each_consumes(
        "the crashing member was killed",
        &mut [&mut staying, &mut rejoined],
    );

    let produced = producer.stop();
    let producer_stopped_at = Instant::now();
    let acknowledged = produced
        .acknowledged_records()
        .into_iter()
        .map(|record| (record.partition, record.offset))
        .collect::<BTreeSet<_>>();
    print_step("producer stopped");

    let deadline = Instant::now() + CATCH_UP_TIMEOUT;
    let consumers_lines = loop {
        let consumers_lines = vec![
            left_lines.clone(),
            crashed_lines.clone(),
            staying.consumer.stdout_lines(),
            rejoined.consumer.stdout_lines(),
        ];

        if unconsumed_offsets(&acknowledged, &consumers_lines).is_empty()
            || Instant::now() >= deadline
        {
            break consumers_lines;
        }

        thread::sleep(Duration::from_millis(500));
    };

    let (staying_output, _) = staying.consumer.terminate();
    let (rejoined_output, _) = rejoined.consumer.terminate();
    print_step("members stopped");

    let stderr = members_stderr(&[
        ("leaving", &left_output),
        ("crashing", &crashed_output),
        ("staying", &staying_output),
        ("rejoined", &rejoined_output),
    ]);

    print_pause(
        "a member left",
        pause_after(left_at, rejoined_at, &consumers_lines),
    );
    print_pause(
        "a member joined",
        pause_after(rejoined_at, crashed_at, &consumers_lines),
    );
    print_pause(
        "a member was killed",
        pause_after(crashed_at, producer_stopped_at, &consumers_lines),
    );

    // The producer picks each record's partition from its key, so the test can check that every
    // partition was consumed only if the producer wrote to every partition.
    let partitions_produced_to = acknowledged
        .iter()
        .map(|(partition, _)| *partition)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        partitions_produced_to,
        (0..PARTITIONS).collect(),
        "the test couldn't check the broker, because the producer didn't write to every \
         partition: {produced}"
    );

    let unconsumed = unconsumed_offsets(&acknowledged, &consumers_lines);
    assert!(
        unconsumed.is_empty(),
        "no member consumed these acknowledged partitions and offsets: {unconsumed:?}\n{stderr}"
    );

    let consumed_after_last_rebalance =
        partitions_consumed_after(reassigned_after_crash_at, &consumers_lines);
    assert_eq!(
        consumed_after_last_rebalance,
        (0..PARTITIONS).collect(),
        "only these partitions were consumed after the last rebalance\n{stderr}"
    );
}

/// Waits until `members` consume a record of every partition at or after `change`, so that
/// [`pause_after`] can measure each partition's pause.
#[track_caller]
fn wait_until_every_partition_is_consumed_after(
    change: Instant,
    after: &str,
    members: &[&mut NamedMember],
) {
    let deadline = Instant::now() + CONSUME_TIMEOUT;

    loop {
        let consumers_lines = members
            .iter()
            .map(|member| member.consumer.stdout_lines())
            .collect::<Vec<_>>();
        let consumed = partitions_consumed_after(change, &consumers_lines);

        if consumed == (0..PARTITIONS).collect() {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "only partitions {consumed:?} were consumed within {CONSUME_TIMEOUT:?} after {after}"
        );

        thread::sleep(Duration::from_millis(100));
    }
}

/// Returns when the first of `consumers_lines` consumed a record.
fn first_record_consumed_at(consumers_lines: &[ConsumerLines]) -> Option<Instant> {
    consumers_lines
        .iter()
        .flat_map(ConsumerLines::consumer_events)
        .filter(|(_, event)| matches!(event, ConsumerEvent::RecordsConsumed { .. }))
        .map(|(read_at, _)| read_at)
        .min()
}

/// Returns when the last of `consumers_lines` reported that it started, just before it subscribed.
/// The time from there excludes how long each member's JVM took to start.
fn last_startup_at(consumers_lines: &[ConsumerLines]) -> Option<Instant> {
    consumers_lines
        .iter()
        .map(|lines| {
            lines
                .consumer_events()
                .into_iter()
                .find(|(_, event)| matches!(event, ConsumerEvent::StartupComplete))
                .map(|(read_at, _)| read_at)
        })
        .collect::<Option<Vec<_>>>()
        .and_then(|started| started.into_iter().max())
}

fn describe(duration: Option<Duration>) -> String {
    duration.map_or("never".to_owned(), |duration| format!("{duration:?}"))
}

/// A group of consumers with the client's default session timeout must pause no longer than it
/// would on Kafka's own broker, when it forms, when a member leaves cleanly and when a member
/// joins. Kafka holds the first join of a new group only for `group.initial.rebalance.delay.ms`,
/// extended while new members join, and ends a later rebalance as soon as every member has
/// rejoined. A broker that holds each join for a part of the session timeout pauses the group for
/// many seconds at each change.
#[test]
fn group_rebalance_follows_kafka_timing() {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(PARTITIONS, &[]);
    let group = unique::name("group");

    let producer = cli
        .verifiable_producer()
        .start(&topic, RECORDS_PER_SECOND, 0);

    let start_member = |name: &'static str| NamedMember {
        name,
        consumer: cli
            .verifiable_consumer()
            .start(&topic, &group, DEFAULT_SESSION_TIMEOUT, None),
    };

    let mut staying = start_member("staying");
    let mut leaving = start_member("leaving");
    let mut other = start_member("other");

    wait_until_reassigned(
        "three members started",
        &mut [&mut staying, &mut leaving, &mut other],
        &[0, 0, 0],
    );
    let assigned_at = Instant::now();
    wait_until_every_partition_is_consumed_after(
        assigned_at,
        "three members started",
        &[&mut staying, &mut leaving, &mut other],
    );
    let started_lines = [
        staying.consumer.stdout_lines(),
        leaving.consumer.stdout_lines(),
        other.consumer.stdout_lines(),
    ];
    let last_started_at = last_startup_at(&started_lines);
    let first_record_at = first_record_consumed_at(&started_lines);

    // Each pause is measured up to records that the members consumed after they got their new
    // partitions. A record consumed before that comes from a partition that a member held before
    // the rebalance, so it can't show how long the rebalance paused the group.
    let line_counts_before_leave = line_counts(&[&mut staying, &mut other]);
    let left_at = Instant::now();
    let (left_output, left_lines) = leaving.consumer.terminate();
    wait_until_reassigned(
        "the leaving member left",
        &mut [&mut staying, &mut other],
        &line_counts_before_leave,
    );
    let reassigned_after_leave_at = Instant::now();
    wait_until_every_partition_is_consumed_after(
        reassigned_after_leave_at,
        "the leaving member left",
        &[&mut staying, &mut other],
    );

    let line_counts_before_join = line_counts(&[&mut staying, &mut other]);
    let joined_at = Instant::now();
    let mut joining = start_member("joining");
    wait_until_reassigned(
        "a member joined",
        &mut [&mut staying, &mut other, &mut joining],
        &[line_counts_before_join[0], line_counts_before_join[1], 0],
    );
    let reassigned_after_join_at = Instant::now();
    wait_until_every_partition_is_consumed_after(
        reassigned_after_join_at,
        "a member joined",
        &[&mut staying, &mut other, &mut joining],
    );
    let measured_until = Instant::now();

    let consumers_lines = vec![
        left_lines,
        staying.consumer.stdout_lines(),
        other.consumer.stdout_lines(),
        joining.consumer.stdout_lines(),
    ];
    let (staying_output, _) = staying.consumer.terminate();
    let (other_output, _) = other.consumer.terminate();
    let (joining_output, _) = joining.consumer.terminate();
    drop(producer.terminate());

    let stderr = members_stderr(&[
        ("leaving", &left_output),
        ("staying", &staying_output),
        ("other", &other_output),
        ("joining", &joining_output),
    ]);

    let first_record_after =
        last_started_at
            .zip(first_record_at)
            .map(|(last_started_at, first_record_at)| {
                first_record_at.saturating_duration_since(last_started_at)
            });
    let pause_after_leave = pause_after(left_at, joined_at, &consumers_lines);
    let pause_after_join = pause_after(joined_at, measured_until, &consumers_lines);

    println!(
        "the first record was consumed {} after the last member started",
        describe(first_record_after)
    );
    print_pause("a member left", pause_after_leave);
    print_pause("a member joined", pause_after_join);

    assert!(
        first_record_after
            .is_some_and(|first_record_after| first_record_after <= FIRST_RECORD_BOUND),
        "the first record was consumed {} after the last member started, allowed \
         {FIRST_RECORD_BOUND:?}\n{stderr}",
        describe(first_record_after)
    );

    assert!(
        pause_after_leave.is_some_and(|pause| pause <= PAUSE_BOUND),
        "consuming paused for {} after a member left, allowed {PAUSE_BOUND:?}\n{stderr}",
        describe(pause_after_leave)
    );

    assert!(
        pause_after_join.is_some_and(|pause| pause <= PAUSE_BOUND),
        "consuming paused for {} after a member joined, allowed {PAUSE_BOUND:?}\n{stderr}",
        describe(pause_after_join)
    );
}

/// A member that stops cleanly sends `LeaveGroup`, and the broker must rebalance the group at once,
/// so that the other members take its partitions. A broker that ignores `LeaveGroup` removes the
/// member only when its session times out, and its partitions go unconsumed until then, every time
/// a consumer restarts. The remaining member must get every partition before the leaving member's
/// session could time out.
#[test]
fn member_that_leaves_cleanly_is_removed_before_its_session_times_out() {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(2, &[]);
    let group = unique::name("group");

    let (mut staying, _) = cli.verifiable_consumer().join_group(
        &topic,
        &group,
        LEAVE_GROUP_SESSION_TIMEOUT,
        None,
        JOIN_TIMEOUT,
    );
    let staying_lines_before_second_join = staying.line_count();
    let (leaving, _) = cli.verifiable_consumer().join_group(
        &topic,
        &group,
        LEAVE_GROUP_SESSION_TIMEOUT,
        None,
        JOIN_TIMEOUT,
    );

    if let Err(reason) =
        staying.wait_for_partitions_assigned(staying_lines_before_second_join, JOIN_TIMEOUT)
    {
        panic!("the first member didn't get its partitions after the second joined: {reason}");
    }

    let staying_lines_before_leave = staying.line_count();
    let left_at = Instant::now();
    let (left_output, _) = leaving.terminate();

    let reassigned = staying
        .wait_for_partitions_assigned(staying_lines_before_leave, LEAVE_GROUP_SESSION_TIMEOUT);
    println!(
        "the remaining member got its partitions {:?} after the other member left",
        left_at.elapsed()
    );

    let (staying_output, _) = staying.terminate();

    let partitions = reassigned.unwrap_or_else(|reason| {
        panic!(
            "the remaining member didn't get the partitions within the session timeout of \
             {LEAVE_GROUP_SESSION_TIMEOUT:?}: {reason}\nthe leaving member: {left_output}"
        )
    });

    assert_eq!(
        partition_indexes(&partitions),
        BTreeSet::from([0, 1]),
        "the remaining member didn't get every partition: {staying_output}"
    );
}

/// A static member that restarts within its session timeout must get its partitions back
/// without a rebalance. Nobody reads the restarting member's partitions while it is down, but the
/// other members keep reading their own partitions and keep them, where a rebalance would make
/// each of them revoke its partitions and pause. The broker must recognise the member's
/// `group.instance.id` when it rejoins, and give it its old assignment without starting a
/// rebalance.
#[test]
fn static_member_restart_keeps_its_partitions_without_a_rebalance() {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(2, &[]);
    let group = unique::name("group");
    let (staying_id, restarting_id) = (unique::name("staying"), unique::name("restarting"));

    let start_static_member = |group_instance_id: &str| {
        cli.verifiable_consumer().join_group(
            &topic,
            &group,
            STATIC_MEMBER_SESSION_TIMEOUT,
            Some(group_instance_id),
            JOIN_TIMEOUT,
        )
    };

    let producer = cli
        .verifiable_producer()
        .start(&topic, RECORDS_PER_SECOND, 0);

    let (mut staying, _) = start_static_member(&staying_id);

    let staying_lines_before_second_join = staying.line_count();
    let (restarting, partitions_before_restart) = start_static_member(&restarting_id);

    if let Err(reason) =
        staying.wait_for_partitions_assigned(staying_lines_before_second_join, JOIN_TIMEOUT)
    {
        panic!(
            "the first static member didn't get its partitions after the second joined: {reason}"
        );
    }

    let staying_lines_before_restart = staying.line_count();
    let restart_started_at = Instant::now();
    drop(restarting.terminate());

    let (restarted, partitions_after_restart) = start_static_member(&restarting_id);
    let rejoined_at = Instant::now();

    thread::sleep(AFTER_STATIC_MEMBER_REJOINS);

    // The test reads the staying member's lines before it stops the member, because the member
    // revokes its partitions when it closes.
    let staying_events = staying
        .stdout_lines()
        .since(staying_lines_before_restart)
        .consumer_events();

    let (restarted_output, _) = restarted.terminate();
    let (staying_output, _) = staying.terminate();
    drop(producer.terminate());

    assert_eq!(
        partitions_after_restart
            .into_iter()
            .collect::<BTreeSet<TopicPartition>>(),
        partitions_before_restart.into_iter().collect(),
        "the restarted member got other partitions: {restarted_output}"
    );

    let assignment_changes = staying_events
        .iter()
        .filter(|(_, event)| {
            matches!(
                event,
                ConsumerEvent::PartitionsRevoked { .. } | ConsumerEvent::PartitionsAssigned { .. }
            )
        })
        .collect::<Vec<_>>();
    assert!(
        assignment_changes.is_empty(),
        "the other member's partitions moved while the static member restarted: \
         {assignment_changes:?}\n{staying_output}"
    );

    let consumed_during_restart = staying_events.iter().any(|(read_at, event)| {
        matches!(event, ConsumerEvent::RecordsConsumed { .. })
            && (restart_started_at..=rejoined_at).contains(read_at)
    });
    assert!(
        consumed_during_restart,
        "the other member didn't consume while the static member restarted: {staying_output}"
    );
}

/// A static member that stays away longer than its session timeout must lose its place in the
/// group: the broker removes it and gives its partitions to the other members, which would
/// otherwise go unconsumed until the member returns. When the member returns with the same
/// `group.instance.id`, the broker must take it back in a rebalance that splits the partitions
/// between the members again.
#[test]
fn static_member_is_removed_when_its_session_times_out() {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(2, &[]);
    let group = unique::name("group");
    let (staying_id, returning_id) = (unique::name("staying"), unique::name("returning"));

    let join_as = |group_instance_id: &str| {
        cli.verifiable_consumer().join_group(
            &topic,
            &group,
            SHORT_SESSION_TIMEOUT,
            Some(group_instance_id),
            JOIN_TIMEOUT,
        )
    };

    let (mut staying, _) = join_as(&staying_id);
    let staying_lines_before_second_join = staying.line_count();
    let (returning, _) = join_as(&returning_id);

    if let Err(reason) =
        staying.wait_for_partitions_assigned(staying_lines_before_second_join, JOIN_TIMEOUT)
    {
        panic!(
            "the first static member didn't get its partitions after the second joined: {reason}"
        );
    }

    // The test kills the member, so that only the end of its session can remove it from the group.
    let staying_lines_before_kill = staying.line_count();
    drop(returning.kill());

    let partitions_after_expiry = staying
        .wait_for_partitions_assigned(staying_lines_before_kill, JOIN_TIMEOUT)
        .unwrap_or_else(|reason| {
            panic!(
                "the remaining member didn't get partitions after the other member's session \
                 timed out: {reason}"
            )
        });
    assert_eq!(
        partition_indexes(&partitions_after_expiry),
        BTreeSet::from([0, 1]),
        "the remaining member didn't get every partition after the other member's session timed out"
    );

    let staying_lines_before_return = staying.line_count();
    let (returned, partitions_of_returned) = join_as(&returning_id);
    let staying_partitions_after_return = staying
        .wait_for_partitions_assigned(staying_lines_before_return, JOIN_TIMEOUT)
        .unwrap_or_else(|reason| {
            panic!(
                "the remaining member wasn't reassigned when the other member returned: {reason}"
            )
        });

    let (returned_output, _) = returned.terminate();
    let (staying_output, _) = staying.terminate();

    let returned_partitions = partition_indexes(&partitions_of_returned);
    let staying_partitions = partition_indexes(&staying_partitions_after_return);
    assert!(
        !returned_partitions.is_empty()
            && returned_partitions.is_disjoint(&staying_partitions)
            && returned_partitions.union(&staying_partitions).count() == 2,
        "the members' partitions after the return don't split both partitions between them: \
         returned {returned_partitions:?}, staying {staying_partitions:?}\n{returned_output}\n\
         {staying_output}"
    );
}

fn partition_indexes(partitions: &[TopicPartition]) -> BTreeSet<u32> {
    partitions
        .iter()
        .map(|partition| partition.partition)
        .collect()
}
