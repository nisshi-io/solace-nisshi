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

//! Tests that a broker under load keeps every record that it acknowledged, when it restarts and
//! when its storage has an outage.
//!
//! A broker stopped with SIGTERM or killed with SIGKILL, and started again on the same storage,
//! must still have every record and committed offset that it acknowledged. During a storage
//! outage, clients must get errors within their timeouts instead of hanging. The broker must
//! neither panic nor exit, and it must serve again after the outage, without a restart.
//!
//! The restart tests run on PostgreSQL and SQLite, and the outage tests on PostgreSQL. Memory
//! storage loses its data on a restart by design.

/// The clients and checks that the restart tests and the outage tests share.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
mod support {
    use std::{collections::HashSet, time::Duration};

    use nisshi_smoke_test::{
        Acks, DescribeGroup, KafkaCli, Output, StoredRecord, VerifiableConsume, polling, unique,
    };

    /// The number of partitions in each test's topic. With more than one, the checks cover each
    /// partition's own numbering.
    pub(super) const PARTITIONS_PER_TOPIC: u32 = 3;

    // kafka-get-offsets sorts the partitions by name, and the checks index its offsets by partition
    // number. The two orders match only below 10 partitions.
    const _: () = assert!(PARTITIONS_PER_TOPIC < 10);

    /// How long a client waits for an answer before it fails. A producer waits
    /// `2 * CLIENT_TIMEOUT` for a record to be acknowledged. A client that runs past these limits
    /// has hung.
    pub(super) const CLIENT_TIMEOUT: Duration = Duration::from_secs(5);

    /// How long a test waits for a consumer to join its group and commit.
    pub(super) const CONSUMER_COMMIT_WAIT: Duration = Duration::from_secs(25);

    /// The session timeout of the consumers, the lowest that a Kafka broker accepts by default.
    pub(super) const CONSUMER_SESSION_TIMEOUT: Duration = Duration::from_secs(6);

    /// How many records [`assert_group_resumes_where_it_committed`] produces, for the group to read
    /// when it rejoins.
    const RECORDS_FOR_RETURNING_GROUP: usize = 30;

    /// The value prefix of the records that [`assert_group_resumes_where_it_committed`] produces.
    const RETURNING_GROUP_VALUE_PREFIX: u32 = 2;

    /// Returns the latest offset of each partition of `topic`, indexed by partition.
    #[track_caller]
    fn latest_offsets(cli: &KafkaCli, topic: &str) -> Vec<i64> {
        let latest = cli.get_offsets().partition_offsets(topic, "latest");
        assert_eq!(
            latest.len(),
            PARTITIONS_PER_TOPIC as usize,
            "latest offsets: {latest:?}"
        );
        latest
    }

    /// Reads `count` records of `topic` as a member of `group`, and returns the consumer's output
    /// and the records it read, sorted.
    #[track_caller]
    fn consume_sorted(
        cli: &KafkaCli,
        topic: &str,
        group: &str,
        count: usize,
    ) -> (Output<VerifiableConsume>, Vec<StoredRecord>) {
        // A consumer that misses records never reaches `count`, and times out. The check skips
        // `succeeded()` for it, so that the caller's offset check reports the missing offsets
        // instead of a bare timeout.
        let read = cli.verifiable_consumer().consume(topic, group, count);
        if read.timeout.is_none() {
            _ = read.succeeded();
        }

        let mut records = read.consumed_records();
        records.sort();
        (read, records)
    }

    /// Returns the offsets of the `records` on `partition`, in the order of `records`.
    fn partition_offsets(records: &[StoredRecord], partition: u32) -> Vec<i64> {
        records
            .iter()
            .filter(|record| record.partition == partition)
            .map(|record| record.offset)
            .collect()
    }

    /// Waits until `group` has committed an offset above 0 on a partition of `topic`. Panics after
    /// [`CONSUMER_COMMIT_WAIT`].
    #[track_caller]
    pub(super) fn wait_for_commit(cli: &KafkaCli, group: &str, topic: &str) {
        polling::wait_until(CONSUMER_COMMIT_WAIT, || {
            let described = cli.consumer_groups().describe(group);

            if (0..PARTITIONS_PER_TOPIC).any(|partition| {
                described
                    .group_offsets(group, topic, partition)
                    .is_some_and(|offsets| offsets.committed > 0)
            }) {
                Ok(())
            } else {
                Err(format!("{group} hasn't committed: {described}"))
            }
        });
    }

    /// Waits until `group` has committed the latest offset of each partition of `topic` that holds
    /// records. Panics after [`CONSUMER_COMMIT_WAIT`]. Nothing may produce to `topic` while it
    /// waits, because it reads the latest offsets once.
    #[cfg(feature = "postgres")]
    pub(super) fn wait_until_group_caught_up(cli: &KafkaCli, group: &str, topic: &str) {
        let latest = latest_offsets(cli, topic);

        polling::wait_until(CONSUMER_COMMIT_WAIT, || {
            let described = cli.consumer_groups().describe(group);

            let behind = (0..PARTITIONS_PER_TOPIC)
                .zip(&latest)
                .filter(|(partition, latest)| {
                    **latest > 0
                        && described
                            .group_offsets(group, topic, *partition)
                            .is_none_or(|offsets| offsets.committed < **latest as u64)
                })
                .collect::<Vec<_>>();

            if behind.is_empty() {
                Ok(())
            } else {
                Err(format!("{group} is behind on {behind:?}: {described}"))
            }
        });
    }

    /// Reads every record of `topic` from the beginning, and asserts that the broker kept each
    /// record of `acknowledged` at the partition and offset that it acknowledged. Each partition's
    /// offsets must be contiguous from 0 to its latest offset. Each value must appear once. Returns
    /// the latest offsets.
    ///
    /// The log can also hold a record that the broker never acknowledged, because the broker can
    /// store a record and fail before it answers.
    #[track_caller]
    pub(super) fn assert_log_keeps(
        cli: &KafkaCli,
        topic: &str,
        acknowledged: &[StoredRecord],
    ) -> Vec<i64> {
        let latest = latest_offsets(cli, topic);
        let total = latest.iter().sum::<i64>() as usize;

        let (read, records) = consume_sorted(cli, topic, &unique::name("group"), total);

        for (partition, latest) in (0..PARTITIONS_PER_TOPIC).zip(&latest) {
            assert_eq!(
                partition_offsets(&records, partition),
                (0..*latest).collect::<Vec<_>>(),
                "partition {partition}'s offsets don't run from 0 to its latest offset {latest} \
                 without a gap: {read}"
            );
        }

        let mut values = HashSet::new();
        let repeated = records
            .iter()
            .filter(|record| !values.insert(record.value.as_str()))
            .collect::<Vec<_>>();
        assert!(repeated.is_empty(), "values stored twice: {repeated:?}");

        let lost = acknowledged
            .iter()
            .filter(|record| records.binary_search(record).is_err())
            .collect::<Vec<_>>();
        assert!(
            lost.is_empty(),
            "{} of {} acknowledged records aren't where the producer was told: {lost:?}",
            lost.len(),
            acknowledged.len()
        );

        latest
    }

    /// Asserts that the broker kept `group`'s committed offsets, and that a consumer that rejoins
    /// `group` reads from them, re-reading and skipping nothing. `acknowledged` holds the records
    /// that the broker acknowledged to the test's producers. `latest_before` holds the latest
    /// offsets of `topic`, read after the test's producers stopped. `described` describes `group`,
    /// read after `group_consumer` stopped.
    #[track_caller]
    pub(super) fn assert_group_resumes_where_it_committed(
        cli: &KafkaCli,
        topic: &str,
        group: &str,
        acknowledged: &[StoredRecord],
        group_consumer: &Output<VerifiableConsume>,
        latest_before: &[i64],
        described: &Output<DescribeGroup>,
    ) {
        // These records give the rejoining group something to read, even when it committed every
        // earlier record. The broker must number each of them after the records it already has.
        let produced = cli.verifiable_producer().produce(
            topic,
            RECORDS_FOR_RETURNING_GROUP,
            Acks::FullIsr,
            RETURNING_GROUP_VALUE_PREFIX,
        );
        let produced = produced.acknowledged_records();
        assert_eq!(produced.len(), RECORDS_FOR_RETURNING_GROUP, "{produced:?}");

        let renumbered = produced
            .iter()
            .filter(|record| record.offset < latest_before[record.partition as usize])
            .collect::<Vec<_>>();
        assert!(
            renumbered.is_empty(),
            "the broker gave new records offsets below the latest offsets {latest_before:?}: \
             {renumbered:?}"
        );

        let consumer_commits = group_consumer.last_acknowledged_commits_before(i64::MAX);

        // The rejoining consumer starts each partition at its stored committed offset, or at 0 when
        // it has none. The stored offset must be at least the last commit that the broker
        // acknowledged to the consumer.
        let committed = (0..PARTITIONS_PER_TOPIC)
            .map(|partition| {
                let stored = described
                    .succeeded()
                    .group_offsets(group, topic, partition)
                    .map(|offsets| offsets.committed as i64);

                match (consumer_commits.get(&partition), stored) {
                    (None, stored) => stored.unwrap_or(0),
                    (Some(acknowledged), Some(stored)) if stored >= *acknowledged => stored,
                    (Some(acknowledged), stored) => panic!(
                        "partition {partition}'s committed offset {stored:?} is below the commit \
                         {acknowledged} that the broker acknowledged: {described}"
                    ),
                }
            })
            .collect::<Vec<_>>();

        let mut read_by_group = group_consumer.consumed_records();
        read_by_group.sort();

        let committed_unread = acknowledged
            .iter()
            .filter(|record| record.offset < committed[record.partition as usize])
            .filter(|record| read_by_group.binary_search(record).is_err())
            .collect::<Vec<_>>();
        assert!(
            committed_unread.is_empty(),
            "the group committed offsets {committed:?} past records its consumer never read: \
             {committed_unread:?}"
        );

        let latest = latest_offsets(cli, topic);
        let unread = latest
            .iter()
            .zip(&committed)
            .map(|(latest, committed)| latest - committed)
            .sum::<i64>() as usize;

        let (resumed, records) = consume_sorted(cli, topic, group, unread);

        for partition in 0..PARTITIONS_PER_TOPIC {
            let (from, to) = (committed[partition as usize], latest[partition as usize]);

            assert_eq!(
                partition_offsets(&records, partition),
                (from..to).collect::<Vec<_>>(),
                "the group didn't read partition {partition} from its committed offset {from} to \
                 its latest offset {to}: {resumed}"
            );
        }

        let missing = acknowledged
            .iter()
            .chain(&produced)
            .filter(|record| record.offset >= committed[record.partition as usize])
            .filter(|record| records.binary_search(record).is_err())
            .collect::<Vec<_>>();
        assert!(
            missing.is_empty(),
            "the group didn't read these acknowledged records: {missing:?}"
        );
    }
}

/// A broker that restarts while a producer sends to it.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
mod restart_under_load {
    use std::{thread, time::Duration};

    use nisshi_smoke_test::{Broker, KafkaCli, unique};

    use super::support::{
        CLIENT_TIMEOUT, CONSUMER_SESSION_TIMEOUT, PARTITIONS_PER_TOPIC,
        assert_group_resumes_where_it_committed, assert_log_keeps, wait_for_commit,
    };

    /// How many records a second the producer sends.
    const PRODUCER_RECORDS_PER_SECOND: u32 = 100;

    /// How long the producer keeps sending after the broker starts again.
    const PRODUCING_AFTER_RESTART: Duration = Duration::from_secs(15);

    const PRODUCER_VALUE_PREFIX: u32 = 0;

    /// After the broker is killed and started again, it must hold each record that it acknowledged,
    /// exactly once, and each offset that it acknowledged as committed.
    #[test]
    fn acknowledged_records_and_committed_offsets_survive_kill() {
        assert_restart_keeps_acknowledged_data(Broker::kill_and_restart);
    }

    /// A broker that gets SIGTERM while a producer sends must exit with 0 in time, as for a rolling
    /// deploy. After it starts again, it must hold each record that it acknowledged, exactly once,
    /// and each offset that it acknowledged as committed.
    #[test]
    fn acknowledged_records_and_committed_offsets_survive_graceful_restart() {
        assert_restart_keeps_acknowledged_data(Broker::restart);
    }

    /// Restarts the broker with `restart` while a producer sends to it, and asserts that the
    /// broker kept every record and committed offset that it acknowledged. `restart` panics if
    /// the broker doesn't stop as it requires.
    #[track_caller]
    fn assert_restart_keeps_acknowledged_data(restart: fn(Broker) -> Broker) {
        let broker = Broker::isolated();
        let cli = KafkaCli::new(broker.bootstrap()).with_client_timeouts(CLIENT_TIMEOUT);
        let topic = cli.topics().create_unique(PARTITIONS_PER_TOPIC, &[]);
        let group = unique::name("group");

        let producer = cli.verifiable_producer().start(
            &topic,
            PRODUCER_RECORDS_PER_SECOND,
            PRODUCER_VALUE_PREFIX,
        );

        // The test stops the consumer before the restart, instead of letting it run across the
        // restart. A consumer that runs on would commit again after the restart, and the new
        // commits would hide a commit that the restart lost.
        let consumer =
            cli.verifiable_consumer()
                .start(&topic, &group, CONSUMER_SESSION_TIMEOUT, None);
        wait_for_commit(&cli, &group, &topic);
        let consumed = consumer.stop();

        let commits = consumed.last_acknowledged_commits_before(i64::MAX);
        assert!(
            !commits.is_empty(),
            "the consumer didn't commit: {consumed}"
        );

        let broker = restart(broker);
        let described = cli.consumer_groups().describe(&group);

        for (partition, acknowledged_commit) in commits {
            let offsets = described
                .succeeded()
                .group_offsets(&group, &topic, partition)
                .unwrap_or_else(|| panic!("partition {partition} has no offsets: {described}"));

            // The broker can store a commit that the consumer reported as failed. An offset can
            // therefore be past the last acknowledged commit, but not past the log end.
            assert!(
                offsets.committed >= acknowledged_commit as u64 && offsets.committed <= offsets.end,
                "partition {partition}'s committed offset {} isn't between the last acknowledged \
                 commit {acknowledged_commit} and the log end {}: {described}",
                offsets.committed,
                offsets.end
            );
        }

        thread::sleep(PRODUCING_AFTER_RESTART);
        let produced = producer.stop();

        let acknowledged_under_load = produced.acknowledged_records();
        assert!(
            !acknowledged_under_load.is_empty(),
            "the broker acknowledged no record: {produced}"
        );
        let latest = assert_log_keeps(&cli, &topic, &acknowledged_under_load);

        assert_group_resumes_where_it_committed(
            &cli,
            &topic,
            &group,
            &acknowledged_under_load,
            &consumed,
            &latest,
            &described,
        );

        if let Err(reason) = broker.stop() {
            panic!("{reason}");
        }
    }
}

/// A broker whose PostgreSQL server goes away and comes back.
#[cfg(feature = "postgres")]
mod postgres_outage {
    use std::{collections::HashSet, thread, time::Duration};

    use nisshi_smoke_test::{
        Acks, Broker, KafkaCli, LaunchOptions, PostgresServer, SendResult, unique,
    };

    use super::support::{
        CLIENT_TIMEOUT, CONSUMER_SESSION_TIMEOUT, PARTITIONS_PER_TOPIC,
        assert_group_resumes_where_it_committed, assert_log_keeps, wait_for_commit,
        wait_until_group_caught_up,
    };

    /// How long each storage outage lasts.
    const OUTAGE_DURATION: Duration = Duration::from_secs(15);

    /// How long the producer sends before the outage starts.
    const PRODUCING_BEFORE_OUTAGE: Duration = Duration::from_secs(5);

    /// How long the producer sends after the outage ends.
    const PRODUCING_AFTER_OUTAGE: Duration = Duration::from_secs(15);

    /// How many records a second the producer sends around the outage.
    const OUTAGE_PRODUCER_RECORDS_PER_SECOND: u32 = 20;

    /// How long the producer sends in all.
    const OUTAGE_PRODUCER_SENDS_FOR: Duration = Duration::from_secs(
        PRODUCING_BEFORE_OUTAGE.as_secs()
            + OUTAGE_DURATION.as_secs()
            + PRODUCING_AFTER_OUTAGE.as_secs(),
    );

    /// How long the producer has to finish. It sends its last record after
    /// [`OUTAGE_PRODUCER_SENDS_FOR`], and each record times out within `2 * CLIENT_TIMEOUT`.
    const OUTAGE_PRODUCER_TIMEOUT: Duration = Duration::from_secs(120);

    /// How many records the test produces before it starts the consumer and the producer. The
    /// broker must acknowledge each of them.
    const RECORDS_BEFORE_OUTAGE: usize = 100;

    /// The value prefix of the [`RECORDS_BEFORE_OUTAGE`] records.
    const BEFORE_OUTAGE_VALUE_PREFIX: u32 = 0;

    /// The value prefix of the records that the producer sends around the outage.
    const DURING_OUTAGE_VALUE_PREFIX: u32 = 1;

    /// How a test takes the PostgreSQL server away.
    #[derive(Clone, Copy, Debug)]
    enum Outage {
        /// The server shuts down, closes its connections and refuses new ones.
        Stop,
        /// The server keeps its connections open and stops answering on them.
        Pause,
    }

    impl Outage {
        #[track_caller]
        fn start(self, postgres: &PostgresServer) {
            match self {
                Self::Stop => postgres.stop(),
                Self::Pause => postgres.pause(),
            }
        }

        #[track_caller]
        fn end(self, postgres: &PostgresServer) {
            match self {
                Self::Stop => postgres.start_again(),
                Self::Pause => postgres.unpause(),
            }
        }
    }

    /// Runs a producer and a consumer group on `topic` while `outage` takes `postgres` away for
    /// [`OUTAGE_DURATION`]. Then asserts that:
    ///
    /// - the producer reported a result for every record, so every send ended;
    /// - a record failed during the outage;
    /// - the broker acknowledged a record after the outage, and the consumer read one;
    /// - the broker kept every acknowledged record;
    /// - the group resumes at its committed offsets.
    ///
    /// `cli` must come from [`KafkaCli::with_client_timeouts`] with [`CLIENT_TIMEOUT`], so that
    /// each record times out within [`OUTAGE_PRODUCER_TIMEOUT`].
    #[track_caller]
    fn assert_clients_ride_out_outage(
        cli: &KafkaCli,
        topic: &str,
        postgres: &PostgresServer,
        outage: Outage,
    ) {
        let before = cli.verifiable_producer().produce(
            topic,
            RECORDS_BEFORE_OUTAGE,
            Acks::FullIsr,
            BEFORE_OUTAGE_VALUE_PREFIX,
        );
        let mut acknowledged = before.acknowledged_records();
        assert_eq!(acknowledged.len(), RECORDS_BEFORE_OUTAGE, "{before}");

        let group = unique::name("group");

        let records = OUTAGE_PRODUCER_RECORDS_PER_SECOND as usize
            * OUTAGE_PRODUCER_SENDS_FOR.as_secs() as usize;

        let consumer =
            cli.verifiable_consumer()
                .start(topic, &group, CONSUMER_SESSION_TIMEOUT, None);
        wait_for_commit(cli, &group, topic);

        let (produced, outage_started_at, outage_ended_at) = thread::scope(|scope| {
            let producer = scope.spawn(|| {
                cli.verifiable_producer().produce_throttled(
                    topic,
                    records,
                    OUTAGE_PRODUCER_RECORDS_PER_SECOND,
                    DURING_OUTAGE_VALUE_PREFIX,
                    OUTAGE_PRODUCER_TIMEOUT,
                )
            });

            // The producer stamps its results with the clock of the tools' container, so the test
            // reads the outage's times from that clock. Each read takes a `docker exec`, so the
            // start time is a little early and the end time a little late. A failure just before
            // the outage can therefore count as a failure during it. A record counts as
            // acknowledged after the outage only if the broker acknowledged it after the end.
            thread::sleep(PRODUCING_BEFORE_OUTAGE);
            let outage_started_at = cli.now_in_millis();
            outage.start(postgres);
            thread::sleep(OUTAGE_DURATION);
            outage.end(postgres);
            let outage_ended_at = cli.now_in_millis();

            let produced = producer
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic));

            (produced, outage_started_at, outage_ended_at)
        });

        wait_until_group_caught_up(cli, &group, topic);
        let consumed = consumer.stop();

        let results = produced.succeeded().send_results_with_report_times();

        // The producer prints a result for each record when the broker answers or the record times
        // out. A record without a result hung.
        assert_eq!(
            Some(results.len()),
            produced.sent_count(),
            "a record the producer sent got no result: {produced}"
        );

        // A record sent just before the storage came back times out up to `2 * CLIENT_TIMEOUT`
        // later. The producer can therefore report its failure after the outage ended.
        let latest_outage_failure = outage_ended_at + 2 * CLIENT_TIMEOUT.as_millis() as i64;
        assert!(
            results.iter().any(|(reported_at, result)| {
                matches!(result, SendResult::Failed { .. })
                    && (outage_started_at..=latest_outage_failure).contains(reported_at)
            }),
            "no record failed while the storage was away, from {outage_started_at} to \
             {outage_ended_at}: {produced}"
        );

        let after_outage = results
            .iter()
            .filter_map(|(reported_at, result)| match result {
                SendResult::Acknowledged(record) if *reported_at > outage_ended_at => Some(record),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert!(
            !after_outage.is_empty(),
            "the broker acknowledged no record after the storage came back: {produced}"
        );

        let consumed_values = consumed
            .consumed_records()
            .into_iter()
            .map(|record| record.value)
            .collect::<HashSet<_>>();
        assert!(
            after_outage
                .iter()
                .any(|record| consumed_values.contains(&record.value)),
            "the consumer read no record acknowledged after the storage came back: {consumed}"
        );

        acknowledged.extend(produced.acknowledged_records());

        let latest = assert_log_keeps(cli, topic, &acknowledged);
        assert_group_resumes_where_it_committed(
            cli,
            topic,
            &group,
            &acknowledged,
            &consumed,
            &latest,
            &cli.consumer_groups().describe(&group),
        );
    }

    /// Starts a broker on a [`PostgresServer`] of its own, and runs
    /// [`assert_clients_ride_out_outage`] with `outage` on it.
    #[track_caller]
    fn assert_broker_recovers_from(outage: Outage) {
        let postgres = PostgresServer::start();
        let broker = Broker::launch(LaunchOptions::new(postgres.storage_url()));
        let cli = KafkaCli::new(broker.bootstrap()).with_client_timeouts(CLIENT_TIMEOUT);
        let topic = cli.topics().create_unique(PARTITIONS_PER_TOPIC, &[]);

        assert_clients_ride_out_outage(&cli, &topic, &postgres, outage);

        if let Err(reason) = broker.stop() {
            panic!("{reason}");
        }
    }

    #[test]
    fn clients_get_errors_and_the_broker_recovers_when_the_server_stops() {
        assert_broker_recovers_from(Outage::Stop);
    }

    /// A broker whose server stops answering can wait on a query in flight until the server answers
    /// again. Clients must still get errors within their timeouts, and the broker must serve again
    /// once the server answers.
    #[test]
    fn clients_get_errors_and_the_broker_recovers_when_the_server_stops_answering() {
        assert_broker_recovers_from(Outage::Pause);
    }
}
