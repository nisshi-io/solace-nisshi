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

//! Tests that a consumer gets exactly the records it asks for, no more and no fewer: from a chosen
//! offset, from where its group left off, and from every topic that matches a pattern.

use std::{
    thread,
    time::{Duration, Instant},
};

use nisshi_smoke_test::{DescribeGroupState, GroupState, KafkaCli, Output, unique};

/// Waits until `group` is `Stable`, so its members have their assignments. Returns the group's
/// last state as an error if it isn't stable after a minute.
#[track_caller]
fn wait_until_stable(cli: &KafkaCli, group: &str) -> Result<(), Output<DescribeGroupState>> {
    let deadline = Instant::now() + Duration::from_secs(60);

    loop {
        let state = cli.consumer_groups().describe_state(group);

        if state.code == Some(0) && state.group_state(group) == Some(GroupState::Stable) {
            return Ok(());
        }

        if Instant::now() >= deadline {
            return Err(state);
        }

        thread::sleep(Duration::from_secs(1));
    }
}

/// A consumer told to start at offset 3 must get record 3 first, then every later record in order.
#[test]
fn reading_from_an_offset_starts_at_that_record() {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(1, &[]);

    _ = cli
        .console_producer()
        .produce_values(&topic, &["r0", "r1", "r2", "r3", "r4", "r5"]);

    assert_eq!(
        cli.console_consumer()
            .read_records(&topic, 0, 3, 3)
            .into_iter()
            .map(|record| (record.offset, record.value))
            .collect::<Vec<_>>(),
        [
            (3, "r3".to_owned()),
            (4, "r4".to_owned()),
            (5, "r5".to_owned())
        ]
    );
}

/// A consumer group commits its offsets as it consumes, so a consumer that stops and starts again
/// carries on after the last record it read, neither reading records twice nor skipping any.
#[test]
fn group_resumes_after_what_it_read() {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(1, &[]);
    let group = unique::name("group");

    _ = cli
        .console_producer()
        .produce_values(&topic, &["first-0", "first-1", "first-2"]);

    let first_run = cli.console_consumer().consume(&topic, &group, 3);
    assert_eq!(
        first_run.succeeded().consumed_values(),
        ["first-0", "first-1", "first-2"],
        "{first_run}"
    );

    _ = cli
        .console_producer()
        .produce_values(&topic, &["second-0", "second-1"]);

    let second_run = cli.console_consumer().consume(&topic, &group, 2);
    assert_eq!(
        second_run.succeeded().consumed_values(),
        ["second-0", "second-1"],
        "{second_run}"
    );
}

/// A consumer subscribed to a pattern must also read from a matching topic created after it
/// started. The consumer matches the pattern itself: it finds the new topic in the broker's
/// metadata and rejoins its group with the new topic in its subscription. The broker must
/// therefore list the new topic in `Metadata`, rebalance the stable group, and deliver the
/// leader's assignment of the new topic's partition to the consumer.
#[test]
fn pattern_subscription_reads_topic_created_later() {
    let cli = KafkaCli::shared();
    let topic_name_prefix = unique::name("include");
    let (early_topic, late_topic) = (
        format!("{topic_name_prefix}.early"),
        format!("{topic_name_prefix}.late"),
    );

    _ = cli.topics().create(&early_topic, 1, &[]).succeeded();
    _ = cli
        .console_producer()
        .produce_values(&early_topic, &["from-early"]);

    let group = unique::name("group");

    let consumed = thread::scope(|scope| {
        let consumer_thread = scope.spawn(|| {
            cli.console_consumer()
                .consume_matching(&format!(r"{topic_name_prefix}\..*"), &group, 2)
        });

        // The late topic must appear after the consumer subscribed, and a group is stable only
        // after its members have subscribed.
        let stable = wait_until_stable(&cli, &group);

        if stable.is_ok() {
            _ = cli.topics().create(&late_topic, 1, &[]).succeeded();
            _ = cli
                .console_producer()
                .produce_values(&late_topic, &["from-late"]);
        }

        // The test joins the consumer before it fails, so the failure shows why the group didn't
        // become stable.
        let consumed = consumer_thread.join().expect("consumer thread");

        if let Err(state) = stable {
            panic!("{group} isn't stable: {state}\nconsumer: {consumed}");
        }

        consumed
    });

    let mut values = consumed.succeeded().consumed_values();
    values.sort_unstable();

    assert_eq!(values, ["from-early", "from-late"], "{consumed}");
}
