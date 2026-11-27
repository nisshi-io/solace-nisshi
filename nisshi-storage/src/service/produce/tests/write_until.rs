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

//! [`Detached::write_until`] on a record of its own, so that no other test's write is in it.
//! Every test but one runs on a paused clock, so the time that a write and a wait take is exact.

use std::{assert_matches, time::Duration};

use tokio::{
    sync::oneshot,
    time::{Instant, sleep, sleep_until, timeout},
};

use crate::{
    StillRunning, Topition,
    service::produce::detached::{Detached, WriteOutcome},
};

/// How long after the call each write's `at` passes.
const TIMEOUT: Duration = Duration::from_secs(1);

/// The producer of the batch in every test that tracks an idempotent producer.
const PRODUCER_ID: i64 = 7;

/// The output of every write that returns one.
const OUTPUT: u32 = 42;

fn topition() -> Topition {
    Topition::new("orders", 0)
}

/// Returns an `on_detached` for [`Detached::write_until`], and the receiver of the output that it
/// gets. The receiver fails when the task ends without calling it.
fn on_detached() -> (impl FnOnce(u32) + Send + 'static, oneshot::Receiver<u32>) {
    let (sender, receiver) = oneshot::channel();

    (
        move |output| {
            _ = sender.send(output);
        },
        receiver,
    )
}

/// Returns each output that a write gave to its caller in `outcome` or to `on_detached`.
async fn outputs(outcome: WriteOutcome<u32>, unclaimed: oneshot::Receiver<u32>) -> Vec<u32> {
    let mut outputs = Vec::new();

    if let WriteOutcome::Ended(output) = outcome {
        outputs.push(output);
    }

    if let Ok(output) = unclaimed.await {
        outputs.push(output);
    }

    outputs
}

#[tokio::test(start_paused = true)]
async fn a_write_that_ends_before_at_returns_its_output() {
    let detached = Detached::new();
    let (on_detached, unclaimed) = on_detached();

    let outcome = detached
        .write_until(
            Instant::now() + TIMEOUT,
            [(topition(), PRODUCER_ID)],
            async { OUTPUT },
            on_detached,
        )
        .await;

    assert_matches!(outcome, WriteOutcome::Ended(42));
    assert!(unclaimed.await.is_err());
    assert!(!detached.contains(&topition(), PRODUCER_ID));
}

#[tokio::test(start_paused = true)]
async fn a_write_that_outlives_at_is_reported_until_it_ends() {
    let detached = Detached::new();
    let (release, released) = oneshot::channel::<()>();
    let (on_detached, unclaimed) = on_detached();

    let outcome = detached
        .write_until(
            Instant::now() + TIMEOUT,
            [
                (topition(), PRODUCER_ID),
                (Topition::new("orders", 1), PRODUCER_ID),
            ],
            async move {
                _ = released.await;
                OUTPUT
            },
            on_detached,
        )
        .await;

    assert_matches!(outcome, WriteOutcome::Detached);
    assert!(detached.contains(&topition(), PRODUCER_ID));
    assert!(detached.contains(&Topition::new("orders", 1), PRODUCER_ID));
    // The record is keyed by producer as well as by partition.
    assert!(!detached.contains(&topition(), PRODUCER_ID + 1));

    _ = release.send(());
    assert_eq!(Ok(42), unclaimed.await);

    assert!(!detached.contains(&topition(), PRODUCER_ID));
    assert!(!detached.contains(&Topition::new("orders", 1), PRODUCER_ID));
}

#[tokio::test(start_paused = true)]
async fn a_write_that_ends_at_at_gives_its_output_to_one_place() {
    let detached = Detached::new();
    let at = Instant::now() + TIMEOUT;
    let (on_detached, unclaimed) = on_detached();

    let outcome = detached
        .write_until(
            at,
            [
                (topition(), PRODUCER_ID),
                (Topition::new("orders", 1), PRODUCER_ID),
            ],
            async move {
                sleep_until(at).await;
                OUTPUT
            },
            on_detached,
        )
        .await;

    assert_eq!(vec![42], outputs(outcome, unclaimed).await);
    assert!(!detached.contains(&topition(), PRODUCER_ID));
    assert!(!detached.contains(&Topition::new("orders", 1), PRODUCER_ID));
}

/// The task and its caller run on separate threads, so that the write can end while the caller
/// detaches it. No run is certain to reach that order, so the test makes many writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_that_end_at_at_on_another_thread_give_their_output_to_one_place() {
    let detached = Detached::new();
    let partitions = [
        (topition(), PRODUCER_ID),
        (Topition::new("orders", 1), PRODUCER_ID),
        (Topition::new("orders", 2), PRODUCER_ID),
    ];

    for _ in 0..200 {
        let at = Instant::now() + Duration::from_millis(1);
        let (on_detached, unclaimed) = on_detached();

        let outcome = detached
            .write_until(
                at,
                partitions.clone(),
                async move {
                    sleep_until(at).await;
                    OUTPUT
                },
                on_detached,
            )
            .await;

        assert_eq!(vec![42], outputs(outcome, unclaimed).await);

        for (topition, producer_id) in &partitions {
            assert!(!detached.contains(topition, *producer_id));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_write_without_partitions_that_outlives_at_is_detached() {
    let detached = Detached::new();
    let (release, released) = oneshot::channel::<()>();
    let (on_detached, unclaimed) = on_detached();

    let outcome = detached
        .write_until(
            Instant::now() + TIMEOUT,
            Vec::new(),
            async move {
                _ = released.await;
                OUTPUT
            },
            on_detached,
        )
        .await;

    assert_matches!(outcome, WriteOutcome::Detached);

    _ = release.send(());
    assert_eq!(Ok(42), unclaimed.await);
}

#[tokio::test(start_paused = true)]
async fn a_write_whose_caller_stops_waiting_gives_its_output_to_on_detached() {
    let detached = Detached::new();
    let (release, released) = oneshot::channel::<()>();
    let (on_detached, unclaimed) = on_detached();

    let waiting = detached.write_until(
        Instant::now() + TIMEOUT,
        [(topition(), PRODUCER_ID)],
        async move {
            _ = released.await;
            OUTPUT
        },
        on_detached,
    );
    // The caller drops the future before `at`, as an aborted request does.
    assert!(timeout(TIMEOUT / 2, waiting).await.is_err());

    _ = release.send(());
    assert_eq!(Ok(42), unclaimed.await);
}

#[tokio::test(start_paused = true)]
async fn a_write_that_panics_before_at_returns_panicked() {
    let detached = Detached::new();
    let (on_detached, unclaimed) = on_detached();

    let outcome = detached
        .write_until(
            Instant::now() + TIMEOUT,
            [(topition(), PRODUCER_ID)],
            async { panic!("the write panics") },
            on_detached,
        )
        .await;

    assert_matches!(outcome, WriteOutcome::Panicked);
    assert!(unclaimed.await.is_err());
    assert!(!detached.contains(&topition(), PRODUCER_ID));
}

#[tokio::test(start_paused = true)]
async fn a_detached_write_that_panics_leaves_no_partition() {
    let detached = Detached::new();
    let (release, released) = oneshot::channel::<()>();
    let (on_detached, unclaimed) = on_detached();

    let outcome = detached
        .write_until(
            Instant::now() + TIMEOUT,
            [(topition(), PRODUCER_ID)],
            async move {
                _ = released.await;
                panic!("the write panics")
            },
            on_detached,
        )
        .await;

    assert_matches!(outcome, WriteOutcome::Detached);
    assert!(detached.contains(&topition(), PRODUCER_ID));

    _ = release.send(());
    assert!(unclaimed.await.is_err());

    assert_eq!(StillRunning::default(), detached.wait(TIMEOUT).await);
    assert!(!detached.contains(&topition(), PRODUCER_ID));
}

#[tokio::test(start_paused = true)]
async fn a_detached_write_of_a_producer_without_an_id_is_not_reported() {
    let detached = Detached::new();
    let (release, released) = oneshot::channel::<()>();

    let outcome = detached
        .write_until(
            Instant::now() + TIMEOUT,
            [(topition(), -1)],
            async move {
                _ = released.await;
            },
            |()| {},
        )
        .await;

    assert_matches!(outcome, WriteOutcome::Detached);
    assert!(!detached.contains(&topition(), -1));

    _ = release.send(());
    _ = detached.wait(TIMEOUT).await;
}

#[tokio::test(start_paused = true)]
async fn the_wait_at_shutdown_waits_for_a_detached_write() {
    let detached = Detached::new();
    let (release, released) = oneshot::channel::<()>();

    let outcome = detached
        .write_until(
            Instant::now() + TIMEOUT,
            [(topition(), PRODUCER_ID)],
            async move {
                _ = released.await;
            },
            |()| {},
        )
        .await;
    assert_matches!(outcome, WriteOutcome::Detached);

    // The write ends one second into the wait.
    drop(tokio::spawn(async move {
        sleep(Duration::from_secs(1)).await;
        _ = release.send(());
    }));
    let started_at = Instant::now();

    assert_eq!(
        StillRunning::default(),
        detached.wait(Duration::from_secs(5)).await
    );
    assert_eq!(Duration::from_secs(1), started_at.elapsed());
}
