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
//! Every test runs on a paused clock, so the time that a write and a wait take is exact.

use std::{assert_matches, time::Duration};

use tokio::{
    sync::oneshot,
    time::{Instant, sleep},
};

use crate::{
    StillRunning, Topition,
    service::produce::detached::{Detached, Detachment, WriteOutcome},
};

/// How long after the call each write's `at` passes.
const TIMEOUT: Duration = Duration::from_secs(1);

/// The producer of the batch in every test that tracks an idempotent producer.
const PRODUCER_ID: i64 = 7;

fn topition() -> Topition {
    Topition::new("orders", 0)
}

#[tokio::test(start_paused = true)]
async fn a_write_that_ends_before_at_returns_its_output() {
    let detached = Detached::new();

    let outcome = detached
        .write_until(
            Instant::now() + TIMEOUT,
            [(topition(), PRODUCER_ID)],
            |_| async { 42 },
        )
        .await;

    assert_matches!(outcome, WriteOutcome::Ended(42));
    assert!(!detached.contains(&topition(), PRODUCER_ID));
}

#[tokio::test(start_paused = true)]
async fn a_write_that_outlives_at_is_reported_until_it_ends() {
    let detached = Detached::new();
    let (release, released) = oneshot::channel::<()>();

    let outcome = detached
        .write_until(
            Instant::now() + TIMEOUT,
            [
                (topition(), PRODUCER_ID),
                (Topition::new("orders", 1), PRODUCER_ID),
            ],
            |_| async move {
                _ = released.await;
            },
        )
        .await;

    assert_matches!(outcome, WriteOutcome::Detached);
    assert!(detached.contains(&topition(), PRODUCER_ID));
    assert!(detached.contains(&Topition::new("orders", 1), PRODUCER_ID));
    // The record is keyed by producer as well as by partition.
    assert!(!detached.contains(&topition(), PRODUCER_ID + 1));

    _ = release.send(());
    assert_eq!(StillRunning::default(), detached.wait(TIMEOUT).await);

    assert!(!detached.contains(&topition(), PRODUCER_ID));
    assert!(!detached.contains(&Topition::new("orders", 1), PRODUCER_ID));
}

#[tokio::test(start_paused = true)]
async fn the_detachment_says_when_the_request_stops_waiting() {
    let detached = Detached::new();
    let (handed, detachment) = oneshot::channel::<Detachment>();
    let (release, released) = oneshot::channel::<()>();

    let outcome = detached
        .write_until(
            Instant::now() + TIMEOUT,
            [(topition(), PRODUCER_ID)],
            |detachment| {
                _ = handed.send(detachment);

                async move {
                    _ = released.await;
                }
            },
        )
        .await;
    let detachment = detachment.await.expect("the handle");

    assert_matches!(outcome, WriteOutcome::Detached);
    assert!(detachment.is_detached());

    _ = release.send(());
    _ = detached.wait(TIMEOUT).await;
}

#[tokio::test(start_paused = true)]
async fn the_detachment_says_the_request_waits_while_it_does() {
    let detached = Detached::new();

    let outcome = detached
        .write_until(
            Instant::now() + TIMEOUT,
            [(topition(), PRODUCER_ID)],
            |detachment| async move { detachment.is_detached() },
        )
        .await;

    assert_matches!(outcome, WriteOutcome::Ended(false));
}

#[tokio::test(start_paused = true)]
async fn a_write_that_panics_returns_panicked_and_leaves_no_partition() {
    let detached = Detached::new();

    let outcome = detached
        .write_until(
            Instant::now() + TIMEOUT,
            [(topition(), PRODUCER_ID)],
            |_| async { panic!("the write panics") },
        )
        .await;

    assert_matches!(outcome, WriteOutcome::<()>::Panicked);
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
            |_| async move {
                _ = released.await;
            },
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
            |_| async move {
                _ = released.await;
            },
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
