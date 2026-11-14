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

//! The wait, at shutdown, for the writes that a request left running, against a
//! [`CountingStorage`](crate::service::counting_storage::CountingStorage) whose produce waits for
//! a gate. Every test runs on a paused clock, so the time that a wait takes is exact.
//!
//! The writes are tracked for the whole process, so each test counts on nextest running it in a
//! process of its own, where no other test has a write parked.

use std::{pin::pin, sync::Arc, time::Duration};

use nisshi_sans_io::ErrorCode;
use tokio::time::{Instant, sleep};

use super::request_deadline::{acked, codes, serve, service, stalled};
use crate::{Result, wait_for_detached_writes};

/// The grace that is left for the writes in every test.
const REMAINING_GRACE: Duration = Duration::from_secs(5);

/// A write that ends before the remaining grace does is waited for, and the wait ends with the
/// write.
#[tokio::test(start_paused = true)]
async fn a_write_that_ends_within_the_grace_is_waited_for() -> Result<()> {
    let (storage, gate) = stalled();
    let service = service(&storage);

    let responses = serve(&service, acked(&[0])?).await?;
    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&responses)?);
    assert_eq!(1, storage.parked());

    // The write ends one second into the grace.
    drop(tokio::spawn({
        let gate = Arc::clone(&gate);

        async move {
            sleep(Duration::from_secs(1)).await;
            gate.add_permits(1);
        }
    }));
    let started_at = Instant::now();

    let still_running = wait_for_detached_writes(REMAINING_GRACE).await;

    assert_eq!(0, still_running);
    assert_eq!(Duration::from_secs(1), started_at.elapsed());
    assert_eq!(1, storage.produced().len());

    Ok(())
}

/// A write that outlasts the remaining grace is left running, and the wait ends at the grace.
#[tokio::test(start_paused = true)]
async fn a_write_that_outlasts_the_grace_is_left_running() -> Result<()> {
    let (storage, _gate) = stalled();
    let service = service(&storage);

    let responses = serve(&service, acked(&[0])?).await?;
    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&responses)?);
    let started_at = Instant::now();

    let still_running = wait_for_detached_writes(REMAINING_GRACE).await;

    assert_eq!(1, still_running);
    assert_eq!(REMAINING_GRACE, started_at.elapsed());
    assert_eq!(1, storage.parked());
    assert!(storage.produced().is_empty());

    Ok(())
}

/// A write that starts while the wait runs is counted with the others, and it does not extend
/// the wait past the grace.
#[tokio::test(start_paused = true)]
async fn a_write_that_starts_during_the_wait_does_not_extend_it() -> Result<()> {
    let (storage, _gate) = stalled();
    let service = service(&storage);

    let responses = serve(&service, acked(&[0])?).await?;
    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&responses)?);
    let started_at = Instant::now();

    let mut wait = pin!(wait_for_detached_writes(REMAINING_GRACE));

    // The wait has run for a second when the second request arrives.
    tokio::select! {
        biased;
        _ = &mut wait => panic!("the wait ended before the second write started"),
        () = sleep(Duration::from_secs(1)) => {}
    }

    // The second request's deadline is past the grace, so the request is still waiting for its
    // write when the wait ends.
    let second = pin!(serve(&service, acked(&[1])?));

    let still_running = tokio::select! {
        still_running = &mut wait => still_running,
        _ = second => panic!("the second request answered before the wait ended"),
    };

    assert_eq!(2, still_running);
    assert_eq!(REMAINING_GRACE, started_at.elapsed());
    assert_eq!(2, storage.parked());

    Ok(())
}
