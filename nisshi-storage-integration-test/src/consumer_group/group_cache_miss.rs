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

//! A group request that finds no cached state for its group starts from an
//! empty group with no version. The storage's conditional write has to
//! reject that as outdated and hand back the stored group, or the request
//! drops every member (`UnknownMemberId`) or fails with a storage error.
//!
//! The coordinator finds nothing cached in two ways:
//!
//! - two requests for the same group overlap on the broker: while the first
//!   is writing the group back to storage, the second finds its cached state
//!   checked out;
//! - the broker restarts: a new coordinator over the same storage has
//!   nothing cached.

use std::time::Duration;

use anyhow::{Result, anyhow};
use nisshi_broker::coordinator::group::{Coordinator as _, administrator::Controller};
use nisshi_sans_io::{Body, ErrorCode, HeartbeatResponse};
use nisshi_storage::Storage;
use tokio::time::sleep;

use crate::group::{self, Formed};

fn heartbeat_error(body: &Body) -> Result<i16> {
    match body {
        Body::HeartbeatResponse(HeartbeatResponse { error_code, .. }) => Ok(*error_code),
        otherwise => Err(anyhow!("expecting heartbeat response: {otherwise:?}")),
    }
}

fn assert_heartbeats_ok<'a>(
    outcomes: impl IntoIterator<Item = (&'a str, nisshi_broker::Result<Body>)>,
) -> Result<()> {
    for (heartbeat, outcome) in outcomes {
        let body = outcome.map_err(|err| anyhow!("{heartbeat} heartbeat: {err}"))?;

        assert_eq!(
            i16::from(ErrorCode::None),
            heartbeat_error(&body)?,
            "{heartbeat} heartbeat"
        );
    }

    Ok(())
}

async fn overlapping_heartbeats(storage: impl Storage + Clone, stagger: Duration) -> Result<()> {
    let coordinator = Controller::with_storage(storage)?;

    let Formed {
        group,
        generation_id,
        member_id,
    } = group::form_group(&coordinator).await?;

    // two heartbeats from the member that overlap on the broker
    //
    let (first, second) = tokio::join!(
        coordinator.heartbeat(&group, generation_id, &member_id, None),
        async {
            if !stagger.is_zero() {
                sleep(stagger).await;
            }

            coordinator
                .heartbeat(&group, generation_id, &member_id, None)
                .await
        },
    );

    // and one more afterwards, to see what was left in storage
    //
    let after = coordinator
        .heartbeat(&group, generation_id, &member_id, None)
        .await;

    assert_heartbeats_ok([("first", first), ("second", second), ("after", after)])
}

async fn heartbeat_after_restart(storage: impl Storage + Clone) -> Result<()> {
    let Formed {
        group,
        generation_id,
        member_id,
    } = group::form_group(&Controller::with_storage(storage.clone())?).await?;

    // a new coordinator over the same storage, as after a broker restart
    //
    let restarted = Controller::with_storage(storage)?;

    let first = restarted
        .heartbeat(&group, generation_id, &member_id, None)
        .await;

    // and one more afterwards, to see what was left in storage
    //
    let after = restarted
        .heartbeat(&group, generation_id, &member_id, None)
        .await;

    assert_heartbeats_ok([("first", first), ("after", after)])
}

/// Both heartbeats start together, so their storage transactions overlap.
const SIMULTANEOUS: Duration = Duration::ZERO;

/// The second heartbeat starts after the first has committed but (on
/// SlateDB) while the first is still waiting for its write to be durable.
const STAGGERED: Duration = Duration::from_millis(20);

/// Sends two heartbeats for one group at the same moment, and checks that both succeed.
pub async fn simultaneous(storage: impl Storage + Clone) -> Result<()> {
    overlapping_heartbeats(storage, SIMULTANEOUS).await
}

/// Sends a second heartbeat for one group `STAGGERED` after the first, and checks that both
/// succeed.
pub async fn staggered(storage: impl Storage + Clone) -> Result<()> {
    overlapping_heartbeats(storage, STAGGERED).await
}

/// Sends heartbeats through a new coordinator over the same storage, as after a broker restart,
/// and checks that they succeed.
pub async fn restart(storage: impl Storage + Clone) -> Result<()> {
    heartbeat_after_restart(storage).await
}
