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

//! The coordinator keeps no state for a group id that a client sends and that storage does not
//! hold, because a client chooses the id and the coordinator never evicts what it keeps.

use nisshi_storage::ArcDynStorage;
use pretty_assertions::assert_eq;

use super::{rebalance_tests::null_storage, *};

const FRESH_GROUPS: usize = 8;
const LATEST_JOIN_GROUP_VERSION: i16 = 9;

fn fresh_group(index: usize) -> String {
    format!("fresh-group-{index}")
}

async fn controller() -> Result<Controller<ArcDynStorage>> {
    Controller::with_storage(null_storage().await?)
}

#[track_caller]
fn assert_nothing_kept(controller: &Controller<ArcDynStorage>) {
    assert_eq!(0, controller.wrappers.lock().expect("a lock").len());
    assert_eq!(0, controller.changes.lock().expect("a lock").len());
}

#[tokio::test]
async fn heartbeat_for_unknown_groups_keeps_nothing() -> Result<()> {
    let controller = controller().await?;

    for index in 0..FRESH_GROUPS {
        _ = controller
            .heartbeat(&fresh_group(index), 1, "member", None)
            .await?;
    }

    assert_nothing_kept(&controller);
    Ok(())
}

#[tokio::test]
async fn leave_for_unknown_groups_keeps_nothing() -> Result<()> {
    let controller = controller().await?;

    for index in 0..FRESH_GROUPS {
        _ = controller
            .leave(&fresh_group(index), Some("member"), None)
            .await?;
    }

    assert_nothing_kept(&controller);
    Ok(())
}

#[tokio::test]
async fn sync_for_unknown_groups_keeps_nothing() -> Result<()> {
    let controller = controller().await?;

    for index in 0..FRESH_GROUPS {
        _ = controller
            .sync(&fresh_group(index), 1, "member", None, None, None, None)
            .await?;
    }

    assert_nothing_kept(&controller);
    Ok(())
}

#[tokio::test]
async fn rejected_join_keeps_nothing() -> Result<()> {
    let controller = controller().await?;

    for index in 0..FRESH_GROUPS {
        // A join without protocols is rejected before the coordinator stores the group.
        _ = controller
            .join(
                LATEST_JOIN_GROUP_VERSION,
                Some("client"),
                None,
                &fresh_group(index),
                45_000,
                Some(300_000),
                "",
                None,
                "consumer",
                Some(&[]),
                None,
            )
            .await?;
    }

    assert_nothing_kept(&controller);
    Ok(())
}

/// Returns a controller that ends the first join phase at once, and the response of the join that
/// creates the group "group".
async fn controller_with_joined_member() -> Result<(Controller<ArcDynStorage>, JoinGroupResponse)> {
    let controller = Controller::new(
        null_storage().await?,
        GroupCoordinatorConfig {
            initial_rebalance_delay: Duration::ZERO,
            ..GroupCoordinatorConfig::default()
        },
    );

    let body = controller
        .join(
            3,
            Some("client"),
            None,
            "group",
            45_000,
            Some(300_000),
            "",
            None,
            "consumer",
            Some(&[JoinGroupRequestProtocol::default()
                .name("range".into())
                .metadata(Bytes::new())]),
            None,
        )
        .await?;

    Ok((controller, JoinGroupResponse::try_from(body)?))
}

#[tokio::test]
async fn completed_join_keeps_the_stored_group_and_no_notifier() -> Result<()> {
    let (controller, response) = controller_with_joined_member().await?;

    // Without a stored group, the assertions on the maps would pass for a join that failed.
    assert_eq!(i16::from(ErrorCode::None), response.error_code);
    assert_eq!(1, controller.wrappers.lock().expect("a lock").len());
    assert_eq!(0, controller.changes.lock().expect("a lock").len());
    Ok(())
}

#[tokio::test]
async fn heartbeat_that_stores_a_group_adds_no_notifier() -> Result<()> {
    let (controller, response) = controller_with_joined_member().await?;

    // A heartbeat stores the group to record the member's contact, and it waits on nothing.
    _ = controller
        .heartbeat("group", response.generation_id, &response.member_id, None)
        .await?;

    assert_eq!(0, controller.changes.lock().expect("a lock").len());
    Ok(())
}

#[tokio::test]
async fn last_subscription_dropped_removes_the_group_entry() -> Result<()> {
    let controller = controller().await?;

    let first = controller.subscribe("group")?;
    let second = controller.subscribe("group")?;
    assert_eq!(1, controller.changes.lock().expect("a lock").len());

    drop(first);
    assert_eq!(1, controller.changes.lock().expect("a lock").len());

    drop(second);
    assert_eq!(0, controller.changes.lock().expect("a lock").len());
    Ok(())
}

#[tokio::test]
async fn change_wakes_a_waiting_subscription() -> Result<()> {
    let controller = controller().await?;
    let subscription = controller.subscribe("group")?;

    let changed = subscription.notified();
    tokio::pin!(changed);
    _ = changed.as_mut().enable();

    controller.wake_waiters("group")?;

    tokio::time::timeout(Duration::from_secs(5), changed)
        .await
        .expect("the wake-up reaches the subscription");
    Ok(())
}

#[tokio::test]
async fn change_of_a_group_without_waiters_adds_no_entry() -> Result<()> {
    let controller = controller().await?;

    controller.wake_waiters("group")?;

    assert_nothing_kept(&controller);
    Ok(())
}

#[tokio::test]
async fn cancelled_held_join_removes_its_notifier() -> Result<()> {
    // The default initial rebalance delay of 3 seconds holds the join of a new group.
    let controller = controller().await?;

    let held = tokio::spawn({
        let controller = controller.clone();
        async move {
            controller
                .join(
                    3,
                    Some("client"),
                    None,
                    "group",
                    45_000,
                    Some(300_000),
                    "",
                    None,
                    "consumer",
                    Some(&[JoinGroupRequestProtocol::default()
                        .name("range".into())
                        .metadata(Bytes::new())]),
                    None,
                )
                .await
        }
    });

    while controller.changes.lock().expect("a lock").is_empty() {
        tokio::task::yield_now().await;
    }

    // A client that disconnects drops its held request at an await point.
    held.abort();
    assert!(held.await.expect_err("an aborted join").is_cancelled());

    assert_eq!(0, controller.changes.lock().expect("a lock").len());
    Ok(())
}

#[tokio::test]
async fn subscription_dropped_while_another_waits_keeps_the_waiter_woken() -> Result<()> {
    let controller = controller().await?;

    let finished = controller.subscribe("group")?;
    let waiting = controller.subscribe("group")?;

    let changed = waiting.notified();
    tokio::pin!(changed);
    _ = changed.as_mut().enable();

    drop(finished);
    controller.wake_waiters("group")?;

    tokio::time::timeout(Duration::from_secs(5), changed)
        .await
        .expect("the wake-up reaches the subscription that waits");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_subscriptions_to_one_group_leave_no_entry() -> Result<()> {
    let controller = controller().await?;

    let tasks = (0..256)
        .map(|_| {
            let controller = controller.clone();
            tokio::spawn(async move {
                let subscription = controller.subscribe("group")?;
                tokio::task::yield_now().await;
                drop(subscription);
                Ok::<_, Error>(())
            })
        })
        .collect::<Vec<_>>();

    for task in tasks {
        task.await??;
    }

    assert_nothing_kept(&controller);
    Ok(())
}

#[tokio::test]
async fn stored_group_wakes_a_waiting_subscription() -> Result<()> {
    let controller = controller().await?;
    let subscription = controller.subscribe("group")?;

    let changed = subscription.notified();
    tokio::pin!(changed);
    _ = changed.as_mut().enable();

    let group = Wrapper::Forming(Inner::new(controller.storage.clone()));
    assert!(
        controller
            .store_group("group", &group, None, "test")
            .await?
    );

    tokio::time::timeout(Duration::from_secs(5), changed)
        .await
        .expect("the stored group wakes the subscription");
    Ok(())
}
