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

//! How the group coordinator fences an `OffsetCommit` when brokers share storage.
//!
//! Kafka rejects an offset commit from a member that is no longer in the group, and stores none of
//! its offsets. A Nisshi broker keeps a cached copy of each group that it coordinates, which can be
//! older than another broker's change, and another broker can change the group while a commit is
//! in progress. So the coordinator stores the group before the offsets, and stores the offsets
//! only if the group's conditional write lands.

use std::{
    slice,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use nisshi_broker::{
    Result,
    coordinator::group::{
        Coordinator as _, OffsetCommit,
        administrator::{Controller, GroupCoordinatorConfig},
    },
};
use nisshi_sans_io::{
    Body, ErrorCode, OffsetCommitResponse,
    create_topics_request::CreatableTopic,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
    sync_group_request::SyncGroupRequestAssignment,
};
use nisshi_storage::{
    Request, RequestChannelService, RequestStorageService, Storage, Topition, bounded_channel,
};
use rama::Service as _;
use rand::{prelude::*, rng};
use url::Url;
use uuid::Uuid;

use crate::common::{
    self, JoinResponse, PROTOCOL_TYPE, RANGE, StorageType, alphanumeric_string, init_tracing, join,
    leave, register_broker, sync_group,
};

const SESSION_TIMEOUT_MS: i32 = 45_000;

const REBALANCE_TIMEOUT_MS: i32 = 300_000;

/// A short initial delay, so that the group forms without waiting Kafka's default 3 s.
const GROUP_COORDINATOR: GroupCoordinatorConfig = GroupCoordinatorConfig {
    initial_rebalance_delay: Duration::from_millis(100),
    poll_interval: Duration::from_millis(500),
};

/// The offset that each test commits.
const COMMITTED_OFFSET: i64 = 12_321;

/// Creates a topic with one partition, and returns its name.
async fn create_topic<G>(storage: &G) -> Result<String>
where
    G: Storage,
{
    let topic = alphanumeric_string(15);

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(topic)
}

/// Forms `group` on `broker` with one member, which joins and syncs, and returns the member.
async fn form_group<G>(broker: &mut Controller<G>, group: &str) -> Result<JoinResponse>
where
    G: Storage + Clone,
{
    let member = join(
        broker,
        group,
        None,
        None,
        None,
        SESSION_TIMEOUT_MS,
        Some(REBALANCE_TIMEOUT_MS),
    )
    .await?;

    let synced = sync_group(
        broker,
        group,
        member.generation(),
        member.id(),
        None,
        PROTOCOL_TYPE,
        RANGE,
        &[SyncGroupRequestAssignment::default()
            .member_id(member.id().to_owned())
            .assignment(Bytes::from_static(b"partition-0"))],
    )
    .await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(synced.error_code)?);

    Ok(member)
}

/// Commits [`COMMITTED_OFFSET`] for partition 0 of `topic` through `broker`, as `member` in the
/// generation that it joined, and returns the error code of each partition in the response.
async fn commit_offset<G>(
    broker: &Controller<G>,
    group: &str,
    topic: &str,
    member: &JoinResponse,
) -> Result<Vec<ErrorCode>>
where
    G: Storage + Clone,
{
    let topics = [OffsetCommitRequestTopic::default()
        .name(topic.to_owned())
        .partitions(Some(vec![
            OffsetCommitRequestPartition::default()
                .partition_index(0)
                .committed_offset(COMMITTED_OFFSET),
        ]))];

    let committed = broker
        .offset_commit(OffsetCommit {
            group_id: group,
            generation_id_or_member_epoch: Some(member.generation()),
            member_id: Some(member.id()),
            group_instance_id: None,
            retention_time_ms: None,
            topics: Some(&topics),
            is_transactional: false,
        })
        .await?;

    let Body::OffsetCommitResponse(OffsetCommitResponse {
        topics: Some(committed_topics),
        ..
    }) = committed
    else {
        panic!("expected an offset commit response: {committed:?}");
    };

    committed_topics
        .iter()
        .flat_map(|topic| topic.partitions.iter().flatten())
        .map(|partition| ErrorCode::try_from(partition.error_code).map_err(Into::into))
        .collect()
}

/// Asserts that `storage` holds no committed offset of `group` for partition 0 of `topic`.
async fn assert_no_offset_stored<G>(storage: &G, group: &str, topic: &str) -> Result<()>
where
    G: Storage,
{
    let topition = Topition::new(topic.to_owned(), 0);
    let stored = storage
        .offset_fetch(Some(group), slice::from_ref(&topition), None)
        .await?;

    assert!(
        stored
            .get(&topition)
            .is_none_or(|committed| committed.offset == -1),
        "a fenced commit stored offset {:?}",
        stored.get(&topition)
    );

    Ok(())
}

/// A member forms a group on broker A, which caches the group. The member then leaves through
/// broker B, and commits an offset through A with the generation that it had. A's cache still
/// holds the member, but storage doesn't, so the commit is rejected and stores nothing.
async fn commit_after_leaving_through_another_broker_is_fenced<G>(
    cluster_id: Uuid,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone + 'static,
{
    register_broker(cluster_id, broker_id, &storage).await?;
    let topic = create_topic(&storage).await?;

    let mut broker_a = Controller::new(storage.clone(), GROUP_COORDINATOR);
    let mut broker_b = Controller::new(storage.clone(), GROUP_COORDINATOR);
    let group = alphanumeric_string(15);

    let member = form_group(&mut broker_a, &group).await?;

    let left = leave(&mut broker_b, &group, member.id(), None).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(left.error_code)?);

    assert_eq!(
        vec![ErrorCode::UnknownMemberId],
        commit_offset(&broker_a, &group, &topic, &member).await?
    );

    assert_no_offset_stored(&storage, &group, &topic).await
}

/// The member whose removal [`interfering_storage`] makes, by group id.
type Removal = Arc<Mutex<Option<(String, String)>>>;

/// Returns `storage` behind a channel that, when `removal` holds a group and a member, removes the
/// member from the group just before the next `update_group` of that group, as another broker
/// would between a commit's read of the group and its conditional write.
fn interfering_storage<G>(storage: G, removal: Removal) -> RequestChannelService
where
    G: Storage + Clone + 'static,
{
    let (sender, mut receiver) = bounded_channel(64);
    let service = RequestStorageService::new(storage.clone());

    _ = tokio::spawn(async move {
        while let Some((request, reply)) = receiver.recv().await {
            if let Request::UpdateGroup { group_id, .. } = &request {
                let pending = removal
                    .lock()
                    .ok()
                    .and_then(|mut removal| removal.take_if(|(group, _)| group == group_id));

                if let Some((group, member_id)) = pending
                    && let Ok(Some((mut detail, version))) =
                        storage.group_with_version(&group).await
                {
                    _ = detail.members.remove(&member_id);
                    _ = storage.update_group(&group, detail, Some(version)).await;
                }
            }

            let service = service.clone();

            _ = tokio::spawn(async move {
                if let Ok(response) = service.serve(request).await {
                    _ = reply.send(response);
                }
            });
        }
    });

    RequestChannelService::new(sender)
}

/// Another broker removes a member while the member's commit is in progress: after the commit has
/// read the group, and before its conditional write of the group. The write loses, and the commit
/// is checked again against the group without the member, so it is rejected and stores nothing.
async fn commit_whose_member_is_removed_during_the_commit_stores_nothing<G>(
    cluster_id: Uuid,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone + 'static,
{
    register_broker(cluster_id, broker_id, &storage).await?;
    let topic = create_topic(&storage).await?;

    let removal = Removal::default();
    let mut broker = Controller::new(
        interfering_storage(storage.clone(), removal.clone()),
        GROUP_COORDINATOR,
    );
    let group = alphanumeric_string(15);

    let member = form_group(&mut broker, &group).await?;

    _ = removal
        .lock()
        .map(|mut removal| removal.replace((group.clone(), member.id().to_owned())));

    assert_eq!(
        vec![ErrorCode::UnknownMemberId],
        commit_offset(&broker, &group, &topic, &member).await?
    );
    assert!(
        removal.lock().is_ok_and(|removal| removal.is_none()),
        "the commit did not write the group, so the member was never removed"
    );

    assert_no_offset_stored(&storage, &group, &topic).await
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage_type:expr) => {
        #[cfg(feature = $feature)]
        mod $name {
            use nisshi_storage::ArcDynStorage;

            use super::*;

            async fn storage_container(cluster_id: Uuid, broker_id: i32) -> Result<ArcDynStorage> {
                common::storage_container(
                    $storage_type,
                    cluster_id,
                    broker_id,
                    Url::parse("tcp://127.0.0.1/")?,
                    None,
                )
                .await
            }

            #[tokio::test]
            async fn commit_after_leaving_through_another_broker_is_fenced() -> Result<()> {
                let _guard = init_tracing()?;
                let cluster_id = Uuid::now_v7();
                let broker_id = rng().random_range(0..i32::MAX);

                super::commit_after_leaving_through_another_broker_is_fenced(
                    cluster_id,
                    broker_id,
                    storage_container(cluster_id, broker_id).await?,
                )
                .await
            }

            #[tokio::test]
            async fn commit_whose_member_is_removed_during_the_commit_stores_nothing() -> Result<()>
            {
                let _guard = init_tracing()?;
                let cluster_id = Uuid::now_v7();
                let broker_id = rng().random_range(0..i32::MAX);

                super::commit_whose_member_is_removed_during_the_commit_stores_nothing(
                    cluster_id,
                    broker_id,
                    storage_container(cluster_id, broker_id).await?,
                )
                .await
            }
        }
    };
}

backend!(in_memory, "dynostore", StorageType::InMemory);
backend!(lite, "libsql", StorageType::Lite);
backend!(pg, "postgres", StorageType::Postgres);
backend!(slatedb, "slatedb", StorageType::SlateDb);
