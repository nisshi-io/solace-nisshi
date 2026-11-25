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

//! `OffsetFetch` returns the leader epoch and the metadata of each committed
//! offset, in the per-topic shape of versions 0 to 7 and the per-group shape of
//! version 8 and later.
//!
//! Kafka stores a null commit metadata as `""`, and answers `-1` and `""` for a
//! partition without a committed offset, in every version.

use crate::commit_metadata::{self, Committed, LEADER_EPOCH, METADATA, OFFSET, PARTITION};
use crate::random;
use nisshi_broker::{
    Result,
    coordinator::group::{Coordinator as _, OffsetCommit, administrator::Controller},
};
use nisshi_sans_io::{
    ErrorCode, OffsetCommitResponse,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
};
use nisshi_storage::{OffsetCommitRequest, Storage};

pub async fn commit_returns_leader_epoch_and_metadata<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = commit_metadata::create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = random::alphanumeric_string(15);

    commit_metadata::offset_commit(
        &storage,
        &group_id,
        &topic,
        OffsetCommitRequest {
            offset: OFFSET,
            leader_epoch: Some(LEADER_EPOCH),
            timestamp: None,
            metadata: Some(METADATA.into()),
        },
    )
    .await?;

    commit_metadata::assert_fetched(
        storage,
        &group_id,
        &topic,
        &Committed::new(OFFSET, LEADER_EPOCH, METADATA),
    )
    .await
}

pub async fn commit_without_epoch_or_metadata_returns_defaults<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = commit_metadata::create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = random::alphanumeric_string(15);

    commit_metadata::offset_commit(
        &storage,
        &group_id,
        &topic,
        OffsetCommitRequest::default().offset(OFFSET),
    )
    .await?;

    commit_metadata::assert_fetched(storage, &group_id, &topic, &Committed::new(OFFSET, -1, ""))
        .await
}

pub async fn uncommitted_partition_returns_defaults<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = commit_metadata::create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = random::alphanumeric_string(15);

    let controller = Controller::with_storage(storage)?;
    let expected = Committed::new(-1, -1, "");

    assert_eq!(
        expected,
        commit_metadata::fetch_by_topic(&controller, &group_id, &topic).await?
    );
    assert_eq!(
        expected,
        commit_metadata::fetch_by_group(&controller, &group_id, Some(&topic)).await?
    );

    Ok(())
}

/// Commits through the group coordinator, as a client's `OffsetCommit` does, so the round trip
/// includes the conversion of the request's partition into the stored commit.
pub async fn offset_commit_request_returns_leader_epoch_and_metadata<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = commit_metadata::create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = random::alphanumeric_string(15);

    let topics = [OffsetCommitRequestTopic::default()
        .name(topic.clone())
        .partitions(Some(vec![
            OffsetCommitRequestPartition::default()
                .partition_index(PARTITION)
                .committed_offset(OFFSET)
                .committed_leader_epoch(Some(LEADER_EPOCH))
                .committed_metadata(Some(METADATA.into())),
        ]))];

    let committed = Controller::with_storage(storage.clone())?
        .offset_commit(OffsetCommit {
            group_id: &group_id,
            generation_id_or_member_epoch: Some(-1),
            member_id: Some(""),
            group_instance_id: None,
            retention_time_ms: None,
            topics: Some(&topics),
            is_transactional: false,
        })
        .await
        .and_then(|body| OffsetCommitResponse::try_from(body).map_err(Into::into))?;

    assert_eq!(
        vec![i16::from(ErrorCode::None)],
        committed
            .topics
            .unwrap_or_default()
            .iter()
            .flat_map(|topic| topic.partitions.iter().flatten())
            .map(|partition| partition.error_code)
            .collect::<Vec<_>>()
    );

    commit_metadata::assert_fetched(
        storage,
        &group_id,
        &topic,
        &Committed::new(OFFSET, LEADER_EPOCH, METADATA),
    )
    .await
}

/// A commit replaces the whole earlier commit, as in Kafka, so a commit without a leader epoch
/// or metadata clears the earlier ones.
pub async fn commit_replaces_leader_epoch_and_metadata<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = commit_metadata::create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = random::alphanumeric_string(15);

    commit_metadata::offset_commit(
        &storage,
        &group_id,
        &topic,
        OffsetCommitRequest {
            offset: OFFSET,
            leader_epoch: Some(LEADER_EPOCH),
            timestamp: None,
            metadata: Some(METADATA.into()),
        },
    )
    .await?;

    commit_metadata::offset_commit(
        &storage,
        &group_id,
        &topic,
        OffsetCommitRequest::default().offset(OFFSET + 1),
    )
    .await?;

    commit_metadata::assert_fetched(
        storage,
        &group_id,
        &topic,
        &Committed::new(OFFSET + 1, -1, ""),
    )
    .await
}
