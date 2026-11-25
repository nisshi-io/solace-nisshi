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

//! `OffsetCommit` rejects the commit of a partition whose metadata is longer than 4096
//! characters with `OFFSET_METADATA_TOO_LARGE`, and stores the other partitions of the request.
//!
//! Kafka counts the metadata's length in UTF-16 code units, against `offset.metadata.max.bytes`
//! (4096 by default).

use std::iter;

use crate::metadata_max_size::{self, MAX_METADATA_LENGTH, OFFSET};
use crate::random;
use nisshi_broker::{
    Result,
    coordinator::group::{Coordinator as _, OffsetCommit, administrator::Controller},
};
use nisshi_sans_io::{
    Body, ErrorCode, OffsetCommitResponse,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
};
use nisshi_storage::Storage;

/// Commits [`OFFSET`] with `metadata` for each partition of `topic` through `controller`, as a
/// consumer outside any generation, and returns each partition's error code.
async fn offset_commit<G>(
    controller: &Controller<G>,
    group_id: &str,
    topic: &str,
    metadata: &[(i32, String)],
) -> Result<Vec<(i32, ErrorCode)>>
where
    G: Storage + Clone,
{
    let topics = [OffsetCommitRequestTopic::default()
        .name(topic.to_owned())
        .partitions(Some(
            metadata
                .iter()
                .map(|(partition, metadata)| {
                    OffsetCommitRequestPartition::default()
                        .partition_index(*partition)
                        .committed_offset(OFFSET)
                        .committed_leader_epoch(Some(-1))
                        .committed_metadata(Some(metadata.clone()))
                })
                .collect(),
        ))];

    let committed = controller
        .offset_commit(OffsetCommit {
            group_id,
            generation_id_or_member_epoch: Some(-1),
            member_id: Some(""),
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
        .map(|partition| {
            ErrorCode::try_from(partition.error_code)
                .map(|error_code| (partition.partition_index, error_code))
                .map_err(Into::into)
        })
        .collect()
}

pub async fn offset_commit_rejects_metadata_over_the_limit<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = metadata_max_size::create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = random::alphanumeric_string(15);
    let controller = Controller::with_storage(storage.clone())?;

    let mut outcome = offset_commit(
        &controller,
        &group_id,
        &topic,
        &[
            (
                0,
                metadata_max_size::metadata_of_length(MAX_METADATA_LENGTH + 1),
            ),
            (
                1,
                metadata_max_size::metadata_of_length(MAX_METADATA_LENGTH),
            ),
        ],
    )
    .await?;
    outcome.sort();

    assert_eq!(
        vec![(0, ErrorCode::OffsetMetadataTooLarge), (1, ErrorCode::None)],
        outcome
    );

    assert_eq!(
        -1,
        metadata_max_size::committed_offset(&storage, &group_id, &topic, 0).await?
    );
    assert_eq!(
        OFFSET,
        metadata_max_size::committed_offset(&storage, &group_id, &topic, 1).await?
    );

    Ok(())
}

/// Kafka compares the length in UTF-16 code units, so 4096 two-byte UTF-8 characters fit.
pub async fn offset_commit_counts_characters_not_bytes<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = metadata_max_size::create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = random::alphanumeric_string(15);
    let controller = Controller::with_storage(storage.clone())?;

    let metadata: String = iter::repeat_n('é', MAX_METADATA_LENGTH).collect();
    assert_eq!(2 * MAX_METADATA_LENGTH, metadata.len());

    assert_eq!(
        vec![(0, ErrorCode::None)],
        offset_commit(&controller, &group_id, &topic, &[(0, metadata)]).await?
    );
    assert_eq!(
        OFFSET,
        metadata_max_size::committed_offset(&storage, &group_id, &topic, 0).await?
    );

    Ok(())
}
