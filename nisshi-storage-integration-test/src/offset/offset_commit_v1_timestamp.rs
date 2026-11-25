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

//! An `OffsetCommit` v1 that sends the default `CommitTimestamp` of -1 stores its offsets. Kafka
//! reads -1 as "use the time of the commit" ([`OffsetAndMetadata.fromRequest`]), and Nisshi
//! stores no commit time, as for a version 2 or later commit, which has no `CommitTimestamp`.
//!
//! [`OffsetAndMetadata.fromRequest`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetAndMetadata.java#L159-L172

use crate::{broker, random};
use nisshi_broker::{
    Result,
    coordinator::group::{Coordinator as _, OffsetCommit, administrator::Controller},
};
use nisshi_sans_io::{
    Body, ErrorCode, OffsetCommitResponse,
    create_topics_request::CreatableTopic,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
};
use nisshi_storage::{Storage, Topition};

const OFFSET: i64 = 10;

/// The `CommitTimestamp` that an `OffsetCommit` v1 sends when the client sets none.
const DEFAULT_COMMIT_TIMESTAMP: i64 = -1;

pub async fn offset_commit_stores_offsets_with_the_default_commit_timestamp<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    broker::register_broker(cluster_id, broker_id, &storage).await?;

    let topic = random::alphanumeric_string(15);

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

    let controller = Controller::with_storage(storage.clone())?;
    let group_id = random::alphanumeric_string(15);

    let topics = [OffsetCommitRequestTopic::default()
        .name(topic.clone())
        .partitions(Some(vec![
            OffsetCommitRequestPartition::default()
                .partition_index(0)
                .committed_offset(OFFSET)
                .commit_timestamp(Some(DEFAULT_COMMIT_TIMESTAMP))
                .committed_metadata(None),
        ]))];

    // A commit outside any generation, as a consumer that only commits offsets sends it.
    let committed = controller
        .offset_commit(OffsetCommit {
            group_id: &group_id,
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

    let error_codes = committed_topics
        .iter()
        .flat_map(|topic| topic.partitions.iter().flatten())
        .map(|partition| partition.error_code)
        .collect::<Vec<_>>();
    assert_eq!(vec![i16::from(ErrorCode::None)], error_codes);

    let topition = Topition::new(topic, 0);
    let stored = storage
        .offset_fetch(Some(&group_id), std::slice::from_ref(&topition), None)
        .await?;
    assert_eq!(
        Some(OFFSET),
        stored.get(&topition).map(|committed| committed.offset)
    );

    Ok(())
}
