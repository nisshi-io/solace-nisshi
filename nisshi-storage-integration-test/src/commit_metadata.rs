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

//! Commits offsets with a leader epoch and metadata, and fetches them back as a consumer does.

use crate::{broker, random};
use nisshi_broker::{
    Result,
    coordinator::group::{Coordinator as _, administrator::Controller},
};
use nisshi_sans_io::{
    ErrorCode, OffsetFetchResponse,
    create_topics_request::CreatableTopic,
    offset_fetch_request::{
        OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
    },
};
use nisshi_storage::{OffsetCommitRequest, Storage, Topition};

pub(crate) const PARTITION: i32 = 0;

pub(crate) const OFFSET: i64 = 10;

pub(crate) const LEADER_EPOCH: i32 = 3;

pub(crate) const METADATA: &str = "abc";

/// The committed offset, leader epoch and metadata that `OffsetFetch` returns
/// for one partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Committed {
    offset: i64,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
}

impl Committed {
    pub(crate) fn new(offset: i64, leader_epoch: i32, metadata: &str) -> Self {
        Self {
            offset,
            leader_epoch: Some(leader_epoch),
            metadata: Some(metadata.into()),
        }
    }
}

/// Registers a broker and creates a topic with one partition, returning the
/// topic's name.
pub(crate) async fn create_topic<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: &G,
) -> Result<String>
where
    G: Storage,
{
    broker::register_broker(cluster_id, broker_id, storage).await?;

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

    Ok(topic)
}

/// Fetches the committed offset of the partition with an `OffsetFetch` in the
/// per-topic shape of versions 0 to 7.
pub(crate) async fn fetch_by_topic<G>(
    controller: &Controller<G>,
    group_id: &str,
    topic: &str,
) -> Result<Committed>
where
    G: Storage + Clone,
{
    let response = controller
        .offset_fetch(
            Some(group_id),
            Some(&[OffsetFetchRequestTopic::default()
                .name(topic.into())
                .partition_indexes(Some(vec![PARTITION]))]),
            None,
            Some(false),
        )
        .await
        .and_then(|body| OffsetFetchResponse::try_from(body).map_err(Into::into))?;

    assert_eq!(Some(i16::from(ErrorCode::None)), response.error_code);

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic, topics[0].name);

    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(PARTITION, partitions[0].partition_index);
    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);

    Ok(Committed {
        offset: partitions[0].committed_offset,
        leader_epoch: partitions[0].committed_leader_epoch,
        metadata: partitions[0].metadata.clone(),
    })
}

/// Fetches the committed offset of the partition with an `OffsetFetch` in the
/// per-group shape of version 8 and later. With `topic` set, the request names
/// the partition. Without it, the request asks for every committed offset of
/// the group.
pub(crate) async fn fetch_by_group<G>(
    controller: &Controller<G>,
    group_id: &str,
    topic: Option<&str>,
) -> Result<Committed>
where
    G: Storage + Clone,
{
    let response = controller
        .offset_fetch(
            None,
            None,
            Some(&[OffsetFetchRequestGroup::default()
                .group_id(group_id.into())
                .member_id(None)
                .member_epoch(Some(-1))
                .topics(topic.map(|topic| {
                    vec![
                        OffsetFetchRequestTopics::default()
                            .name(topic.into())
                            .partition_indexes(Some(vec![PARTITION])),
                    ]
                }))]),
            Some(false),
        )
        .await
        .and_then(|body| OffsetFetchResponse::try_from(body).map_err(Into::into))?;

    let groups = response.groups.unwrap_or_default();
    assert_eq!(1, groups.len());
    assert_eq!(group_id, groups[0].group_id);
    assert_eq!(i16::from(ErrorCode::None), groups[0].error_code);

    let topics = groups[0].topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());

    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(PARTITION, partitions[0].partition_index);
    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);

    Ok(Committed {
        offset: partitions[0].committed_offset,
        leader_epoch: Some(partitions[0].committed_leader_epoch),
        metadata: partitions[0].metadata.clone(),
    })
}

/// Asserts that every shape of `OffsetFetch` returns `expected` for the
/// partition.
pub(crate) async fn assert_fetched<G>(
    storage: G,
    group_id: &str,
    topic: &str,
    expected: &Committed,
) -> Result<()>
where
    G: Storage + Clone,
{
    let controller = Controller::with_storage(storage)?;

    assert_eq!(
        *expected,
        fetch_by_topic(&controller, group_id, topic).await?
    );
    assert_eq!(
        *expected,
        fetch_by_group(&controller, group_id, Some(topic)).await?
    );
    assert_eq!(
        *expected,
        fetch_by_group(&controller, group_id, None).await?
    );

    Ok(())
}

pub(crate) async fn offset_commit<G>(
    storage: &G,
    group_id: &str,
    topic: &str,
    commit: OffsetCommitRequest,
) -> Result<()>
where
    G: Storage,
{
    let outcome = storage
        .offset_commit(
            group_id,
            None,
            &[(Topition::new(topic.to_owned(), PARTITION), commit)],
        )
        .await?;

    assert_eq!(1, outcome.len());
    assert_eq!(ErrorCode::None, outcome[0].1);

    Ok(())
}
