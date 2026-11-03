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

//! `OffsetFetch` returns the leader epoch and the metadata of each committed
//! offset, in the per-topic shape of versions 0 to 7 and the per-group shape of
//! version 8 and later.
//!
//! Kafka stores a null commit metadata as `""`, and answers `-1` and `""` for a
//! partition without a committed offset, in every version.

use crate::common::{self, StorageType, alphanumeric_string, init_tracing, register_broker};
use nisshi_broker::{
    Result,
    coordinator::group::{Coordinator as _, OffsetCommit, administrator::Controller},
};
use nisshi_sans_io::{
    ErrorCode, OffsetCommitResponse, OffsetFetchResponse,
    create_topics_request::CreatableTopic,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
    offset_fetch_request::{
        OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
    },
    txn_offset_commit_request::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic},
};
use nisshi_storage::{OffsetCommitRequest, Storage, Topition, TxnOffsetCommitRequest};
use rand::{RngExt as _, rng};
use url::Url;
use uuid::Uuid;

const PARTITION: i32 = 0;
const OFFSET: i64 = 10;
const LEADER_EPOCH: i32 = 3;
const METADATA: &str = "abc";

/// The committed offset, leader epoch and metadata that `OffsetFetch` returns
/// for one partition.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Committed {
    offset: i64,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
}

impl Committed {
    fn new(offset: i64, leader_epoch: i32, metadata: &str) -> Self {
        Self {
            offset,
            leader_epoch: Some(leader_epoch),
            metadata: Some(metadata.into()),
        }
    }
}

/// Registers a broker and creates a topic with one partition, returning the
/// topic's name.
async fn create_topic<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: &G,
) -> Result<String>
where
    G: Storage,
{
    register_broker(cluster_id, broker_id, storage).await?;

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

/// Fetches the committed offset of the partition with an `OffsetFetch` in the
/// per-topic shape of versions 0 to 7.
async fn fetch_by_topic<G>(
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
async fn fetch_by_group<G>(
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
async fn assert_fetched<G>(
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

async fn offset_commit<G>(
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

pub async fn commit_returns_leader_epoch_and_metadata<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = alphanumeric_string(15);

    offset_commit(
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

    assert_fetched(
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
    let topic = create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = alphanumeric_string(15);

    offset_commit(
        &storage,
        &group_id,
        &topic,
        OffsetCommitRequest::default().offset(OFFSET),
    )
    .await?;

    assert_fetched(storage, &group_id, &topic, &Committed::new(OFFSET, -1, "")).await
}

pub async fn uncommitted_partition_returns_defaults<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = alphanumeric_string(15);

    let controller = Controller::with_storage(storage)?;
    let expected = Committed::new(-1, -1, "");

    assert_eq!(
        expected,
        fetch_by_topic(&controller, &group_id, &topic).await?
    );
    assert_eq!(
        expected,
        fetch_by_group(&controller, &group_id, Some(&topic)).await?
    );

    Ok(())
}

/// Commits `offset` with `leader_epoch` and `metadata` for the partition in a transaction, and
/// commits the transaction.
async fn txn_offset_commit<G>(
    storage: &G,
    group_id: &str,
    topic: &str,
    offset: i64,
    leader_epoch: Option<i32>,
    metadata: Option<&str>,
) -> Result<()>
where
    G: Storage,
{
    let transaction_id = alphanumeric_string(15);

    let producer = storage
        .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
        .await?;

    let outcome = storage
        .txn_offset_commit(TxnOffsetCommitRequest {
            transaction_id: transaction_id.clone(),
            group_id: group_id.to_owned(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            generation_id: None,
            member_id: None,
            group_instance_id: None,
            topics: vec![
                TxnOffsetCommitRequestTopic::default()
                    .name(topic.to_owned())
                    .partitions(Some(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .partition_index(PARTITION)
                            .committed_offset(offset)
                            .committed_leader_epoch(leader_epoch)
                            .committed_metadata(metadata.map(ToOwned::to_owned)),
                    ])),
            ],
        })
        .await?;

    assert_eq!(1, outcome.len());
    assert_eq!(
        Some(vec![i16::from(ErrorCode::None)]),
        outcome[0].partitions.as_ref().map(|partitions| partitions
            .iter()
            .map(|partition| partition.error_code)
            .collect::<Vec<_>>())
    );

    assert_eq!(
        ErrorCode::None,
        storage
            .txn_end(&transaction_id, producer.id, producer.epoch, true)
            .await?
    );

    Ok(())
}

pub async fn txn_offset_commit_returns_leader_epoch_and_metadata<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = alphanumeric_string(15);

    txn_offset_commit(
        &storage,
        &group_id,
        &topic,
        OFFSET,
        Some(LEADER_EPOCH),
        Some(METADATA),
    )
    .await?;

    assert_fetched(
        storage,
        &group_id,
        &topic,
        &Committed::new(OFFSET, LEADER_EPOCH, METADATA),
    )
    .await
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
    let topic = create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = alphanumeric_string(15);

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

    assert_fetched(
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
    let topic = create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = alphanumeric_string(15);

    offset_commit(
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

    offset_commit(
        &storage,
        &group_id,
        &topic,
        OffsetCommitRequest::default().offset(OFFSET + 1),
    )
    .await?;

    assert_fetched(
        storage,
        &group_id,
        &topic,
        &Committed::new(OFFSET + 1, -1, ""),
    )
    .await
}

pub async fn txn_offset_commit_replaces_leader_epoch_and_metadata<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = alphanumeric_string(15);

    offset_commit(
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

    txn_offset_commit(&storage, &group_id, &topic, OFFSET + 1, None, None).await?;

    assert_fetched(
        storage,
        &group_id,
        &topic,
        &Committed::new(OFFSET + 1, -1, ""),
    )
    .await
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage_type:expr $(, #[$attribute:meta])*) => {
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

            backend!(@test commit_returns_leader_epoch_and_metadata $(, #[$attribute])*);
            backend!(@test commit_without_epoch_or_metadata_returns_defaults $(, #[$attribute])*);
            backend!(@test uncommitted_partition_returns_defaults $(, #[$attribute])*);
            backend!(@test txn_offset_commit_returns_leader_epoch_and_metadata $(, #[$attribute])*);
            backend!(@test offset_commit_request_returns_leader_epoch_and_metadata $(, #[$attribute])*);
            backend!(@test commit_replaces_leader_epoch_and_metadata $(, #[$attribute])*);
            backend!(@test txn_offset_commit_replaces_leader_epoch_and_metadata $(, #[$attribute])*);
        }
    };

    (@test $test:ident $(, #[$attribute:meta])*) => {
        $(#[$attribute])*
        #[tokio::test]
        async fn $test() -> Result<()> {
            let _guard = init_tracing()?;

            let cluster_id = Uuid::now_v7();
            let broker_id = rng().random_range(0..i32::MAX);

            super::$test(
                cluster_id,
                broker_id,
                storage_container(cluster_id, broker_id).await?,
            )
            .await
        }
    };
}

backend!(in_memory, "dynostore", StorageType::InMemory);
backend!(lite, "libsql", StorageType::Lite);
backend!(pg, "postgres", StorageType::Postgres);
backend!(
    turso,
    "turso",
    StorageType::Turso,
    #[ignore = "the Turso engine does not start in the broker tests yet, see #866"]
);
