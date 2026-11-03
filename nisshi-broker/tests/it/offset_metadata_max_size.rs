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

//! `OffsetCommit` and `TxnOffsetCommit` reject the commit of a partition whose metadata is
//! longer than 4096 characters with `OFFSET_METADATA_TOO_LARGE`, and store the other partitions
//! of the request.
//!
//! Kafka counts the metadata's length in UTF-16 code units, against `offset.metadata.max.bytes`
//! (4096 by default).

use std::{iter, slice};

use crate::common::{self, StorageType, alphanumeric_string, init_tracing, register_broker};
use nisshi_broker::{
    Result,
    coordinator::group::{Coordinator as _, OffsetCommit, administrator::Controller},
};
use nisshi_sans_io::{
    Body, ErrorCode, OffsetCommitResponse, RequestInput, TxnOffsetCommitRequest,
    create_topics_request::CreatableTopic,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
    txn_offset_commit_request::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic},
};
use nisshi_storage::{Storage, Topition, TxnOffsetCommitService};
use rama::{Service as _, extensions::Extensions};
use rand::{RngExt as _, rng};
use url::Url;
use uuid::Uuid;

const OFFSET: i64 = 10;

/// The longest metadata that Kafka accepts, by default.
const MAX_METADATA_LENGTH: usize = 4096;

/// Returns metadata of `length` characters.
fn metadata_of_length(length: usize) -> String {
    iter::repeat_n('m', length).collect()
}

/// Registers a broker and creates a topic with two partitions, returning the topic's name.
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
                .num_partitions(2)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(topic)
}

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

/// Returns the offset that `storage` holds for `partition` of `topic` in `group_id`, or -1.
async fn committed_offset<G>(
    storage: &G,
    group_id: &str,
    topic: &str,
    partition: i32,
) -> Result<i64>
where
    G: Storage,
{
    let topition = Topition::new(topic.to_owned(), partition);

    Ok(storage
        .offset_fetch(Some(group_id), slice::from_ref(&topition), Some(false))
        .await?
        .get(&topition)
        .map_or(-1, |committed| committed.offset))
}

pub async fn offset_commit_rejects_metadata_over_the_limit<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = alphanumeric_string(15);
    let controller = Controller::with_storage(storage.clone())?;

    let mut outcome = offset_commit(
        &controller,
        &group_id,
        &topic,
        &[
            (0, metadata_of_length(MAX_METADATA_LENGTH + 1)),
            (1, metadata_of_length(MAX_METADATA_LENGTH)),
        ],
    )
    .await?;
    outcome.sort();

    assert_eq!(
        vec![(0, ErrorCode::OffsetMetadataTooLarge), (1, ErrorCode::None)],
        outcome
    );

    assert_eq!(-1, committed_offset(&storage, &group_id, &topic, 0).await?);
    assert_eq!(
        OFFSET,
        committed_offset(&storage, &group_id, &topic, 1).await?
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
    let topic = create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = alphanumeric_string(15);
    let controller = Controller::with_storage(storage.clone())?;

    let metadata: String = iter::repeat_n('é', MAX_METADATA_LENGTH).collect();
    assert_eq!(2 * MAX_METADATA_LENGTH, metadata.len());

    assert_eq!(
        vec![(0, ErrorCode::None)],
        offset_commit(&controller, &group_id, &topic, &[(0, metadata)]).await?
    );
    assert_eq!(
        OFFSET,
        committed_offset(&storage, &group_id, &topic, 0).await?
    );

    Ok(())
}

pub async fn txn_offset_commit_rejects_metadata_over_the_limit<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = alphanumeric_string(15);
    let transaction_id = alphanumeric_string(15);

    let producer = storage
        .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
        .await?;

    let response = TxnOffsetCommitService {
        storage: storage.clone(),
    }
    .serve(RequestInput {
        request: TxnOffsetCommitRequest::default()
            .transactional_id(transaction_id.clone())
            .group_id(group_id.clone())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .generation_id(Some(-1))
            .member_id(Some(String::new()))
            .group_instance_id(None)
            .topics(Some(vec![
                TxnOffsetCommitRequestTopic::default()
                    .name(topic.clone())
                    .partitions(Some(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .partition_index(0)
                            .committed_offset(OFFSET)
                            .committed_leader_epoch(Some(-1))
                            .committed_metadata(Some(metadata_of_length(MAX_METADATA_LENGTH + 1))),
                        TxnOffsetCommitRequestPartition::default()
                            .partition_index(1)
                            .committed_offset(OFFSET)
                            .committed_leader_epoch(Some(-1))
                            .committed_metadata(Some(metadata_of_length(MAX_METADATA_LENGTH))),
                    ])),
            ])),
        extensions: Extensions::default(),
    })
    .await?;

    let mut outcome = response
        .topics
        .unwrap_or_default()
        .iter()
        .flat_map(|topic| topic.partitions.iter().flatten())
        .map(|partition| {
            ErrorCode::try_from(partition.error_code)
                .map(|error_code| (partition.partition_index, error_code))
        })
        .collect::<Result<Vec<_>, _>>()?;
    outcome.sort();

    assert_eq!(
        vec![(0, ErrorCode::OffsetMetadataTooLarge), (1, ErrorCode::None)],
        outcome
    );

    assert_eq!(
        ErrorCode::None,
        storage
            .txn_end(&transaction_id, producer.id, producer.epoch, true)
            .await?
    );

    assert_eq!(-1, committed_offset(&storage, &group_id, &topic, 0).await?);
    assert_eq!(
        OFFSET,
        committed_offset(&storage, &group_id, &topic, 1).await?
    );

    Ok(())
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
            async fn offset_commit_rejects_metadata_over_the_limit() -> Result<()> {
                let _guard = init_tracing()?;

                let cluster_id = Uuid::now_v7();
                let broker_id = rng().random_range(0..i32::MAX);

                super::offset_commit_rejects_metadata_over_the_limit(
                    cluster_id,
                    broker_id,
                    storage_container(cluster_id, broker_id).await?,
                )
                .await
            }

            #[tokio::test]
            async fn offset_commit_counts_characters_not_bytes() -> Result<()> {
                let _guard = init_tracing()?;

                let cluster_id = Uuid::now_v7();
                let broker_id = rng().random_range(0..i32::MAX);

                super::offset_commit_counts_characters_not_bytes(
                    cluster_id,
                    broker_id,
                    storage_container(cluster_id, broker_id).await?,
                )
                .await
            }

            #[tokio::test]
            async fn txn_offset_commit_rejects_metadata_over_the_limit() -> Result<()> {
                let _guard = init_tracing()?;

                let cluster_id = Uuid::now_v7();
                let broker_id = rng().random_range(0..i32::MAX);

                super::txn_offset_commit_rejects_metadata_over_the_limit(
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
