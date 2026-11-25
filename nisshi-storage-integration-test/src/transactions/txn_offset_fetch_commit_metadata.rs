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

//! `OffsetFetch` returns the leader epoch and the metadata of each offset that a transaction
//! commits, and the values of the last commit when a transaction replaces an earlier one.

use crate::commit_metadata::{self, Committed, LEADER_EPOCH, METADATA, OFFSET, PARTITION};
use crate::random;
use nisshi_broker::Result;
use nisshi_sans_io::{
    ErrorCode,
    txn_offset_commit_request::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic},
};
use nisshi_storage::{OffsetCommitRequest, Storage, TxnOffsetCommitRequest};

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
    let transaction_id = random::alphanumeric_string(15);

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
    let topic = commit_metadata::create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = random::alphanumeric_string(15);

    txn_offset_commit(
        &storage,
        &group_id,
        &topic,
        OFFSET,
        Some(LEADER_EPOCH),
        Some(METADATA),
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

pub async fn txn_offset_commit_replaces_leader_epoch_and_metadata<G>(
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

    txn_offset_commit(&storage, &group_id, &topic, OFFSET + 1, None, None).await?;

    commit_metadata::assert_fetched(
        storage,
        &group_id,
        &topic,
        &Committed::new(OFFSET + 1, -1, ""),
    )
    .await
}
