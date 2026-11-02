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

//! A producer that commits offsets with `TxnOffsetCommit` and dies before `EndTxn` must not
//! hold the group's partition unstable for good. Kafka starts the transaction, and so its
//! timeout, at `AddOffsetsToTxn`, so the broker aborts it after `transaction.timeout.ms`, and
//! `OffsetFetch` with `require_stable`, which Java consumers always send and retry without
//! limit, answers the last committed offset again.
//!
//! Only the engines with a timeout sweep are here: memory, S3 and PostgreSQL. The libSQL and
//! Turso engines start the transaction, and have no sweep yet.

use std::time::{Duration, SystemTime};

use crate::common::{self, StorageType, alphanumeric_string, init_tracing, register_broker};
use nisshi_broker::{
    Result,
    coordinator::group::{Coordinator as _, administrator::Controller},
};
use nisshi_sans_io::{
    ErrorCode, OffsetFetchResponse,
    create_topics_request::CreatableTopic,
    offset_fetch_request::{OffsetFetchRequestGroup, OffsetFetchRequestTopics},
    txn_offset_commit_request::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic},
};
use nisshi_storage::{OffsetCommitRequest, Storage, Topition, TxnOffsetCommitRequest};
use rand::{RngExt as _, rng};
use url::Url;
use uuid::Uuid;

const TIMEOUT_MS: i32 = 10_000;
const COMMITTED: i64 = 100;
const IN_TRANSACTION: i64 = 200;

/// What `OffsetFetch` with `require_stable` answers for partition 0: its error code and offset.
async fn fetch_stable<G>(storage: &G, group_id: &str, topic: &str) -> Result<(i16, i64)>
where
    G: Storage + Clone,
{
    let groups = [OffsetFetchRequestGroup::default()
        .group_id(group_id.into())
        .member_id(None)
        .member_epoch(Some(-1))
        .topics(Some(vec![
            OffsetFetchRequestTopics::default()
                .name(topic.into())
                .partition_indexes(Some(vec![0])),
        ]))];

    let response = Controller::with_storage(storage.clone())?
        .offset_fetch(None, None, Some(&groups), Some(true))
        .await
        .and_then(|body| OffsetFetchResponse::try_from(body).map_err(Into::into))?;

    let groups = response.groups.unwrap_or_default();
    assert_eq!(1, groups.len());

    let topics = groups[0].topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());

    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());

    Ok((partitions[0].error_code, partitions[0].committed_offset))
}

/// A transaction that commits an offset and never ends: unstable until its timeout, stable after,
/// at the offset committed outside the transaction. The aborted producer is fenced.
pub async fn offsets_only_transaction_times_out<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &storage).await?;

    let topic = alphanumeric_string(15);
    let group_id = alphanumeric_string(15);
    let transaction_id = alphanumeric_string(15);

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

    let outcome = storage
        .offset_commit(
            &group_id,
            None,
            &[(
                Topition::new(topic.clone(), 0),
                OffsetCommitRequest::default().offset(COMMITTED),
            )],
        )
        .await?;
    assert_eq!(ErrorCode::None, outcome[0].1);

    let producer = storage
        .init_producer(Some(&transaction_id), TIMEOUT_MS, Some(-1), Some(-1))
        .await?;
    assert_eq!(ErrorCode::None, producer.error);

    assert_eq!(
        ErrorCode::None,
        storage
            .txn_add_offsets(&transaction_id, producer.id, producer.epoch, &group_id)
            .await?
    );

    let outcome = storage
        .txn_offset_commit(TxnOffsetCommitRequest {
            transaction_id: transaction_id.clone(),
            group_id: group_id.clone(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            generation_id: None,
            member_id: None,
            group_instance_id: None,
            topics: vec![
                TxnOffsetCommitRequestTopic::default()
                    .name(topic.clone())
                    .partitions(Some(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .partition_index(0)
                            .committed_offset(IN_TRANSACTION)
                            .committed_leader_epoch(None)
                            .committed_metadata(None),
                    ])),
            ],
        })
        .await?;
    assert_eq!(
        Some(i16::from(ErrorCode::None)),
        outcome[0]
            .partitions
            .as_deref()
            .and_then(|partitions| partitions.first())
            .map(|partition| partition.error_code)
    );

    let unstable = (i16::from(ErrorCode::UnstableOffsetCommit), -1);
    assert_eq!(unstable, fetch_stable(&storage, &group_id, &topic).await?);

    // The timeout has not run out: the sweep leaves the transaction.
    storage.maintain_transactions(SystemTime::now()).await?;
    assert_eq!(unstable, fetch_stable(&storage, &group_id, &topic).await?);

    // `now` is a parameter, so pass one past the timeout instead of sleeping.
    storage
        .maintain_transactions(SystemTime::now() + Duration::from_millis(TIMEOUT_MS as u64 * 2))
        .await?;
    assert_eq!(
        (i16::from(ErrorCode::None), COMMITTED),
        fetch_stable(&storage, &group_id, &topic).await?
    );

    // Kafka fences the producer of a timed out transaction.
    assert_ne!(
        ErrorCode::None,
        storage
            .txn_end(&transaction_id, producer.id, producer.epoch, true)
            .await
            .unwrap_or_else(|err| match err {
                nisshi_storage::Error::Api(code) => code,
                otherwise => panic!("{otherwise:?}"),
            })
    );
    assert_eq!(
        (i16::from(ErrorCode::None), COMMITTED),
        fetch_stable(&storage, &group_id, &topic).await?
    );

    Ok(())
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage_type:expr) => {
        #[cfg(feature = $feature)]
        mod $name {
            use nisshi_storage::ArcDynStorage;

            use super::*;

            #[tokio::test]
            async fn offsets_only_transaction_times_out() -> Result<()> {
                let _guard = init_tracing()?;

                let cluster_id = Uuid::now_v7();
                let broker_id = rng().random_range(0..i32::MAX);

                let storage: ArcDynStorage = common::storage_container(
                    $storage_type,
                    cluster_id,
                    broker_id,
                    Url::parse("tcp://127.0.0.1/")?,
                    None,
                )
                .await?;

                super::offsets_only_transaction_times_out(cluster_id, broker_id, storage).await
            }
        }
    };
}

backend!(in_memory, "dynostore", StorageType::InMemory);
backend!(pg, "postgres", StorageType::Postgres);
