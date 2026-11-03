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

//! A `TxnOffsetCommit` from a member that does not match its group is refused and records
//! nothing, as Kafka does (KIP-447). Without the check, a zombie producer whose consumer lost its
//! partitions in a rebalance could still move their offsets.

use std::{assert_matches, slice, time::Duration};

use bytes::Bytes;
use nisshi_broker::{
    Error, Result,
    coordinator::group::administrator::{Controller, GroupCoordinatorConfig},
    service,
};
use nisshi_sans_io::{
    ApiKey as _, Body, ErrorCode, Frame, FrameInput, Header, TxnOffsetCommitRequest,
    create_topics_request::CreatableTopic,
    sync_group_request::SyncGroupRequestAssignment,
    txn_offset_commit_request::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic},
};
use nisshi_service::FrameRouteService;
use nisshi_storage::{ArcDynStorage, AutoCreateRefusals, Storage, TopicCreationConfig, Topition};
use rama::{Service as _, extensions::Extensions};
use rand::{RngExt as _, rng};
use url::Url;
use uuid::Uuid;

use crate::common::{
    JoinResponse, PROTOCOL_TYPE, RANGE, StorageType, alphanumeric_string, join, register_broker,
    sync_group,
};

const SESSION_TIMEOUT_MS: i32 = 45_000;
const REBALANCE_TIMEOUT_MS: i32 = 300_000;
const TRANSACTION_TIMEOUT_MS: i32 = 10_000;
const COMMITTED_OFFSET: i64 = 32_123;

/// A short initial delay, so that the group forms without waiting Kafka's default 3 s.
const GROUP_COORDINATOR: GroupCoordinatorConfig = GroupCoordinatorConfig {
    initial_rebalance_delay: Duration::from_millis(100),
    poll_interval: Duration::from_millis(500),
};

/// A transactional producer that has added `group` to its transaction, a topic with one
/// partition, and the broker's route table, which serves `TxnOffsetCommit` with the group's
/// coordinator.
struct Fixture {
    storage: ArcDynStorage,
    broker: Controller<ArcDynStorage>,
    route: FrameRouteService<Error>,
    transaction_id: String,
    producer_id: i64,
    producer_epoch: i16,
    group: String,
    topic: String,
}

impl Fixture {
    async fn new(storage_type: StorageType) -> Result<Self> {
        let cluster = Uuid::now_v7();
        let node = rng().random_range(0..i32::MAX);

        let storage = crate::common::storage_container(
            storage_type,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await?;

        register_broker(cluster, node, &storage).await?;

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

        let transaction_id = alphanumeric_string(10);
        let producer = storage
            .init_producer(
                Some(transaction_id.as_str()),
                TRANSACTION_TIMEOUT_MS,
                Some(-1),
                Some(-1),
            )
            .await?;

        let group = alphanumeric_string(15);

        assert_eq!(
            ErrorCode::None,
            storage
                .txn_add_offsets(&transaction_id, producer.id, producer.epoch, &group)
                .await?
        );

        let broker = Controller::new(storage.clone(), GROUP_COORDINATOR);

        // The broker's own route table, so that the fence is where the broker puts it, not
        // where this test does.
        let route = service::routes(
            broker.clone(),
            storage.clone(),
            TopicCreationConfig::default(),
            AutoCreateRefusals::default(),
        )
        .and_then(|builder| builder.build().map_err(Into::into))?;

        Ok(Self {
            storage,
            broker,
            route,
            transaction_id,
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            group,
            topic,
        })
    }

    /// Forms the group with one member, which joins and syncs.
    async fn form_group(&mut self, group_instance_id: Option<&str>) -> Result<JoinResponse> {
        let member = join(
            &mut self.broker,
            &self.group,
            None,
            group_instance_id,
            None,
            SESSION_TIMEOUT_MS,
            Some(REBALANCE_TIMEOUT_MS),
        )
        .await?;

        let synced = sync_group(
            &mut self.broker,
            &self.group,
            member.generation(),
            member.id(),
            group_instance_id,
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

    /// Sends a `TxnOffsetCommit` of `api_version` through the route, and returns the error code
    /// of each partition.
    async fn commit(
        &self,
        api_version: i16,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<Vec<ErrorCode>> {
        let request = TxnOffsetCommitRequest::default()
            .transactional_id(self.transaction_id.clone())
            .group_id(self.group.clone())
            .producer_id(self.producer_id)
            .producer_epoch(self.producer_epoch)
            .generation_id(Some(generation_id))
            .member_id(Some(member_id.into()))
            .group_instance_id(group_instance_id.map(Into::into))
            .topics(Some(vec![
                TxnOffsetCommitRequestTopic::default()
                    .name(self.topic.clone())
                    .partitions(Some(vec![
                        TxnOffsetCommitRequestPartition::default()
                            .partition_index(0)
                            .committed_offset(COMMITTED_OFFSET)
                            .committed_leader_epoch(Some(-1))
                            .committed_metadata(None),
                    ])),
            ]));

        let response = self
            .route
            .serve(FrameInput {
                frame: Frame {
                    size: 0,
                    header: Header::Request {
                        api_key: TxnOffsetCommitRequest::KEY,
                        api_version,
                        correlation_id: 12321,
                        client_id: None,
                    },
                    body: request.into(),
                },
                extensions: Extensions::default(),
            })
            .await?;

        let Body::TxnOffsetCommitResponse(response) = response.body else {
            panic!("a TxnOffsetCommit response: {response:?}");
        };

        response
            .topics
            .iter()
            .flatten()
            .flat_map(|topic| topic.partitions.iter().flatten())
            .map(|partition| ErrorCode::try_from(partition.error_code).map_err(Into::into))
            .collect()
    }

    /// Commits the transaction, and returns the committed offset of the group's partition.
    async fn committed_offset_after_commit(&self) -> Result<Option<i64>> {
        assert_eq!(
            ErrorCode::None,
            self.storage
                .txn_end(
                    &self.transaction_id,
                    self.producer_id,
                    self.producer_epoch,
                    true,
                )
                .await?
        );

        let topition = Topition::new(self.topic.clone(), 0);

        Ok(self
            .storage
            .offset_fetch(Some(&self.group), slice::from_ref(&topition), Some(false))
            .await?
            .get(&topition)
            .map(|committed| committed.offset))
    }
}

/// A commit from a member that is not in the group is refused, and its offset is not applied when
/// the transaction commits.
async fn unknown_member(storage_type: StorageType) -> Result<()> {
    let mut fixture = Fixture::new(storage_type).await?;
    let _member = fixture.form_group(None).await?;

    assert_eq!(
        vec![ErrorCode::UnknownMemberId],
        fixture.commit(3, 9999, "zombie-member", None).await?
    );

    assert_matches!(
        fixture.committed_offset_after_commit().await?,
        None | Some(-1)
    );

    Ok(())
}

/// A member in the wrong generation is refused.
async fn illegal_generation(storage_type: StorageType) -> Result<()> {
    let mut fixture = Fixture::new(storage_type).await?;
    let member = fixture.form_group(None).await?;

    assert_eq!(
        vec![ErrorCode::IllegalGeneration],
        fixture.commit(3, 9999, member.id(), None).await?
    );

    assert_matches!(
        fixture.committed_offset_after_commit().await?,
        None | Some(-1)
    );

    Ok(())
}

/// A static member's ID with another member's group instance ID is refused.
async fn fenced_instance(storage_type: StorageType) -> Result<()> {
    let mut fixture = Fixture::new(storage_type).await?;
    let member = fixture.form_group(Some("instance-a")).await?;

    assert_eq!(
        vec![ErrorCode::FencedInstanceId],
        fixture
            .commit(3, member.generation(), "another-member", Some("instance-a"))
            .await?
    );

    Ok(())
}

/// The member in the group's current generation commits, and the offset applies when the
/// transaction commits.
async fn current_member(storage_type: StorageType) -> Result<()> {
    let mut fixture = Fixture::new(storage_type).await?;
    let member = fixture.form_group(None).await?;

    assert_eq!(
        vec![ErrorCode::None],
        fixture
            .commit(3, member.generation(), member.id(), None)
            .await?
    );

    assert_eq!(
        Some(COMMITTED_OFFSET),
        fixture.committed_offset_after_commit().await?
    );

    Ok(())
}

/// A commit without a generation or a member, from a client on TxnOffsetCommit v0 to v2, is not
/// checked against the group, also when the group has members.
async fn without_member_fields(storage_type: StorageType) -> Result<()> {
    let mut fixture = Fixture::new(storage_type).await?;
    let _member = fixture.form_group(None).await?;

    assert_eq!(
        vec![ErrorCode::None],
        fixture.commit(2, -1, "", None).await?
    );

    assert_eq!(
        Some(COMMITTED_OFFSET),
        fixture.committed_offset_after_commit().await?
    );

    Ok(())
}

/// A transactional commit is not refused while the group waits for its leader's assignment, where
/// an `OffsetCommit` gets REBALANCE_IN_PROGRESS: Kafka does not hold a transactional commit to the
/// group's state, and the Java producer treats that error as fatal.
async fn group_completing_rebalance(storage_type: StorageType) -> Result<()> {
    let mut fixture = Fixture::new(storage_type).await?;

    // The member joins and does not sync, so the group waits for the leader's assignment.
    let member = join(
        &mut fixture.broker,
        &fixture.group,
        None,
        None,
        None,
        SESSION_TIMEOUT_MS,
        Some(REBALANCE_TIMEOUT_MS),
    )
    .await?;

    assert_eq!(
        vec![ErrorCode::None],
        fixture
            .commit(3, member.generation(), member.id(), None)
            .await?
    );

    Ok(())
}

macro_rules! engine_tests {
    ($engine:ident, $storage_type:expr) => {
        mod $engine {
            use super::*;

            #[tokio::test]
            async fn unknown_member() -> Result<()> {
                let _guard = crate::common::init_tracing()?;
                super::unknown_member($storage_type).await
            }

            #[tokio::test]
            async fn illegal_generation() -> Result<()> {
                let _guard = crate::common::init_tracing()?;
                super::illegal_generation($storage_type).await
            }

            #[tokio::test]
            async fn fenced_instance() -> Result<()> {
                let _guard = crate::common::init_tracing()?;
                super::fenced_instance($storage_type).await
            }

            #[tokio::test]
            async fn current_member() -> Result<()> {
                let _guard = crate::common::init_tracing()?;
                super::current_member($storage_type).await
            }

            #[tokio::test]
            async fn group_completing_rebalance() -> Result<()> {
                let _guard = crate::common::init_tracing()?;
                super::group_completing_rebalance($storage_type).await
            }

            #[tokio::test]
            async fn without_member_fields() -> Result<()> {
                let _guard = crate::common::init_tracing()?;
                super::without_member_fields($storage_type).await
            }
        }
    };
}

#[cfg(feature = "dynostore")]
engine_tests!(in_memory, StorageType::InMemory);

#[cfg(feature = "libsql")]
engine_tests!(lite, StorageType::Lite);

#[cfg(feature = "postgres")]
engine_tests!(pg, StorageType::Postgres);
