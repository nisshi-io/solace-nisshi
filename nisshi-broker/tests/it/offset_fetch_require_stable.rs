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

//! `OffsetFetch` with `require_stable` answers `UNSTABLE_OFFSET_COMMIT` for a
//! partition whose offset an open transaction of the group has committed but
//! not ended (KIP-447), as Kafka does. The client retries until the transaction
//! ends, so a consumer that takes over the partition does not start from the
//! older offset while the transaction can still commit a newer one.
//!
//! Without `require_stable`, `OffsetFetch` answers the last committed offset.
//!
//! The tests set up the transaction through `Storage`, and fetch through the
//! group coordinator in the per-topic shape of versions 0 to 7 and the
//! per-group shapes of version 8 and later.

use std::collections::BTreeMap;

use crate::common::{self, StorageType, alphanumeric_string, init_tracing, register_broker};
use bytes::Bytes;
use nisshi_broker::{
    Result,
    coordinator::group::{Coordinator as _, administrator::Controller},
};
use nisshi_sans_io::{
    BatchAttribute, ErrorCode, OffsetFetchResponse,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::CreatableTopic,
    offset_fetch_request::{
        OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
    },
    record::{Record, deflated, inflated},
    txn_offset_commit_request::{TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic},
};
use nisshi_storage::{
    OffsetCommitRequest, ProducerIdResponse, Storage, Topition, TxnAddPartitionsRequest,
    TxnOffsetCommitRequest,
};
use rand::{RngExt as _, rng};
use url::Url;
use uuid::Uuid;

/// The partition that the open transaction commits an offset for.
const PENDING: i32 = 0;

/// A partition of the same topic that the transaction leaves alone.
const OTHER: i32 = 1;

const PARTITIONS: [i32; 2] = [PENDING, OTHER];

const COMMITTED: i64 = 100;
const OTHER_COMMITTED: i64 = 50;
const IN_TRANSACTION: i64 = 200;

/// What `OffsetFetch` answers for one partition.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Fetched {
    error_code: i16,
    offset: i64,
    leader_epoch: i32,
    metadata: String,
}

impl Fetched {
    /// A committed offset. The tests commit without a leader epoch or metadata.
    fn stable(offset: i64) -> Self {
        Self {
            error_code: ErrorCode::None.into(),
            offset,
            leader_epoch: -1,
            metadata: "".into(),
        }
    }

    /// Kafka answers an unstable partition with offset -1, leader epoch -1 and
    /// metadata `""`.
    fn unstable() -> Self {
        Self {
            error_code: ErrorCode::UnstableOffsetCommit.into(),
            offset: -1,
            leader_epoch: -1,
            metadata: "".into(),
        }
    }
}

/// The shapes of an `OffsetFetch` request.
#[derive(Clone, Copy, Debug)]
enum Shape {
    /// Versions 0 to 7: the group and its topics at the top level.
    ByTopic,

    /// Version 8 and later: each group names its topics.
    ByGroup,

    /// Version 8 and later: each group asks for every committed offset.
    AllPartitions,
}

const SHAPES: [Shape; 3] = [Shape::ByTopic, Shape::ByGroup, Shape::AllPartitions];

/// Registers a broker and creates a topic with two partitions, returning the
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
                .num_partitions(PARTITIONS.len() as i32)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(topic)
}

/// Fetches the offsets of the topic's partitions for the group, keyed by
/// partition index.
async fn fetch<G>(
    storage: G,
    group_id: &str,
    topic: &str,
    shape: Shape,
    require_stable: Option<bool>,
) -> Result<BTreeMap<i32, Fetched>>
where
    G: Storage + Clone,
{
    let controller = Controller::with_storage(storage)?;

    let topics = [OffsetFetchRequestTopic::default()
        .name(topic.into())
        .partition_indexes(Some(PARTITIONS.into()))];

    let groups = [OffsetFetchRequestGroup::default()
        .group_id(group_id.into())
        .member_id(None)
        .member_epoch(Some(-1))
        .topics(matches!(shape, Shape::ByGroup).then(|| {
            vec![
                OffsetFetchRequestTopics::default()
                    .name(topic.into())
                    .partition_indexes(Some(PARTITIONS.into())),
            ]
        }))];

    let response = match shape {
        Shape::ByTopic => {
            controller
                .offset_fetch(Some(group_id), Some(&topics), None, require_stable)
                .await
        }

        Shape::ByGroup | Shape::AllPartitions => {
            controller
                .offset_fetch(None, None, Some(&groups), require_stable)
                .await
        }
    }
    .and_then(|body| OffsetFetchResponse::try_from(body).map_err(Into::into))?;

    let mut fetched = BTreeMap::new();

    if let Shape::ByTopic = shape {
        assert_eq!(Some(i16::from(ErrorCode::None)), response.error_code);

        for response_topic in response.topics.unwrap_or_default() {
            assert_eq!(topic, response_topic.name);

            for partition in response_topic.partitions.unwrap_or_default() {
                assert_eq!(
                    None,
                    fetched.insert(
                        partition.partition_index,
                        Fetched {
                            error_code: partition.error_code,
                            offset: partition.committed_offset,
                            leader_epoch: partition.committed_leader_epoch.unwrap_or(-1),
                            metadata: partition.metadata.unwrap_or_default(),
                        },
                    )
                );
            }
        }
    } else {
        let groups = response.groups.unwrap_or_default();
        assert_eq!(1, groups.len());
        assert_eq!(group_id, groups[0].group_id);
        assert_eq!(i16::from(ErrorCode::None), groups[0].error_code);

        for response_topic in groups[0].topics.as_deref().unwrap_or_default() {
            assert_eq!(topic, response_topic.name);

            for partition in response_topic.partitions.as_deref().unwrap_or_default() {
                assert_eq!(
                    None,
                    fetched.insert(
                        partition.partition_index,
                        Fetched {
                            error_code: partition.error_code,
                            offset: partition.committed_offset,
                            leader_epoch: partition.committed_leader_epoch,
                            metadata: partition.metadata.clone().unwrap_or_default(),
                        },
                    )
                );
            }
        }
    }

    Ok(fetched)
}

/// Asserts that every shape of `OffsetFetch` answers `expected`.
async fn assert_fetched<G>(
    storage: &G,
    group_id: &str,
    topic: &str,
    require_stable: Option<bool>,
    expected: &BTreeMap<i32, Fetched>,
) -> Result<()>
where
    G: Storage + Clone,
{
    for shape in SHAPES {
        assert_eq!(
            *expected,
            fetch(storage.clone(), group_id, topic, shape, require_stable).await?,
            "{shape:?}, require_stable: {require_stable:?}"
        );
    }

    Ok(())
}

/// Commits `offset` for the partition outside a transaction.
async fn offset_commit<G>(
    storage: &G,
    group_id: &str,
    topic: &str,
    partition: i32,
    offset: i64,
) -> Result<()>
where
    G: Storage,
{
    let outcome = storage
        .offset_commit(
            group_id,
            None,
            &[(
                Topition::new(topic.to_owned(), partition),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;

    assert_eq!(1, outcome.len());
    assert_eq!(ErrorCode::None, outcome[0].1);

    Ok(())
}

/// A transaction that has committed an offset and has not ended.
struct OpenTransaction {
    transaction_id: String,
    producer: ProducerIdResponse,
}

impl OpenTransaction {
    /// Starts a transaction that commits `IN_TRANSACTION` for the group's
    /// `PENDING` partition, as a consume-transform-produce client does with
    /// `AddOffsetsToTxn` and `TxnOffsetCommit`.
    async fn begin<G>(storage: &G, group_id: &str, topic: &str) -> Result<Self>
    where
        G: Storage,
    {
        let transaction_id = alphanumeric_string(15);

        let producer = storage
            .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
            .await?;
        assert_eq!(ErrorCode::None, producer.error);

        assert_eq!(
            ErrorCode::None,
            storage
                .txn_add_offsets(&transaction_id, producer.id, producer.epoch, group_id)
                .await?
        );

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
                                .partition_index(PENDING)
                                .committed_offset(IN_TRANSACTION)
                                .committed_leader_epoch(None)
                                .committed_metadata(None),
                        ])),
                ],
            })
            .await?;

        assert_eq!(
            vec![i16::from(ErrorCode::None)],
            outcome
                .iter()
                .flat_map(|topic| topic.partitions.iter().flatten())
                .map(|partition| partition.error_code)
                .collect::<Vec<_>>()
        );

        Ok(Self {
            transaction_id,
            producer,
        })
    }

    async fn end<G>(self, storage: &G, committed: bool) -> Result<()>
    where
        G: Storage,
    {
        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(
                    &self.transaction_id,
                    self.producer.id,
                    self.producer.epoch,
                    committed,
                )
                .await?
        );

        Ok(())
    }
}

/// Commits `COMMITTED` and `OTHER_COMMITTED` for the group outside a
/// transaction, then starts a transaction that commits `IN_TRANSACTION` for the
/// `PENDING` partition.
async fn commit_then_begin<G>(storage: &G, group_id: &str, topic: &str) -> Result<OpenTransaction>
where
    G: Storage,
{
    offset_commit(storage, group_id, topic, PENDING, COMMITTED).await?;
    offset_commit(storage, group_id, topic, OTHER, OTHER_COMMITTED).await?;

    OpenTransaction::begin(storage, group_id, topic).await
}

/// Only the partition with the transaction's offset is unstable.
pub async fn pending_offset_is_unstable_with_require_stable<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = alphanumeric_string(15);

    let _transaction = commit_then_begin(&storage, &group_id, &topic).await?;

    assert_fetched(
        &storage,
        &group_id,
        &topic,
        Some(true),
        &BTreeMap::from([
            (PENDING, Fetched::unstable()),
            (OTHER, Fetched::stable(OTHER_COMMITTED)),
        ]),
    )
    .await
}

pub async fn pending_offset_is_ignored_without_require_stable<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = alphanumeric_string(15);

    let _transaction = commit_then_begin(&storage, &group_id, &topic).await?;

    let expected = BTreeMap::from([
        (PENDING, Fetched::stable(COMMITTED)),
        (OTHER, Fetched::stable(OTHER_COMMITTED)),
    ]);

    assert_fetched(&storage, &group_id, &topic, Some(false), &expected).await?;
    assert_fetched(&storage, &group_id, &topic, None, &expected).await
}

pub async fn committed_transaction_offset_is_stable<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = alphanumeric_string(15);

    commit_then_begin(&storage, &group_id, &topic)
        .await?
        .end(&storage, true)
        .await?;

    assert_fetched(
        &storage,
        &group_id,
        &topic,
        Some(true),
        &BTreeMap::from([
            (PENDING, Fetched::stable(IN_TRANSACTION)),
            (OTHER, Fetched::stable(OTHER_COMMITTED)),
        ]),
    )
    .await
}

pub async fn aborted_transaction_offset_is_stable<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = alphanumeric_string(15);

    commit_then_begin(&storage, &group_id, &topic)
        .await?
        .end(&storage, false)
        .await?;

    assert_fetched(
        &storage,
        &group_id,
        &topic,
        Some(true),
        &BTreeMap::from([
            (PENDING, Fetched::stable(COMMITTED)),
            (OTHER, Fetched::stable(OTHER_COMMITTED)),
        ]),
    )
    .await
}

/// A transaction's offsets are pending for the group that it commits them for,
/// not for another group that reads the same partition.
pub async fn pending_offset_of_another_group_is_stable<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = alphanumeric_string(15);
    let other_group_id = alphanumeric_string(15);

    offset_commit(&storage, &other_group_id, &topic, PENDING, COMMITTED).await?;
    offset_commit(&storage, &other_group_id, &topic, OTHER, OTHER_COMMITTED).await?;

    let _transaction = commit_then_begin(&storage, &group_id, &topic).await?;

    assert_fetched(
        &storage,
        &other_group_id,
        &topic,
        Some(true),
        &BTreeMap::from([
            (PENDING, Fetched::stable(COMMITTED)),
            (OTHER, Fetched::stable(OTHER_COMMITTED)),
        ]),
    )
    .await
}

/// A partition that the group has never committed is unstable while a
/// transaction holds its offset, when the request names it. A request for every
/// committed offset of the group lists only the partitions with a committed
/// offset, as in Kafka, so it lists none.
pub async fn pending_offset_without_commit_is_unstable<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = alphanumeric_string(15);

    let _transaction = OpenTransaction::begin(&storage, &group_id, &topic).await?;

    let expected = BTreeMap::from([(PENDING, Fetched::unstable()), (OTHER, Fetched::stable(-1))]);

    for shape in [Shape::ByTopic, Shape::ByGroup] {
        assert_eq!(
            expected,
            fetch(storage.clone(), &group_id, &topic, shape, Some(true)).await?,
            "{shape:?}"
        );
    }

    assert_eq!(
        BTreeMap::new(),
        fetch(storage, &group_id, &topic, Shape::AllPartitions, Some(true)).await?
    );

    Ok(())
}

/// A new `InitProducerId` for the transactional ID ends the producer's open
/// transaction in Kafka, so the transaction's offset is no longer pending, and
/// the group's committed offset is stable.
pub async fn reinitialized_producer_ends_pending_offset<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = alphanumeric_string(15);

    let transaction = commit_then_begin(&storage, &group_id, &topic).await?;

    let producer = storage
        .init_producer(
            Some(transaction.transaction_id.as_str()),
            10_000,
            Some(transaction.producer.id),
            Some(transaction.producer.epoch),
        )
        .await?;
    assert_eq!(ErrorCode::None, producer.error);

    assert_fetched(
        &storage,
        &group_id,
        &topic,
        Some(true),
        &BTreeMap::from([
            (PENDING, Fetched::stable(COMMITTED)),
            (OTHER, Fetched::stable(OTHER_COMMITTED)),
        ]),
    )
    .await
}

impl OpenTransaction {
    /// Adds the `PENDING` partition to the transaction and produces one transactional record
    /// to it, so that the transaction holds the partition's last stable offset.
    async fn produce_to_pending<G>(&self, storage: &G, topic: &str) -> Result<()>
    where
        G: Storage,
    {
        _ = storage
            .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id: self.transaction_id.clone(),
                producer_id: self.producer.id,
                producer_epoch: self.producer.epoch,
                topics: vec![
                    AddPartitionsToTxnTopic::default()
                        .name(topic.to_owned())
                        .partitions(Some(vec![PENDING])),
                ],
            })
            .await?;

        let batch: deflated::Batch = inflated::Batch::builder()
            .record(Record::builder().value(Some(Bytes::from_static(b"Lorem ipsum"))))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(self.producer.id)
            .producer_epoch(self.producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(
                Some(self.transaction_id.as_str()),
                &Topition::new(topic.to_owned(), PENDING),
                batch,
            )
            .await?;

        Ok(())
    }
}

/// A transaction whose commit waits behind an older open transaction on the same partition
/// stays prepared with its offset rows. An `InitProducerId` then moves its producer to a new
/// epoch, so the rows belong to an earlier epoch, and they keep the offset pending until the
/// deferred commit applies. Answering the group's older committed offset as stable meanwhile
/// would let a consumer start from it and process the transaction's records twice.
pub async fn deferred_commit_of_reinitialized_producer_keeps_offset_pending<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let topic = create_topic(cluster_id, broker_id, &storage).await?;
    let group_id = alphanumeric_string(15);

    offset_commit(&storage, &group_id, &topic, PENDING, COMMITTED).await?;
    offset_commit(&storage, &group_id, &topic, OTHER, OTHER_COMMITTED).await?;

    // The older transaction produces to the partition, and stays open.
    let older = alphanumeric_string(15);
    let older_producer = storage
        .init_producer(Some(older.as_str()), 10_000, Some(-1), Some(-1))
        .await?;
    assert_eq!(ErrorCode::None, older_producer.error);

    let older = OpenTransaction {
        transaction_id: older,
        producer: older_producer,
    };
    older.produce_to_pending(&storage, &topic).await?;

    // The group's transaction commits an offset and produces to the same partition. Its commit
    // waits for the older transaction.
    let transaction = OpenTransaction::begin(&storage, &group_id, &topic).await?;
    transaction.produce_to_pending(&storage, &topic).await?;

    assert_eq!(
        ErrorCode::None,
        storage
            .txn_end(
                &transaction.transaction_id,
                transaction.producer.id,
                transaction.producer.epoch,
                true,
            )
            .await?
    );

    let producer = storage
        .init_producer(
            Some(transaction.transaction_id.as_str()),
            10_000,
            Some(transaction.producer.id),
            Some(transaction.producer.epoch),
        )
        .await?;
    assert_eq!(ErrorCode::None, producer.error);

    assert_fetched(
        &storage,
        &group_id,
        &topic,
        Some(true),
        &BTreeMap::from([
            (PENDING, Fetched::unstable()),
            (OTHER, Fetched::stable(OTHER_COMMITTED)),
        ]),
    )
    .await?;

    older.end(&storage, true).await?;

    assert_fetched(
        &storage,
        &group_id,
        &topic,
        Some(true),
        &BTreeMap::from([
            (PENDING, Fetched::stable(IN_TRANSACTION)),
            (OTHER, Fetched::stable(OTHER_COMMITTED)),
        ]),
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

            backend!(@test pending_offset_is_unstable_with_require_stable $(, #[$attribute])*);
            backend!(@test pending_offset_is_ignored_without_require_stable $(, #[$attribute])*);
            backend!(@test committed_transaction_offset_is_stable $(, #[$attribute])*);
            backend!(@test aborted_transaction_offset_is_stable $(, #[$attribute])*);
            backend!(@test pending_offset_of_another_group_is_stable $(, #[$attribute])*);
            backend!(@test pending_offset_without_commit_is_unstable $(, #[$attribute])*);
            backend!(@test reinitialized_producer_ends_pending_offset $(, #[$attribute])*);
            backend!(@test deferred_commit_of_reinitialized_producer_keeps_offset_pending $(, #[$attribute])*);
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
