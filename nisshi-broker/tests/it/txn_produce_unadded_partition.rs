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

//! A transactional batch for a partition that is not in its producer's ongoing transaction is
//! refused and not stored, as Kafka does (KIP-890). Storing it would leave records that no
//! transaction marker ever closes, holding back the partition's last stable offset.

use std::assert_matches;

use crate::common::{StorageType, alphanumeric_string, register_broker};
use bytes::Bytes;
use nisshi_broker::{Error, Result};
use nisshi_sans_io::{
    ApiKey as _, BatchAttribute, Body, ErrorCode, Frame, FrameInput, Header, IsolationLevel,
    ListOffset, ProduceRequest,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::CreatableTopic,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated},
};
use nisshi_service::FrameRouteService;
use nisshi_storage::{ArcDynStorage, Storage, Topition, TxnAddPartitionsRequest};
use rama::{Service as _, extensions::Extensions};
use rand::{RngExt as _, rng};
use url::Url;
use uuid::Uuid;

const TRANSACTION_TIMEOUT_MS: i32 = 10_000;

async fn storage(storage_type: StorageType) -> Result<ArcDynStorage> {
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

    Ok(storage)
}

/// Creates a topic with two partitions, and returns its name.
async fn create_topic(storage: &ArcDynStorage) -> Result<String> {
    let name = alphanumeric_string(15);

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.clone())
                .num_partitions(2)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(name)
}

/// A transactional producer: its transactional ID, producer ID and epoch.
struct Producer {
    transaction_id: String,
    id: i64,
    epoch: i16,
}

async fn init_producer(storage: &ArcDynStorage) -> Result<Producer> {
    let transaction_id = alphanumeric_string(10);

    let producer = storage
        .init_producer(
            Some(transaction_id.as_str()),
            TRANSACTION_TIMEOUT_MS,
            Some(-1),
            Some(-1),
        )
        .await?;

    Ok(Producer {
        transaction_id,
        id: producer.id,
        epoch: producer.epoch,
    })
}

async fn add_partition(
    storage: &ArcDynStorage,
    producer: &Producer,
    topition: &Topition,
) -> Result<()> {
    _ = storage
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: producer.transaction_id.clone(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: [AddPartitionsToTxnTopic::default()
                .name(topition.topic().into())
                .partitions(Some([topition.partition()].into()))]
            .into(),
        })
        .await?;

    Ok(())
}

/// A batch of one record, transactional when `transactional` is set, from `producer` with
/// first sequence `base_sequence`.
fn batch(producer: &Producer, transactional: bool, base_sequence: i32) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"Lorem ipsum"))))
        .attributes(BatchAttribute::default().transaction(transactional).into())
        .producer_id(producer.id)
        .producer_epoch(producer.epoch)
        .base_sequence(base_sequence)
        .build()
        .and_then(TryInto::try_into)
        .map_err(Into::into)
}

/// The partition's high watermark, which counts every stored record, committed or not.
async fn high_watermark(storage: &ArcDynStorage, topition: &Topition) -> Result<Option<i64>> {
    let responses = storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await?;

    assert_eq!(1, responses.len());
    assert_eq!(ErrorCode::None, responses[0].1.error_code);

    Ok(responses[0].1.offset)
}

/// A producer that never adds the partition to its transaction has its transactional batch
/// refused, and nothing is stored.
async fn never_added(storage_type: StorageType) -> Result<()> {
    let storage = storage(storage_type).await?;
    let topition = Topition::new(create_topic(&storage).await?, 0);
    let producer = init_producer(&storage).await?;

    assert_matches!(
        storage
            .produce(
                Some(producer.transaction_id.as_str()),
                &topition,
                batch(&producer, true, 0)?,
            )
            .await,
        Err(nisshi_storage::Error::Api(ErrorCode::TransactionAbortable))
    );

    assert_eq!(Some(0), high_watermark(&storage, &topition).await?);

    Ok(())
}

/// A producer that added partition 0 stores a batch there, and has a batch for partition 1
/// refused.
async fn other_partition_added(storage_type: StorageType) -> Result<()> {
    let storage = storage(storage_type).await?;
    let topic = create_topic(&storage).await?;
    let added = Topition::new(topic.clone(), 0);
    let not_added = Topition::new(topic, 1);
    let producer = init_producer(&storage).await?;

    add_partition(&storage, &producer, &added).await?;

    assert_eq!(
        0,
        storage
            .produce(
                Some(producer.transaction_id.as_str()),
                &added,
                batch(&producer, true, 0)?,
            )
            .await?
    );

    assert_matches!(
        storage
            .produce(
                Some(producer.transaction_id.as_str()),
                &not_added,
                batch(&producer, true, 0)?,
            )
            .await,
        Err(nisshi_storage::Error::Api(ErrorCode::TransactionAbortable))
    );

    assert_eq!(Some(1), high_watermark(&storage, &added).await?);
    assert_eq!(Some(0), high_watermark(&storage, &not_added).await?);

    Ok(())
}

/// Ending a transaction takes its partitions out of it, so a producer must add a partition
/// again before its next transaction writes there.
async fn ended_transaction(storage_type: StorageType) -> Result<()> {
    let storage = storage(storage_type).await?;
    let topition = Topition::new(create_topic(&storage).await?, 0);
    let producer = init_producer(&storage).await?;

    add_partition(&storage, &producer, &topition).await?;

    assert_eq!(
        0,
        storage
            .produce(
                Some(producer.transaction_id.as_str()),
                &topition,
                batch(&producer, true, 0)?,
            )
            .await?
    );

    assert_eq!(
        ErrorCode::None,
        storage
            .txn_end(
                producer.transaction_id.as_str(),
                producer.id,
                producer.epoch,
                true,
            )
            .await?
    );

    // The record and the commit marker.
    let stored = high_watermark(&storage, &topition).await?;
    assert_eq!(Some(2), stored);

    assert_matches!(
        storage
            .produce(
                Some(producer.transaction_id.as_str()),
                &topition,
                batch(&producer, true, 1)?,
            )
            .await,
        Err(nisshi_storage::Error::Api(ErrorCode::TransactionAbortable))
    );

    assert_eq!(stored, high_watermark(&storage, &topition).await?);

    Ok(())
}

/// Only a transactional batch is checked: an idempotent, non-transactional batch in a request
/// with a transactional ID is stored, as in Kafka.
async fn non_transactional_batch(storage_type: StorageType) -> Result<()> {
    let storage = storage(storage_type).await?;
    let topition = Topition::new(create_topic(&storage).await?, 0);
    let producer = init_producer(&storage).await?;

    assert_eq!(
        0,
        storage
            .produce(
                Some(producer.transaction_id.as_str()),
                &topition,
                batch(&producer, false, 0)?,
            )
            .await?
    );

    assert_eq!(Some(1), high_watermark(&storage, &topition).await?);

    Ok(())
}

/// A transactional batch whose transactional ID was never initialised is refused with
/// INVALID_PRODUCER_ID_MAPPING, which Kafka's coordinator answers for an unknown
/// transactional ID.
async fn unknown_transactional_id(storage_type: StorageType) -> Result<()> {
    let storage = storage(storage_type).await?;
    let topition = Topition::new(create_topic(&storage).await?, 0);

    // An idempotent producer ID, which no transactional ID owns.
    let idempotent = storage
        .init_producer(None, TRANSACTION_TIMEOUT_MS, Some(-1), Some(-1))
        .await?;

    let producer = Producer {
        transaction_id: alphanumeric_string(10),
        id: idempotent.id,
        epoch: idempotent.epoch,
    };

    assert_matches!(
        storage
            .produce(
                Some(producer.transaction_id.as_str()),
                &topition,
                batch(&producer, true, 0)?,
            )
            .await,
        Err(nisshi_storage::Error::Api(
            ErrorCode::InvalidProducerIdMapping
        ))
    );

    assert_eq!(Some(0), high_watermark(&storage, &topition).await?);

    Ok(())
}

/// Through the broker's Produce route, a client that sent Produce v11 gets
/// TRANSACTION_ABORTABLE and an older client gets INVALID_TXN_STATE, as from Kafka.
async fn error_code_by_version(storage_type: StorageType) -> Result<()> {
    let storage = storage(storage_type).await?;
    let topition = Topition::new(create_topic(&storage).await?, 0);
    let producer = init_producer(&storage).await?;

    let route = nisshi_broker::service::storage::produce(
        FrameRouteService::<Error>::builder(),
        storage.clone(),
    )
    .and_then(|builder| builder.build().map_err(Into::into))?;

    for (api_version, expected) in [
        (3, ErrorCode::InvalidTxnState),
        (10, ErrorCode::InvalidTxnState),
        (11, ErrorCode::TransactionAbortable),
    ] {
        let request = ProduceRequest::default()
            .transactional_id(Some(producer.transaction_id.clone()))
            .acks(-1)
            .timeout_ms(5_000)
            .topic_data(Some(
                [TopicProduceData::default()
                    .name(topition.topic().into())
                    .partition_data(Some(
                        [PartitionProduceData::default()
                            .index(topition.partition())
                            .records(Some(deflated::Frame {
                                batches: vec![batch(&producer, true, 0)?],
                            }))]
                        .into(),
                    ))]
                .into(),
            ));

        let response = route
            .serve(FrameInput {
                frame: Frame {
                    size: 0,
                    header: Header::Request {
                        api_key: ProduceRequest::KEY,
                        api_version,
                        correlation_id: 12321,
                        client_id: None,
                    },
                    body: request.into(),
                },
                extensions: Extensions::default(),
            })
            .await?;

        let Body::ProduceResponse(response) = response.body else {
            panic!("a produce response: {response:?}");
        };

        let error_codes = response
            .responses
            .iter()
            .flatten()
            .flat_map(|topic| topic.partition_responses.iter().flatten())
            .map(|partition| partition.error_code)
            .collect::<Vec<_>>();

        assert_eq!(vec![i16::from(expected)], error_codes, "v{api_version}");
    }

    assert_eq!(Some(0), high_watermark(&storage, &topition).await?);

    Ok(())
}

/// A transaction that has asked to end while an older transaction still holds the partition is
/// preparing: its marker is written and its partitions stay listed until the older one ends.
/// A batch for it is refused, as Kafka answers CONCURRENT_TRANSACTIONS, and not stored after
/// the marker.
async fn preparing_transaction(storage_type: StorageType) -> Result<()> {
    let storage = storage(storage_type).await?;
    let topition = Topition::new(create_topic(&storage).await?, 0);

    let older = init_producer(&storage).await?;
    let younger = init_producer(&storage).await?;

    for producer in [&older, &younger] {
        add_partition(&storage, producer, &topition).await?;

        _ = storage
            .produce(
                Some(producer.transaction_id.as_str()),
                &topition,
                batch(producer, true, 0)?,
            )
            .await?;
    }

    // The younger transaction cannot finish while the older one is open.
    assert_eq!(
        ErrorCode::None,
        storage
            .txn_end(
                younger.transaction_id.as_str(),
                younger.id,
                younger.epoch,
                true,
            )
            .await?
    );

    let stored = high_watermark(&storage, &topition).await?;

    assert_matches!(
        storage
            .produce(
                Some(younger.transaction_id.as_str()),
                &topition,
                batch(&younger, true, 1)?,
            )
            .await,
        Err(nisshi_storage::Error::Api(
            ErrorCode::ConcurrentTransactions
        ))
    );

    assert_eq!(stored, high_watermark(&storage, &topition).await?);

    Ok(())
}

macro_rules! engine_tests {
    ($engine:ident, $storage_type:expr) => {
        mod $engine {
            use super::*;

            #[tokio::test]
            async fn never_added() -> Result<()> {
                let _guard = crate::common::init_tracing()?;
                super::never_added($storage_type).await
            }

            #[tokio::test]
            async fn other_partition_added() -> Result<()> {
                let _guard = crate::common::init_tracing()?;
                super::other_partition_added($storage_type).await
            }

            #[tokio::test]
            async fn ended_transaction() -> Result<()> {
                let _guard = crate::common::init_tracing()?;
                super::ended_transaction($storage_type).await
            }

            #[tokio::test]
            async fn preparing_transaction() -> Result<()> {
                let _guard = crate::common::init_tracing()?;
                super::preparing_transaction($storage_type).await
            }

            #[tokio::test]
            async fn non_transactional_batch() -> Result<()> {
                let _guard = crate::common::init_tracing()?;
                super::non_transactional_batch($storage_type).await
            }

            #[tokio::test]
            async fn unknown_transactional_id() -> Result<()> {
                let _guard = crate::common::init_tracing()?;
                super::unknown_transactional_id($storage_type).await
            }

            #[tokio::test]
            async fn error_code_by_version() -> Result<()> {
                let _guard = crate::common::init_tracing()?;
                super::error_code_by_version($storage_type).await
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
