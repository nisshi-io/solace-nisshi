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

use std::time::{Duration, SystemTime};

use crate::common::{self, StorageType, alphanumeric_string, init_tracing, register_broker};
use bytes::Bytes;
use nisshi_broker::{Error, Result, service::storage};
use nisshi_sans_io::{
    Ack, BatchAttribute, CreateTopicsRequest, ErrorCode, IsolationLevel, ListOffset,
    ListOffsetsRequest, ProduceRequest, RequestInput,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated},
    to_timestamp,
};
use nisshi_service::{
    BytesFrameLayer, BytesFrameService, BytesLayer, BytesService, FrameBytesLayer,
    FrameBytesService, FrameRouteService, RequestFrameLayer, RequestFrameService,
};
use nisshi_storage::{
    AutoCreateRefusals, Storage, TopicCreationConfig, Topition, TxnAddPartitionsRequest,
};
use rama::{Layer as _, Service, extensions::Extensions};
use rand::{prelude::*, rng};
use tracing::debug;
use url::Url;
use uuid::Uuid;

pub(crate) type Broker = RequestFrameService<
    FrameBytesService<BytesService<BytesFrameService<FrameRouteService<Error>>>>,
>;

pub(crate) fn broker<S>(storage: S) -> Result<Broker>
where
    S: Storage + Clone,
{
    storage::services(
        FrameRouteService::<Error>::builder(),
        storage,
        TopicCreationConfig::default(),
        AutoCreateRefusals::default(),
    )
    .and_then(|builder| builder.build().map_err(Into::into))
    .map(|frame_route| {
        (
            RequestFrameLayer,
            FrameBytesLayer,
            BytesLayer,
            BytesFrameLayer::default(),
        )
            .into_layer(frame_route)
    })
}

/// How many records a test produces before retention deletes them all, at offsets 0 to 4.
const PRODUCED: i64 = 5;

/// A `retention.ms` of ten minutes, which a maintenance run an hour later has expired.
const TEN_MINUTE_RETENTION: &str = "600000";

/// How far after producing the records a test runs maintenance, past their `retention.ms`.
const AN_HOUR: Duration = Duration::from_secs(3_600);

/// How far ahead of now a test sets a record's `CreateTime`, so that the record is still within
/// its `retention.ms` when maintenance runs [`AN_HOUR`] later. Produce rejects a `CreateTime`
/// more than an hour ahead, the default `message.timestamp.after.max.ms`.
const FIFTY_FIVE_MINUTES: Duration = Duration::from_secs(3_300);

async fn create_topic(
    broker: &Broker,
    extensions: &Extensions,
    name: &str,
    configs: &[(&str, &str)],
) -> Result<()> {
    let response = broker
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .timeout_ms(5_000)
                .validate_only(Some(false))
                .topics(Some(
                    [CreatableTopic::default()
                        .num_partitions(1)
                        .configs(Some(
                            configs
                                .iter()
                                .map(|(name, value)| {
                                    CreatableTopicConfig::default()
                                        .name((*name).into())
                                        .value(Some((*value).into()))
                                })
                                .collect(),
                        ))
                        .assignments(Some([].into()))
                        .replication_factor(1)
                        .name(name.into())]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(i16::from(ErrorCode::None), topics[0].error_code);
    Ok(())
}

async fn produce(
    broker: &Broker,
    extensions: &Extensions,
    name: &str,
    value: Bytes,
) -> Result<i64> {
    produce_with_create_time(broker, extensions, name, value, SystemTime::now()).await
}

/// Produces one record with `create_time` as its `CreateTime`, which retention compares with
/// `retention.ms`, and returns its offset.
async fn produce_with_create_time(
    broker: &Broker,
    extensions: &Extensions,
    name: &str,
    value: Bytes,
    create_time: SystemTime,
) -> Result<i64> {
    let timestamp = to_timestamp(&create_time)?;

    let frame = inflated::Batch::builder()
        .base_timestamp(timestamp)
        .max_timestamp(timestamp)
        .record(Record::builder().value(Some(value)))
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)?;

    let response = broker
        .serve(RequestInput {
            request: ProduceRequest::default()
                .timeout_ms(5_000)
                .acks(Ack::Leader.into())
                .topic_data(Some(
                    [TopicProduceData::default()
                        .name(name.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(0)
                                .records(Some(frame))]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await
        .inspect(|response| debug!("{response:?}"))?;

    let partition = &response.responses.as_deref().unwrap_or_default()[0]
        .partition_responses
        .as_deref()
        .unwrap_or_default()[0];
    assert_eq!(i16::from(ErrorCode::None), partition.error_code);
    Ok(partition.base_offset)
}

async fn list_offset(
    broker: &Broker,
    extensions: &Extensions,
    name: &str,
    isolation_level: IsolationLevel,
    which: ListOffset,
) -> Result<Option<i64>> {
    let response = broker
        .serve(RequestInput {
            request: ListOffsetsRequest::default()
                .isolation_level(Some(isolation_level.into()))
                .replica_id(-1)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(name.into())
                        .partitions(Some(
                            [ListOffsetsPartition::default()
                                .partition_index(0)
                                .max_num_offsets(Some(1))
                                .timestamp(which.try_into()?)
                                .current_leader_epoch(Some(-1))]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let partition = &response.topics.as_deref().unwrap_or_default()[0]
        .partitions
        .as_deref()
        .unwrap_or_default()[0];
    assert_eq!(i16::from(ErrorCode::None), partition.error_code);
    Ok(partition.offset)
}

/// When retention deletes every record of a partition, the earliest and the latest offset are both
/// the offset after the last deleted record, and the next record gets that offset.
pub async fn offsets_kept_when_retention_empties_partition<G>(sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;
    let extensions = Extensions::default();
    let topic = &alphanumeric_string(15)[..];

    create_topic(
        &broker,
        &extensions,
        topic,
        &[
            ("cleanup.policy", "delete"),
            ("retention.ms", TEN_MINUTE_RETENTION),
        ],
    )
    .await?;

    for n in 0..PRODUCED {
        let value = Bytes::from(format!("value-{n}"));
        assert_eq!(n, produce(&broker, &extensions, topic, value).await?);
    }

    sc.maintain(SystemTime::now() + AN_HOUR).await?;

    for isolation_level in [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
    ] {
        for which in [ListOffset::Earliest, ListOffset::Latest] {
            assert_eq!(
                Some(PRODUCED),
                list_offset(&broker, &extensions, topic, isolation_level, which).await?,
                "{isolation_level:?} {which:?}"
            );
        }
    }

    let value = Bytes::from_static(b"after-retention");
    assert_eq!(PRODUCED, produce(&broker, &extensions, topic, value).await?);

    Ok(())
}

/// A partition that has never held a record answers 0 for its earliest and latest offset.
pub async fn offsets_of_new_partition_are_zero<G>(sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;
    let extensions = Extensions::default();
    let topic = &alphanumeric_string(15)[..];

    create_topic(&broker, &extensions, topic, &[("cleanup.policy", "delete")]).await?;

    for isolation_level in [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
    ] {
        for which in [ListOffset::Earliest, ListOffset::Latest] {
            assert_eq!(
                Some(0),
                list_offset(&broker, &extensions, topic, isolation_level, which).await?,
                "{isolation_level:?} {which:?}"
            );
        }
    }

    Ok(())
}

/// Opens a transaction on partition 0 of `topic`, produces one record in it, and returns the
/// record's offset. The transaction stays open.
async fn produce_in_open_transaction<G>(sc: &G, topic: &str) -> Result<i64>
where
    G: Storage,
{
    let transaction_id = alphanumeric_string(10);
    let producer = sc
        .init_producer(Some(transaction_id.as_str()), 10_000, Some(-1), Some(-1))
        .await?;

    _ = sc
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: transaction_id.clone(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: vec![
                AddPartitionsToTxnTopic::default()
                    .name(topic.into())
                    .partitions(Some(vec![0])),
            ],
        })
        .await?;

    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"uncommitted"))))
        .attributes(BatchAttribute::default().transaction(true).into())
        .producer_id(producer.id)
        .producer_epoch(producer.epoch)
        .base_sequence(0)
        .build()
        .and_then(TryInto::try_into)?;

    sc.produce(
        Some(transaction_id.as_str()),
        &Topition::new(topic, 0),
        batch,
    )
    .await
    .map_err(Into::into)
}

/// Creates a topic with a ten minute `retention.ms`, and produces three records to it: `first`
/// at `first_create_time`, one in a transaction that stays open, and one more at the current
/// time. Returns the offset of the transaction's record.
pub(crate) async fn create_topic_with_open_transaction<G>(
    sc: &G,
    broker: &Broker,
    extensions: &Extensions,
    topic: &str,
    first_create_time: SystemTime,
) -> Result<i64>
where
    G: Storage,
{
    create_topic(
        broker,
        extensions,
        topic,
        &[
            ("cleanup.policy", "delete"),
            ("retention.ms", TEN_MINUTE_RETENTION),
        ],
    )
    .await?;

    let first = Bytes::from_static(b"first");
    assert_eq!(
        0,
        produce_with_create_time(broker, extensions, topic, first, first_create_time).await?
    );

    let transaction_start = produce_in_open_transaction(sc, topic).await?;
    assert_eq!(1, transaction_start);

    assert_eq!(
        2,
        produce(broker, extensions, topic, Bytes::from_static(b"after")).await?
    );

    Ok(transaction_start)
}

/// An open transaction pins `read_committed` Latest at the transaction's first offset, even after
/// retention deletes the record at that offset. A latest offset above it would let a
/// `read_committed` consumer read past records that the transaction may still abort.
pub async fn open_transaction_pins_read_committed_latest_after_retention<G>(sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;
    let extensions = Extensions::default();
    let topic = &alphanumeric_string(15)[..];

    // The first record is newer than the others, so retention keeps it, and the transaction's
    // first offset stays above the earliest offset.
    let transaction_start = create_topic_with_open_transaction(
        &sc,
        &broker,
        &extensions,
        topic,
        SystemTime::now() + FIFTY_FIVE_MINUTES,
    )
    .await?;

    sc.maintain(SystemTime::now() + AN_HOUR).await?;

    assert_eq!(
        Some(0),
        list_offset(
            &broker,
            &extensions,
            topic,
            IsolationLevel::ReadCommitted,
            ListOffset::Earliest
        )
        .await?
    );
    assert_eq!(
        Some(transaction_start),
        list_offset(
            &broker,
            &extensions,
            topic,
            IsolationLevel::ReadCommitted,
            ListOffset::Latest
        )
        .await?
    );
    assert_eq!(
        Some(3),
        list_offset(
            &broker,
            &extensions,
            topic,
            IsolationLevel::ReadUncommitted,
            ListOffset::Latest
        )
        .await?
    );

    Ok(())
}

/// When retention deletes every record of a partition with an open transaction, `read_committed`
/// Latest answers the earliest offset. Kafka never answers a last stable offset below the log
/// start, so a consumer that resets to Latest gets an offset that it can fetch from.
pub async fn read_committed_latest_is_earliest_when_retention_deletes_open_transaction<G>(
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;
    let extensions = Extensions::default();
    let topic = &alphanumeric_string(15)[..];

    _ = create_topic_with_open_transaction(&sc, &broker, &extensions, topic, SystemTime::now())
        .await?;

    sc.maintain(SystemTime::now() + AN_HOUR).await?;

    for which in [ListOffset::Earliest, ListOffset::Latest] {
        assert_eq!(
            Some(3),
            list_offset(
                &broker,
                &extensions,
                topic,
                IsolationLevel::ReadCommitted,
                which
            )
            .await?,
            "{which:?}"
        );
    }

    Ok(())
}

/// When DeleteRecords moves the log start past an open transaction's first offset,
/// `read_committed` Latest answers the new log start, as in Kafka.
pub async fn read_committed_latest_is_log_start_after_delete_records_past_open_transaction<G>(
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;
    let extensions = Extensions::default();
    let topic = &alphanumeric_string(15)[..];

    let transaction_start =
        create_topic_with_open_transaction(&sc, &broker, &extensions, topic, SystemTime::now())
            .await?;

    let log_start = transaction_start + 1;

    let results = sc
        .delete_records(&[DeleteRecordsTopic::default()
            .name(topic.into())
            .partitions(Some(
                [DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(log_start)]
                .into(),
            ))])
        .await?;
    let partition = &results[0].partitions.as_deref().unwrap_or_default()[0];
    assert_eq!(i16::from(ErrorCode::None), partition.error_code);
    assert_eq!(log_start, partition.low_watermark);

    for which in [ListOffset::Earliest, ListOffset::Latest] {
        assert_eq!(
            Some(log_start),
            list_offset(
                &broker,
                &extensions,
                topic,
                IsolationLevel::ReadCommitted,
                which
            )
            .await?,
            "{which:?}"
        );
    }

    Ok(())
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage_type:expr) => {
        #[cfg(feature = $feature)]
        mod $name {
            use nisshi_storage::ArcDynStorage;

            use super::*;

            async fn storage_container() -> Result<ArcDynStorage> {
                let cluster_id = Uuid::now_v7();
                let broker_id = rng().random_range(0..i32::MAX);

                let sc = common::storage_container(
                    $storage_type,
                    cluster_id,
                    broker_id,
                    Url::parse("tcp://127.0.0.1/")?,
                    None,
                )
                .await?;
                register_broker(cluster_id, broker_id, &sc).await?;
                Ok(sc)
            }

            #[tokio::test]
            async fn offsets_kept_when_retention_empties_partition() -> Result<()> {
                let _guard = init_tracing()?;
                super::offsets_kept_when_retention_empties_partition(storage_container().await?)
                    .await
            }

            #[tokio::test]
            async fn open_transaction_pins_read_committed_latest_after_retention() -> Result<()> {
                let _guard = init_tracing()?;
                super::open_transaction_pins_read_committed_latest_after_retention(
                    storage_container().await?,
                )
                .await
            }

            #[tokio::test]
            async fn read_committed_latest_is_earliest_when_retention_deletes_open_transaction(
            ) -> Result<()> {
                let _guard = init_tracing()?;
                super::read_committed_latest_is_earliest_when_retention_deletes_open_transaction(
                    storage_container().await?,
                )
                .await
            }

            #[tokio::test]
            async fn read_committed_latest_is_log_start_after_delete_records_past_open_transaction(
            ) -> Result<()> {
                let _guard = init_tracing()?;
                super::read_committed_latest_is_log_start_after_delete_records_past_open_transaction(
                    storage_container().await?,
                )
                .await
            }

            #[tokio::test]
            async fn offsets_of_new_partition_are_zero() -> Result<()> {
                let _guard = init_tracing()?;
                super::offsets_of_new_partition_are_zero(storage_container().await?).await
            }
        }
    };
}

backend!(pg, "postgres", StorageType::Postgres);
backend!(lite, "libsql", StorageType::Lite);

// DynoStore has no retention, so only the tests that move the log start with
// DeleteRecords run on it.
#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;

    #[tokio::test]
    async fn read_committed_latest_is_log_start_after_delete_records_past_open_transaction()
    -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = common::storage_container(
            StorageType::InMemory,
            cluster_id,
            broker_id,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::read_committed_latest_is_log_start_after_delete_records_past_open_transaction(sc)
            .await
    }
}
