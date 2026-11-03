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

//! Tests that maintenance deletes the records that a topic's `retention.ms` expires, and keeps the
//! others, as Kafka does.

use std::time::{Duration, SystemTime};

use crate::common::{self, StorageType, alphanumeric_string, init_tracing, register_broker};
use bytes::Bytes;
use nisshi_broker::{Error, Result, service::storage};
use nisshi_sans_io::{
    Ack, CreateTopicsRequest, ErrorCode, IsolationLevel, ListOffset, ListOffsetsRequest,
    ProduceRequest, RequestInput,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated},
};
use nisshi_service::{
    BytesFrameLayer, BytesFrameService, BytesLayer, BytesService, FrameBytesLayer,
    FrameBytesService, FrameRouteService, RequestFrameLayer, RequestFrameService,
};
use nisshi_storage::{AutoCreateRefusals, Storage, TopicCreationConfig};
use rama::{Layer as _, Service, extensions::Extensions};
use rand::{prelude::*, rng};
use url::Url;
use uuid::Uuid;

type Broker = RequestFrameService<
    FrameBytesService<BytesService<BytesFrameService<FrameRouteService<Error>>>>,
>;

fn broker<S>(storage: S) -> Result<Broker>
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

/// A retention that has expired a record that is an hour old.
const TEN_MINUTE_RETENTION: (&str, &str) = ("retention.ms", "600000");

/// A retention larger than a 32-bit integer holds.
const THIRTY_DAY_RETENTION: (&str, &str) = ("retention.ms", "2592000000");

const KEEP_FOREVER_RETENTION: (&str, &str) = ("retention.ms", "-1");

const DELETE_POLICY: (&str, &str) = ("cleanup.policy", "delete");

const COMPACT_POLICY: (&str, &str) = ("cleanup.policy", "compact");

/// The `cleanup.policy` that `SUBTRACT` leaves when it removes the last policy.
const EMPTY_POLICY: (&str, &str) = ("cleanup.policy", "");

/// How many records a test produces before maintenance, at offsets 0, 1 and 2.
const RECORDS_BEFORE_MAINTENANCE: i64 = 3;

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

/// Creates a topic with `configs` through [`Storage::create_topic`], which stores them without the
/// broker's checks. A test uses it to set up a topic that an earlier version of the broker created,
/// with a config value that the broker now rejects.
async fn create_topic_without_validation<G>(
    sc: &G,
    name: &str,
    configs: &[(&str, &str)],
) -> Result<()>
where
    G: Storage,
{
    _ = sc
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some(
                    configs
                        .iter()
                        .map(|(name, value)| {
                            CreatableTopicConfig::default()
                                .name((*name).into())
                                .value(Some((*value).into()))
                        })
                        .collect(),
                )),
            false,
        )
        .await?;
    Ok(())
}

/// Produces one record to partition 0 of `name`, with the current time as its timestamp, and
/// returns its offset.
async fn produce(broker: &Broker, extensions: &Extensions, name: &str) -> Result<i64> {
    let timestamp = i64::try_from(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_millis(),
    )?;

    let frame = inflated::Batch::builder()
        .base_timestamp(timestamp)
        .max_timestamp(timestamp)
        .record(Record::builder().value(Some(Bytes::from_static(b"value"))))
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
        .await?;

    let partition = &response.responses.as_deref().unwrap_or_default()[0]
        .partition_responses
        .as_deref()
        .unwrap_or_default()[0];
    assert_eq!(i16::from(ErrorCode::None), partition.error_code);
    Ok(partition.base_offset)
}

async fn produce_records_before_maintenance(
    broker: &Broker,
    extensions: &Extensions,
    name: &str,
) -> Result<()> {
    for _ in 0..RECORDS_BEFORE_MAINTENANCE {
        _ = produce(broker, extensions, name).await?;
    }
    Ok(())
}

/// Runs maintenance with a clock an hour ahead, so that the records produced before it are an
/// hour old, both by their timestamp and by the time that the broker appended them.
async fn maintain_an_hour_later<G>(sc: &G) -> Result<()>
where
    G: Storage,
{
    sc.maintain(SystemTime::now() + Duration::from_hours(1))
        .await
        .map_err(Into::into)
}

async fn list_offset(
    broker: &Broker,
    extensions: &Extensions,
    name: &str,
    which: ListOffset,
) -> Result<Option<i64>> {
    let response = broker
        .serve(RequestInput {
            request: ListOffsetsRequest::default()
                .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
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

/// Produces a record after maintenance, and returns the earliest offset of `name`. The new record
/// stops the partition being empty, because the broker reports an earliest offset of 0 for a
/// partition that maintenance emptied. The earliest offset is then 0 when maintenance kept the
/// records before it, and [`RECORDS_BEFORE_MAINTENANCE`] when it deleted them.
async fn earliest_offset_after_maintenance(
    broker: &Broker,
    extensions: &Extensions,
    name: &str,
) -> Result<Option<i64>> {
    assert_eq!(
        RECORDS_BEFORE_MAINTENANCE,
        produce(broker, extensions, name).await?
    );
    list_offset(broker, extensions, name, ListOffset::Earliest).await
}

/// Kafka's default `cleanup.policy` is `delete`, so maintenance must delete the expired records of
/// a topic that sets only `retention.ms`. A broker that skips the topic lets it grow forever.
pub async fn topic_without_cleanup_policy_loses_expired_records<G>(sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;
    let extensions = Extensions::default();
    let topic = &alphanumeric_string(15)[..];
    create_topic(&broker, &extensions, topic, &[TEN_MINUTE_RETENTION]).await?;
    produce_records_before_maintenance(&broker, &extensions, topic).await?;

    maintain_an_hour_later(&sc).await?;

    assert_eq!(
        Some(RECORDS_BEFORE_MAINTENANCE),
        earliest_offset_after_maintenance(&broker, &extensions, topic).await?
    );
    Ok(())
}

/// `retention.ms=-1` means that the topic keeps every record, however old. A broker that reads it
/// as a retention of 0 deletes a topic that is the only copy of its data.
pub async fn retention_ms_minus_1_keeps_every_record<G>(sc: G) -> Result<()>
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
        &[DELETE_POLICY, KEEP_FOREVER_RETENTION],
    )
    .await?;
    produce_records_before_maintenance(&broker, &extensions, topic).await?;

    maintain_an_hour_later(&sc).await?;

    assert_eq!(
        Some(0),
        earliest_offset_after_maintenance(&broker, &extensions, topic).await?
    );
    Ok(())
}

/// A topic's `retention.ms` of 30 days must not stop maintenance deleting the expired records of
/// another topic. A broker that fails on the value never deletes another expired record, so its
/// storage grows until it is full.
pub async fn thirty_day_retention_does_not_stop_retention_on_another_topic<G>(sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;
    let extensions = Extensions::default();
    let thirty_day_topic = &alphanumeric_string(15)[..];
    create_topic(
        &broker,
        &extensions,
        thirty_day_topic,
        &[DELETE_POLICY, THIRTY_DAY_RETENTION],
    )
    .await?;
    // The topic holds a record, so that maintenance checks the record's age against the topic's
    // `retention.ms`.
    _ = produce(&broker, &extensions, thirty_day_topic).await?;
    let ten_minute_topic = &alphanumeric_string(15)[..];
    create_topic(
        &broker,
        &extensions,
        ten_minute_topic,
        &[DELETE_POLICY, TEN_MINUTE_RETENTION],
    )
    .await?;
    produce_records_before_maintenance(&broker, &extensions, ten_minute_topic).await?;

    maintain_an_hour_later(&sc).await?;

    assert_eq!(
        Some(RECORDS_BEFORE_MAINTENANCE),
        earliest_offset_after_maintenance(&broker, &extensions, ten_minute_topic).await?
    );
    Ok(())
}

/// A topic with a `retention.ms` of 30 days must keep a record an hour old. A broker that reads the
/// value into a 32-bit integer gets a negative or a small retention, and deletes the record.
pub async fn thirty_day_retention_keeps_an_hour_old_record<G>(sc: G) -> Result<()>
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
        &[DELETE_POLICY, THIRTY_DAY_RETENTION],
    )
    .await?;
    produce_records_before_maintenance(&broker, &extensions, topic).await?;

    maintain_an_hour_later(&sc).await?;

    assert_eq!(
        Some(0),
        earliest_offset_after_maintenance(&broker, &extensions, topic).await?
    );
    Ok(())
}

/// A `retention.ms` that isn't a whole number must not stop maintenance deleting the expired
/// records of another topic, and must not delete the records of its own topic. The broker rejects
/// such a value, so only a topic created before the broker checked the value holds it.
pub async fn stored_retention_ms_that_is_not_a_number_does_not_stop_retention<G>(
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;
    let extensions = Extensions::default();
    let not_a_number_topic = &alphanumeric_string(15)[..];
    create_topic_without_validation(
        &sc,
        not_a_number_topic,
        &[DELETE_POLICY, ("retention.ms", "abc")],
    )
    .await?;
    produce_records_before_maintenance(&broker, &extensions, not_a_number_topic).await?;
    let ten_minute_topic = &alphanumeric_string(15)[..];
    create_topic(
        &broker,
        &extensions,
        ten_minute_topic,
        &[DELETE_POLICY, TEN_MINUTE_RETENTION],
    )
    .await?;
    produce_records_before_maintenance(&broker, &extensions, ten_minute_topic).await?;

    maintain_an_hour_later(&sc).await?;

    assert_eq!(
        Some(RECORDS_BEFORE_MAINTENANCE),
        earliest_offset_after_maintenance(&broker, &extensions, ten_minute_topic).await?
    );
    assert_eq!(
        Some(0),
        earliest_offset_after_maintenance(&broker, &extensions, not_a_number_topic).await?
    );
    Ok(())
}

/// A stored `retention.ms` below -1 means that the topic keeps every record, because Kafka reads
/// any negative value as no limit. The broker rejects such a value, so only a topic created before
/// the broker checked the value holds it. A broker that reads it as a limit deletes every record of
/// a topic that is the only copy of its data.
pub async fn stored_retention_ms_below_minus_1_keeps_every_record<G>(sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;
    let extensions = Extensions::default();
    let topic = &alphanumeric_string(15)[..];
    create_topic_without_validation(&sc, topic, &[DELETE_POLICY, ("retention.ms", "-2")]).await?;
    produce_records_before_maintenance(&broker, &extensions, topic).await?;

    maintain_an_hour_later(&sc).await?;

    assert_eq!(
        Some(0),
        earliest_offset_after_maintenance(&broker, &extensions, topic).await?
    );
    Ok(())
}

/// A topic whose `cleanup.policy` is `compact` keeps every record that compaction keeps, however
/// old. A broker that applies `retention.ms` to it deletes the only copy of each key's latest value.
pub async fn compacted_topic_keeps_expired_records<G>(sc: G) -> Result<()>
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
        &[COMPACT_POLICY, TEN_MINUTE_RETENTION],
    )
    .await?;
    produce_records_before_maintenance(&broker, &extensions, topic).await?;

    maintain_an_hour_later(&sc).await?;

    assert_eq!(
        Some(0),
        earliest_offset_after_maintenance(&broker, &extensions, topic).await?
    );
    Ok(())
}

/// A topic whose `cleanup.policy` is empty has neither policy, so Kafka deletes none of its
/// records. `SUBTRACT` leaves an empty policy when it removes the last one. A broker that reads the
/// empty policy as the default, `delete`, deletes records that the topic was set to keep.
pub async fn topic_with_empty_cleanup_policy_keeps_expired_records<G>(sc: G) -> Result<()>
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
        &[EMPTY_POLICY, TEN_MINUTE_RETENTION],
    )
    .await?;
    produce_records_before_maintenance(&broker, &extensions, topic).await?;

    maintain_an_hour_later(&sc).await?;

    assert_eq!(
        Some(0),
        earliest_offset_after_maintenance(&broker, &extensions, topic).await?
    );
    Ok(())
}

/// Kafka accepts a `retention.ms` with a `+` or white space around the number. Maintenance must
/// apply it as the number, and not keep every record as it does for a value it can't read.
pub async fn retention_ms_with_a_plus_sign_expires_records<G>(sc: G) -> Result<()>
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
        &[DELETE_POLICY, ("retention.ms", "+600000\n")],
    )
    .await?;
    produce_records_before_maintenance(&broker, &extensions, topic).await?;

    maintain_an_hour_later(&sc).await?;

    assert_eq!(
        Some(RECORDS_BEFORE_MAINTENANCE),
        earliest_offset_after_maintenance(&broker, &extensions, topic).await?
    );
    Ok(())
}

/// A stored `retention.ms` with a `+`, which an earlier broker stored as sent, expires records as
/// the number. A broker that reads it as no limit keeps every record of a topic that set a limit.
pub async fn stored_retention_ms_with_a_plus_sign_expires_records<G>(sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;
    let extensions = Extensions::default();
    let topic = &alphanumeric_string(15)[..];
    create_topic_without_validation(&sc, topic, &[DELETE_POLICY, ("retention.ms", "+600000")])
        .await?;
    produce_records_before_maintenance(&broker, &extensions, topic).await?;

    maintain_an_hour_later(&sc).await?;

    assert_eq!(
        Some(RECORDS_BEFORE_MAINTENANCE),
        earliest_offset_after_maintenance(&broker, &extensions, topic).await?
    );
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
            async fn topic_without_cleanup_policy_loses_expired_records() -> Result<()> {
                let _guard = init_tracing()?;
                super::topic_without_cleanup_policy_loses_expired_records(
                    storage_container().await?,
                )
                .await
            }

            #[tokio::test]
            async fn retention_ms_minus_1_keeps_every_record() -> Result<()> {
                let _guard = init_tracing()?;
                super::retention_ms_minus_1_keeps_every_record(storage_container().await?).await
            }

            #[tokio::test]
            async fn thirty_day_retention_does_not_stop_retention_on_another_topic() -> Result<()> {
                let _guard = init_tracing()?;
                super::thirty_day_retention_does_not_stop_retention_on_another_topic(
                    storage_container().await?,
                )
                .await
            }

            #[tokio::test]
            async fn thirty_day_retention_keeps_an_hour_old_record() -> Result<()> {
                let _guard = init_tracing()?;
                super::thirty_day_retention_keeps_an_hour_old_record(storage_container().await?)
                    .await
            }

            #[tokio::test]
            async fn stored_retention_ms_that_is_not_a_number_does_not_stop_retention()
            -> Result<()> {
                let _guard = init_tracing()?;
                super::stored_retention_ms_that_is_not_a_number_does_not_stop_retention(
                    storage_container().await?,
                )
                .await
            }

            #[tokio::test]
            async fn stored_retention_ms_below_minus_1_keeps_every_record() -> Result<()> {
                let _guard = init_tracing()?;
                super::stored_retention_ms_below_minus_1_keeps_every_record(
                    storage_container().await?,
                )
                .await
            }
            #[tokio::test]
            async fn compacted_topic_keeps_expired_records() -> Result<()> {
                let _guard = init_tracing()?;
                super::compacted_topic_keeps_expired_records(storage_container().await?).await
            }

            #[tokio::test]
            async fn topic_with_empty_cleanup_policy_keeps_expired_records() -> Result<()> {
                let _guard = init_tracing()?;
                super::topic_with_empty_cleanup_policy_keeps_expired_records(
                    storage_container().await?,
                )
                .await
            }

            #[tokio::test]
            async fn retention_ms_with_a_plus_sign_expires_records() -> Result<()> {
                let _guard = init_tracing()?;
                super::retention_ms_with_a_plus_sign_expires_records(storage_container().await?)
                    .await
            }
            #[tokio::test]
            async fn stored_retention_ms_with_a_plus_sign_expires_records() -> Result<()> {
                let _guard = init_tracing()?;
                super::stored_retention_ms_with_a_plus_sign_expires_records(
                    storage_container().await?,
                )
                .await
            }
        }
    };
}

backend!(pg, "postgres", StorageType::Postgres);
backend!(lite, "libsql", StorageType::Lite);
