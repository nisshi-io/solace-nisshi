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

use std::time::SystemTime;

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
use tracing::debug;
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

const VALUE_BYTES: usize = 1000;
const RETENTION_BYTES: usize = 5 * VALUE_BYTES;
const PRODUCED: i64 = 20;

/// How many of the newest records fit within [`RETENTION_BYTES`], and remain after maintenance.
const KEPT: i64 = (RETENTION_BYTES / VALUE_BYTES) as i64;

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

async fn produce(
    broker: &Broker,
    extensions: &Extensions,
    name: &str,
    value: Bytes,
) -> Result<i64> {
    let frame = inflated::Batch::builder()
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

/// Produce 20 records of 1000 bytes to a partition capped at 5000 bytes. After maintenance the
/// newest 5 remain, and offsets carry on from the last one produced.
pub async fn retention_bytes_deletes_oldest<G>(sc: G) -> Result<()>
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
            ("retention.bytes", &RETENTION_BYTES.to_string()),
        ],
    )
    .await?;

    for n in 0..PRODUCED {
        let value = Bytes::from(format!("{n:0>VALUE_BYTES$}"));
        assert_eq!(n, produce(&broker, &extensions, topic, value).await?);
    }

    sc.maintain(SystemTime::now()).await?;

    assert_eq!(
        Some(PRODUCED - KEPT),
        list_offset(&broker, &extensions, topic, ListOffset::Earliest).await?
    );
    assert_eq!(
        Some(PRODUCED),
        list_offset(&broker, &extensions, topic, ListOffset::Latest).await?
    );

    let value = Bytes::from(format!("{PRODUCED:0>VALUE_BYTES$}"));
    assert_eq!(PRODUCED, produce(&broker, &extensions, topic, value).await?);

    Ok(())
}

/// Kafka's default `cleanup.policy` is `delete`, so a topic that sets only `retention.bytes` must
/// stay within it. A broker that skips the topic lets it grow forever.
pub async fn retention_bytes_without_cleanup_policy_deletes_oldest<G>(sc: G) -> Result<()>
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
        &[("retention.bytes", &RETENTION_BYTES.to_string())],
    )
    .await?;

    for n in 0..PRODUCED {
        let value = Bytes::from(format!("{n:0>VALUE_BYTES$}"));
        _ = produce(&broker, &extensions, topic, value).await?;
    }

    sc.maintain(SystemTime::now()).await?;

    assert_eq!(
        Some(PRODUCED - KEPT),
        list_offset(&broker, &extensions, topic, ListOffset::Earliest).await?
    );
    Ok(())
}

/// A topic without `retention.bytes`, or with the default of -1, keeps every record.
pub async fn retention_bytes_unlimited_keeps_everything<G>(sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;
    let extensions = Extensions::default();

    for configs in [
        vec![("cleanup.policy", "delete")],
        vec![("cleanup.policy", "delete"), ("retention.bytes", "-1")],
    ] {
        let topic = &alphanumeric_string(15)[..];
        create_topic(&broker, &extensions, topic, &configs).await?;

        for n in 0..PRODUCED {
            let value = Bytes::from(format!("{n:0>VALUE_BYTES$}"));
            _ = produce(&broker, &extensions, topic, value).await?;
        }

        sc.maintain(SystemTime::now()).await?;

        assert_eq!(
            Some(0),
            list_offset(&broker, &extensions, topic, ListOffset::Earliest).await?
        );
    }

    Ok(())
}

/// The newest record is kept even when it alone is larger than `retention.bytes`.
pub async fn retention_bytes_keeps_newest_record<G>(sc: G) -> Result<()>
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
        &[("cleanup.policy", "delete"), ("retention.bytes", "10")],
    )
    .await?;

    for n in 0..3 {
        let value = Bytes::from(format!("{n:0>VALUE_BYTES$}"));
        _ = produce(&broker, &extensions, topic, value).await?;
    }

    sc.maintain(SystemTime::now()).await?;

    assert_eq!(
        Some(2),
        list_offset(&broker, &extensions, topic, ListOffset::Earliest).await?
    );
    Ok(())
}

/// A topic whose `cleanup.policy` is `compact` keeps every record that compaction keeps, whatever
/// their size. A broker that applies `retention.bytes` to it deletes the only copy of each key's
/// latest value.
pub async fn retention_bytes_compacted_topic_keeps_everything<G>(sc: G) -> Result<()>
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
            ("cleanup.policy", "compact"),
            ("retention.bytes", &RETENTION_BYTES.to_string()),
        ],
    )
    .await?;

    for n in 0..PRODUCED {
        let value = Bytes::from(format!("{n:0>VALUE_BYTES$}"));
        _ = produce(&broker, &extensions, topic, value).await?;
    }

    sc.maintain(SystemTime::now()).await?;

    assert_eq!(
        Some(0),
        list_offset(&broker, &extensions, topic, ListOffset::Earliest).await?
    );
    Ok(())
}

/// A topic whose stored `retention.bytes` doesn't fit in a long keeps every record, and maintenance
/// still trims the other topics. Maintenance must not fail on the value, because one failure stops
/// it for every topic.
pub async fn retention_bytes_stored_value_that_is_not_a_long_does_not_stop_retention<G>(
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;
    let extensions = Extensions::default();
    let not_a_long_topic = &alphanumeric_string(15)[..];
    create_topic_without_validation(
        &sc,
        not_a_long_topic,
        &[
            ("cleanup.policy", "delete"),
            ("retention.bytes", "99999999999999999999"),
        ],
    )
    .await?;
    let capped_topic = &alphanumeric_string(15)[..];
    create_topic(
        &broker,
        &extensions,
        capped_topic,
        &[
            ("cleanup.policy", "delete"),
            ("retention.bytes", &RETENTION_BYTES.to_string()),
        ],
    )
    .await?;

    for topic in [not_a_long_topic, capped_topic] {
        for n in 0..PRODUCED {
            let value = Bytes::from(format!("{n:0>VALUE_BYTES$}"));
            _ = produce(&broker, &extensions, topic, value).await?;
        }
    }

    sc.maintain(SystemTime::now()).await?;

    assert_eq!(
        Some(PRODUCED - KEPT),
        list_offset(&broker, &extensions, capped_topic, ListOffset::Earliest).await?
    );
    assert_eq!(
        Some(0),
        list_offset(&broker, &extensions, not_a_long_topic, ListOffset::Earliest).await?
    );
    Ok(())
}

/// A second sweep after more records arrive deletes exactly the records that the new ones push
/// out, and a sweep that finds the partition within its limit deletes nothing.
pub async fn retention_bytes_second_sweep_deletes_the_new_excess<G>(sc: G) -> Result<()>
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
            ("retention.bytes", &RETENTION_BYTES.to_string()),
        ],
    )
    .await?;

    for n in 0..PRODUCED {
        let value = Bytes::from(format!("{n:0>VALUE_BYTES$}"));
        _ = produce(&broker, &extensions, topic, value).await?;
    }

    sc.maintain(SystemTime::now()).await?;
    assert_eq!(
        Some(PRODUCED - KEPT),
        list_offset(&broker, &extensions, topic, ListOffset::Earliest).await?
    );

    // within the limit: nothing to delete
    sc.maintain(SystemTime::now()).await?;
    assert_eq!(
        Some(PRODUCED - KEPT),
        list_offset(&broker, &extensions, topic, ListOffset::Earliest).await?
    );

    // three more records push out the three oldest of the five that remain
    for n in PRODUCED..PRODUCED + 3 {
        let value = Bytes::from(format!("{n:0>VALUE_BYTES$}"));
        _ = produce(&broker, &extensions, topic, value).await?;
    }

    sc.maintain(SystemTime::now()).await?;
    assert_eq!(
        Some(PRODUCED + 3 - KEPT),
        list_offset(&broker, &extensions, topic, ListOffset::Earliest).await?
    );
    assert_eq!(
        Some(PRODUCED + 3),
        list_offset(&broker, &extensions, topic, ListOffset::Latest).await?
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
            async fn deletes_oldest() -> Result<()> {
                let _guard = init_tracing()?;
                super::retention_bytes_deletes_oldest(storage_container().await?).await
            }

            #[tokio::test]
            async fn without_cleanup_policy_deletes_oldest() -> Result<()> {
                let _guard = init_tracing()?;
                super::retention_bytes_without_cleanup_policy_deletes_oldest(
                    storage_container().await?,
                )
                .await
            }

            #[tokio::test]
            async fn unlimited_keeps_everything() -> Result<()> {
                let _guard = init_tracing()?;
                super::retention_bytes_unlimited_keeps_everything(storage_container().await?).await
            }

            #[tokio::test]
            async fn keeps_newest_record() -> Result<()> {
                let _guard = init_tracing()?;
                super::retention_bytes_keeps_newest_record(storage_container().await?).await
            }
            #[tokio::test]
            async fn compacted_topic_keeps_everything() -> Result<()> {
                let _guard = init_tracing()?;
                super::retention_bytes_compacted_topic_keeps_everything(storage_container().await?)
                    .await
            }

            #[tokio::test]
            async fn stored_value_that_is_not_a_long_does_not_stop_retention() -> Result<()> {
                let _guard = init_tracing()?;
                super::retention_bytes_stored_value_that_is_not_a_long_does_not_stop_retention(
                    storage_container().await?,
                )
                .await
            }

            #[tokio::test]
            async fn second_sweep_deletes_the_new_excess() -> Result<()> {
                let _guard = init_tracing()?;
                super::retention_bytes_second_sweep_deletes_the_new_excess(
                    storage_container().await?,
                )
                .await
            }
        }
    };
}

backend!(pg, "postgres", StorageType::Postgres);
backend!(lite, "libsql", StorageType::Lite);
