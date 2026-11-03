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
    Ack, CreateTopicsRequest, ErrorCode, IsolationLevel, ListOffset, ListOffsetsRequest,
    ProduceRequest, RequestInput,
    create_topics_request::CreatableTopic,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated},
    to_system_time, to_timestamp,
};
use nisshi_service::{
    BytesFrameLayer, BytesFrameService, BytesLayer, BytesService, FrameBytesLayer,
    FrameBytesService, FrameRouteService, RequestFrameLayer, RequestFrameService,
};
use nisshi_storage::{AutoCreateRefusals, Storage, TopicCreationConfig, Topition};
use rama::{Layer as _, Service, extensions::Extensions};
use rand::{prelude::*, rng};
use tokio::time::sleep;
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
    .inspect(|builder| debug!(?builder))
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

pub async fn multiple_record(broker: Broker) -> Result<()> {
    let topic_name = &alphanumeric_string(15)[..];
    debug!(?topic_name);

    let timeout = 5_000;
    let num_partitions = 6;
    let replication_factor = 1;

    let extensions = Extensions::default();

    let response = broker
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .timeout_ms(timeout)
                .validate_only(Some(false))
                .topics(Some(
                    [CreatableTopic::default()
                        .num_partitions(num_partitions)
                        .configs(Some([].into()))
                        .assignments(Some([].into()))
                        .replication_factor(replication_factor)
                        .name(topic_name.into())]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(i16::from(ErrorCode::None), topics[0].error_code);

    let replica_id = -1;
    let max_num_offsets = Some(6);
    let current_leader_epoch = -1;

    debug!(phase = "empty topic: uncommitted latest offset");
    let isolation = Some(IsolationLevel::ReadUncommitted.into());
    let timestamp = ListOffset::Latest.try_into()?;

    let response = broker
        .serve(RequestInput {
            request: ListOffsetsRequest::default()
                .isolation_level(isolation)
                .replica_id(replica_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic_name.into())
                        .partitions(Some(
                            (0..num_partitions)
                                .map(|partition_index| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition_index)
                                        .max_num_offsets(max_num_offsets)
                                        .timestamp(timestamp)
                                        .current_leader_epoch(Some(current_leader_epoch))
                                })
                                .collect::<Vec<_>>(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(num_partitions as usize, partitions.len());

    for partition in partitions {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(0), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    debug!(phase = "empty topic: uncommitted earliest offset");
    let timestamp = ListOffset::Earliest.try_into()?;

    let response = broker
        .serve(RequestInput {
            request: ListOffsetsRequest::default()
                .isolation_level(isolation)
                .replica_id(replica_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic_name.into())
                        .partitions(Some(
                            (0..num_partitions)
                                .map(|partition_index| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition_index)
                                        .max_num_offsets(max_num_offsets)
                                        .timestamp(timestamp)
                                        .current_leader_epoch(Some(current_leader_epoch))
                                })
                                .collect::<Vec<_>>(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(num_partitions as usize, partitions.len());

    for partition in partitions {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(0), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    sleep(Duration::from_millis(500)).await;
    let first = SystemTime::now();

    let frame = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"one"))))
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)?;

    let partition = 0;

    let response = broker
        .serve(RequestInput {
            request: ProduceRequest::default()
                .timeout_ms(timeout)
                .acks(Ack::Leader.into())
                .topic_data(Some(
                    [TopicProduceData::default()
                        .name(topic_name.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(partition)
                                .records(Some(frame))]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await
        .inspect(|response| debug!("{response:?}"))?;

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(partition, partitions[0].index);
    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(0, partitions[0].base_offset);

    sleep(Duration::from_millis(500)).await;
    let second = SystemTime::now();

    let frame = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"two"))))
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)?;

    let response = broker
        .serve(RequestInput {
            request: ProduceRequest::default()
                .timeout_ms(timeout)
                .acks(Ack::Leader.into())
                .topic_data(Some(
                    [TopicProduceData::default()
                        .name(topic_name.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(partition)
                                .records(Some(frame))]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await
        .inspect(|response| debug!("{response:?}"))?;

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(partition, partitions[0].index);
    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(1, partitions[0].base_offset);

    sleep(Duration::from_millis(500)).await;
    let third = SystemTime::now();

    let frame = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"three"))))
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)?;

    let response = broker
        .serve(RequestInput {
            request: ProduceRequest::default()
                .timeout_ms(timeout)
                .acks(Ack::Leader.into())
                .topic_data(Some(
                    [TopicProduceData::default()
                        .name(topic_name.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(partition)
                                .records(Some(frame))]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await
        .inspect(|response| debug!("{response:?}"))?;

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(partition, partitions[0].index);
    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(2, partitions[0].base_offset);

    debug!(phase = "topic: uncommitted earliest offset");
    let timestamp = ListOffset::Earliest.try_into()?;

    let response = broker
        .serve(RequestInput {
            request: ListOffsetsRequest::default()
                .isolation_level(isolation)
                .replica_id(replica_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic_name.into())
                        .partitions(Some(
                            (0..num_partitions)
                                .map(|partition_index| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition_index)
                                        .max_num_offsets(max_num_offsets)
                                        .timestamp(timestamp)
                                        .current_leader_epoch(Some(current_leader_epoch))
                                })
                                .collect::<Vec<_>>(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(num_partitions as usize, partitions.len());

    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(Some(0), partitions[0].offset);
    assert_eq!(Some(-1), partitions[0].timestamp);

    for partition in partitions[1..].iter() {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(0), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    debug!(phase = "topic: uncommitted latest offset");
    let timestamp = ListOffset::Latest.try_into()?;

    let response = broker
        .serve(RequestInput {
            request: ListOffsetsRequest::default()
                .isolation_level(isolation)
                .replica_id(replica_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic_name.into())
                        .partitions(Some(
                            (0..num_partitions)
                                .map(|partition_index| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition_index)
                                        .max_num_offsets(max_num_offsets)
                                        .timestamp(timestamp)
                                        .current_leader_epoch(Some(current_leader_epoch))
                                })
                                .collect::<Vec<_>>(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(num_partitions as usize, partitions.len());

    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(Some(3), partitions[0].offset);
    assert_eq!(Some(-1), partitions[0].timestamp);

    for partition in partitions[1..].iter() {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(0), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    debug!(phase = "first offset");
    let timestamp = ListOffset::Timestamp(first)
        .try_into()
        .inspect(|first| debug!(?first))?;

    let response = broker
        .serve(RequestInput {
            request: ListOffsetsRequest::default()
                .isolation_level(isolation)
                .replica_id(replica_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic_name.into())
                        .partitions(Some(
                            (0..num_partitions)
                                .map(|partition_index| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition_index)
                                        .max_num_offsets(max_num_offsets)
                                        .timestamp(timestamp)
                                        .current_leader_epoch(Some(current_leader_epoch))
                                })
                                .collect::<Vec<_>>(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(num_partitions as usize, partitions.len());

    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(Some(0), partitions[0].offset);
    assert!(
        partitions[0]
            .timestamp
            .is_some_and(|timestamp| timestamp > 0)
    );

    // Other partitions are empty: no record matches this timestamp, so this is
    // a not-found result, not an empty-partition result. Offset is -1, matching
    // Kafka's buildErrorResponse default.
    for partition in partitions[1..].iter() {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(-1), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    debug!(phase = "second offset");
    let timestamp = ListOffset::Timestamp(second)
        .try_into()
        .inspect(|second| debug!(?second))?;

    let response = broker
        .serve(RequestInput {
            request: ListOffsetsRequest::default()
                .isolation_level(isolation)
                .replica_id(replica_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic_name.into())
                        .partitions(Some(
                            (0..num_partitions)
                                .map(|partition_index| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition_index)
                                        .max_num_offsets(max_num_offsets)
                                        .timestamp(timestamp)
                                        .current_leader_epoch(Some(current_leader_epoch))
                                })
                                .collect::<Vec<_>>(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(num_partitions as usize, partitions.len());

    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(Some(1), partitions[0].offset);
    assert!(
        partitions[0]
            .timestamp
            .is_some_and(|timestamp| timestamp > 0)
    );

    // Other partitions are empty: no record matches this timestamp, so this is
    // a not-found result, not an empty-partition result. Offset is -1, matching
    // Kafka's buildErrorResponse default.
    for partition in partitions[1..].iter() {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(-1), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    debug!(phase = "third");
    let timestamp = ListOffset::Timestamp(third)
        .try_into()
        .inspect(|third| debug!(?third))?;

    let response = broker
        .serve(RequestInput {
            request: ListOffsetsRequest::default()
                .isolation_level(isolation)
                .replica_id(replica_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic_name.into())
                        .partitions(Some(
                            (0..num_partitions)
                                .map(|partition_index| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition_index)
                                        .max_num_offsets(max_num_offsets)
                                        .timestamp(timestamp)
                                        .current_leader_epoch(Some(current_leader_epoch))
                                })
                                .collect::<Vec<_>>(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(num_partitions as usize, partitions.len());

    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(Some(2), partitions[0].offset);
    assert!(
        partitions[0]
            .timestamp
            .is_some_and(|timestamp| timestamp > 0)
    );

    // Other partitions are empty: no record matches this timestamp, so this is
    // a not-found result, not an empty-partition result. Offset is -1, matching
    // Kafka's buildErrorResponse default.
    for partition in partitions[1..].iter() {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(-1), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    debug!(phase = "after last record");
    let timestamp = ListOffset::Timestamp(third + Duration::from_secs(1))
        .try_into()
        .inspect(|after| debug!(?after))?;

    let response = broker
        .serve(RequestInput {
            request: ListOffsetsRequest::default()
                .isolation_level(isolation)
                .replica_id(replica_id)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic_name.into())
                        .partitions(Some(
                            (0..num_partitions)
                                .map(|partition_index| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition_index)
                                        .max_num_offsets(max_num_offsets)
                                        .timestamp(timestamp)
                                        .current_leader_epoch(Some(current_leader_epoch))
                                })
                                .collect::<Vec<_>>(),
                        ))]
                    .into(),
                )),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(topic_name, topics[0].name);
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(num_partitions as usize, partitions.len());

    // Partition 0 holds records, but none at or after the target: Kafka
    // answers offset -1 and timestamp -1 with no error.
    for partition in partitions {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(-1), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    Ok(())
}

/// Produces one batch of `values` to partition 0 of `topic_name` through
/// `broker`, and returns its base offset.
async fn produce_batch(broker: &Broker, topic_name: &str, values: &[&'static [u8]]) -> Result<i64> {
    let mut builder = inflated::Batch::builder()
        .last_offset_delta(i32::try_from(values.len() - 1).unwrap_or_default());

    for (offset_delta, value) in values.iter().enumerate() {
        builder = builder.record(
            Record::builder()
                .offset_delta(i32::try_from(offset_delta).unwrap_or_default())
                .value(Some(Bytes::from_static(value))),
        );
    }

    let frame = builder
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
                        .name(topic_name.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(0)
                                .records(Some(frame))]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);

    Ok(partitions[0].base_offset)
}

/// Returns the offset and timestamp that `broker` answers for a
/// READ_UNCOMMITTED `ListOffsets` of partition 0 of `topic_name`.
async fn list_offset(
    broker: &Broker,
    topic_name: &str,
    list_offset: ListOffset,
) -> Result<(Option<i64>, Option<i64>)> {
    let response = broker
        .serve(RequestInput {
            request: ListOffsetsRequest::default()
                .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
                .replica_id(-1)
                .topics(Some(
                    [ListOffsetsTopic::default()
                        .name(topic_name.into())
                        .partitions(Some(
                            [ListOffsetsPartition::default()
                                .partition_index(0)
                                .timestamp(list_offset.try_into()?)
                                .current_leader_epoch(Some(-1))]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);

    Ok((partitions[0].offset, partitions[0].timestamp))
}

/// After a batch of 3 records and a batch of 2, `Latest` answers the high
/// watermark, 5, and `Earliest` answers the log start, 0. Kafka answers both
/// without a timestamp (-1).
pub async fn multi_record_batch(broker: Broker) -> Result<()> {
    let topic_name = &alphanumeric_string(15)[..];
    debug!(?topic_name);

    let response = broker
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .timeout_ms(5_000)
                .validate_only(Some(false))
                .topics(Some(
                    [CreatableTopic::default()
                        .num_partitions(1)
                        .configs(Some([].into()))
                        .assignments(Some([].into()))
                        .replication_factor(1)
                        .name(topic_name.into())]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let topics = response.topics.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(i16::from(ErrorCode::None), topics[0].error_code);

    assert_eq!(
        0,
        produce_batch(&broker, topic_name, &[b"one", b"two", b"three"]).await?
    );
    assert_eq!(
        3,
        produce_batch(&broker, topic_name, &[b"four", b"five"]).await?
    );

    assert_eq!(
        (Some(5), Some(-1)),
        list_offset(&broker, topic_name, ListOffset::Latest).await?
    );
    assert_eq!(
        (Some(0), Some(-1)),
        list_offset(&broker, topic_name, ListOffset::Earliest).await?
    );

    Ok(())
}

pub async fn new_topic<G>(cluster_id: impl Into<String>, broker_id: i32, sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;
    debug!(?topic_id);

    let offsets = (0..num_partitions)
        .map(|partition| {
            (
                Topition::new(topic_name.clone(), partition),
                ListOffset::Latest,
            )
        })
        .collect::<Vec<_>>();

    let items = sc
        .list_offsets(IsolationLevel::ReadUncommitted, &offsets[..])
        .await
        .inspect(|response| debug!(?response))?;

    assert!(!items.is_empty());

    for (_toptition, response) in items {
        assert_eq!(Some(0), response.offset);
        assert_eq!(None, response.timestamp);
    }

    let offsets = (0..num_partitions)
        .map(|partition| {
            (
                Topition::new(topic_name.clone(), partition),
                ListOffset::Earliest,
            )
        })
        .collect::<Vec<_>>();

    let items = sc
        .list_offsets(IsolationLevel::ReadUncommitted, &offsets[..])
        .await
        .inspect(|response| debug!(?response))?;

    assert!(!items.is_empty());

    for (_toptition, response) in items {
        assert_eq!(Some(0), response.offset);
        assert_eq!(None, response.timestamp);
    }

    let timestamp = to_system_time(0)?;

    let offsets = (0..num_partitions)
        .map(|partition| {
            (
                Topition::new(topic_name.clone(), partition),
                ListOffset::Timestamp(timestamp),
            )
        })
        .collect::<Vec<_>>();

    let items = sc
        .list_offsets(IsolationLevel::ReadUncommitted, &offsets[..])
        .await
        .inspect(|response| debug!(?response))?;

    assert!(!items.is_empty());

    // The topic has no records at all, so no record matches this timestamp:
    // it's a not-found result at the raw storage layer (the -1 default is
    // applied by the broker service, not the storage backend itself).
    for (_toptition, response) in items {
        assert_eq!(None, response.offset);
        assert_eq!(None, response.timestamp);
    }

    Ok(())
}

pub async fn single_record<G>(cluster_id: impl Into<String>, broker_id: i32, sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 6;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some([].into());

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments.clone())
                .configs(configs.clone()),
            false,
        )
        .await?;
    debug!(?topic_id);

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let value = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());

    let before = SystemTime::now();
    debug!(before = to_timestamp(&before)?);
    sleep(Duration::from_millis(500)).await;

    let batch = inflated::Batch::builder()
        .record(Record::builder().value(value.clone().into()))
        .build()
        .and_then(TryInto::try_into)
        .inspect(|deflated| debug!(?deflated))?;

    assert_eq!(
        0,
        sc.produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(?offset))?
    );

    // A Timestamp lookup matches a record at or after the target, at
    // millisecond precision. `after` is one second past now, so it is
    // later than the record's timestamp.
    let after = SystemTime::now() + Duration::from_secs(1);
    debug!(after = to_timestamp(&after)?);

    let offsets = [(topition.clone(), ListOffset::Latest)];

    let responses = sc
        .list_offsets(IsolationLevel::ReadUncommitted, &offsets[..])
        .await
        .inspect(|responses| debug!(?responses))?;

    assert_eq!(1, responses.len());
    assert_eq!(Some(1), responses[0].1.offset);

    let offsets = [(topition.clone(), ListOffset::Earliest)];

    let responses = sc
        .list_offsets(IsolationLevel::ReadUncommitted, &offsets[..])
        .await
        .inspect(|responses| debug!(?responses))?;

    assert_eq!(1, responses.len());
    assert_eq!(Some(0), responses[0].1.offset);

    let offsets = [(topition.clone(), ListOffset::Timestamp(before))];

    let responses = sc
        .list_offsets(IsolationLevel::ReadUncommitted, &offsets[..])
        .await?;

    assert_eq!(1, responses.len());
    assert_eq!(Some(0), responses[0].1.offset);

    let offsets = [(topition.clone(), ListOffset::Timestamp(after))];

    let responses = sc
        .list_offsets(IsolationLevel::ReadUncommitted, &offsets[..])
        .await?;

    // No record's timestamp is >= `after`: not-found at the raw storage layer
    // (the -1 default is applied by the broker service, not the storage
    // backend itself).
    assert_eq!(1, responses.len());
    assert_eq!(None, responses[0].1.offset);
    assert_eq!(None, responses[0].1.timestamp);

    Ok(())
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;
    use nisshi_storage::ArcDynStorage;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::Postgres,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn multiple_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        let broker = broker(sc)?;
        super::multiple_record(broker).await
    }

    #[tokio::test]
    async fn multi_record_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        let broker = broker(sc)?;
        super::multi_record_batch(broker).await
    }

    #[tokio::test]
    async fn new_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::new_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn single_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_record(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;
    use nisshi_storage::ArcDynStorage;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::InMemory,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn multiple_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        let broker = broker(sc)?;
        super::multiple_record(broker).await
    }

    #[tokio::test]
    async fn multi_record_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        let broker = broker(sc)?;
        super::multi_record_batch(broker).await
    }

    #[tokio::test]
    async fn new_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::new_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn single_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_record(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;
    use nisshi_storage::ArcDynStorage;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::Lite,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn multiple_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        let broker = broker(sc)?;
        super::multiple_record(broker).await
    }

    #[tokio::test]
    async fn multi_record_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        let broker = broker(sc)?;
        super::multi_record_batch(broker).await
    }

    #[tokio::test]
    async fn new_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::new_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn single_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_record(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "turso")]
mod turso {
    use super::*;
    use nisshi_storage::ArcDynStorage;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::Turso,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[ignore]
    #[tokio::test]
    async fn multiple_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        let broker = broker(sc)?;
        super::multiple_record(broker).await
    }

    #[ignore]
    #[tokio::test]
    async fn multi_record_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        let broker = broker(sc)?;
        super::multi_record_batch(broker).await
    }

    #[ignore]
    #[tokio::test]
    async fn new_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::new_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[ignore]
    #[tokio::test]
    async fn single_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_record(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use super::*;
    use nisshi_storage::ArcDynStorage;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::SlateDb,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn multiple_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        let broker = broker(sc)?;
        super::multiple_record(broker).await
    }

    #[tokio::test]
    async fn multi_record_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        let broker = broker(sc)?;
        super::multi_record_batch(broker).await
    }

    #[tokio::test]
    async fn new_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::new_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn single_record() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::single_record(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}
