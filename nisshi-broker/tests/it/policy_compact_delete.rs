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
    Ack, CreateTopicsRequest, ErrorCode, FetchRequest, IsolationLevel, ListOffset,
    ListOffsetsRequest, NULL_TOPIC_ID, ProduceRequest, RequestInput,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    fetch_request::{FetchPartition, FetchTopic, ReplicaState},
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Header, Record, deflated, inflated},
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

const CLEANUP_POLICY: &str = "cleanup.policy";
const COMPACT: &str = "compact";
const DELETE: &str = "delete";
const RETENTION_MS: &str = "retention.ms";

/// The partition that each test produces its records to.
const PRODUCED_PARTITION: i32 = 0;
/// How many records each test produces, at offsets 0, 1 and 2.
const PRODUCED_RECORD_COUNT: i64 = 3;

/// Returns the earliest and latest offset of a partition after retention has deleted every
/// record that a test produced.
fn offset_after_retention_deleted_every_record(partition_index: i32) -> i64 {
    if partition_index == PRODUCED_PARTITION {
        PRODUCED_RECORD_COUNT
    } else {
        0
    }
}

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

pub async fn compact_only<G>(sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;

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
                        .configs(Some(
                            [CreatableTopicConfig::default()
                                .name(CLEANUP_POLICY.into())
                                .value(Some(COMPACT.into()))]
                            .into(),
                        ))
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

    let topic_id = topics[0].topic_id.unwrap_or(NULL_TOPIC_ID);

    let replica_id = -1;
    let max_num_offsets = Some(6);
    let current_leader_epoch = -1;
    let isolation = Some(IsolationLevel::ReadUncommitted.into());

    const KEY: Bytes = Bytes::from_static(b"alpha");

    const ONE: Bytes = Bytes::from_static(b"one");

    let frame = inflated::Batch::builder()
        .record(
            Record::builder().key(Some(KEY)).value(Some(ONE)).header(
                Header::builder()
                    .key(Bytes::from_static(b"x"))
                    .value(Bytes::from_static(b"y")),
            ),
        )
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)?;

    let partition = PRODUCED_PARTITION;

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

    const TWO: Bytes = Bytes::from_static(b"two");
    let frame = inflated::Batch::builder()
        .record(Record::builder().key(Some(KEY)).value(Some(TWO)))
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

    const THREE: Bytes = Bytes::from_static(b"three");
    let frame = inflated::Batch::builder()
        .record(Record::builder().key(Some(KEY)).value(Some(THREE)))
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

    debug!(phase = "pre: uncommitted earliest offset");
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

    debug!(phase = "pre: uncommitted latest offset");
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

    debug!(phase = "maintenance");
    sc.maintain(SystemTime::now()).await?;

    debug!(phase = "post: uncommitted earliest offset");
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
    assert_eq!(Some(2), partitions[0].offset);
    assert_eq!(Some(-1), partitions[0].timestamp);

    for partition in partitions[1..].iter() {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(0), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    debug!(phase = "post: uncommitted latest offset");
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

    let response = broker
        .serve(RequestInput {
            request: FetchRequest::default()
                .cluster_id(Some("".into()))
                .replica_id(Some(-1))
                .replica_state(Some(ReplicaState::default()))
                .max_wait_ms(500)
                .max_bytes(Some(4096))
                .min_bytes(1)
                .isolation_level(Some(1))
                .session_id(Some(-1))
                .session_epoch(Some(-1))
                .topics(Some(vec![
                    FetchTopic::default()
                        .topic(Some(topic_name.into()))
                        .topic_id(Some(topic_id))
                        .partitions(Some(vec![
                            FetchPartition::default()
                                .partition(partition)
                                .log_start_offset(Some(0))
                                .partition_max_bytes(4096),
                        ])),
                ]))
                .forgotten_topics_data(Some([].into()))
                .rack_id(Some("".into())),
            extensions: extensions.clone(),
        })
        .await?;

    {
        let batch = response
            .responses
            .and_then(|mut responses| responses.pop())
            .and_then(|topic| topic.partitions)
            .and_then(|mut partitions| partitions.pop())
            .and_then(|partition_data| partition_data.records)
            .and_then(|deflated| inflated::Frame::try_from(deflated).ok())
            .map(|inflated| inflated.batches)
            .and_then(|mut batches| batches.pop())
            .unwrap_or_default();

        assert_eq!(2, batch.base_offset);

        assert_eq!(1, batch.records.len());

        assert_eq!(Some(KEY), batch.records[0].key);
        assert_eq!(Some(THREE), batch.records[0].value);
        assert_eq!(0, batch.records[0].offset_delta);
    }

    Ok(())
}

pub async fn delete_only<G>(sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;

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
                        .configs(Some(
                            [
                                CreatableTopicConfig::default()
                                    .name(CLEANUP_POLICY.into())
                                    .value(Some(DELETE.into())),
                                CreatableTopicConfig::default()
                                    .name(RETENTION_MS.into())
                                    .value(Some(Duration::from_mins(30).as_millis().to_string())),
                            ]
                            .into(),
                        ))
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

    let topic_id = topics[0].topic_id.unwrap_or(NULL_TOPIC_ID);

    let replica_id = -1;
    let max_num_offsets = Some(6);
    let current_leader_epoch = -1;
    let isolation = Some(IsolationLevel::ReadUncommitted.into());

    const KEY: Bytes = Bytes::from_static(b"alpha");

    const ONE: Bytes = Bytes::from_static(b"one");

    let frame = inflated::Batch::builder()
        .record(
            Record::builder().key(Some(KEY)).value(Some(ONE)).header(
                Header::builder()
                    .key(Bytes::from_static(b"x"))
                    .value(Bytes::from_static(b"y")),
            ),
        )
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)?;

    let partition = PRODUCED_PARTITION;

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

    const TWO: Bytes = Bytes::from_static(b"two");

    let frame = inflated::Batch::builder()
        .record(Record::builder().key(Some(KEY)).value(Some(TWO)))
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

    const THREE: Bytes = Bytes::from_static(b"three");

    let frame = inflated::Batch::builder()
        .record(Record::builder().key(Some(KEY)).value(Some(THREE)))
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

    debug!(phase = "pre: uncommitted earliest offset");
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

    debug!(phase = "pre: uncommitted latest offset");
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

    let response = broker
        .serve(RequestInput {
            request: FetchRequest::default()
                .cluster_id(Some("".into()))
                .replica_id(Some(-1))
                .replica_state(Some(ReplicaState::default()))
                .max_wait_ms(500)
                .max_bytes(Some(4096))
                .min_bytes(1)
                .isolation_level(Some(1))
                .session_id(Some(-1))
                .session_epoch(Some(-1))
                .topics(Some(vec![
                    FetchTopic::default()
                        .topic(Some(topic_name.into()))
                        .topic_id(Some(topic_id))
                        .partitions(Some(vec![
                            FetchPartition::default()
                                .partition(partition)
                                .log_start_offset(Some(0))
                                .partition_max_bytes(4096),
                        ])),
                ]))
                .forgotten_topics_data(Some([].into()))
                .rack_id(Some("".into())),
            extensions: extensions.clone(),
        })
        .await?;

    {
        let records = response
            .responses
            .and_then(|mut responses| responses.pop())
            .and_then(|topic| topic.partitions)
            .and_then(|mut partitions| partitions.pop())
            .and_then(|partition_data| partition_data.records)
            .and_then(|deflated| inflated::Frame::try_from(deflated).ok())
            .map(|inflated| inflated.batches)
            .and_then(|mut batches| batches.pop())
            .map(|batch| batch.records)
            .inspect(|records| debug!(?records))
            .unwrap_or_default();

        assert_eq!(3, records.len());
        assert_eq!(Some(KEY), records[0].key);
        assert_eq!(Some(ONE), records[0].value);
        assert_eq!(0, records[0].offset_delta);

        assert_eq!(Some(KEY), records[1].key);
        assert_eq!(Some(TWO), records[1].value);
        assert_eq!(1, records[1].offset_delta);

        assert_eq!(Some(KEY), records[2].key);
        assert_eq!(Some(THREE), records[2].value);
        assert_eq!(2, records[2].offset_delta);
    }

    debug!(phase = "maintenance");
    sc.maintain(SystemTime::now()).await?;

    debug!(phase = "post: uncommitted earliest offset");
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

    debug!(phase = "post: uncommitted latest offset");
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

    let response = broker
        .serve(RequestInput {
            request: FetchRequest::default()
                .cluster_id(Some("".into()))
                .replica_id(Some(-1))
                .replica_state(Some(ReplicaState::default()))
                .max_wait_ms(500)
                .max_bytes(Some(4096))
                .min_bytes(1)
                .isolation_level(Some(1))
                .session_id(Some(-1))
                .session_epoch(Some(-1))
                .topics(Some(vec![
                    FetchTopic::default()
                        .topic(Some(topic_name.into()))
                        .topic_id(Some(topic_id))
                        .partitions(Some(vec![
                            FetchPartition::default()
                                .partition(partition)
                                .log_start_offset(Some(0))
                                .partition_max_bytes(4096),
                        ])),
                ]))
                .forgotten_topics_data(Some([].into()))
                .rack_id(Some("".into())),
            extensions: extensions.clone(),
        })
        .await?;

    {
        let records = response
            .responses
            .and_then(|mut responses| responses.pop())
            .and_then(|topic| topic.partitions)
            .and_then(|mut partitions| partitions.pop())
            .and_then(|partition_data| partition_data.records)
            .and_then(|deflated| inflated::Frame::try_from(deflated).ok())
            .map(|inflated| inflated.batches)
            .and_then(|mut batches| batches.pop())
            .map(|batch| batch.records)
            .inspect(|records| debug!(?records))
            .unwrap_or_default();

        assert_eq!(3, records.len());
        assert_eq!(Some(KEY), records[0].key);
        assert_eq!(Some(ONE), records[0].value);
        assert_eq!(0, records[0].offset_delta);

        assert_eq!(Some(KEY), records[1].key);
        assert_eq!(Some(TWO), records[1].value);
        assert_eq!(1, records[1].offset_delta);

        assert_eq!(Some(KEY), records[2].key);
        assert_eq!(Some(THREE), records[2].value);
        assert_eq!(2, records[2].offset_delta);
    }

    debug!(phase = "maintenance");
    sc.maintain(
        SystemTime::now()
            .checked_add(Duration::from_hours(1))
            .expect("an hour ahead"),
    )
    .await?;

    debug!(phase = "post 1 hour: uncommitted earliest offset");
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

    for partition in partitions {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(
            Some(offset_after_retention_deleted_every_record(
                partition.partition_index
            )),
            partition.offset
        );
        assert_eq!(Some(-1), partition.timestamp);
    }

    debug!(phase = "post 1 hour: uncommitted latest offset");
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

    for partition in partitions {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(
            Some(offset_after_retention_deleted_every_record(
                partition.partition_index
            )),
            partition.offset
        );
        assert_eq!(Some(-1), partition.timestamp);
    }

    let response = broker
        .serve(RequestInput {
            request: FetchRequest::default()
                .cluster_id(Some("".into()))
                .replica_id(Some(-1))
                .replica_state(Some(ReplicaState::default()))
                .max_wait_ms(500)
                .max_bytes(Some(4096))
                .min_bytes(1)
                .isolation_level(Some(1))
                .session_id(Some(-1))
                .session_epoch(Some(-1))
                .topics(Some(vec![
                    FetchTopic::default()
                        .topic(Some(topic_name.into()))
                        .topic_id(Some(topic_id))
                        .partitions(Some(vec![
                            FetchPartition::default()
                                .partition(partition)
                                .log_start_offset(Some(0))
                                .partition_max_bytes(4096),
                        ])),
                ]))
                .forgotten_topics_data(Some([].into()))
                .rack_id(Some("".into())),
            extensions: extensions.clone(),
        })
        .await?;

    {
        let records = response
            .responses
            .and_then(|mut responses| responses.pop())
            .and_then(|topic| topic.partitions)
            .and_then(|mut partitions| partitions.pop())
            .and_then(|partition_data| partition_data.records)
            .and_then(|deflated| inflated::Frame::try_from(deflated).ok())
            .map(|inflated| inflated.batches)
            .and_then(|mut batches| batches.pop())
            .map(|batch| batch.records)
            .inspect(|records| debug!(?records))
            .unwrap_or_default();

        assert_eq!(0, records.len());
    }

    Ok(())
}

pub async fn delete_no_retention_ms_only<G>(sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;

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
                        .configs(Some(
                            [CreatableTopicConfig::default()
                                .name(CLEANUP_POLICY.into())
                                .value(Some(DELETE.into()))]
                            .into(),
                        ))
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

    let topic_id = topics[0].topic_id.unwrap_or(NULL_TOPIC_ID);

    let replica_id = -1;
    let max_num_offsets = Some(6);
    let current_leader_epoch = -1;
    let isolation = Some(IsolationLevel::ReadUncommitted.into());

    const KEY: Bytes = Bytes::from_static(b"alpha");

    const ONE: Bytes = Bytes::from_static(b"one");

    let frame = inflated::Batch::builder()
        .record(
            Record::builder().key(Some(KEY)).value(Some(ONE)).header(
                Header::builder()
                    .key(Bytes::from_static(b"x"))
                    .value(Bytes::from_static(b"y")),
            ),
        )
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)?;

    let partition = PRODUCED_PARTITION;

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

    const TWO: Bytes = Bytes::from_static(b"two");

    let frame = inflated::Batch::builder()
        .record(Record::builder().key(Some(KEY)).value(Some(TWO)))
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

    const THREE: Bytes = Bytes::from_static(b"three");

    let frame = inflated::Batch::builder()
        .record(Record::builder().key(Some(KEY)).value(Some(THREE)))
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

    debug!(phase = "pre: uncommitted earliest offset");
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

    debug!(phase = "pre: uncommitted latest offset");
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

    let response = broker
        .serve(RequestInput {
            request: FetchRequest::default()
                .cluster_id(Some("".into()))
                .replica_id(Some(-1))
                .replica_state(Some(ReplicaState::default()))
                .max_wait_ms(500)
                .max_bytes(Some(4096))
                .min_bytes(1)
                .isolation_level(Some(1))
                .session_id(Some(-1))
                .session_epoch(Some(-1))
                .topics(Some(vec![
                    FetchTopic::default()
                        .topic(Some(topic_name.into()))
                        .topic_id(Some(topic_id))
                        .partitions(Some(vec![
                            FetchPartition::default()
                                .partition(partition)
                                .log_start_offset(Some(0))
                                .partition_max_bytes(4096),
                        ])),
                ]))
                .forgotten_topics_data(Some([].into()))
                .rack_id(Some("".into())),
            extensions: extensions.clone(),
        })
        .await?;

    {
        let records = response
            .responses
            .and_then(|mut responses| responses.pop())
            .and_then(|topic| topic.partitions)
            .and_then(|mut partitions| partitions.pop())
            .and_then(|partition_data| partition_data.records)
            .and_then(|deflated| inflated::Frame::try_from(deflated).ok())
            .map(|inflated| inflated.batches)
            .and_then(|mut batches| batches.pop())
            .map(|batch| batch.records)
            .inspect(|records| debug!(?records))
            .unwrap_or_default();

        assert_eq!(3, records.len());
        assert_eq!(Some(KEY), records[0].key);
        assert_eq!(Some(ONE), records[0].value);
        assert_eq!(0, records[0].offset_delta);

        assert_eq!(Some(KEY), records[1].key);
        assert_eq!(Some(TWO), records[1].value);
        assert_eq!(1, records[1].offset_delta);

        assert_eq!(Some(KEY), records[2].key);
        assert_eq!(Some(THREE), records[2].value);
        assert_eq!(2, records[2].offset_delta);
    }

    debug!(phase = "maintenance");
    sc.maintain(SystemTime::now()).await?;

    debug!(phase = "post: uncommitted earliest offset");
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

    debug!(phase = "post: uncommitted latest offset");
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

    let response = broker
        .serve(RequestInput {
            request: FetchRequest::default()
                .cluster_id(Some("".into()))
                .replica_id(Some(-1))
                .replica_state(Some(ReplicaState::default()))
                .max_wait_ms(500)
                .max_bytes(Some(4096))
                .min_bytes(1)
                .isolation_level(Some(1))
                .session_id(Some(-1))
                .session_epoch(Some(-1))
                .topics(Some(vec![
                    FetchTopic::default()
                        .topic(Some(topic_name.into()))
                        .topic_id(Some(topic_id))
                        .partitions(Some(vec![
                            FetchPartition::default()
                                .partition(partition)
                                .log_start_offset(Some(0))
                                .partition_max_bytes(4096),
                        ])),
                ]))
                .forgotten_topics_data(Some([].into()))
                .rack_id(Some("".into())),
            extensions: extensions.clone(),
        })
        .await?;

    {
        let records = response
            .responses
            .and_then(|mut responses| responses.pop())
            .and_then(|topic| topic.partitions)
            .and_then(|mut partitions| partitions.pop())
            .and_then(|partition_data| partition_data.records)
            .and_then(|deflated| inflated::Frame::try_from(deflated).ok())
            .map(|inflated| inflated.batches)
            .and_then(|mut batches| batches.pop())
            .map(|batch| batch.records)
            .inspect(|records| debug!(?records))
            .unwrap_or_default();

        assert_eq!(3, records.len());
        assert_eq!(Some(KEY), records[0].key);
        assert_eq!(Some(ONE), records[0].value);
        assert_eq!(0, records[0].offset_delta);

        assert_eq!(Some(KEY), records[1].key);
        assert_eq!(Some(TWO), records[1].value);
        assert_eq!(1, records[1].offset_delta);

        assert_eq!(Some(KEY), records[2].key);
        assert_eq!(Some(THREE), records[2].value);
        assert_eq!(2, records[2].offset_delta);
    }

    debug!(phase = "maintenance");
    sc.maintain(
        SystemTime::now()
            .checked_add(Duration::from_hours((7 * 24) + 1))
            .expect("7 days + 1 hour"),
    )
    .await?;

    debug!(phase = "post 7 days + 1 hour: uncommitted earliest offset");
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

    for partition in partitions {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(
            Some(offset_after_retention_deleted_every_record(
                partition.partition_index
            )),
            partition.offset
        );
        assert_eq!(Some(-1), partition.timestamp);
    }

    debug!(phase = "post 7 days + 1 hour: uncommitted latest offset");
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

    for partition in partitions {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(
            Some(offset_after_retention_deleted_every_record(
                partition.partition_index
            )),
            partition.offset
        );
        assert_eq!(Some(-1), partition.timestamp);
    }

    let response = broker
        .serve(RequestInput {
            request: FetchRequest::default()
                .cluster_id(Some("".into()))
                .replica_id(Some(-1))
                .replica_state(Some(ReplicaState::default()))
                .max_wait_ms(500)
                .max_bytes(Some(4096))
                .min_bytes(1)
                .isolation_level(Some(1))
                .session_id(Some(-1))
                .session_epoch(Some(-1))
                .topics(Some(vec![
                    FetchTopic::default()
                        .topic(Some(topic_name.into()))
                        .topic_id(Some(topic_id))
                        .partitions(Some(vec![
                            FetchPartition::default()
                                .partition(partition)
                                .log_start_offset(Some(0))
                                .partition_max_bytes(4096),
                        ])),
                ]))
                .forgotten_topics_data(Some([].into()))
                .rack_id(Some("".into())),
            extensions: extensions.clone(),
        })
        .await?;

    {
        let records = response
            .responses
            .and_then(|mut responses| responses.pop())
            .and_then(|topic| topic.partitions)
            .and_then(|mut partitions| partitions.pop())
            .and_then(|partition_data| partition_data.records)
            .and_then(|deflated| inflated::Frame::try_from(deflated).ok())
            .map(|inflated| inflated.batches)
            .and_then(|mut batches| batches.pop())
            .map(|batch| batch.records)
            .inspect(|records| debug!(?records))
            .unwrap_or_default();

        assert_eq!(0, records.len());
    }

    Ok(())
}

pub async fn compact_delete_001<G>(sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;

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
                        .configs(Some(
                            [
                                CreatableTopicConfig::default()
                                    .name(CLEANUP_POLICY.into())
                                    .value(Some([COMPACT, DELETE].join(","))),
                                CreatableTopicConfig::default()
                                    .name(RETENTION_MS.into())
                                    .value(Some(Duration::from_mins(30).as_millis().to_string())),
                            ]
                            .into(),
                        ))
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

    let topic_id = topics[0].topic_id.unwrap_or(NULL_TOPIC_ID);

    let replica_id = -1;
    let max_num_offsets = Some(6);
    let current_leader_epoch = -1;
    let isolation = Some(IsolationLevel::ReadUncommitted.into());

    const KEY: Bytes = Bytes::from_static(b"alpha");

    const ONE: Bytes = Bytes::from_static(b"one");

    let frame = inflated::Batch::builder()
        .record(
            Record::builder().key(Some(KEY)).value(Some(ONE)).header(
                Header::builder()
                    .key(Bytes::from_static(b"x"))
                    .value(Bytes::from_static(b"y")),
            ),
        )
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)?;

    let partition = PRODUCED_PARTITION;

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

    const TWO: Bytes = Bytes::from_static(b"two");

    let frame = inflated::Batch::builder()
        .record(Record::builder().key(Some(KEY)).value(Some(TWO)))
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

    const THREE: Bytes = Bytes::from_static(b"three");

    let frame = inflated::Batch::builder()
        .record(Record::builder().key(Some(KEY)).value(Some(THREE)))
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

    debug!(phase = "pre: uncommitted earliest offset");
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

    debug!(phase = "pre: uncommitted latest offset");
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

    let response = broker
        .serve(RequestInput {
            request: FetchRequest::default()
                .cluster_id(Some("".into()))
                .replica_id(Some(-1))
                .replica_state(Some(ReplicaState::default()))
                .max_wait_ms(500)
                .max_bytes(Some(4096))
                .min_bytes(1)
                .isolation_level(Some(1))
                .session_id(Some(-1))
                .session_epoch(Some(-1))
                .topics(Some(vec![
                    FetchTopic::default()
                        .topic(Some(topic_name.into()))
                        .topic_id(Some(topic_id))
                        .partitions(Some(vec![
                            FetchPartition::default()
                                .partition(partition)
                                .log_start_offset(Some(0))
                                .partition_max_bytes(4096),
                        ])),
                ]))
                .forgotten_topics_data(Some([].into()))
                .rack_id(Some("".into())),
            extensions: extensions.clone(),
        })
        .await?;

    {
        let batch = response
            .responses
            .and_then(|mut responses| responses.pop())
            .and_then(|topic| topic.partitions)
            .and_then(|mut partitions| partitions.pop())
            .and_then(|partition_data| partition_data.records)
            .and_then(|deflated| inflated::Frame::try_from(deflated).ok())
            .map(|inflated| inflated.batches)
            .and_then(|mut batches| batches.pop())
            .unwrap_or_default();

        assert_eq!(0, batch.base_offset);
        assert_eq!(3, batch.records.len());

        assert_eq!(Some(KEY), batch.records[0].key);
        assert_eq!(Some(ONE), batch.records[0].value);
        assert_eq!(0, batch.records[0].offset_delta);

        assert_eq!(Some(KEY), batch.records[1].key);
        assert_eq!(Some(TWO), batch.records[1].value);
        assert_eq!(1, batch.records[1].offset_delta);

        assert_eq!(Some(KEY), batch.records[2].key);
        assert_eq!(Some(THREE), batch.records[2].value);
        assert_eq!(2, batch.records[2].offset_delta);
    }

    debug!(phase = "maintenance");
    sc.maintain(SystemTime::now()).await?;

    debug!(phase = "post: uncommitted earliest offset");
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
    assert_eq!(Some(2), partitions[0].offset);
    assert_eq!(Some(-1), partitions[0].timestamp);

    for partition in partitions[1..].iter() {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(0), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    debug!(phase = "post: uncommitted latest offset");
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

    let response = broker
        .serve(RequestInput {
            request: FetchRequest::default()
                .cluster_id(Some("".into()))
                .replica_id(Some(-1))
                .replica_state(Some(ReplicaState::default()))
                .max_wait_ms(500)
                .max_bytes(Some(4096))
                .min_bytes(1)
                .isolation_level(Some(1))
                .session_id(Some(-1))
                .session_epoch(Some(-1))
                .topics(Some(vec![
                    FetchTopic::default()
                        .topic(Some(topic_name.into()))
                        .topic_id(Some(topic_id))
                        .partitions(Some(vec![
                            FetchPartition::default()
                                .partition(partition)
                                .log_start_offset(Some(0))
                                .partition_max_bytes(4096),
                        ])),
                ]))
                .forgotten_topics_data(Some([].into()))
                .rack_id(Some("".into())),
            extensions: extensions.clone(),
        })
        .await?;

    {
        let batch = response
            .responses
            .and_then(|mut responses| responses.pop())
            .and_then(|topic| topic.partitions)
            .and_then(|mut partitions| partitions.pop())
            .and_then(|partition_data| partition_data.records)
            .and_then(|deflated| inflated::Frame::try_from(deflated).ok())
            .map(|inflated| inflated.batches)
            .and_then(|mut batches| batches.pop())
            .unwrap_or_default();

        assert_eq!(2, batch.base_offset);

        assert_eq!(1, batch.records.len());

        assert_eq!(Some(KEY), batch.records[0].key);
        assert_eq!(Some(THREE), batch.records[0].value);
        assert_eq!(0, batch.records[0].offset_delta);
    }

    Ok(())
}

pub async fn compact_delete_002<G>(sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    let broker = broker(sc.clone())?;

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
                        .configs(Some(
                            [
                                CreatableTopicConfig::default()
                                    .name(CLEANUP_POLICY.into())
                                    .value(Some([COMPACT, DELETE].join(","))),
                                CreatableTopicConfig::default()
                                    .name(RETENTION_MS.into())
                                    .value(Some(Duration::from_mins(30).as_millis().to_string())),
                            ]
                            .into(),
                        ))
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

    let topic_id = topics[0].topic_id.unwrap_or(NULL_TOPIC_ID);

    let replica_id = -1;
    let max_num_offsets = Some(6);
    let current_leader_epoch = -1;
    let isolation = Some(IsolationLevel::ReadUncommitted.into());

    const KEY: Bytes = Bytes::from_static(b"alpha");

    const ONE: Bytes = Bytes::from_static(b"one");

    let frame = inflated::Batch::builder()
        .record(
            Record::builder().key(Some(KEY)).value(Some(ONE)).header(
                Header::builder()
                    .key(Bytes::from_static(b"x"))
                    .value(Bytes::from_static(b"y")),
            ),
        )
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)?;

    let partition = PRODUCED_PARTITION;

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

    const TWO: Bytes = Bytes::from_static(b"two");

    let frame = inflated::Batch::builder()
        .record(Record::builder().key(Some(KEY)).value(Some(TWO)))
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

    const THREE: Bytes = Bytes::from_static(b"three");

    let frame = inflated::Batch::builder()
        .record(Record::builder().key(Some(KEY)).value(Some(THREE)))
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

    debug!(phase = "pre: uncommitted earliest offset");
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

    debug!(phase = "pre: uncommitted latest offset");
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

    let response = broker
        .serve(RequestInput {
            request: FetchRequest::default()
                .cluster_id(Some("".into()))
                .replica_id(Some(-1))
                .replica_state(Some(ReplicaState::default()))
                .max_wait_ms(500)
                .max_bytes(Some(4096))
                .min_bytes(1)
                .isolation_level(Some(1))
                .session_id(Some(-1))
                .session_epoch(Some(-1))
                .topics(Some(vec![
                    FetchTopic::default()
                        .topic(Some(topic_name.into()))
                        .topic_id(Some(topic_id))
                        .partitions(Some(vec![
                            FetchPartition::default()
                                .partition(partition)
                                .log_start_offset(Some(0))
                                .partition_max_bytes(4096),
                        ])),
                ]))
                .forgotten_topics_data(Some([].into()))
                .rack_id(Some("".into())),
            extensions: extensions.clone(),
        })
        .await?;

    {
        let batch = response
            .responses
            .and_then(|mut responses| responses.pop())
            .and_then(|topic| topic.partitions)
            .and_then(|mut partitions| partitions.pop())
            .and_then(|partition_data| partition_data.records)
            .and_then(|deflated| inflated::Frame::try_from(deflated).ok())
            .map(|inflated| inflated.batches)
            .and_then(|mut batches| batches.pop())
            .unwrap_or_default();

        assert_eq!(0, batch.base_offset);

        assert_eq!(3, batch.records.len());
        assert_eq!(Some(KEY), batch.records[0].key);
        assert_eq!(Some(ONE), batch.records[0].value);
        assert_eq!(0, batch.records[0].offset_delta);

        assert_eq!(Some(KEY), batch.records[1].key);
        assert_eq!(Some(TWO), batch.records[1].value);
        assert_eq!(1, batch.records[1].offset_delta);

        assert_eq!(Some(KEY), batch.records[2].key);
        assert_eq!(Some(THREE), batch.records[2].value);
        assert_eq!(2, batch.records[2].offset_delta);
    }

    debug!(phase = "maintenance");
    sc.maintain(SystemTime::now()).await?;

    debug!(phase = "post: uncommitted earliest offset");
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
    assert_eq!(Some(2), partitions[0].offset);
    assert_eq!(Some(-1), partitions[0].timestamp);

    for partition in partitions[1..].iter() {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(Some(0), partition.offset);
        assert_eq!(Some(-1), partition.timestamp);
    }

    debug!(phase = "post: uncommitted latest offset");
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

    let response = broker
        .serve(RequestInput {
            request: FetchRequest::default()
                .cluster_id(Some("".into()))
                .replica_id(Some(-1))
                .replica_state(Some(ReplicaState::default()))
                .max_wait_ms(500)
                .max_bytes(Some(4096))
                .min_bytes(1)
                .isolation_level(Some(1))
                .session_id(Some(-1))
                .session_epoch(Some(-1))
                .topics(Some(vec![
                    FetchTopic::default()
                        .topic(Some(topic_name.into()))
                        .topic_id(Some(topic_id))
                        .partitions(Some(vec![
                            FetchPartition::default()
                                .partition(partition)
                                .log_start_offset(Some(0))
                                .partition_max_bytes(4096),
                        ])),
                ]))
                .forgotten_topics_data(Some([].into()))
                .rack_id(Some("".into())),
            extensions: extensions.clone(),
        })
        .await?;

    {
        let batch = response
            .responses
            .and_then(|mut responses| responses.pop())
            .and_then(|topic| topic.partitions)
            .and_then(|mut partitions| partitions.pop())
            .and_then(|partition_data| partition_data.records)
            .and_then(|deflated| inflated::Frame::try_from(deflated).ok())
            .map(|inflated| inflated.batches)
            .and_then(|mut batches| batches.pop())
            .unwrap_or_default();

        assert_eq!(2, batch.base_offset);

        assert_eq!(1, batch.records.len());
        assert_eq!(Some(KEY), batch.records[0].key);
        assert_eq!(Some(THREE), batch.records[0].value);
        assert_eq!(0, batch.records[0].offset_delta);
    }

    debug!(phase = "maintenance");
    sc.maintain(
        SystemTime::now()
            .checked_add(Duration::from_hours(1))
            .expect("an hour ahead"),
    )
    .await?;

    debug!(phase = "post 1 hour: uncommitted earliest offset");
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

    for partition in partitions {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(
            Some(offset_after_retention_deleted_every_record(
                partition.partition_index
            )),
            partition.offset
        );
        assert_eq!(Some(-1), partition.timestamp);
    }

    debug!(phase = "post 1 hour: uncommitted latest offset");
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

    for partition in partitions {
        assert_eq!(i16::from(ErrorCode::None), partition.error_code);
        assert_eq!(
            Some(offset_after_retention_deleted_every_record(
                partition.partition_index
            )),
            partition.offset
        );
        assert_eq!(Some(-1), partition.timestamp);
    }

    let response = broker
        .serve(RequestInput {
            request: FetchRequest::default()
                .cluster_id(Some("".into()))
                .replica_id(Some(-1))
                .replica_state(Some(ReplicaState::default()))
                .max_wait_ms(500)
                .max_bytes(Some(4096))
                .min_bytes(1)
                .isolation_level(Some(1))
                .session_id(Some(-1))
                .session_epoch(Some(-1))
                .topics(Some(vec![
                    FetchTopic::default()
                        .topic(Some(topic_name.into()))
                        .topic_id(Some(topic_id))
                        .partitions(Some(vec![
                            FetchPartition::default()
                                .partition(partition)
                                .log_start_offset(Some(0))
                                .partition_max_bytes(4096),
                        ])),
                ]))
                .forgotten_topics_data(Some([].into()))
                .rack_id(Some("".into())),
            extensions: extensions.clone(),
        })
        .await?;

    {
        let batch = response
            .responses
            .and_then(|mut responses| responses.pop())
            .and_then(|topic| topic.partitions)
            .and_then(|mut partitions| partitions.pop())
            .and_then(|partition_data| partition_data.records)
            .and_then(|deflated| inflated::Frame::try_from(deflated).ok())
            .map(|inflated| inflated.batches)
            .and_then(|mut batches| batches.pop())
            .unwrap_or_default();

        assert_eq!(0, batch.base_offset);
        assert_eq!(0, batch.records.len());
    }

    Ok(())
}

#[cfg(feature = "postgres")]
mod pg {
    use nisshi_storage::ArcDynStorage;

    use super::*;

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
    async fn compact_only() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::compact_only(sc).await
    }

    #[tokio::test]
    async fn delete_only() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::delete_only(sc).await
    }

    #[tokio::test]
    async fn delete_no_retention_ms_only() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::delete_no_retention_ms_only(sc).await
    }

    #[tokio::test]
    async fn compact_delete_001() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::compact_delete_001(sc).await
    }

    #[tokio::test]
    async fn compact_delete_002() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::compact_delete_002(sc).await
    }
}

mod in_memory {
    use nisshi_storage::ArcDynStorage;

    use super::*;

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

    #[ignore]
    #[tokio::test]
    async fn compact_only() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::compact_only(sc).await
    }

    #[ignore]
    #[tokio::test]
    async fn delete_only() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::delete_only(sc).await
    }

    #[ignore]
    #[tokio::test]
    async fn delete_no_retention_ms_only() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::delete_no_retention_ms_only(sc).await
    }

    #[ignore]
    #[tokio::test]
    async fn compact_delete_001() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::compact_delete_001(sc).await
    }

    #[ignore]
    #[tokio::test]
    async fn compact_delete_002() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::compact_delete_002(sc).await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use nisshi_storage::ArcDynStorage;

    use super::*;

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
    async fn compact_only() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::compact_only(sc).await
    }

    #[tokio::test]
    async fn delete_only() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::delete_only(sc).await
    }

    #[tokio::test]
    async fn delete_no_retention_ms_only() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::delete_no_retention_ms_only(sc).await
    }

    #[tokio::test]
    async fn compact_delete_001() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::compact_delete_001(sc).await
    }

    #[tokio::test]
    async fn compact_delete_002() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::compact_delete_002(sc).await
    }
}

#[cfg(feature = "turso")]
mod turso {
    use nisshi_storage::ArcDynStorage;

    use super::*;

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

    #[tokio::test]
    async fn compact_only() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::compact_only(sc).await
    }

    #[tokio::test]
    async fn delete_only() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::delete_only(sc).await
    }

    #[tokio::test]
    async fn delete_no_retention_ms_only() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::delete_no_retention_ms_only(sc).await
    }

    #[tokio::test]
    async fn compact_delete_001() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::compact_delete_001(sc).await
    }

    #[tokio::test]
    async fn compact_delete_002() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let sc = storage_container(cluster_id, broker_id).await?;
        register_broker(cluster_id, broker_id, &sc).await?;

        super::compact_delete_002(sc).await
    }
}
