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

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant, SystemTime},
};

use crate::common::{self, StorageType, alphanumeric_string, init_tracing, register_broker};
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    BatchAttribute, Compression, ErrorCode, FetchRequest, FetchResponse, IsolationLevel,
    ListOffset, NULL_TOPIC_ID, RequestInput,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
    fetch_request::{FetchPartition, FetchTopic},
    record::{Header, Record, deflated, inflated},
};
use nisshi_storage::{FetchService, FetchValidationLayer, ListOffsetResponse, Storage, Topition};
use rama::{Layer as _, Service, extensions::Extensions};
use rand::{prelude::*, rng};
use tracing::{debug, error};
use url::Url;
use uuid::Uuid;

pub async fn empty_topic<G>(cluster_id: Uuid, broker_id: i32, sc: G) -> Result<()>
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

    let record_count = 0;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let max_wait_ms = 500;
    let min_bytes = 1;
    let max_bytes = Some(50 * 1024);
    let isolation_level = &IsolationLevel::ReadUncommitted;
    let topics = [FetchTopic::default()
        .topic(Some(topition.topic().to_string()))
        .topic_id(Some(NULL_TOPIC_ID))
        .partitions(Some(vec![
            FetchPartition::default()
                .partition(topition.partition())
                .current_leader_epoch(Some(-1))
                .fetch_offset(0)
                .last_fetched_epoch(Some(-1))
                .log_start_offset(Some(-1))
                .partition_max_bytes(50 * 1024)
                .replica_directory_id(None),
        ]))];

    let extensions = Extensions::default();

    let fetch = FetchValidationLayer::new()
        .layer(FetchService {
            storage: sc.clone(),
        })
        .serve(RequestInput {
            request: FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(min_bytes)
                .max_bytes(max_bytes)
                .isolation_level(Some(isolation_level.into()))
                .topics(Some(topics.into())),
            extensions: extensions.clone(),
        })
        .await?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(fetch.error_code.unwrap())?
    );

    for response in fetch.responses.as_ref().unwrap_or(&vec![]) {
        for partition in response.partitions.as_ref().unwrap_or(&vec![]) {
            for batch in &partition.records.as_ref().unwrap().batches {
                assert_eq!(-1, batch.producer_id);
                assert_eq!(-1, batch.producer_epoch);
            }
        }
    }

    assert_eq!(
        record_count,
        fetch
            .responses
            .unwrap_or_default()
            .iter()
            .map(|response| {
                response
                    .partitions
                    .as_deref()
                    .unwrap_or(&[])
                    .iter()
                    .map(|partition| {
                        partition
                            .records
                            .as_ref()
                            .unwrap()
                            .batches
                            .iter()
                            .map(|batch| batch.record_count as i64)
                            .sum::<i64>()
                    })
                    .sum::<i64>()
            })
            .sum::<i64>()
    );

    Ok(())
}

pub async fn kv_header(
    cluster_id: impl Into<String>,
    broker_id: i32,
    sc: impl Storage + Clone,
) -> Result<()> {
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

    let record_count = 6;
    let header_count = 3;

    let mut messages = BTreeMap::new();

    let random_data = || Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());

    for n in 0..record_count {
        let message_key = random_data();
        let message_value = random_data();

        let headers = (0..header_count)
            .map(|_| (random_data(), random_data()))
            .collect::<Vec<_>>();

        let batch = inflated::Batch::builder()
            .record(
                headers.iter().cloned().fold(
                    Record::builder()
                        .key(message_key.clone().into())
                        .value(message_value.clone().into()),
                    |builder, (key, value)| builder.header(Header::builder().key(key).value(value)),
                ),
            )
            .producer_id(-1)
            .producer_epoch(-1)
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        debug!(n, ?batch);

        let offset = sc
            .produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(?offset))?;

        debug!(offset);

        assert_eq!(
            None,
            messages.insert(
                offset,
                FullFatRecord {
                    key: Some(message_key),
                    value: Some(message_value),
                    headers: headers
                        .iter()
                        .cloned()
                        .map(|(key, value)| Header::builder().key(key).value(value).build())
                        .collect(),
                }
            )
        );
    }

    let max_wait_ms = 500;
    let min_bytes = 1;
    let max_bytes = Some(50 * 1024);
    let isolation_level = &IsolationLevel::ReadUncommitted;
    let fetch_offset = 0;

    let topics = [FetchTopic::default()
        .topic(Some(topition.topic().to_string()))
        .topic_id(Some(NULL_TOPIC_ID))
        .partitions(Some(
            [FetchPartition::default()
                .partition(topition.partition())
                .current_leader_epoch(Some(-1))
                .fetch_offset(fetch_offset)
                .last_fetched_epoch(Some(-1))
                .log_start_offset(Some(-1))
                .partition_max_bytes(50 * 1024)
                .replica_directory_id(None)]
            .into(),
        ))];

    let extensions = Extensions::default();

    let fetched = FetchValidationLayer::new()
        .layer(FetchService {
            storage: sc.clone(),
        })
        .serve(RequestInput {
            request: FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(min_bytes)
                .max_bytes(max_bytes)
                .isolation_level(Some(isolation_level.into()))
                .topics(Some(topics.into())),
            extensions: extensions.clone(),
        })
        .await
        .map(records)?;

    assert_eq!(messages.len(), fetched.len());

    for (offset, record) in fetched {
        assert_eq!(
            messages
                .get(&offset)
                .and_then(|message| message.key.clone())
                .unwrap_or_default(),
            record.key.clone().unwrap_or_default()
        );

        assert_eq!(
            messages
                .get(&offset)
                .and_then(|message| message.value.clone())
                .unwrap_or_default(),
            record.value.clone().unwrap_or_default()
        );

        assert_eq!(
            messages
                .get(&offset)
                .map(|message| message.headers.clone())
                .unwrap_or_default()
                .iter()
                .collect::<BTreeSet<_>>(),
            record.headers.clone().iter().collect::<BTreeSet<_>>()
        );
    }

    Ok(())
}

impl From<Record> for FullFatRecord {
    fn from(record: Record) -> Self {
        Self {
            key: record.key,
            value: record.value,
            headers: record.headers,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct FullFatRecord {
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<Header>,
}

fn records(fetch: FetchResponse) -> BTreeMap<i64, FullFatRecord> {
    fetch
        .responses
        .unwrap_or_default()
        .into_iter()
        .flat_map(|topic| {
            topic
                .partitions
                .unwrap_or_default()
                .into_iter()
                .flat_map(|partition| {
                    partition
                        .records
                        .map(|records| records.batches)
                        .unwrap_or_default()
                })
        })
        .filter_map(|deflated| inflated::Batch::try_from(deflated).ok())
        .flat_map(|batch| {
            batch.records.into_iter().map(move |record| {
                (
                    batch.base_offset + (record.offset_delta as i64),
                    record.into(),
                )
            })
        })
        .collect()
}

// regression test for headers being attached to the wrong record when the
// first record visible to a fetch is not the record at the requested fetch
// offset (here because an earlier record sharing the same key has been
// removed by `cleanup.policy=compact`)
pub async fn compacted_header(
    cluster_id: impl Into<String>,
    broker_id: i32,
    sc: impl Storage + Clone,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let num_partitions = 1;
    let replication_factor = 0;
    let assignments = Some([].into());
    let configs = Some(
        [CreatableTopicConfig::default()
            .name("cleanup.policy".into())
            .value(Some("compact".into()))]
        .into(),
    );

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(replication_factor)
                .assignments(assignments)
                .configs(configs),
            false,
        )
        .await?;
    debug!(?topic_id);

    let topition = Topition::new(topic_name.clone(), 0);

    let key = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());

    let compacted_away = vec![
        Header::builder()
            .key(Bytes::from_static(b"version"))
            .value(Bytes::from_static(b"compacted-away"))
            .build(),
    ];

    let retained = vec![
        Header::builder()
            .key(Bytes::from_static(b"version"))
            .value(Bytes::from_static(b"retained"))
            .build(),
    ];

    // two records sharing the same key: the first (offset 0) is removed by
    // compaction, leaving only the second (offset 1)
    for (value, headers) in [
        (Bytes::from_static(b"older"), &compacted_away),
        (Bytes::from_static(b"newer"), &retained),
    ] {
        let batch = inflated::Batch::builder()
            .record(headers.iter().cloned().fold(
                Record::builder().key(Some(key.clone())).value(Some(value)),
                |builder, header| builder.header(header.into()),
            ))
            .producer_id(-1)
            .producer_epoch(-1)
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        _ = sc
            .produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(?offset))?;
    }

    sc.maintain(SystemTime::now())
        .await
        .inspect_err(|err| error!(?err))?;

    let max_wait_ms = 500;
    let min_bytes = 1;
    let max_bytes = Some(50 * 1024);
    let isolation_level = &IsolationLevel::ReadUncommitted;
    let fetch_offset = 0;

    let topics = [FetchTopic::default()
        .topic(Some(topition.topic().to_string()))
        .topic_id(Some(NULL_TOPIC_ID))
        .partitions(Some(
            [FetchPartition::default()
                .partition(topition.partition())
                .current_leader_epoch(Some(-1))
                .fetch_offset(fetch_offset)
                .last_fetched_epoch(Some(-1))
                .log_start_offset(Some(-1))
                .partition_max_bytes(50 * 1024)
                .replica_directory_id(None)]
            .into(),
        ))];

    let fetched = FetchValidationLayer::new()
        .layer(FetchService {
            storage: sc.clone(),
        })
        .serve(RequestInput {
            request: FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(min_bytes)
                .max_bytes(max_bytes)
                .isolation_level(Some(isolation_level.into()))
                .topics(Some(topics.into())),
            extensions: Extensions::default(),
        })
        .await
        .map(records)?;

    // the record at offset 0 has been compacted away, leaving only the
    // record at offset 1, which must be fetched with its own headers
    assert_eq!(1, fetched.len());

    let (offset, record) = fetched.into_iter().next().expect("single record");
    assert_eq!(1, offset);
    assert_eq!(Some(key), record.key);
    assert_eq!(Some(Bytes::from_static(b"newer")), record.value);
    assert_eq!(retained, record.headers);

    Ok(())
}

pub async fn simple_non_txn<C, G>(cluster_id: C, broker_id: i32, sc: G) -> Result<()>
where
    C: Into<String>,
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

    let transaction_timeout_ms = 10_000;

    let partition_index = rng().random_range(0..num_partitions);
    let topition = Topition::new(topic_name.clone(), partition_index);

    let list_offsets = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await
        .inspect_err(|err| error!(?err))
        .inspect(|list_offsets| debug!(?list_offsets))?;

    assert_eq!(1, list_offsets.len());

    assert!(matches!(
        list_offsets[0].1,
        ListOffsetResponse {
            offset: Some(0),
            ..
        }
    ));

    let record_count = 6;

    let mut offset_producer = BTreeMap::new();
    let mut bytes = 0;

    for n in 0..record_count {
        let producer = sc
            .init_producer(None, transaction_timeout_ms, Some(-1), Some(-1))
            .await?;

        let value = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());
        bytes += value.len();

        let batch = inflated::Batch::builder()
            .record(Record::builder().value(value.clone().into()))
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))?;

        debug!(n, bytes, ?batch);

        let offset = sc
            .produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(?offset))?;

        debug!(offset);

        assert_eq!(None, offset_producer.insert(offset, (producer, value)));
    }

    let list_offsets = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await
        .inspect_err(|err| error!(?err))
        .inspect(|list_offsets| debug!(?list_offsets))?;

    assert_eq!(1, list_offsets.len());

    assert!(matches!(
        list_offsets[0].1,
        ListOffsetResponse {
            offset: Some(records),
            ..
            } if records == record_count
    ));

    let max_wait_ms = 500;
    let min_bytes = 1;
    let max_bytes = Some(50 * 1024);
    let isolation_level = &IsolationLevel::ReadUncommitted;
    let topics = [FetchTopic::default()
        .topic(Some(topition.topic().to_string()))
        .topic_id(Some(NULL_TOPIC_ID))
        .partitions(Some(
            [FetchPartition::default()
                .partition(topition.partition())
                .current_leader_epoch(Some(-1))
                .fetch_offset(0)
                .last_fetched_epoch(Some(-1))
                .log_start_offset(Some(-1))
                .partition_max_bytes(50 * 1024)
                .replica_directory_id(None)]
            .into(),
        ))];

    let fetch = FetchValidationLayer::new()
        .layer(FetchService {
            storage: sc.clone(),
        })
        .serve(RequestInput {
            request: FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(min_bytes)
                .max_bytes(max_bytes)
                .isolation_level(Some(isolation_level.into()))
                .topics(Some(topics.into())),
            extensions: Extensions::default(),
        })
        .await?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(fetch.error_code.unwrap())?
    );

    assert_eq!(
        record_count,
        fetch
            .responses
            .unwrap_or_default()
            .iter()
            .map(|response| {
                response
                    .partitions
                    .as_deref()
                    .unwrap_or(&[])
                    .iter()
                    .map(|partition| {
                        partition
                            .records
                            .as_ref()
                            .unwrap()
                            .batches
                            .iter()
                            .map(|batch| batch.record_count as i64)
                            .sum::<i64>()
                    })
                    .sum::<i64>()
            })
            .sum::<i64>()
    );

    Ok(())
}

/// A fetch offset falling inside a batch returns that batch whole, as
/// Kafka does: the client skips the records below the fetch offset. Only
/// batches ending before the fetch offset may be omitted.
pub async fn mid_batch<C, G>(cluster_id: C, broker_id: i32, sc: G) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;
    debug!(?topic_id);

    let topition = Topition::new(topic_name.clone(), 0);

    let values: Vec<Bytes> = (0..6)
        .map(|_| Bytes::copy_from_slice(alphanumeric_string(15).as_bytes()))
        .collect();

    // two batches of three records: offsets 0..=2 and 3..=5
    for batch_values in values.chunks(3) {
        let producer = sc.init_producer(None, 10_000, Some(-1), Some(-1)).await?;

        let mut builder = inflated::Batch::builder()
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .last_offset_delta(batch_values.len() as i32 - 1);

        for (delta, value) in batch_values.iter().enumerate() {
            builder = builder.record(
                Record::builder()
                    .offset_delta(delta as i32)
                    .value(Some(value.clone())),
            );
        }

        let batch = builder.build().and_then(TryInto::try_into)?;

        _ = sc
            .produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(offset))?;
    }

    let min_bytes = 1;
    let max_bytes = 50 * 1024;
    let isolation = IsolationLevel::ReadUncommitted;
    let max_wait = Duration::from_millis(500);

    let high_watermark = values.len() as i64;

    for fetch_offset in 0..=high_watermark {
        let batches = sc
            .fetch(
                &topition,
                fetch_offset,
                min_bytes,
                max_bytes,
                isolation,
                max_wait,
            )
            .await
            .inspect_err(|err| error!(?err, fetch_offset))?
            .into_iter()
            .try_fold(Vec::new(), |mut acc, batch| {
                inflated::Batch::try_from(batch)
                    .map(|inflated| {
                        acc.push(inflated);
                        acc
                    })
                    .map_err(nisshi_broker::Error::from)
            })?;

        // every record from the fetch offset to the high watermark is
        // returned, in order, and no returned batch ends before the
        // fetch offset
        let mut expected = fetch_offset;

        for batch in &batches {
            // the pg and lite engines return an empty placeholder batch
            // when there are no records to fetch
            if batch.records.is_empty() {
                continue;
            }

            assert!(
                batch.base_offset + i64::from(batch.last_offset_delta) >= fetch_offset,
                "fetch at {fetch_offset} returned a batch ending before it"
            );

            for record in &batch.records {
                let offset = batch.base_offset + i64::from(record.offset_delta);

                // a whole batch can begin before the fetch offset: the
                // client skips the records below it
                if offset < fetch_offset {
                    continue;
                }

                assert_eq!(expected, offset, "fetch at {fetch_offset}");
                assert_eq!(
                    values.get(offset as usize).cloned(),
                    record.value(),
                    "fetch at {fetch_offset}, record at {offset}"
                );

                expected += 1;
            }
        }

        assert_eq!(
            high_watermark, expected,
            "fetch at {fetch_offset} is missing records"
        );
    }

    Ok(())
}

/// A fetched batch carries the compression codec it was produced with, as
/// Kafka does with `compression.type=producer`. Engines that rebuild
/// batches from per-record rows must keep the codec and re-deflate with it
/// rather than hand back plaintext.
pub async fn compression_preserved<C, G>(cluster_id: C, broker_id: i32, sc: G) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;
    debug!(?topic_id);

    let topition = Topition::new(topic_name.clone(), 0);

    let codecs = [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ];

    // one batch of three records per codec, produced in order
    let mut produced = Vec::with_capacity(codecs.len());

    for codec in codecs {
        let values: Vec<Bytes> = (0..3)
            .map(|_| Bytes::copy_from_slice(alphanumeric_string(64).as_bytes()))
            .collect();

        let mut builder = inflated::Batch::builder()
            .attributes(BatchAttribute::default().compression(codec.clone()).into())
            .last_offset_delta(values.len() as i32 - 1);

        for (delta, value) in values.iter().enumerate() {
            builder = builder.record(
                Record::builder()
                    .offset_delta(delta as i32)
                    .value(Some(value.clone())),
            );
        }

        let batch = builder.build().and_then(TryInto::try_into)?;

        _ = sc
            .produce(None, &topition, batch)
            .await
            .inspect(|offset| debug!(?codec, offset))?;

        produced.push((codec, values));
    }

    // a generous max_wait: the pg and lite engines stop assembling at the
    // deadline, and nothing here depends on timing
    let fetched = sc
        .fetch(
            &topition,
            0,
            1,
            50 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_secs(5),
        )
        .await
        .inspect_err(|err| error!(?err))?
        .into_iter()
        // the pg and lite engines return an empty placeholder batch when
        // there are no records to fetch
        .filter(|batch| batch.record_count > 0)
        .map(|batch| {
            let codec = Compression::try_from(batch.attributes)?;

            inflated::Batch::try_from(batch).map(|inflated| {
                (
                    codec,
                    inflated
                        .records
                        .iter()
                        .filter_map(Record::value)
                        .collect::<Vec<_>>(),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(produced, fetched);

    Ok(())
}

/// A fetch for a topic name that was never created must be answered with
/// `UNKNOWN_TOPIC_OR_PARTITION` immediately, on an open connection.
/// `fetch_topic` returning `Err` would close the connection instead (the
/// only mechanism that does: the `?` in `nisshi-service/src/stream.rs`'s
/// request loop), and waiting out the full `max_wait` before answering
/// `NONE` would silently stall the client. No leader hint is sent either:
/// Kafka sends one only with a leadership error.
pub async fn unknown_topic_name<C, G>(cluster_id: C, broker_id: i32, sc: G) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    // long enough that waiting it out would fail the elapsed check
    let max_wait_ms = 10_000;
    let answered_within = Duration::from_secs(5);

    let fetch_request = |topic_name: &str| {
        FetchRequest::default()
            .max_wait_ms(max_wait_ms)
            .min_bytes(1)
            .max_bytes(Some(50 * 1024))
            .isolation_level(Some((&IsolationLevel::ReadUncommitted).into()))
            .topics(Some(
                [FetchTopic::default()
                    .topic(Some(topic_name.into()))
                    .topic_id(Some(NULL_TOPIC_ID))
                    .partitions(Some(
                        [FetchPartition::default()
                            .partition(0)
                            .current_leader_epoch(Some(-1))
                            .fetch_offset(0)
                            .last_fetched_epoch(Some(-1))
                            .log_start_offset(Some(-1))
                            .partition_max_bytes(50 * 1024)]
                        .into(),
                    ))]
                .into(),
            ))
    };

    let started_at = SystemTime::now();

    let response = FetchValidationLayer::new()
        .layer(FetchService {
            storage: sc.clone(),
        })
        .serve(RequestInput {
            request: fetch_request(&topic_name),
            extensions: Extensions::default(),
        })
        .await?;

    let elapsed = started_at.elapsed()?;
    assert!(elapsed < answered_within, "took {elapsed:?}");

    let topics = response.responses.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(Some(topic_name.as_str()), topics[0].topic.as_deref());
    assert_eq!(Some(NULL_TOPIC_ID), topics[0].topic_id);

    let partitions = topics
        .into_iter()
        .flat_map(|topic| topic.partitions.unwrap_or_default())
        .collect::<Vec<_>>();

    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert_eq!(None, partitions[0].current_leader);

    // the service still answers a normal fetch afterwards: no regression to
    // the working path. A short max_wait keeps this quick, since an empty,
    // existing topic otherwise waits out max_wait for more data that will
    // never come.
    _ = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let response = FetchValidationLayer::new()
        .layer(FetchService { storage: sc })
        .serve(RequestInput {
            request: fetch_request(&topic_name).max_wait_ms(200),
            extensions: Extensions::default(),
        })
        .await?;

    let partitions = response
        .responses
        .unwrap_or_default()
        .into_iter()
        .flat_map(|topic| topic.partitions.unwrap_or_default())
        .collect::<Vec<_>>();

    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );

    Ok(())
}

/// A fetch for an unknown topic name beside an existing topic idle at its
/// high watermark waits out `max_wait`, as in Kafka, which sets unknown
/// topics aside and waits for the rest. Answering at once would have a
/// consumer still assigned to a deleted topic fetch at round trip speed.
pub async fn unknown_topic_name_beside_idle_topic<C, G>(
    cluster_id: C,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let unknown_name: String = alphanumeric_string(15);
    let idle_name: String = alphanumeric_string(15);
    debug!(?unknown_name, ?idle_name);

    _ = sc
        .create_topic(
            CreatableTopic::default()
                .name(idle_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let fetch_topic = |topic_name: String| {
        FetchTopic::default()
            .topic(Some(topic_name))
            .topic_id(Some(NULL_TOPIC_ID))
            .partitions(Some(
                [FetchPartition::default()
                    .partition(0)
                    .current_leader_epoch(Some(-1))
                    .fetch_offset(0)
                    .last_fetched_epoch(Some(-1))
                    .log_start_offset(Some(-1))
                    .partition_max_bytes(50 * 1024)]
                .into(),
            ))
    };

    let max_wait_ms = 1_000;
    let started_at = SystemTime::now();

    let response = FetchValidationLayer::new()
        .layer(FetchService { storage: sc })
        .serve(RequestInput {
            request: FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(1)
                .max_bytes(Some(50 * 1024))
                .isolation_level(Some((&IsolationLevel::ReadUncommitted).into()))
                .topics(Some(
                    [fetch_topic(unknown_name.clone()), fetch_topic(idle_name)].into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let elapsed = started_at.elapsed()?;
    assert!(
        elapsed >= Duration::from_millis(max_wait_ms as u64 / 2),
        "answered after {elapsed:?}"
    );

    let topics = response.responses.unwrap_or_default();
    assert_eq!(2, topics.len());
    assert_eq!(Some(unknown_name.as_str()), topics[0].topic.as_deref());

    let partitions = topics
        .into_iter()
        .flat_map(|topic| topic.partitions.unwrap_or_default())
        .collect::<Vec<_>>();

    assert_eq!(2, partitions.len());
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[1].error_code)?
    );

    Ok(())
}

/// A fetch by topic id (v13 and later) for an id that does not exist is
/// answered with `UNKNOWN_TOPIC_OR_PARTITION` at once, and the response
/// carries the requested id. The Java client matches a v13 response topic by
/// id and drops one it cannot match, so the null id would hide the error
/// from it, and a consumer on a deleted topic would fetch again at round
/// trip speed instead of refreshing its metadata.
pub async fn unknown_topic_id<C, G>(cluster_id: C, broker_id: i32, sc: G) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_id = Uuid::now_v7().into_bytes();
    debug!(?topic_id);

    // long enough that waiting it out would fail the elapsed check
    let max_wait_ms = 10_000;
    let answered_within = Duration::from_secs(5);

    let started_at = SystemTime::now();

    let response = FetchValidationLayer::new()
        .layer(FetchService { storage: sc })
        .serve(RequestInput {
            request: FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(1)
                .max_bytes(Some(50 * 1024))
                .isolation_level(Some((&IsolationLevel::ReadUncommitted).into()))
                .topics(Some(
                    [FetchTopic::default()
                        .topic(None)
                        .topic_id(Some(topic_id))
                        .partitions(Some(
                            [FetchPartition::default()
                                .partition(0)
                                .current_leader_epoch(Some(-1))
                                .fetch_offset(0)
                                .last_fetched_epoch(Some(-1))
                                .log_start_offset(Some(-1))
                                .partition_max_bytes(50 * 1024)]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let elapsed = started_at.elapsed()?;
    assert!(elapsed < answered_within, "took {elapsed:?}");

    let topics = response.responses.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(Some(topic_id), topics[0].topic_id);

    let partitions = topics
        .into_iter()
        .flat_map(|topic| topic.partitions.unwrap_or_default())
        .collect::<Vec<_>>();

    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert_eq!(None, partitions[0].current_leader);

    Ok(())
}

/// Produce `count` single record batches to `topition`.
async fn produce_single_record_batches<G>(sc: &G, topition: &Topition, count: usize) -> Result<()>
where
    G: Storage,
{
    for _ in 0..count {
        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Some(Bytes::copy_from_slice(
                alphanumeric_string(15).as_bytes(),
            ))))
            .build()
            .and_then(TryInto::try_into)?;

        _ = sc
            .produce(None, topition, batch)
            .await
            .inspect(|offset| debug!(offset))?;
    }

    Ok(())
}

/// Fetch through [`FetchService`], one entry per `(partition, fetch_offset)`,
/// with a `max_bytes` and a `partition_max_bytes` of 50 KiB.
async fn fetch_offsets<G>(
    sc: &G,
    topic: &str,
    partitions: &[(i32, i64)],
    max_wait_ms: i32,
) -> Result<FetchResponse>
where
    G: Storage + Clone,
{
    fetch_offsets_within(sc, topic, partitions, max_wait_ms, 1, 50 * 1024, 50 * 1024).await
}

/// Fetch through [`FetchService`], one entry per `(partition, fetch_offset)`,
/// with the given `min_bytes`, `max_bytes` and `partition_max_bytes`.
async fn fetch_offsets_within<G>(
    sc: &G,
    topic: &str,
    partitions: &[(i32, i64)],
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    partition_max_bytes: i32,
) -> Result<FetchResponse>
where
    G: Storage + Clone,
{
    FetchValidationLayer::new()
        .layer(FetchService {
            storage: sc.clone(),
        })
        .serve(RequestInput {
            request: FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(min_bytes)
                .max_bytes(Some(max_bytes))
                .isolation_level(Some((&IsolationLevel::ReadUncommitted).into()))
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(topic.into()))
                        .topic_id(Some(NULL_TOPIC_ID))
                        .partitions(Some(
                            partitions
                                .iter()
                                .map(|(partition, fetch_offset)| {
                                    FetchPartition::default()
                                        .partition(*partition)
                                        .current_leader_epoch(Some(-1))
                                        .fetch_offset(*fetch_offset)
                                        .last_fetched_epoch(Some(-1))
                                        .log_start_offset(Some(-1))
                                        .partition_max_bytes(partition_max_bytes)
                                })
                                .collect(),
                        ))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await
        .map_err(Into::into)
}

/// The partition answers in a fetch response, in request order.
fn partition_data(response: FetchResponse) -> Vec<nisshi_sans_io::fetch_response::PartitionData> {
    response
        .responses
        .unwrap_or_default()
        .into_iter()
        .flat_map(|topic| topic.partitions.unwrap_or_default())
        .collect()
}

/// The offsets of the records in a partition answer, skipping the empty
/// placeholder batch that the pg and lite engines return.
fn record_offsets(partition: &nisshi_sans_io::fetch_response::PartitionData) -> Result<Vec<i64>> {
    let mut offsets = vec![];

    for batch in partition
        .records
        .as_ref()
        .map(|frame| frame.batches.clone())
        .unwrap_or_default()
    {
        if batch.record_count == 0 {
            continue;
        }

        let batch = inflated::Batch::try_from(batch)?;

        offsets.extend(
            batch
                .records
                .iter()
                .map(|record| batch.base_offset + i64::from(record.offset_delta)),
        );
    }

    Ok(offsets)
}

fn assert_offset_out_of_range(
    partition: &nisshi_sans_io::fetch_response::PartitionData,
    fetch_offset: i64,
) -> Result<()> {
    assert_eq!(
        ErrorCode::OffsetOutOfRange,
        ErrorCode::try_from(partition.error_code)?,
        "fetch at {fetch_offset}"
    );

    // Kafka sends unknown offsets with this error
    assert_eq!(-1, partition.high_watermark, "fetch at {fetch_offset}");
    assert_eq!(
        Some(-1),
        partition.last_stable_offset,
        "fetch at {fetch_offset}"
    );
    assert_eq!(
        Some(-1),
        partition.log_start_offset,
        "fetch at {fetch_offset}"
    );
    assert!(partition.records.is_none(), "fetch at {fetch_offset}");

    Ok(())
}

/// A fetch offset below the log start is answered with
/// `OFFSET_OUT_OF_RANGE` straight away (not after `max_wait`), without
/// disturbing another partition in the same request, and the service keeps
/// answering valid fetches afterwards. A fetch offset above the high
/// watermark is answered with `NONE` and no records on every engine, where
/// Kafka answers `NONE` only up to the log end offset.
pub async fn offset_out_of_range<C, G>(cluster_id: C, broker_id: i32, sc: G) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    _ = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(2)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let records = 3;

    for partition in 0..2 {
        produce_single_record_batches(&sc, &Topition::new(topic_name.clone(), partition), records)
            .await?;
    }

    let high_watermark = records as i64;

    // long enough that waiting it out would fail the elapsed check
    let max_wait_ms = 10_000;
    let answered_within = Duration::from_secs(5);

    for fetch_offset in [-5, i64::MIN] {
        let started_at = SystemTime::now();
        let response = fetch_offsets(&sc, &topic_name, &[(0, fetch_offset)], max_wait_ms).await?;
        let elapsed = started_at.elapsed()?;

        assert!(
            elapsed < answered_within,
            "fetch at {fetch_offset} took {elapsed:?}"
        );

        let partitions = partition_data(response);
        assert_eq!(1, partitions.len());
        assert_offset_out_of_range(&partitions[0], fetch_offset)?;
    }

    // a client can be ahead of the high watermark that a broker reads
    for fetch_offset in [high_watermark + 1, i64::MAX] {
        let response = fetch_offsets(&sc, &topic_name, &[(0, fetch_offset)], 500).await?;
        let partitions = partition_data(response);
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?,
            "fetch at {fetch_offset}"
        );
        assert_eq!(
            high_watermark, partitions[0].high_watermark,
            "fetch at {fetch_offset}"
        );
        assert!(partitions[0].records.is_none(), "fetch at {fetch_offset}");
    }

    // one partition out of range, the other idle at its high watermark: the
    // error ends the wait
    {
        let started_at = SystemTime::now();
        let response = fetch_offsets(
            &sc,
            &topic_name,
            &[(0, -5), (1, high_watermark)],
            max_wait_ms,
        )
        .await?;
        let elapsed = started_at.elapsed()?;

        assert!(elapsed < answered_within, "took {elapsed:?}");

        let partitions = partition_data(response);
        assert_eq!(2, partitions.len());
        assert_offset_out_of_range(&partitions[0], -5)?;
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[1].error_code)?
        );
        assert!(record_offsets(&partitions[1])?.is_empty());
    }

    // one partition out of range, the other holding records
    {
        let started_at = SystemTime::now();
        let response = fetch_offsets(&sc, &topic_name, &[(0, -5), (1, 0)], max_wait_ms).await?;
        let elapsed = started_at.elapsed()?;

        assert!(elapsed < answered_within, "took {elapsed:?}");

        let partitions = partition_data(response);
        assert_eq!(2, partitions.len());

        assert_eq!(0, partitions[0].partition_index);
        assert_offset_out_of_range(&partitions[0], -5)?;

        assert_eq!(1, partitions[1].partition_index);
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[1].error_code)?
        );
        assert_eq!(high_watermark, partitions[1].high_watermark);
        assert_eq!(vec![0, 1, 2], record_offsets(&partitions[1])?);
    }

    // the high watermark is in range: a caught-up consumer waits there
    {
        let response = fetch_offsets(&sc, &topic_name, &[(0, high_watermark)], 500).await?;
        let partitions = partition_data(response);
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(high_watermark, partitions[0].high_watermark);
        assert!(record_offsets(&partitions[0])?.is_empty());
    }

    // and the service still answers a valid fetch
    {
        let response = fetch_offsets(&sc, &topic_name, &[(0, 0)], max_wait_ms).await?;
        let partitions = partition_data(response);
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(vec![0, 1, 2], record_offsets(&partitions[0])?);
    }

    Ok(())
}

/// After DeleteRecords moves the log start offset, a fetch below it is out
/// of range and a fetch at it returns the surviving records. Only engines
/// that advance the log start offset can run this.
pub async fn below_log_start<C, G>(cluster_id: C, broker_id: i32, sc: G) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    _ = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(topic_name.clone(), 0);
    produce_single_record_batches(&sc, &topition, 5).await?;

    let log_start = 3;

    let deleted = sc
        .delete_records(&[DeleteRecordsTopic::default()
            .name(topic_name.clone())
            .partitions(Some(
                [DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(log_start)]
                .into(),
            ))])
        .await?;
    debug!(?deleted);

    assert_eq!(log_start, sc.offset_stage(&topition).await?.log_start());

    {
        let response = fetch_offsets(&sc, &topic_name, &[(0, log_start - 1)], 10_000).await?;
        let partitions = partition_data(response);
        assert_eq!(1, partitions.len());
        assert_offset_out_of_range(&partitions[0], log_start - 1)?;
    }

    {
        let response = fetch_offsets(&sc, &topic_name, &[(0, log_start)], 10_000).await?;
        let partitions = partition_data(response);
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(Some(log_start), partitions[0].log_start_offset);
        assert_eq!(vec![3, 4], record_offsets(&partitions[0])?);
    }

    Ok(())
}

/// A fetch for an unknown topic beside a live topic idle at its high
/// watermark waits out `max_wait`, as in Kafka, which sets unknown topics
/// aside and waits for the rest. A consumer still assigned to a deleted
/// topic then fetches once per `max_wait`, not at round trip speed.
pub async fn unknown_topic_beside_idle_topic<C, G>(
    cluster_id: C,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    _ = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let partition = |fetch_offset| {
        FetchPartition::default()
            .partition(0)
            .current_leader_epoch(Some(-1))
            .fetch_offset(fetch_offset)
            .last_fetched_epoch(Some(-1))
            .log_start_offset(Some(-1))
            .partition_max_bytes(50 * 1024)
    };

    let max_wait_ms = 1_000;
    let started_at = SystemTime::now();

    let response = FetchValidationLayer::new()
        .layer(FetchService {
            storage: sc.clone(),
        })
        .serve(RequestInput {
            request: FetchRequest::default()
                .max_wait_ms(max_wait_ms)
                .min_bytes(1)
                .max_bytes(Some(50 * 1024))
                .isolation_level(Some((&IsolationLevel::ReadUncommitted).into()))
                .topics(Some(
                    [
                        FetchTopic::default()
                            .topic(None)
                            .topic_id(Some(Uuid::now_v7().into_bytes()))
                            .partitions(Some([partition(0)].into())),
                        FetchTopic::default()
                            .topic(Some(topic_name))
                            .topic_id(Some(NULL_TOPIC_ID))
                            .partitions(Some([partition(0)].into())),
                    ]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    let elapsed = started_at.elapsed()?;
    assert!(
        elapsed >= Duration::from_millis(max_wait_ms as u64 / 2),
        "answered after {elapsed:?}"
    );

    let partitions = partition_data(response);
    assert_eq!(2, partitions.len());
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[1].error_code)?
    );
    assert!(record_offsets(&partitions[1])?.is_empty());

    Ok(())
}

/// A record larger than the fetch's `max_bytes` is returned when it is the
/// first record at the fetch offset, so that a consumer always makes progress
/// (KIP-74). The record uses up the budget, so the fetch returns nothing after
/// it.
pub async fn oversized_first_record_is_returned_alone<C, G>(
    cluster_id: C,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    _ = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(topic_name.clone(), 0);

    // four times the 50 KiB that `fetch_offsets` allows
    let oversized = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from(vec![b'x'; 200 * 1024]))))
        .build()
        .and_then(TryInto::try_into)?;

    _ = sc.produce(None, &topition, oversized).await?;
    produce_single_record_batches(&sc, &topition, 1).await?;

    for (fetch_offset, expected) in [(0, vec![0]), (1, vec![1])] {
        let partitions =
            partition_data(fetch_offsets(&sc, &topic_name, &[(0, fetch_offset)], 500).await?);
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?,
            "fetch at {fetch_offset}"
        );
        assert_eq!(
            expected,
            record_offsets(&partitions[0])?,
            "fetch at {fetch_offset}"
        );
    }

    Ok(())
}

/// A response holds at most `max_bytes` and one batch, however many
/// partitions it lists. A partition that returns a batch larger than its
/// `partition_max_bytes` spends the batch's size from the response's
/// `max_bytes`, not only its own limit.
pub async fn oversized_records_spend_the_response_max_bytes<C, G>(
    cluster_id: C,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    _ = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(4)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    produce_single_record_batches(&sc, &Topition::new(topic_name.clone(), 0), 1).await?;

    for partition in 1..4 {
        let oversized = inflated::Batch::builder()
            .record(Record::builder().value(Some(Bytes::from(vec![b'x'; 200 * 1024]))))
            .build()
            .and_then(TryInto::try_into)?;

        _ = sc
            .produce(
                None,
                &Topition::new(topic_name.clone(), partition),
                oversized,
            )
            .await?;
    }

    let partitions = partition_data(
        fetch_offsets_within(
            &sc,
            &topic_name,
            &[(0, 0), (1, 0), (2, 0), (3, 0)],
            500,
            1,
            100 * 1024,
            1024,
        )
        .await?,
    );
    assert_eq!(4, partitions.len());

    // Partition 1 returns its record although the record is larger than its
    // 1 KiB limit. Kafka returns nothing there, because only the first
    // non-empty partition may exceed a limit. The record spends the rest of
    // the 100 KiB, so partitions 2 and 3 return nothing.
    let mut offsets = vec![];
    for partition in &partitions {
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partition.error_code)?,
            "partition {}",
            partition.partition_index
        );
        offsets.push(record_offsets(partition)?);
    }
    assert_eq!(vec![vec![0], vec![0], vec![], vec![]], offsets);

    Ok(())
}

/// A Fetch whose `min_bytes` is never met waits for its `max_wait`, and then
/// returns the records that it read. Each wait round reads the partition again
/// with the whole `max_bytes`, so the last round still returns records after
/// earlier rounds read the same bytes. The Postgres engine reads at most
/// 10,000 rows at a time, so there the service reads again from the next
/// offset, and the response spans several reads.
pub async fn long_wait_for_min_bytes_spans_several_reads<C, G>(
    cluster_id: C,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    // more records than two reads of the Postgres engine return
    const RECORDS: i32 = 25_000;
    const MAX_WAIT: Duration = Duration::from_secs(5);

    // The service gives a storage read a deadline of its own, after
    // `max_wait`, and the margin also covers a slow CI runner.
    const OVERRUN: Duration = Duration::from_secs(10);

    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name: String = alphanumeric_string(15);
    debug!(?topic_name);

    _ = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let batch: deflated::Batch = (0..RECORDS)
        .fold(inflated::Batch::builder(), |builder, delta| {
            builder.record(
                Record::builder()
                    .value(Some(Bytes::from_static(b"a")))
                    .offset_delta(delta),
            )
        })
        .last_offset_delta(RECORDS - 1)
        .build()
        .and_then(TryInto::try_into)?;

    // One round reads the whole batch at most, which leaves `min_bytes`
    // unmet. A round that started with the bytes that the round before it
    // spent would have a tenth of a batch to read.
    let max_bytes = i32::try_from(batch.record_data.len() + batch.record_data.len() / 10)?;

    _ = sc
        .produce(None, &Topition::new(topic_name.clone(), 0), batch)
        .await?;

    let started_at = Instant::now();

    let partitions = partition_data(
        fetch_offsets_within(
            &sc,
            &topic_name,
            &[(0, 0)],
            i32::try_from(MAX_WAIT.as_millis())?,
            i32::MAX,
            max_bytes,
            max_bytes,
        )
        .await?,
    );

    // The service returns before `max_wait` when the time left is shorter
    // than its last round took, so that it does not overrun `max_wait`. A
    // round that does not meet `min_bytes` waits half the time left, so the
    // fetch returns after half of `max_wait` at the earliest.
    let elapsed = started_at.elapsed();
    assert!(
        elapsed >= MAX_WAIT / 2,
        "the fetch returned after {elapsed:?}, before half its max_wait of {MAX_WAIT:?}"
    );
    assert!(
        elapsed < MAX_WAIT + OVERRUN,
        "the fetch returned after {elapsed:?}, more than {OVERRUN:?} after its max_wait"
    );

    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );

    let offsets = record_offsets(&partitions[0])?;
    assert!(
        offsets.len() > 10_000,
        "expected more than one Postgres read of 10,000 records after the wait, got {}",
        offsets.len()
    );
    assert_eq!(
        (0..i64::try_from(offsets.len())?).collect::<Vec<_>>(),
        offsets
    );

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
    async fn compression_preserved() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::compression_preserved(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn kv_header() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::kv_header(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn empty_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn mid_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::mid_batch(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn unknown_topic_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::unknown_topic_name(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn unknown_topic_name_beside_idle_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::unknown_topic_name_beside_idle_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn unknown_topic_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::unknown_topic_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn offset_out_of_range() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::offset_out_of_range(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn unknown_topic_beside_idle_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::unknown_topic_beside_idle_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn oversized_first_record_is_returned_alone() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::oversized_first_record_is_returned_alone(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn oversized_records_spend_the_response_max_bytes() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::oversized_records_spend_the_response_max_bytes(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn long_wait_for_min_bytes_spans_several_reads() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::long_wait_for_min_bytes_spans_several_reads(
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
    async fn compression_preserved() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::compression_preserved(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn kv_header() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::kv_header(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn empty_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn mid_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::mid_batch(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn unknown_topic_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::unknown_topic_name(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn unknown_topic_name_beside_idle_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::unknown_topic_name_beside_idle_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn unknown_topic_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::unknown_topic_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn offset_out_of_range() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::offset_out_of_range(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn unknown_topic_beside_idle_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::unknown_topic_beside_idle_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn oversized_first_record_is_returned_alone() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::oversized_first_record_is_returned_alone(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn oversized_records_spend_the_response_max_bytes() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::oversized_records_spend_the_response_max_bytes(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn long_wait_for_min_bytes_spans_several_reads() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::long_wait_for_min_bytes_spans_several_reads(
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
    async fn compression_preserved() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::compression_preserved(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn kv_header() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::kv_header(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn compacted_header() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::compacted_header(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn empty_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn mid_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::mid_batch(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn unknown_topic_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::unknown_topic_name(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn unknown_topic_name_beside_idle_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::unknown_topic_name_beside_idle_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn unknown_topic_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::unknown_topic_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn offset_out_of_range() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::offset_out_of_range(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn unknown_topic_beside_idle_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::unknown_topic_beside_idle_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn oversized_first_record_is_returned_alone() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::oversized_first_record_is_returned_alone(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn oversized_records_spend_the_response_max_bytes() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::oversized_records_spend_the_response_max_bytes(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn long_wait_for_min_bytes_spans_several_reads() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::long_wait_for_min_bytes_spans_several_reads(
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

    #[ignore = "the Turso engine does not start in the broker tests yet, see #866"]
    #[tokio::test]
    async fn oversized_first_record_is_returned_alone() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::oversized_first_record_is_returned_alone(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[ignore = "the Turso engine does not start in the broker tests yet, see #866"]
    #[tokio::test]
    async fn oversized_records_spend_the_response_max_bytes() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::oversized_records_spend_the_response_max_bytes(
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
    async fn compression_preserved() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::compression_preserved(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn kv_header() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::kv_header(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    // compaction keeps the log start at 0, so a fetch at 0 passes the
    // bounds check and reads the surviving record at 1
    #[tokio::test]
    async fn compacted_header() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::compacted_header(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn empty_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn simple_non_txn() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::simple_non_txn(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn mid_batch() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::mid_batch(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn unknown_topic_name() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::unknown_topic_name(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn unknown_topic_name_beside_idle_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::unknown_topic_name_beside_idle_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn unknown_topic_id() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::unknown_topic_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn offset_out_of_range() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::offset_out_of_range(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn below_log_start() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::below_log_start(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn unknown_topic_beside_idle_topic() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::unknown_topic_beside_idle_topic(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}
