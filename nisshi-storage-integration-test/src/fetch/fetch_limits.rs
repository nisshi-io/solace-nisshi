// Copyright (c) Solace Corporation
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

//! Cases about the byte limits of a Fetch request: `max_bytes` for the response, and
//! `partition_max_bytes` for each partition.

use std::time::{Duration, Instant};

use crate::{broker::register_broker, random::alphanumeric_string};
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    ByteSize as _, ErrorCode, FetchRequest, FetchResponse, IsolationLevel, NULL_TOPIC_ID,
    RequestInput,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    record::{Record, deflated, inflated},
};
use nisshi_storage::{FetchService, FetchValidationLayer, Storage, Topition};
use rama::{Layer as _, Service, extensions::Extensions};
use tracing::debug;

/// Partition 0 is empty, partition 1 holds a 200 KiB record, and partitions 2 and 3 hold small
/// records. The Fetch has a `max_bytes` of 100 KiB and a `partition_max_bytes` of 1 KiB. Partition
/// 1 returns its record, although the record exceeds both limits, because it is the first
/// partition with records. Partitions 2 and 3 return nothing, because that record used up the
/// response's `max_bytes`.
pub async fn an_oversized_first_batch_counts_in_full_against_the_response_max_bytes<C, G>(
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

    // Partition 0 stays empty, so partition 1 is the first non-empty one.
    let oversized = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from(vec![b'x'; 200 * 1024]))))
        .build()
        .and_then(TryInto::try_into)?;

    _ = sc
        .produce(None, &Topition::new(topic_name.clone(), 1), oversized)
        .await?;

    // Each of these records fits its partition's 1 KiB limit, so the response leaves them out only
    // because partition 1's record left no room in the response's `max_bytes`.
    for partition in 2..4 {
        produce_single_record_batches(&sc, &Topition::new(topic_name.clone(), partition), 1)
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
    // Partition 1's 200 KiB record is larger than the response's 100 KiB `max_bytes`, so the record
    // leaves no room for the other partitions.
    assert_eq!(vec![vec![], vec![0], vec![], vec![]], offsets);

    Ok(())
}

/// Partition 0 holds a small record, and partitions 1 to 3 each hold a 200 KiB record. The Fetch
/// has a `partition_max_bytes` of 1 KiB. Partition 0 returns its record. Partitions 1 to 3 return
/// nothing, because only the first partition with records may exceed its limit. Kafka reads them
/// the same way, with `minOneMessage` set to false ([ReplicaManager.scala#L1731-L1740]).
///
/// [ReplicaManager.scala#L1731-L1740]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1731-L1740
pub async fn a_later_partition_returns_no_batch_larger_than_its_limit<C, G>(
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
    assert_eq!(vec![vec![0], vec![], vec![], vec![]], offsets);

    Ok(())
}

/// Partition 0 holds a small record, and partition 1 holds a 200 KiB record. The Fetch has a
/// `min_bytes` of 1,000, a `partition_max_bytes` of 1 KiB and a `max_wait` of 10 seconds.
/// Partition 1 returns nothing, because its record does not fit, but its 1 KiB limit counts toward
/// `min_bytes`, as Kafka counts the bytes available up to each partition's limit
/// ([DelayedFetch.scala#L108-L110]). So the Fetch answers at once instead of waiting 10 seconds.
///
/// [DelayedFetch.scala#L108-L110]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/DelayedFetch.scala#L108-L110
pub async fn a_partition_at_its_hard_limit_counts_toward_min_bytes<C, G>(
    cluster_id: C,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    const MAX_WAIT: Duration = Duration::from_secs(10);

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

    produce_single_record_batches(&sc, &Topition::new(topic_name.clone(), 0), 1).await?;

    let oversized = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from(vec![b'x'; 200 * 1024]))))
        .build()
        .and_then(TryInto::try_into)?;

    _ = sc
        .produce(None, &Topition::new(topic_name.clone(), 1), oversized)
        .await?;

    let started_at = Instant::now();

    // Partition 0 returns fewer than `min_bytes` alone, and partition 1's limit of 1,024 bytes
    // brings the bytes available above it.
    let partitions = partition_data(
        fetch_offsets_within(
            &sc,
            &topic_name,
            &[(0, 0), (1, 0)],
            i32::try_from(MAX_WAIT.as_millis())?,
            1_000,
            100 * 1024,
            1024,
        )
        .await?,
    );

    assert!(
        started_at.elapsed() < MAX_WAIT / 2,
        "the Fetch waited {:?}",
        started_at.elapsed()
    );

    let mut offsets = vec![];
    for partition in &partitions {
        offsets.push(record_offsets(partition)?);
    }
    assert_eq!(vec![vec![0], vec![]], offsets);

    Ok(())
}

/// Two partitions each hold 80 records of 1,000 bytes, more than their 64 KiB
/// `partition_max_bytes`. Each partition returns records that fill its limit to within one record,
/// and never past it. The SQL engines count each record a few bytes smaller than its encoding, and
/// `fetched::tests::records_fill_the_limit_by_their_encoded_size` in `nisshi-storage` checks that
/// the read drops a record that an engine sends past the limit.
pub async fn every_partition_returns_the_records_that_fit_its_limit<C, G>(
    cluster_id: C,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    const PARTITION_MAX_BYTES: i32 = 64 * 1024;

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

    let batch: deflated::Batch = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from(vec![b'x'; 1_000]))))
        .build()
        .and_then(TryInto::try_into)?;

    // A full partition leaves less room than one more record. A one-record batch is at least that
    // size, also when an engine rebuilds the records into larger batches.
    let one_record_batch_size = batch.size_in_bytes()?;

    // Each partition holds more records than fit its limit.
    for partition in 0..2 {
        for _ in 0..80 {
            _ = sc
                .produce(
                    None,
                    &Topition::new(topic_name.clone(), partition),
                    batch.clone(),
                )
                .await?;
        }
    }

    // The Fetch answers after its first round, because `min_bytes` is 1, so a long `max_wait`
    // does not slow the test. It gives each read time to fill its limit on a slow machine,
    // where a read cut short by its deadline would return fewer records than fit.
    let partitions = partition_data(
        fetch_offsets_within(
            &sc,
            &topic_name,
            &[(0, 0), (1, 0)],
            30_000,
            1,
            50 * 1024 * 1024,
            PARTITION_MAX_BYTES,
        )
        .await?,
    );
    assert_eq!(2, partitions.len());

    for partition in &partitions {
        let size: usize = partition.records.as_ref().map_or(Ok(0), |frame| {
            frame
                .batches
                .iter()
                .map(|batch| batch.size_in_bytes())
                .sum::<nisshi_sans_io::Result<usize>>()
        })?;

        assert!(
            !record_offsets(partition)?.is_empty(),
            "partition {}",
            partition.partition_index
        );
        assert!(
            size <= usize::try_from(PARTITION_MAX_BYTES)?,
            "partition {}: {size} bytes of batches",
            partition.partition_index
        );
        assert!(
            size + one_record_batch_size > usize::try_from(PARTITION_MAX_BYTES)?,
            "partition {}: {size} bytes of batches leave room for another record",
            partition.partition_index
        );
    }

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
