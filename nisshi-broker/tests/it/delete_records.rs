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

use crate::common::{self, StorageType, alphanumeric_string, init_tracing, register_broker};
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    DeleteRecordsRequest, ErrorCode, FetchRequest, IsolationLevel, ListOffset, NULL_TOPIC_ID,
    RequestInput,
    create_topics_request::CreatableTopic,
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
    delete_records_response::DeleteRecordsPartitionResult,
    fetch_request::{FetchPartition, FetchTopic},
    record::{Record, inflated},
};
use nisshi_storage::{
    DeleteRecordsService, FetchService, FetchValidationLayer, ListOffsetResponse, Storage, Topition,
};
use rama::{Layer as _, Service, extensions::Extensions};
use rand::{prelude::*, rng};
use std::time::Duration;
use tracing::debug;
use url::Url;
use uuid::Uuid;

async fn create_topic<G>(sc: &G, num_partitions: i32) -> Result<String>
where
    G: Storage,
{
    let topic_name: String = alphanumeric_string(15);

    let topic_id = sc
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;
    debug!(?topic_id);

    Ok(topic_name)
}

async fn produce_one<G>(sc: &G, topition: &Topition) -> Result<i64>
where
    G: Storage,
{
    let value = Bytes::copy_from_slice(alphanumeric_string(15).as_bytes());

    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Some(value)))
        .build()
        .and_then(TryInto::try_into)?;

    sc.produce(None, topition, batch).await.map_err(Into::into)
}

async fn produce_n<G>(sc: &G, topition: &Topition, n: i64) -> Result<()>
where
    G: Storage,
{
    for _ in 0..n {
        _ = produce_one(sc, topition).await?;
    }

    Ok(())
}

/// Sends a single-topic, single-partition `DeleteRecords` request and
/// returns that one partition's result.
async fn delete_records_partition<G>(
    sc: &G,
    topic: &str,
    partition_index: i32,
    offset: i64,
) -> Result<DeleteRecordsPartitionResult>
where
    G: Storage + Clone,
{
    let response = DeleteRecordsService {
        storage: sc.clone(),
    }
    .serve(RequestInput {
        request: DeleteRecordsRequest::default().topics(Some(
            [DeleteRecordsTopic::default()
                .name(topic.into())
                .partitions(Some(
                    [DeleteRecordsPartition::default()
                        .partition_index(partition_index)
                        .offset(offset)]
                    .into(),
                ))]
            .into(),
        )),
        extensions: Extensions::default(),
    })
    .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());

    let partitions = topics[0].partitions.clone().unwrap_or_default();
    assert_eq!(1, partitions.len());

    Ok(partitions[0].clone())
}

/// Fetches from `fetch_offset` and returns the total number of records
/// returned.
async fn fetch_record_count<G>(sc: &G, topition: &Topition, fetch_offset: i64) -> Result<i64>
where
    G: Storage + Clone,
{
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

    let fetch = FetchValidationLayer::new()
        .layer(FetchService {
            storage: sc.clone(),
        })
        .serve(RequestInput {
            request: FetchRequest::default()
                .max_wait_ms(500)
                .min_bytes(1)
                .max_bytes(Some(50 * 1024))
                .isolation_level(Some((&IsolationLevel::ReadUncommitted).into()))
                .topics(Some(topics.into())),
            extensions: Extensions::default(),
        })
        .await?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(fetch.error_code.unwrap())?
    );

    Ok(fetch
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
                        .map(|records| {
                            records
                                .batches
                                .iter()
                                .map(|batch| batch.record_count as i64)
                                .sum::<i64>()
                        })
                        .unwrap_or_default()
                })
                .sum::<i64>()
        })
        .sum::<i64>())
}

/// Produces `record_count` records, deletes everything up to a mid-stream
/// offset, and checks that the response, `offset_stage`, `ListOffsets` and
/// `Fetch` all agree on the new low watermark.
pub async fn delete_up_to_offset<G>(cluster_id: Uuid, broker_id: i32, sc: G) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name = create_topic(&sc, 1).await?;
    let topition = Topition::new(topic_name.clone(), 0);

    let record_count = 6;
    produce_n(&sc, &topition, record_count).await?;

    let cutoff = 3;

    let result = delete_records_partition(&sc, &topic_name, 0, cutoff).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(result.error_code)?);
    assert_eq!(cutoff, result.low_watermark);

    let stage = sc.offset_stage(&topition).await?;
    assert_eq!(cutoff, stage.log_start());
    assert_eq!(record_count, stage.high_watermark());

    let earliest = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Earliest)],
        )
        .await?;
    assert!(matches!(
        earliest[..],
        [(_, ListOffsetResponse { offset: Some(offset), .. })] if offset == cutoff
    ));

    let latest = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await?;
    assert!(matches!(
        latest[..],
        [(_, ListOffsetResponse { offset: Some(offset), .. })] if offset == record_count
    ));

    assert_eq!(
        record_count - cutoff,
        fetch_record_count(&sc, &topition, cutoff).await?
    );

    Ok(())
}

/// A cutoff that falls strictly inside a multi-record batch's range must
/// not take the whole batch with it.
///
/// Every other scenario in this file produces single-record batches, so a
/// cutoff always lands exactly on a batch boundary. Here one batch holds
/// four records (offsets 0..=3) and the cutoff is 2: strictly above the
/// batch's base offset (0) and strictly below its end offset (3). The
/// correct rule deletes a batch only once its *end* offset is below the
/// cutoff; a backend that instead checks only a batch's *start* offset
/// would delete this whole batch -- including offsets 2 and 3, which must
/// still be visible -- because its base (0) is below the cutoff (2).
///
/// A second, standalone record follows the batch, so that the straddled
/// batch is not the newest one. DynoStore never deletes its newest batch.
/// If the straddled batch were the newest, DynoStore would keep it even with
/// a wrong deletion rule, and the test would pass on a broken backend.
pub async fn delete_offset_inside_a_batch_keeps_the_whole_batch<G>(
    cluster_id: Uuid,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name = create_topic(&sc, 1).await?;
    let topition = Topition::new(topic_name.clone(), 0);

    let batch_record_count = 4i32;

    let mut builder = inflated::Batch::builder().last_offset_delta(batch_record_count - 1);

    for delta in 0..batch_record_count {
        builder = builder.record(
            Record::builder()
                .offset_delta(delta)
                .value(Some(Bytes::from(format!("record-{delta}")))),
        );
    }

    let batch = builder.build().and_then(TryInto::try_into)?;
    _ = sc.produce(None, &topition, batch).await?;

    // A standalone record at offset 4, after the batch.
    _ = produce_one(&sc, &topition).await?;

    let record_count = i64::from(batch_record_count) + 1;
    let cutoff = 2;

    let result = delete_records_partition(&sc, &topic_name, 0, cutoff).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(result.error_code)?);
    assert_eq!(cutoff, result.low_watermark);

    let stage = sc.offset_stage(&topition).await?;
    assert_eq!(cutoff, stage.log_start());
    assert_eq!(record_count, stage.high_watermark());

    let earliest = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Earliest)],
        )
        .await?;

    assert!(
        matches!(
            earliest[..],
            [(_, ListOffsetResponse { offset: Some(offset), .. })] if offset == cutoff
        ),
        "{earliest:?}"
    );

    // Every record at or after the cutoff must still be reachable, in
    // order and without gaps. Fetching at an offset that falls inside a
    // batch returns that batch whole (the client skips the records below
    // the fetch offset) -- so on a batch-granular backend this legitimately
    // hands back offsets 0 and 1 too, folded in with the batch that
    // survived them. What it must never do is lose offsets 2 or 3: a
    // start-based mutant deletes this whole batch merely because it
    // *starts* (at 0) before the cutoff, which would leave only the
    // standalone record at offset 4 -- a gap this loop catches.
    let batches = sc
        .fetch(
            &topition,
            cutoff,
            1,
            50 * 1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(500),
        )
        .await?
        .into_iter()
        .try_fold(Vec::new(), |mut acc, batch| {
            inflated::Batch::try_from(batch)
                .map(|inflated| {
                    acc.push(inflated);
                    acc
                })
                .map_err(nisshi_broker::Error::from)
        })?;

    let mut expected = cutoff;

    for batch in &batches {
        for record in &batch.records {
            let offset = batch.base_offset + i64::from(record.offset_delta);

            // a whole batch can begin before the fetch offset on a
            // batch-granular backend: the client skips the records below it
            if offset < cutoff {
                continue;
            }

            assert_eq!(expected, offset, "fetch at cutoff {cutoff}");
            expected += 1;
        }
    }

    assert_eq!(
        record_count, expected,
        "fetch at cutoff {cutoff} is missing records"
    );

    Ok(())
}

/// `offset == -1` is the common case ("delete everything up to the high
/// watermark"). The delete moves the log start to the high watermark and
/// leaves the high watermark as it was, so `ListOffsets` answers the high
/// watermark for both `Earliest` and `Latest`.
pub async fn delete_to_high_watermark_keeps_latest<G>(
    cluster_id: Uuid,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name = create_topic(&sc, 1).await?;
    let topition = Topition::new(topic_name.clone(), 0);

    let record_count = 6;
    produce_n(&sc, &topition, record_count).await?;

    let result = delete_records_partition(&sc, &topic_name, 0, -1).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(result.error_code)?);
    assert_eq!(record_count, result.low_watermark);

    let stage = sc.offset_stage(&topition).await?;
    assert_eq!(record_count, stage.log_start());
    assert_eq!(record_count, stage.high_watermark());

    let latest = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Latest)],
        )
        .await?;

    assert!(matches!(
        latest[..],
        [(_, ListOffsetResponse { offset: Some(offset), .. })] if offset == record_count
    ));

    let earliest = sc
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), ListOffset::Earliest)],
        )
        .await?;

    assert!(
        matches!(
            earliest[..],
            [(_, ListOffsetResponse { offset: Some(offset), .. })] if offset == record_count
        ),
        "{earliest:?}"
    );

    Ok(())
}

/// An offset above the high watermark is rejected and leaves the watermark
/// untouched.
pub async fn delete_above_high_watermark_is_rejected<G>(
    cluster_id: Uuid,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name = create_topic(&sc, 1).await?;
    let topition = Topition::new(topic_name.clone(), 0);

    let record_count = 3;
    produce_n(&sc, &topition, record_count).await?;

    let result = delete_records_partition(&sc, &topic_name, 0, record_count + 1).await?;
    assert_eq!(
        ErrorCode::OffsetOutOfRange,
        ErrorCode::try_from(result.error_code)?
    );
    assert_eq!(-1, result.low_watermark);

    let stage = sc.offset_stage(&topition).await?;
    assert_eq!(0, stage.log_start());
    assert_eq!(record_count, stage.high_watermark());

    Ok(())
}

/// Any negative offset other than `-1` is rejected.
pub async fn delete_negative_offset_other_than_minus_one_is_rejected<G>(
    cluster_id: Uuid,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name = create_topic(&sc, 1).await?;
    let topition = Topition::new(topic_name.clone(), 0);

    produce_n(&sc, &topition, 3).await?;

    let result = delete_records_partition(&sc, &topic_name, 0, -2).await?;
    assert_eq!(
        ErrorCode::OffsetOutOfRange,
        ErrorCode::try_from(result.error_code)?
    );
    assert_eq!(-1, result.low_watermark);

    Ok(())
}

/// A request at or below the current log start is a no-op: it succeeds and
/// reports the *existing* low watermark, never moving it backward.
pub async fn delete_at_or_below_log_start_is_a_noop<G>(
    cluster_id: Uuid,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name = create_topic(&sc, 1).await?;
    let topition = Topition::new(topic_name.clone(), 0);

    let record_count = 6;
    produce_n(&sc, &topition, record_count).await?;

    let first = delete_records_partition(&sc, &topic_name, 0, 3).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(first.error_code)?);
    assert_eq!(3, first.low_watermark);

    // Repeating the same cutoff, and asking for an even earlier one, are
    // both no-ops that report the log start unchanged.
    for offset in [3, 0] {
        let repeat = delete_records_partition(&sc, &topic_name, 0, offset).await?;
        assert_eq!(ErrorCode::None, ErrorCode::try_from(repeat.error_code)?);
        assert_eq!(3, repeat.low_watermark);
    }

    let stage = sc.offset_stage(&topition).await?;
    assert_eq!(3, stage.log_start());

    Ok(())
}

/// An unknown topic and an out-of-range partition each get their own
/// per-partition `UnknownTopicOrPartition`, while a sibling valid partition
/// in the *same* request still succeeds -- proving a per-partition problem
/// never fails the whole request.
pub async fn delete_unknown_topic_or_partition_does_not_fail_siblings<G>(
    cluster_id: Uuid,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let topic_name = create_topic(&sc, 1).await?;
    let topition = Topition::new(topic_name.clone(), 0);

    let record_count = 4;
    produce_n(&sc, &topition, record_count).await?;

    let unknown_topic: String = alphanumeric_string(15);

    let response = DeleteRecordsService {
        storage: sc.clone(),
    }
    .serve(RequestInput {
        request: DeleteRecordsRequest::default().topics(Some(
            [
                DeleteRecordsTopic::default()
                    .name(unknown_topic.clone())
                    .partitions(Some(
                        [DeleteRecordsPartition::default()
                            .partition_index(0)
                            .offset(0)]
                        .into(),
                    )),
                DeleteRecordsTopic::default()
                    .name(topic_name.clone())
                    .partitions(Some(
                        [
                            DeleteRecordsPartition::default()
                                .partition_index(1)
                                .offset(0),
                            DeleteRecordsPartition::default()
                                .partition_index(0)
                                .offset(2),
                        ]
                        .into(),
                    )),
            ]
            .into(),
        )),
        extensions: Extensions::default(),
    })
    .await?;

    let topics = response.topics.unwrap_or_default();
    assert_eq!(2, topics.len());

    let unknown_topic_result = topics
        .iter()
        .find(|topic| topic.name == unknown_topic)
        .expect("unknown topic result");
    let unknown_topic_partitions = unknown_topic_result.partitions.clone().unwrap_or_default();
    assert_eq!(1, unknown_topic_partitions.len());
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(unknown_topic_partitions[0].error_code)?
    );
    assert_eq!(-1, unknown_topic_partitions[0].low_watermark);

    let known_topic_result = topics
        .iter()
        .find(|topic| topic.name == topic_name)
        .expect("known topic result");
    let known_topic_partitions = known_topic_result.partitions.clone().unwrap_or_default();
    assert_eq!(2, known_topic_partitions.len());

    let out_of_range_partition = known_topic_partitions
        .iter()
        .find(|partition| partition.partition_index == 1)
        .expect("out of range partition result");
    assert_eq!(
        ErrorCode::UnknownTopicOrPartition,
        ErrorCode::try_from(out_of_range_partition.error_code)?
    );
    assert_eq!(-1, out_of_range_partition.low_watermark);

    let valid_partition = known_topic_partitions
        .iter()
        .find(|partition| partition.partition_index == 0)
        .expect("valid sibling partition result");
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(valid_partition.error_code)?
    );
    assert_eq!(2, valid_partition.low_watermark);

    let stage = sc.offset_stage(&topition).await?;
    assert_eq!(2, stage.log_start());

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
    async fn delete_up_to_offset() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_up_to_offset(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_offset_inside_a_batch_keeps_the_whole_batch() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_offset_inside_a_batch_keeps_the_whole_batch(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_to_high_watermark_keeps_latest() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_to_high_watermark_keeps_latest(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_above_high_watermark_is_rejected() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_above_high_watermark_is_rejected(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_negative_offset_other_than_minus_one_is_rejected() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_negative_offset_other_than_minus_one_is_rejected(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_at_or_below_log_start_is_a_noop() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_at_or_below_log_start_is_a_noop(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_unknown_topic_or_partition_does_not_fail_siblings() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_unknown_topic_or_partition_does_not_fail_siblings(
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
    async fn delete_up_to_offset() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_up_to_offset(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_offset_inside_a_batch_keeps_the_whole_batch() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_offset_inside_a_batch_keeps_the_whole_batch(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_to_high_watermark_keeps_latest() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_to_high_watermark_keeps_latest(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_above_high_watermark_is_rejected() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_above_high_watermark_is_rejected(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_negative_offset_other_than_minus_one_is_rejected() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_negative_offset_other_than_minus_one_is_rejected(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_at_or_below_log_start_is_a_noop() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_at_or_below_log_start_is_a_noop(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_unknown_topic_or_partition_does_not_fail_siblings() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_unknown_topic_or_partition_does_not_fail_siblings(
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
    async fn delete_up_to_offset() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_up_to_offset(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_offset_inside_a_batch_keeps_the_whole_batch() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_offset_inside_a_batch_keeps_the_whole_batch(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_to_high_watermark_keeps_latest() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_to_high_watermark_keeps_latest(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_above_high_watermark_is_rejected() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_above_high_watermark_is_rejected(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_negative_offset_other_than_minus_one_is_rejected() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_negative_offset_other_than_minus_one_is_rejected(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_at_or_below_log_start_is_a_noop() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_at_or_below_log_start_is_a_noop(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_unknown_topic_or_partition_does_not_fail_siblings() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_unknown_topic_or_partition_does_not_fail_siblings(
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

    #[tokio::test]
    async fn delete_up_to_offset() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_up_to_offset(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_offset_inside_a_batch_keeps_the_whole_batch() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_offset_inside_a_batch_keeps_the_whole_batch(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_to_high_watermark_keeps_latest() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_to_high_watermark_keeps_latest(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_above_high_watermark_is_rejected() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_above_high_watermark_is_rejected(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_negative_offset_other_than_minus_one_is_rejected() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_negative_offset_other_than_minus_one_is_rejected(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_at_or_below_log_start_is_a_noop() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_at_or_below_log_start_is_a_noop(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_unknown_topic_or_partition_does_not_fail_siblings() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_unknown_topic_or_partition_does_not_fail_siblings(
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
    async fn delete_up_to_offset() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_up_to_offset(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_offset_inside_a_batch_keeps_the_whole_batch() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_offset_inside_a_batch_keeps_the_whole_batch(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_to_high_watermark_keeps_latest() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_to_high_watermark_keeps_latest(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_above_high_watermark_is_rejected() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_above_high_watermark_is_rejected(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_negative_offset_other_than_minus_one_is_rejected() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_negative_offset_other_than_minus_one_is_rejected(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_at_or_below_log_start_is_a_noop() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_at_or_below_log_start_is_a_noop(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[tokio::test]
    async fn delete_unknown_topic_or_partition_does_not_fail_siblings() -> Result<()> {
        let _guard = init_tracing()?;
        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);
        super::delete_unknown_topic_or_partition_does_not_fail_siblings(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}
