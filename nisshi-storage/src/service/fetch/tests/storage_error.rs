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

//! A storage error in one partition of a Fetch answers that partition with an error code, and
//! the other partitions keep their answers. The Fetch returns `Ok`, so the connection stays
//! open.

use std::collections::BTreeMap;

use nisshi_sans_io::{
    ErrorCode, IsolationLevel,
    fetch_request::{FetchPartition, FetchTopic},
    fetch_response::PartitionData,
};
use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, SdkMeterProvider,
    data::{AggregatedMetrics, MetricData, SumDataPoint},
};
use tokio::time::Duration;
use uuid::Uuid;

use super::{
    FETCH_DEADLINE, MAX_WAIT, Partitions, Read, Scripted, TOPIC, batch, batches, bounded,
    capture_metrics, fetch_partition_at, fetch_topics,
};
use crate::{
    Error, FetchService, Result, StorageOperation,
    service::fetch::{STORAGE_FAILURE_WARN_INTERVAL, storage_failure_warns_now},
};

/// Fetches an offset above the cached high watermark, which makes the service read the current
/// offset stage.
async fn fetch_above_the_high_watermark(storage: Scripted) -> Result<PartitionData> {
    fetch_partition_at(
        storage,
        Duration::from_millis(100),
        1024,
        IsolationLevel::ReadUncommitted,
        1_050,
    )
    .await
}

/// An error that a retry may clear.
fn transient() -> Error {
    Error::from(std::io::Error::other("the read failed"))
}

/// A `current_offset_stage` read that a retry may clear answers its partition with
/// `KAFKA_STORAGE_ERROR` and unknown offsets, and reads no records.
#[tokio::test(start_paused = true)]
async fn a_current_offset_stage_read_that_fails_transiently_answers_kafka_storage_error()
-> Result<()> {
    let mut storage = bounded();
    storage.current_error = Some(transient());

    let partition = fetch_above_the_high_watermark(storage.clone()).await?;

    assert_eq!(
        ErrorCode::KafkaStorageError,
        ErrorCode::try_from(partition.error_code)?
    );
    assert_eq!(-1, partition.high_watermark);
    assert_eq!(Some(-1), partition.log_start_offset);
    assert_eq!(None, partition.records);
    assert_eq!(1, storage.current_offset_stage_reads());
    assert!(storage.call_offsets().is_empty());

    Ok(())
}

/// A `current_offset_stage` read that no retry can clear answers `UNKNOWN_SERVER_ERROR`, as
/// Kafka answers an exception that it has no code for.
#[tokio::test(start_paused = true)]
async fn a_current_offset_stage_read_that_fails_permanently_answers_unknown_server_error()
-> Result<()> {
    let mut storage = bounded();
    storage.current_error = Some(Error::Message("the read failed".into()));

    let partition = fetch_above_the_high_watermark(storage).await?;

    assert_eq!(
        ErrorCode::UnknownServerError,
        ErrorCode::try_from(partition.error_code)?
    );
    assert_eq!(-1, partition.high_watermark);

    Ok(())
}

/// A metadata read that fails answers every partition of its topic with `KAFKA_STORAGE_ERROR`,
/// without waiting for `max_wait`, and the other topics of the Fetch keep their records.
#[tokio::test(start_paused = true)]
async fn a_metadata_read_that_fails_answers_the_partitions_of_its_topic() -> Result<()> {
    let storage = Partitions::new([(0, vec![Read::Batch(batch(0, &[0])?)])])
        .fail_metadata("bad", transient());

    let (topics, elapsed) =
        fetch_topics(storage, MAX_WAIT, &[("bad", &[0, 1]), (TOPIC, &[0])]).await?;

    assert_eq!(Duration::ZERO, elapsed);
    assert_eq!(2, topics.len());

    assert_eq!(2, topics[0].len());
    for (index, partition) in topics[0].iter().enumerate() {
        assert_eq!(i32::try_from(index)?, partition.partition_index);
        assert_eq!(
            ErrorCode::KafkaStorageError,
            ErrorCode::try_from(partition.error_code)?
        );
        assert_eq!(-1, partition.high_watermark);
        assert_eq!(0, batches(partition));
    }

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(topics[1][0].error_code)?
    );
    assert_eq!(1, batches(&topics[1][0]));

    Ok(())
}

/// The partitions of a topic whose metadata read failed count as read, so the partition that
/// follows them has the whole fetch deadline, not a half of it.
#[tokio::test(start_paused = true)]
async fn a_metadata_read_that_fails_leaves_the_fetch_deadline_to_the_later_topics() -> Result<()> {
    let storage = Partitions::new([(0, vec![Read::Stall])]).fail_metadata("bad", transient());

    let (topics, elapsed) =
        fetch_topics(storage, MAX_WAIT, &[("bad", &[0, 1]), (TOPIC, &[0])]).await?;

    assert_eq!(FETCH_DEADLINE, elapsed);
    assert_eq!(
        ErrorCode::KafkaStorageError,
        ErrorCode::try_from(topics[0][0].error_code)?
    );

    Ok(())
}

/// A partition whose read fails without records, with an error that a retry may clear, answers
/// `KAFKA_STORAGE_ERROR`, and the records that an earlier partition read stay in the answer.
#[tokio::test(start_paused = true)]
async fn a_read_that_fails_transiently_answers_its_partition_with_kafka_storage_error() -> Result<()>
{
    let storage = Partitions::new([
        (0, vec![Read::Batch(batch(0, &[0])?)]),
        (1, vec![Read::FailTransient]),
    ]);

    let (mut topics, elapsed) = fetch_topics(storage, MAX_WAIT, &[(TOPIC, &[0, 1])]).await?;
    let partitions = topics.remove(0);

    assert_eq!(Duration::ZERO, elapsed);
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert_eq!(1, batches(&partitions[0]));
    assert_eq!(
        ErrorCode::KafkaStorageError,
        ErrorCode::try_from(partitions[1].error_code)?
    );
    assert_eq!(-1, partitions[1].high_watermark);

    Ok(())
}

/// A partition whose offset stage read fails with an error that a retry may clear answers
/// `KAFKA_STORAGE_ERROR` without a read of its records, and the other partitions keep theirs.
#[tokio::test(start_paused = true)]
async fn an_offset_stage_read_that_fails_transiently_answers_kafka_storage_error() -> Result<()> {
    let storage = Partitions::new([(0, vec![Read::Batch(batch(0, &[0])?)]), (1, vec![])])
        .fail_offset_stage_transiently(1);

    let (mut topics, elapsed) =
        fetch_topics(storage.clone(), MAX_WAIT, &[(TOPIC, &[0, 1])]).await?;
    let partitions = topics.remove(0);

    assert_eq!(Duration::ZERO, elapsed);
    assert_eq!(1, batches(&partitions[0]));
    assert_eq!(
        ErrorCode::KafkaStorageError,
        ErrorCode::try_from(partitions[1].error_code)?
    );
    assert_eq!(-1, partitions[1].high_watermark);
    assert!(storage.calls(1).is_empty());

    Ok(())
}

/// The value of each data point of the counter `nisshi_fetch_storage_error`, by its `topic`,
/// `operation` and `error_code` attributes.
fn counted_storage_errors(
    exporter: &InMemoryMetricExporter,
    provider: &SdkMeterProvider,
) -> BTreeMap<(String, String, String), u64> {
    provider.force_flush().expect("flush");

    let attribute = |point: &SumDataPoint<u64>, key: &str| {
        point
            .attributes()
            .find(|attribute| attribute.key.as_str() == key)
            .map(|attribute| attribute.value.to_string())
            .unwrap_or_default()
    };

    exporter
        .get_finished_metrics()
        .expect("finished metrics")
        .iter()
        .flat_map(|resource| resource.scope_metrics())
        .flat_map(|scope| scope.metrics())
        .filter(|metric| metric.name() == "nisshi_fetch_storage_error")
        .filter_map(|metric| match metric.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => Some(sum),
            _ => None,
        })
        .flat_map(|sum| sum.data_points())
        .map(|point| {
            // A fourth attribute could carry a client-chosen value, so the counter must have
            // exactly these three.
            assert_eq!(3, point.attributes().count());
            (
                (
                    attribute(point, "topic"),
                    attribute(point, "operation"),
                    attribute(point, "error_code"),
                ),
                point.value(),
            )
        })
        .collect()
}

/// A series of the counter `nisshi_fetch_storage_error`: its `topic`, `operation` and
/// `error_code` attributes.
fn series(topic: &str, operation: StorageOperation, error_code: &str) -> (String, String, String) {
    (topic.into(), operation.as_str().into(), error_code.into())
}

/// Each partition that a Fetch answers with a storage error counts once, under the topic, the
/// operation that failed and the error code that it answers.
#[tokio::test(start_paused = true)]
async fn a_storage_error_answer_is_counted() -> Result<()> {
    let (exporter, provider) = capture_metrics();

    // Partition 0 of `abc` fails at its offset stage, and partition 1 at its read.
    let storage = Partitions::new([(0, vec![]), (1, vec![Read::FailTransient])])
        .fail_offset_stage_transiently(0)
        .fail_metadata("bad", transient());

    let _ = fetch_topics(storage, MAX_WAIT, &[("bad", &[0, 1, 2]), (TOPIC, &[0, 1])]).await?;

    assert_eq!(
        BTreeMap::from([
            (
                series(
                    "unresolved",
                    StorageOperation::Metadata,
                    "KafkaStorageError"
                ),
                3
            ),
            (
                series(TOPIC, StorageOperation::OffsetStage, "KafkaStorageError"),
                1
            ),
            (
                series(TOPIC, StorageOperation::Fetch, "KafkaStorageError"),
                1
            ),
        ]),
        counted_storage_errors(&exporter, &provider)
    );

    Ok(())
}

/// A metadata read that no retry can clear answers every partition of its topic with
/// `UNKNOWN_SERVER_ERROR`.
#[tokio::test(start_paused = true)]
async fn a_metadata_read_that_fails_permanently_answers_unknown_server_error() -> Result<()> {
    let (exporter, provider) = capture_metrics();

    let storage =
        Partitions::new([]).fail_metadata("bad", Error::Message("the read failed".into()));

    let (topics, _) = fetch_topics(storage, MAX_WAIT, &[("bad", &[0, 1])]).await?;

    for partition in &topics[0] {
        assert_eq!(
            ErrorCode::UnknownServerError,
            ErrorCode::try_from(partition.error_code)?
        );
    }
    assert_eq!(
        BTreeMap::from([(
            series(
                "unresolved",
                StorageOperation::Metadata,
                "UnknownServerError"
            ),
            2
        )]),
        counted_storage_errors(&exporter, &provider)
    );

    Ok(())
}

/// A metadata read that fails with an `Error::Api` answers every partition of its topic with
/// that code.
#[tokio::test(start_paused = true)]
async fn a_metadata_read_that_fails_with_an_api_error_keeps_its_code() -> Result<()> {
    let (exporter, provider) = capture_metrics();
    // A code that is neither of the two that a storage error answers by default.
    let storage = Partitions::new([]).fail_metadata("bad", Error::Api(ErrorCode::RequestTimedOut));

    let (topics, _) = fetch_topics(storage, MAX_WAIT, &[("bad", &[0])]).await?;

    assert_eq!(
        ErrorCode::RequestTimedOut,
        ErrorCode::try_from(topics[0][0].error_code)?
    );
    assert_eq!(
        BTreeMap::from([(
            series("unresolved", StorageOperation::Metadata, "RequestTimedOut"),
            1
        )]),
        counted_storage_errors(&exporter, &provider)
    );

    Ok(())
}

/// A failure that a retry may clear logs at `warn` once in each interval for each operation, so a
/// client that retries a failed Fetch does not flood the log.
#[tokio::test(start_paused = true)]
async fn a_storage_failure_warns_once_in_each_interval_for_each_operation() {
    // These two operations are ones that no Fetch read uses, so no other test shares their state.
    assert!(storage_failure_warns_now(StorageOperation::ListOffsets));
    assert!(!storage_failure_warns_now(StorageOperation::ListOffsets));
    assert!(storage_failure_warns_now(StorageOperation::Brokers));

    tokio::time::advance(STORAGE_FAILURE_WARN_INTERVAL - Duration::from_millis(1)).await;
    assert!(!storage_failure_warns_now(StorageOperation::ListOffsets));

    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(storage_failure_warns_now(StorageOperation::ListOffsets));
}

/// A Fetch that names its topic by ID, as Fetch v13 and later do, has no name. The answer echoes
/// the ID, because a client drops a topic that it cannot match, and the counter does not use the
/// ID as a label, because a client chooses it.
#[tokio::test(start_paused = true)]
async fn a_metadata_read_that_fails_for_a_topic_id_echoes_the_id_and_counts_it_as_unresolved()
-> Result<()> {
    let (exporter, provider) = capture_metrics();

    let id = Uuid::from_bytes([7; 16]);
    let storage = Partitions::new([]).fail_metadata(&id.to_string(), transient());

    let topics = FetchService { storage }
        .fetch(
            MAX_WAIT,
            1,
            1_048_576,
            IsolationLevel::ReadUncommitted,
            &[FetchTopic::default()
                .topic(None)
                .topic_id(Some(*id.as_bytes()))
                .partitions(Some(vec![
                    FetchPartition::default()
                        .partition(0)
                        .partition_max_bytes(1_048_576),
                ]))],
        )
        .await?;

    assert_eq!(None, topics[0].topic);
    assert_eq!(Some(*id.as_bytes()), topics[0].topic_id);
    assert_eq!(
        ErrorCode::KafkaStorageError,
        ErrorCode::try_from(topics[0].partitions.as_ref().expect("partitions")[0].error_code)?
    );
    assert_eq!(
        BTreeMap::from([(
            series(
                "unresolved",
                StorageOperation::Metadata,
                "KafkaStorageError"
            ),
            1
        )]),
        counted_storage_errors(&exporter, &provider)
    );

    Ok(())
}
