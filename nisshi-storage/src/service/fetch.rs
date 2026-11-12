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
    cmp::min,
    collections::HashMap,
    sync::{LazyLock, Mutex, PoisonError},
};

use nisshi_sans_io::{
    ApiKey, ErrorCode, FetchRequest, FetchResponse, IsolationLevel,
    fetch_request::{FetchPartition, FetchTopic},
    fetch_response::{EpochEndOffset, FetchableTopicResponse, PartitionData, SnapshotId},
    metadata_response::MetadataResponseTopic,
    record::deflated::{Batch, Frame},
};
use opentelemetry::{KeyValue, metrics::Counter};
use rama::Service;
use tokio::time::{Duration, Instant, sleep};
use tracing::{debug, error, instrument, warn};

use crate::{
    Error, METER, OffsetStage, Result, Storage, StorageOperation, TopicId, Topition,
    fetched::FetchUntilDeadline as _,
    service::{
        read_deadline::{ReadDeadline, ReadWait, TimedOut},
        read_share::{FetchReadShare, warn_timeout_at_client_share},
        storage_error_code,
    },
};
use validation::ValidatedFetchRequest;

/// Partition reads whose fetch offset falls outside the partition, by
/// `topic` and by `bound`: `below_log_start` or `above_high_watermark`. Each
/// is answered with `OFFSET_OUT_OF_RANGE`, once per Fetch, without waiting
/// for `max_wait`.
static FETCH_OFFSET_OUT_OF_BOUNDS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_fetch_offset_out_of_bounds")
        .with_description(
            "Partition reads whose fetch offset is below the log start or above the high \
             watermark, answered with OFFSET_OUT_OF_RANGE (once per Fetch)",
        )
        .build()
});

/// Partitions that a Fetch answers with an error code because a storage read failed, without
/// waiting for `max_wait`. A failed metadata read counts each partition of its topic. A read
/// that fails after the partition has records answers with them and is not counted.
///
/// The attribute `topic` is the topic name, or [`UNRESOLVED_TOPIC`] when the metadata read
/// failed. The attribute `operation` is the [`StorageOperation`] that failed. The attribute
/// `error_code` is the code of an `Error::Api`. For any other error, it is `KafkaStorageError` if
/// a retry may clear the error, and `UnknownServerError` if not.
static FETCH_STORAGE_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_fetch_storage_error")
        .with_description(
            "Fetch partitions answered with an error code because a storage read failed. A \
             failed metadata read counts each partition of its topic. Attributes: topic (the \
             topic name, or unresolved when the metadata read failed), operation (the storage \
             operation that failed), error_code (the code of an Error::Api; for any other \
             error, KafkaStorageError if a retry may clear it, UnknownServerError if not)",
        )
        .build()
});

/// The `topic` attribute of [`FETCH_STORAGE_ERROR`] for a failed metadata read. A client chooses
/// the topic name or ID of a Fetch, and a value that storage has not confirmed would make a new
/// series of the counter for each name.
const UNRESOLVED_TOPIC: &str = "unresolved";

/// The shortest time between two `warn` lines for failures of one storage operation that a retry
/// may clear.
const STORAGE_FAILURE_WARN_INTERVAL: Duration = Duration::from_secs(10);

/// When each storage operation last logged a `warn` line for a failure that a retry may clear.
static STORAGE_FAILURE_LAST_WARNED: LazyLock<Mutex<HashMap<StorageOperation, Instant>>> =
    LazyLock::new(Mutex::default);

/// Returns whether a failure of `operation` that a retry may clear logs at `warn` now, which it
/// does at most once in each [`STORAGE_FAILURE_WARN_INTERVAL`].
///
/// A client retries a failed Fetch as often as it likes, so a `warn` line for each failure would
/// flood the log while the store is down. The map has one entry for each operation, so its size
/// is bounded.
fn storage_failure_warns_now(operation: StorageOperation) -> bool {
    let now = Instant::now();
    let mut last_warned = STORAGE_FAILURE_LAST_WARNED
        .lock()
        .unwrap_or_else(PoisonError::into_inner);

    if last_warned
        .get(&operation)
        .is_some_and(|last| now.duration_since(*last) < STORAGE_FAILURE_WARN_INTERVAL)
    {
        return false;
    }

    _ = last_warned.insert(operation, now);
    true
}

static FETCH_AHEAD_OF_CACHED_HIGH_WATERMARK: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_fetch_ahead_of_cached_high_watermark")
        .with_description(
            "Partition reads above this broker's cached high watermark that a fresh read of the \
             store found in range, and that the broker then read from the log (once per poll \
             round, whether or not records came back)",
        )
        .build()
});

pub(super) mod validation;

/// Partitions that a Fetch answers with `UNKNOWN_TOPIC_OR_PARTITION` although
/// their topic exists, by `topic` and by `source`: `request` for an index
/// outside the topic's partitions, and `storage` for a partition that the
/// metadata listed and storage then reported unknown. A Fetch counts once
/// per poll round. A `storage` count that does not stop points at metadata
/// and storage that disagree.
static FETCH_UNKNOWN_PARTITION: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_fetch_unknown_partition")
        .with_description(
            "Fetch partitions of an existing topic answered UNKNOWN_TOPIC_OR_PARTITION, by \
             source: request (an index outside the topic) or storage (storage reported the \
             partition unknown after the metadata listed it); counted once per poll round",
        )
        .build()
});

/// How long past the client's `max_wait` a Fetch may spend reading storage.
///
/// It keeps a Fetch under each client's default timeout, so the broker answers before a client
/// gives up and sends the Fetch again. franz-go and librdkafka add `max_wait` to their timeouts of
/// 10s and 60s, so any `max_wait` fits. Java does not add `max_wait` to its 30s timeout, so a Java
/// client with a `max_wait` over 25s gives up before the read ends.
const FETCH_DEADLINE_OVERHEAD: Duration = Duration::from_secs(5);

/// Logs a read of `tp` at `offset` that its deadline stopped. The [`ReadDeadline`] that ran the
/// read has already counted it in the `nisshi_storage_read_deadline_exceeded` counter.
///
/// `operation` is the [`Storage`] method that was read: [`StorageOperation::Fetch`] for
/// the records, [`StorageOperation::OffsetStage`] or [`StorageOperation::CurrentOffsetStage`]
/// for the offsets.
///
/// A read that was never started logs at debug: once a slow backend has
/// spent the read deadline, every partition after it is not started, and a
/// warning for each of them would blame partitions that were never read.
fn log_timed_out(
    tp: &Topition,
    operation: StorageOperation,
    offset: i64,
    timed_out: TimedOut,
    started_at: Instant,
) {
    let stage = timed_out.stage();
    let wait = timed_out.wait();

    match timed_out {
        TimedOut::NotStarted => {
            debug!(
                ?tp,
                %operation,
                offset,
                stage,
                elapsed = ?started_at.elapsed(),
                "fetch read not started"
            )
        }
        // The log names the client, see `warn_timeout_at_client_share`.
        TimedOut::Queued(ReadWait::ClientReadLimit) => {
            warn_timeout_at_client_share(tp, operation, offset, started_at)
        }
        TimedOut::Queued(_) | TimedOut::Reading => {
            warn!(
                ?tp,
                %operation,
                offset,
                stage,
                wait,
                elapsed = ?started_at.elapsed(),
                "fetch read deadline exceeded"
            )
        }
    }
}

/// Returns the partition deadline of the next partition: when its reads must finish, with `left`
/// partitions, this one included, still to read before `fetch_deadline`.
///
/// Each partition may spend half of the time left, and the last all of it,
/// so one partition that never answers delays the rest of the request by
/// that half, rather than holding it until the client gives up. The half is
/// taken of the time left now, so a partition that answers quickly leaves
/// its unused share to those after it.
fn next_partition_deadline(fetch_deadline: Instant, left: usize) -> Instant {
    if left <= 1 {
        fetch_deadline
    } else {
        let now = Instant::now();
        now + fetch_deadline.saturating_duration_since(now) / 2
    }
}

/// Returns the fetch deadline of a Fetch that started at `started_at`: the time by which every
/// storage read of the Fetch must finish.
fn fetch_deadline(started_at: Instant, max_wait: Duration) -> Instant {
    started_at + max_wait + FETCH_DEADLINE_OVERHEAD
}

/// A [`Service`] using its [`Storage`] taking [`ValidatedFetchRequest`] returning [`FetchResponse`].
///
/// Wrap it in a [`FetchValidationLayer`](validation::FetchValidationLayer) to serve a
/// [`FetchRequest`].
/// ```no_run
/// use rama::{Layer as _, Service as _};
/// use nisshi_sans_io::{
///     CreateTopicsRequest, ErrorCode, FetchRequest,
///     create_topics_request::CreatableTopic,
///     fetch_request::{FetchPartition, FetchTopic},
/// };
/// use nisshi_storage::{
///     CreateTopicsService, CreateTopicsValidationLayer, Error, FetchService, FetchValidationLayer, StorageContainer,
/// };
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// const CLUSTER_ID: &str = "nisshi";
/// const NODE_ID: i32 = 111;
/// const HOST: &str = "localhost";
/// const PORT: i32 = 9092;
///
/// let storage = StorageContainer::builder()
///     .cluster_id(CLUSTER_ID)
///     .node_id(NODE_ID)
///     .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
///     storage: storage.clone(),
/// });
///
/// let name = "abcba";
///
/// let response = create_topic
///     .serve(
///         CreateTopicsRequest::default()
///             .topics(Some(vec![
///                 CreatableTopic::default()
///                     .name(name.into())
///                     .num_partitions(5)
///                     .replication_factor(3)
///                     .assignments(Some([].into()))
///                     .configs(Some([].into())),
///             ]))
///             .validate_only(Some(false)),
///     )
///     .await?;
///
/// let topics = response.topics.unwrap_or_default();
/// assert_eq!(1, topics.len());
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);
///
/// let fetch = FetchValidationLayer::new().layer(FetchService {
///     storage: storage.clone(),
/// });
///
/// let partition = 0;
///
/// let response = fetch
///     .serve(
///         FetchRequest::default()
///             .topics(Some(
///                 [FetchTopic::default()
///                     .topic(Some(name.into()))
///                     .partitions(Some(
///                         [FetchPartition::default().partition(partition)].into(),
///                     ))]
///                 .into(),
///             ))
///             .max_bytes(Some(0))
///             .max_wait_ms(5_000),
///     )
///     .await?;
///
/// let topics = response.responses.as_deref().unwrap_or_default();
/// assert_eq!(1, topics.len());
/// let partitions = topics[0].partitions.as_deref().unwrap_or_default();
/// assert_eq!(1, partitions.len());
/// assert_eq!(
///     ErrorCode::None,
///     ErrorCode::try_from(partitions[0].error_code)?
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct FetchService<G> {
    pub storage: G,
}

impl<G> ApiKey for FetchService<G> {
    const KEY: i16 = FetchRequest::KEY;
}

impl<G> FetchService<G>
where
    G: Storage,
{
    /// Reads one partition of a Fetch and returns the partition's answer by its partition deadline.
    /// `left` is the number of partitions that the Fetch still has to read, this one included.
    ///
    /// The read runs until the partition deadline, not until `max_wait` ends, because `max_wait`
    /// limits only how long a Fetch waits for new records ([ReplicaManager.scala#L1523-L1563]). A
    /// read that reaches the partition deadline answers with what it finished.
    ///
    /// A read that fails after the partition has records sets `read_failed`, and the partition
    /// answers with those records. A read that fails before the partition has records answers the
    /// partition with an error code, and the other partitions of the Fetch are still answered.
    ///
    /// [ReplicaManager.scala#L1523-L1563]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1523-L1563
    #[allow(clippy::too_many_arguments)]
    #[instrument(
        skip(self, min_bytes, isolation, fetch_partition, read_failed),
        fields(partition = fetch_partition.partition)
    )]
    async fn fetch_partition(
        &self,
        fetch_deadline: Instant,
        left: usize,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        topic: &str,
        fetch_partition: &FetchPartition,
        read_failed: &mut bool,
    ) -> Result<PartitionData>
    where
        G: Storage,
    {
        let started_at = Instant::now();
        let partition_deadline = next_partition_deadline(fetch_deadline, left);

        let partition_index = fetch_partition.partition;
        let tp = Topition::new(topic, partition_index);

        let fetch_offset = fetch_partition.fetch_offset;

        // The fetch offset comes from the client, and storage may build a key
        // range from it, so the offset is checked against the partition
        // before it reaches storage. The bounds are the same for every
        // isolation level, as in Kafka.
        let offset_stage =
            match ReadDeadline::new(StorageOperation::OffsetStage, partition_deadline)
                .run_until_deadline(self.storage.offset_stage(&tp))
                .await
            {
                Ok(Err(Error::Api(ErrorCode::UnknownTopicOrPartition))) => {
                    return Ok(Self::unknown_to_storage(&tp, StorageOperation::OffsetStage));
                }
                Ok(Ok(offset_stage)) => offset_stage,
                Ok(Err(error)) => {
                    return Ok(Self::storage_failure(
                        &tp,
                        StorageOperation::OffsetStage,
                        fetch_offset,
                        &error,
                    ));
                }
                Err(reason) => {
                    log_timed_out(
                        &tp,
                        StorageOperation::OffsetStage,
                        fetch_offset,
                        reason,
                        started_at,
                    );
                    return Ok(Self::unknown_offsets(partition_index, ErrorCode::None));
                }
            };

        // Below the log start offset the records are gone, so Kafka answers
        // OFFSET_OUT_OF_RANGE, and the client applies `auto.offset.reset`.
        // The answer does not wait for `max_wait`, so a client that keeps
        // fetching this offset does so at round-trip speed: the counter, not
        // a log line per fetch, is what makes it visible.
        if fetch_offset < offset_stage.log_start() {
            return Ok(Self::out_of_bounds(
                topic,
                partition_index,
                fetch_offset,
                &offset_stage,
                "below_log_start",
            ));
        }

        // Above the log end offset, the records are not written yet, and Kafka answers
        // OFFSET_OUT_OF_RANGE as it does below the log start, so a client that seeks past the
        // end, or restores an older log, applies `auto.offset.reset`
        // ([LocalLog.scala#L368-L371]). Kafka's bound is the log end offset, which for a
        // broker without replicas is the high watermark, and it is the same for every
        // isolation level.
        //
        // On dynostore the high watermark that this broker reads can lag a write through another
        // broker by the retention of its metadata cache (5s), so a client can be ahead of it. The
        // answer is then not taken from `offset_stage`: `current_offset_stage` reads the high
        // watermark from the store, and only a fetch offset that is still above it is out of
        // range. The other engines read the high watermark from their database, or hold it in
        // memory, so the second read repeats the first and gives the same answer. A consumer waits at
        // the high watermark, not above it, so this second read is rare.
        //
        // [LocalLog.scala#L368-L371]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/log/LocalLog.scala#L368-L371
        let ahead_of_cached_high_watermark = fetch_offset > offset_stage.high_watermark();

        let mut offset_stage = if ahead_of_cached_high_watermark {
            match ReadDeadline::new(StorageOperation::CurrentOffsetStage, partition_deadline)
                .run_until_deadline(self.storage.current_offset_stage(&tp))
                .await
            {
                Ok(Err(Error::Api(ErrorCode::UnknownTopicOrPartition))) => {
                    return Ok(Self::unknown_to_storage(
                        &tp,
                        StorageOperation::CurrentOffsetStage,
                    ));
                }
                Ok(Ok(current)) => current,
                Ok(Err(error)) => {
                    return Ok(Self::storage_failure(
                        &tp,
                        StorageOperation::CurrentOffsetStage,
                        fetch_offset,
                        &error,
                    ));
                }
                Err(reason) => {
                    log_timed_out(
                        &tp,
                        StorageOperation::CurrentOffsetStage,
                        fetch_offset,
                        reason,
                        started_at,
                    );
                    return Ok(Self::unknown_offsets(partition_index, ErrorCode::None));
                }
            }
        } else {
            offset_stage
        };

        if fetch_offset > offset_stage.high_watermark() {
            return Ok(Self::out_of_bounds(
                topic,
                partition_index,
                fetch_offset,
                &offset_stage,
                "above_high_watermark",
            ));
        }

        // The log can have been deleted from, between the two reads.
        if fetch_offset < offset_stage.log_start() {
            return Ok(Self::out_of_bounds(
                topic,
                partition_index,
                fetch_offset,
                &offset_stage,
                "below_log_start",
            ));
        }

        if ahead_of_cached_high_watermark {
            FETCH_AHEAD_OF_CACHED_HIGH_WATERMARK
                .add(1, &[KeyValue::new("topic", topic.to_owned())]);
        }

        let mut batches = Vec::new();

        let mut offset = fetch_offset;

        loop {
            if *max_bytes == 0 {
                break;
            }

            debug!(offset);

            let read = self
                .storage
                .fetch_until_deadline_or_failure(
                    &tp,
                    offset,
                    min_bytes,
                    *max_bytes,
                    isolation,
                    partition_deadline,
                )
                .await;

            let read = match read {
                Err(Error::Api(ErrorCode::UnknownTopicOrPartition)) => {
                    return Ok(Self::unknown_to_storage(&tp, StorageOperation::Fetch));
                }
                // An earlier read of this partition returned records, so the partition answers
                // with them instead of with the read's error code. The consumer's next Fetch then
                // starts after them.
                Err(error) if !batches.is_empty() => {
                    error!(?tp, offset, ?error, "fetch read failed after earlier reads");
                    *read_failed = true;
                    break;
                }
                // Kafka answers a failed read in the partition's error code, and still answers
                // the other partitions of the Fetch ([`ReplicaManager.readFromLog`]).
                //
                // [`ReplicaManager.readFromLog`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1698-L1727
                Err(error) => {
                    return Ok(Self::storage_failure(
                        &tp,
                        StorageOperation::Fetch,
                        offset,
                        &error,
                    ));
                }
                Ok(read) => read,
            };

            if let Some(error) = &read.failed {
                *read_failed = true;

                // The read failed after its last record, so the next Fetch starts there.
                let failed_at = read
                    .batches
                    .iter()
                    .map(|batch| batch.max_offset() + 1)
                    .max()
                    .unwrap_or(offset);
                let records: u32 = read.batches.iter().map(|batch| batch.record_count).sum();

                error!(
                    ?tp,
                    offset,
                    failed_at,
                    records,
                    ?error,
                    "fetch read failed after it sent records"
                );
            }

            // The reported offsets must bound every record in `batches`. Offsets only grow, so
            // the maximum of each offset bounds the records of every read. The loop does not
            // replace the stage with the read's, because a read can send an older stage than the
            // one that the service read before the loop.
            if let Some(read_offset_stage) = read.offsets {
                offset_stage = OffsetStage {
                    last_stable: offset_stage.last_stable.max(read_offset_stage.last_stable),
                    high_watermark: offset_stage
                        .high_watermark
                        .max(read_offset_stage.high_watermark),
                    log_start: offset_stage.log_start.max(read_offset_stage.log_start),
                };
            }

            if let Some(reason) = read.timed_out {
                // A partition whose reads returned nothing by its deadline makes no progress for
                // its consumer, so it warns. A read cut short with batches still moves the
                // consumer on, so it logs at debug.
                if batches.is_empty() && read.batches.is_empty() {
                    log_timed_out(&tp, StorageOperation::Fetch, offset, reason, started_at);
                } else {
                    debug!(
                        ?tp,
                        offset,
                        elapsed = ?started_at.elapsed(),
                        "fetch read stopped at its deadline"
                    );
                }
            }

            let mut fetched = read.batches;

            debug!(?tp, ?offset, ?fetched);

            *max_bytes =
                u32::try_from(fetched.byte_size()).map(|bytes| max_bytes.saturating_sub(bytes))?;

            if fetched.is_empty() {
                break;
            }

            // the offset after the last one returned; a batch can have gaps
            // (compaction), so this is not the record count
            if let Some(latest) = fetched
                .iter()
                .map(|batch| batch.max_offset() + 1)
                .max()
                .inspect(|latest| debug!(latest))
            {
                offset = latest;
            }

            batches.append(&mut fetched);

            // The loop ends at the partition deadline instead of starting another read, because a
            // read started after the deadline returns no batches and counts as timed out.
            if read.timed_out.is_some() || Instant::now() >= partition_deadline {
                break;
            }

            // The loop ends after a failed read instead of starting another, so that it does not
            // send more reads to storage that is failing.
            if read.failed.is_some() {
                break;
            }
        }

        // Each engine bounds the records that it sends by the offset stage that it sent first, so
        // the stage already covers `offset`, the offset after the last record. The assertion makes
        // an engine that breaks that contract fail its tests. In a release build, the service
        // raises the high watermark, so a client never receives one below its records. It leaves
        // the last stable offset alone, because raising it would tell a `ReadCommitted` client
        // that records of an open transaction are committed.
        if !batches.is_empty() {
            debug_assert!(
                offset_stage.covers(offset, isolation),
                "{tp:?}: a read sent records up to {offset} past the offsets that it sent: \
                 {offset_stage:?}"
            );
            offset_stage = offset_stage.with_high_watermark_at_least(offset);
        }

        Ok(Self::partition_data(partition_index, offset_stage, batches))
            .inspect(|r| debug!(?r, elapsed = ?started_at.elapsed()))
    }

    fn partition_data(
        partition_index: i32,
        offset_stage: OffsetStage,
        batches: Vec<Batch>,
    ) -> PartitionData {
        PartitionData::default()
            .partition_index(partition_index)
            .error_code(ErrorCode::None.into())
            .high_watermark(offset_stage.high_watermark())
            .last_stable_offset(Some(offset_stage.last_stable()))
            .log_start_offset(Some(offset_stage.log_start()))
            .diverging_epoch(None)
            .current_leader(None)
            .snapshot_id(None)
            .aborted_transactions(Some([].into()))
            .preferred_read_replica(Some(-1))
            .records(if batches.is_empty() {
                None
            } else {
                Some(Frame { batches })
            })
    }

    /// Logs and counts a fetch offset beyond `bound` of the log, `below_log_start` or
    /// `above_high_watermark`, and answers it with `OFFSET_OUT_OF_RANGE`.
    fn out_of_bounds(
        topic: &str,
        partition_index: i32,
        fetch_offset: i64,
        offset_stage: &OffsetStage,
        bound: &'static str,
    ) -> PartitionData {
        debug!(
            topic,
            partition = partition_index,
            fetch_offset,
            log_start = offset_stage.log_start(),
            high_watermark = offset_stage.high_watermark(),
            bound,
            "fetch offset is out of range",
        );

        FETCH_OFFSET_OUT_OF_BOUNDS.add(
            1,
            &[
                KeyValue::new("topic", topic.to_owned()),
                KeyValue::new("bound", bound),
            ],
        );

        Self::offset_out_of_range(partition_index)
    }

    /// The partition answer for a fetch offset outside the partition, with
    /// the unknown (-1) offsets that Kafka sends alongside this error.
    fn offset_out_of_range(partition_index: i32) -> PartitionData {
        Self::unknown_offsets(partition_index, ErrorCode::OffsetOutOfRange)
    }

    /// Counts `partitions` partitions of `topic` that answer `error`, which a read of `operation`
    /// returned, and returns the code that they answer.
    fn count_storage_failure(
        topic: &str,
        operation: StorageOperation,
        partitions: u64,
        error: &Error,
    ) -> ErrorCode {
        let error_code = storage_error_code(error);

        FETCH_STORAGE_ERROR.add(
            partitions,
            &[
                KeyValue::new("topic", topic.to_owned()),
                KeyValue::new("operation", operation.as_str()),
                KeyValue::new("error_code", format!("{error_code:?}")),
            ],
        );

        error_code
    }

    /// The answer for a partition whose storage read of `operation` at `offset` failed with
    /// `error`, so the Fetch answers the partition and keeps the connection, and still answers
    /// the others, as Kafka does for a failed log read ([ReplicaManager.scala#L1698-L1727]). It
    /// has no records and unknown (-1) offsets.
    ///
    /// `KAFKA_STORAGE_ERROR` answers a failure that a retry may clear, which a client retries
    /// after a metadata refresh, as many times per second as it likes. That answer counts in
    /// `nisshi_fetch_storage_error`, and logs at `warn` at most once in each
    /// [`STORAGE_FAILURE_WARN_INTERVAL`] for each operation, and at debug after that. A storage
    /// engine may log the cause only at debug, so this `warn` line is the broker's signal at the
    /// default log level. Any other answer is a failure that a retry does not fix, so it logs at
    /// error each time.
    ///
    /// [ReplicaManager.scala#L1698-L1727]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1698-L1727
    fn storage_failure(
        tp: &Topition,
        operation: StorageOperation,
        offset: i64,
        error: &Error,
    ) -> PartitionData {
        let error_code = Self::count_storage_failure(tp.topic(), operation, 1, error);

        if error_code != ErrorCode::KafkaStorageError {
            error!(?tp, %operation, offset, ?error, ?error_code, "fetch storage read failed");
        } else if storage_failure_warns_now(operation) {
            warn!(?tp, %operation, offset, ?error, "fetch storage read failed");
        } else {
            debug!(?tp, %operation, offset, ?error, "fetch storage read failed");
        }

        Self::unknown_offsets(tp.partition(), error_code)
    }

    /// The answer for each partition of `fetch`, when the read of the topic's metadata failed
    /// with `error`: each answers as [`Self::storage_failure`] does, and the other topics of the
    /// Fetch keep their answers. It logs once for the topic, and adds the topic's partition count
    /// to the counter in one call, because a request can list millions of partitions.
    fn metadata_failure(fetch: &FetchTopic, error: &Error) -> FetchableTopicResponse {
        let partitions = fetch.partitions.as_ref().map_or(0, Vec::len);

        let error_code = Self::count_storage_failure(
            UNRESOLVED_TOPIC,
            StorageOperation::Metadata,
            u64::try_from(partitions).unwrap_or(u64::MAX),
            error,
        );

        if error_code != ErrorCode::KafkaStorageError {
            error!(
                topic = ?fetch.topic,
                topic_id = ?fetch.topic_id,
                operation = %StorageOperation::Metadata,
                partitions,
                ?error,
                ?error_code,
                "fetch storage read failed"
            );
        } else if storage_failure_warns_now(StorageOperation::Metadata) {
            warn!(
                topic = ?fetch.topic,
                topic_id = ?fetch.topic_id,
                operation = %StorageOperation::Metadata,
                partitions,
                ?error,
                "fetch storage read failed"
            );
        } else {
            debug!(
                topic = ?fetch.topic,
                topic_id = ?fetch.topic_id,
                operation = %StorageOperation::Metadata,
                partitions,
                ?error,
                "fetch storage read failed"
            );
        }

        FetchableTopicResponse::default()
            .topic(fetch.topic.clone())
            // From v13 the client matches a response topic by id and drops one it cannot match, so
            // the requested id is echoed.
            .topic_id(fetch.topic_id)
            .partitions(fetch.partitions.as_ref().map(|partitions| {
                partitions
                    .iter()
                    .map(|partition| Self::unknown_offsets(partition.partition, error_code))
                    .collect()
            }))
    }

    /// The answer for a partition with `error_code`, no records, and
    /// unknown (-1) offsets.
    ///
    /// With [`ErrorCode::None`] it answers a partition whose offset stage
    /// was not read in time, so nothing of it is known: the client fetches
    /// it again on its next request. Java ignores negative offsets and
    /// keeps the ones it had; librdkafka and franz-go report `-1` as the
    /// partition's watermarks until the next answer that has them.
    fn unknown_offsets(partition_index: i32, error_code: ErrorCode) -> PartitionData {
        PartitionData::default()
            .partition_index(partition_index)
            .error_code(error_code.into())
            .high_watermark(-1)
            .last_stable_offset(Some(-1))
            .log_start_offset(Some(-1))
            .diverging_epoch(None)
            .current_leader(None)
            .snapshot_id(None)
            .aborted_transactions(Some([].into()))
            .preferred_read_replica(Some(-1))
            .records(None)
    }

    fn unknown_topic_response(
        &self,
        fetch: &FetchTopic,
        error_code: ErrorCode,
    ) -> Result<FetchableTopicResponse> {
        Ok(FetchableTopicResponse::default()
            .topic(fetch.topic.clone())
            // From v13 the client matches a response topic by id and drops
            // one it cannot match, so the requested id is echoed: the client
            // then sees the error and refreshes its metadata.
            .topic_id(fetch.topic_id)
            .partitions(fetch.partitions.as_ref().map(|partitions| {
                partitions
                    .iter()
                    .map(|partition| Self::unknown_partition_with(partition.partition, error_code))
                    .collect()
            })))
    }

    /// The answer for a partition that storage reports as unknown after the
    /// request read the topic's metadata, because the topic was deleted, or
    /// recreated with fewer partitions, in between.
    ///
    /// Storage reports it with [`ErrorCode::UnknownTopicOrPartition`] (see
    /// [`Storage::offset_stage`]), and the request answers it in the
    /// response, as for any other unknown partition, instead of closing the
    /// connection. The answer is counted in `nisshi_fetch_unknown_partition`
    /// with `source` `storage`, because a count that does not stop shows
    /// metadata and storage that disagree.
    fn unknown_to_storage(tp: &Topition, operation: StorageOperation) -> PartitionData {
        debug!(
            ?tp,
            %operation,
            "storage reports the partition unknown"
        );

        FETCH_UNKNOWN_PARTITION.add(
            1,
            &[
                KeyValue::new("topic", tp.topic().to_owned()),
                KeyValue::new("source", "storage"),
            ],
        );

        Self::unknown_partition(tp.partition())
    }

    /// The answer for a partition that the topic does not have, and for
    /// each partition of an unknown topic.
    fn unknown_partition(partition_index: i32) -> PartitionData {
        Self::unknown_partition_with(partition_index, ErrorCode::UnknownTopicOrPartition)
    }

    fn unknown_partition_with(partition_index: i32, error_code: ErrorCode) -> PartitionData {
        PartitionData::default()
            .partition_index(partition_index)
            .error_code(error_code.into())
            .high_watermark(0)
            .last_stable_offset(Some(0))
            .log_start_offset(Some(-1))
            .diverging_epoch(Some(EpochEndOffset::default().epoch(-1).end_offset(-1)))
            // Kafka sends a leader hint only with a leadership error;
            // librdkafka acts on one here, and its consumer close then hangs
            // on a deleted topic
            .current_leader(None)
            .snapshot_id(Some(SnapshotId::default().end_offset(-1).epoch(-1)))
            .aborted_transactions(Some([].into()))
            .preferred_read_replica(Some(-1))
            .records(None)
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self, min_bytes, isolation, fetch))]
    async fn fetch_topic(
        &self,
        fetch_deadline: Instant,
        left: &mut usize,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        fetch: &FetchTopic,
        is_first_non_empty: &mut bool,
        read_failed: &mut bool,
    ) -> Result<FetchableTopicResponse> {
        let metadata = match self.storage.metadata(Some(&[fetch.into()])).await {
            Ok(metadata) => metadata,
            Err(error) => {
                *left = left.saturating_sub(fetch.partitions.as_ref().map_or(0, Vec::len));

                return Ok(Self::metadata_failure(fetch, &error));
            }
        };

        if let Some(MetadataResponseTopic {
            topic_id,
            name: Some(name),
            error_code,
            partitions: topic_partitions,
            ..
        }) = metadata.topics().first()
            && *error_code == i16::from(ErrorCode::None)
        {
            // Every engine numbers a topic's partitions from 0, so the count
            // bounds the valid indexes without a scan for each partition.
            let partition_count = topic_partitions.as_deref().map_or(0, <[_]>::len);

            let mut partitions = Vec::new();
            let mut outside_topic = 0;

            for fetch_partition in fetch.partitions.as_ref().unwrap_or(&Vec::new()) {
                // Kafka answers a partition that the topic does not have
                // before it reads any log, so that partition does not end the
                // wait for `min_bytes` ([KafkaApis.scala#L806-L811]). The
                // index must not reach storage, because some engines keep
                // state for each partition that they read.
                //
                // [KafkaApis.scala#L806-L811]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L806-L811
                if !usize::try_from(fetch_partition.partition)
                    .is_ok_and(|index| index < partition_count)
                {
                    *left = left.saturating_sub(1);
                    outside_topic += 1;
                    partitions.push(Self::unknown_partition(fetch_partition.partition));

                    // A request can hold millions of such indexes, and this
                    // branch does not await, so it yields to the runtime once
                    // its budget is spent instead of holding the worker for
                    // the whole request.
                    tokio::task::consume_budget().await;
                    continue;
                }

                let partition_max_bytes = if *is_first_non_empty {
                    *max_bytes
                } else {
                    min(fetch_partition.partition_max_bytes as u32, *max_bytes)
                };

                let mut partition_bytes = partition_max_bytes;
                debug!(partition_bytes, is_first_non_empty);

                let partition = self
                    .fetch_partition(
                        fetch_deadline,
                        *left,
                        min_bytes,
                        &mut partition_bytes,
                        isolation,
                        name,
                        fetch_partition,
                        read_failed,
                    )
                    .await?;

                *left = left.saturating_sub(1);

                *is_first_non_empty = *is_first_non_empty
                    && partition
                        .records
                        .as_ref()
                        .is_some_and(|records| records.batches.is_empty());

                debug!(partition_bytes, is_first_non_empty);

                // The partition spends the bytes that it returned, not only its
                // own limit. A storage read can exceed its limit by one batch
                // (`Storage::fetch`), so a partition can exceed
                // `partition_max_bytes` by one batch. When the response spends
                // that batch as well, the response holds at most `max_bytes` and
                // one batch, however many partitions the request lists. Kafka
                // lets only the first non-empty partition exceed a limit
                // ([ReplicaManager.scala#L1733-L1739]). Storage cannot tell the
                // first read of the first non-empty partition from any other
                // read, so here every partition can.
                //
                // [ReplicaManager.scala#L1733-L1739]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1733-L1739
                *max_bytes = max_bytes.saturating_sub(u32::try_from(partition.byte_size())?);

                partitions.push(partition);
            }

            if outside_topic > 0 {
                debug!(
                    topic = name,
                    outside_topic, partition_count, "fetch of partitions outside the topic"
                );

                FETCH_UNKNOWN_PARTITION.add(
                    outside_topic,
                    &[
                        KeyValue::new("topic", name.to_owned()),
                        KeyValue::new("source", "request"),
                    ],
                );
            }

            Ok(FetchableTopicResponse::default()
                .topic(fetch.topic.to_owned())
                .topic_id(topic_id.to_owned())
                .partitions(Some(partitions)))
        } else {
            // Any metadata error answers the client as an unknown topic, so a
            // storage failure would otherwise leave no trace naming the topic.
            if let Some(MetadataResponseTopic { error_code, .. }) = metadata.topics().first()
                && *error_code != i16::from(ErrorCode::None)
                && *error_code != i16::from(ErrorCode::UnknownTopicOrPartition)
            {
                tracing::warn!(
                    topic = ?fetch.topic,
                    topic_id = ?fetch.topic_id,
                    error_code,
                    error = ?ErrorCode::try_from(*error_code).ok(),
                    "metadata error, answering UNKNOWN_TOPIC_OR_PARTITION"
                );
            }

            *left = left.saturating_sub(fetch.partitions.as_ref().map_or(0, Vec::len));

            // Kafka answers each partition of a topic ID that it cannot resolve to a name with
            // `UNKNOWN_TOPIC_ID`, and each partition of a name that it does not know with
            // `UNKNOWN_TOPIC_OR_PARTITION`. It picks the code in `KafkaApis` from whether the ID
            // resolved, so the choice sits here and not in each storage engine, which answers
            // a miss the same for an ID and a name. Any other code that storage reports is
            // answered `UNKNOWN_TOPIC_OR_PARTITION` and logged above.
            //
            // https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L798-L802
            let error_code = match (
                TopicId::from(fetch),
                metadata.topics().first().map(|topic| topic.error_code),
            ) {
                (TopicId::Id(_), Some(error_code))
                    if error_code == i16::from(ErrorCode::UnknownTopicOrPartition) =>
                {
                    ErrorCode::UnknownTopicId
                }
                _ => ErrorCode::UnknownTopicOrPartition,
            };

            self.unknown_topic_response(fetch, error_code)
        }
    }

    #[instrument(skip(self, isolation, topics))]
    pub(crate) async fn fetch(
        &self,
        max_wait: Duration,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        topics: &[FetchTopic],
    ) -> Result<Vec<FetchableTopicResponse>> {
        debug!(?isolation, ?topics);

        if topics.is_empty() {
            Ok(vec![])
        } else {
            // Instant rather than SystemTime: the deadlines below must not
            // move with the wall clock
            let started_at = Instant::now();
            let fetch_deadline = fetch_deadline(started_at, max_wait);
            let mut responses = vec![];
            let mut iteration = 0;

            // The first round reads before any check of `max_wait`, as in Kafka, so a Fetch with
            // `max_wait` of zero answers with every record that the round found.
            loop {
                responses.clear();

                // Every round reads every partition again from its fetch
                // offset, and answers with only its own records. So each
                // round starts with the whole `max_bytes`, and only its own
                // bytes count toward `min_bytes`. Kafka also counts the bytes
                // available at each check, not a sum over the checks
                // ([DelayedFetch.scala]).
                //
                // [DelayedFetch.scala]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/DelayedFetch.scala#L74-L148
                let mut round_max_bytes = max_bytes;
                let mut is_first_non_empty = true;
                let mut read_failed = false;

                let mut left = topics
                    .iter()
                    .map(|topic| topic.partitions.as_ref().map_or(0, Vec::len))
                    .sum::<usize>();

                let fetch_started_at = Instant::now();
                for fetch in topics.iter() {
                    let fetch_response = self
                        .fetch_topic(
                            fetch_deadline,
                            &mut left,
                            min_bytes,
                            &mut round_max_bytes,
                            isolation,
                            fetch,
                            &mut is_first_non_empty,
                            &mut read_failed,
                        )
                        .await?;

                    responses.push(fetch_response);
                }

                let bytes = u32::try_from(responses.byte_size())?;

                let remaining = max_wait.saturating_sub(started_at.elapsed());

                debug!(?iteration, ?max_wait, ?remaining, ?bytes, ?min_bytes);

                if remaining.is_zero() {
                    break;
                }

                // Kafka answers once the bytes reach `min_bytes`
                // ([DelayedFetch.scala#L147]).
                //
                // [DelayedFetch.scala#L147]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/DelayedFetch.scala#L147
                if bytes >= min_bytes {
                    break;
                }

                if answers_without_waiting(&responses) {
                    debug!(?iteration, "partition error, not waiting for min_bytes");
                    break;
                }

                // A partition whose read failed after it sent records answers with those records
                // and no error code. Kafka answers a failed read with the partition's error code and
                // no records ([`ReplicaManager.readFromLog`]). We keep the records, because each one
                // was complete when the engine sent it, and the consumer's next Fetch starts at the
                // failure and gets its error code. The Fetch answers now instead of reading the
                // failing partition again in another round, as Kafka answers at once when the read
                // of a partition fails ([`ReplicaManager.fetchMessages`]).
                //
                // [`ReplicaManager.readFromLog`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1698-L1727
                // [`ReplicaManager.fetchMessages`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1550-L1558
                if read_failed {
                    debug!(
                        ?iteration,
                        "partition read failed, not waiting for min_bytes"
                    );
                    break;
                }

                {
                    let fetch_elapsed = fetch_started_at.elapsed();

                    // we have some data to return to the client,
                    // we haven't met the minimum size requirement,
                    // but we don't have enough (estimated) time remaining to do another round
                    if !responses.is_empty() && remaining < fetch_elapsed {
                        debug!(responses.len = responses.len(), ?remaining, ?fetch_elapsed);
                        break;
                    }
                }

                sleep(remaining / 2).await;

                iteration += 1;
            }

            Ok(responses)
        }
    }
}

/// Builds a [`FetchableTopicResponse`] reporting `error_code` on every partition `fetch` requested.
pub(super) fn topic_error_response(
    fetch: &FetchTopic,
    error_code: ErrorCode,
) -> Result<FetchableTopicResponse> {
    Ok(FetchableTopicResponse::default()
        .topic(fetch.topic.clone())
        // From v13 the client matches a response topic by id and drops
        // one it cannot match, so the requested id is echoed: the client
        // then sees the error and refreshes its metadata.
        .topic_id(fetch.topic_id)
        .partitions(fetch.partitions.as_ref().map(|partitions| {
            partitions
                .iter()
                .map(|partition| {
                    PartitionData::default()
                        .partition_index(partition.partition)
                        .error_code(error_code.into())
                        .high_watermark(0)
                        .last_stable_offset(Some(0))
                        .log_start_offset(Some(-1))
                        .diverging_epoch(Some(EpochEndOffset::default().epoch(-1).end_offset(-1)))
                        // Kafka sends a leader hint only with a leadership
                        // error; librdkafka acts on one here, and its
                        // consumer close then hangs on a deleted topic
                        .current_leader(None)
                        .snapshot_id(Some(SnapshotId::default().end_offset(-1).epoch(-1)))
                        .aborted_transactions(Some([].into()))
                        .preferred_read_replica(Some(-1))
                        .records(None)
                })
                .collect()
        })))
}

impl<G> Service<ValidatedFetchRequest> for FetchService<G>
where
    G: Storage,
{
    type Output = FetchResponse;
    type Error = Error;

    #[instrument(skip(self, request))]
    async fn serve(&self, request: ValidatedFetchRequest) -> Result<Self::Output, Self::Error> {
        let started_at = Instant::now();

        FetchReadShare::of(&request.extensions)
            .scope(self.fetch(
                request.max_wait,
                request.min_bytes,
                request.max_bytes,
                request.isolation_level,
                &request.topics,
            ))
            .await
            .map(|responses| {
                FetchResponse::default()
                    .throttle_time_ms(Some(0))
                    .error_code(Some(ErrorCode::None.into()))
                    .session_id(Some(0))
                    .node_endpoints(Some([].into()))
                    .responses(Some(responses))
            })
            .inspect(|r| debug!(?r, elapsed = ?started_at.elapsed()))
    }
}

/// Returns true when a fetch answers before `min_bytes` or `max_wait`, as
/// Kafka does: a log read error, such as `OFFSET_OUT_OF_RANGE`, in any
/// partition ([ReplicaManager.scala]), or an error in every partition.
///
/// An unknown topic alone, by name or by ID, does not end the wait while
/// another partition has no error, because Kafka sets unknown topics aside
/// before it reads the log,
/// and waits for the other partitions ([KafkaApis.scala]). A consumer still
/// assigned to a deleted topic then fetches once per `max_wait`, instead of
/// at round trip speed.
///
/// [ReplicaManager.scala]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1550-L1558
/// [KafkaApis.scala]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1031-L1032
fn answers_without_waiting(responses: &[FetchableTopicResponse]) -> bool {
    let unknown_topic = [
        i16::from(ErrorCode::UnknownTopicOrPartition),
        i16::from(ErrorCode::UnknownTopicId),
    ];
    let none = i16::from(ErrorCode::None);

    let mut error_codes = responses
        .iter()
        .flat_map(|topic| topic.partitions.as_deref().unwrap_or_default())
        .map(|partition| partition.error_code)
        .peekable();

    if error_codes.peek().is_none() {
        return false;
    }

    let mut every_partition_has_error = true;

    for error_code in error_codes {
        if error_code == none {
            every_partition_has_error = false;
        } else if !unknown_topic.contains(&error_code) {
            return true;
        }
    }

    every_partition_has_error
}

trait ByteSize {
    fn byte_size(&self) -> u64;
}

impl<T> ByteSize for Vec<T>
where
    T: ByteSize,
{
    fn byte_size(&self) -> u64 {
        self.iter().map(|item| item.byte_size()).sum()
    }
}

impl<T> ByteSize for Option<T>
where
    T: ByteSize,
{
    fn byte_size(&self) -> u64 {
        self.as_ref().map_or(0, |some| some.byte_size())
    }
}

impl ByteSize for Batch {
    fn byte_size(&self) -> u64 {
        self.record_data.len() as u64
    }
}

impl ByteSize for Frame {
    fn byte_size(&self) -> u64 {
        self.batches.byte_size()
    }
}

impl ByteSize for PartitionData {
    fn byte_size(&self) -> u64 {
        self.records.byte_size()
    }
}

impl ByteSize for FetchableTopicResponse {
    fn byte_size(&self) -> u64 {
        self.partitions.byte_size()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        future::pending,
        mem, slice,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use bytes::Bytes;
    use nisshi_sans_io::{
        ErrorCode, FetchRequest, IsolationLevel, RequestInput,
        fetch_request::{FetchPartition, FetchTopic},
        fetch_response::PartitionData,
        metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
        record::{Record, deflated, inflated},
    };
    use opentelemetry::global;
    use opentelemetry_sdk::metrics::{
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        data::{AggregatedMetrics, MetricData},
    };
    use rama::{Layer as _, Service as _, extensions::Extensions};
    use tokio::{
        sync::OwnedSemaphorePermit,
        time::{Duration, Instant},
    };

    use super::{FETCH_DEADLINE_OVERHEAD, FetchService, ReadWait, TimedOut, fetch_deadline};
    use crate::{
        Error, FetchValidationLayer, OffsetStage, Result, StorageOperation, Topition,
        fetched::FetchedSender,
        read_share::{ClientIdentity, FetchReadShare},
        storage_double::storage_double,
    };

    /// A batch at `base_offset` holding one record per entry in `deltas`,
    /// each at that offset delta.
    fn batch(base_offset: i64, deltas: &[i32]) -> Result<deflated::Batch> {
        let mut builder = inflated::Batch::builder()
            .base_offset(base_offset)
            .last_offset_delta(deltas.last().copied().unwrap_or_default());

        for delta in deltas {
            builder = builder.record(
                Record::builder()
                    .offset_delta(*delta)
                    .value(Some(Bytes::from_static(b"foobarfoobarfoobarfoobar"))),
            );
        }

        builder
            .build()
            .and_then(deflated::Batch::try_from)
            .map_err(Into::into)
    }

    /// Storage whose `fetch` replays scripted responses and stops each read at its deadline. It
    /// records each call. Each read of the offset stage raises its last stable offset and high
    /// watermark by `stage_growth`, as writes during a fetch would, so a test can tell which read
    /// answered.
    #[derive(Clone, Debug)]
    struct Scripted {
        responses: Arc<Mutex<Vec<Vec<deflated::Batch>>>>,
        /// The offset that each call to `fetch` read from.
        call_offsets: Arc<Mutex<Vec<i64>>>,
        /// Makes the first call send its batches and then never return, like an engine that spends
        /// its whole deadline assembling one truncated batch.
        first_call_stalls: bool,
        /// Makes the second call fail before it sends anything.
        second_call_fails: bool,
        offset_stage: OffsetStage,
        offset_stage_reads: Arc<AtomicUsize>,
        stage_growth: i64,
        /// Offset stage reads, counted from 0, from which the partition is
        /// unknown to storage.
        unknown_from_stage_read: Option<usize>,
        /// Records reads, counted from 1, from which the partition is
        /// unknown to storage.
        unknown_from_fetch_call: Option<usize>,
        /// The high watermark that `current_offset_stage` reads, where it
        /// differs from the one that `offset_stage` reads, as a cache
        /// behind `offset_stage` does.
        current_high_watermark: Option<i64>,
        /// The log start that `current_offset_stage` reads, where it differs from the
        /// one that `offset_stage` reads, as a delete between the two reads does.
        current_log_start: Option<i64>,
        /// How long `current_offset_stage` takes, on the (paused) clock.
        current_delay: Option<Duration>,
        /// The error that `current_offset_stage` answers.
        current_error: Option<Error>,
        current_offset_stage_reads: Arc<AtomicUsize>,
    }

    impl Scripted {
        fn new(responses: Vec<Vec<deflated::Batch>>, offset_stage: OffsetStage) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses)),
                call_offsets: Arc::new(Mutex::new(vec![])),
                first_call_stalls: false,
                second_call_fails: false,
                offset_stage,
                offset_stage_reads: Arc::new(AtomicUsize::new(0)),
                stage_growth: 0,
                unknown_from_stage_read: None,
                unknown_from_fetch_call: None,
                current_high_watermark: None,
                current_log_start: None,
                current_delay: None,
                current_error: None,
                current_offset_stage_reads: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn current_offset_stage_reads(&self) -> usize {
            self.current_offset_stage_reads.load(Ordering::SeqCst)
        }

        fn call_offsets(&self) -> Vec<i64> {
            self.call_offsets.lock().expect("call offsets").clone()
        }

        fn offset_stage_reads(&self) -> usize {
            self.offset_stage_reads.load(Ordering::SeqCst)
        }
    }

    storage_double!(Scripted {
        async fn fetch(
            &self,
            _topition: &Topition,
            offset: i64,
            _min_bytes: u32,
            _max_bytes: u32,
            _isolation_level: IsolationLevel,
            fetched_sender: FetchedSender,
        ) -> Result<()> {
            let calls = {
                let mut call_offsets = self.call_offsets.lock()?;
                call_offsets.push(offset);
                call_offsets.len()
            };

            if self
                .unknown_from_fetch_call
                .is_some_and(|unknown_from| calls >= unknown_from)
            {
                return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
            }

            if calls == 2 && self.second_call_fails {
                return Err(Error::Message("the read failed".into()));
            }

            let response = {
                let mut responses = self.responses.lock()?;

                if responses.is_empty() {
                    return Err(Error::Message(format!(
                        "storage fetch called {calls} times, past the end of the script"
                    )));
                }

                responses.remove(0)
            };

            // The future holds a sender of its own, so the read keeps the channel open while it
            // waits. A read that closed the channel would end its caller's read at once.
            let reader_stopped = fetched_sender.reader_stopped();

            let offset_stage = covering_stage(self.offset_stage, &response);
            send_batches(fetched_sender, offset_stage, response).await?;

            if calls == 1 && self.first_call_stalls {
                reader_stopped.await;
            }

            Ok(())
        }

        async fn current_offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
            _ = self.current_offset_stage_reads.fetch_add(1, Ordering::SeqCst);

            if let Some(delay) = self.current_delay {
                tokio::time::sleep(delay).await;
            }

            if let Some(error) = &self.current_error {
                return Err(error.clone());
            }

            let high_watermark = self
                .current_high_watermark
                .unwrap_or(self.offset_stage.high_watermark);

            Ok(OffsetStage {
                last_stable: self.offset_stage.last_stable.min(high_watermark),
                high_watermark,
                log_start: self.current_log_start.unwrap_or(self.offset_stage.log_start),
            })
        }

        async fn offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
            let reads = self.offset_stage_reads.fetch_add(1, Ordering::SeqCst);

            if self
                .unknown_from_stage_read
                .is_some_and(|unknown_from| reads >= unknown_from)
            {
                return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
            }

            let growth = self.stage_growth * i64::try_from(reads)?;

            Ok(OffsetStage {
                last_stable: self.offset_stage.last_stable + growth,
                high_watermark: self.offset_stage.high_watermark + growth,
                ..self.offset_stage
            })
        }

        async fn metadata(&self, _topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
            unimplemented!()
        }

    });

    const OFFSET_STAGE: OffsetStage = OffsetStage {
        last_stable: 1_000,
        high_watermark: 1_000,
        log_start: 0,
    };

    async fn fetch_partition_at(
        storage: Scripted,
        max_wait: Duration,
        max_bytes: u32,
        isolation: IsolationLevel,
        fetch_offset: i64,
    ) -> Result<PartitionData> {
        let mut remaining = max_bytes;

        FetchService { storage }
            .fetch_partition(
                fetch_deadline(Instant::now(), max_wait),
                1,
                1,
                &mut remaining,
                isolation,
                "abc",
                &FetchPartition::default()
                    .partition(0)
                    .fetch_offset(fetch_offset)
                    .partition_max_bytes(max_bytes as i32),
                &mut false,
            )
            .await
    }

    async fn fetch_partition(
        storage: Scripted,
        max_wait: Duration,
        max_bytes: u32,
    ) -> Result<Vec<deflated::Batch>> {
        fetch_partition_at(
            storage,
            max_wait,
            max_bytes,
            IsolationLevel::ReadUncommitted,
            0,
        )
        .await
        .map(|partition| {
            partition
                .records
                .map(|frame| frame.batches)
                .unwrap_or_default()
        })
    }

    /// A read of the records that returns at its partition deadline is the partition's last read:
    /// the service does not call storage again for the rest of `max_bytes`.
    #[tokio::test(start_paused = true)]
    async fn stops_at_the_deadline() -> Result<()> {
        let max_wait = Duration::from_millis(100);

        // The test reads one partition, so its partition deadline is the fetch deadline.
        let deadline = fetch_deadline(Instant::now(), max_wait);

        // The first read sends one truncated batch and then runs until the deadline. Each later
        // read would return one record.
        let responses = (0..10_000)
            .map(|offset| batch(offset, &[0]).map(|batch| vec![batch]))
            .collect::<Result<Vec<_>>>()?;

        let storage = Scripted {
            first_call_stalls: true,
            ..Scripted::new(responses, OFFSET_STAGE)
        };

        let batches = fetch_partition(storage.clone(), max_wait, 1024 * 1024).await?;

        assert_eq!(deadline, Instant::now());
        assert_eq!(1, batches.len());
        assert_eq!(0, batches[0].base_offset);
        assert_eq!(
            vec![0],
            storage.call_offsets(),
            "storage was called again after the deadline"
        );

        Ok(())
    }

    /// The next fetch offset follows the last offset in the batch, not its
    /// record count: compaction leaves gaps, and re-reading from inside a
    /// batch that has already been returned duplicates records.
    #[tokio::test(start_paused = true)]
    async fn next_offset_follows_the_last_offset_in_the_batch() -> Result<()> {
        let max_wait = Duration::from_millis(100);

        // records at offsets 0 and 3 (1 and 2 compacted away), then nothing
        let responses = vec![vec![batch(0, &[0, 3])?], vec![]];

        let storage = Scripted::new(responses, OFFSET_STAGE);

        let batches = fetch_partition(storage.clone(), max_wait, 1024 * 1024).await?;

        assert_eq!(1, batches.len());
        assert_eq!(vec![0, 4], storage.call_offsets());

        Ok(())
    }

    /// A read that fails without records, after an earlier read of the partition returned
    /// records, ends the partition's reads, and the partition answers with the earlier records.
    #[tokio::test(start_paused = true)]
    async fn a_read_that_fails_after_an_earlier_read_answers_with_its_records() -> Result<()> {
        // The partition has a record at offset 2, which the failed second read would have
        // returned.
        let responses = vec![vec![batch(0, &[0, 1])?], vec![batch(2, &[0])?]];

        let storage = Scripted {
            second_call_fails: true,
            ..Scripted::new(responses, OFFSET_STAGE)
        };

        let partition = fetch_partition_at(
            storage.clone(),
            Duration::from_millis(100),
            1024 * 1024,
            IsolationLevel::ReadUncommitted,
            0,
        )
        .await?;
        let batches = partition
            .records
            .map(|frame| frame.batches)
            .unwrap_or_default();

        assert_eq!(
            vec![(0, 1)],
            batches
                .iter()
                .map(|batch| (batch.base_offset, batch.max_offset()))
                .collect::<Vec<_>>()
        );
        assert_eq!(OFFSET_STAGE.high_watermark, partition.high_watermark);
        assert_eq!(vec![0, 2], storage.call_offsets());

        Ok(())
    }

    /// A partition whose log starts at 100, whose last stable offset is
    /// 500, and whose high watermark is 1,000.
    fn bounded() -> Scripted {
        Scripted::new(
            vec![],
            OffsetStage {
                last_stable: 500,
                high_watermark: 1_000,
                log_start: 100,
            },
        )
    }

    /// A fetch offset below the log start is answered with
    /// `OFFSET_OUT_OF_RANGE` and never reaches storage, which may build an
    /// invalid key range from it (a negative offset, as a big-endian key).
    #[tokio::test(start_paused = true)]
    async fn below_log_start_is_out_of_range() -> Result<()> {
        for fetch_offset in [i64::MIN, -5, -1, 0, 99] {
            let storage = bounded();

            let partition = fetch_partition_at(
                storage.clone(),
                Duration::from_millis(100),
                1024,
                IsolationLevel::ReadUncommitted,
                fetch_offset,
            )
            .await?;

            assert_eq!(
                ErrorCode::OffsetOutOfRange,
                ErrorCode::try_from(partition.error_code)?,
                "fetch at {fetch_offset}"
            );
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
            assert!(
                storage.call_offsets().is_empty(),
                "fetch at {fetch_offset} reached storage: {:?}",
                storage.call_offsets()
            );
        }

        Ok(())
    }

    /// A fetch offset above the high watermark is answered with
    /// `OFFSET_OUT_OF_RANGE`, as Kafka does above the log end offset, and
    /// never reaches storage, which may build an invalid key range from it
    /// (`i64::MAX`, as a big-endian key). The high watermark is read again
    /// from the source of truth first, and the answer follows that read.
    #[tokio::test(start_paused = true)]
    async fn above_high_watermark_is_out_of_range_without_storage() -> Result<()> {
        for fetch_offset in [1_001, i64::MAX] {
            let storage = bounded();

            let partition = fetch_partition_at(
                storage.clone(),
                Duration::from_millis(100),
                1024,
                IsolationLevel::ReadUncommitted,
                fetch_offset,
            )
            .await?;

            assert_eq!(
                ErrorCode::OffsetOutOfRange,
                ErrorCode::try_from(partition.error_code)?,
                "fetch at {fetch_offset}"
            );
            assert_eq!(-1, partition.high_watermark, "fetch at {fetch_offset}");
            assert!(partition.records.is_none(), "fetch at {fetch_offset}");
            assert_eq!(
                1,
                storage.current_offset_stage_reads(),
                "fetch at {fetch_offset}"
            );
            assert!(
                storage.call_offsets().is_empty(),
                "fetch at {fetch_offset} reached storage: {:?}",
                storage.call_offsets()
            );
        }

        Ok(())
    }

    /// Makes the fetch counters report to the returned provider. The counters are statics that
    /// read the global meter provider when a test first adds to them, and each test runs in its
    /// own process, so the provider is in place before that first count.
    fn capture_metrics() -> (InMemoryMetricExporter, SdkMeterProvider) {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();

        global::set_meter_provider(provider.clone());

        (exporter, provider)
    }

    /// Sums the counter `name` over all its attributes, which is 0 for a counter that no test
    /// step has added to.
    fn counted(exporter: &InMemoryMetricExporter, provider: &SdkMeterProvider, name: &str) -> u64 {
        provider.force_flush().expect("flush");

        exporter
            .get_finished_metrics()
            .expect("finished metrics")
            .iter()
            .flat_map(|resource| resource.scope_metrics())
            .flat_map(|scope| scope.metrics())
            .filter(|metric| metric.name() == name)
            .filter_map(|metric| match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => Some(sum),
                _ => None,
            })
            .flat_map(|sum| sum.data_points())
            .map(|point| point.value())
            .sum()
    }

    /// A high watermark that `offset_stage` reads can lag a write through
    /// another broker. A fetch offset above it but within the high watermark
    /// that the source of truth holds is read, not rejected, and the response
    /// carries the current high watermark.
    #[tokio::test(start_paused = true)]
    async fn above_a_stale_high_watermark_reads_the_log() -> Result<()> {
        let (exporter, provider) = capture_metrics();

        let mut storage = bounded();
        storage.current_high_watermark = Some(1_100);
        storage.responses.lock()?.push(vec![]);

        let partition = fetch_partition_at(
            storage.clone(),
            Duration::from_millis(100),
            1024,
            IsolationLevel::ReadUncommitted,
            1_050,
        )
        .await?;

        assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
        assert_eq!(1_100, partition.high_watermark);
        assert_eq!(vec![1_050], storage.call_offsets());
        assert_eq!(1, storage.current_offset_stage_reads());

        // beyond the current high watermark as well
        let mut storage = bounded();
        storage.current_high_watermark = Some(1_100);

        let partition = fetch_partition_at(
            storage.clone(),
            Duration::from_millis(100),
            1024,
            IsolationLevel::ReadUncommitted,
            1_101,
        )
        .await?;

        assert_eq!(
            ErrorCode::OffsetOutOfRange,
            ErrorCode::try_from(partition.error_code)?
        );
        assert!(storage.call_offsets().is_empty());

        // only the fetch that was served counts: the second one is rejected
        assert_eq!(
            1,
            counted(
                &exporter,
                &provider,
                "nisshi_fetch_ahead_of_cached_high_watermark"
            )
        );

        Ok(())
    }

    /// A second read of the offset stage that does not finish in time is not an answer, so it
    /// must not reset the consumer: the partition is answered with `NONE` and unknown offsets,
    /// and the client fetches it again.
    #[tokio::test(start_paused = true)]
    async fn a_second_offset_stage_read_that_times_out_does_not_reset_the_consumer() -> Result<()> {
        let mut storage = bounded();
        storage.current_delay = Some(Duration::from_secs(10));

        let partition = fetch_partition_at(
            storage.clone(),
            Duration::from_millis(100),
            1024,
            IsolationLevel::ReadUncommitted,
            1_050,
        )
        .await?;

        assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
        assert_eq!(-1, partition.high_watermark);
        assert_eq!(Some(-1), partition.log_start_offset);
        assert!(partition.records.is_none());
        assert_eq!(1, storage.current_offset_stage_reads());
        assert!(storage.call_offsets().is_empty());

        Ok(())
    }

    /// A partition that storage reports unknown on the second read of the offset stage is
    /// answered with `UNKNOWN_TOPIC_OR_PARTITION` in its own partition.
    #[tokio::test(start_paused = true)]
    async fn a_second_offset_stage_read_of_an_unknown_partition_answers_it_unknown() -> Result<()> {
        let mut storage = bounded();
        storage.current_error = Some(Error::Api(ErrorCode::UnknownTopicOrPartition));

        let partition = fetch_partition_at(
            storage.clone(),
            Duration::from_millis(100),
            1024,
            IsolationLevel::ReadUncommitted,
            1_050,
        )
        .await?;

        assert_eq!(
            ErrorCode::UnknownTopicOrPartition,
            ErrorCode::try_from(partition.error_code)?
        );
        assert_eq!(1, storage.current_offset_stage_reads());
        assert!(storage.call_offsets().is_empty());

        Ok(())
    }

    /// A delete that moves the log start past the fetch offset between the two reads of the
    /// offset stage makes the offset out of range, though it is within the high watermark.
    #[tokio::test(start_paused = true)]
    async fn a_log_start_that_moves_past_the_fetch_offset_between_the_reads_is_out_of_range()
    -> Result<()> {
        let (exporter, provider) = capture_metrics();

        let mut storage = bounded();
        storage.current_high_watermark = Some(1_200);
        storage.current_log_start = Some(1_100);

        let partition = fetch_partition_at(
            storage.clone(),
            Duration::from_millis(100),
            1024,
            IsolationLevel::ReadUncommitted,
            1_050,
        )
        .await?;

        assert_eq!(
            ErrorCode::OffsetOutOfRange,
            ErrorCode::try_from(partition.error_code)?
        );
        assert!(storage.call_offsets().is_empty());

        // This stops the test from passing because no counter was captured.
        assert_eq!(
            1,
            counted(&exporter, &provider, "nisshi_fetch_offset_out_of_bounds")
        );
        assert_eq!(
            0,
            counted(
                &exporter,
                &provider,
                "nisshi_fetch_ahead_of_cached_high_watermark"
            )
        );

        Ok(())
    }

    /// A fetch offset in range does not read the offset stage a second
    /// time: a consumer waits at the high watermark.
    #[tokio::test(start_paused = true)]
    async fn in_range_offsets_read_the_offset_stage_once() -> Result<()> {
        for fetch_offset in [100, 1_000] {
            let storage = bounded();
            storage.responses.lock()?.push(vec![]);

            _ = fetch_partition_at(
                storage.clone(),
                Duration::from_millis(100),
                1024,
                IsolationLevel::ReadUncommitted,
                fetch_offset,
            )
            .await?;

            assert_eq!(0, storage.current_offset_stage_reads());
        }

        Ok(())
    }

    /// The log start offset and the high watermark are both valid fetch
    /// offsets: the first is the oldest record, the second the next one to
    /// be written, where a caught-up consumer waits.
    #[tokio::test(start_paused = true)]
    async fn boundary_offsets_reach_storage() -> Result<()> {
        for fetch_offset in [100, 1_000] {
            let storage = bounded();
            storage.responses.lock()?.push(vec![]);

            let partition = fetch_partition_at(
                storage.clone(),
                Duration::from_millis(100),
                1024,
                IsolationLevel::ReadUncommitted,
                fetch_offset,
            )
            .await?;

            assert_eq!(
                ErrorCode::None,
                ErrorCode::try_from(partition.error_code)?,
                "fetch at {fetch_offset}"
            );
            assert_eq!(1_000, partition.high_watermark);
            assert_eq!(Some(100), partition.log_start_offset);
            assert_eq!(vec![fetch_offset], storage.call_offsets());
        }

        Ok(())
    }

    /// The upper bound is the high watermark for every isolation level: a
    /// `ReadCommitted` fetch between the last stable offset and the high
    /// watermark reaches storage, which bounds it by the last stable offset.
    #[tokio::test(start_paused = true)]
    async fn read_committed_above_last_stable_reaches_storage() -> Result<()> {
        for fetch_offset in [700, 1_000] {
            let storage = bounded();
            storage.responses.lock()?.push(vec![]);

            let partition = fetch_partition_at(
                storage.clone(),
                Duration::from_millis(100),
                1024,
                IsolationLevel::ReadCommitted,
                fetch_offset,
            )
            .await?;

            assert_eq!(
                ErrorCode::None,
                ErrorCode::try_from(partition.error_code)?,
                "fetch at {fetch_offset}"
            );
            assert_eq!(vec![fetch_offset], storage.call_offsets());
        }

        Ok(())
    }

    /// A fetch response never contains a record past the high watermark that it reports, even
    /// when a producer writes after the service first reads the offset stage. The service takes
    /// the stage from the read of the records, so it reads the offset stage only once.
    #[tokio::test(start_paused = true)]
    async fn reported_bounds_cover_returned_records() -> Result<()> {
        struct Case {
            name: &'static str,
            responses: Vec<Vec<deflated::Batch>>,
            stage: OffsetStage,
            isolation: IsolationLevel,
            fetch_offset: i64,
        }

        let cases = [
            Case {
                name: "idle",
                responses: vec![vec![]],
                stage: OFFSET_STAGE,
                isolation: IsolationLevel::ReadUncommitted,
                fetch_offset: 1_000,
            },
            Case {
                name: "catching up",
                responses: vec![vec![batch(0, &[0])?], vec![]],
                stage: OFFSET_STAGE,
                isolation: IsolationLevel::ReadUncommitted,
                fetch_offset: 0,
            },
            // records up to offset 3, past a high watermark of 1
            Case {
                name: "past the high watermark",
                responses: vec![vec![batch(0, &[0, 3])?], vec![]],
                stage: OffsetStage {
                    last_stable: 1,
                    high_watermark: 1,
                    log_start: 0,
                },
                isolation: IsolationLevel::ReadUncommitted,
                fetch_offset: 0,
            },
            // the last record is at 999, so the next offset is the high
            // watermark, which covers it
            Case {
                name: "ending at the high watermark",
                responses: vec![vec![batch(996, &[0, 3])?], vec![]],
                stage: OFFSET_STAGE,
                isolation: IsolationLevel::ReadUncommitted,
                fetch_offset: 996,
            },
            // the last record is at 1,000, one past what the high watermark
            // covers
            Case {
                name: "ending one past the high watermark",
                responses: vec![vec![batch(997, &[0, 3])?], vec![]],
                stage: OFFSET_STAGE,
                isolation: IsolationLevel::ReadUncommitted,
                fetch_offset: 997,
            },
            // under ReadCommitted, a transaction commits during the fetch, so
            // the records end past the last stable offset that the service
            // read first, though they end below the high watermark
            Case {
                name: "committed during the fetch",
                responses: vec![vec![batch(0, &[0, 3])?], vec![]],
                stage: OffsetStage {
                    last_stable: 1,
                    high_watermark: 1_000,
                    log_start: 0,
                },
                isolation: IsolationLevel::ReadCommitted,
                fetch_offset: 0,
            },
            // under ReadCommitted, a fetch between the last stable offset
            // and the high watermark returns nothing, and the first read
            // answers it
            Case {
                name: "ReadCommitted above the last stable offset",
                responses: vec![vec![]],
                stage: OffsetStage {
                    last_stable: 500,
                    high_watermark: 1_000,
                    log_start: 0,
                },
                isolation: IsolationLevel::ReadCommitted,
                fetch_offset: 700,
            },
        ];

        for case in cases {
            let storage = Scripted::new(case.responses, case.stage);

            let partition = fetch_partition_at(
                storage.clone(),
                Duration::from_millis(100),
                1024,
                case.isolation,
                case.fetch_offset,
            )
            .await?;

            assert_eq!(1, storage.offset_stage_reads(), "{}", case.name);

            // the offset after the last record that the response returns
            let next_offset = partition
                .records
                .iter()
                .flat_map(|frame| &frame.batches)
                .map(|batch| batch.max_offset() + 1)
                .max();

            if let Some(next_offset) = next_offset {
                let reported_bound = match case.isolation {
                    IsolationLevel::ReadCommitted => partition.last_stable_offset,
                    IsolationLevel::ReadUncommitted => Some(partition.high_watermark),
                };

                assert!(
                    reported_bound.is_some_and(|reported_bound| next_offset <= reported_bound),
                    "{}: records end at {next_offset}, but the response reports a bound of \
                     {reported_bound:?}",
                    case.name
                );
            }
        }

        Ok(())
    }

    /// The offset stage of every partition of [`Partitions`].
    const PARTITION_STAGE: OffsetStage = OffsetStage {
        last_stable: 1_000,
        high_watermark: 1_000,
        log_start: 0,
    };

    /// Returns `offset_stage` raised to cover `batches`, as an engine reads it when a producer
    /// wrote the batches after the service read its own stage.
    fn covering_stage(offset_stage: OffsetStage, batches: &[deflated::Batch]) -> OffsetStage {
        let next = batches
            .iter()
            .map(|batch| batch.max_offset() + 1)
            .max()
            .unwrap_or_default();

        OffsetStage {
            last_stable: offset_stage.last_stable.max(next),
            high_watermark: offset_stage.high_watermark.max(next),
            log_start: offset_stage.log_start,
        }
    }

    /// Sends `offset_stage`, and then each of `batches`, through `fetched_sender`.
    async fn send_batches(
        fetched_sender: FetchedSender,
        offset_stage: OffsetStage,
        batches: Vec<deflated::Batch>,
    ) -> Result<()> {
        let mut data = fetched_sender.send_offsets(offset_stage).await?;

        for batch in batches {
            data.send_batch(batch).await?;
        }

        Ok(())
    }

    /// What one storage read of a partition does, in [`Partitions`].
    #[derive(Clone, Debug)]
    enum Read {
        /// Returns this batch.
        Batch(deflated::Batch),
        /// Returns nothing.
        Empty,
        /// Never returns, so its caller drops it at its partition deadline.
        Stall,
        /// Sends this batch, and then never returns, so its caller drops it at its partition
        /// deadline.
        SendThenStall(deflated::Batch),
        /// Sends this batch, and then fails.
        SendThenFail(deflated::Batch),
        /// Fails before it sends anything.
        Fail,
        /// Fails before it sends anything, with an error that a retry may clear.
        FailTransient,
    }

    /// One storage read of a partition, as [`Partitions`] saw it.
    #[derive(Clone, Debug, PartialEq)]
    struct Call {
        topic: String,
        partition: i32,
        max_bytes: u32,
    }

    /// When one storage read of a partition ended, as [`Partitions`] saw it: when it returned, or
    /// when its caller dropped it.
    #[derive(Clone, Debug)]
    struct ReadEnd {
        topic: String,
        partition: i32,
        ended_at: Instant,
    }

    /// Records the end of one storage read of a partition when it is dropped, so that a read that
    /// its caller drops at its deadline records that time.
    struct ReadEndRecorder {
        read_ends: Arc<Mutex<Vec<ReadEnd>>>,
        topic: String,
        partition: i32,
    }

    impl Drop for ReadEndRecorder {
        fn drop(&mut self) {
            self.read_ends.lock().expect("read ends").push(ReadEnd {
                topic: mem::take(&mut self.topic),
                partition: self.partition,
                ended_at: Instant::now(),
            });
        }
    }

    /// The reads of each partition, by topic and partition.
    type Scripts = BTreeMap<(String, i32), Vec<Read>>;

    /// How long an offset stage read takes to answer, by topic, partition
    /// and the position of the read among that partition's stage reads,
    /// counted from 1.
    type StageDelays = BTreeMap<(String, i32, usize), Duration>;

    /// Storage whose topics each answer their partitions' reads in turn
    /// from their own script, with nothing once it runs out. A topic that
    /// has no script is unknown.
    #[derive(Clone, Debug, Default)]
    struct Partitions {
        scripts: Arc<Mutex<Scripts>>,
        /// A stage read not listed answers at once.
        offset_stage_delays: Arc<Mutex<StageDelays>>,
        offset_stage_reads: Arc<Mutex<BTreeMap<(String, i32), usize>>>,
        /// Partitions of [`TOPIC`] that the metadata lists, and whose
        /// offset stage read answers [`ErrorCode::UnknownTopicOrPartition`].
        deleted: Arc<Mutex<BTreeSet<i32>>>,
        /// Partitions of [`TOPIC`] whose offset stage read fails with an error that has no Kafka
        /// error code.
        failing_offset_stages: Arc<Mutex<BTreeSet<i32>>>,
        /// Partitions of [`TOPIC`] whose offset stage read fails with an error that a retry may
        /// clear.
        transient_offset_stages: Arc<Mutex<BTreeSet<i32>>>,
        /// Topics whose metadata read fails, with the error that it fails with.
        failing_metadata: Arc<Mutex<BTreeMap<String, Error>>>,
        calls: Arc<Mutex<Vec<Call>>>,
        read_ends: Arc<Mutex<Vec<ReadEnd>>>,
        /// Whether each offset stage read takes a permit from its read
        /// share and keeps it in `kept_read_shares`, as an abandoned read
        /// keeps its permit until its statement ends.
        keeps_read_share: bool,
        kept_read_shares: Arc<Mutex<Vec<OwnedSemaphorePermit>>>,
    }

    impl Partitions {
        /// Storage with one topic, [`TOPIC`].
        fn new(scripts: impl IntoIterator<Item = (i32, Vec<Read>)>) -> Self {
            Self::topics(
                scripts
                    .into_iter()
                    .map(|(partition, script)| (TOPIC, partition, script)),
            )
        }

        fn topics<'a>(scripts: impl IntoIterator<Item = (&'a str, i32, Vec<Read>)>) -> Self {
            Self {
                scripts: Arc::new(Mutex::new(
                    scripts
                        .into_iter()
                        .map(|(topic, partition, script)| ((topic.into(), partition), script))
                        .collect(),
                )),
                ..Default::default()
            }
        }

        /// Makes the `read`th offset stage read of `partition` of
        /// [`TOPIC`], counted from 1, never answer.
        fn stall_offset_stage(self, partition: i32, read: usize) -> Self {
            self.delay_offset_stage(partition, read, Duration::from_secs(3_600))
        }

        /// Makes the `read`th offset stage read of `partition` of
        /// [`TOPIC`], counted from 1, answer after `delay`.
        fn delay_offset_stage(self, partition: i32, read: usize, delay: Duration) -> Self {
            _ = self
                .offset_stage_delays
                .lock()
                .expect("offset stage delays")
                .insert((TOPIC.into(), partition, read), delay);
            self
        }

        /// Makes `partition` of [`TOPIC`] look deleted after the request
        /// read the metadata.
        fn delete_after_metadata(self, partition: i32) -> Self {
            _ = self.deleted.lock().expect("deleted").insert(partition);
            self
        }

        /// Makes each offset stage read of `partition` of [`TOPIC`] fail.
        fn fail_offset_stage(self, partition: i32) -> Self {
            _ = self
                .failing_offset_stages
                .lock()
                .expect("failing offset stages")
                .insert(partition);
            self
        }

        /// Makes each offset stage read of `partition` of [`TOPIC`] fail with an error that a
        /// retry may clear.
        fn fail_offset_stage_transiently(self, partition: i32) -> Self {
            _ = self
                .transient_offset_stages
                .lock()
                .expect("transient offset stages")
                .insert(partition);
            self
        }

        /// Makes each metadata read of `topic` fail with `error`.
        fn fail_metadata(self, topic: &str, error: Error) -> Self {
            _ = self
                .failing_metadata
                .lock()
                .expect("failing metadata")
                .insert(topic.into(), error);
            self
        }

        fn calls(&self, partition: i32) -> Vec<Call> {
            self.calls
                .lock()
                .expect("calls")
                .iter()
                .filter(|call| call.topic == TOPIC && call.partition == partition)
                .cloned()
                .collect()
        }

        /// Returns when each storage read of `partition` of [`TOPIC`] ended, in the order in which
        /// the reads ended.
        fn read_ends(&self, partition: i32) -> Vec<Instant> {
            self.read_ends
                .lock()
                .expect("read ends")
                .iter()
                .filter(|end| end.topic == TOPIC && end.partition == partition)
                .map(|end| end.ended_at)
                .collect()
        }

        /// Makes each offset stage read take a permit from a read share of
        /// one permit, and keep it.
        fn keep_read_share(self) -> Self {
            Self {
                keeps_read_share: true,
                ..self
            }
        }

        /// How many permits offset stage reads took from their read share.
        fn kept_read_shares(&self) -> usize {
            self.kept_read_shares
                .lock()
                .expect("kept read shares")
                .len()
        }

        /// How many times the offset stage of `partition` of [`TOPIC`] was
        /// read.
        fn offset_stage_reads(&self, partition: i32) -> usize {
            self.offset_stage_reads
                .lock()
                .expect("offset stage reads")
                .get(&(TOPIC.into(), partition))
                .copied()
                .unwrap_or_default()
        }
    }

    storage_double!(Partitions {
        async fn fetch(
            &self,
            topition: &Topition,
            _offset: i64,
            _min_bytes: u32,
            max_bytes: u32,
            _isolation_level: IsolationLevel,
            fetched_sender: FetchedSender,
        ) -> Result<()> {
            self.calls.lock()?.push(Call {
                topic: topition.topic().into(),
                partition: topition.partition(),
                max_bytes,
            });

            let _read_end = ReadEndRecorder {
                read_ends: self.read_ends.clone(),
                topic: topition.topic().into(),
                partition: topition.partition(),
            };

            let read = {
                let mut scripts = self.scripts.lock()?;
                let script = scripts
                    .entry((topition.topic().into(), topition.partition()))
                    .or_default();

                if script.is_empty() {
                    Read::Empty
                } else {
                    script.remove(0)
                }
            };

            match read {
                Read::Batch(batch) => {
                    let batches = vec![batch];
                    let offset_stage = covering_stage(PARTITION_STAGE, &batches);
                    send_batches(fetched_sender, offset_stage, batches).await
                }
                Read::Empty => Ok(()),
                Read::Stall => pending().await,
                Read::SendThenStall(batch) => {
                    let mut data = fetched_sender
                        .send_offsets(covering_stage(PARTITION_STAGE, slice::from_ref(&batch)))
                        .await?;
                    data.send_batch(batch).await?;

                    // The read keeps its sender while it stalls. A read that dropped its sender
                    // would send nothing more, so its caller would stop waiting at once.
                    pending::<()>().await;
                    drop(data);
                    Ok(())
                }
                Read::SendThenFail(batch) => {
                    let mut data = fetched_sender
                        .send_offsets(covering_stage(PARTITION_STAGE, slice::from_ref(&batch)))
                        .await?;
                    data.send_batch(batch).await?;
                    Err(Error::Message("the read failed".into()))
                }
                Read::Fail => Err(Error::Message("the read failed".into())),
                Read::FailTransient => Err(Error::from(std::io::Error::other("the read failed"))),
            }
        }

        async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
            if self.keeps_read_share {
                let permit = FetchReadShare::acquire(1)
                    .await?
                    .ok_or_else(|| Error::Message("the read ran outside a read share".into()))?;
                self.kept_read_shares.lock()?.push(permit);
            }

            let key = (topition.topic().to_owned(), topition.partition());

            let read = {
                let mut reads = self.offset_stage_reads.lock()?;
                let read = reads.entry(key.clone()).or_default();
                *read += 1;
                *read
            };

            let delay = self
                .offset_stage_delays
                .lock()?
                .get(&(key.0, key.1, read))
                .copied();

            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }

            if topition.topic() == TOPIC && self.deleted.lock()?.contains(&topition.partition()) {
                return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
            }

            if topition.topic() == TOPIC
                && self
                    .failing_offset_stages
                    .lock()?
                    .contains(&topition.partition())
            {
                return Err(Error::Message("the offset stage read failed".into()));
            }

            if topition.topic() == TOPIC
                && self
                    .transient_offset_stages
                    .lock()?
                    .contains(&topition.partition())
            {
                return Err(Error::from(std::io::Error::other(
                    "the offset stage read failed",
                )));
            }

            Ok(PARTITION_STAGE)
        }

        /// Lists each scripted topic with partitions up to its highest
        /// scripted index.
        async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
            let failing_metadata = self.failing_metadata.lock()?;

            if let Some(error) = topics.unwrap_or_default().iter().find_map(|topic| {
                failing_metadata.get(&match topic {
                    TopicId::Name(name) => name.clone(),
                    TopicId::Id(id) => id.to_string(),
                })
            }) {
                return Err(error.clone());
            }

            drop(failing_metadata);

            let known = {
                let scripts = self.scripts.lock()?;

                topics
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|topic| match topic {
                        TopicId::Name(name) => Some(name.clone()),
                        TopicId::Id(_) => None,
                    })
                    .filter_map(|name| {
                        scripts
                            .keys()
                            .filter(|(topic, _)| *topic == name)
                            .map(|(_, partition)| *partition)
                            .max()
                            .map(|highest| (name, highest))
                    })
                    .collect::<Vec<_>>()
            };

            Ok(MetadataResponse {
                cluster: None,
                controller: None,
                brokers: vec![],
                topics: known
                    .into_iter()
                    .map(|(name, highest)| {
                        MetadataResponseTopic::default()
                            .error_code(ErrorCode::None.into())
                            .name(Some(name))
                            .topic_id(Some([1; 16]))
                            .partitions(Some(
                                (0..=highest)
                                    .map(|partition| {
                                        MetadataResponsePartition::default()
                                            .error_code(ErrorCode::None.into())
                                            .partition_index(partition)
                                    })
                                    .collect(),
                            ))
                    })
                    .collect(),
            })
        }
    });

    mod storage_error;

    const TOPIC: &str = "abc";
    const MAX_WAIT: Duration = Duration::from_millis(500);
    const MAX_BYTES: i32 = 1024 * 1024;

    /// Fetches `partitions` of [`TOPIC`] from offset 0 with [`MAX_WAIT`],
    /// returning each partition's answer and how long the request took.
    async fn fetch(
        storage: Partitions,
        partitions: &[i32],
    ) -> Result<(Vec<PartitionData>, Duration)> {
        let (mut topics, elapsed) = fetch_topics(storage, MAX_WAIT, &[(TOPIC, partitions)]).await?;
        assert_eq!(1, topics.len());

        Ok((topics.remove(0), elapsed))
    }

    /// Fetches the partitions of each topic from offset 0 with `max_wait`,
    /// returning each topic's partition answers and how long the request
    /// took.
    async fn fetch_topics(
        storage: Partitions,
        max_wait: Duration,
        topics: &[(&str, &[i32])],
    ) -> Result<(Vec<Vec<PartitionData>>, Duration)> {
        fetch_isolated(storage, max_wait, IsolationLevel::ReadUncommitted, topics).await
    }

    /// As [`fetch_topics`], with the given isolation level.
    async fn fetch_isolated(
        storage: Partitions,
        max_wait: Duration,
        isolation: IsolationLevel,
        topics: &[(&str, &[i32])],
    ) -> Result<(Vec<Vec<PartitionData>>, Duration)> {
        fetch_on_connection(storage, max_wait, isolation, topics, Extensions::default()).await
    }

    /// As [`fetch_isolated`], on the connection that `extensions` belongs
    /// to.
    async fn fetch_on_connection(
        storage: Partitions,
        max_wait: Duration,
        isolation: IsolationLevel,
        topics: &[(&str, &[i32])],
        extensions: Extensions,
    ) -> Result<(Vec<Vec<PartitionData>>, Duration)> {
        fetch_with_min_bytes(storage, max_wait, 1, isolation, topics, extensions).await
    }

    /// As [`fetch_on_connection`], with the given `min_bytes`.
    async fn fetch_with_min_bytes(
        storage: Partitions,
        max_wait: Duration,
        min_bytes: i32,
        isolation: IsolationLevel,
        topics: &[(&str, &[i32])],
        extensions: Extensions,
    ) -> Result<(Vec<Vec<PartitionData>>, Duration)> {
        let started_at = Instant::now();

        let request = FetchRequest::default()
            .max_wait_ms(i32::try_from(max_wait.as_millis())?)
            .isolation_level(Some(isolation.into()))
            .min_bytes(min_bytes)
            .max_bytes(Some(MAX_BYTES))
            .topics(Some(
                topics
                    .iter()
                    .map(|(topic, partitions)| {
                        FetchTopic::default()
                            .topic(Some((*topic).into()))
                            .partitions(Some(
                                partitions
                                    .iter()
                                    .map(|partition| {
                                        FetchPartition::default()
                                            .partition(*partition)
                                            .fetch_offset(0)
                                            .partition_max_bytes(MAX_BYTES)
                                    })
                                    .collect(),
                            ))
                    })
                    .collect(),
            ));

        let response = FetchValidationLayer::new()
            .layer(FetchService { storage })
            .serve(RequestInput {
                request,
                extensions,
            })
            .await?;

        let elapsed = started_at.elapsed();

        Ok((
            response
                .responses
                .unwrap_or_default()
                .into_iter()
                .map(|topic| topic.partitions.unwrap_or_default())
                .collect(),
            elapsed,
        ))
    }

    /// The log lines written at `warn` or above.
    #[derive(Clone, Default)]
    struct WarnLog(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for WarnLog {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("log").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Returns what `log_timed_out` logs at `warn` or above for a read of a request from `identity`.
    async fn warning_for(identity: ClientIdentity, stage: TimedOut) -> String {
        let log = WarnLog::default();
        let writer = log.clone();

        identity
            .scope(async {
                tracing::subscriber::with_default(
                    tracing_subscriber::fmt()
                        .with_max_level(tracing::Level::WARN)
                        .with_ansi(false)
                        .with_writer(move || writer.clone())
                        .finish(),
                    || {
                        super::log_timed_out(
                            &Topition::new("orders", 0),
                            StorageOperation::Fetch,
                            0,
                            stage,
                            Instant::now(),
                        );
                    },
                );
            })
            .await;

        String::from_utf8(log.0.lock().expect("log").clone()).expect("utf-8")
    }

    /// A read that timed out waiting for its client's share names the client in the log, and not
    /// in the metric, so that an operator can tell a client that abandons reads from a consumer
    /// group that shares one `client_id`. A timeout at another stage does not blame the client.
    #[tokio::test]
    async fn a_timeout_at_the_client_share_names_the_client() {
        let identity = ClientIdentity::new(Some("alice"), Some("reporting"));

        let log = warning_for(
            identity.clone(),
            TimedOut::Queued(ReadWait::ClientReadLimit),
        )
        .await;
        assert!(log.contains("reporting"), "{log:?}");
        assert!(log.contains("alice"), "{log:?}");

        let log = warning_for(identity, TimedOut::Queued(ReadWait::ConnectionReadLimit)).await;
        assert!(!log.contains("reporting"), "{log:?}");
        assert!(log.contains("fetch read deadline exceeded"), "{log:?}");
    }

    /// A Fetch's reads take their permits from the read share of the request's connection. A
    /// permit that one request's read keeps then holds back the reads of the connection's
    /// next request, which answers no records and no error by its fetch deadline, and not
    /// those of another connection.
    #[tokio::test(start_paused = true)]
    async fn requests_on_a_connection_share_its_read_share() -> Result<()> {
        let storage = Partitions::new([(0, vec![Read::Batch(batch(0, &[0])?)])]).keep_read_share();
        let listener = Extensions::default();
        let connection = listener.fork();

        let fetch = |extensions: Extensions| {
            fetch_on_connection(
                storage.clone(),
                MAX_WAIT,
                IsolationLevel::ReadUncommitted,
                &[(TOPIC, &[0])],
                extensions,
            )
        };

        let (topics, _) = fetch(connection.clone()).await?;
        assert_eq!(1, batches(&topics[0][0]));
        assert_eq!(1, storage.kept_read_shares());

        let (topics, elapsed) = fetch(connection).await?;
        assert_eq!(0, batches(&topics[0][0]));
        assert_eq!(i16::from(ErrorCode::None), topics[0][0].error_code);
        assert!(elapsed <= FETCH_DEADLINE, "{elapsed:?}");
        assert_eq!(1, storage.kept_read_shares());

        _ = fetch(listener.fork()).await?;
        assert_eq!(2, storage.kept_read_shares());

        Ok(())
    }

    fn batches(partition: &PartitionData) -> usize {
        partition
            .records
            .as_ref()
            .map_or(0, |records| records.batches.len())
    }

    /// The fetch deadline of a request with [`MAX_WAIT`].
    const FETCH_DEADLINE: Duration = MAX_WAIT.saturating_add(FETCH_DEADLINE_OVERHEAD);

    /// A partition that never answers holds the request for half of the fetch deadline, and the
    /// partitions after it then each get a partition deadline of their own in the time left: with
    /// none, engines read nothing or one record.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_partition_does_not_starve_the_rest() -> Result<()> {
        // Partitions 1 and 3 send their batch and then run until their partition deadline, so the
        // test can read their partition deadlines from when they ended.
        let storage = Partitions::new([
            (0, vec![Read::Stall]),
            (1, vec![Read::SendThenStall(batch(0, &[0])?)]),
            (2, vec![Read::Batch(batch(0, &[0])?)]),
            (3, vec![Read::SendThenStall(batch(0, &[0])?)]),
        ]);

        let started_at = Instant::now();
        let (partitions, elapsed) = fetch(storage.clone(), &[0, 1, 2, 3]).await?;

        assert_eq!(FETCH_DEADLINE, elapsed);

        assert_eq!(0, partitions[0].partition_index);
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(0, batches(&partitions[0]));

        // the stage read before the records answers the stalled partition
        assert_eq!(1_000, partitions[0].high_watermark);
        assert_eq!(Some(1_000), partitions[0].last_stable_offset);
        assert_eq!(Some(0), partitions[0].log_start_offset);

        for partition in &partitions[1..] {
            assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
            assert_eq!(
                1,
                batches(partition),
                "partition {}",
                partition.partition_index
            );
            assert_eq!(1_000, partition.high_watermark);
        }

        // Each partition's read ran until its partition deadline: half of the time left for each
        // partition before the last, and all of it for the last. Partition 2 returned at once, so
        // partition 3 had all of the quarter that partition 1 left.
        for (partition, ended_at) in [
            (0, FETCH_DEADLINE / 2),
            (1, FETCH_DEADLINE * 3 / 4),
            (3, FETCH_DEADLINE),
        ] {
            assert_eq!(
                vec![started_at + ended_at],
                storage.read_ends(partition),
                "partition {partition}"
            );
        }

        Ok(())
    }

    /// However many partitions never answer, the request is answered by its fetch deadline.
    #[tokio::test(start_paused = true)]
    async fn stalled_partitions_end_at_the_fetch_deadline() -> Result<()> {
        let storage = Partitions::new([
            (0, vec![Read::Stall]),
            (1, vec![Read::Stall]),
            (2, vec![Read::Stall]),
        ]);

        let (partitions, elapsed) = fetch(storage.clone(), &[0, 1, 2]).await?;

        assert_eq!(FETCH_DEADLINE, elapsed);
        assert_eq!(3, partitions.len());

        for partition in &partitions {
            assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
            assert_eq!(0, batches(partition));

            // max_wait has passed, so no round follows the first
            assert_eq!(1, storage.calls(partition.partition_index).len());
        }

        Ok(())
    }

    /// A stalled read uses none of `max_bytes`: the reads after it get `max_bytes` minus only the
    /// batch that the partition returned before the stall.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_read_uses_none_of_max_bytes() -> Result<()> {
        let first = batch(0, &[0])?;
        let size = u32::try_from(first.record_data.len())?;

        let storage = Partitions::new([
            (0, vec![Read::Batch(first), Read::Stall]),
            (1, vec![Read::Batch(batch(0, &[0])?)]),
        ]);

        let (partitions, _) = fetch(storage.clone(), &[0, 1]).await?;

        assert_eq!(1, batches(&partitions[0]));
        assert_eq!(1, batches(&partitions[1]));

        let max_bytes = u32::try_from(MAX_BYTES)?;
        assert_eq!(max_bytes - size, storage.calls(0)[1].max_bytes);
        assert_eq!(max_bytes - size, storage.calls(1)[0].max_bytes);

        Ok(())
    }

    /// Every round of the long poll shares the fetch deadline among all of the partitions
    /// again: a count of partitions left over from the round before would give the first
    /// partition the whole deadline.
    #[tokio::test(start_paused = true)]
    async fn every_round_shares_the_fetch_deadline() -> Result<()> {
        let storage = Partitions::new([
            (0, vec![Read::Empty, Read::Stall]),
            (1, vec![Read::Empty, Read::Batch(batch(0, &[0])?)]),
        ]);

        let (partitions, elapsed) = fetch(storage, &[0, 1]).await?;

        // The first round finds nothing, and the long poll waits half of
        // max_wait before the second
        let second_round = MAX_WAIT / 2;
        assert_eq!(second_round + (FETCH_DEADLINE - second_round) / 2, elapsed);

        assert_eq!(0, batches(&partitions[0]));
        assert_eq!(1, batches(&partitions[1]));

        Ok(())
    }

    /// Every round of the long poll reads the partitions again as the first
    /// round does: the first partition with the whole `max_bytes`, and the
    /// others with their `partition_max_bytes`. Only the round's own bytes
    /// count toward `min_bytes`. Each round reads the same records again, so
    /// `max_bytes` spent by the rounds before would leave the last round with
    /// nothing to return, and a sum over the rounds would meet `min_bytes`
    /// with the same records counted more than once.
    #[tokio::test(start_paused = true)]
    async fn every_round_reads_as_the_first() -> Result<()> {
        const PARTITION_MAX_BYTES: i32 = 1024;

        let record_batch = batch(0, &[0])?;
        let round_bytes = 2 * u32::try_from(record_batch.record_data.len())?;

        // Each round reads a batch from each partition, then reads nothing
        // from the next offset. The script holds more rounds than the long
        // poll can take within `MAX_WAIT`.
        let script = (0..64)
            .flat_map(|_| [Read::Batch(record_batch.clone()), Read::Empty])
            .collect::<Vec<_>>();
        let storage = Partitions::new([(0, script.clone()), (1, script)]);

        let responses = FetchService {
            storage: storage.clone(),
        }
        .fetch(
            MAX_WAIT,
            round_bytes + 1,
            u32::try_from(MAX_BYTES)?,
            IsolationLevel::ReadUncommitted,
            &[FetchTopic::default()
                .topic(Some(TOPIC.into()))
                .partitions(Some(
                    (0..2)
                        .map(|partition| {
                            FetchPartition::default()
                                .partition(partition)
                                .fetch_offset(0)
                                .partition_max_bytes(PARTITION_MAX_BYTES)
                        })
                        .collect(),
                ))],
        )
        .await?;

        for (partition, round_max_bytes) in [(0, MAX_BYTES), (1, PARTITION_MAX_BYTES)] {
            let calls = storage.calls(partition);
            assert!(
                calls.len() > 4,
                "the long poll stopped after two rounds: {calls:?}"
            );
            assert!(
                calls
                    .iter()
                    .step_by(2)
                    .all(|call| i64::from(call.max_bytes) == i64::from(round_max_bytes)),
                "a round read partition {partition} with other than {round_max_bytes} bytes: \
                 {calls:?}"
            );
        }

        let partitions = responses
            .into_iter()
            .flat_map(|topic| topic.partitions.unwrap_or_default())
            .collect::<Vec<_>>();
        assert_eq!(2, partitions.len());
        assert_eq!(1, batches(&partitions[0]));
        assert_eq!(1, batches(&partitions[1]));

        Ok(())
    }

    /// A Fetch with `max_wait` of zero reads its partitions before it answers, as Kafka
    /// does, and each read may run until its partition deadline, whatever `max_wait` is.
    #[tokio::test(start_paused = true)]
    async fn zero_max_wait_reads_until_its_partition_deadline() -> Result<()> {
        let storage = Partitions::new([(0, vec![Read::SendThenStall(batch(0, &[0])?)])]);

        let started_at = Instant::now();
        let (mut topics, elapsed) =
            fetch_topics(storage.clone(), Duration::ZERO, &[(TOPIC, &[0])]).await?;
        let partitions = topics.remove(0);

        assert_eq!(FETCH_DEADLINE_OVERHEAD, elapsed);
        assert_eq!(1, batches(&partitions[0]));
        assert_eq!(1_000, partitions[0].high_watermark);

        assert_eq!(
            vec![started_at + FETCH_DEADLINE_OVERHEAD],
            storage.read_ends(0)
        );

        Ok(())
    }

    /// A Fetch with `max_wait` of zero answers a partition without records with
    /// its offsets at once.
    #[tokio::test(start_paused = true)]
    async fn zero_max_wait_answers_an_empty_partition() -> Result<()> {
        let storage = Partitions::new([(0, vec![])]);

        let (mut topics, elapsed) = fetch_topics(storage, Duration::ZERO, &[(TOPIC, &[0])]).await?;
        let partitions = topics.remove(0);

        assert_eq!(Duration::ZERO, elapsed);
        assert_eq!(1, partitions.len());
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(0, batches(&partitions[0]));
        assert_eq!(1_000, partitions[0].high_watermark);

        Ok(())
    }

    /// A Fetch whose partitions hold exactly `min_bytes` answers at once, as Kafka does, and so
    /// does one with `min_bytes` of zero and no records.
    #[tokio::test(start_paused = true)]
    async fn exactly_min_bytes_answers_at_once() -> Result<()> {
        let record_batch = batch(0, &[0])?;
        let size = i32::try_from(record_batch.record_data.len())?;

        for (min_bytes, script) in [(size, vec![Read::Batch(record_batch)]), (0, vec![])] {
            let (_, elapsed) = fetch_with_min_bytes(
                Partitions::new([(0, script)]),
                MAX_WAIT,
                min_bytes,
                IsolationLevel::ReadUncommitted,
                &[(TOPIC, &[0])],
                Extensions::default(),
            )
            .await?;

            assert_eq!(Duration::ZERO, elapsed, "min_bytes {min_bytes}");
        }

        Ok(())
    }

    /// A Fetch whose partition read fails after it sent records answers at once with those records,
    /// below `min_bytes`, and does not read the partition again.
    #[tokio::test(start_paused = true)]
    async fn a_read_that_fails_after_it_sent_records_answers_below_min_bytes() -> Result<()> {
        let storage = Partitions::new([(0, vec![Read::SendThenFail(batch(0, &[0])?)])]);

        let (mut topics, elapsed) = fetch_with_min_bytes(
            storage.clone(),
            MAX_WAIT,
            i32::MAX,
            IsolationLevel::ReadUncommitted,
            &[(TOPIC, &[0])],
            Extensions::default(),
        )
        .await?;
        let partitions = topics.remove(0);

        assert_eq!(Duration::ZERO, elapsed);
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(1, batches(&partitions[0]));
        assert_eq!(PARTITION_STAGE.high_watermark, partitions[0].high_watermark);
        assert_eq!(1, storage.calls(0).len());

        Ok(())
    }

    /// A Fetch whose partition read fails without records, after an earlier read of the partition
    /// returned records, answers at once with those records, below `min_bytes`.
    #[tokio::test(start_paused = true)]
    async fn a_read_that_fails_after_an_earlier_read_answers_below_min_bytes() -> Result<()> {
        let storage = Partitions::new([(0, vec![Read::Batch(batch(0, &[0])?), Read::Fail])]);

        let (mut topics, elapsed) = fetch_with_min_bytes(
            storage.clone(),
            MAX_WAIT,
            i32::MAX,
            IsolationLevel::ReadUncommitted,
            &[(TOPIC, &[0])],
            Extensions::default(),
        )
        .await?;
        let partitions = topics.remove(0);

        assert_eq!(Duration::ZERO, elapsed);
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(1, batches(&partitions[0]));
        assert_eq!(2, storage.calls(0).len());

        Ok(())
    }

    /// A partition whose read fails without records answers with an error code, and the records
    /// that an earlier partition kept from its own failed read stay in the answer.
    #[tokio::test(start_paused = true)]
    async fn a_read_that_fails_without_records_answers_its_partition_with_an_error_code()
    -> Result<()> {
        let storage = Partitions::new([
            (0, vec![Read::SendThenFail(batch(0, &[0])?)]),
            (1, vec![Read::Fail]),
        ]);

        let (mut topics, elapsed) = fetch_with_min_bytes(
            storage.clone(),
            MAX_WAIT,
            i32::MAX,
            IsolationLevel::ReadUncommitted,
            &[(TOPIC, &[0, 1])],
            Extensions::default(),
        )
        .await?;
        let partitions = topics.remove(0);

        assert_eq!(Duration::ZERO, elapsed);
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(1, batches(&partitions[0]));
        // The read fails with a permanent error, which Kafka also answers with
        // `UNKNOWN_SERVER_ERROR`.
        assert_eq!(
            ErrorCode::UnknownServerError,
            ErrorCode::try_from(partitions[1].error_code)?
        );
        assert_eq!(-1, partitions[1].high_watermark);

        Ok(())
    }

    /// A partition whose offset stage read fails answers with an error code, without a read of
    /// its records, and the other partitions of the Fetch keep their records.
    #[tokio::test(start_paused = true)]
    async fn an_offset_stage_read_that_fails_answers_its_partition_with_an_error_code() -> Result<()>
    {
        let storage = Partitions::new([(0, vec![Read::Batch(batch(0, &[0])?)]), (1, vec![])])
            .fail_offset_stage(1);

        let (mut topics, elapsed) = fetch_with_min_bytes(
            storage.clone(),
            MAX_WAIT,
            i32::MAX,
            IsolationLevel::ReadUncommitted,
            &[(TOPIC, &[0, 1])],
            Extensions::default(),
        )
        .await?;
        let partitions = topics.remove(0);

        assert_eq!(Duration::ZERO, elapsed);
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(1, batches(&partitions[0]));
        // The read fails with a permanent error, which Kafka also answers with
        // `UNKNOWN_SERVER_ERROR`.
        assert_eq!(
            ErrorCode::UnknownServerError,
            ErrorCode::try_from(partitions[1].error_code)?
        );
        assert_eq!(-1, partitions[1].high_watermark);
        assert!(storage.calls(1).is_empty());

        Ok(())
    }

    /// A read that runs until its partition deadline keeps what it assembled, and the partitions
    /// after it are still read. With `max_wait` of 5s, the first of two partitions has 5s until
    /// its partition deadline.
    #[tokio::test(start_paused = true)]
    async fn a_read_that_stops_at_its_partition_deadline_keeps_what_it_read() -> Result<()> {
        let max_wait = Duration::from_secs(5);
        let time_left = (max_wait + FETCH_DEADLINE_OVERHEAD) / 2;

        let storage = Partitions::new([
            (0, vec![Read::SendThenStall(batch(0, &[0])?)]),
            (1, vec![Read::Batch(batch(0, &[0])?)]),
        ]);

        let started_at = Instant::now();
        let (mut topics, elapsed) =
            fetch_topics(storage.clone(), max_wait, &[(TOPIC, &[0, 1])]).await?;
        let partitions = topics.remove(0);

        assert_eq!(time_left, elapsed);
        assert_eq!(vec![started_at + time_left], storage.read_ends(0));

        assert_eq!(1, batches(&partitions[0]));
        assert_eq!(1_000, partitions[0].high_watermark);
        assert_eq!(1, batches(&partitions[1]));

        Ok(())
    }

    /// A `max_wait` whose fetch deadline halves into whole milliseconds seven times,
    /// since tokio's timers have millisecond resolution: 5504ms, so the 32nd of it is
    /// 172ms and the 128th is 43ms.
    const HALVING_MAX_WAIT: Duration = Duration::from_millis(504);

    /// A read that never returns is dropped at its partition deadline, so the partitions after it
    /// are still read, each with its own deadline.
    #[tokio::test(start_paused = true)]
    async fn a_read_that_never_returns_leaves_time_for_the_later_partitions() -> Result<()> {
        let storage = Partitions::new([
            (0, vec![Read::Stall]),
            (1, vec![Read::Batch(batch(0, &[0])?)]),
            (2, vec![Read::Batch(batch(0, &[0])?)]),
        ]);

        let (partitions, elapsed) = fetch(storage.clone(), &[0, 1, 2]).await?;

        assert!(elapsed < FETCH_DEADLINE, "{elapsed:?}");
        assert_eq!(0, batches(&partitions[0]));

        for partition in [1, 2] {
            assert_eq!(
                1,
                batches(&partitions[usize::try_from(partition)?]),
                "partition {partition}"
            );
        }

        Ok(())
    }

    /// A partition with little time until its partition deadline still keeps what its read
    /// assembled by that deadline. After five stalls the last partition has a 32nd of the fetch
    /// deadline, under 200ms.
    #[tokio::test(start_paused = true)]
    async fn a_short_partition_deadline_keeps_what_its_read_assembled() -> Result<()> {
        let fetch_deadline = HALVING_MAX_WAIT + FETCH_DEADLINE_OVERHEAD;

        let storage = Partitions::new([
            (0, vec![Read::Stall]),
            (1, vec![Read::Stall]),
            (2, vec![Read::Stall]),
            (3, vec![Read::Stall]),
            (4, vec![Read::Stall]),
            (5, vec![Read::SendThenStall(batch(0, &[0])?)]),
        ]);

        let started_at = Instant::now();
        let (mut topics, elapsed) = fetch_topics(
            storage.clone(),
            HALVING_MAX_WAIT,
            &[(TOPIC, &[0, 1, 2, 3, 4, 5])],
        )
        .await?;
        let partitions = topics.remove(0);

        assert_eq!(fetch_deadline, elapsed);
        assert_eq!(vec![started_at + fetch_deadline], storage.read_ends(5));
        assert_eq!(1, batches(&partitions[5]));
        assert_eq!(1_000, partitions[5].high_watermark);

        Ok(())
    }

    /// A partition with a few milliseconds until its partition deadline is still read. After seven
    /// stalls the last partition has a 128th of the fetch deadline, under 50ms.
    #[tokio::test(start_paused = true)]
    async fn a_partition_with_little_time_left_is_still_read() -> Result<()> {
        let fetch_deadline = HALVING_MAX_WAIT + FETCH_DEADLINE_OVERHEAD;
        let time_left = fetch_deadline / 128;

        let storage = Partitions::new([
            (0, vec![Read::Stall]),
            (1, vec![Read::Stall]),
            (2, vec![Read::Stall]),
            (3, vec![Read::Stall]),
            (4, vec![Read::Stall]),
            (5, vec![Read::Stall]),
            (6, vec![Read::Stall]),
            (7, vec![Read::Batch(batch(0, &[0])?)]),
        ]);

        let (mut topics, elapsed) = fetch_topics(
            storage.clone(),
            HALVING_MAX_WAIT,
            &[(TOPIC, &[0, 1, 2, 3, 4, 5, 6, 7])],
        )
        .await?;
        let partitions = topics.remove(0);

        assert_eq!(fetch_deadline - time_left, elapsed);
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[7].error_code)?
        );
        assert_eq!(1, batches(&partitions[7]));
        assert_eq!(1_000, partitions[7].high_watermark);

        Ok(())
    }

    /// The read of the records has the partition deadline that was set before the offset stage was
    /// read, so a slow read of the offset stage leaves the read of the records less time.
    #[tokio::test(start_paused = true)]
    async fn the_records_read_shares_its_partition_deadline_with_the_offset_stage() -> Result<()> {
        let stage_read = MAX_WAIT - MAX_WAIT / 4;

        let storage = Partitions::new([
            (0, vec![Read::SendThenStall(batch(0, &[0])?)]),
            (1, vec![Read::Batch(batch(0, &[0])?)]),
        ])
        .delay_offset_stage(0, 1, stage_read);

        let started_at = Instant::now();
        let (partitions, _) = fetch(storage.clone(), &[0, 1]).await?;

        assert_eq!(vec![started_at + FETCH_DEADLINE / 2], storage.read_ends(0));
        assert_eq!(1, batches(&partitions[0]));
        assert_eq!(1, batches(&partitions[1]));

        Ok(())
    }

    /// A partition whose offset stage never answers before its records are read is answered with no
    /// records and unknown offsets at its partition deadline, its records are never read, and the
    /// partitions after it get a partition deadline of their own.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_offset_stage_before_the_read_answers_unknown_offsets() -> Result<()> {
        let storage = Partitions::new([
            (0, vec![Read::Batch(batch(0, &[0])?)]),
            (1, vec![Read::Batch(batch(0, &[0])?)]),
        ])
        .stall_offset_stage(0, 1);

        let (partitions, elapsed) = fetch(storage.clone(), &[0, 1]).await?;

        assert_eq!(FETCH_DEADLINE / 2, elapsed);

        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(0, batches(&partitions[0]));
        assert_eq!(-1, partitions[0].high_watermark);
        assert_eq!(Some(-1), partitions[0].last_stable_offset);
        assert_eq!(Some(-1), partitions[0].log_start_offset);
        assert!(storage.calls(0).is_empty(), "{:?}", storage.calls(0));

        assert_eq!(1, batches(&partitions[1]));
        assert_eq!(1_000, partitions[1].high_watermark);

        Ok(())
    }

    /// The partitions of every topic share one fetch deadline: a stall in the only partition of the
    /// first topic takes half of it, not all of it, so the next topic is still read.
    #[tokio::test(start_paused = true)]
    async fn topics_share_the_fetch_deadline() -> Result<()> {
        let storage = Partitions::topics([
            (TOPIC, 0, vec![Read::Stall]),
            ("def", 0, vec![Read::Batch(batch(0, &[0])?)]),
        ]);

        let (topics, elapsed) =
            fetch_topics(storage, MAX_WAIT, &[(TOPIC, &[0]), ("def", &[0])]).await?;

        assert_eq!(FETCH_DEADLINE / 2, elapsed);
        assert_eq!(0, batches(&topics[0][0]));
        assert_eq!(1, batches(&topics[1][0]));

        Ok(())
    }

    /// The partitions of an unknown topic count as read, so the last partition of the request gets
    /// all of the fetch deadline left.
    #[tokio::test(start_paused = true)]
    async fn an_unknown_topic_counts_as_read() -> Result<()> {
        let storage = Partitions::new([(0, vec![Read::Stall])]);

        let (topics, elapsed) =
            fetch_topics(storage, MAX_WAIT, &[("unknown", &[0]), (TOPIC, &[0])]).await?;

        assert_eq!(FETCH_DEADLINE, elapsed);
        assert_eq!(
            ErrorCode::UnknownTopicOrPartition,
            ErrorCode::try_from(topics[0][0].error_code)?
        );
        assert_eq!(0, batches(&topics[1][0]));

        Ok(())
    }

    /// A partition that storage reports as unknown at any of its reads
    /// answers `UNKNOWN_TOPIC_OR_PARTITION` in the response, and the read
    /// does not fail.
    #[tokio::test(start_paused = true)]
    async fn storage_unknown_partition_at_each_read_answers_in_band() -> Result<()> {
        let stage = OffsetStage {
            last_stable: 1,
            high_watermark: 1,
            log_start: 0,
        };

        let cases = [
            (
                "first offset stage read",
                Scripted {
                    unknown_from_stage_read: Some(0),
                    ..Scripted::new(vec![], stage)
                },
            ),
            (
                "records read",
                Scripted {
                    unknown_from_fetch_call: Some(1),
                    ..Scripted::new(vec![], stage)
                },
            ),
        ];

        for (name, storage) in cases {
            let partition = fetch_partition_at(
                storage,
                Duration::from_secs(1),
                1024 * 1024,
                IsolationLevel::ReadUncommitted,
                0,
            )
            .await?;

            assert_eq!(
                ErrorCode::UnknownTopicOrPartition,
                ErrorCode::try_from(partition.error_code)?,
                "{name}"
            );
            assert_eq!(None, partition.records, "{name}");
        }

        Ok(())
    }

    /// A partition index outside the topic answers
    /// `UNKNOWN_TOPIC_OR_PARTITION` without reaching storage, and the
    /// partitions that the topic has are read as usual.
    #[tokio::test(start_paused = true)]
    async fn unknown_partition_index_skips_storage() -> Result<()> {
        let storage = Partitions::new([(0, vec![Read::Batch(batch(0, &[0])?)]), (1, vec![])]);

        let unknown = [-1, 2, i32::MAX];

        let (partitions, _) = fetch(storage.clone(), &[0, -1, 2, i32::MAX]).await?;

        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(1, batches(&partitions[0]));

        for (answer, index) in partitions[1..].iter().zip(unknown) {
            assert_eq!(index, answer.partition_index);
            assert_eq!(
                ErrorCode::UnknownTopicOrPartition,
                ErrorCode::try_from(answer.error_code)?
            );
            assert_eq!(0, storage.offset_stage_reads(index));
            assert_eq!(Vec::<Call>::new(), storage.calls(index));
        }

        Ok(())
    }

    /// A request for only partitions that the topic does not have answers
    /// at once, as Kafka does.
    #[tokio::test(start_paused = true)]
    async fn only_unknown_partitions_answer_without_waiting() -> Result<()> {
        let storage = Partitions::new([(0, vec![])]);

        let (partitions, elapsed) = fetch(storage, &[1, -1]).await?;

        assert_eq!(Duration::ZERO, elapsed);
        assert!(
            partitions
                .iter()
                .all(|partition| partition.error_code
                    == i16::from(ErrorCode::UnknownTopicOrPartition))
        );

        Ok(())
    }

    /// A partition that the topic does not have does not end the wait for
    /// `min_bytes` while another partition is fine, as in Kafka.
    #[tokio::test(start_paused = true)]
    async fn an_unknown_partition_does_not_end_the_wait() -> Result<()> {
        let storage = Partitions::new([(0, vec![])]);

        let (partitions, elapsed) = fetch(storage, &[0, 1]).await?;

        assert_eq!(MAX_WAIT, elapsed);
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(
            ErrorCode::UnknownTopicOrPartition,
            ErrorCode::try_from(partitions[1].error_code)?
        );

        Ok(())
    }

    /// An unknown topic ID is set aside like an unknown partition: it ends
    /// the wait only when every partition has an error.
    #[test]
    fn an_unknown_topic_id_is_set_aside_when_deciding_to_wait() {
        let answer = |error_code: ErrorCode| {
            nisshi_sans_io::fetch_response::FetchableTopicResponse::default().partitions(Some(
                vec![PartitionData::default().error_code(error_code.into())],
            ))
        };

        assert!(super::answers_without_waiting(&[answer(
            ErrorCode::UnknownTopicId
        )]));
        assert!(!super::answers_without_waiting(&[
            answer(ErrorCode::UnknownTopicId),
            answer(ErrorCode::None),
        ]));
        assert!(super::answers_without_waiting(&[
            answer(ErrorCode::UnknownTopicId),
            answer(ErrorCode::OffsetOutOfRange),
        ]));
    }

    /// A partition that storage reports as unknown, after the request read
    /// the metadata, answers `UNKNOWN_TOPIC_OR_PARTITION` in the response,
    /// and the request goes on.
    #[tokio::test(start_paused = true)]
    async fn storage_unknown_partition_answers_in_band() -> Result<()> {
        let storage = Partitions::new([(0, vec![]), (1, vec![Read::Batch(batch(0, &[0])?)])])
            .delete_after_metadata(0);

        let (partitions, _) = fetch(storage, &[0, 1]).await?;

        assert_eq!(
            ErrorCode::UnknownTopicOrPartition,
            ErrorCode::try_from(partitions[0].error_code)?
        );
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[1].error_code)?
        );
        assert_eq!(1, batches(&partitions[1]));

        Ok(())
    }
}
