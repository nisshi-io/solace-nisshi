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

use std::{cmp::min, future::Future, sync::LazyLock};

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
    Error, METER, OffsetStage, Result, Storage, Topition,
    service::{
        deadline::{self, Missed},
        read_share::FetchReadShare,
    },
};
use validation::ValidatedFetchRequest;

/// Partition reads whose fetch offset falls outside the partition, by
/// `topic` and by `bound`: `below_log_start` (answered with
/// `OFFSET_OUT_OF_RANGE`, once per Fetch) or `above_high_watermark`
/// (answered with `NONE` and no records). The second does not end the wait
/// for `max_wait`, so one Fetch counts once per poll round: its rate follows
/// storage latency, not the number of parked consumers, and on dynostore
/// with several brokers it also counts, for the life of a cached high
/// watermark, consumers that are only ahead of a stale one.
static FETCH_OFFSET_OUT_OF_BOUNDS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_fetch_offset_out_of_bounds")
        .with_description(
            "Partition reads whose fetch offset is below the log start (once per Fetch) or above \
             the high watermark (once per poll round: shows only whether parked fetches exist; \
             a short burst on multi-broker dynostore is expected)",
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
/// A client abandons a Fetch at its own read deadline, then sends it again
/// while the abandoned one keeps running:
///
/// | Client     | Fetch read deadline            |
/// |------------|--------------------------------|
/// | Java       | 30s (`max_wait` not added)     |
/// | librdkafka | 60s + `fetch.wait.max.ms`      |
/// | franz-go   | 10s + `max_wait`               |
///
/// `max_wait` plus this sits under all three with room for the response,
/// for any `max_wait` under Java's 30s less this.
const FETCH_DEADLINE_OVERHEAD: Duration = Duration::from_secs(5);

/// The smallest stop margin: how long an engine stops assembling batches before its partition
/// deadline, when the broker abandons the read and loses every batch it assembled. An engine checks
/// the time only between records or object reads, and starts its clock only after it gets a
/// connection, so it returns a little after it stops.
const MIN_STOP_MARGIN: Duration = Duration::from_millis(50);

static READ_DEADLINE_EXCEEDED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_storage_read_deadline_exceeded")
        .with_description("Storage reads abandoned at the request deadline")
        .build()
});

/// Runs `read` until `deadline`, see [`deadline::bounded`].
///
/// Dropping the read cancels nisshi's side of it, and only that side.
///
/// - Postgres: the engine asks the server to cancel the statement, and
///   keeps the connection and a permit from its limit on Fetch reads in
///   flight until the statement has ended. The connection then goes back
///   to the pool if it is outside any transaction, and is closed if not.
/// - libSQL: a local statement runs synchronously inside the read's
///   `async fn`, so the deadline interrupts only a wait for a permit or a
///   connection, never a slow statement.
/// - SlateDB: the engine ignores when to stop, so a slow read is
///   abandoned with nothing kept.
/// - Object store: work the engine runs on its own tasks carries on until
///   it finishes.
async fn before<F>(deadline: Instant, read: F) -> Result<F::Output, Missed>
where
    F: Future,
{
    deadline::bounded(deadline, read).await
}

/// Records a read of `tp` at `offset` that [`before`] abandoned, in the
/// `nisshi_storage_read_deadline_exceeded` counter and the log.
///
/// `operation` is the [`Storage`] method that was read: `fetch` for the
/// records, `offset_stage` for the offsets. The counter carries it with
/// the stage of the read, so a slow engine is told apart from a request
/// that was too wide for its deadline.
///
/// A read that was never started logs at debug: once a slow backend has
/// spent the read deadline, every partition after it is not started, and a
/// warning for each of them would blame partitions that were never read.
fn missed(
    tp: &Topition,
    operation: &'static str,
    offset: i64,
    missed: Missed,
    started_at: Instant,
) {
    let stage = missed.stage();
    let limit = missed.limit();

    READ_DEADLINE_EXCEEDED.add(1, &missed.attributes(operation));

    match missed {
        Missed::NotStarted => {
            debug!(?tp, operation, offset, stage, elapsed = ?started_at.elapsed(), "fetch read not started")
        }
        Missed::Queued | Missed::QueuedForConnectionShare | Missed::Reading => {
            warn!(?tp, operation, offset, stage, limit, elapsed = ?started_at.elapsed(), "fetch read deadline exceeded")
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
/// its unused share to those after it. A partition whose offset stage is
/// read again after its records takes a fresh half for that read, so it
/// can spend up to three quarters of the time left in all.
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

/// Returns the stop margin of a partition read with `time_left` until its partition deadline: a
/// quarter of `time_left`, and at least [`MIN_STOP_MARGIN`]. A read with less time left than
/// that stops at once. [`next_partition_deadline`] sets the partition deadline.
///
/// A read stops this much time before its partition deadline, not when `max_wait` ends,
/// because `max_wait` limits only how long a Fetch waits for new records. Kafka reads the log up
/// to the request's byte limits without a time limit ([ReplicaManager.scala#L1523-L1563]).
///
/// [ReplicaManager.scala#L1523-L1563]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1523-L1563
fn stop_margin(time_left: Duration) -> Duration {
    // TODO(SOL-157095): let each engine choose its stop margin from what one of its read steps
    // costs, instead of a fraction of the time left that nothing measured.
    (time_left / 4).max(MIN_STOP_MARGIN).min(time_left)
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
    /// Reads one partition, with `left` partitions, this one included, still to read
    /// before `fetch_deadline`.
    ///
    /// A read that has not finished by this partition's deadline (see [`next_partition_deadline`])
    /// is abandoned; an offset stage read again after the records gets a partition deadline of its
    /// own. Each records read stops a stop margin (see [`stop_margin`]) before the partition
    /// deadline, and the reads stop once that time has passed.
    ///
    /// A partition whose read does not finish in time is answered with
    /// [`ErrorCode::None`], whatever batches it had already read, and the
    /// offset stage read before them, raised to cover those batches (see
    /// [`Self::covering`]), so the client fetches it again on its next
    /// request. A partition whose offset stage is not read in time is
    /// answered with unknown offsets; see [`Self::unknown_offsets`].
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip(self,min_bytes,isolation,fetch_partition), fields(partition = fetch_partition.partition))]
    async fn fetch_partition(
        &self,
        fetch_deadline: Instant,
        left: usize,
        min_bytes: u32,
        max_bytes: &mut u32,
        isolation: IsolationLevel,
        topic: &str,
        fetch_partition: &FetchPartition,
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
        let offset_stage = match before(partition_deadline, self.storage.offset_stage(&tp)).await {
            Ok(Err(Error::Api(ErrorCode::UnknownTopicOrPartition))) => {
                return Ok(Self::unknown_to_storage(&tp, "offset_stage"));
            }
            Ok(offset_stage) => offset_stage.inspect_err(|error| error!(?error, ?tp))?,
            Err(reason) => {
                missed(&tp, "offset_stage", fetch_offset, reason, started_at);
                return Ok(Self::unknown_offsets(partition_index, ErrorCode::None));
            }
        };

        // Below the log start offset the records are gone, so Kafka answers
        // OFFSET_OUT_OF_RANGE, and the client applies `auto.offset.reset`.
        // The answer does not wait for `max_wait`, so a client that keeps
        // fetching this offset does so at round-trip speed: the counter, not
        // a log line per fetch, is what makes it visible.
        if fetch_offset < offset_stage.log_start() {
            debug!(
                topic,
                partition = partition_index,
                fetch_offset,
                log_start = offset_stage.log_start(),
                high_watermark = offset_stage.high_watermark(),
                "fetch offset is below the log start",
            );

            FETCH_OFFSET_OUT_OF_BOUNDS.add(
                1,
                &[
                    KeyValue::new("topic", topic.to_owned()),
                    KeyValue::new("bound", "below_log_start"),
                ],
            );

            return Ok(Self::offset_out_of_range(partition_index));
        }

        // Above the high watermark, every engine answers NONE with no
        // records. Kafka does so only up to the log end offset, and answers
        // OFFSET_OUT_OF_RANGE above it ([LocalLog.scala#L368-L371]). This is
        // a deliberate difference. On dynostore, the high watermark that
        // this broker reads can lag a write through another broker, so a
        // client can be ahead of it, and an error would reset the client's
        // position. One rule for every engine keeps the answer independent
        // of the engine. The consequence: a consumer whose position is past
        // the end of the log (after a restore, or a seek past the end) never
        // applies `auto.offset.reset`. It resumes at its position once the
        // log reaches it, and skips the records written below that position.
        // The counter makes such a parked consumer visible.
        //
        // [LocalLog.scala#L368-L371]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/log/LocalLog.scala#L368-L371
        if fetch_offset > offset_stage.high_watermark() {
            debug!(
                topic,
                partition = partition_index,
                fetch_offset,
                log_start = offset_stage.log_start(),
                high_watermark = offset_stage.high_watermark(),
                "fetch offset is above the high watermark",
            );

            FETCH_OFFSET_OUT_OF_BOUNDS.add(
                1,
                &[
                    KeyValue::new("topic", topic.to_owned()),
                    KeyValue::new("bound", "above_high_watermark"),
                ],
            );

            return Ok(Self::partition_data(partition_index, offset_stage, vec![]));
        }

        let stop_at = partition_deadline
            - stop_margin(partition_deadline.saturating_duration_since(Instant::now()));

        let mut batches = Vec::new();

        let mut offset = fetch_offset;

        let mut stalled = false;

        loop {
            if *max_bytes == 0 {
                break;
            }

            debug!(offset);

            let fetched = match before(
                partition_deadline,
                self.storage.fetch(
                    &tp,
                    offset,
                    min_bytes,
                    *max_bytes,
                    isolation,
                    stop_at.saturating_duration_since(Instant::now()),
                ),
            )
            .await
            {
                Ok(Err(Error::Api(ErrorCode::UnknownTopicOrPartition))) => {
                    return Ok(Self::unknown_to_storage(&tp, "fetch"));
                }
                Ok(fetched) => fetched,
                Err(reason) => {
                    missed(&tp, "fetch", offset, reason, started_at);
                    stalled = true;
                    break;
                }
            };

            let mut fetched = fetched
                .inspect(|r| debug!(?tp, ?offset, ?r))
                .inspect_err(|error| error!(?tp, ?error))?;

            *max_bytes =
                u32::try_from(fetched.byte_size()).map(|bytes| max_bytes.saturating_sub(bytes))?;

            debug!(?offset, ?fetched, max_bytes);

            if fetched.is_empty() || fetched.first().is_some_and(|batch| batch.record_count == 0) {
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

            // Engines that assemble batches from rows stop at `stop_at` but return what they have,
            // so another read now would return one record per round trip until max_bytes is spent
            if Instant::now() >= stop_at {
                debug!(?offset, elapsed = ?started_at.elapsed(), ?stop_at);
                break;
            }
        }

        // While there are no records, or they end at or below the stage read
        // before the fetch (a consumer catching up), that stage answers
        // the client: its offsets only grow, and the next fetch reads them
        // again. The bound is the one the engine reads to: the last stable
        // offset under ReadCommitted, which a transaction completing during
        // the fetch can move, and the high watermark otherwise. Records past
        // it were written or committed since, so the stage is read again,
        // so that the answer covers them.
        let covered_to = match isolation {
            IsolationLevel::ReadCommitted => offset_stage.last_stable(),
            IsolationLevel::ReadUncommitted => offset_stage.high_watermark(),
        };

        let offset_stage = if batches.is_empty() || offset <= covered_to {
            offset_stage
        } else if stalled {
            // A read that stalled is not followed by another to the same
            // partition.
            Self::covering(offset_stage, offset, isolation)
        } else {
            // A read that assembled batches until `stop_at` did not stall, so the stage gets a
            // fresh partition deadline rather than what may be none at all.
            match before(
                next_partition_deadline(fetch_deadline, left),
                self.storage.offset_stage(&tp),
            )
            .await
            {
                Ok(Err(Error::Api(ErrorCode::UnknownTopicOrPartition))) => {
                    return Ok(Self::unknown_to_storage(&tp, "offset_stage"));
                }
                Ok(offset_stage) => offset_stage.inspect_err(|error| error!(?error, ?tp))?,
                Err(reason) => {
                    missed(&tp, "offset_stage", offset, reason, started_at);
                    Self::covering(offset_stage, offset, isolation)
                }
            }
        };

        Ok(Self::partition_data(partition_index, offset_stage, batches))
            .inspect(|r| debug!(?r, elapsed = ?started_at.elapsed()))
    }

    /// `offset_stage`, read before the records, raised to cover records
    /// that reach past it when the stage could not be read again in time.
    ///
    /// The high watermark is at least `next`, the offset after the last
    /// record returned. Under `ReadCommitted` the engine returns only
    /// committed records, so the last stable offset is at least `next` too.
    /// Unlike Kafka, which answers a remote read that runs out of time with
    /// the offsets it already holds, nisshi holds only the stage it read
    /// before the records, so the answer can lag a write made since.
    fn covering(offset_stage: OffsetStage, next: i64, isolation: IsolationLevel) -> OffsetStage {
        OffsetStage {
            high_watermark: offset_stage.high_watermark.max(next),
            last_stable: match isolation {
                IsolationLevel::ReadCommitted => offset_stage.last_stable.max(next),
                IsolationLevel::ReadUncommitted => offset_stage.last_stable,
            },
            log_start: offset_stage.log_start,
        }
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

    /// The partition answer for a fetch offset outside the partition, with
    /// the unknown (-1) offsets that Kafka sends alongside this error.
    fn offset_out_of_range(partition_index: i32) -> PartitionData {
        Self::unknown_offsets(partition_index, ErrorCode::OffsetOutOfRange)
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

    fn unknown_topic_response(&self, fetch: &FetchTopic) -> Result<FetchableTopicResponse> {
        Ok(FetchableTopicResponse::default()
            .topic(fetch.topic.clone())
            // From v13 the client matches a response topic by id and drops
            // one it cannot match, so the requested id is echoed: the client
            // then sees the error and refreshes its metadata.
            .topic_id(fetch.topic_id)
            .partitions(fetch.partitions.as_ref().map(|partitions| {
                partitions
                    .iter()
                    .map(|partition| Self::unknown_partition(partition.partition))
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
    fn unknown_to_storage(tp: &Topition, operation: &'static str) -> PartitionData {
        debug!(?tp, operation, "storage reports the partition unknown");

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
        PartitionData::default()
            .partition_index(partition_index)
            .error_code(ErrorCode::UnknownTopicOrPartition.into())
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
    ) -> Result<FetchableTopicResponse> {
        let metadata = self.storage.metadata(Some(&[fetch.into()])).await?;

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
            self.unknown_topic_response(fetch)
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
/// An unknown topic alone does not end the wait while another partition has
/// no error, because Kafka sets unknown topics aside before it reads the log,
/// and waits for the other partitions ([KafkaApis.scala]). A consumer still
/// assigned to a deleted topic then fetches once per `max_wait`, instead of
/// at round trip speed.
///
/// [ReplicaManager.scala]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1550-L1558
/// [KafkaApis.scala]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1031-L1032
fn answers_without_waiting(responses: &[FetchableTopicResponse]) -> bool {
    let unknown_topic = i16::from(ErrorCode::UnknownTopicOrPartition);
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
        } else if error_code != unknown_topic {
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
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::SystemTime,
    };

    use async_trait::async_trait;
    use bytes::Bytes;
    use nisshi_sans_io::{
        ConfigResource, ErrorCode, FetchRequest, IsolationLevel, ListOffset, RequestInput,
        ScramMechanism,
        create_topics_request::CreatableTopic,
        delete_groups_response::DeletableGroupResult,
        delete_records_request::DeleteRecordsTopic,
        delete_records_response::DeleteRecordsTopicResult,
        describe_cluster_response::DescribeClusterBroker,
        describe_configs_response::DescribeConfigsResult,
        describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
        fetch_request::{FetchPartition, FetchTopic},
        fetch_response::{AbortedTransaction, PartitionData},
        incremental_alter_configs_request::AlterConfigsResource,
        incremental_alter_configs_response::AlterConfigsResourceResponse,
        metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
        record::{Record, deflated, inflated},
        txn_offset_commit_response::TxnOffsetCommitResponseTopic,
    };
    use rama::{Layer as _, Service as _, extensions::Extensions};
    use tokio::{
        sync::OwnedSemaphorePermit,
        time::{Duration, Instant, advance},
    };
    use url::Url;
    use uuid::Uuid;

    use super::{
        FETCH_DEADLINE_OVERHEAD, FetchService, MIN_STOP_MARGIN, Missed, before, fetch_deadline,
    };
    use crate::{
        BrokerRegistrationRequest, CommittedOffset, Error, FetchValidationLayer, GroupDetail,
        ListOffsetResponse, MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage,
        ProducerIdResponse, Result, ScramCredential, Storage, TopicId, Topition,
        TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError,
        Version, read_share::FetchReadShare,
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

    /// Implements [`Storage`] for a double with the methods given,
    /// leaving those the doubles here never call unimplemented.
    macro_rules! storage_double {
        ($double:ty { $($method:item)* }) => {
            #[async_trait]
            impl Storage for $double {
                $($method)*

                async fn register_broker(
                    &self,
                    _broker_registration: BrokerRegistrationRequest,
                ) -> Result<()> {
                    unimplemented!()
                }

                async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
                    unimplemented!()
                }

                async fn create_topic(&self, _topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
                    unimplemented!()
                }

                async fn delete_records(
                    &self,
                    _topics: &[DeleteRecordsTopic],
                ) -> Result<Vec<DeleteRecordsTopicResult>> {
                    unimplemented!()
                }

                async fn delete_topic(&self, _topic: &TopicId) -> Result<ErrorCode> {
                    unimplemented!()
                }

                async fn incremental_alter_resource(
                    &self,
                    _resource: AlterConfigsResource,
                ) -> Result<AlterConfigsResourceResponse> {
                    unimplemented!()
                }

                async fn produce(
                    &self,
                    _transaction_id: Option<&str>,
                    _topition: &Topition,
                    _deflated: deflated::Batch,
                ) -> Result<i64> {
                    unimplemented!()
                }

                async fn offset_commit(
                    &self,
                    _group: &str,
                    _retention: Option<Duration>,
                    _offsets: &[(Topition, OffsetCommitRequest)],
                ) -> Result<Vec<(Topition, ErrorCode)>> {
                    unimplemented!()
                }

                async fn committed_offset_topitions(
                    &self,
                    _group_id: &str,
                ) -> Result<BTreeMap<Topition, CommittedOffset>> {
                    unimplemented!()
                }

                async fn pending_txn_offsets(&self, _group_id: &str) -> Result<BTreeSet<Topition>> {
                    unimplemented!()
                }

                async fn offset_fetch(
                    &self,
                    _group_id: Option<&str>,
                    _topics: &[Topition],
                    _require_stable: Option<bool>,
                ) -> Result<BTreeMap<Topition, CommittedOffset>> {
                    unimplemented!()
                }

                async fn list_offsets(
                    &self,
                    _isolation_level: IsolationLevel,
                    _offsets: &[(Topition, ListOffset)],
                ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
                    unimplemented!()
                }

                async fn describe_config(
                    &self,
                    _name: &str,
                    _resource: ConfigResource,
                    _keys: Option<&[String]>,
                ) -> Result<DescribeConfigsResult> {
                    unimplemented!()
                }

                async fn describe_topic_partitions(
                    &self,
                    _topics: Option<&[TopicId]>,
                    _partition_limit: i32,
                    _cursor: Option<Topition>,
                ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
                    unimplemented!()
                }

                async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
                    unimplemented!()
                }

                async fn delete_groups(
                    &self,
                    _group_ids: Option<&[String]>,
                ) -> Result<Vec<DeletableGroupResult>> {
                    unimplemented!()
                }

                async fn describe_groups(
                    &self,
                    _group_ids: Option<&[String]>,
                    _include_authorized_operations: bool,
                ) -> Result<Vec<NamedGroupDetail>> {
                    unimplemented!()
                }

                async fn group_with_version(
                    &self,
                    _group_id: &str,
                ) -> Result<Option<(GroupDetail, Version)>> {
                    unimplemented!()
                }

                async fn update_group(
                    &self,
                    _group_id: &str,
                    _detail: GroupDetail,
                    _version: Option<Version>,
                ) -> Result<Version, UpdateError<GroupDetail>> {
                    unimplemented!()
                }

                async fn init_producer(
                    &self,
                    _transaction_id: Option<&str>,
                    _transaction_timeout_ms: i32,
                    _producer_id: Option<i64>,
                    _producer_epoch: Option<i16>,
                ) -> Result<ProducerIdResponse> {
                    unimplemented!()
                }

                async fn txn_add_offsets(
                    &self,
                    _transaction_id: &str,
                    _producer_id: i64,
                    _producer_epoch: i16,
                    _group_id: &str,
                ) -> Result<ErrorCode> {
                    unimplemented!()
                }

                async fn txn_add_partitions(
                    &self,
                    _partitions: TxnAddPartitionsRequest,
                ) -> Result<TxnAddPartitionsResponse> {
                    unimplemented!()
                }

                async fn txn_offset_commit(
                    &self,
                    _offsets: TxnOffsetCommitRequest,
                ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
                    unimplemented!()
                }

                async fn txn_end(
                    &self,
                    _transaction_id: &str,
                    _producer_id: i64,
                    _producer_epoch: i16,
                    _committed: bool,
                ) -> Result<ErrorCode> {
                    unimplemented!()
                }

                async fn maintain(&self, _now: SystemTime) -> Result<()> {
                    unimplemented!()
                }

                async fn maintain_transactions(&self, _now: SystemTime) -> Result<()> {
                    unimplemented!()
                }

                async fn aborted_transactions(
                    &self,
                    _topition: &Topition,
                    _offset: i64,
                    _last_stable_offset: i64,
                ) -> Result<Vec<AbortedTransaction>> {
                    unimplemented!()
                }

                async fn cluster_id(&self) -> Result<String> {
                    unimplemented!()
                }

                async fn node(&self) -> Result<i32> {
                    unimplemented!()
                }

                async fn advertised_listener(&self) -> Result<Url> {
                    unimplemented!()
                }

                async fn ping(&self) -> Result<()> {
                    unimplemented!()
                }

                async fn delete_user_scram_credential(
                    &self,
                    _user: &str,
                    _mechanism: ScramMechanism,
                ) -> Result<()> {
                    unimplemented!()
                }

                async fn upsert_user_scram_credential(
                    &self,
                    _user: &str,
                    _mechanism: ScramMechanism,
                    _credential: ScramCredential,
                ) -> Result<()> {
                    unimplemented!()
                }

                async fn user_scram_credential(
                    &self,
                    _user: &str,
                    _mechanism: ScramMechanism,
                ) -> Result<Option<ScramCredential>> {
                    unimplemented!()
                }
            }
        };
    }

    /// Storage whose `fetch` replays scripted responses, records the offset and remaining
    /// `max_wait` of each call, and optionally advances the (paused) clock past the deadline on the
    /// first call, as an engine that assembles one truncated batch until it has to stop does. Each
    /// read of the offset stage moves its last stable offset and high watermark up by
    /// `stage_growth`, as writes during a fetch do, so a test can tell which read answered.
    #[derive(Clone, Debug)]
    struct Scripted {
        responses: Arc<Mutex<Vec<Vec<deflated::Batch>>>>,
        calls: Arc<Mutex<Vec<(i64, Duration)>>>,
        first_call_takes: Option<Duration>,
        offset_stage: OffsetStage,
        offset_stage_reads: Arc<AtomicUsize>,
        stage_growth: i64,
        /// Offset stage reads, counted from 0, from which the partition is
        /// unknown to storage.
        unknown_from_stage_read: Option<usize>,
        /// Records reads, counted from 1, from which the partition is
        /// unknown to storage.
        unknown_from_fetch_call: Option<usize>,
    }

    impl Scripted {
        fn new(responses: Vec<Vec<deflated::Batch>>, offset_stage: OffsetStage) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses)),
                calls: Arc::new(Mutex::new(vec![])),
                first_call_takes: None,
                offset_stage,
                offset_stage_reads: Arc::new(AtomicUsize::new(0)),
                stage_growth: 0,
                unknown_from_stage_read: None,
                unknown_from_fetch_call: None,
            }
        }

        fn calls(&self) -> Vec<(i64, Duration)> {
            self.calls.lock().expect("calls").clone()
        }

        fn call_offsets(&self) -> Vec<i64> {
            self.calls().into_iter().map(|(offset, _)| offset).collect()
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
            max_wait: Duration,
        ) -> Result<Vec<deflated::Batch>> {
            let calls = {
                let mut calls = self.calls.lock()?;
                calls.push((offset, max_wait));
                calls.len()
            };

            if self
                .unknown_from_fetch_call
                .is_some_and(|unknown_from| calls >= unknown_from)
            {
                return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
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

            if calls == 1
                && let Some(duration) = self.first_call_takes
            {
                advance(duration).await;
            }

            Ok(response)
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

    /// Engines that rebuild batches from rows stop assembling at the
    /// deadline, but still return the record they were on, so after the
    /// deadline every storage call yields one record. Once the deadline
    /// has passed the loop must return what it has rather than issue a
    /// storage round trip per remaining record until `max_bytes` is spent.
    #[tokio::test(start_paused = true)]
    async fn stops_at_the_deadline() -> Result<()> {
        let max_wait = Duration::from_millis(100);

        // The test reads one partition, so its partition deadline is the fetch deadline, and the
        // engine stops a quarter of that time before it.
        let fetch_deadline = max_wait + FETCH_DEADLINE_OVERHEAD;
        let stop_after = fetch_deadline - fetch_deadline / 4;

        // one truncated batch takes until the engine has to stop, then a single
        // record per call forever
        let responses = (0..10_000)
            .map(|offset| batch(offset, &[0]).map(|batch| vec![batch]))
            .collect::<Result<Vec<_>>>()?;

        let storage = Scripted {
            first_call_takes: Some(stop_after),
            ..Scripted::new(responses, OFFSET_STAGE)
        };

        let batches = fetch_partition(storage.clone(), max_wait, 1024 * 1024).await?;

        assert_eq!(1, batches.len());
        assert_eq!(0, batches[0].base_offset);
        assert_eq!(
            vec![(0, stop_after)],
            storage.calls(),
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
                storage.calls().is_empty(),
                "fetch at {fetch_offset} reached storage: {:?}",
                storage.calls()
            );
        }

        Ok(())
    }

    /// A fetch offset above the high watermark is answered with `NONE` and
    /// no records, on every engine. Kafka does so only up to the log end
    /// offset; here the high watermark that a broker reads can lag a write
    /// through another broker (dynostore). The offset never reaches
    /// storage, which may build an invalid key range from it (`i64::MAX`,
    /// as a big-endian key).
    #[tokio::test(start_paused = true)]
    async fn above_high_watermark_answers_none_without_storage() -> Result<()> {
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
                ErrorCode::None,
                ErrorCode::try_from(partition.error_code)?,
                "fetch at {fetch_offset}"
            );
            assert_eq!(1_000, partition.high_watermark, "fetch at {fetch_offset}");
            assert_eq!(Some(500), partition.last_stable_offset);
            assert_eq!(Some(100), partition.log_start_offset);
            assert!(partition.records.is_none(), "fetch at {fetch_offset}");
            assert!(
                storage.calls().is_empty(),
                "fetch at {fetch_offset} reached storage: {:?}",
                storage.calls()
            );
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

    /// A fetch whose records end at or below the high watermark read for
    /// the bounds check (none at all, as for an idle consumer on every long
    /// poll, or a consumer catching up) reads the offset stage once, for
    /// both the check and the answer. A fetch whose records reach past that
    /// high watermark reads the stage again, so that the answer covers them.
    /// The stage grows by 10 on each read, so the answered offsets show
    /// which read answered.
    #[tokio::test(start_paused = true)]
    async fn offset_stage_reads() -> Result<()> {
        const GROWTH: i64 = 10;

        struct Case {
            name: &'static str,
            responses: Vec<Vec<deflated::Batch>>,
            stage: OffsetStage,
            isolation: IsolationLevel,
            fetch_offset: i64,
            reads: usize,
        }

        let cases = [
            Case {
                name: "idle",
                responses: vec![vec![]],
                stage: OFFSET_STAGE,
                isolation: IsolationLevel::ReadUncommitted,
                fetch_offset: 1_000,
                reads: 1,
            },
            Case {
                name: "catching up",
                responses: vec![vec![batch(0, &[0])?], vec![]],
                stage: OFFSET_STAGE,
                isolation: IsolationLevel::ReadUncommitted,
                fetch_offset: 0,
                reads: 1,
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
                reads: 2,
            },
            // the last record is at 999, so the next offset is the high
            // watermark, which covers it
            Case {
                name: "ending at the high watermark",
                responses: vec![vec![batch(996, &[0, 3])?], vec![]],
                stage: OFFSET_STAGE,
                isolation: IsolationLevel::ReadUncommitted,
                fetch_offset: 996,
                reads: 1,
            },
            // the last record is at 1,000, one past what the high watermark
            // covers
            Case {
                name: "ending one past the high watermark",
                responses: vec![vec![batch(997, &[0, 3])?], vec![]],
                stage: OFFSET_STAGE,
                isolation: IsolationLevel::ReadUncommitted,
                fetch_offset: 997,
                reads: 2,
            },
            // under ReadCommitted, records past the last stable offset read
            // before the fetch (a transaction completed meanwhile) read the
            // stage again, though they end below the high watermark
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
                reads: 2,
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
                reads: 1,
            },
        ];

        for case in cases {
            let storage = Scripted {
                stage_growth: GROWTH,
                ..Scripted::new(case.responses, case.stage)
            };

            let partition = fetch_partition_at(
                storage.clone(),
                Duration::from_millis(100),
                1024,
                case.isolation,
                case.fetch_offset,
            )
            .await?;

            assert_eq!(case.reads, storage.offset_stage_reads(), "{}", case.name);

            // the answer comes from the last read
            let growth = GROWTH * i64::try_from(case.reads - 1)?;
            assert_eq!(
                case.stage.high_watermark + growth,
                partition.high_watermark,
                "{}",
                case.name
            );
            assert_eq!(
                Some(case.stage.last_stable + growth),
                partition.last_stable_offset,
                "{}",
                case.name
            );

            // the answered bound covers every record returned
            let next_offset = partition
                .records
                .iter()
                .flat_map(|frame| &frame.batches)
                .map(|batch| batch.max_offset() + 1)
                .max();

            if let Some(next_offset) = next_offset {
                let covered_to = match case.isolation {
                    IsolationLevel::ReadCommitted => partition.last_stable_offset,
                    IsolationLevel::ReadUncommitted => Some(partition.high_watermark),
                };

                assert!(
                    covered_to.is_some_and(|covered_to| next_offset <= covered_to),
                    "{}: records end at {next_offset}, answer covers {covered_to:?}",
                    case.name
                );
            }
        }

        Ok(())
    }

    /// What one storage read of a partition does, in [`Partitions`].
    #[derive(Clone, Debug)]
    enum Read {
        /// Returns this batch, or nothing when it has to stop at once, as engines that stop
        /// assembling at their stop time do.
        Batch(deflated::Batch),

        /// Returns nothing.
        Empty,

        /// Does not answer for an hour.
        Stall,

        /// Returns this batch once its time to stop has passed, plus the given overshoot, as an
        /// engine does that checks when to stop only between records.
        Assemble(deflated::Batch, Duration),
    }

    /// How long past its time to stop a [`Read::Assemble`] returns, when the test is not
    /// about the overshoot.
    const OVERSHOOT: Duration = Duration::from_millis(8);

    /// One storage read of a partition, as [`Partitions`] saw it.
    #[derive(Clone, Debug, PartialEq)]
    struct Call {
        topic: String,
        partition: i32,
        max_bytes: u32,
        max_wait: Duration,
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
        calls: Arc<Mutex<Vec<Call>>>,
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

        fn calls(&self, partition: i32) -> Vec<Call> {
            self.calls
                .lock()
                .expect("calls")
                .iter()
                .filter(|call| call.topic == TOPIC && call.partition == partition)
                .cloned()
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
            max_wait: Duration,
        ) -> Result<Vec<deflated::Batch>> {
            self.calls.lock()?.push(Call {
                topic: topition.topic().into(),
                partition: topition.partition(),
                max_bytes,
                max_wait,
            });

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
                Read::Batch(_) if max_wait.is_zero() => Ok(vec![]),
                Read::Batch(batch) => Ok(vec![batch]),
                Read::Empty => Ok(vec![]),
                Read::Stall => {
                    tokio::time::sleep(Duration::from_secs(3_600)).await;
                    Ok(vec![])
                }
                Read::Assemble(batch, overshoot) => {
                    tokio::time::sleep(max_wait + overshoot).await;
                    Ok(vec![batch])
                }
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

            Ok(OffsetStage {
                last_stable: 1_000,
                high_watermark: 1_000,
                log_start: 0,
            })
        }

        /// Lists each scripted topic with partitions up to its highest
        /// scripted index.
        async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
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
        let storage = Partitions::new([
            (0, vec![Read::Stall]),
            (1, vec![Read::Batch(batch(0, &[0])?)]),
            (2, vec![Read::Batch(batch(0, &[0])?)]),
            (3, vec![Read::Batch(batch(0, &[0])?)]),
        ]);

        let (partitions, elapsed) = fetch(storage.clone(), &[0, 1, 2, 3]).await?;

        assert_eq!(FETCH_DEADLINE / 2, elapsed);

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

        // The stall left half of the fetch deadline. Each read after it stopped a quarter of its
        // time short of its partition deadline: half of what was left for the first two partitions,
        // and all of it for the last.
        let left = FETCH_DEADLINE / 2;
        for (partition, time_left) in [(1, left / 2), (2, left / 2), (3, left)] {
            assert_eq!(
                time_left - time_left / 4,
                storage.calls(partition)[0].max_wait,
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

    /// A partition that stalls after reading some batches returns them,
    /// with the stage read before them raised to cover them rather than
    /// read again, and the bytes they took are not given again to the
    /// partitions after it. Under `ReadCommitted` the engine returns only
    /// committed records, so the last stable offset is raised to cover
    /// them too.
    #[tokio::test(start_paused = true)]
    async fn a_partition_that_stalls_keeps_what_it_read() -> Result<()> {
        for (isolation, last_stable) in [
            (IsolationLevel::ReadUncommitted, 1_000),
            (IsolationLevel::ReadCommitted, 1_002),
        ] {
            // the records reach past the high watermark the stage read
            // before them holds
            let first = batch(1_000, &[0, 1])?;
            let size = u32::try_from(first.record_data.len())?;

            let storage = Partitions::new([
                (0, vec![Read::Batch(first), Read::Stall]),
                (1, vec![Read::Batch(batch(0, &[0])?)]),
            ]);

            let (mut topics, elapsed) =
                fetch_isolated(storage.clone(), MAX_WAIT, isolation, &[(TOPIC, &[0, 1])]).await?;
            let partitions = topics.remove(0);

            assert_eq!(FETCH_DEADLINE / 2, elapsed, "{isolation:?}");

            assert_eq!(1, batches(&partitions[0]), "{isolation:?}");
            assert_eq!(1_002, partitions[0].high_watermark, "{isolation:?}");
            assert_eq!(
                Some(last_stable),
                partitions[0].last_stable_offset,
                "{isolation:?}"
            );
            assert_eq!(Some(0), partitions[0].log_start_offset, "{isolation:?}");
            assert_eq!(1, storage.offset_stage_reads(0), "{isolation:?}");

            assert_eq!(1, batches(&partitions[1]), "{isolation:?}");

            let max_bytes = u32::try_from(MAX_BYTES)?;
            assert_eq!(max_bytes - size, storage.calls(0)[1].max_bytes);
            assert_eq!(max_bytes - size, storage.calls(1)[0].max_bytes);
        }

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
    /// does, and each read stops only its stop margin short of its partition deadline,
    /// whatever `max_wait` is.
    #[tokio::test(start_paused = true)]
    async fn zero_max_wait_reads_until_its_stop_margin() -> Result<()> {
        let storage = Partitions::new([(0, vec![Read::Batch(batch(0, &[0])?)])]);

        let (mut topics, elapsed) =
            fetch_topics(storage.clone(), Duration::ZERO, &[(TOPIC, &[0])]).await?;
        let partitions = topics.remove(0);

        assert_eq!(Duration::ZERO, elapsed);
        assert_eq!(1, batches(&partitions[0]));
        assert_eq!(1_000, partitions[0].high_watermark);

        let time_left = FETCH_DEADLINE_OVERHEAD;
        assert_eq!(time_left - time_left / 4, storage.calls(0)[0].max_wait);

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

    /// A read stops a quarter of its time short of its partition deadline, so a read that returns
    /// within that quarter after it stops keeps what it assembled. With `max_wait` of 5s, the first
    /// of two partitions has 5s until its partition deadline, and stops after 3.75s.
    #[tokio::test(start_paused = true)]
    async fn a_read_that_stops_at_its_stop_margin_keeps_what_it_read() -> Result<()> {
        let max_wait = Duration::from_secs(5);
        let time_left = (max_wait + FETCH_DEADLINE_OVERHEAD) / 2;

        // past the least stop margin, within a quarter of the time left
        let overshoot = Duration::from_secs(1);

        let storage = Partitions::new([
            (0, vec![Read::Assemble(batch(0, &[0])?, overshoot)]),
            (1, vec![Read::Batch(batch(0, &[0])?)]),
        ]);

        let (mut topics, _) = fetch_topics(storage.clone(), max_wait, &[(TOPIC, &[0, 1])]).await?;
        let partitions = topics.remove(0);

        assert_eq!(time_left - time_left / 4, storage.calls(0)[0].max_wait);

        assert_eq!(1, batches(&partitions[0]));
        assert_eq!(1_000, partitions[0].high_watermark);
        assert_eq!(1, batches(&partitions[1]));

        Ok(())
    }

    /// An offset stage read again after records reached past it, and that never answers, is
    /// abandoned at a fresh partition deadline taken after the read. The partition keeps its
    /// batches, and the stage read before them answers, raised to cover the records: the last
    /// stable offset too, under `ReadCommitted`.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_offset_stage_is_abandoned_at_its_own_partition_deadline() -> Result<()> {
        for (isolation, last_stable) in [
            (IsolationLevel::ReadUncommitted, 1_000),
            (IsolationLevel::ReadCommitted, 1_001),
        ] {
            // the record is at the high watermark the stage read before the
            // records holds, so the stage is read again
            let storage = Partitions::new([
                (0, vec![Read::Assemble(batch(1_000, &[0])?, OVERSHOOT)]),
                (1, vec![Read::Batch(batch(0, &[0])?)]),
            ])
            .stall_offset_stage(0, 2);

            let (mut topics, elapsed) =
                fetch_isolated(storage, MAX_WAIT, isolation, &[(TOPIC, &[0, 1])]).await?;
            let partitions = topics.remove(0);

            // The read runs three quarters of the time until its partition deadline, half of the
            // fetch deadline, and its overshoot, then the offset stage gets half of the time
            // left. Tokio's timers have millisecond resolution, so the stage's deadline can end
            // up to a millisecond later.
            let time_left = FETCH_DEADLINE / 2;
            let read = time_left - time_left / 4 + OVERSHOOT;
            let expected = read + (FETCH_DEADLINE - read) / 2;
            assert!(
                elapsed.abs_diff(expected) <= Duration::from_millis(1),
                "{isolation:?}: {elapsed:?}, expected {expected:?}"
            );

            assert_eq!(1, batches(&partitions[0]), "{isolation:?}");
            assert_eq!(1_001, partitions[0].high_watermark, "{isolation:?}");
            assert_eq!(
                Some(last_stable),
                partitions[0].last_stable_offset,
                "{isolation:?}"
            );
            assert_eq!(Some(0), partitions[0].log_start_offset, "{isolation:?}");

            assert_eq!(1, batches(&partitions[1]), "{isolation:?}");
            assert_eq!(1_000, partitions[1].high_watermark, "{isolation:?}");
        }

        Ok(())
    }

    /// A `max_wait` whose fetch deadline halves into whole milliseconds seven times,
    /// since tokio's timers have millisecond resolution: 5504ms, so the 32nd of it is
    /// 172ms and the 128th is 43ms.
    const HALVING_MAX_WAIT: Duration = Duration::from_millis(504);

    /// A partition whose time until its partition deadline is too short for a quarter of it
    /// to cover an engine's overshoot keeps the least stop margin instead. After five stalls
    /// the last partition has a 32nd of the fetch deadline, under 200ms, so it stops 50ms
    /// before its partition deadline, and a read that overshoots its stop by less than that
    /// keeps what it assembled.
    #[tokio::test(start_paused = true)]
    async fn a_short_partition_deadline_keeps_the_least_stop_margin() -> Result<()> {
        let time_left = (HALVING_MAX_WAIT + FETCH_DEADLINE_OVERHEAD) / 32;
        assert!(time_left / 4 < MIN_STOP_MARGIN && time_left > MIN_STOP_MARGIN);

        // past a quarter of the time left, within the least stop margin
        let overshoot = Duration::from_millis(45);
        assert!(time_left / 4 < overshoot && overshoot < MIN_STOP_MARGIN);

        let storage = Partitions::new([
            (0, vec![Read::Stall]),
            (1, vec![Read::Stall]),
            (2, vec![Read::Stall]),
            (3, vec![Read::Stall]),
            (4, vec![Read::Stall]),
            (5, vec![Read::Assemble(batch(0, &[0])?, overshoot)]),
        ]);

        let (mut topics, _) = fetch_topics(
            storage.clone(),
            HALVING_MAX_WAIT,
            &[(TOPIC, &[0, 1, 2, 3, 4, 5])],
        )
        .await?;
        let partitions = topics.remove(0);

        assert_eq!(time_left - MIN_STOP_MARGIN, storage.calls(5)[0].max_wait);
        assert_eq!(1, batches(&partitions[5]));
        assert_eq!(1_000, partitions[5].high_watermark);

        Ok(())
    }

    /// A partition with less time until its partition deadline than the least stop margin is read
    /// with no time to assemble anything, and answers with the stage read before the records. After
    /// seven stalls the last partition has a 128th of the fetch deadline, under 50ms.
    #[tokio::test(start_paused = true)]
    async fn less_time_than_the_least_stop_margin_stops_the_read_at_once() -> Result<()> {
        let fetch_deadline = HALVING_MAX_WAIT + FETCH_DEADLINE_OVERHEAD;
        let time_left = fetch_deadline / 128;
        assert!(time_left < MIN_STOP_MARGIN);

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

        assert_eq!(Duration::ZERO, storage.calls(7)[0].max_wait);
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(partitions[7].error_code)?
        );
        assert_eq!(0, batches(&partitions[7]));
        assert_eq!(1_000, partitions[7].high_watermark);

        Ok(())
    }

    /// The stop margin of the records read is a quarter of the time left after a slow offset stage
    /// read, not of the time before that read.
    #[tokio::test(start_paused = true)]
    async fn the_stop_margin_is_taken_after_the_offset_stage() -> Result<()> {
        let stage_read = MAX_WAIT - MAX_WAIT / 4;

        let storage = Partitions::new([
            (0, vec![Read::Batch(batch(0, &[0])?)]),
            (1, vec![Read::Batch(batch(0, &[0])?)]),
        ])
        .delay_offset_stage(0, 1, stage_read);

        let (partitions, _) = fetch(storage.clone(), &[0, 1]).await?;

        let time_left = FETCH_DEADLINE / 2 - stage_read;
        assert_eq!(time_left - time_left / 4, storage.calls(0)[0].max_wait);
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

        // the last partition's time is all of the half that the stall left
        let time_left = FETCH_DEADLINE / 2;
        assert_eq!(time_left - time_left / 4, storage.calls(1)[0].max_wait);

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
        // records up to offset 3, past a high watermark of 1, so the offset
        // stage is read again after the records
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
            (
                "offset stage read after the records",
                Scripted {
                    unknown_from_stage_read: Some(1),
                    ..Scripted::new(vec![vec![batch(0, &[0, 3])?], vec![]], stage)
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

    /// A read that does not finish by the deadline is abandoned there.
    #[tokio::test(start_paused = true)]
    async fn before_abandons_a_read_at_the_deadline() {
        let started_at = Instant::now();
        let deadline = started_at + Duration::from_secs(1);

        assert_eq!(
            Err(Missed::Reading),
            before(deadline, std::future::pending::<()>()).await
        );
        assert_eq!(Duration::from_secs(1), started_at.elapsed());
    }

    /// Once the deadline has passed, the read is never polled.
    #[tokio::test(start_paused = true)]
    async fn before_does_not_start_after_the_deadline() {
        let deadline = Instant::now();
        advance(Duration::from_millis(1)).await;

        let polled = Arc::new(AtomicBool::new(false));

        let read = {
            let polled = polled.clone();
            async move { polled.store(true, Ordering::SeqCst) }
        };

        assert_eq!(Err(Missed::NotStarted), before(deadline, read).await);
        assert!(!polled.load(Ordering::SeqCst));
    }

    /// A read abandoned while it waits for its engine's limit on reads in
    /// flight is reported as queued, not as a slow read.
    #[tokio::test(start_paused = true)]
    async fn before_reports_a_read_abandoned_in_its_engines_queue() {
        let deadline = Instant::now() + Duration::from_secs(1);

        assert_eq!(
            Err(Missed::Queued),
            before(
                deadline,
                crate::service::deadline::queued(std::future::pending::<()>())
            )
            .await
        );
    }
}
