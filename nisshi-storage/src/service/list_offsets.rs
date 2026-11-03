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

use std::{collections::BTreeSet, slice};

use futures::{StreamExt as _, stream};
use nisshi_sans_io::{
    ApiKey, ErrorCode, IsolationLevel, ListOffset, ListOffsetsRequest, ListOffsetsResponse,
    RequestInput,
    list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
};
use rama::Service;
use tokio::time::Instant;
use tracing::{debug, error, instrument, warn};

use crate::{
    Error, ListOffsetResponse, Result, Storage, Topition,
    service::{
        deadline::{LIST_OFFSETS_READ_DEADLINE, Missed, within},
        read_share::ListOffsetsReadShare,
    },
};

/// How many partitions of one ListOffsets request are read from storage at
/// once.
///
/// A storage engine with a connection pool adds its own limit across all
/// requests, where it can size that limit from the pool.
pub const LIST_OFFSETS_CONCURRENCY: usize = 4;

/// How many partitions each per-request warning of a timeout or a storage
/// error names.
const PARTITION_SAMPLE: usize = 8;

/// A [`Service`] using its [`Storage`] taking [`ListOffsetsRequest`] returning [`ListOffsetsResponse`].
/// ```no_run
/// use rama::Service;
/// use nisshi_sans_io::{
///     ErrorCode, IsolationLevel, ListOffset, ListOffsetsRequest,
///     list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
/// };
/// use nisshi_storage::{Error, ListOffsetsService, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// const HOST: &str = "localhost";
/// const PORT: i32 = 9092;
/// const NODE_ID: i32 = 111;
///
/// let storage = StorageContainer::builder()
///     .cluster_id("nisshi")
///     .node_id(NODE_ID)
///     .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let service = ListOffsetsService { storage };
///
/// let topic = "abcba";
///
/// let response = service
///     .serve(
///         ListOffsetsRequest::default()
///             .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
///             .replica_id(NODE_ID)
///             .topics(Some(
///                 [ListOffsetsTopic::default()
///                     .name(topic.into())
///                     .partitions(Some(
///                         [ListOffsetsPartition::default()
///                             .current_leader_epoch(Some(-1))
///                             .max_num_offsets(Some(3))
///                             .partition_index(0)
///                             .timestamp(ListOffset::Earliest.try_into()?)]
///                         .into(),
///                     ))]
///                 .into(),
///             )),
///     )
///     .await?;
///
/// let topics = response.topics.as_deref().unwrap_or_default();
/// assert_eq!(1, topics.len());
/// assert_eq!(topic, topics[0].name);
///
/// let partitions = topics[0].partitions.as_deref().unwrap_or_default();
/// assert_eq!(1, partitions.len());
/// assert_eq!(0, partitions[0].partition_index);
/// assert_eq!(Some(vec![0]), partitions[0].old_style_offsets);
/// assert_eq!(
///     ErrorCode::None,
///     ErrorCode::try_from(partitions[0].error_code)?
/// );
/// assert_eq!(Some(-1), partitions[0].timestamp);
/// assert_eq!(Some(0), partitions[0].offset);
/// assert_eq!(Some(0), partitions[0].leader_epoch);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct ListOffsetsService<G> {
    pub storage: G,
}

impl<G> ApiKey for ListOffsetsService<G> {
    const KEY: i16 = ListOffsetsRequest::KEY;
}

impl<G> ListOffsetsService<G>
where
    G: Storage,
{
    /// Reads each partition's offset separately, a few at a time, each
    /// bounded by one deadline for the whole request. A partition still
    /// unanswered at the deadline gets `REQUEST_TIMED_OUT`, which clients
    /// retry, so one slow partition neither delays the others nor holds the
    /// request past the client's own timeout. A storage error answers in its
    /// own partition (see [`Self::storage_error_code`]), and the other
    /// partitions still answer.
    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Vec<(Topition, ListOffsetResponse)> {
        let started_at = Instant::now();
        let deadline = started_at + LIST_OFFSETS_READ_DEADLINE;

        // The reads are built up front (but not started) rather than in a
        // `StreamExt::map` closure, whose lifetimes keep `serve` from being `Send`.
        let reads = offsets
            .iter()
            .enumerate()
            .map(|(index, offset)| async move {
                let answer = within(
                    "list_offsets",
                    deadline,
                    self.storage
                        .list_offsets(isolation_level, slice::from_ref(offset)),
                )
                .await;

                (index, offset, answer)
            })
            .collect::<Vec<_>>();

        let mut answers = stream::iter(reads).buffer_unordered(LIST_OFFSETS_CONCURRENCY);

        let mut responses = vec![Vec::new(); offsets.len()];
        let mut timed_out = Vec::new();
        let mut failed = Vec::new();
        let mut first_error = None;

        while let Some((index, (topition, list_offset), answer)) = answers.next().await {
            responses[index] = match answer {
                // Kafka answers `Earliest` and `Latest` without a timestamp
                // ([Partition.scala#L1617-L1619], [UnifiedLog.scala#L1325-L1336]),
                // so the timestamp that a storage engine returns for them is
                // dropped here, for every engine.
                //
                // [Partition.scala#L1617-L1619]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/cluster/Partition.scala#L1617-L1619
                // [UnifiedLog.scala#L1325-L1336]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/log/UnifiedLog.scala#L1325-L1336
                Ok(Ok(answer))
                    if matches!(list_offset, ListOffset::Earliest | ListOffset::Latest) =>
                {
                    answer
                        .into_iter()
                        .map(|(topition, response)| {
                            (
                                topition,
                                ListOffsetResponse {
                                    timestamp: None,
                                    ..response
                                },
                            )
                        })
                        .collect()
                }

                Ok(Ok(answer)) => answer,

                Ok(Err(error)) => {
                    let error_code = Self::storage_error_code(&error);
                    debug!(?error, ?topition, ?error_code, "list offsets");

                    failed.push((topition, error_code));
                    _ = first_error.get_or_insert(error);

                    vec![(
                        topition.clone(),
                        ListOffsetResponse {
                            error_code,
                            timestamp: None,
                            offset: Some(-1),
                        },
                    )]
                }

                Err(missed) => {
                    timed_out.push((topition, missed));

                    vec![(
                        topition.clone(),
                        ListOffsetResponse {
                            error_code: ErrorCode::RequestTimedOut,
                            timestamp: None,
                            offset: Some(-1),
                        },
                    )]
                }
            };
        }

        if !timed_out.is_empty() {
            let count = |stage: Missed| {
                timed_out
                    .iter()
                    .filter(|(_, missed)| *missed == stage)
                    .count()
            };

            warn!(
                deadline = ?LIST_OFFSETS_READ_DEADLINE,
                elapsed = ?started_at.elapsed(),
                timed_out = timed_out.len(),
                partitions = offsets.len(),
                reading = count(Missed::Reading),
                queued = count(Missed::Queued),
                queued_for_connection_share = count(Missed::QueuedForConnectionShare),
                not_started = count(Missed::NotStarted),
                sample = ?&timed_out[..timed_out.len().min(PARTITION_SAMPLE)],
                "list offsets answered REQUEST_TIMED_OUT for partitions unread at the deadline"
            );
        }

        // One line per request, not per partition, so an unavailable storage
        // engine can't flood the log with a line for every partition of every
        // retry; each partition is at `debug!` above.
        if let Some(first_error) = first_error {
            error!(
                failed = failed.len(),
                partitions = offsets.len(),
                sample = ?&failed[..failed.len().min(PARTITION_SAMPLE)],
                ?first_error,
                "list offsets answered an error for partitions whose storage read failed"
            );
        }

        responses.into_iter().flatten().collect()
    }

    /// Returns the error code that a partition answers when storage fails
    /// to read its offset: the code of an [`Error::Api`], and
    /// `KAFKA_STORAGE_ERROR` for every other error.
    ///
    /// Kafka answers `UNKNOWN_SERVER_ERROR` for an exception that it does
    /// not map to a code ([KafkaApis.scala#L1228-L1251]). We answer
    /// `KAFKA_STORAGE_ERROR` instead, because these errors come from the
    /// storage engine (a database or object store that times out or is
    /// unavailable), and Kafka answers that code for a log that it cannot
    /// read. A client retries `KAFKA_STORAGE_ERROR` after a metadata
    /// refresh, and gives up on `UNKNOWN_SERVER_ERROR`.
    ///
    /// [KafkaApis.scala#L1228-L1251]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1228-L1251
    fn storage_error_code(error: &Error) -> ErrorCode {
        match error {
            Error::Api(error_code) => *error_code,
            _ => ErrorCode::KafkaStorageError,
        }
    }
}

impl<G, I> Service<I> for ListOffsetsService<G>
where
    G: Storage,
    I: Into<RequestInput<ListOffsetsRequest>> + Send + 'static,
{
    type Output = ListOffsetsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();
        let throttle_time_ms = Some(0);

        // `isolation_level` applies to the whole request, and the wire response has no
        // top-level error code (only per-partition ones), so an out of range value is
        // reported by marking every requested partition `INVALID_REQUEST`.
        //
        // This deliberately differs from Kafka, whose `IsolationLevel.forId` throws
        // `IllegalArgumentException` for an unknown id, so the client only sees
        // `UNKNOWN_SERVER_ERROR`. `INVALID_REQUEST` tells it what it got wrong.
        let isolation_level = match input.request.isolation_level.map_or(
            Ok(IsolationLevel::ReadUncommitted),
            IsolationLevel::try_from,
        ) {
            Ok(isolation_level) => isolation_level,

            Err(error) => {
                warn!(?error, isolation_level = ?input.request.isolation_level, "invalid isolation level, answering INVALID_REQUEST");

                return Ok(ListOffsetsResponse::default()
                    .throttle_time_ms(throttle_time_ms)
                    .topics(input.request.topics.map(|topics| {
                        topics
                            .into_iter()
                            .map(|topic| {
                                ListOffsetsTopicResponse::default()
                                    .name(topic.name)
                                    .partitions(topic.partitions.map(|partitions| {
                                        partitions
                                            .into_iter()
                                            .map(|partition| {
                                                ListOffsetsPartitionResponse::default()
                                                    .partition_index(partition.partition_index)
                                                    .error_code(ErrorCode::InvalidRequest.into())
                                                    .old_style_offsets(None)
                                                    .timestamp(Some(-1))
                                                    .offset(Some(0))
                                                    .leader_epoch(Some(0))
                                            })
                                            .collect()
                                    }))
                            })
                            .collect()
                    })));
            }
        };

        let topics = if let Some(topics) = input.request.topics {
            let mut offsets = vec![];

            // An out of range `timestamp` is scoped to its own partition (unlike
            // `isolation_level` above), so only that partition is marked
            // `INVALID_TIMESTAMP`; every other requested partition still resolves
            // normally through storage.
            let mut invalid_timestamps = vec![];

            for topic in topics {
                if let Some(ref partitions) = topic.partitions {
                    for partition in partitions {
                        let tp = Topition::new(topic.name.clone(), partition.partition_index);

                        match ListOffset::try_from(partition.timestamp) {
                            Ok(offset) => offsets.push((tp, offset)),

                            Err(error) => {
                                debug!(
                                    ?error,
                                    ?tp,
                                    timestamp = partition.timestamp,
                                    "invalid timestamp, answering INVALID_TIMESTAMP"
                                );

                                invalid_timestamps.push((
                                    tp,
                                    ListOffsetResponse {
                                        error_code: ErrorCode::InvalidTimestamp,
                                        ..Default::default()
                                    },
                                ));
                            }
                        }
                    }
                }
            }

            // One line per request, not per partition, so a large request of bad timestamps
            // can't flood the log; each entry is at `debug!` above.
            if let Some((tp, _)) = invalid_timestamps.first() {
                warn!(
                    count = invalid_timestamps.len(),
                    first = ?tp,
                    "invalid timestamps, answering INVALID_TIMESTAMP"
                );
            }

            let mut offsets = ListOffsetsReadShare::of(&input.extensions)
                .scope(self.list_offsets(isolation_level, &offsets))
                .await;
            debug!(?offsets);

            offsets.extend(invalid_timestamps);

            Some(
                offsets
                    .iter()
                    .fold(BTreeSet::new(), |mut topics, (topition, _)| {
                        _ = topics.insert(topition.topic());
                        topics
                    })
                    .iter()
                    .map(|topic_name| {
                        ListOffsetsTopicResponse::default()
                            .name((*topic_name).into())
                            .partitions(Some(
                                offsets
                                    .iter()
                                    .filter_map(|(topition, offset)| {
                                        if topition.topic() == *topic_name {
                                            Some(
                                                ListOffsetsPartitionResponse::default()
                                                    .partition_index(topition.partition())
                                                    .error_code(offset.error_code().into())
                                                    // Only present on the wire for a v0
                                                    // request (`OldStyleOffsets` is
                                                    // `"versions": "0"` in
                                                    // `ListOffsetsResponse.json`); the
                                                    // encoder drops this field for any
                                                    // later version based on the request's
                                                    // own negotiated version, so populating
                                                    // it unconditionally is safe for v1+.
                                                    .old_style_offsets(Some(
                                                        offset.offset().into_iter().collect(),
                                                    ))
                                                    .timestamp(
                                                        offset
                                                            .timestamp()
                                                            .unwrap_or(Some(-1))
                                                            .or(Some(-1)),
                                                    )
                                                    // `None` is sent as -1, as Kafka does:
                                                    // [KafkaApis.scala#L1171-L1177].
                                                    //
                                                    // [KafkaApis.scala#L1171-L1177]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1171-L1177
                                                    .offset(offset.offset().or(Some(-1)))
                                                    // Kafka sends -1 (the schema default) when
                                                    // the offset is unknown; nisshi always sends
                                                    // 0. Clients ignore the epoch for offset -1.
                                                    .leader_epoch(Some(0)),
                                            )
                                        } else {
                                            None
                                        }
                                    })
                                    .collect(),
                            ))
                    })
                    .collect(),
            )
        } else {
            None
        };

        Ok(ListOffsetsResponse::default()
            .throttle_time_ms(throttle_time_ms)
            .topics(topics))
        .inspect(|r| debug!(?r))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        future::pending,
        sync::{Arc, Mutex},
        time::SystemTime,
    };

    use async_trait::async_trait;
    use nisshi_sans_io::{
        ConfigResource, ErrorCode, IsolationLevel, ListOffset, ListOffsetsRequest, RequestInput,
        ScramMechanism,
        create_topics_request::CreatableTopic,
        delete_groups_response::DeletableGroupResult,
        delete_records_request::DeleteRecordsTopic,
        delete_records_response::DeleteRecordsTopicResult,
        describe_cluster_response::DescribeClusterBroker,
        describe_configs_response::DescribeConfigsResult,
        describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
        fetch_response::AbortedTransaction,
        incremental_alter_configs_request::AlterConfigsResource,
        incremental_alter_configs_response::AlterConfigsResourceResponse,
        list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
        list_offsets_response::ListOffsetsPartitionResponse,
        record::deflated,
        to_system_time,
        txn_offset_commit_response::TxnOffsetCommitResponseTopic,
    };
    use rama::{Service as _, extensions::Extensions};
    use tokio::{
        sync::OwnedSemaphorePermit,
        time::{Duration, Instant, timeout},
    };
    use url::Url;
    use uuid::Uuid;

    use super::{LIST_OFFSETS_CONCURRENCY, ListOffsetsService};
    use crate::{
        BrokerRegistrationRequest, CommittedOffset, Error, GroupDetail, ListOffsetResponse,
        MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse,
        Result, ScramCredential, SemaphoreProxy, Storage, TopicId, Topition,
        TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError,
        Version,
        service::{
            RequestChannelService, bounded_channel,
            deadline::{LIST_OFFSETS_READ_DEADLINE, Missed, within},
            read_share::ListOffsetsReadShare,
        },
    };

    const TOPIC: &str = "abc";

    /// Fails a test that would otherwise hang on a storage read that never
    /// finishes, as every stalled read did before the deadline.
    const HANG: Duration = Duration::from_secs(60);

    #[derive(Clone, Copy, Debug)]
    enum Answer {
        Offset(i64),
        /// An offset and the timestamp, in milliseconds, of its record.
        OffsetAt(i64, i64),
        /// An offset, read with a permit from a read share of one permit
        /// that the read keeps, as an abandoned read keeps its permit until
        /// its statement ends.
        OffsetKeepingReadShare(i64),
        Stall,
        Fail,
        FailWith(ErrorCode),
    }

    /// Records the partition of a stalled storage read when nisshi drops it.
    #[derive(Debug)]
    struct Dropped {
        partition: i32,
        dropped: Arc<Mutex<Vec<i32>>>,
    }

    impl Drop for Dropped {
        fn drop(&mut self) {
            if let Ok(mut dropped) = self.dropped.lock() {
                dropped.push(self.partition);
            }
        }
    }

    /// Storage answering `list_offsets` per partition: an offset, a read that
    /// never finishes, or an error. Records each call and each dropped read.
    #[derive(Clone, Debug, Default)]
    struct Stub {
        partitions: BTreeMap<i32, Answer>,
        calls: Arc<Mutex<Vec<i32>>>,
        dropped: Arc<Mutex<Vec<i32>>>,
        kept_read_shares: Arc<Mutex<Vec<OwnedSemaphorePermit>>>,
    }

    impl Stub {
        fn new(answers: &[Answer]) -> Self {
            Self {
                partitions: (0..).zip(answers.iter().copied()).collect(),
                ..Self::default()
            }
        }

        fn calls(&self) -> Vec<i32> {
            let mut calls = self.calls.lock().expect("calls").clone();
            calls.sort_unstable();
            calls
        }

        fn dropped(&self) -> Vec<i32> {
            let mut dropped = self.dropped.lock().expect("dropped").clone();
            dropped.sort_unstable();
            dropped
        }
    }

    #[async_trait]
    impl Storage for Stub {
        async fn fetch(
            &self,
            _topition: &Topition,
            _offset: i64,
            _min_bytes: u32,
            _max_bytes: u32,
            _isolation_level: IsolationLevel,
            _max_wait: Duration,
        ) -> Result<Vec<deflated::Batch>> {
            unimplemented!()
        }

        async fn offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
            unimplemented!()
        }

        async fn list_offsets(
            &self,
            _isolation_level: IsolationLevel,
            offsets: &[(Topition, ListOffset)],
        ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
            let [(topition, _)] = offsets else {
                return Err(Error::Message(format!(
                    "expected one partition per storage call, got {offsets:?}"
                )));
            };

            let partition = topition.partition();
            self.calls.lock()?.push(partition);

            match self.partitions.get(&partition) {
                Some(Answer::Offset(offset)) => Ok(vec![(
                    topition.clone(),
                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        timestamp: None,
                        offset: Some(*offset),
                    },
                )]),

                Some(Answer::OffsetAt(offset, timestamp)) => Ok(vec![(
                    topition.clone(),
                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        timestamp: Some(to_system_time(*timestamp)?),
                        offset: Some(*offset),
                    },
                )]),

                Some(Answer::OffsetKeepingReadShare(offset)) => {
                    let permit = ListOffsetsReadShare::acquire(1).await?.ok_or_else(|| {
                        Error::Message("the read ran outside a read share".into())
                    })?;
                    self.kept_read_shares.lock()?.push(permit);

                    Ok(vec![(
                        topition.clone(),
                        ListOffsetResponse {
                            error_code: ErrorCode::None,
                            timestamp: None,
                            offset: Some(*offset),
                        },
                    )])
                }

                Some(Answer::Stall) => {
                    let _guard = Dropped {
                        partition,
                        dropped: self.dropped.clone(),
                    };

                    pending().await
                }

                Some(Answer::FailWith(error_code)) => Err(Error::Api(*error_code)),

                Some(Answer::Fail) | None => Err(Error::Message(format!(
                    "storage failed for partition {partition}"
                ))),
            }
        }

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

        async fn metadata(&self, _topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
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

    /// Asks for the latest offset of `partitions` partitions of one topic.
    async fn list_offsets(
        storage: Stub,
        partitions: i32,
    ) -> Result<Vec<ListOffsetsPartitionResponse>> {
        list_offsets_on_connection(storage, partitions, Extensions::default()).await
    }

    /// As [`list_offsets`], on the connection that `extensions` belongs to.
    async fn list_offsets_on_connection(
        storage: Stub,
        partitions: i32,
        extensions: Extensions,
    ) -> Result<Vec<ListOffsetsPartitionResponse>> {
        let request = ListOffsetsRequest::default()
            .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
            .replica_id(-1)
            .topics(Some(vec![
                ListOffsetsTopic::default()
                    .name(TOPIC.into())
                    .partitions(Some(
                        (0..partitions)
                            .map(|partition| {
                                ListOffsetsPartition::default()
                                    .partition_index(partition)
                                    .current_leader_epoch(Some(-1))
                                    .timestamp(ListOffset::Latest.try_into().expect("latest"))
                            })
                            .collect(),
                    )),
            ]));

        let response = timeout(
            HANG,
            ListOffsetsService { storage }.serve(RequestInput {
                request,
                extensions,
            }),
        )
        .await
        .map_err(|_| Error::Message("list offsets hung on a stalled read".into()))??;

        let topics = response.topics.unwrap_or_default();
        assert_eq!(1, topics.len());
        Ok(topics[0].partitions.clone().unwrap_or_default())
    }

    /// A ListOffsets's reads take their permits from the read share of the
    /// request's connection. A permit that one request's read keeps then
    /// holds back the reads of the connection's next request until the
    /// deadline, and not those of another connection.
    #[tokio::test(start_paused = true)]
    async fn requests_on_a_connection_share_its_read_share() -> Result<()> {
        let storage = Stub::new(&[Answer::OffsetKeepingReadShare(10)]);
        let listener = Extensions::default();
        let connection = listener.fork();

        assert_eq!(
            vec![(0, ErrorCode::None, Some(10))],
            answers(&list_offsets_on_connection(storage.clone(), 1, connection.clone()).await?)?
        );

        let started_at = Instant::now();
        assert_eq!(
            vec![(0, ErrorCode::RequestTimedOut, Some(-1))],
            answers(&list_offsets_on_connection(storage.clone(), 1, connection).await?)?
        );
        assert_eq!(LIST_OFFSETS_READ_DEADLINE, started_at.elapsed());

        assert_eq!(
            vec![(0, ErrorCode::None, Some(10))],
            answers(&list_offsets_on_connection(storage, 1, listener.fork()).await?)?
        );

        Ok(())
    }

    fn answers(
        partitions: &[ListOffsetsPartitionResponse],
    ) -> Result<Vec<(i32, ErrorCode, Option<i64>)>> {
        partitions
            .iter()
            .map(|partition| {
                ErrorCode::try_from(partition.error_code)
                    .map(|error_code| (partition.partition_index, error_code, partition.offset))
                    .map_err(Into::into)
            })
            .collect()
    }

    /// A partition that is slow in storage answers `REQUEST_TIMED_OUT` at
    /// the deadline, and the partitions either side of it get their offsets.
    #[tokio::test(start_paused = true)]
    async fn slow_partition_times_out_alone() -> Result<()> {
        let storage = Stub::new(&[Answer::Offset(10), Answer::Stall, Answer::Offset(30)]);

        let started_at = Instant::now();
        let partitions = list_offsets(storage.clone(), 3).await?;

        assert_eq!(LIST_OFFSETS_READ_DEADLINE, started_at.elapsed());
        assert_eq!(
            vec![
                (0, ErrorCode::None, Some(10)),
                (1, ErrorCode::RequestTimedOut, Some(-1)),
                (2, ErrorCode::None, Some(30)),
            ],
            answers(&partitions)?
        );
        assert_eq!(vec![0, 1, 2], storage.calls());
        assert_eq!(vec![1], storage.dropped());

        Ok(())
    }

    /// Without a slow partition, every partition is answered straight away,
    /// in the order asked.
    #[tokio::test(start_paused = true)]
    async fn answers_in_request_order() -> Result<()> {
        let offsets = [50, 40, 30, 20, 10, 0];
        let storage = Stub::new(&offsets.map(Answer::Offset));

        let started_at = Instant::now();
        let partitions = list_offsets(storage.clone(), 6).await?;

        assert_eq!(Duration::ZERO, started_at.elapsed());
        assert_eq!(
            (0..)
                .zip(offsets)
                .map(|(partition, offset)| (partition, ErrorCode::None, Some(offset)))
                .collect::<Vec<_>>(),
            answers(&partitions)?
        );

        Ok(())
    }

    /// At most `LIST_OFFSETS_CONCURRENCY` reads run at once, and a read not
    /// started by the deadline is never sent to storage.
    #[tokio::test(start_paused = true)]
    async fn bounded_and_not_started_after_the_deadline() -> Result<()> {
        let partitions = i32::try_from(LIST_OFFSETS_CONCURRENCY + 2)?;
        let storage = Stub::new(&vec![Answer::Stall; partitions as usize]);

        let started_at = Instant::now();
        let answered = list_offsets(storage.clone(), partitions).await?;

        assert_eq!(LIST_OFFSETS_READ_DEADLINE, started_at.elapsed());
        assert!(
            answers(&answered)?
                .iter()
                .all(|(_, error_code, _)| *error_code == ErrorCode::RequestTimedOut)
        );
        assert_eq!(
            (0..i32::try_from(LIST_OFFSETS_CONCURRENCY)?).collect::<Vec<_>>(),
            storage.calls()
        );
        assert_eq!(storage.calls(), storage.dropped());

        Ok(())
    }

    /// `Earliest` and `Latest` answer timestamp -1, as Kafka does, whatever
    /// timestamp storage returns. A `Timestamp` lookup answers the timestamp
    /// of the record it matched.
    #[tokio::test]
    async fn earliest_and_latest_answer_no_timestamp() -> Result<()> {
        let timestamp = 1_577_836_800_000;
        let storage = Stub::new(&[Answer::OffsetAt(7, timestamp); 3]);

        let request = ListOffsetsRequest::default()
            .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
            .replica_id(-1)
            .topics(Some(vec![
                ListOffsetsTopic::default()
                    .name(TOPIC.into())
                    .partitions(Some(
                        (0..)
                            .zip([
                                ListOffset::Earliest,
                                ListOffset::Latest,
                                ListOffset::Timestamp(to_system_time(timestamp)?),
                            ])
                            .map(|(partition, list_offset)| {
                                list_offset.try_into().map(|timestamp| {
                                    ListOffsetsPartition::default()
                                        .partition_index(partition)
                                        .current_leader_epoch(Some(-1))
                                        .timestamp(timestamp)
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    )),
            ]));

        let response = ListOffsetsService { storage }.serve(request).await?;

        let topics = response.topics.unwrap_or_default();
        assert_eq!(1, topics.len());

        assert_eq!(
            vec![
                (0, Some(7), Some(-1)),
                (1, Some(7), Some(-1)),
                (2, Some(7), Some(timestamp)),
            ],
            topics[0]
                .partitions
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|partition| (
                    partition.partition_index,
                    partition.offset,
                    partition.timestamp
                ))
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    /// A storage error answers `KAFKA_STORAGE_ERROR` in its own partition, an
    /// API error answers its own code, and the other partitions still answer,
    /// beside one that times out.
    #[tokio::test(start_paused = true)]
    async fn storage_error_answers_in_its_partition() -> Result<()> {
        let storage = Stub::new(&[
            Answer::Offset(10),
            Answer::Fail,
            Answer::Offset(30),
            Answer::FailWith(ErrorCode::NotLeaderOrFollower),
            Answer::Stall,
        ]);

        let partitions = list_offsets(storage.clone(), 5).await?;

        assert_eq!(
            vec![
                (0, ErrorCode::None, Some(10)),
                (1, ErrorCode::KafkaStorageError, Some(-1)),
                (2, ErrorCode::None, Some(30)),
                (3, ErrorCode::NotLeaderOrFollower, Some(-1)),
                (4, ErrorCode::RequestTimedOut, Some(-1)),
            ],
            answers(&partitions)?
        );
        assert!(
            partitions
                .iter()
                .all(|partition| partition.timestamp == Some(-1)),
            "{partitions:?}"
        );
        assert_eq!(vec![0, 1, 2, 3, 4], storage.calls());

        Ok(())
    }

    /// A read abandoned while it waits for the storage proxy's permit is a
    /// queued miss, so a libSQL engine in `semaphore` mode whose permit
    /// Produce and Fetch hold shows as queued, not as a slow engine.
    #[tokio::test(start_paused = true)]
    async fn read_waiting_for_the_proxy_permit_is_queued() -> Result<()> {
        let proxy = SemaphoreProxy::new(Stub::new(&[Answer::Stall, Answer::Stall]));

        let permit_holder = {
            let proxy = proxy.clone();

            tokio::spawn(async move {
                proxy
                    .list_offsets(
                        IsolationLevel::ReadUncommitted,
                        &[(Topition::new(TOPIC, 0), ListOffset::Latest)],
                    )
                    .await
            })
        };

        // The stalled read holds the proxy's only permit once it has run.
        tokio::time::sleep(Duration::from_millis(1)).await;

        let outcome = within(
            "test",
            Instant::now() + Duration::from_secs(1),
            proxy.list_offsets(
                IsolationLevel::ReadUncommitted,
                &[(Topition::new(TOPIC, 1), ListOffset::Latest)],
            ),
        )
        .await;
        assert!(matches!(outcome, Err(Missed::Queued)), "{outcome:?}");

        permit_holder.abort();

        Ok(())
    }

    /// A read abandoned while it waits for room on the storage request
    /// channel is a queued miss, so a libSQL engine in `mpsc` mode behind
    /// other requests shows as queued, not as a slow engine.
    #[tokio::test(start_paused = true)]
    async fn read_waiting_for_the_request_channel_is_queued() -> Result<()> {
        let (tx, _rx) = bounded_channel(1);
        let _only_slot = tx.reserve().await.map_err(|_| Error::UnableToSend)?;

        let service = RequestChannelService::new(tx.clone());

        let outcome = within(
            "test",
            Instant::now() + Duration::from_secs(1),
            service.list_offsets(
                IsolationLevel::ReadUncommitted,
                &[(Topition::new(TOPIC, 0), ListOffset::Latest)],
            ),
        )
        .await;
        assert!(matches!(outcome, Err(Missed::Queued)), "{outcome:?}");

        Ok(())
    }
}
