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

use std::slice;

use futures::{StreamExt as _, stream};

use nisshi_sans_io::{
    ApiKey, DeleteRecordsRequest, DeleteRecordsResponse, ErrorCode, RequestInput,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
};
use rama::Service;
use tokio::time::Instant;
use tracing::{debug, error, instrument, warn};

use crate::{
    DELETE_RECORDS_INVALID_LOW_WATERMARK, Error, Result, Storage, StorageOperation, Topition,
    service::{
        deadline::{DELETE_RECORDS_DEADLINE, started_before},
        storage_error_code,
    },
};

/// How many partitions of one DeleteRecords request are in storage at once.
///
/// The value is the one of `LIST_OFFSETS_CONCURRENCY`.
/// A request takes up to this many connections of an engine's pool, and an
/// engine that serialises its writes (libSQL by default) gains nothing from
/// it, but loses nothing either.
const DELETE_RECORDS_CONCURRENCY: usize = 4;

/// How many failed partitions one log line names.
const PARTITION_SAMPLE: usize = 8;

/// A [`Service`] using its [`Storage`] taking [`DeleteRecordsRequest`] returning [`DeleteRecordsResponse`].
#[derive(Clone, Debug)]
pub struct DeleteRecordsService<G> {
    pub storage: G,
}

impl<G> ApiKey for DeleteRecordsService<G> {
    const KEY: i16 = DeleteRecordsRequest::KEY;
}

impl<G> DeleteRecordsService<G>
where
    G: Storage,
{
    /// Deletes the records of each partition with its own storage call, up to
    /// [`DELETE_RECORDS_CONCURRENCY`] at a time, so that a storage error
    /// answers in the partition that it belongs to (see
    /// [`storage_error_code`]) and the other partitions still answer. Kafka
    /// also answers each partition's failure in that partition
    /// ([ReplicaManager.scala#L1122-L1146]).
    ///
    /// An engine can fail a whole call, as libSQL, Turso and DynoStore do,
    /// and a call with every partition would then fail partitions whose
    /// records were already deleted.
    ///
    /// One deadline, [`DELETE_RECORDS_DEADLINE`], bounds when the calls
    /// start. A partition whose call has not started at the deadline answers
    /// `REQUEST_TIMED_OUT`, so a degraded engine does not hold the request
    /// past the client's own timeout, one engine timeout per partition. The
    /// Java admin client then resends only the partitions that timed out
    /// ([DeleteRecordsHandler.java#L147-L165]), so a request with more
    /// partitions than one round serves still completes.
    ///
    /// A call that has started runs to its end, so the request takes longer
    /// than the deadline by at most the longest call of the last calls to
    /// start. The service does not drop a started call, because a write that
    /// its caller drops can leave its storage engine's connection busy for
    /// the next caller. For that reason the loop below reads the stream to
    /// its end and never breaks out of it.
    ///
    /// [ReplicaManager.scala#L1122-L1146]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1122-L1146
    /// [DeleteRecordsHandler.java#L147-L165]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/admin/internals/DeleteRecordsHandler.java#L147-L165
    async fn delete_records(&self, topics: &[DeleteRecordsTopic]) -> Vec<DeleteRecordsTopicResult> {
        let started_at = Instant::now();
        let deadline = started_at + DELETE_RECORDS_DEADLINE;

        // The calls are built up front (but not started) rather than in a
        // `StreamExt::map` closure, whose lifetimes keep `serve` from being
        // `Send`. A call checks the deadline when the stream first polls it,
        // that is when one of the running calls ends and frees its place.
        let calls = topics
            .iter()
            .enumerate()
            .flat_map(|(topic_index, topic)| {
                topic.partitions.iter().flatten().enumerate().map(
                    move |(partition_index, partition)| {
                        (topic_index, partition_index, topic, partition)
                    },
                )
            })
            .map(
                |(topic_index, partition_index, topic, partition)| async move {
                    let request = DeleteRecordsTopic::default()
                        .name(topic.name.clone())
                        .partitions(Some(vec![partition.clone()]));

                    let answer = started_before(
                        StorageOperation::DeleteRecords,
                        deadline,
                        self.storage.delete_records(slice::from_ref(&request)),
                    )
                    .await;

                    (topic_index, partition_index, topic, partition, answer)
                },
            )
            .collect::<Vec<_>>();

        let mut answers = stream::iter(calls).buffer_unordered(DELETE_RECORDS_CONCURRENCY);

        // One slot for each partition of the request, so that the response
        // keeps the order of the request whatever order the calls end in.
        let mut slots = topics
            .iter()
            .map(|topic| vec![Vec::new(); topic.partitions.as_ref().map_or(0, Vec::len)])
            .collect::<Vec<_>>();

        let mut failed = Vec::new();
        let mut not_started = Vec::new();
        let mut first_error = None;

        while let Some((topic_index, partition_index, topic, partition, answer)) =
            answers.next().await
        {
            slots[topic_index][partition_index] = match answer {
                Ok(Ok(answer)) => answer
                    .into_iter()
                    .flat_map(|topic| topic.partitions.unwrap_or_default())
                    .collect(),

                Ok(Err(error)) => {
                    let topition = Topition::new(topic.name.as_str(), partition.partition_index);
                    let error_code = storage_error_code(&error);
                    debug!(?error, ?topition, ?error_code, "delete records");

                    failed.push((topition, error_code));
                    _ = first_error.get_or_insert(error);

                    vec![
                        DeleteRecordsPartitionResult::default()
                            .partition_index(partition.partition_index)
                            .low_watermark(DELETE_RECORDS_INVALID_LOW_WATERMARK)
                            .error_code(error_code.into()),
                    ]
                }

                Err(_not_started) => {
                    not_started.push(Topition::new(
                        topic.name.as_str(),
                        partition.partition_index,
                    ));

                    vec![
                        DeleteRecordsPartitionResult::default()
                            .partition_index(partition.partition_index)
                            .low_watermark(DELETE_RECORDS_INVALID_LOW_WATERMARK)
                            .error_code(ErrorCode::RequestTimedOut.into()),
                    ]
                }
            };
        }

        let responses = topics
            .iter()
            .zip(slots)
            .map(|(topic, partitions)| {
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partitions.into_iter().flatten().collect()))
            })
            .collect();

        if !not_started.is_empty() {
            warn!(
                deadline = ?DELETE_RECORDS_DEADLINE,
                elapsed = ?started_at.elapsed(),
                not_started = not_started.len(),
                sample = ?&not_started[..not_started.len().min(PARTITION_SAMPLE)],
                "delete records answered REQUEST_TIMED_OUT for partitions not started at the deadline"
            );
        }

        // One line per request, not per partition, so an unavailable storage
        // engine can't flood the log with a line for every partition of every
        // retry; each partition is at `debug!` above.
        if let Some(first_error) = first_error {
            error!(
                failed = failed.len(),
                sample = ?&failed[..failed.len().min(PARTITION_SAMPLE)],
                ?first_error,
                "delete records answered an error for partitions whose storage call failed"
            );
        }

        responses
    }
}

impl<G, I> Service<I> for DeleteRecordsService<G>
where
    G: Storage,
    I: Into<RequestInput<DeleteRecordsRequest>> + Send + 'static,
{
    type Output = DeleteRecordsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        Ok(DeleteRecordsResponse::default()
            .throttle_time_ms(0)
            .topics(Some(
                self.delete_records(input.request.topics.as_deref().unwrap_or_default())
                    .await,
            )))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, SystemTime},
    };

    use async_trait::async_trait;
    use nisshi_sans_io::{
        ConfigResource, DeleteRecordsRequest, ErrorCode, IsolationLevel, ListOffset, RequestInput,
        ScramMechanism,
        create_topics_request::CreatableTopic,
        delete_groups_response::DeletableGroupResult,
        delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
        delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
        describe_cluster_response::DescribeClusterBroker,
        describe_configs_response::DescribeConfigsResult,
        describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
        fetch_response::AbortedTransaction,
        incremental_alter_configs_request::AlterConfigsResource,
        incremental_alter_configs_response::AlterConfigsResourceResponse,
        record::deflated,
        txn_offset_commit_response::TxnOffsetCommitResponseTopic,
    };
    use rama::{Service as _, extensions::Extensions};
    use url::Url;
    use uuid::Uuid;

    use super::{DELETE_RECORDS_CONCURRENCY, DeleteRecordsService};
    use crate::{
        BrokerRegistrationRequest, CommittedOffset, DELETE_RECORDS_INVALID_LOW_WATERMARK, Error,
        GroupDetail, ListOffsetResponse, MetadataResponse, NamedGroupDetail, OffsetCommitRequest,
        OffsetStage, ProducerIdResponse, Result, ScramCredential, Storage, TopicId, Topition,
        TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError,
        Version, service::deadline::DELETE_RECORDS_DEADLINE,
    };

    /// What the stub engine does with a partition.
    #[derive(Clone, Copy, Debug)]
    enum Answer {
        /// Moves the log start to this offset.
        Cut(i64),
        /// Fails the call with an error that has no Kafka code and that a
        /// retry may fix, as an unavailable database or object store does.
        Fail,
        /// Fails the call with an error that has no Kafka code and that a
        /// retry can't fix.
        FailPermanently,
        /// Fails the call with this code.
        Api(ErrorCode),
    }

    /// The topics and partitions of one `delete_records` call.
    type Call = Vec<(String, Vec<i32>)>;

    /// An engine that fails its whole `delete_records` call when any
    /// partition in it fails, as libSQL, Turso and DynoStore do.
    #[derive(Clone, Debug, Default)]
    struct Stub {
        answers: BTreeMap<(String, i32), Answer>,
        calls: Arc<Mutex<Vec<Call>>>,
        delay: Duration,
        /// The delay of a partition's call, for a partition that does not
        /// take `delay`.
        partition_delays: BTreeMap<(String, i32), Duration>,
        /// Calls in storage now, the most at once, and those that ran to
        /// their end.
        running: Arc<AtomicUsize>,
        most_running: Arc<AtomicUsize>,
        completed: Arc<AtomicUsize>,
    }

    impl Stub {
        fn new(answers: &[(&str, i32, Answer)]) -> Self {
            Self {
                answers: answers
                    .iter()
                    .map(|(topic, partition, answer)| (((*topic).to_owned(), *partition), *answer))
                    .collect(),
                calls: Arc::default(),
                delay: Duration::ZERO,
                partition_delays: BTreeMap::new(),
                running: Arc::default(),
                most_running: Arc::default(),
                completed: Arc::default(),
            }
        }

        /// Returns a stub whose each `delete_records` call takes `delay`
        /// to answer, as a degraded engine does.
        fn with_delay(self, delay: Duration) -> Self {
            Self { delay, ..self }
        }

        /// Returns a stub whose call for each listed partition takes its
        /// own delay, so that calls that start together end in an order of
        /// the test's choosing.
        fn with_partition_delays(self, delays: &[(&str, i32, Duration)]) -> Self {
            Self {
                partition_delays: delays
                    .iter()
                    .map(|(topic, partition, delay)| (((*topic).to_owned(), *partition), *delay))
                    .collect(),
                ..self
            }
        }

        /// Returns how long the call for `topics` takes: the delay of its
        /// first partition when it has its own, and `delay` otherwise.
        fn delay_of(&self, topics: &[DeleteRecordsTopic]) -> Duration {
            topics
                .first()
                .and_then(|topic| {
                    topic
                        .partitions
                        .iter()
                        .flatten()
                        .next()
                        .and_then(|partition| {
                            self.partition_delays
                                .get(&(topic.name.clone(), partition.partition_index))
                        })
                })
                .copied()
                .unwrap_or(self.delay)
        }

        /// The topics and partitions of each `delete_records` call.
        fn calls(&self) -> Vec<Call> {
            let mut calls = self.calls.lock().expect("calls").clone();
            calls.sort();
            calls
        }

        fn delete(&self, topics: &[DeleteRecordsTopic]) -> Result<Vec<DeleteRecordsTopicResult>> {
            self.calls.lock().expect("calls").push(
                topics
                    .iter()
                    .map(|topic| {
                        (
                            topic.name.clone(),
                            topic
                                .partitions
                                .iter()
                                .flatten()
                                .map(|partition| partition.partition_index)
                                .collect(),
                        )
                    })
                    .collect(),
            );

            topics
                .iter()
                .map(|topic| {
                    topic
                        .partitions
                        .iter()
                        .flatten()
                        .map(|partition| {
                            match self.answers[&(topic.name.clone(), partition.partition_index)] {
                                Answer::Cut(offset) => Ok(DeleteRecordsPartitionResult::default()
                                    .partition_index(partition.partition_index)
                                    .low_watermark(offset)
                                    .error_code(ErrorCode::None.into())),
                                Answer::Fail => {
                                    Err(Error::from(std::io::Error::other("unavailable")))
                                }
                                Answer::FailPermanently => {
                                    Err(Error::Message(String::from("can't delete records")))
                                }
                                Answer::Api(error_code) => Err(Error::Api(error_code)),
                            }
                        })
                        .collect::<Result<Vec<_>>>()
                        .map(|partitions| {
                            DeleteRecordsTopicResult::default()
                                .name(topic.name.clone())
                                .partitions(Some(partitions))
                        })
                })
                .collect()
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
            _offsets: &[(Topition, ListOffset)],
        ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
            unimplemented!()
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
            topics: &[DeleteRecordsTopic],
        ) -> Result<Vec<DeleteRecordsTopicResult>> {
            let answer = self.delete(topics);

            let running = self.running.fetch_add(1, Ordering::SeqCst) + 1;
            _ = self.most_running.fetch_max(running, Ordering::SeqCst);
            tokio::time::sleep(self.delay_of(topics)).await;
            _ = self.running.fetch_sub(1, Ordering::SeqCst);
            _ = self.completed.fetch_add(1, Ordering::SeqCst);

            answer
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

    fn topic(name: &str, partitions: Option<&[i32]>) -> DeleteRecordsTopic {
        DeleteRecordsTopic::default()
            .name(name.into())
            .partitions(partitions.map(|partitions| {
                partitions
                    .iter()
                    .map(|partition| {
                        DeleteRecordsPartition::default()
                            .partition_index(*partition)
                            .offset(-1)
                    })
                    .collect()
            }))
    }

    async fn delete_records(
        stub: &Stub,
        topics: Vec<DeleteRecordsTopic>,
    ) -> Result<Vec<(String, Option<Vec<(i32, ErrorCode, i64)>>)>> {
        let response = DeleteRecordsService {
            storage: stub.clone(),
        }
        .serve(RequestInput {
            request: DeleteRecordsRequest::default()
                .topics(Some(topics))
                .timeout_ms(5_000),
            extensions: Extensions::default(),
        })
        .await?;

        response
            .topics
            .unwrap_or_default()
            .into_iter()
            .map(|topic| {
                topic
                    .partitions
                    .map(|partitions| {
                        partitions
                            .into_iter()
                            .map(|partition| {
                                ErrorCode::try_from(partition.error_code)
                                    .map(|error_code| {
                                        (
                                            partition.partition_index,
                                            error_code,
                                            partition.low_watermark,
                                        )
                                    })
                                    .map_err(Into::into)
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()
                    .map(|partitions| (topic.name, partitions))
            })
            .collect()
    }

    /// A storage error answers in its own partition: `KAFKA_STORAGE_ERROR`
    /// for a transient error without a Kafka code, `UNKNOWN_SERVER_ERROR` for a
    /// permanent one, the code of an `Error::Api`, and the partitions around it
    /// still answer their cut.
    #[tokio::test]
    async fn storage_error_answers_in_its_partition() -> Result<()> {
        let stub = Stub::new(&[
            ("a", 0, Answer::Cut(5)),
            ("a", 1, Answer::Fail),
            ("a", 2, Answer::Api(ErrorCode::NotLeaderOrFollower)),
            ("a", 3, Answer::FailPermanently),
            ("b", 0, Answer::Cut(7)),
        ]);

        assert_eq!(
            vec![
                (
                    String::from("a"),
                    Some(vec![
                        (0, ErrorCode::None, 5),
                        (
                            1,
                            ErrorCode::KafkaStorageError,
                            DELETE_RECORDS_INVALID_LOW_WATERMARK
                        ),
                        (
                            2,
                            ErrorCode::NotLeaderOrFollower,
                            DELETE_RECORDS_INVALID_LOW_WATERMARK
                        ),
                        (
                            3,
                            ErrorCode::UnknownServerError,
                            DELETE_RECORDS_INVALID_LOW_WATERMARK
                        ),
                    ])
                ),
                (String::from("b"), Some(vec![(0, ErrorCode::None, 7)])),
            ],
            delete_records(
                &stub,
                vec![topic("a", Some(&[0, 1, 2, 3])), topic("b", Some(&[0]))]
            )
            .await?
        );

        // one partition to each call
        assert_eq!(
            vec![
                vec![(String::from("a"), vec![0])],
                vec![(String::from("a"), vec![1])],
                vec![(String::from("a"), vec![2])],
                vec![(String::from("a"), vec![3])],
                vec![(String::from("b"), vec![0])],
            ],
            stub.calls()
        );

        Ok(())
    }

    /// Each call takes 2s and the deadline is 5s. Partitions run four at a
    /// time, so 8 partitions all start before the deadline (at 0s and 2s)
    /// and finish at 4s. One at a time, only 3 would have started before the
    /// deadline and the other 5 would have answered `REQUEST_TIMED_OUT`.
    #[tokio::test(start_paused = true)]
    async fn many_partitions_finish_within_the_deadline() -> Result<()> {
        let partitions = (0..8).collect::<Vec<_>>();
        let stub = Stub::new(
            &partitions
                .iter()
                .map(|partition| ("a", *partition, Answer::Cut(i64::from(*partition) + 10)))
                .collect::<Vec<_>>(),
        )
        .with_delay(Duration::from_secs(2));

        let started_at = tokio::time::Instant::now();

        assert_eq!(
            vec![(
                String::from("a"),
                Some(
                    partitions
                        .iter()
                        .map(|partition| (*partition, ErrorCode::None, i64::from(*partition) + 10))
                        .collect()
                )
            )],
            delete_records(&stub, vec![topic("a", Some(&partitions))]).await?
        );

        assert_eq!(Duration::from_secs(4), started_at.elapsed());
        assert_eq!(8, stub.calls().len());

        Ok(())
    }

    /// The response lists the topics and partitions in the order of the
    /// request, and each partition answers its own cut, whatever order the
    /// calls end in.
    ///
    /// The four calls start together and end in the reverse of the request
    /// order within each topic, and the second topic's first call ends before
    /// the first topic's.
    #[tokio::test(start_paused = true)]
    async fn response_keeps_the_request_order_when_calls_end_out_of_order() -> Result<()> {
        let stub = Stub::new(&[
            ("a", 0, Answer::Cut(10)),
            ("a", 1, Answer::Cut(11)),
            ("b", 0, Answer::Cut(20)),
            ("b", 1, Answer::Cut(21)),
        ])
        .with_partition_delays(&[
            ("a", 0, Duration::from_secs(4)),
            ("a", 1, Duration::from_secs(1)),
            ("b", 0, Duration::from_secs(3)),
            ("b", 1, Duration::from_secs(2)),
        ]);

        assert_eq!(
            vec![
                (
                    String::from("a"),
                    Some(vec![(0, ErrorCode::None, 10), (1, ErrorCode::None, 11)])
                ),
                (
                    String::from("b"),
                    Some(vec![(0, ErrorCode::None, 20), (1, ErrorCode::None, 21)])
                ),
            ],
            delete_records(
                &stub,
                vec![topic("a", Some(&[0, 1])), topic("b", Some(&[0, 1]))]
            )
            .await?
        );

        Ok(())
    }

    /// No more than [`DELETE_RECORDS_CONCURRENCY`] calls are in storage at
    /// once, and that many are.
    #[tokio::test(start_paused = true)]
    async fn partitions_run_at_most_the_bound_at_once() -> Result<()> {
        let partitions = (0..10).collect::<Vec<_>>();
        let stub = Stub::new(
            &partitions
                .iter()
                .map(|partition| ("a", *partition, Answer::Cut(1)))
                .collect::<Vec<_>>(),
        )
        .with_delay(Duration::from_secs(1));

        _ = delete_records(&stub, vec![topic("a", Some(&partitions))]).await?;

        assert_eq!(4, DELETE_RECORDS_CONCURRENCY);
        assert_eq!(4, stub.most_running.load(Ordering::SeqCst));

        Ok(())
    }

    /// Each call takes 2s and the deadline is 5s, so with 20 partitions the
    /// calls start at 0s, 2s and 4s, 12 of them. The 13th partition would
    /// start at 6s, after the deadline, so it and the 7 after it are never
    /// called and answer `REQUEST_TIMED_OUT`. The calls that started at 4s
    /// run to their end at 6s: none is dropped.
    #[tokio::test(start_paused = true)]
    async fn partitions_not_started_at_the_deadline_time_out() -> Result<()> {
        let partitions = (0..20).collect::<Vec<_>>();
        let stub = Stub::new(
            &partitions
                .iter()
                .map(|partition| ("a", *partition, Answer::Cut(1)))
                .collect::<Vec<_>>(),
        )
        .with_delay(Duration::from_secs(2));

        let started_at = tokio::time::Instant::now();

        let answers = delete_records(&stub, vec![topic("a", Some(&partitions))]).await?;

        let (_, answered) = &answers[0];
        let answered = answered.as_ref().expect("partitions");

        assert_eq!(
            partitions.clone(),
            answered
                .iter()
                .map(|(partition, ..)| *partition)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            12,
            answered
                .iter()
                .filter(|(_, error_code, _)| *error_code == ErrorCode::None)
                .count()
        );
        assert!(answered[12..].iter().all(|answer| *answer
            == (
                answer.0,
                ErrorCode::RequestTimedOut,
                DELETE_RECORDS_INVALID_LOW_WATERMARK
            )));

        // the last calls started 1s before the deadline and ran 1s past it
        assert_eq!(
            DELETE_RECORDS_DEADLINE + Duration::from_secs(1),
            started_at.elapsed()
        );

        assert_eq!(12, stub.calls().len());
        assert_eq!(12, stub.completed.load(Ordering::SeqCst));

        Ok(())
    }

    /// A topic without partitions answers an empty partition list, as the
    /// engines do, and calls no engine.
    #[tokio::test]
    async fn topic_without_partitions_answers_an_empty_list() -> Result<()> {
        let stub = Stub::new(&[]);

        assert_eq!(
            vec![(String::from("a"), Some(vec![]))],
            delete_records(&stub, vec![topic("a", None)]).await?
        );

        Ok(())
    }
}
