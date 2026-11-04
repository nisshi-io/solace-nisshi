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
    /// Deletes the records of each partition with its own storage call, one
    /// partition after another, so that a storage error answers in the
    /// partition that it belongs to (see [`storage_error_code`]) and the
    /// other partitions still answer. Kafka also answers each partition's
    /// failure in that partition ([ReplicaManager.scala#L1122-L1146]).
    ///
    /// An engine can fail a whole call, as libSQL, Turso and DynoStore do,
    /// and a call with every partition would then fail partitions whose
    /// records were already deleted.
    ///
    /// One deadline, [`DELETE_RECORDS_DEADLINE`], bounds when the calls
    /// start. A partition whose call has not started at the deadline answers
    /// `REQUEST_TIMED_OUT`, so a degraded engine does not hold the request
    /// past the client's own timeout, one engine timeout per partition.
    ///
    /// A call that has started runs to its end, so the request takes longer
    /// than the deadline by at most one partition's call. The service does
    /// not drop a started call, because a write that its caller drops can
    /// leave its storage engine's connection busy for the next caller.
    ///
    /// [ReplicaManager.scala#L1122-L1146]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1122-L1146
    async fn delete_records(&self, topics: &[DeleteRecordsTopic]) -> Vec<DeleteRecordsTopicResult> {
        let started_at = Instant::now();
        let deadline = started_at + DELETE_RECORDS_DEADLINE;

        let mut responses = Vec::with_capacity(topics.len());
        let mut failed = Vec::new();
        let mut not_started = Vec::new();
        let mut first_error = None;

        for topic in topics {
            let mut partitions = Vec::with_capacity(topic.partitions.as_ref().map_or(0, Vec::len));

            for partition in topic.partitions.iter().flatten() {
                let request = DeleteRecordsTopic::default()
                    .name(topic.name.clone())
                    .partitions(Some(vec![partition.clone()]));

                let answer = started_before(
                    StorageOperation::DeleteRecords,
                    deadline,
                    self.storage.delete_records(slice::from_ref(&request)),
                )
                .await;

                match answer {
                    Ok(Ok(answer)) => partitions.extend(
                        answer
                            .into_iter()
                            .flat_map(|topic| topic.partitions.unwrap_or_default()),
                    ),

                    Ok(Err(error)) => {
                        let topition =
                            Topition::new(topic.name.as_str(), partition.partition_index);
                        let error_code = storage_error_code(&error);
                        debug!(?error, ?topition, ?error_code, "delete records");

                        failed.push((topition, error_code));
                        _ = first_error.get_or_insert(error);

                        partitions.push(
                            DeleteRecordsPartitionResult::default()
                                .partition_index(partition.partition_index)
                                .low_watermark(DELETE_RECORDS_INVALID_LOW_WATERMARK)
                                .error_code(error_code.into()),
                        );
                    }

                    Err(_not_started) => {
                        not_started.push(Topition::new(
                            topic.name.as_str(),
                            partition.partition_index,
                        ));

                        partitions.push(
                            DeleteRecordsPartitionResult::default()
                                .partition_index(partition.partition_index)
                                .low_watermark(DELETE_RECORDS_INVALID_LOW_WATERMARK)
                                .error_code(ErrorCode::RequestTimedOut.into()),
                        );
                    }
                }
            }

            responses.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partitions)),
            );
        }

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
        sync::{Arc, Mutex},
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

    use super::DeleteRecordsService;
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
            }
        }

        /// Returns a stub whose each `delete_records` call takes `delay`
        /// to answer, as a degraded engine does.
        fn with_delay(self, delay: Duration) -> Self {
            Self { delay, ..self }
        }

        /// The topics and partitions of each `delete_records` call.
        fn calls(&self) -> Vec<Call> {
            self.calls.lock().expect("calls").clone()
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
            tokio::time::sleep(self.delay).await;
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

        // one partition to each call, in the order of the request
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

    /// Each call takes 2s and the deadline is 5s. The third call starts at
    /// 4s, before the deadline, so it runs to its end at 6s and answers its
    /// cut. The fourth call would start after the deadline, so it is never
    /// made and answers `REQUEST_TIMED_OUT`.
    #[tokio::test(start_paused = true)]
    async fn partitions_not_started_at_the_deadline_time_out() -> Result<()> {
        let stub = Stub::new(&[
            ("a", 0, Answer::Cut(5)),
            ("a", 1, Answer::Cut(6)),
            ("a", 2, Answer::Cut(7)),
            ("b", 0, Answer::Cut(8)),
        ])
        .with_delay(Duration::from_secs(2));

        let started_at = tokio::time::Instant::now();

        assert_eq!(
            vec![
                (
                    String::from("a"),
                    Some(vec![
                        (0, ErrorCode::None, 5),
                        (1, ErrorCode::None, 6),
                        (2, ErrorCode::None, 7),
                    ])
                ),
                (
                    String::from("b"),
                    Some(vec![(
                        0,
                        ErrorCode::RequestTimedOut,
                        DELETE_RECORDS_INVALID_LOW_WATERMARK
                    )])
                ),
            ],
            delete_records(
                &stub,
                vec![topic("a", Some(&[0, 1, 2])), topic("b", Some(&[0]))]
            )
            .await?
        );

        // the third call ran 1s past the deadline
        assert_eq!(
            DELETE_RECORDS_DEADLINE + Duration::from_secs(1),
            started_at.elapsed()
        );

        // the fourth partition was not started, because the deadline had passed
        assert_eq!(3, stub.calls().len());

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
