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
    fmt::Debug,
    sync::{Arc, LazyLock},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use nisshi_sans_io::{
    ConfigResource, ErrorCode, IsolationLevel, ListOffset, ScramMechanism,
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
    record::{deflated, inflated},
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use opentelemetry::metrics::Histogram;
use tokio::{sync::Semaphore, time::Instant};
use tracing::instrument;
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, CommittedOffset, DeleteRecordsConcurrency, GroupDetail,
    ListOffsetResponse, METER, MetadataResponse, NamedGroupDetail, OffsetCommitRequest,
    OffsetStage, ProducerIdResponse, Result, ScramCredential, Storage, StorageOperation, TopicId,
    Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest,
    UpdateError, Version, produce_path,
    service::read_deadline::{QueueWait as _, ReadDeadline, ReadWait},
};

static SEMAPHORE_ACQUIRE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_storage_proxy_semaphore_acquire_duration")
        .with_boundaries(
            [
                0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0,
                1000.0,
            ]
            .into(),
        )
        .with_unit("ms")
        .with_description("Storage proxy semaphore acquisition duration in ms")
        .build()
});

#[derive(Clone, Debug)]
pub struct SemaphoreProxy<G> {
    storage: G,
    semaphore: Arc<Semaphore>,
}

impl<G> SemaphoreProxy<G> {
    pub fn new(storage: G) -> Self {
        Self {
            storage,
            semaphore: Arc::new(Semaphore::new(1)),
        }
    }
}

fn elapsed_millis(start: SystemTime) -> u64 {
    start
        .elapsed()
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[async_trait]
impl<G> Storage for SemaphoreProxy<G>
where
    G: Storage + Clone,
{
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::RegisterBroker.attribute()],
            )
        })?;
        self.storage.register_broker(broker_registration).await
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::CreateTopic.attribute()],
            )
        })?;
        self.storage.create_topic(topic, validate_only).await
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::IncrementalAlterResource.attribute()],
            )
        })?;
        self.storage.incremental_alter_resource(resource).await
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::DeleteRecords.attribute()],
            )
        })?;
        self.storage.delete_records(topics).await
    }

    /// The proxy holds one permit for each call, so a second call waits for
    /// the first, whatever the storage behind it allows.
    fn delete_records_concurrency(&self) -> DeleteRecordsConcurrency {
        DeleteRecordsConcurrency::serial()
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::DeleteTopic.attribute()],
            )
        })?;
        self.storage.delete_topic(topic).await
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::Brokers.attribute()],
            )
        })?;
        self.storage.brokers().await
    }

    #[instrument(skip_all, fields(transaction_id, topic = topition.topic, partition = topition.partition))]
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::Produce.attribute()],
            )
        })?;
        self.storage
            .produce(transaction_id, topition, deflated)
            .await
    }

    #[instrument(skip_all, fields(transaction_id, topic = topition.topic, partition = topition.partition))]
    async fn produce_inflated(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        inflated: inflated::Batch,
    ) -> Result<i64> {
        // A produce that waits for the permit holds only its compressed bytes, so the one decoded
        // batch that the proxy holds is the batch of the produce that has the permit. The produce
        // then reaches the storage without the decoded batch, which decodes it again.
        let start = SystemTime::now();
        let Ok(_permit) = self.semaphore.try_acquire() else {
            drop(inflated);
            produce_path::fallback();

            return self.produce(transaction_id, topition, deflated).await;
        };

        produce_path::direct();
        SEMAPHORE_ACQUIRE_DURATION.record(
            elapsed_millis(start),
            &[StorageOperation::Produce.attribute()],
        );

        self.storage
            .produce_inflated(transaction_id, topition, deflated, inflated)
            .await
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        deadline: Instant,
    ) -> Result<Vec<deflated::Batch>> {
        let start = SystemTime::now();

        let Ok(permit) = ReadDeadline::new(StorageOperation::Fetch, deadline)
            .timeout(
                self.semaphore
                    .acquire()
                    .queued_for(ReadWait::ExclusiveEngineAccess),
            )
            .await
        else {
            return Ok(vec![]);
        };

        let _permit = permit.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::Fetch.attribute()],
            )
        })?;

        self.storage
            .fetch(topition, offset, min_bytes, max_bytes, isolation, deadline)
            .await
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::OffsetStage.attribute()],
            )
        })?;
        self.storage.offset_stage(topition).await
    }

    async fn current_offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::CurrentOffsetStage.attribute()],
            )
        })?;
        self.storage.current_offset_stage(topition).await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let start = SystemTime::now();
        let _permit = self
            .semaphore
            .acquire()
            .queued_for(ReadWait::ExclusiveEngineAccess)
            .await
            .inspect(|_| {
                SEMAPHORE_ACQUIRE_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::ListOffsets.attribute()],
                )
            })?;
        self.storage.list_offsets(isolation_level, offsets).await
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::OffsetCommit.attribute()],
            )
        })?;
        self.storage
            .offset_commit(group_id, retention_time_ms, offsets)
            .await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::OffsetFetch.attribute()],
            )
        })?;
        self.storage
            .offset_fetch(group_id, topics, require_stable)
            .await
    }

    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::CommittedOffsetTopitions.attribute()],
            )
        })?;
        self.storage.committed_offset_topitions(group_id).await
    }

    async fn pending_txn_offsets(&self, group_id: &str) -> Result<BTreeSet<Topition>> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::PendingTxnOffsets.attribute()],
            )
        })?;
        self.storage.pending_txn_offsets(group_id).await
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::Metadata.attribute()],
            )
        })?;
        self.storage.metadata(topics).await
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::UpsertUserScramCredential.attribute()],
            )
        })?;
        self.storage
            .upsert_user_scram_credential(user, mechanism, credential)
            .await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::DeleteUserScramCredential.attribute()],
            )
        })?;
        self.storage
            .delete_user_scram_credential(user, mechanism)
            .await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::UserScramCredential.attribute()],
            )
        })?;
        self.storage.user_scram_credential(user, mechanism).await
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::DescribeConfig.attribute()],
            )
        })?;
        self.storage.describe_config(name, resource, keys).await
    }

    async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::ListGroups.attribute()],
            )
        })?;
        self.storage.list_groups().await
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::DeleteGroups.attribute()],
            )
        })?;
        self.storage.delete_groups(group_ids).await
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::DescribeGroups.attribute()],
            )
        })?;
        self.storage
            .describe_groups(group_ids, include_authorized_operations)
            .await
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::DescribeTopicPartitions.attribute()],
            )
        })?;
        self.storage
            .describe_topic_partitions(topics, partition_limit, cursor)
            .await
    }

    async fn group_with_version(&self, group_id: &str) -> Result<Option<(GroupDetail, Version)>> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::GroupWithVersion.attribute()],
            )
        })?;
        self.storage.group_with_version(group_id).await
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let start = SystemTime::now();
        let _permit = self
            .semaphore
            .acquire()
            .await
            .inspect(|_| {
                SEMAPHORE_ACQUIRE_DURATION.record(
                    elapsed_millis(start),
                    &[StorageOperation::UpdateGroup.attribute()],
                )
            })
            .map_err(|err| UpdateError::Error(err.into()))?;
        self.storage.update_group(group_id, detail, version).await
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::InitProducer.attribute()],
            )
        })?;
        self.storage
            .init_producer(
                transaction_id,
                transaction_timeout_ms,
                producer_id,
                producer_epoch,
            )
            .await
    }

    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::TxnAddOffsets.attribute()],
            )
        })?;
        self.storage
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::TxnAddPartitions.attribute()],
            )
        })?;
        self.storage.txn_add_partitions(partitions).await
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::TxnOffsetCommit.attribute()],
            )
        })?;
        self.storage.txn_offset_commit(offsets).await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[StorageOperation::TxnEnd.attribute()],
            )
        })?;
        self.storage
            .txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
    }

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        self.storage.maintain(now).await
    }

    async fn maintain_transactions(&self, now: SystemTime) -> Result<()> {
        self.storage.maintain_transactions(now).await
    }

    async fn aborted_transactions(
        &self,
        topition: &Topition,
        offset: i64,
        last_stable_offset: i64,
    ) -> Result<Vec<AbortedTransaction>> {
        self.storage
            .aborted_transactions(topition, offset, last_stable_offset)
            .await
    }

    async fn cluster_id(&self) -> Result<String> {
        self.storage.cluster_id().await
    }

    async fn node(&self) -> Result<i32> {
        self.storage.node().await
    }

    async fn advertised_listener(&self) -> Result<Url> {
        self.storage.advertised_listener().await
    }

    async fn ping(&self) -> Result<()> {
        self.storage.ping().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use nisshi_sans_io::{IsolationLevel, record::deflated};
    use tokio::{
        task::yield_now,
        time::{Duration, Instant, advance},
    };

    use super::SemaphoreProxy;
    use crate::{
        Result, Storage, Topition,
        decoded_batch_probe::probe,
        service::{ChannelRequest, Request, RequestChannelService, Response, bounded_channel},
    };

    /// A produce that gets the permit at once passes its decoded batch on.
    #[tokio::test]
    async fn an_uncontended_produce_keeps_the_decoded_batch() -> Result<()> {
        let (tx, mut rx) = bounded_channel(1);
        let proxy = SemaphoreProxy::new(RequestChannelService::new(tx));

        let (inflated, _dropped) = probe();
        let tp = Topition::new("abc", 0);

        let produce = proxy.produce_inflated(None, &tp, deflated::Batch::default(), inflated);

        let answer = async {
            let ChannelRequest {
                request,
                response: reply,
                ..
            } = rx.recv().await.expect("a produce request");

            let Request::Produce { inflated, .. } = request else {
                panic!("{request:?}");
            };

            reply
                .send(Ok(Response::Produce(Ok(0))))
                .expect("the producer");

            inflated.is_some()
        };

        let (offset, kept) = tokio::join!(produce, answer);

        assert_eq!(0, offset?);
        assert!(kept, "the proxy dropped the decoded batch");

        Ok(())
    }

    /// A produce that waits for the permit holds only its compressed bytes, so a queue of waiting
    /// produces does not hold a decoded batch each.
    #[tokio::test]
    async fn a_produce_that_waits_for_the_permit_holds_no_decoded_batch() -> Result<()> {
        let (tx, mut rx) = bounded_channel(1);
        let proxy = SemaphoreProxy::new(RequestChannelService::new(tx));

        // The permit is taken, so the produce below has to wait for it.
        let permit = proxy.semaphore.clone().acquire_owned().await?;

        let (inflated, dropped) = probe();
        let tp = Topition::new("xyz", 3);

        let produce = proxy.produce_inflated(
            Some("transaction"),
            &tp,
            deflated::Batch::default(),
            inflated,
        );

        let release = async {
            // Every branch has been polled once after this yield, so the produce is waiting.
            yield_now().await;
            let held = !dropped.load(Ordering::SeqCst);

            drop(permit);

            held
        };

        let answer = async {
            let ChannelRequest {
                request,
                response: reply,
                ..
            } = rx.recv().await.expect("a produce request");

            let Request::Produce {
                transaction_id,
                topition,
                inflated,
                ..
            } = request
            else {
                panic!("{request:?}");
            };

            reply
                .send(Ok(Response::Produce(Ok(0))))
                .expect("the producer");

            (transaction_id, topition, inflated.is_none())
        };

        let (offset, held, (transaction_id, topition, sent_without)) =
            tokio::join!(produce, release, answer);

        assert_eq!(0, offset?);
        assert!(!held, "the waiting produce still held the decoded batch");
        assert!(sent_without, "the produce reached storage with the batch");

        // A waiting produce that lost its transaction id would be stored as a non-transactional
        // batch.
        assert_eq!(
            Some("transaction"),
            transaction_id.as_deref(),
            "the waiting produce did not forward the transaction id"
        );
        assert_eq!(
            tp, topition,
            "the waiting produce did not forward the topition"
        );

        Ok(())
    }

    /// A read that waits for the permit gives storage the same deadline that it was given, so the
    /// time spent waiting counts against that deadline.
    #[tokio::test(start_paused = true)]
    async fn fetch_keeps_its_deadline_after_waiting_for_the_permit() -> Result<()> {
        let (tx, mut rx) = bounded_channel(1);
        let proxy = SemaphoreProxy::new(RequestChannelService::new(tx));

        let permit = proxy.semaphore.clone().acquire_owned().await?;
        let tp = Topition::new("abc", 0);
        let deadline = Instant::now() + Duration::from_millis(500);

        let (offset, min_bytes, max_bytes) = (0, 1, 1024);
        let read = proxy.fetch(
            &tp,
            offset,
            min_bytes,
            max_bytes,
            IsolationLevel::ReadUncommitted,
            deadline,
        );

        let release = async {
            advance(Duration::from_millis(200)).await;
            drop(permit);
        };

        let answer = async {
            let ChannelRequest {
                request,
                response: reply,
                ..
            } = rx.recv().await.expect("a fetch request");

            let Request::Fetch { deadline, .. } = request else {
                panic!("{request:?}");
            };

            reply
                .send(Ok(Response::Fetch(Ok(vec![]))))
                .expect("the reader");

            deadline
        };

        let (fetched, (), received) = tokio::join!(read, release, answer);

        assert!(fetched?.is_empty());
        assert_eq!(deadline, received);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn fetch_stops_waiting_for_a_permit_at_its_deadline() -> Result<()> {
        let (tx, mut rx) = bounded_channel(1);
        let proxy = SemaphoreProxy::new(RequestChannelService::new(tx));

        // The permit is never released, so the read waits for it until its deadline.
        let _permit = proxy.semaphore.clone().acquire_owned().await?;

        let started_at = Instant::now();
        let deadline = started_at + Duration::from_millis(500);

        let (offset, min_bytes, max_bytes) = (0, 1, 1024);
        let fetched = proxy
            .fetch(
                &Topition::new("abc", 0),
                offset,
                min_bytes,
                max_bytes,
                IsolationLevel::ReadUncommitted,
                deadline,
            )
            .await?;

        assert!(fetched.is_empty());
        assert_eq!(Duration::from_millis(500), started_at.elapsed());
        assert!(rx.try_recv().is_err(), "the read reached storage");

        Ok(())
    }
}
