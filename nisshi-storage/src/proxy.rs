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
    create_topics_request::CreatableTopic, delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic, delete_records_response::DeleteRecordsTopicResult,
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::DescribeConfigsResult,
    describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
    fetch_response::AbortedTransaction, incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse, record::deflated,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use opentelemetry::{KeyValue, metrics::Histogram};
use tokio::{sync::Semaphore, time::Instant};
use tracing::instrument;
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, CommittedOffset, GroupDetail, ListOffsetResponse, METER,
    MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse,
    Result, ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest,
    TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError, Version, service::deadline,
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
                &[KeyValue::new("operation", "register_broker")],
            )
        })?;
        self.storage.register_broker(broker_registration).await
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "create_topic")],
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
                &[KeyValue::new("operation", "incremental_alter_resource")],
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
                &[KeyValue::new("operation", "delete_records")],
            )
        })?;
        self.storage.delete_records(topics).await
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "delete_topic")],
            )
        })?;
        self.storage.delete_topic(topic).await
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "brokers")],
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
                &[KeyValue::new("operation", "produce")],
            )
        })?;
        self.storage
            .produce(transaction_id, topition, deflated)
            .await
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        let start = SystemTime::now();
        let started_at = Instant::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "fetch")],
            )
        })?;

        // The wait for the permit is charged to the budget, so that the
        // read ends when its caller's budget does, rather than that long
        // after the wait.
        let max_wait = max_wait.saturating_sub(started_at.elapsed());

        self.storage
            .fetch(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            .await
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "offset_stage")],
            )
        })?;
        self.storage.offset_stage(topition).await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let start = SystemTime::now();
        let _permit = deadline::queued(self.semaphore.acquire())
            .await
            .inspect(|_| {
                SEMAPHORE_ACQUIRE_DURATION.record(
                    elapsed_millis(start),
                    &[KeyValue::new("operation", "list_offsets")],
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
                &[KeyValue::new("operation", "offset_commit")],
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
                &[KeyValue::new("operation", "offset_fetch")],
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
                &[KeyValue::new("operation", "committed_offset_topitions")],
            )
        })?;
        self.storage.committed_offset_topitions(group_id).await
    }

    async fn pending_txn_offsets(&self, group_id: &str) -> Result<BTreeSet<Topition>> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "pending_txn_offsets")],
            )
        })?;
        self.storage.pending_txn_offsets(group_id).await
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "metadata")],
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
                &[KeyValue::new("operation", "upsert_user_scram_credential")],
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
                &[KeyValue::new("operation", "delete_user_scram_credential")],
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
                &[KeyValue::new("operation", "user_scram_credential")],
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
                &[KeyValue::new("operation", "describe_config")],
            )
        })?;
        self.storage.describe_config(name, resource, keys).await
    }

    async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
        let start = SystemTime::now();
        let _permit = self.semaphore.acquire().await.inspect(|_| {
            SEMAPHORE_ACQUIRE_DURATION.record(
                elapsed_millis(start),
                &[KeyValue::new("operation", "list_groups")],
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
                &[KeyValue::new("operation", "delete_groups")],
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
                &[KeyValue::new("operation", "describe_groups")],
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
                &[KeyValue::new("operation", "describe_topic_partitions")],
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
                &[KeyValue::new("operation", "group_with_version")],
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
                    &[KeyValue::new("operation", "update_group")],
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
                &[KeyValue::new("operation", "init_producer")],
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
                &[KeyValue::new("operation", "txn_add_offsets")],
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
                &[KeyValue::new("operation", "txn_add_partitions")],
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
                &[KeyValue::new("operation", "txn_offset_commit")],
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
                &[KeyValue::new("operation", "txn_end")],
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
    use nisshi_sans_io::IsolationLevel;
    use tokio::time::{Duration, advance};

    use super::SemaphoreProxy;
    use crate::{
        Result, Storage, Topition,
        service::{Request, RequestChannelService, Response, bounded_channel},
    };

    /// A read that waits for a permit reaches storage with the budget left
    /// after the wait, so that the read ends when its caller's budget does.
    #[tokio::test(start_paused = true)]
    async fn fetch_charges_the_permit_wait_to_the_budget() -> Result<()> {
        let (tx, mut rx) = bounded_channel(1);
        let proxy = SemaphoreProxy::new(RequestChannelService::new(tx));

        let permit = proxy.semaphore.clone().acquire_owned().await?;
        let tp = Topition::new("abc", 0);

        let read = proxy.fetch(
            &tp,
            0,
            1,
            1024,
            IsolationLevel::ReadUncommitted,
            Duration::from_millis(500),
        );

        let release = async {
            advance(Duration::from_millis(200)).await;
            drop(permit);
        };

        let answer = async {
            let (request, reply) = rx.recv().await.expect("a fetch request");

            let Request::Fetch { max_wait, .. } = request else {
                panic!("{request:?}");
            };

            reply.send(Response::Fetch(Ok(vec![]))).expect("the reader");

            max_wait
        };

        let (fetched, (), max_wait) = tokio::join!(read, release, answer);

        assert!(fetched?.is_empty());
        assert_eq!(Duration::from_millis(300), max_wait);

        Ok(())
    }
}
