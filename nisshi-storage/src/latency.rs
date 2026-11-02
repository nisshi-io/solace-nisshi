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
    ops::Range,
    sync::{Arc, Mutex},
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
use rand::{prelude::*, rngs::SmallRng};
use tokio::time::sleep;
use tracing::{debug, instrument};
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, CommittedOffset, GroupDetail, ListOffsetResponse, MetadataResponse,
    NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse, Result,
    ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, UpdateError, Version,
};

#[derive(Clone, Debug)]
pub struct LatencyIntroducingStorage<S> {
    storage: S,
    rng: Arc<Mutex<SmallRng>>,
    latency: Range<u64>,
}

impl<S> LatencyIntroducingStorage<S>
where
    S: Storage,
{
    pub fn new(storage: S) -> Self {
        Self {
            storage,
            rng: Arc::new(Mutex::new(SmallRng::seed_from_u64(0))),
            latency: 50..150,
        }
    }

    pub fn with_seed(self, seed: u64) -> Self {
        Self {
            rng: Arc::new(Mutex::new(SmallRng::seed_from_u64(seed))),
            ..self
        }
    }

    pub fn with_latency(self, latency: Range<u64>) -> Self {
        Self { latency, ..self }
    }

    #[instrument(skip_all)]
    async fn introduce_latency(&self) -> Result<()> {
        let latency = self
            .rng
            .lock()
            .map(|mut rng| rng.random_range(self.latency.clone()))
            .map(Duration::from_millis)
            .inspect(|latency| debug!(?latency))?;

        sleep(latency).await;

        Ok(())
    }
}

#[async_trait]
impl<G> Storage for LatencyIntroducingStorage<G>
where
    G: Storage + Clone,
{
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        self.introduce_latency().await?;

        self.storage.register_broker(broker_registration).await
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        self.introduce_latency().await?;

        self.storage.create_topic(topic, validate_only).await
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        self.introduce_latency().await?;

        self.storage.incremental_alter_resource(resource).await
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        self.introduce_latency().await?;

        self.storage.delete_records(topics).await
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        self.introduce_latency().await?;

        self.storage.delete_topic(topic).await
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        self.introduce_latency().await?;

        self.storage.brokers().await
    }

    #[instrument(skip_all, fields(transaction_id, topic = topition.topic, partition = topition.partition))]
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        self.introduce_latency().await?;

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
        self.introduce_latency().await?;

        self.storage
            .fetch(topition, offset, min_bytes, max_bytes, isolation, max_wait)
            .await
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        self.introduce_latency().await?;

        self.storage.offset_stage(topition).await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        self.introduce_latency().await?;

        self.storage.list_offsets(isolation_level, offsets).await
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        self.introduce_latency().await?;

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
        self.introduce_latency().await?;

        self.storage
            .offset_fetch(group_id, topics, require_stable)
            .await
    }

    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        self.introduce_latency().await?;

        self.storage.committed_offset_topitions(group_id).await
    }

    async fn pending_txn_offsets(&self, group_id: &str) -> Result<BTreeSet<Topition>> {
        self.introduce_latency().await?;

        self.storage.pending_txn_offsets(group_id).await
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        self.introduce_latency().await?;

        self.storage.metadata(topics).await
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        self.introduce_latency().await?;

        self.storage
            .upsert_user_scram_credential(user, mechanism, credential)
            .await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        self.introduce_latency().await?;

        self.storage
            .delete_user_scram_credential(user, mechanism)
            .await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        self.introduce_latency().await?;

        self.storage.user_scram_credential(user, mechanism).await
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        self.introduce_latency().await?;

        self.storage.describe_config(name, resource, keys).await
    }

    async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
        self.introduce_latency().await?;

        self.storage.list_groups().await
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        self.introduce_latency().await?;

        self.storage.delete_groups(group_ids).await
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        self.introduce_latency().await?;

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
        self.introduce_latency().await?;

        self.storage
            .describe_topic_partitions(topics, partition_limit, cursor)
            .await
    }

    async fn group_with_version(&self, group_id: &str) -> Result<Option<(GroupDetail, Version)>> {
        self.introduce_latency().await?;

        self.storage.group_with_version(group_id).await
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        self.introduce_latency().await?;

        self.storage.update_group(group_id, detail, version).await
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        self.introduce_latency().await?;

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
        self.introduce_latency().await?;

        self.storage
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        self.introduce_latency().await?;

        self.storage.txn_add_partitions(partitions).await
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.introduce_latency().await?;

        self.storage.txn_offset_commit(offsets).await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        self.introduce_latency().await?;

        self.storage
            .txn_end(transaction_id, producer_id, producer_epoch, committed)
            .await
    }

    async fn maintain(&self, now: SystemTime) -> Result<()> {
        self.introduce_latency().await?;

        self.storage.maintain(now).await
    }

    async fn maintain_transactions(&self, now: SystemTime) -> Result<()> {
        self.introduce_latency().await?;

        self.storage.maintain_transactions(now).await
    }

    async fn aborted_transactions(
        &self,
        topition: &Topition,
        offset: i64,
        last_stable_offset: i64,
    ) -> Result<Vec<AbortedTransaction>> {
        self.introduce_latency().await?;

        self.storage
            .aborted_transactions(topition, offset, last_stable_offset)
            .await
    }

    async fn cluster_id(&self) -> Result<String> {
        self.introduce_latency().await?;

        self.storage.cluster_id().await
    }

    async fn node(&self) -> Result<i32> {
        self.introduce_latency().await?;

        self.storage.node().await
    }

    async fn advertised_listener(&self) -> Result<Url> {
        self.introduce_latency().await?;

        self.storage.advertised_listener().await
    }

    async fn ping(&self) -> Result<()> {
        self.introduce_latency().await?;

        self.storage.ping().await
    }
}
