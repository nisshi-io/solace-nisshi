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

//! A [`Storage`] for service tests that holds only topic names, counts the `metadata` calls that
//! list every topic and those that look up named topics, and records the topics that it creates.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::SystemTime,
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
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    metadata_response::MetadataResponseTopic, record::deflated,
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use tokio::time::Duration;
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, CommittedOffset, Error, GroupDetail, ListOffsetResponse,
    MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse,
    Result, ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest,
    TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError, Version,
};

/// How the `metadata` listing of every topic answers.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) enum Listing {
    /// Lists the topics that exist.
    #[default]
    Topics,
    /// Fails with an error.
    Error,
    /// One topic without a name, with an error code: the PostgreSQL engine answers a failed
    /// listing query this way.
    NamelessTopic,
}

/// A [`Storage`] that holds only topic names. Clones share their topics and counts.
#[derive(Clone, Debug, Default)]
pub(crate) struct CountingStorage {
    listing: Listing,
    topics: Arc<Mutex<BTreeSet<String>>>,
    created: Arc<Mutex<Vec<String>>>,
    listings: Arc<Mutex<usize>>,
    lookups: Arc<Mutex<usize>>,
}

impl CountingStorage {
    /// A storage holding the topics `names`, whose listing answers as `listing`.
    pub(crate) fn new(listing: Listing, names: &[&str]) -> Self {
        Self {
            listing,
            topics: Arc::new(Mutex::new(
                names.iter().map(|name| (*name).to_owned()).collect(),
            )),
            ..Self::default()
        }
    }

    /// Deletes the topic `name`.
    pub(crate) fn delete(&self, name: &str) {
        _ = self.topics.lock().expect("topics").remove(name);
    }

    /// The names of the topics created, in order.
    pub(crate) fn created(&self) -> Vec<String> {
        self.created.lock().expect("created").clone()
    }

    /// The number of `metadata` calls that listed every topic.
    pub(crate) fn listings(&self) -> usize {
        *self.listings.lock().expect("listings")
    }

    /// The number of `metadata` calls that looked up named topics.
    pub(crate) fn lookups(&self) -> usize {
        *self.lookups.lock().expect("lookups")
    }

    fn list(&self) -> Result<Vec<MetadataResponseTopic>> {
        match self.listing {
            Listing::Topics => Ok(self
                .topics
                .lock()?
                .iter()
                .map(|name| existing(name))
                .collect()),

            Listing::Error => Err(Error::Message("listing failed".into())),

            Listing::NamelessTopic => Ok(vec![
                MetadataResponseTopic::default()
                    .error_code(ErrorCode::UnknownTopicOrPartition.into())
                    .name(None),
            ]),
        }
    }

    fn lookup(&self, ids: &[TopicId]) -> Result<Vec<MetadataResponseTopic>> {
        let topics = self.topics.lock()?;

        Ok(ids
            .iter()
            .filter_map(|id| match id {
                TopicId::Name(name) => Some(name),
                TopicId::Id(_) => None,
            })
            .map(|name| {
                if topics.contains(name) {
                    existing(name)
                } else {
                    MetadataResponseTopic::default()
                        .error_code(ErrorCode::UnknownTopicOrPartition.into())
                        .name(Some(name.to_owned()))
                        .partitions(Some([].into()))
                }
            })
            .collect())
    }
}

/// The metadata of the existing topic `name`.
fn existing(name: &str) -> MetadataResponseTopic {
    MetadataResponseTopic::default()
        .error_code(ErrorCode::None.into())
        .name(Some(name.to_owned()))
        .partitions(Some([].into()))
}

#[async_trait]
impl Storage for CountingStorage {
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

    async fn register_broker(&self, _broker_registration: BrokerRegistrationRequest) -> Result<()> {
        unimplemented!()
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        unimplemented!()
    }

    async fn create_topic(&self, topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
        _ = self.topics.lock()?.insert(topic.name.clone());
        self.created.lock()?.push(topic.name);
        Ok(Uuid::new_v4())
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

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let topics = match topics {
            Some(topics) if !topics.is_empty() => {
                *self.lookups.lock()? += 1;
                self.lookup(topics)?
            }

            _ => {
                *self.listings.lock()? += 1;
                self.list()?
            }
        };

        Ok(MetadataResponse {
            cluster: None,
            controller: None,
            brokers: vec![],
            topics,
        })
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

    async fn group_with_version(&self, _group_id: &str) -> Result<Option<(GroupDetail, Version)>> {
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
