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
//! list every topic and those that look up named topics, records the topics that it creates, and
//! records each batch that it is asked to produce.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::SystemTime,
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
    metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
    record::{deflated, inflated},
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use tokio::{sync::Semaphore, time::Duration};
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
    /// Fails with an error that a retry can't fix.
    Error,
    /// Fails with an error that a retry may fix, such as a lost connection.
    TransientError,
    /// One topic without a name, with an error code: the PostgreSQL engine answers a failed
    /// listing query this way.
    NamelessTopic,
}

/// How the `metadata` lookup of named topics answers.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) enum Lookup {
    /// Answers for each topic that the lookup names.
    #[default]
    Named,
    /// Fails with an error.
    Error,
    /// Answers as the `Listing` does, whatever topics the lookup names, so that a test can
    /// check the service against a lookup that answers for other topics.
    Listed,
}

/// A batch that [`CountingStorage`] was asked to produce, and the method that it was asked with.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Produced {
    pub(crate) transaction_id: Option<String>,
    pub(crate) topition: Topition,
    pub(crate) deflated: deflated::Batch,
    /// The batch that the caller decoded: `Some` when the call was `produce_inflated`, and `None`
    /// when it was `produce`.
    pub(crate) inflated: Option<inflated::Batch>,
}

/// A [`Storage`] that holds only topic names. Clones share their topics and counts.
#[derive(Clone, Debug, Default)]
pub(crate) struct CountingStorage {
    listing: Listing,
    lookup: Lookup,
    topics: Arc<Mutex<BTreeSet<String>>>,
    created: Arc<Mutex<Vec<String>>>,
    listings: Arc<Mutex<usize>>,
    lookups: Arc<Mutex<usize>>,
    init_producer_error: Option<Error>,
    create_topic_failure: Option<(String, Error)>,
    produced: Arc<Mutex<Vec<Produced>>>,
    produce_gate: Option<Arc<Semaphore>>,
    parked: Arc<AtomicUsize>,
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

    /// A storage whose `init_producer` fails with `error`.
    pub(crate) fn failing_init_producer(error: Error) -> Self {
        Self {
            init_producer_error: Some(error),
            ..Self::default()
        }
    }

    /// A storage whose `create_topic` of the topic `name` fails with `error`. A create of any
    /// other topic succeeds.
    pub(crate) fn failing_create_topic(name: &str, error: Error) -> Self {
        Self {
            create_topic_failure: Some((name.to_owned(), error)),
            ..Self::default()
        }
    }

    /// Returns this storage, with the lookup of named topics answering as `lookup`.
    pub(crate) fn with_lookup(self, lookup: Lookup) -> Self {
        Self { lookup, ..self }
    }

    /// Returns this storage, with each produce waiting for a permit of `gate` before it answers,
    /// as an engine waits for a connection. The batch it was given stays held while it waits.
    pub(crate) fn with_produce_gate(self, gate: Arc<Semaphore>) -> Self {
        Self {
            produce_gate: Some(gate),
            ..self
        }
    }

    /// The number of produces now waiting for the gate.
    pub(crate) fn parked(&self) -> usize {
        self.parked.load(Ordering::SeqCst)
    }

    /// Records a produce of `deflated` and `inflated`, after the gate lets it through. Both
    /// produce methods call this, so that the gate holds each produce the same way.
    async fn produce_batch(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        inflated: Option<inflated::Batch>,
    ) -> Result<i64> {
        if let Some(gate) = &self.produce_gate {
            _ = self.parked.fetch_add(1, Ordering::SeqCst);
            let permit = gate.acquire().await;
            _ = self.parked.fetch_sub(1, Ordering::SeqCst);
            permit?.forget();
        }

        self.produced.lock()?.push(Produced {
            transaction_id: transaction_id.map(str::to_owned),
            topition: topition.clone(),
            deflated,
            inflated,
        });

        Ok(0)
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

    /// The batches that `produce` and `produce_inflated` got, in order.
    pub(crate) fn produced(&self) -> Vec<Produced> {
        self.produced.lock().expect("produced").clone()
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

            Listing::TransientError => Err(Error::from(std::io::Error::other("listing failed"))),

            Listing::NamelessTopic => Ok(vec![
                MetadataResponseTopic::default()
                    .error_code(ErrorCode::UnknownTopicOrPartition.into())
                    .name(None),
            ]),
        }
    }

    fn lookup(&self, ids: &[TopicId]) -> Result<Vec<MetadataResponseTopic>> {
        match self.lookup {
            Lookup::Named => {}
            Lookup::Error => return Err(Error::Message("lookup failed".into())),
            Lookup::Listed => return self.list(),
        }

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
        .partitions(Some(
            [MetadataResponsePartition::default().partition_index(0)].into(),
        ))
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
        _fetched: crate::FetchedSender,
    ) -> Result<()> {
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
        if let Some((name, error)) = &self.create_topic_failure
            && *name == topic.name
        {
            return Err(error.clone());
        }

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
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        self.produce_batch(transaction_id, topition, deflated, None)
            .await
    }

    async fn produce_inflated(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        inflated: inflated::Batch,
    ) -> Result<i64> {
        self.produce_batch(transaction_id, topition, deflated, Some(inflated))
            .await
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
            Some(topics) => {
                *self.lookups.lock()? += 1;
                self.lookup(topics)?
            }

            None => {
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
        name: &str,
        resource: ConfigResource,
        _keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        let holds_topic = !matches!(resource, ConfigResource::Topic)
            || self
                .topics
                .lock()
                .expect("the topics lock is not poisoned")
                .contains(name);

        let error_code = if holds_topic {
            ErrorCode::None
        } else {
            ErrorCode::UnknownTopicOrPartition
        };

        Ok(DescribeConfigsResult::default().error_code(error_code.into()))
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
        Err(self
            .init_producer_error
            .clone()
            .expect("a storage built with `failing_init_producer`"))
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
