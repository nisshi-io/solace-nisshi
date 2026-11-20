// Copyright (c) Solace Corporation
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
//! list every topic and those that look up named topics, records the topics that it creates and
//! deletes, records each batch that it is asked to produce, and answers the broker's own
//! registration.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex, PoisonError,
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
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    record::{deflated, inflated},
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use tokio::{
    sync::{Semaphore, watch},
    time::{Duration, timeout},
};
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, ByteLimit, CommittedOffset, Error, GroupDetail, ListOffsetResponse,
    MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProduceEntry,
    ProducerIdResponse, ReadEnd, Result, ScramCredential, Storage, TopicId, Topition,
    TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError,
    Version,
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

/// A `Storage` call that answers from this broker's own registration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BrokerCall {
    Brokers,
    ClusterId,
    Node,
    AdvertisedListener,
}

/// How long [`CountingStorage::until`] waits for its condition. It is longer than any test needs
/// for its produces to reach storage on a busy machine. It is shorter than a request's deadline,
/// so that a test on a paused clock fails at the wait, with its own message, and not when the
/// request answers.
pub(crate) const UNTIL_LIMIT: Duration = Duration::from_secs(15);

/// The node id that [`CountingStorage`] answers for `node` and lists in `brokers`.
pub(crate) const NODE_ID: i32 = 111;

/// The host and port of the listener that [`CountingStorage`] answers for `advertised_listener`
/// and lists in `brokers`.
pub(crate) const HOST: &str = "localhost";
pub(crate) const PORT: i32 = 9092;

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
    named_lookup_failure: Arc<Mutex<Option<Error>>>,
    lookup_failure: Arc<Mutex<Option<Error>>>,
    init_producer_error: Option<Error>,
    create_topic_failure: Option<(String, Error)>,
    delete_topic_failure: Option<(TopicId, Error)>,
    delete_topic_failure_delay: Duration,
    deleted: Arc<Mutex<Vec<TopicId>>>,
    broker_call_failure: Option<(BrokerCall, Error)>,
    produced: Arc<Mutex<Vec<Produced>>>,
    produce_failure: Option<(Topition, Error)>,
    produce_gate: Option<Arc<Semaphore>>,
    produce_all_calls: Arc<AtomicUsize>,
    gated_partitions: Option<BTreeSet<i32>>,
    config_gate: Option<Arc<Semaphore>>,
    parked: Arc<AtomicUsize>,
    produce_delay: Option<Duration>,
    partition_failure: Option<(i32, Error)>,
    produce_concurrency: Option<usize>,
    produce_permits: Option<Arc<Semaphore>>,
    entered: Arc<AtomicUsize>,
    /// The produces now running for each partition, from their start to their end.
    running: Arc<Mutex<BTreeMap<Topition, usize>>>,
    /// The partitions that got a produce while another produce to them still ran.
    overlapped: Arc<Mutex<BTreeSet<Topition>>>,
    /// Changes each time a produce starts, parks at the gate, leaves the gate, or is recorded, so
    /// that [`CountingStorage::until`] checks its condition again.
    changes: Arc<watch::Sender<()>>,
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

    /// A storage whose `delete_topic` of `topic` fails with `error`. A delete of any other topic
    /// succeeds.
    pub(crate) fn failing_delete_topic(topic: TopicId, error: Error) -> Self {
        Self {
            delete_topic_failure: Some((topic, error)),
            ..Self::default()
        }
    }

    /// A storage whose `call` fails with `error`. The other broker calls answer as they do by
    /// default: node [`NODE_ID`] on [`HOST`]:[`PORT`], in the cluster `nisshi`.
    pub(crate) fn failing_broker_call(call: BrokerCall, error: Error) -> Self {
        Self {
            broker_call_failure: Some((call, error)),
            ..Self::default()
        }
    }

    /// Returns this storage, with a failing `delete_topic` waiting `delay` before it fails, as a
    /// call that runs into its deadline does. A test with a paused clock moves the time by it.
    pub(crate) fn with_delete_failure_delay(self, delay: Duration) -> Self {
        Self {
            delete_topic_failure_delay: delay,
            ..self
        }
    }

    /// The topics that `delete_topic` deleted, in call order. A failed delete is not in it.
    pub(crate) fn deleted(&self) -> Result<Vec<TopicId>> {
        Ok(self.deleted.lock()?.clone())
    }

    /// Returns `Err` if this storage was built to fail `call`.
    fn broker_call_failure(&self, call: BrokerCall) -> Result<()> {
        match &self.broker_call_failure {
            Some((failing, error)) if *failing == call => Err(error.clone()),
            _ => Ok(()),
        }
    }

    /// Makes each lookup that names a topic fail with `error` from now on, or answer again when
    /// `error` is `None`. Clones share the setting. A lookup of no topics, which reads only the
    /// brokers, still answers.
    pub(crate) fn fail_named_lookups(&self, error: Option<Error>) {
        *self
            .named_lookup_failure
            .lock()
            .expect("named lookup failure") = error;
    }

    /// Makes every lookup fail with `error` from now on, also a lookup of no topics, or answer
    /// again when `error` is `None`. Clones share the setting.
    pub(crate) fn fail_lookups(&self, error: Option<Error>) {
        *self.lookup_failure.lock().expect("lookup failure") = error;
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

    /// Returns this storage, with each produce to `topition` failing with `error`, and recording
    /// nothing. A produce to any other partition succeeds.
    pub(crate) fn with_failing_produce(self, topition: Topition, error: Error) -> Self {
        Self {
            produce_failure: Some((topition, error)),
            ..self
        }
    }

    /// Returns this storage, with the gate holding only the produces to `partitions`. A produce to
    /// another partition goes through at once.
    pub(crate) fn with_gated_partitions(self, partitions: &[i32]) -> Self {
        Self {
            gated_partitions: Some(partitions.iter().copied().collect()),
            ..self
        }
    }

    /// The number of calls to `produce_all` on this storage. A wrapper that does not forward
    /// `produce_all` calls `produce` on this storage instead, and leaves this count unchanged.
    pub(crate) fn produce_all_calls(&self) -> usize {
        self.produce_all_calls.load(Ordering::SeqCst)
    }

    /// Returns this storage, with each read of a topic's configuration waiting for a permit of
    /// `gate` before it answers, as a read waits for an engine connection.
    pub(crate) fn with_config_gate(self, gate: Arc<Semaphore>) -> Self {
        Self {
            config_gate: Some(gate),
            ..self
        }
    }

    /// Returns this storage, with each produce taking `delay` after the gate lets it through, as
    /// an engine takes time for its write.
    pub(crate) fn with_produce_delay(self, delay: Duration) -> Self {
        Self {
            produce_delay: Some(delay),
            ..self
        }
    }

    /// Returns this storage, with each produce to `partition` of any topic failing with `error`
    /// after the gate lets it through. A produce to another partition succeeds.
    pub(crate) fn with_produce_failure(self, partition: i32, error: Error) -> Self {
        Self {
            partition_failure: Some((partition, error)),
            ..self
        }
    }

    /// Returns this storage answering `concurrency` to [`Storage::produce_concurrency`], as an
    /// engine that sets its own limit does.
    pub(crate) fn with_produce_concurrency(self, concurrency: usize) -> Self {
        Self {
            produce_concurrency: Some(concurrency),
            ..self
        }
    }

    /// Returns this storage answering `permits` to [`Storage::produce_permits`], as an engine that
    /// limits its writes across requests does.
    pub(crate) fn with_produce_permits(self, permits: Arc<Semaphore>) -> Self {
        Self {
            produce_permits: Some(permits),
            ..self
        }
    }

    /// The number of produces now waiting for the gate.
    pub(crate) fn parked(&self) -> usize {
        self.parked.load(Ordering::SeqCst)
    }

    /// The number of produces that have started, whether or not they have ended.
    pub(crate) fn entered(&self) -> usize {
        self.entered.load(Ordering::SeqCst)
    }

    /// Returns whether `topition` got a produce while another produce to it still ran.
    pub(crate) fn overlapped(&self, topition: &Topition) -> bool {
        self.overlapped
            .lock()
            .expect("overlapped")
            .contains(topition)
    }

    /// Waits until `condition` holds for this storage, and fails the test if it does not hold
    /// within [`UNTIL_LIMIT`]. The storage checks it now, and again after each change to its
    /// produces: a produce that starts, parks at the gate, leaves the gate, or ends. A condition
    /// on other state, such as the topics that it created, is checked only at those changes.
    ///
    /// The check can miss a state that lasts only between two checks, so `condition` must stay
    /// true once it holds, such as `entered() >= 2` rather than `entered() == 2`. `parked()`
    /// falls when the gate lets a produce through, so a condition on it stays true only while the
    /// gate holds its produces.
    pub(crate) async fn until(&self, condition: impl Fn(&Self) -> bool) {
        let mut changes = self.changes.subscribe();

        // The storage holds the sender, so the channel stays open while `self` lives.
        _ = timeout(UNTIL_LIMIT, changes.wait_for(|()| condition(self)))
            .await
            .expect("the storage reached the condition in time");
    }

    /// Wakes each [`CountingStorage::until`] that waits, to check its condition again.
    fn changed(&self) {
        self.changes.send_modify(|()| {});
    }

    /// Records a produce of `deflated` and `inflated`, after the gate lets it through. Both
    /// produce methods call this, so that the gate holds each produce the same way.
    ///
    /// Returns the partition of `topition` as the offset, so that the results of produces to
    /// different partitions differ.
    async fn produce_batch(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        inflated: Option<inflated::Batch>,
    ) -> Result<i64> {
        let gated = self
            .gated_partitions
            .as_ref()
            .is_none_or(|partitions| partitions.contains(&topition.partition()));

        _ = self.entered.fetch_add(1, Ordering::SeqCst);
        // The guard wakes each `until` when the produce ends, on every path out of this function.
        let _running = Running::start(self, topition)?;
        self.changed();

        if gated && let Some(gate) = &self.produce_gate {
            _ = self.parked.fetch_add(1, Ordering::SeqCst);
            self.changed();
            let permit = gate.acquire().await;
            _ = self.parked.fetch_sub(1, Ordering::SeqCst);
            self.changed();
            permit?.forget();
        }

        if let Some(delay) = self.produce_delay {
            tokio::time::sleep(delay).await;
        }

        if let Some((partition, error)) = &self.partition_failure
            && *partition == topition.partition()
        {
            return Err(error.clone());
        }

        if let Some((failing, error)) = &self.produce_failure
            && failing == topition
        {
            return Err(error.clone());
        }

        self.produced.lock()?.push(Produced {
            transaction_id: transaction_id.map(str::to_owned),
            topition: topition.clone(),
            deflated,
            inflated,
        });

        Ok(i64::from(topition.partition()))
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

/// One produce to a partition of a [`CountingStorage`], from its start to its end, also when it
/// fails or is dropped. Its end wakes each [`CountingStorage::until`] that waits.
struct Running {
    running: Arc<Mutex<BTreeMap<Topition, usize>>>,
    changes: Arc<watch::Sender<()>>,
    topition: Topition,
}

impl Running {
    /// Counts a produce to `topition` as running, and records an overlap if another one runs.
    fn start(storage: &CountingStorage, topition: &Topition) -> Result<Self> {
        let mut running = storage.running.lock()?;
        let count = running.entry(topition.clone()).or_default();

        if *count > 0 {
            _ = storage.overlapped.lock()?.insert(topition.clone());
        }

        *count += 1;

        Ok(Self {
            running: Arc::clone(&storage.running),
            changes: Arc::clone(&storage.changes),
            topition: topition.clone(),
        })
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        if let Some(count) = self
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_mut(&self.topition)
        {
            *count -= 1;
        }

        self.changes.send_modify(|()| {});
    }
}

/// The metadata of the existing topic `name`: one partition, led by [`NODE_ID`] at epoch 0.
fn existing(name: &str) -> MetadataResponseTopic {
    MetadataResponseTopic::default()
        .error_code(ErrorCode::None.into())
        .name(Some(name.to_owned()))
        .partitions(Some(
            [MetadataResponsePartition::default()
                .partition_index(0)
                .leader_id(NODE_ID)
                .leader_epoch(Some(0))
                .replica_nodes(Some(vec![NODE_ID]))
                .isr_nodes(Some(vec![NODE_ID]))
                .offline_replicas(Some(vec![]))]
            .into(),
        ))
}

#[async_trait]
impl Storage for CountingStorage {
    async fn fetch(
        &self,
        _topition: &Topition,
        _offset: i64,
        _min_bytes: u32,
        _limit: ByteLimit,
        _isolation_level: IsolationLevel,
        _fetched: crate::FetchedSender,
    ) -> Result<ReadEnd> {
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
        self.broker_call_failure(BrokerCall::Brokers)?;

        Ok(vec![
            DescribeClusterBroker::default()
                .broker_id(NODE_ID)
                .host(HOST.into())
                .port(PORT)
                .rack(None),
        ])
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

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        if let Some((failing, error)) = &self.delete_topic_failure
            && failing == topic
        {
            tokio::time::sleep(self.delete_topic_failure_delay).await;
            return Err(error.clone());
        }

        self.deleted.lock()?.push(topic.clone());
        Ok(ErrorCode::None)
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

    async fn produce_all(
        &self,
        transaction_id: Option<&str>,
        entries: Vec<ProduceEntry>,
    ) -> Vec<Result<i64>> {
        _ = self.produce_all_calls.fetch_add(1, Ordering::SeqCst);

        ProduceEntry::produce_each(self, transaction_id, entries).await
    }

    fn produce_concurrency(&self) -> usize {
        self.produce_concurrency
            .unwrap_or(crate::service::produce::PRODUCE_CONCURRENCY)
    }

    fn produce_permits(&self) -> Option<Arc<Semaphore>> {
        self.produce_permits.clone()
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

                if let Some(error) = self.lookup_failure.lock()?.clone() {
                    return Err(error);
                }

                if !topics.is_empty()
                    && let Some(error) = self.named_lookup_failure.lock()?.clone()
                {
                    return Err(error);
                }

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
            brokers: vec![
                MetadataResponseBroker::default()
                    .node_id(NODE_ID)
                    .host(HOST.into())
                    .port(PORT)
                    .rack(None),
            ],
            topics,
        })
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        _keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        if matches!(resource, ConfigResource::Topic)
            && let Some(gate) = &self.config_gate
        {
            gate.acquire().await?.forget();
        }

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
        self.broker_call_failure(BrokerCall::ClusterId)?;
        Ok("nisshi".into())
    }

    async fn node(&self) -> Result<i32> {
        self.broker_call_failure(BrokerCall::Node)?;
        Ok(NODE_ID)
    }

    async fn advertised_listener(&self) -> Result<Url> {
        self.broker_call_failure(BrokerCall::AdvertisedListener)?;
        Ok(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
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
