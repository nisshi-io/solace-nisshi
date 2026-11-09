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
    pin::Pin,
    sync::{Arc, LazyLock, Mutex},
    task::{Context, Poll, Waker},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use nisshi_sans_io::{
    BatchAttribute, Compression, ConfigResource, ErrorCode, IsolationLevel, ListOffset,
    ScramMechanism, TimestampType,
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
    record::{MAX_DECODED_BATCH_BYTES, deflated, inflated},
    txn_offset_commit_response::TxnOffsetCommitResponseTopic,
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Histogram},
};
use tokio::time::{Instant, sleep};
use tracing::{debug, instrument, warn};
use url::Url;
use uuid::Uuid;

use crate::{
    BrokerRegistrationRequest, CommittedOffset, DeleteRecordsConcurrency, Error, GroupDetail,
    ListOffsetResponse, METER, MetadataResponse, NamedGroupDetail, OffsetCommitRequest,
    OffsetStage, ProducerIdResponse, Result, ScramCredential, Storage, TopicId, Topition,
    TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError,
    Version, inflate_produced, offload,
};

static BATCH_REQUESTS_LENGTH: LazyLock<Gauge<u64>> =
    LazyLock::new(|| METER.u64_gauge("batch_request_gauge").build());

static BATCH_RESPONSES_LENGTH: LazyLock<Gauge<u64>> =
    LazyLock::new(|| METER.u64_gauge("batch_response_gauge").build());

static BATCH_TICKET_POLL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_ticket_poll")
        .with_description("The number of ticket polls")
        .build()
});

static SEND_QUEUED_PRODUCED_RECORDS_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_send_queued_records")
        .with_description("The number of produced send queued records")
        .build()
});

static SEND_QUEUED_WAKE_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_send_queued_wake_event")
        .with_description("The number of wake events sent")
        .build()
});

static PRODUCE_REQUEST_MINIMUM_SIZE_TRIGGER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_produce_minimum_size_trigger")
        .with_description("The number of times the minimum size was a trigger")
        .build()
});

static PRODUCE_REQUEST_YOUR_TICKET_IS_READY: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_produce_your_ticket_is_ready")
        .with_description("The number of notifications that your ticket was ready while waiting")
        .build()
});

static PRODUCE_REQUEST_TIMEOUT_EXPIRED_TRIGGER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_produce_timeout_expired_trigger")
        .with_description("The number of times the timeout expiry was a trigger")
        .build()
});

static PRODUCE_REQUEST_QUEUED_COUNTER: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("batch_produce_queued")
        .with_description("The number of produce requests queued")
        .build()
});

static PRODUCE_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("batch_produce_duration")
        .with_unit("ms")
        .with_description("The batch produce latency in milliseconds")
        .build()
});

#[derive(Clone, Debug)]
struct Ticket<G> {
    id: Uuid,
    batcher: ProduceRequestBatcher<G>,
}

impl<G> Ticket<G> {
    fn new(batcher: ProduceRequestBatcher<G>) -> Self {
        Self {
            id: Uuid::now_v7(),
            batcher,
        }
    }
}

impl<G> AsRef<Uuid> for Ticket<G> {
    fn as_ref(&self) -> &Uuid {
        &self.id
    }
}

impl<G> Future for Ticket<G> {
    type Output = Result<i64, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut responses = self.batcher.responses.lock()?;

        match responses.remove(&self.id) {
            Some(BatchResponse::Response(response)) => {
                let outcome = if response.is_ok() { "ready" } else { "error" };
                BATCH_TICKET_POLL.add(1, &[KeyValue::new("outcome", outcome)]);
                Poll::Ready(response)
            }
            Some(BatchResponse::Waker(_)) | None => {
                BATCH_TICKET_POLL.add(1, &[KeyValue::new("outcome", "pending")]);
                _ = responses.insert(self.id, BatchResponse::Waker(cx.waker().clone()));
                Poll::Pending
            }
        }
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TopitionProducerId {
    topition: Topition,
    producer_id: i64,
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct BatchRequest {
    id: Uuid,
    batch: deflated::Batch,
}

impl BatchRequest {
    /// The number of records in the batch, and so the number of offsets that it takes.
    fn record_count(&self) -> i64 {
        i64::from(self.batch.last_offset_delta) + 1
    }
}

/// One write of a flush: the batch that [`combine`] merged from consecutive queued requests, and
/// those requests, in queue order.
#[derive(Clone, Debug)]
struct Run {
    batch: deflated::Batch,
    owners: Vec<BatchRequest>,
}

#[derive(Clone, Debug)]
enum BatchResponse {
    Waker(Waker),
    Response(Result<i64, Error>),
}

#[derive(Clone, Debug)]
pub struct ProduceRequestBatcher<G> {
    storage: G,
    maximum_delay: Option<Duration>,
    minimum_size: Option<usize>,

    /// The largest decoded size of a merged batch, [`MAX_DECODED_BATCH_BYTES`] outside tests.
    decoded_limit: usize,

    /// The error that a test makes every merge fail with, after [`combine`] has run.
    #[cfg(test)]
    merge_error: Option<Error>,

    requests: Arc<Mutex<BTreeMap<TopitionProducerId, Vec<BatchRequest>>>>,
    responses: Arc<Mutex<BTreeMap<Uuid, BatchResponse>>>,
}

impl<G> ProduceRequestBatcher<G> {
    fn update_metrics(&self) -> Result<()> {
        self.requests
            .lock()
            .map_err(Into::into)
            .map(|requests| requests.values().map(|queue| queue.len() as u64).sum())
            .map(|length| BATCH_REQUESTS_LENGTH.record(length, &[]))
            .and(
                self.responses
                    .lock()
                    .map_err(Into::into)
                    .map(|responses| responses.len() as u64)
                    .map(|length| BATCH_RESPONSES_LENGTH.record(length, &[])),
            )
    }
}

impl<G> ProduceRequestBatcher<G>
where
    G: Storage,
{
    pub fn new(storage: G) -> Self {
        Self {
            storage,
            minimum_size: Default::default(),
            maximum_delay: Default::default(),
            decoded_limit: MAX_DECODED_BATCH_BYTES,
            #[cfg(test)]
            merge_error: None,

            requests: Arc::new(Mutex::new(BTreeMap::new())),
            responses: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn with_minimum_size(self, minimum_size: Option<usize>) -> Self {
        Self {
            minimum_size,
            ..self
        }
    }

    pub fn with_maximum_delay(self, maximum_delay: Option<Duration>) -> Self {
        Self {
            maximum_delay,
            ..self
        }
    }

    /// Makes every merge fail with `merge_error`, because no batch that decodes makes the merge
    /// fail with an error other than an offset delta overflow.
    #[cfg(test)]
    fn with_merge_error(self, merge_error: Error) -> Self {
        Self {
            merge_error: Some(merge_error),
            ..self
        }
    }

    /// Sets a decoded-size limit below [`MAX_DECODED_BATCH_BYTES`], so that a test can fill a run
    /// with small batches.
    #[cfg(test)]
    fn with_decoded_limit(self, decoded_limit: usize) -> Self {
        Self {
            decoded_limit,
            ..self
        }
    }

    #[instrument(skip(self, transaction_id, topition, producer_id))]
    async fn send_queued(
        &self,
        id: &Uuid,
        transaction_id: Option<&str>,
        topition: &Topition,
        producer_id: i64,
    ) -> Result<(), Error> {
        let Some(queued) = self.requests.lock().map(|mut requests| {
            BATCH_REQUESTS_LENGTH
                .record(requests.values().map(|queue| queue.len() as u64).sum(), &[]);

            requests.remove(&TopitionProducerId {
                topition: topition.to_owned(),
                producer_id,
            })
        })?
        else {
            BATCH_REQUESTS_LENGTH.record(0, &[]);

            return Ok(());
        };

        debug!(owners = queued.len());

        let attributes = [KeyValue::new("topic", topition.topic.clone())];

        // We write a queue of mixed codecs one batch at a time. The merged batch takes the first
        // batch's codec, so a batch over `max.message.bytes` as sent could fit once merged.
        //
        // We also write a queue of mixed record timestamps one batch at a time. The merged batch
        // takes the first batch's timestamp type and the largest `max_timestamp`. A consumer reads
        // a `LogAppendTime` batch's `max_timestamp` as each record's timestamp, so a merge would
        // give a record a broker time other than the one its produce response returned.
        //
        // We also write an idempotent producer's queue one batch at a time. The storage answers a
        // retried batch as a duplicate only when its first and last sequence match a batch that it
        // stored, as Kafka does. After a merge it stores only the merged span, so a retry of one of
        // the batches as sent gets OUT_OF_ORDER_SEQUENCE_NUMBER, which a client treats as fatal.
        {
            let batches = queued
                .iter()
                .map(|queued| &queued.batch)
                .collect::<Vec<_>>();

            if !shares_one_codec(&batches)
                || !shares_one_log_append_time(&batches)
                || batches.iter().any(|batch| batch.is_idempotent())
            {
                let responses_by_owner = self
                    .write_each_as_sent(transaction_id, topition, queued, &attributes)
                    .await;

                return self.respond(responses_by_owner, &attributes);
            }
        }

        // A clone of a batch shares its record bytes, so keeping the requests as sent costs no
        // copy.
        let queued_as_sent = queued.clone();
        let decoded_limit = self.decoded_limit;

        let Combined { runs, rejected } =
            match offload(move || Ok(combine(queued, decoded_limit))).await {
                Ok(combined) => combined,

                // The offload failed before `combine` ran, so no batch is known to be rejected.
                Err(error) => Combined {
                    runs: Err(error),
                    rejected: Vec::new(),
                },
            };

        #[cfg(test)]
        let runs = self.merge_error.clone().map_or(runs, Err);

        // A batch that does not decode fails only its own producer, on every path below. The
        // others in the flush are written as if the bad batch had not been queued.
        let rejected_owners = rejected
            .iter()
            .map(|(owner, _)| *owner)
            .collect::<BTreeSet<_>>();

        self.respond(
            rejected
                .into_iter()
                .map(|(owner, error)| (owner, Err(error)))
                .collect(),
            &attributes,
        )?;

        let queued_as_sent = queued_as_sent
            .into_iter()
            .filter(|owner| !rejected_owners.contains(&owner.id))
            .collect::<Vec<_>>();

        let runs = match runs {
            Ok(runs) => runs,

            // One of the merged offset deltas overflows an `i32`. Kafka checks and stores each
            // batch as sent, and each one may be valid, so we write each one on its own.
            Err(Error::SansIo(nisshi_sans_io::Error::OffsetDeltaOverflow(_))) => {
                let responses_by_owner = self
                    .write_each_as_sent(transaction_id, topition, queued_as_sent, &attributes)
                    .await;

                return self.respond(responses_by_owner, &attributes);
            }

            // Every queued request that decoded shares the fate of a merge that fails, so every
            // such owner, not just the one that called `send_queued`, is told the outcome.
            Err(error) => {
                let responses_by_owner = queued_as_sent
                    .into_iter()
                    .map(|owner| (owner.id, Err(error.clone())))
                    .collect();

                return self.respond(responses_by_owner, &attributes);
            }
        };

        debug!(runs = runs.len());

        // Each run is one write, in queue order, so the offsets of the owners stay in queue order.
        for Run { batch, owners } in runs {
            let record_count = i64::from(batch.last_offset_delta) + 1;

            let responses_by_owner =
                match self.storage.produce(transaction_id, topition, batch).await {
                    // The merged batch is over `max.message.bytes`. Kafka checks and stores each batch
                    // as sent, and each one may be valid, so we write each one on its own.
                    Err(Error::MessageTooLarge { .. }) => {
                        self.write_each_as_sent(transaction_id, topition, owners, &attributes)
                            .await
                    }

                    outcome => {
                        if let Ok(base_offset) = outcome {
                            debug!(base_offset);
                            SEND_QUEUED_PRODUCED_RECORDS_COUNTER
                                .add(record_count as u64, &attributes);
                        }

                        let mut cumulative = 0;

                        owners
                            .into_iter()
                            .map(|owner| {
                                let response = outcome
                                    .as_ref()
                                    .map(|base_offset| base_offset + cumulative)
                                    .map_err(Clone::clone);

                                cumulative += owner.record_count();

                                (owner.id, response)
                            })
                            .collect()
                    }
                };

            // The owners of this run are answered when their own write is done, so they do not
            // wait for the writes of the runs after it.
            self.respond(responses_by_owner, &attributes)?;
        }

        Ok(())
    }

    /// Writes the batch of each of `owners` on its own, in queue order, and returns each owner's
    /// result.
    async fn write_each_as_sent(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        owners: Vec<BatchRequest>,
        attributes: &[KeyValue],
    ) -> Vec<(Uuid, Result<i64>)> {
        let mut responses_by_owner = Vec::with_capacity(owners.len());

        for owner in owners {
            let count = owner.record_count();
            let response = self
                .storage
                .produce(transaction_id, topition, owner.batch)
                .await;

            if response.is_ok() {
                SEND_QUEUED_PRODUCED_RECORDS_COUNTER.add(count as u64, attributes);
            }

            responses_by_owner.push((owner.id, response));
        }

        responses_by_owner
    }

    /// Hands each owner its result, and wakes an owner that is waiting.
    fn respond(
        &self,
        responses_by_owner: Vec<(Uuid, Result<i64>)>,
        attributes: &[KeyValue],
    ) -> Result<(), Error> {
        self.responses.lock().map(|mut responses| {
            for (owner, response) in responses_by_owner {
                if let Some(BatchResponse::Waker(waker)) =
                    responses.insert(owner, BatchResponse::Response(response))
                {
                    debug!(waking = %owner);
                    SEND_QUEUED_WAKE_COUNTER.add(1, attributes);
                    waker.wake();
                }
            }
        })?;

        Ok(())
    }
}

#[async_trait]
impl<G> Storage for ProduceRequestBatcher<G>
where
    G: Storage + Clone,
{
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        self.storage.register_broker(broker_registration).await
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        self.storage.create_topic(topic, validate_only).await
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        self.storage.incremental_alter_resource(resource).await
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        self.storage.delete_records(topics).await
    }

    fn delete_records_concurrency(&self) -> DeleteRecordsConcurrency {
        self.storage.delete_records_concurrency()
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        self.storage.delete_topic(topic).await
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        self.storage.brokers().await
    }

    /// Without a `maximum_delay` the batcher queues nothing, so the decoded batch goes straight
    /// to the storage. With one, the batch joins a queue and is combined with others, which
    /// decodes it again: the default drops the decoded batch.
    async fn produce_inflated(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
        inflated: inflated::Batch,
    ) -> Result<i64> {
        if self.maximum_delay.is_none() {
            return self
                .storage
                .produce_inflated(transaction_id, topition, deflated, inflated)
                .await;
        }

        drop(inflated);

        self.produce(transaction_id, topition, deflated).await
    }

    #[instrument(skip_all, fields(transaction_id, topic = topition.topic, partition = topition.partition))]
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        let Some(maximum_delay) = self.maximum_delay else {
            return self
                .storage
                .produce(transaction_id, topition, deflated)
                .await;
        };

        let start = SystemTime::now();

        let attributes = [KeyValue::new("topic", topition.topic.clone())];

        let producer_id = deflated.producer_id;

        let topition_producer_id = TopitionProducerId {
            topition: topition.to_owned(),
            producer_id,
        };

        let ticket = self.requests.lock().map(|mut requests| {
            let ticket = Ticket::new(self.clone());

            let queue = requests.entry(topition_producer_id.clone()).or_default();

            queue.push(BatchRequest {
                id: ticket.id,
                batch: deflated,
            });

            PRODUCE_REQUEST_QUEUED_COUNTER.add(1, &attributes);
            debug!(queue_len = queue.len());

            ticket
        })?;

        debug!(ticket = %ticket.id);

        let mut iteration = -1;

        loop {
            self.update_metrics()?;

            let ticket = ticket.clone();
            let id = ticket.id;

            iteration += 1;

            let queued_bytes = self
                .requests
                .lock()
                .map(|requests| {
                    requests
                        .get(&topition_producer_id)
                        .map(|queue| {
                            queue
                                .iter()
                                .map(|batch_request| batch_request.batch.record_data.len())
                                .sum::<usize>()
                        })
                        .unwrap_or_default()
                })
                .inspect(|queued_bytes| debug!(queued_bytes))?;

            if self
                .minimum_size
                .inspect(|minimum_size| debug!(minimum_size, queued_bytes))
                .is_some_and(|minimum_size| queued_bytes > minimum_size)
            {
                PRODUCE_REQUEST_MINIMUM_SIZE_TRIGGER.add(1, &attributes);

                self.send_queued(&id, transaction_id, topition, producer_id)
                    .await?;
            }

            let patience = sleep(maximum_delay);

            tokio::select! {
                response = ticket  => {
                    let elapsed = start.elapsed().map_or(0, |duration| duration.as_millis() as u64);
                    debug!(ready = %id, elapsed, iteration);
                    PRODUCE_REQUEST_YOUR_TICKET_IS_READY.add(1, &attributes);
                    PRODUCE_DURATION.record(elapsed, &attributes);
                    self.update_metrics()?;
                    return response;
                }

                _ = patience => {
                    if iteration > 1 {
                        warn!(ticket = %id, iteration);
                    }

                    PRODUCE_REQUEST_TIMEOUT_EXPIRED_TRIGGER.add(1, &attributes);
                    self.send_queued(&id, transaction_id, topition, producer_id).await?;
                }
            }
        }
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
        self.storage
            .fetch(topition, offset, min_bytes, max_bytes, isolation, deadline)
            .await
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        self.storage.offset_stage(topition).await
    }

    async fn current_offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        self.storage.current_offset_stage(topition).await
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        self.storage.list_offsets(isolation_level, offsets).await
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
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
        self.storage
            .offset_fetch(group_id, topics, require_stable)
            .await
    }

    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        self.storage.committed_offset_topitions(group_id).await
    }

    async fn pending_txn_offsets(&self, group_id: &str) -> Result<BTreeSet<Topition>> {
        self.storage.pending_txn_offsets(group_id).await
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        self.storage.metadata(topics).await
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        self.storage
            .upsert_user_scram_credential(user, mechanism, credential)
            .await
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        self.storage
            .delete_user_scram_credential(user, mechanism)
            .await
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        self.storage.user_scram_credential(user, mechanism).await
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        self.storage.describe_config(name, resource, keys).await
    }

    async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
        self.storage.list_groups().await
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        self.storage.delete_groups(group_ids).await
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
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
        self.storage
            .describe_topic_partitions(topics, partition_limit, cursor)
            .await
    }

    async fn group_with_version(&self, group_id: &str) -> Result<Option<(GroupDetail, Version)>> {
        self.storage.group_with_version(group_id).await
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        self.storage.update_group(group_id, detail, version).await
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
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
        self.storage
            .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
            .await
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        self.storage.txn_add_partitions(partitions).await
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.storage.txn_offset_commit(offsets).await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
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

/// Returns whether all of `batches` use the same compression codec.
///
/// The function skips a batch with an unknown codec, so that [`combine`] rejects it.
fn shares_one_codec(batches: &[&deflated::Batch]) -> bool {
    let mut codecs = batches.iter().filter_map(|batch| {
        BatchAttribute::try_from(batch.attributes)
            .map(|attributes| attributes.compression)
            .ok()
    });

    codecs
        .next()
        .is_none_or(|first| codecs.all(|codec| codec == first))
}

/// Returns whether all of `batches` are `CreateTime`, or all are `LogAppendTime` with the same
/// `max_timestamp`: the broker's time when it stamped the batch.
fn shares_one_log_append_time(batches: &[&deflated::Batch]) -> bool {
    let mut log_append_times =
        batches
            .iter()
            .map(|batch| match TimestampType::from(batch.attributes) {
                TimestampType::CreateTime => None,
                TimestampType::LogAppendTime => Some(batch.max_timestamp),
            });

    log_append_times
        .next()
        .is_none_or(|first| log_append_times.all(|log_append_time| log_append_time == first))
}

/// What [`combine`] made of a queue.
#[derive(Debug)]
struct Combined {
    /// The batches that decoded, merged into runs in queue order, or the error of the merge itself.
    runs: Result<Vec<Run>>,

    /// The owner and the error of each batch that does not decode. A merge that fails does not
    /// drop these.
    rejected: Vec<(Uuid, Error)>,
}

/// Merges the batches of `queued`, which share one codec, into runs. A run is one batch of
/// consecutive queued batches, in queue order, whose decoded size is at most `limit`, so that the
/// merged batch decodes. A run closes before the next batch would take it over `limit`.
///
/// A batch whose own decoded size is over `limit` is a run on its own. The decoder accepted it
/// as sent, so the storage can too.
///
/// A batch that does not decode is rejected with its own error, and the other batches merge as if
/// it had not been queued, so that one producer's bad batch fails only that producer. Kafka
/// validates each batch on its own ([`UnifiedLog.analyzeAndValidateRecords`]) and has no
/// cross-request coalescing.
///
/// This holds only for a queue that `send_queued` merges. A queue that it writes as sent, such as
/// one of mixed codecs, never reaches this function, so each of its batches goes to the storage
/// as sent.
///
/// An offset delta that overflows an `i32`, or another error from the merge itself, fails the
/// merge of the whole queue, and the result still lists each batch that did not decode.
///
/// [`UnifiedLog.analyzeAndValidateRecords`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/log/UnifiedLog.scala#L1181-L1187
#[instrument(skip_all)]
fn combine(queued: Vec<BatchRequest>, limit: usize) -> Combined {
    debug!(len = queued.len(), limit);

    let mut rejected = Vec::new();
    let mut remaining = queued.into_iter();
    let runs = merge_runs(&mut remaining, limit, &mut rejected);

    // A merge that fails stops before the end of the queue. We still decode the rest of the queue,
    // one batch at a time, so that each batch that does not decode gets its own error and none of
    // them reaches the storage.
    for request in remaining {
        if let Err(error) = decode(&request) {
            rejected.push((request.id, error));
        }
    }

    Combined { runs, rejected }
}

/// Decodes the batch of `request`, and returns it with its codec.
fn decode(request: &BatchRequest) -> Result<(Compression, inflated::Batch)> {
    let compression = BatchAttribute::try_from(request.batch.attributes)?.compression;

    inflate_produced(request.batch.clone()).map(|batch| (compression, batch))
}

/// Merges the batches that `queued` yields into runs, as [`combine`] does, and adds each batch that
/// does not decode to `rejected`. The merge stops at the first error of its own, and leaves the
/// batches after it in `queued`.
fn merge_runs(
    queued: &mut impl Iterator<Item = BatchRequest>,
    limit: usize,
    rejected: &mut Vec<(Uuid, Error)>,
) -> Result<Vec<Run>> {
    let mut runs = Vec::new();
    let mut open: Option<OpenRun> = None;

    // Inflating clears the codec, so we cache the codec of the first batch that decodes, to measure
    // and compress each run the same way.
    let mut compression: Option<Compression> = None;

    for request in queued {
        let (codec, batch) = match decode(&request) {
            Ok(decoded) => decoded,

            Err(error) => {
                rejected.push((request.id, error));
                continue;
            }
        };

        let compression = compression.get_or_insert(codec);

        open = Some(match open.take() {
            None => OpenRun::new(batch, request, compression)?,

            Some(mut run) => {
                let appended = run.sink.appended_decoded_size(&batch, compression)?;

                match run
                    .decoded_size
                    .checked_add(appended)
                    .filter(|decoded_size| *decoded_size <= limit)
                {
                    Some(decoded_size) => {
                        run.sink.append_rebased(batch)?;
                        run.decoded_size = decoded_size;
                        run.owners.push(request);
                        run
                    }

                    None => {
                        debug!(run.decoded_size, appended, limit);
                        runs.push(run.close(compression)?);
                        OpenRun::new(batch, request, compression)?
                    }
                }
            }
        });
    }

    if let (Some(run), Some(compression)) = (open, compression) {
        runs.push(run.close(&compression)?);
    }

    Ok(runs)
}

/// A run that [`combine`] is still adding batches to.
struct OpenRun {
    sink: inflated::Batch,
    decoded_size: usize,
    owners: Vec<BatchRequest>,
}

impl OpenRun {
    fn new(sink: inflated::Batch, owner: BatchRequest, compression: &Compression) -> Result<Self> {
        Ok(Self {
            decoded_size: sink.decoded_size(compression)?,
            sink,
            owners: vec![owner],
        })
    }

    /// Deflates the merged batch with `compression`, so that only one run is inflated at a time.
    fn close(self, compression: &Compression) -> Result<Run> {
        let Self {
            mut sink, owners, ..
        } = self;

        debug!(
            sink.base_offset,
            sink.last_offset_delta,
            sink.base_sequence,
            sink.max_timestamp,
            owners = owners.len()
        );

        sink.attributes = BatchAttribute::try_from(sink.attributes)?
            .compression(compression.clone())
            .into();

        deflated::Batch::try_from(sink)
            .map(|batch| Run { batch, owners })
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use bytes::Bytes;
    use nisshi_sans_io::{
        BatchAttribute, ByteSize as _, Compression,
        record::{Record, deflated, inflated},
    };
    use tokio::{task::yield_now, time::advance};

    use super::*;

    /// The topic's `max.message.bytes` in the tests whose storage enforces one.
    const MAX_MESSAGE_BYTES: usize = 1_000;

    /// Merges `batches` as [`combine`] does at the production limit, and returns the one merged
    /// batch, or `None` for no batches. The batches must all decode.
    fn combine_into_one(batches: Vec<deflated::Batch>) -> Result<Option<deflated::Batch>> {
        let queued = batches
            .into_iter()
            .map(|batch| BatchRequest {
                id: Uuid::now_v7(),
                batch,
            })
            .collect();

        let Combined { runs, rejected } = combine(queued, MAX_DECODED_BATCH_BYTES);
        assert!(rejected.is_empty(), "{rejected:?}");

        let mut runs = runs?;
        assert!(runs.len() <= 1, "{} runs", runs.len());

        Ok(runs.pop().map(|run| run.batch))
    }

    #[derive(Clone, Debug, Default)]
    struct FlightRecorder {
        produced: Arc<Mutex<BTreeMap<Topition, Vec<deflated::Batch>>>>,
        fail_next_produce: Arc<std::sync::atomic::AtomicBool>,
        max_message_bytes: Option<usize>,
    }

    impl FlightRecorder {
        fn new() -> Self {
            Self {
                produced: Arc::new(Mutex::new(BTreeMap::new())),
                fail_next_produce: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                max_message_bytes: None,
            }
        }

        /// Returns a recorder that rejects a batch larger than `max_message_bytes` bytes.
        fn with_max_message_bytes(max_message_bytes: usize) -> Self {
            Self {
                max_message_bytes: Some(max_message_bytes),
                ..Self::new()
            }
        }

        /// Cause the *next* call to `produce` to fail, simulating a storage
        /// write failure for whichever coalesced batch is in flight.
        fn fail_next_produce(&self) {
            self.fail_next_produce
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }

        fn produced(&self, topition: &Topition) -> Result<Option<Vec<inflated::Batch>>> {
            self.produced
                .as_ref()
                .lock()
                .map_err(Into::into)
                .and_then(|produced| {
                    produced
                        .get(topition)
                        .map(|produced| {
                            produced
                                .iter()
                                .map(|deflated| {
                                    inflated::Batch::try_from(deflated).map_err(Into::into)
                                })
                                .collect::<Result<Vec<_>>>()
                        })
                        .transpose()
                })
        }
    }

    #[async_trait]
    impl Storage for FlightRecorder {
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
            topition: &Topition,
            deflated: deflated::Batch,
        ) -> Result<i64> {
            if self
                .fail_next_produce
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(Error::Message("simulated storage failure".to_string()));
            }

            if let Some(max_message_bytes) = self.max_message_bytes {
                crate::max_message_bytes::ensure_batch_fits(&deflated, max_message_bytes)?;
            }

            self.produced
                .lock()
                .map(|mut produced| {
                    let batches = produced.entry(topition.to_owned()).or_default();

                    let base_offset = batches
                        .iter()
                        .map(|batch| i64::from(batch.last_offset_delta) + 1)
                        .sum();

                    batches.push(deflated);

                    base_offset
                })
                .map_err(Into::into)
        }

        async fn fetch(
            &self,
            _topition: &Topition,
            _offset: i64,
            _min_bytes: u32,
            _max_bytes: u32,
            _isolation_level: IsolationLevel,
            _deadline: Instant,
        ) -> Result<Vec<deflated::Batch>> {
            unimplemented!()
        }

        async fn offset_stage(&self, _topition: &Topition) -> Result<OffsetStage> {
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

    fn into_batch(
        attributes: i16,
        producer_id: i64,
        producer_epoch: i16,
        base_offset: i64,
        records: &[Bytes],
    ) -> Result<deflated::Batch> {
        // The batch has no sequence, so it is not idempotent, and the batcher can merge it with
        // the batches queued beside it.
        let base_sequence = -1;

        let mut inflated = inflated::Batch::builder()
            .attributes(attributes)
            .producer_id(producer_id)
            .producer_epoch(producer_epoch)
            .base_offset(base_offset)
            .last_offset_delta(records.len() as i32 - 1)
            .base_sequence(base_sequence);

        for (offset_delta, value) in records.iter().enumerate() {
            inflated = inflated.record(
                Record::builder()
                    .value(value.clone().into())
                    .offset_delta(offset_delta as i32),
            );
        }

        inflated
            .build()
            .and_then(TryInto::try_into)
            .inspect(|deflated| debug!(?deflated))
            .map_err(Into::into)
    }

    #[tokio::test(start_paused = true)]
    async fn single_produce_in_window() -> Result<()> {
        const MINIMUM_DELAY: Duration = Duration::from_secs(1);
        const ADVANCE_DELAY: Duration = Duration::from_secs(5);

        let recorder = FlightRecorder::new();
        let storage =
            ProduceRequestBatcher::new(recorder.clone()).with_maximum_delay(Some(MINIMUM_DELAY));

        let producer_id = 54345;
        let producer_epoch = 32123;
        let base_offset = 0;
        let attributes: i16 = BatchAttribute::default().into();

        let transaction_id = None;
        let abc0 = Topition::new("abc", 0);

        const A: Bytes = Bytes::from_static(b"a");
        const B: Bytes = Bytes::from_static(b"b");
        const C: Bytes = Bytes::from_static(b"c");

        let batch_a = {
            let storage = storage.clone();
            let abc0 = abc0.clone();

            tokio::spawn(async move {
                storage
                    .produce(
                        transaction_id,
                        &abc0,
                        into_batch(
                            attributes,
                            producer_id,
                            producer_epoch,
                            base_offset,
                            &[A, B, C],
                        )?,
                    )
                    .await
            })
        };

        advance(ADVANCE_DELAY).await;
        yield_now().await;

        let response_a = batch_a
            .await
            .expect("join_handle")
            .inspect(|produce_response| debug!(?produce_response))?;
        assert_eq!(0, response_a);

        let sent = recorder.produced(&abc0)?.unwrap();
        assert_eq!(1, sent.len());
        assert_eq!(3, sent[0].records.len());
        assert_eq!(Some(A), sent[0].records[0].value());
        assert_eq!(Some(B), sent[0].records[1].value());
        assert_eq!(Some(C), sent[0].records[2].value());

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn two_produces_in_window() -> Result<()> {
        const MINIMUM_DELAY: Duration = Duration::from_secs(1);
        const ADVANCE_DELAY: Duration = Duration::from_secs(5);

        let recorder = FlightRecorder::new();
        let storage =
            ProduceRequestBatcher::new(recorder.clone()).with_maximum_delay(Some(MINIMUM_DELAY));

        let producer_id = 54345;
        let producer_epoch = 32123;
        let base_offset = 0;
        let attributes: i16 = BatchAttribute::default().into();

        let transaction_id = None;
        let abc0 = Topition::new("abc", 0);

        const A: Bytes = Bytes::from_static(b"a");
        const B: Bytes = Bytes::from_static(b"b");
        const C: Bytes = Bytes::from_static(b"c");

        let batch_a = {
            let storage = storage.clone();
            let abc0 = abc0.clone();

            tokio::spawn(async move {
                storage
                    .produce(
                        transaction_id,
                        &abc0,
                        into_batch(
                            attributes,
                            producer_id,
                            producer_epoch,
                            base_offset,
                            &[A, B, C],
                        )?,
                    )
                    .await
            })
        };

        const D: Bytes = Bytes::from_static(b"d");
        const E: Bytes = Bytes::from_static(b"e");

        let batch_b = {
            let storage = storage.clone();
            let abc0 = abc0.clone();

            tokio::spawn(async move {
                storage
                    .produce(
                        transaction_id,
                        &abc0,
                        into_batch(
                            attributes,
                            producer_id,
                            producer_epoch,
                            base_offset,
                            &[D, E],
                        )?,
                    )
                    .await
            })
        };

        advance(ADVANCE_DELAY).await;
        yield_now().await;

        let response_a = batch_a
            .await
            .expect("join_handle")
            .inspect(|produce_response| debug!(?produce_response))?;
        assert_eq!(0, response_a);

        let response_b = batch_b
            .await
            .expect("join_handle")
            .inspect(|produce_response| debug!(?produce_response))?;
        // Batch A occupies offsets 0..=2 (3 records) in the merged write, so
        // batch B's own records start at offset 3, not at the merged
        // write's base offset (0).
        assert_eq!(3, response_b);

        let sent = recorder.produced(&abc0)?.unwrap();
        assert_eq!(1, sent.len());
        assert_eq!(5, sent[0].records.len());
        assert_eq!(Some(A), sent[0].records[0].value());
        assert_eq!(Some(B), sent[0].records[1].value());
        assert_eq!(Some(C), sent[0].records[2].value());
        assert_eq!(Some(D), sent[0].records[3].value());
        assert_eq!(Some(E), sent[0].records[4].value());

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn coalesced_produce_failure_is_delivered_to_every_producer() -> Result<()> {
        const MINIMUM_DELAY: Duration = Duration::from_secs(1);
        const ADVANCE_DELAY: Duration = Duration::from_secs(5);

        let recorder = FlightRecorder::new();
        let storage =
            ProduceRequestBatcher::new(recorder.clone()).with_maximum_delay(Some(MINIMUM_DELAY));

        // The single underlying storage write that the flush performs (for
        // both coalesced requests together) will fail.
        recorder.fail_next_produce();

        let producer_id = 54345;
        let producer_epoch = 32123;
        let base_offset = 0;
        let attributes: i16 = BatchAttribute::default().into();

        let transaction_id = None;
        let abc0 = Topition::new("abc", 0);

        const A: Bytes = Bytes::from_static(b"a");
        const B: Bytes = Bytes::from_static(b"b");
        const C: Bytes = Bytes::from_static(b"c");

        let batch_a = {
            let storage = storage.clone();
            let abc0 = abc0.clone();

            tokio::spawn(async move {
                storage
                    .produce(
                        transaction_id,
                        &abc0,
                        into_batch(
                            attributes,
                            producer_id,
                            producer_epoch,
                            base_offset,
                            &[A, B, C],
                        )?,
                    )
                    .await
            })
        };

        const D: Bytes = Bytes::from_static(b"d");
        const E: Bytes = Bytes::from_static(b"e");

        let batch_b = {
            let storage = storage.clone();
            let abc0 = abc0.clone();

            tokio::spawn(async move {
                storage
                    .produce(
                        transaction_id,
                        &abc0,
                        into_batch(
                            attributes,
                            producer_id,
                            producer_epoch,
                            base_offset,
                            &[D, E],
                        )?,
                    )
                    .await
            })
        };

        advance(ADVANCE_DELAY).await;
        yield_now().await;

        // Both coalesced requests share a single underlying write, so both
        // must observe that write's failure. Neither one should hang
        // waiting for a response that only the other producer's flush
        // caller would otherwise have received. Each await is bounded by a
        // timeout so that a regression here fails this test outright,
        // rather than hanging the test binary indefinitely (this repo has
        // no nextest slow-timeout/terminate-after configured); under
        // `start_paused`, tokio auto-advances the virtual clock while
        // idling on timers, so a genuine hang still surfaces quickly.
        let response_a = tokio::time::timeout(Duration::from_secs(60), batch_a)
            .await
            .expect("producer A must not hang waiting for the failed write's outcome")
            .expect("join_handle");
        assert!(
            response_a.is_err(),
            "expected producer A to observe the failed write, got {response_a:?}"
        );

        let response_b = tokio::time::timeout(Duration::from_secs(60), batch_b)
            .await
            .expect("producer B must not hang waiting for the failed write's outcome")
            .expect("join_handle");
        assert!(
            response_b.is_err(),
            "expected producer B to observe the failed write, got {response_b:?}"
        );

        // Nothing should have actually landed in storage.
        assert_eq!(None, recorder.produced(&abc0)?);

        Ok(())
    }

    /// The producer id of the batches in the tests of `max.message.bytes`.
    const PRODUCER_ID: i64 = 54345;

    /// Returns a batch from `producer_id` with `attributes`, of one record whose value is `value`.
    fn batch_of(producer_id: i64, attributes: i16, value: &Bytes) -> Result<deflated::Batch> {
        into_batch(
            attributes,
            producer_id,
            32123,
            0,
            std::slice::from_ref(value),
        )
    }

    /// Returns `len` pseudo-random bytes, which gzip cannot shrink. Each call advances `seed`.
    fn pseudo_random_bytes(seed: &mut u32, len: usize) -> Bytes {
        (0..len)
            .map(|_| {
                *seed ^= *seed << 13;
                *seed ^= *seed >> 17;
                *seed ^= *seed << 5;
                *seed as u8
            })
            .collect()
    }

    /// Spawns a produce of `batch` to `topition` through `batcher`, and returns its task.
    fn spawn_produce(
        batcher: &ProduceRequestBatcher<FlightRecorder>,
        topition: &Topition,
        batch: deflated::Batch,
    ) -> tokio::task::JoinHandle<Result<i64>> {
        let batcher = batcher.clone();
        let topition = topition.clone();

        tokio::spawn(async move { batcher.produce(None, &topition, batch).await })
    }

    /// A queue that mixes codecs is written one batch at a time, so the uncompressed batch over the
    /// limit is rejected. A merged batch takes the gzip batch's codec, and would fit. Each request
    /// receives the result of its own batch.
    #[tokio::test(start_paused = true)]
    async fn queue_with_mixed_codecs_is_written_as_sent() -> Result<()> {
        const MINIMUM_DELAY: Duration = Duration::from_secs(1);
        const ADVANCE_DELAY: Duration = Duration::from_secs(5);

        let recorder = FlightRecorder::with_max_message_bytes(MAX_MESSAGE_BYTES);
        let batcher =
            ProduceRequestBatcher::new(recorder.clone()).with_maximum_delay(Some(MINIMUM_DELAY));
        let topition = Topition::new("abc", 0);

        let gzip_attributes: i16 = BatchAttribute::default()
            .compression(Compression::Gzip)
            .into();
        // Every non-idempotent client has this producer id, so two such clients share a queue.
        let non_idempotent_producer_id = -1;

        let compressible_value = Bytes::from(vec![b'a'; 2 * MAX_MESSAGE_BYTES]);
        let gzip_batch = batch_of(
            non_idempotent_producer_id,
            gzip_attributes,
            &compressible_value,
        )?;
        assert!(gzip_batch.size_in_bytes()? < MAX_MESSAGE_BYTES);

        let oversized_value = Bytes::from(vec![b'b'; 2 * MAX_MESSAGE_BYTES]);
        let uncompressed_batch_over_limit = batch_of(
            non_idempotent_producer_id,
            BatchAttribute::default().into(),
            &oversized_value,
        )?;

        let gzip_produce = spawn_produce(&batcher, &topition, gzip_batch);
        // Lets the first request join the queue before the second, so their order in it is fixed.
        yield_now().await;
        let uncompressed_produce =
            spawn_produce(&batcher, &topition, uncompressed_batch_over_limit);

        // The clock is paused, so the batching timer fires only here. A request then flushes the
        // queue that holds both batches.
        advance(ADVANCE_DELAY).await;
        yield_now().await;

        assert_matches!(
            tokio::time::timeout(Duration::from_secs(60), gzip_produce).await,
            Ok(Ok(Ok(0)))
        );
        assert_matches!(
            tokio::time::timeout(Duration::from_secs(60), uncompressed_produce).await,
            Ok(Ok(Err(Error::MessageTooLarge { .. })))
        );

        let stored_batches = recorder.produced(&topition)?.unwrap_or_default();
        assert_eq!(1, stored_batches.len());
        assert_eq!(
            Some(compressible_value),
            stored_batches[0].records[0].value()
        );

        Ok(())
    }

    /// Returns an idempotent batch of one record from [`PRODUCER_ID`], at `base_sequence`.
    fn idempotent_batch_of(base_sequence: i32) -> Result<deflated::Batch> {
        inflated::Batch::builder()
            .producer_id(PRODUCER_ID)
            .producer_epoch(0)
            .base_sequence(base_sequence)
            .record(Record::builder().value(Bytes::from_static(b"value").into()))
            .build()
            .and_then(TryInto::try_into)
            .map_err(Into::into)
    }

    /// An idempotent producer's queue is written one batch at a time, so the storage stores each
    /// batch with the sequences that the producer sent, and can recognise a retry of each one.
    #[tokio::test(start_paused = true)]
    async fn idempotent_queue_is_written_as_sent() -> Result<()> {
        const MINIMUM_DELAY: Duration = Duration::from_secs(1);
        const ADVANCE_DELAY: Duration = Duration::from_secs(5);

        let recorder = FlightRecorder::with_max_message_bytes(MAX_MESSAGE_BYTES);
        let batcher =
            ProduceRequestBatcher::new(recorder.clone()).with_maximum_delay(Some(MINIMUM_DELAY));
        let topition = Topition::new("abc", 0);

        let first_produce = spawn_produce(&batcher, &topition, idempotent_batch_of(0)?);
        // Lets the first request join the queue before the second, so their order in it is fixed.
        yield_now().await;
        let second_produce = spawn_produce(&batcher, &topition, idempotent_batch_of(1)?);

        // The clock is paused, so the batching timer fires only here. A request then flushes the
        // queue that holds both batches.
        advance(ADVANCE_DELAY).await;
        yield_now().await;

        assert_matches!(
            tokio::time::timeout(Duration::from_secs(60), first_produce).await,
            Ok(Ok(Ok(0)))
        );
        assert_matches!(
            tokio::time::timeout(Duration::from_secs(60), second_produce).await,
            Ok(Ok(Ok(1)))
        );

        let stored_sequences = recorder
            .produced(&topition)?
            .unwrap_or_default()
            .iter()
            .map(|batch| batch.base_sequence)
            .collect::<Vec<_>>();
        assert_eq!(vec![0, 1], stored_sequences);

        Ok(())
    }

    /// The merged batch keeps its batches' gzip codec, so two gzip batches that fit the limit
    /// together stay merged. Their uncompressed records are larger than the limit.
    #[tokio::test(start_paused = true)]
    async fn compressed_batches_that_fit_together_stay_merged() -> Result<()> {
        const MINIMUM_DELAY: Duration = Duration::from_secs(1);
        const ADVANCE_DELAY: Duration = Duration::from_secs(5);

        let recorder = FlightRecorder::with_max_message_bytes(MAX_MESSAGE_BYTES);
        let batcher =
            ProduceRequestBatcher::new(recorder.clone()).with_maximum_delay(Some(MINIMUM_DELAY));
        let topition = Topition::new("abc", 0);

        let gzip_attributes: i16 = BatchAttribute::default()
            .compression(Compression::Gzip)
            .into();
        let compressible_value = Bytes::from(vec![b'a'; MAX_MESSAGE_BYTES]);

        let first_produce = spawn_produce(
            &batcher,
            &topition,
            batch_of(PRODUCER_ID, gzip_attributes, &compressible_value)?,
        );
        // Lets the first request join the queue before the second, so their order in it is fixed.
        yield_now().await;
        let second_produce = spawn_produce(
            &batcher,
            &topition,
            batch_of(PRODUCER_ID, gzip_attributes, &compressible_value)?,
        );

        // The clock is paused, so the batching timer fires only here. A request then flushes the
        // queue that holds both batches.
        advance(ADVANCE_DELAY).await;
        yield_now().await;

        assert_matches!(
            tokio::time::timeout(Duration::from_secs(60), first_produce).await,
            Ok(Ok(Ok(0)))
        );
        assert_matches!(
            tokio::time::timeout(Duration::from_secs(60), second_produce).await,
            Ok(Ok(Ok(1)))
        );

        let stored_batches = recorder
            .produced
            .lock()?
            .get(&topition)
            .cloned()
            .unwrap_or_default();
        assert_eq!(1, stored_batches.len());
        assert_eq!(
            Compression::Gzip,
            BatchAttribute::try_from(stored_batches[0].attributes)?.compression
        );

        Ok(())
    }

    /// Returns a `LogAppendTime` batch of one record from [`PRODUCER_ID`], with the producer's
    /// `base_timestamp`, stamped at `log_append_time`.
    fn log_append_time_batch(base_timestamp: i64, log_append_time: i64) -> Result<deflated::Batch> {
        inflated::Batch::builder()
            .attributes(
                BatchAttribute::default()
                    .timestamp(TimestampType::LogAppendTime)
                    .into(),
            )
            .producer_id(PRODUCER_ID)
            .base_timestamp(base_timestamp)
            .max_timestamp(log_append_time)
            .record(
                Record::builder()
                    .value(Some(Bytes::from_static(b"abc")))
                    .offset_delta(0),
            )
            .build()
            .and_then(TryInto::try_into)
            .map_err(Into::into)
    }

    /// Two `LogAppendTime` batches that the broker stamped at the same time merge into one, even
    /// when the difference of their producers' base timestamps overflows an `i64`.
    #[test]
    fn log_append_time_batches_merge_at_the_extremes_of_base_timestamp() -> Result<()> {
        const LOG_APPEND_TIME: i64 = 1_000;

        let combined = combine_into_one(vec![
            log_append_time_batch(i64::MAX, LOG_APPEND_TIME)?,
            log_append_time_batch(i64::MIN, LOG_APPEND_TIME)?,
        ])?
        .expect("two batches merge into one");

        assert_eq!(1, combined.last_offset_delta);
        assert_eq!(LOG_APPEND_TIME, combined.max_timestamp);

        Ok(())
    }

    /// Two `LogAppendTime` batches that the broker stamped at different times are written one at a
    /// time, so each record keeps the broker time of its own produce request.
    #[tokio::test(start_paused = true)]
    async fn log_append_time_batches_of_different_times_are_written_as_sent() -> Result<()> {
        const MINIMUM_DELAY: Duration = Duration::from_secs(1);
        const ADVANCE_DELAY: Duration = Duration::from_secs(5);
        const FIRST_LOG_APPEND_TIME: i64 = 1_000;
        const SECOND_LOG_APPEND_TIME: i64 = 2_000;

        let recorder = FlightRecorder::new();
        let batcher =
            ProduceRequestBatcher::new(recorder.clone()).with_maximum_delay(Some(MINIMUM_DELAY));
        let topition = Topition::new("abc", 0);

        let first_produce = spawn_produce(
            &batcher,
            &topition,
            log_append_time_batch(FIRST_LOG_APPEND_TIME, FIRST_LOG_APPEND_TIME)?,
        );
        // Lets the first request join the queue before the second, so their order in it is fixed.
        yield_now().await;
        let second_produce = spawn_produce(
            &batcher,
            &topition,
            log_append_time_batch(SECOND_LOG_APPEND_TIME, SECOND_LOG_APPEND_TIME)?,
        );

        // The clock is paused, so the batching timer fires only here. A request then flushes the
        // queue that holds both batches.
        advance(ADVANCE_DELAY).await;
        yield_now().await;

        assert_matches!(
            tokio::time::timeout(Duration::from_secs(60), first_produce).await,
            Ok(Ok(Ok(0)))
        );
        assert_matches!(
            tokio::time::timeout(Duration::from_secs(60), second_produce).await,
            Ok(Ok(Ok(1)))
        );

        let stored_log_append_times = recorder
            .produced(&topition)?
            .unwrap_or_default()
            .iter()
            .map(|batch| batch.max_timestamp)
            .collect::<Vec<_>>();
        assert_eq!(
            vec![FIRST_LOG_APPEND_TIME, SECOND_LOG_APPEND_TIME],
            stored_log_append_times
        );

        Ok(())
    }

    /// Two gzip batches are written one at a time when each fits the limit but their merged batch
    /// is over it. Each request receives its own offset. The batches are stored in queue order.
    #[tokio::test(start_paused = true)]
    async fn merged_batch_over_max_message_bytes_is_written_as_sent() -> Result<()> {
        const MINIMUM_DELAY: Duration = Duration::from_secs(1);
        const ADVANCE_DELAY: Duration = Duration::from_secs(5);

        let recorder = FlightRecorder::with_max_message_bytes(MAX_MESSAGE_BYTES);
        let batcher =
            ProduceRequestBatcher::new(recorder.clone()).with_maximum_delay(Some(MINIMUM_DELAY));
        let topition = Topition::new("abc", 0);

        let gzip_attributes: i16 = BatchAttribute::default()
            .compression(Compression::Gzip)
            .into();
        let mut seed = 0x2545_f491_u32;
        let first_record_value = pseudo_random_bytes(&mut seed, MAX_MESSAGE_BYTES * 6 / 10);
        let second_record_value = pseudo_random_bytes(&mut seed, MAX_MESSAGE_BYTES * 6 / 10);

        let first_batch = batch_of(PRODUCER_ID, gzip_attributes, &first_record_value)?;
        let second_batch = batch_of(PRODUCER_ID, gzip_attributes, &second_record_value)?;
        assert!(first_batch.size_in_bytes()? <= MAX_MESSAGE_BYTES);
        assert!(second_batch.size_in_bytes()? <= MAX_MESSAGE_BYTES);
        assert!(first_batch.size_in_bytes()? + second_batch.size_in_bytes()? > MAX_MESSAGE_BYTES);

        let first_produce = spawn_produce(&batcher, &topition, first_batch);
        // Lets the first request join the queue before the second, so their order in it is fixed.
        yield_now().await;
        let second_produce = spawn_produce(&batcher, &topition, second_batch);

        // The clock is paused, so the batching timer fires only here. A request then flushes the
        // queue that holds both batches.
        advance(ADVANCE_DELAY).await;
        yield_now().await;

        assert_matches!(
            tokio::time::timeout(Duration::from_secs(60), first_produce).await,
            Ok(Ok(Ok(0)))
        );
        assert_matches!(
            tokio::time::timeout(Duration::from_secs(60), second_produce).await,
            Ok(Ok(Ok(1)))
        );

        let stored_batches = recorder.produced(&topition)?.unwrap_or_default();
        assert_eq!(2, stored_batches.len());
        assert_eq!(
            Some(first_record_value),
            stored_batches[0].records[0].value()
        );
        assert_eq!(
            Some(second_record_value),
            stored_batches[1].records[0].value()
        );

        Ok(())
    }

    /// Two batches are written one at a time when a record offset delta of the second overflows
    /// once that batch is appended to the first. Kafka accepts each batch as sent, so each request
    /// receives its own offset, and the record keeps the offset delta that its producer sent.
    #[tokio::test(start_paused = true)]
    async fn merged_batch_whose_offset_delta_overflows_is_written_as_sent() -> Result<()> {
        const MINIMUM_DELAY: Duration = Duration::from_secs(1);
        const ADVANCE_DELAY: Duration = Duration::from_secs(5);

        let recorder = FlightRecorder::new();
        let batcher =
            ProduceRequestBatcher::new(recorder.clone()).with_maximum_delay(Some(MINIMUM_DELAY));
        let topition = Topition::new("abc", 0);

        let first_batch = batch_of(
            PRODUCER_ID,
            BatchAttribute::default().into(),
            &Bytes::from_static(b"first"),
        )?;
        // The batch has no sequence, so the batcher can merge it with the first.
        let batch_with_maximum_offset_delta = inflated::Batch::builder()
            .producer_id(PRODUCER_ID)
            .base_sequence(-1)
            .record(
                Record::builder()
                    .offset_delta(i32::MAX)
                    .value(Bytes::from_static(b"second").into()),
            )
            .build()
            .and_then(TryInto::try_into)?;

        let first_produce = spawn_produce(&batcher, &topition, first_batch);
        // Lets the first request join the queue before the second, so their order in it is fixed.
        yield_now().await;
        let second_produce = spawn_produce(&batcher, &topition, batch_with_maximum_offset_delta);

        // The clock is paused, so the batching timer fires only here. A request then flushes the
        // queue that holds both batches.
        advance(ADVANCE_DELAY).await;
        yield_now().await;

        assert_matches!(
            tokio::time::timeout(Duration::from_secs(60), first_produce).await,
            Ok(Ok(Ok(0)))
        );
        assert_matches!(
            tokio::time::timeout(Duration::from_secs(60), second_produce).await,
            Ok(Ok(Ok(1)))
        );

        let stored_batches = recorder.produced(&topition)?.unwrap_or_default();
        assert_eq!(2, stored_batches.len());
        assert_eq!(i32::MAX, stored_batches[1].records[0].offset_delta);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn undecodable_batch_fails_alone_and_the_others_in_the_flush_are_written() -> Result<()> {
        // One producer's batch carries a reserved compression attribute, so it does not decode.
        // Kafka validates each batch on its own, so that producer gets its own error and the other
        // producers in the flush still get their offsets.
        const MINIMUM_DELAY: Duration = Duration::from_secs(1);
        const ADVANCE_DELAY: Duration = Duration::from_secs(5);

        let recorder = FlightRecorder::new();
        let storage =
            ProduceRequestBatcher::new(recorder.clone()).with_maximum_delay(Some(MINIMUM_DELAY));

        let producer_id = 54345;
        let producer_epoch = 32123;
        let base_offset = 0;
        let attributes: i16 = BatchAttribute::default().into();

        let transaction_id = None;
        let abc0 = Topition::new("abc", 0);

        const A: Bytes = Bytes::from_static(b"a");
        const B: Bytes = Bytes::from_static(b"b");
        const C: Bytes = Bytes::from_static(b"c");

        let batch_a = {
            let storage = storage.clone();
            let abc0 = abc0.clone();

            tokio::spawn(async move {
                storage
                    .produce(
                        transaction_id,
                        &abc0,
                        into_batch(
                            attributes,
                            producer_id,
                            producer_epoch,
                            base_offset,
                            &[A, B, C],
                        )?,
                    )
                    .await
            })
        };

        const D: Bytes = Bytes::from_static(b"d");
        const E: Bytes = Bytes::from_static(b"e");

        let batch_b = {
            let storage = storage.clone();
            let abc0 = abc0.clone();

            tokio::spawn(async move {
                // Build a structurally valid batch, then corrupt its
                // attributes with a reserved/unknown compression code
                // (the low 3 bits, 5-7, are reserved). The batch was
                // already compressed with a *valid* codec when built, so
                // this only breaks decoding it back during `combine`, not
                // its construction here.
                let mut malformed = into_batch(
                    attributes,
                    producer_id,
                    producer_epoch,
                    base_offset,
                    &[D, E],
                )?;
                malformed.attributes |= 0b101;

                storage.produce(transaction_id, &abc0, malformed).await
            })
        };

        advance(ADVANCE_DELAY).await;
        yield_now().await;

        let response_a = tokio::time::timeout(Duration::from_secs(60), batch_a)
            .await
            .expect("producer A must not hang")
            .expect("join_handle");
        assert_matches!(response_a, Ok(0));

        let response_b = tokio::time::timeout(Duration::from_secs(60), batch_b)
            .await
            .expect("producer B must not hang")
            .expect("join_handle");
        assert!(response_b.is_err(), "{response_b:?}");

        // Only the good batch landed in storage.
        assert_eq!(
            1,
            recorder.produced(&abc0)?.map_or(0, |batches| batches.len())
        );

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn head_of_queue_batch_with_truncated_records_fails_alone() -> Result<()> {
        let recorder = FlightRecorder::new();
        let batcher = ProduceRequestBatcher::new(recorder.clone())
            .with_maximum_delay(Some(Duration::from_secs(1)));

        let attributes: i16 = BatchAttribute::default().into();
        let abc0 = Topition::new("abc", 0);

        let mut batches = vec![
            into_batch(attributes, -1, -1, 0, &[Bytes::from_static(b"a")])?,
            into_batch(attributes, -1, -1, 0, &[Bytes::from_static(b"b")])?,
            into_batch(attributes, -1, -1, 0, &[Bytes::from_static(b"c")])?,
        ];

        // The first batch is the head of the queue, and half of its records are missing.
        let len = batches[0].record_data.len();
        batches[0].record_data = batches[0].record_data.slice(0..len / 2);

        let mut produces = Vec::new();
        for batch in batches {
            // Each request joins the queue before the next, so their order in it is fixed.
            produces.push(spawn_produce(&batcher, &abc0, batch));
            yield_now().await;
        }

        advance(Duration::from_secs(5)).await;
        yield_now().await;

        let mut responses = Vec::new();
        for produce in produces {
            responses.push(produce.await.expect("join_handle"));
        }

        assert_matches!(responses[0], Err(Error::Api(ErrorCode::InvalidRecord)));
        assert_matches!(responses[1], Ok(0));
        assert_matches!(responses[2], Ok(1));
        assert_eq!(
            1,
            recorder.produced(&abc0)?.map_or(0, |batches| batches.len())
        );

        Ok(())
    }

    /// A batch that does not decode, from the middle of the queue, fails alone. Each producer after
    /// it keeps the offset of its own first record, as if the bad batch had not been queued.
    #[tokio::test(start_paused = true)]
    async fn undecodable_batch_in_the_middle_of_a_queue_fails_alone() -> Result<()> {
        let recorder = FlightRecorder::new();
        let batcher = ProduceRequestBatcher::new(recorder.clone())
            .with_maximum_delay(Some(Duration::from_secs(1)));

        let attributes: i16 = BatchAttribute::default().into();
        let abc0 = Topition::new("abc", 0);

        let mut batches = vec![
            into_batch(
                attributes,
                -1,
                -1,
                0,
                &[Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            )?,
            into_batch(attributes, -1, -1, 0, &[Bytes::from_static(b"c")])?,
            into_batch(
                attributes,
                -1,
                -1,
                0,
                &[
                    Bytes::from_static(b"d"),
                    Bytes::from_static(b"e"),
                    Bytes::from_static(b"f"),
                ],
            )?,
        ];

        let len = batches[1].record_data.len();
        batches[1].record_data = batches[1].record_data.slice(0..len / 2);

        let mut produces = Vec::new();
        for batch in batches {
            // Each request joins the queue before the next, so their order in it is fixed.
            produces.push(spawn_produce(&batcher, &abc0, batch));
            yield_now().await;
        }

        advance(Duration::from_secs(5)).await;
        yield_now().await;

        let mut responses = Vec::new();
        for produce in produces {
            responses.push(produce.await.expect("join_handle"));
        }

        assert_matches!(responses[0], Ok(0));
        assert_matches!(responses[1], Err(Error::Api(ErrorCode::InvalidRecord)));
        assert_matches!(responses[2], Ok(2));

        let stored_batches = recorder.produced(&abc0)?.unwrap_or_default();
        assert_eq!(1, stored_batches.len());
        assert_eq!(5, stored_batches[0].records.len());

        Ok(())
    }

    /// A flush whose every batch does not decode answers each producer with its own error and
    /// writes nothing.
    #[tokio::test(start_paused = true)]
    async fn queue_of_only_undecodable_batches_writes_nothing() -> Result<()> {
        let recorder = FlightRecorder::new();
        let batcher = ProduceRequestBatcher::new(recorder.clone())
            .with_maximum_delay(Some(Duration::from_secs(1)));

        let attributes: i16 = BatchAttribute::default().into();
        let abc0 = Topition::new("abc", 0);

        let mut batches = vec![
            into_batch(attributes, -1, -1, 0, &[Bytes::from_static(b"a")])?,
            into_batch(attributes, -1, -1, 0, &[Bytes::from_static(b"b")])?,
        ];

        for batch in &mut batches {
            let len = batch.record_data.len();
            batch.record_data = batch.record_data.slice(0..len / 2);
        }

        let mut produces = Vec::new();
        for batch in batches {
            produces.push(spawn_produce(&batcher, &abc0, batch));
            yield_now().await;
        }

        advance(Duration::from_secs(5)).await;
        yield_now().await;

        for produce in produces {
            assert_matches!(
                produce.await.expect("join_handle"),
                Err(Error::Api(ErrorCode::InvalidRecord))
            );
        }

        assert_eq!(None, recorder.produced(&abc0)?);

        Ok(())
    }

    /// A merge that fails still leaves a batch that does not decode out of the storage: the batches
    /// that decode are written one at a time, and the bad one gets its own error.
    #[tokio::test(start_paused = true)]
    async fn undecodable_batch_is_not_written_as_sent_when_the_merge_overflows() -> Result<()> {
        let recorder = FlightRecorder::new();
        let batcher = ProduceRequestBatcher::new(recorder.clone())
            .with_maximum_delay(Some(Duration::from_secs(1)));
        let topition = Topition::new("abc", 0);

        let first_batch = batch_of(
            PRODUCER_ID,
            BatchAttribute::default().into(),
            &Bytes::from_static(b"first"),
        )?;
        let batch_with_maximum_offset_delta = inflated::Batch::builder()
            .producer_id(PRODUCER_ID)
            .base_sequence(-1)
            .record(
                Record::builder()
                    .offset_delta(i32::MAX)
                    .value(Bytes::from_static(b"second").into()),
            )
            .build()
            .and_then(TryInto::try_into)?;

        let mut undecodable_batch = batch_of(
            PRODUCER_ID,
            BatchAttribute::default().into(),
            &Bytes::from_static(b"third"),
        )?;
        let len = undecodable_batch.record_data.len();
        undecodable_batch.record_data = undecodable_batch.record_data.slice(0..len / 2);

        let first_produce = spawn_produce(&batcher, &topition, first_batch);
        yield_now().await;
        let second_produce = spawn_produce(&batcher, &topition, batch_with_maximum_offset_delta);
        yield_now().await;
        let third_produce = spawn_produce(&batcher, &topition, undecodable_batch);

        advance(Duration::from_secs(5)).await;
        yield_now().await;

        assert_matches!(first_produce.await.expect("join_handle"), Ok(0));
        assert_matches!(second_produce.await.expect("join_handle"), Ok(1));
        assert_matches!(
            third_produce.await.expect("join_handle"),
            Err(Error::Api(ErrorCode::InvalidRecord))
        );

        // The two batches that decode, and not the third.
        assert_eq!(
            2,
            recorder
                .produced(&topition)?
                .map_or(0, |batches| batches.len())
        );

        Ok(())
    }

    /// A merge that fails with an error other than an offset delta overflow answers each producer
    /// whose batch decoded with that error, and the producer whose batch does not decode with its
    /// own error. Nothing is written.
    #[tokio::test(start_paused = true)]
    async fn merge_error_fails_every_decoded_producer_and_keeps_the_rejected_error() -> Result<()> {
        let recorder = FlightRecorder::new();
        let batcher = ProduceRequestBatcher::new(recorder.clone())
            .with_maximum_delay(Some(Duration::from_secs(1)))
            .with_merge_error(Error::Api(ErrorCode::UnknownServerError));
        let topition = Topition::new("abc", 0);
        let attributes: i16 = BatchAttribute::default().into();

        let first_batch = batch_of(PRODUCER_ID, attributes, &Bytes::from_static(b"first"))?;

        let mut undecodable_batch =
            batch_of(PRODUCER_ID, attributes, &Bytes::from_static(b"second"))?;
        let len = undecodable_batch.record_data.len();
        undecodable_batch.record_data = undecodable_batch.record_data.slice(0..len / 2);

        let third_batch = batch_of(PRODUCER_ID, attributes, &Bytes::from_static(b"third"))?;

        let first_produce = spawn_produce(&batcher, &topition, first_batch);
        yield_now().await;
        let second_produce = spawn_produce(&batcher, &topition, undecodable_batch);
        yield_now().await;
        let third_produce = spawn_produce(&batcher, &topition, third_batch);

        advance(Duration::from_secs(5)).await;
        yield_now().await;

        assert_matches!(
            first_produce.await.expect("join_handle"),
            Err(Error::Api(ErrorCode::UnknownServerError))
        );
        assert_matches!(
            second_produce.await.expect("join_handle"),
            Err(Error::Api(ErrorCode::InvalidRecord))
        );
        assert_matches!(
            third_produce.await.expect("join_handle"),
            Err(Error::Api(ErrorCode::UnknownServerError))
        );

        assert_eq!(None, recorder.produced(&topition)?);

        Ok(())
    }

    // The test runtime has a single thread. If `send_queued` combined the
    // batch inline, the produce task would decode, store and resolve its
    // ticket within its first poll, so it would already be finished when
    // `yield_now` returns. Offloaded, it is still waiting on the decode.
    #[tokio::test]
    async fn combine_runs_off_the_runtime_thread() -> Result<()> {
        const RECORDS: usize = 1_000;
        const RECORD_SIZE: usize = 1_024;

        let recorder = FlightRecorder::new();
        let storage = ProduceRequestBatcher::new(recorder.clone())
            .with_maximum_delay(Some(Duration::from_secs(1)))
            .with_minimum_size(Some(0));

        let abc0 = Topition::new("abc", 0);
        let attributes: i16 = BatchAttribute::default()
            .compression(Compression::Gzip)
            .into();

        // Pseudo-random values, so gzip has real work to do on decode.
        let mut seed = 0x2545_f491_u32;
        let values = (0..RECORDS)
            .map(|_| {
                (0..RECORD_SIZE)
                    .map(|_| {
                        seed ^= seed << 13;
                        seed ^= seed >> 17;
                        seed ^= seed << 5;
                        seed as u8
                    })
                    .collect::<Bytes>()
            })
            .collect::<Vec<_>>();

        let batch = into_batch(attributes, 54345, 32123, 0, &values)?;

        let produce = {
            let storage = storage.clone();
            let abc0 = abc0.clone();

            tokio::spawn(async move { storage.produce(None, &abc0, batch).await })
        };

        yield_now().await;

        assert!(
            !produce.is_finished(),
            "the batch was combined on the runtime thread"
        );

        assert_eq!(0, produce.await.expect("join_handle")?);

        let sent = recorder.produced(&abc0)?.unwrap();
        assert_eq!(1, sent.len());
        assert_eq!(RECORDS, sent[0].records.len());

        Ok(())
    }

    #[test]
    fn combine_empty() -> Result<()> {
        assert_eq!(None, combine_into_one(vec![])?);
        Ok(())
    }

    #[test]
    fn combine_rejects_only_the_undecodable_batch() -> Result<()> {
        let attributes: i16 = BatchAttribute::default().into();

        let mut batches = into_batches(
            attributes,
            54345,
            32123,
            0,
            &[
                vec![Bytes::from_static(b"a")],
                vec![Bytes::from_static(b"b")],
                vec![Bytes::from_static(b"c")],
            ],
        )?;

        let len = batches[1].record_data.len();
        batches[1].record_data = batches[1].record_data.slice(0..len / 2);

        let queued = batches
            .into_iter()
            .map(|batch| BatchRequest {
                id: Uuid::now_v7(),
                batch,
            })
            .collect::<Vec<_>>();
        let ids = queued.iter().map(|owner| owner.id).collect::<Vec<_>>();

        let Combined { runs, rejected } = combine(queued, MAX_DECODED_BATCH_BYTES);

        assert_matches!(
            rejected.as_slice(),
            [(owner, Error::Api(ErrorCode::InvalidRecord))] if *owner == ids[1]
        );

        // The batches on either side of the rejected one merge into one run.
        assert_eq!(
            vec![vec![ids[0], ids[2]]],
            runs?.iter().map(owner_ids).collect::<Vec<_>>()
        );

        Ok(())
    }

    fn into_batches(
        attributes: i16,
        producer_id: i64,
        producer_epoch: i16,
        base_offset: i64,
        batches: &[Vec<Bytes>],
    ) -> Result<Vec<deflated::Batch>> {
        let mut split = vec![];
        let mut base_sequence = 0;

        for batch in batches {
            let mut inflated = inflated::Batch::builder()
                .attributes(attributes)
                .producer_id(producer_id)
                .producer_epoch(producer_epoch)
                .base_offset(base_offset)
                .last_offset_delta(batch.len() as i32 - 1)
                .base_sequence(base_sequence);

            for (offset_delta, value) in batch.iter().enumerate() {
                inflated = inflated.record(
                    Record::builder()
                        .value(value.clone().into())
                        .offset_delta(offset_delta as i32),
                );
            }

            split.push(
                inflated
                    .build()
                    .and_then(TryInto::try_into)
                    .inspect(|deflated| debug!(?deflated))?,
            );

            base_sequence += batch.len() as i32;
        }

        Ok(split)
    }

    /// The second batch's record is rebuilt with `offset_delta` 64, one byte
    /// longer as a varint than the 0 it was encoded with, so the combined
    /// batch must still decode record by record.
    #[test]
    fn combine_offset_delta_crossing_varint_width_decodes_by_value() -> Result<()> {
        let batches = [
            vec![Bytes::from_static(b"a"); 64],
            vec![Bytes::from_static(b"b")],
        ];

        let combined = into_batches(BatchAttribute::default().into(), 54345, 32123, 0, &batches)
            .and_then(combine_into_one)?
            .expect("a batch");

        let records = Vec::<Record>::try_from(combined)?;
        assert_eq!(65, records.len());
        assert_eq!(64, records[64].offset_delta);
        assert_eq!(Some(Bytes::from_static(b"b")), records[64].value);

        Ok(())
    }

    #[test]
    fn combine_batches() -> Result<()> {
        let batches = [
            vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
            ],
            vec![Bytes::from_static(b"f"), Bytes::from_static(b"g")],
            vec![Bytes::from_static(b"i")],
            vec![Bytes::from_static(b"j")],
            vec![Bytes::from_static(b"k")],
            vec![
                Bytes::from_static(b"p"),
                Bytes::from_static(b"q"),
                Bytes::from_static(b"r"),
                Bytes::from_static(b"s"),
            ],
        ];

        let producer_id = 54345;
        let producer_epoch = 32123;
        let base_offset = 0;
        let attributes: i16 = BatchAttribute::default().into();
        let base_sequence: i32 = 0;

        let combined = inflated::Batch::try_from(
            into_batches(
                attributes,
                producer_id,
                producer_epoch,
                base_offset,
                &batches[..],
            )
            .and_then(combine_into_one)?
            .expect("a batch"),
        )?;

        assert_eq!(combined.producer_id, producer_id);
        assert_eq!(combined.producer_epoch, producer_epoch);
        assert_eq!(combined.base_sequence, base_sequence);
        assert_eq!(combined.base_offset, base_offset);
        assert_eq!(combined.attributes, attributes);

        let index = 0;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[0][0].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 1;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[0][1].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 2;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[0][2].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 3;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[1][0].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 4;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[1][1].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 5;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[2][0].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 6;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[3][0].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 7;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[4][0].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 8;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[5][0].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 9;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[5][1].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 10;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[5][2].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        let index = 11;
        assert_eq!(None, combined.records[index].key);
        assert_eq!(Some(batches[5][3].clone()), combined.records[index].value);
        assert_eq!(index, combined.records[index].offset_delta as usize);

        Ok(())
    }

    /// The batches have different base timestamps, so `combine` must rebase the
    /// second batch's timestamp deltas onto the first batch's base.
    #[test]
    fn combine_batches_preserves_timestamps_across_different_base_timestamps() -> Result<()> {
        let first = inflated::Batch::builder()
            .producer_id(1)
            .producer_epoch(0)
            .base_offset(0)
            .last_offset_delta(1)
            .base_sequence(0)
            .base_timestamp(1_000)
            .max_timestamp(1_005)
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"a").into())
                    .offset_delta(0)
                    .timestamp_delta(0),
            )
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"b").into())
                    .offset_delta(1)
                    .timestamp_delta(5),
            )
            .build()
            .and_then(deflated::Batch::try_from)?;

        let second = inflated::Batch::builder()
            .producer_id(1)
            .producer_epoch(0)
            .base_offset(2)
            .last_offset_delta(1)
            .base_sequence(2)
            .base_timestamp(1_010)
            .max_timestamp(1_013)
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"c").into())
                    .offset_delta(0)
                    .timestamp_delta(0),
            )
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"d").into())
                    .offset_delta(1)
                    .timestamp_delta(3),
            )
            .build()
            .and_then(deflated::Batch::try_from)?;

        let original_times: BTreeMap<&'static [u8], i64> = BTreeMap::from([
            (&b"a"[..], 1_000),
            (&b"b"[..], 1_005),
            (&b"c"[..], 1_010),
            (&b"d"[..], 1_013),
        ]);

        let combined =
            inflated::Batch::try_from(combine_into_one(vec![first, second])?.expect("a batch"))?;

        assert_eq!(4, combined.records.len());

        assert_eq!(1_000, combined.base_timestamp);
        assert_eq!(1_013, combined.max_timestamp);

        for record in &combined.records {
            let value = record.value.as_deref().expect("value");
            let expected = original_times[value];
            let actual = combined.base_timestamp + record.timestamp_delta;
            assert_eq!(
                expected, actual,
                "record {value:?} should keep its original absolute timestamp"
            );
        }

        Ok(())
    }

    /// The second batch's base timestamp is earlier than the first's, so the
    /// rebased deltas are negative and the first batch's maximum stays the
    /// combined maximum.
    #[test]
    fn combine_batches_preserves_timestamps_when_second_base_timestamp_is_earlier() -> Result<()> {
        let first = inflated::Batch::builder()
            .producer_id(1)
            .producer_epoch(0)
            .base_offset(0)
            .last_offset_delta(1)
            .base_sequence(0)
            .base_timestamp(1_000)
            .max_timestamp(1_005)
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"a").into())
                    .offset_delta(0)
                    .timestamp_delta(0),
            )
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"b").into())
                    .offset_delta(1)
                    .timestamp_delta(5),
            )
            .build()
            .and_then(deflated::Batch::try_from)?;

        let second = inflated::Batch::builder()
            .producer_id(1)
            .producer_epoch(0)
            .base_offset(2)
            .last_offset_delta(1)
            .base_sequence(2)
            .base_timestamp(990)
            .max_timestamp(993)
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"c").into())
                    .offset_delta(0)
                    .timestamp_delta(0),
            )
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"d").into())
                    .offset_delta(1)
                    .timestamp_delta(3),
            )
            .build()
            .and_then(deflated::Batch::try_from)?;

        let original_times: BTreeMap<&'static [u8], i64> = BTreeMap::from([
            (&b"a"[..], 1_000),
            (&b"b"[..], 1_005),
            (&b"c"[..], 990),
            (&b"d"[..], 993),
        ]);

        let combined =
            inflated::Batch::try_from(combine_into_one(vec![first, second])?.expect("a batch"))?;

        assert_eq!(4, combined.records.len());

        assert_eq!(1_000, combined.base_timestamp);
        assert_eq!(1_005, combined.max_timestamp);

        assert_eq!(
            vec![0, 5, -10, -7],
            combined
                .records
                .iter()
                .map(|record| record.timestamp_delta)
                .collect::<Vec<_>>()
        );

        for record in &combined.records {
            let value = record.value.as_deref().expect("value");
            let expected = original_times[value];
            let actual = combined.base_timestamp + record.timestamp_delta;
            assert_eq!(
                expected, actual,
                "record {value:?} should keep its original absolute timestamp"
            );
        }

        Ok(())
    }

    /// Returns a gzip batch from [`PRODUCER_ID`], of one record for each of `values`.
    fn gzip_batch_of(values: &[Bytes]) -> Result<deflated::Batch> {
        let attributes = BatchAttribute::default()
            .compression(Compression::Gzip)
            .into();

        into_batch(attributes, PRODUCER_ID, 32123, 0, values)
    }

    /// Returns `count` values of `len` bytes of `byte`.
    fn values_of(byte: u8, len: usize, count: usize) -> Vec<Bytes> {
        vec![Bytes::from(vec![byte; len]); count]
    }

    fn queued(batches: &[&deflated::Batch]) -> Vec<BatchRequest> {
        batches
            .iter()
            .map(|&batch| BatchRequest {
                id: Uuid::now_v7(),
                batch: batch.clone(),
            })
            .collect()
    }

    /// The decoded size of `batch` once inflated, with its own codec.
    fn decoded_size_of(batch: &deflated::Batch) -> Result<usize> {
        let compression = BatchAttribute::try_from(batch.attributes)?.compression;

        inflated::Batch::try_from(batch)?
            .decoded_size(&compression)
            .map_err(Into::into)
    }

    /// The decoded size of `batches` merged into one.
    fn merged_decoded_size(batches: &[&deflated::Batch]) -> Result<usize> {
        combine_into_one(batches.iter().map(|&batch| batch.clone()).collect())?
            .as_ref()
            .map_or(Ok(0), decoded_size_of)
    }

    fn owner_ids(run: &Run) -> Vec<Uuid> {
        run.owners.iter().map(|owner| owner.id).collect()
    }

    /// Batches that fit together under the decoded-size limit merge into one run, and the run
    /// closes before the batch that would take it over the limit. Each run decodes, holds its
    /// owners in queue order, and has contiguous offset deltas.
    #[test]
    fn combine_closes_a_run_before_the_batch_that_would_take_it_over_the_limit() -> Result<()> {
        let first = gzip_batch_of(&values_of(b'a', 1_000, 2))?;
        let second = gzip_batch_of(&values_of(b'b', 1_000, 3))?;
        let third = gzip_batch_of(&values_of(b'c', 1_000, 1))?;

        let limit = merged_decoded_size(&[&first, &second])?;
        assert!(limit < merged_decoded_size(&[&first, &second, &third])?);

        // Control: at the production limit the three batches merge into one run.
        assert_eq!(
            1,
            combine(queued(&[&first, &second, &third]), MAX_DECODED_BATCH_BYTES)
                .runs?
                .len()
        );

        let queued = queued(&[&first, &second, &third]);
        let ids = queued.iter().map(|owner| owner.id).collect::<Vec<_>>();

        let runs = combine(queued, limit).runs?;

        assert_eq!(2, runs.len());
        assert_eq!(ids[..2], owner_ids(&runs[0]));
        assert_eq!(ids[2..], owner_ids(&runs[1]));

        for (run, values) in runs.iter().zip([
            [values_of(b'a', 1_000, 2), values_of(b'b', 1_000, 3)].concat(),
            values_of(b'c', 1_000, 1),
        ]) {
            assert!(decoded_size_of(&run.batch)? <= limit);

            let inflated = inflated::Batch::try_from(&run.batch)?;
            assert_eq!(
                values.len() as i32 - 1,
                inflated.last_offset_delta,
                "{run:?}"
            );
            assert_eq!(
                (0..values.len() as i32).collect::<Vec<_>>(),
                inflated
                    .records
                    .iter()
                    .map(|record| record.offset_delta)
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                values.into_iter().map(Some).collect::<Vec<_>>(),
                inflated
                    .records
                    .iter()
                    .map(Record::value)
                    .collect::<Vec<_>>()
            );
        }

        Ok(())
    }

    /// A batch whose own decoded size is over the limit is a run on its own, and the batches on
    /// either side of it, which would fit together, do not merge across it.
    #[test]
    fn combine_makes_a_batch_over_the_limit_a_run_of_its_own() -> Result<()> {
        let before = gzip_batch_of(&values_of(b'a', 100, 1))?;
        let over_the_limit = gzip_batch_of(&values_of(b'b', 10_000, 1))?;
        let after = gzip_batch_of(&values_of(b'c', 100, 1))?;

        let limit = merged_decoded_size(&[&before, &after])?;
        assert!(decoded_size_of(&over_the_limit)? > limit);

        let queued = queued(&[&before, &over_the_limit, &after]);
        let ids = queued.iter().map(|owner| owner.id).collect::<Vec<_>>();

        let runs = combine(queued, limit).runs?;

        assert_eq!(
            vec![vec![ids[0]], vec![ids[1]], vec![ids[2]]],
            runs.iter().map(owner_ids).collect::<Vec<_>>()
        );
        assert_eq!(
            Some(Bytes::from(vec![b'b'; 10_000])),
            inflated::Batch::try_from(&runs[1].batch)?.records[0].value()
        );

        Ok(())
    }

    /// A flush whose batches would merge past the decoded-size limit is written as one batch for
    /// each run, and each producer receives the offset of its own first record.
    #[tokio::test(start_paused = true)]
    async fn flush_over_the_decoded_limit_is_written_as_runs() -> Result<()> {
        const MINIMUM_DELAY: Duration = Duration::from_secs(1);
        const ADVANCE_DELAY: Duration = Duration::from_secs(5);

        let first = gzip_batch_of(&values_of(b'a', 1_000, 2))?;
        let second = gzip_batch_of(&values_of(b'b', 1_000, 3))?;
        let third = gzip_batch_of(&values_of(b'c', 1_000, 1))?;

        let limit = merged_decoded_size(&[&first, &second])?;

        let recorder = FlightRecorder::new();
        let batcher = ProduceRequestBatcher::new(recorder.clone())
            .with_maximum_delay(Some(MINIMUM_DELAY))
            .with_decoded_limit(limit);
        let topition = Topition::new("abc", 0);

        // Each request joins the queue before the next, so their order in it is fixed.
        let first_produce = spawn_produce(&batcher, &topition, first);
        yield_now().await;
        let second_produce = spawn_produce(&batcher, &topition, second);
        yield_now().await;
        let third_produce = spawn_produce(&batcher, &topition, third);

        advance(ADVANCE_DELAY).await;
        yield_now().await;

        for (produce, offset) in [(first_produce, 0), (second_produce, 2), (third_produce, 5)] {
            assert_matches!(
                tokio::time::timeout(Duration::from_secs(60), produce).await,
                Ok(Ok(Ok(base_offset))) if base_offset == offset
            );
        }

        let stored = recorder.produced(&topition)?.unwrap_or_default();

        assert_eq!(
            vec![5, 1],
            stored
                .iter()
                .map(|batch| batch.records.len())
                .collect::<Vec<_>>()
        );

        for batch in stored {
            assert!(batch.decoded_size(&Compression::Gzip)? <= limit);
        }

        Ok(())
    }

    /// When the write of one run fails, only that run's producers receive the error, and the later
    /// runs are still written. The runs are `[first, second]` and `[third]`.
    #[tokio::test(start_paused = true)]
    async fn failed_run_fails_only_its_own_producers() -> Result<()> {
        const MINIMUM_DELAY: Duration = Duration::from_secs(1);
        const ADVANCE_DELAY: Duration = Duration::from_secs(5);

        let first = gzip_batch_of(&values_of(b'a', 1_000, 2))?;
        let second = gzip_batch_of(&values_of(b'b', 1_000, 3))?;
        let third = gzip_batch_of(&values_of(b'c', 1_000, 1))?;

        let limit = merged_decoded_size(&[&first, &second])?;

        let recorder = FlightRecorder::new();
        let batcher = ProduceRequestBatcher::new(recorder.clone())
            .with_maximum_delay(Some(MINIMUM_DELAY))
            .with_decoded_limit(limit);
        let topition = Topition::new("abc", 0);

        // The first write of the flush is the write of the first run.
        recorder.fail_next_produce();

        // Each request joins the queue before the next, so their order in it is fixed.
        let first_produce = spawn_produce(&batcher, &topition, first);
        yield_now().await;
        let second_produce = spawn_produce(&batcher, &topition, second);
        yield_now().await;
        let third_produce = spawn_produce(&batcher, &topition, third);

        advance(ADVANCE_DELAY).await;
        yield_now().await;

        for produce in [first_produce, second_produce] {
            assert_matches!(
                tokio::time::timeout(Duration::from_secs(60), produce).await,
                Ok(Ok(Err(_)))
            );
        }

        assert_matches!(
            tokio::time::timeout(Duration::from_secs(60), third_produce).await,
            Ok(Ok(Ok(0)))
        );

        // Only the run of the third request is stored.
        let stored = recorder.produced(&topition)?.unwrap_or_default();
        assert_eq!(
            vec![1],
            stored
                .iter()
                .map(|batch| batch.records.len())
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    /// When the merged batch of one run is over `max.message.bytes`, only that run is written as
    /// sent, and the later run is still written as its own batch. The runs are `[first, second]`
    /// and `[third]`.
    #[tokio::test(start_paused = true)]
    async fn run_over_max_message_bytes_is_written_as_sent_and_the_next_run_is_written()
    -> Result<()> {
        const MINIMUM_DELAY: Duration = Duration::from_secs(1);
        const ADVANCE_DELAY: Duration = Duration::from_secs(5);

        let gzip_attributes: i16 = BatchAttribute::default()
            .compression(Compression::Gzip)
            .into();
        let mut seed = 0x2545_f491_u32;
        let first_value = pseudo_random_bytes(&mut seed, MAX_MESSAGE_BYTES * 6 / 10);
        let second_value = pseudo_random_bytes(&mut seed, MAX_MESSAGE_BYTES * 6 / 10);
        let third_value = pseudo_random_bytes(&mut seed, MAX_MESSAGE_BYTES * 6 / 10);

        let first = batch_of(PRODUCER_ID, gzip_attributes, &first_value)?;
        let second = batch_of(PRODUCER_ID, gzip_attributes, &second_value)?;
        let third = batch_of(PRODUCER_ID, gzip_attributes, &third_value)?;

        // Each batch fits on its own, and the first two together do not, so only the first run is
        // over the limit.
        for batch in [&first, &second, &third] {
            assert!(batch.size_in_bytes()? <= MAX_MESSAGE_BYTES);
        }
        assert!(first.size_in_bytes()? + second.size_in_bytes()? > MAX_MESSAGE_BYTES);

        let limit = merged_decoded_size(&[&first, &second])?;

        let recorder = FlightRecorder::with_max_message_bytes(MAX_MESSAGE_BYTES);
        let batcher = ProduceRequestBatcher::new(recorder.clone())
            .with_maximum_delay(Some(MINIMUM_DELAY))
            .with_decoded_limit(limit);
        let topition = Topition::new("abc", 0);

        // Each request joins the queue before the next, so their order in it is fixed.
        let first_produce = spawn_produce(&batcher, &topition, first);
        yield_now().await;
        let second_produce = spawn_produce(&batcher, &topition, second);
        yield_now().await;
        let third_produce = spawn_produce(&batcher, &topition, third);

        advance(ADVANCE_DELAY).await;
        yield_now().await;

        for (produce, offset) in [(first_produce, 0), (second_produce, 1), (third_produce, 2)] {
            assert_matches!(
                tokio::time::timeout(Duration::from_secs(60), produce).await,
                Ok(Ok(Ok(base_offset))) if base_offset == offset
            );
        }

        let stored = recorder.produced(&topition)?.unwrap_or_default();
        assert_eq!(
            vec![Some(first_value), Some(second_value), Some(third_value)],
            stored
                .iter()
                .map(|batch| batch.records[0].value())
                .collect::<Vec<_>>()
        );

        Ok(())
    }
}
