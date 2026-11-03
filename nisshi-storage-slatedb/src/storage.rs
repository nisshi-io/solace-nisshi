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

//! Storage trait implementation for SlateDB Engine

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use nisshi_sans_io::{
    BatchAttribute, ConfigResource, ConfigSource, ControlBatch, EndTransactionMarker, ErrorCode,
    IsolationLevel, ListOffset, NULL_TOPIC_ID, OpType, ScramMechanism,
    add_partitions_to_txn_response::{
        AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
    },
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    delete_groups_response::DeletableGroupResult,
    delete_records_request::DeleteRecordsTopic,
    delete_records_response::{DeleteRecordsPartitionResult, DeleteRecordsTopicResult},
    describe_cluster_response::DescribeClusterBroker,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    describe_topic_partitions_response::{
        DescribeTopicPartitionsResponsePartition, DescribeTopicPartitionsResponseTopic,
    },
    fetch_response::AbortedTransaction,
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    metadata_response::{MetadataResponseBroker, MetadataResponseTopic},
    record::{Record, deflated::Batch, inflated::Batch as InflatedBatch},
    ser::RecordBatchEncoder,
    to_system_time,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use nisshi_schema::lake::LakeHouse as _;
use nisshi_storage::{
    BrokerRegistrationRequest, CommittedOffset, DELETE_RECORDS_INVALID_LOW_WATERMARK, Error,
    GroupDetail, ListOffsetResponse, MetadataResponse, NamedGroupDetail, OffsetCommitRequest,
    OffsetStage, ProducerIdResponse, Result, ScramCredential, Storage, TopicId, Topition,
    TxnAddPartitionsRequest, TxnAddPartitionsResponse, TxnOffsetCommitRequest, TxnState,
    UpdateError, Version, check_claim, config::apply_op, delete_records_cutoff, inflate,
    max_message_bytes, producer_claim,
};
use serde::{Serialize, de::DeserializeOwned};
use tokio::time::sleep;
use tracing::{debug, error, warn};
use uuid::Uuid;

use super::engine::Engine;
use super::types::{
    BatchKey, BatchKeyPrefix, BrokerInfo, Brokers, GroupDetailVersion, GroupKey, GroupKeyPrefix,
    OffsetCommitKey, OffsetCommitKeyPrefix, OffsetCommitValue, Producers, StoredScramCredential,
    TopicMetadata, Topics, Transactions, Txn, TxnCommitOffset, TxnDetail, TxnProduceOffset,
    UserScramCredentialKey, Watermark, WatermarkKey,
};

const CLEANUP_POLICY: &str = "cleanup.policy";
const COMPACT: &str = "compact";
const DELETE: &str = "delete";
const RETENTION_MS: &str = "retention.ms";

/// Default retention period applied when a topic has no `retention.ms`
/// configuration, matching the PG engine.
const DEFAULT_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// The number of times an alter of a topic's configuration tries to commit.
const ALTER_ATTEMPTS: u32 = 8;

/// The delay before an alter retries its first conflict. It doubles for each
/// later conflict, so the alter waits about 1.3 seconds in all before it fails.
const ALTER_RETRY_DELAY: Duration = Duration::from_millis(10);

/// Decodes a stored committed offset key or value.
///
/// A committed offset that doesn't decode stays that way, so the error is
/// `UNKNOWN_SERVER_ERROR`, which OffsetFetch reports to the client instead of
/// a code that the client retries.
fn decode_offset<T: DeserializeOwned>(encoded: &[u8]) -> Result<T> {
    postcard::from_bytes(encoded)
        .inspect_err(|err| error!(?err))
        .map_err(|_| Error::Api(ErrorCode::UnknownServerError))
}

impl Engine {
    /// Applies every change in `resource` to its topic in one transaction, so
    /// that a failed change leaves the topic's configuration unchanged.
    async fn alter_topic(&self, resource: &AlterConfigsResource) -> Result<()> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut topics: Topics = self.load_metadata(&tx, Self::TOPICS).await?;

        let metadata = topics
            .get_mut(&resource.resource_name[..])
            .ok_or(Error::Api(ErrorCode::UnknownTopicOrPartition))?;

        let mut configuration: BTreeMap<String, Option<String>> = metadata
            .topic
            .configs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|item| (item.name.clone(), item.value.clone()))
            .collect();

        for change in resource.configs.as_deref().unwrap_or_default() {
            let op = OpType::try_from(change.config_operation)?;
            let current = configuration.get(&change.name).and_then(Option::as_deref);

            match apply_op(
                ConfigResource::Topic,
                &change.name,
                current,
                op,
                change.value.as_deref(),
            )? {
                Some(value) => _ = configuration.insert(change.name.clone(), Some(value)),
                None => _ = configuration.remove(&change.name),
            }
        }

        _ = metadata.topic.configs.replace(
            configuration
                .into_iter()
                .map(|(key, value)| CreatableTopicConfig::default().name(key).value(value))
                .collect(),
        );

        self.save_metadata(&tx, Self::TOPICS, &topics)?;
        tx.commit().await.map_err(Error::from).and(Ok(()))
    }

    fn topic_config<'a>(metadata: &'a TopicMetadata, name: &str) -> Option<&'a str> {
        metadata
            .topic
            .configs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .find(|config| config.name == name)
            .and_then(|config| config.value.as_deref())
    }

    fn has_cleanup_policy(metadata: &TopicMetadata, policy: &str) -> bool {
        Self::topic_config(metadata, CLEANUP_POLICY)
            .is_some_and(|value| value.split(',').any(|candidate| candidate.trim() == policy))
    }

    async fn partition_watermark(
        &self,
        tx: &slatedb::DbTransaction,
        watermark_key: &[u8],
    ) -> Result<Watermark> {
        tx.get(watermark_key)
            .await
            .map_err(Error::from)
            .and_then(|watermark| {
                watermark.map_or(Ok(Watermark::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })
    }

    /// The least batch base offset at or after `probe` within a partition,
    /// or `None` when no batch starts at or after it.
    async fn batch_base_at_or_after(
        &self,
        prefix: &[u8],
        topic: Uuid,
        partition: i32,
        probe: i64,
    ) -> Result<Option<i64>> {
        let from = postcard::to_stdvec(&BatchKey::scan_from(topic, partition, probe))?;

        let mut i = self.db.scan(from..).await?;

        Ok(match i.next().await? {
            Some(kv) if kv.key.starts_with(prefix) => {
                Some(postcard::from_bytes::<BatchKey>(&kv.key)?.offset)
            }
            _ => None,
        })
    }

    /// The greatest batch base offset at or before `offset` within a
    /// partition. SlateDB has forward-only iteration, so this is a binary
    /// search over [`Self::batch_base_at_or_after`] successor probes.
    async fn batch_base_at_or_before(
        &self,
        prefix: &[u8],
        topic: Uuid,
        partition: i32,
        offset: i64,
    ) -> Result<Option<i64>> {
        let mut floor = None;
        let (mut lo, mut hi) = (0, offset);

        while lo <= hi {
            let mid = lo + (hi - lo) / 2;

            match self
                .batch_base_at_or_after(prefix, topic, partition, mid)
                .await?
            {
                Some(base) if base <= offset => {
                    floor = Some(base);
                    lo = base + 1;
                }
                _ => hi = mid - 1,
            }
        }

        Ok(floor)
    }

    /// Apply the `delete` cleanup policy: remove the prefix of each partition
    /// where every batch is older than the topic's `retention.ms`.
    async fn policy_delete(&self, now: SystemTime) -> Result<u64> {
        let Some(now_ms) = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .and_then(|since_epoch| i64::try_from(since_epoch.as_millis()).ok())
        else {
            return Ok(0);
        };

        let topics = self.get_topics().await?;
        let mut deleted = 0;

        for metadata in topics
            .values()
            .filter(|metadata| Self::has_cleanup_policy(metadata, DELETE))
        {
            let retention_ms = Self::topic_config(metadata, RETENTION_MS)
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(DEFAULT_RETENTION.as_millis() as i64);

            for partition in 0..metadata.topic.num_partitions {
                let tx = self
                    .db
                    .begin(slatedb::IsolationLevel::SerializableSnapshot)
                    .await
                    .inspect_err(|err| debug!(?err))?;

                let prefix = postcard::to_stdvec(&BatchKeyPrefix::new(metadata.id, partition))?;
                let scan_start =
                    postcard::to_stdvec(&BatchKey::scan_from(metadata.id, partition, 0))?;

                let mut scan = self.db.scan(scan_start..).await?;
                let mut removed = 0;
                let mut first_remaining = None;

                while let Some(kv) = scan.next().await? {
                    if !kv.key.starts_with(&prefix) {
                        break;
                    }

                    let key: BatchKey = postcard::from_bytes(&kv.key)?;
                    let batch = self.decode(kv.value)?;

                    // Only a prefix of the log is removed: deletion stops at
                    // the first batch within the retention period.
                    if now_ms.saturating_sub(batch.max_timestamp) <= retention_ms {
                        first_remaining = Some(key.offset);
                        break;
                    }

                    tx.delete(&kv.key)?;
                    removed += 1;
                }

                if removed == 0 {
                    continue;
                }

                let watermark_key =
                    postcard::to_stdvec(&WatermarkKey::new(metadata.id, partition))?;
                let mut watermark = self.partition_watermark(&tx, &watermark_key).await?;

                // When the partition is now empty the log start meets the
                // high watermark: offsets are never reused.
                let low = first_remaining.unwrap_or_else(|| watermark.high.unwrap_or(0));
                watermark.low = Some(low);

                if let Some(ref mut timestamps) = watermark.timestamps {
                    timestamps.retain(|_, offset| *offset >= low);
                }

                tx.put(&watermark_key, postcard::to_stdvec(&watermark)?)?;
                _ = tx.commit().await.map_err(Error::from)?;

                deleted += removed;
            }
        }

        Ok(deleted)
    }

    /// Apply the `compact` cleanup policy: for each record key, retain only
    /// the most recent record in the partition. Batches that become empty
    /// are removed; control batches are left untouched.
    async fn policy_compact(&self) -> Result<u64> {
        let topics = self.get_topics().await?;
        let mut compacted = 0;

        for metadata in topics
            .values()
            .filter(|metadata| Self::has_cleanup_policy(metadata, COMPACT))
        {
            for partition in 0..metadata.topic.num_partitions {
                let prefix = postcard::to_stdvec(&BatchKeyPrefix::new(metadata.id, partition))?;
                let scan_start =
                    postcard::to_stdvec(&BatchKey::scan_from(metadata.id, partition, 0))?;

                let mut batches = vec![];
                let mut scan = self.db.scan(scan_start..).await?;
                while let Some(kv) = scan.next().await? {
                    if !kv.key.starts_with(&prefix) {
                        break;
                    }
                    let key: BatchKey = postcard::from_bytes(&kv.key)?;
                    let batch = self.decode(kv.value)?;
                    batches.push((kv.key, key.offset, batch));
                }

                let tx = self
                    .db
                    .begin(slatedb::IsolationLevel::SerializableSnapshot)
                    .await
                    .inspect_err(|err| debug!(?err))?;

                // Working from the newest batch to the oldest, a record is
                // dropped when its key reappears in a newer batch.
                let mut head = BTreeSet::new();
                let mut removed_offsets = BTreeSet::new();
                let mut removed_records = 0;

                for (raw_key, offset, deflated) in batches.iter().rev() {
                    // A stored batch that cannot be inflated (malformed, or
                    // over the decoded-size limit) is left as it is rather
                    // than failing the whole pass. Its keys are unknown, so
                    // older records it may supersede are kept: the
                    // conservative outcome, and every other batch is still
                    // compacted.
                    let inflated = match InflatedBatch::try_from(deflated.clone()) {
                        Ok(inflated) => inflated,
                        Err(error) => {
                            warn!(
                                topic = %metadata.topic.name,
                                partition,
                                offset,
                                ?error,
                                "compaction is skipping a batch it cannot inflate"
                            );
                            continue;
                        }
                    };

                    if BatchAttribute::try_from(inflated.attributes)?.control {
                        continue;
                    }

                    let keys = inflated.keys();
                    let compaction = inflated.compact(&head)?;
                    head.extend(keys);

                    if compaction.records == 0 {
                        continue;
                    }

                    removed_records += compaction.records as u64;

                    if compaction.batch.records.is_empty() {
                        tx.delete(raw_key)?;
                        _ = removed_offsets.insert(*offset);
                    } else {
                        let rewritten: Batch = compaction.batch.try_into()?;

                        let encoded = {
                            let mut encoder = RecordBatchEncoder::new(BytesMut::new());
                            rewritten.serialize(&mut encoder)?;
                            Bytes::from(encoder)
                        };

                        tx.put(raw_key, &encoded[..])?;
                    }
                }

                if removed_records == 0 {
                    continue;
                }

                let watermark_key =
                    postcard::to_stdvec(&WatermarkKey::new(metadata.id, partition))?;
                let mut watermark = self.partition_watermark(&tx, &watermark_key).await?;

                // Compaction leaves the log start where it is, as in Kafka,
                // whose cleaner never moves it: a fetch below the first
                // surviving batch steps over the gap to it, where moving the
                // log start would answer OFFSET_OUT_OF_RANGE and reset the
                // consumer. Only retention and DeleteRecords move it.
                if let Some(ref mut timestamps) = watermark.timestamps {
                    timestamps.retain(|_, offset| !removed_offsets.contains(offset));
                }

                tx.put(&watermark_key, postcard::to_stdvec(&watermark)?)?;
                _ = tx.commit().await.map_err(Error::from)?;

                compacted += removed_records;
            }
        }

        Ok(compacted)
    }
}

#[async_trait]
impl Storage for Engine {
    /// Register a broker in the cluster.
    ///
    /// NOTE: This should be a null operation for SlateDB. All brokers are the same
    /// (single-node cluster), so there is no need to store broker information.
    /// The Postgres engine needs to store broker records to be able to join on them,
    /// but for SlateDB this is unnecessary overhead.
    ///
    /// Currently persists broker information to SlateDB under the `BROKERS` key,
    /// but this could be removed in favor of a no-op implementation.
    async fn register_broker(&self, broker_registration: BrokerRegistrationRequest) -> Result<()> {
        debug!(?broker_registration);

        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut brokers: Brokers = self.load_metadata(&tx, Self::BROKERS).await?;

        // Persist broker info to storage
        // NOTE: This is stored permanently - no cleanup mechanism exists yet
        let broker_info = BrokerInfo {
            broker_id: self.node,
            host: self
                .advertised_listener
                .host_str()
                .unwrap_or("0.0.0.0")
                .into(),
            port: self.advertised_listener.port().unwrap_or(9092).into(),
            rack: broker_registration.rack,
        };

        _ = brokers.insert(self.node, broker_info);
        self.save_metadata(&tx, Self::BROKERS, &brokers)?;

        _ = tx.commit().await.map_err(Error::from)?;

        Ok(())
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let stored_brokers = self
            .db
            .get(Self::BROKERS)
            .await
            .map_err(Error::from)
            .and_then(|brokers| {
                brokers.map_or(Ok(Brokers::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })?;

        if stored_brokers.is_empty() {
            // Return self as the only broker if no registrations yet
            let broker_id = self.node;
            let host = self
                .advertised_listener
                .host_str()
                .unwrap_or("0.0.0.0")
                .into();
            let port = self.advertised_listener.port().unwrap_or(9092).into();

            Ok(vec![
                DescribeClusterBroker::default()
                    .broker_id(broker_id)
                    .host(host)
                    .port(port)
                    .rack(None),
            ])
        } else {
            Ok(stored_brokers
                .values()
                .map(|info| {
                    DescribeClusterBroker::default()
                        .broker_id(info.broker_id)
                        .host(info.host.clone())
                        .port(info.port)
                        .rack(info.rack.clone())
                })
                .collect())
        }
    }

    async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
        // TODO: Implement validate_only mode properly.
        // Currently, it logs a warning but proceeds with creation, which violates the protocol contract.
        // It should validate the config and return without side effects.
        if validate_only {
            tracing::warn!("validate_only mode is not implemented, proceeding with creation");
        }
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        // NOTE: Contention Hotspot
        // Reading the entire TOPICS map creates a serialization bottleneck and high conflict rate
        // for concurrent topic creation.
        let mut topics: Topics = self.load_metadata(&tx, Self::TOPICS).await?;

        let name = topic.name.clone();

        if topics.contains_key(&name[..]) {
            return Err(Error::Api(ErrorCode::TopicAlreadyExists));
        }

        let id = Uuid::now_v7();
        let td = TopicMetadata { id, topic };

        _ = topics.insert(name, td);
        self.save_metadata(&tx, Self::TOPICS, &topics)?;

        tx.commit().await.map_err(Error::from).and(Ok(id))
    }

    /// Delete records up to a specified offset.
    ///
    /// Physically deletes batch data below the specified offset and updates
    /// the low watermark. This aligns with PG's delete_records implementation.
    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let all_topics: Topics = self.load_metadata(&tx, Self::TOPICS).await?;

        let mut results = Vec::with_capacity(topics.len());

        for topic in topics {
            let mut partition_results = vec![];

            let topic_metadata = all_topics.get(&topic.name[..]);

            if let Some(partitions) = topic.partitions.as_ref() {
                for partition in partitions {
                    let (error_code, low_watermark) = if let Some(metadata) = topic_metadata {
                        if partition.partition_index < 0
                            || partition.partition_index >= metadata.topic.num_partitions
                        {
                            (
                                ErrorCode::UnknownTopicOrPartition,
                                DELETE_RECORDS_INVALID_LOW_WATERMARK,
                            )
                        } else {
                            let watermark_key = postcard::to_stdvec(&WatermarkKey::new(
                                metadata.id,
                                partition.partition_index,
                            ))?;

                            let mut watermark =
                                tx.get(&watermark_key).await.map_err(Error::from).and_then(
                                    |watermark| {
                                        watermark.map_or(Ok(Watermark::default()), |encoded| {
                                            postcard::from_bytes(&encoded[..]).map_err(Into::into)
                                        })
                                    },
                                )?;

                            let high_watermark = watermark.high.unwrap_or(0);
                            let log_start = watermark.low.unwrap_or(0);

                            let stage = OffsetStage {
                                last_stable: high_watermark,
                                high_watermark,
                                log_start,
                            };

                            match delete_records_cutoff(partition.offset, &stage) {
                                Err(error_code) => {
                                    (error_code, DELETE_RECORDS_INVALID_LOW_WATERMARK)
                                }

                                Ok(cutoff) => {
                                    if cutoff > log_start {
                                        // A batch key holds the base offset of the batch,
                                        // and not its end offset. A batch ends before the
                                        // base offset of the next batch, so this deletes a
                                        // batch when the next batch's base offset is at or
                                        // below the cutoff, without decoding either batch.
                                        // A batch that straddles the cutoff stays whole.
                                        // The last batch has no successor, so this never
                                        // deletes it. That batch holds `high_watermark - 1`.
                                        let batch_prefix =
                                            postcard::to_stdvec(&BatchKeyPrefix::new(
                                                metadata.id,
                                                partition.partition_index,
                                            ))?;
                                        let scan_start =
                                            postcard::to_stdvec(&BatchKey::scan_from(
                                                metadata.id,
                                                partition.partition_index,
                                                0,
                                            ))?;

                                        let mut previous: Option<Bytes> = None;

                                        let mut scan = self.db.scan(scan_start..).await?;
                                        while let Some(kv) = scan.next().await? {
                                            if !kv.key.starts_with(&batch_prefix) {
                                                break;
                                            }

                                            let Ok(batch_key) =
                                                postcard::from_bytes::<BatchKey>(&kv.key)
                                            else {
                                                continue;
                                            };

                                            if batch_key.offset > cutoff {
                                                break;
                                            }

                                            if let Some(key) = previous.replace(kv.key) {
                                                tx.delete(&key)?;
                                            }

                                            if batch_key.offset == cutoff {
                                                break;
                                            }
                                        }
                                    }

                                    watermark.low = Some(cutoff);

                                    // Remove timestamps before the new low watermark
                                    if let Some(ref mut timestamps) = watermark.timestamps {
                                        timestamps.retain(|_, offset| *offset >= cutoff);
                                    }

                                    let watermark_value = postcard::to_stdvec(&watermark)?;
                                    tx.put(&watermark_key, watermark_value)?;

                                    (ErrorCode::None, cutoff)
                                }
                            }
                        }
                    } else {
                        (
                            ErrorCode::UnknownTopicOrPartition,
                            DELETE_RECORDS_INVALID_LOW_WATERMARK,
                        )
                    };

                    partition_results.push(
                        DeleteRecordsPartitionResult::default()
                            .partition_index(partition.partition_index)
                            .low_watermark(low_watermark)
                            .error_code(error_code.into()),
                    );
                }
            }

            results.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_results)),
            );
        }

        _ = tx.commit().await.map_err(Error::from)?;

        Ok(results)
    }

    /// Delete a topic from the cluster.
    ///
    /// Deletes all associated data: batches, watermarks, consumer offsets,
    /// producer sequences, and transaction data. This aligns with PG's
    /// delete_topic implementation.
    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut topics: Topics = self.load_metadata(&tx, Self::TOPICS).await?;

        let (topic_name, topic_metadata) = match topic {
            TopicId::Name(name) => {
                if let Some(metadata) = topics.get(&name[..]) {
                    (name.clone(), metadata.clone())
                } else {
                    return Ok(ErrorCode::UnknownTopicOrPartition);
                }
            }
            TopicId::Id(id) => {
                if let Some((name, metadata)) = topics.iter().find(|(_, tm)| tm.id == *id) {
                    (name.clone(), metadata.clone())
                } else {
                    return Ok(ErrorCode::UnknownTopicOrPartition);
                }
            }
        };

        // 1. Delete all batches for this topic
        for partition in 0..topic_metadata.topic.num_partitions {
            let batch_prefix =
                postcard::to_stdvec(&BatchKeyPrefix::new(topic_metadata.id, partition))?;
            let scan_start =
                postcard::to_stdvec(&BatchKey::scan_from(topic_metadata.id, partition, 0))?;

            let mut scan = self.db.scan(scan_start..).await?;
            while let Some(kv) = scan.next().await? {
                if !kv.key.starts_with(&batch_prefix) {
                    break;
                }
                tx.delete(&kv.key)?;
            }
        }

        // 2. Delete all watermarks for this topic
        for partition in 0..topic_metadata.topic.num_partitions {
            let watermark_key =
                postcard::to_stdvec(&WatermarkKey::new(topic_metadata.id, partition))?;
            tx.delete(&watermark_key)?;
        }

        // 3. Delete consumer offsets for this topic (scan all groups)
        // Use just the prefix character 'c' to scan all consumer offsets
        let scan_start = vec![b'c'];
        let mut scan = self.db.scan(scan_start..).await?;
        while let Some(kv) = scan.next().await? {
            // Stop if we've moved past the 'c' prefix
            if kv.key.first() != Some(&b'c') {
                break;
            }
            // Try to decode and check if it's for this topic
            if let Ok(key) = postcard::from_bytes::<OffsetCommitKey>(&kv.key)
                && key.topic == topic_name
            {
                tx.delete(&kv.key)?;
            }
        }

        // 4. Clean up producer sequences for this topic
        let mut producers: Producers = self.load_metadata(&tx, Self::PRODUCERS).await?;
        for producer_detail in producers.values_mut() {
            for epoch_sequences in producer_detail.sequences.values_mut() {
                _ = epoch_sequences.remove(&topic_name);
            }
        }
        self.save_metadata(&tx, Self::PRODUCERS, &producers)?;

        // 5. Clean up transaction data for this topic
        let mut transactions: Transactions = self.load_metadata(&tx, Self::TRANSACTIONS).await?;
        for txn in transactions.values_mut() {
            for txn_detail in txn.epochs.values_mut() {
                _ = txn_detail.produces.remove(&topic_name);
                for group_offsets in txn_detail.offsets.values_mut() {
                    _ = group_offsets.remove(&topic_name);
                }
            }
        }
        self.save_metadata(&tx, Self::TRANSACTIONS, &transactions)?;

        // 6. Remove topic from metadata
        _ = topics.remove(&topic_name);
        self.save_metadata(&tx, Self::TOPICS, &topics)?;

        _ = tx.commit().await.map_err(Error::from)?;

        Ok(ErrorCode::None)
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        match ConfigResource::from(resource.resource_type) {
            ConfigResource::Topic => {
                // Every alter, create and delete of a topic rewrites the one
                // TOPICS key, so concurrent writers conflict at commit. The
                // alter retries a conflict with a growing delay, and gives up
                // after ALTER_ATTEMPTS so that constant topic churn can't hold
                // one request forever.
                let mut delay = ALTER_RETRY_DELAY;
                let mut attempt = 1;

                loop {
                    match self.alter_topic(&resource).await {
                        Err(Error::Slate(error))
                            if error.kind() == slatedb::ErrorKind::Transaction
                                && attempt < ALTER_ATTEMPTS =>
                        {
                            debug!(?error, attempt, resource = resource.resource_name);
                            sleep(delay).await;
                            delay *= 2;
                            attempt += 1;
                        }

                        otherwise => break otherwise?,
                    }
                }

                Ok(AlterConfigsResourceResponse::default()
                    .error_code(ErrorCode::None.into())
                    .error_message(Some("".into()))
                    .resource_type(resource.resource_type)
                    .resource_name(resource.resource_name))
            }
            // For other resource types, just return success
            _ => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
        }
    }

    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: Batch,
    ) -> Result<i64> {
        // The batch is inflated before the transaction starts, so the wait
        // for a decode permit and the decode do not lengthen the optimistic
        // transaction.
        let inflated = if self.schemas.is_some() || self.lake.is_some() {
            Some(inflate(deflated.clone()).await?)
        } else {
            None
        };

        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let topics: Topics = self.load_metadata(&tx, Self::TOPICS).await?;

        let Some(metadata) = topics.get(&topition.topic[..]) else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        if topition.partition < 0 || topition.partition >= metadata.topic.num_partitions {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        let limit = max_message_bytes::limit(
            topition.topic(),
            metadata
                .topic
                .configs
                .as_deref()
                .unwrap_or_default()
                .iter()
                .find(|config| config.name == max_message_bytes::KEY)
                .and_then(|config| config.value.as_deref()),
        );
        max_message_bytes::ensure_batch_fits(&deflated, limit)?;

        // Schema validation (if schemas registry is configured)
        if let Some(ref schemas) = self.schemas
            && let Some(ref inflated) = inflated
        {
            let attributes = BatchAttribute::try_from(inflated.attributes)?;

            // Only validate non-control batches
            if !attributes.control {
                schemas.validate(topition.topic(), inflated).await?;
            }
        }

        // Idempotent message check
        if deflated.is_idempotent() {
            // NOTE: Contention Hotspot
            // Loading all producers to check/update sequence numbers prevents high-throughput idempotent production.
            // This map needs to be sharded or converted to per-producer keys.
            let mut producers: Producers = self.load_metadata(&tx, Self::PRODUCERS).await?;

            let Some(producer_detail) = producers.get_mut(&deflated.producer_id) else {
                return Err(Error::Api(ErrorCode::UnknownProducerId));
            };

            // Get current epoch for this producer
            let Some(current_epoch) = producer_detail.sequences.last_key_value().map(|(e, _)| *e)
            else {
                return Err(Error::Api(ErrorCode::UnknownProducerId));
            };

            // Get current sequence for this topic/partition
            let current_sequence = producer_detail
                .sequences
                .get(&deflated.producer_epoch)
                .and_then(|topics| topics.get(&topition.topic))
                .and_then(|partitions| partitions.get(&topition.partition))
                .copied()
                .unwrap_or(0);

            debug!(
                producer_id = deflated.producer_id,
                producer_epoch = deflated.producer_epoch,
                current_epoch,
                current_sequence,
                base_sequence = deflated.base_sequence,
            );

            // Check sequence validity
            let increment =
                Self::idempotent_sequence_check(&current_epoch, &current_sequence, &deflated)?;

            // Update sequence
            _ = producer_detail
                .sequences
                .entry(deflated.producer_epoch)
                .or_default()
                .entry(topition.topic.clone())
                .or_default()
                .insert(topition.partition, current_sequence + increment);

            // Save updated producers
            self.save_metadata(&tx, Self::PRODUCERS, &producers)?;
        }

        let mut watermark = tx
            .get(postcard::to_stdvec(&WatermarkKey::new(
                metadata.id,
                topition.partition,
            ))?)
            .await
            .map_err(Error::from)
            .and_then(|watermark| {
                watermark.map_or(Ok(Watermark::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })?;

        let offset = watermark.high.unwrap_or_default();
        let offset_end = offset + deflated.last_offset_delta as i64;

        // Handle transactional produce - update transaction state with offset range
        if let Some(transaction_id) = transaction_id {
            let mut transactions: Transactions =
                self.load_metadata(&tx, Self::TRANSACTIONS).await?;

            if let Some(txn) = transactions.get_mut(transaction_id)
                && let Some(txn_detail) = txn.epochs.get_mut(&deflated.producer_epoch)
            {
                // Get or create the partition entry in produces map
                let partition_entry = txn_detail
                    .produces
                    .entry(topition.topic.clone())
                    .or_default()
                    .entry(topition.partition)
                    .or_insert(None);

                // Update offset range - keep original offset_start if already set
                if let Some(existing) = partition_entry {
                    // Just update offset_end
                    existing.offset_end = offset_end;
                } else {
                    // First produce to this partition - set both start and end
                    *partition_entry = Some(TxnProduceOffset {
                        offset_start: offset,
                        offset_end,
                    });
                }

                self.save_metadata(&tx, Self::TRANSACTIONS, &transactions)?;
            }
        }

        watermark.high = watermark
            .high
            .map_or(Some(deflated.last_offset_delta as i64 + 1i64), |high| {
                Some(high + deflated.last_offset_delta as i64 + 1i64)
            });

        _ = watermark
            .timestamps
            .get_or_insert_default()
            .insert(deflated.base_timestamp, offset);

        debug!(?watermark);

        let encoded = {
            let mut encoder = RecordBatchEncoder::new(BytesMut::new());
            deflated.serialize(&mut encoder)?;

            Bytes::from(encoder)
        };

        let batch_key =
            postcard::to_stdvec(&BatchKey::new(metadata.id, topition.partition, offset))?;

        tx.put(batch_key, &encoded[..])?;

        // Also save the updated watermark
        let watermark_key =
            postcard::to_stdvec(&WatermarkKey::new(metadata.id, topition.partition))?;
        let watermark_value = postcard::to_stdvec(&watermark)?;
        tx.put(watermark_key, watermark_value)?;

        // Store to data lake if configured
        if let Some(ref lake) = self.lake
            && let Some(ref inflated) = inflated
        {
            let attributes = BatchAttribute::try_from(inflated.attributes)?;

            if !attributes.control {
                // TODO: Optimization - Avoid synchronous call
                // Loading config and writing to lake synchronously inside the transaction critical path
                // increases latency and lock holding time. Consider moving this to an async background task.
                let config = self
                    .describe_config(topition.topic(), ConfigResource::Topic, None)
                    .await?;

                lake.store(
                    topition.topic(),
                    topition.partition(),
                    offset,
                    inflated,
                    config,
                )
                .await?;
            }
        }

        tx.commit().await.map_err(Error::from).and(Ok(offset))
    }

    async fn fetch(
        &self,
        topition: &Topition,
        offset: i64,
        min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
        _max_wait: Duration,
    ) -> Result<Vec<Batch>> {
        // Get the high watermark based on isolation level
        let offset_stage = self.offset_stage(topition).await?;
        let high_watermark = if isolation_level == IsolationLevel::ReadCommitted {
            offset_stage.last_stable
        } else {
            offset_stage.high_watermark
        };

        debug!(
            ?isolation_level,
            high_watermark, offset, min_bytes, max_bytes
        );

        let topics = self
            .db
            .get(Self::TOPICS)
            .await
            .map_err(Error::from)
            .and_then(|topics| {
                topics.map_or(Ok(Topics::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })?;

        let Some(metadata) = topics.get(&topition.topic[..]) else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        if topition.partition < 0 || topition.partition >= metadata.topic.num_partitions {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        if offset >= high_watermark {
            return Ok(vec![]);
        }

        let prefix = postcard::to_stdvec(&BatchKeyPrefix::new(metadata.id, topition.partition))?;

        // Batch keys hold base offsets, but the fetch offset can fall inside
        // a batch that starts before it; Kafka returns that batch whole,
        // leaving the client to skip the records below the fetch offset.
        // When no batch starts exactly at the fetch offset, scan from the
        // greatest base offset before it; the loop below drops a batch that
        // ends before the fetch offset.
        let start = match self
            .batch_base_at_or_after(&prefix, metadata.id, topition.partition, offset)
            .await?
        {
            Some(base) if base == offset => offset,
            _ => self
                .batch_base_at_or_before(&prefix, metadata.id, topition.partition, offset)
                .await?
                .unwrap_or(offset),
        };

        let mut i = {
            let from =
                postcard::to_stdvec(&BatchKey::scan_from(metadata.id, topition.partition, start))?;

            self.db.scan(from..).await?
        };

        let mut batches = vec![];
        let mut total_bytes: usize = 0;
        let min_bytes = min_bytes as usize;
        let max_bytes = max_bytes as usize;

        while let Some(kv) = i.next().await? {
            // Check if the key still belongs to the same topic/partition
            if !kv.key.starts_with(&prefix) {
                break;
            }

            let size = kv.value.len();

            let key: BatchKey = postcard::from_bytes(&kv.key)?;

            // Stop if we've reached the high watermark (respecting isolation level)
            if key.offset >= high_watermark {
                break;
            }

            // TODO: Performance - Avoid full decode
            // We decode the entire batch just to check size limits or return it.
            // For scanning/filtering, we should only decode the header or use a lightweight check.
            let mut batch = self.decode(kv.value)?;
            batch.base_offset = key.offset;

            // only the batch at the scan start can precede the fetch offset
            if key.offset + i64::from(batch.last_offset_delta) < offset {
                continue;
            }

            batches.push(batch);
            total_bytes += size;

            // Stop if we've exceeded max_bytes (unless we haven't reached min_bytes yet)
            if total_bytes >= max_bytes
                || (total_bytes >= min_bytes && size > (max_bytes - total_bytes))
            {
                break;
            }
        }

        Ok(batches)
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        let topics = self.get_topics().await?;

        let Some(metadata) = topics.get(&topition.topic[..]) else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        if topition.partition < 0 || topition.partition >= metadata.topic.num_partitions {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        let watermark_key =
            postcard::to_stdvec(&WatermarkKey::new(metadata.id, topition.partition))?;

        let watermark = self
            .db
            .get(&watermark_key)
            .await
            .map_err(Error::from)
            .and_then(|watermark| {
                watermark.map_or(Ok(Watermark::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })?;

        let high_watermark = watermark.high.unwrap_or(0);

        // Calculate last_stable by finding the minimum offset of any in-progress transaction
        let transactions = self.get_transactions().await?;
        let mut last_stable = high_watermark;

        for txn in transactions.values() {
            for txn_detail in txn.epochs.values() {
                // Consider transactions that are in-progress (Begin, PrepareCommit, or PrepareAbort)
                // These states indicate the transaction is not yet fully committed/aborted
                let is_in_progress = matches!(
                    txn_detail.state,
                    Some(TxnState::Begin)
                        | Some(TxnState::PrepareCommit)
                        | Some(TxnState::PrepareAbort)
                );
                if is_in_progress {
                    // Check if this transaction has produced to this topic/partition
                    if let Some(partitions) = txn_detail.produces.get(&topition.topic)
                        && let Some(Some(offset_range)) = partitions.get(&topition.partition)
                    {
                        // The last_stable should be the minimum of all in-flight txn start offsets
                        last_stable = last_stable.min(offset_range.offset_start);
                    }
                }
            }
        }

        Ok(OffsetStage {
            log_start: watermark.low.unwrap_or(0),
            last_stable,
            high_watermark,
        })
    }

    async fn offset_commit(
        &self,
        group: &str,
        retention: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        // TODO: Implement offset retention
        // Offsets should expire after the configured retention period.
        // Currently, they are stored indefinitely, leading to storage leaks.
        if retention.is_some() {
            tracing::warn!(
                "offset retention is not implemented, offsets will be kept indefinitely"
            );
        }
        // NOTE: Reading global TOPICS map for validation is inefficient.
        let topics = self.get_topics().await?;
        let mut responses = Vec::with_capacity(offsets.len());

        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut group_inserted = false;

        for (topition, offset_commit) in offsets {
            // Verify topic exists
            if !topics.contains_key(&topition.topic[..]) {
                responses.push((topition.clone(), ErrorCode::UnknownTopicOrPartition));
                continue;
            }

            // Insert group entry if not already done in this transaction
            if !group_inserted {
                let group_key = postcard::to_stdvec(&GroupKey::new(group))?;
                // Check if group already exists
                if tx.get(&group_key).await?.is_none() {
                    // Create a minimal group entry for offset tracking
                    let group_value = postcard::to_stdvec(&GroupDetailVersion::default())?;
                    tx.put(group_key, group_value)?;
                }
                group_inserted = true;
            }

            let key = postcard::to_stdvec(&OffsetCommitKey::new(
                group,
                &topition.topic,
                topition.partition,
            ))?;

            let value = postcard::to_stdvec(&OffsetCommitValue {
                offset: offset_commit.offset,
                leader_epoch: offset_commit.leader_epoch,
                metadata: offset_commit.metadata.clone(),
            })?;

            tx.put(key, value)?;
            responses.push((topition.clone(), ErrorCode::None));
        }

        _ = tx.commit().await.map_err(Error::from)?;

        Ok(responses)
    }

    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        let prefix = postcard::to_stdvec(&OffsetCommitKeyPrefix::new(group_id))?;

        let mut topitions = BTreeMap::new();
        let mut scan = self.db.scan(prefix.clone()..).await?;

        while let Some(kv) = scan.next().await? {
            // Check if key still has our prefix
            if !kv.key.starts_with(&prefix) {
                break;
            }

            let key: OffsetCommitKey = decode_offset(&kv.key)?;
            let value: OffsetCommitValue = decode_offset(&kv.value)?;

            _ = topitions.insert(
                Topition::new(key.topic, key.partition),
                CommittedOffset {
                    offset: value.offset,
                    leader_epoch: None,
                    metadata: None,
                },
            );
        }

        Ok(topitions)
    }

    /// SlateDB is deprecated and takes only security fixes, so it keeps its earlier answer
    /// to `require_stable`: it reports no pending transactional offset.
    async fn pending_txn_offsets(&self, _group_id: &str) -> Result<BTreeSet<Topition>> {
        Ok(BTreeSet::new())
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        // TODO: Implement require_stable
        // When true, we must return the Last Stable Offset instead of High Watermark.
        // Current implementation violates READ_COMMITTED isolation by returning potentially unstable offsets.
        if require_stable == Some(true) {
            tracing::warn!(
                "require_stable is not implemented, returning potentially unstable offsets"
            );
        }
        let mut responses = BTreeMap::new();

        if let Some(group_id) = group_id {
            // Get current topics to check existence
            let existing_topics = self.get_topics().await?;

            for topition in topics {
                // If the topic doesn't exist, return -1 (mimics PG behavior with JOINs)
                if !existing_topics.contains_key(&topition.topic[..]) {
                    _ = responses.insert(topition.clone(), CommittedOffset::uncommitted());
                    continue;
                }

                let key = postcard::to_stdvec(&OffsetCommitKey::new(
                    group_id,
                    &topition.topic,
                    topition.partition,
                ))?;

                let offset = match self.db.get(&key).await {
                    Ok(Some(encoded)) => {
                        let value: OffsetCommitValue = decode_offset(&encoded)?;
                        CommittedOffset {
                            offset: value.offset,
                            leader_epoch: None,
                            metadata: None,
                        }
                    }
                    Ok(None) => CommittedOffset::uncommitted(), // No committed offset
                    Err(err) => {
                        debug!(?err, ?group_id, ?topition);
                        return Err(Error::Slate(Arc::new(err)));
                    }
                };

                _ = responses.insert(topition.clone(), offset);
            }
        }

        Ok(responses)
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        let topics = self.get_topics().await?;
        let mut responses = Vec::with_capacity(offsets.len());

        for (topition, list_offset) in offsets {
            let Some(metadata) = topics.get(&topition.topic[..]) else {
                responses.push((
                    topition.clone(),
                    ListOffsetResponse {
                        error_code: ErrorCode::UnknownTopicOrPartition,
                        offset: None,
                        timestamp: None,
                    },
                ));
                continue;
            };

            if topition.partition < 0 || topition.partition >= metadata.topic.num_partitions {
                responses.push((
                    topition.clone(),
                    ListOffsetResponse {
                        error_code: ErrorCode::UnknownTopicOrPartition,
                        offset: None,
                        timestamp: None,
                    },
                ));
                continue;
            }

            let watermark_key =
                postcard::to_stdvec(&WatermarkKey::new(metadata.id, topition.partition))?;

            let watermark = self
                .db
                .get(&watermark_key)
                .await
                .map_err(Error::from)
                .and_then(|watermark| {
                    watermark.map_or(Ok(Watermark::default()), |encoded| {
                        postcard::from_bytes(&encoded[..]).map_err(Into::into)
                    })
                })?;

            let response = match list_offset {
                ListOffset::Earliest => {
                    // The log start offset is the low watermark, advanced by
                    // delete_records; timestamps below it are pruned there.
                    let offset = watermark.low.unwrap_or(0);
                    let timestamp = watermark
                        .timestamps
                        .as_ref()
                        .and_then(|ts| ts.iter().find(|(_, off)| **off >= offset))
                        .and_then(|(ts, _)| to_system_time(*ts).ok());

                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        offset: Some(offset),
                        timestamp,
                    }
                }
                ListOffset::Latest => {
                    // For ReadCommitted, return Last Stable Offset instead of High Watermark
                    let offset = if isolation_level == IsolationLevel::ReadCommitted {
                        let offset_stage = self.offset_stage(topition).await?;
                        offset_stage.last_stable
                    } else {
                        watermark.high.unwrap_or(0)
                    };
                    let timestamp = watermark
                        .timestamps
                        .as_ref()
                        .and_then(|ts| ts.last_key_value())
                        .and_then(|(ts, _)| to_system_time(*ts).ok());

                    ListOffsetResponse {
                        error_code: ErrorCode::None,
                        offset: Some(offset),
                        timestamp,
                    }
                }
                ListOffset::Timestamp(target_ts) => {
                    // Find the first offset with timestamp >= target
                    // target_ts is SystemTime, need to convert to i64 for comparison
                    let target_millis = target_ts
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);

                    let result = watermark.timestamps.as_ref().and_then(|ts| {
                        ts.range(target_millis..)
                            .next()
                            .map(|(ts, off)| (*off, *ts))
                    });

                    match result {
                        Some((offset, ts)) => ListOffsetResponse {
                            error_code: ErrorCode::None,
                            offset: Some(offset),
                            timestamp: to_system_time(ts).ok(),
                        },
                        // No record at or after the target: no offset (see
                        // `ListOffsetResponse::offset`).
                        None => ListOffsetResponse {
                            error_code: ErrorCode::None,
                            offset: None,
                            timestamp: None,
                        },
                    }
                }
            };

            responses.push((topition.clone(), response));
        }

        Ok(responses)
    }

    async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
        let brokers = vec![
            MetadataResponseBroker::default()
                .node_id(self.node)
                .host(
                    self.advertised_listener
                        .host_str()
                        .unwrap_or("0.0.0.0")
                        .into(),
                )
                .port(self.advertised_listener.port().unwrap_or(9092).into())
                .rack(None),
        ];

        let existing_topics = self.get_topics().await?;

        let topic_to_response = |topic_metadata: &TopicMetadata| {
            let name = Some(topic_metadata.topic.name.to_owned());
            let error_code = ErrorCode::None.into();
            let topic_id = Some(topic_metadata.id.into_bytes());
            let is_internal = Some(false);
            let num_partitions = topic_metadata.topic.num_partitions;
            let replication_factor = topic_metadata.topic.replication_factor;

            let partitions = Some(nisshi_storage::assign_replicas(
                &[self.node],
                num_partitions,
                replication_factor,
                error_code,
                0,
            )?);

            Ok(MetadataResponseTopic::default()
                .error_code(error_code)
                .name(name)
                .topic_id(topic_id)
                .is_internal(is_internal)
                .partitions(partitions)
                .topic_authorized_operations(Some(i32::MIN)))
        };

        let topic_responses = match topics {
            Some(topic_ids) if !topic_ids.is_empty() => {
                // Filter by requested topics
                let mut responses = Vec::with_capacity(topic_ids.len());

                for topic_id in topic_ids {
                    match topic_id {
                        TopicId::Name(name) => {
                            if let Some(metadata) = existing_topics.get(name.as_str()) {
                                responses.push(topic_to_response(metadata)?);
                            } else {
                                // Topic not found - return error response
                                responses.push(
                                    MetadataResponseTopic::default()
                                        .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                        .name(Some(name.clone()))
                                        .topic_id(Some(NULL_TOPIC_ID))
                                        .is_internal(Some(false))
                                        .partitions(Some([].into()))
                                        .topic_authorized_operations(Some(i32::MIN)),
                                );
                            }
                        }
                        TopicId::Id(id) => {
                            // Find topic by UUID
                            let found =
                                existing_topics.values().find(|metadata| metadata.id == *id);

                            if let Some(metadata) = found {
                                responses.push(topic_to_response(metadata)?);
                            } else {
                                // Topic not found - return error response
                                responses.push(
                                    MetadataResponseTopic::default()
                                        .error_code(ErrorCode::UnknownTopicOrPartition.into())
                                        .name(None)
                                        .topic_id(Some(id.into_bytes()))
                                        .is_internal(Some(false))
                                        .partitions(Some([].into()))
                                        .topic_authorized_operations(Some(i32::MIN)),
                                );
                            }
                        }
                    }
                }

                responses
            }
            _ => {
                // Return all topics
                existing_topics
                    .values()
                    .map(topic_to_response)
                    .collect::<Result<_>>()?
            }
        };

        Ok(MetadataResponse {
            cluster: Some(self.cluster.clone()),
            controller: Some(self.node),
            brokers,
            topics: topic_responses,
        })
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        // TODO: Filter config entries by requested keys
        if keys.is_some() {
            tracing::warn!(
                "describe_config key filtering is not implemented, returning all configs"
            );
        }
        match resource {
            ConfigResource::Topic => match self.topic_metadata(&TopicId::Name(name.into())).await {
                Ok(Some(topic_metadata)) => {
                    let error_code = ErrorCode::None;

                    Ok(DescribeConfigsResult::default()
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .resource_type(i8::from(resource))
                        .resource_name(name.into())
                        .configs(topic_metadata.topic.configs.map(|configs| {
                            configs
                                .iter()
                                .map(|config| {
                                    DescribeConfigsResourceResult::default()
                                        .name(config.name.clone())
                                        .value(config.value.clone())
                                        .read_only(false)
                                        .is_default(None)
                                        .config_source(Some(ConfigSource::DefaultConfig.into()))
                                        .is_sensitive(false)
                                        .synonyms(Some([].into()))
                                        .config_type(Some(ConfigResource::Topic.into()))
                                        .documentation(Some("".into()))
                                })
                                .collect()
                        })))
                }

                Ok(None) => {
                    let error_code = ErrorCode::UnknownTopicOrPartition;

                    Ok(DescribeConfigsResult::default()
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .resource_type(i8::from(resource))
                        .resource_name(name.into())
                        .configs(Some([].into())))
                }

                Err(_) => {
                    let error_code = ErrorCode::UnknownServerError;

                    Ok(DescribeConfigsResult::default()
                        .error_code(error_code.into())
                        .error_message(Some(error_code.to_string()))
                        .resource_type(i8::from(resource))
                        .resource_name(name.into())
                        .configs(Some([].into())))
                }
            },
            _ => {
                // For other resource types, return empty config
                Ok(DescribeConfigsResult::default()
                    .error_code(ErrorCode::None.into())
                    .error_message(Some(ErrorCode::None.to_string()))
                    .resource_type(i8::from(resource))
                    .resource_name(name.into())
                    .configs(Some([].into())))
            }
        }
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        // TODO: Implement pagination with partition_limit and cursor
        // Currently returns all partitions regardless of limit
        if partition_limit > 0 || cursor.is_some() {
            tracing::warn!(
                "describe_topic_partitions pagination is not implemented, returning all partitions"
            );
        }
        let mut responses =
            Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or_default());

        for topic in topics.unwrap_or_default() {
            match self.topic_metadata(topic).await {
                Ok(Some(topic_metadata)) => {
                    responses.push(
                        DescribeTopicPartitionsResponseTopic::default()
                            .error_code(ErrorCode::None.into())
                            .name(Some(topic_metadata.topic.name))
                            .topic_id(topic.into())
                            .is_internal(false)
                            .partitions(Some(
                                (0..topic_metadata.topic.num_partitions)
                                    .map(|partition_index| {
                                        DescribeTopicPartitionsResponsePartition::default()
                                            .error_code(ErrorCode::None.into())
                                            .partition_index(partition_index)
                                            .leader_id(self.node)
                                            .leader_epoch(0)
                                            .replica_nodes(Some(
                                                nisshi_storage::single_node_replicas(
                                                    self.node,
                                                    topic_metadata.topic.replication_factor,
                                                ),
                                            ))
                                            .isr_nodes(Some(nisshi_storage::single_node_replicas(
                                                self.node,
                                                topic_metadata.topic.replication_factor,
                                            )))
                                            .eligible_leader_replicas(Some(vec![]))
                                            .last_known_elr(Some(vec![]))
                                            .offline_replicas(Some(vec![]))
                                    })
                                    .collect(),
                            ))
                            .topic_authorized_operations(-2147483648),
                    );
                }

                Ok(None) => {
                    responses.push(
                        DescribeTopicPartitionsResponseTopic::default()
                            .error_code(ErrorCode::UnknownTopicOrPartition.into())
                            .name(match topic {
                                TopicId::Name(name) => Some(name.into()),
                                TopicId::Id(_) => None,
                            })
                            .topic_id(match topic {
                                TopicId::Name(_) => NULL_TOPIC_ID,
                                TopicId::Id(id) => id.into_bytes(),
                            })
                            .is_internal(false)
                            .partitions(Some([].into()))
                            .topic_authorized_operations(-2147483648),
                    );
                }

                Err(_) => {
                    responses.push(
                        DescribeTopicPartitionsResponseTopic::default()
                            .error_code(ErrorCode::UnknownServerError.into())
                            .name(match topic {
                                TopicId::Name(name) => Some(name.into()),
                                TopicId::Id(_) => None,
                            })
                            .topic_id(match topic {
                                TopicId::Name(_) => NULL_TOPIC_ID,
                                TopicId::Id(id) => id.into_bytes(),
                            })
                            .is_internal(false)
                            .partitions(Some([].into()))
                            .topic_authorized_operations(-2147483648),
                    );
                }
            }
        }

        Ok(responses)
    }

    async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
        let prefix = postcard::to_stdvec(&GroupKeyPrefix::new())?;
        let mut groups = vec![];

        let mut scan = self.db.scan(prefix.clone()..).await?;

        while let Some(kv) = scan.next().await? {
            if !kv.key.starts_with(&prefix) {
                break;
            }

            if let Ok(key) = postcard::from_bytes::<GroupKey>(&kv.key) {
                let stored = GroupDetailVersion::decode(&kv.value).map(|gdv| gdv.detail);

                groups.push(NamedGroupDetail::decoded(key.group_id, Some(stored)));
            }
        }

        Ok(groups)
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            let tx = self
                .db
                .begin(slatedb::IsolationLevel::SerializableSnapshot)
                .await
                .inspect_err(|err| debug!(?err))?;

            for group_id in group_ids {
                // Delete group state
                let group_key = postcard::to_stdvec(&GroupKey::new(group_id))?;
                let had_group = tx.get(&group_key).await?.is_some();

                if had_group {
                    tx.delete(&group_key)?;
                }

                // Delete committed offsets for this group
                let offset_prefix = postcard::to_stdvec(&OffsetCommitKeyPrefix::new(group_id))?;
                let mut deleted_offsets = false;

                // Note: SlateDB doesn't support range deletes directly,
                // so we scan and delete individually
                let mut scan = self.db.scan(offset_prefix.clone()..).await?;
                while let Some(kv) = scan.next().await? {
                    if !kv.key.starts_with(&offset_prefix) {
                        break;
                    }
                    tx.delete(&kv.key)?;
                    deleted_offsets = true;
                }

                results.push(
                    DeletableGroupResult::default()
                        .group_id(group_id.into())
                        .error_code(
                            if had_group || deleted_offsets {
                                ErrorCode::None
                            } else {
                                ErrorCode::GroupIdNotFound
                            }
                            .into(),
                        ),
                );
            }

            _ = tx.commit().await.map_err(Error::from)?;
        }

        Ok(results)
    }

    async fn describe_groups(
        &self,
        group_ids: Option<&[String]>,
        _include_authorized_operations: bool,
    ) -> Result<Vec<NamedGroupDetail>> {
        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                let key = postcard::to_stdvec(&GroupKey::new(group_id))?;

                match self.db.get(&key).await {
                    Ok(Some(encoded)) => match GroupDetailVersion::decode(&encoded) {
                        Ok(gdv) => {
                            results.push(NamedGroupDetail::found(group_id.into(), gdv.detail));
                        }
                        // A group that can't be decoded is reported as an error, not as an empty
                        // group, so that a delete of the group refuses it instead of deleting it.
                        Err(err) => {
                            warn!(?err, group_id, "the stored group can't be decoded");
                            results.push(NamedGroupDetail::error_code(
                                group_id.into(),
                                ErrorCode::UnknownServerError,
                            ));
                        }
                    },
                    Ok(None) => {
                        results.push(NamedGroupDetail::found(
                            group_id.into(),
                            GroupDetail::default(),
                        ));
                    }
                    Err(_) => {
                        results.push(NamedGroupDetail::error_code(
                            group_id.into(),
                            ErrorCode::UnknownServerError,
                        ));
                    }
                }
            }
        }

        Ok(results)
    }

    async fn group_with_version(&self, group_id: &str) -> Result<Option<(GroupDetail, Version)>> {
        let key = postcard::to_stdvec(&GroupKey::new(group_id))?;

        let stored = self
            .db
            .get(&key)
            .await
            .map_err(Error::from)
            .inspect_err(|err| debug!(?err, group_id))?
            .map(|encoded| GroupDetailVersion::decode(&encoded))
            .transpose()
            .inspect_err(|err| error!(?err, group_id, "the stored group can't be decoded"))?;

        // An offset commit stores a placeholder for a group without state, with the default
        // version, which no `update_group` stores. The group reads as having no state, as on the
        // other backends, and `update_group` accepts no version for the placeholder.
        Ok(stored
            .filter(|stored| stored.version != Version::default())
            .map(|stored| (stored.detail, stored.version)))
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))
            .map_err(|err| UpdateError::Error(Error::Slate(Arc::new(err))))?;

        let key = postcard::to_stdvec(&GroupKey::new(group_id))
            .map_err(|err| UpdateError::Error(Error::Postcard(err)))?;

        // Load the existing group; a missing key yields the default detail
        // and version. The caller's version must match: no version matches
        // only a missing group or the empty placeholder that offset_commit
        // stores, so a group with state is reported as outdated rather than
        // silently overwritten.
        let current = self
            .load_group(&tx, &key)
            .await
            .map_err(UpdateError::Error)?;

        if version.unwrap_or_default() != current.version {
            tx.rollback();
            return Err(UpdateError::Outdated {
                current: Box::new(current.detail),
                version: current.version,
            });
        }

        let updated_version = Version::from(&Uuid::now_v7());
        let new_group = GroupDetailVersion::default()
            .detail(detail)
            .version(updated_version.clone());

        self.save_metadata(&tx, &key, &new_group)
            .map_err(UpdateError::Error)?;

        match tx.commit().await {
            // `commit` now returns the write handle; nothing here needs it.
            Ok(_) => Ok(updated_version),

            // Another update to this group committed while this transaction
            // was open: report the group as outdated, so that the caller
            // retries against what is now stored.
            Err(err) if err.kind() == slatedb::ErrorKind::Transaction => {
                debug!(?err, ?group_id);

                let current: GroupDetailVersion = self
                    .db
                    .get(&key)
                    .await
                    .map_err(Error::from)
                    .and_then(|encoded| {
                        encoded.map_or(Ok(GroupDetailVersion::default()), |encoded| {
                            GroupDetailVersion::decode(&encoded).map_err(Into::into)
                        })
                    })
                    .map_err(UpdateError::Error)?;

                Err(UpdateError::Outdated {
                    current: Box::new(current.detail),
                    version: current.version,
                })
            }

            Err(err) => Err(UpdateError::Error(Error::Slate(Arc::new(err)))),
        }
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        let claim = match producer_claim(producer_id, producer_epoch) {
            Ok(claim) => claim,
            Err(error) => return Ok(ProducerIdResponse::failed(error)),
        };

        if let Some(transaction_id) = transaction_id {
            // Transactional producer initialization
            let tx = self
                .db
                .begin(slatedb::IsolationLevel::SerializableSnapshot)
                .await
                .inspect_err(|err| debug!(?err))?;

            let mut transactions: Transactions =
                self.load_metadata(&tx, Self::TRANSACTIONS).await?;
            let mut producers: Producers = self.load_metadata(&tx, Self::PRODUCERS).await?;

            // Check if transaction already exists
            if transactions.contains_key(transaction_id) {
                let existing_txn = transactions
                    .get_mut(transaction_id)
                    .ok_or_else(|| Error::Message("transaction not found".into()))?;
                let producer_id = existing_txn.producer;

                // Check if there's an active epoch that needs to be aborted
                let (old_epoch, needs_abort) = existing_txn
                    .epochs
                    .last_key_value()
                    .map(|(e, detail)| (*e, detail.state == Some(TxnState::Begin)))
                    .unwrap_or((0, false));

                if let Some(claim) = claim {
                    let error = check_claim((producer_id, old_epoch), claim);

                    if error != ErrorCode::None {
                        return Ok(ProducerIdResponse::failed(error));
                    }
                }

                // If old epoch is in Begin state, we need to abort it
                if needs_abort && let Some(old_detail) = existing_txn.epochs.get_mut(&old_epoch) {
                    // Write abort markers for all partitions this transaction produced to
                    let topics = self.get_topics().await?;
                    for (topic_name, partitions) in &old_detail.produces {
                        for partition in partitions.keys() {
                            let Some(metadata) = topics.get(topic_name.as_str()) else {
                                continue;
                            };

                            // Create abort marker batch
                            let control_batch: Bytes =
                                ControlBatch::default().abort().try_into()?;
                            let end_transaction_marker: Bytes =
                                EndTransactionMarker::default().try_into()?;

                            let batch: Batch = InflatedBatch::builder()
                                .record(
                                    Record::builder()
                                        .key(control_batch.into())
                                        .value(end_transaction_marker.into()),
                                )
                                .attributes(
                                    BatchAttribute::default()
                                        .control(true)
                                        .transaction(true)
                                        .into(),
                                )
                                .producer_id(producer_id)
                                .producer_epoch(old_epoch)
                                .base_sequence(-1)
                                .build()
                                .and_then(TryInto::try_into)?;

                            // Get current watermark and increment it
                            let watermark_key =
                                postcard::to_stdvec(&WatermarkKey::new(metadata.id, *partition))?;
                            let mut watermark =
                                tx.get(&watermark_key).await.map_err(Error::from).and_then(
                                    |watermark| {
                                        watermark.map_or(Ok(Watermark::default()), |encoded| {
                                            postcard::from_bytes(&encoded[..]).map_err(Into::into)
                                        })
                                    },
                                )?;

                            let offset = watermark.high.unwrap_or_default();

                            watermark.high = watermark
                                .high
                                .map_or(Some(batch.last_offset_delta as i64 + 1i64), |high| {
                                    Some(high + batch.last_offset_delta as i64 + 1i64)
                                });

                            _ = watermark
                                .timestamps
                                .get_or_insert_default()
                                .insert(batch.base_timestamp, offset);

                            // Encode and store the batch
                            let encoded = {
                                let mut encoder = RecordBatchEncoder::new(BytesMut::new());
                                batch.serialize(&mut encoder)?;
                                Bytes::from(encoder)
                            };

                            let batch_key = postcard::to_stdvec(&BatchKey::new(
                                metadata.id,
                                *partition,
                                offset,
                            ))?;
                            tx.put(batch_key, &encoded[..])?;

                            // Save updated watermark
                            let watermark_value = postcard::to_stdvec(&watermark)?;
                            tx.put(watermark_key, watermark_value)?;
                        }
                    }

                    // Mark old epoch as aborted
                    old_detail.state = Some(TxnState::Aborted);
                }

                // Bump epoch for existing transaction
                let new_epoch = old_epoch + 1;

                // Re-get mutable reference after potential modification
                let existing_txn = transactions
                    .get_mut(transaction_id)
                    .ok_or_else(|| Error::Message("transaction not found".into()))?;
                _ = existing_txn.epochs.insert(
                    new_epoch,
                    TxnDetail {
                        transaction_timeout_ms,
                        started_at: Some(SystemTime::now()),
                        state: Some(TxnState::Begin),
                        ..Default::default()
                    },
                );

                // Also update producer's sequences with the new epoch
                if let Some(producer_detail) = producers.get_mut(&producer_id) {
                    _ = producer_detail.sequences.insert(new_epoch, BTreeMap::new());
                }

                self.save_metadata(&tx, Self::TRANSACTIONS, &transactions)?;
                self.save_metadata(&tx, Self::PRODUCERS, &producers)?;

                _ = tx.commit().await.map_err(Error::from)?;

                return Ok(ProducerIdResponse {
                    id: producer_id,
                    epoch: new_epoch,
                    ..Default::default()
                });
            }

            // Create new transactional producer
            let new_producer_id = producers.last_key_value().map_or(1, |(k, _)| k + 1);
            let epoch = 0i16;

            let mut pd = super::types::ProducerDetail::default();
            _ = pd.sequences.insert(epoch, BTreeMap::new());
            _ = producers.insert(new_producer_id, pd);

            let mut txn = Txn {
                producer: new_producer_id,
                epochs: BTreeMap::new(),
            };
            _ = txn.epochs.insert(
                epoch,
                TxnDetail {
                    transaction_timeout_ms,
                    started_at: Some(SystemTime::now()),
                    state: Some(TxnState::Begin),
                    ..Default::default()
                },
            );
            _ = transactions.insert(transaction_id.to_string(), txn);

            self.save_metadata(&tx, Self::PRODUCERS, &producers)?;
            self.save_metadata(&tx, Self::TRANSACTIONS, &transactions)?;

            _ = tx.commit().await.map_err(Error::from)?;

            Ok(ProducerIdResponse {
                id: new_producer_id,
                epoch,
                ..Default::default()
            })
        } else {
            let tx = self
                .db
                .begin(slatedb::IsolationLevel::SerializableSnapshot)
                .await
                .inspect_err(|err| debug!(?err))?;

            let mut producers: Producers = self.load_metadata(&tx, Self::PRODUCERS).await?;

            let producer = producers.last_key_value().map_or(1.into(), |(k, _v)| k + 1);

            let epoch = 0;
            let mut pd = super::types::ProducerDetail::default();
            _ = pd.sequences.insert(epoch, BTreeMap::new());
            debug!(?producer, ?pd);
            _ = producers.insert(producer, pd);

            self.save_metadata(&tx, Self::PRODUCERS, &producers)?;

            tx.commit()
                .await
                .map_err(Error::from)
                .and(Ok(ProducerIdResponse {
                    id: producer,
                    epoch,
                    ..Default::default()
                }))
        }
    }

    /// Validate that a transaction may commit offsets for a consumer group.
    ///
    /// The group itself is recorded when the offsets arrive via
    /// `txn_offset_commit`, so this only validates the transaction and
    /// producer, matching the PG and object store engines.
    async fn txn_add_offsets(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<ErrorCode> {
        debug!(transaction_id, producer_id, producer_epoch, group_id);

        let transactions = self.get_transactions().await?;

        let Some(transaction) = transactions.get(transaction_id) else {
            return Ok(ErrorCode::TransactionalIdNotFound);
        };

        if transaction.producer != producer_id {
            return Ok(ErrorCode::UnknownProducerId);
        }

        match transaction.epochs.last_key_value() {
            Some((current_epoch, _)) if *current_epoch == producer_epoch => Ok(ErrorCode::None),
            _ => Ok(ErrorCode::ProducerFenced),
        }
    }

    async fn txn_add_partitions(
        &self,
        partitions: TxnAddPartitionsRequest,
    ) -> Result<TxnAddPartitionsResponse> {
        match partitions {
            TxnAddPartitionsRequest::VersionZeroToThree {
                transaction_id,
                producer_id,
                producer_epoch,
                ref topics,
            } => {
                let tx = self
                    .db
                    .begin(slatedb::IsolationLevel::SerializableSnapshot)
                    .await
                    .inspect_err(|err| debug!(?err))?;

                // Helper to create error responses for all topics/partitions
                let make_error_response =
                    |error_code: ErrorCode| -> Vec<AddPartitionsToTxnTopicResult> {
                        topics
                            .iter()
                            .map(|topic| {
                                let results_by_partition = topic
                                    .partitions
                                    .as_deref()
                                    .unwrap_or(&[])
                                    .iter()
                                    .map(|p| {
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*p)
                                            .partition_error_code(error_code.into())
                                    })
                                    .collect();
                                AddPartitionsToTxnTopicResult::default()
                                    .name(topic.name.clone())
                                    .results_by_partition(Some(results_by_partition))
                            })
                            .collect()
                    };

                let mut transactions: Transactions =
                    self.load_metadata(&tx, Self::TRANSACTIONS).await?;

                let Some(transaction) = transactions.get_mut(&transaction_id) else {
                    return Ok(TxnAddPartitionsResponse::VersionZeroToThree(
                        make_error_response(ErrorCode::TransactionalIdNotFound),
                    ));
                };

                if transaction.producer != producer_id {
                    return Ok(TxnAddPartitionsResponse::VersionZeroToThree(
                        make_error_response(ErrorCode::UnknownProducerId),
                    ));
                }

                let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                    return Ok(TxnAddPartitionsResponse::VersionZeroToThree(
                        make_error_response(ErrorCode::ProducerFenced),
                    ));
                };

                if &producer_epoch != current_epoch.key() {
                    return Ok(TxnAddPartitionsResponse::VersionZeroToThree(
                        make_error_response(ErrorCode::ProducerFenced),
                    ));
                }

                let txn_detail = current_epoch.get_mut();
                let mut results = vec![];

                for topic in topics {
                    let mut results_by_partition = vec![];
                    for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                        _ = txn_detail
                            .produces
                            .entry(topic.name.clone())
                            .or_default()
                            .insert(*partition_index, None);

                        results_by_partition.push(
                            AddPartitionsToTxnPartitionResult::default()
                                .partition_index(*partition_index)
                                .partition_error_code(ErrorCode::None.into()),
                        );
                    }
                    results.push(
                        AddPartitionsToTxnTopicResult::default()
                            .name(topic.name.clone())
                            .results_by_partition(Some(results_by_partition)),
                    );
                }

                self.save_metadata(&tx, Self::TRANSACTIONS, &transactions)?;

                _ = tx.commit().await.map_err(Error::from)?;

                Ok(TxnAddPartitionsResponse::VersionZeroToThree(results))
            }

            TxnAddPartitionsRequest::VersionFourPlus { transactions } => {
                use nisshi_sans_io::add_partitions_to_txn_response::AddPartitionsToTxnResult;

                let tx = self
                    .db
                    .begin(slatedb::IsolationLevel::SerializableSnapshot)
                    .await
                    .inspect_err(|err| debug!(?err))?;

                let mut stored_transactions: Transactions =
                    self.load_metadata(&tx, Self::TRANSACTIONS).await?;

                let mut results = Vec::with_capacity(transactions.len());

                for txn_request in transactions {
                    let transaction_id = txn_request.transactional_id.clone();
                    let producer_id = txn_request.producer_id;
                    let producer_epoch = txn_request.producer_epoch;
                    let topics = txn_request.topics.as_deref().unwrap_or(&[]);

                    let make_topic_results =
                        |error_code: ErrorCode| -> Vec<AddPartitionsToTxnTopicResult> {
                            topics
                                .iter()
                                .map(|topic| {
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(
                                            topic
                                                .partitions
                                                .as_deref()
                                                .unwrap_or(&[])
                                                .iter()
                                                .map(|p| {
                                                    AddPartitionsToTxnPartitionResult::default()
                                                        .partition_index(*p)
                                                        .partition_error_code(error_code.into())
                                                })
                                                .collect(),
                                        ))
                                })
                                .collect()
                        };

                    let topic_results = if let Some(transaction) =
                        stored_transactions.get_mut(&transaction_id)
                    {
                        if transaction.producer != producer_id {
                            make_topic_results(ErrorCode::UnknownProducerId)
                        } else if let Some(mut current_epoch) = transaction.epochs.last_entry() {
                            if &producer_epoch != current_epoch.key() {
                                make_topic_results(ErrorCode::ProducerFenced)
                            } else {
                                // Success - add partitions
                                let txn_detail = current_epoch.get_mut();
                                topics
                                    .iter()
                                    .map(|topic| {
                                        let partition_results: Vec<_> = topic
                                            .partitions
                                            .as_deref()
                                            .unwrap_or(&[])
                                            .iter()
                                            .map(|p| {
                                                _ = txn_detail
                                                    .produces
                                                    .entry(topic.name.clone())
                                                    .or_default()
                                                    .insert(*p, None);

                                                AddPartitionsToTxnPartitionResult::default()
                                                    .partition_index(*p)
                                                    .partition_error_code(ErrorCode::None.into())
                                            })
                                            .collect();

                                        AddPartitionsToTxnTopicResult::default()
                                            .name(topic.name.clone())
                                            .results_by_partition(Some(partition_results))
                                    })
                                    .collect()
                            }
                        } else {
                            // No epoch found
                            make_topic_results(ErrorCode::ProducerFenced)
                        }
                    } else {
                        // Transaction not found
                        make_topic_results(ErrorCode::TransactionalIdNotFound)
                    };

                    results.push(
                        AddPartitionsToTxnResult::default()
                            .transactional_id(transaction_id)
                            .topic_results(Some(topic_results)),
                    );
                }

                self.save_metadata(&tx, Self::TRANSACTIONS, &stored_transactions)?;

                _ = tx.commit().await.map_err(Error::from)?;

                Ok(TxnAddPartitionsResponse::VersionFourPlus(results))
            }
        }
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut transactions: Transactions = self.load_metadata(&tx, Self::TRANSACTIONS).await?;

        let error_response = |error_code: ErrorCode| -> Vec<TxnOffsetCommitResponseTopic> {
            offsets
                .topics
                .iter()
                .map(|topic| {
                    TxnOffsetCommitResponseTopic::default()
                        .name(topic.name.clone())
                        .partitions(Some(
                            topic
                                .partitions
                                .as_deref()
                                .unwrap_or(&[])
                                .iter()
                                .map(|p| {
                                    TxnOffsetCommitResponsePartition::default()
                                        .partition_index(p.partition_index)
                                        .error_code(error_code.into())
                                })
                                .collect(),
                        ))
                })
                .collect()
        };

        let Some(transaction) = transactions.get_mut(&offsets.transaction_id) else {
            return Ok(error_response(ErrorCode::TransactionalIdNotFound));
        };

        if transaction.producer != offsets.producer_id {
            return Ok(error_response(ErrorCode::UnknownProducerId));
        }

        let Some(mut current_epoch) = transaction.epochs.last_entry() else {
            return Ok(error_response(ErrorCode::ProducerFenced));
        };

        if &offsets.producer_epoch != current_epoch.key() {
            return Ok(error_response(ErrorCode::ProducerFenced));
        }

        let txn_detail = current_epoch.get_mut();
        let mut responses = vec![];

        for topic in &offsets.topics {
            let mut partition_responses = vec![];

            if let Some(partitions) = topic.partitions.as_deref() {
                for partition in partitions {
                    _ = txn_detail
                        .offsets
                        .entry(offsets.group_id.clone())
                        .or_default()
                        .entry(topic.name.clone())
                        .or_default()
                        .insert(
                            partition.partition_index,
                            TxnCommitOffset {
                                committed_offset: partition.committed_offset,
                                leader_epoch: partition.committed_leader_epoch,
                                metadata: partition.committed_metadata.clone(),
                            },
                        );

                    partition_responses.push(
                        TxnOffsetCommitResponsePartition::default()
                            .partition_index(partition.partition_index)
                            .error_code(ErrorCode::None.into()),
                    );
                }
            }

            responses.push(
                TxnOffsetCommitResponseTopic::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_responses)),
            );
        }

        postcard::to_stdvec(&transactions)
            .map_err(Error::from)
            .and_then(|encoded| tx.put(Self::TRANSACTIONS, encoded).map_err(Into::into))?;

        _ = tx.commit().await.map_err(Error::from)?;

        Ok(responses)
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let tx = self
            .db
            .begin(slatedb::IsolationLevel::SerializableSnapshot)
            .await
            .inspect_err(|err| debug!(?err))?;

        let mut transactions: Transactions = self.load_metadata(&tx, Self::TRANSACTIONS).await?;

        // First, validate the transaction and collect necessary information
        let (current_produces, current_offsets) = {
            let Some(transaction) = transactions.get_mut(transaction_id) else {
                return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
            };

            if transaction.producer != producer_id {
                return Err(Error::Api(ErrorCode::UnknownProducerId));
            }

            let Some(mut current_epoch_entry) = transaction.epochs.last_entry() else {
                return Err(Error::Api(ErrorCode::ProducerFenced));
            };

            if &producer_epoch != current_epoch_entry.key() {
                return Err(Error::Api(ErrorCode::ProducerFenced));
            }

            let txn_detail = current_epoch_entry.get_mut();

            if txn_detail.state == Some(TxnState::Begin) {
                txn_detail.state = Some(if committed {
                    TxnState::PrepareCommit
                } else {
                    TxnState::PrepareAbort
                });
            }

            // Clone the produces and offsets for later use
            let produces = txn_detail.produces.clone();
            let offsets = txn_detail.offsets.clone();

            (produces, offsets)
        };

        // Produce commit/abort marker batches for each partition
        // Track the maximum offset after producing control batches (for overlap detection)
        let topics = self.get_topics().await?;
        let mut current_offset_end: i64 = 0;

        for (topic_name, partitions) in &current_produces {
            for partition in partitions.keys() {
                let Some(metadata) = topics.get(topic_name.as_str()) else {
                    continue;
                };

                let topition = Topition::new(topic_name.clone(), *partition);

                // Create the control batch marker
                let control_batch: Bytes = if committed {
                    ControlBatch::default().commit().try_into()?
                } else {
                    ControlBatch::default().abort().try_into()?
                };
                let end_transaction_marker: Bytes = EndTransactionMarker::default().try_into()?;

                let batch: Batch = InflatedBatch::builder()
                    .record(
                        Record::builder()
                            .key(control_batch.into())
                            .value(end_transaction_marker.into()),
                    )
                    .attributes(
                        BatchAttribute::default()
                            .control(true)
                            .transaction(true)
                            .into(),
                    )
                    .producer_id(producer_id)
                    .producer_epoch(producer_epoch)
                    .base_sequence(-1)
                    .build()
                    .and_then(TryInto::try_into)?;

                // Get current watermark and increment it
                let watermark_key =
                    postcard::to_stdvec(&WatermarkKey::new(metadata.id, *partition))?;
                let mut watermark =
                    tx.get(&watermark_key)
                        .await
                        .map_err(Error::from)
                        .and_then(|watermark| {
                            watermark.map_or(Ok(Watermark::default()), |encoded| {
                                postcard::from_bytes(&encoded[..]).map_err(Into::into)
                            })
                        })?;

                let offset = watermark.high.unwrap_or_default();

                // Track the control batch offset (this is the new offset_end for overlap detection)
                current_offset_end = current_offset_end.max(offset);

                watermark.high = watermark
                    .high
                    .map_or(Some(batch.last_offset_delta as i64 + 1i64), |high| {
                        Some(high + batch.last_offset_delta as i64 + 1i64)
                    });

                _ = watermark
                    .timestamps
                    .get_or_insert_default()
                    .insert(batch.base_timestamp, offset);

                // Encode and store the batch
                let encoded = {
                    let mut encoder = RecordBatchEncoder::new(BytesMut::new());
                    batch.serialize(&mut encoder)?;
                    Bytes::from(encoder)
                };

                let batch_key =
                    postcard::to_stdvec(&BatchKey::new(metadata.id, topition.partition, offset))?;
                tx.put(batch_key, &encoded[..])?;

                // Save updated watermark
                let watermark_value = postcard::to_stdvec(&watermark)?;
                tx.put(watermark_key, watermark_value)?;
            }
        }

        // Check for overlapping transactions and collect prepared ones
        // An overlapping transaction is one whose offset range intersects with this transaction
        let mut prepared_overlaps: Vec<(String, i16)> = vec![]; // (transaction_id, epoch)
        let mut has_unprepared_overlap = false;

        for (other_txn_id, other_txn) in transactions.iter() {
            if other_txn_id == transaction_id {
                continue;
            }

            for (other_epoch, other_detail) in &other_txn.epochs {
                // Check if there's any partition overlap where other's offset_start < current's offset_end
                let has_overlap = other_detail
                    .produces
                    .iter()
                    .any(|(topic, other_partitions)| {
                        if let Some(current_partitions) = current_produces.get(topic) {
                            other_partitions.iter().any(|(partition, other_range)| {
                                if current_partitions.contains_key(partition)
                                    && let Some(range) = other_range
                                {
                                    return range.offset_start < current_offset_end;
                                }
                                false
                            })
                        } else {
                            false
                        }
                    });

                if has_overlap {
                    match other_detail.state {
                        Some(TxnState::Begin) => {
                            has_unprepared_overlap = true;
                        }
                        Some(TxnState::PrepareCommit) | Some(TxnState::PrepareAbort) => {
                            prepared_overlaps.push((other_txn_id.clone(), *other_epoch));
                        }
                        _ => {}
                    }
                }
            }
        }

        // If there are unprepared overlapping transactions, only mark as PrepareCommit/PrepareAbort
        // Otherwise, complete all prepared overlapping transactions and the current transaction
        if !has_unprepared_overlap {
            // First, apply offset commits for the current transaction
            if committed {
                for (group_id, group_topics) in &current_offsets {
                    for (topic_name, partitions) in group_topics {
                        for (partition, commit_offset) in partitions {
                            let key = postcard::to_stdvec(&OffsetCommitKey::new(
                                group_id, topic_name, *partition,
                            ))?;

                            let value = postcard::to_stdvec(&OffsetCommitValue {
                                offset: commit_offset.committed_offset,
                                leader_epoch: commit_offset.leader_epoch,
                                metadata: commit_offset.metadata.clone(),
                            })?;

                            tx.put(key, value)?;
                        }
                    }
                }
            }

            // Mark current transaction as complete
            {
                let transaction = transactions
                    .get_mut(transaction_id)
                    .ok_or_else(|| Error::Message("transaction not found".into()))?;
                let txn_detail = transaction
                    .epochs
                    .get_mut(&producer_epoch)
                    .ok_or_else(|| Error::Message("transaction epoch not found".into()))?;
                txn_detail.state = Some(if committed {
                    TxnState::Committed
                } else {
                    TxnState::Aborted
                });
            }

            // Also complete all prepared overlapping transactions
            for (overlap_txn_id, overlap_epoch) in &prepared_overlaps {
                let Some(overlap_txn) = transactions.get_mut(overlap_txn_id) else {
                    continue;
                };
                let Some(overlap_detail) = overlap_txn.epochs.get_mut(overlap_epoch) else {
                    continue;
                };

                // Apply offset commits for PrepareCommit transactions
                if overlap_detail.state == Some(TxnState::PrepareCommit) {
                    for (group_id, group_topics) in &overlap_detail.offsets {
                        for (topic_name, partitions) in group_topics {
                            for (partition, commit_offset) in partitions {
                                let key = postcard::to_stdvec(&OffsetCommitKey::new(
                                    group_id, topic_name, *partition,
                                ))?;

                                let value = postcard::to_stdvec(&OffsetCommitValue {
                                    offset: commit_offset.committed_offset,
                                    leader_epoch: commit_offset.leader_epoch,
                                    metadata: commit_offset.metadata.clone(),
                                })?;

                                tx.put(key, value)?;
                            }
                        }
                    }
                }

                // Update state to final
                overlap_detail.state =
                    Some(if overlap_detail.state == Some(TxnState::PrepareCommit) {
                        TxnState::Committed
                    } else {
                        TxnState::Aborted
                    });
            }
        }

        self.save_metadata(&tx, Self::TRANSACTIONS, &transactions)?;

        _ = tx.commit().await.map_err(Error::from)?;

        Ok(ErrorCode::None)
    }

    /// Maintenance callback for periodic cleanup operations.
    ///
    /// Applies the `delete` and `compact` cleanup policies and runs lake
    /// maintenance if configured. This aligns with PG's maintain
    /// implementation.
    async fn maintain(&self, now: SystemTime) -> Result<()> {
        let deleted = self.policy_delete(now).await?;
        debug!(deleted);

        let compacted = self.policy_compact().await?;
        debug!(compacted);

        if let Some(ref lake) = self.lake {
            return lake.maintain().await.map_err(Into::into);
        }

        Ok(())
    }

    async fn maintain_transactions(&self, _now: SystemTime) -> Result<()> {
        Ok(())
    }

    async fn aborted_transactions(
        &self,
        _topition: &Topition,
        _offset: i64,
        _last_stable_offset: i64,
    ) -> Result<Vec<AbortedTransaction>> {
        Ok(vec![])
    }

    async fn cluster_id(&self) -> Result<String> {
        Ok(self.cluster.clone())
    }

    async fn node(&self) -> Result<i32> {
        Ok(self.node)
    }

    async fn advertised_listener(&self) -> Result<url::Url> {
        Ok(self.advertised_listener.clone())
    }

    async fn delete_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<()> {
        let key = postcard::to_stdvec(&UserScramCredentialKey::new(user, mechanism))?;
        self.db.delete(key).await.map(|_| ()).map_err(Into::into)
    }

    async fn upsert_user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        credential: ScramCredential,
    ) -> Result<()> {
        let key = postcard::to_stdvec(&UserScramCredentialKey::new(user, mechanism))?;
        let value = postcard::to_stdvec(&StoredScramCredential::from(credential))?;
        self.db
            .put(key, value)
            .await
            .map(|_| ())
            .map_err(Into::into)
    }

    async fn user_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        let key = postcard::to_stdvec(&UserScramCredentialKey::new(user, mechanism))?;

        self.db
            .get(&key)
            .await
            .map_err(Error::from)
            .and_then(|maybe| {
                maybe
                    .map(|encoded| {
                        postcard::from_bytes::<StoredScramCredential>(&encoded)
                            .map(ScramCredential::from)
                            .map_err(Into::into)
                    })
                    .transpose()
            })
    }

    async fn ping(&self) -> Result<()> {
        // Verify connectivity by attempting a simple read operation
        let _ = self.db.get(Self::BROKERS).await?;
        Ok(())
    }
}
