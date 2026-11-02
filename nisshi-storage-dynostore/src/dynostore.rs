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

//! Dynamic Object Storage engine (S3, memory, ...)

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    fmt::{Debug, Display},
    future::ready,
    str::FromStr,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Instant, SystemTime},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{
    StreamExt,
    stream::{self, BoxStream, TryStreamExt},
};
use metadata::Cache;
use nisshi_sans_io::topic_config;
use nisshi_sans_io::{
    BatchAttribute, ByteSize as _, ConfigResource, ConfigSource, ConfigType, ControlBatch,
    EndTransactionMarker, ErrorCode, IsolationLevel, ListOffset, NULL_TOPIC_ID, OpType,
    ScramMechanism,
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
    incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    metadata_response::{MetadataResponseBroker, MetadataResponseTopic},
    record::{Record, deflated, inflated},
    to_system_time, to_timestamp,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use nisshi_schema::{
    Registry,
    lake::{House, LakeHouse as _},
};
use nisshi_storage::{
    BrokerRegistrationRequest, CommittedOffset, DELETE_RECORDS_INVALID_LOW_WATERMARK, Error,
    GroupDetail, ListOffsetResponse, METER, MetadataResponse, NamedGroupDetail, OLDER_EPOCH_ERROR,
    OffsetCommitRequest, OffsetStage, ProduceEpoch, ProducerIdResponse, Result, ScramCredential,
    Storage, StoredEpochs, TopicId, Topition, TxnAddPartitionsRequest, TxnAddPartitionsResponse,
    TxnOffsetCommitRequest, TxnState, UpdateError, Version, check_claim, config::apply_op,
    delete_records_cutoff, epoch_is_exhausted, inflate_produced, max_message_bytes, offload,
    producer_claim,
};
use object_store::{
    Attribute, AttributeValue, Attributes, CopyOptions, DynObjectStore, GetOptions, GetResult,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, UpdateVersion,
    path::{Path, PathPart},
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};
use opticon::OptiCon;
use percent_encoding::percent_decode_str;
use produced_batches::{ProducedBatches, SequenceCheck};
use rand::{prelude::*, rng};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use time_index::TimeIndex;
use tokio::time::Duration;
use tracing::{debug, error, info, instrument, warn};
use url::Url;
use uuid::Uuid;

mod metadata;
mod opticon;
mod produced_batches;
mod time_index;

#[cfg(test)]
mod tests;

const APPLICATION_JSON: &str = "application/json";

/// Dynostore stores a consumer group's id as a single, opaque, percent-encoded
/// [`PathPart`] (see [`group_path_part`]), so any group id - including `""`,
/// `"/"`, or one containing any other `object_store`-reserved character -
/// becomes its own distinct, independently addressable group.
///
/// The empty group id is the one case `PathPart`'s own encoding can't carry:
/// percent-encoding `""` still produces an empty string, and joining an empty
/// segment onto a `Path` produces a key containing an empty path segment
/// (e.g. `consumers//offsets/...`). Plain prefix-based operations
/// (`list`/`delete`, used everywhere else in this module) happen to still
/// treat that key as its own, correctly isolated group, because
/// `object_store`'s prefix-match semantics require the byte right after the
/// matched prefix to be a delimiter, and the doubled slash supplies it.
/// `list_groups` is the exception: it lists via `list_with_delimiter`, whose
/// common-prefix computation normalizes the doubled slash away, which
/// surfaces a phantom group under the wrong name instead of the empty-id
/// group. `EMPTY_GROUP_SENTINEL` stands in for the empty id instead, so
/// `list_groups` never has to see a double slash in the first place.
///
/// This sentinel can never collide with another group's real, encoded path
/// segment:
/// - It can't collide with an *encoded* segment, because `percent_encode_byte`
///   always emits uppercase hex after `%`, and `m` (lowercase) is not a hex
///   digit in either case - so no real percent-escape sequence can ever begin
///   `%em...`, which means `%empty` can never be the start of the encoded
///   form of some other input.
/// - It can't collide with an *unencoded* literal group id of `"%empty"`
///   either, because `%` itself is in `object_store`'s reserved/escaped
///   character set: a group literally named `"%empty"` is non-empty, so it
///   goes through ordinary [`PathPart`] encoding and comes out as `%25empty`
///   (see `percent_empty_literal_is_escaped_as_percent25empty` in `tests.rs`),
///   which is a different string to the sentinel.
const EMPTY_GROUP_SENTINEL: &str = "%empty";

/// How long after a batch object's write, on the store's clock, every
/// produce below it is taken to have written its own batch object or
/// failed.
///
/// A produce assigns its offset in the watermark document before it writes
/// its batch object, so a listing can see a later batch and not an earlier
/// one that is still in flight. A gap below a batch written this long ago
/// is taken as a produce that failed in between, which holds no records,
/// and the backfill logs a warning for it. The broker does not enforce
/// this bound: the batch object's PUT gives up after the object store
/// client's retry timeout, three minutes by default, and a lake commit or
/// a transaction CAS between the two writes has no limit of its own. A
/// produce that overruns this margin is indexed past, and nothing rebuilds
/// a complete index.
///
/// The age is the difference between two write times that the store
/// reports, the batch object's and the watermark document's, so the
/// broker's clock does not enter into it.
const BATCH_SETTLED_AFTER: Duration = Duration::from_secs(10 * 60);

/// How many group state objects a `ListGroups` reads from the object store at once.
///
/// A listing that reads the objects one after another waits one round trip per group. A listing
/// that reads them all at once opens a connection per group: with thousands of groups, it can use
/// up the broker's file descriptors, which every other request also needs, or exceed the object
/// store's request rate. The limit is a small bound between the two. Its value is not measured:
/// any small bound gives most of the speed-up.
const CONCURRENT_GROUP_STATE_READS: usize = 16;

/// How far below the fetch offset a Fetch starts its second listing, when
/// its first listing, which starts at the fetch offset, finds no batch at or
/// below the fetch offset.
///
/// The fetch offset is then inside a batch that starts below it. Each later
/// listing starts [`FETCH_LISTING_GROWTH`] times further below, so a batch
/// of `n` records takes about `log₈ n` listings to find, however large the
/// partition is.
const FETCH_LISTING_FIRST_WINDOW: i64 = 512;

/// How many times further below the fetch offset each listing after the
/// second starts (see [`FETCH_LISTING_FIRST_WINDOW`]).
const FETCH_LISTING_GROWTH: i64 = 8;

/// The batches that a fetch reads from a partition: each batch below
/// `high_watermark` that ends at or after `offset`, until the batches fill
/// `max_bytes` or `deadline` passes.
#[derive(Clone, Copy, Debug)]
struct FetchBounds {
    offset: i64,
    max_bytes: u32,
    high_watermark: i64,
    deadline: Option<Instant>,
}

/// Counts the fetches that list the whole partition, because the listing
/// near the fetch offset was not a contiguous run of batches from the batch
/// that holds it.
static FETCH_FULL_LISTING: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_fetch_full_listing")
        .with_description(
            "Fetches that listed the whole partition, because the listing near the fetch \
             offset was out of key order or had a gap",
        )
        .build()
});

/// Whether this process has logged a warning for a fetch that listed the
/// whole partition. Each later one logs at debug, because a store that lists
/// out of key order makes every fetch list the whole partition.
static FETCH_FULL_LISTING_WARNED: AtomicBool = AtomicBool::new(false);

/// A backfill of one partition's time index in progress (see
/// [`DynoStore::begin_time_index_backfill`]).
#[derive(Clone, Copy, Debug)]
struct Backfill {
    /// The token that [`TimeIndex::begin_backfill`] set, or found set.
    token: u64,
    /// The partition's log start when the backfill began.
    low: i64,
    /// The partition's high watermark when the backfill began: every batch
    /// below it had assigned its offset before the backfill's listing.
    high: i64,
    /// When the backfill began, on the store's clock: the write time the
    /// store reports for the watermark document after the begin CAS.
    began: SystemTime,
    /// Whether the index was at an older version when the backfill began
    /// (see [`TimeIndex::is_older_version`]), so that the logs say why the
    /// partition is rebuilt.
    older_version: bool,
}

/// Encode a Kafka group id as a single, opaque `object_store` path segment.
///
/// Every character [`PathPart`] treats as reserved (including `/`) is percent
/// encoded, so the result is always exactly one path segment regardless of
/// what the group id contains. See [`EMPTY_GROUP_SENTINEL`] for why the empty
/// string needs special-casing rather than going through the same encoding.
/// The sentinel is validated rather than encoded, so the result is fallible
/// in its type, though a constant without `/` or a control character passes.
fn group_path_part(group_id: &str) -> Result<PathPart<'_>> {
    if group_id.is_empty() {
        parse_segment(EMPTY_GROUP_SENTINEL)
    } else {
        Ok(PathPart::from(group_id))
    }
}

/// Validates `segment` as one already-encoded path segment, as
/// [`PathPart::parse`] does, and reports a bad segment as a storage error.
fn parse_segment(segment: &str) -> Result<PathPart<'_>> {
    PathPart::parse(segment).map_err(|source| {
        object_store::Error::from(object_store::path::Error::BadSegment {
            path: segment.to_owned(),
            source,
        })
        .into()
    })
}

/// Recover the original group id from a listed `object_store` path segment
/// previously written by [`group_path_part`].
///
/// A key written by something else under the `consumers/` prefix (or simply
/// corrupted data) could percent-decode to invalid UTF-8; rather than panic,
/// this returns `None` so `list_groups` can skip that listing entry.
fn decode_group_segment(segment: &str) -> Option<String> {
    if segment == EMPTY_GROUP_SENTINEL {
        return Some(String::new());
    }

    percent_decode_str(segment)
        .decode_utf8()
        .inspect_err(|error| {
            warn!(
                segment,
                %error,
                "skipping list_groups entry: path segment is not valid UTF-8 after percent-decoding"
            )
        })
        .ok()
        .map(Cow::into_owned)
}

#[derive(Clone, Debug)]
pub struct DynoStore {
    /// The storage engine's name for people, in errors.
    engine: &'static str,
    cluster: String,
    node: i32,
    advertised_listener: Url,
    schemas: Option<Registry>,
    lake: Option<House>,
    watermarks: Arc<Mutex<BTreeMap<Topition, OptiCon<Watermark>>>>,
    /// One gate per partition, so that concurrent `ListOffsets(Timestamp)`
    /// requests in this process share one time index backfill instead of
    /// each reading every batch of the partition.
    backfills: Arc<Mutex<BTreeMap<Topition, Arc<tokio::sync::Mutex<()>>>>>,
    meta: OptiCon<Meta>,

    object_store: Arc<DynObjectStore>,
}

type Group = String;
type Offset = i64;
type Partition = i32;
type ProducerEpoch = i16;
type ProducerId = i64;
type Sequence = i32;
type Topic = String;

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct Meta {
    producers: BTreeMap<ProducerId, ProducerDetail>,
    topics: BTreeMap<Topic, TopicMetadata>,
    transactions: BTreeMap<String, Txn>,
}

impl OptiCon<Meta> {
    fn new(cluster: &str) -> Self {
        Self::path(format!("clusters/{cluster}/meta.json"))
    }
}

impl Meta {
    /// Whether a transactional ID owns the producer ID.
    fn transactional_id_owns(&self, producer_id: ProducerId) -> bool {
        self.transactions
            .values()
            .any(|txn| txn.producer == producer_id)
    }

    fn produced(
        &self,
        transaction_id: &str,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
    ) -> Result<BTreeMap<Topition, TxnProduceOffset>> {
        let Some(txn) = self.transactions.get(transaction_id) else {
            return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
        };

        if txn.producer != producer_id {
            return Err(Error::Api(ErrorCode::UnknownProducerId));
        }

        let Some(txn_detail) = txn.epochs.get(&producer_epoch) else {
            return Err(Error::Api(ErrorCode::ProducerFenced));
        };

        let mut produced = BTreeMap::new();

        for (topic, partitions) in txn_detail.produces.iter() {
            for (partition, offset_range) in partitions.iter() {
                let Some(offset_range) = offset_range else {
                    continue;
                };

                let tp = Topition::new(topic.to_owned(), *partition);
                assert_eq!(None, produced.insert(tp, *offset_range));
            }
        }

        Ok(produced)
    }

    /// Refuses a client's transactional batch for a partition that is not in its producer's
    /// ongoing transaction. EndTxn clears a transaction's `produces`, so a partition is there
    /// only while the transaction is ongoing.
    ///
    /// Kafka's transaction coordinator answers an unknown transactional ID, or one that another
    /// producer ID owns, `INVALID_PRODUCER_ID_MAPPING`, and a partition that is not in the
    /// transaction `TRANSACTION_ABORTABLE` ([TransactionCoordinator.scala#L357-L402]). Storing the
    /// batch anyway would leave records that no transaction marker ever closes.
    ///
    /// [TransactionCoordinator.scala#L357-L402]: https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala#L357-L402
    fn ensure_partition_in_transaction(
        &self,
        transaction_id: &str,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
        topition: &Topition,
    ) -> Result<()> {
        let Some(txn) = self
            .transactions
            .get(transaction_id)
            .filter(|txn| txn.producer == producer_id)
        else {
            return Err(Error::Api(ErrorCode::InvalidProducerIdMapping));
        };

        let Some(txn_detail) = txn.epochs.get(&producer_epoch).filter(|txn_detail| {
            txn_detail
                .produces
                .get(topition.topic())
                .is_some_and(|partitions| partitions.contains_key(&topition.partition()))
        }) else {
            return Err(Error::Api(ErrorCode::TransactionAbortable));
        };

        // A transaction that is preparing to commit or abort has its markers written, and its
        // `produces` stays until an older overlapping transaction resolves, so a batch stored
        // now would land after the marker.
        if txn_detail.state.is_some_and(|state| state.is_prepared()) {
            Err(Error::Api(ErrorCode::ConcurrentTransactions))
        } else {
            Ok(())
        }
    }

    fn overlapping_transactions(
        &self,
        transaction_id: &str,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
    ) -> Result<Vec<TxnId>> {
        let candidates = self.produced(transaction_id, producer_id, producer_epoch)?;

        let mut overlapping = Vec::new();

        'candidates: for (candidate_id, txn) in self.transactions.iter() {
            for (epoch, txn_detail) in txn.epochs.iter() {
                if transaction_id == candidate_id
                    && producer_id == txn.producer
                    && producer_epoch == *epoch
                {
                    continue;
                }

                let Some(state) = txn_detail.state else {
                    continue;
                };

                for (topic, partitions) in txn_detail.produces.iter() {
                    for (partition, offset_range) in partitions.iter() {
                        let Some(offset_range) = offset_range else {
                            continue;
                        };

                        let tp = Topition::new(topic.to_owned(), *partition);

                        if let Some(candidate) = candidates.get(&tp)
                            && offset_range.offset_start < candidate.offset_end
                        {
                            overlapping.push(TxnId {
                                transaction: candidate_id.to_owned(),
                                producer_id: txn.producer,
                                producer_epoch: *epoch,
                                state,
                            });

                            continue 'candidates;
                        }
                    }
                }
            }
        }

        Ok(overlapping)
    }

    fn alter_topic(&mut self, topic: &str, changes: &[AlterableConfig]) -> Result<()> {
        let metadata = self
            .topics
            .get_mut(topic)
            .ok_or(Error::Api(ErrorCode::UnknownTopicOrPartition))?;

        let mut configuration = metadata
            .topic
            .configs
            .as_deref()
            .unwrap_or_default()
            .iter()
            .fold(BTreeMap::new(), |mut acc, item| {
                _ = acc.insert(item.name.clone(), item.value.clone());
                acc
            });

        for change in changes {
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
                None => _ = configuration.remove(change.name.as_str()),
            }
        }

        _ = metadata
            .topic
            .configs
            .replace(
                configuration
                    .into_iter()
                    .fold(Vec::new(), |mut acc, (key, value)| {
                        acc.push(CreatableTopicConfig::default().name(key).value(value));
                        acc
                    }),
            );

        Ok(())
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct ProducerDetail {
    sequences: BTreeMap<ProducerEpoch, BTreeMap<String, BTreeMap<i32, Sequence>>>,
    /// The batches that the producer last wrote to each partition, at the partition's epoch.
    ///
    /// A partition's newest remembered batch ends just before its next sequence in
    /// `sequences`. Only the advance that follows a record write changes either of them, and
    /// it changes both. A partition has no entry when its sequence was stored before the broker
    /// remembered batches.
    #[serde(default)]
    batches: BTreeMap<ProducerEpoch, BTreeMap<String, BTreeMap<i32, ProducedBatches>>>,
    /// The base offset of the first batch that the producer wrote to each partition at the
    /// partition's epoch, for an epoch above 0.
    ///
    /// The advance after a record write uses it to tell whether a batch at the previous epoch
    /// is in the log ahead of every batch at this epoch. The key is left out while the map is
    /// empty, so a producer that never bumps its epoch adds nothing.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    epoch_starts: BTreeMap<ProducerEpoch, BTreeMap<String, BTreeMap<i32, i64>>>,
}

impl ProducerDetail {
    /// Returns the epochs that a batch from this producer to `topition` is checked against.
    fn stored_epochs(&self, transactional: bool, topition: &Topition) -> Option<StoredEpochs> {
        let newest = *self.sequences.keys().next_back()?;

        let partition = self
            .sequences
            .iter()
            .rev()
            .find(|(_, topics)| {
                topics
                    .get(&topition.topic)
                    .is_some_and(|partitions| partitions.contains_key(&topition.partition))
            })
            .map(|(epoch, _)| *epoch);

        Some(StoredEpochs {
            transactional,
            newest,
            partition,
        })
    }

    /// Returns the sequence that the producer's next batch to the partition must start at, at
    /// `epoch`, 0 when the partition has none.
    fn next_sequence(&self, epoch: ProducerEpoch, topition: &Topition) -> Sequence {
        self.sequences
            .get(&epoch)
            .and_then(|topics| topics.get(&topition.topic))
            .and_then(|partitions| partitions.get(&topition.partition))
            .copied()
            .unwrap_or_default()
    }

    /// Returns the batches that the producer last wrote to the partition at `epoch`, or `None`
    /// when the partition's sequence was stored before the broker remembered batches.
    fn remembered_batches(
        &self,
        epoch: ProducerEpoch,
        topition: &Topition,
    ) -> Option<&ProducedBatches> {
        self.batches
            .get(&epoch)
            .and_then(|topics| topics.get(&topition.topic))
            .and_then(|partitions| partitions.get(&topition.partition))
    }

    /// Whether a batch at `epoch`, stored at `offset`, is in the log ahead of every batch at a
    /// newer epoch on the partition.
    ///
    /// It is when the partition's epoch is the one after `epoch`, and that epoch's first batch
    /// on the partition is at a higher offset. When the partition's epoch is further ahead, the
    /// partition no longer holds where the epochs in between started, so this answers false.
    fn is_ahead_of_next_epoch(
        &self,
        epoch: ProducerEpoch,
        topition: &Topition,
        offset: i64,
    ) -> bool {
        epoch
            .checked_add(1)
            .and_then(|next| self.epoch_starts.get(&next))
            .and_then(|topics| topics.get(&topition.topic))
            .and_then(|partitions| partitions.get(&topition.partition))
            .is_some_and(|&start| offset < start)
    }

    /// Records a batch stored at `base_offset`: advances the partition's next sequence at
    /// `epoch` past the batch, and remembers the batch. `produce_epoch` says whether the batch
    /// starts `epoch` on the partition.
    fn advance(
        &mut self,
        epoch: ProducerEpoch,
        produce_epoch: ProduceEpoch,
        topition: &Topition,
        base_sequence: Sequence,
        last_offset_delta: i32,
        base_offset: i64,
    ) {
        let sequence = match produce_epoch {
            ProduceEpoch::Continue => self.sequence(epoch, topition),
            ProduceEpoch::Start => self.start_epoch(epoch, topition, base_offset),
        };

        *sequence += last_offset_delta + 1;

        self.batches
            .entry(epoch)
            .or_default()
            .entry(topition.topic.clone())
            .or_default()
            .entry(topition.partition)
            .or_default()
            .remember(base_sequence, last_offset_delta, base_offset);
    }

    /// Returns the next sequence of `topition` at `epoch`, after the producer starts that
    /// epoch on the partition with a batch at `base_offset`.
    ///
    /// Starting an epoch removes the partition's sequences, remembered batches and epoch
    /// starts at older epochs, and each older epoch that then holds none. A produce never reads
    /// them again, because the broker refuses a batch at an older epoch than its partition's.
    /// Keeping them would grow meta.json, which every idempotent produce rewrites, by an entry
    /// for each bump.
    fn start_epoch(
        &mut self,
        epoch: ProducerEpoch,
        topition: &Topition,
        base_offset: i64,
    ) -> &mut Sequence {
        forget_older_epochs(&mut self.sequences, epoch, topition);
        forget_older_epochs(&mut self.batches, epoch, topition);
        forget_older_epochs(&mut self.epoch_starts, epoch, topition);

        if epoch > 0 {
            _ = self
                .epoch_starts
                .entry(epoch)
                .or_default()
                .entry(topition.topic.clone())
                .or_default()
                .insert(topition.partition, base_offset);
        }

        self.sequence(epoch, topition)
    }

    /// Returns the next sequence of `topition` at `epoch`, 0 when the partition has none.
    fn sequence(&mut self, epoch: ProducerEpoch, topition: &Topition) -> &mut Sequence {
        self.sequences
            .entry(epoch)
            .or_default()
            .entry(topition.topic.clone())
            .or_default()
            .entry(topition.partition)
            .or_default()
    }
}

/// Removes `topition` from each epoch of `by_epoch` older than `epoch`, and each older epoch
/// that then holds no partition.
fn forget_older_epochs<T>(
    by_epoch: &mut BTreeMap<ProducerEpoch, BTreeMap<String, BTreeMap<i32, T>>>,
    epoch: ProducerEpoch,
    topition: &Topition,
) {
    by_epoch.retain(|older, topics| {
        if *older >= epoch {
            return true;
        }

        if let Some(partitions) = topics.get_mut(&topition.topic) {
            _ = partitions.remove(&topition.partition);

            if partitions.is_empty() {
                _ = topics.remove(&topition.topic);
            }
        }

        !topics.is_empty()
    });
}

/// Allocates a new producer ID, one more than the newest, and gives it epoch 0.
///
/// Returns `None` when the newest producer ID is `i64::MAX`.
fn allocate_producer_id(
    producers: &mut BTreeMap<ProducerId, ProducerDetail>,
) -> Option<ProducerId> {
    let id = producers
        .last_key_value()
        .map_or(Some(1), |(newest, _)| newest.checked_add(1))?;

    _ = producers.insert(
        id,
        ProducerDetail {
            sequences: BTreeMap::from([(0, BTreeMap::new())]),
            batches: BTreeMap::new(),
            epoch_starts: BTreeMap::new(),
        },
    );

    Some(id)
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TxnId {
    transaction: String,
    producer_id: ProducerId,
    producer_epoch: ProducerEpoch,
    state: TxnState,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct Txn {
    producer: ProducerId,
    epochs: BTreeMap<ProducerEpoch, TxnDetail>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TxnDetail {
    transaction_timeout_ms: i32,
    started_at: Option<SystemTime>,
    state: Option<TxnState>,
    produces: BTreeMap<Topic, BTreeMap<Partition, Option<TxnProduceOffset>>>,
    offsets: BTreeMap<Group, BTreeMap<Topic, BTreeMap<Partition, TxnCommitOffset>>>,
}

impl From<&TxnDetail> for BTreeMap<Topition, Offset> {
    fn from(value: &TxnDetail) -> Self {
        let mut result = BTreeMap::new();

        for (topic, partitions) in value.produces.iter() {
            for (partition, offset_range) in partitions.iter() {
                let Some(offset_range) = offset_range else {
                    continue;
                };

                let tp = Topition::new(topic.to_owned(), *partition);
                assert_eq!(None, result.insert(tp, offset_range.offset_start));
            }
        }

        result
    }
}

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
struct TxnProduceOffset {
    offset_start: Offset,
    offset_end: Offset,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TxnCommitOffset {
    committed_offset: Offset,
    leader_epoch: Option<i32>,
    metadata: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct TopicMetadata {
    id: Uuid,
    topic: CreatableTopic,
}

impl TopicMetadata {
    /// Returns the `DescribeConfigs` result for this topic. The result holds each config that the
    /// topic sets, and gives `name` as the resource name.
    fn describe_configs_result(&self, name: &str) -> DescribeConfigsResult {
        let error_code = ErrorCode::None;

        DescribeConfigsResult::default()
            .error_code(error_code.into())
            .error_message(Some(error_code.to_string()))
            .resource_type(i8::from(ConfigResource::Topic))
            .resource_name(name.into())
            .configs(self.topic.configs.as_ref().map(|configs| {
                configs
                    .iter()
                    .map(|config| {
                        DescribeConfigsResourceResult::default()
                            .name(config.name.clone())
                            .value(config.value.clone())
                            .read_only(false)
                            .is_default(None)
                            .config_source(Some(ConfigSource::DynamicTopicConfig.into()))
                            .is_sensitive(false)
                            .synonyms(Some([].into()))
                            .config_type(Some(ConfigType::String.into()))
                            .documentation(Some("".into()))
                    })
                    .collect()
            }))
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct Watermark {
    low: Option<i64>,
    high: Option<i64>,
    /// The index that answers `ListOffsets(Timestamp)` for this partition.
    /// A document written before the index existed lacks this key, and
    /// decodes to an incomplete index that the first lookup backfills (see
    /// `dynostore::tests::watermark_decodes_pre_time_index_documents`).
    /// Those documents carry a `timestamps` key that was always `null`, and
    /// this type ignores it.
    #[serde(default)]
    time_index: TimeIndex,
    /// The newest epoch above 0 of each idempotent producer that wrote to this partition.
    ///
    /// The produce path checks and records it in the compare-and-swap that gives a batch its
    /// offset, so a batch at an older epoch is refused before it gets an offset. The sequence
    /// check in `meta.json` runs in separate steps, before the offset and after the record
    /// write, so it cannot refuse a batch that passed it before the producer's epoch moved.
    ///
    /// The key is left out while the map is empty, so a producer that never bumps its epoch
    /// adds nothing. A document written before the key existed decodes to an empty map, and
    /// a broker that does not know the key drops it when it writes the document.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    producer_epochs: BTreeMap<ProducerId, ProducerEpoch>,
}

impl Watermark {
    /// The most producers whose epoch [`Watermark::producer_epochs`] holds for one partition.
    ///
    /// When a new producer goes over this limit, the partition forgets the producer with the
    /// smallest ID. Producer IDs only go up, so that is the producer whose ID the broker
    /// allocated first, which is not always the one that wrote last. A forgotten producer's
    /// older-epoch batch is checked only against `meta.json` again, which refuses it after its
    /// record write if it is behind the newer epoch's batches. Kafka 3.9.1
    /// limits the producers it keeps by idle time (`producer.id.expiration.ms`) and not by
    /// count ([`ProducerStateManager.removeExpiredProducers`]). We limit by count instead,
    /// because every produce to the partition reads and writes this document, and a count
    /// keeps it small without a clock.
    ///
    /// [`ProducerStateManager.removeExpiredProducers`]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerStateManager.java#L359-L374
    const MAX_FENCED_PRODUCERS: usize = 1024;

    /// Refuses a batch whose epoch is older than the newest epoch this partition holds for its
    /// producer, with [`OLDER_EPOCH_ERROR`], as the sequence check does.
    fn fence_producer_epoch(
        &self,
        producer_id: ProducerId,
        producer_epoch: ProducerEpoch,
    ) -> Result<()> {
        if self
            .producer_epochs
            .get(&producer_id)
            .is_some_and(|&newest| newest > producer_epoch)
        {
            Err(Error::Api(OLDER_EPOCH_ERROR))
        } else {
            Ok(())
        }
    }

    /// Records a batch's epoch as its producer's newest on this partition, when it is above 0
    /// and above the epoch already recorded.
    fn record_producer_epoch(&mut self, producer_id: ProducerId, producer_epoch: ProducerEpoch) {
        if producer_epoch <= 0
            || self
                .producer_epochs
                .get(&producer_id)
                .is_some_and(|&newest| newest >= producer_epoch)
        {
            return;
        }

        _ = self.producer_epochs.insert(producer_id, producer_epoch);

        if self.producer_epochs.len() > Self::MAX_FENCED_PRODUCERS {
            _ = self.producer_epochs.pop_first();
        }
    }
}

/// Returns the last stable offset of a partition, from its high watermark,
/// its log start and the first offset of its earliest open transaction.
fn last_stable(
    high_watermark: Offset,
    log_start: Offset,
    open_transaction_start: Option<Offset>,
) -> Offset {
    // A broker can pair a cached high watermark with open transactions that
    // another broker recorded after it, so an open transaction can start
    // past the high watermark read here. The last stable offset never passes
    // the high watermark it is answered with.
    //
    // DeleteRecords can move the log start past the first offset of an open
    // transaction. Kafka then raises the last stable offset to the log start
    // ([UnifiedLog.scala#L964-L978]), so that it never names an offset below
    // the log.
    //
    // [UnifiedLog.scala#L964-L978]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/log/UnifiedLog.scala#L964-L978
    open_transaction_start
        .unwrap_or(high_watermark)
        .min(high_watermark)
        .max(log_start)
}

/// Returns the [`OffsetStage`] of a partition (see [`last_stable`]).
fn offset_stage_of(
    high_watermark: Offset,
    log_start: Offset,
    open_transaction_start: Option<Offset>,
) -> OffsetStage {
    OffsetStage {
        last_stable: last_stable(high_watermark, log_start, open_transaction_start),
        high_watermark,
        log_start,
    }
}

/// `clusters/{cluster}/topics/{topic}`, the prefix every key of a topic's
/// data is stored beneath.
///
/// The topic is exactly one segment: [`PathPart`] percent-encodes "/", so a
/// name holding "/" cannot reach another topic's keys. A valid topic name
/// (`[a-zA-Z0-9._-]`, not `.` or `..`) holds no character [`PathPart`]
/// encodes, so its keys are the ones `Path::from(format!(..))` gave. Only a
/// name holding "/" has keys that differ. `extend` skips an empty segment,
/// as `Path::from` does, so the empty name keeps its key too.
fn topic_prefix(cluster: &str, topic: &str) -> Path {
    let mut path = Path::from(format!("clusters/{cluster}/topics"));
    path.extend([PathPart::from(topic)]);
    path
}

/// `clusters/{cluster}/topics/{topic}/partitions/{partition:0>10}`.
fn partition_prefix(cluster: &str, topition: &Topition) -> Path {
    topic_prefix(cluster, topition.topic.as_str())
        .join("partitions")
        .join(format!("{:0>10}", topition.partition))
}

/// The prefix a partition's batch objects are stored beneath.
fn records_prefix(cluster: &str, topition: &Topition) -> Path {
    partition_prefix(cluster, topition).join("records")
}

/// The key of the batch object whose first record is at `base_offset`.
fn batch_path(cluster: &str, topition: &Topition, base_offset: i64) -> Path {
    records_prefix(cluster, topition).join(format!("{base_offset:0>20}.batch"))
}

/// The base offset of a batch object that sits directly under
/// `records_prefix`, or `None` for any other object, such as one a produce
/// to a name holding "/" left there before topic keys were one segment.
fn batch_base_offset(records_prefix: &Path, location: &Path) -> Option<i64> {
    let mut parts = location.prefix_match(records_prefix)?;
    let name = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let digits = name.as_ref().strip_suffix(".batch")?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    i64::from_str(digits).ok()
}

impl OptiCon<Watermark> {
    fn new(cluster: &str, topition: &Topition) -> Self {
        Self::path(partition_prefix(cluster, topition).join("watermark.json"))
    }
}

fn json_content_type() -> Attributes {
    let mut attributes = Attributes::new();
    _ = attributes.insert(
        Attribute::ContentType,
        AttributeValue::from(APPLICATION_JSON),
    );
    attributes
}

impl DynoStore {
    /// Only `factory` constructs a [`DynoStore`] outside tests; everywhere else goes through
    /// [`nisshi_storage::StorageFactory::build`] and a configured URL, so this (and the builder
    /// methods below) is `pub` only under `test-support`, for `nisshi-broker`'s
    /// fault-injection tests to supply their own [`ObjectStore`].
    #[cfg(feature = "test-support")]
    pub fn new(cluster: &str, node: i32, object_store: impl ObjectStore) -> Result<Self> {
        Self::new_with(cluster, node, object_store)
    }

    #[cfg(not(feature = "test-support"))]
    pub(crate) fn new(cluster: &str, node: i32, object_store: impl ObjectStore) -> Result<Self> {
        Self::new_with(cluster, node, object_store)
    }

    fn new_with(cluster: &str, node: i32, object_store: impl ObjectStore) -> Result<Self> {
        Ok(Self {
            engine: "object storage",
            cluster: cluster.into(),
            node,
            advertised_listener: Url::parse("tcp://127.0.0.1/")?,
            schemas: None,

            lake: None,

            watermarks: Arc::new(Mutex::new(BTreeMap::new())),
            backfills: Arc::new(Mutex::new(BTreeMap::new())),
            meta: OptiCon::<Meta>::new(cluster),
            object_store: Arc::new(Cache::new(
                Metron::new(object_store, cluster),
                Duration::from_millis(5_000),
            )?),
        })
    }

    /// Names the storage engine, e.g. `S3 (s3://)`, in the errors that it returns.
    pub(crate) fn engine(self, engine: &'static str) -> Self {
        Self { engine, ..self }
    }

    #[cfg(feature = "test-support")]
    pub fn advertised_listener(self, advertised_listener: Url) -> Self {
        Self {
            advertised_listener,
            ..self
        }
    }

    #[cfg(not(feature = "test-support"))]
    pub(crate) fn advertised_listener(self, advertised_listener: Url) -> Self {
        Self {
            advertised_listener,
            ..self
        }
    }

    #[cfg(feature = "test-support")]
    pub fn schemas(self, schemas: Option<Registry>) -> Self {
        Self { schemas, ..self }
    }

    #[cfg(not(feature = "test-support"))]
    pub(crate) fn schemas(self, schemas: Option<Registry>) -> Self {
        Self { schemas, ..self }
    }

    #[cfg(feature = "test-support")]
    pub fn lake(self, lake: Option<House>) -> Self {
        Self { lake, ..self }
    }

    #[cfg(not(feature = "test-support"))]
    pub(crate) fn lake(self, lake: Option<House>) -> Self {
        Self { lake, ..self }
    }

    async fn topic_metadata(&self, topic: &TopicId) -> Result<Option<TopicMetadata>> {
        debug!(?topic);

        self.meta
            .with(&self.object_store, |meta| {
                Ok(match topic {
                    TopicId::Name(name) => meta.topics.get(name).cloned(),
                    TopicId::Id(id) => meta
                        .topics
                        .values()
                        .find(|metadata| &metadata.id == id)
                        .cloned(),
                })
            })
            .await
    }

    /// Ensures that `topition` names an existing partition: its topic exists, and its index is
    /// below the topic's partition count. Returns the topic's metadata.
    ///
    /// Returns [`Error::Api`] with [`ErrorCode::UnknownTopicOrPartition`] for an unknown topic or an
    /// index outside the topic, and with [`ErrorCode::KafkaStorageError`] for a failed read of
    /// the object store.
    async fn ensure_partition_exists(&self, topition: &Topition) -> Result<TopicMetadata> {
        let Some(metadata) = self
            .topic_metadata(&TopicId::Name(topition.topic.clone()))
            .await
            .map_err(metadata_read_error)?
        else {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        };

        if topition.partition < 0 || topition.partition >= metadata.topic.num_partitions {
            return Err(Error::Api(ErrorCode::UnknownTopicOrPartition));
        }

        Ok(metadata)
    }

    fn encode(&self, deflated: deflated::Batch) -> Result<PutPayload> {
        Ok(PutPayload::from(Bytes::from(deflated)))
    }

    fn decode(&self, encoded: Bytes) -> Result<deflated::Batch> {
        debug!(encoded = encoded.len());
        deflated::Batch::try_from(encoded)
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
    }

    /// `clusters/{cluster}/groups/consumers`, the prefix every group - state
    /// file and committed offsets alike - is stored beneath.
    fn group_consumers_prefix(&self) -> Path {
        Path::from(format!("clusters/{}/groups/consumers", self.cluster))
    }

    /// The single-file location of a group's persisted [`GroupDetail`] state,
    /// e.g. `clusters/{cluster}/groups/consumers/{group}.json`.
    fn group_state_location(&self, group_id: &str) -> Result<Path> {
        let file_name = format!("{}.json", group_path_part(group_id)?.as_ref());

        Ok(self
            .group_consumers_prefix()
            .join(parse_segment(&file_name)?))
    }

    /// The prefix a group's committed offsets are stored beneath, e.g.
    /// `clusters/{cluster}/groups/consumers/{group}/offsets`.
    fn group_offsets_prefix(&self, group_id: &str) -> Result<Path> {
        Ok(self
            .group_consumers_prefix()
            .join(group_path_part(group_id)?)
            .join("offsets"))
    }

    /// The location of a single committed offset, e.g.
    /// `clusters/{cluster}/groups/consumers/{group}/offsets/{topic}/partitions/{partition:0>10}.json`.
    ///
    /// The topic keeps the segment rule `Path::from`, which splits on "/",
    /// unlike its data keys (see [`topic_prefix`]); for a valid name the two
    /// rules agree. A topic named before CreateTopics rejected "/" keeps the
    /// key it had, and `delete_topic` matches its offsets by the same rule.
    fn committed_offset_location(&self, group_id: &str, topition: &Topition) -> Result<Path> {
        let mut location = self.group_offsets_prefix(group_id)?;
        location.extend(Path::from(topition.topic.as_str()).parts());

        Ok(location
            .join("partitions")
            .join(format!("{:0>10}.json", topition.partition)))
    }

    /// Removes the batch objects of one partition that lie entirely below
    /// `cutoff`, after `DeleteRecords` has committed `cutoff` as the low
    /// watermark.
    ///
    /// Each failure is logged and skipped, instead of returned, because the
    /// watermark advance has already taken effect: an error here would report
    /// a failed request for a delete that succeeded.
    ///
    /// An object is named by the base offset of its batch, and the name does
    /// not hold the end offset. A batch ends before the base offset of the
    /// next batch, so this deletes an object when the next object's base
    /// offset is at or below `cutoff`, without reading either object. The
    /// last object has no successor, so this deletes it only when `cutoff`
    /// is the `high_watermark` that the cut was checked against. Every batch
    /// that begins below that high watermark also ends at or below it. Below
    /// the high watermark, the last object can hold records at or above the
    /// cut, so it stays.
    ///
    /// The listing covers this partition's `records/` prefix only, so that a
    /// mistake here cannot remove the objects of another partition.
    async fn delete_records_batches(&self, topition: &Topition, cutoff: i64, high_watermark: i64) {
        if cutoff <= 0 {
            return;
        }

        let prefix = records_prefix(&self.cluster, topition);

        let mut objects = match self
            .object_store
            .list(Some(&prefix))
            .map_ok(|meta| meta.location)
            .try_collect::<Vec<Path>>()
            .await
        {
            Ok(locations) => locations
                .into_iter()
                .filter_map(|location| {
                    let base_offset = batch_base_offset(&prefix, &location)?;

                    Some((location, base_offset))
                })
                .collect::<Vec<_>>(),

            Err(error) => {
                error!(?error, ?topition, cutoff);
                return;
            }
        };

        objects.sort_by_key(|(_, base_offset)| *base_offset);

        let mut expired = objects
            .windows(2)
            .filter(|window| window[1].1 <= cutoff)
            .map(|window| Ok::<_, object_store::Error>(window[0].0.clone()))
            .collect::<Vec<_>>();

        // A produce that raced this delete can write a batch at `cutoff`
        // after the listing began, so the last object is deleted only when
        // it begins below the cut.
        if cutoff >= high_watermark
            && let Some((last, base_offset)) = objects.last()
            && *base_offset < cutoff
        {
            expired.push(Ok(last.clone()));
        }

        let mut deleted = self
            .object_store
            .delete_stream(stream::iter(expired).boxed());

        while let Some(outcome) = deleted.next().await {
            if let Err(error) = outcome {
                error!(?error, ?topition, cutoff);
            }
        }
    }

    /// Returns [`Self::watermark_for`] `topition`, after it ensures, on the
    /// partition's first read, that the partition exists.
    ///
    /// The partition index comes from the client, and the broker keeps each
    /// cached watermark until the topic is deleted, so the first read checks
    /// the partition (see [`Self::ensure_partition_exists`]). A later read
    /// skips that check, because it would read the metadata again on each
    /// Fetch of the partition.
    /// Returns the open transaction starts: for each partition with an open
    /// transaction, the first offset of the earliest one.
    async fn open_transaction_starts(&self) -> Result<BTreeMap<Topition, Offset>> {
        let starts = self
            .meta
            .with(&self.object_store, |meta| {
                Ok(meta
                    .transactions
                    .values()
                    .flat_map(|txn| {
                        debug!(?txn);

                        txn.epochs
                            .values()
                            .filter(|detail| {
                                detail.state.is_some_and(|state| {
                                    state != TxnState::Committed && state != TxnState::Aborted
                                })
                            })
                            .map(BTreeMap::<Topition, Offset>::from)
                            .collect::<Vec<_>>()
                    })
                    .reduce(|mut acc, e| {
                        debug!(?acc, ?e);

                        for (topition, offset_start) in e.iter() {
                            _ = acc
                                .entry(topition.to_owned())
                                .and_modify(|existing_offset_start| {
                                    if *existing_offset_start > *offset_start {
                                        *existing_offset_start = *offset_start
                                    }
                                })
                                .or_insert(*offset_start);
                        }

                        acc
                    })
                    .unwrap_or(BTreeMap::new()))
            })
            .await?;

        debug!(?starts);

        Ok(starts)
    }

    /// Returns the high watermark and the log start of `topition`.
    ///
    /// A reader of the last stable offset calls this before it reads the open
    /// transactions (see [`Self::open_transaction_starts`]). A transactional
    /// produce records its partition's open transaction before it raises the
    /// high watermark, so a reader that sees the raised high watermark then
    /// sees the open transaction too. Reading them the other way round could
    /// pair the open transactions from before the produce with the high
    /// watermark from after it.
    ///
    /// This holds within one broker process. Across brokers, each caches
    /// `watermark.json` and `meta.json` separately for up to 5s, so a reader
    /// can still pair a fresh high watermark with an older `meta.json`.
    async fn watermark_offsets(&self, topition: &Topition) -> Result<(Offset, Offset)> {
        self.checked_watermark_for(topition)
            .await?
            .with(&self.object_store, |watermark| {
                debug!(?watermark);
                Ok((watermark.high.unwrap_or(0), watermark.low.unwrap_or(0)))
            })
            .await
    }

    /// Records a lower bound of the first offset of `transaction_id` in
    /// `topition`, when the transaction has no offset recorded there yet: the
    /// partition's high watermark before this produce raises it.
    ///
    /// A produce calls this before it raises the high watermark, so that the
    /// last stable offset never passes the records of an open transaction
    /// whose offsets are allocated but not yet recorded. The partition is
    /// then also recorded when the produce fails after it raised the high
    /// watermark, so the end of the transaction writes its marker there.
    ///
    /// The bound can be below the transaction's real first offset, when
    /// another batch takes offsets between the read and the raise. It holds
    /// the last stable offset back until the transaction ends.
    async fn record_transaction_start(
        &self,
        transaction_id: &str,
        producer_epoch: ProducerEpoch,
        topition: &Topition,
    ) -> Result<()> {
        let recorded = |meta: &Meta| {
            meta.transactions
                .get(transaction_id)
                .and_then(|transaction| transaction.epochs.get(&producer_epoch))
                .is_none_or(|txn_detail| {
                    txn_detail
                        .produces
                        .get(&topition.topic)
                        .and_then(|partitions| partitions.get(&topition.partition))
                        .is_some_and(Option::is_some)
                })
        };

        // A later batch of the transaction finds its partition recorded, and
        // writes nothing here.
        if self
            .meta
            .with(&self.object_store, |meta| Ok(recorded(meta)))
            .await?
        {
            return Ok(());
        }

        // The high watermark only rises while the topic exists, so a cached
        // high watermark is at most the offset that the produce allocates.
        let (high_watermark, _) = self.watermark_offsets(topition).await?;

        self.meta
            .with_mut(&self.object_store, |meta| {
                if let Some(txn_detail) = meta
                    .transactions
                    .get_mut(transaction_id)
                    .and_then(|transaction| transaction.epochs.get_mut(&producer_epoch))
                {
                    // The partition is already in the transaction: the check before
                    // this call refused a batch for any other partition.
                    if let Some(range) = txn_detail
                        .produces
                        .get_mut(&topition.topic)
                        .and_then(|partitions| partitions.get_mut(&topition.partition))
                        .filter(|range| range.is_none())
                    {
                        debug!(transaction_id, ?topition, high_watermark);

                        *range = Some(TxnProduceOffset {
                            offset_start: high_watermark,
                            offset_end: high_watermark,
                        });
                    }
                }

                Ok(())
            })
            .await
            .inspect_err(|err| error!(?err, transaction_id, ?topition))
    }

    async fn checked_watermark_for(&self, topition: &Topition) -> Result<OptiCon<Watermark>> {
        if let Some(watermark) = self.watermarks.lock()?.get(topition) {
            return Ok(watermark.to_owned());
        }

        _ = self.ensure_partition_exists(topition).await?;

        self.watermark_for(topition)
    }

    /// Refuses a client's transactional batch for a partition that is not in its producer's
    /// ongoing transaction (see [`Meta::ensure_partition_in_transaction`]). A batch without a
    /// transactional ID, a non-transactional batch and a control batch are not checked: only the
    /// broker writes control batches, when it ends a transaction.
    ///
    /// This reads `meta.json` with a conditional GET; it does not change it.
    async fn ensure_partition_in_transaction(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: &deflated::Batch,
    ) -> Result<()> {
        let Some(transaction_id) = transaction_id else {
            return Ok(());
        };

        if !deflated.is_transactional() || deflated.is_control() {
            return Ok(());
        }

        self.meta
            .with(&self.object_store, |meta| {
                meta.ensure_partition_in_transaction(
                    transaction_id,
                    deflated.producer_id,
                    deflated.producer_epoch,
                    topition,
                )
            })
            .await
            .inspect_err(|err| {
                debug!(
                    transaction_id,
                    ?topition,
                    deflated.producer_id,
                    deflated.producer_epoch,
                    ?err
                )
            })
    }

    /// Returns the (cached, shared) `OptiCon` handle for `topition`'s
    /// watermark document, inserting one if this is the first time this
    /// partition has been touched in this process.
    ///
    /// The caller must already know that the partition exists, because the
    /// broker keeps the handle until the topic is deleted. For an index from
    /// the client, use [`Self::checked_watermark_for`].
    fn watermark_for(&self, topition: &Topition) -> Result<OptiCon<Watermark>> {
        self.watermarks
            .lock()
            .map(|mut locked| {
                locked
                    .entry(topition.to_owned())
                    .or_insert_with(|| OptiCon::<Watermark>::new(self.cluster.as_str(), topition))
                    .to_owned()
            })
            .map_err(Into::into)
    }

    /// Returns the gate that serializes time index backfills of `topition`
    /// in this process.
    fn backfill_gate_for(&self, topition: &Topition) -> Result<Arc<tokio::sync::Mutex<()>>> {
        self.backfills
            .lock()
            .map(|mut locked| locked.entry(topition.to_owned()).or_default().to_owned())
            .map_err(Into::into)
    }

    /// Streams the batch objects of `topition` at or after `start_offset`,
    /// in offset order, as the base offset and write time of each. The
    /// stream lists lazily, so a consumer that stops early lists only up to
    /// where it stopped.
    fn batch_stream(
        &self,
        topition: &Topition,
        start_offset: i64,
    ) -> BoxStream<'static, Result<(i64, SystemTime)>> {
        let prefix = records_prefix(&self.cluster, topition);

        // A batch key zero-pads its base offset, so key order is offset
        // order. A store that supports a start-after LIST then begins at
        // `start_offset`, instead of at the first batch of the partition.
        let list_stream = if start_offset > 0 {
            self.object_store.list_with_offset(
                Some(&prefix),
                &batch_path(&self.cluster, topition, start_offset - 1),
            )
        } else {
            self.object_store.list(Some(&prefix))
        };

        let topition = topition.to_owned();

        list_stream
            .map_err(move |error| {
                error!(?error, ?topition, start_offset, "listing batches");
                Error::Api(ErrorCode::KafkaStorageError)
            })
            .try_filter_map(move |meta| {
                ready(Ok(batch_base_offset(&prefix, &meta.location)
                    .filter(|&base_offset| base_offset >= start_offset)
                    .map(|base_offset| {
                        (base_offset, SystemTime::from(meta.last_modified))
                    })))
            })
            .boxed()
    }

    /// Lists the batch objects of `topition` with a base offset in
    /// `start_offset..end_offset`: the base offset and write time of each.
    async fn list_batches(
        &self,
        topition: &Topition,
        start_offset: i64,
        end_offset: i64,
    ) -> Result<BTreeMap<i64, SystemTime>> {
        self.batch_stream(topition, start_offset)
            .try_take_while(|&(base_offset, _)| ready(Ok(base_offset < end_offset)))
            .try_collect()
            .await
    }

    /// Reads and decodes the batch object at `base_offset`, or returns
    /// `Ok(None)` when the object does not exist.
    ///
    /// A missing object is a batch that was deleted after a LIST returned
    /// it, so a caller can skip it. Every other failure is an error, because
    /// a caller that skips a batch after a transient GET failure answers
    /// from an incomplete log and gives no sign of it. The error is
    /// `KAFKA_STORAGE_ERROR`, the code Kafka answers for a log it cannot
    /// read, which a client retries; `UNKNOWN_SERVER_ERROR` would make it
    /// give up.
    async fn read_batch(
        &self,
        topition: &Topition,
        base_offset: i64,
    ) -> Result<Option<deflated::Batch>> {
        let path = batch_path(&self.cluster, topition, base_offset);

        let encoded = match self.object_store.get(&path).await {
            Ok(get_result) => get_result.bytes().await,
            Err(object_store::Error::NotFound { .. }) => {
                warn!(
                    ?topition,
                    base_offset, "skipping a batch deleted after listing"
                );
                return Ok(None);
            }
            Err(error) => Err(error),
        };

        encoded
            .map_err(Error::from)
            .and_then(|encoded| self.decode(encoded))
            .map(Some)
            .inspect_err(|error| error!(?error, ?topition, base_offset, "reading batch"))
            .map_err(|_| Error::Api(ErrorCode::KafkaStorageError))
    }

    /// Streams the base offsets of `topition`'s batches from the batch that
    /// holds `offset`: the last batch at or below `offset`. Returns
    /// `Ok(None)` when a listing descends, or when no batch is at or below
    /// `offset`.
    ///
    /// Each listing starts further below `offset` than the one before,
    /// until one finds a batch at or below it (see
    /// [`FETCH_LISTING_FIRST_WINDOW`]), so the cost depends on the size of
    /// the batch that holds `offset`, and not on the size of the partition.
    async fn batch_stream_holding(
        &self,
        topition: &Topition,
        offset: i64,
    ) -> Result<Option<BoxStream<'static, Result<i64>>>> {
        let mut window = 0;

        loop {
            let start_offset = offset.saturating_sub(window).max(0);

            // The listing can end inside the loop below and then go on
            // behind the batches it found. An S3 listing panics when it is
            // polled after it ends, so the listing is fused.
            let mut listing = self
                .batch_stream(topition, start_offset)
                .map_ok(|(base_offset, _)| base_offset)
                .fuse();

            let mut holding = None;
            let mut after_holding = None;

            while let Some(base_offset) = listing.try_next().await? {
                if holding.is_some_and(|holding| base_offset <= holding) {
                    return Ok(None);
                }

                if base_offset > offset {
                    after_holding = Some(base_offset);
                    break;
                }

                holding = Some(base_offset);
            }

            if holding.is_some() {
                return Ok(Some(
                    stream::iter(holding.into_iter().chain(after_holding).map(Ok))
                        .chain(listing)
                        .boxed(),
                ));
            }

            // From the log start up, each offset has a batch at or below it,
            // unless the partition holds no batch objects. A listing from
            // offset 0 that finds none is out of key order, or `offset` is
            // below the log start, and the full listing answers both.
            if start_offset == 0 {
                return Ok(None);
            }

            window = if window == 0 {
                FETCH_LISTING_FIRST_WINDOW
            } else {
                window.saturating_mul(FETCH_LISTING_GROWTH)
            };
        }
    }

    /// Reads the batches of `topition` within `bounds`, from `base_offsets`
    /// in offset order.
    ///
    /// With `requires_contiguous`, returns `Ok(None)` at the first batch that
    /// does not start at the offset after the previous batch's last offset.
    /// A produce gives each batch the offsets right after the previous
    /// batch's, so such a batch shows that the listing is out of key order,
    /// or that it skips a gap that a failed produce left. Either way the
    /// batches read so far may not be the ones that the fetch must return.
    ///
    /// The batch that fills `max_bytes` is returned whole, wherever it is.
    /// Kafka returns a first batch larger than `max_bytes` whole, so that a
    /// consumer always gets a record ([`ReplicaManager.readFromLog`]), and
    /// cuts a later batch at `max_bytes`, which leaves the client a partial
    /// batch to drop.
    ///
    /// [`ReplicaManager.readFromLog`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1733
    async fn read_fetched_batches(
        &self,
        topition: &Topition,
        bounds: FetchBounds,
        mut base_offsets: BoxStream<'static, Result<i64>>,
        requires_contiguous: bool,
    ) -> Result<Option<Vec<deflated::Batch>>> {
        let mut batches = vec![];
        let mut bytes_left = u64::from(bounds.max_bytes);
        let mut next_base_offset = None;

        while let Some(base_offset) = base_offsets.try_next().await? {
            if requires_contiguous && next_base_offset.is_some_and(|next| base_offset != next) {
                return Ok(None);
            }

            if base_offset >= bounds.high_watermark {
                break;
            }

            // A batch deleted after the listing leaves no last offset, so the
            // batch after it is not checked.
            let Some(mut batch) = self.read_batch(topition, base_offset).await? else {
                next_base_offset = None;
                continue;
            };

            batch.base_offset = base_offset;

            let last_offset = base_offset + i64::from(batch.last_offset_delta);
            next_base_offset = Some(last_offset + 1);

            if last_offset < bounds.offset {
                continue;
            }

            let size = u64::try_from(batch.size_in_bytes()?)?;
            batches.push(batch);

            if size > bytes_left {
                break;
            }

            bytes_left -= size;

            // The deadline ends a fetch only after it has a batch, because a
            // fetch that answers empty when its listing is slow answers empty
            // on every retry, and the consumer never moves (#797). The Fetch
            // service bounds the whole read with a deadline of its own.
            if bounds
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                break;
            }
        }

        Ok(Some(batches))
    }

    /// Reads the batches of `topition` within `bounds`, from a listing of the
    /// whole partition that this function sorts.
    async fn fetch_from_full_listing(
        &self,
        topition: &Topition,
        bounds: FetchBounds,
    ) -> Result<Vec<deflated::Batch>> {
        let mut base_offsets = self
            .batch_stream(topition, 0)
            .map_ok(|(base_offset, _)| base_offset)
            .try_filter(|&base_offset| ready(base_offset < bounds.high_watermark))
            .try_collect::<BTreeSet<_>>()
            .await?;

        let holding = base_offsets
            .range(..=bounds.offset)
            .next_back()
            .copied()
            .unwrap_or(bounds.offset);

        let wanted = base_offsets.split_off(&holding);

        self.read_fetched_batches(
            topition,
            bounds,
            stream::iter(wanted.into_iter().map(Ok)).boxed(),
            false,
        )
        .await
        .map(Option::unwrap_or_default)
    }

    /// Marks a backfill of `topition`'s time index in progress, with a CAS
    /// write of its token (see [`TimeIndex::begin_backfill`]), and returns
    /// the token with the partition's log start and high watermark at that
    /// moment, and the store's write time of that CAS. The three steps of a
    /// backfill are this, then [`DynoStore::collect_time_index_candidate`],
    /// then [`DynoStore::commit_time_index_backfill`], so that a produce
    /// landing between any two of them is never lost (see
    /// [`TimeIndex::merge`]).
    async fn begin_time_index_backfill(&self, topition: &Topition) -> Result<Backfill> {
        let token = rng().random::<u64>();
        let watermark = self.watermark_for(topition)?;

        let (token, low, high, older_version) = watermark
            .with_mut(&self.object_store, |w| {
                let older_version = w.time_index.is_older_version();

                Ok((
                    w.time_index.begin_backfill(token),
                    w.low.unwrap_or(0),
                    w.high.unwrap_or(0),
                    older_version,
                ))
            })
            .await?;

        // The store's write time of the document the CAS just wrote is the
        // backfill's start on the store's clock, the clock that stamps every
        // batch object's write time as well.
        let began = self
            .object_store
            .head(watermark.location())
            .await
            .map(|meta| SystemTime::from(meta.last_modified))?;

        Ok(Backfill {
            token,
            low,
            high,
            began,
            older_version,
        })
    }

    /// Lists every batch object of `topition` below the high watermark of
    /// `backfill` and replays the time index's append rule over each batch's
    /// maximum record timestamp (see [`DynoStore::max_record_timestamp`]),
    /// in offset order, into a candidate index. Returns that index
    /// and the number of batches listed. Pure read: never writes anything.
    ///
    /// The candidate is complete when the listed batches cover every offset
    /// from the log start to that high watermark. A gap in the listing is a
    /// produce that assigned its offset and has not yet written its batch
    /// object, or that failed in between; a batch written after the gap
    /// that is older than [`BATCH_SETTLED_AFTER`] settles it as the latter.
    /// The batches past a gap that is not settled get no entry, because an
    /// entry past the batch in flight would start a scan after it, and the
    /// candidate is left incomplete, so that the next lookup lists again.
    /// Their timestamps still raise the candidate's maximum, so that a
    /// lookup does not answer no match for a listed batch. An empty listing
    /// is complete: a candidate without an entry cannot misplace a scan.
    ///
    /// Returns an error when a batch cannot be read, so that the caller
    /// does not commit an index that misses that batch.
    async fn collect_time_index_candidate(
        &self,
        topition: &Topition,
        backfill: &Backfill,
    ) -> Result<(TimeIndex, usize)> {
        let listed = self.list_batches(topition, 0, backfill.high).await?;
        let batches = listed.len();

        info!(
            ?topition,
            batches,
            older_version = backfill.older_version,
            "time index backfill: starting"
        );

        let mut candidate = TimeIndex::default();

        // The offset after the last batch read, or `None` after a batch
        // deleted between the listing and its read, whose length is unknown,
        // so the gap check is off for the batch after it.
        let mut next_offset = Some(backfill.low);
        let mut settled = true;
        let mut headers_disagreeing = 0_usize;
        let mut first_disagreeing_offset = None;

        for (base_offset, written) in listed {
            if settled
                && let Some(expected) = next_offset.filter(|&expected| base_offset > expected)
            {
                // The batch's age when the backfill began, both times the
                // store's own, so the broker's clock does not enter into it.
                let age = backfill
                    .began
                    .duration_since(written)
                    .unwrap_or(Duration::ZERO);

                if age >= BATCH_SETTLED_AFTER {
                    warn!(
                        ?topition,
                        gap = ?(expected..base_offset),
                        age_secs = age.as_secs(),
                        "time index backfill: taking a gap below a settled batch as a failed produce"
                    );
                } else {
                    settled = false;
                }
            }

            let Some(batch) = self.read_batch(topition, base_offset).await? else {
                next_offset = None;
                continue;
            };

            let max_timestamp = Self::max_record_timestamp(topition, base_offset, &batch).await?;
            if max_timestamp != batch.max_timestamp {
                debug!(
                    ?topition,
                    base_offset,
                    header = batch.max_timestamp,
                    records = max_timestamp,
                    "time index backfill: header disagrees with its records"
                );

                headers_disagreeing += 1;
                _ = first_disagreeing_offset.get_or_insert(base_offset);
            }

            if settled {
                candidate.append(
                    max_timestamp,
                    base_offset,
                    u64::try_from(batch.batch_length).unwrap_or_default(),
                );
            } else {
                candidate.observe(max_timestamp);
            }

            next_offset = Some(base_offset + i64::from(batch.last_offset_delta) + 1);
        }

        // A gap after the last listed batch has no later batch to settle it,
        // so it is in flight until a later batch is written and ages. A
        // partition whose newest produce failed in between therefore lists
        // again on each lookup until its next produce. An empty listing is
        // taken as complete, so that a partition without batch objects (a
        // lake sink's) does not list again on every lookup; the one case
        // this misses is a partition's first produce from a binary without
        // the index, still in flight during this listing.
        if batches > 0 && next_offset.is_some_and(|expected| expected < backfill.high) {
            settled = false;
        }

        // A broker rewrites each header from its records at produce, so only
        // a batch that an older broker stored can disagree.
        if let Some(first_disagreeing_offset) = first_disagreeing_offset {
            warn!(
                ?topition,
                batches,
                headers_disagreeing,
                first_disagreeing_offset,
                "time index backfill: taking each batch's maximum timestamp from its records, not \
                 from a header that disagrees with them, as an older broker stored it"
            );
        }

        if settled {
            candidate.mark_complete();
        }

        Ok((candidate, batches))
    }

    /// Returns the greatest timestamp that a consumer reads from `batch`
    /// (see [`inflated::Batch::max_record_timestamp`]), for the time index.
    ///
    /// A batch that a broker stored before it rewrote `max_timestamp` from
    /// the records carries the header that its client wrote, which can be
    /// below a record (or -1) or far above every record. An index built from
    /// such a header answers no match for a record that exists, or gives no
    /// entry to the batches after it. Kafka's index reads the header, which
    /// `LogValidator` has already rewritten from the records, so the values
    /// here are the ones that Kafka's index holds ([`LogSegment.java`]).
    ///
    /// The function returns the header for a batch that does not decode,
    /// instead of failing the backfill, so that one corrupt batch does not
    /// fail every lookup on its partition. A scan that reaches the batch
    /// still answers an error.
    ///
    /// The decode runs on the blocking pool, as a produce's does, because a
    /// backfill decodes every batch of its partition.
    ///
    /// [`LogSegment.java`]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java#L487-L495
    async fn max_record_timestamp(
        topition: &Topition,
        base_offset: i64,
        batch: &deflated::Batch,
    ) -> Result<i64> {
        let decoded = {
            let batch = batch.clone();
            offload(move || {
                Ok(inflated::Batch::try_from(&batch)
                    .map(|inflated| inflated.max_record_timestamp()))
            })
        }
        .await?;

        Ok(decoded
            .inspect_err(|error| {
                warn!(
                    ?error,
                    ?topition,
                    base_offset,
                    max_timestamp = batch.max_timestamp,
                    "time index backfill: taking the header's maximum timestamp of a batch that \
                     does not decode"
                )
            })
            .unwrap_or(batch.max_timestamp))
    }

    /// Commits a backfill `candidate` (from
    /// [`DynoStore::collect_time_index_candidate`]) via a CAS write that
    /// merges it with whatever is live at write time (see
    /// [`TimeIndex::merge`]). Returns the merged index actually persisted,
    /// which is complete unless the candidate was incomplete or the
    /// backfill's token is gone.
    async fn commit_time_index_backfill(
        &self,
        topition: &Topition,
        candidate: &TimeIndex,
        token: u64,
    ) -> Result<TimeIndex> {
        let watermark = self.watermark_for(topition)?;

        watermark
            .with_mut(&self.object_store, |w| {
                w.time_index = TimeIndex::merge(candidate.clone(), &w.time_index, token);
                Ok(w.time_index.clone())
            })
            .await
    }

    /// Returns this partition's time index and log start (`low`) when the
    /// index is complete, and `None` when a backfill must run first.
    ///
    /// A partition without a watermark document has no batches, so its
    /// empty index is complete. This read does not create the document,
    /// because a read must not create state for a partition.
    async fn complete_time_index(&self, topition: &Topition) -> Result<Option<(TimeIndex, i64)>> {
        self.watermark_for(topition)?
            .with(&self.object_store, |w| {
                Ok((w.time_index.is_complete() || w.high.is_none())
                    .then(|| (w.time_index.clone(), w.low.unwrap_or(0))))
            })
            .await
    }

    /// Returns this partition's time index and log start (`low`) for a
    /// lookup, running a backfill first if the index isn't complete yet (see
    /// [`TimeIndex::is_complete`]).
    ///
    /// The backfill can leave the index incomplete, when its listing found a
    /// batch in flight or when a binary without the index rewrote the
    /// watermark document meanwhile (see
    /// [`DynoStore::collect_time_index_candidate`] and [`TimeIndex::merge`]).
    /// Every entry of that index still keeps the index's guarantee, and its
    /// maximum timestamp covers every listed batch, so the lookup answers
    /// from it, and the next lookup runs the backfill again.
    async fn time_index_for_lookup(&self, topition: &Topition) -> Result<(TimeIndex, i64)> {
        if let Some(found) = self.complete_time_index(topition).await? {
            return Ok(found);
        }

        let gate = self.backfill_gate_for(topition)?;
        let _backfill = gate.lock().await;

        // Another request in this process can complete the backfill while
        // this one waits for the gate.
        if let Some(found) = self.complete_time_index(topition).await? {
            return Ok(found);
        }

        let started = Instant::now();
        let backfill = self.begin_time_index_backfill(topition).await?;
        let (candidate, batches) = self
            .collect_time_index_candidate(topition, &backfill)
            .await?;
        let merged = self
            .commit_time_index_backfill(topition, &candidate, backfill.token)
            .await?;

        info!(
            ?topition,
            batches,
            entries = merged.entries().len(),
            complete = merged.is_complete(),
            elapsed = ?started.elapsed(),
            "time index backfill: committed"
        );

        // The log start is read after the commit, not taken from the read
        // before the backfill, so that a lookup never filters with a log
        // start older than its index. Nothing moves the log start today;
        // DeleteRecords will.
        let low = self
            .watermark_for(topition)?
            .with(&self.object_store, |w| Ok(w.low.unwrap_or(0)))
            .await?;

        Ok((merged, low))
    }

    /// Forward sequential scan of real batch objects in `topition`, starting
    /// at `start_offset`, for the first logical record, one whose offset is
    /// `>= low` (so a record `DeleteRecords` has logically removed is never
    /// answered as a match, even if its batch object is still there) and
    /// whose timestamp is `>= target`. The scan decodes the records of each
    /// batch, because the answer is the first record at or after the target
    /// and a batch header's `max_timestamp` cannot place it inside the
    /// batch.
    ///
    /// Returns `Ok(None)` - not an error - when nothing qualifies: an empty
    /// partition, a `start_offset` past every real batch (the lake-sink
    /// produce path never writes a `.batch` object, see `DynoStore::produce`),
    /// or every surviving record's timestamp below `target`. Returns an
    /// error when a batch cannot be read, because the skipped batch can hold
    /// the match.
    async fn sequential_timestamp_scan(
        &self,
        topition: &Topition,
        start_offset: i64,
        target: i64,
        low: i64,
    ) -> Result<Option<(i64, i64)>> {
        let mut batches = self.batch_stream(topition, start_offset);

        while let Some((base_offset, _)) = batches.try_next().await? {
            let Some(deflated) = self.read_batch(topition, base_offset).await? else {
                continue;
            };

            let inflated = inflated::Batch::try_from(&deflated)
                .inspect_err(|error| error!(?error, ?topition, base_offset, "inflating batch"))
                .map_err(|_| Error::Api(ErrorCode::KafkaStorageError))?;

            for record in &inflated.records {
                let offset = base_offset + i64::from(record.offset_delta);
                if offset < low {
                    continue;
                }

                let timestamp = inflated.record_timestamp(record);
                if timestamp >= target {
                    return Ok(Some((offset, timestamp)));
                }
            }
        }

        Ok(None)
    }

    /// Answers `ListOffsets(Timestamp)` for one partition: the first record
    /// at or after `target`, below `last_stable` when the lookup is under
    /// `READ_COMMITTED`, or no match. Runs the time index backfill first when the index is
    /// incomplete (see [`DynoStore::time_index_for_lookup`]).
    async fn timestamp_lookup(
        &self,
        topition: &Topition,
        target: i64,
        last_stable: Option<i64>,
    ) -> Result<ListOffsetResponse> {
        let (time_index, low) = self.time_index_for_lookup(topition).await?;

        // No record is at or after a target above the greatest timestamp of
        // every batch, so there is no scan to run.
        if target > time_index.max_timestamp() {
            return Ok(ListOffsetResponse {
                error_code: ErrorCode::None,
                offset: None,
                ..Default::default()
            });
        }

        let start_offset = time_index.floor_offset(target);

        // Under READ_COMMITTED, Kafka answers only a record before the last stable offset, and
        // answers no match otherwise ([Partition.scala#L1589]).
        //
        // [Partition.scala#L1589]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/cluster/Partition.scala#L1589
        let found = self
            .sequential_timestamp_scan(topition, start_offset, target, low)
            .await?
            .filter(|(offset, _)| last_stable.is_none_or(|last_stable| *offset < last_stable));

        Ok(match found {
            Some((offset, timestamp)) => ListOffsetResponse {
                error_code: ErrorCode::None,
                offset: Some(offset),
                timestamp: Some(to_system_time(timestamp)?),
            },
            None => ListOffsetResponse {
                error_code: ErrorCode::None,
                offset: None,
                ..Default::default()
            },
        })
    }

    async fn get<V>(&self, location: &Path) -> Result<(V, Version)>
    where
        V: DeserializeOwned,
    {
        let get_result = self.object_store.get(location).await?;
        let meta = get_result.meta.clone();

        let payload = get_result
            .bytes()
            .await
            .map_err(Into::into)
            .and_then(|encoded| serde_json::from_reader(&encoded[..]).map_err(Error::from))?;

        Ok((payload, meta.into()))
    }

    async fn put<V>(
        &self,
        location: &Path,
        value: V,
        attributes: Attributes,
        update_version: Option<UpdateVersion>,
    ) -> Result<PutResult, UpdateError<V>>
    where
        V: PartialEq + Serialize + DeserializeOwned + Debug,
    {
        debug!(%location, ?attributes, ?update_version, ?value);

        let options = PutOptions {
            mode: update_version.map_or(PutMode::Create, PutMode::Update),
            attributes,
            ..Default::default()
        };

        let payload = serde_json::to_vec(&value)
            .map(Bytes::from)
            .map(PutPayload::from)?;

        match self
            .object_store
            .put_opts(location, payload, options)
            .await
            .inspect_err(|error| debug!(%location, ?error))
        {
            Ok(put_result) => Ok(put_result),

            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::AlreadyExists { .. }) => {
                let (current, version) = self
                    .get(location)
                    .await
                    .inspect_err(|error| error!(%location, ?error))?;

                debug!(%location, ?value, ?current);

                Err(UpdateError::Outdated {
                    current: Box::new(current),
                    version,
                })
            }

            Err(otherwise) => Err(otherwise.into()),
        }
    }

    fn txn_offset_commit_response_error(
        offsets: &TxnOffsetCommitRequest,
        error_code: ErrorCode,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        let mut responses = vec![];

        for topic in &offsets.topics {
            let mut partition_responses = vec![];

            if let Some(partitions) = topic.partitions.as_deref() {
                for partition in partitions {
                    partition_responses.push(
                        TxnOffsetCommitResponsePartition::default()
                            .partition_index(partition.partition_index)
                            .error_code(error_code.into()),
                    );
                }
            }

            responses.push(
                TxnOffsetCommitResponseTopic::default()
                    .name(topic.name.to_string())
                    .partitions(Some(partition_responses)),
            );
        }

        Ok(responses)
    }

    /// Checks `batch` against the producer's stored sequence for `topition`, and changes
    /// nothing.
    ///
    /// Returns [`SequenceCheck::Duplicate`] with the stored base offset for a retry of a
    /// remembered batch, and an error for a batch out of order or from a fenced producer. A
    /// produce runs this check before it takes offsets, and advances the sequence with
    /// [`DynoStore::advance_idempotent_sequence`] only after it stores the batch. A failed write
    /// then leaves the sequence where it was, so the client's retry is stored, instead of
    /// answered as a duplicate of a batch that was never stored.
    async fn check_idempotent_sequence(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: &IdempotentBatch,
        transactional: bool,
    ) -> Result<SequenceCheck> {
        let check = self
            .meta
            .with(&self.object_store, |meta| {
                let Some(pd) = meta.producers.get(&batch.producer_id) else {
                    debug!(producer_id = batch.producer_id, ?meta.producers);
                    return Err(Error::Api(ErrorCode::UnknownProducerId));
                };

                let Some(stored) = pd.stored_epochs(
                    transactional || meta.transactional_id_owns(batch.producer_id),
                    topition,
                ) else {
                    debug!(producer_id = batch.producer_id, "no epoch");
                    return Err(Error::Api(ErrorCode::UnknownServerError));
                };

                let (sequence, remembered_batches) = match stored
                    .check_batch(batch.producer_epoch, batch.base_sequence)
                    .inspect_err(|error| {
                        debug!(?error, ?stored, producer_epoch = batch.producer_epoch)
                    })
                    .map_err(Error::Api)?
                {
                    ProduceEpoch::Continue => (
                        pd.next_sequence(batch.producer_epoch, topition),
                        pd.remembered_batches(batch.producer_epoch, topition),
                    ),

                    ProduceEpoch::Start => (0, None),
                };

                produced_batches::check_sequence(
                    sequence,
                    remembered_batches,
                    batch.base_sequence,
                    batch.last_offset_delta,
                )
                .inspect_err(|err| {
                    // A client treats this answer as fatal, and stops the producer.
                    if matches!(err, Error::Api(ErrorCode::OutOfOrderSequenceNumber)) {
                        warn!(
                            producer_id = batch.producer_id,
                            producer_epoch = batch.producer_epoch,
                            ?topition,
                            next_sequence = sequence,
                            base_sequence = batch.base_sequence,
                            last_offset_delta = batch.last_offset_delta,
                            ?remembered_batches,
                            "idempotent batch out of order: it does not start at the next \
                             sequence and does not repeat a recent batch"
                        );
                    } else {
                        debug!(?sequence, base_sequence = batch.base_sequence);
                    }
                })
            })
            .await
            .inspect(|check| {
                IDEMPOTENT_SEQUENCE_CHECK.add(
                    1,
                    &[
                        KeyValue::new("cluster", self.cluster.clone()),
                        KeyValue::new("outcome", check.outcome()),
                    ],
                );
            })
            .inspect_err(|err| {
                if let Some(outcome) = produced_batches::rejected_outcome(err) {
                    IDEMPOTENT_SEQUENCE_CHECK.add(
                        1,
                        &[
                            KeyValue::new("cluster", self.cluster.clone()),
                            KeyValue::new("outcome", outcome),
                        ],
                    );
                } else {
                    error!(?err, transaction_id, ?topition);
                }
            })?;

        if let SequenceCheck::Duplicate { base_offset } = check {
            debug!(
                base_offset,
                producer_id = batch.producer_id,
                base_sequence = batch.base_sequence,
                transaction_id,
                ?topition
            );
        }

        Ok(check)
    }

    /// Advances the producer's stored sequence for `topition` past `batch`, which a produce
    /// has just stored at `offset`.
    ///
    /// This checks the sequence again, instead of only applying it, because the check before
    /// the write and this advance are not atomic: a concurrent request for the same producer,
    /// epoch and partition can advance the sequence between them. `location` is the batch's
    /// object, when the produce writes one, and appears only in the log.
    ///
    /// Known residual: if this advance fails (a transient object store error, or a shutdown
    /// that aborts the request after the write), the batch is stored but the sequence does not
    /// advance. The client's retry then passes the check and stores the batch a second time
    /// at a new offset, also without a concurrent request. Closing this needs a dedupe against
    /// the partition's tail batch, as Kafka does by rebuilding producer state from the log,
    /// tracked as a follow-up.
    async fn advance_idempotent_sequence(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        batch: IdempotentBatch,
        transactional: bool,
        offset: i64,
        location: Option<&Path>,
    ) -> Result<()> {
        let IdempotentBatch {
            producer_id,
            producer_epoch,
            base_sequence,
            last_offset_delta,
        } = batch;

        self.meta
            .with_mut(&self.object_store, |meta| {
                let transactional = transactional || meta.transactional_id_owns(producer_id);

                let Some(pd) = meta.producers.get_mut(&producer_id) else {
                    debug!(producer_id, ?meta.producers);
                    return Err(Error::Api(ErrorCode::UnknownProducerId));
                };

                let Some(stored) = pd.stored_epochs(transactional, topition) else {
                    debug!(producer_id, "no epoch");
                    return Err(Error::Api(ErrorCode::UnknownServerError));
                };

                // with_mut stores each change that this closure makes, also when it
                // then answers an error, so every check runs before any change.
                let produce_epoch = match stored.check_batch(producer_epoch, base_sequence) {
                    Ok(produce_epoch) => produce_epoch,

                    // A batch that the producer's next epoch superseded after it got its
                    // offset is in the log ahead of that epoch's batches, and Kafka
                    // acknowledges such a batch too. Its sequence belongs to the older epoch,
                    // so nothing advances. This compares offsets instead of relying on the
                    // watermark fence, because the fence does not cover a producer that the
                    // watermark has forgotten.
                    Err(error)
                        if stored.is_superseded(producer_epoch)
                            && pd.is_ahead_of_next_epoch(producer_epoch, topition, offset) =>
                    {
                        info!(
                            ?error,
                            ?stored,
                            producer_id,
                            producer_epoch,
                            offset,
                            ?topition,
                            "acknowledged a batch that a newer epoch superseded after it was \
                             given its offset"
                        );
                        return Ok(());
                    }

                    Err(error) => {
                        debug!(?error, ?stored, producer_epoch);
                        return Err(Error::Api(error));
                    }
                };

                let (sequence, remembered_batches) = match produce_epoch {
                    ProduceEpoch::Continue => (
                        pd.next_sequence(producer_epoch, topition),
                        pd.remembered_batches(producer_epoch, topition),
                    ),

                    ProduceEpoch::Start => (0, None),
                };

                // A concurrent retry of the same batch can be stored and
                // advance the sequence first. This write is then a second
                // copy, and the client gets an error for it instead of
                // the duplicate's offset.
                if let SequenceCheck::Duplicate { .. } = produced_batches::check_sequence(
                    sequence,
                    remembered_batches,
                    base_sequence,
                    last_offset_delta,
                )
                .inspect_err(|_| debug!(sequence, base_sequence))?
                {
                    debug!(sequence, base_sequence);
                    return Err(Error::Api(ErrorCode::DuplicateSequenceNumber));
                }

                debug!(sequence, delta = last_offset_delta + 1);
                pd.advance(
                    producer_epoch,
                    produce_epoch,
                    topition,
                    base_sequence,
                    last_offset_delta,
                    offset,
                );

                Ok(())
            })
            .await
            .inspect(|outcome| debug!(transaction_id, ?topition, ?outcome))
            .inspect_err(|err| {
                // The batch is already stored, so unlike the check before the
                // write any failure here is not harmless: the client is told
                // the batch failed while it is stored. That covers a rejected
                // sequence, a fenced producer (the check before the write
                // passed on stale metadata, and the watermark fence did not
                // hold the producer) and a transient object store error
                // (the retry then stores the batch a second time).
                POST_WRITE_ADVANCE_FAILED.add(
                    1,
                    &[
                        KeyValue::new("cluster", self.cluster.clone()),
                        KeyValue::new("reason", post_write_failure_reason(err)),
                    ],
                );

                error!(
                    ?err,
                    transaction_id,
                    ?topition,
                    offset,
                    ?location,
                    producer_id,
                    base_sequence,
                );
            })
    }
}

#[async_trait]
impl Storage for DynoStore {
    async fn register_broker(&self, _broker_registration: BrokerRegistrationRequest) -> Result<()> {
        Ok(())
    }

    async fn incremental_alter_resource(
        &self,
        resource: AlterConfigsResource,
    ) -> Result<AlterConfigsResourceResponse> {
        let _ = resource;

        match ConfigResource::from(resource.resource_type) {
            ConfigResource::Group => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::ClientMetric => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::BrokerLogger => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::Broker => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
            ConfigResource::Topic => self
                .meta
                .with_mut(&self.object_store, |meta| {
                    meta.alter_topic(
                        resource.resource_name.as_str(),
                        resource.configs.as_deref().unwrap_or_default(),
                    )
                })
                .await
                .map(|()| {
                    AlterConfigsResourceResponse::default()
                        .error_code(ErrorCode::None.into())
                        .error_message(Some("".into()))
                        .resource_type(resource.resource_type)
                        .resource_name(resource.resource_name)
                }),
            ConfigResource::Unknown => Ok(AlterConfigsResourceResponse::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some("".into()))
                .resource_type(resource.resource_type)
                .resource_name(resource.resource_name)),
        }
    }

    #[instrument(skip_all, fields(topic = %topic.name))]
    async fn create_topic(&self, topic: CreatableTopic, _validate_only: bool) -> Result<Uuid> {
        match self
            .meta
            .with_mut(&self.object_store, |meta| {
                if meta.topics.contains_key(topic.name.as_str()) {
                    return Err(Error::Api(ErrorCode::TopicAlreadyExists));
                }

                let id = Uuid::now_v7();
                debug!(%id);

                let td = TopicMetadata {
                    id,
                    topic: topic.clone(),
                };

                assert_eq!(None, meta.topics.insert(topic.name.clone(), td));
                Ok(id)
            })
            .await
        {
            Ok(id) => {
                for partition in 0..topic.num_partitions {
                    let topition = Topition::new(topic.name.as_str(), partition);

                    let watermark = self.watermark_for(&topition)?;

                    watermark
                        .with_mut(&self.object_store, |watermark| {
                            _ = watermark.high.take();
                            _ = watermark.low.take();
                            watermark.time_index = TimeIndex::complete();

                            Ok(())
                        })
                        .await?;
                }

                Ok(id)
            }

            error @ Err(_) => error,
        }
    }

    async fn delete_records(
        &self,
        topics: &[DeleteRecordsTopic],
    ) -> Result<Vec<DeleteRecordsTopicResult>> {
        debug!(?topics);

        let mut responses = vec![];

        for topic in topics {
            let mut partition_responses = vec![];

            if let Some(ref partitions) = topic.partitions {
                let metadata = self
                    .topic_metadata(&TopicId::Name(topic.name.clone()))
                    .await?;

                for partition in partitions {
                    let (error_code, low_watermark) = match &metadata {
                        None => (
                            ErrorCode::UnknownTopicOrPartition,
                            DELETE_RECORDS_INVALID_LOW_WATERMARK,
                        ),

                        Some(metadata)
                            if partition.partition_index < 0
                                || partition.partition_index >= metadata.topic.num_partitions =>
                        {
                            (
                                ErrorCode::UnknownTopicOrPartition,
                                DELETE_RECORDS_INVALID_LOW_WATERMARK,
                            )
                        }

                        Some(_) => {
                            let topition =
                                Topition::new(topic.name.as_str(), partition.partition_index);

                            let watermark = self.watermarks.lock().map(|mut locked| {
                                locked
                                    .entry(topition.to_owned())
                                    .or_insert_with(|| {
                                        OptiCon::<Watermark>::new(self.cluster.as_str(), &topition)
                                    })
                                    .to_owned()
                            })?;

                            // Resolve and validate the requested offset against the
                            // *current* watermark, and commit it atomically: `with_mut`
                            // re-runs this closure against the latest value on a
                            // concurrent-write conflict (e.g. a produce racing this
                            // delete), so the cutoff is always computed from the value
                            // actually being persisted.
                            let (error_code, cutoff, high_watermark) = watermark
                                .with_mut(&self.object_store, |w| {
                                    let stage = OffsetStage {
                                        last_stable: w.high.unwrap_or(0),
                                        high_watermark: w.high.unwrap_or(0),
                                        log_start: w.low.unwrap_or(0),
                                    };

                                    match delete_records_cutoff(partition.offset, &stage) {
                                        Err(error_code) => Ok((
                                            error_code,
                                            DELETE_RECORDS_INVALID_LOW_WATERMARK,
                                            stage.high_watermark,
                                        )),

                                        Ok(cutoff) => {
                                            w.low = Some(cutoff);
                                            Ok((ErrorCode::None, cutoff, stage.high_watermark))
                                        }
                                    }
                                })
                                .await?;

                            if error_code == ErrorCode::None {
                                // Best-effort physical cleanup of batches now fully
                                // below the new low watermark. Failing to reclaim
                                // storage here does not undo the watermark advance
                                // above: the logical delete has already taken effect,
                                // same as Kafka's own segment reclaim lagging behind
                                // a logStartOffset bump.
                                self.delete_records_batches(&topition, cutoff, high_watermark)
                                    .await;
                            }

                            (error_code, cutoff)
                        }
                    };

                    partition_responses.push(
                        DeleteRecordsPartitionResult::default()
                            .partition_index(partition.partition_index)
                            .low_watermark(low_watermark)
                            .error_code(error_code.into()),
                    );
                }
            }

            responses.push(
                DeleteRecordsTopicResult::default()
                    .name(topic.name.clone())
                    .partitions(Some(partition_responses)),
            );
        }

        Ok(responses)
    }

    async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
        if let Some(metadata) = self.topic_metadata(topic).await? {
            // Metadata is removed *last*, only once both cleanups below
            // have actually succeeded. Storage paths are keyed by topic
            // name, not a UUID, so if metadata were removed first (as it
            // used to be) and a later step then failed, a retry would find
            // no metadata, report `UnknownTopicOrPartition` without
            // retrying anything, and leave the partition's record objects
            // orphaned at that name-keyed path forever - permanently
            // blocking any topic later recreated with the same name
            // (`PutMode::Create` colliding with the orphan). Leaving
            // metadata in place until cleanup is confirmed done means a
            // retry after a transient failure here finds the topic again
            // and finishes the job; `delete_stream` over an
            // already-emptied prefix is a no-op, so retrying the whole
            // sequence is safe.

            // Both sweeps below build a key prefix from the name. A name
            // with "/" or an empty name can only predate CreateTopics' name
            // check. A sweep whose prefix can reach another topic's objects
            // is skipped, and the objects stay.
            //
            // The resolved name is checked, not the request's, because a
            // delete by id carries no name until `topic_metadata` resolves it.
            let name = metadata.topic.name.as_str();

            // The data sweep deletes everything under the topic's key
            // segment, which holds the whole name, "/" included. The empty
            // name drops out of the key, so its prefix is every topic's data.
            let data_prefix_is_shared = name.is_empty();

            // The offsets sweep matches whole segments after `offsets/`, so a
            // name with no empty segment does not reach the keys of the plain
            // name it starts with; only a name that `Path::from` collapses
            // onto another ("a/", "/a", "a//b", "") is skipped.
            let offsets_prefix_is_shared = name.split('/').any(str::is_empty);

            if data_prefix_is_shared {
                warn!(
                    name,
                    topic_id = %metadata.id,
                    location = %topic_prefix(&self.cluster, name),
                    "topic deleted, but its data was left in place: \
                     its name is not a safe key prefix"
                );
            } else {
                if name.contains('/') {
                    warn!(
                        name,
                        topic_id = %metadata.id,
                        location = %Path::from(format!(
                            "clusters/{}/topics/{}/partitions",
                            self.cluster, name
                        )),
                        "topic deleted, but objects under the keys its name \
                         had before it was stored as one segment were left in place"
                    );
                }

                let prefix = topic_prefix(&self.cluster, name);

                let locations = self
                    .object_store
                    .list(Some(&prefix))
                    .map_ok(|m| m.location)
                    .boxed();

                _ = self
                    .object_store
                    .delete_stream(locations)
                    .try_collect::<Vec<Path>>()
                    .await?;
            }

            if offsets_prefix_is_shared {
                warn!(
                    name,
                    topic_id = %metadata.id,
                    location = format!(
                        "clusters/{}/groups/consumers/<group>/{}",
                        self.cluster,
                        Path::from(format!("offsets/{}/partitions", name))
                    ),
                    "topic deleted, but its committed offsets were left in place: \
                     its name is not a safe key prefix"
                );
            } else {
                let prefix = self.group_consumers_prefix();

                let topic_name = metadata.topic.name.clone();
                let prefix_clone = prefix.clone();
                let locations = self
                    .object_store
                    .list(Some(&prefix))
                    .filter_map(move |m| {
                        let prefix = prefix_clone.clone();
                        let topic_name = topic_name.clone();
                        async move {
                            m.map_or(None, |m| {
                                debug!(?m.location);

                                m.location.prefix_match(&prefix).and_then(|mut i| {
                                    // skip over the consumer group name
                                    _ = i.next();

                                    let sub = Path::from_iter(i);
                                    debug!(?sub);

                                    if sub.prefix_matches(&Path::from(format!(
                                        "offsets/{}/partitions/",
                                        topic_name
                                    ))) {
                                        Some(Ok(m.location.clone()))
                                    } else {
                                        None
                                    }
                                })
                            })
                        }
                    })
                    .boxed();

                _ = self
                    .object_store
                    .delete_stream(locations)
                    .try_collect::<Vec<Path>>()
                    .await?;
            }

            self.meta
                .with_mut(&self.object_store, |meta| {
                    _ = meta.topics.remove(metadata.topic.name.as_str());
                    Ok(())
                })
                .await?;

            // The broker keeps a cached watermark for each partition that it
            // reads, so it drops the topic's watermarks here, and creating
            // and deleting topics through it does not grow its cache. This
            // cleanup is per broker: with several brokers on one object
            // store, another broker that read the topic keeps its entries
            // until it restarts. That broker never reads those entries again,
            // because Fetch, ListOffsets and Produce check the partition
            // against the metadata before they read its watermark.
            self.watermarks
                .lock()?
                .retain(|topition, _| topition.topic != metadata.topic.name);

            Ok(ErrorCode::None)
        } else {
            Ok(ErrorCode::UnknownTopicOrPartition)
        }
    }

    async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
        let broker_id = self.node;
        let host = self
            .advertised_listener
            .host_str()
            .unwrap_or("0.0.0.0")
            .into();
        let port = self.advertised_listener.port().unwrap_or(9092).into();
        let rack = None;

        Ok(vec![
            DescribeClusterBroker::default()
                .broker_id(broker_id)
                .host(host)
                .port(port)
                .rack(rack),
        ])
    }

    /// Appends `deflated` to `topition`, and returns the base offset of the batch.
    ///
    /// The produce takes its offsets in the watermark document of the
    /// partition, before the writes that follow: the transaction metadata,
    /// the lake table and the batch object. If one of those writes
    /// fails, the client gets an error and the taken offsets hold no batch.
    /// The produce does not move the high watermark back, because a rollback
    /// is another conditional write that can fail in the same way, and a
    /// concurrent produce can already hold the next offset. A fetch skips the
    /// gap, and the time index backfill settles it as a failed produce after
    /// `BATCH_SETTLED_AFTER`. `ListOffsets` reports a high watermark past the
    /// gap.
    ///
    /// Kafka does not use an offset for an append that it rejects. After it
    /// appends to the log, Kafka does not move the log end offset back, also
    /// when the transaction index write that follows fails
    /// ([`UnifiedLog.append`]). This engine takes its offsets earlier, at the
    /// watermark write, so a failed batch write leaves a gap that Kafka does
    /// not have.
    ///
    /// [`UnifiedLog.append`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/log/UnifiedLog.scala#L905-L912
    async fn produce(
        &self,
        transaction_id: Option<&str>,
        topition: &Topition,
        deflated: deflated::Batch,
    ) -> Result<i64> {
        // Checked before any key is built, so a produce to a topic or
        // partition that does not exist writes nothing, as in Kafka.
        let metadata = self.ensure_partition_exists(topition).await?;

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

        let config = metadata.describe_configs_result(topition.topic());

        if self.lake.is_some()
            && config
                .configs
                .as_ref()
                .map(|configs| {
                    topic_config::lookup(
                        configs
                            .iter()
                            .inspect(|config| debug!(?config))
                            .filter_map(|config| {
                                Some((config.name.as_str(), config.value.as_deref()?))
                            }),
                        topic_config::LAKE_SINK,
                    )
                    .and_then(|value| bool::from_str(value).ok())
                    .unwrap_or(false)
                })
                .unwrap_or(false)
        {
            // Keep in step with the check on the non-lake path below.
            self.ensure_partition_in_transaction(transaction_id, topition, &deflated)
                .await?;

            // Decode and validate before the high watermark moves, so that a
            // batch rejected here leaves no state behind.
            let inflated = if let Some(ref registry) = self.schemas {
                let batch_attribute = BatchAttribute::try_from(deflated.attributes)
                    .inspect(|batch_attribute| debug!(?batch_attribute))
                    .inspect_err(|err| debug!(?err))?;

                if batch_attribute.control {
                    None
                } else {
                    let inflated = {
                        let deflated = deflated.clone();
                        offload(move || inflate_produced(&deflated))
                    }
                    .await
                    .inspect(|inflated| debug!(?inflated))
                    .inspect_err(|err| debug!(?err))?;

                    registry
                        .validate(topition.topic(), &inflated)
                        .await
                        .inspect(|validation| debug!(?validation))
                        .inspect_err(|err| debug!(?err))?;

                    Some(inflated)
                }
            } else {
                None
            };

            let transactional = transaction_id.is_some() || deflated.is_transactional();
            let idempotent = IdempotentBatch::of(&deflated);

            // A client retries an idempotent batch that got no answer, so this branch checks
            // the sequence as the other branch does. Without the check, the retry takes a new
            // offset range and stores its rows in the lake a second time.
            if let Some(ref idempotent) = idempotent
                && let SequenceCheck::Duplicate { base_offset } = self
                    .check_idempotent_sequence(transaction_id, topition, idempotent, transactional)
                    .await?
            {
                return Ok(base_offset);
            }

            // Get watermark to calculate proper offset for lake sink
            let watermark = self.watermark_for(topition)?;

            let offset = watermark
                .with_mut(&self.object_store, |watermark| {
                    debug!(?watermark);

                    // with_mut stores each change that this closure makes, also when it then
                    // answers an error, so the fence runs before any change.
                    if let Some(ref idempotent) = idempotent {
                        watermark.fence_producer_epoch(
                            idempotent.producer_id,
                            idempotent.producer_epoch,
                        )?;
                    }

                    let offset = watermark.high.unwrap_or_default();
                    watermark.high = watermark.high.map_or_else(
                        || Some(deflated.last_offset_delta as i64 + 1i64),
                        |high| Some(high + deflated.last_offset_delta as i64 + 1i64),
                    );

                    if let Some(ref idempotent) = idempotent {
                        watermark.record_producer_epoch(
                            idempotent.producer_id,
                            idempotent.producer_epoch,
                        );
                    }

                    // Deliberately not touched: this branch (`nisshi.lake.sink`)
                    // never writes a `records/*.batch` object, so there is
                    // nothing for a `time_index` entry here to point at. A
                    // sequential scan starting from such an entry would list
                    // nothing and must handle that cleanly (see
                    // `DynoStore::sequential_timestamp_scan`), but the
                    // simpler fix is to just never create the entry.

                    debug!(?watermark);

                    Ok(offset)
                })
                .await
                .inspect(|offset| debug!(offset, transaction_id, ?topition))
                .inspect_err(|err| error!(?err, transaction_id, ?topition))?;

            if let Some(ref lake) = self.lake
                && let Some(ref inflated) = inflated
            {
                lake.store(
                    topition.topic(),
                    topition.partition(),
                    offset,
                    inflated,
                    config,
                )
                .await
                .inspect(|store| debug!(?store))
                .inspect_err(|err| debug!(?err))?;
            }

            if let Some(idempotent) = idempotent {
                self.advance_idempotent_sequence(
                    transaction_id,
                    topition,
                    idempotent,
                    transactional,
                    offset,
                    None,
                )
                .await?;
            }

            Ok(offset)
        } else {
            let attributes =
                BatchAttribute::try_from(deflated.attributes).inspect_err(|err| debug!(?err))?;

            // Decode and validate before the producer's sequence or the high
            // watermark moves, so that a batch rejected here leaves no state
            // behind. A producer can reuse a rejected batch's sequence for its
            // next batch. If the stored sequence had moved, that batch would
            // get DUPLICATE_SEQUENCE_NUMBER, which a client reports as success
            // although nothing was stored.
            let inflated = if !attributes.control && (self.schemas.is_some() || self.lake.is_some())
            {
                let inflated = {
                    let deflated = deflated.clone();
                    offload(move || inflate_produced(&deflated))
                }
                .await
                .inspect_err(|err| debug!(?err))?;

                if let Some(ref registry) = self.schemas {
                    registry
                        .validate(topition.topic(), &inflated)
                        .await
                        .inspect_err(|err| debug!(?err))?;
                }

                // Only the lake reads the decoded batch after this point.
                // Dropping it here, rather than at the end of the function,
                // frees its memory before the object store writes.
                self.lake.is_some().then_some(inflated)
            } else {
                None
            };

            let transactional = transaction_id.is_some() || deflated.is_transactional();
            let idempotent = IdempotentBatch::of(&deflated);

            if let Some(ref idempotent) = idempotent
                && let SequenceCheck::Duplicate { base_offset } = self
                    .check_idempotent_sequence(transaction_id, topition, idempotent, transactional)
                    .await?
            {
                return Ok(base_offset);
            }

            self.ensure_partition_in_transaction(transaction_id, topition, &deflated)
                .await?;

            if let Some(transaction_id) = transaction_id
                && attributes.transaction
            {
                // This costs a `meta.json` write on the transaction's first batch to the
                // partition, and a cached read on each later batch. Recording the bound in
                // AddPartitionsToTxn instead would ride on that call's `meta.json` write, but
                // the end of the transaction writes a marker to each partition with a recorded
                // range, so an added partition that gets no batch would get a marker too.
                self.record_transaction_start(transaction_id, deflated.producer_epoch, topition)
                    .await?;
            }

            let watermark = self.watermark_for(topition)?;

            let offset = watermark
                .with_mut(&self.object_store, |watermark| {
                    debug!(?watermark);

                    // with_mut stores each change that this closure makes, also when it then
                    // answers an error, so the fence runs before any change. A refused batch
                    // still costs one get and one put of an unchanged watermark.
                    if let Some(ref idempotent) = idempotent {
                        watermark.fence_producer_epoch(
                            idempotent.producer_id,
                            idempotent.producer_epoch,
                        )?;
                    }

                    let offset = watermark.high.unwrap_or_default();
                    let is_new_partition = watermark.high.is_none();

                    watermark.high = watermark.high.map_or_else(
                        || Some(deflated.last_offset_delta as i64 + 1i64),
                        |high| Some(high + deflated.last_offset_delta as i64 + 1i64),
                    );

                    if is_new_partition {
                        watermark.time_index.mark_complete();
                    }

                    // The index is appended to whether or not it is complete.
                    // A produce that waited for a concurrent backfill would
                    // leave a gap: the backfill reads the index before this
                    // entry exists, and merges nothing in for it.
                    watermark.time_index.append(
                        deflated.max_timestamp,
                        offset,
                        u64::try_from(deflated.batch_length).unwrap_or_default(),
                    );

                    // The epoch is recorded before the record write. When that write fails,
                    // the partition refuses the older epoch although it stores nothing at the
                    // newer one. The client has already moved to the newer epoch, so it does
                    // not send the older one again.
                    if let Some(ref idempotent) = idempotent {
                        watermark.record_producer_epoch(
                            idempotent.producer_id,
                            idempotent.producer_epoch,
                        );
                    }

                    debug!(?watermark);

                    Ok(offset)
                })
                .await
                .inspect(|offset| debug!(offset, transaction_id, ?topition))
                .inspect_err(|err| error!(?err, transaction_id, ?topition))?;

            if let Some(transaction_id) = transaction_id
                && attributes.transaction
            {
                self.meta
                    .with_mut(&self.object_store, |meta| {
                        // The check before the write is a read that can go stale. This one
                        // runs on the version of `meta.json` that the write below replaces,
                        // so it decides, and the bookkeeping never adds a partition itself.
                        //
                        // A refusal here leaves the offset reserved above empty. The lake
                        // write runs after this update, so a refused batch does not reach a
                        // lake table.
                        if !deflated.is_control() {
                            meta.ensure_partition_in_transaction(
                                transaction_id,
                                deflated.producer_id,
                                deflated.producer_epoch,
                                topition,
                            )?;
                        }

                        if let Some(transaction) = meta.transactions.get_mut(transaction_id) {
                            debug!(?transaction);

                            if let Some(txn_detail) =
                                transaction.epochs.get_mut(&deflated.producer_epoch)
                            {
                                debug!(?txn_detail);

                                let offset_end = offset + deflated.last_offset_delta as i64;

                                _ = txn_detail
                                    .produces
                                    .get_mut(&topition.topic)
                                    .and_then(|partitions| partitions.get_mut(&topition.partition))
                                    .map(|entry| {
                                        let range = entry.get_or_insert(TxnProduceOffset {
                                            offset_start: offset,
                                            offset_end,
                                        });

                                        // The start only moves down (a defensive
                                        // guard: the high watermark never falls, so
                                        // the recorded bound is normally at or below
                                        // every attempt's offset). It is not raised
                                        // from the lower bound that
                                        // `record_transaction_start` wrote: an attempt
                                        // can hold a lower offset that it has not
                                        // recorded yet. A concurrent retry of a batch
                                        // can raise the high watermark after the first
                                        // attempt did, and record its range first. The
                                        // first attempt's records still go in the log
                                        // (it then answers DUPLICATE_SEQUENCE_NUMBER),
                                        // and raising the start to the retry's offset
                                        // would let the last stable offset pass them.
                                        if offset < range.offset_start {
                                            range.offset_start = offset;
                                        }

                                        if offset_end > range.offset_end {
                                            range.offset_end = offset_end;
                                        }
                                    });
                            }
                        }

                        Ok(())
                    })
                    .await
                    .inspect(|outcome| debug!(?outcome, transaction_id, ?topition))
                    .inspect_err(|err| error!(?err, transaction_id, ?topition))?;
            }

            if let Some(ref lake) = self.lake
                && let Some(ref inflated) = inflated
            {
                lake.store(
                    topition.topic(),
                    topition.partition(),
                    offset,
                    inflated,
                    config,
                )
                .await
                .inspect(|store| debug!(?store))
                .inspect_err(|err| debug!(?err))?;
            }

            let location = batch_path(&self.cluster, topition, offset);

            let payload = self.encode(deflated).inspect_err(|err| debug!(?err))?;

            _ = self
                .object_store
                .put_opts(
                    &location,
                    payload,
                    PutOptions {
                        mode: PutMode::Create,
                        attributes: Attributes::new(),
                        ..Default::default()
                    },
                )
                .await
                .inspect(|outcome| debug!(?outcome, transaction_id, ?topition))
                .inspect_err(|error| error!(?error, transaction_id, ?topition))?;

            if let Some(idempotent) = idempotent {
                self.advance_idempotent_sequence(
                    transaction_id,
                    topition,
                    idempotent,
                    transactional,
                    offset,
                    Some(&location),
                )
                .await?;
            }

            Ok(offset)
        }
    }

    async fn fetch(
        &self,
        topition: &'_ Topition,
        offset: i64,
        _min_bytes: u32,
        max_bytes: u32,
        isolation_level: IsolationLevel,
        max_wait: Duration,
    ) -> Result<Vec<deflated::Batch>> {
        let deadline = Instant::now().checked_add(max_wait);

        let high_watermark = self.offset_stage(topition).await.map(|offset_stage| {
            if isolation_level == IsolationLevel::ReadCommitted {
                offset_stage.last_stable
            } else {
                offset_stage.high_watermark
            }
        })?;

        debug!(high_watermark);

        if offset >= high_watermark {
            return Ok(vec![]);
        }

        let bounds = FetchBounds {
            offset,
            max_bytes,
            high_watermark,
            deadline,
        };

        // A batch key zero-pads its base offset, so key order is offset
        // order. A listing that starts near `offset` finds the batch that
        // holds it only on a store that lists keys in ascending key order.
        // This fetch takes the listing only when it is a contiguous run of
        // batches from that batch, which a store that lists out of key order
        // cannot give without listing in key order. Otherwise the fetch lists
        // the whole partition and sorts it, at a cost that grows with the
        // partition (#684).
        if let Some(base_offsets) = self.batch_stream_holding(topition, offset).await?
            && let Some(batches) = self
                .read_fetched_batches(topition, bounds, base_offsets, true)
                .await?
        {
            return Ok(batches);
        }

        FETCH_FULL_LISTING.add(1, &[KeyValue::new("cluster", self.cluster.clone())]);

        if FETCH_FULL_LISTING_WARNED.swap(true, Ordering::Relaxed) {
            debug!(?topition, offset, "fetch lists the whole partition");
        } else {
            warn!(
                ?topition,
                offset,
                "fetch lists the whole partition, because the listing near the fetch offset was \
                 out of key order or had a gap; an object store that does not list keys in \
                 ascending order makes every fetch do this, at a cost that grows with the \
                 partition; later occurrences log at debug and count in nisshi_fetch_full_listing"
            );
        }

        self.fetch_from_full_listing(topition, bounds).await
    }

    async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
        // The watermark first, then the open transactions (see
        // `DynoStore::watermark_offsets`).
        let (high_watermark, log_start) = self.watermark_offsets(topition).await?;
        let open_transaction_starts = self.open_transaction_starts().await?;

        Ok(offset_stage_of(
            high_watermark,
            log_start,
            open_transaction_starts.get(topition).copied(),
        ))
    }

    async fn list_offsets(
        &self,
        isolation_level: IsolationLevel,
        offsets: &[(Topition, ListOffset)],
    ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
        // Every watermark of the request is read before the open transactions
        // (see `DynoStore::watermark_offsets`). Each partition gets either its
        // high watermark and log start, or the answer for a failed lookup.
        let mut watermarks = Vec::with_capacity(offsets.len());

        for (topition, _) in offsets {
            // Kafka answers a partition that does not exist in the response,
            // with no offset ([KafkaApis.scala#L1231-L1239]). The check
            // comes before every lookup, because each one caches the
            // partition's watermark.
            //
            // [KafkaApis.scala#L1231-L1239]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1231-L1239
            match self.ensure_partition_exists(topition).await {
                Err(Error::Api(ErrorCode::UnknownTopicOrPartition)) => {
                    watermarks.push(Err(ListOffsetResponse {
                        error_code: ErrorCode::UnknownTopicOrPartition,
                        offset: None,
                        timestamp: None,
                    }));

                    continue;
                }
                checked => _ = checked?,
            }

            // A store failure answers `KAFKA_STORAGE_ERROR` in its own
            // partition, which a client retries, so the other partitions of
            // the request still get their answers.
            watermarks.push(self.watermark_offsets(topition).await.map_err(|error| {
                error!(?error, ?topition, "list offsets");

                ListOffsetResponse {
                    error_code: match error {
                        Error::Api(error_code) => error_code,
                        _ => ErrorCode::KafkaStorageError,
                    },
                    offset: None,
                    timestamp: None,
                }
            }));
        }

        // Only READ_COMMITTED reads a last stable offset, so only it reads the
        // open transactions, once for the whole request.
        let open_transaction_starts = if isolation_level == IsolationLevel::ReadCommitted {
            self.open_transaction_starts().await?
        } else {
            BTreeMap::new()
        };

        let mut responses = vec![];

        for ((topition, offset_request), watermark) in offsets.iter().zip(watermarks) {
            let stage = match watermark {
                Ok((high_watermark, log_start)) => offset_stage_of(
                    high_watermark,
                    log_start,
                    open_transaction_starts.get(topition).copied(),
                ),

                Err(response) => {
                    responses.push((topition.to_owned(), response));
                    continue;
                }
            };

            // Kafka answers `Earliest` with the log start offset, and
            // `Latest` with the last stable offset under READ_COMMITTED and
            // the high watermark otherwise. It reads no record for either
            // ([Partition.scala#L1589-L1593], [Partition.scala#L1617-L1619],
            // [UnifiedLog.scala#L1325-L1336]).
            //
            // [Partition.scala#L1589-L1593]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/cluster/Partition.scala#L1589-L1593
            // [Partition.scala#L1617-L1619]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/cluster/Partition.scala#L1617-L1619
            // [UnifiedLog.scala#L1325-L1336]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/log/UnifiedLog.scala#L1325-L1336
            let offset = match offset_request {
                ListOffset::Earliest => stage.log_start,

                ListOffset::Latest if isolation_level == IsolationLevel::ReadCommitted => {
                    stage.last_stable
                }

                // A produce raises `watermark.high` before it writes its
                // batch object, so this can count a batch that is not
                // readable yet, unlike Kafka's high watermark. Fetch reads
                // the same `watermark.high`, so the two answers agree.
                ListOffset::Latest => stage.high_watermark,

                ListOffset::Timestamp(system_time) => {
                    let target = to_timestamp(system_time)?;

                    // A lookup that fails answers the error in its own
                    // partition, as Kafka does, so the other partitions of
                    // the request still get their answers.
                    let response = self
                        .timestamp_lookup(
                            topition,
                            target,
                            (isolation_level == IsolationLevel::ReadCommitted)
                                .then_some(stage.last_stable),
                        )
                        .await
                        .unwrap_or_else(|error| {
                            error!(?error, ?topition, target, "list offsets by timestamp");

                            ListOffsetResponse {
                                error_code: match error {
                                    Error::Api(error_code) => error_code,
                                    _ => ErrorCode::KafkaStorageError,
                                },
                                offset: None,
                                ..Default::default()
                            }
                        });

                    responses.push((topition.to_owned(), response));
                    continue;
                }
            };

            responses.push((
                topition.to_owned(),
                ListOffsetResponse {
                    error_code: ErrorCode::None,
                    offset: Some(offset),
                    timestamp: None,
                },
            ));
        }

        Ok(responses)
    }

    async fn offset_commit(
        &self,
        group_id: &str,
        _retention_time_ms: Option<Duration>,
        offsets: &[(Topition, OffsetCommitRequest)],
    ) -> Result<Vec<(Topition, ErrorCode)>> {
        let mut responses = vec![];

        for (topition, offset_commit) in offsets {
            // Kafka answers `UNKNOWN_TOPIC_OR_PARTITION` for each partition
            // of an unknown topic, and for each index that a known topic does
            // not have, and commits the other partitions of the request
            // ([KafkaApis.scala#L476-L491]).
            //
            // [KafkaApis.scala#L476-L491]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L476-L491
            match self.ensure_partition_exists(topition).await {
                Err(Error::Api(ErrorCode::UnknownTopicOrPartition)) => {
                    responses.push((topition.to_owned(), ErrorCode::UnknownTopicOrPartition));
                    continue;
                }
                checked => _ = checked?,
            }

            let location = self.committed_offset_location(group_id, topition)?;

            let payload = serde_json::to_vec(&offset_commit)
                .map(Bytes::from)
                .map(PutPayload::from)?;

            let options = PutOptions {
                mode: PutMode::Overwrite,
                attributes: json_content_type(),
                ..Default::default()
            };

            let error_code = self
                .object_store
                .put_opts(&location, payload, options)
                .await
                .inspect_err(|err| error!(?err))
                .inspect(|outcome| debug!(?outcome))
                .map_or(ErrorCode::UnknownServerError, |_| ErrorCode::None);

            responses.push((topition.to_owned(), error_code));
        }

        Ok(responses)
    }

    async fn committed_offset_topitions(
        &self,
        group_id: &str,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        let mut topitions = vec![];

        {
            let location = self.group_offsets_prefix(group_id)?;

            let mut list_stream = self.object_store.list(Some(&location));

            while let Some(meta) = list_stream
                .next()
                .await
                .inspect(|meta| debug!(?meta))
                .transpose()
                .inspect_err(|error| error!(?error))?
            {
                debug!(?meta);

                // Below the offsets prefix: `{topic}/partitions/{partition:0>10}.json`.
                let Some(mut parts) = meta.location.prefix_match(&location) else {
                    continue;
                };

                let Some(topic): Option<String> = parts
                    .next()
                    .inspect(|topic| debug!(?topic))
                    .map(|topic| topic.as_ref().into())
                else {
                    continue;
                };

                let Some(partition) = parts
                    .nth(1)
                    .inspect(|partition| debug!(?partition))
                    .map(|partition| {
                        partition
                            .as_ref()
                            .get(0..10)
                            .and_then(|partition| i32::from_str(partition).ok())
                            .ok_or(Error::Api(ErrorCode::UnknownServerError))
                    })
                    .transpose()
                    .inspect_err(|error| error!(?error, ?meta))?
                else {
                    continue;
                };

                debug!(topic, partition);

                topitions.push(Topition::new(topic, partition));
            }
        }

        self.offset_fetch(Some(group_id), topitions.as_ref(), Some(false))
            .await
    }

    async fn offset_fetch(
        &self,
        group_id: Option<&str>,
        topics: &[Topition],
        _require_stable: Option<bool>,
    ) -> Result<BTreeMap<Topition, CommittedOffset>> {
        let mut responses = BTreeMap::new();

        if let Some(group_id) = group_id {
            for topition in topics {
                let location = self.committed_offset_location(group_id, topition)?;

                // An object store failure may clear on a retry, so it stays a
                // plain error. An offset that doesn't decode won't, so it is
                // UNKNOWN_SERVER_ERROR.
                let committed = match self.object_store.get(&location).await {
                    Ok(get_result) => {
                        let encoded = get_result
                            .bytes()
                            .await
                            .inspect_err(|error| error!(?error, ?group_id, ?topition))?;

                        serde_json::from_slice::<OffsetCommitRequest>(&encoded[..])
                            .map(CommittedOffset::from)
                            .inspect_err(|error| error!(?error, ?group_id, ?topition))
                            .map_err(|_| Error::Api(ErrorCode::UnknownServerError))
                    }

                    Err(object_store::Error::NotFound { .. }) => Ok(CommittedOffset::uncommitted()),

                    Err(error) => {
                        error!(?error, ?group_id, ?topition);
                        Err(Error::from(error))
                    }
                }?;

                _ = responses.insert(topition.to_owned(), committed);
            }
        }

        Ok(responses)
    }

    /// A `TxnOffsetCommit` holds its offsets in the detail of the producer's current epoch,
    /// and the completed end of the transaction clears them. A new epoch leaves the earlier
    /// epoch's detail in place, so an earlier epoch counts only while its end is prepared
    /// and waits behind an older open transaction.
    async fn pending_txn_offsets(&self, group_id: &str) -> Result<BTreeSet<Topition>> {
        self.meta
            .with(&self.object_store, |meta| {
                Ok(meta
                    .transactions
                    .values()
                    .flat_map(|txn| {
                        let current = txn.epochs.last_key_value().map(|(epoch, _)| *epoch);

                        txn.epochs.iter().filter_map(move |(epoch, txn_detail)| {
                            (Some(*epoch) == current
                                || txn_detail.state.is_some_and(|state| state.is_prepared()))
                            .then_some(txn_detail)
                        })
                    })
                    .filter_map(|txn_detail| txn_detail.offsets.get(group_id))
                    .flat_map(|topics| {
                        topics.iter().flat_map(|(topic, partitions)| {
                            partitions
                                .keys()
                                .map(|partition| Topition::new(topic.to_owned(), *partition))
                        })
                    })
                    .collect())
            })
            .await
            .inspect(|pending| debug!(group_id, ?pending))
            .inspect_err(|error| error!(?error, group_id))
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

        let responses = match topics {
            Some(topics) if !topics.is_empty() => {
                let mut responses = vec![];

                for topic in topics {
                    let response = match self
                        .topic_metadata(topic)
                        .await
                        .inspect_err(|error| error!(?error))
                    {
                        Ok(Some(topic_metadata)) => {
                            let name = Some(topic_metadata.topic.name.to_owned());
                            let error_code = ErrorCode::None.into();
                            let topic_id = Some(topic_metadata.id.into_bytes());
                            let is_internal = Some(false);
                            let partitions = topic_metadata.topic.num_partitions;
                            let replication_factor = topic_metadata.topic.replication_factor;

                            debug!(
                                ?error_code,
                                ?topic_id,
                                ?name,
                                ?is_internal,
                                ?partitions,
                                ?replication_factor
                            );

                            let mut rng = rng();
                            let mut broker_ids: Vec<_> =
                                brokers.iter().map(|broker| broker.node_id).collect();
                            broker_ids.shuffle(&mut rng);

                            let partitions = Some(nisshi_storage::assign_replicas(
                                &broker_ids,
                                partitions,
                                replication_factor,
                                error_code,
                                0,
                            )?);

                            MetadataResponseTopic::default()
                                .error_code(error_code)
                                .name(name)
                                .topic_id(topic_id)
                                .is_internal(is_internal)
                                .partitions(partitions)
                                .topic_authorized_operations(Some(-2147483648))
                        }

                        Ok(None) => MetadataResponseTopic::default()
                            .error_code(ErrorCode::UnknownTopicOrPartition.into())
                            .name(match topic {
                                TopicId::Name(name) => Some(name.into()),
                                TopicId::Id(_) => None,
                            })
                            .topic_id(Some(match topic {
                                TopicId::Name(_) => NULL_TOPIC_ID,
                                TopicId::Id(id) => id.into_bytes(),
                            }))
                            .is_internal(Some(false))
                            .partitions(Some([].into()))
                            .topic_authorized_operations(Some(-2147483648)),

                        Err(_) => MetadataResponseTopic::default()
                            .error_code(ErrorCode::UnknownServerError.into())
                            .name(match topic {
                                TopicId::Name(name) => Some(name.into()),
                                TopicId::Id(_) => Some("".into()),
                            })
                            .topic_id(Some(match topic {
                                TopicId::Name(_) => NULL_TOPIC_ID,
                                TopicId::Id(id) => id.into_bytes(),
                            }))
                            .is_internal(Some(false))
                            .partitions(Some([].into()))
                            .topic_authorized_operations(Some(-2147483648)),
                    };

                    responses.push(response);
                }

                responses
            }

            _ => {
                self.meta
                    .with(&self.object_store, |meta| {
                        let mut responses = vec![];

                        for (name, topic_metadata) in meta.topics.iter() {
                            debug!(?name, ?topic_metadata);

                            let name = Some(name.to_owned());
                            let error_code = ErrorCode::None.into();
                            let topic_id = Some(topic_metadata.id.into_bytes());
                            let is_internal = Some(false);
                            let partitions = topic_metadata.topic.num_partitions;
                            let replication_factor = topic_metadata.topic.replication_factor;

                            debug!(
                                ?error_code,
                                ?topic_id,
                                ?name,
                                ?is_internal,
                                ?partitions,
                                ?replication_factor
                            );

                            let mut rng = rng();
                            let mut broker_ids: Vec<_> =
                                brokers.iter().map(|broker| broker.node_id).collect();
                            broker_ids.shuffle(&mut rng);

                            let partitions = Some(nisshi_storage::assign_replicas(
                                &broker_ids,
                                partitions,
                                replication_factor,
                                error_code,
                                0,
                            )?);

                            responses.push(
                                MetadataResponseTopic::default()
                                    .error_code(error_code)
                                    .name(name)
                                    .topic_id(topic_id)
                                    .is_internal(is_internal)
                                    .partitions(partitions)
                                    .topic_authorized_operations(Some(-2147483648)),
                            );
                        }
                        Ok(responses)
                    })
                    .await?
            }
        };

        Ok(MetadataResponse {
            cluster: Some(self.cluster.clone()),
            controller: Some(self.node),
            brokers,
            topics: responses,
        })
    }

    async fn describe_config(
        &self,
        name: &str,
        resource: ConfigResource,
        _keys: Option<&[String]>,
    ) -> Result<DescribeConfigsResult> {
        match resource {
            ConfigResource::Topic => match self.topic_metadata(&TopicId::Name(name.into())).await {
                Ok(Some(topic_metadata)) => Ok(topic_metadata.describe_configs_result(name)),

                Ok(None) => Ok(DescribeConfigsResult::default()
                    .error_code(ErrorCode::UnknownTopicOrPartition.into())
                    .error_message(Some(ErrorCode::UnknownTopicOrPartition.to_string()))
                    .resource_type(i8::from(resource))
                    .resource_name(name.into())
                    .configs(Some(vec![]))),

                Err(error) => Err(metadata_read_error(error)),
            },

            _ => Ok(DescribeConfigsResult::default()
                .error_code(ErrorCode::None.into())
                .error_message(Some(ErrorCode::None.to_string()))
                .resource_type(i8::from(resource))
                .resource_name(name.into())
                .configs(Some(vec![]))),
        }
    }

    async fn describe_topic_partitions(
        &self,
        topics: Option<&[TopicId]>,
        partition_limit: i32,
        cursor: Option<Topition>,
    ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
        let _ = (partition_limit, cursor);

        let mut responses =
            Vec::with_capacity(topics.map(|topics| topics.len()).unwrap_or_default());

        for topic in topics.unwrap_or_default() {
            match self
                .topic_metadata(topic)
                .await
                .inspect_err(|error| error!(?error))
            {
                Ok(Some(topic_metadata)) => responses.push(
                    DescribeTopicPartitionsResponseTopic::default()
                        .error_code(ErrorCode::None.into())
                        .name(Some(topic_metadata.topic.name))
                        .topic_id(topic_metadata.id.into_bytes())
                        .is_internal(false)
                        .partitions(Some(
                            (0..topic_metadata.topic.num_partitions)
                                .map(|partition_index| {
                                    DescribeTopicPartitionsResponsePartition::default()
                                        .error_code(ErrorCode::None.into())
                                        .partition_index(partition_index)
                                        .leader_id(self.node)
                                        .leader_epoch(0)
                                        .replica_nodes(Some(nisshi_storage::single_node_replicas(
                                            self.node,
                                            topic_metadata.topic.replication_factor,
                                        )))
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
                ),

                Ok(None) => responses.push(
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
                ),

                Err(_) => responses.push(
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
                ),
            }
        }

        Ok(responses)
    }

    async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
        let location = self.group_consumers_prefix();

        // `list_with_delimiter` returns a group's committed offsets as a common prefix and its
        // state as a `.json` object. A group with no committed offset has only the state object.
        let list_result = self
            .object_store
            .list_with_delimiter(Some(&location))
            .await
            .inspect(|list_result| debug!(?list_result))
            .inspect_err(|error| error!(?error, cluster = self.cluster))?;

        // A group has a state object once a member joins it, and an offsets folder once it
        // commits an offset. A group can have either one without the other.
        let groups_with_state = list_result
            .objects
            .iter()
            .filter_map(|object| {
                object
                    .location
                    .filename()
                    .and_then(|filename| filename.strip_suffix(".json"))
                    .and_then(decode_group_segment)
            })
            .collect::<BTreeSet<_>>();

        let groups_with_offsets_only = list_result
            .common_prefixes
            .iter()
            .filter_map(|prefix| {
                prefix
                    .parts()
                    .next_back()
                    .and_then(|segment| decode_group_segment(segment.as_ref()))
            })
            .filter(|group_id| !groups_with_state.contains(group_id))
            .collect::<BTreeSet<_>>();

        let mut listed_groups = groups_with_offsets_only
            .into_iter()
            .map(|group_id| NamedGroupDetail::found(group_id, GroupDetail::default()))
            .collect::<Vec<_>>();

        let stored_states = stream::iter(groups_with_state)
            .map(|group_id| async move {
                let stored = self.group_with_version(&group_id).await;
                (group_id, stored)
            })
            .buffered(CONCURRENT_GROUP_STATE_READS)
            .collect::<Vec<_>>()
            .await;

        listed_groups.extend(stored_states.into_iter().filter_map(
            |(group_id, stored)| match stored {
                Ok(Some((detail, _))) => Some(NamedGroupDetail::found(group_id, detail)),

                // The group was deleted after the listing, so it is left out.
                Ok(None) => None,

                // A group whose state can't be read is listed in the `Unknown` state, so that
                // one unreadable object does not hide every other group.
                Err(err) => Some(NamedGroupDetail::decoded(group_id, Some(Err(err)))),
            },
        ));

        Ok(listed_groups)
    }

    async fn delete_groups(
        &self,
        group_ids: Option<&[String]>,
    ) -> Result<Vec<DeletableGroupResult>> {
        let mut results = vec![];

        if let Some(group_ids) = group_ids {
            for group_id in group_ids {
                let location = self.group_state_location(group_id)?;

                let had_group_state = self
                    .object_store
                    .delete(&location)
                    .await
                    .inspect(|outcome| debug!(group_id, ?outcome))
                    .inspect_err(|err| error!(group_id, ?err))
                    .is_ok();

                debug!(group_id, had_group_state);

                let prefix = self
                    .group_consumers_prefix()
                    .join(group_path_part(group_id)?);

                let locations = self
                    .object_store
                    .list(Some(&prefix))
                    .map_ok(|m| m.location)
                    .boxed();

                let deleted_committed_offsets = self
                    .object_store
                    .delete_stream(locations)
                    .try_collect::<Vec<Path>>()
                    .await?;

                debug!(group_id, ?deleted_committed_offsets);

                results.push(
                    DeletableGroupResult::default()
                        .group_id(group_id.into())
                        .error_code(
                            if had_group_state || !deleted_committed_offsets.is_empty() {
                                ErrorCode::None
                            } else {
                                ErrorCode::GroupIdNotFound
                            }
                            .into(),
                        ),
                );
            }
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
                let location = self.group_state_location(group_id)?;

                match self
                    .get::<GroupDetail>(&location)
                    .await
                    .inspect(|o| debug!(?o, group_id))
                    .inspect_err(|err| error!(?err, group_id))
                {
                    Ok((group_detail, _)) => {
                        results.push(NamedGroupDetail::found(group_id.into(), group_detail));
                    }

                    // A group that commits offsets and never joins has no state object. The
                    // group exists while any of its committed offsets remains.
                    Err(Error::ObjectStore(error))
                        if matches!(error.as_ref(), object_store::Error::NotFound { .. }) =>
                    {
                        let offsets_prefix = self.group_offsets_prefix(group_id)?;

                        match self.object_store.list(Some(&offsets_prefix)).next().await {
                            Some(Ok(_)) => results.push(NamedGroupDetail::found(
                                group_id.into(),
                                GroupDetail::default(),
                            )),

                            None => results.push(NamedGroupDetail::error_code(
                                group_id.into(),
                                ErrorCode::GroupIdNotFound,
                            )),

                            Some(Err(err)) => {
                                error!(?err, group_id);
                                results.push(NamedGroupDetail::error_code(
                                    group_id.into(),
                                    ErrorCode::CoordinatorNotAvailable,
                                ));
                            }
                        }
                    }

                    // An object store error is usually transient. Kafka clients retry
                    // `COORDINATOR_NOT_AVAILABLE`, and fail on `UNKNOWN_SERVER_ERROR`.
                    Err(Error::ObjectStore(_)) => {
                        results.push(NamedGroupDetail::error_code(
                            group_id.into(),
                            ErrorCode::CoordinatorNotAvailable,
                        ));
                    }

                    // A retry does not fix a stored group that can't be decoded.
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
        let location = self.group_state_location(group_id)?;

        match self.get::<GroupDetail>(&location).await {
            Ok(stored) => Ok(Some(stored)),

            Err(Error::ObjectStore(error))
                if matches!(error.as_ref(), object_store::Error::NotFound { .. }) =>
            {
                Ok(None)
            }

            Err(otherwise) => Err(otherwise).inspect_err(|err| error!(?err, group_id)),
        }
    }

    async fn update_group(
        &self,
        group_id: &str,
        detail: GroupDetail,
        version: Option<Version>,
    ) -> Result<Version, UpdateError<GroupDetail>> {
        let location = self.group_state_location(group_id)?;

        self.put(
            &location,
            detail,
            json_content_type(),
            version.map(Into::into),
        )
        .await
        .map(Into::into)
    }

    async fn init_producer(
        &self,
        transaction_id: Option<&str>,
        transaction_timeout_ms: i32,
        producer_id: Option<i64>,
        producer_epoch: Option<i16>,
    ) -> Result<ProducerIdResponse> {
        #[derive(Clone, Debug)]
        enum InitProducer {
            Completed(ProducerIdResponse),
            NeedToRollback {
                producer_id: i64,
                producer_epoch: i16,
            },
        }

        let claim = match producer_claim(producer_id, producer_epoch) {
            Ok(claim) => claim,
            Err(error) => return Ok(ProducerIdResponse::failed(error)),
        };

        if let Some(transaction_id) = transaction_id {
            match self
                .meta
                .with_mut(&self.object_store, |meta| {
                    debug!(?meta, ?claim);
                    match meta.transactions.entry(transaction_id.to_string()) {
                        Entry::Vacant(vacant) => {
                            let Some(id) = allocate_producer_id(&mut meta.producers) else {
                                error!(transaction_id, "no producer ID left");

                                return Ok(InitProducer::Completed(ProducerIdResponse::failed(
                                    ErrorCode::UnknownServerError,
                                )));
                            };

                            _ = vacant.insert(Txn {
                                producer: id,
                                epochs: BTreeMap::from([(
                                    0,
                                    TxnDetail {
                                        transaction_timeout_ms,
                                        ..Default::default()
                                    },
                                )]),
                            });

                            Ok(InitProducer::Completed(ProducerIdResponse {
                                id,
                                epoch: 0,
                                error: ErrorCode::None,
                            }))
                        }

                        Entry::Occupied(mut occupied) => {
                            let txn = occupied.get();

                            let Some((&current_epoch, txn_detail)) = txn.epochs.last_key_value()
                            else {
                                // Unreachable: every transaction starts with epoch 0,
                                // and no code removes an epoch.
                                error!(transaction_id, "transaction has no epoch");

                                return Ok(InitProducer::Completed(ProducerIdResponse::failed(
                                    ErrorCode::UnknownServerError,
                                )));
                            };

                            if let Some(claim) = claim {
                                let error = check_claim((txn.producer, current_epoch), claim);

                                if error != ErrorCode::None {
                                    return Ok(InitProducer::Completed(
                                        ProducerIdResponse::failed(error),
                                    ));
                                }
                            }

                            if txn_detail.state == Some(TxnState::Begin) {
                                return Ok(InitProducer::NeedToRollback {
                                    producer_id: txn.producer,
                                    producer_epoch: current_epoch,
                                });
                            }

                            let previous_id = txn.producer;

                            // The producer's newest epoch is the epoch of its newest
                            // transaction, or i16::MAX once the rollback below fences it.
                            let newest_epoch = meta
                                .producers
                                .get(&previous_id)
                                .and_then(|producer| producer.sequences.last_key_value())
                                .map_or(current_epoch, |(epoch, _)| current_epoch.max(*epoch));

                            if epoch_is_exhausted(newest_epoch) {
                                // As in Kafka, only the transaction of the current epoch holds
                                // back the move: the client retries CONCURRENT_TRANSACTIONS
                                // until it finishes.
                                if txn_detail.state.is_some_and(|state| !state.is_finished()) {
                                    return Ok(InitProducer::Completed(
                                        ProducerIdResponse::failed(
                                            ErrorCode::ConcurrentTransactions,
                                        ),
                                    ));
                                }

                                let Some(id) = allocate_producer_id(&mut meta.producers) else {
                                    error!(transaction_id, "no producer ID left");

                                    return Ok(InitProducer::Completed(
                                        ProducerIdResponse::failed(ErrorCode::UnknownServerError),
                                    ));
                                };

                                // A produce compares the batch's epoch with the producer's
                                // newest, so epoch i16::MAX fences every batch of the
                                // previous producer ID. Its older epochs and sequences are
                                // never read again, so they go, instead of growing meta.json
                                // by an entry for each epoch.
                                if let Some(previous) = meta.producers.get_mut(&previous_id) {
                                    previous.sequences =
                                        BTreeMap::from([(i16::MAX, BTreeMap::new())]);
                                    previous.batches.clear();
                                }

                                info!(
                                    transaction_id,
                                    previous_id,
                                    id,
                                    "producer epoch exhausted, moved to a new producer ID"
                                );

                                _ = occupied.insert(Txn {
                                    producer: id,
                                    epochs: BTreeMap::from([(
                                        0,
                                        TxnDetail {
                                            transaction_timeout_ms,
                                            ..Default::default()
                                        },
                                    )]),
                                });

                                return Ok(InitProducer::Completed(ProducerIdResponse {
                                    id,
                                    epoch: 0,
                                    error: ErrorCode::None,
                                }));
                            }

                            // with_mut stores each change that this closure makes, also
                            // when it then answers an error, so every check runs first.
                            let Some(epoch) = current_epoch.checked_add(1).filter(|epoch| {
                                meta.producers
                                    .get(&previous_id)
                                    .is_none_or(|producer| !producer.sequences.contains_key(epoch))
                            }) else {
                                error!(transaction_id, previous_id, current_epoch, "epoch taken");

                                return Ok(InitProducer::Completed(ProducerIdResponse::failed(
                                    ErrorCode::UnknownServerError,
                                )));
                            };

                            if let Some(producer) = meta.producers.get_mut(&previous_id) {
                                _ = producer.sequences.insert(epoch, BTreeMap::new());
                            }

                            _ = occupied.get_mut().epochs.insert(
                                epoch,
                                TxnDetail {
                                    transaction_timeout_ms,
                                    ..Default::default()
                                },
                            );

                            Ok(InitProducer::Completed(ProducerIdResponse {
                                id: previous_id,
                                epoch,
                                error: ErrorCode::None,
                            }))
                        }
                    }
                })
                .await?
            {
                InitProducer::Completed(completed) => Ok(completed),
                InitProducer::NeedToRollback {
                    producer_id: rollback_producer_id,
                    producer_epoch: rollback_producer_epoch,
                } => {
                    let error_code = self
                        .txn_end(
                            transaction_id,
                            rollback_producer_id,
                            rollback_producer_epoch,
                            false,
                        )
                        .await?;

                    debug!(?rollback_producer_id, ?rollback_producer_epoch, ?error_code);

                    if error_code == ErrorCode::None {
                        // The abort can stay prepared behind an older open transaction, and
                        // the move to a new producer ID then waits for it. Kafka fences the
                        // producer at that point, so that it cannot add to the aborted
                        // transaction, and so does this: a produce compares the batch's
                        // epoch with the producer's newest.
                        if epoch_is_exhausted(rollback_producer_epoch) {
                            self.meta
                                .with_mut(&self.object_store, |meta| {
                                    if let Some(producer) =
                                        meta.producers.get_mut(&rollback_producer_id)
                                    {
                                        _ = producer.sequences.entry(i16::MAX).or_default();
                                    }

                                    Ok(())
                                })
                                .await?;
                        }

                        return self
                            .init_producer(
                                Some(transaction_id),
                                transaction_timeout_ms,
                                producer_id,
                                producer_epoch,
                            )
                            .await;
                    } else {
                        Ok(ProducerIdResponse {
                            id: -1,
                            epoch: -1,
                            error: ErrorCode::UnknownServerError,
                        })
                    }
                }
            }
        } else {
            self.meta
                .with_mut(&self.object_store, |meta| {
                    debug!(?meta);

                    let Some(producer) = allocate_producer_id(&mut meta.producers) else {
                        error!("no producer ID left");
                        return Ok(ProducerIdResponse::failed(ErrorCode::UnknownServerError));
                    };

                    Ok(ProducerIdResponse {
                        id: producer,
                        epoch: 0,
                        ..Default::default()
                    })
                })
                .await
        }
    }

    async fn txn_add_offsets(
        &self,
        _transaction_id: &str,
        _producer_id: i64,
        _producer_epoch: i16,
        _group_id: &str,
    ) -> Result<ErrorCode> {
        Ok(ErrorCode::None)
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
                self.meta
                    .with_mut(&self.object_store, |meta| {
                        let Some(transaction) = meta.transactions.get_mut(&transaction_id) else {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*partition_index)
                                            .partition_error_code(
                                                ErrorCode::TransactionalIdNotFound.into(),
                                            ),
                                    );
                                }

                                results.push(
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(results_by_partition)),
                                )
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        };

                        if transaction.producer != producer_id {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*partition_index)
                                            .partition_error_code(
                                                ErrorCode::UnknownProducerId.into(),
                                            ),
                                    );
                                }

                                results.push(
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(results_by_partition)),
                                )
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        }

                        let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*partition_index)
                                            .partition_error_code(ErrorCode::ProducerFenced.into()),
                                    );
                                }

                                results.push(
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(results_by_partition)),
                                )
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
                        };

                        if &producer_epoch != current_epoch.key() {
                            let mut results = vec![];

                            for topic in topics {
                                let mut results_by_partition = vec![];

                                for partition_index in topic.partitions.as_deref().unwrap_or(&[]) {
                                    results_by_partition.push(
                                        AddPartitionsToTxnPartitionResult::default()
                                            .partition_index(*partition_index)
                                            .partition_error_code(ErrorCode::ProducerFenced.into()),
                                    );
                                }

                                results.push(
                                    AddPartitionsToTxnTopicResult::default()
                                        .name(topic.name.clone())
                                        .results_by_partition(Some(results_by_partition)),
                                )
                            }

                            return Ok(TxnAddPartitionsResponse::VersionZeroToThree(results));
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
                                    .entry(*partition_index)
                                    .or_default();

                                results_by_partition.push(
                                    AddPartitionsToTxnPartitionResult::default()
                                        .partition_index(*partition_index)
                                        .partition_error_code(i16::from(ErrorCode::None)),
                                );
                            }

                            results.push(
                                AddPartitionsToTxnTopicResult::default()
                                    .name(topic.name.clone())
                                    .results_by_partition(Some(results_by_partition)),
                            )
                        }

                        txn_detail.started_at = Some(SystemTime::now());
                        txn_detail.state = Some(TxnState::Begin);

                        Ok(TxnAddPartitionsResponse::VersionZeroToThree(results))
                    })
                    .await
            }

            TxnAddPartitionsRequest::VersionFourPlus { .. } => {
                Err(Error::UnsupportedTxnAddPartitionsVersion)
            }
        }
    }

    async fn txn_offset_commit(
        &self,
        offsets: TxnOffsetCommitRequest,
    ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
        self.meta
            .with_mut(&self.object_store, |meta| {
                let Some(transaction) = meta.transactions.get_mut(&offsets.transaction_id) else {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::TransactionalIdNotFound,
                    );
                };

                if transaction.producer != offsets.producer_id {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::UnknownProducerId,
                    );
                }

                let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::ProducerFenced,
                    );
                };

                if &offsets.producer_epoch != current_epoch.key() {
                    return Self::txn_offset_commit_response_error(
                        &offsets,
                        ErrorCode::ProducerFenced,
                    );
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
                            .name(topic.name.to_string())
                            .partitions(Some(partition_responses)),
                    );
                }

                Ok(responses)
            })
            .await
    }

    async fn txn_end(
        &self,
        transaction_id: &str,
        producer_id: i64,
        producer_epoch: i16,
        committed: bool,
    ) -> Result<ErrorCode> {
        let produced = self
            .meta
            .with_mut(&self.object_store, |meta| {
                debug!(transactions = ?meta.transactions);

                let Some(transaction) = meta.transactions.get_mut(transaction_id) else {
                    return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
                };

                if transaction.producer != producer_id {
                    return Err(Error::Api(ErrorCode::UnknownProducerId));
                }

                let Some(mut current_epoch) = transaction.epochs.last_entry() else {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                };

                if &producer_epoch != current_epoch.key() {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                }

                let txn_detail = current_epoch.get_mut();

                // A retry must repeat the decision that was prepared:
                // markers of the opposite kind after the first would
                // commit some partitions and abort others.
                match (&txn_detail.state, committed) {
                    (Some(TxnState::PrepareCommit), false)
                    | (Some(TxnState::PrepareAbort), true) => {
                        debug!(state = ?txn_detail.state, committed);
                        return Err(Error::Api(ErrorCode::InvalidTxnState));
                    }
                    _ => {}
                }

                if txn_detail.state == Some(TxnState::Begin) {
                    assert_eq!(
                        Some(TxnState::Begin),
                        txn_detail.state.replace(if committed {
                            TxnState::PrepareCommit
                        } else {
                            TxnState::PrepareAbort
                        })
                    );
                }

                let mut produced = vec![];

                // Recomputed from whatever is still in `produces` whenever
                // we're in (freshly, or already) a `Prepare*` state, not
                // only on a fresh transition out of `Begin`: `produces`
                // isn't cleared until every marker below has been written
                // and the transaction moves past `Prepare*` (further down
                // in `txn_end`), so a retry after a partial failure here
                // (some partitions' end-of-transaction marker written,
                // others not) recomputes the same set and retries the
                // missing ones, rather than computing an empty set and
                // silently completing the transaction with some partitions
                // never marked. Retrying a partition whose marker already
                // landed just writes a second, harmless marker at a new
                // offset - strictly better than one that's missing
                // forever.
                if matches!(
                    txn_detail.state,
                    Some(TxnState::PrepareCommit) | Some(TxnState::PrepareAbort)
                ) {
                    for (topic, partitions) in &txn_detail.produces {
                        for (partition, offset_range) in partitions {
                            debug!(?topic, partition, ?offset_range);

                            if offset_range.is_some() {
                                produced.push(Topition::new(topic.to_owned(), *partition));
                            }
                        }
                    }
                }

                Ok(produced)
            })
            .await
            .inspect(|produced| debug!(?produced))
            .inspect_err(|err| error!(?err))?;

        for topition in produced {
            debug!(?topition);

            let control_batch: Bytes = if committed {
                ControlBatch::default().commit().try_into()?
            } else {
                ControlBatch::default().abort().try_into()?
            };

            let end_transaction_marker: Bytes = EndTransactionMarker::default().try_into()?;

            let batch = inflated::Batch::builder()
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
                .and_then(TryInto::try_into)
                .inspect(|deflated| debug!(?deflated))?;

            match self.produce(Some(transaction_id), &topition, batch).await {
                Ok(offset) => debug!(
                    offset,
                    ?topition,
                    producer_id,
                    producer_epoch,
                    transaction_id,
                    committed,
                ),

                // The topic was deleted while the transaction was open, so
                // there is no log left to mark. Kafka's coordinator drops
                // such a partition from the markers it writes; failing here
                // instead would fail every retry of EndTxn and hold the
                // last stable offset of the other partitions back for good.
                Err(Error::Api(ErrorCode::UnknownTopicOrPartition)) => warn!(
                    ?topition,
                    producer_id,
                    producer_epoch,
                    transaction_id,
                    committed,
                    "partition no longer exists, end transaction marker skipped"
                ),

                Err(err) => {
                    error!(
                        ?err,
                        ?topition,
                        producer_id,
                        producer_epoch,
                        transaction_id,
                        committed,
                    );

                    return Err(err);
                }
            }
        }

        let offsets_to_commit = self
            .meta
            .with_mut(&self.object_store, |meta| {
                debug!(transactions = ?meta.transactions);

                let Some(transaction) = meta.transactions.get_mut(transaction_id) else {
                    return Err(Error::Api(ErrorCode::TransactionalIdNotFound));
                };

                if transaction.producer != producer_id {
                    return Err(Error::Api(ErrorCode::UnknownProducerId));
                }

                let Some(current_epoch) = transaction.epochs.last_entry() else {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                };

                if &producer_epoch != current_epoch.key() {
                    return Err(Error::Api(ErrorCode::ProducerFenced));
                }

                let mut overlaps =
                    meta.overlapping_transactions(transaction_id, producer_id, producer_epoch)?;
                debug!(?overlaps);

                let mut offsets_to_commit: BTreeMap<
                    Group,
                    BTreeMap<Topic, BTreeMap<Partition, TxnCommitOffset>>,
                > = BTreeMap::new();

                if overlaps.iter().all(|txn_id| txn_id.state.is_prepared()) {
                    let txn_ids = {
                        overlaps.push(TxnId {
                            transaction: transaction_id.into(),
                            producer_id,
                            producer_epoch,
                            state: if committed {
                                TxnState::PrepareCommit
                            } else {
                                TxnState::PrepareAbort
                            },
                        });

                        overlaps
                    };

                    for txn_id in txn_ids {
                        debug!(?txn_id);

                        if let Some(txn) = meta.transactions.get_mut(txn_id.transaction.as_str())
                            && let Some(txn_detail) = txn.epochs.get_mut(&txn_id.producer_epoch)
                        {
                            debug!(?txn_detail);

                            match txn_detail.state {
                                None | Some(TxnState::PrepareCommit) => {
                                    _ = txn_detail.state.replace(TxnState::Committed);
                                }

                                Some(TxnState::PrepareAbort) => {
                                    _ = txn_detail.state.replace(TxnState::Aborted);
                                }

                                otherwise => {
                                    warn!(
                                        transaction = txn_id.transaction,
                                        producer = txn_id.producer_id,
                                        epoch = txn_id.producer_epoch,
                                        ?otherwise,
                                    );

                                    continue;
                                }
                            }

                            if txn_id.state == TxnState::PrepareCommit {
                                for (group, topics) in txn_detail.offsets.iter() {
                                    for (topic, partitions) in topics.iter() {
                                        for (partition, committed_offset) in partitions {
                                            _ = offsets_to_commit
                                                .entry(group.to_owned())
                                                .or_default()
                                                .entry(topic.to_owned())
                                                .or_default()
                                                .insert(*partition, committed_offset.to_owned());
                                        }
                                    }
                                }
                            }

                            txn_detail.produces.clear();
                            txn_detail.offsets.clear();
                            _ = txn_detail.started_at.take();
                        }
                    }
                }

                Ok(offsets_to_commit)
            })
            .await
            .inspect(|outcome| debug!(?outcome))
            .inspect_err(|err| error!(?err))?;

        debug!(?offsets_to_commit);

        for (group, topics) in offsets_to_commit.iter() {
            let mut offsets = vec![];

            for (topic, partitions) in topics.iter() {
                for (partition, txn_co) in partitions {
                    let tp = Topition::new(topic.to_owned(), *partition);
                    let ocr = OffsetCommitRequest {
                        offset: txn_co.committed_offset,
                        leader_epoch: txn_co.leader_epoch,
                        timestamp: None,
                        metadata: txn_co.metadata.clone(),
                    };

                    offsets.push((tp, ocr));
                }
            }

            _ = self.offset_commit(group, None, &offsets[..]).await?;
        }

        Ok(ErrorCode::None)
    }

    async fn maintain(&self, _now: SystemTime) -> Result<()> {
        if let Some(ref lake) = self.lake {
            return lake
                .maintain()
                .await
                .inspect(|maintain| debug!(?maintain))
                .inspect_err(|err| debug!(?err))
                .map_err(Into::into);
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

    async fn advertised_listener(&self) -> Result<Url> {
        Ok(self.advertised_listener.clone())
    }

    /// DynoStore keeps no SCRAM credentials, so it refuses the change instead of reporting
    /// success for a credential that it would drop.
    #[instrument(skip_all)]
    async fn delete_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<()> {
        Err(Error::UserCredentialsUnsupported {
            engine: self.engine.into(),
        })
    }

    /// See [`Self::delete_user_scram_credential`].
    #[instrument(skip_all)]
    async fn upsert_user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
        _credential: ScramCredential,
    ) -> Result<()> {
        Err(Error::UserCredentialsUnsupported {
            engine: self.engine.into(),
        })
    }

    /// No user has a credential here: [`Self::upsert_user_scram_credential`] stores none.
    async fn user_scram_credential(
        &self,
        _user: &str,
        _mechanism: ScramMechanism,
    ) -> Result<Option<ScramCredential>> {
        Ok(None)
    }

    #[instrument(skip_all)]
    async fn ping(&self) -> Result<()> {
        // A `list()` failure here means storage is unusable (no usable
        // credentials, an unreachable endpoint, a missing bucket), so we
        // return it and the broker fails to start, instead of failing later
        // on its first real request.
        //
        // We list the cluster's prefix and not the bucket root, because the
        // broker reads only under that prefix. A policy can allow
        // `s3:ListBucket` only for that prefix in a shared bucket.
        let _ = self
            .object_store
            .list(Some(&Path::from(format!("clusters/{}/", self.cluster))))
            .next()
            .await
            .transpose()?;
        Ok(())
    }
}

/// Returns the error that a client gets for a failed read of the cluster's metadata: an object
/// store error becomes [`Error::Api`] with [`ErrorCode::KafkaStorageError`], and any other error
/// stays as it is.
///
/// A failed read of the object store is usually transient. Kafka answers a failed log read with
/// `KAFKA_STORAGE_ERROR` ([`LocalLog.scala`]), and a producer retries that code, but fails on
/// `UNKNOWN_SERVER_ERROR`. A retry does not fix metadata that can't be decoded, so that error
/// keeps its type.
///
/// [`LocalLog.scala`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/log/LocalLog.scala#L709-L721
fn metadata_read_error(error: Error) -> Error {
    match error {
        Error::ObjectStore(error) => {
            error!(?error, "reading metadata");
            Error::Api(ErrorCode::KafkaStorageError)
        }

        otherwise => otherwise,
    }
}

fn object_store_error_name(error: &object_store::Error) -> &'static str {
    match error {
        object_store::Error::Precondition { .. } => "pre_condition",

        object_store::Error::AlreadyExists { .. } => "already_exists",

        object_store::Error::NotModified { .. } => "not_modified",

        object_store::Error::NotFound { .. } => "not_found",

        otherwise => {
            debug!(?otherwise);
            "otherwise"
        }
    }
}

/// The producer fields of an idempotent batch that its sequence check and advance read.
///
/// A produce copies them out of the batch, because it moves the batch into its encoding
/// before it advances the sequence.
#[derive(Clone, Copy, Debug)]
struct IdempotentBatch {
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    last_offset_delta: i32,
}

impl IdempotentBatch {
    /// Returns the producer fields of `deflated`, or `None` when it is not idempotent.
    fn of(deflated: &deflated::Batch) -> Option<Self> {
        deflated.is_idempotent().then_some(Self {
            producer_id: deflated.producer_id,
            producer_epoch: deflated.producer_epoch,
            base_sequence: deflated.base_sequence,
            last_offset_delta: deflated.last_offset_delta,
        })
    }
}

/// Counts the idempotent batches that a produce checks against the producer's sequence, by
/// `outcome`: see [`SequenceCheck::outcome`] and [`produced_batches::rejected_outcome`].
static IDEMPOTENT_SEQUENCE_CHECK: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_produce_idempotent_sequence_check")
        .with_description(
            "Idempotent produce batches checked against the producer's sequence, by outcome",
        )
        .build()
});

static POST_WRITE_ADVANCE_FAILED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_produce_post_write_advance_failed")
        .with_description(
            "Idempotent produce batches stored but reported as failed because the post-write \
             sequence advance failed, by reason",
        )
        .build()
});

/// The `reason` attribute of [`POST_WRITE_ADVANCE_FAILED`]: the Kafka error
/// code name for an API error, the [`object_store_error_name`] for an object
/// store error, and `other` for anything else, so the cardinality stays
/// bounded.
fn post_write_failure_reason(error: &Error) -> Cow<'static, str> {
    match error {
        Error::Api(code) => Cow::Owned(format!("{code:?}")),
        Error::ObjectStore(error) => Cow::Borrowed(object_store_error_name(error)),
        _ => Cow::Borrowed("other"),
    }
}

#[derive(Debug, Clone)]
struct Metron<O> {
    request_duration: Histogram<u64>,
    request_error: Counter<u64>,

    cluster: String,
    object_store: O,
}

impl<O> Display for Metron<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metron").finish()
    }
}

impl<O> Metron<O>
where
    O: ObjectStore,
{
    fn new(object_store: O, cluster: &str) -> Self {
        Self {
            cluster: cluster.into(),
            request_duration: METER
                .u64_histogram("nisshi_object_store_request_duration")
                .with_unit("ms")
                .with_description("The object store request latencies in milliseconds")
                .build(),
            request_error: METER
                .u64_counter("nisshi_object_store_request_error")
                .with_description("The object store request errors")
                .build(),

            object_store,
        }
    }
}

#[async_trait]
impl<O> ObjectStore for Metron<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        debug!(%location, ?opts);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "put_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .put_opts(location, payload, opts.clone())
            .await
            .inspect(|put_result| {
                debug!(%location, etag = ?put_result.e_tag, version = ?put_result.version);

                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                debug!(%location, opts = ?opts, err = ?err);

                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        debug!(%location, ?opts);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "put_multipart_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .put_multipart_opts(location, opts)
            .await
            .inspect(|_put_result| {
                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    #[instrument(skip_all, fields(%location), ret)]
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        debug!(?options);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "get_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .get_opts(location, options.clone())
            .await
            .inspect(|get_result| {
                debug!(meta = ?get_result.meta);

                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                debug!(?err);

                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        debug!("delete_stream");

        let cluster = self.cluster.clone();
        let request_duration = self.request_duration.clone();
        let request_error = self.request_error.clone();

        let inner = self.object_store.delete_stream(locations);

        Box::pin(inner.inspect(move |result| {
            let attributes = vec![
                KeyValue::new("method", "delete_stream"),
                KeyValue::new("cluster", cluster.clone()),
            ];

            if let Err(err) = result {
                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.extend(attributes);
                request_error.add(1, &additional[..]);
            } else {
                request_duration.record(0, attributes.as_ref());
            }
        }))
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        debug!(?prefix);

        self.object_store.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        debug!(?prefix, %offset);

        self.object_store.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        debug!(?prefix);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "list_with_delimiter"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        if let Some(prefix) = prefix {
            attributes.push(KeyValue::new("prefix", prefix.to_string()));
        }

        self.object_store
            .list_with_delimiter(prefix)
            .await
            .inspect(|_list_result| {
                debug!(?prefix);

                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                debug!(?prefix, err = ?err);

                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        debug!(%from, %to, ?opts);

        let execute_start = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("method", "copy_opts"),
            KeyValue::new("cluster", self.cluster.clone()),
        ];

        self.object_store
            .copy_opts(from, to, opts)
            .await
            .inspect(|_| {
                debug!(%from, %to);

                self.request_duration.record(
                    execute_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    attributes.as_ref(),
                )
            })
            .inspect_err(|err| {
                debug!(%from, %to, err = ?err);

                let mut additional = vec![KeyValue::new("reason", object_store_error_name(err))];
                additional.append(&mut attributes);
                self.request_error.add(1, &additional[..]);
            })
    }
}
