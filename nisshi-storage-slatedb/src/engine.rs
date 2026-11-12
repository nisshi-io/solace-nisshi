// Copyright (c) Solace Corporation
//
// Contributors outside Solace:
//   Enwei Jiao
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

//! SlateDB Engine and Builder
//!
//! # SlateDB Storage Architecture
//!
//! This engine uses SlateDB as the underlying key-value store. SlateDB is an LSM-tree
//! based embedded database built on object storage (S3, GCS, Azure Blob, etc.).
//!
//! ## Key Design
//!
//! Keys are designed with LSM-tree best practices in mind:
//!
//! ### Design Principles
//!
//! 1. **Type prefix first**: Each key type has a single-char prefix (`b/`, `c/`, `g/`, `u/`, `w/`)
//!    - Enables bloom filter to skip irrelevant SSTable blocks
//!    - Groups related data together during compaction
//!    - Allows efficient type-specific range scans
//!
//! 2. **Big-endian integers**: All numeric fields use big-endian encoding
//!    - Preserves numeric ordering in lexicographic sort
//!    - offset 1 < offset 2 < offset 256 (correct order)
//!    - Contrast with little-endian where 256 < 1 (wrong order!)
//!
//! 3. **Fixed-width encoding**: Uses postcard with `fixint::be` for consistent key sizes
//!
//! ### Global Metadata (singleton keys)
//!
//! These store aggregated metadata in single values (simple but has scaling limits):
//! - `brokers.pc.bin` → `BTreeMap<i32, BrokerInfo>`
//! - `producers.pc.bin` → `BTreeMap<ProducerId, ProducerDetail>`
//! - `topics.pc.bin` → `BTreeMap<Topic, TopicMetadata>`
//! - `transactions.pc.bin` → `BTreeMap<String, Txn>`
//!
//! ### Per-Partition Data (prefixed composite keys)
//!
//! | Prefix | Key Structure | Value | Access Pattern |
//! |--------|---------------|-------|----------------|
//! | `b` | `b/{topic:uuid}/{partition:be32}/{offset:be64}` | Batch | Sequential read by offset |
//! | `c` | `c/{group}/{topic}/{partition:be32}` | OffsetCommitValue | Per-group offset lookup |
//! | `g` | `g/{group_id}` | GroupDetailVersion | Group state management |
//! | `u` | `u/{user}/{mechanism:be32}` | StoredScramCredential | SASL/SCRAM lookup per user |
//! | `w` | `w/{topic:uuid}/{partition:be32}` | Watermark | Watermark per partition |
//!
//! ### Key Ordering Example
//!
//! ```text
//! b/<topic1>/0/0     ← First batch of partition 0
//! b/<topic1>/0/1     ← Second batch of partition 0
//! b/<topic1>/0/100   ← 101st batch (big-endian: 100 > 1, correct!)
//! b/<topic1>/1/0     ← First batch of partition 1
//! c/group1/topic1/0  ← Consumer group offset
//! g/group1           ← Group state
//! w/<topic1>/0       ← Watermark for partition 0
//! ```
//!
//! ## Known Limitations
//!
//! 1. **Global Metadata Contention**: Single-key metadata requires read-modify-write
//!    of entire map. At scale (1000s of topics), consider per-entity keys.
//!
//! 2. **No Broker Cleanup**: Brokers are never removed (no TTL/heartbeat).
//!
//! 3. **Transaction State Growth**: Completed transactions are never compacted.
//!
//! 4. **Large Partition Scans**: Very large partitions may benefit from skip-list indexing.

use std::{cmp::Ordering, fmt, sync::Arc};

use bytes::Bytes;
use nisshi_sans_io::{ErrorCode, de::BatchDecoder, record::deflated::Batch};
use nisshi_schema::{Registry, lake::House};
use nisshi_storage::{Error, Result, TopicId};
use serde::Deserialize;
use slatedb::Db;
use tracing::debug;
use url::Url;

use super::types::{GroupDetailVersion, TopicMetadata, Topics, Transactions};

/// SlateDB Storage Engine
///
/// Implements the Kafka storage interface using SlateDB as the backing store.
/// SlateDB provides ACID transactions and is built on object storage.
#[derive(Clone)]
pub(crate) struct Engine {
    pub(super) cluster: String,
    pub(super) node: i32,
    pub(super) advertised_listener: Url,
    pub(super) db: Arc<Db>,
    pub(super) schemas: Option<Registry>,
    pub(super) lake: Option<House>,
}

impl fmt::Debug for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Engine))
            .field("cluster", &self.cluster)
            .field("node", &self.node)
            .finish()
    }
}

/// Builder for SlateDB Engine
///
/// Provides a fluent API for constructing an Engine with required and optional parameters.
#[derive(Clone, Default)]
pub(crate) struct Builder {
    cluster: Option<String>,
    node: Option<i32>,
    advertised_listener: Option<Url>,
    db: Option<Arc<Db>>,
    schemas: Option<Registry>,
    lake: Option<House>,
}

impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Builder")
            .field("cluster", &self.cluster)
            .field("node", &self.node)
            .field("advertised_listener", &self.advertised_listener)
            .field("db", &self.db.as_ref().map(|_| "Arc<Db>"))
            .field("schemas", &self.schemas.as_ref().map(|_| "Registry"))
            .field("lake", &self.lake.as_ref().map(|_| "House"))
            .finish()
    }
}

impl Builder {
    pub(crate) fn cluster(mut self, cluster: impl Into<String>) -> Self {
        self.cluster = Some(cluster.into());
        self
    }

    pub(crate) fn node(mut self, node: i32) -> Self {
        self.node = Some(node);
        self
    }

    pub(crate) fn advertised_listener(mut self, advertised_listener: Url) -> Self {
        self.advertised_listener = Some(advertised_listener);
        self
    }

    pub(crate) fn db(mut self, db: Arc<Db>) -> Self {
        self.db = Some(db);
        self
    }

    pub(crate) fn schemas(mut self, schemas: Option<Registry>) -> Self {
        self.schemas = schemas;
        self
    }

    pub(crate) fn lake(mut self, lake: Option<House>) -> Self {
        self.lake = lake;
        self
    }

    /// Builds the Engine, returning an error if a required field is missing.
    pub(crate) fn build(self) -> Result<Engine> {
        Ok(Engine {
            cluster: self
                .cluster
                .ok_or_else(|| Error::Message("cluster is required".into()))?,
            node: self
                .node
                .ok_or_else(|| Error::Message("node is required".into()))?,
            advertised_listener: self
                .advertised_listener
                .ok_or_else(|| Error::Message("advertised_listener is required".into()))?,
            db: self
                .db
                .ok_or_else(|| Error::Message("db is required".into()))?,
            schemas: self.schemas,
            lake: self.lake,
        })
    }
}

impl Engine {
    // Global Metadata Keys
    //
    // The following keys store all metadata of a given type in a single value (serialized BTreeMap).
    // This is simple and works well for small to medium scale deployments.
    //
    // At large scale (1000s of topics), consider splitting into per-topic keys:
    // - Instead of a single `topics.pc.bin`, use `topics/{name}` keys.
    // - Retrieving the list of topics would be a scan of all matching keys.
    // - This would also enable SlateDB's writer partitioning feature.
    // - The main challenge is that a transaction can enlist multiple topics,
    //   which could be in different partitions.

    /// Key for storing all broker registrations.
    pub(super) const BROKERS: &[u8] = b"brokers.pc.bin";
    /// Key for storing all producer states (idempotent/transactional).
    pub(super) const PRODUCERS: &[u8] = b"producers.pc.bin";
    /// Key for storing all topic metadata.
    pub(super) const TOPICS: &[u8] = b"topics.pc.bin";
    /// Key for storing all transaction states.
    pub(super) const TRANSACTIONS: &[u8] = b"transactions.pc.bin";

    /// Create a new Engine with the simple constructor (without optional features)
    #[cfg(test)]
    pub(crate) fn new(cluster: &str, node: i32, advertised_listener: Url, db: Arc<Db>) -> Self {
        Self {
            cluster: cluster.to_string(),
            node,
            advertised_listener,
            db,
            schemas: None,
            lake: None,
        }
    }

    /// Create a builder for configuring the Engine with optional features
    pub(crate) fn builder() -> Builder {
        Builder::default()
    }

    pub(super) fn decode(&self, encoded: Bytes) -> Result<Batch> {
        let decoder = BatchDecoder::new(encoded);
        Batch::deserialize(decoder).map_err(Into::into)
    }

    pub(super) async fn load_metadata<T>(
        &self,
        tx: &slatedb::DbTransaction,
        key: &[u8],
    ) -> Result<T>
    where
        T: serde::de::DeserializeOwned + Default,
    {
        tx.get(key)
            .await
            .map_err(Error::from)
            .and_then(|bytes: Option<Bytes>| {
                bytes.map_or(Ok(T::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })
    }

    /// Returns the group stored at `key`, or the default group and version if none is.
    pub(super) async fn load_group(
        &self,
        tx: &slatedb::DbTransaction,
        key: &[u8],
    ) -> Result<GroupDetailVersion> {
        tx.get(key)
            .await
            .map_err(Error::from)
            .and_then(|bytes: Option<Bytes>| {
                bytes.map_or(Ok(GroupDetailVersion::default()), |encoded| {
                    GroupDetailVersion::decode(&encoded).map_err(Into::into)
                })
            })
    }

    pub(super) fn save_metadata<T>(
        &self,
        tx: &slatedb::DbTransaction,
        key: &[u8],
        value: &T,
    ) -> Result<()>
    where
        T: serde::Serialize,
    {
        postcard::to_stdvec(value)
            .map_err(Error::from)
            .and_then(|encoded| tx.put(key, encoded).map_err(Into::into))
    }

    pub(super) async fn get_topics(&self) -> Result<Topics> {
        self.db
            .get(Self::TOPICS)
            .await
            .map_err(Error::from)
            .and_then(|topics| {
                topics.map_or(Ok(Topics::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })
    }

    pub(super) async fn get_transactions(&self) -> Result<Transactions> {
        self.db
            .get(Self::TRANSACTIONS)
            .await
            .map_err(Error::from)
            .and_then(|txns| {
                txns.map_or(Ok(Transactions::default()), |encoded| {
                    postcard::from_bytes(&encoded[..]).map_err(Into::into)
                })
            })
    }

    pub(super) async fn topic_metadata(&self, topic_id: &TopicId) -> Result<Option<TopicMetadata>> {
        let topics = self.get_topics().await?;
        match topic_id {
            TopicId::Name(name) => Ok(topics.get(name.as_str()).cloned()),
            TopicId::Id(id) => Ok(topics.values().find(|tm| tm.id == *id).cloned()),
        }
    }

    /// Validates idempotent message sequence for exactly-once semantics.
    ///
    /// Kafka's idempotent producer uses a sequence number to detect duplicates and
    /// ensure messages are processed exactly once. This function validates that the
    /// incoming batch's sequence number is valid given the current producer state.
    ///
    /// # Arguments
    /// - `producer_epoch`: The current epoch stored for this producer
    /// - `sequence`: The expected next sequence number (what we've seen so far + 1)
    /// - `deflated`: The incoming batch with producer_id, producer_epoch, and base_sequence
    ///
    /// # Returns
    /// - `Ok(increment)`: The number to add to current sequence (last_offset_delta + 1)
    /// - `Err(DuplicateSequenceNumber)`: The batch was already processed (sequence > expected)
    /// - `Err(OutOfOrderSequenceNumber)`: A batch was skipped (sequence < expected)
    /// - `Err(ProducerFenced)`: A newer producer epoch exists (epoch mismatch, new > old)
    /// - `Err(InvalidProducerEpoch)`: The batch uses an old epoch (epoch mismatch, old > new)
    ///
    /// # Sequence Number Logic
    ///
    /// For a producer with epoch E:
    /// - First batch should have base_sequence = 0
    /// - Each subsequent batch should have base_sequence = previous.base_sequence + previous.record_count
    /// - If we receive a sequence we've already seen, it's a duplicate (retry)
    /// - If we receive a sequence ahead of expected, something was lost
    ///
    /// # Epoch Logic
    ///
    /// Producer epochs handle producer restarts:
    /// - When a producer restarts, it gets a new (higher) epoch
    /// - Messages with the old epoch should be rejected (ProducerFenced)
    /// - Messages claiming a future epoch are invalid (InvalidProducerEpoch)
    pub(super) fn idempotent_sequence_check(
        producer_epoch: &i16,
        sequence: &i32,
        deflated: &Batch,
    ) -> Result<i32> {
        match producer_epoch.cmp(&deflated.producer_epoch) {
            // Epochs match - check sequence numbers
            Ordering::Equal => match sequence.cmp(&deflated.base_sequence) {
                // Expected sequence - this is the next batch in order
                Ordering::Equal => Ok(deflated.last_offset_delta + 1),

                // We expected a lower sequence than received - this is a duplicate/retry
                // The producer already sent this and we processed it
                Ordering::Greater => {
                    debug!(
                        expected_sequence = ?sequence,
                        received_sequence = ?deflated.base_sequence,
                        "Duplicate sequence detected"
                    );
                    Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
                }

                // We expected a higher sequence - messages were skipped/lost
                Ordering::Less => {
                    debug!(
                        expected_sequence = ?sequence,
                        received_sequence = ?deflated.base_sequence,
                        "Out of order sequence detected"
                    );
                    Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
                }
            },

            // Our stored epoch is higher - a newer producer instance exists
            // This old producer should stop sending
            Ordering::Greater => Err(Error::Api(ErrorCode::ProducerFenced)),

            // Batch claims a higher epoch than we know - invalid
            Ordering::Less => Err(Error::Api(ErrorCode::InvalidProducerEpoch)),
        }
    }
}
