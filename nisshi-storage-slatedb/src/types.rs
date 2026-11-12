// Copyright ⓒ Solace Corporation
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

//! Type definitions for SlateDB storage engine
//!
//! # Key Design for LSM-tree
//!
//! All keys follow a consistent pattern optimized for LSM-tree storage:
//!
//! ```text
//! {type_prefix}/{hierarchy...}/{leaf_id}
//! ```
//!
//! ## Key Prefixes (sorted by access pattern)
//!
//! | Prefix | Description | Key Structure |
//! |--------|-------------|---------------|
//! | `b/` | Batch data | `b/{topic_uuid}/{partition:be32}/{offset:be64}` |
//! | `c/` | Consumer group commits | `c/{group}/{topic}/{partition:be32}` |
//! | `g/` | Group state | `g/{group_id}` |
//! | `u/` | SCRAM credentials | `u/{user}/{mechanism:be32}` |
//! | `w/` | Watermarks | `w/{topic_uuid}/{partition:be32}` |
//!
//! ## Design Principles
//!
//! 1. **Prefix-first**: Type prefix comes first for efficient filtering
//! 2. **Big-endian integers**: Preserves numeric ordering in lexicographic sort
//! 3. **Fixed-width encoding**: Ensures consistent key ordering
//! 4. **Hierarchical structure**: Enables efficient prefix scans
//!
//! ## LSM-tree Considerations
//!
//! - Keys with same prefix are stored together → better compaction
//! - Bloom filters can efficiently skip unrelated key types
//! - Range scans for a partition only touch relevant SSTable blocks

use std::{collections::BTreeMap, fmt, time::SystemTime};

use bytes::Bytes;
use nisshi_sans_io::{ScramMechanism, create_topics_request::CreatableTopic};
use nisshi_storage::{GroupDetail, GroupMember, GroupState, ScramCredential, TxnState, Version};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Type aliases
pub(super) type Group = String;
pub(super) type Offset = i64;
pub(super) type Partition = i32;
pub(super) type ProducerEpoch = i16;
pub(super) type ProducerId = i64;
pub(super) type Sequence = i32;
pub(super) type Topic = String;

// Collection types
pub(super) type Topics = BTreeMap<Topic, TopicMetadata>;
pub(super) type Producers = BTreeMap<ProducerId, ProducerDetail>;
pub(super) type Brokers = BTreeMap<i32, BrokerInfo>;
pub(super) type Transactions = BTreeMap<String, Txn>;

/// Transaction produce offset range
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub(super) struct TxnProduceOffset {
    pub offset_start: Offset,
    pub offset_end: Offset,
}

/// Transaction commit offset
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct TxnCommitOffset {
    pub committed_offset: Offset,
    pub leader_epoch: Option<i32>,
    pub metadata: Option<String>,
}

/// Topic metadata
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct TopicMetadata {
    pub id: Uuid,
    pub topic: CreatableTopic,
}

/// Watermark for a topic partition
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct Watermark {
    pub low: Option<i64>,
    pub high: Option<i64>,
    pub timestamps: Option<BTreeMap<i64, i64>>,
}

/// Key for watermark storage: `w/{topic_uuid}/{partition:be32}`
///
/// Watermarks are accessed per-partition, so we use topic+partition as the key.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct WatermarkKey {
    /// Type prefix for LSM-tree grouping
    pub prefix: char,
    /// Topic UUID (16 bytes, fixed)
    pub topic: Uuid,
    /// Partition number (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub partition: Partition,
}

impl Default for WatermarkKey {
    fn default() -> Self {
        Self {
            prefix: 'w',
            topic: Uuid::nil(),
            partition: 0,
        }
    }
}

impl WatermarkKey {
    pub(super) fn new(topic: Uuid, partition: Partition) -> Self {
        Self {
            prefix: 'w',
            topic,
            partition,
        }
    }
}

/// Group detail with version
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct GroupDetailVersion {
    pub detail: GroupDetail,
    pub version: Version,
}

/// A stored group in the layout from before [`GroupDetail::rebalance`] existed.
#[derive(Deserialize)]
struct PreRebalanceGroupDetailVersion {
    detail: PreRebalanceGroupDetail,
    version: Version,
}

#[derive(Deserialize)]
struct PreRebalanceGroupDetail {
    session_timeout_ms: i32,
    rebalance_timeout_ms: Option<i32>,
    members: BTreeMap<String, GroupMember>,
    generation_id: i32,
    skip_assignment: Option<bool>,
    inception: SystemTime,
    state: GroupState,
}

impl From<PreRebalanceGroupDetailVersion> for GroupDetailVersion {
    fn from(value: PreRebalanceGroupDetailVersion) -> Self {
        let PreRebalanceGroupDetail {
            session_timeout_ms,
            rebalance_timeout_ms,
            members,
            generation_id,
            skip_assignment,
            inception,
            state,
        } = value.detail;

        Self {
            detail: GroupDetail {
                session_timeout_ms,
                rebalance_timeout_ms,
                members,
                generation_id,
                skip_assignment,
                inception,
                state,
                rebalance: Box::default(),
            },
            version: value.version,
        }
    }
}

/// Decodes `encoded` as `T` only if `T` takes every byte.
fn decode_exactly<'de, T>(encoded: &'de [u8]) -> postcard::Result<T>
where
    T: Deserialize<'de>,
{
    match postcard::take_from_bytes(encoded)? {
        (decoded, []) => Ok(decoded),
        _ => Err(postcard::Error::DeserializeBadEncoding),
    }
}

impl GroupDetailVersion {
    /// Decodes a stored group, in the current layout or in the layout from before
    /// [`GroupDetail::rebalance`] existed.
    ///
    /// Postcard encodes a struct's fields by position and does not record their names, so it
    /// can't default a field that older bytes lack. It also ignores bytes left after a value. Each
    /// layout must therefore take every byte, or bytes in one layout could decode as the other.
    pub(super) fn decode(encoded: &[u8]) -> postcard::Result<Self> {
        decode_exactly::<Self>(encoded).or_else(|error| {
            decode_exactly::<PreRebalanceGroupDetailVersion>(encoded)
                .map(Self::from)
                .map_err(|_| error)
        })
    }

    pub(super) fn detail(self, detail: GroupDetail) -> Self {
        Self { detail, ..self }
    }

    pub(super) fn version(self, version: Version) -> Self {
        Self { version, ..self }
    }
}

/// Producer detail with sequence tracking
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct ProducerDetail {
    pub sequences: BTreeMap<ProducerEpoch, BTreeMap<String, BTreeMap<i32, Sequence>>>,
}

/// Transaction identifier
#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct TxnId {
    pub transaction: String,
    pub producer_id: ProducerId,
    pub producer_epoch: ProducerEpoch,
    pub state: TxnState,
}

/// Transaction state
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct Txn {
    pub producer: ProducerId,
    pub epochs: BTreeMap<ProducerEpoch, TxnDetail>,
}

/// Transaction detail
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct TxnDetail {
    pub transaction_timeout_ms: i32,
    pub started_at: Option<SystemTime>,
    pub state: Option<TxnState>,
    pub produces: BTreeMap<Topic, BTreeMap<Partition, Option<TxnProduceOffset>>>,
    pub offsets: BTreeMap<Group, BTreeMap<Topic, BTreeMap<Partition, TxnCommitOffset>>>,
}

/// Key for batch storage: `b/{topic_uuid}/{partition:be32}/{offset:be64}`
///
/// Batches are the most frequently accessed data. The key structure enables:
/// - Efficient sequential reads by offset (big-endian preserves order)
/// - Prefix scan for all batches in a partition
/// - Bloom filter can quickly skip non-batch keys
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct BatchKey {
    /// Type prefix 'b' for batch
    pub prefix: char,
    /// Topic UUID (16 bytes, fixed)
    pub topic: Uuid,
    /// Partition number (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub partition: Partition,
    /// Offset within partition (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub offset: Offset,
}

impl Default for BatchKey {
    fn default() -> Self {
        Self {
            prefix: 'b',
            topic: Uuid::nil(),
            partition: 0,
            offset: 0,
        }
    }
}

impl BatchKey {
    pub(super) fn new(topic: Uuid, partition: Partition, offset: Offset) -> Self {
        Self {
            prefix: 'b',
            topic,
            partition,
            offset,
        }
    }

    /// Create a key for range scan starting from this offset
    pub(super) fn scan_from(topic: Uuid, partition: Partition, offset: Offset) -> Self {
        Self::new(topic, partition, offset)
    }
}

/// Prefix key for scanning all batches in a topic partition: `b/{topic_uuid}/{partition}`
///
/// This is a separate struct from `BatchKey` because we need to check if a scanned key
/// still belongs to the same topic/partition before decoding the batch data.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct BatchKeyPrefix {
    /// Type prefix 'b' for batch
    pub prefix: char,
    /// Topic UUID (16 bytes, fixed)
    pub topic: Uuid,
    /// Partition number (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub partition: Partition,
}

impl BatchKeyPrefix {
    pub(super) fn new(topic: Uuid, partition: Partition) -> Self {
        Self {
            prefix: 'b',
            topic,
            partition,
        }
    }
}

/// Key for storing committed offsets: `c/{group}/{topic}/{partition:be32}`
///
/// Consumer group offsets are accessed by group, then by topic-partition.
/// This enables efficient "get all offsets for a group" scans.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct OffsetCommitKey {
    /// Type prefix 'c' for commit
    pub prefix: char,
    /// Consumer group ID
    pub group: String,
    /// Topic name
    pub topic: String,
    /// Partition number (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub partition: Partition,
}

impl Default for OffsetCommitKey {
    fn default() -> Self {
        Self {
            prefix: 'c',
            group: String::new(),
            topic: String::new(),
            partition: 0,
        }
    }
}

impl OffsetCommitKey {
    pub(super) fn new(
        group: impl Into<String>,
        topic: impl Into<String>,
        partition: Partition,
    ) -> Self {
        Self {
            prefix: 'c',
            group: group.into(),
            topic: topic.into(),
            partition,
        }
    }
}

/// Prefix key for scanning all offsets in a consumer group: `c/{group}`
///
/// This is a separate struct from `OffsetCommitKey` because postcard serialization
/// includes length prefixes for strings, so we can't use an `OffsetCommitKey` with
/// empty topic as a scan prefix - it would include the empty string's length marker.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct OffsetCommitKeyPrefix {
    /// Type prefix 'c' for commit
    pub prefix: char,
    /// Consumer group ID
    pub group: String,
}

impl OffsetCommitKeyPrefix {
    pub(super) fn new(group: impl Into<String>) -> Self {
        Self {
            prefix: 'c',
            group: group.into(),
        }
    }
}

/// Value stored for offset commits
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct OffsetCommitValue {
    pub offset: i64,
    pub leader_epoch: Option<i32>,
    pub metadata: Option<String>,
}

/// Key for storing group state: `g/{group_id}`
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct GroupKey {
    /// Type prefix 'g' for group
    pub prefix: char,
    /// Group ID
    pub group_id: String,
}

impl Default for GroupKey {
    fn default() -> Self {
        Self {
            prefix: 'g',
            group_id: String::new(),
        }
    }
}

impl GroupKey {
    pub(super) fn new(group_id: impl Into<String>) -> Self {
        Self {
            prefix: 'g',
            group_id: group_id.into(),
        }
    }
}

/// Prefix key for scanning all groups: `g`
///
/// This is a separate struct from `GroupKey` because postcard serialization
/// includes length prefixes for strings, so we can't use a `GroupKey` with
/// empty group_id as a scan prefix.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct GroupKeyPrefix {
    /// Type prefix 'g' for group
    pub prefix: char,
}

impl GroupKeyPrefix {
    pub(super) fn new() -> Self {
        Self { prefix: 'g' }
    }
}

/// Stored broker information
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct BrokerInfo {
    pub broker_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

/// Key for storing SASL/SCRAM credentials: `u/{user}/{mechanism:be32}`
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct UserScramCredentialKey {
    /// Type prefix 'u' for user credential
    pub prefix: char,
    /// SASL user name
    pub user: String,
    /// SCRAM mechanism (big-endian for correct ordering)
    #[serde(with = "postcard::fixint::be")]
    pub mechanism: i32,
}

impl UserScramCredentialKey {
    pub(super) fn new(user: impl Into<String>, mechanism: ScramMechanism) -> Self {
        Self {
            prefix: 'u',
            user: user.into(),
            mechanism: i32::from(mechanism),
        }
    }
}

/// Value stored for SASL/SCRAM credentials
///
/// `Debug` writes `stored_key` and `server_key` as `[hidden]`, as
/// [`ScramCredential`] does.
#[derive(Clone, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct StoredScramCredential {
    pub salt: Vec<u8>,
    pub iterations: i32,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
}

impl fmt::Debug for StoredScramCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoredScramCredential")
            .field("salt", &self.salt)
            .field("iterations", &self.iterations)
            .field("stored_key", &format_args!("[hidden]"))
            .field("server_key", &format_args!("[hidden]"))
            .finish()
    }
}

impl From<ScramCredential> for StoredScramCredential {
    fn from(credential: ScramCredential) -> Self {
        Self {
            salt: credential.salt.to_vec(),
            iterations: credential.iterations,
            stored_key: credential.stored_key.to_vec(),
            server_key: credential.server_key.to_vec(),
        }
    }
}

impl From<StoredScramCredential> for ScramCredential {
    fn from(stored: StoredScramCredential) -> Self {
        Self {
            salt: Bytes::from(stored.salt),
            iterations: stored.iterations,
            stored_key: Bytes::from(stored.stored_key),
            server_key: Bytes::from(stored.server_key),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, time::Duration};

    use nisshi_storage::{GroupRebalance, JoinPhase, PendingSync};

    use super::*;

    /// The layout of a stored group before `GroupDetail::rebalance` existed, for encoding.
    #[derive(Serialize)]
    struct PreRebalanceLayout<'a> {
        session_timeout_ms: i32,
        rebalance_timeout_ms: Option<i32>,
        members: &'a BTreeMap<String, GroupMember>,
        generation_id: i32,
        skip_assignment: Option<bool>,
        inception: SystemTime,
        state: &'a GroupState,
        version: &'a Version,
    }

    fn version() -> Version {
        Version {
            e_tag: Some("e-tag".into()),
            version: None,
        }
    }

    #[test]
    fn group_stored_before_the_rebalance_state_decodes_without_a_rebalance() -> postcard::Result<()>
    {
        let detail = GroupDetail {
            generation_id: 7,
            state: GroupState::Formed {
                protocol_type: "consumer".into(),
                protocol_name: "range".into(),
                leader: "leader".into(),
                assignments: BTreeMap::new(),
            },
            ..GroupDetail::default()
        };
        let version = version();

        let encoded = postcard::to_stdvec(&PreRebalanceLayout {
            session_timeout_ms: detail.session_timeout_ms,
            rebalance_timeout_ms: detail.rebalance_timeout_ms,
            members: &detail.members,
            generation_id: detail.generation_id,
            skip_assignment: detail.skip_assignment,
            inception: detail.inception,
            state: &detail.state,
            version: &version,
        })?;

        let decoded = GroupDetailVersion::decode(&encoded)?;
        assert_eq!(detail, decoded.detail);
        assert_eq!(version, decoded.version);
        Ok(())
    }

    #[test]
    fn group_with_a_rebalance_decodes_as_stored() -> postcard::Result<()> {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);

        let stored = GroupDetailVersion::default()
            .detail(GroupDetail {
                rebalance: Box::new(GroupRebalance {
                    join_phase: Some(JoinPhase {
                        deadline: now,
                        joined: BTreeSet::from(["member".to_owned()]),
                        initial_delay: None,
                    }),
                    pending_sync: Some(PendingSync {
                        deadline: now,
                        members: BTreeSet::from(["member".to_owned()]),
                    }),
                    pending_members: BTreeMap::from([("pending".to_owned(), now)]),
                }),
                ..GroupDetail::default()
            })
            .version(version());

        let encoded = postcard::to_stdvec(&stored)?;
        assert_eq!(stored, GroupDetailVersion::decode(&encoded)?);
        Ok(())
    }
}
