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

use std::cmp::min;

use nisshi_sans_io::{
    IsolationLevel,
    fetch_request::{FetchPartition, FetchTopic},
};
use tokio::time::Instant;

use super::{ByteSize as _, PartitionFetch, PartitionRead, next_partition_deadline};
use crate::{Result, fetched::ByteLimit};

/// One round of a Fetch, in which each partition of the request is read once. The round tracks
/// the time and bytes that its partitions can still use, and the totals of their answers.
///
/// Every round reads every partition again from its fetch offset, and answers with only its own
/// records. So each round starts with the whole `max_bytes`, and only its own bytes count toward
/// `min_bytes`. Kafka also counts the bytes available at each check, not a sum over the checks
/// ([DelayedFetch.scala]).
///
/// [DelayedFetch.scala]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/DelayedFetch.scala#L74-L148
#[derive(Debug)]
pub(super) struct FetchRound {
    /// When every storage read of the Fetch must finish.
    fetch_deadline: Instant,
    /// The Fetch's `min_bytes`.
    min_bytes: u32,
    /// The Fetch's isolation level.
    isolation: IsolationLevel,
    /// The partitions that the round has not answered.
    partitions_left: usize,
    /// What is left of the request's `max_bytes` after the partitions that the round answered.
    bytes_left: u32,
    /// Whether a partition of the round has returned records.
    has_records: bool,
    /// The bytes that count toward `min_bytes`.
    available_bytes: u64,
    /// Whether a partition's read failed after the partition had records.
    read_failed: bool,
}

impl FetchRound {
    /// Returns a round of the partitions of `topics` that has answered none of them, with
    /// `max_bytes` to return and until `fetch_deadline` to read. Each read of the round uses
    /// `min_bytes` and `isolation`.
    pub(super) fn new(
        topics: &[FetchTopic],
        max_bytes: u32,
        fetch_deadline: Instant,
        min_bytes: u32,
        isolation: IsolationLevel,
    ) -> Self {
        Self {
            fetch_deadline,
            min_bytes,
            isolation,
            partitions_left: topics
                .iter()
                .map(|topic| topic.partitions.as_ref().map_or(0, Vec::len))
                .sum(),
            bytes_left: max_bytes,
            has_records: false,
            available_bytes: 0,
            read_failed: false,
        }
    }

    /// Returns when the next partition's reads must finish, as [`next_partition_deadline`]
    /// describes.
    fn partition_deadline(&self) -> Instant {
        next_partition_deadline(self.fetch_deadline, self.partitions_left)
    }

    /// Counts `partitions` that the round answered without reading them, such as the partitions
    /// of an unknown topic.
    pub(super) fn skip_partitions(&mut self, partitions: usize) {
        debug_assert!(self.partitions_left >= partitions, "{self:?}");
        self.partitions_left = self.partitions_left.saturating_sub(partitions);
    }

    /// Reads the partition `request` of `topic` with `fetch_partition`, which gets the limit and
    /// deadline that the round gives the read, and counts the read toward the round.
    pub(super) async fn read_partition<'a, F, Fut>(
        &mut self,
        topic: &'a str,
        request: &'a FetchPartition,
        fetch_partition: F,
    ) -> Result<PartitionRead>
    where
        F: FnOnce(PartitionFetch<'a>) -> Fut,
        Fut: Future<Output = Result<PartitionRead>>,
    {
        let partition = PartitionFetch {
            topic,
            request,
            limit: self.limit_for(request.partition_max_bytes),
            deadline: self.partition_deadline(),
            min_bytes: self.min_bytes,
            isolation: self.isolation,
        };

        let partition_read = fetch_partition(partition).await?;
        self.finish_partition(&partition_read);
        Ok(partition_read)
    }

    /// Returns the bytes that count toward `min_bytes`: what the round's partitions returned, or
    /// their whole limit for a partition whose read stopped at it.
    pub(super) fn available_bytes(&self) -> u64 {
        self.available_bytes
    }

    /// Returns whether a partition's read failed after the partition had records.
    pub(super) fn read_failed(&self) -> bool {
        self.read_failed
    }

    /// Returns the limit of the next partition's read.
    ///
    /// Every partition reads within its own `partition_max_bytes` and what is left of the
    /// response's `max_bytes`, the first one included ([ReplicaManager.scala#L1633]). Until a
    /// partition of the round returns records, each partition gets a soft limit, which its first
    /// record or batch may exceed. After that, each gets a hard limit. Kafka reads with
    /// `minOneMessage` until a partition returns records ([ReplicaManager.scala#L1731-L1740]), so
    /// that the consumer makes progress whatever its limits.
    ///
    /// [ReplicaManager.scala#L1633]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1633
    /// [ReplicaManager.scala#L1731-L1740]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1731-L1740
    fn limit_for(&self, partition_max_bytes: i32) -> ByteLimit {
        let bytes = min(partition_max_bytes as u32, self.bytes_left);

        if self.has_records {
            ByteLimit::Hard(bytes)
        } else {
            ByteLimit::Soft(bytes)
        }
    }

    /// Updates the round after a partition's read has finished with `partition_read`.
    ///
    /// This subtracts the bytes that the partition returned, not its limit, from what the response
    /// can still return, because the first partition with records can return a batch larger than
    /// its limit. The response then stays within `max_bytes`, unless that batch alone exceeds it,
    /// and then the response holds only that batch, as in Kafka.
    ///
    /// A partition whose read stopped at its limit returns nothing for the batch that did not fit,
    /// so it takes nothing from what the response can still return. Kafka instead sends the start
    /// of that batch and subtracts its bytes ([ReplicaManager.scala#L1736-L1740]), but the client
    /// drops a partial batch, so this leaves those bytes to the partitions after it.
    ///
    /// The round answers once the bytes of its partitions reach `min_bytes`. A partition counts
    /// the bytes that it returned. A partition whose read stopped at its limit counts its whole
    /// limit instead, because at least that many bytes wait in it. Kafka counts each partition
    /// the same way: the bytes available after the fetch offset, up to the partition's limit
    /// ([DelayedFetch.scala#L108-L110]).
    ///
    /// Without this, a Fetch whose later partition holds a batch larger than its limit would wait
    /// the whole `max_wait`, and then return the same records, because that batch is returned
    /// only under a soft limit. The partition returns the batch in a later Fetch, when no
    /// partition before it returns records.
    ///
    /// [ReplicaManager.scala#L1736-L1740]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ReplicaManager.scala#L1736-L1740
    /// [DelayedFetch.scala#L108-L110]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/DelayedFetch.scala#L108-L110
    fn finish_partition(&mut self, partition_read: &PartitionRead) {
        let returned = partition_read.data.byte_size();

        debug_assert!(self.partitions_left > 0, "{self:?}");
        self.partitions_left = self.partitions_left.saturating_sub(1);
        self.read_failed |= partition_read.read_failed;

        self.bytes_left = self
            .bytes_left
            .saturating_sub(u32::try_from(returned).unwrap_or(u32::MAX));

        // Only a partition that returned records makes the limits of the next partitions hard.
        self.has_records |= partition_read
            .data
            .records
            .as_ref()
            .is_some_and(|records| !records.batches.is_empty());

        self.available_bytes += match partition_read.reached_limit {
            Some(limit) => returned.max(u64::from(limit.bytes())),
            None => returned,
        };
    }
}
