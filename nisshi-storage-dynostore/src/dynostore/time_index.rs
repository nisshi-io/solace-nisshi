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

//! The per-partition time index that answers `ListOffsets(Timestamp)`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Kafka's own `NO_TIMESTAMP` sentinel: a batch whose `max_timestamp` is at
/// or below this is never indexed.
pub(super) const NO_TIMESTAMP: i64 = -1;

/// The most entries a [`TimeIndex`] holds. An append past this count drops
/// every other entry and doubles the interval, so a partition's watermark
/// document stays about this size however long the partition lives.
pub(super) const MAX_ENTRIES: usize = 1024;

/// The initial interval, in batch bytes, between two entries: Apache Kafka's
/// default `index.interval.bytes`.
pub(super) const INITIAL_INTERVAL_BYTES: u64 = 4096;

/// The version of the rule that builds a [`TimeIndex`]. At version 1, each
/// batch's maximum timestamp comes from its records, or from a header that
/// the broker rewrote from them, and never from a header as a client wrote
/// it. The next lookup rebuilds an index at an older version (see
/// [`TimeIndex::is_complete`] and [`TimeIndex::begin_backfill`]).
pub(super) const VERSION: u32 = 1;

/// A sparse time index of one partition: `max_timestamp -> base_offset` of
/// some of its batches, in the partition's watermark document.
///
/// Every entry keeps one guarantee: each batch before the entry's offset has
/// a `max_timestamp` below the entry's timestamp. A lookup takes the greatest
/// entry at or below its target (see [`TimeIndex::floor_offset`]) and scans
/// batches forward from that offset, as Kafka's
/// `LogSegment.findOffsetByTimestamp` does, because a record at or after
/// the target is never before that offset. Removing an entry keeps the
/// guarantee, so the index can be thinned, and pruned below a new log start,
/// without a lookup answering wrong; a longer scan is the only cost.
///
/// An entry is appended under Kafka's `TimeIndex.maybeAppend` rule, only
/// when a batch's `max_timestamp` exceeds every one appended before it, and
/// only once `interval_bytes` of batches have been appended since the last
/// entry (see [`TimeIndex::append`]). The index therefore grows with the
/// partition's bytes and not with its batch count, and [`MAX_ENTRIES`] bounds
/// it from there.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub(super) struct TimeIndex {
    /// `max_timestamp -> base_offset`.
    #[serde(default)]
    entries: BTreeMap<i64, i64>,

    /// The greatest `max_timestamp` of every batch appended so far, indexed
    /// or not. A lookup above it has no match.
    #[serde(default = "no_timestamp")]
    max_timestamp: i64,

    /// Batch bytes appended since the last entry.
    #[serde(default)]
    bytes_since_entry: u64,

    /// Batch bytes between two entries. Doubles each time the index is
    /// thinned.
    #[serde(default = "initial_interval_bytes")]
    interval_bytes: u64,

    /// Whether the index has seen every batch of the partition from offset 0
    /// onward. A watermark document written before the index existed lacks
    /// this field, so it defaults to `false`, and the first lookup backfills
    /// the index from the partition's batches.
    #[serde(default)]
    complete: bool,

    /// The [`VERSION`] of the rule that built the index's entries and
    /// maximum. A document written before the field existed, or rewritten
    /// by a broker without it, decodes as version 0.
    #[serde(default)]
    version: u32,

    /// The token of the backfill in progress, set before its listing and
    /// cleared by its commit. A binary without the index rewrites the
    /// document without this field, so a commit after such a write finds
    /// its token gone and leaves the index incomplete (see
    /// [`TimeIndex::merge`]).
    #[serde(default)]
    backfill: Option<u64>,
}

fn no_timestamp() -> i64 {
    NO_TIMESTAMP
}

fn initial_interval_bytes() -> u64 {
    INITIAL_INTERVAL_BYTES
}

impl Default for TimeIndex {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            max_timestamp: NO_TIMESTAMP,
            bytes_since_entry: 0,
            interval_bytes: INITIAL_INTERVAL_BYTES,
            complete: false,
            version: 0,
            backfill: None,
        }
    }
}

impl TimeIndex {
    /// Returns the empty index of a partition without a batch, which is
    /// complete from offset 0 onward.
    pub(super) fn complete() -> Self {
        Self {
            complete: true,
            version: VERSION,
            ..Self::default()
        }
    }

    /// Returns the complete, empty index of a new partition with an
    /// `interval_bytes` other than [`INITIAL_INTERVAL_BYTES`], for a test
    /// that needs a denser or a sparser index than the default.
    #[cfg(test)]
    pub(super) fn complete_with_interval(interval_bytes: u64) -> Self {
        Self {
            interval_bytes,
            ..Self::complete()
        }
    }

    /// Returns whether the index is complete at the current [`VERSION`] or a
    /// later one. An index completed at an older version reads as
    /// incomplete, so that the next lookup rebuilds it from the records. An
    /// index at a later version reads as complete, so that a rollback does
    /// not rebuild it.
    pub(super) fn is_complete(&self) -> bool {
        self.complete && self.version >= VERSION
    }

    /// Returns whether the index was built by an older version's rule, and
    /// its entries and maximum can come from headers that disagree with
    /// their records.
    pub(super) fn is_older_version(&self) -> bool {
        self.version < VERSION
    }

    /// Marks the index complete from offset 0 onward, at the current
    /// [`VERSION`]: for a partition whose first batch is being appended,
    /// nothing predates it; for a backfill candidate, its listing covered
    /// every batch below the high watermark.
    pub(super) fn mark_complete(&mut self) {
        self.complete = true;
        self.version = VERSION;
    }

    /// Marks a backfill in progress and returns its token: `token` when
    /// none was in progress, else the token of the backfill another process
    /// began, which then covers this one's listing as well.
    ///
    /// When no backfill is in progress, the function first clears an index
    /// at an older [`VERSION`], complete or not, and stamps it with the
    /// current one. Its entries and its maximum timestamp can come from
    /// headers that overstate their records, and [`TimeIndex::merge`] keeps
    /// a live entry above the candidate's maximum, and the live maximum, so
    /// an overstated header would survive the rebuild. The listing covers
    /// every batch below the high watermark that this same write reads, so
    /// the cleared index loses nothing that the candidate does not hold.
    ///
    /// The function does not clear while a backfill is in progress, because
    /// that backfill's listing does not cover the produces after it, and a
    /// clear would drop their maximum timestamp.
    pub(super) fn begin_backfill(&mut self, token: u64) -> u64 {
        if self.backfill.is_none() && self.is_older_version() {
            *self = Self {
                version: VERSION,
                ..Self::default()
            };
        }

        *self.backfill.get_or_insert(token)
    }

    pub(super) fn entries(&self) -> &BTreeMap<i64, i64> {
        &self.entries
    }

    pub(super) fn max_timestamp(&self) -> i64 {
        self.max_timestamp
    }

    #[cfg(test)]
    pub(super) fn interval_bytes(&self) -> u64 {
        self.interval_bytes
    }

    /// Records a batch of `bytes` with `max_timestamp` appended at
    /// `base_offset`, and indexes it when it raises the partition's maximum
    /// timestamp and the interval since the last entry is full.
    ///
    /// Batches must be appended in offset order, because the guarantee of an
    /// entry is over every batch before it.
    pub(super) fn append(&mut self, max_timestamp: i64, base_offset: i64, bytes: u64) {
        self.bytes_since_entry = self.bytes_since_entry.saturating_add(bytes);

        if max_timestamp <= self.max_timestamp {
            return;
        }

        self.max_timestamp = max_timestamp;

        if self.entries.is_empty() || self.bytes_since_entry >= self.interval_bytes {
            _ = self.entries.insert(max_timestamp, base_offset);
            self.bytes_since_entry = 0;

            if self.entries.len() > MAX_ENTRIES {
                self.thin();
            }
        }
    }

    /// Raises the maximum timestamp for a batch of `max_timestamp` that gets
    /// no entry, so that a lookup above every indexed timestamp and at or
    /// below this one still scans for it.
    pub(super) fn observe(&mut self, max_timestamp: i64) {
        self.max_timestamp = self.max_timestamp.max(max_timestamp);
    }

    /// Drops every other entry, keeping the newest, and doubles the interval
    /// so the index fills again at half the rate.
    fn thin(&mut self) {
        let mut keep = self.entries.len().is_multiple_of(2);

        self.entries.retain(|_, _| {
            keep = !keep;
            keep
        });

        self.interval_bytes = self.interval_bytes.saturating_mul(2);
    }

    /// Returns the offset to scan from for the first record at or after
    /// `target`: the offset of the greatest entry whose timestamp is at or
    /// below `target`, or offset 0 when `target` is below every entry.
    pub(super) fn floor_offset(&self, target: i64) -> i64 {
        self.entries
            .range(..=target)
            .next_back()
            .map_or(0, |(_, &base_offset)| base_offset)
    }

    /// Merges a `candidate` index built from the partition's listed batches
    /// with the `live` index that produces have maintained meanwhile. The
    /// result keeps the index's guarantee, and is complete only under the
    /// conditions in the last paragraph.
    ///
    /// The merge is not a union of the two maps. The live index of a
    /// partition that predates the index starts from no maximum timestamp,
    /// so an entry it holds can be below a batch that the candidate listed,
    /// indexed or not. A live entry is kept only when it is above the
    /// candidate's maximum timestamp: the candidate listed every batch up
    /// to its maximum, and the live index saw every batch after the
    /// listing in offset order, because a produce appends to the index
    /// whether or not it is complete. The entries of both are then replayed
    /// in offset order under the append rule. Each entry of the result is
    /// above every batch before it, so the result keeps the index's
    /// guarantee.
    ///
    /// The result is complete only when the candidate is complete (its
    /// listing covered every batch below the high watermark) and the live
    /// index is already complete or still carries the backfill's `token`.
    /// A binary without the index rewrites the document without the token
    /// and without the batches it appended, so a commit that finds the
    /// token gone leaves the index incomplete, and the next lookup
    /// backfills again.
    pub(super) fn merge(candidate: Self, live: &Self, token: u64) -> Self {
        let candidate_max = candidate.max_timestamp;
        let owned = live.is_complete() || live.backfill == Some(token);
        let complete = candidate.is_complete() && owned;

        // An older broker's backfill can commit from the headers while this
        // one runs, and take its token. The result then keeps the live
        // version, so that the next backfill clears the entries it took from
        // the headers.
        let version = if owned { VERSION } else { live.version };

        let mut by_offset: BTreeMap<i64, i64> = candidate
            .entries
            .into_iter()
            .map(|(timestamp, base_offset)| (base_offset, timestamp))
            .collect();

        for (&timestamp, &base_offset) in live.entries.range(candidate_max.saturating_add(1)..) {
            _ = by_offset.insert(base_offset, timestamp);
        }

        let mut merged = Self {
            entries: BTreeMap::new(),
            max_timestamp: NO_TIMESTAMP,
            bytes_since_entry: 0,
            interval_bytes: candidate.interval_bytes.max(live.interval_bytes),
            complete,
            version,
            backfill: None,
        };

        for (base_offset, timestamp) in by_offset {
            // Every entry of either side counts, whatever its interval, so
            // the merge passes the interval's worth of bytes with each.
            merged.append(timestamp, base_offset, merged.interval_bytes);
        }

        merged.max_timestamp = merged
            .max_timestamp
            .max(candidate.max_timestamp)
            .max(live.max_timestamp);
        merged.bytes_since_entry = live.bytes_since_entry;

        merged
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: i64 = 1_577_836_800_000;

    fn dense() -> TimeIndex {
        TimeIndex {
            interval_bytes: 1,
            ..TimeIndex::complete()
        }
    }

    #[test]
    fn append_skips_a_batch_below_the_maximum_timestamp() {
        let mut index = dense();
        index.append(T0, 0, 1);
        index.append(T0 - 50, 1, 1);
        index.append(T0 + 50, 2, 1);

        assert_eq!(&BTreeMap::from([(T0, 0), (T0 + 50, 2)]), index.entries());
        assert_eq!(T0 + 50, index.max_timestamp());
    }

    #[test]
    fn append_never_indexes_no_timestamp() {
        let mut index = dense();
        index.append(NO_TIMESTAMP, 0, 1);
        index.append(NO_TIMESTAMP - 1, 1, 1);

        assert!(index.entries().is_empty());
        assert_eq!(NO_TIMESTAMP, index.max_timestamp());
    }

    /// A batch that raises the maximum timestamp inside the interval raises
    /// `max_timestamp` without an entry, so a later batch with a lower
    /// timestamp is still not indexed.
    #[test]
    fn append_tracks_the_maximum_between_entries() {
        let mut index = TimeIndex {
            interval_bytes: 100,
            ..TimeIndex::complete()
        };
        index.append(T0, 0, 10);
        index.append(T0 + 50, 1, 10);
        index.append(T0 + 25, 2, 10);
        index.append(T0 + 75, 3, 100);

        assert_eq!(&BTreeMap::from([(T0, 0), (T0 + 75, 3)]), index.entries());
        assert_eq!(T0 + 75, index.max_timestamp());
    }

    #[test]
    fn floor_offset_takes_the_greatest_entry_at_or_below_the_target() {
        let mut index = dense();
        index.append(100, 0, 1);
        index.append(150, 2, 1);
        index.append(200, 5, 1);

        assert_eq!(0, index.floor_offset(99));
        assert_eq!(0, index.floor_offset(100));
        assert_eq!(0, index.floor_offset(149));
        assert_eq!(2, index.floor_offset(150));
        assert_eq!(5, index.floor_offset(200));
        assert_eq!(5, index.floor_offset(10_000));
    }

    #[test]
    fn thinning_keeps_the_index_within_the_cap_and_doubles_the_interval() {
        let mut index = dense();
        let cap = i64::try_from(MAX_ENTRIES).expect("fits");

        for i in 0..cap {
            index.append(T0 + i, i, 1);
        }
        assert_eq!(MAX_ENTRIES, index.entries().len());
        assert_eq!(1, index.interval_bytes());

        // The append past the cap thins the index, keeping the newest entry.
        index.append(T0 + cap, cap, 1);
        assert_eq!(MAX_ENTRIES / 2 + 1, index.entries().len());
        assert_eq!(2, index.interval_bytes());
        assert_eq!(Some((&(T0 + cap), &cap)), index.entries().last_key_value());

        let count = cap * 3;
        for i in cap + 1..count {
            index.append(T0 + i, i, 1);
            assert!(index.entries().len() <= MAX_ENTRIES, "after {i}");
        }
        assert_eq!(T0 + count - 1, index.max_timestamp());

        // Every surviving entry keeps its guarantee: the floor for a target
        // is at or before the target's own batch.
        for target in [T0, T0 + 1, T0 + count / 2, T0 + count - 1] {
            assert!(index.floor_offset(target) <= target - T0);
        }
    }

    /// A live entry below a candidate entry of a lower offset is dropped, so
    /// the merged index cannot start a scan past the earlier, matching batch.
    #[test]
    fn merge_drops_a_live_entry_below_an_earlier_candidate_entry() {
        let mut candidate = dense();
        candidate.append(T0, 0, 1);
        candidate.append(T0 - 50, 1, 1);

        let mut live = TimeIndex::default();
        assert_eq!(7, live.begin_backfill(7));
        live.append(T0 - 20, 2, 1);

        let merged = TimeIndex::merge(candidate, &live, 7);

        assert!(merged.is_complete());
        assert_eq!(&BTreeMap::from([(T0, 0)]), merged.entries());
        assert_eq!(T0, merged.max_timestamp());
    }

    /// The merged index is complete only when the candidate's listing was
    /// complete and the live index still carries the backfill's token. A
    /// live index rewritten without the token (by a binary without the
    /// index), or under another backfill's token, leaves it incomplete. A
    /// live index that is already complete stays complete.
    #[test]
    fn merge_completes_only_a_complete_candidate_under_its_own_backfill() {
        let candidate = dense();

        let mut live = TimeIndex::default();
        assert_eq!(7, live.begin_backfill(7));
        assert_eq!(
            7,
            live.begin_backfill(8),
            "a second backfill joins the first"
        );
        assert!(TimeIndex::merge(candidate.clone(), &live, 7).is_complete());
        assert!(!TimeIndex::merge(candidate.clone(), &live, 8).is_complete());
        assert!(!TimeIndex::merge(candidate.clone(), &TimeIndex::default(), 7).is_complete());
        assert!(TimeIndex::merge(candidate.clone(), &TimeIndex::complete(), 7).is_complete());

        let incomplete = TimeIndex::default();
        assert!(!TimeIndex::merge(incomplete, &live, 7).is_complete());

        let merged = TimeIndex::merge(candidate, &live, 7);
        assert_eq!(None, merged.backfill, "the commit clears the token");
    }

    /// The candidate's maximum can belong to a batch inside the interval,
    /// with no entry. A live entry below that maximum is still below a
    /// listed batch, so it is dropped.
    #[test]
    fn merge_drops_a_live_entry_below_the_candidate_maximum() {
        let mut candidate = TimeIndex {
            interval_bytes: 100,
            ..TimeIndex::complete()
        };
        candidate.append(T0, 0, 10);
        candidate.append(T0 + 100, 1, 10);
        candidate.append(T0 + 50, 2, 10);
        assert_eq!(&BTreeMap::from([(T0, 0)]), candidate.entries());

        let mut live = TimeIndex::default();
        live.append(T0 + 70, 3, 10);

        let merged = TimeIndex::merge(candidate, &live, 7);

        assert_eq!(&BTreeMap::from([(T0, 0)]), merged.entries());
        assert_eq!(T0 + 100, merged.max_timestamp());
    }

    #[test]
    fn merge_keeps_a_live_entry_above_the_candidate() {
        let mut candidate = dense();
        candidate.append(T0, 0, 1);

        let mut live = TimeIndex::default();
        live.append(T0 + 50, 2, 1);

        let merged = TimeIndex::merge(candidate, &live, 7);

        assert_eq!(&BTreeMap::from([(T0, 0), (T0 + 50, 2)]), merged.entries());
        assert_eq!(T0 + 50, merged.max_timestamp());
    }

    /// A watermark document written before the index existed decodes into
    /// an incomplete, empty index.
    #[test]
    fn decodes_an_empty_document() -> Result<(), serde_json::Error> {
        let index: TimeIndex = serde_json::from_str("{}")?;
        assert_eq!(TimeIndex::default(), index);
        assert!(!index.is_complete());
        Ok(())
    }

    fn completed_before_version() -> Result<TimeIndex, serde_json::Error> {
        serde_json::from_str(&format!(
            r#"{{"entries":{{"{}":0}},"max_timestamp":{},"complete":true}}"#,
            T0 + 1_000,
            T0 + 1_000
        ))
    }

    /// An index that a broker completed before [`VERSION`] existed reads as
    /// incomplete, so that the next lookup rebuilds it.
    #[test]
    fn index_completed_at_an_older_version_is_not_complete() -> Result<(), serde_json::Error> {
        let index = completed_before_version()?;
        assert_eq!(0, index.version);
        assert!(!index.is_complete());
        Ok(())
    }

    /// The backfill clears the entries and the maximum of an index completed
    /// at an older version, so that the merge cannot keep them.
    #[test]
    fn begin_backfill_clears_an_index_completed_at_an_older_version()
    -> Result<(), serde_json::Error> {
        let mut index = completed_before_version()?;
        assert_eq!(7, index.begin_backfill(7));

        assert!(index.entries().is_empty());
        assert_eq!(NO_TIMESTAMP, index.max_timestamp());
        assert!(!index.is_complete());

        let mut candidate = dense();
        candidate.append(T0, 0, 1);
        let merged = TimeIndex::merge(candidate, &index, 7);

        assert!(merged.is_complete());
        assert_eq!(&BTreeMap::from([(T0, 0)]), merged.entries());
        assert_eq!(T0, merged.max_timestamp());
        Ok(())
    }

    /// An older broker's backfill can leave an index incomplete with entries
    /// from the headers, so the backfill clears an incomplete index at an
    /// older version too.
    #[test]
    fn begin_backfill_clears_an_incomplete_index_at_an_older_version() {
        let mut index = TimeIndex::default();
        index.append(T0 + 1_000, 0, 1);
        assert_eq!(7, index.begin_backfill(7));

        assert!(index.entries().is_empty());
        assert_eq!(NO_TIMESTAMP, index.max_timestamp());
        assert_eq!(VERSION, index.version);
    }

    /// The backfill keeps an index at an older version while another
    /// backfill is in progress, and joins it. That backfill's listing does
    /// not cover a produce after it, so a clear would drop that produce's
    /// maximum, and a lookup would answer no match for it.
    #[test]
    fn begin_backfill_joins_a_backfill_in_progress_without_clearing() {
        let mut index = TimeIndex::default();
        assert_eq!(7, index.begin_backfill(7));
        index.append(T0, 3, 1);

        // A broker without the version rewrites the document.
        index.version = 0;

        assert_eq!(7, index.begin_backfill(8));
        assert_eq!(&BTreeMap::from([(T0, 3)]), index.entries());
        assert_eq!(T0, index.max_timestamp());
    }

    #[test]
    fn begin_backfill_keeps_an_index_at_the_current_version() {
        let mut index = dense();
        index.append(T0, 0, 1);
        assert_eq!(7, index.begin_backfill(7));
        assert_eq!(&BTreeMap::from([(T0, 0)]), index.entries());
        assert!(index.is_complete());
    }

    /// A broker that knows a later version reads an index at that version
    /// as complete, so that a rollback does not rebuild it.
    #[test]
    fn index_completed_at_a_later_version_is_complete() {
        let index = TimeIndex {
            version: VERSION + 1,
            ..TimeIndex::complete()
        };
        assert!(index.is_complete());
    }

    /// The merge marks the result complete only when the live index is
    /// complete at the current version, or carries the backfill's token. An
    /// older broker's backfill can commit over the token, and the result
    /// then keeps the live version, so that the next backfill clears the
    /// entries that the older broker took from the headers.
    #[test]
    fn merge_over_an_older_version_keeps_the_live_version() -> Result<(), serde_json::Error> {
        let mut candidate = dense();
        candidate.append(T0, 0, 1);

        let live = completed_before_version()?;
        let merged = TimeIndex::merge(candidate.clone(), &live, 7);
        assert!(!merged.is_complete());
        assert_eq!(0, merged.version);

        let mut live = TimeIndex::default();
        assert_eq!(7, live.begin_backfill(7));
        let merged = TimeIndex::merge(candidate, &live, 7);
        assert!(merged.is_complete());
        assert_eq!(VERSION, merged.version);
        Ok(())
    }

    #[test]
    fn index_of_a_new_partition_is_complete_at_the_current_version() {
        assert!(TimeIndex::complete().is_complete());
        assert_eq!(VERSION, TimeIndex::complete().version);

        let mut index = TimeIndex::default();
        index.mark_complete();
        assert!(index.is_complete());
        assert_eq!(VERSION, index.version);
    }
}
