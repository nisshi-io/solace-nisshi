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

//! Tests for `ListOffsets`: `Earliest` and `Latest` from the partition's
//! watermark, and the `time_index`-backed `Timestamp` lookup.
//! Calls `DynoStore`'s private helpers directly (this module is
//! a descendant of `dynostore`, so it can see them) rather than only going
//! through the `Storage` trait, because some of the scenarios below need to
//! control the exact interleaving between a backfill's read and its write.

use std::{
    collections::BTreeMap,
    fmt::Display,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};

use crate::dynostore::{
    BATCH_SETTLED_AFTER, DynoStore, Txn, TxnDetail, TxnProduceOffset, Watermark, batch_path,
    tests::init_tracing,
    time_index::{INITIAL_INTERVAL_BYTES, MAX_ENTRIES, TimeIndex},
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use nisshi_sans_io::{
    BatchAttribute, Compression, ErrorCode, IsolationLevel, ListOffset, TimestampType,
    create_topics_request::CreatableTopic,
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
    to_system_time, to_timestamp,
};
use nisshi_storage::{Error, ListOffsetResponse, Result, Storage, Topition, TxnState};
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    memory::InMemory, path::Path,
};

/// An arbitrary fixed point in 2020, in Kafka's millisecond-since-epoch
/// timestamp representation. A real, far-from-`now` value keeps a test from
/// passing by coincidence the way a `SystemTime::now()`-based one could.
const T0: i64 = 1_577_836_800_000; // 2020-01-01T00:00:00Z

fn storage() -> Result<DynoStore> {
    DynoStore::new("nisshi", 111, InMemory::new())
}

async fn create_topic(storage: &DynoStore, topic: &str, num_partitions: i32) -> Result<()> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.into())
                .num_partitions(num_partitions)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(())
}

/// Builds a single-record batch with an explicit, fully-controlled
/// `base_timestamp`/`max_timestamp`, carrying `last_offset_delta` extra
/// records beyond the first so a caller can build a multi-record batch by
/// supplying more than one timestamp.
fn batch_with_timestamps(base_offset: i64, timestamps: &[i64]) -> Result<deflated_batch::Batch> {
    deflated_batch::build(base_offset, timestamps, Bytes::from_static(b"v"))
}

/// A batch that fills the index's initial interval on its own, so that it
/// gets an entry whenever it raises the maximum timestamp.
fn interval_batch(base_offset: i64, timestamp: i64) -> Result<deflated_batch::Batch> {
    deflated_batch::build(
        base_offset,
        &[timestamp],
        Bytes::from(vec![b'v'; usize::try_from(INITIAL_INTERVAL_BYTES)?]),
    )
}

// Keeps the `deflated`/`inflated` plumbing out of the test bodies above.
mod deflated_batch {
    use super::*;
    use nisshi_sans_io::record::{Record, deflated, inflated};

    pub(super) use deflated::Batch;

    pub(super) fn build(base_offset: i64, timestamps: &[i64], value: Bytes) -> Result<Batch> {
        let base_timestamp = timestamps[0];
        let max_timestamp = timestamps.iter().copied().max().unwrap_or(base_timestamp);

        let mut builder = inflated::Batch::builder()
            .base_offset(base_offset)
            .base_timestamp(base_timestamp)
            .max_timestamp(max_timestamp)
            .last_offset_delta(i32::try_from(timestamps.len() - 1).unwrap_or_default());

        for (i, timestamp) in timestamps.iter().enumerate() {
            builder = builder.record(
                Record::builder()
                    .value(Some(value.clone()))
                    .timestamp_delta(timestamp - base_timestamp)
                    .offset_delta(i32::try_from(i).unwrap_or_default()),
            );
        }

        builder
            .build()
            .and_then(Batch::try_from)
            .map_err(Into::into)
    }

    /// Builds a batch whose header claims `max_timestamp`, whatever its
    /// records hold, as a client wrote it before the broker rewrote the
    /// header from the records.
    pub(super) fn with_header(
        base_offset: i64,
        timestamp_type: TimestampType,
        base_timestamp: i64,
        max_timestamp: i64,
        timestamp_deltas: &[i64],
    ) -> Result<Batch> {
        timestamp_deltas
            .iter()
            .zip(0..)
            .fold(
                inflated::Batch::builder()
                    .base_offset(base_offset)
                    .attributes(BatchAttribute::default().timestamp(timestamp_type).into())
                    .base_timestamp(base_timestamp)
                    .max_timestamp(max_timestamp)
                    .last_offset_delta(i32::try_from(timestamp_deltas.len())? - 1),
                |builder, (&timestamp_delta, offset_delta)| {
                    builder.record(
                        Record::builder()
                            .value(Some(Bytes::from_static(b"v")))
                            .timestamp_delta(timestamp_delta)
                            .offset_delta(offset_delta),
                    )
                },
            )
            .build()
            .and_then(Batch::try_from)
            .map_err(Into::into)
    }
}

/// Produces one batch through the normal path (so `time_index` is maintained
/// the way a real produce maintains it), returning its base offset.
async fn produce(storage: &DynoStore, topition: &Topition, timestamps: &[i64]) -> Result<i64> {
    let batch = batch_with_timestamps(0, timestamps)?;
    storage.produce(None, topition, batch).await
}

/// Writes a raw batch object directly to the object store, bypassing
/// `produce` entirely - simulating a batch written before this code (and its
/// `time_index` maintenance) existed.
async fn write_legacy_batch(
    storage: &DynoStore,
    topition: &Topition,
    base_offset: i64,
    timestamp: i64,
) -> Result<()> {
    write_batch_object(
        storage,
        topition,
        batch_with_timestamps(base_offset, &[timestamp])?,
    )
    .await
}

/// Writes `batch`'s object directly to the object store, at its own base
/// offset, as the last step of `produce` does.
async fn write_batch_object(
    storage: &DynoStore,
    topition: &Topition,
    batch: deflated_batch::Batch,
) -> Result<()> {
    let base_offset = batch.base_offset;
    let path = batch_path(&storage.cluster, topition, base_offset);
    let payload = storage.encode(batch)?;

    _ = storage
        .object_store
        .put_opts(
            &path,
            payload,
            PutOptions {
                mode: PutMode::Create,
                attributes: Attributes::new(),
                ..Default::default()
            },
        )
        .await
        .map_err(Error::from)?;

    Ok(())
}

/// Directly sets a partition's watermark fields, bypassing both `produce`
/// and `create_topic`'s reset - used to set up pre-migration ("legacy") or
/// post-`DeleteRecords` (`low > 0`) states that nothing in the normal write
/// path produces on its own.
async fn seed_watermark(
    storage: &DynoStore,
    topition: &Topition,
    f: impl Fn(&mut Watermark) -> Result<()>,
) -> Result<()> {
    let watermark = storage.watermark_for(topition)?;
    watermark.with_mut(&storage.object_store, f).await
}

async fn list_offsets_timestamp(
    storage: &DynoStore,
    topition: &Topition,
    target: i64,
) -> Result<(Option<i64>, Option<i64>)> {
    list_offsets_timestamp_at(storage, IsolationLevel::ReadUncommitted, topition, target).await
}

/// The offset and timestamp a lookup answers, after checking that it
/// answered without an error.
async fn list_offsets_timestamp_at(
    storage: &DynoStore,
    isolation_level: IsolationLevel,
    topition: &Topition,
    target: i64,
) -> Result<(Option<i64>, Option<i64>)> {
    let responses =
        list_offsets_timestamp_responses(storage, isolation_level, &[(topition, target)]).await?;

    assert_eq!(1, responses.len());
    assert_eq!(ErrorCode::None, responses[0].error_code);

    Ok((
        responses[0].offset,
        responses[0]
            .timestamp
            .map(|t| to_timestamp(&t))
            .transpose()?,
    ))
}

/// One `ListOffsets` request with a timestamp `target` for each partition
/// of `lookups`, answered in the same order.
async fn list_offsets_timestamp_responses(
    storage: &DynoStore,
    isolation_level: IsolationLevel,
    lookups: &[(&Topition, i64)],
) -> Result<Vec<ListOffsetResponse>> {
    let offsets = lookups
        .iter()
        .map(|&(topition, target)| {
            to_system_time(target)
                .map(|target| (topition.to_owned(), ListOffset::Timestamp(target)))
                .map_err(Error::from)
        })
        .collect::<Result<Vec<_>>>()?;

    let responses = storage.list_offsets(isolation_level, &offsets).await?;

    assert_eq!(
        offsets
            .iter()
            .map(|(topition, _)| topition)
            .collect::<Vec<_>>(),
        responses
            .iter()
            .map(|(topition, _)| topition)
            .collect::<Vec<_>>()
    );

    Ok(responses
        .into_iter()
        .map(|(_, response)| response)
        .collect())
}

/// The error code a lookup answers for its partition.
async fn list_offsets_timestamp_error(
    storage: &DynoStore,
    topition: &Topition,
    target: i64,
) -> Result<ErrorCode> {
    let responses = list_offsets_timestamp_responses(
        storage,
        IsolationLevel::ReadUncommitted,
        &[(topition, target)],
    )
    .await?;

    assert_eq!(1, responses.len());
    assert_eq!(None, responses[0].offset);

    Ok(responses[0].error_code)
}

/// Splits the backfill into its two steps and interleaves a concurrent
/// produce between them, to confirm a produce landing mid-backfill is never
/// lost: a write-side guard that defers inserting into `time_index` until
/// the index is complete would miss this entry (the produce never touches
/// `time_index`, so the backfill's commit has nothing to merge it with),
/// which is why `produce` carries no such guard.
#[tokio::test]
async fn backfill_commit_picks_up_concurrent_produce() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "legacy";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;

    // Two legacy batches, written directly (not through `produce`): off0 has
    // the higher timestamp, off1 a lower one that the monotonic rule must
    // not let override it.
    write_legacy_batch(&storage, &topition, 0, T0).await?;
    write_legacy_batch(&storage, &topition, 1, T0 - 50).await?;

    seed_watermark(&storage, &topition, |w| {
        w.low = Some(0);
        w.high = Some(2);
        w.time_index = TimeIndex::default();
        Ok(())
    })
    .await?;

    // Step 1: collect a candidate from the pre-concurrent-produce state.
    let backfill = storage.begin_time_index_backfill(&topition).await?;
    assert_eq!((0, 2), (backfill.low, backfill.high));
    let (candidate, batches) = storage
        .collect_time_index_candidate(&topition, &backfill)
        .await?;
    assert_eq!(2, batches);
    assert_eq!(&BTreeMap::from([(T0, 0)]), candidate.entries());
    assert_eq!(T0, candidate.max_timestamp());
    assert!(candidate.is_complete());

    // Step 2: a concurrent produce lands before the backfill commits, with a
    // new running max above everything seen so far.
    let concurrent_timestamp = T0 + 50;
    let offset = produce(&storage, &topition, &[concurrent_timestamp]).await?;
    assert_eq!(2, offset);

    // Step 3: commit the now-stale candidate.
    let merged = storage
        .commit_time_index_backfill(&topition, &candidate, backfill.token)
        .await?;
    assert_eq!(
        &BTreeMap::from([(T0, 0), (concurrent_timestamp, 2)]),
        merged.entries()
    );
    assert_eq!(concurrent_timestamp, merged.max_timestamp());
    assert!(merged.is_complete());

    // Step 4: the concurrent produce's entry must still be answerable.
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 25).await?;
    assert_eq!(Some(2), offset);
    assert_eq!(Some(concurrent_timestamp), timestamp);

    Ok(())
}

/// A plain `BTreeMap` union of a backfill candidate and whatever is live
/// would keep offset 2's entry below offset 0's already-established higher
/// one. A floor lookup between the two would then start its scan at offset
/// 2 and miss offset 0's record. The correct merge (replay the append rule
/// over the union, sorted by offset) drops that entry, so the lookup
/// answers offset 0.
#[tokio::test]
async fn merge_does_not_let_a_lower_concurrent_entry_outrank_an_established_one() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "legacy-union";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;

    write_legacy_batch(&storage, &topition, 0, T0).await?; // the eventual floor
    write_legacy_batch(&storage, &topition, 1, T0 - 50).await?;

    seed_watermark(&storage, &topition, |w| {
        w.low = Some(0);
        w.high = Some(2);
        w.time_index = TimeIndex::default();
        Ok(())
    })
    .await?;

    // A post-upgrade produce lands before any read ever triggers a backfill:
    // `time_index` is still `None` at this point, so this insert goes into a
    // fresh map, exactly the scenario that makes a plain union wrong.
    let offset = produce(&storage, &topition, &[T0 - 20]).await?;
    assert_eq!(2, offset);

    // The first read now triggers the backfill. The target is above off2's
    // T0 - 20, so an entry for off2 would be the floor, and a scan from
    // there finds nothing. Off0's T0 is the first record at or after it.
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 - 10).await?;
    assert_eq!(Some(0), offset);
    assert_eq!(Some(T0), timestamp);

    Ok(())
}

/// A `low > 0` straddling batch: one batch holds records at offsets 0, 1 and
/// 2, and `DeleteRecords` (simulated here by setting `low` directly, since
/// `delete_records` is still `todo!()` on this branch) has logically removed
/// offsets 0 and 1. A Timestamp lookup must skip the deleted prefix and only
/// match a record at or after `low`, even though the matching batch's own
/// base offset (0) is below `low`.
#[tokio::test]
async fn timestamp_lookup_skips_deleted_prefix_of_a_straddling_batch() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "straddling";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;

    // One batch, three records: offsets 0, 1, 2 with increasing timestamps.
    let timestamps = [T0, T0 + 50, T0 + 100];
    let offset = produce(&storage, &topition, &timestamps).await?;
    assert_eq!(0, offset);

    // Simulate DeleteRecords(cutoff = 2): offsets 0 and 1 are logically
    // gone, and the index still points at the batch's base offset (0).
    seed_watermark(&storage, &topition, |w| {
        w.low = Some(2);
        Ok(())
    })
    .await?;

    // Target below every surviving record's timestamp: must still answer the
    // first surviving record (offset 2), not offset 0 or 1.
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0).await?;
    assert_eq!(Some(2), offset);
    assert_eq!(Some(T0 + 100), timestamp);

    Ok(())
}

/// The sequential scan helper must return `Ok(None)`, not error, when
/// `start_offset` is past every real batch - the case both an out-of-range
/// query and the lake-sink produce path (which never writes a `.batch`
/// object at all) hit.
#[tokio::test]
async fn sequential_scan_past_every_real_batch_returns_none() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "empty";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    _ = produce(&storage, &topition, &[T0]).await?;

    let found = storage
        .sequential_timestamp_scan(&topition, 1_000_000, T0, 0)
        .await?;
    assert_eq!(None, found);

    Ok(())
}

/// A Timestamp lookup against a partition with no batches at all (never
/// produced to) must answer "no match", not error and not offset 0.
#[tokio::test]
async fn empty_partition_no_match() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "never-produced";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0).await?;
    assert_eq!(None, offset);
    assert_eq!(None, timestamp);

    Ok(())
}

/// A Timestamp lookup above every record's timestamp must answer "no
/// match", found directly from the index's greatest timestamp - no scan
/// required.
#[tokio::test]
async fn future_timestamp_no_match() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "future";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    _ = produce(&storage, &topition, &[T0]).await?;

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 1_000_000).await?;
    assert_eq!(None, offset);
    assert_eq!(None, timestamp);

    Ok(())
}

/// A brand-new partition's first produce must mark its index complete
/// immediately: nothing predates offset 0, so no backfill is ever needed for
/// it.
#[tokio::test]
async fn new_partition_first_produce_marks_index_complete() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "brand-new";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    _ = produce(&storage, &topition, &[T0]).await?;

    let watermark = storage.watermark_for(&topition)?;
    let complete = watermark
        .with(&storage.object_store, |w| Ok(w.time_index.is_complete()))
        .await?;
    assert!(complete);

    Ok(())
}

/// Pins the ticket's two remaining named scenarios, both inside one
/// multi-record batch: a target equal to a record's own timestamp (the
/// inclusive boundary) and a target that falls strictly between two records'
/// timestamps (a genuine mid-batch match). One batch, offsets 0/1/2 at
/// T0/T0+50/T0+100, is indexed under a single `time_index` entry keyed by its
/// `max_timestamp` (T0+100), so every assertion here is answered by the
/// sequential scan walking records inside that one batch, not by a floor
/// lookup landing on a different batch.
#[tokio::test]
async fn equal_and_mid_batch_timestamps_match_inside_a_batch() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "boundary";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;

    let timestamps = [T0, T0 + 50, T0 + 100];
    let offset = produce(&storage, &topition, &timestamps).await?;
    assert_eq!(0, offset);

    // Inclusive boundary: target equals offset 1's own timestamp exactly.
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 50).await?;
    assert_eq!(Some(1), offset);
    assert_eq!(Some(T0 + 50), timestamp);

    // Mid-batch match: target falls strictly between offset 0's and offset
    // 1's timestamps, so the first record at or after it is offset 1.
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 25).await?;
    assert_eq!(Some(1), offset);
    assert_eq!(Some(T0 + 50), timestamp);

    // Mid-batch match on the last record: target falls strictly between
    // offset 1's and offset 2's timestamps.
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 51).await?;
    assert_eq!(Some(2), offset);
    assert_eq!(Some(T0 + 100), timestamp);

    Ok(())
}

/// Each produce into a freshly created topic maintains the index under the
/// monotonic rule: a batch whose `max_timestamp` is below the running max
/// adds no entry. An entry for offset 1 here would be the floor of a lookup
/// between the two timestamps, and a scan from offset 1 would miss offset
/// 0, which holds the first record at or after the target.
#[tokio::test]
async fn produce_skips_an_entry_below_the_running_max() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "fresh-out-of-order";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    seed_dense_index(&storage, &topition).await?;

    assert_eq!(0, produce(&storage, &topition, &[T0]).await?);
    assert_eq!(1, produce(&storage, &topition, &[T0 - 50]).await?);

    assert_eq!(
        BTreeMap::from([(T0, 0)]),
        time_index_entries(&storage, &topition).await?
    );

    for target in [T0 - 60, T0 - 40] {
        let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, target).await?;
        assert_eq!(Some(0), offset, "target {target}");
        assert_eq!(Some(T0), timestamp, "target {target}");
    }

    Ok(())
}

/// Sets a fresh partition's index interval to one byte, so that every batch
/// that raises the maximum timestamp is indexed.
async fn seed_dense_index(storage: &DynoStore, topition: &Topition) -> Result<()> {
    seed_index_interval(storage, topition, 1).await
}

async fn seed_index_interval(
    storage: &DynoStore,
    topition: &Topition,
    interval_bytes: u64,
) -> Result<()> {
    seed_watermark(storage, topition, |w| {
        w.time_index = TimeIndex::complete_with_interval(interval_bytes);
        Ok(())
    })
    .await
}

async fn time_index_entries(
    storage: &DynoStore,
    topition: &Topition,
) -> Result<BTreeMap<i64, i64>> {
    storage
        .watermark_for(topition)?
        .with(&storage.object_store, |w| {
            Ok(w.time_index.entries().clone())
        })
        .await
}

/// The index is sparse: a batch that raises the maximum timestamp inside
/// the interval gets no entry. A lookup for a target between two entries
/// must scan from the greatest entry at or below the target (the floor), so
/// that it finds the unindexed batch. A scan from the entry above the
/// target would answer that entry's batch, a later offset than Kafka's.
#[tokio::test]
async fn lookup_scans_from_the_floor_entry_past_an_unindexed_batch() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "sparse";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;

    // Every batch here is one record of the same value, so they are all the
    // same size. The interval admits an entry every third batch.
    let batch_bytes = u64::try_from(batch_with_timestamps(0, &[T0])?.batch_length)?;
    seed_index_interval(&storage, &topition, 2 * batch_bytes + 1).await?;

    for (expected, timestamp) in (0..).zip([T0, T0 + 100, T0 + 200, T0 + 300]) {
        assert_eq!(expected, produce(&storage, &topition, &[timestamp]).await?);
    }

    assert_eq!(
        BTreeMap::from([(T0, 0), (T0 + 300, 3)]),
        time_index_entries(&storage, &topition).await?
    );

    // Offset 2 holds the first record at or after the target, and has no
    // entry.
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 150).await?;
    assert_eq!(Some(2), offset);
    assert_eq!(Some(T0 + 200), timestamp);

    // A target equal to an entry's timestamp answers that entry's batch.
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 300).await?;
    assert_eq!(Some(3), offset);
    assert_eq!(Some(T0 + 300), timestamp);

    // A target between the maximum timestamp and the last entry's timestamp
    // is a match; only a target above the maximum is no match.
    assert_eq!(4, produce(&storage, &topition, &[T0 + 400]).await?);
    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 350).await?;
    assert_eq!(Some(4), offset);
    assert_eq!(Some(T0 + 400), timestamp);
    assert_eq!(
        (None, None),
        list_offsets_timestamp(&storage, &topition, T0 + 401).await?
    );

    Ok(())
}

/// The index holds at most `MAX_ENTRIES` entries however many batches the
/// partition has, and still answers each lookup with the first record at or
/// after the target once it has been thinned.
#[tokio::test]
async fn index_stays_within_the_cap_and_answers_after_thinning() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "capped";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    seed_dense_index(&storage, &topition).await?;

    let batches = i64::try_from(MAX_ENTRIES)? + 1;
    for i in 0..batches {
        assert_eq!(i, produce(&storage, &topition, &[T0 + i]).await?);
    }

    let entries = time_index_entries(&storage, &topition).await?;
    assert!(entries.len() <= MAX_ENTRIES, "{} entries", entries.len());
    assert!(
        entries.len() >= MAX_ENTRIES / 2,
        "{} entries",
        entries.len()
    );

    for target in [T0, T0 + 1, T0 + 2, T0 + batches / 2, T0 + batches - 1] {
        let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, target).await?;
        assert_eq!(Some(target - T0), offset, "target {target}");
        assert_eq!(Some(target), timestamp, "target {target}");
    }

    Ok(())
}

/// A lookup on a partition that does not exist answers
/// `UNKNOWN_TOPIC_OR_PARTITION` with no offset, as Kafka does, and does not
/// create the partition's watermark document.
#[tokio::test]
async fn lookup_does_not_create_a_watermark_document() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topition = Topition::new("absent", 0);

    let responses = list_offsets_timestamp_responses(
        &storage,
        IsolationLevel::ReadUncommitted,
        &[(&topition, T0)],
    )
    .await?;
    assert_eq!(1, responses.len());
    assert_eq!(ErrorCode::UnknownTopicOrPartition, responses[0].error_code);
    assert_eq!(None, responses[0].offset);
    assert_eq!(None, responses[0].timestamp);

    let path = Path::from("clusters/nisshi/topics/absent/partitions/0000000000/watermark.json");
    assert!(matches!(
        storage.object_store.head(&path).await,
        Err(object_store::Error::NotFound { .. })
    ));

    Ok(())
}

/// Under READ_COMMITTED, a match at or after the first offset of an open
/// transaction is no match, as in Kafka. READ_UNCOMMITTED still answers it.
#[tokio::test]
async fn read_committed_lookup_stops_at_the_last_stable_offset() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "open-transaction";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    assert_eq!(0, produce(&storage, &topition, &[T0]).await?);
    assert_eq!(1, produce(&storage, &topition, &[T0 + 50]).await?);

    storage
        .meta
        .with_mut(&storage.object_store, |meta| {
            _ = meta.transactions.insert(
                "txn".into(),
                Txn {
                    producer: 1,
                    epochs: BTreeMap::from([(
                        0,
                        TxnDetail {
                            state: Some(TxnState::Begin),
                            produces: BTreeMap::from([(
                                topic.into(),
                                BTreeMap::from([(
                                    0,
                                    Some(TxnProduceOffset {
                                        offset_start: 1,
                                        offset_end: 2,
                                    }),
                                )]),
                            )]),
                            ..Default::default()
                        },
                    )]),
                },
            );
            Ok(())
        })
        .await?;

    let committed =
        list_offsets_timestamp_at(&storage, IsolationLevel::ReadCommitted, &topition, T0 + 25)
            .await?;
    assert_eq!((None, None), committed);

    let committed =
        list_offsets_timestamp_at(&storage, IsolationLevel::ReadCommitted, &topition, T0).await?;
    assert_eq!((Some(0), Some(T0)), committed);

    let uncommitted = list_offsets_timestamp(&storage, &topition, T0 + 25).await?;
    assert_eq!((Some(1), Some(T0 + 50)), uncommitted);

    Ok(())
}

/// The error that [`Faulty`] returns for the next GET of its armed path.
#[derive(Clone, Copy, Debug)]
enum Fault {
    NotFound,
    Unavailable,
}

/// An object store that fails the next GET of one armed path, counts the
/// GETs of batch objects and the listings, once aged lists every object as written longer
/// than [`BATCH_SETTLED_AFTER`] ago, and can report every write time from
/// a clock behind the test's own. Every GET first yields to the runtime,
/// so that concurrent requests in a test interleave.
#[derive(Clone, Debug, Default)]
struct Faulty {
    inner: Arc<InMemory>,
    armed: Arc<Mutex<Option<(Path, Fault)>>>,
    batch_gets: Arc<AtomicUsize>,
    lists: Arc<AtomicUsize>,
    aged: Arc<AtomicBool>,
    /// How far the store's clock is behind the test's, in seconds, in every
    /// write time the store reports.
    clock_behind_secs: Arc<AtomicU64>,
    /// Conditional PUTs that failed because the object had changed.
    put_conflicts: Arc<AtomicUsize>,
}

impl Faulty {
    fn arm(&self, path: Path, fault: Fault) -> Result<()> {
        self.armed
            .lock()
            .map(|mut armed| _ = armed.replace((path, fault)))
            .map_err(Into::into)
    }

    fn batch_gets(&self) -> usize {
        self.batch_gets.load(Ordering::SeqCst)
    }

    fn lists(&self) -> usize {
        self.lists.load(Ordering::SeqCst)
    }

    fn put_conflicts(&self) -> usize {
        self.put_conflicts.load(Ordering::SeqCst)
    }

    fn age_listing(&self) {
        self.aged.store(true, Ordering::SeqCst);
    }

    fn set_clock_behind(&self, behind: Duration) {
        self.clock_behind_secs
            .store(behind.as_secs(), Ordering::SeqCst);
    }

    /// Shifts a write time the inner store reports onto this store's
    /// clock.
    fn on_store_clock(&self, meta: &mut ObjectMeta) {
        let behind = Duration::from_secs(self.clock_behind_secs.load(Ordering::SeqCst));
        meta.last_modified = (SystemTime::from(meta.last_modified) - behind).into();
    }
}

impl Display for Faulty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Faulty").finish()
    }
}

#[async_trait]
impl ObjectStore for Faulty {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        self.inner
            .put_opts(location, payload, opts)
            .await
            .inspect_err(|error| {
                if matches!(error, object_store::Error::Precondition { .. }) {
                    _ = self.put_conflicts.fetch_add(1, Ordering::SeqCst);
                }
            })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        tokio::task::yield_now().await;

        if location.as_ref().ends_with(".batch") {
            _ = self.batch_gets.fetch_add(1, Ordering::SeqCst);
        }

        let fault = self
            .armed
            .lock()
            .ok()
            .and_then(|mut armed| armed.take_if(|(path, _)| path == location))
            .map(|(_, fault)| fault);

        let source = || "injected by test".into();

        match fault {
            Some(Fault::NotFound) => Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: source(),
            }),
            Some(Fault::Unavailable) => Err(object_store::Error::Generic {
                store: "Faulty",
                source: source(),
            }),
            None => self
                .inner
                .get_opts(location, options)
                .await
                .map(|mut get_result| {
                    self.on_store_clock(&mut get_result.meta);
                    get_result
                }),
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        _ = self.lists.fetch_add(1, Ordering::SeqCst);

        let aged = self.aged.load(Ordering::SeqCst);
        let faulty = self.clone();

        self.inner
            .list(prefix)
            .map_ok(move |mut meta| {
                if aged {
                    meta.last_modified = (SystemTime::now() - 2 * BATCH_SETTLED_AFTER).into();
                }
                faulty.on_store_clock(&mut meta);
                meta
            })
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.inner.copy_opts(from, to, opts).await
    }
}

/// Writes three legacy batches, with the running max at offset 1, into a
/// partition whose index is incomplete.
async fn legacy_partition_with_max_at_offset_1(
    storage: &DynoStore,
    topic: &str,
) -> Result<Topition> {
    let topition = Topition::new(topic, 0);

    create_topic(storage, topic, 1).await?;

    write_legacy_batch(storage, &topition, 0, T0).await?;
    write_legacy_batch(storage, &topition, 1, T0 + 100).await?;
    write_legacy_batch(storage, &topition, 2, T0 + 50).await?;

    seed_watermark(storage, &topition, |w| {
        w.low = Some(0);
        w.high = Some(3);
        w.time_index = TimeIndex::default();
        Ok(())
    })
    .await?;

    Ok(topition)
}

async fn time_index_complete(storage: &DynoStore, topition: &Topition) -> Result<bool> {
    storage
        .watermark_for(topition)?
        .with(&storage.object_store, |w| Ok(w.time_index.is_complete()))
        .await
}

/// A produce into a legacy partition before its first lookup indexes its
/// batch against an empty live index, whatever the legacy batches hold.
/// The backfill's candidate is sparse, so its maximum (offset 1 here) has
/// no entry of its own. The merge must still drop the live entry (offset
/// 3), which is below that maximum: kept, it would be the floor of a lookup
/// between the two, and a scan from offset 3 would miss offset 1.
#[tokio::test]
async fn backfill_merge_drops_a_live_entry_below_an_unindexed_legacy_batch() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topition = legacy_partition_with_max_at_offset_1(&storage, "legacy-sparse").await?;

    assert_eq!(3, produce(&storage, &topition, &[T0 + 70]).await?);

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 80).await?;
    assert_eq!(Some(1), offset);
    assert_eq!(Some(T0 + 100), timestamp);

    assert_eq!(
        BTreeMap::from([(T0, 0)]),
        time_index_entries(&storage, &topition).await?
    );

    Ok(())
}

/// A backfill that cannot read a batch answers an error for the partition
/// and leaves the index incomplete, so the next request runs the backfill
/// again. An index committed without offset 1 would answer no match for
/// this target.
#[tokio::test]
async fn backfill_get_failure_leaves_the_index_incomplete() -> Result<()> {
    let _guard = init_tracing()?;
    let faulty = Faulty::default();
    let storage = DynoStore::new("nisshi", 111, faulty.clone())?;
    let topition = legacy_partition_with_max_at_offset_1(&storage, "backfill-failure").await?;

    faulty.arm(
        batch_path(&storage.cluster, &topition, 1),
        Fault::Unavailable,
    )?;

    assert_eq!(
        ErrorCode::KafkaStorageError,
        list_offsets_timestamp_error(&storage, &topition, T0 + 60).await?
    );
    assert!(!time_index_complete(&storage, &topition).await?);

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 60).await?;
    assert_eq!(Some(1), offset);
    assert_eq!(Some(T0 + 100), timestamp);
    assert!(time_index_complete(&storage, &topition).await?);

    Ok(())
}

/// Writes a `CreateTime` batch whose header claims `max_timestamp`, whatever
/// its records hold.
async fn write_batch_with_header(
    storage: &DynoStore,
    topition: &Topition,
    base_offset: i64,
    max_timestamp: i64,
    timestamps: &[i64],
) -> Result<()> {
    let base_timestamp = timestamps[0];
    let deltas = timestamps
        .iter()
        .map(|timestamp| timestamp - base_timestamp)
        .collect::<Vec<_>>();

    write_batch_object(
        storage,
        topition,
        deflated_batch::with_header(
            base_offset,
            TimestampType::CreateTime,
            base_timestamp,
            max_timestamp,
            &deltas,
        )?,
    )
    .await
}

/// Writes two batches whose headers are below their records, as an older
/// client wrote them before the broker rewrote the header: offset 0 claims
/// no timestamp and holds `T0`, and offsets 1 and 2 claim `T0 + 10` and hold
/// `T0 + 10` and `T0 + 100`.
async fn write_understated_batches(storage: &DynoStore, topition: &Topition) -> Result<()> {
    write_batch_with_header(storage, topition, 0, -1, &[T0]).await?;
    write_batch_with_header(storage, topition, 1, T0 + 10, &[T0 + 10, T0 + 100]).await
}

async fn time_index_max_timestamp(storage: &DynoStore, topition: &Topition) -> Result<i64> {
    storage
        .watermark_for(topition)?
        .with(&storage.object_store, |w| Ok(w.time_index.max_timestamp()))
        .await
}

/// A time index as a broker wrote it before the index took a batch's
/// maximum from its records.
fn index_from_headers(
    entries: &[(i64, i64)],
    max_timestamp: i64,
    complete: bool,
) -> Result<TimeIndex> {
    serde_json::from_value(serde_json::json!({
        "entries": entries.iter().copied().collect::<BTreeMap<i64, i64>>(),
        "max_timestamp": max_timestamp,
        "complete": complete,
    }))
    .map_err(Into::into)
}

/// Seeds a partition whose offset 0 has a header far above its record, and
/// whose offset 1 fills an index interval, under an index that a broker
/// built from those headers, complete or not. A lookup then checks that the
/// rebuild drops the header's entry and maximum. Kept, the old entry at
/// offset 0 would stay above every record, and offset 1 would get no entry.
async fn assert_overstated_index_is_rebuilt(topic: &str, complete: bool) -> Result<()> {
    let storage = storage()?;
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    write_batch_with_header(&storage, &topition, 0, T0 + 1_000, &[T0]).await?;
    write_batch_object(&storage, &topition, interval_batch(1, T0 + 100)?).await?;
    let from_headers = index_from_headers(&[(T0 + 1_000, 0)], T0 + 1_000, complete)?;
    seed_watermark(&storage, &topition, |w| {
        w.low = Some(0);
        w.high = Some(2);
        w.time_index = from_headers.clone();
        Ok(())
    })
    .await?;

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 50).await?;
    assert_eq!(Some(1), offset);
    assert_eq!(Some(T0 + 100), timestamp);

    assert!(time_index_complete(&storage, &topition).await?);
    assert_eq!(
        BTreeMap::from([(T0, 0), (T0 + 100, 1)]),
        time_index_entries(&storage, &topition).await?
    );
    assert_eq!(
        T0 + 100,
        time_index_max_timestamp(&storage, &topition).await?
    );

    Ok(())
}

/// The backfill takes each stored batch's maximum timestamp from its
/// records, not from its header, as Kafka's time index does after
/// `LogValidator` has rewritten the header. An index maximum from these
/// headers is `T0 + 10`, so a lookup for `T0 + 50` would answer no match,
/// although offset 2 is at `T0 + 100`.
#[tokio::test]
async fn backfill_takes_a_stored_batch_maximum_from_its_records() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "understated-headers";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    write_understated_batches(&storage, &topition).await?;
    seed_watermark(&storage, &topition, |w| {
        w.low = Some(0);
        w.high = Some(3);
        w.time_index = TimeIndex::default();
        Ok(())
    })
    .await?;

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 50).await?;
    assert_eq!(Some(2), offset);
    assert_eq!(Some(T0 + 100), timestamp);

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 - 5).await?;
    assert_eq!(Some(0), offset);
    assert_eq!(Some(T0), timestamp);

    assert!(time_index_complete(&storage, &topition).await?);
    assert_eq!(
        BTreeMap::from([(T0, 0)]),
        time_index_entries(&storage, &topition).await?
    );
    assert_eq!(
        T0 + 100,
        time_index_max_timestamp(&storage, &topition).await?
    );

    Ok(())
}

/// A header far above its records does not raise the index maximum, so a
/// later batch that raises the maximum of the records still gets an entry,
/// and a lookup scans from it instead of from offset 0.
#[tokio::test]
async fn batch_after_an_overstated_header_gets_an_index_entry() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "overstated-header";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    write_batch_with_header(&storage, &topition, 0, T0 + 1_000, &[T0]).await?;
    write_batch_object(&storage, &topition, interval_batch(1, T0 + 100)?).await?;
    seed_watermark(&storage, &topition, |w| {
        w.low = Some(0);
        w.high = Some(2);
        w.time_index = TimeIndex::default();
        Ok(())
    })
    .await?;

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 50).await?;
    assert_eq!(Some(1), offset);
    assert_eq!(Some(T0 + 100), timestamp);

    assert_eq!(
        BTreeMap::from([(T0, 0), (T0 + 100, 1)]),
        time_index_entries(&storage, &topition).await?
    );
    assert_eq!(
        T0 + 100,
        time_index_max_timestamp(&storage, &topition).await?
    );

    Ok(())
}

/// A broker that backfilled the index from the batch headers marked it
/// complete. The next lookup rebuilds it once from the records, so that a
/// target between a header and its record answers the record.
#[tokio::test]
async fn complete_index_built_from_understated_headers_is_rebuilt_from_records() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "understated-complete";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    write_understated_batches(&storage, &topition).await?;
    let from_headers = index_from_headers(&[(T0 + 10, 1)], T0 + 10, true)?;
    seed_watermark(&storage, &topition, |w| {
        w.low = Some(0);
        w.high = Some(3);
        w.time_index = from_headers.clone();
        Ok(())
    })
    .await?;

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 50).await?;
    assert_eq!(Some(2), offset);
    assert_eq!(Some(T0 + 100), timestamp);

    assert!(time_index_complete(&storage, &topition).await?);
    assert_eq!(
        BTreeMap::from([(T0, 0)]),
        time_index_entries(&storage, &topition).await?
    );
    assert_eq!(
        T0 + 100,
        time_index_max_timestamp(&storage, &topition).await?
    );

    Ok(())
}

#[tokio::test]
async fn complete_index_built_from_an_overstated_header_is_rebuilt_from_records() -> Result<()> {
    let _guard = init_tracing()?;
    assert_overstated_index_is_rebuilt("overstated-complete", true).await
}

/// A broker's backfill from the headers can also leave the index
/// incomplete, for a batch in flight during its listing. The rebuild drops
/// that index's entries and maximum too, because the merge would keep an
/// entry above every record.
#[tokio::test]
async fn incomplete_index_built_from_an_overstated_header_is_rebuilt_from_records() -> Result<()> {
    let _guard = init_tracing()?;
    assert_overstated_index_is_rebuilt("overstated-incomplete", false).await
}

/// The backfill takes the header of a batch whose records do not decode, so
/// that the index completes and a lookup below that batch still answers. A
/// lookup whose scan reaches the batch answers an error, because the batch
/// can hold the match.
#[tokio::test]
async fn backfill_takes_the_header_of_a_batch_that_does_not_decode() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "undecodable";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    write_batch_with_header(&storage, &topition, 0, T0, &[T0]).await?;

    // The attributes claim gzip, but the records are not compressed, so the
    // batch passes its CRC check and fails to inflate.
    let mut undecodable =
        deflated_batch::with_header(1, TimestampType::CreateTime, T0 + 100, T0 + 100, &[0])?;
    undecodable.attributes = BatchAttribute::default()
        .compression(Compression::Gzip)
        .into();
    undecodable.crc = undecodable.computed_crc();
    write_batch_object(&storage, &topition, undecodable).await?;

    seed_watermark(&storage, &topition, |w| {
        w.low = Some(0);
        w.high = Some(2);
        w.time_index = TimeIndex::default();
        Ok(())
    })
    .await?;

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0).await?;
    assert_eq!(Some(0), offset);
    assert_eq!(Some(T0), timestamp);

    assert!(time_index_complete(&storage, &topition).await?);
    assert_eq!(
        T0 + 100,
        time_index_max_timestamp(&storage, &topition).await?
    );

    assert_eq!(
        ErrorCode::KafkaStorageError,
        list_offsets_timestamp_error(&storage, &topition, T0 + 50).await?
    );

    Ok(())
}

/// A stored record whose `base_timestamp + timestamp_delta` overflows has
/// the wrapped timestamp, as Java's `long` addition gives a consumer. Here
/// it wraps below the target, so the scan passes it, and the backfill does
/// not take it as the batch's maximum. The broker rejects such a record at
/// produce, so only a batch stored before that check holds one.
#[tokio::test]
async fn record_timestamp_that_wraps_is_read_as_a_consumer_reads_it() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "overflow";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    write_batch_object(
        &storage,
        &topition,
        deflated_batch::with_header(
            0,
            TimestampType::CreateTime,
            T0,
            T0 + 100,
            &[i64::MAX, 0, 100],
        )?,
    )
    .await?;
    seed_watermark(&storage, &topition, |w| {
        w.low = Some(0);
        w.high = Some(3);
        w.time_index = TimeIndex::default();
        Ok(())
    })
    .await?;

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 50).await?;
    assert_eq!(Some(2), offset);
    assert_eq!(Some(T0 + 100), timestamp);
    assert_eq!(
        T0 + 100,
        time_index_max_timestamp(&storage, &topition).await?
    );

    Ok(())
}

/// A consumer reads a `LogAppendTime` batch's `max_timestamp` for each of
/// its records, whatever their deltas, so every lookup at or below it
/// answers the batch's first record at that timestamp, and the backfill
/// takes it as the batch's maximum.
#[tokio::test]
async fn log_append_time_batch_answers_every_record_at_its_max_timestamp() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "log-append-time";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    write_batch_object(
        &storage,
        &topition,
        deflated_batch::with_header(0, TimestampType::LogAppendTime, T0, T0 + 500, &[0, 50])?,
    )
    .await?;
    seed_watermark(&storage, &topition, |w| {
        w.low = Some(0);
        w.high = Some(2);
        w.time_index = TimeIndex::default();
        Ok(())
    })
    .await?;

    for target in [T0, T0 + 50, T0 + 500] {
        let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, target).await?;
        assert_eq!(Some(0), offset, "target {target}");
        assert_eq!(Some(T0 + 500), timestamp, "target {target}");
    }

    assert_eq!(
        (None, None),
        list_offsets_timestamp(&storage, &topition, T0 + 501).await?
    );
    assert_eq!(
        T0 + 500,
        time_index_max_timestamp(&storage, &topition).await?
    );

    Ok(())
}

/// A backfill skips a batch that was deleted after the LIST, and completes
/// the index.
#[tokio::test]
async fn backfill_skips_a_batch_deleted_after_listing() -> Result<()> {
    let _guard = init_tracing()?;
    let faulty = Faulty::default();
    let storage = DynoStore::new("nisshi", 111, faulty.clone())?;
    let topition = legacy_partition_with_max_at_offset_1(&storage, "backfill-not-found").await?;

    faulty.arm(batch_path(&storage.cluster, &topition, 2), Fault::NotFound)?;

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 60).await?;
    assert_eq!(Some(1), offset);
    assert_eq!(Some(T0 + 100), timestamp);
    assert!(time_index_complete(&storage, &topition).await?);

    Ok(())
}

/// A scan that cannot read a batch answers an error for the partition,
/// instead of answering a later offset from the next batch.
#[tokio::test]
async fn scan_get_failure_answers_an_error_for_the_partition() -> Result<()> {
    let _guard = init_tracing()?;
    let faulty = Faulty::default();
    let storage = DynoStore::new("nisshi", 111, faulty.clone())?;
    let topic = "scan-failure";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    assert_eq!(0, produce(&storage, &topition, &[T0]).await?);
    assert_eq!(1, produce(&storage, &topition, &[T0 + 50]).await?);
    assert_eq!(2, produce(&storage, &topition, &[T0 + 100]).await?);

    faulty.arm(
        batch_path(&storage.cluster, &topition, 1),
        Fault::Unavailable,
    )?;

    assert_eq!(
        ErrorCode::KafkaStorageError,
        list_offsets_timestamp_error(&storage, &topition, T0 + 25).await?
    );

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 25).await?;
    assert_eq!(Some(1), offset);
    assert_eq!(Some(T0 + 50), timestamp);

    Ok(())
}

/// A lookup that fails answers the error in its own partition's
/// `error_code`, as Kafka does, and the other partitions of the same
/// request still get their answers.
#[tokio::test]
async fn failed_lookup_answers_only_its_own_partition_with_an_error() -> Result<()> {
    let _guard = init_tracing()?;
    let faulty = Faulty::default();
    let storage = DynoStore::new("nisshi", 111, faulty.clone())?;
    let topic = "partial-failure";
    let failing = Topition::new(topic, 0);
    let healthy = Topition::new(topic, 1);

    create_topic(&storage, topic, 2).await?;
    assert_eq!(0, produce(&storage, &failing, &[T0]).await?);
    assert_eq!(0, produce(&storage, &healthy, &[T0]).await?);

    faulty.arm(
        batch_path(&storage.cluster, &failing, 0),
        Fault::Unavailable,
    )?;

    let responses = list_offsets_timestamp_responses(
        &storage,
        IsolationLevel::ReadUncommitted,
        &[(&failing, T0), (&healthy, T0)],
    )
    .await?;

    assert_eq!(ErrorCode::KafkaStorageError, responses[0].error_code);
    assert_eq!(None, responses[0].offset);
    assert_eq!(ErrorCode::None, responses[1].error_code);
    assert_eq!(Some(0), responses[1].offset);

    Ok(())
}

/// Concurrent lookups on one partition in one process share one backfill:
/// the backfill reads each of the three batches once, and each lookup's
/// scan then reads the one batch that holds its match.
#[tokio::test]
async fn concurrent_lookups_share_one_backfill() -> Result<()> {
    let _guard = init_tracing()?;
    let faulty = Faulty::default();
    let storage = DynoStore::new("nisshi", 111, faulty.clone())?;
    let topition = legacy_partition_with_max_at_offset_1(&storage, "single-flight").await?;

    let before = faulty.batch_gets();

    let (first, second) = tokio::join!(
        list_offsets_timestamp(&storage, &topition, T0),
        list_offsets_timestamp(&storage, &topition, T0),
    );
    assert_eq!((Some(0), Some(T0)), first?);
    assert_eq!((Some(0), Some(T0)), second?);

    assert_eq!(3 + 2, faulty.batch_gets() - before);

    Ok(())
}

/// `produce` assigns a batch's offset in the watermark document before it
/// writes the batch object, so a backfill's listing can see a later batch
/// and not an earlier one still in flight. Here offset 4 (T0 + 200) is in
/// flight, with no entry because it is inside the live index's interval,
/// and offset 5 (T0 + 150) is written. A candidate entry for offset 5
/// would be the floor of a lookup for T0 + 180, and a scan from offset 5
/// would miss offset 4 for good. The batches past the gap get no entry
/// instead, and the index is left incomplete, so the lookup after the
/// batch lands rebuilds it.
#[tokio::test]
async fn backfill_stops_its_listing_at_a_batch_in_flight() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "in-flight";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;

    // The interval batch at offset 1 fills the candidate's interval, so
    // offset 1 gets an entry.
    write_legacy_batch(&storage, &topition, 0, T0).await?;
    write_batch_object(&storage, &topition, interval_batch(1, T0 + 100)?).await?;
    write_legacy_batch(&storage, &topition, 2, T0 + 50).await?;

    seed_watermark(&storage, &topition, |w| {
        w.low = Some(0);
        w.high = Some(3);
        w.time_index = TimeIndex::default();
        Ok(())
    })
    .await?;

    // A produce by this binary before any lookup: the live index's first
    // entry.
    assert_eq!(3, produce(&storage, &topition, &[T0 + 10]).await?);

    // Offset 4 has assigned its offset and raised the live maximum without
    // an entry, and has not yet written its batch object.
    let in_flight = batch_with_timestamps(4, &[T0 + 200])?;
    let in_flight_bytes = u64::try_from(in_flight.batch_length)?;
    seed_watermark(&storage, &topition, |w| {
        w.high = Some(5);
        w.time_index.append(T0 + 200, 4, in_flight_bytes);
        Ok(())
    })
    .await?;

    // Offset 5 is written, and fills the candidate's interval since its
    // entry at offset 1.
    assert_eq!(
        5,
        storage
            .produce(None, &topition, interval_batch(0, T0 + 150)?)
            .await?
    );

    // The batch in flight is not readable yet, so no match is the answer
    // for now. The index stays incomplete, with no entry past the gap.
    let (offset, _) = list_offsets_timestamp(&storage, &topition, T0 + 180).await?;
    assert_eq!(None, offset);
    assert!(!time_index_complete(&storage, &topition).await?);
    assert_eq!(
        BTreeMap::from([(T0, 0), (T0 + 100, 1)]),
        time_index_entries(&storage, &topition).await?
    );

    write_batch_object(&storage, &topition, in_flight).await?;

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 180).await?;
    assert_eq!(Some(4), offset);
    assert_eq!(Some(T0 + 200), timestamp);
    assert!(time_index_complete(&storage, &topition).await?);

    Ok(())
}

/// A gap below a batch written longer than `BATCH_SETTLED_AFTER` ago is a
/// produce that failed between assigning its offset and writing its batch
/// object, and holds no records. The listing continues past it and the
/// index completes. The same gap below a batch written just now is taken
/// as in flight, and the index stays incomplete.
#[tokio::test]
async fn backfill_completes_past_a_settled_gap() -> Result<()> {
    let _guard = init_tracing()?;

    async fn partition_with_a_gap_at_offset_1(faulty: &Faulty) -> Result<(DynoStore, Topition)> {
        let storage = DynoStore::new("nisshi", 111, faulty.clone())?;
        let topic = "gap";
        let topition = Topition::new(topic, 0);

        create_topic(&storage, topic, 1).await?;

        write_legacy_batch(&storage, &topition, 0, T0).await?;
        write_legacy_batch(&storage, &topition, 2, T0 + 100).await?;

        seed_watermark(&storage, &topition, |w| {
            w.low = Some(0);
            w.high = Some(3);
            w.time_index = TimeIndex::default();
            Ok(())
        })
        .await?;

        Ok((storage, topition))
    }

    let settled = Faulty::default();
    let (storage, topition) = partition_with_a_gap_at_offset_1(&settled).await?;
    settled.age_listing();

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 50).await?;
    assert_eq!(Some(2), offset);
    assert_eq!(Some(T0 + 100), timestamp);
    assert!(time_index_complete(&storage, &topition).await?);

    let fresh = Faulty::default();
    let (storage, topition) = partition_with_a_gap_at_offset_1(&fresh).await?;

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 50).await?;
    assert_eq!(Some(2), offset);
    assert_eq!(Some(T0 + 100), timestamp);
    assert!(!time_index_complete(&storage, &topition).await?);

    Ok(())
}

/// The age of a batch below a gap is measured between two write times of
/// the store, the batch's and the watermark document's, not against the
/// broker's clock. Here the store's clock is behind the broker's by twice
/// `BATCH_SETTLED_AFTER`, and both batches were written just now on the
/// store's clock: the gap between them is in flight, and the index stays
/// incomplete. An age taken from the broker's clock would settle it.
#[tokio::test]
async fn settled_gap_age_is_measured_on_the_store_clock() -> Result<()> {
    let _guard = init_tracing()?;
    let faulty = Faulty::default();
    faulty.set_clock_behind(2 * BATCH_SETTLED_AFTER);
    let storage = DynoStore::new("nisshi", 111, faulty.clone())?;
    let topic = "store-clock";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;

    write_legacy_batch(&storage, &topition, 0, T0).await?;
    write_legacy_batch(&storage, &topition, 2, T0 + 100).await?;

    seed_watermark(&storage, &topition, |w| {
        w.low = Some(0);
        w.high = Some(3);
        w.time_index = TimeIndex::default();
        Ok(())
    })
    .await?;

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 50).await?;
    assert_eq!(Some(2), offset);
    assert_eq!(Some(T0 + 100), timestamp);
    assert!(!time_index_complete(&storage, &topition).await?);

    Ok(())
}

/// A gap after the last listed batch has no later batch to settle it,
/// however old the listed batches are, so the backfill leaves the index
/// incomplete and answers from the listed batches. The lookup after the
/// batch lands lists again and completes the index.
#[tokio::test]
async fn backfill_is_left_incomplete_by_a_gap_after_the_last_listed_batch() -> Result<()> {
    let _guard = init_tracing()?;
    let faulty = Faulty::default();
    let storage = DynoStore::new("nisshi", 111, faulty.clone())?;
    let topic = "trailing-gap";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;

    write_legacy_batch(&storage, &topition, 0, T0).await?;
    write_legacy_batch(&storage, &topition, 1, T0 + 100).await?;

    // Offset 2 has assigned its offset and not yet written its batch
    // object.
    seed_watermark(&storage, &topition, |w| {
        w.low = Some(0);
        w.high = Some(3);
        w.time_index = TimeIndex::default();
        Ok(())
    })
    .await?;
    faulty.age_listing();

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 50).await?;
    assert_eq!(Some(1), offset);
    assert_eq!(Some(T0 + 100), timestamp);
    assert!(!time_index_complete(&storage, &topition).await?);

    write_legacy_batch(&storage, &topition, 2, T0 + 200).await?;

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 150).await?;
    assert_eq!(Some(2), offset);
    assert_eq!(Some(T0 + 200), timestamp);
    assert!(time_index_complete(&storage, &topition).await?);

    Ok(())
}

/// A produce from another process between a backfill's listing and its
/// commit changes the watermark document under the committing process, so
/// the commit's conditional PUT fails. The commit then reads the document
/// again and merges against the index that the produce appended to, so
/// that produce's batch is not lost.
#[tokio::test]
async fn backfill_commit_merges_again_after_a_cas_conflict() -> Result<()> {
    let _guard = init_tracing()?;
    let faulty = Faulty::default();
    let committing = DynoStore::new("nisshi", 111, faulty.clone())?;
    let producing = DynoStore::new("nisshi", 111, faulty.clone())?;
    let topition = legacy_partition_with_max_at_offset_1(&committing, "cas-conflict").await?;

    let backfill = committing.begin_time_index_backfill(&topition).await?;
    let (candidate, _) = committing
        .collect_time_index_candidate(&topition, &backfill)
        .await?;
    assert!(candidate.is_complete());

    assert_eq!(3, produce(&producing, &topition, &[T0 + 200]).await?);

    let conflicts = faulty.put_conflicts();
    let merged = committing
        .commit_time_index_backfill(&topition, &candidate, backfill.token)
        .await?;
    assert_eq!(1, faulty.put_conflicts() - conflicts);

    assert!(merged.is_complete());
    assert_eq!(&BTreeMap::from([(T0, 0), (T0 + 200, 3)]), merged.entries());
    assert_eq!(T0 + 200, merged.max_timestamp());

    let (offset, timestamp) = list_offsets_timestamp(&committing, &topition, T0 + 150).await?;
    assert_eq!(Some(3), offset);
    assert_eq!(Some(T0 + 200), timestamp);

    Ok(())
}

/// A binary without the index rewrites the watermark document without it,
/// and with the batch it produced in neither the backfill's listing nor
/// the live index. A commit that finds its token gone leaves the index
/// incomplete, so the next lookup backfills again and finds that batch.
#[tokio::test]
async fn backfill_is_left_incomplete_by_an_older_broker_write() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topition = legacy_partition_with_max_at_offset_1(&storage, "older-broker").await?;

    let backfill = storage.begin_time_index_backfill(&topition).await?;
    let (candidate, _) = storage
        .collect_time_index_candidate(&topition, &backfill)
        .await?;
    assert!(candidate.is_complete());

    // The older broker's produce lands between the listing and the commit.
    write_legacy_batch(&storage, &topition, 3, T0 + 200).await?;
    seed_watermark(&storage, &topition, |w| {
        w.high = Some(4);
        w.time_index = TimeIndex::default();
        Ok(())
    })
    .await?;

    let merged = storage
        .commit_time_index_backfill(&topition, &candidate, backfill.token)
        .await?;
    assert!(!merged.is_complete());

    let (offset, timestamp) = list_offsets_timestamp(&storage, &topition, T0 + 150).await?;
    assert_eq!(Some(3), offset);
    assert_eq!(Some(T0 + 200), timestamp);
    assert!(time_index_complete(&storage, &topition).await?);

    Ok(())
}

/// The offset and timestamp that `storage` answers for `list_offset` of
/// `topition`, after checking that it answered without an error.
async fn list_offset_at(
    storage: &DynoStore,
    isolation_level: IsolationLevel,
    topition: &Topition,
    list_offset: ListOffset,
) -> Result<(Option<i64>, Option<SystemTime>)> {
    let responses = storage
        .list_offsets(isolation_level, &[(topition.to_owned(), list_offset)])
        .await?;

    assert_eq!(1, responses.len());
    assert_eq!(topition, &responses[0].0);
    assert_eq!(ErrorCode::None, responses[0].1.error_code);

    Ok((responses[0].1.offset, responses[0].1.timestamp))
}

/// Produces one batch of `records` records through `storage`, and returns
/// its base offset.
async fn produce_records(storage: &DynoStore, topition: &Topition, records: usize) -> Result<i64> {
    storage
        .produce(
            None,
            topition,
            batch_with_timestamps(0, &vec![T0; records])?,
        )
        .await
}

/// `Earliest` answers the log start and `Latest` the high watermark, both
/// read from the watermark alone: this partition has no batch object at
/// all. Kafka answers both without a timestamp.
#[tokio::test]
async fn earliest_and_latest_answer_from_the_watermark() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "watermark";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;

    seed_watermark(&storage, &topition, |watermark| {
        watermark.low = Some(2);
        watermark.high = Some(10);
        Ok(())
    })
    .await?;

    for isolation_level in [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
    ] {
        assert_eq!(
            (Some(2), None),
            list_offset_at(&storage, isolation_level, &topition, ListOffset::Earliest).await?
        );
        assert_eq!(
            (Some(10), None),
            list_offset_at(&storage, isolation_level, &topition, ListOffset::Latest).await?
        );
    }

    Ok(())
}

/// Two brokers share one bucket and produce multi-record batches to one
/// partition in turn. After each batch, the broker that produced it answers
/// `Latest` with every record so far, from both brokers. A third broker,
/// which has read nothing yet, answers the same. After `DeleteRecords`,
/// `Earliest` answers the cut.
#[tokio::test]
async fn brokers_sharing_a_bucket_answer_the_shared_watermark() -> Result<()> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let a = DynoStore::new("nisshi", 1, bucket.clone())?;
    let b = DynoStore::new("nisshi", 2, bucket.clone())?;

    let topic = "shared";
    let topition = Topition::new(topic, 0);

    create_topic(&a, topic, 1).await?;

    let mut high_watermark = 0;

    for (broker, records) in [(&a, 3), (&b, 2), (&a, 4), (&b, 1)] {
        assert_eq!(
            high_watermark,
            produce_records(broker, &topition, records).await?
        );
        high_watermark += i64::try_from(records)?;

        assert_eq!(
            (Some(high_watermark), None),
            list_offset_at(
                broker,
                IsolationLevel::ReadUncommitted,
                &topition,
                ListOffset::Latest
            )
            .await?
        );
    }

    let c = DynoStore::new("nisshi", 3, bucket.clone())?;
    assert_eq!(
        (Some(high_watermark), None),
        list_offset_at(
            &c,
            IsolationLevel::ReadUncommitted,
            &topition,
            ListOffset::Latest
        )
        .await?
    );

    let cut = 4;
    let results = a
        .delete_records(&[DeleteRecordsTopic::default()
            .name(topic.into())
            .partitions(Some(
                [DeleteRecordsPartition::default()
                    .partition_index(0)
                    .offset(cut)]
                .into(),
            ))])
        .await?;
    let partitions = results[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
    assert_eq!(cut, partitions[0].low_watermark);

    let d = DynoStore::new("nisshi", 4, bucket)?;

    for broker in [&a, &d] {
        assert_eq!(
            (Some(cut), None),
            list_offset_at(
                broker,
                IsolationLevel::ReadUncommitted,
                &topition,
                ListOffset::Earliest
            )
            .await?
        );
    }

    Ok(())
}

/// `Earliest` and `Latest` read the watermark and never list the
/// partition's batch objects, so their cost does not grow with the
/// partition.
#[tokio::test]
async fn earliest_and_latest_do_not_list_batch_objects() -> Result<()> {
    let _guard = init_tracing()?;
    let faulty = Faulty::default();
    let storage = DynoStore::new("nisshi", 111, faulty.clone())?;
    let topic = "no-listing";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;

    for records in [3, 2] {
        _ = produce_records(&storage, &topition, records).await?;
    }

    let lists = faulty.lists();

    for isolation_level in [
        IsolationLevel::ReadUncommitted,
        IsolationLevel::ReadCommitted,
    ] {
        assert_eq!(
            (Some(0), None),
            list_offset_at(&storage, isolation_level, &topition, ListOffset::Earliest).await?
        );
        assert_eq!(
            (Some(5), None),
            list_offset_at(&storage, isolation_level, &topition, ListOffset::Latest).await?
        );
    }

    assert_eq!(lists, faulty.lists());

    Ok(())
}

/// A store failure while `Earliest` or `Latest` reads one partition's
/// watermark answers `KAFKA_STORAGE_ERROR`, which a client retries, in that
/// partition only. The other partitions of the request still get their
/// offsets.
#[tokio::test]
async fn watermark_read_failure_answers_an_error_for_the_partition() -> Result<()> {
    let _guard = init_tracing()?;
    let faulty = Faulty::default();
    let writer = DynoStore::new("nisshi", 1, faulty.clone())?;
    let topic = "watermark-failure";

    create_topic(&writer, topic, 2).await?;

    for partition in 0..2 {
        _ = produce_records(&writer, &Topition::new(topic, partition), 3).await?;
    }

    // A broker that has read nothing yet sends its watermark GETs to the
    // store, past the cache of conditional GETs.
    let reader = DynoStore::new("nisshi", 2, faulty.clone())?;
    let failing = Topition::new(topic, 1);

    faulty.arm(
        reader.watermark_for(&failing)?.location().clone(),
        Fault::Unavailable,
    )?;

    for list_offset in [ListOffset::Earliest, ListOffset::Latest] {
        let responses = reader
            .list_offsets(
                IsolationLevel::ReadUncommitted,
                &[
                    (Topition::new(topic, 0), list_offset),
                    (failing.clone(), list_offset),
                ],
            )
            .await?;

        assert_eq!(2, responses.len());

        assert_eq!(
            ErrorCode::None,
            responses[0].1.error_code,
            "{list_offset:?}"
        );
        assert!(responses[0].1.offset.is_some(), "{list_offset:?}");

        assert_eq!(
            ErrorCode::KafkaStorageError,
            responses[1].1.error_code,
            "{list_offset:?}"
        );
        assert_eq!(None, responses[1].1.offset, "{list_offset:?}");

        faulty.arm(
            reader.watermark_for(&failing)?.location().clone(),
            Fault::Unavailable,
        )?;
    }

    Ok(())
}

/// Under READ_COMMITTED, `Latest` answers the last stable offset: the
/// first offset of the earliest open transaction on the partition. Under
/// READ_UNCOMMITTED it answers the high watermark.
#[tokio::test]
async fn read_committed_latest_answers_the_last_stable_offset() -> Result<()> {
    let _guard = init_tracing()?;
    let storage = storage()?;
    let topic = "latest-open-transaction";
    let topition = Topition::new(topic, 0);

    create_topic(&storage, topic, 1).await?;
    assert_eq!(0, produce_records(&storage, &topition, 2).await?);
    assert_eq!(2, produce_records(&storage, &topition, 3).await?);

    storage
        .meta
        .with_mut(&storage.object_store, |meta| {
            _ = meta.transactions.insert(
                "txn".into(),
                Txn {
                    producer: 1,
                    epochs: BTreeMap::from([(
                        0,
                        TxnDetail {
                            state: Some(TxnState::Begin),
                            produces: BTreeMap::from([(
                                topic.into(),
                                BTreeMap::from([(
                                    0,
                                    Some(TxnProduceOffset {
                                        offset_start: 2,
                                        offset_end: 4,
                                    }),
                                )]),
                            )]),
                            ..Default::default()
                        },
                    )]),
                },
            );
            Ok(())
        })
        .await?;

    assert_eq!(
        (Some(2), None),
        list_offset_at(
            &storage,
            IsolationLevel::ReadCommitted,
            &topition,
            ListOffset::Latest
        )
        .await?
    );
    assert_eq!(
        (Some(5), None),
        list_offset_at(
            &storage,
            IsolationLevel::ReadUncommitted,
            &topition,
            ListOffset::Latest
        )
        .await?
    );

    Ok(())
}
