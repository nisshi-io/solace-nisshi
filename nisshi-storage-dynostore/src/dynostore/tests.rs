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

use dotenv::dotenv;
use nisshi_storage::{Error, Result, Topition};
use object_store::{
    memory::InMemory,
    path::{Path, PathPart},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs::File, sync::Arc, thread};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

use super::{DynoStore, EMPTY_GROUP_SENTINEL, decode_group_segment, group_path_part};

mod concurrent_retry;
mod delete_records;
mod describe_groups;
mod epoch_fence;
mod failing_object_store;
mod fetch_listing;
mod held_put_store;
mod init_producer;
mod lake_sink_idempotence;
mod latency;
mod list_groups;
mod list_offsets;
mod list_offsets_storage_error;
mod metadata_read_error;
mod offset_commit_unknown_partition;
mod ping;
mod post_write_failure_reason;
mod produce_topic_guard;
mod produce_write_failure;
mod producer_detail;
mod stale_watermark;
mod transaction_last_stable;
mod unknown_partition_guard;

pub(crate) fn init_tracing() -> Result<DefaultGuard, Error> {
    _ = dotenv().ok();

    Ok(tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
            )
            .with_writer(
                thread::current()
                    .name()
                    .ok_or(Error::Message(String::from("unnamed thread")))
                    .and_then(|name| {
                        File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                            .map_err(Into::into)
                    })
                    .map(Arc::new)?,
            )
            .finish(),
    ))
}

#[test]
fn range_check() {
    let map = BTreeMap::from([(3, "a"), (5, "b"), (8, "c")]);

    assert_eq!(Some((&3, &"a")), map.range(2..).next());
    assert_eq!(Some((&5, &"b")), map.range(4..).next());
    assert_eq!(None, map.range(9..).next());
}

#[test]
fn schema_change() -> Result<()> {
    #[derive(
        Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    struct X0 {
        low: Option<i64>,
        high: Option<i64>,
    }

    let low = Some(6);
    let high = Some(66);

    let x0 = X0 { low, high };

    let encoded = serde_json::to_string(&x0)?;

    #[derive(
        Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    struct X1 {
        low: Option<i64>,
        high: Option<i64>,
        timestamps: Option<BTreeMap<i64, i64>>,
    }

    let x1: X1 = serde_json::from_str(&encoded[..])?;

    assert_eq!(low, x1.low);
    assert_eq!(high, x1.high);
    assert!(x1.timestamps.is_none());

    Ok(())
}

/// A group id that is the literal string "%empty" is non-empty, so it goes
/// through ordinary `PathPart` encoding rather than the sentinel. `%` is in
/// `object_store`'s reserved/escaped character set, so the leading `%` gets
/// escaped to `%25`, producing a segment distinct from `EMPTY_GROUP_SENTINEL`
/// itself. This is the empirical fact `EMPTY_GROUP_SENTINEL`'s safety argument
/// depends on, confirmed directly here rather than just inferred.
#[test]
fn percent_empty_literal_is_escaped_as_percent25empty() {
    let part: PathPart<'_> = "%empty".into();
    assert_eq!("%25empty", part.as_ref());
}

#[test]
fn empty_group_id_encodes_as_sentinel() -> Result<()> {
    assert_eq!(EMPTY_GROUP_SENTINEL, group_path_part("")?.as_ref());
    Ok(())
}

#[test]
fn literal_percent_empty_group_id_does_not_collide_with_sentinel() -> Result<()> {
    let sentinel_segment = group_path_part("")?;
    let literal_segment = group_path_part("%empty")?;

    assert_ne!(sentinel_segment.as_ref(), literal_segment.as_ref());
    assert_eq!(EMPTY_GROUP_SENTINEL, sentinel_segment.as_ref());
    assert_eq!("%25empty", literal_segment.as_ref());
    Ok(())
}

#[test]
fn group_path_part_round_trips_through_decode_group_segment() -> Result<()> {
    for group_id in [
        "", "a", "a/b", "/", "//", "a/", "%empty", ".", "..", "a#b", "café",
    ] {
        let encoded = group_path_part(group_id)?;
        let decoded = decode_group_segment(encoded.as_ref()).expect("encoded segment must decode");
        assert_eq!(group_id, decoded, "round trip failed for {group_id:?}");
    }
    Ok(())
}

#[test]
fn group_path_part_gives_every_id_a_distinct_segment() -> Result<()> {
    let ids = [
        "", "a", "a/b", "/", "//", "a/", "%empty", ".", "..", "a#b", "café",
    ];

    for (i, a) in ids.iter().enumerate() {
        for (j, b) in ids.iter().enumerate() {
            let segments_equal = group_path_part(a)?.as_ref() == group_path_part(b)?.as_ref();
            assert_eq!(i == j, segments_equal, "{a:?} vs {b:?}");
        }
    }
    Ok(())
}

#[test]
fn decode_group_segment_rejects_invalid_utf8() {
    // 0x80 alone is not valid UTF-8, and is not the percent-encoded sentinel.
    assert_eq!(None, decode_group_segment("%80"));
}

/// Existing data is stored under these keys and is not migrated, so a group
/// id without a `/` (other than `.` and `..`, whose state file moved) must
/// keep the exact key it had when keys were built with
/// `Path::from(format!(..))` from these templates.
#[test]
fn group_keys_are_stable_for_ids_without_a_slash() -> Result<()> {
    let storage = DynoStore::new("c", 111, InMemory::new())?;
    let topition = Topition::new("t", 3);

    assert_eq!(
        "clusters/c/groups/consumers/grp/offsets/t/partitions/0000000003.json",
        storage
            .committed_offset_location("grp", &topition)?
            .as_ref()
    );
    assert_eq!(
        "clusters/c/groups/consumers/grp.json",
        storage.group_state_location("grp")?.as_ref()
    );

    for group_id in ["grp", "a#b", "a%b", "a b", "café", "%empty"] {
        assert_eq!(
            Path::from(format!(
                "clusters/c/groups/consumers/{group_id}/offsets/t/partitions/0000000003.json"
            )),
            storage.committed_offset_location(group_id, &topition)?,
            "committed offset key moved for {group_id:?}"
        );
        assert_eq!(
            Path::from(format!("clusters/c/groups/consumers/{group_id}.json")),
            storage.group_state_location(group_id)?,
            "group state key moved for {group_id:?}"
        );
    }
    Ok(())
}

/// Only the group id became one segment. The topic keeps the `Path::from`
/// rule of its data keys, so a topic named before CreateTopics rejected "/"
/// keeps the committed offset key it had, which `delete_topic` matches on.
#[test]
fn committed_offset_key_keeps_the_topic_segment_rule() -> Result<()> {
    let storage = DynoStore::new("c", 111, InMemory::new())?;

    for topic in ["t", "a/b", "a#b", "café"] {
        assert_eq!(
            Path::from(format!(
                "clusters/c/groups/consumers/grp/offsets/{topic}/partitions/0000000003.json"
            )),
            storage.committed_offset_location("grp", &Topition::new(topic, 3))?,
            "committed offset key moved for topic {topic:?}"
        );
    }
    Ok(())
}

/// Checked-in version of the serde-compatibility claim in
/// [`super::Watermark::time_index`]'s doc comment: every shape of
/// pre-time-index watermark document on disk decodes cleanly into the
/// current [`super::Watermark`], with an empty, incomplete time index.
#[test]
fn watermark_decodes_pre_time_index_documents() -> Result<()> {
    use super::{Watermark, time_index::TimeIndex};

    // Every watermark document written before the time index has
    // `timestamps` explicitly `null`.
    let null_timestamps: Watermark =
        serde_json::from_str(r#"{"low":6,"high":66,"timestamps":null}"#)?;
    assert_eq!(Some(6), null_timestamps.low);
    assert_eq!(Some(66), null_timestamps.high);
    assert_eq!(TimeIndex::default(), null_timestamps.time_index);
    assert!(!null_timestamps.time_index.is_complete());

    // Documents written before the `timestamps` key existed at all lack it
    // entirely.
    let key_omitted: Watermark = serde_json::from_str(r#"{"low":6,"high":66}"#)?;
    assert_eq!(Some(6), key_omitted.low);
    assert_eq!(Some(66), key_omitted.high);
    assert_eq!(TimeIndex::default(), key_omitted.time_index);

    // The current code writes no `timestamps` key, and a binary without the
    // time index, whose watermark has the shape of `Legacy`, reads the
    // document it writes.
    #[derive(Deserialize)]
    struct Legacy {
        low: Option<i64>,
        high: Option<i64>,
        timestamps: Option<BTreeMap<i64, i64>>,
    }

    let written = serde_json::to_string(&Watermark {
        low: Some(6),
        high: Some(66),
        time_index: TimeIndex::complete(),
        producer_epochs: BTreeMap::new(),
    })?;
    assert!(!written.contains("timestamps"), "{written}");
    let round_trip: Watermark = serde_json::from_str(&written)?;
    assert!(round_trip.time_index.is_complete());
    let legacy: Legacy = serde_json::from_str(&written)?;
    assert_eq!(
        (Some(6), Some(66), None),
        (legacy.low, legacy.high, legacy.timestamps)
    );

    Ok(())
}

#[test]
fn watermark_without_producer_epochs_decodes_to_an_empty_map() -> Result<()> {
    use super::Watermark;

    let watermark: Watermark = serde_json::from_str(r#"{"low":6,"high":66}"#)?;
    assert!(watermark.producer_epochs.is_empty());

    let written = serde_json::to_string(&watermark)?;
    assert!(!written.contains("producer_epochs"), "{written}");

    Ok(())
}

#[test]
fn watermark_producer_epochs_round_trip() -> Result<()> {
    use super::Watermark;

    let mut watermark = Watermark::default();
    watermark.record_producer_epoch(7, 3);

    let round_trip: Watermark = serde_json::from_str(&serde_json::to_string(&watermark)?)?;
    assert_eq!(BTreeMap::from([(7, 3)]), round_trip.producer_epochs);

    Ok(())
}

#[test]
fn watermark_records_only_a_newer_epoch_above_zero() {
    use super::Watermark;

    let mut watermark = Watermark::default();

    watermark.record_producer_epoch(7, 0);
    assert!(watermark.producer_epochs.is_empty());

    watermark.record_producer_epoch(7, 3);
    watermark.record_producer_epoch(7, 2);
    assert_eq!(BTreeMap::from([(7, 3)]), watermark.producer_epochs);
}

#[test]
fn watermark_fences_only_an_older_epoch() {
    use super::Watermark;
    use nisshi_storage::OLDER_EPOCH_ERROR;
    use std::assert_matches;

    let mut watermark = Watermark::default();
    watermark.record_producer_epoch(7, 3);

    assert_matches!(
        watermark.fence_producer_epoch(7, 2),
        Err(Error::Api(error)) if error == OLDER_EPOCH_ERROR
    );
    assert_matches!(watermark.fence_producer_epoch(7, 3), Ok(()));
    assert_matches!(watermark.fence_producer_epoch(7, 4), Ok(()));

    // The partition holds no epoch for producer 8, so it fences none of its batches.
    assert_matches!(watermark.fence_producer_epoch(8, 0), Ok(()));
}

#[test]
fn batch_is_ahead_only_of_the_next_epoch_that_started_after_it() {
    use super::{ProduceEpoch, ProducerDetail};

    let topition = Topition::new("t", 0);
    let mut producer = ProducerDetail::default();

    producer.advance(0, ProduceEpoch::Start, &topition, 0, 0, 0);
    producer.advance(1, ProduceEpoch::Start, &topition, 0, 0, 5);

    assert!(producer.is_ahead_of_next_epoch(0, &topition, 4));
    assert!(!producer.is_ahead_of_next_epoch(0, &topition, 6));

    // Epoch 2 starting forgets where epoch 1 started, so a batch at epoch 0 can no longer be
    // shown to be ahead of the epoch 1 batches.
    producer.advance(2, ProduceEpoch::Start, &topition, 0, 0, 9);

    assert!(!producer.is_ahead_of_next_epoch(0, &topition, 4));
    assert!(producer.is_ahead_of_next_epoch(1, &topition, 8));
}

#[test]
fn producer_that_never_bumps_records_no_epoch_start() -> Result<()> {
    use super::{ProduceEpoch, ProducerDetail};

    let mut producer = ProducerDetail::default();
    producer.advance(0, ProduceEpoch::Start, &Topition::new("t", 0), 0, 0, 0);

    let written = serde_json::to_string(&producer)?;
    assert!(!written.contains("epoch_starts"), "{written}");

    Ok(())
}

#[test]
fn watermark_forgets_the_smallest_producer_id_over_the_limit() -> Result<()> {
    use super::Watermark;

    let mut watermark = Watermark::default();

    let limit = i64::try_from(Watermark::MAX_FENCED_PRODUCERS)?;
    for producer_id in 1..=limit {
        watermark.record_producer_epoch(producer_id, 1);
    }
    assert_eq!(
        Watermark::MAX_FENCED_PRODUCERS,
        watermark.producer_epochs.len()
    );

    watermark.record_producer_epoch(limit + 1, 1);

    assert_eq!(
        Watermark::MAX_FENCED_PRODUCERS,
        watermark.producer_epochs.len()
    );
    assert!(!watermark.producer_epochs.contains_key(&1));
    assert!(watermark.producer_epochs.contains_key(&2));
    assert!(watermark.producer_epochs.contains_key(&(limit + 1)));

    Ok(())
}
