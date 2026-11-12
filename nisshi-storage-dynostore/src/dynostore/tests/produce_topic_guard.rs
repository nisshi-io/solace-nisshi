// Copyright ⓒ Solace Corporation
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

//! Produce answers `UNKNOWN_TOPIC_OR_PARTITION` for a topic or partition that
//! does not exist, and writes nothing. A topic's keys hold its name as one
//! path segment, so a name containing "/" cannot reach another topic's keys,
//! and listing a partition skips any object that is not one of its batches.
//!
//! These tests use only the `Storage` trait and the raw bucket, so they
//! compile against the code before the fix and fail there at runtime.

use nisshi_storage::fetched::FetchUntilDeadline as _;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::time::Instant;

use bytes::Bytes;
use futures::TryStreamExt as _;
use nisshi_sans_io::{
    BatchAttribute, ErrorCode, IsolationLevel, ListOffset,
    add_partitions_to_txn_request::AddPartitionsToTxnTopic,
    create_topics_request::CreatableTopic,
    record::{Record, deflated, inflated},
    topic::is_valid_topic_name,
};
use nisshi_storage::{Error, Storage as _, TopicId, Topition, TxnAddPartitionsRequest};
use object_store::{
    ObjectStore as _, ObjectStoreExt as _, PutPayload,
    memory::InMemory,
    path::{Path, PathPart},
};

use crate::dynostore::{DynoStore, tests::init_tracing};

/// A name that, split on "/", lands inside partition 0 of topic `victim`.
const CRAFTED: &str = "victim/partitions/0000000000/records/x";

async fn create(storage: &DynoStore, name: &str, partitions: i32) -> Result<(), Error> {
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(partitions)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(())
}

fn batch() -> Result<deflated::Batch, Error> {
    inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"abc"))))
        .build()
        .and_then(TryInto::try_into)
        .map_err(Into::into)
}

/// Every key in the bucket, sorted.
async fn keys(bucket: &InMemory) -> Result<Vec<String>, Error> {
    let mut keys = bucket
        .list(None)
        .map_ok(|meta| meta.location.to_string())
        .try_collect::<Vec<_>>()
        .await?;

    keys.sort();

    Ok(keys)
}

async fn fetch_base_offsets(storage: &DynoStore, topition: &Topition) -> Result<Vec<i64>, Error> {
    Ok(storage
        .fetch_until_deadline(
            topition,
            0,
            1,
            1024 * 1024,
            IsolationLevel::ReadUncommitted,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .map(|read| read.batches)?
        .iter()
        .map(|batch| batch.base_offset)
        .collect())
}

async fn list_offset(
    storage: &DynoStore,
    topition: &Topition,
    request: ListOffset,
) -> Result<Option<i64>, Error> {
    let responses = storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(topition.clone(), request)],
        )
        .await?;

    assert_eq!(ErrorCode::None, responses[0].1.error_code);

    Ok(responses[0].1.offset)
}

fn assert_unknown_topic_or_partition(outcome: &Result<i64, Error>) {
    assert!(
        matches!(outcome, Err(Error::Api(ErrorCode::UnknownTopicOrPartition))),
        "expected UNKNOWN_TOPIC_OR_PARTITION, got {outcome:?}"
    );
}

#[tokio::test]
async fn produce_to_unknown_topic_writes_nothing() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let storage = DynoStore::new("nisshi", 1, bucket.clone())?;

    let before = keys(&bucket).await?;

    assert_unknown_topic_or_partition(
        &storage
            .produce(None, &Topition::new("never-created", 0), batch()?)
            .await,
    );

    assert_eq!(before, keys(&bucket).await?);

    Ok(())
}

#[tokio::test]
async fn produce_to_unknown_partition_index_writes_nothing() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let storage = DynoStore::new("nisshi", 1, bucket.clone())?;

    create(&storage, "abc", 2).await?;

    let before = keys(&bucket).await?;

    for index in [2, -1] {
        assert_unknown_topic_or_partition(
            &storage
                .produce(None, &Topition::new("abc", index), batch()?)
                .await,
        );
    }

    assert_eq!(before, keys(&bucket).await?);

    Ok(())
}

/// A produce to a name containing "/" no longer writes into the partition of
/// the topic the name starts with, so that partition's Fetch still works.
#[tokio::test]
async fn produce_to_slash_name_does_not_break_fetch() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let storage = DynoStore::new("nisshi", 1, bucket.clone())?;

    create(&storage, "victim", 1).await?;
    let victim = Topition::new("victim", 0);
    assert_eq!(0, storage.produce(None, &victim, batch()?).await?);

    assert_unknown_topic_or_partition(
        &storage
            .produce(None, &Topition::new(CRAFTED, 0), batch()?)
            .await,
    );

    assert_eq!(vec![0], fetch_base_offsets(&storage, &victim).await?);

    Ok(())
}

/// ListOffsets Latest answers the partition's own high offset after a
/// produce to a name containing "/", not one past a batch that produce
/// left under the partition's keys.
#[tokio::test]
async fn produce_to_slash_name_does_not_move_latest() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let storage = DynoStore::new("nisshi", 1, bucket.clone())?;

    create(&storage, "victim", 1).await?;
    let victim = Topition::new("victim", 0);

    for expected in 0..3 {
        assert_eq!(expected, storage.produce(None, &victim, batch()?).await?);
    }

    _ = storage
        .produce(None, &Topition::new(CRAFTED, 0), batch()?)
        .await;

    assert_eq!(
        Some(3),
        list_offset(&storage, &victim, ListOffset::Latest).await?
    );

    Ok(())
}

/// A topic whose name contains "/" can exist if it was created before
/// CreateTopics checked names. Its keys stay under its own single segment.
#[tokio::test]
async fn existing_slash_topic_keys_stay_in_their_own_segment() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let storage = DynoStore::new("nisshi", 1, bucket.clone())?;

    create(&storage, "victim", 1).await?;
    let victim = Topition::new("victim", 0);
    assert_eq!(0, storage.produce(None, &victim, batch()?).await?);
    assert_eq!(1, storage.produce(None, &victim, batch()?).await?);

    create(&storage, CRAFTED, 1).await?;
    let crafted = Topition::new(CRAFTED, 0);
    assert_eq!(0, storage.produce(None, &crafted, batch()?).await?);

    assert_eq!(vec![0, 1], fetch_base_offsets(&storage, &victim).await?);

    assert_eq!(
        Some(2),
        list_offset(&storage, &victim, ListOffset::Latest).await?
    );

    assert_eq!(vec![0], fetch_base_offsets(&storage, &crafted).await?);

    Ok(())
}

/// Objects that a produce to a name containing "/" left under a partition's
/// `records/` before this fix, and objects there not named as a batch, are
/// skipped by ListOffsets and Fetch.
#[tokio::test]
async fn stray_objects_under_records_are_skipped() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let storage = DynoStore::new("nisshi", 1, bucket.clone())?;

    create(&storage, "victim", 1).await?;
    let victim = Topition::new("victim", 0);

    let before_batches = SystemTime::now() - Duration::from_secs(60);
    assert_eq!(0, storage.produce(None, &victim, batch()?).await?);
    assert_eq!(1, storage.produce(None, &victim, batch()?).await?);

    tokio::time::sleep(Duration::from_millis(20)).await;
    let before_strays = SystemTime::now();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let records = "clusters/nisshi/topics/victim/partitions/0000000000/records";
    let stray = format!("{records}/x/partitions/0000000000");

    for key in [
        // left by a produce to "victim/partitions/0000000000/records/x"
        format!("{stray}/watermark.json"),
        format!("{stray}/records/00000000000000000005.batch"),
        // directly under records/, but not named as a batch is
        format!("{records}/5.batch"),
        format!("{records}/-0000000000000000005.batch"),
        format!("{records}/00000000000000000005.json"),
    ] {
        _ = bucket
            .put(&Path::from(key), PutPayload::from_static(b"{}"))
            .await?;
    }

    assert_eq!(
        Some(0),
        list_offset(&storage, &victim, ListOffset::Timestamp(before_batches)).await?
    );

    assert_eq!(
        None,
        list_offset(&storage, &victim, ListOffset::Timestamp(before_strays)).await?
    );

    assert_eq!(
        Some(2),
        list_offset(&storage, &victim, ListOffset::Latest).await?
    );

    assert_eq!(vec![0, 1], fetch_base_offsets(&storage, &victim).await?);

    Ok(())
}

/// Keys of a topic with a valid name are the same as before the fix.
#[tokio::test]
async fn valid_topic_keys_do_not_move() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let storage = DynoStore::new("nisshi", 1, bucket.clone())?;

    let name = "a.b_c-D9";
    create(&storage, name, 1).await?;
    assert_eq!(
        0,
        storage
            .produce(None, &Topition::new(name, 0), batch()?)
            .await?
    );

    let keys = keys(&bucket).await?;

    for expected in [
        "clusters/nisshi/topics/a.b_c-D9/partitions/0000000000/watermark.json",
        "clusters/nisshi/topics/a.b_c-D9/partitions/0000000000/records/00000000000000000000.batch",
    ] {
        assert!(
            keys.iter().any(|key| key == expected),
            "{expected} not in {keys:?}"
        );
    }

    Ok(())
}

/// Every character a valid topic name may hold is left as is by `PathPart`,
/// so a valid name is its own key segment.
#[test]
fn valid_topic_name_characters_encode_to_themselves() {
    let characters = (b'a'..=b'z')
        .chain(b'A'..=b'Z')
        .chain(b'0'..=b'9')
        .chain(*b"._-")
        .map(char::from)
        .collect::<String>();

    let longest = characters.repeat(4)[..249].to_owned();

    // "." alone is not a valid name, so each character is tried after an "a".
    for name in characters
        .chars()
        .map(|character| format!("a{character}"))
        .chain([characters.clone(), longest])
    {
        assert!(is_valid_topic_name(&name), "{name}");
        assert_eq!(name.as_str(), PathPart::from(name.as_str()).as_ref());
    }
}

/// Deleting a topic whose name contains "/" removes its data, which now
/// sits under one segment, and leaves the topic its name starts with alone.
#[tokio::test]
async fn delete_slash_topic_removes_its_data_only() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let storage = DynoStore::new("nisshi", 1, bucket.clone())?;

    create(&storage, "victim", 1).await?;
    let victim = Topition::new("victim", 0);
    assert_eq!(0, storage.produce(None, &victim, batch()?).await?);

    let before = keys(&bucket)
        .await?
        .into_iter()
        .filter(|key| !key.ends_with("/meta.json"))
        .collect::<Vec<_>>();

    let name = "victim/x";
    create(&storage, name, 1).await?;
    assert_eq!(
        0,
        storage
            .produce(None, &Topition::new(name, 0), batch()?)
            .await?
    );

    assert_eq!(
        ErrorCode::None,
        storage.delete_topic(&TopicId::Name(name.into())).await?
    );

    let after = keys(&bucket)
        .await?
        .into_iter()
        .filter(|key| !key.ends_with("/meta.json"))
        .collect::<Vec<_>>();

    assert_eq!(before, after);
    assert_eq!(vec![0], fetch_base_offsets(&storage, &victim).await?);

    Ok(())
}

/// A topic with the empty name can exist if it was created before
/// CreateTopics checked names. Its keys are the ones it had before topic
/// keys were one segment, and deleting it leaves its data in place, because
/// its prefix would be every topic's data.
#[tokio::test]
async fn empty_topic_name_keeps_its_keys_and_delete_leaves_data() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let storage = DynoStore::new("nisshi", 1, bucket.clone())?;

    create(&storage, "victim", 1).await?;
    let victim = Topition::new("victim", 0);
    assert_eq!(0, storage.produce(None, &victim, batch()?).await?);

    create(&storage, "", 1).await?;
    assert_eq!(
        0,
        storage
            .produce(None, &Topition::new("", 0), batch()?)
            .await?
    );

    let before = keys(&bucket)
        .await?
        .into_iter()
        .filter(|key| !key.ends_with("/meta.json"))
        .collect::<Vec<_>>();

    for expected in [
        "clusters/nisshi/topics/partitions/0000000000/watermark.json",
        "clusters/nisshi/topics/partitions/0000000000/records/00000000000000000000.batch",
    ] {
        assert!(
            before.iter().any(|key| key == expected),
            "{expected} not in {before:?}"
        );
    }

    assert_eq!(
        ErrorCode::None,
        storage.delete_topic(&TopicId::Name("".into())).await?
    );

    let after = keys(&bucket)
        .await?
        .into_iter()
        .filter(|key| !key.ends_with("/meta.json"))
        .collect::<Vec<_>>();

    assert_eq!(before, after);
    assert_eq!(vec![0], fetch_base_offsets(&storage, &victim).await?);

    Ok(())
}

/// A transaction that produced to a topic deleted before EndTxn still ends:
/// the deleted partition gets no marker, and the surviving partition's last
/// stable offset reaches its high watermark.
#[tokio::test]
async fn txn_end_skips_a_topic_deleted_mid_transaction() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let storage = DynoStore::new("nisshi", 1, bucket.clone())?;

    create(&storage, "keep", 1).await?;
    create(&storage, "gone", 1).await?;

    let txn = "txn-1";

    let producer = storage
        .init_producer(Some(txn), 10_000, Some(-1), Some(-1))
        .await?;

    _ = storage
        .txn_add_partitions(TxnAddPartitionsRequest::VersionZeroToThree {
            transaction_id: txn.into(),
            producer_id: producer.id,
            producer_epoch: producer.epoch,
            topics: ["keep", "gone"]
                .into_iter()
                .map(|name| {
                    AddPartitionsToTxnTopic::default()
                        .name(name.into())
                        .partitions(Some(vec![0]))
                })
                .collect(),
        })
        .await?;

    for name in ["keep", "gone"] {
        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Some(Bytes::from_static(b"abc"))))
            .attributes(BatchAttribute::default().transaction(true).into())
            .producer_id(producer.id)
            .producer_epoch(producer.epoch)
            .base_sequence(0)
            .build()
            .and_then(TryInto::try_into)?;

        _ = storage
            .produce(Some(txn), &Topition::new(name, 0), batch)
            .await?;
    }

    assert_eq!(
        ErrorCode::None,
        storage.delete_topic(&TopicId::Name("gone".into())).await?
    );

    for _ in 0..2 {
        assert_eq!(
            ErrorCode::None,
            storage
                .txn_end(txn, producer.id, producer.epoch, true)
                .await?
        );
    }

    let stage = storage.offset_stage(&Topition::new("keep", 0)).await?;
    assert_eq!(stage.high_watermark(), stage.last_stable());

    Ok(())
}
