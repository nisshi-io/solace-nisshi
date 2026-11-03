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

//! What a `fetch` answers from a stored batch: how `max_bytes` counts the records, and which
//! headers each record has. Every engine must answer the same, so that a client cannot tell the
//! engines apart.

use std::time::Duration;

use crate::common::{self, StorageType, alphanumeric_string, init_tracing};
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    IsolationLevel,
    create_topics_request::CreatableTopic,
    record::{self, Header, Record, deflated, header, inflated},
};
use nisshi_storage::{Storage, Topition};
use rand::{prelude::*, rng};
use url::Url;
use uuid::Uuid;

async fn topic(storage: &impl Storage) -> Result<Topition> {
    let topic = alphanumeric_string(15);

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    Ok(Topition::new(topic, 0))
}

/// Stores `records` as one batch, at offsets 0, 1, 2 and so on.
async fn store(
    storage: &impl Storage,
    topition: &Topition,
    records: Vec<record::Builder>,
) -> Result<()> {
    let last = i32::try_from(records.len())? - 1;

    let batch = records
        .into_iter()
        .enumerate()
        .try_fold(
            inflated::Batch::builder()
                .producer_id(-1)
                .producer_epoch(-1)
                .last_offset_delta(last),
            |batch, (index, record)| {
                Ok::<_, nisshi_broker::Error>(
                    batch.record(record.offset_delta(i32::try_from(index)?)),
                )
            },
        )?
        .build()
        .and_then(deflated::Batch::try_from)?;

    _ = storage.produce(None, topition, batch).await?;

    Ok(())
}

/// The records that a fetch from `offset` answers, with their offsets.
async fn fetch(
    storage: &impl Storage,
    topition: &Topition,
    offset: i64,
    max_bytes: u32,
) -> Result<Vec<(i64, Record)>> {
    let mut answered = vec![];

    for batch in storage
        .fetch(
            topition,
            offset,
            1,
            max_bytes,
            IsolationLevel::ReadUncommitted,
            Duration::from_secs(60),
        )
        .await?
    {
        let batch = inflated::Batch::try_from(batch)?;

        for record in batch.records {
            answered.push((batch.base_offset + i64::from(record.offset_delta), record));
        }
    }

    Ok(answered)
}

/// A record whose key and value are 100 bytes together.
fn record_of_100_bytes() -> record::Builder {
    Record::builder()
        .key(Some(Bytes::from_static(b"kk")))
        .value(Some(Bytes::from(vec![b'v'; 98])))
}

/// The records of a fetch are in its answer while their sizes, the length of the key and of the
/// value, add up to less than `max_bytes`: a record that brings the sum to `max_bytes` is not in it,
/// except the first, which is always in it so that a consumer gets past a record larger than its limit.
async fn max_bytes_is_the_exclusive_sum_of_keys_and_values(storage: impl Storage) -> Result<()> {
    let topition = topic(&storage).await?;
    store(
        &storage,
        &topition,
        (0..10).map(|_| record_of_100_bytes()).collect(),
    )
    .await?;

    for (max_bytes, expected) in [
        (450, 4),
        (400, 3),
        (401, 4),
        (1_000, 9),
        (1_001, 10),
        (100, 1),
    ] {
        assert_eq!(
            expected,
            fetch(&storage, &topition, 0, max_bytes).await?.len(),
            "max_bytes {max_bytes}"
        );
    }

    // the sum starts at the offset of the fetch
    let offsets = fetch(&storage, &topition, 6, 1_000)
        .await?
        .into_iter()
        .map(|(offset, _)| offset)
        .collect::<Vec<_>>();
    assert_eq!(vec![6, 7, 8, 9], offsets);

    Ok(())
}

fn header(key: &'static [u8], value: Option<&'static [u8]>) -> header::Builder {
    let header = Header::builder().key(Bytes::from_static(key));
    match value {
        Some(value) => header.value(Bytes::from_static(value)),
        None => header,
    }
}

/// Each record has its own headers, in their order: a repeated key, a header without a value,
/// three headers, and records without any, between records that have some.
async fn headers_follow_their_records(storage: impl Storage) -> Result<()> {
    let topition = topic(&storage).await?;
    store(
        &storage,
        &topition,
        vec![
            Record::builder()
                .value(Some(Bytes::from_static(b"zero")))
                .header(header(b"repeated", Some(b"first")))
                .header(header(b"repeated", Some(b"second"))),
            Record::builder()
                .value(Some(Bytes::from_static(b"one")))
                .header(header(b"no-value", None)),
            Record::builder()
                .value(Some(Bytes::from_static(b"two")))
                .header(header(b"a", Some(b"1")))
                .header(header(b"b", Some(b"2")))
                .header(header(b"c", Some(b"3"))),
            Record::builder().value(Some(Bytes::from_static(b"three"))),
            Record::builder().value(Some(Bytes::from_static(b"four"))),
        ],
    )
    .await?;

    let headers_of = |answer: Vec<(i64, Record)>| {
        answer
            .into_iter()
            .map(|(offset, record)| {
                (
                    offset,
                    record
                        .headers
                        .into_iter()
                        .map(|header| (header.key, header.value))
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>()
    };

    let from_start = headers_of(fetch(&storage, &topition, 0, 1_000_000).await?);
    assert_eq!(
        vec![0, 1, 2, 3, 4],
        from_start
            .iter()
            .map(|(offset, _)| *offset)
            .collect::<Vec<_>>()
    );
    assert_eq!(2, from_start[0].1.len());
    assert_eq!(1, from_start[1].1.len());
    assert_eq!(3, from_start[2].1.len());
    assert!(from_start[3].1.is_empty());
    assert!(from_start[4].1.is_empty());

    // the order of the headers of a record is the order that they were sent in
    let repeated = &from_start[0].1;
    assert_eq!(Some(Bytes::from_static(b"first")), repeated[0].1);
    assert_eq!(Some(Bytes::from_static(b"second")), repeated[1].1);
    assert_eq!(None, from_start[1].1[0].1);

    // a fetch that starts later does not give a record the headers of an earlier one
    let from_two = headers_of(fetch(&storage, &topition, 2, 1_000_000).await?);
    assert_eq!(from_start[2..].to_vec(), from_two);

    Ok(())
}

macro_rules! backend {
    ($name:ident, $feature:literal, $storage_type:expr) => {
        #[cfg(feature = $feature)]
        mod $name {
            use nisshi_storage::ArcDynStorage;

            use super::*;

            async fn storage_container() -> Result<ArcDynStorage> {
                common::storage_container(
                    $storage_type,
                    Uuid::now_v7(),
                    rng().random_range(0..i32::MAX),
                    Url::parse("tcp://127.0.0.1/")?,
                    None,
                )
                .await
            }

            #[tokio::test]
            async fn max_bytes_is_the_exclusive_sum_of_keys_and_values() -> Result<()> {
                let _guard = init_tracing()?;
                super::max_bytes_is_the_exclusive_sum_of_keys_and_values(storage_container().await?)
                    .await
            }

            #[tokio::test]
            async fn headers_follow_their_records() -> Result<()> {
                let _guard = init_tracing()?;
                super::headers_follow_their_records(storage_container().await?).await
            }
        }
    };
}

backend!(lite, "libsql", StorageType::Lite);
backend!(turso, "turso", StorageType::Turso);
