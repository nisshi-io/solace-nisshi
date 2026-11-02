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

//! Tests for produces that go through the `ProduceRequestBatcher`, which the
//! `s3://` and `gs://` engines put in front of `DynoStore` when
//! `batch_max_delay` is set.

use std::time::Duration;

use bytes::Bytes;
use nisshi_sans_io::{
    BatchAttribute, Compression, ErrorCode, IsolationLevel, ListOffset,
    create_topics_request::CreatableTopic,
    record::{MAX_DECODED_BATCH_BYTES, Record, deflated, inflated},
    to_system_time,
};
use nisshi_storage::{ProduceRequestBatcher, Result, Storage as _, Topition};
use object_store::memory::InMemory;

use crate::dynostore::DynoStore;

/// An arbitrary fixed point in 2020, in Kafka's millisecond-since-epoch
/// timestamp representation.
const T0: i64 = 1_577_836_800_000;

/// A gzip batch of one record whose value is `len` bytes of `byte`, with the
/// record's timestamp at `timestamp`. It compresses to about a thousandth of
/// `len`.
fn gzip_batch(byte: u8, len: usize, timestamp: i64) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .attributes(
            BatchAttribute::default()
                .compression(Compression::Gzip)
                .into(),
        )
        .base_timestamp(timestamp)
        .max_timestamp(timestamp)
        .record(
            Record::builder()
                .value(Some(Bytes::from(vec![byte; len])))
                .offset_delta(0),
        )
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// Two batches that each decode under the decoded-size limit, and that
/// together would not, are stored as two batches that each decode. Merged,
/// they would fit under the default `max.message.bytes`, and the stored batch
/// would not decode: a ListOffsets by timestamp that reads it would then
/// answer `KAFKA_STORAGE_ERROR`.
#[tokio::test]
async fn batches_that_would_merge_past_the_decoded_limit_are_stored_so_they_decode() -> Result<()> {
    const LEN: usize = 60 * 1024 * 1024;
    const _: () = assert!(LEN < MAX_DECODED_BATCH_BYTES && 2 * LEN > MAX_DECODED_BATCH_BYTES);

    let storage = DynoStore::new("nisshi", 111, InMemory::new())?;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name("abc".into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new("abc", 0);

    let first = gzip_batch(b'a', LEN, T0)?;
    let second = gzip_batch(b'b', LEN, T0 + 1)?;

    let batcher = ProduceRequestBatcher::new(storage.clone())
        .with_maximum_delay(Some(Duration::from_millis(100)));

    // Both requests join the queue inside one delay, so one flush writes both.
    let (first_offset, second_offset) = tokio::join!(
        batcher.produce(None, &topition, first),
        batcher.produce(None, &topition, second),
    );

    assert_eq!(0, first_offset?);
    assert_eq!(1, second_offset?);

    // A fetch can answer one batch at a time, so each offset is fetched on its own.
    for (offset, value) in [(0, b'a'), (1, b'b')] {
        let stored = storage
            .fetch(
                &topition,
                offset,
                1,
                u32::MAX,
                IsolationLevel::ReadUncommitted,
                Duration::ZERO,
            )
            .await?;

        let inflated = inflated::Batch::try_from(&stored[0])?;

        assert_eq!(offset, inflated.base_offset);
        assert_eq!(1, inflated.records.len());
        assert_eq!(
            Some(Bytes::from(vec![value; LEN])),
            inflated.records[0].value()
        );
    }

    let responses = storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(
                topition.clone(),
                ListOffset::Timestamp(to_system_time(T0 + 1)?),
            )],
        )
        .await?;

    assert_eq!(1, responses.len());
    assert_eq!(ErrorCode::None, responses[0].1.error_code());
    assert_eq!(Some(1), responses[0].1.offset());

    Ok(())
}
