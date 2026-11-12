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

use nisshi_storage::fetched::FetchUntilDeadline as _;
use std::time::Duration;

use tokio::time::Instant;

use bytes::Bytes;
use nisshi_sans_io::{
    BatchAttribute, Compression, ErrorCode, IsolationLevel, ListOffset, ProduceRequest,
    ProduceResponse, RequestInput,
    create_topics_request::CreatableTopic,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{
        MAX_DECODED_BATCH_BYTES, Record,
        deflated::{self, Frame},
        inflated,
    },
    to_system_time,
};
use nisshi_schema::Registry;
use nisshi_storage::{ProduceRequestBatcher, ProduceService, Result, Storage as _, Topition};
use object_store::memory::InMemory;
use rama::{Service as _, extensions::Extensions};
use url::Url;

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
            .fetch_until_deadline(
                &topition,
                offset,
                1,
                u32::MAX,
                IsolationLevel::ReadUncommitted,
                Instant::now() + Duration::from_secs(5),
            )
            .await
            .map(|read| read.batches)?;

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

/// A `ProduceRequest` for partition 0 of `person` with one record, a person of `age`. The person
/// schema rejects a negative age.
fn person_request(age: i64) -> Result<RequestInput<ProduceRequest>> {
    let batch = inflated::Batch::builder()
        .record(
            Record::builder()
                .key(Some(Bytes::from_static(b"\"345-67-6543\"")))
                .value(Some(Bytes::from(format!(
                    r#"{{"firstName":"John","lastName":"Doe","age":{age}}}"#
                )))),
        )
        .build()
        .and_then(deflated::Batch::try_from)?;

    Ok(RequestInput {
        request: ProduceRequest::default().acks(-1).topic_data(Some(vec![
            TopicProduceData::default()
                .name("person".into())
                .partition_data(Some(vec![
                    PartitionProduceData::default()
                        .index(0)
                        .records(Some(Frame {
                            batches: vec![batch],
                        })),
                ])),
        ])),
        extensions: Extensions::default(),
    })
}

/// The error code and the base offset of the only partition in `response`.
fn only_partition(response: ProduceResponse) -> (i16, i64) {
    let topics = response.responses.unwrap_or_default();
    assert_eq!(1, topics.len());

    let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());

    (partitions[0].error_code, partitions[0].base_offset)
}

/// The service validates a batch against the schema before the batcher queues it. A client whose
/// records break the schema is rejected on its own, and the batcher stores the batch that it
/// merges with it. If the batcher merged first, the one failed write would fail both clients.
#[tokio::test]
async fn a_batch_that_breaks_the_schema_does_not_fail_the_batch_queued_beside_it() -> Result<()> {
    let storage = DynoStore::new("nisshi", 111, InMemory::new())?;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name("person".into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let schemas = Registry::builder_try_from_url(&Url::parse("file://../etc/schema")?)
        .map(|builder| builder.build())?;

    let batcher = ProduceRequestBatcher::new(storage.clone())
        .with_maximum_delay(Some(Duration::from_millis(100)));

    let produce = ProduceService::with_schemas(batcher, Some(schemas)).with_validation();

    // Both requests join the queue inside one delay, so the batcher would write them as one batch.
    let (valid, invalid) = tokio::join!(
        produce.serve(person_request(21)?),
        produce.serve(person_request(-1)?),
    );

    assert_eq!((i16::from(ErrorCode::None), 0), only_partition(valid?));
    assert_eq!(
        i16::from(ErrorCode::InvalidRecord),
        only_partition(invalid?).0
    );

    // The rejected batch took no offset, so the next valid batch follows the first.
    assert_eq!(
        (i16::from(ErrorCode::None), 1),
        only_partition(produce.serve(person_request(22)?).await?)
    );

    Ok(())
}
