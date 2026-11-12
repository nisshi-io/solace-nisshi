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

//! The steps of a partition together, as [`ProduceService::new`] composes them, against a
//! [`CountingStorage`].

use std::{
    assert_matches,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use nisshi_sans_io::{
    Ack, ErrorCode, ProduceRequest, ProduceResponse, RequestInput,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::PartitionProduceResponse,
    record::{MAX_DECODED_BATCH_BYTES, Record, deflated, deflated::Frame, inflated},
};
use rama::{Service as _, extensions::Extensions};
use tokio::time::Instant;

use crate::{
    ProduceService, Result,
    decoded_budget::DecodedBytesBudget,
    service::counting_storage::{CountingStorage, Listing, Produced},
};

const TOPIC: &str = "abc";

/// Returns a request that produces `batch` to partition `index` of [`TOPIC`].
fn produce_request(index: i32, batch: deflated::Batch) -> RequestInput<ProduceRequest> {
    RequestInput {
        request: ProduceRequest::default()
            .acks(i16::from(Ack::Leader))
            .topic_data(Some(
                [TopicProduceData::default()
                    .name(TOPIC.into())
                    .partition_data(Some(
                        [PartitionProduceData::default()
                            .index(index)
                            .records(Some(Frame {
                                batches: vec![batch],
                            }))]
                        .into(),
                    ))]
                .into(),
            )),
        extensions: Extensions::default(),
    }
}

/// Returns the response for the only partition of the only topic of `response`.
fn only_partition(response: ProduceResponse) -> PartitionProduceResponse {
    let mut partitions = response
        .responses
        .unwrap_or_default()
        .remove(0)
        .partition_responses
        .unwrap_or_default();

    partitions.remove(0)
}

/// Produces `batch` to partition `index` of [`TOPIC`] through [`ProduceService::new`].
async fn produce(
    storage: &CountingStorage,
    index: i32,
    batch: deflated::Batch,
) -> Result<PartitionProduceResponse> {
    let response = ProduceService::new(storage.clone())
        .with_validation()
        .serve(produce_request(index, batch))
        .await?;

    Ok(only_partition(response))
}

fn batch(max_timestamp: i64) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .base_timestamp(1_800_000_000_000)
        .max_timestamp(max_timestamp)
        .last_offset_delta(0)
        .record(
            Record::builder()
                .value(Bytes::from_static(b"value").into())
                .timestamp_delta(0),
        )
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// A batch with record data that does not decode, and a CRC that matches it.
fn undecodable() -> Result<deflated::Batch> {
    let mut batch = batch(1_800_000_000_000)?;
    batch.record_data = Bytes::from_static(b"\xff\xff\xff");
    batch.crc = batch.computed_crc();

    Ok(batch)
}

/// The decode in the timestamp step reaches storage, so that no backend decodes the batch again.
#[tokio::test]
async fn the_decoded_batch_reaches_storage() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &[TOPIC]);
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;

    // Within the timestamp window, with a header that the service rewrites.
    let sent = {
        let mut sent = batch(-1)?;
        sent.base_timestamp = now;
        sent.crc = sent.computed_crc();
        sent
    };

    let response = produce(&storage, 0, sent).await?;
    assert_eq!(i16::from(ErrorCode::None), response.error_code);

    let produced = storage.produced();
    assert_eq!(1, produced.len());

    let inflated = produced[0]
        .inflated
        .as_ref()
        .expect("stored through produce_inflated");
    assert_eq!(
        &inflated::Batch::try_from(produced[0].deflated.clone())?,
        inflated
    );
    assert_eq!(now, inflated.max_timestamp, "the rewritten header");

    Ok(())
}

#[tokio::test]
async fn a_batch_that_does_not_decode_does_not_reach_storage() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &[TOPIC]);

    let response = produce(&storage, 0, undecodable()?).await?;

    assert_eq!(i16::from(ErrorCode::InvalidRecord), response.error_code);
    assert_eq!(Vec::<Produced>::new(), storage.produced());

    Ok(())
}

/// Kafka looks up the partition before it validates the batch, so a rejected batch for a
/// partition that does not exist gets `UNKNOWN_TOPIC_OR_PARTITION`.
#[tokio::test]
async fn a_rejected_batch_for_a_missing_topic_answers_unknown_topic_or_partition() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &[]);

    let response = produce(&storage, 0, undecodable()?).await?;

    assert_matches!(
        ErrorCode::try_from(response.error_code)?,
        ErrorCode::UnknownTopicOrPartition
    );
    assert!(storage.produced().is_empty());

    Ok(())
}

/// A topic that storage does not hold is answered before the batch is decoded, so that a produce
/// to it does not wait for the decoded-bytes budget.
#[tokio::test]
async fn a_valid_batch_for_a_missing_topic_does_not_wait_for_the_budget() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &[]);

    // Another request holds the whole budget, so a produce that reserves bytes waits and then
    // answers `REQUEST_TIMED_OUT`.
    let budget = Arc::new(DecodedBytesBudget::new(MAX_DECODED_BATCH_BYTES));
    let _held = budget
        .reserve(
            MAX_DECODED_BATCH_BYTES,
            Instant::now() + Duration::from_secs(60),
        )
        .await;

    let response = only_partition(
        ProduceService::new(storage.clone())
            .with_decoded_budget(budget, Duration::from_millis(100))
            .with_validation()
            .serve(produce_request(0, batch(-1)?))
            .await?,
    );

    assert_matches!(
        ErrorCode::try_from(response.error_code)?,
        ErrorCode::UnknownTopicOrPartition
    );
    assert!(storage.produced().is_empty());

    Ok(())
}
