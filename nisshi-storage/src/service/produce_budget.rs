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

//! The byte budget for decoded batches: a produce that waits for storage holds its decoded batch
//! against the budget, and a produce that cannot get its bytes waits holding only its compressed
//! batch, and answers `REQUEST_TIMED_OUT` when the wait ends.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use nisshi_sans_io::{
    Ack, BatchAttribute, Compression, ErrorCode, ProduceRequest, RequestInput,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::PartitionProduceResponse,
    record::{MAX_DECODED_BATCH_BYTES, Record, deflated, deflated::Frame, inflated},
};
use rama::{Service as _, extensions::Extensions};
use tokio::{
    sync::Semaphore,
    task::JoinSet,
    time::{Instant, sleep},
};

use crate::{
    ProduceService, Result,
    decoded_budget::DecodedBytesBudget,
    service::counting_storage::{CountingStorage, Listing},
};

const TOPIC: &str = "abc";
const MIB: usize = 1024 * 1024;

/// The decoded size of each batch that the tests produce, a little over this.
const DECODED: usize = 16 * MIB;

/// A gzip batch of a few hundred bytes that decodes to [`DECODED`] bytes of zeros: the shape of
/// the request that a client sends to make the broker hold memory.
fn compressible() -> Result<deflated::Batch> {
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;

    let batch: deflated::Batch = inflated::Batch::builder()
        .attributes(
            BatchAttribute::default()
                .compression(Compression::Gzip)
                .into(),
        )
        .base_timestamp(now)
        .max_timestamp(now)
        .last_offset_delta(0)
        .record(
            Record::builder()
                .value(Bytes::from(vec![0; DECODED]).into())
                .timestamp_delta(0),
        )
        .build()
        .and_then(deflated::Batch::try_from)?;

    assert!(
        batch.record_data.len() < 64 * 1024,
        "the batch is not small: {} bytes",
        batch.record_data.len()
    );

    Ok(batch)
}

/// Produces one batch to each of the partitions `0` and `1` in one request, and returns the
/// partitions' responses in order.
async fn produce_two_partitions(
    storage: &CountingStorage,
    budget: &Arc<DecodedBytesBudget>,
    wait: Duration,
) -> Result<Vec<PartitionProduceResponse>> {
    let partition = |index| -> Result<_> {
        Ok(PartitionProduceData::default()
            .index(index)
            .records(Some(Frame {
                batches: vec![compressible()?],
            })))
    };

    let response = ProduceService::new(storage.clone())
        .with_decoded_budget(Arc::clone(budget), wait)
        .with_validation()
        .serve(RequestInput {
            request: ProduceRequest::default()
                .acks(i16::from(Ack::Leader))
                .topic_data(Some(
                    [TopicProduceData::default()
                        .name(TOPIC.into())
                        .partition_data(Some([partition(0)?, partition(1)?].into()))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    Ok(response
        .responses
        .unwrap_or_default()
        .remove(0)
        .partition_responses
        .unwrap_or_default())
}

async fn produce(
    storage: &CountingStorage,
    budget: &Arc<DecodedBytesBudget>,
    wait: Duration,
    batch: deflated::Batch,
) -> Result<PartitionProduceResponse> {
    let response = ProduceService::new(storage.clone())
        .with_decoded_budget(Arc::clone(budget), wait)
        .with_validation()
        .serve(RequestInput {
            request: ProduceRequest::default()
                .acks(i16::from(Ack::Leader))
                .topic_data(Some(
                    [TopicProduceData::default()
                        .name(TOPIC.into())
                        .partition_data(Some(
                            [PartitionProduceData::default()
                                .index(0)
                                .records(Some(Frame {
                                    batches: vec![batch],
                                }))]
                            .into(),
                        ))]
                    .into(),
                )),
            extensions: Extensions::default(),
        })
        .await?;

    Ok(response
        .responses
        .unwrap_or_default()
        .remove(0)
        .partition_responses
        .unwrap_or_default()
        .remove(0))
}

/// Many produces of small, highly compressible batches wait for storage, as when the pool is
/// saturated. The decoded batches that they hold never exceed the budget, and each produce
/// completes once storage answers.
#[tokio::test]
async fn waiting_produces_hold_no_more_decoded_bytes_than_the_budget() -> Result<()> {
    const PRODUCES: usize = 32;
    // A compressed batch reserves the 100 MiB maximum before its decode, so the budget is four
    // of them: room for 18 batches of 16 MiB, because the next decode needs 100 MiB free before it starts.
    let budget = Arc::new(DecodedBytesBudget::new(4 * MAX_DECODED_BATCH_BYTES));
    let gate = Arc::new(Semaphore::new(0));
    let storage =
        CountingStorage::new(Listing::Topics, &[TOPIC]).with_produce_gate(Arc::clone(&gate));

    let mut produces = JoinSet::new();
    for _ in 0..PRODUCES {
        let (storage, budget) = (storage.clone(), Arc::clone(&budget));
        let batch = compressible()?;
        _ = produces
            .spawn(async move { produce(&storage, &budget, Duration::from_secs(60), batch).await });
    }

    // Wait until the budget is nearly full, then give the other produces time to take more if
    // they can.
    for _ in 0..2_000 {
        if storage.parked() >= 18 {
            break;
        }
        sleep(Duration::from_millis(5)).await;
    }
    sleep(Duration::from_millis(300)).await;

    let parked = storage.parked();
    assert!(
        (18..PRODUCES).contains(&parked),
        "{parked} decoded batches held while storage waits"
    );
    assert!(
        parked * DECODED <= budget.total_bytes(),
        "{parked} batches of {DECODED} bytes held against a budget of {}",
        budget.total_bytes()
    );
    assert!(
        budget.in_use_bytes() <= budget.total_bytes(),
        "{} bytes held of a budget of {}",
        budget.in_use_bytes(),
        budget.total_bytes()
    );
    assert_eq!(
        Vec::<crate::service::counting_storage::Produced>::new(),
        storage.produced()
    );

    // Storage answers: every waiting produce completes, and the budget is whole again.
    gate.add_permits(PRODUCES);
    while let Some(response) = produces.join_next().await {
        assert_eq!(i16::from(ErrorCode::None), response??.error_code);
    }

    assert_eq!(PRODUCES, storage.produced().len());
    assert_eq!(0, budget.in_use_bytes());

    Ok(())
}

/// A produce that cannot get its bytes before its deadline answers `REQUEST_TIMED_OUT`, which a
/// producer retries, and it never reaches storage.
#[tokio::test]
async fn a_produce_that_cannot_get_its_bytes_in_time_is_told_to_retry() -> Result<()> {
    let budget = Arc::new(DecodedBytesBudget::new(DECODED + MIB));
    let gate = Arc::new(Semaphore::new(0));
    let storage =
        CountingStorage::new(Listing::Topics, &[TOPIC]).with_produce_gate(Arc::clone(&gate));

    let holder = tokio::spawn({
        let (storage, budget, batch) = (storage.clone(), Arc::clone(&budget), compressible()?);
        async move { produce(&storage, &budget, Duration::from_secs(60), batch).await }
    });

    for _ in 0..2_000 {
        if storage.parked() == 1 {
            break;
        }
        sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(1, storage.parked());

    let late = produce(
        &storage,
        &budget,
        Duration::from_millis(200),
        compressible()?,
    )
    .await?;

    assert_eq!(i16::from(ErrorCode::RequestTimedOut), late.error_code);
    assert_eq!(1, storage.parked(), "the late produce reached storage");

    gate.add_permits(2);
    assert_eq!(i16::from(ErrorCode::None), holder.await??.error_code);
    assert_eq!(1, storage.produced().len());
    assert_eq!(0, budget.in_use_bytes());

    // The bytes are back, so the same produce now succeeds.
    let retried = produce(&storage, &budget, Duration::from_secs(60), compressible()?).await?;
    assert_eq!(i16::from(ErrorCode::None), retried.error_code);

    Ok(())
}

/// The wait for the budget is the same for every partition of a request, and only the time spent
/// waiting for the budget counts against it. The first partition spends longer in storage than the
/// whole wait, and then the second partition waits a short time for the budget behind another
/// request. It still gets its bytes, because the budget stayed spent for much less than the wait.
#[tokio::test]
async fn a_slow_earlier_partition_does_not_use_up_the_wait_of_a_later_one() -> Result<()> {
    // One compressed batch at a time: a reservation is the 100 MiB maximum, clamped to the budget.
    let budget = Arc::new(DecodedBytesBudget::new(MAX_DECODED_BATCH_BYTES));
    let gate = Arc::new(Semaphore::new(0));
    let storage =
        CountingStorage::new(Listing::Topics, &[TOPIC]).with_produce_gate(Arc::clone(&gate));

    let request = tokio::spawn({
        let (storage, budget) = (storage.clone(), Arc::clone(&budget));
        async move { produce_two_partitions(&storage, &budget, Duration::from_secs(1)).await }
    });

    // The first partition holds the budget and waits in storage.
    for _ in 0..2_000 {
        if storage.parked() == 1 {
            break;
        }
        sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(1, storage.parked());

    // Another request queues for the budget ahead of the second partition, and holds it a while
    // once it has it.
    let other = tokio::spawn({
        let budget = Arc::clone(&budget);
        async move {
            let reservation = budget
                .reserve(
                    MAX_DECODED_BATCH_BYTES,
                    Instant::now() + Duration::from_secs(60),
                )
                .await;
            sleep(Duration::from_millis(300)).await;
            drop(reservation);
        }
    });
    sleep(Duration::from_millis(100)).await;

    // The first partition outlasts the 1 second wait without waiting for the budget at all.
    sleep(Duration::from_millis(1_200)).await;
    gate.add_permits(2);

    let responses = request.await??;
    other.await?;

    assert_eq!(
        vec![i16::from(ErrorCode::None); 2],
        responses
            .iter()
            .map(|response| response.error_code)
            .collect::<Vec<_>>()
    );
    assert_eq!(2, storage.produced().len());
    assert_eq!(0, budget.in_use_bytes());

    Ok(())
}

/// A small uncompressed batch reserves bytes in proportion to its size, and holds only what it
/// decodes to, so it does not need the 100 MiB that a compressed batch reserves before its decode.
/// The budget serves requests in arrival order, so this holds while no compressed batch waits
/// ahead of it.
#[tokio::test]
async fn small_uncompressed_batches_do_not_wait_for_each_other() -> Result<()> {
    const PRODUCES: usize = 20;
    let budget = Arc::new(DecodedBytesBudget::new(64 * 1024));
    let gate = Arc::new(Semaphore::new(0));
    let storage =
        CountingStorage::new(Listing::Topics, &[TOPIC]).with_produce_gate(Arc::clone(&gate));

    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let mut produces = JoinSet::new();
    for _ in 0..PRODUCES {
        let batch: deflated::Batch = inflated::Batch::builder()
            .base_timestamp(now)
            .max_timestamp(now)
            .last_offset_delta(0)
            .record(
                Record::builder()
                    .value(Bytes::from_static(b"value").into())
                    .timestamp_delta(0),
            )
            .build()
            .and_then(deflated::Batch::try_from)?;
        let (storage, budget) = (storage.clone(), Arc::clone(&budget));
        _ = produces
            .spawn(async move { produce(&storage, &budget, Duration::from_secs(60), batch).await });
    }

    for _ in 0..2_000 {
        if storage.parked() == PRODUCES {
            break;
        }
        sleep(Duration::from_millis(5)).await;
    }

    assert_eq!(PRODUCES, storage.parked());
    assert!(
        budget.in_use_bytes() <= PRODUCES * 1024,
        "{}",
        budget.in_use_bytes()
    );

    gate.add_permits(PRODUCES);
    while let Some(response) = produces.join_next().await {
        assert_eq!(i16::from(ErrorCode::None), response??.error_code);
    }

    Ok(())
}
