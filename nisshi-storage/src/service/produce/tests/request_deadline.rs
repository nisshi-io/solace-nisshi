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

//! The deadline of a Produce request, against a [`CountingStorage`] whose produce waits for a
//! gate, as a storage backend whose produce call is slow does. Every test runs on a paused clock,
//! so the time that a request takes is exact.

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
use opentelemetry::{KeyValue, global};
use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
    data::{AggregatedMetrics, MetricData, ResourceMetrics, ScopeMetrics},
};
use rama::{Service, extensions::Extensions};
use tokio::{
    sync::Semaphore,
    time::{Instant, sleep, timeout},
};

use crate::{
    Error, ProduceService, Result,
    decoded_budget::DecodedBytesBudget,
    service::counting_storage::{CountingStorage, Listing},
};

const TOPIC: &str = "abc";

/// A `timeout_ms` that no test reaches.
const LONG: i32 = 600_000;

/// How long a client with default settings waits for a response before it closes the connection.
const CLIENT_GIVES_UP: Duration = Duration::from_secs(30);

fn batch() -> Result<deflated::Batch> {
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;

    inflated::Batch::builder()
        .base_timestamp(now)
        .max_timestamp(now)
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

/// A request with one batch for each of `partitions`, in order.
fn request(partitions: &[i32], acks: Ack, timeout_ms: i32) -> Result<RequestInput<ProduceRequest>> {
    let partitions = partitions
        .iter()
        .map(|index| {
            Ok(PartitionProduceData::default()
                .index(*index)
                .records(Some(Frame {
                    batches: vec![batch()?],
                })))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(RequestInput {
        request: ProduceRequest::default()
            .acks(i16::from(acks))
            .timeout_ms(timeout_ms)
            .topic_data(Some(
                [TopicProduceData::default()
                    .name(TOPIC.into())
                    .partition_data(Some(partitions))]
                .into(),
            )),
        extensions: Extensions::default(),
    })
}

/// A request for `partitions` that waits for the broker's answer, with a `timeout_ms` that no
/// test reaches.
fn acked(partitions: &[i32]) -> Result<RequestInput<ProduceRequest>> {
    request(partitions, Ack::Leader, LONG)
}

/// The service that the broker runs for Produce, over `storage`, with the process-wide budget.
fn service(
    storage: &CountingStorage,
) -> impl Service<RequestInput<ProduceRequest>, Output = ProduceResponse, Error = Error> {
    ProduceService::new(storage.clone()).with_validation()
}

/// Serves `request` and returns the responses of its partitions, in order. It fails the test when
/// the request has not answered by the time that a client with default settings gives up.
async fn serve(
    service: &impl Service<RequestInput<ProduceRequest>, Output = ProduceResponse, Error = Error>,
    request: RequestInput<ProduceRequest>,
) -> Result<Vec<PartitionProduceResponse>> {
    Ok(timeout(CLIENT_GIVES_UP, service.serve(request))
        .await
        .expect("the request answered before the client gave up")?
        .responses
        .unwrap_or_default()
        .remove(0)
        .partition_responses
        .unwrap_or_default())
}

fn codes(responses: &[PartitionProduceResponse]) -> Result<Vec<ErrorCode>> {
    responses
        .iter()
        .map(|response| ErrorCode::try_from(response.error_code).map_err(Into::into))
        .collect()
}

/// A storage whose produce waits for a permit of the returned gate.
fn stalled() -> (CountingStorage, Arc<Semaphore>) {
    let gate = Arc::new(Semaphore::new(0));

    (
        CountingStorage::new(Listing::Topics, &[TOPIC]).with_produce_gate(Arc::clone(&gate)),
        gate,
    )
}

/// Lets the tasks that are ready run, so that a write that a test released has finished.
async fn settle() {
    sleep(Duration::from_millis(1)).await;
}

/// A request for three partitions over a store that stalls every put answers before the client
/// gives up, and each partition answers a code that the client retries.
#[tokio::test(start_paused = true)]
async fn three_partitions_over_a_stalled_store_answer_before_the_client_gives_up() -> Result<()> {
    let (storage, _gate) = stalled();
    let service = service(&storage);
    let started_at = Instant::now();

    let responses = serve(&service, acked(&[0, 1, 2])?).await?;

    assert_eq!(Duration::from_secs(20), started_at.elapsed());
    assert_eq!(
        vec![ErrorCode::RequestTimedOut; 3],
        codes(&responses)?,
        "{responses:?}"
    );
    assert_eq!(vec![-1; 3], offsets(&responses));

    Ok(())
}

fn offsets(responses: &[PartitionProduceResponse]) -> Vec<i64> {
    responses
        .iter()
        .map(|response| response.base_offset)
        .collect()
}

/// A partition that was written before the deadline keeps its offset. The partitions after it
/// answer a retriable code, and the one that was not started leaves no trace in storage.
#[tokio::test(start_paused = true)]
async fn a_partition_written_before_the_deadline_keeps_its_offset() -> Result<()> {
    let (storage, _gate) = stalled();
    let storage = storage.with_gated_partitions(&[2, 3]);
    let service = service(&storage);

    let responses = serve(&service, acked(&[1, 2, 3])?).await?;

    assert_eq!(
        vec![
            ErrorCode::None,
            ErrorCode::RequestTimedOut,
            ErrorCode::RequestTimedOut
        ],
        codes(&responses)?
    );
    assert_eq!(vec![1000, -1, -1], offsets(&responses));

    // Partition 3 never reached storage, and partition 2 waits there.
    assert_eq!(1, storage.parked());
    assert_eq!(
        vec![1],
        storage
            .produced()
            .iter()
            .map(|produced| produced.topition.partition)
            .collect::<Vec<_>>()
    );

    Ok(())
}

/// The write that the deadline left behind runs to its end: the request answered before the gate
/// opened, and the batch is stored once the gate does.
#[tokio::test(start_paused = true)]
async fn a_write_that_started_finishes_after_the_request_answered() -> Result<()> {
    let (storage, gate) = stalled();
    let service = service(&storage);

    let responses = serve(&service, acked(&[0])?).await?;

    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&responses)?);
    assert_eq!(1, storage.parked());
    assert!(storage.produced().is_empty());

    gate.add_permits(1);
    settle().await;

    assert_eq!(0, storage.parked());
    assert_eq!(1, storage.produced().len());

    Ok(())
}

/// A retry to a partition whose write the broker still runs does not start a second write. It
/// answers at once, and the storage sees one produce. After the first write ends, the partition
/// takes writes again.
#[tokio::test(start_paused = true)]
async fn a_retry_to_a_partition_with_a_write_in_flight_does_not_start_another() -> Result<()> {
    let (storage, gate) = stalled();
    let service = service(&storage);

    let first = serve(&service, acked(&[0])?).await?;
    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&first)?);

    let retried_at = Instant::now();
    let retry = serve(&service, acked(&[0])?).await?;

    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&retry)?);
    assert_eq!(Duration::ZERO, retried_at.elapsed());

    // A partition that has no write in flight is not refused.
    let other = serve(&service, acked(&[1])?).await?;
    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&other)?);

    gate.add_permits(3);
    settle().await;

    // One write for partition 0, and one for partition 1.
    assert_eq!(2, storage.produced().len());

    let after = serve(&service, acked(&[0])?).await?;
    assert_eq!(vec![ErrorCode::None], codes(&after)?);
    assert_eq!(3, storage.produced().len());

    Ok(())
}

/// A `timeout_ms` under the deadline shortens it. A `timeout_ms` of zero or less leaves the
/// deadline as it is.
#[tokio::test(start_paused = true)]
async fn the_request_timeout_shortens_the_deadline() -> Result<()> {
    for (timeout_ms, expected) in [
        (3_000, Duration::from_secs(3)),
        (LONG, Duration::from_secs(20)),
        (0, Duration::from_secs(20)),
        (-1, Duration::from_secs(20)),
    ] {
        // A storage of its own, so that no write of an earlier round is in flight.
        let (storage, _gate) = stalled();
        let service = service(&storage);
        let started_at = Instant::now();

        let responses = serve(&service, request(&[0], Ack::Leader, timeout_ms)?).await?;

        assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&responses)?);
        assert_eq!(expected, started_at.elapsed(), "timeout_ms {timeout_ms}");
    }

    Ok(())
}

/// A request that does not wait for an answer has no deadline: Kafka never times it out, and the
/// service closes the connection on any partition error.
#[tokio::test(start_paused = true)]
async fn a_request_with_acks_zero_has_no_deadline() -> Result<()> {
    let (storage, gate) = stalled();
    let service = service(&storage);

    let mut produce = std::pin::pin!(serve(&service, request(&[0], Ack::None, LONG)?));

    tokio::select! {
        biased;
        _ = &mut produce => panic!("the request answered before storage did"),
        () = sleep(Duration::from_secs(25)) => {}
    }

    assert_eq!(1, storage.parked());

    gate.add_permits(1);
    _ = produce.await;

    assert_eq!(1, storage.produced().len());

    Ok(())
}

/// A request whose batches wait for the decoded-bytes budget stops waiting at the deadline, not at
/// the longer wait of the budget.
#[tokio::test(start_paused = true)]
async fn the_wait_for_the_budget_ends_at_the_deadline() -> Result<()> {
    let storage = CountingStorage::new(Listing::Topics, &[TOPIC]);
    let budget = Arc::new(DecodedBytesBudget::new(MAX_DECODED_BATCH_BYTES));
    let _held = budget
        .reserve(
            MAX_DECODED_BATCH_BYTES,
            Instant::now() + Duration::from_secs(600),
        )
        .await;

    let service = ProduceService::new(storage.clone())
        .with_decoded_budget(budget, Duration::from_secs(60))
        .with_validation();
    let started_at = Instant::now();

    let responses = serve(&service, request(&[0, 1], Ack::Leader, 3_000)?).await?;

    assert_eq!(vec![ErrorCode::RequestTimedOut; 2], codes(&responses)?);
    assert_eq!(Duration::from_secs(3), started_at.elapsed());
    assert!(storage.produced().is_empty());

    Ok(())
}

/// The write that the deadline left behind still holds its bytes of the budget, and returns them
/// when it ends, so that the budget keeps counting the memory that the broker holds.
#[tokio::test(start_paused = true)]
async fn a_detached_write_holds_its_budget_until_it_ends() -> Result<()> {
    let (storage, gate) = stalled();
    let budget = Arc::new(DecodedBytesBudget::new(4 * MAX_DECODED_BATCH_BYTES));
    let service = ProduceService::new(storage.clone())
        .with_decoded_budget(Arc::clone(&budget), Duration::from_secs(60))
        .with_validation();

    let responses = serve(&service, acked(&[0])?).await?;

    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&responses)?);
    assert_eq!(1, storage.parked());
    assert_ne!(0, budget.in_use_bytes());

    gate.add_permits(1);
    settle().await;

    assert_eq!(0, budget.in_use_bytes());

    Ok(())
}

/// A request that finishes in time answers as before.
#[tokio::test(start_paused = true)]
async fn a_request_that_finishes_in_time_answers_as_before() -> Result<()> {
    let (storage, gate) = stalled();
    gate.add_permits(3);
    let service = service(&storage);
    let started_at = Instant::now();

    let responses = serve(&service, acked(&[0, 1, 2])?).await?;

    assert_eq!(vec![ErrorCode::None; 3], codes(&responses)?);
    assert_eq!(vec![0, 1000, 2000], offsets(&responses));
    assert_eq!(Duration::ZERO, started_at.elapsed());

    Ok(())
}

/// Installs an in-memory exporter as the global meter provider. A test calls this before the code
/// creates its first instrument. nextest runs each test in a process of its own, so the
/// instruments are created after this in every test that calls it.
fn install_exporter() -> (SdkMeterProvider, InMemoryMetricExporter) {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    global::set_meter_provider(provider.clone());
    (provider, exporter)
}

/// Returns the attributes and the value of each data point of the sum `name`, as the last export
/// holds them.
fn recorded(
    provider: &SdkMeterProvider,
    exporter: &InMemoryMetricExporter,
    name: &str,
) -> Vec<(Vec<(String, String)>, i64)> {
    provider.force_flush().expect("flush");

    exporter
        .get_finished_metrics()
        .expect("finished metrics")
        .last()
        .into_iter()
        .flat_map(ResourceMetrics::scope_metrics)
        .flat_map(ScopeMetrics::metrics)
        .filter(|metric| metric.name() == name)
        .flat_map(|metric| match metric.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                .data_points()
                .map(|point| {
                    (
                        sorted(point.attributes()),
                        i64::try_from(point.value()).unwrap_or(i64::MAX),
                    )
                })
                .collect::<Vec<_>>(),
            AggregatedMetrics::I64(MetricData::Sum(sum)) => sum
                .data_points()
                .map(|point| (sorted(point.attributes()), point.value()))
                .collect::<Vec<_>>(),
            _ => vec![],
        })
        .collect()
}

fn sorted<'a>(attributes: impl Iterator<Item = &'a KeyValue>) -> Vec<(String, String)> {
    let mut sorted = attributes
        .map(|attribute| (attribute.key.to_string(), attribute.value.to_string()))
        .collect::<Vec<_>>();
    sorted.sort();
    sorted
}

fn stage(stage: &str) -> Vec<(String, String)> {
    vec![("stage".to_owned(), stage.to_owned())]
}

/// The deadline counts the partitions that it stopped, by whether their write had started, and a
/// gauge counts the writes that run on after their answer.
#[tokio::test(start_paused = true)]
async fn the_deadline_counts_each_partition_it_stopped() -> Result<()> {
    let (provider, exporter) = install_exporter();
    let (storage, gate) = stalled();
    let service = service(&storage);

    let responses = serve(&service, acked(&[0, 1, 2])?).await?;
    assert_eq!(vec![ErrorCode::RequestTimedOut; 3], codes(&responses)?);

    let mut exceeded = recorded(&provider, &exporter, "nisshi_produce_deadline_exceeded");
    exceeded.sort();
    assert_eq!(
        vec![(stage("not_started"), 2), (stage("writing"), 1)],
        exceeded
    );
    assert_matches!(
        recorded(&provider, &exporter, "nisshi_produce_detached_writes").as_slice(),
        [(_, 1)]
    );

    gate.add_permits(1);
    settle().await;

    assert_matches!(
        recorded(&provider, &exporter, "nisshi_produce_detached_writes").as_slice(),
        [(_, 0)]
    );

    Ok(())
}

/// A request that finishes in time counts nothing.
#[tokio::test(start_paused = true)]
async fn a_request_that_finishes_in_time_counts_nothing() -> Result<()> {
    let (provider, exporter) = install_exporter();
    let (storage, gate) = stalled();
    gate.add_permits(1);
    let service = service(&storage);

    let responses = serve(&service, acked(&[0])?).await?;
    assert_eq!(vec![ErrorCode::None], codes(&responses)?);

    assert!(recorded(&provider, &exporter, "nisshi_produce_deadline_exceeded").is_empty());

    Ok(())
}
