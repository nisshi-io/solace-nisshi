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
    Error, ProduceService, Result, StillRunning,
    decoded_budget::{self, DecodedBytesBudget},
    service::{
        counting_storage::{CountingStorage, Listing},
        produce::detached::Detached,
    },
};

const TOPIC: &str = "abc";

/// A `timeout_ms` that no test reaches.
const LONG: i32 = 600_000;

/// How long a client with default settings waits for a response before it closes the connection.
const CLIENT_GIVES_UP: Duration = Duration::from_secs(30);

/// A batch from the producer `producer_id`. The ID -1 is a batch that is not idempotent.
fn batch(producer_id: i64) -> Result<deflated::Batch> {
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;

    inflated::Batch::builder()
        .base_timestamp(now)
        .max_timestamp(now)
        .last_offset_delta(0)
        .producer_id(producer_id)
        .producer_epoch(if producer_id < 0 { -1 } else { 0 })
        .base_sequence(if producer_id < 0 { -1 } else { 0 })
        .record(
            Record::builder()
                .value(Bytes::from_static(b"value").into())
                .timestamp_delta(0),
        )
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// A request with one batch for each of `partitions`, in order, from the producer `producer_id`.
fn request_from(
    producer_id: i64,
    partitions: &[i32],
    acks: Ack,
    timeout_ms: i32,
) -> Result<RequestInput<ProduceRequest>> {
    let partitions = partitions
        .iter()
        .map(|index| {
            Ok(PartitionProduceData::default()
                .index(*index)
                .records(Some(Frame {
                    batches: vec![batch(producer_id)?],
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

/// A request with one batch for each of `partitions`, in order, that is not idempotent.
fn request(partitions: &[i32], acks: Ack, timeout_ms: i32) -> Result<RequestInput<ProduceRequest>> {
    request_from(-1, partitions, acks, timeout_ms)
}

/// A request for `partitions` from the idempotent producer `producer_id` that waits for the
/// broker's answer, with a `timeout_ms` that no test reaches.
fn acked_by(producer_id: i64, partitions: &[i32]) -> Result<RequestInput<ProduceRequest>> {
    request_from(producer_id, partitions, Ack::Leader, LONG)
}

/// A request for `partitions` that waits for the broker's answer, with a `timeout_ms` that no
/// test reaches.
pub(super) fn acked(partitions: &[i32]) -> Result<RequestInput<ProduceRequest>> {
    request(partitions, Ack::Leader, LONG)
}

/// The service that the broker runs for Produce, over `storage`, with a decoded-bytes budget and a
/// set of in-flight writes of its own.
pub(super) fn service(
    storage: &CountingStorage,
) -> impl Service<RequestInput<ProduceRequest>, Output = ProduceResponse, Error = Error> {
    service_sharing(storage, Detached::default())
}

/// A service like [`service`] that tracks its in-flight writes in `detached`. The broker builds
/// one service for each connection over the set of the process, so services that share a set
/// stand for connections of one broker.
pub(super) fn service_sharing(
    storage: &CountingStorage,
    detached: Detached,
) -> impl Service<RequestInput<ProduceRequest>, Output = ProduceResponse, Error = Error> {
    // A budget of its own as well, because a write that another test leaves running holds
    // bytes of the process's budget, and the tests that run in one process would then wait for
    // each other.
    ProduceService::new_sharing(storage.clone(), detached)
        .with_decoded_budget(
            Arc::new(DecodedBytesBudget::new(4 * MAX_DECODED_BATCH_BYTES)),
            decoded_budget::WAIT_TIMEOUT,
        )
        .with_validation()
}

/// Serves `request` and returns the responses of its partitions, in order. It fails the test when
/// the request has not answered by the time that a client with default settings gives up.
pub(super) async fn serve(
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

pub(super) fn codes(responses: &[PartitionProduceResponse]) -> Result<Vec<ErrorCode>> {
    responses
        .iter()
        .map(|response| ErrorCode::try_from(response.error_code).map_err(Into::into))
        .collect()
}

/// A storage whose produce waits for a permit of the returned gate.
pub(super) fn stalled() -> (CountingStorage, Arc<Semaphore>) {
    let gate = Arc::new(Semaphore::new(0));

    (
        CountingStorage::new(Listing::Topics, &[TOPIC]).with_produce_gate(Arc::clone(&gate)),
        gate,
    )
}

/// How long a test waits for the writes that it released to end.
const GRACE: Duration = Duration::from_secs(5);

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
    // One write at a time, so that partition 3 waits for partition 2's write and the deadline
    // reaches it before it starts.
    let storage = storage
        .with_gated_partitions(&[2, 3])
        .with_produce_concurrency(1);
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
    assert_eq!(vec![1, -1, -1], offsets(&responses));

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
/// opened, and the batch is stored once the gate does and the write has taken its time.
#[tokio::test(start_paused = true)]
async fn a_write_that_started_finishes_after_the_request_answered() -> Result<()> {
    let (storage, gate) = stalled();
    // The write takes a second after the gate opens, as an engine's write does, so the test must
    // wait for the write to end, not only for the runtime to have no task ready.
    let storage = storage.with_produce_delay(Duration::from_secs(1));
    let detached = Detached::default();
    let service = service_sharing(&storage, detached.clone());

    let responses = serve(&service, acked(&[0])?).await?;

    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&responses)?);
    assert_eq!(1, storage.parked());
    assert!(storage.produced().is_empty());

    gate.add_permits(1);
    assert_eq!(StillRunning::default(), detached.wait(GRACE).await);

    assert_eq!(0, storage.parked());
    assert_eq!(1, storage.produced().len());

    Ok(())
}

/// A retry to a partition whose write the broker still runs does not start a second write, also
/// when it arrives on another connection. The broker builds a service for each connection, so the
/// retry goes through a second service over the same storage. It answers at once, and the storage
/// sees one produce for the partition. After the first write ends, the partition takes writes
/// again.
#[tokio::test(start_paused = true)]
async fn a_retry_to_a_partition_with_a_write_in_flight_does_not_start_another() -> Result<()> {
    let (storage, gate) = stalled();
    let detached = Detached::default();
    let connection = service_sharing(&storage, detached.clone());
    let other_connection = service_sharing(&storage, detached.clone());

    let first = serve(&connection, acked_by(7, &[0])?).await?;
    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&first)?);

    let retried_at = Instant::now();
    let retry = serve(&other_connection, acked_by(7, &[0])?).await?;

    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&retry)?);
    assert_eq!(Duration::ZERO, retried_at.elapsed());

    // A partition that the producer has no write in flight to is not refused.
    let other = serve(&other_connection, acked_by(7, &[1])?).await?;
    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&other)?);

    gate.add_permits(3);
    assert_eq!(StillRunning::default(), detached.wait(GRACE).await);

    // One write for partition 0, and one for partition 1.
    assert_eq!(vec![0, 1], produced_partitions_sorted(&storage));

    let after = serve(&other_connection, acked_by(7, &[0])?).await?;
    assert_eq!(vec![ErrorCode::None], codes(&after)?);
    assert_eq!(vec![0, 0, 1], produced_partitions_sorted(&storage));

    Ok(())
}

/// A produce from another producer, and a batch that is not idempotent, are not refused while
/// one producer's write to the partition is in flight. A client that detaches its own writes with
/// a short `timeout_ms` must not hold the partition for the others.
#[tokio::test(start_paused = true)]
async fn another_producer_and_a_non_idempotent_batch_are_not_refused_by_a_write_in_flight()
-> Result<()> {
    let (storage, gate) = stalled();
    let detached = Detached::default();
    let service = service_sharing(&storage, detached.clone());

    let first = serve(&service, acked_by(7, &[0])?).await?;
    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&first)?);

    // The same producer is refused without a write, as in the retry test.
    let refused = serve(&service, acked_by(7, &[0])?).await?;
    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&refused)?);
    assert_eq!(1, storage.parked());

    // Producer 8 and a batch with no producer ID each start a write, and each answers at its own
    // deadline, not at once.
    let other_started_at = Instant::now();
    let other = serve(&service, acked_by(8, &[0])?).await?;
    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&other)?);
    assert_eq!(Duration::from_secs(20), other_started_at.elapsed());

    let plain_started_at = Instant::now();
    let plain = serve(&service, acked(&[0])?).await?;
    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&plain)?);
    assert_eq!(Duration::from_secs(20), plain_started_at.elapsed());
    assert_eq!(3, storage.parked());

    gate.add_permits(3);
    assert_eq!(StillRunning::default(), detached.wait(GRACE).await);

    assert_eq!(vec![0, 0, 0], produced_partitions_sorted(&storage));

    Ok(())
}

/// The partitions that storage got a produce for, by index. The order of the calls is not a
/// contract: writes that the gate releases together reach storage in no fixed order.
fn produced_partitions_sorted(storage: &CountingStorage) -> Vec<i32> {
    let mut partitions = storage
        .produced()
        .iter()
        .map(|produced| produced.topition.partition)
        .collect::<Vec<_>>();
    partitions.sort_unstable();
    partitions
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

    let service = ProduceService::new_sharing(storage.clone(), Detached::default())
        .with_decoded_budget(budget, Duration::from_secs(60))
        .with_validation();
    let started_at = Instant::now();

    let responses = serve(&service, request(&[0, 1], Ack::Leader, 3_000)?).await?;

    assert_eq!(vec![ErrorCode::RequestTimedOut; 2], codes(&responses)?);
    assert_eq!(Duration::from_secs(3), started_at.elapsed());
    assert!(storage.produced().is_empty());

    // Both partitions start at once and wait for the budget, so each names it, and neither reads
    // as a plain deadline.
    let messages = responses
        .iter()
        .map(|response| response.error_message.as_deref().unwrap_or_default())
        .collect::<Vec<_>>();
    assert!(
        messages
            .iter()
            .all(|message| message.contains("decoded-batch budget")),
        "{messages:?}"
    );

    Ok(())
}

/// A request whose read of the topic's configuration does not end answers at the deadline, with
/// every partition not started, and storage is not asked to write.
#[tokio::test(start_paused = true)]
async fn the_read_of_the_topic_configuration_ends_at_the_deadline() -> Result<()> {
    let config_gate = Arc::new(Semaphore::new(0));
    let (storage, _gate) = stalled();
    let storage = storage.with_config_gate(config_gate);
    let service = service(&storage);
    let started_at = Instant::now();

    let responses = serve(&service, request(&[0, 1, 2], Ack::Leader, 3_000)?).await?;

    assert_eq!(Duration::from_secs(3), started_at.elapsed());
    assert_eq!(
        vec![ErrorCode::RequestTimedOut; 3],
        codes(&responses)?,
        "{responses:?}"
    );

    for response in &responses {
        assert!(
            response
                .error_message
                .as_deref()
                .is_some_and(|message| message.contains("before the broker started")),
            "{response:?}"
        );
    }

    assert_eq!(0, storage.parked());
    assert!(storage.produced().is_empty());

    Ok(())
}

/// The write that the deadline left behind still holds its bytes of the budget, and returns them
/// when it ends, so that the budget keeps counting the memory that the broker holds.
#[tokio::test(start_paused = true)]
async fn a_detached_write_holds_its_budget_until_it_ends() -> Result<()> {
    let (storage, gate) = stalled();
    let budget = Arc::new(DecodedBytesBudget::new(4 * MAX_DECODED_BATCH_BYTES));
    let detached = Detached::default();
    let service = ProduceService::new_sharing(storage.clone(), detached.clone())
        .with_decoded_budget(Arc::clone(&budget), Duration::from_secs(60))
        .with_validation();

    let responses = serve(&service, acked(&[0])?).await?;

    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&responses)?);
    assert_eq!(1, storage.parked());
    assert_ne!(0, budget.in_use_bytes());

    gate.add_permits(1);
    assert_eq!(StillRunning::default(), detached.wait(GRACE).await);

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
    assert_eq!(vec![0, 1, 2], offsets(&responses));
    assert_eq!(Duration::ZERO, started_at.elapsed());

    Ok(())
}

/// Installs an in-memory exporter as the global meter provider. A test calls this before the code
/// creates its first instrument. The instruments are process-wide, so only the tests that call
/// this need nextest's process for each test. The other tests share no state.
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

/// Returns the count and the sum of each data point of the histogram `name`, as the last export
/// holds them.
fn histogram(
    provider: &SdkMeterProvider,
    exporter: &InMemoryMetricExporter,
    name: &str,
) -> Vec<(u64, u64)> {
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
            AggregatedMetrics::U64(MetricData::Histogram(histogram)) => histogram
                .data_points()
                .map(|point| (point.count(), point.sum()))
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
    // One write at a time, so that the deadline stops partitions at every stage.
    let storage = storage.with_produce_concurrency(1);
    let detached = Detached::default();
    let service = service_sharing(&storage, detached.clone());

    let responses = serve(&service, acked_by(7, &[0, 1, 2])?).await?;
    assert_eq!(vec![ErrorCode::RequestTimedOut; 3], codes(&responses)?);

    // A retry meets the write that the first request left running.
    let retry = serve(&service, acked_by(7, &[0])?).await?;
    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&retry)?);

    let mut exceeded = recorded(&provider, &exporter, "nisshi_produce_deadline_exceeded");
    exceeded.sort();
    assert_eq!(
        vec![
            (stage("in_flight"), 1),
            (stage("not_started"), 2),
            (stage("writing"), 1)
        ],
        exceeded
    );
    assert_matches!(
        recorded(&provider, &exporter, "nisshi_produce_detached_writes").as_slice(),
        [(_, 1)]
    );

    gate.add_permits(1);
    assert_eq!(StillRunning::default(), detached.wait(GRACE).await);

    assert_matches!(
        recorded(&provider, &exporter, "nisshi_produce_detached_writes").as_slice(),
        [(_, 0)]
    );

    Ok(())
}

/// A partition that the deadline stops while it waits for the decoded-batch budget counts at the
/// `budget_wait` stage, and does not count as an exhausted budget, which is the budget's own
/// timeout. The wait is recorded, although the deadline drops it before it returns.
#[tokio::test(start_paused = true)]
async fn the_deadline_counts_a_partition_that_waited_for_the_budget() -> Result<()> {
    let (provider, exporter) = install_exporter();
    let storage = CountingStorage::new(Listing::Topics, &[TOPIC]);
    let budget = Arc::new(DecodedBytesBudget::new(MAX_DECODED_BATCH_BYTES));
    let _held = budget
        .reserve(
            MAX_DECODED_BATCH_BYTES,
            Instant::now() + Duration::from_secs(600),
        )
        .await;
    let service = ProduceService::new_sharing(storage, Detached::default())
        .with_decoded_budget(budget, Duration::from_secs(60))
        .with_validation();

    let responses = serve(&service, request(&[0], Ack::Leader, 3_000)?).await?;
    assert_eq!(vec![ErrorCode::RequestTimedOut], codes(&responses)?);

    assert_eq!(
        vec![(stage("budget_wait"), 1)],
        recorded(&provider, &exporter, "nisshi_produce_deadline_exceeded")
    );
    assert!(
        recorded(
            &provider,
            &exporter,
            "nisshi_produce_decoded_budget_exhausted"
        )
        .is_empty()
    );
    // Two waits: the setup's reservation, which took no time, and the request's, which took the
    // 3 s that the deadline allowed.
    assert_eq!(
        vec![(2, 3_000)],
        histogram(
            &provider,
            &exporter,
            "nisshi_produce_decoded_budget_wait_duration"
        )
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
