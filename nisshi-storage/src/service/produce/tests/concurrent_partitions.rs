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

//! The partitions of a Produce request written at once, within [`PRODUCE_CONCURRENCY`], against a
//! [`CountingStorage`] whose produce takes time or waits for a gate. Every test runs on a paused
//! clock, so the time that a request takes is exact.

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
    record::{Record, deflated, deflated::Frame, inflated},
};
use rama::{Service, extensions::Extensions};
use tokio::{
    sync::Semaphore,
    time::{Instant, sleep, timeout},
};

use super::super::{
    PRODUCE_CONCURRENCY,
    deadline::{NOT_STARTED, SLOT_WAIT, WRITING},
};
use crate::{
    Error, ProduceService, Result,
    service::counting_storage::{CountingStorage, Listing},
};

const TOPIC: &str = "abc";
const OTHER_TOPIC: &str = "def";

/// How long each produce of a storage with a delay takes.
const WRITE: Duration = Duration::from_secs(1);

/// A `timeout_ms` that no test reaches.
const LONG: i32 = 600_000;

/// How long a client with default settings waits for a response before it closes the connection.
const CLIENT_GIVES_UP: Duration = Duration::from_secs(30);

/// A batch with one record whose value is `value`, so that a test can tell the batches of one
/// partition apart.
fn batch(value: &'static [u8]) -> Result<deflated::Batch> {
    let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;

    inflated::Batch::builder()
        .base_timestamp(now)
        .max_timestamp(now)
        .last_offset_delta(0)
        .record(
            Record::builder()
                .value(Bytes::from_static(value).into())
                .timestamp_delta(0),
        )
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// A topic with one batch for each of `entries`, in order: a partition index and the value of
/// the batch's record.
fn topic(name: &str, entries: &[(i32, &'static [u8])]) -> Result<TopicProduceData> {
    let partitions = entries
        .iter()
        .map(|(index, value)| {
            Ok(PartitionProduceData::default()
                .index(*index)
                .records(Some(Frame {
                    batches: vec![batch(value)?],
                })))
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(TopicProduceData::default()
        .name(name.into())
        .partition_data(Some(partitions)))
}

/// The entries of `partitions`, each with the value `value`.
fn entries(partitions: &[i32], value: &'static [u8]) -> Vec<(i32, &'static [u8])> {
    partitions.iter().map(|index| (*index, value)).collect()
}

/// A request for `topics` that waits for the broker's answer, with a `timeout_ms` that no test
/// reaches.
fn request(topics: Vec<TopicProduceData>, acks: Ack) -> RequestInput<ProduceRequest> {
    RequestInput {
        request: ProduceRequest::default()
            .acks(i16::from(acks))
            .timeout_ms(LONG)
            .topic_data(Some(topics)),
        extensions: Extensions::default(),
    }
}

/// A request for `partitions` of [`TOPIC`] that waits for the broker's answer.
fn acked(partitions: &[i32]) -> Result<RequestInput<ProduceRequest>> {
    Ok(request(
        vec![topic(TOPIC, &entries(partitions, b"value"))?],
        Ack::Leader,
    ))
}

/// The service that the broker runs for Produce, over `storage`.
fn service(
    storage: &CountingStorage,
) -> impl Service<RequestInput<ProduceRequest>, Output = ProduceResponse, Error = Error> {
    ProduceService::new(storage.clone()).with_validation()
}

/// Serves `request` and returns the responses of its partitions, topic after topic in order. It
/// fails the test when the request has not answered by the time that a client with default
/// settings gives up.
async fn serve(
    service: &impl Service<RequestInput<ProduceRequest>, Output = ProduceResponse, Error = Error>,
    request: RequestInput<ProduceRequest>,
) -> Result<Vec<PartitionProduceResponse>> {
    Ok(timeout(CLIENT_GIVES_UP, service.serve(request))
        .await
        .expect("the request answered before the client gave up")?
        .responses
        .unwrap_or_default()
        .into_iter()
        .flat_map(|topic| topic.partition_responses.unwrap_or_default())
        .collect())
}

fn codes(responses: &[PartitionProduceResponse]) -> Result<Vec<ErrorCode>> {
    responses
        .iter()
        .map(|response| ErrorCode::try_from(response.error_code).map_err(Into::into))
        .collect()
}

fn offsets(responses: &[PartitionProduceResponse]) -> Vec<i64> {
    responses
        .iter()
        .map(|response| response.base_offset)
        .collect()
}

fn indexes(responses: &[PartitionProduceResponse]) -> Vec<i32> {
    responses.iter().map(|response| response.index).collect()
}

/// A storage whose every produce takes [`WRITE`].
fn slow() -> CountingStorage {
    CountingStorage::new(Listing::Topics, &[TOPIC, OTHER_TOPIC]).with_produce_delay(WRITE)
}

/// A storage whose produce waits for a permit of the returned gate.
fn stalled() -> (CountingStorage, Arc<Semaphore>) {
    let gate = Arc::new(Semaphore::new(0));

    (
        CountingStorage::new(Listing::Topics, &[TOPIC, OTHER_TOPIC])
            .with_produce_gate(Arc::clone(&gate)),
        gate,
    )
}

/// The partitions that storage got a produce for, in order.
fn produced_partitions(storage: &CountingStorage) -> Vec<i32> {
    storage
        .produced()
        .iter()
        .map(|produced| produced.topition.partition)
        .collect()
}

/// Lets the tasks that are ready run, so that a write that a test released has finished.
async fn settle() {
    sleep(Duration::from_millis(1)).await;
}

/// A request for twice [`PRODUCE_CONCURRENCY`] partitions, each of which takes [`WRITE`], answers
/// in two writes' time, and every partition has its offset.
#[tokio::test(start_paused = true)]
async fn partitions_are_written_up_to_the_bound_at_once() -> Result<()> {
    let storage = slow();
    let service = service(&storage);
    let partitions = (0..2 * PRODUCE_CONCURRENCY)
        .map(i32::try_from)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let started_at = Instant::now();

    let responses = serve(&service, acked(&partitions)?).await?;

    assert_eq!(2 * WRITE, started_at.elapsed());
    assert_eq!(vec![ErrorCode::None; partitions.len()], codes(&responses)?);
    assert_eq!(
        partitions
            .iter()
            .map(|index| i64::from(*index))
            .collect::<Vec<_>>(),
        offsets(&responses)
    );

    Ok(())
}

/// One slow partition does not hold the later partitions back: with the first partition of a
/// request stalled in storage, every other partition is written before the deadline.
#[tokio::test(start_paused = true)]
async fn a_slow_partition_does_not_hold_the_later_ones_back() -> Result<()> {
    let (storage, _gate) = stalled();
    let storage = storage
        .with_gated_partitions(&[0])
        .with_produce_delay(WRITE);
    let service = service(&storage);
    let partitions = (0..2 * PRODUCE_CONCURRENCY)
        .map(i32::try_from)
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let responses = serve(&service, acked(&partitions)?).await?;

    assert_eq!(
        [
            vec![ErrorCode::RequestTimedOut],
            vec![ErrorCode::None; partitions.len() - 1]
        ]
        .concat(),
        codes(&responses)?
    );
    assert_eq!(partitions[1..].to_vec(), produced_partitions(&storage));

    Ok(())
}

/// The topics of a request are written at once as its partitions are: two topics with one
/// partition each answer in one write's time.
#[tokio::test(start_paused = true)]
async fn the_topics_of_a_request_are_written_at_once() -> Result<()> {
    let storage = slow();
    let service = service(&storage);
    let started_at = Instant::now();

    let responses = serve(
        &service,
        request(
            vec![
                topic(TOPIC, &[(0, b"a")])?,
                topic(OTHER_TOPIC, &[(0, b"b")])?,
            ],
            Ack::Leader,
        ),
    )
    .await?;

    assert_eq!(WRITE, started_at.elapsed());
    assert_eq!(vec![ErrorCode::None; 2], codes(&responses)?);

    Ok(())
}

/// A request with more topics than the bound, each with partitions, answers in the time of its
/// writes divided by the bound, with or without a deadline: a topic gives its slot back after the
/// configuration read, so the topics never hold every slot while their partitions wait.
#[tokio::test(start_paused = true)]
async fn many_topics_with_partitions_never_wait_for_each_other() -> Result<()> {
    let names = (0..3 * PRODUCE_CONCURRENCY)
        .map(|topic| format!("topic-{topic}"))
        .collect::<Vec<_>>();
    let names = names.iter().map(String::as_str).collect::<Vec<_>>();

    for acks in [Ack::Leader, Ack::None] {
        let storage = CountingStorage::new(Listing::Topics, &names).with_produce_delay(WRITE);
        let service = service(&storage);
        let topics = names
            .iter()
            .map(|name| topic(name, &entries(&[0, 1], b"value")))
            .collect::<Result<Vec<_>>>()?;
        let writes = u32::try_from(2 * topics.len() / PRODUCE_CONCURRENCY)?;
        let started_at = Instant::now();

        let responses = serve(&service, request(topics, acks)).await?;

        assert_eq!(writes * WRITE, started_at.elapsed(), "acks {acks:?}");
        assert_eq!(vec![ErrorCode::None; 2 * names.len()], codes(&responses)?);
    }

    Ok(())
}

/// The values of the records that storage got for `partition`, in order.
fn produced_values(storage: &CountingStorage, partition: i32) -> Vec<Option<Bytes>> {
    storage
        .produced()
        .iter()
        .filter(|produced| produced.topition.partition == partition)
        .map(|produced| {
            produced
                .inflated
                .as_ref()
                .and_then(|inflated| inflated.records.first())
                .and_then(|record| record.value.clone())
        })
        .collect()
}

/// A request that names a partition twice writes it in the order of the request, and writes the
/// partitions with other indexes at once: over a gated store the first entry and the other
/// partition wait in storage, and the second entry does not start until the first one ended.
#[tokio::test(start_paused = true)]
async fn writes_to_one_partition_stay_in_request_order() -> Result<()> {
    let (storage, gate) = stalled();
    let service = service(&storage);
    let request = request(
        vec![topic(
            TOPIC,
            &[(0, b"first"), (1, b"other"), (0, b"second")],
        )?],
        Ack::Leader,
    );

    let mut produce = std::pin::pin!(serve(&service, request));

    tokio::select! {
        biased;
        _ = &mut produce => panic!("the request answered before storage did"),
        () = settle() => {}
    }

    assert_eq!(2, storage.parked());

    gate.add_permits(3);
    let responses = produce.await?;

    assert_eq!(vec![ErrorCode::None; 3], codes(&responses)?);
    assert_eq!(vec![0, 1, 0], indexes(&responses));
    assert_eq!(
        vec![
            Some(Bytes::from_static(b"first")),
            Some(Bytes::from_static(b"second"))
        ],
        produced_values(&storage, 0)
    );

    Ok(())
}

/// A request that names a topic twice writes the entries of that name in the order of the
/// request, so that a partition in both keeps the request's order: over a gated store the second
/// entry's write does not start until the first one ended.
#[tokio::test(start_paused = true)]
async fn the_entries_of_one_topic_are_written_in_request_order() -> Result<()> {
    let (storage, gate) = stalled();
    let service = service(&storage);
    let request = request(
        vec![
            topic(TOPIC, &[(0, b"first")])?,
            topic(TOPIC, &[(0, b"second")])?,
        ],
        Ack::Leader,
    );

    let mut produce = std::pin::pin!(serve(&service, request));

    tokio::select! {
        biased;
        _ = &mut produce => panic!("the request answered before storage did"),
        () = settle() => {}
    }

    assert_eq!(1, storage.parked());

    gate.add_permits(2);
    let responses = produce.await?;

    assert_eq!(vec![ErrorCode::None; 2], codes(&responses)?);
    assert_eq!(
        vec![
            Some(Bytes::from_static(b"first")),
            Some(Bytes::from_static(b"second"))
        ],
        produced_values(&storage, 0)
    );

    Ok(())
}

/// The answer is unchanged in order and content: the partitions answer in the order of the
/// request, each with its own offset, whichever one finished first.
#[tokio::test(start_paused = true)]
async fn the_partitions_answer_in_request_order_with_their_own_offsets() -> Result<()> {
    let (storage, gate) = stalled();
    // Only partition 2, the first of the request, waits for the gate, so it finishes last.
    let storage = storage.with_gated_partitions(&[2]);
    let service = service(&storage);

    let mut produce = std::pin::pin!(serve(&service, acked(&[2, 0, 1])?));

    tokio::select! {
        biased;
        _ = &mut produce => panic!("the request answered before storage did"),
        () = settle() => {}
    }

    assert_eq!(vec![0, 1], produced_partitions(&storage));

    gate.add_permits(1);
    let responses = produce.await?;

    let expected = |index: i32| {
        PartitionProduceResponse::default()
            .index(index)
            .error_code(ErrorCode::None.into())
            .base_offset(i64::from(index))
            .log_append_time_ms(Some(-1))
            .log_start_offset(Some(0))
            .record_errors(Some([].into()))
            .error_message(None)
            .current_leader(None)
    };
    assert_eq!(vec![expected(2), expected(0), expected(1)], responses);

    Ok(())
}

/// A failure on one partition does not change another's answer.
#[tokio::test(start_paused = true)]
async fn a_failed_partition_leaves_the_others_their_answers() -> Result<()> {
    let storage = slow().with_produce_failure(1, Error::Api(ErrorCode::NotLeaderOrFollower));
    let service = service(&storage);

    let responses = serve(&service, acked(&[0, 1, 2])?).await?;

    assert_eq!(
        vec![
            ErrorCode::None,
            ErrorCode::NotLeaderOrFollower,
            ErrorCode::None
        ],
        codes(&responses)?
    );
    assert_eq!(vec![0, -1, 2], offsets(&responses));

    Ok(())
}

/// The deadline answers the partitions that it reached as it did: six partitions over a stalled
/// store answer at 20 s, the four whose writes run as writing, and the two that got no slot as
/// not started: their group began after the earlier ones had answered, so none waited for a slot.
#[tokio::test(start_paused = true)]
async fn the_deadline_answers_the_partitions_it_reached_as_before() -> Result<()> {
    let (storage, _gate) = stalled();
    let service = service(&storage);
    let started_at = Instant::now();

    let responses = serve(&service, acked(&[0, 1, 2, 3, 4, 5])?).await?;

    assert_eq!(Duration::from_secs(20), started_at.elapsed());
    assert_eq!(vec![ErrorCode::RequestTimedOut; 6], codes(&responses)?);
    assert_eq!(
        vec![
            Some(WRITING.to_owned()),
            Some(WRITING.to_owned()),
            Some(WRITING.to_owned()),
            Some(WRITING.to_owned()),
            Some(NOT_STARTED.to_owned()),
            Some(NOT_STARTED.to_owned()),
        ],
        responses
            .iter()
            .map(|response| response.error_message.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(PRODUCE_CONCURRENCY, storage.parked());
    assert!(storage.produced().is_empty());

    Ok(())
}

/// A storage that runs its writes one at a time gets one write of a request at a time, so a
/// partition never queues inside the engine past the deadline: over a stalled store, one
/// partition is writing at the deadline and the others were not started.
#[tokio::test(start_paused = true)]
async fn a_serial_storage_gets_one_write_of_a_request_at_a_time() -> Result<()> {
    let (storage, _gate) = stalled();
    let storage = storage.with_produce_concurrency(1);
    let service = service(&storage);

    let responses = serve(&service, acked(&[0, 1, 2])?).await?;

    assert_eq!(vec![ErrorCode::RequestTimedOut; 3], codes(&responses)?);
    assert_matches!(
        responses
            .iter()
            .map(|response| response.error_message.as_deref())
            .collect::<Vec<_>>()
            .as_slice(),
        [Some(WRITING), Some(NOT_STARTED), Some(NOT_STARTED)]
    );
    assert_eq!(1, storage.parked());

    Ok(())
}

/// A storage that answers 0 for its produce concurrency is served as if it answered 1: the
/// request answers, and does not wait for a topic group that the buffer never starts.
#[tokio::test(start_paused = true)]
async fn a_storage_that_answers_zero_concurrency_is_served_as_one() -> Result<()> {
    let storage = slow().with_produce_concurrency(0);
    let service = service(&storage);

    let responses = serve(&service, acked(&[0, 1])?).await?;

    assert_eq!(vec![ErrorCode::None; 2], codes(&responses)?);
    assert_eq!(vec![0, 1], produced_partitions(&storage));

    Ok(())
}

/// A storage that shares two permits across requests gets at most two writes at once from two
/// requests that each have four partitions: the pool bounds the requests together, not each
/// request alone.
#[tokio::test(start_paused = true)]
async fn the_writes_of_two_requests_share_the_storage_permits() -> Result<()> {
    let permits = Arc::new(Semaphore::new(2));
    let storage = slow().with_produce_permits(Arc::clone(&permits));
    let service = service(&storage);
    let started_at = Instant::now();

    let (first, second) = tokio::join!(
        serve(&service, acked(&[0, 1, 2, 3])?),
        serve(&service, acked(&[4, 5, 6, 7])?),
    );

    assert_eq!(vec![ErrorCode::None; 4], codes(&first?)?);
    assert_eq!(vec![ErrorCode::None; 4], codes(&second?)?);

    // Eight writes of one second, two at a time.
    assert_eq!(4 * WRITE, started_at.elapsed());
    assert_eq!(2, permits.available_permits());

    Ok(())
}

/// A partition that waits for a permit that the storage shares at the deadline answers
/// `REQUEST_TIMED_OUT` as a slot wait, and storage never gets its write.
#[tokio::test(start_paused = true)]
async fn a_partition_without_a_shared_permit_at_the_deadline_is_a_slot_wait() -> Result<()> {
    let permits = Arc::new(Semaphore::new(1));
    let (storage, _gate) = stalled();
    let storage = storage.with_produce_permits(permits);
    let service = service(&storage);

    let responses = serve(&service, acked(&[0, 1])?).await?;

    assert_eq!(vec![ErrorCode::RequestTimedOut; 2], codes(&responses)?);
    assert_matches!(
        responses
            .iter()
            .map(|response| response.error_message.as_deref())
            .collect::<Vec<_>>()
            .as_slice(),
        [Some(WRITING), Some(SLOT_WAIT)]
    );

    Ok(())
}

/// The shared permit of a write that the deadline left running stays with the write until it
/// ends, because the write holds the engine's connection until then.
#[tokio::test(start_paused = true)]
async fn a_write_left_running_at_the_deadline_keeps_its_permit_until_it_ends() -> Result<()> {
    let permits = Arc::new(Semaphore::new(2));
    let (storage, gate) = stalled();
    let storage = storage.with_produce_permits(Arc::clone(&permits));
    let service = service(&storage);

    _ = serve(&service, acked(&[0, 1])?).await?;

    assert_eq!(0, permits.available_permits());

    gate.add_permits(2);
    settle().await;

    assert_eq!(2, permits.available_permits());

    Ok(())
}
