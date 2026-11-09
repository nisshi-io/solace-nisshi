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

//! Exercises `ChannelRequestService::serve`, the mpsc-mode storage supervisor loop
//! (SOL-155270), directly against a minimal test double - rather than a real storage
//! backend - so a panic or a dropped response receiver can be triggered
//! deterministically, independent of any backend's own behaviour.

use std::{
    assert_matches,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use nisshi_sans_io::{ErrorCode, IsolationLevel};
use nisshi_storage::{
    ChannelRequest, ChannelRequestLayer, Error, Request, RequestChannelService, Response, Storage,
    Topition, bounded_channel,
};
use rama::{Layer, Service};
use tokio::{sync::oneshot, task::JoinSet, time::Instant};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::common::{Error as TestError, init_tracing};

mod common;

/// A minimal [`Service<Request>`] standing in for real storage, so the supervisor
/// loop's own behaviour (what SOL-155270 fixes) can be tested independently of any
/// storage backend. Panics exactly once - on the first request it serves, unless
/// built with [`FlakyService::benign`] - then answers every request with
/// `Response::Ping(Ok(()))`.
#[derive(Clone, Default)]
struct FlakyService {
    panic_once: Arc<AtomicBool>,
}

impl FlakyService {
    /// A `FlakyService` that never panics - its one-shot flag starts already spent.
    fn benign() -> Self {
        Self {
            panic_once: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl Service<Request> for FlakyService {
    type Output = Response;
    type Error = Error;

    async fn serve(&self, _req: Request) -> Result<Self::Output, Self::Error> {
        if self.panic_once.swap(true, Ordering::SeqCst) {
            Ok(Response::Ping(Ok(())))
        } else {
            panic!("synthetic panic injected for SOL-155270 test coverage")
        }
    }
}

/// A well-behaved server and client must round-trip a request successfully - basic
/// sanity coverage for the plumbing the two tests below build on.
#[tokio::test]
async fn request_response_round_trip() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let (sender, receiver) = bounded_channel(10);
    let cancellation = CancellationToken::new();

    let mut join = JoinSet::new();

    {
        let cancellation = cancellation.clone();
        let server = FlakyService::benign();

        let _ = join.spawn(async move {
            ChannelRequestLayer::new(cancellation)
                .into_layer(server)
                .serve(receiver)
                .await
        });
    }

    let client = RequestChannelService::new(sender);
    client.ping().await?;

    cancellation.cancel();
    let joined = join.join_all().await;
    debug!(?joined);

    Ok(())
}

/// F2: a client that sends a request and then drops its response receiver (simulating
/// a cancelled/disconnected caller) must not end the shared supervisor loop - a second,
/// well-behaved request on the same channel must still succeed.
#[tokio::test]
async fn dropped_response_receiver_does_not_end_server_loop() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let (sender, receiver) = bounded_channel(10);
    let cancellation = CancellationToken::new();

    let mut join = JoinSet::new();

    {
        let cancellation = cancellation.clone();
        let server = FlakyService::benign();

        let _ = join.spawn(async move {
            ChannelRequestLayer::new(cancellation)
                .into_layer(server)
                .serve(receiver)
                .await
        });
    }

    // Send a request directly on the raw channel (bypassing `RequestChannelService`,
    // which always keeps its own receiver alive) so the test controls the oneshot and
    // can drop it - simulating a caller that cancelled or disconnected mid-request.
    let (resp_tx, resp_rx) = oneshot::channel();
    let (started, _) = oneshot::channel();
    sender
        .send(ChannelRequest {
            request: Request::Ping,
            response: resp_tx,
            started,
        })
        .await
        .map_err(|_| TestError::Message(String::from("send failed")))?;
    drop(resp_rx);

    // Give the supervisor loop a generous window to process the request and observe
    // the failed send before asserting it is still alive - and assert that directly,
    // rather than inferring survival only from a later request succeeding, so a
    // regression fails for the right reason instead of an ambiguous timeout.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        join.try_join_next().is_none(),
        "supervisor loop ended after a dropped response receiver"
    );

    // A second, well-behaved request on the same channel must still succeed.
    let client = RequestChannelService::new(sender);
    client.ping().await?;

    cancellation.cancel();
    let joined = join.join_all().await;
    debug!(?joined);

    Ok(())
}

/// Dropping every [`RequestSender`](nisshi_storage::RequestSender) (no cancellation)
/// must end the supervisor loop with `Ok(())`, instead of parking forever on
/// `cancellation.cancelled()` once the channel has closed.
#[tokio::test]
async fn dropping_every_sender_ends_server_loop() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let (sender, receiver) = bounded_channel(10);
    let cancellation = CancellationToken::new();
    let mut join = JoinSet::new();

    {
        let cancellation = cancellation.clone();
        let server = FlakyService::benign();

        let _ = join.spawn(async move {
            ChannelRequestLayer::new(cancellation)
                .into_layer(server)
                .serve(receiver)
                .await
        });
    }

    let client = RequestChannelService::new(sender);
    client.ping().await?;
    drop(client);

    let joined = tokio::time::timeout(Duration::from_secs(2), join.join_next())
        .await
        .map_err(|_elapsed| {
            TestError::Message(String::from(
                "supervisor loop did not end after the channel closed (leaked)",
            ))
        })?;

    assert!(
        matches!(joined, Some(Ok(Ok(())))),
        "supervisor loop ended, but not with Ok(()): {joined:?}"
    );

    Ok(())
}

/// F1: a panic while handling one request must not end the shared supervisor loop -
/// a second, well-behaved request on the same channel must still succeed.
#[tokio::test]
async fn panicking_request_does_not_end_server_loop() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let (sender, receiver) = bounded_channel(10);
    let cancellation = CancellationToken::new();

    let mut join = JoinSet::new();

    {
        let cancellation = cancellation.clone();
        let flaky = FlakyService::default();

        let _ = join.spawn(async move {
            ChannelRequestLayer::new(cancellation)
                .into_layer(flaky)
                .serve(receiver)
                .await
        });
    }

    let client = RequestChannelService::new(sender);

    // First request panics inside its per-request child task; the caller observes a
    // channel failure (its oneshot `tx` was dropped without a send), not a hang.
    assert!(client.ping().await.is_err());

    // The supervisor loop itself must have survived the panic.
    assert!(
        join.try_join_next().is_none(),
        "supervisor loop ended after a panicking request"
    );

    // A second request (the flaky service's panic-once flag is now consumed) must
    // still succeed - proving the shared loop kept serving other callers.
    client.ping().await?;

    cancellation.cancel();
    let joined = join.join_all().await;
    debug!(?joined);

    Ok(())
}

/// A [`Service<Request>`] that fails every request with the error it holds.
#[derive(Clone)]
struct FailingService {
    error: Error,
}

impl Service<Request> for FailingService {
    type Output = Response;
    type Error = Error;

    async fn serve(&self, _req: Request) -> Result<Self::Output, Self::Error> {
        Err(self.error.clone())
    }
}

/// Starts the supervisor loop over `server` and returns a client for it.
fn serve_over_channel<S>(server: S, cancellation: CancellationToken) -> RequestChannelService
where
    S: Service<Request, Output = Response, Error = Error> + Clone,
{
    let (sender, receiver) = bounded_channel(10);

    _ = tokio::spawn(async move {
        ChannelRequestLayer::new(cancellation)
            .into_layer(server)
            .serve(receiver)
            .await
    });

    RequestChannelService::new(sender)
}

/// A storage error reaches the caller as the error that storage returned, with its error
/// code and its classification, and not as a closed channel.
#[tokio::test]
async fn storage_error_reaches_the_caller_with_its_type() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let cancellation = CancellationToken::new();

    let client = serve_over_channel(
        FailingService {
            error: Error::Api(ErrorCode::OutOfOrderSequenceNumber),
        },
        cancellation.clone(),
    );

    let error = client
        .ping()
        .await
        .err()
        .ok_or_else(|| TestError::Message(String::from("the request did not fail")))?;
    assert_matches!(error, Error::Api(ErrorCode::OutOfOrderSequenceNumber));
    assert_eq!(
        ErrorCode::OutOfOrderSequenceNumber,
        error.error_code(ErrorCode::KafkaStorageError)
    );

    cancellation.cancel();

    Ok(())
}

/// A panic in a request's task reaches the caller as a permanent error, and a closed channel
/// stays the transient error that its caller retries.
#[tokio::test]
async fn panicked_request_reaches_the_caller_as_a_permanent_error() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let cancellation = CancellationToken::new();
    let client = serve_over_channel(FlakyService::default(), cancellation.clone());

    let error = client
        .ping()
        .await
        .err()
        .ok_or_else(|| TestError::Message(String::from("the panicked request did not fail")))?;
    assert_matches!(error, Error::Join(_));
    assert!(!error.is_transient());

    cancellation.cancel();

    let (sender, receiver) = bounded_channel(1);
    let closed = RequestChannelService::new(sender);
    drop(receiver);

    let error = closed
        .ping()
        .await
        .err()
        .ok_or_else(|| TestError::Message(String::from("a closed channel did not fail")))?;
    assert!(error.is_transient());

    Ok(())
}

/// The supervisor answers a fetch whose deadline has passed with no batches, without serving it.
#[tokio::test]
async fn expired_fetch_is_answered_without_being_served() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let (sender, receiver) = bounded_channel(10);
    let cancellation = CancellationToken::new();

    let mut join = JoinSet::new();

    {
        let cancellation = cancellation.clone();

        // The service answers every request it serves with a ping, so a fetch answer can only
        // come from the supervisor.
        let _ = join.spawn(async move {
            ChannelRequestLayer::new(cancellation)
                .into_layer(FlakyService::benign())
                .serve(receiver)
                .await
        });
    }

    // The test sends on the raw channel, because `RequestChannelService` does not send a fetch
    // whose deadline has passed.
    let (response, answered) = oneshot::channel();
    let (started, _) = oneshot::channel();
    sender
        .send(ChannelRequest {
            request: Request::Fetch {
                topition: Topition::new("abc", 0),
                offset: 0,
                min_bytes: 1,
                max_bytes: 1024,
                isolation: IsolationLevel::ReadUncommitted,
                deadline: Instant::now(),
            },
            response,
            started,
        })
        .await
        .map_err(|_| TestError::Message(String::from("send failed")))?;

    let answer = answered
        .await
        .map_err(|_| TestError::Message(String::from("no response")))?;

    assert_matches!(&answer, Ok(Response::Fetch(Ok(batches))) if batches.is_empty());

    cancellation.cancel();
    let joined = join.join_all().await;
    debug!(?joined);

    Ok(())
}

/// A [`Service<Request>`] that records how many `serve` calls overlap, yielding inside
/// each call so that overlapping calls would actually interleave.
#[derive(Clone, Default)]
struct InFlightService {
    in_flight: Arc<AtomicUsize>,
    high_water: Arc<AtomicUsize>,
}

impl Service<Request> for InFlightService {
    type Output = Response;
    type Error = Error;

    async fn serve(&self, _req: Request) -> Result<Self::Output, Self::Error> {
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        _ = self.high_water.fetch_max(now, Ordering::SeqCst);

        for _ in 0..5 {
            tokio::task::yield_now().await;
        }

        _ = self.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(Response::Ping(Ok(())))
    }
}

/// The supervisor loop serves one request at a time, so a client of the channel answers a
/// serial DeleteRecords concurrency, without a request over the channel: the service asks
/// before its deadline check, when a request could wait behind the others.
#[tokio::test]
async fn channel_client_answers_a_serial_delete_records_concurrency() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let (sender, receiver) = bounded_channel(1);

    // nothing serves the channel, so a request over it would never answer
    let client = RequestChannelService::new(sender);
    let concurrency = client.delete_records_concurrency();

    assert_eq!(1, concurrency.per_request());
    assert!(concurrency.permits().is_none());

    drop(receiver);

    Ok(())
}

/// mpsc mode's single-writer discipline depends on the supervisor loop serving one
/// request at a time. Concurrent requests through a channel with room for all of them
/// must still never overlap inside storage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_are_served_one_at_a_time() -> Result<(), TestError> {
    let _guard = init_tracing()?;

    let (sender, receiver) = bounded_channel(10);
    let cancellation = CancellationToken::new();
    let server = InFlightService::default();
    let high_water = server.high_water.clone();

    let mut join = JoinSet::new();

    {
        let cancellation = cancellation.clone();

        let _ = join.spawn(async move {
            ChannelRequestLayer::new(cancellation)
                .into_layer(server)
                .serve(receiver)
                .await
        });
    }

    let client = RequestChannelService::new(sender);

    let mut pings = JoinSet::new();
    for _ in 0..8 {
        let client = client.clone();
        let _ = pings.spawn(async move { client.ping().await });
    }

    for ping in pings.join_all().await {
        ping?;
    }

    assert_eq!(1, high_water.load(Ordering::SeqCst));

    cancellation.cancel();
    let joined = join.join_all().await;
    debug!(?joined);

    Ok(())
}
