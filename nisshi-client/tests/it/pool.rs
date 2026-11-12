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

use std::{
    future::{self, Future},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use deadpool::managed::{PoolError, TimeoutType};
use nisshi_client::{
    Builder, BytesConnectionService, Client, ConnectionManager, Error, FrameConnectionLayer,
    FramePoolLayer, Pool,
};
use nisshi_sans_io::{
    ApiKey as _, ApiVersionsRequest, ApiVersionsResponse, Body, ErrorCode, FetchRequest,
    FetchResponse, Frame, FrameInput, Header, MetadataRequest, MetadataResponse, RootMessageMeta,
    api_versions_response::ApiVersion,
};
use rama::{Layer as _, Service as _, extensions::Extensions};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::{
        Notify,
        mpsc::{self, UnboundedReceiver, UnboundedSender},
    },
    task::JoinSet,
    time::{sleep, timeout},
};
use url::Url;

/// How the fake origin answers one request.
#[derive(Clone, Debug)]
enum Reply {
    Answer,
    AnswerAfter(Duration),
    AnswerThenClose,
    AnswerThenExtraBytes,
    Hang,
    /// Answers with a stale cluster id once the [`Notify`] fires.
    Late(Arc<Notify>),
}

/// What the fake origin did with the request at this index. The index counts every
/// request except ApiVersions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Event {
    Received(usize),
    Replied(usize),
}

type Script = Arc<dyn Fn(usize) -> Reply + Send + Sync>;

const CLUSTER_ID: &str = "abc";
const STALE_CLUSTER_ID: &str = "stale";

/// The time we give the runtime to see a close or bytes from the origin before the pool
/// recycles the connection.
const SETTLE: Duration = Duration::from_millis(100);

/// The longest a test waits for a call or an event, so that a regression fails the test
/// instead of hanging it.
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The upper bound for a timeout that the client should report quickly. It only needs to
/// tell that timeout apart from the default 30s timeouts, so it leaves room for a slow CI
/// runner.
const QUICK: Duration = Duration::from_secs(5);

/// A broker that answers ApiVersions, and answers each Metadata and Fetch request as its
/// [`Script`] says.
struct Origin {
    url: Url,
    accepted: Arc<AtomicUsize>,
    events: UnboundedReceiver<Event>,
    _tasks: JoinSet<()>,
}

impl Origin {
    async fn start(script: impl Fn(usize) -> Reply + Send + Sync + 'static) -> Result<Self, Error> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = Url::parse(&format!("tcp://{}", listener.local_addr()?))?;

        let accepted = Arc::new(AtomicUsize::new(0));
        let (sender, events) = mpsc::unbounded_channel();
        let script: Script = Arc::new(script);

        let mut tasks = JoinSet::new();
        _ = tasks.spawn(accept(listener, script, accepted.clone(), sender));

        Ok(Self {
            url,
            accepted,
            events,
            _tasks: tasks,
        })
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }

    async fn next_event(&mut self) -> Result<Event, Error> {
        bounded(self.events.recv())
            .await?
            .ok_or(Error::Message("origin stopped".into()))
    }

    fn builder(&self) -> Builder {
        ConnectionManager::builder(self.url.clone()).client_id(Some(env!("CARGO_PKG_NAME").into()))
    }
}

async fn accept(
    listener: TcpListener,
    script: Script,
    accepted: Arc<AtomicUsize>,
    events: UnboundedSender<Event>,
) {
    // Dropping this set when the origin stops aborts every connection task.
    let mut connections = JoinSet::new();
    let requests = Arc::new(AtomicUsize::new(0));

    while let Ok((stream, _)) = listener.accept().await {
        _ = accepted.fetch_add(1, Ordering::SeqCst);
        _ = connections.spawn(serve(
            stream,
            script.clone(),
            requests.clone(),
            events.clone(),
        ));
    }
}

async fn serve(
    mut stream: TcpStream,
    script: Script,
    requests: Arc<AtomicUsize>,
    events: UnboundedSender<Event>,
) -> Result<(), Error> {
    loop {
        let mut size = [0u8; 4];
        if stream.read_exact(&mut size).await.is_err() {
            return Ok(());
        }

        let mut buffer = vec![0u8; size.len() + u32::from_be_bytes(size) as usize];
        buffer[..size.len()].copy_from_slice(&size);
        _ = stream.read_exact(&mut buffer[size.len()..]).await?;

        let frame = Frame::request_from_bytes(Bytes::from(buffer))?;
        let api_key = frame.api_key()?;
        let api_version = frame.api_version()?;
        let header = Header::Response {
            correlation_id: frame.correlation_id()?,
        };

        if api_key == ApiVersionsRequest::KEY {
            let versions = Frame::response(header, api_versions().into(), api_key, api_version)?;
            stream.write_all(&versions).await?;
            continue;
        }

        let response = |cluster_id| {
            let body: Body = if api_key == FetchRequest::KEY {
                fetch_response().into()
            } else {
                metadata_response(cluster_id).into()
            };

            Frame::response(header.clone(), body, api_key, api_version)
        };

        let index = requests.fetch_add(1, Ordering::SeqCst);
        _ = events.send(Event::Received(index));

        match script(index) {
            Reply::Answer => stream.write_all(&response(CLUSTER_ID)?).await?,

            Reply::AnswerAfter(delay) => {
                sleep(delay).await;
                stream.write_all(&response(CLUSTER_ID)?).await?;
            }

            Reply::AnswerThenClose => {
                stream.write_all(&response(CLUSTER_ID)?).await?;
                drop(stream);
                _ = events.send(Event::Replied(index));
                return Ok(());
            }

            Reply::AnswerThenExtraBytes => {
                stream.write_all(&response(CLUSTER_ID)?).await?;
                stream.write_all(&[0u8; 4]).await?;
            }

            Reply::Hang => future::pending().await,

            Reply::Late(notify) => {
                notify.notified().await;
                stream.write_all(&response(STALE_CLUSTER_ID)?).await?;
            }
        }

        _ = events.send(Event::Replied(index));
    }
}

fn api_versions() -> ApiVersionsResponse {
    ApiVersionsResponse::default()
        .error_code(ErrorCode::None.into())
        .api_keys(Some(
            RootMessageMeta::messages()
                .requests()
                .iter()
                .filter(|(api_key, _)| {
                    [
                        ApiVersionsRequest::KEY,
                        FetchRequest::KEY,
                        MetadataRequest::KEY,
                    ]
                    .contains(api_key)
                })
                .map(|(_, meta)| {
                    ApiVersion::default()
                        .api_key(meta.api_key)
                        .min_version(meta.version.valid.start)
                        .max_version(meta.version.valid.end)
                })
                .collect(),
        ))
        .throttle_time_ms(Some(0))
}

fn metadata_response(cluster_id: &str) -> MetadataResponse {
    MetadataResponse::default()
        .brokers(Some([].into()))
        .topics(Some([].into()))
        .cluster_id(Some(cluster_id.into()))
        .controller_id(Some(111))
        .throttle_time_ms(Some(0))
        .cluster_authorized_operations(Some(-1))
}

fn fetch_response() -> FetchResponse {
    FetchResponse::default()
        .throttle_time_ms(Some(0))
        .error_code(Some(ErrorCode::None.into()))
        .session_id(Some(0))
        .node_endpoints(Some([].into()))
        .responses(Some([].into()))
}

fn metadata_request() -> MetadataRequest {
    MetadataRequest::default()
        .topics(Some([].into()))
        .allow_auto_topic_creation(Some(false))
        .include_cluster_authorized_operations(Some(false))
        .include_topic_authorized_operations(Some(false))
}

fn fetch_request(max_wait_ms: i32) -> FetchRequest {
    FetchRequest::default()
        .cluster_id(None)
        .replica_id(None)
        .replica_state(None)
        .max_wait_ms(max_wait_ms)
        .min_bytes(1)
        .max_bytes(Some(1_024))
        .isolation_level(Some(0))
        .session_id(Some(-1))
        .session_epoch(Some(-1))
        .topics(Some([].into()))
        .forgotten_topics_data(Some([].into()))
        .rack_id(Some("".into()))
}

/// Fails with an error once [`TEST_TIMEOUT`] passes.
async fn bounded<F: Future>(future: F) -> Result<F::Output, Error> {
    timeout(TEST_TIMEOUT, future)
        .await
        .map_err(|_elapsed| Error::Message(format!("still waiting after {TEST_TIMEOUT:?}")))
}

async fn cluster_id(client: &Client) -> Result<Option<String>, Error> {
    bounded(client.call(metadata_request()))
        .await?
        .map(|response| response.cluster_id)
}

/// Sends a request as `nisshi proxy` does: as a [`Frame`] through the frame layers.
async fn frame_call(pool: &Pool, body: Body, api_key: i16) -> Result<Frame, Error> {
    let api_version = pool.manager().api_version(api_key)?;

    let service = (FramePoolLayer::new(pool.clone()), FrameConnectionLayer)
        .into_layer(BytesConnectionService);

    bounded(service.serve(FrameInput {
        frame: Frame {
            size: 0,
            header: Header::Request {
                api_key,
                api_version,
                correlation_id: 0,
                client_id: Some(env!("CARGO_PKG_NAME").into()),
            },
            body,
        },
        extensions: Extensions::default(),
    }))
    .await?
}

#[tokio::test]
async fn pool_has_wait_and_create_timeouts() -> Result<(), Error> {
    let origin = Origin::start(|_| Reply::Answer).await?;

    let wait_timeout = Duration::from_millis(250);
    let connect_timeout = Duration::from_millis(750);

    let pool = origin
        .builder()
        .wait_timeout(wait_timeout)
        .connect_timeout(connect_timeout)
        .build()
        .await?;

    let timeouts = pool.timeouts();
    assert_eq!(Some(wait_timeout), timeouts.wait);
    assert_eq!(Some(connect_timeout), timeouts.create);

    Ok(())
}

#[tokio::test]
async fn idle_connection_is_reused() -> Result<(), Error> {
    let origin = Origin::start(|_| Reply::Answer).await?;
    let client = origin
        .builder()
        .max_size(1)
        .build()
        .await
        .map(Client::new)?;

    assert_eq!(Some(CLUSTER_ID.into()), cluster_id(&client).await?);
    let accepted = origin.accepted();

    sleep(SETTLE).await;

    assert_eq!(Some(CLUSTER_ID.into()), cluster_id(&client).await?);
    assert_eq!(accepted, origin.accepted());

    Ok(())
}

#[tokio::test]
async fn connection_idle_too_long_is_replaced() -> Result<(), Error> {
    let max_idle = Duration::from_millis(100);

    let origin = Origin::start(|_| Reply::Answer).await?;
    let client = origin
        .builder()
        .max_size(1)
        .max_idle(max_idle)
        .build()
        .await
        .map(Client::new)?;

    assert_eq!(Some(CLUSTER_ID.into()), cluster_id(&client).await?);
    let accepted = origin.accepted();

    sleep(2 * max_idle).await;

    assert_eq!(Some(CLUSTER_ID.into()), cluster_id(&client).await?);
    assert_eq!(accepted + 1, origin.accepted());

    Ok(())
}

#[tokio::test]
async fn hung_origin_times_out_and_frees_connection() -> Result<(), Error> {
    let request_timeout = Duration::from_millis(200);

    let origin = Origin::start(|index| {
        if index == 0 {
            Reply::Hang
        } else {
            Reply::Answer
        }
    })
    .await?;

    let client = origin
        .builder()
        .max_size(1)
        .request_timeout(request_timeout)
        .build()
        .await
        .map(Client::new)?;

    let start = Instant::now();
    let result = cluster_id(&client).await;
    let elapsed = start.elapsed();

    assert!(
        matches!(result, Err(Error::Timeout(timeout)) if timeout == request_timeout),
        "{result:?}"
    );
    assert!(elapsed >= request_timeout, "{elapsed:?}");
    assert!(elapsed < QUICK, "{elapsed:?}");

    let accepted = origin.accepted();
    assert_eq!(Some(CLUSTER_ID.into()), cluster_id(&client).await?);
    assert_eq!(accepted + 1, origin.accepted());

    Ok(())
}

#[tokio::test]
async fn hung_origin_times_out_and_frees_connection_through_frames() -> Result<(), Error> {
    let request_timeout = Duration::from_millis(200);

    let origin = Origin::start(|index| {
        if index == 0 {
            Reply::Hang
        } else {
            Reply::Answer
        }
    })
    .await?;

    let pool = origin
        .builder()
        .max_size(1)
        .request_timeout(request_timeout)
        .build()
        .await?;

    let result = frame_call(&pool, metadata_request().into(), MetadataRequest::KEY).await;
    assert!(
        matches!(result, Err(Error::Timeout(timeout)) if timeout == request_timeout),
        "{result:?}"
    );

    let accepted = origin.accepted();

    let frame = frame_call(&pool, metadata_request().into(), MetadataRequest::KEY).await?;
    let response = MetadataResponse::try_from(frame.body)?;
    assert_eq!(Some(CLUSTER_ID), response.cluster_id.as_deref());
    assert_eq!(accepted + 1, origin.accepted());

    Ok(())
}

#[tokio::test]
async fn fetch_waits_beyond_request_timeout() -> Result<(), Error> {
    let request_timeout = Duration::from_millis(100);

    let origin = Origin::start(move |_| Reply::AnswerAfter(3 * request_timeout)).await?;

    let client = origin
        .builder()
        .request_timeout(request_timeout)
        .build()
        .await
        .map(Client::new)?;

    let response = bounded(client.call(fetch_request(400))).await??;
    assert_eq!(Some(i16::from(ErrorCode::None)), response.error_code);

    Ok(())
}

#[tokio::test]
async fn fetch_waits_beyond_request_timeout_through_frames() -> Result<(), Error> {
    let request_timeout = Duration::from_millis(100);

    let origin = Origin::start(move |_| Reply::AnswerAfter(3 * request_timeout)).await?;

    let pool = origin
        .builder()
        .request_timeout(request_timeout)
        .build()
        .await?;

    let frame = frame_call(&pool, fetch_request(400).into(), FetchRequest::KEY).await?;
    let response = FetchResponse::try_from(frame.body)?;
    assert_eq!(Some(i16::from(ErrorCode::None)), response.error_code);

    Ok(())
}

#[tokio::test]
async fn connection_closed_by_origin_is_replaced() -> Result<(), Error> {
    let mut origin = Origin::start(|index| {
        if index == 0 {
            Reply::AnswerThenClose
        } else {
            Reply::Answer
        }
    })
    .await?;

    let client = origin
        .builder()
        .max_size(1)
        .build()
        .await
        .map(Client::new)?;

    assert_eq!(Some(CLUSTER_ID.into()), cluster_id(&client).await?);
    assert_eq!(Event::Received(0), origin.next_event().await?);
    assert_eq!(Event::Replied(0), origin.next_event().await?);

    sleep(SETTLE).await;

    let accepted = origin.accepted();
    assert_eq!(Some(CLUSTER_ID.into()), cluster_id(&client).await?);
    assert_eq!(accepted + 1, origin.accepted());

    Ok(())
}

#[tokio::test]
async fn connection_with_unread_bytes_is_replaced() -> Result<(), Error> {
    let mut origin = Origin::start(|index| {
        if index == 0 {
            Reply::AnswerThenExtraBytes
        } else {
            Reply::Answer
        }
    })
    .await?;

    let client = origin
        .builder()
        .max_size(1)
        .build()
        .await
        .map(Client::new)?;

    assert_eq!(Some(CLUSTER_ID.into()), cluster_id(&client).await?);
    assert_eq!(Event::Received(0), origin.next_event().await?);
    assert_eq!(Event::Replied(0), origin.next_event().await?);

    sleep(SETTLE).await;

    let accepted = origin.accepted();
    assert_eq!(Some(CLUSTER_ID.into()), cluster_id(&client).await?);
    assert_eq!(accepted + 1, origin.accepted());

    Ok(())
}

#[tokio::test]
async fn cancelled_request_does_not_return_late_response() -> Result<(), Error> {
    let late = Arc::new(Notify::new());

    let mut origin = Origin::start({
        let late = late.clone();
        move |index| {
            if index == 0 {
                Reply::Late(late.clone())
            } else {
                Reply::Answer
            }
        }
    })
    .await?;

    let client = origin
        .builder()
        .max_size(1)
        .build()
        .await
        .map(Client::new)?;

    {
        let call = cluster_id(&client);
        tokio::pin!(call);

        tokio::select! {
            result = &mut call => {
                return Err(Error::Message(format!("unexpected response: {result:?}")));
            }

            event = origin.next_event() => assert_eq!(Event::Received(0), event?),
        }
    }

    let accepted = origin.accepted();

    // The next request starts before the late response arrives, so the connection has
    // nothing to read yet, and only the request still in flight on it tells the pool to
    // discard it. A reused connection would return the late response to this request.
    let mut next = JoinSet::new();
    _ = next.spawn({
        let client = client.clone();
        async move { cluster_id(&client).await }
    });

    sleep(SETTLE).await;
    late.notify_one();

    let result = bounded(next.join_next())
        .await?
        .ok_or(Error::Message("no request".into()))??;

    assert_eq!(Some(CLUSTER_ID.into()), result?);
    assert_eq!(accepted + 1, origin.accepted());

    Ok(())
}

#[tokio::test]
async fn pool_wait_times_out() -> Result<(), Error> {
    let wait_timeout = Duration::from_millis(100);

    let mut origin = Origin::start(|_| Reply::Hang).await?;

    let client = origin
        .builder()
        .max_size(1)
        .wait_timeout(wait_timeout)
        .request_timeout(Duration::from_secs(30))
        .build()
        .await
        .map(Client::new)?;

    let mut hung = JoinSet::new();
    _ = hung.spawn({
        let client = client.clone();
        async move { cluster_id(&client).await }
    });

    assert_eq!(Event::Received(0), origin.next_event().await?);

    let start = Instant::now();
    let result = cluster_id(&client).await;
    let elapsed = start.elapsed();

    let Err(Error::Pool(error)) = result else {
        return Err(Error::Message(format!("expected a pool error: {result:?}")));
    };

    assert!(
        matches!(
            error.downcast_ref::<PoolError<Error>>(),
            Some(PoolError::Timeout(TimeoutType::Wait))
        ),
        "{error:?}"
    );
    assert!(elapsed >= wait_timeout, "{elapsed:?}");
    assert!(elapsed < QUICK, "{elapsed:?}");

    Ok(())
}
