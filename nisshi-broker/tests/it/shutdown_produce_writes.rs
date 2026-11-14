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

//! A broker that shuts down waits for the produce writes that ran on after their request
//! answered at its deadline, within what its connections left of the shutdown grace.
//!
//! The broker runs the memory engine over an object store whose write of a batch waits for the
//! test to let it through, so that one produce has a write in progress when the broker is told
//! to shut down.

#![cfg(feature = "dynostore")]

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use nisshi_broker::{NODE_ID, broker::Broker, coordinator::group::administrator::Controller};
use nisshi_sans_io::{
    Ack, ApiKey as _, CreateTopicsRequest, CreateTopicsResponse, ErrorCode, ProduceRequest,
    ProduceResponse,
    create_topics_request::CreatableTopic,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::PartitionProduceResponse,
    record::{Record, deflated, inflated},
};
use nisshi_storage::ArcDynStorage;
use nisshi_storage_dynostore::DynoStore;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tokio::{
    net::TcpStream,
    sync::{Notify, Semaphore},
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::{
    common::{
        alphanumeric_string, init_tracing,
        wire::{free_port, round_trip},
    },
    shutdown::stuck_connection,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// The `timeout_ms` of each produce: long enough for the steps before the write on a slow
/// machine, and short enough that a test does not wait long for the answer.
const PRODUCE_TIMEOUT_MS: i32 = 1_000;

/// An object store that holds each write of a batch until the test lets it through, and counts
/// the writes that went through. Every other call goes to the store that it wraps, so that the
/// engine's reads and its other writes run as usual.
#[derive(Debug)]
struct GatedRecordWrites<O> {
    inner: O,
    gate: Semaphore,
    reached: Notify,
    completed: AtomicUsize,
}

impl<O> GatedRecordWrites<O> {
    fn new(inner: O) -> Self {
        Self {
            inner,
            gate: Semaphore::new(0),
            reached: Notify::new(),
            completed: AtomicUsize::new(0),
        }
    }

    /// Lets one batch write through the gate.
    fn open(&self) {
        self.gate.add_permits(1);
    }

    /// Waits until a batch write has reached the gate.
    async fn reached(&self) {
        self.reached.notified().await;
    }

    /// The number of batch writes that went through the gate and the store.
    fn completed(&self) -> usize {
        self.completed.load(Ordering::Acquire)
    }
}

impl<O> std::fmt::Display for GatedRecordWrites<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GatedRecordWrites")
    }
}

#[async_trait]
impl<O: ObjectStore> ObjectStore for GatedRecordWrites<O> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        // The engine writes a batch under its partition's `records/` prefix, and nothing else
        // there.
        if location.as_ref().contains("/records/") {
            self.reached.notify_one();

            let permit = self
                .gate
                .acquire()
                .await
                .map_err(|err| object_store::Error::Generic {
                    store: "GatedRecordWrites",
                    source: Box::new(err),
                })?;
            permit.forget();

            let result = self.inner.put_opts(location, payload, opts).await;
            _ = self.completed.fetch_add(1, Ordering::AcqRel);

            return result;
        }

        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

struct RunningBroker {
    addr: SocketAddr,
    token: CancellationToken,
    run: JoinHandle<nisshi_broker::Result<ErrorCode>>,
    writes: Arc<GatedRecordWrites<InMemory>>,
}

/// Starts a broker on the memory engine over a [`GatedRecordWrites`], which shuts down when its
/// token is cancelled, and waits until its listener accepts connections.
async fn run_broker(shutdown_grace: Duration) -> Result<RunningBroker> {
    let writes = Arc::new(GatedRecordWrites::new(InMemory::new()));
    let cluster_id = format!("shutdown-produce-{}", Uuid::now_v7());
    let addr = SocketAddr::from(([127, 0, 0, 1], free_port().await?));
    let listener = Url::parse(&format!("tcp://{addr}"))?;

    let storage: ArcDynStorage = Arc::new(Box::new(
        DynoStore::new(&cluster_id, NODE_ID, Arc::clone(&writes))?
            .advertised_listener(listener.clone())
            .schemas(None)
            .lake(None),
    ));

    let token = CancellationToken::new();

    let broker = Broker::new(
        NODE_ID,
        &cluster_id,
        listener.clone(),
        listener,
        storage.clone(),
        Controller::with_storage(storage)?,
        Uuid::now_v7(),
    )
    .with_cancellation(token.clone())
    .with_shutdown_grace(shutdown_grace);

    let run = tokio::spawn(broker.run(Instant::now()));

    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        match TcpStream::connect(addr).await {
            Ok(_) => break,
            Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(50)).await,
            Err(err) => return Err(anyhow!("broker did not start listening on {addr}: {err}")),
        }
    }

    Ok(RunningBroker {
        addr,
        token,
        run,
        writes,
    })
}

async fn create_topic(client: &mut TcpStream, name: &str) -> Result<()> {
    let frame = round_trip(
        client,
        CreateTopicsRequest::KEY,
        7,
        1,
        CreateTopicsRequest::default()
            .validate_only(Some(false))
            .topics(Some(
                [CreatableTopic::default()
                    .name(name.into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into()))]
                .into(),
            ))
            .into(),
    )
    .await?;

    let topics = CreateTopicsResponse::try_from(frame.body)?
        .topics
        .unwrap_or_default();

    if topics
        .first()
        .is_some_and(|topic| topic.error_code == i16::from(ErrorCode::None))
    {
        Ok(())
    } else {
        Err(anyhow!("create_topic failed: {topics:?}"))
    }
}

/// Produces one record to partition 0 of `topic` with `acks=1`, and returns the partition's
/// response.
async fn produce(client: &mut TcpStream, topic: &str) -> Result<PartitionProduceResponse> {
    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(b"held at the gate"))))
        .build()
        .and_then(deflated::Batch::try_from)?;

    let frame = round_trip(
        client,
        ProduceRequest::KEY,
        9,
        2,
        ProduceRequest::default()
            .acks(i16::from(Ack::Leader))
            .timeout_ms(PRODUCE_TIMEOUT_MS)
            .topic_data(Some(
                [TopicProduceData::default()
                    .name(topic.into())
                    .partition_data(Some(
                        [PartitionProduceData::default()
                            .index(0)
                            .records(Some(deflated::Frame {
                                batches: vec![batch],
                            }))]
                        .into(),
                    ))]
                .into(),
            ))
            .into(),
    )
    .await?;

    ProduceResponse::try_from(frame.body)?
        .responses
        .unwrap_or_default()
        .remove(0)
        .partition_responses
        .unwrap_or_default()
        .pop()
        .ok_or_else(|| anyhow!("the produce response has no partition"))
}

/// Produces to a new topic of `broker`, and returns once the request answered at its deadline
/// with the write held at the gate.
async fn produce_held_at_the_gate(broker: &RunningBroker) -> Result<()> {
    let mut client = TcpStream::connect(broker.addr).await?;
    let topic = alphanumeric_string(15);
    create_topic(&mut client, &topic).await?;

    let partition = produce(&mut client, &topic).await?;
    assert_eq!(
        ErrorCode::RequestTimedOut,
        ErrorCode::try_from(partition.error_code)?,
        "{partition:?}"
    );

    // The deadline also answers `REQUEST_TIMED_OUT` for a write that it did not start, so the
    // test needs the write to have reached the gate.
    timeout(TEST_TIMEOUT, broker.writes.reached())
        .await
        .map_err(|_| anyhow!("the write never reached the gate"))?;

    Ok(())
}

#[tokio::test]
async fn a_write_that_ends_inside_the_grace_is_not_cut() -> Result<()> {
    let _guard = init_tracing()?;

    let broker = run_broker(Duration::from_secs(5)).await?;
    produce_held_at_the_gate(&broker).await?;

    let cancelled = Instant::now();
    broker.token.cancel();

    // The client's connection is idle, so the broker is waiting for the write alone by now.
    sleep(Duration::from_millis(300)).await;
    assert!(
        !broker.run.is_finished(),
        "the broker stopped before the write ended"
    );

    broker.writes.open();

    assert_eq!(ErrorCode::None, timeout(TEST_TIMEOUT, broker.run).await???);
    assert_eq!(
        1,
        broker.writes.completed(),
        "the write did not end before the broker stopped"
    );

    let elapsed = cancelled.elapsed();
    assert!(
        elapsed < Duration::from_secs(3),
        "the broker waited past the write's end: {elapsed:?}"
    );

    Ok(())
}

#[tokio::test]
async fn a_write_that_outlasts_the_grace_does_not_extend_the_shutdown() -> Result<()> {
    let _guard = init_tracing()?;

    let grace = Duration::from_millis(500);
    let broker = run_broker(grace).await?;
    produce_held_at_the_gate(&broker).await?;

    let cancelled = Instant::now();
    broker.token.cancel();

    assert_eq!(ErrorCode::None, timeout(TEST_TIMEOUT, broker.run).await???);

    let elapsed = cancelled.elapsed();
    assert!(
        elapsed >= grace,
        "the broker stopped before its grace: {elapsed:?}"
    );
    assert!(
        elapsed < grace + Duration::from_secs(2),
        "the broker stopped long after its grace: {elapsed:?}"
    );
    assert_eq!(0, broker.writes.completed());

    Ok(())
}

/// A connection that holds the broker for the whole grace leaves the write nothing of it: the
/// broker stops after one grace and the abort of the connection, not after a second grace.
#[tokio::test]
async fn a_stuck_connection_leaves_the_write_no_second_grace() -> Result<()> {
    let _guard = init_tracing()?;

    let grace = Duration::from_secs(3);
    let broker = run_broker(grace).await?;
    produce_held_at_the_gate(&broker).await?;
    let _stuck = stuck_connection(broker.addr).await?;

    let cancelled = Instant::now();
    broker.token.cancel();

    assert_eq!(ErrorCode::None, timeout(TEST_TIMEOUT, broker.run).await???);

    // Two graces would take 6 s. One grace, and the abort of the stuck connection, which takes
    // the broker at most a second, ends under 5 s with a second to spare on a slow machine.
    let elapsed = cancelled.elapsed();
    assert!(
        elapsed >= grace,
        "the broker stopped before its grace: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "the broker gave the write a grace of its own: {elapsed:?}"
    );
    assert_eq!(0, broker.writes.completed());

    Ok(())
}
