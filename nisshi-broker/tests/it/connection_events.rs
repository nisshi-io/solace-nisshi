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

//! The broker's `nisshi::connection` events, over a real socket: one event when a connection is
//! accepted, and one when it closes that names the client, the requests it sent by API, and the
//! topic-partitions it produced to and fetched from.

use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use nisshi_sans_io::{
    ApiKey as _, CreateTopicsRequest, CreateTopicsResponse, FetchRequest, FetchResponse,
    ProduceRequest, ProduceResponse,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated::Frame as RecordBatch, inflated},
};
use tokio::{
    net::TcpStream,
    time::{Instant, sleep, timeout},
};
use tracing::Level;

use crate::common::{
    alphanumeric_string,
    wire::{round_trip, spawn_broker, spawn_broker_with_connection_snapshots},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

const PAYLOAD: &[u8] = b"Lorem ipsum dolor sit amet";

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|err| io::Error::other(err.to_string()))?
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Capture {
    fn text(&self) -> String {
        let written = self
            .0
            .lock()
            .map(|buffer| buffer.clone())
            .unwrap_or_default();
        String::from_utf8_lossy(&written).into_owned()
    }

    /// The captured lines that hold `needle`.
    fn lines_with(&self, needle: &str) -> Vec<String> {
        self.text()
            .lines()
            .filter(|line| line.contains(needle))
            .map(str::to_owned)
            .collect()
    }
}

async fn create_topic(stream: &mut TcpStream, name: &str, partitions: i32) -> Result<()> {
    let frame = round_trip(
        stream,
        CreateTopicsRequest::KEY,
        7,
        1,
        CreateTopicsRequest::default()
            .topics(Some(
                [CreatableTopic::default()
                    .name(name.into())
                    .num_partitions(partitions)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into()))]
                .into(),
            ))
            .timeout_ms(5_000)
            .validate_only(Some(false))
            .into(),
    )
    .await?;

    _ = CreateTopicsResponse::try_from(frame.body)?;

    Ok(())
}

async fn produce(stream: &mut TcpStream, topic: &str, partition: i32) -> Result<()> {
    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Bytes::from_static(PAYLOAD).into()))
        .build()
        .and_then(TryInto::try_into)?;

    let frame = round_trip(
        stream,
        ProduceRequest::KEY,
        9,
        2,
        ProduceRequest::default()
            .acks(-1)
            .timeout_ms(5_000)
            .topic_data(Some(
                [TopicProduceData::default()
                    .name(topic.into())
                    .partition_data(Some(
                        [PartitionProduceData::default()
                            .index(partition)
                            .records(Some(RecordBatch {
                                batches: vec![batch],
                            }))]
                        .into(),
                    ))]
                .into(),
            ))
            .into(),
    )
    .await?;

    _ = ProduceResponse::try_from(frame.body)?;

    Ok(())
}

async fn fetch(stream: &mut TcpStream, topic: &str, partition: i32) -> Result<()> {
    let frame = round_trip(
        stream,
        FetchRequest::KEY,
        12,
        3,
        FetchRequest::default()
            .replica_id(Some(-1))
            .max_wait_ms(100)
            .min_bytes(1)
            .max_bytes(Some(50 * 1024))
            .isolation_level(Some(0))
            .session_id(Some(0))
            .session_epoch(Some(-1))
            .topics(Some(
                [FetchTopic::default()
                    .topic(Some(topic.into()))
                    .partitions(Some(
                        [FetchPartition::default()
                            .partition(partition)
                            .current_leader_epoch(Some(-1))
                            .fetch_offset(0)
                            .last_fetched_epoch(Some(-1))
                            .log_start_offset(Some(-1))
                            .partition_max_bytes(50 * 1024)]
                        .into(),
                    ))]
                .into(),
            ))
            .forgotten_topics_data(Some([].into()))
            .rack_id(Some("".into()))
            .into(),
    )
    .await?;

    _ = FetchResponse::try_from(frame.body)?;

    Ok(())
}

/// Waits for the close event of the connection that sent Produce requests, and returns its line.
///
/// `spawn_broker` opens a connection of its own to see that the broker is listening. That one
/// closes without a request, so the connection that produced is the one to wait for.
async fn closed_connection_that_produced(capture: &Capture) -> Result<String> {
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        let closed = capture
            .lines_with("connection closed")
            .into_iter()
            .find(|line| line.contains("Produce=2"));

        if let Some(closed) = closed {
            return Ok(closed);
        }

        if Instant::now() > deadline {
            return Err(anyhow!("no close event with requests: {}", capture.text()));
        }

        sleep(Duration::from_millis(10)).await;
    }
}

/// A connection that creates a topic, produces to one partition and fetches from it, then
/// closes: the broker logs one open event and one close event for it, and the close event
/// carries the client, the requests by API, and the partition it produced to and fetched from.
#[tokio::test]
async fn a_closed_connection_reports_its_client_requests_and_topic_partitions() -> Result<()> {
    let capture = Capture::default();
    let writer = capture.clone();

    // The subscriber is the thread's default, and the test runs on a current-thread runtime,
    // so the broker's connection task logs on this thread. Only the connection events are
    // enabled, as `RUST_LOG=warn,nisshi::connection=info` enables them.
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_max_level(Level::INFO)
            .with_env_filter("nisshi::connection=info")
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish(),
    );

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(None).await?;
        let topic = alphanumeric_string(15);

        let mut stream = TcpStream::connect(broker.addr).await?;
        create_topic(&mut stream, &topic, 2).await?;
        produce(&mut stream, &topic, 1).await?;
        produce(&mut stream, &topic, 1).await?;
        fetch(&mut stream, &topic, 1).await?;
        drop(stream);

        let closed = closed_connection_that_produced(&capture).await?;

        assert!(
            closed.contains("client_id=\"nisshi-broker\""),
            "client id: {closed}"
        );
        assert!(
            closed.contains("requests=Produce=2,Fetch=1,CreateTopics=1 "),
            "requests by API: {closed}"
        );

        // Two requests of one record each, in the one partition produced to.
        let produced = format!("produce={topic}-1=2/2/");
        assert!(closed.contains(&produced), "produced: {closed}");
        assert!(
            !closed.contains(&format!("{topic}-0=")),
            "partition 0 was neither produced to nor fetched: {closed}"
        );
        assert!(
            closed.contains(&format!("fetch={topic}-1=1 ")),
            "fetched: {closed}"
        );

        // Each accepted connection logged an open event: the probe's, and the test's.
        assert!(
            capture.lines_with("connection opened").len() >= 2,
            "open events: {}",
            capture.text()
        );

        Ok(())
    })
    .await?
}

/// With a snapshot interval, the broker logs the state of a connection while it is still open,
/// and a later snapshot counts it out once it has closed.
#[tokio::test]
async fn snapshots_report_an_open_connection_and_then_count_it_out() -> Result<()> {
    let capture = Capture::default();
    let writer = capture.clone();

    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_max_level(Level::INFO)
            .with_env_filter("nisshi::connection=info")
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish(),
    );

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker_with_connection_snapshots(Duration::from_millis(100)).await?;
        let topic = alphanumeric_string(15);

        let mut stream = TcpStream::connect(broker.addr).await?;
        create_topic(&mut stream, &topic, 1).await?;
        produce(&mut stream, &topic, 0).await?;

        let wait_for = async |needle: &str, from: usize| -> Result<String> {
            let deadline = Instant::now() + Duration::from_secs(10);

            loop {
                if let Some(line) = capture.text()[from..]
                    .lines()
                    .find(|line| line.contains(needle))
                {
                    return Ok(line.to_owned());
                }

                if Instant::now() > deadline {
                    return Err(anyhow!("no {needle:?} line: {}", capture.text()));
                }

                sleep(Duration::from_millis(10)).await;
            }
        };

        let snapshot = wait_for("connection snapshot", 0).await?;
        assert!(
            snapshot.contains("client_id=\"nisshi-broker\"")
                && snapshot.contains("requests=Produce=1,CreateTopics=1 ")
                && snapshot.contains(&format!("produce={topic}-0=1/1/")),
            "snapshot of the open connection: {snapshot}"
        );

        // Only what the broker logs after the connection closes can count it out.
        let after_close = capture.text().len();
        drop(stream);

        let open = wait_for("open=0", after_close).await?;
        assert!(open.contains("connections open"), "{open}");

        Ok(())
    })
    .await?
}
