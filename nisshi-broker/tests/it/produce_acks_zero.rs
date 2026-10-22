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

//! Real-socket tests for `acks=0` Produce: the broker must never write a
//! response for a successful `acks=0` Produce, and must close the connection
//! (rather than respond) when one fails. These two outcomes only exist at the
//! TCP boundary, so (unlike `produce.rs`'s direct `ProduceService` calls) they
//! need a real listener and a real socket to observe.

use std::{net::SocketAddr, time::Duration};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use nisshi_broker::{NODE_ID, broker::Broker, coordinator::group::administrator::Controller};
use nisshi_sans_io::{
    Ack, ApiKey as _, Body, CreateTopicsRequest, CreateTopicsResponse, ErrorCode, Frame, Header,
    IsolationLevel, ListOffset, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest,
    ProduceRequest, ProduceResponse,
    create_topics_request::CreatableTopic,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, deflated, inflated},
};
use nisshi_storage::ArcDynStorage;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use url::Url;
use uuid::Uuid;

use crate::common::{alphanumeric_string, init_tracing};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

struct RunningBroker {
    handle: JoinHandle<()>,
    addr: SocketAddr,
}

impl Drop for RunningBroker {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// Builds and starts a real broker, as [`tls.rs`][crate::tls]'s `spawn_broker` does,
/// parametrized on the storage URL so the same test body runs against every engine.
async fn spawn_broker(storage: Url) -> Result<RunningBroker> {
    let port = free_port().await?;
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = Url::parse(&format!("tcp://{addr}"))?;

    let mut broker = Broker::<Controller<ArcDynStorage>, ArcDynStorage>::builder()
        .node_id(NODE_ID)
        .cluster_id(format!("acks-zero-{}", Uuid::now_v7()))
        .incarnation_id(Uuid::now_v7())
        .advertised_listener(listener.clone())
        .storage(storage)
        .listener(listener)
        .silent(true)
        .build()
        .await?;

    let handle = tokio::spawn(async move {
        if let Err(err) = broker.serve(Instant::now()).await {
            tracing::debug!(?err);
        }
    });

    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        match TcpStream::connect(addr).await {
            Ok(_) => break,
            Err(_) if Instant::now() < deadline => sleep(Duration::from_millis(50)).await,
            Err(err) => return Err(anyhow!("broker did not start listening on {addr}: {err}")),
        }
    }

    Ok(RunningBroker { handle, addr })
}

async fn write_request<S>(
    stream: &mut S,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: Body,
) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let request = Frame::request(
        Header::Request {
            api_key,
            api_version,
            correlation_id,
            client_id: Some(env!("CARGO_PKG_NAME").into()),
        },
        body,
    )?;

    stream.write_all(&request).await?;
    stream.flush().await?;
    Ok(())
}

/// Reads exactly one response frame's bytes off `stream`, decoding only the
/// header's `correlation_id`: enough to prove *which* response (if any)
/// arrived first, without needing a per-api_key/api_version body decode.
async fn read_one_response_correlation_id<S>(stream: &mut S) -> Result<i32>
where
    S: AsyncRead + Unpin,
{
    let mut size = [0u8; 4];
    _ = stream.read_exact(&mut size).await?;

    let length = usize::try_from(i32::from_be_bytes(size))
        .map_err(|_| anyhow!("negative frame length prefix: {size:02x?}"))?;

    let mut correlation_id = [0u8; 4];
    _ = stream.read_exact(&mut correlation_id).await?;

    // Drain the rest of the frame so the stream is left at the next frame's
    // boundary, in case a test reads again afterwards.
    let mut rest = vec![0u8; length.saturating_sub(correlation_id.len())];
    _ = stream.read_exact(&mut rest).await?;

    Ok(i32::from_be_bytes(correlation_id))
}

async fn round_trip<S>(
    stream: &mut S,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: Body,
) -> Result<Frame>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    write_request(stream, api_key, api_version, correlation_id, body).await?;
    read_response(stream, api_key, api_version).await
}

/// Reads one response frame off `stream` and decodes it as a response to
/// `api_key` at `api_version`.
async fn read_response<S>(stream: &mut S, api_key: i16, api_version: i16) -> Result<Frame>
where
    S: AsyncRead + Unpin,
{
    let mut size = [0u8; 4];
    _ = stream.read_exact(&mut size).await?;

    let length = usize::try_from(i32::from_be_bytes(size))
        .map_err(|_| anyhow!("negative frame length prefix: {size:02x?}"))?;

    let mut buffer = vec![0u8; length + size.len()];
    buffer[..size.len()].copy_from_slice(&size);
    _ = stream.read_exact(&mut buffer[size.len()..]).await?;

    Frame::response_from_bytes(Bytes::from(buffer), api_key, api_version).map_err(Into::into)
}

async fn create_topic<S>(stream: &mut S, name: &str) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let frame = round_trip(
        stream,
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

    let response = CreateTopicsResponse::try_from(frame.body)?;
    let topics = response.topics.unwrap_or_default();

    if topics
        .first()
        .is_some_and(|topic| topic.error_code == i16::from(ErrorCode::None))
    {
        Ok(())
    } else {
        Err(anyhow!("create_topic failed: {topics:?}"))
    }
}

fn produce_body(topic: &str, partition: i32, value: &'static [u8], acks: i16) -> Result<Body> {
    let batch = inflated::Batch::builder()
        .record(Record::builder().value(Some(Bytes::from_static(value))))
        .build()
        .and_then(deflated::Batch::try_from)?;

    Ok(ProduceRequest::default()
        .acks(acks)
        .timeout_ms(5_000)
        .topic_data(Some(
            [TopicProduceData::default()
                .name(topic.into())
                .partition_data(Some(
                    [PartitionProduceData::default()
                        .index(partition)
                        .records(Some(deflated::Frame {
                            batches: vec![batch],
                        }))]
                    .into(),
                ))]
            .into(),
        ))
        .into())
}

/// A batch `ProduceService::partition`'s own `rejection` check rejects with
/// `INVALID_RECORD` before storage is ever touched (no records at all):
/// guaranteed to fail on every backend, unlike an unknown-topic produce,
/// which some backends create lazily on first write.
fn invalid_produce_body(topic: &str, partition: i32, acks: i16) -> Result<Body> {
    let batch = inflated::Batch::builder()
        .last_offset_delta(-1)
        .build()
        .and_then(deflated::Batch::try_from)?;

    Ok(ProduceRequest::default()
        .acks(acks)
        .timeout_ms(5_000)
        .topic_data(Some(
            [TopicProduceData::default()
                .name(topic.into())
                .partition_data(Some(
                    [PartitionProduceData::default()
                        .index(partition)
                        .records(Some(deflated::Frame {
                            batches: vec![batch],
                        }))]
                    .into(),
                ))]
            .into(),
        ))
        .into())
}

/// An idempotent produce with a `producer_id` that was never registered via
/// `InitProducerId`: rejected inside `storage.produce()` itself (`UnknownProducerId`),
/// not by `ProduceService::partition`'s pre-storage `rejection()` check, the
/// same pattern `produce.rs::non_txn_idempotent_unknown_producer_id` already
/// proves is backend-uniform.
fn unknown_producer_id_produce_body(topic: &str, partition: i32, acks: i16) -> Result<Body> {
    let batch = inflated::Batch::builder()
        .producer_id(54345)
        .record(Record::builder().value(Some(Bytes::from_static(b"lorem"))))
        .build()
        .and_then(deflated::Batch::try_from)?;

    Ok(ProduceRequest::default()
        .acks(acks)
        .timeout_ms(5_000)
        .topic_data(Some(
            [TopicProduceData::default()
                .name(topic.into())
                .partition_data(Some(
                    [PartitionProduceData::default()
                        .index(partition)
                        .records(Some(deflated::Frame {
                            batches: vec![batch],
                        }))]
                    .into(),
                ))]
            .into(),
        ))
        .into())
}

/// Confirms via `ListOffsets(Latest)` that the partition's high watermark
/// moved, i.e. storage actually happened even though no Produce response did.
async fn latest_offset<S>(stream: &mut S, topic: &str, partition: i32) -> Result<Option<i64>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let frame = round_trip(
        stream,
        ListOffsetsRequest::KEY,
        // The highest version the broker advertises for ListOffsets.
        6,
        2,
        ListOffsetsRequest::default()
            .isolation_level(Some(IsolationLevel::ReadUncommitted.into()))
            .topics(Some(
                [ListOffsetsTopic::default()
                    .name(topic.into())
                    .partitions(Some(
                        [ListOffsetsPartition::default()
                            .partition_index(partition)
                            .current_leader_epoch(Some(-1))
                            .timestamp(ListOffset::Latest.try_into()?)]
                        .into(),
                    ))]
                .into(),
            ))
            .into(),
    )
    .await?;

    let response = ListOffsetsResponse::try_from(frame.body)?;
    let topics = response.topics.unwrap_or_default();
    let partitions = topics
        .first()
        .and_then(|topic| topic.partitions.as_deref())
        .unwrap_or_default();

    Ok(partitions.first().and_then(|partition| partition.offset))
}

/// A successful `acks=0` Produce must not interpose its response ahead of the
/// next request on the connection: send it with correlation id `A`, then a
/// Metadata request with correlation id `B`, then read exactly one frame and
/// confirm its correlation id is `B`. `TcpBytesService::serve` processes one
/// request at a time per connection, so if a Produce response had been
/// written anyway it would arrive first, with id `A` -- a positive proof of
/// suppression, not just an absence-of-bytes timing guess.
async fn acks_zero_success_is_not_written(storage: Url) -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(storage).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        let topic = &alphanumeric_string(15)[..];
        let partition = 0;

        create_topic(&mut stream, topic).await?;

        write_request(
            &mut stream,
            ProduceRequest::KEY,
            9,
            100,
            produce_body(
                topic,
                partition,
                b"acks zero, no response expected",
                i16::from(Ack::None),
            )?,
        )
        .await?;

        write_request(
            &mut stream,
            MetadataRequest::KEY,
            12,
            200,
            MetadataRequest::default()
                .topics(Some([].into()))
                .allow_auto_topic_creation(Some(false))
                .include_cluster_authorized_operations(Some(false))
                .include_topic_authorized_operations(Some(false))
                .into(),
        )
        .await?;

        let correlation_id = read_one_response_correlation_id(&mut stream).await?;
        assert_eq!(
            200, correlation_id,
            "the first (and only) frame read must be the Metadata response (200), \
             not an interposed Produce response (100)"
        );

        let offset = latest_offset(&mut stream, topic, partition).await?;
        assert_eq!(
            Some(1),
            offset,
            "the acks=0 record must still have been stored"
        );

        Ok(())
    })
    .await?
}

/// Several `acks=0` Produce requests on one connection, then an `acks=1`
/// Produce and a Metadata request: only the last two get a response, in
/// order. The second and third `acks=0` requests reuse the connection's
/// suppression marker after the first one cleared it, and the `acks=1`
/// response proves the marker suppresses only the request that set it.
async fn acks_zero_then_acks_one_on_one_connection(storage: Url) -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(storage).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        let topic = &alphanumeric_string(15)[..];
        let partition = 0;

        create_topic(&mut stream, topic).await?;

        for correlation_id in 100..103 {
            write_request(
                &mut stream,
                ProduceRequest::KEY,
                9,
                correlation_id,
                produce_body(topic, partition, b"acks zero", i16::from(Ack::None))?,
            )
            .await?;
        }

        write_request(
            &mut stream,
            ProduceRequest::KEY,
            9,
            103,
            produce_body(topic, partition, b"acks one", i16::from(Ack::Leader))?,
        )
        .await?;

        write_request(
            &mut stream,
            MetadataRequest::KEY,
            12,
            104,
            MetadataRequest::default()
                .topics(Some([].into()))
                .allow_auto_topic_creation(Some(false))
                .include_cluster_authorized_operations(Some(false))
                .include_topic_authorized_operations(Some(false))
                .into(),
        )
        .await?;

        let produce = read_response(&mut stream, ProduceRequest::KEY, 9).await?;
        assert_eq!(
            Header::Response {
                correlation_id: 103
            },
            produce.header,
            "the first frame must be the acks=1 Produce response"
        );

        let base_offset = ProduceResponse::try_from(produce.body)?
            .responses
            .unwrap_or_default()
            .first()
            .and_then(|topic| topic.partition_responses.as_deref())
            .and_then(|partitions| partitions.first())
            .map(|partition| partition.base_offset);
        assert_eq!(
            Some(3),
            base_offset,
            "the acks=1 record must follow the three acks=0 records"
        );

        let metadata = read_response(&mut stream, MetadataRequest::KEY, 12).await?;
        assert_eq!(
            Header::Response {
                correlation_id: 104
            },
            metadata.header,
            "the second frame must be the Metadata response"
        );

        let offset = latest_offset(&mut stream, topic, partition).await?;
        assert_eq!(Some(4), offset, "all four records must be stored");

        Ok(())
    })
    .await?
}

/// An `acks=0` Produce that fails (here: a record-less batch, rejected with
/// `INVALID_RECORD` before storage is touched) must close the connection
/// rather than respond: a bounded `read_exact` on the connection must observe
/// EOF/reset, never a response frame.
async fn acks_zero_failure_closes_connection(storage: Url) -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(storage).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        let topic = &alphanumeric_string(15)[..];

        create_topic(&mut stream, topic).await?;

        write_request(
            &mut stream,
            ProduceRequest::KEY,
            9,
            1,
            invalid_produce_body(topic, 0, i16::from(Ack::None))?,
        )
        .await?;

        let mut buf = [0u8; 1];
        let outcome = timeout(Duration::from_secs(5), stream.read_exact(&mut buf)).await?;

        assert!(
            outcome.as_ref().is_err_and(|err| matches!(
                err.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            )),
            "an acks=0 produce failure must close the connection, got {outcome:?}"
        );

        Ok(())
    })
    .await?
}

/// An `acks=0` Produce that fails inside `storage.produce()` itself (an
/// idempotent producer id never registered via `InitProducerId`, giving
/// `UnknownProducerId`) must also close the connection: the genuine
/// storage/protocol-layer failure case this ticket exists for, distinct from
/// `acks_zero_failure_closes_connection`'s pre-storage validation rejection.
async fn acks_zero_storage_failure_closes_connection(storage: Url) -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(storage).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        let topic = &alphanumeric_string(15)[..];

        create_topic(&mut stream, topic).await?;

        write_request(
            &mut stream,
            ProduceRequest::KEY,
            9,
            1,
            unknown_producer_id_produce_body(topic, 0, i16::from(Ack::None))?,
        )
        .await?;

        let mut buf = [0u8; 1];
        let outcome = timeout(Duration::from_secs(5), stream.read_exact(&mut buf)).await?;

        assert!(
            outcome.as_ref().is_err_and(|err| matches!(
                err.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            )),
            "an acks=0 produce failure at the storage layer (unknown producer id) \
             must close the connection, got {outcome:?}"
        );

        Ok(())
    })
    .await?
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;

    #[tokio::test]
    async fn success_is_not_written() -> Result<()> {
        let _guard = init_tracing()?;
        acks_zero_success_is_not_written(Url::parse("memory://")?).await
    }

    #[tokio::test]
    async fn acks_zero_then_acks_one() -> Result<()> {
        let _guard = init_tracing()?;
        acks_zero_then_acks_one_on_one_connection(Url::parse("memory://")?).await
    }

    #[tokio::test]
    async fn failure_closes_connection() -> Result<()> {
        let _guard = init_tracing()?;
        acks_zero_failure_closes_connection(Url::parse("memory://")?).await
    }

    #[tokio::test]
    async fn storage_failure_closes_connection() -> Result<()> {
        let _guard = init_tracing()?;
        acks_zero_storage_failure_closes_connection(Url::parse("memory://")?).await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use super::*;

    #[tokio::test]
    async fn success_is_not_written() -> Result<()> {
        let _guard = init_tracing()?;
        acks_zero_success_is_not_written(Url::parse("slatedb://memory")?).await
    }

    #[tokio::test]
    async fn acks_zero_then_acks_one() -> Result<()> {
        let _guard = init_tracing()?;
        acks_zero_then_acks_one_on_one_connection(Url::parse("slatedb://memory")?).await
    }

    #[tokio::test]
    async fn failure_closes_connection() -> Result<()> {
        let _guard = init_tracing()?;
        acks_zero_failure_closes_connection(Url::parse("slatedb://memory")?).await
    }

    #[tokio::test]
    async fn storage_failure_closes_connection() -> Result<()> {
        let _guard = init_tracing()?;
        acks_zero_storage_failure_closes_connection(Url::parse("slatedb://memory")?).await
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    #[tokio::test]
    async fn success_is_not_written() -> Result<()> {
        let _guard = init_tracing()?;
        acks_zero_success_is_not_written(Url::parse(&crate::common::postgres_url())?).await
    }

    #[tokio::test]
    async fn acks_zero_then_acks_one() -> Result<()> {
        let _guard = init_tracing()?;
        acks_zero_then_acks_one_on_one_connection(Url::parse(&crate::common::postgres_url())?).await
    }

    #[tokio::test]
    async fn failure_closes_connection() -> Result<()> {
        let _guard = init_tracing()?;
        acks_zero_failure_closes_connection(Url::parse(&crate::common::postgres_url())?).await
    }

    #[tokio::test]
    async fn storage_failure_closes_connection() -> Result<()> {
        let _guard = init_tracing()?;
        acks_zero_storage_failure_closes_connection(Url::parse(&crate::common::postgres_url())?)
            .await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use std::{env, io::ErrorKind, thread};

    use tokio::fs::remove_file;

    use super::*;

    async fn sqlite_url(name: &str) -> Result<Url> {
        let relative = format!("../logs/{}/{name}.db", env!("CARGO_PKG_NAME"));
        let mut path = env::current_dir()?;
        path.push(&relative);

        match remove_file(path).await {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }?;

        Url::parse(&format!("sqlite://{relative}")).map_err(Into::into)
    }

    #[tokio::test]
    async fn success_is_not_written() -> Result<()> {
        let _guard = init_tracing()?;
        let name = thread::current()
            .name()
            .ok_or_else(|| anyhow!("unnamed thread"))?
            .to_owned();
        acks_zero_success_is_not_written(sqlite_url(&name).await?).await
    }

    #[tokio::test]
    async fn acks_zero_then_acks_one() -> Result<()> {
        let _guard = init_tracing()?;
        let name = thread::current()
            .name()
            .ok_or_else(|| anyhow!("unnamed thread"))?
            .to_owned();
        acks_zero_then_acks_one_on_one_connection(sqlite_url(&name).await?).await
    }

    #[tokio::test]
    async fn failure_closes_connection() -> Result<()> {
        let _guard = init_tracing()?;
        let name = thread::current()
            .name()
            .ok_or_else(|| anyhow!("unnamed thread"))?
            .to_owned();
        acks_zero_failure_closes_connection(sqlite_url(&name).await?).await
    }

    #[tokio::test]
    async fn storage_failure_closes_connection() -> Result<()> {
        let _guard = init_tracing()?;
        let name = thread::current()
            .name()
            .ok_or_else(|| anyhow!("unnamed thread"))?
            .to_owned();
        acks_zero_storage_failure_closes_connection(sqlite_url(&name).await?).await
    }
}

#[cfg(feature = "turso")]
mod turso {
    use std::{env, io::ErrorKind, thread};

    use tokio::fs::remove_file;

    use super::*;

    async fn sqlite_url(name: &str) -> Result<Url> {
        let relative = format!("../logs/{}/{name}.db", env!("CARGO_PKG_NAME"));
        let mut path = env::current_dir()?;
        path.push(&relative);

        match remove_file(path).await {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }?;

        Url::parse(&format!("sqlite://{relative}")).map_err(Into::into)
    }

    #[tokio::test]
    async fn success_is_not_written() -> Result<()> {
        let _guard = init_tracing()?;
        let name = thread::current()
            .name()
            .ok_or_else(|| anyhow!("unnamed thread"))?
            .to_owned();
        acks_zero_success_is_not_written(sqlite_url(&name).await?).await
    }

    #[tokio::test]
    async fn acks_zero_then_acks_one() -> Result<()> {
        let _guard = init_tracing()?;
        let name = thread::current()
            .name()
            .ok_or_else(|| anyhow!("unnamed thread"))?
            .to_owned();
        acks_zero_then_acks_one_on_one_connection(sqlite_url(&name).await?).await
    }

    #[tokio::test]
    async fn failure_closes_connection() -> Result<()> {
        let _guard = init_tracing()?;
        let name = thread::current()
            .name()
            .ok_or_else(|| anyhow!("unnamed thread"))?
            .to_owned();
        acks_zero_failure_closes_connection(sqlite_url(&name).await?).await
    }

    #[tokio::test]
    async fn storage_failure_closes_connection() -> Result<()> {
        let _guard = init_tracing()?;
        let name = thread::current()
            .name()
            .ok_or_else(|| anyhow!("unnamed thread"))?
            .to_owned();
        acks_zero_storage_failure_closes_connection(sqlite_url(&name).await?).await
    }
}
