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

//! An `acks=0` Produce through the proxy, on a real socket: the client reads no
//! response for it, every batch reaches the origin, and the connection answers the
//! client's next request. The origin in these tests answers an `acks=0` Produce as
//! Kafka does, with nothing.

use std::{
    assert_matches,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use nisshi_proxy::Proxy;
use nisshi_sans_io::{
    ApiKey as _, ApiVersionsRequest, ApiVersionsResponse, Body, ConfigType, DescribeConfigsRequest,
    DescribeConfigsResponse, ErrorCode, Frame, FrameInput, Header, MetadataRequest,
    MetadataResponse, ProduceRequest, ProduceResponse, SuppressResponseExtension,
    api_versions_response::ApiVersion,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    record::{Record, deflated, inflated},
};
use nisshi_service::{
    BytesFrameLayer, Error as ServiceError, FrameService, TcpBytesLayer, TcpContextLayer,
    TcpListenerInput, TcpListenerLayer,
};
use rama::{Layer as _, Service as _, error::BoxError, extensions::Extensions};
use tokio::{
    io::{self, AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    task::JoinSet,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use url::Url;

/// The longest that a test waits for one read. The proxy's own wait for an
/// unanswered request is far longer, so a test that hits this limit has found a
/// stalled connection.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a test waits to show that the proxy writes nothing more.
const QUIET_PERIOD: Duration = Duration::from_millis(200);

/// The origin batches a topic whose name starts with this prefix.
const BATCHED_PREFIX: &str = "batched";

/// The origin's `tansu.batch.timeout_ms` for a batched topic.
const BATCH_TIMEOUT_MS: &str = "250";

/// The origin's `tansu.batch.timeout_ms` for [`MIXED_TOPIC`]. It is longer, so that
/// two requests that a test sends together reach the proxy within one batch timeout.
const MIXED_BATCH_TIMEOUT_MS: &str = "1000";

/// A batched topic that a test sends `acks=0` and `acks=1` requests to.
const MIXED_TOPIC: &str = "batched-mixed";

/// The origin rejects every partition of this topic.
const REJECTED_TOPIC: &str = "rejected";

const PRODUCE_VERSION: i16 = 9;
const METADATA_VERSION: i16 = 12;

/// A Produce request as the origin received it.
#[derive(Clone, Debug, PartialEq)]
struct Produced {
    acks: i16,
    base_timestamps: Vec<i64>,
    record_count: u32,
}

/// The Produce requests that reached the origin, in arrival order.
type ProducedAtOrigin = Arc<Mutex<Vec<Produced>>>;

fn describe_configs_response(request: DescribeConfigsRequest) -> DescribeConfigsResponse {
    let config = |name: &str, value: &str| {
        DescribeConfigsResourceResult::default()
            .name(name.into())
            .value(Some(value.into()))
            .config_type(Some(i8::from(ConfigType::String)))
            .config_source(Some(1))
            .synonyms(Some([].into()))
            .documentation(None)
    };

    DescribeConfigsResponse::default()
        .throttle_time_ms(0)
        .results(Some(
            request
                .resources
                .unwrap_or_default()
                .into_iter()
                .map(|resource| {
                    let configs = if resource.resource_name.starts_with(BATCHED_PREFIX) {
                        vec![
                            config("tansu.batch", "true"),
                            config(
                                "tansu.batch.timeout_ms",
                                if resource.resource_name == MIXED_TOPIC {
                                    MIXED_BATCH_TIMEOUT_MS
                                } else {
                                    BATCH_TIMEOUT_MS
                                },
                            ),
                        ]
                    } else {
                        vec![]
                    };

                    DescribeConfigsResult::default()
                        .error_code(ErrorCode::None.into())
                        .resource_type(resource.resource_type)
                        .resource_name(resource.resource_name)
                        .configs(Some(configs))
                })
                .collect(),
        ))
}

fn produce_response(request: ProduceRequest) -> ProduceResponse {
    ProduceResponse::default()
        .responses(request.topic_data.map(|topics| {
            topics
                .into_iter()
                .map(|topic| {
                    let error_code = if topic.name == REJECTED_TOPIC {
                        ErrorCode::UnknownTopicOrPartition
                    } else {
                        ErrorCode::None
                    };

                    TopicProduceResponse::default()
                        .name(topic.name)
                        .partition_responses(topic.partition_data.map(|partitions| {
                            partitions
                                .into_iter()
                                .map(|partition| {
                                    PartitionProduceResponse::default()
                                        .index(partition.index)
                                        .error_code(error_code.into())
                                        .base_offset(0)
                                        .log_append_time_ms(Some(-1))
                                        .log_start_offset(Some(0))
                                        .record_errors(Some([].into()))
                                })
                                .collect()
                        }))
                })
                .collect()
        }))
        .throttle_time_ms(Some(0))
}

fn record_produced(produced: &ProducedAtOrigin, request: &ProduceRequest) {
    let batches = request
        .topic_data
        .iter()
        .flatten()
        .flat_map(|topic| topic.partition_data.iter().flatten())
        .flat_map(|partition| partition.records.iter())
        .flat_map(|records| records.batches.iter())
        .collect::<Vec<_>>();

    produced.lock().unwrap().push(Produced {
        acks: request.acks,
        base_timestamps: batches.iter().map(|batch| batch.base_timestamp).collect(),
        record_count: batches.iter().map(|batch| batch.record_count).sum(),
    });
}

/// Runs an origin that answers as Kafka does: it answers every request except an
/// `acks=0` Produce, which it handles and leaves unanswered.
async fn origin(
    listener: TcpListener,
    produced: ProducedAtOrigin,
    cancellation: CancellationToken,
) -> Result<(), BoxError> {
    let service = (
        TcpListenerLayer::new(cancellation),
        TcpContextLayer::default(),
        TcpBytesLayer,
        BytesFrameLayer::default(),
    )
        .into_layer(FrameService::new(move |input: FrameInput| {
            let api_key = input.frame.api_key()?;
            let correlation_id = input.frame.correlation_id()?;

            let body: Body = match input.frame.body {
                Body::ApiVersionsRequest(_) => ApiVersionsResponse::default()
                    .error_code(ErrorCode::None.into())
                    .api_keys(Some(
                        [
                            (ApiVersionsRequest::KEY, 3),
                            (MetadataRequest::KEY, METADATA_VERSION),
                            (ProduceRequest::KEY, PRODUCE_VERSION),
                            (DescribeConfigsRequest::KEY, 4),
                        ]
                        .into_iter()
                        .map(|(api_key, max_version)| {
                            ApiVersion::default()
                                .api_key(api_key)
                                .min_version(0)
                                .max_version(max_version)
                        })
                        .collect(),
                    ))
                    .throttle_time_ms(Some(0))
                    .into(),

                Body::MetadataRequest(_) => MetadataResponse::default()
                    .brokers(Some([].into()))
                    .topics(Some([].into()))
                    .cluster_id(Some("origin".into()))
                    .controller_id(Some(111))
                    .throttle_time_ms(Some(0))
                    .cluster_authorized_operations(Some(-1))
                    .into(),

                Body::DescribeConfigsRequest(request) => describe_configs_response(request).into(),

                Body::ProduceRequest(request) => {
                    record_produced(&produced, &request);

                    if request.acks == 0 {
                        SuppressResponseExtension::mark(&input.extensions);
                    }

                    produce_response(request).into()
                }

                _ => {
                    return Err(ServiceError::Message(format!(
                        "unexpected api key {api_key}"
                    )));
                }
            };

            Ok::<_, ServiceError>(Frame {
                size: 0,
                header: Header::Response { correlation_id },
                body,
            })
        }));

    service
        .serve(TcpListenerInput {
            listener,
            extensions: Extensions::default(),
        })
        .await
        .map_err(Into::into)
}

/// Starts an origin and a proxy in front of it, and returns the proxy's address and
/// the Produce requests that reach the origin.
async fn proxy_with_origin(
    join: &mut JoinSet<Result<(), BoxError>>,
    cancellation: CancellationToken,
    configure: impl FnOnce(Proxy) -> Proxy,
) -> Result<(SocketAddr, ProducedAtOrigin), BoxError> {
    let produced = ProducedAtOrigin::default();

    let origin_listener = TcpListener::bind("127.0.0.1:0").await?;
    let origin_url = Url::parse(&format!("tcp://{}", origin_listener.local_addr()?))?;
    _ = join.spawn(origin(origin_listener, produced.clone(), cancellation));

    let proxy_listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy_addr = proxy_listener.local_addr()?;
    let proxy_url = Url::parse(&format!("tcp://{proxy_addr}"))?;

    let proxy = configure(Proxy::new(proxy_url.clone(), proxy_url, origin_url));
    _ = join.spawn(async move { proxy.serve(proxy_listener).await.map_err(Into::into) });

    Ok((proxy_addr, produced))
}

fn request_frame(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    body: Body,
) -> Result<Bytes, BoxError> {
    Frame::request(
        Header::Request {
            api_key,
            api_version,
            correlation_id,
            client_id: Some(env!("CARGO_PKG_NAME").into()),
        },
        body,
    )
    .map_err(Into::into)
}

/// Returns a Produce frame with one batch of one record on partition 0. The batch's
/// base timestamp identifies the batch at the origin.
fn produce_frame(
    correlation_id: i32,
    topic: &str,
    acks: i16,
    base_timestamp: i64,
) -> Result<Bytes, BoxError> {
    let batch = inflated::Batch::builder()
        .base_timestamp(base_timestamp)
        .max_timestamp(base_timestamp)
        .record(Record::builder().value(Bytes::from_static(b"value").into()))
        .build()
        .and_then(deflated::Batch::try_from)?;

    request_frame(
        ProduceRequest::KEY,
        PRODUCE_VERSION,
        correlation_id,
        ProduceRequest::default()
            .acks(acks)
            .timeout_ms(30_000)
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
}

fn metadata_frame(correlation_id: i32) -> Result<Bytes, BoxError> {
    request_frame(
        MetadataRequest::KEY,
        METADATA_VERSION,
        correlation_id,
        MetadataRequest::default()
            .topics(Some([].into()))
            .allow_auto_topic_creation(Some(false))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false))
            .into(),
    )
}

/// Reads one response frame and returns its correlation id.
async fn read_correlation_id(stream: &mut TcpStream) -> io::Result<i32> {
    let mut size = [0u8; 4];
    _ = stream.read_exact(&mut size).await?;

    let mut frame = vec![0u8; u32::from_be_bytes(size) as usize];
    _ = stream.read_exact(&mut frame).await?;

    frame
        .get(..4)
        .and_then(|correlation_id| correlation_id.try_into().ok())
        .map(i32::from_be_bytes)
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))
}

/// Reads the next response within [`READ_TIMEOUT`], and returns its correlation id.
async fn next_correlation_id(stream: &mut TcpStream) -> Result<i32, BoxError> {
    Ok(timeout(READ_TIMEOUT, read_correlation_id(stream)).await??)
}

/// Fails when the proxy writes anything more to `stream` within [`QUIET_PERIOD`].
async fn assert_nothing_more(stream: &mut TcpStream) {
    let mut byte = [0u8; 1];
    let read = timeout(QUIET_PERIOD, stream.read(&mut byte)).await;
    assert_matches!(read, Err(_), "unexpected bytes after the last response");
}

fn produced(produced: &ProducedAtOrigin) -> Vec<Produced> {
    produced.lock().unwrap().clone()
}

/// Sends an `acks=1` Produce to `topic` and waits for its response, so that the
/// proxy has the topic's configuration before a test sends `acks=0`.
async fn load_topic_configuration(stream: &mut TcpStream, topic: &str) -> Result<(), BoxError> {
    stream.write_all(&produce_frame(1, topic, 1, 0)?).await?;
    assert_eq!(1, next_correlation_id(stream).await?);
    Ok(())
}

/// Pipelines five `acks=0` Produce requests and a Metadata request on one
/// connection, and checks that the Metadata response is the only response.
async fn pipelined_acks_zero_then_metadata(
    stream: &mut TcpStream,
    topic: &str,
) -> Result<(), BoxError> {
    let mut pipeline = Vec::new();
    for base_timestamp in 1..=5 {
        pipeline.extend_from_slice(&produce_frame(
            i32::try_from(base_timestamp)? + 1,
            topic,
            0,
            base_timestamp,
        )?);
    }
    pipeline.extend_from_slice(&metadata_frame(7)?);
    stream.write_all(&pipeline).await?;

    assert_eq!(7, next_correlation_id(stream).await?);
    assert_nothing_more(stream).await;
    Ok(())
}

#[tokio::test]
async fn pass_through_answers_the_request_after_pipelined_acks_zero_batches() -> Result<(), BoxError>
{
    let cancellation = CancellationToken::new();
    let mut join = JoinSet::new();
    let (proxy_addr, at_origin) =
        proxy_with_origin(&mut join, cancellation.clone(), |proxy| proxy).await?;

    let mut stream = TcpStream::connect(proxy_addr).await?;
    load_topic_configuration(&mut stream, "unbatched").await?;
    pipelined_acks_zero_then_metadata(&mut stream, "unbatched").await?;

    let at_origin = produced(&at_origin);
    assert_eq!(
        vec![0, 1, 2, 3, 4, 5],
        at_origin
            .iter()
            .flat_map(|produced| produced.base_timestamps.clone())
            .collect::<Vec<_>>()
    );
    assert!(
        at_origin.iter().all(|produced| produced.acks == 1),
        "{at_origin:?}"
    );

    cancellation.cancel();
    join.abort_all();
    Ok(())
}

#[tokio::test]
async fn batched_topic_answers_only_the_request_after_pipelined_acks_zero_batches()
-> Result<(), BoxError> {
    let cancellation = CancellationToken::new();
    let mut join = JoinSet::new();
    let (proxy_addr, at_origin) =
        proxy_with_origin(&mut join, cancellation.clone(), |proxy| proxy).await?;

    let mut stream = TcpStream::connect(proxy_addr).await?;
    load_topic_configuration(&mut stream, "batched-pipeline").await?;
    pipelined_acks_zero_then_metadata(&mut stream, "batched-pipeline").await?;

    let at_origin = produced(&at_origin);
    assert_eq!(
        6,
        at_origin
            .iter()
            .map(|produced| produced.record_count)
            .sum::<u32>(),
        "{at_origin:?}"
    );
    assert!(
        at_origin.iter().all(|produced| produced.acks == 1),
        "{at_origin:?}"
    );

    cancellation.cancel();
    join.abort_all();
    Ok(())
}

#[tokio::test]
async fn acks_zero_producers_beyond_the_origin_pool_size_all_complete() -> Result<(), BoxError> {
    const ORIGIN_POOL_MAX_SIZE: usize = 2;
    const PRODUCERS: i64 = 8;

    let cancellation = CancellationToken::new();
    let mut join = JoinSet::new();
    let (proxy_addr, at_origin) = proxy_with_origin(&mut join, cancellation.clone(), |proxy| {
        proxy.origin_pool_max_size(ORIGIN_POOL_MAX_SIZE)
    })
    .await?;

    let mut producers = JoinSet::new();
    for producer in 0..PRODUCERS {
        _ = producers.spawn(async move {
            let mut stream = TcpStream::connect(proxy_addr).await?;

            let mut pipeline = produce_frame(1, "unbatched", 0, producer)?.to_vec();
            pipeline.extend_from_slice(&metadata_frame(2)?);
            stream.write_all(&pipeline).await?;

            assert_eq!(2, next_correlation_id(&mut stream).await?);
            Ok::<_, BoxError>(())
        });
    }

    let completed = timeout(READ_TIMEOUT, producers.join_all()).await?;
    for producer in completed {
        producer?;
    }

    let at_origin = produced(&at_origin);
    let mut base_timestamps = at_origin
        .iter()
        .flat_map(|produced| produced.base_timestamps.clone())
        .collect::<Vec<_>>();
    base_timestamps.sort_unstable();
    assert_eq!((0..PRODUCERS).collect::<Vec<_>>(), base_timestamps);
    assert!(
        at_origin.iter().all(|produced| produced.acks == 1),
        "{at_origin:?}"
    );

    cancellation.cancel();
    join.abort_all();
    Ok(())
}

#[tokio::test]
async fn mixed_batch_answers_only_the_acks_one_client() -> Result<(), BoxError> {
    let cancellation = CancellationToken::new();
    let mut join = JoinSet::new();
    let (proxy_addr, at_origin) =
        proxy_with_origin(&mut join, cancellation.clone(), |proxy| proxy).await?;

    let mut acks_one = TcpStream::connect(proxy_addr).await?;
    let mut acks_zero = TcpStream::connect(proxy_addr).await?;
    load_topic_configuration(&mut acks_one, MIXED_TOPIC).await?;
    let loaded = produced(&at_origin).len();

    // Both requests reach the proxy within one batch timeout, so the proxy
    // combines them into one request to the origin.
    let mut pipeline = produce_frame(10, MIXED_TOPIC, 0, 1)?.to_vec();
    pipeline.extend_from_slice(&metadata_frame(11)?);
    acks_zero.write_all(&pipeline).await?;
    acks_one
        .write_all(&produce_frame(20, MIXED_TOPIC, 1, 2)?)
        .await?;

    assert_eq!(20, next_correlation_id(&mut acks_one).await?);
    assert_eq!(11, next_correlation_id(&mut acks_zero).await?);
    assert_nothing_more(&mut acks_zero).await;

    assert_matches!(
        &produced(&at_origin)[loaded..],
        [Produced {
            acks: 1,
            record_count: 2,
            ..
        }]
    );

    cancellation.cancel();
    join.abort_all();
    Ok(())
}

#[tokio::test]
async fn acks_zero_partition_error_closes_the_client_connection() -> Result<(), BoxError> {
    let cancellation = CancellationToken::new();
    let mut join = JoinSet::new();
    let (proxy_addr, _) = proxy_with_origin(&mut join, cancellation.clone(), |proxy| proxy).await?;

    let mut stream = TcpStream::connect(proxy_addr).await?;

    // An `acks=1` client reads the error in its response, and keeps its connection.
    load_topic_configuration(&mut stream, REJECTED_TOPIC).await?;

    let mut pipeline = produce_frame(2, REJECTED_TOPIC, 0, 1)?.to_vec();
    pipeline.extend_from_slice(&metadata_frame(3)?);
    stream.write_all(&pipeline).await?;

    let read = timeout(READ_TIMEOUT, read_correlation_id(&mut stream)).await;
    assert_matches!(read, Ok(Err(_)), "the proxy must close the connection");

    cancellation.cancel();
    join.abort_all();
    Ok(())
}

#[tokio::test]
async fn acks_one_and_full_isr_reach_the_origin_unchanged() -> Result<(), BoxError> {
    let cancellation = CancellationToken::new();
    let mut join = JoinSet::new();
    let (proxy_addr, at_origin) =
        proxy_with_origin(&mut join, cancellation.clone(), |proxy| proxy).await?;

    let mut stream = TcpStream::connect(proxy_addr).await?;

    stream
        .write_all(&produce_frame(1, "unbatched", 1, 1)?)
        .await?;
    assert_eq!(1, next_correlation_id(&mut stream).await?);

    stream
        .write_all(&produce_frame(2, "unbatched", -1, 2)?)
        .await?;
    assert_eq!(2, next_correlation_id(&mut stream).await?);

    assert_eq!(
        vec![1, -1],
        produced(&at_origin)
            .iter()
            .map(|produced| produced.acks)
            .collect::<Vec<_>>()
    );

    cancellation.cancel();
    join.abort_all();
    Ok(())
}
