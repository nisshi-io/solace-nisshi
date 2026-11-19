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

//! `ExtendRoutes` adds a route to a broker, decorates a built-in route, or replaces one.

use std::{
    assert_matches,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, anyhow};
use nisshi_broker::{
    Error, NODE_ID, broker::Broker, coordinator::group::administrator::Controller,
    service::extend_routes::ExtendRoutes,
};
use nisshi_sans_io::{
    ApiKey as _, ApiVersionsRequest, ApiVersionsResponse, DescribeClusterRequest,
    DescribeClusterResponse, ErrorCode, Frame, FrameInput, GetTelemetrySubscriptionsRequest,
    GetTelemetrySubscriptionsResponse, Header, MetadataRequest, MetadataResponse, ProduceRequest,
    ProduceResponse, RequestInput, metadata_request::MetadataRequestTopic,
};
use nisshi_service::{FrameRequestLayer, ResponseService};
use nisshi_storage::{ArcDynStorage, Storage as _};
use rama::{
    Layer as _, Service as _,
    layer::MapErrLayer,
    service::{BoxService, service_fn},
};
use tokio::{
    net::TcpStream,
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use url::Url;
use uuid::Uuid;

use crate::common::{
    init_tracing,
    wire::{free_port, round_trip},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

type Routes = ExtendRoutes<Controller<ArcDynStorage>, ArcDynStorage>;

type MemoryBroker = Broker<Controller<ArcDynStorage>, ArcDynStorage>;

struct BuiltBroker {
    broker: MemoryBroker,
    addr: SocketAddr,
    cluster_id: String,
}

struct RunningBroker {
    addr: SocketAddr,
    cluster_id: String,
    serve: JoinHandle<nisshi_broker::Result<()>>,
}

/// Builds a memory-backed broker, with `extend_routes` when there is one.
async fn build_broker(extend_routes: Option<Routes>) -> Result<BuiltBroker> {
    let addr = SocketAddr::from(([127, 0, 0, 1], free_port().await?));
    let listener = Url::parse(&format!("tcp://{addr}"))?;
    let cluster_id = format!("extend-routes-{}", Uuid::now_v7());

    let builder = MemoryBroker::builder();

    // The call comes before every setter that changes the type of the builder, so a setter that
    // does not carry it over drops it.
    let builder = match extend_routes {
        Some(extend_routes) => builder.extend_routes(extend_routes),
        None => builder,
    };

    let broker = builder
        .node_id(NODE_ID)
        .cluster_id(cluster_id.clone())
        .incarnation_id(Uuid::now_v7())
        .advertised_listener(listener.clone())
        .storage(Url::parse("memory://")?)
        .listener(listener)
        .silent(true)
        .build()
        .await?;

    Ok(BuiltBroker {
        broker,
        addr,
        cluster_id,
    })
}

/// Serves `built` and waits until its listener accepts connections.
async fn start_broker(built: BuiltBroker) -> Result<RunningBroker> {
    let BuiltBroker {
        mut broker,
        addr,
        cluster_id,
    } = built;

    let serve = tokio::spawn(async move { broker.serve(Instant::now()).await });

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
        cluster_id,
        serve,
    })
}

async fn spawn_broker(extend_routes: Option<Routes>) -> Result<RunningBroker> {
    start_broker(build_broker(extend_routes).await?).await
}

/// A service that answers every GetTelemetrySubscriptions with a subscription that requests no
/// metrics.
fn telemetry_subscriptions_service() -> BoxService<FrameInput, Frame, Error> {
    (
        MapErrLayer::new(Error::from),
        FrameRequestLayer::<GetTelemetrySubscriptionsRequest>::new(),
    )
        .into_layer(ResponseService::new(
            |_: RequestInput<GetTelemetrySubscriptionsRequest>| -> Result<_, nisshi_service::Error> {
                Ok(GetTelemetrySubscriptionsResponse::default()
                    .throttle_time_ms(0)
                    .error_code(ErrorCode::None.into())
                    .client_instance_id([0; 16])
                    .subscription_id(0)
                    .accepted_compression_types(Some([].into()))
                    .push_interval_ms(0)
                    .telemetry_max_bytes(0)
                    .delta_temporality(false)
                    .requested_metrics(Some([].into())))
            },
        ))
        .boxed()
}

fn describe_cluster_response(cluster_id: &str) -> DescribeClusterResponse {
    DescribeClusterResponse::default()
        .throttle_time_ms(0)
        .error_code(ErrorCode::None.into())
        .cluster_id(cluster_id.into())
        .controller_id(NODE_ID)
        .brokers(Some([].into()))
        .cluster_authorized_operations(i32::MIN)
}

/// A service that answers every DescribeCluster with the cluster id `cluster_id`.
fn describe_cluster_service(cluster_id: &'static str) -> BoxService<FrameInput, Frame, Error> {
    (
        MapErrLayer::new(Error::from),
        FrameRequestLayer::<DescribeClusterRequest>::new(),
    )
        .into_layer(ResponseService::new(
            move |_: RequestInput<DescribeClusterRequest>| -> Result<_, nisshi_service::Error> {
                Ok(describe_cluster_response(cluster_id))
            },
        ))
        .boxed()
}

/// A service that answers every Produce with no topic responses.
fn produce_service() -> BoxService<FrameInput, Frame, Error> {
    (
        MapErrLayer::new(Error::from),
        FrameRequestLayer::<ProduceRequest>::new(),
    )
        .into_layer(ResponseService::new(
            |_: RequestInput<ProduceRequest>| -> Result<_, nisshi_service::Error> {
                Ok(ProduceResponse::default()
                    .responses(Some([].into()))
                    .throttle_time_ms(Some(0)))
            },
        ))
        .boxed()
}

/// A service that answers every Metadata with no topics.
fn metadata_service() -> BoxService<FrameInput, Frame, Error> {
    (
        MapErrLayer::new(Error::from),
        FrameRequestLayer::<MetadataRequest>::new(),
    )
        .into_layer(ResponseService::new(
            |_: RequestInput<MetadataRequest>| -> Result<_, nisshi_service::Error> {
                Ok(MetadataResponse::default()
                    .throttle_time_ms(Some(0))
                    .brokers(Some([].into()))
                    .cluster_id(Some("replaced".into()))
                    .controller_id(Some(NODE_ID))
                    .topics(Some([].into()))
                    .cluster_authorized_operations(Some(i32::MIN)))
            },
        ))
        .boxed()
}

/// Returns the API key, minimum version and maximum version that the broker at `addr` advertises
/// for each API.
async fn advertised_versions(addr: SocketAddr) -> Result<Vec<(i16, i16, i16)>> {
    let mut stream = TcpStream::connect(addr).await?;

    let frame = round_trip(
        &mut stream,
        ApiVersionsRequest::KEY,
        0,
        1,
        ApiVersionsRequest::default().into(),
    )
    .await?;

    Ok(ApiVersionsResponse::try_from(frame.body)?
        .api_keys
        .unwrap_or_default()
        .iter()
        .map(|api_version| {
            (
                api_version.api_key,
                api_version.min_version,
                api_version.max_version,
            )
        })
        .collect())
}

async fn advertised_api_keys(addr: SocketAddr) -> Result<Vec<i16>> {
    Ok(advertised_versions(addr)
        .await?
        .into_iter()
        .map(|(api_key, _, _)| api_key)
        .collect())
}

async fn produce(addr: SocketAddr, api_version: i16) -> Result<ProduceResponse> {
    let mut stream = TcpStream::connect(addr).await?;

    let frame = round_trip(
        &mut stream,
        ProduceRequest::KEY,
        api_version,
        1,
        ProduceRequest::default()
            .transactional_id(None)
            .acks(1)
            .timeout_ms(1_000)
            .topic_data(Some([].into()))
            .into(),
    )
    .await?;

    ProduceResponse::try_from(frame.body).map_err(Into::into)
}

async fn describe_cluster(addr: SocketAddr) -> Result<DescribeClusterResponse> {
    let mut stream = TcpStream::connect(addr).await?;

    let frame = round_trip(
        &mut stream,
        DescribeClusterRequest::KEY,
        0,
        1,
        DescribeClusterRequest::default()
            .include_cluster_authorized_operations(false)
            .into(),
    )
    .await?;

    DescribeClusterResponse::try_from(frame.body).map_err(Into::into)
}

#[tokio::test]
async fn added_route_answers_an_api_that_no_built_in_route_serves() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        // Without the extension, GetTelemetrySubscriptions has no route, so the extension is what
        // makes the broker serve it.
        let plain = spawn_broker(None).await?;
        assert!(
            !advertised_api_keys(plain.addr)
                .await?
                .contains(&GetTelemetrySubscriptionsRequest::KEY)
        );

        let extended = spawn_broker(Some(ExtendRoutes::new(|builder, _, _| {
            builder
                .with_route(
                    GetTelemetrySubscriptionsRequest::KEY,
                    telemetry_subscriptions_service(),
                )
                .map_err(Into::into)
        })))
        .await?;

        assert!(
            advertised_api_keys(extended.addr)
                .await?
                .contains(&GetTelemetrySubscriptionsRequest::KEY)
        );

        let mut stream = TcpStream::connect(extended.addr).await?;

        let frame = round_trip(
            &mut stream,
            GetTelemetrySubscriptionsRequest::KEY,
            0,
            1,
            GetTelemetrySubscriptionsRequest::default()
                .client_instance_id([0; 16])
                .into(),
        )
        .await?;

        assert_eq!(
            i16::from(ErrorCode::None),
            GetTelemetrySubscriptionsResponse::try_from(frame.body)?.error_code
        );

        Ok(())
    })
    .await?
}

#[tokio::test]
async fn broker_extend_routes_adds_a_route() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let built = build_broker(None).await?;

        let extended = BuiltBroker {
            broker: built
                .broker
                .extend_routes(ExtendRoutes::new(|builder, _, _| {
                    builder
                        .with_route(
                            GetTelemetrySubscriptionsRequest::KEY,
                            telemetry_subscriptions_service(),
                        )
                        .map_err(Into::into)
                })),
            ..built
        };

        let broker = start_broker(extended).await?;

        assert!(
            advertised_api_keys(broker.addr)
                .await?
                .contains(&GetTelemetrySubscriptionsRequest::KEY)
        );

        Ok(())
    })
    .await?
}

#[tokio::test]
async fn replaced_route_answers_instead_of_the_built_in_route() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(Some(ExtendRoutes::new(|builder, _, _| {
            builder
                .wrap_route(DescribeClusterRequest::KEY, |_built_in| {
                    describe_cluster_service("replaced")
                })
                .map_err(Into::into)
        })))
        .await?;

        assert_eq!("replaced", describe_cluster(broker.addr).await?.cluster_id);

        Ok(())
    })
    .await?
}

#[tokio::test]
async fn replaced_produce_route_keeps_the_range_of_the_built_in_route() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(Some(ExtendRoutes::new(|builder, _, _| {
            builder
                .wrap_route(ProduceRequest::KEY, |_built_in| produce_service())
                .map_err(Into::into)
        })))
        .await?;

        let (_, min_version, max_version) = advertised_versions(broker.addr)
            .await?
            .into_iter()
            .find(|(api_key, _, _)| *api_key == ProduceRequest::KEY)
            .ok_or_else(|| anyhow!("Produce is not advertised"))?;

        // Produce advertises a range that starts above the protocol's v0.
        assert!(min_version > 0, "{min_version}");

        for api_version in [min_version, max_version] {
            assert_eq!(
                Some([].into()),
                produce(broker.addr, api_version).await?.responses,
                "version {api_version}"
            );
        }

        // The route table closes the connection for a version below the range, so the
        // replacement never sees the request.
        assert!(produce(broker.addr, min_version - 1).await.is_err());

        Ok(())
    })
    .await?
}

#[tokio::test]
async fn extension_serves_and_advertises_the_versions_that_it_sets() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(Some(ExtendRoutes::new(|builder, _, _| {
            builder
                .wrap_route(ProduceRequest::KEY, |_built_in| produce_service())
                .and_then(|builder| builder.with_versions(ProduceRequest::KEY, 0..=11))
                .map_err(Into::into)
        })))
        .await?;

        assert!(
            advertised_versions(broker.addr)
                .await?
                .contains(&(ProduceRequest::KEY, 0, 11))
        );

        assert_eq!(Some([].into()), produce(broker.addr, 0).await?.responses);

        Ok(())
    })
    .await?
}

#[tokio::test]
async fn extension_cannot_widen_a_capped_route_without_changing_its_handler() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let mut built = build_broker(Some(ExtendRoutes::new(|builder, _, _| {
            builder
                .with_versions(ProduceRequest::KEY, 0..=11)
                .map_err(Into::into)
        })))
        .await?;

        assert_matches!(
            built.broker.serve(Instant::now()).await,
            Err(Error::Service(nisshi_service::Error::VersionRangeNotAllowed { api_key, .. }))
                if api_key == ProduceRequest::KEY
        );

        Ok(())
    })
    .await?
}

#[tokio::test]
async fn replaced_metadata_route_keeps_the_topic_id_layer() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(Some(ExtendRoutes::new(|builder, _, _| {
            builder
                .wrap_route(MetadataRequest::KEY, |_built_in| metadata_service())
                .map_err(Into::into)
        })))
        .await?;

        let mut stream = TcpStream::connect(broker.addr).await?;

        // Metadata v10 refuses a topic ID and a null name. The replacement answers with no
        // topics, so a topic in the response shows that the layer answered.
        let frame = round_trip(
            &mut stream,
            MetadataRequest::KEY,
            10,
            1,
            MetadataRequest::default()
                .topics(Some(vec![
                    MetadataRequestTopic::default()
                        .topic_id(Some([7; 16]))
                        .name(None),
                ]))
                .allow_auto_topic_creation(Some(false))
                .include_cluster_authorized_operations(Some(false))
                .include_topic_authorized_operations(Some(false))
                .into(),
        )
        .await?;

        let topics = MetadataResponse::try_from(frame.body)?
            .topics
            .unwrap_or_default();

        assert_eq!(1, topics.len());
        assert_eq!(i16::from(ErrorCode::InvalidRequest), topics[0].error_code);

        Ok(())
    })
    .await?
}

#[tokio::test]
async fn wrapped_route_delegates_to_the_built_in_route() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let wrapped_calls = Arc::new(AtomicUsize::new(0));
        let counter = wrapped_calls.clone();

        let broker = spawn_broker(Some(ExtendRoutes::new(move |builder, _, _| {
            let counter = counter.clone();

            builder
                .wrap_route(DescribeClusterRequest::KEY, move |built_in| {
                    service_fn(move |request: FrameInput| {
                        let built_in = built_in.clone();
                        let counter = counter.clone();

                        async move {
                            _ = counter.fetch_add(1, Ordering::SeqCst);
                            built_in.serve(request).await
                        }
                    })
                    .boxed()
                })
                .map_err(Into::into)
        })))
        .await?;

        let response = describe_cluster(broker.addr).await?;

        assert_eq!(broker.cluster_id, response.cluster_id);
        assert_eq!(1, wrapped_calls.load(Ordering::SeqCst));

        Ok(())
    })
    .await?
}

#[tokio::test]
async fn extension_receives_the_storage_that_the_broker_serves() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(Some(ExtendRoutes::new(
            |builder, _, storage: &ArcDynStorage| {
                let storage = storage.clone();

                builder
                    .wrap_route(DescribeClusterRequest::KEY, move |_built_in| {
                        service_fn(move |request: FrameInput| {
                            let storage = storage.clone();

                            async move {
                                let cluster_id = storage.cluster_id().await?;

                                Ok::<_, Error>(Frame {
                                    size: 0,
                                    header: Header::Response {
                                        correlation_id: request.frame.correlation_id()?,
                                    },
                                    body: describe_cluster_response(&cluster_id).into(),
                                })
                            }
                        })
                        .boxed()
                    })
                    .map_err(Into::into)
            },
        )))
        .await?;

        assert_eq!(
            broker.cluster_id,
            describe_cluster(broker.addr).await?.cluster_id
        );

        Ok(())
    })
    .await?
}

#[tokio::test]
async fn extension_runs_for_each_connection() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();

        let broker = spawn_broker(Some(ExtendRoutes::new(move |builder, _, _| {
            _ = counter.fetch_add(1, Ordering::SeqCst);
            Ok(builder)
        })))
        .await?;

        // The broker accepts connections one at a time, so after this answer the broker has built
        // the service of every earlier connection, including the one that started the broker.
        _ = advertised_api_keys(broker.addr).await?;
        let before = calls.load(Ordering::SeqCst);

        _ = advertised_api_keys(broker.addr).await?;
        _ = advertised_api_keys(broker.addr).await?;

        assert_eq!(before + 2, calls.load(Ordering::SeqCst));

        Ok(())
    })
    .await?
}

#[tokio::test]
async fn failed_call_for_a_connection_does_not_stop_the_broker() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let fail = Arc::new(AtomicBool::new(false));
        let failing = fail.clone();

        let broker = spawn_broker(Some(ExtendRoutes::new(move |builder, _, _| {
            if failing.load(Ordering::SeqCst) {
                Err(nisshi_service::Error::DuplicateRoute(MetadataRequest::KEY).into())
            } else {
                Ok(builder)
            }
        })))
        .await?;

        fail.store(true, Ordering::SeqCst);

        // The broker drops the connection that it cannot build a service for.
        assert!(advertised_api_keys(broker.addr).await.is_err());

        fail.store(false, Ordering::SeqCst);

        assert!(!advertised_api_keys(broker.addr).await?.is_empty());
        assert!(!broker.serve.is_finished());

        Ok(())
    })
    .await?
}

#[tokio::test]
async fn adding_an_api_that_a_built_in_route_serves_fails_the_start() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let mut built = build_broker(Some(ExtendRoutes::new(|builder, _, _| {
            builder
                .with_route(MetadataRequest::KEY, telemetry_subscriptions_service())
                .map_err(Into::into)
        })))
        .await?;

        assert_matches!(
            built.broker.serve(Instant::now()).await,
            Err(Error::Service(nisshi_service::Error::DuplicateRoute(api_key)))
                if api_key == MetadataRequest::KEY
        );

        Ok(())
    })
    .await?
}

#[tokio::test]
async fn wrapping_an_api_without_a_route_fails_the_start() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let mut built = build_broker(Some(ExtendRoutes::new(|builder, _, _| {
            builder
                .wrap_route(GetTelemetrySubscriptionsRequest::KEY, |built_in| built_in)
                .map_err(Into::into)
        })))
        .await?;

        assert_matches!(
            built.broker.serve(Instant::now()).await,
            Err(Error::Service(nisshi_service::Error::NoRouteToWrap(api_key)))
                if api_key == GetTelemetrySubscriptionsRequest::KEY
        );

        Ok(())
    })
    .await?
}
