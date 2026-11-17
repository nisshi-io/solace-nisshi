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

//! A route extension adds a route to a broker, decorates a built-in route, or replaces one.

use std::{
    assert_matches,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, anyhow};
use nisshi_broker::{
    Error, NODE_ID, broker::Broker, coordinator::group::administrator::Controller,
    service::route_extension::RouteExtension,
};
use nisshi_sans_io::{
    ApiKey as _, ApiVersionsRequest, ApiVersionsResponse, DescribeClusterRequest,
    DescribeClusterResponse, ErrorCode, Frame, FrameInput, GetTelemetrySubscriptionsRequest,
    GetTelemetrySubscriptionsResponse, MetadataRequest, RequestInput,
};
use nisshi_service::{FrameRequestLayer, ResponseService};
use nisshi_storage::ArcDynStorage;
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

type Routes = RouteExtension<Controller<ArcDynStorage>, ArcDynStorage>;

struct RunningBroker {
    addr: SocketAddr,
    cluster_id: String,
    serve: JoinHandle<nisshi_broker::Result<()>>,
}

/// Starts a memory-backed broker, with `routes` as its route extension when there is one, and
/// waits until its listener accepts connections.
async fn spawn_broker(routes: Option<Routes>) -> Result<RunningBroker> {
    let addr = SocketAddr::from(([127, 0, 0, 1], free_port().await?));
    let listener = Url::parse(&format!("tcp://{addr}"))?;
    let cluster_id = format!("route-extension-{}", Uuid::now_v7());

    let builder = Broker::<Controller<ArcDynStorage>, ArcDynStorage>::builder()
        .node_id(NODE_ID)
        .cluster_id(cluster_id.clone())
        .incarnation_id(Uuid::now_v7())
        .advertised_listener(listener.clone())
        .storage(Url::parse("memory://")?)
        .listener(listener)
        .silent(true);

    let mut broker = match routes {
        Some(routes) => builder.routes(routes),
        None => builder,
    }
    .build()
    .await?;

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

/// A service that answers every DescribeCluster with the cluster id `cluster_id`.
fn describe_cluster_service(cluster_id: &'static str) -> BoxService<FrameInput, Frame, Error> {
    (
        MapErrLayer::new(Error::from),
        FrameRequestLayer::<DescribeClusterRequest>::new(),
    )
        .into_layer(ResponseService::new(
            move |_: RequestInput<DescribeClusterRequest>| -> Result<_, nisshi_service::Error> {
                Ok(DescribeClusterResponse::default()
                    .throttle_time_ms(0)
                    .error_code(ErrorCode::None.into())
                    .cluster_id(cluster_id.into())
                    .controller_id(NODE_ID)
                    .brokers(Some([].into()))
                    .cluster_authorized_operations(i32::MIN))
            },
        ))
        .boxed()
}

async fn advertised_api_keys(addr: SocketAddr) -> Result<Vec<i16>> {
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
        .map(|api_version| api_version.api_key)
        .collect())
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

        let extended = spawn_broker(Some(RouteExtension::new(|builder, _, _| {
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
async fn replaced_route_answers_instead_of_the_built_in_route() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(Some(RouteExtension::new(|builder, _, _| {
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
async fn wrapped_route_delegates_to_the_built_in_route() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let wrapped_calls = Arc::new(AtomicUsize::new(0));
        let counter = wrapped_calls.clone();

        let broker = spawn_broker(Some(RouteExtension::new(move |builder, _, _| {
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
async fn adding_an_api_that_a_built_in_route_serves_fails_the_broker() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        // The readiness probe in `spawn_broker` is the first connection, and building its
        // service is what runs the extension.
        let broker = spawn_broker(Some(RouteExtension::new(|builder, _, _| {
            builder
                .with_route(MetadataRequest::KEY, telemetry_subscriptions_service())
                .map_err(Into::into)
        })))
        .await?;

        assert_matches!(
            broker.serve.await?,
            Err(Error::Service(nisshi_service::Error::DuplicateRoute(api_key)))
                if api_key == MetadataRequest::KEY
        );

        Ok(())
    })
    .await?
}
