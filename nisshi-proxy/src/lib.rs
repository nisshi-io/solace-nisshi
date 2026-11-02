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

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
use nisshi_client::{
    BytesConnectionService, ConnectionManager, FrameConnectionLayer, FramePoolLayer,
    RequestConnectionLayer, RequestPoolLayer,
};
use nisshi_otel::meter_provider;
use nisshi_sans_io::{
    ApiKey, DescribeConfigsRequest, ErrorCode, FindCoordinatorRequest, FindCoordinatorResponse,
    MetadataRequest, ProduceRequest, RequestInput, find_coordinator_response::Coordinator,
    topic_config,
};
use nisshi_service::{
    BytesFrameLayer, FrameApiKeyMatcher, FrameRequestLayer, TcpBytesLayer, TcpContextLayer,
    TcpListenerInput, TcpListenerLayer, host_port,
};
use opentelemetry::{InstrumentationScope, global, metrics::Meter};
use opentelemetry_otlp::ExporterBuildError;
use opentelemetry_sdk::error::OTelSdkError;
use opentelemetry_semantic_conventions::SCHEMA_URL;
use rama::{
    Layer, Service,
    extensions::Extensions,
    layer::{HijackLayer, MapErrLayer, MapInputLayer, MapOutputLayer},
};
use std::{
    fmt, io,
    sync::{Arc, LazyLock},
};
use tokio::{
    net::TcpListener,
    task::{JoinError, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing_subscriber::filter::ParseError;
use url::Url;

use crate::{
    acks_zero::AcksZeroLayer,
    produce::BatchProduceLayer,
    topic::{ResourceConfig, ResourceConfigValue, ResourceConfigValueMatcher, TopicConfigLayer},
};

mod acks_zero;
mod metadata;
mod produce;
mod sasl;
mod topic;

#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    Client(#[from] nisshi_client::Error),
    ExporterBuild(Arc<ExporterBuildError>),
    FrameTooBig(usize),
    Io(Arc<io::Error>),
    Join(Arc<JoinError>),
    Otel(#[from] nisshi_otel::Error),
    OtelSdk(Arc<OTelSdkError>),
    ParseFilter(Arc<ParseError>),
    Protocol(#[from] nisshi_sans_io::Error),
    ResourceLock {
        name: String,
        key: Option<String>,
        value: Option<ResourceConfigValue>,
    },
    Service(#[from] nisshi_service::Error),
    UnknownHost(Url),
    Message(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<JoinError> for Error {
    fn from(value: JoinError) -> Self {
        Self::Join(Arc::new(value))
    }
}

impl From<OTelSdkError> for Error {
    fn from(value: OTelSdkError) -> Self {
        Self::OtelSdk(Arc::new(value))
    }
}

impl From<ExporterBuildError> for Error {
    fn from(value: ExporterBuildError) -> Self {
        Self::ExporterBuild(Arc::new(value))
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        Self::ParseFilter(Arc::new(value))
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(Arc::new(value))
    }
}

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

/// The default maximum number of connections from the proxy to its origin.
///
/// Every client of the proxy shares these connections, and the origin holds a fetch or a
/// JoinGroup on its connection until it answers. So the pool needs about one connection
/// for each client that waits on the origin at the same time. The pool opens a connection
/// only when a request needs one, so a large maximum costs nothing while the proxy is
/// quiet.
const ORIGIN_POOL_MAX_SIZE: usize = 256;

#[derive(Clone, Debug)]
pub struct Proxy {
    listener: Url,
    advertised_listener: Url,
    origin: Url,
    origin_pool_max_size: usize,
}

impl Proxy {
    pub fn new(listener: Url, advertised_listener: Url, origin: Url) -> Self {
        Self {
            listener,
            advertised_listener,
            origin,
            origin_pool_max_size: ORIGIN_POOL_MAX_SIZE,
        }
    }

    /// Returns a proxy with the given origin pool maximum size: the most connections
    /// that the proxy opens to its origin, by default 256.
    pub fn origin_pool_max_size(self, origin_pool_max_size: usize) -> Self {
        Self {
            origin_pool_max_size,
            ..self
        }
    }

    pub async fn listen(&self) -> Result<(), Error> {
        let listener = TcpListener::bind(host_port(self.listener.clone()).await?).await?;
        self.serve(listener).await
    }

    /// Serves clients that connect to `listener`, which the caller has already bound,
    /// instead of binding the proxy's listener URL.
    pub async fn serve(&self, listener: TcpListener) -> Result<(), Error> {
        debug!(%self.listener, %self.advertised_listener, %self.origin);

        let configuration = ResourceConfig::default();

        let token = CancellationToken::new();

        let pool = ConnectionManager::builder(self.origin.clone())
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .max_size(self.origin_pool_max_size)
            .build()
            .await
            .inspect(|pool| debug!(?pool))?;

        let request_origin = (
            MapErrLayer::new(Error::from),
            RequestPoolLayer::new(pool.clone()),
            RequestConnectionLayer,
        )
            .into_layer(BytesConnectionService);

        let frame_origin = (
            MapErrLayer::new(Error::from),
            FramePoolLayer::new(pool.clone()),
            FrameConnectionLayer,
        )
            .into_layer(BytesConnectionService);

        let host = String::from(self.advertised_listener.host_str().unwrap_or("localhost"));
        let port = i32::from(self.advertised_listener.port().unwrap_or(9092));

        let meta = HijackLayer::new(
            FrameApiKeyMatcher(MetadataRequest::KEY),
            metadata::service(request_origin.clone(), host.clone(), port),
        );

        let host = String::from(self.advertised_listener.host_str().unwrap_or("localhost"));

        let find_coordinator = HijackLayer::new(
            FrameApiKeyMatcher(FindCoordinatorRequest::KEY),
            (
                FrameRequestLayer::<FindCoordinatorRequest>::new(),
                MapInputLayer::new(move |input: RequestInput<FindCoordinatorRequest>| {
                    FindCoordinatorRequest::default()
                        .key_type(
                            // A v0 request has no key type and the origin may be asked at v1+,
                            // which needs one: v0 coordinators are group coordinators.
                            input.request.key_type.or(Some(0)),
                        )
                        .coordinator_keys(
                            input
                                .request
                                .coordinator_keys
                                .or(input.request.key.map(|key| vec![key])),
                        )
                }),
                MapOutputLayer::new(move |response: FindCoordinatorResponse| {
                    let coordinators = response.coordinators.as_ref().map(|coordinators| {
                        coordinators
                            .iter()
                            .map(|coordinator| {
                                Coordinator::default()
                                    .key(coordinator.key.clone())
                                    .error_code(coordinator.error_code)
                                    .host(host.clone())
                                    .port(port)
                                    .node_id(coordinator.node_id)
                            })
                            .collect()
                    });

                    let coordinator = response
                        .coordinators
                        .as_deref()
                        .and_then(|coordinators| coordinators.first());

                    FindCoordinatorResponse::default()
                        .throttle_time_ms(Some(0))
                        .coordinators(coordinators)
                        .error_code(
                            response
                                .error_code
                                .or(coordinator.map(|coordinator| coordinator.error_code)),
                        )
                        .error_message(
                            response
                                .error_message
                                .or(coordinator
                                    .and_then(|coordinator| coordinator.error_message.clone()))
                                .or(Some("NONE".into())),
                        )
                        .host(Some(host.clone()))
                        .port(Some(port))
                        .node_id(
                            response
                                .node_id
                                .or(coordinator.map(|coordinator| coordinator.node_id)),
                        )
                }),
            )
                .into_layer(request_origin.clone()),
        );

        let produce_origin =
            MapInputLayer::new(|input: RequestInput<ProduceRequest>| input.request)
                .into_layer(request_origin.clone());

        let produce = HijackLayer::new(
            FrameApiKeyMatcher(ProduceRequest::KEY),
            (
                FrameRequestLayer::<ProduceRequest>::new(),
                AcksZeroLayer,
                TopicConfigLayer::new(
                    configuration.clone(),
                    MapInputLayer::new(|input: RequestInput<DescribeConfigsRequest>| input.request)
                        .into_layer(request_origin.clone()),
                ),
            )
                .into_layer(
                    HijackLayer::new(
                        ResourceConfigValueMatcher::new(
                            configuration.clone(),
                            topic_config::BATCH,
                            "true",
                        ),
                        BatchProduceLayer::new(configuration.clone())
                            .into_layer(produce_origin.clone()),
                    )
                    .into_layer(produce_origin.clone()),
                ),
        );

        // We box the frame service, because its nested type is deep enough that the
        // compiler exceeds its query depth limit when it computes the layout of the
        // listener's accept loop.
        let frame = (sasl::reject_layer(), meta, produce, find_coordinator)
            .into_layer(frame_origin)
            .boxed();

        let s = (
            TcpListenerLayer::new(token),
            TcpContextLayer::default(),
            TcpBytesLayer,
            BytesFrameLayer::default(),
        )
            .into_layer(frame);

        s.serve(TcpListenerInput {
            listener,
            extensions: Extensions::default(),
        })
        .await?;

        Ok(())
    }

    pub async fn main(
        listener_url: Url,
        advertised_listener_url: Url,
        origin_url: Url,
        otlp_endpoint_url: Option<Url>,
    ) -> Result<ErrorCode, Error> {
        let mut set = JoinSet::new();

        let meter_provider = otlp_endpoint_url.map_or(Ok(None), |otlp_endpoint_url| {
            meter_provider(otlp_endpoint_url, env!("CARGO_PKG_NAME")).map(Some)
        })?;

        {
            let proxy = Proxy::new(listener_url, advertised_listener_url, origin_url);
            _ = set.spawn(async move { proxy.listen().await });
        }

        loop {
            if set.join_next().await.is_none() {
                break;
            }
        }

        if let Some(meter_provider) = meter_provider {
            meter_provider
                .force_flush()
                .inspect(|force_flush| debug!(?force_flush))?;

            meter_provider
                .shutdown()
                .inspect(|shutdown| debug!(?shutdown))?;
        }

        Ok(ErrorCode::None)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::Arc, thread};

    use nisshi_sans_io::{
        DescribeConfigsRequest, DescribeConfigsResponse, Frame, FrameInput, Header,
        MetadataResponse, ProduceResponse,
    };
    use nisshi_service::{FrameService, RequestApiKeyMatcher, ResponseService};
    use rama::extensions::Extensions;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use super::*;

    fn init_tracing() -> Result<DefaultGuard, Error> {
        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(
                    EnvFilter::from_default_env()
                        .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
                )
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[tokio::test]
    async fn produce_hijack() -> Result<(), Error> {
        let _guard = init_tracing()?;

        const THROTTLE_TIME_MS: Option<i32> = Some(43234);

        let produce = HijackLayer::new(
            FrameApiKeyMatcher(ProduceRequest::KEY),
            FrameRequestLayer::<ProduceRequest>::new().into_layer(ResponseService::new(
                |_req: RequestInput<ProduceRequest>| {
                    Ok::<_, Error>(ProduceResponse::default().throttle_time_ms(THROTTLE_TIME_MS))
                },
            )),
        )
        .into_layer(FrameRequestLayer::<ProduceRequest>::new().into_layer(
            ResponseService::new(|_req: RequestInput<ProduceRequest>| {
                Ok::<_, Error>(ProduceResponse::default())
            }),
        ));

        let frame = produce
            .serve(FrameInput {
                frame: Frame {
                    size: 0,
                    header: Header::Request {
                        api_key: ProduceRequest::KEY,
                        api_version: 12,
                        correlation_id: 12321,
                        client_id: Some("abc".into()),
                    },
                    body: ProduceRequest::default().into(),
                },
                extensions: Extensions::default(),
            })
            .await?;

        let response = ProduceResponse::try_from(frame.body)?;
        assert_eq!(THROTTLE_TIME_MS, response.throttle_time_ms);

        Ok(())
    }

    #[tokio::test]
    async fn request_api_matcher() -> Result<(), Error> {
        let _guard = init_tracing()?;

        const THROTTLE_TIME_MS: Option<i32> = Some(43234);

        let service = HijackLayer::new(
            RequestApiKeyMatcher(ProduceRequest::KEY),
            ResponseService::new(|_req: RequestInput<ProduceRequest>| {
                Ok::<_, Error>(ProduceResponse::default().throttle_time_ms(THROTTLE_TIME_MS))
            }),
        )
        .into_layer(ResponseService::new(
            |_req: RequestInput<ProduceRequest>| Ok::<_, Error>(ProduceResponse::default()),
        ));

        let response = service
            .serve(RequestInput {
                request: ProduceRequest::default(),
                extensions: Extensions::default(),
            })
            .await?;

        assert_eq!(THROTTLE_TIME_MS, response.throttle_time_ms);

        Ok(())
    }

    #[tokio::test]
    async fn frame_topic_config() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let configuration = ResourceConfig::default();
        const THROTTLE_TIME_MS: Option<i32> = Some(43234);

        let service = HijackLayer::new(
            FrameApiKeyMatcher(ProduceRequest::KEY),
            (
                FrameRequestLayer::<ProduceRequest>::new(),
                TopicConfigLayer::new(
                    configuration.clone(),
                    ResponseService::new(|_req: RequestInput<DescribeConfigsRequest>| {
                        Ok::<_, Error>(DescribeConfigsResponse::default())
                    }),
                ),
            )
                .into_layer(ResponseService::new(
                    |_req: RequestInput<ProduceRequest>| {
                        Ok::<_, Error>(
                            ProduceResponse::default().throttle_time_ms(THROTTLE_TIME_MS),
                        )
                    },
                )),
        )
        .into_layer(FrameService::new(|_req: FrameInput| {
            Ok::<_, Error>(Frame {
                size: 0,
                header: Header::Response {
                    correlation_id: 12321,
                },
                body: MetadataResponse::default().into(),
            })
        }));

        let response = service
            .serve(FrameInput {
                frame: Frame {
                    size: 0,
                    header: Header::Request {
                        api_key: ProduceRequest::KEY,
                        api_version: 123,
                        correlation_id: 321,
                        client_id: Some("abc".into()),
                    },
                    body: ProduceRequest::default().into(),
                },
                extensions: Extensions::default(),
            })
            .await?;

        assert!(ProduceResponse::try_from(response.body).is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn response_topic_config() -> Result<(), Error> {
        let configuration = ResourceConfig::default();
        const THROTTLE_TIME_MS: Option<i32> = Some(43234);

        let service = TopicConfigLayer::new(
            configuration,
            ResponseService::new(|_req: RequestInput<DescribeConfigsRequest>| {
                Ok::<_, Error>(DescribeConfigsResponse::default())
            }),
        )
        .layer(ResponseService::new(
            |_req: RequestInput<ProduceRequest>| {
                Ok::<_, Error>(ProduceResponse::default().throttle_time_ms(THROTTLE_TIME_MS))
            },
        ));

        let response = service
            .serve(RequestInput::from(ProduceRequest::default()))
            .await?;

        assert_eq!(THROTTLE_TIME_MS, response.throttle_time_ms);

        Ok(())
    }

    #[tokio::test]
    async fn frame_api_matcher() -> Result<(), Error> {
        let service = HijackLayer::new(
            FrameApiKeyMatcher(ProduceRequest::KEY),
            FrameRequestLayer::<ProduceRequest>::new().into_layer(ResponseService::new(
                |_req: RequestInput<ProduceRequest>| Ok::<_, Error>(ProduceResponse::default()),
            )),
        )
        .into_layer(FrameService::new(|_req: FrameInput| {
            Ok::<_, Error>(Frame {
                size: 0,
                header: Header::Response {
                    correlation_id: 12321,
                },
                body: MetadataResponse::default().into(),
            })
        }));

        let response = service
            .serve(
                Frame {
                    size: 0,
                    header: Header::Request {
                        api_key: ProduceRequest::KEY,
                        api_version: 123,
                        correlation_id: 321,
                        client_id: Some("abc".into()),
                    },
                    body: ProduceRequest::default().into(),
                }
                .into(),
            )
            .await?;

        assert!(ProduceResponse::try_from(response.body).is_ok());

        let response = service
            .serve(
                Frame {
                    size: 0,
                    header: Header::Request {
                        api_key: MetadataRequest::KEY,
                        api_version: 123,
                        correlation_id: 321,
                        client_id: Some("abc".into()),
                    },
                    body: MetadataRequest::default().into(),
                }
                .into(),
            )
            .await?;

        assert!(MetadataResponse::try_from(response.body).is_ok());

        Ok(())
    }
}
