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

use std::{collections::BTreeMap, fmt, marker::PhantomData, ops::RangeInclusive, sync::Arc};

use nisshi_sans_io::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, Body, BodyInput, ErrorCode, Frame, FrameInput,
    Header, RequestInput, RootMessageMeta, api_versions_response::ApiVersion,
};
use rama::{Layer, Service, extensions::Extensions, service::BoxService};
use tracing::warn;

use crate::{
    Error,
    version_gate::{protocol_range, validate_versions},
};

/// An [`ApiVersionsResponse`] [`Service`] that advertises, for each API key, the version range
/// that its route serves.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiVersionsService<E> {
    versions: BTreeMap<i16, (i16, i16)>,
    error: PhantomData<E>,
}

impl<E> Service<RequestInput<ApiVersionsRequest>> for ApiVersionsService<E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    type Output = ApiVersionsResponse;
    type Error = E;

    async fn serve(
        &self,
        _req: RequestInput<ApiVersionsRequest>,
    ) -> Result<Self::Output, Self::Error> {
        Ok::<_, E>(
            ApiVersionsResponse::default()
                .finalized_features(Some([].into()))
                .finalized_features_epoch(Some(-1))
                .supported_features(Some([].into()))
                .zk_migration_ready(Some(false))
                .error_code(ErrorCode::None.into())
                .api_keys(Some(
                    self.versions
                        .iter()
                        .map(|(api_key, (min_version, max_version))| {
                            ApiVersion::default()
                                .api_key(*api_key)
                                .min_version(*min_version)
                                .max_version(*max_version)
                        })
                        .collect(),
                ))
                .throttle_time_ms(Some(0)),
        )
    }
}

impl<E> Service<BodyInput> for ApiVersionsService<E>
where
    E: std::error::Error + From<nisshi_sans_io::Error> + Send + Sync + 'static,
{
    type Output = Body;
    type Error = E;

    async fn serve(&self, req: BodyInput) -> Result<Self::Output, Self::Error> {
        let req = ApiVersionsRequest::try_from(req.body).map(|request| RequestInput {
            request,
            extensions: req.extensions,
        })?;
        self.serve(req).await.map(Into::into)
    }
}

impl<E> Service<FrameInput> for ApiVersionsService<E>
where
    E: std::error::Error + From<nisshi_sans_io::Error> + Send + Sync + 'static,
{
    type Output = Frame;
    type Error = E;

    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        let correlation_id = req.frame.correlation_id()?;

        self.serve(BodyInput {
            body: req.frame.body,
            extensions: req.extensions,
        })
        .await
        .map(|body| Frame {
            size: 0,
            header: Header::Response { correlation_id },
            body,
        })
    }
}

/// Route [`Frame`] to a [`Service`] via [API key][`Frame#method.api_key`]
///
/// A simple example that routes [`MetadataRequest`][`nisshi_sans_io::MetadataRequest`]
/// and [`CreateTopicsRequest`][`nisshi_sans_io::CreateTopicsRequest`].
/// [`ApiVersionsRequest`][`nisshi_sans_io::ApiVersionsRequest`] is created by the
///  builder including both of the implemented services using the version ranges
///  from [`RootMessageMeta`][`nisshi_sans_io::RootMessageMeta`].
///
/// ```
/// # use rama::Layer as _;
/// # use nisshi_sans_io::{CreateTopicsRequest, CreateTopicsResponse, MetadataRequest, MetadataResponse};
/// # use nisshi_service::{Error, FrameRouteService, RequestLayer, ResponseService};
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let router = FrameRouteService::<Error>::builder()
///     .with_service(
///         RequestLayer::<MetadataRequest>::new().into_layer(ResponseService::new(|_| {
///             Ok(MetadataResponse::default()
///                 .brokers(Some([].into()))
///                 .topics(Some([].into()))
///                 .cluster_id(Some("nisshi".into()))
///                 .controller_id(Some(111))
///                 .throttle_time_ms(Some(0))
///                 .cluster_authorized_operations(Some(-1)))
///         })),
///     )
///     .and_then(|builder| {
///         builder.with_service(RequestLayer::<CreateTopicsRequest>::new().into_layer(
///             ResponseService::new(|_| {
///                 Ok(CreateTopicsResponse::default()
///                     .throttle_time_ms(Some(0))
///                     .topics(Some([].into())))
///             }),
///         ))
///     })
///     .and_then(|builder| builder.build())?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct FrameRouteService<E = Error> {
    routes: Arc<BTreeMap<i16, ServedRoute<E>>>,
}

/// A route as [`FrameRouteService`] serves it: its service, with the guards of the route applied,
/// and the version range that the service accepts.
#[derive(Clone, Debug)]
struct ServedRoute<E> {
    service: BoxService<FrameInput, Frame, E>,

    /// `None` when this build has no protocol metadata for the route's API key, so no version
    /// is served.
    versions: Option<RangeInclusive<i16>>,
}

impl<E> FrameRouteService<E>
where
    E: std::error::Error + From<nisshi_sans_io::Error> + From<Error> + Send + Sync + 'static,
{
    /// Returns a router for `routes`, each serving the protocol's own version range for its API.
    pub fn new(routes: Arc<BTreeMap<i16, BoxService<FrameInput, Frame, E>>>) -> Self {
        Self {
            routes: Arc::new(
                routes
                    .iter()
                    .map(|(api_key, service)| {
                        (
                            *api_key,
                            ServedRoute {
                                service: service.clone(),
                                versions: protocol_range(*api_key),
                            },
                        )
                    })
                    .collect(),
            ),
        }
    }

    pub fn builder() -> FrameRouteBuilder<E> {
        FrameRouteBuilder::<E>::new()
    }

    /// The number of routes registered, including the `ApiVersions` route `build` always adds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.routes.len()
    }

    /// Whether no routes are registered. `build` always adds an `ApiVersions` route, so this is
    /// only ever true for a [`FrameRouteService`] built some other way.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }

    /// The API key of every registered route.
    pub fn api_keys(&self) -> impl Iterator<Item = i16> + '_ {
        self.routes.keys().copied()
    }
}

/// Whether `api_version` falls inside the Kafka protocol's own declared range for `api_key`,
/// per [`RootMessageMeta`].
///
/// [`FrameRouteService`] closes the connection for a version outside this range instead of
/// passing the frame to a handler, because the handler's request type has no wire shape for
/// that version. `BytesFrameService` answers an `ApiVersions` request outside this range
/// itself, before it decodes the body.
pub(crate) fn is_within_protocol_range(api_key: i16, api_version: i16) -> bool {
    RootMessageMeta::messages()
        .requests()
        .get(&api_key)
        .is_some_and(|meta| meta.version.valid.within(api_version))
}

/// Returns the `ApiVersions` entry for `api_key` with its protocol range, the range that
/// [`is_within_protocol_range`] checks.
pub(crate) fn protocol_api_version(api_key: i16) -> Option<ApiVersion> {
    RootMessageMeta::messages()
        .requests()
        .get(&api_key)
        .map(|meta| {
            ApiVersion::default()
                .api_key(meta.api_key)
                .min_version(meta.version.valid.start)
                .max_version(meta.version.valid.end)
        })
}

impl<E> Service<FrameInput> for FrameRouteService<E>
where
    E: std::error::Error + From<nisshi_sans_io::Error> + From<Error> + Send + Sync + 'static,
{
    type Output = Frame;
    type Error = E;

    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        let api_key = req.frame.api_key()?;

        let Some(route) = self.routes.get(&api_key) else {
            return Err(E::from(Error::UnknownServiceFrame {
                api_key,
                api_name: req.frame.api_name(),
                api_version: req.frame.api_version()?,
                correlation_id: req.frame.correlation_id()?,
                size: req.frame.size,
            }));
        };

        let api_version = req.frame.api_version()?;

        if route
            .versions
            .as_ref()
            .is_some_and(|versions| versions.contains(&api_version))
        {
            return route.service.serve(req).await;
        }

        // The broker closes the connection, as Kafka 3.9.1 does for a version it has not
        // enabled ([SocketServer.scala#L1119-L1125]). `ApiVersions` advertises this range, so
        // only a client that skips the negotiation sends such a version.
        //
        // [SocketServer.scala#L1119-L1125]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/network/SocketServer.scala#L1119-L1125
        warn!(
            api_key,
            api_name = req.frame.api_name(),
            api_version,
            client_id = req.frame.client_id().ok().flatten(),
            supported = ?route.versions,
            "request version outside the supported range"
        );

        Err(E::from(Error::UnsupportedVersion {
            api_key,
            api_version,
        }))
    }
}

/// Routes a bare [`Frame`] with no [`Extensions`], for callers on the extensions-free
/// channel-based path (e.g. [`FrameChannelService`][crate::channel::FrameChannelService]),
/// where there is no ambient extensions to propagate in the first place.
impl<E> Service<Frame> for FrameRouteService<E>
where
    E: std::error::Error + From<nisshi_sans_io::Error> + From<Error> + Send + Sync + 'static,
{
    type Output = Frame;
    type Error = E;

    async fn serve(&self, req: Frame) -> Result<Self::Output, Self::Error> {
        Service::<FrameInput>::serve(
            self,
            FrameInput {
                frame: req,
                extensions: Extensions::default(),
            },
        )
        .await
    }
}

/// A function that wraps the service of a route in a guard.
type Guard<E> =
    Arc<dyn Fn(BoxService<FrameInput, Frame, E>) -> BoxService<FrameInput, Frame, E> + Send + Sync>;

/// A route in a [`FrameRouteBuilder`]: its handler, the guards that wrap the handler when the
/// builder builds, and the version range that it serves.
struct Route<E> {
    handler: BoxService<FrameInput, Frame, E>,

    /// In registration order. The first guard is the outermost.
    guards: Vec<Guard<E>>,

    /// `None` when this build has no protocol metadata for the route's API key.
    versions: Option<RangeInclusive<i16>>,
}

impl<E> Route<E> {
    /// Returns the handler with each guard around it.
    fn guarded(&self) -> BoxService<FrameInput, Frame, E> {
        self.guards
            .iter()
            .rev()
            .fold(self.handler.clone(), |inner, guard| guard(inner))
    }
}

impl<E> fmt::Debug for Route<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Route")
            .field("guards", &self.guards.len())
            .field("versions", &self.versions)
            .finish_non_exhaustive()
    }
}

/// A [`Frame`] route builder providing an [`ApiVersionsResponse`] for all available routes
///
/// A route has a handler, guards and a version range. A guard keeps a promise that the broker
/// makes to every client, so [`FrameRouteBuilder::wrap_route`] changes only the handler, and
/// [`FrameRouteBuilder::build`] puts each guard around whatever handler the route has. The route
/// serves the version range that [`FrameRouteBuilder::with_versions`] sets, or the protocol's
/// own range for its API, and `ApiVersions` advertises that range.
#[derive(Debug)]
pub struct FrameRouteBuilder<E> {
    routes: BTreeMap<i16, Route<E>>,
}

impl<E> FrameRouteBuilder<E>
where
    E: std::error::Error + From<nisshi_sans_io::Error> + Send + Sync + 'static,
{
    fn new() -> Self {
        Self {
            routes: BTreeMap::new(),
        }
    }

    pub fn with_service<S>(self, service: S) -> Result<Self, Error>
    where
        S: Into<BoxService<FrameInput, Frame, E>> + ApiKey,
    {
        self.with_route(S::KEY, service.into())
    }

    /// Registers `service` as the handler of `api_key`, with no guards and with the protocol's
    /// own version range for that API.
    ///
    /// Fails with [`Error::DuplicateRoute`] when `api_key` already has a route.
    pub fn with_route(
        mut self,
        api_key: i16,
        service: BoxService<FrameInput, Frame, E>,
    ) -> Result<Self, Error> {
        if self.routes.contains_key(&api_key) {
            return Err(Error::DuplicateRoute(api_key));
        }

        _ = self.routes.insert(
            api_key,
            Route {
                handler: service,
                guards: Vec::new(),
                versions: protocol_range(api_key),
            },
        );

        Ok(self)
    }

    /// Replaces the handler of `api_key` with the service that `wrap` builds from it. The
    /// guards and the version range of the route stay as they are.
    /// Returns [`Error::NoRouteToWrap`] when no route of `api_key` is registered, so that a
    /// route table that registers it after this call, or not at all, does not build without
    /// what the wrapper adds.
    pub fn wrap_route<F>(mut self, api_key: i16, wrap: F) -> Result<Self, Error>
    where
        F: FnOnce(BoxService<FrameInput, Frame, E>) -> BoxService<FrameInput, Frame, E>,
    {
        let route = self
            .routes
            .get_mut(&api_key)
            .ok_or(Error::NoRouteToWrap(api_key))?;

        route.handler = wrap(route.handler.clone());

        Ok(self)
    }

    /// Adds `guard` around the route of `api_key`, inside the guards that the route already
    /// has. A guard stays around the route when [`Self::wrap_route`] replaces its handler.
    ///
    /// Returns [`Error::NoRouteToGuard`] when no route of `api_key` is registered.
    pub fn with_guard<L>(mut self, api_key: i16, guard: L) -> Result<Self, Error>
    where
        L: Layer<BoxService<FrameInput, Frame, E>> + Send + Sync + 'static,
        L::Service: Service<FrameInput, Output = Frame, Error = E> + Send + Sync + 'static,
    {
        let route = self
            .routes
            .get_mut(&api_key)
            .ok_or(Error::NoRouteToGuard(api_key))?;

        route
            .guards
            .push(Arc::new(move |inner| guard.layer(inner).boxed()));

        Ok(self)
    }

    /// Sets the version range that the route of `api_key` serves and `ApiVersions` advertises.
    ///
    /// Returns [`Error::NoRouteForVersions`] when no route of `api_key` is registered, and
    /// [`Error::CapRangeExceedsProtocolRange`] when `versions` is not a subset of the protocol's
    /// own range for that API.
    pub fn with_versions(
        mut self,
        api_key: i16,
        versions: RangeInclusive<i16>,
    ) -> Result<Self, Error> {
        let route = self
            .routes
            .get_mut(&api_key)
            .ok_or(Error::NoRouteForVersions(api_key))?;

        validate_versions(api_key, versions.clone())?;
        route.versions = Some(versions);

        Ok(self)
    }

    /// The number of routes registered so far (`build` has not yet added `ApiVersions`).
    #[must_use]
    pub fn len(&self) -> usize {
        self.routes.len()
    }

    /// Whether no routes have been registered yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }

    /// The API key of every route registered so far.
    pub fn api_keys(&self) -> impl Iterator<Item = i16> + '_ {
        self.routes.keys().copied()
    }

    pub fn build(self) -> Result<FrameRouteService<E>, Error> {
        let api_key = ApiVersionsRequest::KEY;

        let mut versions = self
            .routes
            .iter()
            .filter_map(|(api_key, route)| {
                route
                    .versions
                    .as_ref()
                    .map(|versions| (*api_key, (*versions.start(), *versions.end())))
            })
            .collect::<BTreeMap<_, _>>();

        if let Some(protocol) = protocol_range(api_key) {
            _ = versions.insert(api_key, (*protocol.start(), *protocol.end()));
        }

        self.with_route(
            api_key,
            ApiVersionsService {
                versions,
                error: PhantomData,
            }
            .boxed(),
        )
        .map(|builder| FrameRouteService {
            routes: Arc::new(
                builder
                    .routes
                    .iter()
                    .map(|(api_key, route)| {
                        (
                            *api_key,
                            ServedRoute {
                                service: route.guarded(),
                                versions: route.versions.clone(),
                            },
                        )
                    })
                    .collect(),
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{assert_matches, sync::Mutex};

    use nisshi_sans_io::{ApiVersionsRequest, ProduceRequest, ProduceResponse};
    use rama::service::service_fn;

    use super::*;

    type Log = Arc<Mutex<Vec<&'static str>>>;

    fn logged(log: &Log) -> Vec<&'static str> {
        log.lock().expect("the log is not poisoned").clone()
    }

    /// A handler that records `name` in `log` when it serves a request.
    fn handler(name: &'static str, log: &Log) -> BoxService<FrameInput, Frame, Error> {
        let log = log.clone();

        service_fn(move |request: FrameInput| {
            let log = log.clone();

            async move {
                log.lock().expect("the log is not poisoned").push(name);

                Ok::<_, Error>(Frame {
                    size: 0,
                    header: Header::Response {
                        correlation_id: request.frame.correlation_id()?,
                    },
                    body: ProduceResponse::default().into(),
                })
            }
        })
        .boxed()
    }

    /// A guard that records `name` in `log` before it passes a request on.
    #[derive(Clone)]
    struct RecordingGuard {
        name: &'static str,
        log: Log,
    }

    struct RecordingGuardService {
        name: &'static str,
        log: Log,
        inner: BoxService<FrameInput, Frame, Error>,
    }

    impl Layer<BoxService<FrameInput, Frame, Error>> for RecordingGuard {
        type Service = RecordingGuardService;

        fn layer(&self, inner: BoxService<FrameInput, Frame, Error>) -> Self::Service {
            RecordingGuardService {
                name: self.name,
                log: self.log.clone(),
                inner,
            }
        }
    }

    impl Service<FrameInput> for RecordingGuardService {
        type Output = Frame;
        type Error = Error;

        async fn serve(&self, request: FrameInput) -> Result<Frame, Error> {
            self.log
                .lock()
                .expect("the log is not poisoned")
                .push(self.name);

            self.inner.serve(request).await
        }
    }

    fn produce_frame(api_version: i16) -> Frame {
        Frame {
            size: 0,
            header: Header::Request {
                api_key: ProduceRequest::KEY,
                api_version,
                correlation_id: 1,
                client_id: None,
            },
            body: ProduceRequest::default().into(),
        }
    }

    /// The version range that `ApiVersions` of `route` advertises for Produce.
    async fn advertised_produce_versions(route: &FrameRouteService<Error>) -> Option<(i16, i16)> {
        let response = route
            .serve(api_versions_frame(4))
            .await
            .expect("an ApiVersions request within the protocol range");

        ApiVersionsResponse::try_from(response.body)
            .expect("an ApiVersions response")
            .api_keys
            .unwrap_or_default()
            .into_iter()
            .find(|version| version.api_key == ProduceRequest::KEY)
            .map(|version| (version.min_version, version.max_version))
    }

    // A client negotiates a version from what `ApiVersions` advertises, so a route advertises
    // the range that it serves, not the protocol's wider range, or the client picks a version
    // that the router rejects.
    #[tokio::test]
    async fn api_versions_response_uses_the_range_of_the_route() {
        let service = ApiVersionsService::<Error> {
            versions: BTreeMap::from([(ProduceRequest::KEY, (3, 11))]),
            error: PhantomData,
        };

        let response = service
            .serve(RequestInput {
                request: ApiVersionsRequest::default(),
                extensions: Extensions::default(),
            })
            .await
            .expect("ApiVersionsService::serve is infallible here");

        let produce = response
            .api_keys
            .unwrap_or_default()
            .into_iter()
            .find(|version| version.api_key == ProduceRequest::KEY)
            .expect("Produce is in the supported list");

        assert_eq!(3, produce.min_version);
        assert_eq!(11, produce.max_version);
    }

    // Produce's own protocol range is 0-11.
    #[test]
    fn version_within_protocol_range_is_accepted() {
        assert!(is_within_protocol_range(ProduceRequest::KEY, 11));
    }

    #[test]
    fn version_outside_protocol_range_is_rejected() {
        assert!(!is_within_protocol_range(ProduceRequest::KEY, 12));
    }

    #[test]
    fn unknown_api_key_is_rejected() {
        assert!(!is_within_protocol_range(i16::MAX, 0));
    }

    fn api_versions_frame(api_version: i16) -> Frame {
        Frame {
            size: 0,
            header: Header::Request {
                api_key: ApiVersionsRequest::KEY,
                api_version,
                correlation_id: 0,
                client_id: None,
            },
            body: ApiVersionsRequest::default().into(),
        }
    }

    fn assert_unsupported_version(result: Result<Frame, Error>, expected_version: i16) {
        assert_unsupported_version_for(result, ApiVersionsRequest::KEY, expected_version);
    }

    fn assert_unsupported_version_of(result: Result<Frame, Error>, expected_version: i16) {
        assert_unsupported_version_for(result, ProduceRequest::KEY, expected_version);
    }

    fn assert_unsupported_version_for(
        result: Result<Frame, Error>,
        expected_api_key: i16,
        expected_version: i16,
    ) {
        assert_matches!(
            result,
            Err(Error::UnsupportedVersion {
                api_key,
                api_version,
            }) if api_key == expected_api_key && api_version == expected_version
        );
    }

    // The `ApiVersions` route answers any request it is given, so a frame outside the protocol
    // range (0-4) reaches the handler only if the backstop lets it through.
    #[tokio::test]
    async fn frame_input_outside_protocol_range_is_rejected() {
        let frame_route = FrameRouteService::<Error>::builder()
            .build()
            .expect("a route table with only the ApiVersions route");

        assert_unsupported_version(
            frame_route
                .serve(FrameInput {
                    frame: api_versions_frame(9),
                    extensions: Extensions::default(),
                })
                .await,
            9,
        );
    }

    #[tokio::test]
    async fn frame_outside_protocol_range_is_rejected() {
        let frame_route = FrameRouteService::<Error>::builder()
            .build()
            .expect("a route table with only the ApiVersions route");

        assert_unsupported_version(frame_route.serve(api_versions_frame(9)).await, 9);
    }

    #[tokio::test]
    async fn frame_within_protocol_range_is_routed() {
        let frame_route = FrameRouteService::<Error>::builder()
            .build()
            .expect("a route table with only the ApiVersions route");

        let response = frame_route
            .serve(api_versions_frame(4))
            .await
            .expect("v4 is within the ApiVersions protocol range");

        assert!(ApiVersionsResponse::try_from(response.body).is_ok());
    }

    #[test]
    fn wrapping_an_unregistered_route_is_an_error() {
        let result = FrameRouteService::<Error>::builder()
            .wrap_route(ProduceRequest::KEY, |route| route)
            .map(|builder| builder.len());

        assert_matches!(result, Err(Error::NoRouteToWrap(api_key)) if api_key == ProduceRequest::KEY);
    }

    #[tokio::test]
    async fn wrapped_route_keeps_its_guards() {
        let log = Log::default();

        let route = FrameRouteService::<Error>::builder()
            .with_route(ProduceRequest::KEY, handler("built-in", &log))
            .and_then(|builder| {
                builder.with_guard(
                    ProduceRequest::KEY,
                    RecordingGuard {
                        name: "guard",
                        log: log.clone(),
                    },
                )
            })
            .and_then(|builder| {
                builder.wrap_route(ProduceRequest::KEY, |_built_in| {
                    handler("replacement", &log)
                })
            })
            .and_then(FrameRouteBuilder::build)
            .expect("a route table with a guarded Produce route");

        _ = route
            .serve(produce_frame(3))
            .await
            .expect("Produce v3 is within the protocol range");

        assert_eq!(vec!["guard", "replacement"], logged(&log));
    }

    #[tokio::test]
    async fn the_first_guard_of_a_route_is_the_outermost() {
        let log = Log::default();

        let route = FrameRouteService::<Error>::builder()
            .with_route(ProduceRequest::KEY, handler("handler", &log))
            .and_then(|builder| {
                ["first", "second"]
                    .into_iter()
                    .try_fold(builder, |builder, name| {
                        builder.with_guard(
                            ProduceRequest::KEY,
                            RecordingGuard {
                                name,
                                log: log.clone(),
                            },
                        )
                    })
            })
            .and_then(FrameRouteBuilder::build)
            .expect("a route table with a Produce route and two guards");

        _ = route
            .serve(produce_frame(3))
            .await
            .expect("Produce v3 is within the protocol range");

        assert_eq!(vec!["first", "second", "handler"], logged(&log));
    }

    #[tokio::test]
    async fn route_serves_and_advertises_the_protocol_range_until_versions_are_set() {
        let log = Log::default();

        let route = FrameRouteService::<Error>::builder()
            .with_route(ProduceRequest::KEY, handler("handler", &log))
            .and_then(FrameRouteBuilder::build)
            .expect("a route table with a Produce route");

        // Produce's protocol range is 0-11.
        assert_eq!(Some((0, 11)), advertised_produce_versions(&route).await);

        _ = route
            .serve(produce_frame(0))
            .await
            .expect("Produce v0 is within the protocol range");

        assert_unsupported_version_of(route.serve(produce_frame(12)).await, 12);
    }

    #[tokio::test]
    async fn versions_set_for_a_route_are_served_and_advertised() {
        let log = Log::default();

        let route = FrameRouteService::<Error>::builder()
            .with_route(ProduceRequest::KEY, handler("handler", &log))
            .and_then(|builder| builder.with_versions(ProduceRequest::KEY, 3..=10))
            .and_then(FrameRouteBuilder::build)
            .expect("a route table with a Produce route that serves v3 to v10");

        assert_eq!(Some((3, 10)), advertised_produce_versions(&route).await);

        for api_version in [3, 10] {
            _ = route
                .serve(produce_frame(api_version))
                .await
                .expect("a version in the range of the route");
        }

        for api_version in [2, 11] {
            assert_unsupported_version_of(
                route.serve(produce_frame(api_version)).await,
                api_version,
            );
        }
    }

    #[tokio::test]
    async fn wrapped_route_keeps_its_versions() {
        let log = Log::default();

        let route = FrameRouteService::<Error>::builder()
            .with_route(ProduceRequest::KEY, handler("built-in", &log))
            .and_then(|builder| builder.with_versions(ProduceRequest::KEY, 3..=11))
            .and_then(|builder| {
                builder.wrap_route(ProduceRequest::KEY, |_built_in| {
                    handler("replacement", &log)
                })
            })
            .and_then(FrameRouteBuilder::build)
            .expect("a route table with a replaced Produce route");

        assert_eq!(Some((3, 11)), advertised_produce_versions(&route).await);
        assert_unsupported_version_of(route.serve(produce_frame(2)).await, 2);
        assert!(logged(&log).is_empty());
    }

    #[test]
    fn versions_wider_than_the_protocol_range_are_an_error() {
        let log = Log::default();

        let result = FrameRouteService::<Error>::builder()
            .with_route(ProduceRequest::KEY, handler("handler", &log))
            .and_then(|builder| builder.with_versions(ProduceRequest::KEY, 0..=12))
            .map(|builder| builder.len());

        assert_matches!(
            result,
            Err(Error::CapRangeExceedsProtocolRange { api_key, .. }) if api_key == ProduceRequest::KEY
        );
    }

    #[test]
    fn guarding_an_unregistered_route_is_an_error() {
        let log = Log::default();

        let result = FrameRouteService::<Error>::builder()
            .with_guard(ProduceRequest::KEY, RecordingGuard { name: "guard", log })
            .map(|builder| builder.len());

        assert_matches!(result, Err(Error::NoRouteToGuard(api_key)) if api_key == ProduceRequest::KEY);
    }

    #[test]
    fn setting_versions_of_an_unregistered_route_is_an_error() {
        let result = FrameRouteService::<Error>::builder()
            .with_versions(ProduceRequest::KEY, 3..=11)
            .map(|builder| builder.len());

        assert_matches!(
            result,
            Err(Error::NoRouteForVersions(api_key)) if api_key == ProduceRequest::KEY
        );
    }
}
