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

use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};

use nisshi_sans_io::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, Body, BodyInput, ErrorCode, Frame, FrameInput,
    Header, RequestInput, RootMessageMeta, api_versions_response::ApiVersion,
};
use rama::{Service, extensions::Extensions, service::BoxService};
use tracing::warn;

use crate::Error;

/// An [`ApiVersionsResponse`] [`Service`] with a supported set of API and versions from
/// [`RootMessageMeta`], narrowed by `capped` for a route registered through
/// [`FrameRouteBuilder::with_capped_route`].
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApiVersionsService<E> {
    supported: Vec<i16>,
    capped: BTreeMap<i16, (i16, i16)>,
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
                    RootMessageMeta::messages()
                        .requests()
                        .iter()
                        .filter(|(api_key, _)| self.supported.contains(api_key))
                        .map(|(api_key, meta)| {
                            let (min_version, max_version) = self
                                .capped
                                .get(api_key)
                                .copied()
                                .unwrap_or((meta.version.valid.start, meta.version.valid.end));

                            ApiVersion::default()
                                .api_key(meta.api_key)
                                .min_version(min_version)
                                .max_version(max_version)
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
    routes: Arc<BTreeMap<i16, BoxService<FrameInput, Frame, E>>>,
}

impl<E> FrameRouteService<E>
where
    E: std::error::Error + From<nisshi_sans_io::Error> + From<Error> + Send + Sync + 'static,
{
    pub fn new(routes: Arc<BTreeMap<i16, BoxService<FrameInput, Frame, E>>>) -> Self {
        Self { routes }
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

        let Some(service) = self.routes.get(&api_key) else {
            return Err(E::from(Error::UnknownServiceFrame {
                api_key,
                api_name: req.frame.api_name(),
                api_version: req.frame.api_version()?,
                correlation_id: req.frame.correlation_id()?,
                size: req.frame.size,
            }));
        };

        let api_version = req.frame.api_version()?;

        if is_within_protocol_range(api_key, api_version) {
            return service.serve(req).await;
        }

        warn!(
            api_key,
            api_name = req.frame.api_name(),
            api_version,
            client_id = req.frame.client_id().ok().flatten(),
            "request version outside the protocol range"
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

/// A [`Frame`] route builder providing an [`ApiVersionsResponse`] for all available routes
#[derive(Debug)]
pub struct FrameRouteBuilder<E> {
    routes: BTreeMap<i16, BoxService<FrameInput, Frame, E>>,
    capped: BTreeMap<i16, (i16, i16)>,
}

impl<E> FrameRouteBuilder<E>
where
    E: std::error::Error + From<nisshi_sans_io::Error> + Send + Sync + 'static,
{
    fn new() -> Self {
        Self {
            routes: BTreeMap::new(),
            capped: BTreeMap::new(),
        }
    }

    /// Records that `api_key`'s route advertises `(min_version, max_version)` in
    /// [`ApiVersionsResponse`] rather than the protocol's own range for it, for
    /// [`FrameRouteBuilder::with_capped_route`] to call: a client negotiates against what
    /// `ApiVersions` advertises, so a capped route's handler must never reject a version
    /// `ApiVersions` itself told the client was fine to send.
    pub(crate) fn with_capped_range(
        mut self,
        api_key: i16,
        min_version: i16,
        max_version: i16,
    ) -> Self {
        _ = self.capped.insert(api_key, (min_version, max_version));
        self
    }

    pub fn with_service<S>(self, service: S) -> Result<Self, Error>
    where
        S: Into<BoxService<FrameInput, Frame, E>> + ApiKey,
    {
        self.with_route(S::KEY, service.into())
    }

    pub fn with_route(
        mut self,
        api_key: i16,
        service: BoxService<FrameInput, Frame, E>,
    ) -> Result<Self, Error> {
        self.routes
            .insert(api_key, service)
            .map_or(Ok(self), |_existing| Err(Error::DuplicateRoute(api_key)))
    }

    /// Replaces the registered route of `api_key` with the service that `wrap` builds from it.
    /// Returns [`Error::NoRouteToWrap`] when no route of `api_key` is registered, so that a
    /// route table that registers it after this call, or not at all, does not build without
    /// what the wrapper adds.
    pub fn wrap_route<F>(mut self, api_key: i16, wrap: F) -> Result<Self, Error>
    where
        F: FnOnce(BoxService<FrameInput, Frame, E>) -> BoxService<FrameInput, Frame, E>,
    {
        let route = self
            .routes
            .remove(&api_key)
            .ok_or(Error::NoRouteToWrap(api_key))?;

        _ = self.routes.insert(api_key, wrap(route));

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
        let mut supported = self.routes.keys().copied().collect::<Vec<_>>();
        supported.push(api_key);
        let capped = self.capped.clone();

        self.with_route(
            api_key,
            ApiVersionsService {
                supported,
                capped,
                error: PhantomData,
            }
            .boxed(),
        )
        .map(|builder| FrameRouteService {
            routes: Arc::new(builder.routes),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use nisshi_sans_io::{ApiVersionsRequest, ProduceRequest};

    use super::*;

    // A client negotiates a version from what `ApiVersions` advertises, so a capped route
    // advertises its cap, not the protocol's wider range, or the client picks a version that
    // `VersionGateLayer` rejects.
    #[tokio::test]
    async fn api_versions_response_uses_capped_range_for_a_capped_api() {
        let service = ApiVersionsService::<Error> {
            supported: vec![ProduceRequest::KEY],
            capped: BTreeMap::from([(ProduceRequest::KEY, (3, 11))]),
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
        assert!(matches!(
            result,
            Err(Error::UnsupportedVersion {
                api_key,
                api_version,
            }) if api_key == ApiVersionsRequest::KEY && api_version == expected_version
        ));
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
}
