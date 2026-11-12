// Copyright ⓒ Solace Corporation
//
// Contributors outside Solace:
//   Erik Wickstrom
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
    fmt::{self, Debug},
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use bytes::{BufMut as _, Bytes, BytesMut};
use nisshi_auth::AuthenticationExtension;
use nisshi_sans_io::{
    ApiKey, ApiVersionsRequest, Body, BodyInput, BytesInput, ErrorCode, Frame, FrameInput, Header,
    Request, RequestInput, Response, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest,
};
use nisshi_storage::read_share::ClientIdentity;
use opentelemetry::KeyValue;
use rama::{Layer, Service, extensions::Extensions, matcher::Matcher, service::BoxService};
use rsasl::config::SASLConfig;
use tokio::task::spawn_blocking;
use tracing::{debug, error, instrument};

use crate::{API_ERRORS, API_REQUESTS, PeerAddressExtension, ProgressBarExtension};

/// A [Matcher] of [`Request`]s using their [API key][`ApiKey`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestApiKeyMatcher(pub i16);

impl<Q> Matcher<RequestInput<Q>> for RequestApiKeyMatcher
where
    Q: Request,
{
    fn matches(&self, ext: Option<&Extensions>, req: &RequestInput<Q>) -> bool {
        debug!(?ext, ?req);
        Q::KEY == self.0
    }
}

/// A [`Matcher`] of [`Frame`]s using their [API key][`Frame#method.api_key`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameApiKeyMatcher(pub i16);

impl Matcher<FrameInput> for FrameApiKeyMatcher {
    fn matches(&self, _ext: Option<&Extensions>, req: &FrameInput) -> bool {
        req.frame.api_key().is_ok_and(|api_key| api_key == self.0)
    }
}

/// A [`Layer`] for handling API [`Request`]s responding with an API [`Response`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestLayer<Q> {
    request: PhantomData<Q>,
}

impl<Q> RequestLayer<Q> {
    pub fn new() -> Self {
        Self {
            request: PhantomData,
        }
    }
}

impl<S, Q> Layer<S> for RequestLayer<Q> {
    type Service = RequestService<S, Q>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            request: PhantomData,
        }
    }
}

/// A [`Service`] that handles API [`Request`]s responding with an API [`Response`]
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestService<S, Q> {
    inner: S,
    request: PhantomData<Q>,
}

impl<S, Q> Debug for RequestService<S, Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(RequestService)).finish()
    }
}

impl<S, Q> Service<RequestInput<Q>> for RequestService<S, Q>
where
    S: Service<RequestInput<Q>>,
    Q: Request,
    S::Error: From<<Q as TryFrom<Body>>::Error> + From<<S as Service<RequestInput<Q>>>::Error>,
    S::Output: Response,
    Body: From<<S as Service<RequestInput<Q>>>::Output>,
{
    type Output = S::Output;
    type Error = S::Error;

    #[instrument(skip(req))]
    async fn serve(&self, req: RequestInput<Q>) -> Result<Self::Output, Self::Error> {
        debug!(?req);
        self.inner
            .serve(req)
            .await
            .inspect(|response| debug!(?response))
    }
}

impl<S, Q> ApiKey for RequestService<S, Q>
where
    Q: Request,
{
    const KEY: i16 = Q::KEY;
}

/// A [`Layer`] that transforms [`Frame`] into [`Request`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameRequestLayer<Q> {
    request: PhantomData<Q>,
}

impl<Q> FrameRequestLayer<Q> {
    pub fn new() -> Self {
        Self {
            request: PhantomData,
        }
    }
}

impl<S, Q> Layer<S> for FrameRequestLayer<Q> {
    type Service = FrameRequestService<S, Q>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            request: PhantomData,
        }
    }
}

/// A [`Service`] that transforms a [`Frame`] into a [`Request`]
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameRequestService<S, Q> {
    inner: S,
    request: PhantomData<Q>,
}

impl<S, Q> Debug for FrameRequestService<S, Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(FrameRequestService)).finish()
    }
}

impl<S, Q, I> Service<I> for FrameRequestService<S, Q>
where
    I: Into<FrameInput> + Send + 'static,
    S: Service<RequestInput<Q>>,
    S::Output: Response,
    S::Error: From<nisshi_sans_io::Error>,
    Q: Request + TryFrom<Body>,
    <Q as TryFrom<Body>>::Error: Into<S::Error>,
{
    type Output = Frame;
    type Error = S::Error;

    #[instrument(skip(req))]
    async fn serve(&self, req: I) -> Result<Self::Output, Self::Error> {
        let req = req.into();

        let correlation_id = req.frame.correlation_id()?;

        // The identity of the client that this request comes from, for the read
        // shares of a storage engine. It scopes the request's task, rather than
        // going into its extensions: those are shared by the connection's
        // requests and cannot be replaced, and a fork of them would hide the
        // state that a handler keeps there for the connection.
        let identity = ClientIdentity::new(
            req.extensions
                .get_ref::<AuthenticationExtension>()
                .and_then(AuthenticationExtension::principal)
                .as_deref(),
            req.frame.client_id()?,
        );

        let req = Q::try_from(req.frame.body)
            .map(|request| RequestInput {
                request,
                extensions: req.extensions,
            })
            .map_err(Into::into)?;

        identity
            .scope(self.inner.serve(req))
            .await
            .map(|response| Frame {
                size: 0,
                header: Header::Response { correlation_id },
                body: response.into(),
            })
    }
}

impl<S, Q> Matcher<FrameInput> for FrameRequestService<S, Q>
where
    S: Clone + Send + Sync + 'static,
    Q: Request,
{
    fn matches(&self, ext: Option<&Extensions>, req: &FrameInput) -> bool {
        debug!(?ext, ?req);
        req.frame.api_key().is_ok_and(|api_key| api_key == Q::KEY)
    }
}

/// A [`Layer`] that transforms [`Bytes`] into [`Frame`]s
#[derive(Clone, Debug, Default)]
pub struct BytesFrameLayer {
    sasl_config: Option<Arc<SASLConfig>>,
}

impl BytesFrameLayer {
    pub fn with_sasl_config(self, sasl_config: Option<Arc<SASLConfig>>) -> Self {
        Self { sasl_config }
    }
}

impl<S> Layer<S> for BytesFrameLayer {
    type Service = BytesFrameService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            af: self
                .sasl_config
                .clone()
                .map(|sasl_config| AuthenticationFrame {
                    authentication: AuthenticationExtension::server(sasl_config),
                    v0: Arc::new(Mutex::new(None)),
                }),
        }
    }
}

#[derive(Clone)]
struct AuthenticationFrame {
    authentication: AuthenticationExtension,
    v0: Arc<Mutex<Option<bool>>>,
}

impl AuthenticationFrame {
    fn is_authenticated(&self) -> bool {
        self.authentication.is_authenticated()
    }
}

/// Returns the `N` bytes at `offset` in a raw frame, or `None` when the frame
/// is too short to hold them.
fn peek<const N: usize>(bytes: &[u8], offset: usize) -> Option<[u8; N]> {
    bytes.get(offset..offset + N)?.try_into().ok()
}

/// The header fields of an `ApiVersions` request whose version is outside the protocol range.
#[derive(Clone, Copy, Debug)]
struct UnsupportedApiVersionsHeader {
    api_version: i16,
    correlation_id: i32,
}

/// Returns the header of an `ApiVersions` request whose version is outside the protocol range,
/// read from the fixed offsets of the request header, `[size:4][api_key:2][api_version:2]
/// [correlation_id:4]`.
///
/// The broker must not decode the body of such a request, because a later version can add
/// fields that this build's decoder does not know, and the decode then fails. Kafka also skips
/// the body in this case ([RequestContext.java#L111-L115]).
///
/// [RequestContext.java#L111-L115]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/RequestContext.java#L111-L115
fn unsupported_api_versions_header(bytes: &Bytes) -> Option<UnsupportedApiVersionsHeader> {
    let api_key = peek::<2>(bytes, 4).map(i16::from_be_bytes)?;
    let api_version = peek::<2>(bytes, 6).map(i16::from_be_bytes)?;
    let correlation_id = peek::<4>(bytes, 8).map(i32::from_be_bytes)?;

    (api_key == ApiVersionsRequest::KEY
        && !crate::api::is_within_protocol_range(api_key, api_version))
    .then_some(UnsupportedApiVersionsHeader {
        api_version,
        correlation_id,
    })
}

/// A [`Service`] transforming [`Bytes`]s into [`Frame`]s
#[derive(Clone, Default)]
pub struct BytesFrameService<S> {
    inner: S,
    af: Option<AuthenticationFrame>,
}

impl<S> Debug for BytesFrameService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(BytesFrameService)).finish()
    }
}

impl<S> BytesFrameService<S> {
    fn is_authenticated(&self, api_key: i16) -> bool {
        self.af.as_ref().is_none_or(|af| {
            af.authentication.is_authenticated()
                || api_key == SaslHandshakeRequest::KEY
                || api_key == SaslAuthenticateRequest::KEY
                || api_key == ApiVersionsRequest::KEY
        })
    }
}

impl<S> Service<BytesInput> for BytesFrameService<S>
where
    S: Service<FrameInput, Output = Frame>,
    S::Error: From<nisshi_sans_io::Error> + From<tokio::task::JoinError> + Debug,
{
    type Output = Bytes;
    type Error = S::Error;

    #[instrument(skip(req))]
    async fn serve(&self, req: BytesInput) -> Result<Self::Output, Self::Error> {
        let sasl_handshake_v0 = self
            .af
            .as_ref()
            .and_then(|af| af.v0.lock().ok())
            .inspect(|v0| debug!(?v0))
            .map(|v0| v0.unwrap_or_default())
            .unwrap_or_default();

        debug!(len = req.bytes.len(), sasl_handshake_v0);

        let extensions = req.extensions;

        // The version of an `ApiVersions` request outside the protocol range. The broker
        // answers such a request with `UNSUPPORTED_VERSION`, encoded at v0.
        let mut unsupported_api_versions = None;

        let req = if sasl_handshake_v0 {
            //  If SaslHandshakeRequest version is v0, a series of SASL client and server tokens
            //  corresponding to the mechanism are sent as opaque packets without wrapping the
            //  messages with Kafka protocol headers. If SaslHandshakeRequest version is v1,
            //  the SaslAuthenticate request/response are used, where the actual SASL tokens
            //  are wrapped in the Kafka protocol. The error code in the final message from
            //  the broker will indicate if authentication succeeded or failed.
            Frame {
                size: 0,
                header: Header::Request {
                    api_key: SaslAuthenticateRequest::KEY,
                    api_version: 0,
                    correlation_id: 0,
                    client_id: None,
                },
                body: Body::SaslAuthenticateRequest(
                    SaslAuthenticateRequest::default().auth_bytes(req.bytes.slice(4..)),
                ),
            }
        } else {
            // Peek `api_key` from the raw frame bytes, before paying for a
            // full decode of a body that may be unbounded on an
            // unauthenticated connection. `[size:4][api_key:2]...`: a frame
            // too short to even hold an api_key falls through to the normal
            // decode below, which already rejects malformed frames.
            if let Some(peeked_api_key) = req
                .bytes
                .get(4..6)
                .map(|api_key| i16::from_be_bytes([api_key[0], api_key[1]]))
                && !self.is_authenticated(peeked_api_key)
            {
                debug!(api_key = peeked_api_key, "request before authentication");
                return Err(Into::into(nisshi_sans_io::Error::NotAuthenticated));
            }

            // The request header holds no secret, and it identifies a frame
            // that fails to decode, since we don't log the frame's bytes.
            debug!(
                api_key = ?peek::<2>(&req.bytes, 4).map(i16::from_be_bytes),
                api_version = ?peek::<2>(&req.bytes, 6).map(i16::from_be_bytes),
                correlation_id = ?peek::<4>(&req.bytes, 8).map(i32::from_be_bytes),
            );

            if let Some(header) = unsupported_api_versions_header(&req.bytes) {
                debug!(?header, "ApiVersions request outside the protocol range");
                unsupported_api_versions = Some(header.api_version);

                // The broker routes the request at v0, inside the protocol range, so that
                // `FrameRouteService` passes it to the `ApiVersions` handler. The body is
                // empty, because the request fields do not change the response.
                Frame {
                    size: 0,
                    header: Header::Request {
                        api_key: ApiVersionsRequest::KEY,
                        api_version: 0,
                        correlation_id: header.correlation_id,
                        client_id: None,
                    },
                    body: ApiVersionsRequest::default().into(),
                }
            } else {
                spawn_blocking(|| Frame::request_from_bytes(req.bytes))
                    .await?
                    .inspect(|request| debug!(?request))?
            }
        };

        let api_key = req.api_key()?;

        if !self.is_authenticated(api_key) {
            debug!(
                api_key,
                api_name = %req.api_name(),
                "request before authentication"
            );

            return Err(Into::into(nisshi_sans_io::Error::NotAuthenticated));
        }

        let api_version = req.api_version()?;
        let correlation_id = req.correlation_id()?;
        let requested_version = unsupported_api_versions.unwrap_or(api_version);

        if let Some(pb) = extensions.get_ref::<ProgressBarExtension>() {
            let api_name = req.api_name();

            pb.as_ref()
                .set_message(format!("{api_name} v{requested_version}/{correlation_id}"));
            pb.as_ref().tick();
        }

        let attributes = vec![
            KeyValue::new("api_key", api_key as i64),
            KeyValue::new("api_version", requested_version as i64),
        ];

        if !extensions.contains::<AuthenticationExtension>()
            && let Some(authentication) = self.af.as_ref().map(|af| af.authentication.clone())
        {
            _ = extensions.insert(authentication);
        }

        // A response that fails to encode closes the connection, so the error log names who
        // the connection belongs to.
        let client_id = req.client_id()?.map(str::to_owned);
        let client_host = extensions
            .get_ref::<PeerAddressExtension>()
            .map(PeerAddressExtension::client_host);

        let Frame { body, .. } = {
            self.inner
                .serve(FrameInput {
                    frame: req,
                    extensions,
                })
                .await
                .inspect(|response| debug!(?response))?
        };

        if sasl_handshake_v0 {
            //  If SaslHandshakeRequest version is v0, a series of SASL client and server tokens
            //  corresponding to the mechanism are sent as opaque packets without wrapping the
            //  messages with Kafka protocol headers.

            // when authenticated, this is final handshake:
            if let Some(af) = self.af.as_ref()
                && af.is_authenticated()
                && let Ok(mut v0) = af.v0.lock()
                && v0.is_some()
            {
                *v0 = None
            }

            SaslAuthenticateResponse::try_from(body)
                .and_then(|response| {
                    i32::try_from(response.auth_bytes.len())
                        .map_err(Into::into)
                        .map(|size| {
                            let mut frame = BytesMut::new();
                            frame.put(&size.to_be_bytes()[..]);
                            frame.put(response.auth_bytes);
                            Bytes::from(frame)
                        })
                })
                .map_err(Into::into)
        } else {
            //  If SaslHandshakeRequest version is v0, a series of SASL client and server tokens
            //  corresponding to the mechanism are sent as opaque packets without wrapping the
            //  messages with Kafka protocol headers.
            //
            // Following messages will be opaque, but only after a handshake that
            // the broker accepted: a rejected one leaves the connection framed.
            if let Some(af) = self.af.as_ref()
                && (api_key == SaslHandshakeRequest::KEY && api_version == 0)
                && matches!(
                    &body,
                    Body::SaslHandshakeResponse(response)
                        if response.error_code == i16::from(ErrorCode::None)
                )
                && let Ok(mut v0) = af.v0.lock()
            {
                *v0 = Some(true)
            }

            // Kafka answers an `ApiVersions` request outside the protocol range with
            // `UNSUPPORTED_VERSION`, encoded at v0, and lists only its own `ApiVersions` range,
            // so that the client retries at a version in range ([ApiVersionsRequest.java#L112-L129]).
            // `api_version` is already 0 for such a request.
            //
            // The reply lists only that entry because franz-go retries only on such a reply,
            // and takes a longer list as the broker's supported versions.
            //
            // The entry is this build's protocol range, not the inner service's entry, because
            // franz-go retries only at a version lower than the one it sent. Behind the proxy,
            // the inner service is the origin, which can support a later version than this
            // layer decodes.
            //
            // [ApiVersionsRequest.java#L112-L129]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/ApiVersionsRequest.java#L112-L129
            let body = match body {
                Body::ApiVersionsResponse(response) if unsupported_api_versions.is_some() => {
                    let api_keys = crate::api::protocol_api_version(ApiVersionsRequest::KEY)
                        .into_iter()
                        .collect();

                    Body::ApiVersionsResponse(
                        response
                            .error_code(ErrorCode::UnsupportedVersion.into())
                            .api_keys(Some(api_keys)),
                    )
                }
                body => body,
            };

            spawn_blocking(move || {
                Frame::response(
                    Header::Response { correlation_id },
                    body,
                    api_key,
                    api_version,
                )
            })
            .await?
            .inspect(|response| {
                debug!(len = response.len());
                API_REQUESTS.add(1, &attributes);
            })
            .inspect_err(|err| {
                error!(
                    api_key,
                    api_version,
                    correlation_id,
                    ?client_id,
                    ?client_host,
                    ?err
                );
                API_ERRORS.add(1, &attributes);
            })
            .map_err(Into::into)
        }
    }
}

/// A [`Layer`] that transforms [`Frame`]s into [`Bytes`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBytesLayer;

impl<S> Layer<S> for FrameBytesLayer {
    type Service = FrameBytesService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A [`Service`] that transforms [`Frame`]s into [`Bytes`]
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBytesService<S> {
    inner: S,
}

impl<S> Debug for FrameBytesService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(FrameBytesService)).finish()
    }
}

impl<S> Service<FrameInput> for FrameBytesService<S>
where
    S: Service<BytesInput, Output = Bytes>,
    S::Error: From<nisshi_sans_io::Error>,
{
    type Output = Frame;
    type Error = S::Error;

    #[instrument(skip(req), fields(api_key = req.frame.api_key()?, api_version = req.frame.api_version()?, correlation_id = req.frame.correlation_id()?))]
    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        debug!(?req);

        let api_key = req.frame.api_key()?;
        let api_version = req.frame.api_version()?;

        let req = BytesInput {
            bytes: Frame::request(req.frame.header, req.frame.body)?,
            extensions: req.extensions.fork(),
        };

        self.inner
            .serve(req)
            .await
            .and_then(|response| {
                Frame::response_from_bytes(response, api_key, api_version).map_err(Into::into)
            })
            .inspect(|response| debug!(?response))
    }
}

/// A [`Layer`] that transforms [`Frame`] into [`Body`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBodyLayer;

impl<S> Layer<S> for FrameBodyLayer {
    type Service = FrameBodyService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A [`Service`] that transforms [`Frame`] into [`Body`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FrameBodyService<S> {
    inner: S,
}

impl<S> Service<FrameInput> for FrameBodyService<S>
where
    S: Service<BodyInput, Output = Body>,
    S::Error: From<nisshi_sans_io::Error>,
{
    type Output = Frame;

    type Error = S::Error;

    #[instrument(skip_all, fields(api_key = req.frame.api_key()?, api_version = req.frame.api_version()?, correlation_id = req.frame.correlation_id()?))]
    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        let correlation_id = req.frame.correlation_id()?;

        self.inner
            .serve(BodyInput {
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

/// A [`Layer`] that transforms [`Body`] into [`Request`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BodyRequestLayer<Q> {
    request: PhantomData<Q>,
}

impl<Q> BodyRequestLayer<Q> {
    pub fn new() -> Self {
        Self {
            request: PhantomData,
        }
    }
}

impl<S, Q> Layer<S> for BodyRequestLayer<Q> {
    type Service = BodyRequestService<S, Q>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            request: PhantomData,
        }
    }
}

/// A [`Layer`] that transforms [`Body`] into [`Request`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BodyRequestService<S, Q> {
    inner: S,
    request: PhantomData<Q>,
}

impl<S, Q> ApiKey for BodyRequestService<S, Q>
where
    Q: Request,
{
    const KEY: i16 = Q::KEY;
}

impl<S, Q> Service<BodyInput> for BodyRequestService<S, Q>
where
    S: Service<RequestInput<Q>>,
    Q: Request,
    S::Error: From<<Q as TryFrom<Body>>::Error> + From<<S as Service<RequestInput<Q>>>::Error>,
    Body: From<<S as Service<RequestInput<Q>>>::Output>,
{
    type Output = Body;
    type Error = S::Error;

    #[instrument(skip_all)]
    async fn serve(&self, req: BodyInput) -> Result<Self::Output, Self::Error> {
        let req = Q::try_from(req.body).map(|request| RequestInput {
            request,
            extensions: req.extensions,
        })?;
        self.inner.serve(req).await.map(Body::from)
    }
}

/// A [`Layer`] that transforms [`Request`] into [`Frame`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestFrameLayer;

impl<S> Layer<S> for RequestFrameLayer {
    type Service = RequestFrameService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service { inner }
    }
}

/// A [`Service`] that transforms [`Request`] into [`Frame`]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestFrameService<S> {
    inner: S,
}

impl<S, Q> Service<RequestInput<Q>> for RequestFrameService<S>
where
    Q: Request,
    S: Service<FrameInput, Output = Frame>,
    S::Error: From<<<Q as Request>::Response as TryFrom<Body>>::Error>,
{
    type Output = Q::Response;
    type Error = S::Error;

    #[instrument(skip_all)]
    async fn serve(&self, req: RequestInput<Q>) -> Result<Self::Output, Self::Error> {
        debug!(?req);

        let api_key = Q::KEY;
        let api_version = crate::routable_max_version(api_key).unwrap_or_default();
        let correlation_id = 0;
        let client_id = Some(env!("CARGO_CRATE_NAME").into());

        self.inner
            .serve(FrameInput {
                frame: Frame {
                    size: 0,
                    header: Header::Request {
                        api_key,
                        api_version,
                        correlation_id,
                        client_id,
                    },
                    body: req.request.into(),
                },
                extensions: req.extensions,
            })
            .await
            .and_then(|response| Q::Response::try_from(response.body).map_err(Into::into))
            .inspect(|response| debug!(?response))
    }
}

impl<S, Q, E> From<RequestService<S, Q>> for BoxService<BodyInput, Body, E>
where
    S: Service<RequestInput<Q>, Error = E>,
    Q: Request,
    <S as Service<RequestInput<Q>>>::Output: Response,
    E: From<<Q as TryFrom<Body>>::Error> + From<<S as Service<RequestInput<Q>>>::Error>,
    Body: From<<S as Service<RequestInput<Q>>>::Output>,
{
    fn from(value: RequestService<S, Q>) -> Self {
        BodyRequestLayer::<Q>::new().into_layer(value).boxed()
    }
}

impl<S, Q, E> From<RequestService<S, Q>> for BoxService<FrameInput, Frame, E>
where
    S: Service<RequestInput<Q>, Error = E>,
    Q: Request,
    <S as Service<RequestInput<Q>>>::Output: Response,
    E: From<nisshi_sans_io::Error>
        + From<<Q as TryFrom<Body>>::Error>
        + From<<S as Service<RequestInput<Q>>>::Error>,
    Body: From<<S as Service<RequestInput<Q>>>::Output>,
{
    fn from(value: RequestService<S, Q>) -> Self {
        (FrameBodyLayer, BodyRequestLayer::<Q>::new())
            .into_layer(value)
            .boxed()
    }
}

impl<S, Q, E> From<BodyRequestService<S, Q>> for BoxService<FrameInput, Frame, E>
where
    S: Service<RequestInput<Q>, Error = E>,
    Q: Request,
    E: From<nisshi_sans_io::Error>
        + From<<Q as TryFrom<Body>>::Error>
        + From<<S as Service<RequestInput<Q>>>::Error>,
    Body: From<<S as Service<RequestInput<Q>>>::Output>,
{
    fn from(value: BodyRequestService<S, Q>) -> Self {
        FrameBodyLayer.into_layer(value).boxed()
    }
}

/// A [`Service`] that transforms [`Frame`] into a [`Frame`] using a closure.
#[derive(Clone, Copy, Debug, Hash)]
pub struct FrameService<F> {
    response: F,
}

impl<E, F> Service<FrameInput> for FrameService<F>
where
    F: Fn(FrameInput) -> Result<Frame, E> + Clone + Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    type Output = Frame;
    type Error = E;

    #[instrument(skip_all)]
    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        (self.response)(req)
    }
}

impl<F> FrameService<F> {
    pub fn new<E>(response: F) -> Self
    where
        F: Fn(FrameInput) -> Result<Frame, E> + Clone,
        E: Send + Sync + 'static,
    {
        Self { response }
    }
}

/// A [`Service`] that transforms [`Request`] into a [`Response`] using a closure.
#[derive(Clone, Copy, Hash)]
pub struct ResponseService<F> {
    response: F,
}

impl<F> Debug for ResponseService<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(ResponseService)).finish()
    }
}

impl<Q, E, F> Service<RequestInput<Q>> for ResponseService<F>
where
    F: Fn(RequestInput<Q>) -> Result<Q::Response, E> + Clone + Send + Sync + 'static,
    Q: Request,
    E: Send + Sync + 'static,
{
    type Output = Q::Response;
    type Error = E;

    #[instrument(skip_all)]
    async fn serve(&self, req: RequestInput<Q>) -> Result<Self::Output, Self::Error> {
        (self.response)(req)
    }
}

impl<F> ResponseService<F> {
    pub fn new<Q, E>(response: F) -> Self
    where
        F: Fn(RequestInput<Q>) -> Result<Q::Response, E> + Clone,
        Q: Request,
        E: Send + Sync + 'static,
    {
        Self { response }
    }
}

#[cfg(test)]
mod tests {
    use nisshi_sans_io::{ApiVersionsResponse, Error as SansIoError};
    use nisshi_storage::read_share::{ClientAdmission, ClientIdentity, ClientReadShares};
    use tokio::{
        sync::Semaphore,
        time::{Duration, timeout},
    };

    use super::*;

    /// Takes the one permit of its client's read share and holds it, with a
    /// permit of an engine limit that has permits to spare.
    #[derive(Clone)]
    struct HoldingTheClientShare {
        shares: ClientReadShares,
        limit: Arc<Semaphore>,
        held: Arc<Mutex<Vec<ClientAdmission>>>,
    }

    impl Default for HoldingTheClientShare {
        fn default() -> Self {
            Self {
                shares: ClientReadShares::default(),
                limit: Arc::new(Semaphore::new(10)),
                held: Arc::default(),
            }
        }
    }

    impl Service<RequestInput<ApiVersionsRequest>> for HoldingTheClientShare {
        type Output = ApiVersionsResponse;
        type Error = SansIoError;

        async fn serve(
            &self,
            _req: RequestInput<ApiVersionsRequest>,
        ) -> Result<Self::Output, Self::Error> {
            let admission = timeout(
                Duration::from_millis(50),
                self.shares.acquire(1, &self.limit),
            )
            .await;

            // A request past its client's share is not the first of the
            // client, and `None` is a request outside any client.
            let code = match admission {
                Err(_) => ErrorCode::RequestTimedOut,
                Ok(Ok(admission)) => {
                    let code = if admission.is_within_share() {
                        ErrorCode::None
                    } else {
                        ErrorCode::RequestTimedOut
                    };
                    self.held.lock().expect("held").push(admission);
                    code
                }
                Ok(Err(_)) => ErrorCode::UnknownServerError,
            };

            Ok(ApiVersionsResponse::default().error_code(code.into()))
        }
    }

    async fn request(
        service: &FrameRequestService<HoldingTheClientShare, ApiVersionsRequest>,
        client_id: &str,
    ) -> Result<i16, SansIoError> {
        let frame = Frame {
            size: 0,
            header: Header::Request {
                api_key: ApiVersionsRequest::KEY,
                api_version: 3,
                correlation_id: 1,
                client_id: Some(client_id.into()),
            },
            body: ApiVersionsRequest::default().into(),
        };

        // Each request has the extensions of a connection of its own.
        let response = service.serve(FrameInput::from(frame)).await?;
        Ok(ApiVersionsResponse::try_from(response.body)?.error_code)
    }

    /// A request runs with the identity of its client, so the requests of one
    /// `client_id` share its read share, over any number of connections, and
    /// another `client_id` has a share of its own.
    #[tokio::test]
    async fn requests_run_in_the_identity_of_their_client() -> Result<(), SansIoError> {
        let service =
            FrameRequestLayer::<ApiVersionsRequest>::new().layer(HoldingTheClientShare::default());

        assert_eq!(i16::from(ErrorCode::None), request(&service, "one").await?);
        assert_eq!(
            i16::from(ErrorCode::RequestTimedOut),
            request(&service, "one").await?
        );
        assert_eq!(i16::from(ErrorCode::None), request(&service, "two").await?);

        // The identity is not left behind for the next request on the task.
        assert_eq!(None, ClientIdentity::current());

        Ok(())
    }
}
