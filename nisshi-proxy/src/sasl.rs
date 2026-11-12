// Copyright ⓒ Solace Corporation
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

use nisshi_sans_io::{
    ApiKey, ErrorCode, RequestInput, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest, SaslHandshakeResponse,
};
use nisshi_service::{FrameApiKeyMatcher, FrameRequestLayer, FrameRequestService};
use rama::{Layer as _, Service, layer::HijackLayer};
use tracing::warn;

use crate::Error;

/// Returns the layers that answer SaslHandshake and SaslAuthenticate at the proxy with
/// `ILLEGAL_SASL_STATE`, instead of forwarding them to the origin.
///
/// The proxy forwards requests over origin connections that its clients share, and the
/// origin binds a login to the connection that carried it. So a forwarded login applies
/// to every client that later sends a request over that connection.
///
/// The proxy listener does not authenticate its clients. So the proxy answers as a Kafka
/// PLAINTEXT listener does, with `ILLEGAL_SASL_STATE` and an empty list of enabled
/// mechanisms ([KafkaApis.scala]).
///
/// [KafkaApis.scala]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1970-L1980
pub(crate) fn reject_layer() -> (
    HijackRequestLayer<SaslHandshakeRequest, SaslHandshakeRejectService>,
    HijackRequestLayer<SaslAuthenticateRequest, SaslAuthenticateRejectService>,
) {
    (
        hijack_request(SaslHandshakeRejectService),
        hijack_request(SaslAuthenticateRejectService),
    )
}

/// A layer that sends each frame of request `Q` to the service `S`, and every other frame
/// to the inner service.
type HijackRequestLayer<Q, S> = HijackLayer<FrameRequestService<S, Q>, FrameApiKeyMatcher>;

fn hijack_request<Q: ApiKey, S>(service: S) -> HijackRequestLayer<Q, S> {
    HijackLayer::new(
        FrameApiKeyMatcher(Q::KEY),
        FrameRequestLayer::<Q>::new().into_layer(service),
    )
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct SaslHandshakeRejectService;

impl Service<RequestInput<SaslHandshakeRequest>> for SaslHandshakeRejectService {
    type Output = SaslHandshakeResponse;
    type Error = Error;

    async fn serve(
        &self,
        input: RequestInput<SaslHandshakeRequest>,
    ) -> Result<Self::Output, Self::Error> {
        // The client's own error does not name the proxy, so the proxy log is where an
        // operator finds the cause.
        warn!(
            mechanism = input.request.mechanism,
            "{REJECT_MESSAGE}: {REMEDY}"
        );

        Ok(SaslHandshakeResponse::default()
            .error_code(ErrorCode::IllegalSaslState.into())
            .mechanisms(Some(vec![])))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct SaslAuthenticateRejectService;

impl Service<RequestInput<SaslAuthenticateRequest>> for SaslAuthenticateRejectService {
    type Output = SaslAuthenticateResponse;
    type Error = Error;

    async fn serve(
        &self,
        _input: RequestInput<SaslAuthenticateRequest>,
    ) -> Result<Self::Output, Self::Error> {
        warn!("{REJECT_MESSAGE}: {REMEDY}");

        Ok(SaslAuthenticateResponse::default()
            .error_code(ErrorCode::IllegalSaslState.into())
            .error_message(Some(REJECT_MESSAGE.into()))
            // Version 1 and later carry this field, and the encoder refuses a `None`
            // for it (`MandatoryFieldUnset`), so the response would never be sent.
            .session_lifetime_ms(Some(0)))
    }
}

const REJECT_MESSAGE: &str = "the proxy does not support SASL authentication";

const REMEDY: &str = "configure the client with security.protocol=PLAINTEXT, and point \
    the proxy at an origin listener that does not require authentication";

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use nisshi_sans_io::{Body, Frame, FrameInput, Header, MetadataRequest};
    use nisshi_service::FrameService;
    use rama::extensions::Extensions;

    use super::*;

    const FORWARDED: &str = "forwarded to origin";

    /// Serves a request through [`reject_layer`] in front of an origin that fails every
    /// request, and returns the response encoded and decoded again at `api_version`.
    async fn serve(api_key: i16, api_version: i16, body: Body) -> Result<Frame, Error> {
        let service = reject_layer().into_layer(FrameService::new(|_input: FrameInput| {
            Err::<Frame, _>(Error::Message(FORWARDED.into()))
        }));

        let response = service
            .serve(FrameInput {
                frame: Frame {
                    size: 0,
                    header: Header::Request {
                        api_key,
                        api_version,
                        correlation_id: 6789,
                        client_id: Some("abc".into()),
                    },
                    body,
                },
                extensions: Extensions::default(),
            })
            .await?;

        let encoded = Frame::response(response.header, response.body, api_key, api_version)?;
        Frame::response_from_bytes(encoded, api_key, api_version).map_err(Into::into)
    }

    #[tokio::test]
    async fn sasl_handshake_is_answered_without_forwarding() -> Result<(), Error> {
        for api_version in 0..=1 {
            let frame = serve(
                SaslHandshakeRequest::KEY,
                api_version,
                SaslHandshakeRequest::default()
                    .mechanism("PLAIN".into())
                    .into(),
            )
            .await?;

            assert_eq!(
                Header::Response {
                    correlation_id: 6789
                },
                frame.header
            );

            let response = SaslHandshakeResponse::try_from(frame.body)?;
            assert_eq!(i16::from(ErrorCode::IllegalSaslState), response.error_code);
            assert_matches!(response.mechanisms.as_deref(), Some([]));
        }

        Ok(())
    }

    #[tokio::test]
    async fn sasl_authenticate_is_answered_without_forwarding() -> Result<(), Error> {
        for api_version in 0..=2 {
            let frame = serve(
                SaslAuthenticateRequest::KEY,
                api_version,
                SaslAuthenticateRequest::default().into(),
            )
            .await?;

            assert_eq!(
                Header::Response {
                    correlation_id: 6789
                },
                frame.header
            );

            let response = SaslAuthenticateResponse::try_from(frame.body)?;
            assert_eq!(i16::from(ErrorCode::IllegalSaslState), response.error_code);
            assert_eq!(Some(REJECT_MESSAGE), response.error_message.as_deref());
        }

        Ok(())
    }

    #[tokio::test]
    async fn other_requests_are_forwarded() {
        let error = serve(MetadataRequest::KEY, 12, MetadataRequest::default().into())
            .await
            .unwrap_err();

        assert_matches!(error, Error::Message(message) if message == FORWARDED);
    }
}
