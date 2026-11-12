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

use crate::{AuthenticationExtension, Error, Stage};
use nisshi_sans_io::{
    ApiKey, CloseAfterResponseExtension, ErrorCode, RequestInput, SaslHandshakeRequest,
    SaslHandshakeResponse,
};
use rama::{Service, extensions::ExtensionsRef as _};
use rsasl::prelude::Mechname;
use tracing::{debug, instrument};

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SaslHandshakeService;

impl ApiKey for SaslHandshakeService {
    const KEY: i16 = SaslHandshakeRequest::KEY;
}

impl Service<RequestInput<SaslHandshakeRequest>> for SaslHandshakeService {
    type Output = SaslHandshakeResponse;
    type Error = Error;

    /// Answers with the mechanisms that the broker enables, whether it accepts
    /// the requested one or not, as Kafka does ([SaslServerAuthenticator.java]).
    ///
    /// For a mechanism that the broker hasn't enabled, the broker answers
    /// `UNSUPPORTED_SASL_MECHANISM` and then closes the connection. Without
    /// SASL, it answers `ILLEGAL_SASL_STATE` with no mechanisms, as a Kafka
    /// listener without SASL does ([KafkaApis.scala]), and keeps the
    /// connection open.
    ///
    /// [SaslServerAuthenticator.java]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/security/authenticator/SaslServerAuthenticator.java#L565-L581
    /// [KafkaApis.scala]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1970-L1973
    #[instrument(skip(self), ret)]
    async fn serve(
        &self,
        input: RequestInput<SaslHandshakeRequest>,
    ) -> Result<Self::Output, Self::Error> {
        let Some(authentication) = input
            .extensions()
            .get_ref::<AuthenticationExtension>()
            .cloned()
        else {
            return Ok(SaslHandshakeResponse::default()
                .error_code(ErrorCode::IllegalSaslState.into())
                .mechanisms(Some(vec![])));
        };

        let enabled = authentication.enabled_mechanisms();

        let accepted = authentication
            .stage
            .lock()
            .map_err(Error::from)
            .map(|mut guard| {
                // Re-authentication (KIP-368): a Java kafka-clients
                // connection periodically issues another SaslHandshake
                // on the same TCP socket. The previous handshake left
                // the Stage in `Session`/`Finished`, so a stale
                // `take()` would fall into the else branch and reject
                // a perfectly valid mechanism with
                // `UnsupportedSaslMechanism`. Always start with a
                // fresh `SASLServer` built from the stored config.
                if guard
                    .as_ref()
                    .is_none_or(|guard| !matches!(guard, Stage::Server(_)))
                {
                    debug!(?guard);
                    _ = guard.replace(authentication.fresh_server());
                }

                if let Some(Stage::Server(server)) = guard.take()
                    && let Ok(mechanism) = Mechname::parse(input.request.mechanism.as_bytes())
                {
                    server
                        .start_suggested(mechanism)
                        .inspect_err(|err| debug!(?err, ?mechanism))
                        .map(|session| _ = guard.replace(Stage::Session(session)))
                        .is_ok()
                } else {
                    false
                }
            })?;

        if accepted {
            Ok(SaslHandshakeResponse::default()
                .error_code(ErrorCode::None.into())
                .mechanisms(Some(enabled)))
        } else {
            debug!(?enabled, "requested mechanism is not enabled");

            CloseAfterResponseExtension::mark(input.extensions());

            Ok(SaslHandshakeResponse::default()
                .error_code(ErrorCode::UnsupportedSaslMechanism.into())
                .mechanisms(Some(enabled)))
        }
    }
}
