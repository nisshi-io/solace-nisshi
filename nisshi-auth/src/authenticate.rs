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

use std::io::Cursor;

use crate::{AuthenticationExtension, Error, Stage};
use bytes::Bytes;
use nisshi_sans_io::{
    ApiKey, ErrorCode, RequestInput, SaslAuthenticateRequest, SaslAuthenticateResponse,
};
use rama::{Service, extensions::ExtensionsRef as _};
use rsasl::prelude::State;
use tokio::task;
use tracing::debug;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SaslAuthenticateService {
    session_lifetime_ms: Option<i64>,
}

impl Default for SaslAuthenticateService {
    fn default() -> Self {
        Self {
            session_lifetime_ms: Some(60_000),
        }
    }
}

impl SaslAuthenticateService {
    pub fn session_lifetime_ms(self, session_lifetime_ms: Option<i64>) -> Self {
        Self {
            session_lifetime_ms,
        }
    }
}

impl ApiKey for SaslAuthenticateService {
    const KEY: i16 = SaslAuthenticateRequest::KEY;
}

impl Service<RequestInput<SaslAuthenticateRequest>> for SaslAuthenticateService {
    type Output = SaslAuthenticateResponse;
    type Error = Error;

    async fn serve(
        &self,
        input: RequestInput<SaslAuthenticateRequest>,
    ) -> Result<Self::Output, Self::Error> {
        if let Some(authentication) = input
            .extensions()
            .get_ref::<AuthenticationExtension>()
            .cloned()
        {
            let session_lifetime_ms = self.session_lifetime_ms;

            task::spawn_blocking(move || {
                authentication
                    .stage
                    .lock()
                    .map_err(Into::into)
                    .map(|mut guard| {
                        if let Some(Stage::Session(session)) = guard.as_mut() {
                            let mut outcome = Cursor::new(Vec::new());

                            let Ok(state) = session
                                .step(Some(&input.request.auth_bytes), &mut outcome)
                                .inspect(|state| debug!(?state))
                                .inspect_err(|err| debug!(?err))
                            else {
                                _ = guard.take();

                                return SaslAuthenticateResponse::default()
                                    .error_code(ErrorCode::SaslAuthenticationFailed.into())
                                    .error_message(Some(
                                        ErrorCode::SaslAuthenticationFailed.to_string(),
                                    ))
                                    .auth_bytes(Bytes::from_static(b""))
                                    .session_lifetime_ms(Some(0));
                            };

                            let success = session
                                .validation()
                                .transpose()
                                .ok()
                                .flatten()
                                .inspect(|success| debug!(?success));

                            if let State::Finished(_) = state {
                                _ = guard.replace(Stage::Finished(success))
                            }

                            SaslAuthenticateResponse::default()
                                .error_code(ErrorCode::None.into())
                                .error_message(Some("NONE".into()))
                                .auth_bytes(Bytes::from(outcome.into_inner()))
                                .session_lifetime_ms(session_lifetime_ms)
                        } else {
                            _ = guard.take();

                            SaslAuthenticateResponse::default()
                                .error_code(ErrorCode::IllegalSaslState.into())
                                .error_message(Some(ErrorCode::IllegalSaslState.to_string()))
                                .auth_bytes(Bytes::from_static(b""))
                                .session_lifetime_ms(Some(0))
                        }
                    })
            })
            .await?
        } else {
            Ok(SaslAuthenticateResponse::default()
                .error_code(ErrorCode::IllegalSaslState.into())
                .error_message(Some(ErrorCode::IllegalSaslState.to_string()))
                .auth_bytes(Bytes::from_static(b""))
                .session_lifetime_ms(Some(0)))
        }
    }
}
