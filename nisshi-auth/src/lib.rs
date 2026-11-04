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
use nisshi_sans_io::ScramMechanism;
use nisshi_storage::Storage;
use rama::extensions::Extension;
use rsasl::{
    callback::{Context, Request, SessionCallback, SessionData},
    config::SASLConfig,
    mechanisms::scram::properties::ScramStoredPassword,
    mechanisms::scram::{SCRAM_SHA256, SCRAM_SHA512},
    prelude::{Mechanism, Registry, SASLError, SASLServer, Session, SessionError, Validation},
    property::{AuthId, AuthzId},
    validate::{Validate, ValidationError},
};
use std::{
    fmt::{self, Debug, Formatter},
    str::FromStr,
    sync::{Arc, Mutex, PoisonError},
};
use thiserror::Error;
use tokio::task::JoinError;
use tracing::{debug, instrument};

mod authenticate;
mod handshake;

pub use authenticate::SaslAuthenticateService;
pub use handshake::SaslHandshakeService;

#[derive(Clone, Debug, Error)]
pub enum Error {
    Join(Arc<JoinError>),
    Poison,
    SansIo(#[from] nisshi_sans_io::Error),
    Sasl(Arc<SASLError>),
    SaslSession(Arc<SessionError>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<JoinError> for Error {
    fn from(value: JoinError) -> Self {
        Self::Join(Arc::new(value))
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl From<SASLError> for Error {
    fn from(value: SASLError) -> Self {
        Self::Sasl(Arc::new(value))
    }
}

impl From<SessionError> for Error {
    fn from(value: SessionError) -> Self {
        Self::SaslSession(Arc::new(value))
    }
}

#[derive(Clone, Extension)]
pub struct AuthenticationExtension {
    config: Arc<SASLConfig>,
    stage: Arc<Mutex<Option<Stage>>>,
}

pub enum Stage {
    Server(SASLServer<Justification>),
    Session(Session<Justification>),
    Finished(Option<Success>),
}

impl Debug for Stage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Stage)).finish()
    }
}

impl AuthenticationExtension {
    pub fn server(config: Arc<SASLConfig>) -> Self {
        let server = SASLServer::<Justification>::new(config.clone());
        Self {
            config,
            stage: Arc::new(Mutex::new(Some(Stage::Server(server)))),
        }
    }

    pub fn is_authenticated(&self) -> bool {
        // require a validated identity: SCRAM finishes even on a bad proof
        self.stage
            .lock()
            .map(|guard| matches!(guard.as_ref(), Some(Stage::Finished(Some(_)))))
            .ok()
            .unwrap_or_default()
    }

    /// The name that the connection authenticated as, or `None` while it has
    /// not authenticated.
    pub fn principal(&self) -> Option<String> {
        self.stage
            .lock()
            .ok()
            .and_then(|guard| match guard.as_ref() {
                Some(Stage::Finished(Some(success))) => Some(success.auth_id().to_owned()),
                _ => None,
            })
    }

    /// Build a fresh `Stage::Server` from the stored config. Used by
    /// the SASL handshake handler to support re-authentication
    /// (KIP-368): a client periodically issues a new SaslHandshake on
    /// an existing connection, and the broker must be willing to
    /// initiate a new SASL exchange even though the previous one
    /// already succeeded.
    pub fn fresh_server(&self) -> Stage {
        Stage::Server(SASLServer::<Justification>::new(self.config.clone()))
    }

    /// The names of the mechanisms that the stored config enables, which a
    /// SaslHandshake response lists whether it accepts the requested one or
    /// not.
    pub fn enabled_mechanisms(&self) -> Vec<String> {
        SASLServer::<Justification>::new(self.config.clone())
            .get_available()
            .into_iter()
            .map(|mechanism| mechanism.mechanism.to_string())
            .collect()
    }
}

impl Debug for AuthenticationExtension {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Authentication)).finish()
    }
}

#[derive(Debug, Error)]
pub enum AuthError {
    Bad,
    Io(nisshi_sans_io::Error),
    MissingProperty { mechanism: String, property: String },
    NoSuchUser,
    UnknownMechanism(String),
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Success {
    auth_id: String,
}

impl Success {
    /// The name that the client authenticated as.
    pub fn auth_id(&self) -> &str {
        &self.auth_id
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Justification;

impl Validation for Justification {
    type Value = Result<Success, AuthError>;
}

#[derive(Clone, Debug)]
pub struct Callback<S> {
    storage: S,
}

impl<S> Callback<S>
where
    S: Storage,
{
    pub fn new(storage: S) -> Self
    where
        S: Storage,
    {
        Self { storage }
    }

    #[instrument(skip_all)]
    fn check(
        &self,
        session_data: &SessionData,
        context: &Context<'_>,
    ) -> Result<Result<Success, AuthError>, Error> {
        debug!(mechanism = %session_data.mechanism().mechanism);

        if session_data.mechanism().mechanism.starts_with("SCRAM-") {
            Ok(context
                .get_ref::<AuthId>()
                .inspect(|auth_id| debug!(mechanism = %session_data.mechanism().mechanism, auth_id))
                .ok_or(AuthError::MissingProperty {
                    mechanism: session_data.mechanism().mechanism.to_string(),
                    property: "AuthId".into(),
                })
                .and_then(|auth_id| {
                    context
                        .get_ref::<AuthzId>()
                        .inspect(|authz_id| {
                            debug!(mechanism = %session_data.mechanism().mechanism, authz_id)
                        })
                        .map_or(Ok(Success{
                            auth_id:auth_id.to_string()
                        }), |authz_id| {
                            if authz_id == auth_id {
                                Ok(Success{
                                    auth_id:auth_id.to_string()
                                })
                            } else {
                                Err(AuthError::Bad)
                            }
                        })
                }))
        } else {
            Ok(Err(AuthError::UnknownMechanism(
                session_data.mechanism().mechanism.to_string(),
            )))
        }
    }
}

impl<S> SessionCallback for Callback<S>
where
    S: Storage,
{
    #[instrument(skip_all)]
    fn callback(
        &self,
        session_data: &SessionData,
        context: &Context<'_>,
        request: &mut Request<'_>,
    ) -> Result<(), SessionError> {
        debug!(?session_data);

        if session_data.mechanism().mechanism.starts_with("SCRAM-") {
            let mechanism = ScramMechanism::from_str(session_data.mechanism().mechanism)
                .map_err(|error| SessionError::Boxed(Box::new(error)))?;

            let auth_id = context
                .get_ref::<AuthId>()
                .ok_or(SessionError::ValidationError(
                    ValidationError::MissingRequiredProperty,
                ))?;

            debug!(?auth_id, ?mechanism);

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            if let Ok(Some(credential)) = rt
                .block_on(
                    async move { self.storage.user_scram_credential(auth_id, mechanism).await },
                )
                .inspect_err(|err| debug!(auth_id, ?mechanism, ?err))
            {
                _ = request
                    .satisfy::<ScramStoredPassword<'_>>(&ScramStoredPassword::new(
                        credential.iterations as u32,
                        &credential.salt[..],
                        &credential.stored_key[..],
                        &credential.server_key[..],
                    ))
                    .inspect_err(|err| debug!(auth_id, ?mechanism, ?err))?;
            }
        }

        Ok(())
    }

    #[instrument(skip_all)]
    fn validate(
        &self,
        session_data: &SessionData,
        context: &Context<'_>,
        validate: &mut Validate<'_>,
    ) -> Result<(), ValidationError> {
        debug!(?session_data);

        _ = validate.with::<Justification, _>(|| {
            self.check(session_data, context)
                .map_err(|e| ValidationError::Boxed(Box::new(e)))
        })?;

        Ok(())
    }
}

/// The mechanisms that the broker enables: the SCRAM mechanisms that
/// [`Callback`] can look up a credential for and validate.
///
/// The config names them, not rsasl's defaults, because the defaults follow
/// rsasl's enabled features and so can include a mechanism (such as
/// OAUTHBEARER) that the broker would accept at handshake and then fail at
/// every authentication.
static MECHANISMS: &[Mechanism] = &[SCRAM_SHA512, SCRAM_SHA256];

pub fn configuration<S>(storage: S) -> Result<Arc<SASLConfig>, Error>
where
    S: Storage,
{
    SASLConfig::builder()
        .with_registry(Registry::with_mechanisms(MECHANISMS))
        .with_callback(Callback::new(storage))
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_send<T: Send>() {}
    fn is_sync<T: Sync>() {}

    #[test]
    fn authentication() {
        is_send::<AuthenticationExtension>();
        is_sync::<AuthenticationExtension>();
    }

    /// Returns an extension whose stage is `stage`, on a configuration that no
    /// test exchange uses.
    fn extension_at(stage: Stage) -> AuthenticationExtension {
        let config = SASLConfig::with_credentials(None, "unused".into(), "unused".into())
            .expect("a client configuration with credentials is valid");
        let extension = AuthenticationExtension::server(config);
        *extension
            .stage
            .lock()
            .expect("no other thread holds the lock") = Some(stage);
        extension
    }

    /// A connection has a principal only after it authenticated.
    #[test]
    fn principal_is_none_until_authentication_succeeds() {
        let config = SASLConfig::with_credentials(None, "unused".into(), "unused".into())
            .expect("a client configuration with credentials is valid");

        // Before any exchange, and after one that finished without a success, as SCRAM
        // does for a bad proof.
        assert_eq!(None, AuthenticationExtension::server(config).principal());
        assert_eq!(None, extension_at(Stage::Finished(None)).principal());

        let authenticated = extension_at(Stage::Finished(Some(Success {
            auth_id: "alice".to_owned(),
        })));
        assert_eq!(Some("alice".to_owned()), authenticated.principal());
        assert!(authenticated.is_authenticated());
    }

    /// The broker enables exactly the SCRAM mechanisms that it can look up a
    /// credential for.
    #[test]
    fn mechanisms_are_the_scram_mechanisms() {
        let mut enabled = MECHANISMS
            .iter()
            .map(|mechanism| ScramMechanism::from_str(mechanism.mechanism.as_str()))
            .collect::<Result<Vec<_>, _>>()
            .expect("every enabled mechanism is a SCRAM mechanism");
        enabled.sort_unstable();

        assert_eq!(
            vec![ScramMechanism::Scram256, ScramMechanism::Scram512],
            enabled
        );
    }
}
