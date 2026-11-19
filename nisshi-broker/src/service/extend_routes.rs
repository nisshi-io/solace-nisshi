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

use std::{fmt, sync::Arc};

use nisshi_service::FrameRouteBuilder;

use crate::{Error, Result};

type Extend<G, S> =
    dyn Fn(FrameRouteBuilder<Error>, &G, &S) -> Result<FrameRouteBuilder<Error>> + Send + Sync;

/// A function that changes the route table of the service that the broker builds for a
/// connection.
///
/// The broker calls the function after it has registered its own routes, and before it adds
/// `ApiVersions`. The function receives the route builder, and the coordinator and the storage of
/// the broker. It changes the route table in one of three ways:
///
/// - [`FrameRouteBuilder::with_route`] adds a route. If the API key already has a route, this
///   fails with [`nisshi_service::Error::DuplicateRoute`].
/// - [`FrameRouteBuilder::wrap_route`] decorates the route of an API key. If the API key has no
///   route, this fails with [`nisshi_service::Error::NoRouteToWrap`].
/// - `wrap_route(api_key, |_built_in| replacement)` replaces the route of an API key.
///
/// # Calls
///
/// The broker calls the function once before it opens its listener, so that a route table that
/// cannot build fails the start of the broker. It then calls the function for each connection that
/// it accepts. If a call for a connection fails, the broker logs the error and drops that
/// connection. The broker calls the function on the task that accepts connections, so the function
/// must be quick and must not block. A panic in the function ends the broker.
///
/// # Replacing a route
///
/// A replacement skips what the built-in route does for its API key, such as the schema validation
/// of Produce. The group coordinator's fence of TxnOffsetCommit still wraps a replacement.
///
/// The layers that the broker builds into a route when it registers the route are lost with the
/// route. These are the version gate of Produce, ListOffsets and AddPartitionsToTxn, the
/// translation of an abortable-transaction error for a Produce below v11, and the topic ID
/// handling of Metadata. `ApiVersions` still advertises the version range of the built-in route,
/// so a replacement serves every version in that range and rejects every version outside it.
///
/// # Authentication
///
/// The function receives the routes of `SaslHandshake` and `SaslAuthenticate`, so it can wrap or
/// replace them, for example to plug in another login mechanism. A wrapper runs for clients that
/// have not logged in, so it treats its input as untrusted. A replacement of `SaslAuthenticate`
/// decides when a connection counts as authenticated. A route that the function adds under another
/// API key is reachable only after login. The function cannot add or wrap `ApiVersions`, because
/// the route builder adds it after the function returns.
pub struct ExtendRoutes<G, S>(Arc<Extend<G, S>>);

impl<G, S> ExtendRoutes<G, S> {
    /// Returns an `ExtendRoutes` that changes the route table with `extend`.
    pub fn new<F>(extend: F) -> Self
    where
        F: Fn(FrameRouteBuilder<Error>, &G, &S) -> Result<FrameRouteBuilder<Error>>
            + Send
            + Sync
            + 'static,
    {
        Self(Arc::new(extend))
    }

    pub(super) fn apply(
        &self,
        builder: FrameRouteBuilder<Error>,
        coordinator: &G,
        storage: &S,
    ) -> Result<FrameRouteBuilder<Error>> {
        (self.0)(builder, coordinator, storage)
    }
}

impl<G, S> Clone for ExtendRoutes<G, S> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<G, S> fmt::Debug for ExtendRoutes<G, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExtendRoutes").finish_non_exhaustive()
    }
}
