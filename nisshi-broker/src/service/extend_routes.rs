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
/// the broker. A route has a handler, guards and a version range, and the function changes them
/// with these methods:
///
/// - [`FrameRouteBuilder::with_route`] adds a route. If the API key already has a route, this
///   fails with [`nisshi_service::Error::DuplicateRoute`].
/// - [`FrameRouteBuilder::wrap_route`] changes the handler of a route. This fails with
///   [`nisshi_service::Error::NoRouteToWrap`] if the API key has no route.
///   `wrap_route(api_key, |_built_in| replacement)` replaces the handler.
/// - [`FrameRouteBuilder::with_guard`] adds a guard around a route.
/// - [`FrameRouteBuilder::with_versions`] sets the version range that a route serves and that
///   `ApiVersions` advertises.
///
/// # Calls
///
/// The broker calls the function once when it starts serving, before it registers itself in
/// storage, so that a route table that cannot build fails the start of the broker. It then calls
/// the function for each connection that it accepts. If a call for a connection fails, the broker
/// logs the error and drops that connection. The broker calls the function on the task that
/// accepts connections, so the function must be quick and must not block. A panic in the function
/// ends the broker.
///
/// # Replacing a handler
///
/// A replacement of a handler keeps the guards and the version range of its route. These are
/// the fence of TxnOffsetCommit, the translation of an abortable-transaction error for a Produce
/// below v11, the topic ID handling of Metadata, and the version range. The route table closes the
/// connection of a request for a version outside that range, before the handler sees the
/// request.
///
/// A replacement loses what the built-in handler does, such as the request validation of
/// CreateTopics and Fetch, the schema validation of Produce, and the work on the storage.
///
/// The version range stays the range of the built-in route, and `ApiVersions` advertises it.
/// [`FrameRouteBuilder::with_versions`] sets another range. For Produce, ListOffsets and
/// AddPartitionsToTxn, the built-in handler implements fewer versions than the protocol has, so
/// `with_versions` accepts only a range within the built-in range until `wrap_route` changes the
/// handler. After that, it accepts any range within the protocol's own range for the API.
///
/// # Guards
///
/// A guard that the function adds with `with_guard` runs inside the guards of the broker. It runs
/// after the route table has checked the version, and after the fence of TxnOffsetCommit, the
/// translation for Produce and the topic ID handling of Metadata, so it does not see a request
/// that one of them refuses.
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
