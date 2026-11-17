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

/// A function that changes the routes of each connection's service, after the broker has
/// registered its own routes and before `ApiVersions` is added.
///
/// The function receives the route builder, the broker's coordinator and its storage. It adds a
/// route with [`FrameRouteBuilder::with_route`], decorates a built-in route with
/// [`FrameRouteBuilder::wrap_route`], or replaces one with `wrap_route(api_key, |_built_in| ...)`.
/// Adding an API key that is already routed fails with `DuplicateRoute`, and wrapping one that is
/// not routed fails with `NoRouteToWrap`.
///
/// A replacement for a built-in route skips the behaviour of that route, such as the schema
/// validation of Produce. `ApiVersions` still advertises the range of API versions that the
/// built-in route served, so a replacement serves every version in that range, including the
/// narrowed range of Produce, ListOffsets and AddPartitionsToTxn.
pub struct RouteExtension<G, S>(Arc<Extend<G, S>>);

impl<G, S> RouteExtension<G, S> {
    /// Returns an extension that calls `extend` once for each connection that the broker
    /// accepts.
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

impl<G, S> Clone for RouteExtension<G, S> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<G, S> fmt::Debug for RouteExtension<G, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RouteExtension").finish_non_exhaustive()
    }
}
