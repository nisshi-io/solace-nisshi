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

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use bytes::Bytes;
use rama::{
    ServiceInput,
    extensions::{Extension, Extensions, ExtensionsRef},
};

use crate::{Body, Frame, Request};

#[derive(Clone, Debug)]
pub struct FrameInput {
    pub frame: Frame,
    pub extensions: Extensions,
}

impl AsRef<Frame> for FrameInput {
    fn as_ref(&self) -> &Frame {
        &self.frame
    }
}

impl ExtensionsRef for FrameInput {
    fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

impl From<Frame> for FrameInput {
    fn from(frame: Frame) -> Self {
        Self {
            frame,
            extensions: Extensions::default(),
        }
    }
}

impl From<ServiceInput<Frame>> for FrameInput {
    fn from(value: ServiceInput<Frame>) -> Self {
        Self {
            frame: value.input,
            extensions: value.extensions,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BodyInput {
    pub body: Body,
    pub extensions: Extensions,
}

impl ExtensionsRef for BodyInput {
    fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

impl From<Body> for BodyInput {
    fn from(body: Body) -> Self {
        Self {
            body,
            extensions: Extensions::default(),
        }
    }
}

impl From<ServiceInput<Body>> for BodyInput {
    fn from(value: ServiceInput<Body>) -> Self {
        Self {
            body: value.input,
            extensions: value.extensions,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RequestInput<Q: Request> {
    pub request: Q,
    pub extensions: Extensions,
}

impl<Q> ExtensionsRef for RequestInput<Q>
where
    Q: Request,
{
    fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

impl<Q> From<Q> for RequestInput<Q>
where
    Q: Request,
{
    fn from(request: Q) -> Self {
        Self {
            request,
            extensions: Extensions::default(),
        }
    }
}

impl<Q> From<ServiceInput<Q>> for RequestInput<Q>
where
    Q: Request,
{
    fn from(value: ServiceInput<Q>) -> Self {
        Self {
            request: value.input,
            extensions: value.extensions,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BytesInput {
    pub bytes: Bytes,
    pub extensions: Extensions,
}

impl ExtensionsRef for BytesInput {
    fn extensions(&self) -> &Extensions {
        &self.extensions
    }
}

impl From<Bytes> for BytesInput {
    fn from(bytes: Bytes) -> Self {
        Self {
            bytes,
            extensions: Extensions::default(),
        }
    }
}

impl From<ServiceInput<Bytes>> for BytesInput {
    fn from(value: ServiceInput<Bytes>) -> Self {
        Self {
            bytes: value.input,
            extensions: value.extensions,
        }
    }
}

/// Marks that the response currently being assembled for a connection's
/// in-flight request must not be written to the peer.
///
/// The connection's request/response loop inserts one marker into the
/// connection's own [`Extensions`] store before the first request, and
/// processes one request at a time. `Extensions` has no way to remove an
/// entry once inserted, so the marker holds an [`AtomicBool`] that
/// [`Self::take`] reads and clears in one atomic step. Only the request that
/// set the flag is suppressed.
///
/// A handler sets the flag and never inserts the marker. An insert below a
/// `.fork()` lands in the child scope, where the connection loop does not
/// see it. An insert into a store that several connections share suppresses
/// the response of another connection.
#[derive(Clone, Debug, Default, Extension)]
pub struct SuppressResponseExtension(Arc<AtomicBool>);

impl SuppressResponseExtension {
    /// Marks the in-flight request's response for suppression.
    ///
    /// Does nothing when `extensions` has no marker, because then no
    /// connection loop owns the response and the caller writes it.
    pub fn mark(extensions: &Extensions) {
        if let Some(marker) = extensions.get_ref::<Self>() {
            marker.0.store(true, Ordering::Release);
        }
    }

    /// Reads and clears the marker, reporting whether it was set.
    #[must_use]
    pub fn take(extensions: &Extensions) -> bool {
        extensions
            .get_ref::<Self>()
            .is_some_and(|marker| marker.0.swap(false, Ordering::AcqRel))
    }
}

/// Marks that the connection must close once the response to its in-flight
/// request is written.
///
/// A handler sets the flag when the protocol requires the broker to answer a
/// request and then end the connection, as Kafka does for a SaslHandshake that
/// asks for a mechanism the broker has not enabled. The connection's
/// request/response loop inserts one marker into the connection's own
/// [`Extensions`] store before the first request, writes the response, and then
/// reads and clears the marker with [`Self::take`]. It follows the same rules
/// as [`SuppressResponseExtension`]: a handler sets the flag and never inserts
/// the marker.
#[derive(Clone, Debug, Default, Extension)]
pub struct CloseAfterResponseExtension(Arc<AtomicBool>);

impl CloseAfterResponseExtension {
    /// Marks the connection to close after the in-flight request's response.
    ///
    /// Does nothing when `extensions` has no marker, because then no
    /// connection loop owns the connection.
    pub fn mark(extensions: &Extensions) {
        if let Some(marker) = extensions.get_ref::<Self>() {
            marker.0.store(true, Ordering::Release);
        }
    }

    /// Reads and clears the marker, reporting whether it was set.
    #[must_use]
    pub fn take(extensions: &Extensions) -> bool {
        extensions
            .get_ref::<Self>()
            .is_some_and(|marker| marker.0.swap(false, Ordering::AcqRel))
    }
}
