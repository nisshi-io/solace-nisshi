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

use std::{fmt, ops::RangeInclusive};

use nisshi_sans_io::{
    AddPartitionsToTxnRequest, ApiKey as _, Body, Frame, FrameInput, ListOffsetsRequest,
    ProduceRequest, Request, RequestInput, RootMessageMeta,
};
use rama::{Layer, Service, layer::MapErrLayer};
use tracing::{instrument, warn};

use crate::{Error, FrameRequestLayer, FrameRequestService, FrameRouteBuilder};

/// The version range nisshi routes for each API whose handler implements only part of the
/// Kafka protocol's own range for that API.
///
/// [`FrameRouteBuilder::with_capped_route`] reads its range from this table, and
/// [`routable_max_version`] reads the highest version from it, so a new cap is one entry here.
pub const CAPPED_API_VERSIONS: [(i16, RangeInclusive<i16>); 3] = [
    // Kafka's Produce range is 0-11, and Kafka 3.9.1 advertises all of it. nisshi decodes only
    // the `RecordBatch` (magic v2) format that Produce v3 introduced. Versions 0-2 carry the
    // older message-set formats, which nisshi does not decode. A client that negotiates through
    // `ApiVersions` picks the highest version both sides support, so this cap is stricter than
    // Kafka, but a negotiating client does not send v0-2.
    (ProduceRequest::KEY, 3..=11),
    // Kafka's ListOffsets range is 0-9. Versions 7-9 add the MAX_TIMESTAMP,
    // EARLIEST_LOCAL_TIMESTAMP and LATEST_TIERED_TIMESTAMP sentinel timestamps, which nisshi's
    // storage backends do not look up. v0 is in range, and `ListOffsetsService` fills in its
    // `OldStyleOffsets` field.
    (ListOffsetsRequest::KEY, 0..=6),
    // Kafka's AddPartitionsToTxn range is 0-5. Version 4 adds the multi-transaction request
    // shape (`Transactions`), which only the SlateDB backend implements. Kafka 3.9.1 advertises
    // 0-5 and protects v4+ with a `CLUSTER_ACTION` authorization check
    // ([KafkaApis.scala#L2586-L2587]), and the Java client limits itself to v3
    // ([AddPartitionsToTxnRequest.java#L43-L59]). So this cap is stricter than Kafka, but no
    // client sends v4+.
    //
    // [KafkaApis.scala#L2586-L2587]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L2586-L2587
    // [AddPartitionsToTxnRequest.java#L43-L59]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/AddPartitionsToTxnRequest.java#L43-L59
    (AddPartitionsToTxnRequest::KEY, 0..=3),
];

/// Returns the version range nisshi routes for `api_key`, when [`CAPPED_API_VERSIONS`] caps it.
#[must_use]
pub fn capped_range(api_key: i16) -> Option<RangeInclusive<i16>> {
    CAPPED_API_VERSIONS
        .iter()
        .find(|(key, _)| *key == api_key)
        .map(|(_, range)| range.clone())
}

/// Returns the highest version an internal caller may send for `api_key` without an
/// `ApiVersions` negotiation, or `None` for an `api_key` this build has no protocol metadata
/// for.
///
/// An internal caller that builds a request directly has no negotiated version to use. The
/// protocol maximum of a capped API is a version that [`VersionGateLayer`] rejects, so this
/// returns the cap's maximum for such an API, and the protocol maximum for every other API.
#[must_use]
pub fn routable_max_version(api_key: i16) -> Option<i16> {
    capped_range(api_key)
        .map(|range| *range.end())
        .or_else(|| protocol_range(api_key).map(|range| *range.end()))
}

fn protocol_range(api_key: i16) -> Option<RangeInclusive<i16>> {
    RootMessageMeta::messages()
        .requests()
        .get(&api_key)
        .map(|meta| meta.version.valid.start..=meta.version.valid.end)
}

/// Fails unless `declared` is a subset of the protocol's own valid range for `api_key`.
fn validate_capped_range(api_key: i16, declared: RangeInclusive<i16>) -> Result<(), Error> {
    let protocol = protocol_range(api_key);

    if protocol.as_ref().is_some_and(|protocol| {
        declared.start() >= protocol.start() && declared.end() <= protocol.end()
    }) {
        Ok(())
    } else {
        Err(Error::CapRangeExceedsProtocolRange {
            api_key,
            declared,
            protocol,
        })
    }
}

/// A [`Layer`] that rejects a request whose version falls outside the range nisshi routes for
/// its API, before [`FrameRequestLayer`] decodes the body into a handler's request type.
#[derive(Clone, Debug)]
pub struct VersionGateLayer {
    api_key: i16,
    supported: RangeInclusive<i16>,
}

impl VersionGateLayer {
    pub fn new(api_key: i16, supported: RangeInclusive<i16>) -> Self {
        Self { api_key, supported }
    }
}

impl<S> Layer<S> for VersionGateLayer {
    type Service = VersionGateService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Self::Service {
            inner,
            api_key: self.api_key,
            supported: self.supported.clone(),
        }
    }
}

/// A [`Service`] built by [`VersionGateLayer`].
#[derive(Clone)]
pub struct VersionGateService<S> {
    inner: S,
    api_key: i16,
    supported: RangeInclusive<i16>,
}

impl<S> fmt::Debug for VersionGateService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(VersionGateService))
            .field("api_key", &self.api_key)
            .field("supported", &self.supported)
            .finish()
    }
}

impl<S> Service<FrameInput> for VersionGateService<S>
where
    S: Service<FrameInput, Output = Frame>,
    S::Error: From<nisshi_sans_io::Error> + From<Error>,
{
    type Output = Frame;
    type Error = S::Error;

    #[instrument(skip_all)]
    async fn serve(&self, req: FrameInput) -> Result<Self::Output, Self::Error> {
        let api_version = req.frame.api_version()?;

        if self.supported.contains(&api_version) {
            return self.inner.serve(req).await;
        }

        // The broker closes the connection, as Kafka 3.9.1 does for a version it has not
        // enabled ([SocketServer.scala#L1119-L1125]). `ApiVersions` advertises this range,
        // so only a client that skips the negotiation sends such a version.
        //
        // [SocketServer.scala#L1119-L1125]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/network/SocketServer.scala#L1119-L1125
        warn!(
            api_key = self.api_key,
            api_name = req.frame.api_name(),
            api_version,
            client_id = req.frame.client_id().ok().flatten(),
            supported = ?self.supported,
            "request version outside the supported range"
        );

        Err(S::Error::from(Error::UnsupportedVersion {
            api_key: self.api_key,
            api_version,
        }))
    }
}

impl<E> FrameRouteBuilder<E>
where
    E: std::error::Error + From<nisshi_sans_io::Error> + From<Error> + Send + Sync + 'static,
{
    /// Registers `handler` as the route for `Q`, behind a [`VersionGateLayer`] with `Q`'s range
    /// from [`CAPPED_API_VERSIONS`], and advertises that range in `ApiVersions`, so a client
    /// never negotiates a version that this route rejects.
    ///
    /// # Errors
    ///
    /// Fails with [`Error::UncappedApi`] when [`CAPPED_API_VERSIONS`] has no entry for `Q`, and
    /// with [`Error::CapRangeExceedsProtocolRange`] when that entry is not a subset of the
    /// protocol's own valid range.
    pub fn with_capped_route<Q, S>(self, handler: S) -> Result<Self, Error>
    where
        Q: Request + TryFrom<Body>,
        <Q as TryFrom<Body>>::Error: Into<S::Error>,
        S: Service<RequestInput<Q>, Output = Q::Response>,
        S::Error: From<nisshi_sans_io::Error>,
        E: From<S::Error>,
    {
        self.with_capped_route_layer::<Q, _, _>((), handler)
    }

    /// As [`Self::with_capped_route`], with `layer` between the [`VersionGateLayer`] and the
    /// decoding of the request frame, so that `layer` sees the request's version and the
    /// encoded response.
    ///
    /// # Errors
    ///
    /// As [`Self::with_capped_route`].
    pub fn with_capped_route_layer<Q, L, S>(self, layer: L, handler: S) -> Result<Self, Error>
    where
        Q: Request + TryFrom<Body>,
        <Q as TryFrom<Body>>::Error: Into<S::Error>,
        S: Service<RequestInput<Q>, Output = Q::Response>,
        S::Error: From<nisshi_sans_io::Error>,
        E: From<S::Error>,
        L: Layer<FrameRequestService<S, Q>>,
        L::Service: Service<FrameInput, Output = Frame, Error = S::Error>,
    {
        let supported = capped_range(Q::KEY).ok_or(Error::UncappedApi(Q::KEY))?;
        validate_capped_range(Q::KEY, supported.clone())?;

        let service = (
            VersionGateLayer::new(Q::KEY, supported.clone()),
            MapErrLayer::new(E::from),
            layer,
            FrameRequestLayer::<Q>::new(),
        )
            .into_layer(handler)
            .boxed();

        self.with_capped_range(Q::KEY, *supported.start(), *supported.end())
            .with_route(Q::KEY, service)
    }
}

#[cfg(test)]
mod tests {
    use nisshi_sans_io::MetadataRequest;

    use super::*;

    #[test]
    fn every_capped_range_is_within_its_protocol_range() {
        for (api_key, range) in CAPPED_API_VERSIONS {
            validate_capped_range(api_key, range).expect("a cap within the protocol range");
        }
    }

    #[test]
    fn declared_range_wider_than_protocol_range_fails_validation() {
        let err = validate_capped_range(MetadataRequest::KEY, 0..=999)
            .expect_err("0..=999 exceeds MetadataRequest's protocol range");

        assert!(matches!(
            err,
            Error::CapRangeExceedsProtocolRange {
                api_key,
                ..
            } if api_key == MetadataRequest::KEY
        ));
    }

    #[test]
    fn routable_max_version_uses_the_cap_for_a_capped_api() {
        assert_eq!(Some(6), routable_max_version(ListOffsetsRequest::KEY));
    }

    #[test]
    fn routable_max_version_uses_the_protocol_maximum_for_an_uncapped_api() {
        assert_eq!(
            protocol_range(MetadataRequest::KEY).map(|range| *range.end()),
            routable_max_version(MetadataRequest::KEY)
        );
    }
}
