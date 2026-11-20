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

use std::ops::RangeInclusive;

use nisshi_sans_io::{
    AddPartitionsToTxnRequest, ApiKey as _, Body, Frame, FrameInput, ListOffsetsRequest,
    ProduceRequest, Request, RequestInput, RootMessageMeta,
};
use rama::{Layer, Service, layer::MapErrLayer, service::BoxService};

use crate::{Error, FrameRequestLayer, FrameRouteBuilder};

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

/// Returns the versions of `api_key` that a client can reach, or `None` for an `api_key` this
/// build has no protocol metadata for.
///
/// This is the range in [`CAPPED_API_VERSIONS`] for a capped API, because
/// [`FrameRouteService`](crate::FrameRouteService) rejects every other version before a handler
/// sees the request, and the protocol's own range for every other API. It describes the built-in
/// handlers: [`FrameRouteBuilder::with_versions`] changes the range of a route in a route table.
#[must_use]
pub fn routable_range(api_key: i16) -> Option<RangeInclusive<i16>> {
    capped_range(api_key).or_else(|| protocol_range(api_key))
}

/// Returns the highest version an internal caller may send for `api_key` without an
/// `ApiVersions` negotiation, or `None` for an `api_key` this build has no protocol metadata
/// for.
///
/// An internal caller that builds a request directly has no negotiated version to use, so this
/// is the highest version in [`routable_range`].
#[must_use]
pub fn routable_max_version(api_key: i16) -> Option<i16> {
    routable_range(api_key).map(|range| *range.end())
}

pub(crate) fn protocol_range(api_key: i16) -> Option<RangeInclusive<i16>> {
    RootMessageMeta::messages()
        .requests()
        .get(&api_key)
        .map(|meta| meta.version.valid.start..=meta.version.valid.end)
}

/// Fails unless `declared` has a version and is a subset of `allowed`.
pub(crate) fn validate_versions(
    api_key: i16,
    declared: RangeInclusive<i16>,
    allowed: Option<RangeInclusive<i16>>,
) -> Result<(), Error> {
    if declared.is_empty() {
        return Err(Error::EmptyVersionRange { api_key, declared });
    }

    if allowed.as_ref().is_some_and(|allowed| {
        declared.start() >= allowed.start() && declared.end() <= allowed.end()
    }) {
        Ok(())
    } else {
        Err(Error::VersionRangeNotAllowed {
            api_key,
            declared,
            allowed,
        })
    }
}

impl<E> FrameRouteBuilder<E>
where
    E: std::error::Error + From<nisshi_sans_io::Error> + From<Error> + Send + Sync + 'static,
{
    /// Registers `handler` as the route for `Q`, serving the range for `Q` from
    /// [`CAPPED_API_VERSIONS`], which `ApiVersions` advertises, so a client never negotiates a
    /// version that this route rejects.
    ///
    /// # Errors
    ///
    /// Fails with [`Error::UncappedApi`] when [`CAPPED_API_VERSIONS`] has no entry for `Q`, and
    /// with [`Error::VersionRangeNotAllowed`] when that entry is not a subset of the protocol's
    /// own valid range.
    ///
    /// The route keeps that range as the versions that its handler implements: until
    /// [`FrameRouteBuilder::wrap_route`] changes the handler, [`FrameRouteBuilder::with_versions`]
    /// accepts only a range within it.
    pub fn with_capped_route<Q, S>(self, handler: S) -> Result<Self, Error>
    where
        Q: Request + TryFrom<Body>,
        <Q as TryFrom<Body>>::Error: Into<S::Error>,
        S: Service<RequestInput<Q>, Output = Q::Response>,
        S::Error: From<nisshi_sans_io::Error>,
        E: From<S::Error>,
    {
        let supported = capped_range(Q::KEY).ok_or(Error::UncappedApi(Q::KEY))?;

        let route = (MapErrLayer::new(E::from), FrameRequestLayer::<Q>::new())
            .into_layer(handler)
            .boxed();

        self.with_route(Q::KEY, route)?
            .with_capped_versions(Q::KEY, supported)
    }

    /// As [`Self::with_capped_route`], with `guard` around the route, so that `guard` sees the
    /// request's version and the encoded response.
    ///
    /// # Errors
    ///
    /// As [`Self::with_capped_route`].
    pub fn with_capped_route_layer<Q, L, S>(self, guard: L, handler: S) -> Result<Self, Error>
    where
        Q: Request + TryFrom<Body>,
        <Q as TryFrom<Body>>::Error: Into<S::Error>,
        S: Service<RequestInput<Q>, Output = Q::Response>,
        S::Error: From<nisshi_sans_io::Error>,
        E: From<S::Error>,
        L: Layer<BoxService<FrameInput, Frame, E>> + Send + Sync + 'static,
        L::Service: Service<FrameInput, Output = Frame, Error = E> + Send + Sync + 'static,
    {
        self.with_capped_route::<Q, S>(handler)?
            .with_guard(Q::KEY, guard)
    }
}

#[cfg(test)]
mod tests {
    use nisshi_sans_io::MetadataRequest;

    use super::*;

    #[test]
    fn every_capped_range_is_within_its_protocol_range() {
        for (api_key, range) in CAPPED_API_VERSIONS {
            validate_versions(api_key, range, protocol_range(api_key))
                .expect("a cap within the protocol range");
        }
    }

    #[test]
    fn declared_range_wider_than_protocol_range_fails_validation() {
        let err = validate_versions(
            MetadataRequest::KEY,
            0..=999,
            protocol_range(MetadataRequest::KEY),
        )
        .expect_err("0..=999 exceeds MetadataRequest's protocol range");

        assert!(matches!(
            err,
            Error::VersionRangeNotAllowed {
                api_key,
                ..
            } if api_key == MetadataRequest::KEY
        ));
    }

    #[test]
    fn empty_declared_range_fails_validation() {
        let err = validate_versions(
            MetadataRequest::KEY,
            RangeInclusive::new(5, 3),
            protocol_range(MetadataRequest::KEY),
        )
        .expect_err("a range that starts at 5 and ends at 3 has no version");

        assert!(matches!(
            err,
            Error::EmptyVersionRange {
                api_key,
                ..
            } if api_key == MetadataRequest::KEY
        ));
    }

    #[test]
    fn routable_range_is_the_cap_for_a_capped_api() {
        assert_eq!(Some(0..=6), routable_range(ListOffsetsRequest::KEY));
    }

    #[test]
    fn routable_range_is_the_protocol_range_for_an_uncapped_api() {
        assert_eq!(
            protocol_range(MetadataRequest::KEY),
            routable_range(MetadataRequest::KEY)
        );
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
