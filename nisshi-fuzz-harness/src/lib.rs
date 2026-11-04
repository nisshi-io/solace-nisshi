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

//! The checks that the fuzz targets in `fuzz/` and the tests of this crate share.
//!
//! The checks live in a crate of their own, and not in `fuzz`, because `fuzz` links libFuzzer and
//! builds every target as a binary. A test of this crate builds none of them, and
//! `just test-workspace` runs it with every other workspace test.

// A check in this crate fails by panicking, because that is how libFuzzer and a test see a
// failure, so the crate may call `expect`.
#![allow(clippy::expect_used)]

use nisshi_sans_io::{ApiKey, Body, Error, Frame, Header, MetadataRequest, Request, Response};
use nisshi_service::routable_range;

/// `len` bytes of a xorshift64 stream: the same bytes on every run for a `seed`, so that a test
/// that builds arbitrary values from them fails the same way each time.
#[must_use]
pub fn pseudo_random_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;

    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// Maps a fuzzed `selector` onto one of the versions of `T`'s API that a client can reach, so that
/// every target reaches every version, not only the latest.
///
/// A request and the response to it are encoded at the same version, so pick it once and pass it
/// to both [`check_request`] and [`check_response`].
pub fn api_version<T>(selector: u16) -> i16
where
    T: ApiKey,
{
    let versions = routable_range(T::KEY).expect("every generated API has protocol metadata");
    let count = u16::try_from(versions.end() - versions.start() + 1).expect("a few versions");

    versions.start() + i16::try_from(selector % count).expect("fits: below the version count")
}

/// Whether the encoder may refuse a fuzzed request of API `api_key` at `api_version` because it
/// leaves `field` unset.
///
/// The encoder refuses a field that is mandatory at the version and nullable only at a later one,
/// and the generator leaves a field unset when it is nullable at the latest version. A refusal is
/// right for the fields that this list names, at the versions that it names. Any other refusal
/// means that a version stopped being fuzzed, or that the encoder rejects a request that it should
/// encode, so [`check_request`] fails on it. Naming the versions keeps a refusal at one version
/// from hiding behind a refusal at another.
fn may_be_unset(api_key: i16, api_version: i16, field: &str) -> bool {
    // A topic's `Name` is nullable from v10, so a request at an earlier version has no encoding
    // when it leaves one unset. The version test makes a refusal at v10 or later fail.
    api_key == MetadataRequest::KEY
        && api_version < 10
        && field == "Frame.MetadataRequest.MetadataRequestTopic.name"
}

fn at_version(error: Error, api_key: i16, api_version: i16) -> Error {
    Error::Message(format!("api key {api_key} v{api_version}: {error}"))
}

/// Encodes `request` at `api_version`, decodes it, and asserts that encoding the decoded request
/// gives the same bytes again. Returns the decoded request, or `None` when `request` has no
/// encoding at that version.
///
/// The check is on bytes, not on the values. A fuzzed request is an arbitrary in-memory value,
/// and a codec isn't lossless against one: a service may fill in fields from several version eras
/// at once and trust the encoder to drop whatever doesn't apply, and a mandatory array left at
/// `None` is wire-equivalent to `Some(vec![])` but not `PartialEq`. What a codec does have to
/// satisfy is that the decoder's output is a fixed point: the bytes of what it decoded must
/// decode and encode back to those same bytes. A field the encoder skipped or wrote at the wrong
/// width shows as different bytes the second time, where comparing two decodes can't see it.
///
/// `None` means the encoder refused with [`Error::MandatoryFieldUnset`] for a field that
/// `may_be_unset` allows: the value leaves a field unset that is mandatory at `api_version` and
/// nullable only at later versions, which no client could have sent at this version, so there is
/// nothing to compare. A refusal for any other field panics, and so does a mismatch. Any other
/// error is returned, with the API key and the version in its message.
///
/// The decoded request is what a broker would be served: a mandatory array left at `None` arrives
/// as `Some(vec![])`, and fields outside the version's range arrive at their defaults.
pub fn check_request<T>(request: T, api_version: i16) -> Result<Option<T>, Error>
where
    T: Request,
{
    let header = Header::Request {
        api_key: T::KEY,
        api_version,
        correlation_id: 0,
        client_id: Some("fuzzer".into()),
    };

    let once = match Frame::request(header.clone(), request.into()) {
        Err(Error::MandatoryFieldUnset { field, .. }) => {
            assert!(
                may_be_unset(T::KEY, api_version, &field),
                "api key {} v{api_version}: the encoder refused a request that leaves {field} unset",
                T::KEY,
            );

            return Ok(None);
        }
        encoded => encoded.map_err(|error| at_version(error, T::KEY, api_version))?,
    };

    let body = Frame::request_from_bytes(once.clone())
        .map_err(|error| at_version(error, T::KEY, api_version))?
        .body;

    // Everything the decoder returns has an encoding at this version, so this one can't refuse.
    let twice = Frame::request(header, body.clone())
        .map_err(|error| at_version(error, T::KEY, api_version))?;

    assert_eq!(
        once,
        twice,
        "api key {} v{api_version}: re-encoding the decoded request changed its bytes",
        T::KEY,
    );

    T::try_from(body).map(Some).map_err(|_| {
        at_version(
            Error::Message("decoded the wrong request type".into()),
            T::KEY,
            api_version,
        )
    })
}

/// The request that a client could send as `body` at `api_version`: `body` encoded and decoded
/// again, for a caller that serves requests and so needs one that has an encoding at that version.
///
/// A fuzzed request is an arbitrary in-memory value, so it may leave a field unset that is
/// mandatory at `api_version` and nullable only at a later one. No client could send that at this
/// version, and the encoder refuses it with [`Error::MandatoryFieldUnset`], so the answer is
/// `None`, whatever the field. [`check_request`] is stricter, and names the fields that it allows.
pub fn request_at_version(
    body: Body,
    api_key: i16,
    api_version: i16,
) -> Result<Option<Body>, Error> {
    let header = Header::Request {
        api_key,
        api_version,
        correlation_id: 0,
        client_id: Some("fuzzer".into()),
    };

    match Frame::request(header, body) {
        Err(Error::MandatoryFieldUnset { .. }) => Ok(None),
        encoded => {
            let encoded = encoded.map_err(|error| at_version(error, api_key, api_version))?;

            Frame::request_from_bytes(encoded)
                .map(|frame| Some(frame.body))
                .map_err(|error| at_version(error, api_key, api_version))
        }
    }
}

/// The response counterpart of [`check_request`], with one difference: a response comes out of a
/// service, not out of a fuzzer, so [`Error::MandatoryFieldUnset`] is an error here. It is a
/// service that answers a client at `api_version` with a field that version requires left unset.
pub fn check_response<T>(response: T, api_version: i16) -> Result<(), Error>
where
    T: Response,
{
    check_response_body(response.into(), T::KEY, api_version)
}

/// [`check_response`] for a response that a caller holds as a [`Body`], as it comes out of a
/// route: `api_key` is the API of the request that it answers.
pub fn check_response_body(body: Body, api_key: i16, api_version: i16) -> Result<(), Error> {
    let encode = |body| {
        Frame::response(
            Header::Response { correlation_id: 0 },
            body,
            api_key,
            api_version,
        )
    };

    let once = encode(body).map_err(|error| at_version(error, api_key, api_version))?;
    let body = Frame::response_from_bytes(once.clone(), api_key, api_version)
        .map_err(|error| at_version(error, api_key, api_version))?
        .body;
    let twice = encode(body).map_err(|error| at_version(error, api_key, api_version))?;

    assert_eq!(
        once, twice,
        "api key {api_key} v{api_version}: re-encoding the decoded response changed its bytes",
    );

    Ok(())
}
