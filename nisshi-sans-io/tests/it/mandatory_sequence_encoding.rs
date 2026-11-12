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

//! Every sequence field is generated as `Option<Vec<T>>` regardless of
//! whether the Kafka spec actually marks it `nullableVersions` (see
//! `nisshi-sans-io/build.rs`'s `kind()`), so a *mandatory* array field can
//! still be `None` at the Rust level (e.g. a handler's `Default::default()`
//! response that never set it). `fuzz_describe_user_scram_credentials_storage`
//! found that the encoder wrote nothing at all for such a field instead of an
//! empty array, desyncing every field written after it and producing an
//! `UnexpectedEof` on decode.

use crate::common::init_tracing;
use nisshi_sans_io::{
    Body, DescribeConfigsRequest, DescribeUserScramCredentialsResponse, Frame, Header, Result,
};

#[test]
fn mandatory_sequence_left_none_round_trips_as_empty_flexible() -> Result<()> {
    let _guard = init_tracing()?;

    // `results` (`[]DescribeUserScramCredentialsResult`) has no
    // `nullableVersions`: it's mandatory, but `::default()` leaves it `None`.
    let response: DescribeUserScramCredentialsResponse =
        DescribeUserScramCredentialsResponse::default();
    assert_eq!(None, response.results);

    let decoded = Frame::response(
        Header::Response { correlation_id: 0 },
        response.into(),
        50,
        0,
    )
    .and_then(|encoded| Frame::response_from_bytes(encoded, 50, 0))?;

    let Body::DescribeUserScramCredentialsResponse(decoded) = decoded.body else {
        panic!("wrong body variant: {decoded:?}");
    };

    assert_eq!(
        Some(vec![]),
        decoded.results,
        "a mandatory sequence left at None must decode back as an empty array, not be lost"
    );

    Ok(())
}

#[test]
fn mandatory_sequence_left_none_round_trips_as_empty_non_flexible() -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 32,
        api_version: 0,
        correlation_id: 0,
        client_id: Some("test".into()),
    };

    // `resources` (`[]DescribeConfigsResource`) has no `nullableVersions`
    // either; leaving it `None` must not desync `include_synonyms`, which
    // follows it on the wire.
    let request = DescribeConfigsRequest::default();
    assert_eq!(None, request.resources);

    let decoded = Frame::request(header, request.into()).and_then(Frame::request_from_bytes)?;

    let Body::DescribeConfigsRequest(decoded) = decoded.body else {
        panic!("wrong body variant: {decoded:?}");
    };

    assert_eq!(
        Some(vec![]),
        decoded.resources,
        "a mandatory sequence left at None must decode back as an empty array, not be lost"
    );

    Ok(())
}
