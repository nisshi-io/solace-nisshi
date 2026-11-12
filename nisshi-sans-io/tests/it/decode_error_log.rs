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

//! A string field that is not valid UTF-8 fails to decode with an error whose
//! `Debug` and `Display` text stays short however long the field is, so
//! logging the error can't write the client's bytes. One case puts the
//! string in the request body, the other in the request header.

use std::assert_matches;

use bytes::Bytes;
use nisshi_sans_io::{
    ApiKey as _, Error, Frame, Header, MetadataRequest, Result,
    metadata_request::MetadataRequestTopic,
};

/// Lengths of the invalid field: a short one, and one whose bytes, printed
/// one by one, would be far longer than [`MAXIMUM_TEXT`].
const LENGTHS: [usize; 2] = [16, 16 * 1024];

/// The longest `Debug` or `Display` text the error may have, whatever the
/// field's length.
const MAXIMUM_TEXT: usize = 128;

/// The invalid field's bytes, which are never valid UTF-8, repeated.
const FILL: [u8; 2] = [0xfe, 0xff];

/// Renderings of [`FILL`] that a text holding the field's bytes would
/// contain: a decimal list, `Bytes`-style escapes, and hex.
const FILL_FORMS: [&str; 4] = ["254, 255", r"\xfe\xff", "fe, ff", "feff"];

/// Replaces the only run of `len` `b'a'` bytes in `frame` with [`FILL`],
/// keeping every length prefix intact.
fn invalid_utf8(frame: Bytes, len: usize) -> Bytes {
    let placeholder = vec![b'a'; len];
    let start = frame
        .windows(len)
        .position(|window| window == placeholder.as_slice())
        .expect("the frame holds the placeholder");

    let mut patched = frame.to_vec();
    for (byte, fill) in patched[start..start + len]
        .iter_mut()
        .zip(FILL.iter().cycle())
    {
        *byte = *fill;
    }
    Bytes::from(patched)
}

/// Decodes `frame`, which must fail on a string of `len` invalid bytes, and
/// asserts that the error's `Debug` and `Display` text stay within
/// [`MAXIMUM_TEXT`] and hold none of the field's bytes, so a short prefix of
/// them can't leak either.
fn assert_bounded(frame: Bytes, len: usize) {
    let error = Frame::request_from_bytes(frame).expect_err("the frame has an invalid string");
    assert_matches!(error, Error::FromUtf8 { len: found, .. } if found == len);

    for (name, text) in [
        ("Debug", format!("{error:?}")),
        ("Display", format!("{error}")),
    ] {
        assert!(
            text.len() <= MAXIMUM_TEXT,
            "a {len} byte field gives {} bytes of {name} text",
            text.len()
        );

        for form in FILL_FORMS {
            assert!(
                !text.contains(form),
                "the {name} text of a {len} byte field holds its bytes as {form:?}"
            );
        }
    }
}

fn metadata(client_id: Option<String>, topic: String) -> Result<Bytes> {
    Frame::request(
        Header::Request {
            api_key: MetadataRequest::KEY,
            api_version: 12,
            correlation_id: 1,
            client_id,
        },
        MetadataRequest::default()
            .topics(Some(
                [MetadataRequestTopic::default()
                    .topic_id(Some([0; 16]))
                    .name(Some(topic))]
                .into(),
            ))
            .allow_auto_topic_creation(Some(false))
            .include_topic_authorized_operations(Some(false))
            .into(),
    )
}

#[test]
fn body_string() -> Result<()> {
    for len in LENGTHS {
        let frame = metadata(Some("client".into()), "a".repeat(len))?;
        assert_bounded(invalid_utf8(frame, len), len);
    }

    Ok(())
}

#[test]
fn header_client_id() -> Result<()> {
    for len in LENGTHS {
        let frame = metadata(Some("a".repeat(len)), "topic".into())?;
        assert_bounded(invalid_utf8(frame, len), len);
    }

    Ok(())
}
