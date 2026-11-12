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

//! Gap 6: Encoder/Decoder error path tests.
//!
//! Tests that malformed, truncated, or invalid input produces `Err`

use bytes::Bytes;
use nisshi_sans_io::{Body, Compression, ErrorCode, FindCoordinatorRequest, Frame, Header, Result};

// ---------------------------------------------------------------------------
// Helper: build a minimal valid request frame for FindCoordinator (api_key=10, v4)
// ---------------------------------------------------------------------------

fn minimal_find_coordinator_v4_bytes() -> Vec<u8> {
    // A valid FindCoordinatorRequest v4 captured from existing tests (decode.rs)
    vec![
        0, 0, 0, 50, // size = 50
        0, 10, // api_key = 10 (FindCoordinator)
        0, 4, // api_version = 4
        0, 0, 0, 0, // correlation_id = 0
        0, 16, // client_id length = 16
        99, 111, 110, 115, 111, 108, 101, 45, 99, 111, 110, 115, 117, 109, 101,
        114, // "console-consumer"
        0,   // tag_buffer (empty, flexible version)
        0, 2,  // coordinator_keys compact array length (1 element, +1 for compact)
        20, // compact string length (19 + 1)
        116, 101, 115, 116, 45, 99, 111, 110, 115, 117, 109, 101, 114, 45, 103, 114, 111, 117,
        112, // "test-consumer-group"
        1,   // coordinator type = GROUP
        0,   // tag_buffer
        0,   // tag_buffer
    ]
}

// ===========================================================================
// 1. Empty input
// ===========================================================================

#[test]
fn request_from_empty_bytes() -> Result<()> {
    let result = Frame::request_from_bytes(Bytes::new());
    assert!(result.is_err(), "empty bytes should fail");
    Ok(())
}

#[test]
fn response_from_empty_bytes() -> Result<()> {
    // api_key=10 (FindCoordinator), api_version=4
    let result = Frame::response_from_bytes(Bytes::new(), 10, 4);
    assert!(result.is_err(), "empty bytes should fail for response");
    Ok(())
}

// ===========================================================================
// 2. Truncated input — cut at various points in the frame
// ===========================================================================

#[test]
fn request_truncated_at_size_field() -> Result<()> {
    // Only 2 of the 4 size bytes
    let result = Frame::request_from_bytes(Bytes::from_static(&[0, 0]));
    assert!(result.is_err(), "truncated size field should fail");
    Ok(())
}

#[test]
fn request_truncated_at_api_key() -> Result<()> {
    // 4-byte size + only 1 byte of api_key
    let result = Frame::request_from_bytes(Bytes::from_static(&[0, 0, 0, 10, 0]));
    assert!(result.is_err(), "truncated api_key should fail");
    Ok(())
}

#[test]
fn request_truncated_at_api_version() -> Result<()> {
    // 4-byte size + 2-byte api_key, but no api_version
    let result = Frame::request_from_bytes(Bytes::from_static(&[0, 0, 0, 10, 0, 10]));
    assert!(result.is_err(), "truncated api_version should fail");
    Ok(())
}

#[test]
fn request_truncated_at_correlation_id() -> Result<()> {
    // size + api_key + api_version, but only 2 of 4 bytes of correlation_id
    let result = Frame::request_from_bytes(Bytes::from_static(&[
        0, 0, 0, 20, // size
        0, 10, // api_key = 10
        0, 4, // api_version = 4
        0, 0, // only 2 bytes of correlation_id
    ]));
    assert!(result.is_err(), "truncated correlation_id should fail");
    Ok(())
}

#[test]
fn request_truncated_mid_body() -> Result<()> {
    // Take a valid frame and cut it in half
    let full = minimal_find_coordinator_v4_bytes();
    let half = &full[..full.len() / 2];
    let result = Frame::request_from_bytes(Bytes::copy_from_slice(half));
    assert!(result.is_err(), "frame truncated mid-body should fail");
    Ok(())
}

#[test]
fn response_truncated_mid_frame() -> Result<()> {
    // Only a partial size field for a response
    let result = Frame::response_from_bytes(Bytes::from_static(&[0, 0, 0]), 10, 4);
    assert!(result.is_err(), "truncated response frame should fail");
    Ok(())
}

// ===========================================================================
// 3. Unknown API key
// ===========================================================================

#[test]
fn request_unknown_api_key() -> Result<()> {
    // api_key=999 doesn't correspond to any Kafka API.
    let bytes = Bytes::from_static(&[
        0, 0, 0, 12, // size = 12
        3, 231, // api_key = 999 (unknown)
        0, 0, // api_version = 0
        0, 0, 0, 1, // correlation_id = 1
        255, 255, // client_id = null (length -1)
    ]);
    let result = Frame::request_from_bytes(bytes);
    assert!(result.is_err(), "unknown api_key should fail");
    Ok(())
}

#[test]
fn response_unknown_api_key() -> Result<()> {
    // api_key=999 doesn't correspond to any Kafka API.
    let bytes = Bytes::from_static(&[
        0, 0, 0, 4, // size = 4
        0, 0, 0, 1, // correlation_id = 1
    ]);
    let result = Frame::response_from_bytes(bytes, 999, 0);
    assert!(result.is_err(), "unknown api_key in response should fail");
    Ok(())
}

// ===========================================================================
// 4. Frame accessor errors (ResponseFrame error)
// ===========================================================================

#[test]
fn response_header_api_key_returns_error() -> Result<()> {
    let frame = Frame {
        size: 0,
        header: Header::Response { correlation_id: 0 },
        body: Body::FindCoordinatorRequest(FindCoordinatorRequest::default()),
    };
    assert!(
        frame.api_key().is_err(),
        "api_key() on response header should fail"
    );
    assert!(
        frame.api_version().is_err(),
        "api_version() on response header should fail"
    );
    assert!(
        frame.client_id().is_err(),
        "client_id() on response header should fail"
    );
    Ok(())
}

// ===========================================================================
// 5. ErrorCode — out-of-range values
// ===========================================================================

#[test]
fn error_code_unknown_positive() {
    assert!(
        ErrorCode::try_from(9999i16).is_err(),
        "unknown positive error code should fail"
    );
}

#[test]
fn error_code_unknown_negative() {
    assert!(
        ErrorCode::try_from(-100i16).is_err(),
        "unknown negative error code should fail"
    );
}

#[test]
fn error_code_min_value() {
    assert!(
        ErrorCode::try_from(i16::MIN).is_err(),
        "i16::MIN error code should fail"
    );
}

#[test]
fn error_code_max_value() {
    assert!(
        ErrorCode::try_from(i16::MAX).is_err(),
        "i16::MAX error code should fail"
    );
}

// ===========================================================================
// 6. Compression — invalid discriminants
// ===========================================================================

#[test]
fn compression_invalid_value() {
    assert!(
        Compression::try_from(5i16).is_err(),
        "compression type 5 should fail"
    );
    assert!(
        Compression::try_from(6i16).is_err(),
        "compression type 6 should fail"
    );
    assert!(
        Compression::try_from(7i16).is_err(),
        "compression type 7 should fail"
    );
}

// ===========================================================================
// 7. Garbage bytes — random data that isn't valid protocol
// ===========================================================================

#[test]
fn request_from_garbage_bytes() -> Result<()> {
    let garbage = Bytes::from_static(&[0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE]);
    let result = Frame::request_from_bytes(garbage);
    assert!(result.is_err(), "garbage bytes should fail");
    Ok(())
}

#[test]
fn request_from_all_zeros() -> Result<()> {
    // api_key=0 is Produce, version=0, but body is empty
    let zeros = Bytes::from_static(&[0u8; 20]);
    let result = Frame::request_from_bytes(zeros);
    assert!(
        result.is_err(),
        "all-zero bytes should fail (truncated body)"
    );
    Ok(())
}

#[test]
fn request_from_all_ones() -> Result<()> {
    // 0xFF 0xFF as api_key = -1, which is not a valid Kafka API key.
    let ones = Bytes::from_static(&[0xFF; 20]);
    let result = Frame::request_from_bytes(ones);
    assert!(result.is_err(), "all-0xFF bytes should fail");
    Ok(())
}

// ===========================================================================
// 8. Size field mismatch — size says more bytes than available
// ===========================================================================

#[test]
fn request_size_larger_than_payload() -> Result<()> {
    // Size claims 1000 bytes but only 8 bytes follow
    let bytes = Bytes::from_static(&[
        0, 0, 3, 232, // size = 1000
        0, 10, // api_key = 10
        0, 4, // api_version = 4
        0, 0, 0, 0, // correlation_id = 0
    ]);
    let result = Frame::request_from_bytes(bytes);
    assert!(
        result.is_err(),
        "size larger than available payload should fail"
    );
    Ok(())
}

// ===========================================================================
// 9. Negative string length (non-nullable context)
// ===========================================================================

#[test]
fn request_with_corrupt_string_length() -> Result<()> {
    // Valid header for FindCoordinator v0 (non-flexible), but corrupt client_id length
    let bytes = Bytes::from_static(&[
        0, 0, 0, 20, // size
        0, 10, // api_key = 10 (FindCoordinator)
        0, 0, // api_version = 0
        0, 0, 0, 1, // correlation_id = 1
        0x7F, 0xFF, // client_id length = 32767 (way more than available)
        0, 0, 0, 0, 0, 0, // some trailing bytes
    ]);
    let result = Frame::request_from_bytes(bytes);
    assert!(
        result.is_err(),
        "string length exceeding available bytes should fail"
    );
    Ok(())
}

// ===========================================================================
// 10. Single byte input
// ===========================================================================

#[test]
fn request_from_single_byte() -> Result<()> {
    let result = Frame::request_from_bytes(Bytes::from_static(&[0x00]));
    assert!(result.is_err(), "single byte should fail");
    Ok(())
}

#[test]
fn response_from_single_byte() -> Result<()> {
    let result = Frame::response_from_bytes(Bytes::from_static(&[0x00]), 10, 4);
    assert!(result.is_err(), "single byte response should fail");
    Ok(())
}

// ===========================================================================
// 11. Negative API key
// ===========================================================================

#[test]
fn request_negative_api_key() -> Result<()> {
    // api_key = -2 is not a valid Kafka API key.
    let bytes = Bytes::from_static(&[
        0, 0, 0, 12, // size
        0xFF, 0xFE, // api_key = -2
        0, 0, // api_version = 0
        0, 0, 0, 1, // correlation_id = 1
        255, 255, // client_id = null
    ]);
    let result = Frame::request_from_bytes(bytes);
    assert!(result.is_err(), "negative api_key should fail");
    Ok(())
}

// ===========================================================================
// 12. Tagged field data length lies about its size
// ===========================================================================

#[test]
fn request_tagged_field_oversized_length_does_not_abort() -> Result<()> {
    // `fuzz_request_decode` found this exact input (minimized) aborting the
    // process: a tagged field's byte-length varint claims ~3.5GB, and
    // `TagField`'s decoder pre-allocated a `Vec` of that size before reading
    // a single byte, well before the truncated buffer could fail the read.
    // It must now fail gracefully (`Err`) instead of aborting.
    let bytes = Bytes::from_static(&[
        148, 239, 1, 8, 0, 0, 7, 1, 0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 210, 255, 255, 255, 108, 230,
    ]);
    let result = Frame::request_from_bytes(bytes);
    assert!(result.is_err(), "oversized tagged field length should fail");
    Ok(())
}
