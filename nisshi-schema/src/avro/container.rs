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

//! The header of an Avro object container.

use nisshi_sans_io::ErrorCode;
use tracing::debug;

use crate::{Error, Result};

const MAGIC: &[u8] = b"Obj\x01";
const CODEC_KEY: &[u8] = b"avro.codec";
const NULL_CODEC: &[u8] = b"null";
const MAX_LOGGED_CODEC_LENGTH: usize = 32;

/// The most bytes that a variable-length `long` takes: 64 bits at 7 bits a byte.
const MAX_LONG_LENGTH: usize = 10;

/// Refuses an Avro object container whose header names a codec other than `null`, before any
/// block is decompressed.
///
/// Returns [`Error::Api`] with [`ErrorCode::InvalidRecord`] for any other codec, and for a header
/// that is truncated or malformed. A header without an `avro.codec` entry uses `null`, as the
/// Avro specification of [Object Container Files] says.
///
/// [Object Container Files]: https://avro.apache.org/docs/1.12.0/specification/#object-container-files
pub(crate) fn require_null_codec(encoded: &[u8]) -> Result<()> {
    let mut metadata = encoded
        .strip_prefix(MAGIC)
        .ok_or(Error::Api(ErrorCode::InvalidRecord))?;

    loop {
        let count = read_long(&mut metadata)?;
        if count == 0 {
            return Ok(());
        }

        // A block with a negative count also gives its size in bytes, which this check does not
        // need.
        if count < 0 {
            _ = read_long(&mut metadata)?;
        }

        // Each entry takes at least two bytes, so a count larger than the input ends with an
        // error when the input runs out.
        for _ in 0..count.unsigned_abs() {
            let key = read_bytes(&mut metadata)?;
            let value = read_bytes(&mut metadata)?;

            // A header can repeat a key, so every `avro.codec` entry must be `null`, not only the
            // first or the last.
            if key == CODEC_KEY && value != NULL_CODEC {
                // The producer chooses the codec name, so the log shows at most its first bytes.
                debug!(
                    codec = %String::from_utf8_lossy(&value[..value.len().min(MAX_LOGGED_CODEC_LENGTH)]),
                    "Avro object container names a codec other than null"
                );
                return Err(Error::Api(ErrorCode::InvalidRecord));
            }
        }
    }
}

/// Reads a zigzag-encoded variable-length `long` from the start of `input`, and advances `input`
/// past it.
fn read_long(input: &mut &[u8]) -> Result<i64> {
    let mut zigzag = 0u64;

    for (index, byte) in input.iter().copied().enumerate().take(MAX_LONG_LENGTH) {
        // The last byte holds only bit 63, so any higher bit overflows 64 bits.
        if index == MAX_LONG_LENGTH - 1 && byte > 1 {
            return Err(Error::Api(ErrorCode::InvalidRecord));
        }

        zigzag |= u64::from(byte & 0x7f) << (7 * index);

        if byte & 0x80 == 0 {
            *input = &input[index + 1..];
            return Ok(((zigzag >> 1) as i64) ^ -((zigzag & 1) as i64));
        }
    }

    Err(Error::Api(ErrorCode::InvalidRecord))
}

/// Reads a length-prefixed `bytes` or `string` from the start of `input`, and advances `input`
/// past it.
fn read_bytes<'a>(input: &mut &'a [u8]) -> Result<&'a [u8]> {
    let length = read_long(input).and_then(|length| {
        usize::try_from(length).map_err(|_| Error::Api(ErrorCode::InvalidRecord))
    })?;

    let (bytes, remaining) = input
        .split_at_checked(length)
        .ok_or(Error::Api(ErrorCode::InvalidRecord))?;

    *input = remaining;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use apache_avro::{Codec, DeflateSettings, Schema, Writer, types::Value};

    use super::*;

    const SYNC_MARKER: [u8; 16] = [0xaa; 16];

    fn long(value: i64) -> Vec<u8> {
        let mut zigzag = ((value << 1) ^ (value >> 63)) as u64;
        let mut encoded = vec![];
        loop {
            let low_bits = (zigzag & 0x7f) as u8;
            zigzag >>= 7;
            if zigzag == 0 {
                encoded.push(low_bits);
                return encoded;
            }
            encoded.push(low_bits | 0x80);
        }
    }

    fn length_prefixed(bytes: &[u8]) -> Vec<u8> {
        let mut encoded = long(bytes.len() as i64);
        encoded.extend_from_slice(bytes);
        encoded
    }

    /// Returns an object container header with one metadata block holding `entries`, written
    /// with a positive count.
    fn header(entries: &[(&[u8], &[u8])]) -> Vec<u8> {
        let mut encoded = b"Obj\x01".to_vec();
        if !entries.is_empty() {
            encoded.extend(long(entries.len() as i64));
            for (key, value) in entries {
                encoded.extend(length_prefixed(key));
                encoded.extend(length_prefixed(value));
            }
        }
        encoded.extend(long(0));
        encoded.extend(SYNC_MARKER);
        encoded
    }

    fn container_with_codec(codec: Codec) -> Result<Vec<u8>> {
        let schema = Schema::parse_str(r#""int""#)?;
        let mut writer = Writer::with_codec(&schema, vec![], codec)?;
        _ = writer.append_value(Value::Int(32123))?;
        writer.into_inner().map_err(Into::into)
    }

    fn is_invalid_record(result: Result<()>) {
        assert_matches!(result, Err(Error::Api(ErrorCode::InvalidRecord)));
    }

    #[test]
    fn absent_codec_is_accepted() -> Result<()> {
        require_null_codec(&header(&[(b"avro.schema", br#""int""#)]))
    }

    #[test]
    fn null_codec_is_accepted() -> Result<()> {
        require_null_codec(&header(&[(b"avro.codec", b"null")]))
    }

    #[test]
    fn null_codec_container_is_accepted() -> Result<()> {
        require_null_codec(&container_with_codec(Codec::Null)?)
    }

    #[test]
    fn empty_metadata_is_accepted() -> Result<()> {
        require_null_codec(&header(&[]))
    }

    #[test]
    fn deflate_container_is_refused() -> Result<()> {
        is_invalid_record(require_null_codec(&container_with_codec(Codec::Deflate(
            DeflateSettings::default(),
        ))?));
        Ok(())
    }

    #[test]
    fn snappy_container_is_refused() -> Result<()> {
        is_invalid_record(require_null_codec(&container_with_codec(Codec::Snappy)?));
        Ok(())
    }

    #[test]
    fn zstandard_container_is_refused() -> Result<()> {
        is_invalid_record(require_null_codec(&container_with_codec(
            Codec::Zstandard(apache_avro::ZstandardSettings::default()),
        )?));
        Ok(())
    }

    #[test]
    fn unknown_codec_is_refused() {
        is_invalid_record(require_null_codec(&header(&[(b"avro.codec", b"lz4")])));
    }

    #[test]
    fn later_duplicate_codec_is_refused() {
        is_invalid_record(require_null_codec(&header(&[
            (b"avro.codec", b"null"),
            (b"avro.codec", b"deflate"),
        ])));
    }

    #[test]
    fn codec_in_block_with_negative_count_is_refused() {
        let mut encoded = b"Obj\x01".to_vec();
        encoded.extend(block_with_negative_count(b"avro.codec", b"deflate"));
        encoded.extend(long(0));
        encoded.extend(SYNC_MARKER);

        is_invalid_record(require_null_codec(&encoded));
    }

    /// Returns a metadata block with a negative count, which gives its size in bytes.
    fn block_with_negative_count(key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut entry = length_prefixed(key);
        entry.extend(length_prefixed(value));

        let mut encoded = long(-1);
        encoded.extend(long(entry.len() as i64));
        encoded.extend(entry);
        encoded
    }

    #[test]
    fn null_codec_in_block_with_negative_count_is_accepted() -> Result<()> {
        let mut encoded = b"Obj\x01".to_vec();
        encoded.extend(block_with_negative_count(b"avro.codec", b"null"));
        encoded.extend(long(0));
        encoded.extend(SYNC_MARKER);

        require_null_codec(&encoded)
    }

    #[test]
    fn codec_in_second_block_is_refused() {
        let mut encoded = b"Obj\x01".to_vec();
        encoded.extend(long(1));
        encoded.extend(length_prefixed(b"avro.schema"));
        encoded.extend(length_prefixed(br#""int""#));
        encoded.extend(block_with_negative_count(b"avro.codec", b"deflate"));
        encoded.extend(long(0));
        encoded.extend(SYNC_MARKER);

        is_invalid_record(require_null_codec(&encoded));
    }

    #[test]
    fn empty_input_is_refused() {
        is_invalid_record(require_null_codec(b""));
    }

    #[test]
    fn wrong_magic_is_refused() {
        let mut encoded = header(&[]);
        encoded[3] = 2;
        is_invalid_record(require_null_codec(&encoded));
    }

    #[test]
    fn truncated_header_is_refused() {
        let encoded = header(&[(b"avro.codec", b"null")]);
        for length in 0..encoded.len() - SYNC_MARKER.len() {
            is_invalid_record(require_null_codec(&encoded[..length]));
        }
    }

    #[test]
    fn length_past_end_of_input_is_refused() {
        let mut encoded = b"Obj\x01".to_vec();
        encoded.extend(long(1));
        encoded.extend(long(i64::MAX));
        encoded.extend(b"avro.codec");
        is_invalid_record(require_null_codec(&encoded));
    }

    #[test]
    fn negative_length_is_refused() {
        let mut encoded = b"Obj\x01".to_vec();
        encoded.extend(long(1));
        encoded.extend(long(-1));
        encoded.extend(long(0));
        is_invalid_record(require_null_codec(&encoded));
    }

    #[test]
    fn minimum_count_is_refused() {
        let mut encoded = b"Obj\x01".to_vec();
        encoded.extend(long(i64::MIN));
        encoded.extend(long(0));
        is_invalid_record(require_null_codec(&encoded));
    }

    #[test]
    fn varint_longer_than_ten_bytes_is_refused() {
        let mut encoded = b"Obj\x01".to_vec();
        encoded.extend([0x80; 10]);
        encoded.push(0);
        is_invalid_record(require_null_codec(&encoded));
    }

    #[test]
    fn varint_overflowing_i64_is_refused() {
        // The overflowing varint is the size of a block with a negative count, which the parser
        // reads and ignores, so the overflow check is the only check that refuses this header.
        let mut entry = length_prefixed(b"avro.codec");
        entry.extend(length_prefixed(b"null"));

        let mut encoded = b"Obj\x01".to_vec();
        encoded.extend(long(-1));
        encoded.extend([0xff; 9]);
        encoded.push(0x02);
        encoded.extend(entry);
        encoded.extend(long(0));
        encoded.extend(SYNC_MARKER);

        is_invalid_record(require_null_codec(&encoded));
    }
}
