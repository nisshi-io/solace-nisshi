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

//! The limit on the metadata of a committed offset.
//!
//! `OffsetCommit` and `TxnOffsetCommit` answer `OFFSET_METADATA_TOO_LARGE` for each partition whose
//! metadata is over the limit, and do not store that partition's offset. They store the other
//! partitions of the request. Kafka does the same ([`OffsetMetadataManager.commitOffset`]).
//!
//! `OffsetFetch` returns the stored metadata of each partition, so the limit also bounds the size
//! of an `OffsetFetch` response. An `OffsetFetch` of version 0 to 5 encodes the metadata with a
//! 16-bit length, and can't encode a value longer than 32767 bytes.
//!
//! [`OffsetMetadataManager.commitOffset`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java#L471-L504

/// The longest metadata that a commit stores, in UTF-16 code units.
///
/// The limit is the default of Kafka's `offset.metadata.max.bytes`. Kafka compares it with
/// `String.length()`, which counts UTF-16 code units, not bytes ([`isMetadataInvalid`]). Nisshi
/// does not make the limit configurable.
///
/// [`isMetadataInvalid`]: https://github.com/apache/kafka/blob/3.9.1/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java#L414-L416
pub const MAX_LENGTH: usize = 4096;

/// Returns whether `metadata` is too large to commit: longer than [`MAX_LENGTH`] UTF-16 code
/// units. A null metadata is never too large.
pub fn is_too_large(metadata: Option<&str>) -> bool {
    metadata.is_some_and(|metadata| metadata.encode_utf16().nth(MAX_LENGTH).is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_metadata_is_not_too_large() {
        assert!(!is_too_large(None));
    }

    #[test]
    fn metadata_of_max_length_is_not_too_large() {
        assert!(!is_too_large(Some(&"m".repeat(MAX_LENGTH))));
    }

    #[test]
    fn metadata_longer_than_max_length_is_too_large() {
        assert!(is_too_large(Some(&"m".repeat(MAX_LENGTH + 1))));
    }

    #[test]
    fn length_counts_utf16_code_units_not_bytes() {
        assert!(!is_too_large(Some(&"é".repeat(MAX_LENGTH))));
        assert!(is_too_large(Some(&"😀".repeat(MAX_LENGTH / 2 + 1))));
    }
}
