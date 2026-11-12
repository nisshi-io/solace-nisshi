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

//! A topic's `max.message.bytes`: the size in bytes of the largest record batch that a produce
//! stores for the topic.
//!
//! [`BatchValidationService`](crate::BatchValidationService) checks a produced batch against the
//! limit before it validates the records of the batch, as Kafka does. Each storage engine checks
//! the batch again in its own produce, so a caller of [`Storage::produce`](crate::Storage::produce)
//! gets the same answer.

use nisshi_sans_io::{ByteSize as _, record::deflated};
use tracing::debug;

use crate::{Error, Result};

/// The topic configuration key.
pub const KEY: &str = "max.message.bytes";

/// Kafka's default `max.message.bytes` for a topic that does not set it.
///
/// The value is 1 MiB plus 12 bytes for `base_offset` and `batch_length`. Kafka counts those two
/// fields as log overhead ([`LogConfig.java#L171`]).
///
/// [`LogConfig.java#L171`]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java#L171
pub const DEFAULT: usize = 1024 * 1024 + 12;

/// Parses `configured` as an integer from 0 to `i32::MAX`, and ignores surrounding whitespace.
/// Kafka parses a `max.message.bytes` value the same way ([`LogConfig.java#L281`],
/// [`ConfigDef.java#L694-L776`]).
///
/// The error is the message that Kafka sends with `INVALID_CONFIG`.
///
/// [`LogConfig.java#L281`]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogConfig.java#L281
/// [`ConfigDef.java#L694-L776`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/config/ConfigDef.java#L694-L776
pub fn parse(configured: &str) -> std::result::Result<usize, String> {
    let limit = configured.trim().parse::<i32>().map_err(|_| {
        format!("Invalid value {configured} for configuration {KEY}: Not a number of type INT")
    })?;

    usize::try_from(limit).map_err(|_| {
        format!("Invalid value {configured} for configuration {KEY}: Value must be at least 0")
    })
}

/// Returns the topic's `max.message.bytes` limit from `configured`, its stored value. The limit is
/// [`DEFAULT`] when the topic does not set the key.
///
/// The limit is also [`DEFAULT`] when [`parse`] rejects the stored value. This function uses the
/// default instead of rejecting every produce, because a mistyped value would then stop all writes
/// to the topic.
pub fn limit(topic: &str, configured: Option<&str>) -> usize {
    let Some(configured) = configured else {
        return DEFAULT;
    };

    parse(configured).unwrap_or_else(|reason| {
        debug!(topic, reason, "using the default max.message.bytes");
        DEFAULT
    })
}

/// Ensures that `batch` is no larger than `limit` bytes.
///
/// The size is the batch's encoded size, from `base_offset` to the end of its records. Kafka checks
/// the same size against the limit. The function returns [`Error::MessageTooLarge`] when `batch` is
/// larger than the limit.
pub fn ensure_batch_fits(batch: &deflated::Batch, limit: usize) -> Result<()> {
    let size = batch.size_in_bytes()?;

    if size > limit {
        Err(Error::MessageTooLarge { size, limit })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use bytes::Bytes;

    use super::*;

    const TOPIC: &str = "abc";

    /// The size of a batch's header: the bytes before its records.
    const BATCH_HEADER_BYTES: usize = 61;

    const LIMIT: usize = 1_000;

    fn batch_with_record_data(len: usize) -> deflated::Batch {
        deflated::Batch {
            record_data: Bytes::from(vec![0; len]),
            ..Default::default()
        }
    }

    fn batch_of_size(size: usize) -> deflated::Batch {
        batch_with_record_data(size - BATCH_HEADER_BYTES)
    }

    #[test]
    fn an_unset_limit_is_the_kafka_default() {
        assert_eq!(1_048_588, limit(TOPIC, None));
    }

    #[test]
    fn a_set_limit_is_used() {
        assert_eq!(10_000, limit(TOPIC, Some("10000")));
        assert_eq!(0, limit(TOPIC, Some("0")));
    }

    #[test]
    fn surrounding_whitespace_is_ignored() {
        assert_eq!(Ok(10_000), parse(" 10000 "));
    }

    #[test]
    fn a_value_that_kafka_refuses_has_kafkas_message() {
        assert_eq!(
            Err(String::from(
                "Invalid value abc for configuration max.message.bytes: Not a number of type INT"
            )),
            parse("abc")
        );
        assert_eq!(
            Err(String::from(
                "Invalid value -1 for configuration max.message.bytes: Value must be at least 0"
            )),
            parse("-1")
        );
    }

    #[test]
    fn a_value_that_kafka_refuses_is_the_default() {
        for configured in ["", "abc", "-1", "1.5", "2147483648"] {
            assert_eq!(DEFAULT, limit(TOPIC, Some(configured)), "{configured:?}");
        }
    }

    #[test]
    fn a_batch_of_exactly_the_limit_is_accepted() -> Result<()> {
        ensure_batch_fits(&batch_of_size(LIMIT), LIMIT)
    }

    #[test]
    fn a_batch_one_byte_over_the_limit_is_rejected() {
        assert_matches!(
            ensure_batch_fits(&batch_of_size(LIMIT + 1), LIMIT),
            Err(Error::MessageTooLarge {
                size: 1_001,
                limit: LIMIT
            })
        );
    }

    /// The size includes the 61-byte batch header. Kafka's `RecordBatch.sizeInBytes` also includes
    /// it.
    #[test]
    fn the_size_includes_the_batch_header() -> Result<()> {
        assert_eq!(
            BATCH_HEADER_BYTES + 100,
            batch_with_record_data(100).size_in_bytes()?
        );
        Ok(())
    }
}
