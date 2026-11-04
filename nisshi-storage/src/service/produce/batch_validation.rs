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

use nisshi_sans_io::{BatchAttribute, ErrorCode, record::deflated};

use super::rejection::Rejection;

/// The message sent with `CORRUPT_MESSAGE` for a batch whose CRC does not
/// match its contents.
pub(super) const CRC_MISMATCH: &str = "batch crc does not match its contents";

/// Why a client batch must be rejected before anything is written, with the
/// error code to send, or `None` if it may be stored. Kafka's `UnifiedLog` and
/// `LogValidator` reject the same batches.
///
/// - The CRC must match the batch's contents, from `attributes` through the
///   end of the records, as Kafka computes it ([DefaultRecordBatch.java#L393-L398]).
///   Kafka checks it before the batch's header fields
///   ([UnifiedLog.scala#L1189-L1193]): when the CRC is wrong, the attributes,
///   the record count and the control bit are suspect too, so this check runs
///   first. The memory and object store backends store the bytes as sent, so
///   a consumer that checks CRCs would fail on every fetch of the batch.
///   `CORRUPT_MESSAGE`.
/// - Only the broker writes control batches (transaction commit/abort
///   markers), and those go directly to storage rather than through
///   [`ProduceService`](super::ProduceService). Every backend skips schema validation and lake
///   writes for a control batch, so a client must not be able to set the bit.
///   `INVALID_RECORD`.
/// - The compression codec in the attributes must be one Kafka defines. An
///   unknown codec id is a defect in the client's batch header, like a bad
///   record count. Kafka returns `UNKNOWN_SERVER_ERROR` for it only because
///   its codec lookup throws an unchecked exception that reaches the generic
///   handler. Checking here also stops a backend from advancing the high
///   watermark before it parses the attributes. `INVALID_RECORD`.
/// - A consistent header has at least one record and
///   `last_offset_delta + 1 == record_count`. Every backend uses
///   `last_offset_delta` to advance the high watermark, so a mismatch corrupts
///   or wedges the partition. `record_count` is an int32 on the wire, so a
///   value above `i32::MAX` is rejected too. `INVALID_RECORD`.
/// - `record_count` alone must not imply more decoded memory than the
///   decoded-size limit allows. `nisshi-sans-io` enforces the limit during
///   decode as well; checking here rejects the batch before `storage.produce()`
///   and before any decompression is paid for. `MESSAGE_TOO_LARGE`, the same
///   code the decode-time rejection maps to (see [`storage_error_code`](super::rejection::storage_error_code)).
///
/// [DefaultRecordBatch.java#L393-L398]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L393-L398
/// [UnifiedLog.scala#L1189-L1193]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/log/UnifiedLog.scala#L1189-L1193
pub(super) fn rejection(batch: &deflated::Batch) -> Option<(ErrorCode, &'static str)> {
    if batch.computed_crc() != batch.crc {
        return Some((ErrorCode::CorruptMessage, CRC_MISMATCH));
    }

    if batch.is_control() {
        return Some((
            ErrorCode::InvalidRecord,
            "clients may not write control batches",
        ));
    }

    if BatchAttribute::try_from(batch.attributes).is_err() {
        return Some((ErrorCode::InvalidRecord, "unknown compression codec"));
    }

    let Ok(record_count) = i32::try_from(batch.record_count) else {
        return Some((ErrorCode::InvalidRecord, "record_count exceeds i32::MAX"));
    };

    if record_count < 1 {
        Some((ErrorCode::InvalidRecord, "batch has no records"))
    } else if batch.last_offset_delta.checked_add(1) != Some(record_count) {
        Some((
            ErrorCode::InvalidRecord,
            "last_offset_delta + 1 does not equal record_count",
        ))
    } else if batch.exceeds_decoded_record_count_limit() {
        Some((
            ErrorCode::MessageTooLarge,
            "record_count exceeds the maximum decoded batch size",
        ))
    } else {
        None
    }
}

/// The rejection for a batch whose CRC does not match its contents, as
/// [`rejection`] answers it.
pub(super) fn corrupt_batch() -> Rejection {
    Rejection::new(ErrorCode::CorruptMessage, CRC_MISMATCH)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `batch` with a CRC that matches its contents, so that [`rejection`]
    /// gets past its CRC check to the check under test.
    fn with_valid_crc(mut batch: deflated::Batch) -> deflated::Batch {
        batch.crc = batch.computed_crc();
        batch
    }

    #[test]
    fn a_batch_whose_crc_does_not_match_is_a_corrupt_message() {
        let mut batch = with_valid_crc(deflated::Batch {
            record_count: 1,
            last_offset_delta: 0,
            ..Default::default()
        });
        assert_eq!(None, rejection(&batch));

        batch.crc ^= 1;
        assert_eq!(
            Some((ErrorCode::CorruptMessage, CRC_MISMATCH)),
            rejection(&batch)
        );
    }

    /// Kafka checks the CRC before the control bit, so a control batch with
    /// a bad CRC is corrupt, not an invalid record.
    #[test]
    fn the_crc_is_checked_before_the_control_bit() {
        let mut batch = with_valid_crc(deflated::Batch {
            attributes: BatchAttribute::default().control(true).into(),
            record_count: 1,
            last_offset_delta: 0,
            ..Default::default()
        });
        assert_eq!(
            Some((
                ErrorCode::InvalidRecord,
                "clients may not write control batches"
            )),
            rejection(&batch)
        );

        batch.crc ^= 1;
        assert_eq!(
            Some((ErrorCode::CorruptMessage, CRC_MISMATCH)),
            rejection(&batch)
        );
    }

    #[test]
    fn oversized_record_count_is_rejected_before_storage_is_called() {
        let batch = with_valid_crc(deflated::Batch {
            record_count: 1_000_000,
            last_offset_delta: 999_999,
            ..Default::default()
        });

        assert_eq!(
            Some((
                ErrorCode::MessageTooLarge,
                "record_count exceeds the maximum decoded batch size"
            )),
            rejection(&batch)
        );
    }

    #[test]
    fn a_record_count_within_the_limit_is_not_rejected_for_this_reason() {
        let batch = with_valid_crc(deflated::Batch {
            record_count: 1,
            last_offset_delta: 0,
            ..Default::default()
        });

        assert_eq!(None, rejection(&batch));
    }

    #[test]
    fn an_inconsistent_header_is_an_invalid_record() {
        let batch = with_valid_crc(deflated::Batch {
            record_count: 2,
            last_offset_delta: 0,
            ..Default::default()
        });

        assert_eq!(
            Some((
                ErrorCode::InvalidRecord,
                "last_offset_delta + 1 does not equal record_count"
            )),
            rejection(&batch)
        );
    }
}
