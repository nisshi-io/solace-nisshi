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

use std::{
    convert::Infallible,
    fmt::{self, Debug},
};

use nisshi_sans_io::{
    BatchAttribute, ErrorCode, produce_response::PartitionProduceResponse, record::deflated,
};
use rama::{Layer, Service};
use tracing::{instrument, warn};

use super::{partition::PartitionProduce, rejection::Rejection};

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
fn rejection(batch: &deflated::Batch) -> Option<(ErrorCode, &'static str)> {
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

/// A [`Layer`] that gives its inner [`Service`] the one batch of a partition, after the checks
/// that need no storage: the number of batches, and the fields of the batch header.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BatchValidationLayer;

impl BatchValidationLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for BatchValidationLayer {
    type Service = BatchValidationService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BatchValidationService { inner }
    }
}

/// A [`Service`] passing the one batch of a partition to `inner`, or answering the partition with
/// the error code that Kafka gives a partition whose batches it must reject: `INVALID_RECORD` for
/// a partition that does not have exactly one batch, and the code of the failed check for a
/// batch whose header fails one.
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BatchValidationService<S> {
    inner: S,
}

impl<S> Debug for BatchValidationService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(BatchValidationService)).finish()
    }
}

impl<S> Service<PartitionProduce<Vec<deflated::Batch>>> for BatchValidationService<S>
where
    S: Service<
            PartitionProduce<deflated::Batch>,
            Output = PartitionProduceResponse,
            Error = Infallible,
        >,
{
    type Output = PartitionProduceResponse;
    type Error = Infallible;

    #[instrument(skip_all)]
    async fn serve(
        &self,
        request: PartitionProduce<Vec<deflated::Batch>>,
    ) -> Result<Self::Output, Self::Error> {
        let (request, batches) = request.take_batch();

        // Kafka allows exactly one batch per partition from Produce v3, the
        // first version that carries record batches, which is the only format
        // this broker decodes. Kafka checks the count before it validates any
        // batch, so this check runs before `rejection`. See
        // [ProduceRequest.java#L225-L246].
        //
        // [ProduceRequest.java#L225-L246]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/ProduceRequest.java#L225-L246
        let batch_count = batches.len();
        let Ok([batch]) = <[deflated::Batch; 1]>::try_from(batches) else {
            const REASON: &str =
                "a produce request must contain exactly one record batch per partition";

            warn!(
                topic = request.topic,
                partition = request.index,
                batch_count,
                reason = REASON,
                "rejecting produce request",
            );

            return Ok(
                Rejection::new(ErrorCode::InvalidRecord, REASON).into_response(request.index)
            );
        };

        if let Some(rejection) = rejection(&batch) {
            let rejection = Rejection::from(rejection);
            rejection.log(&request.topic, request.index, &batch);

            return Ok(rejection.into_response(request.index));
        }

        self.inner.serve(request.with_batch(batch)).await
    }
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

    mod service {
        use super::*;
        use crate::service::produce::test_support::{
            INDEX, RecordingPartitions, error_code, partition_produce,
        };

        fn valid_batch() -> deflated::Batch {
            with_valid_crc(deflated::Batch {
                record_count: 1,
                last_offset_delta: 0,
                ..Default::default()
            })
        }

        #[tokio::test]
        async fn a_partition_without_a_batch_is_an_invalid_record() {
            let inner = RecordingPartitions::<deflated::Batch>::default();

            let Ok(response) = BatchValidationLayer::new()
                .layer(inner.clone())
                .serve(partition_produce(vec![], &[]))
                .await;

            assert_eq!(ErrorCode::InvalidRecord, error_code(&response));
            assert_eq!(INDEX, response.index);
            assert!(inner.served().is_empty());
        }

        #[tokio::test]
        async fn a_partition_with_two_batches_is_an_invalid_record() {
            let inner = RecordingPartitions::<deflated::Batch>::default();

            let Ok(response) = BatchValidationLayer::new()
                .layer(inner.clone())
                .serve(partition_produce(vec![valid_batch(), valid_batch()], &[]))
                .await;

            assert_eq!(ErrorCode::InvalidRecord, error_code(&response));
            assert!(inner.served().is_empty());
        }

        #[tokio::test]
        async fn a_batch_that_fails_a_header_check_is_answered_with_its_error_code() {
            let inner = RecordingPartitions::<deflated::Batch>::default();

            let mut batch = valid_batch();
            batch.crc ^= 1;

            let Ok(response) = BatchValidationLayer::new()
                .layer(inner.clone())
                .serve(partition_produce(vec![batch], &[]))
                .await;

            assert_eq!(ErrorCode::CorruptMessage, error_code(&response));
            assert_eq!(Some(CRC_MISMATCH), response.error_message.as_deref(),);
            assert!(inner.served().is_empty());
        }

        #[tokio::test]
        async fn a_valid_batch_is_passed_to_the_inner_service() {
            let inner = RecordingPartitions::<deflated::Batch>::default();
            let batch = valid_batch();

            let Ok(response) = BatchValidationLayer::new()
                .layer(inner.clone())
                .serve(partition_produce(vec![batch.clone()], &[]))
                .await;

            assert_eq!(ErrorCode::None, error_code(&response));
            assert_eq!(vec![batch], inner.served());
        }
    }
}
