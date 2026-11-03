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

//! Rules that Apache Kafka applies to a producer's ID and epoch, in an InitProducerId
//! request and in a produced batch, shared by every [`Storage`](crate::Storage) backend.

use std::cmp::Ordering;

use nisshi_sans_io::ErrorCode;

use crate::ProducerIdResponse;

/// The producer ID and epoch in an InitProducerId request.
///
/// Returns `Ok(None)` for a fresh producer, and `Ok(Some((id, epoch)))` when the
/// producer claims the ID and epoch it already holds, to bump its epoch after an
/// error ([KIP-360]).
///
/// v0-2 have no ProducerId or ProducerEpoch fields, so they decode as `None`. Kafka
/// gives an absent field its default of -1, so `(None, None)` is a fresh request.
///
/// A request with only one of the two set to -1 is an
/// [`InvalidRequest`](ErrorCode::InvalidRequest), as in Kafka 3.9.1
/// ([KafkaApis.scala]).
///
/// [KIP-360]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-360%3A+Improve+reliability+of+idempotent%2Ftransactional+producer
/// [KafkaApis.scala]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L2343-L2347
pub fn producer_claim(
    producer_id: Option<i64>,
    producer_epoch: Option<i16>,
) -> Result<Option<(i64, i16)>, ErrorCode> {
    match (producer_id, producer_epoch) {
        (None, None) | (Some(-1), Some(-1)) => Ok(None),
        (Some(id), Some(epoch)) if id != -1 && epoch != -1 => Ok(Some((id, epoch))),
        _ => Err(ErrorCode::InvalidRequest),
    }
}

/// Checks a producer's claim against the producer ID and epoch stored for its
/// transactional ID: [`None`](ErrorCode::None) on an exact match, otherwise
/// [`ProducerFenced`](ErrorCode::ProducerFenced).
///
/// Kafka 3.9.1 also accepts a claim of the previous epoch, from a producer retrying
/// a bump whose response it never received, and answers with the current epoch
/// ([TransactionMetadata.scala]). The backends do not store the previous epoch, so
/// this check fences that claim, and the producer initialises again. We do not take
/// the previous epoch to be the current one minus one, because after a fresh
/// InitProducerId from another producer that guess lets the fenced producer take
/// over.
///
/// After an exhausted epoch moves the transactional ID to a new producer ID (see
/// [`epoch_is_exhausted`]), this check also fences a claim of the previous producer
/// ID. Kafka accepts that claim from a producer retrying the request that moved it
/// ([TransactionMetadata.scala#isValidProducerId]), because it keeps the previous
/// producer ID and epoch in the transaction's metadata. This check compares the claim
/// with the current producer ID and epoch only, and the producer initialises again.
///
/// [TransactionMetadata.scala]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/coordinator/transaction/TransactionMetadata.scala#L294-L299
/// [TransactionMetadata.scala#isValidProducerId]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/coordinator/transaction/TransactionMetadata.scala#L212-L224
pub fn check_claim(stored: (i64, i16), claim: (i64, i16)) -> ErrorCode {
    if stored == claim {
        ErrorCode::None
    } else {
        ErrorCode::ProducerFenced
    }
}

/// Whether a producer has exhausted its epoch: an InitProducerId can no longer bump
/// it, and the transactional ID moves to a new producer ID at epoch 0 instead.
///
/// The limit is `i16::MAX - 1`, as in Kafka 3.9.1 ([TransactionMetadata.scala]),
/// so that `i16::MAX` stays free to fence the previous producer ID.
///
/// [TransactionMetadata.scala]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/coordinator/transaction/TransactionMetadata.scala#L180
pub fn epoch_is_exhausted(epoch: i16) -> bool {
    epoch >= i16::MAX - 1
}

/// The error that answers an idempotent batch whose epoch is older than the epoch its partition
/// holds for the producer, as Kafka answers.
pub const OLDER_EPOCH_ERROR: ErrorCode = ErrorCode::InvalidProducerEpoch;

/// The epochs stored for a producer, which a produced batch's epoch is checked against.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoredEpochs {
    /// Whether the producer is transactional: the request has a transactional ID, the batch
    /// is transactional, or a transactional ID owns the producer ID.
    pub transactional: bool,

    /// The producer's newest epoch.
    pub newest: i16,

    /// The producer's newest epoch that holds a sequence on the batch's partition, or `None`
    /// when the producer has not written to the partition.
    pub partition: Option<i16>,
}

/// How a produced batch's epoch relates to the epochs stored for its producer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProduceEpoch {
    /// The batch continues the epoch that the partition holds for the producer, or the
    /// transactional producer's newest epoch.
    Continue,

    /// The batch starts its epoch on the partition, at sequence 0. The backend stores the
    /// epoch for the producer when the producer does not have it yet.
    Start,
}

impl StoredEpochs {
    /// Checks a produced batch's epoch and first sequence against these epochs.
    ///
    /// An idempotent producer bumps its own epoch after an error ([KIP-360]): it keeps its
    /// producer ID, adds one to its epoch, and restarts its sequence at 0. Kafka 3.9.1 keeps
    /// the epoch for each partition. It accepts any epoch higher than the partition's whose
    /// first sequence is 0, and answers
    /// [`OutOfOrderSequenceNumber`](ErrorCode::OutOfOrderSequenceNumber) for any other
    /// sequence ([ProducerAppendInfo.java]). We compare with the partition's epoch too,
    /// because the Java client retries a batch at its old epoch on one partition after it
    /// bumps the epoch for another ([RecordAccumulator.java]).
    ///
    /// A transactional producer's epoch moves only through InitProducerId, so its batch must
    /// be at the producer's newest epoch. [`transactional`](Self::transactional) includes a
    /// producer ID that a transactional ID owns, so that a non-transactional batch cannot
    /// move a transactional producer's epoch.
    ///
    /// Returns [`OLDER_EPOCH_ERROR`] for an epoch lower than the partition's, as Kafka answers
    /// for a client's batch ([ProducerAppendInfo.java]).
    ///
    /// Returns [`InvalidProducerEpoch`](ErrorCode::InvalidProducerEpoch) for:
    /// - a transactional producer's epoch other than its newest. Kafka's coordinator answers
    ///   PRODUCER_FENCED for that epoch, and the broker answers the Produce request with
    ///   INVALID_PRODUCER_EPOCH instead ([AddPartitionsToTxnManager.scala]).
    /// - an epoch that [`epoch_is_exhausted`]. Kafka's partition state accepts it, and we
    ///   refuse it because `i16::MAX` stays free to fence a retired producer ID. The Java
    ///   client bumps its epoch after this error, and at `i16::MAX` it asks for a new
    ///   producer ID ([TransactionManager.java]).
    ///
    /// Returns [`ProducerFenced`](ErrorCode::ProducerFenced) for a batch from a producer ID
    /// whose newest epoch is `i16::MAX`, because that epoch retires the producer ID.
    ///
    /// The Java client treats PRODUCER_FENCED as fatal. It answers INVALID_PRODUCER_EPOCH on
    /// an idempotent producer by bumping its epoch, so these errors let it recover.
    ///
    /// [KIP-360]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-360%3A+Improve+reliability+of+idempotent%2Ftransactional+producer
    /// [ProducerAppendInfo.java]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerAppendInfo.java#L96-L143
    /// [AddPartitionsToTxnManager.scala]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/AddPartitionsToTxnManager.scala#L225-L228
    /// [TransactionManager.java]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/producer/internals/TransactionManager.java#L535-L542
    /// [RecordAccumulator.java]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/producer/internals/RecordAccumulator.java#L920-L935
    pub fn check_batch(
        &self,
        producer_epoch: i16,
        base_sequence: i32,
    ) -> Result<ProduceEpoch, ErrorCode> {
        // Only a zombie producer sends from a retired producer ID, so the broker answers
        // PRODUCER_FENCED, which the client treats as fatal. INVALID_PRODUCER_EPOCH makes
        // the client bump its epoch and send again.
        if self.newest == i16::MAX {
            return Err(ErrorCode::ProducerFenced);
        }

        if self.transactional {
            return if producer_epoch == self.newest {
                Ok(ProduceEpoch::Continue)
            } else {
                Err(ErrorCode::InvalidProducerEpoch)
            };
        }

        match self
            .partition
            .map(|partition| producer_epoch.cmp(&partition))
        {
            Some(Ordering::Equal) => Ok(ProduceEpoch::Continue),
            Some(Ordering::Less) => Err(OLDER_EPOCH_ERROR),
            Some(Ordering::Greater) | None if epoch_is_exhausted(producer_epoch) => {
                Err(ErrorCode::InvalidProducerEpoch)
            }
            Some(Ordering::Greater) | None if base_sequence != 0 => {
                Err(ErrorCode::OutOfOrderSequenceNumber)
            }
            Some(Ordering::Greater) | None => Ok(ProduceEpoch::Start),
        }
    }

    /// Whether the partition has moved an idempotent producer on from a batch's epoch: the
    /// producer is not transactional, its producer ID is not retired, and the partition holds
    /// a newer epoch for it.
    pub fn is_superseded(&self, producer_epoch: i16) -> bool {
        !self.transactional
            && self.newest != i16::MAX
            && self
                .partition
                .is_some_and(|partition| partition > producer_epoch)
    }
}

impl ProducerIdResponse {
    /// A failed InitProducerId, with no producer ID or epoch, as Kafka answers.
    pub fn failed(error: ErrorCode) -> Self {
        Self {
            error,
            id: -1,
            epoch: -1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh() {
        assert_eq!(Ok(None), producer_claim(None, None));
        assert_eq!(Ok(None), producer_claim(Some(-1), Some(-1)));
    }

    #[test]
    fn claim() {
        assert_eq!(Ok(Some((3, 0))), producer_claim(Some(3), Some(0)));
        assert_eq!(Ok(Some((3, 7))), producer_claim(Some(3), Some(7)));
    }

    #[test]
    fn mixed_is_invalid() {
        for (id, epoch) in [
            (Some(-1), Some(0)),
            (Some(3), Some(-1)),
            (Some(-1), None),
            (None, Some(-1)),
            (Some(3), None),
            (None, Some(0)),
        ] {
            assert_eq!(
                Err(ErrorCode::InvalidRequest),
                producer_claim(id, epoch),
                "{id:?}, {epoch:?}"
            );
        }
    }

    #[test]
    fn check() {
        assert_eq!(ErrorCode::None, check_claim((3, 2), (3, 2)));
        assert_eq!(ErrorCode::ProducerFenced, check_claim((3, 2), (3, 1)));
        assert_eq!(ErrorCode::ProducerFenced, check_claim((3, 2), (3, 3)));
        assert_eq!(ErrorCode::ProducerFenced, check_claim((3, 2), (4, 2)));
    }

    #[test]
    fn exhausted() {
        assert!(!epoch_is_exhausted(0));
        assert!(!epoch_is_exhausted(32765));
        assert!(epoch_is_exhausted(32766));
        assert!(epoch_is_exhausted(i16::MAX));
    }

    const IDEMPOTENT: StoredEpochs = StoredEpochs {
        transactional: false,
        newest: 5,
        partition: Some(3),
    };

    const TRANSACTIONAL: StoredEpochs = StoredEpochs {
        transactional: true,
        newest: 3,
        partition: Some(3),
    };

    #[test]
    fn idempotent_continues_partition_epoch() {
        assert_eq!(Ok(ProduceEpoch::Continue), IDEMPOTENT.check_batch(3, 7));
    }

    #[test]
    fn idempotent_below_partition_epoch_is_invalid() {
        assert_eq!(Err(OLDER_EPOCH_ERROR), IDEMPOTENT.check_batch(2, 0));
    }

    #[test]
    fn idempotent_below_partition_epoch_is_superseded() {
        assert!(IDEMPOTENT.is_superseded(2));
        assert!(!IDEMPOTENT.is_superseded(3));
        assert!(!IDEMPOTENT.is_superseded(4));
    }

    #[test]
    fn unwritten_partition_supersedes_nothing() {
        let unwritten = StoredEpochs {
            partition: None,
            ..IDEMPOTENT
        };

        assert!(!unwritten.is_superseded(0));
    }

    #[test]
    fn transactional_mismatch_is_not_superseded() {
        assert!(!TRANSACTIONAL.is_superseded(2));
        assert!(!TRANSACTIONAL.is_superseded(4));
    }

    #[test]
    fn retired_producer_id_supersedes_nothing() {
        let retired = StoredEpochs {
            newest: i16::MAX,
            ..IDEMPOTENT
        };

        assert!(!retired.is_superseded(2));
    }

    #[test]
    fn idempotent_bump_at_sequence_zero_starts_epoch() {
        assert_eq!(Ok(ProduceEpoch::Start), IDEMPOTENT.check_batch(4, 0));
        assert_eq!(Ok(ProduceEpoch::Start), IDEMPOTENT.check_batch(6, 0));
        assert_eq!(Ok(ProduceEpoch::Start), IDEMPOTENT.check_batch(32765, 0));
    }

    #[test]
    fn idempotent_bump_at_other_sequence_is_out_of_order() {
        assert_eq!(
            Err(ErrorCode::OutOfOrderSequenceNumber),
            IDEMPOTENT.check_batch(4, 1)
        );
    }

    #[test]
    fn idempotent_bump_into_exhausted_epoch_is_invalid() {
        assert_eq!(
            Err(ErrorCode::InvalidProducerEpoch),
            IDEMPOTENT.check_batch(32766, 0)
        );
        assert_eq!(
            Err(ErrorCode::InvalidProducerEpoch),
            IDEMPOTENT.check_batch(i16::MAX, 0)
        );
    }

    #[test]
    fn idempotent_first_batch_on_partition_starts_its_epoch() {
        let unwritten = StoredEpochs {
            partition: None,
            ..IDEMPOTENT
        };

        assert_eq!(Ok(ProduceEpoch::Start), unwritten.check_batch(5, 0));
        assert_eq!(Ok(ProduceEpoch::Start), unwritten.check_batch(4, 0));
        assert_eq!(Ok(ProduceEpoch::Start), unwritten.check_batch(6, 0));
        assert_eq!(
            Err(ErrorCode::OutOfOrderSequenceNumber),
            unwritten.check_batch(5, 1)
        );
    }

    #[test]
    fn retired_producer_id_is_fenced() {
        let retired = StoredEpochs {
            newest: i16::MAX,
            partition: Some(32766),
            ..TRANSACTIONAL
        };

        assert_eq!(
            Err(ErrorCode::ProducerFenced),
            retired.check_batch(32766, 1)
        );
        assert_eq!(
            Err(ErrorCode::ProducerFenced),
            retired.check_batch(i16::MAX, 0)
        );
    }

    #[test]
    fn transactional_continues_newest_epoch_only() {
        assert_eq!(Ok(ProduceEpoch::Continue), TRANSACTIONAL.check_batch(3, 7));
        assert_eq!(
            Err(ErrorCode::InvalidProducerEpoch),
            TRANSACTIONAL.check_batch(2, 0)
        );
        assert_eq!(
            Err(ErrorCode::InvalidProducerEpoch),
            TRANSACTIONAL.check_batch(4, 0)
        );

        let unwritten = StoredEpochs {
            partition: None,
            ..TRANSACTIONAL
        };
        assert_eq!(Ok(ProduceEpoch::Continue), unwritten.check_batch(3, 0));
        assert_eq!(
            Err(ErrorCode::InvalidProducerEpoch),
            unwritten.check_batch(4, 0)
        );
    }
}
