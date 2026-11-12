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

//! The batches that an idempotent producer most recently wrote to a partition, which tell a
//! client's retry of a stored batch from a new batch.

use std::collections::VecDeque;

use nisshi_sans_io::ErrorCode;
use nisshi_storage::{Error, Result};
use serde::{Deserialize, Serialize};

/// The most batches that [`ProducedBatches`] remembers for one partition: Kafka's
/// `ProducerStateEntry.NUM_BATCHES_TO_RETAIN`. An idempotent client keeps at most this many
/// batches in flight to a partition, so a retry after a lost connection repeats one of them.
const MAX_BATCHES: usize = 5;

/// A batch that an idempotent producer wrote to a partition.
///
/// It is stored as a `[first_sequence, last_sequence, base_offset]` array, not an object with
/// named fields, because every idempotent produce rewrites all of meta.json, and meta.json holds
/// up to [`MAX_BATCHES`] of these for each producer and partition.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(from = "(i32, i32, i64)", into = "(i32, i32, i64)")]
struct ProducedBatch {
    first_sequence: i32,
    last_sequence: i32,
    base_offset: i64,
}

impl From<(i32, i32, i64)> for ProducedBatch {
    fn from((first_sequence, last_sequence, base_offset): (i32, i32, i64)) -> Self {
        Self {
            first_sequence,
            last_sequence,
            base_offset,
        }
    }
}

impl From<ProducedBatch> for (i32, i32, i64) {
    fn from(batch: ProducedBatch) -> Self {
        (batch.first_sequence, batch.last_sequence, batch.base_offset)
    }
}

/// The last [`MAX_BATCHES`] batches that an idempotent producer wrote to a partition at one
/// epoch, oldest first.
#[derive(Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub(super) struct ProducedBatches(VecDeque<ProducedBatch>);

/// What a produce does with an idempotent batch that [`check_sequence`] accepts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SequenceCheck {
    /// The batch starts at the partition's next sequence: store it.
    Append,
    /// The batch repeats a remembered batch: answer the stored batch's base offset and store
    /// nothing.
    Duplicate { base_offset: i64 },
}

impl SequenceCheck {
    /// Returns the name of this outcome in the `outcome` attribute of a metric.
    pub(super) fn outcome(&self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Duplicate { .. } => "duplicate",
        }
    }
}

/// Returns the name, in the `outcome` attribute of a metric, of an error that [`check_sequence`]
/// returns, or `None` for any other error.
pub(super) fn rejected_outcome(error: &Error) -> Option<&'static str> {
    match error {
        Error::Api(ErrorCode::OutOfOrderSequenceNumber) => Some("out_of_order"),
        Error::Api(ErrorCode::DuplicateSequenceNumber) => {
            Some("duplicate_without_remembered_batches")
        }
        _ => None,
    }
}

impl ProducedBatches {
    /// Remembers a batch that was just stored, and forgets the oldest batch beyond
    /// [`MAX_BATCHES`].
    pub(super) fn remember(
        &mut self,
        base_sequence: i32,
        last_offset_delta: i32,
        base_offset: i64,
    ) {
        let Some(last_sequence) = base_sequence.checked_add(last_offset_delta) else {
            return;
        };

        if self.0.len() == MAX_BATCHES {
            _ = self.0.pop_front();
        }

        self.0.push_back(ProducedBatch {
            first_sequence: base_sequence,
            last_sequence,
            base_offset,
        });
    }

    /// Returns the base offset of the remembered batch with this first and last sequence.
    fn base_offset_of(&self, first_sequence: i32, last_sequence: i32) -> Option<i64> {
        self.0
            .iter()
            .find(|batch| {
                batch.first_sequence == first_sequence && batch.last_sequence == last_sequence
            })
            .map(|batch| batch.base_offset)
    }
}

/// Returns the sequence after a batch: the batch's first sequence plus its record count, which
/// wraps to 0 past `i32::MAX` as Kafka's [`DefaultRecordBatch.incrementSequence`] does.
///
/// `base_sequence` and `last_offset_delta` are not negative.
///
/// [`DefaultRecordBatch.incrementSequence`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L556-L560
pub(super) fn next_sequence(base_sequence: i32, last_offset_delta: i32) -> i32 {
    increment_sequence(increment_sequence(base_sequence, last_offset_delta), 1)
}

fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    if sequence > i32::MAX - increment {
        increment - (i32::MAX - sequence) - 1
    } else {
        sequence + increment
    }
}

/// Checks an idempotent batch's sequence against the partition's next sequence and its
/// remembered batches, as Kafka's `ProducerAppendInfo.checkSequence` and
/// `ProducerStateEntry.findDuplicateBatch` do.
///
/// A batch that repeats a remembered batch, with the same first and last sequence, is a
/// duplicate. Returns [`Error::Api`] with [`ErrorCode::OutOfOrderSequenceNumber`] for any other
/// batch that does not start at the next sequence. A client counts a duplicate as delivered, so
/// a new batch answered as a duplicate would be acknowledged and never stored.
///
/// `batches` is `None` for a partition whose state was written before the broker remembered
/// batches. A lower sequence there gets [`ErrorCode::DuplicateSequenceNumber`] instead of
/// `OUT_OF_ORDER_SEQUENCE_NUMBER`, because the upgrade restarts the broker, and the client
/// retries the batches that were in flight. An idempotent client treats
/// `OUT_OF_ORDER_SEQUENCE_NUMBER` on its oldest batch in flight as a fatal error.
pub(super) fn check_sequence(
    next_sequence: i32,
    batches: Option<&ProducedBatches>,
    base_sequence: i32,
    last_offset_delta: i32,
) -> Result<SequenceCheck> {
    if base_sequence == next_sequence {
        return Ok(SequenceCheck::Append);
    }

    match batches {
        Some(batches) => base_sequence
            .checked_add(last_offset_delta)
            .and_then(|last_sequence| batches.base_offset_of(base_sequence, last_sequence))
            .map(|base_offset| SequenceCheck::Duplicate { base_offset })
            .ok_or(Error::Api(ErrorCode::OutOfOrderSequenceNumber)),

        None if base_sequence < next_sequence => {
            Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
        }

        None => Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber)),
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;

    fn batches(stored: &[(i32, i32, i64)]) -> ProducedBatches {
        let mut batches = ProducedBatches::default();

        for &(base_sequence, last_offset_delta, base_offset) in stored {
            batches.remember(base_sequence, last_offset_delta, base_offset);
        }

        batches
    }

    #[test]
    fn sequence_after_a_batch_wraps_to_0_past_the_largest_sequence() {
        assert_eq!(8, next_sequence(5, 2));
        assert_eq!(i32::MAX, next_sequence(i32::MAX - 1, 0));
        assert_eq!(0, next_sequence(i32::MAX, 0));
        assert_eq!(0, next_sequence(i32::MAX - 1, 1));
        assert_eq!(2, next_sequence(i32::MAX - 3, 5));
    }

    #[test]
    fn next_sequence_is_appended() {
        assert_matches!(
            check_sequence(5, Some(&batches(&[(0, 4, 0)])), 5, 2),
            Ok(SequenceCheck::Append)
        );
        assert_matches!(check_sequence(0, None, 0, 0), Ok(SequenceCheck::Append));
    }

    #[test]
    fn repeat_of_a_remembered_batch_is_a_duplicate_at_its_offset() {
        let stored = batches(&[(0, 2, 10), (3, 1, 20)]);

        assert_matches!(
            check_sequence(5, Some(&stored), 0, 2),
            Ok(SequenceCheck::Duplicate { base_offset: 10 })
        );
        assert_matches!(
            check_sequence(5, Some(&stored), 3, 1),
            Ok(SequenceCheck::Duplicate { base_offset: 20 })
        );
    }

    #[test]
    fn same_first_sequence_with_another_last_sequence_is_out_of_order() {
        assert_matches!(
            check_sequence(3, Some(&batches(&[(0, 2, 0)])), 0, 1),
            Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
        );
    }

    #[test]
    fn lower_sequence_of_no_remembered_batch_is_out_of_order() {
        assert_matches!(
            check_sequence(8, Some(&batches(&[(0, 3, 0), (4, 3, 4)])), 2, 0),
            Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
        );
    }

    #[test]
    fn sequence_past_the_next_is_out_of_order() {
        assert_matches!(
            check_sequence(3, Some(&batches(&[(0, 2, 0)])), 4, 0),
            Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
        );
        assert_matches!(
            check_sequence(3, None, 4, 0),
            Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
        );
    }

    #[test]
    fn lower_sequence_without_remembered_batches_is_a_duplicate() {
        assert_matches!(
            check_sequence(3, None, 0, 2),
            Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
        );
    }

    #[test]
    fn only_the_last_five_batches_are_remembered() {
        let stored = batches(&[
            (0, 0, 0),
            (1, 0, 1),
            (2, 0, 2),
            (3, 0, 3),
            (4, 0, 4),
            (5, 0, 5),
        ]);

        assert_matches!(
            check_sequence(6, Some(&stored), 0, 0),
            Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
        );
        assert_matches!(
            check_sequence(6, Some(&stored), 1, 0),
            Ok(SequenceCheck::Duplicate { base_offset: 1 })
        );
    }

    #[test]
    fn remembered_batches_are_stored_as_arrays() -> serde_json::Result<()> {
        let stored = batches(&[(0, 2, 7), (3, 1, 10)]);
        let json = serde_json::to_string(&stored)?;

        assert_eq!("[[0,2,7],[3,4,10]]", json);
        assert_eq!(stored, serde_json::from_str::<ProducedBatches>(&json)?);

        Ok(())
    }
}
