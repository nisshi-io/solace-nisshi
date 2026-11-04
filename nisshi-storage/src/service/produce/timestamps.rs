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

use nisshi_sans_io::{
    ErrorCode, TimestampType,
    produce_response::BatchIndexAndErrorMessage,
    record::{deflated, inflated},
};

use super::{
    batch_validation::corrupt_batch,
    inflate_produced,
    rejection::{Rejection, undecodable_batch},
};
use crate::{Result, offload};

/// Kafka's wire sentinel for "no timestamp", also the fold-start value
/// `LogValidator` uses for a batch's running maximum.
pub(super) const NO_TIMESTAMP: i64 = -1;

/// How far ahead of the broker's clock a `CreateTime` record may claim to be.
///
/// This is the `log.message.timestamp.after.max.ms` default of Kafka 4.0
/// ([KIP-1030], [`ServerLogConfigs` 4.0.0]). It differs from Kafka 3.9.1,
/// whose protocol this broker follows: 3.9.1 defaults to `Long.MAX_VALUE`
/// ([`ServerLogConfigs` 3.9.1]) and accepts any future timestamp. A record
/// far in the future keeps its batch's `max_timestamp` ahead of every
/// retention deadline, so the broker uses the 4.0 limit.
///
/// [KIP-1030]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-1030%3A+Change+constraints+and+default+values+for+various+configurations
/// [`ServerLogConfigs` 4.0.0]: https://github.com/apache/kafka/blob/4.0.0/server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java#L121
/// [`ServerLogConfigs` 3.9.1]: https://github.com/apache/kafka/blob/3.9.1/server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java#L148
const TIMESTAMP_AFTER_MAX_MS: i64 = 3_600_000;

/// Kafka's own `log.message.timestamp.before.max.ms` default
/// (`Long.MAX_VALUE`): no lower bound.
const TIMESTAMP_BEFORE_MAX_MS: i64 = i64::MAX;

/// Validates the timestamp of each record in a `CreateTime` batch, and
/// rewrites the batch's `max_timestamp` to the largest record timestamp
/// when the header claims a different value.
///
/// Kafka's `LogValidator` does the same: it starts from [`NO_TIMESTAMP`],
/// takes the maximum record timestamp, and overwrites only the header. It
/// exempts a record timestamped [`NO_TIMESTAMP`] from the bounds check.
///
/// `inflated` is `batch` decoded.
///
/// Returns `Ok(Some(_))` for a batch that the client must not store: a record timestamp outside the
/// window that [`TIMESTAMP_BEFORE_MAX_MS`] and [`TIMESTAMP_AFTER_MAX_MS`] set around `now`, a
/// timestamp that overflows, or a header rewrite on a batch whose CRC does not match its contents.
/// `Err` is a broker-side failure.
fn rewrite_create_time(
    batch: &mut deflated::Batch,
    inflated: &inflated::Batch,
    now: i64,
) -> Result<Option<Rejection>> {
    let lower = now.saturating_sub(TIMESTAMP_BEFORE_MAX_MS);
    let upper = now.saturating_add(TIMESTAMP_AFTER_MAX_MS);

    let mut actual_max = NO_TIMESTAMP;

    for (batch_index, record) in inflated.records.iter().enumerate() {
        let batch_index = i32::try_from(batch_index)?;

        let Some(timestamp) = batch.base_timestamp.checked_add(record.timestamp_delta) else {
            return Ok(Some(Rejection {
                record_error: Some(
                    BatchIndexAndErrorMessage::default()
                        .batch_index(batch_index)
                        .batch_index_error_message(Some(format!(
                            "base_timestamp {} plus timestamp_delta {} of the record at batch \
                             index {batch_index} overflows",
                            batch.base_timestamp, record.timestamp_delta
                        ))),
                ),
                ..Rejection::new(ErrorCode::InvalidRecord, "record timestamp overflows i64")
            }));
        };

        // Kafka's check is strict, so a timestamp exactly at a bound is
        // accepted.
        if timestamp != NO_TIMESTAMP && (timestamp > upper || timestamp < lower) {
            return Ok(Some(Rejection {
                record_error: Some(
                    BatchIndexAndErrorMessage::default()
                        .batch_index(batch_index)
                        .batch_index_error_message(Some(format!(
                            "Timestamp {timestamp} of the record at batch index {batch_index} \
                             is out of range. The timestamp should be within [{lower}, \
                             {upper}], from the broker's clock at {now}"
                        ))),
                ),
                ..Rejection::new(ErrorCode::InvalidTimestamp, INVALID_TIMESTAMP)
            }));
        }

        actual_max = actual_max.max(timestamp);
    }

    // A batch that the producer marks `LogAppendTime` is rewritten as `CreateTime`, even when its
    // `max_timestamp` is correct. The topic's `message.timestamp.type` decides the type, not the
    // batch's own bit, as in Kafka. Otherwise a client could skip the timestamp window by setting
    // the bit.
    //
    // For such a batch, Kafka checks the header's `max_timestamp` against the window instead of
    // each record's timestamp, because it reads `max_timestamp` as the timestamp of every record
    // of a `LogAppendTime` batch ([`DefaultRecordBatch.java#L578`]). It then clears the bit, so a
    // consumer reads each record's own timestamp, which Kafka did not check. This function checks
    // each record's own timestamp instead. That is stricter when a record is outside the window
    // and the header is inside it. It is looser in the opposite case: the function then stores
    // the batch with `max_timestamp` rewritten to the records' maximum, so every stored timestamp
    // has been checked. A producer that Kafka accepts does not set the bit, so the stricter check
    // does not reject it.
    //
    // [`DefaultRecordBatch.java#L578`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecordBatch.java#L578
    if actual_max == batch.max_timestamp
        && TimestampType::from(batch.attributes) == TimestampType::CreateTime
    {
        return Ok(None);
    }

    set_timestamps(
        batch,
        TimestampType::CreateTime,
        batch.base_timestamp,
        actual_max,
    )
}

/// Stamps `batch` with the broker's time, `now`, for a `LogAppendTime` topic.
///
/// The function sets the batch's timestamp type to `LogAppendTime` and its `max_timestamp` to
/// `now`. A consumer reads `max_timestamp` as the timestamp of each record of a `LogAppendTime`
/// batch. The function keeps `base_timestamp`, as Kafka's [`LogValidator`] does.
///
/// Returns `Ok(Some(_))` for a batch whose CRC does not match its contents.
///
/// [`LogValidator`]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogValidator.java#L278-L282
fn stamp_log_append_time(batch: &mut deflated::Batch, now: i64) -> Result<Option<Rejection>> {
    set_timestamps(
        batch,
        TimestampType::LogAppendTime,
        batch.base_timestamp,
        now,
    )
}

/// Sets the timestamp type and the timestamps of `batch` with [`deflated::Batch::set_timestamps`].
/// Returns [`corrupt_batch`] for a batch whose CRC does not match its contents.
fn set_timestamps(
    batch: &mut deflated::Batch,
    timestamp_type: TimestampType,
    base_timestamp: i64,
    max_timestamp: i64,
) -> Result<Option<Rejection>> {
    match batch.set_timestamps(timestamp_type, base_timestamp, max_timestamp) {
        Ok(()) => Ok(None),
        Err(nisshi_sans_io::Error::ApiError(ErrorCode::CorruptMessage)) => {
            Ok(Some(corrupt_batch()))
        }
        Err(error) => Err(error.into()),
    }
}

/// The top-level message that Kafka sends with `INVALID_TIMESTAMP` ([`LogValidator.java#L624-L631`]).
///
/// [`LogValidator.java#L624-L631`]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogValidator.java#L624-L631
const INVALID_TIMESTAMP: &str = "One or more records have been rejected due to invalid timestamp";

/// Returns the rejection for a batch on which the producer set the `LogAppendTime` bit where Kafka
/// refuses that bit, or `None` where Kafka accepts it.
///
/// Kafka refuses the bit on a `LogAppendTime` topic, and on a `CreateTime` topic when the batch's
/// `max_timestamp` is [`NO_TIMESTAMP`] ([`LogValidator.java#L583-L602`]). Kafka reads
/// `max_timestamp` as the timestamp of each record of a marked batch, and it exempts a
/// [`NO_TIMESTAMP`] record from the window, so the second case is a marked batch that skips the
/// window. On a `CreateTime` topic, Kafka accepts any other marked batch, checks it against the
/// window and clears the bit, as [`rewrite_create_time`] does.
///
/// Kafka answers `INVALID_TIMESTAMP` with one record error for each record of the batch, because
/// it checks each record. This function returns one record error, for the record at batch index 0,
/// as every other [`Rejection`] does.
///
/// [`LogValidator.java#L583-L602`]: https://github.com/apache/kafka/blob/3.9.1/storage/src/main/java/org/apache/kafka/storage/internals/log/LogValidator.java#L583-L602
fn producer_log_append_time(
    batch: &deflated::Batch,
    timestamp_type: TimestampType,
) -> Option<Rejection> {
    let refused = TimestampType::from(batch.attributes) == TimestampType::LogAppendTime
        && (timestamp_type == TimestampType::LogAppendTime || batch.max_timestamp == NO_TIMESTAMP);

    refused.then(|| Rejection {
        record_error: Some(
            BatchIndexAndErrorMessage::default()
                .batch_index(0)
                .batch_index_error_message(Some(
                    "Invalid timestamp type in the record at batch index 0. Producer should not \
                     set timestamp type to LogAppendTime."
                        .into(),
                )),
        ),
        ..Rejection::new(ErrorCode::InvalidTimestamp, INVALID_TIMESTAMP)
    })
}

/// Validates a client batch for a topic whose `message.timestamp.type` is `timestamp_type`, and
/// sets its timestamps for that type: [`rewrite_create_time`] for `CreateTime`, and
/// [`stamp_log_append_time`] for `LogAppendTime`.
///
/// The function decodes the batch for both types, so a batch that does not decode is rejected on
/// every topic, with the error code that [`undecodable_batch`] gives it.
///
/// Returns `Ok(Some(_))` for a batch that the client must not store, and `Err` for a broker-side
/// failure.
fn validate_timestamps(
    batch: &mut deflated::Batch,
    timestamp_type: TimestampType,
    now: i64,
) -> Result<Option<Rejection>> {
    let inflated = match inflate_produced(&*batch) {
        Ok(inflated) => inflated,
        Err(error) => return Ok(Some(undecodable_batch(&error))),
    };

    if let Some(rejection) = producer_log_append_time(batch, timestamp_type) {
        return Ok(Some(rejection));
    }

    match timestamp_type {
        TimestampType::CreateTime => rewrite_create_time(batch, &inflated, now),
        TimestampType::LogAppendTime => stamp_log_append_time(batch, now),
    }
}

/// Runs [`validate_timestamps`] on Tokio's blocking thread pool, because it decodes the batch, and
/// hands the batch back with the outcome.
pub(super) async fn validate_timestamps_offloaded(
    batch: deflated::Batch,
    timestamp_type: TimestampType,
    now: i64,
) -> Result<(deflated::Batch, Option<Rejection>)> {
    // The batch moves into the closure and back out, because `offload` needs a `'static` closure. A
    // clone would only add a handle to the same record bytes, so moving the batch keeps one owner.
    offload(move || {
        let mut batch = batch;
        let rejection = validate_timestamps(&mut batch, timestamp_type, now)?;
        Ok((batch, rejection))
    })
    .await
}

#[cfg(test)]
mod validate_timestamps_tests {
    use std::assert_matches;

    use bytes::{BufMut as _, BytesMut};
    use nisshi_sans_io::{BatchAttribute, Compression, record::Record};
    use tokio::task::yield_now;

    use super::*;

    const FIXED_NOW: i64 = 1_800_000_000_000;

    fn batch(base_timestamp: i64, max_timestamp: i64, deltas: &[i64]) -> Result<deflated::Batch> {
        let builder = deltas.iter().enumerate().try_fold(
            inflated::Batch::builder()
                .base_timestamp(base_timestamp)
                .max_timestamp(max_timestamp)
                .last_offset_delta(i32::try_from(deltas.len())? - 1),
            |builder, (offset_delta, timestamp_delta)| {
                i32::try_from(offset_delta).map(|offset_delta| {
                    builder.record(
                        Record::builder()
                            .offset_delta(offset_delta)
                            .timestamp_delta(*timestamp_delta),
                    )
                })
            },
        )?;

        builder
            .build()
            .and_then(deflated::Batch::try_from)
            .map_err(Into::into)
    }

    fn record_error_index(rejection: &Rejection) -> Option<i32> {
        rejection
            .record_error
            .as_ref()
            .map(|record_error| record_error.batch_index)
    }

    #[test]
    fn max_timestamp_is_the_largest_record_not_the_last() -> Result<()> {
        let mut batch = batch(FIXED_NOW, -1, &[0, 300, 100])?;

        assert_eq!(
            None,
            validate_timestamps(&mut batch, TimestampType::CreateTime, FIXED_NOW)?
        );
        assert_eq!(FIXED_NOW + 300, batch.max_timestamp);
        assert_eq!(batch.computed_crc(), batch.crc);

        Ok(())
    }

    #[test]
    fn negative_delta_folds_into_the_maximum() -> Result<()> {
        let mut batch = batch(FIXED_NOW, FIXED_NOW, &[-5_000, -1_000])?;

        assert_eq!(
            None,
            validate_timestamps(&mut batch, TimestampType::CreateTime, FIXED_NOW)?
        );
        assert_eq!(FIXED_NOW - 1_000, batch.max_timestamp);

        Ok(())
    }

    #[test]
    fn a_header_that_already_matches_is_unchanged() -> Result<()> {
        let mut batch = batch(FIXED_NOW, FIXED_NOW + 7, &[0, 7])?;
        let original = batch.clone();

        assert_eq!(
            None,
            validate_timestamps(&mut batch, TimestampType::CreateTime, FIXED_NOW)?
        );
        assert_eq!(original, batch);

        Ok(())
    }

    #[test]
    fn exactly_the_upper_bound_is_accepted() -> Result<()> {
        let mut batch = batch(FIXED_NOW, -1, &[TIMESTAMP_AFTER_MAX_MS])?;

        assert_eq!(
            None,
            validate_timestamps(&mut batch, TimestampType::CreateTime, FIXED_NOW)?
        );
        assert_eq!(FIXED_NOW + TIMESTAMP_AFTER_MAX_MS, batch.max_timestamp);

        Ok(())
    }

    #[test]
    fn one_past_the_upper_bound_is_rejected() -> Result<()> {
        let mut batch = batch(FIXED_NOW, -1, &[TIMESTAMP_AFTER_MAX_MS + 1])?;

        let rejection = validate_timestamps(&mut batch, TimestampType::CreateTime, FIXED_NOW)?
            .expect("one millisecond past the upper bound must be rejected");

        assert_eq!(ErrorCode::InvalidTimestamp, rejection.error_code);
        assert_eq!(Some(0), record_error_index(&rejection));

        Ok(())
    }

    #[test]
    fn a_future_record_after_a_sane_one_is_rejected_with_its_index() -> Result<()> {
        let mut batch = batch(FIXED_NOW, FIXED_NOW, &[0, 2 * TIMESTAMP_AFTER_MAX_MS, 0])?;
        let original = batch.clone();

        let rejection = validate_timestamps(&mut batch, TimestampType::CreateTime, FIXED_NOW)?
            .expect("a record two hours ahead must be rejected");

        assert_eq!(ErrorCode::InvalidTimestamp, rejection.error_code);
        assert_eq!(Some(1), record_error_index(&rejection));

        let message = rejection
            .record_error
            .and_then(|record_error| record_error.batch_index_error_message)
            .unwrap_or_default();
        assert!(message.contains(&(FIXED_NOW + 2 * TIMESTAMP_AFTER_MAX_MS).to_string()));
        assert!(message.contains(&(FIXED_NOW + TIMESTAMP_AFTER_MAX_MS).to_string()));
        assert!(message.contains(&FIXED_NOW.to_string()));

        assert_eq!(original, batch);

        Ok(())
    }

    #[test]
    fn a_timestamp_that_overflows_is_rejected() -> Result<()> {
        let mut batch = batch(i64::MAX - 1, -1, &[10])?;

        let rejection = validate_timestamps(&mut batch, TimestampType::CreateTime, FIXED_NOW)?
            .expect("an overflowing timestamp must be rejected");

        assert_eq!(ErrorCode::InvalidRecord, rejection.error_code);
        assert_eq!(Some(0), record_error_index(&rejection));

        Ok(())
    }

    #[test]
    fn no_timestamp_is_exempt_from_the_bounds_check() -> Result<()> {
        let mut only_no_timestamp = batch(NO_TIMESTAMP, FIXED_NOW, &[0])?;

        assert_eq!(
            None,
            validate_timestamps(&mut only_no_timestamp, TimestampType::CreateTime, FIXED_NOW)?
        );
        assert_eq!(NO_TIMESTAMP, only_no_timestamp.max_timestamp);

        let mut mixed = batch(NO_TIMESTAMP, NO_TIMESTAMP, &[0, FIXED_NOW + 1])?;

        assert_eq!(
            None,
            validate_timestamps(&mut mixed, TimestampType::CreateTime, FIXED_NOW)?
        );
        assert_eq!(FIXED_NOW, mixed.max_timestamp);

        Ok(())
    }

    #[test]
    fn a_batch_that_does_not_decode_is_an_invalid_record() -> Result<()> {
        let mut batch = batch(FIXED_NOW, -1, &[0])?;
        batch.record_data = bytes::Bytes::from_static(&[0xff, 0xff, 0xff]);

        let rejection = validate_timestamps(&mut batch, TimestampType::CreateTime, FIXED_NOW)?
            .expect("a batch that does not decode must be rejected");

        assert_eq!(ErrorCode::InvalidRecord, rejection.error_code);

        Ok(())
    }

    #[test]
    fn a_batch_over_the_decoded_size_limit_is_message_too_large() -> Result<()> {
        let mut batch = batch(FIXED_NOW, -1, &[0])?;
        batch.attributes = BatchAttribute::default()
            .compression(Compression::Snappy)
            .into();

        // A raw snappy block starts with its uncompressed length as a
        // varint. This one claims 2,000,000,000 bytes.
        let mut claimed = 2_000_000_000u64;
        let mut record_data = BytesMut::new();
        while claimed >= 0x80 {
            record_data.put_u8((claimed as u8) | 0x80);
            claimed >>= 7;
        }
        record_data.put_u8(claimed as u8);
        batch.record_data = record_data.freeze();

        let rejection = validate_timestamps(&mut batch, TimestampType::CreateTime, FIXED_NOW)?
            .expect("a batch over the decoded size limit must be rejected");

        assert_eq!(ErrorCode::MessageTooLarge, rejection.error_code);

        Ok(())
    }

    #[test]
    fn a_rewrite_of_a_batch_with_a_bad_crc_is_a_corrupt_message() -> Result<()> {
        let mut batch = batch(FIXED_NOW, -1, &[0])?;
        batch.crc ^= 1;
        let damaged = batch.clone();

        assert_eq!(
            Some(corrupt_batch()),
            validate_timestamps(&mut batch, TimestampType::CreateTime, FIXED_NOW)?
        );
        assert_eq!(damaged, batch);

        Ok(())
    }

    #[test]
    fn log_append_time_stamps_the_broker_time_outside_the_window() -> Result<()> {
        let producer_base_timestamp = FIXED_NOW + 10 * TIMESTAMP_AFTER_MAX_MS;
        let mut batch = batch(producer_base_timestamp, -1, &[0, 7])?;

        assert_eq!(
            None,
            validate_timestamps(&mut batch, TimestampType::LogAppendTime, FIXED_NOW)?
        );
        assert_eq!(
            TimestampType::LogAppendTime,
            TimestampType::from(batch.attributes)
        );
        assert_eq!(producer_base_timestamp, batch.base_timestamp);
        assert_eq!(FIXED_NOW, batch.max_timestamp);
        assert_eq!(batch.computed_crc(), batch.crc);

        Ok(())
    }

    fn marked(base_timestamp: i64, max_timestamp: i64, deltas: &[i64]) -> Result<deflated::Batch> {
        let mut batch = batch(base_timestamp, max_timestamp, deltas)?;
        batch.attributes = BatchAttribute::default()
            .timestamp(TimestampType::LogAppendTime)
            .into();
        batch.crc = batch.computed_crc();
        Ok(batch)
    }

    fn assert_refused_log_append_time(rejection: Option<Rejection>) {
        let rejection = rejection.expect("a refused LogAppendTime bit must be rejected");

        assert_eq!(ErrorCode::InvalidTimestamp, rejection.error_code);
        assert_eq!(INVALID_TIMESTAMP, rejection.message);
        assert_eq!(Some(0), record_error_index(&rejection));
        assert_matches!(
            rejection
                .record_error
                .and_then(|record_error| record_error.batch_index_error_message),
            Some(message) if message.ends_with(
                "Producer should not set timestamp type to LogAppendTime."
            )
        );
    }

    #[test]
    fn log_append_time_topic_rejects_a_batch_marked_log_append_time() -> Result<()> {
        let mut batch = marked(FIXED_NOW, FIXED_NOW, &[0])?;
        let original = batch.clone();

        assert_refused_log_append_time(validate_timestamps(
            &mut batch,
            TimestampType::LogAppendTime,
            FIXED_NOW,
        )?);
        assert_eq!(original, batch);

        Ok(())
    }

    #[test]
    fn create_time_topic_rejects_a_marked_batch_without_a_timestamp() -> Result<()> {
        let mut batch = marked(FIXED_NOW, NO_TIMESTAMP, &[0])?;
        let original = batch.clone();

        assert_refused_log_append_time(validate_timestamps(
            &mut batch,
            TimestampType::CreateTime,
            FIXED_NOW,
        )?);
        assert_eq!(original, batch);

        Ok(())
    }

    #[test]
    fn create_time_topic_accepts_a_marked_batch_and_clears_the_bit() -> Result<()> {
        let mut batch = marked(FIXED_NOW, FIXED_NOW + 7, &[0, 7])?;

        assert_eq!(
            None,
            validate_timestamps(&mut batch, TimestampType::CreateTime, FIXED_NOW)?
        );
        assert_eq!(
            TimestampType::CreateTime,
            TimestampType::from(batch.attributes)
        );
        assert_eq!(FIXED_NOW + 7, batch.max_timestamp);
        assert_eq!(batch.computed_crc(), batch.crc);

        Ok(())
    }

    /// A marked batch that does not decode gets the decode error, because Kafka decodes each
    /// record before it checks the record's timestamp type.
    #[test]
    fn a_marked_batch_that_does_not_decode_is_an_invalid_record() -> Result<()> {
        let mut batch = marked(FIXED_NOW, FIXED_NOW, &[0])?;
        batch.record_data = bytes::Bytes::from_static(&[0xff, 0xff, 0xff]);

        let rejection = validate_timestamps(&mut batch, TimestampType::LogAppendTime, FIXED_NOW)?
            .expect("a batch that does not decode must be rejected");

        assert_eq!(ErrorCode::InvalidRecord, rejection.error_code);

        Ok(())
    }

    #[test]
    fn log_append_time_rejects_a_batch_that_does_not_decode() -> Result<()> {
        let mut batch = batch(FIXED_NOW, -1, &[0])?;
        batch.record_data = bytes::Bytes::from_static(&[0xff, 0xff, 0xff]);

        let rejection = validate_timestamps(&mut batch, TimestampType::LogAppendTime, FIXED_NOW)?
            .expect("a batch that does not decode must be rejected");

        assert_eq!(ErrorCode::InvalidRecord, rejection.error_code);

        Ok(())
    }

    #[test]
    fn log_append_time_rejects_a_batch_with_a_bad_crc() -> Result<()> {
        let mut batch = batch(FIXED_NOW, -1, &[0])?;
        batch.crc ^= 1;
        let damaged = batch.clone();

        assert_eq!(
            Some(corrupt_batch()),
            validate_timestamps(&mut batch, TimestampType::LogAppendTime, FIXED_NOW)?
        );
        assert_eq!(damaged, batch);

        Ok(())
    }

    // The test runtime has a single thread. If the rewrite decoded the batch
    // inline, the spawned task would decode, rewrite and return within its
    // first poll, so it would already be finished when `yield_now` returns.
    // Offloaded, it is still waiting on the decode.
    #[tokio::test]
    async fn rewrite_runs_off_the_runtime_thread() -> Result<()> {
        const RECORDS: usize = 1_000;
        const RECORD_SIZE: usize = 1_024;

        // Pseudo-random values, so gzip has real work to do on decode.
        let mut seed = 0x2545_f491_u32;
        let mut builder = inflated::Batch::builder()
            .attributes(
                BatchAttribute::default()
                    .compression(Compression::Gzip)
                    .into(),
            )
            .base_timestamp(FIXED_NOW)
            .max_timestamp(NO_TIMESTAMP)
            .last_offset_delta(i32::try_from(RECORDS)? - 1);

        for offset_delta in 0..RECORDS {
            let value = (0..RECORD_SIZE)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 17;
                    seed ^= seed << 5;
                    seed as u8
                })
                .collect::<bytes::Bytes>();

            builder = builder.record(
                Record::builder()
                    .value(value.into())
                    .offset_delta(i32::try_from(offset_delta)?)
                    .timestamp_delta(i64::try_from(offset_delta)?),
            );
        }

        let batch: deflated::Batch = builder.build().and_then(TryInto::try_into)?;

        let rewrite = tokio::spawn(validate_timestamps_offloaded(
            batch,
            TimestampType::CreateTime,
            FIXED_NOW,
        ));

        yield_now().await;

        assert!(
            !rewrite.is_finished(),
            "the batch was decoded on the runtime thread"
        );

        let (batch, rejection) = rewrite.await??;

        assert_eq!(None, rejection);
        assert_eq!(FIXED_NOW + i64::try_from(RECORDS)? - 1, batch.max_timestamp);
        assert_eq!(batch.computed_crc(), batch.crc);

        Ok(())
    }
}
