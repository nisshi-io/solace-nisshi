// Copyright (c) Solace Corporation
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

use std::assert_matches;

use bytes::{BufMut as _, BytesMut};
use nisshi_sans_io::{BatchAttribute, Compression, record::Record};
use tokio::task::yield_now;

use super::*;

const FIXED_NOW: i64 = 1_800_000_000_000;

/// [`super::validate_timestamps`], reduced to the rejection that most tests look at.
fn validate_timestamps(
    batch: &mut deflated::Batch,
    timestamp_type: TimestampType,
    now: i64,
) -> Result<Option<Rejection>> {
    super::validate_timestamps(batch, timestamp_type, now).map(Result::err)
}

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

/// The batch that [`super::validate_timestamps`] returns is `batch` decoded as a backend
/// decodes a stored batch, after the timestamps are set. A backend that gets it through
/// [`Storage::produce_inflated`](crate::Storage::produce_inflated) then sees what its own
/// decode would give.
fn assert_decoded_matches(batch: &deflated::Batch, inflated: &inflated::Batch) -> Result<()> {
    assert_eq!(&inflated::Batch::try_from(batch.clone())?, inflated);

    Ok(())
}

fn validate_timestamps_ok(
    batch: &mut deflated::Batch,
    timestamp_type: TimestampType,
) -> Result<inflated::Batch> {
    match super::validate_timestamps(batch, timestamp_type, FIXED_NOW)? {
        Ok(inflated) => Ok(inflated),
        Err(rejection) => panic!("rejected: {rejection:?}"),
    }
}

#[test]
fn decoded_batch_matches_after_a_create_time_rewrite() -> Result<()> {
    let mut batch = batch(FIXED_NOW, -1, &[0, 300, 100])?;
    let before = batch.clone();

    let inflated = validate_timestamps_ok(&mut batch, TimestampType::CreateTime)?;

    assert_ne!(
        before.max_timestamp, batch.max_timestamp,
        "the header changed"
    );
    assert_decoded_matches(&batch, &inflated)
}

#[test]
fn decoded_batch_matches_when_the_header_is_unchanged() -> Result<()> {
    let mut batch = batch(FIXED_NOW, FIXED_NOW + 7, &[0, 7])?;

    let inflated = validate_timestamps_ok(&mut batch, TimestampType::CreateTime)?;

    assert_decoded_matches(&batch, &inflated)
}

#[test]
fn decoded_batch_matches_after_a_log_append_time_stamp() -> Result<()> {
    let mut batch = batch(FIXED_NOW - 5_000, FIXED_NOW - 5_000, &[0, 10])?;

    let inflated = validate_timestamps_ok(&mut batch, TimestampType::LogAppendTime)?;

    assert_eq!(FIXED_NOW, batch.max_timestamp);
    assert_eq!(FIXED_NOW, inflated.max_timestamp);
    assert_decoded_matches(&batch, &inflated)
}

#[test]
fn decoded_batch_matches_for_a_compressed_batch() -> Result<()> {
    let mut batch: deflated::Batch = inflated::Batch::builder()
        .attributes(
            BatchAttribute::default()
                .compression(Compression::Gzip)
                .into(),
        )
        .base_timestamp(FIXED_NOW)
        .max_timestamp(NO_TIMESTAMP)
        .last_offset_delta(1)
        .record(Record::builder().offset_delta(0).timestamp_delta(0))
        .record(Record::builder().offset_delta(1).timestamp_delta(9))
        .build()
        .and_then(TryInto::try_into)?;

    let inflated = validate_timestamps_ok(&mut batch, TimestampType::CreateTime)?;

    assert_eq!(
        Compression::Gzip,
        BatchAttribute::try_from(batch.attributes)?.compression,
        "the stored batch keeps its codec"
    );
    assert_eq!(
        Compression::None,
        BatchAttribute::try_from(inflated.attributes)?.compression,
        "the decoded batch holds no codec"
    );
    assert_decoded_matches(&batch, &inflated)
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

    let (batch, outcome) = rewrite.await??;

    assert_eq!(None, outcome.err());
    assert_eq!(FIXED_NOW + i64::try_from(RECORDS)? - 1, batch.max_timestamp);
    assert_eq!(batch.computed_crc(), batch.crc);

    Ok(())
}

mod service {
    use std::time::{SystemTime, UNIX_EPOCH};

    use nisshi_sans_io::TimestampType;

    use super::*;
    use crate::{
        message_timestamp_type,
        service::produce::tests::support::{RecordingPartitions, error_code, partition_produce},
    };

    type Decoded = (deflated::Batch, inflated::Batch);

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn a_batch_within_the_window_is_passed_on_with_its_decoded_records() -> Result<()> {
        let inner = RecordingPartitions::<Decoded>::default();
        let now = now_ms();

        let Ok(response) = TimestampLayer::new()
            .layer(inner.clone())
            .serve(partition_produce(batch(now, -1, &[0, 5])?, &[]))
            .await;

        assert_eq!(ErrorCode::None, error_code(&response));

        let served = inner.served();
        assert_eq!(1, served.len());

        let (deflated, inflated) = &served[0];
        assert_eq!(2, inflated.records.len());
        assert_eq!(now + 5, deflated.max_timestamp);

        Ok(())
    }

    #[tokio::test]
    async fn a_record_ahead_of_the_window_is_rejected_and_not_passed_on() -> Result<()> {
        let inner = RecordingPartitions::<Decoded>::default();

        let Ok(response) = TimestampLayer::new()
            .layer(inner.clone())
            .serve(partition_produce(
                batch(now_ms() + 2 * TIMESTAMP_AFTER_MAX_MS, -1, &[0])?,
                &[],
            ))
            .await;

        assert_eq!(ErrorCode::InvalidTimestamp, error_code(&response));
        assert!(inner.served().is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn a_log_append_time_topic_stamps_the_batch_with_the_time_of_the_append() -> Result<()> {
        let inner = RecordingPartitions::<Decoded>::default();
        let before = now_ms();

        let Ok(response) = TimestampLayer::new()
            .layer(inner.clone())
            .serve(partition_produce(
                batch(before, -1, &[0])?,
                &[(message_timestamp_type::KEY, "LogAppendTime")],
            ))
            .await;

        assert_eq!(ErrorCode::None, error_code(&response));

        let served = inner.served();
        assert_eq!(1, served.len());

        let (deflated, _) = &served[0];
        assert_eq!(
            TimestampType::LogAppendTime,
            TimestampType::from(deflated.attributes)
        );
        assert!(deflated.max_timestamp >= before);

        Ok(())
    }

    #[tokio::test]
    async fn a_batch_that_does_not_decode_is_rejected_and_not_passed_on() -> Result<()> {
        let inner = RecordingPartitions::<Decoded>::default();

        let mut undecodable = batch(now_ms(), -1, &[0])?;
        undecodable.record_data = bytes::Bytes::from_static(&[0xff, 0xff, 0xff]);

        let Ok(response) = TimestampLayer::new()
            .layer(inner.clone())
            .serve(partition_produce(undecodable, &[]))
            .await;

        assert_eq!(ErrorCode::InvalidRecord, error_code(&response));
        assert!(inner.served().is_empty());

        Ok(())
    }
}
