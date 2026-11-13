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

use std::{
    convert::Infallible,
    fmt::{self, Debug},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use nisshi_sans_io::{
    BatchAttribute, Compression, ErrorCode, TimestampType,
    produce_response::{BatchIndexAndErrorMessage, PartitionProduceResponse},
    record::{deflated, inflated},
};
use rama::{Layer, Service};
use tracing::{error, instrument, warn};

use super::{
    admission::{BUDGET_SPENT, decoded_upper_bound},
    batch_validation::corrupt_batch,
    deadline::{Deadline, Stage},
    inflate_produced,
    partition::PartitionProduce,
    rejection::{Rejection, undecodable_batch},
    response,
};
use crate::{Result, decoded_budget::Exhausted, offload};

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
/// Returns the decoded batch, so that the caller does not decode it again, or the [`Rejection`]
/// for a batch that the client must not store. The decoded batch matches `batch` after the
/// timestamps are set, as [`Storage::produce_inflated`](crate::Storage::produce_inflated) requires, so that the storage backend
/// does not decode the batch again. Returns `Err` for a broker-side failure.
fn validate_timestamps(
    batch: &mut deflated::Batch,
    timestamp_type: TimestampType,
    now: i64,
) -> Result<Result<inflated::Batch, Rejection>> {
    // The decode takes a clone, which shares the record bytes with `batch`, so that it clears the
    // compression bits of the attributes as a backend's own decode of a stored batch does.
    let mut inflated = match inflate_produced(batch.clone()) {
        Ok(inflated) => inflated,
        Err(error) => return Ok(Err(undecodable_batch(&error))),
    };

    if let Some(rejection) = producer_log_append_time(batch, timestamp_type) {
        return Ok(Err(rejection));
    }

    let rejection = match timestamp_type {
        TimestampType::CreateTime => rewrite_create_time(batch, &inflated, now)?,
        TimestampType::LogAppendTime => stamp_log_append_time(batch, now)?,
    };

    if let Some(rejection) = rejection {
        return Ok(Err(rejection));
    }

    // The rewrite changed these header fields of `batch`, and only these. The decoded records are
    // the same.
    inflated.attributes = BatchAttribute::try_from(batch.attributes)?
        .compression(Compression::None)
        .into();
    inflated.base_timestamp = batch.base_timestamp;
    inflated.max_timestamp = batch.max_timestamp;
    inflated.crc = batch.crc;

    // A header rewrite that this copy does not know leaves the stored header and the decoded
    // batch apart, and a backend that stores the decoded batch's header then stores the wrong one.
    debug_assert!(
        headers_agree(batch, &inflated),
        "the decoded batch's header differs from the stored batch's header: {batch:?} and {inflated:?}"
    );

    Ok(Ok(inflated))
}

/// Returns whether `inflated` has every header field of `deflated`, apart from the compression
/// bits of the attributes, which a decode clears, and the record bytes.
fn headers_agree(deflated: &deflated::Batch, inflated: &inflated::Batch) -> bool {
    let without_compression = |attributes: i16| {
        BatchAttribute::try_from(attributes)
            .map(|attributes| i16::from(attributes.compression(Compression::None)))
    };

    deflated.base_offset == inflated.base_offset
        && deflated.batch_length == inflated.batch_length
        && deflated.partition_leader_epoch == inflated.partition_leader_epoch
        && deflated.magic == inflated.magic
        && deflated.crc == inflated.crc
        && without_compression(deflated.attributes).ok()
            == without_compression(inflated.attributes).ok()
        && deflated.last_offset_delta == inflated.last_offset_delta
        && deflated.base_timestamp == inflated.base_timestamp
        && deflated.max_timestamp == inflated.max_timestamp
        && deflated.producer_id == inflated.producer_id
        && deflated.producer_epoch == inflated.producer_epoch
        && deflated.base_sequence == inflated.base_sequence
        && usize::try_from(deflated.record_count).is_ok_and(|count| count == inflated.records.len())
}

/// Runs [`validate_timestamps`] on Tokio's blocking thread pool, because it decodes the batch, and
/// hands the batch back with the outcome.
pub(super) async fn validate_timestamps_offloaded(
    batch: deflated::Batch,
    timestamp_type: TimestampType,
    now: i64,
) -> Result<(deflated::Batch, Result<inflated::Batch, Rejection>)> {
    // The batch moves into the closure and back out, because `offload` needs a `'static` closure. A
    // clone would only add a handle to the same record bytes, so moving the batch keeps one owner.
    offload(move || {
        let mut batch = batch;
        let outcome = validate_timestamps(&mut batch, timestamp_type, now)?;
        Ok((batch, outcome))
    })
    .await
}

/// A [`Layer`] that validates the timestamps of a batch against the topic's
/// `message.timestamp.type`, and gives its inner [`Service`] the batch with its timestamps set,
/// and the same batch decoded.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TimestampLayer;

impl TimestampLayer {
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for TimestampLayer {
    type Service = TimestampService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TimestampService { inner }
    }
}

/// A [`Service`] passing a batch to `inner` after the timestamp validation has set its timestamps
/// for the topic's `message.timestamp.type`, with the batch decoded, or answering the partition
/// with the error code for a batch that the client must not store.
///
/// The service reserves the bytes of the decoded batch from the request's [`Admission`] before it
/// decodes the batch, and holds them until `inner` returns. It answers `REQUEST_TIMED_OUT` when
/// the budget stays spent, and `UNKNOWN_SERVER_ERROR` for a broker-side failure.
///
/// [`Admission`]: super::Admission
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TimestampService<S> {
    inner: S,
}

impl<S> Debug for TimestampService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(TimestampService)).finish()
    }
}

impl<S> Service<PartitionProduce<deflated::Batch>> for TimestampService<S>
where
    S: Service<
            PartitionProduce<(deflated::Batch, inflated::Batch)>,
            Output = PartitionProduceResponse,
            Error = Infallible,
        >,
{
    type Output = PartitionProduceResponse;
    type Error = Infallible;

    #[instrument(skip_all)]
    async fn serve(
        &self,
        request: PartitionProduce<deflated::Batch>,
    ) -> Result<Self::Output, Self::Error> {
        let (request, batch) = request.take_batch();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_default();

        // The reservation is held until the inner service returns, so that the decoded batch is
        // counted while storage holds it, and dropped on every other path out of this function. A
        // partition that waits here holds only its compressed batch.
        //
        // The wait ends at the request's deadline if that comes first. Nothing was written, so
        // the partition answers as one that was not started.
        let compression = Compression::try_from(batch.attributes).unwrap_or_default();
        let reserved = request
            .deadline
            .bound(request.admission.reserve(decoded_upper_bound(&batch)))
            .await;

        let mut reservation = match reserved {
            Err(_) => {
                return Ok(Deadline::timed_out(Stage::NotStarted, request.index));
            }
            Ok(Ok(reservation)) => reservation,
            Ok(Err(Exhausted { requested, waited })) => {
                warn!(
                    topic = request.topic,
                    partition = request.index,
                    requested,
                    ?waited,
                    "decoded-batch budget stayed spent, answering REQUEST_TIMED_OUT"
                );

                return Ok(
                    response::partition_error(request.index, ErrorCode::RequestTimedOut)
                        .error_message(Some(BUDGET_SPENT.into())),
                );
            }
        };

        match validate_timestamps_offloaded(batch, request.configuration.timestamp_type(), now)
            .await
        {
            Ok((batch, Ok(inflated))) => {
                // The decoded size is known now. A compressed batch is charged for its inflated
                // record bytes as well as its `Record` structures, which is what the decoder
                // charges, and these bytes are what a few KB of compressed input turns into.
                reservation.shrink_to(
                    inflated
                        .decoded_size(&compression)
                        .unwrap_or_else(|_| reservation.bytes()),
                );

                // A write that the deadline leaves running keeps the bytes until it ends, so the
                // reservation is shared with it.
                let reservation = Arc::new(reservation);

                self.inner
                    .serve(
                        request
                            .with_reservation(Arc::clone(&reservation))
                            .with_batch((batch, inflated)),
                    )
                    .await
            }

            Ok((batch, Err(rejection))) => {
                rejection.log(&request.topic, request.index, &batch);
                Ok(rejection.into_response(request.index))
            }

            Err(err) => {
                error!(?err);
                Ok(response::partition_error(
                    request.index,
                    ErrorCode::UnknownServerError,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests;
