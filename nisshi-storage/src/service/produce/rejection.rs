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

use std::borrow::Cow;

use nisshi_sans_io::{
    ErrorCode,
    produce_response::{BatchIndexAndErrorMessage, PartitionProduceResponse},
    record::deflated,
};
use tracing::warn;

use super::response;
use crate::Error;

/// A batch that the broker rejects before it writes anything, with what the
/// client and the operator need to find the cause.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Rejection {
    pub(super) error_code: ErrorCode,
    pub(super) message: Cow<'static, str>,
    pub(super) record_error: Option<BatchIndexAndErrorMessage>,
}

impl Rejection {
    pub(super) fn new(error_code: ErrorCode, message: impl Into<Cow<'static, str>>) -> Self {
        Self {
            error_code,
            message: message.into(),
            record_error: None,
        }
    }

    /// Logs the rejection of `batch`, sent for `partition` of `topic`, once at warn.
    pub(super) fn log(&self, topic: &str, partition: i32, batch: &deflated::Batch) {
        warn!(
            topic,
            partition,
            producer_id = batch.producer_id,
            attributes = batch.attributes,
            record_count = batch.record_count,
            last_offset_delta = batch.last_offset_delta,
            base_timestamp = batch.base_timestamp,
            max_timestamp = batch.max_timestamp,
            error_code = ?self.error_code,
            reason = %self.message,
            record_error = self
                .record_error
                .as_ref()
                .and_then(|record_error| record_error.batch_index_error_message.as_deref()),
            "rejecting produce batch",
        );
    }

    /// Returns the response that tells the client why the batch for the partition at `index`
    /// was rejected.
    pub(super) fn into_response(self, index: i32) -> PartitionProduceResponse {
        response::partition_error(index, self.error_code)
            .error_message(Some(self.message.into_owned()))
            .record_errors(Some(self.record_error.into_iter().collect()))
    }
}

impl From<(ErrorCode, &'static str)> for Rejection {
    fn from((error_code, message): (ErrorCode, &'static str)) -> Self {
        Self::new(error_code, message)
    }
}

/// The error code for a failed `storage.produce()`.
///
/// A batch over the decoded-size limit is reported as `MESSAGE_TOO_LARGE`:
/// Kafka's code for "this batch is too large for the broker", which a
/// producer can recover from on its own (the Java producer splits a batch of
/// more than one record and retries). `UNKNOWN_SERVER_ERROR` is reserved for
/// failures the client did not cause.
pub(super) fn storage_error_code(error: &Error) -> ErrorCode {
    match error {
        Error::Api(error_code) => *error_code,
        Error::SansIo(nisshi_sans_io::Error::MessageMaxSizeExceeded(_))
        | Error::MessageTooLarge { .. } => ErrorCode::MessageTooLarge,
        _ => ErrorCode::UnknownServerError,
    }
}

/// The rejection for a client batch that [`inflate_produced`](super::inflate_produced) did not decode,
/// with the error code [`storage_error_code`] gives `error`: `MESSAGE_TOO_LARGE`
/// for a batch over the decoded-size limit, which a producer recovers from by
/// splitting the batch, and `INVALID_RECORD` for any other decode failure.
pub(super) fn undecodable_batch(error: &Error) -> Rejection {
    let error_code = storage_error_code(error);

    let message = if error_code == ErrorCode::MessageTooLarge {
        "batch exceeds the maximum decoded batch size"
    } else {
        "batch could not be decoded"
    };

    Rejection::new(error_code, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decoded_size_limit_from_storage_is_message_too_large() {
        assert_eq!(
            ErrorCode::MessageTooLarge,
            storage_error_code(&Error::SansIo(
                nisshi_sans_io::Error::MessageMaxSizeExceeded(1)
            ))
        );
    }

    #[test]
    fn a_batch_over_max_message_bytes_from_storage_is_message_too_large() {
        assert_eq!(
            ErrorCode::MessageTooLarge,
            storage_error_code(&Error::MessageTooLarge {
                size: 20_061,
                limit: 10_000,
            })
        );
    }

    #[test]
    fn api_errors_from_storage_pass_through() {
        assert_eq!(
            ErrorCode::UnknownTopicOrPartition,
            storage_error_code(&Error::Api(ErrorCode::UnknownTopicOrPartition))
        );
    }

    #[test]
    fn other_storage_errors_are_unknown_server_error() {
        assert_eq!(
            ErrorCode::UnknownServerError,
            storage_error_code(&Error::SansIo(nisshi_sans_io::Error::Overflow))
        );
    }
}
