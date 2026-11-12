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

use nisshi_sans_io::{BatchAttribute, Compression, record::deflated};

use super::super::*;
use crate::service::produce::rejection::storage_error_code;

/// The batch is compressed: on the by-value path an uncompressed batch
/// whose `record_count` exceeds what its `record_data` could hold is
/// reported as corrupt before the decoded-size limit is checked.
#[test]
fn a_produced_batch_over_the_decoded_size_limit_is_message_too_large() {
    let oversized = || deflated::Batch {
        attributes: BatchAttribute::default()
            .compression(Compression::Gzip)
            .into(),
        record_count: 1_000_000,
        last_offset_delta: 999_999,
        ..Default::default()
    };

    for result in [
        inflate_produced(&oversized()),
        inflate_produced(oversized()),
    ] {
        let error = result.expect_err("an oversized batch must not decode");
        assert_eq!(ErrorCode::MessageTooLarge, storage_error_code(&error));
    }
}

#[test]
fn a_produced_batch_that_does_not_decode_is_an_invalid_record() {
    let truncated = || deflated::Batch {
        record_count: 1,
        last_offset_delta: 0,
        ..Default::default()
    };

    for result in [
        inflate_produced(&truncated()),
        inflate_produced(truncated()),
    ] {
        let error = result.expect_err("a batch without record data must not decode");
        assert_eq!(ErrorCode::InvalidRecord, storage_error_code(&error));
    }
}
