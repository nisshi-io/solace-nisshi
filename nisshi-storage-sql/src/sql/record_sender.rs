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

use nisshi_storage::{ByteLimit, FetchedDataSender, FetchedRecord, ReadEnd, Result};

/// Sends the records of a SQL engine's fetch, and finds out whether a [`ByteLimit::Hard`] limit
/// stopped the fetch before its first record. If it did, [`RecordSender::end`] returns
/// [`ReadEnd::ReachedLimit`].
///
/// Under a hard limit, the query leaves out a first record that does not fit the limit and does not
/// return a row for it, so the sender cannot see that record:
///
/// - If the fetch has no key filter, every offset from the fetch offset up to, and not including,
///   the high watermark (the last stable offset, for `ReadCommitted`) holds a record. So if that
///   range is not empty and the query does not return a row, the first record did not fit.
/// - If the fetch has a key filter, the query also returns no rows if no record in the range has
///   that key. So the query with a key filter returns a marker row for a first record that does
///   not fit, and the engine calls [`RecordSender::stop_at_limit`] when it reads that row.
#[derive(Debug)]
pub(crate) struct RecordSender {
    data: FetchedDataSender,
    /// Whether the first record may exceed the limit, which holds for a soft limit.
    first_may_exceed: bool,
    /// Whether a fetch that sends no record under a hard limit stopped at that limit: the fetch has
    /// no key filter and starts below the high watermark, or the last stable offset for
    /// `ReadCommitted`.
    empty_fetch_reaches_limit: bool,
    sent_records: bool,
    /// Whether the query returned a marker row for a first record that did not fit.
    stopped_at_limit: bool,
}

impl RecordSender {
    /// Returns a sender, through `data`, for a fetch that reads from `offset` within `limit`, up to
    /// and not including `end_offset`: the high watermark, or the last stable offset for
    /// `ReadCommitted`. `key` is the fetch's key filter, if it has one.
    pub(crate) fn new(
        data: FetchedDataSender,
        limit: ByteLimit,
        key: Option<&str>,
        offset: i64,
        end_offset: i64,
    ) -> Self {
        Self {
            data,
            first_may_exceed: matches!(limit, ByteLimit::Soft(_)),
            empty_fetch_reaches_limit: key.is_none() && offset < end_offset,
            sent_records: false,
            stopped_at_limit: false,
        }
    }

    /// Returns whether the first record may exceed the limit. The fetch query takes this as its
    /// last parameter.
    pub(crate) fn first_may_exceed(&self) -> bool {
        self.first_may_exceed
    }

    /// Sends one record, which follows the record sent before it in offset order.
    pub(crate) async fn send_record(&mut self, record: FetchedRecord) -> Result<()> {
        self.data.send_record(record).await?;
        self.sent_records = true;
        Ok(())
    }

    /// Notes that the query returned a marker row: the fetch stopped at its hard limit before the
    /// row's record. After this call, [`RecordSender::end`] returns [`ReadEnd::ReachedLimit`].
    pub(crate) fn stop_at_limit(&mut self) {
        self.stopped_at_limit = true;
    }

    /// Ends the fetch, and returns [`ReadEnd::ReachedLimit`] if a hard limit stopped the fetch
    /// before its first record, or [`ReadEnd::Finished`] if not.
    pub(crate) fn end(self) -> ReadEnd {
        let empty_fetch_at_limit =
            !self.first_may_exceed && self.empty_fetch_reaches_limit && !self.sent_records;

        if self.stopped_at_limit || empty_fetch_at_limit {
            ReadEnd::ReachedLimit
        } else {
            ReadEnd::Finished
        }
    }
}
