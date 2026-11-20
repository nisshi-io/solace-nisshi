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

use nisshi_storage::{ByteLimit, FetchedDataSender, FetchedRecord, Result};

/// Sends the records of a SQL engine's fetch, and reports when a hard limit stopped the fetch
/// before its first record.
///
/// The plain fetch query leaves out a first record that does not fit. In SQL storage, every offset
/// below the bound holds a record, so if a plain fetch starts below its bound and sends no record,
/// the query left out that first record. The keyed query can match no record at all, so it returns
/// a marker row instead, and the engine passes that on with [`RecordSender::stop_at_limit`].
#[derive(Debug)]
pub(crate) struct RecordSender {
    data: FetchedDataSender,
    /// Whether the first record may exceed the limit, which holds for a soft limit.
    first_may_exceed: bool,
    /// Whether a fetch that sends no record stopped at its limit: the fetch has no key filter and
    /// starts below its bound.
    empty_fetch_reaches_limit: bool,
    sent_records: bool,
    /// Whether the query returned a marker row for a first record that did not fit.
    stopped_at_limit: bool,
}

impl RecordSender {
    /// Returns a sender, through `data`, for a fetch that reads from `offset` within `limit`, up to
    /// `bound`: the high watermark, or the last stable offset for `ReadCommitted`. `key` is the
    /// fetch's key filter, if it has one.
    pub(crate) fn new(
        data: FetchedDataSender,
        limit: ByteLimit,
        key: Option<&str>,
        offset: i64,
        bound: i64,
    ) -> Self {
        Self {
            data,
            first_may_exceed: matches!(limit, ByteLimit::Soft(_)),
            empty_fetch_reaches_limit: key.is_none() && offset < bound,
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
    /// row's record. [`RecordSender::end`] then reports it.
    pub(crate) fn stop_at_limit(&mut self) {
        self.stopped_at_limit = true;
    }

    /// Ends the fetch. If a hard limit stopped the fetch before its first record, this reports it.
    pub(crate) async fn end(mut self) -> Result<()> {
        let empty_fetch_at_limit =
            !self.first_may_exceed && self.empty_fetch_reaches_limit && !self.sent_records;

        if self.stopped_at_limit || empty_fetch_at_limit {
            self.data.send_limit_reached().await?;
        }

        Ok(())
    }
}
