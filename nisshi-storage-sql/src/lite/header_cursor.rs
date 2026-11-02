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

use libsql::Rows;

use nisshi_storage::Result;

/// A header's key and value.
pub(super) type HeaderKeyValue = (Option<Vec<u8>>, Option<Vec<u8>>);

/// The headers of a partition's records in an offset range, read with one query in step with
/// the records that a fetch returns.
///
/// A query per record costs a statement preparation and a round trip for each record, all
/// inside the storage permit. The cursor reads the headers in offset order, as the records are.
/// It copies each header out of its row, because a libsql `Row` reads its values from the
/// statement, and the next call to `Rows::next` replaces them.
pub(super) struct HeaderCursor {
    rows: Rows,
    pending: Option<(i64, HeaderKeyValue)>,
}

impl HeaderCursor {
    pub(super) fn new(rows: Rows) -> Self {
        Self {
            rows,
            pending: None,
        }
    }

    /// Returns the headers of the record at the offset, in ordinal order. Offsets passed in
    /// successive calls must not decrease.
    pub(super) async fn take(&mut self, offset: i64) -> Result<Vec<HeaderKeyValue>> {
        let mut headers = vec![];

        loop {
            let (header_offset, header) = match self.pending.take() {
                Some(pending) => pending,
                None => match self.rows.next().await? {
                    Some(row) => (
                        row.get::<i64>(0)?,
                        (
                            row.get::<Option<Vec<u8>>>(1)?,
                            row.get::<Option<Vec<u8>>>(2)?,
                        ),
                    ),
                    None => return Ok(headers),
                },
            };

            match header_offset.cmp(&offset) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => headers.push(header),
                std::cmp::Ordering::Greater => {
                    self.pending = Some((header_offset, header));
                    return Ok(headers);
                }
            }
        }
    }
}
