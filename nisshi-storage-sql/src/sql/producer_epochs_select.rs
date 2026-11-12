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

//! The query that reads the epochs a produced batch is checked against: the producer's newest
//! epoch, its newest epoch on the batch's partition, and whether a transactional ID owns it. It
//! returns no row when the producer does not exist or has no epoch.
//!
//! The query takes four parameters: `$1` is the cluster's name, `$2` the producer ID, `$3` the
//! topic's name, and `$4` the partition's index.

use super::Query;
use nisshi_storage::StoredEpochs;

/// The query's name in the SQL cache.
pub(crate) const QUERY: Query<Row> = Query::new("producer_epochs_select.sql");

sql_row! {
    /// The epochs that a produced batch is checked against.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The producer's newest epoch.
        newest_epoch: i16,
        /// The producer's newest epoch with a sequence on the partition, or `None` when the
        /// producer has not written to the partition.
        partition_epoch: Option<i16>,
        /// 1 when a transactional ID owns the producer, and 0 otherwise.
        transactional: i32,
    }
}

impl From<Row> for StoredEpochs {
    fn from(row: Row) -> Self {
        Self {
            transactional: row.transactional != 0,
            newest: row.newest_epoch,
            partition: row.partition_epoch,
        }
    }
}
