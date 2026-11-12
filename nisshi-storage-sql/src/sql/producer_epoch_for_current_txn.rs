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

//! The query that reads the producer that owns a transactional ID, the newest of its epochs that
//! has a transaction, and that transaction's status. It returns no row when the transactional ID
//! does not exist, or when no epoch of its producer has a transaction.
//!
//! The query takes two parameters: `$1` is the cluster's name, and `$2` the transactional ID.

use super::Query;

/// The query's name in the SQL cache.
pub(crate) const QUERY: Query<Row> = Query::new("producer_epoch_for_current_txn.sql");

sql_row! {
    /// The transactional ID's producer, epoch and transaction status.
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct Row {
        /// The producer ID.
        producer: i64,
        /// The producer's newest epoch with a transaction.
        epoch: i16,
        /// The transaction's status, or `None` when the transaction has no status.
        status: Option<String>,
    }
}
