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

//! The query that reads whether a transactional batch's partition is in its producer's ongoing
//! transaction: the producer ID that owns the transactional ID, and whether the partition was
//! added to the transaction at the batch's producer ID and epoch, and if so whether the
//! transaction is preparing to commit or abort. It returns no row when the
//! transactional ID does not exist.
//!
//! The query takes six parameters: `$1` is the cluster's name, `$2` the transactional ID, `$3`
//! the batch's producer ID, `$4` its producer epoch, `$5` the topic's name, and `$6` the
//! partition's index.

/// The query's name in the SQL cache.
pub(crate) const QUERY: &str = "txn_produce_partition_select.sql";

/// The column that holds the producer ID that owns the transactional ID.
pub(crate) const PRODUCER: &str = "producer";

/// The column that holds 0 when the partition is not in the transaction, 1 when it is, and 2
/// when it is and the transaction is preparing to commit or abort.
pub(crate) const ADDED: &str = "added";

/// The `added` value for a partition in a transaction that is ongoing.
pub(crate) const ADDED_IN_TRANSACTION: i32 = 1;

/// The `added` value for a partition in a transaction that is preparing to commit or abort.
pub(crate) const ADDED_IN_PREPARING_TRANSACTION: i32 = 2;
