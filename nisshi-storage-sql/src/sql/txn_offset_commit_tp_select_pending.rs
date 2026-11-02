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

//! The `txn_offset_commit_tp_select_pending.sql` query and the names of the columns that the
//! engines read from its result. A change to the query's `select` list must keep these names.
//!
//! The query takes two parameters: `$1` the cluster's name and `$2` the group's name. It returns
//! one row for each partition for which a transaction holds an offset of the group that it has
//! not yet committed or aborted: in its current epoch, or in an earlier epoch whose end is
//! prepared and waits behind an older open transaction. The end of a transaction deletes its
//! rows.

/// The query's name in the SQL cache.
pub(crate) const QUERY: &str = "txn_offset_commit_tp_select_pending.sql";

/// The topic's name.
pub(crate) const TOPIC: &str = "topic";

/// The partition's index.
pub(crate) const PARTITION: &str = "partition";
