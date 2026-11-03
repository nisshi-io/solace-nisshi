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

//! The `consumer_offset_select.sql` query and the names of the columns that the engines read from
//! its result. A change to the query's `select` list must keep these names.
//!
//! The query takes four parameters: `$1` the cluster's name, `$2` the group's name, `$3` the
//! topic's name and `$4` the partition's index. It returns at most one row: the offset that the
//! group committed for the partition.

/// The query's name in the SQL cache.
pub(crate) const QUERY: &str = "consumer_offset_select.sql";

/// The committed offset.
pub(crate) const COMMITTED_OFFSET: &str = "committed_offset";

/// The leader epoch of the commit. It is null when the commit did not give one.
pub(crate) const LEADER_EPOCH: &str = "leader_epoch";

/// The metadata of the commit. It is null when the commit did not give any.
pub(crate) const METADATA: &str = "metadata";
