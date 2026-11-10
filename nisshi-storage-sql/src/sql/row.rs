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

//! The row types that more than one query returns. Each query that returns one re-exports it
//! from its own module, so an engine names the query's module, not the row's.

/// A producer's epoch.
pub(crate) mod producer_epoch {
    sql_row! {
        /// A producer's epoch.
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(crate) struct Row {
            /// The epoch.
            epoch: i16,
        }
    }
}

/// A record's offset.
pub(crate) mod record_offset {
    sql_row! {
        /// One record's offset.
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(crate) struct Row {
            /// The record's offset.
            offset_id: i64,
        }
    }
}

/// A topic of the cluster, as the queries that find topics return it.
pub(crate) mod topic {
    sql_row! {
        /// One topic of the cluster.
        #[derive(Clone, Debug, Eq, PartialEq)]
        pub(crate) struct Row {
            /// The topic's ID.
            uuid: uuid::Uuid,
            /// The topic's name.
            name: String,
            /// Whether the topic is internal.
            is_internal: bool,
            /// The topic's partition count.
            partitions: i32,
            /// The topic's replication factor.
            replication_factor: i32,
        }
    }
}

/// A topic's name, read from a result that returns a whole [`topic::Row`].
///
/// A delete reads only this, so that it can delete a topic whose other columns can't be read.
pub(crate) mod topic_name {
    sql_row! {
        /// One topic's name.
        #[derive(Clone, Debug, Eq, PartialEq)]
        pub(crate) struct Row {
            /// The topic's name.
            name: String,
        }
    }
}

/// The ID of the topic or virtual topic that an insert or an upsert wrote, from its
/// `returning` list.
pub(crate) mod topic_id {
    sql_row! {
        /// The topic that the query wrote.
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(crate) struct Row {
            /// The topic's ID.
            uuid: uuid::Uuid,
        }
    }
}

/// The ID of the row that an insert or an upsert wrote, from its `returning` list.
pub(crate) mod written_id {
    sql_row! {
        /// The row that the query wrote.
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub(crate) struct Row {
            /// The row's ID.
            id: i64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{topic, topic_name};

    /// An engine reads the topic queries both as [`topic::Row`] and as [`topic_name::Row`], and
    /// the column check of `queries!` reads each topic query only as [`topic::Row`]. That check
    /// covers [`topic_name::Row`] only while each of its columns is a column of [`topic::Row`]
    /// that reads the same kind of value.
    #[test]
    fn each_topic_name_column_is_a_topic_column() {
        for name_column in topic_name::Row::COLUMNS {
            assert!(
                topic::Row::COLUMNS.iter().any(|topic_column| {
                    topic_column.name == name_column.name && topic_column.kind == name_column.kind
                }),
                "{name_column:?} is not a column of topic::Row"
            );
        }
    }
}
