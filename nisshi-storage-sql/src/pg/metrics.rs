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

//! The metrics of the statements that the PostgreSQL storage engine runs.

use std::{sync::LazyLock, time::SystemTime};

use nisshi_storage::{METER, StorageOperation};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram},
};

static SQL_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("nisshi_sql_duration")
        .with_unit("ms")
        .with_description("The SQL request latencies in milliseconds")
        .build()
});

static SQL_REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_sql_requests")
        .with_description("The number of SQL requests made")
        .build()
});

static SQL_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_sql_error")
        .with_description("The SQL error count")
        .build()
});

static SQL_STATEMENTS_ABANDONED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_sql_statements_abandoned")
        .with_description(
            "The number of reads dropped while their statement may still run, each of which \
             costs a cancel request and keeps its connection until the statement has ended",
        )
        .build()
});

/// Counts the statement `sql` on `cluster`, which started at `started_at`, as completed.
pub(super) fn completed(sql: &str, cluster: &str, started_at: SystemTime) {
    count(
        started_at,
        &[
            KeyValue::new("sql", sql.to_owned()),
            KeyValue::new("cluster_id", cluster.to_owned()),
        ],
    );
}

/// Counts the statement `sql` on `cluster` as failed with `error`.
pub(super) fn failed(sql: &str, cluster: &str, error: &tokio_postgres::Error) {
    let mut attributes = vec![
        KeyValue::new("sql", sql.to_owned()),
        KeyValue::new("cluster_id", cluster.to_owned()),
    ];

    if let Some(db_error) = error.as_db_error() {
        if let Some(schema) = db_error.schema() {
            attributes.push(KeyValue::new("schema", schema.to_owned()));
        }

        if let Some(table) = db_error.table() {
            attributes.push(KeyValue::new("table", table.to_owned()));
        }

        if let Some(constraint) = db_error.constraint() {
            attributes.push(KeyValue::new("constraint", constraint.to_owned()));
        }
    }

    if let Some(code) = error.code() {
        attributes.push(KeyValue::new("code", format!("{code:?}")));
    }

    SQL_ERROR.add(1, &attributes);
}

/// Counts a failure of the pool of `cluster` to give out a connection.
pub(super) fn pool_failed(cluster: &str) {
    SQL_ERROR.add(
        1,
        &[
            KeyValue::new("cluster_id", cluster.to_owned()),
            KeyValue::new("kind", "pool"),
        ],
    );
}

/// Counts a read of `operation` on `cluster` that was dropped while its statement may still run.
pub(super) fn abandoned(cluster: &str, operation: StorageOperation) {
    SQL_STATEMENTS_ABANDONED.add(
        1,
        &[
            KeyValue::new("cluster_id", cluster.to_owned()),
            KeyValue::new("operation", operation.as_str()),
        ],
    );
}

/// Counts a row of the statement `sql` on `cluster` that failed to decode.
pub(super) fn decode_failed(sql: &str, cluster: &str) {
    SQL_ERROR.add(
        1,
        &[
            KeyValue::new("sql", sql.to_owned()),
            KeyValue::new("cluster_id", cluster.to_owned()),
            KeyValue::new("kind", "decode"),
        ],
    );
}

fn count(started_at: SystemTime, attributes: &[KeyValue]) {
    SQL_DURATION.record(
        started_at
            .elapsed()
            .map_or(0, |duration| duration.as_millis() as u64),
        attributes,
    );
    SQL_REQUESTS.add(1, attributes);
}

/// Counts the records statement of a fetch in `nisshi_sql_requests`, with an `outcome` attribute
/// of `completed` or `stopped`, and the duration of a completed statement in
/// `nisshi_sql_duration`.
///
/// The statement counts as `stopped` when the guard is dropped before
/// [`FetchStatementMetrics::completed`] or [`FetchStatementMetrics::failed`] runs. A caller that
/// stops a read at its deadline drops the read, so `Drop` is the only code that runs then.
///
/// A stopped statement records no duration, because its time ends at the drop, which would put the
/// deadline into the latency of the statements that completed. The duration of a completed
/// statement includes the time that the read waits for room in its channel.
pub(super) struct FetchStatementMetrics {
    sql: String,
    cluster: String,
    started_at: SystemTime,
    counted: bool,
}

impl FetchStatementMetrics {
    /// Returns the guard of the statement `sql` on `cluster`, which started at `started_at`.
    pub(super) fn started(sql: &str, cluster: &str, started_at: SystemTime) -> Self {
        Self {
            sql: sql.to_owned(),
            cluster: cluster.to_owned(),
            started_at,
            counted: false,
        }
    }

    /// Counts the statement as `completed`: it sent all of its rows.
    pub(super) fn completed(mut self) {
        count(self.started_at, &self.attributes("completed"));
        self.counted = true;
    }

    /// Drops the guard without counting the statement. A statement that fails counts in
    /// `nisshi_sql_error` instead, as every other statement does.
    pub(super) fn failed(mut self) {
        self.counted = true;
    }

    fn attributes(&self, outcome: &'static str) -> [KeyValue; 3] {
        [
            KeyValue::new("sql", self.sql.clone()),
            KeyValue::new("cluster_id", self.cluster.clone()),
            KeyValue::new("outcome", outcome),
        ]
    }
}

impl Drop for FetchStatementMetrics {
    fn drop(&mut self) {
        if !self.counted {
            SQL_REQUESTS.add(1, &self.attributes("stopped"));
        }
    }
}
