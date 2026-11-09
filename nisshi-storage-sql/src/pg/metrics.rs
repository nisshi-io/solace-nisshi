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

use std::{
    sync::LazyLock,
    time::{Duration, SystemTime},
};

use futures::{Stream, StreamExt as _};
use tokio::time::Instant;

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

/// Counts the records statement of a fetch in `nisshi_sql_requests`, and records the time that
/// PostgreSQL spent on it in `nisshi_sql_duration`, each with an `outcome` attribute of `completed`
/// or `stopped`.
///
/// The statement counts as `stopped` when the guard is dropped before
/// [`FetchStatementMetrics::completed`] or [`FetchStatementMetrics::failed`] runs. A caller that
/// stops a read at its deadline drops the read, so `Drop` is the only code that runs then.
///
/// The duration is database time only: the time that the statement took to start, and each wait
/// for its next row through [`FetchStatementMetrics::next_row`]. It leaves out the time that the
/// read waits for room in its channel, which the broker spends, not PostgreSQL. A stopped
/// statement's duration ends at the drop, so a read stopped while it waits for a slow row still
/// shows that wait.
pub(super) struct FetchStatementMetrics {
    sql: String,
    cluster: String,
    database_time: Duration,
    waiting_for_row_since: Option<Instant>,
    counted: bool,
}

impl FetchStatementMetrics {
    /// Returns the guard of the statement `sql` on `cluster`, which started at `started_at` and
    /// has just been accepted by the server.
    pub(super) fn started(sql: &str, cluster: &str, started_at: Instant) -> Self {
        Self {
            sql: sql.to_owned(),
            cluster: cluster.to_owned(),
            database_time: started_at.elapsed(),
            waiting_for_row_since: None,
            counted: false,
        }
    }

    /// Returns the next row of `rows`, and adds the wait for it to the statement's database time.
    pub(super) async fn next_row<S>(&mut self, rows: &mut S) -> Option<S::Item>
    where
        S: Stream + Unpin,
    {
        self.waiting_for_row_since = Some(Instant::now());
        let row = rows.next().await;
        self.end_wait_for_row();
        row
    }

    /// Counts the statement as `completed`: it sent all of its rows.
    pub(super) fn completed(mut self) {
        self.count("completed");
    }

    /// Drops the guard without counting the statement. A statement that fails counts in
    /// `nisshi_sql_error` instead, as every other statement does.
    pub(super) fn failed(mut self) {
        self.counted = true;
    }

    fn end_wait_for_row(&mut self) {
        if let Some(waiting_since) = self.waiting_for_row_since.take() {
            self.database_time += waiting_since.elapsed();
        }
    }

    fn count(&mut self, outcome: &'static str) {
        self.end_wait_for_row();

        let attributes = [
            KeyValue::new("sql", self.sql.clone()),
            KeyValue::new("cluster_id", self.cluster.clone()),
            KeyValue::new("outcome", outcome),
        ];

        SQL_DURATION.record(
            u64::try_from(self.database_time.as_millis()).unwrap_or(u64::MAX),
            &attributes,
        );
        SQL_REQUESTS.add(1, &attributes);
        self.counted = true;
    }
}

impl Drop for FetchStatementMetrics {
    fn drop(&mut self) {
        if !self.counted {
            self.count("stopped");
        }
    }
}
