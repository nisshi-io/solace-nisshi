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

pub(crate) mod consumer_group_select;
pub(crate) mod consumer_offset_select;
pub(crate) mod consumer_offset_select_by_group;
pub(crate) mod producer_epochs_select;
#[cfg(any(feature = "libsql", feature = "turso"))]
pub(crate) mod result_columns;
pub(crate) mod topic_configuration_select;
pub(crate) mod topic_configuration_select_keys;
pub(crate) mod topic_select;
pub(crate) mod txn_offset_commit_tp_select_pending;
pub(crate) mod txn_produce_partition_select;
pub(crate) mod watermark_select_for_write;

use nisshi_sans_io::{ErrorCode, record::deflated};
use nisshi_storage::{Error, ProduceEpoch, Result, StoredEpochs};
use std::{
    cmp::Ordering,
    hash::{DefaultHasher, Hash, Hasher as _},
};
use tracing::debug;
use uuid::Uuid;

use std::{collections::BTreeMap, ops::Deref, sync::LazyLock};

pub(crate) struct Cache(pub BTreeMap<&'static str, String>);

impl Deref for Cache {
    type Target = BTreeMap<&'static str, String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Cache {
    pub(crate) fn new(inner: BTreeMap<&'static str, String>) -> Self {
        Self(inner)
    }

    pub(crate) fn get(&self, key: &str) -> Result<&str> {
        self.0
            .get(key)
            .map(|s| s.as_str())
            .ok_or(Error::UnknownCacheKey(key.to_owned()))
    }
}

macro_rules! include_sql {
    ($e: expr) => {
        remove_comments(include_str!($e))
    };
}

pub(crate) static SQL: LazyLock<Cache> = LazyLock::new(|| {
    let mapping = [
        (
            "maintain-vacuum.sql",
            include_sql!("sql/maintain-vacuum.sql"),
        ),
        (
            "consumer_group_delete.sql",
            include_sql!("sql/consumer_group_delete.sql"),
        ),
        (
            "consumer_group_detail_delete_by_cg.sql",
            include_sql!("sql/consumer_group_detail_delete_by_cg.sql"),
        ),
        (
            "consumer_group_detail_insert.sql",
            include_sql!("sql/consumer_group_detail_insert.sql"),
        ),
        (
            "consumer_group_detail.sql",
            include_sql!("sql/consumer_group_detail.sql"),
        ),
        (
            "consumer_group_insert.sql",
            include_sql!("sql/consumer_group_insert.sql"),
        ),
        (
            "consumer_group_select_by_name.sql",
            include_sql!("sql/consumer_group_select_by_name.sql"),
        ),
        (
            consumer_group_select::QUERY,
            include_sql!("sql/consumer_group_select.sql"),
        ),
        (
            "consumer_offset_delete_by_cg.sql",
            include_sql!("sql/consumer_offset_delete_by_cg.sql"),
        ),
        (
            "consumer_offset_delete_by_topic.sql",
            include_sql!("sql/consumer_offset_delete_by_topic.sql"),
        ),
        (
            "consumer_offset_insert_from_txn.sql",
            include_sql!("sql/consumer_offset_insert_from_txn.sql"),
        ),
        (
            "consumer_offset_insert.sql",
            include_sql!("sql/consumer_offset_insert.sql"),
        ),
        (
            "consumer_offset_select_by_group.sql",
            include_sql!("sql/consumer_offset_select_by_group.sql"),
        ),
        (
            "consumer_offset_select.sql",
            include_sql!("sql/consumer_offset_select.sql"),
        ),
        ("header_copy.sql", include_sql!("sql/header_copy.sql")),
        (
            "header_delete_by_topic.sql",
            include_sql!("sql/header_delete_by_topic.sql"),
        ),
        ("header_fetch.sql", include_sql!("sql/header_fetch.sql")),
        (
            "header_fetch_range.sql",
            include_sql!("sql/header_fetch_range.sql"),
        ),
        ("header_insert.sql", include_sql!("sql/header_insert.sql")),
        (
            "list_earliest_offset.sql",
            include_sql!("sql/list_earliest_offset.sql"),
        ),
        (
            "list_latest_offset_committed.sql",
            include_sql!("sql/list_latest_offset_committed.sql"),
        ),
        (
            "list_latest_offset_timestamp.sql",
            include_sql!("sql/list_latest_offset_timestamp.sql"),
        ),
        (
            "list_latest_offset_uncommitted.sql",
            include_sql!("sql/list_latest_offset_uncommitted.sql"),
        ),
        (
            "lite/policy_compact_compaction.sql",
            include_sql!("lite/policy_compact_compaction.sql"),
        ),
        (
            "lite/policy_compact_delete.sql",
            include_sql!("lite/policy_compact_delete.sql"),
        ),
        (
            "lite/policy_compact_distinct_k.sql",
            include_sql!("lite/policy_compact_distinct_k.sql"),
        ),
        (
            "lite/policy_compact_max_offset_id.sql",
            include_sql!("lite/policy_compact_max_offset_id.sql"),
        ),
        (
            "lite/policy_compact_topitions.sql",
            include_sql!("lite/policy_compact_topitions.sql"),
        ),
        (
            "lite/policy_delete.sql",
            include_sql!("lite/policy_delete.sql"),
        ),
        (
            "lite/policy_delete_bytes.sql",
            include_sql!("lite/policy_delete_bytes.sql"),
        ),
        ("lite/vacuum_into.sql", include_sql!("lite/vacuum_into.sql")),
        (
            "nisshi_schema_version_create.sql",
            include_sql!("sql/nisshi_schema_version_create.sql"),
        ),
        (
            "nisshi_schema_version_insert.sql",
            include_sql!("sql/nisshi_schema_version_insert.sql"),
        ),
        (
            "nisshi_schema_version_select.sql",
            include_sql!("sql/nisshi_schema_version_select.sql"),
        ),
        (
            "pg/nisshi_schema_version_exists.sql",
            include_sql!("pg/nisshi_schema_version_exists.sql"),
        ),
        (
            "pg/search_path_set_to_cluster_namespace.sql",
            include_sql!("pg/search_path_set_to_cluster_namespace.sql"),
        ),
        ("policy_compact.sql", include_sql!("sql/policy_compact.sql")),
        ("policy_delete.sql", include_sql!("sql/policy_delete.sql")),
        (
            "policy_delete_bytes.sql",
            include_sql!("sql/policy_delete_bytes.sql"),
        ),
        ("ping.sql", "select 1 + 1".to_string()),
        (
            "producer_detail_delete_by_topic.sql",
            include_sql!("sql/producer_detail_delete_by_topic.sql"),
        ),
        (
            "producer_detail_insert.sql",
            include_sql!("sql/producer_detail_insert.sql"),
        ),
        (
            "producer_epoch_current_for_producer.sql",
            include_sql!("sql/producer_epoch_current_for_producer.sql"),
        ),
        (
            "producer_epoch_for_current_txn.sql",
            include_sql!("sql/producer_epoch_for_current_txn.sql"),
        ),
        (
            "producer_epoch_insert.sql",
            include_sql!("sql/producer_epoch_insert.sql"),
        ),
        (
            "producer_epoch_insert_given.sql",
            include_sql!("sql/producer_epoch_insert_given.sql"),
        ),
        (
            producer_epochs_select::QUERY,
            include_sql!("sql/producer_epochs_select.sql"),
        ),
        (
            "producer_insert.sql",
            include_sql!("sql/producer_insert.sql"),
        ),
        (
            "producer_select_for_update.sql",
            include_sql!("sql/producer_select_for_update.sql"),
        ),
        (
            "producer_update_epoch_with_txn.sql",
            include_sql!("sql/producer_update_epoch_with_txn.sql"),
        ),
        (
            "producer_update_sequence.sql",
            include_sql!("sql/producer_update_sequence.sql"),
        ),
        ("record_copy.sql", include_sql!("sql/record_copy.sql")),
        (
            "record_delete_by_offset.sql",
            include_sql!("sql/record_delete_by_offset.sql"),
        ),
        (
            "record_delete_below_log_start.sql",
            include_sql!("sql/record_delete_below_log_start.sql"),
        ),
        (
            "record_delete_by_topic.sql",
            include_sql!("sql/record_delete_by_topic.sql"),
        ),
        ("record_fetch.sql", include_sql!("sql/record_fetch.sql")),
        (
            "record_fetch_keyed.sql",
            include_sql!("sql/record_fetch_keyed.sql"),
        ),
        ("record_fetch_pg.sql", include_sql!("pg/record_fetch.sql")),
        (
            "record_fetch_pg_keyed.sql",
            include_sql!("pg/record_fetch_keyed.sql"),
        ),
        ("record_insert.sql", include_sql!("sql/record_insert.sql")),
        (
            "register_broker.sql",
            include_sql!("sql/register_broker.sql"),
        ),
        (
            "scram_credential_delete.sql",
            include_sql!("sql/scram_credential_delete.sql"),
        ),
        (
            "scram_credential_insert.sql",
            include_sql!("sql/scram_credential_insert.sql"),
        ),
        (
            "scram_credential_select.sql",
            include_sql!("sql/scram_credential_select.sql"),
        ),
        (
            "topic_by_cluster.sql",
            include_sql!("sql/topic_by_cluster.sql"),
        ),
        ("topic_by_uuid.sql", include_sql!("sql/topic_by_uuid.sql")),
        (
            "topic_configuration_delete_by_topic.sql",
            include_sql!("sql/topic_configuration_delete_by_topic.sql"),
        ),
        (
            "topic_configuration_delete.sql",
            include_sql!("sql/topic_configuration_delete.sql"),
        ),
        (
            topic_configuration_select::QUERY,
            include_sql!("sql/topic_configuration_select.sql"),
        ),
        (
            "topic_configuration_select_name.sql",
            include_sql!("sql/topic_configuration_select_name.sql"),
        ),
        (
            "topic_configuration_upsert.sql",
            include_sql!("sql/topic_configuration_upsert.sql"),
        ),
        (
            "topic_delete_by.sql",
            include_sql!("sql/topic_delete_by.sql"),
        ),
        ("topic_insert.sql", include_sql!("sql/topic_insert.sql")),
        (topic_select::QUERY, include_sql!("sql/topic_select.sql")),
        (
            "topic_select_name_for_update.sql",
            include_sql!("sql/topic_select_name_for_update.sql"),
        ),
        (
            "topic_select_name.sql",
            include_sql!("sql/topic_select_name.sql"),
        ),
        (
            "topic_select_uuid.sql",
            include_sql!("sql/topic_select_uuid.sql"),
        ),
        (
            "pg/topic_select_uuid.sql",
            include_sql!("pg/topic_select_uuid.sql"),
        ),
        (
            topic_configuration_select_keys::QUERY,
            include_sql!("pg/topic_configuration_select_keys.sql"),
        ),
        (
            "topition_delete_by_topic.sql",
            include_sql!("sql/topition_delete_by_topic.sql"),
        ),
        (
            "topition_insert.sql",
            include_sql!("sql/topition_insert.sql"),
        ),
        (
            "topition_select.sql",
            include_sql!("sql/topition_select.sql"),
        ),
        (
            "topition_select_id.sql",
            include_sql!("sql/topition_select_id.sql"),
        ),
        (
            "txn_detail_insert.sql",
            include_sql!("sql/txn_detail_insert.sql"),
        ),
        (
            "txn_detail_select_current.sql",
            include_sql!("sql/txn_detail_select_current.sql"),
        ),
        (
            "txn_detail_select_for_update.sql",
            include_sql!("sql/txn_detail_select_for_update.sql"),
        ),
        (
            "txn_detail_select_status_for_update.sql",
            include_sql!("sql/txn_detail_select_status_for_update.sql"),
        ),
        (
            "txn_detail_select_timed_out.sql",
            include_sql!("sql/txn_detail_select_timed_out.sql"),
        ),
        (
            "txn_detail_select.sql",
            include_sql!("sql/txn_detail_select.sql"),
        ),
        (
            "txn_detail_update_sequence.sql",
            include_sql!("sql/txn_detail_update_sequence.sql"),
        ),
        (
            "txn_detail_update_started_at.sql",
            include_sql!("sql/txn_detail_update_started_at.sql"),
        ),
        ("txn_insert.sql", include_sql!("sql/txn_insert.sql")),
        (
            "txn_offset_commit_delete_by_txn.sql",
            include_sql!("sql/txn_offset_commit_delete_by_txn.sql"),
        ),
        (
            "txn_offset_commit_insert.sql",
            include_sql!("sql/txn_offset_commit_insert.sql"),
        ),
        (
            "txn_offset_commit_tp_delete_by_topic.sql",
            include_sql!("sql/txn_offset_commit_tp_delete_by_topic.sql"),
        ),
        (
            "txn_offset_commit_tp_delete_by_txn.sql",
            include_sql!("sql/txn_offset_commit_tp_delete_by_txn.sql"),
        ),
        (
            "txn_offset_commit_tp_insert.sql",
            include_sql!("sql/txn_offset_commit_tp_insert.sql"),
        ),
        (
            "txn_offset_commit_tp_select_pending.sql",
            include_sql!("sql/txn_offset_commit_tp_select_pending.sql"),
        ),
        (
            "txn_produce_offset_delete_by_topic.sql",
            include_sql!("sql/txn_produce_offset_delete_by_topic.sql"),
        ),
        (
            "txn_produce_offset_delete_by_txn.sql",
            include_sql!("sql/txn_produce_offset_delete_by_txn.sql"),
        ),
        (
            "txn_produce_offset_insert.sql",
            include_sql!("sql/txn_produce_offset_insert.sql"),
        ),
        (
            txn_produce_partition_select::QUERY,
            include_sql!("sql/txn_produce_partition_select.sql"),
        ),
        (
            "txn_produce_offset_select_offset_range.sql",
            include_sql!("sql/txn_produce_offset_select_offset_range.sql"),
        ),
        (
            "txn_produce_offset_select_overlapping_txn.sql",
            include_sql!("sql/txn_produce_offset_select_overlapping_txn.sql"),
        ),
        (
            "txn_select_name.sql",
            include_sql!("sql/txn_select_name.sql"),
        ),
        (
            "txn_select_name_for_update.sql",
            include_sql!("sql/txn_select_name_for_update.sql"),
        ),
        (
            "txn_select_produced_topitions.sql",
            include_sql!("sql/txn_select_produced_topitions.sql"),
        ),
        (
            "txn_select_producer_epoch.sql",
            include_sql!("sql/txn_select_producer_epoch.sql"),
        ),
        (
            "txn_status_update.sql",
            include_sql!("sql/txn_status_update.sql"),
        ),
        (
            "txn_topition_delete_by_topic.sql",
            include_sql!("sql/txn_topition_delete_by_topic.sql"),
        ),
        (
            "txn_topition_delete_by_txn.sql",
            include_sql!("sql/txn_topition_delete_by_txn.sql"),
        ),
        (
            "txn_topition_insert.sql",
            include_sql!("sql/txn_topition_insert.sql"),
        ),
        (
            "txn_topition_select_txns.sql",
            include_sql!("sql/txn_topition_select_txns.sql"),
        ),
        (
            "txn_topition_select.sql",
            include_sql!("sql/txn_topition_select.sql"),
        ),
        (
            "txn_update_producer.sql",
            include_sql!("sql/txn_update_producer.sql"),
        ),
        (
            "virtual_topic_upsert.sql",
            include_sql!("sql/virtual_topic_upsert.sql"),
        ),
        (
            "watermark_delete_by_topic.sql",
            include_sql!("sql/watermark_delete_by_topic.sql"),
        ),
        (
            "watermark_insert_from_txn.sql",
            include_sql!("sql/watermark_insert_from_txn.sql"),
        ),
        (
            "watermark_insert.sql",
            include_sql!("sql/watermark_insert.sql"),
        ),
        (
            watermark_select_for_write::QUERY,
            include_sql!("sql/watermark_select_for_write.sql"),
        ),
        (
            watermark_select_for_write::LITE_QUERY,
            include_sql!("lite/watermark_select_for_write.sql"),
        ),
        (
            "watermark_select_stable.sql",
            include_sql!("sql/watermark_select_stable.sql"),
        ),
        (
            "watermark_select.sql",
            include_sql!("sql/watermark_select.sql"),
        ),
        (
            "watermark_update.sql",
            include_sql!("sql/watermark_update.sql"),
        ),
    ];

    Cache::new(BTreeMap::from(mapping))
});

pub(crate) fn remove_comments(commented: &str) -> String {
    commented.lines().fold(String::new(), |uncommented, line| {
        if let Some(position) = line.find("--") {
            match line.split_at(position) {
                ("", _) => uncommented,
                (before, _) => {
                    if uncommented.is_empty() {
                        before.trim().into()
                    } else {
                        format!("{uncommented} {before}")
                    }
                }
            }
        } else if line.trim().is_empty() {
            uncommented
        } else if uncommented.is_empty() {
            line.trim().into()
        } else {
            format!("{uncommented} {}", line.trim())
        }
    })
}

/// Checks a batch's epoch against the epochs stored for its producer, read with
/// [`producer_epochs_select`]. The batch's producer is transactional when `stored` says so,
/// when the request has a transactional ID, or when the batch is transactional.
pub(crate) fn idempotent_epoch_check(
    transaction_id: Option<&str>,
    stored: StoredEpochs,
    deflated: &deflated::Batch,
) -> Result<ProduceEpoch> {
    let stored = StoredEpochs {
        transactional: stored.transactional
            || transaction_id.is_some()
            || deflated.is_transactional(),
        ..stored
    };

    stored
        .check_batch(deflated.producer_epoch, deflated.base_sequence)
        .inspect_err(|error| debug!(?error, ?stored, deflated.producer_epoch))
        .map_err(Error::Api)
}

/// Checks that a client's transactional batch goes to a partition in its producer's ongoing
/// transaction, from the row that [`txn_produce_partition_select`] read: `None` when the
/// transactional ID does not exist, otherwise the producer ID that owns it and whether the
/// partition was added at the batch's producer ID and epoch (the query's `added` column).
///
/// Kafka's transaction coordinator answers an unknown transactional ID, or one that another
/// producer ID owns, `INVALID_PRODUCER_ID_MAPPING`, a transaction that is preparing to commit
/// or abort `CONCURRENT_TRANSACTIONS`, and a partition that is not in the transaction
/// `TRANSACTION_ABORTABLE` ([TransactionCoordinator.scala#L357-L402]). A preparing transaction
/// has its marker written and its partitions still listed, while it waits for an older
/// overlapping transaction, so a batch stored then would land after the marker. Storing the
/// batch anyway would leave records that no transaction marker ever closes, holding back the
/// partition's last stable offset.
///
/// [TransactionCoordinator.scala#L357-L402]: https://github.com/apache/kafka/blob/4.1.0/core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala#L357-L402
pub(crate) fn txn_produce_partition_check(
    row: Option<(Option<i64>, i32)>,
    producer_id: i64,
) -> Result<()> {
    match row {
        Some((Some(producer), txn_produce_partition_select::ADDED_IN_TRANSACTION))
            if producer == producer_id =>
        {
            Ok(())
        }
        Some((Some(producer), txn_produce_partition_select::ADDED_IN_PREPARING_TRANSACTION))
            if producer == producer_id =>
        {
            Err(Error::Api(ErrorCode::ConcurrentTransactions))
        }
        Some((Some(producer), _not_added)) if producer == producer_id => {
            Err(Error::Api(ErrorCode::TransactionAbortable))
        }
        None | Some(_) => Err(Error::Api(ErrorCode::InvalidProducerIdMapping)),
    }
}

/// Checks a batch's first sequence against `sequence`, the next sequence stored for its
/// producer's epoch on the partition, and returns how far the batch advances it.
///
/// The caller checks the batch's epoch first, with [`idempotent_epoch_check`].
pub(crate) fn idempotent_sequence_check(sequence: i32, deflated: &deflated::Batch) -> Result<i32> {
    match sequence.cmp(&deflated.base_sequence) {
        Ordering::Equal => Ok(deflated.last_offset_delta + 1),

        Ordering::Greater => {
            debug!(?sequence, ?deflated.base_sequence);
            Err(Error::Api(ErrorCode::DuplicateSequenceNumber))
        }

        Ordering::Less => {
            debug!(?sequence, ?deflated.base_sequence);
            Err(Error::Api(ErrorCode::OutOfOrderSequenceNumber))
        }
    }
}

pub(crate) fn default_hash<H>(h: &H) -> Uuid
where
    H: Hash,
{
    let mut s = DefaultHasher::new();
    h.hash(&mut s);
    Uuid::from_u128(s.finish() as u128)
}

#[cfg(test)]
mod tests {
    use nisshi_storage::{Error, Result};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(EnvFilter::from_default_env().add_directive(
                    format!("{}=debug", env!("CARGO_PKG_NAME").replace("-", "_")).parse()?,
                ))
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[test]
    fn txn_produce_partition_check() {
        use nisshi_sans_io::ErrorCode;
        use std::assert_matches;

        assert_matches!(
            super::txn_produce_partition_check(Some((Some(7), 1)), 7),
            Ok(())
        );

        assert_matches!(
            super::txn_produce_partition_check(Some((Some(7), 0)), 7),
            Err(Error::Api(ErrorCode::TransactionAbortable))
        );

        assert_matches!(
            super::txn_produce_partition_check(Some((Some(7), 2)), 7),
            Err(Error::Api(ErrorCode::ConcurrentTransactions))
        );

        for row in [
            None,
            Some((None, 1)),
            Some((Some(8), 1)),
            Some((Some(8), 0)),
            Some((Some(8), 2)),
        ] {
            assert_matches!(
                super::txn_produce_partition_check(row, 7),
                Err(Error::Api(ErrorCode::InvalidProducerIdMapping)),
                "{row:?}"
            );
        }
    }

    #[test]
    fn remove_comments() -> Result<()> {
        let _guard = init_tracing()?;

        assert_eq!(String::from(""), super::remove_comments(""));
        assert_eq!(String::from("pqr"), super::remove_comments("-- abc\npqr"));
        assert_eq!(String::from("abc"), super::remove_comments("abc -- def"));
        assert_eq!(String::from("abc def"), super::remove_comments("abc\ndef"));
        assert_eq!(
            String::from("abc def"),
            super::remove_comments("abc\n\ndef")
        );
        assert_eq!(
            String::from("abc def"),
            super::remove_comments("abc \ndef ")
        );

        Ok(())
    }
}
