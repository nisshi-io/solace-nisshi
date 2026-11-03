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

//! The topic's `message.timestamp.type` selects the timestamp of each stored record. On a
//! `LogAppendTime` topic, the broker stamps each batch with its own clock. On a `CreateTime` topic,
//! the broker keeps the producer's timestamps, even for a batch that the producer marks
//! `LogAppendTime`. The broker refuses a producer's `LogAppendTime` bit where Kafka refuses it.

use crate::{
    common::{StorageType, alphanumeric_string, init_tracing},
    produce::{
        create_time_batch, create_topic_with_configs, fetch_batches, now_ms, produce_batches,
    },
    produce_max_message_bytes::create_topic_result,
};
use bytes::Bytes;
use nisshi_broker::Result;
use nisshi_sans_io::{
    BatchAttribute, Compression, ConfigResource, ErrorCode, IncrementalAlterConfigsRequest,
    IsolationLevel, ListOffset, OpType, RequestInput, TimestampType,
    incremental_alter_configs_request::{AlterConfigsResource, AlterableConfig},
    incremental_alter_configs_response::AlterConfigsResourceResponse,
    produce_response::PartitionProduceResponse,
    record::{Record, deflated, inflated},
};
use nisshi_storage::{
    ArcDynStorage, IncrementalAlterConfigsService, Storage, Topition, message_timestamp_type,
};
use rama::{Service as _, extensions::Extensions};
use rand::{RngExt as _, rng};
use std::time::{Duration, UNIX_EPOCH};
use url::Url;
use uuid::Uuid;

const PARTITION: i32 = 0;

const LOG_APPEND_TIME: (&str, &str) = (message_timestamp_type::KEY, "LogAppendTime");

const TEN_YEARS_MS: i64 = 10 * 365 * 24 * 60 * 60 * 1000;

async fn storage(storage_type: StorageType) -> Result<ArcDynStorage> {
    crate::common::storage_container(
        storage_type,
        Uuid::now_v7(),
        rng().random_range(0..i32::MAX),
        Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await
}

/// Returns a batch with one record `timestamp_delta` milliseconds after `base_timestamp`, on which
/// the producer has set the `LogAppendTime` bit. A normal producer leaves the bit clear, because
/// the topic's config decides the type.
fn marked_log_append_time(base_timestamp: i64, timestamp_delta: i64) -> Result<deflated::Batch> {
    marked_log_append_time_with_max(
        base_timestamp,
        timestamp_delta,
        base_timestamp + timestamp_delta,
    )
}

/// Returns [`marked_log_append_time`] with `max_timestamp` in its header instead of the record's
/// timestamp.
fn marked_log_append_time_with_max(
    base_timestamp: i64,
    timestamp_delta: i64,
    max_timestamp: i64,
) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .attributes(
            BatchAttribute::default()
                .timestamp(TimestampType::LogAppendTime)
                .into(),
        )
        .base_timestamp(base_timestamp)
        .max_timestamp(max_timestamp)
        .record(
            Record::builder()
                .value(Bytes::from_static(b"marked log append time").into())
                .timestamp_delta(timestamp_delta),
        )
        .build()
        .and_then(deflated::Batch::try_from)
        .map_err(Into::into)
}

/// On a `LogAppendTime` topic, the broker replaces the producer's timestamps with the time when it
/// stores the batch. Records that the producer timestamped a day ago therefore read as the broker's
/// time, the produce response returns that time, and the stored batch is marked `LogAppendTime`.
async fn stamps_each_record_with_the_broker_clock(storage: impl Storage + Clone) -> Result<()> {
    let topic = create_topic_with_configs(storage.clone(), &[LOG_APPEND_TIME]).await?;
    let a_day_ago = now_ms()? - 24 * 60 * 60 * 1000;

    let before = now_ms()?;
    let response = produce_batches(
        storage.clone(),
        &topic,
        PARTITION,
        vec![create_time_batch(
            Compression::Gzip,
            a_day_ago,
            -1,
            &[0, 5, 10],
        )?],
    )
    .await?;
    let after = now_ms()?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

    let log_append_time = response
        .log_append_time_ms
        .expect("a produce response carries log_append_time_ms");
    assert!(
        (before..=after).contains(&log_append_time),
        "{log_append_time} is not between {before} and {after}"
    );

    let fetched = fetch_batches(storage, &topic, PARTITION).await?;
    assert_eq!(1, fetched.len());
    assert_eq!(fetched[0].computed_crc(), fetched[0].crc);

    let batch = inflated::Batch::try_from(&fetched[0])?;
    assert_eq!(
        TimestampType::LogAppendTime,
        TimestampType::from(batch.attributes)
    );
    assert_eq!(3, batch.records.len());

    for record in &batch.records {
        assert_eq!(log_append_time, batch.record_timestamp(record));
    }

    Ok(())
}

/// Two produce requests that the broker stores at different times keep their own times, even when
/// one fetch returns both. The SQL engines store records as rows and rebuild batches on fetch. A
/// `LogAppendTime` batch has one timestamp for all its records, so a rebuild that merged the two
/// requests' records would give all of them the later time.
async fn each_batch_keeps_its_own_broker_timestamp(storage: impl Storage + Clone) -> Result<()> {
    let topic = create_topic_with_configs(storage.clone(), &[LOG_APPEND_TIME]).await?;

    let mut log_append_times = vec![];

    for _ in 0..2 {
        let response = produce_batches(
            storage.clone(),
            &topic,
            PARTITION,
            vec![create_time_batch(
                Compression::None,
                now_ms()?,
                -1,
                &[0, 1],
            )?],
        )
        .await?;
        assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

        log_append_times.push(
            response
                .log_append_time_ms
                .expect("a produce response carries log_append_time_ms"),
        );

        // The sleep makes the broker give the next batch a later time.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let mut record_timestamps = vec![];

    for fetched in fetch_batches(storage, &topic, PARTITION).await? {
        let batch = inflated::Batch::try_from(&fetched)?;

        for record in &batch.records {
            record_timestamps.push(batch.record_timestamp(record));
        }
    }

    assert_eq!(
        vec![
            log_append_times[0],
            log_append_times[0],
            log_append_times[1],
            log_append_times[1]
        ],
        record_timestamps
    );

    Ok(())
}

/// On a `LogAppendTime` topic, the broker accepts a record that the producer dated ten years in
/// the future. A `CreateTime` topic rejects a record more than an hour ahead of the broker's clock.
/// On a `LogAppendTime` topic, the broker replaces the record's timestamp, so that limit does not
/// apply.
async fn accepts_a_record_outside_the_timestamp_window(
    storage: impl Storage + Clone,
) -> Result<()> {
    let topic = create_topic_with_configs(storage.clone(), &[LOG_APPEND_TIME]).await?;

    let response = produce_batches(
        storage,
        &topic,
        PARTITION,
        vec![create_time_batch(
            Compression::None,
            now_ms()?,
            -1,
            &[TEN_YEARS_MS],
        )?],
    )
    .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
    assert_eq!(0, response.base_offset);

    Ok(())
}

/// On a `CreateTime` topic, the broker ignores a `LogAppendTime` bit that the producer set, as
/// Kafka does. The topic's config decides the type, not the batch. A marked batch with a record ten
/// years ahead is therefore rejected like any `CreateTime` batch, instead of skipping the one-hour
/// limit. A marked batch whose `max_timestamp` is -1 is rejected, as Kafka rejects it. A marked
/// batch within the limit is stored as `CreateTime`, and its record keeps the producer's
/// timestamp.
async fn create_time_topic_validates_a_batch_marked_log_append_time(
    storage: impl Storage + Clone,
) -> Result<()> {
    let topic = create_topic_with_configs(storage.clone(), &[]).await?;
    let now = now_ms()?;

    let rejected = produce_batches(
        storage.clone(),
        &topic,
        PARTITION,
        vec![marked_log_append_time(now, TEN_YEARS_MS)?],
    )
    .await?;
    assert_eq!(
        ErrorCode::InvalidTimestamp,
        ErrorCode::try_from(rejected.error_code)?
    );

    // Kafka reads a marked batch's `max_timestamp` as each record's timestamp, and -1 skips the
    // window, so Kafka refuses the bit here.
    let without_timestamp = produce_batches(
        storage.clone(),
        &topic,
        PARTITION,
        vec![marked_log_append_time_with_max(now, 0, -1)?],
    )
    .await?;
    assert_refused_log_append_time(&without_timestamp)?;

    let accepted = produce_batches(
        storage.clone(),
        &topic,
        PARTITION,
        vec![marked_log_append_time(now, 7)?],
    )
    .await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(accepted.error_code)?);
    assert_eq!(0, accepted.base_offset);
    assert_eq!(Some(-1), accepted.log_append_time_ms);

    let fetched = fetch_batches(storage, &topic, PARTITION).await?;
    assert_eq!(1, fetched.len());

    let batch = inflated::Batch::try_from(&fetched[0])?;
    assert_eq!(
        TimestampType::CreateTime,
        TimestampType::from(batch.attributes)
    );
    assert_eq!(1, batch.records.len());
    assert_eq!(now + 7, batch.record_timestamp(&batch.records[0]));

    Ok(())
}

/// Asserts that `response` refuses a producer's `LogAppendTime` bit with `INVALID_TIMESTAMP`, and
/// Kafka's top-level message and record error.
fn assert_refused_log_append_time(response: &PartitionProduceResponse) -> Result<()> {
    assert_eq!(
        ErrorCode::InvalidTimestamp,
        ErrorCode::try_from(response.error_code)?
    );
    assert_eq!(
        Some("One or more records have been rejected due to invalid timestamp"),
        response.error_message.as_deref()
    );

    let record_errors = response.record_errors.as_deref().unwrap_or_default();
    assert_eq!(1, record_errors.len());
    assert_eq!(0, record_errors[0].batch_index);
    assert_eq!(
        Some(
            "Invalid timestamp type in the record at batch index 0. Producer should not set \
             timestamp type to LogAppendTime."
        ),
        record_errors[0].batch_index_error_message.as_deref()
    );

    Ok(())
}

/// On a `LogAppendTime` topic, the broker refuses a batch on which the producer set the
/// `LogAppendTime` bit, as Kafka does. Only the broker sets the bit on such a topic.
async fn log_append_time_topic_rejects_a_batch_marked_log_append_time(
    storage: impl Storage + Clone,
) -> Result<()> {
    let topic = create_topic_with_configs(storage.clone(), &[LOG_APPEND_TIME]).await?;

    let response = produce_batches(
        storage.clone(),
        &topic,
        PARTITION,
        vec![marked_log_append_time(now_ms()?, 0)?],
    )
    .await?;
    assert_refused_log_append_time(&response)?;
    assert_eq!(-1, response.base_offset);

    assert!(fetch_batches(storage, &topic, PARTITION).await?.is_empty());

    Ok(())
}

/// A lookup by timestamp, such as a consumer's `offsetsForTimes`, returns the first offset whose
/// timestamp is at or after the target. On a `LogAppendTime` topic, it must compare the broker's
/// times, which a consumer reads, and not the producer's. A record that the producer dated a day
/// ago is therefore found by a lookup for the time when the broker stored it.
async fn timestamp_lookup_uses_the_broker_clock(storage: impl Storage + Clone) -> Result<()> {
    let topic = create_topic_with_configs(storage.clone(), &[LOG_APPEND_TIME]).await?;
    let a_day_ago = now_ms()? - 24 * 60 * 60 * 1000;

    let response = produce_batches(
        storage.clone(),
        &topic,
        PARTITION,
        vec![create_time_batch(Compression::None, a_day_ago, -1, &[0])?],
    )
    .await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

    let log_append_time = response
        .log_append_time_ms
        .expect("a produce response carries log_append_time_ms");

    let target = UNIX_EPOCH + Duration::from_millis(u64::try_from(log_append_time)?);

    let responses = storage
        .list_offsets(
            IsolationLevel::ReadUncommitted,
            &[(
                Topition::new(topic.as_str(), PARTITION),
                ListOffset::Timestamp(target),
            )],
        )
        .await?;

    assert_eq!(1, responses.len());
    let (_, found) = &responses[0];
    assert_eq!(ErrorCode::None, found.error_code);
    assert_eq!(Some(0), found.offset);
    assert_eq!(Some(target), found.timestamp);

    Ok(())
}

/// Sets the topic's `message.timestamp.type` to `configured` through IncrementalAlterConfigs, and
/// returns its result.
async fn alter_timestamp_type(
    storage: impl Storage + Clone,
    topic: &str,
    configured: &str,
) -> Result<AlterConfigsResourceResponse> {
    let response = IncrementalAlterConfigsService { storage }
        .serve(RequestInput {
            request: IncrementalAlterConfigsRequest::default()
                .resources(Some(vec![
                    AlterConfigsResource::default()
                        .resource_type(ConfigResource::Topic.into())
                        .resource_name(topic.into())
                        .configs(Some(vec![
                            AlterableConfig::default()
                                .name(message_timestamp_type::KEY.into())
                                .config_operation(OpType::Set.into())
                                .value(Some(configured.into())),
                        ])),
                ]))
                .validate_only(false),
            extensions: Extensions::default(),
        })
        .await?;

    let mut resources = response.responses.unwrap_or_default();
    assert_eq!(1, resources.len());
    Ok(resources.remove(0))
}

/// CreateTopics rejects a `message.timestamp.type` other than `CreateTime` or `LogAppendTime` with
/// `INVALID_CONFIG` and Kafka's message, and creates nothing. The match is case-sensitive, so
/// `logappendtime` is rejected.
async fn create_topics_rejects_an_invalid_timestamp_type(
    storage: impl Storage + Clone,
) -> Result<()> {
    let topic = alphanumeric_string(15);

    let result = create_topic_result(
        storage.clone(),
        &topic,
        &[(message_timestamp_type::KEY, "logappendtime")],
    )
    .await?;
    assert_eq!(
        ErrorCode::InvalidConfig,
        ErrorCode::try_from(result.error_code)?
    );
    assert_eq!(
        Some(
            "Invalid value logappendtime for configuration message.timestamp.type: String must be \
             one of: CreateTime, LogAppendTime"
        ),
        result.error_message.as_deref()
    );

    let result = create_topic_result(storage, &topic, &[]).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(result.error_code)?);

    Ok(())
}

/// IncrementalAlterConfigs rejects a `message.timestamp.type` other than `CreateTime` or
/// `LogAppendTime` with `INVALID_CONFIG` and Kafka's message. The topic keeps `LogAppendTime`, so
/// the next produce still gets the broker's time.
async fn incremental_alter_configs_rejects_an_invalid_timestamp_type(
    storage: impl Storage + Clone,
) -> Result<()> {
    let topic = create_topic_with_configs(storage.clone(), &[LOG_APPEND_TIME]).await?;

    let altered = alter_timestamp_type(storage.clone(), &topic, "Bogus").await?;
    assert_eq!(
        ErrorCode::InvalidConfig,
        ErrorCode::try_from(altered.error_code)?
    );
    assert_eq!(
        Some(
            "Invalid value Bogus for configuration message.timestamp.type: String must be one \
             of: CreateTime, LogAppendTime"
        ),
        altered.error_message.as_deref()
    );

    let response = produce_batches(
        storage,
        &topic,
        PARTITION,
        vec![create_time_batch(Compression::None, now_ms()?, -1, &[0])?],
    )
    .await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
    assert_ne!(Some(-1), response.log_append_time_ms);

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use super::*;

    #[tokio::test]
    async fn create_topics_rejects_an_invalid_timestamp_type() -> Result<()> {
        let _guard = init_tracing()?;
        super::create_topics_rejects_an_invalid_timestamp_type(
            storage(StorageType::InMemory).await?,
        )
        .await
    }

    #[tokio::test]
    async fn incremental_alter_configs_rejects_an_invalid_timestamp_type() -> Result<()> {
        let _guard = init_tracing()?;
        super::incremental_alter_configs_rejects_an_invalid_timestamp_type(
            storage(StorageType::InMemory).await?,
        )
        .await
    }

    #[tokio::test]
    async fn stamps_each_record_with_the_broker_clock() -> Result<()> {
        let _guard = init_tracing()?;
        super::stamps_each_record_with_the_broker_clock(storage(StorageType::InMemory).await?).await
    }

    #[tokio::test]
    async fn each_batch_keeps_its_own_broker_timestamp() -> Result<()> {
        let _guard = init_tracing()?;
        super::each_batch_keeps_its_own_broker_timestamp(storage(StorageType::InMemory).await?)
            .await
    }

    #[tokio::test]
    async fn accepts_a_record_outside_the_timestamp_window() -> Result<()> {
        let _guard = init_tracing()?;
        super::accepts_a_record_outside_the_timestamp_window(storage(StorageType::InMemory).await?)
            .await
    }

    #[tokio::test]
    async fn create_time_topic_validates_a_batch_marked_log_append_time() -> Result<()> {
        let _guard = init_tracing()?;
        super::create_time_topic_validates_a_batch_marked_log_append_time(
            storage(StorageType::InMemory).await?,
        )
        .await
    }

    #[tokio::test]
    async fn timestamp_lookup_uses_the_broker_clock() -> Result<()> {
        let _guard = init_tracing()?;
        super::timestamp_lookup_uses_the_broker_clock(storage(StorageType::InMemory).await?).await
    }

    #[tokio::test]
    async fn log_append_time_topic_rejects_a_batch_marked_log_append_time() -> Result<()> {
        let _guard = init_tracing()?;
        super::log_append_time_topic_rejects_a_batch_marked_log_append_time(
            storage(StorageType::InMemory).await?,
        )
        .await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use super::*;

    #[tokio::test]
    async fn create_topics_rejects_an_invalid_timestamp_type() -> Result<()> {
        let _guard = init_tracing()?;
        super::create_topics_rejects_an_invalid_timestamp_type(storage(StorageType::Lite).await?)
            .await
    }

    #[tokio::test]
    async fn incremental_alter_configs_rejects_an_invalid_timestamp_type() -> Result<()> {
        let _guard = init_tracing()?;
        super::incremental_alter_configs_rejects_an_invalid_timestamp_type(
            storage(StorageType::Lite).await?,
        )
        .await
    }

    #[tokio::test]
    async fn stamps_each_record_with_the_broker_clock() -> Result<()> {
        let _guard = init_tracing()?;
        super::stamps_each_record_with_the_broker_clock(storage(StorageType::Lite).await?).await
    }

    #[tokio::test]
    async fn each_batch_keeps_its_own_broker_timestamp() -> Result<()> {
        let _guard = init_tracing()?;
        super::each_batch_keeps_its_own_broker_timestamp(storage(StorageType::Lite).await?).await
    }

    #[tokio::test]
    async fn accepts_a_record_outside_the_timestamp_window() -> Result<()> {
        let _guard = init_tracing()?;
        super::accepts_a_record_outside_the_timestamp_window(storage(StorageType::Lite).await?)
            .await
    }

    #[tokio::test]
    async fn create_time_topic_validates_a_batch_marked_log_append_time() -> Result<()> {
        let _guard = init_tracing()?;
        super::create_time_topic_validates_a_batch_marked_log_append_time(
            storage(StorageType::Lite).await?,
        )
        .await
    }

    #[tokio::test]
    async fn timestamp_lookup_uses_the_broker_clock() -> Result<()> {
        let _guard = init_tracing()?;
        super::timestamp_lookup_uses_the_broker_clock(storage(StorageType::Lite).await?).await
    }

    #[tokio::test]
    async fn log_append_time_topic_rejects_a_batch_marked_log_append_time() -> Result<()> {
        let _guard = init_tracing()?;
        super::log_append_time_topic_rejects_a_batch_marked_log_append_time(
            storage(StorageType::Lite).await?,
        )
        .await
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use super::*;

    #[tokio::test]
    async fn create_topics_rejects_an_invalid_timestamp_type() -> Result<()> {
        let _guard = init_tracing()?;
        super::create_topics_rejects_an_invalid_timestamp_type(
            storage(StorageType::Postgres).await?,
        )
        .await
    }

    #[tokio::test]
    async fn incremental_alter_configs_rejects_an_invalid_timestamp_type() -> Result<()> {
        let _guard = init_tracing()?;
        super::incremental_alter_configs_rejects_an_invalid_timestamp_type(
            storage(StorageType::Postgres).await?,
        )
        .await
    }

    #[tokio::test]
    async fn stamps_each_record_with_the_broker_clock() -> Result<()> {
        let _guard = init_tracing()?;
        super::stamps_each_record_with_the_broker_clock(storage(StorageType::Postgres).await?).await
    }

    #[tokio::test]
    async fn each_batch_keeps_its_own_broker_timestamp() -> Result<()> {
        let _guard = init_tracing()?;
        super::each_batch_keeps_its_own_broker_timestamp(storage(StorageType::Postgres).await?)
            .await
    }

    #[tokio::test]
    async fn accepts_a_record_outside_the_timestamp_window() -> Result<()> {
        let _guard = init_tracing()?;
        super::accepts_a_record_outside_the_timestamp_window(storage(StorageType::Postgres).await?)
            .await
    }

    #[tokio::test]
    async fn create_time_topic_validates_a_batch_marked_log_append_time() -> Result<()> {
        let _guard = init_tracing()?;
        super::create_time_topic_validates_a_batch_marked_log_append_time(
            storage(StorageType::Postgres).await?,
        )
        .await
    }

    #[tokio::test]
    async fn timestamp_lookup_uses_the_broker_clock() -> Result<()> {
        let _guard = init_tracing()?;
        super::timestamp_lookup_uses_the_broker_clock(storage(StorageType::Postgres).await?).await
    }

    #[tokio::test]
    async fn log_append_time_topic_rejects_a_batch_marked_log_append_time() -> Result<()> {
        let _guard = init_tracing()?;
        super::log_append_time_topic_rejects_a_batch_marked_log_append_time(
            storage(StorageType::Postgres).await?,
        )
        .await
    }
}

#[cfg(feature = "slatedb")]
mod slate {
    use super::*;

    #[tokio::test]
    async fn create_topics_rejects_an_invalid_timestamp_type() -> Result<()> {
        let _guard = init_tracing()?;
        super::create_topics_rejects_an_invalid_timestamp_type(storage(StorageType::SlateDb).await?)
            .await
    }

    #[tokio::test]
    async fn incremental_alter_configs_rejects_an_invalid_timestamp_type() -> Result<()> {
        let _guard = init_tracing()?;
        super::incremental_alter_configs_rejects_an_invalid_timestamp_type(
            storage(StorageType::SlateDb).await?,
        )
        .await
    }

    #[tokio::test]
    async fn stamps_each_record_with_the_broker_clock() -> Result<()> {
        let _guard = init_tracing()?;
        super::stamps_each_record_with_the_broker_clock(storage(StorageType::SlateDb).await?).await
    }

    #[tokio::test]
    async fn each_batch_keeps_its_own_broker_timestamp() -> Result<()> {
        let _guard = init_tracing()?;
        super::each_batch_keeps_its_own_broker_timestamp(storage(StorageType::SlateDb).await?).await
    }

    #[tokio::test]
    async fn accepts_a_record_outside_the_timestamp_window() -> Result<()> {
        let _guard = init_tracing()?;
        super::accepts_a_record_outside_the_timestamp_window(storage(StorageType::SlateDb).await?)
            .await
    }

    #[tokio::test]
    async fn create_time_topic_validates_a_batch_marked_log_append_time() -> Result<()> {
        let _guard = init_tracing()?;
        super::create_time_topic_validates_a_batch_marked_log_append_time(
            storage(StorageType::SlateDb).await?,
        )
        .await
    }
}

#[cfg(feature = "turso")]
mod turso {
    use super::*;

    #[ignore = "the turso backend fails its connection check in tests"]
    #[tokio::test]
    async fn stamps_each_record_with_the_broker_clock() -> Result<()> {
        let _guard = init_tracing()?;
        super::stamps_each_record_with_the_broker_clock(storage(StorageType::Turso).await?).await
    }

    #[ignore = "the turso backend fails its connection check in tests"]
    #[tokio::test]
    async fn each_batch_keeps_its_own_broker_timestamp() -> Result<()> {
        let _guard = init_tracing()?;
        super::each_batch_keeps_its_own_broker_timestamp(storage(StorageType::Turso).await?).await
    }

    #[ignore = "the turso backend fails its connection check in tests"]
    #[tokio::test]
    async fn accepts_a_record_outside_the_timestamp_window() -> Result<()> {
        let _guard = init_tracing()?;
        super::accepts_a_record_outside_the_timestamp_window(storage(StorageType::Turso).await?)
            .await
    }

    #[ignore = "the turso backend fails its connection check in tests"]
    #[tokio::test]
    async fn create_time_topic_validates_a_batch_marked_log_append_time() -> Result<()> {
        let _guard = init_tracing()?;
        super::create_time_topic_validates_a_batch_marked_log_append_time(
            storage(StorageType::Turso).await?,
        )
        .await
    }

    #[ignore = "the turso backend fails its connection check in tests"]
    #[tokio::test]
    async fn timestamp_lookup_uses_the_broker_clock() -> Result<()> {
        let _guard = init_tracing()?;
        super::timestamp_lookup_uses_the_broker_clock(storage(StorageType::Turso).await?).await
    }

    #[ignore = "the turso backend fails its connection check in tests"]
    #[tokio::test]
    async fn log_append_time_topic_rejects_a_batch_marked_log_append_time() -> Result<()> {
        let _guard = init_tracing()?;
        super::log_append_time_topic_rejects_a_batch_marked_log_append_time(
            storage(StorageType::Turso).await?,
        )
        .await
    }
}
