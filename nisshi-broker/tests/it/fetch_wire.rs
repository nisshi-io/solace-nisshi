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

//! A Fetch at an offset below the log start, over a real connection: the
//! broker answers `OFFSET_OUT_OF_RANGE` without waiting out `max_wait`, and
//! the connection stays open for the next request.
//!
//! The check is independent of the storage backend, so only the memory
//! backend is used.

use std::time::Duration;

use anyhow::{Result, anyhow};
use nisshi_sans_io::{
    ApiKey as _, ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest,
    CreateTopicsResponse, ErrorCode, FetchRequest, FetchResponse,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
};
use tokio::{
    net::TcpStream,
    time::{Instant, timeout},
};

use crate::common::{
    alphanumeric_string, init_tracing,
    wire::{round_trip, spawn_broker},
};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Long enough that waiting it out would fail the elapsed check.
const MAX_WAIT_MS: i32 = 10_000;

async fn create_topic(stream: &mut TcpStream, name: &str) -> Result<()> {
    let frame = round_trip(
        stream,
        CreateTopicsRequest::KEY,
        7,
        1,
        CreateTopicsRequest::default()
            .topics(Some(
                [CreatableTopic::default()
                    .name(name.into())
                    .num_partitions(1)
                    .replication_factor(1)
                    .assignments(Some([].into()))
                    .configs(Some([].into()))]
                .into(),
            ))
            .timeout_ms(5_000)
            .validate_only(Some(false))
            .into(),
    )
    .await?;

    let response = CreateTopicsResponse::try_from(frame.body)?;
    let topics = response.topics.unwrap_or_default();
    assert_eq!(1, topics.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    Ok(())
}

async fn fetch(
    stream: &mut TcpStream,
    correlation_id: i32,
    topic: &str,
    fetch_offset: i64,
) -> Result<FetchResponse> {
    let frame = round_trip(
        stream,
        FetchRequest::KEY,
        12,
        correlation_id,
        FetchRequest::default()
            .replica_id(Some(-1))
            .max_wait_ms(MAX_WAIT_MS)
            .min_bytes(1)
            .max_bytes(Some(50 * 1024))
            .isolation_level(Some(0))
            .session_id(Some(0))
            .session_epoch(Some(-1))
            .topics(Some(
                [FetchTopic::default()
                    .topic(Some(topic.into()))
                    .partitions(Some(
                        [FetchPartition::default()
                            .partition(0)
                            .current_leader_epoch(Some(-1))
                            .fetch_offset(fetch_offset)
                            .last_fetched_epoch(Some(-1))
                            .log_start_offset(Some(-1))
                            .partition_max_bytes(50 * 1024)]
                        .into(),
                    ))]
                .into(),
            ))
            .forgotten_topics_data(Some([].into()))
            .rack_id(Some("".into()))
            .into(),
    )
    .await?;

    FetchResponse::try_from(frame.body).map_err(Into::into)
}

/// Fetch at -5, then at `i64::MIN`, then ApiVersions, all on one connection.
#[tokio::test]
async fn out_of_range_fetch_keeps_the_connection() -> Result<()> {
    let _guard = init_tracing()?;

    timeout(TEST_TIMEOUT, async {
        let broker = spawn_broker(None).await?;
        let mut stream = TcpStream::connect(broker.addr).await?;

        let topic = alphanumeric_string(15);
        create_topic(&mut stream, &topic).await?;

        for (correlation_id, fetch_offset) in [(2, -5), (3, i64::MIN)] {
            let started_at = Instant::now();
            let response = fetch(&mut stream, correlation_id, &topic, fetch_offset).await?;
            let elapsed = started_at.elapsed();

            assert!(
                elapsed < Duration::from_secs(5),
                "fetch at {fetch_offset} took {elapsed:?}"
            );

            let partitions = response
                .responses
                .unwrap_or_default()
                .into_iter()
                .flat_map(|topic| topic.partitions.unwrap_or_default())
                .collect::<Vec<_>>();

            assert_eq!(1, partitions.len(), "fetch at {fetch_offset}");
            assert_eq!(
                ErrorCode::OffsetOutOfRange,
                ErrorCode::try_from(partitions[0].error_code)?,
                "fetch at {fetch_offset}"
            );
        }

        // the connection survived both
        let frame = round_trip(
            &mut stream,
            ApiVersionsRequest::KEY,
            3,
            4,
            ApiVersionsRequest::default()
                .client_software_name(Some(env!("CARGO_PKG_NAME").into()))
                .client_software_version(Some(env!("CARGO_PKG_VERSION").into()))
                .into(),
        )
        .await?;

        let response = ApiVersionsResponse::try_from(frame.body)?;
        assert_eq!(i16::from(ErrorCode::None), response.error_code);

        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(|_| anyhow!("timed out after {TEST_TIMEOUT:?}"))?
}
