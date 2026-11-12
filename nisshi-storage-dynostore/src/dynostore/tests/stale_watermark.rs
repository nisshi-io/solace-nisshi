// Copyright (c) Solace Corporation
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

use std::sync::Arc;

use bytes::Bytes;
use nisshi_sans_io::fetch_response::PartitionData;
use nisshi_sans_io::{
    ErrorCode, FetchRequest, RequestInput,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    record::{Record, inflated},
};
use nisshi_storage::{Error, FetchService, FetchValidationLayer, Storage as _, Topition};
use object_store::memory::InMemory;
use rama::{Layer as _, Service as _, extensions::Extensions};

use crate::dynostore::{DynoStore, tests::init_tracing};

/// Two brokers share one bucket. A consumer whose position came from
/// broker A fetches through broker B, which still caches the high watermark
/// from before A's writes. B reads the high watermark from the bucket before
/// it answers, so a position that A's writes have reached is in range:
/// `NONE` with no records, the current high watermark, and no
/// `auto.offset.reset`. A position beyond the log is out of range, as in
/// Kafka.
#[tokio::test]
async fn fetch_ahead_of_a_stale_high_watermark() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let bucket = Arc::new(InMemory::new());
    let a = DynoStore::new("nisshi", 1, bucket.clone())?;
    let b = DynoStore::new("nisshi", 2, bucket)?;

    let name = "abc";

    _ = a
        .create_topic(
            CreatableTopic::default()
                .name(name.into())
                .num_partitions(1)
                .replication_factor(1)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(name, 0);

    // broker B reads (and caches) the high watermark before A's writes
    let stale = b.offset_stage(&topition).await?.high_watermark();

    for _ in 0..3 {
        let batch = inflated::Batch::builder()
            .record(Record::builder().value(Some(Bytes::from_static(b"abc"))))
            .build()
            .and_then(TryInto::try_into)?;

        _ = a.produce(None, &topition, batch).await?;
    }

    let high_watermark = a.offset_stage(&topition).await?.high_watermark();
    assert_eq!(stale, b.offset_stage(&topition).await?.high_watermark());
    assert!(stale < high_watermark, "{stale} < {high_watermark}");

    let fetch = |fetch_offset: i64| {
        let service = FetchValidationLayer::new().layer(FetchService { storage: b.clone() });

        async move {
            let response = service
                .serve(RequestInput {
                    request: FetchRequest::default()
                        .max_wait_ms(100)
                        .min_bytes(1)
                        .max_bytes(Some(50 * 1024))
                        .topics(Some(
                            [FetchTopic::default()
                                .topic(Some(name.into()))
                                .partitions(Some(
                                    [FetchPartition::default()
                                        .partition(0)
                                        .fetch_offset(fetch_offset)
                                        .partition_max_bytes(50 * 1024)]
                                    .into(),
                                ))]
                            .into(),
                        )),
                    extensions: Extensions::default(),
                })
                .await?;

            let topics = response.responses.unwrap_or_default();
            assert_eq!(1, topics.len());
            let partitions = topics[0].partitions.clone().unwrap_or_default();
            assert_eq!(1, partitions.len());

            Ok::<PartitionData, Error>(partitions[0].clone())
        }
    };

    let partition = fetch(high_watermark).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(partition.error_code)?);
    assert_eq!(high_watermark, partition.high_watermark);
    assert!(partition.records.is_none());

    // the high watermark that B read is now current, so what is beyond it is out of range
    let partition = fetch(high_watermark + 1).await?;
    assert_eq!(
        ErrorCode::OffsetOutOfRange,
        ErrorCode::try_from(partition.error_code)?
    );

    Ok(())
}
