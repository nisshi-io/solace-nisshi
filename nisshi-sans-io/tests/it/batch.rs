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

use crate::common::init_tracing;
use bytes::Bytes;
use nisshi_sans_io::{
    Ack, BatchAttribute, Compression, Frame, Header, ProduceRequest, Result,
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{Record, inflated},
};
use tracing::debug;

fn produce_request(compression: Compression) -> Result<()> {
    let _guard = init_tracing()?;

    let header = Header::Request {
        api_key: 0,
        api_version: 9,
        correlation_id: 6,
        client_id: Some("console-producer".into()),
    };

    let value = Bytes::from_static(&[0u8; 8_192]);

    let batch = inflated::Batch::builder()
        .attributes(BatchAttribute::default().compression(compression).into())
        .record(Record::builder().value(value.clone().into()))
        .producer_id(-1)
        .producer_epoch(-1)
        .build()?;

    let request = ProduceRequest::default()
        .transactional_id(None)
        .acks(Ack::None.into())
        .timeout_ms(1_500)
        .topic_data(Some(
            [TopicProduceData::default()
                .name("test".into())
                .partition_data(Some(
                    [PartitionProduceData::default().index(0).records(Some(
                        inflated::Frame {
                            batches: [batch].into(),
                        }
                        .try_into()?,
                    ))]
                    .into(),
                ))]
            .into(),
        ));

    let encoded = Frame::request(header, request.into())?;

    debug!(encoded.len = encoded.len());
    assert!(
        value.len() > encoded.len(),
        "value_len: {}, encoded_len: {}",
        value.len(),
        encoded.len()
    );

    let decoded = Frame::request_from_bytes(encoded).map(|decoded| {
        as_records(decoded)
            .into_iter()
            .filter_map(|record| record.value)
            .collect::<Vec<_>>()
    })?;

    assert_eq!(vec![value], decoded);

    Ok(())
}

#[test]
fn produce_request_gzip() -> Result<()> {
    produce_request(Compression::Gzip)
}

#[test]
fn produce_request_lz4() -> Result<()> {
    produce_request(Compression::Lz4)
}

#[test]
fn produce_request_zstd() -> Result<()> {
    produce_request(Compression::Zstd)
}

#[test]
fn produce_request_snappy() -> Result<()> {
    produce_request(Compression::Snappy)
}

fn as_records(frame: Frame) -> Vec<Record> {
    frame
        .body
        .as_produce_request()
        .map(|request| {
            request
                .topic_data
                .unwrap_or_default()
                .into_iter()
                .flat_map(|topic| {
                    topic
                        .partition_data
                        .unwrap_or_default()
                        .into_iter()
                        .flat_map(|partition| {
                            partition
                                .records
                                .map(|frame| frame.batches)
                                .unwrap_or_default()
                        })
                })
                .flat_map(|deflated| {
                    inflated::Batch::try_from(deflated)
                        .map(|inflated| inflated.records)
                        .ok()
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}
