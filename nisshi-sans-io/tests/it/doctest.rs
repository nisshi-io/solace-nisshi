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

use nisshi_sans_io::Error;

#[test]
fn fetch_response() -> Result<(), Error> {
    use bytes::Bytes;
    use nisshi_sans_io::{ApiKey as _, FetchResponse, Frame, record::inflated};

    let api_key = FetchResponse::KEY;
    let api_version = 16;

    let v = [
        0, 0, 0, 186, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 28, 205, 172, 195, 142, 19,
        71, 71, 182, 128, 13, 18, 65, 142, 210, 222, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 1, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 0, 0, 74, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 61, 255, 255, 255, 255, 2, 153, 143, 24, 144, 0, 0, 0, 0, 0, 0, 0,
        0, 1, 144, 238, 148, 84, 54, 0, 0, 1, 144, 238, 148, 84, 54, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 1, 22, 0, 0, 0, 1, 10, 112, 111, 105, 117, 121, 0, 3, 0, 13, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 1, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
        13, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0,
    ];

    let fetch_response = Frame::response_from_bytes(&v[..], api_key, api_version)
        .and_then(|message_frame| FetchResponse::try_from(message_frame.body))?;

    let deflated = fetch_response
        .responses
        .as_ref()
        .and_then(|topics| topics.first())
        .and_then(|topic| topic.partitions.as_ref())
        .and_then(|partitions| partitions.first())
        .and_then(|partition| partition.records.as_ref())
        .map(|record_frame| record_frame.batches.as_slice())
        .and_then(|batches| batches.first())
        .expect("deflated batch");
    assert_eq!(12, deflated.record_data.len());

    let inflated = inflated::Batch::try_from(deflated)?;

    assert_eq!(
        Some(Bytes::from_static(b"poiuy")),
        inflated
            .records
            .first()
            .and_then(|first| first.value.clone())
    );

    Ok(())
}

#[test]
fn record_builder() -> Result<(), Error> {
    use bytes::Bytes;
    use nisshi_sans_io::record::{Header, Record};

    let record = Record::builder()
        .key(Some(Bytes::from_static(b"message")))
        .value(Some(Bytes::from_static(b"hello world!")))
        .header(
            Header::builder()
                .key(Bytes::from_static(b"format"))
                .value(Bytes::from_static(b"text")),
        )
        .header(
            Header::builder()
                .key(Bytes::from_static(b"importance"))
                .value(Bytes::from_static(b"high")),
        );

    Ok(())
}

#[test]
fn build_batch() -> Result<(), Error> {
    use bytes::Bytes;
    use nisshi_sans_io::{
        BatchAttribute, Compression,
        record::{self, deflated, inflated},
    };

    let timestamp = 1_234_567_890 * 1_000;

    let message = Some(Bytes::from_static(b"hello world!"));

    let deflated = inflated::Batch::builder()
        .attributes(
            BatchAttribute::default()
                .compression(Compression::Lz4)
                .into(),
        )
        .base_timestamp(timestamp)
        .max_timestamp(timestamp)
        .record(record::Record::builder().value(message.clone()))
        .build()
        .and_then(deflated::Batch::try_from)?;

    let inflated = inflated::Batch::try_from(deflated)?;
    assert_eq!(
        message,
        inflated
            .records
            .first()
            .and_then(|first| first.value.clone())
    );

    Ok(())
}

#[test]
fn produce_request() -> Result<(), Error> {
    use bytes::Bytes;
    use nisshi_sans_io::{
        ApiKey as _, BatchAttribute, Compression, Frame, Header, ProduceRequest,
        produce_request::{PartitionProduceData, TopicProduceData},
        record::{self, deflated, inflated},
    };

    let batch = inflated::Batch::builder()
        .attributes(
            BatchAttribute::default()
                .compression(Compression::Lz4)
                .into(),
        )
        .record(record::Record::builder().value(Bytes::from_static(b"hello world!").into()))
        .build()
        .and_then(deflated::Batch::try_from)?;

    let produce_request = ProduceRequest::default()
        .topic_data(Some(
            [TopicProduceData::default()
                .name("test".into())
                .partition_data(Some(
                    [PartitionProduceData::default()
                        .index(0)
                        .records(Some(deflated::Frame {
                            batches: vec![batch],
                        }))]
                    .into(),
                ))]
            .into(),
        ))
        .into();

    let correlation_id = 12321;

    let request = Frame::request(
        Header::Request {
            api_key: ProduceRequest::KEY,
            api_version: 6,
            correlation_id,
            client_id: Some("nisshi".into()),
        },
        produce_request,
    )?;

    Ok(())
}
