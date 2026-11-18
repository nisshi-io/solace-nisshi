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

use std::collections::BTreeMap;

use bytes::Bytes;
use nisshi_sans_io::{
    ApiKey as _, FetchRequest, FetchResponse, Frame, FrameInput, Header, MetadataRequest,
    MetadataResponse, ProduceRequest, ProduceResponse, RequestInput,
    fetch_request::{FetchPartition, FetchTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    record::deflated::{Batch, Frame as RecordBatch},
};
use nisshi_service::{
    ConnectionStats, FetchStatsLayer, FrameRouteService, ProduceStatsLayer, ProducedTraffic,
    RequestLayer, ResponseService,
};
use rama::{
    Layer as _, Service as _,
    extensions::{Extensions, ExtensionsRef as _},
};

use crate::common::Error;

fn metadata_frame(client_id: Option<&str>) -> Frame {
    Frame {
        header: Header::Request {
            api_key: MetadataRequest::KEY,
            api_version: 12,
            correlation_id: 0,
            client_id: client_id.map(str::to_owned),
        },
        body: MetadataRequest::default().into(),
        size: 0,
    }
}

fn metadata_router() -> Result<FrameRouteService<Error>, Error> {
    FrameRouteService::<Error>::builder()
        .with_service(
            RequestLayer::<MetadataRequest>::new().into_layer(ResponseService::new(|_| {
                Ok(MetadataResponse::default()
                    .brokers(Some([].into()))
                    .topics(Some([].into())))
            })),
        )
        .and_then(|builder| builder.build())
        .map_err(Into::into)
}

#[tokio::test]
async fn router_counts_each_request_by_api_key_and_records_the_client_id() -> Result<(), Error> {
    let router = metadata_router()?;
    let extensions = Extensions::default();
    let stats = ConnectionStats::new();
    _ = extensions.insert(stats.clone());

    for client_id in [Some("bench-producer-0"), None] {
        _ = router
            .serve(FrameInput {
                frame: metadata_frame(client_id),
                extensions: extensions.clone(),
            })
            .await?;
    }

    let summary = stats.summary();

    assert_eq!(Some("bench-producer-0"), summary.client_id.as_deref());
    assert_eq!(
        BTreeMap::from([(MetadataRequest::KEY, 2)]),
        summary.requests
    );

    Ok(())
}

#[tokio::test]
async fn router_serves_a_connection_that_has_no_stats() -> Result<(), Error> {
    let frame = metadata_router()?
        .serve(FrameInput {
            frame: metadata_frame(Some("nisshi")),
            extensions: Extensions::default(),
        })
        .await?;

    let response: MetadataResponse = frame.body.try_into()?;
    assert!(response.brokers.is_some());

    Ok(())
}

#[tokio::test]
async fn produce_layer_records_topic_partitions_records_and_bytes_and_passes_the_request_on()
-> Result<(), Error> {
    let service =
        ProduceStatsLayer.into_layer(ResponseService::new(|req: RequestInput<ProduceRequest>| {
            // The layer hands the same extensions on, so the handler still sees the connection's.
            assert!(req.extensions().get_ref::<ConnectionStats>().is_some());
            Ok::<_, Error>(ProduceResponse::default())
        }));

    let batch = |record_count, len| RecordBatch {
        batches: vec![Batch {
            record_count,
            record_data: Bytes::from(vec![0; len]),
            ..Batch::default()
        }],
    };

    let request = ProduceRequest::default().topic_data(Some(
        [TopicProduceData::default()
            .name("t".into())
            .partition_data(Some(
                [
                    PartitionProduceData::default()
                        .index(0)
                        .records(Some(batch(5, 100))),
                    PartitionProduceData::default().index(3).records(None),
                ]
                .into(),
            ))]
        .into(),
    ));

    let extensions = Extensions::default();
    let stats = ConnectionStats::new();
    _ = extensions.insert(stats.clone());

    for _ in 0..2 {
        _ = service
            .serve(RequestInput {
                request: request.clone(),
                extensions: extensions.clone(),
            })
            .await?;
    }

    let summary = stats.summary();

    assert_eq!(
        BTreeMap::from([
            (
                ("t".to_owned(), 0),
                ProducedTraffic {
                    requests: 2,
                    records: 10,
                    bytes: 200
                }
            ),
            (
                ("t".to_owned(), 3),
                ProducedTraffic {
                    requests: 2,
                    records: 0,
                    bytes: 0
                }
            ),
        ]),
        summary.produce
    );

    Ok(())
}

#[tokio::test]
async fn fetch_layer_records_topic_partitions_by_name_or_by_topic_id() -> Result<(), Error> {
    let service =
        FetchStatsLayer.into_layer(ResponseService::new(|_: RequestInput<FetchRequest>| {
            Ok::<_, Error>(FetchResponse::default())
        }));

    let id = [7; 16];

    let request = FetchRequest::default().topics(Some(
        [
            FetchTopic::default()
                .topic(Some("t".into()))
                .partitions(Some([FetchPartition::default().partition(2)].into())),
            FetchTopic::default()
                .topic(None)
                .topic_id(Some(id))
                .partitions(Some([FetchPartition::default().partition(5)].into())),
        ]
        .into(),
    ));

    let extensions = Extensions::default();
    let stats = ConnectionStats::new();
    _ = extensions.insert(stats.clone());

    _ = service
        .serve(RequestInput {
            request,
            extensions,
        })
        .await?;

    assert_eq!(
        BTreeMap::from([
            (("t".to_owned(), 2), 1),
            (("07070707-0707-0707-0707-070707070707".to_owned(), 5), 1),
        ]),
        stats.summary().fetch
    );

    Ok(())
}

#[tokio::test]
async fn stats_layers_pass_a_request_on_when_the_connection_has_no_stats() -> Result<(), Error> {
    let produce =
        ProduceStatsLayer.into_layer(ResponseService::new(|_: RequestInput<ProduceRequest>| {
            Ok::<_, Error>(ProduceResponse::default())
        }));

    _ = produce
        .serve(RequestInput::from(ProduceRequest::default()))
        .await?;

    Ok(())
}
