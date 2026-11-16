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

use std::sync::{Arc, Mutex};

use nisshi_sans_io::{
    RequestInput,
    produce_request::{PartitionProduceData, TopicProduceData},
};
use rama::extensions::Extensions;

use super::super::*;

/// A topic that [`RecordingTopics`] served.
#[derive(Clone, Debug, PartialEq)]
struct ServedTopic {
    name: String,
    transaction_id: Option<String>,
}

impl ServedTopic {
    fn new(name: &str, transaction_id: Option<&str>) -> Self {
        Self {
            name: name.to_owned(),
            transaction_id: transaction_id.map(Into::into),
        }
    }
}

/// A topic service that records each topic that it serves, and answers the topic with its name
/// and no partitions.
#[derive(Clone, Debug, Default)]
struct RecordingTopics {
    served: Arc<Mutex<Vec<ServedTopic>>>,
    slots: Arc<Mutex<Vec<Slots>>>,
}

impl RecordingTopics {
    fn slots(&self) -> Vec<Slots> {
        self.slots
            .lock()
            .expect("a test thread panicked while it held the lock")
            .clone()
    }

    fn served(&self) -> Vec<ServedTopic> {
        self.served
            .lock()
            .expect("a test thread panicked while it held the lock")
            .clone()
    }
}

impl Service<ProduceTopic> for RecordingTopics {
    type Output = TopicProduceResponse;
    type Error = Infallible;

    async fn serve(&self, topic: ProduceTopic) -> Result<Self::Output, Self::Error> {
        self.slots
            .lock()
            .expect("a test thread panicked while it held the lock")
            .push(topic.slots);

        let topic = topic.topic;

        self.served
            .lock()
            .expect("a test thread panicked while it held the lock")
            .push(ServedTopic {
                name: topic.name.clone(),
                transaction_id: topic.transaction_id,
            });

        Ok(TopicProduceResponse::default().name(topic.name))
    }
}

/// Serves a request for each of `topics` with `topics_service`, through the layer that builds
/// each [`ValidatedTopic`] from the request.
async fn serve(
    topics_service: &RecordingTopics,
    transactional_id: Option<&str>,
    topics: &[&str],
) -> Result<Vec<String>> {
    let topic_data = topics
        .iter()
        .map(|name| {
            TopicProduceData::default()
                .name((*name).into())
                .partition_data(Some(vec![PartitionProduceData::default().index(0)]))
        })
        .collect();

    let response = ProduceRequestValidationLayer::new()
        .layer(ProduceService::from_topics(
            topics_service.clone(),
            PRODUCE_CONCURRENCY,
        ))
        .serve(RequestInput {
            request: ProduceRequest::default()
                .acks(-1)
                .transactional_id(transactional_id.map(Into::into))
                .topic_data(Some(topic_data)),
            extensions: Extensions::default(),
        })
        .await?;

    Ok(response
        .responses
        .unwrap_or_default()
        .into_iter()
        .map(|topic| topic.name)
        .collect())
}

#[tokio::test]
async fn topics_are_served_and_answered_in_the_order_of_the_request() -> Result<()> {
    let topics_service = RecordingTopics::default();

    let answered = serve(&topics_service, None, &["c", "a", "b"]).await?;

    assert_eq!(["c", "a", "b"], answered.as_slice());
    assert_eq!(
        ["c", "a", "b"],
        topics_service
            .served()
            .iter()
            .map(|served| served.name.as_str())
            .collect::<Vec<_>>()
            .as_slice()
    );

    Ok(())
}

#[tokio::test]
async fn each_topic_is_served_in_the_transaction_of_the_request() -> Result<()> {
    let topics_service = RecordingTopics::default();

    _ = serve(&topics_service, Some("transaction"), &["a", "b"]).await?;

    assert_eq!(
        [
            ServedTopic::new("a", Some("transaction")),
            ServedTopic::new("b", Some("transaction")),
        ],
        topics_service.served().as_slice()
    );

    Ok(())
}

#[tokio::test]
async fn a_request_outside_a_transaction_serves_each_topic_without_one() -> Result<()> {
    let topics_service = RecordingTopics::default();

    _ = serve(&topics_service, None, &["a"]).await?;

    assert_eq!(
        [ServedTopic::new("a", None)],
        topics_service.served().as_slice()
    );

    Ok(())
}

#[tokio::test]
async fn a_request_without_topics_serves_no_topic() -> Result<()> {
    let topics_service = RecordingTopics::default();

    let answered = serve(&topics_service, None, &[]).await?;

    assert!(answered.is_empty());
    assert!(topics_service.served().is_empty());

    Ok(())
}

/// The service builds the slots once for a request, from the concurrency that it was built with:
/// every topic of the request gets the same slots, a request of its own gets others, and the
/// validation layer decides neither.
#[tokio::test]
async fn the_service_builds_the_slots_once_for_each_request() -> Result<()> {
    let topics_service = RecordingTopics::default();

    let service = ProduceRequestValidationLayer::new()
        .layer(ProduceService::from_topics(topics_service.clone(), 2));

    for _ in 0..2 {
        _ = service
            .serve(RequestInput {
                request: ProduceRequest::default().acks(-1).topic_data(Some(
                    ["a", "b", "c"]
                        .map(|name| {
                            TopicProduceData::default()
                                .name(name.into())
                                .partition_data(Some(vec![
                                    PartitionProduceData::default().index(0),
                                ]))
                        })
                        .to_vec(),
                )),
                extensions: Extensions::default(),
            })
            .await?;
    }

    let slots = topics_service.slots();

    assert_eq!(6, slots.len());
    assert!(slots.iter().all(|each| each.count() == 2));
    assert!(slots[..3].iter().all(|each| each.is_same_as(&slots[0])));
    assert!(slots[3..].iter().all(|each| each.is_same_as(&slots[3])));
    assert!(!slots[0].is_same_as(&slots[3]));

    Ok(())
}
