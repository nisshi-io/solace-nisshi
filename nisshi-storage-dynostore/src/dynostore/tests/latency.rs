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

use std::{
    fmt::{Debug, Display},
    time::{Duration, SystemTime},
};

use crate::dynostore::{DynoStore, tests::init_tracing};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use nisshi_sans_io::{
    CreateTopicsRequest, ErrorCode, FetchRequest, NULL_TOPIC_ID, ProduceRequest, RequestInput,
    create_topics_request::CreatableTopic,
    fetch_request::{FetchPartition, FetchTopic},
    produce_request::{PartitionProduceData, TopicProduceData},
    record::{
        Record,
        deflated::{self, Frame},
        inflated,
    },
};
use nisshi_storage::{
    CreateTopicsService, CreateTopicsValidationLayer, Error, FetchService, FetchValidationLayer,
    ProduceService,
};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use rama::{Layer as _, Service as _, extensions::Extensions};
use tokio::time::sleep;
use tracing::{debug, instrument};
use url::Url;

const LOREM_IPSUM: Bytes =
    Bytes::from_static(b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do");

const EIUSMOD_TEMPOR: Bytes =
    Bytes::from_static(b"eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad");

const MINIM_VENIAM: Bytes =
    Bytes::from_static(b"minim veniam, quis nostrud exercitation ullamco laboris nisi ut");

const ALIQUIP_EX: Bytes =
    Bytes::from_static(b"aliquip ex ea commodo consequat. Duis aute irure dolor in");

const REPREHENDERIT_IN: Bytes =
    Bytes::from_static(b"reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla");

const PARIATUR_EXCEPTEUR: Bytes =
    Bytes::from_static(b"pariatur. Excepteur sint occaecat cupidatat non proident, sunt in");

const CULPA_QUI: Bytes =
    Bytes::from_static(b"culpa qui officia deserunt mollit anim id est laborum.");

/// The values that the fetch tests produce, one record to a batch, in offset order.
const MESSAGES: [Bytes; 7] = [
    LOREM_IPSUM,
    EIUSMOD_TEMPOR,
    MINIM_VENIAM,
    ALIQUIP_EX,
    REPREHENDERIT_IN,
    PARIATUR_EXCEPTEUR,
    CULPA_QUI,
];

#[derive(Clone)]
pub(crate) struct LatencyIntroducingObjectStore<O> {
    object_store: O,
    latency: Option<Duration>,
}

impl<O> LatencyIntroducingObjectStore<O>
where
    O: ObjectStore,
{
    fn new(object_store: O) -> Self {
        Self {
            object_store,
            latency: Default::default(),
        }
    }

    fn with_latency(self, latency: Option<Duration>) -> Self {
        Self { latency, ..self }
    }

    async fn latency(&self) {
        if let Some(duration) = self.latency {
            debug!(?duration);
            sleep(duration).await;
        }
    }
}

impl<O> Debug for LatencyIntroducingObjectStore<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LatencyObjectStore").finish()
    }
}

impl<O> Display for LatencyIntroducingObjectStore<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LatencyObjectStore").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for LatencyIntroducingObjectStore<O>
where
    O: ObjectStore,
{
    #[instrument(skip_all, fields(location = %location))]
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        self.latency().await;
        self.object_store.put_opts(location, payload, opts).await
    }

    #[instrument(skip_all, fields(location = %location))]
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        self.latency().await;
        self.object_store.put_multipart_opts(location, opts).await
    }

    #[instrument(skip_all, fields(%location, if_none_match = options.if_none_match), ret)]
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        self.latency().await;
        self.object_store.get_opts(location, options.clone()).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        self.object_store.delete_stream(locations)
    }

    #[instrument(skip_all, fields(prefix))]
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        self.object_store.list(prefix)
    }

    #[instrument(skip_all, fields(prefix))]
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        self.latency().await;
        self.object_store.list_with_delimiter(prefix).await
    }

    #[instrument(skip_all, fields(from = %from, to = %to))]
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        self.latency().await;
        self.object_store.copy_opts(from, to, opts).await
    }
}

fn topic_data(
    topic: &str,
    index: i32,
    builder: inflated::Builder,
) -> Result<Option<Vec<TopicProduceData>>, Error> {
    builder
        .build()
        .and_then(deflated::Batch::try_from)
        .map(|deflated| {
            let partition_data =
                PartitionProduceData::default()
                    .index(index)
                    .records(Some(Frame {
                        batches: vec![deflated],
                    }));

            Some(vec![
                TopicProduceData::default()
                    .name(topic.into())
                    .partition_data(Some(vec![partition_data])),
            ])
        })
        .map_err(Into::into)
}

#[tokio::test]
async fn empty_topic_5_000ms_max_wait() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let advertised_listener = Url::parse("tcp://localhost:9092")?;

    const LATENCY_INTRODUCED: u64 = 100;
    const MAX_WAIT_MS: i32 = 5_000;

    let object_store = LatencyIntroducingObjectStore::new(InMemory::new())
        .with_latency(Some(Duration::from_millis(LATENCY_INTRODUCED)));

    let storage =
        DynoStore::new("nisshi", 12321, object_store)?.advertised_listener(advertised_listener);

    let extensions = Extensions::default();

    let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let name = "pqr";
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = create_topic
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    let fetch = FetchValidationLayer::new().layer(FetchService {
        storage: storage.clone(),
    });

    let partition = 0;

    let response = fetch
        .serve(RequestInput {
            request: FetchRequest::default()
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(name.into()))
                        .partitions(Some(
                            [FetchPartition::default().partition(partition)].into(),
                        ))]
                    .into(),
                ))
                .max_bytes(Some(1))
                .max_wait_ms(MAX_WAIT_MS),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );

    Ok(())
}

#[tokio::test]
async fn empty_topic_50ms_max_wait() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let advertised_listener = Url::parse("tcp://localhost:9092")?;

    const LATENCY_INTRODUCED: Option<Duration> = Some(Duration::from_millis(100));
    const MAX_WAIT_MS: i32 = 50;

    let extensions = Extensions::default();

    let object_store =
        LatencyIntroducingObjectStore::new(InMemory::new()).with_latency(LATENCY_INTRODUCED);

    let storage =
        DynoStore::new("nisshi", 12321, object_store)?.advertised_listener(advertised_listener);

    let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let name = "pqr";
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = create_topic
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    let fetch = FetchValidationLayer::new().layer(FetchService {
        storage: storage.clone(),
    });

    let partition = 0;

    let response = fetch
        .serve(RequestInput {
            request: FetchRequest::default()
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(name.into()))
                        .partitions(Some(
                            [FetchPartition::default().partition(partition)].into(),
                        ))]
                    .into(),
                ))
                .max_bytes(Some(1))
                .max_wait_ms(MAX_WAIT_MS),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );

    Ok(())
}

#[tokio::test]
async fn fetch_1_min_bytes_5_000ms_max_wait() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let advertised_listener = Url::parse("tcp://localhost:9092")?;

    const LATENCY_INTRODUCED: Duration = Duration::from_millis(100);
    const MAX_WAIT: Duration = Duration::from_secs(5);

    let extensions = Extensions::default();

    let object_store =
        LatencyIntroducingObjectStore::new(InMemory::new()).with_latency(Some(LATENCY_INTRODUCED));

    let storage =
        DynoStore::new("nisshi", 12321, object_store)?.advertised_listener(advertised_listener);

    let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let produce = ProduceService {
        storage: storage.clone(),
    };

    let fetch = FetchValidationLayer::new().layer(FetchService {
        storage: storage.clone(),
    });

    let name = "pqr";
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = create_topic
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    const PARTITION: i32 = 0;

    for value in MESSAGES {
        let response = produce
            .serve(RequestInput {
                request: ProduceRequest::default().topic_data(topic_data(
                    name,
                    PARTITION,
                    inflated::Batch::builder().record(Record::builder().value(value.into())),
                )?),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(ErrorCode::None, partitions[0].error_code.try_into()?);
    }

    let started_at = SystemTime::now();
    let response = fetch
        .serve(RequestInput {
            request: FetchRequest::default()
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(name.into()))
                        .partitions(Some(
                            [FetchPartition::default().partition(PARTITION)].into(),
                        ))]
                    .into(),
                ))
                .min_bytes(1)
                .max_bytes(Some(52_428_800))
                .max_wait_ms(MAX_WAIT.as_millis() as i32),
            extensions: extensions.clone(),
        })
        .await?;
    let elapsed = started_at.elapsed()?;
    assert!(
        elapsed > LATENCY_INTRODUCED,
        "elapsed: {elapsed:?}, with latency introduced of: {LATENCY_INTRODUCED:?}, and max wait: {MAX_WAIT:?}"
    );
    assert!(
        elapsed < MAX_WAIT,
        "elapsed: {elapsed:?}, with latency introduced of: {LATENCY_INTRODUCED:?}, and max wait: {MAX_WAIT:?}"
    );

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );

    let batches = partitions
        .first()
        .and_then(|partition_data| partition_data.records.as_ref())
        .map(|frame| &frame.batches[..])
        .unwrap_or_default();

    assert_eq!(7, batches.len());

    let inflated = batches
        .first()
        .map(inflated::Batch::try_from)
        .transpose()?
        .unwrap();

    assert_eq!(1, inflated.records.len());

    assert_eq!(
        Some(LOREM_IPSUM),
        inflated
            .records
            .first()
            .cloned()
            .and_then(|record| record.value)
    );

    let inflated = batches
        .get(1)
        .map(inflated::Batch::try_from)
        .transpose()?
        .unwrap();

    assert_eq!(1, inflated.records.len());

    assert_eq!(
        Some(EIUSMOD_TEMPOR),
        inflated
            .records
            .first()
            .cloned()
            .and_then(|record| record.value)
    );

    let inflated = batches
        .get(2)
        .map(inflated::Batch::try_from)
        .transpose()?
        .unwrap();

    assert_eq!(1, inflated.records.len());

    assert_eq!(
        Some(MINIM_VENIAM),
        inflated
            .records
            .first()
            .cloned()
            .and_then(|record| record.value)
    );

    let inflated = batches
        .get(3)
        .map(inflated::Batch::try_from)
        .transpose()?
        .unwrap();

    assert_eq!(1, inflated.records.len());

    assert_eq!(
        Some(ALIQUIP_EX),
        inflated
            .records
            .first()
            .cloned()
            .and_then(|record| record.value)
    );

    let inflated = batches
        .get(4)
        .map(inflated::Batch::try_from)
        .transpose()?
        .unwrap();

    assert_eq!(1, inflated.records.len());

    assert_eq!(
        Some(REPREHENDERIT_IN),
        inflated
            .records
            .first()
            .cloned()
            .and_then(|record| record.value)
    );

    let inflated = batches
        .get(5)
        .map(inflated::Batch::try_from)
        .transpose()?
        .unwrap();

    assert_eq!(1, inflated.records.len());

    assert_eq!(
        Some(PARIATUR_EXCEPTEUR),
        inflated
            .records
            .first()
            .cloned()
            .and_then(|record| record.value)
    );

    let inflated = batches
        .get(6)
        .map(inflated::Batch::try_from)
        .transpose()?
        .unwrap();

    assert_eq!(1, inflated.records.len());

    assert_eq!(
        Some(CULPA_QUI),
        inflated
            .records
            .first()
            .cloned()
            .and_then(|record| record.value)
    );

    Ok(())
}

/// A `max_wait` as short as the object store's latency does not cut the read short: the client's
/// byte limits, not its `max_wait`, decide how much a fetch returns, as in Kafka.
#[tokio::test]
async fn fetch_1_min_bytes_max_wait_of_1x_latency_reads_every_batch() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let advertised_listener = Url::parse("tcp://localhost:9092")?;

    let extensions = Extensions::default();

    const LATENCY_INTRODUCED: Duration = Duration::from_millis(100);
    let max_wait = 1 * LATENCY_INTRODUCED;

    let object_store =
        LatencyIntroducingObjectStore::new(InMemory::new()).with_latency(Some(LATENCY_INTRODUCED));

    let storage =
        DynoStore::new("nisshi", 12321, object_store)?.advertised_listener(advertised_listener);

    let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let produce = ProduceService {
        storage: storage.clone(),
    };

    let fetch = FetchValidationLayer::new().layer(FetchService {
        storage: storage.clone(),
    });

    let name = "pqr";
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = create_topic
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    const PARTITION: i32 = 0;

    for value in MESSAGES {
        let response = produce
            .serve(RequestInput {
                request: ProduceRequest::default().topic_data(topic_data(
                    name,
                    PARTITION,
                    inflated::Batch::builder().record(Record::builder().value(value.into())),
                )?),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(ErrorCode::None, partitions[0].error_code.try_into()?);
    }

    let started_at = SystemTime::now();
    let response = fetch
        .serve(RequestInput {
            request: FetchRequest::default()
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(name.into()))
                        .partitions(Some(
                            [FetchPartition::default().partition(PARTITION)].into(),
                        ))]
                    .into(),
                ))
                .min_bytes(1)
                .max_bytes(Some(52_428_800))
                .max_wait_ms(max_wait.as_millis() as i32),
            extensions: extensions.clone(),
        })
        .await?;
    let elapsed = started_at.elapsed()?;
    assert!(
        elapsed > LATENCY_INTRODUCED,
        "elapsed: {elapsed:?}, with latency introduced of: {LATENCY_INTRODUCED:?}, and max wait: {max_wait:?}"
    );
    assert!(
        elapsed > max_wait,
        "elapsed: {elapsed:?}, with latency introduced of: {LATENCY_INTRODUCED:?}, and max wait: {max_wait:?}"
    );

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );

    let batches = partitions
        .first()
        .and_then(|partition_data| partition_data.records.as_ref())
        .map(|frame| &frame.batches[..])
        .unwrap_or_default();

    assert_eq!(MESSAGES.len(), batches.len());

    let values = batches
        .iter()
        .map(|batch| {
            inflated::Batch::try_from(batch).map(|inflated| {
                assert_eq!(1, inflated.records.len());
                inflated
                    .records
                    .first()
                    .cloned()
                    .and_then(|record| record.value)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(MESSAGES.map(Some).to_vec(), values);

    Ok(())
}

/// A `max_wait` as short as the object store's latency does not cut the read short: the client's
/// byte limits, not its `max_wait`, decide how much a fetch returns, as in Kafka.
#[tokio::test]
async fn fetch_1_min_bytes_max_wait_of_2x_latency_reads_every_batch() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let advertised_listener = Url::parse("tcp://localhost:9092")?;

    let extensions = Extensions::default();

    const LATENCY_INTRODUCED: Duration = Duration::from_millis(100);
    let max_wait = 2 * LATENCY_INTRODUCED;

    let object_store =
        LatencyIntroducingObjectStore::new(InMemory::new()).with_latency(Some(LATENCY_INTRODUCED));

    let storage =
        DynoStore::new("nisshi", 12321, object_store)?.advertised_listener(advertised_listener);

    let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let produce = ProduceService {
        storage: storage.clone(),
    };

    let fetch = FetchValidationLayer::new().layer(FetchService {
        storage: storage.clone(),
    });

    let name = "pqr";
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = create_topic
        .serve(RequestInput {
            request: CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
            extensions: extensions.clone(),
        })
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    const PARTITION: i32 = 0;

    for value in MESSAGES {
        let response = produce
            .serve(RequestInput {
                request: ProduceRequest::default().topic_data(topic_data(
                    name,
                    PARTITION,
                    inflated::Batch::builder().record(Record::builder().value(value.into())),
                )?),
                extensions: extensions.clone(),
            })
            .await?;

        let topics = response.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(ErrorCode::None, partitions[0].error_code.try_into()?);
    }

    let started_at = SystemTime::now();
    let response = fetch
        .serve(RequestInput {
            request: FetchRequest::default()
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(name.into()))
                        .partitions(Some(
                            [FetchPartition::default().partition(PARTITION)].into(),
                        ))]
                    .into(),
                ))
                .min_bytes(1)
                .max_bytes(Some(52_428_800))
                .max_wait_ms(max_wait.as_millis() as i32),
            extensions: extensions.clone(),
        })
        .await?;
    let elapsed = started_at.elapsed()?;
    assert!(
        elapsed > LATENCY_INTRODUCED,
        "elapsed: {elapsed:?}, with latency introduced of: {LATENCY_INTRODUCED:?}, and max wait: {max_wait:?}"
    );
    assert!(
        elapsed > max_wait,
        "elapsed: {elapsed:?}, with latency introduced of: {LATENCY_INTRODUCED:?}, and max wait: {max_wait:?}"
    );

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );

    let batches = partitions
        .first()
        .and_then(|partition_data| partition_data.records.as_ref())
        .map(|frame| &frame.batches[..])
        .unwrap_or_default();

    assert_eq!(MESSAGES.len(), batches.len());

    let values = batches
        .iter()
        .map(|batch| {
            inflated::Batch::try_from(batch).map(|inflated| {
                assert_eq!(1, inflated.records.len());
                inflated
                    .records
                    .first()
                    .cloned()
                    .and_then(|record| record.value)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(MESSAGES.map(Some).to_vec(), values);

    Ok(())
}

#[tokio::test]
async fn fetch_max_bytes_for_1_message_5_000ms_max_wait() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let advertised_listener = Url::parse("tcp://localhost:9092")?;

    const LATENCY_INTRODUCED: Duration = Duration::from_millis(100);
    const MAX_WAIT: Duration = Duration::from_secs(5);

    let object_store =
        LatencyIntroducingObjectStore::new(InMemory::new()).with_latency(Some(LATENCY_INTRODUCED));

    let storage =
        DynoStore::new("nisshi", 12321, object_store)?.advertised_listener(advertised_listener);

    let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let produce = ProduceService {
        storage: storage.clone(),
    };

    let fetch = FetchValidationLayer::new().layer(FetchService {
        storage: storage.clone(),
    });

    let name = "pqr";
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = create_topic
        .serve(
            CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
        )
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    const PARTITION: i32 = 0;

    for value in MESSAGES {
        let response = produce
            .serve(ProduceRequest::default().topic_data(topic_data(
                name,
                PARTITION,
                inflated::Batch::builder().record(Record::builder().value(value.into())),
            )?))
            .await?;

        let topics = response.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(ErrorCode::None, partitions[0].error_code.try_into()?);
    }

    let started_at = SystemTime::now();
    let response = fetch
        .serve(
            FetchRequest::default()
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(name.into()))
                        .partitions(Some(
                            [FetchPartition::default().partition(PARTITION)].into(),
                        ))]
                    .into(),
                ))
                .max_bytes(Some(LOREM_IPSUM.len() as i32))
                .max_wait_ms(MAX_WAIT.as_millis() as i32),
        )
        .await?;
    let elapsed = started_at.elapsed()?;
    assert!(
        elapsed > LATENCY_INTRODUCED,
        "elapsed: {elapsed:?}, with latency introduced of: {LATENCY_INTRODUCED:?}, and max wait: {MAX_WAIT:?}"
    );
    assert!(
        elapsed < MAX_WAIT,
        "elapsed: {elapsed:?}, with latency introduced of: {LATENCY_INTRODUCED:?}, and max wait: {MAX_WAIT:?}"
    );

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert!(partitions[0].records.is_some());
    assert_eq!(1, partitions[0].records.as_ref().unwrap().batches.len());

    let batches = partitions
        .first()
        .and_then(|partition_data| partition_data.records.as_ref())
        .map(|frame| &frame.batches[..])
        .unwrap_or_default();

    assert_eq!(1, batches.len());

    let inflated = batches
        .first()
        .map(inflated::Batch::try_from)
        .transpose()?
        .unwrap();

    assert_eq!(1, inflated.records.len());

    assert_eq!(
        Some(LOREM_IPSUM),
        inflated
            .records
            .first()
            .cloned()
            .and_then(|record| record.value)
    );

    Ok(())
}

#[tokio::test]
async fn fetch_max_bytes_for_1_message_50ms_max_wait() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let advertised_listener = Url::parse("tcp://localhost:9092")?;

    const LATENCY_INTRODUCED: Duration = Duration::from_millis(100);
    const MAX_WAIT: Duration = Duration::from_millis(50);

    let object_store =
        LatencyIntroducingObjectStore::new(InMemory::new()).with_latency(Some(LATENCY_INTRODUCED));

    let storage =
        DynoStore::new("nisshi", 12321, object_store)?.advertised_listener(advertised_listener);

    let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let produce = ProduceService {
        storage: storage.clone(),
    };

    let fetch = FetchValidationLayer::new().layer(FetchService {
        storage: storage.clone(),
    });

    let name = "pqr";
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = create_topic
        .serve(
            CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
        )
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    const PARTITION: i32 = 0;

    for value in MESSAGES {
        let response = produce
            .serve(ProduceRequest::default().topic_data(topic_data(
                name,
                PARTITION,
                inflated::Batch::builder().record(Record::builder().value(value.into())),
            )?))
            .await?;

        let topics = response.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(ErrorCode::None, partitions[0].error_code.try_into()?);
    }

    let started_at = SystemTime::now();
    let response = fetch
        .serve(
            FetchRequest::default()
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(name.into()))
                        .partitions(Some(
                            [FetchPartition::default().partition(PARTITION)].into(),
                        ))]
                    .into(),
                ))
                .max_bytes(Some(LOREM_IPSUM.len() as i32))
                .max_wait_ms(MAX_WAIT.as_millis() as i32),
        )
        .await?;

    let elapsed = started_at.elapsed()?;
    assert!(
        elapsed > MAX_WAIT,
        "elapsed: {elapsed:?}, with latency introduced of: {LATENCY_INTRODUCED:?}, and max wait: {MAX_WAIT:?}"
    );
    assert!(
        elapsed > LATENCY_INTRODUCED,
        "elapsed: {elapsed:?}, with latency introduced of: {LATENCY_INTRODUCED:?}, and max wait: {MAX_WAIT:?}"
    );

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );
    assert!(partitions[0].records.is_some());
    assert_eq!(1, partitions[0].records.as_ref().unwrap().batches.len());

    let batches = partitions
        .first()
        .and_then(|partition_data| partition_data.records.as_ref())
        .map(|frame| &frame.batches[..])
        .unwrap_or_default();

    assert_eq!(1, batches.len());

    let inflated = batches
        .first()
        .map(inflated::Batch::try_from)
        .transpose()?
        .unwrap();

    assert_eq!(1, inflated.records.len());

    assert_eq!(
        Some(LOREM_IPSUM),
        inflated
            .records
            .first()
            .cloned()
            .and_then(|record| record.value)
    );

    Ok(())
}

#[tokio::test]
async fn fetch_max_bytes_for_2_messages_5_000ms_max_wait() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let advertised_listener = Url::parse("tcp://localhost:9092")?;

    const LATENCY_INTRODUCED: Duration = Duration::from_millis(100);
    const MAX_WAIT: Duration = Duration::from_secs(5);

    let object_store =
        LatencyIntroducingObjectStore::new(InMemory::new()).with_latency(Some(LATENCY_INTRODUCED));

    let storage =
        DynoStore::new("nisshi", 12321, object_store)?.advertised_listener(advertised_listener);

    let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let produce = ProduceService {
        storage: storage.clone(),
    };

    let fetch = FetchValidationLayer::new().layer(FetchService {
        storage: storage.clone(),
    });

    let name = "pqr";
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = create_topic
        .serve(
            CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
        )
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    const PARTITION: i32 = 0;

    for value in MESSAGES {
        let response = produce
            .serve(ProduceRequest::default().topic_data(topic_data(
                name,
                PARTITION,
                inflated::Batch::builder().record(Record::builder().value(value.into())),
            )?))
            .await?;

        let topics = response.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(ErrorCode::None, partitions[0].error_code.try_into()?);
    }

    let started_at = SystemTime::now();
    let response = fetch
        .serve(
            FetchRequest::default()
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(name.into()))
                        .partitions(Some(
                            [FetchPartition::default().partition(PARTITION)].into(),
                        ))]
                    .into(),
                ))
                .max_bytes(Some((LOREM_IPSUM.len() + EIUSMOD_TEMPOR.len()) as i32))
                .max_wait_ms(MAX_WAIT.as_millis() as i32),
        )
        .await?;
    let elapsed = started_at.elapsed()?;
    assert!(
        elapsed > LATENCY_INTRODUCED,
        "elapsed: {elapsed:?}, with latency introduced of: {LATENCY_INTRODUCED:?}, and max wait: {MAX_WAIT:?}"
    );
    assert!(
        elapsed < MAX_WAIT,
        "elapsed: {elapsed:?}, with latency introduced of: {LATENCY_INTRODUCED:?}, and max wait: {MAX_WAIT:?}"
    );

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );

    let batches = partitions
        .first()
        .and_then(|partition_data| partition_data.records.as_ref())
        .map(|frame| &frame.batches[..])
        .unwrap_or_default();

    assert_eq!(2, batches.len());

    let inflated = batches
        .first()
        .map(inflated::Batch::try_from)
        .transpose()?
        .unwrap();

    assert_eq!(1, inflated.records.len());

    assert_eq!(
        Some(LOREM_IPSUM),
        inflated
            .records
            .first()
            .cloned()
            .and_then(|record| record.value)
    );

    let inflated = batches
        .get(1)
        .map(inflated::Batch::try_from)
        .transpose()?
        .unwrap();

    assert_eq!(1, inflated.records.len());

    assert_eq!(
        Some(EIUSMOD_TEMPOR),
        inflated
            .records
            .first()
            .cloned()
            .and_then(|record| record.value)
    );

    Ok(())
}

/// A `max_wait` shorter than the object store's latency does not cut the read short: the fetch
/// returns as many batches as `max_bytes` allows, as in Kafka.
#[tokio::test]
async fn fetch_max_bytes_for_2_messages_50ms_max_wait_reads_up_to_max_bytes() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let advertised_listener = Url::parse("tcp://localhost:9092")?;

    const LATENCY_INTRODUCED: Duration = Duration::from_millis(100);
    const MAX_WAIT: Duration = Duration::from_millis(50);

    let object_store =
        LatencyIntroducingObjectStore::new(InMemory::new()).with_latency(Some(LATENCY_INTRODUCED));

    let storage =
        DynoStore::new("nisshi", 12321, object_store)?.advertised_listener(advertised_listener);

    let create_topic = CreateTopicsValidationLayer::new().layer(CreateTopicsService {
        storage: storage.clone(),
    });

    let produce = ProduceService {
        storage: storage.clone(),
    };

    let fetch = FetchValidationLayer::new().layer(FetchService {
        storage: storage.clone(),
    });

    let name = "pqr";
    let num_partitions = 5;
    let replication_factor = 3;
    let assignments = Some([].into());
    let configs = Some([].into());

    let response = create_topic
        .serve(
            CreateTopicsRequest::default()
                .topics(Some(vec![
                    CreatableTopic::default()
                        .name(name.into())
                        .num_partitions(num_partitions)
                        .replication_factor(replication_factor)
                        .assignments(assignments)
                        .configs(configs),
                ]))
                .validate_only(Some(false)),
        )
        .await?;

    let topics = response.topics.unwrap_or_default();

    assert_eq!(1, topics.len());
    assert_eq!(name, topics[0].name.as_str());
    assert_ne!(Some(NULL_TOPIC_ID), topics[0].topic_id);
    assert_eq!(Some(5), topics[0].num_partitions);
    assert_eq!(Some(3), topics[0].replication_factor);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(topics[0].error_code)?);

    const PARTITION: i32 = 0;

    for value in MESSAGES {
        let response = produce
            .serve(ProduceRequest::default().topic_data(topic_data(
                name,
                PARTITION,
                inflated::Batch::builder().record(Record::builder().value(value.into())),
            )?))
            .await?;

        let topics = response.responses.as_deref().unwrap_or_default();
        assert_eq!(1, topics.len());

        let partitions = topics[0].partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partitions.len());
        assert_eq!(ErrorCode::None, partitions[0].error_code.try_into()?);
    }

    let started_at = SystemTime::now();
    let response = fetch
        .serve(
            FetchRequest::default()
                .topics(Some(
                    [FetchTopic::default()
                        .topic(Some(name.into()))
                        .partitions(Some(
                            [FetchPartition::default().partition(PARTITION)].into(),
                        ))]
                    .into(),
                ))
                .max_bytes(Some((LOREM_IPSUM.len() + EIUSMOD_TEMPOR.len()) as i32))
                .max_wait_ms(MAX_WAIT.as_millis() as i32),
        )
        .await?;
    let elapsed = started_at.elapsed()?;

    assert!(
        elapsed > LATENCY_INTRODUCED,
        "elapsed: {elapsed:?}, with latency introduced of: {LATENCY_INTRODUCED:?}, and max wait: {MAX_WAIT:?}"
    );

    // exceed the maximum wait in order to progress the fetcher:
    assert!(
        elapsed > MAX_WAIT,
        "elapsed: {elapsed:?}, with latency introduced of: {LATENCY_INTRODUCED:?}, and max wait: {MAX_WAIT:?}"
    );

    let topics = response.responses.as_deref().unwrap_or_default();
    assert_eq!(1, topics.len());
    let partitions = topics[0].partitions.as_deref().unwrap_or_default();
    assert_eq!(1, partitions.len());
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(partitions[0].error_code)?
    );

    let batches = partitions
        .first()
        .and_then(|partition_data| partition_data.records.as_ref())
        .map(|frame| &frame.batches[..])
        .unwrap_or_default();

    assert_eq!(2, batches.len());

    let values = batches
        .iter()
        .map(|batch| {
            inflated::Batch::try_from(batch).map(|inflated| {
                assert_eq!(1, inflated.records.len());
                inflated
                    .records
                    .first()
                    .cloned()
                    .and_then(|record| record.value)
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    assert_eq!(
        MESSAGES[..2].iter().cloned().map(Some).collect::<Vec<_>>(),
        values
    );

    Ok(())
}
