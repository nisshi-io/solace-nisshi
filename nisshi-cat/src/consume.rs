// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use std::marker::PhantomData;

use crate::{Error, Result};

use bytes::Bytes;
use futures::SinkExt;
use nisshi_client::{Client, ConnectionManager};
use nisshi_sans_io::{
    ErrorCode, MetadataRequest, NULL_TOPIC_ID,
    fetch_request::{FetchPartition, FetchRequest, FetchTopic, ReplicaState},
    fetch_response,
    metadata_request::MetadataRequestTopic,
    record::inflated,
};
use nisshi_schema::{AsJsonValue, Registry};
use serde_json::{Value, json};
use tokio::io::stdout;
use tokio_util::codec::{FramedWrite, LinesCodec};
use tracing::debug;
use url::Url;

#[derive(Clone, Debug, Default)]
pub struct Builder<B, T, P, S> {
    broker: B,
    topic: T,
    partition: P,
    schema_registry: S,
    max_wait_time_ms: i32,
    min_bytes: i32,
    max_bytes: Option<i32>,
    fetch_offset: i64,
    partition_max_bytes: i32,
}

pub(crate) type PhantomBuilder =
    Builder<PhantomData<Url>, PhantomData<String>, PhantomData<i32>, PhantomData<Option<Url>>>;

impl<B, T, P, S> Builder<B, T, P, S> {
    pub fn broker(self, broker: impl Into<Url>) -> Builder<Url, T, P, S> {
        Builder {
            broker: broker.into(),
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            max_wait_time_ms: self.max_wait_time_ms,
            min_bytes: self.min_bytes,
            max_bytes: self.max_bytes,
            fetch_offset: self.fetch_offset,
            partition_max_bytes: self.partition_max_bytes,
        }
    }

    pub fn topic(self, topic: impl Into<String>) -> Builder<B, String, P, S> {
        Builder {
            broker: self.broker,
            topic: topic.into(),
            partition: self.partition,
            schema_registry: self.schema_registry,
            max_wait_time_ms: self.max_wait_time_ms,
            min_bytes: self.min_bytes,
            max_bytes: self.max_bytes,
            fetch_offset: self.fetch_offset,
            partition_max_bytes: self.partition_max_bytes,
        }
    }

    pub fn partition(self, partition: i32) -> Builder<B, T, i32, S> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition,
            schema_registry: self.schema_registry,
            max_wait_time_ms: self.max_wait_time_ms,
            min_bytes: self.min_bytes,
            max_bytes: self.max_bytes,
            fetch_offset: self.fetch_offset,
            partition_max_bytes: self.partition_max_bytes,
        }
    }

    pub fn schema_registry(self, schema_registry: Option<Url>) -> Builder<B, T, P, Option<Url>> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry,
            max_wait_time_ms: self.max_wait_time_ms,
            min_bytes: self.min_bytes,
            max_bytes: self.max_bytes,
            fetch_offset: self.fetch_offset,
            partition_max_bytes: self.partition_max_bytes,
        }
    }

    pub fn max_wait_time_ms(self, max_wait_time_ms: i32) -> Self {
        Self {
            max_wait_time_ms,
            ..self
        }
    }

    pub fn min_bytes(self, min_bytes: i32) -> Self {
        Self { min_bytes, ..self }
    }

    pub fn max_bytes(self, max_bytes: Option<i32>) -> Self {
        Self { max_bytes, ..self }
    }

    pub fn fetch_offset(self, fetch_offset: i64) -> Self {
        Self {
            fetch_offset,
            ..self
        }
    }

    pub fn partition_max_bytes(self, partition_max_bytes: i32) -> Self {
        Self {
            partition_max_bytes,
            ..self
        }
    }
}

impl Builder<Url, String, i32, Option<Url>> {
    pub fn build(self) -> super::Cat {
        super::Cat::Consume(Box::new(Configuration::from(self)))
    }
}

impl From<Builder<Url, String, i32, Option<Url>>> for Configuration {
    fn from(builder: Builder<Url, String, i32, Option<Url>>) -> Self {
        Self {
            broker: builder.broker,
            topic: builder.topic,
            partition: builder.partition,
            schema_registry: builder.schema_registry,
            max_wait_time_ms: builder.max_wait_time_ms,
            min_bytes: builder.min_bytes,
            max_bytes: builder.max_bytes,
            fetch_offset: builder.fetch_offset,
            partition_max_bytes: builder.partition_max_bytes,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Configuration {
    broker: Url,
    topic: String,
    partition: i32,
    schema_registry: Option<Url>,
    max_wait_time_ms: i32,
    min_bytes: i32,
    max_bytes: Option<i32>,
    fetch_offset: i64,
    partition_max_bytes: i32,
}

#[derive(Clone, Debug)]
pub(crate) struct Consume {
    configuration: Configuration,
    registry: Option<Registry>,
}

impl TryFrom<Configuration> for Consume {
    type Error = Error;

    fn try_from(configuration: Configuration) -> Result<Self, Self::Error> {
        configuration
            .schema_registry
            .as_ref()
            .map(|url| Registry::builder_try_from_url(url).map(|builder| builder.build()))
            .transpose()
            .map(|registry| Self {
                configuration,
                registry,
            })
            .map_err(Into::into)
    }
}

impl Consume {
    pub(crate) async fn main(self) -> Result<ErrorCode> {
        let stdout = stdout();

        let mut writer = FramedWrite::new(stdout, LinesCodec::new());

        let schema = if let Some(ref registry) = self.registry {
            registry
                .schema(&self.configuration.topic)
                .await
                .inspect(|schema| debug!(?schema))?
        } else {
            None
        };

        let client = ConnectionManager::builder(self.configuration.broker.clone())
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        let metadata = client
            .call(
                MetadataRequest::default()
                    .allow_auto_topic_creation(Some(false))
                    .include_cluster_authorized_operations(Some(false))
                    .include_topic_authorized_operations(Some(false))
                    .topics(Some(
                        [MetadataRequestTopic::default()
                            .name(Some(self.configuration.topic.clone()))
                            .topic_id(Some(NULL_TOPIC_ID))]
                        .into(),
                    )),
            )
            .await?;

        let topic = metadata
            .topics
            .unwrap_or_default()
            .into_iter()
            .find(|topic| {
                topic
                    .name
                    .as_ref()
                    .is_some_and(|name| self.configuration.topic.as_str() == name)
            });

        // Kafka answers a fetch by an unknown topic ID with `UNKNOWN_TOPIC_ID` before it checks
        // authorization, so a topic the user may not read would be reported as unknown. The
        // metadata error names the real cause on both brokers.
        if let Some(error_code) = topic
            .as_ref()
            .map(|topic| topic.error_code)
            .filter(|error_code| *error_code != i16::from(ErrorCode::None))
        {
            return Err(Error::Metadata {
                topic: self.configuration.topic.clone(),
                error_code,
            });
        }

        let topic_id = topic
            .and_then(|topic| topic.topic_id)
            // The encoder leaves out a `None` topic ID, which moves the fields after it, so a
            // topic missing from the metadata is asked for by the null ID instead.
            .unwrap_or(NULL_TOPIC_ID);

        let response = client.call(self.fetch_request(topic_id)).await?;

        for response in response.responses.unwrap_or_default() {
            debug!(?response);

            for partition in response.partitions.unwrap_or_default() {
                debug!(?partition);

                // A fetch offset outside the partition, or a topic or partition that doesn't
                // exist, is answered with an error code and no records, which would otherwise
                // look like a partition with nothing to read. Kafka's consumer retries after a
                // metadata refresh on `UNKNOWN_TOPIC_OR_PARTITION`, but `cat consume` reads
                // once, so it reports the error and exits non-zero.
                if partition.error_code != i16::from(ErrorCode::None) {
                    return Err(Error::Fetch {
                        topic: self.configuration.topic.clone(),
                        partition: partition.partition_index,
                        error_code: partition.error_code,
                    });
                }

                for batch in readable_batches(
                    &self.configuration.topic,
                    partition,
                    self.configuration.fetch_offset,
                )? {
                    let batch = batch?;

                    debug!(?batch);

                    if let Some(ref schema) = schema {
                        writer
                            .send(schema.as_json_value(&batch).map(|kv| kv.to_string())?)
                            .await?;
                    } else {
                        writer.send( Value::from(
                            batch
                                .records
                                .iter()
                                .map(|record| json!({"key": self.maybe_json(record.key.clone()), "value": self.maybe_json(record.value.clone())}))
                                .collect::<Vec<_>>(),
                        )
                        .to_string()).await?
                    }
                }
            }
        }

        Ok(ErrorCode::None)
    }

    fn fetch_request(&self, topic_id: [u8; 16]) -> FetchRequest {
        FetchRequest::default()
            .cluster_id(Some("".into()))
            .replica_id(Some(-1))
            .replica_state(Some(ReplicaState::default()))
            .max_wait_ms(self.configuration.max_wait_time_ms)
            .min_bytes(self.configuration.min_bytes)
            .max_bytes(self.configuration.max_bytes)
            // `cat consume` asks for `read_committed` but does not use the response's
            // `aborted_transactions`, so it still prints the records of an aborted transaction.
            .isolation_level(Some(1))
            .session_id(Some(-1))
            .session_epoch(Some(-1))
            .topics(Some(vec![
                FetchTopic::default()
                    .topic(Some(self.configuration.topic.clone()))
                    .topic_id(Some(topic_id))
                    .partitions(Some(vec![
                        // Every field that the request's version has is set: the encoder
                        // leaves out a field that is `None`, which moves the fields after it.
                        // `cat consume` tracks no leader epoch and is not a follower, so it
                        // sends -1 for both epochs and the log start offset. Kafka's consumer
                        // sends -1 for an epoch it doesn't know ([`FetchRequest.build`]) and
                        // always for the log start offset ([`AbstractFetch.prepareFetchRequests`]).
                        //
                        // [`FetchRequest.build`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/requests/FetchRequest.java#L296-L302
                        // [`AbstractFetch.prepareFetchRequests`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractFetch.java#L447-L452
                        FetchPartition::default()
                            .partition(self.configuration.partition)
                            .current_leader_epoch(Some(-1))
                            .fetch_offset(self.configuration.fetch_offset)
                            .last_fetched_epoch(Some(-1))
                            .log_start_offset(Some(-1))
                            .partition_max_bytes(self.configuration.partition_max_bytes),
                    ])),
            ]))
            .forgotten_topics_data(Some([].into()))
            .rack_id(Some("".into()))
    }

    fn maybe_json(&self, data: Option<Bytes>) -> Option<Value> {
        data.and_then(|data| serde_json::from_slice::<Value>(&data[..]).ok())
    }
}

/// The batches of `partition`, a partition of `topic` in the broker's fetch response, that
/// `cat consume` prints, each with only the records it prints.
///
/// The iterator reads a batch when it is asked for it, so the batches before an overflowing one
/// are still printed, and the overflowing one is an [`Error::RecordOffsetOverflow`] that names
/// the partition of the response.
fn readable_batches(
    topic: &str,
    partition: fetch_response::PartitionData,
    fetch_offset: i64,
) -> Result<impl Iterator<Item = Result<inflated::Batch>>> {
    let partition_index = partition.partition_index;

    let batches = match partition.records {
        Some(frame) => {
            debug!(?frame);
            inflated::Frame::try_from(frame)?.batches
        }
        None => Vec::new(),
    };

    let topic = topic.to_owned();

    Ok(batches.into_iter().filter_map(move |batch| {
        readable(batch, fetch_offset)
            .map_err(|overflow| Error::RecordOffsetOverflow {
                topic: topic.clone(),
                partition: partition_index,
                base_offset: overflow.base_offset,
                offset_delta: overflow.offset_delta,
            })
            .transpose()
    }))
}

/// The records of `batch` that `cat consume` prints, or `None` when it has none.
///
/// A control batch holds a transaction marker, not a record. Kafka's consumer skips it, as
/// [`CompletedFetch`] does, and so does `cat consume`. The records before `fetch_offset` are
/// skipped as [`from_offset`] does.
///
/// [`CompletedFetch`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/CompletedFetch.java#L232-L238
fn readable(
    batch: inflated::Batch,
    fetch_offset: i64,
) -> Result<Option<inflated::Batch>, OffsetOverflow> {
    if batch.is_control() {
        // A fetch that returns only a marker prints nothing and exits 0, like a partition with
        // nothing to read, so this line is the only trace of it.
        debug!(base_offset = batch.base_offset, "skipped a control batch");
        Ok(None)
    } else {
        from_offset(batch, fetch_offset)
    }
}

/// The records of `batch` at or after `fetch_offset`, or `None` when it has none.
///
/// Fetch answers with whole batches, so the first batch can start before `fetch_offset`. A Kafka
/// consumer skips the records before it, as [`CompletedFetch`] does, and so does `cat consume`.
///
/// [`CompletedFetch`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/CompletedFetch.java#L227-L228
///
/// A record whose offset overflows an `i64` is an error, not a record before `fetch_offset`: no
/// partition has such an offset, so the response is corrupt, and dropping the record would hide
/// that. Kafka's consumer is looser: it adds the two unchecked in Java, as
/// [`DefaultRecord.readFrom`] does, and wraps. `cat consume` reads once and already exits
/// non-zero on other broker errors, so it is stricter.
///
/// [`DefaultRecord.readFrom`]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/common/record/DefaultRecord.java#L319
fn from_offset(
    mut batch: inflated::Batch,
    fetch_offset: i64,
) -> Result<Option<inflated::Batch>, OffsetOverflow> {
    let base_offset = batch.base_offset;

    if let Some(overflow) = batch.records.iter().find_map(|record| {
        base_offset
            .checked_add(i64::from(record.offset_delta))
            .is_none()
            .then_some(OffsetOverflow {
                base_offset,
                offset_delta: record.offset_delta,
            })
    }) {
        return Err(overflow);
    }

    // Every sum fits, as checked above.
    batch
        .records
        .retain(|record| base_offset + i64::from(record.offset_delta) >= fetch_offset);

    Ok((!batch.records.is_empty()).then_some(batch))
}

/// A record's offset, its batch's base offset plus its offset delta, that overflows an `i64`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OffsetOverflow {
    base_offset: i64,
    offset_delta: i32,
}

#[cfg(test)]
mod tests {
    use super::{Configuration, Consume, OffsetOverflow, from_offset, readable, readable_batches};
    use crate::Error;
    use bytes::Bytes;
    use nisshi_sans_io::{
        ApiKey, BatchAttribute, Body, ControlBatch, EndTransactionMarker, FetchRequest, Frame,
        Header, fetch_response,
        record::{Record, deflated, inflated},
    };
    use std::assert_matches;
    use url::Url;

    fn consume(fetch_offset: i64, partition_max_bytes: i32) -> Consume {
        Consume::try_from(Configuration {
            broker: Url::parse("tcp://127.0.0.1:9092").unwrap(),
            topic: "taxi".into(),
            partition: 2,
            schema_registry: None,
            max_wait_time_ms: 5_000,
            min_bytes: 1,
            max_bytes: Some(52_428_800),
            fetch_offset,
            partition_max_bytes,
        })
        .unwrap()
    }

    /// The request as the broker reads it at `api_version`.
    fn decoded(request: FetchRequest, api_version: i16) -> FetchRequest {
        let encoded = Frame::request(
            Header::Request {
                api_key: FetchRequest::KEY,
                api_version,
                correlation_id: 0,
                client_id: None,
            },
            request.into(),
        )
        .unwrap();

        let Body::FetchRequest(decoded) = Frame::request_from_bytes(encoded).unwrap().body else {
            panic!("not a fetch request")
        };

        decoded
    }

    #[test]
    fn fetch_request_asks_for_the_fetch_offset_and_partition_max_bytes() {
        let request = consume(42, 321).fetch_request([7; 16]);

        for api_version in 4..=17 {
            let partitions = decoded(request.clone(), api_version)
                .topics
                .unwrap()
                .pop()
                .and_then(|topic| topic.partitions)
                .unwrap();

            assert_eq!(1, partitions.len(), "{api_version}");
            assert_eq!(2, partitions[0].partition, "{api_version}");
            assert_eq!(42, partitions[0].fetch_offset, "{api_version}");
            assert_eq!(321, partitions[0].partition_max_bytes, "{api_version}");
        }
    }

    /// A batch at `base_offset` with one record for each of `values`.
    fn batch(base_offset: i64, values: &[&'static str]) -> inflated::Batch {
        values
            .iter()
            .zip(0..)
            .fold(
                inflated::Batch::builder().base_offset(base_offset),
                |batch, (value, offset_delta)| {
                    batch.record(
                        Record::builder()
                            .offset_delta(offset_delta)
                            .value(Some(Bytes::from_static(value.as_bytes()))),
                    )
                },
            )
            .last_offset_delta(values.len() as i32 - 1)
            .build()
            .unwrap()
    }

    fn values(batch: Option<inflated::Batch>) -> Option<Vec<Bytes>> {
        batch.map(|batch| batch.records.into_iter().filter_map(|r| r.value).collect())
    }

    #[test]
    fn records_before_the_fetch_offset_are_skipped() {
        assert_eq!(
            Some(vec![Bytes::from("c"), Bytes::from("d")]),
            values(from_offset(batch(10, &["a", "b", "c", "d"]), 12).unwrap())
        );
    }

    #[test]
    fn batch_at_or_after_the_fetch_offset_is_kept_whole() {
        assert_eq!(
            Some(vec![Bytes::from("a"), Bytes::from("b")]),
            values(from_offset(batch(10, &["a", "b"]), 10).unwrap())
        );

        assert_eq!(
            Some(vec![Bytes::from("a")]),
            values(from_offset(batch(10, &["a"]), 0).unwrap())
        );
    }

    #[test]
    fn batch_before_the_fetch_offset_is_dropped() {
        assert_eq!(
            None,
            values(from_offset(batch(10, &["a", "b"]), 12).unwrap())
        );
    }

    /// A batch at `base_offset` with one record at each of `offset_deltas`.
    fn batch_at(base_offset: i64, offset_deltas: &[i32]) -> inflated::Batch {
        offset_deltas
            .iter()
            .fold(
                inflated::Batch::builder().base_offset(base_offset),
                |batch, offset_delta| batch.record(Record::builder().offset_delta(*offset_delta)),
            )
            .last_offset_delta(offset_deltas.iter().copied().max().unwrap_or(0))
            .build()
            .unwrap()
    }

    #[test]
    fn offset_past_i64_max_is_an_error() {
        assert_eq!(
            Err(OffsetOverflow {
                base_offset: i64::MAX,
                offset_delta: 1
            }),
            from_offset(batch_at(i64::MAX, &[0, 1]), 0)
        );
    }

    #[test]
    fn offset_before_i64_min_is_an_error() {
        assert_eq!(
            Err(OffsetOverflow {
                base_offset: i64::MIN,
                offset_delta: -1
            }),
            from_offset(batch_at(i64::MIN, &[-1]), 0)
        );
    }

    #[test]
    fn offset_at_i64_max_is_kept() {
        assert_eq!(
            1,
            from_offset(batch_at(i64::MAX, &[0]), i64::MAX)
                .unwrap()
                .unwrap()
                .records
                .len()
        );
    }

    /// A partition of the broker's fetch response, with index `partition_index` and `batches`.
    fn partition_with(
        partition_index: i32,
        batches: Vec<inflated::Batch>,
    ) -> fetch_response::PartitionData {
        fetch_response::PartitionData::default()
            .partition_index(partition_index)
            .records(Some(
                deflated::Frame::try_from(inflated::Frame { batches }).unwrap(),
            ))
    }

    #[test]
    fn overflowing_offset_names_the_topic_and_the_partition_of_the_response() {
        let Err(error) =
            readable_batches("taxi", partition_with(5, vec![batch_at(i64::MAX, &[1])]), 0)
                .unwrap()
                .next()
                .unwrap()
        else {
            panic!("an overflowing offset was returned as a record")
        };

        assert_matches!(
            error,
            Error::RecordOffsetOverflow {
                ref topic,
                partition: 5,
                base_offset: i64::MAX,
                offset_delta: 1,
            } if topic == "taxi"
        );
    }

    #[test]
    fn batches_before_an_overflowing_one_are_still_returned() {
        let mut batches = readable_batches(
            "taxi",
            partition_with(0, vec![batch(10, &["a"]), batch_at(i64::MAX, &[1])]),
            0,
        )
        .unwrap();

        assert_eq!(
            Some(vec![Bytes::from("a")]),
            values(Some(batches.next().unwrap().unwrap()))
        );
        assert_matches!(
            batches.next(),
            Some(Err(Error::RecordOffsetOverflow { .. }))
        );
        assert_matches!(batches.next(), None);
    }

    #[test]
    fn partition_without_records_has_no_batches() {
        assert_eq!(
            0,
            readable_batches(
                "taxi",
                fetch_response::PartitionData::default().partition_index(0),
                0
            )
            .unwrap()
            .count()
        );
    }

    #[test]
    fn readable_reports_an_overflowing_offset() {
        assert_eq!(
            Err(OffsetOverflow {
                base_offset: i64::MAX,
                offset_delta: 1
            }),
            readable(batch_at(i64::MAX, &[1]), 0)
        );
    }

    /// The batch that the broker's storage writes when it ends a transaction.
    fn commit_marker(base_offset: i64) -> inflated::Batch {
        let key: Bytes = ControlBatch::default().commit().try_into().unwrap();
        let value: Bytes = EndTransactionMarker::default().try_into().unwrap();

        inflated::Batch::builder()
            .base_offset(base_offset)
            .record(Record::builder().key(Some(key)).value(Some(value)))
            .attributes(
                BatchAttribute::default()
                    .control(true)
                    .transaction(true)
                    .into(),
            )
            .producer_id(1)
            .producer_epoch(0)
            .base_sequence(-1)
            .build()
            .unwrap()
    }

    #[test]
    fn commit_marker_is_not_readable() {
        let marker = commit_marker(10);

        assert!(marker.is_control());
        assert_eq!(None, values(readable(marker, 0).unwrap()));
    }

    #[test]
    fn data_batch_in_a_transaction_is_readable() {
        let data = batch(10, &["a"])
            .into_builder()
            .attributes(BatchAttribute::default().transaction(true).into())
            .build()
            .unwrap();

        assert!(!data.is_control());
        assert_eq!(
            Some(vec![Bytes::from("a")]),
            values(readable(data, 0).unwrap())
        );
    }
}
