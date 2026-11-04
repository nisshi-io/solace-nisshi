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

use std::{
    fmt::{self, Display},
    io::Write,
    marker::PhantomData,
};

use crate::{Error, Location, Result};

use bytes::Bytes;
use futures::StreamExt;
use nisshi_client::{Client, ConnectionManager};
use nisshi_sans_io::{
    ErrorCode, MetadataRequest, NULL_TOPIC_ID, ProduceRequest, ProduceResponse,
    metadata_request::MetadataRequestTopic,
    produce_request::{PartitionProduceData, TopicProduceData},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    record::{Record, deflated, inflated},
};
use nisshi_schema::{AsKafkaRecord, Registry, Schema};
use serde_json::Value;
use tokio::{
    fs::File,
    io::{self, AsyncRead, AsyncReadExt},
};
use tokio_util::codec::{FramedRead, LinesCodec};
use tracing::{debug, error};
use url::Url;

#[derive(Clone, Debug, Default)]
pub struct Builder<B, T, P, S, F> {
    broker: B,
    topic: T,
    partition: P,
    schema_registry: S,
    file_name: F,
    confirm: bool,
}

pub(crate) type PhantomBuilder = Builder<
    PhantomData<Url>,
    PhantomData<String>,
    PhantomData<i32>,
    PhantomData<Option<Url>>,
    PhantomData<String>,
>;

impl<B, T, P, S, F> Builder<B, T, P, S, F> {
    pub fn broker(self, broker: impl Into<Url>) -> Builder<Url, T, P, S, F> {
        Builder {
            broker: broker.into(),
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            file_name: self.file_name,
            confirm: self.confirm,
        }
    }

    pub fn topic(self, topic: impl Into<String>) -> Builder<B, String, P, S, F> {
        Builder {
            broker: self.broker,
            topic: topic.into(),
            partition: self.partition,
            schema_registry: self.schema_registry,
            file_name: self.file_name,
            confirm: self.confirm,
        }
    }

    pub fn partition(self, partition: i32) -> Builder<B, T, i32, S, F> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition,
            schema_registry: self.schema_registry,
            file_name: self.file_name,
            confirm: self.confirm,
        }
    }

    pub fn schema_registry(self, schema_registry: Option<Url>) -> Builder<B, T, P, Option<Url>, F> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry,
            file_name: self.file_name,
            confirm: self.confirm,
        }
    }

    pub fn file_name(self, file_name: String) -> Builder<B, T, P, S, String> {
        Builder {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            file_name,
            confirm: self.confirm,
        }
    }

    /// Print a confirmation line to stdout on success. Off by default, so a library caller gets no output.
    pub fn confirm(self, confirm: bool) -> Builder<B, T, P, S, F> {
        Self { confirm, ..self }
    }
}

impl Builder<Url, String, i32, Option<Url>, String> {
    pub fn build(self) -> super::Cat {
        super::Cat::Produce(Box::new(Configuration {
            broker: self.broker,
            topic: self.topic,
            partition: self.partition,
            schema_registry: self.schema_registry,
            file_name: self.file_name,
            confirm: self.confirm,
        }))
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Configuration {
    pub broker: Url,
    pub topic: String,
    pub partition: i32,
    pub schema_registry: Option<Url>,
    pub file_name: String,
    pub confirm: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct Produce {
    configuration: Configuration,
    registry: Option<Registry>,
}

impl TryFrom<Configuration> for Produce {
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

impl Produce {
    fn add_to_batch(
        &self,
        mut batch: inflated::Builder,
        schema: Option<&Schema>,
        location: Location,
        data: &Value,
        offset_delta: &mut i32,
    ) -> Result<inflated::Builder> {
        if let Some(schema) = schema {
            batch = batch.record(
                schema
                    .as_kafka_record(data)
                    .map(|record| record.offset_delta(*offset_delta))?,
            );

            *offset_delta += 1;
        } else {
            let key = data
                .get("key")
                .and_then(|key| serde_json::to_vec(key).map(Bytes::from).ok());

            let value = data
                .get("value")
                .and_then(|value| serde_json::to_vec(value).map(Bytes::from).ok());

            if key.is_none() && value.is_none() {
                return Err(Error::MessageWithoutKeyOrValue {
                    location,
                    message: data.to_string(),
                });
            }

            batch = batch.record(
                Record::builder()
                    .key(key)
                    .value(value)
                    .offset_delta(*offset_delta),
            );

            *offset_delta += 1;
        }

        Ok(batch)
    }

    /// Adds the message on one line of stdin to `batch`. `line_number` counts from 1.
    fn add_line(
        &self,
        batch: inflated::Builder,
        schema: Option<&Schema>,
        line_number: usize,
        line: &str,
        offset_delta: &mut i32,
    ) -> Result<inflated::Builder> {
        let location = Location::StdinLine(line_number);

        match serde_json::from_str::<Value>(line) {
            Ok(data) => {
                debug!(%data);
                self.add_to_batch(batch, schema, location, &data, offset_delta)
            }
            Err(source) => Err(Error::InvalidJson { location, source }),
        }
    }

    /// Adds the message on each line of `input`, which is stdin outside tests, to `batch`.
    /// A blank line holds no message, but it still counts in the line numbers that errors
    /// report.
    async fn add_lines(
        &self,
        mut batch: inflated::Builder,
        schema: Option<&Schema>,
        input: impl AsyncRead + Unpin,
        offset_delta: &mut i32,
    ) -> Result<inflated::Builder> {
        let mut reader = FramedRead::new(input, LinesCodec::new());
        let mut line_number = 0;

        while let Some(line) = reader.next().await.transpose()? {
            line_number += 1;

            if line.trim().is_empty() {
                continue;
            }

            debug!(%line);

            batch = self.add_line(batch, schema, line_number, &line, offset_delta)?;
        }

        Ok(batch)
    }

    /// Adds each message in the JSON array that a file holds to `batch`.
    fn add_file(
        &self,
        mut batch: inflated::Builder,
        schema: Option<&Schema>,
        contents: &[u8],
        offset_delta: &mut i32,
    ) -> Result<inflated::Builder> {
        let v = serde_json::from_slice::<Value>(contents).map_err(|source| Error::InvalidJson {
            location: Location::File(self.configuration.file_name.clone()),
            source,
        })?;

        let records = v.as_array().ok_or_else(|| Error::NotAnArray {
            location: Location::File(self.configuration.file_name.clone()),
        })?;

        for (index, record) in records.iter().enumerate() {
            debug!(%record);

            batch = self.add_to_batch(
                batch,
                schema,
                Location::FileElement {
                    file_name: self.configuration.file_name.clone(),
                    index,
                },
                record,
                offset_delta,
            )?;
        }

        Ok(batch)
    }

    pub(crate) async fn main(self) -> Result<ErrorCode> {
        debug!(%self.configuration.file_name);

        let schema = if let Some(ref registry) = self.registry {
            registry
                .schema(&self.configuration.topic)
                .await
                .inspect(|schema| debug!(?schema))?
        } else {
            None
        };

        let mut offset_delta = 0;

        let frame = {
            let mut batch = inflated::Batch::builder();

            let location = if self.configuration.file_name == "-" {
                batch = self
                    .add_lines(batch, schema.as_ref(), io::stdin(), &mut offset_delta)
                    .await?;

                Location::Stdin
            } else {
                let mut file = File::open(self.configuration.file_name.as_str()).await?;

                let mut contents = vec![];
                _ = file.read_to_end(&mut contents).await?;

                batch = self.add_file(batch, schema.as_ref(), &contents, &mut offset_delta)?;

                Location::File(self.configuration.file_name.clone())
            };

            frame(batch, offset_delta, location)?
        };

        debug!(?frame);

        let client = ConnectionManager::builder(self.configuration.broker.clone())
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        // A real Kafka client's first Metadata request allows auto-creation, so a produce to a
        // topic that doesn't exist yet creates it instead of failing with UnknownTopicOrPartition.
        // Mirrors the Metadata request in `consume`, but with auto-creation allowed instead of
        // refused. It goes after the input is read, so that a missing or malformed input file
        // leaves no empty topic behind.
        _ = client
            .call(
                MetadataRequest::default()
                    .allow_auto_topic_creation(Some(true))
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

        let topic = self.configuration.topic;

        let req = ProduceRequest::default()
            .transactional_id(None)
            .acks(-1)
            .timeout_ms(1_500)
            .topic_data(Some(
                [TopicProduceData::default()
                    .name(topic.clone())
                    .partition_data(Some(
                        [PartitionProduceData::default()
                            .index(self.configuration.partition)
                            .records(Some(frame))]
                        .into(),
                    ))]
                .into(),
            ));

        let ProduceResponse { responses, .. } = client.call(req).await?;
        let responses = responses.unwrap_or_default();
        assert_eq!(1, responses.len());

        let TopicProduceResponse {
            partition_responses,
            ..
        } = responses.first().ok_or_else(|| {
            Error::Message(format!("expected exactly one response: {responses:?}"))
        })?;
        let partition_responses = partition_responses.as_deref().unwrap_or_default();
        assert_eq!(1, partition_responses.len());

        let partition_response = partition_responses.first().ok_or_else(|| {
            Error::Message(format!(
                "expected exactly one partition response: {partition_responses:?}"
            ))
        })?;

        let error_code = ErrorCode::try_from(partition_response.error_code)?;

        if error_code != ErrorCode::None {
            if let Some(reason) = rejection_reason(&topic, partition_response) {
                error!("{reason}");
            }
        } else if self.configuration.confirm {
            let produced = Produced {
                topic,
                partition: self.configuration.partition,
                base_offset: partition_response.base_offset,
                count: offset_delta,
            };

            // The broker has already written the messages. A failure to print the line, such as
            // a closed pipe, is ignored instead of panicking in `println!`, because a non-zero
            // exit would make a caller that retries write the messages again.
            _ = writeln!(std::io::stdout(), "{produced}").inspect_err(|error| debug!(?error));
        }

        Ok(error_code)
    }
}

/// The line `cat produce` prints when the broker has written the messages.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Produced {
    topic: String,
    partition: i32,
    /// The offset of the first message written.
    base_offset: i64,
    /// The number of messages written.
    count: i32,
}

impl Display for Produced {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            topic,
            partition,
            base_offset,
            count,
        } = self;

        match count {
            0 => write!(f, "Produced 0 messages to {topic} partition {partition}."),
            1 => write!(
                f,
                "Produced 1 message to {topic} partition {partition}, offset {base_offset}."
            ),
            _ => write!(
                f,
                "Produced {count} messages to {topic} partition {partition}, offsets \
                {base_offset} to {}.",
                // The broker supplies `base_offset`, and `fmt` can't fail, so a base offset
                // too close to `i64::MAX` prints `i64::MAX` instead of overflowing.
                base_offset.saturating_add(i64::from(count - 1))
            ),
        }
    }
}

/// Returns `text` with each line break written as `\n` or `\r`. The text comes from the
/// broker, and a line break in it would start a new line in the terminal, where it could pass
/// for the confirmation line.
fn escape_line_breaks(text: &str) -> String {
    text.replace('\n', "\\n").replace('\r', "\\r")
}

/// Returns the broker's reason for rejecting the messages for `topic`, from the error
/// message and the record errors in the partition's response. Returns `None` when the
/// response has neither.
fn rejection_reason(topic: &str, response: &PartitionProduceResponse) -> Option<String> {
    let record_errors = response.record_errors.iter().flatten().map(|record_error| {
        match record_error.batch_index_error_message.as_deref() {
            Some(message) => format!(
                "record {}: {}",
                record_error.batch_index,
                escape_line_breaks(message)
            ),
            None => format!("record {}", record_error.batch_index),
        }
    });

    let reasons = response
        .error_message
        .iter()
        .map(|message| escape_line_breaks(message))
        .chain(record_errors)
        .collect::<Vec<_>>();

    (!reasons.is_empty()).then(|| {
        format!(
            "the broker rejected the messages for topic {topic}, partition {}: {}",
            response.index,
            reasons.join("; ")
        )
    })
}

/// Returns the frame that carries `batch`, which holds `records` records read from `location`.
fn frame(batch: inflated::Builder, records: i32, location: Location) -> Result<deflated::Frame> {
    if records == 0 {
        return Err(Error::NoMessages { location });
    }

    batch
        .last_offset_delta(records - 1)
        .build()
        .map(|batch| inflated::Frame {
            batches: vec![batch],
        })
        .and_then(deflated::Frame::try_from)
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::{Configuration, Produce, Produced, frame, rejection_reason};
    use crate::{Error, Location, Result};
    use nisshi_sans_io::{
        ErrorCode,
        produce_response::{BatchIndexAndErrorMessage, PartitionProduceResponse},
        record::inflated,
    };
    use std::assert_matches;
    use url::Url;

    fn produce(file_name: &str) -> Produce {
        Produce::try_from(Configuration {
            broker: Url::parse("tcp://127.0.0.1:9092").unwrap(),
            topic: "greetings".into(),
            partition: 0,
            schema_registry: None,
            file_name: file_name.into(),
            confirm: true,
        })
        .unwrap()
    }

    /// Adds `line`, as line 3 of stdin, to an empty batch, and returns the batch and its
    /// record count.
    fn add_line(line: &str) -> (Result<inflated::Builder>, i32) {
        let mut records = 0;
        let batch = produce("-").add_line(inflated::Batch::builder(), None, 3, line, &mut records);
        (batch, records)
    }

    /// Adds the messages in `contents`, as the file `messages.json`, to an empty batch, and
    /// returns the batch and its record count.
    fn add_file(contents: &str) -> (Result<inflated::Builder>, i32) {
        let mut records = 0;
        let batch = produce("messages.json").add_file(
            inflated::Batch::builder(),
            None,
            contents.as_bytes(),
            &mut records,
        );
        (batch, records)
    }

    #[test]
    fn line_without_key_or_value_is_rejected_with_its_line_number() {
        let (batch, _) = add_line(r#"{"message":"hello world"}"#);

        assert_matches!(
            batch,
            Err(Error::MessageWithoutKeyOrValue {
                location: Location::StdinLine(3),
                ref message,
            }) if message == r#"{"message":"hello world"}"#
        );
    }

    #[test]
    fn line_that_is_not_an_object_is_rejected() {
        for line in [r#""hello""#, "42", "[]", "null"] {
            let (batch, _) = add_line(line);

            assert_matches!(
                batch,
                Err(Error::MessageWithoutKeyOrValue {
                    location: Location::StdinLine(3),
                    ..
                }),
                "{line}"
            );
        }
    }

    #[test]
    fn file_element_without_key_or_value_is_rejected_with_its_index() {
        let (batch, _) = add_file(r#"[{"value": 1}, {"message": "hello world"}]"#);

        assert_matches!(
            batch,
            Err(Error::MessageWithoutKeyOrValue {
                location: Location::FileElement { ref file_name, index: 1 },
                ..
            }) if file_name == "messages.json"
        );
    }

    #[test]
    fn message_with_only_a_value_or_only_a_key_adds_one_record() {
        for line in [r#"{"value": "hello world"}"#, r#"{"key": "1"}"#] {
            let (batch, records) = add_line(line);

            assert!(batch.is_ok(), "{line}");
            assert_eq!(1, records, "{line}");
        }
    }

    /// Adds the messages on the lines of `input`, as stdin, to an empty batch, and returns the
    /// batch and its record count.
    async fn add_lines(input: &str) -> (Result<inflated::Builder>, i32) {
        let mut records = 0;
        let batch = produce("-")
            .add_lines(
                inflated::Batch::builder(),
                None,
                input.as_bytes(),
                &mut records,
            )
            .await;
        (batch, records)
    }

    #[tokio::test]
    async fn stdin_line_numbers_count_blank_lines() {
        let (batch, _) = add_lines("{\"value\": 1}\n\n \n{\"message\": \"x\"}\n").await;

        assert_matches!(
            batch,
            Err(Error::MessageWithoutKeyOrValue {
                location: Location::StdinLine(4),
                ..
            })
        );
    }

    #[tokio::test]
    async fn stdin_of_blank_lines_has_no_records() {
        let (batch, records) = add_lines("\n \n").await;

        assert_eq!(0, records);
        assert_matches!(
            frame(batch.unwrap(), records, Location::Stdin),
            Err(Error::NoMessages {
                location: Location::Stdin
            })
        );
    }

    #[test]
    fn file_that_is_not_an_array_is_rejected() {
        let (batch, _) = add_file(r#"{"key": "1", "value": "hello world"}"#);

        assert_matches!(
            batch,
            Err(Error::NotAnArray {
                location: Location::File(ref file_name),
            }) if file_name == "messages.json"
        );
    }

    #[test]
    fn input_without_messages_is_rejected() {
        assert_matches!(
            frame(inflated::Batch::builder(), 0, Location::Stdin),
            Err(Error::NoMessages {
                location: Location::Stdin
            })
        );

        let (batch, records) = add_file("[]");
        assert_matches!(
            frame(
                batch.unwrap(),
                records,
                Location::File("messages.json".into())
            ),
            Err(Error::NoMessages {
                location: Location::File(_)
            })
        );
    }

    #[test]
    fn message_without_key_or_value_error_names_the_line_and_the_format() {
        let error = Error::MessageWithoutKeyOrValue {
            location: Location::StdinLine(1),
            message: r#"{"message":"hello world"}"#.into(),
        };

        assert_eq!(
            r#"line 1 of stdin has no "key" or "value" field: {"message":"hello world"}. Write one message per line, as a JSON object with a "value" and an optional "key", for example {"key": "1", "value": {"message": "hello world"}}"#,
            error.to_string()
        );
    }

    #[test]
    fn no_messages_error_names_the_file_and_the_format() {
        let error = Error::NoMessages {
            location: Location::File("messages.json".into()),
        };

        assert_eq!(
            r#"messages.json has no messages. Write the file as a JSON array of messages, each a JSON object with a "value" and an optional "key", for example [{"key": "1", "value": {"message": "hello world"}}]"#,
            error.to_string()
        );
    }

    #[test]
    fn line_that_is_not_json_is_rejected_with_its_line_number() {
        let (batch, _) = add_line("hello");

        assert_matches!(
            batch,
            Err(Error::InvalidJson {
                location: Location::StdinLine(3),
                ..
            })
        );
    }

    #[test]
    fn file_that_is_not_json_is_rejected() {
        let (batch, _) = add_file(r#"[{"value": 1},"#);

        assert_matches!(
            batch,
            Err(Error::InvalidJson {
                location: Location::File(ref file_name),
                ..
            }) if file_name == "messages.json"
        );
    }

    #[test]
    fn invalid_json_error_names_the_line_the_parse_error_and_the_format() {
        let (batch, _) = add_line("hello");

        assert_eq!(
            r#"line 3 of stdin is not valid JSON: expected value at line 1 column 1. Write one message per line, as a JSON object with a "value" and an optional "key", for example {"key": "1", "value": {"message": "hello world"}}"#,
            batch.unwrap_err().to_string()
        );
    }

    fn rejected(
        error_message: Option<&str>,
        record_errors: Option<Vec<BatchIndexAndErrorMessage>>,
    ) -> PartitionProduceResponse {
        PartitionProduceResponse::default()
            .index(2)
            .error_code(ErrorCode::InvalidRecord.into())
            .error_message(error_message.map(Into::into))
            .record_errors(record_errors)
    }

    #[test]
    fn rejection_reason_names_the_partition_the_message_and_each_record_error() {
        let response = rejected(
            Some("batch has no records"),
            Some(vec![
                BatchIndexAndErrorMessage::default()
                    .batch_index(0)
                    .batch_index_error_message(Some("value is not an integer".into())),
                BatchIndexAndErrorMessage::default()
                    .batch_index(3)
                    .batch_index_error_message(None),
            ]),
        );

        assert_eq!(
            Some(
                "the broker rejected the messages for topic greetings, partition 2: batch has no \
                records; record 0: value is not an integer; record 3"
                    .into()
            ),
            rejection_reason("greetings", &response)
        );
    }

    #[test]
    fn rejection_reason_without_error_message_lists_the_record_errors() {
        let response = rejected(
            None,
            Some(vec![
                BatchIndexAndErrorMessage::default()
                    .batch_index(1)
                    .batch_index_error_message(Some("value is not an integer".into())),
            ]),
        );

        assert_eq!(
            Some(
                "the broker rejected the messages for topic greetings, partition 2: record 1: \
                value is not an integer"
                    .into()
            ),
            rejection_reason("greetings", &response)
        );
    }

    #[test]
    fn rejection_reason_writes_line_breaks_from_the_broker_as_escapes() {
        let response = rejected(
            Some("bad\r\nProduced 1 message to taxi partition 0, offset 0."),
            Some(vec![
                BatchIndexAndErrorMessage::default()
                    .batch_index(0)
                    .batch_index_error_message(Some("a\nb".into())),
            ]),
        );

        let reason = rejection_reason("greetings", &response).unwrap();

        assert!(!reason.contains(['\n', '\r']), "{reason}");
        assert!(reason.contains("bad\\r\\nProduced"), "{reason}");
        assert!(reason.contains("record 0: a\\nb"), "{reason}");
    }

    #[test]
    fn rejection_without_a_reason_has_none() {
        assert_eq!(None, rejection_reason("greetings", &rejected(None, None)));
        assert_eq!(
            None,
            rejection_reason("greetings", &rejected(None, Some(vec![])))
        );
    }

    fn produced(base_offset: i64, count: i32) -> String {
        Produced {
            topic: "taxi".into(),
            partition: 0,
            base_offset,
            count,
        }
        .to_string()
    }

    #[test]
    fn produced_gives_the_range_of_offsets() {
        assert_eq!(
            "Produced 4 messages to taxi partition 0, offsets 0 to 3.",
            produced(0, 4)
        );
    }

    #[test]
    fn produced_range_past_i64_max_stops_at_it() {
        assert_eq!(
            format!(
                "Produced 3 messages to taxi partition 0, offsets {0} to {0}.",
                i64::MAX
            ),
            produced(i64::MAX, 3)
        );
    }

    #[test]
    fn produced_one_message_gives_its_offset() {
        assert_eq!(
            "Produced 1 message to taxi partition 0, offset 4.",
            produced(4, 1)
        );
    }

    #[test]
    fn produced_no_messages_gives_no_offset() {
        assert_eq!("Produced 0 messages to taxi partition 0.", produced(-1, 0));
    }
}
