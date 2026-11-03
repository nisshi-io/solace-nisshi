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
    io::{self, Write},
};

use nisshi_client::{Client, ConnectionManager};
use nisshi_sans_io::{
    ErrorCode, MetadataRequest, MetadataResponse, metadata_response::MetadataResponseTopic,
};
use tracing::debug;
use url::Url;

use crate::{Error, Result, Topic};

/// How `topic list` prints the topics.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Format {
    /// A table of topic names and partition counts, for people to read.
    #[default]
    Table,
    /// The topics of the metadata response as JSON on one line, for scripts.
    Json,
}

#[derive(Clone, Debug, Default)]
pub struct Builder<B> {
    broker: B,
    format: Format,
}

impl<B> Builder<B> {
    pub fn broker(self, broker: Url) -> Builder<Url> {
        Builder {
            broker,
            format: self.format,
        }
    }

    pub fn format(self, format: Format) -> Self {
        Self { format, ..self }
    }
}

impl Builder<Url> {
    pub fn build(self) -> Topic {
        Topic::List(Configuration {
            broker: self.broker,
            format: self.format,
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Configuration {
    broker: Url,
    format: Format,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct List {
    configuration: Configuration,
}

impl TryFrom<Configuration> for List {
    type Error = Error;

    fn try_from(configuration: Configuration) -> Result<Self, Self::Error> {
        Ok(List { configuration })
    }
}

impl List {
    pub(crate) async fn main(self) -> Result<ErrorCode> {
        let client = ConnectionManager::builder(self.configuration.broker.clone())
            .client_id(Some(env!("CARGO_PKG_NAME").into()))
            .build()
            .await
            .inspect(|pool| debug!(?pool))
            .map(Client::new)?;

        let req = MetadataRequest::default()
            .allow_auto_topic_creation(Some(false))
            .include_cluster_authorized_operations(Some(false))
            .include_topic_authorized_operations(Some(false))
            .topics(None);

        let MetadataResponse { topics, .. } = client
            .call(req)
            .await
            .inspect(|response| debug!(?response))?;

        let topics = topics.as_deref().unwrap_or_default();

        match self.configuration.format {
            Format::Table => writeln!(io::stdout(), "{}", Topics::from(topics))?,
            Format::Json => writeln!(io::stdout(), "{}", serde_json::to_string(topics)?)?,
        }

        Ok(ErrorCode::None)
    }
}

/// The table that `topic list` prints by default: one row for each topic, sorted by name.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct Topics(Vec<Row>);

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Row {
    name: String,
    partitions: usize,
    /// The name of the topic's error code, `None` when the topic has no error.
    error: Option<String>,
}

impl From<&[MetadataResponseTopic]> for Topics {
    fn from(topics: &[MetadataResponseTopic]) -> Self {
        let mut rows = topics
            .iter()
            .map(|topic| Row {
                name: topic.name.clone().unwrap_or_default(),
                partitions: topic.partitions.as_deref().map_or(0, <[_]>::len),
                error: match ErrorCode::try_from(topic.error_code) {
                    Ok(ErrorCode::None) => None,
                    Ok(error_code) => Some(format!("{error_code:?}")),
                    Err(_) => Some(topic.error_code.to_string()),
                },
            })
            .collect::<Vec<_>>();

        rows.sort();
        Self(rows)
    }
}

impl Display for Topics {
    /// Prints the `ERROR` column only when some topic has an error, so the usual table has two
    /// columns. The last column has no padding, so a line has no trailing spaces.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const TOPIC: &str = "TOPIC";
        const PARTITIONS: &str = "PARTITIONS";

        if self.0.is_empty() {
            return f.write_str("No topics.");
        }

        let names = self.0.iter().map(Row::escaped_name).collect::<Vec<_>>();

        let name_width = names
            .iter()
            .map(|name| name.chars().count())
            .chain([TOPIC.len()])
            .max()
            .unwrap_or_default();

        let has_errors = self.0.iter().any(|row| row.error.is_some());

        if has_errors {
            write!(f, "{TOPIC:<name_width$}  {PARTITIONS}  ERROR")?;
        } else {
            write!(f, "{TOPIC:<name_width$}  {PARTITIONS}")?;
        }

        for (row, name) in self.0.iter().zip(names) {
            let partitions = row.partitions;

            match &row.error {
                Some(error) => write!(
                    f,
                    "\n{name:<name_width$}  {partitions:<width$}  {error}",
                    width = PARTITIONS.len()
                )?,
                None => write!(f, "\n{name:<name_width$}  {partitions}")?,
            }
        }

        Ok(())
    }
}

impl Row {
    /// Returns the topic's name with each character outside printable ASCII escaped.
    ///
    /// The name comes from the network. A terminal acts on a control character in it, and a
    /// format character such as U+202E reorders the line. A valid topic name holds only
    /// printable ASCII, so the escape changes only a name that a broker should never send, and
    /// the column width is then the escaped name's character count.
    fn escaped_name(&self) -> String {
        self.name.chars().fold(String::new(), |mut escaped, c| {
            if !c.is_ascii_graphic() {
                escaped.extend(c.escape_default());
            } else {
                escaped.push(c);
            }
            escaped
        })
    }
}

#[cfg(test)]
mod tests {
    use nisshi_sans_io::{
        ErrorCode,
        metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
    };

    use super::{Row, Topics};

    fn row(name: &str, partitions: usize, error: Option<&str>) -> Row {
        Row {
            name: name.into(),
            partitions,
            error: error.map(Into::into),
        }
    }

    #[test]
    fn topics_are_aligned_under_a_header() {
        let topics = Topics(vec![row("hello", 3, None), row("taxi", 1, None)]);

        assert_eq!("TOPIC  PARTITIONS\nhello  3\ntaxi   1", topics.to_string());
    }

    #[test]
    fn no_topics_prints_a_sentence_instead_of_a_bare_header() {
        assert_eq!("No topics.", Topics::default().to_string());
    }

    #[test]
    fn control_characters_in_a_name_are_escaped() {
        let topics = Topics(vec![row("red\x1b[31m", 1, None)]);

        // The column is as wide as the escaped name, `red\u{1b}[31m`, which is 13 characters.
        assert_eq!(
            "TOPIC          PARTITIONS\nred\\u{1b}[31m  1",
            topics.to_string()
        );
    }

    fn topic(
        name: Option<&str>,
        partitions: Option<i32>,
        error_code: i16,
    ) -> MetadataResponseTopic {
        MetadataResponseTopic::default()
            .name(name.map(Into::into))
            .error_code(error_code)
            .partitions(partitions.map(|partitions| {
                (0..partitions)
                    .map(|partition| {
                        MetadataResponsePartition::default().partition_index(partition)
                    })
                    .collect()
            }))
    }

    #[test]
    fn rows_from_metadata_are_sorted_by_name_and_name_each_error() {
        let topics = Topics::from(
            &[
                topic(Some("taxi"), Some(3), ErrorCode::None.into()),
                topic(
                    Some("hello"),
                    None,
                    ErrorCode::UnknownTopicOrPartition.into(),
                ),
                // No `ErrorCode` has the value 32767, so the row shows the number.
                topic(None, Some(1), i16::MAX),
            ][..],
        );

        assert_eq!(
            Topics(vec![
                row("", 1, Some("32767")),
                row("hello", 0, Some("UnknownTopicOrPartition")),
                row("taxi", 3, None),
            ]),
            topics
        );
    }

    #[test]
    fn characters_outside_printable_ascii_in_a_name_are_escaped() {
        // U+202E reverses the rest of the line on a terminal that supports bidirectional text.
        let topics = Topics(vec![row("taxi\u{202e}3", 1, None)]);

        assert_eq!(
            "TOPIC          PARTITIONS\ntaxi\\u{202e}3  1",
            topics.to_string()
        );
    }

    #[test]
    fn a_topic_with_an_error_adds_an_error_column() {
        let topics = Topics(vec![
            row("hello", 3, None),
            row("taxi", 0, Some("UnknownTopicOrPartition")),
        ]);

        assert_eq!(
            "TOPIC  PARTITIONS  ERROR\nhello  3\ntaxi   0           UnknownTopicOrPartition",
            topics.to_string()
        );
    }
}
