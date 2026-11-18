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

use anyhow::{Context as _, Result, anyhow};
use nisshi_service::{CONNECTION_TARGET, ProducedTraffic, TopicPartition};
use regex::Regex;

use crate::connection::{Connection, ConnectionState};

/// The `name=value` fields of an event.
struct Fields(BTreeMap<String, String>);

impl Fields {
    fn text(&self, name: &str) -> &str {
        self.0.get(name).map_or("", String::as_str)
    }

    fn number(&self, name: &str) -> Result<u64> {
        self.text(name)
            .parse()
            .with_context(|| format!("field {name} is not a number"))
    }

    fn flag(&self, name: &str) -> bool {
        self.text(name) == "true"
    }
}

/// Reads the connection events of a broker log, a line at a time.
pub(crate) struct EventParser {
    ansi: Regex,
    event: Regex,
    field: Regex,
}

impl EventParser {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self {
            ansi: Regex::new(r"\x1b\[[;\d]*[[:alpha:]]")?,

            // The target, then the source line that the text layer prints after it, then the
            // message. A `connections open` line does not match, because it names a count.
            event: Regex::new(&format!(
                r"{}:(?: \d+:)? (?<message>connection opened|connection snapshot|connection closed)(?<fields>.*)$",
                regex::escape(CONNECTION_TARGET)
            ))?,

            // A value is a quoted string, which may hold spaces, or the run up to the next
            // space. It can hold `=` (`requests=Produce=2,Fetch=1`) and it can be empty.
            field: Regex::new(r#"(?<name>\w+)=(?<value>"(?:[^"\\]|\\.)*"|\S*)"#)?,
        })
    }

    /// Returns the connection that `line` reports, or `None` when `line` is not a connection
    /// event.
    pub(crate) fn connection(&self, line: &str) -> Result<Option<Connection>> {
        let stripped = self.ansi.replace_all(line, "");

        let Some(captures) = self.event.captures(&stripped) else {
            return Ok(None);
        };

        let state = match &captures["message"] {
            "connection opened" => ConnectionState::Opened,
            "connection snapshot" => ConnectionState::Snapshot,
            _ => ConnectionState::Closed,
        };

        let fields = Fields(
            self.field
                .captures_iter(&captures["fields"])
                .map(|field| (field["name"].to_owned(), unquote(&field["value"])))
                .collect(),
        );

        Ok(Some(Connection {
            id: fields.number("connection")?,
            peer: fields.text("addr").to_owned(),
            client_id: fields.0.get("client_id").cloned(),
            state,
            age_ms: fields.number("age_ms")?,
            requests: counts(fields.text("requests"))?,
            produced: produced(fields.text("produce"))?,
            produce_truncated: fields.flag("produce_truncated"),
            fetched: partition_counts(fields.text("fetch"))?,
            fetch_truncated: fields.flag("fetch_truncated"),
        }))
    }
}

/// Removes the quotes around a value that the log writes as a quoted string, and the escapes
/// inside them.
fn unquote(value: &str) -> String {
    match value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
    {
        Some(inner) => inner.replace("\\\"", "\"").replace("\\\\", "\\"),
        None => value.to_owned(),
    }
}

/// Parses comma-separated `name=count` items.
fn counts(text: &str) -> Result<BTreeMap<String, u64>> {
    text.split(',')
        .filter(|item| !item.is_empty())
        .map(|item| {
            let (name, count) = item
                .rsplit_once('=')
                .ok_or_else(|| anyhow!("{item:?} is not name=count"))?;

            Ok((name.to_owned(), count.parse()?))
        })
        .collect()
}

/// Parses a `topic-partition` name. A topic name may hold hyphens and digits, so the partition is
/// what follows the last hyphen.
fn topic_partition(text: &str) -> Result<TopicPartition> {
    let (topic, partition) = text
        .rsplit_once('-')
        .ok_or_else(|| anyhow!("{text:?} is not topic-partition"))?;

    Ok((topic.to_owned(), partition.parse()?))
}

/// Parses comma-separated `topic-partition=count` items.
fn partition_counts(text: &str) -> Result<BTreeMap<TopicPartition, u64>> {
    counts(text)?
        .into_iter()
        .map(|(name, count)| Ok((topic_partition(&name)?, count)))
        .collect()
}

/// Parses comma-separated `topic-partition=requests/records/bytes` items.
fn produced(text: &str) -> Result<BTreeMap<TopicPartition, ProducedTraffic>> {
    text.split(',')
        .filter(|item| !item.is_empty())
        .map(|item| {
            let (name, traffic) = item
                .rsplit_once('=')
                .ok_or_else(|| anyhow!("{item:?} is not topic-partition=requests/records/bytes"))?;

            let mut numbers = traffic.split('/').map(str::parse::<u64>);
            let mut next = || {
                numbers
                    .next()
                    .ok_or_else(|| anyhow!("{traffic:?} is not requests/records/bytes"))?
                    .map_err(anyhow::Error::from)
            };

            Ok((
                topic_partition(name)?,
                ProducedTraffic {
                    requests: next()?,
                    records: next()?,
                    bytes: next()?,
                },
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // A line as the broker writes it with colours on: the fmt layer's escapes around each
    // field, and the source line after the target.
    const COLOURED_OPENED: &str = "\x1b[2m2026-11-16T11:20:34.185112Z\x1b[0m \x1b[32m INFO\x1b[0m \
        \x1b[2mnisshi::connection\x1b[0m\x1b[2m:\x1b[0m \x1b[2m134:\x1b[0m connection opened \
        \x1b[3mconnection\x1b[0m\x1b[2m=\x1b[0m4 \x1b[3maddr\x1b[0m\x1b[2m=\x1b[0m127.0.0.1:51949 \
        age_ms=0 requests= produce= produce_truncated=false fetch= fetch_truncated=false";

    const CLOSED: &str = "2026-11-16T11:20:41.962577Z  INFO nisshi::connection: 134: \
        connection closed connection=4 addr=127.0.0.1:51949 client_id=\"producer-1\" age_ms=7777 \
        requests=Produce=219,Metadata=1 produce=my-topic-0=219/219/226227,my-topic-2=1/1/1031 \
        produce_truncated=false fetch=t-1=3 fetch_truncated=true";

    fn parse(line: &str) -> Option<Connection> {
        EventParser::new().unwrap().connection(line).unwrap()
    }

    #[test]
    fn colours_and_the_source_line_do_not_hide_an_event() {
        let connection = parse(COLOURED_OPENED).expect("an opened event");

        assert_eq!(4, connection.id);
        assert_eq!("127.0.0.1:51949", connection.peer);
        assert_eq!(ConnectionState::Opened, connection.state);
        assert_eq!(None, connection.client_id);
    }

    #[test]
    fn a_close_event_carries_the_requests_and_topic_partitions() {
        let connection = parse(CLOSED).expect("a closed event");

        assert_eq!(ConnectionState::Closed, connection.state);
        assert_eq!(Some("producer-1"), connection.client_id.as_deref());
        assert_eq!(7777, connection.age_ms);
        assert_eq!(
            BTreeMap::from([("Metadata".to_owned(), 1), ("Produce".to_owned(), 219)]),
            connection.requests
        );
        assert_eq!(
            BTreeMap::from([
                (
                    ("my-topic".to_owned(), 0),
                    ProducedTraffic {
                        requests: 219,
                        records: 219,
                        bytes: 226227
                    }
                ),
                (
                    ("my-topic".to_owned(), 2),
                    ProducedTraffic {
                        requests: 1,
                        records: 1,
                        bytes: 1031
                    }
                ),
            ]),
            connection.produced
        );
        assert_eq!(
            BTreeMap::from([(("t".to_owned(), 1), 3)]),
            connection.fetched
        );
        assert!(!connection.produce_truncated);
        assert!(connection.fetch_truncated);
    }

    #[test]
    fn a_quoted_client_id_may_hold_spaces_and_escaped_quotes() {
        let connection = parse(
            r#"nisshi::connection: connection closed connection=1 addr=[::1]:5 client_id="my \"odd\" id" age_ms=1 requests= produce= produce_truncated=false fetch= fetch_truncated=false"#,
        )
        .expect("a closed event");

        assert_eq!(Some(r#"my "odd" id"#), connection.client_id.as_deref());
        assert_eq!("[::1]:5", connection.peer);
    }

    #[test]
    fn a_line_that_is_not_a_connection_event_is_not_an_event() {
        for line in [
            "2026-11-16T11:20:36Z  WARN nisshi_broker: something else connection=9",
            "nisshi::connection: 97: connections open open=2",
            "",
        ] {
            assert_eq!(None, parse(line), "{line:?}");
        }
    }

    #[test]
    fn an_event_with_a_field_that_is_not_a_number_is_an_error() {
        let parser = EventParser::new().unwrap();

        let error = parser
            .connection("nisshi::connection: connection closed connection=x age_ms=1")
            .expect_err("connection=x is not a number");

        assert_eq!("field connection is not a number", error.to_string());
    }
}
