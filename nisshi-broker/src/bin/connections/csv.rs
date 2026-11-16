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

use std::{borrow::Cow, fmt};

use crate::{log::Log, report};

/// The CSV form of a [`Log`]: one row per connection.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Csv<'a> {
    log: &'a Log,
}

impl<'a> Csv<'a> {
    pub(crate) fn new(log: &'a Log) -> Self {
        Self { log }
    }
}

/// Quotes `text` when it holds a comma, a quote or a line break.
fn quoted(text: &str) -> Cow<'_, str> {
    if text.contains([',', '"', '\n', '\r']) {
        Cow::Owned(format!("\"{}\"", text.replace('"', "\"\"")))
    } else {
        Cow::Borrowed(text)
    }
}

impl fmt::Display for Csv<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "connection,peer,client_id,kind,state,age_ms,requests,produce_partitions,\
             produce_records,produce_bytes,fetch_partitions"
        )?;

        for connection in self.log.connections.values() {
            let requests = report::request_list(
                connection
                    .requests
                    .iter()
                    .map(|(name, count)| (name.as_str(), *count))
                    .collect(),
            );

            writeln!(
                f,
                "{},{},{},{},{},{},{},{},{},{},{}",
                connection.id,
                quoted(&connection.peer),
                quoted(connection.client_id.as_deref().unwrap_or_default()),
                quoted(connection.client_kind()),
                connection.state,
                connection.age_ms,
                quoted(&requests),
                connection.produced.len(),
                connection.produced_records(),
                connection.produced_bytes(),
                connection.fetched.len(),
            )?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nisshi_service::ProducedTraffic;

    use super::*;
    use crate::connection::{Connection, ConnectionState};

    #[test]
    fn each_connection_is_a_row() {
        let connection = Connection {
            id: 2,
            peer: "127.0.0.1:50002".into(),
            client_id: Some("producer-1".into()),
            state: ConnectionState::Closed,
            age_ms: 10,
            requests: BTreeMap::from([("Produce".to_owned(), 5)]),
            produced: BTreeMap::from([(
                ("t".to_owned(), 0),
                ProducedTraffic {
                    requests: 5,
                    records: 50,
                    bytes: 500,
                },
            )]),
            ..Connection::default()
        };
        let log = Log {
            connections: BTreeMap::from([(2, connection)]),
            peak_open: 1,
        };

        assert_eq!(
            "connection,peer,client_id,kind,state,age_ms,requests,produce_partitions,\
             produce_records,produce_bytes,fetch_partitions\n\
             2,127.0.0.1:50002,producer-1,producer,closed,10,Produce=5,1,50,500,0\n",
            Csv::new(&log).to_string()
        );
    }

    #[test]
    fn a_field_with_a_comma_or_a_quote_is_quoted() {
        assert_eq!("plain", quoted("plain"));
        assert_eq!("\"a,b\"", quoted("a,b"));
        assert_eq!("\"say \"\"hi\"\"\"", quoted("say \"hi\""));
    }
}
