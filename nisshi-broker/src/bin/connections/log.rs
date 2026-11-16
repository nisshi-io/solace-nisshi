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

use anyhow::{Context as _, Result};

use crate::{
    connection::{Connection, ConnectionState},
    event::EventParser,
};

/// The connections that a broker log reports.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct Log {
    pub(crate) connections: BTreeMap<u64, Connection>,

    /// The most connections that were open at once.
    pub(crate) peak_open: usize,
}

impl Log {
    /// Reads the connection events in `text`, and keeps the last event of each connection.
    ///
    /// Returns an error that names the line of an event that does not parse.
    pub(crate) fn read(text: &str) -> Result<Self> {
        let parser = EventParser::new()?;
        let mut log = Self::default();
        let mut open = 0usize;

        for (index, line) in text.lines().enumerate() {
            let Some(connection) = parser
                .connection(line)
                .with_context(|| format!("line {}", index + 1))?
            else {
                continue;
            };

            match connection.state {
                ConnectionState::Opened => {
                    open += 1;
                    log.peak_open = log.peak_open.max(open);
                }
                ConnectionState::Closed => open = open.saturating_sub(1),
                ConnectionState::Snapshot => {}
            }

            _ = log.connections.insert(connection.id, connection);
        }

        Ok(log)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    /// The connections whose last event is not a close.
    pub(crate) fn still_open(&self) -> usize {
        self.connections
            .values()
            .filter(|connection| connection.state != ConnectionState::Closed)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(message: &str, connection: u64, client_id: Option<&str>, requests: &str) -> String {
        let client_id = client_id.map_or(String::new(), |id| format!(" client_id=\"{id}\""));

        format!(
            "2026-11-16T11:20:36Z  INFO nisshi::connection: 134: {message} connection={connection} \
             addr=127.0.0.1:{port}{client_id} age_ms=10 requests={requests} produce= \
             produce_truncated=false fetch= fetch_truncated=false",
            port = 50000 + connection
        )
    }

    fn read(events: &[String]) -> Log {
        Log::read(&events.join("\n")).unwrap()
    }

    #[test]
    fn a_close_event_is_the_final_record_of_a_connection() {
        let log = read(&[
            event("connection opened", 4, None, ""),
            event("connection snapshot", 4, Some("producer-1"), "Produce=72"),
            event("connection closed", 4, Some("producer-1"), "Produce=219"),
        ]);

        let connection = &log.connections[&4];

        assert_eq!(ConnectionState::Closed, connection.state);
        assert_eq!(
            BTreeMap::from([("Produce".to_owned(), 219)]),
            connection.requests
        );
        assert_eq!(0, log.still_open());
    }

    #[test]
    fn a_connection_that_never_closed_keeps_its_latest_snapshot() {
        let log = read(&[
            event("connection opened", 7, None, ""),
            event("connection snapshot", 7, Some("consumer-1"), "Fetch=3"),
            event("connection snapshot", 7, Some("consumer-1"), "Fetch=9"),
        ]);

        let connection = &log.connections[&7];

        assert_eq!(ConnectionState::Snapshot, connection.state);
        assert_eq!(
            BTreeMap::from([("Fetch".to_owned(), 9)]),
            connection.requests
        );
        assert_eq!(1, log.still_open());
    }

    #[test]
    fn the_peak_counts_the_connections_open_at_once() {
        let log = read(&[
            event("connection opened", 1, None, ""),
            event("connection opened", 2, None, ""),
            event("connection closed", 1, None, ""),
            event("connection opened", 3, None, ""),
            event("connection closed", 2, None, ""),
            event("connection closed", 3, None, ""),
        ]);

        assert_eq!(3, log.connections.len());
        assert_eq!(2, log.peak_open);
    }

    #[test]
    fn a_close_without_an_open_event_does_not_count_a_connection_out() {
        let log = read(&[
            event("connection closed", 1, None, ""),
            event("connection opened", 2, None, ""),
        ]);

        assert_eq!(1, log.peak_open);
    }

    #[test]
    fn a_log_without_connection_events_is_empty() {
        assert!(Log::read("nothing to see\nhere\n").unwrap().is_empty());
    }

    #[test]
    fn an_event_that_does_not_parse_names_its_line() {
        let error = Log::read(&format!(
            "first line\n{}",
            "nisshi::connection: connection closed connection=x age_ms=1"
        ))
        .expect_err("connection=x is not a number");

        assert_eq!("line 2", error.to_string());
    }
}
