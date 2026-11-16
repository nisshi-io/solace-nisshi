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

use std::{collections::BTreeMap, fmt};

use nisshi_service::{ProducedTraffic, TopicPartition};

/// What the report calls a connection that has no `client_id`.
pub(crate) const NO_REQUEST: &str = "(no request)";

/// The event that gave a [`Connection`] its record.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum ConnectionState {
    #[default]
    Opened,
    Snapshot,
    Closed,
}

impl fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Opened => "opened",
            Self::Snapshot => "snapshot",
            Self::Closed => "closed",
        })
    }
}

/// What the broker logged about one connection, as of its last event.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct Connection {
    pub(crate) id: u64,
    pub(crate) peer: String,
    pub(crate) client_id: Option<String>,
    pub(crate) state: ConnectionState,
    pub(crate) age_ms: u64,

    /// The requests received, by API name.
    pub(crate) requests: BTreeMap<String, u64>,

    pub(crate) produced: BTreeMap<TopicPartition, ProducedTraffic>,
    pub(crate) produce_truncated: bool,

    /// The Fetch requests that named each topic-partition.
    pub(crate) fetched: BTreeMap<TopicPartition, u64>,
    pub(crate) fetch_truncated: bool,
}

impl Connection {
    /// Returns the client's id, or [`NO_REQUEST`] when the connection sent no request.
    pub(crate) fn client_label(&self) -> &str {
        self.client_id.as_deref().unwrap_or(NO_REQUEST)
    }

    /// Returns the client's id without its trailing number: `producer` for `producer-3`. A
    /// connection with no `client_id` is [`NO_REQUEST`], and an id that is only a number stays
    /// as it is.
    pub(crate) fn client_kind(&self) -> &str {
        let Some(client_id) = self.client_id.as_deref() else {
            return NO_REQUEST;
        };

        let kind = client_id
            .trim_end_matches(|c: char| c.is_ascii_digit())
            .trim_end_matches(['-', '_']);

        if kind.is_empty() { client_id } else { kind }
    }

    pub(crate) fn produced_records(&self) -> u64 {
        self.produced.values().map(|traffic| traffic.records).sum()
    }

    pub(crate) fn produced_bytes(&self) -> u64 {
        self.produced.values().map(|traffic| traffic.bytes).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn connection(client_id: Option<&str>) -> Connection {
        Connection {
            client_id: client_id.map(str::to_owned),
            ..Connection::default()
        }
    }

    #[test]
    fn client_kind_is_the_client_id_without_its_trailing_number() {
        for (client_id, kind) in [
            ("producer-12", "producer"),
            ("adminclient-1", "adminclient"),
            ("consumer_3", "consumer"),
            ("nisshi-perf", "nisshi-perf"),
            ("7", "7"),
        ] {
            assert_eq!(kind, connection(Some(client_id)).client_kind());
        }
    }

    #[test]
    fn a_connection_without_a_client_id_has_no_request() {
        assert_eq!(NO_REQUEST, connection(None).client_kind());
        assert_eq!(NO_REQUEST, connection(None).client_label());
    }

    #[test]
    fn produced_totals_add_up_the_topic_partitions() {
        let traffic = |records, bytes| ProducedTraffic {
            requests: 1,
            records,
            bytes,
        };

        let connection = Connection {
            produced: BTreeMap::from([
                (("t".to_owned(), 0), traffic(2, 20)),
                (("t".to_owned(), 1), traffic(3, 30)),
            ]),
            ..Connection::default()
        };

        assert_eq!(5, connection.produced_records());
        assert_eq!(50, connection.produced_bytes());
    }
}
