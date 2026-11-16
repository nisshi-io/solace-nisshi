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

use crate::{connection::Connection, log::Log, table::Table};

/// The Markdown report of a [`Log`]: connections per client kind, per client, and optionally per
/// connection.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Report<'a> {
    log: &'a Log,
    per_connection: bool,
}

impl<'a> Report<'a> {
    pub(crate) fn new(log: &'a Log, per_connection: bool) -> Self {
        Self {
            log,
            per_connection,
        }
    }

    /// The connections of each client, in the order of the client ids. Connections that sent no
    /// request have no client id, and come first.
    fn by_client(&self) -> BTreeMap<Option<&'a str>, Vec<&'a Connection>> {
        let mut clients: BTreeMap<_, Vec<_>> = BTreeMap::new();

        for connection in self.log.connections.values() {
            clients
                .entry(connection.client_id.as_deref())
                .or_default()
                .push(connection);
        }

        clients
    }

    fn kinds(&self) -> Table {
        let mut kinds: BTreeMap<&str, Vec<usize>> = BTreeMap::new();

        for connections in self.by_client().into_values() {
            if let Some(first) = connections.first() {
                kinds
                    .entry(first.client_kind())
                    .or_default()
                    .push(connections.len());
            }
        }

        let mut table = Table::new(&[
            "kind",
            "clients",
            "connections",
            "min per client",
            "mean per client",
            "max per client",
        ]);

        for (kind, counts) in kinds {
            let total = counts.iter().sum::<usize>();

            table.row(vec![
                kind.to_owned(),
                counts.len().to_string(),
                total.to_string(),
                counts.iter().min().copied().unwrap_or_default().to_string(),
                format!("{:.1}", total as f64 / counts.len() as f64),
                counts.iter().max().copied().unwrap_or_default().to_string(),
            ]);
        }

        table
    }

    fn clients(&self) -> Table {
        let mut table = Table::new(&[
            "client id",
            "connections",
            "requests",
            "partitions produced to",
            "records produced",
            "partitions fetched",
            "truncated",
        ]);

        for connections in self.by_client().into_values() {
            let mut requests: BTreeMap<&str, u64> = BTreeMap::new();
            let mut produced = Vec::new();
            let mut fetched = Vec::new();

            for connection in &connections {
                for (name, count) in &connection.requests {
                    *requests.entry(name).or_default() += count;
                }

                produced.extend(connection.produced.keys());
                fetched.extend(connection.fetched.keys());
            }

            produced.sort();
            produced.dedup();
            fetched.sort();
            fetched.dedup();

            let truncated = connections
                .iter()
                .any(|connection| connection.produce_truncated || connection.fetch_truncated);

            table.row(vec![
                connections
                    .first()
                    .map_or("", |connection| connection.client_label())
                    .to_owned(),
                connections.len().to_string(),
                request_list(requests),
                produced.len().to_string(),
                connections
                    .iter()
                    .map(|connection| connection.produced_records())
                    .sum::<u64>()
                    .to_string(),
                fetched.len().to_string(),
                if truncated { "yes" } else { "" }.to_owned(),
            ]);
        }

        table
    }

    fn connections(&self) -> Table {
        let mut table = Table::new(&[
            "id",
            "peer",
            "client id",
            "state",
            "age ms",
            "requests",
            "produced",
            "fetched",
        ]);

        for connection in self.log.connections.values() {
            table.row(vec![
                connection.id.to_string(),
                connection.peer.clone(),
                connection.client_label().to_owned(),
                connection.state.to_string(),
                connection.age_ms.to_string(),
                request_list(
                    connection
                        .requests
                        .iter()
                        .map(|(name, count)| (name.as_str(), *count))
                        .collect(),
                ),
                partition_list(connection.produced.keys()),
                partition_list(connection.fetched.keys()),
            ]);
        }

        table
    }
}

/// Returns the requests as space-separated `Name=count` pairs, or `-` when there are none.
pub(crate) fn request_list(requests: BTreeMap<&str, u64>) -> String {
    if requests.is_empty() {
        return "-".into();
    }

    requests
        .iter()
        .map(|(name, count)| format!("{name}={count}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn partition_list<'a>(partitions: impl Iterator<Item = &'a (String, i32)>) -> String {
    let partitions = partitions
        .map(|(topic, partition)| format!("{topic}-{partition}"))
        .collect::<Vec<_>>();

    if partitions.is_empty() {
        "-".into()
    } else {
        partitions.join(",")
    }
}

impl fmt::Display for Report<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{} connections accepted, {} open at the peak, {} still open when the log ends.\n",
            self.log.connections.len(),
            self.log.peak_open,
            self.log.still_open()
        )?;

        write!(
            f,
            "## Connections per client kind\n\n{}\n\n## Per client\n\n{}",
            self.kinds(),
            self.clients()
        )?;

        if self.per_connection {
            write!(f, "\n\n## Per connection\n\n{}", self.connections())?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nisshi_service::ProducedTraffic;

    use super::*;
    use crate::connection::ConnectionState;

    fn closed(id: u64, client_id: &str, requests: &[(&str, u64)]) -> Connection {
        Connection {
            id,
            client_id: Some(client_id.to_owned()),
            state: ConnectionState::Closed,
            requests: requests
                .iter()
                .map(|(name, count)| ((*name).to_owned(), *count))
                .collect(),
            ..Connection::default()
        }
    }

    fn produced(partition: i32, records: u64) -> BTreeMap<(String, i32), ProducedTraffic> {
        BTreeMap::from([(
            ("t".to_owned(), partition),
            ProducedTraffic {
                requests: 1,
                records,
                bytes: records * 10,
            },
        )])
    }

    fn log() -> Log {
        let mut connections = vec![
            closed(1, "adminclient-1", &[("ApiVersions", 1)]),
            Connection {
                produced: produced(0, 50),
                ..closed(2, "producer-1", &[("Produce", 5)])
            },
            closed(3, "producer-1", &[("Metadata", 1)]),
            Connection {
                produced: produced(1, 20),
                ..closed(4, "producer-2", &[("Produce", 2)])
            },
            Connection {
                state: ConnectionState::Snapshot,
                fetched: BTreeMap::from([(("t".to_owned(), 0), 4), (("t".to_owned(), 1), 4)]),
                ..closed(5, "consumer-1", &[("Fetch", 8)])
            },
        ];
        connections.push(Connection {
            id: 6,
            ..Connection::default()
        });

        Log {
            connections: connections
                .into_iter()
                .map(|connection| (connection.id, connection))
                .collect(),
            peak_open: 3,
        }
    }

    fn row<'a>(report: &'a str, first_cell: &str) -> Vec<&'a str> {
        report
            .lines()
            .find(|line| line.starts_with(&format!("| {first_cell} ")))
            .unwrap_or_else(|| panic!("no row for {first_cell}:\n{report}"))
            .trim_matches('|')
            .split('|')
            .map(str::trim)
            .collect()
    }

    #[test]
    fn the_summary_line_counts_accepted_peak_and_open_connections() {
        let report = Report::new(&log(), false).to_string();

        assert_eq!(
            "6 connections accepted, 3 open at the peak, 2 still open when the log ends.",
            report.lines().next().unwrap()
        );
    }

    #[test]
    fn clients_are_grouped_by_kind_with_their_connection_counts() {
        let report = Report::new(&log(), false).to_string();

        // Two producers with three connections between them: one has two.
        assert_eq!(
            ["producer", "2", "3", "1", "1.5", "2"],
            row(&report, "producer")[..]
        );
        assert_eq!(
            ["(no request)", "1", "1", "1", "1.0", "1"],
            row(&report, "(no request)")[..]
        );
    }

    #[test]
    fn a_client_reports_the_partitions_it_used_across_its_connections() {
        let report = Report::new(&log(), false).to_string();

        assert_eq!(
            [
                "producer-1",
                "2",
                "Metadata=1 Produce=5",
                "1",
                "50",
                "0",
                ""
            ],
            row(&report, "producer-1")[..]
        );
        assert_eq!(
            ["consumer-1", "1", "Fetch=8", "0", "0", "2", ""],
            row(&report, "consumer-1")[..]
        );
    }

    #[test]
    fn a_truncated_connection_marks_its_client() {
        let mut log = log();
        log.connections.get_mut(&4).unwrap().produce_truncated = true;

        let report = Report::new(&log, false).to_string();

        assert_eq!("yes", *row(&report, "producer-2").last().unwrap());
    }

    #[test]
    fn the_per_connection_table_is_optional() {
        assert!(
            !Report::new(&log(), false)
                .to_string()
                .contains("## Per connection")
        );

        let report = Report::new(&log(), true).to_string();

        assert!(report.contains("## Per connection"));
        assert_eq!(
            [
                "5",
                "",
                "consumer-1",
                "snapshot",
                "0",
                "Fetch=8",
                "-",
                "t-0,t-1"
            ],
            row(&report, "5")[..]
        );
    }
}
