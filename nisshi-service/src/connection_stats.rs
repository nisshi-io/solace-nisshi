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

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, PoisonError},
    time::Instant,
};

use nisshi_sans_io::{FetchRequest, ProduceRequest, RequestInput, RootMessageMeta};
use rama::{Layer, Service, extensions::Extension, extensions::ExtensionsRef as _};
use uuid::Uuid;

/// The most topic-partitions that [`ConnectionStats`] tracks for Produce, and as many again for
/// Fetch, so a client that touches a great many partitions can't grow a connection's record
/// without bound. [`ConnectionSummary::produce_truncated`] and
/// [`ConnectionSummary::fetch_truncated`] report that a partition was left out.
pub const MAX_TRACKED_PARTITIONS: usize = 1024;

/// The `tracing` target of the broker's per-connection events, so that
/// `RUST_LOG=warn,nisshi::connection=info` turns them on without turning on anything else.
pub const CONNECTION_TARGET: &str = "nisshi::connection";

/// A topic and the index of one of its partitions.
pub type TopicPartition = (String, i32);

/// What a connection produced to one topic-partition.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProducedTraffic {
    /// The Produce requests that carried this partition.
    pub requests: u64,

    /// The records in those requests' batches.
    pub records: u64,

    /// The bytes of record data in those requests' batches.
    pub bytes: u64,
}

/// A snapshot of what one connection has done, from [`ConnectionStats::summary`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConnectionSummary {
    /// The `client_id` of the first request that carried one.
    pub client_id: Option<String>,

    /// The requests received, by API key.
    pub requests: BTreeMap<i16, u64>,

    /// The produce traffic, by topic-partition.
    pub produce: BTreeMap<TopicPartition, ProducedTraffic>,

    /// Whether a produced partition went untracked because of [`MAX_TRACKED_PARTITIONS`].
    pub produce_truncated: bool,

    /// The Fetch requests that named each topic-partition.
    pub fetch: BTreeMap<TopicPartition, u64>,

    /// Whether a fetched partition went untracked because of [`MAX_TRACKED_PARTITIONS`].
    pub fetch_truncated: bool,
}

impl ConnectionSummary {
    /// The requests received as comma-separated `Name=count` pairs, in API key order. A name is
    /// the message name without its `Request` suffix, or the API key for an API this build has no
    /// protocol metadata for.
    #[must_use]
    pub fn requests_text(&self) -> String {
        let messages = RootMessageMeta::messages().requests();

        self.requests
            .iter()
            .map(|(api_key, count)| match messages.get(api_key) {
                Some(meta) => format!(
                    "{}={count}",
                    meta.name.strip_suffix("Request").unwrap_or(meta.name)
                ),
                None => format!("{api_key}={count}"),
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    /// The produce traffic as `topic-partition=requests/records/bytes` triples.
    #[must_use]
    pub fn produce_text(&self) -> String {
        self.produce
            .iter()
            .map(|((topic, partition), traffic)| {
                format!(
                    "{topic}-{partition}={}/{}/{}",
                    traffic.requests, traffic.records, traffic.bytes
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    /// The fetched partitions as `topic-partition=requests` pairs.
    #[must_use]
    pub fn fetch_text(&self) -> String {
        self.fetch
            .iter()
            .map(|((topic, partition), requests)| format!("{topic}-{partition}={requests}"))
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Debug, Default)]
struct State {
    summary: ConnectionSummary,
}

/// What a connection has done, shared by every clone and kept in the connection's extensions.
///
/// The broker inserts one when it accepts a connection. `FrameRouteService` counts each routed
/// request by API key and `client_id`, and [`ProduceStatsLayer`] and [`FetchStatsLayer`] count
/// the topic-partitions of Produce and Fetch requests. The broker reads the
/// [`ConnectionSummary`] when the connection closes.
///
/// A connection serves its requests one at a time, so the lock is never contended by the
/// connection itself. Only a periodic snapshot reads it from another task.
#[derive(Clone, Debug, Extension)]
pub struct ConnectionStats {
    accepted: Instant,
    state: Arc<Mutex<State>>,
}

impl Default for ConnectionStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionStats {
    /// Starts the record of a connection accepted now.
    #[must_use]
    pub fn new() -> Self {
        Self {
            accepted: Instant::now(),
            state: Arc::default(),
        }
    }

    /// When the connection was accepted.
    #[must_use]
    pub fn accepted(&self) -> Instant {
        self.accepted
    }

    fn with<T>(&self, f: impl FnOnce(&mut ConnectionSummary) -> T) -> T {
        // A panic while the lock is held leaves counters that are still valid, so a later
        // reader takes them rather than panicking too.
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        f(&mut state.summary)
    }

    /// Counts one request of `api_key`, and keeps the first `client_id` that a request carries.
    pub fn record_request(&self, api_key: i16, client_id: Option<&str>) {
        self.with(|summary| {
            *summary.requests.entry(api_key).or_default() += 1;

            if summary.client_id.is_none() {
                summary.client_id = client_id.map(str::to_owned);
            }
        });
    }

    /// Counts one Produce request that carried `records` records of `bytes` bytes of record data
    /// for `topic` and `partition`.
    pub fn record_produce(&self, topic: &str, partition: i32, records: u64, bytes: u64) {
        self.with(|summary| {
            let key = (topic.to_owned(), partition);

            if !summary.produce.contains_key(&key)
                && summary.produce.len() >= MAX_TRACKED_PARTITIONS
            {
                summary.produce_truncated = true;
                return;
            }

            let traffic = summary.produce.entry(key).or_default();
            traffic.requests += 1;
            traffic.records += records;
            traffic.bytes += bytes;
        });
    }

    /// Counts one Fetch request that named `topic` and `partition`.
    pub fn record_fetch(&self, topic: &str, partition: i32) {
        self.with(|summary| {
            let key = (topic.to_owned(), partition);

            if !summary.fetch.contains_key(&key) && summary.fetch.len() >= MAX_TRACKED_PARTITIONS {
                summary.fetch_truncated = true;
                return;
            }

            *summary.fetch.entry(key).or_default() += 1;
        });
    }

    /// A copy of what the connection has done so far.
    #[must_use]
    pub fn summary(&self) -> ConnectionSummary {
        self.with(|summary| summary.clone())
    }
}

/// A [`Layer`] that records the topic-partitions of a Produce request in the connection's
/// [`ConnectionStats`], when the request's extensions have one, and passes the request on
/// unchanged.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProduceStatsLayer;

impl<S> Layer<S> for ProduceStatsLayer {
    type Service = ProduceStatsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ProduceStatsService { inner }
    }
}

/// A [`Service`] built by [`ProduceStatsLayer`].
#[derive(Clone)]
pub struct ProduceStatsService<S> {
    inner: S,
}

impl<S> fmt::Debug for ProduceStatsService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(ProduceStatsService)).finish()
    }
}

impl<S> Service<RequestInput<ProduceRequest>> for ProduceStatsService<S>
where
    S: Service<RequestInput<ProduceRequest>>,
{
    type Output = S::Output;
    type Error = S::Error;

    async fn serve(&self, req: RequestInput<ProduceRequest>) -> Result<Self::Output, Self::Error> {
        if let Some(stats) = req.extensions().get_ref::<ConnectionStats>() {
            for topic in req.request.topic_data.iter().flatten() {
                for partition in topic.partition_data.iter().flatten() {
                    let (records, bytes) = partition
                        .records
                        .iter()
                        .flat_map(|frame| &frame.batches)
                        .fold((0, 0), |(records, bytes), batch| {
                            (
                                records + u64::from(batch.record_count),
                                bytes + batch.record_data.len() as u64,
                            )
                        });

                    stats.record_produce(&topic.name, partition.index, records, bytes);
                }
            }
        }

        self.inner.serve(req).await
    }
}

/// A [`Layer`] that records the topic-partitions of a Fetch request in the connection's
/// [`ConnectionStats`], when the request's extensions have one, and passes the request on
/// unchanged.
#[derive(Clone, Copy, Debug, Default)]
pub struct FetchStatsLayer;

impl<S> Layer<S> for FetchStatsLayer {
    type Service = FetchStatsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        FetchStatsService { inner }
    }
}

/// A [`Service`] built by [`FetchStatsLayer`].
#[derive(Clone)]
pub struct FetchStatsService<S> {
    inner: S,
}

impl<S> fmt::Debug for FetchStatsService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(FetchStatsService)).finish()
    }
}

impl<S> Service<RequestInput<FetchRequest>> for FetchStatsService<S>
where
    S: Service<RequestInput<FetchRequest>>,
{
    type Output = S::Output;
    type Error = S::Error;

    async fn serve(&self, req: RequestInput<FetchRequest>) -> Result<Self::Output, Self::Error> {
        if let Some(stats) = req.extensions().get_ref::<ConnectionStats>() {
            for topic in req.request.topics.iter().flatten() {
                // Fetch v13 and later name a topic by its ID alone, which this layer can't
                // resolve to a name, so it records the ID.
                let name = match (&topic.topic, topic.topic_id) {
                    (Some(name), _) => name.clone(),
                    (None, Some(id)) => Uuid::from_bytes(id).to_string(),
                    (None, None) => continue,
                };

                for partition in topic.partitions.iter().flatten() {
                    stats.record_fetch(&name, partition.partition);
                }
            }
        }

        self.inner.serve(req).await
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;

    #[test]
    fn summary_text_names_apis_and_lists_partitions() {
        let stats = ConnectionStats::new();

        stats.record_request(0, Some("p"));
        stats.record_request(0, None);
        stats.record_request(3, None);
        stats.record_request(i16::MAX, None);
        stats.record_produce("t", 1, 5, 100);
        stats.record_fetch("t", 2);

        let summary = stats.summary();

        assert_eq!("Produce=2,Metadata=1,32767=1", summary.requests_text());
        assert_eq!("t-1=1/5/100", summary.produce_text());
        assert_eq!("t-2=1", summary.fetch_text());
    }

    #[test]
    fn requests_are_counted_by_api_key_and_keep_the_first_client_id() {
        let stats = ConnectionStats::new();

        stats.record_request(0, None);
        stats.record_request(0, Some("producer-1"));
        stats.record_request(0, Some("other"));
        stats.record_request(3, Some("producer-1"));

        let summary = stats.summary();

        assert_eq!(Some("producer-1"), summary.client_id.as_deref());
        assert_eq!(BTreeMap::from([(0, 3), (3, 1)]), summary.requests);
    }

    #[test]
    fn clones_share_one_record() {
        let stats = ConnectionStats::new();
        let clone = stats.clone();

        clone.record_request(1, Some("consumer-1"));

        assert_eq!(Some(&1), stats.summary().requests.get(&1));
    }

    #[test]
    fn produce_accumulates_per_topic_partition() {
        let stats = ConnectionStats::new();

        stats.record_produce("t", 0, 10, 1000);
        stats.record_produce("t", 0, 5, 500);
        stats.record_produce("t", 1, 1, 100);

        let summary = stats.summary();

        assert_eq!(
            Some(&ProducedTraffic {
                requests: 2,
                records: 15,
                bytes: 1500
            }),
            summary.produce.get(&("t".to_owned(), 0))
        );
        assert_eq!(
            Some(&ProducedTraffic {
                requests: 1,
                records: 1,
                bytes: 100
            }),
            summary.produce.get(&("t".to_owned(), 1))
        );
        assert!(!summary.produce_truncated);
    }

    #[test]
    fn produce_stops_tracking_new_partitions_at_the_cap_but_keeps_counting_known_ones() {
        let stats = ConnectionStats::new();

        for partition in 0..MAX_TRACKED_PARTITIONS {
            stats.record_produce("t", partition as i32, 1, 1);
        }

        stats.record_produce("t", MAX_TRACKED_PARTITIONS as i32, 1, 1);
        stats.record_produce("t", 0, 1, 1);

        let summary = stats.summary();

        assert_eq!(MAX_TRACKED_PARTITIONS, summary.produce.len());
        assert!(summary.produce_truncated);
        assert_matches!(
            summary.produce.get(&("t".to_owned(), 0)),
            Some(ProducedTraffic { requests: 2, .. })
        );
    }

    #[test]
    fn fetch_counts_per_topic_partition_and_truncates_at_the_cap() {
        let stats = ConnectionStats::new();

        stats.record_fetch("t", 0);
        stats.record_fetch("t", 0);

        for partition in 1..=MAX_TRACKED_PARTITIONS {
            stats.record_fetch("t", partition as i32);
        }

        let summary = stats.summary();

        assert_eq!(MAX_TRACKED_PARTITIONS, summary.fetch.len());
        assert!(summary.fetch_truncated);
        assert_eq!(Some(&2), summary.fetch.get(&("t".to_owned(), 0)));
    }
}
