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

//! The connections a broker has open: what the `nisshi::connection` events and the connection
//! metrics report.
//!
//! A connection registers when the broker accepts it, and a [`ConnectionGuard`] removes it when
//! the connection's task ends, however it ends: a task that the broker aborts drops its guard
//! too, so neither the registry nor the open-connections gauge keeps a connection that is gone.
//! The metrics carry no `client_id` or topic labels, because a client may choose its `client_id`
//! and a topic may have any number of partitions. Those details go only to the log.

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{Arc, LazyLock, Mutex, PoisonError},
};

use nisshi_service::{CONNECTION_TARGET, ConnectionStats};
use opentelemetry::metrics::{Counter, UpDownCounter};
use tracing::info;

use crate::METER;

static ACCEPTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_connections_accepted")
        .with_description("connections that the broker accepted")
        .build()
});

static OPEN: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    METER
        .i64_up_down_counter("nisshi_connections_open")
        .with_description("connections that the broker has open")
        .build()
});

#[derive(Clone, Debug)]
struct Connection {
    addr: SocketAddr,
    stats: ConnectionStats,
}

/// The open connections of a broker, shared by every clone of it.
#[derive(Clone, Debug, Default)]
pub(super) struct ConnectionRegistry {
    open: Arc<Mutex<BTreeMap<u64, Connection>>>,
}

impl ConnectionRegistry {
    /// Records that the broker accepted connection `id` from `addr`, and returns the guard that
    /// removes it.
    pub(super) fn register(
        &self,
        id: u64,
        addr: SocketAddr,
        stats: ConnectionStats,
    ) -> ConnectionGuard {
        self.with(|open| _ = open.insert(id, Connection { addr, stats }));

        ACCEPTED.add(1, &[]);
        OPEN.add(1, &[]);

        ConnectionGuard {
            id,
            registry: self.clone(),
        }
    }

    fn with<T>(&self, f: impl FnOnce(&mut BTreeMap<u64, Connection>) -> T) -> T {
        // A panic while the lock is held leaves a map that is still valid.
        f(&mut self.open.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// How many connections are open.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.with(|open| open.len())
    }

    /// Logs the state of each open connection, and how many there are.
    pub(super) fn log_snapshot(&self) {
        let open = self.with(|open| open.clone());

        info!(target: CONNECTION_TARGET, open = open.len(), "connections open");

        for (id, connection) in open {
            log_summary(
                "connection snapshot",
                id,
                connection.addr,
                &connection.stats,
            );
        }
    }
}

/// Removes its connection from the [`ConnectionRegistry`] when dropped.
#[derive(Debug)]
pub(super) struct ConnectionGuard {
    id: u64,
    registry: ConnectionRegistry,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.registry.with(|open| _ = open.remove(&self.id));

        OPEN.add(-1, &[]);
    }
}

/// Logs what `stats` holds of connection `id`, from `addr`, under `message`.
pub(super) fn log_summary(
    message: &'static str,
    id: u64,
    addr: SocketAddr,
    stats: &ConnectionStats,
) {
    let summary = stats.summary();

    info!(
        target: CONNECTION_TARGET,
        connection = id,
        %addr,
        client_id = summary.client_id.as_deref(),
        age_ms = u64::try_from(stats.accepted().elapsed().as_millis()).unwrap_or(u64::MAX),
        requests = %summary.requests_text(),
        produce = %summary.produce_text(),
        produce_truncated = summary.produce_truncated,
        fetch = %summary.fetch_text(),
        fetch_truncated = summary.fetch_truncated,
        "{message}"
    );
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, port))
    }

    #[test]
    fn a_connection_is_open_until_its_guard_drops() {
        let registry = ConnectionRegistry::default();

        let first = registry.register(1, addr(1), ConnectionStats::new());
        let second = registry.register(2, addr(2), ConnectionStats::new());
        assert_eq!(2, registry.len());

        drop(first);
        assert_eq!(1, registry.len());

        drop(second);
        assert_eq!(0, registry.len());
    }

    #[test]
    fn clones_share_the_open_connections() {
        let registry = ConnectionRegistry::default();
        let clone = registry.clone();

        let _guard = registry.register(1, addr(1), ConnectionStats::new());

        assert_eq!(1, clone.len());
    }
}
