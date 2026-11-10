// Copyright ⓒ Solace Corporation
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

//! Limits how often the pool opens a connection while its address leads to a read-only server.

use std::{
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

#[cfg(test)]
use deadpool_postgres::Timeouts;
use deadpool_postgres::{HookError, Object, Pool, PoolError, Status};
use nisshi_storage::Error;
use tokio::time::Instant;

/// How long a gated pool waits between two attempts to open a connection.
///
/// Each new connection costs a TCP, TLS and SCRAM handshake, and a new backend process on the
/// server. While the address leads to a read-only server, each new connection fails its check.
/// Without this interval, the pool opens a connection for every request until the address leads
/// to a writable server.
pub(super) const GATED_CONNECT_INTERVAL: Duration = Duration::from_secs(1);

/// The gate that limits how often a pool opens a connection: once per [`GATED_CONNECT_INTERVAL`]
/// while the pool is gated, and without a limit while it is not.
#[derive(Debug)]
pub(super) struct ConnectGate {
    /// Whether the pool is gated. A checkout reads it without the lock, so a checkout of a pool
    /// that is not gated never waits for the lock.
    gated: AtomicBool,
    /// When the next attempt to open a connection may start while the pool is gated.
    next_connect: Mutex<Instant>,
}

impl Default for ConnectGate {
    fn default() -> Self {
        Self {
            gated: AtomicBool::new(false),
            next_connect: Mutex::new(Instant::now()),
        }
    }
}

impl ConnectGate {
    /// Starts gating, or restarts it if the pool is already gated: the next attempt to open a
    /// connection may start one [`GATED_CONNECT_INTERVAL`] from now.
    pub(super) fn start_or_restart_gating(&self) {
        let mut next_connect = self.next_connect();
        *next_connect = Instant::now() + GATED_CONNECT_INTERVAL;
        self.gated.store(true, Ordering::Release);
    }

    /// Stops gating, so each checkout may open a connection again.
    pub(super) fn stop_gating(&self) {
        self.gated.store(false, Ordering::Release);
    }

    pub(super) fn is_gated(&self) -> bool {
        self.gated.load(Ordering::Acquire)
    }

    /// Takes the slot for one attempt to open a connection, and returns whether it took it.
    ///
    /// While the pool is gated, one call takes the slot after each [`GATED_CONNECT_INTERVAL`],
    /// and the next slot opens one interval after it. The slot is taken when the attempt starts,
    /// and the attempt does not give it back. So an attempt that hangs, or whose caller drops
    /// it, holds up no other attempt for longer than the interval.
    pub(super) fn try_take_connect_slot(&self) -> bool {
        if !self.is_gated() {
            return true;
        }

        let mut next_connect = self.next_connect();
        let now = Instant::now();

        // A new connection can stop gating between the first read and the lock.
        if !self.is_gated() || now >= *next_connect {
            *next_connect = now + GATED_CONNECT_INTERVAL;
            true
        } else {
            false
        }
    }

    fn next_connect(&self) -> std::sync::MutexGuard<'_, Instant> {
        self.next_connect
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// Why a checkout from a [`GatedPool`] failed.
#[derive(Debug)]
pub(super) enum GatedCheckoutError {
    /// The pool is gated, it holds no idle connection, and the checkout could not take a connect
    /// slot.
    Gated,
    /// The pool failed the checkout.
    Pool(PoolError),
}

impl From<GatedCheckoutError> for Error {
    /// Converts a gated checkout to the error of a failed `post_create` hook, which is transient,
    /// so a client retries it as it retries the checkout that found the read-only server.
    fn from(value: GatedCheckoutError) -> Self {
        match value {
            GatedCheckoutError::Gated => Self::from(PoolError::PostCreateHook(HookError::message(
                "the pool opens at most one connection per second while new connections reach a \
                read-only server",
            ))),
            GatedCheckoutError::Pool(error) => Self::from(error),
        }
    }
}

/// A pool whose checkouts open a new connection only when they take a connect slot from its
/// [`ConnectGate`].
///
/// A checkout that finds an idle connection takes it whether the pool is gated or not, because a
/// read-only server still serves reads. Only an attempt to open a connection costs the server a
/// handshake and a backend.
///
/// The limit is approximate, because the gate reads the pool's status before the pool takes a
/// connection. If two checkouts find the same idle connection, the second opens a new connection.
/// If the idle connection fails its recycle check, the checkout opens a new connection too.
#[derive(Clone, Debug)]
pub(super) struct GatedPool {
    pool: Pool,
    gate: Arc<ConnectGate>,
}

impl GatedPool {
    /// Returns `pool` gated by `gate`. The pool's `post_create` hook must start and stop `gate`.
    pub(super) fn new(pool: Pool, gate: Arc<ConnectGate>) -> Self {
        Self { pool, gate }
    }

    /// Checks out a connection, and fails with [`GatedCheckoutError::Gated`] if the pool is
    /// gated, would open a connection, and the checkout can't take a connect slot.
    ///
    /// The pool would open a connection only when it holds no idle connection and has room for
    /// one more. A checkout of a full pool waits for a connection to come back, as it does when
    /// the pool is not gated, and takes no slot.
    pub(super) async fn get(&self) -> Result<Object, GatedCheckoutError> {
        if self.gate.is_gated() {
            // The pool reads its status under a lock, so a checkout reads it only while gated.
            let status = self.pool.status();

            if status.available == 0
                && status.size < status.max_size
                && !self.gate.try_take_connect_slot()
            {
                return Err(GatedCheckoutError::Gated);
            }
        }

        self.pool.get().await.map_err(GatedCheckoutError::Pool)
    }

    pub(super) fn status(&self) -> Status {
        self.pool.status()
    }

    #[cfg(test)]
    pub(super) fn timeouts(&self) -> Timeouts {
        self.pool.timeouts()
    }

    #[cfg(test)]
    pub(super) fn resize(&self, max_size: usize) {
        self.pool.resize(max_size);
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn gate_admits_every_attempt_until_gating_starts() {
        let gate = ConnectGate::default();

        assert!(!gate.is_gated());
        assert!(gate.try_take_connect_slot());
        assert!(gate.try_take_connect_slot());
    }

    #[tokio::test(start_paused = true)]
    async fn gated_gate_admits_one_attempt_per_interval() {
        let gate = ConnectGate::default();
        gate.start_or_restart_gating();

        assert!(!gate.try_take_connect_slot());

        tokio::time::advance(GATED_CONNECT_INTERVAL).await;
        assert!(gate.try_take_connect_slot());
        assert!(!gate.try_take_connect_slot());

        // One millisecond before the next slot opens, one interval after the last slot was taken.
        tokio::time::advance(GATED_CONNECT_INTERVAL - Duration::from_millis(1)).await;
        assert!(!gate.try_take_connect_slot());

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(gate.try_take_connect_slot());
        assert!(gate.is_gated());
    }

    #[tokio::test(start_paused = true)]
    async fn restarting_gating_counts_the_interval_from_now() {
        let gate = ConnectGate::default();
        gate.start_or_restart_gating();

        tokio::time::advance(GATED_CONNECT_INTERVAL).await;
        gate.start_or_restart_gating();

        assert!(!gate.try_take_connect_slot());
    }

    #[tokio::test(start_paused = true)]
    async fn stopped_gate_admits_every_attempt() {
        let gate = ConnectGate::default();
        gate.start_or_restart_gating();

        assert!(gate.is_gated());
        assert!(!gate.try_take_connect_slot());

        // Stopping halfway through the interval shows that the gate gives slots at once, instead
        // of waiting for the interval to end.
        tokio::time::advance(GATED_CONNECT_INTERVAL / 2).await;
        assert!(!gate.try_take_connect_slot());

        gate.stop_gating();

        assert!(!gate.is_gated());
        assert!(gate.try_take_connect_slot());
        assert!(gate.try_take_connect_slot());
    }

    #[test]
    fn gated_checkout_is_a_transient_pool_error() {
        let error = Error::from(GatedCheckoutError::Gated);

        assert_matches!(&error, Error::Pool(_));
        assert!(error.is_transient(), "{error:?}");
    }
}
