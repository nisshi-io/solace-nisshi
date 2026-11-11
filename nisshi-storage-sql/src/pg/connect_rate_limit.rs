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
use deadpool_postgres::{Object, Pool, PoolError, Status};
use nisshi_storage::Error;
use tokio::time::Instant;

/// How long a rate-limited pool waits between two attempts to open a connection.
///
/// Each new connection costs a TCP, TLS and SCRAM handshake, and a new backend process on the
/// server. While the address leads to a read-only server, each new connection fails its check.
/// Without this interval, the pool opens a connection for every request until the address leads
/// to a writable server.
pub(super) const RATE_LIMITED_CONNECT_INTERVAL: Duration = Duration::from_secs(1);

/// The rate limit on how often a pool opens a connection: once per
/// [`RATE_LIMITED_CONNECT_INTERVAL`] while the limit is on, and without a limit while it is off.
#[derive(Debug)]
pub(super) struct ConnectRateLimit {
    /// Whether the limit is on. A checkout reads it without the lock, so a checkout of a pool
    /// whose limit is off never waits for the lock.
    limiting: AtomicBool,
    /// When the next attempt to open a connection may start while the limit is on.
    next_connect: Mutex<Instant>,
}

impl Default for ConnectRateLimit {
    fn default() -> Self {
        Self {
            limiting: AtomicBool::new(false),
            next_connect: Mutex::new(Instant::now()),
        }
    }
}

impl ConnectRateLimit {
    /// Turns the limit on, or restarts it if it is already on: the next attempt to open a
    /// connection may start one [`RATE_LIMITED_CONNECT_INTERVAL`] from now. Returns whether the
    /// limit was off before the call.
    pub(super) fn start_or_restart(&self) -> bool {
        let mut next_connect = self.next_connect();
        *next_connect = Instant::now() + RATE_LIMITED_CONNECT_INTERVAL;
        !self.limiting.swap(true, Ordering::AcqRel)
    }

    /// Turns the limit off, so each checkout may open a connection again. Returns whether the
    /// limit was on before the call.
    pub(super) fn stop(&self) -> bool {
        self.limiting.swap(false, Ordering::AcqRel)
    }

    pub(super) fn is_limiting(&self) -> bool {
        self.limiting.load(Ordering::Acquire)
    }

    /// Takes the slot for one attempt to open a connection, and returns whether it took it.
    ///
    /// While the limit is on, one call takes the slot after each
    /// [`RATE_LIMITED_CONNECT_INTERVAL`], and the next slot opens one interval after it. The slot
    /// is taken when the attempt starts, and the attempt does not give it back. So an attempt
    /// that hangs, or whose caller drops it, holds up no other attempt for longer than the
    /// interval.
    pub(super) fn try_take_connect_slot(&self) -> bool {
        if !self.is_limiting() {
            return true;
        }

        let mut next_connect = self.next_connect();
        let now = Instant::now();

        // A new connection can turn the limit off between the first read and the lock.
        if !self.is_limiting() || now >= *next_connect {
            *next_connect = now + RATE_LIMITED_CONNECT_INTERVAL;
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

/// Why a checkout from a [`RateLimitedPool`] failed.
#[derive(Debug)]
pub(super) enum CheckoutError {
    /// The rate limit is on, the pool holds no idle connection, and the checkout could not take
    /// a connect slot.
    RateLimited,
    /// The pool failed the checkout.
    Pool(PoolError),
}

impl From<CheckoutError> for Error {
    fn from(value: CheckoutError) -> Self {
        match value {
            CheckoutError::RateLimited => Self::PoolConnectRateLimited {
                interval: RATE_LIMITED_CONNECT_INTERVAL,
            },
            CheckoutError::Pool(error) => Self::from(error),
        }
    }
}

/// A pool whose checkouts open a new connection only when they take a connect slot from its
/// [`ConnectRateLimit`].
///
/// A checkout that finds an idle connection takes it whether the limit is on or not, because a
/// read-only server still serves reads. Only an attempt to open a connection costs the server a
/// handshake and a backend.
///
/// The limit is approximate, because the pool reads its status before it takes a connection. If
/// two checkouts find the same idle connection, the second opens a new connection. If the idle
/// connection fails its recycle check, the checkout opens a new connection too.
#[derive(Clone, Debug)]
pub(super) struct RateLimitedPool {
    pool: Pool,
    rate_limit: Arc<ConnectRateLimit>,
}

impl RateLimitedPool {
    /// Returns `pool` limited by `rate_limit`. The pool's `post_create` hook must start and stop
    /// `rate_limit`.
    pub(super) fn new(pool: Pool, rate_limit: Arc<ConnectRateLimit>) -> Self {
        Self { pool, rate_limit }
    }

    /// Checks out a connection, and fails with [`CheckoutError::RateLimited`] if the rate limit
    /// is on, the pool would open a connection, and the checkout can't take a connect slot.
    ///
    /// The pool would open a connection only when it holds no idle connection and has room for
    /// one more. A checkout of a full pool waits for a connection to come back, as it does when
    /// the limit is off, and takes no slot.
    pub(super) async fn get(&self) -> Result<Object, CheckoutError> {
        if self.rate_limit.is_limiting() {
            // The pool reads its status under a lock, so a checkout reads it only while the limit
            // is on.
            let status = self.pool.status();

            if status.available == 0
                && status.size < status.max_size
                && !self.rate_limit.try_take_connect_slot()
            {
                return Err(CheckoutError::RateLimited);
            }
        }

        self.pool.get().await.map_err(CheckoutError::Pool)
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
    async fn rate_limit_admits_every_attempt_until_it_starts() {
        let rate_limit = ConnectRateLimit::default();

        assert!(!rate_limit.is_limiting());
        assert!(rate_limit.try_take_connect_slot());
        assert!(rate_limit.try_take_connect_slot());
    }

    #[tokio::test(start_paused = true)]
    async fn started_rate_limit_admits_one_attempt_per_interval() {
        let rate_limit = ConnectRateLimit::default();
        assert!(rate_limit.start_or_restart());

        assert!(!rate_limit.try_take_connect_slot());

        tokio::time::advance(RATE_LIMITED_CONNECT_INTERVAL).await;
        assert!(rate_limit.try_take_connect_slot());
        assert!(!rate_limit.try_take_connect_slot());

        // One millisecond before the next slot opens, one interval after the last slot was taken.
        tokio::time::advance(RATE_LIMITED_CONNECT_INTERVAL - Duration::from_millis(1)).await;
        assert!(!rate_limit.try_take_connect_slot());

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(rate_limit.try_take_connect_slot());
        assert!(rate_limit.is_limiting());
    }

    #[tokio::test(start_paused = true)]
    async fn restarting_the_rate_limit_counts_the_interval_from_now() {
        let rate_limit = ConnectRateLimit::default();
        assert!(rate_limit.start_or_restart());

        tokio::time::advance(RATE_LIMITED_CONNECT_INTERVAL).await;
        assert!(!rate_limit.start_or_restart());

        assert!(!rate_limit.try_take_connect_slot());
    }

    #[tokio::test(start_paused = true)]
    async fn stopped_rate_limit_admits_every_attempt() {
        let rate_limit = ConnectRateLimit::default();
        assert!(rate_limit.start_or_restart());

        assert!(rate_limit.is_limiting());
        assert!(!rate_limit.try_take_connect_slot());

        // Stopping halfway through the interval shows that the stopped limit gives slots at once,
        // instead of waiting for the interval to end.
        tokio::time::advance(RATE_LIMITED_CONNECT_INTERVAL / 2).await;
        assert!(!rate_limit.try_take_connect_slot());

        assert!(rate_limit.stop());
        assert!(!rate_limit.stop());

        assert!(!rate_limit.is_limiting());
        assert!(rate_limit.try_take_connect_slot());
        assert!(rate_limit.try_take_connect_slot());
    }

    #[test]
    fn rate_limited_checkout_is_a_transient_error() {
        let error = Error::from(CheckoutError::RateLimited);

        assert_matches!(&error, Error::PoolConnectRateLimited { .. });
        assert!(error.is_transient(), "{error:?}");
    }
}
