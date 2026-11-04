// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

//! Each connection's read share: how many of a storage engine's reads in
//! flight one client connection may hold at once.
//!
//! A storage engine that limits its reads in flight counts an abandoned read
//! until its statement ends. Without a share, one connection whose reads are
//! abandoned in a row can hold most of that limit, and every other
//! connection's reads then wait for it. Kafka does not need a share, because
//! it handles one request per connection at a time and a read ends with its
//! request.
//!
//! The service that answers a request runs its reads in the connection's
//! share, with [`FetchReadShare::scope`] or [`ListOffsetsReadShare::scope`].
//! The engine takes a permit from that share, with
//! [`FetchReadShare::acquire`] or [`ListOffsetsReadShare::acquire`], before
//! it takes one from its own limit. The share lives in the connection's
//! [`Extensions`], so it outlives each request, and an abandoned read that
//! keeps its permit counts against the connection's next request.
//!
//! The share is a task-local of the task that called the [`Storage`]
//! method, so an engine that reads on a task of its own, as in `mpsc` mode,
//! sees no share.
//!
//! The share assumes one client per connection. A proxy that sends several
//! clients' requests over one connection, as `nisshi-proxy` does with its
//! pool of origin connections, gives those clients one share, so one
//! client's abandoned reads hold back the next client on that connection.
//! A client that reconnects gets a new share, while its abandoned reads
//! still hold the engine's permits. Kafka has no such carry-over, because
//! none of a request's work outlives its response.
//!
//! Not isolating clients behind `nisshi-proxy` is a decision, not an
//! oversight: the proxy is for development and demos, so a proxy in front of
//! a broker on PostgreSQL is not a supported production setup. Behind the
//! proxy the share bounds no single client. The proxy takes an origin
//! connection from its pool for each request, so one client's abandoned reads
//! can spread over many origin connections' shares, up to the engine's whole
//! limit.
//!
//! Keying the share on `client_id` would work without a proxy change,
//! because the proxy forwards a client's `client_id` on Fetch and
//! ListOffsets. It is not done because `client_id` is chosen by the client
//! and not authenticated, so it is no isolation boundary and clients
//! collide on its defaults, and because the share would then need a
//! broker-wide map with eviction instead of a per-connection extension, for
//! a setup that is not for production.
//!
//! [`Storage`]: crate::Storage

use std::sync::{Arc, OnceLock};

use rama::extensions::{Extension, Extensions};
use tokio::{
    sync::{AcquireError, OwnedSemaphorePermit, Semaphore},
    task::LocalKey,
};

use super::read_deadline::{QueueWait as _, ReadWait};

/// One connection's share of a storage engine's limit on reads in flight.
///
/// Clones count against the same share. The engine that first acquires a
/// permit sets how many permits the share has, because only the engine
/// knows its own limit.
#[derive(Clone, Debug, Default)]
struct ReadShare(Arc<OnceLock<Arc<Semaphore>>>);

impl ReadShare {
    /// Returns the share's permits, creating `size` of them on first use.
    fn permits(&self, size: usize) -> Arc<Semaphore> {
        self.0
            .get_or_init(|| Arc::new(Semaphore::new(size)))
            .clone()
    }

    /// Waits for a permit from the read share that `key` holds on this task. `size` is the number
    /// of permits that the share gets on its first use.
    ///
    /// Returns `None` without waiting when the task holds no share, as for a read that does not
    /// come from a client request.
    async fn acquire(
        key: &'static LocalKey<Self>,
        size: usize,
    ) -> Result<Option<OwnedSemaphorePermit>, AcquireError> {
        let Ok(permits) = key.try_with(|share| share.permits(size)) else {
            return Ok(None);
        };

        permits
            .acquire_owned()
            .queued_for(ReadWait::ConnectionReadLimit)
            .await
            .map(Some)
    }
}

tokio::task_local! {
    /// The share that [`FetchReadShare::acquire`] takes its permits from.
    static FETCH: ReadShare;

    /// The share that [`ListOffsetsReadShare::acquire`] takes its permits
    /// from.
    static LIST_OFFSETS: ReadShare;
}

/// A connection's share of the Fetch reads in flight.
#[derive(Clone, Debug, Default, Extension)]
pub struct FetchReadShare(ReadShare);

impl FetchReadShare {
    /// Returns the Fetch read share of the connection that `extensions`
    /// belongs to, inserting one on the connection's first Fetch.
    pub fn of(extensions: &Extensions) -> Self {
        extensions.get_ref_or_insert(Self::default).clone()
    }

    /// Runs `reads` with this share as the one that
    /// [`FetchReadShare::acquire`] takes its permits from.
    pub async fn scope<F>(self, reads: F) -> F::Output
    where
        F: Future,
    {
        FETCH.scope(self.0, reads).await
    }

    /// Waits for a permit from the Fetch read share of the enclosing
    /// [`FetchReadShare::scope`], creating `size` permits in the share if it
    /// has none yet.
    ///
    /// Returns `None` outside a scope.
    pub async fn acquire(size: usize) -> Result<Option<OwnedSemaphorePermit>, AcquireError> {
        ReadShare::acquire(&FETCH, size).await
    }
}

/// A connection's share of the ListOffsets reads in flight.
#[derive(Clone, Debug, Default, Extension)]
pub struct ListOffsetsReadShare(ReadShare);

impl ListOffsetsReadShare {
    /// Returns the ListOffsets read share of the connection that
    /// `extensions` belongs to, inserting one on the connection's first
    /// ListOffsets.
    pub fn of(extensions: &Extensions) -> Self {
        extensions.get_ref_or_insert(Self::default).clone()
    }

    /// Runs `reads` with this share as the one that
    /// [`ListOffsetsReadShare::acquire`] takes its permits from.
    pub async fn scope<F>(self, reads: F) -> F::Output
    where
        F: Future,
    {
        LIST_OFFSETS.scope(self.0, reads).await
    }

    /// Waits for a permit from the ListOffsets read share of the enclosing
    /// [`ListOffsetsReadShare::scope`], creating `size` permits in the share
    /// if it has none yet.
    ///
    /// Returns `None` outside a scope.
    pub async fn acquire(size: usize) -> Result<Option<OwnedSemaphorePermit>, AcquireError> {
        ReadShare::acquire(&LIST_OFFSETS, size).await
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use tokio::time::{Duration, Instant, timeout};

    use super::*;
    use crate::{
        StorageOperation,
        service::read_deadline::{Missed, ReadDeadline},
    };

    #[tokio::test]
    async fn acquire_outside_a_scope_returns_none() -> Result<(), AcquireError> {
        assert!(FetchReadShare::acquire(1).await?.is_none());
        assert!(ListOffsetsReadShare::acquire(1).await?.is_none());
        Ok(())
    }

    /// A ListOffsets read inside a Fetch scope takes nothing from the Fetch
    /// share, and the other way round.
    #[tokio::test]
    async fn each_kind_of_read_takes_only_its_own_share() -> Result<(), AcquireError> {
        assert!(
            FetchReadShare::default()
                .scope(ListOffsetsReadShare::acquire(1))
                .await?
                .is_none()
        );
        assert!(
            ListOffsetsReadShare::default()
                .scope(FetchReadShare::acquire(1))
                .await?
                .is_none()
        );
        Ok(())
    }

    /// A read waits for a permit while the share's permits are held, and
    /// one abandoned during that wait is a miss queued for its connection's
    /// share.
    #[tokio::test(start_paused = true)]
    async fn read_waiting_for_its_share_is_abandoned_as_queued() {
        let deadline = Instant::now() + Duration::from_secs(1);

        let mut deadline = ReadDeadline::new(StorageOperation::Fetch, deadline);

        let outcome = FetchReadShare::default()
            .scope(deadline.timeout(async {
                let held = FetchReadShare::acquire(1).await;
                assert!(held.as_ref().is_ok_and(Option::is_some), "{held:?}");

                _ = FetchReadShare::acquire(1).await;
                pending::<()>().await
            }))
            .await;

        assert_eq!(Err(Missed::Queued(ReadWait::ConnectionReadLimit)), outcome);
    }

    /// A permit taken in one scope counts against every scope with a clone
    /// of the same share, and not against another share.
    #[tokio::test]
    async fn clones_of_a_share_count_against_it() -> Result<(), AcquireError> {
        let share = FetchReadShare::default();
        let held = share.clone().scope(FetchReadShare::acquire(1)).await?;
        assert!(held.is_some());

        assert!(
            timeout(
                Duration::from_millis(50),
                share.clone().scope(FetchReadShare::acquire(1))
            )
            .await
            .is_err(),
            "a clone of the share took a second permit"
        );

        assert!(
            FetchReadShare::default()
                .scope(FetchReadShare::acquire(1))
                .await?
                .is_some()
        );

        drop(held);
        assert!(share.scope(FetchReadShare::acquire(1)).await?.is_some());

        Ok(())
    }

    /// Requests on one connection share its read share, and each connection
    /// that a listener accepts has a share of its own.
    #[test]
    fn each_connection_has_its_own_share() {
        let listener = Extensions::default();
        let connection = listener.fork();
        let other_connection = listener.fork();

        let share = FetchReadShare::of(&connection).0.0;

        assert!(Arc::ptr_eq(&share, &FetchReadShare::of(&connection).0.0));
        assert!(Arc::ptr_eq(
            &share,
            &FetchReadShare::of(&connection.clone()).0.0
        ));
        assert!(!Arc::ptr_eq(
            &share,
            &FetchReadShare::of(&other_connection).0.0
        ));
        assert!(!Arc::ptr_eq(
            &share,
            &ListOffsetsReadShare::of(&connection).0.0
        ));
    }
}
