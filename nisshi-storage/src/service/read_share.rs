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

//! Each connection's and each client's read share: how many of a storage
//! engine's reads in flight one client connection, and one client over all its
//! connections, may hold at once.
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
//! # The client share
//!
//! A connection's share does not bound a client that opens several
//! connections, or that reconnects, because each connection has a share of its
//! own while its abandoned reads still hold the engine's permits. So an engine
//! also keeps a [`ClientReadShares`], which gives each [`ClientIdentity`], the
//! authenticated principal and the `client_id` of the request, one share of
//! its own. A read takes a permit from its connection's share, then from its
//! client's share, then from the engine's limit, so a client never waits on
//! the engine's limit for more reads than its share.
//!
//! A client's share is dropped once no read of that client holds a permit. A
//! client whose abandoned reads still hold permits keeps its share, so
//! reconnecting does not hand it a fresh one.
//!
//! ## What a client sees at the bound
//!
//! Nothing but a wait. A read past its client's share waits for a permit in
//! the broker. The connection stays open, the response carries no error and
//! `throttle_time_ms` stays 0. If the request's deadline passes first, a Fetch
//! answers the partition with no records, and a ListOffsets answers
//! `REQUEST_TIMED_OUT` for it, and the `nisshi_storage_read_deadline_exceeded`
//! metric rises with `stage` set to `queued` and `limit` set to `client`.
//!
//! ## Kafka
//!
//! Kafka 3.9.1 has no limit like this, and none of what it has is on by
//! default. Its connection limits, `max.connections.per.ip`, `max.connections`
//! and `max.connection.creation.rate`, default to
//! [`Integer.MAX_VALUE`][connection-defaults]. A connection past
//! `max.connections.per.ip` is closed as soon as the broker accepts it, and
//! the client sees the connection reset
//! ([`SocketServer.scala`][connection-rejected]). Past `max.connections`, the
//! acceptor waits for a free slot and closes nothing
//! ([`SocketServer.scala`][connection-wait]). Past
//! `max.connection.creation.rate`, the broker holds the new socket and closes
//! it when the throttle time ends ([`SocketServer.scala`][connection-throttled]).
//! Its request and bandwidth quotas are keyed on the user and the `client_id`
//! ([`ClientQuotaManager.scala`][quota-key]), default to
//! [`Long.MaxValue`][quota-default], and a client over one gets a response
//! with `throttle_time_ms` set and its connection muted for that time. A Fetch
//! over its quota gets an empty response
//! ([`KafkaApis.scala`][quota-throttle]). The client share is keyed the same
//! way as the quota entity. It is stricter than Kafka, as a Kafka broker does
//! not isolate such a client at all, and no client breaks on it, because a
//! client sees only a wait that its own request deadline bounds.
//!
//! Nisshi adds no connection limit. A limit on connections per source address
//! does not bound a client that spreads over addresses, and it would close the
//! connections of every client behind one NAT or proxy.
//!
//! `client_id` is chosen by the client and is not authenticated, in Kafka
//! as here. The key is the principal and the `client_id` together, so a client
//! that changes its `client_id` for each connection avoids its share, with or
//! without authentication, as it avoided the connection share by opening more
//! connections. A user that logs in does not get a share over all its
//! `client_id`s. The share protects other clients from one that abandons
//! reads, and does not stop a user that changes its `client_id` on purpose.
//! Clients that send the same `client_id` and the same principal, or no
//! principal, share one client share. That includes every instance of
//! librdkafka's default `rdkafka`, and every replica of a Java consumer group
//! without a `client.id`, which sends `consumer-<group.id>-<n>` with `n`
//! counting from 1 in each JVM. The share is sized to leave a full
//! connection's share to a newcomer and no more.
//!
//! The registry holds one entry for each client that has a read in flight or
//! an abandoned read, and at most its sweep threshold of idle entries. A
//! `client_id` is cut to [`ClientIdentity::MAX_LEN`] bytes in
//! its key, so an entry's size is bounded.
//!
//! The share assumes one client per connection for the connection tier. A
//! proxy that sends several clients' requests over one connection, as
//! `nisshi-proxy` does with its pool of origin connections, gives those
//! clients one connection share, but not one client share: the proxy forwards
//! each request's `client_id`, so a client behind it is bounded by its client
//! share, over all the origin connections it uses. The proxy sends no
//! principal, so proxied clients are told apart by `client_id` alone. A proxy
//! in front of a broker on PostgreSQL is still not a supported production
//! setup, as the proxy is for development and demos.
//!
//! [connection-defaults]: https://github.com/apache/kafka/blob/3.9.1/server/src/main/java/org/apache/kafka/network/SocketServerConfigs.java#L136-L152
//! [connection-rejected]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/network/SocketServer.scala#L792-L794
//! [quota-key]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/ClientQuotaManager.scala#L200-L207
//! [quota-default]: https://github.com/apache/kafka/blob/3.9.1/server-common/src/main/java/org/apache/kafka/server/config/QuotaConfigs.java#L89
//! [connection-wait]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/network/SocketServer.scala#L1408-L1410
//! [connection-throttled]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/network/SocketServer.scala#L796-L800
//! [quota-throttle]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/server/KafkaApis.scala#L1006-L1017
//!
//! [`Storage`]: crate::Storage

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock, PoisonError},
};

use rama::extensions::{Extension, Extensions};
use tokio::{
    sync::{AcquireError, OwnedSemaphorePermit, Semaphore},
    task::LocalKey,
};

use super::read_deadline::{QueueWait as _, ReadWait};

tokio::task_local! {
    /// The identity of the client whose request the task is answering.
    static IDENTITY: ClientIdentity;
}

/// Who a request comes from, for [`ClientReadShares`]: the authenticated
/// principal, when the connection has one, and the request's `client_id`.
///
/// This is the pair that Kafka keys its client quotas on.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct ClientIdentity {
    principal: Option<Arc<str>>,
    client_id: Option<Arc<str>>,
}

impl ClientIdentity {
    /// The longest `principal` or `client_id`, in bytes, that an identity
    /// keeps. A client chooses its `client_id`, so this bounds the memory of
    /// a [`ClientReadShares`] entry. A longer one is cut, which only means
    /// that clients with the same long prefix share a share.
    pub const MAX_LEN: usize = 256;

    /// Returns the identity of a request with `client_id`, on a connection
    /// authenticated as `principal`.
    pub fn new(principal: Option<&str>, client_id: Option<&str>) -> Self {
        let bounded = |value: &str| -> Arc<str> {
            let end = value.floor_char_boundary(Self::MAX_LEN);
            Arc::from(&value[..end])
        };

        Self {
            principal: principal.map(bounded),
            client_id: client_id.map(bounded),
        }
    }

    /// Returns the identity of the request that the task is answering, or
    /// `None` outside a [`ClientIdentity::scope`].
    pub fn current() -> Option<Self> {
        IDENTITY.try_with(Clone::clone).ok()
    }

    /// Returns the request's `client_id`, cut to [`ClientIdentity::MAX_LEN`] bytes.
    ///
    /// Log it, and do not use it as a metric label: a client chooses it, so
    /// its values have no bound.
    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    /// Returns the authenticated principal, cut to [`ClientIdentity::MAX_LEN`] bytes.
    pub fn principal(&self) -> Option<&str> {
        self.principal.as_deref()
    }

    /// Runs `reads` with this as the identity that
    /// [`ClientReadShares::acquire`] takes its permits for.
    pub async fn scope<F>(self, reads: F) -> F::Output
    where
        F: Future,
    {
        IDENTITY.scope(self, reads).await
    }
}

/// How many entries a [`ClientReadShares`] holds before it looks for idle
/// ones to drop.
const MINIMUM_SWEEP_THRESHOLD: usize = 1_024;

#[derive(Debug)]
struct Registry {
    shares: HashMap<ClientIdentity, Arc<Semaphore>>,
    sweep_threshold: usize,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            shares: HashMap::new(),
            sweep_threshold: MINIMUM_SWEEP_THRESHOLD,
        }
    }
}

/// A storage engine's share of its limit on reads in flight for each
/// [`ClientIdentity`], see the [module](self) documentation.
///
/// Clones count against the same shares.
#[derive(Clone, Debug, Default)]
pub struct ClientReadShares(Arc<Mutex<Registry>>);

impl ClientReadShares {
    /// Waits for a permit from the share of the client in the enclosing
    /// [`ClientIdentity::scope`], creating the share with `size` permits if
    /// it has none yet.
    ///
    /// Returns `None` outside a scope, so a read that no client makes is
    /// limited only by its engine. A read abandoned during the wait is counted as
    /// queued for [`ReadWait::ClientReadLimit`].
    pub async fn acquire(&self, size: usize) -> Result<Option<OwnedSemaphorePermit>, AcquireError> {
        let Ok(identity) = IDENTITY.try_with(Clone::clone) else {
            return Ok(None);
        };

        let permits = self.share(identity, size);

        permits
            .acquire_owned()
            .queued_for(ReadWait::ClientReadLimit)
            .await
            .map(Some)
    }

    /// Returns the permits of `identity`'s share.
    ///
    /// Once the registry reaches its sweep threshold, it drops the shares
    /// that nothing else holds. A permit holds its semaphore, so a share
    /// that a read waits on, or that an abandoned read still holds a permit
    /// from, is kept.
    fn share(&self, identity: ClientIdentity, size: usize) -> Arc<Semaphore> {
        let mut registry = self.0.lock().unwrap_or_else(PoisonError::into_inner);

        if registry.shares.len() >= registry.sweep_threshold {
            registry
                .shares
                .retain(|_, share| Arc::strong_count(share) > 1);
            registry.sweep_threshold = MINIMUM_SWEEP_THRESHOLD.max(registry.shares.len() * 2);
        }

        registry
            .shares
            .entry(identity)
            .or_insert_with(|| Arc::new(Semaphore::new(size)))
            .clone()
    }

    /// Returns how many clients have a share.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .shares
            .len()
    }
}

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

    fn identity(principal: Option<&str>, client_id: &str) -> ClientIdentity {
        ClientIdentity::new(principal, Some(client_id))
    }

    #[tokio::test]
    async fn client_share_outside_a_scope_returns_none() -> Result<(), AcquireError> {
        assert!(ClientReadShares::default().acquire(1).await?.is_none());
        Ok(())
    }

    /// Reads of one identity count against one share, however many
    /// connections they come from, and another identity has a share of its
    /// own.
    #[tokio::test]
    async fn an_identity_has_one_share_for_all_its_connections() -> Result<(), AcquireError> {
        let shares = ClientReadShares::default();
        let one = identity(None, "one");

        let held = one.clone().scope(shares.acquire(1)).await?;
        assert!(held.is_some());

        assert!(
            timeout(
                Duration::from_millis(50),
                one.clone().scope(shares.acquire(1))
            )
            .await
            .is_err(),
            "a second connection of the identity took a second permit"
        );

        assert!(
            identity(None, "two")
                .scope(shares.acquire(1))
                .await?
                .is_some(),
            "another client waited for the first"
        );
        assert!(
            identity(Some("alice"), "one")
                .scope(shares.acquire(1))
                .await?
                .is_some(),
            "the same client_id under another principal waited"
        );

        drop(held);
        assert!(one.scope(shares.acquire(1)).await?.is_some());

        Ok(())
    }

    /// A read abandoned while it waits for its client's share is a miss
    /// queued for that share.
    #[tokio::test(start_paused = true)]
    async fn read_waiting_for_its_client_share_is_abandoned_as_queued() {
        let shares = ClientReadShares::default();
        let deadline = Instant::now() + Duration::from_secs(1);

        let outcome = identity(None, "one")
            .scope(bounded(deadline, async {
                let held = shares.acquire(1).await;
                assert!(held.as_ref().is_ok_and(Option::is_some), "{held:?}");

                _ = shares.acquire(1).await;
                pending::<()>().await
            }))
            .await;

        assert_eq!(Err(Missed::Queued(ReadWait::ClientReadLimit)), outcome);
    }

    /// The registry drops the shares that nothing holds when it fills, and
    /// keeps one whose permit is still held, so reconnecting does not hand
    /// an identity a fresh share.
    #[tokio::test]
    async fn idle_shares_are_evicted_and_held_ones_are_not() -> Result<(), AcquireError> {
        let shares = ClientReadShares::default();
        let abandoned = identity(None, "abandoned");

        let held = abandoned.clone().scope(shares.acquire(1)).await?;
        assert!(held.is_some());

        for n in 0..MINIMUM_SWEEP_THRESHOLD {
            drop(
                identity(None, &format!("idle-{n}"))
                    .scope(shares.acquire(1))
                    .await?,
            );
        }
        assert!(shares.len() <= MINIMUM_SWEEP_THRESHOLD);

        // The registry swept when it filled: the idle ones are gone.
        assert!(
            shares.len() < MINIMUM_SWEEP_THRESHOLD / 2 + 2,
            "{}",
            shares.len()
        );

        assert!(
            timeout(
                Duration::from_millis(50),
                abandoned.scope(shares.acquire(1))
            )
            .await
            .is_err(),
            "the share of an identity that holds a permit was evicted"
        );

        Ok(())
    }

    /// A task sees the identity of the request it answers, so that a log line
    /// can name the client at its share.
    #[tokio::test]
    async fn current_identity_is_the_one_in_scope() {
        assert_eq!(None, ClientIdentity::current());

        let identity = ClientIdentity::new(Some("alice"), Some("reporting"));
        let seen = identity
            .clone()
            .scope(async { ClientIdentity::current() })
            .await;

        assert_eq!(Some(identity), seen);
        assert_eq!(
            (Some("reporting"), Some("alice")),
            seen.as_ref()
                .map(|seen| (seen.client_id(), seen.principal()))
                .unwrap_or_default()
        );
    }

    #[test]
    fn a_long_client_id_is_cut_on_a_character_boundary() {
        // The prefix puts each "é" at an odd byte, so byte `MAX_LEN` is inside a
        // character, and the cut has to fall back to the byte before it.
        let long = format!("a{}", "é".repeat(ClientIdentity::MAX_LEN));
        let identity = ClientIdentity::new(Some(&long), Some(&long));

        assert_eq!(
            Some(ClientIdentity::MAX_LEN - 1),
            identity.client_id().map(str::len)
        );
        assert_eq!(
            Some(ClientIdentity::MAX_LEN - 1),
            identity.principal().map(str::len)
        );
        assert!(long.starts_with(identity.client_id().unwrap_or_default()));
    }
}
