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
//! its own. A read takes a permit from its connection's share, and then
//! permits from its client's share and the engine's limit, see
//! [`ClientReadShares::acquire`].
//!
//! The share binds only while the limit is used up and another client waits
//! for it. A client within its share waits for the limit in its queue, like
//! any read. A read past its share takes a permit of the limit only if one is
//! free, so a client that is alone may use the whole limit, and it never
//! waits in the limit's queue. It waits for a permit of its share instead.
//! The limit's queue is first in, first out, so a permit that frees while
//! another client waits goes to that client, and no read past its share takes
//! it. The reads that the client already runs past its share go on, and
//! return their permits as they end, so the client falls back to its share
//! while the other client waits. A client never waits in the limit's queue
//! with more reads than its share.
//!
//! A client's share is dropped once no read of that client holds a permit from
//! it. A client whose abandoned reads still hold permits keeps its share, so
//! reconnecting does not hand it a fresh one. A read past the share holds a
//! permit of the limit only, and an abandoned one counts against the limit
//! until its statement ends.
//!
//! ## What the share does not do
//!
//! The share reserves no permit for a newcomer. A client that is alone and
//! abandons its reads can fill the whole limit, and a client that arrives
//! later waits for a permit until some of those statements end: up to
//! `statement_timeout` plus 5 seconds when a cancel request does not reach
//! the server, as behind pgbouncer. Only the reads of the client that already
//! wait are held to its share. A reserve, a number of permits that only a
//! client within its share may take, would keep a newcomer's wait short. It
//! would also keep a client that is alone from using the whole limit, which
//! is the cost that this design avoids.
//!
//! ## What a client sees at the bound
//!
//! Nothing but a wait. A read of a client that already holds its share waits
//! for a permit in the broker while the limit is used up, and `wait` is
//! `client_read_limit` in the metric below, whether or not another client waits. With
//! the share set to the whole limit, a read waits with `wait` set to `client_read_limit`
//! only when its client holds every permit of the limit. The
//! connection stays open, the response carries no error and
//! `throttle_time_ms` stays 0. If the request's deadline passes first, a Fetch
//! answers the partition with no records, and a ListOffsets answers
//! `REQUEST_TIMED_OUT` for it, and the `nisshi_storage_read_deadline_exceeded`
//! metric rises with `stage` set to `queued` and `wait` set to `client_read_limit`.
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
//! counting from 1 in each JVM. They share one client share, which binds only
//! while another client waits for the limit.
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
    sync::{
        Arc, Mutex, OnceLock, PoisonError,
        atomic::{AtomicUsize, Ordering, fence},
    },
};

use rama::extensions::{Extension, Extensions};
use tokio::{
    sync::{AcquireError, Notify, OwnedSemaphorePermit, Semaphore},
    task::LocalKey,
    time::Instant,
};
use tracing::warn;

use super::read_deadline::{QueueWait as _, ReadWait};
use crate::{StorageOperation, Topition};

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

/// Logs at `warn` that a read of `operation` at `offset` of `topition`, which started at
/// `started_at`, reached its deadline while it waited for the read share of its client.
///
/// The log names the client of the enclosing [`ClientIdentity::scope`], and the metric does not,
/// because a client chooses its `client_id`. It tells a client that abandons its reads from a
/// consumer group whose instances share one `client_id`, and those need different fixes. A
/// storage engine calls this when its own wait for the share times out, because the
/// Fetch service sees only an answer with no records then.
pub fn warn_timeout_at_client_share(
    topition: &Topition,
    operation: StorageOperation,
    offset: i64,
    started_at: Instant,
) {
    let client = ClientIdentity::current();

    warn!(
        ?topition,
        %operation,
        offset,
        stage = "queued",
        wait = ReadWait::ClientReadLimit.as_str(),
        elapsed = ?started_at.elapsed(),
        client_id = ?client.as_ref().and_then(ClientIdentity::client_id),
        principal = ?client.as_ref().and_then(ClientIdentity::principal),
        "read deadline exceeded waiting for the client's read share"
    );
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
pub struct ClientReadShares {
    registry: Arc<Mutex<Registry>>,
    waiters: Arc<Waiters>,
}

/// The reads that wait for their client's share, and the wake-up that they
/// wait on.
#[derive(Debug, Default)]
struct Waiters {
    /// Wakes one read that waits for its share.
    released: Notify,
    /// How many reads wait for their share, or are about to.
    past_share: AtomicUsize,
    /// How many reads a wake-up ended the wait of.
    #[cfg(test)]
    woken: AtomicUsize,
}

impl Waiters {
    /// Counts a read whose wait ended with a wake-up.
    #[cfg(test)]
    fn woke(&self) {
        _ = self.woken.fetch_add(1, Ordering::SeqCst);
    }

    #[cfg(not(test))]
    fn woke(&self) {}
}

/// Counts a read that waits for its client's share, from the moment it starts
/// to look for a permit of the limit to the end of its wait.
struct PastShare<'a>(&'a Waiters);

impl<'a> PastShare<'a> {
    fn new(waiters: &'a Waiters) -> Self {
        _ = waiters.past_share.fetch_add(1, Ordering::SeqCst);

        // The read's count must be visible before its last look for a permit, and a release
        // must put its permit back before it reads the count. Each side writes one variable
        // and reads the other, so a read that the count misses sees the permit. Atomics that
        // use SeqCst do not promise that across two variables to the language, only the
        // hardware here does, so the fence states it.
        fence(Ordering::SeqCst);
        Self(waiters)
    }
}

impl Drop for PastShare<'_> {
    fn drop(&mut self) {
        _ = self.0.past_share.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The permits of a read: one from the engine's limit, and one from its
/// client's share unless the read is beyond that share or outside any client,
/// see [`ClientReadShares::acquire`].
///
/// Dropping it returns the permits. When the permit of the limit goes back to
/// the limit, and not to a read that waits in its queue, it wakes one read
/// that waits for its client's share, because that read can take it.
#[derive(Debug)]
pub struct ClientAdmission {
    limit: Option<OwnedSemaphorePermit>,
    share: Option<OwnedSemaphorePermit>,
    /// The semaphore that `limit` is a permit of.
    engine: Arc<Semaphore>,
    waiters: Arc<Waiters>,
}

impl ClientAdmission {
    /// Returns whether the read holds a permit from its client's share. A read
    /// beyond the share, or outside any client, holds only one from the
    /// engine's limit.
    pub fn is_within_share(&self) -> bool {
        self.share.is_some()
    }
}

impl Drop for ClientAdmission {
    fn drop(&mut self) {
        drop(self.limit.take());
        drop(self.share.take());

        // The permit went back first. If the limit has a permit now, no read is queued for it:
        // a permit that frees while reads queue goes to the first of them, and does not count
        // as available. A read that waits for its share can take it, so one is woken. Asking
        // only for the count after the release means that a read which took the permit
        // meanwhile, or another release, cannot make this one miss its wake-up.
        //
        // A read of a client within its share that is cancelled after the limit gave it a
        // permit, and before it polled, returns the permit through tokio's `Acquire::drop`,
        // and not through here, so it wakes nobody. The read that waits stays parked until the
        // next release, while a permit is free.
        //
        // See `PastShare::new` for the fence.
        fence(Ordering::SeqCst);
        if self.engine.available_permits() > 0 && self.waiters.past_share.load(Ordering::SeqCst) > 0
        {
            self.waiters.released.notify_one();
        }
    }
}

impl ClientReadShares {
    /// Waits for the permits of a read of the client in the enclosing
    /// [`ClientIdentity::scope`]: one from the client's share, which has
    /// `size` permits, and one from `limit`, the engine's limit on reads in
    /// flight.
    ///
    /// A client past its share still takes a permit from `limit` when one is
    /// free, so a client that is alone may use the whole limit. It never
    /// queues for a permit of `limit` beyond its share, so it holds no place
    /// in the queue ahead of another client's read. It waits for a permit of
    /// its share instead. A permit that frees while another client waits goes
    /// to the first read in the queue of `limit`, so the share holds a client
    /// to its size as its reads end once `limit` is used up and another
    /// client waits for it.
    ///
    /// Outside a scope, a read that no client makes takes only a permit from
    /// `limit`. A read abandoned during the wait for the share is counted as queued for
    /// [`ReadWait::ClientReadLimit`], and one abandoned during the wait for `limit` as queued
    /// for [`ReadWait::EngineReadLimit`].
    pub async fn acquire(
        &self,
        size: usize,
        limit: &Arc<Semaphore>,
    ) -> Result<ClientAdmission, AcquireError> {
        let Ok(identity) = IDENTITY.try_with(Clone::clone) else {
            let permit = limit
                .clone()
                .acquire_owned()
                .queued_for(ReadWait::EngineReadLimit)
                .await?;
            return Ok(self.admission(limit, permit, None));
        };

        let share = self.share(identity, size);

        loop {
            // The read starts listening before it checks the permits, so that a permit
            // freed after the checks wakes it. Otherwise it could wait on its share while
            // a permit of `limit` is free.
            let released = self.waiters.released.notified();
            tokio::pin!(released);
            _ = released.as_mut().enable();

            // A closed share has no permits, and waiting for one returns its error.
            let within_share = share.clone().try_acquire_owned().ok();

            let share_permit = match within_share {
                Some(permit) => permit,
                None => {
                    let waiting = PastShare::new(&self.waiters);

                    if let Ok(permit) = limit.clone().try_acquire_owned() {
                        return Ok(self.admission(limit, permit, None));
                    }

                    // A read waits for its share until one of the client's reads ends, or a
                    // permit of `limit` goes back to it, because the read can take it then.
                    let waited = async {
                        tokio::select! {
                            permit = share.clone().acquire_owned() => Some(permit),
                            () = &mut released => None,
                        }
                    }
                    .queued_for(ReadWait::ClientReadLimit)
                    .await;
                    drop(waiting);

                    match waited {
                        Some(permit) => permit?,
                        None => {
                            self.waiters.woke();
                            continue;
                        }
                    }
                }
            };

            let limit_permit = limit
                .clone()
                .acquire_owned()
                .queued_for(ReadWait::EngineReadLimit)
                .await?;

            return Ok(self.admission(limit, limit_permit, Some(share_permit)));
        }
    }

    fn admission(
        &self,
        engine: &Arc<Semaphore>,
        limit: OwnedSemaphorePermit,
        share: Option<OwnedSemaphorePermit>,
    ) -> ClientAdmission {
        ClientAdmission {
            limit: Some(limit),
            share,
            engine: engine.clone(),
            waiters: self.waiters.clone(),
        }
    }

    /// Returns the permits of `identity`'s share.
    ///
    /// Once the registry reaches its sweep threshold, it drops the shares
    /// that nothing else holds. A permit holds its semaphore, so a share
    /// that a read waits on, or that an abandoned read still holds a permit
    /// from, is kept.
    fn share(&self, identity: ClientIdentity, size: usize) -> Arc<Semaphore> {
        let mut registry = self.registry.lock().unwrap_or_else(PoisonError::into_inner);

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
        self.registry
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
        service::read_deadline::{ReadDeadline, TimedOut},
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
    /// one abandoned during that wait times out as queued for its
    /// connection's share.
    #[tokio::test(start_paused = true)]
    async fn read_waiting_for_its_share_is_abandoned_as_queued() {
        let deadline = Instant::now() + Duration::from_secs(1);

        let mut deadline = ReadDeadline::new(StorageOperation::Fetch, deadline);

        let outcome = FetchReadShare::default()
            .scope(deadline.run_until_deadline(async {
                let held = FetchReadShare::acquire(1).await;
                assert!(held.is_ok(), "{held:?}");

                _ = FetchReadShare::acquire(1).await;
                pending::<()>().await
            }))
            .await;

        assert_eq!(
            Err(TimedOut::Queued(ReadWait::ConnectionReadLimit)),
            outcome
        );
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

    /// Returns an engine limit with `permits` permits.
    fn limit(permits: usize) -> Arc<Semaphore> {
        Arc::new(Semaphore::new(permits))
    }

    /// Takes the permits of a read of `client`, which must not wait.
    async fn admit(
        shares: &ClientReadShares,
        client: &ClientIdentity,
        size: usize,
        limit: &Arc<Semaphore>,
    ) -> ClientAdmission {
        timeout(
            Duration::from_millis(200),
            client.clone().scope(shares.acquire(size, limit)),
        )
        .await
        .expect("the read did not wait")
        .expect("the limit is open")
    }

    /// Returns whether a read of `client` waits for its permits.
    async fn waits(
        shares: &ClientReadShares,
        client: &ClientIdentity,
        size: usize,
        limit: &Arc<Semaphore>,
    ) -> bool {
        timeout(
            Duration::from_millis(50),
            client.clone().scope(shares.acquire(size, limit)),
        )
        .await
        .is_err()
    }

    /// A read that no client makes takes only a permit of the limit.
    #[tokio::test]
    async fn outside_a_scope_a_read_takes_only_a_permit_of_the_limit() -> Result<(), AcquireError> {
        let limit = limit(1);
        let shares = ClientReadShares::default();

        let held = shares.acquire(1, &limit).await?;
        assert!(!held.is_within_share());
        assert_eq!(0, limit.available_permits());
        assert_eq!(0, shares.len());

        drop(held);
        assert_eq!(1, limit.available_permits());
        Ok(())
    }

    /// A client that nobody else reads for may hold the whole limit, past its
    /// share.
    #[tokio::test]
    async fn a_lone_client_uses_the_whole_limit() {
        let shares = ClientReadShares::default();
        let limit = limit(3);
        let client = identity(None, "alone");

        let held = [
            admit(&shares, &client, 1, &limit).await,
            admit(&shares, &client, 1, &limit).await,
            admit(&shares, &client, 1, &limit).await,
        ];

        assert_eq!(
            vec![true, false, false],
            held.iter()
                .map(ClientAdmission::is_within_share)
                .collect::<Vec<_>>()
        );
        assert_eq!(0, limit.available_permits());

        // The limit is used up, so the fourth read waits for the share.
        assert!(waits(&shares, &client, 1, &limit).await);
    }

    /// A client past its share does not queue for the limit. A permit that
    /// frees while another client waits goes to that client, and the client
    /// past its share waits for its share, and takes the limit again once
    /// nobody waits.
    #[tokio::test]
    async fn a_client_past_its_share_gives_way_to_a_waiting_client() {
        let shares = ClientReadShares::default();
        let limit = limit(2);
        let greedy = identity(None, "greedy");

        let within = admit(&shares, &greedy, 1, &limit).await;
        let beyond = admit(&shares, &greedy, 1, &limit).await;
        assert!(within.is_within_share());
        assert!(!beyond.is_within_share());

        // The first client's next read waits for its share, not for the limit, and so it holds no
        // place in the limit's queue ahead of the client that waits next.
        let next = tokio::spawn({
            let shares = shares.clone();
            let limit = limit.clone();

            greedy
                .clone()
                .scope(async move { shares.acquire(1, &limit).await })
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!next.is_finished());

        // Another client waits for the limit, which the first has used up.
        let waiting = tokio::spawn({
            let shares = shares.clone();
            let limit = limit.clone();

            identity(None, "polite").scope(async move { shares.acquire(1, &limit).await })
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiting.is_finished());

        // The permit that frees goes to the client that waits.
        drop(beyond);
        let polite = timeout(Duration::from_secs(1), waiting)
            .await
            .expect("the waiting client got the permit that freed")
            .expect("the task ran")
            .expect("the limit is open");
        assert!(polite.is_within_share());
        assert!(!next.is_finished(), "the greedy client took the permit");

        // Once nobody waits, the greedy client's read takes the permit that frees.
        drop(within);
        let next = timeout(Duration::from_secs(1), next)
            .await
            .expect("the read took the permit that freed")
            .expect("the task ran")
            .expect("the limit is open");
        drop(next);
    }

    /// A read waiting for its share wakes when a permit of the limit frees, so
    /// a client past its share that nobody else waits behind keeps the whole
    /// limit.
    #[tokio::test]
    async fn a_read_waiting_for_its_share_takes_a_permit_that_frees() {
        let shares = ClientReadShares::default();
        let limit = limit(2);
        let client = identity(None, "alone");

        let within = admit(&shares, &client, 1, &limit).await;
        let beyond = admit(&shares, &client, 1, &limit).await;

        let next = tokio::spawn({
            let shares = shares.clone();
            let limit = limit.clone();

            client
                .clone()
                .scope(async move { shares.acquire(1, &limit).await })
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!next.is_finished());

        // The share is still in use by `within`: only the limit frees.
        drop(beyond);
        let next = timeout(Duration::from_secs(1), next)
            .await
            .expect("the read woke for the permit of the limit")
            .expect("the task ran")
            .expect("the limit is open");
        assert!(!next.is_within_share());
        drop(within);
    }

    /// A read of a client that waits is cancelled without holding a permit, so
    /// the permits are all back once the holders end.
    #[tokio::test]
    async fn cancelled_reads_leave_no_permit_behind() {
        let shares = ClientReadShares::default();
        let limit = limit(2);
        let client = identity(None, "cancelled");

        let held = [
            admit(&shares, &client, 1, &limit).await,
            admit(&shares, &client, 1, &limit).await,
        ];

        for _ in 0..3 {
            assert!(waits(&shares, &client, 1, &limit).await);
        }
        drop(held);

        assert_eq!(2, limit.available_permits());
        let again = [
            admit(&shares, &client, 1, &limit).await,
            admit(&shares, &client, 1, &limit).await,
        ];
        assert!(again[0].is_within_share());
    }

    /// Reads of one identity count against one share, however many
    /// connections they come from, and another identity has a share of its
    /// own.
    #[tokio::test]
    async fn an_identity_has_one_share_for_all_its_connections() {
        let shares = ClientReadShares::default();
        let limit = limit(10);
        let one = identity(None, "one");

        let first = admit(&shares, &one, 1, &limit).await;
        let second = admit(&shares, &one, 1, &limit).await;
        assert!(first.is_within_share());
        assert!(
            !second.is_within_share(),
            "a second connection of the identity took a second permit of its share"
        );

        assert!(
            admit(&shares, &identity(None, "two"), 1, &limit)
                .await
                .is_within_share(),
            "another client used the share of the first"
        );
        assert!(
            admit(&shares, &identity(Some("alice"), "one"), 1, &limit)
                .await
                .is_within_share(),
            "the same client_id under another principal used the share of the first"
        );
    }

    /// A read abandoned while it waits for its client's share times out as
    /// queued for that share.
    #[tokio::test(start_paused = true)]
    async fn read_waiting_for_its_client_share_is_abandoned_as_queued() {
        let shares = ClientReadShares::default();
        let limit = limit(1);
        let deadline = Instant::now() + Duration::from_secs(1);

        let mut deadline = ReadDeadline::new(StorageOperation::Fetch, deadline);

        let outcome = identity(None, "one")
            .scope(deadline.run_until_deadline(async {
                let held = shares.acquire(1, &limit).await;
                assert!(held.is_ok(), "{held:?}");

                _ = shares.acquire(1, &limit).await;
                pending::<()>().await
            }))
            .await;

        assert_eq!(Err(TimedOut::Queued(ReadWait::ClientReadLimit)), outcome);
    }

    /// The registry drops the shares that nothing holds when it fills, and
    /// keeps one whose permit is still held, so reconnecting does not hand
    /// an identity a fresh share.
    #[tokio::test]
    async fn idle_shares_are_evicted_and_held_ones_are_not() {
        let shares = ClientReadShares::default();
        let limit = limit(MINIMUM_SWEEP_THRESHOLD + 2);
        let abandoned = identity(None, "abandoned");

        let held = admit(&shares, &abandoned, 1, &limit).await;
        assert!(held.is_within_share());

        for n in 0..MINIMUM_SWEEP_THRESHOLD {
            drop(admit(&shares, &identity(None, &format!("idle-{n}")), 1, &limit).await);
        }
        assert!(shares.len() <= MINIMUM_SWEEP_THRESHOLD);

        // The registry swept when it filled: the idle ones are gone.
        assert!(
            shares.len() < MINIMUM_SWEEP_THRESHOLD / 2 + 2,
            "{}",
            shares.len()
        );

        // A fresh share would give the read a permit of its own.
        assert!(
            !admit(&shares, &abandoned, 1, &limit)
                .await
                .is_within_share(),
            "the share of an identity that holds a permit was evicted"
        );
    }

    /// Returns how many reads a wake-up ended the wait of.
    fn woken(shares: &ClientReadShares) -> usize {
        shares.waiters.woken.load(Ordering::SeqCst)
    }

    /// A permit of the limit that goes back to it wakes one read that waits
    /// for its share, and not all of them: only one can take the permit.
    #[tokio::test]
    async fn a_released_permit_wakes_one_read_that_waits_for_its_share() {
        let shares = ClientReadShares::default();
        let limit = limit(2);
        let client = identity(None, "crowded");

        let within = admit(&shares, &client, 1, &limit).await;
        let beyond = admit(&shares, &client, 1, &limit).await;

        let waiting = (0..8)
            .map(|_| {
                let shares = shares.clone();
                let limit = limit.clone();

                tokio::spawn(
                    client
                        .clone()
                        .scope(async move { shares.acquire(1, &limit).await }),
                )
            })
            .collect::<Vec<_>>();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(waiting.iter().all(|read| !read.is_finished()));
        let before = woken(&shares);

        drop(beyond);
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(1, waiting.iter().filter(|read| read.is_finished()).count());
        assert_eq!(1, woken(&shares) - before, "more than one read woke");

        // The next permit that goes back wakes the next read.
        drop(within);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(2, waiting.iter().filter(|read| read.is_finished()).count());

        for read in waiting {
            read.abort();
        }
    }

    /// Starts `count` reads of `client`, which wait because the limit is used up
    /// and the client holds its share.
    fn spawn_waiting(
        shares: &ClientReadShares,
        client: &ClientIdentity,
        limit: &Arc<Semaphore>,
        count: usize,
    ) -> Vec<tokio::task::JoinHandle<Result<ClientAdmission, AcquireError>>> {
        (0..count)
            .map(|_| {
                let shares = shares.clone();
                let limit = limit.clone();

                tokio::spawn(
                    client
                        .clone()
                        .scope(async move { shares.acquire(1, &limit).await }),
                )
            })
            .collect()
    }

    /// A read that a release woke takes the permit, and the next release wakes
    /// the next read: the count that a release looks at is the one after it,
    /// so the read that took the permit does not hide the second release.
    #[tokio::test]
    async fn each_release_wakes_the_next_read_that_waits_for_its_share() {
        let shares = ClientReadShares::default();
        let limit = limit(3);
        let client = identity(None, "queued");

        let within = admit(&shares, &client, 1, &limit).await;
        let first = admit(&shares, &client, 1, &limit).await;
        let second = admit(&shares, &client, 1, &limit).await;

        let waiting = spawn_waiting(&shares, &client, &limit, 2);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(waiting.iter().all(|read| !read.is_finished()));

        drop(first);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(1, waiting.iter().filter(|read| read.is_finished()).count());

        drop(second);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(2, waiting.iter().filter(|read| read.is_finished()).count());

        drop(within);
        for read in waiting {
            read.abort();
        }
    }

    /// Two releases in a row, before either woken read runs, wake two reads:
    /// the second finds a permit in the count that the first already counted.
    #[tokio::test]
    async fn two_releases_in_a_row_wake_two_reads() {
        let shares = ClientReadShares::default();
        let limit = limit(3);
        let client = identity(None, "queued");

        let within = admit(&shares, &client, 1, &limit).await;
        let first = admit(&shares, &client, 1, &limit).await;
        let second = admit(&shares, &client, 1, &limit).await;

        let waiting = spawn_waiting(&shares, &client, &limit, 2);
        tokio::time::sleep(Duration::from_millis(100)).await;

        drop(first);
        drop(second);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(2, waiting.iter().filter(|read| read.is_finished()).count());

        drop(within);
        for read in waiting {
            read.abort();
        }
    }

    /// A permit of the limit that goes to a client that waits in the limit's
    /// queue is not free, so it wakes nobody.
    #[tokio::test]
    async fn a_permit_that_goes_to_a_waiting_client_wakes_nobody() {
        let shares = ClientReadShares::default();
        let limit = limit(2);
        let client = identity(None, "first");

        // The first client's share is in use, and another client holds the rest of the limit.
        let _within = admit(&shares, &client, 1, &limit).await;
        let other = admit(&shares, &identity(None, "other"), 1, &limit).await;

        let queued = tokio::spawn({
            let shares = shares.clone();
            let limit = limit.clone();

            identity(None, "second").scope(async move { shares.acquire(1, &limit).await })
        });
        let past_share = tokio::spawn({
            let shares = shares.clone();
            let limit = limit.clone();

            client
                .clone()
                .scope(async move { shares.acquire(1, &limit).await })
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!queued.is_finished() && !past_share.is_finished());
        let before = woken(&shares);

        // The permit goes to the client in the queue of the limit, and the read of the first
        // client has nothing to take.
        drop(other);
        let second = timeout(Duration::from_secs(1), queued)
            .await
            .expect("the client that waits got the permit")
            .expect("the task ran")
            .expect("the limit is open");

        assert!(second.is_within_share());
        assert_eq!(0, woken(&shares) - before, "a read woke for nothing");
        assert!(!past_share.is_finished());
        past_share.abort();
    }

    /// A read that no client makes returns its permit of the limit through the
    /// client shares too, so a read that waits for its share takes it.
    #[tokio::test]
    async fn a_read_outside_any_client_wakes_a_read_that_waits_for_its_share() {
        let shares = ClientReadShares::default();
        let limit = limit(2);
        let client = identity(None, "waiting");

        let _within = admit(&shares, &client, 1, &limit).await;
        let outside = shares.acquire(1, &limit).await.expect("the limit is open");
        assert!(!outside.is_within_share());

        let next = tokio::spawn({
            let shares = shares.clone();
            let limit = limit.clone();

            client
                .clone()
                .scope(async move { shares.acquire(1, &limit).await })
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!next.is_finished());

        drop(outside);
        let next = timeout(Duration::from_secs(1), next)
            .await
            .expect("the read took the permit that went back")
            .expect("the task ran")
            .expect("the limit is open");
        assert!(!next.is_within_share());
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
