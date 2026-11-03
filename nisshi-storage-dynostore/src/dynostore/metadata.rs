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

use std::{
    fmt::{Debug, Display},
    hash::{DefaultHasher, Hash as _, Hasher as _},
    ops::DerefMut,
    slice::from_ref,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use cached::{CachedExt as _, stores::TtlSortedCache};
use futures::stream::{BoxStream, StreamExt};
use nisshi_storage::Error;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, UpdateVersion, path::Path,
};
use opentelemetry::{KeyValue, metrics::Counter};
use tracing::{debug, instrument};

use crate::dynostore::object_store_error_name;

use super::METER;

#[derive(Clone, Debug, Eq, PartialEq)]
struct CacheEntry {
    version: UpdateVersion,
}

impl From<GetOptions> for CacheEntry {
    fn from(value: GetOptions) -> Self {
        Self {
            version: UpdateVersion {
                e_tag: value.if_none_match,
                version: None,
            },
        }
    }
}

impl From<&PutResult> for CacheEntry {
    fn from(put_result: &PutResult) -> Self {
        debug!(?put_result);

        let e_tag = put_result.e_tag.clone();
        let version = put_result.version.clone();

        Self {
            version: UpdateVersion { e_tag, version },
        }
    }
}

impl From<&GetResult> for CacheEntry {
    fn from(get_result: &GetResult) -> Self {
        debug!(?get_result);

        let e_tag = get_result.meta.e_tag.clone();
        let version = get_result.meta.version.clone();

        Self {
            version: UpdateVersion { e_tag, version },
        }
    }
}

/// The number of counters that count the changes to the cache's entries, shared by the paths
/// that hash to the same one.
///
/// A path that shares a counter with another loses its entry when their writes overlap, and
/// the next read of it asks the store. The table is sized so that the engine's replacing
/// writes, a watermark and a `meta.json` write per produce across every partition and the
/// offset commits, seldom share a counter within one store round trip. 4,096 counters take
/// 32 KiB.
const GENERATIONS: usize = 4_096;

fn generation<'a>(generations: &'a [AtomicU64; GENERATIONS], location: &Path) -> &'a AtomicU64 {
    let mut hasher = DefaultHasher::new();
    location.hash(&mut hasher);
    &generations[hasher.finish() as usize % GENERATIONS]
}

#[derive(Clone)]
pub(super) struct Cache<O> {
    entries: Arc<Mutex<TtlSortedCache<Path, CacheEntry>>>,
    /// Counts the entries' changes, to see whether a path's entry changed while a call to the
    /// object store was in flight. A path that shares a counter with another only loses a
    /// cache entry it could have kept, never keeps a stale one.
    generations: Arc<[AtomicU64; GENERATIONS]>,
    object_store: O,
    retention: Duration,
}

impl<O> Debug for Cache<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache")
            .field("retention", &self.retention)
            .finish()
    }
}

impl<O> Display for Cache<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cache").finish()
    }
}

impl<O> Cache<O>
where
    O: ObjectStore,
{
    pub(super) fn new(object_store: O, retention: Duration) -> Result<Self, Error> {
        let entries = Arc::new(Mutex::new(
            TtlSortedCache::builder()
                .ttl(retention)
                .build()
                .map_err(|_| Error::Message("metadata cache retention must be non-zero".into()))?,
        ));

        Ok(Self {
            entries,
            generations: Arc::new(std::array::from_fn(|_| AtomicU64::new(0))),
            object_store,
            retention,
        })
    }

    #[cfg(test)]
    fn inner(&self) -> &O {
        &self.object_store
    }

    fn generation(&self, location: &Path) -> &AtomicU64 {
        generation(&self.generations, location)
    }

    /// Records what an object store call learned about `location`'s current version, if no
    /// other call changed the entry since `before`, the generation read before the call began.
    ///
    /// A call that overlapped a change learned a version that may be older than the entry's,
    /// and storing it would let readers that present that version be answered NotModified
    /// without asking the store. Removing the entry instead is always safe: the next reader
    /// asks the store. The removal does not count as a change, because it does not change the
    /// object's version: a call that began after the change read a version at least as new,
    /// and may store it. The caller holds the lock on `entries`, which orders this with every
    /// other change.
    fn record(
        &self,
        guard: &mut TtlSortedCache<Path, CacheEntry>,
        location: &Path,
        before: u64,
        replacement: CacheEntry,
    ) -> &'static str {
        let generation = self.generation(location);

        if generation.load(Ordering::SeqCst) == before {
            match guard
                .set_with(location.to_owned(), replacement.clone())
                .evict()
                .set()
            {
                None => "add",

                Some(existing) if existing == replacement => "existing",

                Some(_) => "replace",
            }
        } else {
            _ = guard.remove(location);
            "overlapped"
        }
    }
}

static REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_objectstore_cache_requests")
        .with_description("object_store cache requests")
        .build()
});

static ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_objectstore_cache_errors")
        .with_description("object_store cache errors")
        .build()
});

static OUTCOMES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_objectstore_cache_outcomes")
        .with_description("object_store cache outcomes")
        .build()
});

static ENTRIES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_objectstore_cache_entries")
        .with_description("object_store cache entries")
        .build()
});

#[async_trait]
impl<O> ObjectStore for Cache<O>
where
    O: ObjectStore,
{
    #[instrument(skip_all, fields(location = %location))]
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        let method = KeyValue::new("method", "put_opts");

        REQUESTS.add(1, from_ref(&method));

        let before = self.generation(location).load(Ordering::SeqCst);
        let replaces = !matches!(opts.mode, PutMode::Create);

        self.object_store
            .put_opts(location, payload, opts)
            .await
            .inspect(|put_result| {
                debug!(?put_result);
                if let Ok(mut guard) = self.entries.lock() {
                    debug!(cached = guard.len());

                    let outcome = self.record(
                        guard.deref_mut(),
                        location,
                        before,
                        CacheEntry::from(put_result),
                    );
                    debug!(outcome);

                    ENTRIES.add(1, &[method.clone(), KeyValue::new("outcome", outcome)]);

                    // A reader that began before this write must not store the version it
                    // read, which this write has replaced. A write that creates the object
                    // replaces no version: a reader that began before it found nothing, or
                    // read the created object. Each batch object is created, so this keeps
                    // the batch writes out of the counters that the watermarks share.
                    if replaces {
                        _ = self.generation(location).fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
            .inspect_err(|error| {
                ERRORS.add(
                    1,
                    &[
                        method,
                        KeyValue::new("error", object_store_error_name(error)),
                    ],
                );
            })
    }

    #[instrument(skip_all, fields(location = %location))]
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
        debug!(%location, ?opts);

        let method = KeyValue::new("method", "put_multipart_opts");

        REQUESTS.add(1, from_ref(&method));

        self.object_store
            .put_multipart_opts(location, opts)
            .await
            .inspect_err(|error| {
                debug!(%location, ?error);

                ERRORS.add(
                    1,
                    &[
                        method,
                        KeyValue::new("error", object_store_error_name(error)),
                    ],
                );
            })
    }

    #[instrument(skip_all, fields(%location, if_none_match = options.if_none_match), ret)]
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, object_store::Error> {
        debug!(?options);

        let method = KeyValue::new("method", "get_opts");

        REQUESTS.add(1, from_ref(&method));

        let before = self.generation(location).load(Ordering::SeqCst);

        if let Ok(mut guard) = self.entries.lock() {
            if let Some(entry) = guard.deref_mut().get(location) {
                debug!(?entry);

                if let Some(ref cached_e_tag) = entry.version.e_tag {
                    debug!(cached_e_tag);

                    if let Some(ref presented) = options.if_none_match {
                        debug!(cached_e_tag, presented);

                        if cached_e_tag == presented {
                            let outcome = "hit";

                            OUTCOMES.add(1, &[method, KeyValue::new("outcome", outcome)]);
                            debug!(outcome);

                            return Err(object_store::Error::NotModified {
                                path: location.to_string(),
                                source: Box::new(Error::PhantomCached()),
                            });
                        } else {
                            let outcome = "no_match";
                            debug!(outcome);

                            OUTCOMES.add(1, &[method.clone(), KeyValue::new("outcome", outcome)]);
                        }
                    } else {
                        let outcome = "miss";

                        debug!(outcome);
                        OUTCOMES.add(1, &[method.clone(), KeyValue::new("outcome", outcome)]);
                    }
                } else {
                    let outcome = "miss";

                    debug!(outcome);
                    OUTCOMES.add(1, &[method.clone(), KeyValue::new("outcome", outcome)]);
                }
            } else {
                let outcome = "miss";

                debug!(outcome);
                OUTCOMES.add(1, &[method.clone(), KeyValue::new("outcome", outcome)]);
            }
        }

        self.object_store
            .get_opts(location, options.clone())
            .await
            .inspect(|get_result| {
                let e_tag = get_result.meta.e_tag.clone();
                let version = get_result.meta.version.clone();

                if let Ok(mut guard) = self.entries.lock() {
                    debug!(e_tag, version);

                    let outcome = self.record(
                        guard.deref_mut(),
                        location,
                        before,
                        CacheEntry::from(get_result),
                    );

                    debug!(outcome);

                    ENTRIES.add(1, &[method.clone(), KeyValue::new("outcome", outcome)]);
                }
            })
            .inspect_err(|error| {
                debug!(%location, ?error);

                if matches!(error, object_store::Error::NotModified { .. }) {
                    if let Ok(mut guard) = self.entries.lock() {
                        let outcome = self.record(
                            guard.deref_mut(),
                            location,
                            before,
                            CacheEntry::from(options),
                        );

                        debug!(outcome);

                        ENTRIES.add(1, &[method.clone(), KeyValue::new("outcome", outcome)]);
                    }
                } else {
                    ERRORS.add(
                        1,
                        &[
                            method,
                            KeyValue::new("error", object_store_error_name(error)),
                        ],
                    );
                }
            })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, object_store::Error>>,
    ) -> BoxStream<'static, Result<Path, object_store::Error>> {
        REQUESTS.add(1, &[KeyValue::new("method", "delete_stream")]);

        let entries = self.entries.clone();
        let generations = self.generations.clone();
        let inner = self.object_store.delete_stream(locations);

        Box::pin(inner.inspect(move |result| {
            if let Ok(location) = result
                && let Ok(mut guard) = entries.lock()
            {
                _ = generation(&generations, location).fetch_add(1, Ordering::SeqCst);

                if guard.deref_mut().remove(location).is_some() {
                    ENTRIES.add(1, &[KeyValue::new("outcome", "delete")]);
                }
            }
        }))
    }

    #[instrument(skip_all, fields(prefix))]
    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        debug!(?prefix);
        REQUESTS.add(1, &[KeyValue::new("method", "list")]);
        self.object_store.list(prefix)
    }

    #[instrument(skip_all, fields(prefix))]
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
        debug!(?prefix, %offset);
        REQUESTS.add(1, &[KeyValue::new("method", "list_with_offset")]);
        self.object_store.list_with_offset(prefix, offset)
    }

    #[instrument(skip_all, fields(prefix))]
    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, object_store::Error> {
        debug!(?prefix);
        REQUESTS.add(1, &[KeyValue::new("method", "list_with_delimiter")]);
        self.object_store
            .list_with_delimiter(prefix)
            .await
            .inspect_err(|error| {
                debug!(?prefix, ?error);

                ERRORS.add(
                    1,
                    &[
                        KeyValue::new("method", "list_with_delimiter"),
                        KeyValue::new("error", object_store_error_name(error)),
                    ],
                );
            })
    }

    #[instrument(skip_all, fields(from = %from, to = %to))]
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> Result<(), object_store::Error> {
        debug!(%from, %to, ?opts);
        REQUESTS.add(1, &[KeyValue::new("method", "copy_opts")]);
        self.object_store
            .copy_opts(from, to, opts)
            .await
            .inspect_err(|error| {
                debug!(%from, %to, ?error);

                ERRORS.add(
                    1,
                    &[
                        KeyValue::new("method", "copy_opts"),
                        KeyValue::new("error", object_store_error_name(error)),
                    ],
                );
            })
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use bytes::Bytes;
    use nisshi_storage::Result;
    use object_store::{ObjectStoreExt, PutMode, memory::InMemory};
    use serde::{Deserialize, Serialize};
    use tokio::time::sleep;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use super::*;
    use crate::dynostore::held::Held;

    #[derive(
        Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    struct X(i32);

    #[derive(Clone, Debug)]
    struct Counter<O> {
        put_opts: Arc<Mutex<u64>>,
        get_opts: Arc<Mutex<u64>>,
        object_store: O,
    }

    #[allow(dead_code)]
    impl<O> Counter<O> {
        fn new(object_store: O) -> Self {
            Self {
                put_opts: Default::default(),
                get_opts: Default::default(),
                object_store,
            }
        }

        fn put_opts(&self) -> Result<u64> {
            self.put_opts.lock().map(|guard| *guard).map_err(Into::into)
        }

        fn get_opts(&self) -> Result<u64> {
            self.get_opts.lock().map(|guard| *guard).map_err(Into::into)
        }
    }

    impl<O> Display for Counter<O> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Counter").finish()
        }
    }

    #[async_trait]
    impl<O> ObjectStore for Counter<O>
    where
        O: ObjectStore,
    {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Result<PutResult, object_store::Error> {
            if let Ok(mut guard) = self.put_opts.lock() {
                *guard += 1;
            }

            self.object_store.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>, object_store::Error> {
            self.object_store.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> Result<GetResult, object_store::Error> {
            if let Ok(mut guard) = self.get_opts.lock() {
                *guard += 1;
            }

            self.object_store.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, Result<Path, object_store::Error>>,
        ) -> BoxStream<'static, Result<Path, object_store::Error>> {
            self.object_store.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, Result<ObjectMeta, object_store::Error>> {
            self.object_store.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> Result<ListResult, object_store::Error> {
            self.object_store.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            opts: CopyOptions,
        ) -> Result<(), object_store::Error> {
            self.object_store.copy_opts(from, to, opts).await
        }
    }

    fn init_tracing() -> Result<DefaultGuard> {
        use std::{fs::File, sync::Arc, thread};

        Ok(tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_level(true)
                .with_line_number(true)
                .with_thread_names(false)
                .with_env_filter(EnvFilter::from_default_env().add_directive(
                    format!("{}=debug", env!("CARGO_PKG_NAME").replace("-", "_")).parse()?,
                ))
                .with_writer(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .and_then(|name| {
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    #[tokio::test]
    async fn get() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = Counter::new(InMemory::new());

        _ = object_store
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let duration = Duration::from_millis(100);

        let cache = Cache::new(object_store, duration)?;

        assert_eq!(0, cache.inner().get_opts()?);

        let metadata = cache.get(&path).await?;
        assert_eq!(1, cache.inner().get_opts()?);

        let options = GetOptions {
            if_none_match: metadata.meta.e_tag.clone(),
            ..Default::default()
        };

        assert!(matches!(
            cache.get_opts(&path, options).await,
            Err(object_store::Error::NotModified { .. })
        ));
        assert_eq!(1, cache.inner().get_opts()?);

        Ok(())
    }

    #[tokio::test]
    async fn get_evict_get() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = Counter::new(InMemory::new());

        _ = object_store
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let duration = Duration::from_millis(100);
        let cache = Cache::new(object_store, duration)?;

        assert_eq!(0, cache.inner().get_opts()?);

        let metadata = cache.get(&path).await?;
        assert_eq!(1, cache.inner().get_opts()?);

        let options = GetOptions {
            if_none_match: metadata.meta.e_tag.clone(),
            ..Default::default()
        };

        assert!(matches!(
            cache.get_opts(&path, options.clone()).await,
            Err(object_store::Error::NotModified { .. })
        ));
        assert_eq!(1, cache.inner().get_opts()?);

        sleep(duration).await;

        assert!(matches!(
            cache.get_opts(&path, options).await,
            Err(object_store::Error::NotModified { .. })
        ));
        assert_eq!(2, cache.inner().get_opts()?);

        Ok(())
    }

    #[tokio::test]
    async fn put_get() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let duration = Duration::from_millis(5_000);
        let cache = Cache::new(Counter::new(InMemory::new()), duration)?;

        assert_eq!(0, cache.inner().put_opts()?);
        assert_eq!(0, cache.inner().get_opts()?);

        let put_result = cache
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;
        assert_eq!(1, cache.inner().put_opts()?);
        assert_eq!(0, cache.inner().get_opts()?);

        let options = GetOptions {
            if_none_match: put_result.e_tag,
            ..Default::default()
        };

        assert!(matches!(
            cache.get_opts(&path, options.clone()).await,
            Err(object_store::Error::NotModified { .. })
        ));
        assert_eq!(0, cache.inner().get_opts()?);

        assert!(matches!(
            cache.get_opts(&path, options).await,
            Err(object_store::Error::NotModified { .. })
        ));
        assert_eq!(0, cache.inner().get_opts()?);

        Ok(())
    }

    /// Inserting sweeps entries that have already expired, so keys that are
    /// never read again don't accumulate.
    #[tokio::test]
    async fn sweep_evicts_expired_entries_on_insert() -> Result<()> {
        let _guard = init_tracing()?;

        let ttl = Duration::from_millis(100);
        let cache = Cache::new(Counter::new(InMemory::new()), ttl)?;

        for (i, id) in ["a", "b"].into_iter().enumerate() {
            if i > 0 {
                sleep(ttl * 2).await;
            }

            _ = cache
                .put(
                    &Path::from(format!("/abc/{id}.json")),
                    serde_json::to_vec(&X(6))
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            assert_eq!(1, cache.entries.lock().map(|guard| guard.len())?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn put_delete_get() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let duration = Duration::from_millis(100);
        let cache = Cache::new(Counter::new(InMemory::new()), duration)?;

        assert_eq!(0, cache.inner().put_opts()?);
        assert_eq!(0, cache.inner().get_opts()?);

        let put_result = cache
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;
        assert_eq!(1, cache.inner().put_opts()?);
        assert_eq!(0, cache.inner().get_opts()?);

        cache.delete(&path).await?;

        let options = GetOptions {
            if_none_match: put_result.e_tag,
            ..Default::default()
        };

        assert!(matches!(
            cache.get_opts(&path, options.clone()).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert_eq!(1, cache.inner().get_opts()?);

        Ok(())
    }

    /// A get that read an object before a put and returns after it must not make the cache
    /// answer NotModified to a reader that presents the version the get read.
    #[tokio::test]
    async fn get_overlapping_a_put_does_not_cache_the_older_version() -> Result<()> {
        let _guard = init_tracing()?;

        let path = Path::from("/abc/test.json");

        let cache = Arc::new(Cache::new(Held::default(), Duration::from_secs(60))?);

        let put = |value: i32, mode: PutMode| {
            let cache = cache.clone();
            let path = path.clone();

            async move {
                cache
                    .put_opts(
                        &path,
                        serde_json::to_vec(&X(value))
                            .map(Bytes::from)
                            .map(PutPayload::from)?,
                        PutOptions {
                            mode,
                            ..PutOptions::default()
                        },
                    )
                    .await
                    .map_err(Error::from)
            }
        };

        let first = put(1, PutMode::Create).await?;

        // A reader that does not know the object's version reads it and is held.
        cache.inner().hold_next_get();
        let reader = tokio::spawn({
            let cache = cache.clone();
            let path = path.clone();
            async move { cache.get_opts(&path, GetOptions::default()).await }
        });
        cache.inner().reached().await;

        // A write replaces the version that the reader read.
        let second = put(
            2,
            PutMode::Update(UpdateVersion {
                e_tag: first.e_tag.clone(),
                version: first.version.clone(),
            }),
        )
        .await?;

        cache.inner().release();
        _ = reader.await.expect("reader")?;

        // A reader that holds the older version must get the newer one, not NotModified.
        let current = cache
            .get_opts(
                &path,
                GetOptions {
                    if_none_match: first.e_tag.clone(),
                    ..GetOptions::default()
                },
            )
            .await?;

        assert_eq!(second.e_tag, current.meta.e_tag);

        Ok(())
    }

    /// Two writes finish in the store in one order and report to the cache in the other. The
    /// cache must not keep the version of the write that finished first, which the second
    /// replaced.
    #[tokio::test]
    async fn put_overlapping_a_put_does_not_cache_the_older_version() -> Result<()> {
        let _guard = init_tracing()?;

        let path = Path::from("/abc/test.json");
        let payload = |value: i32| -> Result<PutPayload> {
            serde_json::to_vec(&X(value))
                .map(Bytes::from)
                .map(PutPayload::from)
                .map_err(Into::into)
        };

        let cache = Arc::new(Cache::new(Held::default(), Duration::from_secs(60))?);

        // The first write is stored, and its response is held.
        cache.inner().hold_next_put();
        let first = tokio::spawn({
            let cache = cache.clone();
            let path = path.clone();
            let payload = payload(1)?;
            async move { cache.put_opts(&path, payload, PutOptions::default()).await }
        });
        cache.inner().reached().await;

        let first_e_tag = cache.inner().inner().head(&path).await?.e_tag;

        // The second write replaces it, and finishes first.
        let second = cache
            .put_opts(
                &path,
                payload(2)?,
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: first_e_tag.clone(),
                        version: None,
                    }),
                    ..PutOptions::default()
                },
            )
            .await?;

        cache.inner().release();
        _ = first.await.expect("first")?;

        // A reader that holds the first version must get the second, not NotModified.
        let current = cache
            .get_opts(
                &path,
                GetOptions {
                    if_none_match: first_e_tag,
                    ..GetOptions::default()
                },
            )
            .await?;

        assert_eq!(second.e_tag, current.meta.e_tag);

        Ok(())
    }

    /// A write that creates an object replaces no version, so it does not count as a change
    /// for the readers in flight on the paths that share its counter. A write that replaces
    /// a version does.
    #[tokio::test]
    async fn only_a_replacing_put_counts_as_a_change() -> Result<()> {
        let _guard = init_tracing()?;

        let path = Path::from("/abc/test.json");
        let cache = Cache::new(Counter::new(InMemory::new()), Duration::from_secs(60))?;
        let payload = |value: i32| -> Result<PutPayload> {
            serde_json::to_vec(&X(value))
                .map(Bytes::from)
                .map(PutPayload::from)
                .map_err(Into::into)
        };

        let before_create = cache.generation(&path).load(Ordering::SeqCst);

        let created = cache
            .put_opts(
                &path,
                payload(1)?,
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await?;

        assert_eq!(
            before_create,
            cache.generation(&path).load(Ordering::SeqCst)
        );

        _ = cache
            .put_opts(
                &path,
                payload(2)?,
                PutOptions {
                    mode: PutMode::Update(UpdateVersion {
                        e_tag: created.e_tag,
                        version: created.version,
                    }),
                    ..PutOptions::default()
                },
            )
            .await?;

        assert_eq!(
            before_create + 1,
            cache.generation(&path).load(Ordering::SeqCst)
        );

        Ok(())
    }

    /// A write that creates an object on a path sharing a reader's counter does not cost the
    /// reader its entry: the reader stores the version it read, and the next reader that
    /// presents that version is answered without the store.
    #[tokio::test]
    async fn creating_put_on_a_sharing_path_keeps_an_overlapping_readers_entry() -> Result<()> {
        let _guard = init_tracing()?;

        let path = Path::from("/abc/test.json");
        let cache = Arc::new(Cache::new(Held::default(), Duration::from_secs(60))?);
        let payload = |value: i32| -> Result<PutPayload> {
            serde_json::to_vec(&X(value))
                .map(Bytes::from)
                .map(PutPayload::from)
                .map_err(Into::into)
        };
        let create = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };

        let created = cache.put_opts(&path, payload(1)?, create.clone()).await?;

        let sharing = (0..)
            .map(|n| Path::from(format!("/abc/sharing-{n}.json")))
            .find(|candidate| std::ptr::eq(cache.generation(&path), cache.generation(candidate)))
            .expect("a path that shares the counter");

        // A reader that does not know the object's version reads it and is held.
        cache.inner().hold_next_get();
        let reader = tokio::spawn({
            let cache = cache.clone();
            let path = path.clone();
            async move { cache.get_opts(&path, GetOptions::default()).await }
        });
        cache.inner().reached().await;

        _ = cache.put_opts(&sharing, payload(2)?, create).await?;

        cache.inner().release();
        _ = reader.await.expect("reader")?;

        let gets_before = cache.inner().if_none_match().len();

        assert_matches!(
            cache
                .get_opts(
                    &path,
                    GetOptions {
                        if_none_match: created.e_tag.clone(),
                        ..GetOptions::default()
                    },
                )
                .await,
            Err(object_store::Error::NotModified { .. })
        );
        assert_eq!(gets_before, cache.inner().if_none_match().len());

        Ok(())
    }
}
