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
    fmt::Debug,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::dynostore::object_store_error_name;
use bytes::Bytes;
use nisshi_storage::Result;
use object_store::{
    Attributes, GetOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, TagSet,
    UpdateVersion, path::Path,
};
use opentelemetry::{KeyValue, metrics::Counter};
use serde::{Serialize, de::DeserializeOwned};
use tracing::{debug, instrument};

use super::METER;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DataVersion<D> {
    data: D,
    version: Option<UpdateVersion>,
}

impl<D> From<&DataVersion<D>> for PutMode {
    fn from(value: &DataVersion<D>) -> Self {
        value
            .version
            .clone()
            .map_or(PutMode::Create, PutMode::Update)
    }
}

static REQUESTS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_opticon_requests")
        .with_description("OptiCon requests")
        .build()
});

static ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_opticon_errors")
        .with_description("OptiCon requests")
        .build()
});

#[derive(Clone, Debug, Default)]
pub(super) struct OptiCon<D> {
    path: Path,
    tags: TagSet,
    attributes: Attributes,
    data_version: Arc<Mutex<Option<DataVersion<D>>>>,
    /// Counts the changes to `data_version`. A call to the object store that began before a
    /// change must not install what it read or wrote, which may be older than what the change
    /// stored.
    generation: Arc<AtomicU64>,
}

impl<D> OptiCon<D> {
    pub(super) fn path(path: impl Into<Path>) -> Self {
        Self {
            path: path.into(),
            tags: Default::default(),
            attributes: Default::default(),
            data_version: Default::default(),
            generation: Default::default(),
        }
    }

    /// Returns the location of the document in the object store.
    pub(super) fn location(&self) -> &Path {
        &self.path
    }
}

impl<D> OptiCon<D>
where
    D: Clone + Debug + Default + DeserializeOwned + PartialEq + Serialize,
{
    /// Installs `replacement` as the cached version, unless it changed since `before`, the
    /// generation read before the object store call that produced `replacement`. Returns
    /// `replacement` when it did not install it.
    fn install(
        &self,
        before: u64,
        replacement: Option<DataVersion<D>>,
    ) -> Result<Option<Option<DataVersion<D>>>> {
        let mut guard = self.data_version.lock()?;

        if self.generation.load(Ordering::SeqCst) == before {
            *guard = replacement;
            _ = self.generation.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        } else {
            Ok(Some(replacement))
        }
    }

    #[instrument(skip_all, fields(path = %self.path))]
    async fn get(&self, object_store: &impl ObjectStore) -> Result<()> {
        const METHOD: &str = "get";
        REQUESTS.add(1, &[KeyValue::new("method", METHOD)]);

        let on_error = |error: &object_store::Error| {
            ERRORS.add(
                1,
                &[
                    KeyValue::new("method", METHOD),
                    KeyValue::new("error", object_store_error_name(error)),
                ],
            );
        };

        let before = self.generation.load(Ordering::SeqCst);

        match object_store.get(&self.path).await.inspect_err(|error| {
            debug!(?error);
            on_error(error)
        }) {
            Ok(get_result) => {
                let version = Some(UpdateVersion {
                    e_tag: get_result.meta.e_tag.clone(),
                    version: get_result.meta.version.clone(),
                });

                let encoded = get_result.bytes().await.inspect_err(|error| {
                    debug!(?error);
                    on_error(error)
                })?;
                let data = serde_json::from_slice::<D>(&encoded)?;

                debug!(?version);

                self.install(before, Some(DataVersion { data, version }))
                    .map(|_not_installed| ())
            }

            Err(object_store::Error::NotFound { .. }) => {
                self.install(before, None).map(|_not_installed| ())
            }

            Err(otherwise) => Err(otherwise.into()),
        }
    }

    #[instrument(skip_all, fields(path = %self.path))]
    pub(super) async fn with<E, F>(&self, object_store: &impl ObjectStore, f: F) -> Result<E>
    where
        F: Fn(&D) -> Result<E>,
    {
        const METHOD: &str = "with";
        REQUESTS.add(1, &[KeyValue::new("method", METHOD)]);

        let on_error = |error: &object_store::Error| {
            ERRORS.add(
                1,
                &[
                    KeyValue::new("method", METHOD),
                    KeyValue::new("error", object_store_error_name(error)),
                ],
            );
        };

        // Read before the version below: a change after this read, including one between the
        // two reads, stops this call from installing what it fetches.
        let before = self.generation.load(Ordering::SeqCst);

        let version = self
            .data_version
            .lock()
            .map(|guard| guard.as_ref().and_then(|dv| dv.version.clone()))?;
        debug!(?version);

        // What this call fetched, when it could not install it because the cached version
        // changed meanwhile. The fetch began after this call did, so it is at least as new as
        // anything written before this call.
        let fetched = match object_store
            .get_opts(
                &self.path,
                GetOptions {
                    if_none_match: version.as_ref().and_then(|version| version.e_tag.clone()),
                    ..GetOptions::default()
                },
            )
            .await
            .inspect_err(|error| {
                debug!(?error);
                on_error(error)
            }) {
            Ok(get_result) => {
                let version = Some(UpdateVersion {
                    e_tag: get_result.meta.e_tag.clone(),
                    version: get_result.meta.version.clone(),
                });

                debug!(action = "out of date", ?version);

                let encoded = get_result.bytes().await.inspect_err(|error| {
                    debug!(?error);
                    on_error(error)
                })?;

                let data = serde_json::from_slice::<D>(&encoded)?;

                self.install(before, Some(DataVersion { data, version }))?
                    .map(|not_installed| not_installed.map(|dv| dv.data).unwrap_or_default())
            }

            Err(object_store::Error::NotFound { .. }) => {
                debug!(action = "not found");

                self.install(before, None)?
                    .map(|_not_installed| D::default())
            }

            Err(object_store::Error::NotModified { .. }) => {
                debug!(action = "not modified");
                None
            }

            Err(otherwise) => return Err(otherwise.into()),
        };

        if let Some(data) = fetched {
            debug!(action = "not installed", ?data);
            return f(&data);
        }

        self.data_version
            .lock()
            .map_err(Into::into)
            .and_then(|lock| {
                if let Some(dv @ DataVersion { data, .. }) = lock.as_ref() {
                    debug!(?dv);
                    f(data)
                } else {
                    let data = D::default();
                    debug!(?data);
                    f(&data)
                }
            })
    }

    #[instrument(skip_all, fields(path = %self.path))]
    pub(super) async fn with_mut<E, F>(&self, object_store: &impl ObjectStore, f: F) -> Result<E>
    where
        E: Debug,
        F: Fn(&mut D) -> Result<E>,
    {
        const METHOD: &str = "with_mut";
        REQUESTS.add(1, &[KeyValue::new("method", METHOD)]);

        let on_error = |error: &object_store::Error| {
            ERRORS.add(
                1,
                &[
                    KeyValue::new("method", METHOD),
                    KeyValue::new("error", object_store_error_name(error)),
                ],
            );
        };

        loop {
            REQUESTS.add(1, &[KeyValue::new("method", "with_mut_loop")]);

            // Read before the data below, as in `with`.
            let before = self.generation.load(Ordering::SeqCst);

            let (outcome, dv) = self.data_version.lock().map(|guard| {
                let mut dv = guard.clone().unwrap_or_default();
                let outcome = f(&mut dv.data);
                (outcome, dv)
            })?;

            let payload = serde_json::to_vec(&dv.data)
                .map(Bytes::from)
                .map(PutPayload::from)?;

            let opts = PutOptions {
                mode: PutMode::from(&dv),
                tags: self.tags.clone(),
                attributes: self.attributes.clone(),
                ..Default::default()
            };

            match object_store
                .put_opts(&self.path, payload, opts)
                .await
                .inspect_err(|error| {
                    debug!(?error);
                    on_error(error)
                }) {
                Ok(put_result) => {
                    let written_e_tag = put_result.e_tag.clone();

                    let written = DataVersion {
                        data: dv.data,
                        version: Some(UpdateVersion {
                            e_tag: put_result.e_tag,
                            version: put_result.version,
                        }),
                    };

                    // A change while the put was in flight may have installed a later write
                    // than this one, which this put's version must not replace. The cached
                    // version is dropped instead, so the next read asks the store. A reader
                    // that read this put's own version installed it already, and it stays.
                    if self.install(before, Some(written))?.is_some() {
                        let mut guard = self.data_version.lock()?;

                        let holds_this_write = written_e_tag.is_some()
                            && guard
                                .as_ref()
                                .and_then(|cached| cached.version.as_ref())
                                .is_some_and(|version| version.e_tag == written_e_tag);

                        if !holds_this_write {
                            *guard = None;
                        }
                    }

                    return outcome;
                }

                Err(
                    object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. },
                ) => {
                    self.get(object_store).await?;
                    continue;
                }

                Err(err) => return Err(err.into()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nisshi_storage::Error;
    use object_store::{PutPayload, memory::InMemory};
    use serde::{Deserialize, Serialize};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use super::*;
    use crate::dynostore::held::Held;

    #[derive(
        Clone, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    struct X(i32);

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
    async fn with_does_not_exist() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = InMemory::new();

        let o = OptiCon::path(path.clone());

        assert_eq!(1, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

        assert!(matches!(
            object_store.get(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));

        assert_eq!(1, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

        assert!(matches!(
            object_store.get(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));

        Ok(())
    }

    #[tokio::test]
    async fn with_mut_does_not_exist() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = InMemory::new();

        let o = OptiCon::path(path.clone());

        let expected = 1;
        assert_eq!(
            expected,
            o.with_mut(&object_store, |x: &mut X| {
                x.0 += 1;
                Ok(x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(expected, data.0);

        let expected = 2;
        assert_eq!(
            expected,
            o.with_mut(&object_store, |x: &mut X| {
                x.0 += 1;
                Ok(x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(expected, data.0);

        Ok(())
    }

    #[tokio::test]
    async fn with_did_exist() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = InMemory::new();

        _ = object_store
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let o = OptiCon::path(path.clone());

        assert_eq!(7, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

        object_store.delete(&path).await?;

        assert_eq!(1, o.with(&object_store, |x| Ok(x.0 + 1)).await?);

        assert!(matches!(
            object_store.get(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));

        assert_eq!(1, o.with(&object_store, |x| Ok(x.0 + 1)).await?);

        assert!(matches!(
            object_store.get(&path).await,
            Err(object_store::Error::NotFound { .. })
        ));

        Ok(())
    }

    #[tokio::test]
    async fn with_mut_did_exist() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = InMemory::new();

        _ = object_store
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let o = OptiCon::path(path.clone());

        let expected = 7;
        assert_eq!(
            expected,
            o.with_mut(&object_store, |x: &mut X| {
                x.0 += 1;
                Ok(x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(expected, data.0);

        object_store.delete(&path).await?;

        let expected = 1;
        assert_eq!(
            expected,
            o.with_mut(&object_store, |x| {
                x.0 += 1;
                Ok(x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(expected, data.0);

        let expected = 2;
        assert_eq!(
            expected,
            o.with_mut(&object_store, |x| {
                x.0 += 1;
                Ok(x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(expected, data.0);

        Ok(())
    }

    #[tokio::test]
    async fn with_already_exists() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = InMemory::new();

        _ = object_store
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let o = OptiCon::path(path.clone());

        assert_eq!(7, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(6, data.0);

        assert_eq!(7, o.with(&object_store, |x: &X| Ok(x.0 + 1)).await?);

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(6, data.0);

        Ok(())
    }

    #[tokio::test]
    async fn with_mut_already_exists() -> Result<()> {
        let _guard = init_tracing()?;

        let id = "test";
        let path = Path::from(format!("/abc/{id}.json"));

        let object_store = InMemory::new();

        _ = object_store
            .put(
                &path,
                serde_json::to_vec(&X(6))
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let o = OptiCon::path(path.clone());

        assert_eq!(
            42,
            o.with_mut(&object_store, |x: &mut X| {
                x.0 += 1;

                Ok(6 * x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(7, data.0);

        assert_eq!(
            48,
            o.with_mut(&object_store, |x: &mut X| {
                x.0 += 1;

                Ok(6 * x.0)
            })
            .await?
        );

        let get_result = object_store.get(&path).await?;
        let encoded = get_result.bytes().await?;
        let data = serde_json::from_slice::<X>(&encoded)?;
        assert_eq!(8, data.0);

        Ok(())
    }

    /// A read that fetched a version before a write and returns after it must not install that
    /// version over the write's: the next read presents the write's version.
    #[tokio::test]
    async fn with_overlapping_a_write_does_not_install_the_older_version() -> Result<()> {
        let _guard = init_tracing()?;

        let object_store = Arc::new(Held::default());
        let path = Path::from("/abc/test.json");

        // The object exists, at a version this OptiCon has not read.
        OptiCon::<X>::path(path.clone())
            .with_mut(object_store.as_ref(), |x| {
                x.0 = 1;
                Ok(())
            })
            .await?;

        let opticon = OptiCon::<X>::path(path.clone());

        object_store.hold_next_get();
        let reader = tokio::spawn({
            let opticon = opticon.clone();
            let object_store = object_store.clone();
            async move { opticon.with(object_store.as_ref(), |x| Ok(x.0)).await }
        });
        object_store.reached().await;

        opticon
            .with_mut(object_store.as_ref(), |x| {
                x.0 = 2;
                Ok(())
            })
            .await?;

        let written = object_store.inner().head(&path).await?.e_tag;

        object_store.release();

        // The held read began before the write, so it may answer with the older value.
        assert_eq!(1, reader.await.expect("reader")?);

        assert_eq!(2, opticon.with(object_store.as_ref(), |x| Ok(x.0)).await?);
        assert_eq!(Some(written), object_store.if_none_match().last().cloned());

        Ok(())
    }

    /// A write whose response returns after a later write to the same document must not
    /// install its version over the later write's: the next read does not present it.
    #[tokio::test]
    async fn with_mut_overlapping_a_write_does_not_install_the_older_version() -> Result<()> {
        let _guard = init_tracing()?;

        let object_store = Arc::new(Held::default());
        let path = Path::from("/abc/test.json");
        let opticon = OptiCon::<X>::path(path.clone());

        // The first write is stored, and its response is held.
        object_store.hold_next_put();
        let first = tokio::spawn({
            let opticon = opticon.clone();
            let object_store = object_store.clone();
            async move {
                opticon
                    .with_mut(object_store.as_ref(), |x| {
                        x.0 = 1;
                        Ok(())
                    })
                    .await
            }
        });
        object_store.reached().await;

        let first_written = object_store.inner().head(&path).await?.e_tag;

        // A second write finds the first in the store, and replaces it.
        opticon
            .with_mut(object_store.as_ref(), |x| {
                x.0 = 2;
                Ok(())
            })
            .await?;

        object_store.release();
        first.await.expect("first")?;

        // The first write's version is not cached: the read asks the store without a version.
        assert_eq!(2, opticon.with(object_store.as_ref(), |x| Ok(x.0)).await?);
        assert_ne!(
            first_written,
            object_store.if_none_match().last().cloned().flatten()
        );
        assert_eq!(Some(&None), object_store.if_none_match().last());

        Ok(())
    }

    /// A reader that installs a write's own version while the write's response is held leaves
    /// that version cached: the next read presents it, and the store answers not modified.
    #[tokio::test]
    async fn with_mut_keeps_the_version_that_a_reader_installed_for_the_same_write() -> Result<()> {
        let _guard = init_tracing()?;

        let object_store = Arc::new(Held::default());
        let path = Path::from("/abc/test.json");
        let opticon = OptiCon::<X>::path(path.clone());

        object_store.hold_next_put();
        let write = tokio::spawn({
            let opticon = opticon.clone();
            let object_store = object_store.clone();
            async move {
                opticon
                    .with_mut(object_store.as_ref(), |x| {
                        x.0 = 1;
                        Ok(())
                    })
                    .await
            }
        });
        object_store.reached().await;

        let written = object_store.inner().head(&path).await?.e_tag;
        assert!(written.is_some());

        // The cache is empty, so the reader fetches the write's version and installs it.
        assert_eq!(1, opticon.with(object_store.as_ref(), |x| Ok(x.0)).await?);

        object_store.release();
        write.await.expect("write")?;

        assert_eq!(1, opticon.with(object_store.as_ref(), |x| Ok(x.0)).await?);
        assert_eq!(Some(written), object_store.if_none_match().last().cloned());

        Ok(())
    }
}
