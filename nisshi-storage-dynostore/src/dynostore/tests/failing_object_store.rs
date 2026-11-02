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

//! An object store whose reads or chosen writes fail, to test how storage answers an object
//! store error.

use std::{
    fmt::{Debug, Display},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};

/// Selects a write by its path and payload.
pub(crate) type PutSelector = fn(&Path, &PutPayload) -> bool;

/// Wraps an [`ObjectStore`] and makes each `get()`, each `list()` or each `put()` call fail, or
/// the next `put()` that a [`PutSelector`] picks.
#[derive(Clone)]
pub(crate) struct FailingObjectStore<O> {
    object_store: O,
    get_fails: bool,
    list_fails: bool,
    put_fails: bool,
    next_failing_put: Arc<Mutex<Option<PutSelector>>>,
}

impl<O> FailingObjectStore<O> {
    pub(crate) fn failing_get(object_store: O) -> Self {
        Self {
            object_store,
            get_fails: true,
            list_fails: false,
            put_fails: false,
            next_failing_put: Arc::default(),
        }
    }

    pub(crate) fn failing_list(object_store: O) -> Self {
        Self {
            object_store,
            get_fails: false,
            list_fails: true,
            put_fails: false,
            next_failing_put: Arc::default(),
        }
    }

    pub(crate) fn failing_put(object_store: O) -> Self {
        Self {
            object_store,
            get_fails: false,
            list_fails: false,
            put_fails: true,
            next_failing_put: Arc::default(),
        }
    }

    /// Passes every call through until [`Self::fail_next_put`] picks a write to fail.
    pub(crate) fn failing_put_on_demand(object_store: O) -> Self {
        Self {
            object_store,
            get_fails: false,
            list_fails: false,
            put_fails: false,
            next_failing_put: Arc::default(),
        }
    }

    /// Makes the next `put()` that `selector` picks fail, and only that one. A clone of this
    /// store shares the choice, so a test can keep a clone and arm it after setup.
    pub(crate) fn fail_next_put(&self, selector: PutSelector) {
        if let Ok(mut next_failing_put) = self.next_failing_put.lock() {
            _ = next_failing_put.replace(selector);
        }
    }

    fn selected_put_fails(&self, location: &Path, payload: &PutPayload) -> bool {
        self.next_failing_put
            .lock()
            .ok()
            .is_some_and(|mut next_failing_put| {
                next_failing_put
                    .take_if(|selector| selector(location, payload))
                    .is_some()
            })
    }
}

fn forced_failure() -> object_store::Error {
    object_store::Error::Generic {
        store: "test",
        source: "forced failure".into(),
    }
}

impl<O> Debug for FailingObjectStore<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailingObjectStore").finish()
    }
}

impl<O> Display for FailingObjectStore<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailingObjectStore").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for FailingObjectStore<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        if self.put_fails || self.selected_put_fails(location, &payload) {
            return Err(forced_failure());
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
        if self.get_fails {
            return Err(forced_failure());
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
        if self.list_fails {
            return Box::pin(stream::once(async { Err(forced_failure()) }));
        }

        self.object_store.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<object_store::ListResult, object_store::Error> {
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
