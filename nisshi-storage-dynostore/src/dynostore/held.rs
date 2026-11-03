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

//! An object store for tests that holds back one response after the store has produced it, so
//! that a test can run another call in between.

use std::{
    fmt::{self, Display},
    sync::Mutex,
};

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tokio::sync::Notify;

#[derive(Debug, Default)]
pub(super) struct Held {
    object_store: InMemory,
    hold_get: Mutex<bool>,
    hold_put: Mutex<bool>,
    if_none_match: Mutex<Vec<Option<String>>>,
    reached: Notify,
    released: Notify,
}

impl Held {
    /// Holds the response of the next get, after the store has read.
    pub(super) fn hold_next_get(&self) {
        *self.hold_get.lock().expect("hold_get") = true;
    }

    /// Holds the response of the next put, after the store has written.
    pub(super) fn hold_next_put(&self) {
        *self.hold_put.lock().expect("hold_put") = true;
    }

    /// Waits until a call is held.
    pub(super) async fn reached(&self) {
        self.reached.notified().await;
    }

    pub(super) fn release(&self) {
        self.released.notify_one();
    }

    /// The `if_none_match` of every get so far.
    pub(super) fn if_none_match(&self) -> Vec<Option<String>> {
        self.if_none_match.lock().expect("if_none_match").clone()
    }

    pub(super) fn inner(&self) -> &InMemory {
        &self.object_store
    }

    async fn hold(&self, flag: &Mutex<bool>) {
        if std::mem::take(&mut *flag.lock().expect("flag")) {
            self.reached.notify_one();
            self.released.notified().await;
        }
    }
}

impl Display for Held {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Held").finish()
    }
}

#[async_trait]
impl ObjectStore for Held {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let result = self.object_store.put_opts(location, payload, opts).await;
        self.hold(&self.hold_put).await;
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.object_store.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.if_none_match
            .lock()
            .expect("if_none_match")
            .push(options.if_none_match.clone());

        let result = self.object_store.get_opts(location, options).await;
        self.hold(&self.hold_get).await;
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.object_store.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.object_store.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.object_store.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.object_store.copy_opts(from, to, opts).await
    }
}
