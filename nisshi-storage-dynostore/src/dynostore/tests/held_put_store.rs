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

//! An object store that holds one put until a test releases it, so a test can run another
//! request while the first one waits at a chosen step.

use std::{
    fmt::{self, Debug, Display},
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use futures::stream::BoxStream;
use nisshi_storage::Error;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use tokio::{
    sync::{Notify, oneshot},
    time::timeout,
};

/// Passes every call through to an [`InMemory`] store, except the next put to a held path
/// after [`hold_next_put`](Self::hold_next_put), which waits until the test releases it.
#[derive(Clone)]
pub(super) struct HeldPutStore {
    object_store: Arc<InMemory>,
    path_is_held: fn(&Path) -> bool,
    hold: Arc<Mutex<Option<Hold>>>,
}

struct Hold {
    held: Arc<Notify>,
    release: oneshot::Receiver<()>,
}

/// The test's side of a held put.
pub(super) struct HeldPut {
    held: Arc<Notify>,
    release: oneshot::Sender<()>,
}

impl HeldPutStore {
    /// Returns a store that can hold a put to a path for which `path_is_held` is true.
    pub(super) fn new(path_is_held: fn(&Path) -> bool) -> Self {
        Self {
            object_store: Arc::new(InMemory::new()),
            path_is_held,
            hold: Arc::new(Mutex::new(None)),
        }
    }

    /// Holds the next put to a held path, and only that put, until [`HeldPut::release`].
    ///
    /// A put made before this call is not held, so a test can create its topic and write its
    /// first batches before it arms the hold.
    pub(super) fn hold_next_put(&self) -> HeldPut {
        let held = Arc::new(Notify::new());
        let (release, receiver) = oneshot::channel();

        if let Ok(mut hold) = self.hold.lock() {
            _ = hold.replace(Hold {
                held: held.clone(),
                release: receiver,
            });
        }

        HeldPut { held, release }
    }
}

impl HeldPut {
    /// The longest a test waits for the store to hold the put.
    const HOLD_TIMEOUT: Duration = Duration::from_secs(10);

    /// Waits until the store holds the put.
    ///
    /// Returns an error when the store has not held a put after [`HeldPut::HOLD_TIMEOUT`],
    /// so a request that never makes the put fails the test instead of hanging it.
    pub(super) async fn wait_until_held(&self) -> Result<(), Error> {
        timeout(Self::HOLD_TIMEOUT, self.held.notified())
            .await
            .map_err(|_| Error::Message(String::from("the store did not hold a put")))
    }

    /// Lets the held put continue to the object store.
    pub(super) fn release(self) {
        _ = self.release.send(());
    }
}

impl Debug for HeldPutStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HeldPutStore").finish()
    }
}

impl Display for HeldPutStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HeldPutStore").finish()
    }
}

#[async_trait]
impl ObjectStore for HeldPutStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
        let hold = (self.path_is_held)(location)
            .then(|| self.hold.lock().ok().and_then(|mut hold| hold.take()))
            .flatten();

        if let Some(hold) = hold {
            // Notify stores a permit, so the test sees the hold also when it starts to wait
            // after this call.
            hold.held.notify_one();
            _ = hold.release.await;
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

/// Whether a put writes a batch object.
pub(super) fn is_batch(location: &Path) -> bool {
    location.as_ref().ends_with(".batch")
}

/// Whether a put writes a partition's watermark.
pub(super) fn is_watermark(location: &Path) -> bool {
    location.as_ref().ends_with("watermark.json")
}
