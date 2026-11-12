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

//! Regression coverage for `DynoStore::ping()`: when the underlying object
//! store's `list()` fails (e.g. no usable AWS credentials), `ping()` must
//! return that error rather than `Ok(())`, so the broker fails to start
//! instead of failing confusingly on the first real request.

use std::fmt::{Debug, Display};

use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use nisshi_storage::{Error, Result, Storage as _};
use object_store::{
    CopyOptions, GetOptions, GetResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};
use url::Url;

use crate::dynostore::{DynoStore, tests::init_tracing};

/// Wraps an [`ObjectStore`] and makes each `list()` call fail, except a call
/// for a prefix under `allowed_prefix`, to exercise the `ping()` path without
/// real credentials or network access.
#[derive(Clone)]
struct FailingListObjectStore<O> {
    object_store: O,
    allowed_prefix: Option<Path>,
}

impl<O> FailingListObjectStore<O> {
    fn new(object_store: O) -> Self {
        Self {
            object_store,
            allowed_prefix: None,
        }
    }

    /// Allows `list()` only under `prefix`, like an IAM policy that grants
    /// `s3:ListBucket` with an `s3:prefix` condition.
    fn allow_prefix(object_store: O, prefix: &str) -> Self {
        Self {
            object_store,
            allowed_prefix: Some(Path::from(prefix)),
        }
    }
}

impl<O> Debug for FailingListObjectStore<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailingListObjectStore").finish()
    }
}

impl<O> Display for FailingListObjectStore<O> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FailingListObjectStore").finish()
    }
}

#[async_trait]
impl<O> ObjectStore for FailingListObjectStore<O>
where
    O: ObjectStore,
{
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, object_store::Error> {
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
        if let (Some(prefix), Some(allowed)) = (prefix, self.allowed_prefix.as_ref())
            && prefix.prefix_matches(allowed)
        {
            return self.object_store.list(Some(prefix));
        }

        Box::pin(stream::once(async {
            Err(object_store::Error::Generic {
                store: "test",
                source: "forced list failure".into(),
            })
        }))
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

#[tokio::test]
async fn ping_propagates_list_error() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let advertised_listener = Url::parse("tcp://localhost:9092")?;

    let object_store = FailingListObjectStore::new(InMemory::new());

    let storage =
        DynoStore::new("nisshi", 12321, object_store)?.advertised_listener(advertised_listener);

    assert!(storage.ping().await.is_err());

    Ok(())
}

/// Regression guard: a genuinely healthy store must still report `Ok`.
#[tokio::test]
async fn ping_ok_on_healthy_store() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let advertised_listener = Url::parse("tcp://localhost:9092")?;

    let storage =
        DynoStore::new("nisshi", 12322, InMemory::new())?.advertised_listener(advertised_listener);

    assert!(storage.ping().await.is_ok());

    Ok(())
}

/// `ping()` lists only the cluster's prefix, so a policy that allows listing
/// only under `clusters/<cluster>/` still passes the startup check.
#[tokio::test]
async fn ping_lists_only_the_cluster_prefix() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let advertised_listener = Url::parse("tcp://localhost:9092")?;

    let object_store = FailingListObjectStore::allow_prefix(InMemory::new(), "clusters/nisshi/");

    let storage =
        DynoStore::new("nisshi", 12323, object_store)?.advertised_listener(advertised_listener);

    assert!(storage.ping().await.is_ok());

    Ok(())
}
