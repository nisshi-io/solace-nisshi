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

//! The error code that `DynoStore::describe_groups` answers when it can't read a group. Kafka
//! clients retry `COORDINATOR_NOT_AVAILABLE`, so an object store error answers that code, and a
//! group that can't be decoded answers `UNKNOWN_SERVER_ERROR`, because a retry does not fix it.

use nisshi_sans_io::ErrorCode;
use nisshi_storage::{Error, NamedGroupDetail, Result, Storage as _};
use object_store::{ObjectStoreExt as _, PutPayload, memory::InMemory};

use crate::dynostore::{
    DynoStore,
    tests::{failing_object_store::FailingObjectStore, init_tracing},
};

const GROUP_ID: &str = "abc";

async fn describe(storage: &DynoStore) -> Result<Vec<NamedGroupDetail>> {
    storage
        .describe_groups(Some(&[GROUP_ID.to_owned()]), false)
        .await
}

#[tokio::test]
async fn failed_state_read_answers_coordinator_not_available() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(
        "nisshi",
        111,
        FailingObjectStore::failing_get(InMemory::new()),
    )?;

    assert_eq!(
        vec![NamedGroupDetail::error_code(
            GROUP_ID.into(),
            ErrorCode::CoordinatorNotAvailable
        )],
        describe(&storage).await?
    );

    Ok(())
}

#[tokio::test]
async fn failed_offsets_list_answers_coordinator_not_available() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let storage = DynoStore::new(
        "nisshi",
        111,
        FailingObjectStore::failing_list(InMemory::new()),
    )?;

    assert_eq!(
        vec![NamedGroupDetail::error_code(
            GROUP_ID.into(),
            ErrorCode::CoordinatorNotAvailable
        )],
        describe(&storage).await?
    );

    Ok(())
}

#[tokio::test]
async fn undecodable_state_answers_unknown_server_error() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = InMemory::new();
    let storage = DynoStore::new("nisshi", 111, object_store.clone())?;

    _ = object_store
        .put(
            &storage.group_state_location(GROUP_ID)?,
            PutPayload::from_static(b"not json"),
        )
        .await?;

    assert_eq!(
        vec![NamedGroupDetail::error_code(
            GROUP_ID.into(),
            ErrorCode::UnknownServerError
        )],
        describe(&storage).await?
    );

    Ok(())
}
