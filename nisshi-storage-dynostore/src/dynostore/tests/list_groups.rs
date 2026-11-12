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

//! The `ListGroups` response when a group's state can't be read or decoded. The group is listed in
//! the `Unknown` state, and the request still lists every other group.

use nisshi_sans_io::{ErrorCode, ListGroupsRequest};
use nisshi_storage::{Error, GroupDetail, ListGroupsService, Result};
use object_store::{ObjectStoreExt as _, PutPayload, memory::InMemory};
use rama::Service as _;

use crate::dynostore::{
    DynoStore,
    tests::{failing_object_store::FailingObjectStore, init_tracing},
};

const GROUP_ID: &str = "abc";

#[tokio::test]
async fn unreadable_state_is_listed_as_unknown() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = InMemory::new();
    let storage = DynoStore::new(
        "nisshi",
        111,
        FailingObjectStore::failing_get(object_store.clone()),
    )?;

    _ = object_store
        .put(
            &storage.group_state_location(GROUP_ID)?,
            PutPayload::from(serde_json::to_vec(&GroupDetail::default())?),
        )
        .await?;

    let response = ListGroupsService { storage }
        .serve(ListGroupsRequest::default())
        .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

    let groups = response.groups.unwrap_or_default();
    assert_eq!(1, groups.len());
    assert_eq!(GROUP_ID, groups[0].group_id);
    assert_eq!(Some("Unknown".into()), groups[0].group_state);

    Ok(())
}

#[tokio::test]
async fn undecodable_state_is_listed_as_unknown() -> Result<(), Error> {
    let _guard = init_tracing()?;

    let object_store = InMemory::new();
    let storage = DynoStore::new("nisshi", 111, object_store.clone())?;

    _ = object_store
        .put(
            &storage.group_state_location(GROUP_ID)?,
            PutPayload::from_static(b"not json"),
        )
        .await?;

    let response = ListGroupsService { storage }
        .serve(ListGroupsRequest::default())
        .await?;

    assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);

    let groups = response.groups.unwrap_or_default();
    assert_eq!(1, groups.len());
    assert_eq!(GROUP_ID, groups[0].group_id);
    assert_eq!(Some("Unknown".into()), groups[0].group_state);

    Ok(())
}
