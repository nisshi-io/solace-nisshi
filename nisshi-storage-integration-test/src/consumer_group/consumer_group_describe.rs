// Copyright (c) Solace Corporation
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

use std::slice;

use nisshi_broker::{Error, Result};
use nisshi_sans_io::{
    ConsumerGroupDescribeRequest, ErrorCode, RequestInput,
    consumer_group_describe_response::DescribedGroup,
};
use nisshi_storage::{ConsumerGroupDescribeService, GroupDetail, Storage};
use rama::{Service as _, extensions::Extensions};
use tracing::debug;
use url::Url;

use crate::common::{self, alphanumeric_string, register_broker};

async fn create_group(storage: &impl Storage, group_id: &str) -> Result<()> {
    _ = storage
        .update_group(group_id, GroupDetail::default(), None)
        .await
        .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;
    Ok(())
}

async fn describe(
    storage: impl Storage,
    group_ids: &[String],
    include_authorized_operations: bool,
) -> Result<Vec<DescribedGroup>> {
    let response = ConsumerGroupDescribeService { storage }
        .serve(RequestInput {
            request: ConsumerGroupDescribeRequest::default()
                .group_ids(Some(group_ids.into()))
                .include_authorized_operations(include_authorized_operations),
            extensions: Extensions::default(),
        })
        .await?;

    Ok(response.groups.unwrap_or_default())
}

/// Every Nisshi group is a classic group. An admin client describes a group with `DescribeGroups`
/// only if `ConsumerGroupDescribe` answers it with an error, so an answer without an error makes
/// `kafka-consumer-groups --describe --members` show no members. The broker must answer
/// `GROUP_ID_NOT_FOUND` for a stored group, as Kafka does for a classic group, and omit the
/// group's authorized operations, as it does on every error row.
async fn stored_group_answers_group_id_not_found(storage: impl Storage + Clone) -> Result<()> {
    let group_id = alphanumeric_string(15);
    create_group(&storage, &group_id).await?;

    for include_authorized_operations in [false, true] {
        let groups = describe(
            storage.clone(),
            slice::from_ref(&group_id),
            include_authorized_operations,
        )
        .await?;
        assert_eq!(1, groups.len());
        assert_eq!(
            ErrorCode::GroupIdNotFound,
            ErrorCode::try_from(groups[0].error_code)?
        );
        assert_eq!(
            Some(format!("Group {group_id} is not a consumer group.")),
            groups[0].error_message
        );
        assert_eq!(i32::MIN, groups[0].authorized_operations);
    }

    Ok(())
}

/// Requires an engine that answers an unknown group with `GROUP_ID_NOT_FOUND`.
#[cfg(any(feature = "dynostore", feature = "libsql", feature = "postgres"))]
async fn error_row_omits_authorized_operations(storage: impl Storage + Clone) -> Result<()> {
    for include_authorized_operations in [false, true] {
        let groups = describe(
            storage.clone(),
            &[alphanumeric_string(15)],
            include_authorized_operations,
        )
        .await?;
        assert_eq!(1, groups.len());
        assert_eq!(
            ErrorCode::GroupIdNotFound,
            ErrorCode::try_from(groups[0].error_code)?
        );
        assert_eq!(i32::MIN, groups[0].authorized_operations);
    }

    Ok(())
}

pub async fn describe_non_existent_group<C, G>(
    cluster_id: C,
    broker_id: i32,
    advertised_listener: Url,
    storage: G,
) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    debug!(broker_id, %advertised_listener);
    register_broker(cluster_id, broker_id, &storage).await?;

    let service = ConsumerGroupDescribeService { storage };

    let group_id = "abc";

    let response = service
        .serve(RequestInput {
            request: ConsumerGroupDescribeRequest::default()
                .group_ids(Some([group_id.into()].into()))
                .include_authorized_operations(false),
            extensions: Extensions::default(),
        })
        .await
        .inspect(|response| debug!(?response))?;

    let groups = response.groups.unwrap_or_default();
    assert_eq!(1, groups.len());
    assert_eq!(
        ErrorCode::GroupIdNotFound,
        ErrorCode::try_from(groups[0].error_code)?
    );
    assert_eq!(group_id, groups[0].group_id.as_str());

    Ok(())
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use nisshi_storage::ArcDynStorage;

    use crate::common::{StorageType, init_tracing};
    use rand::{prelude::*, rng};
    use uuid::Uuid;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
        advertised_listener: Url,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::InMemory,
            cluster,
            node,
            advertised_listener,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn describe_non_existent_group() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = Uuid::now_v7();
        let node = rng().random_range(0..i32::MAX);
        let advertised_listener = Url::parse("tcp://example.com:9092/")?;

        super::describe_non_existent_group(
            cluster,
            node,
            advertised_listener.clone(),
            storage_container(cluster, node, advertised_listener).await?,
        )
        .await
    }

    async fn storage() -> Result<ArcDynStorage> {
        common::memory_storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await
    }

    #[tokio::test]
    async fn stored_group_answers_group_id_not_found() -> Result<()> {
        let _guard = init_tracing()?;
        super::stored_group_answers_group_id_not_found(storage().await?).await
    }

    #[tokio::test]
    async fn error_row_omits_authorized_operations() -> Result<()> {
        let _guard = init_tracing()?;
        super::error_row_omits_authorized_operations(storage().await?).await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use nisshi_storage::ArcDynStorage;
    use rand::{prelude::*, rng};
    use uuid::Uuid;

    use crate::common::init_tracing;

    use super::*;

    async fn storage() -> Result<ArcDynStorage> {
        common::lite_storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await
    }

    #[tokio::test]
    async fn stored_group_answers_group_id_not_found() -> Result<()> {
        let _guard = init_tracing()?;
        super::stored_group_answers_group_id_not_found(storage().await?).await
    }

    #[tokio::test]
    async fn error_row_omits_authorized_operations() -> Result<()> {
        let _guard = init_tracing()?;
        super::error_row_omits_authorized_operations(storage().await?).await
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use nisshi_storage::ArcDynStorage;
    use rand::{prelude::*, rng};
    use uuid::Uuid;

    use crate::common::init_tracing;

    use super::*;

    async fn storage() -> Result<ArcDynStorage> {
        common::postgres_storage(Uuid::now_v7(), rng().random_range(0..i32::MAX)).await
    }

    #[tokio::test]
    async fn stored_group_answers_group_id_not_found() -> Result<()> {
        let _guard = init_tracing()?;
        super::stored_group_answers_group_id_not_found(storage().await?).await
    }

    #[tokio::test]
    async fn error_row_omits_authorized_operations() -> Result<()> {
        let _guard = init_tracing()?;
        super::error_row_omits_authorized_operations(storage().await?).await
    }
}
