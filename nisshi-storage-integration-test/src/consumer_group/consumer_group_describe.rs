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

use crate::{broker, random};

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
pub async fn stored_group_answers_group_id_not_found(storage: impl Storage + Clone) -> Result<()> {
    let group_id = random::alphanumeric_string(15);
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

/// An unknown group answers `GROUP_ID_NOT_FOUND`, and its row omits the authorized operations.
pub async fn error_row_omits_authorized_operations(storage: impl Storage + Clone) -> Result<()> {
    for include_authorized_operations in [false, true] {
        let groups = describe(
            storage.clone(),
            &[random::alphanumeric_string(15)],
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
    storage: G,
) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    debug!(broker_id);
    broker::register_broker(cluster_id, broker_id, &storage).await?;

    let service = ConsumerGroupDescribeService { storage };

    let group_id = random::alphanumeric_string(15);

    let response = service
        .serve(RequestInput {
            request: ConsumerGroupDescribeRequest::default()
                .group_ids(Some([group_id.clone()].into()))
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
