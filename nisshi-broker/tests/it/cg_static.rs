// Copyright (c) Solace Corporation
//
// Contributors outside Solace:
//   Enwei Jiao
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

use crate::common::{
    self, CLIENT_ID, COOPERATIVE_STICKY, PROTOCOL_TYPE, RANGE, StorageType, register_broker,
};
use bytes::Bytes;
use nisshi_broker::{Result, coordinator::group::administrator::Controller};
use nisshi_sans_io::{ErrorCode, join_group_request::JoinGroupRequestProtocol};
use nisshi_storage::Storage;
use rand::{prelude::*, rng};
use tracing::debug;
use url::Url;
use uuid::Uuid;

pub async fn join_with_empty_member_id<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let mut controller = Controller::with_storage(sc)?;

    let session_timeout_ms = 45_000;
    let rebalance_timeout_ms = Some(300_000);
    let group_instance_id = common::alphanumeric_string(6);
    let reason = None;

    let group_id: String = common::alphanumeric_string(15);
    debug!(?group_id);

    const CLIENT_ID: &str = "console-consumer";
    const RANGE: &str = "range";
    const COOPERATIVE_STICKY: &str = "cooperative-sticky";

    const PROTOCOL_TYPE: &str = "consumer";

    let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
    let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

    let protocols = [
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(first_member_range_meta.clone()),
        JoinGroupRequestProtocol::default()
            .name(COOPERATIVE_STICKY.into())
            .metadata(first_member_sticky_meta),
    ];

    // join static group without a member id
    //
    let join_response = common::join_group(
        &mut controller,
        Some(CLIENT_ID),
        group_id.as_str(),
        session_timeout_ms,
        rebalance_timeout_ms,
        "",
        Some(group_instance_id.as_str()),
        PROTOCOL_TYPE,
        Some(&protocols[..]),
        reason,
    )
    .await?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(join_response.error_code)?
    );
    assert_eq!(Some(PROTOCOL_TYPE.into()), join_response.protocol_type);
    assert_eq!(Some(RANGE.into()), join_response.protocol_name);
    assert_eq!(join_response.member_id, join_response.leader);
    assert!(
        join_response
            .member_id
            .starts_with(group_instance_id.as_str())
    );
    assert_eq!(1, join_response.members.unwrap().len());

    Ok(())
}

pub async fn rejoin_with_empty_member_id<G>(
    cluster_id: impl Into<String>,
    broker_id: i32,
    sc: G,
) -> Result<()>
where
    G: Storage + Clone,
{
    register_broker(cluster_id, broker_id, &sc).await?;

    let mut controller = Controller::with_storage(sc.clone())?;

    let session_timeout_ms = 45_000;
    let rebalance_timeout_ms = Some(300_000);
    let group_instance_id = common::alphanumeric_string(6);
    let reason = None;

    let group_id: String = common::alphanumeric_string(15);
    debug!(?group_id);

    let first_member_range_meta = Bytes::from_static(b"first_member_range_meta_01");
    let first_member_sticky_meta = Bytes::from_static(b"first_member_sticky_meta_01");

    let protocols = [
        JoinGroupRequestProtocol::default()
            .name(RANGE.into())
            .metadata(first_member_range_meta.clone()),
        JoinGroupRequestProtocol::default()
            .name(COOPERATIVE_STICKY.into())
            .metadata(first_member_sticky_meta),
    ];

    // join static group without a member id
    //
    let join_response = common::join_group(
        &mut controller,
        Some(CLIENT_ID),
        group_id.as_str(),
        session_timeout_ms,
        rebalance_timeout_ms,
        "",
        Some(group_instance_id.as_str()),
        PROTOCOL_TYPE,
        Some(&protocols[..]),
        reason,
    )
    .await?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(join_response.error_code)?
    );
    assert_eq!(Some(PROTOCOL_TYPE.into()), join_response.protocol_type);
    assert_eq!(Some(RANGE.into()), join_response.protocol_name);
    assert_eq!(join_response.member_id, join_response.leader);
    assert!(
        join_response
            .member_id
            .starts_with(group_instance_id.as_str())
    );
    assert_eq!(1, join_response.members.unwrap().len());

    let rejoin_response = common::join_group(
        &mut controller,
        Some(CLIENT_ID),
        group_id.as_str(),
        session_timeout_ms,
        rebalance_timeout_ms,
        "",
        Some(group_instance_id.as_str()),
        PROTOCOL_TYPE,
        Some(&protocols[..]),
        reason,
    )
    .await?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(rejoin_response.error_code)?
    );
    assert_eq!(Some(PROTOCOL_TYPE.into()), rejoin_response.protocol_type);
    assert_eq!(Some(RANGE.into()), rejoin_response.protocol_name);
    // As in Kafka, the restarted instance gets a new member id and takes the place of the
    // earlier one. The group was waiting for the leader's assignment, which may name the old
    // member id, so it rebalances into the next generation.
    assert_ne!(rejoin_response.member_id, join_response.member_id);
    assert!(
        rejoin_response
            .member_id
            .starts_with(group_instance_id.as_str())
    );
    assert_eq!(rejoin_response.member_id, rejoin_response.leader);
    assert_eq!(
        join_response.generation_id + 1,
        rejoin_response.generation_id
    );
    assert_eq!(1, rejoin_response.members.unwrap().len());

    Ok(())
}

#[cfg(feature = "postgres")]
mod pg {
    use nisshi_storage::ArcDynStorage;

    use super::*;

    async fn storage_container(cluster: Uuid, node: i32) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::Postgres,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[ignore]
    #[tokio::test]
    async fn join_with_empty_member_id() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::join_with_empty_member_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[ignore]
    #[tokio::test]
    async fn rejoin_with_empty_member_id() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::rejoin_with_empty_member_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use nisshi_storage::ArcDynStorage;

    use super::*;

    async fn storage_container(cluster: Uuid, node: i32) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::InMemory,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
    }

    #[ignore]
    #[tokio::test]
    async fn join_with_empty_member_id() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::join_with_empty_member_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }

    #[ignore]
    #[tokio::test]
    async fn rejoin_with_empty_member_id() -> Result<()> {
        let _guard = common::init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        super::rejoin_with_empty_member_id(
            cluster_id,
            broker_id,
            storage_container(cluster_id, broker_id).await?,
        )
        .await
    }
}
