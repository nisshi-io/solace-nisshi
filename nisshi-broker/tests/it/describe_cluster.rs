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

use crate::common::{self, register_broker};
use nisshi_broker::Result;
use nisshi_sans_io::{
    DescribeClusterRequest, DescribeClusterResponse, ErrorCode, RequestInput,
    describe_cluster_response::DescribeClusterBroker,
};
use nisshi_storage::{DescribeClusterService, Storage};
use rama::{Service, extensions::Extensions};
use tracing::debug;
use url::Url;
use uuid::Uuid;

pub async fn describe<C, G>(
    cluster_id: C,
    broker_id: i32,
    advertised_listener: Url,
    sc: G,
) -> Result<()>
where
    C: Into<String>,
    G: Storage + Clone,
{
    debug!(broker_id, %advertised_listener);
    register_broker(cluster_id, broker_id, &sc).await?;

    let include_cluster_authorized_operations = true;
    let endpoint_type = Some(6);

    let service = DescribeClusterService {
        storage: sc.clone(),
    };

    let response = service
        .serve(RequestInput {
            request: DescribeClusterRequest::default()
                .include_cluster_authorized_operations(include_cluster_authorized_operations)
                .endpoint_type(endpoint_type),
            extensions: Extensions::default(),
        })
        .await?;

    let host = advertised_listener.host_str().unwrap().to_string();
    let port = advertised_listener.port().unwrap() as i32;
    let rack = None;

    assert!(matches!(
        response,
        DescribeClusterResponse {
            throttle_time_ms: 0,
            error_code,
            error_message: None,
            controller_id: broker_id,
            brokers,
            cluster_authorized_operations: -2_147_483_648,
            ..
        } if error_code == i16::from(ErrorCode::None)
        && brokers == Some(vec![DescribeClusterBroker::default()
            .broker_id(broker_id)
            .host(host)
            .port(port)
            .rack(rack)
        ])
    ));

    Ok(())
}

#[cfg(feature = "postgres")]
mod pg {
    use crate::common::{StorageType, init_tracing};
    use nisshi_storage::ArcDynStorage;
    use rand::{prelude::*, rng};

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
        advertised_listener: Url,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::Postgres,
            cluster,
            node,
            advertised_listener,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn describe() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = Uuid::now_v7();
        let node = rng().random_range(0..i32::MAX);
        let advertised_listener = Url::parse("tcp://example.com:9092/")?;

        super::describe(
            cluster,
            node,
            advertised_listener.clone(),
            storage_container(cluster, node, advertised_listener).await?,
        )
        .await
    }
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use crate::common::{StorageType, init_tracing};
    use nisshi_storage::ArcDynStorage;
    use rand::{prelude::*, rng};

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
    async fn describe() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = Uuid::now_v7();
        let node = rng().random_range(0..i32::MAX);
        let advertised_listener = Url::parse("tcp://example.com:9092/")?;

        super::describe(
            cluster,
            node,
            advertised_listener.clone(),
            storage_container(cluster, node, advertised_listener).await?,
        )
        .await
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use crate::common::{StorageType, init_tracing};
    use nisshi_storage::ArcDynStorage;
    use rand::{prelude::*, rng};

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
        advertised_listener: Url,
    ) -> Result<ArcDynStorage> {
        common::storage_container(StorageType::Lite, cluster, node, advertised_listener, None).await
    }

    #[tokio::test]
    async fn describe() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = Uuid::now_v7();
        let node = rng().random_range(0..i32::MAX);
        let advertised_listener = Url::parse("tcp://example.com:9092/")?;

        super::describe(
            cluster,
            node,
            advertised_listener.clone(),
            storage_container(cluster, node, advertised_listener).await?,
        )
        .await
    }
}

#[cfg(feature = "turso")]
mod turso {
    use crate::common::{StorageType, init_tracing};
    use nisshi_storage::ArcDynStorage;
    use rand::{prelude::*, rng};

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
        advertised_listener: Url,
    ) -> Result<ArcDynStorage> {
        common::storage_container(StorageType::Turso, cluster, node, advertised_listener, None)
            .await
    }

    #[tokio::test]
    async fn describe() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = Uuid::now_v7();
        let node = rng().random_range(0..i32::MAX);
        let advertised_listener = Url::parse("tcp://example.com:9092/")?;

        super::describe(
            cluster,
            node,
            advertised_listener.clone(),
            storage_container(cluster, node, advertised_listener).await?,
        )
        .await
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use crate::common::{StorageType, init_tracing};
    use nisshi_storage::ArcDynStorage;
    use rand::{prelude::*, rng};

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
        advertised_listener: Url,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::SlateDb,
            cluster,
            node,
            advertised_listener,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn describe() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster = Uuid::now_v7();
        let node = rng().random_range(0..i32::MAX);
        let advertised_listener = Url::parse("tcp://example.com:9092/")?;

        super::describe(
            cluster,
            node,
            advertised_listener.clone(),
            storage_container(cluster, node, advertised_listener).await?,
        )
        .await
    }
}
