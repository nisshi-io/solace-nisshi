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

#![allow(dead_code)]

pub(crate) mod wire;

use bytes::Bytes;
use glob::glob;
use nisshi_broker::{
    Error, Result,
    coordinator::group::{Coordinator, administrator::Controller},
    service::coordinator::services,
};
use nisshi_sans_io::{
    Body, ErrorCode, HeartbeatResponse, JoinGroupResponse, LeaveGroupResponse, MetadataResponse,
    OffsetFetchResponse, SyncGroupResponse,
    join_group_request::JoinGroupRequestProtocol,
    join_group_response::JoinGroupResponseMember,
    leave_group_request::MemberIdentity,
    metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
    offset_fetch_request::OffsetFetchRequestTopic,
    sync_group_request::SyncGroupRequestAssignment,
};
use nisshi_schema::Registry;
use nisshi_service::{BytesFrameLayer, ConsumerGroupLayer, FrameBytesLayer, FrameRouteService};
use nisshi_storage::{ArcDynStorage, BrokerRegistrationRequest, Storage, StorageContainer};
use rama::{Layer as _, Service};
use rand::{
    distr::{Alphanumeric, StandardUniform},
    prelude::*,
    rng,
};
use std::{env, io::ErrorKind, sync::Arc, thread};
use tokio::fs::remove_file;
use tracing::{debug, subscriber::DefaultGuard};
use tracing_subscriber::EnvFilter;
use url::Url;
use uuid::Uuid;

pub(crate) fn init_tracing() -> Result<DefaultGuard> {
    use std::{fs::File, sync::Arc};

    Ok(tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
            )
            .with_writer(
                thread::current()
                    .name()
                    .ok_or(Error::Message(String::from("unnamed thread")))
                    .and_then(|name| {
                        File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME")))
                            .map_err(Into::into)
                    })
                    .map(Arc::new)?,
            )
            .finish(),
    ))
}

/// Returns the URL of the PostgreSQL database that [`StorageType::Postgres`]
/// stores to: `NISSHI_TEST_DATABASE_URL`, or else the `db` service of
/// compose.yaml on its default port.
pub(crate) fn postgres_url() -> String {
    env::var("NISSHI_TEST_DATABASE_URL")
        .unwrap_or_else(|_| String::from("postgres://postgres:postgres@localhost"))
}

pub(crate) enum StorageType {
    InMemory,
    Lite,
    Postgres,
    SlateDb,
    Turso,
}

pub(crate) async fn storage_container(
    storage_type: StorageType,
    cluster: impl Into<String> + Clone,
    node: i32,
    advertised_listener: Url,
    schemas: Option<Registry>,
) -> Result<ArcDynStorage> {
    let builder = {
        let mut builder = StorageContainer::builder();

        #[cfg(feature = "dynostore")]
        builder.with_factory(Arc::new(nisshi_storage_dynostore::MemoryEngineFactory));

        #[cfg(feature = "dynostore")]
        builder.with_factory(Arc::new(
            nisshi_storage_dynostore::S3OptimisticConcurrencyEngineFactory,
        ));

        #[cfg(feature = "dynostore")]
        builder.with_factory(Arc::new(
            nisshi_storage_dynostore::GoogleCloudStorageEngineFactory,
        ));

        #[cfg(feature = "libsql")]
        builder.with_factory(Arc::new(nisshi_storage_sql::LiteEngineFactory));

        #[cfg(feature = "postgres")]
        builder.with_factory(Arc::new(nisshi_storage_sql::PostgresEngineFactory));

        #[cfg(feature = "slatedb")]
        builder.with_factory(Arc::new(nisshi_storage_slatedb::EngineFactory));

        #[cfg(feature = "turso")]
        builder.with_factory(Arc::new(nisshi_storage_sql::LimboEngineFactory));

        builder
    };

    let storage = match storage_type {
        StorageType::Postgres => {
            builder
                .cluster_id(cluster.clone())
                .node_id(node)
                .advertised_listener(advertised_listener)
                .schema_registry(schemas)
                .storage(Url::parse(&postgres_url())?)
                .build()
                .await
        }

        StorageType::InMemory => {
            builder
                .cluster_id(cluster.clone())
                .node_id(node)
                .advertised_listener(advertised_listener)
                .schema_registry(schemas)
                .storage(Url::parse("memory://")?)
                .build()
                .await
        }

        StorageType::Lite => {
            builder
                .cluster_id(cluster.clone())
                .node_id(node)
                .advertised_listener(advertised_listener)
                .schema_registry(schemas)
                .storage(lite_url().await?)
                .build()
                .await
        }

        StorageType::Turso => {
            let relative = thread::current()
                .name()
                .ok_or(Error::Message(String::from("unnamed thread")))
                .map(|name| format!("../logs/{}/{name}.db*", env!("CARGO_PKG_NAME")))?;

            let mut path = env::current_dir()?;
            path.push(relative);
            debug!(?path);

            if let Some(pattern) = path.to_str() {
                debug!(pattern);

                let paths = glob(pattern)?;

                for path in paths.flatten() {
                    debug!(?path);

                    remove_file(path).await?;
                }
            }

            builder
                .cluster_id(cluster.clone())
                .node_id(node)
                .advertised_listener(advertised_listener)
                .schema_registry(schemas)
                .storage(
                    thread::current()
                        .name()
                        .ok_or(Error::Message(String::from("unnamed thread")))
                        .map(|name| format!("turso://../logs/{}/{name}.db", env!("CARGO_PKG_NAME")))
                        .inspect(|url| debug!(url))
                        .and_then(|url| Url::parse(&url).map_err(Into::into))?,
                )
                .build()
                .await
        }

        // Uses slatedb://memory for in-memory testing, no external S3 needed
        StorageType::SlateDb => {
            builder
                .cluster_id(cluster.clone())
                .node_id(node)
                .advertised_listener(advertised_listener)
                .schema_registry(schemas)
                .storage(Url::parse("slatedb://memory")?)
                .build()
                .await
        }
    }?;

    register_broker(cluster, node, &storage).await?;

    Ok(storage)
}

/// A `sqlite://` URL for this test thread's own database file, which is removed first, so
/// that each test starts with an empty database.
pub(crate) async fn lite_url() -> Result<Url> {
    let name = thread::current()
        .name()
        .map(String::from)
        .ok_or(Error::Message(String::from("unnamed thread")))?;

    let mut path = env::current_dir()?;
    path.push(format!("../logs/{}/{name}.db", env!("CARGO_PKG_NAME")));
    debug!(?path);

    match remove_file(path).await {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        otherwise @ Err(_) => otherwise,
    }?;

    let url = format!("sqlite://../logs/{}/{name}.db", env!("CARGO_PKG_NAME"));
    debug!(url);
    Url::parse(&url).map_err(Into::into)
}

pub(crate) async fn memory_storage(
    cluster: impl Into<String> + Clone,
    node: i32,
) -> Result<ArcDynStorage> {
    storage_container(
        StorageType::InMemory,
        cluster,
        node,
        Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await
}

pub(crate) async fn lite_storage(
    cluster: impl Into<String> + Clone,
    node: i32,
) -> Result<ArcDynStorage> {
    storage_container(
        StorageType::Lite,
        cluster,
        node,
        Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await
}

pub(crate) async fn slate_storage(
    cluster: impl Into<String> + Clone,
    node: i32,
) -> Result<ArcDynStorage> {
    storage_container(
        StorageType::SlateDb,
        cluster,
        node,
        Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await
}

pub(crate) async fn postgres_storage(
    cluster: impl Into<String> + Clone,
    node: i32,
) -> Result<ArcDynStorage> {
    storage_container(
        StorageType::Postgres,
        cluster,
        node,
        Url::parse("tcp://127.0.0.1/")?,
        None,
    )
    .await
}

pub(crate) fn alphanumeric_string(length: usize) -> String {
    rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

pub(crate) fn random_bytes(length: usize) -> Bytes {
    rng()
        .sample_iter(StandardUniform)
        .take(length)
        .collect::<Vec<u8>>()
        .into()
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn join_group<G>(
    controller: &mut Controller<G>,
    client_id: Option<&str>,
    group_id: &str,
    session_timeout_ms: i32,
    rebalance_timeout_ms: Option<i32>,
    member_id: &str,
    group_instance_id: Option<&str>,
    protocol_type: &str,
    protocols: Option<&[JoinGroupRequestProtocol]>,
    reason: Option<&str>,
) -> Result<JoinGroupResponse>
where
    G: Storage + Clone,
{
    controller
        .join(
            JOIN_GROUP_VERSION,
            client_id,
            None,
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            group_instance_id,
            protocol_type,
            protocols,
            reason,
        )
        .await
        .and_then(|body| TryInto::try_into(body).map_err(Into::into))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn sync_group<G>(
    controller: &mut Controller<G>,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    group_instance_id: Option<&str>,
    protocol_type: &str,
    protocol_name: &str,
    assignments: &[SyncGroupRequestAssignment],
) -> Result<SyncGroupResponse>
where
    G: Storage + Clone,
{
    controller
        .sync(
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            Some(protocol_type),
            Some(protocol_name),
            Some(assignments),
        )
        .await
        .and_then(|body| TryInto::try_into(body).map_err(Into::into))
}

pub(crate) async fn heartbeat<G>(
    controller: &mut Controller<G>,
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    group_instance_id: Option<&str>,
) -> Result<HeartbeatResponse>
where
    G: Storage + Clone,
{
    controller
        .heartbeat(group_id, generation_id, member_id, group_instance_id)
        .await
        .and_then(|body| TryInto::try_into(body).map_err(Into::into))
}

pub(crate) async fn offset_fetch<G>(
    controller: &mut Controller<G>,
    group_id: &str,
    topics: &[OffsetFetchRequestTopic],
) -> Result<OffsetFetchResponse>
where
    G: Storage + Clone,
{
    controller
        .offset_fetch(Some(group_id), Some(topics), None, Some(false))
        .await
        .and_then(|body| TryInto::try_into(body).map_err(Into::into))
}

pub(crate) async fn leave<G>(
    controller: &mut Controller<G>,
    group_id: &str,
    member_id: &str,
    group_instance_id: Option<&str>,
) -> Result<LeaveGroupResponse>
where
    G: Storage + Clone,
{
    controller
        .leave(
            group_id,
            None,
            Some(&[MemberIdentity::default()
                .member_id(member_id.into())
                .group_instance_id(group_instance_id.map(|s| s.to_owned()))
                .reason(Some("the consumer is being closed".into()))]),
        )
        .await
        .and_then(|body| TryInto::try_into(body).map_err(Into::into))
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum JoinResponse {
    Leader {
        id: String,
        generation: i32,
        members: Vec<JoinGroupResponseMember>,
        protocols: Vec<JoinGroupRequestProtocol>,
    },
    Follower {
        leader: String,
        id: String,
        generation: i32,
        protocols: Vec<JoinGroupRequestProtocol>,
    },
}

impl JoinResponse {
    #[allow(dead_code)]
    pub(crate) fn leader(&self) -> &str {
        match self {
            Self::Leader { id: leader, .. } | Self::Follower { leader, .. } => leader.as_str(),
        }
    }

    pub(crate) fn id(&self) -> &str {
        match self {
            Self::Leader { id, .. } | Self::Follower { id, .. } => id.as_str(),
        }
    }

    pub(crate) fn generation(&self) -> i32 {
        match self {
            Self::Leader { generation, .. } | Self::Follower { generation, .. } => *generation,
        }
    }

    pub(crate) fn is_leader(&self) -> bool {
        matches!(self, Self::Leader { .. })
    }

    pub(crate) fn protocols(&self) -> &[JoinGroupRequestProtocol] {
        match self {
            Self::Leader { protocols, .. } | Self::Follower { protocols, .. } => &protocols[..],
        }
    }
}

/// The newest `JoinGroup` version, which the test helpers send.
pub(crate) const JOIN_GROUP_VERSION: i16 = 9;

pub(crate) const CLIENT_ID: &str = "console-consumer";
pub(crate) const RANGE: &str = "range";
pub(crate) const COOPERATIVE_STICKY: &str = "cooperative-sticky";
pub(crate) const PROTOCOL_TYPE: &str = "consumer";

#[allow(clippy::too_many_arguments)]
pub(crate) async fn join<G>(
    controller: &mut Controller<G>,
    group_id: &str,
    member_id: Option<&str>,
    group_instance_id: Option<&str>,
    protocols: Option<Vec<JoinGroupRequestProtocol>>,
    session_timeout_ms: i32,
    rebalance_timeout_ms: Option<i32>,
) -> Result<JoinResponse>
where
    G: Storage + Clone,
{
    let reason = None;

    let protocols = protocols.unwrap_or_else(|| {
        [
            JoinGroupRequestProtocol::default()
                .name(RANGE.into())
                .metadata(random_bytes(15)),
            JoinGroupRequestProtocol::default()
                .name(COOPERATIVE_STICKY.into())
                .metadata(random_bytes(15)),
        ]
        .into()
    });

    let join_response = join_group(
        controller,
        Some(CLIENT_ID),
        group_id,
        session_timeout_ms,
        rebalance_timeout_ms,
        member_id.unwrap_or_default(),
        group_instance_id,
        PROTOCOL_TYPE,
        Some(&protocols[..]),
        reason,
    )
    .await?;

    if member_id.is_none() && group_instance_id.is_none() {
        // join rejected as member id is required for a dynamic group
        //
        assert_eq!(
            ErrorCode::MemberIdRequired,
            ErrorCode::try_from(join_response.error_code)?
        );
        assert_eq!(Some("".into()), join_response.protocol_name);
        assert!(join_response.leader.is_empty());
        assert!(join_response.member_id.starts_with(CLIENT_ID));
        assert_eq!(0, join_response.members.unwrap_or_default().len());

        Box::pin(join(
            controller,
            group_id,
            Some(join_response.member_id.as_str()),
            group_instance_id,
            Some(protocols),
            session_timeout_ms,
            rebalance_timeout_ms,
        ))
        .await
    } else if join_response.member_id == join_response.leader {
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(join_response.error_code)?
        );
        assert_eq!(Some(PROTOCOL_TYPE.into()), join_response.protocol_type);
        assert_eq!(Some(RANGE.into()), join_response.protocol_name);

        let id = join_response.leader;
        let generation = join_response.generation_id;
        let members = join_response.members.unwrap_or_default();

        Ok(JoinResponse::Leader {
            id,
            generation,
            members,
            protocols,
        })
    } else {
        assert_eq!(
            ErrorCode::None,
            ErrorCode::try_from(join_response.error_code)?
        );
        assert_eq!(Some(PROTOCOL_TYPE.into()), join_response.protocol_type);
        assert_eq!(Some(RANGE.into()), join_response.protocol_name);
        assert_ne!(join_response.member_id, join_response.leader);
        assert_eq!(0, join_response.members.unwrap_or_default().len());

        let id = join_response.member_id;
        let leader = join_response.leader;
        let generation = join_response.generation_id;

        Ok(JoinResponse::Follower {
            leader,
            id,
            generation,
            protocols,
        })
    }
}

pub(crate) async fn register_broker(
    cluster_id: impl Into<String>,
    broker_id: i32,
    sc: &impl Storage,
) -> Result<()> {
    let incarnation_id = Uuid::now_v7();

    // debug!(?cluster_id, ?broker_id, ?incarnation_id);

    let broker_registration = BrokerRegistrationRequest {
        broker_id,
        cluster_id: cluster_id.into(),
        incarnation_id,
        rack: None,
    };

    sc.register_broker(broker_registration)
        .await
        .map_err(Into::into)
}

async fn serve<S>(service: &S, input: Option<Body>) -> anyhow::Result<Body>
where
    S: Service<Option<Body>, Output = Body>,
    S::Error: Into<anyhow::Error>,
{
    service.serve(input).await.map_err(Into::into)
}

/// A group with one member, formed through a coordinator: member id required,
/// join, sync.
pub(crate) struct Formed {
    pub group: String,
    pub generation_id: i32,
    pub member_id: String,
}

pub(crate) async fn form_group<S>(coordinator: &Controller<S>) -> anyhow::Result<Formed>
where
    S: Storage + Clone,
{
    use anyhow::anyhow;

    let group = alphanumeric_string(15);

    let metadata = MetadataResponse::default().topics(Some(vec![
        MetadataResponseTopic::default()
            .name(Some("t".into()))
            .partitions(Some(
                (0..3)
                    .map(|partition_index| {
                        MetadataResponsePartition::default().partition_index(partition_index)
                    })
                    .collect(),
            )),
    ]));

    let route = services(FrameRouteService::<Error>::builder(), coordinator.clone())
        .and_then(|builder| builder.build().map_err(Into::into))?;

    let consumer = (
        ConsumerGroupLayer::new(group.clone(), ["t"], metadata),
        FrameBytesLayer,
        BytesFrameLayer::default(),
    )
        .into_layer(route);

    let member_id_required = serve(&consumer, None).await?;
    let joined = serve(&consumer, Some(member_id_required)).await?;

    let Body::JoinGroupResponse(JoinGroupResponse {
        error_code,
        generation_id,
        ref member_id,
        ..
    }) = joined
    else {
        return Err(anyhow!("expecting join response: {joined:?}"));
    };
    assert_eq!(i16::from(ErrorCode::None), error_code);
    let member_id = member_id.clone();

    let synced = serve(&consumer, Some(joined)).await?;
    assert!(matches!(synced, Body::SyncGroupResponse(_)), "{synced:?}");

    Ok(Formed {
        group,
        generation_id,
        member_id,
    })
}
