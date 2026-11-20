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

use std::sync::Arc;

use nisshi_schema::Registry;
use nisshi_service::{
    BytesFrameLayer, BytesFrameService, DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE,
    FrameRouteBuilder, FrameRouteService, TcpBytesLayer, TcpBytesService, TcpContext,
    TcpContextLayer, TcpContextService,
};
use nisshi_storage::{AutoCreateRefusals, KnownTopics, Storage, TopicCreationConfig};
use rama::Layer;
use rsasl::config::SASLConfig;
use tracing::debug;

use crate::{Error, Result, coordinator::group::Coordinator};

use self::extend_routes::ExtendRoutes;

pub mod auth;
pub mod coordinator;
pub mod extend_routes;
pub mod storage;
pub mod txn_offset_commit;

pub(crate) type TcpRouteFrame =
    TcpContextService<TcpBytesService<BytesFrameService<FrameRouteService<Error>>>>;

/// Returns the broker's routes, changed by `extend_routes`, before `build` adds `ApiVersions`.
pub fn routes<C, S>(
    coordinator: C,
    storage: S,
    schemas: Option<Registry>,
    topic_creation: TopicCreationConfig,
    auto_create_refusals: AutoCreateRefusals,
    known_topics: KnownTopics,
    extend_routes: Option<&ExtendRoutes<C, S>>,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage + Clone,
    C: Coordinator,
{
    let extend_storage = storage.clone();

    storage::services(
        FrameRouteService::<Error>::builder(),
        storage,
        schemas,
        topic_creation,
        auto_create_refusals,
        known_topics,
    )
    .inspect(|builder| debug!(?builder))
    .and_then(|builder| {
        coordinator::services(builder, coordinator.clone()).inspect(|builder| debug!(?builder))
    })
    .and_then(|builder| txn_offset_commit::fence_txn_offset_commit(builder, coordinator.clone()))
    .and_then(auth::services)
    .and_then(|builder| match extend_routes {
        Some(extend_routes) => extend_routes.apply(builder, &coordinator, &extend_storage),
        None => Ok(builder),
    })
}

#[allow(clippy::too_many_arguments)]
pub fn services<C, S>(
    cluster_id: &str,
    coordinator: C,
    storage: S,
    schemas: Option<Registry>,
    topic_creation: TopicCreationConfig,
    auto_create_refusals: AutoCreateRefusals,
    known_topics: KnownTopics,
    extend_routes: Option<&ExtendRoutes<C, S>>,
    sasl_config: Option<Arc<SASLConfig>>,
) -> Result<TcpRouteFrame, Error>
where
    S: Storage + Clone,
    C: Coordinator,
{
    routes(
        coordinator,
        storage,
        schemas,
        topic_creation,
        auto_create_refusals,
        known_topics,
        extend_routes,
    )
    .and_then(|builder| builder.build().map_err(Into::into))
    .map(|route| {
        let pre_authentication_maximum_frame_size = sasl_config
            .is_some()
            .then_some(DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE);

        (
            TcpContextLayer::new(
                TcpContext::default()
                    .cluster_id(Some(cluster_id.into()))
                    .pre_authentication_maximum_frame_size(pre_authentication_maximum_frame_size),
            ),
            TcpBytesLayer,
            BytesFrameLayer::default().with_sasl_config(sasl_config),
        )
            .into_layer(route)
    })
}
