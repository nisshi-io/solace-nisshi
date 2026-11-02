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

use std::sync::Arc;

use nisshi_service::{
    BytesFrameLayer, BytesFrameService, DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE,
    FrameRouteBuilder, FrameRouteService, TcpBytesLayer, TcpBytesService, TcpContext,
    TcpContextLayer, TcpContextService,
};
use nisshi_storage::{AutoCreateRefusals, Storage, TopicCreationConfig};
use rama::Layer;
use rsasl::config::SASLConfig;
use tracing::debug;

use crate::{Error, Result, coordinator::group::Coordinator};

pub mod auth;
pub mod coordinator;
pub mod storage;
pub mod txn_offset_commit;

type TcpRouteFrame =
    TcpContextService<TcpBytesService<BytesFrameService<FrameRouteService<Error>>>>;

/// Returns the broker's routes, before `build` adds `ApiVersions`.
pub fn routes<C, S>(
    coordinator: C,
    storage: S,
    topic_creation: TopicCreationConfig,
    auto_create_refusals: AutoCreateRefusals,
) -> Result<FrameRouteBuilder<Error>, Error>
where
    S: Storage + Clone,
    C: Coordinator,
{
    storage::services(
        FrameRouteService::<Error>::builder(),
        storage,
        topic_creation,
        auto_create_refusals,
    )
    .inspect(|builder| debug!(?builder))
    .and_then(|builder| {
        coordinator::services(builder, coordinator.clone()).inspect(|builder| debug!(?builder))
    })
    .and_then(|builder| txn_offset_commit::fence_txn_offset_commit(builder, coordinator))
    .and_then(auth::services)
}

pub fn services<C, S>(
    cluster_id: &str,
    coordinator: C,
    storage: S,
    topic_creation: TopicCreationConfig,
    auto_create_refusals: AutoCreateRefusals,
    sasl_config: Option<Arc<SASLConfig>>,
) -> Result<TcpRouteFrame, Error>
where
    S: Storage + Clone,
    C: Coordinator,
{
    routes(coordinator, storage, topic_creation, auto_create_refusals)
        .and_then(|builder| builder.build().map_err(Into::into))
        .map(|route| {
            let pre_authentication_maximum_frame_size = sasl_config
                .is_some()
                .then_some(DEFAULT_PRE_AUTHENTICATION_MAXIMUM_FRAME_SIZE);

            (
                TcpContextLayer::new(
                    TcpContext::default()
                        .cluster_id(Some(cluster_id.into()))
                        .pre_authentication_maximum_frame_size(
                            pre_authentication_maximum_frame_size,
                        ),
                ),
                TcpBytesLayer,
                BytesFrameLayer::default().with_sasl_config(sasl_config),
            )
                .into_layer(route)
        })
}
