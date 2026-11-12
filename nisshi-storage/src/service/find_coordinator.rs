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

use nisshi_sans_io::{
    ApiKey, ErrorCode, FindCoordinatorRequest, FindCoordinatorResponse, RequestInput,
    find_coordinator_response::Coordinator,
};
use rama::Service;
use tracing::instrument;

use crate::{Error, Result, Storage};

/// A [`Service`] using its [`Storage`] taking [`FindCoordinatorRequest`] returning [`FindCoordinatorResponse`].
/// ```no_run
/// use rama::Service;
/// use nisshi_sans_io::{ErrorCode, FindCoordinatorRequest};
/// use nisshi_storage::{Error, FindCoordinatorService, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// const HOST: &str = "localhost";
/// const PORT: i32 = 9092;
/// const NODE_ID: i32 = 111;
///
/// let storage = StorageContainer::builder()
///     .cluster_id("nisshi")
///     .node_id(NODE_ID)
///     .advertised_listener(Url::parse(&format!("tcp://{HOST}:{PORT}"))?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let service = FindCoordinatorService { storage };
///
/// let response = service
///     .serve(
///         FindCoordinatorRequest::default()
///             .key(Some("abcba".into()))
///             .key_type(Some(0))
///             .coordinator_keys(Some(["xyzyx".into()].into())),
///     )
///     .await?;
///
/// assert_eq!(
///     Some(ErrorCode::None.into()),
///     if let Some(error_code) = response.error_code {
///         ErrorCode::try_from(error_code).map(Some)?
///     } else {
///         None
///     }
/// );
///
/// assert_eq!(Some(NODE_ID), response.node_id);
/// assert_eq!(Some(HOST), response.host.as_deref());
/// assert_eq!(Some(PORT), response.port);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct FindCoordinatorService<G> {
    pub storage: G,
}

impl<G> ApiKey for FindCoordinatorService<G> {
    const KEY: i16 = FindCoordinatorRequest::KEY;
}

impl<G, I> Service<I> for FindCoordinatorService<G>
where
    G: Storage,
    I: Into<RequestInput<FindCoordinatorRequest>> + Send + 'static,
{
    type Output = FindCoordinatorResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        let node_id = self.storage.node().await?;

        let listener = self.storage.advertised_listener().await?;
        let host = listener.host_str().unwrap_or("localhost");
        let port = i32::from(listener.port().unwrap_or(9092));

        Ok(FindCoordinatorResponse::default()
            .throttle_time_ms(Some(0))
            .error_code(Some(ErrorCode::None.into()))
            .error_message(Some("NONE".into()))
            .node_id(Some(node_id))
            .host(Some(host.into()))
            .port(Some(port))
            .coordinators(input.request.coordinator_keys.map(|keys| {
                keys.iter()
                    .map(|key| {
                        Coordinator::default()
                            .key(key.to_string())
                            .node_id(node_id)
                            .host(host.into())
                            .port(port)
                            .error_code(ErrorCode::None.into())
                            .error_message(None)
                    })
                    .collect()
            })))
    }
}
