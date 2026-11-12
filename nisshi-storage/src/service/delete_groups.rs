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

use nisshi_sans_io::{ApiKey, DeleteGroupsRequest, DeleteGroupsResponse, RequestInput};
use rama::Service;
use tracing::instrument;

use crate::{Error, Result, Storage};

/// A [`Service`] using its [`Storage`] taking [`DeleteGroupsRequest`] returning [`DeleteGroupsResponse`].
/// ```no_run
/// use rama::Service as _;
/// use nisshi_sans_io::{DeleteGroupsRequest, ErrorCode};
/// use nisshi_storage::{DeleteGroupsService, Error, StorageContainer};
/// use url::Url;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Error> {
/// let storage = StorageContainer::builder()
///     .cluster_id("nisshi")
///     .node_id(111)
///     .advertised_listener(Url::parse("tcp://localhost:9092")?)
///     .storage(Url::parse("memory://nisshi/")?)
///     .build()
///     .await?;
///
/// let service = DeleteGroupsService { storage };
///
/// let group_id = "abcba";
///
/// let response = service
///     .serve(
///         DeleteGroupsRequest::default().groups_names(Some([group_id.into()].into())),
///     )
///     .await?;
///
/// let results = response.results.unwrap_or_default();
/// assert_eq!(1, results.len());
/// assert_eq!(group_id, results[0].group_id.as_str());
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct DeleteGroupsService<G> {
    pub storage: G,
}

impl<G> ApiKey for DeleteGroupsService<G> {
    const KEY: i16 = DeleteGroupsRequest::KEY;
}

impl<G, I> Service<I> for DeleteGroupsService<G>
where
    G: Storage,
    I: Into<RequestInput<DeleteGroupsRequest>> + Send + 'static,
{
    type Output = DeleteGroupsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        self.storage
            .delete_groups(input.request.groups_names.as_deref())
            .await
            .map(Some)
            .map(|results| {
                DeleteGroupsResponse::default()
                    .throttle_time_ms(0)
                    .results(results)
            })
    }
}
