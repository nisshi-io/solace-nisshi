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

use nisshi_sans_io::{
    ApiKey, ConsumerGroupDescribeRequest, RequestInput,
    consumer_group_describe_response::{ConsumerGroupDescribeResponse, DescribedGroup},
};
use rama::Service;
use tracing::{debug, instrument};

use crate::{Error, Result, Storage, authorized_operations};

/// A [`Service`] using its [`Storage`] taking [`ConsumerGroupDescribeRequest`] returning [`ConsumerGroupDescribeResponse`].
/// ```no_run
/// use rama::Service as _;
/// use nisshi_sans_io::{ConsumerGroupDescribeRequest, ErrorCode};
/// use nisshi_storage::{ConsumerGroupDescribeService, Error, StorageContainer};
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
/// let service = ConsumerGroupDescribeService { storage };
///
/// let group_id = "abc";
///
/// let response = service
///     .serve(
///         ConsumerGroupDescribeRequest::default()
///             .group_ids(Some([group_id.into()].into()))
///             .include_authorized_operations(false),
///     )
///     .await?;
///
/// let groups = response.groups.unwrap_or_default();
/// assert_eq!(1, groups.len());
/// assert_eq!(ErrorCode::GroupIdNotFound, ErrorCode::try_from(groups[0].error_code)?);
/// assert_eq!(group_id, groups[0].group_id.as_str());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct ConsumerGroupDescribeService<G> {
    pub storage: G,
}

impl<G> ApiKey for ConsumerGroupDescribeService<G> {
    const KEY: i16 = ConsumerGroupDescribeRequest::KEY;
}

impl<G, I> Service<I> for ConsumerGroupDescribeService<G>
where
    G: Storage,
    I: Into<RequestInput<ConsumerGroupDescribeRequest>> + Send + 'static,
{
    type Output = ConsumerGroupDescribeResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();
        let include_authorized_operations = input.request.include_authorized_operations;

        self.storage
            .describe_groups(
                input.request.group_ids.as_deref(),
                include_authorized_operations,
            )
            .await
            .inspect(|described| debug!(?described))
            .map(|described| {
                described
                    .iter()
                    .map(DescribedGroup::from)
                    .map(|group| {
                        let operations = authorized_operations::group(
                            include_authorized_operations,
                            group.error_code,
                        );
                        group.authorized_operations(operations)
                    })
                    .collect::<Vec<_>>()
            })
            .map(Some)
            .map(|groups| {
                ConsumerGroupDescribeResponse::default()
                    .throttle_time_ms(0)
                    .groups(groups)
            })
    }
}
