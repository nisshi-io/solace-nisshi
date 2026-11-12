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
    ApiKey, DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, RequestInput,
};
use rama::Service;
use tracing::instrument;

use crate::{Error, Result, Storage, TopicId};

/// A [`Service`] using its [`Storage`] taking [`DescribeTopicPartitionsRequest`] returning [`DescribeTopicPartitionsResponse`].
/// ```no_run
/// use rama::Service;
/// use nisshi_sans_io::{
///     DescribeTopicPartitionsRequest, ErrorCode,
///     describe_topic_partitions_request::TopicRequest,
/// };
/// use nisshi_storage::{DescribeTopicPartitionsService, Error, StorageContainer};
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
/// let service = DescribeTopicPartitionsService { storage };
///
/// let topic = "abcba";
///
/// let response = service
///     .serve(
///         DescribeTopicPartitionsRequest::default()
///             .topics(Some([TopicRequest::default().name(topic.into())].into())),
///     )
///     .await?;
///
/// let topics = response.topics.unwrap_or_default();
/// assert_eq!(1, topics.len());
/// assert_eq!(
///     ErrorCode::UnknownTopicOrPartition,
///     ErrorCode::try_from(topics[0].error_code)?
/// );
/// assert_eq!(Some(topic), topics[0].name.as_deref());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct DescribeTopicPartitionsService<G> {
    pub storage: G,
}

impl<G> ApiKey for DescribeTopicPartitionsService<G> {
    const KEY: i16 = DescribeTopicPartitionsRequest::KEY;
}

impl<G, I> Service<I> for DescribeTopicPartitionsService<G>
where
    G: Storage,
    I: Into<RequestInput<DescribeTopicPartitionsRequest>> + Send + 'static,
{
    type Output = DescribeTopicPartitionsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();
        self.storage
            .describe_topic_partitions(
                input
                    .request
                    .topics
                    .as_ref()
                    .map(|topics| topics.iter().map(TopicId::from).collect::<Vec<_>>())
                    .as_deref(),
                input.request.response_partition_limit,
                input.request.cursor.map(Into::into),
            )
            .await
            .map(|topics| {
                DescribeTopicPartitionsResponse::default()
                    .throttle_time_ms(0)
                    .topics(Some(topics))
                    .next_cursor(None)
            })
    }
}
