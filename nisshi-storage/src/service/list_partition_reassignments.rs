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

use crate::{Error, Result, Storage};
use nisshi_sans_io::{
    ApiKey, ErrorCode, ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse,
    RequestInput,
};
use rama::Service;
use tracing::instrument;

/// A [`Service`] taking [`ListPartitionReassignmentsRequest`] and answering that no partition
/// reassignment is in progress, for any request.
///
/// The answer has no error and an empty topics list, as Kafka answers when nothing is moving,
/// whether the request lists every topic or names some, existing or not. The [`Storage`] is
/// not read.
#[derive(Clone, Debug)]
pub struct ListPartitionReassignmentsService<G> {
    pub storage: G,
}

impl<G> ApiKey for ListPartitionReassignmentsService<G> {
    const KEY: i16 = ListPartitionReassignmentsRequest::KEY;
}

impl<G, I> Service<I> for ListPartitionReassignmentsService<G>
where
    G: Storage,
    I: Into<RequestInput<ListPartitionReassignmentsRequest>> + Send + 'static,
{
    type Output = ListPartitionReassignmentsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let _ = input;

        // The broker is a single node and never moves a partition, so nothing is ever being
        // reassigned. Deliberately narrow: answering from topic metadata would report every
        // partition as reassigning and add a storage failure mode to a constant answer.
        Ok(ListPartitionReassignmentsResponse::default()
            .throttle_time_ms(0)
            .error_code(ErrorCode::None.into())
            .error_message(None)
            .topics(Some(vec![])))
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use nisshi_sans_io::{
        ErrorCode, ListPartitionReassignmentsRequest, RequestInput,
        list_partition_reassignments_request::ListPartitionReassignmentsTopics,
    };
    use rama::{Service as _, extensions::Extensions};

    use super::ListPartitionReassignmentsService;
    use crate::{
        Result,
        service::{RequestChannelService, bounded_channel},
    };

    /// Storage whose channel is closed fails every call, so any answer proves storage
    /// was never read.
    #[tokio::test]
    async fn answers_without_reading_storage() -> Result<()> {
        let (tx, rx) = bounded_channel(1);
        drop(rx);

        let service = ListPartitionReassignmentsService {
            storage: RequestChannelService::new(tx),
        };

        let named = vec![
            ListPartitionReassignmentsTopics::default()
                .name("t".into())
                .partition_indexes(Some(vec![0, 1])),
        ];

        for topics in [None, Some(named), Some(vec![])] {
            let response = service
                .serve(RequestInput {
                    request: ListPartitionReassignmentsRequest::default().topics(topics.clone()),
                    extensions: Extensions::default(),
                })
                .await?;

            assert_eq!(
                ErrorCode::None,
                ErrorCode::try_from(response.error_code)?,
                "{topics:?}"
            );
            assert_eq!(None, response.error_message, "{topics:?}");
            assert_matches!(response.topics.as_deref(), Some([]), "{topics:?}");
        }

        Ok(())
    }
}
