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
    ApiKey, ErrorCode, ListGroupsRequest, ListGroupsResponse, RequestInput,
    list_groups_response::ListedGroup,
};
use rama::Service;
use tracing::{error, instrument};

use crate::{ConsumerGroupState, Error, NamedGroupDetail, Result, Storage};

/// A [`Service`] using its [`Storage`] taking [`ListGroupsRequest`] returning [`ListGroupsResponse`].
/// ```no_run
/// use rama::Service;
/// use nisshi_sans_io::{ErrorCode, ListGroupsRequest};
/// use nisshi_storage::{Error, ListGroupsService, StorageContainer};
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
/// let service = ListGroupsService { storage };
///
/// let response = service
///     .serve(
///         ListGroupsRequest::default().states_filter(Some(["Empty".into()].into())),
///     )
///     .await?;
///
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(response.error_code)?);
/// assert_eq!(Some([].into()), response.groups);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct ListGroupsService<G> {
    pub storage: G,
}

impl<G> ApiKey for ListGroupsService<G> {
    const KEY: i16 = ListGroupsRequest::KEY;
}

impl<G, I> Service<I> for ListGroupsService<G>
where
    G: Storage,
    I: Into<RequestInput<ListGroupsRequest>> + Send + 'static,
{
    type Output = ListGroupsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        Ok(list_groups_response(
            self.storage.list_groups().await,
            input.request.states_filter.as_deref(),
        ))
    }
}

/// Returns the `ListGroups` response that lists each group in `listed` whose state matches
/// `states_filter`.
///
/// A storage error is answered with an error code instead of being returned, because the
/// connection closes on a returned error, and the client gets no answer.
pub fn list_groups_response(
    listed: Result<Vec<NamedGroupDetail>>,
    states_filter: Option<&[String]>,
) -> ListGroupsResponse {
    match listed {
        Ok(groups) => {
            let groups = groups
                .iter()
                .filter(|group| ConsumerGroupState::from(*group).matches(states_filter))
                .map(ListedGroup::from)
                .collect::<Vec<_>>();

            ListGroupsResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(ErrorCode::None.into())
                .groups(Some(groups))
        }

        Err(err) => {
            error!(?err, "the groups could not be listed");

            ListGroupsResponse::default()
                .throttle_time_ms(Some(0))
                .error_code(err.error_code(ErrorCode::CoordinatorNotAvailable).into())
                .groups(Some([].into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error_code_of(error: Error) -> Result<ErrorCode> {
        let response = list_groups_response(Err(error), None);

        assert_eq!(Some(vec![]), response.groups);

        Ok(ErrorCode::try_from(response.error_code)?)
    }

    /// A client retries `COORDINATOR_NOT_AVAILABLE`, so it answers a failure that may clear.
    #[test]
    fn transient_storage_error_answers_coordinator_not_available() -> Result<()> {
        assert_eq!(
            ErrorCode::CoordinatorNotAvailable,
            error_code_of(Error::from(std::io::Error::other("connection reset")))?
        );

        Ok(())
    }

    /// A client reports `UNKNOWN_SERVER_ERROR` at once. `COORDINATOR_NOT_AVAILABLE` would keep it
    /// retrying a failure that a retry can't fix, until its API timeout.
    #[test]
    fn permanent_storage_error_answers_unknown_server_error() -> Result<()> {
        assert_eq!(
            ErrorCode::UnknownServerError,
            error_code_of(Error::Message("cannot parse".into()))?
        );

        Ok(())
    }

    #[test]
    fn storage_error_with_a_code_keeps_that_code() -> Result<()> {
        assert_eq!(
            ErrorCode::GroupAuthorizationFailed,
            error_code_of(Error::Api(ErrorCode::GroupAuthorizationFailed))?
        );

        Ok(())
    }
}
