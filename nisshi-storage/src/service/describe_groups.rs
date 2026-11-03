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

use nisshi_sans_io::{
    ApiKey, DescribeGroupsRequest, DescribeGroupsResponse, RequestInput,
    describe_groups_response::DescribedGroup,
};
use rama::Service;
use tracing::{error, instrument};

use crate::{Error, NamedGroupDetail, Result, Storage, authorized_operations};

/// A [`Service`] using its [`Storage`] taking [`DescribeGroupsRequest`] returning [`DescribeGroupsResponse`].
/// ```no_run
/// use rama::Service;
/// use nisshi_sans_io::{DescribeGroupsRequest, ErrorCode};
/// use nisshi_storage::{DescribeGroupsService, Error, StorageContainer};
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
/// let service = DescribeGroupsService { storage };
///
/// let group_id = "abcba";
///
/// let response = service
///     .serve(
///         DescribeGroupsRequest::default()
///             .groups(Some([group_id.into()].into()))
///             .include_authorized_operations(Some(false)),
///     )
///     .await?;
///
/// let groups = response.groups.unwrap_or_default();
/// assert_eq!(1, groups.len());
/// assert_eq!(ErrorCode::None, ErrorCode::try_from(groups[0].error_code)?);
/// assert_eq!("Dead", groups[0].group_state);
/// assert_eq!(group_id, groups[0].group_id.as_str());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct DescribeGroupsService<G> {
    pub storage: G,
}

impl<G> ApiKey for DescribeGroupsService<G> {
    const KEY: i16 = DescribeGroupsRequest::KEY;
}

impl<G, I> Service<I> for DescribeGroupsService<G>
where
    G: Storage,
    I: Into<RequestInput<DescribeGroupsRequest>> + Send + 'static,
{
    type Output = DescribeGroupsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();
        let include_authorized_operations =
            input.request.include_authorized_operations.unwrap_or(false);

        let group_ids = input.request.groups.as_deref();

        Ok(describe_groups_response(
            self.storage
                .describe_groups(group_ids, include_authorized_operations)
                .await,
            group_ids,
            include_authorized_operations,
        ))
    }
}

/// Returns the `DescribeGroups` response that describes each group in `described`, with its
/// authorized operations when `include_authorized_operations` asks for them.
///
/// A storage error answers each group in `group_ids` with an error code instead of being
/// returned, as Kafka answers each group, because the connection closes on a returned error, and
/// the client gets no answer.
pub fn describe_groups_response(
    described: Result<Vec<NamedGroupDetail>>,
    group_ids: Option<&[String]>,
    include_authorized_operations: bool,
) -> DescribeGroupsResponse {
    let described = described.unwrap_or_else(|err| {
        error!(?err, "the groups could not be described");

        group_ids
            .unwrap_or_default()
            .iter()
            .map(|group_id| {
                NamedGroupDetail::error_code(group_id.clone(), err.group_request_error_code())
            })
            .collect()
    });

    let groups = described
        .iter()
        .map(DescribedGroup::from)
        .map(|group| {
            let operations =
                authorized_operations::group(include_authorized_operations, group.error_code);
            group.authorized_operations(Some(operations))
        })
        .collect::<Vec<_>>();

    DescribeGroupsResponse::default()
        .throttle_time_ms(Some(0))
        .groups(Some(groups))
}

#[cfg(test)]
mod tests {
    use nisshi_sans_io::ErrorCode;

    use super::*;

    #[test]
    fn storage_error_answers_each_group_with_coordinator_not_available() -> Result<()> {
        let group_ids = ["a".to_owned(), "b".to_owned()];

        let response = describe_groups_response(
            Err(Error::Message("connection refused".into())),
            Some(&group_ids),
            false,
        );

        let answered = response
            .groups
            .unwrap_or_default()
            .into_iter()
            .map(|group| Ok((group.group_id, ErrorCode::try_from(group.error_code)?)))
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            vec![
                ("a".to_owned(), ErrorCode::CoordinatorNotAvailable),
                ("b".to_owned(), ErrorCode::CoordinatorNotAvailable),
            ],
            answered
        );

        Ok(())
    }

    /// Kafka answers a group that does not exist with the error NONE and the state `Dead` on
    /// `DescribeGroups` v0 to v5, and an admin client reads the group as missing from that state.
    /// An error of `GROUP_ID_NOT_FOUND` fails the client as unexpected.
    #[test]
    fn group_that_does_not_exist_answers_none_and_dead() -> Result<()> {
        for include_authorized_operations in [false, true] {
            let response = describe_groups_response(
                Ok(vec![NamedGroupDetail::error_code(
                    "missing".into(),
                    ErrorCode::GroupIdNotFound,
                )]),
                Some(&["missing".to_owned()]),
                include_authorized_operations,
            );

            let groups = response.groups.unwrap_or_default();
            assert_eq!(1, groups.len());
            assert_eq!(ErrorCode::None, ErrorCode::try_from(groups[0].error_code)?);
            assert_eq!("Dead", groups[0].group_state);
            assert_eq!("missing", groups[0].group_id);
            assert_eq!(Some(vec![]), groups[0].members);
            assert_eq!(
                Some(authorized_operations::group(
                    include_authorized_operations,
                    ErrorCode::None.into()
                )),
                groups[0].authorized_operations
            );
        }

        Ok(())
    }

    /// A group whose stored detail can't be read keeps its error and the state `Unknown`.
    #[test]
    fn group_that_can_not_be_read_keeps_its_error() -> Result<()> {
        let response = describe_groups_response(
            Ok(vec![NamedGroupDetail::error_code(
                "unreadable".into(),
                ErrorCode::UnknownServerError,
            )]),
            Some(&["unreadable".to_owned()]),
            false,
        );

        let groups = response.groups.unwrap_or_default();
        assert_eq!(
            ErrorCode::UnknownServerError,
            ErrorCode::try_from(groups[0].error_code)?
        );
        assert_eq!("Unknown", groups[0].group_state);

        Ok(())
    }
}
