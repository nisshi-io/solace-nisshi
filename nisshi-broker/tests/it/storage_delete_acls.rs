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

use nisshi_broker::Result;
use nisshi_sans_io::{
    DeleteAclsRequest, ErrorCode, RequestInput, delete_acls_request::DeleteAclsFilter,
};
use nisshi_storage::DeleteAclsService;
use rama::{Service, extensions::Extensions};
use rand::{prelude::*, rng};
use uuid::Uuid;

use crate::common::init_tracing;

/// The broker does not store ACLs, so it answers each filter with `SECURITY_DISABLED`, the message
/// `No Authorizer is configured.` and an empty list of matching ACLs, one result for each filter.
/// A client that deletes ACLs must get an error, not a result that says that no ACL matched.
#[tokio::test]
async fn every_filter_is_answered_security_disabled() -> Result<()> {
    let _guard = init_tracing()?;

    let service = DeleteAclsService {
        storage: crate::common::memory_storage(Uuid::now_v7(), rng().random_range(0..i32::MAX))
            .await?,
    };

    // A request with no filters gets no results, and a request with several gets one each.
    for filter_count in [0, 1, 3] {
        let response = service
            .serve(RequestInput {
                request: DeleteAclsRequest::default().filters(Some(
                    (0..filter_count)
                        .map(|_| DeleteAclsFilter::default())
                        .collect::<Vec<_>>(),
                )),
                extensions: Extensions::default(),
            })
            .await?;

        let results = response.filter_results.as_deref().unwrap_or_default();
        assert_eq!(filter_count, results.len());

        for result in results {
            assert_eq!(
                ErrorCode::SecurityDisabled,
                ErrorCode::try_from(result.error_code)?
            );
            assert_eq!(
                Some("No Authorizer is configured."),
                result.error_message.as_deref()
            );
            assert_eq!(Some(0), result.matching_acls.as_ref().map(Vec::len));
        }
    }

    Ok(())
}
