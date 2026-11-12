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
    ApiKey, DeleteAclsRequest, DeleteAclsResponse, ErrorCode, RequestInput,
    delete_acls_response::DeleteAclsFilterResult,
};
use rama::Service;
use tracing::instrument;

use crate::{Error, Storage};

#[derive(Clone, Debug)]
pub struct DeleteAclsService<G> {
    pub storage: G,
}

impl<G> ApiKey for DeleteAclsService<G> {
    const KEY: i16 = DeleteAclsRequest::KEY;
}

impl<G, I> Service<I> for DeleteAclsService<G>
where
    G: Storage,
    I: Into<RequestInput<DeleteAclsRequest>> + Send + 'static,
{
    type Output = DeleteAclsResponse;
    type Error = Error;

    // The broker does not store ACLs, so it answers each filter with `SecurityDisabled` and an
    // empty list of matching ACLs. Apache Kafka gives the same answer if it does not have an
    // authorizer. The protocol requires one result for each filter, in the order of the filters.
    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        Ok(DeleteAclsResponse::default()
            .throttle_time_ms(0)
            .filter_results(Some(
                input
                    .into()
                    .request
                    .filters
                    .unwrap_or_default()
                    .iter()
                    .map(|_| {
                        DeleteAclsFilterResult::default()
                            .error_code(ErrorCode::SecurityDisabled.into())
                            .error_message(Some("No Authorizer is configured.".into()))
                            .matching_acls(Some([].into()))
                    })
                    .collect(),
            )))
    }
}
