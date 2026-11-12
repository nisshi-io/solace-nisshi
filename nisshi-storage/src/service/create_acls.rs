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
    ApiKey, CreateAclsRequest, CreateAclsResponse, ErrorCode, RequestInput,
    create_acls_response::AclCreationResult,
};
use rama::Service;
use tracing::instrument;

use crate::{Error, Storage};

#[derive(Clone, Debug)]
pub struct CreateAclsService<G> {
    pub storage: G,
}

impl<G> ApiKey for CreateAclsService<G> {
    const KEY: i16 = CreateAclsRequest::KEY;
}

impl<G, I> Service<I> for CreateAclsService<G>
where
    G: Storage,
    I: Into<RequestInput<CreateAclsRequest>> + Send + 'static,
{
    type Output = CreateAclsResponse;
    type Error = Error;

    // ACLs have no backing storage yet, so every creation is reported as
    // `SecurityDisabled` (the same error a real broker returns when no
    // authorizer is configured), one result per requested creation, in
    // order, as the protocol requires.
    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        Ok(CreateAclsResponse::default()
            .throttle_time_ms(0)
            .results(Some(
                input
                    .into()
                    .request
                    .creations
                    .unwrap_or_default()
                    .iter()
                    .map(|_| {
                        AclCreationResult::default()
                            .error_code(ErrorCode::SecurityDisabled.into())
                            .error_message(Some("No Authorizer is configured.".into()))
                    })
                    .collect(),
            )))
    }
}
