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

use nisshi_sans_io::{ApiKey, DescribeAclsRequest, DescribeAclsResponse, ErrorCode, RequestInput};
use rama::Service;
use tracing::instrument;

use crate::{Error, Storage};

#[derive(Clone, Debug)]
pub struct DescribeAclsService<G> {
    pub storage: G,
}

impl<G> ApiKey for DescribeAclsService<G> {
    const KEY: i16 = DescribeAclsRequest::KEY;
}

impl<G, I> Service<I> for DescribeAclsService<G>
where
    G: Storage,
    I: Into<RequestInput<DescribeAclsRequest>> + Send + 'static,
{
    type Output = DescribeAclsResponse;
    type Error = Error;

    // The broker does not store ACLs, so it refuses the request with `SecurityDisabled` and an
    // empty list of resources. Apache Kafka gives the same answer if it does not have an
    // authorizer. An empty list with no error would tell an operator that ACLs are supported and
    // that none exist.
    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let _ = input;
        Ok(DescribeAclsResponse::default()
            .error_code(i16::from(ErrorCode::SecurityDisabled))
            .error_message(Some("No Authorizer is configured on the broker".into()))
            .resources(Some([].into())))
    }
}
