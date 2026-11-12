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
    ApiKey, DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse, RequestInput,
};
use rama::Service;
use tracing::instrument;

use crate::{Error, Storage};

#[derive(Clone, Debug)]
pub struct DescribeUserScramCredentialsService<G> {
    pub storage: G,
}

impl<G> ApiKey for DescribeUserScramCredentialsService<G> {
    const KEY: i16 = DescribeUserScramCredentialsRequest::KEY;
}

impl<G, I> Service<I> for DescribeUserScramCredentialsService<G>
where
    G: Storage,
    I: Into<RequestInput<DescribeUserScramCredentialsRequest>> + Send + 'static,
{
    type Output = DescribeUserScramCredentialsResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let _ = input;

        Ok(DescribeUserScramCredentialsResponse::default())
    }
}
