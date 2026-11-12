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

use nisshi_sans_io::{ApiKey, EndTxnRequest, EndTxnResponse, RequestInput};
use rama::Service;
use tracing::instrument;

use crate::{Error, Result, Storage};

#[derive(Clone, Debug)]
pub struct EndService<G> {
    pub storage: G,
}

impl<G> ApiKey for EndService<G> {
    const KEY: i16 = EndTxnRequest::KEY;
}

impl<G, I> Service<I> for EndService<G>
where
    G: Storage,
    I: Into<RequestInput<EndTxnRequest>> + Send + 'static,
{
    type Output = EndTxnResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();
        self.storage
            .txn_end(
                input.request.transactional_id.as_str(),
                input.request.producer_id,
                input.request.producer_epoch,
                input.request.committed,
            )
            .await
            .map(|error_code| EndTxnResponse::default().error_code(i16::from(error_code)))
    }
}
