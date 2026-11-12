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
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, ApiKey, ErrorCode, RequestInput,
};
use rama::Service;
use tracing::instrument;

use crate::{Error, Result, Storage, TxnAddPartitionsRequest, TxnAddPartitionsResponse};

/// A [`Service`] using its [`Storage`] taking [`AddPartitionsToTxnRequest`] returning [`AddPartitionsToTxnResponse`].
#[derive(Clone, Debug)]
pub struct AddPartitionService<G> {
    pub storage: G,
}

impl<G> ApiKey for AddPartitionService<G> {
    const KEY: i16 = AddPartitionsToTxnRequest::KEY;
}

impl<G, I> Service<I> for AddPartitionService<G>
where
    G: Storage,
    I: Into<RequestInput<AddPartitionsToTxnRequest>> + Send + 'static,
{
    type Output = AddPartitionsToTxnResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = TxnAddPartitionsRequest::try_from(input.into().request)?;

        match self.storage.txn_add_partitions(input).await? {
            TxnAddPartitionsResponse::VersionZeroToThree(results_by_topic_v_3_and_below) => {
                Ok(AddPartitionsToTxnResponse::default()
                    .throttle_time_ms(0)
                    .error_code(Some(ErrorCode::None.into()))
                    .results_by_transaction(Some([].into()))
                    .results_by_topic_v_3_and_below(Some(results_by_topic_v_3_and_below)))
            }

            TxnAddPartitionsResponse::VersionFourPlus(results_by_transaction) => {
                Ok(AddPartitionsToTxnResponse::default()
                    .throttle_time_ms(0)
                    .error_code(Some(ErrorCode::None.into()))
                    .results_by_transaction(Some(results_by_transaction))
                    .results_by_topic_v_3_and_below(Some([].into())))
            }
        }
    }
}
