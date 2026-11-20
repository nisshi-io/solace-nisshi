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

//! Sends `InitProducerId` requests as a producer does, and checks the responses.

use nisshi_broker::Result;
use nisshi_sans_io::{
    ApiKey as _, Body, ErrorCode, Frame, Header, InitProducerIdRequest, InitProducerIdResponse,
    RequestInput,
};
use nisshi_storage::{InitProducerIdService, Storage};
use rama::{Service, extensions::Extensions};

pub(crate) const TRANSACTION_TIMEOUT_MS: i32 = 10_000;

/// Serves the `InitProducerId` request that an idempotent producer sends, which does not carry a
/// transactional ID, with this producer ID and epoch.
pub(crate) async fn init_idempotent_producer(
    storage: &(impl Storage + Clone),
    producer_id: i64,
    producer_epoch: i16,
) -> Result<InitProducerIdResponse> {
    InitProducerIdService {
        storage: storage.clone(),
    }
    .serve(RequestInput {
        request: InitProducerIdRequest::default()
            .transactional_id(None)
            .transaction_timeout_ms(TRANSACTION_TIMEOUT_MS)
            .producer_id(Some(producer_id))
            .producer_epoch(Some(producer_epoch)),
        extensions: Extensions::default(),
    })
    .await
    .map_err(Into::into)
}

pub(crate) fn assert_ok(response: &InitProducerIdResponse) {
    assert_eq!(
        i16::from(ErrorCode::None),
        response.error_code,
        "{response:?}"
    );
}

pub(crate) fn assert_failed(error: ErrorCode, response: &InitProducerIdResponse) {
    assert_eq!(i16::from(error), response.error_code, "{response:?}");
    assert_eq!(-1, response.producer_id, "{response:?}");
    assert_eq!(-1, response.producer_epoch, "{response:?}");
}

/// Returns `request` as the broker decodes it after a client sends it at `api_version`.
pub(crate) fn request_decoded_at_version(
    request: InitProducerIdRequest,
    api_version: i16,
) -> Result<InitProducerIdRequest> {
    let encoded = Frame::request(
        Header::Request {
            api_key: InitProducerIdRequest::KEY,
            api_version,
            correlation_id: api_version.into(),
            client_id: None,
        },
        Body::InitProducerIdRequest(request),
    )?;

    InitProducerIdRequest::try_from(Frame::request_from_bytes(encoded)?.body).map_err(Into::into)
}
