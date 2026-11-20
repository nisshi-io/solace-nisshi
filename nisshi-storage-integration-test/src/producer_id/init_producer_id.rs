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

//! Cases about producer IDs that an idempotent producer asks for, without a transactional ID.

use crate::broker::register_broker;
use nisshi_broker::Result;
use nisshi_sans_io::{
    ApiKey as _, Body, ErrorCode, Frame, Header, InitProducerIdRequest, InitProducerIdResponse,
    RequestInput,
};
use nisshi_storage::{InitProducerIdService, Storage};
use rama::{Service, extensions::Extensions};

pub async fn no_txn_init_producer_id(storage: impl Storage + Clone) -> Result<()> {
    let service = InitProducerIdService {
        storage: storage.clone(),
    };

    let transactional_id = None;
    let transaction_timeout_ms = 0;
    let producer_id = Some(-1);
    let producer_epoch = Some(-1);

    let extensions = Extensions::default();

    let r0 = service
        .serve(RequestInput {
            request: InitProducerIdRequest::default()
                .transactional_id(transactional_id.clone())
                .transaction_timeout_ms(transaction_timeout_ms)
                .producer_id(producer_id)
                .producer_epoch(producer_epoch),
            extensions: extensions.clone(),
        })
        .await?;

    assert_eq!(r0.error_code, i16::from(ErrorCode::None));
    assert_eq!(r0.producer_epoch, 0);
    assert!(r0.producer_id > 0);

    let r1 = service
        .serve(RequestInput {
            request: InitProducerIdRequest::default()
                .transactional_id(transactional_id.clone())
                .transaction_timeout_ms(transaction_timeout_ms)
                .producer_id(producer_id)
                .producer_epoch(producer_epoch),
            extensions: extensions.clone(),
        })
        .await?;

    assert_eq!(r1.error_code, i16::from(ErrorCode::None));
    assert_eq!(r1.producer_epoch, 0);
    assert!(r1.producer_id > r0.producer_id);

    Ok(())
}

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

/// v0-2 have no ProducerId or ProducerEpoch, so a fresh request decodes with neither set.
pub async fn old_versions_are_fresh(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: impl Storage + Clone,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &storage).await?;

    let service = InitProducerIdService { storage };

    let mut previous: Option<InitProducerIdResponse> = None;

    for api_version in 0..=5 {
        let request = request_decoded_at_version(
            InitProducerIdRequest::default()
                .transactional_id(None)
                .transaction_timeout_ms(TRANSACTION_TIMEOUT_MS)
                .producer_id(Some(-1))
                .producer_epoch(Some(-1)),
            api_version,
        )?;

        if api_version < 3 {
            assert_eq!((None, None), (request.producer_id, request.producer_epoch));
        }

        let response = service
            .serve(RequestInput {
                request,
                extensions: Extensions::default(),
            })
            .await?;

        assert_ok(&response);
        assert_eq!(0, response.producer_epoch);

        if let Some(previous) = previous {
            assert!(response.producer_id > previous.producer_id);
        }

        previous = Some(response);
    }

    Ok(())
}

/// Without a transactional ID, the storage ignores a claim and grants a new ID.
pub async fn no_txn_claim_gets_new_id(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: impl Storage + Clone,
) -> Result<()> {
    register_broker(cluster_id, broker_id, &storage).await?;

    let first = init_idempotent_producer(&storage, -1, -1).await?;
    assert_ok(&first);

    let claimed =
        init_idempotent_producer(&storage, first.producer_id, first.producer_epoch).await?;
    assert_ok(&claimed);
    assert!(claimed.producer_id > first.producer_id);
    assert_eq!(0, claimed.producer_epoch);

    let unknown = init_idempotent_producer(&storage, claimed.producer_id + 1_000, 7).await?;
    assert_ok(&unknown);
    assert!(unknown.producer_id > claimed.producer_id);
    assert_eq!(0, unknown.producer_epoch);

    Ok(())
}
