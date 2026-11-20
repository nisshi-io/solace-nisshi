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

use crate::{
    broker,
    init_producer::{self, TRANSACTION_TIMEOUT_MS},
};
use nisshi_broker::Result;
use nisshi_sans_io::{ErrorCode, InitProducerIdRequest, InitProducerIdResponse, RequestInput};
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

/// v0-2 have no ProducerId or ProducerEpoch, so a fresh request decodes with neither set.
pub async fn old_versions_are_fresh(
    cluster_id: impl Into<String>,
    broker_id: i32,
    storage: impl Storage + Clone,
) -> Result<()> {
    broker::register_broker(cluster_id, broker_id, &storage).await?;

    let service = InitProducerIdService { storage };

    let mut previous: Option<InitProducerIdResponse> = None;

    for api_version in 0..=5 {
        let request = init_producer::request_decoded_at_version(
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

        init_producer::assert_ok(&response);
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
    broker::register_broker(cluster_id, broker_id, &storage).await?;

    let first = init_producer::init_idempotent_producer(&storage, -1, -1).await?;
    init_producer::assert_ok(&first);

    let claimed =
        init_producer::init_idempotent_producer(&storage, first.producer_id, first.producer_epoch)
            .await?;
    init_producer::assert_ok(&claimed);
    assert!(claimed.producer_id > first.producer_id);
    assert_eq!(0, claimed.producer_epoch);

    let unknown =
        init_producer::init_idempotent_producer(&storage, claimed.producer_id + 1_000, 7).await?;
    init_producer::assert_ok(&unknown);
    assert!(unknown.producer_id > claimed.producer_id);
    assert_eq!(0, unknown.producer_epoch);

    Ok(())
}
