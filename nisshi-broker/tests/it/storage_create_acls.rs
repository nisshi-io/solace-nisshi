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
    CreateAclsRequest, ErrorCode, RequestInput, create_acls_request::AclCreation,
};
use nisshi_storage::CreateAclsService;
use rama::{Service, extensions::Extensions};
use rand::{prelude::*, rng};
use uuid::Uuid;

use crate::common::init_tracing;

/// ACLs have no backing storage, so every creation is answered `SECURITY_DISABLED` with
/// Kafka's message, one result per creation, in request order.
#[tokio::test]
async fn every_creation_is_answered_security_disabled() -> Result<()> {
    let _guard = init_tracing()?;

    let service = CreateAclsService {
        storage: crate::common::memory_storage(Uuid::now_v7(), rng().random_range(0..i32::MAX))
            .await?,
    };

    for creations in [0, 1, 3] {
        let response = service
            .serve(RequestInput {
                request: CreateAclsRequest::default().creations(Some(
                    (0..creations)
                        .map(|_| AclCreation::default())
                        .collect::<Vec<_>>(),
                )),
                extensions: Extensions::default(),
            })
            .await?;

        let results = response.results.as_deref().unwrap_or_default();
        assert_eq!(creations, results.len());

        for result in results {
            assert_eq!(
                ErrorCode::SecurityDisabled,
                ErrorCode::try_from(result.error_code)?
            );
            assert_eq!(
                Some("No Authorizer is configured."),
                result.error_message.as_deref()
            );
        }
    }

    Ok(())
}
