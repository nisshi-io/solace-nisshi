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

use nisshi_broker::Result;
use nisshi_sans_io::{DescribeAclsRequest, ErrorCode, RequestInput};
use nisshi_storage::DescribeAclsService;
use rama::{Service, extensions::Extensions};
use rand::{prelude::*, rng};
use uuid::Uuid;

use crate::common::init_tracing;

/// The broker does not store ACLs, so it refuses a request to list them with `SECURITY_DISABLED`
/// and the message Apache Kafka sends without an authorizer. An empty list with no error would
/// tell an operator that ACLs are supported and that none exist.
#[tokio::test]
async fn describe_is_answered_security_disabled() -> Result<()> {
    let _guard = init_tracing()?;

    let service = DescribeAclsService {
        storage: crate::common::memory_storage(Uuid::now_v7(), rng().random_range(0..i32::MAX))
            .await?,
    };

    let response = service
        .serve(RequestInput {
            request: DescribeAclsRequest::default(),
            extensions: Extensions::default(),
        })
        .await?;

    assert_eq!(
        ErrorCode::SecurityDisabled,
        ErrorCode::try_from(response.error_code)?
    );
    assert_eq!(
        Some("No Authorizer is configured on the broker"),
        response.error_message.as_deref()
    );
    assert_eq!(Some(0), response.resources.as_ref().map(Vec::len));

    Ok(())
}
