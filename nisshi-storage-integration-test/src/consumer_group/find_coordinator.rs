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

use crate::random;
use nisshi_broker::Result;
use nisshi_sans_io::{ErrorCode, FindCoordinatorRequest, RequestInput};
use nisshi_storage::{FindCoordinatorService, Storage};
use rama::{Service, extensions::Extensions};

pub async fn simple(broker_id: i32, storage: impl Storage + Clone) -> Result<()> {
    let service = FindCoordinatorService { storage };
    let name = &random::alphanumeric_string(15)[..];

    let response = service
        .serve(RequestInput {
            request: FindCoordinatorRequest::default()
                .key(Some(name.into()))
                .key_type(Some(0))
                .coordinator_keys(Some([name.into()].into())),
            extensions: Extensions::default(),
        })
        .await?;

    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(response.error_code.expect("error code"))?
    );

    assert_eq!(Some(broker_id), response.node_id);

    Ok(())
}
