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

use rama::extensions::Extension;
use tokio_util::sync::CancellationToken;

/// The token that drains a connection when it is cancelled, which a listener inserts into each
/// connection's extensions when it accepts the connection.
///
/// [`TcpBytesService`](crate::TcpBytesService) watches it while it waits for the first byte of
/// the next request. A request that has started to arrive when the token is cancelled is read,
/// served and answered, and then the connection closes. A connection without this extension
/// serves requests until the client hangs up.
#[derive(Clone, Debug, Default, Extension)]
pub struct ShutdownExtension(CancellationToken);

impl ShutdownExtension {
    pub fn new(token: CancellationToken) -> Self {
        Self(token)
    }

    pub fn token(&self) -> &CancellationToken {
        &self.0
    }
}
