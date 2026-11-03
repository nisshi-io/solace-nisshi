// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

//! The exit status and error line of `nisshi user create` when the broker refuses the user.

#![cfg(feature = "dynostore")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::Broker;

#[test]
fn user_create_fails_when_the_broker_refuses_the_user() {
    // `memory://` stores no SCRAM credentials, so it refuses every user with
    // `UnsupportedVersion`.
    let broker = Broker::start();

    let output = broker.run(
        &[
            "user",
            "create",
            "alice",
            "secret",
            "--mechanism",
            "scram256",
        ],
        "",
    );

    assert!(!output.status.success(), "{output:?}");

    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        stderr.lines().any(|line| {
            line.starts_with("error: user alice: ") && line.ends_with("(UnsupportedVersion)")
        }),
        "{output:?}"
    );
}
