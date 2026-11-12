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

//! The broker's environment variables, against the `nisshi` binary: a bad value fails the broker,
//! and no other command.

#![cfg(feature = "dynostore")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::run_with_env;

/// A broker address that no DNS resolves, so that a tool that gets as far as connecting ends
/// with 3 (broker unreachable) whatever runs on this machine.
const NO_BROKER: &str = "tcp://nisshi-no-such-host.invalid:9092";

const BAD_BROKER_ENV: [(&str, &str); 2] = [("STORAGE_ENGINE", "foo"), ("NUM_PARTITIONS", "0")];

#[test]
fn a_bad_broker_variable_does_not_fail_a_tool() {
    for env in BAD_BROKER_ENV {
        for tool in [
            &["topic", "list"][..],
            &["user", "delete", "alice"],
            &["cat", "consume", "taxi"],
            &["perf", "taxi", "produce", "--duration", "1s"],
        ] {
            let args = [
                &[tool[0], tool[1]][..],
                &["--broker", NO_BROKER],
                &tool[2..],
            ]
            .concat();
            let output = run_with_env(&args, &[env]);

            assert_eq!(
                Some(3),
                output.status.code(),
                "{env:?} {tool:?}: {output:?}"
            );
        }
    }
}

#[test]
fn a_bad_broker_variable_fails_the_broker_with_or_without_its_name() {
    for env in BAD_BROKER_ENV {
        for args in [&["broker"][..], &[]] {
            let output = run_with_env(args, &[env]);

            assert_eq!(
                Some(2),
                output.status.code(),
                "{env:?} {args:?}: {output:?}"
            );
            assert!(
                String::from_utf8_lossy(&output.stderr)
                    .contains(env.0.to_lowercase().replace('_', "-").as_str()),
                "{output:?}"
            );
        }
    }
}
