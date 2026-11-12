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

//! The startup warning for each environment variable that turns off certificate checks on the
//! broker's connections to object stores.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::process::{Command, Output};

/// Runs `nisshi broker` with `vars` as its only environment, from a directory without a `.env`.
///
/// No storage engine accepts the storage URL, so the broker exits during startup. The warning
/// must still appear, because an operator whose broker fails to start reads that output first.
fn broker_output(vars: &[(&str, &str)]) -> String {
    let Output {
        status,
        stdout,
        stderr,
    } = Command::new(env!("CARGO_BIN_EXE_nisshi"))
        .env_clear()
        .envs(vars.iter().copied())
        .current_dir(std::env::temp_dir())
        .args([
            "broker",
            "--storage-engine",
            "no-such-engine://nisshi/",
            "--silent",
        ])
        .output()
        .expect("broker runs");

    assert!(!status.success(), "broker started on an unknown engine");

    format!(
        "{}{}",
        String::from_utf8_lossy(&stdout),
        String::from_utf8_lossy(&stderr)
    )
}

#[test]
fn broker_warns_for_each_variable_that_turns_off_certificate_checks() {
    let output = broker_output(&[
        ("AWS_ALLOW_INVALID_CERTIFICATES", "true"),
        ("ALLOW_INVALID_CERTIFICATES", "yes"),
    ]);

    for variable in [
        "AWS_ALLOW_INVALID_CERTIFICATES",
        "ALLOW_INVALID_CERTIFICATES",
    ] {
        assert!(
            output.contains(&format!(
                "{variable} turns off certificate checks on connections to object stores"
            )),
            "no warning for {variable}: {output}"
        );
    }
    assert!(output.contains("#outbound-tls"), "{output}");
}

#[test]
fn broker_does_not_warn_for_a_false_value() {
    let output = broker_output(&[("GOOGLE_ALLOW_INVALID_CERTIFICATES", "false")]);

    assert!(!output.contains("turns off certificate checks"), "{output}");
}
