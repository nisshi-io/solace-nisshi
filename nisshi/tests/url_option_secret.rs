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

//! A URL option that does not parse reports its option and the reason. It leaves the value out,
//! because the value can hold a password and stderr goes to the container log.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::run_with_env;

const SECRET: &str = "s3cr3t-hunter2";

fn typo() -> String {
    format!("postgres://user:{SECRET}@ho st/db")
}

fn assert_reports_without_the_value(output: &std::process::Output, option: &str) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert_eq!(Some(2), output.status.code(), "{output:?}");
    assert!(stderr.contains(option), "{stderr}");
    assert!(stderr.contains("invalid URL"), "{stderr}");
    for text in [&stderr, &stdout] {
        assert!(!text.contains(SECRET), "{text}");
        assert!(!text.contains("ho st"), "{text}");
    }
}

#[test]
fn a_url_flag_that_does_not_parse_keeps_its_value_out_of_stderr() {
    let output = run_with_env(&["broker", "--storage-engine", &typo()], &[]);

    assert_reports_without_the_value(&output, "--storage-engine");
}

#[test]
fn a_url_variable_that_does_not_parse_keeps_its_value_out_of_stderr() {
    let output = run_with_env(&["broker"], &[("STORAGE_ENGINE", &typo())]);

    assert_reports_without_the_value(&output, "--storage-engine");
}

#[test]
fn help_does_not_print_the_value_of_a_url_variable() {
    let url = format!("postgres://user:{SECRET}@host/db");
    let output = run_with_env(&["broker", "--help"], &[("STORAGE_ENGINE", &url)]);

    assert_eq!(Some(0), output.status.code(), "{output:?}");
    assert!(!String::from_utf8_lossy(&output.stdout).contains(SECRET));
}
