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

//! Checks that `cargo install nisshi`, which builds the `default` feature, gets a broker
//! with a storage engine.
//!
//! The test reads the manifest instead of checking `cfg!(feature = ...)`, because CI
//! builds the tests with `--all-features`, and that turns on every engine whatever
//! `default` says.

#![allow(clippy::expect_used)]

use std::process::Command;

use serde_json::Value;

fn default_features() -> Vec<String> {
    let output = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--manifest-path",
        ])
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .output()
        .expect("cargo metadata runs");
    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let metadata: Value = serde_json::from_slice(&output.stdout).expect("cargo metadata is JSON");
    let package = metadata["packages"]
        .as_array()
        .expect("packages is an array")
        .iter()
        .find(|package| package["name"] == env!("CARGO_PKG_NAME"))
        .expect("metadata lists this package");

    package["features"]["default"]
        .as_array()
        .expect("the package has a default feature")
        .iter()
        .map(|feature| feature.as_str().expect("a feature is a string").to_owned())
        .collect()
}

#[test]
fn default_features_include_the_default_storage_engine() {
    // The broker's default `--storage-engine` is `memory://`, which `dynostore` provides.
    let default_features = default_features();
    assert!(
        default_features
            .iter()
            .any(|feature| feature == "dynostore"),
        "dynostore is missing from {default_features:?}"
    );
}

#[test]
fn default_features_include_sqlite_and_postgres() {
    let default_features = default_features();
    for engine in ["libsql", "postgres"] {
        assert!(
            default_features.iter().any(|feature| feature == engine),
            "{engine} is missing from {default_features:?}"
        );
    }
}
