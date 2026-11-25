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

//! The crate does not depend on a storage engine, so its cases can't build one.

use std::process::Command;

/// The crate that `nisshi-broker` always depends on: a stub engine that does not store records. It
/// can't stand in for the caller's engine, so the crate may depend on it.
const STUB_ENGINE: &str = "nisshi-storage-null";

/// Returns the name of each crate that the crate depends on: its normal dependencies, on every
/// target, with every feature of the crate on.
///
/// The test asks `cargo tree` instead of reading `Cargo.toml`, because a manifest can name an
/// engine in many forms: a table, a renamed package, a target section, or a crate that depends
/// on an engine itself.
fn resolved_dependencies() -> Vec<String> {
    let output = Command::new(env!("CARGO"))
        .args([
            "tree",
            "--package",
            env!("CARGO_PKG_NAME"),
            "--edges",
            "normal",
            "--all-features",
            "--target",
            "all",
            "--prefix",
            "none",
            "--format",
            "{p}",
            "--manifest-path",
        ])
        .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .output()
        .expect("cargo tree runs");
    assert!(
        output.status.success(),
        "cargo tree failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout)
        .expect("cargo tree prints UTF-8")
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .map(str::to_owned)
        .collect()
}

/// The crate does not depend on an engine crate, directly or through another crate, and does not
/// turn on a feature of `nisshi-broker` that builds an engine.
#[test]
fn dependencies_include_no_engine() {
    let dependencies = resolved_dependencies();
    assert!(
        dependencies.iter().any(|name| name == "nisshi-broker"),
        "cargo tree lists nisshi-broker, so the test reads the right package: {dependencies:?}"
    );

    let mut engines: Vec<&str> = dependencies
        .iter()
        .map(String::as_str)
        .filter(|name| name.starts_with("nisshi-storage-"))
        .filter(|name| *name != STUB_ENGINE && *name != env!("CARGO_PKG_NAME"))
        .collect();
    engines.sort_unstable();
    engines.dedup();
    assert!(
        engines.is_empty(),
        "nisshi-storage-integration-test must not depend on a storage engine, also through \
         another crate or a feature of nisshi-broker: {engines:?}"
    );
}
