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

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{fs, path::Path};

pub mod acls;
pub mod broker_lifecycle;
pub mod groups;
pub mod log_cleanup;
pub mod produce_consume;
pub mod scram;
pub mod tls;
pub mod topic_admin;

// Cargo only builds the modules that a `pub mod` line declares, so a file in `tests/smoke/` without
// one would never compile or run. Fail instead of skipping it silently. `main.rs` declares each
// file directly in `tests/smoke/`, and `<folder>.rs` declares each file in `<folder>/`.
#[test]
fn every_test_file_is_declared() -> std::io::Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/smoke");
    let mut undeclared = Vec::new();
    let mut folders = vec![root.clone()];

    while let Some(folder) = folders.pop() {
        let declaring_file = if folder == root {
            root.join("main.rs")
        } else {
            folder.with_extension("rs")
        };
        let declarations = fs::read_to_string(&declaring_file).unwrap_or_default();

        for entry in fs::read_dir(&folder)? {
            let path = entry?.path();

            if path.is_dir() {
                folders.push(path);
                continue;
            }

            let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };

            if path.extension().is_some_and(|extension| extension == "rs")
                && path != root.join("main.rs")
                && !declarations.contains(&format!("\npub mod {stem};\n"))
            {
                undeclared.push(format!(
                    "{} in {}",
                    path.strip_prefix(&root).unwrap_or(&path).display(),
                    declaring_file
                        .strip_prefix(&root)
                        .unwrap_or(&declaring_file)
                        .display()
                ));
            }
        }
    }

    assert!(
        undeclared.is_empty(),
        "add a `pub mod <name>;` line for each file: {undeclared:?}"
    );
    Ok(())
}
