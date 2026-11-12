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

// `pub` keeps each file's `pub` helpers reachable, as when every file was its own crate.
pub mod pool;
pub mod security;

// Cargo only builds the modules declared above, so a file in `tests/it/` without a
// `pub mod` line would never compile or run. Fail instead of skipping it silently.
#[test]
fn every_test_file_is_declared() -> std::io::Result<()> {
    let declared = include_str!("main.rs");
    let mut undeclared = Vec::new();

    for entry in std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/it"))? {
        let path = entry?.path();
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };

        if path.extension().is_some_and(|extension| extension == "rs")
            && stem != "main"
            && !declared.contains(&format!("\npub mod {stem};\n"))
        {
            undeclared.push(stem.to_owned());
        }
    }

    assert!(
        undeclared.is_empty(),
        "add `pub mod <name>;` to tests/it/main.rs for: {undeclared:?}"
    );
    Ok(())
}
