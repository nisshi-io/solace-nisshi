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

//! Every tracked `.rs`, `.proto` and `.sql` file starts with the Solace copyright line, as
//! `.claude/rules/file-header.md` and CONTRIBUTING.md describe.

#![allow(clippy::expect_used)]

use std::{fs, path::Path, process::Command};

/// The notice is ASCII because the broker sends `pg/schema.sql` to PostgreSQL with its comments,
/// and PostgreSQL converts a whole query to the database's encoding, so a non-ASCII character fails
/// on a database that is not UTF8.
const NOTICE: &str = "Copyright (c) Solace Corporation";

/// Returns the comment marker of a source file of this type.
fn marker(path: &str) -> &'static str {
    if path.ends_with(".sql") { "--" } else { "//" }
}

/// Returns each tracked `.rs`, `.proto` and `.sql` file, as a path relative to `workspace`.
fn source_files(workspace: &Path) -> Vec<String> {
    let output = Command::new("git")
        .args(["ls-files", "-z", "--", "*.rs", "*.proto", "*.sql"])
        .current_dir(workspace)
        .output()
        .expect("git runs in the workspace");
    assert!(
        output.status.success(),
        "`git ls-files` failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    String::from_utf8(output.stdout)
        .expect("the tracked paths are UTF-8")
        .split('\0')
        .filter(|path| !path.is_empty())
        .map(str::to_owned)
        .collect()
}

#[test]
fn every_source_file_starts_with_the_copyright_header() {
    // This crate's manifest folder is one level below the workspace root.
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");

    // The test lists files with git, so it checks only a git checkout. The crate that crates.io
    // serves includes this test but has no `.git` above it. In a linked worktree `.git` is a file,
    // so the test checks that `.git` exists, not that it is a folder.
    if !workspace.join(".git").exists() {
        return;
    }

    let files = source_files(&workspace);
    assert!(!files.is_empty(), "`git ls-files` listed no source file");

    let mut missing = Vec::new();
    for path in files {
        let text = fs::read_to_string(workspace.join(&path))
            .unwrap_or_else(|error| panic!("cannot read {path}: {error}"));
        let mut lines = text.lines().peekable();

        // Emacs reads its mode line only from the first line, so the header may follow one.
        if lines.peek().is_some_and(|line| line.contains("-*-")) {
            _ = lines.next();
        }

        let marker = marker(&path);
        if lines.next() != Some(format!("{marker} {NOTICE}").as_str())
            || lines.next() != Some(marker)
        {
            missing.push(path);
        }
    }

    assert!(
        missing.is_empty(),
        "start each of these files with `// {NOTICE}` (`-- ` in SQL), then a line with only the \
         comment marker, as .claude/rules/file-header.md says: {missing:#?}"
    );
}
