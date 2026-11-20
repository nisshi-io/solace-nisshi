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

//! Each case runs on every engine, because the macro lists it.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

/// The source of the macro, which lists every case.
const MACRO: &str = include_str!("../../src/lib.rs");

/// Returns the case list that the macro gives `area`'s file `file`, or `None` if the macro has no
/// list for the file.
fn case_list(area: &str, file: &str) -> Option<&'static str> {
    let start = MACRO.find(&format!("$crate::__file!({area}, {file}, ["))?;
    let list = &MACRO[start..];
    let end = list.find("], $config);")?;
    Some(&list[..end])
}

/// Returns whether `list` names `case`, written as the case's name and its arguments.
fn lists(list: &str, case: &str) -> bool {
    list.match_indices(&format!("{case}(")).any(|(at, _)| {
        list[..at]
            .chars()
            .next_back()
            .is_none_or(|before| !(before.is_alphanumeric() || before == '_'))
    })
}

/// Returns the name of each public async function in `source`: a case.
fn cases(source: &str) -> impl Iterator<Item = String> + '_ {
    source.lines().filter_map(|line| {
        line.strip_prefix("pub async fn ").map(|rest| {
            rest.chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect()
        })
    })
}

/// Returns the name of each file or directory in `directory`, without its extension.
fn stems(directory: &Path) -> io::Result<Vec<(String, PathBuf)>> {
    let mut stems = Vec::new();
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) {
            stems.push((stem.to_owned(), path));
        }
    }
    Ok(stems)
}

/// A case that the macro doesn't list in its own file's list runs on no engine, and nothing else
/// warns about it, because an unused public function is not a warning. Each area is a directory
/// in `src`, and each file in it holds cases.
#[test]
fn every_case_is_registered() -> io::Result<()> {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut unregistered = Vec::new();

    for (area, area_dir) in stems(&crate_dir.join("src"))? {
        if !area_dir.is_dir() {
            continue;
        }

        // The macro expands an area only when `__areas!` calls the area's macro.
        if !MACRO.contains(&format!("$crate::__area_{area}!($config);")) {
            unregistered.push(format!(
                "src/{area}: no `__area_{area}!` call in `__areas!`"
            ));
        }

        for (file, path) in stems(&area_dir)? {
            let list = case_list(&area, &file).unwrap_or_default();
            for case in cases(&fs::read_to_string(&path)?) {
                if !lists(list, &case) {
                    unregistered.push(format!("src/{area}/{file}.rs: {case}"));
                }
            }
        }
    }

    assert!(
        unregistered.is_empty(),
        "list each case in its file's `__file!` list in the macro in src/lib.rs: {unregistered:?}"
    );
    Ok(())
}
