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

use std::path::PathBuf;
use url::Url;

/// Returns the database file that a `sqlite://` storage URL names, relative to
/// `working_directory`: the URL's host, then its path with one leading `/` removed.
///
/// A URL such as `sqlite://nisshi.db` or `sqlite://nisshi.db/` has an empty path once that `/`
/// is removed. The empty path is skipped, because `PathBuf::push("")` appends a trailing
/// separator, and Windows refuses to open `nisshi.db\` as a file.
pub(crate) fn path(mut working_directory: PathBuf, storage: &Url) -> PathBuf {
    if let Some(domain) = storage.domain() {
        working_directory.push(domain);
    }

    let path = storage.path();
    let relative = path.strip_prefix("/").unwrap_or(path);

    if !relative.is_empty() {
        working_directory.push(relative);
    }

    working_directory
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn resolve(storage: &str) -> PathBuf {
        path(PathBuf::from("working"), &Url::parse(storage).unwrap())
    }

    // Compares the paths as strings, because `Path` equality ignores a trailing separator.
    fn assert_same_path(actual: PathBuf, expected: PathBuf) {
        assert_eq!(actual.as_os_str(), expected.as_os_str());
    }

    #[test]
    fn host_only_url_names_a_file_in_the_working_directory() {
        assert_same_path(
            resolve("sqlite://nisshi.db"),
            Path::new("working").join("nisshi.db"),
        );
    }

    #[test]
    fn host_with_trailing_slash_names_the_same_file() {
        assert_same_path(
            resolve("sqlite://nisshi.db/"),
            Path::new("working").join("nisshi.db"),
        );
    }

    #[test]
    fn query_string_is_not_part_of_the_file_name() {
        assert_same_path(
            resolve("sqlite://nisshi.db?busy_timeout=5s"),
            Path::new("working").join("nisshi.db"),
        );
    }

    #[test]
    fn host_and_path_name_a_file_in_a_subdirectory() {
        assert_same_path(
            resolve("sqlite://data/nisshi.db"),
            Path::new("working").join("data").join("nisshi.db"),
        );
    }

    #[test]
    fn empty_host_names_a_file_in_the_working_directory() {
        assert_same_path(
            resolve("sqlite:///nisshi.db"),
            Path::new("working").join("nisshi.db"),
        );
    }
}
