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

//! Values that no other test, process or earlier run uses, so tests can share one broker and one
//! database.

use nanoid::nanoid;

/// Returns a name that no other test, process or earlier run uses, so tests can share one broker
/// and one database.
///
/// The random part of the name contains only lowercase letters and digits.
pub fn name(prefix: &str) -> String {
    // This alphabet replaces nanoid's default alphabet, which contains `_` and uppercase letters.
    // `kafka-topics` prints a warning before its output for a topic name that contains `_`, and
    // the tests read the first line of that output.
    const NAME_ALPHABET: &str = "abcdefghijklmnopqrstuvwxyz0123456789";

    let alphabet = NAME_ALPHABET.chars().collect::<Vec<_>>();
    format!("{prefix}-{}", nanoid!(21, &alphabet))
}

/// Returns a random password for a SCRAM user that a test creates.
///
/// The password is a random number in decimal. CodeQL reports a password built from any constant,
/// such as a literal or the alphabet that [`name`] picks from, as a hard-coded cryptographic
/// value, so this one is built from none.
pub fn password() -> String {
    rand::random::<u128>().to_string()
}

#[cfg(test)]
mod tests {
    use super::name;

    #[test]
    fn name_appends_only_lowercase_letters_and_digits() {
        let name = name("topic");
        let random = name.strip_prefix("topic-").unwrap_or_default();

        assert_eq!(random.len(), 21, "{name}");
        assert!(
            random
                .chars()
                .all(|character| character.is_ascii_lowercase() || character.is_ascii_digit()),
            "{name}"
        );
    }
}
