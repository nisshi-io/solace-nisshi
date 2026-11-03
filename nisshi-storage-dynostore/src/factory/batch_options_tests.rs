// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

//! The S3 and Google Cloud Storage engines must refuse a `batch_min_size` or
//! `batch_max_delay` whose value doesn't parse, instead of running without the
//! batching that the user asked for. These tests read the options without an
//! object store.

use std::{assert_matches, time::Duration};

use nisshi_storage::Error;
use url::Url;

use super::{BATCH_MAX_DELAY, BATCH_MIN_SIZE, BatchOptions, DURATION_HINT, SIZE_HINT};

const SCHEMES: [&str; 2] = ["s3", "gs"];

fn parse(scheme: &str, query: &str) -> Result<BatchOptions, Error> {
    BatchOptions::parse(&Url::parse(&format!("{scheme}://bucket?{query}")).expect("url"))
}

/// The values that a hint names: the text after "such as", split at commas and "or".
fn examples(hint: &str) -> Vec<&str> {
    hint.rsplit_once("such as ")
        .map_or(hint, |(_, examples)| examples)
        .split(", ")
        .flat_map(|part| part.split(" or "))
        .collect()
}

#[test]
fn absent_options_mean_no_batching() {
    for scheme in SCHEMES {
        assert_eq!(BatchOptions::default(), parse(scheme, "").expect(scheme));
    }
}

#[test]
fn valid_options_are_read() {
    for scheme in SCHEMES {
        assert_eq!(
            BatchOptions {
                minimum_size: Some(64 * 1024),
                maximum_delay: Some(Duration::from_millis(500)),
            },
            parse(scheme, "batch_min_size=64k&batch_max_delay=500ms").expect(scheme)
        );
    }
}

#[test]
fn unparsable_values_are_refused_with_the_scheme_option_and_value() {
    for scheme in SCHEMES {
        for (option, value, hint) in [
            (BATCH_MIN_SIZE, "banana", SIZE_HINT),
            (BATCH_MIN_SIZE, "64KiB", SIZE_HINT),
            (BATCH_MAX_DELAY, "banana", DURATION_HINT),
            (BATCH_MAX_DELAY, "5 minutes", DURATION_HINT),
        ] {
            assert_matches!(
                parse(scheme, &format!("{option}={value}")),
                Err(Error::InvalidStorageOptionValue {
                    scheme: error_scheme,
                    option: error_option,
                    value: error_value,
                    expected,
                }) if error_scheme == scheme
                    && error_option == option
                    && error_value == value
                    && expected == hint,
                "{scheme} {option}={value}"
            );
        }
    }
}

/// `human_units` holds a size in a `u64` and refuses one that overflows it. On a
/// 32-bit target, a size that fits a `u64` but not a `usize` is refused the same way.
#[test]
fn too_large_a_size_is_refused() {
    for scheme in SCHEMES {
        assert_matches!(
            parse(scheme, "batch_min_size=20000000t"),
            Err(Error::InvalidStorageOptionValue { option, value, .. })
                if option == BATCH_MIN_SIZE && value == "20000000t",
            "{scheme}"
        );
    }
}

/// A bad value is refused even when another occurrence of the option parses.
#[test]
fn every_occurrence_must_parse() {
    for scheme in SCHEMES {
        assert_matches!(
            parse(scheme, "batch_max_delay=1s&batch_max_delay=banana"),
            Err(Error::InvalidStorageOptionValue { value, .. }) if value == "banana",
            "{scheme}"
        );
    }
}

/// The error message shows the hint, so every value that it names must be one that
/// the option takes.
#[test]
fn every_example_in_a_hint_parses() {
    for (option, hint) in [
        (BATCH_MIN_SIZE, SIZE_HINT),
        (BATCH_MAX_DELAY, DURATION_HINT),
    ] {
        let examples = examples(hint);
        assert!(examples.len() > 1, "{hint}");

        for example in examples {
            assert!(
                parse("s3", &format!("{option}={example}")).is_ok(),
                "{option}={example} from the hint \"{hint}\" doesn't parse"
            );
        }
    }
}
