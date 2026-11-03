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

//! The SQLite engine must refuse a `busy_timeout`, `compaction` or `mode` whose value
//! doesn't parse, instead of running with the default, and must do so before it
//! creates the database file.

use std::{assert_matches, env, fs, path::PathBuf, time::Duration};

use nisshi_storage::{Error, Result};
use url::Url;
use uuid::Uuid;

use super::{
    BUSY_TIMEOUT, BUSY_TIMEOUT_HINT, COMPACTION, COMPACTION_HINT, CommunicationMode,
    CompactionMode, Engine, LiteOptions, MODE, MODE_HINT, database_file,
};

fn parse(query: &str) -> Result<LiteOptions> {
    LiteOptions::parse(&Url::parse(&format!("sqlite://nisshi.db?{query}"))?)
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
fn absent_options_take_their_defaults() -> Result<()> {
    assert_eq!(
        LiteOptions {
            busy_timeout: Duration::from_secs(5),
            compaction: CompactionMode::Multi,
            mode: CommunicationMode::Semaphore,
        },
        parse("vacuum_into=nisshi.vacuum.db")?
    );
    Ok(())
}

#[test]
fn valid_options_are_read() -> Result<()> {
    assert_eq!(
        LiteOptions {
            busy_timeout: Duration::from_millis(250),
            compaction: CompactionMode::Single,
            mode: CommunicationMode::Direct,
        },
        parse("busy_timeout=250ms&compaction=single&mode=direct")?
    );
    Ok(())
}

#[test]
fn unparsable_values_are_refused_with_the_scheme_option_and_value() {
    for (option, value, hint) in [
        (BUSY_TIMEOUT, "banana", BUSY_TIMEOUT_HINT),
        (BUSY_TIMEOUT, "5 seconds", BUSY_TIMEOUT_HINT),
        (COMPACTION, "Single", COMPACTION_HINT),
        (COMPACTION, "none", COMPACTION_HINT),
        (MODE, "Direct", MODE_HINT),
        (MODE, "memory", MODE_HINT),
    ] {
        assert_matches!(
            parse(&format!("{option}={value}")),
            Err(Error::InvalidStorageOptionValue {
                scheme,
                option: error_option,
                value: error_value,
                expected,
            }) if scheme == "sqlite"
                && error_option == option
                && error_value == value
                && expected == hint,
            "{option}={value}"
        );
    }
}

/// A bad value is refused even when another occurrence of the option parses.
#[test]
fn every_occurrence_must_parse() {
    assert_matches!(
        parse("mode=direct&mode=Direct"),
        Err(Error::InvalidStorageOptionValue { option, value, .. })
            if option == MODE && value == "Direct"
    );
}

/// The error message shows the hint, so every value that it names must be one that
/// the option takes.
#[test]
fn every_example_in_a_hint_parses() {
    for (option, hint) in [
        (BUSY_TIMEOUT, BUSY_TIMEOUT_HINT),
        (COMPACTION, COMPACTION_HINT),
        (MODE, MODE_HINT),
    ] {
        let examples = examples(hint);
        assert!(examples.len() > 1, "{hint}");

        for example in examples {
            assert!(
                parse(&format!("{option}={example}")).is_ok(),
                "{option}={example} from the hint \"{hint}\" doesn't parse"
            );
        }
    }
}

/// Removes a database file and its journal files when dropped, also on panic.
struct DatabaseFile(PathBuf);

impl Drop for DatabaseFile {
    fn drop(&mut self) {
        for suffix in ["", "-shm", "-wal"] {
            let mut path = self.0.clone().into_os_string();
            path.push(suffix);
            _ = fs::remove_file(path);
        }
    }
}

async fn build(storage: &Url) -> Result<()> {
    Engine::builder()
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
        .cluster("nisshi".to_owned())
        .storage(storage.clone())
        .node(111)
        .build()
        .await
        .map(|_| ())
}

/// An option is read before the database file is opened, so a bad value leaves no
/// file behind. The same URL with a valid value creates the file, which shows that
/// the test looks where the engine writes.
#[tokio::test]
async fn bad_option_stops_startup_before_the_database_file_is_created() -> Result<()> {
    let name = format!("bad-option-{}.db", Uuid::now_v7());
    let bad = Url::parse(&format!("sqlite://{name}?mode=Direct"))?;
    let good = Url::parse(&format!("sqlite://{name}?mode=direct"))?;

    let file = DatabaseFile(database_file::path(env::current_dir()?, &bad));
    assert_eq!(file.0, database_file::path(env::current_dir()?, &good));

    assert_matches!(
        build(&bad).await,
        Err(Error::InvalidStorageOptionValue { option, .. }) if option == MODE
    );
    assert!(!file.0.exists(), "{} was created", file.0.display());

    build(&good).await?;
    assert!(file.0.exists(), "{} was not created", file.0.display());

    Ok(())
}
