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

//! Tests that the broker uses the storage URL it is given, or stops at startup with a message that
//! names the bad value. A broker that ignores a value runs with a setting the user didn't choose,
//! and one that panics gives the user only a backtrace.

use nisshi_smoke_test::{Broker, LaunchOptions, StorageUrl, settings};

/// A storage URL that doesn't parse must stop the broker with a message that names it.
#[test]
fn unparsable_storage_url_stops_the_broker() {
    Broker::launch_expecting_refusal(LaunchOptions::new(StorageUrl::as_given("not a url")))
        .assert_error_names("not a url");
}

/// A `maintenance_interval` that doesn't parse must stop the broker with a message that names it,
/// instead of the broker running with the default interval.
#[test]
fn unparsable_maintenance_interval_stops_the_broker() {
    let option = "maintenance_interval=banana";

    let storage = settings::storage_url_under_test().with_query_option(option);

    Broker::launch_expecting_refusal(LaunchOptions::new(storage)).assert_error_names(option);
}

/// An interval of zero would mean running maintenance without pause, so it must be refused.
#[test]
fn zero_maintenance_interval_stops_the_broker() {
    let option = "maintenance_interval=0s";

    let storage = settings::storage_url_under_test().with_query_option(option);

    Broker::launch_expecting_refusal(LaunchOptions::new(storage)).assert_error_names(option);
}

/// A SQLite database path in the storage URL. A relative path and an absolute path that the broker
/// can write must work. A path it can't write must stop the broker at startup, so the user sees
/// what to fix instead of a panic.
#[cfg(feature = "sqlite")]
mod sqlite {
    use std::fs;

    use nisshi_smoke_test::{Broker, KafkaCli, LaunchOptions, StorageUrl, unique};

    /// A database file at an absolute path, removed with its journal files when the test ends,
    /// whether it passes or not.
    struct TemporarySqliteDatabase(String);

    impl Drop for TemporarySqliteDatabase {
        fn drop(&mut self) {
            for suffix in ["", "-shm", "-wal"] {
                _ = fs::remove_file(format!("{}{suffix}", self.0));
            }
        }
    }

    /// Starts a broker on `storage`, checks that it stores a record and returns it, and returns
    /// the broker.
    #[track_caller]
    fn stores_and_returns_a_record(storage: &str) -> Broker {
        let broker = Broker::launch(LaunchOptions::new(StorageUrl::as_given(storage)));
        let cli = KafkaCli::new(broker.bootstrap());
        let topic = cli.topics().create_unique(1, &[]);

        _ = cli.console_producer().produce_values(&topic, &["stored"]);

        let [record] = &cli.console_consumer().read_records(&topic, 0, 0, 1)[..] else {
            unreachable!("read_records checks the record count")
        };
        assert_eq!(record.value, "stored");

        broker
    }

    /// The broker resolves a relative path against its working directory.
    #[test]
    fn relative_path_works() {
        _ = stores_and_returns_a_record("sqlite://nisshi.db");
    }

    /// The broker must keep its database at the absolute path in the URL, because a user who
    /// gives one expects the data there, for example on a mounted volume. The broker reads the path
    /// after `sqlite:///` as relative, so an absolute path needs a fourth slash:
    /// `sqlite:////tmp/...`. `/tmp` can be written both by a broker process and in the broker image.
    #[test]
    fn absolute_path_works() {
        let database_file =
            TemporarySqliteDatabase(format!("/tmp/{}.db", unique::name("nisshi-smoke")));

        let broker = stores_and_returns_a_record(&format!("sqlite:///{}", database_file.0));

        assert!(
            broker.file_exists(&database_file.0),
            "the broker didn't create its database at {}",
            database_file.0
        );
    }

    /// A `mode` that the engine doesn't take must stop the broker with a message that names it,
    /// instead of the broker running in the default mode. The match is case-sensitive.
    #[test]
    fn unparsable_mode_stops_the_broker() {
        let option = "mode=Direct";

        let storage = StorageUrl::as_given(format!("sqlite://nisshi.db?{option}"));

        Broker::launch_expecting_refusal(LaunchOptions::new(storage)).assert_error_names(option);
    }

    /// A database path the broker can't write must stop it at startup with a message that names the
    /// path, so the user sees what to fix.
    #[test]
    fn unwritable_path_stops_the_broker_with_its_name() {
        // Nothing can create a directory in `/proc`, not even root in a container: on Linux it
        // belongs to the kernel, and on macOS it would go in the read-only root of the disk.
        let path = "/proc/nisshi/nisshi.db";

        let storage = StorageUrl::as_given(format!("sqlite:///{path}"));

        Broker::launch_expecting_refusal(LaunchOptions::new(storage)).assert_error_names(path);
    }
}
