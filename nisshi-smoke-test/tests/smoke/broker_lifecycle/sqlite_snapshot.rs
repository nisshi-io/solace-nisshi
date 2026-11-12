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

//! Tests the SQLite storage URL option `vacuum_into`, which makes each maintenance run write a
//! snapshot of the database to a file. A user keeps the snapshot as a backup, and a backup is
//! useful only if a broker can start on it and find the topics and records that it holds.

#![cfg(feature = "sqlite")]

use std::time::Duration;

use nisshi_smoke_test::{
    Broker, FREQUENT_MAINTENANCE, KafkaCli, LaunchOptions, StorageUrl, polling, settings,
};

/// The snapshot's path, relative to the broker's working directory, where its database is too.
const SNAPSHOT_PATH: &str = "snapshot.db";

/// How long the test waits for a maintenance run to write the snapshot again.
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(60);

/// A broker started on a snapshot must have the topics and records that the snapshot's broker
/// stored before it wrote the snapshot. The test deletes the broker's own database before it
/// starts the broker on the snapshot, as a user restores a lost database, so the records can come
/// only from the snapshot.
#[test]
fn broker_started_on_a_snapshot_has_its_topics_and_records() {
    let storage = settings::storage_url_under_test();
    let live_database = storage
        .sqlite_database_path()
        .expect("the SQLite leg runs on a sqlite:// URL")
        .to_owned();
    let broker = Broker::launch(LaunchOptions::new(
        storage
            .with_query_option(FREQUENT_MAINTENANCE)
            .with_query_option(&format!("vacuum_into={SNAPSHOT_PATH}")),
    ));
    let cli = KafkaCli::new(broker.bootstrap());
    let topic = cli.topics().create_unique(1, &[]);
    let values = ["value-0", "value-1", "value-2"];
    _ = cli.console_producer().produce_values(&topic, &values);

    // A run that started before the records were stored writes a snapshot without them, so the
    // test waits until the broker has written the snapshot twice.
    let mut last_written = broker.file_modification_time(SNAPSHOT_PATH);
    for _ in 0..2 {
        last_written = Some(polling::wait_until(SNAPSHOT_TIMEOUT, || {
            match broker.file_modification_time(SNAPSHOT_PATH) {
                Some(written) if Some(&written) != last_written.as_ref() => Ok(written),
                _ => Err(format!("the broker didn't write {SNAPSHOT_PATH} again")),
            }
        }));
    }

    // SQLite keeps the newest writes in the `-wal` file, and its index in the `-shm` file.
    let live_database_files = ["", "-wal", "-shm"].map(|suffix| format!("{live_database}{suffix}"));
    let broker = broker.restart_on_storage(
        StorageUrl::as_given(format!("sqlite://{SNAPSHOT_PATH}")),
        &live_database_files,
    );
    let cli = KafkaCli::new(broker.bootstrap());

    assert_eq!(
        cli.console_consumer()
            .read_records(&topic, 0, 0, values.len())
            .into_iter()
            .map(|record| record.value)
            .collect::<Vec<_>>(),
        values
    );
}
