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

//! Tests a broker that starts on a database without tables.
//!
//! The broker must create its tables at startup, because the user does not run a setup step. A
//! broker that starts again on that database must keep what was stored there.

#[cfg(any(feature = "postgres", feature = "sqlite"))]
use nisshi_smoke_test::{Broker, KafkaCli, LaunchOptions, StorageUrl};

#[cfg(any(feature = "postgres", feature = "sqlite"))]
const VALUES: [&str; 2] = ["before-the-restart-0", "before-the-restart-1"];

/// Checks that a broker creates its tables in the empty `storage` and keeps the stored [`VALUES`]
/// across a restart.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
#[track_caller]
fn creates_its_tables_and_keeps_them_across_a_restart(storage: StorageUrl) {
    let broker = Broker::launch(LaunchOptions::new(storage));
    let cli = KafkaCli::new(broker.bootstrap());
    let topic = cli.topics().create_unique(1, &[]);
    _ = cli
        .console_producer()
        .produce_values(&topic, &VALUES)
        .succeeded();

    let broker = broker.restart();
    let cli = KafkaCli::new(broker.bootstrap());

    assert_eq!(
        cli.console_consumer()
            .read_records(&topic, 0, 0, VALUES.len())
            .into_iter()
            .map(|record| record.value)
            .collect::<Vec<_>>(),
        VALUES
    );
}

#[cfg(feature = "postgres")]
mod postgres {
    use nisshi_smoke_test::PostgresServer;

    /// A broker must create its tables in the empty database of a new PostgreSQL server.
    #[test]
    fn broker_creates_its_tables_on_an_empty_database() {
        let postgres = PostgresServer::start();

        super::creates_its_tables_and_keeps_them_across_a_restart(postgres.storage_url());
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use nisshi_smoke_test::{StorageUrl, unique};

    /// A broker must create its tables in a SQLite file that doesn't exist yet. The relative path
    /// puts the file in the broker's own working directory.
    #[test]
    fn broker_creates_its_tables_in_a_new_file() {
        let storage = StorageUrl::as_given(format!("sqlite://{}.db", unique::name("fresh")));

        super::creates_its_tables_and_keeps_them_across_a_restart(storage);
    }
}
