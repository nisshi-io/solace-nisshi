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

//! Tests a broker that starts while its storage is down, as when an orchestrator starts the broker
//! before its database.
//!
//! The broker must exit with an error that names the storage, so that the orchestrator restarts
//! it, and it must start once the storage is up.

#[cfg(feature = "postgres")]
mod postgres {
    use std::time::{Duration, Instant};

    use nisshi_smoke_test::{Broker, KafkaCli, LaunchOptions, PostgresServer, unique};

    /// How long a broker whose storage is down may take to exit. The broker checks its storage
    /// once at startup, and exits when the check fails, instead of retrying. The limit allows for
    /// the time to launch a broker container.
    const EXIT_LIMIT: Duration = Duration::from_secs(15);

    const VALUES: [&str; 1] = ["after-the-server-started"];

    /// A broker whose PostgreSQL server is stopped must exit within [`EXIT_LIMIT`], with an error
    /// that names the server's address. Once the server is up, a broker on the same storage must
    /// start and serve.
    #[test]
    fn broker_exits_while_the_server_is_down_and_starts_once_it_is_up() {
        let postgres = PostgresServer::start();
        postgres.stop();

        let launched_at = Instant::now();
        let refused = Broker::launch_expecting_refusal(LaunchOptions::new(postgres.storage_url()));
        let exited_after = launched_at.elapsed();

        assert!(
            exited_after <= EXIT_LIMIT,
            "the broker took {exited_after:?} to exit, more than {EXIT_LIMIT:?}: {refused}"
        );
        refused.assert_error_names(&postgres.address());

        postgres.start_again();
        let broker = Broker::launch(LaunchOptions::new(postgres.storage_url()));
        let cli = KafkaCli::new(broker.bootstrap());
        let topic = cli.topics().create_unique(1, &[]);

        _ = cli
            .console_producer()
            .produce_values(&topic, &VALUES)
            .succeeded();
        let consumed = cli
            .console_consumer()
            .consume(&topic, &unique::name("group"), VALUES.len());
        assert_eq!(consumed.succeeded().consumed_values(), VALUES, "{consumed}");

        if let Err(reason) = broker.stop() {
            panic!("{reason}");
        }
    }
}
