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

//! The exit statuses of the client tools, against the `nisshi` binary: 0 success, 2 usage error,
//! 3 broker unreachable, 4 broker refused. docs/commands.md documents them.

#![cfg(feature = "dynostore")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use common::{Broker, run};
use std::{net::TcpListener, thread};

const TOOLS: [&[&str]; 5] = [
    &["topic", "list"],
    &["cat", "consume", "taxi"],
    &["user", "delete", "alice"],
    &[
        "generator",
        "taxi",
        "--per-second",
        "1",
        "--duration-seconds",
        "1",
    ],
    &["perf", "taxi", "produce", "--duration", "1s"],
];

#[test]
fn success_is_0() {
    let broker = Broker::start();

    assert_eq!(Some(0), broker.run(&["topic", "list"], "").status.code());
}

#[test]
fn a_wrong_command_line_is_a_usage_error_for_every_tool() {
    for tool in ["topic", "cat", "user", "generator", "perf"] {
        let output = run(&[tool, "--no-such-option"]);

        assert_eq!(Some(2), output.status.code(), "{tool}: {output:?}");
    }

    let output = run(&["topic", "list", "--output", "yaml"]);
    assert_eq!(Some(2), output.status.code(), "{output:?}");

    let output = run(&["topic", "create", "taxi", "--tls-cert-file", "cert.pem"]);
    assert_eq!(Some(2), output.status.code(), "{output:?}");
}

#[test]
fn an_unusable_tls_or_sasl_option_is_a_usage_error() {
    let output = run(&["topic", "list", "--sasl-username", "alice"]);

    assert_eq!(Some(2), output.status.code(), "{output:?}");
}

#[test]
fn a_refusal_by_the_broker_is_4() {
    let broker = Broker::start();
    _ = broker.run(&["topic", "create", "taxi"], "");

    for args in [
        &["topic", "create", "taxi"][..],
        &["topic", "delete", "missing"][..],
        &["user", "delete", "alice"][..],
        &["cat", "consume", "missing"][..],
    ] {
        let output = broker.run(args, "");

        assert_eq!(Some(4), output.status.code(), "{args:?}: {output:?}");
    }

    let output = broker.run(
        &["cat", "produce", "taxi", "--partition", "7"],
        "{\"value\": 1}\n",
    );
    assert_eq!(Some(4), output.status.code(), "{output:?}");
}

/// A TCP port with nothing listening on it.
fn closed_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|address| address.port())
        .expect("free port")
}

/// Runs each tool against a port nobody listens on. A client retries a refused connection for 30
/// seconds before it gives up, so the tools run side by side.
#[test]
fn a_broker_that_is_not_there_is_3_for_every_client_tool() {
    let url = format!("tcp://127.0.0.1:{}", closed_port());
    let registry = std::env::temp_dir().join(format!("nisshi-exit-codes-{}", std::process::id()));
    std::fs::create_dir_all(&registry).expect("registry directory");
    _ = std::fs::copy("../etc/schema/taxi.proto", registry.join("taxi.proto")).expect("schema");
    let schema_registry = format!(
        "file://./{}",
        registry.file_name().unwrap().to_str().unwrap()
    );

    let runs = TOOLS.map(|args| {
        let mut args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
        // `perf` takes `--broker` before its subcommand, the others after theirs.
        if args[0] == "perf" {
            args.insert(1, url.clone());
            args.insert(1, "--broker".into());
        } else {
            args.extend(["--broker".into(), url.clone()]);
        }
        if args[0] == "generator" {
            args.extend(["--schema-registry".into(), schema_registry.clone()]);
        }
        thread::spawn(move || {
            let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
            (args.clone(), run(&borrowed))
        })
    });

    let results: Vec<_> = runs
        .into_iter()
        .map(|run| run.join().expect("run"))
        .collect();
    _ = std::fs::remove_dir_all(&registry);

    for (args, output) in results {
        assert_eq!(Some(3), output.status.code(), "{args:?}: {output:?}");
    }
}
