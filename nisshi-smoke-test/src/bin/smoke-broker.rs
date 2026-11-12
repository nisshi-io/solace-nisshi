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

//! `smoke-broker -- <command>...` starts the broker the smoke tests share,
//! runs the command with `NISSHI_SMOKE_BOOTSTRAP` pointing at it, then stops
//! the broker. The run fails if the command fails, or if the broker exited,
//! panicked or didn't exit with 0 on SIGTERM.
//!
//! nextest runs each test in its own process, so a test cannot own a broker that the other tests
//! share. `just smoke` owns the shared broker through smoke-broker instead.
//!
//! The smoke report has one row per test, so smoke-broker adds a row for each failure that the
//! tests do not report. The `shared_broker` row is FAIL if the broker fails its checks, even if
//! every test passes. The `suite` row is FAIL if the command fails, including a run that stopped
//! before any test reported. run.sh parses these rows from the `smoke-broker: result` lines.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::process::{Command, ExitCode};

use clap::Parser;
use nisshi_smoke_test::{Broker, LaunchOptions, SHARED_CLUSTER_ID, settings};

#[derive(Debug, Parser)]
struct Arguments {
    /// The command to run against the shared broker, after `--`.
    #[arg(last = true, required = true)]
    command: Vec<String>,
}

fn main() -> ExitCode {
    let Arguments { command } = Arguments::parse();
    let (program, args) = command
        .split_first()
        .expect("clap requires at least one command argument");

    let broker = Broker::launch(LaunchOptions {
        cluster_id: SHARED_CLUSTER_ID.to_owned(),
        log: settings::shared_broker_log(),
        ..LaunchOptions::new(settings::storage_url_under_test())
    });

    let status = Command::new(program)
        .args(args)
        .env("NISSHI_SMOKE_BOOTSTRAP", broker.bootstrap())
        .status();

    let stopped = broker.stop();

    let mut code = ExitCode::SUCCESS;

    let suite = match status {
        Ok(status) if status.success() => "PASS",

        Ok(status) => {
            eprintln!("smoke-broker: {program} exited with {status}");
            code = ExitCode::FAILURE;
            "FAIL"
        }

        Err(err) => {
            eprintln!("smoke-broker: could not run {program}: {err}");
            code = ExitCode::FAILURE;
            "FAIL"
        }
    };

    eprintln!("smoke-broker: result suite,{suite}");

    let outcome = match stopped {
        Ok(()) => "PASS",

        Err(reason) => {
            eprintln!("smoke-broker: {reason}");
            code = ExitCode::FAILURE;
            "FAIL"
        }
    };

    eprintln!("smoke-broker: result shared_broker,{outcome}");

    code
}
