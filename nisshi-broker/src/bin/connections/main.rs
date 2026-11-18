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

//! Reports the connections that each client opened, from the `nisshi::connection` events in a
//! broker log.
//!
//! The broker logs these events when it runs with `RUST_LOG=warn,nisshi::connection=info`. The
//! last event of a connection is its record: the close event when the log has one, otherwise the
//! latest snapshot, for a connection that was still open when the broker stopped.
//!
//! The report groups the records by `client_id`. A Kafka client numbers its id (`producer-1`,
//! `consumer-2`), so the kind of a client is its id without the trailing number. A connection that
//! closed before it sent a request has no `client_id`.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod connection;
mod csv;
mod event;
mod log;
mod report;
mod table;

use std::{fs, path::PathBuf};

use anyhow::{Context as _, Result, bail};
use clap::Parser;
use nisshi_service::CONNECTION_TARGET;

use crate::{csv::Csv, log::Log, report::Report};

#[derive(Clone, Debug, Parser)]
struct Arg {
    /// The broker log to read
    #[arg(long, default_value = "broker.log")]
    filename: PathBuf,

    /// List every connection after the per-client table
    #[arg(long = "connections")]
    per_connection: bool,

    /// Write one row per connection to this CSV file
    #[arg(long)]
    csv: Option<PathBuf>,
}

fn main() -> Result<()> {
    let arg = Arg::parse();

    let bytes =
        fs::read(&arg.filename).with_context(|| format!("reading {}", arg.filename.display()))?;
    let log = Log::read(&String::from_utf8_lossy(&bytes))?;

    if log.is_empty() {
        bail!(
            "no {CONNECTION_TARGET} events in {}; start the broker with \
             RUST_LOG=warn,{CONNECTION_TARGET}=info",
            arg.filename.display()
        );
    }

    println!("{}", Report::new(&log, arg.per_connection));

    if let Some(path) = arg.csv {
        fs::write(&path, Csv::new(&log).to_string())
            .with_context(|| format!("writing {}", path.display()))?;
    }

    Ok(())
}
