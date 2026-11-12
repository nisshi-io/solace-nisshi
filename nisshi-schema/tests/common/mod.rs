// Copyright (c) Solace Corporation
//
// Contributors outside Solace:
//   Enwei Jiao
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

use nisshi_schema::{Error, Result};
use rand::{distr::Alphanumeric, prelude::*, rng};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

pub(crate) fn init_tracing() -> Result<DefaultGuard> {
    use std::{fs::File, sync::Arc, thread};

    Ok(tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_level(true)
            .with_line_number(true)
            .with_thread_names(false)
            .with_env_filter(
                EnvFilter::from_default_env()
                    .add_directive(format!("{}=debug", env!("CARGO_CRATE_NAME")).parse()?),
            )
            .with_writer(
                thread::current()
                    .name()
                    .ok_or(Error::Message(String::from("unnamed thread")))
                    .and_then(|name| {
                        File::create(format!(
                            "../logs/{}/{}::{name}.log",
                            env!("CARGO_PKG_NAME"),
                            env!("CARGO_CRATE_NAME")
                        ))
                        .map_err(Into::into)
                    })
                    .map(Arc::new)?,
            )
            .finish(),
    ))
}

pub(crate) fn alphanumeric_string(length: usize) -> String {
    // Ensure first character is a letter (valid SQL identifier)
    let first: char = rng()
        .sample_iter(&Alphanumeric)
        .map(char::from)
        .find(|c| c.is_ascii_alphabetic())
        .unwrap_or('a');

    let rest: String = rng()
        .sample_iter(&Alphanumeric)
        .take(length.saturating_sub(1))
        .map(char::from)
        .collect();

    format!("{first}{rest}").to_lowercase()
}
