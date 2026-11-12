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

use std::{process::Command, time::Duration};

use crate::{common::timed_command, settings};

/// Runs a short `docker` command, such as `docker inspect`, and kills it if it hangs.
pub(crate) fn execute(args: &[&str]) -> Result<timed_command::Finished, String> {
    execute_within(args, Duration::from_secs(60))
}

/// Runs a short `docker` command, and kills it if it takes longer than `timeout`.
pub(crate) fn execute_within(
    args: &[&str],
    timeout: Duration,
) -> Result<timed_command::Finished, String> {
    timed_command::run(Command::new("docker").args(args), None, timeout)
}

pub(crate) fn inspect(name: &str, format: &str) -> Result<String, String> {
    inspect_within(name, format, Duration::from_secs(60))
}

/// Like [`inspect`], and kills `docker inspect` if it takes longer than `timeout`.
pub(crate) fn inspect_within(
    name: &str,
    format: &str,
    timeout: Duration,
) -> Result<String, String> {
    match execute_within(&["inspect", "--format", format, name], timeout)? {
        inspected if inspected.code == Some(0) => Ok(inspected.stdout.trim().to_owned()),
        inspected => Err(format!(
            "docker inspect {name}: {}",
            inspected.stderr.trim()
        )),
    }
}

/// The `docker --label` on every container and volume this run creates.
pub(crate) fn label() -> String {
    format!("--label=nisshi-smoke={}", settings::run_id())
}
