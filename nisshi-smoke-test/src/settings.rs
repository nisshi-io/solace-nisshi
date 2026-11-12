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

//! The settings that `just smoke <engine>` passes to the suite through environment variables.
//! nextest runs each test in its own process, so the environment is how a test gets them.
//!
//! Each function reads one variable, and panics if a variable the suite needs is unset. The suite
//! then fails at once, instead of running with a value that `just smoke` didn't choose.

use std::path::PathBuf;

use crate::StorageUrl;

/// Returns the URL of the storage engine under test, from `NISSHI_SMOKE_STORAGE`.
///
/// A SQLite URL must hold a relative path. Each broker resolves the path in a directory of its
/// own, so each SQLite broker gets its own database. PostgreSQL and S3 brokers share the engine's
/// storage, and their cluster ids keep it apart.
pub fn storage_url_under_test() -> StorageUrl {
    let storage = required("NISSHI_SMOKE_STORAGE");

    // The broker reads a path after `sqlite:///` as relative, so a fourth slash starts an
    // absolute path.
    assert!(
        !storage.starts_with("sqlite:////"),
        "NISSHI_SMOKE_STORAGE {storage} must hold a relative path, or every SQLite broker would \
         share one database"
    );

    StorageUrl::as_given(storage)
}

/// Returns the address of the broker that all tests share, from `NISSHI_SMOKE_BOOTSTRAP`, which
/// smoke-broker sets for the tests.
pub fn shared_broker_bootstrap() -> String {
    required("NISSHI_SMOKE_BOOTSTRAP")
}

/// Returns where to keep the shared broker's log, from `NISSHI_SMOKE_LOG`, or `None` to keep it in
/// the broker's own directory.
pub fn shared_broker_log() -> Option<PathBuf> {
    optional("NISSHI_SMOKE_LOG").map(PathBuf::from)
}

/// Returns the Docker image to launch brokers from, from `NISSHI_SMOKE_IMAGE`, or `None` to launch
/// [`broker_binary`] as a process.
pub(crate) fn broker_image() -> Option<String> {
    optional("NISSHI_SMOKE_IMAGE")
}

/// Returns the `nisshi` binary to launch brokers from, from `NISSHI_SMOKE_BIN`, when
/// [`broker_image`] is unset.
pub(crate) fn broker_binary() -> String {
    optional("NISSHI_SMOKE_BIN").unwrap_or_else(|| {
        panic!("set NISSHI_SMOKE_BIN or NISSHI_SMOKE_IMAGE: {RUN_WITH_JUST_SMOKE}")
    })
}

/// Returns the name of the running container with the Kafka CLI tools, from `NISSHI_SMOKE_KAFKA`.
pub(crate) fn kafka_container() -> String {
    required("NISSHI_SMOKE_KAFKA")
}

/// Returns the run's id, from `NISSHI_SMOKE_RUN`. It labels every container and volume the suite
/// creates, so a run removes only its own.
pub(crate) fn run_id() -> String {
    required("NISSHI_SMOKE_RUN")
}

/// Returns the directory where each broker a test launches keeps its files and logs, from
/// `NISSHI_SMOKE_WORK_DIR`.
pub(crate) fn broker_work_dir() -> PathBuf {
    PathBuf::from(required("NISSHI_SMOKE_WORK_DIR"))
}

/// Returns the image of the PostgreSQL server that a test starts for itself, from
/// `NISSHI_SMOKE_POSTGRES_IMAGE`.
pub(crate) fn postgres_image() -> String {
    required("NISSHI_SMOKE_POSTGRES_IMAGE")
}

/// Returns the directory of the files that reserve ports, from `NISSHI_SMOKE_PORT_LOCKS`. Every
/// run of the same user shares it.
pub(crate) fn port_lock_dir() -> PathBuf {
    PathBuf::from(required("NISSHI_SMOKE_PORT_LOCKS"))
}

const RUN_WITH_JUST_SMOKE: &str = "run the suite with `just smoke <engine>`";

fn required(name: &str) -> String {
    optional(name).unwrap_or_else(|| panic!("{name} is not set: {RUN_WITH_JUST_SMOKE}"))
}

fn optional(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}
