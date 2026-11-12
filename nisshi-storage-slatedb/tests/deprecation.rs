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

//! Checks that selecting the `slatedb://` storage engine logs a deprecation warning,
//! and that the warning does not repeat the storage URL.

use std::{
    error::Error,
    io,
    sync::{Arc, Mutex},
};

use nisshi_storage::StorageContainer;
use nisshi_storage_slatedb::EngineFactory;
use tracing::Level;
use url::Url;

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|err| io::Error::other(err.to_string()))?
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Capture {
    fn text(&self) -> io::Result<String> {
        let written = self
            .0
            .lock()
            .map_err(|err| io::Error::other(err.to_string()))?;

        Ok(String::from_utf8_lossy(&written).into_owned())
    }
}

const PASSWORD: &str = "hunter2-slatedb-secret";

#[tokio::test]
async fn slatedb_storage_logs_a_deprecation_warning_without_the_url() -> Result<(), Box<dyn Error>>
{
    let capture = Capture::default();
    let writer = capture.clone();

    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish(),
    );

    let mut builder = StorageContainer::builder()
        .node_id(111)
        .cluster_id("deprecation-test")
        .advertised_listener(Url::parse("tcp://127.0.0.1:9092")?)
        .storage(Url::parse(&format!("slatedb://user:{PASSWORD}@memory"))?)
        .silent(true);
    builder.with_factory(Arc::new(EngineFactory));

    let _storage = builder.build().await?;

    let logs = capture.text()?;

    let warning = logs
        .lines()
        .find(|line| line.contains("WARN") && line.contains("deprecated"))
        .unwrap_or_else(|| panic!("no deprecation warning in the log:\n{logs}"));

    assert!(warning.contains("slatedb"), "{warning}");
    assert!(!logs.contains(PASSWORD), "the log holds the URL password");

    Ok(())
}
