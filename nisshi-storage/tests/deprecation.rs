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

//! Checks that `StorageContainer` logs a factory's deprecation notice when it selects
//! that factory, and logs nothing for a factory that has no notice.

use std::{
    assert_matches,
    error::Error as StdError,
    fmt, io,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use nisshi_storage::{
    ArcDynStorage, Error, Result, StorageCapabilities, StorageContainer, StorageFactory,
    StorageFactoryConfiguration,
};
use regex::Regex;
use tracing::Level;
use url::Url;

const NOTICE: &str = "the old:// storage engine is deprecated";
const PASSWORD: &str = "hunter2-old-secret";

#[derive(Debug)]
struct OldEngine {
    notice: Option<&'static str>,
}

impl fmt::Display for OldEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("old")
    }
}

#[async_trait]
impl StorageFactory for OldEngine {
    fn scheme(&self) -> Result<Regex> {
        Regex::new("old").map_err(Into::into)
    }

    fn capabilities(&self) -> StorageCapabilities {
        StorageCapabilities {
            user_credentials: false,
        }
    }

    fn deprecation(&self) -> Option<&'static str> {
        self.notice
    }

    async fn build(&self, _configuration: StorageFactoryConfiguration) -> Result<ArcDynStorage> {
        Err(Error::Message("this test engine stores nothing".into()))
    }
}

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

/// Selects `engine` for an `old://` URL that carries a password, and returns the log
/// and the build result.
async fn select(engine: OldEngine) -> Result<(String, Result<ArcDynStorage>), Box<dyn StdError>> {
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
        .storage(Url::parse(&format!("old://user:{PASSWORD}@host"))?)
        .silent(true);
    builder.with_factory(Arc::new(engine));

    let built = builder.build().await;

    let logs = String::from_utf8_lossy(
        &capture
            .0
            .lock()
            .map_err(|err| io::Error::other(err.to_string()))?,
    )
    .into_owned();

    Ok((logs, built))
}

#[tokio::test]
async fn deprecated_factory_logs_its_notice_as_a_warning_without_the_url()
-> Result<(), Box<dyn StdError>> {
    let (logs, built) = select(OldEngine {
        notice: Some(NOTICE),
    })
    .await?;

    assert_matches!(built, Err(Error::Message(_)));
    assert!(
        logs.lines()
            .any(|line| line.contains("WARN") && line.contains(NOTICE)),
        "no warning in the log:\n{logs}"
    );
    assert!(!logs.contains(PASSWORD), "the log holds the URL password");

    Ok(())
}

#[tokio::test]
async fn factory_without_a_notice_logs_no_warning() -> Result<(), Box<dyn StdError>> {
    let (logs, built) = select(OldEngine { notice: None }).await?;

    assert_matches!(built, Err(Error::Message(_)));
    assert!(
        !logs.contains("WARN"),
        "unexpected warning in the log:\n{logs}"
    );

    Ok(())
}
