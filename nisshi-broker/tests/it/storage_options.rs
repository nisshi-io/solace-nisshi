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

//! Storage URL query options at broker startup: the broker accepts its sweep
//! interval options on every engine, and rejects an invalid interval or an option
//! that the engine does not read.

use nisshi_broker::{
    Error, NODE_ID, Result, broker::Broker, coordinator::group::administrator::Controller,
};
use nisshi_storage::ArcDynStorage;
use std::assert_matches;
use url::Url;
use uuid::Uuid;

use crate::common::init_tracing;

async fn build(storage: &str) -> Result<()> {
    let listener = Url::parse("tcp://127.0.0.1:9092")?;

    Broker::<Controller<ArcDynStorage>, ArcDynStorage>::builder()
        .node_id(NODE_ID)
        .cluster_id(format!("storage-options-{}", Uuid::now_v7()))
        .incarnation_id(Uuid::now_v7())
        .advertised_listener(listener.clone())
        .storage(Url::parse(storage)?)
        .listener(listener)
        .silent(true)
        .build()
        .await
        .map(|_| ())
}

fn is_invalid_interval(error: &Error, expected_option: &str, expected_value: &str) -> bool {
    matches!(
        error,
        Error::InvalidStorageOptionValue { option, value }
            if option == expected_option && value == expected_value
    )
}

fn is_unrecognized(error: &Error, expected_scheme: &str, expected_option: &str) -> bool {
    matches!(
        error,
        Error::Storage(nisshi_storage::Error::UnrecognizedStorageOption { scheme, option })
            if scheme == expected_scheme && option == expected_option
    )
}

fn is_invalid_value(
    error: &Error,
    expected_scheme: &str,
    expected_option: &str,
    expected_value: &str,
) -> bool {
    matches!(
        error,
        Error::Storage(nisshi_storage::Error::InvalidStorageOptionValue {
            scheme, option, value, ..
        }) if scheme == expected_scheme && option == expected_option && value == expected_value
    )
}

#[tokio::test]
async fn memory_accepts_interval_options() -> Result<()> {
    let _guard = init_tracing()?;

    build("memory://?maintenance_interval=1m&transaction_maintenance_interval=5s").await
}

#[tokio::test]
async fn memory_rejects_zero_interval() -> Result<()> {
    let _guard = init_tracing()?;

    let error = build("memory://?maintenance_interval=0s")
        .await
        .unwrap_err();
    assert!(
        is_invalid_interval(&error, "maintenance_interval", "0s"),
        "{error:?}"
    );

    Ok(())
}

#[tokio::test]
async fn memory_rejects_misspelt_option() -> Result<()> {
    let _guard = init_tracing()?;

    let error = build("memory://?maintenance_intervl=1m").await.unwrap_err();
    assert!(
        is_unrecognized(&error, "memory", "maintenance_intervl"),
        "{error:?}"
    );

    Ok(())
}

#[cfg(feature = "postgres")]
mod postgres {
    use super::*;

    /// The engine rejects the pool size before it connects, so this test
    /// needs no server.
    #[tokio::test]
    async fn rejects_invalid_pool_max_size() -> Result<()> {
        let _guard = init_tracing()?;

        let error = build("postgres://postgres:postgres@localhost?pool_max_size=1")
            .await
            .unwrap_err();
        assert_matches!(
            &error,
            Error::Storage(nisshi_storage::Error::InvalidStorageOptionValue {
                scheme,
                option,
                value,
                ..
            }) if scheme == "postgres" && option == "pool_max_size" && value == "1"
        );

        Ok(())
    }
}

#[cfg(feature = "libsql")]
mod sqlite {
    use super::*;

    fn url(query: &str) -> String {
        format!(
            "sqlite://../logs/{}/storage-options-{}.db?{query}",
            env!("CARGO_PKG_NAME"),
            Uuid::now_v7()
        )
    }

    #[tokio::test]
    async fn accepts_interval_and_engine_options() -> Result<()> {
        let _guard = init_tracing()?;

        build(&url(concat!(
            "maintenance_interval=1m&busy_timeout=5s",
            "&vacuum_into=../logs/nisshi-broker/storage-options.vacuum.db"
        )))
        .await
    }

    #[tokio::test]
    async fn rejects_zero_interval() -> Result<()> {
        let _guard = init_tracing()?;

        let error = build(&url("maintenance_interval=0s")).await.unwrap_err();
        assert!(
            is_invalid_interval(&error, "maintenance_interval", "0s"),
            "{error:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn rejects_misspelt_option() -> Result<()> {
        let _guard = init_tracing()?;

        let error = build(&url("vacuum_onto=/tmp/x")).await.unwrap_err();
        assert!(
            is_unrecognized(&error, "sqlite", "vacuum_onto"),
            "{error:?}"
        );

        Ok(())
    }

    /// A value that the engine can't parse stops startup instead of the engine
    /// running with the default.
    #[tokio::test]
    async fn rejects_unparsable_engine_options() -> Result<()> {
        let _guard = init_tracing()?;

        for (option, value) in [
            ("busy_timeout", "banana"),
            ("compaction", "Single"),
            ("mode", "Direct"),
        ] {
            let error = build(&url(&format!("{option}={value}"))).await.unwrap_err();
            assert!(
                is_invalid_value(&error, "sqlite", option, value),
                "{option}={value}: {error:?}"
            );
        }

        Ok(())
    }
}
