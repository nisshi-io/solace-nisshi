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

//! [`ProduceService`](nisshi_storage::ProduceService) validates each batch against the topic's
//! schema, on every storage engine, because the engines do not validate it themselves.

use bytes::Bytes;
use nisshi_broker::{Error, Result};
use nisshi_sans_io::{
    ErrorCode,
    create_topics_request::{CreatableTopic, CreatableTopicConfig},
    record::{Record, deflated, inflated},
    topic_config,
};
use nisshi_schema::Registry;
use nisshi_storage::{ArcDynStorage, Storage};
use rand::{prelude::*, rng};
use serde_json::json;
use url::Url;
use uuid::Uuid;

use crate::{
    common::{self, StorageType, register_broker},
    produce::produce_batches_with_schemas,
};

const TOPIC: &str = "person";

fn person_schemas() -> Result<Registry> {
    Url::parse("file://../etc/schema")
        .map_err(Error::from)
        .and_then(|url| {
            Registry::builder_try_from_url(&url)
                .map(|builder| builder.build())
                .map_err(Into::into)
        })
}

fn person_batch(age: i64) -> Result<deflated::Batch> {
    inflated::Batch::builder()
        .record(
            Record::builder()
                .key(Some(Bytes::from_static(b"\"345-67-6543\"")))
                .value(Some(Bytes::from(serde_json::to_vec(&json!({
                    "firstName": "John",
                    "lastName": "Doe",
                    "age": age,
                }))?))),
        )
        .build()
        .and_then(TryInto::try_into)
        .map_err(Into::into)
}

/// Creates the `person` topic with `configs` in a storage engine of `storage_type`, which holds
/// the person schema as well, and produces a person of `age` through a service that has
/// `schemas`. Returns the error code of the partition's response.
async fn produce_person(
    storage_type: StorageType,
    configs: Vec<CreatableTopicConfig>,
    schemas: Option<Registry>,
    age: i64,
) -> Result<ErrorCode> {
    let cluster = Uuid::now_v7();
    let node = rng().random_range(0..i32::MAX);

    let storage: ArcDynStorage = common::storage_container(
        storage_type,
        cluster,
        node,
        Url::parse("tcp://127.0.0.1/")?,
        Some(person_schemas()?),
    )
    .await?;
    register_broker(cluster, node, &storage).await?;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(TOPIC.into())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some(configs)),
            false,
        )
        .await?;

    let response =
        produce_batches_with_schemas(storage, schemas, TOPIC, 0, vec![person_batch(age)?]).await?;

    ErrorCode::try_from(response.error_code).map_err(Into::into)
}

fn schema_validation(value: &str) -> CreatableTopicConfig {
    CreatableTopicConfig::default()
        .name(topic_config::SCHEMA_VALIDATION.into())
        .value(Some(value.into()))
}

macro_rules! engine {
    ($module:ident, $feature:literal, $storage_type:expr) => {
        #[cfg(feature = $feature)]
        mod $module {
            use super::*;

            #[tokio::test]
            async fn a_record_that_matches_the_schema_is_stored() -> Result<()> {
                assert_eq!(
                    ErrorCode::None,
                    produce_person($storage_type, vec![], Some(person_schemas()?), 21).await?
                );
                Ok(())
            }

            #[tokio::test]
            async fn a_record_that_breaks_the_schema_is_an_invalid_record() -> Result<()> {
                assert_eq!(
                    ErrorCode::InvalidRecord,
                    produce_person($storage_type, vec![], Some(person_schemas()?), -1).await?
                );
                Ok(())
            }

            #[tokio::test]
            async fn schema_validation_false_stores_a_record_that_breaks_the_schema() -> Result<()> {
                assert_eq!(
                    ErrorCode::None,
                    produce_person(
                        $storage_type,
                        vec![schema_validation("false")],
                        Some(person_schemas()?),
                        -1
                    )
                    .await?
                );
                Ok(())
            }

            #[tokio::test]
            async fn a_service_without_schemas_stores_a_record_that_breaks_the_schema() -> Result<()>
            {
                assert_eq!(
                    ErrorCode::None,
                    produce_person($storage_type, vec![], None, -1).await?
                );
                Ok(())
            }
        }
    };
}

engine!(in_memory, "dynostore", StorageType::InMemory);
engine!(lite, "libsql", StorageType::Lite);
engine!(pg, "postgres", StorageType::Postgres);
