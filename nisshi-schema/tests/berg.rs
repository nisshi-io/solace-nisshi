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

#![allow(clippy::unwrap_used, clippy::expect_used)]
use crate::common::alphanumeric_string;
use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
use bytes::Bytes;
use common::init_tracing;
use datafusion::prelude::SessionContext;
use dotenv::dotenv;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_datafusion::IcebergStaticTableProvider;
use nisshi_sans_io::{
    ConfigResource, ErrorCode,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    record::{Record, inflated::Batch},
};
use nisshi_schema::{
    Error, Registry, Result,
    lake::{House, LakeHouse, berg::iceberg_catalog},
};
use object_store::{ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path};
use serde_json::{Value as JsonValue, json};
use std::{env::var, sync::Arc};
use tracing::debug;
use url::Url;

pub mod common;

fn catalog_url() -> Result<Url> {
    Url::parse(&var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into()))
        .map_err(Into::into)
}

pub async fn lake_store(
    namespace: &str,
    topic: &str,
    partition: i32,
    schema_registry: Registry,
    config: DescribeConfigsResult,
    inflated: &Batch,
) -> Result<Vec<RecordBatch>> {
    lake_store_with_catalog(
        &catalog_url()?,
        namespace,
        topic,
        partition,
        schema_registry,
        config,
        inflated,
    )
    .await
}

/// Stores the batch in the lake through the catalog at `catalog_url`, and
/// returns the rows of the topic's table.
async fn lake_store_with_catalog(
    catalog_url: &Url,
    namespace: &str,
    topic: &str,
    partition: i32,
    schema_registry: Registry,
    config: DescribeConfigsResult,
    inflated: &Batch,
) -> Result<Vec<RecordBatch>> {
    let catalog_uri = catalog_url.as_str();
    let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
    let warehouse = var("ICEBERG_WAREHOUSE").ok();

    debug!(catalog_uri, location_uri, ?warehouse, namespace, ?inflated);

    let lake_house = House::iceberg()
        .location(Url::parse(location_uri)?)
        .catalog(Url::parse(catalog_uri)?)
        .namespace(Some(namespace.to_owned()))
        .warehouse(warehouse.clone())
        .schema_registry(schema_registry)
        .build()
        .await?;

    let offset = 543212345;

    // Build the same catalog (storage factory, path-style pin and all)
    // that nisshi itself uses, rather than re-deriving a `RestCatalogBuilder`
    // call here that could drift from `iceberg_catalog`'s.
    let catalog = iceberg_catalog(&Url::parse(catalog_uri)?, warehouse.clone()).await?;

    let namespace_ident = NamespaceIdent::new(namespace.to_owned());
    let results = async {
        lake_house
            .store(topic, partition, offset, inflated, config)
            .await
            .inspect(|result| debug!(?result))
            .inspect_err(|err| debug!(?err))?;

        // The test loads only the topic's table. A catalog provider for the
        // whole catalog would load every table of every namespace at once,
        // and with many tests running, that opens hundreds of connections to
        // the catalog.
        let table = catalog
            .load_table(&TableIdent::new(namespace_ident.clone(), topic.to_owned()))
            .await?;
        let ctx = SessionContext::new();
        _ = ctx.register_table(
            topic,
            Arc::new(IcebergStaticTableProvider::try_new_from_table(table).await?),
        )?;
        ctx.sql(&format!("select * from {topic}"))
            .await?
            .collect()
            .await
            .map_err(Error::from)
    }
    .await;

    // The namespace is removed before the caller checks the results, so a
    // test that fails leaves no namespace behind either. A store that failed
    // can fail before it creates the namespace.
    let cleanup = async {
        if catalog.namespace_exists(&namespace_ident).await? {
            drop_namespace_with_tables(catalog.as_ref(), &namespace_ident).await?;
        }
        Ok::<_, Error>(())
    }
    .await
    .inspect_err(|err| debug!(?err, "cleanup"));

    // The store's or the query's error is the one that explains a failed test,
    // so it wins over an error of the cleanup.
    results.and_then(|results| cleanup.map(|()| results))
}

/// Removes the namespace and purges each of its tables, so that the catalog
/// keeps no table or namespace that a test created.
async fn drop_namespace_with_tables(
    catalog: &dyn Catalog,
    namespace: &NamespaceIdent,
) -> Result<()> {
    for table in catalog.list_tables(namespace).await? {
        catalog.purge_table(&table).await?;
    }
    catalog.drop_namespace(namespace).await.map_err(Into::into)
}

fn empty_config(topic: &str) -> DescribeConfigsResult {
    DescribeConfigsResult::default()
        .error_code(ErrorCode::None.into())
        .error_message(None)
        .resource_type(ConfigResource::Topic.into())
        .resource_name(topic.into())
        .configs(Some(vec![]))
}

fn normalized_config(topic: &str) -> DescribeConfigsResult {
    DescribeConfigsResult::default()
        .error_code(ErrorCode::None.into())
        .error_message(None)
        .resource_type(ConfigResource::Topic.into())
        .resource_name(topic.into())
        .configs(Some(
            [DescribeConfigsResourceResult::default()
                .name(String::from("tansu.lake.normalize"))
                .value(Some(String::from("true")))
                .read_only(true)
                .is_default(None)
                .config_source(None)
                .is_sensitive(false)
                .synonyms(None)
                .config_type(None)
                .documentation(None)]
            .into(),
        ))
}

mod json {
    use super::*;

    #[tokio::test]
    async fn key_and_value() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];
        let namespace = &alphanumeric_string(5)[..];

        let schema = json!({
            "type": "object",
            "properties": {
                "key": {
                    "type": "number"
                },
                "value": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                        },
                        "email": {
                            "type": "string",
                            "format": "email"
                        }
                    }
                }
            }
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.json"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&schema)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let kv = [
            (
                json!(12321),
                json!({"name": "alice", "email": "alice@example.com"}),
            ),
            (
                json!(32123),
                json!({"name": "bob", "email": "bob@example.com"}),
            ),
        ];

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for (ref key, ref value) in kv {
                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                        .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-----------------------------------------------------------------------------------------+-------+-----------------------------------------+",
            "| meta                                                                                    | key   | value                                   |",
            "+-----------------------------------------------------------------------------------------+-------+-----------------------------------------+",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 12321 | {email: alice@example.com, name: alice} |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 32123 | {email: bob@example.com, name: bob}     |",
            "+-----------------------------------------------------------------------------------------+-------+-----------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn grade() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let topic = &alphanumeric_string(5)[..];
        let namespace = &alphanumeric_string(5)[..];

        let schema_registry = {
            let schema = Bytes::from_static(include_bytes!("../../etc/schema/grade.json"));

            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.json"));
            _ = object_store
                .put(&location, PutPayload::from(schema))
                .await?;

            Registry::new(object_store)
        };

        let kv = if let JsonValue::Array(values) =
            serde_json::from_slice::<JsonValue>(include_bytes!("../../etc/data/grades.json"))?
        {
            values
                .into_iter()
                .map(|value| {
                    (
                        value.get("key").cloned().unwrap(),
                        value.get("value").cloned().unwrap(),
                    )
                })
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for (ref key, ref value) in kv {
                debug!(?key, ?value);

                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                        .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-----------------------------------------------------------------------------------------+-------------+---------------------------------------------------------------------------------------------------------------+",
            "| meta                                                                                    | key         | value                                                                                                         |",
            "+-----------------------------------------------------------------------------------------+-------------+---------------------------------------------------------------------------------------------------------------+",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 123-45-6789 | {final: 49.0, first: Aloysius, grade: D-, last: Alfalfa, test1: 40.0, test2: 90.0, test3: 100.0, test4: 83.0} |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 123-12-1234 | {final: 48.0, first: University, grade: D+, last: Alfred, test1: 41.0, test2: 97.0, test3: 96.0, test4: 97.0} |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 567-89-0123 | {final: 44.0, first: Gramma, grade: C, last: Gerty, test1: 41.0, test2: 80.0, test3: 60.0, test4: 40.0}       |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 087-65-4321 | {final: 47.0, first: Electric, grade: B-, last: Android, test1: 42.0, test2: 23.0, test3: 36.0, test4: 45.0}  |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 456-78-9012 | {final: 45.0, first: Fred, grade: A-, last: Bumpkin, test1: 43.0, test2: 78.0, test3: 88.0, test4: 77.0}      |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 234-56-7890 | {final: 46.0, first: Betty, grade: C-, last: Rubble, test1: 44.0, test2: 90.0, test3: 80.0, test4: 90.0}      |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 345-67-8901 | {final: 43.0, first: Cecil, grade: F, last: Noshow, test1: 45.0, test2: 11.0, test3: -1.0, test4: 4.0}        |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 632-79-9939 | {final: 50.0, first: Bif, grade: B+, last: Buff, test1: 46.0, test2: 20.0, test3: 30.0, test4: 40.0}          |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 223-45-6789 | {final: 83.0, first: Andrew, grade: A, last: Airpump, test1: 49.0, test2: 1.0, test3: 90.0, test4: 100.0}     |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 143-12-1234 | {final: 97.0, first: Jim, grade: A+, last: Backus, test1: 48.0, test2: 1.0, test3: 97.0, test4: 96.0}         |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 565-89-0123 | {final: 40.0, first: Art, grade: D+, last: Carnivore, test1: 44.0, test2: 1.0, test3: 80.0, test4: 60.0}      |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 087-75-4321 | {final: 45.0, first: Jim, grade: C+, last: Dandy, test1: 47.0, test2: 1.0, test3: 23.0, test4: 36.0}          |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 456-71-9012 | {final: 77.0, first: Ima, grade: B-, last: Elephant, test1: 45.0, test2: 1.0, test3: 78.0, test4: 88.0}       |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 234-56-2890 | {final: 90.0, first: Benny, grade: B-, last: Franklin, test1: 50.0, test2: 1.0, test3: 90.0, test4: 80.0}     |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 345-67-3901 | {final: 4.0, first: Boy, grade: B, last: George, test1: 40.0, test2: 1.0, test3: 11.0, test4: -1.0}           |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 632-79-9439 | {final: 40.0, first: Harvey, grade: C, last: Heffalump, test1: 30.0, test2: 1.0, test3: 20.0, test4: 30.0}    |",
            "+-----------------------------------------------------------------------------------------+-------------+---------------------------------------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn key() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let schema_registry = {
            let schema = json!({
                "type": "object",
                "properties": {
                    "key": {
                        "type": "number"
                    }
                }
            });

            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.json"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&schema)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let keys = [json!(12321), json!(23432), json!(34543)];
        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for ref key in keys {
                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-----------------------------------------------------------------------------------------+-------+",
            "| meta                                                                                    | key   |",
            "+-----------------------------------------------------------------------------------------+-------+",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 12321 |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 23432 |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 34543 |",
            "+-----------------------------------------------------------------------------------------+-------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn primitive_key_and_value() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let schema_registry = {
            let schema = json!({
                "type": "object",
                "properties": {
                    "key": {
                        "type": "number"
                    },
                    "value": {
                        "type": "string",
                    }
                }
            });

            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.json"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&schema)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let kv = [
            (json!(12321), json!("alice@example.com")),
            (json!(32123), json!("bob@example.com")),
        ];

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for (ref key, ref value) in kv {
                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                        .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-----------------------------------------------------------------------------------------+-------+-------------------+",
            "| meta                                                                                    | key   | value             |",
            "+-----------------------------------------------------------------------------------------+-------+-------------------+",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 12321 | alice@example.com |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 32123 | bob@example.com   |",
            "+-----------------------------------------------------------------------------------------+-------+-------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn primitive_key_and_array_value() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let schema_registry = {
            let schema = json!({
                "type": "object",
                "properties": {
                    "key": {
                        "type": "number"
                    },
                    "value": {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    }
                }
            });

            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.json"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&schema)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let kv = [
            (json!(12321), json!(["a", "b", "c"])),
            (json!(32123), json!(["p", "q", "r"])),
        ];

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for (ref key, ref value) in kv {
                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                        .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-----------------------------------------------------------------------------------------+-------+-----------+",
            "| meta                                                                                    | key   | value     |",
            "+-----------------------------------------------------------------------------------------+-------+-----------+",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 12321 | [a, b, c] |",
            "| {day: 13, month: 2, partition: 32123, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 32123 | [p, q, r] |",
            "+-----------------------------------------------------------------------------------------+-------+-----------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn primitive_key_and_array_object_value() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5);
        let topic = &alphanumeric_string(5);

        let schema_registry = {
            let schema = json!({
                "type": "object",
                "properties": {
                    "key": {
                        "type": "number"
                    },
                    "value": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "quantity": {
                                    "type": "integer",
                                },
                                "location": {
                                    "type": "string",
                                }
                            }
                        }
                    }
                }
            });

            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.json"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&schema)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let kv = [
            (
                json!(12321),
                json!([{"quantity": 6, "location": "abc"}, {"quantity": 11, "location": "pqr"}]),
            ),
            (
                json!(32123),
                json!([{"quantity": 3, "location": "abc"},
                       {"quantity": 33, "location": "def"},
                       {"quantity": 21, "location": "xyz"}]),
            ),
        ];

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for (ref key, ref value) in kv {
                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                        .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-------+-----------+",
            "| key   | value     |",
            "+-------+-----------+",
            "| 12321 | [a, b, c] |",
            "| 32123 | [p, q, r] |",
            "+-------+-----------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn primitive_key_and_struct_with_array_field_value() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let schema_registry = {
            let schema = json!({
                "type": "object",
                "properties": {
                    "key": {
                        "type": "number"
                    },
                    "value": {
                        "type": "object",
                        "properties": {
                            "zone": {
                                "type": "number",
                            },
                            "locations": {
                                "type": "array",
                                "items": {
                                    "type": "string"
                                }
                            }
                        }
                    }
                }
            });

            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.json"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&schema)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let kv = [
            (
                json!(12321),
                json!({"zone": 6, "locations": ["abc", "def"]}),
            ),
            (json!(32123), json!({"zone": 11, "locations": ["pqr"]})),
        ];

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for (ref key, ref value) in kv {
                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                        .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-------+-----------+",
            "| key   | value     |",
            "+-------+-----------+",
            "| 12321 | [a, b, c] |",
            "| 32123 | [p, q, r] |",
            "+-------+-----------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }
}

mod proto {
    use super::*;
    use nisshi_schema::{
        Generator as _,
        proto::{MessageKind, Schema},
    };

    #[tokio::test]
    async fn message_descriptor_singular_to_field() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let proto = Bytes::from_static(
            br#"
            syntax = 'proto3';

            message Key {
                int32 id = 1;
            }

            message Value {
                double a = 1;
                float b = 2;
                int32 c = 3;
                int64 d = 4;
                uint32 e = 5;
                uint64 f = 6;
                sint32 g = 7;
                sint64 h = 8;
                fixed32 i = 9;
                fixed64 j = 10;
                sfixed32 k = 11;
                sfixed64 l = 12;
                bool m = 13;
                string n = 14;
            }
            "#,
        );

        let kv = [(
            json!({"id": 32123}),
            json!({"a": 567.65,
                    "b": 45.654,
                    "c": -6,
                    "d": -66,
                    "e": 23432,
                    "f": 34543,
                    "g": 45654,
                    "h": 67876,
                    "i": 78987,
                    "j": 89098,
                    "k": 90109,
                    "l": 12321,
                    "m": true,
                    "n": "Hello World!"}),
        )];

        let partition = 32123;

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.proto"));
            _ = object_store
                .put(&location, PutPayload::from(proto.clone()))
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::try_from(proto)?;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(119_731_017_000);

            for (delta, (key, value)) in kv.iter().enumerate() {
                batch = batch.record(
                    Record::builder()
                        .key(schema.encode_from_value(MessageKind::Key, key)?.into())
                        .value(schema.encode_from_value(MessageKind::Value, value)?.into())
                        .timestamp_delta(delta as i64)
                        .offset_delta(delta as i32),
                );
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+------------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| meta                                                                               | key         | value                                                                                                                                           |",
            "+------------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+",
            "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {id: 32123} | {a: 567.65, b: 45.654, c: -6, d: -66, e: 23432, f: 34543, g: 45654, h: 67876, i: 78987, j: 89098, k: 90109, l: 12321, m: true, n: Hello World!} |",
            "+------------------------------------------------------------------------------------+-------------+-------------------------------------------------------------------------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn taxi_plain() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let proto = Bytes::from_static(include_bytes!("../../etc/schema/taxi.proto"));

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.proto"));
            _ = object_store
                .put(&location, PutPayload::from(proto.clone()))
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::try_from(proto)?;

        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
              "vendor_id": 1,
              "trip_id": 1000371,
              "trip_distance": 1.8,
              "fare_amount": 15.32,
              "store_and_fwd": "N"
            }),
        )?;

        let partition = 32123;

        let record_batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .base_timestamp(119_731_017_000)
            .build()?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+",
            "| meta                                                                               | value                                                                                      |",
            "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+",
            "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {vendor_id: 1, trip_id: 1000371, trip_distance: 1.8, fare_amount: 15.32, store_and_fwd: 0} |",
            "+------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn taxi_normalized() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let proto = Bytes::from_static(include_bytes!("../../etc/schema/taxi.proto"));

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.proto"));
            _ = object_store
                .put(&location, PutPayload::from(proto.clone()))
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::try_from(proto)?;

        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
              "vendor_id": 1,
              "trip_id": 1000371,
              "trip_distance": 1.8,
              "fare_amount": 15.32,
              "store_and_fwd": "N"
            }),
        )?;

        let partition = 32123;

        let record_batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .base_timestamp(119_731_017_000)
            .build()?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            normalized_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+----------------+---------------------+-----------------+---------------+---------------------+-------------------+---------------------+",
            "| meta.partition | meta.timestamp      | value.vendor_id | value.trip_id | value.trip_distance | value.fare_amount | value.store_and_fwd |",
            "+----------------+---------------------+-----------------+---------------+---------------------+-------------------+---------------------+",
            "| 32123          | 1973-10-17T18:36:57 | 1               | 1000371       | 1.8                 | 15.32             | 0                   |",
            "+----------------+---------------------+-----------------+---------------+---------------------+-------------------+---------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn value_message_ref() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let proto = Bytes::from_static(
            br#"
                syntax = 'proto3';

                message Project {
                    string name = 1;
                    float complete = 2;
                }

                message Value {
                    Project project = 1;
                    string title = 2;
                }
                "#,
        );

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.proto"));
            _ = object_store
                .put(&location, PutPayload::from(proto.clone()))
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::try_from(proto)?;

        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
                "project": {"name": "xyz", "complete": 0.99},
                "title": "abc",
            }),
        )?;

        let partition = 32123;

        let record_batch = Batch::builder()
            .base_timestamp(119_731_017_000)
            .record(Record::builder().value(value.into()))
            .build()?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+------------------------------------------------------------------------------------+----------------------------------------------------+",
            "| meta                                                                               | value                                              |",
            "+------------------------------------------------------------------------------------+----------------------------------------------------+",
            "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {project: {name: xyz, complete: 0.99}, title: abc} |",
            "+------------------------------------------------------------------------------------+----------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn simple_repeated() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let proto = Bytes::from_static(
            br#"
            syntax = 'proto3';

            message Value {
              string url = 1;
              string title = 2;
              repeated string snippets = 3;
            }
            "#,
        );

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.proto"));
            _ = object_store
                .put(&location, PutPayload::from(proto.clone()))
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::try_from(proto)?;

        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
                "url": "https://example.com/a", "title": "a", "snippets": ["p", "q", "r"]
            }),
        )?;

        let partition = 32123;

        let record_batch = Batch::builder()
            .base_timestamp(119_731_017_000)
            .record(Record::builder().value(value.into()))
            .build()?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+------------------------------------------------------------------------------------+-------------------------------------------------------------+",
            "| meta                                                                               | value                                                       |",
            "+------------------------------------------------------------------------------------+-------------------------------------------------------------+",
            "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {url: https://example.com/a, title: a, snippets: [p, q, r]} |",
            "+------------------------------------------------------------------------------------+-------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn repeated() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let proto = Bytes::from_static(
            br#"
                syntax = 'proto3';

                message Value {
                  repeated Result results = 1;
                }

                message Result {
                  string url = 1;
                  string title = 2;
                  repeated string snippets = 3;
                }
                "#,
        );

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.proto"));
            _ = object_store
                .put(&location, PutPayload::from(proto.clone()))
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::try_from(proto)?;

        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
                "results": [{"url": "https://example.com/abc", "title": "a", "snippets": ["p", "q", "r"]},
                            {"url": "https://example.com/def", "title": "b", "snippets": ["x", "y", "z"]}]
            }),
        )?;

        let partition = 32123;

        let record_batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .base_timestamp(119_731_017_000)
            .build()?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------+",
            "| meta                                                                               | value                                                                                                                                     |",
            "+------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------+",
            "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {results: [{url: https://example.com/abc, title: a, snippets: [p, q, r]}, {url: https://example.com/def, title: b, snippets: [x, y, z]}]} |",
            "+------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn customer_schema_migration() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];
        let partition = 32123;

        let (schema_registry, record_batch_001) = {
            let proto = Bytes::from_static(include_bytes!("migrate-001.proto"));

            let schema_registry = {
                let object_store = InMemory::new();
                let location = Path::from(format!("{topic}.proto"));
                _ = object_store
                    .put(&location, PutPayload::from(proto.clone()))
                    .await?;

                Registry::new(object_store)
            };

            let schema = Schema::try_from(proto)?;

            (
                schema_registry,
                Batch::builder()
                    .record(schema.generate()?)
                    .base_timestamp(119_731_017_000)
                    .build()?,
            )
        };

        schema_registry.validate(topic, &record_batch_001).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch_001,
        )
        .await?;
        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+------------------------------------------------------------------------------------+------------------------+",
            "| meta                                                                               | value                  |",
            "+------------------------------------------------------------------------------------+------------------------+",
            "| {partition: 32123, timestamp: 1973-10-17T18:36:57, year: 1973, month: 10, day: 17} | {email_address: lorem} |",
            "+------------------------------------------------------------------------------------+------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        let (schema_registry, record_batch_002) = {
            let proto = Bytes::from_static(include_bytes!("migrate-002.proto"));

            let schema_registry = {
                let object_store = InMemory::new();
                let location = Path::from(format!("{topic}.proto"));
                _ = object_store
                    .put(&location, PutPayload::from(proto.clone()))
                    .await?;

                Registry::new(object_store)
            };

            let schema = Schema::try_from(proto)?;

            (
                schema_registry,
                Batch::builder()
                    .record(schema.generate()?)
                    .base_timestamp(119_731_017_000)
                    .build()?,
            )
        };

        schema_registry.validate(topic, &record_batch_002).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            normalized_config(topic),
            &record_batch_002,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+----------------+---------------------+-----------+------------+----------+---------------------+-----------------+",
            "| meta.partition | meta.timestamp      | meta.year | meta.month | meta.day | value.email_address | value.full_name |",
            "+----------------+---------------------+-----------+------------+----------+---------------------+-----------------+",
            "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | lorem               | ipsum           |",
            "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | lorem               |                 |",
            "+----------------+---------------------+-----------+------------+----------+---------------------+-----------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        let (schema_registry, record_batch_003) = {
            let proto = Bytes::from_static(include_bytes!("migrate-003.proto"));

            let schema_registry = {
                let object_store = InMemory::new();
                let location = Path::from(format!("{topic}.proto"));
                _ = object_store
                    .put(&location, PutPayload::from(proto.clone()))
                    .await?;

                Registry::new(object_store)
            };

            let schema = Schema::try_from(proto)?;

            (
                schema_registry,
                Batch::builder()
                    .record(schema.generate()?)
                    .base_timestamp(119_731_017_000)
                    .build()?,
            )
        };

        schema_registry.validate(topic, &record_batch_003).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            normalized_config(topic),
            &record_batch_003,
        )
        .await?;
        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+----------------+---------------------+-----------+------------+----------+---------------------+-----------------+----------------------------+------------------------+-----------------+----------------------+-------------------------+--------------------+",
            "| meta.partition | meta.timestamp      | meta.year | meta.month | meta.day | value.email_address | value.full_name | value.home.building_number | value.home.street_name | value.home.city | value.home.post_code | value.home.country_name | value.industry     |",
            "+----------------+---------------------+-----------+------------+----------+---------------------+-----------------+----------------------------+------------------------+-----------------+----------------------+-------------------------+--------------------+",
            "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | lorem               | ipsum           | dolor                      | sit                    | amet            | consectetur          | adipiscing              | [elit, elit, elit] |",
            "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | lorem               | ipsum           |                            |                        |                 |                      |                         |                    |",
            "| 32123          | 1973-10-17T18:36:57 | 1973      | 10         | 17       | lorem               |                 |                            |                        |                 |                      |                         |                    |",
            "+----------------+---------------------+-----------+------------+----------+---------------------+-----------------+----------------------------+------------------------+-----------------+----------------------+-------------------------+--------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }
}

mod avro {
    use super::*;
    use apache_avro::types::Value as AvroValue;
    use nisshi_schema::{
        AsKafkaRecord,
        avro::{Schema, r, schema_write},
    };
    use uuid::Uuid;

    #[tokio::test]
    async fn record_of_primitive_data_types() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
                  "type": "record",
                  "name": "Message",
                  "fields": [
                    {
                      "name": "value",
                      "type": {
                        "name": "sub",
                        "type": "record",
                        "fields": [
                          { "name": "b", "type": "boolean" },
                          { "name": "c", "type": "int" },
                          { "name": "d", "type": "long" },
                          { "name": "e", "type": "float" },
                          { "name": "f", "type": "double" },
                          { "name": "g", "type": "bytes" },
                          { "name": "h", "type": "string" }
                        ]
                      }
                    }
                  ]
                }
        );

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [r(
                schema.value().as_ref().unwrap(),
                [
                    // ("a", Value::Null),
                    ("b", false.into()),
                    ("c", i32::MAX.into()),
                    ("d", i64::MAX.into()),
                    ("e", f32::MAX.into()),
                    ("f", f64::MAX.into()),
                    ("g", Vec::from(&b"abcdef"[..]).into()),
                    ("h", "pqr".into()),
                ],
            )
            .unwrap()];

            for value in values {
                batch =
                    batch.record(Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value.into())?.into(),
                    ))
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+",
            "| value                                                                                                                  | meta                                                                              |",
            "+------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+",
            "| {b: false, c: 2147483647, d: 9223372036854775807, e: 3.4028235e38, f: 1.7976931348623157e308, g: 616263646566, h: pqr} | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "+------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn record_of_with_list_of_primitive_data_types() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
          "type": "record",
          "name": "Message",
          "fields": [
            {
              "name": "value",
              "type": {
                "name": "sub",
                "type": "record",
                "fields": [
                  { "name": "b", "type": "array", "items": "boolean" },
                  { "name": "c", "type": "array", "items": "int" },
                  { "name": "d", "type": "array", "items": "long" },
                  { "name": "e", "type": "array", "items": "float" },
                  { "name": "f", "type": "array", "items": "double" },
                  { "name": "g", "type": "array", "items": "bytes" },
                  { "name": "h", "type": "array", "items": "string" }
                ]
              }
            }
          ]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [r(
                schema.value().as_ref().unwrap(),
                [
                    ("b", AvroValue::Array(vec![false.into(), true.into()])),
                    (
                        "c",
                        AvroValue::Array(vec![i32::MIN.into(), 0.into(), i32::MAX.into()]),
                    ),
                    (
                        "d",
                        AvroValue::Array(vec![i64::MIN.into(), 0.into(), i64::MAX.into()]),
                    ),
                    (
                        "e",
                        AvroValue::Array(vec![f32::MIN.into(), 0.0f32.into(), f32::MAX.into()]),
                    ),
                    (
                        "f",
                        AvroValue::Array(vec![f64::MIN.into(), 0.0f64.into(), f64::MAX.into()]),
                    ),
                    (
                        "g",
                        AvroValue::Array(vec![Vec::from(&b"abcdef"[..]).into()]),
                    ),
                    (
                        "h",
                        AvroValue::Array(vec!["abc".into(), "pqr".into(), "xyz".into()]),
                    ),
                ],
            )
            .unwrap()];

            for value in values {
                batch =
                    batch.record(Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value.into())?.into(),
                    ))
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+",
            "| value                                                                                                                                                                                                                                           | meta                                                                              |",
            "+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+",
            "| {b: [false, true], c: [-2147483648, 0, 2147483647], d: [-9223372036854775808, 0, 9223372036854775807], e: [-3.4028235e38, 0.0, 3.4028235e38], f: [-1.7976931348623157e308, 0.0, 1.7976931348623157e308], g: [616263646566], h: [abc, pqr, xyz]} | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn union() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5);
        let topic = &alphanumeric_string(5);

        let avro = json!({
            "type": "record",
            "name": "union",
            "fields": [{"name": "value", "type": ["null", "float"]}]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [
                AvroValue::Union(1, Box::new(AvroValue::Float(f32::MIN))),
                AvroValue::Union(0, Box::new(AvroValue::Null)),
                AvroValue::Union(1, Box::new(AvroValue::Float(f32::MAX))),
            ];

            for value in values {
                batch = batch.record(
                    Record::builder()
                        .value(schema_write(schema.value().as_ref().unwrap(), value)?.into()),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+---------------+-----------------------------------------------------------------------------------+",
            "| value         | meta                                                                              |",
            "+---------------+-----------------------------------------------------------------------------------+",
            "| -3.4028235e38 | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "|               | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| 3.4028235e38  | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "+---------------+-----------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn enumeration() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "Suit",
            "fields": [
                {
                    "name": "value",
                    "type": "enum",
                    "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
                }
            ]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [
                AvroValue::from(json!("CLUBS")),
                AvroValue::from(json!("HEARTS")),
            ];

            for value in values {
                batch = batch.record(
                    Record::builder()
                        .value(schema_write(schema.value().as_ref().unwrap(), value)?.into()),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+--------+-----------------------------------------------------------------------------------+",
            "| value  | meta                                                                              |",
            "+--------+-----------------------------------------------------------------------------------+",
            "| CLUBS  | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| HEARTS | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "+--------+-----------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn observation_enumeration() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
          "type": "record",
          "name": "observation",
          "fields": [
            { "name": "key", "type": "string", "logicalType": "uuid" },
            {
              "name": "value",
              "type": {
                "name": "sub",
                "type": "record",
                "fields": [
                  { "name": "amount", "type": "double" },
                  { "name": "unit", "type": "enum", "symbols": ["CELSIUS", "MILLIBAR"] }
                ]
              }
            }
          ]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [json!({
                "key": "1E44D9C2-5E7A-443B-BF10-2B1E5FD72F15",
                "value": {
                    "amount": 23.2,
                    "unit": "CELSIUS"
                }
            })];

            for value in values {
                batch = batch.record(schema.as_kafka_record(&value)?);
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+--------------------------------------+-------------------------------+-----------------------------------------------------------------------------------+",
            "| key                                  | value                         | meta                                                                              |",
            "+--------------------------------------+-------------------------------+-----------------------------------------------------------------------------------+",
            "| 1e44d9c2-5e7a-443b-bf10-2b1e5fd72f15 | {amount: 23.2, unit: CELSIUS} | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "+--------------------------------------+-------------------------------+-----------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn map() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "Long",
            "fields": [
                {"name": "value", "type": "map", "values": "long", "default": {}},
            ],
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder();

            let values = [AvroValue::from(json!({"a": 1, "b": 3, "c": 5}))];

            for value in values {
                batch = batch.record(
                    Record::builder()
                        .value(schema_write(schema.value().as_ref().unwrap(), value)?.into()),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+--------+----------------------------------------------------+",
            "| value  | meta                                               |",
            "+--------+----------------------------------------------------+",
            "| CLUBS  | {partition: 32123, timestamp: 2009-02-13T23:31:30} |",
            "| HEARTS | {partition: 32123, timestamp: 2009-02-13T23:31:30} |",
            "+--------+----------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn simple_integer_key() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "key", "type": "int"}
            ]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let keys = [32123, 45654, 87678, 12321];

            for key in keys {
                batch = batch.record(
                    Record::builder()
                        .key(schema_write(schema.key().as_ref().unwrap(), key.into())?.into()),
                );
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+-------+-----------------------------------------------------------------------------------+",
            "| key   | meta                                                                              |",
            "+-------+-----------------------------------------------------------------------------------+",
            "| 32123 | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| 45654 | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| 87678 | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| 12321 | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "+-------+-----------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn simple_record_value() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
          "type": "record",
          "name": "Person",
          "fields": [
            {
              "name": "value",
              "type": {
                "name": "sub",

                "type": "record",
                "fields": [
                  { "name": "id", "type": "int" },
                  { "name": "name", "type": "string" },
                  { "name": "lucky", "type": "array", "items": "int", "default": [] }
                ]
              }
            }
          ]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [
                r(
                    schema.value().as_ref().unwrap(),
                    [
                        ("id", 32123.into()),
                        ("name", "alice".into()),
                        ("lucky", AvroValue::Array([6.into()].into())),
                    ],
                )
                .unwrap(),
                r(
                    schema.value().as_ref().unwrap(),
                    [
                        ("id", 45654.into()),
                        ("name", "bob".into()),
                        ("lucky", AvroValue::Array([5.into(), 9.into()].into())),
                    ],
                )
                .unwrap(),
            ];

            for value in values {
                batch =
                    batch.record(Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value.into())?.into(),
                    ))
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+---------------------------------------+-----------------------------------------------------------------------------------+",
            "| value                                 | meta                                                                              |",
            "+---------------------------------------+-----------------------------------------------------------------------------------+",
            "| {id: 32123, name: alice, lucky: [6]}  | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| {id: 45654, name: bob, lucky: [5, 9]} | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "+---------------------------------------+-----------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn array_bool_value() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "array",
                "items": "boolean",
                "default": []
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let values = [[true, true], [false, true], [true, false], [false, false]]
            .into_iter()
            .map(|l| AvroValue::Array(l.into_iter().map(AvroValue::Boolean).collect::<Vec<_>>()))
            .collect::<Vec<_>>();

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+----------------+-----------------------------------------------------------------------------------+",
            "| value          | meta                                                                              |",
            "+----------------+-----------------------------------------------------------------------------------+",
            "| [true, true]   | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| [false, true]  | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| [true, false]  | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| [false, false] | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "+----------------+-----------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn array_int_value() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "array",
                "items": "int",
                "default": []
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [vec![32123, 23432, 12321, 56765], vec![i32::MIN, i32::MAX]]
                .into_iter()
                .map(|l| AvroValue::Array(l.into_iter().map(AvroValue::Int).collect::<Vec<_>>()))
                .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+------------------------------+-----------------------------------------------------------------------------------+",
            "| value                        | meta                                                                              |",
            "+------------------------------+-----------------------------------------------------------------------------------+",
            "| [32123, 23432, 12321, 56765] | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| [-2147483648, 2147483647]    | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "+------------------------------+-----------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn array_long_value() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "array",
                "items": "long",
                "default": []
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [vec![32123, 23432, 12321, 56765], vec![i64::MIN, i64::MAX]]
                .into_iter()
                .map(|l| AvroValue::Array(l.into_iter().map(AvroValue::Long).collect::<Vec<_>>()))
                .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+---------------------------------------------+-----------------------------------------------------------------------------------+",
            "| value                                       | meta                                                                              |",
            "+---------------------------------------------+-----------------------------------------------------------------------------------+",
            "| [32123, 23432, 12321, 56765]                | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| [-9223372036854775808, 9223372036854775807] | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "+---------------------------------------------+-----------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn array_float_value() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "name": "test",
            "type": "record",
            "fields": [{
                "name": "value",
                "type": "array",
                "items": "float",
                "default": []
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [
                vec![3.2123, 23.432, 123.21, 5676.5],
                vec![f32::MIN, f32::MAX],
            ]
            .into_iter()
            .map(|l| AvroValue::Array(l.into_iter().map(AvroValue::Float).collect::<Vec<_>>()))
            .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+----------------------------------+-----------------------------------------------------------------------------------+",
            "| value                            | meta                                                                              |",
            "+----------------------------------+-----------------------------------------------------------------------------------+",
            "| [3.2123, 23.432, 123.21, 5676.5] | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| [-3.4028235e38, 3.4028235e38]    | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "+----------------------------------+-----------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn array_double_value() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
               "name": "value",
                "type": "array",
                "items": "double",
                "default": []
            }],
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [
                vec![3.2123, 23.432, 123.21, 5676.5],
                vec![f64::MIN, f64::MAX],
            ]
            .into_iter()
            .map(|l| AvroValue::Array(l.into_iter().map(AvroValue::Double).collect::<Vec<_>>()))
            .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+---------------------------------------------------+-----------------------------------------------------------------------------------+",
            "| value                                             | meta                                                                              |",
            "+---------------------------------------------------+-----------------------------------------------------------------------------------+",
            "| [3.2123, 23.432, 123.21, 5676.5]                  | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| [-1.7976931348623157e308, 1.7976931348623157e308] | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "+---------------------------------------------------+-----------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn array_string_value() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "array",
                "items": "string",
                "default": []
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [
                vec!["abc".to_string(), "def".to_string(), "pqr".to_string()],
                vec!["xyz".to_string()],
            ]
            .into_iter()
            .map(|l| AvroValue::Array(l.into_iter().map(AvroValue::String).collect::<Vec<_>>()))
            .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+-----------------+-----------------------------------------------------------------------------------+",
            "| value           | meta                                                                              |",
            "+-----------------+-----------------------------------------------------------------------------------+",
            "| [abc, def, pqr] | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| [xyz]           | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "+-----------------+-----------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn array_record_value() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "xyz",
                    "fields": [{
                        "name": "id",
                        "type": "int"
                    },
                    {
                        "name": "name",
                        "type": "string"
                    }
                ]},
                "default": []
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder();

            let values = [
                AvroValue::Array(vec![
                    AvroValue::Record(vec![
                        ("id".into(), 32123.into()),
                        ("name".into(), "alice".into()),
                    ]),
                    AvroValue::Record(vec![
                        ("id".into(), 45654.into()),
                        ("name".into(), "bob".into()),
                    ]),
                ]),
                AvroValue::Array(vec![AvroValue::Record(vec![
                    ("id".into(), 54345.into()),
                    ("name".into(), "betty".into()),
                ])]),
            ];

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+--------------------------+----------------------------------------------------+",
            "| value                    | meta                                               |",
            "+--------------------------+----------------------------------------------------+",
            "| [616263, 646566, 707172] | {partition: 32123, timestamp: 2009-02-13T23:31:30} |",
            "| [3534333435]             | {partition: 32123, timestamp: 2009-02-13T23:31:30} |",
            "+--------------------------+----------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn array_bytes_value() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "array",
                "items": "bytes",
                "default": []
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [
                vec![b"abc".to_vec(), b"def".to_vec(), b"pqr".to_vec()],
                vec![b"54345".to_vec()],
            ]
            .into_iter()
            .map(|l| AvroValue::Array(l.into_iter().map(AvroValue::Bytes).collect::<Vec<_>>()))
            .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+--------------------------+-----------------------------------------------------------------------------------+",
            "| value                    | meta                                                                              |",
            "+--------------------------+-----------------------------------------------------------------------------------+",
            "| [616263, 646566, 707172] | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| [3534333435]             | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "+--------------------------+-----------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn uuid_logical_type() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "string",
                "logicalType": "uuid"
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [
                "383BB977-7D38-42B5-8BE7-58A1C606DE7A",
                "2C1FDDC8-4EBE-43FD-8F1C-47E18B7A4E21",
                "F9B45334-9AA2-4978-8735-9800D27A551C",
            ]
            .into_iter()
            .map(|uuid| {
                Uuid::parse_str(uuid)
                    .map(AvroValue::Uuid)
                    .map_err(Into::into)
            })
            .collect::<Result<Vec<_>>>()?;

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+--------------------------------------+-----------------------------------------------------------------------------------+",
            "| value                                | meta                                                                              |",
            "+--------------------------------------+-----------------------------------------------------------------------------------+",
            "| 383bb977-7d38-42b5-8be7-58a1c606de7a | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| 2c1fddc8-4ebe-43fd-8f1c-47e18b7a4e21 | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| f9b45334-9aa2-4978-8735-9800d27a551c | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "+--------------------------------------+-----------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn time_millis_logical_type() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "int",
                "logicalType": "time-millis"
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder();

            let values = [1, 2, 3]
                .into_iter()
                .map(AvroValue::TimeMillis)
                .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+--------------------------------------+----------------------------------------------------+",
            "| value                                | meta                                               |",
            "+--------------------------------------+----------------------------------------------------+",
            "| 383bb977-7d38-42b5-8be7-58a1c606de7a | {partition: 32123, timestamp: 2009-02-13T23:31:30} |",
            "| 2c1fddc8-4ebe-43fd-8f1c-47e18b7a4e21 | {partition: 32123, timestamp: 2009-02-13T23:31:30} |",
            "| f9b45334-9aa2-4978-8735-9800d27a551c | {partition: 32123, timestamp: 2009-02-13T23:31:30} |",
            "+--------------------------------------+----------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn time_micros_logical_type() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "long",
                "logicalType": "time-micros"
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [1, 2, 3]
                .into_iter()
                .map(AvroValue::TimeMicros)
                .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+-----------------+-----------------------------------------------------------------------------------+",
            "| value           | meta                                                                              |",
            "+-----------------+-----------------------------------------------------------------------------------+",
            "| 00:00:00.000001 | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| 00:00:00.000002 | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| 00:00:00.000003 | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "+-----------------+-----------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn timestamp_millis_logical_type() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "long",
                "logicalType": "timestamp-millis"
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder();

            let values = [119_731_017, 1_000_000_000, 1_234_567_890]
                .into_iter()
                .map(|seconds| AvroValue::TimestampMillis(seconds * 1_000))
                .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+-----------------+----------------------------------------------------+",
            "| value           | meta                                               |",
            "+-----------------+----------------------------------------------------+",
            "| 00:00:00.000001 | {partition: 32123, timestamp: 2009-02-13T23:31:30} |",
            "| 00:00:00.000002 | {partition: 32123, timestamp: 2009-02-13T23:31:30} |",
            "| 00:00:00.000003 | {partition: 32123, timestamp: 2009-02-13T23:31:30} |",
            "+-----------------+----------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    async fn timestamp_micros_logical_type() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let namespace = &alphanumeric_string(5)[..];
        let topic = &alphanumeric_string(5)[..];

        let avro = json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "value",
                "type": "long",
                "logicalType": "timestamp-micros"
            }]
        });

        let schema_registry = {
            let object_store = InMemory::new();
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&avro)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;

            Registry::new(object_store)
        };

        let schema = Schema::from(avro);

        let partition = 32123;

        let record_batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            let values = [119_731_017, 1_000_000_000, 1_234_567_890]
                .into_iter()
                .map(|seconds| AvroValue::TimestampMicros(seconds * 1_000 * 1_000))
                .collect::<Vec<_>>();

            for value in values {
                batch = batch.record(
                    Record::builder().value(
                        schema_write(schema.value().as_ref().unwrap(), value)
                            .inspect(|encoded| debug!(?encoded))?
                            .into(),
                    ),
                )
            }

            batch.build()
        }?;

        schema_registry.validate(topic, &record_batch).await?;

        let results = lake_store(
            namespace,
            topic,
            partition,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;

        let pretty_results = pretty_format_batches(&results).map(|pretty| pretty.to_string())?;

        let expected = vec![
            "+---------------------+-----------------------------------------------------------------------------------+",
            "| value               | meta                                                                              |",
            "+---------------------+-----------------------------------------------------------------------------------+",
            "| 1973-10-17T18:36:57 | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| 2001-09-09T01:46:40 | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "| 2009-02-13T23:31:30 | {partition: 32123, timestamp: 2009-02-13T23:31:30, year: 2009, month: 2, day: 13} |",
            "+---------------------+-----------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }
}

mod catalog_connections {
    use super::*;
    use iceberg::{
        TableCreation,
        spec::{NestedField, PrimitiveType, Schema, Type},
    };
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use tokio::{
        io::copy_bidirectional,
        net::{TcpListener, TcpStream},
    };

    /// A TCP forwarder to the catalog that counts the connections that it
    /// accepts.
    struct CountingForwarder {
        url: Url,
        connections: Arc<AtomicUsize>,
    }

    impl CountingForwarder {
        async fn start(catalog: &Url) -> Result<Self> {
            let upstream = format!(
                "{}:{}",
                catalog.host_str().unwrap(),
                catalog.port_or_known_default().unwrap()
            );
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let mut url = catalog.clone();
            url.set_host(Some("127.0.0.1"))?;
            url.set_port(Some(listener.local_addr()?.port())).unwrap();

            let connections = Arc::new(AtomicUsize::new(0));
            let accepted = connections.clone();
            _ = tokio::spawn(async move {
                while let Ok((mut client, _)) = listener.accept().await {
                    _ = accepted.fetch_add(1, Ordering::SeqCst);
                    let upstream = upstream.clone();
                    _ = tokio::spawn(async move {
                        if let Ok(mut server) = TcpStream::connect(upstream).await {
                            _ = copy_bidirectional(&mut client, &mut server).await;
                        }
                    });
                }
            });

            Ok(Self { url, connections })
        }

        /// Returns the connections accepted since the last call.
        fn take_count(&self) -> usize {
            self.connections.swap(0, Ordering::SeqCst)
        }
    }

    async fn json_schema_registry(topic: &str) -> Result<Registry> {
        let object_store = InMemory::new();
        let schema = json!({
            "type": "object",
            "properties": {"key": {"type": "number"}}
        });
        _ = object_store
            .put(
                &Path::from(format!("{topic}.json")),
                serde_json::to_vec(&schema)
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;
        Ok(Registry::new(object_store))
    }

    /// Creates `count` namespaces, each with one table.
    async fn create_namespaces_with_a_table(
        catalog: &dyn Catalog,
        count: usize,
    ) -> Result<Vec<NamespaceIdent>> {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()?;
        let mut namespaces = Vec::with_capacity(count);
        for _ in 0..count {
            let namespace = NamespaceIdent::new(alphanumeric_string(8));
            _ = catalog.create_namespace(&namespace, HashMap::new()).await?;
            namespaces.push(namespace.clone());
            _ = catalog
                .create_table(
                    &namespace,
                    TableCreation::builder()
                        .name(alphanumeric_string(8))
                        .schema(schema.clone())
                        .build(),
                )
                .await?;
        }
        Ok(namespaces)
    }

    /// Stores one record through the forwarder, and returns how many
    /// connections the store and the read opened to the catalog.
    async fn connections_to_store_and_read(forwarder: &CountingForwarder) -> Result<usize> {
        let topic = &alphanumeric_string(5)[..];
        let namespace = &alphanumeric_string(5)[..];
        let schema_registry = json_schema_registry(topic).await?;
        let record_batch = Batch::builder()
            .base_timestamp(1_234_567_890 * 1_000)
            .record(Record::builder().key(Some(Bytes::from_static(b"12321"))))
            .build()?;

        _ = forwarder.take_count();
        let results = lake_store_with_catalog(
            &forwarder.url,
            namespace,
            topic,
            32123,
            schema_registry,
            empty_config(topic),
            &record_batch,
        )
        .await?;
        let connections = forwarder.take_count();

        assert_eq!(1, results.iter().map(RecordBatch::num_rows).sum::<usize>());

        // The namespace is gone, so a test run leaves no namespace behind in
        // the catalog.
        let catalog = iceberg_catalog(&catalog_url()?, var("ICEBERG_WAREHOUSE").ok()).await?;
        assert!(
            !catalog
                .namespace_exists(&NamespaceIdent::new(namespace.to_owned()))
                .await?,
            "{namespace} is still in the catalog"
        );

        Ok(connections)
    }

    /// Reading a topic's table opens as many connections to the catalog
    /// whatever else the catalog holds, so tests that run at once can't use
    /// up the catalog's connections.
    #[tokio::test]
    async fn catalog_connections_are_bounded() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_url = catalog_url()?;
        let catalog = iceberg_catalog(&catalog_url, var("ICEBERG_WAREHOUSE").ok()).await?;
        let forwarder = CountingForwarder::start(&catalog_url).await?;

        let alone = connections_to_store_and_read(&forwarder).await?;

        // Twenty more tables in the catalog, which the read must not load.
        let others = create_namespaces_with_a_table(catalog.as_ref(), 20).await;
        let beside_others = match &others {
            Ok(_) => connections_to_store_and_read(&forwarder).await,
            Err(_) => Ok(0),
        };
        for namespace in others.as_ref().into_iter().flatten() {
            drop_namespace_with_tables(catalog.as_ref(), namespace).await?;
        }
        let beside_others = others.and(beside_others)?;

        debug!(alone, beside_others);
        // The store and the read each connect at least once, so a lower count
        // means that they reached the catalog without the forwarder, and the
        // test counted nothing.
        assert!(
            (2..=10).contains(&alone),
            "{alone} connections to the catalog"
        );
        // A client can reconnect when the catalog closes an idle connection,
        // so the counts may differ by a connection or two. Loading every
        // table of the 20 namespaces would add 20.
        assert!(
            beside_others <= alone + 2,
            "{alone} connections alone, {beside_others} beside 20 other tables"
        );

        Ok(())
    }
}
