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

use std::{
    collections::HashMap,
    env::vars,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use crate::{
    AsArrow as _, Error, Registry, Result,
    lake::{LakeHouse, LakeHouseType},
};
use async_trait::async_trait;
use iceberg::memory::MemoryCatalogBuilder;
use iceberg::{
    Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent,
    io::{
        S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
        S3_SESSION_TOKEN,
    },
    spec::{DataFileFormat, Schema, TableMetadataBuilder},
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
            rolling_writer::RollingFileWriterBuilder,
        },
    },
};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use nisshi_sans_io::{describe_configs_response::DescribeConfigsResult, record::inflated::Batch};
use parquet::file::properties::WriterProperties;
use tracing::{debug, error};
use url::Url;
use uuid::Uuid;

use super::House;

fn env_mapping(k: &str) -> Option<&str> {
    match k {
        "AWS_ACCESS_KEY_ID" => Some(S3_ACCESS_KEY_ID),
        "AWS_SECRET_ACCESS_KEY" => Some(S3_SECRET_ACCESS_KEY),
        "AWS_SESSION_TOKEN" => Some(S3_SESSION_TOKEN),
        "AWS_DEFAULT_REGION" => Some(S3_REGION),
        "AWS_ENDPOINT" => Some(S3_ENDPOINT),
        _ => None,
    }
}

/// Parses an env var the same way `object_store`'s own `ConfigValue<bool>`
/// does, so `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` means the same thing here as
/// it does for nisshi's own S3 clients.
fn env_var_is_truthy(name: &str) -> bool {
    vars().find(|(k, _)| k == name).is_some_and(|(_, v)| {
        matches!(
            v.to_ascii_lowercase().as_str(),
            "1" | "true" | "on" | "yes" | "y"
        )
    })
}

pub fn env_s3_props() -> impl Iterator<Item = (String, String)> {
    vars().filter_map(|(k, v)| env_mapping(k.as_str()).map(|k| (k.to_owned(), v)))
}

#[derive(Clone, Debug, Default)]
pub struct Builder<C = PhantomData<Url>, L = PhantomData<Url>, R = PhantomData<Registry>> {
    location: L,
    catalog: C,
    schema_registry: R,
    namespace: Option<String>,
    warehouse: Option<String>,
}

impl<C, L, R> Builder<C, L, R> {
    pub fn location(self, location: Url) -> Builder<C, Url, R> {
        Builder {
            location,
            catalog: self.catalog,
            schema_registry: self.schema_registry,
            namespace: self.namespace,
            warehouse: self.warehouse,
        }
    }

    pub fn catalog(self, catalog: Url) -> Builder<Url, L, R> {
        Builder {
            location: self.location,
            catalog,
            schema_registry: self.schema_registry,
            namespace: self.namespace,
            warehouse: self.warehouse,
        }
    }

    pub fn schema_registry(self, schema_registry: Registry) -> Builder<C, L, Registry> {
        Builder {
            catalog: self.catalog,
            location: self.location,
            schema_registry,
            namespace: self.namespace,
            warehouse: self.warehouse,
        }
    }

    pub fn namespace(self, namespace: Option<String>) -> Self {
        Self { namespace, ..self }
    }

    pub fn warehouse(self, warehouse: Option<String>) -> Self {
        Self { warehouse, ..self }
    }
}

impl Builder<Url, Url, Registry> {
    pub async fn build(self) -> Result<House> {
        Iceberg::new(self).await.map(House::Iceberg)
    }
}

#[derive(Clone, Debug)]
pub struct Iceberg {
    catalog: Arc<dyn Catalog>,
    namespace: String,
    tables: Arc<Mutex<HashMap<String, Table>>>,
    schema_registry: Registry,
}

impl Iceberg {
    async fn new(value: Builder<Url, Url, Registry>) -> Result<Self> {
        let catalog = iceberg_catalog(&value.catalog, value.warehouse.clone()).await?;
        Ok(Self {
            catalog,
            namespace: value.namespace.unwrap_or(String::from("nisshi")),
            tables: Arc::new(Mutex::new(HashMap::new())),
            schema_registry: value.schema_registry,
        })
    }
}

/// Builds the [`Catalog`] for `catalog`'s scheme (`http(s)` -> REST,
/// `memory` -> in-process). `pub` so integration tests can build the same
/// catalog nisshi itself uses (storage factory, path-style pin and all)
/// instead of re-deriving their own `RestCatalogBuilder` call and risking
/// drift between the two.
pub async fn iceberg_catalog(catalog: &Url, warehouse: Option<String>) -> Result<Arc<dyn Catalog>> {
    debug!(catalog = %crate::redact_url(catalog), ?warehouse);

    match (catalog.scheme(), catalog.path()) {
        ("http" | "https", "/") | ("http" | "https", _) => {
            let uri = if catalog.path() == "/" {
                format!(
                    "{}://{}:{}",
                    catalog.scheme(),
                    catalog.host_str().unwrap_or("localhost"),
                    catalog.port().unwrap_or(80)
                )
            } else {
                catalog.to_string()
            };

            let mut props: HashMap<String, String> = env_s3_props().collect();
            _ = props.insert(REST_CATALOG_PROP_URI.to_string(), uri);
            if let Some(wh) = warehouse {
                _ = props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), wh);
            }
            // iceberg-storage-opendal defaults to virtual-host-style S3
            // addressing. The storage built into iceberg 0.8 used
            // path-style, so keep path-style as the default rather than
            // change addressing as a side effect of this upgrade. This is
            // a client prop, and iceberg-catalog-rest 0.10 applies client
            // props last in both create_table and load_table, so it wins
            // over catalog server and per-table config, not the other way
            // around. An endpoint that only accepts virtual-host addressing
            // (S3 Express directory buckets, access points, some
            // S3-compatible stores) needs a way to opt out, so honour the
            // same env var nisshi's own object_store S3 clients already
            // read via `from_env()`.
            if !env_var_is_truthy("AWS_VIRTUAL_HOSTED_STYLE_REQUEST") {
                _ = props
                    .entry(S3_PATH_STYLE_ACCESS.to_string())
                    .or_insert_with(|| "true".to_string());
            }

            // iceberg 0.10 no longer bundles storage backends into the
            // catalog; it needs an explicit factory. Resolve by URL scheme
            // (s3, file, memory) the way iceberg 0.8's FileIO::from_path
            // did, rather than hardcoding S3 and breaking file:// and
            // other non-S3 warehouse locations.
            let catalog = RestCatalogBuilder::default()
                .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
                .load("rest", props)
                .await
                .map_err(|e| Error::Iceberg(Box::new(e)))?;

            Ok(Arc::new(catalog) as Arc<dyn Catalog>)
        }

        ("memory", _) => {
            let catalog = MemoryCatalogBuilder::default()
                .load("memory", HashMap::new())
                .await
                .map_err(|e| Error::Iceberg(Box::new(e)))?;
            Ok(Arc::new(catalog) as Arc<dyn Catalog>)
        }

        (_otherwise, _) => Err(Error::UnsupportedIcebergCatalogUrl(crate::redact_url(
            catalog,
        ))),
    }
}

impl Iceberg {
    async fn create_namespace(&self) -> Result<NamespaceIdent> {
        let namespace_ident = NamespaceIdent::new(self.namespace.clone());
        debug!(%namespace_ident);

        if !self
            .catalog
            .namespace_exists(&namespace_ident)
            .await
            .inspect(|namespace| debug!(?namespace))
            .inspect_err(|err| debug!(?err))?
        {
            match self
                .catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await
            {
                Ok(namespace) => debug!(?namespace),

                Err(err) => {
                    // The create error is logged before the check below, because a failed
                    // check returns its own error instead.
                    debug!(?err);

                    // Produces to different partitions store concurrently, so the first
                    // produces each try to create the namespace. A produce that loses that race
                    // uses the namespace that the winner created, instead of failing.
                    if !self.catalog.namespace_exists(&namespace_ident).await? {
                        return Err(err.into());
                    }
                }
            }
        }

        Ok(namespace_ident)
    }

    async fn load_or_create_table(&self, name: &str, schema: Schema) -> Result<Table> {
        if let Some(table) = self.tables.lock().map(|guard| guard.get(name).cloned())? {
            return Ok(table);
        }

        let namespace_ident = self.create_namespace().await?;
        let table_ident = TableIdent::new(namespace_ident.clone(), name.into());

        let table = if self.catalog.table_exists(&table_ident).await? {
            let table = self
                .catalog
                .load_table(&table_ident)
                .await
                .inspect_err(|err| debug!(?err))?;

            if table.metadata().current_schema().as_ref() != &schema {
                debug!(current = ?table.metadata(), ?schema);

                _ = TableMetadataBuilder::new_from_metadata(
                    table.metadata().to_owned(),
                    table
                        .metadata_location()
                        .map(|location| location.to_owned()),
                )
                .add_schema(schema.clone())?
                .set_current_schema(-1)?
                .build()
                .inspect(|update| {
                    debug!(?update.metadata);
                    debug!(?update.changes);
                    debug!(?update.expired_metadata_logs);
                })?;
            }

            table
        } else {
            match self
                .catalog
                .create_table(
                    &namespace_ident,
                    TableCreation::builder()
                        .name(name.into())
                        .schema(schema.clone())
                        .build(),
                )
                .await
            {
                Ok(table) => {
                    debug!(?table);
                    table
                }

                Err(err) => {
                    // The create error is logged before the check below, because a failed
                    // check returns its own error instead.
                    debug!(?err);

                    // Produces to different partitions of a topic store concurrently, so the
                    // first produces to a new topic each try to create its table. A produce that
                    // loses that race loads the table that the winner created, instead of
                    // failing.
                    if !self.catalog.table_exists(&table_ident).await? {
                        return Err(err.into());
                    }

                    self.catalog
                        .load_table(&table_ident)
                        .await
                        .inspect_err(|err| debug!(?err))?
                }
            }
        };

        _ = self
            .tables
            .lock()
            .map(|mut guard| guard.insert(name.to_owned(), table.clone()))?;

        Ok(table)
    }
}

#[async_trait]
impl LakeHouse for Iceberg {
    async fn store(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        inflated: &Batch,
        config: DescribeConfigsResult,
    ) -> Result<()> {
        let _ = config;

        let record_batch = self
            .schema_registry
            .as_arrow(topic, partition, inflated, LakeHouseType::Iceberg)
            .await?;

        debug!(num_rows = record_batch.num_rows());

        debug!(schema = ?record_batch.schema());

        let schema = Schema::try_from(record_batch.schema().as_ref())
            .inspect(|schema| {
                for field in schema.as_struct().fields() {
                    debug!(?field);
                }
            })
            .inspect_err(|err| debug!(?err))?;

        let table = self
            .load_or_create_table(topic, schema.clone())
            .await
            .inspect(|table| {
                for field in table.metadata().current_schema().as_struct().fields() {
                    debug!(?field);
                }
            })
            .inspect_err(|err| debug!(?err))?;

        let parquet_writer_builder = ParquetWriterBuilder::new(
            WriterProperties::default(),
            table.metadata().current_schema().clone(),
        );

        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            DefaultLocationGenerator::new(table.metadata())?,
            DefaultFileNameGenerator::new(
                topic.to_owned(),
                Some(format!("{partition:0>10}-{offset:0>20}")),
                DataFileFormat::Parquet,
            ),
        );

        let mut data_file_writer = DataFileWriterBuilder::new(rolling_writer_builder)
            .build(None)
            .await
            .inspect_err(|err| error!(?err))?;

        data_file_writer
            .write(record_batch)
            .await
            .inspect_err(|err| debug!(?err))?;

        let data_files = data_file_writer
            .close()
            .await
            .inspect(|data_files| debug!(data_files = data_files.len()))
            .inspect_err(|err| debug!(?err))?;

        let commit_uuid = Uuid::now_v7();
        debug!(%commit_uuid);

        let tx = Transaction::new(&table);

        let tx = tx
            .fast_append()
            .set_commit_uuid(commit_uuid)
            .add_data_files(data_files)
            .apply(tx)
            .inspect_err(|err| debug!(?err))?;

        tx.commit(self.catalog.as_ref())
            .await
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
            .and(Ok(()))
    }

    async fn maintain(&self) -> Result<()> {
        Ok(())
    }

    async fn lake_type(&self) -> Result<LakeHouseType> {
        Ok(LakeHouseType::Iceberg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenv::dotenv;
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    use iceberg::{
        ErrorKind, Namespace, TableCommit,
        memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog},
    };
    use rand::{distr::Alphanumeric, prelude::*, rng};
    use std::{
        assert_matches,
        env::var,
        fs::File,
        marker::PhantomData,
        str::FromStr as _,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
    };
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    pub(crate) fn alphanumeric_string(length: usize) -> String {
        rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }

    fn init_tracing() -> Result<DefaultGuard> {
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
                            File::create(format!("../logs/{}/{name}.log", env!("CARGO_PKG_NAME"),))
                                .map_err(Into::into)
                        })
                        .map(Arc::new)?,
                )
                .finish(),
        ))
    }

    impl Iceberg {
        /// Removes the lake's namespace and purges each of its tables, so that
        /// a test leaves no namespace in the catalog.
        async fn drop_namespace(&self) -> Result<()> {
            let namespace = NamespaceIdent::new(self.namespace.clone());
            for table in self.catalog.list_tables(&namespace).await? {
                self.catalog.purge_table(&table).await?;
            }
            self.catalog
                .drop_namespace(&namespace)
                .await
                .map_err(Into::into)
        }
    }

    #[tokio::test]
    async fn create_namespace() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
        let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
        let warehouse = var("ICEBERG_WAREHOUSE").ok();
        let namespace = alphanumeric_string(5);
        debug!(catalog_uri, location_uri, ?warehouse, namespace);

        let schema_registry = Registry::from_str("memory://")?;

        let lake = Iceberg::new(
            Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
                .location(Url::parse(location_uri)?)
                .catalog(Url::parse(catalog_uri)?)
                .warehouse(warehouse.clone())
                .schema_registry(schema_registry)
                .namespace(Some(namespace.clone())),
        )
        .await?;

        let ident = lake.create_namespace().await?;
        lake.drop_namespace().await?;
        assert_eq!(namespace, ident.to_url_string());

        Ok(())
    }

    #[tokio::test]
    async fn create_duplicate_namespace() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
        let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
        let warehouse = var("ICEBERG_WAREHOUSE").ok();
        let namespace = alphanumeric_string(5);
        debug!(catalog_uri, location_uri, ?warehouse, namespace);

        let schema_registry = Registry::from_str("memory://")?;

        {
            let lake = Iceberg::new(
                Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
                    .location(Url::parse(location_uri)?)
                    .catalog(Url::parse(catalog_uri)?)
                    .warehouse(warehouse.clone())
                    .schema_registry(schema_registry.clone())
                    .namespace(Some(namespace.clone())),
            )
            .await?;

            let ident = lake.create_namespace().await?;
            assert_eq!(namespace, ident.to_url_string());
        }

        {
            let lake = Iceberg::new(
                Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
                    .location(Url::parse(location_uri)?)
                    .catalog(Url::parse(catalog_uri)?)
                    .warehouse(warehouse)
                    .schema_registry(schema_registry)
                    .namespace(Some(namespace.clone())),
            )
            .await?;

            let ident = lake.create_namespace().await?;
            lake.drop_namespace().await?;
            assert_eq!(namespace, ident.to_url_string());
        }

        Ok(())
    }

    #[tokio::test]
    async fn create_table() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
        let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
        let warehouse = var("ICEBERG_WAREHOUSE").ok();
        let namespace = alphanumeric_string(5);

        debug!(catalog_uri, location_uri, ?warehouse, namespace);

        let schema_registry = Registry::from_str("memory://")?;

        let lake_house = Iceberg::new(
            Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
                .location(Url::parse(location_uri)?)
                .catalog(Url::parse(catalog_uri)?)
                .namespace(Some(namespace.clone()))
                .schema_registry(schema_registry)
                .warehouse(warehouse.clone()),
        )
        .await?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .build()?;

        let table_name = alphanumeric_string(5);

        let table = lake_house.load_or_create_table(&table_name, schema).await?;
        lake_house.drop_namespace().await?;
        assert_eq!(table_name, table.identifier().name());
        assert_eq!(namespace, table.identifier().namespace().to_url_string());

        Ok(())
    }

    #[tokio::test]
    async fn create_duplicate_table() -> Result<()> {
        _ = dotenv().ok();
        let _guard = init_tracing()?;

        let catalog_uri = &var("ICEBERG_CATALOG").unwrap_or("http://localhost:8181".into())[..];
        let location_uri = &var("DATA_LAKE").unwrap_or("s3://lake".into())[..];
        let warehouse = var("ICEBERG_WAREHOUSE").ok();
        let namespace = alphanumeric_string(5);
        let table_name = alphanumeric_string(5);

        debug!(catalog_uri, location_uri, ?warehouse, namespace, table_name);

        let schema_registry = Registry::from_str("memory://")?;

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .build()?;

        {
            let lake_house = Iceberg::new(
                Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
                    .location(Url::parse(location_uri)?)
                    .catalog(Url::parse(catalog_uri)?)
                    .warehouse(warehouse.clone())
                    .schema_registry(schema_registry.clone())
                    .namespace(Some(namespace.clone())),
            )
            .await?;

            let table = lake_house
                .load_or_create_table(&table_name, schema.clone())
                .await?;
            assert_eq!(table_name, table.identifier().name());
            assert_eq!(namespace, table.identifier().namespace().to_url_string());
        }

        {
            let lake_house = Iceberg::new(
                Builder::<PhantomData<Url>, PhantomData<Url>, PhantomData<Registry>>::default()
                    .location(Url::parse(location_uri)?)
                    .catalog(Url::parse(catalog_uri)?)
                    .namespace(Some(namespace.clone()))
                    .schema_registry(schema_registry)
                    .warehouse(warehouse),
            )
            .await?;

            let table = lake_house.load_or_create_table(&table_name, schema).await?;
            lake_house.drop_namespace().await?;
            assert_eq!(table_name, table.identifier().name());
            assert_eq!(namespace, table.identifier().namespace().to_url_string());
        }

        Ok(())
    }

    #[test]
    fn url_parse() -> Result<()> {
        let uri = Url::parse("http://localhost:8181")?;
        assert_eq!("http://localhost:8181/", uri.as_str());
        assert_eq!("http", uri.scheme());
        assert!(uri.has_host());
        assert_eq!(Some("localhost"), uri.host_str());
        assert_eq!(Some(8181), uri.port());
        assert_eq!("/", uri.path());

        let uri = Url::parse("http://localhost:8181/catalog")?;
        assert_eq!("http://localhost:8181/catalog", uri.as_str());
        assert_eq!("http", uri.scheme());
        assert!(uri.has_host());
        assert_eq!(Some("localhost"), uri.host_str());
        assert_eq!(Some(8181), uri.port());
        assert_eq!("/catalog", uri.path());

        Ok(())
    }

    /// A catalog whose existence checks answer `false` for a set number of calls, and then ask
    /// its memory catalog. A missed check acts as a check that runs just before another produce
    /// creates the namespace or the table, so a test can lose a create race on demand.
    #[derive(Debug)]
    struct CatalogWithMissedChecks {
        memory: MemoryCatalog,
        namespace_checks_to_miss: AtomicUsize,
        table_checks_to_miss: AtomicUsize,
    }

    impl CatalogWithMissedChecks {
        async fn new(namespace_checks_to_miss: usize, table_checks_to_miss: usize) -> Result<Self> {
            let memory = MemoryCatalogBuilder::default()
                .load(
                    "memory",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_owned(),
                        String::from("memory://warehouse"),
                    )]),
                )
                .await?;

            Ok(Self {
                memory,
                namespace_checks_to_miss: AtomicUsize::new(namespace_checks_to_miss),
                table_checks_to_miss: AtomicUsize::new(table_checks_to_miss),
            })
        }
    }

    /// Returns whether the next check misses, and counts it against `checks_to_miss`.
    fn check_misses(checks_to_miss: &AtomicUsize) -> bool {
        checks_to_miss
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                count.checked_sub(1)
            })
            .is_ok()
    }

    #[async_trait]
    impl Catalog for CatalogWithMissedChecks {
        async fn list_namespaces(
            &self,
            parent: Option<&NamespaceIdent>,
        ) -> iceberg::Result<Vec<NamespaceIdent>> {
            self.memory.list_namespaces(parent).await
        }

        async fn create_namespace(
            &self,
            namespace: &NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> iceberg::Result<Namespace> {
            self.memory.create_namespace(namespace, properties).await
        }

        async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
            self.memory.get_namespace(namespace).await
        }

        async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
            if check_misses(&self.namespace_checks_to_miss) {
                Ok(false)
            } else {
                self.memory.namespace_exists(namespace).await
            }
        }

        async fn update_namespace(
            &self,
            namespace: &NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> iceberg::Result<()> {
            self.memory.update_namespace(namespace, properties).await
        }

        async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
            self.memory.drop_namespace(namespace).await
        }

        async fn list_tables(
            &self,
            namespace: &NamespaceIdent,
        ) -> iceberg::Result<Vec<TableIdent>> {
            self.memory.list_tables(namespace).await
        }

        async fn create_table(
            &self,
            namespace: &NamespaceIdent,
            creation: TableCreation,
        ) -> iceberg::Result<Table> {
            self.memory.create_table(namespace, creation).await
        }

        async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
            self.memory.load_table(table).await
        }

        async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
            self.memory.drop_table(table).await
        }

        async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
            self.memory.purge_table(table).await
        }

        async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
            if check_misses(&self.table_checks_to_miss) {
                Ok(false)
            } else {
                self.memory.table_exists(table).await
            }
        }

        async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
            self.memory.rename_table(src, dest).await
        }

        async fn register_table(
            &self,
            table: &TableIdent,
            metadata_location: String,
        ) -> iceberg::Result<Table> {
            self.memory.register_table(table, metadata_location).await
        }

        async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
            self.memory.update_table(commit).await
        }
    }

    fn race_lake(catalog: CatalogWithMissedChecks, namespace: &str) -> Result<Iceberg> {
        Ok(Iceberg {
            catalog: Arc::new(catalog),
            namespace: namespace.to_owned(),
            tables: Arc::default(),
            schema_registry: Registry::from_str("memory://")?,
        })
    }

    fn race_table_creation(name: &str) -> Result<TableCreation> {
        Ok(TableCreation::builder()
            .name(name.to_owned())
            .schema(race_schema()?)
            .build())
    }

    fn race_schema() -> Result<Schema> {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .with_schema_id(1)
            .build()
            .map_err(Into::into)
    }

    #[tokio::test]
    async fn create_namespace_uses_the_namespace_that_won_a_create_race() -> Result<()> {
        let namespace = alphanumeric_string(5);
        let namespace_ident = NamespaceIdent::new(namespace.clone());

        let catalog = CatalogWithMissedChecks::new(1, 0).await?;
        _ = catalog
            .memory
            .create_namespace(&namespace_ident, HashMap::new())
            .await?;

        let lake = race_lake(catalog, &namespace)?;

        assert_eq!(namespace_ident, lake.create_namespace().await?);

        Ok(())
    }

    #[tokio::test]
    async fn load_or_create_table_loads_the_table_that_won_a_create_race() -> Result<()> {
        let namespace = alphanumeric_string(5);
        let namespace_ident = NamespaceIdent::new(namespace.clone());
        let table_name = alphanumeric_string(5);

        let catalog = CatalogWithMissedChecks::new(0, 1).await?;
        _ = catalog
            .memory
            .create_namespace(&namespace_ident, HashMap::new())
            .await?;
        let winner = catalog
            .memory
            .create_table(&namespace_ident, race_table_creation(&table_name)?)
            .await?;

        let lake = race_lake(catalog, &namespace)?;

        let table = lake
            .load_or_create_table(&table_name, race_schema()?)
            .await?;
        assert_eq!(winner.identifier(), table.identifier());
        assert_eq!(winner.metadata_location(), table.metadata_location());

        Ok(())
    }

    #[tokio::test]
    async fn load_or_create_table_returns_the_create_error_when_the_table_is_missing() -> Result<()>
    {
        let namespace = alphanumeric_string(5);
        let namespace_ident = NamespaceIdent::new(namespace.clone());
        let table_name = alphanumeric_string(5);

        // Both table checks miss, so the table that makes the create fail stays hidden.
        let catalog = CatalogWithMissedChecks::new(0, 2).await?;
        _ = catalog
            .memory
            .create_namespace(&namespace_ident, HashMap::new())
            .await?;
        _ = catalog
            .memory
            .create_table(&namespace_ident, race_table_creation(&table_name)?)
            .await?;

        let lake = race_lake(catalog, &namespace)?;

        assert_matches!(
            lake.load_or_create_table(&table_name, race_schema()?).await,
            Err(Error::Iceberg(err)) if err.kind() == ErrorKind::TableAlreadyExists
        );

        Ok(())
    }
}
