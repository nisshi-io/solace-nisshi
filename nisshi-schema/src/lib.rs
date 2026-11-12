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

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
//! Schema
//!
//! Schema includes the following:
//! - Validation of Kafka messages with an AVRO, JSON or Protobuf schema

use std::{
    collections::BTreeMap,
    env::{self},
    fmt::{self, Display, Formatter},
    io,
    num::TryFromIntError,
    result,
    str::FromStr,
    string::FromUtf8Error,
    sync::{Arc, LazyLock, Mutex, PoisonError},
    time::{Duration, SystemTime},
};

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use arrow::{datatypes::DataType, error::ArrowError, record_batch::RecordBatch};

use bytes::Bytes;

#[cfg(any(feature = "iceberg", feature = "delta"))]
use datafusion::error::DataFusionError;

#[cfg(feature = "delta")]
use deltalake::DeltaTableError;

use governor::InsufficientCapacity;

#[cfg(feature = "iceberg")]
use iceberg::spec::DataFileBuilderError;

use jsonschema::ValidationError;
use object_store::{
    DynObjectStore, ObjectStore, ObjectStoreExt, aws::AmazonS3Builder, local::LocalFileSystem,
    memory::InMemory, path::Path,
};
use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Histogram, Meter},
};
use opentelemetry_semantic_conventions::SCHEMA_URL;

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use parquet::errors::ParquetError;

use nisshi_sans_io::{ErrorCode, record::inflated::Batch};
use rhai::EvalAltResult;
use serde_json::Value;
use tracing::{debug, instrument};
use tracing_subscriber::filter::ParseError;
use url::Url;

pub mod avro;
pub mod json;
pub mod lake;
pub mod proto;

#[cfg(feature = "delta")]
pub(crate) mod sql;

pub(crate) const ARROW_LIST_FIELD_NAME: &str = "element";

/// Error
#[derive(thiserror::Error, Debug)]
pub enum Error {
    Anyhow(#[from] anyhow::Error),

    Api(ErrorCode),

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    Arrow(#[from] ArrowError),

    Avro(Box<apache_avro::Error>),

    /// An Avro record failed to read or write against its schema. This holds
    /// no detail, because the `apache_avro` error raised for a record writes
    /// the record's values into its message.
    AvroRecord,

    /// Holds the kind of the value, not the value, so that logging the error
    /// doesn't log record data.
    AvroToJson(apache_avro::schema::SchemaKind),

    BadDowncast {
        field: String,
    },

    EvalAlt(#[from] Box<EvalAltResult>),

    BuilderExhausted,

    ChronoParse(#[from] chrono::ParseError),

    #[cfg(feature = "iceberg")]
    DataFileBuilder(#[from] DataFileBuilderError),

    #[cfg(any(feature = "iceberg", feature = "delta"))]
    DataFusion(Box<DataFusionError>),

    #[cfg(feature = "delta")]
    DeltaTable(Box<DeltaTableError>),

    Downcast,

    FromUtf8(#[from] FromUtf8Error),

    #[cfg(feature = "iceberg")]
    Iceberg(Box<::iceberg::Error>),

    /// Holds the kind of the value, not the value, as [`Error::AvroToJson`]
    /// does.
    InvalidValue(apache_avro::schema::SchemaKind),

    InsufficientCapacity(#[from] InsufficientCapacity),

    Io(#[from] io::Error),

    /// Holds the kind of the JSON value, not the value, as
    /// [`Error::AvroToJson`] does.
    JsonToAvro(Box<apache_avro::Schema>, &'static str),

    JsonToAvroFieldNotFound {
        schema: Box<apache_avro::Schema>,
        field: String,
    },

    KafkaSansIo(#[from] nisshi_sans_io::Error),

    Message(String),

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    NoCommonType(Vec<DataType>),

    ObjectStore(#[from] object_store::Error),

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    Parquet(#[from] ParquetError),

    ParseFilter(#[from] ParseError),

    ParseUrl(#[from] url::ParseError),

    Poison,

    ProtobufJsonMapping(#[from] protobuf_json_mapping::ParseError),

    ProtobufJsonMappingPrint(#[from] protobuf_json_mapping::PrintError),

    Protobuf(#[from] protobuf::Error),

    ProtobufFileDescriptorMissing(Bytes),

    SchemaValidation,

    SerdeJson(#[from] serde_json::Error),

    #[cfg(any(feature = "iceberg", feature = "delta"))]
    SqlParser(#[from] datafusion::logical_expr::sqlparser::parser::ParserError),

    TopicWithoutSchema(String),

    TryFromInt(#[from] TryFromIntError),

    UnsupportedIcebergCatalogUrl(Url),

    UnsupportedLakeHouseUrl(Url),

    UnsupportedSchemaRegistryUrl(Url),

    /// Holds the kind of the JSON value, not the value, as
    /// [`Error::AvroToJson`] does.
    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    UnsupportedSchemaRuntimeValue(DataType, &'static str),

    Uuid(#[from] uuid::Error),
}

/// The kind of a JSON value, for an error that must not hold the value.
pub(crate) fn json_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[cfg(any(feature = "iceberg", feature = "delta"))]
impl From<DataFusionError> for Error {
    fn from(value: DataFusionError) -> Self {
        Self::DataFusion(Box::new(value))
    }
}

#[cfg(feature = "iceberg")]
impl From<::iceberg::Error> for Error {
    fn from(value: ::iceberg::Error) -> Self {
        Self::Iceberg(Box::new(value))
    }
}

#[cfg(feature = "delta")]
impl From<DeltaTableError> for Error {
    fn from(value: DeltaTableError) -> Self {
        Self::DeltaTable(Box::new(value))
    }
}

impl From<apache_avro::Error> for Error {
    fn from(value: apache_avro::Error) -> Self {
        Self::Avro(Box::new(value))
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poison
    }
}

impl From<ValidationError<'_>> for Error {
    fn from(_value: ValidationError<'_>) -> Self {
        Self::SchemaValidation
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;

/// Validate a Batch with a Schema
pub trait Validator {
    fn validate(&self, batch: &Batch) -> Result<()>;
}

/// Represent a Batch in the Arrow columnar data format
#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
trait AsArrow {
    async fn as_arrow(
        &self,
        topic: &str,
        partition: i32,
        batch: &Batch,
        lake_type: lake::LakeHouseType,
    ) -> Result<RecordBatch>;
}

/// Convert a JSON message into a Kafka record
pub trait AsKafkaRecord {
    fn as_kafka_record(&self, value: &Value) -> Result<nisshi_sans_io::record::Builder>;
}

/// Convert a Batch into a JSON value
pub trait AsJsonValue {
    fn as_json_value(&self, batch: &Batch) -> Result<Value>;
}

/// Generate a record
pub trait Generator {
    fn generate(&self) -> Result<nisshi_sans_io::record::Builder>;
}

// Schema
//
// This is wrapper enumeration of the supported schema types
#[derive(Clone, Debug)]
pub enum Schema {
    Avro(Box<avro::Schema>),
    Json(Arc<json::Schema>),
    Proto(Box<proto::Schema>),
}

#[derive(Clone, Debug)]
struct CachedSchema {
    loaded_at: SystemTime,
    schema: Schema,
}

impl CachedSchema {
    fn new(schema: Schema) -> Self {
        Self {
            schema,
            loaded_at: SystemTime::now(),
        }
    }
}

impl AsKafkaRecord for Schema {
    fn as_kafka_record(&self, value: &Value) -> Result<nisshi_sans_io::record::Builder> {
        match self {
            Self::Avro(schema) => schema.as_kafka_record(value),
            Self::Json(schema) => schema.as_kafka_record(value),
            Self::Proto(schema) => schema.as_kafka_record(value),
        }
    }
}

impl Validator for Schema {
    #[instrument(skip(self, batch), ret)]
    fn validate(&self, batch: &Batch) -> Result<()> {
        match self {
            Self::Avro(schema) => schema.validate(batch),
            Self::Json(schema) => schema.validate(batch),
            Self::Proto(schema) => schema.validate(batch),
        }
    }
}

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
impl AsArrow for Schema {
    #[instrument(skip(self, batch))]
    async fn as_arrow(
        &self,
        topic: &str,
        partition: i32,
        batch: &Batch,
        lake_type: lake::LakeHouseType,
    ) -> Result<RecordBatch> {
        match self {
            Self::Avro(schema) => schema.as_arrow(topic, partition, batch, lake_type).await,
            Self::Json(schema) => schema.as_arrow(topic, partition, batch, lake_type).await,
            Self::Proto(schema) => schema.as_arrow(topic, partition, batch, lake_type).await,
        }
    }
}

impl AsJsonValue for Schema {
    #[instrument(skip(self, batch))]
    fn as_json_value(&self, batch: &Batch) -> Result<Value> {
        debug!(?batch);

        match self {
            Self::Avro(schema) => schema.as_json_value(batch),
            Self::Json(schema) => schema.as_json_value(batch),
            Self::Proto(schema) => schema.as_json_value(batch),
        }
    }
}

impl Generator for Schema {
    fn generate(&self) -> Result<nisshi_sans_io::record::Builder> {
        match self {
            Schema::Avro(schema) => schema.generate(),
            Schema::Json(schema) => schema.generate(),
            Schema::Proto(schema) => schema.generate(),
        }
    }
}

type SchemaCache = Arc<Mutex<BTreeMap<String, CachedSchema>>>;

/// Returns a copy of `url` with any password component removed.
///
/// Intended for logging or displaying a connection URL (storage,
/// schema registry, lake location, etc.) without leaking a plaintext
/// password from the userinfo component. Only the password is
/// touched: everything else, including any query parameters, passes
/// through unchanged.
///
/// This does not redact a password passed as a query parameter (e.g.
/// `postgres://user@host/db?password=secret`, which `tokio-postgres`
/// accepts): only the userinfo component is covered. Prefer the
/// userinfo form for any URL that flows through this function.
///
/// `url` itself is never mutated: this clones before redacting, so the
/// value actually used to establish a connection is unaffected.
pub fn redact_url(url: &Url) -> Url {
    let mut redacted = url.clone();

    if redacted.password().is_some() {
        let _ = redacted.set_password(None);
    }

    redacted
}

// Schema Registry
#[derive(Clone, Debug)]
pub struct Registry {
    object_store: Arc<DynObjectStore>,
    schemas: SchemaCache,
    cache_expiry_after: Option<Duration>,
}

impl FromStr for Registry {
    type Err = Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        Url::parse(s)
            .map_err(Into::into)
            .and_then(|location| Builder::try_from(&location))
            .map(Into::into)
    }
}

// Schema Registry builder
#[derive(Clone, Debug)]
pub struct Builder {
    object_store: Arc<DynObjectStore>,
    cache_expiry_after: Option<Duration>,
}

impl TryFrom<&Url> for Builder {
    type Error = Error;

    fn try_from(storage: &Url) -> Result<Self, Self::Error> {
        debug!(storage = %redact_url(storage));

        match storage.scheme() {
            "s3" => {
                let bucket_name = storage.host_str().unwrap_or("schema");

                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket_name)
                    .build()
                    .map_err(Into::into)
                    .map(Self::new)
            }

            "file" => {
                let mut path = env::current_dir().inspect(|current_dir| debug!(?current_dir))?;

                if let Some(domain) = storage.domain() {
                    path.push(domain);
                }

                if let Some(relative) = storage.path().strip_prefix("/") {
                    path.push(relative);
                } else {
                    path.push(storage.path());
                }

                debug!(?path);

                LocalFileSystem::new_with_prefix(path)
                    .map_err(Into::into)
                    .map(Self::new)
            }

            "memory" => Ok(Self::new(InMemory::new())),

            _unsupported => Err(Error::UnsupportedSchemaRegistryUrl(redact_url(storage))),
        }
    }
}

impl From<Builder> for Registry {
    fn from(builder: Builder) -> Self {
        Self {
            object_store: builder.object_store,
            schemas: Arc::new(Mutex::new(BTreeMap::new())),
            cache_expiry_after: builder.cache_expiry_after,
        }
    }
}

impl Builder {
    pub fn new(object_store: impl ObjectStore) -> Self {
        Self {
            object_store: Arc::new(object_store),
            cache_expiry_after: None,
        }
    }

    pub fn with_cache_expiry_after(self, cache_expiry_after: Option<Duration>) -> Self {
        Self {
            cache_expiry_after,
            ..self
        }
    }

    pub fn build(self) -> Registry {
        Registry::from(self)
    }
}

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

static VALIDATION_DURATION: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    METER
        .u64_histogram("registry_validation_duration")
        .with_unit("ms")
        .with_description("The registry validation request latencies in milliseconds")
        .build()
});

static VALIDATION_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("registry_validation_error")
        .with_description("The registry validation error count")
        .build()
});

impl Registry {
    pub fn new(object_store: impl ObjectStore) -> Self {
        Builder::new(object_store).build()
    }

    pub fn builder(object_store: impl ObjectStore) -> Builder {
        Builder::new(object_store)
    }

    pub fn builder_try_from_url(url: &Url) -> Result<Builder> {
        Builder::try_from(url)
    }

    #[instrument(skip(self))]
    pub async fn schema(&self, topic: &str) -> Result<Option<Schema>> {
        let proto = Path::from(format!("{topic}.proto"));
        let json = Path::from(format!("{topic}.json"));
        let avro = Path::from(format!("{topic}.avsc"));

        if let Some(cached) = self.schemas.lock().map(|guard| guard.get(topic).cloned())? {
            if self.cache_expiry_after.is_some_and(|cache_expiry_after| {
                SystemTime::now()
                    .duration_since(cached.loaded_at)
                    .unwrap_or_default()
                    > cache_expiry_after
            }) {
                return Ok(Some(cached.schema));
            } else {
                debug!(cache_expiry = topic);
            }
        }

        if let Ok(get_result) = self
            .object_store
            .get(&proto)
            .await
            .inspect(|get_result| debug!(?get_result))
            .inspect_err(|err| debug!(?err))
        {
            get_result
                .bytes()
                .await
                .map_err(Into::into)
                .and_then(proto::Schema::try_from)
                .map(Box::new)
                .map(Schema::Proto)
                .and_then(|schema| {
                    self.schemas
                        .lock()
                        .map_err(Into::into)
                        .map(|mut guard| {
                            guard.insert(topic.to_owned(), CachedSchema::new(schema.clone()))
                        })
                        .and(Ok(Some(schema)))
                })
        } else if let Ok(get_result) = self.object_store.get(&json).await {
            get_result
                .bytes()
                .await
                .map_err(Into::into)
                .and_then(json::Schema::try_from)
                .map(Arc::new)
                .map(Schema::Json)
                .and_then(|schema| {
                    self.schemas
                        .lock()
                        .map_err(Into::into)
                        .map(|mut guard| {
                            guard.insert(topic.to_owned(), CachedSchema::new(schema.clone()))
                        })
                        .and(Ok(Some(schema)))
                })
        } else if let Ok(get_result) = self.object_store.get(&avro).await {
            get_result
                .bytes()
                .await
                .map_err(Into::into)
                .and_then(avro::Schema::try_from)
                .map(Box::new)
                .map(Schema::Avro)
                .and_then(|schema| {
                    self.schemas
                        .lock()
                        .map_err(Into::into)
                        .map(|mut guard| {
                            guard.insert(topic.to_owned(), CachedSchema::new(schema.clone()))
                        })
                        .and(Ok(Some(schema)))
                })
        } else {
            Ok(None)
        }
    }

    #[instrument(skip(self, batch))]
    pub async fn validate(&self, topic: &str, batch: &Batch) -> Result<()> {
        let validation_start = SystemTime::now();

        let Some(schema) = self.schema(topic).await? else {
            debug!(no_schema_for_topic = %topic);
            return Ok(());
        };

        schema
            .validate(batch)
            .inspect(|_| {
                VALIDATION_DURATION.record(
                    validation_start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[KeyValue::new("topic", topic.to_owned())],
                )
            })
            .inspect_err(|err| {
                VALIDATION_ERROR.add(
                    1,
                    &[
                        KeyValue::new("topic", topic.to_owned()),
                        KeyValue::new("reason", err.to_string()),
                    ],
                )
            })
    }
}

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
impl AsArrow for Registry {
    #[instrument(skip(self, batch))]
    async fn as_arrow(
        &self,
        topic: &str,
        partition: i32,
        batch: &Batch,
        lake_type: lake::LakeHouseType,
    ) -> Result<RecordBatch> {
        let start = SystemTime::now();

        let schema = self
            .schema(topic)
            .await
            .and_then(|schema| schema.ok_or(Error::TopicWithoutSchema(topic.to_owned())))?;

        schema
            .as_arrow(topic, partition, batch, lake_type)
            .await
            .inspect(|_| {
                lake::AS_ARROW_DURATION.record(
                    start
                        .elapsed()
                        .map_or(0, |duration| duration.as_millis() as u64),
                    &[KeyValue::new("topic", topic.to_owned())],
                )
            })
            .inspect_err(|err| debug!(?err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Result;
    use bytes::Bytes;
    use nisshi_sans_io::record::Record;
    use object_store::PutPayload;
    use serde_json::json;
    use std::{fs::File, sync::Arc, thread};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

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

    const DEF_PROTO: &[u8] = br#"
      syntax = 'proto3';

      message Key {
        int32 id = 1;
      }

      message Value {
        string name = 1;
        string email = 2;
      }
    "#;

    const PQR_AVRO: &[u8] = br#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "a", "type": "long", "default": 42},
                {"name": "b", "type": "string"},
                {"name": "c", "type": "long", "default": 43}
            ]
        }
    "#;

    async fn populate() -> Result<Registry> {
        let _guard = init_tracing()?;

        let object_store = InMemory::new();

        let location = Path::from("abc.json");
        let payload = serde_json::to_vec(&json!({
            "type": "object",
            "properties": {
                "key": {
                    "type": "number",
                    "multipleOf": 10
                }
            }
        }))
        .map(Bytes::from)
        .map(PutPayload::from)?;

        _ = object_store.put(&location, payload).await?;

        let location = Path::from("def.proto");
        let payload = PutPayload::from(Bytes::from_static(DEF_PROTO));
        _ = object_store.put(&location, payload).await?;

        let location = Path::from("pqr.avsc");
        let payload = PutPayload::from(Bytes::from_static(PQR_AVRO));
        _ = object_store.put(&location, payload).await?;

        Ok(Registry::new(object_store))
    }

    #[tokio::test]
    async fn abc_valid() -> Result<()> {
        let _guard = init_tracing()?;

        let registry = populate().await?;

        let key = Bytes::from_static(b"5450");

        let batch = Batch::builder()
            .record(Record::builder().key(key.clone().into()))
            .build()?;

        registry.validate("abc", &batch).await?;

        Ok(())
    }

    #[tokio::test]
    async fn abc_invalid() -> Result<()> {
        let _guard = init_tracing()?;
        let registry = populate().await?;

        let key = Bytes::from_static(b"545");

        let batch = Batch::builder()
            .record(Record::builder().key(key.clone().into()))
            .build()?;

        assert!(matches!(
            registry
                .validate("abc", &batch)
                .await
                .inspect_err(|err| debug!(?err)),
            Err(Error::Api(ErrorCode::InvalidRecord))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn pqr_valid() -> Result<()> {
        let _guard = init_tracing()?;
        let registry = populate().await?;

        let key = Bytes::from_static(b"5450");

        let batch = Batch::builder()
            .record(Record::builder().key(key.clone().into()))
            .build()?;

        registry.validate("pqr", &batch).await?;

        Ok(())
    }

    #[test]
    fn error_size_of() -> Result<()> {
        let _guard = init_tracing()?;

        debug!(error = size_of::<Error>());
        debug!(anyhow = size_of::<anyhow::Error>());
        #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
        debug!(arrow = size_of::<ArrowError>());
        debug!(avro_to_json = size_of::<apache_avro::types::Value>());
        #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
        debug!(data_file_builder = size_of::<DataFileBuilderError>());
        #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
        debug!(data_fusion = size_of::<Box<DataFusionError>>());
        #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
        debug!(delta_table = size_of::<Box<DeltaTableError>>());
        #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
        debug!(iceberg = size_of::<Box<::iceberg::Error>>());
        debug!(sans_io = size_of::<nisshi_sans_io::Error>());
        debug!(object_store = size_of::<object_store::Error>());

        #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
        debug!(parquet = size_of::<ParquetError>());

        debug!(parse_filter = size_of::<ParseError>());
        debug!(protobuf_json_mapping = size_of::<protobuf_json_mapping::ParseError>());
        debug!(protobuf_json_mapping_print = size_of::<protobuf_json_mapping::PrintError>());
        debug!(protobuf = size_of::<protobuf::Error>());
        debug!(serde_json = size_of::<serde_json::Error>());
        #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
        debug!(sql_parser = size_of::<datafusion::logical_expr::sqlparser::parser::ParserError>());
        debug!(try_from_int = size_of::<TryFromIntError>());
        debug!(url = size_of::<Url>());
        #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
        debug!(unsupported_schema_runtime_value = size_of::<(DataType, serde_json::Value)>());
        debug!(uuid = size_of::<uuid::Error>());
        Ok(())
    }

    #[test]
    fn redact_url_strips_password_only() -> Result<()> {
        let url = Url::parse("postgres://user:secret@host/db")?;
        let redacted = redact_url(&url);

        assert_eq!(None, redacted.password());
        assert_eq!("user", redacted.username());
        assert_eq!(Some("host"), redacted.host_str());
        assert!(!redacted.as_str().contains("secret"));

        // the original is untouched: it's the value still used to connect
        assert_eq!(Some("secret"), url.password());

        Ok(())
    }

    #[test]
    fn redact_url_leaves_passwordless_url_unchanged() -> Result<()> {
        let url = Url::parse("memory://nisshi/")?;
        assert_eq!(url, redact_url(&url));
        Ok(())
    }

    #[test]
    fn unsupported_schema_registry_url_does_not_leak_password() -> Result<()> {
        let _guard = init_tracing()?;

        let url = Url::parse("bogus://user:secret@host/db")?;
        let err = Builder::try_from(&url).unwrap_err();

        // `Error` has no `#[error(...)]` attributes, so `Display` falls back
        // to `Debug`, e.g. `UnsupportedSchemaRegistryUrl(Url { username:
        // "user", password: None, host: Some(Domain("host")), .. })`.
        let message = err.to_string();
        assert!(!message.contains("secret"));
        assert!(message.contains("\"user\""));
        assert!(message.contains("\"host\""));

        let log = std::fs::read_to_string(format!(
            "../logs/{}/{}.log",
            env!("CARGO_PKG_NAME"),
            thread::current().name().unwrap()
        ))?;
        assert!(!log.contains("secret"));
        assert!(log.contains("user@host"));

        Ok(())
    }
}
