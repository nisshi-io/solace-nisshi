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

//! AVRO schema

use std::collections::HashMap;

use apache_avro::{
    Reader,
    schema::{Schema as AvroSchema, SchemaKind},
    types::Value,
};
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};

use nisshi_sans_io::{ErrorCode, record::inflated::Batch};
use serde_json::{Map, Number, Value as JsonValue};
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::{AsJsonValue, AsKafkaRecord, Error, Generator, Result, Validator};

mod container;

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
use apache_avro::schema::RecordSchema;

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
mod arrow;

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum MessageKind {
    Key,
    Meta,
    Value,
}

impl AsRef<str> for MessageKind {
    fn as_ref(&self) -> &str {
        match self {
            MessageKind::Key => "key",
            MessageKind::Meta => "meta",
            MessageKind::Value => "value",
        }
    }
}

/// AVRO Schema
#[derive(Clone, Debug, Default)]
pub struct Schema {
    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    complete: Option<RecordSchema>,
    pub(crate) key: Option<AvroSchema>,
    pub(crate) value: Option<AvroSchema>,
    pub(crate) meta: Option<AvroSchema>,

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    ids: HashMap<String, i32>,
}

impl Schema {
    pub fn key(&self) -> Option<&AvroSchema> {
        self.key.as_ref()
    }

    pub fn value(&self) -> Option<&AvroSchema> {
        self.value.as_ref()
    }

    pub fn meta(&self) -> Option<&AvroSchema> {
        self.meta.as_ref()
    }
}

impl TryFrom<Bytes> for Schema {
    type Error = Error;

    fn try_from(encoded: Bytes) -> Result<Self, Self::Error> {
        const FIELDS: &str = "fields";

        let meta =
            serde_json::from_slice::<JsonValue>(&Bytes::from_static(include_bytes!("meta.avsc")))
                .inspect(|meta| debug!(%meta))
                .map(|mut meta| meta[FIELDS].take())
                .inspect(|meta| debug!(%meta))?;

        serde_json::from_slice::<JsonValue>(&encoded[..])
            .map(|mut schema| {
                _ = schema
                    .get_mut(FIELDS)
                    .and_then(|fields| fields.as_object_mut())
                    .and_then(|object| object.insert(MessageKind::Meta.as_ref().to_owned(), meta));
                schema
            })
            .map_err(Into::into)
            .map(Self::from)
    }
}

impl From<JsonValue> for Schema {
    fn from(mut schema: JsonValue) -> Self {
        debug!(%schema);

        const FIELDS: &str = "fields";

        let meta =
            serde_json::from_slice::<JsonValue>(&Bytes::from_static(include_bytes!("meta.avsc")))
                .inspect(|meta| debug!(%meta))
                .ok();

        let schema = {
            if let Some(meta) = meta
                && let Some(fields) = schema.get_mut(FIELDS)
                && let Some(array) = fields.as_array_mut()
            {
                array.push(JsonValue::Object(Map::from_iter([
                    ("name".into(), MessageKind::Meta.as_ref().into()),
                    ("type".into(), meta),
                ])))
            }

            debug!(%schema);

            schema
        };

        schema
            .get(FIELDS)
            .inspect(|fields| debug!(?fields))
            .and_then(|fields| fields.as_array())
            .inspect(|fields| debug!(?fields))
            .map_or(
                Self {
                    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
                    complete: None,
                    key: None,
                    value: None,
                    meta: None,
                    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
                    ids: HashMap::new(),
                },
                |fields| {
                    if let Ok(schema) =
                        AvroSchema::parse(&schema).inspect_err(|err| error!(?err, ?schema))
                    {
                        #[cfg(not(any(
                            feature = "parquet",
                            feature = "iceberg",
                            feature = "delta"
                        )))]
                        let _ = schema;

                        Self {
                            #[cfg(any(
                                feature = "parquet",
                                feature = "iceberg",
                                feature = "delta"
                            ))]
                            ids: field_ids(&schema),

                            #[cfg(any(
                                feature = "parquet",
                                feature = "iceberg",
                                feature = "delta"
                            ))]
                            complete: if let AvroSchema::Record(record) = schema {
                                Some(record)
                            } else {
                                None
                            },

                            key: fields
                                .iter()
                                .find(|field| {
                                    field
                                        .get("name")
                                        .is_some_and(|name| name == MessageKind::Key.as_ref())
                                })
                                .inspect(|value| debug!(?value))
                                .and_then(|schema| {
                                    AvroSchema::parse(schema)
                                        .inspect_err(|err| error!(?err, ?schema))
                                        .ok()
                                }),

                            value: fields
                                .iter()
                                .find(|field| {
                                    field
                                        .get("name")
                                        .is_some_and(|name| name == MessageKind::Value.as_ref())
                                })
                                .inspect(|value| debug!(?value))
                                .and_then(|schema| {
                                    AvroSchema::parse(schema)
                                        .inspect_err(|err| error!(?err, ?schema))
                                        .ok()
                                }),

                            meta: fields
                                .iter()
                                .find(|field| {
                                    field
                                        .get("name")
                                        .is_some_and(|name| name == MessageKind::Meta.as_ref())
                                })
                                .inspect(|value| debug!(?value))
                                .and_then(|schema| {
                                    AvroSchema::parse(schema)
                                        .inspect_err(|err| error!(?err, ?schema))
                                        .ok()
                                }),
                        }
                    } else {
                        Self {
                            #[cfg(any(
                                feature = "parquet",
                                feature = "iceberg",
                                feature = "delta"
                            ))]
                            complete: None,
                            key: None,
                            value: None,
                            meta: None,
                            #[cfg(any(
                                feature = "parquet",
                                feature = "iceberg",
                                feature = "delta"
                            ))]
                            ids: HashMap::new(),
                        }
                    }
                },
            )
    }
}

impl Schema {
    fn to_json_value(
        &self,
        message_kind: MessageKind,
        schema: Option<&AvroSchema>,
        encoded: Option<Bytes>,
    ) -> Result<(String, JsonValue)> {
        decode(schema, encoded).and_then(|decoded| {
            decoded.map_or(
                Ok((message_kind.as_ref().to_owned(), JsonValue::Null)),
                |value| json_value(value).map(|value| (message_kind.as_ref().to_owned(), value)),
            )
        })
    }
}

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
fn field_ids(schema: &AvroSchema) -> HashMap<String, i32> {
    use crate::ARROW_LIST_FIELD_NAME;

    fn field_ids_with_path(
        path: &[&str],
        schema: &AvroSchema,
        id: &mut i32,
    ) -> HashMap<String, i32> {
        debug!(?path, ?schema, id);

        let mut ids = HashMap::new();

        match schema {
            AvroSchema::Array(inner) => {
                let mut path = Vec::from(path);
                path.push(ARROW_LIST_FIELD_NAME);
                _ = ids.insert(path.join("."), *id);
                *id += 1;

                ids.extend(field_ids_with_path(&path[..], &inner.items, id));
            }

            AvroSchema::Map(inner) => {
                let mut path = Vec::from(path);
                path.push("entries");
                _ = ids.insert(path.join("."), *id);
                *id += 1;

                {
                    let mut path = path.clone();
                    path.push("keys");
                    _ = ids.insert(path.join("."), *id);
                    *id += 1;
                }

                {
                    let mut path = path.clone();
                    path.push("values");
                    _ = ids.insert(path.join("."), *id);
                    *id += 1;

                    ids.extend(field_ids_with_path(&path[..], &inner.types, id))
                }
            }

            AvroSchema::Record(inner) => {
                for field in inner.fields.iter() {
                    let mut path = Vec::from(path);
                    path.push(field.name.as_str());

                    _ = ids.insert(path.join("."), *id);
                    *id += 1;
                }

                for field in inner.fields.iter() {
                    let mut path = Vec::from(path);
                    path.push(field.name.as_str());
                    ids.extend(field_ids_with_path(&path[..], &field.schema, id))
                }
            }

            _ => (),
        }

        ids
    }

    field_ids_with_path(&[], schema, &mut 1)
}

fn decode(validator: Option<&AvroSchema>, encoded: Option<Bytes>) -> Result<Option<Value>> {
    debug!(?validator, len = ?encoded.as_ref().map(Bytes::len));
    validator.map_or(Ok(None), |schema| {
        encoded.map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
            // apache_avro decompresses a block without a limit on its decompressed size, so a few
            // compressed bytes can expand to gigabytes. We refuse every codec except `null`
            // before the reader reads a block, because Nisshi writes Avro with `null`.
            container::require_null_codec(&encoded).inspect_err(|_| {
                debug!("record is not an Avro object container with the null codec")
            })?;

            // The apache_avro error writes the record's values into its
            // message, so we log that the record failed and not the error.
            Reader::with_schema(schema, &encoded[..])
                .and_then(|reader| reader.into_iter().next().transpose())
                .inspect_err(|_| debug!("record does not match its Avro schema"))
                .map_err(|_| Error::Api(ErrorCode::InvalidRecord))
                .and_then(|value| value.ok_or(Error::Api(ErrorCode::InvalidRecord)))
                .map(Some)
        })
    })
}

fn validate(validator: Option<&AvroSchema>, encoded: Option<Bytes>) -> Result<()> {
    decode(validator, encoded).and(Ok(()))
}

impl Validator for Schema {
    fn validate(&self, batch: &Batch) -> Result<()> {
        debug!(?batch);

        for record in &batch.records {
            debug!(?record);

            validate(self.key.as_ref(), record.key.clone())
                .and(validate(self.value.as_ref(), record.value.clone()))
                .inspect_err(|err| info!(?err, ?batch))?
        }

        Ok(())
    }
}

fn from_json(schema: &AvroSchema, json: &JsonValue) -> Result<Value> {
    debug!(?schema);

    match (schema, json) {
        (AvroSchema::Null, JsonValue::Null) => Ok(Value::Null),

        (AvroSchema::Boolean, JsonValue::Bool(value)) => Ok(Value::Boolean(*value)),

        (AvroSchema::Int, JsonValue::Number(value)) => value
            .as_i64()
            .ok_or(Error::JsonToAvro(
                Box::new(schema.to_owned()),
                crate::json_kind(json),
            ))
            .and_then(|value| i32::try_from(value).map_err(Into::into))
            .map(Value::Int)
            .inspect_err(|err| debug!(?schema, ?err)),

        (AvroSchema::Long, JsonValue::Number(value)) => value
            .as_i64()
            .ok_or(Error::JsonToAvro(
                Box::new(schema.to_owned()),
                crate::json_kind(json),
            ))
            .map(Value::Long),

        (AvroSchema::Double, JsonValue::Number(value)) => value
            .as_f64()
            .ok_or(Error::JsonToAvro(
                Box::new(schema.to_owned()),
                crate::json_kind(json),
            ))
            .map(Value::Double)
            .inspect_err(|err| debug!(?schema, ?err)),

        (AvroSchema::Float, JsonValue::Number(value)) => value
            .as_f64()
            .ok_or(Error::JsonToAvro(
                Box::new(schema.to_owned()),
                crate::json_kind(json),
            ))
            .map(|double| double as f32)
            .map(Value::Float)
            .inspect_err(|err| debug!(?schema, ?err)),

        (AvroSchema::Uuid, JsonValue::String(value)) => {
            Uuid::parse_str(value).map_err(Into::into).map(Value::Uuid)
        }

        (AvroSchema::Bytes, JsonValue::String(value)) => {
            Ok(Value::Bytes(value.as_bytes().to_vec()))
        }

        (AvroSchema::TimestampMillis, JsonValue::String(value)) => {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
                .map(|date_time| date_time.and_utc().timestamp_millis())
                .map(Value::TimestampMillis)
                .inspect_err(|err| debug!(?err))
                .map_err(Into::into)
        }

        (AvroSchema::TimestampMicros, JsonValue::String(value)) => {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
                .map(|date_time| date_time.and_utc().timestamp_micros())
                .map(Value::TimestampMicros)
                .inspect_err(|err| debug!(?err))
                .map_err(Into::into)
        }

        (AvroSchema::TimestampNanos, JsonValue::String(value)) => {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
                .inspect_err(|err| debug!(?err))
                .map_err(Into::into)
                .and_then(|date_time| {
                    date_time
                        .and_utc()
                        .timestamp_nanos_opt()
                        .ok_or(Error::JsonToAvro(
                            Box::new(schema.to_owned()),
                            crate::json_kind(json),
                        ))
                })
                .map(Value::TimestampNanos)
        }

        (AvroSchema::Enum(inner), JsonValue::String(value)) => inner
            .symbols
            .iter()
            .enumerate()
            .find(|(_, symbol)| *symbol == value)
            .ok_or(Error::JsonToAvro(
                Box::new(schema.to_owned()),
                crate::json_kind(json),
            ))
            .and_then(|(index, symbol)| {
                u32::try_from(index)
                    .map(|index| (index, symbol))
                    .map_err(Into::into)
            })
            .map(|(index, symbol)| Value::Enum(index, symbol.to_owned())),

        (AvroSchema::String, JsonValue::String(value)) => Ok(Value::String(value.to_owned())),

        (AvroSchema::Array(schema), JsonValue::Array(values)) => values
            .iter()
            .map(|value| from_json(schema.items.as_ref(), value))
            .collect::<Result<Vec<_>>>()
            .map(Value::Array)
            .inspect_err(|err| debug!(?schema, ?err)),

        (AvroSchema::Map(inner), JsonValue::Object(values)) => values
            .iter()
            .map(|(k, v)| from_json(inner.types.as_ref(), v).map(|v| (k.to_owned(), v)))
            .collect::<Result<HashMap<_, _>>>()
            .map(Value::Map),

        (AvroSchema::Record(record), JsonValue::Object(value)) => record
            .fields
            .iter()
            .map(|field| {
                value
                    .get(&field.name)
                    .ok_or(Error::JsonToAvroFieldNotFound {
                        schema: Box::new(schema.to_owned()),
                        field: field.name.clone(),
                    })
                    .and_then(|value| from_json(&field.schema, value))
                    .inspect(|_| debug!(name = ?field.name))
                    .map(|value| (field.name.clone(), value))
            })
            .collect::<Result<Vec<_>>>()
            .map(Value::Record)
            .inspect_err(|err| debug!(%err)),

        (schema, value) => Err(Error::JsonToAvro(
            Box::new(schema.to_owned()),
            crate::json_kind(value),
        )),
    }
}

impl AsKafkaRecord for Schema {
    fn as_kafka_record(&self, value: &JsonValue) -> Result<nisshi_sans_io::record::Builder> {
        let mut builder = nisshi_sans_io::record::Record::builder();

        if let Some(value) = value.get(MessageKind::Key.as_ref())
            && let Some(ref schema) = self.key
        {
            builder = builder.key(
                from_json(schema, value)
                    .and_then(|value| schema_write(schema, value))
                    .map(Into::into)?,
            );
        }

        if let Some(value) = value.get(MessageKind::Value.as_ref())
            && let Some(ref schema) = self.value
        {
            builder = builder.value(
                from_json(schema, value)
                    .and_then(|value| schema_write(schema, value))
                    .map(Into::into)?,
            );
        }

        Ok(builder)
    }
}

impl Generator for Schema {
    fn generate(&self) -> Result<nisshi_sans_io::record::Builder> {
        todo!()
    }
}

fn timestamp(value: &Value, date_time: Option<DateTime<Utc>>) -> Result<JsonValue> {
    date_time
        .map(|date_time| {
            JsonValue::String(
                date_time
                    .naive_utc()
                    .format("%Y-%m-%dT%H:%M:%S%.f")
                    .to_string(),
            )
        })
        .ok_or(Error::AvroToJson(SchemaKind::from(value)))
}

fn json_value(value: Value) -> Result<JsonValue> {
    match value {
        Value::Null => Ok(JsonValue::Null),

        Value::Boolean(inner) => Ok(JsonValue::Bool(inner)),

        Value::Int(inner) => Ok(JsonValue::Number(Number::from(inner))),

        Value::Long(inner) => Ok(JsonValue::Number(Number::from(inner))),

        Value::Float(inner) => Number::from_f64(inner as f64)
            .ok_or(Error::AvroToJson(SchemaKind::from(&value)))
            .map(JsonValue::Number),

        Value::Double(inner) => Number::from_f64(inner)
            .ok_or(Error::AvroToJson(SchemaKind::from(&value)))
            .map(JsonValue::Number),

        Value::Bytes(inner) => Ok(JsonValue::String(String::from(String::from_utf8_lossy(
            &inner[..],
        )))),

        Value::String(inner) | Value::Enum(_, inner) => Ok(JsonValue::String(inner)),

        Value::Fixed(_, inner) => Ok(JsonValue::String(
            String::from_utf8_lossy(&inner[..]).into_owned(),
        )),

        Value::Union(_, value) => json_value(*value),

        Value::Array(values) => values
            .into_iter()
            .map(json_value)
            .collect::<Result<Vec<_>>>()
            .map(JsonValue::Array),

        Value::Map(inner) => inner
            .into_iter()
            .map(|(k, v)| json_value(v).map(|v| (k, v)))
            .collect::<Result<Vec<_>>>()
            .map(Map::from_iter)
            .map(JsonValue::Object),

        Value::Record(inner) => inner
            .into_iter()
            .map(|(k, v)| json_value(v).map(|v| (k, v)))
            .collect::<Result<Vec<_>>>()
            .map(Map::from_iter)
            .map(JsonValue::Object),

        // `cat produce` reads a timestamp as a string in this form, so `cat consume` writes one
        // the same way and a record round-trips.
        Value::TimestampMillis(inner) => timestamp(&value, DateTime::from_timestamp_millis(inner)),
        Value::TimestampMicros(inner) => timestamp(&value, DateTime::from_timestamp_micros(inner)),
        Value::TimestampNanos(inner) => {
            timestamp(&value, Some(DateTime::from_timestamp_nanos(inner)))
        }

        // A local timestamp has no zone, and the string above has none either, so a local
        // timestamp prints in the same form as a UTC one.
        Value::LocalTimestampMillis(inner) => {
            timestamp(&value, DateTime::from_timestamp_millis(inner))
        }
        Value::LocalTimestampMicros(inner) => {
            timestamp(&value, DateTime::from_timestamp_micros(inner))
        }
        Value::LocalTimestampNanos(inner) => {
            timestamp(&value, Some(DateTime::from_timestamp_nanos(inner)))
        }

        // The other logical types are written as their underlying type, as the Avro
        // specification's JSON encoding does.
        Value::Date(inner) | Value::TimeMillis(inner) => Ok(JsonValue::Number(Number::from(inner))),

        Value::TimeMicros(inner) => Ok(JsonValue::Number(Number::from(inner))),

        Value::BigDecimal(inner) => Ok(JsonValue::String(inner.to_string())),

        // A decimal's scale is in the schema, which this function is not given, and a duration
        // has no JSON encoding. Refuse the record rather than guess.
        Value::Decimal(_) | Value::Duration(_) => Err(Error::AvroToJson(SchemaKind::from(&value))),

        Value::Uuid(uuid) => json_value(Value::String(uuid.to_string())),
    }
}

impl AsJsonValue for Schema {
    fn as_json_value(&self, batch: &Batch) -> Result<JsonValue> {
        batch
            .records
            .iter()
            .map(|record| {
                // A side that fails is an error, not a missing field: `Result` is an iterator
                // of zero items for an `Err`, which would drop the field from the output.
                let key =
                    self.to_json_value(MessageKind::Key, self.key.as_ref(), record.key.clone())?;
                let value = self.to_json_value(
                    MessageKind::Value,
                    self.value.as_ref(),
                    record.value.clone(),
                )?;

                Ok(JsonValue::Object(Map::from_iter([key, value])))
            })
            .collect::<Result<Vec<_>>>()
            .map(JsonValue::Array)
    }
}

#[doc(hidden)]
pub fn r<'a>(
    schema: &AvroSchema,
    fields: impl IntoIterator<Item = (&'a str, Value)>,
) -> Result<apache_avro::types::Record<'_>> {
    apache_avro::types::Record::new(schema)
        .map(|mut record| {
            for (name, value) in fields {
                record.put(name, value);
            }
            record
        })
        .ok_or_else(|| Error::Message(format!("not a record schema: {schema:?}")))
}

#[doc(hidden)]
pub fn schema_write(schema: &AvroSchema, value: Value) -> Result<Bytes> {
    debug!(?schema);
    let mut writer = apache_avro::Writer::new(schema, vec![]);
    _ = writer.append(value).map_err(|_| Error::AvroRecord)?;
    writer.into_inner().map(Bytes::from).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use std::{assert_matches, fs::File, sync::Arc, thread};

    use crate::Registry;

    use super::*;
    use apache_avro::{Codec, DeflateSettings, Reader, types::Value};

    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path};

    use nisshi_sans_io::record::Record;
    use serde_json::json;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;
    use uuid::Uuid;

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

    #[tokio::test]
    async fn key_only_invalid_record() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let schema = json!({
            "type": "record",
            "name": "Test",
            "fields": [{
                "name": "key",
                "type": "int"
            },
            {
                "name": "value",
                "type": {
                    "type": "record",
                    "name": "person",
                    "fields": [{
                        "name": "name",
                        "type": "string"
                    },
                    {
                        "name": "email",
                        "type": "string"
                    }]
                }
        }]});

        let object_store = InMemory::new();
        {
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&schema)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;
        }

        let registry = Registry::new(object_store);

        let key = AvroSchema::parse(&json!({
            "type": "int"
        }))
        .and_then(|schema| {
            let mut writer = apache_avro::Writer::new(&schema, vec![]);
            writer
                .append(Value::Int(32123))
                .and(writer.into_inner())
                .map(Bytes::from)
        })?;

        let batch = Batch::builder()
            .record(Record::builder().key(key.into()))
            .build()?;

        assert!(matches!(
            registry.validate(topic, &batch).await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn key_and_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let schema = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "key", "type": "int"},
                {"name": "value", "type": {
                    "type": "record",
                    "fields": [
                        {"name": "name", "type": "string"},
                        {"name": "email", "type": "string"}]}}]});

        let object_store = InMemory::new();
        {
            let location = Path::from(format!("{topic}/.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&schema)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;
        }

        let registry = Registry::new(object_store);

        let key = AvroSchema::parse(&json!({
            "type": "int"
        }))
        .and_then(|schema| {
            let mut writer = apache_avro::Writer::new(&schema, vec![]);
            writer
                .append(Value::Int(32123))
                .and(writer.into_inner())
                .map(Bytes::from)
        })?;

        let value = AvroSchema::parse(&json!({
            "type": "record",
            "name": "Message",
            "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": "string"}]
        }))
        .and_then(|schema| {
            let mut writer = apache_avro::Writer::new(&schema, vec![]);
            let mut record = apache_avro::types::Record::new(&schema).unwrap();
            record.put("name", "alice");
            record.put("email", "alice@example.com");

            writer
                .append(record)
                .and(writer.into_inner())
                .map(Bytes::from)
        })?;

        let batch = Batch::builder()
            .record(Record::builder().key(key.into()).value(value.into()))
            .build()?;

        registry.validate(topic, &batch).await
    }

    #[tokio::test]
    async fn value_only_invalid_record() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let schema = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "key", "type": "int"},
                {"name": "value", "type": {
                    "name": "value",
                    "type": "record",
                    "fields": [
                        {"name": "name", "type": "string"},
                        {"name": "email", "type": "string"}]}}]});

        let object_store = InMemory::new();
        {
            let location = Path::from(format!("{topic}.avsc"));
            _ = object_store
                .put(
                    &location,
                    serde_json::to_vec(&schema)
                        .map(Bytes::from)
                        .map(PutPayload::from)?,
                )
                .await?;
        }

        let registry = Registry::new(object_store);

        let value = AvroSchema::parse(&json!({
            "type": "record",
            "name": "Message",
            "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": "string"}]
        }))
        .and_then(|schema| {
            let mut writer = apache_avro::Writer::new(&schema, vec![]);
            let mut record = apache_avro::types::Record::new(&schema).unwrap();
            record.put("name", "alice");
            record.put("email", "alice@example.com");

            writer
                .append(record)
                .and(writer.into_inner())
                .map(Bytes::from)
        })?;

        let batch = Batch::builder()
            .record(Record::builder().value(value.into()))
            .build()?;

        assert!(matches!(
            registry.validate(topic, &batch).await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn no_schema() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let registry = Registry::new(InMemory::new());

        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");
        let value = Bytes::from_static(b"Consectetur adipiscing elit");

        let batch = Batch::builder()
            .record(
                Record::builder()
                    .key(key.clone().into())
                    .value(value.clone().into()),
            )
            .build()?;

        registry.validate(topic, &batch).await
    }

    #[test]
    fn key() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = Schema::from(json!({
            "type": "record",
            "name": "test",
            "fields": [{"name": "key", "type": "int"}]
        }));

        let input = {
            let mut writer = apache_avro::Writer::new(schema.key.as_ref().unwrap(), vec![]);

            writer
                .append(Value::Int(32123))
                .and(writer.into_inner())
                .map(Bytes::from)?
        };

        let batch = Batch::builder()
            .record(Record::builder().key(input.clone().into()))
            .build()?;

        schema.validate(&batch)
    }

    #[test]
    fn invalid_key() -> Result<()> {
        let _guard = init_tracing()?;

        let input = {
            let schema = Schema::from(json!({
                "type": "record",
                "name": "test",
                "fields": [{"name": "key", "type": "long"}]
            }));

            let mut writer = apache_avro::Writer::new(schema.key.as_ref().unwrap(), vec![]);
            writer
                .append(Value::Long(32123))
                .and(writer.into_inner())
                .map(Bytes::from)?
        };

        let batch = Batch::builder()
            .record(Record::builder().key(input.clone().into()))
            .build()?;

        let s = Schema::from(json!({
            "type": "record",
            "name": "test",
            "fields": [{
                "name": "key",
                "type": "string"
            }]
        }));

        assert!(matches!(
            s.validate(&batch),
            Err(Error::Api(ErrorCode::InvalidRecord))
        ));
        Ok(())
    }

    #[test]
    fn simple_schema() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = json!({
            "type": "record",
            "name": "Message",
            "fields": [{"name": "title", "type": "string"}, {"name": "message", "type": "string"}]
        });

        let schema = AvroSchema::parse(&schema)?;

        let mut record = apache_avro::types::Record::new(&schema).unwrap();
        record.put("title", "Lorem ipsum dolor sit amet");
        record.put("message", "consectetur adipiscing elit");

        let mut writer = apache_avro::Writer::new(&schema, vec![]);
        assert!(writer.append(record)? > 0);

        let input = writer.into_inner()?;
        let reader = Reader::with_schema(&schema, &input[..])?;

        let v = reader.into_iter().next().unwrap()?;

        assert_eq!(
            Value::Record(vec![
                (
                    "title".into(),
                    Value::String("Lorem ipsum dolor sit amet".into()),
                ),
                (
                    "message".into(),
                    Value::String("consectetur adipiscing elit".into()),
                ),
            ]),
            v
        );

        Ok(())
    }

    #[test]
    fn from_json() -> Result<()> {
        let _guard = init_tracing()?;

        assert_eq!(
            Value::Null,
            super::from_json(&AvroSchema::parse(&json!({"type": "null"}))?, &json!(null))?
        );

        assert_eq!(
            Value::Boolean(true),
            super::from_json(
                &AvroSchema::parse(&json!({"type": "boolean"}))?,
                &json!(true)
            )?
        );

        assert_eq!(
            Value::Boolean(false),
            super::from_json(
                &AvroSchema::parse(&json!({"type": "boolean"}))?,
                &json!(false)
            )?
        );

        assert_eq!(
            Value::Int(i32::MIN),
            super::from_json(
                &AvroSchema::parse(&json!({"type": "int"}))?,
                &json!(i32::MIN)
            )?
        );

        assert_eq!(
            Value::Int(i32::MAX),
            super::from_json(
                &AvroSchema::parse(&json!({"type": "int"}))?,
                &json!(i32::MAX)
            )?
        );

        assert_eq!(
            Value::Long(i64::MIN),
            super::from_json(
                &AvroSchema::parse(&json!({"type": "long"}))?,
                &json!(i64::MIN)
            )?
        );

        assert_eq!(
            Value::Long(i64::MAX),
            super::from_json(
                &AvroSchema::parse(&json!({"type": "long"}))?,
                &json!(i64::MAX)
            )?
        );

        assert_eq!(
            Value::Float(f32::MIN),
            super::from_json(
                &AvroSchema::parse(&json!({"type": "float"}))?,
                &json!(f32::MIN)
            )?
        );

        assert_eq!(
            Value::Float(f32::MAX),
            super::from_json(
                &AvroSchema::parse(&json!({"type": "float"}))?,
                &json!(f32::MAX)
            )?
        );

        assert_eq!(
            Value::Double(f64::MIN),
            super::from_json(
                &AvroSchema::parse(&json!({"type": "double"}))?,
                &json!(f64::MIN)
            )?
        );

        assert_eq!(
            Value::Double(f64::MAX),
            super::from_json(
                &AvroSchema::parse(&json!({"type": "double"}))?,
                &json!(f64::MAX)
            )?
        );

        assert_eq!(
            Value::String("hello world!".into()),
            super::from_json(
                &AvroSchema::parse(&json!({"type": "string"}))?,
                &json!("hello world!")
            )?
        );

        assert_eq!(
            Value::Array(vec![
                Value::String("abc".into()),
                Value::String("pqr".into()),
                Value::String("xyz".into()),
            ]),
            super::from_json(
                &AvroSchema::parse(&json!({"type": "array", "items": "string"}))?,
                &json!(["abc", "pqr", "xyz"])
            )?
        );

        assert_eq!(
            Value::Enum(2, "DIAMONDS".into()),
            super::from_json(
                &AvroSchema::parse(&json!({
                    "type": "enum",
                    "name": "Suit",
                    "symbols": ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
                }))?,
                &json!("DIAMONDS")
            )?
        );

        assert_eq!(
            Value::Bytes([97, 98, 99].into()),
            super::from_json(
                &AvroSchema::parse(&json!({"type": "bytes"}))?,
                &json!("abc")
            )?
        );

        {
            let uuid = Uuid::new_v4();

            assert_eq!(
                Value::Uuid(uuid),
                super::from_json(
                    &AvroSchema::parse(&json!({"type": "string", "logicalType": "uuid"}))?,
                    &json!(uuid.to_string())
                )?
            );
        }

        {
            let value = Value::TimestampMillis(119_731_017_000);

            assert_eq!(
                value,
                super::from_json(
                    &AvroSchema::parse(
                        &json!({"type": "long", "logicalType": "timestamp-millis"})
                    )?,
                    &json!("1973-10-17T18:36:57")
                )?
            );

            let value = Value::TimestampMillis(119_731_017_123);

            assert_eq!(
                value,
                super::from_json(
                    &AvroSchema::parse(
                        &json!({"type": "long", "logicalType": "timestamp-millis"})
                    )?,
                    &json!("1973-10-17T18:36:57.123")
                )?
            );

            assert_eq!(
                value,
                super::from_json(
                    &AvroSchema::parse(
                        &json!({"type": "long", "logicalType": "timestamp-millis"})
                    )?,
                    &json!("1973-10-17T18:36:57.123456")
                )?
            );

            assert_eq!(
                value,
                super::from_json(
                    &AvroSchema::parse(
                        &json!({"type": "long", "logicalType": "timestamp-millis"})
                    )?,
                    &json!("1973-10-17T18:36:57.123456789")
                )?
            );
        }

        {
            assert_eq!(
                Value::TimestampMicros(119_731_017_000_000),
                super::from_json(
                    &AvroSchema::parse(
                        &json!({"type": "long", "logicalType": "timestamp-micros"})
                    )?,
                    &json!("1973-10-17T18:36:57")
                )?
            );

            assert_eq!(
                Value::TimestampMicros(119_731_017_123_000),
                super::from_json(
                    &AvroSchema::parse(
                        &json!({"type": "long", "logicalType": "timestamp-micros"})
                    )?,
                    &json!("1973-10-17T18:36:57.123")
                )?
            );

            assert_eq!(
                Value::TimestampMicros(119_731_017_123_456),
                super::from_json(
                    &AvroSchema::parse(
                        &json!({"type": "long", "logicalType": "timestamp-micros"})
                    )?,
                    &json!("1973-10-17T18:36:57.123456")
                )?
            );

            assert_eq!(
                Value::TimestampMicros(119_731_017_123_456),
                super::from_json(
                    &AvroSchema::parse(
                        &json!({"type": "long", "logicalType": "timestamp-micros"})
                    )?,
                    &json!("1973-10-17T18:36:57.123456789")
                )?
            );
        }

        {
            let v = super::from_json(
                &AvroSchema::parse(&json!({
                    "type": "map",
                    "values": "long"
                }))?,
                &json!({"a": 1, "b": 3, "c": 5}),
            )?;

            assert!(matches!(v, Value::Map(_)));

            let Value::Map(values) = v else {
                panic!("{v:?}")
            };

            assert_eq!(Some(&Value::Long(1)), values.get("a"));
            assert_eq!(Some(&Value::Long(3)), values.get("b"));
            assert_eq!(Some(&Value::Long(5)), values.get("c"));
        }

        {
            let v = super::from_json(
                &AvroSchema::parse(&json!({
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "people",
                    "fields": [
                        {"name": "id", "type": "int"},
                        {"name": "name", "type": "string"},
                        {"name": "lucky", "type": "array", "items": "int"}
                    ]}
                }))?,
                &json!([
                    {"id": 32123, "name": "alice", "lucky": [6]},
                    {"id": 45654, "name": "bob", "lucky": [5, 9]}]),
            )?;

            assert!(matches!(v, Value::Array(_)));

            let Value::Array(values) = v else {
                panic!("{v:?}")
            };

            assert_eq!(2, values.len());

            let Some(Value::Record(r0)) = values.first() else {
                panic!("{:?}", values[0])
            };

            assert_eq!(
                Value::Int(32123),
                r0.iter().find(|(name, _)| name == "id").unwrap().1
            );

            assert_eq!(
                Value::String("alice".into()),
                r0.iter().find(|(name, _)| name == "name").unwrap().1
            );

            assert_eq!(
                Value::Array(vec![Value::Int(6)]),
                r0.iter().find(|(name, _)| name == "lucky").unwrap().1
            );

            let Some(Value::Record(r1)) = values.get(1) else {
                panic!("{:?}", values[0])
            };

            assert_eq!(
                Value::Int(45654),
                r1.iter().find(|(name, _)| name == "id").unwrap().1
            );

            assert_eq!(
                Value::String("bob".into()),
                r1.iter().find(|(name, _)| name == "name").unwrap().1
            );

            assert_eq!(
                Value::Array(vec![Value::Int(5), Value::Int(9)]),
                r1.iter().find(|(name, _)| name == "lucky").unwrap().1
            );
        }

        Ok(())
    }

    async fn validate_with_codecs(key_codec: Codec, value_codec: Codec) -> Result<()> {
        let topic = "codec";

        let schema = json!({
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "key", "type": "int"},
                {"name": "value", "type": {
                    "name": "value",
                    "type": "record",
                    "fields": [
                        {"name": "name", "type": "string"},
                        {"name": "email", "type": "string"}]}}]});

        let object_store = InMemory::new();
        _ = object_store
            .put(
                &Path::from(format!("{topic}.avsc")),
                serde_json::to_vec(&schema)
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        let registry = Registry::new(object_store);

        let key = encode_with_codec(&json!({"type": "int"}), Value::Int(32123), key_codec)?;

        let value = encode_with_codec(
            &json!({
                "type": "record",
                "name": "value",
                "fields": [{"name": "name", "type": "string"}, {"name": "email", "type": "string"}]
            }),
            Value::Record(vec![
                ("name".into(), Value::String("alice".into())),
                ("email".into(), Value::String("alice@example.com".into())),
            ]),
            value_codec,
        )?;

        let batch = Batch::builder()
            .record(Record::builder().key(key.into()).value(value.into()))
            .build()?;

        registry.validate(topic, &batch).await
    }

    fn encode_with_codec(schema: &JsonValue, value: Value, codec: Codec) -> Result<Bytes> {
        let schema = AvroSchema::parse(schema)?;
        let mut writer = apache_avro::Writer::with_codec(&schema, vec![], codec);
        _ = writer.append(value)?;
        writer.into_inner().map(Bytes::from).map_err(Into::into)
    }

    #[tokio::test]
    async fn null_codec_key_and_value_is_valid() -> Result<()> {
        let _guard = init_tracing()?;

        validate_with_codecs(Codec::Null, Codec::Null).await
    }

    #[tokio::test]
    async fn deflate_key_is_invalid_record() -> Result<()> {
        let _guard = init_tracing()?;

        assert_matches!(
            validate_with_codecs(Codec::Deflate(DeflateSettings::default()), Codec::Null).await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        );

        Ok(())
    }

    #[tokio::test]
    async fn deflate_value_is_invalid_record() -> Result<()> {
        let _guard = init_tracing()?;

        assert_matches!(
            validate_with_codecs(Codec::Null, Codec::Deflate(DeflateSettings::default())).await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        );

        Ok(())
    }

    #[cfg(feature = "iceberg")]
    #[tokio::test]
    async fn snappy_value_is_invalid_record() -> Result<()> {
        let _guard = init_tracing()?;

        assert_matches!(
            validate_with_codecs(Codec::Null, Codec::Snappy).await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        );

        Ok(())
    }

    #[cfg(feature = "iceberg")]
    #[tokio::test]
    async fn zstandard_value_is_invalid_record() -> Result<()> {
        let _guard = init_tracing()?;

        assert_matches!(
            validate_with_codecs(
                Codec::Null,
                Codec::Zstandard(apache_avro::ZstandardSettings::default())
            )
            .await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        );

        Ok(())
    }

    /// A topic whose schema has a key of `key` and a value of `value`, and a batch of one record.
    async fn as_json_value_of(
        key: (JsonValue, Value),
        value: (JsonValue, Value),
    ) -> Result<JsonValue> {
        let topic = "abc";

        let object_store = InMemory::new();
        _ = object_store
            .put(
                &Path::from(format!("{topic}.avsc")),
                serde_json::to_vec(&json!({
                    "type": "record",
                    "name": "Test",
                    "fields": [
                        {"name": "key", "type": key.0},
                        {"name": "value", "type": value.0}
                    ]
                }))
                .map(Bytes::from)
                .map(PutPayload::from)?,
            )
            .await?;

        let encode = |schema: &JsonValue, value: Value| {
            let schema = AvroSchema::parse(schema)?;
            let mut writer = apache_avro::Writer::new(&schema, vec![]);
            _ = writer.append(value)?;
            writer.into_inner().map(Bytes::from).map_err(Error::from)
        };

        let batch = Batch::builder()
            .record(
                Record::builder()
                    .key(encode(&json!(key.0), key.1)?.into())
                    .value(encode(&json!(value.0), value.1)?.into()),
            )
            .build()?;

        Registry::new(object_store)
            .schema(topic)
            .await?
            .expect("a schema")
            .as_json_value(&batch)
    }

    #[tokio::test]
    async fn as_json_value_prints_key_and_value() -> Result<()> {
        let _guard = init_tracing()?;

        assert_eq!(
            json!([{"key": 32123, "value": {"name": "alice"}}]),
            as_json_value_of(
                (json!("int"), Value::Int(32123)),
                (
                    json!({"type": "record", "name": "person", "fields": [{"name": "name", "type": "string"}]}),
                    Value::Record(vec![("name".into(), Value::String("alice".into()))]),
                ),
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn as_json_value_prints_logical_types() -> Result<()> {
        let _guard = init_tracing()?;

        assert_eq!(
            json!([{"key": "1970-01-02T00:00:00.123", "value": 19000}]),
            as_json_value_of(
                (
                    json!({"type": "long", "logicalType": "timestamp-millis"}),
                    Value::TimestampMillis(86_400_123),
                ),
                (
                    json!({"type": "int", "logicalType": "date"}),
                    Value::Date(19000),
                ),
            )
            .await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn as_json_value_prints_each_timestamp_precision_and_a_negative_one() -> Result<()> {
        let _guard = init_tracing()?;

        for (logical_type, value, printed) in [
            (
                "timestamp-micros",
                Value::TimestampMicros(86_400_123_456),
                "1970-01-02T00:00:00.123456",
            ),
            (
                "timestamp-nanos",
                Value::TimestampNanos(86_400_123_456_789),
                "1970-01-02T00:00:00.123456789",
            ),
            (
                "timestamp-millis",
                Value::TimestampMillis(-1),
                "1969-12-31T23:59:59.999",
            ),
            (
                "local-timestamp-millis",
                Value::LocalTimestampMillis(86_400_123),
                "1970-01-02T00:00:00.123",
            ),
            (
                "local-timestamp-micros",
                Value::LocalTimestampMicros(86_400_123_456),
                "1970-01-02T00:00:00.123456",
            ),
            (
                "local-timestamp-nanos",
                Value::LocalTimestampNanos(86_400_123_456_789),
                "1970-01-02T00:00:00.123456789",
            ),
        ] {
            assert_eq!(
                json!([{"key": 1, "value": printed}]),
                as_json_value_of(
                    (json!("int"), Value::Int(1)),
                    (json!({"type": "long", "logicalType": logical_type}), value),
                )
                .await?,
                "{logical_type}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn as_json_value_refuses_a_decimal() -> Result<()> {
        let _guard = init_tracing()?;

        assert_matches!(
            as_json_value_of(
                (json!("int"), Value::Int(1)),
                (
                    json!({"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}),
                    Value::Decimal(vec![1, 2].into()),
                ),
            )
            .await,
            Err(Error::AvroToJson(SchemaKind::Decimal))
        );

        Ok(())
    }
}
