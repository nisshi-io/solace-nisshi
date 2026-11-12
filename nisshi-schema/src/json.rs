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

//! JSON schema

use std::collections::BTreeMap;

use crate::{
    ARROW_LIST_FIELD_NAME, AsJsonValue, AsKafkaRecord, Error, Generator, Result, Validator,
};

use bytes::Bytes;

use serde_json::{Map, Value};

use nisshi_sans_io::{ErrorCode, record::inflated::Batch};
use tracing::{debug, instrument, warn};

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
mod arrow;

#[derive(Debug, Default)]
pub struct Schema {
    key: Option<jsonschema::Validator>,
    value: Option<jsonschema::Validator>,

    #[allow(dead_code)]
    ids: BTreeMap<String, i32>,
}

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

/// Parses the key or value of a record, which the schema has a schema for, as JSON.
///
/// A record that is missing or not JSON is `INVALID_RECORD`, and the error holds no part of the
/// record. [`validate`] and [`json_value`] both parse through this function, so that a produce and
/// a consume refuse the same records.
fn parse(encoded: Option<Bytes>) -> Result<Value> {
    encoded.map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
        serde_json::from_slice(&encoded[..]).map_err(|err| {
            warn!(?err, len = encoded.len());
            Error::Api(ErrorCode::InvalidRecord)
        })
    })
}

fn validate(validator: Option<&jsonschema::Validator>, encoded: Option<Bytes>) -> Result<()> {
    debug!(validator = ?validator, len = ?encoded.as_ref().map(Bytes::len));

    validator
        .map_or(Ok(()), |validator| {
            parse(encoded).and_then(|instance| {
                validator
                    .validate(&instance)
                    .inspect_err(|err| {
                        warn!(
                            instance_path = %err.instance_path(),
                            schema_path = %err.schema_path(),
                            ?validator
                        );
                    })
                    .map_err(|_err| Error::Api(ErrorCode::InvalidRecord))
            })
        })
        .inspect(|r| debug!(?r))
        .inspect_err(|err| warn!(?err))
}

impl TryFrom<Bytes> for Schema {
    type Error = Error;

    fn try_from(encoded: Bytes) -> Result<Self, Self::Error> {
        debug!(encoded = &encoded[..]);
        const PROPERTIES: &str = "properties";

        let mut schema = serde_json::from_slice::<Value>(&encoded[..])?;

        let key = schema
            .get(PROPERTIES)
            .and_then(|properties| properties.get(MessageKind::Key.as_ref()))
            .inspect(|key| debug!(?key))
            .and_then(|key| jsonschema::validator_for(key).ok());

        let value = schema
            .get(PROPERTIES)
            .and_then(|properties| properties.get(MessageKind::Value.as_ref()))
            .inspect(|value| debug!(?value))
            .and_then(|value| jsonschema::validator_for(value).ok());

        let meta =
            serde_json::from_slice::<Value>(&Bytes::from_static(include_bytes!("meta.json")))
                .inspect(|meta| debug!(%meta))?;

        _ = schema
            .get_mut(PROPERTIES)
            .and_then(|properties| properties.as_object_mut())
            .inspect(|properties| debug!(?properties))
            .and_then(|object| object.insert(MessageKind::Meta.as_ref().to_owned(), meta));

        let ids = field_ids(&schema);
        debug!(?ids);

        Ok(Self { key, value, ids })
    }
}

impl Validator for Schema {
    #[instrument(skip(self, batch), ret)]
    fn validate(&self, batch: &Batch) -> Result<()> {
        for record in &batch.records {
            debug!(?record);

            validate(self.key.as_ref(), record.key.clone())
                .and(validate(self.value.as_ref(), record.value.clone()))?
        }

        Ok(())
    }
}

impl AsKafkaRecord for Schema {
    fn as_kafka_record(&self, value: &Value) -> Result<nisshi_sans_io::record::Builder> {
        let mut builder = nisshi_sans_io::record::Record::builder();

        if let Some(value) = value.get(MessageKind::Key.as_ref())
            && self.key.is_some()
        {
            builder = builder.key(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?);
        }

        if let Some(value) = value.get(MessageKind::Value.as_ref())
            && self.value.is_some()
        {
            builder = builder.value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?);
        }

        Ok(builder)
    }
}

impl Generator for Schema {
    fn generate(&self) -> Result<nisshi_sans_io::record::Builder> {
        todo!()
    }
}

/// The JSON value of a record's key or value, or `Null` when the schema has no schema for it.
///
/// This function parses the record but does not validate it against the schema, because a
/// consumer prints what the topic holds. Avro and Protobuf cannot print a record that breaks its
/// schema, since they decode it with the schema.
fn json_value(validator: Option<&jsonschema::Validator>, encoded: Option<Bytes>) -> Result<Value> {
    validator.map_or(Ok(Value::Null), |_| parse(encoded))
}

impl AsJsonValue for Schema {
    fn as_json_value(&self, batch: &Batch) -> Result<Value> {
        batch
            .records
            .iter()
            .map(|record| {
                let key = json_value(self.key.as_ref(), record.key.clone())?;
                let value = json_value(self.value.as_ref(), record.value.clone())?;

                Ok(Value::Object(Map::from_iter([
                    (MessageKind::Key.as_ref().to_owned(), key),
                    (MessageKind::Value.as_ref().to_owned(), value),
                ])))
            })
            .collect::<Result<Vec<_>>>()
            .map(Value::Array)
    }
}

#[instrument(skip(schema), ret)]
fn field_ids(schema: &Value) -> BTreeMap<String, i32> {
    fn field_ids_with_path(path: &[&str], schema: &Value, id: &mut i32) -> BTreeMap<String, i32> {
        debug!(?path, %schema, id);

        let mut ids = BTreeMap::new();

        match schema.get("type").and_then(|r#type| r#type.as_str()) {
            Some("object") => {
                if let Some(properties) = schema
                    .get("properties")
                    .and_then(|properties| properties.as_object())
                {
                    for (k, v) in properties {
                        let mut path = Vec::from(path);
                        path.push(k);

                        _ = ids.insert(path.join("."), *id);
                        *id += 1;

                        ids.extend(field_ids_with_path(&path[..], v, id))
                    }
                }
            }

            Some("array") => {
                let mut path = Vec::from(path);
                path.push(ARROW_LIST_FIELD_NAME);
                _ = ids.insert(path.join("."), *id);
                *id += 1;

                if let Some(items) = schema.get("items") {
                    debug!(?items);

                    ids.extend(field_ids_with_path(&path[..], items, id))
                }
            }

            None | Some(_) => (),
        }

        ids
    }

    let mut ids = BTreeMap::new();
    let mut id = 1;
    let kinds = [MessageKind::Meta, MessageKind::Key, MessageKind::Value];

    for kind in kinds {
        if schema
            .get("properties")
            .and_then(|schema| schema.get(kind.as_ref()))
            .inspect(|schema| debug!(?kind, ?schema))
            .is_some()
        {
            _ = ids.insert(kind.as_ref().into(), id);
            id += 1;
        }
    }

    for kind in kinds {
        if let Some(schema) = schema
            .get("properties")
            .and_then(|schema| schema.get(kind.as_ref()))
            .inspect(|schema| debug!(?kind, ?schema))
        {
            ids.extend(field_ids_with_path(&[kind.as_ref()], schema, &mut id));
        }
    }

    ids
}

#[cfg(test)]
mod tests {
    use crate::Registry;

    use super::*;

    use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path};

    use nisshi_sans_io::record::Record;
    use serde_json::json;
    use std::{assert_matches, fs::File, sync::Arc, thread};
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

    #[test]
    fn assign_field_id() {
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

        let ids = field_ids(&schema);

        assert!(ids.contains_key("key"));
        assert!(ids.contains_key("value"));
        assert!(ids.contains_key("value.name"));
        assert!(ids.contains_key("value.email"));
    }

    #[test]
    fn assign_field_id_with_array() {
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

        let ids = field_ids(&schema);

        assert!(ids.contains_key("key"));
        assert!(ids.contains_key("value"));
        assert!(ids.contains_key("value.element"));
    }

    #[tokio::test]
    async fn key_only_invalid_record() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let payload = serde_json::to_vec(&json!({
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
        }))
        .map(Bytes::from)
        .map(PutPayload::from)?;

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.json"));
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let key = serde_json::to_vec(&json!(12320)).map(Bytes::from)?;

        let batch = Batch::builder()
            .base_timestamp(1_234_567_890 * 1_000)
            .record(Record::builder().key(key.clone().into()))
            .build()?;

        assert!(matches!(
            registry.validate(topic, &batch).await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn value_only_invalid_record() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let payload = serde_json::to_vec(&json!({
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
        }))
        .map(Bytes::from)
        .map(PutPayload::from)?;

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.json"));
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let value = serde_json::to_vec(&json!({
            "name": "alice",
            "email": "alice@example.com"}))
        .map(Bytes::from)?;

        let batch = Batch::builder()
            .base_timestamp(1_234_567_890 * 1_000)
            .record(Record::builder().value(value.clone().into()))
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

        let payload = serde_json::to_vec(&json!({
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
        }))
        .map(Bytes::from)
        .map(PutPayload::from)?;

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.json"));
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let key = serde_json::to_vec(&json!(12320)).map(Bytes::from)?;

        let value = serde_json::to_vec(&json!({
                "name": "alice",
                "email": "alice@example.com"}))
        .map(Bytes::from)?;

        let batch = Batch::builder()
            .base_timestamp(1_234_567_890 * 1_000)
            .record(
                Record::builder()
                    .key(key.clone().into())
                    .value(value.clone().into()),
            )
            .build()?;

        registry.validate(topic, &batch).await
    }

    #[tokio::test]
    async fn no_schema() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let registry = Registry::new(InMemory::new());

        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");
        let value = Bytes::from_static(b"Consectetur adipiscing elit");

        let batch = Batch::builder()
            .base_timestamp(1_234_567_890 * 1_000)
            .record(
                Record::builder()
                    .key(key.clone().into())
                    .value(value.clone().into()),
            )
            .build()?;

        registry.validate(topic, &batch).await
    }

    #[tokio::test]
    async fn empty_schema() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let payload = serde_json::to_vec(&json!({}))
            .map(Bytes::from)
            .map(PutPayload::from)?;

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.json"));
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");
        let value = Bytes::from_static(b"Consectetur adipiscing elit");

        let batch = Batch::builder()
            .base_timestamp(1_234_567_890 * 1_000)
            .record(
                Record::builder()
                    .key(key.clone().into())
                    .value(value.clone().into()),
            )
            .build()?;

        registry.validate(topic, &batch).await
    }

    #[tokio::test]
    async fn key_schema_only() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let payload = serde_json::to_vec(&json!({
            "type": "object",
            "properties": {
                "key": {
                    "type": "number"
                },
            }
        }))
        .map(Bytes::from)
        .map(PutPayload::from)?;

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.json"));
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let key = serde_json::to_vec(&json!(12320)).map(Bytes::from)?;

        let value = Bytes::from_static(b"Consectetur adipiscing elit");

        let batch = Batch::builder()
            .base_timestamp(1_234_567_890 * 1_000)
            .record(
                Record::builder()
                    .key(key.clone().into())
                    .value(value.clone().into()),
            )
            .build()?;

        registry.validate(topic, &batch).await
    }

    #[tokio::test]
    async fn bad_key() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let payload = serde_json::to_vec(&json!({
            "type": "object",
            "properties": {
                "key": {
                    "type": "number"
                },
            }
        }))
        .map(Bytes::from)
        .map(PutPayload::from)?;

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.json"));
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");

        let batch = Batch::builder()
            .base_timestamp(1_234_567_890 * 1_000)
            .record(Record::builder().key(key.clone().into()))
            .build()?;

        assert!(matches!(
            registry.validate(topic, &batch).await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        ));

        Ok(())
    }

    #[tokio::test]
    async fn value_schema_only() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let payload = serde_json::to_vec(&json!({
            "type": "object",
            "properties": {
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
        }))
        .map(Bytes::from)
        .map(PutPayload::from)?;

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.json"));
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");

        let value = serde_json::to_vec(&json!({
                    "name": "alice",
                    "email": "alice@example.com"}))
        .map(Bytes::from)?;

        let batch = Batch::builder()
            .base_timestamp(1_234_567_890 * 1_000)
            .record(
                Record::builder()
                    .key(key.clone().into())
                    .value(value.clone().into()),
            )
            .build()?;

        registry.validate(topic, &batch).await
    }

    #[tokio::test]
    async fn bad_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let payload = serde_json::to_vec(&json!({
            "type": "object",
            "properties": {
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
        }))
        .map(Bytes::from)
        .map(PutPayload::from)?;

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.json"));
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let value = Bytes::from_static(b"Consectetur adipiscing elit");

        let batch = Batch::builder()
            .base_timestamp(1_234_567_890 * 1_000)
            .record(Record::builder().value(value.clone().into()))
            .build()?;

        assert!(matches!(
            registry.validate(topic, &batch).await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        ));

        Ok(())
    }

    #[test]
    fn integer_type_can_be_float_dot_zero() -> Result<()> {
        let schema = json!({"type": "integer"});
        let validator = jsonschema::validator_for(&schema)?;

        assert!(validator.is_valid(&json!(42)));
        assert!(validator.is_valid(&json!(-1)));
        assert!(validator.is_valid(&json!(1.0)));

        Ok(())
    }

    #[test]
    fn array_with_items_type_basic_output() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = serde_json::to_vec(&json!({
            "type": "object",
            "properties": {
                "value": {
                    "type": "array",
                    "items": {
                        "type": "number"
                    }
                }
            }
        }))
        .map_err(Into::into)
        .map(Bytes::from)
        .and_then(Schema::try_from)?;

        assert!(
            schema
                .value
                .as_ref()
                .unwrap()
                .evaluate(&json!([1, 2, 3, 4, 5]))
                .flag()
                .valid
        );

        assert!(
            schema
                .value
                .as_ref()
                .unwrap()
                .evaluate(&json!([-1, 2.3, 3, 4.0, 5]))
                .flag()
                .valid
        );

        assert!(
            !schema
                .value
                .as_ref()
                .unwrap()
                .evaluate(&json!([3, "different", { "types": "of values" }]))
                .flag()
                .valid
        );

        assert!(
            !schema
                .value
                .as_ref()
                .unwrap()
                .evaluate(&json!({"Not": "an array"}))
                .flag()
                .valid,
        );

        Ok(())
    }

    #[test]
    fn array_basic_output() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = serde_json::to_vec(&json!({
            "type": "object",
            "properties": {
                "value": {
                    "type": "array",
                }
            }
        }))
        .map_err(Into::into)
        .map(Bytes::from)
        .and_then(Schema::try_from)?;

        assert!(
            schema
                .value
                .as_ref()
                .unwrap()
                .evaluate(&json!([1, 2, 3, 4, 5]))
                .flag()
                .valid
        );

        assert!(
            schema
                .value
                .as_ref()
                .unwrap()
                .evaluate(&json!([3, "different", { "types": "of values" }]))
                .flag()
                .valid
        );

        assert!(
            !schema
                .value
                .as_ref()
                .unwrap()
                .evaluate(&json!({"Not": "an array"}))
                .flag()
                .valid,
        );

        Ok(())
    }

    #[test]
    fn schema_basic_output() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = serde_json::to_vec(&json!({
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
        }))
        .map_err(Into::into)
        .map(Bytes::from)
        .and_then(Schema::try_from)?;

        debug!(?schema);

        assert!(
            schema
                .key
                .as_ref()
                .unwrap()
                .evaluate(&json!(12321))
                .flag()
                .valid
        );

        assert!(
            schema
                .value
                .as_ref()
                .unwrap()
                .evaluate(&json!({"name": "alice", "email": "alice@example.com"}))
                .flag()
                .valid
        );

        Ok(())
    }

    async fn registry_with(topic: &str, schema: &Value) -> Result<Registry> {
        let object_store = InMemory::new();
        _ = object_store
            .put(
                &Path::from(format!("{topic}.json")),
                serde_json::to_vec(schema)
                    .map(Bytes::from)
                    .map(PutPayload::from)?,
            )
            .await?;

        Ok(Registry::new(object_store))
    }

    fn encoded(value: &Value) -> Result<Bytes> {
        serde_json::to_vec(value)
            .map(Bytes::from)
            .map_err(Into::into)
    }

    #[tokio::test]
    async fn as_json_value_prints_key_and_value() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "abc";
        let registry = registry_with(
            topic,
            &json!({
                "type": "object",
                "properties": {
                    "key": {"type": "number"},
                    "value": {"type": "object"}
                }
            }),
        )
        .await?;

        let batch = Batch::builder()
            .record(
                Record::builder()
                    .key(encoded(&json!(12320))?.into())
                    .value(encoded(&json!({"name": "alice"}))?.into()),
            )
            .record(
                Record::builder()
                    .key(encoded(&json!(12321))?.into())
                    .value(encoded(&json!({"name": "bob"}))?.into()),
            )
            .build()?;

        let schema = registry.schema(topic).await?.expect("a schema");

        assert_eq!(
            json!([
                {"key": 12320, "value": {"name": "alice"}},
                {"key": 12321, "value": {"name": "bob"}},
            ]),
            schema.as_json_value(&batch)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn as_json_value_is_null_for_a_side_without_a_schema() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "abc";
        let registry = registry_with(
            topic,
            &json!({
                "type": "object",
                "properties": {"value": {"type": "string"}}
            }),
        )
        .await?;

        let batch = Batch::builder()
            .record(Record::builder().value(encoded(&json!("alice"))?.into()))
            .build()?;

        let schema = registry.schema(topic).await?.expect("a schema");

        assert_eq!(
            json!([{"key": null, "value": "alice"}]),
            schema.as_json_value(&batch)?
        );

        Ok(())
    }

    #[tokio::test]
    async fn as_json_value_refuses_a_record_that_is_not_json() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "abc";
        let registry = registry_with(
            topic,
            &json!({
                "type": "object",
                "properties": {"value": {"type": "string"}}
            }),
        )
        .await?;

        let schema = registry.schema(topic).await?.expect("a schema");

        let not_json = Batch::builder()
            .record(Record::builder().value(Bytes::from_static(b"{not json").into()))
            .build()?;

        assert_matches!(
            schema.as_json_value(&not_json),
            Err(Error::Api(ErrorCode::InvalidRecord))
        );

        let no_value = Batch::builder().record(Record::builder()).build()?;

        assert_matches!(
            schema.as_json_value(&no_value),
            Err(Error::Api(ErrorCode::InvalidRecord))
        );

        Ok(())
    }
}
