// Copyright ⓒ Solace Corporation
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

use std::{any::type_name_of_val, sync::Arc};

use crate::{
    ARROW_LIST_FIELD_NAME, AsArrow, Error, Result,
    json::{MessageKind, Schema},
    lake::LakeHouseType,
};

use arrow::{
    array::{
        ArrayBuilder, BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, NullBuilder,
        StringBuilder, StructBuilder,
    },
    datatypes::{DataType, Field, FieldRef, Fields, Schema as ArrowSchema},
    record_batch::RecordBatch,
};

use chrono::{DateTime, Datelike};

use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use serde_json::{Map, Value, json};

use nisshi_sans_io::record::inflated::Batch;
use tracing::{debug, error, instrument};

const NULLABLE: bool = true;

struct Record {
    meta: Value,
    key: Option<Value>,
    value: Option<Value>,
}

fn sort_dedup(mut input: Vec<DataType>) -> Vec<DataType> {
    input.sort();
    input.dedup();
    input
}

impl Schema {
    fn new_list_field(&self, path: &[&str], data_type: DataType) -> Field {
        self.new_field(path, ARROW_LIST_FIELD_NAME, data_type)
    }

    #[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
    fn new_field(&self, path: &[&str], name: &str, data_type: DataType) -> Field {
        debug!(?path, name, ?data_type, ids = ?self.ids);

        let path = {
            let mut path = Vec::from(path);
            path.push(name);
            path.join(".")
        };

        Field::new(name.to_owned(), data_type, NULLABLE).with_metadata(
            self.ids
                .get(path.as_str())
                .inspect(|field_id| debug!(?path, field_id))
                .map(|field_id| (PARQUET_FIELD_ID_META_KEY.to_string(), field_id.to_string()))
                .into_iter()
                .collect(),
        )
    }

    #[cfg(not(any(feature = "parquet", feature = "iceberg", feature = "delta")))]
    fn new_field(&self, path: &[&str], name: &str, data_type: DataType) -> Field {
        debug!(?path, name, ?data_type, ids = ?self.ids);

        Field::new(name.to_owned(), data_type, NULLABLE)
    }

    fn data_type(&self, path: &[&str], value: &Value) -> Result<DataType> {
        match value {
            Value::Null => Ok(DataType::Null),

            Value::Bool(_) => Ok(DataType::Boolean),

            Value::Number(value) => {
                if value.is_i64() | value.is_u64() {
                    Ok(DataType::Int64)
                } else {
                    Ok(DataType::Float64)
                }
            }

            Value::String(_) => Ok(DataType::Utf8),

            Value::Array(values) => self.common_data_type(path, values).map(|data_type| {
                DataType::List(FieldRef::new(self.new_list_field(path, data_type)))
            }),

            Value::Object(object) => object
                .iter()
                .map(|(k, v)| {
                    let child_path = {
                        let mut path = Vec::from(path);
                        path.push(k.as_str());
                        path
                    };

                    self.data_type(&child_path[..], v)
                        .map(|data_type| self.new_field(path, k, data_type))
                })
                .collect::<Result<Vec<_>>>()
                .map(Fields::from)
                .map(DataType::Struct),
        }
        .inspect(|data_type| debug!(?path, ?data_type))
        .inspect_err(|err| error!(?err, ?path))
    }

    fn common_data_type(&self, path: &[&str], values: &[Value]) -> Result<DataType> {
        debug!(?path, values = values.len());

        values
            .iter()
            .map(|value| self.data_type(path, value))
            .inspect(|data_type| debug!(?data_type))
            .collect::<Result<Vec<_>>>()
            .map(sort_dedup)
            .inspect(|data_types| debug!(?data_types))
            .and_then(|mut data_types| {
                if data_types.len() > 1 {
                    Err(Error::NoCommonType(data_types))
                } else if let Some(data_type) = data_types.pop() {
                    Ok(data_type)
                } else {
                    Ok(DataType::Null)
                }
            })
            .inspect(|data_type| debug!(?path, ?data_type))
            .inspect_err(|err| error!(?err, ?path))
    }

    fn data_type_builder(&self, path: &[&str], data_type: &DataType) -> Box<dyn ArrayBuilder> {
        debug!(path = path.join("."), ?data_type);

        match data_type {
            DataType::Null => Box::new(NullBuilder::new()),
            DataType::Boolean => Box::new(BooleanBuilder::new()),
            DataType::UInt64 => Box::new(Int64Builder::new()),
            DataType::Int64 => Box::new(Int64Builder::new()),
            DataType::Float64 => Box::new(Float64Builder::new()),
            DataType::Utf8 => Box::new(StringBuilder::new()),

            DataType::List(element) => {
                debug!(?element);

                Box::new(
                    ListBuilder::new(self.data_type_builder(
                        &append_path(path, ARROW_LIST_FIELD_NAME)[..],
                        element.data_type(),
                    ))
                    .with_field(self.new_list_field(path, element.data_type().to_owned())),
                ) as Box<dyn ArrayBuilder>
            }

            DataType::Struct(fields) => {
                debug!(?fields);

                Box::new(StructBuilder::new(
                    fields.to_owned(),
                    fields
                        .iter()
                        .map(|field| {
                            self.data_type_builder(
                                &append_path(path, field.name())[..],
                                field.data_type(),
                            )
                        })
                        .collect::<Vec<_>>(),
                ))
            }

            _ => unimplemented!("unexpected: {}", type_name_of_val(data_type)),
        }
    }
}

fn append_path<'a>(path: &[&'a str], name: &'a str) -> Vec<&'a str> {
    let mut path = Vec::from(path);
    path.push(name);
    path
}

fn append_list_builder(
    element: Arc<Field>,
    items: Vec<Value>,
    builder: &mut ListBuilder<Box<dyn ArrayBuilder>>,
) -> Result<()> {
    let values = builder.values().as_any_mut();

    for item in items {
        match (element.data_type(), item) {
            (_, Value::Null) => values
                .downcast_mut::<NullBuilder>()
                .ok_or(Error::Downcast)
                .map(|builder| builder.append_null())?,

            (_, Value::Bool(value)) => values
                .downcast_mut::<BooleanBuilder>()
                .ok_or(Error::Downcast)
                .map(|builder| builder.append_value(value))?,

            (DataType::Int64, Value::Number(value)) if value.is_u64() => values
                .downcast_mut::<Int64Builder>()
                .ok_or(Error::Downcast)
                .map(|builder| {
                    if let Some(value) = value.as_u64() {
                        builder.append_value(value as i64)
                    } else {
                        builder.append_null()
                    }
                })
                .inspect_err(|err| error!(?err))?,

            (DataType::Int64, Value::Number(value)) if value.is_i64() => values
                .downcast_mut::<Int64Builder>()
                .ok_or(Error::Downcast)
                .map(|builder| {
                    if let Some(value) = value.as_i64() {
                        builder.append_value(value)
                    } else {
                        builder.append_null()
                    }
                })
                .inspect_err(|err| error!(?err))?,

            (DataType::Float64, Value::Number(value)) if value.is_f64() => values
                .downcast_mut::<Float64Builder>()
                .ok_or(Error::Downcast)
                .map(|builder| {
                    if let Some(value) = value.as_f64() {
                        builder.append_value(value)
                    } else {
                        builder.append_null()
                    }
                })
                .inspect_err(|err| error!(?err))?,

            (_, Value::String(value)) => values
                .downcast_mut::<StringBuilder>()
                .ok_or(Error::Downcast)
                .map(|builder| builder.append_value(value))?,

            (DataType::List(element), Value::Array(items)) => values
                .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
                .ok_or(Error::Downcast)
                .inspect_err(|err| error!(?err, ?element, items = items.len()))
                .and_then(|builder| append_list_builder(element.to_owned(), items, builder))?,

            (DataType::Struct(fields), Value::Object(object)) => values
                .downcast_mut::<StructBuilder>()
                .ok_or(Error::Downcast)
                .inspect_err(|err| error!(?err, ?fields, object = object.len()))
                .and_then(|builder| append_struct_builder(fields, object, builder))?,

            (data_type, value) => Err(Error::UnsupportedSchemaRuntimeValue(
                data_type.to_owned(),
                crate::json_kind(&value),
            ))?,
        }
    }

    builder.append(true);

    Ok(())
}

fn append_struct_builder(
    fields: &Fields,
    mut object: Map<String, Value>,
    builder: &mut StructBuilder,
) -> Result<()> {
    debug!(?fields, object = object.len());

    for (index, field) in fields.iter().enumerate() {
        if let Some(value) = object.remove(field.name()) {
            match (field.data_type(), value) {
                (_, Value::Null) => builder
                    .field_builder::<NullBuilder>(index)
                    .ok_or(Error::Downcast)
                    .map(|builder| builder.append_null())
                    .inspect_err(|err| error!(?err))?,

                (_, Value::Bool(value)) => builder
                    .field_builder::<BooleanBuilder>(index)
                    .ok_or(Error::Downcast)
                    .map(|builder| builder.append_value(value))
                    .inspect_err(|err| error!(?err))?,

                (DataType::Int64, Value::Number(value)) if value.is_u64() => builder
                    .field_builder::<Int64Builder>(index)
                    .ok_or(Error::Downcast)
                    .map(|builder| {
                        if let Some(value) = value.as_u64() {
                            builder.append_value(value as i64)
                        } else {
                            builder.append_null()
                        }
                    })
                    .inspect_err(|err| error!(?field, ?err))?,

                (DataType::Int64, Value::Number(value)) if value.is_i64() => builder
                    .field_builder::<Int64Builder>(index)
                    .ok_or(Error::Downcast)
                    .map(|builder| {
                        if let Some(value) = value.as_i64() {
                            builder.append_value(value)
                        } else {
                            builder.append_null()
                        }
                    })?,

                (DataType::Float64, Value::Number(value)) if value.is_f64() => builder
                    .field_builder::<Float64Builder>(index)
                    .ok_or(Error::Downcast)
                    .map(|builder| {
                        if let Some(value) = value.as_f64() {
                            builder.append_value(value)
                        } else {
                            builder.append_null()
                        }
                    })?,

                (DataType::Utf8, Value::String(value)) => builder
                    .field_builder::<StringBuilder>(index)
                    .ok_or(Error::Downcast)
                    .map(|builder| builder.append_value(value))
                    .inspect_err(|err| error!(?err))?,

                (DataType::List(element), Value::Array(items)) => builder
                    .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(index)
                    .ok_or(Error::Downcast)
                    .and_then(|builder| append_list_builder(element.to_owned(), items, builder))
                    .inspect_err(|err| error!(?err))?,

                (DataType::Struct(fields), Value::Object(object)) => builder
                    .field_builder::<StructBuilder>(index)
                    .ok_or(Error::Downcast)
                    .and_then(|builder| append_struct_builder(fields, object, builder))
                    .inspect_err(|err| error!(?err))?,

                (data_type, value) => Err(Error::UnsupportedSchemaRuntimeValue(
                    data_type.to_owned(),
                    crate::json_kind(&value),
                ))?,
            }
        }
    }

    builder.append(true);

    Ok(())
}

fn append(field: &Field, value: Value, builder: &mut dyn ArrayBuilder) -> Result<()> {
    debug!(?field, builder = type_name_of_val(builder));

    match (field.data_type(), value) {
        (DataType::Null, _) => builder
            .as_any_mut()
            .downcast_mut::<NullBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_null()),

        (DataType::Boolean, Value::Bool(value)) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (DataType::Int64, Value::Number(value)) if value.is_u64() => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| {
                if let Some(value) = value.as_u64() {
                    builder.append_value(value as i64)
                } else {
                    builder.append_null()
                }
            })
            .inspect_err(|err| error!(?field, ?err)),

        (DataType::Int64, Value::Number(value)) if value.is_i64() => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| {
                if let Some(value) = value.as_i64() {
                    builder.append_value(value)
                } else {
                    builder.append_null()
                }
            }),

        (DataType::Float64, Value::Number(value)) if value.is_f64() => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .ok_or(Error::Downcast)
            .map(|builder| {
                if let Some(value) = value.as_f64() {
                    builder.append_value(value)
                } else {
                    builder.append_null()
                }
            }),

        (DataType::Utf8, Value::String(value)) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .ok_or(Error::Downcast)
            .map(|builder| builder.append_value(value)),

        (DataType::List(element), Value::Array(items)) => builder
            .as_any_mut()
            .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?element, items = items.len()))
            .and_then(|builder| append_list_builder(element.to_owned(), items, builder)),

        (DataType::Struct(fields), Value::Object(object)) => builder
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .ok_or(Error::Downcast)
            .inspect_err(|err| error!(?err, ?fields, object = object.len()))
            .and_then(|builder| append_struct_builder(fields, object, builder)),

        (data_type, value) => Err(Error::UnsupportedSchemaRuntimeValue(
            data_type.to_owned(),
            crate::json_kind(&value),
        ))?,
    }
}

impl AsArrow for Schema {
    #[instrument(skip(self, batch))]
    async fn as_arrow(
        &self,
        topic: &str,
        partition: i32,
        batch: &Batch,
        _lake_type: LakeHouseType,
    ) -> Result<RecordBatch> {
        let mut builders = vec![];
        let mut fields = vec![];

        {
            let meta = DateTime::from_timestamp_millis(batch.base_timestamp)
                .as_ref()
                .map(|date_time| {
                    json!({
                    "partition": partition,
                    "timestamp": date_time.to_rfc3339(),
                    "year": date_time.date_naive().year(),
                    "month": date_time.date_naive().month(),
                    "day": date_time.date_naive().day()})
                })
                .unwrap_or(json!({"partition": partition}));

            let data_type = self.common_data_type(&[MessageKind::Meta.as_ref()], &[meta][..])?;

            debug!(?data_type);
            builders.push(self.data_type_builder(&[MessageKind::Meta.as_ref()], &data_type));
            fields.push(self.new_field(&[], MessageKind::Meta.as_ref(), data_type))
        }

        if let Some(data_type) = batch
            .records
            .iter()
            .map(|record| {
                record.key.clone().map_or(Ok(None), |encoded| {
                    serde_json::from_slice::<Value>(&encoded[..])
                        .map(Some)
                        .map_err(Into::into)
                })
            })
            .collect::<Result<Vec<_>>>()
            .map(|values| values.into_iter().flatten().collect::<Vec<_>>())
            .and_then(|values| {
                if values.is_empty() {
                    Ok(None)
                } else {
                    self.common_data_type(&[MessageKind::Key.as_ref()], values.as_slice())
                        .map(Some)
                }
            })
            .inspect(|data_type| debug!(?data_type))?
        {
            builders.push(self.data_type_builder(&[MessageKind::Key.as_ref()], &data_type));
            fields.push(self.new_field(&[], MessageKind::Key.as_ref(), data_type))
        };

        if let Some(data_type) = batch
            .records
            .iter()
            .map(|record| {
                record.value.clone().map_or(Ok(None), |encoded| {
                    serde_json::from_slice::<Value>(&encoded[..])
                        .map(Some)
                        .map_err(Into::into)
                })
            })
            .collect::<Result<Vec<_>>>()
            .map(|values| values.into_iter().flatten().collect::<Vec<_>>())
            .and_then(|values| {
                if values.is_empty() {
                    Ok(None)
                } else {
                    self.common_data_type(&[MessageKind::Value.as_ref()], values.as_slice())
                        .map(Some)
                }
            })
            .inspect(|data_type| debug!(?data_type))?
        {
            builders.push(self.data_type_builder(&[MessageKind::Value.as_ref()], &data_type));
            fields.push(self.new_field(&[], MessageKind::Value.as_ref(), data_type))
        };

        for kv in batch
            .records
            .iter()
            .map(|record| {
                record
                    .key
                    .as_ref()
                    .map(|encoded| serde_json::from_slice::<Value>(&encoded[..]))
                    .transpose()
                    .map_err(Into::into)
                    .and_then(|key| {
                        let meta = DateTime::from_timestamp_millis(batch.record_timestamp(record))
                            .as_ref()
                            .map(|date_time| {
                                json!({
                            "partition": partition,
                            "timestamp": date_time.to_rfc3339(),
                            "year": date_time.date_naive().year(),
                            "month": date_time.date_naive().month(),
                            "day": date_time.date_naive().day()})
                            })
                            .unwrap_or(json!({"partition": partition}));

                        record
                            .value
                            .as_ref()
                            .map(|encoded| serde_json::from_slice::<Value>(&encoded[..]))
                            .transpose()
                            .map_err(Into::into)
                            .map(|value| Record { meta, key, value })
                    })
            })
            .collect::<Result<Vec<_>>>()?
        {
            let mut i = fields.iter().zip(builders.iter_mut());

            let (field, builder) = i
                .next()
                .ok_or_else(|| Error::Message("fields/builders out of sync".into()))?;
            debug!(meta = %kv.meta, ?field);
            append(field, kv.meta, builder)?;

            if let Some(key) = kv.key {
                let (field, builder) = i
                    .next()
                    .ok_or_else(|| Error::Message("fields/builders out of sync".into()))?;
                debug!(?field);
                append(field, key, builder)?;
            }

            if let Some(value) = kv.value {
                let (field, builder) = i
                    .next()
                    .ok_or_else(|| Error::Message("fields/builders out of sync".into()))?;
                debug!(?field);
                append(field, value, builder)?;
            }
        }

        debug!(len = ?builders.iter().map(|builder|builder.len()).collect::<Vec<_>>());

        RecordBatch::try_new(
            Arc::new(ArrowSchema::new(Fields::from(fields))),
            builders
                .iter_mut()
                .map(|builder| builder.finish())
                .collect(),
        )
        .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "iceberg")]
    use arrow::util::pretty::pretty_format_batches;

    #[cfg(feature = "iceberg")]
    use datafusion::prelude::*;

    #[cfg(feature = "iceberg")]
    use iceberg::{
        io::FileIO,
        spec::{
            DataFile, DataFileFormat::Parquet, Schema as IcebergSchema,
            SchemaRef as IcebergSchemaRef,
        },
        writer::{
            IcebergWriter, IcebergWriterBuilder,
            base_writer::data_file_writer::DataFileWriterBuilder,
            file_writer::{
                ParquetWriterBuilder,
                location_generator::{DefaultFileNameGenerator, LocationGenerator},
                rolling_writer::RollingFileWriterBuilder,
            },
        },
    };

    #[cfg(feature = "iceberg")]
    use parquet::file::properties::WriterProperties;

    use bytes::Bytes;
    use nisshi_sans_io::record::Record;
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

    #[cfg(feature = "iceberg")]
    async fn iceberg_write(record_batch: RecordBatch) -> Result<Vec<DataFile>> {
        let iceberg_schema = IcebergSchema::try_from(record_batch.schema().as_ref())
            .map(IcebergSchemaRef::new)
            .inspect(|schema| debug!(?schema))
            .inspect_err(|err| debug!(?err))?;

        let memory = FileIO::new_with_memory();

        #[derive(Clone)]
        struct Location;

        impl LocationGenerator for Location {
            fn generate_location(
                &self,
                _partition_key: Option<&iceberg::spec::PartitionKey>,
                file_name: &str,
            ) -> String {
                format!("abc/{file_name}")
            }
        }

        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::default(), iceberg_schema);

        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            memory,
            Location,
            DefaultFileNameGenerator::new("pqr".into(), None, Parquet),
        );

        let mut data_file_writer = DataFileWriterBuilder::new(rolling_writer_builder)
            .build(None)
            .await
            .inspect_err(|err| error!(?err))?;

        data_file_writer
            .write(record_batch)
            .await
            .inspect_err(|err| debug!(?err))?;

        data_file_writer
            .close()
            .await
            .inspect(|data_files| debug!(?data_files))
            .inspect_err(|err| debug!(?err))
            .map_err(Into::into)
    }

    #[tokio::test]
    #[cfg(feature = "iceberg")]
    async fn key_and_value_as_arrow() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

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

        let batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for (ref key, ref value) in kv {
                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                        .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()?
        };

        let record_batch = schema
            .as_arrow(topic, 0, &batch, LakeHouseType::Parquet)
            .await?;
        let data_files = iceberg_write(record_batch.clone()).await?;
        assert_eq!(1, data_files.len());
        assert_eq!(2, data_files[0].record_count());

        let ctx = SessionContext::new();

        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-------------------------------------------------------------------------------------+-------+-----------------------------------------+",
            "| meta                                                                                | key   | value                                   |",
            "+-------------------------------------------------------------------------------------+-------+-----------------------------------------+",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 12321 | {email: alice@example.com, name: alice} |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 32123 | {email: bob@example.com, name: bob}     |",
            "+-------------------------------------------------------------------------------------+-------+-----------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "iceberg")]
    async fn grade() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let schema = Schema::try_from(Bytes::from_static(include_bytes!(
            "../../../etc/schema/grade.json"
        )))?;

        let kv = if let Value::Array(values) =
            serde_json::from_slice::<Value>(include_bytes!("../../../etc/data/grades.json"))?
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

        let batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for (ref key, ref value) in kv {
                debug!(?key, ?value);

                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                        .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()?
        };

        let record_batch = schema
            .as_arrow(topic, 0, &batch, LakeHouseType::Parquet)
            .await?;
        let data_files = iceberg_write(record_batch.clone()).await?;
        assert_eq!(1, data_files.len());
        assert_eq!(16, data_files[0].record_count());

        let ctx = SessionContext::new();

        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-------------------------------------------------------------------------------------+-------------+---------------------------------------------------------------------------------------------------------------+",
            "| meta                                                                                | key         | value                                                                                                         |",
            "+-------------------------------------------------------------------------------------+-------------+---------------------------------------------------------------------------------------------------------------+",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 123-45-6789 | {final: 49.0, first: Aloysius, grade: D-, last: Alfalfa, test1: 40.0, test2: 90.0, test3: 100.0, test4: 83.0} |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 123-12-1234 | {final: 48.0, first: University, grade: D+, last: Alfred, test1: 41.0, test2: 97.0, test3: 96.0, test4: 97.0} |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 567-89-0123 | {final: 44.0, first: Gramma, grade: C, last: Gerty, test1: 41.0, test2: 80.0, test3: 60.0, test4: 40.0}       |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 087-65-4321 | {final: 47.0, first: Electric, grade: B-, last: Android, test1: 42.0, test2: 23.0, test3: 36.0, test4: 45.0}  |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 456-78-9012 | {final: 45.0, first: Fred, grade: A-, last: Bumpkin, test1: 43.0, test2: 78.0, test3: 88.0, test4: 77.0}      |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 234-56-7890 | {final: 46.0, first: Betty, grade: C-, last: Rubble, test1: 44.0, test2: 90.0, test3: 80.0, test4: 90.0}      |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 345-67-8901 | {final: 43.0, first: Cecil, grade: F, last: Noshow, test1: 45.0, test2: 11.0, test3: -1.0, test4: 4.0}        |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 632-79-9939 | {final: 50.0, first: Bif, grade: B+, last: Buff, test1: 46.0, test2: 20.0, test3: 30.0, test4: 40.0}          |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 223-45-6789 | {final: 83.0, first: Andrew, grade: A, last: Airpump, test1: 49.0, test2: 1.0, test3: 90.0, test4: 100.0}     |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 143-12-1234 | {final: 97.0, first: Jim, grade: A+, last: Backus, test1: 48.0, test2: 1.0, test3: 97.0, test4: 96.0}         |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 565-89-0123 | {final: 40.0, first: Art, grade: D+, last: Carnivore, test1: 44.0, test2: 1.0, test3: 80.0, test4: 60.0}      |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 087-75-4321 | {final: 45.0, first: Jim, grade: C+, last: Dandy, test1: 47.0, test2: 1.0, test3: 23.0, test4: 36.0}          |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 456-71-9012 | {final: 77.0, first: Ima, grade: B-, last: Elephant, test1: 45.0, test2: 1.0, test3: 78.0, test4: 88.0}       |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 234-56-2890 | {final: 90.0, first: Benny, grade: B-, last: Franklin, test1: 50.0, test2: 1.0, test3: 90.0, test4: 80.0}     |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 345-67-3901 | {final: 4.0, first: Boy, grade: B, last: George, test1: 40.0, test2: 1.0, test3: 11.0, test4: -1.0}           |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 632-79-9439 | {final: 40.0, first: Harvey, grade: C, last: Heffalump, test1: 30.0, test2: 1.0, test3: 20.0, test4: 30.0}    |",
            "+-------------------------------------------------------------------------------------+-------------+---------------------------------------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "iceberg")]
    async fn key_as_arrow() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let schema = serde_json::to_vec(&json!({
            "type": "object",
            "properties": {
                "key": {
                    "type": "number"
                }
            }
        }))
        .map_err(Into::into)
        .map(Bytes::from)
        .and_then(Schema::try_from)?;

        let keys = [json!(12321), json!(23432), json!(34543)];

        let batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for ref key in keys {
                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()?
        };

        let record_batch = schema
            .as_arrow(topic, 0, &batch, LakeHouseType::Parquet)
            .await?;
        let data_files = iceberg_write(record_batch.clone()).await?;
        assert_eq!(1, data_files.len());
        assert_eq!(3, data_files[0].record_count());

        let ctx = SessionContext::new();

        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-------------------------------------------------------------------------------------+-------+",
            "| meta                                                                                | key   |",
            "+-------------------------------------------------------------------------------------+-------+",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 12321 |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 23432 |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 34543 |",
            "+-------------------------------------------------------------------------------------+-------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "iceberg")]
    async fn primitive_key_and_value_as_arrow() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let schema = serde_json::to_vec(&json!({
            "type": "object",
            "properties": {
                "key": {
                    "type": "number"
                },
                "value": {
                    "type": "string",
                }
            }
        }))
        .map_err(Into::into)
        .map(Bytes::from)
        .and_then(Schema::try_from)?;

        let kv = [
            (json!(12321), json!("alice@example.com")),
            (json!(32123), json!("bob@example.com")),
        ];

        let batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for (ref key, ref value) in kv {
                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                        .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()?
        };

        let record_batch = schema
            .as_arrow(topic, 0, &batch, LakeHouseType::Parquet)
            .await?;
        let data_files = iceberg_write(record_batch.clone()).await?;
        assert_eq!(1, data_files.len());
        assert_eq!(2, data_files[0].record_count());

        let ctx = SessionContext::new();

        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-------------------------------------------------------------------------------------+-------+-------------------+",
            "| meta                                                                                | key   | value             |",
            "+-------------------------------------------------------------------------------------+-------+-------------------+",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 12321 | alice@example.com |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 32123 | bob@example.com   |",
            "+-------------------------------------------------------------------------------------+-------+-------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[tokio::test]
    #[cfg(feature = "iceberg")]
    async fn primitive_key_and_array_value_as_arrow() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let schema = serde_json::to_vec(&json!({
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
        }))
        .map_err(Into::into)
        .map(Bytes::from)
        .and_then(Schema::try_from)?;

        let kv = [
            (json!(12321), json!(["a", "b", "c"])),
            (json!(32123), json!(["p", "q", "r"])),
        ];

        let batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for (ref key, ref value) in kv {
                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                        .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()?
        };

        let record_batch = schema
            .as_arrow(topic, 0, &batch, LakeHouseType::Parquet)
            .await?;
        let data_files = iceberg_write(record_batch.clone()).await?;
        assert_eq!(1, data_files.len());
        assert_eq!(2, data_files[0].record_count());

        let ctx = SessionContext::new();

        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-------------------------------------------------------------------------------------+-------+-----------+",
            "| meta                                                                                | key   | value     |",
            "+-------------------------------------------------------------------------------------+-------+-----------+",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 12321 | [a, b, c] |",
            "| {day: 13, month: 2, partition: 0, timestamp: 2009-02-13T23:31:30+00:00, year: 2009} | 32123 | [p, q, r] |",
            "+-------------------------------------------------------------------------------------+-------+-----------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    #[cfg(feature = "iceberg")]
    async fn primitive_key_and_array_object_value_as_arrow() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let schema = serde_json::to_vec(&json!({
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
        }))
        .map_err(Into::into)
        .map(Bytes::from)
        .and_then(Schema::try_from)?;

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

        let batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for (ref key, ref value) in kv {
                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                        .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()?
        };

        let record_batch = schema
            .as_arrow(topic, 0, &batch, LakeHouseType::Parquet)
            .await?;
        let data_files = iceberg_write(record_batch.clone()).await?;
        assert_eq!(1, data_files.len());
        assert_eq!(3, data_files[0].record_count());

        let ctx = SessionContext::new();

        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-------+----------------------------------------------------------------------------------------------+",
            "| key   | value                                                                                        |",
            "+-------+----------------------------------------------------------------------------------------------+",
            "| 12321 | [{location: abc, quantity: 6}, {location: pqr, quantity: 11}]                                |",
            "| 32123 | [{location: abc, quantity: 3}, {location: def, quantity: 33}, {location: xyz, quantity: 21}] |",
            "+-------+----------------------------------------------------------------------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    #[cfg(feature = "iceberg")]
    async fn primitive_key_and_struct_with_array_field_value_as_arrow() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let schema = serde_json::to_vec(&json!({
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
        }))
        .map_err(Into::into)
        .map(Bytes::from)
        .and_then(Schema::try_from)?;

        let kv = [
            (
                json!(12321),
                json!({"zone": 6, "locations": ["abc", "def"]}),
            ),
            (json!(32123), json!({"zone": 11, "locations": ["pqr"]})),
        ];

        let batch = {
            let mut batch = Batch::builder().base_timestamp(1_234_567_890 * 1_000);

            for (ref key, ref value) in kv {
                batch = batch.record(
                    Record::builder()
                        .key(serde_json::to_vec(key).map(Bytes::from).map(Into::into)?)
                        .value(serde_json::to_vec(value).map(Bytes::from).map(Into::into)?),
                );
            }

            batch.build()?
        };

        let record_batch = schema
            .as_arrow(topic, 0, &batch, LakeHouseType::Parquet)
            .await?;
        let data_files = iceberg_write(record_batch.clone()).await?;
        assert_eq!(1, data_files.len());
        assert_eq!(2, data_files[0].record_count());

        let ctx = SessionContext::new();

        _ = ctx.register_batch(topic, record_batch)?;
        let df = ctx.sql(format!("select * from {topic}").as_str()).await?;
        let results = df.collect().await?;

        let pretty_results = pretty_format_batches(&results)?.to_string();

        let expected = vec![
            "+-------+----------------------------------+",
            "| key   | value                            |",
            "+-------+----------------------------------+",
            "| 12321 | {locations: [abc, def], zone: 6} |",
            "| 32123 | {locations: [pqr], zone: 11}     |",
            "+-------+----------------------------------+",
        ];

        assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);

        Ok(())
    }
}
