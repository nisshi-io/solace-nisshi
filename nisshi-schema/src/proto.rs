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

//! Protocol Buffer message schema

use std::{
    collections::BTreeMap,
    io::Write,
    ops::{Deref, RangeInclusive},
    sync::LazyLock,
};

use crate::{AsJsonValue, AsKafkaRecord, Error, Generator, Result, Validator};

use bytes::{BufMut, Bytes, BytesMut};
use fake::Fake;

use nisshi_sans_io::{ErrorCode, record::inflated::Batch};
use protobuf::{
    CodedInputStream, MessageDyn, UnknownValueRef,
    descriptor::{self, FieldDescriptorProto},
    reflect::{
        EnumDescriptor, FileDescriptor, MessageDescriptor, ReflectValueBox, ReflectValueRef,
        RuntimeFieldType, RuntimeType,
    },
    well_known_types,
};
use protobuf_json_mapping::{parse_dyn_from_str, print_to_string};
use rand::prelude::*;
use rhai::{Engine, packages::Package};
use rhai_rand::RandomPackage;
use serde_json::{Map, Value};
use tempfile::{NamedTempFile, tempdir};
use tracing::{debug, error};

#[cfg(any(feature = "parquet", feature = "iceberg", feature = "delta"))]
mod arrow;

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum MessageKind {
    Key,
    Meta,
    Value,
}

impl AsRef<str> for MessageKind {
    fn as_ref(&self) -> &str {
        match self {
            MessageKind::Key => "Key",
            MessageKind::Meta => "Meta",
            MessageKind::Value => "Value",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Schema {
    file_descriptors: Vec<FileDescriptor>,
}

impl Schema {
    fn message_by_package_relative_name(
        &self,
        message_kind: MessageKind,
    ) -> Option<MessageDescriptor> {
        self.file_descriptors
            .iter()
            .find_map(|fd| fd.message_by_package_relative_name(message_kind.as_ref()))
    }

    fn value_to_message(
        &self,
        message_kind: MessageKind,
        json: &Value,
    ) -> Result<Box<dyn MessageDyn>> {
        self.file_descriptors
            .iter()
            .find_map(|fd| fd.message_by_package_relative_name(message_kind.as_ref()))
            .ok_or(Error::Message(format!(
                "message {message_kind:?} not found"
            )))
            .and_then(|message_descriptor| {
                serde_json::to_string(json)
                    .map_err(Error::from)
                    .and_then(|json| {
                        parse_dyn_from_str(&message_descriptor, json.as_str()).map_err(Into::into)
                    })
            })
    }

    pub fn encode_from_value(&self, message_kind: MessageKind, json: &Value) -> Result<Bytes> {
        self.value_to_message(message_kind, json)
            .and_then(message_to_bytes)
    }

    fn message_generator(&self) -> Option<MessageGenerator> {
        self.file_descriptors
            .iter()
            .find_map(|fd| fd.message_by_package_relative_name("Generator"))
            .map(|generator_descriptor| MessageGenerator {
                generator_descriptor,
            })
    }

    fn generate_message_kind(&self, message_kind: MessageKind) -> Result<Option<Bytes>> {
        debug!(?message_kind);

        let engine = {
            let mut engine = Engine::new();

            _ = engine
                .register_fn("first_name", || {
                    fake::faker::name::raw::FirstName(fake::locales::EN).fake::<String>()
                })
                .register_fn("last_name", || {
                    fake::faker::name::raw::LastName(fake::locales::EN).fake::<String>()
                })
                .register_fn("safe_email", || {
                    fake::faker::internet::raw::SafeEmail(fake::locales::EN).fake::<String>()
                })
                .register_fn("building_number", || {
                    fake::faker::address::raw::BuildingNumber(fake::locales::EN).fake::<String>()
                })
                .register_fn("street_name", || {
                    fake::faker::address::raw::StreetName(fake::locales::EN).fake::<String>()
                })
                .register_fn("city_name", || {
                    fake::faker::address::raw::CityName(fake::locales::EN).fake::<String>()
                })
                .register_fn("post_code", || {
                    fake::faker::address::raw::PostCode(fake::locales::EN).fake::<String>()
                })
                .register_fn("country_name", || {
                    fake::faker::address::raw::CountryName(fake::locales::EN).fake::<String>()
                })
                .register_fn("industry", || {
                    fake::faker::company::raw::Industry(fake::locales::EN).fake::<String>()
                });

            let random = RandomPackage::new();
            _ = random.register_into_engine(&mut engine);
            engine
        };

        self.message_by_package_relative_name(message_kind)
            .map_or(Ok(None), |message_descriptor| {
                self.message_generator()
                    .map_or(Ok(None), |message_generator| {
                        message_generator
                            .generate(&engine, &message_descriptor)
                            .and_then(message_to_bytes)
                            .map(Some)
                    })
            })
    }

    fn message_value_as_bytes(
        &self,
        message_kind: MessageKind,
        json: &Value,
    ) -> Result<Option<Bytes>> {
        self.message_by_package_relative_name(message_kind)
            .map(|message_descriptor| {
                serde_json::to_string(json)
                    .map_err(Error::from)
                    .and_then(|json| {
                        parse_dyn_from_str(&message_descriptor, json.as_str()).map_err(Into::into)
                    })
                    .and_then(|message| {
                        let mut w = BytesMut::new().writer();
                        message
                            .write_to_writer_dyn(&mut w)
                            .map(|()| Bytes::from(w.into_inner()))
                            .map_err(Into::into)
                    })
                    .inspect_err(|err| error!(?err))
            })
            .transpose()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct MessageGenerator {
    generator_descriptor: MessageDescriptor,
}

impl MessageGenerator {
    fn generate(
        &self,
        engine: &Engine,
        message_descriptor: &MessageDescriptor,
    ) -> Result<Box<dyn MessageDyn>> {
        debug!(message_descriptor = message_descriptor.full_name());

        let mut message_dyn = message_descriptor.new_instance();

        for (field_proto, field) in message_descriptor
            .proto()
            .field
            .iter()
            .zip(message_dyn.descriptor_dyn().fields())
        {
            let field_generator = FieldGenerator {
                generator_descriptor: &self.generator_descriptor,
                configuration: FieldGeneratorConfiguration::with_field_generator(
                    field_proto,
                    &self.generator_descriptor,
                ),
            };

            if field_generator.configuration.skip() {
                continue;
            }

            match field.runtime_field_type() {
                RuntimeFieldType::Singular(ref singular) => field_generator
                    .singular_value(engine, singular)
                    .map(|value| field.set_singular_field(message_dyn.as_mut(), value))?,

                RuntimeFieldType::Repeated(ref repeated) => {
                    let mut r = field.mut_repeated(message_dyn.as_mut());
                    for element in field_generator.repeated_value(engine, repeated)? {
                        r.push(element);
                    }
                }

                RuntimeFieldType::Map(key, value) => todo!("key={key:?} value={value:?}"),
            }
        }

        Ok(message_dyn)
    }
}

struct FieldGenerator<'a> {
    generator_descriptor: &'a MessageDescriptor,
    configuration: FieldGeneratorConfiguration,
}

impl<'a> FieldGenerator<'a> {
    fn singular_value(
        &self,
        engine: &Engine,
        runtime_type: &RuntimeType,
    ) -> Result<ReflectValueBox> {
        let mut rng = rand::rng();
        match runtime_type {
            RuntimeType::I32 => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<i32>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::I64 => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<i64>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::U32 => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<u32>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::U64 => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<u64>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::F32 => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<f32>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::F64 => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<f64>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::Bool => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(rng.random()), |script| {
                    engine
                        .eval::<bool>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::String => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(String::from("abc")), |script| {
                    engine
                        .eval::<String>(script)
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map(ReflectValueBox::from)
                .map_err(Into::into),

            RuntimeType::VecU8 => todo!(),

            RuntimeType::Enum(descriptor) => self
                .configuration
                .script()
                .inspect(|script| debug!(script))
                .map_or(Ok(ReflectValueBox::Enum(descriptor.clone(), 1)), |script| {
                    engine
                        .eval::<String>(script)
                        .inspect(|name| debug!(name))
                        .and_then(|name| {
                            descriptor
                                .value_by_name(&name[..])
                                .inspect(|value_descriptor| debug!(?value_descriptor))
                                .map(|value_descriptor| {
                                    ReflectValueBox::Enum(
                                        descriptor.clone(),
                                        value_descriptor.value(),
                                    )
                                })
                                .inspect(|value| debug!(?value))
                                .ok_or_else(|| format!("unknown enum value name: {name}").into())
                        })
                        .inspect_err(|err| debug!(script, ?err))
                })
                .inspect(|result| debug!(?result))
                .map_err(Into::into),

            RuntimeType::Message(message_descriptor) => {
                let generator = MessageGenerator {
                    generator_descriptor: self.generator_descriptor.to_owned(),
                };

                generator
                    .generate(engine, message_descriptor)
                    .map(ReflectValueBox::Message)
            }
        }
    }

    fn repeated_value(
        &self,
        engine: &Engine,
        runtime_type: &RuntimeType,
    ) -> Result<Vec<ReflectValueBox>> {
        let upper = self.configuration.repeated_len().unwrap_or_else(|| {
            rand::rng().random_range(self.configuration.repeated_range().unwrap_or(0..=1))
        });

        (0..upper)
            .inspect(|i| debug!(i))
            .map(|_| self.singular_value(engine, runtime_type))
            .collect::<Result<Vec<_>>>()
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
enum FieldGeneratorConfiguration {
    Bool(bool),
    Bytes(Bytes),
    F32(f32),
    F64(f64),
    I32(i32),
    I64(i64),
    List(Vec<FieldGeneratorConfiguration>),
    Message(BTreeMap<String, FieldGeneratorConfiguration>),
    String(String),
    U32(u32),
    U64(u64),
}

impl Default for FieldGeneratorConfiguration {
    fn default() -> Self {
        Self::Message(Default::default())
    }
}

impl FieldGeneratorConfiguration {
    fn with_field_generator(
        field: &FieldDescriptorProto,
        generator: &MessageDescriptor,
    ) -> FieldGeneratorConfiguration {
        debug!(field = field.name(), generator = generator.full_name(),);

        field
            .options
            .special_fields
            .unknown_fields()
            .iter()
            .find_map(|(id, unknown)| {
                if id != 51215 {
                    None
                } else if let UnknownValueRef::LengthDelimited(items) = unknown {
                    let mut message = generator.new_instance();

                    _ = message
                        .merge_from_bytes_dyn(items)
                        .inspect_err(|err| debug!(?err))
                        .ok();

                    Some(Self::from(message.as_ref()))
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }

    fn skip(&self) -> bool {
        self.get("skip")
            .cloned()
            .and_then(|value| value.as_bool())
            .inspect(|skip| debug!(?skip))
            .unwrap_or_default()
    }

    fn script(&self) -> Option<&str> {
        self.get("script")
            .inspect(|script| debug!(?script))
            .and_then(|value| value.as_str())
            .inspect(|script| debug!(?script))
            .or(self.repeated_script())
    }

    fn repeated_range(&self) -> Option<RangeInclusive<u32>> {
        self.get("repeated")
            .inspect(|repeated| debug!(?repeated))
            .and_then(|repeated| {
                repeated
                    .get("range")
                    .inspect(|range| debug!(?range))
                    .and_then(|range| {
                        range
                            .get("min")
                            .inspect(|min| debug!(?min))
                            .and_then(|min| min.as_u32())
                            .and_then(|min| {
                                range
                                    .get("max")
                                    .inspect(|max| debug!(?max))
                                    .and_then(|max| max.as_u32())
                                    .map(|max| min..=max)
                            })
                    })
            })
    }

    fn repeated_len(&self) -> Option<u32> {
        self.get("repeated")
            .and_then(|repeated| repeated.get("len"))
            .and_then(|len| len.as_u32())
    }

    fn repeated_script(&self) -> Option<&str> {
        self.get("repeated")
            .and_then(|repeated| repeated.get("script"))
            .and_then(|script| script.as_str())
    }

    fn get(&self, key: &str) -> Option<&FieldGeneratorConfiguration> {
        if let Self::Message(message) = self {
            message.get(key)
        } else {
            None
        }
    }

    fn as_bool(&self) -> Option<bool> {
        if let Self::Bool(flag) = self {
            Some(*flag)
        } else {
            None
        }
    }

    fn as_str(&self) -> Option<&str> {
        if let Self::String(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn as_u32(&self) -> Option<u32> {
        if let Self::U32(value) = self {
            Some(*value)
        } else {
            None
        }
    }
}

impl From<EnumDescriptor> for FieldGeneratorConfiguration {
    fn from(_value: EnumDescriptor) -> Self {
        todo!()
    }
}

impl<'a> From<ReflectValueRef<'a>> for FieldGeneratorConfiguration {
    fn from(value: ReflectValueRef<'a>) -> Self {
        match value {
            ReflectValueRef::U32(value) => Self::U32(value),
            ReflectValueRef::U64(value) => Self::U64(value),
            ReflectValueRef::I32(value) => Self::I32(value),
            ReflectValueRef::I64(value) => Self::I64(value),
            ReflectValueRef::F32(value) => Self::F32(value),
            ReflectValueRef::F64(value) => Self::F64(value),
            ReflectValueRef::Bool(value) => Self::Bool(value),
            ReflectValueRef::String(value) => Self::String(value.to_owned()),
            ReflectValueRef::Bytes(items) => Self::Bytes(Bytes::copy_from_slice(items)),
            ReflectValueRef::Enum(enum_descriptor, _) => Self::from(enum_descriptor),
            ReflectValueRef::Message(message_ref) => Self::from(message_ref.deref()),
        }
    }
}

impl From<&dyn MessageDyn> for FieldGeneratorConfiguration {
    fn from(message: &dyn MessageDyn) -> Self {
        debug!(%message);

        Self::Message(
            message
                .descriptor_dyn()
                .fields()
                .inspect(|field| debug!(field = field.name()))
                .filter_map(|field| match field.runtime_field_type() {
                    RuntimeFieldType::Singular(singular) => {
                        debug!(?singular);
                        field
                            .get_singular(message)
                            .inspect(|value| debug!(field = field.name(), ?value))
                            .map(|value| (field.name().to_owned(), Self::from(value)))
                    }

                    RuntimeFieldType::Repeated(repeated) => {
                        debug!(?repeated);
                        Some((
                            field.name().to_owned(),
                            Self::List(
                                field
                                    .get_repeated(message)
                                    .into_iter()
                                    .map(Self::from)
                                    .inspect(|configuration| debug!(?configuration))
                                    .collect::<Vec<_>>(),
                            ),
                        ))
                    }

                    RuntimeFieldType::Map(key, value) => todo!("key={key:?} value={value:?}"),
                })
                .inspect(|(field, value)| debug!(field, ?value))
                .collect::<BTreeMap<String, FieldGeneratorConfiguration>>(),
        )
    }
}

fn message_to_bytes(message: Box<dyn MessageDyn>) -> Result<Bytes> {
    let mut w = BytesMut::new().writer();
    message
        .write_to_writer_dyn(&mut w)
        .and(Ok(Bytes::from(w.into_inner())))
        .map_err(Into::into)
}

impl AsKafkaRecord for Schema {
    fn as_kafka_record(&self, value: &Value) -> Result<nisshi_sans_io::record::Builder> {
        let mut builder = nisshi_sans_io::record::Record::builder();

        if let Some(value) = value.get("key")
            && let Some(encoded) = self.message_value_as_bytes(MessageKind::Key, value)?
        {
            builder = builder.key(encoded.into());
        }

        if let Some(value) = value.get("value")
            && let Some(encoded) = self.message_value_as_bytes(MessageKind::Value, value)?
        {
            builder = builder.value(encoded.into());
        }

        Ok(builder)
    }
}

impl Generator for Schema {
    fn generate(&self) -> Result<nisshi_sans_io::record::Builder> {
        let mut builder = nisshi_sans_io::record::Record::builder();

        if let Some(generated) = self.generate_message_kind(MessageKind::Key)? {
            builder = builder.key(generated.into());
        }

        if let Some(generated) = self.generate_message_kind(MessageKind::Value)? {
            builder = builder.value(generated.into());
        }

        Ok(builder)
    }
}

fn decode(
    message_descriptor: Option<MessageDescriptor>,
    encoded: Option<Bytes>,
) -> Result<Option<Box<dyn MessageDyn>>> {
    debug!(?message_descriptor, len = ?encoded.as_ref().map(Bytes::len));

    message_descriptor.map_or(Ok(None), |message_descriptor| {
        encoded.map_or(Err(Error::Api(ErrorCode::InvalidRecord)), |encoded| {
            let mut message = message_descriptor.new_instance();

            message
                .merge_from_dyn(&mut CodedInputStream::from_tokio_bytes(&encoded))
                .inspect_err(|err| error!(?err))
                .map_err(|_err| Error::Api(ErrorCode::InvalidRecord))
                .and(Ok(Some(message)))
        })
    })
}

fn validate(message_descriptor: Option<MessageDescriptor>, encoded: Option<Bytes>) -> Result<()> {
    decode(message_descriptor, encoded).and(Ok(()))
}

impl Validator for Schema {
    fn validate(&self, batch: &Batch) -> Result<()> {
        debug!(?batch);

        for record in &batch.records {
            debug!(?record);

            validate(
                self.message_by_package_relative_name(MessageKind::Key),
                record.key.clone(),
            )
            .and(validate(
                self.message_by_package_relative_name(MessageKind::Value),
                record.value.clone(),
            ))
            .inspect_err(|err| error!(?err))?
        }

        Ok(())
    }
}

impl TryFrom<Bytes> for Schema {
    type Error = Error;

    fn try_from(proto: Bytes) -> Result<Self, Self::Error> {
        make_fd(proto)
            .map(|mut protos| {
                debug!(
                    protos = ?protos
                        .iter()
                        .flat_map(|proto| {
                            proto
                                .messages()
                                .map(|message| message.name_to_package().to_owned())
                        })
                        .collect::<Vec<_>>()
                );

                if let Some(mut meta) = META_FILE_DESCRIPTOR.clone() {
                    debug!(
                        meta = ?meta
                            .iter()
                            .flat_map(|proto| {
                                proto
                                    .messages()
                                    .map(|message| message.name_to_package().to_owned())
                            })
                            .collect::<Vec<_>>()
                    );

                    protos.append(&mut meta);
                }

                protos
            })
            .map(|file_descriptors| Self { file_descriptors })
    }
}

static WELL_KNOWN_TYPES: LazyLock<Vec<FileDescriptor>> = LazyLock::new(|| {
    vec![
        descriptor::file_descriptor().to_owned(),
        well_known_types::duration::file_descriptor().to_owned(),
        well_known_types::empty::file_descriptor().to_owned(),
        well_known_types::source_context::file_descriptor().to_owned(),
        well_known_types::timestamp::file_descriptor().to_owned(),
        well_known_types::wrappers::file_descriptor().to_owned(),
    ]
});

static META_FILE_DESCRIPTOR: LazyLock<Option<Vec<FileDescriptor>>> =
    LazyLock::new(|| make_fd(Bytes::from_static(include_bytes!("meta.proto"))).ok());

fn make_fd(proto: Bytes) -> Result<Vec<FileDescriptor>> {
    tempdir().map_err(Into::into).and_then(|temp_dir| {
        NamedTempFile::new_in(&temp_dir)
            .inspect(|temp_dir| debug!(?temp_dir))
            .map_err(Into::into)
            .and_then(|mut temp_file| {
                temp_file.write_all(&proto).map_err(Into::into).and(
                    protobuf_parse::Parser::new()
                        .pure()
                        .input(&temp_file)
                        .include(&temp_dir)
                        .parse_and_typecheck()
                        .inspect_err(|err| debug!(?err))
                        .map_err(Into::into)
                        .and_then(|parsed| {
                            parsed
                                .file_descriptors
                                .into_iter()
                                .map(|file_descriptor_proto| {
                                    FileDescriptor::new_dynamic(
                                        file_descriptor_proto,
                                        &WELL_KNOWN_TYPES[..],
                                    )
                                    .inspect_err(|err| debug!(?err))
                                    .map_err(Into::into)
                                })
                                .collect::<Result<Vec<_>>>()
                        }),
                )
            })
    })
}

impl Schema {
    fn to_json_value(
        &self,
        message_kind: MessageKind,
        encoded: Option<Bytes>,
    ) -> Result<(String, Value)> {
        decode(self.message_by_package_relative_name(message_kind), encoded).and_then(|decoded| {
            decoded.map_or(
                Ok((message_kind.as_ref().to_lowercase(), Value::Null)),
                |message| {
                    print_to_string(message.as_ref())
                        .map_err(Into::into)
                        .and_then(|s| serde_json::from_str::<Value>(&s).map_err(Into::into))
                        .map(|value| (message_kind.as_ref().to_lowercase(), value))
                        .inspect(|(k, _)| debug!(k))
                },
            )
        })
    }
}

impl AsJsonValue for Schema {
    fn as_json_value(&self, batch: &Batch) -> Result<Value> {
        batch
            .records
            .iter()
            .inspect(|record| debug!(?record))
            .map(|record| {
                // A side that fails is an error, not a missing field: `Result` is an iterator
                // of zero items for an `Err`, which would drop the field from the output.
                let key = self.to_json_value(MessageKind::Key, record.key.clone())?;
                let value = self.to_json_value(MessageKind::Value, record.value.clone())?;

                Ok(Value::Object(Map::from_iter([key, value])))
            })
            .collect::<Result<Vec<_>>>()
            .map(Value::Array)
    }
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

    #[tokio::test]
    async fn key_only_invalid_record() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let proto = Bytes::from_static(
            br#"
            syntax = 'proto3';

            message Key {
              int32 id = 1;
            }

            message Value {
              string name = 1;
              string email = 2;
            }
            "#,
        );

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.proto"));
        let payload = PutPayload::from(proto.clone());
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let key = Schema::try_from(proto.clone())
            .and_then(|schema| schema.encode_from_value(MessageKind::Key, &json!({"id": 12321})))?;

        let batch = Batch::builder()
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

        let proto = Bytes::from_static(
            br#"
                syntax = 'proto3';

                message Key {
                  int32 id = 1;
                }

                message Value {
                  string name = 1;
                  string email = 2;
                }
                "#,
        );

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.proto"));
        let payload = PutPayload::from(proto.clone());
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let value = Schema::try_from(proto).and_then(|schema| {
            schema.encode_from_value(
                MessageKind::Value,
                &json!({
                    "name": "alice",
                    "email": "alice@example.com"
                }),
            )
        })?;

        let batch = Batch::builder()
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

        let proto = Bytes::from_static(
            br#"
                syntax = 'proto3';

                message Key {
                  int32 id = 1;
                }

                message Value {
                  string name = 1;
                  string email = 2;
                }
                "#,
        );

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.proto"));
        let payload = PutPayload::from(proto.clone());
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let schema = Schema::try_from(proto.clone())?;

        let key = schema.encode_from_value(MessageKind::Key, &json!({"id": 12321}))?;
        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
                "name": "alice",
                "email": "alice@example.com"
            }),
        )?;

        let batch = Batch::builder()
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

        let proto = Bytes::from_static(br#"syntax = 'proto3';"#);

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.proto"));
        let payload = PutPayload::from(proto.clone());
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

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

    #[tokio::test]
    async fn key_schema_only() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let proto = Bytes::from_static(
            br#"
                syntax = 'proto3';

                message Key {
                  int32 id = 1;
                }
                "#,
        );

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.proto"));
        let payload = PutPayload::from(proto.clone());
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let schema = Schema::try_from(proto)?;

        let key = schema.encode_from_value(MessageKind::Key, &json!({"id": 12321}))?;
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

    #[tokio::test]
    async fn bad_key() -> Result<()> {
        let _guard = init_tracing()?;

        let topic = "def";

        let proto = Bytes::from_static(
            br#"
                syntax = 'proto3';

                message Key {
                  int32 id = 1;
                }
                "#,
        );

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.proto"));
        let payload = PutPayload::from(proto.clone());
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");

        let batch = Batch::builder()
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

        let proto = Bytes::from_static(
            br#"
            syntax = 'proto3';

            message Value {
                string name = 1;
                string email = 2;
            }
            "#,
        );

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.proto"));
        let payload = PutPayload::from(proto.clone());
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let schema = Schema::try_from(proto)?;

        let key = Bytes::from_static(b"Lorem ipsum dolor sit amet");

        let value = schema.encode_from_value(
            MessageKind::Value,
            &json!({
                "name": "alice",
                "email": "alice@example.com"
            }),
        )?;

        let batch = Batch::builder()
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

        let proto = Bytes::from_static(
            br#"
                syntax = 'proto3';

                message Value {
                  string name = 1;
                  string email = 2;
                }
                "#,
        );

        let object_store = InMemory::new();
        let location = Path::from(format!("{topic}.proto"));
        let payload = PutPayload::from(proto.clone());
        _ = object_store.put(&location, payload).await?;

        let registry = Registry::new(object_store);

        let value = Bytes::from_static(b"Consectetur adipiscing elit");

        let batch = Batch::builder()
            .record(Record::builder().value(value.clone().into()))
            .build()?;

        assert!(matches!(
            registry.validate(topic, &batch).await,
            Err(Error::Api(ErrorCode::InvalidRecord))
        ));

        Ok(())
    }

    #[test]
    fn timestamp_well_known_type() -> Result<()> {
        let _guard = init_tracing()?;
        let proto = Bytes::from_static(
            br#"
            syntax = "proto3";

            import "google/protobuf/timestamp.proto";

            message Value {
                google.protobuf.Timestamp timestamp = 1;
            }
            "#,
        );

        let _file_descriptor = make_fd(proto).inspect(|fds| debug!(?fds))?;

        Ok(())
    }

    #[tokio::test]
    async fn customer_002_user_id() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = Schema::try_from(Bytes::from_static(include_bytes!(
            "../tests/customer-002.proto"
        )))?;

        let message_generator = schema.message_generator().unwrap();

        let user_id = schema
            .message_by_package_relative_name(MessageKind::Value)
            .and_then(|message_descriptor| message_descriptor.field_by_name("user_id"))
            .unwrap();

        let configuration = FieldGeneratorConfiguration::with_field_generator(
            user_id.proto(),
            &message_generator.generator_descriptor,
        );

        assert!(configuration.skip());

        Ok(())
    }

    #[tokio::test]
    async fn customer_002_email_address() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = Schema::try_from(Bytes::from_static(include_bytes!(
            "../tests/customer-002.proto"
        )))?;

        let message_generator = schema.message_generator().unwrap();

        let user_id = schema
            .message_by_package_relative_name(MessageKind::Value)
            .and_then(|message_descriptor| message_descriptor.field_by_name("email_address"))
            .unwrap();

        let configuration = FieldGeneratorConfiguration::with_field_generator(
            user_id.proto(),
            &message_generator.generator_descriptor,
        );

        assert!(!configuration.skip());
        assert_eq!(Some("\"lorem\""), configuration.script());

        Ok(())
    }

    #[tokio::test]
    async fn customer_002_industry() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = Schema::try_from(Bytes::from_static(include_bytes!(
            "../tests/customer-002.proto"
        )))?;

        let message_generator = schema.message_generator().unwrap();

        let user_id = schema
            .message_by_package_relative_name(MessageKind::Value)
            .and_then(|message_descriptor| message_descriptor.field_by_name("industry"))
            .unwrap();

        let configuration = FieldGeneratorConfiguration::with_field_generator(
            user_id.proto(),
            &message_generator.generator_descriptor,
        );

        assert!(!configuration.skip());
        assert_eq!(Some(3), configuration.repeated_len());
        assert_eq!(Some("\"elit\""), configuration.repeated_script());

        Ok(())
    }

    #[tokio::test]
    async fn customer_003_industry() -> Result<()> {
        let _guard = init_tracing()?;

        let schema = Schema::try_from(Bytes::from_static(include_bytes!(
            "../tests/customer-003.proto"
        )))?;

        let message_generator = schema.message_generator().unwrap();

        let user_id = schema
            .message_by_package_relative_name(MessageKind::Value)
            .and_then(|message_descriptor| message_descriptor.field_by_name("industry"))
            .unwrap();

        let configuration = FieldGeneratorConfiguration::with_field_generator(
            user_id.proto(),
            &message_generator.generator_descriptor,
        );

        assert!(!configuration.skip());
        assert_eq!(Some(1..=3), configuration.repeated_range());
        assert_eq!(Some("\"elit\""), configuration.repeated_script());

        Ok(())
    }

    const KEY_VALUE_PROTO: &[u8] = br#"
        syntax = 'proto3';

        message Key {
          int32 id = 1;
        }

        message Value {
          string name = 1;
          string email = 2;
        }
        "#;

    #[test]
    fn as_json_value_prints_key_and_value() -> Result<()> {
        let schema = Schema::try_from(Bytes::from_static(KEY_VALUE_PROTO))?;

        let batch = Batch::builder()
            .record(
                Record::builder()
                    .key(
                        schema
                            .encode_from_value(MessageKind::Key, &json!({"id": 12321}))?
                            .into(),
                    )
                    .value(
                        schema
                            .encode_from_value(MessageKind::Value, &json!({"name": "alice"}))?
                            .into(),
                    ),
            )
            .build()?;

        assert_eq!(
            json!([{"key": {"id": 12321}, "value": {"name": "alice"}}]),
            schema.as_json_value(&batch)?
        );

        Ok(())
    }

    #[test]
    fn as_json_value_refuses_a_record_that_is_not_protobuf() -> Result<()> {
        let schema = Schema::try_from(Bytes::from_static(KEY_VALUE_PROTO))?;

        // A field with the wrong wire type, which is not an empty message but an error.
        let batch = Batch::builder()
            .record(
                Record::builder()
                    .key(
                        schema
                            .encode_from_value(MessageKind::Key, &json!({"id": 1}))?
                            .into(),
                    )
                    .value(Bytes::from_static(&[0x0a, 0xff]).into()),
            )
            .build()?;

        assert_matches!(
            schema.as_json_value(&batch),
            Err(Error::Api(ErrorCode::InvalidRecord))
        );

        Ok(())
    }
}
