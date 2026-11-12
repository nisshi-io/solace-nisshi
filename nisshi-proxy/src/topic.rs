// Copyright ⓒ Solace Corporation
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
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use nisshi_sans_io::{
    ConfigResource, ConfigType, DescribeConfigsRequest, DescribeConfigsResponse, ErrorCode,
    ProduceRequest, ProduceResponse, RequestInput,
    describe_configs_request::DescribeConfigsResource, topic_config,
};
use rama::{Layer, Service, extensions::Extensions, matcher::Matcher};
use tracing::{debug, error};

use crate::{Error, produce::topic_names};

#[allow(dead_code)]
fn with_topics(
    req: &mut DescribeConfigsRequest,
    topics: impl IntoIterator<Item = impl Into<String>>,
) {
    let resources = req.resources.get_or_insert_default();

    resources.extend(topics.into_iter().map(|topic| {
        DescribeConfigsResource::default()
            .resource_type(i8::from(ConfigResource::Topic))
            .resource_name(topic.into())
            .configuration_keys(Some([].into()))
    }));
}

#[allow(dead_code)]
fn with_topic(req: &mut DescribeConfigsRequest, resource_name: impl Into<String>) {
    let resources = req.resources.get_or_insert_default();
    resources.push(
        DescribeConfigsResource::default()
            .resource_type(i8::from(ConfigResource::Topic))
            .resource_name(resource_name.into())
            .configuration_keys(Some([].into())),
    );
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum ResourceConfigValue {
    Unknown(String),
    Boolean(bool),
    String(String),
    Int(i32),
    Short(i16),
    Long(i64),
    Double(f64),
    List(Vec<String>),
    Class(String),
    Password(String),
}

impl From<(String, ConfigType)> for ResourceConfigValue {
    fn from((value, config_type): (String, ConfigType)) -> Self {
        match config_type {
            ConfigType::Unknown => Self::Unknown(value),
            ConfigType::Boolean => value.parse().ok().map_or(
                ResourceConfigValue::Unknown(value),
                ResourceConfigValue::Boolean,
            ),
            ConfigType::String => ResourceConfigValue::String(value),
            ConfigType::Int => value.parse().ok().map_or(
                ResourceConfigValue::Unknown(value),
                ResourceConfigValue::Int,
            ),
            ConfigType::Short => value.parse().ok().map_or(
                ResourceConfigValue::Unknown(value),
                ResourceConfigValue::Short,
            ),
            ConfigType::Long => value.parse().ok().map_or(
                ResourceConfigValue::Unknown(value),
                ResourceConfigValue::Long,
            ),
            ConfigType::Double => value.parse().ok().map_or(
                ResourceConfigValue::Unknown(value),
                ResourceConfigValue::Double,
            ),
            ConfigType::List => ResourceConfigValue::String(value),
            ConfigType::Class => ResourceConfigValue::String(value),
            ConfigType::Password => ResourceConfigValue::String(value),
        }
    }
}

impl From<bool> for ResourceConfigValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<i32> for ResourceConfigValue {
    fn from(value: i32) -> Self {
        Self::Int(value)
    }
}

impl From<&str> for ResourceConfigValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ResourceConfigValueMatcher {
    resource_config: ResourceConfig,
    key: String,
    value: ResourceConfigValue,
}

impl ResourceConfigValueMatcher {
    pub(crate) fn new(
        resource_config: ResourceConfig,
        key: impl Into<String>,
        value: impl Into<ResourceConfigValue>,
    ) -> Self {
        Self {
            resource_config,
            key: key.into(),
            value: value.into(),
        }
    }
}

impl Matcher<RequestInput<ProduceRequest>> for ResourceConfigValueMatcher {
    fn matches(&self, _ext: Option<&Extensions>, req: &RequestInput<ProduceRequest>) -> bool {
        debug!(?req);

        topic_names(&req.request)
            .into_iter()
            .inspect(|topic_name| debug!(%topic_name))
            .any(|topic_name| {
                self.resource_config
                    .get(&topic_name, &self.key)
                    .inspect(|value| debug!(%topic_name, key = %self.key, ?value))
                    .is_some_and(|value| value == self.value)
            })
    }
}

impl ResourceConfigValue {
    #[allow(dead_code)]
    pub(crate) fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(value) => Some(*value),
            Self::String(value) => value.parse().ok(),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    pub(crate) fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Short(value) if !value.is_negative() => Some(*value as u64),
            Self::Int(value) if !value.is_negative() => Some(*value as u64),
            Self::Long(value) if !value.is_negative() => Some(*value as u64),
            Self::String(value) => value.parse().ok(),
            _ => None,
        }
    }

    pub(crate) fn as_usize(&self) -> Option<usize> {
        match self {
            Self::Short(value) if !value.is_negative() => Some(*value as usize),
            Self::Int(value) if !value.is_negative() => Some(*value as usize),
            Self::Long(value) if !value.is_negative() => Some(*value as usize),
            Self::String(value) => value.parse().ok(),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn as_i16(&self) -> Option<i16> {
        match self {
            Self::Short(value) => Some(*value),
            Self::String(value) => value.parse().ok(),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn as_i32(&self) -> Option<i32> {
        match self {
            Self::Int(value) => Some(*value),
            Self::String(value) => value.parse().ok(),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Long(value) => Some(*value),
            Self::String(value) => value.parse().ok(),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ResourceConfig {
    configuration: Arc<Mutex<BTreeMap<String, BTreeMap<String, ResourceConfigValue>>>>,
}

impl ResourceConfig {
    pub(crate) fn has_resource(&self, resource_name: &str) -> bool {
        debug!(?self, resource_name);
        self.configuration
            .lock()
            .map(|guard| guard.contains_key(resource_name))
            .ok()
            .unwrap_or_default()
    }

    pub(crate) fn get(&self, resource_name: &str, key: &str) -> Option<ResourceConfigValue> {
        debug!(resource_name, key);

        self.configuration
            .lock()
            .map(|guard| {
                guard
                    .get(resource_name)
                    .inspect(|resource_configuration| debug!(?resource_configuration))
                    .and_then(|resource_configuration| {
                        topic_config::lookup_by(key, |name| resource_configuration.get(name))
                            .cloned()
                    })
            })
            .inspect_err(|err| error!(resource_name, key, ?err))
            .ok()
            .flatten()
    }

    pub(crate) fn put(
        &self,
        resource_name: impl Into<String>,
        key: impl Into<String>,
        value: impl Into<ResourceConfigValue>,
    ) -> Result<(), Error> {
        let resource_name = resource_name.into();
        let key = key.into();
        let value = value.into();

        debug!(?self, resource_name, key, ?value);

        self.configuration
            .lock()
            .map(|mut guard| {
                _ = guard
                    .entry(resource_name.clone())
                    .or_default()
                    .entry(key.clone())
                    .or_insert(value.clone());
            })
            .inspect_err(|err| error!(resource_name, key, ?value, ?err))
            .map_err(|_| Error::ResourceLock {
                name: resource_name,
                key: Some(key),
                value: Some(value),
            })
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TopicConfigService<I, O> {
    resource_config: ResourceConfig,
    inner: I,
    outer: O,
}

impl<I, O> TopicConfigService<I, O> {
    fn add_topic_configuration(&self, response: DescribeConfigsResponse) -> Result<(), Error> {
        debug!(?response);
        for result in response.results.unwrap_or_default() {
            debug!(?result);

            if result.error_code != i16::from(ErrorCode::None)
                || result.resource_type != i8::from(ConfigResource::Topic)
                || result.configs.is_none()
            {
                continue;
            }

            for resource_result in result.configs.as_deref().unwrap_or_default() {
                debug!(?resource_result);

                if let Some(config_value) = resource_result
                    .value
                    .as_deref()
                    .and_then(|value| {
                        resource_result
                            .config_type
                            .map(ConfigType::from)
                            .map(|config_type| (value.to_string(), config_type))
                            .map(ResourceConfigValue::from)
                    })
                    .inspect(|config_value| debug!(?config_value))
                {
                    self.resource_config.put(
                        result.resource_name.clone(),
                        resource_result.name.clone(),
                        config_value,
                    )?;
                }
            }
        }

        Ok(())
    }
}

impl<I, O> Service<RequestInput<ProduceRequest>> for TopicConfigService<I, O>
where
    I: Service<RequestInput<ProduceRequest>, Output = ProduceResponse>,
    O: Service<RequestInput<DescribeConfigsRequest>, Output = DescribeConfigsResponse>,
    I::Error: From<Error> + From<O::Error>,
{
    type Output = ProduceResponse;
    type Error = I::Error;

    async fn serve(&self, req: RequestInput<ProduceRequest>) -> Result<Self::Output, Self::Error> {
        debug!(?req);

        if !topic_names(&req.request)
            .iter()
            .all(|topic| self.resource_config.has_resource(topic))
        {
            self.outer
                .serve(RequestInput {
                    request: DescribeConfigsRequest::default()
                        .include_documentation(Some(false))
                        .include_synonyms(Some(false))
                        .resources(Some(
                            topic_names(&req.request)
                                .into_iter()
                                .map(|resource_name| {
                                    DescribeConfigsResource::default()
                                        .resource_type(i8::from(ConfigResource::Topic))
                                        .resource_name(resource_name)
                                        .configuration_keys(Some([].into()))
                                })
                                .collect(),
                        )),
                    extensions: req.extensions.fork(),
                })
                .await
                .map_err(I::Error::from)
                .and_then(|config_response| {
                    self.add_topic_configuration(config_response)
                        .map_err(Into::into)
                })?;
        }

        self.inner.serve(req).await
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TopicConfigLayer<O> {
    resource_config: ResourceConfig,
    outer: O,
}

impl<O> TopicConfigLayer<O> {
    pub(crate) fn new(resource_config: ResourceConfig, outer: O) -> Self {
        Self {
            resource_config,
            outer,
        }
    }
}

impl<I, O> Layer<I> for TopicConfigLayer<O>
where
    O: Clone,
{
    type Service = TopicConfigService<I, O>;

    fn layer(&self, inner: I) -> Self::Service {
        TopicConfigService {
            resource_config: self.resource_config.clone(),
            inner,
            outer: self.outer.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, thread};

    use bytes::Bytes;
    use nisshi_sans_io::{
        BatchAttribute, Compression,
        describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
        produce_request::{PartitionProduceData, TopicProduceData},
        produce_response::{PartitionProduceResponse, TopicProduceResponse},
        record::{Record, deflated, inflated},
    };
    use nisshi_service::{
        BytesFrameLayer, FrameBytesLayer, FrameRouteService, RequestFrameLayer, RequestLayer,
        ResponseService,
    };
    use rama::layer::HijackLayer;
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::EnvFilter;

    use super::*;

    fn init_tracing() -> Result<DefaultGuard, Error> {
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
    async fn fetch_topic_configuration_on_produce() -> Result<(), Error> {
        let _guard = init_tracing()?;

        let configuration = ResourceConfig::default();

        const RESOURCE_NAME_0: &str = "abc";
        const RESOURCE_NAME_1: &str = "xyz";
        const PARAMETER: &str = "pqr";

        fn produce_response(
            error_code: ErrorCode,
        ) -> impl Fn(RequestInput<ProduceRequest>) -> Result<ProduceResponse, Error> + Clone
        {
            move |req: RequestInput<ProduceRequest>| {
                Ok(ProduceResponse::default()
                    .node_endpoints(Some([].into()))
                    .responses(req.request.topic_data.map(|topics| {
                        topics
                            .into_iter()
                            .map(|topic| {
                                TopicProduceResponse::default()
                                    .name(topic.name)
                                    .partition_responses(topic.partition_data.map(|partitions| {
                                        partitions
                                            .into_iter()
                                            .map(|partition| {
                                                PartitionProduceResponse::default()
                                                    .index(partition.index)
                                                    .error_code(i16::from(error_code))
                                                    .log_append_time_ms(Some(0))
                                                    .log_start_offset(Some(0))
                                            })
                                            .collect::<Vec<_>>()
                                    }))
                            })
                            .collect::<Vec<_>>()
                    }))
                    .throttle_time_ms(Some(0)))
            }
        }

        let produce_request = |topic: &str| -> Result<RequestInput<ProduceRequest>, Error> {
            inflated::Batch::builder()
                .attributes(
                    BatchAttribute::default()
                        .compression(Compression::None)
                        .into(),
                )
                .record(Record::builder().value(Bytes::from_static(b"fgh").into()))
                .build()
                .and_then(deflated::Batch::try_from)
                .map(|batch| {
                    ProduceRequest::default().topic_data(Some(
                        [TopicProduceData::default()
                            .name(topic.into())
                            .partition_data(Some(
                                [PartitionProduceData::default().index(0).records(Some(
                                    deflated::Frame {
                                        batches: [batch].into(),
                                    },
                                ))]
                                .into(),
                            ))]
                        .into(),
                    ))
                })
                .map(Into::into)
                .map_err(Into::into)
        };

        let describe_configuration = RequestLayer::<DescribeConfigsRequest>::new().into_layer(
            ResponseService::new(|req: RequestInput<DescribeConfigsRequest>| {
                Ok::<_, Error>(DescribeConfigsResponse::default().results(
                    req.request.resources.map(|resources| {
                        resources
                            .into_iter()
                            .map(|resource| match resource.resource_name.as_str() {
                                RESOURCE_NAME_0 => DescribeConfigsResult::default()
                                    .resource_name(RESOURCE_NAME_0.into())
                                    .resource_type(i8::from(ConfigResource::Topic))
                                    .configs(Some(
                                        [DescribeConfigsResourceResult::default()
                                            .config_type(Some(i8::from(ConfigType::Boolean)))
                                            .name(PARAMETER.into())
                                            .value(Some("true".into()))]
                                        .into(),
                                    )),

                                RESOURCE_NAME_1 => DescribeConfigsResult::default()
                                    .resource_name(RESOURCE_NAME_1.into())
                                    .resource_type(i8::from(ConfigResource::Topic))
                                    .configs(Some(
                                        [DescribeConfigsResourceResult::default()
                                            .config_type(Some(i8::from(ConfigType::Boolean)))
                                            .name(PARAMETER.into())
                                            .value(Some("false".into()))]
                                        .into(),
                                    )),

                                otherwise => unreachable!("{otherwise:?}"),
                            })
                            .collect::<Vec<_>>()
                    }),
                ))
            }),
        );

        let parameter_set = HijackLayer::new(
            ResourceConfigValueMatcher::new(configuration.clone(), PARAMETER, true),
            ResponseService::new(produce_response(ErrorCode::CoordinatorNotAvailable)),
        );

        let produce = (
            RequestLayer::<ProduceRequest>::new(),
            TopicConfigLayer::new(configuration.clone(), describe_configuration),
            parameter_set,
        )
            .into_layer(ResponseService::new(produce_response(ErrorCode::None)));

        let service = (
            RequestFrameLayer,
            FrameBytesLayer,
            BytesFrameLayer::default(),
        )
            .into_layer(
                FrameRouteService::builder()
                    .with_service(produce)
                    .and_then(|builder| builder.build())?,
            );

        assert!(!configuration.has_resource(RESOURCE_NAME_0));

        {
            let response = service.serve(produce_request(RESOURCE_NAME_0)?).await?;

            let responses = response.responses.unwrap_or_default();
            assert_eq!(1, responses.len());
            assert_eq!(RESOURCE_NAME_0, responses[0].name);

            let partitions = responses[0]
                .partition_responses
                .as_deref()
                .unwrap_or_default();
            assert_eq!(1, partitions.len());
            assert_eq!(0, partitions[0].index);
            assert_eq!(
                i16::from(ErrorCode::CoordinatorNotAvailable),
                partitions[0].error_code
            );
        }

        assert!(configuration.has_resource(RESOURCE_NAME_0));
        assert!(!configuration.has_resource(RESOURCE_NAME_1));

        assert_eq!(
            Some(ResourceConfigValue::Boolean(true)),
            configuration.get(RESOURCE_NAME_0, PARAMETER)
        );

        {
            let response = service.serve(produce_request(RESOURCE_NAME_1)?).await?;

            let responses = response.responses.unwrap_or_default();
            assert_eq!(1, responses.len());
            assert_eq!(RESOURCE_NAME_1, responses[0].name);

            let partitions = responses[0]
                .partition_responses
                .as_deref()
                .unwrap_or_default();
            assert_eq!(1, partitions.len());
            assert_eq!(0, partitions[0].index);
            assert_eq!(i16::from(ErrorCode::None), partitions[0].error_code);
        }

        assert!(configuration.has_resource(RESOURCE_NAME_0));
        assert!(configuration.has_resource(RESOURCE_NAME_1));

        assert_eq!(
            Some(ResourceConfigValue::Boolean(true)),
            configuration.get(RESOURCE_NAME_0, PARAMETER)
        );

        assert_eq!(
            Some(ResourceConfigValue::Boolean(false)),
            configuration.get(RESOURCE_NAME_1, PARAMETER)
        );

        Ok(())
    }

    fn produce_request_for(topic: &str) -> RequestInput<ProduceRequest> {
        RequestInput {
            request: ProduceRequest::default().topic_data(Some(
                [TopicProduceData::default().name(topic.into())].into(),
            )),
            extensions: Extensions::default(),
        }
    }

    #[test]
    fn alias_supplies_the_value_of_its_key() -> Result<(), Error> {
        let configuration = ResourceConfig::default();
        configuration.put("a", "tansu.batch.max_records", "7")?;

        assert_eq!(
            Some(7),
            configuration
                .get("a", topic_config::BATCH_MAX_RECORDS)
                .and_then(|value| value.as_usize())
        );

        Ok(())
    }

    #[test]
    fn key_wins_over_alias_whatever_the_order_of_arrival() -> Result<(), Error> {
        for first_alias in [true, false] {
            let configuration = ResourceConfig::default();
            let alias = ("tansu.batch.max_records", "7");
            let key = ("nisshi.batch.max_records", "9");

            for (name, value) in if first_alias {
                [alias, key]
            } else {
                [key, alias]
            } {
                configuration.put("a", name, value)?;
            }

            assert_eq!(
                Some(9),
                configuration
                    .get("a", topic_config::BATCH_MAX_RECORDS)
                    .and_then(|value| value.as_usize())
            );
        }

        Ok(())
    }

    #[test]
    fn batch_matcher_matches_the_alias_like_its_key() -> Result<(), Error> {
        for name in ["tansu.batch", "nisshi.batch"] {
            let configuration = ResourceConfig::default();
            configuration.put("a", name, "true")?;

            let matcher =
                ResourceConfigValueMatcher::new(configuration, topic_config::BATCH, "true");

            assert!(matcher.matches(None, &produce_request_for("a")), "{name}");
            assert!(!matcher.matches(None, &produce_request_for("b")), "{name}");
        }

        Ok(())
    }
}
