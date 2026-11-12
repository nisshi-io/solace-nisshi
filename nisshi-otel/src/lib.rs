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

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
use std::{
    env,
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use opentelemetry::{Key, KeyValue, Value, global};
use opentelemetry_otlp::{ExporterBuildError, Protocol, WithExportConfig as _};
use opentelemetry_sdk::{
    Resource,
    metrics::SdkMeterProvider,
    resource::{EnvResourceDetector, ResourceDetector as _},
};
use opentelemetry_semantic_conventions::resource::SERVICE_NAME;
use tracing::{debug, info, warn};
use url::{ParseError, Url};

#[derive(Clone, Debug, thiserror::Error)]
pub enum Error {
    ExporterBuild(Arc<ExporterBuildError>),
    Parse(#[from] ParseError),
}

impl From<ExporterBuildError> for Error {
    fn from(value: ExporterBuildError) -> Self {
        Self::ExporterBuild(Arc::new(value))
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

const OTEL_SERVICE_NAME: &str = "OTEL_SERVICE_NAME";
const OTEL_RESOURCE_ATTRIBUTES: &str = "OTEL_RESOURCE_ATTRIBUTES";

/// Resource describing this process, attached to exported telemetry.
///
/// Attributes come from `OTEL_RESOURCE_ATTRIBUTES` (via the SDK's
/// `EnvResourceDetector`). `service.name` is, in order: `OTEL_SERVICE_NAME`,
/// `service.name` in `OTEL_RESOURCE_ATTRIBUTES`, then `fallback_service_name`.
/// Unset, empty or whitespace-only values fall through to the next source.
///
/// The precedence is resolved by hand rather than through `Resource::builder()`
/// because `Resource::merge` lets an empty `OTEL_SERVICE_NAME` win over the
/// other sources, which would silently produce an empty `service.name`.
pub fn resource(fallback_service_name: impl Into<Value>) -> Resource {
    warn_on_malformed_resource_attributes();

    let from_env = EnvResourceDetector::new().detect();

    let service_name = env::var(OTEL_SERVICE_NAME)
        .ok()
        .map(|name| name.trim().to_owned())
        .filter(|name| !name.is_empty())
        .map(Value::from)
        .or_else(|| {
            from_env
                .get(&Key::new(SERVICE_NAME))
                .filter(|name| !name.as_str().is_empty())
        })
        .unwrap_or_else(|| fallback_service_name.into());

    Resource::builder_empty()
        .with_attributes(
            from_env
                .iter()
                .map(|(key, value)| KeyValue::new(key.clone(), value.clone())),
        )
        .with_service_name(service_name)
        .build()
}

/// The SDK silently discards `OTEL_RESOURCE_ATTRIBUTES` entries without a
/// `key=value` shape, so name them here instead of leaving an operator to
/// discover a missing attribute in their backend.
fn warn_on_malformed_resource_attributes() {
    if let Ok(attributes) = env::var(OTEL_RESOURCE_ATTRIBUTES) {
        attributes
            .split_terminator(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty() && !entry.contains('='))
            .for_each(|entry| {
                warn!(
                    %entry,
                    "ignoring {OTEL_RESOURCE_ATTRIBUTES} entry without a '=' separator"
                )
            });
    }
}

/// The OTLP/HTTP metrics endpoint: `otlp_endpoint_url` with `v1/metrics`
/// appended to its path.
///
/// `Url::join` replaces the last path segment when the base has no trailing
/// slash (`http://collector/otlp` would become `http://collector/v1/metrics`),
/// so the slash is added first and the path is always appended.
pub fn metrics_endpoint(otlp_endpoint_url: &Url) -> Result<Url> {
    let mut base = otlp_endpoint_url.clone();

    if !base.path().ends_with('/') {
        let path = format!("{}/", base.path());
        base.set_path(&path);
    }

    base.join("v1/metrics").map_err(Into::into)
}

fn without_credentials(url: &Url) -> Url {
    let mut url = url.clone();
    _ = url.set_username("");
    _ = url.set_password(None);
    url
}

pub fn meter_provider(
    otlp_endpoint_url: Url,
    fallback_service_name: impl Into<Value>,
) -> Result<SdkMeterProvider> {
    let resource = resource(fallback_service_name);
    debug!(?resource);

    let endpoint = metrics_endpoint(&otlp_endpoint_url)?;

    info!(
        endpoint = %without_credentials(&endpoint),
        service_name = ?resource.get(&Key::new(SERVICE_NAME)),
        attributes = ?resource.iter().map(|(key, _)| key.as_str()).collect::<Vec<_>>(),
        "exporting OTLP metrics"
    );

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(endpoint.to_string())
        .build()?;

    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter)
        .with_resource(resource)
        .build();

    global::set_meter_provider(meter_provider.clone());

    Ok(meter_provider)
}

#[cfg(test)]
mod tests {
    use opentelemetry::Key;
    use temp_env::{with_vars, with_vars_unset};

    use super::*;

    const FALLBACK: &str = "fallback-svc";

    fn service_name(resource: &Resource) -> Option<String> {
        resource
            .get(&Key::new(SERVICE_NAME))
            .map(|value| value.to_string())
    }

    fn attribute(resource: &Resource, key: &'static str) -> Option<String> {
        resource.get(&Key::new(key)).map(|value| value.to_string())
    }

    #[test]
    fn falls_back_when_env_unset() {
        with_vars_unset([OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES], || {
            let resource = resource(FALLBACK);
            assert_eq!(Some(FALLBACK.to_owned()), service_name(&resource));
        });
    }

    #[test]
    fn otel_service_name_is_trimmed() {
        with_vars(
            [
                (OTEL_SERVICE_NAME, Some(" svc-a\n")),
                (OTEL_RESOURCE_ATTRIBUTES, None),
            ],
            || {
                let resource = resource(FALLBACK);
                assert_eq!(Some("svc-a".to_owned()), service_name(&resource));
            },
        );
    }

    #[test]
    fn whitespace_only_otel_service_name_is_treated_as_unset() {
        with_vars(
            [
                (OTEL_SERVICE_NAME, Some(" \n")),
                (OTEL_RESOURCE_ATTRIBUTES, None),
            ],
            || {
                let resource = resource(FALLBACK);
                assert_eq!(Some(FALLBACK.to_owned()), service_name(&resource));
            },
        );
    }

    #[test]
    fn malformed_resource_attribute_entries_are_ignored() {
        with_vars(
            [
                (OTEL_SERVICE_NAME, None),
                (
                    OTEL_RESOURCE_ATTRIBUTES,
                    Some("service.version=1,oops,deployment.environment.name staging"),
                ),
            ],
            || {
                let resource = resource(FALLBACK);
                assert_eq!(Some(FALLBACK.to_owned()), service_name(&resource));
                assert_eq!(
                    Some("1".to_owned()),
                    attribute(&resource, "service.version")
                );
                assert_eq!(None, attribute(&resource, "deployment.environment.name"));
                assert_eq!(2, resource.len());
            },
        );
    }

    #[test]
    fn metrics_endpoint_appends_v1_metrics() {
        for (base, expected) in [
            ("http://collector:4318", "http://collector:4318/v1/metrics"),
            ("http://collector:4318/", "http://collector:4318/v1/metrics"),
            (
                "http://collector:4318/otlp",
                "http://collector:4318/otlp/v1/metrics",
            ),
            (
                "http://collector:4318/otlp/",
                "http://collector:4318/otlp/v1/metrics",
            ),
            (
                "http://localhost:9090/api/v1/otlp",
                "http://localhost:9090/api/v1/otlp/v1/metrics",
            ),
        ] {
            let base = Url::parse(base).unwrap();
            assert_eq!(
                expected,
                metrics_endpoint(&base).unwrap().as_str(),
                "base {base}"
            );
        }
    }

    #[test]
    fn otel_service_name_sets_service_name() {
        with_vars(
            [
                (OTEL_SERVICE_NAME, Some("svc-a")),
                (OTEL_RESOURCE_ATTRIBUTES, None),
            ],
            || {
                let resource = resource(FALLBACK);
                assert_eq!(Some("svc-a".to_owned()), service_name(&resource));
            },
        );
    }

    #[test]
    fn resource_attributes_set_service_name_and_extra_attributes() {
        with_vars(
            [
                (OTEL_SERVICE_NAME, None),
                (
                    OTEL_RESOURCE_ATTRIBUTES,
                    Some("service.name=svc-b,service.version=1.2.3"),
                ),
            ],
            || {
                let resource = resource(FALLBACK);
                assert_eq!(Some("svc-b".to_owned()), service_name(&resource));
                assert_eq!(
                    Some("1.2.3".to_owned()),
                    attribute(&resource, "service.version")
                );
            },
        );
    }

    #[test]
    fn otel_service_name_wins_over_resource_attributes() {
        with_vars(
            [
                (OTEL_SERVICE_NAME, Some("svc-a")),
                (
                    OTEL_RESOURCE_ATTRIBUTES,
                    Some("service.name=svc-b,deployment.environment.name=staging"),
                ),
            ],
            || {
                let resource = resource(FALLBACK);
                assert_eq!(Some("svc-a".to_owned()), service_name(&resource));
                assert_eq!(
                    Some("staging".to_owned()),
                    attribute(&resource, "deployment.environment.name")
                );
            },
        );
    }

    #[test]
    fn empty_otel_service_name_is_treated_as_unset() {
        with_vars(
            [
                (OTEL_SERVICE_NAME, Some("")),
                (OTEL_RESOURCE_ATTRIBUTES, Some("service.name=svc-b")),
            ],
            || {
                let resource = resource(FALLBACK);
                assert_eq!(Some("svc-b".to_owned()), service_name(&resource));
            },
        );
    }

    #[test]
    fn resource_attributes_without_service_name_keep_fallback() {
        with_vars(
            [
                (OTEL_SERVICE_NAME, None),
                (OTEL_RESOURCE_ATTRIBUTES, Some("service.version=9")),
            ],
            || {
                let resource = resource(FALLBACK);
                assert_eq!(Some(FALLBACK.to_owned()), service_name(&resource));
                assert_eq!(
                    Some("9".to_owned()),
                    attribute(&resource, "service.version")
                );
            },
        );
    }

    #[test]
    fn empty_service_name_attribute_is_treated_as_unset() {
        with_vars(
            [
                (OTEL_SERVICE_NAME, None),
                (OTEL_RESOURCE_ATTRIBUTES, Some("service.name=")),
            ],
            || {
                let resource = resource(FALLBACK);
                assert_eq!(Some(FALLBACK.to_owned()), service_name(&resource));
            },
        );
    }
}
