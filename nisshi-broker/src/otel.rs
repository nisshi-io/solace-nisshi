// Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
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

use opentelemetry_sdk::metrics::SdkMeterProvider;
use url::Url;

use crate::{Result, TracingFormat};

mod tracing;

pub use tracing::log_filter_from;

#[derive(Debug)]
pub struct Guard {
    #[allow(dead_code)]
    tracer: tracing::Guard,
}

pub fn init(tracing_format: TracingFormat) -> Result<Guard> {
    tracing::init_tracing_subscriber(tracing_format).map(|tracer| Guard { tracer })
}

/// Installs the global OTLP meter provider and returns it so the caller can
/// flush and shut it down on exit; the global keeps it alive meanwhile.
pub fn metric_exporter(endpoint: Url) -> Result<SdkMeterProvider> {
    nisshi_otel::meter_provider(endpoint, env!("CARGO_PKG_NAME")).map_err(Into::into)
}
