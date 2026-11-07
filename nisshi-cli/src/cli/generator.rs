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

use std::time::Duration;

use super::{DEFAULT_BROKER, client_security::ClientSecurity, output::Output};
use crate::{EnvVarExp, Result};
use clap::Args;
use human_units::iec::iec_unit;
use nisshi_generator::{Generate, ReportFormat};
use nisshi_sans_io::ErrorCode;
use url::Url;

#[iec_unit(symbol = "B/s")]
#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Throughput(pub u32);

#[derive(Args, Clone, Debug)]
pub(super) struct Arg {
    /// The URL of the broker to produce messages into
    #[arg(long, default_value = DEFAULT_BROKER, env = "ADVERTISED_LISTENER_URL")]
    broker: EnvVarExp<Url>,

    /// The topic to generate messages into
    #[clap(value_parser)]
    topic: String,

    /// The partition to produce messages into
    #[arg(long, default_value = "0")]
    partition: i32,

    /// Schema registry examples are: file://./etc/schema or s3://nisshi/, containing: topic.json, topic.proto or topic.avsc
    #[arg(long, env = "SCHEMA_REGISTRY")]
    schema_registry: EnvVarExp<Url>,

    /// Message batch size used by every producer
    #[arg(long, default_value = "1")]
    batch_size: u32,

    /// The maximum number of messages per second
    #[clap(long, group = "rate")]
    per_second: Option<u32>,

    /// Message throughput
    #[clap(long, group = "rate")]
    throughput: Option<Throughput>,

    /// The number of producers generating messages
    #[arg(long, default_value = "1")]
    producers: u32,

    /// Stop sending messages after this time
    #[arg(long)]
    duration_seconds: Option<u64>,

    /// OTEL Exporter OTLP endpoint
    #[arg(long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    otlp_endpoint_url: Option<EnvVarExp<Url>>,

    /// What to print on stdout: `table` prints nothing, `json` prints one JSON object when the run
    /// ends: {"type":"summary","elapsed_ms","batches_produced","records_produced"}, counting the
    /// batches that the broker accepted. A refused produce request ends the run with exit status 4
    /// and the summary is printed all the same
    #[arg(long, value_enum, default_value_t = Output::Table)]
    output: Output,

    #[command(flatten)]
    security: ClientSecurity,
}

impl Arg {
    pub(super) async fn main(self) -> Result<ErrorCode> {
        let generate = Generate::builder()
            .broker(self.broker.into_inner())
            .security(self.security.build()?)
            .topic(self.topic)
            .partition(self.partition)
            .schema_registry(self.schema_registry.into_inner())
            .batch_size(self.batch_size)
            .per_second(self.per_second)
            .throughput(self.throughput.map(|throughput| throughput.0))
            .producers(self.producers)
            .duration(self.duration_seconds.map(Duration::from_secs))
            .otlp_endpoint_url(
                self.otlp_endpoint_url
                    .map(|expression| expression.into_inner()),
            )
            .report(match self.output {
                Output::Table => ReportFormat::Text,
                Output::Json => ReportFormat::Json,
            })
            .build()?;

        generate.main().await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::{Arg, Output};
    use clap::Parser;

    #[derive(Parser)]
    struct Wrapper {
        #[command(flatten)]
        arg: Arg,
    }

    fn output(args: &[&str]) -> Result<Output, clap::Error> {
        Wrapper::try_parse_from(
            ["generator", "t", "--schema-registry", "file://./etc/schema"]
                .into_iter()
                .chain(args.iter().copied()),
        )
        .map(|wrapper| wrapper.arg.output)
    }

    #[test]
    fn prints_nothing_unless_asked_for_json() {
        assert_eq!(Output::Table, output(&[]).unwrap());
        assert_eq!(Output::Json, output(&["--output", "json"]).unwrap());
        assert_eq!(
            clap::error::ErrorKind::InvalidValue,
            output(&["--output", "yaml"]).unwrap_err().kind()
        );
    }

    #[test]
    fn the_rate_options_exclude_each_other() {
        assert_eq!(
            clap::error::ErrorKind::ArgumentConflict,
            output(&["--per-second", "1", "--throughput", "1B/s"])
                .unwrap_err()
                .kind()
        );
    }
}
