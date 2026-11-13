// Copyright (c) Solace Corporation
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

use clap::{Args, Subcommand};
use human_units::iec::iec_unit;
use nisshi_perf::{Perf, ReportFormat};
use nisshi_sans_io::ErrorCode;
use url::Url;

use crate::{
    EnvVarExp, Result,
    cli::{DEFAULT_BROKER, client_security::ClientSecurity, output::Output},
};

#[derive(Clone, Debug, Subcommand)]
pub(super) enum Command {
    /// Produce messages
    Produce {
        /// The partition to produce messages into
        #[arg(long, default_value = "0")]
        partition: i32,

        /// Message batch size used by every producer
        #[arg(long, default_value = "1")]
        batch_size: u32,

        /// Record size used by every producer
        #[arg(long, default_value = "1k", value_parser=clap::value_parser!(human_units::Size))]
        record_size: human_units::Size,

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
        #[arg(long, default_value = "1m", value_parser=clap::value_parser!(human_units::Duration))]
        duration: Option<human_units::Duration>,

        /// How to print the report: `table` prints a line of text for each interval and one for the
        /// run, `json` prints one JSON object on a line for each interval and a last one for the run:
        /// {"type":"interval"|"summary","elapsed_ms","records_sent","bytes_sent",
        /// "records_per_second","bytes_per_second","latency_ms":{"min","mean","max"}}. The rates
        /// are null when no time has elapsed and the latency is null until a request was
        /// answered. The latency is over the run so far, not over the interval. The first
        /// produce request that the broker refuses ends the run with exit status 4
        #[arg(long, value_enum, default_value_t = Output::Table)]
        output: Output,
    },

    Consume,
}

#[iec_unit(symbol = "B/s")]
#[derive(Copy, Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct Throughput(pub u32);

#[derive(Args, Clone, Debug)]
pub(super) struct Arg {
    #[command(subcommand)]
    command: Command,

    /// The URL of the broker
    #[arg(long, default_value = DEFAULT_BROKER, env = "ADVERTISED_LISTENER_URL")]
    broker: EnvVarExp<Url>,

    /// The topic to generate messages into
    #[clap(value_parser)]
    topic: String,

    #[command(flatten)]
    security: ClientSecurity,
}

impl Arg {
    pub(super) async fn main(self) -> Result<ErrorCode> {
        match self.command {
            Command::Produce {
                partition,
                batch_size,
                record_size,
                per_second,
                throughput,
                producers,
                duration,
                output,
            } => Perf::builder()
                .broker(self.broker.into_inner())
                .security(self.security.build()?)
                .topic(self.topic)
                .partition(partition)
                .batch_size(batch_size)
                .record_size(record_size.0 as usize)
                .per_second(per_second)
                .throughput(throughput.map(|throughput| throughput.0))
                .producers(producers)
                .duration(duration.map(|duration| duration.0))
                .report(match output {
                    Output::Table => ReportFormat::Text,
                    Output::Json => ReportFormat::Json,
                })
                .build()
                .main()
                .await
                .map_err(Into::into),

            // Not implemented: a clean failure, with an exit status that a script can read,
            // instead of a panic.
            Command::Consume => Err(crate::Error::Box(
                "perf consume is not implemented: only perf produce is available".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Arg, Command, Output};
    use clap::Parser;

    #[derive(Parser)]
    struct Wrapper {
        #[command(flatten)]
        arg: Arg,
    }

    fn output(args: &[&str]) -> Result<Output, clap::Error> {
        let wrapper = Wrapper::try_parse_from(
            ["perf", "t", "produce"]
                .into_iter()
                .chain(args.iter().copied()),
        )?;

        match wrapper.arg.command {
            Command::Produce { output, .. } => Ok(output),
            Command::Consume => panic!("expected produce"),
        }
    }

    #[test]
    fn prints_text_unless_asked_for_json() {
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
