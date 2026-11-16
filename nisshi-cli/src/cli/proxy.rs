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

use crate::EnvVarExp;

use super::DEFAULT_BROKER;
use clap::Args;
use url::Url;

#[derive(Args, Clone, Debug)]
pub(super) struct Arg {
    /// The proxy will listen on this address
    #[arg(
        hide_env_values = true,
        long,
        env = "LISTENER_URL",
        default_value = "tcp://0.0.0.0:9092"
    )]
    pub(super) listener_url: EnvVarExp<Url>,

    /// This location is advertised to clients in metadata
    #[arg(
        hide_env_values = true,
        long,
        env = "ADVERTISED_LISTENER_URL",
        default_value = DEFAULT_BROKER,
    )]
    pub(super) advertised_listener_url: EnvVarExp<Url>,

    /// The proxy will forward traffic to this origin broker, which must not require authentication
    #[arg(hide_env_values = true, long, env = "ORIGIN_URL", default_value = DEFAULT_BROKER)]
    pub(super) origin_url: EnvVarExp<Url>,

    /// OTEL Exporter OTLP endpoint
    #[arg(hide_env_values = true, long, env = "OTEL_EXPORTER_OTLP_ENDPOINT")]
    pub(super) otlp_endpoint_url: Option<EnvVarExp<Url>>,
}

#[cfg(test)]
mod tests {
    use super::Arg;
    use clap::Parser;

    #[derive(Parser)]
    struct Wrapper {
        #[command(flatten)]
        inner: Arg,
    }

    fn parse() -> Arg {
        temp_env::with_vars_unset(
            ["LISTENER_URL", "ADVERTISED_LISTENER_URL", "ORIGIN_URL"],
            || {
                Wrapper::try_parse_from(["proxy"])
                    .expect("defaults parse")
                    .inner
            },
        )
    }

    /// The proxy's advertised listener and origin share `DEFAULT_BROKER` with the
    /// broker's own advertised listener, so they resolve to the same IPv4 loopback
    /// address, not `localhost`.
    #[test]
    fn advertised_defaults_resolve_to_loopback() {
        let arg = parse();

        assert_eq!(
            Some("127.0.0.1"),
            arg.advertised_listener_url.into_inner().host_str(),
        );

        assert_eq!(Some("127.0.0.1"), arg.origin_url.into_inner().host_str(),);
    }

    #[test]
    fn listener_default_stays_ipv4_unspecified() {
        let arg = parse();

        assert_eq!(Some("0.0.0.0"), arg.listener_url.into_inner().host_str(),);
    }
}
