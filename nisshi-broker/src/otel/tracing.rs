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

use crate::{Result, TracingFormat};
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::{Level, Subscriber};
use tracing_subscriber::{
    EnvFilter, Layer,
    filter::{FilterExt, LevelFilter, filter_fn},
    fmt::format::FmtSpan,
    layer::{Filter, SubscriberExt},
    util::SubscriberInitExt,
};

#[derive(Debug)]
pub(super) struct Guard {
    tracer: Option<SdkTracerProvider>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(tracer) = self.tracer.as_ref()
            && let Err(err) = tracer.shutdown()
        {
            eprintln!("{err:?}")
        }
    }
}

/// The log filter, built from the `RUST_LOG` environment variable.
fn log_filter<S: Subscriber>() -> Result<impl Filter<S> + use<S>> {
    log_filter_from(std::env::var("RUST_LOG").ok().as_deref())
}

/// The log filter for a `RUST_LOG` value, `None` when it is unset.
///
/// The level is `warn` when `rust_log` is unset or empty, so warnings such
/// as deprecation notices show on a default run; any directives in `rust_log`
/// replace that default. Malformed directives are ignored, as they always
/// have been.
///
/// `reqsign_core` and `turso_core` events above `info` are dropped whatever
/// `rust_log` says.
/// reqsign-core 3.3.2 logs each credential provider it tries at `debug`, and
/// reqsign-aws-core 3.2.0's `StaticCredentialProvider` derives `Debug` over
/// `secret_access_key` and `session_token` without redaction, so
/// `RUST_LOG=debug` (routine when troubleshooting, and what CI's test job
/// uses) would print AWS secrets in cleartext through this subscriber's `log`
/// compatibility layer. The cap is a second filter beside the `EnvFilter`
/// instead of a directive in it, because a directive sets a level, and would
/// raise `reqsign_core` to `info` under `warn,nisshi_broker=debug` or
/// `error,foo=debug`.
///
/// `turso_core` is capped the same way. turso_core 0.8.3 logs the B-tree cell
/// that it deletes or balances at `debug`, with the cell's payload, which is
/// the stored record's key and value, so `RUST_LOG=debug` on a `turso://`
/// broker would write every record that the broker deletes to the log.
pub fn log_filter_from<S: Subscriber>(rust_log: Option<&str>) -> Result<impl Filter<S> + use<S>> {
    let env = EnvFilter::builder()
        .with_default_directive(LevelFilter::WARN.into())
        .parse_lossy(rust_log.unwrap_or_default());

    // A `filter_fn` has no max level hint, and `And` takes the lesser of its
    // two hints, so without one here the combined filter reports none. `init`
    // passes that hint to `LogTracer` as the `log` crate's max level, and
    // `log` records would then all reach the `EnvFilter`. TRACE tells `And`
    // that this filter limits nothing by level, so the `EnvFilter`'s hint wins.
    let dependency_cap = filter_fn(|metadata| {
        !(is_capped_dependency(metadata.target()) && *metadata.level() > Level::INFO)
    })
    .with_max_level_hint(LevelFilter::TRACE);

    Ok(env.and(dependency_cap))
}

/// Whether `target` is the `reqsign_core` or `turso_core` crate, or one of
/// their modules.
fn is_capped_dependency(target: &str) -> bool {
    ["reqsign_core", "turso_core"].iter().any(|krate| {
        target == *krate
            || target
                .strip_prefix(krate)
                .is_some_and(|rest| rest.starts_with("::"))
    })
}

pub(super) fn init_tracing_subscriber(tracing_format: TracingFormat) -> Result<Guard> {
    // let provider = init_tracer_provider()?;

    // let tracer = provider.tracer(format!("{}-otel-subscriber", env!("CARGO_PKG_NAME")));

    match tracing_format {
        TracingFormat::Text => tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_level(true)
                    .with_line_number(true)
                    .with_thread_ids(false)
                    .with_span_events(FmtSpan::FULL)
                    .with_filter(log_filter()?),
            )
            // .with(OpenTelemetryLayer::new(tracer))
            .init(),

        TracingFormat::Json => tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_filter(log_filter()?),
            )
            // .with(OpenTelemetryLayer::new(tracer))
            .init(),
    }

    Ok(Guard { tracer: None })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_matches;
    use tracing::enabled;

    /// Runs `check` under a thread-local subscriber filtered by `rust_log`, so
    /// the process environment is never read or changed.
    fn with_filter<T>(rust_log: Option<&str>, check: impl FnOnce() -> T) -> Result<T> {
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::sink)
                .with_filter(log_filter_from(rust_log)?),
        );
        Ok(tracing::subscriber::with_default(subscriber, check))
    }

    #[test]
    fn unset_rust_log_enables_warn_but_not_info() -> Result<()> {
        let levels = with_filter(None, || (enabled!(Level::WARN), enabled!(Level::INFO)))?;
        assert_matches!(levels, (true, false));
        Ok(())
    }

    #[test]
    fn empty_rust_log_enables_warn_but_not_info() -> Result<()> {
        let levels = with_filter(Some(""), || (enabled!(Level::WARN), enabled!(Level::INFO)))?;
        assert_matches!(levels, (true, false));
        Ok(())
    }

    #[test]
    fn explicit_rust_log_replaces_the_default() -> Result<()> {
        let debug = with_filter(Some("debug"), || enabled!(Level::DEBUG))?;
        assert_matches!(debug, true);

        let levels = with_filter(Some("error"), || {
            (enabled!(Level::ERROR), enabled!(Level::WARN))
        })?;
        assert_matches!(levels, (true, false));
        Ok(())
    }

    #[test]
    fn malformed_rust_log_is_ignored() -> Result<()> {
        let levels = with_filter(Some("=nonsense=["), || {
            (enabled!(Level::WARN), enabled!(Level::INFO))
        })?;
        assert_matches!(levels, (true, false));
        Ok(())
    }

    #[test]
    fn reqsign_core_stays_capped_at_info_under_debug() -> Result<()> {
        let levels = with_filter(Some("debug"), || {
            (
                enabled!(target: "reqsign_core", Level::INFO),
                enabled!(target: "reqsign_core", Level::DEBUG),
                enabled!(target: "other_crate", Level::DEBUG),
            )
        })?;
        assert_matches!(levels, (true, false, true));
        Ok(())
    }

    #[test]
    fn reqsign_core_is_capped_at_info_under_mixed_filters() -> Result<()> {
        for rust_log in [
            "warn,nisshi_broker=debug",
            "error,foo=debug",
            "info,reqsign_core=debug",
            "warn,reqsign_core=trace",
            "trace",
        ] {
            let debug = with_filter(
                Some(rust_log),
                || enabled!(target: "reqsign_core", Level::DEBUG),
            )?;
            assert_matches!(debug, false, "{rust_log}");
        }
        Ok(())
    }

    #[test]
    fn reqsign_core_keeps_the_level_the_filter_gives_it() -> Result<()> {
        let levels = with_filter(Some("warn,nisshi_broker=debug"), || {
            (
                enabled!(target: "reqsign_core", Level::WARN),
                enabled!(target: "reqsign_core", Level::INFO),
                enabled!(target: "reqsign_core::sign", Level::DEBUG),
                enabled!(target: "reqsign_core_other", Level::INFO),
                enabled!(target: "nisshi_broker", Level::DEBUG),
            )
        })?;
        assert_matches!(levels, (true, false, false, false, true));
        Ok(())
    }

    #[test]
    fn turso_core_stays_capped_at_info_under_debug() -> Result<()> {
        let levels = with_filter(Some("debug"), || {
            (
                enabled!(target: "turso_core", Level::INFO),
                enabled!(target: "turso_core::storage::btree", Level::DEBUG),
                enabled!(target: "turso_core_other", Level::DEBUG),
            )
        })?;
        assert_matches!(levels, (true, false, true));
        Ok(())
    }

    #[test]
    fn turso_core_is_not_raised_to_info_by_a_quiet_filter() -> Result<()> {
        let info = with_filter(
            Some("error"),
            || enabled!(target: "turso_core", Level::INFO),
        )?;
        assert_matches!(info, false);
        Ok(())
    }

    #[test]
    fn reqsign_core_is_not_raised_to_info_by_a_quiet_filter() -> Result<()> {
        for rust_log in [None, Some(""), Some("warn"), Some("error")] {
            let info = with_filter(rust_log, || enabled!(target: "reqsign_core", Level::INFO))?;
            assert_matches!(info, false);
        }
        Ok(())
    }

    #[test]
    fn max_level_hint_follows_the_env_filter() -> Result<()> {
        for (rust_log, expected) in [
            (None, LevelFilter::WARN),
            (Some("debug"), LevelFilter::DEBUG),
            (Some("warn,nisshi_broker=debug"), LevelFilter::DEBUG),
        ] {
            let subscriber = tracing_subscriber::registry().with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::sink)
                    .with_filter(log_filter_from(rust_log)?),
            );
            assert_eq!(
                Subscriber::max_level_hint(&subscriber),
                Some(expected),
                "{rust_log:?}"
            );
        }
        Ok(())
    }
}
