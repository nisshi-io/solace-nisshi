// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

//! Names the cargo feature that a subcommand needs, when this binary was built without it.
//!
//! We rewrite clap's parse error instead of declaring a hidden stub for each subcommand
//! that was not built. A stub would be dead code under `--all-features`, so CI would never
//! compile or test it.

use clap::{
    Command,
    error::{ContextKind, ContextValue, Error, ErrorKind},
};

/// A subcommand that exists only when this binary is built with a cargo feature.
struct GatedSubcommand {
    subcommand: &'static str,
    feature: &'static str,
}

const GATED_SUBCOMMANDS: [GatedSubcommand; 3] = [
    GatedSubcommand {
        subcommand: "iceberg",
        feature: "iceberg",
    },
    GatedSubcommand {
        subcommand: "delta",
        feature: "delta",
    },
    GatedSubcommand {
        subcommand: "parquet",
        feature: "parquet",
    },
];

/// Returns the features of [`GATED_SUBCOMMANDS`] that this binary was built with.
pub(super) fn built_features() -> Vec<&'static str> {
    vec![
        #[cfg(feature = "iceberg")]
        "iceberg",
        #[cfg(feature = "delta")]
        "delta",
        #[cfg(feature = "parquet")]
        "parquet",
    ]
}

/// Returns the gated subcommand that `error` reports as unknown, when its feature is not in
/// `built`.
fn unbuilt_subcommand(error: &Error, built: &[&str]) -> Option<&'static GatedSubcommand> {
    // clap reports `nisshi broker iceberg` as an unknown argument, and `nisshi iceberg` as
    // an unknown subcommand, because the broker's arguments are also flattened into `nisshi`.
    let context = match error.kind() {
        ErrorKind::UnknownArgument => ContextKind::InvalidArg,
        ErrorKind::InvalidSubcommand => ContextKind::InvalidSubcommand,
        _ => return None,
    };

    let Some(ContextValue::String(token)) = error.get(context) else {
        return None;
    };

    GATED_SUBCOMMANDS
        .iter()
        .find(|gated| gated.subcommand == token && !built.contains(&gated.feature))
}

/// Returns an error that names the missing cargo feature when `error` reports a gated
/// subcommand that was not built, and `error` unchanged otherwise.
pub(super) fn explain(error: Error, built: &[&str], command: &mut Command) -> Error {
    let Some(GatedSubcommand {
        subcommand,
        feature,
    }) = unbuilt_subcommand(&error, built)
    else {
        return error;
    };

    command.error(
        ErrorKind::InvalidSubcommand,
        format!(
            "'{subcommand}' needs the `{feature}` cargo feature, and this nisshi was built without it\n\n  \
             tip: cargo install nisshi --features {feature}"
        ),
    )
}

#[cfg(test)]
mod tests {
    use clap::{Args, CommandFactory, Parser, Subcommand};

    use super::*;

    /// A parser shaped like `nisshi` built without a lake feature: the broker's arguments
    /// are both flattened at the top level and under a `broker` subcommand.
    #[derive(Debug, Parser)]
    #[command(name = "nisshi", args_conflicts_with_subcommands = true)]
    struct WithoutLakes {
        #[command(subcommand)]
        command: Option<Top>,

        #[command(flatten)]
        broker: Broker,
    }

    #[derive(Debug, Subcommand)]
    enum Top {
        Broker(Broker),
    }

    #[derive(Args, Debug)]
    struct Broker {
        #[arg(long)]
        cluster_id: Option<String>,
    }

    fn parse_error(args: &[&str]) -> Error {
        WithoutLakes::try_parse_from(args).expect_err("parser has no lake subcommand")
    }

    fn unbuilt_feature(error: &Error, built: &[&str]) -> Option<&'static str> {
        unbuilt_subcommand(error, built).map(|gated| gated.feature)
    }

    fn explained(args: &[&str], built: &[&str]) -> String {
        explain(parse_error(args), built, &mut WithoutLakes::command()).to_string()
    }

    #[test]
    fn unbuilt_lake_subcommand_names_its_feature() {
        for gated in GATED_SUBCOMMANDS {
            let args = ["nisshi", "broker", gated.subcommand, "--location", "x"];

            assert_eq!(
                Some(gated.feature),
                unbuilt_feature(&parse_error(&args), &[])
            );

            let message = explained(&args, &[]);
            assert!(message.contains("built without"), "{message}");
            assert!(
                message.contains(&format!(
                    "cargo install nisshi --features {}",
                    gated.feature
                )),
                "{message}"
            );
        }
    }

    #[test]
    fn top_level_lake_subcommand_names_its_feature() {
        for gated in GATED_SUBCOMMANDS {
            let args = ["nisshi", gated.subcommand, "--location", "x"];

            assert_eq!(
                Some(gated.feature),
                unbuilt_feature(&parse_error(&args), &[])
            );
            assert!(
                explained(&args, &[]).contains(&format!("--features {}", gated.feature)),
                "{}",
                explained(&args, &[])
            );
        }
    }

    #[test]
    fn built_lake_subcommand_is_not_rewritten() {
        let args = ["nisshi", "broker", "iceberg", "--location", "x"];

        assert_eq!(None, unbuilt_feature(&parse_error(&args), &["iceberg"]));
        assert_eq!(
            parse_error(&args).to_string(),
            explained(&args, &["iceberg"])
        );
    }

    #[test]
    fn unrelated_unknown_argument_is_not_rewritten() {
        for args in [
            &["nisshi", "frobnicate"][..],
            &["nisshi", "broker", "frobnicate"],
        ] {
            assert_eq!(None, unbuilt_feature(&parse_error(args), &[]));
            assert_eq!(parse_error(args).to_string(), explained(args, &[]));
        }
    }
}
