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

//! The clap value parser for [`EnvVarExp`].

use std::{ffi::OsStr, marker::PhantomData, str::FromStr};

use clap::{
    Arg, Command,
    builder::{TypedValueParser, ValueParserFactory},
    error::ErrorKind,
};

use url::Url;

use crate::{EnvVarExp, Error};

/// Parses an option value as [`EnvVarExp`], and reports a failure without the value. The reason
/// is the `Display` of `T`'s parse error, so `T` must not put its input in that error. `Url`'s
/// error does not.
///
/// clap's own `FromStr` parser prints `invalid value '<raw value>' for '<option>'`. A URL option
/// can hold a password, and that message goes to stderr and on to the container log, and
/// `hide_env_values` hides a value only in `--help`. This parser names the option and the reason
/// and leaves the value out.
#[derive(Clone, Debug)]
pub struct EnvVarExpParser<T>(PhantomData<fn() -> T>);

impl<T> TypedValueParser for EnvVarExpParser<T>
where
    T: FromStr + Clone + Send + Sync + 'static,
    Error: From<<T as FromStr>::Err>,
{
    type Value = EnvVarExp<T>;

    fn parse_ref(
        &self,
        cmd: &Command,
        arg: Option<&Arg>,
        value: &OsStr,
    ) -> Result<Self::Value, clap::Error> {
        match value.to_str().map(EnvVarExp::<T>::from_str) {
            Some(Ok(parsed)) => Ok(parsed),
            Some(Err(err)) => Err(rejection(cmd, arg, &err.to_string())),
            None => Err(rejection(cmd, arg, NOT_UNICODE)),
        }
    }
}

const NOT_UNICODE: &str = "the value is not valid Unicode";

/// The error for a value that does not parse. It names the option and the reason, and leaves
/// the value out.
fn rejection(cmd: &Command, arg: Option<&Arg>, reason: &str) -> clap::Error {
    let option = arg.map_or_else(|| "an argument".to_owned(), |arg| format!("'{arg}'"));

    clap::Error::raw(
        ErrorKind::ValueValidation,
        format!("invalid value for {option}: {reason}\n"),
    )
    .with_cmd(cmd)
}

/// Parses a [`Url`] option that does not expand `${NAME}`, such as `--broker`, and reports a
/// failure as [`EnvVarExpParser`] does: without the value.
#[derive(Clone, Copy, Debug)]
pub(crate) struct UrlParser;

impl TypedValueParser for UrlParser {
    type Value = Url;

    fn parse_ref(
        &self,
        cmd: &Command,
        arg: Option<&Arg>,
        value: &OsStr,
    ) -> Result<Self::Value, clap::Error> {
        match value.to_str().map(Url::from_str) {
            Some(Ok(url)) => Ok(url),
            Some(Err(err)) => Err(rejection(cmd, arg, &Error::from(err).to_string())),
            None => Err(rejection(cmd, arg, NOT_UNICODE)),
        }
    }
}

impl<T> ValueParserFactory for EnvVarExp<T>
where
    T: FromStr + Clone + Send + Sync + 'static,
    Error: From<<T as FromStr>::Err>,
{
    type Parser = EnvVarExpParser<T>;

    fn value_parser() -> Self::Parser {
        EnvVarExpParser(PhantomData)
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser as _;

    use crate::Cli;

    const SECRET: &str = "s3cr3t-hunter2";

    /// A URL with a password and a typo: a space in the host does not parse.
    fn typo() -> String {
        format!("postgres://user:{SECRET}@ho st/db")
    }

    fn parse_error(args: &[&str]) -> String {
        Cli::try_parse_from(args).unwrap_err().to_string()
    }

    fn assert_names_option_and_hides_secret(err: &str, option: &str) {
        assert!(err.contains(option), "{err}");
        assert!(
            err.contains("invalid URL: invalid domain character"),
            "{err}"
        );
        assert!(!err.contains(SECRET), "{err}");
        assert!(!err.contains("ho st"), "{err}");
    }

    #[test]
    fn a_url_flag_that_does_not_parse_keeps_its_value_out_of_the_error() {
        let typo = typo();
        for (flag, args) in [
            ("--storage-engine", ["nisshi", "broker", "--storage-engine"]),
            (
                "--schema-registry",
                ["nisshi", "broker", "--schema-registry"],
            ),
            ("--listener-url", ["nisshi", "broker", "--listener-url"]),
        ] {
            let err = parse_error(&[&args[..], &[&typo]].concat());

            assert_names_option_and_hides_secret(&err, flag);
        }
    }

    #[test]
    fn a_url_flag_with_an_equals_sign_keeps_its_value_out_of_the_error() {
        let err = parse_error(&["nisshi", "broker", &format!("--storage-engine={}", typo())]);

        assert_names_option_and_hides_secret(&err, "--storage-engine");
    }

    #[test]
    fn a_broker_flag_that_does_not_parse_keeps_its_value_out_of_the_error() {
        let typo = typo();
        for command in ["topic list", "user delete alice", "cat consume taxi"] {
            let mut args = vec!["nisshi"];
            args.extend(command.split(' '));
            args.extend(["--broker", &typo]);

            assert_names_option_and_hides_secret(&parse_error(&args), "--broker");
        }
    }
}
