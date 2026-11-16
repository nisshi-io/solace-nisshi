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

use clap::{Parser as _, error::ErrorKind};
use url::Url;

use super::*;

/// The name of a variable that no machine sets.
const UNSET: &str = "NISSHI_TEST_UNSET_VARIABLE";

fn vars(pairs: &[(&str, &str)]) -> VarRep {
    VarRep::from(
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect::<HashMap<_, _>>(),
    )
}

fn unset_name(result: Result<String>) -> String {
    match result {
        Err(Error::UnsetVariable(name)) => name,
        otherwise => panic!("expected an unset variable, got {otherwise:?}"),
    }
}

#[test]
fn every_reference_in_a_value_expands() {
    let vars = vars(&[("HOST", "broker"), ("PORT", "9092")]);

    assert_eq!(
        "tcp://broker:9092",
        vars.replace("tcp://${HOST}:${PORT}").unwrap()
    );
}

#[test]
fn adjacent_references_and_references_at_the_ends_expand() {
    let vars = vars(&[("A", "x"), ("B", "y")]);

    assert_eq!("xy", vars.replace("${A}${B}").unwrap());
    assert_eq!("x/mid/y", vars.replace("${A}/mid/${B}").unwrap());
    assert_eq!("xx", vars.replace("${A}${A}").unwrap());
}

#[test]
fn an_unset_variable_is_an_error_that_names_it() {
    let error = vars(&[]).replace("tcp://${POD_IP}:9092").unwrap_err();

    assert_matches!(&error, Error::UnsetVariable(name) if name == "POD_IP");
    assert_eq!(
        "environment variable POD_IP is not set: set it, or remove ${POD_IP} from the value",
        error.to_string()
    );
}

#[test]
fn an_unset_second_reference_is_found() {
    assert_eq!(
        "PORT",
        unset_name(vars(&[("HOST", "broker")]).replace("tcp://${HOST}:${PORT}"))
    );
}

#[test]
fn a_value_without_a_reference_is_unchanged() {
    let vars = vars(&[]);

    for value in [
        "tcp://localhost:9092",
        "",
        "$HOME",
        "$",
        "$$",
        "${}",
        "${unclosed",
        "}${",
    ] {
        assert_eq!(value, vars.replace(value).unwrap(), "{value}");
    }
}

#[test]
fn a_variable_set_to_an_empty_string_expands_to_nothing() {
    assert_eq!(
        "tcp://:9092",
        vars(&[("HOST", "")]).replace("tcp://${HOST}:9092").unwrap()
    );
}

#[test]
fn a_replacement_is_not_expanded_again() {
    let vars = vars(&[("A", "${B}"), ("B", "x")]);

    assert_eq!("${B}", vars.replace("${A}").unwrap());
}

#[test]
fn text_around_a_reference_keeps_its_non_ascii_characters() {
    let vars = vars(&[("A", "ü")]);

    assert_eq!("café-ü-日本", vars.replace("café-${A}-日本").unwrap());
}

#[test]
fn a_default_in_a_reference_is_part_of_the_name() {
    assert_eq!(
        "PORT:-9092",
        unset_name(vars(&[("PORT", "1")]).replace("${PORT:-9092}"))
    );
}

#[test]
fn a_reference_inside_a_reference_is_not_nested() {
    assert_eq!(
        "A${B",
        unset_name(vars(&[("A", "x"), ("B", "y")]).replace("${A${B}}"))
    );
}

#[test]
fn a_url_option_expands_every_reference_from_the_environment() {
    temp_env::with_vars(
        [
            ("NISSHI_TEST_HOST", Some("broker")),
            ("NISSHI_TEST_PORT", Some("9092")),
        ],
        || {
            let url = EnvVarExp::<Url>::from_str("tcp://${NISSHI_TEST_HOST}:${NISSHI_TEST_PORT}");

            assert_eq!("tcp://broker:9092", url.unwrap().into_inner().as_str());
        },
    );
}

#[test]
fn clap_rejects_an_option_that_refers_to_an_unset_variable() {
    temp_env::with_vars_unset([UNSET], || {
        let error = Cli::try_parse_from([
            "nisshi",
            "broker",
            "--advertised-listener-url",
            &format!("tcp://${{{UNSET}}}:9092"),
        ])
        .unwrap_err();

        assert_eq!(ErrorKind::ValueValidation, error.kind());
        assert_eq!(2, error.exit_code());
        let message = error.to_string();
        assert!(message.contains("--advertised-listener-url"), "{message}");
        assert!(
            message.contains(&format!(
                "environment variable {UNSET} is not set: set it, or remove ${{{UNSET}}} from the value"
            )),
            "{message}"
        );
    });
}
