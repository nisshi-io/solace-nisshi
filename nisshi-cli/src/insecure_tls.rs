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

//! Environment variables that turn off certificate checks on outbound connections.

use std::ffi::OsString;

/// The variables that object_store's and deltalake-aws's `from_env` read as
/// `allow_invalid_certificates`, in lower case. deltalake-aws lower-cases the
/// name of every environment variable, so a bare `ALLOW_INVALID_CERTIFICATES`
/// also reaches its S3 client.
const VARIABLES: [&str; 4] = [
    "aws_allow_invalid_certificates",
    "google_allow_invalid_certificates",
    "azure_allow_invalid_certificates",
    "allow_invalid_certificates",
];

/// Returns the names of the environment variables in `vars` that turn off certificate checks.
///
/// A variable turns them off when object_store parses its value as true:
/// `1`, `true`, `on`, `yes` or `y`, in any case. object_store fails to build a
/// store when the value is neither true nor false, so only a true value turns
/// the checks off.
pub(crate) fn env_variables(vars: impl IntoIterator<Item = (OsString, OsString)>) -> Vec<String> {
    vars.into_iter()
        .filter_map(|(name, value)| Some((name.into_string().ok()?, value.into_string().ok()?)))
        .filter(|(name, value)| {
            VARIABLES.contains(&name.to_ascii_lowercase().as_str())
                && ["1", "true", "on", "yes", "y"].contains(&value.to_ascii_lowercase().as_str())
        })
        .map(|(name, _)| name)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flagged(vars: &[(&str, &str)]) -> Vec<String> {
        env_variables(
            vars.iter()
                .map(|(name, value)| (OsString::from(name), OsString::from(value))),
        )
    }

    #[test]
    fn each_store_variable_set_to_true_is_flagged() {
        assert_eq!(
            vec![
                "AWS_ALLOW_INVALID_CERTIFICATES",
                "GOOGLE_ALLOW_INVALID_CERTIFICATES",
                "AZURE_ALLOW_INVALID_CERTIFICATES",
                "ALLOW_INVALID_CERTIFICATES",
            ],
            flagged(&[
                ("AWS_ALLOW_INVALID_CERTIFICATES", "true"),
                ("GOOGLE_ALLOW_INVALID_CERTIFICATES", "true"),
                ("AZURE_ALLOW_INVALID_CERTIFICATES", "true"),
                ("ALLOW_INVALID_CERTIFICATES", "true"),
            ])
        );
    }

    #[test]
    fn every_value_object_store_reads_as_true_is_flagged() {
        for value in [
            "true", "TRUE", "True", "1", "on", "ON", "yes", "Yes", "y", "Y",
        ] {
            assert_eq!(
                vec!["AWS_ALLOW_INVALID_CERTIFICATES"],
                flagged(&[("AWS_ALLOW_INVALID_CERTIFICATES", value)]),
                "{value}"
            );
        }
    }

    #[test]
    fn a_false_or_unparsable_value_is_not_flagged() {
        for value in [
            "false", "FALSE", "0", "off", "no", "n", "", " true", "enabled",
        ] {
            assert_eq!(
                Vec::<String>::new(),
                flagged(&[("AWS_ALLOW_INVALID_CERTIFICATES", value)]),
                "{value:?}"
            );
        }
    }

    #[test]
    fn a_lower_case_name_is_flagged() {
        assert_eq!(
            vec!["allow_invalid_certificates"],
            flagged(&[("allow_invalid_certificates", "true")])
        );
    }

    #[test]
    fn an_unrelated_variable_is_not_flagged() {
        assert_eq!(
            Vec::<String>::new(),
            flagged(&[
                ("AWS_ALLOW_HTTP", "true"),
                ("AWS_ALLOW_INVALID_CERTIFICATE", "true"),
                ("NISSHI_ALLOW_INVALID_CERTIFICATES", "true"),
                ("PATH", "/usr/bin"),
            ])
        );
    }

    #[test]
    fn no_variable_is_flagged_when_none_is_set() {
        assert_eq!(Vec::<String>::new(), flagged(&[]));
    }

    #[cfg(unix)]
    #[test]
    fn a_value_that_is_not_utf8_is_not_flagged() {
        use std::os::unix::ffi::OsStringExt;

        assert_eq!(
            Vec::<String>::new(),
            env_variables([(
                OsString::from("AWS_ALLOW_INVALID_CERTIFICATES"),
                OsString::from_vec(vec![0x74, 0xff]),
            )])
        );
    }
}
