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

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
use std::{collections::HashMap, env::vars, path::PathBuf, result, str::FromStr};

mod cli;
mod insecure_tls;

pub use cli::{Cli, Outcome};
use regex::{Regex, Replacer};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Box(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    Cat(Box<nisshi_cat::Error>),
    #[error(transparent)]
    Client(Box<nisshi_client::Error>),
    #[error(transparent)]
    DotEnv(#[from] dotenv::Error),
    #[error(transparent)]
    Generate(#[from] nisshi_generator::Error),
    #[error(transparent)]
    InvalidLength(#[from] sha2::digest::InvalidLength),
    /// A data lake subcommand (`iceberg`, `delta`, `parquet`) was given
    /// without `--schema-registry`, which every lake writer requires.
    #[error(
        "{lake} requires --schema-registry: a data lake writer validates every record against a \
            schema."
    )]
    LakeRequiresSchemaRegistry { lake: &'static str },
    #[error(transparent)]
    Perf(#[from] nisshi_perf::Error),
    /// The broker's `--schema-registry`, or the `SCHEMA_REGISTRY` variable, is not a URL.
    #[error("invalid value '{value}' for '--schema-registry <SCHEMA_REGISTRY>': {cause}")]
    SchemaRegistry { value: String, cause: String },
    #[error(transparent)]
    Proxy(#[from] nisshi_proxy::Error),
    #[error(transparent)]
    Regex(#[from] regex::Error),
    #[error(transparent)]
    SansIo(#[from] nisshi_sans_io::Error),
    /// `--sasl-password-file` and the `SASL_PASSWORD` environment variable are both set.
    #[error(
        "Both --sasl-password-file and the SASL_PASSWORD environment variable are set. Use one."
    )]
    SaslPasswordAmbiguous,
    /// The `--sasl-password-file` could not be read.
    #[error("could not read --sasl-password-file {}: {cause}", path.display())]
    SaslPasswordFile {
        path: PathBuf,
        cause: std::io::Error,
    },
    /// `--sasl-username` was given with neither `--sasl-password-file` nor `SASL_PASSWORD`.
    #[error(
        "--sasl-username needs a password, in the SASL_PASSWORD environment variable or in \
            --sasl-password-file."
    )]
    SaslPasswordMissing,
    /// The `SASL_PASSWORD` environment variable is not valid Unicode.
    #[error("The SASL_PASSWORD environment variable is not valid Unicode.")]
    SaslPasswordNotUnicode,
    #[error("schema registry: {0}")]
    Schema(Box<nisshi_schema::Error>),
    #[error("broker: {0}")]
    Server(Box<nisshi_broker::Error>),
    #[error(transparent)]
    Tls(#[from] rustls::Error),
    /// The `--cert` file could not be read or parsed as PEM certificates.
    ///
    /// The message carries `cause`, so the field is not named `source`: thiserror would also
    /// return it from `source()`, and a reporter that prints the chain would show it twice. The
    /// other variants with a `cause` follow the same rule.
    #[error(
        "TLS certificate {} could not be loaded: {cause}. Expected one or more PEM certificates \
            (--cert).",
        path.display()
    )]
    TlsCertificate {
        path: PathBuf,
        cause: rustls::pki_types::pem::Error,
    },
    /// The `--key` file could not be read or parsed as a PEM private key.
    #[error(
        "TLS private key {} could not be loaded: {cause}. Expected a PKCS#8, SEC1 or RSA PEM \
            key (--key).",
        path.display()
    )]
    TlsPrivateKey {
        path: PathBuf,
        cause: rustls::pki_types::pem::Error,
    },
    /// The `--key` file is encrypted but no `--key-passphrase-file` was given.
    #[error(
        "TLS private key {} is encrypted: pass --key-passphrase-file <file>.",
        path.display()
    )]
    TlsKeyPassphraseRequired { path: PathBuf },
    /// The passphrase did not decrypt the key.
    #[error(
        "TLS private key {} could not be decrypted: {cause}. Check the passphrase in \
            --key-passphrase-file.",
        path.display()
    )]
    TlsKeyDecrypt { path: PathBuf, cause: pkcs8::Error },
    /// The key is encrypted with an algorithm this build does not support,
    /// named by `algorithm` (a friendly name, or the OID when unknown).
    #[error(
        "TLS private key {} is encrypted with {algorithm}, which is not supported. Supported: \
            PKCS#8 PBES2 with PBKDF2-HMAC-SHA2 or scrypt and AES-CBC or Triple DES. Re-encrypt \
            it: openssl pkcs8 -topk8 -in key.pem -out key-pkcs8.pem -v2 aes-256-cbc -v2prf \
            hmacWithSHA256",
        path.display()
    )]
    TlsKeyUnsupportedEncryption { path: PathBuf, algorithm: String },
    /// Legacy OpenSSL PEM encryption (`Proc-Type: 4,ENCRYPTED`) is not supported.
    #[error(
        "TLS private key {} uses legacy OpenSSL PEM encryption, which is not supported. \
            Convert it: openssl pkcs8 -topk8 -in key.pem -out key-pkcs8.pem",
        path.display()
    )]
    TlsKeyLegacyEncrypted { path: PathBuf },
    /// The `--key-passphrase-file` could not be read.
    #[error(
        "TLS key passphrase file {} could not be read: {cause}.",
        path.display()
    )]
    TlsKeyPassphraseFile {
        path: PathBuf,
        cause: std::io::Error,
    },
    /// The certificate and the key must be supplied together: `--cert` and `--key` for the
    /// broker, `--tls-cert-file` and `--tls-key-file` for the other commands.
    #[error(
        "TLS requires both a certificate and a key: --cert and --key for the broker, \
            --tls-cert-file and --tls-key-file for the other commands."
    )]
    TlsRequiresCertAndKey,
    #[error(transparent)]
    Topic(#[from] nisshi_topic::Error),
    /// The message carries the parse error, so the field is not `#[from]` or named `source`.
    #[error("invalid URL: {0}")]
    Url(url::ParseError),
}

impl From<url::ParseError> for Error {
    fn from(value: url::ParseError) -> Self {
        Self::Url(value)
    }
}

impl From<nisshi_cat::Error> for Error {
    fn from(value: nisshi_cat::Error) -> Self {
        Self::Cat(Box::new(value))
    }
}

impl From<nisshi_client::Error> for Error {
    fn from(value: nisshi_client::Error) -> Self {
        Self::Client(Box::new(value))
    }
}

impl From<nisshi_schema::Error> for Error {
    fn from(value: nisshi_schema::Error) -> Self {
        Self::Schema(Box::new(value))
    }
}

impl From<nisshi_broker::Error> for Error {
    fn from(value: nisshi_broker::Error) -> Self {
        Self::Server(Box::new(value))
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Clone, Debug)]
pub struct VarRep(HashMap<String, String>);

impl From<HashMap<String, String>> for VarRep {
    fn from(value: HashMap<String, String>) -> Self {
        Self(value)
    }
}

impl VarRep {
    fn replace(&self, haystack: &str) -> Result<String> {
        Regex::new(r"\$\{(?<var>[^\}]+)\}")
            .map(|re| re.replace(haystack, self).into_owned())
            .map_err(Into::into)
    }
}

impl Replacer for &VarRep {
    fn replace_append(&mut self, caps: &regex::Captures<'_>, dst: &mut String) {
        if let Some(variable) = caps.name("var")
            && let Some(value) = self.0.get(variable.as_str())
        {
            dst.push_str(value);
        }
    }
}

#[derive(Clone, Debug)]
pub struct EnvVarExp<T>(T);

impl<T> EnvVarExp<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> FromStr for EnvVarExp<T>
where
    T: FromStr,
    Error: From<<T as FromStr>::Err>,
{
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        VarRep::from(vars().collect::<HashMap<_, _>>())
            .replace(s)
            .and_then(|s| T::from_str(&s).map_err(Into::into))
            .map(|t| Self(t))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_matches;

    #[test]
    fn invalid_url_option_reads_as_a_sentence() {
        let err = EnvVarExp::<url::Url>::from_str("foo").unwrap_err();

        assert_matches!(err, Error::Url(_));
        assert_eq!("invalid URL: relative URL without a base", err.to_string());
    }

    /// The broker reads `--schema-registry` as a string, so a bad value fails its startup, not
    /// the parse of every tool that flattens the broker's arguments.
    #[test]
    fn clap_leaves_an_invalid_schema_registry_to_the_broker() {
        use clap::Parser as _;

        let _ = Cli::try_parse_from(["nisshi", "broker", "--schema-registry", "foo"])
            .expect("clap leaves the value to the broker");
    }

    /// The `nisshi` binary prints these messages as they are, so the text here is the text a
    /// user reads.
    #[test]
    fn tls_and_lake_errors_name_the_fix() {
        let path = PathBuf::from("/etc/nisshi/tls.key");

        assert_eq!(
            "iceberg requires --schema-registry: a data lake writer validates every record \
                against a schema.",
            Error::LakeRequiresSchemaRegistry { lake: "iceberg" }.to_string()
        );
        assert_eq!(
            "TLS requires both a certificate and a key: --cert and --key for the broker, \
                --tls-cert-file and --tls-key-file for the other commands.",
            Error::TlsRequiresCertAndKey.to_string()
        );
        assert_eq!(
            "Both --sasl-password-file and the SASL_PASSWORD environment variable are set. Use \
                one.",
            Error::SaslPasswordAmbiguous.to_string()
        );
        assert_eq!(
            "--sasl-username needs a password, in the SASL_PASSWORD environment variable or in \
                --sasl-password-file.",
            Error::SaslPasswordMissing.to_string()
        );
        assert_eq!(
            "The SASL_PASSWORD environment variable is not valid Unicode.",
            Error::SaslPasswordNotUnicode.to_string()
        );
        assert_eq!(
            "could not read --sasl-password-file /no/such/file: entity not found",
            Error::SaslPasswordFile {
                path: PathBuf::from("/no/such/file"),
                cause: std::io::Error::other("entity not found"),
            }
            .to_string()
        );
        assert_eq!(
            "TLS private key /etc/nisshi/tls.key is encrypted: pass --key-passphrase-file <file>.",
            Error::TlsKeyPassphraseRequired { path: path.clone() }.to_string()
        );
        assert_eq!(
            "TLS private key /etc/nisshi/tls.key uses legacy OpenSSL PEM encryption, which is not \
                supported. Convert it: openssl pkcs8 -topk8 -in key.pem -out key-pkcs8.pem",
            Error::TlsKeyLegacyEncrypted { path: path.clone() }.to_string()
        );

        let message = Error::TlsKeyUnsupportedEncryption {
            path,
            algorithm: "DES-CBC".into(),
        }
        .to_string();
        assert!(
            message.starts_with(
                "TLS private key /etc/nisshi/tls.key is encrypted with DES-CBC, which is not \
                    supported. Supported: "
            ),
            "{message}"
        );
        assert!(
            message.contains("Re-encrypt it: openssl pkcs8 -topk8"),
            "{message}"
        );
    }

    /// A message that carries its cause must not also return the cause from `source()`, or a
    /// reporter that walks the chain prints it twice.
    #[test]
    fn a_message_with_its_cause_does_not_repeat_it_as_the_source() {
        use std::error::Error as _;

        let path = PathBuf::from("/etc/nisshi/tls.crt");
        let cause = rustls::pki_types::pem::Error::NoItemsFound;
        let message = format!("{cause}");

        let err = Error::TlsCertificate {
            path: path.clone(),
            cause,
        };
        assert!(err.to_string().contains(&message), "{err}");
        assert!(err.source().is_none());

        let err = Error::TlsKeyPassphraseFile {
            path,
            cause: std::io::Error::other("permission denied"),
        };
        assert!(err.to_string().contains("permission denied"), "{err}");
        assert!(err.source().is_none());

        let err = Error::from(url::ParseError::RelativeUrlWithoutBase);
        assert!(err.source().is_none());
    }

    #[test]
    fn schema_and_broker_errors_name_their_subsystem() {
        let err = Error::from(nisshi_schema::Error::Message("denied".into()));

        assert_matches!(err, Error::Schema(_));
        assert!(err.to_string().starts_with("schema registry: "), "{err}");
    }
}
