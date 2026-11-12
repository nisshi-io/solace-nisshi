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

use super::*;
use std::assert_matches;

mod variable_expansion;

#[test]
fn invalid_url_option_reads_as_a_sentence() {
    let err = EnvVarExp::<url::Url>::from_str("foo").unwrap_err();

    assert_matches!(err, Error::Url(_));
    assert_eq!("invalid URL: relative URL without a base", err.to_string());
}

#[test]
fn clap_reports_an_invalid_schema_registry_url_as_a_sentence() {
    use clap::Parser as _;

    let err = Cli::try_parse_from(["nisshi", "broker", "--schema-registry", "foo"])
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("invalid URL: relative URL without a base"),
        "{err}"
    );
    assert!(!err.contains("RelativeUrlWithoutBase"), "{err}");
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
        Error::TlsKeyPassphraseRequired {
            option: "--key-passphrase-file",
            path: path.clone()
        }
        .to_string()
    );
    assert_eq!(
        "TLS private key /etc/nisshi/tls.key is encrypted: pass --tls-key-passphrase-file \
            <file>.",
        Error::TlsKeyPassphraseRequired {
            option: "--tls-key-passphrase-file",
            path: path.clone()
        }
        .to_string()
    );
    assert!(
        Error::TlsKeyDecrypt {
            option: "--tls-key-passphrase-file",
            path: path.clone(),
            cause: pkcs8::Error::KeyMalformed(pkcs8::KeyError::Invalid),
        }
        .to_string()
        .ends_with("Check the passphrase in --tls-key-passphrase-file.")
    );
    assert!(
        Error::TlsPrivateKey {
            option: "--tls-key-file",
            path: path.clone(),
            cause: rustls::pki_types::pem::Error::NoItemsFound,
        }
        .to_string()
        .ends_with("key (--tls-key-file).")
    );
    assert!(
        Error::TlsCertificate {
            option: "--cert",
            path: path.clone(),
            cause: rustls::pki_types::pem::Error::NoItemsFound,
        }
        .to_string()
        .ends_with("PEM certificates (--cert).")
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
        option: "--cert",
        path: path.clone(),
        cause,
    };
    assert!(err.to_string().contains(&message), "{err}");
    assert!(err.source().is_none());

    let err = Error::TlsKeyPassphraseFile {
        option: "--key-passphrase-file",
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
