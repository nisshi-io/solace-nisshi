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

//! When the factory-level AWS credential pre-check in
//! [`S3OptimisticConcurrencyEngineFactory::build`] cannot get a credential,
//! it must fail fast with [`Error::NoCredentials`] rather than starting
//! successfully and failing later on the first real request.
//!
//! A real integration test against local MinIO (both credential tiers, bad
//! keys, a missing bucket, a wrong endpoint, zero credentials) exists only
//! as a manual check, not reproduced here. These tests avoid driving
//! `object_store`'s real IMDS lookup in CI: an unreachable
//! `169.254.169.254` behaves differently depending on the host (some
//! environments, notably Azure, answer that address with something other
//! than "unreachable"), so a test that relies on IMDS being absent is not
//! reliably deterministic.
//!
//! `object_store` instead honors an `AWS_METADATA_ENDPOINT` config key
//! (`object_store::aws::builder`), which redirects the IMDS lookup
//! itself to an arbitrary URL. Pointing it at a port nothing listens on
//! gives a deterministic, fast (connection-refused, not a timeout) failure
//! with no dependency on the host's real network environment.

use nisshi_storage::{ArcDynStorage, Error, StorageFactory as _, StorageFactoryConfiguration};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::factory::{S3OptimisticConcurrencyEngineFactory, skip_signature};

/// An address nothing listens on: connections to it are refused immediately,
/// so these tests fail fast instead of waiting out a real IMDS timeout.
const UNREACHABLE_METADATA_ENDPOINT: &str = "http://127.0.0.1:1";

/// Builds the S3 factory with no static keys, no web identity and no task
/// role, so the only credential source left is the (redirected) metadata
/// endpoint, where nothing listens. `skip_signature` sets
/// `AWS_SKIP_SIGNATURE`.
async fn build_without_credentials(skip_signature: Option<&str>) -> Result<ArcDynStorage, Error> {
    let configuration = StorageFactoryConfiguration {
        node_id: 111,
        cluster: "nisshi".to_owned(),
        advertised_listener: Url::parse("tcp://localhost:9092").expect("url"),
        storage: Url::parse("s3://nisshi-test-bucket").expect("url"),
        schema_registry: None,
        lake_house: None,
        cancellation: CancellationToken::new(),
    };

    temp_env::async_with_vars(
        [
            ("AWS_ACCESS_KEY_ID", None::<&str>),
            ("AWS_SECRET_ACCESS_KEY", None::<&str>),
            ("AWS_SESSION_TOKEN", None::<&str>),
            ("AWS_WEB_IDENTITY_TOKEN_FILE", None::<&str>),
            ("AWS_ROLE_ARN", None::<&str>),
            ("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", None::<&str>),
            ("AWS_CONTAINER_CREDENTIALS_FULL_URI", None::<&str>),
            ("AWS_SKIP_SIGNATURE", skip_signature),
            ("AWS_METADATA_ENDPOINT", Some(UNREACHABLE_METADATA_ENDPOINT)),
        ],
        S3OptimisticConcurrencyEngineFactory.build(configuration),
    )
    .await
}

#[tokio::test]
async fn build_fails_fast_with_no_resolvable_credentials() {
    match build_without_credentials(None).await {
        Err(Error::NoCredentials(_)) => {}
        Err(other) => panic!("expected Error::NoCredentials, got a different error: {other}"),
        Ok(_) => panic!("expected Error::NoCredentials, but build() succeeded"),
    }
}

/// With `AWS_SKIP_SIGNATURE` set to a value that `object_store` reads as
/// true, requests are unsigned and need no credential, so the factory must
/// skip the credential check for every spelling `object_store` accepts.
#[tokio::test]
async fn build_skips_credential_check_when_signing_is_off() {
    for value in ["true", "TRUE", "1", "on", "yes", "y", "Yes"] {
        if let Err(error) = build_without_credentials(Some(value)).await {
            panic!("expected build() to succeed with AWS_SKIP_SIGNATURE={value}, got: {error}");
        }
    }
}

/// A value that `object_store` reads as false keeps signing on, so the
/// factory must still run the credential check.
#[tokio::test]
async fn build_checks_credentials_when_signing_is_on() {
    for value in ["false", "0"] {
        match build_without_credentials(Some(value)).await {
            Err(Error::NoCredentials(_)) => {}
            Err(other) => panic!(
                "expected Error::NoCredentials with AWS_SKIP_SIGNATURE={value}, got a different error: {other}"
            ),
            Ok(_) => panic!(
                "expected Error::NoCredentials with AWS_SKIP_SIGNATURE={value}, but build() succeeded"
            ),
        }
    }
}

/// [`skip_signature`] must accept exactly the spellings that `object_store`
/// parses as true, in any case, and nothing else.
#[test]
fn skip_signature_matches_object_store_true_values() {
    for value in ["1", "true", "on", "yes", "y", "TRUE", "On", "YES", "Y"] {
        assert!(skip_signature(value), "{value} must skip the check");
    }

    for value in [
        "0", "false", "off", "no", "n", "FALSE", "", " true", "enabled",
    ] {
        assert!(!skip_signature(value), "{value} must not skip the check");
    }
}
