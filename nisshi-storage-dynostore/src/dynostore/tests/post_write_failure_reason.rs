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

use std::{io, sync::Arc};

use nisshi_sans_io::ErrorCode;
use nisshi_storage::Error;

use crate::dynostore::post_write_failure_reason;

/// The `reason` of `nisshi_produce_post_write_advance_failed` is the Kafka
/// error name for an API error, so a fenced producer, whose batch is stored
/// because the pre-check passed on stale metadata, is told apart from a
/// rejected sequence.
#[test]
fn api_error_is_named_by_its_error_code() {
    assert_eq!(
        "ProducerFenced",
        post_write_failure_reason(&Error::Api(ErrorCode::ProducerFenced))
    );

    assert_eq!(
        "DuplicateSequenceNumber",
        post_write_failure_reason(&Error::Api(ErrorCode::DuplicateSequenceNumber))
    );
}

/// A transient object store error is named as `Metron` names it for
/// `put_opts`, so the two metrics share one vocabulary.
#[test]
fn object_store_error_is_named_as_the_request_error_metric_names_it() {
    assert_eq!(
        "not_found",
        post_write_failure_reason(&Error::ObjectStore(Arc::new(
            object_store::Error::NotFound {
                path: "clusters/test/meta.json".into(),
                source: "gone".into(),
            }
        )))
    );

    assert_eq!(
        "pre_condition",
        post_write_failure_reason(&Error::ObjectStore(Arc::new(
            object_store::Error::Precondition {
                path: "clusters/test/meta.json".into(),
                source: "changed".into(),
            }
        )))
    );
}

/// Anything else collapses to one value, which keeps the attribute's
/// cardinality bounded.
#[test]
fn any_other_error_is_other() {
    assert_eq!(
        "other",
        post_write_failure_reason(&Error::Io(Arc::new(io::Error::other("broken"))))
    );
}
