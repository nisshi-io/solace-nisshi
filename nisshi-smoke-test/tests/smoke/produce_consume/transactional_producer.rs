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

//! Tests of a transactional producer, which the broker refuses.

use nisshi_smoke_test::{KafkaCli, TRANSACTIONAL_ID_AUTHORIZATION_EXCEPTION, unique};

/// The broker must answer a transactional producer's `InitProducerId` with
/// `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` instead of serving the request. The client's
/// `initTransactions()` fails with this error, so the producer stops before it sends a record.
#[test]
fn transactional_producer_is_refused() {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(1, &[]);

    let produced = cli.producer_perf_test().produce_in_transactions(
        &topic,
        10,
        &unique::name("transactional-id"),
    );

    assert!(
        produced
            .exited(1)
            .mentions(TRANSACTIONAL_ID_AUTHORIZATION_EXCEPTION),
        "{produced}"
    );
}
