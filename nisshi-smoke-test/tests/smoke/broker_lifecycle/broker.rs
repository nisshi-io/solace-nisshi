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

//! Tests that the broker names itself as node 111 in `kafka-broker-api-versions`, and reports the
//! cluster id it was started with.

use nisshi_smoke_test::{KafkaCli, SHARED_CLUSTER_ID};

/// The broker is one node, 111.
#[test]
fn api_versions_names_node_111() {
    let cli = KafkaCli::shared();

    let listed = cli.broker_api_versions().list();

    assert_eq!(listed.succeeded().node_id(), Some(111), "{listed}");
}

/// `kafka-cluster cluster-id` must report the id the broker was started with, or a client that
/// checks it is talking to the same cluster as before would find a different one.
#[test]
fn cluster_id_is_the_one_the_broker_started_with() {
    let cli = KafkaCli::shared();

    let cluster_id = cli.cluster().id();

    assert_eq!(
        cluster_id.succeeded().cluster_id(),
        Some(SHARED_CLUSTER_ID),
        "{cluster_id}"
    );
}
