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

//! Tests that `kafka-reassign-partitions --list` does not list a new topic's partition as being
//! reassigned. Nisshi has one broker, so it never moves a partition, and an operator who sees a
//! reassignment in progress waits for one that never ends. The broker must answer
//! `ListPartitionReassignments` with no reassignment in progress.

use nisshi_smoke_test::{KafkaCli, NO_REASSIGNMENTS};

#[test]
fn list_does_not_report_a_new_partition_as_reassigned() {
    let cli = KafkaCli::shared();
    let topic = cli.topics().create_unique(1, &[]);

    let listed = cli.reassign_partitions().list();

    assert!(
        !listed.succeeded().mentions(&format!("{topic}-0")),
        "the broker lists a partition as being reassigned: {listed}"
    );
    assert!(
        listed.mentions(NO_REASSIGNMENTS),
        "the broker reports a reassignment in progress: {listed}"
    );
}
