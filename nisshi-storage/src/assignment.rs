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

//! Leader and replica assignment for topic metadata.

use nisshi_sans_io::metadata_response::MetadataResponsePartition;

use crate::{Error, Result};

/// The largest replication factor that a topic can be created with, and the
/// most replicas reported for a partition.
///
/// Kafka 3.9.1 has no fixed cap. It rejects a factor below one, or above the
/// number of registered brokers, with `INVALID_REPLICATION_FACTOR` when the
/// topic is created ([ReplicationControlManager.java#L727-L729], [#L761-L765],
/// [StripedReplicaPlacer.java#L412-L418]). Nisshi accepts a factor above its
/// broker count, so that a client asking for three replicas of a single node
/// keeps working, and uses this fixed bound instead, well above any real
/// factor.
///
/// [ReplicationControlManager.java#L727-L729]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java#L727-L729
/// [#L761-L765]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java#L761-L765
/// [StripedReplicaPlacer.java#L412-L418]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/metadata/placement/StripedReplicaPlacer.java#L412-L418
pub const MAX_REPLICATION_FACTOR: i16 = 64;

/// The number of replicas to report for a partition with the stored
/// `replication_factor`: none below one, and at most
/// [`MAX_REPLICATION_FACTOR`].
///
/// A stored factor can be out of that range, because CreateTopics did not
/// always check it, so a stored factor cannot panic or grow a response
/// without bound. Within the range, the response reports the stored factor
/// and lists a broker more than once when fewer brokers are listed. Kafka
/// never lists a broker twice, but tools such as the Terraform Kafka provider
/// read a topic's replication factor back as its replica count, and replace a
/// topic whose count differs from the one they created it with.
fn replica_count(replication_factor: impl Into<i32>) -> usize {
    usize::try_from(replication_factor.into())
        .unwrap_or_default()
        .min(usize::from(MAX_REPLICATION_FACTOR.unsigned_abs()))
}

/// The replicas of a partition when `node` is the only broker listed: `node`
/// repeated once per replica, none for a `replication_factor` below one, and
/// at most [`MAX_REPLICATION_FACTOR`] times.
pub fn single_node_replicas(node: i32, replication_factor: impl Into<i32>) -> Vec<i32> {
    vec![node; replica_count(replication_factor)]
}

/// Assign a leader and replicas to each partition, round robin over `broker_ids`.
///
/// Each partition takes the brokers following its leader as its replicas
/// (also reported as in sync): none for a `replication_factor` below one, and
/// at most [`MAX_REPLICATION_FACTOR`], wrapping over `broker_ids` when there
/// are fewer brokers than replicas. Every
/// storage backend builds its Metadata partitions through this function.
pub fn assign_replicas(
    broker_ids: &[i32],
    partitions: i32,
    replication_factor: impl Into<i32>,
    error_code: i16,
    leader_epoch: i32,
) -> Result<Vec<MetadataResponsePartition>> {
    if broker_ids.is_empty() {
        return Err(Error::Message(
            "no brokers available for partition assignment".into(),
        ));
    }

    let replicas = replica_count(replication_factor);
    let per_partition = 1 + replicas;

    Ok((0..partitions)
        .map(|partition_index| {
            let base = usize::try_from(partition_index).unwrap_or_default() * per_partition;
            let leader_id = broker_ids[base % broker_ids.len()];

            let replica_nodes: Vec<i32> = (0..replicas)
                .map(|replica| broker_ids[(base + 1 + replica) % broker_ids.len()])
                .collect();

            MetadataResponsePartition::default()
                .error_code(error_code)
                .partition_index(partition_index)
                .leader_id(leader_id)
                .leader_epoch(Some(leader_epoch))
                .replica_nodes(Some(replica_nodes.clone()))
                .isr_nodes(Some(replica_nodes))
                .offline_replicas(Some([].into()))
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes(partition: &MetadataResponsePartition) -> Vec<i32> {
        partition.replica_nodes.clone().unwrap_or_default()
    }

    #[test]
    fn no_brokers_is_an_error() {
        assert!(assign_replicas(&[], 1, 1, 0, 0).is_err());
    }

    #[test]
    fn leaders_and_replicas_are_consecutive_brokers() -> Result<()> {
        let partitions = assign_replicas(&[10, 20, 30], 3, 1, 0, 0)?;

        let assigned: Vec<_> = partitions
            .iter()
            .map(|partition| (partition.leader_id, nodes(partition)))
            .collect();

        assert_eq!(
            vec![(10, vec![20]), (30, vec![10]), (20, vec![30])],
            assigned
        );
        Ok(())
    }

    #[test]
    fn isr_matches_replicas_and_epoch_is_passed_through() -> Result<()> {
        let partitions = assign_replicas(&[1, 2], 2, 1, 0, -1)?;

        for partition in &partitions {
            assert_eq!(partition.replica_nodes, partition.isr_nodes);
            assert_eq!(Some(-1), partition.leader_epoch);
        }
        Ok(())
    }

    #[test]
    fn negative_replication_factor_has_no_replicas_and_does_not_overflow() -> Result<()> {
        let partitions = assign_replicas(&[1, 2, 3], 5, -2, 0, 0)?;

        assert_eq!(5, partitions.len());
        assert!(
            partitions
                .iter()
                .all(|partition| nodes(partition).is_empty())
        );
        Ok(())
    }

    #[test]
    fn replicas_wrap_over_fewer_brokers_than_the_factor() -> Result<()> {
        let partitions = assign_replicas(&[1, 2], 1, 3, 0, 0)?;

        assert_eq!(1, partitions[0].leader_id);
        assert_eq!(vec![2, 1, 2], nodes(&partitions[0]));
        Ok(())
    }

    #[test]
    fn replica_count_is_zero_below_one() {
        for replication_factor in [-2, 0, i16::MIN] {
            assert_eq!(0, replica_count(replication_factor));
        }
    }

    #[test]
    fn replica_count_is_the_factor_up_to_the_maximum() {
        assert_eq!(1, replica_count(1));
        assert_eq!(3, replica_count(3));
        assert_eq!(64, replica_count(MAX_REPLICATION_FACTOR));
        assert_eq!(64, replica_count(MAX_REPLICATION_FACTOR + 1));
        assert_eq!(64, replica_count(i16::MAX));
    }

    #[test]
    fn single_node_replicas_repeats_the_node_up_to_the_maximum() {
        assert_eq!(Vec::<i32>::new(), single_node_replicas(111, -2));
        assert_eq!(Vec::<i32>::new(), single_node_replicas(111, 0));
        assert_eq!(vec![111; 3], single_node_replicas(111, 3));
        assert_eq!(vec![111; 64], single_node_replicas(111, i16::MAX));
    }
}
