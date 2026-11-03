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

use nisshi_sans_io::{
    ApiKey, ErrorCode, RequestInput, TxnOffsetCommitResponse,
    txn_offset_commit_request::TxnOffsetCommitRequestTopic,
    txn_offset_commit_response::{TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic},
};
use rama::Service;
use tracing::instrument;

use crate::{Error, Result, Storage, offset_metadata};

/// A [`Service`] using its [`Storage`] taking [`nisshi_sans_io::TxnOffsetCommitRequest`] returning [`TxnOffsetCommitResponse`].
#[derive(Clone, Debug)]
pub struct OffsetCommitService<G> {
    pub storage: G,
}

impl<G> ApiKey for OffsetCommitService<G> {
    const KEY: i16 = nisshi_sans_io::TxnOffsetCommitRequest::KEY;
}

impl<G, I> Service<I> for OffsetCommitService<G>
where
    G: Storage,
    I: Into<RequestInput<nisshi_sans_io::TxnOffsetCommitRequest>> + Send + 'static,
{
    type Output = TxnOffsetCommitResponse;
    type Error = Error;

    #[instrument(skip(self, input))]
    async fn serve(&self, input: I) -> Result<Self::Output, Self::Error> {
        let input = input.into();

        let (topics, too_large) =
            without_too_large_metadata(input.request.topics.unwrap_or_default());

        // The storage checks the producer only when it commits a partition. Kafka checks the
        // producer first. So when every partition of a request is over the limit, a fenced
        // producer gets OFFSET_METADATA_TOO_LARGE here, where Kafka answers the fencing error.
        let mut responses = if topics.is_empty() {
            vec![]
        } else {
            self.storage
                .txn_offset_commit(crate::TxnOffsetCommitRequest {
                    transaction_id: input.request.transactional_id.to_owned(),
                    group_id: input.request.group_id.to_owned(),
                    producer_id: input.request.producer_id,
                    producer_epoch: input.request.producer_epoch,
                    generation_id: input.request.generation_id,
                    member_id: input.request.member_id,
                    group_instance_id: input.request.group_instance_id,
                    topics,
                })
                .await?
        };

        responses.extend(too_large);

        Ok(TxnOffsetCommitResponse::default()
            .throttle_time_ms(0)
            .topics(Some(responses)))
    }
}

/// Splits `topics` into the partitions that the storage commits, and a response of
/// `OFFSET_METADATA_TOO_LARGE` for each partition whose metadata is over
/// [`offset_metadata::MAX_LENGTH`]. A topic without a partition to commit is left out of the first.
fn without_too_large_metadata(
    topics: Vec<TxnOffsetCommitRequestTopic>,
) -> (
    Vec<TxnOffsetCommitRequestTopic>,
    Vec<TxnOffsetCommitResponseTopic>,
) {
    let mut committed = vec![];
    let mut too_large = vec![];

    for topic in topics {
        let (rejected, accepted): (Vec<_>, Vec<_>) = topic
            .partitions
            .unwrap_or_default()
            .into_iter()
            .partition(|partition| {
                offset_metadata::is_too_large(partition.committed_metadata.as_deref())
            });

        if !rejected.is_empty() {
            too_large.push(
                TxnOffsetCommitResponseTopic::default()
                    .name(topic.name.clone())
                    .partitions(Some(
                        rejected
                            .iter()
                            .map(|partition| {
                                TxnOffsetCommitResponsePartition::default()
                                    .partition_index(partition.partition_index)
                                    .error_code(ErrorCode::OffsetMetadataTooLarge.into())
                            })
                            .collect(),
                    )),
            );
        }

        if !accepted.is_empty() {
            committed.push(
                TxnOffsetCommitRequestTopic::default()
                    .name(topic.name)
                    .partitions(Some(accepted)),
            );
        }
    }

    (committed, too_large)
}
