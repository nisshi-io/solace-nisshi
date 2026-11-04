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
    ErrorCode, ProduceResponse,
    produce_request::TopicProduceData,
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
};

/// Returns the response for the partition at `index` that answers with `error_code`.
pub(super) fn partition_error(index: i32, error_code: ErrorCode) -> PartitionProduceResponse {
    PartitionProduceResponse::default()
        .index(index)
        .error_code(error_code.into())
        .base_offset(-1)
        .log_append_time_ms(Some(-1))
        .log_start_offset(Some(0))
        .record_errors(Some([].into()))
        .error_message(None)
        .current_leader(None)
}

/// Returns the response for `topic` that answers each of its partitions with `error_code`.
pub(super) fn topic_error(topic: TopicProduceData, error_code: ErrorCode) -> TopicProduceResponse {
    let partitions = topic
        .partition_data
        .unwrap_or_default()
        .iter()
        .map(|partition| partition_error(partition.index, error_code))
        .collect();

    TopicProduceResponse::default()
        .name(topic.name)
        .partition_responses(Some(partitions))
}

/// Returns the [`ProduceResponse`] that carries `responses`, one for each topic of the request.
pub(super) fn produce_response(responses: Vec<TopicProduceResponse>) -> ProduceResponse {
    ProduceResponse::default()
        .responses(Some(responses))
        .throttle_time_ms(Some(0))
        .node_endpoints(None)
}
