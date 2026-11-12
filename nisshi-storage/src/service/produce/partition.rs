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

use std::sync::Arc;

use super::{admission::Admission, topic_configuration::TopicConfiguration};

/// A partition of a Produce request on its way to storage, with the configuration of its topic.
///
/// The steps that validate a partition each take a `PartitionProduce` and pass the next one to
/// their inner service, so the type of `batch` shows how far the partition has come:
///
/// - [`BatchValidationService`](super::BatchValidationService) takes the batches as the client
///   sent them, `Vec<deflated::Batch>`, and passes the one batch that remains.
/// - [`TimestampService`](super::TimestampService) takes a `deflated::Batch` and passes it with
///   the same batch decoded, `(deflated::Batch, inflated::Batch)`.
/// - [`SchemaValidationService`](super::SchemaValidationService) takes the pair, and passes it on.
/// - [`StorePartitionService`](super::StorePartitionService) takes the pair, so that the storage
///   engine does not decode the batch again.
///
/// A step answers a partition that it rejects with [`PartitionProduceResponse`], and does not call
/// its inner service.
///
/// [`PartitionProduceResponse`]: nisshi_sans_io::produce_response::PartitionProduceResponse
#[derive(Clone, Debug)]
pub struct PartitionProduce<B> {
    pub topic: String,
    pub index: i32,

    /// The transaction that the request writes the partition in, if any.
    pub transaction_id: Option<String>,

    /// The topic's configuration, which every partition of the topic shares.
    pub configuration: Arc<TopicConfiguration>,

    /// The decoded-batch budget that the partitions of the request share.
    pub admission: Admission,

    pub batch: B,
}

impl<B> PartitionProduce<B> {
    /// Returns this request without its batch, and the batch.
    pub fn take_batch(self) -> (PartitionProduce<()>, B) {
        (
            PartitionProduce {
                topic: self.topic,
                index: self.index,
                transaction_id: self.transaction_id,
                configuration: self.configuration,
                admission: self.admission,
                batch: (),
            },
            self.batch,
        )
    }
}

impl PartitionProduce<()> {
    /// Returns this request with `batch`.
    pub fn with_batch<B>(self, batch: B) -> PartitionProduce<B> {
        PartitionProduce {
            topic: self.topic,
            index: self.index,
            transaction_id: self.transaction_id,
            configuration: self.configuration,
            admission: self.admission,
            batch,
        }
    }
}
