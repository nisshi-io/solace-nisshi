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

//! A fake inner service, and the requests, for the tests of the steps that validate a partition.

use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
};

use nisshi_sans_io::{
    ErrorCode,
    describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
    produce_response::PartitionProduceResponse,
};
use rama::Service;

use super::super::{PartitionProduce, TopicConfiguration, admission::Admission, response};

/// The index of the partition in every request that [`partition_produce`] builds.
pub(in super::super) const INDEX: i32 = 3;

/// A partition service that records the batch of each request that it serves, and answers the
/// partition with `ErrorCode::None`.
#[derive(Clone, Debug)]
pub(in super::super) struct RecordingPartitions<B> {
    served: Arc<Mutex<Vec<B>>>,
}

impl<B> Default for RecordingPartitions<B> {
    fn default() -> Self {
        Self {
            served: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl<B: Clone> RecordingPartitions<B> {
    /// Returns the batch of each request that the service has served, in order.
    pub(in super::super) fn served(&self) -> Vec<B> {
        self.served
            .lock()
            .expect("a test thread panicked while it held the lock")
            .clone()
    }
}

impl<B> Service<PartitionProduce<B>> for RecordingPartitions<B>
where
    B: Send + Sync + 'static,
{
    type Output = PartitionProduceResponse;
    type Error = Infallible;

    async fn serve(&self, request: PartitionProduce<B>) -> Result<Self::Output, Self::Error> {
        self.served
            .lock()
            .expect("a test thread panicked while it held the lock")
            .push(request.batch);

        Ok(response::partition_error(request.index, ErrorCode::None))
    }
}

/// Returns the configuration of a topic that sets each of `configs`, a name and its value.
pub(in super::super) fn configuration(configs: &[(&str, &str)]) -> Arc<TopicConfiguration> {
    let configs = configs
        .iter()
        .map(|(name, value)| {
            DescribeConfigsResourceResult::default()
                .name((*name).into())
                .value(Some((*value).into()))
        })
        .collect();

    TopicConfiguration::described(
        "person",
        DescribeConfigsResult::default()
            .error_code(ErrorCode::None.into())
            .configs(Some(configs)),
    )
    .map(Arc::new)
    .expect("a described topic has a configuration")
}

/// Returns a request for partition [`INDEX`] of the topic `person`, with `batch`, in a topic that
/// sets `configs`.
pub(in super::super) fn partition_produce<B>(
    batch: B,
    configs: &[(&str, &str)],
) -> PartitionProduce<B> {
    PartitionProduce {
        topic: "person".into(),
        index: INDEX,
        transaction_id: None,
        configuration: configuration(configs),
        admission: Admission::default(),
        batch,
    }
}

/// The error code of `response`.
pub(in super::super) fn error_code(response: &PartitionProduceResponse) -> ErrorCode {
    ErrorCode::try_from(response.error_code).expect("the response has a known error code")
}
