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

//! A count of the produces that pass through a wrapper that tries to take a permit, a channel
//! slot or a share of the batcher's decoded budget without waiting, by the wrapper that tried
//! (`stage`) and by whether the produce got one (`path`).

use std::sync::LazyLock;

use opentelemetry::{KeyValue, metrics::Counter};

use crate::METER;

/// The wrapper that tried to take something without waiting.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Stage {
    /// [`ProduceRequestBatcher`](crate::ProduceRequestBatcher), for a share of its decoded budget.
    Batcher,

    /// The proxy's semaphore, for a permit.
    Semaphore,

    /// The request channel, for a slot.
    Channel,
}

impl Stage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Batcher => "batcher",
            Self::Semaphore => "semaphore",
            Self::Channel => "channel",
        }
    }
}

static PRODUCE_PATH: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_storage_produce_path")
        .with_description(
            "Count of produces that tried for a storage permit, channel slot or share of the \
             batcher's decoded budget without waiting, by stage (batcher, semaphore or channel) \
             and path: direct when the produce got one and passed its decoded batch on to the \
             next stage, fallback when it had to wait or was over the budget and drops its \
             decoded batch. A direct produce is not a guarantee that no stage after it decodes \
             the batch again: the batcher decodes it again when it rebuilds a request after a \
             failed merge, and a storage that reads a merged run takes it as bytes",
        )
        .build()
});

/// Counts a produce that got its permit, slot or share of the budget at once.
pub(crate) fn direct(stage: Stage) {
    PRODUCE_PATH.add(
        1,
        &[
            KeyValue::new("path", "direct"),
            KeyValue::new("stage", stage.as_str()),
        ],
    );
}

/// Counts a produce that had to wait, or was over the budget, so that its decoded batch is dropped.
pub(crate) fn fallback(stage: Stage) {
    PRODUCE_PATH.add(
        1,
        &[
            KeyValue::new("path", "fallback"),
            KeyValue::new("stage", stage.as_str()),
        ],
    );
}
