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

//! A count of the produces that pass through a wrapper that tries to take a permit, a channel
//! slot or a share of the batcher's decoded budget without waiting, by whether the produce got one.

use std::sync::LazyLock;

use opentelemetry::{KeyValue, metrics::Counter};

use crate::METER;

static PRODUCE_PATH: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("nisshi_storage_produce_path")
        .with_description(
            "Count of produces that tried for a storage permit, channel slot or share of the \
             batcher's decoded budget without waiting, by path: direct when the produce got one \
             and passed its decoded batch on, fallback when it had to wait or was over the budget \
             and the batch is decoded again",
        )
        .build()
});

/// Counts a produce that got its permit, slot or share of the budget at once.
pub(crate) fn direct() {
    PRODUCE_PATH.add(1, &[KeyValue::new("path", "direct")]);
}

/// Counts a produce that had to wait, or was over the budget, so that its batch is decoded again.
pub(crate) fn fallback() {
    PRODUCE_PATH.add(1, &[KeyValue::new("path", "fallback")]);
}
