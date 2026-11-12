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

use std::collections::BTreeSet;

use crate::MetadataResponse;

mod cooperative_sticky;
mod range;
mod round_robin;
mod uniform;

pub use cooperative_sticky::CooperativeStickyAssignor;
pub use range::RangeAssignor;
pub use round_robin::RoundRobinAssignor;
pub use uniform::UniformAssignor;

#[derive(Clone, Default, Eq, Hash, Debug, Ord, PartialEq, PartialOrd)]
struct Topition {
    topic: String,
    partition: i32,
}

impl From<&MetadataResponse> for BTreeSet<Topition> {
    fn from(value: &MetadataResponse) -> Self {
        value
            .topics
            .as_deref()
            .unwrap_or_default()
            .iter()
            .flat_map(|topic| {
                if let Some(name) = topic.name.as_deref() {
                    topic
                        .partitions
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .map(|partition| Topition {
                            topic: name.into(),
                            partition: partition.partition_index,
                        })
                        .collect::<Vec<_>>()
                } else {
                    vec![]
                }
            })
            .collect::<BTreeSet<_>>()
    }
}
