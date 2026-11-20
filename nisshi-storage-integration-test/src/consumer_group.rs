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

//! Cases about consumer groups: joining, syncing, describing, listing and deleting them.

pub mod cg_dynamic;
pub mod cg_static;
pub mod consumer_group_describe;
pub mod delete_groups;
pub mod describe_groups;
pub mod find_coordinator;
pub mod group_across_brokers;
pub mod group_cache_miss;
pub mod join_invalid_session_timeout;
pub mod list_groups;
pub mod new_cg;
pub mod rebalance_timing;
