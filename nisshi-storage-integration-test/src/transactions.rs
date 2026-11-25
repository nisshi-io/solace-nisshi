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

//! Cases that need a transactional ID. An engine without transactions leaves out this area.

pub mod offset_fetch_require_stable;
pub mod txn_init_producer_id;
pub mod txn_offset_commit_fence;
pub mod txn_offset_fetch_commit_metadata;
pub mod txn_offset_metadata_max_size;
