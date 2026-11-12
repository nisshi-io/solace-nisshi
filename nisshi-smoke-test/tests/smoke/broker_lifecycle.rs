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

//! Tests of starting, restarting and stopping a broker, and of the storage it starts on.

pub mod broker;
pub mod durability;
pub mod fresh_database;
pub mod restart;
pub mod sqlite_snapshot;
pub mod storage_down_at_startup;
pub mod storage_url;
