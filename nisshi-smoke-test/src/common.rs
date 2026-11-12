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

//! The plumbing that the rest of the harness stands on: running commands, claiming ports, waiting
//! for the broker, and making names. Nothing here knows about the Kafka tools or the broker.

pub(crate) mod docker;
pub mod polling;
mod reserved_port;
pub(crate) mod timed_command;
pub mod unique;

pub use reserved_port::ReservedPort;
