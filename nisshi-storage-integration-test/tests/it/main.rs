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

//! Tests of `nisshi-storage-integration-test` itself.

#![allow(clippy::expect_used)]
// `outside_caller` gives the suite's macro a long `without` list, which needs a higher limit
// than the compiler's default, as the macro's doc tells a caller.
#![recursion_limit = "256"]

pub mod cases;
pub mod dependencies;
pub mod outside_caller;
