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

use nisshi_sans_io::record::{deflated, inflated};

use crate::Topition;

/// One batch for [`Storage::produce_all`](crate::Storage::produce_all) to write to one partition.
#[derive(Clone, Debug)]
pub struct ProduceEntry {
    pub topition: Topition,

    pub deflated: deflated::Batch,

    /// `deflated` decoded, under the same conditions as for
    /// [`Storage::produce_inflated`](crate::Storage::produce_inflated). `None` leaves the decoding
    /// to the storage, as [`Storage::produce`](crate::Storage::produce) does.
    pub inflated: Option<inflated::Batch>,
}
