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

#![no_main]
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use nisshi_sans_io::record::{Record, deflated};

fuzz_target!(|data: &[u8]| {
    // A parsed header carries the codec in its attribute bits, so this also
    // drives the record decoder end to end from the wire bytes.
    if let Ok(batch) = deflated::Batch::try_from(Bytes::copy_from_slice(data)) {
        let _ = Vec::<Record>::try_from(&batch);
    }
});
