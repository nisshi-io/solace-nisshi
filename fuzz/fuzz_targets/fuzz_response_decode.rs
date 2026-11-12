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
use nisshi_sans_io::Frame;

fuzz_target!(|data: &[u8]| {
    if data.len() < 4 {
        return;
    }
    let api_key = i16::from_be_bytes([data[0], data[1]]);
    let api_version = i16::from_be_bytes([data[2], data[3]]);
    let payload = Bytes::copy_from_slice(&data[4..]);
    let _ = Frame::response_from_bytes(payload, api_key, api_version);
});
