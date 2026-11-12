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

//! Decodes the same bytes through both varint decoders and requires them to
//! agree.
//!
//! The by-value conversion of an uncompressed batch decodes its records with
//! `Decode`; every other conversion uses the serde visitors. A visitor that
//! accepts what `Decode` rejects lets a corrupt record through on those
//! paths only.

#![no_main]
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use nisshi_sans_io::{
    Decode, Error,
    de::Decoder,
    primitive::varint::{LongVarInt, VarInt},
};
use std::{fmt::Debug, io::Cursor};

fuzz_target!(|data: &[u8]| {
    let by_decode = VarInt::decode(&mut Bytes::copy_from_slice(data)).map(i32::from);
    let by_serde = VarInt::deserialize(&mut Decoder::new(&mut Cursor::new(data)));
    assert_agree("VarInt", data, by_decode, by_serde);

    let by_decode = LongVarInt::decode(&mut Bytes::copy_from_slice(data)).map(i64::from);
    let by_serde = LongVarInt::deserialize(&mut Decoder::new(&mut Cursor::new(data)));
    assert_agree("LongVarInt", data, by_decode, by_serde);
});

/// Both decoders accept the input with the same value, or both reject it.
/// Error variants are not compared: `Decode` reports an overlong varint as
/// `Error::Overflow` and a visitor as `Error::Message`.
fn assert_agree<T>(name: &str, data: &[u8], by_decode: Result<T, Error>, by_serde: Result<T, Error>)
where
    T: Debug + PartialEq,
{
    match (&by_decode, &by_serde) {
        (Ok(by_decode), Ok(by_serde)) => assert_eq!(
            by_decode, by_serde,
            "{name}: Decode and serde decode {data:02x?} to different values"
        ),
        (Err(_), Err(_)) => {}
        _ => panic!(
            "{name}: Decode and serde disagree on {data:02x?}: {by_decode:?} vs {by_serde:?}"
        ),
    }
}
