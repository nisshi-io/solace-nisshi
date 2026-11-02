// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
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

//! Drives the harness's check functions the way a fuzz target does, over arbitrary requests built
//! from fixed pseudo-random bytes, at every version of each API. This runs without libFuzzer, so
//! a change to the harness, to the encoder or to the `arbitrary` generator that makes a version
//! unreachable (every input skipped) or breaks the byte check fails here.

use arbitrary::{Arbitrary, Unstructured};
use fuzz::{api_version, check_request};
use nisshi_sans_io::{
    AddPartitionsToTxnRequest, ApiKey, CreateTopicsRequest, DescribeConfigsRequest, FetchRequest,
    FindCoordinatorRequest, ListOffsetsRequest, MetadataRequest, ProduceRequest, Request,
    RootMessageMeta,
};
use nisshi_service::{CAPPED_API_VERSIONS, capped_range};

/// A xorshift64 stream: the same bytes on every run, so a failure reproduces.
fn bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;

    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[0]
        })
        .collect()
}

/// Checks `T` at every version a client can reach, with `inputs` arbitrary requests each.
///
/// A request is skipped only when it leaves a field unset that is mandatory at the version and
/// nullable only at later ones, and the generator makes that possible for a field nullable at the
/// latest version alone. So `may_skip` says which versions may skip, and every other input must
/// reach the byte comparison: a skip anywhere else is an encoder refusing a request it should
/// encode.
fn check_every_version<T>(inputs: u64, may_skip: impl Fn(i16) -> bool)
where
    T: Request + for<'a> Arbitrary<'a>,
{
    let routed = capped_range(T::KEY).unwrap_or_else(|| {
        let valid = &RootMessageMeta::messages()
            .requests()
            .get(&T::KEY)
            .expect("metadata")
            .version
            .valid;
        valid.start..=valid.end
    });
    let count = u16::try_from(routed.end() - routed.start() + 1).expect("a few versions");

    for selector in 0..count {
        let version = api_version::<T>(selector);
        assert_eq!(version, routed.start() + i16::try_from(selector).unwrap());

        let mut built = 0;
        let mut skipped = 0;

        for seed in 0..inputs {
            let data = bytes(seed, 4096);

            let Ok(request) = T::arbitrary(&mut Unstructured::new(&data)) else {
                continue;
            };

            built += 1;

            if check_request(request, version)
                .unwrap_or_else(|error| panic!("api key {} v{version}: {error}", T::KEY))
                .is_none()
            {
                skipped += 1;
            }
        }

        assert!(
            skipped == 0 || may_skip(version),
            "api key {} v{version}: {skipped} of {inputs} inputs have no encoding",
            T::KEY,
        );
        assert!(
            built > skipped,
            "api key {} v{version}: no input reached the byte comparison ({built} built, {skipped} skipped)",
            T::KEY,
        );
    }
}

#[test]
fn selector_covers_the_routed_versions() {
    let valid = &RootMessageMeta::messages()
        .requests()
        .get(&FetchRequest::KEY)
        .expect("metadata")
        .version
        .valid;
    let count = valid.end - valid.start + 1;

    for selector in 0..=u16::try_from(count).unwrap() * 2 {
        assert_eq!(
            api_version::<FetchRequest>(selector),
            valid.start + i16::try_from(selector).unwrap() % count,
        );
    }

    assert!(valid.start < 7, "Fetch before v7 is reachable");
}

/// The APIs that nisshi routes at fewer versions than the protocol has are fuzzed at the routed
/// ones only.
#[test]
fn selector_stays_in_a_capped_range() {
    for (api_key, range) in CAPPED_API_VERSIONS {
        let versions: Vec<i16> = (0..64)
            .map(|selector| match api_key {
                k if k == ProduceRequest::KEY => api_version::<ProduceRequest>(selector),
                k if k == ListOffsetsRequest::KEY => api_version::<ListOffsetsRequest>(selector),
                k if k == AddPartitionsToTxnRequest::KEY => {
                    api_version::<AddPartitionsToTxnRequest>(selector)
                }
                k => panic!("a new capped API {k}: add it here"),
            })
            .collect();

        assert_eq!(versions.iter().min(), Some(range.start()));
        assert_eq!(versions.iter().max(), Some(range.end()));
    }
}

#[test]
fn fetch() {
    check_every_version::<FetchRequest>(200, |_| false);
}

#[test]
fn produce() {
    check_every_version::<ProduceRequest>(200, |_| false);
}

/// `Topics` is nullable from v1 and a topic's `Name` from v10, so a fuzzed `None` has no
/// encoding at v0 and below v10 respectively.
#[test]
fn metadata() {
    check_every_version::<MetadataRequest>(200, |version| version < 10);
}

#[test]
fn find_coordinator() {
    check_every_version::<FindCoordinatorRequest>(200, |_| false);
}

#[test]
fn create_topics() {
    check_every_version::<CreateTopicsRequest>(200, |_| false);
}

#[test]
fn list_offsets() {
    check_every_version::<ListOffsetsRequest>(200, |_| false);
}

#[test]
fn describe_configs() {
    check_every_version::<DescribeConfigsRequest>(200, |_| false);
}
