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

//! Waits for something that the broker does in the background.

use std::{
    thread,
    time::{Duration, Instant},
};

/// Calls `attempt` until it returns `Ok`, and returns that value. Panics with the last `Err` if
/// `attempt` hasn't returned `Ok` within `timeout`.
///
/// A test waits with this for something the broker does in the background, such as a maintenance
/// run. The test stops as soon as it sees the result, so `timeout` only limits a test that has
/// already failed.
#[track_caller]
pub fn wait_until<T>(timeout: Duration, mut attempt: impl FnMut() -> Result<T, String>) -> T {
    let deadline = Instant::now() + timeout;

    loop {
        match attempt() {
            Ok(value) => return value,
            Err(reason) if Instant::now() >= deadline => {
                panic!("not within {timeout:?}: {reason}");
            }
            Err(_) => thread::sleep(Duration::from_millis(500)),
        }
    }
}
