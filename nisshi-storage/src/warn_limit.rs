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

use std::{
    collections::HashMap,
    hash::Hash,
    sync::{LazyLock, Mutex, PoisonError},
};

use tokio::time::{Duration, Instant};

/// The shortest time between two `warn` lines for failures of one storage operation, or of one
/// request, that a retry may clear.
pub const STORAGE_FAILURE_WARN_INTERVAL: Duration = Duration::from_secs(10);

/// A limit on how often a failure that a retry may clear logs at `warn`: at most once in each
/// interval for each key.
///
/// A client retries a failed request as often as it likes, so a `warn` line for each failure
/// would flood the log while the store is down. The limit holds one entry for each key that has
/// warned, so a key comes from a bounded set, such as a storage operation or a request name.
#[derive(Debug)]
pub struct WarnLimit<K> {
    interval: Duration,
    last_warned: LazyLock<Mutex<HashMap<K, Instant>>>,
}

impl<K> WarnLimit<K>
where
    K: Eq + Hash,
{
    /// Returns a limit of one `warn` line in each `interval` for each key.
    #[must_use]
    pub const fn new(interval: Duration) -> Self {
        Self {
            interval,
            last_warned: LazyLock::new(Mutex::default),
        }
    }

    /// Returns whether a failure of `key` logs at `warn` now: when `key` has not warned within
    /// the interval. A `true` answer counts as the warning of `key`.
    pub fn warns_now(&self, key: K) -> bool {
        let now = Instant::now();
        let mut last_warned = self
            .last_warned
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        if last_warned
            .get(&key)
            .is_some_and(|last| now.duration_since(*last) < self.interval)
        {
            return false;
        }

        _ = last_warned.insert(key, now);
        true
    }
}
