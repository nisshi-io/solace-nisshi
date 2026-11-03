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

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use tokio::time::{Duration, Instant};

/// How long a refused name is not sent to `CreateTopics` again.
pub(crate) const REFUSAL_TTL: Duration = Duration::from_secs(5);

/// The most refused names remembered at once.
const REFUSAL_CAPACITY: usize = 1_000;

/// The names that Metadata auto-creation refused, each remembered for a few seconds. Clones
/// share one store, so a broker shares it across its connections.
///
/// A producer retries Metadata for a name that collides with an existing topic, for example
/// `a_b` while `a.b` exists, about once a second. Each retry would send the name to
/// `CreateTopics` again, and its collision check lists every topic. Remembering the refusal
/// skips those listings for 5 s.
///
/// Kafka 3.9.1 checks a collision against an index that its controller keeps exact as topics
/// are created ([`replay`]) and deleted ([`deleteTopic`]), so once `a.b` is deleted, the next
/// retry creates `a_b`. A stateless broker has no such index. So a name stays refused for up to
/// 5 s after the topic it collides with is deleted. A refusal can only last too long: it never
/// lets a colliding topic be created. The client sees `UNKNOWN_TOPIC_OR_PARTITION` either way,
/// and keeps retrying.
///
/// Kafka trunk caches auto-creation errors the same way in [`ExpiringErrorCache`], with room for
/// 1,000 names, evicting the entry that expires soonest. It has used the cache to back off
/// Streams internal topic creation since [apache/kafka@2f260d79], keeping an error for twice the
/// Streams group heartbeat interval (10 s by default). Nisshi's 5 s is the stricter choice.
///
/// [`replay`]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java#L442-L449
/// [`deleteTopic`]: https://github.com/apache/kafka/blob/3.9.1/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java#L552-L561
/// [`ExpiringErrorCache`]: https://github.com/apache/kafka/commit/8ba41a2d0dfac115422ff8eb485a7ce00dd7ff84
/// [apache/kafka@2f260d79]: https://github.com/apache/kafka/commit/2f260d79e7f5f8578048922283dd5961a23f045a
#[derive(Clone, Debug, Default)]
pub struct AutoCreateRefusals(Arc<Mutex<BTreeMap<String, Instant>>>);

impl AutoCreateRefusals {
    /// Returns whether auto-creation refused `name` less than `REFUSAL_TTL` ago.
    pub(crate) fn is_refused(&self, name: &str) -> bool {
        let now = Instant::now();
        self.expiries()
            .get(name)
            .is_some_and(|expires| *expires > now)
    }

    /// Remembers that auto-creation refused `name`.
    pub(crate) fn refuse(&self, name: &str) {
        let now = Instant::now();
        let mut expiries = self.expiries();

        expiries.retain(|_, expires| *expires > now);

        if expiries.len() >= REFUSAL_CAPACITY && !expiries.contains_key(name) {
            let soonest = expiries
                .iter()
                .min_by_key(|(_, expires)| **expires)
                .map(|(name, _)| name.clone());

            if let Some(soonest) = soonest {
                _ = expiries.remove(&soonest);
            }
        }

        _ = expiries.insert(name.to_owned(), now + REFUSAL_TTL);
    }

    fn expiries(&self) -> MutexGuard<'_, BTreeMap<String, Instant>> {
        // Each write leaves the map valid, so a panic while it was held leaves nothing to undo.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{Duration, advance};

    use super::{AutoCreateRefusals, REFUSAL_CAPACITY, REFUSAL_TTL};

    #[tokio::test(start_paused = true)]
    async fn a_refusal_expires() {
        let refusals = AutoCreateRefusals::default();
        refusals.refuse("a_b");

        advance(REFUSAL_TTL - Duration::from_millis(1)).await;
        assert!(refusals.is_refused("a_b"));
        assert!(!refusals.is_refused("c_d"));

        advance(Duration::from_millis(1)).await;
        assert!(!refusals.is_refused("a_b"));
    }

    /// A burst of distinct refused names cannot grow the store past its capacity. It evicts the
    /// refusal that expires soonest.
    #[tokio::test(start_paused = true)]
    async fn refusals_are_bounded() {
        let refusals = AutoCreateRefusals::default();

        for index in 0..=REFUSAL_CAPACITY {
            refusals.refuse(&format!("t_{index}"));
            advance(Duration::from_micros(1)).await;
        }

        assert_eq!(REFUSAL_CAPACITY, refusals.expiries().len());
        assert!(!refusals.is_refused("t_0"));
        assert!(refusals.is_refused("t_1"));
        assert!(refusals.is_refused(&format!("t_{REFUSAL_CAPACITY}")));
    }
}
