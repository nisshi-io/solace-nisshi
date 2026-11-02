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
    sync::{Arc, Mutex},
};

use tokio::sync::{Notify, futures::Notified};
use tracing::warn;

/// The notifier of each group that a request is waiting on.
pub(super) type ChangeMap = Arc<Mutex<BTreeMap<String, Arc<Notify>>>>;

/// A request's hold on a group's notifier. Dropping the last hold removes the group's entry from
/// the map, so the map keeps an entry only while a request waits on its group.
#[derive(Debug)]
pub(super) struct Subscription {
    changes: ChangeMap,
    group_id: String,
    notify: Arc<Notify>,
}

impl Subscription {
    pub(super) fn new(changes: &ChangeMap, group_id: &str, notify: Arc<Notify>) -> Self {
        Self {
            changes: changes.clone(),
            group_id: group_id.to_owned(),
            notify,
        }
    }

    pub(super) fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }
}

impl Drop for Subscription {
    /// Gives up this hold while the map is locked. A hold is handed out under the same lock, so
    /// when only the map's own hold remains, no request waits on the entry.
    fn drop(&mut self) {
        let Ok(mut changes) = self.changes.lock() else {
            warn!(group_id = %self.group_id, "the notifier map is poisoned, so its entry stays");
            return;
        };

        // The swap releases this hold while the map is locked. Releasing it after the lock, as the
        // field's own drop would, lets two subscriptions that drop together each count the other's
        // hold and both keep the entry.
        drop(std::mem::take(&mut self.notify));

        if changes
            .get(&self.group_id)
            .is_some_and(|notify| Arc::strong_count(notify) == 1)
        {
            _ = changes.remove(&self.group_id);
        }
    }
}
