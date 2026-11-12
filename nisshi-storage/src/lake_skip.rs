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

//! The warning that a storage engine logs when it leaves a topic out of the data lake, because
//! the topic's records are not validated against its schema.

use std::{
    collections::BTreeSet,
    sync::{Mutex, PoisonError},
};

use tracing::warn;

/// The topics that [`warn_once`] has logged.
static WARNED: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());

/// Logs a warning that the engine does not write `topic` to the data lake, the first time that
/// the process calls it for `topic`.
///
/// An engine calls this for each batch that it leaves out of the lake. The process remembers
/// each topic, so the log has one line for a topic instead of one for each batch.
pub fn warn_once(topic: &str) {
    if first_call(topic) {
        warn!(
            topic,
            "the topic is not written to the data lake, because nisshi.schema.validation is false"
        );
    }
}

/// Returns whether this is the first call for `topic`.
fn first_call(topic: &str) -> bool {
    WARNED
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(topic.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_topic_is_warned_about_once() {
        assert!(first_call("first"));
        assert!(!first_call("first"));
        assert!(first_call("second"));
    }
}
