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

//! The `operation` attribute that the libSQL engine records for its cleanup policies.

use opentelemetry::KeyValue;

/// A cleanup policy that the libSQL engine runs inside [`Storage::maintain`], as the `operation`
/// attribute of `nisshi_sqlite_delegate_request_duration` names it.
///
/// These policies are steps of the engine, not [`Storage`] methods, so they are not
/// [`StorageOperation`] variants. Dashboards and alerts select on these values, so a renamed
/// value is a breaking change for them.
///
/// [`Storage`]: nisshi_storage::Storage
/// [`Storage::maintain`]: nisshi_storage::Storage::maintain
/// [`StorageOperation`]: nisshi_storage::StorageOperation
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PolicyOperation {
    /// Compacts in [`CompactionMode::Single`](super::CompactionMode::Single).
    CompactSingle,
    /// Compacts in [`CompactionMode::Multi`](super::CompactionMode::Multi).
    CompactMulti,
    /// Deletes the batches that have passed the retention limits.
    Delete,
}

impl PolicyOperation {
    /// Returns the label value of this policy.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::CompactSingle => "policy_compact_single",
            Self::CompactMulti => "policy_compact_multi",
            Self::Delete => "policy_delete",
        }
    }

    /// Returns this policy as the `operation` attribute of a metric.
    pub(crate) fn attribute(self) -> KeyValue {
        KeyValue::new("operation", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins each label value, because dashboards and alerts select on it.
    #[test]
    fn as_str_is_the_label_value() {
        let expected = [
            (PolicyOperation::CompactSingle, "policy_compact_single"),
            (PolicyOperation::CompactMulti, "policy_compact_multi"),
            (PolicyOperation::Delete, "policy_delete"),
        ];

        for (operation, label) in expected {
            assert_eq!(label, operation.as_str());
        }
    }
}
