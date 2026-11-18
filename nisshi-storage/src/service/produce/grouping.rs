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

//! Grouping of the entries of a Produce request that write to the same place, so that a request
//! writes each group's entries one after another and only different groups at once.

use std::{collections::HashMap, hash::Hash};

/// Groups `items` by `key`, with the groups in the order in which their keys first appear, and
/// each item paired with its position in `items`.
///
/// A client sends at most one entry for a partition, so a group usually holds one item. The
/// position lets the caller put a group's answers back in request order.
pub(super) fn by_key<K, V>(items: Vec<V>, key: impl Fn(&V) -> K) -> Vec<Vec<(usize, V)>>
where
    K: Eq + Hash,
{
    let mut groups: Vec<Vec<(usize, V)>> = Vec::new();
    let mut group_of = HashMap::new();

    for (position, item) in items.into_iter().enumerate() {
        let group = *group_of.entry(key(&item)).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });

        groups[group].push((position, item));
    }

    groups
}

/// Returns the answers of every group in request order.
pub(super) fn in_request_order<R>(answered: Vec<Vec<(usize, R)>>) -> Vec<R> {
    let mut answered = answered.into_iter().flatten().collect::<Vec<_>>();
    answered.sort_by_key(|(position, _)| *position);
    answered.into_iter().map(|(_, answer)| answer).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groups_keep_first_occurrence_order_and_positions() {
        let grouped = by_key(vec!["b", "a", "b", "c", "a"], |item| *item);

        assert_eq!(
            vec![
                vec![(0, "b"), (2, "b")],
                vec![(1, "a"), (4, "a")],
                vec![(3, "c")]
            ],
            grouped
        );
    }

    #[test]
    fn answers_come_back_in_request_order() {
        let answered = vec![
            vec![(3, "d"), (4, "e")],
            vec![(0, "a"), (2, "c")],
            vec![(1, "b")],
        ];

        assert_eq!(vec!["a", "b", "c", "d", "e"], in_request_order(answered));
    }

    #[test]
    fn nothing_groups_to_nothing() {
        assert!(by_key(Vec::<i32>::new(), |item| *item).is_empty());
        assert!(in_request_order(Vec::<Vec<(usize, i32)>>::new()).is_empty());
    }
}
