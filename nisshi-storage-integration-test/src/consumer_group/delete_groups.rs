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

use std::{assert_matches, collections::BTreeMap, slice, time::Duration};

use crate::{
    group::{self, Formed},
    random,
};
use nisshi_broker::{
    Error,
    coordinator::group::{Coordinator, administrator::Controller},
};
use nisshi_sans_io::{
    DeleteGroupsRequest, DeleteGroupsResponse, ErrorCode, RequestInput,
    create_topics_request::CreatableTopic,
};
use nisshi_storage::{
    DeleteGroupsService, GroupDetail, GroupDetailResponse, OffsetCommitRequest, Storage, Topition,
};
use rama::{Service, extensions::Extensions};
use rand::{RngExt as _, rng};
use tokio::time::sleep;
use unreadable_groups::UnreadableGroups;

/// Kafka's answer to a delete of a group that was never stored.
const NOT_FOUND: &[ErrorCode] = &[ErrorCode::GroupIdNotFound];

/// Kafka's answer to a delete of a group that was never stored, and the `NONE` that the
/// in-memory engine answers instead.
const NONE_OR_NOT_FOUND: &[ErrorCode] = &[ErrorCode::None, ErrorCode::GroupIdNotFound];

/// A consumer joined to a group blocks its deletion (`NON_EMPTY_GROUP`), and
/// the group's committed offsets survive the refused attempt; once the
/// consumer leaves, the now-empty group deletes cleanly and its offsets are
/// gone. This exercises the coordinator-level route
/// (`Coordinator::delete_groups`), not the raw storage primitive the other
/// tests in this file use.
pub async fn non_empty_group_then_empty(storage: impl Storage + Clone) -> Result<(), Error> {
    let mut coordinator = Controller::with_storage(storage.clone())?;

    let Formed {
        group, member_id, ..
    } = group::form_group(&coordinator)
        .await
        .map_err(|err| Error::Message(err.to_string()))?;

    let topic_name = random::alphanumeric_string(15);
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(topic_name, 0);
    let offset = rng().random_range(0..i64::MAX);

    let commit = storage
        .offset_commit(
            &group,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;
    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let response: DeleteGroupsResponse =
        coordinator
            .delete_groups(slice::from_ref(&group))
            .await
            .and_then(|body| TryInto::try_into(body).map_err(Into::into))?;

    let results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());
    assert_eq!(group, results[0].group_id);
    assert_eq!(
        ErrorCode::NonEmptyGroup,
        ErrorCode::try_from(results[0].error_code)?
    );

    // Refused delete must not have touched storage: the offset is still
    // there.
    let offset_fetch = storage
        .offset_fetch(Some(&group), slice::from_ref(&topition), None)
        .await?;
    assert_eq!(
        Some(offset),
        offset_fetch
            .get(&topition)
            .map(|committed| committed.offset)
    );

    _ = group::leave(&mut coordinator, &group, &member_id, None).await?;

    let response: DeleteGroupsResponse =
        coordinator
            .delete_groups(slice::from_ref(&group))
            .await
            .and_then(|body| TryInto::try_into(body).map_err(Into::into))?;

    let results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());
    assert_eq!(group, results[0].group_id);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);

    let offset_fetch = storage
        .offset_fetch(Some(&group), slice::from_ref(&topition), None)
        .await?;
    assert_ne!(
        Some(offset),
        offset_fetch
            .get(&topition)
            .map(|committed| committed.offset)
    );

    Ok(())
}

/// Forms a group through `coordinator`, has its only member leave, and
/// deletes the group, which must succeed. Returns the group's id.
async fn delete_empty_group<G>(coordinator: &mut Controller<G>) -> Result<String, Error>
where
    G: Storage + Clone,
{
    let Formed {
        group, member_id, ..
    } = group::form_group(coordinator)
        .await
        .map_err(|err| Error::Message(err.to_string()))?;

    _ = group::leave(coordinator, &group, &member_id, None).await?;

    let response: DeleteGroupsResponse =
        coordinator
            .delete_groups(slice::from_ref(&group))
            .await
            .and_then(|body| TryInto::try_into(body).map_err(Into::into))?;

    let results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());
    assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);

    Ok(group)
}

/// A group that the coordinator deletes is gone from storage: storage
/// describes it with `GROUP_ID_NOT_FOUND`.
pub async fn deleted_group_is_not_found(storage: impl Storage + Clone) -> Result<(), Error> {
    let mut coordinator = Controller::with_storage(storage.clone())?;
    let group = delete_empty_group(&mut coordinator).await?;

    let described = storage
        .describe_groups(Some(slice::from_ref(&group)), false)
        .await?;
    assert_eq!(1, described.len());
    assert_matches!(
        described[0].response,
        GroupDetailResponse::ErrorCode(ErrorCode::GroupIdNotFound)
    );

    Ok(())
}

/// Deleting a group must forget whatever the coordinator cached for it, or
/// a brand-new member joining under the same, just-deleted group name would
/// silently reuse the deleted group's stale state instead of starting a
/// genuinely new one. On Postgres and libSQL this isn't hypothetical: the
/// conditional group-detail write is an upsert whose `where e_tag = ...`
/// guard only applies to the UPDATE arm, so once the row is gone a stale
/// cached version doesn't fail the write, it just inserts - the rejoin
/// succeeds either way, with or without this fix, so a bare "the join call
/// didn't error" assertion proves nothing on those two backends. The
/// reliable signal is whether the rejoin actually started a fresh
/// generation or continued the deleted group's: compare it against a
/// genuinely new control group joined the same way.
pub async fn delete_evicts_coordinator_cache(storage: impl Storage + Clone) -> Result<(), Error> {
    let mut coordinator = Controller::with_storage(storage)?;
    let group = delete_empty_group(&mut coordinator).await?;

    // A short timeout: the coordinator's own join-rebalance wait sleeps
    // until half the session timeout has elapsed before closing a solo
    // dynamic join (see `administrator.rs`'s `join` loop), and this test
    // doesn't need to exercise that wait.
    let session_timeout_ms = 1_000;

    let rejoined = group::join(
        &mut coordinator,
        &group,
        None,
        None,
        None,
        session_timeout_ms,
        Some(300_000),
    )
    .await?;

    let control_group = random::alphanumeric_string(15);
    let control = group::join(
        &mut coordinator,
        &control_group,
        None,
        None,
        None,
        session_timeout_ms,
        Some(300_000),
    )
    .await?;

    assert_eq!(
        control.generation(),
        rejoined.generation(),
        "rejoin under a just-deleted group name must start the same first \
         generation as a brand-new group, not continue the deleted group's"
    );

    Ok(())
}

/// Member expiry in this codebase is lazy: a member that misses its
/// heartbeat is only evicted when some other request for the same group
/// happens to arrive. `DeleteGroups` must apply that same eviction itself
/// (via `missed_heartbeat`) before deciding a group is non-empty, or a
/// consumer that died without `LeaveGroup` would make its group
/// permanently undeletable.
pub async fn delete_after_session_expiry(storage: impl Storage + Clone) -> Result<(), Error> {
    let mut coordinator = Controller::with_storage(storage.clone())?;

    let group_id = random::alphanumeric_string(15);
    let session_timeout_ms = 500;

    let joined = group::join(
        &mut coordinator,
        &group_id,
        None,
        None,
        None,
        session_timeout_ms,
        Some(300_000),
    )
    .await
    .map_err(|err| Error::Message(err.to_string()))?;
    assert!(joined.is_leader());

    sleep(Duration::from_millis(
        u64::try_from(session_timeout_ms).unwrap_or(500) + 200,
    ))
    .await;

    let response: DeleteGroupsResponse = coordinator
        .delete_groups(slice::from_ref(&group_id))
        .await
        .and_then(|body| TryInto::try_into(body).map_err(Into::into))?;

    let results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());
    assert_eq!(group_id, results[0].group_id);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);

    Ok(())
}

/// A group that only ever committed offsets (no `JoinGroup` ever happened)
/// is deletable. `delete_groups` reads it through
/// `Storage::describe_groups`, which must treat this group's NULL `detail`
/// column on the SQL backends (a group row with no detail row) as empty,
/// not an error.
pub async fn delete_offsets_only_group(storage: impl Storage + Clone) -> Result<(), Error> {
    let coordinator = Controller::with_storage(storage.clone())?;

    let topic_name = random::alphanumeric_string(15);
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(topic_name, 0);
    let group_id = random::alphanumeric_string(15);
    let offset = rng().random_range(0..i64::MAX);

    let commit = storage
        .offset_commit(
            &group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;
    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let response: DeleteGroupsResponse = coordinator
        .delete_groups(slice::from_ref(&group_id))
        .await
        .and_then(|body| TryInto::try_into(body).map_err(Into::into))?;

    let results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());
    assert_eq!(group_id, results[0].group_id);
    assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);

    Ok(())
}

/// Commits one offset for `group_id` on a new single-partition topic, and
/// returns the partition and the offset.
async fn commit_offset(storage: &impl Storage, group_id: &str) -> Result<(Topition, i64), Error> {
    let topic_name = random::alphanumeric_string(15);
    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(topic_name, 0);
    let offset = rng().random_range(0..i64::MAX);

    let commit = storage
        .offset_commit(
            group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;
    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    Ok((topition, offset))
}

async fn committed_offset(
    storage: &impl Storage,
    group_id: &str,
    topition: &Topition,
) -> Result<Option<i64>, Error> {
    storage
        .offset_fetch(Some(group_id), slice::from_ref(topition), None)
        .await
        .map(|offsets| offsets.get(topition).map(|committed| committed.offset))
        .map_err(Into::into)
}

async fn delete(
    coordinator: &impl Coordinator,
    group_ids: &[String],
) -> Result<Vec<(String, ErrorCode)>, Error> {
    let response: DeleteGroupsResponse = coordinator
        .delete_groups(group_ids)
        .await
        .and_then(|body| TryInto::try_into(body).map_err(Into::into))?;

    response
        .results
        .unwrap_or_default()
        .into_iter()
        .map(|result| {
            ErrorCode::try_from(result.error_code)
                .map(|error_code| (result.group_id, error_code))
                .map_err(Into::into)
        })
        .collect()
}

/// A broker that has not cached a group reads the group from storage, and
/// refuses to delete it while a member that joined through another broker
/// is live. The member's next heartbeat through that broker succeeds.
pub async fn cache_miss_with_live_member(storage: impl Storage + Clone) -> Result<(), Error> {
    let a = Controller::with_storage(storage.clone())?;

    let Formed {
        group,
        generation_id,
        member_id,
    } = group::form_group(&a)
        .await
        .map_err(|err| Error::Message(err.to_string()))?;

    let (topition, offset) = commit_offset(&storage, &group).await?;

    let mut b = Controller::with_storage(storage.clone())?;

    assert_eq!(
        vec![(group.clone(), ErrorCode::NonEmptyGroup)],
        delete(&b, slice::from_ref(&group)).await?
    );

    assert_eq!(
        Some(offset),
        committed_offset(&storage, &group, &topition).await?
    );

    let heartbeat = group::heartbeat(&mut b, &group, generation_id, &member_id, None).await?;
    assert_eq!(ErrorCode::None, ErrorCode::try_from(heartbeat.error_code)?);

    Ok(())
}

/// A broker whose cache holds a group as empty still refuses to delete the
/// group after a member joins it through another broker, because the
/// decision comes from storage.
pub async fn stale_cache_on_another_broker(storage: impl Storage + Clone) -> Result<(), Error> {
    let mut a = Controller::with_storage(storage.clone())?;

    let Formed {
        group, member_id, ..
    } = group::form_group(&a)
        .await
        .map_err(|err| Error::Message(err.to_string()))?;

    _ = group::leave(&mut a, &group, &member_id, None).await?;

    let (topition, offset) = commit_offset(&storage, &group).await?;

    let mut b = Controller::with_storage(storage.clone())?;
    _ = group::join(&mut b, &group, None, None, None, 30_000, Some(300_000)).await?;

    assert_eq!(
        vec![(group.clone(), ErrorCode::NonEmptyGroup)],
        delete(&a, slice::from_ref(&group)).await?
    );

    assert_eq!(
        Some(offset),
        committed_offset(&storage, &group, &topition).await?
    );

    Ok(())
}

/// A group whose state the coordinator cannot read is not deleted, and the
/// response carries the storage error for that group.
pub async fn storage_read_error(storage: impl Storage + Clone) -> Result<(), Error> {
    let mut coordinator = Controller::with_storage(storage.clone())?;

    let Formed {
        group, member_id, ..
    } = group::form_group(&coordinator)
        .await
        .map_err(|err| Error::Message(err.to_string()))?;

    _ = group::leave(&mut coordinator, &group, &member_id, None).await?;

    let (topition, offset) = commit_offset(&storage, &group).await?;

    let unreadable = Controller::with_storage(UnreadableGroups {
        storage: storage.clone(),
    })?;

    assert_eq!(
        vec![(group.clone(), ErrorCode::UnknownServerError)],
        delete(&unreadable, slice::from_ref(&group)).await?
    );

    assert_eq!(
        Some(offset),
        committed_offset(&storage, &group, &topition).await?
    );

    Ok(())
}

/// One request through the coordinator that names a non-empty group, an
/// empty group (twice), a group that does not exist, and the empty group
/// id. Each distinct id gets one result, and only the empty group loses its
/// committed offset. Kafka answers the group that does not exist and the empty
/// group id with `GROUP_ID_NOT_FOUND`, which
/// `unknown_group_in_mixed_request_is_not_found` and
/// `empty_group_id_in_mixed_request_is_not_found` check. This case also accepts
/// `NONE`, which the in-memory engine answers for a group that it never stored.
pub async fn mixed_request(storage: impl Storage + Clone) -> Result<(), Error> {
    mixed_request_answering(storage, NONE_OR_NOT_FOUND, NONE_OR_NOT_FOUND).await
}

/// The request of `mixed_request` answers the group that does not exist with
/// `GROUP_ID_NOT_FOUND`, as Kafka does, also when the same request deletes
/// another group.
pub async fn unknown_group_in_mixed_request_is_not_found(
    storage: impl Storage + Clone,
) -> Result<(), Error> {
    mixed_request_answering(storage, NOT_FOUND, NONE_OR_NOT_FOUND).await
}

/// The request of `mixed_request` answers the empty group id with
/// `GROUP_ID_NOT_FOUND`, as Kafka does, also when the same request deletes
/// another group.
pub async fn empty_group_id_in_mixed_request_is_not_found(
    storage: impl Storage + Clone,
) -> Result<(), Error> {
    mixed_request_answering(storage, NONE_OR_NOT_FOUND, NOT_FOUND).await
}

/// Sends the request of `mixed_request`, and checks that the group that does not
/// exist answers one of `for_unknown` and the empty group id one of
/// `for_empty_id`.
async fn mixed_request_answering(
    storage: impl Storage + Clone,
    for_unknown: &[ErrorCode],
    for_empty_id: &[ErrorCode],
) -> Result<(), Error> {
    let mut coordinator = Controller::with_storage(storage.clone())?;

    let non_empty = group::form_group(&coordinator)
        .await
        .map_err(|err| Error::Message(err.to_string()))?
        .group;
    let (non_empty_topition, non_empty_offset) = commit_offset(&storage, &non_empty).await?;

    let Formed {
        group: empty,
        member_id,
        ..
    } = group::form_group(&coordinator)
        .await
        .map_err(|err| Error::Message(err.to_string()))?;
    _ = group::leave(&mut coordinator, &empty, &member_id, None).await?;
    let (empty_topition, empty_offset) = commit_offset(&storage, &empty).await?;

    let unknown = random::alphanumeric_string(15);

    let results = delete(
        &coordinator,
        &[
            non_empty.clone(),
            empty.clone(),
            unknown.clone(),
            String::new(),
            empty.clone(),
        ],
    )
    .await?;
    assert_eq!(4, results.len(), "{results:?}");

    let result_for = |group_id: &str| {
        results
            .iter()
            .find(|(id, _)| id == group_id)
            .map(|(_, error_code)| *error_code)
    };

    assert_eq!(Some(ErrorCode::NonEmptyGroup), result_for(&non_empty));
    assert_eq!(Some(ErrorCode::None), result_for(&empty));
    assert!(
        result_for(&unknown).is_some_and(|error_code| for_unknown.contains(&error_code)),
        "expected one of {for_unknown:?} for the unknown group in {results:?}"
    );
    assert!(
        result_for("").is_some_and(|error_code| for_empty_id.contains(&error_code)),
        "expected one of {for_empty_id:?} for the empty group id in {results:?}"
    );

    assert_eq!(
        Some(non_empty_offset),
        committed_offset(&storage, &non_empty, &non_empty_topition).await?
    );
    assert_ne!(
        Some(empty_offset),
        committed_offset(&storage, &empty, &empty_topition).await?
    );

    Ok(())
}

pub async fn delete_non_existent(storage: impl Storage + Clone) -> Result<(), Error> {
    let service = DeleteGroupsService {
        storage: storage.clone(),
    };

    let group_id = random::alphanumeric_string(15);

    let response = service
        .serve(RequestInput {
            request: DeleteGroupsRequest::default().groups_names(Some([group_id.clone()].into())),
            extensions: Extensions::default(),
        })
        .await?;

    let results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());
    assert_eq!(group_id, results[0].group_id);

    assert_matches!(
        ErrorCode::try_from(results[0].error_code)?,
        ErrorCode::None | ErrorCode::GroupIdNotFound
    );

    Ok(())
}

/// A DeleteGroups request through the coordinator answers a group that was
/// never stored with `GROUP_ID_NOT_FOUND`, as [Kafka's `handleDeleteGroups`]
/// does.
///
/// [Kafka's `handleDeleteGroups`]: https://github.com/apache/kafka/blob/3.9.1/core/src/main/scala/kafka/coordinator/group/GroupCoordinator.scala
pub async fn unknown_group_is_not_found(storage: impl Storage + Clone) -> Result<(), Error> {
    let coordinator = Controller::with_storage(storage)?;
    let unknown = random::alphanumeric_string(15);

    assert_eq!(
        vec![(unknown.clone(), ErrorCode::GroupIdNotFound)],
        delete(&coordinator, slice::from_ref(&unknown)).await?
    );

    Ok(())
}

/// A DeleteGroups request answers the empty group id with `GROUP_ID_NOT_FOUND`,
/// through the coordinator and through the storage's `DeleteGroupsService`,
/// as Kafka does for any group that was never stored.
pub async fn empty_group_id_is_not_found(storage: impl Storage + Clone) -> Result<(), Error> {
    let coordinator = Controller::with_storage(storage.clone())?;

    assert_eq!(
        vec![(String::new(), ErrorCode::GroupIdNotFound)],
        delete(&coordinator, &[String::new()]).await?
    );

    let response = DeleteGroupsService { storage }
        .serve(RequestInput {
            request: DeleteGroupsRequest::default().groups_names(Some([String::new()].into())),
            extensions: Extensions::default(),
        })
        .await?;
    let results = response.results.unwrap_or_default();
    assert_eq!(1, results.len());
    assert_eq!(
        ErrorCode::GroupIdNotFound,
        ErrorCode::try_from(results[0].error_code)?
    );

    Ok(())
}

/// A DeleteGroups request naming an empty group id alongside a real one must
/// never touch groups that were not named. On dynostore an empty id used to
/// widen the delete's key prefix to every group, wiping all group state and
/// committed offsets.
///
/// Kafka still accepts `""` in DeleteGroups for backwards compatibility, and
/// answers it with `GROUP_ID_NOT_FOUND`, which
/// `empty_group_id_in_mixed_list_is_not_found` checks. This case also accepts
/// `NONE`: the in-memory engine answers `NONE`, because deleting an absent
/// object store key succeeds, as an S3 `DeleteObject` does.
pub async fn empty_group_id_mixed_list(storage: impl Storage + Clone) -> Result<(), Error> {
    empty_group_id_mixed_list_answering(storage, NONE_OR_NOT_FOUND).await
}

/// The request of `empty_group_id_mixed_list` answers the empty group id with
/// `GROUP_ID_NOT_FOUND`, as Kafka does, also when the same request deletes
/// another group.
pub async fn empty_group_id_in_mixed_list_is_not_found(
    storage: impl Storage + Clone,
) -> Result<(), Error> {
    empty_group_id_mixed_list_answering(storage, NOT_FOUND).await
}

/// Sends the request of `empty_group_id_mixed_list`, and checks that the empty
/// group id answers one of `for_empty_id`.
async fn empty_group_id_mixed_list_answering(
    storage: impl Storage + Clone,
    for_empty_id: &[ErrorCode],
) -> Result<(), Error> {
    let topic_name = random::alphanumeric_string(15);
    let num_partitions = 6;

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(num_partitions)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(topic_name, 0);

    let group_a = random::alphanumeric_string(15);
    let group_b = random::alphanumeric_string(15);

    let offset_a = rng().random_range(0..i64::MAX);
    let offset_b = rng().random_range(0..i64::MAX);

    for (group_id, offset) in [(&group_a, offset_a), (&group_b, offset_b)] {
        let commit = storage
            .offset_commit(
                group_id,
                None,
                &[(
                    topition.clone(),
                    OffsetCommitRequest::default().offset(offset),
                )],
            )
            .await?;

        assert_eq!(1, commit.len());
        assert_eq!(ErrorCode::None, commit[0].1);
    }

    // group_a must still be describable after the delete, which (on the SQL
    // backends) requires a persisted GroupDetail row, not just committed
    // offsets - establish one the same way a real join/sync would.
    _ = storage
        .update_group(&group_a, GroupDetail::default(), None)
        .await
        .map_err(|err| Error::Message(format!("update_group: {err:?}")))?;

    let service = DeleteGroupsService {
        storage: storage.clone(),
    };

    // group_a is never named: only "" and group_b are in the request.
    let response = service
        .serve(RequestInput {
            request: DeleteGroupsRequest::default()
                .groups_names(Some(["".into(), group_b.clone()].into())),
            extensions: Extensions::default(),
        })
        .await?;

    let results = response.results.unwrap_or_default();
    assert_eq!(2, results.len());

    let empty_result = results
        .iter()
        .find(|result| result.group_id.is_empty())
        .expect("missing result for the empty group id");
    let empty_error_code = ErrorCode::try_from(empty_result.error_code)?;
    assert!(
        for_empty_id.contains(&empty_error_code),
        "expected one of {for_empty_id:?} for the empty group id, got {empty_error_code:?}"
    );

    let group_b_result = results
        .iter()
        .find(|result| result.group_id == group_b)
        .expect("missing result for group_b");
    assert_eq!(
        ErrorCode::None,
        ErrorCode::try_from(group_b_result.error_code)?
    );

    // group_b was named, so its committed offset must be gone. On dynostore
    // a NONE result alone doesn't prove this: deleting a missing key is Ok.
    let offset_fetch = storage
        .offset_fetch(Some(&group_b), slice::from_ref(&topition), None)
        .await?;
    assert_ne!(
        Some(offset_b),
        offset_fetch
            .get(&topition)
            .map(|committed| committed.offset)
    );

    // group_a was never named in the request, so its committed offset must
    // still be fetchable, and it must still be listable/describable.
    let offset_fetch = storage
        .offset_fetch(Some(&group_a), slice::from_ref(&topition), None)
        .await?;
    assert_eq!(
        Some(offset_a),
        offset_fetch
            .get(&topition)
            .map(|committed| committed.offset)
    );

    let groups = storage.list_groups().await?;
    assert!(groups.iter().any(|group| group.name == group_a));

    let described = storage
        .describe_groups(Some(slice::from_ref(&group_a)), false)
        .await?;
    assert_eq!(1, described.len());
    assert_eq!(group_a, described[0].name);
    assert!(matches!(
        described[0].response,
        GroupDetailResponse::Found(_)
    ));

    Ok(())
}

/// The generation of `group_id`'s stored group state, or `None` when
/// `describe_groups` finds no state for it.
async fn described_generation(
    storage: &(impl Storage + Clone),
    group_id: &str,
) -> Result<Option<i32>, Error> {
    let described = storage
        .describe_groups(Some(&[group_id.to_owned()]), false)
        .await?;
    assert_eq!(1, described.len());
    assert_eq!(group_id, described[0].name);

    Ok(match &described[0].response {
        GroupDetailResponse::Found(detail) => Some(detail.generation_id),
        GroupDetailResponse::ErrorCode(_) => None,
    })
}

/// `Storage::delete_groups` has callers other than `DeleteGroupsService`, and
/// the service passes every id through unchanged, so every group id,
/// including `""`, `"/"`, `"//"`, `"a/"` and the literal string `"%empty"`,
/// must be its own independently addressable group.
///
/// This gives each id (plus a baseline id, `"a"`) a distinct committed offset
/// and a distinct group state (its own generation), confirms each reads back
/// its own, then deletes them one at a time, never as a batch, which could
/// mask a collision. After each delete, only the deleted group's offset and
/// state are gone, and every other group still reads back its own.
pub async fn slash_and_empty_group_ids_are_distinct_groups(
    storage: impl Storage + Clone,
) -> Result<(), Error> {
    let topic_name = random::alphanumeric_string(15);

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(topic_name, 0);

    let group_ids = [
        "a".to_string(),
        String::new(),
        "/".to_string(),
        "//".to_string(),
        "a/".to_string(),
        "%empty".to_string(),
    ];

    let offsets = group_ids
        .iter()
        .map(|_| rng().random_range(0..i64::MAX))
        .collect::<Vec<_>>();

    for (group_id, offset) in group_ids.iter().zip(&offsets) {
        let commit = storage
            .offset_commit(
                group_id,
                None,
                &[(
                    topition.clone(),
                    OffsetCommitRequest::default().offset(*offset),
                )],
            )
            .await?;

        assert_eq!(1, commit.len());
        assert_eq!(ErrorCode::None, commit[0].1);
    }

    for (generation_id, group_id) in (1..).zip(&group_ids) {
        _ = storage
            .update_group(
                group_id,
                GroupDetail {
                    generation_id,
                    ..GroupDetail::default()
                },
                None,
            )
            .await
            .map_err(|err| Error::Message(format!("update_group {group_id:?}: {err:?}")))?;
    }

    for (generation_id, (group_id, offset)) in (1..).zip(group_ids.iter().zip(&offsets)) {
        let offset_fetch = storage
            .offset_fetch(Some(group_id), slice::from_ref(&topition), None)
            .await?;
        assert_eq!(
            Some(offset),
            offset_fetch
                .get(&topition)
                .map(|committed| &committed.offset),
            "group {group_id:?} fetched the wrong committed offset"
        );

        assert_eq!(
            Some(generation_id),
            described_generation(&storage, group_id).await?,
            "group {group_id:?} described the wrong group state"
        );
    }

    for (deleted_index, group_id) in group_ids.iter().enumerate() {
        let results = storage
            .delete_groups(Some(slice::from_ref(group_id)))
            .await?;

        assert_eq!(1, results.len());
        assert_eq!(ErrorCode::None, ErrorCode::try_from(results[0].error_code)?);

        for (other_index, (other_group_id, other_offset)) in
            group_ids.iter().zip(&offsets).enumerate()
        {
            let offset_fetch = storage
                .offset_fetch(Some(other_group_id), slice::from_ref(&topition), None)
                .await?;
            let other_generation = i32::try_from(other_index + 1).expect("few groups");
            let described = described_generation(&storage, other_group_id).await?;

            if other_index <= deleted_index {
                // Backends answer a deleted group differently; what matters
                // is that its committed offset and state are gone.
                assert_ne!(
                    Some(other_offset),
                    offset_fetch
                        .get(&topition)
                        .map(|committed| &committed.offset),
                    "group {other_group_id:?} offset should have been deleted by now (deleting {group_id:?})"
                );
                assert_ne!(
                    Some(other_generation),
                    described,
                    "group {other_group_id:?} state should have been deleted by now (deleting {group_id:?})"
                );
            } else {
                assert_eq!(
                    Some(other_offset),
                    offset_fetch
                        .get(&topition)
                        .map(|committed| &committed.offset),
                    "group {other_group_id:?} offset should still be intact after deleting {group_id:?}"
                );
                assert_eq!(
                    Some(other_generation),
                    described,
                    "group {other_group_id:?} state should still be intact after deleting {group_id:?}"
                );
            }
        }
    }

    Ok(())
}

/// `committed_offset_topitions` parses the topic and partition from each key
/// below the group's offsets prefix. A group id containing `/` must not shift
/// those segments, which would turn the partition parse into a
/// `ParseIntError`.
pub async fn slash_in_group_id_does_not_break_committed_offset_topitions(
    storage: impl Storage + Clone,
) -> Result<(), Error> {
    let topic_name = random::alphanumeric_string(15);

    _ = storage
        .create_topic(
            CreatableTopic::default()
                .name(topic_name.clone())
                .num_partitions(1)
                .replication_factor(0)
                .assignments(Some([].into()))
                .configs(Some([].into())),
            false,
        )
        .await?;

    let topition = Topition::new(topic_name, 0);
    let group_id = "a/b";
    let offset = rng().random_range(0..i64::MAX);

    let commit = storage
        .offset_commit(
            group_id,
            None,
            &[(
                topition.clone(),
                OffsetCommitRequest::default().offset(offset),
            )],
        )
        .await?;
    assert_eq!(1, commit.len());
    assert_eq!(ErrorCode::None, commit[0].1);

    let topitions = storage.committed_offset_topitions(group_id).await?;
    assert_eq!(
        Some(offset),
        topitions.get(&topition).map(|committed| committed.offset)
    );

    Ok(())
}

/// `delete_topic` removes a topic's committed offsets by skipping one
/// segment, the group id, in each key below `consumers/` and matching the
/// rest against `offsets/{topic}/partitions/`. That holds only while a group
/// id is one segment: interpolated raw, "a/b" took two segments and ""
/// none, so those groups kept their offsets for a deleted topic.
pub async fn delete_topic_removes_offsets_of_slash_and_empty_group_ids(
    storage: impl Storage + Clone,
) -> Result<(), Error> {
    let deleted_name = random::alphanumeric_string(15);
    let kept_name = random::alphanumeric_string(15);

    for name in [deleted_name.clone(), kept_name.clone()] {
        _ = storage
            .create_topic(
                CreatableTopic::default()
                    .name(name)
                    .num_partitions(1)
                    .replication_factor(0)
                    .assignments(Some([].into()))
                    .configs(Some([].into())),
                false,
            )
            .await?;
    }

    let deleted = Topition::new(deleted_name.clone(), 0);
    let kept = Topition::new(kept_name, 0);

    let group_ids = ["a/b", "", "a"];
    let offsets = group_ids.map(|_| rng().random_range(0..i64::MAX));

    for (group_id, offset) in group_ids.iter().zip(&offsets) {
        let commit = storage
            .offset_commit(
                group_id,
                None,
                &[
                    (
                        deleted.clone(),
                        OffsetCommitRequest::default().offset(*offset),
                    ),
                    (kept.clone(), OffsetCommitRequest::default().offset(*offset)),
                ],
            )
            .await?;

        assert_eq!(2, commit.len());
        assert!(
            commit.iter().all(|(_, code)| *code == ErrorCode::None),
            "{commit:?}"
        );
    }

    assert_eq!(
        ErrorCode::None,
        storage.delete_topic(&deleted_name.into()).await?
    );

    for (group_id, offset) in group_ids.iter().zip(&offsets) {
        let fetched = storage
            .offset_fetch(Some(group_id), &[deleted.clone(), kept.clone()], None)
            .await?;
        assert_ne!(
            Some(offset),
            fetched.get(&deleted).map(|committed| &committed.offset),
            "group {group_id:?} kept its offset for the deleted topic"
        );
        assert_eq!(
            Some(offset),
            fetched.get(&kept).map(|committed| &committed.offset),
            "group {group_id:?} lost its offset for the kept topic"
        );

        let committed = storage.committed_offset_topitions(group_id).await?;
        assert_eq!(
            [(kept.clone(), *offset)]
                .into_iter()
                .collect::<BTreeMap<_, _>>(),
            committed
                .into_iter()
                .map(|(topition, committed)| (topition, committed.offset))
                .collect::<BTreeMap<_, _>>(),
            "group {group_id:?}"
        );
    }

    Ok(())
}

mod unreadable_groups {
    use std::{
        collections::{BTreeMap, BTreeSet},
        time::{Duration, SystemTime},
    };

    use async_trait::async_trait;
    use nisshi_sans_io::{
        ConfigResource, ErrorCode, IsolationLevel, ListOffset, ScramMechanism,
        create_topics_request::CreatableTopic, delete_groups_response::DeletableGroupResult,
        delete_records_request::DeleteRecordsTopic,
        delete_records_response::DeleteRecordsTopicResult,
        describe_cluster_response::DescribeClusterBroker,
        describe_configs_response::DescribeConfigsResult,
        describe_topic_partitions_response::DescribeTopicPartitionsResponseTopic,
        fetch_response::AbortedTransaction,
        incremental_alter_configs_request::AlterConfigsResource,
        incremental_alter_configs_response::AlterConfigsResourceResponse, record::deflated,
        txn_offset_commit_response::TxnOffsetCommitResponseTopic,
    };
    use nisshi_storage::{
        BrokerRegistrationRequest, CommittedOffset, GroupDetail, ListOffsetResponse,
        MetadataResponse, NamedGroupDetail, OffsetCommitRequest, OffsetStage, ProducerIdResponse,
        Result, ScramCredential, Storage, TopicId, Topition, TxnAddPartitionsRequest,
        TxnAddPartitionsResponse, TxnOffsetCommitRequest, UpdateError, Version,
    };
    use url::Url;
    use uuid::Uuid;

    /// Delegates to `storage`, except that `describe_groups` answers
    /// `UNKNOWN_SERVER_ERROR` for every group, as a backend does when its read
    /// fails.
    #[derive(Clone, Debug)]
    pub(super) struct UnreadableGroups<S> {
        pub(super) storage: S,
    }

    #[async_trait]
    impl<S> Storage for UnreadableGroups<S>
    where
        S: Storage,
    {
        async fn register_broker(
            &self,
            broker_registration: BrokerRegistrationRequest,
        ) -> Result<()> {
            self.storage.register_broker(broker_registration).await
        }

        async fn create_topic(&self, topic: CreatableTopic, validate_only: bool) -> Result<Uuid> {
            self.storage.create_topic(topic, validate_only).await
        }

        async fn incremental_alter_resource(
            &self,
            resource: AlterConfigsResource,
        ) -> Result<AlterConfigsResourceResponse> {
            self.storage.incremental_alter_resource(resource).await
        }

        async fn delete_records(
            &self,
            topics: &[DeleteRecordsTopic],
        ) -> Result<Vec<DeleteRecordsTopicResult>> {
            self.storage.delete_records(topics).await
        }

        async fn delete_topic(&self, topic: &TopicId) -> Result<ErrorCode> {
            self.storage.delete_topic(topic).await
        }

        async fn brokers(&self) -> Result<Vec<DescribeClusterBroker>> {
            self.storage.brokers().await
        }

        async fn produce(
            &self,
            transaction_id: Option<&str>,
            topition: &Topition,
            batch: deflated::Batch,
        ) -> Result<i64> {
            self.storage.produce(transaction_id, topition, batch).await
        }

        async fn fetch(
            &self,
            topition: &'_ Topition,
            offset: i64,
            min_bytes: u32,
            max_bytes: u32,
            isolation: IsolationLevel,
            fetched_sender: nisshi_storage::FetchedSender,
        ) -> Result<()> {
            self.storage
                .fetch(
                    topition,
                    offset,
                    min_bytes,
                    max_bytes,
                    isolation,
                    fetched_sender,
                )
                .await
        }

        async fn offset_stage(&self, topition: &Topition) -> Result<OffsetStage> {
            self.storage.offset_stage(topition).await
        }

        async fn list_offsets(
            &self,
            isolation_level: IsolationLevel,
            offsets: &[(Topition, ListOffset)],
        ) -> Result<Vec<(Topition, ListOffsetResponse)>> {
            self.storage.list_offsets(isolation_level, offsets).await
        }

        async fn offset_commit(
            &self,
            group_id: &str,
            retention_time_ms: Option<Duration>,
            offsets: &[(Topition, OffsetCommitRequest)],
        ) -> Result<Vec<(Topition, ErrorCode)>> {
            self.storage
                .offset_commit(group_id, retention_time_ms, offsets)
                .await
        }

        async fn offset_fetch(
            &self,
            group_id: Option<&str>,
            topics: &[Topition],
            require_stable: Option<bool>,
        ) -> Result<BTreeMap<Topition, CommittedOffset>> {
            self.storage
                .offset_fetch(group_id, topics, require_stable)
                .await
        }

        async fn committed_offset_topitions(
            &self,
            group_id: &str,
        ) -> Result<BTreeMap<Topition, CommittedOffset>> {
            self.storage.committed_offset_topitions(group_id).await
        }

        async fn pending_txn_offsets(&self, group_id: &str) -> Result<BTreeSet<Topition>> {
            self.storage.pending_txn_offsets(group_id).await
        }

        async fn metadata(&self, topics: Option<&[TopicId]>) -> Result<MetadataResponse> {
            self.storage.metadata(topics).await
        }

        async fn upsert_user_scram_credential(
            &self,
            user: &str,
            mechanism: ScramMechanism,
            credential: ScramCredential,
        ) -> Result<()> {
            self.storage
                .upsert_user_scram_credential(user, mechanism, credential)
                .await
        }

        async fn delete_user_scram_credential(
            &self,
            user: &str,
            mechanism: ScramMechanism,
        ) -> Result<()> {
            self.storage
                .delete_user_scram_credential(user, mechanism)
                .await
        }

        async fn user_scram_credential(
            &self,
            user: &str,
            mechanism: ScramMechanism,
        ) -> Result<Option<ScramCredential>> {
            self.storage.user_scram_credential(user, mechanism).await
        }

        async fn describe_config(
            &self,
            name: &str,
            resource: ConfigResource,
            keys: Option<&[String]>,
        ) -> Result<DescribeConfigsResult> {
            self.storage.describe_config(name, resource, keys).await
        }

        async fn list_groups(&self) -> Result<Vec<NamedGroupDetail>> {
            self.storage.list_groups().await
        }

        async fn delete_groups(
            &self,
            group_ids: Option<&[String]>,
        ) -> Result<Vec<DeletableGroupResult>> {
            self.storage.delete_groups(group_ids).await
        }

        async fn describe_groups(
            &self,
            group_ids: Option<&[String]>,
            _include_authorized_operations: bool,
        ) -> Result<Vec<NamedGroupDetail>> {
            Ok(group_ids
                .unwrap_or_default()
                .iter()
                .map(|name| {
                    NamedGroupDetail::error_code(name.to_owned(), ErrorCode::UnknownServerError)
                })
                .collect())
        }

        async fn describe_topic_partitions(
            &self,
            topics: Option<&[TopicId]>,
            partition_limit: i32,
            cursor: Option<Topition>,
        ) -> Result<Vec<DescribeTopicPartitionsResponseTopic>> {
            self.storage
                .describe_topic_partitions(topics, partition_limit, cursor)
                .await
        }

        async fn group_with_version(
            &self,
            group_id: &str,
        ) -> Result<Option<(GroupDetail, Version)>> {
            self.storage.group_with_version(group_id).await
        }

        async fn update_group(
            &self,
            group_id: &str,
            detail: GroupDetail,
            version: Option<Version>,
        ) -> Result<Version, UpdateError<GroupDetail>> {
            self.storage.update_group(group_id, detail, version).await
        }

        async fn init_producer(
            &self,
            transaction_id: Option<&str>,
            transaction_timeout_ms: i32,
            producer_id: Option<i64>,
            producer_epoch: Option<i16>,
        ) -> Result<ProducerIdResponse> {
            self.storage
                .init_producer(
                    transaction_id,
                    transaction_timeout_ms,
                    producer_id,
                    producer_epoch,
                )
                .await
        }

        async fn txn_add_offsets(
            &self,
            transaction_id: &str,
            producer_id: i64,
            producer_epoch: i16,
            group_id: &str,
        ) -> Result<ErrorCode> {
            self.storage
                .txn_add_offsets(transaction_id, producer_id, producer_epoch, group_id)
                .await
        }

        async fn txn_add_partitions(
            &self,
            partitions: TxnAddPartitionsRequest,
        ) -> Result<TxnAddPartitionsResponse> {
            self.storage.txn_add_partitions(partitions).await
        }

        async fn txn_offset_commit(
            &self,
            offsets: TxnOffsetCommitRequest,
        ) -> Result<Vec<TxnOffsetCommitResponseTopic>> {
            self.storage.txn_offset_commit(offsets).await
        }

        async fn txn_end(
            &self,
            transaction_id: &str,
            producer_id: i64,
            producer_epoch: i16,
            committed: bool,
        ) -> Result<ErrorCode> {
            self.storage
                .txn_end(transaction_id, producer_id, producer_epoch, committed)
                .await
        }

        async fn maintain(&self, now: SystemTime) -> Result<()> {
            self.storage.maintain(now).await
        }

        async fn maintain_transactions(&self, now: SystemTime) -> Result<()> {
            self.storage.maintain_transactions(now).await
        }

        async fn aborted_transactions(
            &self,
            topition: &Topition,
            offset: i64,
            last_stable_offset: i64,
        ) -> Result<Vec<AbortedTransaction>> {
            self.storage
                .aborted_transactions(topition, offset, last_stable_offset)
                .await
        }

        async fn cluster_id(&self) -> Result<String> {
            self.storage.cluster_id().await
        }

        async fn node(&self) -> Result<i32> {
            self.storage.node().await
        }

        async fn advertised_listener(&self) -> Result<Url> {
            self.storage.advertised_listener().await
        }

        async fn ping(&self) -> Result<()> {
            self.storage.ping().await
        }
    }
}
