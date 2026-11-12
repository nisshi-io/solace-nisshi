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
    collections::BTreeMap,
    fs::read,
    io::{BufRead, Cursor},
    iter::repeat_n,
    time::SystemTime,
};

use anyhow::{Context as _, Result, anyhow};
use clap::Parser;
use console::style;
use nisshi_sans_io::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, Body, CreateTopicsRequest,
    CreateTopicsResponse, ErrorCode, FetchRequest, FetchResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, Frame, HeartbeatRequest, HeartbeatResponse, InitProducerIdRequest,
    InitProducerIdResponse, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest,
    LeaveGroupResponse, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest, MetadataResponse,
    OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse,
    OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse, ProduceRequest, ProduceResponse,
    SyncGroupRequest, SyncGroupResponse,
    consumer::{MemberAssignment, MemberMetadata},
};
use regex::Regex;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Clone, Debug, Parser)]
struct Arg {
    #[arg(long, default_value = "proxy.log")]
    filename: String,

    #[arg(long)]
    group_id: Option<String>,

    #[arg(long)]
    member_id: Option<String>,

    #[arg(long)]
    verbose: bool,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Action {
    Ignore,
    Process { column: usize },
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct RequestDetail {
    timestamp: SystemTime,
    api_key: i16,
    api_version: i16,
    action: Action,
}

struct Api;

impl Api {
    const CONSUMER: &[i16] = &[
        HeartbeatRequest::KEY,
        JoinGroupRequest::KEY,
        LeaveGroupRequest::KEY,
        OffsetCommitRequest::KEY,
        OffsetFetchRequest::KEY,
        SyncGroupRequest::KEY,
    ];

    const _META: &[i16] = &[
        FindCoordinatorRequest::KEY,
        ListOffsetsRequest::KEY,
        MetadataRequest::KEY,
    ];

    const _ADMIN: &[i16] = &[CreateTopicsRequest::KEY, OffsetForLeaderEpochRequest::KEY];

    const _PRODUCER: &[i16] = &[InitProducerIdRequest::KEY, ProduceRequest::KEY];

    const _FETCHER: &[i16] = &[FetchRequest::KEY];
}

fn status_line<'a>(
    id: &str,
    frame: &Frame,
    requests: impl Iterator<Item = (&'a String, &'a RequestDetail)>,
) -> String {
    let mut requests = requests
        .filter_map(|(id, detail)| match detail.action {
            Action::Process { column } => Some((id, column)),
            _ => None,
        })
        .collect::<Vec<_>>();
    requests.sort_by_key(|(id, _)| *id);

    let mut status = String::new();
    let mut overlap = false;

    for (identity, column) in requests {
        for prefix in repeat_n(
            if overlap { '━' } else { ' ' },
            column - status.chars().count(),
        ) {
            status.push(prefix);
        }

        if id == identity {
            status.push(if frame.is_request() { '┏' } else { '┗' });
            overlap = true;
        } else if overlap {
            status.push('╋');
        } else {
            status.push('┃');
        }
    }

    if frame.is_request() {
        status.push('┅');
    } else {
        status.push('━');
    }

    status
}

pub fn main() -> Result<()> {
    let cli = Arg::parse();

    let ansi = Regex::new(r#"\x1b\[[;\d]*[[:alpha:]]"#)?;

    let frame = Regex::new(
        r#"^(?<datetime>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z).*req\{id="(?<id>.{21})"\}.*(request|response)=\[(?<frame>(\d{1,3},?\s*)*)\]"#,
    )?;

    let mut lines = read(cli.filename)
        .map(Cursor::new)
        .map(|contents| contents.lines())
        .map(|lines| lines.into_iter())?;

    let mut requests = BTreeMap::new();

    let api_keys = Api::CONSUMER;

    while let Some(line) = lines.next().and_then(|line| line.ok()) {
        let stripped = ansi.replace_all(line.as_str(), "");

        if let Some(captures) = frame.captures(stripped.as_ref())
            && let Some(datetime) = captures.name("datetime").map(|m| m.as_str())
            && let Some(frame) = captures.name("frame").map(|m| m.as_str())
            && let Some(id) = captures.name("id").map(|m| m.as_str())
        {
            let timestamp = OffsetDateTime::parse(datetime, &Rfc3339).map(SystemTime::from)?;

            let (elapsed, decoded) = match requests.get(id).copied() {
                Some(RequestDetail {
                    action: Action::Ignore,
                    ..
                }) => continue,

                Some(RequestDetail {
                    api_key,
                    api_version,
                    action: Action::Process { .. },
                    timestamp: started_at,
                    ..
                }) => {
                    let encoded = frame
                        .split(", ")
                        .map(|byte| byte.parse::<u8>().map_err(Into::into))
                        .collect::<Result<Vec<_>>>()?;

                    (
                        timestamp.duration_since(started_at).ok(),
                        Frame::response_from_bytes(&encoded[..], api_key, api_version)
                            .with_context(|| id.to_string())?,
                    )
                }

                None => {
                    let encoded = frame
                        .split(", ")
                        .map(|byte| byte.parse::<u8>().map_err(Into::into))
                        .collect::<Result<Vec<_>>>()?;

                    let request =
                        Frame::request_from_bytes(&encoded[..]).with_context(|| id.to_string())?;
                    let api_key = request.api_key()?;
                    let api_version = request.api_version()?;

                    let action = if api_keys.contains(&api_key) {
                        let column = requests
                            .values()
                            .filter_map(|detail| match detail.action {
                                Action::Process { column } => Some(column),
                                _ => None,
                            })
                            .max()
                            .map_or(0, |column| column.saturating_add(1));

                        Action::Process { column }
                    } else {
                        Action::Ignore
                    };

                    assert_eq!(
                        requests.insert(
                            id.to_owned(),
                            RequestDetail {
                                api_key,
                                api_version,
                                action,
                                timestamp,
                            },
                        ),
                        None
                    );

                    (None, request)
                }
            };

            if requests
                .get(id)
                .is_some_and(|detail| detail.action == Action::Ignore)
            {
                if decoded.is_request() {
                    continue;
                } else {
                    _ = requests.remove(id);
                }
            }

            match decoded {
                Frame {
                    body:
                        Body::OffsetFetchRequest(OffsetFetchRequest {
                            group_id: None,
                            topics: None,
                            groups: Some(ref groups),
                            ..
                        }),
                    ..
                } if cli
                    .group_id
                    .as_deref()
                    .is_some_and(|group| !groups.iter().any(|g| g.group_id == group)) =>
                {
                    _ = requests
                        .entry(id.to_string())
                        .and_modify(|detail| detail.action = Action::Ignore);
                }

                Frame {
                    body:
                        Body::OffsetFetchRequest(OffsetFetchRequest {
                            group_id: Some(ref group_id),
                            ..
                        })
                        | Body::OffsetCommitRequest(OffsetCommitRequest { ref group_id, .. }),
                    ..
                } if cli
                    .group_id
                    .as_deref()
                    .is_some_and(|group| group != group_id) =>
                {
                    _ = requests
                        .entry(id.to_string())
                        .and_modify(|detail| detail.action = Action::Ignore);
                }

                Frame {
                    body: Body::LeaveGroupRequest(LeaveGroupRequest { ref group_id, .. }),
                    ..
                } if cli
                    .group_id
                    .as_deref()
                    .is_some_and(|group| group != group_id) =>
                {
                    _ = requests
                        .entry(id.to_string())
                        .and_modify(|detail| detail.action = Action::Ignore);
                }

                Frame {
                    body:
                        Body::LeaveGroupRequest(LeaveGroupRequest {
                            member_id: Some(ref member_id),
                            ..
                        })
                        | Body::OffsetCommitRequest(OffsetCommitRequest {
                            member_id: Some(ref member_id),
                            ..
                        }),
                    ..
                } if cli
                    .member_id
                    .as_deref()
                    .is_some_and(|member| member != member_id) =>
                {
                    _ = requests
                        .entry(id.to_string())
                        .and_modify(|detail| detail.action = Action::Ignore);
                }

                Frame {
                    body:
                        Body::JoinGroupRequest(JoinGroupRequest {
                            ref group_id,
                            ref member_id,
                            ..
                        })
                        | Body::SyncGroupRequest(SyncGroupRequest {
                            ref group_id,
                            ref member_id,
                            ..
                        })
                        | Body::HeartbeatRequest(HeartbeatRequest {
                            ref group_id,
                            ref member_id,
                            ..
                        }),
                    ..
                } if cli
                    .group_id
                    .as_deref()
                    .is_some_and(|group| group != group_id)
                    || cli
                        .member_id
                        .as_deref()
                        .is_some_and(|member| member != member_id) =>
                {
                    _ = requests
                        .entry(id.to_string())
                        .and_modify(|detail| detail.action = Action::Ignore);
                }

                ref frame @ Frame {
                    body:
                        Body::JoinGroupRequest(JoinGroupRequest {
                            ref group_id,
                            ref member_id,
                            session_timeout_ms,
                            rebalance_timeout_ms,
                            ..
                        }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let group = if cli.group_id.as_ref().is_some_and(|group| group == group_id) {
                        "".into()
                    } else {
                        format!(" {group_id}")
                    };

                    println!(
                        "{status_line} ⨝ ({id}){group} {member_id} {session_timeout_ms} {rebalance_timeout_ms:?}"
                    );
                }

                ref frame @ Frame {
                    body:
                        Body::JoinGroupResponse(JoinGroupResponse {
                            error_code,
                            generation_id,
                            ref leader,
                            ref member_id,
                            ref members,
                            ..
                        }),
                    ..
                } if cli
                    .member_id
                    .as_deref()
                    .is_none_or(|member| member == member_id) =>
                {
                    let status_line = status_line(id, frame, requests.iter());

                    let elapsed = elapsed.ok_or(anyhow!("duration"))?;

                    let member = if member_id.is_empty() || member_id == leader {
                        "".into()
                    } else {
                        format!(" member={member_id}")
                    };

                    let leader = if leader == member_id {
                        format!(" leader={}", style(member_id).cyan())
                    } else {
                        "".into()
                    };

                    let generation = if generation_id == -1 {
                        "".into()
                    } else {
                        format!("/{generation_id}")
                    };

                    let members = if cli.verbose {
                        format!(
                            " members={:?}",
                            members
                                .as_deref()
                                .unwrap_or_default()
                                .iter()
                                .map(|join_group_response| {
                                    MemberMetadata::try_from(join_group_response.metadata.clone())
                                        .map(|metadata| {
                                            (
                                                join_group_response.member_id.clone(),
                                                metadata.to_string(),
                                            )
                                        })
                                        .map_err(Into::into)
                                })
                                .collect::<Result<BTreeMap<_, _>>>()?
                        )
                    } else {
                        "".into()
                    };

                    let error_code = match ErrorCode::try_from(error_code)? {
                        ErrorCode::None => "".into(),
                        otherwise => format!(" {}", style(format!("{otherwise:?}")).red()),
                    };

                    println!(
                        "{status_line} ⨝{generation} {elapsed:?}{leader}{member}{members}{error_code}"
                    );
                }

                ref frame @ Frame {
                    body:
                        Body::SyncGroupRequest(SyncGroupRequest {
                            ref group_id,
                            generation_id,
                            ref member_id,
                            ref assignments,
                            ..
                        }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let member = if member_id.is_empty() {
                        "".into()
                    } else if assignments.as_deref().unwrap_or_default().is_empty() {
                        format!(" member={member_id}")
                    } else {
                        format!(" leader={}", style(member_id).cyan())
                    };

                    let generation = if generation_id == -1 {
                        "".into()
                    } else {
                        format!("/{generation_id}")
                    };

                    let assignments = if cli.verbose {
                        format!(
                            " assignments={:?}",
                            assignments
                                .as_deref()
                                .unwrap_or_default()
                                .iter()
                                .map(|ra| {
                                    MemberAssignment::try_from(ra.assignment.clone())
                                        .map(|assignment| {
                                            (ra.member_id.clone(), assignment.to_string())
                                        })
                                        .map_err(Into::into)
                                })
                                .collect::<Result<BTreeMap<_, _>>>()?
                        )
                    } else {
                        "".into()
                    };

                    let group = if cli.group_id.as_ref().is_some_and(|group| group == group_id) {
                        "".into()
                    } else {
                        format!(" {group_id}")
                    };

                    println!("{status_line} ␖{generation} ({id}){group}{member}{assignments}");
                }

                ref frame @ Frame {
                    body:
                        Body::SyncGroupResponse(SyncGroupResponse {
                            error_code,
                            ref assignment,
                            ..
                        }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let elapsed = elapsed.ok_or(anyhow!("duration"))?;

                    let error_code = match ErrorCode::try_from(error_code)? {
                        ErrorCode::None => "".into(),
                        otherwise => format!(" {}", style(format!("{otherwise:?}")).red()),
                    };

                    let assignment = if assignment.is_empty() {
                        "".into()
                    } else {
                        MemberAssignment::try_from(assignment.to_owned())
                            .map(|assignment| {
                                assignment
                                    .assignment
                                    .assigned_partitions
                                    .iter()
                                    .map(ToString::to_string)
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            })
                            .map(|assignment| format!(" assignment={assignment}",))?
                    };

                    println!("{status_line} ␖ {elapsed:?}{assignment}{error_code}");
                }

                ref frame @ Frame {
                    body:
                        Body::HeartbeatRequest(HeartbeatRequest {
                            ref group_id,
                            generation_id,
                            ref member_id,
                            ..
                        }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let generation = if generation_id == -1 {
                        "".into()
                    } else {
                        format!("/{generation_id}")
                    };

                    let group = if cli.group_id.as_ref().is_some_and(|group| group == group_id) {
                        "".into()
                    } else {
                        format!(" {group_id}")
                    };

                    println!("{status_line} ❤️ {generation} ({id}){group} {member_id}")
                }

                ref frame @ Frame {
                    body: Body::HeartbeatResponse(HeartbeatResponse { error_code, .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let elapsed = elapsed.ok_or(anyhow!("duration"))?;

                    let ec = match ErrorCode::try_from(error_code)? {
                        ErrorCode::None => "".into(),
                        otherwise => format!(" {}", style(format!("{otherwise:?}")).red()),
                    };

                    let symbol = if ErrorCode::try_from(error_code)
                        .is_ok_and(|error_code| error_code == ErrorCode::None)
                    {
                        "❤️ "
                    } else {
                        "❤️‍🩹"
                    };

                    println!("{status_line} {symbol} {elapsed:?}{ec}");
                }

                ref frame @ Frame {
                    body: Body::ApiVersionsRequest(ApiVersionsRequest { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());
                    println!("{status_line} api_versions ({id})")
                }

                ref frame @ Frame {
                    body: Body::ApiVersionsResponse(ApiVersionsResponse { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let elapsed = elapsed.ok_or(anyhow!("duration"))?;

                    println!("{status_line} api_versions {elapsed:?}")
                }

                ref frame @ Frame {
                    body: Body::MetadataRequest(MetadataRequest { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());
                    println!("{status_line} metadata ({id})")
                }

                ref frame @ Frame {
                    body: Body::MetadataResponse(MetadataResponse { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let elapsed = elapsed.ok_or(anyhow!("duration"))?;

                    println!("{status_line} metadata {elapsed:?}")
                }

                ref frame @ Frame {
                    body: Body::CreateTopicsRequest(CreateTopicsRequest { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());
                    println!("{status_line} create topics ({id})")
                }

                ref frame @ Frame {
                    body: Body::CreateTopicsResponse(CreateTopicsResponse { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let elapsed = elapsed.ok_or(anyhow!("duration"))?;

                    println!("{status_line} create_topics {elapsed:?}")
                }

                ref frame @ Frame {
                    body: Body::InitProducerIdRequest(InitProducerIdRequest { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());
                    println!("{status_line} init producer ({id})")
                }

                ref frame @ Frame {
                    body: Body::InitProducerIdResponse(InitProducerIdResponse { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let elapsed = elapsed.ok_or(anyhow!("duration"))?;

                    println!("{status_line} init producer {elapsed:?}")
                }

                ref frame @ Frame {
                    body: Body::FindCoordinatorRequest(FindCoordinatorRequest { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());
                    println!("{status_line} find coordinator ({id})")
                }

                ref frame @ Frame {
                    body: Body::FindCoordinatorResponse(FindCoordinatorResponse { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let elapsed = elapsed.ok_or(anyhow!("duration"))?;

                    println!("{status_line} find coordinator {elapsed:?}")
                }

                ref frame @ Frame {
                    body: Body::ProduceRequest(ProduceRequest { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());
                    println!("{status_line} produce ({id})")
                }

                ref frame @ Frame {
                    body: Body::ProduceResponse(ProduceResponse { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let elapsed = elapsed.ok_or(anyhow!("duration"))?;

                    println!("{status_line} produce {elapsed:?}")
                }

                ref frame @ Frame {
                    body:
                        Body::OffsetFetchRequest(OffsetFetchRequest {
                            ref topics,
                            ref groups,
                            ..
                        }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let topics = topics
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .map(|topic| {
                            (
                                topic.name.as_str(),
                                topic
                                    .partition_indexes
                                    .as_deref()
                                    .unwrap_or_default()
                                    .to_vec(),
                            )
                        })
                        .collect::<BTreeMap<_, _>>();

                    let groups = groups
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .map(|group| {
                            (
                                group.group_id.as_str(),
                                group
                                    .topics
                                    .as_deref()
                                    .unwrap_or_default()
                                    .iter()
                                    .map(|topic| {
                                        (
                                            topic.name.as_str(),
                                            topic
                                                .partition_indexes
                                                .as_deref()
                                                .unwrap_or_default()
                                                .to_vec(),
                                        )
                                    })
                                    .collect::<BTreeMap<_, _>>(),
                            )
                        })
                        .collect::<BTreeMap<_, _>>();

                    println!("{status_line} offset fetch ({id}) {topics:?} {groups:?}")
                }

                ref frame @ Frame {
                    body: Body::OffsetFetchResponse(OffsetFetchResponse { ref groups, .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let elapsed = elapsed.ok_or(anyhow!("duration"))?;

                    let groups = groups
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .map(|group| {
                            (
                                &group.group_id[..],
                                group
                                    .topics
                                    .as_deref()
                                    .unwrap_or_default()
                                    .iter()
                                    .map(|topic| {
                                        (
                                            &topic.name[..],
                                            topic
                                                .partitions
                                                .as_deref()
                                                .unwrap_or_default()
                                                .iter()
                                                .map(|partition| {
                                                    (
                                                        partition.partition_index,
                                                        partition.committed_offset,
                                                    )
                                                })
                                                .collect::<BTreeMap<_, _>>(),
                                        )
                                    })
                                    .collect::<BTreeMap<_, _>>(),
                            )
                        })
                        .collect::<BTreeMap<_, _>>();

                    println!("{status_line} offset fetch {elapsed:?} {groups:?}")
                }

                ref frame @ Frame {
                    body: Body::ListOffsetsRequest(ListOffsetsRequest { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());
                    println!("{status_line} list offsets ({id})")
                }

                ref frame @ Frame {
                    body: Body::ListOffsetsResponse(ListOffsetsResponse { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let elapsed = elapsed.ok_or(anyhow!("duration"))?;

                    println!("{status_line} list offsets {elapsed:?}")
                }

                ref frame @ Frame {
                    body: Body::FetchRequest(FetchRequest { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());
                    println!("{status_line} fetch ({id})")
                }

                ref frame @ Frame {
                    body: Body::FetchResponse(FetchResponse { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let elapsed = elapsed.ok_or(anyhow!("duration"))?;

                    println!("{status_line} fetch {elapsed:?}")
                }

                ref frame @ Frame {
                    body:
                        Body::LeaveGroupRequest(LeaveGroupRequest {
                            ref group_id,
                            ref members,
                            ..
                        }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let group = if cli.group_id.as_ref().is_some_and(|group| group == group_id) {
                        "".into()
                    } else {
                        format!(" {group_id}")
                    };

                    let member_ids = match members
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .map(|member| member.member_id.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                    {
                        members if members.is_empty() => "".into(),

                        members => {
                            format!(" members={members}")
                        }
                    };

                    println!("{status_line} leave{group} ({id}){member_ids}")
                }

                ref frame @ Frame {
                    body: Body::LeaveGroupResponse(LeaveGroupResponse { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let elapsed = elapsed.ok_or(anyhow!("duration"))?;

                    println!("{status_line} leave group {elapsed:?}")
                }

                ref frame @ Frame {
                    body:
                        Body::OffsetCommitRequest(OffsetCommitRequest {
                            ref member_id,
                            ref topics,
                            ref generation_id_or_member_epoch,
                            ..
                        }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let generation = generation_id_or_member_epoch
                        .map_or("".into(), |generation| format!("/{generation}"));

                    let member = match member_id.as_deref().unwrap_or_default() {
                        empty @ "" => empty.into(),
                        member_id => format!(" member={member_id}"),
                    };

                    let topics = topics
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .map(|topic| {
                            (
                                topic.name.clone(),
                                topic
                                    .partitions
                                    .as_deref()
                                    .unwrap_or_default()
                                    .iter()
                                    .map(|partition| {
                                        (partition.partition_index, partition.committed_offset)
                                    })
                                    .collect::<BTreeMap<_, _>>(),
                            )
                        })
                        .collect::<BTreeMap<_, _>>();

                    println!("{status_line} offset commit{generation} ({id}) {member} {topics:?}")
                }

                ref frame @ Frame {
                    body: Body::OffsetCommitResponse(OffsetCommitResponse { ref topics, .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let elapsed = elapsed.ok_or(anyhow!("duration"))?;

                    let topics = topics
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .map(|topic| {
                            topic
                                .partitions
                                .as_deref()
                                .unwrap_or_default()
                                .iter()
                                .map(|partition| {
                                    ErrorCode::try_from(partition.error_code)
                                        .map(|error_code| (partition.partition_index, error_code))
                                        .map_err(Into::into)
                                })
                                .collect::<Result<BTreeMap<_, _>>>()
                                .map(|partitions| (topic.name.clone(), partitions))
                        })
                        .collect::<Result<BTreeMap<_, _>>>()?;

                    println!("{status_line} offset commit {elapsed:?} {topics:?}")
                }

                ref frame @ Frame {
                    body: Body::OffsetForLeaderEpochRequest(OffsetForLeaderEpochRequest { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());
                    println!("{status_line} offset for leader epoch ({id})")
                }

                ref frame @ Frame {
                    body: Body::OffsetForLeaderEpochResponse(OffsetForLeaderEpochResponse { .. }),
                    ..
                } => {
                    let status_line = status_line(id, frame, requests.iter());

                    let elapsed = elapsed.ok_or(anyhow!("duration"))?;

                    println!("{status_line} offset for leader epoch {elapsed:?}")
                }

                ref frame @ Frame { .. } if frame.is_request() => {
                    eprintln!("dropped: {body:?}", body = frame.body);

                    _ = requests
                        .entry(id.to_string())
                        .and_modify(|detail| detail.action = Action::Ignore);
                }

                ref frame @ Frame { .. } if frame.is_response() => {
                    _ = requests.remove(id);
                }

                Frame { .. } => continue,
            }

            if decoded.is_response() {
                _ = requests.remove(id);
            }
        }
    }

    Ok(())
}
