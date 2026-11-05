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
    fmt,
    marker::PhantomData,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::{Buf as _, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::{
    Body, Decoder, Encoder, Error, ErrorCode, HeartbeatRequest, HeartbeatResponse,
    JoinGroupRequest, JoinGroupResponse, MetadataResponse, SyncGroupRequest, SyncGroupResponse,
    join_group_request::JoinGroupRequestProtocol, join_group_response::JoinGroupResponseMember,
    sync_group_request::SyncGroupRequestAssignment,
};

mod assignor;
mod codec;

pub use assignor::{CooperativeStickyAssignor, RangeAssignor, RoundRobinAssignor, UniformAssignor};

pub const CONSUMER: &str = "consumer";

pub trait ConsumerAssignor {
    fn assign(
        &self,
        members: &[JoinGroupResponseMember],
        metadata: &MetadataResponse,
    ) -> Result<Vec<SyncGroupRequestAssignment>, Error>;
}

pub enum Assignor {
    Uniform,
    Range,
    RoundRobin,
    CooperativeSticky,
}

impl Assignor {
    pub const COOPERATIVE_STICKY: &str = "cooperative-sticky";
    pub const RANGE: &str = "range";
    pub const ROUND_ROBIN: &str = "roundrobin";
    pub const UNIFORM: &str = "uniform";
}

impl AsRef<str> for Assignor {
    fn as_ref(&self) -> &str {
        match self {
            Self::CooperativeSticky => Self::COOPERATIVE_STICKY,
            Self::Range => Self::RANGE,
            Self::RoundRobin => Self::ROUND_ROBIN,
            Self::Uniform => Self::UNIFORM,
        }
    }
}

impl FromStr for Assignor {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            Self::COOPERATIVE_STICKY => Ok(Self::CooperativeSticky),
            Self::RANGE => Ok(Self::Range),
            Self::ROUND_ROBIN => Ok(Self::RoundRobin),
            Self::UNIFORM => Ok(Self::Uniform),
            otherwise => Err(Error::UnknownAssignor(otherwise.into())),
        }
    }
}

impl ConsumerAssignor for Assignor {
    fn assign(
        &self,
        members: &[JoinGroupResponseMember],
        metadata: &MetadataResponse,
    ) -> Result<Vec<SyncGroupRequestAssignment>, Error> {
        match self {
            Self::CooperativeSticky => CooperativeStickyAssignor.assign(members, metadata),
            Self::Range => RangeAssignor.assign(members, metadata),
            Self::RoundRobin => RoundRobinAssignor.assign(members, metadata),
            Self::Uniform => UniformAssignor.assign(members, metadata),
        }
    }
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "codec::MemberMetadata")]
#[serde(into = "codec::MemberMetadata")]
pub struct MemberMetadata {
    pub version: i16,
    pub subscription: ConsumerProtocolSubscription,
}

impl fmt::Display for MemberMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v: {}, sub: {}", self.version, self.subscription)
    }
}

impl MemberMetadata {
    pub fn version(self, version: i16) -> Self {
        Self { version, ..self }
    }

    pub fn subscription(self, subscription: ConsumerProtocolSubscription) -> Self {
        Self {
            subscription,
            ..self
        }
    }
}

impl TryFrom<Bytes> for MemberMetadata {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        // The consumer codec reads byte by byte and never reaches the
        // decoder's length-prefixed reads, so this bound changes nothing
        // today; it keeps those reads bounded if the codec starts using them.
        let message_max_size = value.len();
        let mut reader = value.reader();
        let mut decoder = Decoder::new(&mut reader).with_message_max_size(message_max_size);
        Self::deserialize(&mut decoder)
    }
}

impl TryFrom<&MemberMetadata> for Bytes {
    type Error = Error;

    fn try_from(value: &MemberMetadata) -> Result<Self, Self::Error> {
        let mut encoder = Encoder::new(BytesMut::new());

        value.serialize(&mut encoder).and(Ok(Bytes::from(encoder)))
    }
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ConsumerProtocolSubscription {
    pub topics: Vec<String>,
    pub user_data: Option<Bytes>,
    pub owned_partitions: Option<Vec<TopicPartition>>,
    pub generation_id: Option<i32>,
    pub rack_id: Option<String>,
}

impl fmt::Display for ConsumerProtocolSubscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "owned: {}",
            self.owned_partitions
                .as_deref()
                .map(|partitions| {
                    partitions
                        .iter()
                        .map(|partition| partition.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default()
        )
    }
}

impl ConsumerProtocolSubscription {
    pub const V0: i16 = 0;
    pub const V1: i16 = 1;
    pub const V2: i16 = 2;
    pub const V3: i16 = 3;

    pub fn topics(self, topics: impl IntoIterator<Item = String>) -> Self {
        Self {
            topics: topics.into_iter().collect(),
            ..self
        }
    }

    pub fn user_data(self, user_data: Option<Bytes>) -> Self {
        Self { user_data, ..self }
    }

    pub fn owned_partitions(self, owned_partitions: impl Iterator<Item = TopicPartition>) -> Self {
        Self {
            owned_partitions: Some(owned_partitions.collect()),
            ..self
        }
    }

    pub fn generation_id(self, generation_id: Option<i32>) -> Self {
        Self {
            generation_id,
            ..self
        }
    }

    pub fn rack_id(self, rack_id: Option<String>) -> Self {
        Self { rack_id, ..self }
    }
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub struct TopicPartition {
    pub topic: String,
    pub partitions: Vec<i32>,
}

impl<T, P> From<(T, P)> for TopicPartition
where
    T: Into<String>,
    P: IntoIterator<Item = i32>,
{
    fn from(value: (T, P)) -> TopicPartition {
        let topic = value.0.into();
        let partitions = value.1.into_iter().collect();

        Self { topic, partitions }
    }
}

impl fmt::Display for TopicPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: [{}]",
            self.topic,
            self.partitions
                .iter()
                .map(|partition| partition.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl TopicPartition {
    pub fn topic(self, topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            ..self
        }
    }

    pub fn partitions(self, partitions: impl Iterator<Item = i32>) -> Self {
        Self {
            partitions: partitions.collect(),
            ..self
        }
    }
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "codec::MemberAssignment")]
#[serde(into = "codec::MemberAssignment")]
pub struct MemberAssignment {
    pub version: i16,
    pub assignment: ConsumerProtocolAssignment,
}

impl fmt::Display for MemberAssignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.assignment.to_string().as_str())
    }
}

impl MemberAssignment {
    pub fn version(self, version: i16) -> Self {
        Self { version, ..self }
    }

    pub fn assignment(self, assignment: ConsumerProtocolAssignment) -> Self {
        Self { assignment, ..self }
    }
}

impl TryFrom<Bytes> for MemberAssignment {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        // Bounded for the same reason as `MemberMetadata` above.
        let message_max_size = value.len();
        let mut reader = value.reader();
        let mut codec = Decoder::new(&mut reader).with_message_max_size(message_max_size);
        Self::deserialize(&mut codec)
    }
}

impl TryFrom<&MemberAssignment> for Bytes {
    type Error = Error;

    fn try_from(value: &MemberAssignment) -> Result<Self, Self::Error> {
        let mut encoder = Encoder::new(BytesMut::new());

        value.serialize(&mut encoder).and(Ok(Bytes::from(encoder)))
    }
}

#[derive(Clone, Default, Deserialize, Eq, Hash, Debug, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ConsumerProtocolAssignment {
    pub assigned_partitions: Vec<TopicPartition>,
    pub user_data: Option<Bytes>,
}

impl fmt::Display for ConsumerProtocolAssignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(
            self.assigned_partitions
                .iter()
                .map(|assigned| assigned.to_string())
                .collect::<Vec<_>>()
                .join(", ")
                .as_str(),
        )
    }
}

impl ConsumerProtocolAssignment {
    pub fn assigned_partitions(
        self,
        assigned_partitions: impl IntoIterator<Item = impl Into<TopicPartition>>,
    ) -> Self {
        Self {
            assigned_partitions: assigned_partitions.into_iter().map(|p| p.into()).collect(),
            ..self
        }
    }

    pub fn user_data(self, user_data: impl IntoIterator<Item = Bytes>) -> Self {
        Self {
            user_data: user_data.into_iter().next(),
            ..self
        }
    }
}

pub type DynConsumerAssignment = dyn Fn(String, MemberAssignment) + Send + Sync + 'static;

pub struct Builder<M, T> {
    group_id: String,
    topics: T,
    metadata: M,
    on_assignment: Option<Arc<DynConsumerAssignment>>,
    rebalance_timeout: Option<Duration>,
    session_timeout: Option<Duration>,
}

impl Builder<PhantomData<MetadataResponse>, PhantomData<Vec<String>>> {
    fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            topics: PhantomData,
            metadata: PhantomData,
            on_assignment: None,
            rebalance_timeout: None,
            session_timeout: None,
        }
    }
}

impl<M, T> Builder<M, T> {
    pub fn topics(
        self,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> Builder<M, Vec<String>> {
        Builder {
            group_id: self.group_id,
            topics: topics.into_iter().map(Into::into).collect(),
            metadata: self.metadata,
            on_assignment: self.on_assignment,
            rebalance_timeout: self.rebalance_timeout,
            session_timeout: self.session_timeout,
        }
    }

    pub fn metadata(self, metadata: MetadataResponse) -> Builder<MetadataResponse, T> {
        Builder {
            group_id: self.group_id,
            topics: self.topics,
            metadata,
            on_assignment: self.on_assignment,
            rebalance_timeout: self.rebalance_timeout,
            session_timeout: self.session_timeout,
        }
    }

    pub fn rebalance_timeout(self, rebalance_timeout: Option<Duration>) -> Self {
        Self {
            rebalance_timeout,
            ..self
        }
    }

    pub fn session_timeout(self, session_timeout: Option<Duration>) -> Self {
        Self {
            session_timeout,
            ..self
        }
    }

    pub fn on_assignment(self, on_assignment: Option<Arc<DynConsumerAssignment>>) -> Builder<M, T> {
        Builder {
            group_id: self.group_id,
            topics: self.topics,
            metadata: self.metadata,
            on_assignment,
            rebalance_timeout: self.rebalance_timeout,
            session_timeout: self.session_timeout,
        }
    }
}

impl Builder<MetadataResponse, Vec<String>> {
    const REBALANCE_TIMEOUT_MS_DEFAULT: u64 = 60_000;
    const SESSION_TIMEOUT_MS_DEFAULT: u64 = 10_000;

    pub fn build(self) -> GroupConsumer {
        GroupConsumer {
            group_id: self.group_id,
            topics: self.topics,
            rebalance_timeout: self
                .rebalance_timeout
                .unwrap_or_else(|| Duration::from_millis(Self::REBALANCE_TIMEOUT_MS_DEFAULT)),
            session_timeout: self
                .session_timeout
                .unwrap_or_else(|| Duration::from_millis(Self::SESSION_TIMEOUT_MS_DEFAULT)),
            member_state: MemberState::Outsider {
                generation_id: None,
                member_id: None,
                member_assignment: None,
            },
            on_assignment: self.on_assignment,
            metadata: self.metadata,
            errors: Default::default(),
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum MemberState {
    Outsider {
        generation_id: Option<i32>,
        member_id: Option<String>,
        member_assignment: Option<MemberAssignment>,
    },
    Follower {
        generation_id: i32,
        member_id: String,
        member_assignment: Option<MemberAssignment>,
    },
    Leader {
        generation_id: i32,
        member_id: String,
        member_assignment: Option<MemberAssignment>,
    },
}

impl MemberState {
    fn is_outsider(&self) -> bool {
        matches!(self, Self::Outsider { .. })
    }

    fn is_follower(&self) -> bool {
        matches!(self, Self::Follower { .. })
    }

    fn is_leader(&self) -> bool {
        matches!(self, Self::Leader { .. })
    }

    fn generation_id(&self) -> Option<i32> {
        match self {
            MemberState::Outsider { generation_id, .. } => *generation_id,
            MemberState::Follower { generation_id, .. } => Some(*generation_id),
            MemberState::Leader { generation_id, .. } => Some(*generation_id),
        }
    }

    fn member_id(&self) -> Option<&str> {
        match self {
            MemberState::Outsider { member_id, .. } => member_id.as_deref(),
            MemberState::Follower { member_id, .. } => Some(member_id.as_str()),
            MemberState::Leader { member_id, .. } => Some(member_id.as_str()),
        }
    }

    fn member_assignment(&self) -> Option<&MemberAssignment> {
        match self {
            MemberState::Outsider {
                member_assignment, ..
            } => member_assignment.as_ref(),
            MemberState::Follower {
                member_assignment, ..
            } => member_assignment.as_ref(),
            MemberState::Leader {
                member_assignment, ..
            } => member_assignment.as_ref(),
        }
    }
}

pub struct GroupConsumer {
    pub group_id: String,
    pub topics: Vec<String>,
    pub rebalance_timeout: Duration,
    pub session_timeout: Duration,
    member_state: MemberState,
    metadata: MetadataResponse,
    on_assignment: Option<Arc<DynConsumerAssignment>>,
    errors: Arc<Mutex<BTreeMap<ErrorCode, u64>>>,
}

impl GroupConsumer {
    pub fn builder(
        group_id: impl Into<String>,
    ) -> Builder<PhantomData<MetadataResponse>, PhantomData<Vec<String>>> {
        Builder::new(group_id)
    }

    pub fn is_outsider(&self) -> bool {
        self.member_state.is_outsider()
    }

    pub fn is_follower(&self) -> bool {
        self.member_state.is_follower()
    }

    pub fn is_leader(&self) -> bool {
        self.member_state.is_leader()
    }

    pub fn generation_id(&self) -> Option<i32> {
        self.member_state.generation_id()
    }

    pub fn member_id(&self) -> Option<&str> {
        self.member_state.member_id()
    }

    pub fn member_assignment(&self) -> Option<&MemberAssignment> {
        self.member_state.member_assignment()
    }

    pub fn rebalance_timeout(&self) -> Duration {
        self.rebalance_timeout
    }

    pub fn session_timeout(&self) -> Duration {
        self.session_timeout
    }

    pub fn errors(&self) -> Result<BTreeMap<ErrorCode, u64>, Error> {
        self.errors
            .lock()
            .map(|errors| errors.clone())
            .map_err(Into::into)
    }
}

impl GroupConsumer {
    const ASSIGNORS: &[Assignor] = &[
        Assignor::Range,
        Assignor::RoundRobin,
        Assignor::Uniform,
        Assignor::CooperativeSticky,
    ];

    fn join_protocol(
        &self,
        assignor: &Assignor,
        generation_id: Option<i32>,
        member_assignment: Option<&MemberAssignment>,
    ) -> Result<JoinGroupRequestProtocol, Error> {
        Bytes::try_from(
            &MemberMetadata::default()
                .version(
                    member_assignment
                        .as_ref()
                        .map_or(ConsumerProtocolSubscription::V3, |member_assignment| {
                            member_assignment.version
                        }),
                )
                .subscription(
                    ConsumerProtocolSubscription::default()
                        .topics(self.topics.clone())
                        .owned_partitions(
                            member_assignment
                                .as_ref()
                                .map(|member_assignment| {
                                    member_assignment.assignment.assigned_partitions.clone()
                                })
                                .unwrap_or_default()
                                .into_iter(),
                        )
                        .generation_id(generation_id.or(Some(-1))),
                ),
        )
        .map(|metadata| {
            JoinGroupRequestProtocol::default()
                .name(assignor.as_ref().into())
                .metadata(metadata)
        })
    }

    fn join_protocols(
        &self,
        generation_id: Option<i32>,
        member_assignment: Option<&MemberAssignment>,
    ) -> Result<Vec<JoinGroupRequestProtocol>, Error> {
        Self::ASSIGNORS
            .iter()
            .map(|assignor| self.join_protocol(assignor, generation_id, member_assignment))
            .collect::<Result<Vec<_>, Error>>()
    }

    fn join_request(
        &self,
        generation_id: Option<i32>,
        member_id: Option<String>,
        member_assignment: Option<MemberAssignment>,
        reason: &str,
    ) -> Result<JoinGroupRequest, Error> {
        self.join_protocols(generation_id, member_assignment.as_ref())
            .map(Some)
            .map(|protocols| {
                JoinGroupRequest::default()
                    .group_id(self.group_id.clone())
                    .protocol_type(CONSUMER.into())
                    .rebalance_timeout_ms(Some(self.rebalance_timeout.as_millis() as i32))
                    .session_timeout_ms(self.session_timeout.as_millis() as i32)
                    .member_id(member_id.to_owned().unwrap_or_default())
                    .reason(Some(reason.into()))
                    .protocols(protocols)
            })
    }

    /// Resets the consumer to an outsider after the coordinator rejected its membership with
    /// `error_code`, and returns the JoinGroup request that rejoins the group.
    ///
    /// Kafka's consumer clears its generation after a [JoinGroup], a [SyncGroup] or a
    /// [Heartbeat] error, and so treats its partitions as [lost]. This consumer does the same,
    /// and clears its assignment. It keeps its member id only for
    /// [`ErrorCode::IllegalGeneration`], because the coordinator still knows the member and
    /// rejects only its generation.
    ///
    /// [JoinGroup]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java#L682-L690
    /// [SyncGroup]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java#L876-L882
    /// [Heartbeat]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractCoordinator.java#L1263-L1270
    /// [lost]: https://github.com/apache/kafka/blob/3.9.1/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java#L773-L774
    fn rejoin(&mut self, error_code: ErrorCode, reason: &str) -> Result<Body, Error> {
        self.errors
            .lock()
            .map(|mut errors| *errors.entry(error_code).or_default() += 1)?;

        let member_id = if error_code == ErrorCode::IllegalGeneration {
            self.member_state.member_id().map(ToOwned::to_owned)
        } else {
            None
        };

        self.member_state = MemberState::Outsider {
            generation_id: None,
            member_id: member_id.clone(),
            member_assignment: None,
        };

        self.join_request(None, member_id, None, reason)
            .map(Into::into)
    }

    #[instrument(skip(self, input), fields(group_id = self.group_id, member_state = ?self.member_state), ret)]
    pub fn next_action(&mut self, input: Option<Body>) -> Result<Body, Error> {
        match (&mut self.member_state, input) {
            // initial
            //
            (
                MemberState::Outsider {
                    generation_id,
                    member_id,
                    member_assignment,
                },
                None,
            ) => {
                let generation_id = *generation_id;
                let member_id = member_id.clone();
                let member_assignment = member_assignment.clone();

                self.join_request(generation_id, member_id, member_assignment, "join")
                    .map(Into::into)
            }

            // Join
            //
            (
                MemberState::Outsider {
                    generation_id,
                    member_id,
                    member_assignment,
                },
                Some(Body::JoinGroupResponse(JoinGroupResponse { error_code, .. })),
            ) if i16::from(ErrorCode::NotCoordinator) == error_code => {
                self.errors
                    .lock()
                    .map(|mut errors| *errors.entry(ErrorCode::NotCoordinator).or_default() += 1)?;

                let generation_id = *generation_id;
                let member_id = member_id.clone();
                let member_assignment = member_assignment.clone();

                self.join_request(generation_id, member_id, member_assignment, "join")
                    .map(Into::into)
            }

            (
                MemberState::Outsider {
                    generation_id,
                    member_id,
                    member_assignment,
                },
                Some(Body::JoinGroupResponse(JoinGroupResponse {
                    error_code,
                    generation_id: current_generation_id,
                    member_id: required_member_id,
                    ..
                })),
            ) if error_code == i16::from(ErrorCode::MemberIdRequired) => {
                self.errors.lock().map(|mut errors| {
                    *errors.entry(ErrorCode::MemberIdRequired).or_default() += 1
                })?;

                _ = generation_id.replace(current_generation_id);
                _ = member_id.replace(required_member_id);

                let generation_id = *generation_id;
                let member_id = member_id.clone();
                let member_assignment = member_assignment.clone();

                self.join_request(
                    generation_id,
                    member_id,
                    member_assignment,
                    "join member id required",
                )
                .map(Into::into)
            }

            (
                MemberState::Outsider { .. },
                Some(Body::JoinGroupResponse(JoinGroupResponse { error_code, .. })),
            ) if error_code == i16::from(ErrorCode::UnknownMemberId) => self.rejoin(
                ErrorCode::UnknownMemberId,
                "join after join with unknown member id",
            ),

            (
                MemberState::Outsider { .. },
                Some(Body::JoinGroupResponse(JoinGroupResponse {
                    error_code,
                    generation_id,
                    protocol_type,
                    protocol_name,
                    leader,
                    member_id,
                    members,
                    ..
                })),
            ) if error_code == i16::from(ErrorCode::None) && leader == member_id => {
                self.errors
                    .lock()
                    .map(|mut errors| *errors.entry(ErrorCode::None).or_default() += 1)?;

                self.member_state = MemberState::Leader {
                    generation_id,
                    member_id,
                    member_assignment: None,
                };

                debug!(?self.member_state);

                protocol_name
                    .as_deref()
                    .and_then(|name| name.parse::<Assignor>().ok())
                    .map_or_else(
                        || Box::new(RangeAssignor) as Box<dyn ConsumerAssignor>,
                        |assignor| Box::new(assignor) as Box<dyn ConsumerAssignor>,
                    )
                    .assign(members.as_deref().unwrap_or_default(), &self.metadata)
                    .inspect(|assignments| {
                        for assignment in assignments {
                            debug!(
                                member_id = assignment.member_id,
                                assignment =
                                    ?MemberAssignment::try_from(assignment.assignment.clone()).ok()
                            );
                        }
                    })
                    .map(Some)
                    .map(|assignments| {
                        SyncGroupRequest::default()
                            .generation_id(generation_id)
                            .group_id(self.group_id.clone())
                            .member_id(leader)
                            .assignments(assignments)
                            .protocol_name(protocol_name)
                            .protocol_type(protocol_type)
                    })
                    .map(Into::into)
            }

            (
                MemberState::Outsider { .. },
                Some(Body::JoinGroupResponse(JoinGroupResponse {
                    error_code,
                    generation_id,
                    protocol_type,
                    protocol_name,
                    member_id,
                    ..
                })),
            ) if i16::from(ErrorCode::None) == error_code => {
                self.errors
                    .lock()
                    .map(|mut errors| *errors.entry(ErrorCode::None).or_default() += 1)?;

                self.member_state = MemberState::Follower {
                    generation_id,
                    member_id: member_id.clone(),
                    member_assignment: None,
                };

                debug!(?self.member_state);

                Ok(SyncGroupRequest::default()
                    .generation_id(generation_id)
                    .group_id(self.group_id.clone())
                    .member_id(member_id)
                    .assignments(Some([].into()))
                    .protocol_name(protocol_name)
                    .protocol_type(protocol_type)
                    .into())
            }

            // Sync
            //
            (
                MemberState::Leader {
                    member_assignment,
                    generation_id,
                    member_id,
                }
                | MemberState::Follower {
                    member_assignment,
                    generation_id,
                    member_id,
                },
                Some(Body::SyncGroupResponse(SyncGroupResponse {
                    error_code,
                    assignment,
                    ..
                })),
            ) if i16::from(ErrorCode::None) == error_code => {
                self.errors
                    .lock()
                    .map(|mut errors| *errors.entry(ErrorCode::None).or_default() += 1)?;

                assert!(!assignment.is_empty(), "member: {member_id}");

                let ma = MemberAssignment::try_from(assignment).inspect(|ma| debug!(%ma))?;
                _ = member_assignment.replace(ma.clone());

                if let Some(on_assignment) = self.on_assignment.as_ref() {
                    on_assignment(self.group_id.clone(), ma)
                }

                Ok(HeartbeatRequest::default()
                    .group_id(self.group_id.clone())
                    .generation_id(*generation_id)
                    .member_id(member_id.clone())
                    .into())
            }

            (
                MemberState::Leader {
                    generation_id,
                    member_id,
                    member_assignment,
                    ..
                }
                | MemberState::Follower {
                    generation_id,
                    member_id,
                    member_assignment,
                    ..
                },
                Some(Body::SyncGroupResponse(SyncGroupResponse {
                    error_code,
                    assignment,
                    ..
                })),
            ) if error_code == i16::from(ErrorCode::RebalanceInProgress) => {
                self.errors.lock().map(|mut errors| {
                    *errors.entry(ErrorCode::RebalanceInProgress).or_default() += 1
                })?;

                assert!(assignment.is_empty());

                let generation_id = None;
                let member_id = Some(member_id.to_owned());
                let member_assignment = member_assignment.to_owned();

                self.member_state = MemberState::Outsider {
                    generation_id,
                    member_id: member_id.clone(),
                    member_assignment: member_assignment.clone(),
                };

                self.join_request(
                    generation_id,
                    member_id,
                    member_assignment,
                    "join after sync with rebalance",
                )
                .map(Into::into)
            }

            (
                MemberState::Leader { .. } | MemberState::Follower { .. },
                Some(Body::SyncGroupResponse(SyncGroupResponse { error_code, .. })),
            ) if error_code == i16::from(ErrorCode::UnknownMemberId) => self.rejoin(
                ErrorCode::UnknownMemberId,
                "join after sync with unknown member id",
            ),

            (
                MemberState::Leader { .. } | MemberState::Follower { .. },
                Some(Body::SyncGroupResponse(SyncGroupResponse { error_code, .. })),
            ) if error_code == i16::from(ErrorCode::IllegalGeneration) => self.rejoin(
                ErrorCode::IllegalGeneration,
                "join after sync with illegal generation",
            ),

            // Heartbeat
            //
            (
                MemberState::Leader { .. } | MemberState::Follower { .. },
                Some(Body::HeartbeatResponse(HeartbeatResponse { error_code, .. })),
            ) if error_code == i16::from(ErrorCode::UnknownMemberId) => {
                self.rejoin(ErrorCode::UnknownMemberId, "join after unknown member id")
            }

            (
                MemberState::Leader { .. } | MemberState::Follower { .. },
                Some(Body::HeartbeatResponse(HeartbeatResponse { error_code, .. })),
            ) if error_code == i16::from(ErrorCode::IllegalGeneration) => self.rejoin(
                ErrorCode::IllegalGeneration,
                "join after illegal generation",
            ),

            (
                MemberState::Leader {
                    generation_id,
                    member_id,
                    member_assignment,
                }
                | MemberState::Follower {
                    generation_id,
                    member_id,
                    member_assignment,
                },
                Some(Body::HeartbeatResponse(HeartbeatResponse { error_code, .. })),
            ) if error_code == i16::from(ErrorCode::RebalanceInProgress) => {
                self.errors.lock().map(|mut errors| {
                    *errors.entry(ErrorCode::RebalanceInProgress).or_default() += 1
                })?;

                let generation_id = Some(generation_id.to_owned());
                let member_id = Some(member_id.to_owned());
                let member_assignment = member_assignment.to_owned();

                self.member_state = MemberState::Outsider {
                    generation_id,
                    member_id: member_id.clone(),
                    member_assignment: member_assignment.clone(),
                };

                self.join_request(
                    generation_id,
                    member_id,
                    member_assignment,
                    "join after rebalance",
                )
                .map(Into::into)
            }

            (
                MemberState::Leader {
                    generation_id,
                    member_id,
                    ..
                }
                | MemberState::Follower {
                    generation_id,
                    member_id,
                    ..
                },
                Some(Body::HeartbeatResponse(HeartbeatResponse { error_code, .. })),
            ) if error_code == i16::from(ErrorCode::None) => {
                self.errors
                    .lock()
                    .map(|mut errors| *errors.entry(ErrorCode::None).or_default() += 1)?;

                Ok(HeartbeatRequest::default()
                    .group_id(self.group_id.clone())
                    .generation_id(*generation_id)
                    .member_id(member_id.clone())
                    .into())
            }

            // Fall through
            //
            (state, input) => todo!("state: {state:?}, input: {input:?}"),
        }
    }
}
