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

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use nisshi_sans_io::{
    ApiKey as _, Body, ErrorCode, Frame, FrameInput, Header, HeartbeatResponse, MetadataResponse,
    consumer::{DynConsumerAssignment, GroupConsumer, MemberAssignment},
};
use rama::{Layer, Service, extensions::Extensions};
use tokio::time::sleep;
use tracing::debug;

#[derive(Clone, Default)]
pub struct ConsumerGroupLayer {
    group_id: String,
    topics: Vec<String>,
    metadata: MetadataResponse,
    rebalance_timeout: Option<Duration>,
    session_timeout: Option<Duration>,
    on_assignment: Option<Arc<DynConsumerAssignment>>,
}

impl fmt::Debug for ConsumerGroupLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(ConsumerGroupLayer))
            .field("group_id", &self.group_id)
            .field("topics", &self.topics)
            .field("metadata", &self.metadata)
            .field("rebalance_timeout", &self.rebalance_timeout)
            .field("session_timeout", &self.session_timeout)
            .finish_non_exhaustive()
    }
}

impl ConsumerGroupLayer {
    pub fn new(
        group_id: impl Into<String>,
        topics: impl IntoIterator<Item = impl Into<String>>,
        metadata: MetadataResponse,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            topics: topics.into_iter().map(Into::into).collect(),
            metadata,
            rebalance_timeout: Default::default(),
            session_timeout: Default::default(),
            on_assignment: Default::default(),
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

    pub fn on_assignment(
        self,
        on_assignment: impl Fn(String, MemberAssignment) + 'static + Send + Sync,
    ) -> Self {
        Self {
            on_assignment: Some(Arc::new(on_assignment)),
            ..self
        }
    }
}

impl<S> Layer<S> for ConsumerGroupLayer {
    type Service = ConsumerGroupService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        let consumer = Arc::new(Mutex::new(
            GroupConsumer::builder(self.group_id.clone())
                .topics(self.topics.clone())
                .metadata(self.metadata.clone())
                .on_assignment(self.on_assignment.clone())
                .build(),
        ));

        Self::Service {
            inner,
            consumer,
            heartbeats: Default::default(),
            extensions: Extensions::default(),
        }
    }
}

#[derive(Clone)]
pub struct ConsumerGroupService<S> {
    inner: S,
    consumer: Arc<Mutex<GroupConsumer>>,
    heartbeats: Arc<Mutex<Option<usize>>>,
    extensions: Extensions,
}

impl<S> ConsumerGroupService<S> {
    pub fn is_leader(&self) -> Result<bool, crate::Error> {
        self.consumer
            .lock()
            .map(|consumer| consumer.is_leader())
            .map_err(Into::into)
    }

    pub fn stable_heartbeat_count(&self) -> Result<Option<usize>, crate::Error> {
        self.heartbeats
            .lock()
            .map(|heartbeats| heartbeats.as_ref().cloned())
            .map_err(Into::into)
    }

    pub fn member_assignment(&self) -> Result<Option<MemberAssignment>, crate::Error> {
        self.consumer
            .lock()
            .map(|consumer| consumer.member_assignment().cloned())
            .map_err(Into::into)
    }

    pub fn rebalance_timeout(&self) -> Result<Duration, crate::Error> {
        self.consumer
            .lock()
            .map(|consumer| consumer.rebalance_timeout())
            .map_err(Into::into)
    }

    pub fn session_timeout(&self) -> Result<Duration, crate::Error> {
        self.consumer
            .lock()
            .map(|consumer| consumer.session_timeout())
            .map_err(Into::into)
    }

    pub fn errors(self) -> Result<BTreeMap<ErrorCode, u64>, crate::Error> {
        self.consumer
            .lock()
            .map_err(Into::into)
            .and_then(|consumer| consumer.errors().map_err(Into::into))
    }
}

impl<S> fmt::Debug for ConsumerGroupService<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(ConsumerGroupService)).finish()
    }
}

impl<S> Service<Option<Body>> for ConsumerGroupService<S>
where
    S: Service<FrameInput, Output = Frame>,
    S::Error: From<nisshi_sans_io::Error>
        + for<'a> From<PoisonError<MutexGuard<'a, GroupConsumer>>>
        + for<'a> From<PoisonError<MutexGuard<'a, Option<usize>>>>,
{
    type Output = Body;

    type Error = S::Error;

    async fn serve(&self, input: Option<Body>) -> Result<Self::Output, Self::Error> {
        let frame = self
            .consumer
            .lock()
            .map_err(Self::Error::from)
            .and_then(|mut consumer| {
                consumer
                    .next_action(input)
                    .inspect(|next_action| debug!(?next_action))
                    .map_err(Into::into)
            })
            .map(|body| {
                let api_key = body.api_key();
                let api_version = crate::routable_max_version(api_key).unwrap_or_default();

                Frame {
                    size: 0,
                    header: Header::Request {
                        api_key,
                        api_version,
                        correlation_id: 0,
                        client_id: Some(env!("CARGO_PKG_NAME").into()),
                    },
                    body,
                }
            })?;

        let output = self
            .inner
            .serve(FrameInput {
                frame,
                extensions: self.extensions.clone(),
            })
            .await
            .map(|frame| frame.body)
            .inspect(|input| debug!(input.api_name = input.api_name()))?;

        if output.api_key() == HeartbeatResponse::KEY {
            self.heartbeats
                .lock()
                .map(|mut heartbeats| *heartbeats.get_or_insert_default() += 1)?;

            // 1/3 of session timeout, otherwise 3 seconds
            sleep(
                self.session_timeout()
                    .ok()
                    .and_then(|session_timeout| session_timeout.checked_div(3))
                    .unwrap_or_else(|| Duration::from_secs(3)),
            )
            .await
        } else {
            _ = self
                .heartbeats
                .lock()
                .map(|mut heartbeats| heartbeats.take())?;
        }

        Ok(output)
    }
}
