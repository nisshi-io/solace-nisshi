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
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt::Debug,
    hash::Hash,
    ops::Range,
    sync::Arc,
    time::Duration,
};

use anyhow::{Error, Result, anyhow};
use nisshi_broker::{
    NODE_ID, coordinator::group::administrator::Controller, service::coordinator::services,
};
use nisshi_sans_io::{
    Body, ErrorCode, Frame, FrameInput, HeartbeatResponse, JoinGroupResponse, MetadataResponse,
    SyncGroupResponse,
    consumer::{ConsumerProtocolAssignment, ConsumerProtocolSubscription, MemberAssignment},
    metadata_response::{MetadataResponsePartition, MetadataResponseTopic},
};
use nisshi_service::{
    BytesFrameLayer, ConsumerGroupLayer, ConsumerGroupService, FrameBytesLayer, FrameRouteService,
    LatencyIntroducingLayer,
};
use nisshi_storage::Storage;
use rama::{Layer as _, Service};
use rand::{RngExt, SeedableRng as _, rngs::SmallRng};
use tokio::{
    sync::{
        Notify,
        mpsc::{Receiver, Sender, channel},
    },
    task::{JoinSet, yield_now},
    time::{Instant, advance, pause, resume, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, warn};
use url::Url;

use crate::common::{self, alphanumeric_string, init_tracing, memory_storage};

pub async fn one_consumer_session_delay_after_initial_join(
    storage: impl Storage + Clone,
) -> Result<()> {
    let group: String = alphanumeric_string(15);

    let tp = [("t", 0..3)];

    let metadata = MetadataResponse::default().topics(Some(
        tp.iter()
            .map(|(topic, partitions)| {
                MetadataResponseTopic::default()
                    .name(Some((*topic).into()))
                    .partitions(Some(
                        partitions
                            .clone()
                            .map(|partition_index| {
                                MetadataResponsePartition::default()
                                    .partition_index(partition_index)
                            })
                            .collect(),
                    ))
            })
            .collect(),
    ));

    let latency_ms = 50..150;
    let seed = 0;

    let coordinator = Controller::with_storage(storage)?;

    let route = services(
        FrameRouteService::<nisshi_broker::Error>::builder(),
        coordinator,
    )
    .and_then(|builder| builder.build().map_err(Into::into))?;

    let c0 = (
        ConsumerGroupLayer::new(
            group,
            tp.iter().map(|(topic, _partitions)| *topic),
            metadata,
        ),
        LatencyIntroducingLayer::default()
            .with_seed(seed)
            .with_latency_millis(latency_ms),
        FrameBytesLayer,
        BytesFrameLayer::default(),
    )
        .into_layer(route);

    // c0: join without member id
    //
    let c0_next_action = join(&c0, None::<JoinGroupResponse>).await?;

    assert_eq!(
        i16::from(ErrorCode::MemberIdRequired),
        c0_next_action.error_code
    );
    assert_eq!(-1, c0_next_action.generation_id);
    assert!(c0_next_action.leader.is_empty());
    assert!(
        c0_next_action
            .members
            .as_ref()
            .is_some_and(|members| members.is_empty())
    );
    assert!(!c0_next_action.member_id.is_empty());

    let c0_member_id = c0_next_action.member_id.clone();

    // allow the session expire
    //
    // Pause the clock only for this deliberate jump. A paused tokio runtime auto-advances to
    // the next pending timer whenever it has nothing runnable, and the Postgres backend parks
    // on real socket I/O while a pool wait/create timeout is armed, so a clock paused for the
    // whole test fires that 10s timeout on the very first `pool.get()` (during storage
    // construction) after ~0.1s of wall time. Blocking-thread backends (lite, slatedb) are
    // unaffected because in-flight `spawn_blocking` work inhibits auto-advance.
    pause();
    yield_now().await;
    advance(c0.session_timeout()?).await;
    yield_now().await;
    resume();

    // join with member id
    //
    let c0_next_action = join(&c0, Some(c0_next_action)).await?;

    assert_eq!(i16::from(ErrorCode::None), c0_next_action.error_code);
    assert!(!c0_next_action.member_id.is_empty());
    assert_eq!(c0_next_action.leader, c0_next_action.member_id);
    assert!(
        c0_next_action
            .members
            .as_ref()
            .is_some_and(|members| members.len() == 1)
    );
    assert_eq!(c0_member_id.as_str(), c0_next_action.member_id.as_str());

    // sync
    //
    let c0_next_action = sync(&c0, Some(c0_next_action)).await?;

    assert!(!c0_next_action.assignment.is_empty());
    assert_eq!(i16::from(ErrorCode::None), c0_next_action.error_code);

    // heartbeat
    //
    let c0_next_action = heartbeat(&c0, Some(c0_next_action)).await?;

    assert_eq!(i16::from(ErrorCode::None), c0_next_action.error_code);

    assert_eq!(
        Some(
            MemberAssignment::default()
                .version(ConsumerProtocolSubscription::V3)
                .assignment(ConsumerProtocolAssignment::default().assigned_partitions(tp))
        ),
        c0.member_assignment()?
    );

    Ok(())
}

pub async fn one_consumer_next_action(storage: impl Storage + Clone) -> Result<()> {
    let group: String = alphanumeric_string(15);

    let tp = [("t", 0..3)];

    let metadata = MetadataResponse::default().topics(Some(
        tp.iter()
            .map(|(topic, partitions)| {
                MetadataResponseTopic::default()
                    .name(Some((*topic).into()))
                    .partitions(Some(
                        partitions
                            .clone()
                            .map(|partition_index| {
                                MetadataResponsePartition::default()
                                    .partition_index(partition_index)
                            })
                            .collect(),
                    ))
            })
            .collect(),
    ));

    let latency_ms = 50..150;
    let seed = 0;

    let coordinator = Controller::with_storage(storage)?;

    let route = services(
        FrameRouteService::<nisshi_broker::Error>::builder(),
        coordinator,
    )
    .and_then(|builder| builder.build().map_err(Into::into))?;

    let c0 = (
        ConsumerGroupLayer::new(
            group,
            tp.iter().map(|(topic, _partitions)| *topic),
            metadata,
        ),
        LatencyIntroducingLayer::default()
            .with_seed(seed)
            .with_latency_millis(latency_ms),
        FrameBytesLayer,
        BytesFrameLayer::default(),
    )
        .into_layer(route);

    let mut tasks = JoinSet::new();

    for (id, consumer) in [c0.clone()].into_iter().enumerate() {
        _ = tasks.spawn(async move { simple_consumer(format!("c{id}").as_str(), &consumer).await });
    }

    let results = tasks.join_all().await;
    debug!(?results);
    assert!(results.iter().all(|result| result.is_ok()));

    assert_eq!(
        Some(
            MemberAssignment::default()
                .version(ConsumerProtocolSubscription::V3)
                .assignment(ConsumerProtocolAssignment::default().assigned_partitions(tp))
        ),
        c0.member_assignment()?
    );

    Ok(())
}

pub async fn two_consumer_next_action(storage: impl Storage + Clone) -> Result<()> {
    let group: String = alphanumeric_string(15);

    let tp = [("t", 0..3)];

    let metadata = MetadataResponse::default().topics(Some(
        tp.iter()
            .map(|(topic, partitions)| {
                MetadataResponseTopic::default()
                    .name(Some((*topic).into()))
                    .partitions(Some(
                        partitions
                            .clone()
                            .map(|partition_index| {
                                MetadataResponsePartition::default()
                                    .partition_index(partition_index)
                            })
                            .collect(),
                    ))
            })
            .collect(),
    ));

    let latency_ms = 50..150;

    let coordinator = Controller::with_storage(storage)?;

    let route = services(
        FrameRouteService::<nisshi_broker::Error>::builder(),
        coordinator,
    )
    .and_then(|builder| builder.build().map_err(Into::into))?;

    let c0 = (
        ConsumerGroupLayer::new(
            group.clone(),
            tp.iter().map(|(topic, _partitions)| *topic),
            metadata.clone(),
        )
        .on_assignment(|group: String, assignment: MemberAssignment| debug!(group, %assignment)),
        LatencyIntroducingLayer::default()
            .with_seed(0)
            .with_latency_millis(latency_ms.clone()),
        FrameBytesLayer,
        BytesFrameLayer::default(),
    )
        .into_layer(route.clone());

    let c1 = (
        ConsumerGroupLayer::new(
            group.clone(),
            tp.iter().map(|(topic, _partitions)| *topic),
            metadata,
        )
        .on_assignment(|group: String, assignment: MemberAssignment| debug!(group, %assignment)),
        LatencyIntroducingLayer::default()
            .with_seed(1)
            .with_latency_millis(latency_ms),
        FrameBytesLayer,
        BytesFrameLayer::default(),
    )
        .into_layer(route);

    let mut tasks = JoinSet::new();

    const ITERATIONS: usize = 10;

    let consumers = [c0.clone(), c1.clone()];

    let (tx, _rx) = channel(consumers.len());
    let simulation_complete = CancellationToken::new();

    for (id, consumer) in consumers.into_iter().enumerate() {
        let tx = tx.clone();
        let simulation_complete = simulation_complete.clone();

        _ = tasks.spawn(async move {
            consumer_with_iterations(id, ITERATIONS, true, tx, simulation_complete, &consumer).await
        });
    }

    let results = tasks.join_all().await;
    debug!(?results);
    assert!(results.iter().all(|result| result.is_ok()));

    let c0_member_assignment = c0
        .member_assignment()?
        .expect("c0 has no member assignment");

    let c1_member_assignment = c1
        .member_assignment()?
        .expect("c1 has no member assignment");

    let assignments = c0_member_assignment
        .assignment
        .assigned_partitions
        .iter()
        .flat_map(|topic_partition| {
            topic_partition
                .partitions
                .iter()
                .map(|partition| (topic_partition.topic.clone(), *partition))
        })
        .chain(
            c1_member_assignment
                .assignment
                .assigned_partitions
                .iter()
                .flat_map(|topic_partition| {
                    topic_partition
                        .partitions
                        .iter()
                        .map(|partition| (topic_partition.topic.clone(), *partition))
                }),
        );

    assert_eq!(
        tp.iter()
            .cloned()
            .map(|(_topic, partitions)| partitions.sum::<i32>() as usize)
            .sum::<usize>(),
        assignments.clone().count()
    );

    assert!(
        has_unique_elements(assignments.clone()),
        "non unique: {assignments:?}"
    );

    Ok(())
}

async fn group_consumer_next_action(
    storage: impl Storage + Clone,
    consumers: Range<i32>,
) -> Result<()> {
    let group: String = alphanumeric_string(15);

    let partitions = 0..32;

    let tp = [("t", partitions)];

    let metadata = metadata(tp.iter());

    let latency_ms = 10..150;

    let coordinator = Controller::with_storage(storage)?;

    let route = services(
        FrameRouteService::<nisshi_broker::Error>::builder(),
        coordinator,
    )
    .and_then(|builder| builder.build().map_err(Into::into))?;

    let mut tasks = JoinSet::new();

    let consumers = consumers
        .into_iter()
        .map(|id| {
            consumer(
                group.as_str(),
                id as u64,
                tp.iter().map(|(topic, _partitions)| *topic),
                route.clone(),
                metadata.clone(),
                latency_ms.clone(),
            )
        })
        .collect::<Vec<_>>();

    let iterations = 5..10;

    let (tx, rx) = channel(consumers.len());
    let simulation = CancellationToken::new();

    let is_transient = |id| id % 2 == 0;

    for (id, consumer) in consumers.clone().into_iter().enumerate() {
        let tx = tx.clone();
        let simulation = simulation.clone();

        let consumer_iterations = if is_transient(id) {
            let mut rng = SmallRng::seed_from_u64(id as u64);
            rng.random_range(iterations.clone())
        } else {
            iterations.end * 4
        };

        debug!(consumer = format!("c{id}"), consumer_iterations);

        _ = tasks.spawn(async move {
            consumer_with_iterations(
                id,
                consumer_iterations,
                consumer_iterations > iterations.end,
                tx,
                simulation,
                &consumer,
            )
            .await
        });
    }

    let notify = Arc::new(Notify::new());

    _ = tasks.spawn(transient_wave_complete(
        rx,
        notify.clone(),
        simulation.clone(),
        consumers.clone(),
        is_transient,
    ));

    _ = tasks.spawn(stability_check(
        notify,
        simulation.clone(),
        consumers.clone(),
        is_transient,
    ));

    let results = tasks.join_all().await;
    debug!(?results);
    assert!(
        results
            .iter()
            .all(|result| result.as_ref().inspect_err(|err| warn!(?err)).is_ok())
    );

    for (id, consumer) in consumers.clone().iter().enumerate() {
        if is_transient(id) {
            debug!(ignore = format!("c{id}"));

            continue;
        }

        if let Ok(Some(member_assignment)) = consumer.member_assignment() {
            for topic_partition in member_assignment.assignment.assigned_partitions {
                debug!(id = format!("c{id}"), %topic_partition);
            }
        }
    }

    let assignments = topition_assignments(consumers.clone(), is_transient)?;

    assert!(
        has_unique_elements(assignments.clone()),
        "non unique: {assignments:?}"
    );

    assert_eq!(
        tp.iter()
            .map(|(_topic, partitions)| partitions.end as usize)
            .sum::<usize>(),
        assignments.clone().count()
    );

    _ = error_codes(consumers.clone()).inspect(|errors| debug!(?errors));
    Ok(())
}

#[tokio::test]
pub async fn two_consumer_interleave_join() -> Result<()> {
    let group: String = alphanumeric_string(15);

    let tp = [("t", 0..3)];

    let metadata = MetadataResponse::default().topics(Some(
        tp.iter()
            .map(|(topic, partitions)| {
                MetadataResponseTopic::default()
                    .name(Some((*topic).into()))
                    .partitions(Some(
                        partitions
                            .clone()
                            .map(|partition_index| {
                                MetadataResponsePartition::default()
                                    .partition_index(partition_index)
                            })
                            .collect(),
                    ))
            })
            .collect(),
    ));

    let latency_ms = 50..150;
    let seed = 0;

    let cluster = "nisshi";

    let storage = memory_storage(cluster, NODE_ID).await?;

    let coordinator = Controller::with_storage(storage)?;

    let route = services(
        FrameRouteService::<nisshi_broker::Error>::builder(),
        coordinator,
    )
    .and_then(|builder| builder.build().map_err(Into::into))?;

    let c0 = (
        ConsumerGroupLayer::new(
            group.clone(),
            tp.iter().map(|(topic, _partitions)| *topic),
            metadata.clone(),
        ),
        LatencyIntroducingLayer::default()
            .with_seed(seed)
            .with_latency_millis(latency_ms.clone()),
        FrameBytesLayer,
        BytesFrameLayer::default(),
    )
        .into_layer(route.clone());

    let c1 = (
        ConsumerGroupLayer::new(
            group,
            tp.iter().map(|(topic, _partitions)| *topic),
            metadata,
        ),
        LatencyIntroducingLayer::default()
            .with_seed(seed)
            .with_latency_millis(latency_ms),
        FrameBytesLayer,
        BytesFrameLayer::default(),
    )
        .into_layer(route);

    // c0: join without member id
    //
    let c0_next_action = join(&c0, None::<JoinGroupResponse>)
        .await
        .inspect(|c0_next_action| debug!(?c0_next_action))?;

    let c0_member_id = c0_next_action.member_id.clone();
    debug!(c0_member_id);

    assert_eq!(
        i16::from(ErrorCode::MemberIdRequired),
        c0_next_action.error_code
    );
    assert_eq!(-1, c0_next_action.generation_id);
    assert!(c0_next_action.leader.is_empty());
    assert!(
        c0_next_action
            .members
            .as_ref()
            .is_some_and(|members| members.is_empty())
    );
    assert!(!c0_next_action.member_id.is_empty());

    // c1: join without member id
    //
    let c1_next_action = join(&c1, None::<JoinGroupResponse>)
        .await
        .inspect(|c1_next_action| debug!(?c1_next_action))?;

    let c1_member_id = c1_next_action.member_id.clone();
    debug!(c1_member_id);

    assert_eq!(
        i16::from(ErrorCode::MemberIdRequired),
        c1_next_action.error_code
    );
    assert_eq!(-1, c1_next_action.generation_id);
    assert!(c1_next_action.leader.is_empty());
    assert!(
        c1_next_action
            .members
            .as_ref()
            .is_some_and(|members| members.is_empty())
    );
    assert!(!c1_next_action.member_id.is_empty());

    // c0 and c1: join with member id. The coordinator holds both joins until the initial
    // rebalance delay ends, as Kafka does, so they run at once, as two clients do.
    //
    let (c0_next_action, c1_next_action) = tokio::join!(
        join(&c0, Some(c0_next_action)),
        join(&c1, Some(c1_next_action))
    );
    let (c0_next_action, c1_next_action) = (c0_next_action?, c1_next_action?);
    debug!(?c0_next_action, ?c1_next_action);

    for next_action in [&c0_next_action, &c1_next_action] {
        assert_eq!(i16::from(ErrorCode::None), next_action.error_code);
        assert_eq!(1, next_action.generation_id);
    }

    assert_eq!(c0_member_id.as_str(), c0_next_action.member_id.as_str());
    assert_eq!(c1_member_id.as_str(), c1_next_action.member_id.as_str());
    assert_eq!(c0_next_action.leader, c1_next_action.leader);

    let (leader, follower) = if c0_next_action.leader == c0_member_id {
        (&c0_next_action, &c1_next_action)
    } else {
        (&c1_next_action, &c0_next_action)
    };

    assert!(
        leader
            .members
            .as_ref()
            .is_some_and(|members| members.len() == 2)
    );
    assert!(
        follower
            .members
            .as_ref()
            .is_some_and(|members| members.is_empty())
    );

    // c0 and c1: sync. The coordinator holds the follower's sync until the leader's
    // assignment arrives.
    //
    let (c0_next_action, c1_next_action) = tokio::join!(
        sync(&c0, Some(c0_next_action)),
        sync(&c1, Some(c1_next_action))
    );
    let (c0_next_action, c1_next_action) = (c0_next_action?, c1_next_action?);
    debug!(?c0_next_action, ?c1_next_action);

    for next_action in [&c0_next_action, &c1_next_action] {
        assert!(!next_action.assignment.is_empty());
        assert_eq!(i16::from(ErrorCode::None), next_action.error_code);
    }

    // c0: heartbeat
    //
    let c0_next_action = heartbeat(&c0, Some(c0_next_action))
        .await
        .inspect(|c0_next_action| debug!(?c0_next_action))?;

    assert_eq!(i16::from(ErrorCode::None), c0_next_action.error_code);

    // c1: heartbeat
    //
    let c1_next_action = heartbeat(&c1, Some(c1_next_action))
        .await
        .inspect(|c1_next_action| debug!(?c1_next_action))?;

    assert_eq!(i16::from(ErrorCode::None), c1_next_action.error_code);

    let c0_member_assignment = c0
        .member_assignment()?
        .expect("c0 has no member assignment");

    let c1_member_assignment = c1
        .member_assignment()?
        .expect("c1 has no member assignment");

    let assignments = c0_member_assignment
        .assignment
        .assigned_partitions
        .iter()
        .flat_map(|topic_partition| {
            topic_partition
                .partitions
                .iter()
                .map(|partition| (topic_partition.topic.clone(), *partition))
        })
        .chain(
            c1_member_assignment
                .assignment
                .assigned_partitions
                .iter()
                .flat_map(|topic_partition| {
                    topic_partition
                        .partitions
                        .iter()
                        .map(|partition| (topic_partition.topic.clone(), *partition))
                }),
        );

    assert_eq!(
        tp.iter()
            .cloned()
            .map(|(_topic, partitions)| partitions.sum::<i32>() as usize)
            .sum::<usize>(),
        assignments.clone().count()
    );

    assert!(
        has_unique_elements(assignments.clone()),
        "non unique: {assignments:?}"
    );

    Ok(())
}

#[instrument(skip(service))]
async fn simple_consumer<S>(name: &str, service: &S) -> Result<()>
where
    S: Service<Option<Body>, Output = Body>,
    S::Error: Into<Error>,
{
    // join without member id
    //
    let now = Instant::now();
    let next_action = join(service, None::<JoinGroupResponse>)
        .await
        .inspect(|next_action| debug!(?next_action, elapsed = ?now.elapsed()))?;

    let member_id = next_action.member_id.clone();
    debug!(member_id);

    // join with member id
    //
    let next_action = join(service, Some(next_action)).await?;

    assert_eq!(i16::from(ErrorCode::None), next_action.error_code);
    assert!(!next_action.member_id.is_empty());
    assert_eq!(next_action.leader, next_action.member_id);
    assert!(
        next_action
            .members
            .as_ref()
            .is_some_and(|members| members.len() == 1)
    );
    assert_eq!(member_id.as_str(), next_action.member_id.as_str());

    // sync
    //
    let next_action = sync(service, Some(next_action)).await?;

    assert!(!next_action.assignment.is_empty());
    assert_eq!(i16::from(ErrorCode::None), next_action.error_code);

    // heartbeat
    //
    let next_action = heartbeat(service, Some(next_action)).await?;

    assert_eq!(i16::from(ErrorCode::None), next_action.error_code);

    Ok(())
}

#[instrument(skip_all)]
async fn transient_wave_complete<S>(
    mut rx: Receiver<usize>,
    notification: Arc<Notify>,
    simulation: CancellationToken,
    consumers: Vec<ConsumerGroupService<S>>,
    is_transient: impl Fn(usize) -> bool,
) -> Result<()> {
    let mut transients = consumers
        .iter()
        .enumerate()
        .filter_map(|(id, _)| is_transient(id).then_some(id))
        .inspect(|transient| debug!(transient))
        .collect::<BTreeSet<_>>();

    while !simulation.is_cancelled() {
        tokio::select! {
            _ = simulation.cancelled() => (),

            Some(id) = rx.recv() => {
                debug!(completed = format!("c{id}"));

                if transients.remove(&id) && transients.is_empty() {
                    debug!("transients are complete");
                    notification.notify_one();
                }
            }
        }
    }

    Ok(())
}

#[instrument(skip_all)]
fn metadata<'b: 'a, 'a, T>(tp: T) -> MetadataResponse
where
    T: Iterator<Item = &'a (&'b str, Range<i32>)>,
{
    MetadataResponse::default().topics(Some(
        tp.map(|(topic, partitions)| {
            MetadataResponseTopic::default()
                .name(Some((*topic).into()))
                .partitions(Some(
                    partitions
                        .clone()
                        .map(|partition_index| {
                            MetadataResponsePartition::default().partition_index(partition_index)
                        })
                        .collect(),
                ))
        })
        .collect(),
    ))
}

#[instrument(skip(topics, route, metadata, latency_ms))]
fn consumer(
    group: &str,
    id: u64,
    topics: impl IntoIterator<Item = impl Into<String>>,
    route: impl Service<FrameInput, Output = Frame, Error = nisshi_broker::Error> + Clone,
    metadata: MetadataResponse,
    latency_ms: Range<u64>,
) -> ConsumerGroupService<
    impl Service<FrameInput, Output = Frame, Error = nisshi_broker::Error> + Clone,
> {
    (
        ConsumerGroupLayer::new(group, topics.into_iter(), metadata.clone()).on_assignment(
            |group: String, assignment: MemberAssignment| debug!(group, %assignment),
        ),
        LatencyIntroducingLayer::default()
            .with_seed(id)
            .with_latency_millis(latency_ms.clone()),
        FrameBytesLayer,
        BytesFrameLayer::default(),
    )
        .into_layer(route)
}

#[instrument(skip_all)]
fn topition_assignments<S>(
    consumers: Vec<ConsumerGroupService<S>>,
    is_transient: impl Fn(usize) -> bool,
) -> Result<impl Iterator<Item = (String, i32)> + Clone + Debug> {
    let assignments = consumers
        .into_iter()
        .enumerate()
        .filter_map(|(id, consumer)| (!is_transient(id)).then_some(consumer))
        .filter_map(|consumer| consumer.member_assignment().map_err(Into::into).transpose())
        .collect::<Result<Vec<_>>>()?;

    Ok(assignments
        .into_iter()
        .map(|member_assignment| member_assignment.assignment)
        .map(|consumer_protocol_assignment| consumer_protocol_assignment.assigned_partitions)
        .flat_map(|topic_partition| topic_partition.into_iter())
        .map(|topic_partition| (topic_partition.topic, topic_partition.partitions))
        .flat_map(|(topic, partitions)| {
            partitions
                .into_iter()
                .map(move |partition| (topic.clone(), partition))
        }))
}

#[instrument(skip_all)]
fn error_codes<S>(consumers: Vec<ConsumerGroupService<S>>) -> Result<BTreeMap<ErrorCode, u64>>
where
    S: Clone,
{
    let mut errors = BTreeMap::new();

    for consumer in consumers
        .clone()
        .into_iter()
        .map(|consumer| consumer.errors().map_err(Into::into))
        .collect::<Result<Vec<_>>>()?
    {
        for (k, v) in consumer {
            _ = errors
                .entry(k)
                .and_modify(|existing| *existing += v)
                .or_insert(v);
        }
    }

    Ok(errors)
}

#[instrument(skip_all)]
async fn stability_check<S>(
    notification: Arc<Notify>,
    simulation: CancellationToken,
    consumers: Vec<ConsumerGroupService<S>>,
    is_transient: impl Fn(usize) -> bool,
) -> Result<()> {
    let mut is_stable = false;

    notification.notified().await;
    debug!("notified, waiting for 2x session timeout");

    sleep(
        consumers
            .first()
            .map(|consumer| {
                consumer
                    .session_timeout()
                    .map(|session_timeout| session_timeout * 2)
            })
            .transpose()?
            .expect("session duration"),
    )
    .await;

    debug!("awaiting stability");

    while !is_stable {
        debug!(is_stable);
        sleep(Duration::from_secs(1)).await;

        is_stable = consumers
            .iter()
            .enumerate()
            .filter(|(id, _)| !is_transient(*id))
            .all(|(id, consumer)| {
                consumer
                    .stable_heartbeat_count()
                    .map(|heartbeats| {
                        heartbeats
                            .inspect(|heartbeats| debug!(id = format!("c{id}"), heartbeats))
                            .is_some_and(|heartbeats| heartbeats > 2)
                    })
                    .unwrap_or_default()
            });
    }

    debug!(is_stable);
    simulation.cancel();

    Ok(())
}

#[instrument(skip(iterations, id, tx, simulation, service), fields(name = format!("c{id}")))]
async fn consumer_with_iterations<S>(
    id: usize,
    mut iterations: usize,
    full_term: bool,
    tx: Sender<usize>,
    simulation: CancellationToken,
    service: &S,
) -> Result<()>
where
    S: Service<Option<Body>, Output = Body>,
    S::Error: Into<Error> + Send + Sync + 'static,
{
    let mut next_action = None;

    while iterations > 0 && !simulation.is_cancelled() {
        let instant = Instant::now();
        next_action = service
            .serve( next_action)
            .await
            .inspect(|next_action| debug!(?next_action, iterations, elapsed = ?instant.elapsed(), simulation = simulation.is_cancelled()))
            .map(Some)
            .map_err(Into::into)?;

        iterations -= 1;
    }

    _ = tx.send(id).await.ok();

    if full_term && !simulation.is_cancelled() {
        warn!("cancelling");
        simulation.cancel();
    }

    Ok(())
}

fn has_unique_elements<T>(iter: T) -> bool
where
    T: IntoIterator,
    T::Item: Eq + Hash,
{
    let mut uniq = HashSet::new();
    iter.into_iter().all(|x| uniq.insert(x))
}

#[instrument(skip_all)]
async fn join<I, S>(service: &S, input: Option<I>) -> Result<JoinGroupResponse, Error>
where
    S: Service<Option<Body>, Output = Body>,
    I: Into<Body>,
    S::Error: Into<Error>,
{
    let next_action = service
        .serve(input.map(Into::into))
        .await
        .inspect(|output| debug!(?output))
        .map_err(Into::into)?;

    if let Body::JoinGroupResponse(join_group) = next_action {
        Ok(join_group)
    } else {
        Err(anyhow!("expecting join response: {next_action:?}"))
    }
}

#[instrument(skip_all)]
async fn sync<I, S>(service: &S, input: Option<I>) -> Result<SyncGroupResponse, Error>
where
    S: Service<Option<Body>, Output = Body>,
    I: Into<Body>,
    S::Error: Into<Error>,
{
    let next_action = service
        .serve(input.map(Into::into))
        .await
        .inspect(|output| debug!(?output))
        .map_err(Into::into)?;

    if let Body::SyncGroupResponse(sync_group) = next_action {
        Ok(sync_group)
    } else {
        Err(anyhow!("expecting sync response: {next_action:?}"))
    }
}

#[instrument(skip_all)]
async fn heartbeat<I, S>(service: &S, input: Option<I>) -> Result<HeartbeatResponse, Error>
where
    S: Service<Option<Body>, Output = Body>,
    I: Into<Body>,
    S::Error: Into<Error>,
{
    let next_action = service
        .serve(input.map(Into::into))
        .await
        .inspect(|output| debug!(?output))
        .map_err(Into::into)?;

    if let Body::HeartbeatResponse(heartbeat) = next_action {
        Ok(heartbeat)
    } else {
        Err(anyhow!("expecting heartbeat response: {next_action:?}"))
    }
}

#[cfg(feature = "postgres")]
mod pg {
    use crate::common::StorageType;
    use nisshi_storage::ArcDynStorage;
    use rand::rng;
    use uuid::Uuid;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        common::storage_container(
            StorageType::Postgres,
            cluster,
            node,
            Url::parse("tcp://127.0.0.1/")?,
            None,
        )
        .await
        .map_err(Into::into)
    }

    #[tokio::test]
    async fn one_consumer_session_delay_after_initial_join() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::one_consumer_session_delay_after_initial_join(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn one_consumer_next_action() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::one_consumer_next_action(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn two_consumer_next_action() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::two_consumer_next_action(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn consumer_next_action_08c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..8).await?;

        Ok(())
    }

    #[tokio::test]
    async fn consumer_next_action_16c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..16).await?;

        Ok(())
    }

    #[tokio::test]
    async fn consumer_next_action_24c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..24).await?;

        Ok(())
    }

    #[tokio::test]
    async fn consumer_next_action_32c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..32).await?;

        Ok(())
    }
}

#[cfg(feature = "dynostore")]
mod in_memory {
    use nisshi_storage::ArcDynStorage;
    use rand::rng;
    use uuid::Uuid;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        memory_storage(cluster, node).await.map_err(Into::into)
    }

    #[tokio::test]
    async fn one_consumer_session_delay_after_initial_join() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::one_consumer_session_delay_after_initial_join(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn one_consumer_next_action() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::one_consumer_next_action(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn two_consumer_next_action() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::two_consumer_next_action(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn consumer_next_action_08c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..8).await?;

        Ok(())
    }

    #[tokio::test]
    async fn consumer_next_action_16c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..16).await?;

        Ok(())
    }

    #[tokio::test]
    async fn consumer_next_action_24c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..24).await?;

        Ok(())
    }

    #[tokio::test]
    async fn consumer_next_action_32c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..32).await?;

        Ok(())
    }
}

#[cfg(feature = "libsql")]
mod lite {
    use crate::common::lite_storage;
    use nisshi_storage::ArcDynStorage;
    use rand::rng;
    use uuid::Uuid;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        lite_storage(cluster, node).await.map_err(Into::into)
    }

    #[tokio::test]
    async fn one_consumer_session_delay_after_initial_join() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::one_consumer_session_delay_after_initial_join(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn one_consumer_next_action() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::one_consumer_next_action(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn two_consumer_next_action() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::two_consumer_next_action(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn consumer_next_action_08c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..8).await?;

        Ok(())
    }

    #[tokio::test]
    async fn consumer_next_action_16c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..16).await?;

        Ok(())
    }

    #[tokio::test]
    async fn consumer_next_action_24c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..24).await?;

        Ok(())
    }

    #[tokio::test]
    async fn consumer_next_action_32c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..32).await?;

        Ok(())
    }
}

#[cfg(feature = "turso")]
mod turso {
    use crate::common::turso_storage;
    use nisshi_storage::ArcDynStorage;
    use rand::rng;
    use uuid::Uuid;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        turso_storage(cluster, node).await.map_err(Into::into)
    }

    #[tokio::test]
    async fn one_consumer_session_delay_after_initial_join() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::one_consumer_session_delay_after_initial_join(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn one_consumer_next_action() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::one_consumer_next_action(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn two_consumer_next_action() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::two_consumer_next_action(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn consumer_next_action_08c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..8).await?;

        Ok(())
    }

    #[tokio::test]
    async fn consumer_next_action_16c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..16).await?;

        Ok(())
    }

    #[tokio::test]
    async fn consumer_next_action_24c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..24).await?;

        Ok(())
    }

    #[tokio::test]
    async fn consumer_next_action_32c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..32).await?;

        Ok(())
    }
}

#[cfg(feature = "slatedb")]
mod slatedb {
    use crate::common::slate_storage;
    use nisshi_storage::ArcDynStorage;
    use rand::rng;
    use uuid::Uuid;

    use super::*;

    async fn storage_container(
        cluster: impl Into<String> + Clone,
        node: i32,
    ) -> Result<ArcDynStorage> {
        slate_storage(cluster, node).await.map_err(Into::into)
    }

    #[tokio::test]
    async fn one_consumer_session_delay_after_initial_join() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::one_consumer_session_delay_after_initial_join(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn one_consumer_next_action() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::one_consumer_next_action(storage).await?;

        Ok(())
    }

    #[tokio::test]
    async fn two_consumer_next_action() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        super::two_consumer_next_action(storage).await?;

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn consumer_next_action_08c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..8).await?;

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn consumer_next_action_16c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..16).await?;

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn consumer_next_action_24c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..24).await?;

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn consumer_next_action_32c() -> Result<()> {
        let _guard = init_tracing()?;

        let cluster_id = Uuid::now_v7();
        let broker_id = rng().random_range(0..i32::MAX);

        let storage = storage_container(cluster_id, broker_id).await?;

        group_consumer_next_action(storage, 0..32).await?;

        Ok(())
    }
}
