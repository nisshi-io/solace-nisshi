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

//! A [`Storage`] that wraps another has two ways in for a produce: [`Storage::produce`] and
//! [`Storage::produce_inflated`]. Client produces use the second, and a wrapper's own fallbacks
//! and the batcher's flushes use the first. These tests check that each wrapper passes the batch
//! on and applies its side effects the same way through both, so that a side effect added to one
//! of them still reaches every produce.

use bytes::Bytes;
use nisshi_sans_io::record::{Record, deflated, inflated};
use opentelemetry::{KeyValue, global};
use opentelemetry_sdk::metrics::{
    InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
    data::{AggregatedMetrics, MetricData, ResourceMetrics, ScopeMetrics},
};
use rama::{Layer as _, Service as _};
use std::{
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};
use tokio::{
    sync::Semaphore,
    task::{JoinSet, yield_now},
};
use tokio_util::sync::CancellationToken;
use tracing::{
    Subscriber,
    span::{Attributes, Id},
    subscriber::DefaultGuard,
};
use tracing_subscriber::{
    Layer,
    layer::{Context, SubscriberExt as _},
    registry::LookupSpan,
};

use crate::{
    ChannelRequestLayer, LatencyIntroducingStorage, ProduceRequestBatcher, RequestChannelService,
    RequestStorageService, Result, SemaphoreProxy, Storage, Topition, bounded_channel,
    decoded_batch_probe::probe,
    service::{
        ChannelRequest, Request, RequestReceiver, Response,
        counting_storage::{CountingStorage, Listing, Produced},
    },
};

/// Produces through `wrapper` once with [`Storage::produce`] and once with
/// [`Storage::produce_inflated`], and checks that `storage` got the same batch from both, with the
/// decoded batch only from the second.
async fn through_both(wrapper: impl Storage, storage: &CountingStorage) -> Result<()> {
    let deflated = deflated::Batch::default();
    let inflated = inflated::Batch::default();
    let topition = Topition::new("abc", 0);

    let plain = wrapper
        .produce(Some("transaction"), &topition, deflated.clone())
        .await?;

    let decoded = wrapper
        .produce_inflated(
            Some("transaction"),
            &topition,
            deflated.clone(),
            inflated.clone(),
        )
        .await?;

    assert_eq!((0, 0), (plain, decoded));

    // A produce that loses its transaction id is stored as a non-transactional batch, which a
    // `read_committed` consumer reads before the transaction commits.
    let expected = |inflated| Produced {
        transaction_id: Some("transaction".to_owned()),
        topition: topition.clone(),
        deflated: deflated.clone(),
        inflated,
    };

    assert_eq!(
        vec![expected(None), expected(Some(inflated.clone()))],
        storage.produced(),
        "the wrapper did not pass on the same batch through both produce methods"
    );

    Ok(())
}

fn storage() -> CountingStorage {
    CountingStorage::new(Listing::Topics, &["abc"])
}

/// Serves the requests of `receiver` from `storage` until `cancellation`.
fn serve_channel(
    receiver: RequestReceiver,
    storage: CountingStorage,
    cancellation: CancellationToken,
) -> JoinSet<impl Send + 'static> {
    let mut serving = JoinSet::new();

    _ = serving.spawn(async move {
        ChannelRequestLayer::new(cancellation)
            .into_layer(RequestStorageService::new(storage))
            .serve(receiver)
            .await
    });

    serving
}

#[tokio::test]
async fn arc_passes_on_both_produce_methods() -> Result<()> {
    let storage = storage();

    through_both(Arc::new(storage.clone()), &storage).await
}

#[tokio::test]
async fn box_passes_on_both_produce_methods() -> Result<()> {
    let storage = storage();

    through_both(Box::new(storage.clone()), &storage).await
}

#[tokio::test]
async fn latency_storage_passes_on_both_produce_methods() -> Result<()> {
    let storage = storage();

    through_both(
        LatencyIntroducingStorage::new(storage.clone()).with_latency(0..1),
        &storage,
    )
    .await
}

#[tokio::test]
async fn semaphore_proxy_passes_on_both_produce_methods() -> Result<()> {
    let storage = storage();

    through_both(SemaphoreProxy::new(storage.clone()), &storage).await
}

#[tokio::test]
async fn request_channel_passes_on_both_produce_methods() -> Result<()> {
    let storage = storage();

    let (sender, receiver) = bounded_channel(1);
    let cancellation = CancellationToken::new();
    let serving = serve_channel(receiver, storage.clone(), cancellation.clone());

    let outcome = through_both(RequestChannelService::new(sender), &storage).await;

    cancellation.cancel();
    _ = serving.join_all().await;

    outcome
}

#[tokio::test]
async fn batcher_without_a_delay_passes_on_both_produce_methods() -> Result<()> {
    let storage = storage();

    through_both(ProduceRequestBatcher::new(storage.clone()), &storage).await
}

/// With a delay the batcher holds the decoded batch in its queue. The default batch is
/// idempotent, so the flush writes it as sent, and passes the held decoded batch on.
#[tokio::test]
async fn batcher_with_a_delay_passes_on_both_produce_methods() -> Result<()> {
    let storage = storage();

    through_both(
        ProduceRequestBatcher::new(storage.clone())
            .with_maximum_delay(Some(Duration::from_millis(1))),
        &storage,
    )
    .await
}

/// A produce that waits for a slot in the channel holds only its compressed bytes, so a queue of
/// waiting produces does not hold a decoded batch each.
#[tokio::test]
async fn a_produce_that_waits_for_a_channel_slot_holds_no_decoded_batch() -> Result<()> {
    let (sender, mut receiver) = bounded_channel(1);
    let channel = RequestChannelService::new(sender);

    let (inflated, dropped) = probe();
    let topition = Topition::new("abc", 0);
    let waiting_topition = Topition::new("xyz", 3);

    // The first produce takes the channel's only slot, and nothing receives it yet.
    let first = channel.produce(None, &topition, deflated::Batch::default());

    let second = async {
        yield_now().await;

        channel
            .produce_inflated(
                Some("transaction"),
                &waiting_topition,
                deflated::Batch::default(),
                inflated,
            )
            .await
    };

    let serve = async {
        // Every branch has been polled once after these yields, so the second produce is waiting.
        for _ in 0..4 {
            yield_now().await;
        }
        let held = !dropped.load(Ordering::SeqCst);

        let mut sent_without = true;
        let mut received = Vec::new();
        for offset in 0..2 {
            let ChannelRequest {
                request,
                response: reply,
                ..
            } = receiver.recv().await.expect("a produce request");

            let Request::Produce {
                transaction_id,
                topition,
                inflated,
                ..
            } = request
            else {
                panic!("{request:?}");
            };
            sent_without &= inflated.is_none();
            received.push((transaction_id, topition));

            reply
                .send(Ok(Response::Produce(Ok(offset))))
                .expect("the producer");
        }

        (held, sent_without, received)
    };

    let (first, second, (held, sent_without, received)) = tokio::join!(first, second, serve);

    assert_eq!(0, first?);
    assert_eq!(1, second?);
    assert!(!held, "the waiting produce still held the decoded batch");
    assert!(
        sent_without,
        "a produce reached storage with a decoded batch"
    );

    // A waiting produce that lost its transaction id would be stored as a non-transactional
    // batch.
    assert_eq!(
        vec![
            (None, topition),
            (Some("transaction".to_owned()), waiting_topition)
        ],
        received,
        "the waiting produce did not forward its transaction id and topition"
    );

    Ok(())
}

/// The target and the name of each span that a subscriber opened, in order.
#[derive(Clone, Debug, Default)]
struct OpenedSpans(Arc<Mutex<Vec<(&'static str, &'static str)>>>);

impl OpenedSpans {
    /// Returns the names of the spans opened with `target`, in order.
    fn named(&self, target: &str) -> Vec<&'static str> {
        self.0
            .lock()
            .expect("spans")
            .iter()
            .filter(|(opened, _)| *opened == target)
            .map(|(_, name)| *name)
            .collect()
    }
}

impl<S> Layer<S> for OpenedSpans
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attributes: &Attributes<'_>, _id: &Id, _context: Context<'_, S>) {
        let metadata = attributes.metadata();
        self.0
            .lock()
            .expect("spans")
            .push((metadata.target(), metadata.name()));
    }
}

/// Records each span that this thread opens until the guard drops. The default subscriber is
/// local to the thread, so a test that uses this runs on the current-thread runtime.
fn record_spans() -> (OpenedSpans, DefaultGuard) {
    let spans = OpenedSpans::default();
    let guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));
    (spans, guard)
}

/// Produces through `proxy` twice, the second with a decoded batch while the first holds the
/// proxy's permit in the storage, and checks that the second waited for the permit: it reached the
/// storage without its decoded batch.
async fn produce_that_waits_for_the_proxy_permit() -> Result<()> {
    // The gate holds the first produce in the storage, so that it keeps the proxy's permit.
    let gate = Arc::new(Semaphore::new(0));
    let storage = storage().with_produce_gate(Arc::clone(&gate));
    let proxy = SemaphoreProxy::new(storage.clone());
    let topition = Topition::new("abc", 0);

    let first = proxy.produce(None, &topition, deflated::Batch::default());

    let second = async {
        while storage.parked() == 0 {
            yield_now().await;
        }

        proxy
            .produce_inflated(
                None,
                &topition,
                deflated::Batch::default(),
                inflated::Batch::default(),
            )
            .await
    };

    let release = async {
        while storage.parked() == 0 {
            yield_now().await;
        }

        // The second produce has been polled after these yields, so it is waiting for the permit.
        for _ in 0..4 {
            yield_now().await;
        }

        gate.add_permits(2);
    };

    let (first, second, ()) = tokio::join!(first, second, release);
    _ = first?;
    _ = second?;

    // The second produce reaches the storage without its decoded batch only if it waited for the
    // permit, which is the path under test.
    assert_eq!(
        vec![None, None],
        storage
            .produced()
            .into_iter()
            .map(|produced| produced.inflated)
            .collect::<Vec<_>>()
    );

    Ok(())
}

/// Each produce through the proxy opens one span, also when it waits for the permit and so drops
/// its decoded batch.
#[tokio::test]
async fn a_produce_that_waits_for_the_proxy_permit_opens_one_span() -> Result<()> {
    let (spans, _guard) = record_spans();

    produce_that_waits_for_the_proxy_permit().await?;

    assert_eq!(
        vec!["produce", "produce"],
        spans.named("nisshi_storage::proxy")
    );

    Ok(())
}

/// Produces through a channel with one slot twice, the second with a decoded batch while the
/// first holds the slot and nothing receives it yet, so that the second waits for a slot.
async fn produce_that_waits_for_a_channel_slot() -> Result<()> {
    let (sender, mut receiver) = bounded_channel(1);
    let channel = RequestChannelService::new(sender);
    let topition = Topition::new("abc", 0);

    // The first produce takes the channel's only slot, and nothing receives it yet.
    let first = channel.produce(None, &topition, deflated::Batch::default());

    let second = async {
        yield_now().await;

        channel
            .produce_inflated(
                None,
                &topition,
                deflated::Batch::default(),
                inflated::Batch::default(),
            )
            .await
    };

    let serve = async {
        // Every branch has been polled once after these yields, so the second produce is waiting.
        for _ in 0..4 {
            yield_now().await;
        }

        for offset in 0..2 {
            let ChannelRequest {
                response: reply, ..
            } = receiver.recv().await.expect("a produce request");

            reply
                .send(Ok(Response::Produce(Ok(offset))))
                .expect("the producer");
        }
    };

    let (first, second, ()) = tokio::join!(first, second, serve);
    _ = first?;
    _ = second?;

    Ok(())
}

/// Each produce through the channel opens one span, whether it gets a slot at once or waits for
/// one, and whichever produce method the caller used.
#[tokio::test]
async fn a_produce_through_the_channel_opens_one_span() -> Result<()> {
    let (spans, _guard) = record_spans();

    produce_that_waits_for_a_channel_slot().await?;

    assert_eq!(
        vec!["produce", "produce"],
        spans.named("nisshi_storage::service")
    );

    Ok(())
}

/// Without a delay, both produce methods of the batcher open its span.
#[tokio::test]
async fn batcher_without_a_delay_opens_its_span_for_both_produce_methods() -> Result<()> {
    let (spans, _guard) = record_spans();

    let storage = storage();
    through_both(ProduceRequestBatcher::new(storage.clone()), &storage).await?;

    assert_eq!(
        vec!["produce_with", "produce_with"],
        spans.named("nisshi_storage::batch")
    );

    Ok(())
}

/// Installs an in-memory exporter as the global meter provider. A test calls this before the code
/// creates its first instrument. nextest runs each test in a process of its own, so the
/// instruments are created after this in every test that calls it.
fn install_exporter() -> (SdkMeterProvider, InMemoryMetricExporter) {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter.clone()).build())
        .build();
    global::set_meter_provider(provider.clone());
    (provider, exporter)
}

/// Returns the attributes and the value of each data point of the metric `name`, as the last
/// export holds them. The value of a histogram point is its count.
///
/// The exporter keeps every export, and the reader can export on its own timer before the flush.
/// Each export holds the totals so far, so the last one holds them all.
fn recorded(
    provider: &SdkMeterProvider,
    exporter: &InMemoryMetricExporter,
    name: &str,
) -> Vec<(Vec<(String, String)>, u64)> {
    provider.force_flush().expect("flush");

    exporter
        .get_finished_metrics()
        .expect("finished metrics")
        .last()
        .into_iter()
        .flat_map(ResourceMetrics::scope_metrics)
        .flat_map(ScopeMetrics::metrics)
        .filter(|metric| metric.name() == name)
        .flat_map(|metric| match metric.data() {
            AggregatedMetrics::U64(MetricData::Histogram(histogram)) => histogram
                .data_points()
                .map(|point| (sorted(point.attributes()), point.count()))
                .collect::<Vec<_>>(),
            AggregatedMetrics::U64(MetricData::Sum(sum)) => sum
                .data_points()
                .map(|point| (sorted(point.attributes()), point.value()))
                .collect::<Vec<_>>(),
            _ => vec![],
        })
        .collect()
}

/// Returns each attribute as its key and value, sorted by key.
fn sorted<'a>(attributes: impl Iterator<Item = &'a KeyValue>) -> Vec<(String, String)> {
    let mut sorted = attributes
        .map(|attribute| (attribute.key.to_string(), attribute.value.to_string()))
        .collect::<Vec<_>>();
    sorted.sort();
    sorted
}

/// Each produce through the proxy records one wait for the permit, through either produce method.
/// The proxy counts its produce path only for a produce that carries a decoded batch.
///
/// This test pins the metrics that the proxy records. It passes both when the produce methods
/// have code of their own and when they share one code path.
#[tokio::test]
async fn semaphore_proxy_records_its_metrics_once_per_produce() -> Result<()> {
    let (provider, exporter) = install_exporter();

    let storage = storage();
    through_both(SemaphoreProxy::new(storage.clone()), &storage).await?;

    assert_eq!(
        vec![(vec![("operation".to_owned(), "produce".to_owned())], 2)],
        recorded(
            &provider,
            &exporter,
            "nisshi_storage_proxy_semaphore_acquire_duration"
        )
    );

    assert_eq!(
        vec![(
            vec![
                ("path".to_owned(), "direct".to_owned()),
                ("stage".to_owned(), "semaphore".to_owned())
            ],
            1
        )],
        recorded(&provider, &exporter, "nisshi_storage_produce_path")
    );

    Ok(())
}

/// The channel counts its produce path only for a produce that carries a decoded batch.
///
/// This test pins the count that the channel records. It passes both when the produce methods
/// have code of their own and when they share one code path.
#[tokio::test]
async fn request_channel_counts_its_produce_path_only_with_a_decoded_batch() -> Result<()> {
    let (provider, exporter) = install_exporter();

    let storage = storage();
    let (sender, receiver) = bounded_channel(1);
    let cancellation = CancellationToken::new();
    let serving = serve_channel(receiver, storage.clone(), cancellation.clone());

    let outcome = through_both(RequestChannelService::new(sender), &storage).await;

    cancellation.cancel();
    _ = serving.join_all().await;
    outcome?;

    assert_eq!(
        vec![(
            vec![
                ("path".to_owned(), "direct".to_owned()),
                ("stage".to_owned(), "channel".to_owned())
            ],
            1
        )],
        recorded(&provider, &exporter, "nisshi_storage_produce_path")
    );

    Ok(())
}

/// The batcher counts its produce path only when it tries for a share of the decoded limit: a
/// produce that carries a decoded batch, with a delay, for a batch the queue would hold.
///
/// This test pins the count that the batcher records.
#[tokio::test]
async fn batcher_counts_its_produce_path_only_when_it_reserves() -> Result<()> {
    let (provider, exporter) = install_exporter();

    // The default batch is idempotent, so a storage that drops the decoded batch takes no share.
    let dropping = ProduceRequestBatcher::new(storage())
        .with_maximum_delay(Some(Duration::from_millis(1)))
        .with_hold_decoded(false);
    let topition = Topition::new("abc", 0);
    _ = dropping
        .produce(None, &topition, deflated::Batch::default())
        .await?;
    _ = dropping
        .produce_inflated(
            None,
            &topition,
            deflated::Batch::default(),
            inflated::Batch::default(),
        )
        .await?;

    let holding = storage();
    through_both(
        ProduceRequestBatcher::new(holding.clone())
            .with_maximum_delay(Some(Duration::from_millis(1))),
        &holding,
    )
    .await?;

    assert_eq!(
        vec![(
            vec![
                ("path".to_owned(), "direct".to_owned()),
                ("stage".to_owned(), "batcher".to_owned())
            ],
            1
        )],
        recorded(&provider, &exporter, "nisshi_storage_produce_path")
    );

    Ok(())
}

/// A produce that carries a decoded batch and waits for the proxy's permit counts the fallback,
/// and still records its wait.
#[tokio::test]
async fn a_produce_that_waits_for_the_proxy_permit_counts_a_fallback() -> Result<()> {
    let (provider, exporter) = install_exporter();

    produce_that_waits_for_the_proxy_permit().await?;

    assert_eq!(
        vec![(
            vec![
                ("path".to_owned(), "fallback".to_owned()),
                ("stage".to_owned(), "semaphore".to_owned())
            ],
            1
        )],
        recorded(&provider, &exporter, "nisshi_storage_produce_path")
    );

    assert_eq!(
        vec![(vec![("operation".to_owned(), "produce".to_owned())], 2)],
        recorded(
            &provider,
            &exporter,
            "nisshi_storage_proxy_semaphore_acquire_duration"
        )
    );

    Ok(())
}

/// A produce that carries a decoded batch and waits for a slot in the channel counts the
/// fallback.
#[tokio::test]
async fn a_produce_that_waits_for_a_channel_slot_counts_a_fallback() -> Result<()> {
    let (provider, exporter) = install_exporter();

    produce_that_waits_for_a_channel_slot().await?;

    assert_eq!(
        vec![(
            vec![
                ("path".to_owned(), "fallback".to_owned()),
                ("stage".to_owned(), "channel".to_owned())
            ],
            1
        )],
        recorded(&provider, &exporter, "nisshi_storage_produce_path")
    );

    Ok(())
}

/// A produce whose decoded batch is over the batcher's decoded limit counts the fallback.
#[tokio::test]
async fn a_produce_over_the_decoded_limit_counts_a_batcher_fallback() -> Result<()> {
    let (provider, exporter) = install_exporter();

    // One record gives the batch a decoded size above the limit of zero.
    let inflated = inflated::Batch::builder()
        .last_offset_delta(0)
        .record(Record::builder().value(Bytes::from_static(b"value").into()))
        .build()?;
    let deflated = deflated::Batch::try_from(inflated.clone())?;

    let batcher = ProduceRequestBatcher::new(storage())
        .with_maximum_delay(Some(Duration::from_millis(1)))
        .with_decoded_limit(0);

    _ = batcher
        .produce_inflated(None, &Topition::new("abc", 0), deflated, inflated)
        .await?;

    assert_eq!(
        vec![(
            vec![
                ("path".to_owned(), "fallback".to_owned()),
                ("stage".to_owned(), "batcher".to_owned())
            ],
            1
        )],
        recorded(&provider, &exporter, "nisshi_storage_produce_path")
    );

    Ok(())
}
